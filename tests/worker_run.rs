// Integration tests for the production `Worker::run` loop: graceful
// shutdown, `wait_for_ready` wakeups, and concurrent activity execution.
// Each test drives `run()` on a single-threaded tokio runtime by joining it
// with the test logic, so no spawning or wall-clock coordination is needed,
// and every test is bounded by a timeout so a hang fails fast.

use durust::{
    DurableBackend, EventId, HistoryEventData, MemoryBackend, RunId, SqliteBackend, Worker,
};
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

fn block_on_tokio<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(TEST_TIMEOUT, future)
                .await
                .expect("test timed out")
        })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct NumberInput {
    value: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct UnitInput {}

#[durust::activity(name = "worker-run.double")]
async fn wr_double(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value * 2)
}

#[durust::workflow(name = "worker-run.double-plus-one", version = 1)]
async fn wr_double_plus_one(input: NumberInput) -> durust::Result<u64> {
    let doubled = durust::call_activity!(wr_double(NumberInput { value: input.value }))
        .task_queue("activities")
        .await?;
    Ok(doubled + 1)
}

#[durust::workflow(name = "worker-run.no-activity", version = 1)]
async fn wr_no_activity(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value + 1)
}

// The slow activity parks on a gate the test controls, so "slow" is a
// logical state (parked until released), not a wall-clock sleep.
fn slow_gate() -> &'static tokio::sync::Notify {
    static GATE: OnceLock<tokio::sync::Notify> = OnceLock::new();
    GATE.get_or_init(tokio::sync::Notify::new)
}

#[durust::activity(name = "worker-run.parked")]
async fn parked_activity(_: UnitInput) -> durust::Result<u64> {
    slow_gate().notified().await;
    Ok(1)
}

#[durust::workflow(name = "worker-run.parked-one", version = 1)]
async fn parked_workflow(_: UnitInput) -> durust::Result<u64> {
    durust::call_activity!(parked_activity(UnitInput {}))
        .task_queue("activities")
        .await
}

// Counts how many times the panicking workflow's body ran, so the test can
// prove the panic actually happened rather than inferring it from an empty
// history.
static PANICKING_WORKFLOW_POLLS: AtomicU64 = AtomicU64::new(0);

// A workflow whose user code panics. The panic is inside a branch so the
// handler body is not `!`-typed, matching how a real bug reaches this state.
#[durust::workflow(name = "worker-run.panicking", version = 1)]
async fn wr_panicking(input: NumberInput) -> durust::Result<u64> {
    if input.value == 0 {
        PANICKING_WORKFLOW_POLLS.fetch_add(1, Ordering::SeqCst);
        panic!("worker-run workflow panicked on purpose");
    }
    Ok(input.value)
}

// A second panicking workflow for the batched case. It carries no counter of
// its own so it cannot race `PANICKING_WORKFLOW_POLLS` with the across-passes
// test, which runs concurrently in this binary.
#[durust::workflow(name = "worker-run.panicking-batched", version = 1)]
async fn wr_panicking_batched(input: NumberInput) -> durust::Result<u64> {
    if input.value == 0 {
        panic!("worker-run batched workflow panicked on purpose");
    }
    Ok(input.value)
}

// Counts activity attempts across retries so the test can assert the retry
// policy ran the activity again after the panicking attempt.
static PANIC_ONCE_ATTEMPTS: AtomicU64 = AtomicU64::new(0);

#[durust::activity(name = "worker-run.panic-once")]
async fn panic_once_activity(_: UnitInput) -> durust::Result<u64> {
    let attempt = PANIC_ONCE_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
    if attempt == 1 {
        panic!("worker-run activity panicked on attempt 1");
    }
    Ok(attempt)
}

#[durust::workflow(name = "worker-run.panic-once-activity", version = 1)]
async fn panic_once_workflow(_: UnitInput) -> durust::Result<u64> {
    // `RetryPolicy::none().max_attempts(2)` retries without a backoff delay,
    // so the retry is visible on the memory backend's virtual clock.
    durust::call_activity!(panic_once_activity(UnitInput {}))
        .task_queue("activities")
        .retry(durust::RetryPolicy::none().max_attempts(2))
        .await
}

async fn history<B>(backend: &B, run_id: &RunId) -> Vec<durust::HistoryEvent>
where
    B: DurableBackend,
{
    backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1_000_000),
            max_events: 1_000,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events
}

async fn has_event<B>(
    backend: &B,
    run_id: &RunId,
    matches: impl Fn(&HistoryEventData) -> bool,
) -> bool
where
    B: DurableBackend,
{
    history(backend, run_id)
        .await
        .iter()
        .any(|event| matches(&event.data))
}

async fn completed_result<B>(backend: &B, run_id: &RunId) -> Option<u64>
where
    B: DurableBackend,
{
    for event in history(backend, run_id).await {
        if let HistoryEventData::WorkflowCompleted { result } = &event.data {
            return Some(durust::decode_payload::<u64>(result).unwrap());
        }
    }
    None
}

// Waits for a logical condition while the joined `run()` future keeps making
// progress; yielding (instead of sleeping) keeps the test progress-driven.
async fn wait_until<F, Fut>(mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    while !probe().await {
        tokio::task::yield_now().await;
    }
}

// The README-shaped end-to-end path on SQLite: build a worker, drive
// `run()`, complete a one-activity workflow, and stop it gracefully through
// the shutdown handle. Pins that `run()` returns Ok on shutdown.
#[test]
fn readme_shaped_worker_run_completes_workflow_on_sqlite() {
    block_on_tokio(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("worker-run.sqlite3")).unwrap();
        let client = durust::Client::new(backend.clone());
        let run_id = client
            .start_workflow::<wr_double_plus_one>(
                "wf/readme",
                "workflows",
                NumberInput { value: 20 },
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("readme-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(wr_double_plus_one)
            .register_activity(wr_double)
            .max_cached_workflows(10_000)
            .max_concurrent_activities(8)
            .idle_wait(Duration::from_millis(10))
            .build();
        let shutdown = worker.shutdown_handle();

        let (run_result, ()) = futures::future::join(worker.run(), async {
            wait_until(|| async { completed_result(&backend, &run_id).await.is_some() }).await;
            shutdown.shutdown();
        })
        .await;

        run_result.unwrap();
        assert_eq!(completed_result(&backend, &run_id).await, Some(41));
    });
}

// A parked activity must not block other claimed activities: with the
// concurrency bound above the claim count, the fast activities' completions
// reach the backend while the parked one is still pending, and releasing
// the gate completes it. Progress is purely logical (gate + history), so no
// wall-clock sleep participates in any assertion.
#[test]
fn parked_activity_does_not_block_fast_activity_completions() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let parked_run = client
            .start_workflow::<parked_workflow>("wf/parked", "workflows", UnitInput {})
            .await
            .unwrap();
        let fast_runs = [
            client
                .start_workflow::<wr_double_plus_one>(
                    "wf/fast-0",
                    "workflows",
                    NumberInput { value: 1 },
                )
                .await
                .unwrap(),
            client
                .start_workflow::<wr_double_plus_one>(
                    "wf/fast-1",
                    "workflows",
                    NumberInput { value: 2 },
                )
                .await
                .unwrap(),
            client
                .start_workflow::<wr_double_plus_one>(
                    "wf/fast-2",
                    "workflows",
                    NumberInput { value: 3 },
                )
                .await
                .unwrap(),
        ];

        let mut worker = Worker::builder(backend.clone())
            .worker_id("parked-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(parked_workflow)
            .register_workflow(wr_double_plus_one)
            .register_activity(parked_activity)
            .register_activity(wr_double)
            // One workflow pass schedules all four activities, and one
            // activity pass claims all four so they run concurrently.
            .max_concurrent_workflow_tasks(8)
            .workflow_task_prefetch_limit(8)
            .max_concurrent_activities(8)
            .build();
        let shutdown = worker.shutdown_handle();

        let (run_result, ()) = futures::future::join(worker.run(), async {
            // All three fast activity completions land while the parked
            // activity still holds the pass.
            wait_until(|| async {
                let mut all = true;
                for run in &fast_runs {
                    all &= has_event(&backend, run, |data| {
                        matches!(data, HistoryEventData::ActivityCompleted(_))
                    })
                    .await;
                }
                all
            })
            .await;
            assert!(
                !has_event(&backend, &parked_run, |data| {
                    matches!(data, HistoryEventData::ActivityCompleted(_))
                })
                .await,
                "parked activity completed before the test released it"
            );

            // `notify_one` stores a permit, so the release cannot be lost
            // even if it raced the activity's registration.
            slow_gate().notify_one();
            wait_until(|| async { completed_result(&backend, &parked_run).await.is_some() }).await;
            for run in &fast_runs {
                wait_until(|| async { completed_result(&backend, run).await.is_some() }).await;
            }
            shutdown.shutdown();
        })
        .await;

        run_result.unwrap();
        assert_eq!(completed_result(&backend, &parked_run).await, Some(1));
        assert_eq!(completed_result(&backend, &fast_runs[0]).await, Some(3));
        assert_eq!(completed_result(&backend, &fast_runs[1]).await, Some(5));
        assert_eq!(completed_result(&backend, &fast_runs[2]).await, Some(7));
    });
}

// Memory backend push wakeups: `run()` parks in `wait_for_ready` with an
// idle wait far longer than the test timeout, so the workflow can only
// complete (and shutdown can only be prompt) if `start_workflow` and
// `shutdown` actually notify the parked waiter.
#[test]
fn memory_wait_for_ready_wakes_parked_run_on_new_workflow() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());

        let mut worker = Worker::builder(backend.clone())
            .worker_id("notify-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_no_activity)
            .idle_wait(Duration::from_secs(3600))
            .build();
        let shutdown = worker.shutdown_handle();

        let (run_result, ()) = futures::future::join(worker.run(), async {
            // Let the run loop finish its empty pass and park before the
            // workflow exists.
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            let run_id = client
                .start_workflow::<wr_no_activity>(
                    "wf/notify",
                    "workflows",
                    NumberInput { value: 6 },
                )
                .await
                .unwrap();
            wait_until(|| async { completed_result(&backend, &run_id).await.is_some() }).await;
            assert_eq!(completed_result(&backend, &run_id).await, Some(7));
            shutdown.shutdown();
        })
        .await;

        run_result.unwrap();
    });
}

// A panicking workflow must fail its own task, not the worker: without the
// `catch_unwind` in `poll_cached` the panic unwinds through `Worker::run` and
// the process loses every other run. The proof is the *second* run's committed
// result — the worker claimed, polled, and committed it after the panic. The
// panicking run's claim is released with the 60 s nondeterminism backoff and
// the memory backend's clock is virtual, so it panics exactly once and commits
// nothing.
#[test]
fn panicking_workflow_fails_its_task_and_the_worker_keeps_serving() {
    block_on_tokio(async {
        PANICKING_WORKFLOW_POLLS.store(0, Ordering::SeqCst);
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let panicking_run = client
            .start_workflow::<wr_panicking>("wf/panicking", "workflows", NumberInput { value: 0 })
            .await
            .unwrap();
        let healthy_run = client
            .start_workflow::<wr_no_activity>(
                "wf/after-panic",
                "workflows",
                NumberInput { value: 41 },
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("panic-workflow-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_panicking)
            .register_workflow(wr_no_activity)
            .idle_wait(Duration::from_millis(5))
            .build();
        let shutdown = worker.shutdown_handle();

        let (run_result, ()) = futures::future::join(worker.run(), async {
            wait_until(|| async { completed_result(&backend, &healthy_run).await.is_some() }).await;
            shutdown.shutdown();
        })
        .await;

        run_result.unwrap();
        assert_eq!(
            PANICKING_WORKFLOW_POLLS.load(Ordering::SeqCst),
            1,
            "the panicking workflow must run once and be released with backoff"
        );
        assert_eq!(
            completed_result(&backend, &healthy_run).await,
            Some(42),
            "the worker must keep serving other runs after a workflow panic"
        );
        // The failed attempt committed nothing: only the start event exists,
        // and the run is neither completed nor failed, so a fixed redeploy can
        // still replay it.
        let panicking_history = history(&backend, &panicking_run).await;
        assert_eq!(panicking_history.len(), 1, "{panicking_history:?}");
        assert!(matches!(
            panicking_history[0].data,
            HistoryEventData::WorkflowStarted { .. }
        ));
        assert!(
            !has_event(&backend, &panicking_run, |data| matches!(
                data,
                HistoryEventData::WorkflowFailed { .. }
                    | HistoryEventData::WorkflowCompleted { .. }
            ))
            .await
        );
    });
}

// The production pass claims a *batch*, so a panicking task and a healthy task
// can land in the same `run_workflow_batch_once` call. The batch loop commits
// every prepared task before it checks `first_error`, so the healthy
// neighbour's commit lands even though the pass then reports the panic as its
// error. Both halves of that observed behaviour are pinned here: the commit
// that lands, which is what "the worker keeps serving" means for a batch, and
// the `Err` the pass still returns despite that progress — asserted rather
// than papered over, so a change in either direction is caught. Reporting a
// pass with committed work as failed also skips that pass's local activities,
// maintenance, and activity execution; that is loop shape, owned by Phase 2
// row 2H, not by this row.
#[test]
fn panicking_workflow_batched_with_a_healthy_task_still_commits_its_neighbor() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let panicking_run = client
            .start_workflow::<wr_panicking_batched>(
                "wf/batched-panicking",
                "workflows",
                NumberInput { value: 0 },
            )
            .await
            .unwrap();
        let healthy_run = client
            .start_workflow::<wr_no_activity>(
                "wf/batched-healthy",
                "workflows",
                NumberInput { value: 7 },
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("batched-panic-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_panicking_batched)
            .register_workflow(wr_no_activity)
            // Above one on both knobs so a single claim RPC takes both tasks
            // into one batch, the shape `run_pass_once` uses in production.
            .max_concurrent_workflow_tasks(4)
            .workflow_task_prefetch_limit(4)
            .build();

        // One batched pass over both tasks. It reports the panic...
        let err = worker.run_workflow_batch_once().await.unwrap_err();
        let durust::Error::Nondeterminism(message) = &err else {
            panic!("a batched workflow panic must fail the task, got {err:?}");
        };
        assert!(
            message.contains("workflow task panicked")
                && message.contains("worker-run batched workflow panicked on purpose"),
            "{message}"
        );

        // ...and the healthy neighbour claimed into that same batch still
        // committed its result, which no later pass could have produced.
        assert_eq!(
            completed_result(&backend, &healthy_run).await,
            Some(8),
            "a batched neighbour's commit must survive the panicking task"
        );
        // The panicking task committed nothing and left no cache entry.
        let panicking_history = history(&backend, &panicking_run).await;
        assert_eq!(panicking_history.len(), 1, "{panicking_history:?}");
        assert!(matches!(
            panicking_history[0].data,
            HistoryEventData::WorkflowStarted { .. }
        ));
        assert_eq!(worker.cached_workflow_count(), 0);

        // The worker is still usable: the panicking claim was released with the
        // 60 s backoff and the memory backend's clock is virtual, so the next
        // pass finds no claimable work and succeeds.
        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 0);
    });
}

// A panicking activity must fail its own attempt and let the retry policy
// decide: attempt 1 panics, the caught panic becomes a retryable activity
// failure, and attempt 2 completes the run. Concurrency is above one so the
// execution runs in the `FuturesUnordered` batch path, where an uncaught panic
// would also drop the concurrently claimed activities.
#[test]
fn panicking_activity_fails_its_task_and_the_retry_policy_completes_the_run() {
    block_on_tokio(async {
        PANIC_ONCE_ATTEMPTS.store(0, Ordering::SeqCst);
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let panicking_run = client
            .start_workflow::<panic_once_workflow>("wf/panic-activity", "workflows", UnitInput {})
            .await
            .unwrap();
        let healthy_run = client
            .start_workflow::<wr_double_plus_one>(
                "wf/activity-after-panic",
                "workflows",
                NumberInput { value: 10 },
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("panic-activity-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(panic_once_workflow)
            .register_workflow(wr_double_plus_one)
            .register_activity(panic_once_activity)
            .register_activity(wr_double)
            .max_concurrent_workflow_tasks(4)
            .workflow_task_prefetch_limit(4)
            .max_concurrent_activities(4)
            .idle_wait(Duration::from_millis(5))
            .build();
        let shutdown = worker.shutdown_handle();

        let (run_result, ()) = futures::future::join(worker.run(), async {
            wait_until(|| async {
                completed_result(&backend, &panicking_run).await.is_some()
                    && completed_result(&backend, &healthy_run).await.is_some()
            })
            .await;
            shutdown.shutdown();
        })
        .await;

        run_result.unwrap();
        assert_eq!(
            PANIC_ONCE_ATTEMPTS.load(Ordering::SeqCst),
            2,
            "the retry policy must run the activity again after the panicking attempt"
        );
        assert_eq!(completed_result(&backend, &panicking_run).await, Some(2));
        assert_eq!(
            completed_result(&backend, &healthy_run).await,
            Some(21),
            "the worker must keep serving other runs after an activity panic"
        );
        // A retried attempt is not a workflow-visible failure.
        assert!(
            !has_event(&backend, &panicking_run, |data| matches!(
                data,
                HistoryEventData::ActivityFailed(_)
            ))
            .await
        );
    });
}

// SQLite keeps the default `wait_for_ready` (a bounded sleep): work started
// while `run()` is parked still completes after the sleep expires.
#[test]
fn sqlite_run_completes_work_via_default_sleep_wait() {
    block_on_tokio(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("worker-sleep.sqlite3")).unwrap();
        let client = durust::Client::new(backend.clone());

        let mut worker = Worker::builder(backend.clone())
            .worker_id("sleep-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_no_activity)
            .idle_wait(Duration::from_millis(5))
            .build();
        let shutdown = worker.shutdown_handle();

        let (run_result, ()) = futures::future::join(worker.run(), async {
            // Park the loop first so completion must come from a wake out of
            // the default sleep, not from the initial pass.
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            let run_id = client
                .start_workflow::<wr_no_activity>(
                    "wf/sleepy",
                    "workflows",
                    NumberInput { value: 41 },
                )
                .await
                .unwrap();
            wait_until(|| async { completed_result(&backend, &run_id).await.is_some() }).await;
            assert_eq!(completed_result(&backend, &run_id).await, Some(42));
            shutdown.shutdown();
        })
        .await;

        run_result.unwrap();
    });
}
