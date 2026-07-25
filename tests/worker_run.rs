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

// A third panicking workflow, with its own counter, for the run-loop test that
// needs more consecutive failing passes than `Worker::run` tolerates. It cannot
// share a counter with the tests above, which run concurrently in this binary.
static MANY_PANICKING_WORKFLOW_POLLS: AtomicU64 = AtomicU64::new(0);

#[durust::workflow(name = "worker-run.panicking-many", version = 1)]
async fn wr_panicking_many(input: NumberInput) -> durust::Result<u64> {
    if input.value == 0 {
        MANY_PANICKING_WORKFLOW_POLLS.fetch_add(1, Ordering::SeqCst);
        panic!("worker-run many-panics workflow panicked on purpose");
    }
    Ok(input.value)
}

// The two zero-backoff tests each assert an exact poll count, so each needs its
// own workflow type and its own counter: every test in this binary runs
// concurrently, and a shared counter would let one test's `store(0)` and polls
// race the other's assertion.
static ZERO_BACKOFF_IDLE_POLLS: AtomicU64 = AtomicU64::new(0);
static ZERO_BACKOFF_RUN_POLLS: AtomicU64 = AtomicU64::new(0);

#[durust::workflow(name = "worker-run.panicking-zero-backoff-idle", version = 1)]
async fn wr_panicking_zero_backoff_idle(input: NumberInput) -> durust::Result<u64> {
    if input.value == 0 {
        ZERO_BACKOFF_IDLE_POLLS.fetch_add(1, Ordering::SeqCst);
        panic!("worker-run zero-backoff idle workflow panicked on purpose");
    }
    Ok(input.value)
}

#[durust::workflow(name = "worker-run.panicking-zero-backoff-run", version = 1)]
async fn wr_panicking_zero_backoff_run(input: NumberInput) -> durust::Result<u64> {
    if input.value == 0 {
        ZERO_BACKOFF_RUN_POLLS.fetch_add(1, Ordering::SeqCst);
        panic!("worker-run zero-backoff run workflow panicked on purpose");
    }
    Ok(input.value)
}

// Parks on a timer, so its run has work only the pass's maintenance stage can
// do.
#[durust::workflow(name = "worker-run.sleeper", version = 1)]
async fn wr_sleeper(input: NumberInput) -> durust::Result<u64> {
    durust::sleep(Duration::from_millis(10)).await?;
    Ok(input.value + 1)
}

#[durust::workflow(name = "worker-run.child-leaf", version = 1)]
async fn wr_child_leaf(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value + 100)
}

// Parks on a child start, so its run has work only the pass's child dispatch
// stage can do.
#[durust::workflow(name = "worker-run.parent-waits-child", version = 1)]
async fn wr_parent_waits_child(input: NumberInput) -> durust::Result<u64> {
    let child = durust::child!(wr_child_leaf(NumberInput { value: input.value }))
        .workflow_id("wf/pass-isolation-child")
        .spawn()
        .await?;
    child.result().await
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
// every prepared task, and the panicking task is settled by the batch itself —
// nothing committed, claim released with the retry backoff — so it is not the
// batch's error. The batch therefore reports the neighbour's commit rather than
// discarding it, which is what "the worker keeps serving" means for a batch.
//
// This assertion was `unwrap_err()` when Phase 1 landed, pinning the observed
// short-circuit; Phase 2 row 2H is the row that fixes exactly that, so the
// assertion moves to the committed count the batch now reports.
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

        // One batched pass over both tasks. The panicking task fails without
        // failing the batch...
        let committed = worker.run_workflow_batch_once().await.unwrap();
        assert_eq!(
            committed, 1,
            "the batch must report its healthy neighbour's commit, not discard it"
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

// A failed workflow task must not take the rest of its pass down with it. The
// pass runs workflow tasks, then local activities, then maintenance, then child
// dispatch, then activity execution; a `?` on the workflow stage skipped every
// later stage for every other run on the worker.
//
// Each healthy run is parked on exactly one of those stages before the
// panicking run is even started, and the assertions bracket a *single* pass
// (`max_iterations: 1`, which runs one pass and then reports the iteration
// bound), so nothing here can be satisfied by a later pass.
#[test]
fn panicking_workflow_task_does_not_suppress_the_rest_of_its_pass() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let sleeper_run = client
            .start_workflow::<wr_sleeper>("wf/pass-sleeper", "workflows", NumberInput { value: 5 })
            .await
            .unwrap();
        let parent_run = client
            .start_workflow::<wr_parent_waits_child>(
                "wf/pass-parent",
                "workflows",
                NumberInput { value: 1 },
            )
            .await
            .unwrap();
        let activity_run = client
            .start_workflow::<wr_double_plus_one>(
                "wf/pass-activity",
                "workflows",
                NumberInput { value: 3 },
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("pass-isolation-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(wr_sleeper)
            .register_workflow(wr_parent_waits_child)
            .register_workflow(wr_child_leaf)
            .register_workflow(wr_double_plus_one)
            .register_workflow(wr_panicking_batched)
            .register_activity(wr_double)
            .max_concurrent_workflow_tasks(4)
            .workflow_task_prefetch_limit(4)
            .build();

        // Park each healthy run on the stage that owns its pending work: a
        // timer for maintenance, an undispatched child start for child
        // dispatch, and a scheduled activity for activity execution.
        for _ in 0..3 {
            assert!(worker.run_workflow_once().await.unwrap());
        }
        backend.advance_time(Duration::from_millis(50));

        // None of the three stages has run yet, so each assertion below is a
        // strict before/after over one pass.
        assert!(
            !has_event(&backend, &sleeper_run, |data| matches!(
                data,
                HistoryEventData::TimerFired(_)
            ))
            .await
        );
        assert!(
            !has_event(&backend, &parent_run, |data| matches!(
                data,
                HistoryEventData::ChildWorkflowStarted(_)
            ))
            .await
        );
        assert!(
            !has_event(&backend, &activity_run, |data| matches!(
                data,
                HistoryEventData::ActivityCompleted(_)
            ))
            .await
        );

        // The only claimable workflow task for the next pass: every healthy run
        // is blocked on work a later stage of that same pass performs.
        let panicking_run = client
            .start_workflow::<wr_panicking_batched>(
                "wf/pass-panicking",
                "workflows",
                NumberInput { value: 0 },
            )
            .await
            .unwrap();

        let err = worker
            .run_until_idle_with(durust::WorkerRunOptions { max_iterations: 1 })
            .await
            .unwrap_err();
        assert!(
            matches!(&err, durust::Error::Backend(message) if message.contains("did not become idle")),
            "the pass must run every stage and stop only at the iteration bound, got {err:?}"
        );

        // The failed workflow task did not suppress maintenance...
        assert!(
            has_event(&backend, &sleeper_run, |data| matches!(
                data,
                HistoryEventData::TimerFired(_)
            ))
            .await,
            "a failed workflow task suppressed the pass's maintenance stage"
        );
        // ...child dispatch...
        assert!(
            has_event(&backend, &parent_run, |data| matches!(
                data,
                HistoryEventData::ChildWorkflowStarted(_)
            ))
            .await,
            "a failed workflow task suppressed the pass's child dispatch stage"
        );
        // ...or activity execution.
        assert!(
            has_event(&backend, &activity_run, |data| matches!(
                data,
                HistoryEventData::ActivityCompleted(_)
            ))
            .await,
            "a failed workflow task suppressed the pass's activity stage"
        );
        // The panicking task itself still committed nothing.
        assert_eq!(history(&backend, &panicking_run).await.len(), 1);

        // The worker is unharmed: every healthy run finishes.
        worker.run_until_idle().await.unwrap();
        assert_eq!(completed_result(&backend, &sleeper_run).await, Some(6));
        assert_eq!(completed_result(&backend, &parent_run).await, Some(101));
        assert_eq!(completed_result(&backend, &activity_run).await, Some(7));
    });
}

// The pass's own accounting: a batch holding a panicking task and a healthy
// task must record the healthy task as committed work, not discard it with the
// error. Exactly two iterations are allowed, so the recorded task and the
// recorded failure can only have come from the same (first) pass — the second
// pass finds the panicking claim released with the retry backoff and nothing
// else to do.
#[test]
fn panicking_workflow_batch_records_its_neighbors_committed_task_in_pass_stats() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let panicking_run = client
            .start_workflow::<wr_panicking_batched>(
                "wf/stats-panicking",
                "workflows",
                NumberInput { value: 0 },
            )
            .await
            .unwrap();
        let healthy_run = client
            .start_workflow::<wr_no_activity>(
                "wf/stats-healthy",
                "workflows",
                NumberInput { value: 7 },
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("stats-panic-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_panicking_batched)
            .register_workflow(wr_no_activity)
            .max_concurrent_workflow_tasks(4)
            .workflow_task_prefetch_limit(4)
            .build();

        let stats = worker
            .run_until_idle_with(durust::WorkerRunOptions { max_iterations: 2 })
            .await
            .unwrap();
        assert_eq!(
            stats.workflow_tasks, 1,
            "the healthy neighbour's committed task must be recorded, got {stats:?}"
        );
        assert_eq!(
            stats.workflow_tasks_failed, 1,
            "the panicking task must be recorded as failed rather than swallowed, got {stats:?}"
        );
        assert_eq!(completed_result(&backend, &healthy_run).await, Some(8));
        assert_eq!(history(&backend, &panicking_run).await.len(), 1);
    });
}

// The local-activity drain sits at the tail of the workflow stage, so a batch
// that reported a per-task fault as its own error skipped it for every task in
// the batch. This asserts the drain from inside a single
// `run_workflow_batch_once` call: no activity stage has run yet, so the only
// thing that can have completed the neighbour's activity is that drain.
#[test]
fn panicking_workflow_batch_still_runs_its_neighbors_local_activities() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let panicking_run = client
            .start_workflow::<wr_panicking_batched>(
                "wf/local-panicking",
                "workflows",
                NumberInput { value: 0 },
            )
            .await
            .unwrap();
        let activity_run = client
            .start_workflow::<wr_double_plus_one>(
                "wf/local-activity",
                "workflows",
                NumberInput { value: 4 },
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("local-activity-panic-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(wr_panicking_batched)
            .register_workflow(wr_double_plus_one)
            .register_activity(wr_double)
            .max_concurrent_workflow_tasks(4)
            .workflow_task_prefetch_limit(4)
            .max_local_activities_per_workflow_task(2)
            .build();

        let committed = worker.run_workflow_batch_once().await.unwrap();
        assert_eq!(committed, 1);
        assert!(
            has_event(&backend, &activity_run, |data| matches!(
                data,
                HistoryEventData::ActivityCompleted(_)
            ))
            .await,
            "the panicking task suppressed its batch's local activity drain"
        );
        assert_eq!(history(&backend, &panicking_run).await.len(), 1);

        worker.run_until_idle().await.unwrap();
        assert_eq!(completed_result(&backend, &activity_run).await, Some(9));
    });
}

// `run_until_idle` must drain the queue rather than abort on the first bad
// workflow, and must not call itself idle while the failed task's neighbours
// are still queued. The panicking run is started first, so it holds the single
// workflow claim of the first pass by itself: that pass commits nothing, and
// only a driver that treats the consumed claim as work to follow up reaches the
// healthy run at all.
#[test]
fn run_until_idle_drains_the_queue_behind_a_panicking_workflow() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let panicking_run = client
            .start_workflow::<wr_panicking_batched>(
                "wf/idle-panicking",
                "workflows",
                NumberInput { value: 0 },
            )
            .await
            .unwrap();
        let healthy_run = client
            .start_workflow::<wr_no_activity>(
                "wf/idle-healthy",
                "workflows",
                NumberInput { value: 11 },
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("idle-panic-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_panicking_batched)
            .register_workflow(wr_no_activity)
            .build();

        // Terminates normally: the failed claim is released with the 60 s
        // retry backoff against a virtual clock the idle loop never advances,
        // so it cannot be re-claimed into a spin, and the loop goes idle
        // instead of exhausting `max_iterations`.
        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 1, "{stats:?}");
        assert_eq!(stats.workflow_tasks_failed, 1, "{stats:?}");
        assert_eq!(
            completed_result(&backend, &healthy_run).await,
            Some(12),
            "the idle loop stopped before draining the run behind the panicking one"
        );
        // The poisoned run is left replayable, not failed.
        assert_eq!(history(&backend, &panicking_run).await.len(), 1);
    });
}

// A failed workflow task is reported as pass progress, so `Worker::run` skips
// its idle wait after one, and the release delay is the only thing bounding how
// fast a permanently poisoned run is retried. A zero backoff would therefore
// make the workflow stage re-claim the same poisoned run every pass forever.
// The builder clamps the knob for exactly that reason, mirroring `idle_wait` and
// both lease durations.
//
// Bound proven, not assumed: the run is polled once, and the idle loop reaches
// idle well inside its iteration budget instead of exhausting it.
#[test]
fn zero_nondeterminism_backoff_is_clamped_so_the_idle_loop_still_terminates() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let panicking_run = client
            .start_workflow::<wr_panicking_zero_backoff_idle>(
                "wf/zero-backoff-idle",
                "workflows",
                NumberInput { value: 0 },
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("zero-backoff-idle-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_panicking_zero_backoff_idle)
            .nondeterminism_retry_backoff(Duration::ZERO)
            .build();

        let stats = worker
            .run_until_idle_with(durust::WorkerRunOptions { max_iterations: 8 })
            .await
            .unwrap();
        assert_eq!(stats.workflow_tasks_failed, 1, "{stats:?}");
        assert_eq!(stats.workflow_tasks, 0, "{stats:?}");
        assert_eq!(
            ZERO_BACKOFF_IDLE_POLLS.load(Ordering::SeqCst),
            1,
            "an unclamped zero backoff re-claims the poisoned run every iteration"
        );
        // Still replayable, never failed.
        assert_eq!(history(&backend, &panicking_run).await.len(), 1);
    });
}

// The other half of the same bound, and the one that matters in production: with
// an unclamped zero backoff every pass claims the poisoned run, so every pass
// reports progress, so `Worker::run` never awaits anything pending. On a
// current-thread runtime that starves the sibling task holding the shutdown
// signal, and the loop becomes unstoppable rather than merely hot. This test
// fails by hanging, which is precisely the reported symptom.
#[test]
fn zero_nondeterminism_backoff_keeps_the_run_loop_responsive_to_shutdown() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        client
            .start_workflow::<wr_panicking_zero_backoff_run>(
                "wf/zero-backoff-run",
                "workflows",
                NumberInput { value: 0 },
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("zero-backoff-run-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_panicking_zero_backoff_run)
            .nondeterminism_retry_backoff(Duration::ZERO)
            .idle_wait(Duration::from_millis(5))
            .build();
        let shutdown = worker.shutdown_handle();

        let (run_result, ()) = futures::future::join(worker.run(), async {
            wait_until(|| async { ZERO_BACKOFF_RUN_POLLS.load(Ordering::SeqCst) >= 1 }).await;
            shutdown.shutdown();
        })
        .await;

        run_result.expect("a poisoned run must not wedge the production loop");
        assert_eq!(
            ZERO_BACKOFF_RUN_POLLS.load(Ordering::SeqCst),
            1,
            "the clamped backoff must keep the poisoned run out of the claim queue"
        );
    });
}

// A workflow panic is `Error::TaskPanic`, not `Error::Nondeterminism`: routed
// identically (nothing committed, claim released with the retry backoff) but
// distinguishable, because "this build has a bug" and "this history diverged
// from this build" need different operator responses. Genuine divergence is
// still `Nondeterminism` — `tests/replay_core.rs` pins that separately.
#[test]
fn workflow_panic_is_task_panic_and_commits_no_terminal_event() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let run_id = client
            .start_workflow::<wr_panicking_batched>(
                "wf/task-panic-variant",
                "workflows",
                NumberInput { value: 0 },
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("task-panic-variant-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_panicking_batched)
            .build();

        let err = worker.run_workflow_once().await.unwrap_err();
        assert!(
            !matches!(err, durust::Error::Nondeterminism(_)),
            "a panic must be distinguishable from history divergence, got {err:?}"
        );
        let durust::Error::TaskPanic(message) = &err else {
            panic!("a workflow panic must be Error::TaskPanic, got {err:?}");
        };
        // Stable contract prefix: greppable and countable without matching on
        // the variant.
        assert!(
            message.starts_with("workflow task panicked:")
                && message.contains("worker-run batched workflow panicked on purpose"),
            "{message}"
        );

        // Routed like `Nondeterminism` where routing matters: non-retryable
        // durable failure, and non-committing.
        let failure = err.durable_failure();
        assert_eq!(failure.error_type, "durust.task_panic");
        assert!(failure.non_retryable);
        assert!(failure.message.starts_with("workflow task panicked:"));

        let history = history(&backend, &run_id).await;
        assert_eq!(
            history.len(),
            1,
            "a task panic must commit nothing: {history:?}"
        );
        assert!(matches!(
            history[0].data,
            HistoryEventData::WorkflowStarted { .. }
        ));
        assert!(
            !has_event(&backend, &run_id, |data| matches!(
                data,
                HistoryEventData::WorkflowFailed { .. }
                    | HistoryEventData::WorkflowCompleted { .. }
            ))
            .await,
            "a task panic must not commit a terminal event"
        );
        assert_eq!(worker.cached_workflow_count(), 0);
    });
}

// `Worker::run` surfaces the last error after
// `MAX_CONSECUTIVE_RUN_PASS_FAILURES` (16) consecutive failing passes, so a
// dead backend fails loudly. A workflow whose task failed and released itself
// is not that: with one poisoned run per pass, more than 16 such passes used to
// exit the loop entirely and take the whole worker down. Twenty panicking runs
// drive well past the cap before any healthy work exists.
#[test]
fn panicking_workflow_passes_do_not_trip_the_consecutive_failure_cap() {
    block_on_tokio(async {
        const PANICKING_RUNS: u64 = 20;
        MANY_PANICKING_WORKFLOW_POLLS.store(0, Ordering::SeqCst);
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        for index in 0..PANICKING_RUNS {
            client
                .start_workflow::<wr_panicking_many>(
                    format!("wf/many-panics-{index}"),
                    "workflows",
                    NumberInput { value: 0 },
                )
                .await
                .unwrap();
        }

        let mut worker = Worker::builder(backend.clone())
            .worker_id("many-panics-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_panicking_many)
            .register_workflow(wr_no_activity)
            .idle_wait(Duration::from_millis(5))
            .build();
        let shutdown = worker.shutdown_handle();

        // Lets the waiter stop as soon as `run` returns, so a worker that dies
        // early fails on its returned error instead of hanging the test.
        let run_finished = std::sync::atomic::AtomicBool::new(false);
        let (run_result, ()) = futures::future::join(
            async {
                let result = worker.run().await;
                run_finished.store(true, Ordering::SeqCst);
                result
            },
            async {
                // Every panicking run is claimed and failed before any healthy
                // work exists, so the failing passes are strictly consecutive.
                wait_until(|| async {
                    run_finished.load(Ordering::SeqCst)
                        || MANY_PANICKING_WORKFLOW_POLLS.load(Ordering::SeqCst) >= PANICKING_RUNS
                })
                .await;
                let healthy_run = client
                    .start_workflow::<wr_no_activity>(
                        "wf/many-panics-healthy",
                        "workflows",
                        NumberInput { value: 41 },
                    )
                    .await
                    .unwrap();
                wait_until(|| async {
                    run_finished.load(Ordering::SeqCst)
                        || completed_result(&backend, &healthy_run).await.is_some()
                })
                .await;
                assert_eq!(
                    completed_result(&backend, &healthy_run).await,
                    Some(42),
                    "the worker died before serving work queued behind the panicking runs"
                );
                shutdown.shutdown();
            },
        )
        .await;

        run_result.expect("passes that only failed their own task must not kill the worker");
        assert_eq!(
            MANY_PANICKING_WORKFLOW_POLLS.load(Ordering::SeqCst),
            PANICKING_RUNS,
            "each poisoned run must be polled exactly once and then backed off"
        );
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
