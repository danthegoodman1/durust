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
