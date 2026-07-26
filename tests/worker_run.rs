// Integration tests for the production `Worker::run` loop: graceful
// shutdown, `wait_for_ready` wakeups, and concurrent activity execution.
// Each test drives `run()` on a single-threaded tokio runtime by joining it
// with the test logic, so no spawning or wall-clock coordination is needed,
// and every test is bounded by a timeout so a hang fails fast.

use durust::{
    DurableBackend, EventId, HistoryEventData, MemoryBackend, RunId, SqliteBackend, Worker,
    WorkerRunOptions,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);
// Bound for a single logical step inside a test, well inside `TEST_TIMEOUT`, so
// a worker that stops making progress fails on the assertion that names the
// property rather than on the harness timeout.
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);

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

// The cross-boundary slow activity: it announces that it is running, then
// parks on a gate the test owns. "Multi-second" is a logical state — parked
// until released — so nothing here waits on a clock.
fn cross_boundary_gate() -> &'static tokio::sync::Notify {
    static GATE: OnceLock<tokio::sync::Notify> = OnceLock::new();
    GATE.get_or_init(tokio::sync::Notify::new)
}

static CROSS_BOUNDARY_ACTIVITY_RUNNING: AtomicBool = AtomicBool::new(false);

#[durust::activity(name = "worker-run.cross-boundary-parked")]
async fn cross_boundary_activity(_: UnitInput) -> durust::Result<u64> {
    CROSS_BOUNDARY_ACTIVITY_RUNNING.store(true, Ordering::SeqCst);
    cross_boundary_gate().notified().await;
    Ok(1)
}

#[durust::workflow(name = "worker-run.cross-boundary", version = 1)]
async fn cross_boundary_workflow(_: UnitInput) -> durust::Result<u64> {
    durust::call_activity!(cross_boundary_activity(UnitInput {}))
        .task_queue("activities")
        .await
}

// A recording/replay pair under one workflow type: the recording sleeps twice,
// the replacement sleeps once, so replaying the recorded history against the
// replacement leaves a `TimerStarted` command event unreplayed. That is genuine
// history divergence, which must never be counted as a panic.
#[durust::workflow(name = "worker-run.timer-count-change", version = 1)]
async fn wr_two_timers(_: UnitInput) -> durust::Result<u64> {
    durust::sleep(Duration::from_millis(1)).await?;
    durust::sleep(Duration::from_millis(1)).await?;
    Ok(2)
}

#[durust::workflow(name = "worker-run.timer-count-change", version = 1)]
async fn wr_one_timer(_: UnitInput) -> durust::Result<u64> {
    durust::sleep(Duration::from_millis(1)).await?;
    Ok(1)
}

// The saturation pair. The workflow loop is handed a backlog far longer than
// its yield cadence, and the activity records how many workflow tasks had
// committed by the time it ran — which is the whole observable difference
// between a loop that hands its peers a turn and one that does not.
static SATURATION_COMMITS: AtomicU64 = AtomicU64::new(0);
static SATURATION_ACTIVITY_AT_COMMIT: AtomicU64 = AtomicU64::new(0);

#[durust::activity(name = "worker-run.saturation-probe")]
async fn saturation_activity(_: UnitInput) -> durust::Result<u64> {
    SATURATION_ACTIVITY_AT_COMMIT
        .store(SATURATION_COMMITS.load(Ordering::SeqCst), Ordering::SeqCst);
    Ok(1)
}

#[durust::workflow(name = "worker-run.saturation-activity", version = 1)]
async fn saturation_workflow(_: UnitInput) -> durust::Result<u64> {
    durust::call_activity!(saturation_activity(UnitInput {}))
        .task_queue("activities")
        .await
}

#[durust::workflow(name = "worker-run.saturation-filler", version = 1)]
async fn saturation_filler(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value + 1)
}

// A parent that must start a child through the outbox, for the maintenance
// opt-out test. It carries its own child workflow id so it cannot collide with
// the pass-isolation test's, which runs concurrently in this binary.
#[durust::workflow(name = "worker-run.optout-child", version = 1)]
async fn wr_optout_child(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value + 100)
}

#[durust::workflow(name = "worker-run.optout-parent", version = 1)]
async fn wr_optout_parent(input: NumberInput) -> durust::Result<u64> {
    let child = durust::child!(wr_optout_child(NumberInput { value: input.value }))
        .workflow_id("wf/maintenance-optout-child")
        .spawn()
        .await?;
    child.result().await
}

// The same shape again, for the *interval loop's* half of the opt-out
// guarantee. It needs its own workflow types and its own child workflow id
// because the pass-driver test above runs concurrently in this binary and both
// would otherwise contend for one child id.
#[durust::workflow(name = "worker-run.optout-loop-child", version = 1)]
async fn wr_optout_loop_child(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value + 200)
}

#[durust::workflow(name = "worker-run.optout-loop-parent", version = 1)]
async fn wr_optout_loop_parent(input: NumberInput) -> durust::Result<u64> {
    let child = durust::child!(wr_optout_loop_child(NumberInput { value: input.value }))
        .workflow_id("wf/maintenance-optout-loop-child")
        .spawn()
        .await?;
    child.result().await
}

// A durable API called from inside a `side_effect` closure. The re-entrancy
// guard reports by panicking, so this is a workflow *bug* even though its
// message says nothing about a panic in user code.
#[durust::workflow(name = "worker-run.reentrant-side-effect", version = 1)]
async fn wr_reentrant_side_effect(_: UnitInput) -> durust::Result<bool> {
    let flag: bool = durust::side_effect("worker-run-nested-call", || {
        durust::patched("worker-run-nested-change").unwrap_or(false)
    })
    .await?;
    Ok(flag)
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

// Counts maintenance scans and can make workflow claims fail, so the loop
// tests can observe cadence and stage isolation from outside the worker.
// Everything else forwards to the inner backend — including `wait_for_ready`,
// so the memory provider's push wakeups keep working and the tests stay
// progress-driven rather than sleep-driven.
#[derive(Clone)]
struct ObservingBackend {
    inner: MemoryBackend,
    maintenance_calls: Arc<AtomicUsize>,
    fail_workflow_claims: Arc<AtomicBool>,
}

impl ObservingBackend {
    fn new(inner: MemoryBackend) -> Self {
        Self {
            inner,
            maintenance_calls: Arc::new(AtomicUsize::new(0)),
            fail_workflow_claims: Arc::new(AtomicBool::new(false)),
        }
    }

    fn maintenance_calls(&self) -> usize {
        self.maintenance_calls.load(Ordering::SeqCst)
    }

    fn fail_workflow_claims(&self, failing: bool) {
        self.fail_workflow_claims.store(failing, Ordering::SeqCst);
    }

    fn workflow_claim_failure(&self) -> Option<durust::Error> {
        self.fail_workflow_claims
            .load(Ordering::SeqCst)
            .then(|| durust::Error::Backend("injected workflow claim failure".to_owned()))
    }
}

macro_rules! forward_to_inner {
    ($(fn $method:ident($($arg:ident: $arg_ty:ty),*) -> $out:ty;)*) => {
        $(
            fn $method(&self, $($arg: $arg_ty),*) -> BoxFuture<'static, durust::Result<$out>> {
                self.inner.$method($($arg),*)
            }
        )*
    };
}

impl DurableBackend for ObservingBackend {
    fn payload_storage_config(&self) -> durust::PayloadStorageConfig {
        self.inner.payload_storage_config()
    }

    fn wait_for_ready(
        &self,
        req: durust::WaitForReadyRequest,
    ) -> BoxFuture<'static, durust::Result<()>> {
        self.inner.wait_for_ready(req)
    }

    fn claim_workflow_task(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::ClaimedWorkflowTask>>> {
        if let Some(err) = self.workflow_claim_failure() {
            return Box::pin(async move { Err(err) });
        }
        self.inner.claim_workflow_task(worker_id, opts)
    }

    fn claim_workflow_tasks(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::ClaimWorkflowTasksOptions,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::ClaimedWorkflowTask>>> {
        if let Some(err) = self.workflow_claim_failure() {
            return Box::pin(async move { Err(err) });
        }
        self.inner.claim_workflow_tasks(worker_id, opts)
    }

    fn run_due_maintenance(
        &self,
        req: durust::RunDueMaintenanceRequest,
    ) -> BoxFuture<'static, durust::Result<durust::RunDueMaintenanceOutcome>> {
        self.maintenance_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.run_due_maintenance(req)
    }

    forward_to_inner! {
        fn start_workflow(req: durust::StartWorkflowRequest) -> durust::StartWorkflowOutcome;
        fn cancel_workflow(req: durust::CancelWorkflowRequest) -> durust::CancelWorkflowOutcome;
        fn current_time() -> durust::TimestampMs;
        fn stream_history(req: durust::StreamHistoryRequest) -> durust::HistoryChunk;
        fn stream_history_for_replay(req: durust::StreamHistoryRequest) -> durust::HistoryChunk;
        fn hydrate_payload(payload: durust::PayloadRef) -> durust::PayloadRef;
        fn hydrate_activity_map_result_manifest(
            payload: durust::PayloadRef
        ) -> durust::PayloadRef;
        fn hydrate_child_workflow_map_result_manifest(
            payload: durust::PayloadRef
        ) -> durust::PayloadRef;
        fn commit_workflow_task(
            claim: durust::WorkflowTaskClaim,
            commit: durust::WorkflowTaskCommit
        ) -> durust::CommitOutcome;
        fn commit_workflow_tasks(
            batch: durust::WorkflowTaskCommitBatch
        ) -> Vec<durust::WorkflowTaskCommitBatchResult>;
        fn release_workflow_task(
            claim: durust::WorkflowTaskClaim,
            release: durust::WorkflowTaskRelease
        ) -> ();
        fn signal_workflow(req: durust::SignalWorkflowRequest) -> durust::SignalWorkflowOutcome;
        fn read_signal_inbox(
            req: durust::ReadSignalInboxRequest
        ) -> Option<durust::SignalInboxRecord>;
        fn read_signal_inboxes(
            req: durust::ReadSignalInboxesRequest
        ) -> Vec<Option<durust::SignalInboxRecord>>;
        fn fire_due_timers(req: durust::FireDueTimersRequest) -> durust::FireDueTimersOutcome;
        fn timeout_due_activities(
            req: durust::TimeoutDueActivitiesRequest
        ) -> durust::TimeoutDueActivitiesOutcome;
        fn claim_activity_task(
            worker_id: durust::WorkerId,
            opts: durust::ClaimActivityOptions
        ) -> Option<durust::ClaimedActivityTask>;
        fn claim_activity_tasks(
            worker_id: durust::WorkerId,
            opts: durust::ClaimActivityTasksOptions
        ) -> Vec<durust::ClaimedActivityTask>;
        fn heartbeat_activity(
            req: durust::ActivityHeartbeatRequest
        ) -> durust::ActivityHeartbeatOutcome;
        fn complete_activity(
            req: durust::CompleteActivityRequest
        ) -> durust::CompleteActivityOutcome;
        fn complete_activity_tasks(
            req: durust::CompleteActivityTasksRequest
        ) -> Vec<durust::CompleteActivityTaskBatchResult>;
        fn fail_activity(req: durust::FailActivityRequest) -> durust::FailActivityOutcome;
        fn dispatch_child_workflow_starts(
            req: durust::DispatchChildWorkflowStartsRequest
        ) -> durust::DispatchChildWorkflowStartsOutcome;
        fn query_projection(req: durust::QueryProjectionRequest) -> durust::QueryProjectionOutcome;
        fn workflow_change_versions(
            req: durust::WorkflowChangeVersionsRequest
        ) -> durust::WorkflowChangeVersionsOutcome;
        fn payload_roots() -> durust::PayloadRootsOutcome;
        fn gc_payload_blobs(
            req: durust::PayloadGarbageCollectionRequest
        ) -> durust::PayloadGarbageCollectionOutcome;
    }
}

// Collects the worker's event sink into a shared log so a test can assert on
// individual occurrences, not just totals. Events borrow, so the sink records
// the shape it needs rather than the event.
#[derive(Clone, Debug, PartialEq, Eq)]
enum RecordedEvent {
    WorkflowTaskCommitted(String),
    WorkflowTaskFailed { run_id: String, error: String },
    WorkflowTaskDeferred(String),
    LoopError { worker_loop: String, error: String },
}

#[derive(Clone, Default)]
struct EventLog(Arc<Mutex<Vec<RecordedEvent>>>);

impl EventLog {
    fn sink(&self) -> impl Fn(durust::WorkerEvent<'_>) + Send + Sync + 'static {
        let events = Arc::clone(&self.0);
        move |event| {
            let recorded = match event {
                durust::WorkerEvent::WorkflowTaskCommitted { run_id } => {
                    Some(RecordedEvent::WorkflowTaskCommitted(run_id.to_string()))
                }
                durust::WorkerEvent::WorkflowTaskFailed { run_id, error } => {
                    Some(RecordedEvent::WorkflowTaskFailed {
                        run_id: run_id.to_string(),
                        error: error.to_string(),
                    })
                }
                durust::WorkerEvent::WorkflowTaskDeferred { run_id } => {
                    Some(RecordedEvent::WorkflowTaskDeferred(run_id.to_string()))
                }
                durust::WorkerEvent::LoopError { worker_loop, error } => {
                    Some(RecordedEvent::LoopError {
                        worker_loop: format!("{worker_loop:?}"),
                        error: error.to_string(),
                    })
                }
                _ => None,
            };
            if let Some(recorded) = recorded {
                events.lock().unwrap().push(recorded);
            }
        }
    }

    fn snapshot(&self) -> Vec<RecordedEvent> {
        self.0.lock().unwrap().clone()
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

// No number of failing passes exits `Worker::run`: a loop catches what its
// stage raised, backs off, and keeps going. The consecutive-failure cap this
// replaced could only ever have fired on a worker that was still serving work,
// and its error went to whatever spawned `run()`, which discards it. Twenty
// panicking runs drive well past the old cap of 16 before any healthy work
// exists, and the worker must still be serving when it arrives.
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

// 2A: the coupling this phase removes. `run_pass_once` ran workflow tasks, then
// activities, in sequence, and the activity stage drained every claimed
// execution before returning — so one long-running activity held the whole
// worker, workflow commits included. Under the split loops the workflow loop
// commits a different run's task while the activity is still parked.
//
// Progress is purely logical: the activity parks on a gate the test releases,
// and every assertion reads history or that gate. No wall-clock sleep
// participates in any of them.
#[test]
fn workflow_task_commits_while_a_multi_second_activity_is_in_flight() {
    block_on_tokio(async {
        CROSS_BOUNDARY_ACTIVITY_RUNNING.store(false, Ordering::SeqCst);
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let parked_run = client
            .start_workflow::<cross_boundary_workflow>(
                "wf/cross-boundary-parked",
                "workflows",
                UnitInput {},
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("cross-boundary-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(cross_boundary_workflow)
            .register_workflow(wr_no_activity)
            .register_activity(cross_boundary_activity)
            .idle_wait(Duration::from_millis(5))
            .build();
        let shutdown = worker.shutdown_handle();

        let (run_result, ()) = futures::future::join(worker.run(), async {
            // The activity is claimed and executing, and it will not finish
            // until this test says so. Bounded well inside the test timeout, so
            // a worker that runs its stages in sequence fails by name rather
            // than by hanging.
            let running = tokio::time::timeout(
                PROGRESS_TIMEOUT,
                wait_until(|| async { CROSS_BOUNDARY_ACTIVITY_RUNNING.load(Ordering::SeqCst) }),
            )
            .await;
            assert!(
                running.is_ok(),
                "the activity loop never started its task while the workflow loop was running"
            );

            // Queued strictly after the activity was already in flight, so its
            // commit cannot have happened before the activity started.
            let committed_run = client
                .start_workflow::<wr_no_activity>(
                    "wf/cross-boundary-committed",
                    "workflows",
                    NumberInput { value: 5 },
                )
                .await
                .unwrap();
            let committed = tokio::time::timeout(
                PROGRESS_TIMEOUT,
                wait_until(|| async { completed_result(&backend, &committed_run).await.is_some() }),
            )
            .await;
            assert!(
                committed.is_ok(),
                "no workflow task committed while the activity was in flight: the workflow \
                 and activity loops are not independent"
            );
            assert_eq!(
                completed_result(&backend, &committed_run).await,
                Some(6),
                "a workflow task must commit while an activity is in flight"
            );
            assert!(
                !has_event(&backend, &parked_run, |data| matches!(
                    data,
                    HistoryEventData::ActivityCompleted(_)
                ))
                .await,
                "the parked activity finished before the test released it, so it \
                 never held the activity loop"
            );

            // `notify_one` stores a permit, so the release cannot be lost even
            // if it raced the activity's registration.
            cross_boundary_gate().notify_one();
            wait_until(|| async { completed_result(&backend, &parked_run).await.is_some() }).await;
            shutdown.shutdown();
        })
        .await;

        run_result.unwrap();
        assert_eq!(completed_result(&backend, &parked_run).await, Some(1));
    });
}

// 2E: a failing workflow stage must not suppress activity completions. With
// every workflow claim failing, the workflow loop can do nothing but catch,
// count, and back off — and the activity loop must still finish the activity
// that was already scheduled.
#[test]
fn injected_workflow_claim_failure_still_lets_an_activity_complete() {
    block_on_tokio(async {
        let backend = ObservingBackend::new(MemoryBackend::new());
        let client = durust::Client::new(backend.clone());
        let run_id = client
            .start_workflow::<wr_double_plus_one>(
                "wf/claim-failure-isolation",
                "workflows",
                NumberInput { value: 8 },
            )
            .await
            .unwrap();

        let events = EventLog::default();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("claim-failure-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(wr_double_plus_one)
            .register_activity(wr_double)
            .idle_wait(Duration::from_millis(5))
            .error_backoff(Duration::from_millis(5))
            .max_error_backoff(Duration::from_millis(5))
            .on_event(events.sink())
            .build();

        // One healthy workflow task schedules the activity; only then do claims
        // start failing, so the activity that must complete is already queued.
        assert!(worker.run_workflow_once().await.unwrap());
        assert!(
            !has_event(&backend, &run_id, |data| matches!(
                data,
                HistoryEventData::ActivityCompleted(_)
            ))
            .await
        );
        backend.fail_workflow_claims(true);

        let shutdown = worker.shutdown_handle();
        // Lets the waiter stop as soon as `run` returns, so a worker that dies
        // on the workflow stage fails on its returned error instead of hanging
        // the test.
        let run_finished = AtomicBool::new(false);
        let (run_result, ()) = futures::future::join(
            async {
                let result = worker.run().await;
                run_finished.store(true, Ordering::SeqCst);
                result
            },
            async {
                wait_until(|| async {
                    run_finished.load(Ordering::SeqCst)
                        || has_event(&backend, &run_id, |data| {
                            matches!(data, HistoryEventData::ActivityCompleted(_))
                        })
                        .await
                })
                .await;
                shutdown.shutdown();
            },
        )
        .await;

        run_result.expect("a failing workflow stage must not kill the worker");
        let metrics = worker.metrics();
        assert_eq!(
            metrics.activity_tasks_completed, 1,
            "the activity loop must finish its task while workflow claims fail: {metrics:?}"
        );
        assert!(
            metrics.workflow_loop_errors > 0,
            "the workflow loop's failures must be counted, not swallowed: {metrics:?}"
        );
        // Counted *and* reported: the loop error is the worker's only account of
        // a stage it kept retrying.
        let loop_errors = events
            .snapshot()
            .into_iter()
            .filter(|event| {
                matches!(
                    event,
                    RecordedEvent::LoopError { worker_loop, error }
                        if worker_loop == "Workflow"
                            && error.contains("injected workflow claim failure")
                )
            })
            .count();
        assert!(loop_errors > 0, "{:?}", events.snapshot());
    });
}

// 2C: maintenance load must track elapsed time, not the task rate. Before the
// interval-paced loop every work pass ran a maintenance scan, so N workflow
// tasks cost N scans — on Postgres, N extra transactions.
//
// Both bounds are asserted. The upper bound is what proves the cadence is
// time-paced: a per-pass scan would blow it by two orders of magnitude. The
// lower bound is what proves maintenance runs at all — without it the test
// passes with the whole maintenance loop deleted.
#[test]
fn maintenance_scans_track_elapsed_time_not_the_workflow_task_rate() {
    block_on_tokio(async {
        const WORKFLOWS: usize = 300;
        const INTERVAL: Duration = Duration::from_millis(25);
        // The jitter floor: no scan can be scheduled closer than half the
        // interval, so this is the fastest an honest cadence can run.
        const FASTEST_MS_PER_SCAN: f64 = 12.5;
        // Idle window for the lower bound, long enough for many intervals.
        const IDLE_WINDOW: Duration = Duration::from_millis(300);

        let backend = ObservingBackend::new(MemoryBackend::new());
        let client = durust::Client::new(backend.clone());
        let mut runs = Vec::with_capacity(WORKFLOWS);
        for index in 0..WORKFLOWS {
            runs.push(
                client
                    .start_workflow::<wr_no_activity>(
                        format!("wf/maintenance-cadence-{index}"),
                        "workflows",
                        NumberInput {
                            value: index as u64,
                        },
                    )
                    .await
                    .unwrap(),
            );
        }

        let mut worker = Worker::builder(backend.clone())
            .worker_id("maintenance-cadence-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_no_activity)
            .idle_wait(Duration::from_millis(1))
            .maintenance_interval(INTERVAL)
            // Pinned to the base interval so the cadence under test is one
            // number rather than a doubling sequence.
            .max_maintenance_interval(INTERVAL)
            .build();
        let shutdown = worker.shutdown_handle();

        let started = Instant::now();
        let scans = Arc::new(Mutex::new((0usize, 0usize, Duration::ZERO)));
        let recorded = Arc::clone(&scans);
        let (run_result, ()) = futures::future::join(worker.run(), async move {
            wait_until(|| async {
                let mut all = true;
                for run in &runs {
                    all &= completed_result(&backend, run).await.is_some();
                }
                all
            })
            .await;
            let working = started.elapsed();
            let after_work = backend.maintenance_calls();

            // Nothing left to do, so every further scan is the loop's own
            // cadence and nothing else.
            tokio::time::sleep(IDLE_WINDOW).await;
            let after_idle = backend.maintenance_calls();
            *recorded.lock().unwrap() = (after_work, after_idle, working);
            shutdown.shutdown();
        })
        .await;

        run_result.unwrap();
        let (after_work, after_idle, working) = *scans.lock().unwrap();
        let working_ms = working.as_secs_f64() * 1_000.0;

        // O(elapsed / interval), not O(N): with one scan per pass this would be
        // at least `WORKFLOWS`, and the memory backend drains 300 trivial runs
        // in far less than 300 * 12.5 ms.
        let working_bound = working_ms / FASTEST_MS_PER_SCAN + 4.0;
        assert!(
            (after_work as f64) <= working_bound,
            "{after_work} maintenance scans while draining {WORKFLOWS} workflow tasks \
             in {working_ms:.1} ms; a time-paced cadence allows at most {working_bound:.1}"
        );
        assert!(
            after_work < WORKFLOWS,
            "{after_work} scans for {WORKFLOWS} tasks is still pass-paced"
        );

        // ...and maintenance did keep running: an idle worker still scans on
        // its interval. `IDLE_WINDOW / INTERVAL` is 12 scans at the nominal
        // cadence and 8 at the slowest jitter; 3 leaves room for a loaded
        // machine while still failing outright if the loop never runs.
        let idle_scans = after_idle - after_work;
        assert!(
            idle_scans >= 3,
            "only {idle_scans} maintenance scans in {IDLE_WINDOW:?} of idle time; \
             the interval-paced loop is not running"
        );
        let idle_bound = IDLE_WINDOW.as_secs_f64() * 1_000.0 / FASTEST_MS_PER_SCAN + 4.0;
        assert!(
            (idle_scans as f64) <= idle_bound,
            "{idle_scans} idle scans exceeds the {idle_bound:.1} a {INTERVAL:?} cadence allows"
        );
        // The worker's own counter agrees with the provider's, so the metric an
        // operator would read is the load the provider actually saw.
        assert_eq!(worker.metrics().maintenance_scans as usize, after_idle);
    });
}

// 2D: `run_timer_maintenance(false)` must stop this worker firing timers — in
// the interval-paced loop *and* in the deterministic pass driver — while a peer
// worker on the same backend still fires them.
//
// And it must stop *only* that. Child-start dispatch is not covered by the
// knob, because `SPEC.md` §11 licenses a worker to skip timer and
// activity-deadline scanning on the grounds that a timer service owns those,
// and says nothing about the child-start outbox — which nothing else in a
// deployment drains. A parent therefore has to complete on the disabled worker
// alone, or the knob is a way to strand every child workflow forever.
#[test]
fn disabled_timer_maintenance_still_dispatches_child_workflow_starts() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let sleeper_run = client
            .start_workflow::<wr_sleeper>(
                "wf/maintenance-optout",
                "workflows",
                NumberInput { value: 5 },
            )
            .await
            .unwrap();
        let parent_run = client
            .start_workflow::<wr_optout_parent>(
                "wf/maintenance-optout-parent",
                "workflows",
                NumberInput { value: 2 },
            )
            .await
            .unwrap();

        let mut disabled = Worker::builder(backend.clone())
            .worker_id("maintenance-disabled-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_sleeper)
            .register_workflow(wr_optout_parent)
            .register_workflow(wr_optout_child)
            .run_timer_maintenance(false)
            .idle_wait(Duration::from_millis(5))
            .maintenance_interval(Duration::from_millis(5))
            .max_maintenance_interval(Duration::from_millis(5))
            .build();

        // The child workflow starts, runs, and reports back to its parent on
        // this worker alone — the outbox dispatch the knob must not touch.
        disabled.run_until_idle().await.unwrap();
        assert_eq!(
            completed_result(&backend, &parent_run).await,
            Some(102),
            "a worker with timer maintenance disabled must still dispatch child starts, \
             or child workflows never run anywhere"
        );
        assert!(
            disabled.metrics().child_workflow_starts_dispatched >= 1,
            "{:?}",
            disabled.metrics()
        );

        // The timer, meanwhile, is still pending: one pass started it, and
        // virtual time now makes it due.
        backend.advance_time(Duration::from_millis(50));

        // The pass driver's maintenance stage is gated too.
        disabled.run_until_idle().await.unwrap();
        assert!(
            !has_event(&backend, &sleeper_run, |data| matches!(
                data,
                HistoryEventData::TimerFired(_)
            ))
            .await,
            "a worker with maintenance disabled fired a due timer from run_until_idle"
        );

        // ...and so is the production loop, over a window many intervals long.
        let shutdown = disabled.shutdown_handle();
        let (run_result, ()) = futures::future::join(disabled.run(), async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            shutdown.shutdown();
        })
        .await;
        run_result.unwrap();
        assert!(
            !has_event(&backend, &sleeper_run, |data| matches!(
                data,
                HistoryEventData::TimerFired(_)
            ))
            .await,
            "a worker with maintenance disabled fired a due timer from its run loop"
        );
        assert_eq!(
            disabled.metrics().maintenance_scans,
            0,
            "a worker with maintenance disabled must not scan at all"
        );

        // The peer, with the default, fires the same timer and finishes the run.
        let mut enabled = Worker::builder(backend.clone())
            .worker_id("maintenance-enabled-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_sleeper)
            .register_workflow(wr_optout_parent)
            .register_workflow(wr_optout_child)
            .build();
        enabled.run_until_idle().await.unwrap();
        assert!(
            has_event(&backend, &sleeper_run, |data| matches!(
                data,
                HistoryEventData::TimerFired(_)
            ))
            .await,
            "the peer worker must still fire due timers"
        );
        assert_eq!(completed_result(&backend, &sleeper_run).await, Some(6));
        assert!(enabled.metrics().timers_fired >= 1);
    });
}

// The other half of the same guarantee, and the half a deployment actually
// depends on.
//
// There are two child-start dispatch sites, not one: `run_pass_once`, which
// only the deterministic driver `run_until_idle` reaches, and
// `run_maintenance_scan_once`, which is the sole site under `Worker::run`.
// A test that drives `run_until_idle` covers the first and says nothing about
// the second, so gating the drain inside `run_maintenance_scan_once` on
// `run_timer_maintenance` — exactly the change that once stopped every child
// workflow forever — passes the sibling test above and still strands every
// child in production.
//
// So this case never lets the pass driver near the parent it measures: it
// drains `run_until_idle` first, with nothing queued, and only then starts the
// parent, which can therefore reach its child through the interval loop alone.
#[test]
fn disabled_timer_maintenance_dispatches_child_starts_from_the_interval_loop() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());

        let mut disabled = Worker::builder(backend.clone())
            .worker_id("maintenance-disabled-loop-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_optout_loop_parent)
            .register_workflow(wr_optout_loop_child)
            .run_timer_maintenance(false)
            .idle_wait(Duration::from_millis(5))
            .maintenance_interval(Duration::from_millis(5))
            .max_maintenance_interval(Duration::from_millis(5))
            .build();

        // Drains to idle against an empty queue. Everything this test asserts
        // happens after this line, so no outbox row it measures can have been
        // dispatched by the pass driver's maintenance stage.
        disabled.run_until_idle().await.unwrap();

        let parent_run = client
            .start_workflow::<wr_optout_loop_parent>(
                "wf/maintenance-optout-loop-parent",
                "workflows",
                NumberInput { value: 2 },
            )
            .await
            .unwrap();

        let shutdown = disabled.shutdown_handle();
        let (run_result, completed) = futures::future::join(disabled.run(), async {
            // Bounded well inside the harness timeout, so a worker that stops
            // draining the outbox fails on the assertion naming the property
            // rather than by hanging.
            let completed = tokio::time::timeout(
                PROGRESS_TIMEOUT,
                wait_until(|| async { completed_result(&backend, &parent_run).await.is_some() }),
            )
            .await;
            shutdown.shutdown();
            completed
        })
        .await;
        run_result.unwrap();

        assert!(
            completed.is_ok(),
            "the parent never completed under `run` alone: with timer maintenance disabled the \
             interval loop stopped dispatching child workflow starts, so every child workflow in \
             a deployment would be stranded forever"
        );
        assert_eq!(
            completed_result(&backend, &parent_run).await,
            Some(202),
            "the child ran but reported the wrong result to its parent"
        );
        assert!(
            disabled.metrics().child_workflow_starts_dispatched >= 1,
            "no child start was dispatched from the interval loop: {:?}",
            disabled.metrics()
        );
        assert_eq!(
            disabled.metrics().maintenance_scans,
            0,
            "the timer and activity-deadline scan must stay disabled while child dispatch runs"
        );
    });
}

// 2F: repeated silent retries of a poisoned run must be observable, and a
// workflow bug must never be reported as history divergence. This is genuine
// divergence — a recorded `TimerStarted` the replacement build never replays —
// so it must land in the divergence counter and nowhere near the panic counter.
#[test]
fn repeated_nondeterministic_replays_are_counted_and_never_confused_with_panics() {
    block_on_tokio(async {
        const RETRIES: u64 = 3;
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let run_id = client
            .start_workflow::<wr_two_timers>("wf/metrics-divergence", "workflows", UnitInput {})
            .await
            .unwrap();

        // Record both timers with the original build.
        let mut recorder = Worker::builder(backend.clone())
            .worker_id("divergence-recorder")
            .workflow_task_queue("workflows")
            .register_workflow(wr_two_timers)
            .build();
        assert!(recorder.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::from_millis(1));
        assert_eq!(recorder.run_timers_once().await.unwrap(), 1);
        assert!(recorder.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::from_millis(1));
        assert_eq!(recorder.run_timers_once().await.unwrap(), 1);
        drop(recorder);

        let events = EventLog::default();
        let mut changed = Worker::builder(backend.clone())
            .worker_id("divergence-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_one_timer)
            .nondeterminism_retry_backoff(Duration::from_millis(25))
            .on_event(events.sink())
            .build();

        for attempt in 0..RETRIES {
            let err = changed.run_workflow_once().await.unwrap_err();
            assert!(
                matches!(err, durust::Error::Nondeterminism(_)),
                "attempt {attempt} expected divergence, got {err:?}"
            );
            // Past the retry backoff on the virtual clock, so the next claim is
            // a real re-attempt rather than the same one.
            backend.advance_time(Duration::from_millis(50));
        }

        let metrics = changed.metrics();
        assert_eq!(
            metrics.workflow_tasks_nondeterministic, RETRIES,
            "every silent retry must be counted: {metrics:?}"
        );
        assert_eq!(
            metrics.workflow_tasks_panicked, 0,
            "history divergence must not be reported as a workflow panic: {metrics:?}"
        );
        assert_eq!(metrics.workflow_tasks_committed, 0, "{metrics:?}");

        // Each retry is individually reportable, naming the run.
        let failures = events
            .snapshot()
            .into_iter()
            .filter(|event| {
                matches!(
                    event,
                    RecordedEvent::WorkflowTaskFailed { run_id: id, error }
                        if id == &run_id.to_string() && error.contains("nondeterministic replay")
                )
            })
            .count();
        assert_eq!(failures as u64, RETRIES, "{:?}", events.snapshot());

        // Still replayable against a fixed build: nothing terminal was written.
        assert!(
            !has_event(&backend, &run_id, |data| matches!(
                data,
                HistoryEventData::WorkflowFailed { .. }
                    | HistoryEventData::WorkflowCompleted { .. }
            ))
            .await
        );
    });
}

// 2F, the other half: a panic in workflow code, and the re-entrancy guard that
// reports by panicking, are both workflow bugs. Both are routed exactly like
// divergence — nothing committed, claim released for retry — so a counter is
// the only thing that can tell an operator to look for a backtrace instead of a
// history mismatch.
#[test]
fn workflow_panics_and_re_entrancy_are_counted_apart_from_divergence() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let panicking_run = client
            .start_workflow::<wr_panicking_batched>(
                "wf/metrics-panic",
                "workflows",
                NumberInput { value: 0 },
            )
            .await
            .unwrap();
        let reentrant_run = client
            .start_workflow::<wr_reentrant_side_effect>(
                "wf/metrics-reentrancy",
                "workflows",
                UnitInput {},
            )
            .await
            .unwrap();

        let events = EventLog::default();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("metrics-panic-worker")
            .workflow_task_queue("workflows")
            .register_workflow(wr_panicking_batched)
            .register_workflow(wr_reentrant_side_effect)
            .on_event(events.sink())
            .build();

        let panic_err = worker.run_workflow_once().await.unwrap_err();
        assert!(
            matches!(panic_err, durust::Error::TaskPanic(_)),
            "{panic_err:?}"
        );
        let reentrancy_err = worker.run_workflow_once().await.unwrap_err();
        let durust::Error::TaskPanic(message) = &reentrancy_err else {
            panic!("the re-entrancy guard must classify as a panic, got {reentrancy_err:?}");
        };
        assert!(
            message.contains("durable APIs are not re-entrant"),
            "{message}"
        );

        let metrics = worker.metrics();
        assert_eq!(
            metrics.workflow_tasks_panicked, 2,
            "a workflow panic and a re-entrant durable call are both panics: {metrics:?}"
        );
        assert_eq!(
            metrics.workflow_tasks_nondeterministic, 0,
            "a workflow bug must not be counted as history divergence: {metrics:?}"
        );

        // Both are individually reportable, and both name their run.
        let failed_runs = events
            .snapshot()
            .into_iter()
            .filter_map(|event| match event {
                RecordedEvent::WorkflowTaskFailed { run_id, error }
                    if error.contains("workflow task panicked:") =>
                {
                    Some(run_id)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            failed_runs,
            vec![panicking_run.to_string(), reentrant_run.to_string()],
            "{:?}",
            events.snapshot()
        );
    });
}

// A claim released without being replayed — recovery admission, a replay
// budget, provider backpressure — is neither committed nor failed. The batch
// path counted it as nothing, so a fully deferred batch was indistinguishable
// from an empty queue and a drain could stop with work still queued.
#[test]
fn a_fully_deferred_batch_is_not_reported_as_an_idle_worker() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        let run_id = client
            .start_workflow::<wr_double_plus_one>(
                "wf/deferred-batch",
                "workflows",
                NumberInput { value: 4 },
            )
            .await
            .unwrap();

        // Progress the run past its first task so the next claim is a cold
        // recovery, which is what the admission bound gates.
        let mut first = Worker::builder(backend.clone())
            .worker_id("deferred-batch-primer")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(wr_double_plus_one)
            .register_activity(wr_double)
            .build();
        assert!(first.run_workflow_once().await.unwrap());
        assert!(first.run_activity_once().await.unwrap());
        drop(first);

        let events = EventLog::default();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("deferred-batch-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(wr_double_plus_one)
            .register_activity(wr_double)
            // Above one on both knobs so the stage takes the batch path.
            .max_concurrent_workflow_tasks(4)
            .workflow_task_prefetch_limit(4)
            // No recovery admission at all, so every cold claim defers.
            .max_concurrent_recoveries(0)
            .on_event(events.sink())
            .build();

        let err = worker
            .run_until_idle_with(WorkerRunOptions { max_iterations: 1 })
            .await
            .unwrap_err();
        assert!(
            matches!(&err, durust::Error::Backend(message) if message.contains("did not become idle")),
            "a deferred claim must count as work, so the pass reports progress; got {err:?}"
        );

        let metrics = worker.metrics();
        assert_eq!(metrics.workflow_tasks_deferred, 1, "{metrics:?}");
        assert_eq!(metrics.workflow_tasks_committed, 0, "{metrics:?}");
        assert_eq!(
            events.snapshot(),
            vec![RecordedEvent::WorkflowTaskDeferred(run_id.to_string())]
        );
    });
}

// The same outcome on the single-task path, which reported it as a *committed*
// task: `run_claimed_workflow_task` returned `Ok(())` after a successful
// backpressure release, so the stage counted a task that wrote nothing.
#[test]
fn a_deferred_single_task_is_reported_as_deferred_not_committed() {
    block_on_tokio(async {
        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        client
            .start_workflow::<wr_double_plus_one>(
                "wf/deferred-single",
                "workflows",
                NumberInput { value: 6 },
            )
            .await
            .unwrap();

        let mut first = Worker::builder(backend.clone())
            .worker_id("deferred-single-primer")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(wr_double_plus_one)
            .register_activity(wr_double)
            .build();
        assert!(first.run_workflow_once().await.unwrap());
        assert!(first.run_activity_once().await.unwrap());
        drop(first);

        let mut worker = Worker::builder(backend.clone())
            .worker_id("deferred-single-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(wr_double_plus_one)
            .register_activity(wr_double)
            // Concurrency knobs left at one, so the stage takes the single-task
            // path.
            .max_concurrent_recoveries(0)
            .build();

        // Pass one defers, pass two finds the run hidden behind the defer delay
        // on the virtual clock and reports idle.
        let stats = worker
            .run_until_idle_with(WorkerRunOptions { max_iterations: 4 })
            .await
            .unwrap();
        assert_eq!(
            stats.workflow_tasks, 0,
            "a deferred task committed nothing and must not be counted as a task: {stats:?}"
        );
        assert_eq!(stats.workflow_tasks_deferred, 1, "{stats:?}");
        assert_eq!(worker.metrics().workflow_tasks_committed, 0);
    });
}

// The peer-yield regression. `Worker::run` joins its three loops on one task,
// so a loop is polled again only when the whole join is polled again — and on a
// provider whose calls resolve without suspending, a loop with work queued
// never returns `Pending` on its own. It therefore hands its peers a poll every
// `PROGRESS_YIELD_PASSES` productive passes.
//
// Without that yield nothing hangs and nothing errors, which is why the rest of
// the suite cannot see it: the activity still runs, just not until the entire
// workflow backlog has drained. So the assertion is *when*, not *whether*. The
// activity records how many workflow tasks had committed when it started; with
// the yield that is one yield cadence in, and without it, the whole backlog.
#[test]
fn a_saturated_workflow_loop_yields_to_the_activity_loop_mid_backlog() {
    block_on_tokio(async {
        // Far more than the yield cadence, so "mid-backlog" and "after the
        // backlog" are separated by dozens of commits rather than a rounding
        // error.
        const FILLERS: u64 = 50;
        SATURATION_COMMITS.store(0, Ordering::SeqCst);
        SATURATION_ACTIVITY_AT_COMMIT.store(0, Ordering::SeqCst);

        let backend = MemoryBackend::new();
        let client = durust::Client::new(backend.clone());
        // Started first, so its task is the first the workflow loop claims and
        // the activity is queued from commit one onward: every later commit is
        // one the activity loop could have interleaved with.
        let activity_run = client
            .start_workflow::<saturation_workflow>(
                "wf/saturation-activity",
                "workflows",
                UnitInput {},
            )
            .await
            .unwrap();
        for index in 0..FILLERS {
            client
                .start_workflow::<saturation_filler>(
                    format!("wf/saturation-filler-{index}"),
                    "workflows",
                    NumberInput { value: index },
                )
                .await
                .unwrap();
        }

        let mut worker = Worker::builder(backend.clone())
            .worker_id("saturation-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(saturation_workflow)
            .register_workflow(saturation_filler)
            .register_activity(saturation_activity)
            // Counting commits from the sink rather than from history keeps the
            // probe on the worker's own timeline.
            .on_event(|event| {
                if matches!(event, durust::WorkerEvent::WorkflowTaskCommitted { .. }) {
                    SATURATION_COMMITS.fetch_add(1, Ordering::SeqCst);
                }
            })
            .build();
        let shutdown = worker.shutdown_handle();

        let (run_result, ()) = futures::future::join(worker.run(), async {
            let drained = tokio::time::timeout(
                PROGRESS_TIMEOUT,
                wait_until(|| async { completed_result(&backend, &activity_run).await.is_some() }),
            )
            .await;
            assert!(drained.is_ok(), "the activity workflow never completed");
            shutdown.shutdown();
        })
        .await;
        run_result.unwrap();

        let at = SATURATION_ACTIVITY_AT_COMMIT.load(Ordering::SeqCst);
        assert!(at > 0, "the activity never ran");
        assert!(
            at < FILLERS,
            "the activity loop only got a turn after the whole {FILLERS}-task workflow \
             backlog had drained (it ran at commit {at}): a saturated loop is starving \
             its peers instead of yielding to them"
        );
        // Tighter than the property itself: a yield every eight productive
        // passes puts the first peer poll inside the first sixteen commits.
        assert!(
            at <= 16,
            "the activity ran at commit {at}, later than the yield cadence allows"
        );
    });
}
