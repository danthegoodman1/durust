use crate::{
    ActivityHeartbeatRequest, ClaimActivityOptions, ClaimActivityTasksOptions,
    ClaimWorkflowTaskOptions, ClaimWorkflowTasksOptions, CompleteActivityRequest, DurableBackend,
    Error, EventId, FailActivityRequest, FireDueTimersRequest, HistoryEvent, HistoryEventData,
    Namespace, NewHistoryEvent, ReadSignalInboxRequest, ReadSignalInboxesRequest, Registry, Result,
    RunDueMaintenanceRequest, RunId, ShardId, StartWorkflowRequest, TaskQueue,
    TimeoutDueActivitiesRequest, WaitForReadyRequest, WorkerId, Workflow, WorkflowId,
    WorkflowTaskCommit, WorkflowTaskRelease, poll_with_activity_context, poll_with_runtime_context,
};
use futures::Future;
use futures::stream::{FuturesOrdered, FuturesUnordered, StreamExt};
use serde::Serialize;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

const DEFAULT_TASK_LEASE_DURATION: Duration = Duration::from_secs(30);
// A lease shorter than this cannot reliably outlive a single claim-to-commit
// round trip, so claims would be reclaimed while their holder is still alive.
const MIN_TASK_LEASE_DURATION: Duration = Duration::from_secs(1);
const DEFAULT_IDLE_WAIT: Duration = Duration::from_millis(100);
// Below this an idle worker degenerates into a busy poll against providers
// without push notifications.
const MIN_IDLE_WAIT: Duration = Duration::from_millis(5);
// A workflow task that failed without committing is released with this delay at
// minimum. It is the only thing standing between a permanently poisoned run and
// a busy re-claim loop: the failed task is work the workflow loop performed, so
// the loop counts it as progress and skips its idle wait, and a zero delay makes
// the run immediately re-claimable. The loop's progress yield keeps the peers
// alive through that, but nothing else bounds the re-claim rate. Matching
// `MIN_IDLE_WAIT` keeps the worst case at the same polling granularity an idle
// worker already costs.
const MIN_NONDETERMINISM_RETRY_BACKOFF: Duration = MIN_IDLE_WAIT;
const DEFAULT_MAX_CACHED_WORKFLOWS: usize = 10_000;

/// Blob reads one workflow task keeps in flight while hydrating offloaded
/// payloads. A resumed fanout blocks on every item at once, so a serial drain
/// costs one provider round trip per item; the bound keeps a wide map from
/// opening one request per item instead.
const DEFAULT_MAX_CONCURRENT_PAYLOAD_HYDRATIONS: usize = 16;
// Ceiling for the doubling idle backoff. A worker that finds nothing for a
// while re-polls at most this often, which is the provider load an idle fleet
// costs.
const DEFAULT_MAX_IDLE_WAIT: Duration = Duration::from_secs(1);
// A task loop that threw waits this long before retrying, doubling to
// `DEFAULT_MAX_ERROR_BACKOFF`. This is what bounds the retry rate of a fault
// the loop cannot settle per task — a claim RPC that keeps failing, or a
// workflow input that cannot be decoded and is therefore re-claimable
// immediately.
const DEFAULT_ERROR_BACKOFF: Duration = Duration::from_millis(250);
// Below this an erroring loop degenerates into a busy retry against the
// provider that is already failing it.
const MIN_ERROR_BACKOFF: Duration = MIN_IDLE_WAIT;
const DEFAULT_MAX_ERROR_BACKOFF: Duration = Duration::from_secs(5);
// Base delay between maintenance scans. Timer latency is bounded by this
// rather than by how busy the worker is, and provider scan load is bounded by
// fleet size rather than by throughput (`SPEC.md` §11).
const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(250);
// Ceiling for the doubling maintenance backoff after consecutive empty scans.
const DEFAULT_MAX_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
// Productive passes a task loop may take before it must hand its peers a poll.
//
// The three loops are cooperatively scheduled inside one `try_join3`, which
// polls a branch again only when the whole join is polled again. A branch that
// never returns `Pending` therefore starves its peers outright rather than
// merely delaying them — the ordinary case on a synchronous provider such as
// `MemoryBackend`, where a whole workflow task resolves without yielding. Not a
// public knob: the cost on a synchronous backend is one extra task poll per
// eight tasks, and on a socket-backed provider every task already yields
// several times.
const PROGRESS_YIELD_PASSES: usize = 8;

/// Requests a graceful stop of `Worker::run`. Cheap to clone and safe to
/// trigger from any task or thread; `run` returns `Ok(())` at the next loop
/// iteration after `shutdown` is called.
#[derive(Clone)]
pub struct WorkerShutdown {
    inner: Arc<(AtomicBool, tokio::sync::Notify)>,
}

impl WorkerShutdown {
    fn new() -> Self {
        Self {
            inner: Arc::new((AtomicBool::new(false), tokio::sync::Notify::new())),
        }
    }

    pub fn shutdown(&self) {
        self.inner.0.store(true, Ordering::SeqCst);
        self.inner.1.notify_waiters();
    }

    fn is_requested(&self) -> bool {
        self.inner.0.load(Ordering::SeqCst)
    }

    fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.inner.1.notified()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerRunOptions {
    pub max_iterations: usize,
}

impl Default for WorkerRunOptions {
    fn default() -> Self {
        Self {
            max_iterations: 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkerRunStats {
    pub workflow_tasks: usize,
    /// Workflow tasks that failed without committing anything and released
    /// their claim for a later retry: a nondeterministic replay, a caught
    /// workflow panic, or a recorded change version this build cannot replay.
    ///
    /// Separate from `workflow_tasks` because nothing was written — counting a
    /// void attempt as a committed task would over-report progress.
    ///
    /// This is the only record of the fault, because a per-task fault no longer
    /// fails its pass (that would take the pass's maintenance, child dispatch,
    /// and activity stages down with one bad run). It is therefore observable
    /// exactly to callers that read the returned stats: `run_until_idle` and
    /// `run_until_idle_with`.
    ///
    /// It is *not* observable under [`Worker::run`], which builds fresh stats
    /// per pass and discards them. The production loop's equivalent is
    /// [`WorkerMetrics::workflow_tasks_panicked`],
    /// [`WorkerMetrics::workflow_tasks_nondeterministic`], and
    /// [`WorkerMetrics::workflow_tasks_unsupported_version`], which split this
    /// count by cause, survive across passes, and are also emitted to the
    /// worker's event sink as [`WorkerEvent::WorkflowTaskFailed`].
    pub workflow_tasks_failed: usize,
    /// Workflow tasks whose claim was released for a later attempt without
    /// being replayed at all: cold-recovery admission, a replay budget, or
    /// provider backpressure.
    ///
    /// Neither committed nor failed. It exists because a driver must not read
    /// "nothing committed" as "nothing to do": a fully backpressured batch
    /// makes no progress but has work queued, and treating it as idle stops a
    /// drain with tasks outstanding. The batch and single-task paths report it
    /// identically; before this counter the batch path reported a deferred task
    /// as nothing and the single-task path reported it as a committed task.
    pub workflow_tasks_deferred: usize,
    pub activity_tasks: usize,
    pub timers_fired: usize,
    pub activities_timed_out: usize,
    pub child_workflow_starts_dispatched: usize,
}

/// Cumulative counters for one worker's lifetime, read through
/// [`Worker::metrics`].
///
/// Fed by every driver — [`Worker::run`], [`Worker::run_until_idle`], and the
/// one-shot entry points alike — so a deterministic test observes exactly what
/// the production loop would.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkerMetrics {
    /// Workflow tasks whose commit landed.
    pub workflow_tasks_committed: u64,
    /// Workflow tasks that failed on a caught panic in workflow code
    /// ([`Error::TaskPanic`]).
    ///
    /// Separate from `workflow_tasks_nondeterministic` because a panic and a
    /// history divergence need different operator responses: a panic means look
    /// for a backtrace and fix the build, divergence means look for a
    /// history/version mismatch and consider a rollback. Both retry silently on
    /// the release backoff forever, so mis-signalling costs recovery time on
    /// precisely the runs that have no other signal. The re-entrancy guard
    /// (`durust durable APIs are not re-entrant`) panics, so it lands here and
    /// not in the divergence count.
    pub workflow_tasks_panicked: u64,
    /// Workflow tasks that failed because the replayed command sequence
    /// diverged from history ([`Error::Nondeterminism`]).
    pub workflow_tasks_nondeterministic: u64,
    /// Workflow tasks that failed because history records a change version this
    /// build cannot replay ([`Error::UnsupportedWorkflowVersion`]).
    pub workflow_tasks_unsupported_version: u64,
    /// Workflow tasks a durable API refused for the workflow's own values: a
    /// payload that would not encode or decode, or an empty side-effect key.
    /// Released for retry like a panic, never committed as `WorkflowFailed`.
    pub workflow_tasks_faulted: u64,
    /// Workflow tasks released for a later attempt without being replayed.
    pub workflow_tasks_deferred: u64,
    pub activity_tasks_completed: u64,
    pub activity_tasks_failed: u64,
    pub timers_fired: u64,
    pub activities_timed_out: u64,
    pub child_workflow_starts_dispatched: u64,
    /// Maintenance scans performed, whether or not they found work.
    pub maintenance_scans: u64,
    /// Errors caught by the workflow loop. Each one is also emitted as
    /// [`WorkerEvent::LoopError`] before the loop backs off and continues.
    pub workflow_loop_errors: u64,
    pub activity_loop_errors: u64,
    pub maintenance_loop_errors: u64,
    /// Times a task loop found no work and waited.
    pub idle_waits: u64,
}

// Interior-mutable counters behind `&WorkerShared`, which is what the three
// concurrent loops share. `Relaxed` throughout: these are monotonic counters
// read for observability, never used to order other memory.
#[derive(Debug, Default)]
struct WorkerMetricsState {
    workflow_tasks_committed: AtomicU64,
    workflow_tasks_panicked: AtomicU64,
    workflow_tasks_nondeterministic: AtomicU64,
    workflow_tasks_unsupported_version: AtomicU64,
    workflow_tasks_faulted: AtomicU64,
    workflow_tasks_deferred: AtomicU64,
    activity_tasks_completed: AtomicU64,
    activity_tasks_failed: AtomicU64,
    timers_fired: AtomicU64,
    activities_timed_out: AtomicU64,
    child_workflow_starts_dispatched: AtomicU64,
    maintenance_scans: AtomicU64,
    workflow_loop_errors: AtomicU64,
    activity_loop_errors: AtomicU64,
    maintenance_loop_errors: AtomicU64,
    idle_waits: AtomicU64,
}

impl WorkerMetricsState {
    fn snapshot(&self) -> WorkerMetrics {
        WorkerMetrics {
            workflow_tasks_committed: self.workflow_tasks_committed.load(Ordering::Relaxed),
            workflow_tasks_panicked: self.workflow_tasks_panicked.load(Ordering::Relaxed),
            workflow_tasks_nondeterministic: self
                .workflow_tasks_nondeterministic
                .load(Ordering::Relaxed),
            workflow_tasks_unsupported_version: self
                .workflow_tasks_unsupported_version
                .load(Ordering::Relaxed),
            workflow_tasks_faulted: self.workflow_tasks_faulted.load(Ordering::Relaxed),
            workflow_tasks_deferred: self.workflow_tasks_deferred.load(Ordering::Relaxed),
            activity_tasks_completed: self.activity_tasks_completed.load(Ordering::Relaxed),
            activity_tasks_failed: self.activity_tasks_failed.load(Ordering::Relaxed),
            timers_fired: self.timers_fired.load(Ordering::Relaxed),
            activities_timed_out: self.activities_timed_out.load(Ordering::Relaxed),
            child_workflow_starts_dispatched: self
                .child_workflow_starts_dispatched
                .load(Ordering::Relaxed),
            maintenance_scans: self.maintenance_scans.load(Ordering::Relaxed),
            workflow_loop_errors: self.workflow_loop_errors.load(Ordering::Relaxed),
            activity_loop_errors: self.activity_loop_errors.load(Ordering::Relaxed),
            maintenance_loop_errors: self.maintenance_loop_errors.load(Ordering::Relaxed),
            idle_waits: self.idle_waits.load(Ordering::Relaxed),
        }
    }
}

/// Which of [`Worker::run`]'s three independent loops raised an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerLoop {
    Workflow,
    Activity,
    Maintenance,
}

/// A worker lifecycle event delivered to the sink installed with
/// [`WorkerBuilder::on_event`].
///
/// Borrowed rather than owned: the sink is on the worker's hot path, and an
/// owned event would put a `RunId` clone on every committed task even when no
/// sink is installed. A sink that needs to keep a value clones it itself.
#[derive(Debug)]
#[non_exhaustive]
pub enum WorkerEvent<'a> {
    WorkflowTaskCommitted {
        run_id: &'a RunId,
    },
    /// A workflow task that wrote nothing and released its claim for a later
    /// attempt. `error` distinguishes a panic from a divergence from an
    /// unsupported recorded version.
    WorkflowTaskFailed {
        run_id: &'a RunId,
        error: &'a Error,
    },
    /// A workflow task released without being replayed: recovery admission, a
    /// replay budget, or provider backpressure.
    WorkflowTaskDeferred {
        run_id: &'a RunId,
    },
    ActivityTaskCompleted {
        activity_id: &'a crate::ActivityId,
    },
    ActivityTaskFailed {
        activity_id: &'a crate::ActivityId,
        failure: &'a crate::DurableFailure,
    },
    /// One maintenance scan's results. Emitted for every scan, including the
    /// ones that found nothing, so a sink can measure the achieved cadence.
    MaintenanceScanned {
        timers_fired: usize,
        activities_timed_out: usize,
        child_workflow_starts_dispatched: usize,
    },
    /// An error one loop caught, counted, and backed off from. The loop
    /// continues; this is the worker's only report of it.
    LoopError {
        worker_loop: WorkerLoop,
        error: &'a Error,
    },
}

// `Fn`, not `FnMut`: three loops share one `&WorkerShared`, so the sink is
// called from all of them and must not require exclusive access. `Send + Sync`
// so a built worker stays movable across threads, which `DurableBackend`
// already requires of the backend.
type WorkerEventSink = Arc<dyn Fn(WorkerEvent<'_>) + Send + Sync>;

// Timing for `Worker::run`'s three loops. Every field is resolved and clamped
// by the builder, so the loops never re-derive a bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WorkerLoopConfig {
    idle_wait: Duration,
    max_idle_wait: Duration,
    error_backoff: Duration,
    max_error_backoff: Duration,
    maintenance_interval: Duration,
    max_maintenance_interval: Duration,
}

impl Default for WorkerLoopConfig {
    fn default() -> Self {
        Self {
            idle_wait: DEFAULT_IDLE_WAIT,
            max_idle_wait: DEFAULT_MAX_IDLE_WAIT,
            error_backoff: DEFAULT_ERROR_BACKOFF,
            max_error_backoff: DEFAULT_MAX_ERROR_BACKOFF,
            maintenance_interval: DEFAULT_MAINTENANCE_INTERVAL,
            max_maintenance_interval: DEFAULT_MAX_MAINTENANCE_INTERVAL,
        }
    }
}

/// What one workflow-task stage did: tasks whose commit landed, tasks that
/// failed without committing and released their own claim, and tasks released
/// unreplayed for a later attempt.
///
/// The split exists because a driver must treat the three differently. A
/// committed task is recorded progress. A failed or deferred task is not
/// progress — nothing reached history — but the stage did consume a claim, so
/// the driver has to take another pass rather than conclude the worker is idle
/// while queued tasks remain.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WorkflowStageOutcome {
    committed: usize,
    failed: usize,
    deferred: usize,
}

impl WorkflowStageOutcome {
    // Whether the stage consumed a claim at all. Any of the three outcomes
    // means the queue was not empty.
    fn consumed_a_claim(self) -> bool {
        self.committed > 0 || self.failed > 0 || self.deferred > 0
    }
}

pub struct Client<B>
where
    B: DurableBackend,
{
    backend: B,
    namespace: Namespace,
    payload_codec: crate::CodecId,
}

impl<B> Client<B>
where
    B: DurableBackend,
{
    pub fn new(backend: B) -> Self {
        let payload_codec = backend.payload_storage_config().codec;
        Self {
            backend,
            namespace: Namespace::default(),
            payload_codec,
        }
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Namespace::new(namespace);
        self
    }

    pub async fn start_workflow<W>(
        &self,
        workflow_id: impl Into<String>,
        task_queue: impl Into<String>,
        input: W::Input,
    ) -> Result<RunId>
    where
        W: Workflow,
    {
        let outcome = self
            .backend
            .start_workflow(StartWorkflowRequest {
                namespace: self.namespace.clone(),
                workflow_id: WorkflowId::new(workflow_id),
                workflow_type: W::workflow_type(),
                task_queue: TaskQueue::new(task_queue),
                input: crate::encode_payload_with_codec(&input, self.payload_codec)?,
            })
            .await?;
        Ok(outcome.run_id().clone())
    }

    pub async fn signal_workflow<T>(
        &self,
        workflow_id: impl Into<String>,
        signal_name: impl Into<String>,
        signal_id: impl Into<String>,
        payload: T,
    ) -> Result<crate::SignalWorkflowOutcome>
    where
        T: Serialize,
    {
        self.backend
            .signal_workflow(crate::SignalWorkflowRequest {
                namespace: self.namespace.clone(),
                workflow_id: WorkflowId::new(workflow_id),
                signal_id: crate::SignalId::new(signal_id),
                signal_name: crate::SignalName::new(signal_name),
                payload: crate::encode_payload_with_codec(&payload, self.payload_codec)?,
            })
            .await
    }

    pub async fn cancel_workflow(
        &self,
        workflow_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<crate::CancelWorkflowOutcome> {
        self.backend
            .cancel_workflow(crate::CancelWorkflowRequest {
                namespace: self.namespace.clone(),
                workflow_id: WorkflowId::new(workflow_id),
                reason: reason.into(),
            })
            .await
    }

    pub async fn query_projection<W>(
        &self,
        workflow_id: impl Into<String>,
    ) -> Result<Option<W::QueryState>>
    where
        W: Workflow,
    {
        match self
            .backend
            .query_projection(crate::QueryProjectionRequest {
                namespace: self.namespace.clone(),
                workflow_id: WorkflowId::new(workflow_id),
            })
            .await?
        {
            crate::QueryProjectionOutcome::Found { payload, .. } => {
                Ok(Some(crate::decode_payload::<W::QueryState>(&payload)?))
            }
            crate::QueryProjectionOutcome::NotFound
            | crate::QueryProjectionOutcome::NoProjection => Ok(None),
        }
    }
}

/// A worker split into disjoint state so its three loops can run at once.
///
/// [`Worker::run`] races a workflow loop, an activity loop, and a maintenance
/// loop. Only the workflow loop mutates anything (the execution cache and the
/// local-activity counter), so the split is what lets the other two borrow the
/// worker immutably at the same time. It is also why no loop needs
/// `tokio::spawn`: nothing crosses a task boundary, so [`DurableBackend`] keeps
/// its freedom from `Send` bounds and runtime-flavor coupling.
pub struct Worker<B>
where
    B: DurableBackend,
{
    shared: WorkerShared<B>,
    workflow: WorkflowState,
    shutdown: WorkerShutdown,
}

// Everything the three loops read concurrently: the backend handle, the
// registry, every tuning knob, and the interior-mutable metrics. Immutable
// after `build`, so all three loops hold `&WorkerShared` at once.
struct WorkerShared<B>
where
    B: DurableBackend,
{
    backend: B,
    namespace: Namespace,
    worker_id: WorkerId,
    workflow_task_queue: TaskQueue,
    activity_task_queue: TaskQueue,
    registry: Registry,
    // The claim RPCs' registered-name filters, materialised once at `build()`.
    // The registry is immutable after that, so walking its two `BTreeMap`s on
    // every claim only ever reproduces the same lists.
    //
    // This saves the key walk and nothing else. `ClaimWorkflowTaskOptions`
    // takes the list by value, so a claim still allocates one `Vec` plus one
    // `String` per registered name — the same allocation count as the
    // `keys().cloned().collect()` it replaced. Removing that would mean
    // changing `ClaimWorkflowTaskOptions::registered_workflow_types` to a
    // shared slice, a public API break across all three providers.
    registered_workflow_types: Vec<crate::WorkflowType>,
    registered_activity_names: Vec<crate::ActivityName>,
    history_chunk_events: usize,
    history_chunk_bytes: usize,
    payload_codec: crate::CodecId,
    nondeterminism_retry_backoff: Duration,
    workflow_task_concurrency: WorkflowTaskConcurrency,
    recovery_flow_control: RecoveryFlowControl,
    active_recoveries: Arc<AtomicUsize>,
    workflow_task_lease_duration: Duration,
    activity_task_lease_duration: Duration,
    activity_task_batch_size: usize,
    max_concurrent_activities: usize,
    max_concurrent_payload_hydrations: usize,
    activity_completion_batch_size: usize,
    max_local_activities_per_workflow_task: usize,
    max_cached_workflows: usize,
    run_timer_maintenance: bool,
    loop_config: WorkerLoopConfig,
    metrics: WorkerMetricsState,
    event_sink: Option<WorkerEventSink>,
}

// The only mutable worker state, and it belongs to the workflow loop alone: the
// cache of live workflow futures plus the local activities the last workflow
// stage drained. Activity execution and maintenance touch neither.
#[derive(Default)]
struct WorkflowState {
    cache: BTreeMap<RunId, CachedWorkflow>,
    // The cache's runs in least-recently-inserted order, keyed by the same
    // monotonic stamp the entry carries in `last_accessed_seq`. This is the
    // Rust shape of the TypeScript worker's insertion-ordered `Map` idiom
    // (`delete` then re-`set`, evict `keys().next()`): the victim is the
    // first key rather than the minimum of a scan.
    //
    // Not literally constant-time, and not literally independent of
    // `max_cached_workflows` — `BTreeMap` is O(log n) and the cache's own map
    // gets deeper as the bound rises. What goes away is the linear scan:
    // measured across a hundredfold rise in the bound, an evicting insert
    // moves 14.2 µs to 669 µs with the scan and stays flat with this index.
    //
    // `None` until the cache first overflows, because a worker below its
    // bound never evicts and must not pay to maintain an index it will never
    // read. Once built it is maintained for the worker's life; the invariant
    // from then on is exactly one entry per cached run, which is why every
    // cache insertion and removal goes through `insert_cached_workflow` and
    // `remove_cached_workflow` rather than touching `cache` directly.
    cache_order: Option<BTreeMap<u64, RunId>>,
    cache_access_seq: u64,
    completed_local_activity_tasks: usize,
}

// The workflow-task pipeline's view of the worker: shared config by reference,
// workflow state by exclusive reference. Constructed per call, so the borrow
// lives exactly as long as one workflow stage rather than for the whole run.
struct WorkflowWorker<'a, B>
where
    B: DurableBackend,
{
    shared: &'a WorkerShared<B>,
    state: &'a mut WorkflowState,
}

struct CachedWorkflow {
    future: Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
    last_event_id: EventId,
    next_command_seq: u64,
    default_activity_options: crate::ActivityOptions,
    // Ready events the committed task left unconsumed (for example a spawned
    // handle's completion the workflow has not awaited yet). They seed the
    // next task's context so the run stays cached; the next chunk starts
    // after `last_event_id`, so carried entries cannot be collected twice.
    unconsumed_indexes: crate::runtime::ReadyEventIndexes,
    // Monotonic worker counter stamped on insert; the bounded cache evicts
    // the minimum, giving LRU behavior because cached runs re-enter the
    // cache through insert after every task.
    last_accessed_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkflowTaskConcurrency {
    max_concurrent_workflow_tasks: usize,
    prefetch_limit: usize,
    commit_batch_size: usize,
    shard_filter: Option<Vec<ShardId>>,
}

impl Default for WorkflowTaskConcurrency {
    fn default() -> Self {
        Self {
            max_concurrent_workflow_tasks: 1,
            prefetch_limit: 1,
            commit_batch_size: 1,
            shard_filter: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RecoveryFlowControl {
    max_concurrent_recoveries: usize,
    replay_event_budget: usize,
    replay_byte_budget: usize,
    prefetch_chunks: usize,
    defer_delay: Duration,
}

impl Default for RecoveryFlowControl {
    fn default() -> Self {
        Self {
            max_concurrent_recoveries: usize::MAX,
            replay_event_budget: usize::MAX,
            replay_byte_budget: usize::MAX,
            prefetch_chunks: usize::MAX,
            defer_delay: Duration::from_millis(100),
        }
    }
}

// Holds one slot of the worker's cold-recovery concurrency budget. Dropping
// the guard releases the slot, so no error or early-return path in the
// prepare pipeline can leak `active_recoveries`. The counter lives behind an
// `Arc` because the guard must outlive individual `&mut self` borrows of the
// worker while prepare awaits backend calls.
struct RecoverySlotGuard {
    active_recoveries: Arc<AtomicUsize>,
}

impl Drop for RecoverySlotGuard {
    fn drop(&mut self) {
        // The guard's existence proves the counter was incremented at acquire,
        // so a plain decrement cannot underflow.
        self.active_recoveries.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug)]
struct RecoveryReplayBudget {
    remaining_events: usize,
    remaining_bytes: usize,
    remaining_chunks: usize,
}

impl RecoveryReplayBudget {
    fn new(flow_control: RecoveryFlowControl) -> Self {
        Self {
            remaining_events: flow_control.replay_event_budget,
            remaining_bytes: flow_control.replay_byte_budget,
            remaining_chunks: flow_control.prefetch_chunks,
        }
    }

    fn next_request_limits(
        &self,
        chunk_events: usize,
        chunk_bytes: usize,
    ) -> Option<(usize, usize)> {
        if self.remaining_events == 0 || self.remaining_bytes == 0 || self.remaining_chunks == 0 {
            return None;
        }

        Some((
            chunk_events.min(self.remaining_events),
            chunk_bytes.min(self.remaining_bytes),
        ))
    }

    fn record_chunk(&mut self, chunk: &crate::HistoryChunk) {
        self.remaining_chunks = self.remaining_chunks.saturating_sub(1);
        self.remaining_events = self.remaining_events.saturating_sub(chunk.events.len());
        let bytes = chunk
            .events
            .iter()
            .map(|event| crate::runtime::event_payload_len(&event.data).max(1))
            .sum::<usize>();
        self.remaining_bytes = self.remaining_bytes.saturating_sub(bytes);
    }
}

// One value per prepared task: boxing the prepared side would trade a
// per-task allocation for the lint.
#[allow(clippy::large_enum_variant)]
enum PreparedWorkflowTaskOutcome {
    Prepared(PreparedWorkflowTask),
    Deferred,
}

struct PreparedWorkflowTask {
    run_id: RunId,
    claim: crate::WorkflowTaskClaim,
    commit: WorkflowTaskCommit,
    future: Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
    runtime_appended_tail: EventId,
    next_command_seq: u64,
    default_activity_options: crate::ActivityOptions,
    unconsumed_indexes: crate::runtime::ReadyEventIndexes,
    terminal: bool,
}

enum WorkflowPollOutcome {
    Ready(Poll<Result<crate::PayloadRef>>),
    Deferred,
}

// What one claimed workflow task settled as, for the single-task path.
//
// The public `bool` cannot carry it, and the distinction is load-bearing: a
// deferred task consumed a claim without replaying anything, so counting it as
// a committed task over-reports progress. The single-task path did exactly
// that, while the batch path counted the same outcome as nothing; both now
// report it as deferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SingleWorkflowTaskOutcome {
    NoTask,
    // Committed, or conflicted and released by the provider. Both leave the
    // task settled against durable history.
    Settled,
    Deferred,
}

#[derive(Clone)]
enum ActivityFinish {
    Complete(crate::CompleteActivityRequest),
    Fail(crate::FailActivityRequest),
}

impl<B> Worker<B>
where
    B: DurableBackend,
{
    pub fn builder(backend: B) -> WorkerBuilder<B> {
        WorkerBuilder {
            backend,
            namespace: Namespace::default(),
            worker_id: WorkerId::new("worker"),
            workflow_task_queue: TaskQueue::default(),
            activity_task_queue: TaskQueue::default(),
            registry: Registry::default(),
            history_chunk_events: 128,
            history_chunk_bytes: 256 * 1024,
            nondeterminism_retry_backoff: Duration::from_secs(60),
            workflow_task_concurrency: WorkflowTaskConcurrency::default(),
            recovery_flow_control: RecoveryFlowControl::default(),
            workflow_task_lease_duration: DEFAULT_TASK_LEASE_DURATION,
            activity_task_lease_duration: DEFAULT_TASK_LEASE_DURATION,
            activity_task_batch_size: 1,
            max_concurrent_activities: 1,
            max_concurrent_payload_hydrations: DEFAULT_MAX_CONCURRENT_PAYLOAD_HYDRATIONS,
            activity_completion_batch_size: 1,
            max_local_activities_per_workflow_task: 0,
            max_cached_workflows: DEFAULT_MAX_CACHED_WORKFLOWS,
            run_timer_maintenance: true,
            loop_config: WorkerLoopConfig::default(),
            event_sink: None,
        }
    }

    /// Handle for requesting a graceful stop of [`Worker::run`].
    pub fn shutdown_handle(&self) -> WorkerShutdown {
        self.shutdown.clone()
    }

    /// Cumulative counters for everything this worker has done, under any
    /// driver.
    ///
    /// This is the production observability surface: [`Worker::run`] returns
    /// only `Result<()>` and this crate has no logging dependency, so without
    /// it a permanently poisoned run is retried on its backoff forever with no
    /// error, no log, and no metric. Pair it with
    /// [`WorkerBuilder::on_event`] when the individual occurrences matter and
    /// not just the totals.
    pub fn metrics(&self) -> WorkerMetrics {
        self.shared.metrics.snapshot()
    }

    /// Runs the worker until [`WorkerShutdown::shutdown`] is requested.
    ///
    /// Workflow processing, activity execution, and due maintenance run as
    /// three independent loops joined on one task rather than as three
    /// sequential stages of one pass. A slow activity therefore no longer holds
    /// up workflow commits on the same worker, a failing stage no longer
    /// suppresses the others — each loop owns its own idle and error backoff —
    /// and maintenance load tracks elapsed time instead of the task rate.
    ///
    /// The loops are joined, never spawned. Nothing crosses a task boundary, so
    /// the [`DurableBackend`] trait stays free of `Send` bounds and of any
    /// runtime-flavor coupling, and a worker still runs on a current-thread
    /// runtime. The cost is that the loops are cooperatively scheduled: a loop
    /// that makes progress on every pass hands its peers a poll on a bounded
    /// cadence, because a synchronous provider would otherwise let it run
    /// without ever returning `Pending`.
    ///
    /// No error kills the worker. Every loop catches what its stage raised,
    /// counts it in [`Worker::metrics`], emits [`WorkerEvent::LoopError`],
    /// waits out an exponential error backoff, and continues. Retrying forever
    /// rather than exiting after a fixed number of consecutive failures is
    /// deliberate: exiting cannot be right per-loop (a maintenance outage would
    /// kill workflow commits, or leave them running unobserved), and the
    /// failure count it replaced was never a signal anyone could see, because
    /// `run`'s `Err` is discarded by every caller that spawns it.
    ///
    /// The `Err` channel therefore has no producer today. It is kept because a
    /// loop that one day *does* fail terminally must stop its peers rather than
    /// leave a half-dead worker serving one queue — but note what stopping them
    /// would mean: `try_join3` cancels by **dropping** the peer futures at their
    /// next suspension point, so an activity executing at that moment is
    /// dropped mid-flight and its claim is only recovered when the lease
    /// expires, exactly as after a process crash. Draining the peers instead —
    /// the `Promise.allSettled` shape the TypeScript worker uses — is the right
    /// end state and is deliberately not built here, because there is no fatal
    /// condition yet for a deterministic test to exercise it against.
    ///
    /// Connection-pool sizing: each loop keeps at most one backend call in
    /// flight, so a running worker demands three pooled connections in steady
    /// state, plus one per activity that heartbeats while others run
    /// (heartbeats are issued by activity code, concurrently with the activity
    /// loop's own call). Size the pool at three or more per worker, or at
    /// `3 + max_concurrent_activities` if activities heartbeat. Below three the
    /// loops queue against each other and the activity loop can delay workflow
    /// commits — the coupling this split exists to remove.
    pub async fn run(&mut self) -> Result<()> {
        let Worker {
            shared,
            workflow,
            shutdown,
        } = self;
        let shared = &*shared;
        let shutdown = &*shutdown;
        futures::future::try_join3(
            shared.run_workflow_loop(workflow, shutdown),
            shared.run_activity_loop(shutdown),
            shared.run_maintenance_loop(shutdown),
        )
        .await?;
        Ok(())
    }

    fn workflow_worker(&mut self) -> WorkflowWorker<'_, B> {
        WorkflowWorker {
            shared: &self.shared,
            state: &mut self.workflow,
        }
    }

    pub async fn run_workflow_once(&mut self) -> Result<bool> {
        self.workflow_worker().run_workflow_once().await
    }

    /// Runs one workflow-task stage and reports how many tasks committed.
    ///
    /// A task that failed without committing (a nondeterministic replay, a
    /// caught workflow panic, an unsupported recorded version) is not an error
    /// of this call: it was already settled — nothing was written and its claim
    /// was released with the retry backoff — so the batch reports the work that
    /// did land. Only an error the stage could not settle per task, such as a
    /// failed claim or commit RPC, is returned.
    pub async fn run_workflow_batch_once(&mut self) -> Result<usize> {
        self.workflow_worker().run_workflow_batch_once().await
    }

    pub async fn run_activity_once(&mut self) -> Result<bool> {
        self.shared.run_activity_once().await
    }

    pub async fn run_activity_batch_once(&mut self) -> Result<usize> {
        self.shared.run_activity_batch_once().await
    }

    pub async fn run_timers_once(&mut self) -> Result<usize> {
        self.shared.run_timers_once().await
    }

    pub async fn run_activity_timeouts_once(&mut self) -> Result<usize> {
        self.shared.run_activity_timeouts_once().await
    }

    pub async fn run_due_maintenance_once(&mut self) -> Result<crate::RunDueMaintenanceOutcome> {
        self.shared.run_due_maintenance_once().await
    }

    pub async fn run_child_workflow_starts_once(&mut self) -> Result<usize> {
        self.shared.run_child_workflow_starts_once().await
    }

    pub async fn run_until_idle(&mut self) -> Result<WorkerRunStats> {
        self.run_until_idle_with(WorkerRunOptions::default()).await
    }

    pub async fn run_until_idle_with(&mut self, opts: WorkerRunOptions) -> Result<WorkerRunStats> {
        let mut stats = WorkerRunStats::default();
        for _ in 0..opts.max_iterations {
            if !self.run_pass_once(&mut stats).await? {
                return Ok(stats);
            }
        }

        Err(Error::Backend(format!(
            "worker did not become idle within {} iterations",
            opts.max_iterations
        )))
    }

    // One full work pass, sequential by construction: the deterministic driver
    // `run_until_idle` needs a single-threaded, single-pass schedule it can
    // reason about, which is the opposite of what the production `run` loop
    // needs. `run` no longer calls this. Returns whether any work was
    // performed.
    async fn run_pass_once(&mut self, stats: &mut WorkerRunStats) -> Result<bool> {
        let mut progressed = false;

        let workflow_stage = self.workflow_worker().run_workflow_stage_once().await?;
        if workflow_stage.committed > 0 {
            stats.workflow_tasks += workflow_stage.committed;
            progressed = true;
        }
        if workflow_stage.failed > 0 {
            // Not recorded progress — nothing was committed — but the pass did
            // consume a claim, so the driver must take another pass instead of
            // declaring the worker idle while the batch's neighbours are still
            // queued.
            //
            // Reporting progress here is also what makes `nondeterminism_retry_backoff`
            // load-bearing: a failed task never reaches the pass's idle wait, so
            // the release delay is the only thing keeping a poisoned run from
            // being re-claimed immediately and forever. That is why the builder
            // clamps it to `MIN_NONDETERMINISM_RETRY_BACKOFF`.
            stats.workflow_tasks_failed += workflow_stage.failed;
            progressed = true;
        }
        if workflow_stage.deferred > 0 {
            // Same reasoning, for a claim released unreplayed: the queue is not
            // empty, so the pass must not report idle.
            stats.workflow_tasks_deferred += workflow_stage.deferred;
            progressed = true;
        }
        let local_activity_tasks = self.workflow_worker().take_completed_local_activity_tasks();
        if local_activity_tasks > 0 {
            stats.activity_tasks += local_activity_tasks;
            progressed = true;
        }
        // Only the timer and activity-deadline scan is configurable; `SPEC.md`
        // §11 licenses exactly that, on the grounds that a timer service owns
        // the obligation.
        if self.shared.run_timer_maintenance {
            let maintenance = self.shared.run_due_maintenance_once().await?;
            if maintenance.timers_fired > 0 {
                stats.timers_fired += maintenance.timers_fired;
                progressed = true;
            }
            if maintenance.activities_timed_out > 0 {
                stats.activities_timed_out += maintenance.activities_timed_out;
                progressed = true;
            }
        }
        // Child dispatch is not configurable and stays where HEAD had it:
        // unconditional. Nothing else in a deployment drains the child-start
        // outbox — no service owns it the way a timer service owns due timers —
        // so a worker that stopped dispatching would strand every child
        // workflow forever rather than merely delaying it.
        let child_starts = self.shared.run_child_workflow_starts_once().await?;
        if child_starts > 0 {
            stats.child_workflow_starts_dispatched += child_starts;
            progressed = true;
        }
        let activity_tasks = self.shared.run_activity_batch_once().await?;
        if activity_tasks > 0 {
            stats.activity_tasks += activity_tasks;
            progressed = true;
        }

        Ok(progressed)
    }

    // Test-only visibility into the cache bound; hidden because integration
    // tests need it but it is not part of the supported API.
    #[doc(hidden)]
    pub fn cached_workflow_count(&self) -> usize {
        self.workflow.cache.len()
    }
}

impl<B> WorkerShared<B>
where
    B: DurableBackend,
{
    // Workflow processing, on its own error and idle budget. Owns the only
    // mutable worker state, which is why it is the one loop that takes a
    // `&mut` argument.
    async fn run_workflow_loop(
        &self,
        state: &mut WorkflowState,
        shutdown: &WorkerShutdown,
    ) -> Result<()> {
        self.run_task_loop(shutdown, WorkerLoop::Workflow, async || {
            let mut worker = WorkflowWorker {
                shared: self,
                state: &mut *state,
            };
            let stage = worker.run_workflow_stage_once().await?;
            // Drained rather than reported: the local activities a workflow
            // stage ran were already counted where they finished. Taking them
            // keeps the counter from growing across a run, and makes "the stage
            // did something" true when the only thing it did was drain them.
            let local_activities = worker.take_completed_local_activity_tasks();
            Ok(stage.consumed_a_claim() || local_activities > 0)
        })
        .await
    }

    // Activity execution, independent of workflow processing. A long-running
    // activity holds only this loop; workflow commits and maintenance keep
    // running beside it.
    async fn run_activity_loop(&self, shutdown: &WorkerShutdown) -> Result<()> {
        self.run_task_loop(shutdown, WorkerLoop::Activity, async || {
            Ok(self.run_activity_batch_once().await? > 0)
        })
        .await
    }

    /// Shared shape of the workflow and activity loops: poll, back off when
    /// idle, back off separately when the step failed. Each loop instance owns
    /// its own backoff state, so one stage's error budget cannot stop the
    /// other's — and one shape, so a fix to the pacing cannot land on one loop
    /// and miss the other.
    ///
    /// A loop that keeps making progress never reaches its idle wait, so it
    /// hands its peers a poll every [`PROGRESS_YIELD_PASSES`] productive
    /// passes. Without that, a saturated loop starves its peers outright on any
    /// provider whose calls resolve without suspending.
    async fn run_task_loop<F>(
        &self,
        shutdown: &WorkerShutdown,
        worker_loop: WorkerLoop,
        mut step: F,
    ) -> Result<()>
    where
        F: AsyncFnMut() -> Result<bool>,
    {
        let mut idle_wait = self.loop_config.idle_wait;
        let mut error_backoff = self.loop_config.error_backoff;
        let mut passes_since_yield = 0usize;
        loop {
            if shutdown.is_requested() {
                return Ok(());
            }
            match step().await {
                Ok(progressed) => {
                    error_backoff = self.loop_config.error_backoff;
                    if progressed {
                        idle_wait = self.loop_config.idle_wait;
                        passes_since_yield += 1;
                        if passes_since_yield >= PROGRESS_YIELD_PASSES {
                            passes_since_yield = 0;
                            if yield_to_peer_loops(shutdown).await == LoopWait::ShutdownRequested {
                                return Ok(());
                            }
                        }
                        continue;
                    }
                    // The idle and error waits below are themselves yields, so
                    // reaching either one discharges the progress-yield debt.
                    passes_since_yield = 0;
                    self.metrics.idle_waits.fetch_add(1, Ordering::Relaxed);
                    if self.wait_for_ready_or_shutdown(shutdown, idle_wait).await
                        == LoopWait::ShutdownRequested
                    {
                        return Ok(());
                    }
                    idle_wait = next_backoff(idle_wait, self.loop_config.max_idle_wait);
                }
                Err(err) => {
                    passes_since_yield = 0;
                    self.loop_error_counter(worker_loop)
                        .fetch_add(1, Ordering::Relaxed);
                    self.emit(WorkerEvent::LoopError {
                        worker_loop,
                        error: &err,
                    });
                    if sleep_or_shutdown(shutdown, error_backoff).await
                        == LoopWait::ShutdownRequested
                    {
                        return Ok(());
                    }
                    error_backoff = next_backoff(error_backoff, self.loop_config.max_error_backoff);
                }
            }
        }
    }

    fn loop_error_counter(&self, worker_loop: WorkerLoop) -> &AtomicU64 {
        match worker_loop {
            WorkerLoop::Workflow => &self.metrics.workflow_loop_errors,
            WorkerLoop::Activity => &self.metrics.activity_loop_errors,
            WorkerLoop::Maintenance => &self.metrics.maintenance_loop_errors,
        }
    }

    /// Interval-paced maintenance: due timers, activity start-to-close
    /// deadlines, and the child-workflow start outbox.
    ///
    /// Pacing by elapsed time keeps provider load proportional to the interval
    /// and the fleet size instead of to the task rate: on Postgres each of
    /// these is its own transaction, so scanning per pass made maintenance cost
    /// scale with throughput.
    ///
    /// The loop always runs, even with [`WorkerBuilder::run_timer_maintenance`]
    /// off, because only the timer and activity-deadline half is optional.
    /// `SPEC.md` §11 makes due-timer delivery a timer service's obligation and
    /// a worker's scan a convenience; it says nothing of the kind about the
    /// child-start outbox, and nothing else in a deployment drains that outbox.
    ///
    /// A scan that found work re-runs immediately, so a backlog drains at full
    /// speed. Consecutive empty scans double the interval to
    /// `max_maintenance_interval`. Every delay is jittered into `[0.5x, 1.5x)`
    /// by a generator seeded from the worker id, including the first one — a
    /// fleet started together would otherwise scan in lockstep forever.
    async fn run_maintenance_loop(&self, shutdown: &WorkerShutdown) -> Result<()> {
        let mut jitter = MaintenanceJitter::new(&self.worker_id);
        let mut error_backoff = self.loop_config.error_backoff;
        let mut interval = self.loop_config.maintenance_interval;
        // The first delay is a phase offset, not a warm-up: without it every
        // worker in a fleet scans at startup, which is exactly the synchronized
        // load the jitter exists to break up.
        let mut delay = jitter.next_delay(interval);

        loop {
            if sleep_or_shutdown(shutdown, delay).await == LoopWait::ShutdownRequested {
                return Ok(());
            }
            match self.run_maintenance_scan_once().await {
                Ok(scanned) => {
                    error_backoff = self.loop_config.error_backoff;
                    if scanned {
                        interval = self.loop_config.maintenance_interval;
                        delay = Duration::ZERO;
                        continue;
                    }
                    interval = next_backoff(interval, self.loop_config.max_maintenance_interval);
                    delay = jitter.next_delay(interval);
                }
                Err(err) => {
                    self.loop_error_counter(WorkerLoop::Maintenance)
                        .fetch_add(1, Ordering::Relaxed);
                    self.emit(WorkerEvent::LoopError {
                        worker_loop: WorkerLoop::Maintenance,
                        error: &err,
                    });
                    if sleep_or_shutdown(shutdown, error_backoff).await
                        == LoopWait::ShutdownRequested
                    {
                        return Ok(());
                    }
                    error_backoff = next_backoff(error_backoff, self.loop_config.max_error_backoff);
                    delay = Duration::ZERO;
                }
            }
        }
    }

    // One maintenance scan: due timers and activity deadlines in the provider's
    // combined call when this worker does timer maintenance at all, then the
    // child-start outbox, which it always does. Reports whether anything was
    // found, which is what decides between an immediate re-scan and the backoff.
    async fn run_maintenance_scan_once(&self) -> Result<bool> {
        let maintenance = if self.run_timer_maintenance {
            self.run_due_maintenance_once().await?
        } else {
            crate::RunDueMaintenanceOutcome::default()
        };
        let child_starts = self.run_child_workflow_starts_once().await?;
        self.emit(WorkerEvent::MaintenanceScanned {
            timers_fired: maintenance.timers_fired,
            activities_timed_out: maintenance.activities_timed_out,
            child_workflow_starts_dispatched: child_starts,
        });
        Ok(
            maintenance.timers_fired > 0
                || maintenance.activities_timed_out > 0
                || child_starts > 0,
        )
    }

    // Parks in the provider's readiness channel, which is a push wakeup where
    // the provider has one and a bounded sleep where it does not. A
    // `wait_for_ready` error is ignored: the wait is bounded either way, and a
    // provider that is genuinely failing surfaces through the claim call the
    // loop makes next.
    async fn wait_for_ready_or_shutdown(
        &self,
        shutdown: &WorkerShutdown,
        max_wait: Duration,
    ) -> LoopWait {
        let wait = self.backend.wait_for_ready(WaitForReadyRequest {
            namespace: self.namespace.clone(),
            workflow_task_queue: self.workflow_task_queue.clone(),
            activity_task_queue: self.activity_task_queue.clone(),
            max_wait,
        });
        wait_or_shutdown(shutdown, wait).await
    }

    fn emit(&self, event: WorkerEvent<'_>) {
        if let Some(sink) = &self.event_sink {
            sink(event);
        }
    }

    // Sorts a settled workflow-task failure into its cause. Called from the one
    // place every such failure passes through, so no path can count a panic as
    // a divergence.
    fn record_workflow_task_failure(&self, run_id: &RunId, err: &Error) {
        let counter = match err {
            Error::TaskPanic(_) => &self.metrics.workflow_tasks_panicked,
            Error::Nondeterminism(_) => &self.metrics.workflow_tasks_nondeterministic,
            Error::UnsupportedWorkflowVersion { .. } => {
                &self.metrics.workflow_tasks_unsupported_version
            }
            Error::PayloadEncode(_) | Error::PayloadDecode(_) => {
                &self.metrics.workflow_tasks_faulted
            }
            other => {
                // Everything else is a stage-level error the loop counts and
                // backs off from instead. The assertion keeps this split from
                // drifting away from the predicate that decides how the claim
                // was released. It compiles out in release, so it is a
                // test-and-CI guard rather than a production check — acceptable
                // because the two live six lines apart in one file and this is
                // the predicate's only caller.
                debug_assert!(
                    !fails_workflow_task_without_committing(other),
                    "a task-scoped failure reached the metrics without a counter"
                );
                return;
            }
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.emit(WorkerEvent::WorkflowTaskFailed { run_id, error: err });
    }

    fn record_workflow_task_deferred(&self, run_id: &RunId) {
        self.metrics
            .workflow_tasks_deferred
            .fetch_add(1, Ordering::Relaxed);
        self.emit(WorkerEvent::WorkflowTaskDeferred { run_id });
    }

    fn record_workflow_task_committed(&self, run_id: &RunId) {
        self.metrics
            .workflow_tasks_committed
            .fetch_add(1, Ordering::Relaxed);
        self.emit(WorkerEvent::WorkflowTaskCommitted { run_id });
    }

    // Commits a single prepared task and decides whether its future stays cached,
    // factored out so the single-claim path reuses the same logic the batch loop
    // applies per committed task.
    async fn commit_prepared_workflow_task(
        &self,
        prepared: PreparedWorkflowTask,
    ) -> Result<Option<CachedWorkflow>> {
        let runtime_appended_tail = prepared.runtime_appended_tail;
        let terminal = prepared.terminal;
        let unconsumed_indexes = prepared.unconsumed_indexes;
        let next_command_seq = prepared.next_command_seq;
        let default_activity_options = prepared.default_activity_options;
        let future = prepared.future;
        // Moved, not cloned: the run id is only needed for accounting, and
        // cloning one per committed task would put an allocation on the commit
        // hot path for the benefit of a sink that is usually absent.
        let run_id = prepared.run_id;
        let last_event_id = self
            .backend
            .commit_workflow_task(prepared.claim, prepared.commit)
            .await?;
        self.record_workflow_task_committed(&run_id);
        Ok(cache_entry_after_commit(
            terminal,
            runtime_appended_tail,
            last_event_id,
            future,
            next_command_seq,
            default_activity_options,
            unconsumed_indexes,
        ))
    }

    // Releases a claim whose commit failed, swallowing backpressure (the task will
    // be retried after the delay) and propagating every other error.
    //
    // Every workflow-task failure funnels through here, which is why it is also
    // where the failure is counted and reported: one site, so no path can
    // release a claim without the worker's accounting seeing it.
    async fn release_failed_workflow_task(
        &self,
        claim: crate::WorkflowTaskClaim,
        err: Error,
    ) -> Result<()> {
        if let Error::Backpressure { retry_after, .. } = &err {
            let delay = self.backpressure_delay(*retry_after);
            self.record_workflow_task_deferred(&claim.run_id);
            let _ = self
                .backend
                .release_workflow_task(claim, WorkflowTaskRelease::delayed(delay))
                .await;
            return Ok(());
        }
        let release = if fails_workflow_task_without_committing(&err) {
            WorkflowTaskRelease::delayed(self.nondeterminism_retry_backoff)
        } else {
            WorkflowTaskRelease::immediate()
        };
        self.record_workflow_task_failure(&claim.run_id, &err);
        let _ = self.backend.release_workflow_task(claim, release).await;
        Err(err)
    }

    async fn run_activity_once(&self) -> Result<bool> {
        let claimed = self
            .backend
            .claim_activity_task(
                self.worker_id.clone(),
                ClaimActivityOptions {
                    namespace: self.namespace.clone(),
                    task_queue: self.activity_task_queue.clone(),
                    registered_activity_names: self.registered_activity_names.clone(),
                    lease_duration: self.activity_task_lease_duration,
                },
            )
            .await?;

        let Some(claimed) = claimed else {
            return Ok(false);
        };

        self.run_claimed_activity(claimed).await?;
        Ok(true)
    }

    async fn run_activity_batch_once(&self) -> Result<usize> {
        let max_in_flight = self.max_concurrent_activities.max(1);
        let claim_batch = self.activity_task_batch_size.max(1);
        if max_in_flight == 1 && claim_batch == 1 {
            return self.run_activity_once().await.map(usize::from);
        }

        // Claim up to the concurrency bound, `claim_batch` tasks per claim
        // RPC; a short claim means the queue is drained for now.
        let mut claimed = Vec::new();
        while claimed.len() < max_in_flight {
            let want = claim_batch.min(max_in_flight - claimed.len());
            let batch = self
                .backend
                .claim_activity_tasks(
                    self.worker_id.clone(),
                    ClaimActivityTasksOptions {
                        claim: ClaimActivityOptions {
                            namespace: self.namespace.clone(),
                            task_queue: self.activity_task_queue.clone(),
                            registered_activity_names: self.registered_activity_names.clone(),
                            lease_duration: self.activity_task_lease_duration,
                        },
                        limit: want,
                    },
                )
                .await?;
            let got = batch.len();
            claimed.extend(batch);
            if got < want {
                break;
            }
        }
        if claimed.is_empty() {
            return Ok(0);
        }
        self.execute_claimed_activities(claimed).await
    }

    async fn run_claimed_activity(&self, claimed: crate::ClaimedActivityTask) -> Result<()> {
        let finish = self.start_claimed_activity(claimed)?.await;
        self.finish_activity(finish).await
    }

    // Executes claimed activities concurrently on the worker task and reports
    // each completion as it finishes.
    //
    // This still drains every claimed execution before returning, so a
    // long-running activity holds the *activity loop* until it finishes. Under
    // `Worker::run` that is all it holds: workflow tasks and maintenance are
    // peer loops, so they keep committing beside it. A CPU-bound activity that
    // never yields is the remaining exception — it starves its concurrent
    // neighbours and its peer loops alike, because nothing can preempt a future
    // that never returns `Pending`.
    async fn execute_claimed_activities(
        &self,
        claimed: Vec<crate::ClaimedActivityTask>,
    ) -> Result<usize> {
        let mut executions = FuturesUnordered::new();
        for task in claimed {
            executions.push(self.start_claimed_activity(task)?);
        }
        let flush_size = self.activity_completion_batch_size.max(1);
        let mut completed = 0usize;
        let mut finishes = Vec::new();
        while let Some(finish) = executions.next().await {
            finishes.push(finish);
            completed += 1;
            // Flush as soon as a completion batch fills so fast activities
            // reach the backend while slower ones are still running.
            if finishes.len() >= flush_size {
                self.finish_activities(std::mem::take(&mut finishes))
                    .await?;
            }
        }
        if !finishes.is_empty() {
            self.finish_activities(finishes).await?;
        }
        Ok(completed)
    }

    // Builds the self-contained execution future for one claimed activity;
    // the future owns its heartbeat context so many can run concurrently.
    fn start_claimed_activity(
        &self,
        claimed: crate::ClaimedActivityTask,
    ) -> Result<impl Future<Output = ActivityFinish> + Send + 'static> {
        let registration = self
            .registry
            .activity(&claimed.task.activity_name)
            .ok_or_else(|| Error::ActivityNotRegistered(claimed.task.activity_name.clone()))?;
        let heartbeat_backend = self.backend.clone();
        let heartbeat_claim = claimed.claim.clone();
        let activity_context = crate::runtime::ActivityRuntimeContext::new(move || {
            let backend = heartbeat_backend.clone();
            let claim = heartbeat_claim.clone();
            Box::pin(async move {
                backend
                    .heartbeat_activity(ActivityHeartbeatRequest { claim })
                    .await
            })
        });
        let mut future = registration.run(claimed.task.input, self.payload_codec);
        let claim = claimed.claim;
        Ok(async move {
            let result = std::future::poll_fn(|cx| {
                // A panic in activity code must fail that activity, not the
                // worker: uncaught it unwinds through
                // `execute_claimed_activities`, dropping every concurrently
                // claimed activity's execution with it and stranding their
                // claims until the leases expire. The panic maps to the
                // ordinary activity failure path with a retryable
                // `DurableFailure`, so the activity's retry policy decides
                // whether it runs again.
                //
                // `AssertUnwindSafe` asserts only that nothing the closure
                // touches is read again after a caught unwind: `future` is a
                // possibly poisoned state machine that `poll_fn` never polls
                // again once it returns `Ready`, and `activity_context` is
                // immutable (`&`) and only supplies the heartbeat callback.
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    poll_with_activity_context(&activity_context, || future.as_mut().poll(cx))
                })) {
                    Ok(poll) => poll,
                    Err(payload) => {
                        Poll::Ready(Err(Error::Application(crate::DurableFailure::new(
                            "durust.activity_panic",
                            format!("activity panicked: {}", panic_message(payload.as_ref())),
                        ))))
                    }
                }
            })
            .await;
            match result {
                Ok(result) => ActivityFinish::Complete(CompleteActivityRequest { claim, result }),
                Err(err) => ActivityFinish::Fail(FailActivityRequest {
                    claim,
                    failure: err.durable_failure(),
                }),
            }
        })
    }

    async fn finish_activity(&self, finish: ActivityFinish) -> Result<()> {
        match finish {
            ActivityFinish::Complete(req) => {
                self.metrics
                    .activity_tasks_completed
                    .fetch_add(1, Ordering::Relaxed);
                self.emit(WorkerEvent::ActivityTaskCompleted {
                    activity_id: &req.claim.activity_id,
                });
                self.backend.complete_activity(req).await?;
            }
            ActivityFinish::Fail(req) => {
                self.metrics
                    .activity_tasks_failed
                    .fetch_add(1, Ordering::Relaxed);
                self.emit(WorkerEvent::ActivityTaskFailed {
                    activity_id: &req.claim.activity_id,
                    failure: &req.failure,
                });
                self.backend.fail_activity(req).await?;
            }
        }
        Ok(())
    }

    async fn finish_activities(&self, finishes: Vec<ActivityFinish>) -> Result<()> {
        let batch_size = self.activity_completion_batch_size.max(1);
        for chunk in finishes.chunks(batch_size) {
            if chunk
                .iter()
                .all(|finish| matches!(finish, ActivityFinish::Complete(_)))
            {
                let completions = chunk
                    .iter()
                    .filter_map(|finish| match finish {
                        ActivityFinish::Complete(req) => Some(req.clone()),
                        ActivityFinish::Fail(_) => None,
                    })
                    .collect::<Vec<_>>();
                for completion in &completions {
                    self.metrics
                        .activity_tasks_completed
                        .fetch_add(1, Ordering::Relaxed);
                    self.emit(WorkerEvent::ActivityTaskCompleted {
                        activity_id: &completion.claim.activity_id,
                    });
                }
                let results = self
                    .backend
                    .complete_activity_tasks(crate::CompleteActivityTasksRequest { completions })
                    .await?;
                for result in results {
                    result.result?;
                }
            } else {
                for finish in chunk {
                    self.finish_activity(finish.clone()).await?;
                }
            }
        }
        Ok(())
    }

    async fn run_timers_once(&self) -> Result<usize> {
        let now = self.backend.current_time().await?;
        let outcome = self
            .backend
            .fire_due_timers(FireDueTimersRequest {
                namespace: self.namespace.clone(),
                now,
                limit: 1024,
            })
            .await?;
        Ok(outcome.fired)
    }

    async fn run_activity_timeouts_once(&self) -> Result<usize> {
        let now = self.backend.current_time().await?;
        let outcome = self
            .backend
            .timeout_due_activities(TimeoutDueActivitiesRequest {
                namespace: self.namespace.clone(),
                now,
                limit: 1024,
            })
            .await?;
        Ok(outcome.timed_out)
    }

    async fn run_due_maintenance_once(&self) -> Result<crate::RunDueMaintenanceOutcome> {
        let now = self.backend.current_time().await?;
        self.metrics
            .maintenance_scans
            .fetch_add(1, Ordering::Relaxed);
        let outcome = self
            .backend
            .run_due_maintenance(RunDueMaintenanceRequest {
                namespace: self.namespace.clone(),
                now,
                timer_limit: 1024,
                activity_limit: 1024,
            })
            .await?;
        self.metrics.timers_fired.fetch_add(
            u64::try_from(outcome.timers_fired).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.metrics.activities_timed_out.fetch_add(
            u64::try_from(outcome.activities_timed_out).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(outcome)
    }

    async fn run_child_workflow_starts_once(&self) -> Result<usize> {
        let outcome = self
            .backend
            .dispatch_child_workflow_starts(crate::DispatchChildWorkflowStartsRequest {
                namespace: self.namespace.clone(),
                limit: 1024,
            })
            .await?;
        self.metrics.child_workflow_starts_dispatched.fetch_add(
            u64::try_from(outcome.dispatched).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(outcome.dispatched)
    }

    async fn stream_history_chunk(
        &self,
        run_id: RunId,
        after_event_id: EventId,
        up_to_event_id: EventId,
    ) -> Result<crate::HistoryChunk> {
        if after_event_id >= up_to_event_id {
            return Ok(crate::HistoryChunk {
                events: Vec::new(),
                last_event_id: after_event_id,
                has_more: false,
            });
        }
        self.backend
            .stream_history_for_replay(crate::StreamHistoryRequest {
                run_id,
                after_event_id,
                up_to_event_id,
                max_events: self.history_chunk_events,
                max_bytes: self.history_chunk_bytes,
            })
            .await
    }

    async fn claim_history_chunk(
        &self,
        claimed: &crate::ClaimedWorkflowTask,
        after_event_id: EventId,
    ) -> Result<crate::HistoryChunk> {
        if after_event_id >= claimed.replay_target_event_id {
            return Ok(crate::HistoryChunk {
                events: Vec::new(),
                last_event_id: after_event_id,
                has_more: false,
            });
        }
        if let Some(chunk) = prefetched_claim_history_chunk(claimed, after_event_id) {
            return Ok(chunk);
        }
        self.stream_history_chunk(
            claimed.run_id.clone(),
            after_event_id,
            claimed.replay_target_event_id,
        )
        .await
    }

    async fn claim_recovery_history_chunk(
        &self,
        claimed: &crate::ClaimedWorkflowTask,
        after_event_id: EventId,
        budget: &mut RecoveryReplayBudget,
    ) -> Result<Option<crate::HistoryChunk>> {
        if after_event_id >= claimed.replay_target_event_id {
            return Ok(Some(crate::HistoryChunk {
                events: Vec::new(),
                last_event_id: after_event_id,
                has_more: false,
            }));
        }
        let Some((max_events, max_bytes)) =
            budget.next_request_limits(self.history_chunk_events, self.history_chunk_bytes)
        else {
            return Ok(None);
        };
        if let Some(chunk) =
            prefetched_claim_history_chunk_bounded(claimed, after_event_id, max_events, max_bytes)
        {
            budget.record_chunk(&chunk);
            return Ok(Some(chunk));
        }
        self.stream_recovery_history_chunk(
            claimed.run_id.clone(),
            after_event_id,
            claimed.replay_target_event_id,
            budget,
        )
        .await
    }

    async fn stream_recovery_history_chunk(
        &self,
        run_id: RunId,
        after_event_id: EventId,
        up_to_event_id: EventId,
        budget: &mut RecoveryReplayBudget,
    ) -> Result<Option<crate::HistoryChunk>> {
        if after_event_id >= up_to_event_id {
            return Ok(Some(crate::HistoryChunk {
                events: Vec::new(),
                last_event_id: after_event_id,
                has_more: false,
            }));
        }

        let Some((max_events, max_bytes)) =
            budget.next_request_limits(self.history_chunk_events, self.history_chunk_bytes)
        else {
            return Ok(None);
        };

        let chunk = self
            .backend
            .stream_history_for_replay(crate::StreamHistoryRequest {
                run_id,
                after_event_id,
                up_to_event_id,
                max_events,
                max_bytes,
            })
            .await?;
        budget.record_chunk(&chunk);
        Ok(Some(chunk))
    }

    async fn poll_until_history_blocked_or_ready(
        &self,
        run_id: &RunId,
        claimed: &crate::ClaimedWorkflowTask,
        future: &mut Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
        context: &mut crate::runtime::RuntimeContext,
        replay_target_event_id: EventId,
        mut recovery_budget: Option<&mut RecoveryReplayBudget>,
    ) -> Result<WorkflowPollOutcome> {
        loop {
            // `?` on a caught panic: returning here drops the in-progress
            // context untouched, so nothing this attempt accumulated is read
            // or committed.
            let poll = poll_cached(future, context)?;
            let signal_requests = context.take_signal_requests();
            if !signal_requests.is_empty() {
                // The names move into the inbox requests; only the command ids
                // are kept to hand the records back.
                let mut command_ids = Vec::with_capacity(signal_requests.len());
                let inbox_requests = signal_requests
                    .into_iter()
                    .map(|request| {
                        command_ids.push(request.command_id);
                        ReadSignalInboxRequest {
                            run_id: run_id.clone(),
                            signal_name: request.signal_name,
                        }
                    })
                    .collect::<Vec<_>>();
                let signals = self
                    .backend
                    .read_signal_inboxes(ReadSignalInboxesRequest {
                        requests: inbox_requests,
                    })
                    .await?;
                if signals.len() != command_ids.len() {
                    return Err(Error::Backend(format!(
                        "backend returned {} signal inbox records for {} requests",
                        signals.len(),
                        command_ids.len()
                    )));
                }
                // Only an accepted record counts as progress: the runtime
                // rejects duplicate deliveries of one inbox record within a
                // task, and re-polling on a rejected record would re-request
                // and re-read the same record forever.
                let mut fulfilled = false;
                for (command_id, signal) in command_ids.into_iter().zip(signals) {
                    let signal = signal.map(|signal| crate::runtime::SignalInboxRecordForRuntime {
                        signal_id: signal.signal_id,
                        signal_name: signal.signal_name,
                        payload: signal.payload,
                    });
                    fulfilled |= context.fulfill_signal_request(command_id, signal);
                }
                if fulfilled {
                    continue;
                }
            }
            let payload_requests = context.take_payload_hydration_requests();
            if !payload_requests.is_empty() {
                // One poll can block on every item of a fanout at once, and
                // those blob reads are independent. Draining them one at a
                // time costs one provider round trip per item on the critical
                // path of a single workflow task. Bounded so a wide map does
                // not open an unbounded number of provider requests.
                let limit = self.max_concurrent_payload_hydrations.max(1);
                let mut queued = payload_requests.into_iter();
                let mut in_flight = FuturesUnordered::new();
                loop {
                    while in_flight.len() < limit {
                        let Some(request) = queued.next() else { break };
                        let payload = request.payload.clone();
                        let hydrate = match request.kind {
                            crate::runtime::PayloadHydrationKind::Payload => {
                                self.backend.hydrate_payload(payload)
                            }
                            crate::runtime::PayloadHydrationKind::ActivityMapResultManifest => {
                                self.backend.hydrate_activity_map_result_manifest(payload)
                            }
                            crate::runtime::PayloadHydrationKind::ChildWorkflowMapResultManifest => {
                                self.backend
                                    .hydrate_child_workflow_map_result_manifest(payload)
                            }
                        };
                        in_flight.push(async move { (request, hydrate.await) });
                    }
                    let Some((request, hydrated)) = in_flight.next().await else {
                        break;
                    };
                    context.fulfill_payload_hydration(request, hydrated?)?;
                }
                continue;
            }
            let Some(after_event_id) = context.needs_more_history_after() else {
                return Ok(WorkflowPollOutcome::Ready(poll));
            };
            let chunk = match recovery_budget.as_deref_mut() {
                Some(budget) => {
                    let Some(chunk) = self
                        .claim_recovery_history_chunk(claimed, after_event_id, budget)
                        .await?
                    else {
                        return Ok(WorkflowPollOutcome::Deferred);
                    };
                    chunk
                }
                None => {
                    self.stream_history_chunk(
                        run_id.clone(),
                        after_event_id,
                        replay_target_event_id,
                    )
                    .await?
                }
            };
            if chunk.events.is_empty() && after_event_id < replay_target_event_id {
                return Err(Error::Backend(format!(
                    "history stream ended at event {after_event_id} before replay target {replay_target_event_id}"
                )));
            }
            context.append_replay_events(chunk.events, chunk.last_event_id);
        }
    }

    async fn defer_workflow_task(
        &self,
        claim: crate::WorkflowTaskClaim,
        delay: Duration,
    ) -> Result<()> {
        self.record_workflow_task_deferred(&claim.run_id);
        self.backend
            .release_workflow_task(claim, WorkflowTaskRelease::delayed(delay))
            .await
    }

    // A batch prepares its tasks concurrently, so several cold recoveries can
    // be admitted from one stage and this is what bounds them. Claimed with a
    // compare-and-swap rather than load-then-add so the bound holds however
    // the acquisitions interleave.
    fn try_acquire_recovery(&self) -> Option<RecoverySlotGuard> {
        let limit = self.recovery_flow_control.max_concurrent_recoveries;
        self.active_recoveries
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |active| {
                (active < limit).then_some(active + 1)
            })
            .ok()?;
        Some(RecoverySlotGuard {
            active_recoveries: Arc::clone(&self.active_recoveries),
        })
    }

    fn backpressure_delay(&self, retry_after: Duration) -> Duration {
        if retry_after == Duration::ZERO {
            self.recovery_flow_control.defer_delay
        } else {
            retry_after
        }
    }

    async fn hydrate_payload_for_decode(
        &self,
        payload: crate::PayloadRef,
    ) -> Result<crate::PayloadRef> {
        let payload = self.backend.hydrate_payload(payload).await?;
        if matches!(payload, crate::PayloadRef::Blob { .. }) {
            return Err(Error::PayloadDecode(
                "backend returned an unresolved blob for workflow input".to_owned(),
            ));
        }
        Ok(payload)
    }

    // Single reconciliation point for claim ownership: every error escaping
    // the inner pipeline releases the claim here, so no fallible await between
    // claim and commit can strand the run until its lease expires.
    // `now` is `None` for a single claimed task, which reads the clock here:
    // the read is a fallible await taken while the claim is held, so it belongs
    // inside this funnel like every other step. A batch passes one reading in
    // for all its tasks and releases the batch itself.
    async fn prepare_claimed_workflow_task(
        &self,
        claimed: crate::ClaimedWorkflowTask,
        cached: Option<CachedWorkflow>,
        now: Option<crate::TimestampMs>,
    ) -> Result<PreparedWorkflowTaskOutcome> {
        let claim_for_release = claimed.claim.clone();
        let now = match now {
            Some(now) => Ok(now),
            None => self.backend.current_time().await,
        };
        let now = match now {
            Ok(now) => now,
            Err(err) => {
                self.release_failed_workflow_task(claim_for_release, err)
                    .await?;
                return Ok(PreparedWorkflowTaskOutcome::Deferred);
            }
        };
        match self
            .prepare_claimed_workflow_task_inner(claimed, cached, now)
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(err) => {
                self.release_failed_workflow_task(claim_for_release, err)
                    .await?;
                Ok(PreparedWorkflowTaskOutcome::Deferred)
            }
        }
    }

    // Errors returned here leave the claim held; the caller releases it. The
    // recovery slot is a drop guard, so `?` cannot leak it.
    async fn prepare_claimed_workflow_task_inner(
        &self,
        claimed: crate::ClaimedWorkflowTask,
        cached: Option<CachedWorkflow>,
        now: crate::TimestampMs,
    ) -> Result<PreparedWorkflowTaskOutcome> {
        let mut load_full_history = false;
        if let Some(mut cached) = cached {
            let chunk = self
                .claim_history_chunk(&claimed, cached.last_event_id)
                .await?;
            let mut context = crate::runtime::RuntimeContext::new(
                claimed.run_id.clone(),
                self.workflow_task_queue.clone(),
                self.activity_task_queue.clone(),
                self.payload_codec,
                now,
                chunk.events,
                cached.default_activity_options,
                cached.next_command_seq,
                chunk.last_event_id,
                claimed.replay_target_event_id,
                cached.unconsumed_indexes,
            );
            let poll = self
                .poll_until_history_blocked_or_ready(
                    &claimed.run_id,
                    &claimed,
                    &mut cached.future,
                    &mut context,
                    claimed.replay_target_event_id,
                    None,
                )
                .await?;
            match poll {
                WorkflowPollOutcome::Ready(_) if context.replay_window_overrun() => {
                    // A change-marker API ran past the loaded window, so the
                    // cached future's state is unusable: the run cold-replays
                    // below with its whole history loaded.
                    load_full_history = true;
                }
                WorkflowPollOutcome::Ready(poll) => {
                    return self
                        .prepare_workflow_poll(claimed, cached.future, context, poll)
                        .await
                        .map(PreparedWorkflowTaskOutcome::Prepared);
                }
                WorkflowPollOutcome::Deferred => {
                    // Deferred is only produced under a recovery budget and the
                    // cached path polls without one; if a budget is ever added
                    // here, this arm must release the claim the way the
                    // cold-path defer does or the claim leaks until its lease
                    // expires.
                    debug_assert!(
                        false,
                        "cached-path workflow poll deferred without a recovery budget"
                    );
                    return Ok(PreparedWorkflowTaskOutcome::Deferred);
                }
            }
        }

        let is_recovery = claimed.replay_target_event_id > EventId(1);
        let _recovery_slot = if is_recovery {
            let Some(slot) = self.try_acquire_recovery() else {
                self.defer_workflow_task(claimed.claim, self.recovery_flow_control.defer_delay)
                    .await?;
                return Ok(PreparedWorkflowTaskOutcome::Deferred);
            };
            Some(slot)
        } else {
            None
        };
        let mut recovery_budget =
            is_recovery.then(|| RecoveryReplayBudget::new(self.recovery_flow_control));
        let Some(registration) = self.registry.workflow(&claimed.workflow_type) else {
            return Err(Error::WorkflowNotRegistered(claimed.workflow_type.clone()));
        };
        loop {
            let mut first_chunk = match self
                .load_cold_history(&claimed, load_full_history, recovery_budget.as_mut())
                .await?
            {
                Some(chunk) => chunk,
                None => {
                    self.defer_workflow_task(claimed.claim, self.recovery_flow_control.defer_delay)
                        .await?;
                    return Ok(PreparedWorkflowTaskOutcome::Deferred);
                }
            };
            let last_loaded_event_id = first_chunk.last_event_id;
            let (input, replay_events) =
                split_start_event(std::mem::take(&mut first_chunk.events))?;
            let input = self.hydrate_payload_for_decode(input).await?;
            let mut future = registration.run(input, self.payload_codec);
            let mut context = crate::runtime::RuntimeContext::new(
                claimed.run_id.clone(),
                self.workflow_task_queue.clone(),
                self.activity_task_queue.clone(),
                self.payload_codec,
                now,
                replay_events,
                crate::ActivityOptions::default(),
                0,
                last_loaded_event_id,
                claimed.replay_target_event_id,
                crate::runtime::ReadyEventIndexes::default(),
            );
            let poll = self
                .poll_until_history_blocked_or_ready(
                    &claimed.run_id,
                    &claimed,
                    &mut future,
                    &mut context,
                    claimed.replay_target_event_id,
                    recovery_budget.as_mut(),
                )
                .await?;
            match poll {
                WorkflowPollOutcome::Ready(_) if context.replay_window_overrun() => {
                    if load_full_history {
                        return Err(Error::Backend(format!(
                            "replay of run {} overran a fully loaded history window",
                            claimed.run_id
                        )));
                    }
                    load_full_history = true;
                    // The reload starts from event zero and the first attempt's
                    // chunks are discarded, so it gets a fresh budget: charging
                    // both would defer a run whose whole history fits the
                    // budget on every claim.
                    recovery_budget =
                        is_recovery.then(|| RecoveryReplayBudget::new(self.recovery_flow_control));
                }
                WorkflowPollOutcome::Ready(poll) => {
                    return self
                        .prepare_workflow_poll(claimed, future, context, poll)
                        .await
                        .map(PreparedWorkflowTaskOutcome::Prepared);
                }
                WorkflowPollOutcome::Deferred => {
                    self.defer_workflow_task(claimed.claim, self.recovery_flow_control.defer_delay)
                        .await?;
                    return Ok(PreparedWorkflowTaskOutcome::Deferred);
                }
            }
        }
    }

    // The history a cold replay starts from: the first chunk, or every chunk
    // up to the replay target when a change-marker API overran a partial
    // window. `None` means the recovery budget ran out and the task defers.
    async fn load_cold_history(
        &self,
        claimed: &crate::ClaimedWorkflowTask,
        load_full_history: bool,
        mut recovery_budget: Option<&mut RecoveryReplayBudget>,
    ) -> Result<Option<crate::HistoryChunk>> {
        let mut loaded = crate::HistoryChunk {
            events: Vec::new(),
            last_event_id: EventId::ZERO,
            has_more: true,
        };
        loop {
            let chunk = match recovery_budget.as_deref_mut() {
                Some(budget) => {
                    let Some(chunk) = self
                        .claim_recovery_history_chunk(claimed, loaded.last_event_id, budget)
                        .await?
                    else {
                        return Ok(None);
                    };
                    chunk
                }
                None => {
                    self.claim_history_chunk(claimed, loaded.last_event_id)
                        .await?
                }
            };
            if chunk.events.is_empty() && loaded.last_event_id < claimed.replay_target_event_id {
                return Err(Error::Backend(format!(
                    "history stream ended at event {} before replay target {}",
                    loaded.last_event_id, claimed.replay_target_event_id
                )));
            }
            loaded.events.extend(chunk.events);
            loaded.last_event_id = chunk.last_event_id;
            loaded.has_more = chunk.has_more;
            if !load_full_history || loaded.last_event_id >= claimed.replay_target_event_id {
                return Ok(Some(loaded));
            }
        }
    }

    async fn prepare_workflow_poll(
        &self,
        claimed: crate::ClaimedWorkflowTask,
        future: Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
        mut context: crate::runtime::RuntimeContext,
        poll: Poll<Result<crate::PayloadRef>>,
    ) -> Result<PreparedWorkflowTask> {
        let poll_reached_terminal_state = match &poll {
            Poll::Pending => false,
            Poll::Ready(Ok(_)) => true,
            Poll::Ready(Err(err)) => !fails_workflow_task_without_committing(err),
        };
        if poll_reached_terminal_state {
            self.reject_terminal_with_unreplayed_command_events(&claimed, &mut context)
                .await?;
        }
        let next_command_seq = context.next_command_seq();
        let unconsumed_indexes = context.take_unconsumed_ready_event_indexes();
        let parts = context.into_commit_parts();
        let default_activity_options = parts.default_activity_options.clone();
        let mut append_events = parts.append_events;
        let mut terminal = false;

        match poll {
            Poll::Ready(Ok(result)) => {
                append_events.push(NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
                    result,
                }));
                terminal = true;
            }
            Poll::Ready(Err(err)) => {
                if let Error::ContinueAsNew { input } = err {
                    append_events.push(NewHistoryEvent::new(
                        HistoryEventData::WorkflowContinuedAsNew { input },
                    ));
                    terminal = true;
                } else if fails_workflow_task_without_committing(&err) {
                    return Err(err);
                } else {
                    append_events.push(NewHistoryEvent::new(HistoryEventData::WorkflowFailed {
                        failure: err.durable_failure(),
                    }));
                    terminal = true;
                }
            }
            Poll::Pending => {}
        }
        let runtime_appended_tail = EventId(
            claimed
                .replay_target_event_id
                .0
                .saturating_add(u64::try_from(append_events.len()).unwrap_or(u64::MAX)),
        );
        Ok(PreparedWorkflowTask {
            run_id: claimed.run_id,
            claim: claimed.claim,
            commit: WorkflowTaskCommit {
                append_events,
                upsert_waits: parts.upsert_waits,
                schedule_activities: parts.schedule_activities,
                schedule_activity_maps: parts.schedule_activity_maps,
                schedule_child_workflow_maps: parts.schedule_child_workflow_maps,
                start_child_workflows: parts.start_child_workflows,
                consume_signals: parts.consume_signals,
                delete_waits: parts.delete_waits,
                cancel_commands: parts.cancel_commands,
                query_projection: parts.query_projection,
            },
            future,
            runtime_appended_tail,
            next_command_seq,
            default_activity_options,
            unconsumed_indexes,
            terminal,
        })
    }

    // A workflow that reaches a terminal state while un-replayed command
    // events remain in history diverged from its recording (for example a
    // removed trailing command); committing the terminal event would
    // silently corrupt replay, so the task fails with nondeterminism and the
    // claim is released with backoff. Unconsumed ready events are legal
    // (fire-and-forget completions), so only command events count; unloaded
    // chunks are streamed in to check the rest of history.
    async fn reject_terminal_with_unreplayed_command_events(
        &self,
        claimed: &crate::ClaimedWorkflowTask,
        context: &mut crate::runtime::RuntimeContext,
    ) -> Result<()> {
        loop {
            if let Some((event_id, event_type)) = context.unreplayed_command_event() {
                return Err(Error::Nondeterminism(format!(
                    "workflow reached a terminal state while command event {event_type:?} at event {event_id} was not replayed"
                )));
            }
            let Some(after_event_id) = context.unloaded_history_after() else {
                return Ok(());
            };
            let chunk = self
                .stream_history_chunk(
                    claimed.run_id.clone(),
                    after_event_id,
                    claimed.replay_target_event_id,
                )
                .await?;
            if chunk.events.is_empty() {
                return Err(Error::Backend(format!(
                    "history stream ended at event {after_event_id} before replay target {}",
                    claimed.replay_target_event_id
                )));
            }
            context.append_replay_events(chunk.events, chunk.last_event_id);
        }
    }
}

impl<B> WorkflowWorker<'_, B>
where
    B: DurableBackend,
{
    // Whether a task was claimed at all, which is the shape the deterministic
    // single-task driver has always reported.
    async fn run_workflow_once(&mut self) -> Result<bool> {
        Ok(self.run_workflow_once_outcome().await? != SingleWorkflowTaskOutcome::NoTask)
    }

    async fn run_workflow_once_outcome(&mut self) -> Result<SingleWorkflowTaskOutcome> {
        let claim = self
            .shared
            .backend
            .claim_workflow_task(
                self.shared.worker_id.clone(),
                ClaimWorkflowTaskOptions {
                    namespace: self.shared.namespace.clone(),
                    task_queue: self.shared.workflow_task_queue.clone(),
                    registered_workflow_types: self.shared.registered_workflow_types.clone(),
                    lease_duration: self.shared.workflow_task_lease_duration,
                },
            )
            .await?;

        let Some(claimed) = claim else {
            return Ok(SingleWorkflowTaskOutcome::NoTask);
        };

        self.run_claimed_workflow_task(claimed).await
    }

    // The committed count [`Worker::run_workflow_batch_once`] publishes; the
    // failed and deferred counts stay behind `run_workflow_stage_once`, which is
    // what the pass driver and the workflow loop need.
    async fn run_workflow_batch_once(&mut self) -> Result<usize> {
        Ok(self.run_workflow_stage_once().await?.committed)
    }

    // One workflow-task stage, with the failed and deferred counts the pass
    // driver and the workflow loop need and the public `usize` cannot carry.
    async fn run_workflow_stage_once(&mut self) -> Result<WorkflowStageOutcome> {
        let limit = self
            .shared
            .workflow_task_concurrency
            .prefetch_limit
            .min(
                self.shared
                    .workflow_task_concurrency
                    .max_concurrent_workflow_tasks,
            )
            .max(1);
        if limit == 1 && self.shared.workflow_task_concurrency.shard_filter.is_none() {
            // `run_workflow_once` is also the deterministic single-task driver
            // tests use to observe a task fault, so it keeps returning the
            // error; the stage is where that fault stops being the pass's.
            return match self.run_workflow_once_outcome().await {
                Ok(SingleWorkflowTaskOutcome::NoTask) => Ok(WorkflowStageOutcome::default()),
                Ok(SingleWorkflowTaskOutcome::Settled) => Ok(WorkflowStageOutcome {
                    committed: 1,
                    ..WorkflowStageOutcome::default()
                }),
                Ok(SingleWorkflowTaskOutcome::Deferred) => Ok(WorkflowStageOutcome {
                    deferred: 1,
                    ..WorkflowStageOutcome::default()
                }),
                Err(err) if fails_workflow_task_without_committing(&err) => {
                    Ok(WorkflowStageOutcome {
                        failed: 1,
                        ..WorkflowStageOutcome::default()
                    })
                }
                Err(err) => Err(err),
            };
        }

        let claimed = self
            .shared
            .backend
            .claim_workflow_tasks(
                self.shared.worker_id.clone(),
                ClaimWorkflowTasksOptions {
                    claim: ClaimWorkflowTaskOptions {
                        namespace: self.shared.namespace.clone(),
                        task_queue: self.shared.workflow_task_queue.clone(),
                        registered_workflow_types: self.shared.registered_workflow_types.clone(),
                        lease_duration: self.shared.workflow_task_lease_duration,
                    },
                    limit,
                    shard_filter: self.shared.workflow_task_concurrency.shard_filter.clone(),
                },
            )
            .await?;
        if claimed.is_empty() {
            return Ok(WorkflowStageOutcome::default());
        }

        // One task's failure must not abandon its batch neighbors' claims:
        // prepare and per-item commit errors release the affected claim and
        // continue, and the first *pass-level* error is propagated only after
        // every claim in the batch has been committed or released.
        let mut pass_error: Option<Error> = None;
        let mut failed = 0usize;
        let mut deferred = 0usize;
        let mut prepared = Vec::with_capacity(claimed.len());

        // One clock reading for the whole batch. Every task in it was claimed
        // by one RPC at one instant, so reading the provider clock per task
        // adds round trips to the critical path and lets tasks from the same
        // claim disagree about `now`. It is still a fallible await taken with
        // every claim held, so a failure releases the whole batch rather than
        // leaving the runs to wait out their leases.
        let now = match self.shared.backend.current_time().await {
            Ok(now) => now,
            Err(err) => {
                for task in &claimed {
                    if let Err(err) = self
                        .shared
                        .release_failed_workflow_task(task.claim.clone(), err.clone())
                        .await
                    {
                        record_workflow_task_error(&mut pass_error, &mut failed, err);
                    }
                }
                return match pass_error {
                    Some(err) => Err(err),
                    None => Ok(WorkflowStageOutcome {
                        failed,
                        deferred: claimed.len(),
                        ..WorkflowStageOutcome::default()
                    }),
                };
            }
        };
        // The cache is the worker's only mutable state and the lookups need no
        // I/O, so they are drained here and the claims carry their entries into
        // the concurrent stage below.
        let mut pending = claimed
            .into_iter()
            .map(|task| {
                let cached = self.remove_cached_workflow(&task.run_id);
                (task, cached)
            })
            .collect::<Vec<_>>()
            .into_iter();

        // The tasks in a batch are distinct runs: their history reads, payload
        // hydrations, and cold replays share nothing. Preparing them one at a
        // time serialises every one of those round trips behind the batch,
        // which is what made `max_concurrent_recoveries` unreachable — the
        // counter could never exceed one.
        let shared = self.shared;
        // Ordered, not unordered: the batch's results drive the commit order
        // and which error becomes the pass's, and both must not depend on
        // which task's provider round trip happened to return first.
        let mut in_flight = FuturesOrdered::new();
        loop {
            while in_flight.len() < limit {
                let Some((task, cached)) = pending.next() else {
                    break;
                };
                in_flight.push_back(shared.prepare_claimed_workflow_task(task, cached, Some(now)));
            }
            let Some(outcome) = in_flight.next().await else {
                break;
            };
            match outcome {
                Ok(PreparedWorkflowTaskOutcome::Prepared(task)) => prepared.push(task),
                // Released unreplayed — recovery admission, a replay budget, or
                // backpressure. Reported, not swallowed: a fully backpressured
                // batch used to look exactly like an empty queue, so a drain
                // could declare the worker idle with work still queued.
                Ok(PreparedWorkflowTaskOutcome::Deferred) => deferred += 1,
                // The prepare funnel already released this task's claim.
                Err(err) => record_workflow_task_error(&mut pass_error, &mut failed, err),
            }
        }

        let mut committed = 0usize;
        let chunk_size = self
            .shared
            .workflow_task_concurrency
            .commit_batch_size
            .max(1);
        let mut start = 0usize;
        while start < prepared.len() {
            let end = (start + chunk_size).min(prepared.len());
            // The commit moves into the batch instead of being deep-copied
            // into it: every append event, activity input, and child start
            // payload this task produced would otherwise be cloned and the
            // original dropped unread. Nothing after the RPC reads
            // `task.commit` — the wholesale-failure path and the per-task
            // result loop below both only need `task.claim` — so the emptied
            // slot is never observed. The claim is still cloned; it is a few
            // ids and a lease token, and both release paths need it after the
            // batch is built.
            let commits = prepared[start..end]
                .iter_mut()
                .map(|task| crate::WorkflowTaskCommitInput {
                    claim: task.claim.clone(),
                    commit: std::mem::take(&mut task.commit),
                })
                .collect::<Vec<_>>();
            let results = match self
                .shared
                .backend
                .commit_workflow_tasks(crate::WorkflowTaskCommitBatch { commits })
                .await
            {
                Ok(results) => results,
                Err(err) => {
                    // The commit RPC failed wholesale: nothing in this chunk or
                    // any later chunk was committed, so release every remaining
                    // claim (delayed for backpressure, immediate otherwise).
                    for task in &prepared[start..] {
                        if let Err(err) = self
                            .shared
                            .release_failed_workflow_task(task.claim.clone(), err.clone())
                            .await
                        {
                            record_workflow_task_error(&mut pass_error, &mut failed, err);
                        }
                    }
                    break;
                }
            };
            for (task, result) in prepared[start..end].iter_mut().zip(results) {
                let last_event_id = match result.result {
                    Ok(new_tail_event_id) => new_tail_event_id,
                    Err(err) => {
                        if let Err(err) = self
                            .shared
                            .release_failed_workflow_task(task.claim.clone(), err)
                            .await
                        {
                            record_workflow_task_error(&mut pass_error, &mut failed, err);
                        }
                        continue;
                    }
                };
                committed += 1;
                self.shared.record_workflow_task_committed(&task.run_id);
                let future = std::mem::replace(
                    &mut task.future,
                    Box::pin(std::future::ready(Err(Error::Backend(
                        "committed workflow future was already moved".to_owned(),
                    )))),
                );
                if let Some(entry) = cache_entry_after_commit(
                    task.terminal,
                    task.runtime_appended_tail,
                    last_event_id,
                    future,
                    task.next_command_seq,
                    std::mem::take(&mut task.default_activity_options),
                    std::mem::take(&mut task.unconsumed_indexes),
                ) {
                    self.insert_cached_workflow(task.run_id.clone(), entry);
                }
            }
            start = end;
        }
        // Only a pass-level error short-circuits the rest of the stage: the
        // backend could not settle this batch, so draining local activities
        // against it is pointless. A per-task fault was already settled, so the
        // committed neighbours' local activities still run and the committed
        // count is still reported.
        if let Some(err) = pass_error {
            return Err(err);
        }

        if committed > 0 {
            self.run_local_activities_after_workflow_tasks(committed)
                .await?;
        }
        Ok(WorkflowStageOutcome {
            committed,
            failed,
            deferred,
        })
    }

    async fn run_claimed_workflow_task(
        &mut self,
        claimed: crate::ClaimedWorkflowTask,
    ) -> Result<SingleWorkflowTaskOutcome> {
        let run_id = claimed.run_id.clone();
        let cached = self.remove_cached_workflow(&claimed.run_id);
        let prepared = match self
            .shared
            .prepare_claimed_workflow_task(claimed, cached, None)
            .await?
        {
            PreparedWorkflowTaskOutcome::Prepared(prepared) => prepared,
            PreparedWorkflowTaskOutcome::Deferred => {
                return Ok(SingleWorkflowTaskOutcome::Deferred);
            }
        };
        // The single-task path commits one prepared task immediately, mirroring the
        // batched commit loop in `run_workflow_batch_once` with a batch of one.
        let claim_for_release = prepared.claim.clone();
        let entry = match self.shared.commit_prepared_workflow_task(prepared).await {
            Ok(entry) => entry,
            Err(err) => {
                self.shared
                    .release_failed_workflow_task(claim_for_release, err)
                    .await?;
                // A release that swallowed backpressure settled the task as
                // deferred; anything else propagated above.
                return Ok(SingleWorkflowTaskOutcome::Deferred);
            }
        };
        if let Some(entry) = entry {
            self.insert_cached_workflow(run_id, entry);
        }
        self.run_local_activities_after_workflow_tasks(1).await?;
        Ok(SingleWorkflowTaskOutcome::Settled)
    }

    async fn run_local_activities_after_workflow_tasks(
        &mut self,
        workflow_tasks: usize,
    ) -> Result<()> {
        if self.shared.max_local_activities_per_workflow_task == 0 {
            return Ok(());
        }
        let limit = self
            .shared
            .max_local_activities_per_workflow_task
            .saturating_mul(workflow_tasks.max(1));
        for _ in 0..limit {
            if self.shared.run_activity_once().await? {
                self.state.completed_local_activity_tasks =
                    self.state.completed_local_activity_tasks.saturating_add(1);
            } else {
                break;
            }
        }
        Ok(())
    }

    fn take_completed_local_activity_tasks(&mut self) -> usize {
        let completed = self.state.completed_local_activity_tasks;
        self.state.completed_local_activity_tasks = 0;
        completed
    }

    // Single insertion point for the workflow cache: stamps the access
    // sequence, files the run in `cache_order` when that index is live, and
    // enforces `max_cached_workflows` by dropping the least-recently-inserted
    // entry. Eviction is a plain drop; the next task for an evicted run
    // cold-replays from history.
    //
    // "Eviction only runs at the bound" is true and is not a reason to scan:
    // at the bound is the steady state for a busy worker, so the scan this
    // replaced ran on every committed task and cost `max_cached_workflows`
    // (default 10,000) comparisons each time. Taking the front of
    // `cache_order` instead puts eviction in the same cost class as the
    // `cache` insert it accompanies rather than adding a linear pass on top.
    //
    // It is equally not a reason to keep an index below the bound, where
    // eviction never runs at all — see `activate_cache_order`.
    fn insert_cached_workflow(&mut self, run_id: RunId, mut entry: CachedWorkflow) {
        self.state.cache_access_seq += 1;
        entry.last_accessed_seq = self.state.cache_access_seq;
        if let Some(order) = self.state.cache_order.as_mut() {
            order.insert(entry.last_accessed_seq, run_id.clone());
        }
        if let Some(replaced) = self.state.cache.insert(run_id, entry) {
            // Re-inserting a run that was still cached retires its previous
            // stamp; without this the order map would outgrow the cache and
            // eviction would start taking already-dead keys.
            if let Some(order) = self.state.cache_order.as_mut() {
                order.remove(&replaced.last_accessed_seq);
            }
        }
        if self.state.cache.len() > self.shared.max_cached_workflows {
            self.activate_cache_order();
            while self.state.cache.len() > self.shared.max_cached_workflows {
                let Some(order) = self.state.cache_order.as_mut() else {
                    break;
                };
                let Some((_, evicted)) = order.pop_first() else {
                    break;
                };
                self.state.cache.remove(&evicted);
            }
        }
        self.debug_assert_cache_order_in_step();
    }

    // Single removal point, and the other half of the `cache_order`
    // invariant: a run taken out of the cache to be replayed must not leave
    // its stamp behind, or eviction would later pop a key with no entry and
    // the loop would stop reclaiming.
    fn remove_cached_workflow(&mut self, run_id: &RunId) -> Option<CachedWorkflow> {
        let entry = self.state.cache.remove(run_id)?;
        if let Some(order) = self.state.cache_order.as_mut() {
            order.remove(&entry.last_accessed_seq);
        }
        self.debug_assert_cache_order_in_step();
        Some(entry)
    }

    // Builds the eviction order from the stamps the cache entries already
    // carry, the first time the cache overflows.
    //
    // Deferring it is not an optimisation detail, it is what keeps the index
    // free for workers that never reach their bound: below the bound nothing
    // is ever evicted, so an index maintained there is pure overhead on every
    // committed task — one `RunId` clone and one index node per commit, which
    // the first version of this row did unconditionally.
    //
    // The build is not free and is not cheaper than the scan it replaces:
    // O(n log n) with an owned `RunId` per entry, measured at 10.3 ms for a
    // 100,000-entry cache against 0.67 ms for one victim scan, so roughly
    // fifteen scans' worth. It is paid **once** per worker, and every eviction
    // after it is a `pop_first` — 1.2 µs at that bound against the scan's
    // 669 µs. See `cache_eviction_cost_does_not_track_the_cache_bound`.
    fn activate_cache_order(&mut self) {
        if self.state.cache_order.is_some() {
            return;
        }
        self.state.cache_order = Some(
            self.state
                .cache
                .iter()
                .map(|(run_id, entry)| (entry.last_accessed_seq, run_id.clone()))
                .collect(),
        );
    }

    fn debug_assert_cache_order_in_step(&self) {
        debug_assert!(
            self.state
                .cache_order
                .as_ref()
                .is_none_or(|order| order.len() == self.state.cache.len()),
            "an active workflow cache order index must hold the same runs as the cache"
        );
    }
}

// The workflow-poll errors that fail one task without committing anything to
// its history: nondeterministic replay, a caught workflow panic, and a recorded
// change version this build cannot replay. All three mean "this attempt is
// void", never "this run is over", so no terminal event is appended, the claim
// is released with `nondeterminism_retry_backoff`, and the next attempt replays
// from durable history against redeployed code.
//
// They are also the errors a work pass survives. The task is already settled —
// nothing written, claim released — so failing the pass on top of that would
// only take the pass's healthy neighbours, local activities, maintenance, child
// dispatch, and activity execution down with one bad run. Every other error
// (backend, conflict, decode, registration) keeps its pass-level meaning.
//
// One predicate rather than a `matches!` copied to each site, so a variant
// added to this class cannot be routed at some sites and missed at others.
// A workflow-code fault: nothing is committed, the claim is released with the
// nondeterminism backoff, and the next claim replays the run, so a fixed
// redeploy recovers it. `PayloadEncode` and `PayloadDecode` are here because
// a durable API raises them for the workflow's own values (a payload that
// will not encode or decode, an empty side-effect key), which TypeScript
// routes the same way; committing `WorkflowFailed` for them would destroy a
// run over a deploy mismatch.
fn fails_workflow_task_without_committing(err: &Error) -> bool {
    matches!(
        err,
        Error::Nondeterminism(_)
            | Error::TaskPanic(_)
            | Error::UnsupportedWorkflowVersion { .. }
            | Error::PayloadEncode(_)
            | Error::PayloadDecode(_)
    )
}

// Sorts one claim's error into "this task failed" and "this pass failed".
// Either way the claim was already released by the funnel that produced the
// error; the split decides only what the stage reports to its driver.
fn record_workflow_task_error(pass_error: &mut Option<Error>, failed: &mut usize, err: Error) {
    if fails_workflow_task_without_committing(&err) {
        *failed += 1;
    } else {
        pass_error.get_or_insert(err);
    }
}

// A panic in workflow code must fail its own task, not the worker: without the
// catch, an `unwrap()` in one workflow unwinds out of `Worker::run` and takes
// every other cached run in the process with it.
//
// The panic becomes `Error::TaskPanic`, routed exactly like
// `Error::Nondeterminism`: nothing is committed, `release_failed_workflow_task`
// re-releases the claim with `nondeterminism_retry_backoff`, and the next
// attempt replays from durable history, so a fixed redeploy recovers the run.
// Committing `WorkflowFailed` instead would make a panic raised while replaying
// an already-progressed run permanently unrecoverable, and a panic carries no
// evidence that the run's recorded progress was wrong. The variant is distinct
// only so a caller can tell a workflow bug from genuine history divergence; the
// `workflow task panicked:` message prefix is a stable contract.
//
// `AssertUnwindSafe` asserts only that nothing the closure touches is read
// again after a caught unwind, which the `Err` return enforces rather than
// assumes. `context` may hold half-appended events, scheduled activities, and
// hydration requests from this attempt; the error propagates out of
// `poll_until_history_blocked_or_ready` before any of that is read, and the
// context is dropped, so a partial attempt cannot reach a commit. `future` may
// be a poisoned state machine; its caller holds the only handle, drops it
// without polling again, and cannot re-cache it because the cache entry is
// removed at claim time and reinserted only after a successful commit.
fn poll_cached(
    future: &mut Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
    context: &mut crate::runtime::RuntimeContext,
) -> Result<Poll<Result<crate::PayloadRef>>> {
    let waker = futures::task::noop_waker();
    let mut task_context = std::task::Context::from_waker(&waker);
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        poll_with_runtime_context(context, || future.as_mut().poll(&mut task_context))
    }))
    .map_err(|payload| {
        Error::TaskPanic(format!(
            "workflow task panicked: {}",
            panic_message(payload.as_ref())
        ))
    })
}

// The one cache decision for a committed task, shared by the single-task and
// batch commit paths. A terminal run has nothing to cache, and provider
// appended events past the runtime's tail (for example inline child starts)
// are invisible to the cached future, so the next task must cold-replay to
// pick them up.
fn cache_entry_after_commit(
    terminal: bool,
    runtime_appended_tail: EventId,
    last_event_id: EventId,
    future: Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
    next_command_seq: u64,
    default_activity_options: crate::ActivityOptions,
    unconsumed_indexes: crate::runtime::ReadyEventIndexes,
) -> Option<CachedWorkflow> {
    if terminal || last_event_id > runtime_appended_tail {
        return None;
    }
    Some(CachedWorkflow {
        future,
        last_event_id,
        next_command_seq,
        default_activity_options,
        unconsumed_indexes,
        last_accessed_seq: 0,
    })
}

// Recovers the operator-facing message from a caught panic payload: `panic!`
// with a literal produces `&'static str`, any formatted `panic!` produces
// `String`, and `panic_any` carries no message to recover.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else {
        "<non-string panic payload>"
    }
}

fn prefetched_claim_history_chunk(
    claimed: &crate::ClaimedWorkflowTask,
    after_event_id: EventId,
) -> Option<crate::HistoryChunk> {
    if after_event_id >= claimed.replay_target_event_id {
        return Some(crate::HistoryChunk {
            events: Vec::new(),
            last_event_id: after_event_id,
            has_more: false,
        });
    }

    // Validation runs over borrowed events and allocates nothing, so a
    // prefetch that does not cover the window contiguously costs a scan
    // instead of a deep copy of every candidate event — payloads included —
    // that is then thrown away. Only a chunk that will be returned is cloned.
    let selected = || {
        claimed.prefetched_history.iter().filter(|event| {
            event.event_id > after_event_id && event.event_id <= claimed.replay_target_event_id
        })
    };
    let mut expected_event_id = after_event_id.next();
    let mut last_event_id = None;
    for event in selected() {
        if event.event_id != expected_event_id {
            return None;
        }
        expected_event_id = event.event_id.next();
        last_event_id = Some(event.event_id);
    }
    let last_event_id = last_event_id?;
    if last_event_id != claimed.replay_target_event_id {
        return None;
    }
    Some(crate::HistoryChunk {
        events: selected().cloned().collect(),
        last_event_id,
        has_more: false,
    })
}

fn prefetched_claim_history_chunk_bounded(
    claimed: &crate::ClaimedWorkflowTask,
    after_event_id: EventId,
    max_events: usize,
    max_bytes: usize,
) -> Option<crate::HistoryChunk> {
    if after_event_id >= claimed.replay_target_event_id {
        return Some(crate::HistoryChunk {
            events: Vec::new(),
            last_event_id: after_event_id,
            has_more: false,
        });
    }
    let max_events = max_events.max(1);
    let max_bytes = max_bytes.max(1);
    let mut next_event_id = after_event_id.next();
    let mut bytes = 0usize;
    let mut events = Vec::new();
    for event in claimed.prefetched_history.iter().filter(|event| {
        event.event_id > after_event_id && event.event_id <= claimed.replay_target_event_id
    }) {
        if event.event_id != next_event_id {
            break;
        }
        let event_bytes = crate::runtime::event_payload_len(&event.data).max(1);
        if !events.is_empty() && (events.len() >= max_events || bytes + event_bytes > max_bytes) {
            break;
        }
        events.push(event.clone());
        bytes = bytes.saturating_add(event_bytes);
        next_event_id = event.event_id.next();
    }
    let last_event_id = events.last()?.event_id;
    Some(crate::HistoryChunk {
        events,
        last_event_id,
        has_more: last_event_id < claimed.replay_target_event_id,
    })
}

// Splits a cold-replay chunk into the run's input and the events the runtime
// replays, by value. The chunk is dead the moment it is split — the caller has
// already taken `last_event_id` and folded the change markers — so the tail
// moves out of the caller's `Vec` rather than being deep-cloned into a second
// one. Removing the head is a single in-place shift of `HistoryEvent` structs;
// the payloads, run ids, and type names they own are never copied.
fn split_start_event(
    mut events: Vec<HistoryEvent>,
) -> Result<(crate::PayloadRef, Vec<HistoryEvent>)> {
    if events.is_empty() {
        return Err(Error::Backend(
            "claimed workflow task without WorkflowStarted event".to_owned(),
        ));
    }
    let HistoryEventData::WorkflowStarted { input, .. } = events.remove(0).data else {
        return Err(Error::Backend(
            "first workflow history event was not WorkflowStarted".to_owned(),
        ));
    };
    Ok((input, events))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoopWait {
    Elapsed,
    ShutdownRequested,
}

// Waits for `wait` or for shutdown, whichever comes first.
//
// The `Notified` future is enabled — that is, registered with the notifier —
// *before* the shutdown flag is re-read. Registering after the read would let a
// `shutdown()` that lands in between be missed by both halves: `notify_waiters`
// wakes only the waiters registered at the time it runs, so the loop would then
// sit out a full backoff before noticing.
async fn wait_or_shutdown<F>(shutdown: &WorkerShutdown, wait: F) -> LoopWait
where
    F: Future,
{
    let mut notified = std::pin::pin!(shutdown.notified());
    notified.as_mut().enable();
    if shutdown.is_requested() {
        return LoopWait::ShutdownRequested;
    }
    let wait = std::pin::pin!(wait);
    match futures::future::select(notified, wait).await {
        futures::future::Either::Left(_) => LoopWait::ShutdownRequested,
        futures::future::Either::Right(_) => LoopWait::Elapsed,
    }
}

// A timed wait that a shutdown cuts short. A zero delay yields to the peer
// loops instead of resolving inline: a loop that only ever resolved inline
// would hold the joined task forever, which is the starvation this split exists
// to remove.
async fn sleep_or_shutdown(shutdown: &WorkerShutdown, delay: Duration) -> LoopWait {
    if delay.is_zero() {
        return yield_to_peer_loops(shutdown).await;
    }
    wait_or_shutdown(shutdown, tokio::time::sleep(delay)).await
}

// Returns `Pending` exactly once, waking immediately.
//
// `Worker::run`'s three loops are branches of one `try_join3`, and the join
// polls a branch only when the join itself is polled. Returning `Pending` here
// is therefore what hands the peers their turn: the join polls every unfinished
// branch on each of its own polls, so one `Pending` from a saturated loop is
// enough for the other two to make a call. Written out rather than delegated to
// `tokio::task::yield_now` because the requirement is about this join's polling
// discipline, not about a runtime's scheduler.
async fn yield_to_peer_loops(shutdown: &WorkerShutdown) -> LoopWait {
    if shutdown.is_requested() {
        return LoopWait::ShutdownRequested;
    }
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
    if shutdown.is_requested() {
        LoopWait::ShutdownRequested
    } else {
        LoopWait::Elapsed
    }
}

// Doubles a backoff up to its ceiling. Zero stays zero, so a caller that opted
// out of pacing keeps opting out.
fn next_backoff(current: Duration, max: Duration) -> Duration {
    if current.is_zero() {
        return Duration::ZERO;
    }
    current.saturating_mul(2).min(max)
}

/// Deterministic per-worker jitter for the maintenance cadence.
///
/// Seeded from the worker id and advanced once per delay, so a given worker id
/// always reproduces the same schedule while two ids diverge from their first
/// delay. Deterministic on purpose: a process-global RNG would break both
/// properties, and this crate keeps global randomness out of worker scheduling
/// for the same reason it keeps it out of replay.
///
/// FNV-1a seeds mulberry32, the same construction the TypeScript worker uses,
/// over the same input: the id's **UTF-16 code units**, because TypeScript
/// hashes `charCodeAt` values. Hashing UTF-8 bytes instead would agree for
/// ASCII worker ids and silently diverge for any other, which is a difference
/// nothing downstream could detect and no test would catch.
struct MaintenanceJitter {
    state: u32,
}

impl MaintenanceJitter {
    fn new(worker_id: &WorkerId) -> Self {
        Self {
            state: fnv1a32(&worker_id.to_string()),
        }
    }

    // mulberry32: one 32-bit state, advanced by an odd increment and avalanched
    // into a uniform value in `[0, 1)`.
    fn next_unit(&mut self) -> f64 {
        self.state = self.state.wrapping_add(0x6d2b_79f5);
        let mut mixed = self.state;
        mixed = (mixed ^ (mixed >> 15)).wrapping_mul(mixed | 1);
        mixed ^= mixed.wrapping_add((mixed ^ (mixed >> 7)).wrapping_mul(mixed | 61));
        f64::from(mixed ^ (mixed >> 14)) / 4_294_967_296.0
    }

    /// Spreads `interval` over `[0.5x, 1.5x)`. Zero stays zero, so a caller can
    /// opt out of pacing entirely.
    fn next_delay(&mut self, interval: Duration) -> Duration {
        if interval.is_zero() {
            return Duration::ZERO;
        }
        let scaled = interval.as_secs_f64() * (0.5 + self.next_unit());
        Duration::from_secs_f64(scaled).max(Duration::from_millis(1))
    }
}

// FNV-1a over UTF-16 code units. Matches JavaScript's `charCodeAt` loop unit
// for unit, including the surrogate halves of an astral character, so a worker
// id produces the same seed in both runtimes whatever it contains.
fn fnv1a32(value: &str) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    for unit in value.encode_utf16() {
        hash ^= u32::from(unit);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

pub struct WorkerBuilder<B>
where
    B: DurableBackend,
{
    backend: B,
    namespace: Namespace,
    worker_id: WorkerId,
    workflow_task_queue: TaskQueue,
    activity_task_queue: TaskQueue,
    registry: Registry,
    history_chunk_events: usize,
    history_chunk_bytes: usize,
    nondeterminism_retry_backoff: Duration,
    workflow_task_concurrency: WorkflowTaskConcurrency,
    recovery_flow_control: RecoveryFlowControl,
    workflow_task_lease_duration: Duration,
    activity_task_lease_duration: Duration,
    activity_task_batch_size: usize,
    max_concurrent_activities: usize,
    max_concurrent_payload_hydrations: usize,
    activity_completion_batch_size: usize,
    max_local_activities_per_workflow_task: usize,
    max_cached_workflows: usize,
    run_timer_maintenance: bool,
    loop_config: WorkerLoopConfig,
    event_sink: Option<WorkerEventSink>,
}

impl<B> WorkerBuilder<B>
where
    B: DurableBackend,
{
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Namespace::new(namespace);
        self
    }

    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = WorkerId::new(worker_id);
        self
    }

    pub fn workflow_task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.workflow_task_queue = TaskQueue::new(task_queue);
        self
    }

    pub fn activity_task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.activity_task_queue = TaskQueue::new(task_queue);
        self
    }

    pub fn history_chunk_events(mut self, max_events: usize) -> Self {
        self.history_chunk_events = max_events.max(1);
        self
    }

    pub fn history_chunk_bytes(mut self, max_bytes: usize) -> Self {
        self.history_chunk_bytes = max_bytes.max(1);
        self
    }

    // How long a workflow task that failed without committing — a
    // nondeterministic replay, a caught workflow panic, an unsupported recorded
    // version — stays invisible before another attempt may claim it. Clamped to
    // [`MIN_NONDETERMINISM_RETRY_BACKOFF`]: this delay is the only bound on a
    // permanently poisoned run's retry rate, so a zero value would turn one bad
    // workflow into a busy loop that never yields.
    pub fn nondeterminism_retry_backoff(mut self, backoff: Duration) -> Self {
        self.nondeterminism_retry_backoff = backoff.max(MIN_NONDETERMINISM_RETRY_BACKOFF);
        self
    }

    // How long a claimed workflow task stays fenced to this worker before a
    // crash makes it reclaimable. Must comfortably exceed one claim-to-commit
    // round trip.
    pub fn workflow_task_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.workflow_task_lease_duration = lease_duration.max(MIN_TASK_LEASE_DURATION);
        self
    }

    // How long a claimed activity task stays fenced to this worker. For
    // activities without an explicit timeout or heartbeat this is also the
    // reclaim deadline, so it must exceed the slowest such activity's runtime.
    pub fn activity_task_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.activity_task_lease_duration = lease_duration.max(MIN_TASK_LEASE_DURATION);
        self
    }

    pub fn max_concurrent_workflow_tasks(mut self, limit: usize) -> Self {
        self.workflow_task_concurrency.max_concurrent_workflow_tasks = limit.max(1);
        self
    }

    pub fn workflow_task_prefetch_limit(mut self, limit: usize) -> Self {
        self.workflow_task_concurrency.prefetch_limit = limit.max(1);
        self
    }

    pub fn workflow_task_commit_batch_size(mut self, limit: usize) -> Self {
        self.workflow_task_concurrency.commit_batch_size = limit.max(1);
        self
    }

    pub fn workflow_task_shard_filter(mut self, shards: impl IntoIterator<Item = ShardId>) -> Self {
        let shards = shards.into_iter().collect::<Vec<_>>();
        self.workflow_task_concurrency.shard_filter = (!shards.is_empty()).then_some(shards);
        self
    }

    // How many activity tasks one claim RPC requests; purely a round-trip
    // batching knob, not a concurrency bound. One activity pass claims up to
    // `max_concurrent_activities` tasks in total, `min` of the two knobs per
    // RPC — so setting only this knob still claims one task per pass
    // (max_concurrent_activities defaults to 1), and high concurrency with
    // this knob unset issues single-task claim RPCs.
    pub fn activity_task_batch_size(mut self, limit: usize) -> Self {
        self.activity_task_batch_size = limit.max(1);
        self
    }

    /// Blob reads one workflow task may have in flight at once while
    /// hydrating offloaded payloads a poll blocked on. One fanout item is one
    /// read, and they are independent, so this is what keeps a resumed fanout
    /// from costing one provider round trip per item.
    pub fn max_concurrent_payload_hydrations(mut self, limit: usize) -> Self {
        self.max_concurrent_payload_hydrations = limit.max(1);
        self
    }

    // How many claimed activities execute concurrently within one activity
    // pass.
    pub fn max_concurrent_activities(mut self, limit: usize) -> Self {
        self.max_concurrent_activities = limit.max(1);
        self
    }

    pub fn activity_completion_batch_size(mut self, limit: usize) -> Self {
        self.activity_completion_batch_size = limit.max(1);
        self
    }

    pub fn max_concurrent_recoveries(mut self, limit: usize) -> Self {
        self.recovery_flow_control.max_concurrent_recoveries = limit;
        self
    }

    pub fn recovery_replay_event_budget(mut self, max_events: usize) -> Self {
        self.recovery_flow_control.replay_event_budget = max_events;
        self
    }

    pub fn recovery_replay_byte_budget(mut self, max_bytes: usize) -> Self {
        self.recovery_flow_control.replay_byte_budget = max_bytes;
        self
    }

    pub fn recovery_prefetch_chunks(mut self, max_chunks: usize) -> Self {
        self.recovery_flow_control.prefetch_chunks = max_chunks;
        self
    }

    pub fn recovery_defer_delay(mut self, delay: Duration) -> Self {
        self.recovery_flow_control.defer_delay = delay;
        self
    }

    pub fn max_local_activities_per_workflow_task(mut self, limit: usize) -> Self {
        self.max_local_activities_per_workflow_task = limit;
        self
    }

    // Upper bound on cached workflow futures; the least recently used run is
    // dropped at the bound and cold-replays on its next task.
    pub fn max_cached_workflows(mut self, limit: usize) -> Self {
        self.max_cached_workflows = limit.max(1);
        self
    }

    /// How long a task loop that found no work parks in the backend's
    /// `wait_for_ready` before re-polling — the *first* such wait. Consecutive
    /// empty polls double it up to [`WorkerBuilder::max_idle_wait`], and any
    /// work resets it.
    ///
    /// This answers "how fast does an idle worker re-poll for work", which is
    /// not the question [`WorkerBuilder::maintenance_interval`] answers ("how
    /// often does this worker scan for due timers"). The two are independent:
    /// a worker can be saturated with work — never idle at all — and still owe
    /// its share of due-timer scanning, and a worker that turns scanning off
    /// entirely still needs a poll cadence. One knob cannot express both.
    pub fn idle_wait(mut self, wait: Duration) -> Self {
        self.loop_config.idle_wait = wait.max(MIN_IDLE_WAIT);
        self
    }

    /// Ceiling for the doubling idle backoff. Clamped up to
    /// [`WorkerBuilder::idle_wait`], so the ceiling can never sit below the
    /// first wait whatever order the two are set in.
    pub fn max_idle_wait(mut self, wait: Duration) -> Self {
        self.loop_config.max_idle_wait = wait.max(MIN_IDLE_WAIT);
        self
    }

    /// How long a task loop waits after catching an error before retrying —
    /// the *first* such wait, doubling to
    /// [`WorkerBuilder::max_error_backoff`] while errors continue and reset by
    /// any successful pass.
    ///
    /// Each loop owns its own budget, so a failing workflow claim does not
    /// delay activity completions. This is also the only bound on a fault the
    /// stage cannot settle per task and the provider re-offers immediately —
    /// an undecodable workflow input, for example, which releases its claim
    /// with no delay and is claimable again at once. Bounding it here rather
    /// than per error variant covers the fault classes that do not exist yet.
    pub fn error_backoff(mut self, backoff: Duration) -> Self {
        self.loop_config.error_backoff = backoff.max(MIN_ERROR_BACKOFF);
        self
    }

    /// Ceiling for the doubling error backoff.
    pub fn max_error_backoff(mut self, backoff: Duration) -> Self {
        self.loop_config.max_error_backoff = backoff.max(MIN_ERROR_BACKOFF);
        self
    }

    /// Whether this worker scans for due timers and for activities past their
    /// start-to-close deadline. Defaults to on.
    ///
    /// Scoped to exactly what `SPEC.md` §11 licenses as configuration: firing
    /// due timers is the timer service's obligation and a worker's scan is a
    /// convenience, and the same holds for reaping activities past their
    /// deadline. So a deployment running a timer service may turn this off on
    /// every worker, which is the configuration §11 names, and no run depends
    /// on any particular worker scanning.
    ///
    /// It does **not** cover dispatching the child-workflow start outbox, which
    /// stays unconditional in both the maintenance loop and
    /// [`Worker::run_until_idle`]'s pass. Nothing else in a deployment drains
    /// that outbox — no service owns it the way a timer service owns due timers
    /// — so making it configurable would turn "this worker does less scanning"
    /// into "child workflows in this deployment never start", which is a
    /// durability outcome and not a tuning knob. TypeScript's
    /// `runTimerMaintenance`, which this mirrors by name and by scope, has the
    /// same boundary.
    ///
    /// API budget: no composition of the existing primitives suppresses the
    /// scan, because its cadence was hardcoded into the work pass. The
    /// invariant it protects is bounded provider load per unit of work rather
    /// than per worker pass: a fleet that has a timer service pays a scan per
    /// worker per interval for nothing, and that cost scales with fleet size on
    /// exactly the deployments large enough to run a timer service.
    pub fn run_timer_maintenance(mut self, enabled: bool) -> Self {
        self.run_timer_maintenance = enabled;
        self
    }

    /// Base delay between maintenance scans, jittered into `[0.5x, 1.5x)` from
    /// the worker id. A scan that found work re-runs immediately; consecutive
    /// empty scans double the interval up to
    /// [`WorkerBuilder::max_maintenance_interval`].
    ///
    /// Timer latency is bounded by this rather than by how busy the worker is,
    /// and scan load on the provider is bounded by fleet size rather than by
    /// throughput. `Duration::ZERO` opts out of pacing: the loop then scans as
    /// fast as it can be polled, which is a busy scan against the provider and
    /// is only ever right for a test.
    pub fn maintenance_interval(mut self, interval: Duration) -> Self {
        self.loop_config.maintenance_interval = interval;
        self
    }

    /// Ceiling for the maintenance backoff after consecutive empty scans.
    pub fn max_maintenance_interval(mut self, interval: Duration) -> Self {
        self.loop_config.max_maintenance_interval = interval;
        self
    }

    /// Installs a sink for [`WorkerEvent`]s: individual committed, failed,
    /// deferred and conflicted workflow tasks, activity outcomes, maintenance
    /// scans, and every error a loop caught.
    ///
    /// API budget: [`Worker::metrics`] answers "how much of each thing has
    /// happened", which is what a scrape endpoint needs; it cannot answer
    /// "which run", "which error", or "when", which is what an operator needs
    /// to act on a poisoned run. A counter cannot be composed into that, and
    /// this crate has no logging dependency to fall back on. The invariant
    /// protected is a recovery one: a run that fails without committing writes
    /// nothing to history, so if the worker does not report it, nothing does.
    ///
    /// The sink is called inline on the worker's path, so it must be cheap and
    /// must not block. Events borrow their payloads to keep the no-sink case
    /// free of allocation; clone what you need to keep.
    pub fn on_event<F>(mut self, sink: F) -> Self
    where
        F: Fn(WorkerEvent<'_>) + Send + Sync + 'static,
    {
        self.event_sink = Some(Arc::new(sink));
        self
    }

    pub fn register_workflow<W>(mut self, _workflow: W) -> Self
    where
        W: Workflow + Default,
    {
        self = self
            .try_register_workflow(_workflow)
            .expect("duplicate workflow registration");
        self
    }

    pub fn try_register_workflow<W>(mut self, _workflow: W) -> Result<Self>
    where
        W: Workflow + Default,
    {
        self.registry.register_workflow::<W>()?;
        Ok(self)
    }

    pub fn register_activity<A>(mut self, _activity: A) -> Self
    where
        A: crate::Activity + Default,
    {
        self = self
            .try_register_activity(_activity)
            .expect("duplicate activity registration");
        self
    }

    pub fn try_register_activity<A>(mut self, _activity: A) -> Result<Self>
    where
        A: crate::Activity + Default,
    {
        self.registry.register_activity::<A>()?;
        Ok(self)
    }

    pub fn build(self) -> Worker<B> {
        let payload_codec = self.backend.payload_storage_config().codec;
        // Every ceiling is clamped to its own floor here rather than at the
        // setter, so the two halves of a pair can be set in either order
        // without one silently overriding the other.
        let loop_config = WorkerLoopConfig {
            max_idle_wait: self
                .loop_config
                .max_idle_wait
                .max(self.loop_config.idle_wait),
            max_error_backoff: self
                .loop_config
                .max_error_backoff
                .max(self.loop_config.error_backoff),
            max_maintenance_interval: self
                .loop_config
                .max_maintenance_interval
                .max(self.loop_config.maintenance_interval),
            ..self.loop_config
        };
        Worker {
            shared: WorkerShared {
                backend: self.backend,
                namespace: self.namespace,
                worker_id: self.worker_id,
                workflow_task_queue: self.workflow_task_queue,
                activity_task_queue: self.activity_task_queue,
                registered_workflow_types: self.registry.workflow_types(),
                registered_activity_names: self.registry.activity_names(),
                registry: self.registry,
                history_chunk_events: self.history_chunk_events,
                history_chunk_bytes: self.history_chunk_bytes,
                payload_codec,
                nondeterminism_retry_backoff: self.nondeterminism_retry_backoff,
                workflow_task_concurrency: self.workflow_task_concurrency,
                recovery_flow_control: self.recovery_flow_control,
                active_recoveries: Arc::new(AtomicUsize::new(0)),
                workflow_task_lease_duration: self.workflow_task_lease_duration,
                activity_task_lease_duration: self.activity_task_lease_duration,
                activity_task_batch_size: self.activity_task_batch_size,
                max_concurrent_activities: self.max_concurrent_activities,
                max_concurrent_payload_hydrations: self.max_concurrent_payload_hydrations,
                activity_completion_batch_size: self.activity_completion_batch_size,
                max_local_activities_per_workflow_task: self.max_local_activities_per_workflow_task,
                max_cached_workflows: self.max_cached_workflows,
                run_timer_maintenance: self.run_timer_maintenance,
                loop_config,
                metrics: WorkerMetricsState::default(),
                event_sink: self.event_sink,
            },
            workflow: WorkflowState::default(),
            shutdown: WorkerShutdown::new(),
        }
    }

    /// Builds the worker and runs it until shutdown; a convenience for workers
    /// that never need the [`Worker`] value itself (for example activity-only
    /// workers).
    pub async fn run(self) -> Result<()> {
        self.build().run().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClaimedWorkflowTask, HistoryEvent, HistoryEventData, WorkflowTaskClaim, WorkflowTaskReason,
    };

    fn event(event_id: u64) -> HistoryEvent {
        HistoryEvent {
            event_id: EventId(event_id),
            data: HistoryEventData::WorkflowTaskStarted,
        }
    }

    fn claimed(
        prefetched_history: Vec<HistoryEvent>,
        replay_target_event_id: u64,
    ) -> ClaimedWorkflowTask {
        ClaimedWorkflowTask {
            run_id: RunId::new("run"),
            workflow_id: WorkflowId::new("workflow"),
            workflow_type: crate::WorkflowType::new("test.workflow", 1),
            claim: WorkflowTaskClaim {
                run_id: RunId::new("run"),
                worker_id: WorkerId::new("worker"),
                token: 1,
            },
            replay_target_event_id: EventId(replay_target_event_id),
            reason: WorkflowTaskReason::WorkflowStarted,
            prefetched_history,
        }
    }

    #[test]
    fn prefetched_claim_history_chunk_uses_complete_contiguous_tail() {
        let claimed = claimed(vec![event(2), event(3), event(4)], 4);

        let chunk = prefetched_claim_history_chunk(&claimed, EventId(1)).unwrap();

        assert_eq!(chunk.last_event_id, EventId(4));
        assert!(!chunk.has_more);
        assert_eq!(
            chunk
                .events
                .iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>(),
            vec![EventId(2), EventId(3), EventId(4)]
        );
    }

    #[test]
    fn prefetched_claim_history_chunk_rejects_missing_start_or_target() {
        let missing_start = claimed(vec![event(3), event(4)], 4);
        assert!(prefetched_claim_history_chunk(&missing_start, EventId(1)).is_none());

        let missing_target = claimed(vec![event(2), event(3)], 4);
        assert!(prefetched_claim_history_chunk(&missing_target, EventId(1)).is_none());
    }

    #[test]
    fn prefetched_claim_history_chunk_rejects_internal_gaps() {
        let claimed = claimed(vec![event(2), event(4)], 4);

        assert!(prefetched_claim_history_chunk(&claimed, EventId(1)).is_none());
    }

    // The shape a real cached wake hits: providers prefetch a bounded tail
    // (`MemoryBackend` keeps the last sixteen events), so a run whose cached
    // task is further behind than that asks for a window the prefetch starts
    // after. Rejecting it is the whole reason the validation runs over
    // borrowed events — this is the case that used to deep-copy every
    // candidate, payloads included, and then discard all of them.
    #[test]
    fn prefetched_claim_history_chunk_rejects_a_tail_that_starts_after_the_window() {
        let claimed = claimed(vec![event(8), event(9), event(10)], 10);

        assert!(prefetched_claim_history_chunk(&claimed, EventId(1)).is_none());
    }

    #[test]
    fn split_start_event_moves_the_tail_and_reports_a_missing_or_wrong_head() {
        let started = HistoryEvent {
            event_id: EventId(1),
            data: HistoryEventData::WorkflowStarted {
                workflow_type: crate::WorkflowType::new("test.workflow", 1),
                input: crate::PayloadRef::inline_messagepack(&7u64).unwrap(),
            },
        };

        let (input, tail) =
            split_start_event(vec![started.clone(), event(2), event(3)]).expect("split");
        assert!(matches!(input, crate::PayloadRef::Inline { .. }));
        assert_eq!(
            tail.iter().map(|event| event.event_id).collect::<Vec<_>>(),
            vec![EventId(2), EventId(3)]
        );

        let empty = split_start_event(Vec::new()).unwrap_err();
        assert_eq!(
            empty.to_string(),
            Error::Backend("claimed workflow task without WorkflowStarted event".to_owned())
                .to_string()
        );

        let headless = split_start_event(vec![event(2)]).unwrap_err();
        assert_eq!(
            headless.to_string(),
            Error::Backend("first workflow history event was not WorkflowStarted".to_owned())
                .to_string()
        );
    }

    fn cached_entry() -> CachedWorkflow {
        CachedWorkflow {
            future: Box::pin(std::future::pending()),
            last_event_id: EventId(1),
            next_command_seq: 0,
            default_activity_options: crate::ActivityOptions::default(),
            unconsumed_indexes: crate::runtime::ReadyEventIndexes::default(),
            last_accessed_seq: 0,
        }
    }

    fn cache_worker(max_cached_workflows: usize) -> Worker<crate::MemoryBackend> {
        Worker::builder(crate::MemoryBackend::new())
            .max_cached_workflows(max_cached_workflows)
            .build()
    }

    // The victim must be the least *recently inserted* run, not the smallest
    // run id. The run ids here are deliberately reverse-ordered against the
    // insertion order, so an eviction that took the cache map's first key
    // would drop the newest entry and keep the oldest.
    #[test]
    fn cache_eviction_drops_the_least_recently_inserted_run() {
        let mut worker = cache_worker(2);
        let mut workflow = worker.workflow_worker();

        for run_id in ["run-c", "run-b", "run-a"] {
            workflow.insert_cached_workflow(RunId::new(run_id), cached_entry());
        }

        assert_eq!(workflow.state.cache.len(), 2);
        assert!(!workflow.state.cache.contains_key(&RunId::new("run-c")));
        assert!(workflow.state.cache.contains_key(&RunId::new("run-b")));
        assert!(workflow.state.cache.contains_key(&RunId::new("run-a")));
    }

    // Re-inserting a run moves it to the back of the eviction order, which is
    // what makes the cache least-recently-*used* for a busy worker: every
    // cached run re-enters through insert after every committed task.
    //
    // The index must already be live when the re-insert happens, or this test
    // does not reach the branch it exists for. An earlier version of it used a
    // bound of two and re-inserted while `cache_order` was still `None`, so
    // deleting the stale-stamp retirement outright left the whole suite green.
    // The three inserts below activate the index first; the re-insert then
    // takes the branch, and a survivor is chosen so the retirement has a stamp
    // to retire.
    #[test]
    fn reinserting_a_cached_run_renews_its_place_in_the_eviction_order() {
        let mut worker = cache_worker(2);
        let mut workflow = worker.workflow_worker();

        // Activates the index and leaves run-b and run-c cached.
        for run_id in ["run-a", "run-b", "run-c"] {
            workflow.insert_cached_workflow(RunId::new(run_id), cached_entry());
        }
        assert!(
            workflow.state.cache_order.is_some(),
            "the re-insert below must run against a live index or it proves nothing"
        );

        // Without removing it first, mirroring a commit that re-caches a run
        // the claim path did not take out. run-b was the oldest survivor, so
        // renewing it must make run-c the next victim instead.
        workflow.insert_cached_workflow(RunId::new("run-b"), cached_entry());
        assert_eq!(
            workflow.state.cache_order.as_ref().map(BTreeMap::len),
            Some(2),
            "a re-inserted run must retire its previous stamp"
        );

        workflow.insert_cached_workflow(RunId::new("run-d"), cached_entry());

        assert_eq!(workflow.state.cache.len(), 2);
        assert!(
            !workflow.state.cache.contains_key(&RunId::new("run-c")),
            "renewing run-b must leave run-c as the oldest, and so the victim"
        );
        assert!(workflow.state.cache.contains_key(&RunId::new("run-b")));
        assert!(workflow.state.cache.contains_key(&RunId::new("run-d")));
    }

    // The cache-bound microbenchmark the plan asks for, at 1,000 and 100,000.
    //
    // Ignored by default: it builds a 100,000-entry cache, which is seconds of
    // work and tens of megabytes, and it asserts on wall-clock time. Run it
    // with `cargo test --release -- --ignored eviction_cost`.
    //
    // It asserts the *ratio*, not a duration, so it means the same thing on
    // any machine. The scan this replaced grows with the bound; taking the
    // front of the order index does not.
    //
    // The first evicting insert is deliberately excluded from the timed
    // region, because it also builds the index. That build is O(n log n) with
    // an owned `RunId` per entry — measured at ~10 ms for a 100,000 bound,
    // which is more than one victim scan, not less — and it happens once per
    // worker. Amortising it over a short measurement is what makes a correct
    // implementation read as a 41x rise; the first version of this test did
    // exactly that and failed against its own subject.
    #[test]
    #[ignore = "builds a 100,000-entry cache and asserts on wall-clock time"]
    fn cache_eviction_cost_does_not_track_the_cache_bound() {
        const MAX_SCALING: f64 = 10.0;
        const EVICTING_INSERTS: usize = 200;

        struct EvictionCost {
            steady_state_nanos: f64,
            activation_nanos: f64,
        }

        fn measure(bound: usize) -> EvictionCost {
            let mut worker = cache_worker(bound);
            let mut workflow = worker.workflow_worker();
            let entry = || CachedWorkflow {
                future: Box::pin(std::future::pending()),
                last_event_id: EventId(1),
                next_command_seq: 0,
                default_activity_options: crate::ActivityOptions::default(),
                unconsumed_indexes: crate::runtime::ReadyEventIndexes::default(),
                last_accessed_seq: 0,
            };
            for index in 0..bound {
                workflow.insert_cached_workflow(RunId::new(format!("fill-{index}")), entry());
            }
            assert_eq!(workflow.state.cache.len(), bound);

            // Every insert past this point is for a run the cache does not
            // hold, so each one pushes it over the bound and evicts. The first
            // one also activates the index.
            let activation = std::time::Instant::now();
            workflow.insert_cached_workflow(RunId::new("activate"), entry());
            let activation_nanos = activation.elapsed().as_nanos() as f64;

            let started = std::time::Instant::now();
            for index in 0..EVICTING_INSERTS {
                workflow.insert_cached_workflow(RunId::new(format!("evict-{index}")), entry());
            }
            let elapsed = started.elapsed();
            assert_eq!(workflow.state.cache.len(), bound);
            EvictionCost {
                steady_state_nanos: elapsed.as_nanos() as f64 / EVICTING_INSERTS as f64,
                activation_nanos,
            }
        }

        let small = measure(1_000);
        let large = measure(100_000);
        let scaling = large.steady_state_nanos / small.steady_state_nanos;
        assert!(
            scaling < MAX_SCALING,
            "a steady-state evicting insert cost {:.0} ns at a 1,000 bound and {:.0} ns at \
             100,000, a {scaling:.1}x rise against a ceiling of {MAX_SCALING}x. Eviction must \
             not scan the cache for its victim.\n\
             one-time index activation: {:.0} ns at 1,000, {:.0} ns at 100,000",
            small.steady_state_nanos,
            large.steady_state_nanos,
            small.activation_nanos,
            large.activation_nanos,
        );
    }

    // Once the index is live, the cache and the index must hold the same runs
    // after every removal, or eviction starts popping stamps with no entry
    // behind them and stops reclaiming. The first three inserts are what make
    // the index live — below the bound there is no index to keep in step.
    #[test]
    fn removing_a_cached_run_keeps_a_live_eviction_order_in_step() {
        let mut worker = cache_worker(2);
        let mut workflow = worker.workflow_worker();

        for run_id in ["run-a", "run-b", "run-c"] {
            workflow.insert_cached_workflow(RunId::new(run_id), cached_entry());
        }
        assert_eq!(
            workflow.state.cache_order.as_ref().map(BTreeMap::len),
            Some(2),
            "the first overflow must build the index from the surviving entries"
        );

        assert!(
            workflow
                .remove_cached_workflow(&RunId::new("run-b"))
                .is_some()
        );
        assert!(
            workflow
                .remove_cached_workflow(&RunId::new("run-b"))
                .is_none()
        );
        assert_eq!(
            workflow.state.cache_order.as_ref().map(BTreeMap::len),
            Some(1),
            "a claimed run must not leave its stamp in the eviction order"
        );

        // Two more inserts against a bound of two: the cache must settle at
        // the bound, evicting the oldest survivor rather than a dead key.
        workflow.insert_cached_workflow(RunId::new("run-d"), cached_entry());
        workflow.insert_cached_workflow(RunId::new("run-e"), cached_entry());

        assert_eq!(workflow.state.cache.len(), 2);
        assert_eq!(
            workflow.state.cache_order.as_ref().map(BTreeMap::len),
            Some(2)
        );
        assert!(workflow.state.cache.contains_key(&RunId::new("run-d")));
        assert!(workflow.state.cache.contains_key(&RunId::new("run-e")));
    }

    // A worker that never fills its cache never evicts, so it must not pay to
    // maintain an eviction index. This is the property that keeps the warm
    // cached-wake path free; maintaining the index unconditionally measured
    // +5.6% on `workflow_cached_wake_poll_memory`.
    #[test]
    fn the_eviction_order_stays_unbuilt_until_the_cache_overflows() {
        let mut worker = cache_worker(4);
        let mut workflow = worker.workflow_worker();

        for run_id in ["run-a", "run-b", "run-c", "run-d"] {
            workflow.insert_cached_workflow(RunId::new(run_id), cached_entry());
        }
        assert!(
            workflow.state.cache_order.is_none(),
            "a cache that has never overflowed must carry no eviction index"
        );

        workflow.insert_cached_workflow(RunId::new("run-e"), cached_entry());

        assert_eq!(workflow.state.cache.len(), 4);
        assert!(!workflow.state.cache.contains_key(&RunId::new("run-a")));
        assert_eq!(
            workflow.state.cache_order.as_ref().map(BTreeMap::len),
            Some(4),
            "the index must exist and match the cache once eviction has run"
        );
    }

    #[derive(Clone, Copy, Default)]
    struct ProbeWorkflow;

    impl Workflow for ProbeWorkflow {
        type Input = u64;
        type Output = u64;
        type QueryState = ();

        const NAME: &'static str = "worker.probe-workflow";
        const VERSION: u32 = 1;
        const RUST_PATH: &'static str = "durust::worker::tests::ProbeWorkflow";

        fn run(self, input: Self::Input) -> crate::BoxWorkflowFuture<Self::Output> {
            Box::pin(std::future::ready(Ok(input)))
        }
    }

    #[derive(Clone, Copy, Default)]
    struct ProbeActivity;

    impl crate::Activity for ProbeActivity {
        type Input = u64;
        type Output = u64;

        const NAME: &'static str = "worker.probe-activity";
        const RUST_PATH: &'static str = "durust::worker::tests::ProbeActivity";

        fn run(self, input: Self::Input) -> crate::BoxActivityFuture<Self::Output> {
            Box::pin(std::future::ready(Ok(input)))
        }
    }

    // The claim RPCs' registered-name filters are materialised at `build()`.
    // If they ever drift from the registry, a worker silently stops claiming
    // work for a registered type, which no other test would notice. The
    // registry is deliberately non-empty: an empty one matches an empty cached
    // list for the wrong reason.
    #[test]
    fn cached_registry_name_lists_match_the_registry() {
        let worker = Worker::builder(crate::MemoryBackend::new())
            .register_workflow(ProbeWorkflow)
            .register_activity(ProbeActivity)
            .build();

        assert_eq!(
            worker.shared.registered_workflow_types,
            vec![crate::WorkflowType::new("worker.probe-workflow", 1)]
        );
        assert_eq!(
            worker.shared.registered_activity_names,
            vec![crate::ActivityName::new("worker.probe-activity")]
        );
        assert_eq!(
            worker.shared.registered_workflow_types,
            worker.shared.registry.workflow_types()
        );
        assert_eq!(
            worker.shared.registered_activity_names,
            worker.shared.registry.activity_names()
        );
    }

    fn jitter_schedule(worker_id: &str, interval: Duration, delays: usize) -> Vec<Duration> {
        let mut jitter = MaintenanceJitter::new(&WorkerId::new(worker_id));
        (0..delays).map(|_| jitter.next_delay(interval)).collect()
    }

    // The whole point of the derivation: a fleet started at once must not scan
    // in lockstep, and it must not need a global RNG to avoid it. Two ids
    // diverge from their *first* delay, because the first delay is the phase
    // offset that breaks up startup load.
    #[test]
    fn maintenance_jitter_gives_two_worker_ids_different_phases() {
        let interval = Duration::from_millis(250);
        let first = jitter_schedule("worker-a", interval, 8);
        let second = jitter_schedule("worker-b", interval, 8);

        assert_ne!(
            first[0], second[0],
            "two worker ids must not share a startup phase: {first:?} vs {second:?}"
        );
        assert_ne!(first, second);
    }

    /// The Rust half of the shared behavioural corpus's `workerStartJitter`
    /// table; `typescript/packages/core/test/behavioral-corpus.test.ts` asserts
    /// the same rows against `maintenanceJitterSource`.
    ///
    /// The jitter stream is bit-for-bit identical across the two runtimes by
    /// construction — FNV-1a-32 over UTF-16 code units seeding mulberry32 —
    /// and until this table nothing in either CI job said so. It lives here
    /// rather than in `tests/` because `MaintenanceJitter` is private to this
    /// module and a Cargo integration test cannot name it.
    ///
    /// Exact equality, not a tolerance. The table stores the raw 32-bit
    /// mulberry32 outputs rather than the `[0, 1)` doubles, because the final
    /// division by `2^32` is exact in both runtimes while decimal text is not
    /// portable: `serde_json` parses `0.09126776782795787` to a double one ULP
    /// away from `f64::from_str`, which would have made a decimal table fail
    /// here for a reason that has nothing to do with jitter.
    #[test]
    fn maintenance_jitter_matches_the_shared_corpus_table() {
        let corpus: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/typescript/fixtures/contract/behavioral-corpus.json"
        )))
        .expect("behavioural corpus is valid JSON");
        let cases = corpus["workerStartJitter"]["cases"]
            .as_array()
            .expect("workerStartJitter cases");
        assert_eq!(cases.len(), 7, "the jitter table lost a worker id");
        let mut saw_astral = false;
        for case in cases {
            let worker_id = case["workerId"].as_str().expect("workerId");
            saw_astral |= worker_id.chars().any(|c| c as u32 > 0xFFFF);
            assert_eq!(
                u64::from(fnv1a32(worker_id)),
                case["seed"].as_u64().expect("seed"),
                "seed for `{worker_id}`"
            );
            let mut jitter = MaintenanceJitter::new(&WorkerId::new(worker_id));
            let expected: Vec<u64> = case["unitsScaled"]
                .as_array()
                .expect("unitsScaled")
                .iter()
                .map(|unit| unit.as_u64().expect("unit"))
                .collect();
            assert_eq!(expected.len(), 6, "units for `{worker_id}`");
            let actual: Vec<u64> = (0..expected.len())
                .map(|_| {
                    let unit = jitter.next_unit();
                    assert!((0.0..1.0).contains(&unit), "unit out of range: {unit}");
                    (unit * 4_294_967_296.0) as u64
                })
                .collect();
            assert_eq!(actual, expected, "jitter stream for `{worker_id}`");
        }
        assert!(
            saw_astral,
            "the table must keep an astral worker id: it is the only row a \
             UTF-8-byte hash would fail"
        );
    }

    // Deterministic from the id alone: the same worker reproduces its schedule
    // across processes, which is what makes a scan cadence reviewable instead
    // of a coin flip.
    #[test]
    fn maintenance_jitter_is_reproducible_for_one_worker_id() {
        let interval = Duration::from_millis(250);

        assert_eq!(
            jitter_schedule("worker-a", interval, 16),
            jitter_schedule("worker-a", interval, 16)
        );
    }

    // The spread is bounded on both sides: half the interval at the fastest, so
    // provider load stays bounded by the configured cadence, and under 1.5x at
    // the slowest, so timer latency stays bounded too.
    #[test]
    fn maintenance_jitter_stays_within_half_and_one_and_a_half_intervals() {
        let interval = Duration::from_millis(1_000);
        for worker in ["a", "worker-1", "worker-2", "durust-worker-abcdef"] {
            for delay in jitter_schedule(worker, interval, 64) {
                assert!(
                    delay >= interval / 2 && delay < interval * 3 / 2,
                    "{worker}: {delay:?} outside [0.5x, 1.5x) of {interval:?}"
                );
            }
        }
    }

    // A worker that opted out of pacing keeps opting out; the loop turns that
    // into a yield rather than a sleep.
    #[test]
    fn maintenance_jitter_leaves_a_zero_interval_alone() {
        assert_eq!(
            jitter_schedule("worker-a", Duration::ZERO, 4),
            vec![Duration::ZERO; 4]
        );
    }

    // The doubling that separates a busy worker from an idle one, and the
    // ceiling that keeps an idle worker's latency bounded.
    #[test]
    fn next_backoff_doubles_to_the_ceiling_and_leaves_zero_alone() {
        let max = Duration::from_millis(1_000);
        assert_eq!(
            next_backoff(Duration::from_millis(250), max),
            Duration::from_millis(500)
        );
        assert_eq!(next_backoff(Duration::from_millis(500), max), max);
        assert_eq!(next_backoff(max, max), max);
        assert_eq!(next_backoff(Duration::ZERO, max), Duration::ZERO);
        // A ceiling below the current value clamps down rather than overflowing.
        assert_eq!(
            next_backoff(Duration::MAX, Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }

    // FNV-1a is the seed, so ids that differ anywhere seed different streams.
    // ASCII code units equal ASCII bytes, so the published byte vectors still
    // pin the arithmetic.
    #[test]
    fn fnv1a32_matches_its_reference_vectors() {
        assert_eq!(fnv1a32(""), 0x811c_9dc5);
        assert_eq!(fnv1a32("a"), 0xe40c_292c);
        assert_eq!(fnv1a32("foobar"), 0xbf9c_f968);
    }

    // The seed is hashed over UTF-16 code units because TypeScript hashes
    // `charCodeAt` values. A UTF-8 byte hash agrees on ASCII and diverges
    // everywhere else, which is the silent cross-runtime split this pins shut.
    #[test]
    fn fnv1a32_hashes_utf16_code_units_not_utf8_bytes() {
        fn over_utf8_bytes(value: &str) -> u32 {
            let mut hash = 0x811c_9dc5_u32;
            for byte in value.as_bytes() {
                hash ^= u32::from(*byte);
                hash = hash.wrapping_mul(0x0100_0193);
            }
            hash
        }

        for ascii in ["", "worker", "durust-worker-7"] {
            assert_eq!(fnv1a32(ascii), over_utf8_bytes(ascii), "{ascii}");
        }
        for non_ascii in ["wörker", "ワーカー", "worker-🙂"] {
            assert_ne!(
                fnv1a32(non_ascii),
                over_utf8_bytes(non_ascii),
                "{non_ascii}"
            );
        }
        // Spot-check one against the code units JavaScript would see.
        let mut expected = 0x811c_9dc5_u32;
        for unit in [u32::from(b'w'), 0x00f6, u32::from(b'r')] {
            expected ^= unit;
            expected = expected.wrapping_mul(0x0100_0193);
        }
        assert_eq!(fnv1a32("wör"), expected);
    }
}
