//! Concurrency budgets for the worker's per-task hot path.
//!
//! The worker batches its endpoints — one claim RPC, one commit RPC, one
//! batched signal-inbox read — but the work between them is per-run and
//! independent: separate history reads, separate payload hydrations, separate
//! cold replays. Running that middle serially is invisible to a behavioural
//! test, because a serial pipeline and a concurrent one commit identical
//! history. It only shows up as latency, so it needs a guard that measures
//! *overlap* rather than behaviour.
//!
//! Each test here wraps `MemoryBackend` in a decorator that records how many
//! backend calls are in flight at once, and asserts the peak. A regression to
//! serial execution drops every peak to 1.

use durust::provider::*;
use durust::{Client, DurableBranchExt, MemoryBackend, PayloadStorageConfig, Worker};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Long enough that a serial pipeline cannot finish inside one call's window,
/// so overlap is real rather than an artifact of the timer granularity.
const CALL_LATENCY: Duration = Duration::from_millis(2);

#[derive(Clone, Default)]
struct Gauge {
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}

impl Gauge {
    fn enter(&self) -> InFlight {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let now = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(now, Ordering::Relaxed);
        InFlight(self.in_flight.clone())
    }
    fn reset(&self) {
        self.peak.store(0, Ordering::Relaxed);
        self.calls.store(0, Ordering::Relaxed);
    }
    fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

struct InFlight(Arc<AtomicUsize>);
impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct GaugedBackend<B: DurableBackend> {
    inner: B,
    gauge: Gauge,
}

macro_rules! gauged {
    ($( fn $name:ident (&self $(, $arg:ident : $ty:ty)* $(,)? ) -> $ret:ty; )*) => {
        $(
            fn $name(&self $(, $arg : $ty)*) -> BoxFuture<'static, durust::Result<$ret>> {
                let inner = self.inner.clone();
                let gauge = self.gauge.clone();
                Box::pin(async move {
                    let _guard = gauge.enter();
                    tokio::time::sleep(CALL_LATENCY).await;
                    inner.$name($($arg),*).await
                })
            }
        )*
    };
}

impl<B: DurableBackend> DurableBackend for GaugedBackend<B> {
    fn payload_storage_config(&self) -> PayloadStorageConfig {
        self.inner.payload_storage_config()
    }

    // Itself a bounded park, so charging it latency would measure the wait
    // rather than the hot path.
    fn wait_for_ready(&self, req: WaitForReadyRequest) -> BoxFuture<'static, durust::Result<()>> {
        self.inner.wait_for_ready(req)
    }

    gauged! {
        fn start_workflow(&self, req: StartWorkflowRequest) -> StartWorkflowOutcome;
        fn cancel_workflow(&self, req: CancelWorkflowRequest) -> CancelWorkflowOutcome;
        fn current_time(&self) -> durust::TimestampMs;
        fn claim_workflow_task(&self, worker_id: durust::WorkerId, opts: ClaimWorkflowTaskOptions) -> Option<ClaimedWorkflowTask>;
        fn claim_workflow_tasks(&self, worker_id: durust::WorkerId, opts: ClaimWorkflowTasksOptions) -> Vec<ClaimedWorkflowTask>;
        fn stream_history(&self, req: StreamHistoryRequest) -> HistoryChunk;
        fn stream_history_for_replay(&self, req: StreamHistoryRequest) -> HistoryChunk;
        fn hydrate_payload(&self, payload: durust::PayloadRef) -> durust::PayloadRef;
        fn hydrate_activity_map_result_manifest(&self, payload: durust::PayloadRef) -> durust::PayloadRef;
        fn hydrate_child_workflow_map_result_manifest(&self, payload: durust::PayloadRef) -> durust::PayloadRef;
        fn commit_workflow_task(&self, claim: WorkflowTaskClaim, batch: WorkflowTaskCommit) -> durust::EventId;
        fn commit_workflow_tasks(&self, batch: WorkflowTaskCommitBatch) -> Vec<WorkflowTaskCommitBatchResult>;
        fn release_workflow_task(&self, claim: WorkflowTaskClaim, release: WorkflowTaskRelease) -> ();
        fn signal_workflow(&self, req: SignalWorkflowRequest) -> SignalWorkflowOutcome;
        fn read_signal_inbox(&self, req: ReadSignalInboxRequest) -> Option<SignalInboxRecord>;
        fn read_signal_inboxes(&self, req: ReadSignalInboxesRequest) -> Vec<Option<SignalInboxRecord>>;
        fn fire_due_timers(&self, req: FireDueTimersRequest) -> FireDueTimersOutcome;
        fn timeout_due_activities(&self, req: TimeoutDueActivitiesRequest) -> TimeoutDueActivitiesOutcome;
        fn claim_activity_task(&self, worker_id: durust::WorkerId, opts: ClaimActivityOptions) -> Option<ClaimedActivityTask>;
        fn claim_activity_tasks(&self, worker_id: durust::WorkerId, opts: ClaimActivityTasksOptions) -> Vec<ClaimedActivityTask>;
        fn heartbeat_activity(&self, req: ActivityHeartbeatRequest) -> ActivityHeartbeatOutcome;
        fn complete_activity(&self, req: CompleteActivityRequest) -> CompleteActivityOutcome;
        fn complete_activity_tasks(&self, req: CompleteActivityTasksRequest) -> Vec<CompleteActivityTaskBatchResult>;
        fn fail_activity(&self, req: FailActivityRequest) -> FailActivityOutcome;
        fn dispatch_child_workflow_starts(&self, req: DispatchChildWorkflowStartsRequest) -> DispatchChildWorkflowStartsOutcome;
        fn query_projection(&self, req: QueryProjectionRequest) -> QueryProjectionOutcome;
        fn workflow_change_versions(&self, req: WorkflowChangeVersionsRequest) -> WorkflowChangeVersionsOutcome;
        fn payload_roots(&self) -> PayloadRootsOutcome;
        fn gc_payload_blobs(&self, req: PayloadGarbageCollectionRequest) -> PayloadGarbageCollectionOutcome;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Unit {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Bulk {
    bytes: Vec<u8>,
}

#[durust::activity(name = "concurrency-budget.bulk")]
async fn bulk(input: Bulk) -> durust::Result<Bulk> {
    Ok(input)
}

const BATCH: usize = 16;

#[durust::workflow(name = "concurrency-budget.one-activity", version = 1)]
async fn one_activity(_input: Unit) -> durust::Result<usize> {
    let out = durust::call_activity!(bulk(Bulk {
        bytes: vec![1u8; 8]
    }))
    .task_queue("activities")
    .await?;
    Ok(out.bytes.len())
}

const FANOUT: usize = 24;
/// Above the backend's inline threshold below, so each result offloads and the
/// resumed task has to hydrate it.
const BLOB_BYTES: usize = 32 * 1024;

#[durust::workflow(name = "concurrency-budget.fanout", version = 1)]
async fn fanout(_input: Unit) -> durust::Result<usize> {
    let mut branches: Vec<durust::BoxSelectBranch<usize>> = Vec::new();
    for index in 0..FANOUT {
        let handle = durust::call_activity!(bulk(Bulk {
            // Distinct per item: identical payloads share one content-addressed
            // hydration key, which is the dedup case rather than the fanout one.
            bytes: vec![index as u8; BLOB_BYTES + index],
        }))
        .task_queue("activities")
        .spawn()
        .await?;
        branches.push(handle.result().map_ok(|out: Bulk| out.bytes.len()).boxed());
    }
    Ok(durust::join_all(branches).await?.into_iter().sum())
}

/// One shared payload across the whole fanout, so the content-addressed
/// hydration key is the same for every consumer.
#[durust::workflow(name = "concurrency-budget.shared-blob", version = 1)]
async fn shared_blob(_input: Unit) -> durust::Result<usize> {
    let mut branches: Vec<durust::BoxSelectBranch<usize>> = Vec::new();
    for _ in 0..FANOUT {
        let handle = durust::call_activity!(bulk(Bulk {
            bytes: vec![7u8; BLOB_BYTES],
        }))
        .task_queue("activities")
        .spawn()
        .await?;
        branches.push(handle.result().map_ok(|out: Bulk| out.bytes.len()).boxed());
    }
    Ok(durust::join_all(branches).await?.into_iter().sum())
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime")
}

/// A batch of workflow tasks prepares its runs concurrently.
///
/// Each run in a claimed batch is a different workflow: its history read, its
/// input hydration, and its cold replay share nothing with its neighbours'.
/// Preparing them one at a time serialises every one of those round trips
/// behind the batch.
#[test]
fn a_batch_prepares_its_runs_concurrently() {
    runtime().block_on(async {
        let gauge = Gauge::default();
        let backend = GaugedBackend {
            inner: MemoryBackend::with_payload_storage(
                PayloadStorageConfig::new().inline_threshold_bytes(1024),
            ),
            gauge: gauge.clone(),
        };
        let client = Client::new(backend.clone());
        for index in 0..BATCH {
            client
                .start_workflow::<one_activity>(
                    format!("concurrency-budget/batch-{index}"),
                    "workflows",
                    Unit {},
                )
                .await
                .unwrap();
        }
        let mut worker = Worker::builder(backend.clone())
            .worker_id("concurrency-budget-batch")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(one_activity)
            .register_activity(bulk)
            .max_concurrent_workflow_tasks(BATCH)
            .workflow_task_prefetch_limit(BATCH)
            .workflow_task_commit_batch_size(BATCH)
            .build();

        gauge.reset();
        assert_eq!(worker.run_workflow_batch_once().await.unwrap(), BATCH);
        assert!(
            gauge.peak() > 1,
            "a batch of {BATCH} cold-starting runs prepared with at most {} backend call in \
             flight, so the stage ran serially. Each run's history read and input hydration is \
             independent; preparing them one at a time puts every round trip on the batch's \
             critical path, and makes `max_concurrent_recoveries` unreachable because the \
             counter can never exceed one.",
            gauge.peak()
        );
    });
}

/// A cached batch costs a fixed number of backend calls, not a number that
/// grows with the batch.
///
/// Claim, clock, commit. Everything else is served by the claim's prefetched
/// history, so a per-task backend call in this path is a round trip the batch
/// did not need.
#[test]
fn a_cached_batch_costs_a_fixed_number_of_backend_calls() {
    runtime().block_on(async {
        let gauge = Gauge::default();
        let backend = GaugedBackend {
            inner: MemoryBackend::new(),
            gauge: gauge.clone(),
        };
        let client = Client::new(backend.clone());
        for index in 0..BATCH {
            client
                .start_workflow::<one_activity>(
                    format!("concurrency-budget/cached-{index}"),
                    "workflows",
                    Unit {},
                )
                .await
                .unwrap();
        }
        let mut worker = Worker::builder(backend.clone())
            .worker_id("concurrency-budget-cached")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(one_activity)
            .register_activity(bulk)
            .max_concurrent_workflow_tasks(BATCH)
            .workflow_task_prefetch_limit(BATCH)
            .workflow_task_commit_batch_size(BATCH)
            .build();

        assert_eq!(worker.run_workflow_batch_once().await.unwrap(), BATCH);
        while worker.run_activity_batch_once().await.unwrap() > 0 {}

        gauge.reset();
        assert_eq!(worker.run_workflow_batch_once().await.unwrap(), BATCH);
        // Claim, clock, commit. The bound is deliberately loose: what it
        // catches is a call that scales with the batch.
        assert!(
            gauge.calls() <= 6,
            "a cached batch of {BATCH} runs cost {} backend calls. This path should cost a fixed \
             few — one claim, one clock reading, one commit — with the claim's prefetched history \
             serving every run. A count that tracks the batch size means a per-task call came \
             back onto it.",
            gauge.calls()
        );
    });
}

/// A resumed fanout hydrates its offloaded results concurrently.
///
/// One poll blocks on every item at once and the runtime collects the requests
/// into one batch; draining that batch one at a time costs a provider round
/// trip per item on a single workflow task's critical path.
#[test]
fn a_resumed_fanout_hydrates_its_payloads_concurrently() {
    runtime().block_on(async {
        let gauge = Gauge::default();
        let backend = GaugedBackend {
            inner: MemoryBackend::with_payload_storage(
                PayloadStorageConfig::new().inline_threshold_bytes(1024),
            ),
            gauge: gauge.clone(),
        };
        Client::new(backend.clone())
            .start_workflow::<fanout>("concurrency-budget/fanout", "workflows", Unit {})
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("concurrency-budget-fanout")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(fanout)
            .register_activity(bulk)
            .history_chunk_events(4_096)
            .history_chunk_bytes(64 * 1024 * 1024)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        while worker.run_activity_batch_once().await.unwrap() > 0 {}

        gauge.reset();
        assert!(worker.run_workflow_once().await.unwrap());
        assert!(
            gauge.peak() > 1,
            "a resumed fanout over {FANOUT} offloaded results hydrated them with at most {} \
             request in flight. They are independent blob reads collected into one batch, so a \
             serial drain puts one provider round trip per item on this task's critical path.",
            gauge.peak()
        );
    });
}

/// Consumers that share one payload hydrate it once.
///
/// Hydration is keyed by payload content, so a fanout whose items produced the
/// same value makes one request. Handing the single hydrated value to the
/// first consumer and making the rest re-request it costs one provider read
/// per consumer — worse than distinct payloads, which at least overlap.
#[test]
fn consumers_sharing_a_payload_hydrate_it_once() {
    runtime().block_on(async {
        let gauge = Gauge::default();
        let backend = GaugedBackend {
            inner: MemoryBackend::with_payload_storage(
                PayloadStorageConfig::new().inline_threshold_bytes(1024),
            ),
            gauge: gauge.clone(),
        };
        Client::new(backend.clone())
            .start_workflow::<shared_blob>("concurrency-budget/shared", "workflows", Unit {})
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("concurrency-budget-shared")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(shared_blob)
            .register_activity(bulk)
            .history_chunk_events(4_096)
            .history_chunk_bytes(64 * 1024 * 1024)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        while worker.run_activity_batch_once().await.unwrap() > 0 {}

        gauge.reset();
        assert!(worker.run_workflow_once().await.unwrap());
        // Claim, clock, one history read, one hydration, commit — well under
        // the one-read-per-consumer this guards against.
        assert!(
            gauge.calls() < FANOUT,
            "{FANOUT} consumers of one shared payload cost {} backend calls. They share a \
             content-addressed hydration key, so the payload should be read once; a count near \
             the consumer count means each consumer claimed the hydrated value and the rest \
             re-requested it.",
            gauge.calls()
        );
    });
}
