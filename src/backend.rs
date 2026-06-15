use crate::{
    ActivityId, ActivityMapTask, ActivityName, ActivityTask, ChildStartOutboxMessage,
    DurableFailure, Error, EventId, Namespace, NewHistoryEvent, PayloadRef, PayloadStorageConfig,
    Result, RunId, ShardId, SignalId, SignalName, TaskQueue, TimestampMs, WaitId, WorkerId,
    WorkflowId, WorkflowType,
};
use futures::future::BoxFuture;
use std::time::Duration;

pub trait DurableBackend: Clone + Send + Sync + 'static {
    fn payload_storage_config(&self) -> PayloadStorageConfig {
        PayloadStorageConfig::default()
    }

    fn start_workflow(
        &self,
        req: StartWorkflowRequest,
    ) -> BoxFuture<'static, Result<StartWorkflowOutcome>>;

    fn cancel_workflow(
        &self,
        req: CancelWorkflowRequest,
    ) -> BoxFuture<'static, Result<CancelWorkflowOutcome>>;

    fn current_time(&self) -> BoxFuture<'static, Result<TimestampMs>>;

    fn claim_workflow_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedWorkflowTask>>>;

    fn claim_workflow_tasks(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTasksOptions,
    ) -> BoxFuture<'static, Result<Vec<ClaimedWorkflowTask>>> {
        let backend = self.clone();
        Box::pin(async move {
            if opts.shard_filter.is_some() {
                return Err(Error::Backend(
                    "workflow task shard filters require a shard-aware backend".to_owned(),
                ));
            }

            let mut claimed = Vec::new();
            for _ in 0..opts.limit {
                let Some(task) = backend
                    .claim_workflow_task(worker_id.clone(), opts.claim.clone())
                    .await?
                else {
                    break;
                };
                claimed.push(task);
            }
            Ok(claimed)
        })
    }

    fn stream_history(&self, req: StreamHistoryRequest)
    -> BoxFuture<'static, Result<HistoryChunk>>;

    fn stream_history_for_replay(
        &self,
        req: StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        self.stream_history(req)
    }

    fn hydrate_payload(&self, payload: PayloadRef) -> BoxFuture<'static, Result<PayloadRef>> {
        Box::pin(async move { Ok(payload) })
    }

    fn hydrate_activity_map_result_manifest(
        &self,
        payload: PayloadRef,
    ) -> BoxFuture<'static, Result<PayloadRef>> {
        self.hydrate_payload(payload)
    }

    fn commit_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> BoxFuture<'static, Result<CommitOutcome>>;

    fn commit_workflow_tasks(
        &self,
        batch: WorkflowTaskCommitBatch,
    ) -> BoxFuture<'static, Result<Vec<WorkflowTaskCommitBatchResult>>> {
        let backend = self.clone();
        Box::pin(async move {
            let mut results = Vec::with_capacity(batch.commits.len());
            for input in batch.commits {
                let claim = input.claim;
                let result = backend
                    .commit_workflow_task(claim.clone(), input.commit)
                    .await;
                results.push(WorkflowTaskCommitBatchResult { claim, result });
            }
            Ok(results)
        })
    }

    fn release_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        release: WorkflowTaskRelease,
    ) -> BoxFuture<'static, Result<()>>;

    fn signal_workflow(
        &self,
        req: SignalWorkflowRequest,
    ) -> BoxFuture<'static, Result<SignalWorkflowOutcome>>;

    fn read_signal_inbox(
        &self,
        req: ReadSignalInboxRequest,
    ) -> BoxFuture<'static, Result<Option<SignalInboxRecord>>>;

    fn fire_due_timers(
        &self,
        req: FireDueTimersRequest,
    ) -> BoxFuture<'static, Result<FireDueTimersOutcome>>;

    fn timeout_due_activities(
        &self,
        req: TimeoutDueActivitiesRequest,
    ) -> BoxFuture<'static, Result<TimeoutDueActivitiesOutcome>>;

    fn run_due_maintenance(
        &self,
        req: RunDueMaintenanceRequest,
    ) -> BoxFuture<'static, Result<RunDueMaintenanceOutcome>> {
        let backend = self.clone();
        Box::pin(async move {
            let timers = backend
                .fire_due_timers(FireDueTimersRequest {
                    namespace: req.namespace.clone(),
                    now: req.now,
                    limit: req.timer_limit,
                })
                .await?;
            let activities = backend
                .timeout_due_activities(TimeoutDueActivitiesRequest {
                    namespace: req.namespace,
                    now: req.now,
                    limit: req.activity_limit,
                })
                .await?;
            Ok(RunDueMaintenanceOutcome {
                timers_fired: timers.fired,
                activities_timed_out: activities.timed_out,
            })
        })
    }

    fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedActivityTask>>>;

    fn claim_activity_tasks(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityTasksOptions,
    ) -> BoxFuture<'static, Result<Vec<ClaimedActivityTask>>> {
        let backend = self.clone();
        Box::pin(async move {
            let mut claimed = Vec::new();
            for _ in 0..opts.limit {
                let Some(task) = backend
                    .claim_activity_task(worker_id.clone(), opts.claim.clone())
                    .await?
                else {
                    break;
                };
                claimed.push(task);
            }
            Ok(claimed)
        })
    }

    fn heartbeat_activity(
        &self,
        req: ActivityHeartbeatRequest,
    ) -> BoxFuture<'static, Result<ActivityHeartbeatOutcome>>;

    fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> BoxFuture<'static, Result<CompleteActivityOutcome>>;

    fn complete_activity_tasks(
        &self,
        req: CompleteActivityTasksRequest,
    ) -> BoxFuture<'static, Result<Vec<CompleteActivityTaskBatchResult>>> {
        let backend = self.clone();
        Box::pin(async move {
            let mut results = Vec::with_capacity(req.completions.len());
            for completion in req.completions {
                let claim = completion.claim.clone();
                let result = backend.complete_activity(completion).await;
                results.push(CompleteActivityTaskBatchResult { claim, result });
            }
            Ok(results)
        })
    }

    fn fail_activity(
        &self,
        req: FailActivityRequest,
    ) -> BoxFuture<'static, Result<FailActivityOutcome>>;

    fn dispatch_child_workflow_starts(
        &self,
        req: DispatchChildWorkflowStartsRequest,
    ) -> BoxFuture<'static, Result<DispatchChildWorkflowStartsOutcome>>;

    fn query_projection(
        &self,
        req: QueryProjectionRequest,
    ) -> BoxFuture<'static, Result<QueryProjectionOutcome>>;

    fn workflow_change_versions(
        &self,
        req: WorkflowChangeVersionsRequest,
    ) -> BoxFuture<'static, Result<WorkflowChangeVersionsOutcome>>;

    fn payload_roots(&self) -> BoxFuture<'static, Result<PayloadRootsOutcome>>;

    fn gc_payload_blobs(
        &self,
        req: PayloadGarbageCollectionRequest,
    ) -> BoxFuture<'static, Result<PayloadGarbageCollectionOutcome>>;
}

#[derive(Clone, Debug)]
pub struct StartWorkflowRequest {
    pub namespace: Namespace,
    pub workflow_id: WorkflowId,
    pub workflow_type: WorkflowType,
    pub task_queue: TaskQueue,
    pub input: PayloadRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartWorkflowOutcome {
    Started { run_id: RunId },
    AlreadyStarted { run_id: RunId },
}

impl StartWorkflowOutcome {
    pub fn run_id(&self) -> &RunId {
        match self {
            Self::Started { run_id } | Self::AlreadyStarted { run_id } => run_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CancelWorkflowRequest {
    pub namespace: Namespace,
    pub workflow_id: WorkflowId,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CancelWorkflowOutcome {
    Cancelled { run_id: RunId, event_id: EventId },
    AlreadyTerminal { run_id: RunId },
}

#[derive(Clone, Debug)]
pub struct ClaimWorkflowTaskOptions {
    pub namespace: Namespace,
    pub task_queue: TaskQueue,
    pub registered_workflow_types: Vec<WorkflowType>,
    pub lease_duration: Duration,
}

#[derive(Clone, Debug)]
pub struct ClaimWorkflowTasksOptions {
    pub claim: ClaimWorkflowTaskOptions,
    pub limit: usize,
    pub shard_filter: Option<Vec<ShardId>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowTaskReason {
    WorkflowStarted,
    ActivityCompleted,
    ActivityFailed,
    ActivityTimedOut,
    ActivityMapCompleted,
    ActivityMapFailed,
    ChildWorkflowStarted,
    ChildWorkflowCompleted,
    ChildWorkflowFailed,
    ChildWorkflowCancelled,
    TimerFired,
    SignalReceived,
    CacheEvicted,
}

#[derive(Clone, Debug)]
pub struct WorkflowTaskRelease {
    pub reason: WorkflowTaskReason,
    pub delay: Duration,
}

impl WorkflowTaskRelease {
    pub fn immediate(reason: WorkflowTaskReason) -> Self {
        Self {
            reason,
            delay: Duration::ZERO,
        }
    }

    pub fn delayed(reason: WorkflowTaskReason, delay: Duration) -> Self {
        Self { reason, delay }
    }
}

#[derive(Clone, Debug)]
pub struct WorkflowTaskClaim {
    pub run_id: RunId,
    pub worker_id: WorkerId,
    pub token: u64,
}

#[derive(Clone, Debug)]
pub struct ClaimedWorkflowTask {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    pub workflow_type: WorkflowType,
    pub claim: WorkflowTaskClaim,
    pub replay_target_event_id: EventId,
    pub reason: WorkflowTaskReason,
    pub prefetched_history: Vec<crate::HistoryEvent>,
}

#[derive(Clone, Debug)]
pub struct StreamHistoryRequest {
    pub run_id: RunId,
    pub after_event_id: EventId,
    pub up_to_event_id: EventId,
    pub max_events: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct HistoryChunk {
    pub events: Vec<crate::HistoryEvent>,
    pub last_event_id: EventId,
    pub has_more: bool,
}

#[derive(Clone, Debug, Default)]
pub struct WorkflowTaskCommit {
    pub expected_tail_event_id: EventId,
    pub append_events: Vec<NewHistoryEvent>,
    pub upsert_waits: Vec<WaitRecord>,
    pub schedule_activities: Vec<ActivityTask>,
    pub schedule_activity_maps: Vec<ActivityMapTask>,
    pub start_child_workflows: Vec<ChildStartOutboxMessage>,
    pub consume_signals: Vec<SignalId>,
    pub delete_waits: Vec<WaitId>,
    pub cancel_commands: Vec<crate::CommandId>,
    pub query_projection: Option<PayloadRef>,
}

#[derive(Clone, Debug, Default)]
pub struct WorkflowTaskCommitBatch {
    pub commits: Vec<WorkflowTaskCommitInput>,
}

#[derive(Clone, Debug)]
pub struct WorkflowTaskCommitInput {
    pub claim: WorkflowTaskClaim,
    pub commit: WorkflowTaskCommit,
}

#[derive(Clone, Debug)]
pub struct WorkflowTaskCommitBatchResult {
    pub claim: WorkflowTaskClaim,
    pub result: Result<CommitOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Committed { new_tail_event_id: EventId },
    Conflict,
}

#[derive(Clone, Debug)]
pub struct ClaimActivityOptions {
    pub namespace: Namespace,
    pub task_queue: TaskQueue,
    pub registered_activity_names: Vec<ActivityName>,
    pub lease_duration: Duration,
}

#[derive(Clone, Debug)]
pub struct ClaimActivityTasksOptions {
    pub claim: ClaimActivityOptions,
    pub limit: usize,
}

#[derive(Clone, Debug)]
pub struct ActivityTaskClaim {
    pub activity_id: ActivityId,
    pub worker_id: WorkerId,
    pub token: u64,
}

#[derive(Clone, Debug)]
pub struct ClaimedActivityTask {
    pub task: ActivityTask,
    pub claim: ActivityTaskClaim,
}

#[derive(Clone, Debug)]
pub struct ActivityHeartbeatRequest {
    pub claim: ActivityTaskClaim,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivityHeartbeatOutcome {
    Recorded,
    AlreadyCompleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WaitKind {
    Timer,
    Signal,
}

#[derive(Clone, Debug)]
pub struct WaitRecord {
    pub wait_id: WaitId,
    pub run_id: RunId,
    pub command_id: crate::CommandId,
    pub kind: WaitKind,
    pub key: String,
    pub ready_at: Option<TimestampMs>,
}

#[derive(Clone, Debug)]
pub struct SignalWorkflowRequest {
    pub namespace: Namespace,
    pub workflow_id: WorkflowId,
    pub signal_id: SignalId,
    pub signal_name: SignalName,
    pub payload: PayloadRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignalWorkflowOutcome {
    Accepted,
    Duplicate,
}

#[derive(Clone, Debug)]
pub struct ReadSignalInboxRequest {
    pub run_id: RunId,
    pub signal_name: SignalName,
}

#[derive(Clone, Debug)]
pub struct SignalInboxRecord {
    pub signal_id: SignalId,
    pub signal_name: SignalName,
    pub payload: PayloadRef,
}

#[derive(Clone, Debug)]
pub struct FireDueTimersRequest {
    pub namespace: Namespace,
    pub now: TimestampMs,
    pub limit: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FireDueTimersOutcome {
    pub fired: usize,
}

#[derive(Clone, Debug)]
pub struct TimeoutDueActivitiesRequest {
    pub namespace: Namespace,
    pub now: TimestampMs,
    pub limit: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimeoutDueActivitiesOutcome {
    pub timed_out: usize,
}

#[derive(Clone, Debug)]
pub struct RunDueMaintenanceRequest {
    pub namespace: Namespace,
    pub now: TimestampMs,
    pub timer_limit: usize,
    pub activity_limit: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunDueMaintenanceOutcome {
    pub timers_fired: usize,
    pub activities_timed_out: usize,
}

#[derive(Clone, Debug)]
pub struct CompleteActivityRequest {
    pub claim: ActivityTaskClaim,
    pub result: PayloadRef,
}

#[derive(Clone, Debug)]
pub struct CompleteActivityTasksRequest {
    pub completions: Vec<CompleteActivityRequest>,
}

#[derive(Debug)]
pub struct CompleteActivityTaskBatchResult {
    pub claim: ActivityTaskClaim,
    pub result: Result<CompleteActivityOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompleteActivityOutcome {
    Completed { event_id: EventId },
    AlreadyCompleted,
}

#[derive(Clone, Debug)]
pub struct FailActivityRequest {
    pub claim: ActivityTaskClaim,
    pub failure: DurableFailure,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailActivityOutcome {
    RetryScheduled { next_attempt: u32 },
    Failed { event_id: EventId },
    AlreadyCompleted,
}

#[derive(Clone, Debug)]
pub struct DispatchChildWorkflowStartsRequest {
    pub namespace: Namespace,
    pub limit: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DispatchChildWorkflowStartsOutcome {
    pub dispatched: usize,
}

#[derive(Clone, Debug)]
pub struct QueryProjectionRequest {
    pub namespace: Namespace,
    pub workflow_id: WorkflowId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryProjectionOutcome {
    Found {
        run_id: RunId,
        event_id: EventId,
        payload: PayloadRef,
    },
    NotFound,
}

#[derive(Clone, Debug, Default)]
pub struct WorkflowChangeVersionsRequest {
    pub namespace: Namespace,
    pub workflow_id: Option<WorkflowId>,
    pub run_id: Option<RunId>,
    pub change_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowChangeVersionRecord {
    pub namespace: Namespace,
    pub workflow_id: WorkflowId,
    pub workflow_type: WorkflowType,
    pub run_id: RunId,
    pub change_id: String,
    pub version: i32,
    pub marker_kind: WorkflowChangeMarkerKind,
    pub status: WorkflowChangeVersionStatus,
    pub command_seq: crate::CommandSeq,
    pub first_event_id: EventId,
    pub last_seen_at: TimestampMs,
}

#[derive(Clone, Debug, Default)]
pub struct PayloadGarbageCollectionRequest {
    pub dry_run: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PayloadRootsOutcome {
    pub roots: Vec<PayloadRootRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PayloadRootRef {
    Payload(PayloadRef),
    ActivityMapInputManifest(PayloadRef),
    ActivityMapResultManifest(PayloadRef),
}

impl PayloadRootRef {
    pub fn payload(&self) -> &PayloadRef {
        match self {
            Self::Payload(payload)
            | Self::ActivityMapInputManifest(payload)
            | Self::ActivityMapResultManifest(payload) => payload,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PayloadGarbageCollectionOutcome {
    pub scanned_blobs: usize,
    pub retained_blobs: usize,
    pub deleted_blobs: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowChangeMarkerKind {
    Version,
    DeprecatedPatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowChangeVersionStatus {
    Open,
    Closed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkflowChangeVersionsOutcome {
    pub records: Vec<WorkflowChangeVersionRecord>,
}

impl WorkflowChangeVersionsOutcome {
    pub fn safe_to_remove(&self) -> bool {
        self.records
            .iter()
            .all(|record| record.status == WorkflowChangeVersionStatus::Closed)
    }
}

pub fn conflict_to_error(outcome: CommitOutcome) -> Result<EventId> {
    match outcome {
        CommitOutcome::Committed { new_tail_event_id } => Ok(new_tail_event_id),
        CommitOutcome::Conflict => Err(Error::Conflict),
    }
}
