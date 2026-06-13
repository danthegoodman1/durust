use crate::{
    ActivityId, ActivityMapTask, ActivityName, ActivityTask, DurableFailure, Error, EventId,
    Namespace, NewHistoryEvent, PayloadRef, Result, RunId, SignalId, SignalName, TaskQueue,
    TimestampMs, WaitId, WorkerId, WorkflowId, WorkflowType,
};
use futures::future::BoxFuture;
use std::time::Duration;

pub trait DurableBackend: Clone + Send + Sync + 'static {
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

    fn stream_history(&self, req: StreamHistoryRequest)
    -> BoxFuture<'static, Result<HistoryChunk>>;

    fn commit_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> BoxFuture<'static, Result<CommitOutcome>>;

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

    fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedActivityTask>>>;

    fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> BoxFuture<'static, Result<CompleteActivityOutcome>>;

    fn fail_activity(
        &self,
        req: FailActivityRequest,
    ) -> BoxFuture<'static, Result<FailActivityOutcome>>;

    fn query_projection(
        &self,
        req: QueryProjectionRequest,
    ) -> BoxFuture<'static, Result<QueryProjectionOutcome>>;
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowTaskReason {
    WorkflowStarted,
    ActivityCompleted,
    ActivityFailed,
    ActivityTimedOut,
    ActivityMapCompleted,
    ActivityMapFailed,
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
    pub consume_signals: Vec<SignalId>,
    pub delete_waits: Vec<WaitId>,
    pub cancel_commands: Vec<crate::CommandId>,
    pub query_projection: Option<PayloadRef>,
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
pub struct CompleteActivityRequest {
    pub claim: ActivityTaskClaim,
    pub result: PayloadRef,
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

pub fn conflict_to_error(outcome: CommitOutcome) -> Result<EventId> {
    match outcome {
        CommitOutcome::Committed { new_tail_event_id } => Ok(new_tail_event_id),
        CommitOutcome::Conflict => Err(Error::Conflict),
    }
}
