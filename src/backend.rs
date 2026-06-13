use crate::{
    ActivityId, ActivityName, ActivityTask, Error, EventId, Namespace, NewHistoryEvent, PayloadRef,
    Result, RunId, TaskQueue, WaitId, WorkerId, WorkflowId, WorkflowType,
};
use futures::future::BoxFuture;
use std::time::Duration;

pub trait DurableBackend: Clone + Send + Sync + 'static {
    fn start_workflow(
        &self,
        req: StartWorkflowRequest,
    ) -> BoxFuture<'static, Result<StartWorkflowOutcome>>;

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

    fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedActivityTask>>>;

    fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> BoxFuture<'static, Result<CompleteActivityOutcome>>;
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
    pub schedule_activities: Vec<ActivityTask>,
    pub delete_waits: Vec<WaitId>,
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

pub fn conflict_to_error(outcome: CommitOutcome) -> Result<EventId> {
    match outcome {
        CommitOutcome::Committed { new_tail_event_id } => Ok(new_tail_event_id),
        CommitOutcome::Conflict => Err(Error::Conflict),
    }
}
