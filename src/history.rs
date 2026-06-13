use crate::{
    ActivityId, ActivityName, CommandId, CommandSeq, EventId, PayloadRef, RunId, TaskQueue,
    WorkflowType,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandFingerprint {
    pub kind: CommandKind,
    pub name: String,
    pub input_digest: Option<String>,
    pub options_digest: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandKind {
    Activity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityScheduled {
    pub command_id: CommandId,
    pub activity_name: ActivityName,
    pub task_queue: TaskQueue,
    pub input: PayloadRef,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityCompleted {
    pub command_id: CommandId,
    pub result: PayloadRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistoryEventData {
    WorkflowStarted {
        workflow_type: WorkflowType,
        input: PayloadRef,
    },
    WorkflowCompleted {
        result: PayloadRef,
    },
    WorkflowFailed {
        message: String,
    },
    WorkflowTaskStarted,
    ActivityScheduled(ActivityScheduled),
    ActivityCompleted(ActivityCompleted),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEvent {
    pub event_id: EventId,
    pub event_type: HistoryEventType,
    pub data: HistoryEventData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistoryEventType {
    WorkflowStarted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowTaskStarted,
    ActivityScheduled,
    ActivityCompleted,
}

impl HistoryEventData {
    pub fn event_type(&self) -> HistoryEventType {
        match self {
            Self::WorkflowStarted { .. } => HistoryEventType::WorkflowStarted,
            Self::WorkflowCompleted { .. } => HistoryEventType::WorkflowCompleted,
            Self::WorkflowFailed { .. } => HistoryEventType::WorkflowFailed,
            Self::WorkflowTaskStarted => HistoryEventType::WorkflowTaskStarted,
            Self::ActivityScheduled(_) => HistoryEventType::ActivityScheduled,
            Self::ActivityCompleted(_) => HistoryEventType::ActivityCompleted,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewHistoryEvent {
    pub data: HistoryEventData,
}

impl NewHistoryEvent {
    pub fn new(data: HistoryEventData) -> Self {
        Self { data }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTask {
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub command_id: CommandId,
    pub activity_name: ActivityName,
    pub task_queue: TaskQueue,
    pub input: PayloadRef,
}

impl ActivityTask {
    pub fn from_scheduled(scheduled: &ActivityScheduled) -> Self {
        Self {
            activity_id: ActivityId::new(&scheduled.command_id),
            run_id: scheduled.command_id.run_id.clone(),
            command_id: scheduled.command_id.clone(),
            activity_name: scheduled.activity_name.clone(),
            task_queue: scheduled.task_queue.clone(),
            input: scheduled.input.clone(),
        }
    }
}

pub fn activity_fingerprint(
    activity_name: ActivityName,
    input_digest: String,
) -> CommandFingerprint {
    CommandFingerprint {
        kind: CommandKind::Activity,
        name: activity_name.0,
        input_digest: Some(input_digest),
        options_digest: "sha256:default".to_owned(),
    }
}

pub fn command_id(run_id: &RunId, seq: u64) -> CommandId {
    CommandId {
        run_id: run_id.clone(),
        seq: CommandSeq(seq),
    }
}
