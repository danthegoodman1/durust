use crate::{ActivityName, RunId, WorkflowType};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("activity `{0}` is not registered on this worker")]
    ActivityNotRegistered(ActivityName),

    #[error("workflow `{0}` is not registered on this worker")]
    WorkflowNotRegistered(WorkflowType),

    #[error("duplicate activity registration for `{0}`")]
    DuplicateActivity(ActivityName),

    #[error("duplicate workflow registration for `{0}`")]
    DuplicateWorkflow(WorkflowType),

    #[error("backend conflict")]
    Conflict,

    #[error("workflow run `{0}` was not found")]
    RunNotFound(RunId),

    #[error("stale lease token")]
    StaleLease,

    #[error("workflow is terminal")]
    TerminalWorkflow,

    #[error("nondeterministic replay: {0}")]
    Nondeterminism(String),

    #[error("payload encode failed: {0}")]
    PayloadEncode(String),

    #[error("payload decode failed: {0}")]
    PayloadDecode(String),

    #[error("backend error: {0}")]
    Backend(String),
}

pub type Result<T> = std::result::Result<T, Error>;
