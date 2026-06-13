use crate::{ActivityName, PayloadRef, RunId, WorkflowType};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableFailure {
    pub error_type: String,
    pub message: String,
    pub non_retryable: bool,
    pub details: Option<PayloadRef>,
}

impl DurableFailure {
    pub fn new(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
            non_retryable: false,
            details: None,
        }
    }

    pub fn non_retryable(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            non_retryable: true,
            ..Self::new(error_type, message)
        }
    }

    pub fn with_details<T>(mut self, details: &T) -> Result<Self>
    where
        T: Serialize,
    {
        self.details = Some(crate::encode_payload(details)?);
        Ok(self)
    }

    pub fn from_error(error: &Error) -> Self {
        match error {
            Error::Application(failure) | Error::ActivityFailed(failure) => failure.clone(),
            Error::ActivityTimedOut(message) => {
                Self::new("durust.activity_timed_out", message.clone())
            }
            Error::ChildWorkflowFailed(failure) => failure.clone(),
            Error::ChildWorkflowCancelled(reason) => {
                Self::new("durust.child_workflow_cancelled", reason.clone()).marked_non_retryable()
            }
            Error::ContinueAsNew { .. } => Self::new(
                "durust.continue_as_new",
                "workflow requested continue-as-new",
            )
            .marked_non_retryable(),
            Error::Nondeterminism(message) => {
                Self::new("durust.nondeterminism", message.clone()).marked_non_retryable()
            }
            Error::UnsupportedWorkflowVersion {
                change_id,
                version,
                min_supported,
                max_supported,
            } => Self::new(
                "durust.unsupported_workflow_version",
                format!(
                    "change `{change_id}` recorded version {version}, supported range {min_supported}..={max_supported}"
                ),
            )
            .marked_non_retryable(),
            Error::PayloadEncode(message) => Self::new("durust.payload_encode", message.clone()),
            Error::PayloadDecode(message) => Self::new("durust.payload_decode", message.clone()),
            Error::Backend(message) => Self::new("durust.backend", message.clone()),
            Error::ActivityNotRegistered(name) => {
                Self::new("durust.activity_not_registered", name.to_string()).marked_non_retryable()
            }
            Error::WorkflowNotRegistered(workflow_type) => {
                Self::new("durust.workflow_not_registered", workflow_type.to_string())
                    .marked_non_retryable()
            }
            Error::DuplicateActivity(name) => {
                Self::new("durust.duplicate_activity", name.to_string()).marked_non_retryable()
            }
            Error::DuplicateWorkflow(workflow_type) => {
                Self::new("durust.duplicate_workflow", workflow_type.to_string())
                    .marked_non_retryable()
            }
            Error::Conflict => Self::new("durust.conflict", "backend conflict"),
            Error::RunNotFound(run_id) => {
                Self::new("durust.run_not_found", run_id.to_string()).marked_non_retryable()
            }
            Error::StaleLease => Self::new("durust.stale_lease", "stale lease token"),
            Error::TerminalWorkflow => {
                Self::new("durust.terminal_workflow", "workflow is terminal").marked_non_retryable()
            }
        }
    }

    fn marked_non_retryable(mut self) -> Self {
        self.non_retryable = true;
        self
    }
}

impl fmt::Display for DurableFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.error_type.is_empty() {
            f.write_str(&self.message)
        } else {
            write!(f, "{}: {}", self.error_type, self.message)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
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

    #[error("application error: {0}")]
    Application(DurableFailure),

    #[error("activity failed: {0}")]
    ActivityFailed(DurableFailure),

    #[error("activity timed out: {0}")]
    ActivityTimedOut(String),

    #[error("child workflow failed: {0}")]
    ChildWorkflowFailed(DurableFailure),

    #[error("child workflow cancelled: {0}")]
    ChildWorkflowCancelled(String),

    #[error("workflow continued as new")]
    ContinueAsNew { input: PayloadRef },

    #[error("nondeterministic replay: {0}")]
    Nondeterminism(String),

    #[error(
        "unsupported workflow version for `{change_id}`: recorded {version}, supported {min_supported}..={max_supported}"
    )]
    UnsupportedWorkflowVersion {
        change_id: String,
        version: i32,
        min_supported: i32,
        max_supported: i32,
    },

    #[error("payload encode failed: {0}")]
    PayloadEncode(String),

    #[error("payload decode failed: {0}")]
    PayloadDecode(String),

    #[error("backend error: {0}")]
    Backend(String),
}

impl Error {
    pub fn application(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Application(DurableFailure::new(error_type, message))
    }

    pub fn non_retryable(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Application(DurableFailure::non_retryable(error_type, message))
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::non_retryable("durust.timeout", message)
    }

    pub fn with_details<T>(self, details: &T) -> Result<Self>
    where
        T: Serialize,
    {
        match self {
            Self::Application(failure) => Ok(Self::Application(failure.with_details(details)?)),
            other => Ok(Self::Application(
                DurableFailure::from_error(&other).with_details(details)?,
            )),
        }
    }

    pub fn is_non_retryable(&self) -> bool {
        match self {
            Self::Application(failure)
            | Self::ActivityFailed(failure)
            | Self::ChildWorkflowFailed(failure) => failure.non_retryable,
            _ => false,
        }
    }

    pub fn durable_failure(&self) -> DurableFailure {
        DurableFailure::from_error(self)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
