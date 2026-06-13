use crate::{Error, Result, TaskQueue};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryBackoff {
    None,
    Exponential,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub backoff: RetryBackoff,
    pub max_attempts: u32,
}

impl RetryPolicy {
    pub fn none() -> Self {
        Self {
            backoff: RetryBackoff::None,
            max_attempts: 1,
        }
    }

    pub fn exponential() -> Self {
        Self {
            backoff: RetryBackoff::Exponential,
            max_attempts: 3,
        }
    }

    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::none()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityOptions {
    pub task_queue: Option<TaskQueue>,
    pub retry_policy: Option<RetryPolicy>,
    pub start_to_close_timeout: Option<std::time::Duration>,
    pub heartbeat_timeout: Option<std::time::Duration>,
}

impl ActivityOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.task_queue = Some(TaskQueue::new(task_queue));
        self
    }

    pub fn retry(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.start_to_close_timeout = Some(timeout);
        self
    }

    pub fn heartbeat_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.heartbeat_timeout = Some(timeout);
        self
    }

    pub(crate) fn merge_overrides(mut self, overrides: Self) -> Self {
        if overrides.task_queue.is_some() {
            self.task_queue = overrides.task_queue;
        }
        if overrides.retry_policy.is_some() {
            self.retry_policy = overrides.retry_policy;
        }
        if overrides.start_to_close_timeout.is_some() {
            self.start_to_close_timeout = overrides.start_to_close_timeout;
        }
        if overrides.heartbeat_timeout.is_some() {
            self.heartbeat_timeout = overrides.heartbeat_timeout;
        }
        self
    }

    pub(crate) fn with_task_queue_fallback(mut self, task_queue: TaskQueue) -> Self {
        if self.task_queue.is_none() {
            self.task_queue = Some(task_queue);
        }
        self
    }

    pub(crate) fn effective_retry_policy(&self) -> RetryPolicy {
        self.retry_policy.clone().unwrap_or_default()
    }

    pub(crate) fn digest(&self) -> Result<String> {
        let bytes =
            rmp_serde::to_vec_named(self).map_err(|err| Error::PayloadEncode(err.to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
    }
}
