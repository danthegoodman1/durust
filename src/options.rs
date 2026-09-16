use crate::{Error, Result, TaskQueue};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

fn duration_millis_u64(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Retry pacing for an activity or map item, stored in the shape both
/// runtimes write to history: `initialIntervalMs`, `maxIntervalMs`,
/// `maxAttempts`, `backoffCoefficient`, `nonRetryableErrorTypes`. The delay
/// before the attempt after attempt `n` fails is
/// `min(max_interval, round(initial_interval * coefficient^(n - 1)))`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryPolicy {
    pub initial_interval_ms: u64,
    pub max_interval_ms: u64,
    pub max_attempts: u32,
    pub backoff_coefficient: f64,
    #[serde(default)]
    pub non_retryable_error_types: Vec<String>,
}

// The builders keep `backoff_coefficient` finite, so equality is an
// equivalence relation over every value this type takes.
impl Eq for RetryPolicy {}

impl RetryPolicy {
    /// One attempt, no backoff.
    pub fn none() -> Self {
        Self {
            initial_interval_ms: 0,
            max_interval_ms: 0,
            max_attempts: 1,
            backoff_coefficient: 1.0,
            non_retryable_error_types: Vec::new(),
        }
    }

    /// Three attempts, one second doubling up to a minute.
    pub fn exponential() -> Self {
        Self {
            initial_interval_ms: 1_000,
            max_interval_ms: 60_000,
            max_attempts: 3,
            backoff_coefficient: 2.0,
            non_retryable_error_types: Vec::new(),
        }
    }

    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    pub fn initial_interval(mut self, interval: std::time::Duration) -> Self {
        self.initial_interval_ms = duration_millis_u64(interval);
        self
    }

    pub fn max_interval(mut self, interval: std::time::Duration) -> Self {
        self.max_interval_ms = duration_millis_u64(interval);
        self
    }

    pub fn backoff_coefficient(mut self, coefficient: f64) -> Self {
        self.backoff_coefficient = if coefficient.is_finite() {
            coefficient.max(1.0)
        } else {
            1.0
        };
        self
    }

    pub fn non_retryable_error_types<I, S>(mut self, error_types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.non_retryable_error_types = error_types.into_iter().map(Into::into).collect();
        self
    }

    /// True unless `error_type` is listed as non-retryable.
    pub fn allows_retry_of(&self, error_type: &str) -> bool {
        !self
            .non_retryable_error_types
            .iter()
            .any(|listed| listed == error_type)
    }

    /// Delay in milliseconds before the attempt that follows `failed_attempt`
    /// (1-based); zero means immediately claimable.
    pub fn retry_delay_ms(&self, failed_attempt: u32) -> u64 {
        let initial = self.initial_interval_ms as f64;
        let max = (self.max_interval_ms as f64).max(initial);
        let coefficient = self.backoff_coefficient.max(1.0);
        let exponent = i32::try_from(failed_attempt.saturating_sub(1)).unwrap_or(i32::MAX);
        let delay = (initial * coefficient.powi(exponent)).round().min(max);
        if delay.is_finite() && delay > 0.0 {
            delay as u64
        } else {
            0
        }
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
