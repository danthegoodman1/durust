//! The TypeScript provider contract, as serde types. Every struct here is the
//! camelCase, `kind`-tagged shape `typescript/packages/core` uses, so a
//! msgpack encoding of one decodes on the Node side into the same object a
//! TypeScript provider would return, and a request the TypeScript worker
//! builds decodes here. Every conversion to and from the Rust types is
//! total.

use serde::{Deserialize, Serialize};
use std::time::Duration;

fn ms_to_duration(ms: Option<i64>) -> Option<Duration> {
    ms.map(|ms| Duration::from_millis(ms.max(0) as u64))
}

fn duration_to_ms(duration: Option<Duration>) -> Option<i64> {
    duration.map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
}

// ---- identifiers -----------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandId {
    pub run_id: String,
    pub seq: u64,
}

impl From<&durust::CommandId> for CommandId {
    fn from(id: &durust::CommandId) -> Self {
        Self {
            run_id: id.run_id.0.clone(),
            seq: id.seq.0,
        }
    }
}

impl From<CommandId> for durust::CommandId {
    fn from(id: CommandId) -> Self {
        durust::command_id(&durust::RunId::new(id.run_id), id.seq)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowType {
    pub name: String,
    pub version: u32,
}

impl From<&durust::WorkflowType> for WorkflowType {
    fn from(value: &durust::WorkflowType) -> Self {
        Self {
            name: value.name.clone(),
            version: value.version,
        }
    }
}

impl From<WorkflowType> for durust::WorkflowType {
    fn from(value: WorkflowType) -> Self {
        durust::WorkflowType::new(value.name, value.version)
    }
}

// ---- payloads --------------------------------------------------------------

// These three already serialize in the TypeScript shape (`kind`-tagged,
// camelCase, inline bytes as a byte string), so the binding passes them
// through as they are.
pub type PayloadRef = durust::PayloadRef;
pub type DurableFailure = durust::DurableFailure;
pub type RetryPolicy = durust::RetryPolicy;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandFingerprint {
    pub kind: durust::CommandKind,
    pub name: String,
    pub input_digest: Option<String>,
    pub options_digest: String,
}

impl From<&durust::CommandFingerprint> for CommandFingerprint {
    fn from(value: &durust::CommandFingerprint) -> Self {
        Self {
            kind: value.kind,
            name: value.name.clone(),
            input_digest: value.input_digest.clone(),
            options_digest: value.options_digest.clone(),
        }
    }
}

impl From<CommandFingerprint> for durust::CommandFingerprint {
    fn from(value: CommandFingerprint) -> Self {
        durust::CommandFingerprint {
            kind: value.kind,
            name: value.name,
            input_digest: value.input_digest,
            options_digest: value.options_digest,
        }
    }
}

// ---- history events --------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityScheduled {
    pub command_id: CommandId,
    pub activity_name: String,
    pub task_queue: String,
    pub retry_policy: RetryPolicy,
    pub start_to_close_timeout_ms: Option<i64>,
    pub heartbeat_timeout_ms: Option<i64>,
    pub input: PayloadRef,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityMapScheduled {
    pub command_id: CommandId,
    pub activity_name: String,
    pub task_queue: String,
    pub retry_policy: RetryPolicy,
    pub start_to_close_timeout_ms: Option<i64>,
    pub heartbeat_timeout_ms: Option<i64>,
    pub input_manifest: PayloadRef,
    pub result_manifest_name: String,
    pub max_in_flight: u64,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityMapCompleted {
    pub command_id: CommandId,
    pub result_manifest: PayloadRef,
    pub item_count: u64,
    pub success_count: u64,
    pub failure_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandFailed {
    pub command_id: CommandId,
    pub failure: DurableFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResult {
    pub command_id: CommandId,
    pub result: PayloadRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityTimedOut {
    pub command_id: CommandId,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildWorkflowStartRequested {
    pub command_id: CommandId,
    pub workflow_type: WorkflowType,
    pub workflow_id: String,
    pub task_queue: String,
    pub input: PayloadRef,
    pub parent_close_policy: durust::ParentClosePolicy,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildWorkflowStarted {
    pub command_id: CommandId,
    pub workflow_id: String,
    pub run_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildWorkflowCancelled {
    pub command_id: CommandId,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildWorkflowMapScheduled {
    pub command_id: CommandId,
    pub workflow_type: WorkflowType,
    pub task_queue: String,
    pub input_manifest: PayloadRef,
    pub result_manifest_name: String,
    pub workflow_id_prefix: String,
    pub max_in_flight: u64,
    pub parent_close_policy: durust::ParentClosePolicy,
    pub failure_mode: durust::ChildWorkflowMapFailureMode,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildWorkflowMapCompleted {
    pub command_id: CommandId,
    pub result_manifest: PayloadRef,
    pub item_count: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub cancellation_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimerStarted {
    pub command_id: CommandId,
    pub fire_at: i64,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimerFired {
    pub command_id: CommandId,
    pub fired_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalConsumed {
    pub command_id: CommandId,
    pub signal_id: String,
    pub signal_name: String,
    pub payload: PayloadRef,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectWinner {
    pub select_command_id: CommandId,
    pub branch_ordinal: u32,
    pub winning_event_id: u64,
    pub branches_digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionMarker {
    pub command_id: CommandId,
    pub change_id: String,
    pub version: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeprecatedPatchMarker {
    pub command_id: CommandId,
    pub patch_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SideEffectMarker {
    pub command_id: CommandId,
    pub key: String,
    pub value: PayloadRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum HistoryEventData {
    WorkflowStarted {
        workflow_type: WorkflowType,
        input: PayloadRef,
    },
    WorkflowCompleted {
        result: PayloadRef,
    },
    WorkflowFailed {
        failure: DurableFailure,
    },
    WorkflowCancelled {
        reason: String,
    },
    WorkflowContinuedAsNew {
        input: PayloadRef,
    },
    WorkflowTaskStarted,
    ActivityScheduled {
        scheduled: ActivityScheduled,
    },
    ActivityMapScheduled {
        scheduled: ActivityMapScheduled,
    },
    ActivityMapCompleted {
        completed: ActivityMapCompleted,
    },
    ActivityMapFailed {
        failed: CommandFailed,
    },
    ActivityCompleted {
        completed: CommandResult,
    },
    ActivityFailed {
        failed: CommandFailed,
    },
    ActivityTimedOut {
        timed_out: ActivityTimedOut,
    },
    ChildWorkflowStartRequested {
        requested: ChildWorkflowStartRequested,
    },
    ChildWorkflowStarted {
        started: ChildWorkflowStarted,
    },
    ChildWorkflowCompleted {
        completed: CommandResult,
    },
    ChildWorkflowFailed {
        failed: CommandFailed,
    },
    ChildWorkflowCancelled {
        cancelled: ChildWorkflowCancelled,
    },
    ChildWorkflowMapScheduled {
        scheduled: ChildWorkflowMapScheduled,
    },
    ChildWorkflowMapCompleted {
        completed: ChildWorkflowMapCompleted,
    },
    ChildWorkflowMapFailed {
        failed: CommandFailed,
    },
    TimerStarted {
        started: TimerStarted,
    },
    TimerFired {
        fired: TimerFired,
    },
    SignalConsumed {
        consumed: SignalConsumed,
    },
    SelectWinner {
        winner: SelectWinner,
    },
    VersionMarker {
        marker: VersionMarker,
    },
    DeprecatedPatchMarker {
        marker: DeprecatedPatchMarker,
    },
    SideEffectMarker {
        marker: SideEffectMarker,
    },
}

impl HistoryEventData {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::WorkflowStarted { .. } => "WorkflowStarted",
            Self::WorkflowCompleted { .. } => "WorkflowCompleted",
            Self::WorkflowFailed { .. } => "WorkflowFailed",
            Self::WorkflowCancelled { .. } => "WorkflowCancelled",
            Self::WorkflowContinuedAsNew { .. } => "WorkflowContinuedAsNew",
            Self::WorkflowTaskStarted => "WorkflowTaskStarted",
            Self::ActivityScheduled { .. } => "ActivityScheduled",
            Self::ActivityMapScheduled { .. } => "ActivityMapScheduled",
            Self::ActivityMapCompleted { .. } => "ActivityMapCompleted",
            Self::ActivityMapFailed { .. } => "ActivityMapFailed",
            Self::ActivityCompleted { .. } => "ActivityCompleted",
            Self::ActivityFailed { .. } => "ActivityFailed",
            Self::ActivityTimedOut { .. } => "ActivityTimedOut",
            Self::ChildWorkflowStartRequested { .. } => "ChildWorkflowStartRequested",
            Self::ChildWorkflowStarted { .. } => "ChildWorkflowStarted",
            Self::ChildWorkflowCompleted { .. } => "ChildWorkflowCompleted",
            Self::ChildWorkflowFailed { .. } => "ChildWorkflowFailed",
            Self::ChildWorkflowCancelled { .. } => "ChildWorkflowCancelled",
            Self::ChildWorkflowMapScheduled { .. } => "ChildWorkflowMapScheduled",
            Self::ChildWorkflowMapCompleted { .. } => "ChildWorkflowMapCompleted",
            Self::ChildWorkflowMapFailed { .. } => "ChildWorkflowMapFailed",
            Self::TimerStarted { .. } => "TimerStarted",
            Self::TimerFired { .. } => "TimerFired",
            Self::SignalConsumed { .. } => "SignalConsumed",
            Self::SelectWinner { .. } => "SelectWinner",
            Self::VersionMarker { .. } => "VersionMarker",
            Self::DeprecatedPatchMarker { .. } => "DeprecatedPatchMarker",
            Self::SideEffectMarker { .. } => "SideEffectMarker",
        }
    }
}

fn failed(command_id: &durust::CommandId, failure: durust::DurableFailure) -> CommandFailed {
    CommandFailed {
        command_id: command_id.into(),
        failure,
    }
}

fn result(command_id: &durust::CommandId, result: durust::PayloadRef) -> CommandResult {
    CommandResult {
        command_id: command_id.into(),
        result,
    }
}

impl From<durust::HistoryEventData> for HistoryEventData {
    fn from(data: durust::HistoryEventData) -> Self {
        use durust::HistoryEventData as R;
        match data {
            R::WorkflowStarted {
                workflow_type,
                input,
            } => Self::WorkflowStarted {
                workflow_type: (&workflow_type).into(),
                input,
            },
            R::WorkflowCompleted { result } => Self::WorkflowCompleted { result },
            R::WorkflowFailed { failure } => Self::WorkflowFailed { failure },
            R::WorkflowCancelled { reason } => Self::WorkflowCancelled { reason },
            R::WorkflowContinuedAsNew { input } => Self::WorkflowContinuedAsNew { input },
            R::WorkflowTaskStarted => Self::WorkflowTaskStarted,
            R::ActivityScheduled(s) => Self::ActivityScheduled {
                scheduled: ActivityScheduled {
                    command_id: (&s.command_id).into(),
                    activity_name: s.activity_name.0,
                    task_queue: s.task_queue.0,
                    retry_policy: s.retry_policy,
                    start_to_close_timeout_ms: duration_to_ms(s.start_to_close_timeout),
                    heartbeat_timeout_ms: duration_to_ms(s.heartbeat_timeout),
                    input: s.input,
                    fingerprint: (&s.fingerprint).into(),
                },
            },
            R::ActivityMapScheduled(s) => Self::ActivityMapScheduled {
                scheduled: ActivityMapScheduled {
                    command_id: (&s.command_id).into(),
                    activity_name: s.activity_name.0,
                    task_queue: s.task_queue.0,
                    retry_policy: s.retry_policy,
                    start_to_close_timeout_ms: duration_to_ms(s.start_to_close_timeout),
                    heartbeat_timeout_ms: duration_to_ms(s.heartbeat_timeout),
                    input_manifest: s.input_manifest,
                    result_manifest_name: s.result_manifest_name,
                    max_in_flight: s.max_in_flight as u64,
                    fingerprint: (&s.fingerprint).into(),
                },
            },
            R::ActivityMapCompleted(c) => Self::ActivityMapCompleted {
                completed: ActivityMapCompleted {
                    command_id: (&c.command_id).into(),
                    result_manifest: c.result_manifest,
                    item_count: c.item_count as u64,
                    success_count: c.success_count as u64,
                    failure_count: c.failure_count as u64,
                },
            },
            R::ActivityMapFailed(f) => Self::ActivityMapFailed {
                failed: failed(&f.command_id, f.failure),
            },
            R::ActivityCompleted(c) => Self::ActivityCompleted {
                completed: result(&c.command_id, c.result),
            },
            R::ActivityFailed(f) => Self::ActivityFailed {
                failed: failed(&f.command_id, f.failure),
            },
            R::ActivityTimedOut(t) => Self::ActivityTimedOut {
                timed_out: ActivityTimedOut {
                    command_id: (&t.command_id).into(),
                    message: t.message,
                },
            },
            R::ChildWorkflowStartRequested(r) => Self::ChildWorkflowStartRequested {
                requested: ChildWorkflowStartRequested {
                    command_id: (&r.command_id).into(),
                    workflow_type: (&r.workflow_type).into(),
                    workflow_id: r.workflow_id.0,
                    task_queue: r.task_queue.0,
                    input: r.input,
                    parent_close_policy: r.parent_close_policy,
                    fingerprint: (&r.fingerprint).into(),
                },
            },
            R::ChildWorkflowStarted(s) => Self::ChildWorkflowStarted {
                started: ChildWorkflowStarted {
                    command_id: (&s.command_id).into(),
                    workflow_id: s.workflow_id.0,
                    run_id: s.run_id.0,
                },
            },
            R::ChildWorkflowCompleted(c) => Self::ChildWorkflowCompleted {
                completed: result(&c.command_id, c.result),
            },
            R::ChildWorkflowFailed(f) => Self::ChildWorkflowFailed {
                failed: failed(&f.command_id, f.failure),
            },
            R::ChildWorkflowCancelled(c) => Self::ChildWorkflowCancelled {
                cancelled: ChildWorkflowCancelled {
                    command_id: (&c.command_id).into(),
                    reason: c.reason,
                },
            },
            R::ChildWorkflowMapScheduled(s) => Self::ChildWorkflowMapScheduled {
                scheduled: ChildWorkflowMapScheduled {
                    command_id: (&s.command_id).into(),
                    workflow_type: (&s.workflow_type).into(),
                    task_queue: s.task_queue.0,
                    input_manifest: s.input_manifest,
                    result_manifest_name: s.result_manifest_name,
                    workflow_id_prefix: s.workflow_id_prefix,
                    max_in_flight: s.max_in_flight as u64,
                    parent_close_policy: s.parent_close_policy,
                    failure_mode: s.failure_mode,
                    fingerprint: (&s.fingerprint).into(),
                },
            },
            R::ChildWorkflowMapCompleted(c) => Self::ChildWorkflowMapCompleted {
                completed: ChildWorkflowMapCompleted {
                    command_id: (&c.command_id).into(),
                    result_manifest: c.result_manifest,
                    item_count: c.item_count as u64,
                    success_count: c.success_count as u64,
                    failure_count: c.failure_count as u64,
                    cancellation_count: c.cancellation_count as u64,
                },
            },
            R::ChildWorkflowMapFailed(f) => Self::ChildWorkflowMapFailed {
                failed: failed(&f.command_id, f.failure),
            },
            R::TimerStarted(t) => Self::TimerStarted {
                started: TimerStarted {
                    command_id: (&t.command_id).into(),
                    fire_at: t.fire_at.0,
                    fingerprint: (&t.fingerprint).into(),
                },
            },
            R::TimerFired(t) => Self::TimerFired {
                fired: TimerFired {
                    command_id: (&t.command_id).into(),
                    fired_at: t.fired_at.0,
                },
            },
            R::SignalConsumed(s) => Self::SignalConsumed {
                consumed: SignalConsumed {
                    command_id: (&s.command_id).into(),
                    signal_id: s.signal_id.0,
                    signal_name: s.signal_name.0,
                    payload: s.payload,
                    fingerprint: (&s.fingerprint).into(),
                },
            },
            R::SelectWinner(w) => Self::SelectWinner {
                winner: SelectWinner {
                    select_command_id: (&w.select_command_id).into(),
                    branch_ordinal: w.branch_ordinal,
                    winning_event_id: w.winning_event_id.0,
                    branches_digest: w.branches_digest,
                },
            },
            R::VersionMarker(m) => Self::VersionMarker {
                marker: VersionMarker {
                    command_id: (&m.command_id).into(),
                    change_id: m.change_id,
                    version: m.version,
                },
            },
            R::DeprecatedPatchMarker(m) => Self::DeprecatedPatchMarker {
                marker: DeprecatedPatchMarker {
                    command_id: (&m.command_id).into(),
                    patch_id: m.patch_id,
                },
            },
            R::SideEffectMarker(m) => Self::SideEffectMarker {
                marker: SideEffectMarker {
                    command_id: (&m.command_id).into(),
                    key: m.key,
                    value: m.value,
                },
            },
        }
    }
}

impl From<HistoryEventData> for durust::HistoryEventData {
    fn from(data: HistoryEventData) -> Self {
        use durust::HistoryEventData as R;
        match data {
            HistoryEventData::WorkflowStarted {
                workflow_type,
                input,
            } => R::WorkflowStarted {
                workflow_type: workflow_type.into(),
                input,
            },
            HistoryEventData::WorkflowCompleted { result } => R::WorkflowCompleted { result },
            HistoryEventData::WorkflowFailed { failure } => R::WorkflowFailed { failure },
            HistoryEventData::WorkflowCancelled { reason } => R::WorkflowCancelled { reason },
            HistoryEventData::WorkflowContinuedAsNew { input } => {
                R::WorkflowContinuedAsNew { input }
            }
            HistoryEventData::WorkflowTaskStarted => R::WorkflowTaskStarted,
            HistoryEventData::ActivityScheduled { scheduled: s } => {
                R::ActivityScheduled(durust::ActivityScheduled {
                    command_id: s.command_id.into(),
                    activity_name: durust::ActivityName::new(s.activity_name),
                    task_queue: durust::TaskQueue::new(s.task_queue),
                    retry_policy: s.retry_policy,
                    start_to_close_timeout: ms_to_duration(s.start_to_close_timeout_ms),
                    heartbeat_timeout: ms_to_duration(s.heartbeat_timeout_ms),
                    input: s.input,
                    fingerprint: s.fingerprint.into(),
                })
            }
            HistoryEventData::ActivityMapScheduled { scheduled: s } => {
                R::ActivityMapScheduled(durust::ActivityMapScheduled {
                    command_id: s.command_id.into(),
                    activity_name: durust::ActivityName::new(s.activity_name),
                    task_queue: durust::TaskQueue::new(s.task_queue),
                    retry_policy: s.retry_policy,
                    start_to_close_timeout: ms_to_duration(s.start_to_close_timeout_ms),
                    heartbeat_timeout: ms_to_duration(s.heartbeat_timeout_ms),
                    input_manifest: s.input_manifest,
                    result_manifest_name: s.result_manifest_name,
                    max_in_flight: s.max_in_flight as usize,
                    fingerprint: s.fingerprint.into(),
                })
            }
            HistoryEventData::ActivityMapCompleted { completed: c } => {
                R::ActivityMapCompleted(durust::ActivityMapCompleted {
                    command_id: c.command_id.into(),
                    result_manifest: c.result_manifest,
                    item_count: c.item_count as usize,
                    success_count: c.success_count as usize,
                    failure_count: c.failure_count as usize,
                })
            }
            HistoryEventData::ActivityMapFailed { failed: f } => {
                R::ActivityMapFailed(durust::ActivityMapFailed {
                    command_id: f.command_id.into(),
                    failure: f.failure,
                })
            }
            HistoryEventData::ActivityCompleted { completed: c } => {
                R::ActivityCompleted(durust::ActivityCompleted {
                    command_id: c.command_id.into(),
                    result: c.result,
                })
            }
            HistoryEventData::ActivityFailed { failed: f } => {
                R::ActivityFailed(durust::ActivityFailed {
                    command_id: f.command_id.into(),
                    failure: f.failure,
                })
            }
            HistoryEventData::ActivityTimedOut { timed_out: t } => {
                R::ActivityTimedOut(durust::ActivityTimedOut {
                    command_id: t.command_id.into(),
                    message: t.message,
                })
            }
            HistoryEventData::ChildWorkflowStartRequested { requested: r } => {
                R::ChildWorkflowStartRequested(r.into())
            }
            HistoryEventData::ChildWorkflowStarted { started: s } => {
                R::ChildWorkflowStarted(durust::ChildWorkflowStarted {
                    command_id: s.command_id.into(),
                    workflow_id: durust::WorkflowId::new(s.workflow_id),
                    run_id: durust::RunId::new(s.run_id),
                })
            }
            HistoryEventData::ChildWorkflowCompleted { completed: c } => {
                R::ChildWorkflowCompleted(durust::ChildWorkflowCompleted {
                    command_id: c.command_id.into(),
                    result: c.result,
                })
            }
            HistoryEventData::ChildWorkflowFailed { failed: f } => {
                R::ChildWorkflowFailed(durust::ChildWorkflowFailed {
                    command_id: f.command_id.into(),
                    failure: f.failure,
                })
            }
            HistoryEventData::ChildWorkflowCancelled { cancelled: c } => {
                R::ChildWorkflowCancelled(durust::ChildWorkflowCancelled {
                    command_id: c.command_id.into(),
                    reason: c.reason,
                })
            }
            HistoryEventData::ChildWorkflowMapScheduled { scheduled: s } => {
                R::ChildWorkflowMapScheduled(durust::ChildWorkflowMapScheduled {
                    command_id: s.command_id.into(),
                    workflow_type: s.workflow_type.into(),
                    task_queue: durust::TaskQueue::new(s.task_queue),
                    input_manifest: s.input_manifest,
                    result_manifest_name: s.result_manifest_name,
                    workflow_id_prefix: s.workflow_id_prefix,
                    max_in_flight: s.max_in_flight as usize,
                    parent_close_policy: s.parent_close_policy,
                    failure_mode: s.failure_mode,
                    fingerprint: s.fingerprint.into(),
                })
            }
            HistoryEventData::ChildWorkflowMapCompleted { completed: c } => {
                R::ChildWorkflowMapCompleted(durust::ChildWorkflowMapCompleted {
                    command_id: c.command_id.into(),
                    result_manifest: c.result_manifest,
                    item_count: c.item_count as usize,
                    success_count: c.success_count as usize,
                    failure_count: c.failure_count as usize,
                    cancellation_count: c.cancellation_count as usize,
                })
            }
            HistoryEventData::ChildWorkflowMapFailed { failed: f } => {
                R::ChildWorkflowMapFailed(durust::ChildWorkflowMapFailed {
                    command_id: f.command_id.into(),
                    failure: f.failure,
                })
            }
            HistoryEventData::TimerStarted { started: t } => {
                R::TimerStarted(durust::TimerStarted {
                    command_id: t.command_id.into(),
                    fire_at: durust::TimestampMs(t.fire_at),
                    fingerprint: t.fingerprint.into(),
                })
            }
            HistoryEventData::TimerFired { fired: t } => R::TimerFired(durust::TimerFired {
                command_id: t.command_id.into(),
                fired_at: durust::TimestampMs(t.fired_at),
            }),
            HistoryEventData::SignalConsumed { consumed: s } => {
                R::SignalConsumed(durust::SignalConsumed {
                    command_id: s.command_id.into(),
                    signal_id: durust::SignalId::new(s.signal_id),
                    signal_name: durust::SignalName::new(s.signal_name),
                    payload: s.payload,
                    fingerprint: s.fingerprint.into(),
                })
            }
            HistoryEventData::SelectWinner { winner: w } => R::SelectWinner(durust::SelectWinner {
                select_command_id: w.select_command_id.into(),
                branch_ordinal: w.branch_ordinal,
                winning_event_id: durust::EventId(w.winning_event_id),
                branches_digest: w.branches_digest,
            }),
            HistoryEventData::VersionMarker { marker: m } => {
                R::VersionMarker(durust::VersionMarker {
                    command_id: m.command_id.into(),
                    change_id: m.change_id,
                    version: m.version,
                })
            }
            HistoryEventData::DeprecatedPatchMarker { marker: m } => {
                R::DeprecatedPatchMarker(durust::DeprecatedPatchMarker {
                    command_id: m.command_id.into(),
                    patch_id: m.patch_id,
                })
            }
            HistoryEventData::SideEffectMarker { marker: m } => {
                R::SideEffectMarker(durust::SideEffectMarker {
                    command_id: m.command_id.into(),
                    key: m.key,
                    value: m.value,
                })
            }
        }
    }
}

impl From<ChildWorkflowStartRequested> for durust::ChildWorkflowStartRequested {
    fn from(r: ChildWorkflowStartRequested) -> Self {
        durust::ChildWorkflowStartRequested {
            command_id: r.command_id.into(),
            workflow_type: r.workflow_type.into(),
            workflow_id: durust::WorkflowId::new(r.workflow_id),
            task_queue: durust::TaskQueue::new(r.task_queue),
            input: r.input,
            parent_close_policy: r.parent_close_policy,
            fingerprint: r.fingerprint.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEvent {
    pub event_id: u64,
    pub event_type: String,
    pub data: HistoryEventData,
}

impl From<durust::HistoryEvent> for HistoryEvent {
    fn from(event: durust::HistoryEvent) -> Self {
        let data = HistoryEventData::from(event.data);
        Self {
            event_id: event.event_id.0,
            event_type: data.event_type().to_owned(),
            data,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NewHistoryEvent {
    pub data: HistoryEventData,
}

// ---- tasks and waits -------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityMapItem {
    pub map_command_id: CommandId,
    pub item_ordinal: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityTask {
    pub activity_id: String,
    pub run_id: String,
    pub command_id: CommandId,
    pub activity_name: String,
    pub task_queue: String,
    pub retry_policy: RetryPolicy,
    pub start_to_close_timeout_ms: Option<i64>,
    pub heartbeat_timeout_ms: Option<i64>,
    pub attempt: u32,
    pub input: PayloadRef,
    pub map_item: Option<ActivityMapItem>,
}

impl From<durust::ActivityTask> for ActivityTask {
    fn from(task: durust::ActivityTask) -> Self {
        Self {
            activity_id: task.activity_id.0,
            run_id: task.run_id.0,
            command_id: (&task.command_id).into(),
            activity_name: task.activity_name.0,
            task_queue: task.task_queue.0,
            retry_policy: task.retry_policy,
            start_to_close_timeout_ms: duration_to_ms(task.start_to_close_timeout),
            heartbeat_timeout_ms: duration_to_ms(task.heartbeat_timeout),
            attempt: task.attempt,
            input: task.input,
            map_item: task.map_item.map(|item| ActivityMapItem {
                map_command_id: (&item.map_command_id).into(),
                item_ordinal: item.item_ordinal,
            }),
        }
    }
}

impl From<ActivityTask> for durust::ActivityTask {
    fn from(task: ActivityTask) -> Self {
        durust::ActivityTask {
            activity_id: durust::ActivityId(task.activity_id),
            run_id: durust::RunId::new(task.run_id),
            command_id: task.command_id.into(),
            activity_name: durust::ActivityName::new(task.activity_name),
            task_queue: durust::TaskQueue::new(task.task_queue),
            retry_policy: task.retry_policy,
            start_to_close_timeout: ms_to_duration(task.start_to_close_timeout_ms),
            heartbeat_timeout: ms_to_duration(task.heartbeat_timeout_ms),
            attempt: task.attempt,
            input: task.input,
            map_item: task.map_item.map(|item| durust::ActivityMapItem {
                map_command_id: item.map_command_id.into(),
                item_ordinal: item.item_ordinal,
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityMapTask {
    pub map_command_id: CommandId,
    pub activity_name: String,
    pub task_queue: String,
    pub retry_policy: RetryPolicy,
    pub start_to_close_timeout_ms: Option<i64>,
    pub heartbeat_timeout_ms: Option<i64>,
    pub input_manifest: PayloadRef,
    pub result_manifest_name: String,
    pub max_in_flight: u64,
}

impl From<ActivityMapTask> for durust::ActivityMapTask {
    fn from(task: ActivityMapTask) -> Self {
        durust::ActivityMapTask {
            map_command_id: task.map_command_id.into(),
            activity_name: durust::ActivityName::new(task.activity_name),
            task_queue: durust::TaskQueue::new(task.task_queue),
            retry_policy: task.retry_policy,
            start_to_close_timeout: ms_to_duration(task.start_to_close_timeout_ms),
            heartbeat_timeout: ms_to_duration(task.heartbeat_timeout_ms),
            input_manifest: task.input_manifest,
            result_manifest_name: task.result_manifest_name,
            max_in_flight: task.max_in_flight as usize,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildWorkflowMapTask {
    pub map_command_id: CommandId,
    pub workflow_type: WorkflowType,
    pub task_queue: String,
    pub input_manifest: PayloadRef,
    pub result_manifest_name: String,
    pub workflow_id_prefix: String,
    pub max_in_flight: u64,
    pub parent_close_policy: durust::ParentClosePolicy,
    pub failure_mode: durust::ChildWorkflowMapFailureMode,
}

impl From<ChildWorkflowMapTask> for durust::ChildWorkflowMapTask {
    fn from(task: ChildWorkflowMapTask) -> Self {
        durust::ChildWorkflowMapTask {
            map_command_id: task.map_command_id.into(),
            workflow_type: task.workflow_type.into(),
            task_queue: durust::TaskQueue::new(task.task_queue),
            input_manifest: task.input_manifest,
            result_manifest_name: task.result_manifest_name,
            workflow_id_prefix: task.workflow_id_prefix,
            max_in_flight: task.max_in_flight as usize,
            parent_close_policy: task.parent_close_policy,
            failure_mode: task.failure_mode,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WaitRecord {
    pub wait_id: String,
    pub run_id: String,
    pub command_id: CommandId,
    pub kind: durust::WaitKind,
    pub key: String,
    pub ready_at: Option<i64>,
}

impl From<WaitRecord> for durust::WaitRecord {
    fn from(wait: WaitRecord) -> Self {
        durust::WaitRecord {
            wait_id: durust::WaitId::new(wait.wait_id),
            run_id: durust::RunId::new(wait.run_id),
            command_id: wait.command_id.into(),
            kind: wait.kind,
            key: wait.key,
            ready_at: wait.ready_at.map(durust::TimestampMs),
        }
    }
}

// ---- requests and outcomes -------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartWorkflowRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub workflow_type: WorkflowType,
    pub task_queue: String,
    pub input: PayloadRef,
}

impl From<StartWorkflowRequest> for durust::StartWorkflowRequest {
    fn from(req: StartWorkflowRequest) -> Self {
        durust::StartWorkflowRequest {
            namespace: durust::Namespace::new(req.namespace),
            workflow_id: durust::WorkflowId::new(req.workflow_id),
            workflow_type: req.workflow_type.into(),
            task_queue: durust::TaskQueue::new(req.task_queue),
            input: req.input,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum StartWorkflowOutcome {
    Started { run_id: String },
    AlreadyStarted { run_id: String },
}

impl From<durust::StartWorkflowOutcome> for StartWorkflowOutcome {
    fn from(outcome: durust::StartWorkflowOutcome) -> Self {
        match outcome {
            durust::StartWorkflowOutcome::Started { run_id } => Self::Started { run_id: run_id.0 },
            durust::StartWorkflowOutcome::AlreadyStarted { run_id } => {
                Self::AlreadyStarted { run_id: run_id.0 }
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimWorkflowTaskOptions {
    pub namespace: String,
    pub task_queue: String,
    pub registered_workflow_types: Vec<WorkflowType>,
    pub lease_duration_ms: u64,
}

impl From<ClaimWorkflowTaskOptions> for durust::ClaimWorkflowTaskOptions {
    fn from(opts: ClaimWorkflowTaskOptions) -> Self {
        durust::ClaimWorkflowTaskOptions {
            namespace: durust::Namespace::new(opts.namespace),
            task_queue: durust::TaskQueue::new(opts.task_queue),
            registered_workflow_types: opts
                .registered_workflow_types
                .into_iter()
                .map(Into::into)
                .collect(),
            lease_duration: Duration::from_millis(opts.lease_duration_ms),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimWorkflowBatchOptions {
    pub namespace: String,
    pub task_queue: String,
    pub registered_workflow_types: Vec<WorkflowType>,
    pub lease_duration_ms: u64,
    pub limit: u64,
}

impl From<ClaimWorkflowBatchOptions> for durust::ClaimWorkflowTasksOptions {
    fn from(opts: ClaimWorkflowBatchOptions) -> Self {
        durust::ClaimWorkflowTasksOptions {
            claim: ClaimWorkflowTaskOptions {
                namespace: opts.namespace,
                task_queue: opts.task_queue,
                registered_workflow_types: opts.registered_workflow_types,
                lease_duration_ms: opts.lease_duration_ms,
            }
            .into(),
            limit: opts.limit.max(1) as usize,
            shard_filter: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowTaskClaim {
    pub run_id: String,
    pub worker_id: String,
    pub token: u64,
}

impl From<&durust::WorkflowTaskClaim> for WorkflowTaskClaim {
    fn from(claim: &durust::WorkflowTaskClaim) -> Self {
        Self {
            run_id: claim.run_id.0.clone(),
            worker_id: claim.worker_id.0.clone(),
            token: claim.token,
        }
    }
}

impl From<WorkflowTaskClaim> for durust::WorkflowTaskClaim {
    fn from(claim: WorkflowTaskClaim) -> Self {
        durust::WorkflowTaskClaim {
            run_id: durust::RunId::new(claim.run_id),
            worker_id: durust::WorkerId::new(claim.worker_id),
            token: claim.token,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalInboxRecord {
    pub signal_id: String,
    pub signal_name: String,
    pub payload: PayloadRef,
}

impl From<durust::SignalInboxRecord> for SignalInboxRecord {
    fn from(record: durust::SignalInboxRecord) -> Self {
        Self {
            signal_id: record.signal_id.0,
            signal_name: record.signal_name.0,
            payload: record.payload,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimedWorkflowTask {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_type: WorkflowType,
    pub claim: WorkflowTaskClaim,
    pub replay_target_event_id: u64,
    pub reason: durust::WorkflowTaskReason,
    pub prefetched_history: Vec<HistoryEvent>,
    pub live_signals: Vec<SignalInboxRecord>,
}

impl ClaimedWorkflowTask {
    pub fn from_claimed(
        task: durust::ClaimedWorkflowTask,
        live_signals: Vec<durust::SignalInboxRecord>,
    ) -> Self {
        Self {
            run_id: task.run_id.0,
            workflow_id: task.workflow_id.0,
            workflow_type: (&task.workflow_type).into(),
            claim: (&task.claim).into(),
            replay_target_event_id: task.replay_target_event_id.0,
            reason: task.reason,
            prefetched_history: task
                .prefetched_history
                .into_iter()
                .map(Into::into)
                .collect(),
            live_signals: live_signals.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamHistoryRequest {
    pub run_id: String,
    pub after_event_id: u64,
    pub up_to_event_id: u64,
    pub max_events: f64,
    pub max_bytes: f64,
}

impl From<StreamHistoryRequest> for durust::StreamHistoryRequest {
    fn from(req: StreamHistoryRequest) -> Self {
        durust::StreamHistoryRequest {
            run_id: durust::RunId::new(req.run_id),
            after_event_id: durust::EventId(req.after_event_id),
            up_to_event_id: durust::EventId(req.up_to_event_id),
            max_events: req.max_events.max(0.0).min(usize::MAX as f64) as usize,
            max_bytes: req.max_bytes.max(0.0).min(usize::MAX as f64) as usize,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryChunk {
    pub events: Vec<HistoryEvent>,
    pub last_event_id: u64,
    pub has_more: bool,
}

impl From<durust::HistoryChunk> for HistoryChunk {
    fn from(chunk: durust::HistoryChunk) -> Self {
        Self {
            events: chunk.events.into_iter().map(Into::into).collect(),
            last_event_id: chunk.last_event_id.0,
            has_more: chunk.has_more,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowTaskCommit {
    pub expected_tail_event_id: u64,
    #[serde(default)]
    pub append_events: Option<Vec<NewHistoryEvent>>,
    #[serde(default)]
    pub upsert_waits: Option<Vec<WaitRecord>>,
    #[serde(default)]
    pub delete_waits: Option<Vec<String>>,
    #[serde(default)]
    pub consume_signals: Option<Vec<String>>,
    #[serde(default)]
    pub schedule_activities: Option<Vec<ActivityTask>>,
    #[serde(default)]
    pub schedule_activity_maps: Option<Vec<ActivityMapTask>>,
    #[serde(default)]
    pub start_child_workflows: Option<Vec<ChildWorkflowStartRequested>>,
    #[serde(default)]
    pub schedule_child_workflow_maps: Option<Vec<ChildWorkflowMapTask>>,
    #[serde(default)]
    pub cancel_commands: Option<Vec<CommandId>>,
    #[serde(default)]
    pub query_projection: Option<PayloadRef>,
}

impl From<WorkflowTaskCommit> for durust::WorkflowTaskCommit {
    fn from(commit: WorkflowTaskCommit) -> Self {
        durust::WorkflowTaskCommit {
            expected_tail_event_id: durust::EventId(commit.expected_tail_event_id),
            append_events: commit
                .append_events
                .unwrap_or_default()
                .into_iter()
                .map(|event| durust::NewHistoryEvent::new(event.data.into()))
                .collect(),
            upsert_waits: commit
                .upsert_waits
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            schedule_activities: commit
                .schedule_activities
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            schedule_activity_maps: commit
                .schedule_activity_maps
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            schedule_child_workflow_maps: commit
                .schedule_child_workflow_maps
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            start_child_workflows: commit
                .start_child_workflows
                .unwrap_or_default()
                .into_iter()
                .map(|requested| {
                    durust::ChildStartOutboxMessage::from_requested(
                        &durust::ChildWorkflowStartRequested::from(requested),
                    )
                })
                .collect(),
            consume_signals: commit
                .consume_signals
                .unwrap_or_default()
                .into_iter()
                .map(durust::SignalId::new)
                .collect(),
            delete_waits: commit
                .delete_waits
                .unwrap_or_default()
                .into_iter()
                .map(durust::WaitId::new)
                .collect(),
            cancel_commands: commit
                .cancel_commands
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            query_projection: commit.query_projection,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum CommitOutcome {
    Committed { new_tail_event_id: u64 },
    Conflict,
}

impl From<durust::CommitOutcome> for CommitOutcome {
    fn from(outcome: durust::CommitOutcome) -> Self {
        match outcome {
            durust::CommitOutcome::Committed { new_tail_event_id } => Self::Committed {
                new_tail_event_id: new_tail_event_id.0,
            },
            durust::CommitOutcome::Conflict => Self::Conflict,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseWorkflowTaskOptions {
    #[serde(default)]
    pub visibility_delay_ms: Option<u64>,
}

impl From<ReleaseWorkflowTaskOptions> for durust::WorkflowTaskRelease {
    fn from(options: ReleaseWorkflowTaskOptions) -> Self {
        durust::WorkflowTaskRelease::delayed(Duration::from_millis(
            options.visibility_delay_ms.unwrap_or(0),
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimActivityOptions {
    pub namespace: String,
    pub task_queue: String,
    pub registered_activity_names: Vec<String>,
    pub lease_duration_ms: u64,
}

impl From<ClaimActivityOptions> for durust::ClaimActivityOptions {
    fn from(opts: ClaimActivityOptions) -> Self {
        durust::ClaimActivityOptions {
            namespace: durust::Namespace::new(opts.namespace),
            task_queue: durust::TaskQueue::new(opts.task_queue),
            registered_activity_names: opts
                .registered_activity_names
                .into_iter()
                .map(durust::ActivityName::new)
                .collect(),
            lease_duration: Duration::from_millis(opts.lease_duration_ms),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimActivityBatchOptions {
    pub namespace: String,
    pub task_queue: String,
    pub registered_activity_names: Vec<String>,
    pub lease_duration_ms: u64,
    pub limit: u64,
}

impl From<ClaimActivityBatchOptions> for durust::ClaimActivityTasksOptions {
    fn from(opts: ClaimActivityBatchOptions) -> Self {
        durust::ClaimActivityTasksOptions {
            claim: ClaimActivityOptions {
                namespace: opts.namespace,
                task_queue: opts.task_queue,
                registered_activity_names: opts.registered_activity_names,
                lease_duration_ms: opts.lease_duration_ms,
            }
            .into(),
            limit: opts.limit.max(1) as usize,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityTaskClaim {
    pub activity_id: String,
    pub worker_id: String,
    pub token: u64,
}

impl From<&durust::ActivityTaskClaim> for ActivityTaskClaim {
    fn from(claim: &durust::ActivityTaskClaim) -> Self {
        Self {
            activity_id: claim.activity_id.0.clone(),
            worker_id: claim.worker_id.0.clone(),
            token: claim.token,
        }
    }
}

impl From<ActivityTaskClaim> for durust::ActivityTaskClaim {
    fn from(claim: ActivityTaskClaim) -> Self {
        durust::ActivityTaskClaim {
            activity_id: durust::ActivityId(claim.activity_id),
            worker_id: durust::WorkerId::new(claim.worker_id),
            token: claim.token,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClaimedActivityTask {
    pub task: ActivityTask,
    pub claim: ActivityTaskClaim,
}

impl From<durust::ClaimedActivityTask> for ClaimedActivityTask {
    fn from(claimed: durust::ClaimedActivityTask) -> Self {
        Self {
            claim: (&claimed.claim).into(),
            task: claimed.task.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteActivityRequest {
    pub claim: ActivityTaskClaim,
    pub result: PayloadRef,
}

impl From<CompleteActivityRequest> for durust::CompleteActivityRequest {
    fn from(req: CompleteActivityRequest) -> Self {
        durust::CompleteActivityRequest {
            claim: req.claim.into(),
            result: req.result,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum CompleteActivityOutcome {
    Completed { event_id: u64 },
    AlreadyCompleted,
}

impl From<durust::CompleteActivityOutcome> for CompleteActivityOutcome {
    fn from(outcome: durust::CompleteActivityOutcome) -> Self {
        match outcome {
            durust::CompleteActivityOutcome::Completed { event_id } => Self::Completed {
                event_id: event_id.0,
            },
            durust::CompleteActivityOutcome::AlreadyCompleted => Self::AlreadyCompleted,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteActivitiesRequest {
    pub completions: Vec<CompleteActivityRequest>,
}

impl From<CompleteActivitiesRequest> for durust::CompleteActivityTasksRequest {
    fn from(req: CompleteActivitiesRequest) -> Self {
        durust::CompleteActivityTasksRequest {
            completions: req.completions.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum CompleteActivityItemOutcome {
    Completed { event_id: u64 },
    AlreadyCompleted,
    StaleLease,
    NotFound,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteActivitiesOutcome {
    pub results: Vec<CompleteActivityItemOutcome>,
}

impl CompleteActivitiesOutcome {
    /// Per-item outcomes in request order. A stale claim and a missing run
    /// are item outcomes, as in the TypeScript contract; any other error is
    /// the call's error.
    pub fn from_results(
        results: Vec<durust::CompleteActivityTaskBatchResult>,
    ) -> durust::Result<Self> {
        let mut outcomes = Vec::with_capacity(results.len());
        for result in results {
            outcomes.push(match result.result {
                Ok(durust::CompleteActivityOutcome::Completed { event_id }) => {
                    CompleteActivityItemOutcome::Completed {
                        event_id: event_id.0,
                    }
                }
                Ok(durust::CompleteActivityOutcome::AlreadyCompleted) => {
                    CompleteActivityItemOutcome::AlreadyCompleted
                }
                Err(durust::Error::StaleLease) => CompleteActivityItemOutcome::StaleLease,
                Err(durust::Error::RunNotFound(_)) => CompleteActivityItemOutcome::NotFound,
                Err(err) => return Err(err),
            });
        }
        Ok(Self { results: outcomes })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailActivityRequest {
    pub claim: ActivityTaskClaim,
    pub failure: DurableFailure,
}

impl From<FailActivityRequest> for durust::FailActivityRequest {
    fn from(req: FailActivityRequest) -> Self {
        durust::FailActivityRequest {
            claim: req.claim.into(),
            failure: req.failure,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum FailActivityOutcome {
    Failed { event_id: u64 },
    RetryScheduled { attempt: u32, ready_at_ms: i64 },
    AlreadyCompleted,
}

impl From<durust::FailActivityOutcome> for FailActivityOutcome {
    fn from(outcome: durust::FailActivityOutcome) -> Self {
        match outcome {
            durust::FailActivityOutcome::Failed { event_id } => Self::Failed {
                event_id: event_id.0,
            },
            durust::FailActivityOutcome::RetryScheduled {
                next_attempt,
                ready_at,
            } => Self::RetryScheduled {
                attempt: next_attempt,
                ready_at_ms: ready_at.0,
            },
            durust::FailActivityOutcome::AlreadyCompleted => Self::AlreadyCompleted,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityHeartbeatRequest {
    pub claim: ActivityTaskClaim,
}

impl From<ActivityHeartbeatRequest> for durust::ActivityHeartbeatRequest {
    fn from(req: ActivityHeartbeatRequest) -> Self {
        durust::ActivityHeartbeatRequest {
            claim: req.claim.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ActivityHeartbeatOutcome {
    Recorded,
    AlreadyCompleted,
}

impl From<durust::ActivityHeartbeatOutcome> for ActivityHeartbeatOutcome {
    fn from(outcome: durust::ActivityHeartbeatOutcome) -> Self {
        match outcome {
            durust::ActivityHeartbeatOutcome::Recorded => Self::Recorded,
            durust::ActivityHeartbeatOutcome::AlreadyCompleted => Self::AlreadyCompleted,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FireDueTimersRequest {
    pub namespace: String,
    pub now: i64,
    pub limit: u64,
}

impl From<FireDueTimersRequest> for durust::FireDueTimersRequest {
    fn from(req: FireDueTimersRequest) -> Self {
        durust::FireDueTimersRequest {
            namespace: durust::Namespace::new(req.namespace),
            now: durust::TimestampMs(req.now),
            limit: req.limit as usize,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FireDueTimersOutcome {
    pub fired: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeoutDueActivitiesRequest {
    pub namespace: String,
    pub now: i64,
    pub limit: u64,
}

impl From<TimeoutDueActivitiesRequest> for durust::TimeoutDueActivitiesRequest {
    fn from(req: TimeoutDueActivitiesRequest) -> Self {
        durust::TimeoutDueActivitiesRequest {
            namespace: durust::Namespace::new(req.namespace),
            now: durust::TimestampMs(req.now),
            limit: req.limit as usize,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeoutDueActivitiesOutcome {
    pub timed_out: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalWorkflowRequest {
    pub namespace: String,
    pub workflow_id: String,
    pub signal_id: String,
    pub signal_name: String,
    pub payload: PayloadRef,
}

impl From<SignalWorkflowRequest> for durust::SignalWorkflowRequest {
    fn from(req: SignalWorkflowRequest) -> Self {
        durust::SignalWorkflowRequest {
            namespace: durust::Namespace::new(req.namespace),
            workflow_id: durust::WorkflowId::new(req.workflow_id),
            signal_id: durust::SignalId::new(req.signal_id),
            signal_name: durust::SignalName::new(req.signal_name),
            payload: req.payload,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SignalWorkflowOutcome {
    Accepted,
    Duplicate,
}

impl From<durust::SignalWorkflowOutcome> for SignalWorkflowOutcome {
    fn from(outcome: durust::SignalWorkflowOutcome) -> Self {
        match outcome {
            durust::SignalWorkflowOutcome::Accepted => Self::Accepted,
            durust::SignalWorkflowOutcome::Duplicate => Self::Duplicate,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadSignalInboxRequest {
    pub run_id: String,
    pub signal_name: String,
}

impl From<ReadSignalInboxRequest> for durust::ReadSignalInboxRequest {
    fn from(req: ReadSignalInboxRequest) -> Self {
        durust::ReadSignalInboxRequest {
            run_id: durust::RunId::new(req.run_id),
            signal_name: durust::SignalName::new(req.signal_name),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryWorkflowRequest {
    pub namespace: String,
    pub workflow_id: String,
}

impl From<QueryWorkflowRequest> for durust::QueryProjectionRequest {
    fn from(req: QueryWorkflowRequest) -> Self {
        durust::QueryProjectionRequest {
            namespace: durust::Namespace::new(req.namespace),
            workflow_id: durust::WorkflowId::new(req.workflow_id),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum QueryWorkflowOutcome {
    Found { projection: PayloadRef },
    NotFound,
    NoProjection,
}

impl From<durust::QueryProjectionOutcome> for QueryWorkflowOutcome {
    fn from(outcome: durust::QueryProjectionOutcome) -> Self {
        match outcome {
            durust::QueryProjectionOutcome::Found { payload, .. } => Self::Found {
                projection: payload,
            },
            durust::QueryProjectionOutcome::NotFound => Self::NotFound,
            durust::QueryProjectionOutcome::NoProjection => Self::NoProjection,
        }
    }
}

// ---- construction options and payload GC ------------------------------------

/// Options every constructor takes: payload offload, when the caller wants
/// it, over one of the blob stores below.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendOptions {
    #[serde(default)]
    pub payload: Option<PayloadOptions>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PayloadOptions {
    #[serde(default)]
    pub inline_threshold_bytes: Option<usize>,
    pub blob_store: BlobStore,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all_fields = "camelCase")]
pub enum BlobStore {
    Memory,
    LocalDirectory {
        root: String,
        #[serde(default)]
        prefix: Option<String>,
    },
    S3 {
        bucket: String,
        endpoint: String,
        region: String,
        #[serde(default)]
        prefix: Option<String>,
        access_key_id: String,
        secret_access_key: String,
    },
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostgresOptions {
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub max_pool_size: Option<usize>,
    #[serde(default)]
    pub logical_shards: Option<u32>,
    #[serde(default)]
    pub physical_partitions: Option<u32>,
    #[serde(default)]
    pub statement_timeout_ms: Option<u64>,
    #[serde(default)]
    pub lock_timeout_ms: Option<u64>,
    #[serde(default)]
    pub payload: Option<PayloadOptions>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PayloadGcRequest {
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub min_age_ms: Option<u64>,
}

impl From<PayloadGcRequest> for durust::PayloadGarbageCollectionRequest {
    fn from(req: PayloadGcRequest) -> Self {
        let mut request = Self {
            dry_run: req.dry_run,
            ..Self::default()
        };
        if let Some(ms) = req.min_age_ms {
            request.min_age = Duration::from_millis(ms);
        }
        request
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PayloadGcOutcome {
    pub scanned_blobs: usize,
    pub retained_blobs: usize,
    pub deleted_blobs: usize,
    pub failed_blobs: usize,
}

impl From<durust::PayloadGarbageCollectionOutcome> for PayloadGcOutcome {
    fn from(outcome: durust::PayloadGarbageCollectionOutcome) -> Self {
        Self {
            scanned_blobs: outcome.scanned_blobs,
            retained_blobs: outcome.retained_blobs,
            deleted_blobs: outcome.deleted_blobs,
            failed_blobs: outcome.failed_blobs,
        }
    }
}
