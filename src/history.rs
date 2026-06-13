use crate::{
    ActivityId, ActivityName, CommandId, CommandSeq, DurableFailure, Error, EventId, PayloadRef,
    Result, RetryPolicy, RunId, SignalId, SignalName, TaskQueue, TimestampMs, WorkflowType,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

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
    ActivityMap,
    Timer,
    Signal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityScheduled {
    pub command_id: CommandId,
    pub activity_name: ActivityName,
    pub task_queue: TaskQueue,
    pub retry_policy: RetryPolicy,
    pub start_to_close_timeout: Option<Duration>,
    pub input: PayloadRef,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityCompleted {
    pub command_id: CommandId,
    pub result: PayloadRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityFailed {
    pub command_id: CommandId,
    pub failure: DurableFailure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTimedOut {
    pub command_id: CommandId,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerStarted {
    pub command_id: CommandId,
    pub fire_at: TimestampMs,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerFired {
    pub command_id: CommandId,
    pub fired_at: TimestampMs,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalConsumed {
    pub command_id: CommandId,
    pub signal_id: SignalId,
    pub signal_name: SignalName,
    pub payload: PayloadRef,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityMapInputManifest {
    pub item_count: usize,
    pub page_lengths: Vec<usize>,
    pub pages: Vec<PayloadRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityMapInputPage {
    pub items: Vec<PayloadRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityMapResultManifest {
    pub name: String,
    pub item_count: usize,
    pub page_lengths: Vec<usize>,
    pub pages: Vec<PayloadRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityMapResultPage {
    pub results: Vec<PayloadRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityMapScheduled {
    pub command_id: CommandId,
    pub activity_name: ActivityName,
    pub task_queue: TaskQueue,
    pub retry_policy: RetryPolicy,
    pub start_to_close_timeout: Option<Duration>,
    pub input_manifest: PayloadRef,
    pub result_manifest_name: String,
    pub max_in_flight: usize,
    pub fingerprint: CommandFingerprint,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityMapCompleted {
    pub command_id: CommandId,
    pub result_manifest: PayloadRef,
    pub item_count: usize,
    pub success_count: usize,
    pub failure_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityMapFailed {
    pub command_id: CommandId,
    pub failure: DurableFailure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectWinner {
    pub select_command_id: CommandId,
    pub branch_ordinal: u32,
    pub winning_event_id: EventId,
    pub branches_digest: String,
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
        failure: DurableFailure,
    },
    WorkflowCancelled {
        reason: String,
    },
    WorkflowTaskStarted,
    ActivityScheduled(ActivityScheduled),
    ActivityMapScheduled(ActivityMapScheduled),
    ActivityMapCompleted(ActivityMapCompleted),
    ActivityMapFailed(ActivityMapFailed),
    ActivityCompleted(ActivityCompleted),
    ActivityFailed(ActivityFailed),
    ActivityTimedOut(ActivityTimedOut),
    TimerStarted(TimerStarted),
    TimerFired(TimerFired),
    SignalConsumed(SignalConsumed),
    SelectWinner(SelectWinner),
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
    WorkflowCancelled,
    WorkflowTaskStarted,
    ActivityScheduled,
    ActivityMapScheduled,
    ActivityMapCompleted,
    ActivityMapFailed,
    ActivityCompleted,
    ActivityFailed,
    ActivityTimedOut,
    TimerStarted,
    TimerFired,
    SignalConsumed,
    SelectWinner,
}

impl HistoryEventData {
    pub fn event_type(&self) -> HistoryEventType {
        match self {
            Self::WorkflowStarted { .. } => HistoryEventType::WorkflowStarted,
            Self::WorkflowCompleted { .. } => HistoryEventType::WorkflowCompleted,
            Self::WorkflowFailed { .. } => HistoryEventType::WorkflowFailed,
            Self::WorkflowCancelled { .. } => HistoryEventType::WorkflowCancelled,
            Self::WorkflowTaskStarted => HistoryEventType::WorkflowTaskStarted,
            Self::ActivityScheduled(_) => HistoryEventType::ActivityScheduled,
            Self::ActivityMapScheduled(_) => HistoryEventType::ActivityMapScheduled,
            Self::ActivityMapCompleted(_) => HistoryEventType::ActivityMapCompleted,
            Self::ActivityMapFailed(_) => HistoryEventType::ActivityMapFailed,
            Self::ActivityCompleted(_) => HistoryEventType::ActivityCompleted,
            Self::ActivityFailed(_) => HistoryEventType::ActivityFailed,
            Self::ActivityTimedOut(_) => HistoryEventType::ActivityTimedOut,
            Self::TimerStarted(_) => HistoryEventType::TimerStarted,
            Self::TimerFired(_) => HistoryEventType::TimerFired,
            Self::SignalConsumed(_) => HistoryEventType::SignalConsumed,
            Self::SelectWinner(_) => HistoryEventType::SelectWinner,
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
    pub retry_policy: RetryPolicy,
    pub start_to_close_timeout: Option<Duration>,
    pub attempt: u32,
    pub input: PayloadRef,
    pub map_item: Option<ActivityMapItem>,
}

impl ActivityTask {
    pub fn from_scheduled(scheduled: &ActivityScheduled) -> Self {
        Self {
            activity_id: ActivityId::new(&scheduled.command_id),
            run_id: scheduled.command_id.run_id.clone(),
            command_id: scheduled.command_id.clone(),
            activity_name: scheduled.activity_name.clone(),
            task_queue: scheduled.task_queue.clone(),
            retry_policy: scheduled.retry_policy.clone(),
            start_to_close_timeout: scheduled.start_to_close_timeout,
            attempt: 1,
            input: scheduled.input.clone(),
            map_item: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityMapItem {
    pub map_command_id: CommandId,
    pub item_ordinal: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityMapTask {
    pub map_command_id: CommandId,
    pub activity_name: ActivityName,
    pub task_queue: TaskQueue,
    pub retry_policy: RetryPolicy,
    pub start_to_close_timeout: Option<Duration>,
    pub input_manifest: PayloadRef,
    pub result_manifest_name: String,
    pub max_in_flight: usize,
}

pub fn activity_fingerprint(
    activity_name: ActivityName,
    input_digest: String,
    options_digest: String,
) -> CommandFingerprint {
    CommandFingerprint {
        kind: CommandKind::Activity,
        name: activity_name.0,
        input_digest: Some(input_digest),
        options_digest,
    }
}

pub fn activity_map_fingerprint(
    activity_name: ActivityName,
    input_manifest_digest: String,
    result_manifest_name: String,
    max_in_flight: usize,
    options_digest: String,
) -> CommandFingerprint {
    CommandFingerprint {
        kind: CommandKind::ActivityMap,
        name: activity_name.0,
        input_digest: Some(input_manifest_digest),
        options_digest: format!(
            "{options_digest}:result={result_manifest_name}:max={max_in_flight}"
        ),
    }
}

pub fn timer_fingerprint(kind: &str, deadline: TimestampMs) -> CommandFingerprint {
    CommandFingerprint {
        kind: CommandKind::Timer,
        name: kind.to_owned(),
        input_digest: None,
        options_digest: format!("timestamp-ms:{}", deadline.0),
    }
}

pub fn signal_fingerprint(signal_name: SignalName) -> CommandFingerprint {
    CommandFingerprint {
        kind: CommandKind::Signal,
        name: signal_name.0,
        input_digest: None,
        options_digest: "sha256:default".to_owned(),
    }
}

pub fn command_id(run_id: &RunId, seq: u64) -> CommandId {
    CommandId {
        run_id: run_id.clone(),
        seq: CommandSeq(seq),
    }
}

pub fn encode_activity_map_input_manifest(
    items: Vec<PayloadRef>,
    page_size: usize,
) -> Result<PayloadRef> {
    let page_size = page_size.max(1);
    let item_count = items.len();
    let mut page_lengths = Vec::new();
    let mut pages = Vec::new();
    for chunk in items.chunks(page_size) {
        page_lengths.push(chunk.len());
        pages.push(crate::encode_payload(&ActivityMapInputPage {
            items: chunk.to_vec(),
        })?);
    }
    crate::encode_payload(&ActivityMapInputManifest {
        item_count,
        page_lengths,
        pages,
    })
}

pub fn activity_map_input_at(
    manifest: &ActivityMapInputManifest,
    item_ordinal: u64,
) -> Result<PayloadRef> {
    let item_ordinal = usize::try_from(item_ordinal)
        .map_err(|_| Error::PayloadDecode("activity map item ordinal overflow".to_owned()))?;
    if item_ordinal >= manifest.item_count {
        return Err(Error::PayloadDecode(format!(
            "activity map item ordinal {item_ordinal} out of bounds"
        )));
    }

    let mut base = 0usize;
    for (page_index, page_len) in manifest.page_lengths.iter().copied().enumerate() {
        let end = base.saturating_add(page_len);
        if item_ordinal < end {
            let page_ref = manifest.pages.get(page_index).ok_or_else(|| {
                Error::PayloadDecode(format!("activity map manifest missing page {page_index}"))
            })?;
            let page: ActivityMapInputPage = crate::decode_payload(page_ref)?;
            let page_offset = item_ordinal - base;
            return page.items.get(page_offset).cloned().ok_or_else(|| {
                Error::PayloadDecode(format!(
                    "activity map page {page_index} missing item offset {page_offset}"
                ))
            });
        }
        base = end;
    }

    Err(Error::PayloadDecode(format!(
        "activity map manifest page lengths do not cover item ordinal {item_ordinal}"
    )))
}

pub fn encode_activity_map_result_manifest(
    name: String,
    results: Vec<PayloadRef>,
    page_lengths: &[usize],
) -> Result<PayloadRef> {
    let item_count = results.len();
    let expected: usize = page_lengths.iter().copied().sum();
    if expected != item_count {
        return Err(Error::PayloadEncode(format!(
            "activity map result page lengths cover {expected} items, expected {item_count}"
        )));
    }

    let mut pages = Vec::new();
    let mut offset = 0usize;
    for page_len in page_lengths {
        let end = offset + page_len;
        pages.push(crate::encode_payload(&ActivityMapResultPage {
            results: results[offset..end].to_vec(),
        })?);
        offset = end;
    }

    crate::encode_payload(&ActivityMapResultManifest {
        name,
        item_count,
        page_lengths: page_lengths.to_vec(),
        pages,
    })
}

pub fn decode_activity_map_result_refs(manifest_ref: &PayloadRef) -> Result<Vec<PayloadRef>> {
    let manifest: ActivityMapResultManifest = crate::decode_payload(manifest_ref)?;
    let mut results = Vec::with_capacity(manifest.item_count);
    for (page_index, page_ref) in manifest.pages.iter().enumerate() {
        let page: ActivityMapResultPage = crate::decode_payload(page_ref)?;
        let expected_len = manifest
            .page_lengths
            .get(page_index)
            .copied()
            .ok_or_else(|| {
                Error::PayloadDecode(format!(
                    "activity map result manifest missing page length {page_index}"
                ))
            })?;
        if page.results.len() != expected_len {
            return Err(Error::PayloadDecode(format!(
                "activity map result page {page_index} has {} results, expected {expected_len}",
                page.results.len()
            )));
        }
        results.extend(page.results);
    }
    if results.len() != manifest.item_count {
        return Err(Error::PayloadDecode(format!(
            "activity map result manifest decoded {} results, expected {}",
            results.len(),
            manifest.item_count
        )));
    }
    Ok(results)
}
pub const ACTIVITY_MAP_MANIFEST_PAGE_SIZE: usize = 1024;
