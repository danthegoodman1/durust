//! Pure, storage-agnostic helpers shared by the SQLite, Postgres, and in-memory
//! providers. These carry no transaction or connection state, so keeping a single
//! copy prevents the providers' retry, timeout, codec, and metadata encodings from
//! silently diverging.

use crate::{
    ActivityId, ActivityTask, ChildWorkflowMapItemOutcome, CommandId, HistoryEventData,
    TimestampMs, WorkflowTaskCommit, WorkflowTaskReason,
};
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::{
    CodecId, CompressionId, EncryptionMetadata, Error, HistoryEventType, ParentClosePolicy, Result,
    WaitKind, WorkflowChangeMarkerKind,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn codec_to_str(codec: CodecId) -> &'static str {
    match codec {
        CodecId::MessagePack => "messagepack",
        CodecId::Json => "json",
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn codec_from_str(value: &str) -> Result<CodecId> {
    match value {
        "messagepack" => Ok(CodecId::MessagePack),
        "json" => Ok(CodecId::Json),
        other => Err(Error::PayloadDecode(format!(
            "unknown payload codec `{other}`"
        ))),
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn compression_to_str(compression: CompressionId) -> &'static str {
    match compression {
        CompressionId::None => "none",
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn compression_from_str(value: &str) -> Result<CompressionId> {
    match value {
        "none" => Ok(CompressionId::None),
        other => Err(Error::PayloadDecode(format!(
            "unknown payload compression `{other}`"
        ))),
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn encode_encryption_metadata(
    encryption: &Option<EncryptionMetadata>,
) -> Result<Option<Vec<u8>>> {
    encryption
        .as_ref()
        .map(|metadata| {
            rmp_serde::to_vec_named(metadata).map_err(|err| Error::PayloadEncode(err.to_string()))
        })
        .transpose()
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn decode_encryption_metadata(
    blob: Option<Vec<u8>>,
) -> Result<Option<EncryptionMetadata>> {
    blob.map(|blob| {
        rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))
    })
    .transpose()
}

/// How a run reached its terminal transition, deciding which operational rows
/// its cleanup may delete. Every provider deletes a terminal run's activity
/// tasks, map descriptors, map results, waits, and dispatched child outbox
/// rows: history is authoritative, so nothing rebuilds from them, and late
/// activity calls answer `AlreadyCompleted` from the row's absence. Signal
/// rows are subtler: unconsumed deliveries always stay readable through the
/// inbox, and consumed rows are the `signal_id` dedup record. A closed run
/// (completed/failed/cancelled) rejects every further send with
/// `TerminalWorkflow` before the dedup lookup can matter, so it may drop its
/// consumed rows; a run that continues as new keeps accepting sends under the
/// same workflow id and must keep them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TerminalCleanup {
    Closed,
    ContinuedAsNew,
}

impl TerminalCleanup {
    pub(crate) fn for_terminal_event(event: &HistoryEventData) -> Self {
        match event {
            HistoryEventData::WorkflowContinuedAsNew { .. } => Self::ContinuedAsNew,
            _ => Self::Closed,
        }
    }

    pub(crate) fn deletes_consumed_signals(self) -> bool {
        self == Self::Closed
    }
}

/// Which deadline reclaimed a running activity. The attribution decides the
/// message persisted in history (`ActivityTimedOut` events and map-item
/// `DurableFailure`s), so existing variants' text is append-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActivityTimeoutAttribution {
    StartToClose,
    MissedHeartbeat,
    /// The implicit lease-as-heartbeat deadline of a task with no explicit
    /// timeouts lapsed: the claim holder stopped heartbeating (or never did).
    LeaseExpired,
}

/// An expired start-to-close deadline is the stronger fact and wins the
/// attribution even when the heartbeat deadline lapsed too. A lapsed
/// heartbeat deadline is a missed heartbeat only when it came from an
/// explicit `heartbeat_timeout`; the implicit lease interval produces the
/// lease-flavored attribution instead.
pub(crate) fn activity_timeout_attribution(
    start_to_close_due: bool,
    heartbeat_due: bool,
    implicit_heartbeat: bool,
) -> ActivityTimeoutAttribution {
    if start_to_close_due || !heartbeat_due {
        ActivityTimeoutAttribution::StartToClose
    } else if implicit_heartbeat {
        ActivityTimeoutAttribution::LeaseExpired
    } else {
        ActivityTimeoutAttribution::MissedHeartbeat
    }
}

pub(crate) fn timeout_message(
    activity_id: &ActivityId,
    attempt: u32,
    attribution: ActivityTimeoutAttribution,
) -> String {
    let attempt = attempt.max(1);
    match attribution {
        ActivityTimeoutAttribution::StartToClose => {
            format!(
                "activity `{}` timed out on attempt {attempt}",
                activity_id.0
            )
        }
        ActivityTimeoutAttribution::MissedHeartbeat => format!(
            "activity `{}` missed heartbeat on attempt {attempt}",
            activity_id.0
        ),
        ActivityTimeoutAttribution::LeaseExpired => format!(
            "activity `{}` claim lease expired without heartbeat on attempt {attempt}",
            activity_id.0
        ),
    }
}

pub(crate) fn should_retry_activity(task: &ActivityTask) -> bool {
    task.attempt < task.retry_policy.max_attempts.max(1)
}

/// Base delay before the first exponential retry; every further failed
/// attempt doubles it.
pub(crate) const RETRY_BACKOFF_BASE_MS: i64 = 1_000;

/// Visibility deadline for the retry scheduled after `failed_attempt` failed
/// (attempts are 1-based). `None` means the retry is immediately claimable
/// (`RetryBackoff::None`); `RetryBackoff::Exponential` yields
/// `now + base * 2^(failed_attempt - 1)`, saturating instead of overflowing so
/// absurd attempt counts push visibility to the far future rather than
/// wrapping into the past. Backoff paces explicit activity failures only;
/// timeout retries are already paced by the timeout deadline itself.
pub(crate) fn retry_visible_at_ms(
    policy: &crate::RetryPolicy,
    failed_attempt: u32,
    now: TimestampMs,
) -> Option<i64> {
    match policy.backoff {
        crate::RetryBackoff::None => None,
        crate::RetryBackoff::Exponential => {
            let exponent = failed_attempt.saturating_sub(1).min(62);
            let factor = 1_i64 << exponent;
            let delay = RETRY_BACKOFF_BASE_MS.saturating_mul(factor);
            Some(now.0.saturating_add(delay))
        }
    }
}

pub(crate) enum ActivityFailureDecision {
    Retry { next_attempt: u32 },
    Fail,
}

/// Single retry-versus-fail decision for a failed activity attempt. Providers
/// honor the generic `non_retryable` flag before the stored retry policy, so a
/// non-retryable failure records the terminal outcome even with attempts left.
pub(crate) fn activity_failure_decision(
    task: &ActivityTask,
    non_retryable: bool,
) -> ActivityFailureDecision {
    if !non_retryable && should_retry_activity(task) {
        ActivityFailureDecision::Retry {
            next_attempt: task.attempt.saturating_add(1),
        }
    } else {
        ActivityFailureDecision::Fail
    }
}

/// Timeouts are always retryable up to the stored policy's attempt budget.
pub(crate) fn activity_timeout_decision(task: &ActivityTask) -> ActivityFailureDecision {
    activity_failure_decision(task, false)
}

/// True when a commit carries any workflow-visible mutation. A terminal run
/// must reject every such commit (`SPEC.md`: "terminal workflow rejects new
/// workflow-visible commands"); only a fully empty commit is an acceptable
/// no-op against a terminal run.
pub(crate) fn commit_has_workflow_visible_mutations(commit: &WorkflowTaskCommit) -> bool {
    !commit.append_events.is_empty()
        || !commit.upsert_waits.is_empty()
        || !commit.schedule_activities.is_empty()
        || !commit.schedule_activity_maps.is_empty()
        || !commit.schedule_child_workflow_maps.is_empty()
        || !commit.start_child_workflows.is_empty()
        || !commit.consume_signals.is_empty()
        || !commit.delete_waits.is_empty()
        || !commit.cancel_commands.is_empty()
        || commit.query_projection.is_some()
}

/// Ready reason a run should carry after a workflow task commit has applied
/// its wait upserts/deletes and signal consumption. Terminal runs are never
/// ready. A child start/failure event appended by the same commit keeps its
/// specific reason; otherwise a still-consumable signal matching a live signal
/// wait re-marks the run so a delivery racing the claim window is not lost.
pub(crate) fn post_commit_ready_reason(
    terminal_after_commit: bool,
    same_commit_child_reason: Option<WorkflowTaskReason>,
    signal_wait_ready: bool,
) -> Option<WorkflowTaskReason> {
    if terminal_after_commit {
        return None;
    }
    same_commit_child_reason
        .or_else(|| signal_wait_ready.then_some(WorkflowTaskReason::SignalReceived))
}

/// Parent-visible history fact and wake reason for a plain (non-map) child
/// workflow reaching a terminal state. Continue-as-new is not terminal from
/// the parent's perspective and maps to `None`.
pub(crate) fn child_terminal_event_data_and_reason(
    command_id: CommandId,
    terminal_event: &HistoryEventData,
) -> Option<(HistoryEventData, WorkflowTaskReason)> {
    match terminal_event {
        HistoryEventData::WorkflowCompleted { result } => Some((
            HistoryEventData::ChildWorkflowCompleted(crate::ChildWorkflowCompleted {
                command_id,
                result: result.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowCompleted,
        )),
        HistoryEventData::WorkflowFailed { failure } => Some((
            HistoryEventData::ChildWorkflowFailed(crate::ChildWorkflowFailed {
                command_id,
                failure: failure.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowFailed,
        )),
        HistoryEventData::WorkflowCancelled { reason } => Some((
            HistoryEventData::ChildWorkflowCancelled(crate::ChildWorkflowCancelled {
                command_id,
                reason: reason.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowCancelled,
        )),
        _ => None,
    }
}

/// Map-item outcome for a child-workflow-map child reaching a terminal state,
/// mirroring `child_terminal_event_data_and_reason` for map-routed children.
pub(crate) fn child_terminal_map_item_outcome(
    terminal_event: &HistoryEventData,
) -> Option<ChildWorkflowMapItemOutcome> {
    match terminal_event {
        HistoryEventData::WorkflowCompleted { result } => {
            Some(ChildWorkflowMapItemOutcome::Succeeded {
                result: result.clone(),
            })
        }
        HistoryEventData::WorkflowFailed { failure } => Some(ChildWorkflowMapItemOutcome::Failed {
            failure: failure.clone(),
        }),
        HistoryEventData::WorkflowCancelled { reason } => {
            Some(ChildWorkflowMapItemOutcome::Cancelled {
                reason: reason.clone(),
            })
        }
        _ => None,
    }
}

pub(crate) fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

pub(crate) fn unix_epoch_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn ready_at_ms_for_delay(delay: Duration) -> i64 {
    if delay.is_zero() {
        0
    } else {
        unix_epoch_millis().saturating_add(duration_millis_i64(delay))
    }
}

/// GC grace-period cutoff: a blob last modified at or before this instant is
/// old enough to delete. With `min_age` zero every existing blob qualifies,
/// which is the pre-grace-period behavior tests use to force collection.
pub(crate) fn payload_gc_cutoff_ms(now_ms: i64, min_age: Duration) -> i64 {
    now_ms.saturating_sub(duration_millis_i64(min_age))
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn activity_timeout_at_ms(timeout: Option<Duration>) -> Option<i64> {
    activity_timeout_at_ms_from(TimestampMs(unix_epoch_millis()), timeout)
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn activity_timeout_at_ms_from(
    now: TimestampMs,
    timeout: Option<Duration>,
) -> Option<i64> {
    timeout.map(|timeout| now.0.saturating_add(duration_millis_i64(timeout)))
}

/// Expiry timestamp for a claim lease taken at `now`. A claim whose lease
/// expiry is `<= now` is reclaimable; the reclaim issues a fresh fencing token
/// so the previous holder's commit/release/complete is rejected as stale.
pub(crate) fn claim_lease_until_ms(now: TimestampMs, lease_duration: Duration) -> i64 {
    now.0.saturating_add(duration_millis_i64(lease_duration))
}

/// Implicit heartbeat interval for a claim on an activity with neither an
/// explicit start-to-close timeout nor an explicit heartbeat timeout. Such a
/// task has no other crash-recovery signal, so the claim lease acts as its
/// heartbeat interval: the claim stamps `heartbeat_deadline = now + lease`,
/// every accepted heartbeat re-stamps it (the interval is persisted on the
/// claimed row so refreshes know it), and the existing heartbeat-deadline
/// scan reclaims the task one lease after the last heartbeat. A faithfully
/// heartbeating holder therefore survives indefinitely; one that never
/// heartbeats is reclaimed one lease after the claim. When either explicit
/// deadline exists it stays authoritative and this returns `None`.
pub(crate) fn activity_claim_implicit_heartbeat_ms(
    start_to_close_timeout: Option<Duration>,
    heartbeat_timeout: Option<Duration>,
    lease_duration: Duration,
) -> Option<i64> {
    (start_to_close_timeout.is_none() && heartbeat_timeout.is_none())
        .then(|| duration_millis_i64(lease_duration))
}

/// Heartbeat deadline to stamp at claim time and on every accepted heartbeat.
/// An explicit `heartbeat_timeout` is authoritative; otherwise the implicit
/// lease interval persisted at claim keeps a live holder ahead of the
/// heartbeat-deadline scan. `None` means no heartbeat deadline (the task has
/// an explicit start-to-close timeout only).
pub(crate) fn activity_heartbeat_deadline_at_ms(
    now: TimestampMs,
    heartbeat_timeout: Option<Duration>,
    implicit_heartbeat_ms: Option<i64>,
) -> Option<i64> {
    heartbeat_timeout
        .map(|timeout| now.0.saturating_add(duration_millis_i64(timeout)))
        .or_else(|| implicit_heartbeat_ms.map(|interval| now.0.saturating_add(interval.max(0))))
}

// The string codecs below are persisted in provider storage (ready reasons,
// history event type columns, wait kinds, marker kinds, parent close
// policies). Values are part of each provider's on-disk format and must never
// change; `persisted_codec_strings_are_pinned` pins them.

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn reason_to_str(reason: &WorkflowTaskReason) -> &'static str {
    match reason {
        WorkflowTaskReason::WorkflowStarted => "workflow_started",
        WorkflowTaskReason::ActivityCompleted => "activity_completed",
        WorkflowTaskReason::ActivityFailed => "activity_failed",
        WorkflowTaskReason::ActivityTimedOut => "activity_timed_out",
        WorkflowTaskReason::ActivityMapCompleted => "activity_map_completed",
        WorkflowTaskReason::ActivityMapFailed => "activity_map_failed",
        WorkflowTaskReason::ChildWorkflowStarted => "child_workflow_started",
        WorkflowTaskReason::ChildWorkflowCompleted => "child_workflow_completed",
        WorkflowTaskReason::ChildWorkflowFailed => "child_workflow_failed",
        WorkflowTaskReason::ChildWorkflowCancelled => "child_workflow_cancelled",
        WorkflowTaskReason::ChildWorkflowMapCompleted => "child_workflow_map_completed",
        WorkflowTaskReason::ChildWorkflowMapFailed => "child_workflow_map_failed",
        WorkflowTaskReason::TimerFired => "timer_fired",
        WorkflowTaskReason::SignalReceived => "signal_received",
        WorkflowTaskReason::CacheEvicted => "cache_evicted",
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn reason_from_str(value: &str) -> Result<WorkflowTaskReason> {
    match value {
        "workflow_started" => Ok(WorkflowTaskReason::WorkflowStarted),
        "activity_completed" => Ok(WorkflowTaskReason::ActivityCompleted),
        "activity_failed" => Ok(WorkflowTaskReason::ActivityFailed),
        "activity_timed_out" => Ok(WorkflowTaskReason::ActivityTimedOut),
        "activity_map_completed" => Ok(WorkflowTaskReason::ActivityMapCompleted),
        "activity_map_failed" => Ok(WorkflowTaskReason::ActivityMapFailed),
        "child_workflow_started" => Ok(WorkflowTaskReason::ChildWorkflowStarted),
        "child_workflow_completed" => Ok(WorkflowTaskReason::ChildWorkflowCompleted),
        "child_workflow_failed" => Ok(WorkflowTaskReason::ChildWorkflowFailed),
        "child_workflow_cancelled" => Ok(WorkflowTaskReason::ChildWorkflowCancelled),
        "child_workflow_map_completed" => Ok(WorkflowTaskReason::ChildWorkflowMapCompleted),
        "child_workflow_map_failed" => Ok(WorkflowTaskReason::ChildWorkflowMapFailed),
        "timer_fired" => Ok(WorkflowTaskReason::TimerFired),
        "signal_received" => Ok(WorkflowTaskReason::SignalReceived),
        "cache_evicted" => Ok(WorkflowTaskReason::CacheEvicted),
        other => Err(Error::Backend(format!(
            "unknown workflow task reason `{other}`"
        ))),
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn event_type_to_str(event_type: &HistoryEventType) -> &'static str {
    match event_type {
        HistoryEventType::WorkflowStarted => "workflow_started",
        HistoryEventType::WorkflowCompleted => "workflow_completed",
        HistoryEventType::WorkflowFailed => "workflow_failed",
        HistoryEventType::WorkflowCancelled => "workflow_cancelled",
        HistoryEventType::WorkflowContinuedAsNew => "workflow_continued_as_new",
        HistoryEventType::WorkflowTaskStarted => "workflow_task_started",
        HistoryEventType::ActivityScheduled => "activity_scheduled",
        HistoryEventType::ActivityMapScheduled => "activity_map_scheduled",
        HistoryEventType::ActivityMapCompleted => "activity_map_completed",
        HistoryEventType::ActivityMapFailed => "activity_map_failed",
        HistoryEventType::ActivityCompleted => "activity_completed",
        HistoryEventType::ActivityFailed => "activity_failed",
        HistoryEventType::ActivityTimedOut => "activity_timed_out",
        HistoryEventType::ChildWorkflowStartRequested => "child_workflow_start_requested",
        HistoryEventType::ChildWorkflowStarted => "child_workflow_started",
        HistoryEventType::ChildWorkflowCompleted => "child_workflow_completed",
        HistoryEventType::ChildWorkflowFailed => "child_workflow_failed",
        HistoryEventType::ChildWorkflowCancelled => "child_workflow_cancelled",
        HistoryEventType::ChildWorkflowMapScheduled => "child_workflow_map_scheduled",
        HistoryEventType::ChildWorkflowMapCompleted => "child_workflow_map_completed",
        HistoryEventType::ChildWorkflowMapFailed => "child_workflow_map_failed",
        HistoryEventType::TimerStarted => "timer_started",
        HistoryEventType::TimerFired => "timer_fired",
        HistoryEventType::SignalConsumed => "signal_consumed",
        HistoryEventType::SelectWinner => "select_winner",
        HistoryEventType::VersionMarker => "version_marker",
        HistoryEventType::DeprecatedPatchMarker => "deprecated_patch_marker",
        HistoryEventType::SideEffectMarker => "side_effect_marker",
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn event_type_from_str(value: &str) -> Result<HistoryEventType> {
    match value {
        "workflow_started" => Ok(HistoryEventType::WorkflowStarted),
        "workflow_completed" => Ok(HistoryEventType::WorkflowCompleted),
        "workflow_failed" => Ok(HistoryEventType::WorkflowFailed),
        "workflow_cancelled" => Ok(HistoryEventType::WorkflowCancelled),
        "workflow_continued_as_new" => Ok(HistoryEventType::WorkflowContinuedAsNew),
        "workflow_task_started" => Ok(HistoryEventType::WorkflowTaskStarted),
        "activity_scheduled" => Ok(HistoryEventType::ActivityScheduled),
        "activity_map_scheduled" => Ok(HistoryEventType::ActivityMapScheduled),
        "activity_map_completed" => Ok(HistoryEventType::ActivityMapCompleted),
        "activity_map_failed" => Ok(HistoryEventType::ActivityMapFailed),
        "activity_completed" => Ok(HistoryEventType::ActivityCompleted),
        "activity_failed" => Ok(HistoryEventType::ActivityFailed),
        "activity_timed_out" => Ok(HistoryEventType::ActivityTimedOut),
        "child_workflow_start_requested" => Ok(HistoryEventType::ChildWorkflowStartRequested),
        "child_workflow_started" => Ok(HistoryEventType::ChildWorkflowStarted),
        "child_workflow_completed" => Ok(HistoryEventType::ChildWorkflowCompleted),
        "child_workflow_failed" => Ok(HistoryEventType::ChildWorkflowFailed),
        "child_workflow_cancelled" => Ok(HistoryEventType::ChildWorkflowCancelled),
        "child_workflow_map_scheduled" => Ok(HistoryEventType::ChildWorkflowMapScheduled),
        "child_workflow_map_completed" => Ok(HistoryEventType::ChildWorkflowMapCompleted),
        "child_workflow_map_failed" => Ok(HistoryEventType::ChildWorkflowMapFailed),
        "timer_started" => Ok(HistoryEventType::TimerStarted),
        "timer_fired" => Ok(HistoryEventType::TimerFired),
        "signal_consumed" => Ok(HistoryEventType::SignalConsumed),
        "select_winner" => Ok(HistoryEventType::SelectWinner),
        "version_marker" => Ok(HistoryEventType::VersionMarker),
        "deprecated_patch_marker" => Ok(HistoryEventType::DeprecatedPatchMarker),
        "side_effect_marker" => Ok(HistoryEventType::SideEffectMarker),
        other => Err(Error::Backend(format!("unknown event type `{other}`"))),
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn wait_kind_to_str(kind: &WaitKind) -> &'static str {
    match kind {
        WaitKind::Timer => "timer",
        WaitKind::Signal => "signal",
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn marker_kind_to_str(kind: WorkflowChangeMarkerKind) -> &'static str {
    match kind {
        WorkflowChangeMarkerKind::Version => "version",
        WorkflowChangeMarkerKind::DeprecatedPatch => "deprecated_patch",
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn marker_kind_from_str(value: &str) -> Result<WorkflowChangeMarkerKind> {
    match value {
        "version" => Ok(WorkflowChangeMarkerKind::Version),
        "deprecated_patch" => Ok(WorkflowChangeMarkerKind::DeprecatedPatch),
        other => Err(Error::Backend(format!(
            "unknown workflow change marker kind `{other}`"
        ))),
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn parent_close_policy_to_str(policy: ParentClosePolicy) -> &'static str {
    match policy {
        ParentClosePolicy::Cancel => "cancel",
        ParentClosePolicy::Abandon => "abandon",
    }
}

/// Test-only catalog of one commit per workflow-visible mutation kind. The
/// provider suites iterate it to pin the shared terminal guard table-driven,
/// so a new `WorkflowTaskCommit` field must be added here (and to
/// `commit_has_workflow_visible_mutations`) to be covered.
#[cfg(test)]
pub(crate) mod commit_test_support {
    use crate::{
        CommandId, EventId, HistoryEventData, ParentClosePolicy, RunId, WaitKind,
        WorkflowTaskCommit,
    };

    pub(crate) fn mutating_commits(
        run_id: &RunId,
        expected_tail_event_id: EventId,
    ) -> Vec<(&'static str, WorkflowTaskCommit)> {
        let command_id = CommandId {
            run_id: run_id.clone(),
            seq: crate::CommandSeq(900),
        };
        let base = WorkflowTaskCommit {
            expected_tail_event_id,
            ..WorkflowTaskCommit::default()
        };
        vec![
            (
                "append_events",
                WorkflowTaskCommit {
                    append_events: vec![crate::NewHistoryEvent::new(
                        HistoryEventData::WorkflowCompleted {
                            result: crate::encode_payload(&0_u64).unwrap(),
                        },
                    )],
                    ..base.clone()
                },
            ),
            (
                "schedule_activities",
                WorkflowTaskCommit {
                    schedule_activities: vec![activity_task(run_id, &command_id)],
                    ..base.clone()
                },
            ),
            (
                "upsert_waits",
                WorkflowTaskCommit {
                    upsert_waits: vec![crate::WaitRecord {
                        wait_id: crate::WaitId::new(format!("{run_id}:900:signal")),
                        run_id: run_id.clone(),
                        command_id: command_id.clone(),
                        kind: WaitKind::Signal,
                        key: "go".to_owned(),
                        ready_at: None,
                    }],
                    ..base.clone()
                },
            ),
            (
                "consume_signals",
                WorkflowTaskCommit {
                    consume_signals: vec![crate::SignalId::new("terminal-guard-signal")],
                    ..base.clone()
                },
            ),
            (
                "delete_waits",
                WorkflowTaskCommit {
                    delete_waits: vec![crate::WaitId::new(format!("{run_id}:900:signal"))],
                    ..base.clone()
                },
            ),
            (
                "start_child_workflows",
                WorkflowTaskCommit {
                    start_child_workflows: vec![crate::ChildStartOutboxMessage {
                        command_id: command_id.clone(),
                        workflow_id: crate::WorkflowId::new(format!("{run_id}/guard-child")),
                        workflow_type: crate::WorkflowType::new("tests.guard-child", 1),
                        task_queue: crate::TaskQueue::new("guard-children"),
                        input: crate::encode_payload(&0_u64).unwrap(),
                        parent_close_policy: ParentClosePolicy::Cancel,
                        child_map_item: None,
                    }],
                    ..base.clone()
                },
            ),
            (
                "schedule_activity_maps",
                WorkflowTaskCommit {
                    schedule_activity_maps: vec![crate::ActivityMapTask {
                        map_command_id: command_id.clone(),
                        activity_name: crate::ActivityName::new("tests.guard-map"),
                        task_queue: crate::TaskQueue::new("guard-activities"),
                        input_manifest: crate::encode_payload(&0_u64).unwrap(),
                        result_manifest_name: "results".to_owned(),
                        max_in_flight: 1,
                        retry_policy: crate::RetryPolicy::default(),
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                    }],
                    ..base.clone()
                },
            ),
            (
                "schedule_child_workflow_maps",
                WorkflowTaskCommit {
                    schedule_child_workflow_maps: vec![crate::ChildWorkflowMapTask {
                        map_command_id: command_id.clone(),
                        workflow_type: crate::WorkflowType::new("tests.guard-child-map", 1),
                        task_queue: crate::TaskQueue::new("guard-children"),
                        input_manifest: crate::encode_payload(&0_u64).unwrap(),
                        result_manifest_name: "results".to_owned(),
                        workflow_id_prefix: format!("{run_id}/guard-child-map"),
                        max_in_flight: 1,
                        parent_close_policy: ParentClosePolicy::Cancel,
                        failure_mode: crate::ChildWorkflowMapFailureMode::FailFast,
                    }],
                    ..base.clone()
                },
            ),
            (
                "cancel_commands",
                WorkflowTaskCommit {
                    cancel_commands: vec![command_id.clone()],
                    ..base.clone()
                },
            ),
            (
                "query_projection",
                WorkflowTaskCommit {
                    query_projection: Some(crate::encode_payload(&0_u64).unwrap()),
                    ..base
                },
            ),
        ]
    }

    pub(crate) fn activity_task(run_id: &RunId, command_id: &CommandId) -> crate::ActivityTask {
        crate::ActivityTask {
            activity_id: crate::ActivityId::new(command_id),
            run_id: run_id.clone(),
            command_id: command_id.clone(),
            activity_name: crate::ActivityName::new("tests.guard-activity"),
            task_queue: crate::TaskQueue::new("guard-activities"),
            input: crate::encode_payload(&0_u64).unwrap(),
            attempt: 1,
            retry_policy: crate::RetryPolicy::default(),
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            map_item: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::commit_test_support;
    use super::*;

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    #[test]
    fn encryption_metadata_round_trips_through_the_provider_codec() {
        // The encryption column is spec'd forward-compatibility metadata. Exercise the
        // non-null branch end to end so the `Some` path the providers persist is covered,
        // not just the `None` default the rest of the suite uses.
        let metadata = EncryptionMetadata {
            key_id: "kms://durust/test-key/1".to_owned(),
        };
        let encoded = encode_encryption_metadata(&Some(metadata.clone()))
            .expect("encryption metadata should encode")
            .expect("Some metadata should produce a stored blob");
        let decoded =
            decode_encryption_metadata(Some(encoded)).expect("encryption metadata should decode");
        assert_eq!(decoded, Some(metadata));

        assert_eq!(encode_encryption_metadata(&None).unwrap(), None);
        assert_eq!(decode_encryption_metadata(None).unwrap(), None);
    }

    #[test]
    fn claim_lease_until_saturates_and_offsets_from_the_given_now() {
        assert_eq!(
            claim_lease_until_ms(TimestampMs(1_000), Duration::from_secs(30)),
            31_000
        );
        // Saturation instead of overflow keeps an absurd lease from wrapping into the past.
        assert_eq!(
            claim_lease_until_ms(TimestampMs(i64::MAX), Duration::from_secs(1)),
            i64::MAX
        );
    }

    #[test]
    fn implicit_heartbeat_claim_stamping_applies_only_without_explicit_deadlines() {
        let now = TimestampMs(500);
        let lease = Duration::from_secs(30);
        // Decision table: both timeouts None -> the lease becomes the implicit
        // heartbeat interval and the claim stamps a heartbeat deadline.
        assert_eq!(
            activity_claim_implicit_heartbeat_ms(None, None, lease),
            Some(30_000)
        );
        assert_eq!(
            activity_heartbeat_deadline_at_ms(now, None, Some(30_000)),
            Some(30_500)
        );
        // Any explicit deadline stays authoritative; the lease must not double-drive.
        assert_eq!(
            activity_claim_implicit_heartbeat_ms(Some(Duration::from_secs(1)), None, lease),
            None
        );
        assert_eq!(
            activity_claim_implicit_heartbeat_ms(None, Some(Duration::from_secs(1)), lease),
            None
        );
        // Explicit heartbeat timeout wins the refresh even when an implicit
        // interval is (incorrectly) present.
        assert_eq!(
            activity_heartbeat_deadline_at_ms(now, Some(Duration::from_secs(1)), Some(30_000)),
            Some(1_500)
        );
        // Explicit start-to-close only: no heartbeat deadline at all.
        assert_eq!(activity_heartbeat_deadline_at_ms(now, None, None), None);
        // A negative persisted interval clamps to "due now" instead of
        // stamping a deadline in the past.
        assert_eq!(
            activity_heartbeat_deadline_at_ms(now, None, Some(-5)),
            Some(500)
        );
    }

    #[test]
    fn timeout_attribution_orders_start_to_close_heartbeat_and_lease() {
        // Start-to-close is the stronger fact and wins even when the
        // heartbeat deadline lapsed too.
        assert_eq!(
            activity_timeout_attribution(true, true, false),
            ActivityTimeoutAttribution::StartToClose
        );
        assert_eq!(
            activity_timeout_attribution(true, false, false),
            ActivityTimeoutAttribution::StartToClose
        );
        // A lapsed explicit heartbeat deadline is a missed heartbeat.
        assert_eq!(
            activity_timeout_attribution(false, true, false),
            ActivityTimeoutAttribution::MissedHeartbeat
        );
        // A lapsed implicit (lease-derived) deadline is a lease expiry.
        assert_eq!(
            activity_timeout_attribution(false, true, true),
            ActivityTimeoutAttribution::LeaseExpired
        );
    }

    #[test]
    fn timeout_messages_are_pinned() {
        // These strings are persisted in history (`ActivityTimedOut` events
        // and map-item failures); existing text is append-only.
        let activity_id = ActivityId("act".to_owned());
        assert_eq!(
            timeout_message(&activity_id, 2, ActivityTimeoutAttribution::StartToClose),
            "activity `act` timed out on attempt 2"
        );
        assert_eq!(
            timeout_message(&activity_id, 2, ActivityTimeoutAttribution::MissedHeartbeat),
            "activity `act` missed heartbeat on attempt 2"
        );
        assert_eq!(
            timeout_message(&activity_id, 2, ActivityTimeoutAttribution::LeaseExpired),
            "activity `act` claim lease expired without heartbeat on attempt 2"
        );
        // Attempt 0 reports as attempt 1 rather than underflowing.
        assert_eq!(
            timeout_message(&activity_id, 0, ActivityTimeoutAttribution::StartToClose),
            "activity `act` timed out on attempt 1"
        );
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    #[test]
    fn codec_and_compression_strings_round_trip() {
        for codec in [CodecId::MessagePack, CodecId::Json] {
            assert_eq!(codec_from_str(codec_to_str(codec)).unwrap(), codec);
        }
        // The removed protobuf codec must now be rejected rather than silently decoded.
        assert!(codec_from_str("protobuf").is_err());
        assert_eq!(
            compression_from_str(compression_to_str(CompressionId::None)).unwrap(),
            CompressionId::None
        );
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    #[test]
    fn persisted_codec_strings_are_pinned() {
        // These strings live in provider storage (ready_reason and event_type
        // columns, wait kinds, marker kinds, parent close policies). Changing
        // any value breaks reads of existing databases, so every variant is
        // pinned byte-for-byte and round-trips through its parser.
        let reasons = [
            (WorkflowTaskReason::WorkflowStarted, "workflow_started"),
            (WorkflowTaskReason::ActivityCompleted, "activity_completed"),
            (WorkflowTaskReason::ActivityFailed, "activity_failed"),
            (WorkflowTaskReason::ActivityTimedOut, "activity_timed_out"),
            (
                WorkflowTaskReason::ActivityMapCompleted,
                "activity_map_completed",
            ),
            (WorkflowTaskReason::ActivityMapFailed, "activity_map_failed"),
            (
                WorkflowTaskReason::ChildWorkflowStarted,
                "child_workflow_started",
            ),
            (
                WorkflowTaskReason::ChildWorkflowCompleted,
                "child_workflow_completed",
            ),
            (
                WorkflowTaskReason::ChildWorkflowFailed,
                "child_workflow_failed",
            ),
            (
                WorkflowTaskReason::ChildWorkflowCancelled,
                "child_workflow_cancelled",
            ),
            (
                WorkflowTaskReason::ChildWorkflowMapCompleted,
                "child_workflow_map_completed",
            ),
            (
                WorkflowTaskReason::ChildWorkflowMapFailed,
                "child_workflow_map_failed",
            ),
            (WorkflowTaskReason::TimerFired, "timer_fired"),
            (WorkflowTaskReason::SignalReceived, "signal_received"),
            (WorkflowTaskReason::CacheEvicted, "cache_evicted"),
        ];
        for (reason, expected) in reasons {
            assert_eq!(reason_to_str(&reason), expected);
            assert_eq!(reason_from_str(expected).unwrap(), reason);
        }
        assert!(reason_from_str("unknown").is_err());

        let event_types = [
            (HistoryEventType::WorkflowStarted, "workflow_started"),
            (HistoryEventType::WorkflowCompleted, "workflow_completed"),
            (HistoryEventType::WorkflowFailed, "workflow_failed"),
            (HistoryEventType::WorkflowCancelled, "workflow_cancelled"),
            (
                HistoryEventType::WorkflowContinuedAsNew,
                "workflow_continued_as_new",
            ),
            (
                HistoryEventType::WorkflowTaskStarted,
                "workflow_task_started",
            ),
            (HistoryEventType::ActivityScheduled, "activity_scheduled"),
            (
                HistoryEventType::ActivityMapScheduled,
                "activity_map_scheduled",
            ),
            (
                HistoryEventType::ActivityMapCompleted,
                "activity_map_completed",
            ),
            (HistoryEventType::ActivityMapFailed, "activity_map_failed"),
            (HistoryEventType::ActivityCompleted, "activity_completed"),
            (HistoryEventType::ActivityFailed, "activity_failed"),
            (HistoryEventType::ActivityTimedOut, "activity_timed_out"),
            (
                HistoryEventType::ChildWorkflowStartRequested,
                "child_workflow_start_requested",
            ),
            (
                HistoryEventType::ChildWorkflowStarted,
                "child_workflow_started",
            ),
            (
                HistoryEventType::ChildWorkflowCompleted,
                "child_workflow_completed",
            ),
            (
                HistoryEventType::ChildWorkflowFailed,
                "child_workflow_failed",
            ),
            (
                HistoryEventType::ChildWorkflowCancelled,
                "child_workflow_cancelled",
            ),
            (
                HistoryEventType::ChildWorkflowMapScheduled,
                "child_workflow_map_scheduled",
            ),
            (
                HistoryEventType::ChildWorkflowMapCompleted,
                "child_workflow_map_completed",
            ),
            (
                HistoryEventType::ChildWorkflowMapFailed,
                "child_workflow_map_failed",
            ),
            (HistoryEventType::TimerStarted, "timer_started"),
            (HistoryEventType::TimerFired, "timer_fired"),
            (HistoryEventType::SignalConsumed, "signal_consumed"),
            (HistoryEventType::SelectWinner, "select_winner"),
            (HistoryEventType::VersionMarker, "version_marker"),
            (
                HistoryEventType::DeprecatedPatchMarker,
                "deprecated_patch_marker",
            ),
            (HistoryEventType::SideEffectMarker, "side_effect_marker"),
        ];
        for (event_type, expected) in event_types {
            assert_eq!(event_type_to_str(&event_type), expected);
            assert_eq!(event_type_from_str(expected).unwrap(), event_type);
        }
        assert!(event_type_from_str("unknown").is_err());

        assert_eq!(wait_kind_to_str(&WaitKind::Timer), "timer");
        assert_eq!(wait_kind_to_str(&WaitKind::Signal), "signal");
        assert_eq!(
            marker_kind_to_str(WorkflowChangeMarkerKind::Version),
            "version"
        );
        assert_eq!(
            marker_kind_to_str(WorkflowChangeMarkerKind::DeprecatedPatch),
            "deprecated_patch"
        );
        assert_eq!(
            marker_kind_from_str("version").unwrap(),
            WorkflowChangeMarkerKind::Version
        );
        assert_eq!(
            marker_kind_from_str("deprecated_patch").unwrap(),
            WorkflowChangeMarkerKind::DeprecatedPatch
        );
        assert!(marker_kind_from_str("unknown").is_err());
        assert_eq!(
            parent_close_policy_to_str(ParentClosePolicy::Cancel),
            "cancel"
        );
        assert_eq!(
            parent_close_policy_to_str(ParentClosePolicy::Abandon),
            "abandon"
        );
    }

    #[test]
    fn every_commit_mutation_kind_is_workflow_visible() {
        // The terminal guard rejects the union of mutation kinds. Each
        // catalog entry carries exactly one kind and must trip the predicate
        // so no provider can drift back to a narrower guard; the empty commit
        // stays an acceptable no-op.
        assert!(!commit_has_workflow_visible_mutations(
            &WorkflowTaskCommit::default()
        ));
        let commits =
            commit_test_support::mutating_commits(&crate::RunId::new("run"), crate::EventId::ZERO);
        assert_eq!(commits.len(), 10, "one catalog entry per mutation kind");
        for (kind, commit) in commits {
            assert!(
                commit_has_workflow_visible_mutations(&commit),
                "commit with only `{kind}` must count as workflow-visible"
            );
        }
    }

    #[test]
    fn post_commit_ready_reason_orders_terminal_child_and_signal() {
        // Terminal always wins (never ready), a same-commit child event keeps
        // its specific reason, and a consumable signal fills the gap so a
        // delivery racing the claim window cannot be lost.
        assert_eq!(
            post_commit_ready_reason(true, Some(WorkflowTaskReason::ChildWorkflowStarted), true),
            None
        );
        assert_eq!(
            post_commit_ready_reason(false, Some(WorkflowTaskReason::ChildWorkflowStarted), true),
            Some(WorkflowTaskReason::ChildWorkflowStarted)
        );
        assert_eq!(
            post_commit_ready_reason(false, None, true),
            Some(WorkflowTaskReason::SignalReceived)
        );
        assert_eq!(post_commit_ready_reason(false, None, false), None);
    }

    #[test]
    fn retry_visible_at_doubles_per_failed_attempt_and_saturates() {
        let now = TimestampMs(10_000);
        // No backoff: the retry is immediately claimable.
        assert_eq!(
            retry_visible_at_ms(&crate::RetryPolicy::none(), 1, now),
            None
        );
        let exponential = crate::RetryPolicy::exponential();
        // base * 2^(failed_attempt - 1): 1s, 2s, 4s, ...
        assert_eq!(
            retry_visible_at_ms(&exponential, 1, now),
            Some(10_000 + RETRY_BACKOFF_BASE_MS)
        );
        assert_eq!(
            retry_visible_at_ms(&exponential, 2, now),
            Some(10_000 + 2 * RETRY_BACKOFF_BASE_MS)
        );
        assert_eq!(
            retry_visible_at_ms(&exponential, 3, now),
            Some(10_000 + 4 * RETRY_BACKOFF_BASE_MS)
        );
        // Attempt 0 is treated as attempt 1 rather than underflowing.
        assert_eq!(
            retry_visible_at_ms(&exponential, 0, now),
            Some(10_000 + RETRY_BACKOFF_BASE_MS)
        );
        // Huge attempt counts and a now near the epoch ceiling saturate
        // instead of wrapping into the past.
        assert_eq!(
            retry_visible_at_ms(&exponential, u32::MAX, now),
            Some(i64::MAX)
        );
        assert_eq!(
            retry_visible_at_ms(&exponential, 1, TimestampMs(i64::MAX)),
            Some(i64::MAX)
        );
    }

    #[test]
    fn activity_failure_decision_honors_policy_and_non_retryable() {
        // Attempt below the policy budget retries with the incremented
        // attempt; non-retryable failures and exhausted budgets fail.
        let retryable = test_activity_task(1, 3);
        assert!(matches!(
            activity_failure_decision(&retryable, false),
            ActivityFailureDecision::Retry { next_attempt: 2 }
        ));
        assert!(matches!(
            activity_failure_decision(&retryable, true),
            ActivityFailureDecision::Fail
        ));
        let exhausted = test_activity_task(3, 3);
        assert!(matches!(
            activity_failure_decision(&exhausted, false),
            ActivityFailureDecision::Fail
        ));
        assert!(matches!(
            activity_timeout_decision(&retryable),
            ActivityFailureDecision::Retry { next_attempt: 2 }
        ));
        assert!(matches!(
            activity_timeout_decision(&exhausted),
            ActivityFailureDecision::Fail
        ));
    }

    #[test]
    fn child_terminal_mappings_cover_exactly_the_terminal_events() {
        let command_id = test_command_id();
        let completed = HistoryEventData::WorkflowCompleted {
            result: crate::encode_payload(&7_u64).unwrap(),
        };
        let (event, reason) =
            child_terminal_event_data_and_reason(command_id.clone(), &completed).unwrap();
        assert!(matches!(
            event,
            HistoryEventData::ChildWorkflowCompleted(ref data) if data.command_id == command_id
        ));
        assert_eq!(reason, WorkflowTaskReason::ChildWorkflowCompleted);
        assert!(matches!(
            child_terminal_map_item_outcome(&completed),
            Some(ChildWorkflowMapItemOutcome::Succeeded { .. })
        ));

        let failed = HistoryEventData::WorkflowFailed {
            failure: crate::DurableFailure::non_retryable("kind", "boom"),
        };
        let (event, reason) =
            child_terminal_event_data_and_reason(command_id.clone(), &failed).unwrap();
        assert!(matches!(event, HistoryEventData::ChildWorkflowFailed(_)));
        assert_eq!(reason, WorkflowTaskReason::ChildWorkflowFailed);
        assert!(matches!(
            child_terminal_map_item_outcome(&failed),
            Some(ChildWorkflowMapItemOutcome::Failed { .. })
        ));

        let cancelled = HistoryEventData::WorkflowCancelled {
            reason: "stop".to_owned(),
        };
        let (event, reason) =
            child_terminal_event_data_and_reason(command_id.clone(), &cancelled).unwrap();
        assert!(matches!(event, HistoryEventData::ChildWorkflowCancelled(_)));
        assert_eq!(reason, WorkflowTaskReason::ChildWorkflowCancelled);
        assert!(matches!(
            child_terminal_map_item_outcome(&cancelled),
            Some(ChildWorkflowMapItemOutcome::Cancelled { .. })
        ));

        // Continue-as-new closes the run but is not a parent-visible child
        // terminal fact; the mapping must skip it.
        let continued = HistoryEventData::WorkflowContinuedAsNew {
            input: crate::encode_payload(&0_u64).unwrap(),
        };
        assert!(child_terminal_event_data_and_reason(command_id, &continued).is_none());
        assert!(child_terminal_map_item_outcome(&continued).is_none());
    }

    fn test_command_id() -> CommandId {
        CommandId {
            run_id: crate::RunId::new("run"),
            seq: crate::CommandSeq(1),
        }
    }

    fn test_activity_task(attempt: u32, max_attempts: u32) -> ActivityTask {
        ActivityTask {
            activity_id: ActivityId::new(&test_command_id()),
            run_id: crate::RunId::new("run"),
            command_id: test_command_id(),
            activity_name: crate::ActivityName::new("activity"),
            task_queue: crate::TaskQueue::new("queue"),
            input: crate::encode_payload(&0_u64).unwrap(),
            attempt,
            retry_policy: crate::RetryPolicy {
                max_attempts,
                ..crate::RetryPolicy::default()
            },
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            map_item: None,
        }
    }
}
