//! The pure fanout state machine shared by the activity-map and
//! child-workflow-map paths of every provider.
//!
//! `0015` Phase 3 unified the single-decision helpers into
//! [`crate::provider_util`]; this module is the other half: the whole map
//! machine — item materialization, `max_in_flight` accounting, per-item retry,
//! failure-mode policy, result-manifest assembly, and parent notification —
//! expressed once as `(descriptor state, event) -> ordered effects`.
//!
//! Invariants this module owns:
//!
//! - **Pure.** No I/O, no async, no clock reads, no randomness. Every input is
//!   an argument, so the in-memory and SQLite providers can apply the result
//!   synchronously and Postgres can apply it inside an async transaction
//!   without an async-trait reshape.
//! - **Effects are plain data and are applied in order.** Applying a prefix and
//!   crashing is safe: every effect is idempotent against the state that
//!   produced it, and the provider's transaction makes the suffix atomic. A
//!   transition that must not be applied at all returns [`MapReject`] instead
//!   of an effect list, so no provider has to scan for pseudo-effects before
//!   applying the first real one.
//! - **Effects are batch-shaped, never per-item.** Materialization returns one
//!   contiguous `[first_ordinal, first_ordinal + count)` range, so Postgres can
//!   issue one set-based `insert ... select ... from unnest(...)` per batch
//!   instead of `count` round trips.
//! - **One slot per admitted ordinal.** Materialization takes a slot for every
//!   ordinal it admits; exactly one terminal outcome per ordinal releases it. A
//!   retry keeps the slot (the item is still in flight); a duplicate terminal
//!   outcome releases nothing.
//! - **Terminal is absorbing.** Once the descriptor is terminal no event
//!   produces effects, so a map can never go completed -> failed or
//!   failed -> completed under any interleaving.
//!
//! Known defect this module deliberately preserves: a map scheduled with an
//! empty input manifest (`item_count == 0`) materializes nothing and never
//! appends a terminal fact, so the parent blocks on `result_manifest()`
//! forever. Every provider behaves that way today and Phase 6 is explicitly
//! barred from changing map semantics, so the stall is reproduced here rather
//! than fixed; the fix lands as its own plan row with per-provider regression
//! tests. `empty_manifest_materializes_nothing_and_never_completes` pins the
//! preserved behaviour so the eventual fix is a visible diff.
//!
//! Row 6B of `impl-plan/0017-architecture-hot-path-remediation.md` replaces the
//! six hand-written copies with calls into this module; until then the crate's
//! only callers are the tests below.
#![allow(dead_code)]

use crate::provider_util::{ActivityFailureDecision, duration_millis_i64, retry_visible_at_ms};
use crate::{
    ChildWorkflowMapFailureMode, ChildWorkflowMapItemOutcome, CommandId, DurableFailure,
    RetryPolicy, TimestampMs,
};
use std::time::Duration;

/// Which map machine a descriptor drives. The two differ in exactly two
/// places, both pinned by tests: an activity map has no per-item outcome row
/// (its only durable per-item fact is the item's own activity task) and it
/// rejects a terminal-parent notification with [`MapReject::TerminalParent`]
/// where a child map drops the notification silently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MapKind {
    Activity,
    ChildWorkflow,
}

impl MapKind {
    /// Activity maps keep per-item results only in the result table; child
    /// maps keep a per-item outcome row that also serves as the dedup record.
    fn records_item_outcomes(self) -> bool {
        matches!(self, Self::ChildWorkflow)
    }

    /// Child maps may cancel already-running children; an activity map has
    /// none.
    fn cancels_children(self) -> bool {
        matches!(self, Self::ChildWorkflow)
    }
}

/// The descriptor fields the engine reads. Providers project their row (or
/// their in-memory record) into this before every transition; nothing else
/// about the descriptor is engine-visible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MapState {
    pub map_command_id: CommandId,
    pub kind: MapKind,
    /// Meaningful for [`MapKind::ChildWorkflow`]. An activity map is always
    /// fail-fast: the first item that exhausts its retries fails the map.
    pub failure_mode: ChildWorkflowMapFailureMode,
    pub item_count: u64,
    /// Lowest ordinal never yet admitted.
    ///
    /// Invariant the providers must uphold: materialization is strictly
    /// forward-only and every recorded outcome sits at an ordinal *below*
    /// `next_ordinal`, so this is a contiguous admission cursor and also the
    /// admitted-item high-water mark. The engine re-admits from it
    /// unconditionally; a descriptor whose cursor sits at or behind a recorded
    /// outcome would have that ordinal admitted twice.
    pub next_ordinal: u64,
    pub in_flight: u64,
    pub max_in_flight: usize,
    /// Item ordinals with a persisted terminal outcome.
    pub recorded_outcomes: u64,
    pub completed: bool,
}

impl MapState {
    /// `max_in_flight` clamped to at least one slot. A zero bound would admit
    /// nothing and stall the map forever, so it is read as one.
    pub(crate) fn slot_limit(&self) -> u64 {
        u64::try_from(self.max_in_flight.max(1)).unwrap_or(u64::MAX)
    }

    pub(crate) fn ordinal_in_bounds(&self, ordinal: u64) -> bool {
        ordinal < self.item_count
    }

    /// The batch materialization admits: as many contiguous unadmitted
    /// ordinals as free slots allow. `None` when the map is saturated or fully
    /// admitted.
    fn materialization_batch(&self, in_flight: u64) -> Option<(u64, u64)> {
        let free = self.slot_limit().saturating_sub(in_flight);
        let unadmitted = self.item_count.saturating_sub(self.next_ordinal);
        let count = free.min(unadmitted);
        (count > 0).then_some((self.next_ordinal, count))
    }
}

/// Whether a failed attempt was an explicit failure or a lapsed deadline.
/// Explicit failures are paced by the retry backoff; timeouts are already
/// paced by the deadline that fired, so their retry is immediately claimable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ItemAttemptFailureKind {
    Failed,
    TimedOut,
}

/// Retry-versus-exhaustion for one item attempt, mirroring
/// [`ActivityFailureDecision`] with the derives an event needs. The
/// [`From`] impl is the only conversion, so the map machine cannot drift from
/// the shared activity retry policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ItemRetryDecision {
    Retry { next_attempt: u32 },
    Exhausted,
}

impl From<ActivityFailureDecision> for ItemRetryDecision {
    fn from(decision: ActivityFailureDecision) -> Self {
        match decision {
            ActivityFailureDecision::Retry { next_attempt } => Self::Retry { next_attempt },
            ActivityFailureDecision::Fail => Self::Exhausted,
        }
    }
}

/// One input to the machine. Every field is a fact the provider has already
/// read inside its transaction, so the transition itself needs no further
/// reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MapEvent {
    /// The scheduling commit inserted the descriptor.
    DescriptorCreated { parent_terminal: bool },
    /// An item reached a terminal outcome: an activity item's result, or a
    /// child item's terminal workflow event mapped through
    /// [`crate::provider_util::child_terminal_map_item_outcome`].
    ItemCompleted {
        ordinal: u64,
        outcome: ChildWorkflowMapItemOutcome,
        /// This ordinal already has a persisted outcome — a duplicate
        /// delivery, which must record nothing and release no slot.
        already_recorded: bool,
        parent_terminal: bool,
    },
    /// An activity-map item's attempt failed or timed out. `decision` is the
    /// shared [`crate::provider_util::activity_failure_decision`] /
    /// [`crate::provider_util::activity_timeout_decision`] verdict.
    ItemAttemptFailed {
        ordinal: u64,
        failure: DurableFailure,
        kind: ItemAttemptFailureKind,
        decision: ItemRetryDecision,
        /// The 1-based attempt that just failed, used for the backoff.
        failed_attempt: u32,
        retry_policy: RetryPolicy,
        /// The item's start-to-close timeout, restarted on every retry.
        start_to_close_timeout: Option<Duration>,
        now: TimestampMs,
        already_recorded: bool,
        parent_terminal: bool,
    },
    /// The parent cancelled this map command (`WorkflowTaskCommit::cancel_commands`).
    ParentCancelled,
}

/// A storage operation for the provider to execute. The list is ordered and
/// total: applying it is the whole of the provider's map work for that event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MapEffect {
    /// Insert the item's terminal outcome row, keyed by
    /// `(map_command_id, ordinal)`. Emitted only for [`MapKind::ChildWorkflow`].
    RecordItemOutcome {
        ordinal: u64,
        outcome: ChildWorkflowMapItemOutcome,
    },
    /// Materialize `[first_ordinal, first_ordinal + count)` as one batch: read
    /// the manifest pages covering the range and issue one set-based insert of
    /// item activity tasks (activity maps) or item child starts (child maps).
    MaterializeItems { first_ordinal: u64, count: u64 },
    /// Persist the descriptor's admission cursor after a materialization
    /// batch. Always immediately follows [`MapEffect::MaterializeItems`], and
    /// is the only effect that writes `in_flight` on a non-terminal path.
    AdvanceDescriptor { next_ordinal: u64, in_flight: u64 },
    /// Requeue the item as `next_attempt`: release its claim, clear both
    /// heartbeat fields, and restamp its deadlines. The item keeps its
    /// `max_in_flight` slot, because it is still in flight.
    ScheduleItemRetry {
        ordinal: u64,
        next_attempt: u32,
        /// Not claimable before this instant; `None` means immediately
        /// claimable.
        visible_at_ms: Option<i64>,
        /// Restarted start-to-close deadline, measured from the visibility
        /// instant so the timeout scanner cannot fire on a task that was never
        /// claimable. `None` means the item has no start-to-close timeout.
        timeout_at_ms: Option<i64>,
    },
    /// Assemble the result manifest from the item outcomes in ascending
    /// ordinal order over the input manifest's page boundaries, append the
    /// terminal success fact to the parent, and wake it.
    CompleteMap { item_count: u64 },
    /// Append the terminal failure fact to the parent and wake it.
    FailMap { failure: DurableFailure },
    /// Tombstone every not-yet-terminal item task and every undispatched item
    /// start of this map so neither the claim path nor the timeout scanner can
    /// resurrect an item of a map that is over.
    AbandonPendingItems,
    /// Cancel every already-running, not-yet-terminal child of this map with
    /// `reason`. Emitted only for [`MapKind::ChildWorkflow`].
    CancelChildren { reason: String },
    /// Flip the descriptor to terminal: `completed = true, in_flight = 0`.
    MarkDescriptorTerminal,
}

/// A transition that must not be applied at all. The provider raises the
/// mapped error and rolls its transaction back; no effect from the same event
/// is applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MapReject {
    /// The item ordinal is outside the input manifest. Maps to
    /// `Error::Backend`.
    OutOfBounds { ordinal: u64 },
    /// The map reached a terminal state but the parent run is already closed,
    /// so its terminal fact has nowhere to go. Maps to
    /// `Error::TerminalWorkflow`.
    TerminalParent,
}

/// Success/failure/cancellation tallies for a terminal map, derived from the
/// assembled outcome list. Providers call [`outcome_counts`] instead of
/// re-deriving the three filters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MapOutcomeCounts {
    pub success_count: usize,
    pub failure_count: usize,
    pub cancellation_count: usize,
}

/// Tallies for a child map's terminal event, over the ordered outcome list the
/// provider assembles for the result manifest.
pub(crate) fn outcome_counts(outcomes: &[ChildWorkflowMapItemOutcome]) -> MapOutcomeCounts {
    let mut counts = MapOutcomeCounts::default();
    for outcome in outcomes {
        match outcome {
            ChildWorkflowMapItemOutcome::Succeeded { .. } => counts.success_count += 1,
            ChildWorkflowMapItemOutcome::Failed { .. } => counts.failure_count += 1,
            ChildWorkflowMapItemOutcome::Cancelled { .. } => counts.cancellation_count += 1,
        }
    }
    counts
}

/// Tallies for an activity map's terminal event. An activity map completes
/// only when every item succeeded, so the counts are structural.
pub(crate) fn activity_outcome_counts(item_count: usize) -> MapOutcomeCounts {
    MapOutcomeCounts {
        success_count: item_count,
        failure_count: 0,
        cancellation_count: 0,
    }
}

/// Parent-visible failure for a child map item that did not succeed. A
/// cancellation is reported as a non-retryable failure naming the ordinal, so
/// the parent's `ChildWorkflowMapFailed` says which item stopped the map.
pub(crate) fn fail_fast_failure(
    ordinal: u64,
    outcome: &ChildWorkflowMapItemOutcome,
) -> Option<DurableFailure> {
    match outcome {
        ChildWorkflowMapItemOutcome::Succeeded { .. } => None,
        ChildWorkflowMapItemOutcome::Failed { failure } => Some(failure.clone()),
        ChildWorkflowMapItemOutcome::Cancelled { reason } => Some(DurableFailure::non_retryable(
            "durust.child_workflow_cancelled",
            format!("child workflow map item {ordinal} was cancelled: {reason}"),
        )),
    }
}

/// Reason stamped on the `WorkflowCancelled` event of every child cancelled
/// because its map failed fast.
pub(crate) fn child_cancellation_reason(map_command_id: &CommandId) -> String {
    format!(
        "child workflow map `{}`:{} failed",
        map_command_id.run_id, map_command_id.seq.0
    )
}

/// The whole machine: descriptor state plus one event in, ordered effects out.
pub(crate) fn step(state: &MapState, event: MapEvent) -> Result<Vec<MapEffect>, MapReject> {
    // Terminal is absorbing. Every later event — a duplicate completion, a
    // late failure, a cancellation, a completion racing the terminal commit —
    // is a no-op, so no interleaving can move a map between terminal states.
    if state.completed {
        return Ok(Vec::new());
    }

    match event {
        // An empty manifest materializes nothing and appends no terminal fact.
        // That is a known permanent stall shared by all three providers; see
        // the module docs for why Phase 6 preserves it rather than fixing it.
        MapEvent::DescriptorCreated { .. } => Ok(materialize(state, state.in_flight)),

        MapEvent::ItemCompleted {
            ordinal,
            outcome,
            already_recorded,
            parent_terminal,
        } => item_terminal(state, ordinal, outcome, already_recorded, parent_terminal),

        MapEvent::ItemAttemptFailed {
            ordinal,
            failure,
            kind,
            decision,
            failed_attempt,
            retry_policy,
            start_to_close_timeout,
            now,
            already_recorded,
            parent_terminal,
        } => {
            if !state.ordinal_in_bounds(ordinal) {
                return Err(MapReject::OutOfBounds { ordinal });
            }
            match decision {
                ItemRetryDecision::Retry { next_attempt } => {
                    // A retry keeps the item's slot: it is still in flight, so
                    // no other ordinal may be admitted in its place.
                    let visible_at_ms = match kind {
                        ItemAttemptFailureKind::Failed => {
                            retry_visible_at_ms(&retry_policy, failed_attempt, now)
                        }
                        // The lapsed deadline already paced this attempt;
                        // delaying crash recovery further only adds latency.
                        ItemAttemptFailureKind::TimedOut => None,
                    };
                    let visible_from = visible_at_ms.unwrap_or(now.0);
                    Ok(vec![MapEffect::ScheduleItemRetry {
                        ordinal,
                        next_attempt,
                        visible_at_ms,
                        timeout_at_ms: retry_timeout_at_ms(visible_from, start_to_close_timeout),
                    }])
                }
                ItemRetryDecision::Exhausted => item_terminal(
                    state,
                    ordinal,
                    ChildWorkflowMapItemOutcome::Failed { failure },
                    already_recorded,
                    parent_terminal,
                ),
            }
        }

        // Cancelling the command tombstones the map's pending work and closes
        // the descriptor. No parent fact is appended: the cancellation is
        // already recorded by the commit that carried it.
        MapEvent::ParentCancelled => Ok(vec![
            MapEffect::AbandonPendingItems,
            MapEffect::MarkDescriptorTerminal,
        ]),
    }
}

/// Restarted start-to-close deadline for a retried item. Mirrors
/// `provider_util::activity_timeout_at_ms_from`, which is feature-gated to the
/// SQL providers and so cannot be called from here unconditionally;
/// `retry_timeout_matches_the_shared_activity_helper` pins the two together.
fn retry_timeout_at_ms(
    visible_from_ms: i64,
    start_to_close_timeout: Option<Duration>,
) -> Option<i64> {
    start_to_close_timeout
        .map(|timeout| visible_from_ms.saturating_add(duration_millis_i64(timeout)))
}

fn item_terminal(
    state: &MapState,
    ordinal: u64,
    outcome: ChildWorkflowMapItemOutcome,
    already_recorded: bool,
    parent_terminal: bool,
) -> Result<Vec<MapEffect>, MapReject> {
    if !state.ordinal_in_bounds(ordinal) {
        return Err(MapReject::OutOfBounds { ordinal });
    }
    // A duplicate terminal outcome for an ordinal that already has one records
    // nothing, releases no slot, and admits nothing. Releasing twice would
    // over-admit the map past `max_in_flight`.
    if already_recorded {
        return Ok(Vec::new());
    }

    let succeeded = matches!(outcome, ChildWorkflowMapItemOutcome::Succeeded { .. });
    let mut effects = Vec::new();
    if state.kind.records_item_outcomes() {
        effects.push(MapEffect::RecordItemOutcome {
            ordinal,
            outcome: outcome.clone(),
        });
    }
    let in_flight_after = state.in_flight.saturating_sub(1);
    let recorded_after = state.recorded_outcomes.saturating_add(1);

    let fail_fast = !succeeded
        && (state.kind == MapKind::Activity
            || state.failure_mode == ChildWorkflowMapFailureMode::FailFast);
    if fail_fast {
        let failure = fail_fast_failure(ordinal, &outcome)
            .expect("a non-succeeded outcome always yields a parent-visible failure");
        return terminal_failure(state, parent_terminal, effects, failure);
    }

    if recorded_after >= state.item_count {
        return terminal_success(state, parent_terminal, effects);
    }

    // Not terminal yet: the released slot may admit the next ordinals.
    effects.extend(materialize(state, in_flight_after));
    Ok(effects)
}

/// Effects that admit as many unadmitted ordinals as free slots allow, as one
/// contiguous batch.
fn materialize(state: &MapState, in_flight: u64) -> Vec<MapEffect> {
    let Some((first_ordinal, count)) = state.materialization_batch(in_flight) else {
        return Vec::new();
    };
    vec![
        MapEffect::MaterializeItems {
            first_ordinal,
            count,
        },
        MapEffect::AdvanceDescriptor {
            next_ordinal: first_ordinal.saturating_add(count),
            in_flight: in_flight.saturating_add(count),
        },
    ]
}

fn terminal_success(
    state: &MapState,
    parent_terminal: bool,
    mut effects: Vec<MapEffect>,
) -> Result<Vec<MapEffect>, MapReject> {
    if parent_terminal {
        return terminal_parent(state, effects);
    }
    effects.push(MapEffect::CompleteMap {
        item_count: state.item_count,
    });
    effects.push(MapEffect::MarkDescriptorTerminal);
    Ok(effects)
}

fn terminal_failure(
    state: &MapState,
    parent_terminal: bool,
    mut effects: Vec<MapEffect>,
    failure: DurableFailure,
) -> Result<Vec<MapEffect>, MapReject> {
    if parent_terminal {
        return terminal_parent(state, effects);
    }
    effects.push(MapEffect::FailMap { failure });
    effects.push(MapEffect::AbandonPendingItems);
    if state.kind.cancels_children() {
        effects.push(MapEffect::CancelChildren {
            reason: child_cancellation_reason(&state.map_command_id),
        });
    }
    effects.push(MapEffect::MarkDescriptorTerminal);
    Ok(effects)
}

/// The map is terminal but the parent run is closed, so its terminal fact has
/// nowhere to go. An activity map rejects the completion so the caller returns
/// `Error::TerminalWorkflow` and the transaction rolls back; a child map drops
/// the notification and keeps the recorded outcome, leaving the descriptor for
/// the parent's terminal cleanup to delete.
fn terminal_parent(state: &MapState, effects: Vec<MapEffect>) -> Result<Vec<MapEffect>, MapReject> {
    match state.kind {
        MapKind::Activity => Err(MapReject::TerminalParent),
        MapKind::ChildWorkflow => Ok(effects),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider_util::{RETRY_BACKOFF_BASE_MS, activity_failure_decision};
    use crate::{CommandSeq, RetryBackoff, RunId};

    fn map_command_id() -> CommandId {
        CommandId {
            run_id: RunId::new("run-1"),
            seq: CommandSeq(7),
        }
    }

    fn activity_map(item_count: u64, max_in_flight: usize) -> MapState {
        MapState {
            map_command_id: map_command_id(),
            kind: MapKind::Activity,
            failure_mode: ChildWorkflowMapFailureMode::FailFast,
            item_count,
            next_ordinal: 0,
            in_flight: 0,
            max_in_flight,
            recorded_outcomes: 0,
            completed: false,
        }
    }

    fn child_map(
        item_count: u64,
        max_in_flight: usize,
        failure_mode: ChildWorkflowMapFailureMode,
    ) -> MapState {
        MapState {
            kind: MapKind::ChildWorkflow,
            failure_mode,
            ..activity_map(item_count, max_in_flight)
        }
    }

    fn success() -> ChildWorkflowMapItemOutcome {
        ChildWorkflowMapItemOutcome::Succeeded {
            result: crate::encode_payload(&1_u64).unwrap(),
        }
    }

    fn item_failure() -> ChildWorkflowMapItemOutcome {
        ChildWorkflowMapItemOutcome::Failed {
            failure: DurableFailure::non_retryable("kind", "boom"),
        }
    }

    fn item_cancelled() -> ChildWorkflowMapItemOutcome {
        ChildWorkflowMapItemOutcome::Cancelled {
            reason: "stop".to_owned(),
        }
    }

    fn completed(ordinal: u64, outcome: ChildWorkflowMapItemOutcome) -> MapEvent {
        MapEvent::ItemCompleted {
            ordinal,
            outcome,
            already_recorded: false,
            parent_terminal: false,
        }
    }

    /// Builder for the wide `ItemAttemptFailed` event so each case names only
    /// the field it varies (enums have no functional-update syntax).
    struct AttemptFailure {
        ordinal: u64,
        kind: ItemAttemptFailureKind,
        decision: ItemRetryDecision,
        failed_attempt: u32,
        retry_policy: RetryPolicy,
        start_to_close_timeout: Option<Duration>,
        now: TimestampMs,
        already_recorded: bool,
        parent_terminal: bool,
    }

    impl AttemptFailure {
        fn new(ordinal: u64, decision: ItemRetryDecision) -> Self {
            Self {
                ordinal,
                kind: ItemAttemptFailureKind::Failed,
                decision,
                failed_attempt: 1,
                retry_policy: RetryPolicy::exponential(),
                start_to_close_timeout: None,
                now: TimestampMs(10_000),
                already_recorded: false,
                parent_terminal: false,
            }
        }

        fn event(self) -> MapEvent {
            MapEvent::ItemAttemptFailed {
                ordinal: self.ordinal,
                failure: DurableFailure::non_retryable("kind", "boom"),
                kind: self.kind,
                decision: self.decision,
                failed_attempt: self.failed_attempt,
                retry_policy: self.retry_policy,
                start_to_close_timeout: self.start_to_close_timeout,
                now: self.now,
                already_recorded: self.already_recorded,
                parent_terminal: self.parent_terminal,
            }
        }
    }

    fn attempt_failed(ordinal: u64, decision: ItemRetryDecision) -> MapEvent {
        AttemptFailure::new(ordinal, decision).event()
    }

    /// `max_in_flight` admission, including the clamp that keeps a zero bound
    /// from stalling the map and the exactly-at/one-over boundaries.
    #[test]
    fn materialization_admits_one_batch_bounded_by_free_slots() {
        struct Case {
            name: &'static str,
            item_count: u64,
            max_in_flight: usize,
            next_ordinal: u64,
            in_flight: u64,
            expected: Option<(u64, u64)>,
        }
        let cases = [
            Case {
                name: "zero bound is read as one slot rather than stalling",
                item_count: 3,
                max_in_flight: 0,
                next_ordinal: 0,
                in_flight: 0,
                expected: Some((0, 1)),
            },
            Case {
                name: "bound of one admits exactly one item",
                item_count: 3,
                max_in_flight: 1,
                next_ordinal: 0,
                in_flight: 0,
                expected: Some((0, 1)),
            },
            Case {
                name: "free slots cap the batch, not the item count",
                item_count: 10,
                max_in_flight: 4,
                next_ordinal: 0,
                in_flight: 0,
                expected: Some((0, 4)),
            },
            Case {
                name: "item count caps the batch, not the slots",
                item_count: 2,
                max_in_flight: 10,
                next_ordinal: 0,
                in_flight: 0,
                expected: Some((0, 2)),
            },
            Case {
                name: "exactly at the bound admits nothing",
                item_count: 10,
                max_in_flight: 4,
                next_ordinal: 4,
                in_flight: 4,
                expected: None,
            },
            Case {
                name: "one below the bound admits exactly one",
                item_count: 10,
                max_in_flight: 4,
                next_ordinal: 4,
                in_flight: 3,
                expected: Some((4, 1)),
            },
            Case {
                name: "one over the bound admits nothing and never underflows",
                item_count: 10,
                max_in_flight: 4,
                next_ordinal: 5,
                in_flight: 5,
                expected: None,
            },
            Case {
                name: "fully admitted map admits nothing even with free slots",
                item_count: 3,
                max_in_flight: 8,
                next_ordinal: 3,
                in_flight: 0,
                expected: None,
            },
        ];

        for case in cases {
            let state = MapState {
                next_ordinal: case.next_ordinal,
                in_flight: case.in_flight,
                ..activity_map(case.item_count, case.max_in_flight)
            };
            let effects = step(
                &state,
                MapEvent::DescriptorCreated {
                    parent_terminal: false,
                },
            )
            .unwrap();
            match case.expected {
                None => assert!(effects.is_empty(), "{}: expected no effects", case.name),
                Some((first_ordinal, count)) => assert_eq!(
                    effects,
                    vec![
                        MapEffect::MaterializeItems {
                            first_ordinal,
                            count
                        },
                        MapEffect::AdvanceDescriptor {
                            next_ordinal: first_ordinal + count,
                            in_flight: case.in_flight + count,
                        },
                    ],
                    "{}",
                    case.name
                ),
            }
        }
    }

    /// A batch is one contiguous range plus one cursor write, which is what
    /// lets Postgres issue one set-based statement per effect group instead of
    /// `count` per-item round trips.
    #[test]
    fn materialization_is_one_range_effect_per_batch() {
        let state = activity_map(10_000, 10_000);
        let effects = step(
            &state,
            MapEvent::DescriptorCreated {
                parent_terminal: false,
            },
        )
        .unwrap();
        assert_eq!(effects.len(), 2, "a 10k-item batch is still two effects");
        assert_eq!(
            effects[0],
            MapEffect::MaterializeItems {
                first_ordinal: 0,
                count: 10_000
            }
        );
    }

    /// An empty input manifest materializes nothing and appends no terminal
    /// fact, so the parent blocks forever. That is a real defect, shared by
    /// all three providers, that Phase 6 is barred from fixing (it would be a
    /// map-semantics change under a "conformance passes unchanged" gate, and
    /// it would drag a second change with it: an empty map scheduled by a
    /// commit that also closes the run would start rejecting that whole
    /// commit). The stall is pinned here so the eventual fix is a visible
    /// diff, not a silent side effect of this extraction.
    #[test]
    fn empty_manifest_materializes_nothing_and_never_completes() {
        for state in [
            activity_map(0, 4),
            child_map(0, 4, ChildWorkflowMapFailureMode::CollectAll),
        ] {
            for parent_terminal in [false, true] {
                assert_eq!(
                    step(&state, MapEvent::DescriptorCreated { parent_terminal }),
                    Ok(Vec::new()),
                    "an empty manifest must produce no effects at all",
                );
            }
        }
    }

    /// A non-final item completion releases exactly one slot and spends it on
    /// the next unadmitted ordinal.
    #[test]
    fn item_completion_releases_one_slot_and_admits_one_item() {
        let state = MapState {
            next_ordinal: 2,
            in_flight: 2,
            ..activity_map(5, 2)
        };
        assert_eq!(
            step(&state, completed(0, success())),
            Ok(vec![
                MapEffect::MaterializeItems {
                    first_ordinal: 2,
                    count: 1
                },
                MapEffect::AdvanceDescriptor {
                    next_ordinal: 3,
                    in_flight: 2
                },
            ]),
        );
    }

    /// A child map records the item outcome row before releasing the slot; an
    /// activity map has no per-item outcome row.
    #[test]
    fn only_child_maps_record_per_item_outcomes() {
        let child = MapState {
            next_ordinal: 2,
            in_flight: 2,
            ..child_map(5, 2, ChildWorkflowMapFailureMode::CollectAll)
        };
        let effects = step(&child, completed(0, success())).unwrap();
        assert_eq!(
            effects[0],
            MapEffect::RecordItemOutcome {
                ordinal: 0,
                outcome: success()
            }
        );
        let activity = MapState {
            next_ordinal: 2,
            in_flight: 2,
            ..activity_map(5, 2)
        };
        let effects = step(&activity, completed(0, success())).unwrap();
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, MapEffect::RecordItemOutcome { .. })),
        );
    }

    /// The last item completing assembles the manifest and notifies the
    /// parent; it never also materializes.
    #[test]
    fn last_item_completes_the_map() {
        let state = MapState {
            next_ordinal: 3,
            in_flight: 1,
            recorded_outcomes: 2,
            ..activity_map(3, 2)
        };
        assert_eq!(
            step(&state, completed(2, success())),
            Ok(vec![
                MapEffect::CompleteMap { item_count: 3 },
                MapEffect::MarkDescriptorTerminal,
            ]),
        );
    }

    /// Retry exhaustion: a retryable attempt keeps its slot and reschedules;
    /// the exhausting attempt fails the whole activity map.
    #[test]
    fn activity_item_retries_until_exhausted_then_fails_the_map() {
        let state = MapState {
            next_ordinal: 2,
            in_flight: 2,
            ..activity_map(5, 2)
        };
        assert_eq!(
            step(
                &state,
                attempt_failed(0, ItemRetryDecision::Retry { next_attempt: 2 })
            ),
            Ok(vec![MapEffect::ScheduleItemRetry {
                ordinal: 0,
                next_attempt: 2,
                visible_at_ms: Some(10_000 + RETRY_BACKOFF_BASE_MS),
                timeout_at_ms: None,
            }]),
        );
        assert_eq!(
            step(&state, attempt_failed(0, ItemRetryDecision::Exhausted)),
            Ok(vec![
                MapEffect::FailMap {
                    failure: DurableFailure::non_retryable("kind", "boom")
                },
                MapEffect::AbandonPendingItems,
                MapEffect::MarkDescriptorTerminal,
            ]),
        );
    }

    /// A retry never releases the slot: nothing else may be admitted while the
    /// item is still in flight.
    #[test]
    fn retry_does_not_release_the_in_flight_slot() {
        let state = MapState {
            next_ordinal: 2,
            in_flight: 2,
            ..activity_map(5, 2)
        };
        let effects = step(
            &state,
            attempt_failed(0, ItemRetryDecision::Retry { next_attempt: 2 }),
        )
        .unwrap();
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            MapEffect::MaterializeItems { .. } | MapEffect::AdvanceDescriptor { .. }
        )));
    }

    /// Explicit failures are paced by the exponential backoff; timeout retries
    /// are already paced by the deadline that fired, so they are immediately
    /// claimable.
    #[test]
    fn timeout_retries_carry_no_backoff() {
        let state = MapState {
            in_flight: 1,
            next_ordinal: 1,
            ..activity_map(3, 1)
        };
        let timed_out = AttemptFailure {
            kind: ItemAttemptFailureKind::TimedOut,
            ..AttemptFailure::new(0, ItemRetryDecision::Retry { next_attempt: 3 })
        };
        assert_eq!(
            step(&state, timed_out.event()),
            Ok(vec![MapEffect::ScheduleItemRetry {
                ordinal: 0,
                next_attempt: 3,
                visible_at_ms: None,
                timeout_at_ms: None,
            }]),
        );
        // The paced variant of the same attempt doubles per failed attempt.
        let paced = AttemptFailure {
            failed_attempt: 3,
            ..AttemptFailure::new(0, ItemRetryDecision::Retry { next_attempt: 4 })
        };
        assert_eq!(
            step(&state, paced.event()),
            Ok(vec![MapEffect::ScheduleItemRetry {
                ordinal: 0,
                next_attempt: 4,
                visible_at_ms: Some(10_000 + 4 * RETRY_BACKOFF_BASE_MS),
                timeout_at_ms: None,
            }]),
        );
        // A policy without backoff stays immediately claimable.
        let unpaced = AttemptFailure {
            retry_policy: RetryPolicy {
                backoff: RetryBackoff::None,
                ..RetryPolicy::exponential()
            },
            ..AttemptFailure::new(0, ItemRetryDecision::Retry { next_attempt: 2 })
        };
        assert_eq!(
            step(&state, unpaced.event()),
            Ok(vec![MapEffect::ScheduleItemRetry {
                ordinal: 0,
                next_attempt: 2,
                visible_at_ms: None,
                timeout_at_ms: None,
            }]),
        );
    }

    /// A retried item's start-to-close clock restarts at the *visibility*
    /// instant, not at `now`, so the timeout scanner cannot fire on a task
    /// that was never claimable. The providers encode this today
    /// (`memory.rs:1258-1262`, `sqlite.rs:1409-1424`, `postgres.rs:4053-4070`)
    /// and the effect must carry it.
    #[test]
    fn retry_restarts_start_to_close_from_the_visibility_instant() {
        let state = MapState {
            in_flight: 1,
            next_ordinal: 1,
            ..activity_map(3, 1)
        };
        // Paced retry: the deadline is measured from `now + backoff`.
        let paced = AttemptFailure {
            start_to_close_timeout: Some(Duration::from_secs(30)),
            ..AttemptFailure::new(0, ItemRetryDecision::Retry { next_attempt: 2 })
        };
        assert_eq!(
            step(&state, paced.event()),
            Ok(vec![MapEffect::ScheduleItemRetry {
                ordinal: 0,
                next_attempt: 2,
                visible_at_ms: Some(10_000 + RETRY_BACKOFF_BASE_MS),
                timeout_at_ms: Some(10_000 + RETRY_BACKOFF_BASE_MS + 30_000),
            }]),
        );
        // Timeout retry: immediately claimable, so the deadline is measured
        // from `now`.
        let timed_out = AttemptFailure {
            kind: ItemAttemptFailureKind::TimedOut,
            start_to_close_timeout: Some(Duration::from_secs(30)),
            ..AttemptFailure::new(0, ItemRetryDecision::Retry { next_attempt: 2 })
        };
        assert_eq!(
            step(&state, timed_out.event()),
            Ok(vec![MapEffect::ScheduleItemRetry {
                ordinal: 0,
                next_attempt: 2,
                visible_at_ms: None,
                timeout_at_ms: Some(10_000 + 30_000),
            }]),
        );
        // An item with no start-to-close timeout gets no deadline at all.
        assert_eq!(retry_timeout_at_ms(10_000, None), None);
        // Absurd timeouts saturate instead of wrapping into the past.
        assert_eq!(
            retry_timeout_at_ms(i64::MAX, Some(Duration::from_secs(1))),
            Some(i64::MAX)
        );
    }

    /// The restarted deadline is the shared activity helper's arithmetic, not
    /// a second copy. `activity_timeout_at_ms_from` is feature-gated to the
    /// SQL providers, so the engine reimplements the one-line body; this pins
    /// them together wherever the gate is on.
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    #[test]
    fn retry_timeout_matches_the_shared_activity_helper() {
        for visible_from in [0_i64, 10_000, i64::MAX] {
            for timeout in [None, Some(Duration::ZERO), Some(Duration::from_secs(30))] {
                assert_eq!(
                    retry_timeout_at_ms(visible_from, timeout),
                    crate::provider_util::activity_timeout_at_ms_from(
                        TimestampMs(visible_from),
                        timeout
                    ),
                    "visible_from={visible_from} timeout={timeout:?}"
                );
            }
        }
    }

    /// The engine's retry verdict is the shared activity retry policy, not a
    /// second copy of it.
    #[test]
    fn item_retry_decision_mirrors_the_shared_activity_policy() {
        let mut task = crate::provider_util::commit_test_support::activity_task(
            &RunId::new("run-1"),
            &map_command_id(),
        );
        task.retry_policy.max_attempts = 3;
        task.attempt = 1;
        assert_eq!(
            ItemRetryDecision::from(activity_failure_decision(&task, false)),
            ItemRetryDecision::Retry { next_attempt: 2 }
        );
        assert_eq!(
            ItemRetryDecision::from(activity_failure_decision(&task, true)),
            ItemRetryDecision::Exhausted
        );
        task.attempt = 3;
        assert_eq!(
            ItemRetryDecision::from(activity_failure_decision(&task, false)),
            ItemRetryDecision::Exhausted
        );
    }

    /// `FailFast` stops the map on the first non-success and cancels the
    /// running children; `CollectAll` records the outcome and keeps going.
    #[test]
    fn child_map_failure_modes_diverge_on_the_first_non_success() {
        for outcome in [item_failure(), item_cancelled()] {
            let fail_fast = MapState {
                next_ordinal: 2,
                in_flight: 2,
                ..child_map(5, 2, ChildWorkflowMapFailureMode::FailFast)
            };
            let expected_failure = fail_fast_failure(0, &outcome).unwrap();
            assert_eq!(
                step(&fail_fast, completed(0, outcome.clone())),
                Ok(vec![
                    MapEffect::RecordItemOutcome {
                        ordinal: 0,
                        outcome: outcome.clone()
                    },
                    MapEffect::FailMap {
                        failure: expected_failure
                    },
                    MapEffect::AbandonPendingItems,
                    MapEffect::CancelChildren {
                        reason: child_cancellation_reason(&map_command_id())
                    },
                    MapEffect::MarkDescriptorTerminal,
                ]),
            );

            let collect_all = MapState {
                next_ordinal: 2,
                in_flight: 2,
                ..child_map(5, 2, ChildWorkflowMapFailureMode::CollectAll)
            };
            assert_eq!(
                step(&collect_all, completed(0, outcome.clone())),
                Ok(vec![
                    MapEffect::RecordItemOutcome {
                        ordinal: 0,
                        outcome
                    },
                    MapEffect::MaterializeItems {
                        first_ordinal: 2,
                        count: 1
                    },
                    MapEffect::AdvanceDescriptor {
                        next_ordinal: 3,
                        in_flight: 2
                    },
                ]),
            );
        }
    }

    /// `CollectAll` completes with an outcome manifest once every item is
    /// terminal, however those items ended.
    #[test]
    fn collect_all_completes_once_every_item_is_terminal() {
        let state = MapState {
            next_ordinal: 3,
            in_flight: 1,
            recorded_outcomes: 2,
            ..child_map(3, 3, ChildWorkflowMapFailureMode::CollectAll)
        };
        assert_eq!(
            step(&state, completed(2, item_failure())),
            Ok(vec![
                MapEffect::RecordItemOutcome {
                    ordinal: 2,
                    outcome: item_failure()
                },
                MapEffect::CompleteMap { item_count: 3 },
                MapEffect::MarkDescriptorTerminal,
            ]),
        );
    }

    /// Cancelling the map command mid-fanout tombstones the pending items and
    /// closes the descriptor without appending a parent fact.
    #[test]
    fn parent_cancellation_mid_fanout_abandons_pending_items() {
        for state in [
            MapState {
                next_ordinal: 2,
                in_flight: 2,
                recorded_outcomes: 1,
                ..activity_map(5, 2)
            },
            MapState {
                next_ordinal: 2,
                in_flight: 2,
                recorded_outcomes: 1,
                ..child_map(5, 2, ChildWorkflowMapFailureMode::CollectAll)
            },
        ] {
            assert_eq!(
                step(&state, MapEvent::ParentCancelled),
                Ok(vec![
                    MapEffect::AbandonPendingItems,
                    MapEffect::MarkDescriptorTerminal,
                ]),
            );
        }
    }

    /// Already-recorded outcomes stay recorded across a cancellation: nothing
    /// rewrites or deletes them, so a later result-manifest read is stable.
    #[test]
    fn cancellation_is_idempotent_and_absorbing() {
        let cancelled = MapState {
            completed: true,
            in_flight: 0,
            next_ordinal: 2,
            recorded_outcomes: 1,
            ..child_map(5, 2, ChildWorkflowMapFailureMode::CollectAll)
        };
        assert_eq!(step(&cancelled, MapEvent::ParentCancelled), Ok(Vec::new()));
        assert_eq!(step(&cancelled, completed(1, success())), Ok(Vec::new()));
        assert_eq!(
            step(&cancelled, attempt_failed(1, ItemRetryDecision::Exhausted)),
            Ok(Vec::new())
        );
        assert_eq!(
            step(
                &cancelled,
                attempt_failed(1, ItemRetryDecision::Retry { next_attempt: 2 })
            ),
            Ok(Vec::new())
        );
        assert_eq!(
            step(
                &cancelled,
                MapEvent::DescriptorCreated {
                    parent_terminal: false
                }
            ),
            Ok(Vec::new())
        );
    }

    /// Idempotency: a duplicate terminal outcome for an ordinal that already
    /// has one records nothing and — critically — releases no second slot.
    #[test]
    fn duplicate_item_outcome_releases_no_second_slot() {
        let state = MapState {
            next_ordinal: 2,
            in_flight: 2,
            recorded_outcomes: 1,
            ..child_map(5, 2, ChildWorkflowMapFailureMode::CollectAll)
        };
        assert_eq!(
            step(
                &state,
                MapEvent::ItemCompleted {
                    ordinal: 0,
                    outcome: success(),
                    already_recorded: true,
                    parent_terminal: false,
                }
            ),
            Ok(Vec::new()),
        );
        // The same guard covers a duplicate failure, including a fail-fast one
        // that would otherwise re-fail an already-recorded item.
        let fail_fast = MapState {
            failure_mode: ChildWorkflowMapFailureMode::FailFast,
            ..state
        };
        assert_eq!(
            step(
                &fail_fast,
                MapEvent::ItemCompleted {
                    ordinal: 0,
                    outcome: item_failure(),
                    already_recorded: true,
                    parent_terminal: false,
                }
            ),
            Ok(Vec::new()),
        );
        let activity = MapState {
            next_ordinal: 2,
            in_flight: 2,
            recorded_outcomes: 1,
            ..activity_map(5, 2)
        };
        let duplicate_failure = AttemptFailure {
            already_recorded: true,
            ..AttemptFailure::new(0, ItemRetryDecision::Exhausted)
        };
        assert_eq!(step(&activity, duplicate_failure.event()), Ok(Vec::new()));
    }

    /// Terminal is absorbing: no event after the map completed or failed can
    /// move it to the other terminal state or reopen it.
    #[test]
    fn terminal_maps_absorb_every_later_event() {
        for kind in [MapKind::Activity, MapKind::ChildWorkflow] {
            let terminal = MapState {
                kind,
                completed: true,
                next_ordinal: 5,
                in_flight: 0,
                recorded_outcomes: 5,
                ..activity_map(5, 2)
            };
            let events = [
                MapEvent::DescriptorCreated {
                    parent_terminal: false,
                },
                completed(0, success()),
                completed(4, item_failure()),
                attempt_failed(0, ItemRetryDecision::Exhausted),
                attempt_failed(0, ItemRetryDecision::Retry { next_attempt: 2 }),
                MapEvent::ParentCancelled,
            ];
            for event in events {
                assert_eq!(
                    step(&terminal, event.clone()),
                    Ok(Vec::new()),
                    "{kind:?} terminal map must absorb {event:?}"
                );
            }
        }
    }

    /// A completion arriving after the parent run closed cannot append a
    /// terminal map fact: the activity map rejects it so the transaction rolls
    /// back, the child map drops the notification but keeps the outcome row.
    #[test]
    fn completion_after_the_parent_closed_never_appends_a_parent_fact() {
        let activity = MapState {
            next_ordinal: 3,
            in_flight: 1,
            recorded_outcomes: 2,
            ..activity_map(3, 2)
        };
        assert_eq!(
            step(
                &activity,
                MapEvent::ItemCompleted {
                    ordinal: 2,
                    outcome: success(),
                    already_recorded: false,
                    parent_terminal: true,
                }
            ),
            Err(MapReject::TerminalParent),
        );
        let late_failure = AttemptFailure {
            parent_terminal: true,
            ..AttemptFailure::new(2, ItemRetryDecision::Exhausted)
        };
        assert_eq!(
            step(&activity, late_failure.event()),
            Err(MapReject::TerminalParent),
        );

        let child = MapState {
            next_ordinal: 3,
            in_flight: 1,
            recorded_outcomes: 2,
            ..child_map(3, 2, ChildWorkflowMapFailureMode::CollectAll)
        };
        assert_eq!(
            step(
                &child,
                MapEvent::ItemCompleted {
                    ordinal: 2,
                    outcome: success(),
                    already_recorded: false,
                    parent_terminal: true,
                }
            ),
            Ok(vec![MapEffect::RecordItemOutcome {
                ordinal: 2,
                outcome: success()
            }]),
        );
        let child_fail_fast = MapState {
            failure_mode: ChildWorkflowMapFailureMode::FailFast,
            ..child
        };
        assert_eq!(
            step(
                &child_fail_fast,
                MapEvent::ItemCompleted {
                    ordinal: 1,
                    outcome: item_failure(),
                    already_recorded: false,
                    parent_terminal: true,
                }
            ),
            Ok(vec![MapEffect::RecordItemOutcome {
                ordinal: 1,
                outcome: item_failure()
            }]),
        );
    }

    /// A parent-terminal notification never partially closes the descriptor:
    /// no `MarkDescriptorTerminal` without the matching parent fact.
    #[test]
    fn terminal_parent_never_marks_the_descriptor_terminal() {
        let child = MapState {
            next_ordinal: 3,
            in_flight: 1,
            recorded_outcomes: 2,
            ..child_map(3, 2, ChildWorkflowMapFailureMode::CollectAll)
        };
        let effects = step(
            &child,
            MapEvent::ItemCompleted {
                ordinal: 2,
                outcome: success(),
                already_recorded: false,
                parent_terminal: true,
            },
        )
        .unwrap();
        assert!(!effects.contains(&MapEffect::MarkDescriptorTerminal));
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            MapEffect::CompleteMap { .. } | MapEffect::FailMap { .. }
        )));
    }

    /// An ordinal outside the input manifest rejects the whole transition, so
    /// the provider raises and rolls back rather than recording a phantom
    /// item. The rejection is never mixed into an effect list.
    #[test]
    fn out_of_bounds_ordinals_reject_the_whole_transition() {
        let state = MapState {
            next_ordinal: 3,
            in_flight: 3,
            ..activity_map(3, 3)
        };
        assert_eq!(
            step(&state, completed(3, success())),
            Err(MapReject::OutOfBounds { ordinal: 3 }),
        );
        assert_eq!(
            step(&state, attempt_failed(9, ItemRetryDecision::Exhausted)),
            Err(MapReject::OutOfBounds { ordinal: 9 }),
        );
        assert_eq!(
            step(
                &state,
                attempt_failed(9, ItemRetryDecision::Retry { next_attempt: 2 })
            ),
            Err(MapReject::OutOfBounds { ordinal: 9 }),
        );
    }

    /// `next_ordinal` is a contiguous admission cursor, never a scan: the
    /// engine admits from it unconditionally and never consults
    /// `recorded_outcomes` to decide *which* ordinals to admit.
    ///
    /// `memory.rs:2050-2052` is the one place that skips the cursor forward
    /// over already-recorded ordinals; `sqlite.rs:4452` and
    /// `postgres.rs:7225` do not. That skip is provably dead — every outcome
    /// is recorded through a path that requires the ordinal to have been
    /// materialized first (`memory.rs:1678`, `:1895`), so no outcome can
    /// exist at or above the cursor — so the engine drops it rather than
    /// carrying a third copy of the rule. This pins the consequence if a
    /// provider ever violates the precondition, so 6B has a visible contract
    /// to uphold rather than an implicit one.
    #[test]
    fn admission_reads_the_cursor_and_never_scans_recorded_outcomes() {
        // Same cursor, wildly different outcome tallies: admission is
        // identical, because only the cursor and the slots decide it.
        for recorded_outcomes in [0, 1, 4] {
            let state = MapState {
                next_ordinal: 2,
                in_flight: 0,
                recorded_outcomes,
                ..activity_map(5, 2)
            };
            assert_eq!(
                step(
                    &state,
                    MapEvent::DescriptorCreated {
                        parent_terminal: false
                    }
                ),
                Ok(vec![
                    MapEffect::MaterializeItems {
                        first_ordinal: 2,
                        count: 2
                    },
                    MapEffect::AdvanceDescriptor {
                        next_ordinal: 4,
                        in_flight: 2
                    },
                ]),
            );
        }
    }

    /// Driving a whole activity map through the engine keeps `in_flight` at or
    /// below the bound at every step and admits every ordinal exactly once.
    #[test]
    fn full_fanout_never_exceeds_the_bound_and_admits_each_ordinal_once() {
        let mut state = activity_map(7, 3);
        let mut admitted = Vec::new();
        let mut apply = |state: &mut MapState, effects: Vec<MapEffect>| {
            for effect in effects {
                match effect {
                    MapEffect::MaterializeItems {
                        first_ordinal,
                        count,
                    } => admitted.extend(first_ordinal..first_ordinal + count),
                    MapEffect::AdvanceDescriptor {
                        next_ordinal,
                        in_flight,
                    } => {
                        state.next_ordinal = next_ordinal;
                        state.in_flight = in_flight;
                    }
                    MapEffect::CompleteMap { .. } => {}
                    MapEffect::MarkDescriptorTerminal => {
                        state.completed = true;
                        state.in_flight = 0;
                    }
                    other => panic!("unexpected effect {other:?}"),
                }
            }
            assert!(
                state.in_flight <= state.slot_limit(),
                "in_flight {} exceeded the bound {}",
                state.in_flight,
                state.slot_limit()
            );
        };

        let effects = step(
            &state,
            MapEvent::DescriptorCreated {
                parent_terminal: false,
            },
        )
        .unwrap();
        apply(&mut state, effects);
        for ordinal in 0..7 {
            let effects = step(&state, completed(ordinal, success())).unwrap();
            state.recorded_outcomes += 1;
            state.in_flight = state.in_flight.saturating_sub(1);
            apply(&mut state, effects);
        }
        assert!(state.completed);
        assert_eq!(admitted, (0..7).collect::<Vec<_>>());
    }

    /// The shared tallies the terminal events carry.
    #[test]
    fn outcome_counts_classify_every_outcome_variant() {
        let outcomes = vec![
            success(),
            item_failure(),
            item_cancelled(),
            item_failure(),
            success(),
        ];
        assert_eq!(
            outcome_counts(&outcomes),
            MapOutcomeCounts {
                success_count: 2,
                failure_count: 2,
                cancellation_count: 1,
            }
        );
        assert_eq!(outcome_counts(&[]), MapOutcomeCounts::default());
        assert_eq!(
            activity_outcome_counts(4),
            MapOutcomeCounts {
                success_count: 4,
                failure_count: 0,
                cancellation_count: 0,
            }
        );
    }

    /// The parent-visible strings the terminal facts carry are persisted in
    /// history, so they are pinned byte-for-byte.
    #[test]
    fn parent_visible_failure_strings_are_pinned() {
        assert_eq!(fail_fast_failure(3, &success()), None);
        let failure = fail_fast_failure(3, &item_cancelled()).unwrap();
        assert_eq!(failure.error_type, "durust.child_workflow_cancelled");
        assert_eq!(
            failure.message,
            "child workflow map item 3 was cancelled: stop"
        );
        assert!(failure.non_retryable);
        assert_eq!(
            fail_fast_failure(3, &item_failure()).unwrap(),
            DurableFailure::non_retryable("kind", "boom")
        );
        assert_eq!(
            child_cancellation_reason(&map_command_id()),
            "child workflow map `run-1`:7 failed"
        );
    }
}
