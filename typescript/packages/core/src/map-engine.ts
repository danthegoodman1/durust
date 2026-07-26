import type { CommandId } from "./types.js";
import type { ChildWorkflowMapItemOutcome, DurableFailure } from "./api.js";
import type { ChildWorkflowMapFailureMode, RetryPolicy } from "./options.js";

// The pure fanout state machine shared by the activity-map and
// child-workflow-map paths of every provider.
//
// The whole map machine — item materialization, `maxInFlight` accounting,
// per-item retry, failure-mode policy, result-manifest assembly, and parent
// notification — expressed once as `(descriptor state, event) -> ordered
// effects`. It is the TypeScript twin of `src/map_engine.rs`; the two must stay
// behaviourally identical because Phase 7's shared corpus runs against both.
//
// Invariants this module owns:
//
// - **Pure.** No I/O, no async, no clock reads, no randomness. Every input is an
//   argument, so `MemoryBackend` and `SqliteBackend` can apply the result
//   synchronously and `PostgresBackend` can apply it inside an async
//   transaction without reshaping the call into an async state machine.
// - **Effects are plain data and are applied in order.** Applying a prefix and
//   crashing is safe: every effect is idempotent against the state that produced
//   it, and the provider's transaction makes the suffix atomic. A transition
//   that must not be applied at all returns a `Reject` instead of an effect
//   list, so no provider has to scan for pseudo-effects before applying the
//   first real one.
// - **Effects are batch-shaped, never per-item.** Materialization returns one
//   contiguous `[firstOrdinal, firstOrdinal + count)` range, so a set-based
//   provider can issue one statement per batch instead of `count` round trips.
// - **One slot per admitted ordinal.** Materialization takes a slot for every
//   ordinal it admits; exactly one terminal outcome per ordinal releases it. A
//   retry keeps the slot (the item is still in flight); a duplicate terminal
//   outcome releases nothing.
// - **Terminal is absorbing.** Once the descriptor is terminal no event produces
//   effects, so a map can never go completed -> failed or failed -> completed
//   under any interleaving.
//
// Empty input manifests complete at descriptor creation. A map scheduled with
// `itemCount === 0` has nothing to admit and nothing outstanding, so the engine
// emits an empty result manifest, notifies the parent, and marks the descriptor
// terminal. That is what every TypeScript provider does today, verified by
// execution rather than by reading: `MemoryBackend` and `PostgresBackend` append
// the terminal fact, and `SqliteBackend`'s map code produces it too —
// `commitWorkflowTask`'s outer `#saveWorkflow(state)`
// (`packages/sqlite/src/index.ts:508`) then overwrites the row that
// `#completeActivityMapIfDone` wrote through its own `#stateForRun` re-read, so
// SQLite's apparent stall is that lost update and not a design choice. The lost
// update is tracked as its own plan row.
//
// `src/map_engine.rs` stalls instead, because all three Rust providers stall.
// The two engines are knowingly divergent here until row 6G lands on the Rust
// side and converges Rust to this behaviour. The divergence is not limited to
// `itemCount === 0`: the completion condition at `DescriptorCreated` is
// `recordedOutcomes >= itemCount`, so for *any* such state this engine emits
// `CompleteMap` where Rust emits nothing, and when `nextOrdinal < itemCount` it
// emits `MaterializeItems` and `CompleteMap` in one list. The shared
// transition table (`typescript/fixtures/contract/map-transitions.json`)
// therefore excludes every `DescriptorCreated` case with
// `recordedOutcomes >= itemCount`, not merely the empty-manifest one, and both
// runners assert that the exclusion is still declared; Phase 7's corpus must
// carry the same exclusion until 6G lands.
//
// Descriptor-creation completion deliberately ignores `parentTerminal`, unlike
// every other terminal path in this module — see `step`.
//
// All three TypeScript providers drive their activity-map and
// child-workflow-map paths through `step` and apply the effect list it
// returns, so the module is exported from the package index for the two
// out-of-package providers to import.

/** Which map machine a descriptor drives. */
export type MapKind = "Activity" | "ChildWorkflow";

/**
 * The descriptor fields the engine reads. Providers project their row (or their
 * in-memory record) into this before every transition; nothing else about the
 * descriptor is engine-visible.
 */
export interface MapState {
  readonly mapCommandId: CommandId;
  readonly kind: MapKind;
  /**
   * Meaningful for `"ChildWorkflow"`. An activity map is always fail-fast: the
   * first item that exhausts its retries fails the map.
   */
  readonly failureMode: ChildWorkflowMapFailureMode;
  readonly itemCount: number;
  /**
   * Lowest ordinal never yet admitted.
   *
   * Invariant the providers must uphold: materialization is strictly
   * forward-only and every recorded outcome sits at an ordinal *below*
   * `nextOrdinal`, so this is a contiguous admission cursor and also the
   * admitted-item high-water mark. The engine re-admits from it
   * unconditionally; a descriptor whose cursor sits at or behind a recorded
   * outcome would have that ordinal admitted twice.
   */
  readonly nextOrdinal: number;
  readonly inFlight: number;
  readonly maxInFlight: number;
  /** Item ordinals with a persisted terminal outcome. */
  readonly recordedOutcomes: number;
  readonly completed: boolean;
}

/**
 * Whether a failed attempt was an explicit failure or a lapsed deadline.
 * Explicit failures are paced by the retry backoff; timeouts are already paced
 * by the deadline that fired, so their retry is immediately claimable.
 */
export type ItemAttemptFailureKind = "Failed" | "TimedOut";

/** Retry-versus-exhaustion verdict for one item attempt. */
export type ItemRetryDecision =
  | { readonly kind: "Retry"; readonly nextAttempt: number }
  | { readonly kind: "Exhausted" };

/**
 * The retry-versus-exhaustion verdict for one item attempt, computed *without*
 * applying it.
 *
 * Every provider used to fold this decision into the same statement that
 * rescheduled the attempt, which meant the map path was reached only on the
 * exhausting attempt. Splitting the verdict from its application is what lets
 * `step` own both outcomes: only the engine knows whether the map is still
 * running, and only it can decide that a retry of an item whose map already
 * ended must reschedule nothing.
 *
 * `failure` is `null` for a lapsed deadline, which is paced by the deadline
 * that fired rather than by the policy's non-retryable rules.
 */
export function itemRetryDecision(
  failedAttempt: number,
  policy: RetryPolicy,
  failure: DurableFailure | null
): ItemRetryDecision {
  const maxAttempts = Math.max(1, Math.trunc(policy.maxAttempts));
  if (failedAttempt >= maxAttempts) {
    return { kind: "Exhausted" };
  }
  if (
    failure !== null &&
    (failure.nonRetryable || policy.nonRetryableErrorTypes.includes(failure.errorType))
  ) {
    return { kind: "Exhausted" };
  }
  return { kind: "Retry", nextAttempt: failedAttempt + 1 };
}

/**
 * One input to the machine. Every field is a fact the provider has already read
 * inside its transaction, so the transition itself needs no further reads.
 */
export type MapEvent =
  /** The scheduling commit inserted the descriptor. */
  | { readonly kind: "DescriptorCreated"; readonly parentTerminal: boolean }
  /**
   * An item reached a terminal outcome: an activity item's result, or a child
   * item's terminal workflow event mapped to a `ChildWorkflowMapItemOutcome`.
   */
  | {
      readonly kind: "ItemCompleted";
      readonly ordinal: number;
      readonly outcome: ChildWorkflowMapItemOutcome<unknown>;
      /**
       * This ordinal already has a persisted outcome — a duplicate delivery,
       * which must record nothing and release no slot.
       */
      readonly alreadyRecorded: boolean;
      readonly parentTerminal: boolean;
    }
  /** An activity-map item's attempt failed or timed out. */
  | {
      readonly kind: "ItemAttemptFailed";
      readonly ordinal: number;
      readonly failure: DurableFailure;
      readonly attemptFailure: ItemAttemptFailureKind;
      readonly decision: ItemRetryDecision;
      /** The 1-based attempt that just failed, used for the backoff. */
      readonly failedAttempt: number;
      readonly retryPolicy: RetryPolicy;
      /** The item's start-to-close timeout, restarted on every retry. */
      readonly startToCloseTimeoutMs: number | null;
      readonly nowMs: number;
      readonly alreadyRecorded: boolean;
      readonly parentTerminal: boolean;
    }
  /**
   * The parent cancelled this map command.
   *
   * **No TypeScript producer.** Rust drives this from
   * `WorkflowTaskCommit::cancel_commands`; the TypeScript `WorkflowTaskCommit`
   * has no such field (`grep -rn "cancelCommands" typescript/packages` is
   * empty), so no provider raises it — the wiring left it unreachable rather
   * than inventing a producer. It is kept so the two engines stay twins and is
   * awaiting `cancelCommands` on the TypeScript commit shape. The shared
   * transition table excludes it for the same reason.
   */
  | { readonly kind: "ParentCancelled" };

/**
 * A storage operation for the provider to execute. The list is ordered and
 * total: applying it is the whole of the provider's map work for that event.
 */
export type MapEffect =
  /**
   * Insert the item's terminal outcome row, keyed by `(mapCommandId, ordinal)`.
   * Emitted only for `"ChildWorkflow"` maps.
   */
  | {
      readonly kind: "RecordItemOutcome";
      readonly ordinal: number;
      readonly outcome: ChildWorkflowMapItemOutcome<unknown>;
    }
  /**
   * Materialize `[firstOrdinal, firstOrdinal + count)` as one batch: read the
   * manifest pages covering the range and issue one set-based insert of item
   * activity tasks (activity maps) or item child starts (child maps).
   */
  | { readonly kind: "MaterializeItems"; readonly firstOrdinal: number; readonly count: number }
  /**
   * Persist the descriptor's admission cursor after a materialization batch.
   * Always immediately follows `MaterializeItems`, and is the only effect that
   * writes `inFlight` on a non-terminal path.
   *
   * Because it is emitted only alongside an admission, a released slot with
   * nothing left to admit is not written back, so a stored `inFlight` can sit
   * above the true number of outstanding items. That is safe in one direction
   * only, and deliberately so: the stored count is never *below* the truth, so
   * it can delay an admission but can never let concurrency past
   * `maxInFlight`. It can only go stale once `nextOrdinal` has reached
   * `itemCount`, where `materialize` yields an empty batch for any `inFlight`
   * whatsoever, so no later decision reads it; `MarkDescriptorTerminal` then
   * resets it to zero.
   */
  | { readonly kind: "AdvanceDescriptor"; readonly nextOrdinal: number; readonly inFlight: number }
  /**
   * Requeue the item as `nextAttempt`: release its claim, clear both heartbeat
   * fields, and restamp its deadlines. The item keeps its `maxInFlight` slot,
   * because it is still in flight.
   */
  | {
      readonly kind: "ScheduleItemRetry";
      readonly ordinal: number;
      readonly nextAttempt: number;
      /** Not claimable before this instant; `null` means immediately claimable. */
      readonly visibleAtMs: number | null;
      /**
       * Restarted start-to-close deadline, measured from the visibility instant
       * so the timeout scanner cannot fire on a task that was never claimable.
       * `null` means the item has no start-to-close timeout.
       */
      readonly timeoutAtMs: number | null;
    }
  /**
   * Assemble the result manifest from the item outcomes in ascending ordinal
   * order over the input manifest's page boundaries, append the terminal success
   * fact to the parent, and wake it.
   */
  | { readonly kind: "CompleteMap"; readonly itemCount: number }
  /** Append the terminal failure fact to the parent and wake it. */
  | { readonly kind: "FailMap"; readonly failure: DurableFailure }
  /**
   * Tombstone every not-yet-terminal item task and every undispatched item start
   * of this map so neither the claim path nor the timeout scanner can resurrect
   * an item of a map that is over.
   */
  | { readonly kind: "AbandonPendingItems" }
  /**
   * Cancel every already-running, not-yet-terminal child of this map with
   * `reason`. Emitted only for `"ChildWorkflow"` maps.
   */
  | { readonly kind: "CancelChildren"; readonly reason: string }
  /** Flip the descriptor to terminal: `completed = true, inFlight = 0`. */
  | { readonly kind: "MarkDescriptorTerminal" };

/**
 * A transition that must not be applied at all. The provider raises the mapped
 * error and rolls its transaction back; no effect from the same event is
 * applied.
 */
export type MapReject =
  /** The item ordinal is outside the input manifest. */
  | { readonly kind: "OutOfBounds"; readonly ordinal: number }
  /**
   * The map reached a terminal state but the parent run is already closed, so
   * its terminal fact has nowhere to go.
   */
  | { readonly kind: "TerminalParent" };

/** The result of one transition: an ordered effect list, or a rejection. */
export type MapTransition =
  | { readonly kind: "Effects"; readonly effects: readonly MapEffect[] }
  | { readonly kind: "Reject"; readonly reject: MapReject };

/** Success/failure/cancellation tallies for a terminal map. */
export interface MapOutcomeCounts {
  readonly successCount: number;
  readonly failureCount: number;
  readonly cancellationCount: number;
}

const NO_EFFECTS: MapTransition = { kind: "Effects", effects: [] };

/**
 * `maxInFlight` clamped to at least one slot. A zero bound would admit nothing
 * and stall the map forever, so it is read as one.
 *
 * The TypeScript providers reject a non-positive `maxInFlight` at descriptor
 * creation, before the engine ever sees it; this clamp is the engine's defence
 * for a descriptor that already exists with a degenerate bound.
 */
export function mapSlotLimit(state: MapState): number {
  return Math.max(1, Math.trunc(state.maxInFlight));
}

/** Whether `ordinal` addresses a real item of the input manifest. */
export function mapOrdinalInBounds(state: MapState, ordinal: number): boolean {
  return Number.isInteger(ordinal) && ordinal >= 0 && ordinal < state.itemCount;
}

/**
 * How many ordinals of a provider's slot array carry a persisted terminal
 * outcome, which is `MapState.recordedOutcomes`.
 *
 * Providers store item outcomes as a dense array of length `itemCount` with a
 * hole for every ordinal that has not landed yet. Projecting the count here
 * keeps the three providers from each inventing their own predicate for what
 * counts as recorded.
 */
export function recordedOutcomeCount(slots: readonly (unknown | null | undefined)[]): number {
  let recorded = 0;
  for (const slot of slots) {
    if (slot !== null && slot !== undefined) {
      recorded += 1;
    }
  }
  return recorded;
}

/**
 * The message a provider raises for a rejected transition. Rejections must not
 * reach storage: the provider raises this and rolls its transaction back, and
 * the transition's effects are never applied.
 */
export function mapRejectMessage(kind: MapKind, reject: MapReject): string {
  if (reject.kind === "OutOfBounds") {
    return kind === "Activity"
      ? `activity map item ordinal ${reject.ordinal} out of bounds`
      : `child workflow map item ordinal ${reject.ordinal} out of bounds`;
  }
  return "terminal workflow rejects workflow-visible mutations";
}

/** Tallies for a child map's terminal event, over the ordered outcome list. */
export function outcomeCounts(
  outcomes: readonly ChildWorkflowMapItemOutcome<unknown>[]
): MapOutcomeCounts {
  let successCount = 0;
  let failureCount = 0;
  let cancellationCount = 0;
  for (const outcome of outcomes) {
    if (outcome.kind === "Succeeded") {
      successCount += 1;
    } else if (outcome.kind === "Failed") {
      failureCount += 1;
    } else {
      cancellationCount += 1;
    }
  }
  return { successCount, failureCount, cancellationCount };
}

/**
 * Tallies for an activity map's terminal event. An activity map completes only
 * when every item succeeded, so the counts are structural.
 */
export function activityOutcomeCounts(itemCount: number): MapOutcomeCounts {
  return { successCount: itemCount, failureCount: 0, cancellationCount: 0 };
}

/**
 * Parent-visible failure for a child map item that did not succeed. A
 * cancellation is reported as a non-retryable failure naming the ordinal, so the
 * parent's `ChildWorkflowMapFailed` says which item stopped the map.
 */
export function failFastFailure(
  ordinal: number,
  outcome: ChildWorkflowMapItemOutcome<unknown>
): DurableFailure | null {
  if (outcome.kind === "Succeeded") {
    return null;
  }
  if (outcome.kind === "Failed") {
    return outcome.failure;
  }
  return {
    errorType: "durust.child_workflow_cancelled",
    message: `child workflow map item ${ordinal} was cancelled: ${outcome.reason}`,
    nonRetryable: true
  };
}

/**
 * Reason stamped on the `WorkflowCancelled` event of every child cancelled
 * because its map failed fast.
 */
export function childCancellationReason(mapCommandId: CommandId): string {
  return `child workflow map \`${mapCommandId.runId}\`:${mapCommandId.seq} failed`;
}

/**
 * Reason stamped on the `WorkflowCancelled` event of every child cancelled
 * because its map's *command* was withdrawn by
 * `WorkflowTaskCommit.cancelCommands`.
 *
 * Distinct from `childCancellationReason`: nothing failed, the parent stopped
 * wanting the results. Cancelling those children is not a `step` effect — the
 * engine's `ParentCancelled` emits only `AbandonPendingItems` and
 * `MarkDescriptorTerminal`, in both runtimes — because the two call sites want
 * different things. When a *run* closes, its children belong to the
 * `parentClosePolicy` path, which must be free to abandon them; when a
 * *command* is withdrawn on a live run, that path never runs and the children
 * would be orphaned. So the cancellation is the provider's, like
 * `cancelChildrenForClosedParent` already is, and only the string lives here so
 * the three providers cannot drift on it.
 */
export function mapCommandCancelledReason(mapCommandId: CommandId): string {
  return `child workflow map \`${mapCommandId.runId}\`:${mapCommandId.seq} cancelled`;
}

/**
 * Backoff for the next attempt of a map item whose attempt `failedAttempt` just
 * failed. Identical to the delay the plain activity retry path applies, so a map
 * item and a standalone activity with the same policy are paced the same way.
 */
export function itemRetryDelayMs(policy: RetryPolicy, failedAttempt: number): number {
  const initial = Math.max(0, policy.initialIntervalMs);
  const max = Math.max(initial, policy.maxIntervalMs);
  const coefficient = Math.max(1, policy.backoffCoefficient);
  return Math.min(max, Math.round(initial * coefficient ** Math.max(0, failedAttempt - 1)));
}

/** The whole machine: descriptor state plus one event in, ordered effects out. */
export function step(state: MapState, event: MapEvent): MapTransition {
  // Terminal is absorbing. Every later event — a duplicate completion, a late
  // failure, a cancellation, a completion racing the terminal commit — is a
  // no-op, so no interleaving can move a map between terminal states.
  if (state.completed) {
    return NO_EFFECTS;
  }

  switch (event.kind) {
    // Mirrors the `#materialize…(); #complete…IfDone();` pair every TypeScript
    // provider runs at descriptor creation. For a non-empty manifest the
    // completion check cannot fire — no outcome can be recorded yet — so this is
    // materialization alone.
    case "DescriptorCreated": {
      const admitted = materialize(state, state.inFlight);
      if (state.recordedOutcomes < state.itemCount) {
        return effects(admitted);
      }
      // An empty input manifest has nothing to admit and nothing outstanding, so
      // the map is terminal the moment its descriptor exists.
      //
      // `parentTerminal` is deliberately ignored here, unlike every other
      // terminal path, because Phase 6 is behaviour-preserving. Verified by
      // execution on one commit that both schedules an empty map and closes the
      // run: all three providers **accept** it, and memory and Postgres append
      // the map's terminal fact *after* the run's own —
      // `WorkflowStarted,ActivityMapScheduled,WorkflowCompleted,ActivityMapCompleted`.
      // (SQLite produces the same completion and then loses it to the
      // `#saveWorkflow` overwrite described above, so it stops at
      // `…,WorkflowCompleted`.)
      //
      // That ordering violates the terminal-commit guard and is tracked as its
      // own plan row. The engine reproduces it rather than half-fixing it:
      // routing this through `terminalParent` would roll the *whole commit*
      // back instead, turning an accepted commit into a rejected one — a
      // strictly larger change, and the one that got the equivalent Rust change
      // reverted.
      return effects([
        ...admitted,
        // Lands after the run's own terminal event when the closing commit
        // schedules this map. Deliberate: see the `parentTerminal` note above.
        { kind: "CompleteMap", itemCount: state.itemCount },
        { kind: "MarkDescriptorTerminal" }
      ]);
    }

    case "ItemCompleted":
      return itemTerminal(
        state,
        event.ordinal,
        event.outcome,
        event.alreadyRecorded,
        event.parentTerminal
      );

    case "ItemAttemptFailed": {
      if (!mapOrdinalInBounds(state, event.ordinal)) {
        return reject({ kind: "OutOfBounds", ordinal: event.ordinal });
      }
      if (event.decision.kind === "Exhausted") {
        return itemTerminal(
          state,
          event.ordinal,
          { kind: "Failed", failure: event.failure },
          event.alreadyRecorded,
          event.parentTerminal
        );
      }
      // A retry keeps the item's slot: it is still in flight, so no other
      // ordinal may be admitted in its place.
      const visibleAtMs = retryVisibleAtMs(event.attemptFailure, event.retryPolicy, event.failedAttempt, event.nowMs);
      return effects([
        {
          kind: "ScheduleItemRetry",
          ordinal: event.ordinal,
          nextAttempt: event.decision.nextAttempt,
          visibleAtMs,
          timeoutAtMs: retryTimeoutAtMs(visibleAtMs ?? event.nowMs, event.startToCloseTimeoutMs)
        }
      ]);
    }

    // Cancelling the command tombstones the map's pending work and closes the
    // descriptor. No parent fact is appended: the cancellation is already
    // recorded by the commit that carried it.
    case "ParentCancelled":
      return effects([{ kind: "AbandonPendingItems" }, { kind: "MarkDescriptorTerminal" }]);
  }
}

function effects(list: readonly MapEffect[]): MapTransition {
  return { kind: "Effects", effects: list };
}

function reject(value: MapReject): MapTransition {
  return { kind: "Reject", reject: value };
}

/**
 * When a retried item becomes claimable. An explicit failure is paced by the
 * policy backoff; a lapsed deadline already paced this attempt, so delaying
 * crash recovery further only adds latency.
 */
function retryVisibleAtMs(
  attemptFailure: ItemAttemptFailureKind,
  policy: RetryPolicy,
  failedAttempt: number,
  nowMs: number
): number | null {
  if (attemptFailure === "TimedOut") {
    return null;
  }
  const delay = itemRetryDelayMs(policy, failedAttempt);
  return delay > 0 ? nowMs + delay : null;
}

/**
 * Restarted start-to-close deadline for a retried item, measured from the
 * visibility instant rather than from `now`.
 */
function retryTimeoutAtMs(visibleFromMs: number, startToCloseTimeoutMs: number | null): number | null {
  return startToCloseTimeoutMs === null ? null : visibleFromMs + Math.max(0, startToCloseTimeoutMs);
}

function itemTerminal(
  state: MapState,
  ordinal: number,
  outcome: ChildWorkflowMapItemOutcome<unknown>,
  alreadyRecorded: boolean,
  parentTerminal: boolean
): MapTransition {
  if (!mapOrdinalInBounds(state, ordinal)) {
    return reject({ kind: "OutOfBounds", ordinal });
  }
  // A duplicate terminal outcome for an ordinal that already has one records
  // nothing, releases no slot, and admits nothing. Releasing twice would
  // over-admit the map past `maxInFlight`.
  if (alreadyRecorded) {
    return NO_EFFECTS;
  }

  const recorded: MapEffect[] =
    state.kind === "ChildWorkflow" ? [{ kind: "RecordItemOutcome", ordinal, outcome }] : [];
  const inFlightAfter = Math.max(0, state.inFlight - 1);
  const recordedAfter = state.recordedOutcomes + 1;

  const failFast =
    outcome.kind !== "Succeeded" &&
    (state.kind === "Activity" || state.failureMode === "FailFast");
  if (failFast) {
    const failure = failFastFailure(ordinal, outcome);
    if (failure === null) {
      throw new Error("a non-succeeded outcome always yields a parent-visible failure");
    }
    return terminalFailure(state, parentTerminal, recorded, failure);
  }

  if (recordedAfter >= state.itemCount) {
    return terminalSuccess(state, parentTerminal, recorded);
  }

  // Not terminal yet: the released slot may admit the next ordinals.
  return effects([...recorded, ...materialize(state, inFlightAfter)]);
}

/**
 * Effects that admit as many unadmitted ordinals as free slots allow, as one
 * contiguous batch.
 */
function materialize(state: MapState, inFlight: number): readonly MapEffect[] {
  const free = Math.max(0, mapSlotLimit(state) - inFlight);
  const unadmitted = Math.max(0, state.itemCount - state.nextOrdinal);
  const count = Math.min(free, unadmitted);
  if (count <= 0) {
    return [];
  }
  return [
    { kind: "MaterializeItems", firstOrdinal: state.nextOrdinal, count },
    {
      kind: "AdvanceDescriptor",
      nextOrdinal: state.nextOrdinal + count,
      inFlight: inFlight + count
    }
  ];
}

function terminalSuccess(
  state: MapState,
  parentTerminal: boolean,
  recorded: readonly MapEffect[]
): MapTransition {
  if (parentTerminal) {
    return terminalParent(state, recorded);
  }
  return effects([
    ...recorded,
    { kind: "CompleteMap", itemCount: state.itemCount },
    { kind: "MarkDescriptorTerminal" }
  ]);
}

function terminalFailure(
  state: MapState,
  parentTerminal: boolean,
  recorded: readonly MapEffect[],
  failure: DurableFailure
): MapTransition {
  if (parentTerminal) {
    return terminalParent(state, recorded);
  }
  return effects([
    ...recorded,
    { kind: "FailMap", failure },
    { kind: "AbandonPendingItems" },
    ...(state.kind === "ChildWorkflow"
      ? ([{ kind: "CancelChildren", reason: childCancellationReason(state.mapCommandId) }] as const)
      : []),
    { kind: "MarkDescriptorTerminal" }
  ]);
}

/**
 * The map is terminal but the parent run is closed, so its terminal fact has
 * nowhere to go. An activity map rejects the completion so the caller raises and
 * the transaction rolls back; a child map drops the notification and keeps the
 * recorded outcome, leaving the descriptor for the parent's terminal cleanup to
 * delete.
 */
function terminalParent(state: MapState, recorded: readonly MapEffect[]): MapTransition {
  return state.kind === "Activity" ? reject({ kind: "TerminalParent" }) : effects(recorded);
}
