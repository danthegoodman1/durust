import { historyEventType } from "./history.js";
import { itemRetryDelayMs } from "./map-engine.js";
import { readMapManifestItems, writeMapManifest } from "./map-manifest.js";
import { eventId } from "./types.js";
import type { ActivityTaskClaim, WorkflowTaskClaim } from "./backend.js";
import type {
  ActivityMapInputManifest,
  ActivityMapInputPage,
  ActivityMapResultManifest,
  ActivityMapResultPage,
  ChildWorkflowMapItemOutcome,
  ChildWorkflowMapResultManifest,
  ChildWorkflowMapResultPage,
  DurableFailure
} from "./api.js";
import type { ActivityTask, HistoryEvent, HistoryEventData } from "./history.js";
import type { PayloadRef } from "./payload.js";
import type { CommandId, EventId, Namespace, WorkerId, WorkflowId, WorkflowType } from "./types.js";

/**
 * The pure helpers every durable provider needs, in one place.
 *
 * `MemoryBackend`, `@durust/sqlite` and `@durust/postgres` each reimplement the
 * same provider mechanics on top of a different store, and each used to carry
 * its own private copy of these functions. That is how `sameCommandId` drifted:
 * two of the three copies compared `Number(left.seq) === Number(right.seq)`
 * while the third compared `left.seq === right.seq`, and nothing in the suite
 * distinguished them. Coercion is the wrong answer for a command identity —
 * `Number(null)`, `Number("")` and `Number(false)` are all `0`, so a `seq` that
 * arrived as any of those would compare *equal* to command `0` and cancel or
 * tombstone an unrelated command — so the strict copy is the one kept here.
 *
 * Only genuinely pure functions belong in this module. A helper that reads or
 * writes provider state (`markWorkflowReady` mutates its argument, `parseJson`'s
 * callers own the storage encoding) stays with the provider that owns that
 * state. The point of the module is that two providers cannot disagree about
 * something neither of them decides.
 *
 * Parameters are typed structurally rather than against each provider's private
 * `WorkflowState`/`ActivityState`/`ActivityLease` interfaces: those differ
 * between providers (sqlite's `ActivityState` has no `workflow` back-pointer,
 * for one), and pulling them into core would move provider state ownership as a
 * side effect of sharing arithmetic.
 */

/** A child run's terminal fact, as the three providers record it. */
export type ChildTerminalUpdate =
  | { readonly kind: "Completed"; readonly result: PayloadRef }
  | { readonly kind: "Failed"; readonly failure: DurableFailure }
  | { readonly kind: "Cancelled"; readonly reason: string };

/** The lease fields the claim-ownership checks read. */
export interface ClaimedLease {
  readonly claim: {
    readonly workerId: WorkerId | string;
    readonly token: number;
  };
}

/** The workflow-state fields `tailEventId` reads. */
export interface WorkflowHistoryTail {
  readonly history: readonly HistoryEvent[];
}

/** The activity-state fields the retry and timeout-message helpers read. */
export interface ActivityAttempt {
  readonly task: ActivityTask;
}

/** The activity-state fields the timeout scanner reads. */
export interface ActivityAttemptDeadline extends ActivityAttempt {
  readonly claim: {
    readonly startedAtMs: number;
    readonly heartbeatDeadlineAtMs: number | null;
  } | null;
  readonly terminalEventId: EventId | null;
}

export function workflowKey(namespace: Namespace | string, workflowId: WorkflowId | string): string {
  return `${namespace}/${workflowId}`;
}

export function workflowTypeKey(workflowTypeValue: WorkflowType): string {
  return `${workflowTypeValue.name}@${workflowTypeValue.version}`;
}

export function commandKey(id: CommandId): string {
  return `${id.runId}:${id.seq}`;
}

/**
 * Command identity, compared exactly as `CommandId` declares it: a branded
 * `number` seq under a run id.
 *
 * Not `Number(left.seq) === Number(right.seq)`. That spelling — which two of the
 * three provider copies carried — buys tolerance of a `seq` that arrives as a
 * string from a database driver, and pays for it with false positives on every
 * value `Number()` maps onto an existing seq: `null`, `""`, `false` and `[]` all
 * become `0`. Every caller here acts on a match by tombstoning an activity task,
 * abandoning a map item, or cancelling a child run, so a false positive is an
 * irreversible write against the wrong command. The tolerance was not needed
 * either: in both SQL providers every `CommandId` reaching this function is
 * decoded from a JSON/JSONB column, so its `seq` is already a JSON number.
 */
export function sameCommandId(left: CommandId, right: CommandId): boolean {
  return left.runId === right.runId && left.seq === right.seq;
}

/** The generated activity id of one materialized activity-map item. */
export function activityMapItemId(mapCommandId: CommandId, ordinal: number): string {
  return `${mapCommandId.runId}:map:${mapCommandId.seq}:${ordinal}`;
}

/** A child run's terminal fact as the map engine's item outcome. */
export function childWorkflowMapItemOutcome(
  terminal: ChildTerminalUpdate
): ChildWorkflowMapItemOutcome<unknown> {
  if (terminal.kind === "Completed") {
    return { kind: "Succeeded", result: terminal.result };
  }
  if (terminal.kind === "Failed") {
    return { kind: "Failed", failure: terminal.failure };
  }
  return { kind: "Cancelled", reason: terminal.reason };
}

/**
 * Whether one item row is still in flight: admitted by the descriptor cursor,
 * without a terminal outcome, on a map that has not ended. Derived rather than
 * tracked, so the descriptor's slot count stays the engine's alone.
 */
export function mapItemInFlight(
  nextOrdinal: number,
  terminal: boolean,
  ordinal: number,
  outcome: unknown
): boolean {
  return !terminal && ordinal < nextOrdinal && (outcome ?? null) === null;
}

export function tailEventId(state: WorkflowHistoryTail): EventId {
  return state.history.at(-1)?.eventId ?? eventId(0);
}

export function workflowLeaseMatches(lease: ClaimedLease, claim: WorkflowTaskClaim): boolean {
  return lease.claim.token === claim.token && lease.claim.workerId === claim.workerId;
}

export function activityLeaseMatches(lease: ClaimedLease, claim: ActivityTaskClaim): boolean {
  return lease.claim.token === claim.token && lease.claim.workerId === claim.workerId;
}

export function retryActivityAfterFailure(
  activity: ActivityAttempt,
  failure: DurableFailure,
  nowMs: number
): { readonly task: ActivityTask; readonly readyAtMs: number } | null {
  const policy = activity.task.retryPolicy;
  const maxAttempts = Math.max(1, Math.trunc(policy.maxAttempts));
  if (
    activity.task.attempt >= maxAttempts ||
    failure.nonRetryable ||
    policy.nonRetryableErrorTypes.includes(failure.errorType)
  ) {
    return null;
  }
  return {
    task: {
      ...activity.task,
      attempt: activity.task.attempt + 1
    },
    readyAtMs: nowMs + itemRetryDelayMs(policy, activity.task.attempt)
  };
}

export function retryActivityAfterTimeout(
  activity: ActivityAttempt,
  nowMs: number
): { readonly task: ActivityTask; readonly readyAtMs: number } | null {
  return retryActivityTaskAfterTimeout(activity.task, nowMs);
}

export function retryActivityTaskAfterTimeout(
  task: ActivityTask,
  nowMs: number
): { readonly task: ActivityTask; readonly readyAtMs: number } | null {
  const policy = task.retryPolicy;
  const maxAttempts = Math.max(1, Math.trunc(policy.maxAttempts));
  if (task.attempt >= maxAttempts) {
    return null;
  }
  return {
    task: {
      ...task,
      attempt: task.attempt + 1
    },
    readyAtMs: nowMs + itemRetryDelayMs(policy, task.attempt)
  };
}

export function activityHeartbeatDeadlineAt(
  task: ActivityTask,
  nowMs: number,
  leaseDurationMs: number
): number | null {
  if (task.heartbeatTimeoutMs !== null) {
    return nowMs + Math.max(0, task.heartbeatTimeoutMs);
  }
  return task.startToCloseTimeoutMs === null ? nowMs + Math.max(0, leaseDurationMs) : null;
}

/**
 * When the timeout scanner should reclaim this attempt, and which deadline
 * lapsed.
 *
 * Map items are covered on the same terms as any other activity. They used to
 * be exempt — the scanner skipped every task with a `mapItem`, so a hung item
 * was never recovered even though `ActivityMapTask` carries
 * `startToCloseTimeoutMs`/`heartbeatTimeoutMs` and every materialized item
 * copies them. Stored and never enforced is worse than not stored: with no
 * explicit timeout an item still gets the implicit lease-length heartbeat
 * deadline, so a worker that dies mid-item left the item to be re-offered by
 * lease expiry forever instead of failing its map.
 */
export function activityTimeoutDeadline(
  activity: ActivityAttemptDeadline
): { readonly deadline: number; readonly kind: "StartToClose" | "Heartbeat" } {
  if (activity.claim === null || activity.terminalEventId !== null) {
    return { deadline: Number.POSITIVE_INFINITY, kind: "StartToClose" };
  }
  const startToCloseDeadline =
    activity.task.startToCloseTimeoutMs === null
      ? Number.POSITIVE_INFINITY
      : activity.claim.startedAtMs + Math.max(0, activity.task.startToCloseTimeoutMs);
  const heartbeatDeadline = activity.claim.heartbeatDeadlineAtMs ?? Number.POSITIVE_INFINITY;
  return heartbeatDeadline < startToCloseDeadline
    ? { deadline: heartbeatDeadline, kind: "Heartbeat" }
    : { deadline: startToCloseDeadline, kind: "StartToClose" };
}

export function activityTimeoutMessage(
  activity: ActivityAttempt,
  kind: "StartToClose" | "Heartbeat"
): string {
  return kind === "Heartbeat"
    ? `activity ${activity.task.activityId} missed heartbeat on attempt ${activity.task.attempt}`
    : `activity ${activity.task.activityId} start-to-close timed out after ${activity.task.startToCloseTimeoutMs}ms`;
}

/**
 * A lapsed activity deadline as the parent-visible failure a map item carries.
 * A plain activity records the same text in its `ActivityTimedOut` event; a map
 * item has no per-item history, so the text rides the failure instead.
 */
export function activityTimeoutFailure(
  activity: ActivityAttempt,
  kind: "StartToClose" | "Heartbeat"
): DurableFailure {
  return {
    errorType: "durust.activity_timed_out",
    message: activityTimeoutMessage(activity, kind),
    nonRetryable: false
  };
}

export function makeHistoryEvent(id: EventId, data: HistoryEventData): HistoryEvent {
  return {
    eventId: id,
    eventType: historyEventType(data),
    data
  };
}

/**
 * JSON with `Uint8Array` round-tripping, for the SQL providers that store
 * payload envelopes as text or `jsonb`. Plain `JSON.stringify` renders a
 * `Uint8Array` as an object of index keys, which does not come back as bytes, so
 * binary payloads carry an explicit tag instead. Shared rather than copied
 * because the two halves are one wire format: a provider that drifted on either
 * side would read back payloads that no longer match what it wrote.
 */
export function stringifyJson(value: unknown): string {
  return JSON.stringify(value, (_key, nested) =>
    nested instanceof Uint8Array
      ? { __durustType: "Uint8Array", data: [...nested] }
      : nested
  );
}

export function parseJson<T>(value: string): T {
  return JSON.parse(value, (_key, nested) => {
    if (
      nested &&
      typeof nested === "object" &&
      (nested as { readonly __durustType?: unknown }).__durustType === "Uint8Array" &&
      Array.isArray((nested as { readonly data?: unknown }).data)
    ) {
      return Uint8Array.from((nested as { readonly data: readonly number[] }).data);
    }
    return nested;
  }) as T;
}

export function decodeActivityMapInputs(inputManifest: PayloadRef): readonly PayloadRef[] {
  return readMapManifestItems<ActivityMapInputPage<object>, PayloadRef>(
    inputManifest as PayloadRef<ActivityMapInputManifest<object>>,
    (page) => page.items,
    "activity map manifest"
  );
}

export function encodeActivityMapResultManifest(
  name: string,
  results: readonly PayloadRef[]
): PayloadRef<ActivityMapResultManifest<unknown>> {
  return writeMapManifest<PayloadRef, ActivityMapResultPage<unknown>>(
    name,
    results,
    (pageResults) => ({ results: pageResults })
  ) as PayloadRef<ActivityMapResultManifest<unknown>>;
}

export function encodeChildWorkflowMapResultManifest(
  name: string,
  outcomes: readonly ChildWorkflowMapItemOutcome<unknown>[]
): PayloadRef<ChildWorkflowMapResultManifest<unknown>> {
  return writeMapManifest<
    ChildWorkflowMapItemOutcome<unknown>,
    ChildWorkflowMapResultPage<unknown>
  >(name, outcomes, (pageOutcomes) => ({ outcomes: pageOutcomes })) as PayloadRef<
    ChildWorkflowMapResultManifest<unknown>
  >;
}
