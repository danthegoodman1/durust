/**
 * The provider contract: what a durable store offers the worker and the
 * client. Every provider lives in Rust and reaches TypeScript through
 * `@durust/native`; this module holds the request, outcome, and record types
 * the two sides exchange.
 */
import type {
  EventId,
  Namespace,
  RunId,
  TaskQueue,
  WorkerId,
  WorkflowId,
  WorkflowType,
  ActivityName,
  CommandId,
  TimestampMs,
  WaitId,
  SignalId,
  SignalName
} from "./types.js";
import type {
  ActivityMapTask,
  ActivityTask,
  ChildWorkflowMapTask,
  ChildWorkflowStartRequested,
  HistoryEvent,
  HistoryEventData
} from "./history.js";
import type { PayloadRef } from "./payload.js";
import type { DurableFailure } from "./api.js";

export interface DurableBackend {
  startWorkflow(req: StartWorkflowRequest): Promise<StartWorkflowOutcome>;
  /**
   * The provider's clock, and the only clock a workflow task may be prepared
   * against.
   *
   * `SPEC.md` §8.1 lists this on the backend trait; Rust has had it since the
   * trait existed and its worker reads it once per prepared workflow task.
   * TypeScript had no equivalent, so `HotWorkflowExecution` was constructed
   * with `nowMs` left at its default of `0` and every `sleep(d)` recorded an
   * absolute deadline of `d` — measured from the epoch, not from now.
   *
   * A worker must not substitute `Date.now()` for this. The deterministic
   * clock a workflow sees is part of the commit §1.2 makes normative, and a
   * provider driving a virtual clock — every simulation and the behavioural
   * corpus — is entitled to have the runtime observe it.
   */
  currentTime(): Promise<TimestampMs>;
  claimWorkflowTask(
    workerId: WorkerId | string,
    opts: ClaimWorkflowTaskOptions
  ): Promise<ClaimedWorkflowTask | null>;
  claimWorkflowTasks?(
    workerId: WorkerId | string,
    opts: ClaimWorkflowBatchOptions
  ): Promise<readonly ClaimedWorkflowTask[]>;
  streamHistory(req: StreamHistoryRequest): Promise<HistoryChunk>;
  /**
   * Applies one workflow task's writes atomically and returns the run's new
   * history tail.
   *
   * The claim token is the whole fence. Only the claim holder appends
   * replay-window events, and the paths that append one from outside a claim
   * revoke the claim in the same transaction, so a provider rejects a commit
   * whose token no longer owns the run and needs no further check. Facts
   * appended concurrently by activity workers, timer sweeps, and child
   * dispatch never invalidate a task.
   */
  commitWorkflowTask(claim: WorkflowTaskClaim, commit: WorkflowTaskCommit): Promise<EventId>;
  releaseWorkflowTask(
    claim: WorkflowTaskClaim,
    options?: ReleaseWorkflowTaskOptions
  ): Promise<void>;
  claimActivityTask(
    workerId: WorkerId | string,
    opts: ClaimActivityOptions
  ): Promise<ClaimedActivityTask | null>;
  claimActivityTasks?(
    workerId: WorkerId | string,
    opts: ClaimActivityBatchOptions
  ): Promise<readonly ClaimedActivityTask[]>;
  completeActivity(req: CompleteActivityRequest): Promise<CompleteActivityOutcome>;
  completeActivities(req: CompleteActivitiesRequest): Promise<CompleteActivitiesOutcome>;
  failActivity(req: FailActivityRequest): Promise<FailActivityOutcome>;
  heartbeatActivity(req: ActivityHeartbeatRequest): Promise<ActivityHeartbeatOutcome>;
  fireDueTimers(req: FireDueTimersRequest): Promise<FireDueTimersOutcome>;
  timeoutDueActivities(req: TimeoutDueActivitiesRequest): Promise<TimeoutDueActivitiesOutcome>;
  signalWorkflow(req: SignalWorkflowRequest): Promise<SignalWorkflowOutcome>;
  readSignalInbox(req: ReadSignalInboxRequest): Promise<SignalInboxRecord | null>;
  queryWorkflow(req: QueryWorkflowRequest): Promise<QueryWorkflowOutcome>;
  payloadRoots(): Promise<readonly unknown[]>;
}

export interface StartWorkflowRequest {
  readonly namespace: Namespace | string;
  readonly workflowId: WorkflowId | string;
  readonly workflowType: WorkflowType;
  readonly taskQueue: TaskQueue | string;
  readonly input: PayloadRef;
}

export type StartWorkflowOutcome =
  | { readonly kind: "Started"; readonly runId: RunId }
  | { readonly kind: "AlreadyStarted"; readonly runId: RunId };

export interface ClaimWorkflowTaskOptions {
  readonly namespace: Namespace | string;
  readonly taskQueue: TaskQueue | string;
  readonly registeredWorkflowTypes: readonly WorkflowType[];
  readonly leaseDurationMs: number;
}

export interface ClaimWorkflowBatchOptions extends ClaimWorkflowTaskOptions {
  readonly limit: number;
}

export type WorkflowTaskReason =
  | "WorkflowStarted"
  | "ActivityCompleted"
  | "ActivityFailed"
  | "ActivityTimedOut"
  | "ActivityMapCompleted"
  | "ActivityMapFailed"
  | "ChildWorkflowStarted"
  | "ChildWorkflowCompleted"
  | "ChildWorkflowFailed"
  | "ChildWorkflowCancelled"
  | "ChildWorkflowMapCompleted"
  | "ChildWorkflowMapFailed"
  | "TimerFired"
  | "SignalReceived"
  | "CacheEvicted";

export interface WorkflowTaskClaim {
  readonly runId: RunId;
  readonly workerId: WorkerId | string;
  readonly token: number;
}

export interface ClaimedWorkflowTask {
  readonly runId: RunId;
  readonly workflowId: WorkflowId | string;
  readonly workflowType: WorkflowType;
  readonly claim: WorkflowTaskClaim;
  readonly replayTargetEventId: EventId;
  readonly reason: WorkflowTaskReason;
  readonly prefetchedHistory: readonly HistoryEvent[];
  /**
   * The first unconsumed inbox record of every signal name the run holds at
   * claim time, in received order. The runtime takes the names it waits on
   * and ignores the rest, so a worker needs no registered-signal list.
   */
  readonly liveSignals: readonly SignalInboxRecord[];
}

export interface StreamHistoryRequest {
  readonly runId: RunId;
  readonly afterEventId: EventId;
  readonly upToEventId: EventId;
  readonly maxEvents: number;
  readonly maxBytes: number;
}

export interface HistoryChunk {
  readonly events: readonly HistoryEvent[];
  readonly lastEventId: EventId;
  readonly hasMore: boolean;
}

export interface NewHistoryEvent {
  readonly data: HistoryEventData;
}

export interface WorkflowTaskCommit {
  readonly appendEvents?: readonly NewHistoryEvent[];
  readonly upsertWaits?: readonly WaitRecord[];
  readonly deleteWaits?: readonly WaitId[];
  readonly consumeSignals?: readonly (SignalId | string)[];
  readonly scheduleActivities?: readonly ActivityTask[];
  readonly scheduleActivityMaps?: readonly ActivityMapTask[];
  readonly startChildWorkflows?: readonly ChildWorkflowStartRequested[];
  readonly scheduleChildWorkflowMaps?: readonly ChildWorkflowMapTask[];
  /**
   * Commands whose operational state this commit withdraws: a scheduled
   * activity that no longer has a waiter, or a map whose fanout the workflow
   * has stopped caring about.
   *
   * `SPEC.md` §8.2 lists this field and §1.2 makes every §8.2 field normative,
   * so its absence here was a gap in TypeScript rather than a narrowing of the
   * contract — and it was the reason `map-engine.ts`'s `ParentCancelled`
   * transition had no producer at all. Cancelling a map command tombstones its
   * pending items and closes its descriptor; no parent fact is appended,
   * because the commit that carries the cancellation is already the record of
   * it.
   */
  readonly cancelCommands?: readonly CommandId[];
  readonly queryProjection?: PayloadRef;
}

export interface ReleaseWorkflowTaskOptions {
  readonly visibilityDelayMs?: number;
}

export function workflowTaskCommitHasWorkflowVisibleMutations(
  commit: WorkflowTaskCommit
): boolean {
  return (
    (commit.appendEvents?.length ?? 0) > 0 ||
    (commit.upsertWaits?.length ?? 0) > 0 ||
    (commit.deleteWaits?.length ?? 0) > 0 ||
    (commit.consumeSignals?.length ?? 0) > 0 ||
    (commit.scheduleActivities?.length ?? 0) > 0 ||
    (commit.scheduleActivityMaps?.length ?? 0) > 0 ||
    (commit.startChildWorkflows?.length ?? 0) > 0 ||
    (commit.scheduleChildWorkflowMaps?.length ?? 0) > 0 ||
    (commit.cancelCommands?.length ?? 0) > 0 ||
    commit.queryProjection !== undefined
  );
}

export interface ClaimActivityOptions {
  readonly namespace: Namespace | string;
  readonly taskQueue: TaskQueue | string;
  readonly registeredActivityNames: readonly (ActivityName | string)[];
  readonly leaseDurationMs: number;
}

export interface ClaimActivityBatchOptions extends ClaimActivityOptions {
  readonly limit: number;
}

export interface ActivityTaskClaim {
  readonly activityId: string;
  readonly workerId: WorkerId | string;
  readonly token: number;
}

export interface ClaimedActivityTask {
  readonly task: ActivityTask;
  readonly claim: ActivityTaskClaim;
}

export interface CompleteActivityRequest {
  readonly claim: ActivityTaskClaim;
  readonly result: PayloadRef;
}

export type CompleteActivityOutcome =
  | { readonly kind: "Completed"; readonly eventId: EventId }
  | { readonly kind: "AlreadyCompleted" };

export interface CompleteActivitiesRequest {
  readonly completions: readonly CompleteActivityRequest[];
}

export type CompleteActivityItemOutcome =
  | CompleteActivityOutcome
  | { readonly kind: "StaleLease" }
  | { readonly kind: "NotFound" };

export interface CompleteActivitiesOutcome {
  readonly results: readonly CompleteActivityItemOutcome[];
}

export interface FailActivityRequest {
  readonly claim: ActivityTaskClaim;
  readonly failure: DurableFailure;
}

export type FailActivityOutcome =
  | { readonly kind: "Failed"; readonly eventId: EventId }
  | { readonly kind: "RetryScheduled"; readonly attempt: number; readonly readyAtMs: number }
  | { readonly kind: "AlreadyCompleted" };

export interface ActivityHeartbeatRequest {
  readonly claim: ActivityTaskClaim;
}

export type ActivityHeartbeatOutcome =
  | { readonly kind: "Recorded" }
  | { readonly kind: "AlreadyCompleted" };

export type WaitKind = "Timer" | "Signal";

export interface WaitRecord {
  readonly waitId: WaitId | string;
  readonly runId: RunId;
  readonly commandId: CommandId;
  readonly kind: WaitKind;
  readonly key: string;
  readonly readyAt: TimestampMs | null;
}

export interface FireDueTimersRequest {
  readonly namespace: Namespace | string;
  readonly now: TimestampMs | number;
  readonly limit: number;
}

export interface FireDueTimersOutcome {
  readonly fired: number;
}

export interface TimeoutDueActivitiesRequest {
  readonly namespace: Namespace | string;
  readonly now: TimestampMs | number;
  readonly limit: number;
}

export interface TimeoutDueActivitiesOutcome {
  readonly timedOut: number;
}

export interface SignalWorkflowRequest {
  readonly namespace: Namespace | string;
  readonly workflowId: WorkflowId | string;
  readonly signalId: SignalId | string;
  readonly signalName: SignalName | string;
  readonly payload: PayloadRef;
}

export type SignalWorkflowOutcome =
  | { readonly kind: "Accepted" }
  | { readonly kind: "Duplicate" };

export interface ReadSignalInboxRequest {
  readonly runId: RunId;
  readonly signalName: SignalName | string;
}

export interface SignalInboxRecord {
  readonly signalId: SignalId | string;
  readonly signalName: SignalName | string;
  readonly payload: PayloadRef;
}

export interface QueryWorkflowRequest {
  readonly namespace: Namespace | string;
  readonly workflowId: WorkflowId | string;
}

export type QueryWorkflowOutcome =
  | { readonly kind: "Found"; readonly projection: PayloadRef }
  | { readonly kind: "NotFound" }
  | { readonly kind: "NoProjection" };
