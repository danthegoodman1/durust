import {
  eventId,
  runId,
  type EventId,
  type Namespace,
  type RunId,
  type TaskQueue,
  type WorkerId,
  type WorkflowId,
  type WorkflowType,
  type ActivityName,
  type CommandId,
  type TimestampMs,
  type WaitId,
  type SignalId,
  type SignalName
} from "./types.js";
import { commandKey, sameCommandId } from "./internal.js";
import {
  activityOutcomeCounts,
  itemRetryDecision,
  itemRetryDelayMs,
  mapCommandCancelledReason,
  mapRejectMessage,
  outcomeCounts,
  recordedOutcomeCount,
  step as stepMap,
  type ItemAttemptFailureKind,
  type ItemRetryDecision,
  type MapEffect,
  type MapEvent,
  type MapState
} from "./map-engine.js";
import type {
  ActivityMapTask,
  ActivityTask,
  ChildWorkflowMapTask,
  ChildWorkflowStartRequested,
  HistoryEvent,
  HistoryEventData
} from "./history.js";
import { historyEventType } from "./history.js";
import type { PayloadRef } from "./payload.js";
import { completeMapItems, readMapManifestItems, writeMapManifest } from "./map-manifest.js";
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

export interface DurableBackend {
  startWorkflow(req: StartWorkflowRequest): Promise<StartWorkflowOutcome>;
  claimWorkflowTask(
    workerId: WorkerId | string,
    opts: ClaimWorkflowTaskOptions
  ): Promise<ClaimedWorkflowTask | null>;
  claimWorkflowTasks?(
    workerId: WorkerId | string,
    opts: ClaimWorkflowBatchOptions
  ): Promise<readonly ClaimedWorkflowTask[]>;
  streamHistory(req: StreamHistoryRequest): Promise<HistoryChunk>;
  commitWorkflowTask(
    claim: WorkflowTaskClaim,
    commit: WorkflowTaskCommit
  ): Promise<CommitOutcome>;
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
  readonly registeredSignalNames?: readonly (SignalName | string)[];
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
  readonly liveSignals?: readonly SignalInboxRecord[];
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
  readonly expectedTailEventId: EventId;
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

export type CommitOutcome =
  | { readonly kind: "Committed"; readonly newTailEventId: EventId }
  | { readonly kind: "Conflict" };

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

interface WorkflowState {
  readonly namespace: string;
  readonly workflowId: string;
  readonly workflowType: WorkflowType;
  readonly taskQueue: string;
  readonly runId: RunId;
  history: HistoryEvent[];
  readyReason: WorkflowTaskReason | null;
  readyAtMs: number;
  claim: WorkflowLease | null;
  queryProjection: PayloadRef | null;
  terminal: boolean;
  parent: ParentWorkflowLink | null;
}

interface WorkflowLease {
  readonly claim: WorkflowTaskClaim;
  readonly reason: WorkflowTaskReason;
  readonly expiresAtMs: number;
}

type ParentWorkflowLink = ChildParentWorkflowLink | ChildWorkflowMapParentLink;

interface ChildParentWorkflowLink {
  readonly kind: "Child";
  readonly parentRunId: RunId;
  readonly commandId: CommandId;
  readonly parentClosePolicy: string;
}

interface ChildWorkflowMapParentLink {
  readonly kind: "ChildWorkflowMap";
  readonly parentRunId: RunId;
  readonly mapCommandId: CommandId;
  readonly itemOrdinal: number;
  readonly parentClosePolicy: string;
}

interface ActivityState {
  readonly namespace: string;
  readonly workflow: WorkflowState;
  task: ActivityTask;
  claim: ActivityLease | null;
  availableAtMs: number;
  terminalEventId: EventId | null;
}

interface ActivityLease {
  readonly claim: ActivityTaskClaim;
  readonly startedAtMs: number;
  readonly heartbeatDeadlineAtMs: number | null;
  readonly expiresAtMs: number;
  readonly leaseDurationMs: number;
}

interface ActivityMapState {
  readonly namespace: string;
  readonly workflow: WorkflowState;
  readonly task: ActivityMapTask;
  readonly inputs: readonly PayloadRef[];
  readonly results: (PayloadRef | null)[];
  /**
   * Slots taken by admitted, not-yet-terminal items. Written only by
   * `AdvanceDescriptor` and `MarkDescriptorTerminal`, never by the provider,
   * so the engine's admission accounting has exactly one owner.
   */
  inFlight: number;
  nextOrdinal: number;
  terminal: boolean;
}

interface ChildWorkflowMapState {
  readonly namespace: string;
  readonly workflow: WorkflowState;
  readonly task: ChildWorkflowMapTask;
  readonly inputs: readonly PayloadRef[];
  readonly outcomes: (ChildWorkflowMapItemOutcome<unknown> | null)[];
  inFlight: number;
  nextOrdinal: number;
  terminal: boolean;
}

interface SignalState {
  readonly runId: RunId;
  readonly signalName: SignalName | string;
  readonly payload: PayloadRef;
  readonly receivedSequence: number;
  consumed: boolean;
}

export interface MemoryBackendOptions {
  readonly nowMs?: () => number;
}

export class MemoryBackend implements DurableBackend {
  readonly #workflowsById = new Map<string, WorkflowState>();
  readonly #workflowsByRun = new Map<string, WorkflowState>();
  readonly #activitiesById = new Map<string, ActivityState>();
  readonly #activityMapsByCommand = new Map<string, ActivityMapState>();
  readonly #childWorkflowMapsByCommand = new Map<string, ChildWorkflowMapState>();
  readonly #waitsById = new Map<string, WaitRecord>();
  readonly #signalsById = new Map<string, SignalState>();
  readonly #nowMs: () => number;
  #nextRun = 1;
  #nextClaimToken = 1;
  #nextActivityClaimToken = 1;
  #nextSignalSequence = 1;

  constructor(options: MemoryBackendOptions = {}) {
    this.#nowMs = options.nowMs ?? Date.now;
  }

  async startWorkflow(req: StartWorkflowRequest): Promise<StartWorkflowOutcome> {
    const key = workflowKey(req.namespace, req.workflowId);
    const existing = this.#workflowsById.get(key);
    if (existing) {
      return { kind: "AlreadyStarted", runId: existing.runId };
    }

    const newRunId = runId(`run-${this.#nextRun++}`);
    const started = makeHistoryEvent(eventId(1), {
      kind: "WorkflowStarted",
      workflowType: req.workflowType,
      input: req.input
    });
    const state: WorkflowState = {
      namespace: String(req.namespace),
      workflowId: String(req.workflowId),
      workflowType: req.workflowType,
      taskQueue: String(req.taskQueue),
      runId: newRunId,
      history: [started],
      readyReason: "WorkflowStarted",
      readyAtMs: 0,
      claim: null,
      queryProjection: null,
      terminal: false,
      parent: null
    };
    this.#workflowsById.set(key, state);
    this.#workflowsByRun.set(newRunId, state);
    return { kind: "Started", runId: newRunId };
  }

  async claimWorkflowTask(
    workerId: WorkerId | string,
    opts: ClaimWorkflowTaskOptions
  ): Promise<ClaimedWorkflowTask | null> {
    const eligibleTypes = new Set(
      opts.registeredWorkflowTypes.map((workflowType) => workflowTypeKey(workflowType))
    );
    for (const state of this.#workflowsById.values()) {
      this.#restoreExpiredWorkflowLease(state);
      if (
        state.namespace !== String(opts.namespace) ||
        state.taskQueue !== String(opts.taskQueue) ||
        state.terminal ||
        (state.readyReason === null && state.claim === null) ||
        state.claim !== null ||
        state.readyAtMs > this.#nowMs() ||
        !eligibleTypes.has(workflowTypeKey(state.workflowType))
      ) {
        continue;
      }

      const reason = state.readyReason;
      if (reason === null) {
        continue;
      }
      const claim: WorkflowTaskClaim = {
        runId: state.runId,
        workerId,
        token: this.#nextClaimToken++
      };
      state.claim = {
        claim,
        reason,
        expiresAtMs: this.#leaseExpiresAt(opts.leaseDurationMs)
      };
      state.readyReason = null;
      state.readyAtMs = 0;
      return {
        runId: state.runId,
        workflowId: state.workflowId,
        workflowType: state.workflowType,
        claim,
        replayTargetEventId: tailEventId(state),
        reason,
        prefetchedHistory: [...state.history]
      };
    }
    return null;
  }

  async streamHistory(req: StreamHistoryRequest): Promise<HistoryChunk> {
    const state = this.#stateForRun(req.runId);
    const matching = state.history.filter(
      (event) => event.eventId > req.afterEventId && event.eventId <= req.upToEventId
    );
    const events = matching.slice(0, Math.max(0, req.maxEvents));
    const lastEvent = events.at(-1);
    return {
      events,
      lastEventId: lastEvent?.eventId ?? req.afterEventId,
      hasMore: events.length < matching.length
    };
  }

  async commitWorkflowTask(
    claim: WorkflowTaskClaim,
    commit: WorkflowTaskCommit
  ): Promise<CommitOutcome> {
    const state = this.#stateForRun(claim.runId);
    this.#restoreExpiredWorkflowLease(state);
    if (
      state.claim === null ||
      !workflowLeaseMatches(state.claim, claim)
    ) {
      throw new Error("stale workflow task lease");
    }
    if (state.terminal && workflowTaskCommitHasWorkflowVisibleMutations(commit)) {
      throw new Error("terminal workflow rejects workflow-visible mutations");
    }

    if (commit.expectedTailEventId !== tailEventId(state)) {
      state.claim = null;
      state.readyReason = "CacheEvicted";
      state.readyAtMs = 0;
      return { kind: "Conflict" };
    }

    let continuedInput: PayloadRef | null = null;
    let childTerminal: ChildTerminalUpdate | null = null;
    for (const event of commit.appendEvents ?? []) {
      const nextEventId = eventId(Number(tailEventId(state)) + 1);
      state.history.push(makeHistoryEvent(nextEventId, event.data));
      if (event.data.kind === "WorkflowCompleted") {
        state.terminal = true;
        childTerminal = { kind: "Completed", result: event.data.result };
      }
      if (event.data.kind === "WorkflowFailed") {
        state.terminal = true;
        childTerminal = { kind: "Failed", failure: event.data.failure };
      }
      if (event.data.kind === "WorkflowCancelled") {
        state.terminal = true;
        childTerminal = { kind: "Cancelled", reason: event.data.reason };
      }
      if (event.data.kind === "WorkflowContinuedAsNew") {
        state.terminal = true;
        continuedInput = event.data.input;
      }
    }
    for (const wait of commit.upsertWaits ?? []) {
      this.#waitsById.set(String(wait.waitId), wait);
    }
    for (const waitId of commit.deleteWaits ?? []) {
      this.#waitsById.delete(String(waitId));
    }
    for (const signalId of commit.consumeSignals ?? []) {
      const signal = this.#signalsById.get(String(signalId));
      if (signal) {
        signal.consumed = true;
      }
    }
    for (const cancelled of commit.cancelCommands ?? []) {
      this.#cancelCommandOperationalState(cancelled);
    }
    for (const task of commit.scheduleActivities ?? []) {
      const activityState: ActivityState = {
        namespace: state.namespace,
        workflow: state,
        task,
        claim: null,
        availableAtMs: 0,
        terminalEventId: null
      };
      this.#activitiesById.set(task.activityId, activityState);
    }
    for (const task of commit.scheduleActivityMaps ?? []) {
      this.#createActivityMap(state, task);
    }
    for (const child of commit.startChildWorkflows ?? []) {
      this.#startChildWorkflow(state, child);
    }
    for (const task of commit.scheduleChildWorkflowMaps ?? []) {
      this.#createChildWorkflowMap(state, task);
    }
    if (commit.queryProjection !== undefined) {
      state.queryProjection = commit.queryProjection;
    }

    state.claim = null;
    this.#wakeIfSignalWaitReady(state);
    if (continuedInput !== null) {
      this.#startContinuedRun(state, continuedInput);
    }
    if (childTerminal !== null && state.parent !== null) {
      this.#notifyParentOfChildTerminal(state.parent, childTerminal);
    }
    if (state.terminal) {
      this.#abandonWorkForClosedRun(state);
      this.#cancelChildrenForClosedParent(state);
    }
    return { kind: "Committed", newTailEventId: tailEventId(state) };
  }

  async releaseWorkflowTask(
    claim: WorkflowTaskClaim,
    options: ReleaseWorkflowTaskOptions = {}
  ): Promise<void> {
    const state = this.#stateForRun(claim.runId);
    this.#restoreExpiredWorkflowLease(state);
    if (state.claim === null || !workflowLeaseMatches(state.claim, claim)) {
      return;
    }
    state.readyReason = state.claim.reason;
    state.readyAtMs = this.#nowMs() + Math.max(0, options.visibilityDelayMs ?? 0);
    state.claim = null;
  }

  async claimActivityTask(
    workerId: WorkerId | string,
    opts: ClaimActivityOptions
  ): Promise<ClaimedActivityTask | null> {
    const eligible = new Set(opts.registeredActivityNames.map(String));
    for (const activity of this.#activitiesById.values()) {
      this.#restoreExpiredActivityLease(activity);
      if (
        activity.namespace !== String(opts.namespace) ||
        String(activity.task.taskQueue) !== String(opts.taskQueue) ||
        !eligible.has(String(activity.task.activityName)) ||
        activity.availableAtMs > this.#nowMs() ||
        activity.claim !== null ||
        activity.terminalEventId !== null ||
        this.#activityMapForTask(activity.task)?.terminal === true
      ) {
        continue;
      }

      const claim: ActivityTaskClaim = {
        activityId: activity.task.activityId,
        workerId,
        token: this.#nextActivityClaimToken++
      };
      const now = this.#nowMs();
      activity.claim = {
        claim,
        startedAtMs: now,
        heartbeatDeadlineAtMs: activityHeartbeatDeadlineAt(
          activity.task,
          now,
          opts.leaseDurationMs
        ),
        expiresAtMs: this.#leaseExpiresAt(opts.leaseDurationMs),
        leaseDurationMs: Math.max(0, opts.leaseDurationMs)
      };
      return { task: activity.task, claim };
    }
    return null;
  }

  async completeActivity(req: CompleteActivityRequest): Promise<CompleteActivityOutcome> {
    const outcome = this.#completeActivityItem(req);
    if (outcome.kind === "NotFound") {
      throw new Error(`activity task not found: ${req.claim.activityId}`);
    }
    if (outcome.kind === "StaleLease") {
      throw new Error("stale activity task lease");
    }
    return outcome;
  }

  async completeActivities(req: CompleteActivitiesRequest): Promise<CompleteActivitiesOutcome> {
    return {
      results: req.completions.map((completion) => this.#completeActivityItem(completion))
    };
  }

  #completeActivityItem(req: CompleteActivityRequest): CompleteActivityItemOutcome {
    const activity = this.#activitiesById.get(req.claim.activityId);
    if (!activity) {
      return { kind: "NotFound" };
    }
    this.#restoreExpiredActivityLease(activity);
    if (activity.terminalEventId !== null) {
      return { kind: "AlreadyCompleted" };
    }
    if (
      activity.claim === null ||
      !activityLeaseMatches(activity.claim, req.claim)
    ) {
      return { kind: "StaleLease" };
    }

    if (activity.task.mapItem !== null) {
      const eventId = this.#completeActivityMapItem(activity, req.result);
      return { kind: "Completed", eventId };
    }

    const event = makeHistoryEvent(eventId(Number(tailEventId(activity.workflow)) + 1), {
      kind: "ActivityCompleted",
      completed: {
        commandId: activity.task.commandId,
        result: req.result
      }
    });
    activity.workflow.history.push(event);
    markWorkflowReady(activity.workflow, "ActivityCompleted");
    activity.terminalEventId = event.eventId;
    activity.claim = null;
    return { kind: "Completed", eventId: event.eventId };
  }

  async failActivity(req: FailActivityRequest): Promise<FailActivityOutcome> {
    const activity = this.#activitiesById.get(req.claim.activityId);
    if (!activity) {
      throw new Error(`activity task not found: ${req.claim.activityId}`);
    }
    this.#restoreExpiredActivityLease(activity);
    if (activity.terminalEventId !== null) {
      return { kind: "AlreadyCompleted" };
    }
    if (
      activity.claim === null ||
      !activityLeaseMatches(activity.claim, req.claim)
    ) {
      throw new Error("stale activity task lease");
    }

    // A map item takes the engine route for *both* verdicts. Deciding
    // retry-versus-exhaust here and only then dispatching to the map path
    // would leave the engine's `ScheduleItemRetry` with no producer, and would
    // reschedule an item onto a map that had already ended.
    if (activity.task.mapItem !== null) {
      return this.#failActivityMapItem(
        activity,
        req.failure,
        itemRetryDecision(activity.task.attempt, activity.task.retryPolicy, req.failure)
      );
    }

    const retry = retryActivityAfterFailure(activity, req.failure, this.#nowMs());
    if (retry !== null) {
      activity.task = retry.task;
      activity.availableAtMs = retry.readyAtMs;
      activity.claim = null;
      return {
        kind: "RetryScheduled",
        attempt: retry.task.attempt,
        readyAtMs: retry.readyAtMs
      };
    }

    const event = makeHistoryEvent(eventId(Number(tailEventId(activity.workflow)) + 1), {
      kind: "ActivityFailed",
      failed: {
        commandId: activity.task.commandId,
        failure: req.failure
      }
    });
    activity.workflow.history.push(event);
    markWorkflowReady(activity.workflow, "ActivityFailed");
    activity.terminalEventId = event.eventId;
    activity.claim = null;
    return { kind: "Failed", eventId: event.eventId };
  }

  async heartbeatActivity(req: ActivityHeartbeatRequest): Promise<ActivityHeartbeatOutcome> {
    const activity = this.#activitiesById.get(req.claim.activityId);
    if (!activity) {
      throw new Error(`activity task not found: ${req.claim.activityId}`);
    }
    this.#restoreExpiredActivityLease(activity);
    if (activity.terminalEventId !== null) {
      return { kind: "AlreadyCompleted" };
    }
    if (
      activity.claim === null ||
      !activityLeaseMatches(activity.claim, req.claim)
    ) {
      throw new Error("stale activity task lease");
    }
    const currentClaim = activity.claim;
    const now = this.#nowMs();
    activity.claim = {
      ...currentClaim,
      heartbeatDeadlineAtMs: activityHeartbeatDeadlineAt(
        activity.task,
        now,
        currentClaim.leaseDurationMs
      ),
      expiresAtMs: now + currentClaim.leaseDurationMs
    };
    return { kind: "Recorded" };
  }

  #leaseExpiresAt(leaseDurationMs: number): number {
    return this.#nowMs() + Math.max(0, leaseDurationMs);
  }

  #restoreExpiredWorkflowLease(state: WorkflowState): void {
    if (state.claim !== null && state.claim.expiresAtMs <= this.#nowMs()) {
      state.readyReason ??= state.claim.reason;
      state.readyAtMs = 0;
      state.claim = null;
    }
  }

  #restoreExpiredActivityLease(activity: ActivityState): void {
    if (activity.claim !== null && activity.claim.expiresAtMs <= this.#nowMs()) {
      const retry = retryActivityAfterTimeout(activity, this.#nowMs());
      if (retry === null) {
        activity.claim = null;
        return;
      }
      activity.task = retry.task;
      activity.availableAtMs = retry.readyAtMs;
      activity.claim = null;
    }
  }

  async fireDueTimers(req: FireDueTimersRequest): Promise<FireDueTimersOutcome> {
    const due = [...this.#waitsById.values()]
      .filter(
        (wait) =>
          wait.kind === "Timer" &&
          wait.readyAt !== null &&
          Number(wait.readyAt) <= Number(req.now)
      )
      .slice(0, Math.max(1, req.limit));
    let fired = 0;
    for (const wait of due) {
      const state = this.#workflowsByRun.get(wait.runId);
      if (!state || state.namespace !== String(req.namespace)) {
        this.#waitsById.delete(String(wait.waitId));
        continue;
      }
      const event = makeHistoryEvent(eventId(Number(tailEventId(state)) + 1), {
        kind: "TimerFired",
        fired: {
          commandId: wait.commandId,
          firedAt: req.now as TimestampMs
        }
      });
      state.history.push(event);
      markWorkflowReady(state, "TimerFired");
      this.#waitsById.delete(String(wait.waitId));
      fired += 1;
    }
    return { fired };
  }

  async timeoutDueActivities(req: TimeoutDueActivitiesRequest): Promise<TimeoutDueActivitiesOutcome> {
    const due = [...this.#activitiesById.values()]
      .filter(
        (activity) =>
          activity.namespace === String(req.namespace) &&
          activityTimeoutDeadline(activity).deadline <= Number(req.now)
      )
      .sort((left, right) =>
        activityTimeoutDeadline(left).deadline - activityTimeoutDeadline(right).deadline ||
        left.task.activityId.localeCompare(right.task.activityId)
      )
      .slice(0, Math.max(1, req.limit));
    let timedOut = 0;
    for (const activity of due) {
      if (activity.claim === null || activity.terminalEventId !== null) {
        continue;
      }
      const timeout = activityTimeoutDeadline(activity);
      if (timeout.deadline > Number(req.now)) {
        continue;
      }
      // A map item's lapsed deadline is an engine event, not a local
      // reschedule: only the engine knows whether the map is still running,
      // and it owns both the retried attempt and the map's terminal failure.
      if (activity.task.mapItem !== null) {
        this.#failActivityMapItem(
          activity,
          activityTimeoutFailure(activity, timeout.kind),
          // A lapsed deadline is not the attempt's own verdict, so the policy's
          // non-retryable rules do not apply to it.
          itemRetryDecision(activity.task.attempt, activity.task.retryPolicy, null),
          "TimedOut"
        );
        timedOut += 1;
        continue;
      }
      const retry = retryActivityAfterTimeout(activity, Number(req.now));
      if (retry !== null) {
        activity.task = retry.task;
        activity.availableAtMs = retry.readyAtMs;
        activity.claim = null;
        timedOut += 1;
        continue;
      }
      const event = makeHistoryEvent(eventId(Number(tailEventId(activity.workflow)) + 1), {
        kind: "ActivityTimedOut",
        timedOut: {
          commandId: activity.task.commandId,
          message: activityTimeoutMessage(activity, timeout.kind)
        }
      });
      activity.workflow.history.push(event);
      markWorkflowReady(activity.workflow, "ActivityTimedOut");
      activity.terminalEventId = event.eventId;
      activity.claim = null;
      timedOut += 1;
    }
    return { timedOut };
  }

  async signalWorkflow(req: SignalWorkflowRequest): Promise<SignalWorkflowOutcome> {
    const signalKey = String(req.signalId);
    if (this.#signalsById.has(signalKey)) {
      return { kind: "Duplicate" };
    }

    const state = this.#workflowsById.get(workflowKey(req.namespace, req.workflowId));
    if (!state) {
      throw new Error(`workflow not found: ${req.workflowId}`);
    }

    this.#signalsById.set(signalKey, {
      runId: state.runId,
      signalName: req.signalName,
      payload: req.payload,
      receivedSequence: this.#nextSignalSequence++,
      consumed: false
    });
    this.#wakeIfSignalWaitReady(state);
    return { kind: "Accepted" };
  }

  async readSignalInbox(req: ReadSignalInboxRequest): Promise<SignalInboxRecord | null> {
    const matching = [...this.#signalsById.entries()]
      .filter(
        ([, signal]) =>
          signal.runId === req.runId &&
          String(signal.signalName) === String(req.signalName) &&
          !signal.consumed
      )
      .sort(([, left], [, right]) => left.receivedSequence - right.receivedSequence);
    const first = matching[0];
    if (!first) {
      return null;
    }
    const [signalId, signal] = first;
    return {
      signalId,
      signalName: signal.signalName,
      payload: signal.payload
    };
  }

  async queryWorkflow(req: QueryWorkflowRequest): Promise<QueryWorkflowOutcome> {
    const state = this.#workflowsById.get(workflowKey(req.namespace, req.workflowId));
    if (!state) {
      return { kind: "NotFound" };
    }
    if (state.queryProjection === null) {
      return { kind: "NoProjection" };
    }
    return { kind: "Found", projection: state.queryProjection };
  }

  async payloadRoots(): Promise<readonly unknown[]> {
    return [
      ...this.#workflowsByRun.values(),
      ...this.#activitiesById.values(),
      ...this.#activityMapsByCommand.values(),
      ...this.#childWorkflowMapsByCommand.values(),
      ...this.#signalsById.values()
    ];
  }

  #stateForRun(id: RunId): WorkflowState {
    const state = this.#workflowsByRun.get(id);
    if (!state) {
      throw new Error(`workflow run not found: ${id}`);
    }
    return state;
  }

  #wakeIfSignalWaitReady(state: WorkflowState): void {
    const ready = [...this.#waitsById.values()].some(
      (wait) =>
        wait.runId === state.runId &&
        wait.kind === "Signal" &&
        [...this.#signalsById.values()].some(
          (signal) =>
            signal.runId === state.runId &&
            String(signal.signalName) === wait.key &&
            !signal.consumed
        )
    );
    if (ready) {
        markWorkflowReady(state, "SignalReceived");
    }
  }

  // ---------------------------------------------------------------------
  // Map fanout. Every decision below — what to admit, when the bound is
  // reached, whether a failed attempt retries, which failure mode ends the
  // map, when the parent is notified — belongs to `map-engine.ts`. What is
  // left here is storage: insert item tasks, start item children, write the
  // descriptor cursor, append a parent event, tombstone leftovers.
  // ---------------------------------------------------------------------

  /**
   * `WorkflowTaskCommit.cancelCommands`: withdraw one command's operational
   * state. A plain activity is tombstoned; a map is handed to the engine as
   * `ParentCancelled`, which tombstones its pending work and closes the
   * descriptor. Routing the map through the engine rather than deleting the
   * descriptor here is what keeps the cancellation path from drifting away
   * from the fail-fast one.
   */
  #cancelCommandOperationalState(cancelled: CommandId): void {
    for (const activity of this.#activitiesById.values()) {
      if (
        activity.task.mapItem === null &&
        sameCommandId(activity.task.commandId, cancelled) &&
        activity.terminalEventId === null
      ) {
        activity.terminalEventId = tailEventId(activity.workflow);
        activity.claim = null;
      }
    }
    const activityMap = this.#activityMapsByCommand.get(commandKey(cancelled));
    if (activityMap !== undefined) {
      this.#stepActivityMap(activityMap, { kind: "ParentCancelled" });
    }
    const childMap = this.#childWorkflowMapsByCommand.get(commandKey(cancelled));
    if (childMap !== undefined && !childMap.terminal) {
      // Withdrawing the command on a *live* run means the `parentClosePolicy`
      // path never runs, so without this the children the map already started
      // keep running with nothing waiting for them.
      //
      // Children first, descriptor last, matching the fail-fast effect order.
      // That is a consistency choice and not a constraint: the reverse order
      // was measured green across the whole memory and SQLite conformance
      // suite, because child cancellation filters on the child's parent link
      // and its own terminal flag and never reads the descriptor's.
      this.#cancelRunningChildWorkflowMapItems(
        childMap,
        mapCommandCancelledReason(childMap.task.mapCommandId)
      );
      this.#stepChildWorkflowMap(childMap, { kind: "ParentCancelled" });
    }
  }

  /**
   * Abandon-on-close: a run that has just reached a terminal event owns no
   * live work any more. Every map of that run is handed to the engine as
   * `ParentCancelled`, and every plain activity of it is tombstoned.
   *
   * Without this a closed workflow keeps spawning and appending: completing an
   * item of its map materialized the next ordinal, and both a map's terminal
   * fact and a plain activity's `ActivityCompleted` were appended to the run's
   * history *after* its terminal event — the exact thing the terminal-commit
   * guard exists to prevent. Rust reaches the same end by deleting the run's
   * activities and descriptors during terminal cleanup.
   *
   * Children are deliberately left alone here. `ParentCancelled` emits only
   * `AbandonPendingItems` and `MarkDescriptorTerminal` — in both runtimes — so
   * this cannot cascade, and a closed run's children belong to the
   * `parentClosePolicy` path, which must stay free to *abandon* them rather
   * than cancel them. The other `ParentCancelled` producer, a withdrawn
   * command on a still-live run, has no such path and cancels the map's
   * children itself; see `#cancelCommandOperationalState`.
   */
  #abandonWorkForClosedRun(state: WorkflowState): void {
    for (const activity of this.#activitiesById.values()) {
      if (
        activity.task.mapItem === null &&
        activity.workflow.runId === state.runId &&
        activity.terminalEventId === null
      ) {
        activity.terminalEventId = tailEventId(state);
        activity.claim = null;
      }
    }
    for (const map of this.#activityMapsByCommand.values()) {
      if (map.workflow.runId === state.runId && !map.terminal) {
        this.#stepActivityMap(map, { kind: "ParentCancelled" });
      }
    }
    for (const map of this.#childWorkflowMapsByCommand.values()) {
      if (map.workflow.runId === state.runId && !map.terminal) {
        this.#stepChildWorkflowMap(map, { kind: "ParentCancelled" });
      }
    }
  }

  #createActivityMap(workflow: WorkflowState, task: ActivityMapTask): void {
    // Validation runs before the engine is consulted. The engine clamps a
    // degenerate bound so an already-persisted descriptor cannot stall, but a
    // caller typo must still be rejected at the scheduling boundary rather
    // than silently reinterpreted as one.
    if (task.maxInFlight <= 0 || !Number.isInteger(task.maxInFlight)) {
      throw new Error("activity map maxInFlight must be a positive integer");
    }
    const inputs = decodeActivityMapInputs(task.inputManifest);
    const map: ActivityMapState = {
      namespace: workflow.namespace,
      workflow,
      task,
      inputs,
      results: Array.from({ length: inputs.length }, () => null),
      inFlight: 0,
      nextOrdinal: 0,
      terminal: false
    };
    this.#activityMapsByCommand.set(commandKey(task.mapCommandId), map);
    this.#stepActivityMap(map, {
      kind: "DescriptorCreated",
      parentTerminal: workflow.terminal
    });
  }

  /** The activity-map descriptor as `map-engine.ts` sees it. */
  #activityMapEngineState(map: ActivityMapState): MapState {
    return {
      mapCommandId: map.task.mapCommandId,
      kind: "Activity",
      // An activity map is always fail-fast; the field is inert for it.
      failureMode: "FailFast",
      itemCount: map.inputs.length,
      nextOrdinal: map.nextOrdinal,
      inFlight: map.inFlight,
      maxInFlight: map.task.maxInFlight,
      recordedOutcomes: recordedOutcomeCount(map.results),
      completed: map.terminal
    };
  }

  /**
   * Run one activity-map transition: project the descriptor, ask the engine,
   * apply the effects it returns. Returns the parent event the transition
   * appended, if any.
   */
  #stepActivityMap(map: ActivityMapState, event: MapEvent): EventId | null {
    const transition = stepMap(this.#activityMapEngineState(map), event);
    if (transition.kind === "Reject") {
      throw new Error(mapRejectMessage("Activity", transition.reject));
    }
    return this.#applyActivityMapEffects(map, transition.effects);
  }

  /**
   * Apply an engine effect list in order. Each arm is one storage primitive;
   * no arm decides anything the engine already decided.
   */
  #applyActivityMapEffects(
    map: ActivityMapState,
    effects: readonly MapEffect[]
  ): EventId | null {
    let appended: EventId | null = null;
    for (const effect of effects) {
      switch (effect.kind) {
        case "MaterializeItems":
          this.#insertActivityMapItemBatch(map, effect.firstOrdinal, effect.count);
          break;
        case "AdvanceDescriptor":
          map.nextOrdinal = effect.nextOrdinal;
          map.inFlight = effect.inFlight;
          break;
        case "ScheduleItemRetry": {
          const activity = this.#activitiesById.get(
            activityMapItemId(map.task.mapCommandId, effect.ordinal)
          );
          if (activity !== undefined) {
            activity.task = { ...activity.task, attempt: effect.nextAttempt };
            // `null` means immediately claimable, which this provider spells
            // as an availability instant already in the past.
            activity.availableAtMs = effect.visibleAtMs ?? 0;
            activity.claim = null;
          }
          // `effect.timeoutAtMs` has no consumer: map items are exempt from
          // the start-to-close and heartbeat scanners in every TypeScript
          // provider, which is tracked as its own plan row.
          break;
        }
        case "CompleteMap":
          appended = this.#appendActivityMapCompleted(map, effect.itemCount);
          break;
        case "FailMap":
          appended = this.#appendActivityMapFailed(map, effect.failure);
          break;
        case "AbandonPendingItems":
          this.#abandonPendingActivityMapItems(map);
          break;
        case "MarkDescriptorTerminal":
          map.terminal = true;
          map.inFlight = 0;
          break;
        case "RecordItemOutcome":
        case "CancelChildren":
          throw new Error(`activity maps never receive ${effect.kind}`);
      }
    }
    return appended;
  }

  /**
   * `MaterializeItems`: insert `[firstOrdinal, firstOrdinal + count)` as
   * claimable item activity tasks.
   */
  #insertActivityMapItemBatch(
    map: ActivityMapState,
    firstOrdinal: number,
    count: number
  ): void {
    for (let ordinal = firstOrdinal; ordinal < firstOrdinal + count; ordinal += 1) {
      const activityId = activityMapItemId(map.task.mapCommandId, ordinal);
      this.#activitiesById.set(activityId, {
        namespace: map.namespace,
        workflow: map.workflow,
        task: {
          activityId,
          runId: map.task.mapCommandId.runId,
          commandId: map.task.mapCommandId,
          activityName: map.task.activityName,
          taskQueue: map.task.taskQueue,
          retryPolicy: map.task.retryPolicy,
          startToCloseTimeoutMs: map.task.startToCloseTimeoutMs,
          heartbeatTimeoutMs: map.task.heartbeatTimeoutMs,
          attempt: 1,
          input: map.inputs[ordinal] as PayloadRef,
          mapItem: {
            mapCommandId: map.task.mapCommandId,
            itemOrdinal: ordinal
          }
        },
        claim: null,
        availableAtMs: 0,
        terminalEventId: null
      });
    }
  }

  /**
   * `AbandonPendingItems`: tombstone every not-yet-terminal item task of a map
   * that is over, so neither the claim path nor a late completion can
   * resurrect it. The claim-time guard that skips items of a terminal map
   * stays as well: a claim already in flight when the map ended must not
   * change the answer.
   */
  #abandonPendingActivityMapItems(map: ActivityMapState): void {
    for (const activity of this.#activitiesById.values()) {
      if (
        activity.task.mapItem === null ||
        !sameCommandId(activity.task.mapItem.mapCommandId, map.task.mapCommandId) ||
        activity.terminalEventId !== null
      ) {
        continue;
      }
      activity.terminalEventId = tailEventId(map.workflow);
      activity.claim = null;
    }
  }

  /**
   * `CompleteMap`: assemble the result manifest in ascending ordinal order,
   * append the terminal success fact to the parent, and wake it.
   */
  #appendActivityMapCompleted(map: ActivityMapState, itemCount: number): EventId {
    const counts = activityOutcomeCounts(itemCount);
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ActivityMapCompleted",
      completed: {
        commandId: map.task.mapCommandId,
        resultManifest: encodeActivityMapResultManifest(
          map.task.resultManifestName,
          completeMapItems("activity map result manifest", itemCount, map.results)
        ),
        itemCount,
        successCount: counts.successCount,
        failureCount: counts.failureCount
      }
    });
    map.workflow.history.push(event);
    markWorkflowReady(map.workflow, "ActivityMapCompleted");
    return event.eventId;
  }

  /** `FailMap`: append the terminal failure fact to the parent and wake it. */
  #appendActivityMapFailed(map: ActivityMapState, failure: DurableFailure): EventId {
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ActivityMapFailed",
      failed: {
        commandId: map.task.mapCommandId,
        failure
      }
    });
    map.workflow.history.push(event);
    markWorkflowReady(map.workflow, "ActivityMapFailed");
    return event.eventId;
  }

  #completeActivityMapItem(activity: ActivityState, result: PayloadRef): EventId {
    const map = this.#activityMapForTask(activity.task);
    if (!map || activity.task.mapItem === null) {
      throw new Error("activity map item missing descriptor");
    }
    if (map.terminal) {
      activity.terminalEventId = tailEventId(map.workflow);
      activity.claim = null;
      return tailEventId(map.workflow);
    }
    const ordinal = activity.task.mapItem.itemOrdinal;
    const alreadyRecorded = (map.results[ordinal] ?? null) !== null;
    // Ask before writing. An activity map's result slot is a storage primitive
    // rather than an effect, so it must not be written for a transition the
    // engine rejects.
    const transition = stepMap(this.#activityMapEngineState(map), {
      kind: "ItemCompleted",
      ordinal,
      // The result payload rides the descriptor's result slot, not the
      // outcome; the engine only needs to know this was a success.
      outcome: { kind: "Succeeded", result },
      alreadyRecorded,
      parentTerminal: map.workflow.terminal
    });
    if (transition.kind === "Reject") {
      throw new Error(mapRejectMessage("Activity", transition.reject));
    }
    if (!alreadyRecorded) {
      map.results[ordinal] = result;
    }
    activity.terminalEventId = tailEventId(map.workflow);
    activity.claim = null;
    return this.#applyActivityMapEffects(map, transition.effects) ?? tailEventId(map.workflow);
  }

  /**
   * One activity-map item attempt ended. `decision` is the shared retry
   * verdict, computed but not applied: the engine turns it into either a
   * rescheduled attempt or the map's terminal failure, because only the engine
   * knows whether the map is still running.
   */
  #failActivityMapItem(
    activity: ActivityState,
    failure: DurableFailure,
    decision: ItemRetryDecision,
    attemptFailure: ItemAttemptFailureKind = "Failed"
  ): FailActivityOutcome {
    const map = this.#activityMapForTask(activity.task);
    if (!map || activity.task.mapItem === null) {
      throw new Error("activity map item missing descriptor");
    }
    if (map.terminal) {
      activity.terminalEventId = tailEventId(map.workflow);
      activity.claim = null;
      return { kind: "Failed", eventId: tailEventId(map.workflow) };
    }
    const ordinal = activity.task.mapItem.itemOrdinal;
    const appended = this.#stepActivityMap(map, {
      kind: "ItemAttemptFailed",
      ordinal,
      failure,
      attemptFailure,
      decision,
      failedAttempt: activity.task.attempt,
      retryPolicy: activity.task.retryPolicy,
      startToCloseTimeoutMs: activity.task.startToCloseTimeoutMs,
      nowMs: this.#nowMs(),
      alreadyRecorded: (map.results[ordinal] ?? null) !== null,
      parentTerminal: map.workflow.terminal
    });
    if (decision.kind === "Retry") {
      return {
        kind: "RetryScheduled",
        attempt: decision.nextAttempt,
        readyAtMs: activity.availableAtMs
      };
    }
    return appended === null
      ? { kind: "AlreadyCompleted" }
      : { kind: "Failed", eventId: appended };
  }

  #activityMapForTask(task: ActivityTask): ActivityMapState | undefined {
    if (task.mapItem === null) {
      return undefined;
    }
    return this.#activityMapsByCommand.get(commandKey(task.mapItem.mapCommandId));
  }

  #createChildWorkflowMap(workflow: WorkflowState, task: ChildWorkflowMapTask): void {
    if (task.maxInFlight <= 0 || !Number.isInteger(task.maxInFlight)) {
      throw new Error("child workflow map maxInFlight must be a positive integer");
    }
    if (task.workflowIdPrefix.length === 0) {
      throw new Error("child workflow map workflowIdPrefix must not be empty");
    }
    const inputs = decodeActivityMapInputs(task.inputManifest);
    const map: ChildWorkflowMapState = {
      namespace: workflow.namespace,
      workflow,
      task,
      inputs,
      outcomes: Array.from({ length: inputs.length }, () => null),
      inFlight: 0,
      nextOrdinal: 0,
      terminal: false
    };
    this.#childWorkflowMapsByCommand.set(commandKey(task.mapCommandId), map);
    this.#stepChildWorkflowMap(map, {
      kind: "DescriptorCreated",
      parentTerminal: workflow.terminal
    });
  }

  /** The child-workflow-map descriptor as `map-engine.ts` sees it. */
  #childWorkflowMapEngineState(map: ChildWorkflowMapState): MapState {
    return {
      mapCommandId: map.task.mapCommandId,
      kind: "ChildWorkflow",
      failureMode: map.task.failureMode,
      itemCount: map.inputs.length,
      nextOrdinal: map.nextOrdinal,
      inFlight: map.inFlight,
      maxInFlight: map.task.maxInFlight,
      recordedOutcomes: recordedOutcomeCount(map.outcomes),
      completed: map.terminal
    };
  }

  /**
   * Run child-map transitions until the queue drains. Starting an item child
   * can fail on an id conflict, which is itself an item outcome; rather than
   * deciding what that means, the batch primitive queues an `ItemCompleted`
   * and the loop feeds it back through the engine.
   */
  #stepChildWorkflowMap(map: ChildWorkflowMapState, event: MapEvent): EventId | null {
    const queue: MapEvent[] = [event];
    let appended: EventId | null = null;
    while (queue.length > 0) {
      const next = queue.shift() as MapEvent;
      const transition = stepMap(this.#childWorkflowMapEngineState(map), next);
      if (transition.kind === "Reject") {
        throw new Error(mapRejectMessage("ChildWorkflow", transition.reject));
      }
      appended = this.#applyChildWorkflowMapEffects(map, transition.effects, queue) ?? appended;
    }
    return appended;
  }

  #applyChildWorkflowMapEffects(
    map: ChildWorkflowMapState,
    effects: readonly MapEffect[],
    queue: MapEvent[]
  ): EventId | null {
    let appended: EventId | null = null;
    for (const effect of effects) {
      switch (effect.kind) {
        case "RecordItemOutcome":
          map.outcomes[effect.ordinal] = effect.outcome;
          break;
        case "MaterializeItems":
          this.#startChildWorkflowMapItemBatch(map, effect.firstOrdinal, effect.count, queue);
          break;
        case "AdvanceDescriptor":
          map.nextOrdinal = effect.nextOrdinal;
          map.inFlight = effect.inFlight;
          break;
        case "CompleteMap":
          appended = this.#appendChildWorkflowMapCompleted(map, effect.itemCount);
          break;
        case "FailMap":
          appended = this.#appendChildWorkflowMapFailed(map, effect.failure);
          break;
        case "AbandonPendingItems":
          // Nothing to tombstone: this provider starts an item's child inside
          // `MaterializeItems`, so a map that is over has no undispatched item
          // start, and ordinals at or past the cursor are never admitted
          // again. `CancelChildren` covers the children that did start.
          break;
        case "CancelChildren":
          this.#cancelRunningChildWorkflowMapItems(map, effect.reason);
          break;
        case "MarkDescriptorTerminal":
          map.terminal = true;
          map.inFlight = 0;
          break;
        case "ScheduleItemRetry":
          throw new Error("child workflow maps never receive ScheduleItemRetry");
      }
    }
    return appended;
  }

  /**
   * `MaterializeItems`: start `[firstOrdinal, firstOrdinal + count)` as item
   * children. An id conflict is not a start; it is the item's terminal
   * outcome, queued for the engine to route through the map's failure mode.
   */
  #startChildWorkflowMapItemBatch(
    map: ChildWorkflowMapState,
    firstOrdinal: number,
    count: number,
    queue: MapEvent[]
  ): void {
    for (let ordinal = firstOrdinal; ordinal < firstOrdinal + count; ordinal += 1) {
      const childWorkflowId = `${map.task.workflowIdPrefix}/${ordinal}`;
      const key = workflowKey(map.namespace, childWorkflowId);
      if (this.#workflowsById.has(key)) {
        queue.push({
          kind: "ItemCompleted",
          ordinal,
          outcome: {
            kind: "Failed",
            failure: {
              errorType: "durust.child_workflow_id_conflict",
              message: `child workflow id already exists: ${childWorkflowId}`,
              nonRetryable: true
            }
          },
          alreadyRecorded: false,
          parentTerminal: map.workflow.terminal
        });
        continue;
      }

      const childRunId = runId(`run-${this.#nextRun++}`);
      const child: WorkflowState = {
        namespace: map.namespace,
        workflowId: childWorkflowId,
        workflowType: map.task.workflowType,
        taskQueue: String(map.task.taskQueue),
        runId: childRunId,
        history: [
          makeHistoryEvent(eventId(1), {
            kind: "WorkflowStarted",
            workflowType: map.task.workflowType,
            input: map.inputs[ordinal] as PayloadRef
          })
        ],
        readyReason: "WorkflowStarted",
        readyAtMs: 0,
        claim: null,
        queryProjection: null,
        terminal: false,
        parent: {
          kind: "ChildWorkflowMap",
          parentRunId: map.workflow.runId,
          mapCommandId: map.task.mapCommandId,
          itemOrdinal: ordinal,
          parentClosePolicy: map.task.parentClosePolicy
        }
      };
      this.#workflowsById.set(key, child);
      this.#workflowsByRun.set(childRunId, child);
    }
  }

  #completeChildWorkflowMapItem(
    parentLink: ChildWorkflowMapParentLink,
    terminal: ChildTerminalUpdate
  ): void {
    const map = this.#childWorkflowMapsByCommand.get(commandKey(parentLink.mapCommandId));
    if (!map || map.terminal) {
      return;
    }
    this.#stepChildWorkflowMap(map, {
      kind: "ItemCompleted",
      ordinal: parentLink.itemOrdinal,
      outcome: childWorkflowMapItemOutcome(terminal),
      alreadyRecorded: (map.outcomes[parentLink.itemOrdinal] ?? null) !== null,
      parentTerminal: map.workflow.terminal
    });
  }

  /**
   * `CompleteMap`: assemble the outcome manifest in ascending ordinal order,
   * append the terminal success fact to the parent, and wake it.
   */
  #appendChildWorkflowMapCompleted(map: ChildWorkflowMapState, itemCount: number): EventId {
    const outcomes = completeMapItems(
      "child workflow map result manifest",
      itemCount,
      map.outcomes
    );
    const counts = outcomeCounts(outcomes);
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ChildWorkflowMapCompleted",
      completed: {
        commandId: map.task.mapCommandId,
        resultManifest: encodeChildWorkflowMapResultManifest(
          map.task.resultManifestName,
          outcomes
        ),
        itemCount,
        successCount: counts.successCount,
        failureCount: counts.failureCount,
        cancellationCount: counts.cancellationCount
      }
    });
    map.workflow.history.push(event);
    markWorkflowReady(map.workflow, "ChildWorkflowMapCompleted");
    return event.eventId;
  }

  /** `FailMap`: append the terminal failure fact to the parent and wake it. */
  #appendChildWorkflowMapFailed(
    map: ChildWorkflowMapState,
    failure: DurableFailure
  ): EventId {
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ChildWorkflowMapFailed",
      failed: {
        commandId: map.task.mapCommandId,
        failure
      }
    });
    map.workflow.history.push(event);
    markWorkflowReady(map.workflow, "ChildWorkflowMapFailed");
    return event.eventId;
  }

  /**
   * `CancelChildren`: cancel every already-running, not-yet-terminal child of
   * this map with the engine's reason.
   */
  #cancelRunningChildWorkflowMapItems(map: ChildWorkflowMapState, reason: string): void {
    for (const child of this.#workflowsByRun.values()) {
      if (
        child.parent?.kind !== "ChildWorkflowMap" ||
        child.parent.parentRunId !== map.workflow.runId ||
        !sameCommandId(child.parent.mapCommandId, map.task.mapCommandId) ||
        child.terminal
      ) {
        continue;
      }
      child.history.push(makeHistoryEvent(eventId(Number(tailEventId(child)) + 1), {
        kind: "WorkflowCancelled",
        reason
      }));
      child.terminal = true;
      child.readyReason = null;
      child.readyAtMs = 0;
      child.claim = null;
      this.#abandonWorkForClosedRun(child);
    }
  }

  #startContinuedRun(previous: WorkflowState, input: PayloadRef): void {
    const newRunId = runId(`run-${this.#nextRun++}`);
    const started = makeHistoryEvent(eventId(1), {
      kind: "WorkflowStarted",
      workflowType: previous.workflowType,
      input
    });
    const next: WorkflowState = {
      namespace: previous.namespace,
      workflowId: previous.workflowId,
      workflowType: previous.workflowType,
      taskQueue: previous.taskQueue,
      runId: newRunId,
      history: [started],
      readyReason: "WorkflowStarted",
      readyAtMs: 0,
      claim: null,
      queryProjection: null,
      terminal: false,
      parent: null
    };
    this.#workflowsById.set(workflowKey(previous.namespace, previous.workflowId), next);
    this.#workflowsByRun.set(newRunId, next);
  }

  #startChildWorkflow(parent: WorkflowState, requested: ChildWorkflowStartRequested): void {
    const key = workflowKey(parent.namespace, requested.workflowId);
    if (this.#workflowsById.has(key)) {
      parent.history.push(makeHistoryEvent(eventId(Number(tailEventId(parent)) + 1), {
        kind: "ChildWorkflowFailed",
        failed: {
          commandId: requested.commandId,
          failure: {
            errorType: "durust.child_workflow_id_conflict",
            message: `child workflow id already exists: ${requested.workflowId}`,
            nonRetryable: true
          }
        }
      }));
      markWorkflowReady(parent, "ChildWorkflowFailed");
      return;
    }

    const childRunId = runId(`run-${this.#nextRun++}`);
    const child: WorkflowState = {
      namespace: parent.namespace,
      workflowId: String(requested.workflowId),
      workflowType: requested.workflowType,
      taskQueue: String(requested.taskQueue),
      runId: childRunId,
      history: [
        makeHistoryEvent(eventId(1), {
          kind: "WorkflowStarted",
          workflowType: requested.workflowType,
          input: requested.input
        })
      ],
      readyReason: "WorkflowStarted",
      readyAtMs: 0,
      claim: null,
      queryProjection: null,
      terminal: false,
      parent: {
        kind: "Child",
        parentRunId: parent.runId,
        commandId: requested.commandId,
        parentClosePolicy: requested.parentClosePolicy
      }
    };
    this.#workflowsById.set(key, child);
    this.#workflowsByRun.set(childRunId, child);
    parent.history.push(makeHistoryEvent(eventId(Number(tailEventId(parent)) + 1), {
      kind: "ChildWorkflowStarted",
      started: {
        commandId: requested.commandId,
        workflowId: requested.workflowId,
        runId: childRunId
      }
    }));
    markWorkflowReady(parent, "ChildWorkflowStarted");
  }

  #notifyParentOfChildTerminal(parentLink: ParentWorkflowLink, terminal: ChildTerminalUpdate): void {
    if (parentLink.kind === "ChildWorkflowMap") {
      this.#completeChildWorkflowMapItem(parentLink, terminal);
      return;
    }
    const parent = this.#workflowsByRun.get(parentLink.parentRunId);
    if (!parent || parent.terminal) {
      return;
    }
    const data: HistoryEventData =
      terminal.kind === "Completed"
        ? {
            kind: "ChildWorkflowCompleted",
            completed: {
              commandId: parentLink.commandId,
              result: terminal.result
            }
          }
        : terminal.kind === "Failed"
          ? {
              kind: "ChildWorkflowFailed",
              failed: {
                commandId: parentLink.commandId,
                failure: terminal.failure
              }
            }
          : {
              kind: "ChildWorkflowCancelled",
              cancelled: {
                commandId: parentLink.commandId,
                reason: terminal.reason
              }
            };
    parent.history.push(makeHistoryEvent(eventId(Number(tailEventId(parent)) + 1), data));
    markWorkflowReady(
      parent,
      terminal.kind === "Completed"
        ? "ChildWorkflowCompleted"
        : terminal.kind === "Failed"
          ? "ChildWorkflowFailed"
          : "ChildWorkflowCancelled"
    );
  }

  #cancelChildrenForClosedParent(parent: WorkflowState): void {
    for (const child of this.#workflowsByRun.values()) {
      if (
        child.parent?.parentRunId !== parent.runId ||
        child.parent.parentClosePolicy !== "Cancel" ||
        child.terminal
      ) {
        continue;
      }
      child.history.push(makeHistoryEvent(eventId(Number(tailEventId(child)) + 1), {
        kind: "WorkflowCancelled",
        reason: `parent workflow closed: ${parent.runId}`
      }));
      child.terminal = true;
      child.readyReason = null;
      child.readyAtMs = 0;
      child.claim = null;
      this.#abandonWorkForClosedRun(child);
    }
  }
}

type ChildTerminalUpdate =
  | { readonly kind: "Completed"; readonly result: PayloadRef }
  | { readonly kind: "Failed"; readonly failure: DurableFailure }
  | { readonly kind: "Cancelled"; readonly reason: string };

function workflowKey(namespace: Namespace | string, workflowId: WorkflowId | string): string {
  return `${namespace}/${workflowId}`;
}

function workflowTypeKey(workflowType: WorkflowType): string {
  return `${workflowType.name}@${workflowType.version}`;
}


/** The generated activity id of one materialized activity-map item. */
function activityMapItemId(mapCommandId: CommandId, ordinal: number): string {
  return `${mapCommandId.runId}:map:${mapCommandId.seq}:${ordinal}`;
}

/** A child run's terminal fact as the map engine's item outcome. */
function childWorkflowMapItemOutcome(
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

function tailEventId(state: WorkflowState): EventId {
  return state.history.at(-1)?.eventId ?? eventId(0);
}

function markWorkflowReady(state: WorkflowState, reason: WorkflowTaskReason): void {
  state.readyReason = reason;
  state.readyAtMs = 0;
}

function workflowLeaseMatches(lease: WorkflowLease, claim: WorkflowTaskClaim): boolean {
  return lease.claim.token === claim.token && lease.claim.workerId === claim.workerId;
}

function activityLeaseMatches(lease: ActivityLease, claim: ActivityTaskClaim): boolean {
  return lease.claim.token === claim.token && lease.claim.workerId === claim.workerId;
}

function retryActivityAfterFailure(
  activity: ActivityState,
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

function retryActivityAfterTimeout(
  activity: ActivityState,
  nowMs: number
): { readonly task: ActivityTask; readonly readyAtMs: number } | null {
  const policy = activity.task.retryPolicy;
  const maxAttempts = Math.max(1, Math.trunc(policy.maxAttempts));
  if (activity.task.attempt >= maxAttempts) {
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

function activityHeartbeatDeadlineAt(
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
function activityTimeoutDeadline(
  activity: ActivityState
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

function activityTimeoutMessage(
  activity: ActivityState,
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
function activityTimeoutFailure(
  activity: ActivityState,
  kind: "StartToClose" | "Heartbeat"
): DurableFailure {
  return {
    errorType: "durust.activity_timed_out",
    message: activityTimeoutMessage(activity, kind),
    nonRetryable: false
  };
}

function makeHistoryEvent(id: EventId, data: HistoryEventData): HistoryEvent {
  return {
    eventId: id,
    eventType: historyEventType(data),
    data
  };
}

function decodeActivityMapInputs(inputManifest: PayloadRef): readonly PayloadRef[] {
  return readMapManifestItems<ActivityMapInputPage<object>, PayloadRef>(
    inputManifest as PayloadRef<ActivityMapInputManifest<object>>,
    (page) => page.items,
    "activity map manifest"
  );
}

function encodeActivityMapResultManifest(
  name: string,
  results: readonly PayloadRef[]
): PayloadRef<ActivityMapResultManifest<unknown>> {
  return writeMapManifest<PayloadRef, ActivityMapResultPage<unknown>>(
    name,
    results,
    (pageResults) => ({ results: pageResults })
  ) as PayloadRef<ActivityMapResultManifest<unknown>>;
}

function encodeChildWorkflowMapResultManifest(
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
