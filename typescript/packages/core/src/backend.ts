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
import type {
  ActivityMapTask,
  ActivityTask,
  ChildWorkflowMapTask,
  ChildWorkflowStartRequested,
  HistoryEvent,
  HistoryEventData
} from "./history.js";
import { historyEventType } from "./history.js";
import { decodePayload, encodePayload, type PayloadRef } from "./payload.js";
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
  streamHistory(req: StreamHistoryRequest): Promise<HistoryChunk>;
  commitWorkflowTask(
    claim: WorkflowTaskClaim,
    commit: WorkflowTaskCommit
  ): Promise<CommitOutcome>;
  claimActivityTask(
    workerId: WorkerId | string,
    opts: ClaimActivityOptions
  ): Promise<ClaimedActivityTask | null>;
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
  readonly queryProjection?: PayloadRef;
}

export type CommitOutcome =
  | { readonly kind: "Committed"; readonly newTailEventId: EventId }
  | { readonly kind: "Conflict" };

export interface ClaimActivityOptions {
  readonly namespace: Namespace | string;
  readonly taskQueue: TaskQueue | string;
  readonly registeredActivityNames: readonly (ActivityName | string)[];
  readonly leaseDurationMs: number;
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
}

interface ActivityMapState {
  readonly namespace: string;
  readonly workflow: WorkflowState;
  readonly task: ActivityMapTask;
  readonly inputs: readonly PayloadRef[];
  readonly results: (PayloadRef | null)[];
  readonly inFlight: Set<number>;
  nextOrdinal: number;
  terminal: boolean;
}

interface ChildWorkflowMapState {
  readonly namespace: string;
  readonly workflow: WorkflowState;
  readonly task: ChildWorkflowMapTask;
  readonly inputs: readonly PayloadRef[];
  readonly outcomes: (ChildWorkflowMapItemOutcome<unknown> | null)[];
  readonly inFlight: Set<number>;
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

    if (commit.expectedTailEventId !== tailEventId(state)) {
      state.claim = null;
      state.readyReason = "CacheEvicted";
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
      this.#cancelChildrenForClosedParent(state);
    }
    return { kind: "Committed", newTailEventId: tailEventId(state) };
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
        heartbeatDeadlineAtMs: activityHeartbeatDeadlineAt(activity.task, now),
        expiresAtMs: this.#leaseExpiresAt(opts.leaseDurationMs)
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
    activity.workflow.readyReason = "ActivityCompleted";
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

    const retry =
      activity.task.mapItem === null || this.#activityMapForTask(activity.task)?.terminal !== true
        ? retryActivityAfterFailure(activity, req.failure, this.#nowMs())
        : null;
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

    if (activity.task.mapItem !== null) {
      const eventId = this.#failActivityMapItem(activity, req.failure);
      return { kind: "Failed", eventId };
    }

    const event = makeHistoryEvent(eventId(Number(tailEventId(activity.workflow)) + 1), {
      kind: "ActivityFailed",
      failed: {
        commandId: activity.task.commandId,
        failure: req.failure
      }
    });
    activity.workflow.history.push(event);
    activity.workflow.readyReason = "ActivityFailed";
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
    activity.claim = {
      ...currentClaim,
      heartbeatDeadlineAtMs: activityHeartbeatDeadlineAt(activity.task, this.#nowMs())
    };
    return { kind: "Recorded" };
  }

  #leaseExpiresAt(leaseDurationMs: number): number {
    return this.#nowMs() + Math.max(0, leaseDurationMs);
  }

  #restoreExpiredWorkflowLease(state: WorkflowState): void {
    if (state.claim !== null && state.claim.expiresAtMs <= this.#nowMs()) {
      state.readyReason ??= state.claim.reason;
      state.claim = null;
    }
  }

  #restoreExpiredActivityLease(activity: ActivityState): void {
    if (activity.claim !== null && activity.claim.expiresAtMs <= this.#nowMs()) {
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
      state.readyReason = "TimerFired";
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
      if (
        activity.claim === null ||
        activity.terminalEventId !== null ||
          activity.task.mapItem !== null
        ) {
          continue;
        }
        const timeout = activityTimeoutDeadline(activity);
        if (timeout.deadline > Number(req.now)) {
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
      activity.workflow.readyReason = "ActivityTimedOut";
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
      state.readyReason = "SignalReceived";
    }
  }

  #createActivityMap(workflow: WorkflowState, task: ActivityMapTask): void {
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
      inFlight: new Set(),
      nextOrdinal: 0,
      terminal: false
    };
    this.#activityMapsByCommand.set(commandKey(task.mapCommandId), map);
    this.#materializeActivityMapItems(map);
    this.#completeActivityMapIfDone(map);
  }

  #materializeActivityMapItems(map: ActivityMapState): void {
    while (
      !map.terminal &&
      map.inFlight.size < map.task.maxInFlight &&
      map.nextOrdinal < map.inputs.length
    ) {
      const ordinal = map.nextOrdinal++;
      const activityId = `${map.task.mapCommandId.runId}:map:${map.task.mapCommandId.seq}:${ordinal}`;
      const activityState: ActivityState = {
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
      };
      map.inFlight.add(ordinal);
      this.#activitiesById.set(activityId, activityState);
    }
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
    map.results[ordinal] = result;
    map.inFlight.delete(ordinal);
    activity.terminalEventId = tailEventId(map.workflow);
    activity.claim = null;
    this.#materializeActivityMapItems(map);
    return this.#completeActivityMapIfDone(map);
  }

  #failActivityMapItem(activity: ActivityState, failure: DurableFailure): EventId {
    const map = this.#activityMapForTask(activity.task);
    if (!map || activity.task.mapItem === null) {
      throw new Error("activity map item missing descriptor");
    }
    if (map.terminal) {
      activity.terminalEventId = tailEventId(map.workflow);
      activity.claim = null;
      return tailEventId(map.workflow);
    }
    map.terminal = true;
    map.inFlight.clear();
    activity.terminalEventId = tailEventId(map.workflow);
    activity.claim = null;
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ActivityMapFailed",
      failed: {
        commandId: map.task.mapCommandId,
        failure
      }
    });
    map.workflow.history.push(event);
    map.workflow.readyReason = "ActivityMapFailed";
    return event.eventId;
  }

  #completeActivityMapIfDone(map: ActivityMapState): EventId {
    if (map.terminal) {
      return tailEventId(map.workflow);
    }
    if (map.results.some((result) => result === null)) {
      return tailEventId(map.workflow);
    }
    map.terminal = true;
    const results = map.results as PayloadRef[];
    const resultManifest = encodeActivityMapResultManifest(
      map.task.resultManifestName,
      results
    );
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ActivityMapCompleted",
      completed: {
        commandId: map.task.mapCommandId,
        resultManifest,
        itemCount: results.length,
        successCount: results.length,
        failureCount: 0
      }
    });
    map.workflow.history.push(event);
    map.workflow.readyReason = "ActivityMapCompleted";
    return event.eventId;
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
      inFlight: new Set(),
      nextOrdinal: 0,
      terminal: false
    };
    this.#childWorkflowMapsByCommand.set(commandKey(task.mapCommandId), map);
    this.#materializeChildWorkflowMapItems(map);
    this.#completeChildWorkflowMapIfDone(map);
  }

  #materializeChildWorkflowMapItems(map: ChildWorkflowMapState): void {
    while (
      !map.terminal &&
      map.inFlight.size < map.task.maxInFlight &&
      map.nextOrdinal < map.inputs.length
    ) {
      const ordinal = map.nextOrdinal++;
      const childWorkflowId = `${map.task.workflowIdPrefix}/${ordinal}`;
      const key = workflowKey(map.namespace, childWorkflowId);
      if (this.#workflowsById.has(key)) {
        this.#recordChildWorkflowMapItemFailure(map, ordinal, {
          errorType: "durust.child_workflow_id_conflict",
          message: `child workflow id already exists: ${childWorkflowId}`,
          nonRetryable: true
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
      map.inFlight.add(ordinal);
      this.#workflowsById.set(key, child);
      this.#workflowsByRun.set(childRunId, child);
    }
  }

  #recordChildWorkflowMapItemFailure(
    map: ChildWorkflowMapState,
    ordinal: number,
    failure: DurableFailure
  ): void {
    if (map.task.failureMode === "FailFast") {
      this.#failChildWorkflowMap(map, failure);
      return;
    }
    map.outcomes[ordinal] = { kind: "Failed", failure };
    this.#completeChildWorkflowMapIfDone(map);
  }

  #completeChildWorkflowMapItem(
    parentLink: ChildWorkflowMapParentLink,
    terminal: ChildTerminalUpdate
  ): void {
    const map = this.#childWorkflowMapsByCommand.get(commandKey(parentLink.mapCommandId));
    if (!map || map.terminal) {
      return;
    }
    map.inFlight.delete(parentLink.itemOrdinal);

    if (terminal.kind === "Completed") {
      map.outcomes[parentLink.itemOrdinal] = {
        kind: "Succeeded",
        result: terminal.result
      };
    } else if (map.task.failureMode === "FailFast") {
      this.#failChildWorkflowMap(
        map,
        terminal.kind === "Failed"
          ? terminal.failure
          : {
              errorType: "durust.child_workflow_cancelled",
              message: terminal.reason,
              nonRetryable: true
            }
      );
      return;
    } else if (terminal.kind === "Failed") {
      map.outcomes[parentLink.itemOrdinal] = {
        kind: "Failed",
        failure: terminal.failure
      };
    } else {
      map.outcomes[parentLink.itemOrdinal] = {
        kind: "Cancelled",
        reason: terminal.reason
      };
    }

    this.#materializeChildWorkflowMapItems(map);
    this.#completeChildWorkflowMapIfDone(map);
  }

  #completeChildWorkflowMapIfDone(map: ChildWorkflowMapState): EventId {
    if (map.terminal) {
      return tailEventId(map.workflow);
    }
    if (map.outcomes.some((outcome) => outcome === null)) {
      return tailEventId(map.workflow);
    }
    map.terminal = true;
    const outcomes = map.outcomes as ChildWorkflowMapItemOutcome<unknown>[];
    const resultManifest = encodeChildWorkflowMapResultManifest(
      map.task.resultManifestName,
      outcomes
    );
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ChildWorkflowMapCompleted",
      completed: {
        commandId: map.task.mapCommandId,
        resultManifest,
        itemCount: outcomes.length,
        successCount: outcomes.filter((outcome) => outcome.kind === "Succeeded").length,
        failureCount: outcomes.filter((outcome) => outcome.kind === "Failed").length,
        cancellationCount: outcomes.filter((outcome) => outcome.kind === "Cancelled").length
      }
    });
    map.workflow.history.push(event);
    map.workflow.readyReason = "ChildWorkflowMapCompleted";
    return event.eventId;
  }

  #failChildWorkflowMap(map: ChildWorkflowMapState, failure: DurableFailure): EventId {
    if (map.terminal) {
      return tailEventId(map.workflow);
    }
    map.terminal = true;
    map.inFlight.clear();
    const event = makeHistoryEvent(eventId(Number(tailEventId(map.workflow)) + 1), {
      kind: "ChildWorkflowMapFailed",
      failed: {
        commandId: map.task.mapCommandId,
        failure
      }
    });
    map.workflow.history.push(event);
    map.workflow.readyReason = "ChildWorkflowMapFailed";
    this.#cancelRunningChildWorkflowMapItems(map);
    return event.eventId;
  }

  #cancelRunningChildWorkflowMapItems(map: ChildWorkflowMapState): void {
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
        reason: `child workflow map failed: ${map.task.mapCommandId.runId}:${map.task.mapCommandId.seq}`
      }));
      child.terminal = true;
      child.readyReason = null;
      child.claim = null;
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
      parent.readyReason = "ChildWorkflowFailed";
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
    parent.readyReason = "ChildWorkflowStarted";
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
    parent.readyReason =
      terminal.kind === "Completed"
        ? "ChildWorkflowCompleted"
        : terminal.kind === "Failed"
          ? "ChildWorkflowFailed"
          : "ChildWorkflowCancelled";
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
      child.claim = null;
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

function commandKey(id: CommandId): string {
  return `${id.runId}:${id.seq}`;
}

function sameCommandId(left: CommandId, right: CommandId): boolean {
  return left.runId === right.runId && Number(left.seq) === Number(right.seq);
}

function tailEventId(state: WorkflowState): EventId {
  return state.history.at(-1)?.eventId ?? eventId(0);
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
    readyAtMs: nowMs + retryDelayMs(activity.task.attempt, policy)
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
    readyAtMs: nowMs + retryDelayMs(activity.task.attempt, policy)
  };
}

function retryDelayMs(
  completedAttempt: number,
  policy: ActivityTask["retryPolicy"]
): number {
  const initial = Math.max(0, policy.initialIntervalMs);
  const max = Math.max(initial, policy.maxIntervalMs);
  const coefficient = Math.max(1, policy.backoffCoefficient);
  return Math.min(max, Math.round(initial * coefficient ** Math.max(0, completedAttempt - 1)));
}

function activityHeartbeatDeadlineAt(task: ActivityTask, nowMs: number): number | null {
  return task.heartbeatTimeoutMs === null
    ? null
    : nowMs + Math.max(0, task.heartbeatTimeoutMs);
}

function activityTimeoutDeadline(
  activity: ActivityState
): { readonly deadline: number; readonly kind: "StartToClose" | "Heartbeat" } {
  if (
    activity.claim === null ||
    activity.terminalEventId !== null ||
    activity.task.mapItem !== null
  ) {
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

function makeHistoryEvent(id: EventId, data: HistoryEventData): HistoryEvent {
  return {
    eventId: id,
    eventType: historyEventType(data),
    data
  };
}

function decodeActivityMapInputs(inputManifest: PayloadRef): readonly PayloadRef[] {
  const manifest = decodePayload<ActivityMapInputManifest<object>>(
    inputManifest as PayloadRef<ActivityMapInputManifest<object>>
  );
  const items: PayloadRef[] = [];
  for (const pageRef of manifest.pages) {
    const page = decodePayload<ActivityMapInputPage<object>>(
      pageRef as PayloadRef<ActivityMapInputPage<object>>
    );
    items.push(...page.items);
  }
  if (items.length !== manifest.itemCount) {
    throw new Error(
      `activity map manifest item count mismatch: expected ${manifest.itemCount}, got ${items.length}`
    );
  }
  const pageItemCount = manifest.pageLengths.reduce((sum, count) => sum + count, 0);
  if (pageItemCount !== manifest.itemCount) {
    throw new Error(
      `activity map manifest page length mismatch: expected ${manifest.itemCount}, got ${pageItemCount}`
    );
  }
  return items;
}

function encodeActivityMapResultManifest(
  name: string,
  results: readonly PayloadRef[]
): PayloadRef<ActivityMapResultManifest<unknown>> {
  const pages =
    results.length === 0
      ? []
      : [
          encodePayload<ActivityMapResultPage<unknown>>({
            results
          })
        ];
  return encodePayload<ActivityMapResultManifest<unknown>>({
    name,
    itemCount: results.length,
    pageLengths: results.length === 0 ? [] : [results.length],
    pages
  });
}

function encodeChildWorkflowMapResultManifest(
  name: string,
  outcomes: readonly ChildWorkflowMapItemOutcome<unknown>[]
): PayloadRef<ChildWorkflowMapResultManifest<unknown>> {
  const pages =
    outcomes.length === 0
      ? []
      : [
          encodePayload<ChildWorkflowMapResultPage<unknown>>({
            outcomes
          })
        ];
  return encodePayload<ChildWorkflowMapResultManifest<unknown>>({
    name,
    itemCount: outcomes.length,
    pageLengths: outcomes.length === 0 ? [] : [outcomes.length],
    pages
  });
}
