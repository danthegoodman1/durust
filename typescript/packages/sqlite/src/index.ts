import { DatabaseSync } from "node:sqlite";
import {
  eventId,
  historyEventType,
  runId,
  timestampMs,
  type ActivityMapInputManifest,
  type ActivityMapInputPage,
  type ActivityMapResultManifest,
  type ActivityMapResultPage,
  type ActivityMapTask,
  type ActivityHeartbeatOutcome,
  type ActivityHeartbeatRequest,
  type ActivityTask,
  type ActivityTaskClaim,
  type ChildWorkflowMapItemOutcome,
  type ChildWorkflowMapResultManifest,
  type ChildWorkflowMapResultPage,
  type ChildWorkflowMapTask,
  type ChildWorkflowStartRequested,
  type ClaimedActivityTask,
  type ClaimedWorkflowTask,
  type ClaimActivityOptions,
  type ClaimWorkflowTaskOptions,
  type CommandId,
  type CommitOutcome,
  type CompleteActivitiesOutcome,
  type CompleteActivitiesRequest,
  type CompleteActivityOutcome,
  type CompleteActivityItemOutcome,
  type CompleteActivityRequest,
  type DurableBackend,
  type DurableFailure,
  type EventId,
  type FailActivityOutcome,
  type FailActivityRequest,
  type FireDueTimersOutcome,
  type FireDueTimersRequest,
  type HistoryChunk,
  type HistoryEvent,
  type HistoryEventData,
  type Namespace,
  type PayloadRef,
  type QueryWorkflowOutcome,
  type QueryWorkflowRequest,
  type ReadSignalInboxRequest,
  type RunId,
  type SignalInboxRecord,
  type SignalWorkflowOutcome,
  type SignalWorkflowRequest,
  type StartWorkflowOutcome,
  type StartWorkflowRequest,
  type StreamHistoryRequest,
  type TimeoutDueActivitiesOutcome,
  type TimeoutDueActivitiesRequest,
  type WaitRecord,
  type WorkflowTaskClaim,
  type WorkflowTaskCommit,
  type WorkflowTaskReason,
  type WorkflowType
} from "@durust/core";
import { decodePayload, encodePayload } from "@durust/core";

export interface SqliteBackendOptions {
  readonly path: string;
  readonly busyTimeoutMs?: number;
  readonly nowMs?: () => number;
}

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
  readonly runId: RunId;
  readonly task: ActivityMapTask;
  readonly inputs: readonly PayloadRef[];
  readonly results: (PayloadRef | null)[];
  readonly inFlight: Set<number>;
  nextOrdinal: number;
  terminal: boolean;
}

interface ChildWorkflowMapState {
  readonly namespace: string;
  readonly runId: RunId;
  readonly task: ChildWorkflowMapTask;
  readonly inputs: readonly PayloadRef[];
  readonly outcomes: (ChildWorkflowMapItemOutcome<unknown> | null)[];
  readonly inFlight: Set<number>;
  nextOrdinal: number;
  terminal: boolean;
}

interface WorkflowRow {
  readonly run_id: string;
  readonly namespace: string;
  readonly workflow_id: string;
  readonly workflow_type: string;
  readonly workflow_type_name: string | null;
  readonly workflow_type_version: number | null;
  readonly task_queue: string;
  readonly history: string;
  readonly ready_reason: string | null;
  readonly claim_worker: string | null;
  readonly claim_token: number | null;
  readonly claim_reason: string | null;
  readonly claim_expires_at_ms: number | null;
  readonly query_projection: string | null;
  readonly terminal: number;
  readonly parent: string | null;
}

interface HistoryEventRow {
  readonly run_id: string;
  readonly event_id: number;
  readonly event_type: string;
  readonly data: string;
}

interface QueryProjectionRow {
  readonly run_id: string;
  readonly namespace: string;
  readonly workflow_id: string;
  readonly projection: string;
}

interface ActivityRow {
  readonly activity_id: string;
  readonly namespace: string;
  readonly run_id: string | null;
  readonly command_key: string | null;
  readonly activity_name: string | null;
  readonly task_queue: string | null;
  readonly map_command_key: string | null;
  readonly map_item_ordinal: number | null;
  readonly task: string;
  readonly input: string | null;
  readonly claim_worker: string | null;
  readonly claim_token: number | null;
  readonly claim_started_at_ms: number | null;
  readonly heartbeat_deadline_at_ms: number | null;
  readonly claim_expires_at_ms: number | null;
  readonly available_at_ms: number | null;
  readonly terminal_event_id: number | null;
}

interface ActivityMapRow {
  readonly command_key: string;
  readonly namespace: string;
  readonly run_id: string;
  readonly task: string;
  readonly inputs: string;
  readonly results: string;
  readonly in_flight: string;
  readonly next_ordinal: number;
  readonly terminal: number;
}

interface ActivityMapItemRow {
  readonly command_key: string;
  readonly namespace: string;
  readonly run_id: string;
  readonly item_ordinal: number;
  readonly input: string;
  readonly result: string | null;
  readonly in_flight: number;
  readonly terminal: number;
}

interface ChildWorkflowMapRow {
  readonly command_key: string;
  readonly namespace: string;
  readonly run_id: string;
  readonly task: string;
  readonly inputs: string;
  readonly outcomes: string;
  readonly in_flight: string;
  readonly next_ordinal: number;
  readonly terminal: number;
}

interface ChildWorkflowMapItemRow {
  readonly command_key: string;
  readonly namespace: string;
  readonly run_id: string;
  readonly item_ordinal: number;
  readonly input: string;
  readonly outcome: string | null;
  readonly in_flight: number;
  readonly terminal: number;
}

interface SignalRow {
  readonly signal_id: string;
  readonly namespace: string | null;
  readonly run_id: string;
  readonly signal_name: string;
  readonly payload: string;
  readonly received_sequence: number;
  readonly consumed: number;
}

interface WaitRow {
  readonly wait_id: string;
  readonly namespace: string | null;
  readonly run_id: string;
  readonly kind: string;
  readonly wait_key: string;
  readonly ready_at: number | null;
  readonly command_run_id: string | null;
  readonly command_seq: number | null;
  readonly record: string;
}

export class SqliteBackend implements DurableBackend {
  readonly #db: DatabaseSync;
  readonly #nowMs: () => number;
  #closed = false;

  constructor(options: SqliteBackendOptions) {
    this.#nowMs = options.nowMs ?? Date.now;
    this.#db = new DatabaseSync(options.path, {
      timeout: options.busyTimeoutMs ?? 5_000
    });
    this.#db.exec("PRAGMA journal_mode = WAL");
    this.#db.exec("PRAGMA synchronous = FULL");
    this.#db.exec("PRAGMA foreign_keys = ON");
    this.#initializeSchema();
  }

  close(): void {
    if (!this.#closed) {
      this.#db.close();
      this.#closed = true;
    }
  }

  async startWorkflow(req: StartWorkflowRequest): Promise<StartWorkflowOutcome> {
    return this.#transaction(() => {
      const existing = this.#currentRunId(req.namespace, req.workflowId);
      if (existing !== null) {
        return { kind: "AlreadyStarted", runId: existing };
      }

      const newRunId = runId(`run-${this.#nextCounter("run")}`);
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
      this.#insertWorkflow(state, true);
      return { kind: "Started", runId: newRunId };
    });
  }

  async claimWorkflowTask(
    workerIdValue: string,
    opts: ClaimWorkflowTaskOptions
  ): Promise<ClaimedWorkflowTask | null> {
    return this.#transaction(() => {
      if (opts.registeredWorkflowTypes.length === 0) {
        return null;
      }
      const eligibleTypes = new Set(
        opts.registeredWorkflowTypes.map((workflowTypeValue) =>
          workflowTypeKey(workflowTypeValue)
        )
      );
      const typeClauses = opts.registeredWorkflowTypes
        .map(() => "(workflow_type_name = ? and workflow_type_version = ?)")
        .join(" or ");
      const typeParams = opts.registeredWorkflowTypes.flatMap((workflowTypeValue) => [
        workflowTypeValue.name,
        workflowTypeValue.version
      ]);
      const rows = this.#db.prepare(`
        select * from workflows
        where namespace = ? and task_queue = ? and terminal = 0
          and (${typeClauses})
          and (ready_reason is not null or claim_worker is not null)
          and (claim_worker is null or claim_expires_at_ms is null or claim_expires_at_ms <= ?)
        order by rowid asc
      `).all(
        String(opts.namespace),
        String(opts.taskQueue),
        ...typeParams,
        this.#nowMs()
      ) as unknown as WorkflowRow[];

      for (const row of rows) {
        const state = this.#workflowStateFromRow(row);
        this.#restoreExpiredWorkflowLease(state);
        if (!eligibleTypes.has(workflowTypeKey(state.workflowType))) {
          continue;
        }
        const claim: WorkflowTaskClaim = {
          runId: state.runId,
          workerId: workerIdValue,
          token: this.#nextCounter("workflow_claim")
        };
        const reason = state.readyReason;
        if (reason === null) {
          continue;
        }
        state.claim = {
          claim,
          reason,
          expiresAtMs: this.#leaseExpiresAt(opts.leaseDurationMs)
        };
        state.readyReason = null;
        this.#saveWorkflow(state);
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
    });
  }

  async streamHistory(req: StreamHistoryRequest): Promise<HistoryChunk> {
    const maxEvents = Math.max(0, req.maxEvents);
    const rows =
      maxEvents === 0
        ? []
        : this.#db.prepare(`
            select * from history_events
            where run_id = ? and event_id > ? and event_id <= ?
            order by event_id asc
            limit ?
          `).all(
            String(req.runId),
            Number(req.afterEventId),
            Number(req.upToEventId),
            maxEvents + 1
          ) as unknown as HistoryEventRow[];
    const events = rows.slice(0, maxEvents).map(historyEventFromRow);
    const lastEvent = events.at(-1);
    return {
      events,
      lastEventId: lastEvent?.eventId ?? req.afterEventId,
      hasMore: rows.length > maxEvents
    };
  }

  async commitWorkflowTask(
    claim: WorkflowTaskClaim,
    commit: WorkflowTaskCommit
  ): Promise<CommitOutcome> {
    return this.#transaction(() => {
      const state = this.#stateForRun(claim.runId);
      this.#restoreExpiredWorkflowLease(state);
      if (
        state.claim === null ||
        !workflowLeaseMatches(state.claim, claim)
      ) {
        this.#saveWorkflow(state);
        throw new Error("stale workflow task lease");
      }

      if (commit.expectedTailEventId !== tailEventId(state)) {
        state.claim = null;
        state.readyReason = "CacheEvicted";
        this.#saveWorkflow(state);
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
        this.#upsertWait(state.namespace, wait);
      }
      for (const waitId of commit.deleteWaits ?? []) {
        this.#db.prepare("delete from waits where wait_id = ?").run(String(waitId));
      }
      for (const signalIdValue of commit.consumeSignals ?? []) {
        this.#db.prepare("update signals set consumed = 1 where signal_id = ?").run(
          String(signalIdValue)
        );
      }
      for (const task of commit.scheduleActivities ?? []) {
        this.#insertActivity({
          namespace: state.namespace,
          task,
          claim: null,
          availableAtMs: 0,
          terminalEventId: null
        });
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
      this.#saveWorkflow(state);
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
    });
  }

  async claimActivityTask(
    workerIdValue: string,
    opts: ClaimActivityOptions
  ): Promise<ClaimedActivityTask | null> {
    return this.#transaction(() => {
      const registeredNames = opts.registeredActivityNames.map(String);
      if (registeredNames.length === 0) {
        return null;
      }
      const now = this.#nowMs();
      const placeholders = registeredNames.map(() => "?").join(", ");
      const rows = this.#db.prepare(`
        select * from activities
        where namespace = ? and terminal_event_id is null
          and task_queue = ?
          and activity_name in (${placeholders})
          and coalesce(available_at_ms, 0) <= ?
          and (claim_worker is null or claim_expires_at_ms is null or claim_expires_at_ms <= ?)
        order by rowid asc
      `).all(
        String(opts.namespace),
        String(opts.taskQueue),
        ...registeredNames,
        now,
        now
      ) as unknown as ActivityRow[];
      for (const row of rows) {
        const activity = activityStateFromRow(row);
        this.#restoreExpiredActivityLease(activity);
        if (
          this.#activityMapForTask(activity.task)?.terminal === true
        ) {
          continue;
        }
        const claim: ActivityTaskClaim = {
          activityId: activity.task.activityId,
          workerId: workerIdValue,
          token: this.#nextCounter("activity_claim")
        };
        const now = this.#nowMs();
        activity.claim = {
          claim,
          startedAtMs: now,
          heartbeatDeadlineAtMs: activityHeartbeatDeadlineAt(activity.task, now),
          expiresAtMs: this.#leaseExpiresAt(opts.leaseDurationMs)
        };
        this.#insertActivity(activity);
        return { task: activity.task, claim };
      }
      return null;
    });
  }

  async completeActivity(req: CompleteActivityRequest): Promise<CompleteActivityOutcome> {
    return this.#transaction(() => {
      const outcome = this.#completeActivityItem(req);
      if (outcome.kind === "NotFound") {
        throw new Error(`activity task not found: ${req.claim.activityId}`);
      }
      if (outcome.kind === "StaleLease") {
        throw new Error("stale activity task lease");
      }
      return outcome;
    });
  }

  async completeActivities(req: CompleteActivitiesRequest): Promise<CompleteActivitiesOutcome> {
    return this.#transaction(() => ({
      results: req.completions.map((completion) => this.#completeActivityItem(completion))
    }));
  }

  #completeActivityItem(req: CompleteActivityRequest): CompleteActivityItemOutcome {
    const activity = this.#activityForIdOrNull(req.claim.activityId);
    if (!activity) {
      return { kind: "NotFound" };
    }
    this.#restoreExpiredActivityLease(activity);
    if (activity.terminalEventId !== null) {
      return { kind: "AlreadyCompleted" };
    }
    if (!this.#activityClaimMatches(activity, req.claim)) {
      this.#insertActivity(activity);
      return { kind: "StaleLease" };
    }

    if (activity.task.mapItem !== null) {
      const eventIdValue = this.#completeActivityMapItem(activity, req.result);
      return { kind: "Completed", eventId: eventIdValue };
    }

    const workflow = this.#stateForRun(activity.task.runId);
    const event = makeHistoryEvent(eventId(Number(tailEventId(workflow)) + 1), {
      kind: "ActivityCompleted",
      completed: {
        commandId: activity.task.commandId,
        result: req.result
      }
    });
    workflow.history.push(event);
    workflow.readyReason = "ActivityCompleted";
    activity.terminalEventId = event.eventId;
    activity.claim = null;
    this.#saveWorkflow(workflow);
    this.#insertActivity(activity);
    return { kind: "Completed", eventId: event.eventId };
  }

  async failActivity(req: FailActivityRequest): Promise<FailActivityOutcome> {
    return this.#transaction(() => {
      const activity = this.#activityForId(req.claim.activityId);
      this.#restoreExpiredActivityLease(activity);
      if (activity.terminalEventId !== null) {
        return { kind: "AlreadyCompleted" };
      }
      if (!this.#activityClaimMatches(activity, req.claim)) {
        this.#insertActivity(activity);
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
        this.#insertActivity(activity);
        return {
          kind: "RetryScheduled",
          attempt: retry.task.attempt,
          readyAtMs: retry.readyAtMs
        };
      }

      if (activity.task.mapItem !== null) {
        const eventIdValue = this.#failActivityMapItem(activity, req.failure);
        return { kind: "Failed", eventId: eventIdValue };
      }

      const workflow = this.#stateForRun(activity.task.runId);
      const event = makeHistoryEvent(eventId(Number(tailEventId(workflow)) + 1), {
        kind: "ActivityFailed",
        failed: {
          commandId: activity.task.commandId,
          failure: req.failure
        }
      });
      workflow.history.push(event);
      workflow.readyReason = "ActivityFailed";
      activity.terminalEventId = event.eventId;
      activity.claim = null;
      this.#saveWorkflow(workflow);
      this.#insertActivity(activity);
      return { kind: "Failed", eventId: event.eventId };
    });
  }

  async heartbeatActivity(req: ActivityHeartbeatRequest): Promise<ActivityHeartbeatOutcome> {
    return this.#transaction(() => {
      const activity = this.#activityForId(req.claim.activityId);
      this.#restoreExpiredActivityLease(activity);
      if (activity.terminalEventId !== null) {
        return { kind: "AlreadyCompleted" };
      }
      if (!this.#activityClaimMatches(activity, req.claim)) {
        this.#insertActivity(activity);
        throw new Error("stale activity task lease");
      }
      const currentClaim = activity.claim;
      if (currentClaim === null) {
        throw new Error("stale activity task lease");
      }
      activity.claim = {
        ...currentClaim,
        heartbeatDeadlineAtMs: activityHeartbeatDeadlineAt(activity.task, this.#nowMs())
      };
      this.#insertActivity(activity);
      return { kind: "Recorded" };
    });
  }

  async fireDueTimers(req: FireDueTimersRequest): Promise<FireDueTimersOutcome> {
    return this.#transaction(() => {
      const rows = this.#db.prepare(`
        select * from waits
        where namespace = ? and kind = 'Timer' and ready_at is not null and ready_at <= ?
          and command_run_id is not null and command_seq is not null
        order by ready_at asc, wait_id asc
        limit ?
      `).all(String(req.namespace), Number(req.now), Math.max(1, req.limit)) as unknown as WaitRow[];
      let fired = 0;
      for (const row of rows) {
        const state = this.#stateForRunOrNull(runId(row.run_id));
        if (!state) {
          this.#db.prepare("delete from waits where wait_id = ?").run(row.wait_id);
          continue;
        }
        const event = makeHistoryEvent(eventId(Number(tailEventId(state)) + 1), {
          kind: "TimerFired",
          fired: {
            commandId: commandIdFromParts(row.command_run_id as string, row.command_seq as number),
            firedAt: timestampMs(Number(req.now))
          }
        });
        state.history.push(event);
        state.readyReason = "TimerFired";
        this.#saveWorkflow(state);
        this.#db.prepare("delete from waits where wait_id = ?").run(row.wait_id);
        fired += 1;
      }
      return { fired };
    });
  }

  async timeoutDueActivities(req: TimeoutDueActivitiesRequest): Promise<TimeoutDueActivitiesOutcome> {
    return this.#transaction(() => {
      const activities = (this.#db.prepare(`
        select * from activities
        where namespace = ?
          and claim_worker is not null
          and terminal_event_id is null
      `).all(String(req.namespace)) as unknown as ActivityRow[])
        .map((row) => activityStateFromRow(row))
        .filter((activity) => activityTimeoutDeadline(activity).deadline <= Number(req.now))
        .sort((left, right) =>
          activityTimeoutDeadline(left).deadline - activityTimeoutDeadline(right).deadline ||
          left.task.activityId.localeCompare(right.task.activityId)
        )
        .slice(0, Math.max(1, req.limit));
      let timedOut = 0;
      for (const activity of activities) {
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
          this.#insertActivity(activity);
          timedOut += 1;
          continue;
        }
        const workflow = this.#stateForRun(activity.task.runId);
        const event = makeHistoryEvent(eventId(Number(tailEventId(workflow)) + 1), {
          kind: "ActivityTimedOut",
          timedOut: {
            commandId: activity.task.commandId,
            message: activityTimeoutMessage(activity, timeout.kind)
          }
        });
        workflow.history.push(event);
        workflow.readyReason = "ActivityTimedOut";
        activity.terminalEventId = event.eventId;
        activity.claim = null;
        this.#saveWorkflow(workflow);
        this.#insertActivity(activity);
        timedOut += 1;
      }
      return { timedOut };
    });
  }

  async signalWorkflow(req: SignalWorkflowRequest): Promise<SignalWorkflowOutcome> {
    return this.#transaction(() => {
      const existing = this.#db.prepare("select 1 from signals where signal_id = ?").get(
        String(req.signalId)
      );
      if (existing) {
        return { kind: "Duplicate" };
      }

      const runIdValue = this.#currentRunId(req.namespace, req.workflowId);
      if (runIdValue === null) {
        throw new Error(`workflow not found: ${req.workflowId}`);
      }
      const state = this.#stateForRun(runIdValue);
      this.#db.prepare(`
        insert into signals(
          signal_id, namespace, run_id, signal_name, payload, received_sequence, consumed
        ) values (?, ?, ?, ?, ?, ?, 0)
      `).run(
        String(req.signalId),
        String(req.namespace),
        String(runIdValue),
        String(req.signalName),
        stringifyJson(req.payload),
        this.#nextCounter("signal")
      );
      this.#wakeIfSignalWaitReady(state);
      this.#saveWorkflow(state);
      return { kind: "Accepted" };
    });
  }

  async readSignalInbox(req: ReadSignalInboxRequest): Promise<SignalInboxRecord | null> {
    const row = this.#db.prepare(`
      select * from signals
      where run_id = ? and signal_name = ? and consumed = 0
      order by received_sequence asc
      limit 1
    `).get(String(req.runId), String(req.signalName)) as SignalRow | undefined;
    if (!row) {
      return null;
    }
    return {
      signalId: row.signal_id,
      signalName: row.signal_name,
      payload: parseJson<PayloadRef>(row.payload)
    };
  }

  async queryWorkflow(req: QueryWorkflowRequest): Promise<QueryWorkflowOutcome> {
    const runIdValue = this.#currentRunId(req.namespace, req.workflowId);
    if (runIdValue === null) {
      return { kind: "NotFound" };
    }
    const row = this.#db.prepare(`
      select projection from query_projections where run_id = ?
    `).get(String(runIdValue)) as { readonly projection: string } | undefined;
    if (row === undefined) {
      return { kind: "NoProjection" };
    }
    return { kind: "Found", projection: parseJson<PayloadRef>(row.projection) };
  }

  async payloadRoots(): Promise<readonly unknown[]> {
    const historyEvents = (this.#db.prepare(`
      select * from history_events order by run_id asc, event_id asc
    `).all() as unknown as HistoryEventRow[]).map(historyEventFromRow);
    const workflowProjections = (this.#db.prepare(`
      select * from query_projections order by run_id asc
    `).all() as unknown as QueryProjectionRow[])
      .map((row) => ({
        runId: row.run_id,
        namespace: row.namespace,
        workflowId: row.workflow_id,
        queryProjection: parseJson<PayloadRef>(row.projection)
      }));
    const activities = (this.#db.prepare("select * from activities").all() as unknown as ActivityRow[])
      .map(activityRootFromRow);
    const activityMapDescriptors = (this.#db.prepare("select * from activity_maps").all() as unknown as ActivityMapRow[])
      .map(activityMapDescriptorRootFromRow);
    const activityMapItems = (this.#db.prepare("select * from activity_map_items").all() as unknown as ActivityMapItemRow[])
      .map(activityMapItemRootFromRow);
    const childWorkflowMapDescriptors = (this.#db.prepare("select * from child_workflow_maps").all() as unknown as ChildWorkflowMapRow[])
      .map(childWorkflowMapDescriptorRootFromRow);
    const childWorkflowMapItems = (this.#db.prepare("select * from child_workflow_map_items").all() as unknown as ChildWorkflowMapItemRow[])
      .map(childWorkflowMapItemRootFromRow);
    const signals = (this.#db.prepare("select * from signals").all() as unknown as SignalRow[])
      .map((row) => ({
        signalId: row.signal_id,
        namespace: row.namespace,
        runId: row.run_id,
        signalName: row.signal_name,
        payload: parseJson<PayloadRef>(row.payload),
        receivedSequence: row.received_sequence,
        consumed: row.consumed !== 0
      }));
    return [
      ...historyEvents,
      ...workflowProjections,
      ...activities,
      ...activityMapDescriptors,
      ...activityMapItems,
      ...childWorkflowMapDescriptors,
      ...childWorkflowMapItems,
      ...signals
    ];
  }

  #initializeSchema(): void {
    this.#db.exec(`
      create table if not exists meta(
        key text primary key,
        value integer not null
      );

      create table if not exists workflows(
        run_id text primary key,
        namespace text not null,
        workflow_id text not null,
        workflow_type text not null,
        workflow_type_name text,
        workflow_type_version integer,
        task_queue text not null,
        history text not null,
        ready_reason text,
        claim_worker text,
        claim_token integer,
        claim_reason text,
        claim_expires_at_ms integer,
        query_projection text,
        terminal integer not null,
        parent text
      );

      create table if not exists history_events(
        run_id text not null,
        event_id integer not null,
        event_type text not null,
        data text not null,
        primary key(run_id, event_id)
      );

      create table if not exists query_projections(
        run_id text primary key,
        namespace text not null,
        workflow_id text not null,
        projection text not null
      );

      create table if not exists workflow_ids(
        namespace text not null,
        workflow_id text not null,
        run_id text not null,
        primary key(namespace, workflow_id)
      );

      create table if not exists activities(
        activity_id text primary key,
        namespace text not null,
        run_id text,
        command_key text,
        activity_name text,
        task_queue text,
        map_command_key text,
        map_item_ordinal integer,
        task text not null,
        input text,
        claim_worker text,
        claim_token integer,
        claim_started_at_ms integer,
        heartbeat_deadline_at_ms integer,
        claim_expires_at_ms integer,
        available_at_ms integer,
        terminal_event_id integer
      );

      create table if not exists activity_maps(
        command_key text primary key,
        namespace text not null,
        run_id text not null,
        task text not null,
        inputs text not null,
        results text not null,
        in_flight text not null,
        next_ordinal integer not null,
        terminal integer not null
      );

      create table if not exists activity_map_items(
        command_key text not null,
        namespace text not null,
        run_id text not null,
        item_ordinal integer not null,
        input text not null,
        result text,
        in_flight integer not null,
        terminal integer not null,
        primary key(command_key, item_ordinal)
      );

      create table if not exists child_workflow_maps(
        command_key text primary key,
        namespace text not null,
        run_id text not null,
        task text not null,
        inputs text not null,
        outcomes text not null,
        in_flight text not null,
        next_ordinal integer not null,
        terminal integer not null
      );

      create table if not exists child_workflow_map_items(
        command_key text not null,
        namespace text not null,
        run_id text not null,
        item_ordinal integer not null,
        input text not null,
        outcome text,
        in_flight integer not null,
        terminal integer not null,
        primary key(command_key, item_ordinal)
      );

      create table if not exists waits(
        wait_id text primary key,
        namespace text,
        run_id text not null,
        kind text not null,
        wait_key text not null,
        ready_at integer,
        command_run_id text,
        command_seq integer,
        record text not null
      );

      create table if not exists signals(
        signal_id text primary key,
        namespace text,
        run_id text not null,
        signal_name text not null,
        payload text not null,
        received_sequence integer not null,
        consumed integer not null
      );

      create index if not exists idx_workflows_ready
        on workflows(namespace, task_queue, ready_reason, run_id)
        where terminal = 0 and ready_reason is not null and claim_worker is null;

      create index if not exists idx_workflows_ready_type_projection
        on workflows(namespace, task_queue, workflow_type_name, workflow_type_version, ready_reason, run_id)
        where terminal = 0 and ready_reason is not null and claim_worker is null;

      create index if not exists idx_history_events_run_order
        on history_events(run_id, event_id);

      create index if not exists idx_query_projections_lookup
        on query_projections(namespace, workflow_id);

      create index if not exists idx_activities_claim
        on activities(namespace)
        where claim_worker is null and terminal_event_id is null;

      create index if not exists idx_activity_map_items_command
        on activity_map_items(command_key, item_ordinal);

      create index if not exists idx_child_workflow_map_items_command
        on child_workflow_map_items(command_key, item_ordinal);

      create index if not exists idx_waits_timer_due
        on waits(namespace, kind, ready_at, wait_id)
        where kind = 'Timer';

      create index if not exists idx_signals_inbox
        on signals(run_id, signal_name, consumed, received_sequence);

      create index if not exists idx_signals_namespace_inbox
        on signals(namespace, run_id, signal_name, consumed, received_sequence);
    `);
    this.#db.exec(`
      create index if not exists idx_activities_ready_projection
        on activities(namespace, task_queue, activity_name, available_at_ms, activity_id)
        where claim_worker is null and terminal_event_id is null;

      create index if not exists idx_activities_expired_projection
        on activities(namespace, task_queue, activity_name, claim_expires_at_ms, activity_id)
        where claim_worker is not null and terminal_event_id is null;

      create index if not exists idx_waits_signal_projection
        on waits(run_id, kind, wait_key)
        where kind = 'Signal';
    `);
  }

  #transaction<T>(fn: () => T): T {
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const result = fn();
      this.#db.exec("COMMIT");
      return result;
    } catch (error) {
      this.#db.exec("ROLLBACK");
      throw error;
    }
  }

  #nextCounter(key: string): number {
    const row = this.#db.prepare("select value from meta where key = ?").get(key) as
      | { readonly value: number }
      | undefined;
    const next = row ? row.value : 1;
    this.#db.prepare(`
      insert into meta(key, value) values (?, ?)
      on conflict(key) do update set value = excluded.value
    `).run(key, next + 1);
    return next;
  }

  #currentRunId(namespaceValue: Namespace | string, workflowIdValue: string): RunId | null {
    const row = this.#db.prepare(`
      select run_id from workflow_ids where namespace = ? and workflow_id = ?
    `).get(String(namespaceValue), String(workflowIdValue)) as
      | { readonly run_id: string }
      | undefined;
    return row ? runId(row.run_id) : null;
  }

  #stateForRun(id: RunId): WorkflowState {
    const state = this.#stateForRunOrNull(id);
    if (!state) {
      throw new Error(`workflow run not found: ${id}`);
    }
    return state;
  }

  #stateForRunOrNull(id: RunId): WorkflowState | null {
    const row = this.#db.prepare("select * from workflows where run_id = ?").get(String(id)) as
      | WorkflowRow
      | undefined;
    return row ? this.#workflowStateFromRow(row) : null;
  }

  #insertWorkflow(state: WorkflowState, updateCurrent: boolean): void {
    this.#db.prepare(`
      insert into workflows(
        run_id, namespace, workflow_id, workflow_type, workflow_type_name,
        workflow_type_version, task_queue, history, ready_reason,
        claim_worker, claim_token, claim_reason, claim_expires_at_ms, query_projection, terminal, parent
      ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      String(state.runId),
      state.namespace,
      state.workflowId,
      stringifyJson(state.workflowType),
      state.workflowType.name,
      state.workflowType.version,
      state.taskQueue,
      stringifyJson(state.history),
      state.readyReason,
      state.claim === null ? null : String(state.claim.claim.workerId),
      state.claim?.claim.token ?? null,
      state.claim?.reason ?? null,
      state.claim?.expiresAtMs ?? null,
      state.queryProjection === null ? null : stringifyJson(state.queryProjection),
      state.terminal ? 1 : 0,
      state.parent === null ? null : stringifyJson(state.parent)
    );
    this.#insertHistoryEvents(state.runId, state.history);
    this.#syncQueryProjection(state);
    if (updateCurrent) {
      this.#db.prepare(`
        insert into workflow_ids(namespace, workflow_id, run_id) values (?, ?, ?)
        on conflict(namespace, workflow_id) do update set run_id = excluded.run_id
      `).run(state.namespace, state.workflowId, String(state.runId));
    }
  }

  #saveWorkflow(state: WorkflowState): void {
    this.#db.prepare(`
      update workflows set
        workflow_type = ?,
        workflow_type_name = ?,
        workflow_type_version = ?,
        task_queue = ?,
        history = ?,
        ready_reason = ?,
        claim_worker = ?,
        claim_token = ?,
        claim_reason = ?,
        claim_expires_at_ms = ?,
        query_projection = ?,
        terminal = ?,
        parent = ?
      where run_id = ?
    `).run(
      stringifyJson(state.workflowType),
      state.workflowType.name,
      state.workflowType.version,
      state.taskQueue,
      stringifyJson(state.history),
      state.readyReason,
      state.claim === null ? null : String(state.claim.claim.workerId),
      state.claim?.claim.token ?? null,
      state.claim?.reason ?? null,
      state.claim?.expiresAtMs ?? null,
      state.queryProjection === null ? null : stringifyJson(state.queryProjection),
      state.terminal ? 1 : 0,
      state.parent === null ? null : stringifyJson(state.parent),
      String(state.runId)
    );
    this.#insertHistoryEvents(state.runId, state.history);
    this.#syncQueryProjection(state);
  }

  #workflowStateFromRow(row: WorkflowRow): WorkflowState {
    const rowRunId = runId(row.run_id);
    const history = this.#historyEventsForRun(rowRunId);
    if (history.length === 0) {
      throw new Error(`workflow history not found: ${rowRunId}`);
    }
    return workflowStateFromRowWithHistory(row, history);
  }

  #historyEventsForRun(id: RunId): HistoryEvent[] {
    return (this.#db.prepare(`
      select * from history_events
      where run_id = ?
      order by event_id asc
    `).all(String(id)) as unknown as HistoryEventRow[]).map(historyEventFromRow);
  }

  #insertHistoryEvents(id: RunId, events: readonly HistoryEvent[]): void {
    const insert = this.#db.prepare(`
      insert into history_events(run_id, event_id, event_type, data)
      values (?, ?, ?, ?)
      on conflict(run_id, event_id) do update set
        event_type = excluded.event_type,
        data = excluded.data
    `);
    for (const event of events) {
      insert.run(
        String(id),
        Number(event.eventId),
        event.eventType,
        stringifyJson(event.data)
      );
    }
  }

  #syncQueryProjection(state: WorkflowState): void {
    if (state.queryProjection === null) {
      this.#db.prepare("delete from query_projections where run_id = ?").run(String(state.runId));
      return;
    }
    this.#db.prepare(`
      insert into query_projections(run_id, namespace, workflow_id, projection)
      values (?, ?, ?, ?)
      on conflict(run_id) do update set
        namespace = excluded.namespace,
        workflow_id = excluded.workflow_id,
        projection = excluded.projection
    `).run(
      String(state.runId),
      state.namespace,
      state.workflowId,
      stringifyJson(state.queryProjection)
    );
  }

  #upsertWait(namespaceValue: string, wait: WaitRecord): void {
    this.#db.prepare(`
      insert into waits(
        wait_id, namespace, run_id, kind, wait_key, ready_at, command_run_id, command_seq, record
      )
      values (?, ?, ?, ?, ?, ?, ?, ?, ?)
      on conflict(wait_id) do update set
        namespace = excluded.namespace,
        run_id = excluded.run_id,
        kind = excluded.kind,
        wait_key = excluded.wait_key,
        ready_at = excluded.ready_at,
        command_run_id = excluded.command_run_id,
        command_seq = excluded.command_seq,
        record = excluded.record
    `).run(
      String(wait.waitId),
      namespaceValue,
      String(wait.runId),
      wait.kind,
      wait.key,
      wait.readyAt === null ? null : Number(wait.readyAt),
      String(wait.commandId.runId),
      Number(wait.commandId.seq),
      stringifyJson(wait)
    );
  }

  #insertActivity(activity: ActivityState): void {
    this.#db.prepare(`
      insert or replace into activities(
        activity_id, namespace, run_id, command_key, activity_name, task_queue,
        map_command_key, map_item_ordinal, task, input, claim_worker, claim_token, claim_started_at_ms,
        heartbeat_deadline_at_ms, claim_expires_at_ms,
        available_at_ms, terminal_event_id
      ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      activity.task.activityId,
      activity.namespace,
      String(activity.task.runId),
      commandKey(activity.task.commandId),
      String(activity.task.activityName),
      String(activity.task.taskQueue),
      activity.task.mapItem === null ? null : commandKey(activity.task.mapItem.mapCommandId),
      activity.task.mapItem?.itemOrdinal ?? null,
      stringifyJson(activity.task),
      stringifyJson(activity.task.input),
      activity.claim === null ? null : String(activity.claim.claim.workerId),
      activity.claim?.claim.token ?? null,
      activity.claim?.startedAtMs ?? null,
      activity.claim?.heartbeatDeadlineAtMs ?? null,
      activity.claim?.expiresAtMs ?? null,
      activity.availableAtMs,
      activity.terminalEventId === null ? null : Number(activity.terminalEventId)
    );
  }

  #activityForId(activityId: string): ActivityState {
    const activity = this.#activityForIdOrNull(activityId);
    if (!activity) {
      throw new Error(`activity task not found: ${activityId}`);
    }
    return activity;
  }

  #activityForIdOrNull(activityId: string): ActivityState | null {
    const row = this.#db.prepare("select * from activities where activity_id = ?").get(
      activityId
    ) as ActivityRow | undefined;
    if (!row) {
      return null;
    }
    return activityStateFromRow(row);
  }

  #activityClaimMatches(activity: ActivityState, claim: ActivityTaskClaim): boolean {
    return (
      activity.claim !== null &&
      activity.claim.claim.token === claim.token &&
      activity.claim.claim.workerId === claim.workerId
    );
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

  #wakeIfSignalWaitReady(state: WorkflowState): void {
    const waits = this.#db.prepare(`
      select wait_key from waits where run_id = ? and kind = 'Signal'
    `).all(String(state.runId)) as { readonly wait_key: string }[];
    for (const waitRow of waits) {
      const signal = this.#db.prepare(`
        select 1 from signals
        where run_id = ? and signal_name = ? and consumed = 0
        limit 1
      `).get(String(state.runId), waitRow.wait_key);
      if (signal) {
        state.readyReason = "SignalReceived";
        return;
      }
    }
  }

  #createActivityMap(workflow: WorkflowState, task: ActivityMapTask): void {
    if (task.maxInFlight <= 0 || !Number.isInteger(task.maxInFlight)) {
      throw new Error("activity map maxInFlight must be a positive integer");
    }
    const inputs = decodeActivityMapInputs(task.inputManifest);
    const map: ActivityMapState = {
      namespace: workflow.namespace,
      runId: workflow.runId,
      task,
      inputs,
      results: Array.from({ length: inputs.length }, () => null),
      inFlight: new Set(),
      nextOrdinal: 0,
      terminal: false
    };
    this.#insertActivityMap(map);
    this.#materializeActivityMapItems(map);
    this.#completeActivityMapIfDone(map);
  }

  #insertActivityMap(map: ActivityMapState): void {
    this.#db.prepare(`
      insert or replace into activity_maps(
        command_key, namespace, run_id, task, inputs, results, in_flight, next_ordinal, terminal
      ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      commandKey(map.task.mapCommandId),
      map.namespace,
      String(map.runId),
      stringifyJson(map.task),
      stringifyJson(map.inputs),
      stringifyJson(map.results),
      stringifyJson([...map.inFlight]),
      map.nextOrdinal,
      map.terminal ? 1 : 0
    );
    this.#replaceActivityMapItemRows(map);
  }

  #replaceActivityMapItemRows(map: ActivityMapState): void {
    const key = commandKey(map.task.mapCommandId);
    this.#db.prepare("delete from activity_map_items where command_key = ?").run(key);
    const insert = this.#db.prepare(`
      insert into activity_map_items(
        command_key, namespace, run_id, item_ordinal, input, result, in_flight, terminal
      ) values (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (let ordinal = 0; ordinal < map.inputs.length; ordinal += 1) {
      const result = map.results[ordinal] ?? null;
      insert.run(
        key,
        map.namespace,
        String(map.runId),
        ordinal,
        stringifyJson(map.inputs[ordinal]),
        result === null ? null : stringifyJson(result),
        map.inFlight.has(ordinal) ? 1 : 0,
        map.terminal || result !== null ? 1 : 0
      );
    }
  }

  #activityMapForTask(task: ActivityTask): ActivityMapState | undefined {
    if (task.mapItem === null) {
      return undefined;
    }
    return this.#activityMapForCommand(task.mapItem.mapCommandId);
  }

  #activityMapForCommand(id: CommandId): ActivityMapState | undefined {
    const row = this.#db.prepare("select * from activity_maps where command_key = ?").get(
      commandKey(id)
    ) as ActivityMapRow | undefined;
    return row ? activityMapStateFromRow(row) : undefined;
  }

  #materializeActivityMapItems(map: ActivityMapState): void {
    while (
      !map.terminal &&
      map.inFlight.size < map.task.maxInFlight &&
      map.nextOrdinal < map.inputs.length
    ) {
      const ordinal = map.nextOrdinal++;
      const activityId = `${map.task.mapCommandId.runId}:map:${map.task.mapCommandId.seq}:${ordinal}`;
      const task: ActivityTask = {
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
      };
      map.inFlight.add(ordinal);
      this.#insertActivity({
        namespace: map.namespace,
        task,
        claim: null,
        availableAtMs: 0,
        terminalEventId: null
      });
    }
    this.#insertActivityMap(map);
  }

  #completeActivityMapItem(activity: ActivityState, result: PayloadRef): EventId {
    const map = this.#activityMapForTask(activity.task);
    if (!map || activity.task.mapItem === null) {
      throw new Error("activity map item missing descriptor");
    }
    if (map.terminal) {
      activity.terminalEventId = tailEventId(this.#stateForRun(map.runId));
      activity.claim = null;
      this.#insertActivity(activity);
      return activity.terminalEventId;
    }
    const ordinal = activity.task.mapItem.itemOrdinal;
    map.results[ordinal] = result;
    map.inFlight.delete(ordinal);
    activity.terminalEventId = tailEventId(this.#stateForRun(map.runId));
    activity.claim = null;
    this.#insertActivity(activity);
    this.#materializeActivityMapItems(map);
    return this.#completeActivityMapIfDone(map);
  }

  #failActivityMapItem(activity: ActivityState, failure: DurableFailure): EventId {
    const map = this.#activityMapForTask(activity.task);
    if (!map || activity.task.mapItem === null) {
      throw new Error("activity map item missing descriptor");
    }
    if (map.terminal) {
      activity.terminalEventId = tailEventId(this.#stateForRun(map.runId));
      activity.claim = null;
      this.#insertActivity(activity);
      return activity.terminalEventId;
    }
    map.terminal = true;
    map.inFlight.clear();
    activity.terminalEventId = tailEventId(this.#stateForRun(map.runId));
    activity.claim = null;
    const workflow = this.#stateForRun(map.runId);
    const event = makeHistoryEvent(eventId(Number(tailEventId(workflow)) + 1), {
      kind: "ActivityMapFailed",
      failed: {
        commandId: map.task.mapCommandId,
        failure
      }
    });
    workflow.history.push(event);
    workflow.readyReason = "ActivityMapFailed";
    this.#saveWorkflow(workflow);
    this.#insertActivity(activity);
    this.#insertActivityMap(map);
    return event.eventId;
  }

  #completeActivityMapIfDone(map: ActivityMapState): EventId {
    const workflow = this.#stateForRun(map.runId);
    if (map.terminal) {
      return tailEventId(workflow);
    }
    if (map.results.some((result) => result === null)) {
      this.#insertActivityMap(map);
      return tailEventId(workflow);
    }
    map.terminal = true;
    const results = map.results as PayloadRef[];
    const resultManifest = encodeActivityMapResultManifest(
      map.task.resultManifestName,
      results
    );
    const event = makeHistoryEvent(eventId(Number(tailEventId(workflow)) + 1), {
      kind: "ActivityMapCompleted",
      completed: {
        commandId: map.task.mapCommandId,
        resultManifest,
        itemCount: results.length,
        successCount: results.length,
        failureCount: 0
      }
    });
    workflow.history.push(event);
    workflow.readyReason = "ActivityMapCompleted";
    this.#saveWorkflow(workflow);
    this.#insertActivityMap(map);
    return event.eventId;
  }

  #startContinuedRun(previous: WorkflowState, input: PayloadRef): void {
    const newRunId = runId(`run-${this.#nextCounter("run")}`);
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
    this.#insertWorkflow(next, true);
  }

  #startChildWorkflow(parent: WorkflowState, requested: ChildWorkflowStartRequested): void {
    if (this.#currentRunId(parent.namespace, String(requested.workflowId)) !== null) {
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

    const childRunId = runId(`run-${this.#nextCounter("run")}`);
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
    this.#insertWorkflow(child, true);
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
      runId: workflow.runId,
      task,
      inputs,
      outcomes: Array.from({ length: inputs.length }, () => null),
      inFlight: new Set(),
      nextOrdinal: 0,
      terminal: false
    };
    this.#insertChildWorkflowMap(map);
    this.#materializeChildWorkflowMapItems(map);
    this.#completeChildWorkflowMapIfDone(map);
  }

  #insertChildWorkflowMap(map: ChildWorkflowMapState): void {
    this.#db.prepare(`
      insert or replace into child_workflow_maps(
        command_key, namespace, run_id, task, inputs, outcomes, in_flight, next_ordinal, terminal
      ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      commandKey(map.task.mapCommandId),
      map.namespace,
      String(map.runId),
      stringifyJson(map.task),
      stringifyJson(map.inputs),
      stringifyJson(map.outcomes),
      stringifyJson([...map.inFlight]),
      map.nextOrdinal,
      map.terminal ? 1 : 0
    );
    this.#replaceChildWorkflowMapItemRows(map);
  }

  #replaceChildWorkflowMapItemRows(map: ChildWorkflowMapState): void {
    const key = commandKey(map.task.mapCommandId);
    this.#db.prepare("delete from child_workflow_map_items where command_key = ?").run(key);
    const insert = this.#db.prepare(`
      insert into child_workflow_map_items(
        command_key, namespace, run_id, item_ordinal, input, outcome, in_flight, terminal
      ) values (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (let ordinal = 0; ordinal < map.inputs.length; ordinal += 1) {
      const outcome = map.outcomes[ordinal] ?? null;
      insert.run(
        key,
        map.namespace,
        String(map.runId),
        ordinal,
        stringifyJson(map.inputs[ordinal]),
        outcome === null ? null : stringifyJson(outcome),
        map.inFlight.has(ordinal) ? 1 : 0,
        map.terminal || outcome !== null ? 1 : 0
      );
    }
  }

  #childWorkflowMapForCommand(id: CommandId): ChildWorkflowMapState | undefined {
    const row = this.#db.prepare("select * from child_workflow_maps where command_key = ?").get(
      commandKey(id)
    ) as ChildWorkflowMapRow | undefined;
    return row ? childWorkflowMapStateFromRow(row) : undefined;
  }

  #materializeChildWorkflowMapItems(map: ChildWorkflowMapState): void {
    while (
      !map.terminal &&
      map.inFlight.size < map.task.maxInFlight &&
      map.nextOrdinal < map.inputs.length
    ) {
      const ordinal = map.nextOrdinal++;
      const childWorkflowId = `${map.task.workflowIdPrefix}/${ordinal}`;
      if (this.#currentRunId(map.namespace, childWorkflowId) !== null) {
        this.#recordChildWorkflowMapItemFailure(map, ordinal, {
          errorType: "durust.child_workflow_id_conflict",
          message: `child workflow id already exists: ${childWorkflowId}`,
          nonRetryable: true
        });
        continue;
      }

      const childRunId = runId(`run-${this.#nextCounter("run")}`);
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
          parentRunId: map.runId,
          mapCommandId: map.task.mapCommandId,
          itemOrdinal: ordinal,
          parentClosePolicy: map.task.parentClosePolicy
        }
      };
      map.inFlight.add(ordinal);
      this.#insertWorkflow(child, true);
    }
    this.#insertChildWorkflowMap(map);
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

  #notifyParentOfChildTerminal(parentLink: ParentWorkflowLink, terminal: ChildTerminalUpdate): void {
    if (parentLink.kind === "ChildWorkflowMap") {
      this.#completeChildWorkflowMapItem(parentLink, terminal);
      return;
    }
    const parent = this.#stateForRunOrNull(parentLink.parentRunId);
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
    this.#saveWorkflow(parent);
  }

  #completeChildWorkflowMapItem(
    parentLink: ChildWorkflowMapParentLink,
    terminal: ChildTerminalUpdate
  ): void {
    const map = this.#childWorkflowMapForCommand(parentLink.mapCommandId);
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
    const workflow = this.#stateForRun(map.runId);
    if (map.terminal) {
      return tailEventId(workflow);
    }
    if (map.outcomes.some((outcome) => outcome === null)) {
      this.#insertChildWorkflowMap(map);
      return tailEventId(workflow);
    }
    map.terminal = true;
    const outcomes = map.outcomes as ChildWorkflowMapItemOutcome<unknown>[];
    const resultManifest = encodeChildWorkflowMapResultManifest(
      map.task.resultManifestName,
      outcomes
    );
    const event = makeHistoryEvent(eventId(Number(tailEventId(workflow)) + 1), {
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
    workflow.history.push(event);
    workflow.readyReason = "ChildWorkflowMapCompleted";
    this.#saveWorkflow(workflow);
    this.#insertChildWorkflowMap(map);
    return event.eventId;
  }

  #failChildWorkflowMap(map: ChildWorkflowMapState, failure: DurableFailure): EventId {
    const workflow = this.#stateForRun(map.runId);
    if (map.terminal) {
      return tailEventId(workflow);
    }
    map.terminal = true;
    map.inFlight.clear();
    const event = makeHistoryEvent(eventId(Number(tailEventId(workflow)) + 1), {
      kind: "ChildWorkflowMapFailed",
      failed: {
        commandId: map.task.mapCommandId,
        failure
      }
    });
    workflow.history.push(event);
    workflow.readyReason = "ChildWorkflowMapFailed";
    this.#saveWorkflow(workflow);
    this.#insertChildWorkflowMap(map);
    this.#cancelRunningChildWorkflowMapItems(map);
    return event.eventId;
  }

  #cancelRunningChildWorkflowMapItems(map: ChildWorkflowMapState): void {
    const rows = this.#db.prepare("select * from workflows where terminal = 0 and parent is not null")
      .all() as unknown as WorkflowRow[];
    for (const row of rows) {
      const child = this.#workflowStateFromRow(row);
      if (
        child.parent?.kind !== "ChildWorkflowMap" ||
        child.parent.parentRunId !== map.runId ||
        !sameCommandId(child.parent.mapCommandId, map.task.mapCommandId)
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
      this.#saveWorkflow(child);
    }
  }

  #cancelChildrenForClosedParent(parent: WorkflowState): void {
    const rows = this.#db.prepare("select * from workflows where terminal = 0 and parent is not null")
      .all() as unknown as WorkflowRow[];
    for (const row of rows) {
      const child = this.#workflowStateFromRow(row);
      if (
        child.parent?.parentRunId !== parent.runId ||
        child.parent.parentClosePolicy !== "Cancel"
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
      this.#saveWorkflow(child);
    }
  }
}

type ChildTerminalUpdate =
  | { readonly kind: "Completed"; readonly result: PayloadRef }
  | { readonly kind: "Failed"; readonly failure: DurableFailure }
  | { readonly kind: "Cancelled"; readonly reason: string };

function workflowStateFromRowWithHistory(
  row: WorkflowRow,
  history: readonly HistoryEvent[]
): WorkflowState {
  return {
    namespace: row.namespace,
    workflowId: row.workflow_id,
    workflowType: parseJson<WorkflowType>(row.workflow_type),
    taskQueue: row.task_queue,
    runId: runId(row.run_id),
    history: [...history],
    readyReason: row.ready_reason as WorkflowTaskReason | null,
    claim:
      row.claim_worker === null || row.claim_token === null
        ? null
        : {
            claim: {
              runId: runId(row.run_id),
              workerId: row.claim_worker,
              token: row.claim_token
            },
            reason: (row.claim_reason ?? row.ready_reason ?? "CacheEvicted") as WorkflowTaskReason,
            expiresAtMs: row.claim_expires_at_ms ?? 0
          },
    queryProjection: row.query_projection === null ? null : parseJson<PayloadRef>(row.query_projection),
    terminal: row.terminal !== 0,
    parent: row.parent === null ? null : parseJson<ParentWorkflowLink>(row.parent)
  };
}

function historyEventFromRow(row: HistoryEventRow): HistoryEvent {
  const data = parseJson<HistoryEventData>(row.data);
  const expectedType = historyEventType(data);
  if (row.event_type !== expectedType) {
    throw new Error(
      `history event type mismatch: row has ${row.event_type}, data has ${expectedType}`
    );
  }
  return {
    eventId: eventId(row.event_id),
    eventType: row.event_type,
    data
  };
}

function activityStateFromRow(row: ActivityRow): ActivityState {
  const task = parseJson<ActivityTask>(row.task);
  return {
    namespace: row.namespace,
    task,
    availableAtMs: row.available_at_ms ?? 0,
    claim:
      row.claim_worker === null || row.claim_token === null
        ? null
        : {
            claim: {
              activityId: row.activity_id,
              workerId: row.claim_worker,
              token: row.claim_token
          },
          startedAtMs: row.claim_started_at_ms ?? 0,
          heartbeatDeadlineAtMs: row.heartbeat_deadline_at_ms,
          expiresAtMs: row.claim_expires_at_ms ?? 0
        },
    terminalEventId: row.terminal_event_id === null ? null : eventId(row.terminal_event_id)
  };
}

function activityRootFromRow(row: ActivityRow): {
  readonly namespace: string;
  readonly activityId: string;
  readonly runId: string | null;
  readonly commandKey: string | null;
  readonly activityName: string | null;
  readonly taskQueue: string | null;
  readonly input: PayloadRef;
} {
  return {
    namespace: row.namespace,
    activityId: row.activity_id,
    runId: row.run_id,
    commandKey: row.command_key,
    activityName: row.activity_name,
    taskQueue: row.task_queue,
    input:
      row.input === null
        ? parseJson<ActivityTask>(row.task).input as PayloadRef
        : parseJson<PayloadRef>(row.input)
  };
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

function activityMapStateFromRow(row: ActivityMapRow): ActivityMapState {
  return {
    namespace: row.namespace,
    runId: runId(row.run_id),
    task: parseJson<ActivityMapTask>(row.task),
    inputs: parseJson<PayloadRef[]>(row.inputs),
    results: parseJson<(PayloadRef | null)[]>(row.results),
    inFlight: new Set(parseJson<number[]>(row.in_flight)),
    nextOrdinal: row.next_ordinal,
    terminal: row.terminal !== 0
  };
}

function activityMapDescriptorRootFromRow(row: ActivityMapRow): {
  readonly commandKey: string;
  readonly namespace: string;
  readonly runId: RunId;
  readonly task: ActivityMapTask;
  readonly nextOrdinal: number;
  readonly terminal: boolean;
} {
  return {
    commandKey: row.command_key,
    namespace: row.namespace,
    runId: runId(row.run_id),
    task: parseJson<ActivityMapTask>(row.task),
    nextOrdinal: row.next_ordinal,
    terminal: row.terminal !== 0
  };
}

function activityMapItemRootFromRow(row: ActivityMapItemRow): {
  readonly commandKey: string;
  readonly namespace: string;
  readonly runId: RunId;
  readonly itemOrdinal: number;
  readonly input: PayloadRef;
  readonly result: PayloadRef | null;
  readonly inFlight: boolean;
  readonly terminal: boolean;
} {
  return {
    commandKey: row.command_key,
    namespace: row.namespace,
    runId: runId(row.run_id),
    itemOrdinal: row.item_ordinal,
    input: parseJson<PayloadRef>(row.input),
    result: row.result === null ? null : parseJson<PayloadRef>(row.result),
    inFlight: row.in_flight !== 0,
    terminal: row.terminal !== 0
  };
}

function childWorkflowMapStateFromRow(row: ChildWorkflowMapRow): ChildWorkflowMapState {
  return {
    namespace: row.namespace,
    runId: runId(row.run_id),
    task: parseJson<ChildWorkflowMapTask>(row.task),
    inputs: parseJson<PayloadRef[]>(row.inputs),
    outcomes: parseJson<(ChildWorkflowMapItemOutcome<unknown> | null)[]>(row.outcomes),
    inFlight: new Set(parseJson<number[]>(row.in_flight)),
    nextOrdinal: row.next_ordinal,
    terminal: row.terminal !== 0
  };
}

function childWorkflowMapDescriptorRootFromRow(row: ChildWorkflowMapRow): {
  readonly commandKey: string;
  readonly namespace: string;
  readonly runId: RunId;
  readonly task: ChildWorkflowMapTask;
  readonly nextOrdinal: number;
  readonly terminal: boolean;
} {
  return {
    commandKey: row.command_key,
    namespace: row.namespace,
    runId: runId(row.run_id),
    task: parseJson<ChildWorkflowMapTask>(row.task),
    nextOrdinal: row.next_ordinal,
    terminal: row.terminal !== 0
  };
}

function childWorkflowMapItemRootFromRow(row: ChildWorkflowMapItemRow): {
  readonly commandKey: string;
  readonly namespace: string;
  readonly runId: RunId;
  readonly itemOrdinal: number;
  readonly input: PayloadRef;
  readonly outcome: ChildWorkflowMapItemOutcome<unknown> | null;
  readonly inFlight: boolean;
  readonly terminal: boolean;
} {
  return {
    commandKey: row.command_key,
    namespace: row.namespace,
    runId: runId(row.run_id),
    itemOrdinal: row.item_ordinal,
    input: parseJson<PayloadRef>(row.input),
    outcome: row.outcome === null ? null : parseJson<ChildWorkflowMapItemOutcome<unknown>>(row.outcome),
    inFlight: row.in_flight !== 0,
    terminal: row.terminal !== 0
  };
}

function workflowTypeKey(workflowTypeValue: WorkflowType): string {
  return `${workflowTypeValue.name}@${workflowTypeValue.version}`;
}

function commandKey(id: CommandId): string {
  return `${id.runId}:${id.seq}`;
}

function sameCommandId(left: CommandId, right: CommandId): boolean {
  return left.runId === right.runId && Number(left.seq) === Number(right.seq);
}

function commandIdFromParts(runIdValue: string, seq: number): CommandId {
  return {
    runId: runId(runIdValue),
    seq: seq as CommandId["seq"]
  };
}

function tailEventId(state: WorkflowState): EventId {
  return state.history.at(-1)?.eventId ?? eventId(0);
}

function workflowLeaseMatches(lease: WorkflowLease, claim: WorkflowTaskClaim): boolean {
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

function stringifyJson(value: unknown): string {
  return JSON.stringify(value, (_key, nested) =>
    nested instanceof Uint8Array
      ? { __durustType: "Uint8Array", data: [...nested] }
      : nested
  );
}

function parseJson<T>(value: string): T {
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
