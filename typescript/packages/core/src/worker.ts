import type {
  ClaimedWorkflowTask,
  CommitOutcome,
  CompleteActivityItemOutcome,
  CompleteActivityOutcome,
  CompleteActivityRequest,
  DurableBackend,
  HistoryChunk,
  WorkflowTaskCommit
} from "./backend.js";
import type { SignalInboxRecord } from "./backend.js";
import type { DurableFailure, WorkflowDefinition } from "./api.js";
import { runWithActivityExecutionContext } from "./activity-context.js";
import type { HistoryEvent } from "./history.js";
import { historyEventType } from "./history.js";
import { decodePayload, encodePayload, type CodecId, type PayloadRef } from "./payload.js";
import type { Registry } from "./registry.js";
import { HotWorkflowExecution } from "./runtime.js";
import {
  eventId,
  type ActivityName,
  type EventId,
  type Namespace,
  type RunId,
  type SignalName,
  type TaskQueue,
  type WorkerId,
  type WorkflowType
} from "./types.js";

export interface WorkerOptions {
  readonly backend: DurableBackend;
  readonly registry: Registry;
  readonly namespace?: Namespace | string;
  readonly workerId: WorkerId | string;
  readonly workflowTaskQueue: TaskQueue | string;
  readonly activityTaskQueue?: TaskQueue | string;
  readonly registeredSignalNames?: readonly (SignalName | string)[];
  readonly leaseDurationMs?: number;
  readonly payloadCodec?: CodecId;
  readonly maxLocalActivitiesPerWorkflowTask?: number;
  readonly activityCompletionBatchSize?: number;
  readonly historyFetchMaxEvents?: number;
  readonly historyFetchMaxBytes?: number;
  readonly workflowHistoryCacheSize?: number;
  readonly workflowExecutionCacheSize?: number;
  readonly onEvent?: WorkerEventSink;
}

export type RunWorkflowTaskOnceOutcome =
  | { readonly kind: "NoTask" }
  | {
      readonly kind: "Committed";
      readonly runId: RunId;
      readonly outcome: CommitOutcome;
      readonly localActivityTasks: number;
    };

export type RunActivityTaskOnceOutcome =
  | { readonly kind: "NoTask" }
  | {
      readonly kind: "Completed";
      readonly activityId: string;
      readonly outcome: CompleteActivityOutcome;
    }
  | {
      readonly kind: "Failed";
      readonly activityId: string;
      readonly outcome: Awaited<ReturnType<DurableBackend["failActivity"]>>;
    };

export type RunActivityTaskBatchOnceOutcome =
  | { readonly kind: "NoTask" }
  | { readonly kind: "Processed"; readonly tasks: number };

export interface WorkerRunOptions {
  readonly signal?: AbortSignal;
  readonly maxIterations?: number;
  readonly idleBackoffMs?: number;
  readonly maxIdleBackoffMs?: number;
  readonly errorBackoffMs?: number;
  readonly maxErrorBackoffMs?: number;
  readonly runTimerMaintenance?: boolean;
  readonly timerMaintenanceLimit?: number;
  readonly activityTimeoutMaintenanceLimit?: number;
  readonly onError?: (error: unknown) => void | Promise<void>;
}

export interface WorkerRunOutcome {
  readonly stopReason: "abort" | "maxIterations";
  readonly iterations: number;
  readonly workflowTasks: number;
  readonly activityTasks: number;
  readonly timersFired: number;
  readonly idleSleeps: number;
  readonly errors: number;
}

export type WorkerEventSink = (event: WorkerEvent) => void | Promise<void>;

export interface WorkerErrorInfo {
  readonly name: string;
  readonly message: string;
}

export type WorkerEvent =
  | {
      readonly kind: "WorkflowTaskClaimed";
      readonly runId: RunId;
      readonly workflowType: WorkflowType;
      readonly reason: string;
    }
  | {
      readonly kind: "WorkflowTaskCommitted";
      readonly runId: RunId;
      readonly outcome: CommitOutcome;
      readonly localActivityTasks: number;
    }
  | {
      readonly kind: "ActivityTaskClaimed";
      readonly activityId: string;
      readonly activityName: string;
      readonly attempt: number;
      readonly batched: boolean;
    }
  | {
      readonly kind: "ActivityTaskCompleted";
      readonly activityId: string;
      readonly outcome: CompleteActivityOutcome;
      readonly batched: boolean;
    }
  | {
      readonly kind: "ActivityTaskFailed";
      readonly activityId: string;
      readonly outcome: Awaited<ReturnType<DurableBackend["failActivity"]>>;
      readonly batched: boolean;
    }
  | {
      readonly kind: "ActivityCompletionBatchFlushed";
      readonly completions: number;
      readonly accepted: number;
      readonly rejected: number;
      readonly results: readonly CompleteActivityItemOutcome[];
    }
  | { readonly kind: "TimersFired"; readonly fired: number }
  | { readonly kind: "ActivityTasksTimedOut"; readonly timedOut: number }
  | { readonly kind: "WorkerLoopError"; readonly error: WorkerErrorInfo };

export interface WorkerMetricsSnapshot {
  readonly workflowTaskClaims: number;
  readonly workflowTaskNoTasks: number;
  readonly workflowTaskCommits: number;
  readonly workflowTaskConflicts: number;
  readonly activityTaskClaims: number;
  readonly activityTaskNoTasks: number;
  readonly activityTaskCompletions: number;
  readonly activityTaskFailures: number;
  readonly activityCompletionBatches: number;
  readonly activityCompletionBatchItems: number;
  readonly workflowHistoryCacheHits: number;
  readonly workflowHistoryCacheMisses: number;
  readonly workflowHistoryCacheEvictions: number;
  readonly workflowExecutionCacheHits: number;
  readonly workflowExecutionCacheMisses: number;
  readonly workflowExecutionCacheEvictions: number;
  readonly historyStreamChunks: number;
  readonly historyStreamEvents: number;
  readonly timersFired: number;
  readonly loopErrors: number;
  readonly idleSleeps: number;
  readonly eventSinkErrors: number;
}

interface MutableWorkerMetrics {
  workflowTaskClaims: number;
  workflowTaskNoTasks: number;
  workflowTaskCommits: number;
  workflowTaskConflicts: number;
  activityTaskClaims: number;
  activityTaskNoTasks: number;
  activityTaskCompletions: number;
  activityTaskFailures: number;
  activityCompletionBatches: number;
  activityCompletionBatchItems: number;
  workflowHistoryCacheHits: number;
  workflowHistoryCacheMisses: number;
  workflowHistoryCacheEvictions: number;
  workflowExecutionCacheHits: number;
  workflowExecutionCacheMisses: number;
  workflowExecutionCacheEvictions: number;
  historyStreamChunks: number;
  historyStreamEvents: number;
  timersFired: number;
  loopErrors: number;
  idleSleeps: number;
  eventSinkErrors: number;
}

interface WorkflowExecutionCacheEntry {
  readonly execution: HotWorkflowExecution;
  tailEventId: EventId;
  ingestedEventId: EventId;
  readonly workflowType: WorkflowType;
}

interface PreparedWorkflowExecution {
  readonly cacheKey: string | null;
  readonly execution: HotWorkflowExecution;
  readonly claim: ClaimedWorkflowTask;
  readonly replayClaim: ClaimedWorkflowTask | null;
  readonly commit: WorkflowTaskCommit;
}

export class Worker {
  readonly #backend: DurableBackend;
  readonly #registry: Registry;
  readonly #namespace: Namespace | string;
  readonly #workerId: WorkerId | string;
  readonly #workflowTaskQueue: TaskQueue | string;
  readonly #activityTaskQueue: TaskQueue | string | null;
  readonly #registeredSignalNames: readonly (SignalName | string)[];
  readonly #leaseDurationMs: number;
  readonly #payloadCodec: CodecId;
  readonly #maxLocalActivitiesPerWorkflowTask: number;
  readonly #activityCompletionBatchSize: number;
  readonly #historyFetchMaxEvents: number;
  readonly #historyFetchMaxBytes: number;
  readonly #workflowHistoryCacheSize: number;
  readonly #workflowExecutionCacheSize: number;
  readonly #eventSink: WorkerEventSink | undefined;
  readonly #metrics: MutableWorkerMetrics = emptyWorkerMetrics();
  readonly #workflowHistoryCache = new Map<string, readonly HistoryEvent[]>();
  readonly #workflowExecutionCache = new Map<string, WorkflowExecutionCacheEntry>();

  constructor(options: WorkerOptions) {
    this.#backend = options.backend;
    this.#registry = options.registry;
    this.#namespace = options.namespace ?? "default";
    this.#workerId = options.workerId;
    this.#workflowTaskQueue = options.workflowTaskQueue;
    this.#activityTaskQueue = options.activityTaskQueue ?? null;
    this.#registeredSignalNames = options.registeredSignalNames ?? [];
    this.#leaseDurationMs = options.leaseDurationMs ?? 30_000;
    this.#payloadCodec = options.payloadCodec ?? "MessagePack";
    this.#maxLocalActivitiesPerWorkflowTask = Math.max(
      0,
      Math.trunc(options.maxLocalActivitiesPerWorkflowTask ?? 0)
    );
    this.#activityCompletionBatchSize = Math.max(
      1,
      Math.trunc(options.activityCompletionBatchSize ?? 1)
    );
    this.#historyFetchMaxEvents = Math.max(
      1,
      Math.trunc(options.historyFetchMaxEvents ?? 128)
    );
    this.#historyFetchMaxBytes = Math.max(
      1,
      Math.trunc(options.historyFetchMaxBytes ?? 1_048_576)
    );
    this.#workflowHistoryCacheSize = Math.max(
      0,
      Math.trunc(options.workflowHistoryCacheSize ?? 1024)
    );
    this.#workflowExecutionCacheSize = Math.max(
      0,
      Math.trunc(options.workflowExecutionCacheSize ?? 1024)
    );
    this.#eventSink = options.onEvent;
  }

  metrics(): WorkerMetricsSnapshot {
    return { ...this.#metrics };
  }

  async run(options: WorkerRunOptions = {}): Promise<WorkerRunOutcome> {
    const stats = {
      iterations: 0,
      workflowTasks: 0,
      activityTasks: 0,
      timersFired: 0,
      idleSleeps: 0,
      errors: 0
    };
    const maxIterations = options.maxIterations;
    const initialIdleBackoffMs = Math.max(0, options.idleBackoffMs ?? 50);
    const maxIdleBackoffMs = Math.max(initialIdleBackoffMs, options.maxIdleBackoffMs ?? 1_000);
    const initialErrorBackoffMs = Math.max(0, options.errorBackoffMs ?? 250);
    const maxErrorBackoffMs = Math.max(initialErrorBackoffMs, options.maxErrorBackoffMs ?? 5_000);
    const timerMaintenanceLimit = Math.max(1, options.timerMaintenanceLimit ?? 64);
    const activityTimeoutMaintenanceLimit = Math.max(
      1,
      options.activityTimeoutMaintenanceLimit ?? timerMaintenanceLimit
    );
    let idleBackoffMs = initialIdleBackoffMs;
    let errorBackoffMs = initialErrorBackoffMs;

    while (!options.signal?.aborted && (maxIterations === undefined || stats.iterations < maxIterations)) {
      stats.iterations += 1;
      try {
        const madeProgress = await this.#runLoopIteration(stats, {
          signal: options.signal,
          runTimerMaintenance: options.runTimerMaintenance ?? true,
          timerMaintenanceLimit,
          activityTimeoutMaintenanceLimit
        });
        errorBackoffMs = initialErrorBackoffMs;
        if (madeProgress) {
          idleBackoffMs = initialIdleBackoffMs;
          continue;
        }

        stats.idleSleeps += 1;
        this.#metrics.idleSleeps += 1;
        if ((await sleepWithAbort(idleBackoffMs, options.signal)) === "aborted") {
          break;
        }
        idleBackoffMs = nextBackoff(idleBackoffMs, maxIdleBackoffMs);
      } catch (error) {
        stats.errors += 1;
        this.#metrics.loopErrors += 1;
        await this.#emit({ kind: "WorkerLoopError", error: workerErrorInfo(error) });
        await options.onError?.(error);
        if ((await sleepWithAbort(errorBackoffMs, options.signal)) === "aborted") {
          break;
        }
        errorBackoffMs = nextBackoff(errorBackoffMs, maxErrorBackoffMs);
      }
    }

    return {
      stopReason: options.signal?.aborted ? "abort" : "maxIterations",
      ...stats
    };
  }

  async runWorkflowTaskOnce(): Promise<RunWorkflowTaskOnceOutcome> {
    return await this.#runWorkflowTaskOnce();
  }

  async #runWorkflowTaskOnce(signal?: AbortSignal): Promise<RunWorkflowTaskOnceOutcome> {
    const claimed = await this.#backend.claimWorkflowTask(this.#workerId, {
      namespace: this.#namespace,
      taskQueue: this.#workflowTaskQueue,
      registeredWorkflowTypes: this.#registeredWorkflowTypes(),
      leaseDurationMs: this.#leaseDurationMs
    });
    if (!claimed) {
      this.#metrics.workflowTaskNoTasks += 1;
      return { kind: "NoTask" };
    }
    this.#metrics.workflowTaskClaims += 1;
    await this.#emit({
      kind: "WorkflowTaskClaimed",
      runId: claimed.runId,
      workflowType: claimed.workflowType,
      reason: claimed.reason
    });

    const definition = this.#registry.workflow(
      claimed.workflowType.name,
      claimed.workflowType.version
    );
    if (!definition) {
      throw new Error(
        `workflow is not registered: ${claimed.workflowType.name}@${claimed.workflowType.version}`
      );
    }

    const liveSignals = await this.#liveSignalsForClaim(claimed.runId);
    const prepared = await this.#prepareWorkflowTaskFromCacheOrReplay(
      definition,
      claimed,
      liveSignals
    );
    let outcome: CommitOutcome;
    try {
      outcome = await this.#backend.commitWorkflowTask(claimed.claim, prepared.commit);
    } catch (error) {
      if (prepared.cacheKey !== null) {
        this.#workflowExecutionCache.delete(prepared.cacheKey);
      }
      throw error;
    }
    if (outcome.kind === "Committed") {
      prepared.execution.markCommitted(outcome.newTailEventId);
      this.#updateWorkflowExecutionCacheAfterCommit(prepared, outcome.newTailEventId);
      if (prepared.replayClaim !== null) {
        this.#updateWorkflowHistoryCacheAfterCommit(prepared.replayClaim, prepared.commit);
      } else {
        this.#updateWorkflowHistoryCacheAfterHotCommit(
          prepared.claim,
          prepared.commit,
          outcome.newTailEventId
        );
      }
    } else if (prepared.cacheKey !== null) {
      this.#workflowExecutionCache.delete(prepared.cacheKey);
    }
    const localActivityTasks =
      outcome.kind === "Committed" && !signal?.aborted
        ? await this.#runLocalActivitiesAfterWorkflowTask(signal)
        : 0;
    if (outcome.kind === "Committed") {
      this.#metrics.workflowTaskCommits += 1;
    } else {
      this.#metrics.workflowTaskConflicts += 1;
    }
    await this.#emit({
      kind: "WorkflowTaskCommitted",
      runId: claimed.runId,
      outcome,
      localActivityTasks
    });
    return { kind: "Committed", runId: claimed.runId, outcome, localActivityTasks };
  }

  async runActivityTaskOnce(): Promise<RunActivityTaskOnceOutcome> {
    if (this.#activityTaskQueue === null) {
      throw new Error("Worker.runActivityTaskOnce requires activityTaskQueue");
    }
    const claimed = await this.#backend.claimActivityTask(this.#workerId, {
      namespace: this.#namespace,
      taskQueue: this.#activityTaskQueue,
      registeredActivityNames: this.#registeredActivityNames(),
      leaseDurationMs: this.#leaseDurationMs
    });
    if (!claimed) {
      this.#metrics.activityTaskNoTasks += 1;
      return { kind: "NoTask" };
    }
    this.#metrics.activityTaskClaims += 1;
    await this.#emit({
      kind: "ActivityTaskClaimed",
      activityId: claimed.task.activityId,
      activityName: String(claimed.task.activityName),
      attempt: claimed.task.attempt,
      batched: false
    });

    const definition = this.#registry.activity(String(claimed.task.activityName));
    if (!definition) {
      throw new Error(`activity is not registered: ${claimed.task.activityName}`);
    }

    const input = decodePayload(
      claimed.task.input as PayloadRef<unknown>,
      definition.inputSchema
    );
    try {
      const output = await runWithActivityExecutionContext(
        {
          heartbeat: (request) => this.#backend.heartbeatActivity(request),
          heartbeatRequest: { claim: claimed.claim }
        },
        () => Promise.resolve(definition.handler(input))
      );
      const result = encodePayload(output, {
        codec: this.#payloadCodec,
        ...(definition.outputSchema === undefined ? {} : { schema: definition.outputSchema })
      });
      const outcome = await this.#backend.completeActivity({
        claim: claimed.claim,
        result
      });
      this.#metrics.activityTaskCompletions += 1;
      await this.#emit({
        kind: "ActivityTaskCompleted",
        activityId: claimed.task.activityId,
        outcome,
        batched: false
      });
      return { kind: "Completed", activityId: claimed.task.activityId, outcome };
    } catch (error) {
      const outcome = await this.#backend.failActivity({
        claim: claimed.claim,
        failure: durableFailureFromUnknown(error)
      });
      this.#metrics.activityTaskFailures += 1;
      await this.#emit({
        kind: "ActivityTaskFailed",
        activityId: claimed.task.activityId,
        outcome,
        batched: false
      });
      return { kind: "Failed", activityId: claimed.task.activityId, outcome };
    }
  }

  async runActivityTaskBatchOnce(limit = this.#activityCompletionBatchSize): Promise<RunActivityTaskBatchOnceOutcome> {
    if (this.#activityTaskQueue === null) {
      throw new Error("Worker.runActivityTaskBatchOnce requires activityTaskQueue");
    }
    const tasks = await this.#runActivityTaskBatch(Math.max(1, Math.trunc(limit)));
    return tasks === 0 ? { kind: "NoTask" } : { kind: "Processed", tasks };
  }

  async runActivityTimeoutMaintenanceOnce(
    limit = 64
  ): Promise<Awaited<ReturnType<DurableBackend["timeoutDueActivities"]>>> {
    const outcome = await this.#backend.timeoutDueActivities({
      namespace: this.#namespace,
      now: Date.now(),
      limit: Math.max(1, Math.trunc(limit))
    });
    if (outcome.timedOut > 0) {
      await this.#emit({ kind: "ActivityTasksTimedOut", timedOut: outcome.timedOut });
    }
    return outcome;
  }

  #registeredWorkflowTypes(): readonly WorkflowType[] {
    return this.#registry.workflows().map((definition) => definition.workflowType);
  }

  #registeredActivityNames(): readonly (ActivityName | string)[] {
    return this.#registry.activities().map((definition) => definition.name);
  }

  async #liveSignalsForClaim(runId: RunId): Promise<readonly SignalInboxRecord[]> {
    const records: SignalInboxRecord[] = [];
    for (const signalName of this.#registeredSignalNames) {
      const record = await this.#backend.readSignalInbox({ runId, signalName });
      if (record !== null) {
        records.push(record);
      }
    }
    return records;
  }

  async #prepareWorkflowTaskFromCacheOrReplay(
    definition: WorkflowDefinition<any, any, any, string>,
    claimed: ClaimedWorkflowTask,
    liveSignals: readonly SignalInboxRecord[]
  ): Promise<PreparedWorkflowExecution> {
    const cacheKey = String(claimed.runId);
    const cached = this.#workflowExecutionCache.get(cacheKey);
    if (
      cached !== undefined &&
      !cached.execution.closed &&
      sameWorkflowType(cached.workflowType, claimed.workflowType) &&
      Number(cached.tailEventId) <= Number(claimed.replayTargetEventId)
    ) {
      this.#touchWorkflowExecutionCacheEntry(cacheKey, cached);
      const hotClaim = await this.#claimWithHotWakeHistory(cached, claimed);
      if (hotClaim.prefetchedHistory.every(isHotWorkflowWakeEvent)) {
        this.#metrics.workflowExecutionCacheHits += 1;
        const commit = await cached.execution.advance(hotClaim, { liveSignals });
        return {
          cacheKey,
          execution: cached.execution,
          claim: hotClaim,
          replayClaim: null,
          commit
        };
      }
      this.#workflowExecutionCache.delete(cacheKey);
    }

    if (cached !== undefined) {
      this.#workflowExecutionCache.delete(cacheKey);
    }
    this.#metrics.workflowExecutionCacheMisses += 1;
    const replayClaim = await this.#claimWithCompleteReplayHistory(claimed);
    const input = decodePayload(
      workflowStartedInput(replayClaim.prefetchedHistory) as PayloadRef<unknown>,
      definition.inputSchema
    );
    const execution = new HotWorkflowExecution(definition, input as object, replayClaim, {
      payloadCodec: this.#payloadCodec,
      defaultWorkflowTaskQueue: String(this.#workflowTaskQueue),
      liveSignals
    });
    const commit = await execution.nextCommit();
    return {
      cacheKey,
      execution,
      claim: replayClaim,
      replayClaim,
      commit
    };
  }

  async #claimWithHotWakeHistory(
    cached: WorkflowExecutionCacheEntry,
    claimed: ClaimedWorkflowTask
  ): Promise<ClaimedWorkflowTask> {
    const target = Number(claimed.replayTargetEventId);
    let afterEventId = cached.ingestedEventId;
    const wakeHistory = contiguousHistoryRange(
      claimed.prefetchedHistory,
      afterEventId,
      claimed.replayTargetEventId
    );
    afterEventId = wakeHistory.at(-1)?.eventId ?? afterEventId;

    while (Number(afterEventId) < target) {
      const chunk = await this.#backend.streamHistory({
        runId: claimed.runId,
        afterEventId,
        upToEventId: claimed.replayTargetEventId,
        maxEvents: this.#historyFetchMaxEvents,
        maxBytes: this.#historyFetchMaxBytes
      });
      this.#recordHistoryChunk(chunk);
      if (chunk.events.length === 0) {
        throw new Error(
          `streamHistory returned no events for hot ${claimed.runId} after ${afterEventId} before replay target ${claimed.replayTargetEventId}`
        );
      }
      wakeHistory.push(...chunk.events);
      const nextAfterEventId = chunk.lastEventId;
      if (Number(nextAfterEventId) <= Number(afterEventId)) {
        throw new Error(
          `streamHistory did not advance for hot ${claimed.runId}: still at ${nextAfterEventId}`
        );
      }
      afterEventId = nextAfterEventId;
    }

    assertContiguousHistoryRange(wakeHistory, cached.ingestedEventId, claimed.replayTargetEventId);
    return { ...claimed, prefetchedHistory: wakeHistory };
  }

  async #claimWithCompleteReplayHistory(
    claimed: ClaimedWorkflowTask
  ): Promise<ClaimedWorkflowTask> {
    const target = Number(claimed.replayTargetEventId);
    const history = contiguousHistoryPrefix(claimed.prefetchedHistory, claimed.replayTargetEventId);
    this.#mergeCachedWorkflowHistory(claimed.runId, history, claimed.replayTargetEventId);
    let afterEventId = history.at(-1)?.eventId ?? eventId(0);

    while (Number(afterEventId) < target) {
      const chunk = await this.#backend.streamHistory({
        runId: claimed.runId,
        afterEventId,
        upToEventId: claimed.replayTargetEventId,
        maxEvents: this.#historyFetchMaxEvents,
        maxBytes: this.#historyFetchMaxBytes
      });
      this.#recordHistoryChunk(chunk);
      if (chunk.events.length === 0) {
        throw new Error(
          `streamHistory returned no events for ${claimed.runId} after ${afterEventId} before replay target ${claimed.replayTargetEventId}`
        );
      }
      history.push(...chunk.events);
      const nextAfterEventId = chunk.lastEventId;
      if (Number(nextAfterEventId) <= Number(afterEventId)) {
        throw new Error(
          `streamHistory did not advance for ${claimed.runId}: still at ${nextAfterEventId}`
        );
      }
      afterEventId = nextAfterEventId;
    }

    assertContiguousHistoryThroughTarget(history, claimed.replayTargetEventId);
    this.#storeWorkflowHistory(claimed.runId, history);
    return { ...claimed, prefetchedHistory: history };
  }

  #mergeCachedWorkflowHistory(
    runIdValue: RunId,
    history: HistoryEvent[],
    targetEventId: EventId
  ): void {
    const cached = this.#workflowHistoryCache.get(String(runIdValue));
    if (cached === undefined) {
      this.#metrics.workflowHistoryCacheMisses += 1;
      return;
    }
    const cachedPrefix = contiguousHistoryPrefix(cached, targetEventId);
    if (cachedPrefix.length <= history.length) {
      this.#metrics.workflowHistoryCacheMisses += 1;
      this.#touchWorkflowHistoryCacheEntry(runIdValue, cached);
      return;
    }
    history.splice(0, history.length, ...cachedPrefix);
    this.#metrics.workflowHistoryCacheHits += 1;
    this.#touchWorkflowHistoryCacheEntry(runIdValue, cached);
  }

  #updateWorkflowHistoryCacheAfterCommit(
    claim: ClaimedWorkflowTask,
    commit: WorkflowTaskCommit
  ): void {
    const history = [...claim.prefetchedHistory];
    for (const [index, event] of (commit.appendEvents ?? []).entries()) {
      const nextEventId = eventId(Number(commit.expectedTailEventId) + index + 1);
      history.push({
        eventId: nextEventId,
        eventType: historyEventType(event.data),
        data: event.data
      });
    }
    this.#storeWorkflowHistory(claim.runId, history);
  }

  #updateWorkflowHistoryCacheAfterHotCommit(
    claim: ClaimedWorkflowTask,
    commit: WorkflowTaskCommit,
    newTailEventId: EventId
  ): void {
    const cached = this.#workflowHistoryCache.get(String(claim.runId));
    if (cached === undefined || Number(cached.at(-1)?.eventId ?? 0) !== Number(commit.expectedTailEventId)) {
      return;
    }
    const history = [...cached];
    for (const [index, event] of (commit.appendEvents ?? []).entries()) {
      const nextEventId = eventId(Number(commit.expectedTailEventId) + index + 1);
      history.push({
        eventId: nextEventId,
        eventType: historyEventType(event.data),
        data: event.data
      });
    }
    if (Number(history.at(-1)?.eventId ?? 0) <= Number(newTailEventId)) {
      this.#storeWorkflowHistory(claim.runId, history);
    }
  }

  #updateWorkflowExecutionCacheAfterCommit(
    prepared: PreparedWorkflowExecution,
    newTailEventId: EventId
  ): void {
    if (prepared.cacheKey === null) {
      return;
    }
    if (prepared.execution.closed || this.#workflowExecutionCacheSize === 0) {
      this.#workflowExecutionCache.delete(prepared.cacheKey);
      return;
    }
    this.#storeWorkflowExecution(prepared.cacheKey, {
      execution: prepared.execution,
      tailEventId: newTailEventId,
      ingestedEventId: eventId(
        Number(prepared.commit.expectedTailEventId) + (prepared.commit.appendEvents?.length ?? 0)
      ),
      workflowType: prepared.claim.workflowType
    });
  }

  #storeWorkflowHistory(runIdValue: RunId, history: readonly HistoryEvent[]): void {
    if (this.#workflowHistoryCacheSize === 0 || history.length === 0) {
      return;
    }
    const key = String(runIdValue);
    this.#workflowHistoryCache.delete(key);
    this.#workflowHistoryCache.set(key, [...history]);
    while (this.#workflowHistoryCache.size > this.#workflowHistoryCacheSize) {
      const oldest = this.#workflowHistoryCache.keys().next().value as string | undefined;
      if (oldest === undefined) {
        return;
      }
      this.#workflowHistoryCache.delete(oldest);
      this.#metrics.workflowHistoryCacheEvictions += 1;
    }
  }

  #storeWorkflowExecution(key: string, entry: WorkflowExecutionCacheEntry): void {
    this.#workflowExecutionCache.delete(key);
    this.#workflowExecutionCache.set(key, entry);
    while (this.#workflowExecutionCache.size > this.#workflowExecutionCacheSize) {
      const oldest = this.#workflowExecutionCache.keys().next().value as string | undefined;
      if (oldest === undefined) {
        return;
      }
      this.#workflowExecutionCache.delete(oldest);
      this.#metrics.workflowExecutionCacheEvictions += 1;
    }
  }

  #touchWorkflowExecutionCacheEntry(
    key: string,
    entry: WorkflowExecutionCacheEntry
  ): void {
    this.#workflowExecutionCache.delete(key);
    this.#workflowExecutionCache.set(key, entry);
  }

  #touchWorkflowHistoryCacheEntry(
    runIdValue: RunId,
    history: readonly HistoryEvent[]
  ): void {
    const key = String(runIdValue);
    this.#workflowHistoryCache.delete(key);
    this.#workflowHistoryCache.set(key, history);
  }

  #recordHistoryChunk(chunk: HistoryChunk): void {
    this.#metrics.historyStreamChunks += 1;
    this.#metrics.historyStreamEvents += chunk.events.length;
  }

  async #runLoopIteration(
    stats: {
      workflowTasks: number;
      activityTasks: number;
      timersFired: number;
    },
    options: {
      readonly signal: AbortSignal | undefined;
      readonly runTimerMaintenance: boolean;
      readonly timerMaintenanceLimit: number;
      readonly activityTimeoutMaintenanceLimit: number;
    }
  ): Promise<boolean> {
    let madeProgress = false;

    const workflow = await this.#runWorkflowTaskOnce(options.signal);
    if (workflow.kind !== "NoTask") {
      stats.workflowTasks += 1;
      stats.activityTasks += workflow.localActivityTasks;
      madeProgress = true;
    }

    if (options.signal?.aborted) {
      return madeProgress;
    }

    if (this.#activityTaskQueue !== null) {
      const activityTasks = await this.#runActivityTasksForLoop(options.signal);
      if (activityTasks > 0) {
        stats.activityTasks += activityTasks;
        madeProgress = true;
      }
    }

    if (options.signal?.aborted) {
      return madeProgress;
    }

    if (options.runTimerMaintenance) {
      const timers = await this.#backend.fireDueTimers({
        namespace: this.#namespace,
        now: Date.now(),
        limit: options.timerMaintenanceLimit
      });
      if (timers.fired > 0) {
        stats.timersFired += timers.fired;
        this.#metrics.timersFired += timers.fired;
        await this.#emit({ kind: "TimersFired", fired: timers.fired });
        madeProgress = true;
      }
      if (options.signal?.aborted) {
        return madeProgress;
      }
      const timeouts = await this.runActivityTimeoutMaintenanceOnce(
        options.activityTimeoutMaintenanceLimit
      );
      if (timeouts.timedOut > 0) {
        madeProgress = true;
      }
    }

    return madeProgress;
  }

  async #runActivityTasksForLoop(signal?: AbortSignal): Promise<number> {
    if (this.#activityCompletionBatchSize <= 1) {
      const activity = await this.runActivityTaskOnce();
      return activity.kind === "NoTask" ? 0 : 1;
    }
    return await this.#runActivityTaskBatch(
      this.#activityCompletionBatchSize,
      signal
    );
  }

  async #runActivityTaskBatch(limit: number, signal?: AbortSignal): Promise<number> {
    if (this.#activityTaskQueue === null) {
      return 0;
    }
    const completions: CompleteActivityRequest[] = [];
    let claimedTasks = 0;
    for (let index = 0; index < limit; index += 1) {
      const claimed = await this.#backend.claimActivityTask(this.#workerId, {
        namespace: this.#namespace,
        taskQueue: this.#activityTaskQueue,
        registeredActivityNames: this.#registeredActivityNames(),
        leaseDurationMs: this.#leaseDurationMs
      });
      if (!claimed) {
        if (index === 0) {
          this.#metrics.activityTaskNoTasks += 1;
        }
        break;
      }
      claimedTasks += 1;
      this.#metrics.activityTaskClaims += 1;
      await this.#emit({
        kind: "ActivityTaskClaimed",
        activityId: claimed.task.activityId,
        activityName: String(claimed.task.activityName),
        attempt: claimed.task.attempt,
        batched: true
      });

      const definition = this.#registry.activity(String(claimed.task.activityName));
      if (!definition) {
        throw new Error(`activity is not registered: ${claimed.task.activityName}`);
      }

      const input = decodePayload(
        claimed.task.input as PayloadRef<unknown>,
        definition.inputSchema
      );
      try {
        const output = await runWithActivityExecutionContext(
          {
            heartbeat: (request) => this.#backend.heartbeatActivity(request),
            heartbeatRequest: { claim: claimed.claim }
          },
          () => Promise.resolve(definition.handler(input))
        );
        completions.push({
          claim: claimed.claim,
          result: encodePayload(output, {
            codec: this.#payloadCodec,
            ...(definition.outputSchema === undefined ? {} : { schema: definition.outputSchema })
          })
        });
      } catch (error) {
        await this.#flushActivityCompletionBatch(completions);
        completions.length = 0;
        const outcome = await this.#backend.failActivity({
          claim: claimed.claim,
          failure: durableFailureFromUnknown(error)
        });
        this.#metrics.activityTaskFailures += 1;
        await this.#emit({
          kind: "ActivityTaskFailed",
          activityId: claimed.task.activityId,
          outcome,
          batched: true
        });
      }
      if (signal?.aborted) {
        break;
      }
    }

    await this.#flushActivityCompletionBatch(completions);
    return claimedTasks;
  }

  async #flushActivityCompletionBatch(completions: CompleteActivityRequest[]): Promise<void> {
    if (completions.length === 0) {
      return;
    }
    const outcome = await this.#backend.completeActivities({ completions });
    const accepted = outcome.results.filter(completedBatchItemAccepted).length;
    this.#metrics.activityCompletionBatches += 1;
    this.#metrics.activityCompletionBatchItems += completions.length;
    this.#metrics.activityTaskCompletions += accepted;
    await this.#emit({
      kind: "ActivityCompletionBatchFlushed",
      completions: completions.length,
      accepted,
      rejected: outcome.results.length - accepted,
      results: outcome.results
    });
    const bad = outcome.results.find((result) => !completedBatchItemAccepted(result));
    if (bad !== undefined) {
      throw new Error(`activity completion batch failed with ${bad.kind}`);
    }
  }

  async #runLocalActivitiesAfterWorkflowTask(signal?: AbortSignal): Promise<number> {
    if (this.#activityTaskQueue === null || this.#maxLocalActivitiesPerWorkflowTask === 0) {
      return 0;
    }

    let ran = 0;
    for (let index = 0; index < this.#maxLocalActivitiesPerWorkflowTask; index += 1) {
      const outcome = await this.runActivityTaskOnce();
      if (outcome.kind === "NoTask") {
        break;
      }
      ran += 1;
      if (signal?.aborted) {
        break;
      }
    }
    return ran;
  }

  async #emit(event: WorkerEvent): Promise<void> {
    if (this.#eventSink === undefined) {
      return;
    }
    try {
      await this.#eventSink(event);
    } catch {
      this.#metrics.eventSinkErrors += 1;
    }
  }
}

function emptyWorkerMetrics(): MutableWorkerMetrics {
  return {
    workflowTaskClaims: 0,
    workflowTaskNoTasks: 0,
    workflowTaskCommits: 0,
    workflowTaskConflicts: 0,
    activityTaskClaims: 0,
    activityTaskNoTasks: 0,
    activityTaskCompletions: 0,
    activityTaskFailures: 0,
    activityCompletionBatches: 0,
    activityCompletionBatchItems: 0,
    workflowHistoryCacheHits: 0,
    workflowHistoryCacheMisses: 0,
    workflowHistoryCacheEvictions: 0,
    workflowExecutionCacheHits: 0,
    workflowExecutionCacheMisses: 0,
    workflowExecutionCacheEvictions: 0,
    historyStreamChunks: 0,
    historyStreamEvents: 0,
    timersFired: 0,
    loopErrors: 0,
    idleSleeps: 0,
    eventSinkErrors: 0
  };
}

function nextBackoff(currentMs: number, maxMs: number): number {
  if (currentMs <= 0) {
    return 0;
  }
  return Math.min(maxMs, currentMs * 2);
}

async function sleepWithAbort(
  delayMs: number,
  signal: AbortSignal | undefined
): Promise<"elapsed" | "aborted"> {
  if (signal?.aborted) {
    return "aborted";
  }
  if (delayMs <= 0) {
    return signal?.aborted ? "aborted" : "elapsed";
  }
  return await new Promise((resolve) => {
    const timeout = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve("elapsed");
    }, delayMs);
    const onAbort = () => {
      clearTimeout(timeout);
      resolve("aborted");
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function durableFailureFromUnknown(error: unknown): DurableFailure {
  if (
    error &&
    typeof error === "object" &&
    "errorType" in error &&
    "message" in error &&
    typeof (error as { readonly errorType?: unknown }).errorType === "string" &&
    typeof (error as { readonly message?: unknown }).message === "string"
  ) {
    const failure = error as DurableFailure;
    return {
      errorType: failure.errorType,
      message: failure.message,
      nonRetryable: failure.nonRetryable,
      ...(failure.details === undefined ? {} : { details: failure.details })
    };
  }
  if (error instanceof Error) {
    return {
      errorType: error.name || "Error",
      message: error.message,
      nonRetryable: false
    };
  }
  return {
    errorType: "Error",
    message: String(error),
    nonRetryable: false
  };
}

function workerErrorInfo(error: unknown): WorkerErrorInfo {
  if (error instanceof Error) {
    return {
      name: error.name || "Error",
      message: error.message
    };
  }
  return {
    name: "Error",
    message: String(error)
  };
}

function completedBatchItemAccepted(result: CompleteActivityItemOutcome): boolean {
  return result.kind === "Completed" || result.kind === "AlreadyCompleted";
}

function contiguousHistoryPrefix(
  events: readonly HistoryEvent[],
  targetEventId: EventId
): HistoryEvent[] {
  const target = Number(targetEventId);
  const sorted = [...events]
    .filter((event) => Number(event.eventId) <= target)
    .sort((left, right) => Number(left.eventId) - Number(right.eventId));
  const prefix: HistoryEvent[] = [];
  let expected = 1;
  for (const event of sorted) {
    const actual = Number(event.eventId);
    if (actual < expected) {
      continue;
    }
    if (actual !== expected) {
      break;
    }
    prefix.push(event);
    expected += 1;
  }
  return prefix;
}

function contiguousHistoryRange(
  events: readonly HistoryEvent[],
  afterEventId: EventId,
  targetEventId: EventId
): HistoryEvent[] {
  const after = Number(afterEventId);
  const target = Number(targetEventId);
  const sorted = [...events]
    .filter((event) => Number(event.eventId) > after && Number(event.eventId) <= target)
    .sort((left, right) => Number(left.eventId) - Number(right.eventId));
  const range: HistoryEvent[] = [];
  let expected = after + 1;
  for (const event of sorted) {
    const actual = Number(event.eventId);
    if (actual < expected) {
      continue;
    }
    if (actual !== expected) {
      break;
    }
    range.push(event);
    expected += 1;
  }
  return range;
}

function assertContiguousHistoryThroughTarget(
  events: readonly HistoryEvent[],
  targetEventId: EventId
): void {
  const target = Number(targetEventId);
  if (target === 0) {
    return;
  }
  const prefix = contiguousHistoryPrefix(events, targetEventId);
  const tail = Number(prefix.at(-1)?.eventId ?? 0);
  if (tail !== target) {
    throw new Error(
      `workflow replay history is incomplete: expected contiguous events through ${target}, got through ${tail}`
    );
  }
}

function assertContiguousHistoryRange(
  events: readonly HistoryEvent[],
  afterEventId: EventId,
  targetEventId: EventId
): void {
  const after = Number(afterEventId);
  const target = Number(targetEventId);
  if (target === after) {
    return;
  }
  const range = contiguousHistoryRange(events, afterEventId, targetEventId);
  const tail = Number(range.at(-1)?.eventId ?? after);
  if (tail !== target) {
    throw new Error(
      `workflow hot wake history is incomplete: expected contiguous events from ${after + 1} through ${target}, got through ${tail}`
    );
  }
}

function isHotWorkflowWakeEvent(event: HistoryEvent): boolean {
  switch (event.data.kind) {
    case "ActivityCompleted":
    case "ActivityFailed":
    case "ActivityTimedOut":
    case "ActivityMapCompleted":
    case "ActivityMapFailed":
    case "ChildWorkflowStarted":
    case "ChildWorkflowCompleted":
    case "ChildWorkflowFailed":
    case "ChildWorkflowCancelled":
    case "ChildWorkflowMapCompleted":
    case "ChildWorkflowMapFailed":
    case "TimerFired":
      return true;
    default:
      return false;
  }
}

function workflowStartedInput(
  events: readonly { readonly data: { readonly kind: string; readonly input?: PayloadRef } }[]
): PayloadRef {
  const started = events.find((event) => event.data.kind === "WorkflowStarted");
  if (!started?.data.input) {
    throw new Error("claimed workflow task is missing WorkflowStarted input");
  }
  return started.data.input;
}

function sameWorkflowType(left: WorkflowType, right: WorkflowType): boolean {
  return left.name === right.name && left.version === right.version;
}
