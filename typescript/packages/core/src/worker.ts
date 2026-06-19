import type {
  CommitOutcome,
  CompleteActivityItemOutcome,
  CompleteActivityOutcome,
  CompleteActivityRequest,
  DurableBackend
} from "./backend.js";
import type { SignalInboxRecord } from "./backend.js";
import type { DurableFailure } from "./api.js";
import { decodePayload, encodePayload, type CodecId, type PayloadRef } from "./payload.js";
import type { Registry } from "./registry.js";
import { prepareWorkflowTaskCommit } from "./runtime.js";
import type { ActivityName, Namespace, RunId, SignalName, TaskQueue, WorkerId, WorkflowType } from "./types.js";

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
  timersFired: number;
  loopErrors: number;
  idleSleeps: number;
  eventSinkErrors: number;
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
  readonly #eventSink: WorkerEventSink | undefined;
  readonly #metrics: MutableWorkerMetrics = emptyWorkerMetrics();

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
    let idleBackoffMs = initialIdleBackoffMs;
    let errorBackoffMs = initialErrorBackoffMs;

    while (!options.signal?.aborted && (maxIterations === undefined || stats.iterations < maxIterations)) {
      stats.iterations += 1;
      try {
        const madeProgress = await this.#runLoopIteration(stats, {
          runTimerMaintenance: options.runTimerMaintenance ?? true,
          timerMaintenanceLimit
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

    const definition = this.#registry.workflow(claimed.workflowType.name, claimed.workflowType.version);
    if (!definition) {
      throw new Error(
        `workflow is not registered: ${claimed.workflowType.name}@${claimed.workflowType.version}`
      );
    }

    const input = decodePayload(
      workflowStartedInput(claimed.prefetchedHistory) as PayloadRef<unknown>,
      definition.inputSchema
    );
    const liveSignals = await this.#liveSignalsForClaim(claimed.runId);
    const commit = await prepareWorkflowTaskCommit(definition, input, claimed, {
      payloadCodec: this.#payloadCodec,
      defaultWorkflowTaskQueue: String(this.#workflowTaskQueue),
      liveSignals
    });
    const outcome = await this.#backend.commitWorkflowTask(claimed.claim, commit);
    const localActivityTasks =
      outcome.kind === "Committed" ? await this.#runLocalActivitiesAfterWorkflowTask() : 0;
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
      const output = await definition.handler(input);
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

  async #runLoopIteration(
    stats: {
      workflowTasks: number;
      activityTasks: number;
      timersFired: number;
    },
    options: {
      readonly runTimerMaintenance: boolean;
      readonly timerMaintenanceLimit: number;
    }
  ): Promise<boolean> {
    let madeProgress = false;

    const workflow = await this.runWorkflowTaskOnce();
    if (workflow.kind !== "NoTask") {
      stats.workflowTasks += 1;
      stats.activityTasks += workflow.localActivityTasks;
      madeProgress = true;
    }

    if (this.#activityTaskQueue !== null) {
      const activityTasks = await this.#runActivityTasksForLoop();
      if (activityTasks > 0) {
        stats.activityTasks += activityTasks;
        madeProgress = true;
      }
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
    }

    return madeProgress;
  }

  async #runActivityTasksForLoop(): Promise<number> {
    if (this.#activityCompletionBatchSize <= 1) {
      const activity = await this.runActivityTaskOnce();
      return activity.kind === "NoTask" ? 0 : 1;
    }
    const activity = await this.runActivityTaskBatchOnce(this.#activityCompletionBatchSize);
    return activity.kind === "NoTask" ? 0 : activity.tasks;
  }

  async #runActivityTaskBatch(limit: number): Promise<number> {
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
        const output = await definition.handler(input);
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

  async #runLocalActivitiesAfterWorkflowTask(): Promise<number> {
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

function workflowStartedInput(
  events: readonly { readonly data: { readonly kind: string; readonly input?: PayloadRef } }[]
): PayloadRef {
  const started = events.find((event) => event.data.kind === "WorkflowStarted");
  if (!started?.data.input) {
    throw new Error("claimed workflow task is missing WorkflowStarted input");
  }
  return started.data.input;
}
