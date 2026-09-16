import type {
  ClaimedActivityTask,
  ClaimedWorkflowTask,
  CompleteActivityItemOutcome,
  CompleteActivityOutcome,
  FailActivityOutcome,
  CompleteActivityRequest,
  DurableBackend,
  HistoryChunk,
  WorkflowTaskCommit
} from "./backend.js";
import type { SignalInboxRecord } from "./backend.js";
import type { WorkflowDefinition } from "./api.js";
import { runWithActivityExecutionContext } from "./activity-context.js";
import type { HistoryEvent } from "./history.js";
import { historyEventType } from "./history.js";
import { decodePayload, encodePayload, type CodecId, type PayloadRef } from "./payload.js";
import type { Registry } from "./registry.js";
import {
  HotWorkflowExecution,
  UnsupportedWorkflowVersionError,
  WorkflowCodeError,
  durableFailureFromUnknown
} from "./runtime.js";
import {
  eventId,
  type ActivityName,
  type EventId,
  type Namespace,
  type RunId,
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
  /**
   * The queue this worker claims activity tasks from, and the queue a
   * `callActivity()` that names none is scheduled onto.
   *
   * The second half is a **breaking change for existing histories** in one
   * deployment shape; see "Upgrading" in `typescript/README.md`. Before, the
   * runtime ignored this option and fell back to the literal `"default"`, so a
   * fleet with a second activity worker on `"default"` worked and now
   * re-fingerprints. Leaving this undefined keeps the `"default"` fallback,
   * which is also Rust's `TaskQueue::default()`.
   */
  readonly activityTaskQueue?: TaskQueue | string;
  readonly leaseDurationMs?: number;
  readonly payloadCodec?: CodecId;
  readonly maxLocalActivitiesPerWorkflowTask?: number;
  readonly activityCompletionBatchSize?: number;
  readonly historyFetchMaxEvents?: number;
  readonly historyFetchMaxBytes?: number;
  readonly workflowHistoryCacheSize?: number;
  /**
   * Total bytes of recorded history the workflow history cache may retain
   * across all runs.
   *
   * The binding limit. `workflowHistoryCacheSize` counts runs, which says
   * nothing about memory: a thousand cached runs are a few kilobytes or a few
   * gigabytes depending on how much payload their events carry inline. This
   * bounds the cache by what it actually holds, and a run whose history alone
   * exceeds the budget is simply not cached — it streams from the provider,
   * which is what the chunked replay path is for.
   *
   * Defaults to 32 MiB.
   */
  readonly workflowHistoryCacheBytes?: number;
  readonly workflowExecutionCacheSize?: number;
  readonly nondeterminismRetryBackoffMs?: number;
  /**
   * Installs the process-global determinism guards when this worker builds a
   * workflow execution. Leaving it undefined selects the default: on unless
   * `process.env.NODE_ENV === "production"`. See
   * {@link PrepareWorkflowTaskOptions.nondeterminismGuards}.
   */
  readonly nondeterminismGuards?: boolean;
  readonly onEvent?: WorkerEventSink;
}

export type RunWorkflowTaskOnceOutcome =
  | { readonly kind: "NoTask" }
  | {
      readonly kind: "Committed";
      readonly runId: RunId;
      readonly newTailEventId: EventId;
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
  /**
   * Bounds each task loop independently: the workflow loop and the activity
   * loop each run at most this many passes. `WorkerRunOutcome.iterations`
   * reports the workflow loop's count. The maintenance loop is interval-paced
   * rather than pass-paced, so it runs until both task loops have finished.
   */
  readonly maxIterations?: number;
  readonly idleBackoffMs?: number;
  readonly maxIdleBackoffMs?: number;
  readonly errorBackoffMs?: number;
  readonly maxErrorBackoffMs?: number;
  readonly runTimerMaintenance?: boolean;
  readonly timerMaintenanceLimit?: number;
  readonly activityTimeoutMaintenanceLimit?: number;
  /**
   * Delay before the first maintenance scan and after a scan that found nothing
   * due, in milliseconds. A scan that fired timers or timed out activities
   * re-runs immediately; consecutive empty scans double this up to
   * `maxMaintenanceIntervalMs`. Every delay is jittered by a factor in
   * `[0.5, 1.5)` drawn from a generator seeded with the worker id, so a fleet
   * started at once does not scan in lockstep and a given worker id always
   * reproduces the same schedule.
   *
   * `0` disables pacing: the loop then scans once per event-loop iteration,
   * independently of the task loops. It is not "a scan per task-loop pass" —
   * the loops no longer share a pass.
   */
  readonly maintenanceIntervalMs?: number;
  /** Ceiling for the maintenance backoff, in milliseconds. */
  readonly maxMaintenanceIntervalMs?: number;
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
      readonly newTailEventId: EventId;
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

/**
 * A run's contiguous history prefix starting at event 1, with the bytes it
 * retains so the cache can be bounded by memory rather than by entry count.
 */
interface WorkflowHistoryCacheEntry {
  readonly events: HistoryEvent[];
  bytes: number;
}

interface PreparedWorkflowExecution {
  readonly cacheKey: string | null;
  readonly execution: HotWorkflowExecution;
  // Carried without `prefetchedHistory`. Only `runId` and `workflowType` are
  // read after the commit, and the array a cold replay starts from now holds
  // the current replay chunk — keeping a reference to it here would pin that
  // for the length of the task, on top of the copy the runtime already owns.
  readonly claim: ClaimedWorkflowTask;
  readonly commit: WorkflowTaskCommit;
}

const NO_PREFETCHED_HISTORY: readonly HistoryEvent[] = [];

function claimWithoutHistory(claimed: ClaimedWorkflowTask): ClaimedWorkflowTask {
  return { ...claimed, prefetchedHistory: NO_PREFETCHED_HISTORY };
}

export class Worker {
  readonly #backend: DurableBackend;
  readonly #registry: Registry;
  readonly #namespace: Namespace | string;
  readonly #workerId: WorkerId | string;
  readonly #workflowTaskQueue: TaskQueue | string;
  readonly #activityTaskQueue: TaskQueue | string | null;
  readonly #leaseDurationMs: number;
  readonly #payloadCodec: CodecId;
  readonly #maxLocalActivitiesPerWorkflowTask: number;
  readonly #activityCompletionBatchSize: number;
  readonly #historyFetchMaxEvents: number;
  readonly #historyFetchMaxBytes: number;
  readonly #workflowHistoryCacheSize: number;
  readonly #workflowHistoryCacheBytes: number;
  readonly #workflowExecutionCacheSize: number;
  readonly #nondeterminismRetryBackoffMs: number;
  // Undefined means "let the runtime apply its NODE_ENV default".
  readonly #nondeterminismGuards: boolean | undefined;
  readonly #eventSink: WorkerEventSink | undefined;
  readonly #metrics: MutableWorkerMetrics = emptyWorkerMetrics();
  // Contiguous history prefixes, one per run, so a cold replay can be served
  // without re-streaming from the provider.
  //
  // Bounded by retained bytes, not by entry count, and appended to in place.
  // Both are consequences of chunked replay. It used to store a fresh copy of a
  // run's whole history on every commit — two O(N) copies per task, so O(N²/k)
  // allocation over a run — and to admit 1024 complete histories with all their
  // inline payloads, which is unbounded memory expressed as a bounded-looking
  // number.
  readonly #workflowHistoryCache = new Map<string, WorkflowHistoryCacheEntry>();
  #workflowHistoryCacheRetainedBytes = 0;
  readonly #workflowExecutionCache = new Map<string, WorkflowExecutionCacheEntry>();

  constructor(options: WorkerOptions) {
    this.#backend = options.backend;
    this.#registry = options.registry;
    this.#namespace = options.namespace ?? "default";
    this.#workerId = options.workerId;
    this.#workflowTaskQueue = options.workflowTaskQueue;
    this.#activityTaskQueue = options.activityTaskQueue ?? null;
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
    this.#workflowHistoryCacheBytes = Math.max(
      0,
      Math.trunc(options.workflowHistoryCacheBytes ?? 32 * 1024 * 1024)
    );
    this.#workflowExecutionCacheSize = Math.max(
      0,
      Math.trunc(options.workflowExecutionCacheSize ?? 1024)
    );
    this.#nondeterminismRetryBackoffMs = Math.max(
      0,
      Math.trunc(options.nondeterminismRetryBackoffMs ?? 60_000)
    );
    this.#nondeterminismGuards = options.nondeterminismGuards;
    this.#eventSink = options.onEvent;
  }

  metrics(): WorkerMetricsSnapshot {
    return { ...this.#metrics };
  }

  /**
   * Runs the worker until the caller's signal aborts or every task loop has
   * used its `maxIterations` budget.
   *
   * Workflow processing, activity processing, and due maintenance run as three
   * independent loops raced under one abort signal instead of as three
   * sequential phases of one pass. A slow activity therefore no longer holds up
   * workflow commits on the same worker, a failing stage no longer suppresses
   * the others — each loop owns its own idle and error backoff — and
   * maintenance load tracks elapsed time instead of the task rate.
   *
   * Connection-pool sizing: each loop keeps at most one backend call in flight,
   * so a running worker demands at most three pooled connections (two when it
   * has no activity queue, and a fourth is never needed because local
   * activities run inside the workflow loop). Size the pool at three or more
   * connections per worker; below that the loops queue against each other and
   * the activity loop can delay workflow commits.
   */
  async run(options: WorkerRunOptions = {}): Promise<WorkerRunOutcome> {
    const stats: MutableRunStats = {
      iterations: 0,
      workflowTasks: 0,
      activityTasks: 0,
      timersFired: 0,
      idleSleeps: 0,
      errors: 0
    };
    const config = resolveRunConfig(options);
    // The worker's own stop signal, separate from the caller's. Aborted when the
    // task loops have used their budgets — which is what unwinds the
    // interval-paced maintenance loop, since it has no budget of its own — and
    // when a loop fails outright.
    const stopLoops = new AbortController();
    const signal =
      options.signal === undefined
        ? stopLoops.signal
        : AbortSignal.any([options.signal, stopLoops.signal]);
    const stopPeersOnFailure = async (loop: Promise<void>): Promise<void> => {
      try {
        await loop;
      } catch (error) {
        // A loop only rejects when its error budget itself failed — an `onError`
        // callback that threw. `run()` still waits for the peer loops below, so
        // this abort is what keeps that wait bounded.
        stopLoops.abort();
        throw error;
      }
    };

    // Settled at creation, not at the `await` in `finally`. The task loops can
    // idle for a full backoff before anything looks at this promise, and an
    // unobserved rejection in that window reaches Node's default
    // `--unhandled-rejections=throw` and kills the worker process.
    const maintenance =
      (options.runTimerMaintenance ?? true)
        ? settleOutcome(
            stopPeersOnFailure(this.#runMaintenanceLoop(stats, signal, config, options))
          )
        : null;
    try {
      // `allSettled` rather than `all`: a loop that is still claiming tasks
      // after `run()` has already thrown is exactly the loop outliving its
      // worker that shutdown is supposed to prevent.
      const taskLoops = await Promise.allSettled([
        stopPeersOnFailure(this.#runWorkflowLoop(stats, signal, config, options)),
        this.#activityTaskQueue === null
          ? Promise.resolve()
          : stopPeersOnFailure(this.#runActivityLoop(stats, signal, config, options))
      ]);
      stopLoops.abort();
      // Maintenance is appended last so a task-loop failure stays the reported
      // cause. Raising it from `finally` instead would silently replace one.
      throwFirstRejection(
        maintenance === null ? taskLoops : [...taskLoops, await maintenance]
      );
    } finally {
      stopLoops.abort();
      // Awaited unconditionally, including on the throwing path: a maintenance
      // loop left running past `run()` would keep scanning the provider for a
      // worker its caller believes has stopped.
      await maintenance;
    }

    return {
      stopReason: options.signal?.aborted ? "abort" : "maxIterations",
      ...stats
    };
  }

  async #runWorkflowLoop(
    stats: MutableRunStats,
    signal: AbortSignal,
    config: ResolvedRunConfig,
    options: WorkerRunOptions
  ): Promise<void> {
    await this.#runTaskLoop(stats, signal, config, options, true, async () => {
      const workflow = await this.#runWorkflowTaskOnce(signal);
      if (workflow.kind === "NoTask") {
        return false;
      }
      stats.workflowTasks += 1;
      stats.activityTasks += workflow.localActivityTasks;
      return true;
    });
  }

  async #runActivityLoop(
    stats: MutableRunStats,
    signal: AbortSignal,
    config: ResolvedRunConfig,
    options: WorkerRunOptions
  ): Promise<void> {
    await this.#runTaskLoop(stats, signal, config, options, false, async () => {
      const activityTasks = await this.#runActivityTasksForLoop(signal);
      if (activityTasks === 0) {
        return false;
      }
      stats.activityTasks += activityTasks;
      return true;
    });
  }

  /**
   * Shared shape of the workflow and activity loops: poll, back off when idle,
   * back off separately when the step throws. Each loop instance owns its own
   * backoff state, so one stage's error budget cannot stop the other stage.
   *
   * A loop that keeps making progress never reaches its idle sleep, so it also
   * yields the event loop every `PROGRESS_YIELD_PASSES` productive passes.
   * Without that, a saturated loop starves its peers outright on any backend
   * whose calls settle on the microtask queue — see `yieldToEventLoop`.
   */
  async #runTaskLoop(
    stats: MutableRunStats,
    signal: AbortSignal,
    config: ResolvedRunConfig,
    options: WorkerRunOptions,
    countsIterations: boolean,
    step: () => Promise<boolean>
  ): Promise<void> {
    let idleBackoffMs = config.initialIdleBackoffMs;
    let errorBackoffMs = config.initialErrorBackoffMs;
    let iterations = 0;
    let passesSinceYield = 0;

    while (
      !signal.aborted &&
      (config.maxIterations === undefined || iterations < config.maxIterations)
    ) {
      iterations += 1;
      if (countsIterations) {
        stats.iterations = iterations;
      }
      try {
        const madeProgress = await step();
        errorBackoffMs = config.initialErrorBackoffMs;
        if (madeProgress) {
          idleBackoffMs = config.initialIdleBackoffMs;
          passesSinceYield += 1;
          if (passesSinceYield >= PROGRESS_YIELD_PASSES) {
            passesSinceYield = 0;
            await yieldToEventLoop();
          }
          continue;
        }

        // The idle and error sleeps below are themselves macrotask yields, so
        // reaching either one discharges the progress-yield debt.
        passesSinceYield = 0;
        stats.idleSleeps += 1;
        this.#metrics.idleSleeps += 1;
        if ((await sleepWithAbort(idleBackoffMs, signal)) === "aborted") {
          break;
        }
        idleBackoffMs = nextBackoff(idleBackoffMs, config.maxIdleBackoffMs);
      } catch (error) {
        passesSinceYield = 0;
        stats.errors += 1;
        this.#metrics.loopErrors += 1;
        await this.#emit({ kind: "WorkerLoopError", error: workerErrorInfo(error) });
        await options.onError?.(error);
        if ((await sleepWithAbort(errorBackoffMs, signal)) === "aborted") {
          break;
        }
        errorBackoffMs = nextBackoff(errorBackoffMs, config.maxErrorBackoffMs);
      }
    }
  }

  /**
   * Interval-paced maintenance. `SPEC.md` §11 gives due-timer delivery to an
   * independent timer service, so a worker scanning for due timers is a
   * convenience, not a durability obligation: pacing it by elapsed time keeps
   * provider load proportional to the interval and the fleet size instead of to
   * the task rate.
   */
  async #runMaintenanceLoop(
    stats: MutableRunStats,
    signal: AbortSignal,
    config: ResolvedRunConfig,
    options: WorkerRunOptions
  ): Promise<void> {
    const jitter = maintenanceJitterSource(String(this.#workerId));
    let errorBackoffMs = config.initialErrorBackoffMs;
    let intervalMs = config.maintenanceIntervalMs;
    // The first delay is a phase offset: without it every worker in a fleet
    // scans at startup, which is exactly the synchronized load the jitter exists
    // to break up.
    let delayMs = jitteredDelayMs(intervalMs, jitter);

    while (!signal.aborted) {
      if ((await sleepWithAbort(delayMs, signal)) === "aborted") {
        break;
      }
      try {
        const scanned = await this.#runMaintenanceScanOnce(stats, signal, config);
        errorBackoffMs = config.initialErrorBackoffMs;
        if (scanned) {
          intervalMs = config.maintenanceIntervalMs;
          delayMs = 0;
          continue;
        }
        intervalMs = nextBackoff(intervalMs, config.maxMaintenanceIntervalMs);
        delayMs = jitteredDelayMs(intervalMs, jitter);
      } catch (error) {
        stats.errors += 1;
        this.#metrics.loopErrors += 1;
        await this.#emit({ kind: "WorkerLoopError", error: workerErrorInfo(error) });
        await options.onError?.(error);
        if ((await sleepWithAbort(errorBackoffMs, signal)) === "aborted") {
          break;
        }
        errorBackoffMs = nextBackoff(errorBackoffMs, config.maxErrorBackoffMs);
        delayMs = 0;
      }
    }
  }

  async #runMaintenanceScanOnce(
    stats: MutableRunStats,
    signal: AbortSignal,
    config: ResolvedRunConfig
  ): Promise<boolean> {
    let scanned = false;
    // The provider's clock, not the process's. `Worker::run_timers_once` in
    // Rust reads `backend.current_time()` here for the same reason the
    // workflow-task path does: a worker that scanned against `Date.now()`
    // while the provider ran a virtual clock would fire timers the provider
    // does not consider due, or miss ones it does.
    const timers = await this.#backend.fireDueTimers({
      namespace: this.#namespace,
      now: await this.#backend.currentTime(),
      limit: config.timerMaintenanceLimit
    });
    if (timers.fired > 0) {
      stats.timersFired += timers.fired;
      this.#metrics.timersFired += timers.fired;
      await this.#emit({ kind: "TimersFired", fired: timers.fired });
      scanned = true;
    }
    if (signal.aborted) {
      return scanned;
    }
    const timeouts = await this.runActivityTimeoutMaintenanceOnce(
      config.activityTimeoutMaintenanceLimit
    );
    return scanned || timeouts.timedOut > 0;
  }

  async runWorkflowTaskOnce(): Promise<RunWorkflowTaskOnceOutcome> {
    return await this.#runWorkflowTaskOnce();
  }

  async runWorkflowTaskBatchOnce(limit: number): Promise<number> {
    const batchLimit = Math.max(1, Math.trunc(limit));
    if (this.#backend.claimWorkflowTasks !== undefined) {
      const claimed = await this.#claimWorkflowTaskBatch(batchLimit);
      // One task's failure must not abandon its batch neighbors' claims:
      // #runClaimedWorkflowTask releases the failing task's own claim (and
      // swallows release errors), so the drain keeps processing the remaining
      // independent tasks and propagates only the first error after every
      // claim in the batch has been committed, conflicted, or released.
      let firstError: unknown = null;
      for (const task of claimed) {
        try {
          await this.#runClaimedWorkflowTask(task);
        } catch (error) {
          firstError ??= error;
        }
      }
      if (firstError !== null) {
        throw firstError;
      }
      return claimed.length;
    }

    let processed = 0;
    for (let index = 0; index < batchLimit; index += 1) {
      const outcome = await this.#runWorkflowTaskOnce();
      if (outcome.kind === "NoTask") {
        break;
      }
      processed += 1;
    }
    return processed;
  }

  async #runWorkflowTaskOnce(signal?: AbortSignal): Promise<RunWorkflowTaskOnceOutcome> {
    const claimed = await this.#claimWorkflowTask();
    if (!claimed) {
      return { kind: "NoTask" };
    }
    return await this.#runClaimedWorkflowTask(claimed, signal);
  }

  async #claimWorkflowTask(): Promise<ClaimedWorkflowTask | null> {
    const claimed = await this.#backend.claimWorkflowTask(this.#workerId, {
      namespace: this.#namespace,
      taskQueue: this.#workflowTaskQueue,
      registeredWorkflowTypes: this.#registeredWorkflowTypes(),
      leaseDurationMs: this.#leaseDurationMs
    });
    if (!claimed) {
      this.#metrics.workflowTaskNoTasks += 1;
      return null;
    }
    this.#metrics.workflowTaskClaims += 1;
    return claimed;
  }

  async #claimWorkflowTaskBatch(limit: number): Promise<readonly ClaimedWorkflowTask[]> {
    const claimed = await this.#backend.claimWorkflowTasks?.(this.#workerId, {
      namespace: this.#namespace,
      taskQueue: this.#workflowTaskQueue,
      registeredWorkflowTypes: this.#registeredWorkflowTypes(),
      leaseDurationMs: this.#leaseDurationMs,
      limit: Math.max(1, Math.trunc(limit))
    });
    const tasks = claimed ?? [];
    if (tasks.length === 0) {
      this.#metrics.workflowTaskNoTasks += 1;
    } else {
      this.#metrics.workflowTaskClaims += tasks.length;
    }
    return tasks;
  }

  async #runClaimedWorkflowTask(
    claimed: ClaimedWorkflowTask,
    signal?: AbortSignal
  ): Promise<RunWorkflowTaskOnceOutcome> {
    let prepared: PreparedWorkflowExecution | null = null;
    let newTailEventId: EventId | null = null;
    try {
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

      const liveSignals = claimed.liveSignals;
      prepared = await this.#prepareWorkflowTaskFromCacheOrReplay(
        definition,
        claimed,
        liveSignals
      );
      newTailEventId = await this.#backend.commitWorkflowTask(claimed.claim, prepared.commit);
    } catch (error) {
      if (prepared !== null) {
        if (prepared.cacheKey !== null) {
          this.#workflowExecutionCache.delete(prepared.cacheKey);
        }
        // The task is released below and the next claim replays the run from
        // history, so this execution is abandoned. Dropping the reference is
        // not enough in JavaScript: without disposal its workflow frame stays
        // parked on an unsettled durable-API waiter forever.
        prepared.execution.dispose("workflow task failed before commit");
      }
      await this.#releaseFailedWorkflowTask(claimed.claim, error);
      throw error;
    }
    if (prepared === null || newTailEventId === null) {
      throw new Error("workflow task finished without a prepared commit");
    }
    {
      prepared.execution.markCommitted(newTailEventId);
      this.#updateWorkflowExecutionCacheAfterCommit(
        prepared,
        newTailEventId,
        claimed.replayTargetEventId
      );
      // One path for both cold and hot commits: the cached prefix is extended
      // by the events this task appended, or dropped if it no longer lines up.
      this.#appendCommittedEventsToHistoryCache(
        prepared.claim.runId,
        prepared.commit,
        newTailEventId
      );
    }
    const localActivityTasks = signal?.aborted
      ? 0
      : await this.#runLocalActivitiesAfterWorkflowTask(signal);
    this.#metrics.workflowTaskCommits += 1;
    await this.#emit({
      kind: "WorkflowTaskCommitted",
      runId: claimed.runId,
      newTailEventId,
      localActivityTasks
    });
    return { kind: "Committed", runId: claimed.runId, newTailEventId, localActivityTasks };
  }

  async #releaseFailedWorkflowTask(
    claim: ClaimedWorkflowTask["claim"],
    error: unknown
  ): Promise<void> {
    const visibilityDelayMs = isNondeterminismError(error)
      ? this.#nondeterminismRetryBackoffMs
      : 0;
    await this.#backend.releaseWorkflowTask(claim, { visibilityDelayMs }).catch(() => undefined);
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
    const outcome = await this.#runClaimedActivity(claimed, false, null);
    switch (outcome.kind) {
      case "Completed":
        return { kind: "Completed", activityId: claimed.task.activityId, outcome: outcome.outcome };
      case "Failed":
        return { kind: "Failed", activityId: claimed.task.activityId, outcome: outcome.outcome };
      case "Batched":
        throw new Error("a single activity task is never batched");
    }
  }

  /**
   * One execution path for a claimed activity: emit the claim, decode, run
   * the handler under its heartbeat context, then either complete it (through
   * the batch when one is being collected) or fail it. The three drivers,
   * single, batch, and sequential batch, differ only in how they claim.
   */
  async #runClaimedActivity(
    claimed: ClaimedActivityTask,
    batched: boolean,
    completions: CompleteActivityRequest[] | null
  ): Promise<
    | { readonly kind: "Completed"; readonly outcome: CompleteActivityOutcome }
    | { readonly kind: "Failed"; readonly outcome: FailActivityOutcome }
    | { readonly kind: "Batched" }
  > {
    await this.#emit({
      kind: "ActivityTaskClaimed",
      activityId: claimed.task.activityId,
      activityName: String(claimed.task.activityName),
      attempt: claimed.task.attempt,
      batched
    });
    const definition = this.#registry.activity(String(claimed.task.activityName));
    if (!definition) {
      if (completions !== null) {
        await this.#flushActivityCompletionBatch(completions);
        completions.length = 0;
      }
      const outcome = await this.#failClaimedActivity(
        claimed,
        new Error(`activity is not registered: ${claimed.task.activityName}`),
        batched
      );
      return { kind: "Failed", outcome };
    }
    const input = decodePayload(claimed.task.input as PayloadRef<unknown>, definition.inputSchema);
    let result: PayloadRef;
    try {
      const output = await runWithActivityExecutionContext(
        {
          heartbeat: (request) => this.#backend.heartbeatActivity(request),
          heartbeatRequest: { claim: claimed.claim }
        },
        () => Promise.resolve(definition.handler(input))
      );
      result = encodePayload(output, {
        codec: this.#payloadCodec,
        ...(definition.outputSchema === undefined ? {} : { schema: definition.outputSchema })
      });
    } catch (error) {
      if (completions !== null) {
        await this.#flushActivityCompletionBatch(completions);
        completions.length = 0;
      }
      const outcome = await this.#failClaimedActivity(claimed, error, batched);
      return { kind: "Failed", outcome };
    }
    if (completions !== null) {
      completions.push({ claim: claimed.claim, result });
      return { kind: "Batched" };
    }
    const outcome = await this.#backend.completeActivity({ claim: claimed.claim, result });
    this.#metrics.activityTaskCompletions += 1;
    await this.#emit({
      kind: "ActivityTaskCompleted",
      activityId: claimed.task.activityId,
      outcome,
      batched
    });
    return { kind: "Completed", outcome };
  }

  async #failClaimedActivity(
    claimed: ClaimedActivityTask,
    error: unknown,
    batched: boolean
  ): Promise<Awaited<ReturnType<DurableBackend["failActivity"]>>> {
    const outcome = await this.#backend.failActivity({
      claim: claimed.claim,
      failure: durableFailureFromUnknown(error)
    });
    this.#metrics.activityTaskFailures += 1;
    await this.#emit({
      kind: "ActivityTaskFailed",
      activityId: claimed.task.activityId,
      outcome,
      batched
    });
    return outcome;
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
      // As in `#runMaintenanceScanOnce`, and as in Rust's
      // `Worker::run_activity_timeouts_once`: one clock per deployment, and it
      // is the provider's.
      namespace: this.#namespace,
      now: await this.#backend.currentTime(),
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

  async #prepareWorkflowTaskFromCacheOrReplay(
    definition: WorkflowDefinition<any, any, any, string>,
    claimed: ClaimedWorkflowTask,
    liveSignals: readonly SignalInboxRecord[]
  ): Promise<PreparedWorkflowExecution> {
    const cacheKey = String(claimed.runId);
    // Read once per prepared task, before either path builds or advances an
    // execution, mirroring `Worker::prepare_claimed_workflow_task_inner` in
    // Rust. It has to come from the provider rather than from `Date.now()`:
    // the clock a workflow sees is part of the commit `SPEC.md` §1.2 makes
    // normative, and a provider driving a virtual clock is entitled to have
    // the runtime observe it.
    const nowMs = Number(await this.#backend.currentTime());
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
        let commit: WorkflowTaskCommit;
        try {
          commit = await cached.execution.advance(hotClaim, { liveSignals, nowMs });
        } catch (error: unknown) {
          // The execution's frame is past the point the claim will be retried
          // from, so the entry must not serve the next claim: evict and
          // dispose it here rather than leaving a poisoned entry for the
          // release path to trip over.
          this.#workflowExecutionCache.delete(cacheKey);
          cached.execution.dispose("task failed before commit");
          throw error;
        }
        return {
          cacheKey,
          execution: cached.execution,
          claim: claimWithoutHistory(hotClaim),
          commit
        };
      }
      this.#workflowExecutionCache.delete(cacheKey);
    }

    if (cached !== undefined) {
      // Reached from both supersession paths: the entry failed the hot-wake
      // criteria above, or it never met them. Either way the run is about to be
      // replayed cold into a fresh execution, so this frame's in-memory
      // position is dead and it is abandoned exactly like a conflicted one.
      // The delete above is redundant with this one and left as it was.
      this.#workflowExecutionCache.delete(cacheKey);
      // Same terminal skip as `#updateWorkflowExecutionCacheAfterCommit`: a
      // closed execution owns no waiter, so disposing it would only pay for the
      // error construction. A closed entry cannot reach the cache today; the
      // two sites state that assumption identically rather than each guessing.
      if (!cached.execution.closed) {
        cached.execution.dispose("superseded by cold replay");
      }
    }
    this.#metrics.workflowExecutionCacheMisses += 1;
    let replayClaim: ClaimedWorkflowTask | null = await this.#claimWithInitialReplayChunk(claimed);
    const input = decodePayload(
      workflowStartedInput(replayClaim.prefetchedHistory) as PayloadRef<unknown>,
      definition.inputSchema
    );
    const execution = this.#buildColdExecution(definition, input, replayClaim, liveSignals, nowMs);
    // Everything past this point needs the claim's identity, never its events.
    const preparedClaim = claimWithoutHistory(replayClaim);
    // Released as soon as the execution has ingested it. That array now carries
    // the current replay chunk, and the runtime keeps its own split of it —
    // window and indexes — so a second reference alive for the length of the
    // task would keep consumed events alive unnecessarily.
    replayClaim = null;
    let commit: WorkflowTaskCommit;
    try {
      commit = await execution.nextCommit();
    } catch (error: unknown) {
      execution.dispose("task failed before commit");
      throw error;
    }
    return {
      cacheKey,
      execution,
      claim: preparedClaim,
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

  #buildColdExecution(
    definition: WorkflowDefinition<any, any, any, string>,
    input: unknown,
    replayClaim: ClaimedWorkflowTask,
    liveSignals: readonly SignalInboxRecord[],
    nowMs: number
  ): HotWorkflowExecution {
    // The loader closes over ids, not over the claim: the claim holds the
    // provider's prefetched event array, and a loader that captured it would
    // pin the whole history for the length of the replay it exists to chunk.
    const replayRunId = replayClaim.runId;
    const replayTargetEventId = replayClaim.replayTargetEventId;
    return new HotWorkflowExecution(definition, input as object, replayClaim, {
      payloadCodec: this.#payloadCodec,
      defaultWorkflowTaskQueue: String(this.#workflowTaskQueue),
      // A `callActivity()` with no explicit `taskQueue` must land on the queue
      // this worker actually claims from. Rust's runtime applies the same
      // fallback (`RuntimeContext::effective_activity_options` ->
      // `ActivityOptions::with_task_queue_fallback`) from the worker's
      // configured activity task queue, and its unconfigured default is
      // `TaskQueue::default()` — the literal "default" the runtime falls back
      // to when this is left out, so an activity-less worker is unaffected.
      ...(this.#activityTaskQueue === null
        ? {}
        : { defaultActivityTaskQueue: String(this.#activityTaskQueue) }),
      // Rust's worker reads `backend.current_time()` once per prepared
      // workflow task and hands it to the runtime as `now`. Without it every
      // `sleep(d)` here recorded `fireAt = d`, an absolute deadline measured
      // from the epoch rather than from now.
      nowMs,
      liveSignals,
      // The pull side of chunked replay. The runtime calls this when its replay
      // window runs low, so the whole history is never in memory at once, and
      // the claim above carries only the head.
      loadReplayHistory: (afterEventId) =>
        this.#loadReplayHistoryChunk(replayRunId, replayTargetEventId, afterEventId),
      // Spread rather than assigned to satisfy `exactOptionalPropertyTypes`,
      // which rejects an explicit `undefined` for an optional property. Both
      // forms behave identically at runtime — the runtime tests
      // `requested !== undefined` and falls back to its NODE_ENV default either
      // way — so this is a type-level requirement, not a behavioural one.
      ...(this.#nondeterminismGuards === undefined
        ? {}
        : { nondeterminismGuards: this.#nondeterminismGuards })
    });
  }

  /**
   * Builds the claim a cold replay starts from: the head of history, not all of
   * it.
   *
   * The rest arrives through the ordinary replay gate, including for markers.
   */
  async #claimWithInitialReplayChunk(
    claimed: ClaimedWorkflowTask
  ): Promise<ClaimedWorkflowTask> {
    const prefix = contiguousHistoryPrefix(claimed.prefetchedHistory, claimed.replayTargetEventId);
    this.#recordHistoryCacheOutcome(claimed.runId, claimed.replayTargetEventId, prefix.length);
    if (prefix.length > 0) {
      // The claim brought these for free, so record them: the chunk loader and
      // any later cold replay of this run read them back instead of streaming
      // the head of history again.
      this.#appendHistoryCacheChunk(claimed.runId, eventId(0), prefix);
    }
    // Capped at one chunk even when the provider volunteered the whole history
    // in the claim. Handing the runtime everything a generous provider sends
    // would put replay memory back under the provider's control.
    const initial =
      prefix.length > this.#historyFetchMaxEvents
        ? prefix.slice(0, this.#historyFetchMaxEvents)
        : prefix;
    if (initial.length === 0 && Number(claimed.replayTargetEventId) > 0) {
      const chunk = await this.#loadReplayHistoryChunk(claimed.runId, claimed.replayTargetEventId, eventId(0));
      initial.push(...chunk.events);
    }
    return { ...claimed, prefetchedHistory: initial };
  }

  /**
   * Counts one history-cache outcome per cold replay: a hit when the cached
   * prefix reaches further than what the claim already carried and so can save
   * streaming, a miss otherwise. Counted here rather than per chunk so the
   * metric keeps meaning the same thing it did before replay was chunked —
   * "could this replay be seeded from the cache?".
   */
  #recordHistoryCacheOutcome(
    runIdValue: RunId,
    targetEventId: EventId,
    claimPrefixLength: number
  ): void {
    const key = String(runIdValue);
    const entry = this.#workflowHistoryCache.get(key);
    if (entry === undefined) {
      this.#metrics.workflowHistoryCacheMisses += 1;
      return;
    }
    const cachedThroughTarget = Math.min(entry.events.length, Number(targetEventId));
    if (cachedThroughTarget > claimPrefixLength) {
      this.#metrics.workflowHistoryCacheHits += 1;
    } else {
      this.#metrics.workflowHistoryCacheMisses += 1;
    }
    this.#touchWorkflowHistoryCacheEntry(key, entry);
  }

  /**
   * Streams the events after `afterEventId`, from the history cache when it has
   * them and from the provider otherwise.
   *
   * Every validation the old bulk loader ran once over the whole history runs
   * here per chunk: non-empty, watermark advanced, events contiguous from
   * `afterEventId`. Since each chunk is contiguous from where the last one
   * ended, and the runtime refuses to commit until its watermark reaches the
   * replay target, the concatenation is still exactly "contiguous events 1
   * through the target".
   */
  async #loadReplayHistoryChunk(
    runIdValue: RunId,
    replayTargetEventId: EventId,
    afterEventId: EventId
  ): Promise<{ readonly events: readonly HistoryEvent[]; readonly lastEventId: EventId }> {
    const cached = this.#cachedHistoryChunkAfter(runIdValue, afterEventId, replayTargetEventId);
    if (cached !== null) {
      return cached;
    }
    const chunk = await this.#backend.streamHistory({
      runId: runIdValue,
      afterEventId,
      upToEventId: replayTargetEventId,
      maxEvents: this.#historyFetchMaxEvents,
      maxBytes: this.#historyFetchMaxBytes
    });
    this.#recordHistoryChunk(chunk);
    if (chunk.events.length === 0) {
      throw new Error(
        `streamHistory returned no events for ${runIdValue} after ${afterEventId} before replay target ${replayTargetEventId}`
      );
    }
    if (Number(chunk.lastEventId) <= Number(afterEventId)) {
      throw new Error(
        `streamHistory did not advance for ${runIdValue}: still at ${chunk.lastEventId}`
      );
    }
    assertContiguousHistoryRange(chunk.events, afterEventId, chunk.lastEventId);
    this.#appendHistoryCacheChunk(runIdValue, afterEventId, chunk.events);
    return { events: chunk.events, lastEventId: chunk.lastEventId };
  }

  /**
   * Records the events a committed task appended.
   *
   * Appends in place. The two functions this replaced each rebuilt the run's
   * whole history array on every commit, so a run of N events across N/k tasks
   * paid O(N²/k) copying; this pays O(appended).
   */
  #appendCommittedEventsToHistoryCache(
    runIdValue: RunId,
    commit: WorkflowTaskCommit,
    newTailEventId: EventId
  ): void {
    const appended = commit.appendEvents ?? [];
    if (appended.length === 0) {
      return;
    }
    const key = String(runIdValue);
    const entry = this.#workflowHistoryCache.get(key);
    if (entry === undefined) {
      return;
    }
    // A commit's appends are contiguous and land at the end of history, so the
    // block starts here. Derived from the real tail rather than from what the
    // task expected: facts appended by activity workers, timer sweeps, and
    // child dispatch no longer void a commit, so they can sit between what this
    // task replayed and what it appended.
    const firstAppendedEventId = Number(newTailEventId) - appended.length;
    if (Number(entry.events.at(-1)?.eventId ?? 0) !== firstAppendedEventId) {
      // The cached prefix does not end where this commit's appends begin, so
      // appending would fabricate a history that never existed. Drop the entry
      // instead; the next cold replay rebuilds it from the provider.
      this.#deleteWorkflowHistoryCacheEntry(key);
      return;
    }
    const events: HistoryEvent[] = [];
    for (const [index, event] of appended.entries()) {
      events.push({
        eventId: eventId(firstAppendedEventId + index + 1),
        eventType: historyEventType(event.data),
        data: event.data
      });
    }
    this.#pushHistoryCacheEvents(key, entry, events);
  }

  #updateWorkflowExecutionCacheAfterCommit(
    prepared: PreparedWorkflowExecution,
    newTailEventId: EventId,
    observedTailEventId: EventId
  ): void {
    if (prepared.cacheKey === null) {
      return;
    }
    // Facts can land under a claim now that the claim token is the whole
    // commit fence. When they do, this execution's state is behind the run's
    // history, so the entry is dropped and the next task cold-replays.
    const runtimeAppendedTail =
      Number(observedTailEventId) + (prepared.commit.appendEvents?.length ?? 0);
    if (Number(newTailEventId) > runtimeAppendedTail) {
      this.#workflowExecutionCache.delete(prepared.cacheKey);
      prepared.execution.dispose("facts were appended while the workflow task was claimed");
      return;
    }
    // Split from the cache-disabled arm below on measured cost, not style. A
    // terminal execution owns no parked waiter, so disposing it settles
    // nothing — but `dispose()` constructs a `HotWorkflowExecutionDisposedError`
    // eagerly, measured at ~1122 ns, dominated by stack capture. Merging the
    // two arms would pay that on every committed terminal task whenever the
    // execution cache is disabled, the highest-volume path through this
    // function, against a memory-backend append-commit in the ~700 ns range.
    if (prepared.execution.closed) {
      this.#workflowExecutionCache.delete(prepared.cacheKey);
      return;
    }
    if (this.#workflowExecutionCacheSize === 0) {
      this.#workflowExecutionCache.delete(prepared.cacheKey);
      // The highest-volume disposal site: with the execution cache disabled,
      // every committed non-terminal task abandons a still-parked frame.
      prepared.execution.dispose("workflow execution cache disabled");
      return;
    }
    this.#storeWorkflowExecution(prepared.cacheKey, {
      execution: prepared.execution,
      tailEventId: newTailEventId,
      ingestedEventId: eventId(runtimeAppendedTail),
      workflowType: prepared.claim.workflowType
    });
  }

  /**
   * Serves the next chunk from the cached prefix, or `null` when the cache
   * cannot cover it.
   *
   * Slices at the same `historyFetchMaxEvents` the provider would, so a cache
   * hit hands the runtime the same shape a stream would and the replay window
   * stays the same size either way.
   */
  #cachedHistoryChunkAfter(
    runIdValue: RunId,
    afterEventId: EventId,
    targetEventId: EventId
  ): { readonly events: readonly HistoryEvent[]; readonly lastEventId: EventId } | null {
    const key = String(runIdValue);
    const entry = this.#workflowHistoryCache.get(key);
    if (entry === undefined) {
      return null;
    }
    const first = Number(entry.events[0]?.eventId ?? 0);
    const last = Number(entry.events.at(-1)?.eventId ?? 0);
    const from = Number(afterEventId) + 1;
    if (first !== 1 || last < from) {
      return null;
    }
    const upTo = Math.min(last, Number(targetEventId), from + this.#historyFetchMaxEvents - 1);
    if (upTo < from) {
      return null;
    }
    const events = entry.events.slice(from - 1, upTo);
    if (events.length === 0) {
      return null;
    }
    this.#touchWorkflowHistoryCacheEntry(key, entry);
    return { events, lastEventId: eventId(upTo) };
  }

  /**
   * Extends a run's cached prefix with a freshly streamed chunk, in place.
   */
  #appendHistoryCacheChunk(
    runIdValue: RunId,
    afterEventId: EventId,
    events: readonly HistoryEvent[]
  ): void {
    if (this.#workflowHistoryCacheSize === 0 || this.#workflowHistoryCacheBytes === 0) {
      return;
    }
    if (events.length === 0) {
      return;
    }
    const key = String(runIdValue);
    const entry = this.#workflowHistoryCache.get(key);
    if (entry === undefined) {
      if (Number(afterEventId) !== 0) {
        // Only a prefix starting at event 1 is usable, because that is the only
        // shape `#cachedHistoryChunkAfter` can answer from.
        return;
      }
      const created: WorkflowHistoryCacheEntry = { events: [], bytes: 0 };
      this.#workflowHistoryCache.set(key, created);
      this.#pushHistoryCacheEvents(key, created, events);
      return;
    }
    if (Number(entry.events.at(-1)?.eventId ?? 0) !== Number(afterEventId)) {
      return;
    }
    this.#pushHistoryCacheEvents(key, entry, events);
  }

  #pushHistoryCacheEvents(
    key: string,
    entry: WorkflowHistoryCacheEntry,
    events: readonly HistoryEvent[]
  ): void {
    let added = 0;
    for (const event of events) {
      entry.events.push(event);
      added += historyEventRetainedBytes(event);
    }
    entry.bytes += added;
    this.#workflowHistoryCacheRetainedBytes += added;
    // Most-recently-used goes last, so the eviction loop below can take the
    // oldest from the front.
    this.#workflowHistoryCache.delete(key);
    this.#workflowHistoryCache.set(key, entry);
    this.#enforceWorkflowHistoryCacheBounds(key);
  }

  /**
   * Evicts until the cache is inside both bounds.
   *
   * Bytes first, because that is the limit that means anything: an entry-count
   * limit lets a thousand runs with megabyte payloads sit in memory while
   * reporting a healthy-looking size. A single run bigger than the whole budget
   * is dropped outright rather than evicting everything else to hold it.
   */
  #enforceWorkflowHistoryCacheBounds(protectedKey: string): void {
    const protectedEntry = this.#workflowHistoryCache.get(protectedKey);
    if (protectedEntry !== undefined && protectedEntry.bytes > this.#workflowHistoryCacheBytes) {
      this.#deleteWorkflowHistoryCacheEntry(protectedKey);
      this.#metrics.workflowHistoryCacheEvictions += 1;
      return;
    }
    while (
      this.#workflowHistoryCache.size > this.#workflowHistoryCacheSize ||
      this.#workflowHistoryCacheRetainedBytes > this.#workflowHistoryCacheBytes
    ) {
      const oldest = this.#workflowHistoryCache.keys().next().value as string | undefined;
      if (oldest === undefined || oldest === protectedKey) {
        return;
      }
      this.#deleteWorkflowHistoryCacheEntry(oldest);
      this.#metrics.workflowHistoryCacheEvictions += 1;
    }
  }

  #deleteWorkflowHistoryCacheEntry(key: string): void {
    const entry = this.#workflowHistoryCache.get(key);
    if (entry === undefined) {
      return;
    }
    this.#workflowHistoryCacheRetainedBytes -= entry.bytes;
    this.#workflowHistoryCache.delete(key);
  }

  #storeWorkflowExecution(key: string, entry: WorkflowExecutionCacheEntry): void {
    this.#workflowExecutionCache.delete(key);
    this.#workflowExecutionCache.set(key, entry);
    while (this.#workflowExecutionCache.size > this.#workflowExecutionCacheSize) {
      const oldest = this.#workflowExecutionCache.keys().next().value as string | undefined;
      if (oldest === undefined) {
        return;
      }
      const evicted = this.#workflowExecutionCache.get(oldest);
      this.#workflowExecutionCache.delete(oldest);
      // An evicted run replays from history on its next claim, so its cached
      // frame is dead. Disposal settles it; otherwise the eviction bounds the
      // cache but not the parked promise chains it used to own.
      evicted?.execution.dispose("workflow execution cache eviction");
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

  #touchWorkflowHistoryCacheEntry(key: string, entry: WorkflowHistoryCacheEntry): void {
    this.#workflowHistoryCache.delete(key);
    this.#workflowHistoryCache.set(key, entry);
  }

  #recordHistoryChunk(chunk: HistoryChunk): void {
    this.#metrics.historyStreamChunks += 1;
    this.#metrics.historyStreamEvents += chunk.events.length;
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
    if (this.#backend.claimActivityTasks === undefined) {
      return await this.#runActivityTaskBatchSequential(limit, signal);
    }
    const claimedBatch = await this.#claimActivityTaskBatch(limit);
    if (claimedBatch.length === 0) {
      return 0;
    }
    // Every task in the batch runs, abort or not: the claims are held under
    // this worker's lease and nothing releases an activity claim, so a task
    // left unrun here would sit until its lease lapsed. The abort stops the
    // next claim, in the loop above.
    const completions: CompleteActivityRequest[] = [];
    let processedTasks = 0;
    for (const claimed of claimedBatch) {
      await this.#runClaimedActivity(claimed, true, completions);
      processedTasks += 1;
    }

    await this.#flushActivityCompletionBatch(completions);
    return processedTasks;
  }

  async #runActivityTaskBatchSequential(limit: number, signal?: AbortSignal): Promise<number> {
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
      await this.#runClaimedActivity(claimed, true, completions);
      if (signal?.aborted) {
        break;
      }
    }

    await this.#flushActivityCompletionBatch(completions);
    return claimedTasks;
  }

  async #claimActivityTaskBatch(limit: number): Promise<readonly ClaimedActivityTask[]> {
    if (this.#activityTaskQueue === null) {
      return [];
    }
    const batchLimit = Math.max(1, Math.trunc(limit));
    if (this.#backend.claimActivityTasks !== undefined) {
      const claimed = await this.#backend.claimActivityTasks(this.#workerId, {
        namespace: this.#namespace,
        taskQueue: this.#activityTaskQueue,
        registeredActivityNames: this.#registeredActivityNames(),
        leaseDurationMs: this.#leaseDurationMs,
        limit: batchLimit
      });
      if (claimed.length === 0) {
        this.#metrics.activityTaskNoTasks += 1;
      } else {
        this.#metrics.activityTaskClaims += claimed.length;
      }
      return claimed;
    }

    const claimed: ClaimedActivityTask[] = [];
    for (let index = 0; index < batchLimit; index += 1) {
      const task = await this.#backend.claimActivityTask(this.#workerId, {
        namespace: this.#namespace,
        taskQueue: this.#activityTaskQueue,
        registeredActivityNames: this.#registeredActivityNames(),
        leaseDurationMs: this.#leaseDurationMs
      });
      if (!task) {
        if (index === 0) {
          this.#metrics.activityTaskNoTasks += 1;
        }
        break;
      }
      claimed.push(task);
      this.#metrics.activityTaskClaims += 1;
    }
    return claimed;
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

interface MutableRunStats {
  iterations: number;
  workflowTasks: number;
  activityTasks: number;
  timersFired: number;
  idleSleeps: number;
  errors: number;
}

interface ResolvedRunConfig {
  readonly maxIterations: number | undefined;
  readonly initialIdleBackoffMs: number;
  readonly maxIdleBackoffMs: number;
  readonly initialErrorBackoffMs: number;
  readonly maxErrorBackoffMs: number;
  readonly timerMaintenanceLimit: number;
  readonly activityTimeoutMaintenanceLimit: number;
  readonly maintenanceIntervalMs: number;
  readonly maxMaintenanceIntervalMs: number;
}

function resolveRunConfig(options: WorkerRunOptions): ResolvedRunConfig {
  const initialIdleBackoffMs = Math.max(0, options.idleBackoffMs ?? 50);
  const initialErrorBackoffMs = Math.max(0, options.errorBackoffMs ?? 250);
  const timerMaintenanceLimit = Math.max(1, options.timerMaintenanceLimit ?? 64);
  const maintenanceIntervalMs = Math.max(0, Math.trunc(options.maintenanceIntervalMs ?? 250));
  return {
    maxIterations: options.maxIterations,
    initialIdleBackoffMs,
    maxIdleBackoffMs: Math.max(initialIdleBackoffMs, options.maxIdleBackoffMs ?? 1_000),
    initialErrorBackoffMs,
    maxErrorBackoffMs: Math.max(initialErrorBackoffMs, options.maxErrorBackoffMs ?? 5_000),
    timerMaintenanceLimit,
    activityTimeoutMaintenanceLimit: Math.max(
      1,
      options.activityTimeoutMaintenanceLimit ?? timerMaintenanceLimit
    ),
    maintenanceIntervalMs,
    maxMaintenanceIntervalMs: Math.max(
      maintenanceIntervalMs,
      Math.trunc(options.maxMaintenanceIntervalMs ?? 1_000)
    )
  };
}

/**
 * Single-promise `Promise.allSettled`. Attaching both handlers here is what
 * keeps a loop's rejection observed from the moment it is created, rather than
 * from whenever the caller gets around to awaiting it.
 */
async function settleOutcome(loop: Promise<void>): Promise<PromiseSettledResult<void>> {
  try {
    return { status: "fulfilled", value: await loop };
  } catch (reason) {
    return { status: "rejected", reason };
  }
}

function throwFirstRejection(results: readonly PromiseSettledResult<unknown>[]): void {
  for (const result of results) {
    if (result.status === "rejected") {
      throw result.reason;
    }
  }
}

/**
 * Deterministic per-worker jitter for the maintenance cadence.
 *
 * Seeded from the worker id and advanced once per sleep, so a given worker id
 * always reproduces the same schedule while two ids diverge immediately. A
 * global RNG would break both properties, and `Math.random` in particular is one
 * of the globals the determinism guard replaces.
 */
/**
 * Exported for the shared behavioural corpus's `workerStartJitter` table
 * (`typescript/packages/core/test/behavioral-corpus.test.ts`), whose Rust half
 * lives in `src/worker.rs`'s unit tests. Not re-exported from the package
 * index: the stream is an implementation detail of the maintenance cadence,
 * but it is one the two runtimes have to agree on bit for bit, and nothing
 * pinned it before.
 */
export function maintenanceJitterSource(workerId: string): () => number {
  let state = fnv1a32(workerId);
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296;
  };
}

/** Seeds {@link maintenanceJitterSource}; exported for the same corpus table. */
export function fnv1a32(value: string): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash >>> 0;
}

/** Spreads `intervalMs` over `[0.5x, 1.5x)`. Zero stays zero, so a caller can
 * opt out of pacing entirely with `maintenanceIntervalMs: 0`. */
function jitteredDelayMs(intervalMs: number, jitter: () => number): number {
  if (intervalMs <= 0) {
    return 0;
  }
  return Math.max(1, Math.round(intervalMs * (0.5 + jitter())));
}

function nextBackoff(currentMs: number, maxMs: number): number {
  if (currentMs <= 0) {
    return 0;
  }
  return Math.min(maxMs, currentMs * 2);
}

/**
 * Productive passes a task loop may take before it must yield the event loop.
 *
 * Bounds how long a saturated loop can hold the thread against its peers, in
 * that loop's own tasks. Not a public knob: the cost on a synchronous backend is
 * one event-loop iteration per eight tasks, and on an I/O backend every task
 * already yields several times, so there is no configuration worth exposing.
 */
const PROGRESS_YIELD_PASSES = 8;

/**
 * Yields to the event loop's macrotask phases so a peer loop parked on a timer
 * can run.
 *
 * The worker's loops are cooperatively scheduled on one event loop, and Node
 * drains the entire microtask queue before any macrotask. On a backend whose
 * calls settle on the microtask queue — `MemoryBackend`, and `@durust/sqlite`
 * because `node:sqlite`'s `DatabaseSync` is synchronous work behind an `async`
 * wrapper — a loop that makes progress every pass therefore starves its peers
 * outright rather than merely delaying them. Only a socket-backed provider such
 * as Postgres interleaves on its own.
 *
 * `setImmediate` rather than `setTimeout(…, 0)`: Node clamps sub-millisecond
 * timeouts to 1 ms, which would cap a busy loop near one task per millisecond.
 */
async function yieldToEventLoop(): Promise<void> {
  await new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
}

/**
 * Sleeps for `delayMs`, resolving early when `signal` aborts.
 *
 * A zero delay yields the event loop rather than resolving on the microtask
 * queue. The worker's loops are peers now: one that only yielded microtasks
 * would drain the microtask queue forever between timer phases, starving the
 * interval-paced maintenance loop and every other timer in the process.
 *
 * It yields through `setImmediate`, not `setTimeout(…, 0)`. Node clamps a zero
 * timeout to 1 ms, and a peer parked on a clamped timer cannot wake inside a
 * backlog that a saturated loop drains in under a millisecond — the ordinary
 * case on a synchronous backend, where a whole workflow task costs microseconds.
 * `yieldToEventLoop` hands the peer a turn; this is what lets the peer be ready
 * to take it. With the clamp in place the two straddle the 1 ms boundary and the
 * handoff succeeds only about half the time.
 */
async function sleepWithAbort(
  delayMs: number,
  signal: AbortSignal | undefined
): Promise<"elapsed" | "aborted"> {
  if (signal?.aborted) {
    return "aborted";
  }
  if (delayMs <= 0) {
    return await new Promise((resolve) => {
      const immediate = setImmediate(() => {
        signal?.removeEventListener("abort", onAbort);
        resolve("elapsed");
      });
      const onAbort = () => {
        clearImmediate(immediate);
        resolve("aborted");
      };
      signal?.addEventListener("abort", onAbort, { once: true });
    });
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

// Task failures that leave the run intact and replay it later: the claim is
// released with the nondeterminism backoff rather than immediately.
function isNondeterminismError(error: unknown): boolean {
  return (
    error instanceof UnsupportedWorkflowVersionError ||
    error instanceof WorkflowCodeError ||
    (error instanceof Error && error.message.startsWith("nondeterminism:"))
  );
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

/**
 * Approximates what one recorded event costs to keep in the history cache.
 *
 * Inline payload bytes are the term that matters and the only one that varies
 * by orders of magnitude, so they are measured; everything else is charged a
 * flat per-event and per-payload overhead.
 *
 * A switch over the event kinds rather than a walk over the data. This runs for
 * every event a commit appends, on every commit, so it is on the warm hot path
 * — and a generic walk pays `Object.values` there, allocating an array per
 * object per event to rediscover a layout that is fixed at compile time. The
 * switch names the payload-bearing fields directly and allocates nothing.
 */
function historyEventRetainedBytes(event: HistoryEvent): number {
  const data = event.data;
  switch (data.kind) {
    case "WorkflowStarted":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.input);
    case "WorkflowCompleted":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.result);
    case "WorkflowContinuedAsNew":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.input);
    case "ActivityScheduled":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.scheduled.input);
    case "ActivityCompleted":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.completed.result);
    case "ActivityMapScheduled":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.scheduled.inputManifest);
    case "ActivityMapCompleted":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.completed.resultManifest);
    case "ChildWorkflowMapScheduled":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.scheduled.inputManifest);
    case "ChildWorkflowMapCompleted":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.completed.resultManifest);
    case "ChildWorkflowStartRequested":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.requested.input);
    case "ChildWorkflowCompleted":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.completed.result);
    case "SignalConsumed":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.consumed.payload);
    case "SideEffectMarker":
      return HISTORY_EVENT_BASE_BYTES + payloadRefBytes(data.marker.value);
    default:
      // Every remaining kind carries identifiers, fingerprints, and failure
      // messages only — bounded, small, and covered by the flat charge.
      return HISTORY_EVENT_BASE_BYTES;
  }
}

const HISTORY_EVENT_BASE_BYTES = 192;
const PAYLOAD_REF_BASE_BYTES = 128;

function payloadRefBytes(payload: PayloadRef | undefined): number {
  if (payload === undefined) {
    return 0;
  }
  // A blob ref keeps only its metadata in memory; the payload itself lives in
  // the blob store, so it is charged the flat overhead alone.
  return payload.kind === "Inline"
    ? PAYLOAD_REF_BASE_BYTES + payload.bytes.byteLength
    : PAYLOAD_REF_BASE_BYTES;
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
