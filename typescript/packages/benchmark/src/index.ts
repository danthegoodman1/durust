#!/usr/bin/env node

import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import {
  Client,
  MemoryBackend,
  Registry,
  Worker,
  activity,
  activityMap,
  activityMapManifest,
  callActivity,
  childWorkflow,
  childWorkflowMap,
  decodeActivityMapResults,
  decodeChildWorkflowMapSuccesses,
  decodePayload,
  eventId,
  heartbeat,
  signal,
  sleep,
  timestampMs,
  workflow,
  type ActivityHeartbeatOutcome,
  type ActivityHeartbeatRequest,
  type ClaimedActivityTask,
  type ClaimedWorkflowTask,
  type CommitOutcome,
  type CompleteActivitiesOutcome,
  type CompleteActivitiesRequest,
  type CompleteActivityOutcome,
  type CompleteActivityRequest,
  type DurableBackend,
  type FailActivityOutcome,
  type FailActivityRequest,
  type FireDueTimersRequest,
  type FireDueTimersOutcome,
  type HistoryChunk,
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
  type PayloadRef,
  type WorkflowHandle,
  type WorkflowTaskClaim,
  type WorkflowTaskCommit,
  type ClaimActivityOptions,
  type ClaimWorkflowTaskOptions
} from "@durust/core";
import { LocalDirectoryBlobStore, PayloadBackend } from "@durust/payload";
import {
  PostgresBackend,
  type PostgresBackendStatsSnapshot,
  type PostgresStatementStatsSnapshot
} from "@durust/postgres";
import { SqliteBackend } from "@durust/sqlite";

const WORKFLOW_QUEUE = "workflows";
const ACTIVITY_QUEUE = "activities";
const DEFAULT_WORKFLOWS = 1000;
const DEFAULT_WORKERS = 4;
const DEFAULT_BATCH = 32;
const DEFAULT_MAX_ROUNDS = 10_000;

export type BenchmarkBackendName = "memory" | "sqlite" | "postgres";
export type BenchmarkMode =
  | "mixed"
  | "activity"
  | "activity-heartbeat"
  | "signal"
  | "timer"
  | "child"
  | "activity-map"
  | "child-map"
  | "recovery"
  | "payload"
  | "write-ceiling";

export interface BenchmarkOptions {
  readonly backend: BenchmarkBackendName;
  readonly mode: BenchmarkMode;
  readonly workflows: number;
  readonly workers: number;
  readonly batch: number;
  readonly max_rounds: number;
  readonly json: boolean;
  readonly keep_db: boolean;
  readonly activity_delay_ms: number;
  readonly workflow_offset: number;
  readonly child_map_items: number;
  readonly child_map_max_in_flight: number;
  readonly shards: number;
  readonly physical_partitions: number;
  readonly activation_concurrency: number;
  readonly activation_prefetch_limit: number;
  readonly activity_completion_batch: number;
  readonly postgres_pool_size: number;
}

export interface BenchmarkCounters {
  readonly workflow_starts: number;
  readonly signals: number;
  readonly child_starts: number;
  readonly child_completions: number;
  readonly timer_handlers: number;
  readonly boot_activities: number;
  readonly activity_heartbeats: number;
  readonly child_activities: number;
  readonly finish_activities: number;
  readonly workflow_tasks: number;
  readonly activity_tasks: number;
  readonly timers_fired: number;
  readonly activities_timed_out: number;
  readonly child_workflow_starts_dispatched: number;
}

export interface WorkerStatsReport {
  readonly workflowTasks: number;
  readonly activityTasks: number;
  readonly workflowHistoryCacheHits: number;
  readonly workflowHistoryCacheMisses: number;
  readonly workflowHistoryCacheEvictions: number;
  readonly workflowExecutionCacheHits: number;
  readonly workflowExecutionCacheMisses: number;
  readonly workflowExecutionCacheEvictions: number;
  readonly historyStreamChunks: number;
  readonly historyStreamEvents: number;
  readonly timersFired: number;
  readonly activitiesTimedOut: number;
  readonly childWorkflowStartsDispatched: number;
}

export interface LatencyReport {
  readonly samples: number;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly p99Ms: number;
  readonly maxMs: number;
}

export interface BackendOperationReport {
  readonly calls: number;
  readonly errors: number;
  readonly items: number;
  readonly totalMs: number;
  readonly itemsPerCall: number;
  readonly itemsPerSecond: number;
  readonly latency: LatencyReport;
}

export interface BackendMetricsReport {
  readonly workflowTaskCommitLatency: LatencyReport;
  readonly operations: Record<string, BackendOperationReport>;
}

export interface PostgresStatsReport {
  readonly walBytes: number;
  readonly walBytesPerSecond: number;
  readonly walRecords: number;
  readonly walRecordsPerSecond: number;
  readonly walFpi: number;
  readonly walBuffersFull: number;
  readonly walWrite: number;
  readonly walSync: number;
  readonly walWriteTimeMs: number;
  readonly walSyncTimeMs: number;
  readonly xactCommit: number;
  readonly xactRollback: number;
  readonly transactionsPerSecond: number;
  readonly transactionsPerMixedAction: number;
  readonly transactionsPerWorkflow: number;
  readonly rowsReturned: number;
  readonly rowsFetched: number;
  readonly rowsInserted: number;
  readonly rowsUpdated: number;
  readonly rowsDeleted: number;
  readonly blocksRead: number;
  readonly blocksHit: number;
  readonly blockCacheHitRatio: number;
  readonly tempFiles: number;
  readonly tempBytes: number;
  readonly deadlocks: number;
  readonly blockReadTimeMs: number;
  readonly blockWriteTimeMs: number;
  readonly activeTimeMs: number;
  readonly sessionTimeMs: number;
  readonly activeConnectionsAfter: number;
  readonly statementStats: PostgresStatementStatsReport | null;
}

export interface PostgresStatementStatsReport {
  readonly calls: number;
  readonly callsPerMixedAction: number;
  readonly callsPerWorkflow: number;
  readonly totalExecTimeMs: number;
  readonly topStatements: readonly PostgresTopStatementReport[];
}

export interface PostgresTopStatementReport {
  readonly queryId: string;
  readonly calls: number;
  readonly totalExecTimeMs: number;
  readonly query: string;
}

export interface BenchmarkResult {
  readonly backend: BenchmarkBackendName;
  readonly mode: BenchmarkMode;
  readonly correct: boolean;
  readonly options: BenchmarkOptions;
  readonly elapsed_ms: number;
  readonly setup_ms: number;
  readonly processing_ms: number;
  readonly verify_ms: number;
  readonly rounds: number;
  readonly activations: number;
  readonly completed_workflows: number;
  readonly mixed_actions: number;
  readonly activations_per_second: number;
  readonly mixed_actions_per_second: number;
  readonly workflows_per_second: number;
  readonly processing_activations_per_second: number;
  readonly processing_mixed_actions_per_second: number;
  readonly processing_workflows_per_second: number;
  readonly counters: BenchmarkCounters;
  readonly worker_stats: WorkerStatsReport;
  readonly backend_metrics: BackendMetricsReport;
  readonly postgres_stats: PostgresStatsReport | null;
  readonly resource_samples: null;
  readonly postgres_schema: string | null;
  readonly db_path: string | null;
  readonly db_bytes: number | null;
}

export interface BenchmarkBaseline {
  readonly name: string;
  readonly kind: "smoke" | "accepted";
  readonly metadata?: BenchmarkBaselineMetadata;
  readonly profile: Partial<BenchmarkOptions>;
  readonly result: BenchmarkBaselineResult;
  readonly thresholds: BenchmarkThresholds;
}

export interface BenchmarkBaselineMetadata {
  readonly measured_at?: string;
  readonly commit?: string;
  readonly os?: string;
  readonly node?: string;
  readonly postgres?: string;
  readonly notes?: string;
}

export interface BenchmarkBaselineResult {
  readonly completed_workflows: number;
  readonly mixed_actions: number;
  readonly counters: Partial<BenchmarkCounters>;
  readonly processing_mixed_actions_per_second: number;
  readonly processing_workflows_per_second: number;
  readonly workflow_task_commit_p95_ms: number;
  readonly worker_stats?: Partial<WorkerStatsReport>;
  readonly operations: Record<string, BenchmarkBaselineOperation>;
  readonly postgres_stats?: BenchmarkBaselinePostgresStats;
}

export interface BenchmarkBaselineOperation {
  readonly calls: number;
  readonly errors: number;
}

export interface BenchmarkBaselinePostgresStats {
  readonly transactionsPerMixedAction?: number;
  readonly statementStats?: {
    readonly callsPerMixedAction?: number;
  };
}

export interface BenchmarkThresholds {
  readonly require_correct?: boolean;
  readonly require_profile_match?: boolean;
  readonly require_exact_completed_workflows?: boolean;
  readonly require_exact_mixed_actions?: boolean;
  readonly require_exact_counters?: readonly (keyof BenchmarkCounters)[];
  readonly require_exact_worker_stats?: readonly (keyof WorkerStatsReport)[];
  readonly required_operation_names?: readonly string[];
  readonly forbidden_operation_names?: readonly string[];
  readonly allow_operation_errors?: boolean;
  readonly min_processing_mixed_actions_per_second_ratio?: number;
  readonly min_processing_workflows_per_second_ratio?: number;
  readonly max_workflow_task_commit_p95_ratio?: number;
  readonly max_workflow_task_commit_p95_ms?: number;
  readonly require_postgres_schema?: string;
  readonly require_postgres_stats?: boolean;
  readonly require_postgres_statement_stats?: boolean;
  readonly max_postgres_transactions_per_mixed_action?: number;
  readonly max_postgres_transactions_per_mixed_action_ratio?: number;
  readonly max_postgres_statement_calls_per_mixed_action?: number;
  readonly max_postgres_statement_calls_per_mixed_action_ratio?: number;
}

export interface BenchmarkThresholdFailure {
  readonly path: string;
  readonly expected: unknown;
  readonly actual: unknown;
  readonly message: string;
}

export interface BenchmarkThresholdComparison {
  readonly passed: boolean;
  readonly baseline: string;
  readonly failures: readonly BenchmarkThresholdFailure[];
}

interface WorkflowInput {
  readonly index: number;
  readonly activityDelayMs: number;
}

interface ParentOutput {
  readonly index: number;
  readonly childValue: number;
  readonly signalValue: number;
  readonly finished: true;
}

interface ChildMapInput {
  readonly index: number;
  readonly items: number;
  readonly maxInFlight: number;
  readonly activityDelayMs: number;
}

interface ChildMapOutput {
  readonly index: number;
  readonly sum: number;
  readonly items: number;
}

interface ValueOutput {
  readonly index: number;
  readonly value: number;
}

interface SignalOutput {
  readonly index: number;
  readonly signalValue: number;
}

interface TimerOutput {
  readonly index: number;
  readonly fired: true;
}

interface RecoveryOutput {
  readonly index: number;
  readonly sum: number;
  readonly timers: number;
}

interface PayloadWorkflowInput extends WorkflowInput {
  readonly body: string;
}

interface PayloadActivityOutput {
  readonly index: number;
  readonly body: string;
  readonly length: number;
}

interface PayloadWorkflowOutput {
  readonly index: number;
  readonly inputBytes: number;
  readonly outputBytes: number;
}

interface StartedRun {
  readonly runId: RunId;
  readonly index: number;
}

interface StartOutcome {
  readonly runs: readonly StartedRun[];
  readonly workflowStarts: number;
  readonly signals: number;
}

interface WorkerRunStats {
  workflowTasks: number;
  activityTasks: number;
  workflowHistoryCacheHits: number;
  workflowHistoryCacheMisses: number;
  workflowHistoryCacheEvictions: number;
  workflowExecutionCacheHits: number;
  workflowExecutionCacheMisses: number;
  workflowExecutionCacheEvictions: number;
  historyStreamChunks: number;
  historyStreamEvents: number;
  timersFired: number;
  activitiesTimedOut: number;
  childWorkflowStartsDispatched: number;
}

interface BackendHandle {
  readonly backend: DurableBackend;
  readonly dbPath: string | null;
  readonly postgresSchema: string | null;
  readonly postgresStatsSnapshot: (() => Promise<PostgresBackendStatsSnapshot>) | null;
  readonly cleanup: () => Promise<void>;
}

class BackendMetrics {
  readonly #operations = new Map<string, OperationSamples>();
  readonly #commitLatencies: number[] = [];

  record(name: string, durationMs: number, items: number, success: boolean): void {
    const samples = this.#operations.get(name) ?? new OperationSamples();
    samples.record(durationMs, items, success);
    this.#operations.set(name, samples);
    if (name === "commitWorkflowTask") {
      this.#commitLatencies.push(durationMs);
    }
  }

  report(): BackendMetricsReport {
    return {
      workflowTaskCommitLatency: latencyReport(this.#commitLatencies),
      operations: Object.fromEntries(
        [...this.#operations.entries()]
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([name, samples]) => [name, samples.report()])
      )
    };
  }
}

class OperationSamples {
  readonly durations: number[] = [];
  totalMs = 0;
  items = 0;
  errors = 0;

  record(durationMs: number, items: number, success: boolean): void {
    this.durations.push(durationMs);
    this.totalMs += durationMs;
    this.items += items;
    if (!success) {
      this.errors += 1;
    }
  }

  report(): BackendOperationReport {
    const calls = this.durations.length;
    const seconds = Math.max(this.totalMs / 1000, Number.EPSILON);
    return {
      calls,
      errors: this.errors,
      items: this.items,
      totalMs: round(this.totalMs),
      itemsPerCall: calls === 0 ? 0 : round(this.items / calls),
      itemsPerSecond: round(this.items / seconds),
      latency: latencyReport(this.durations)
    };
  }
}

class MeasuredBackend implements DurableBackend {
  constructor(
    readonly inner: DurableBackend,
    readonly metrics: BackendMetrics
  ) {}

  async startWorkflow(req: StartWorkflowRequest): Promise<StartWorkflowOutcome> {
    return this.#measure("startWorkflow", 1, () => this.inner.startWorkflow(req));
  }

  async claimWorkflowTask(
    workerId: string,
    opts: ClaimWorkflowTaskOptions
  ): Promise<ClaimedWorkflowTask | null> {
    return this.#measure("claimWorkflowTask", 1, () =>
      this.inner.claimWorkflowTask(workerId, opts)
    );
  }

  async streamHistory(req: StreamHistoryRequest): Promise<HistoryChunk> {
    return this.#measure("streamHistory", 1, () => this.inner.streamHistory(req));
  }

  async commitWorkflowTask(
    claim: WorkflowTaskClaim,
    commit: WorkflowTaskCommit
  ): Promise<CommitOutcome> {
    return this.#measure("commitWorkflowTask", commit.appendEvents?.length ?? 0, () =>
      this.inner.commitWorkflowTask(claim, commit)
    );
  }

  async claimActivityTask(
    workerId: string,
    opts: ClaimActivityOptions
  ): Promise<ClaimedActivityTask | null> {
    return this.#measure("claimActivityTask", 1, () =>
      this.inner.claimActivityTask(workerId, opts)
    );
  }

  async completeActivity(req: CompleteActivityRequest): Promise<CompleteActivityOutcome> {
    return this.#measure("completeActivity", 1, () => this.inner.completeActivity(req));
  }

  async completeActivities(req: CompleteActivitiesRequest): Promise<CompleteActivitiesOutcome> {
    return this.#measure("completeActivities", req.completions.length, () =>
      this.inner.completeActivities(req)
    );
  }

  async failActivity(req: FailActivityRequest): Promise<FailActivityOutcome> {
    return this.#measure("failActivity", 1, () => this.inner.failActivity(req));
  }

  async heartbeatActivity(req: ActivityHeartbeatRequest): Promise<ActivityHeartbeatOutcome> {
    return this.#measure("heartbeatActivity", 1, () => this.inner.heartbeatActivity(req));
  }

  async fireDueTimers(req: FireDueTimersRequest): Promise<FireDueTimersOutcome> {
    return this.#measure("fireDueTimers", req.limit, () => this.inner.fireDueTimers(req));
  }

  async timeoutDueActivities(
    req: TimeoutDueActivitiesRequest
  ): Promise<TimeoutDueActivitiesOutcome> {
    return this.#measure("timeoutDueActivities", req.limit, () =>
      this.inner.timeoutDueActivities(req)
    );
  }

  async signalWorkflow(req: SignalWorkflowRequest): Promise<SignalWorkflowOutcome> {
    return this.#measure("signalWorkflow", 1, () => this.inner.signalWorkflow(req));
  }

  async readSignalInbox(req: ReadSignalInboxRequest): Promise<SignalInboxRecord | null> {
    return this.#measure("readSignalInbox", 1, () => this.inner.readSignalInbox(req));
  }

  async queryWorkflow(req: QueryWorkflowRequest): Promise<QueryWorkflowOutcome> {
    return this.#measure("queryWorkflow", 1, () => this.inner.queryWorkflow(req));
  }

  async payloadRoots(): Promise<readonly unknown[]> {
    return this.#measure("payloadRoots", 1, () => this.inner.payloadRoots());
  }

  async #measure<T>(name: string, items: number, fn: () => Promise<T>): Promise<T> {
    const started = performance.now();
    try {
      const result = await fn();
      this.metrics.record(name, performance.now() - started, items, true);
      return result;
    } catch (error) {
      this.metrics.record(name, performance.now() - started, items, false);
      throw error;
    }
  }
}

const benchmarkActivity = activity({
  name: "bench.workload.activity",
  handler: async (input: { readonly value: number; readonly delayMs: number }): Promise<number> => {
    if (input.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, input.delayMs));
    }
    return input.value;
  }
});

const benchmarkHeartbeatActivity = activity({
  name: "bench.workload.heartbeat-activity",
  handler: async (input: { readonly value: number; readonly delayMs: number }): Promise<number> => {
    await heartbeat();
    if (input.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, input.delayMs));
    }
    return input.value;
  }
});

const benchmarkPayloadActivity = activity({
  name: "bench.workload.payload-activity",
  handler: async (input: PayloadWorkflowInput): Promise<PayloadActivityOutput> => {
    if (input.activityDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, input.activityDelayMs));
    }
    return {
      index: input.index,
      body: `${input.body}:result`,
      length: input.body.length
    };
  }
});

const benchmarkChild = workflow({
  name: "bench.workload.child",
  version: 1,
  handler: async (input: WorkflowInput): Promise<number> => {
    return await callActivity(
      benchmarkActivity,
      { value: input.index * 10, delayMs: input.activityDelayMs },
      { taskQueue: ACTIVITY_QUEUE }
    );
  }
});

const finishSignal = signal<{ readonly value: number }>("finish");

const benchmarkActivityParent = workflow({
  name: "bench.workload.activity-parent",
  version: 1,
  handler: async (input: WorkflowInput): Promise<ValueOutput> => {
    const value = await callActivity(
      benchmarkActivity,
      { value: input.index, delayMs: input.activityDelayMs },
      { taskQueue: ACTIVITY_QUEUE }
    );
    return { index: input.index, value };
  }
});

const benchmarkActivityHeartbeatParent = workflow({
  name: "bench.workload.activity-heartbeat-parent",
  version: 1,
  handler: async (input: WorkflowInput): Promise<ValueOutput> => {
    const value = await callActivity(
      benchmarkHeartbeatActivity,
      { value: input.index, delayMs: input.activityDelayMs },
      {
        taskQueue: ACTIVITY_QUEUE,
        heartbeatTimeoutMs: 60_000
      }
    );
    return { index: input.index, value };
  }
});

const benchmarkWriteCeilingParent = workflow({
  name: "bench.workload.write-ceiling-parent",
  version: 1,
  handler: async (input: WorkflowInput): Promise<ValueOutput> => {
    return { index: input.index, value: input.index };
  }
});

const benchmarkSignalParent = workflow({
  name: "bench.workload.signal-parent",
  version: 1,
  handler: async (input: WorkflowInput): Promise<SignalOutput> => {
    const signalValue = await finishSignal;
    return { index: input.index, signalValue: signalValue.value };
  }
});

const benchmarkTimerParent = workflow({
  name: "bench.workload.timer-parent",
  version: 1,
  handler: async (input: WorkflowInput): Promise<TimerOutput> => {
    await sleep(0);
    return { index: input.index, fired: true };
  }
});

const benchmarkChildParent = workflow({
  name: "bench.workload.child-parent",
  version: 1,
  handler: async (input: WorkflowInput): Promise<ValueOutput> => {
    const child = await childWorkflow(
      benchmarkChild,
      { index: input.index, activityDelayMs: input.activityDelayMs },
      { workflowId: `child-${input.index}`, taskQueue: WORKFLOW_QUEUE }
    ).spawn();
    const value = await child.result();
    return { index: input.index, value };
  }
});

const benchmarkParent = workflow({
  name: "bench.workload.parent",
  version: 1,
  handler: async (input: WorkflowInput): Promise<ParentOutput> => {
    await callActivity(
      benchmarkActivity,
      { value: input.index, delayMs: input.activityDelayMs },
      { taskQueue: ACTIVITY_QUEUE }
    );

    const child = await childWorkflow(
      benchmarkChild,
      { index: input.index, activityDelayMs: input.activityDelayMs },
      { workflowId: `mixed-child-${input.index}`, taskQueue: WORKFLOW_QUEUE }
    ).spawn();
    const childValue = await child.result();

    const signalValue = await finishSignal;
    await sleep(0);

    await callActivity(
      benchmarkActivity,
      { value: input.index + 1, delayMs: input.activityDelayMs },
      { taskQueue: ACTIVITY_QUEUE }
    );

    return {
      index: input.index,
      childValue,
      signalValue: signalValue.value,
      finished: true
    };
  }
});

const benchmarkActivityMapParent = workflow({
  name: "bench.workload.activity-map-parent",
  version: 1,
  handler: async (input: ChildMapInput): Promise<ChildMapOutput> => {
    const mapped = activityMap(benchmarkActivity, {
      inputManifest: activityMapInputManifest(input),
      resultManifest: "activity-map-results",
      taskQueue: ACTIVITY_QUEUE,
      maxInFlight: input.maxInFlight
    });
    const resultManifest = await mapped.resultManifest();
    const sum = decodeActivityMapResults<number>(resultManifest).reduce(
      (acc, value) => acc + value,
      0
    );
    return {
      index: input.index,
      sum,
      items: input.items
    };
  }
});

const benchmarkChildMapParent = workflow({
  name: "bench.workload.child-map-parent",
  version: 1,
  handler: async (input: ChildMapInput): Promise<ChildMapOutput> => {
    const manifest = childMapManifest(input);
    const mapped = childWorkflowMap(benchmarkChild, {
      inputManifest: manifest,
      resultManifest: "child-map-results",
      workflowIdPrefix: `child-map-${input.index}`,
      taskQueue: WORKFLOW_QUEUE,
      maxInFlight: input.maxInFlight
    });
    const resultManifest = await mapped.resultManifest();
    const sum = decodeChildWorkflowMapSuccesses<number>(resultManifest).reduce(
      (acc, value) => acc + value,
      0
    );
    return {
      index: input.index,
      sum,
      items: input.items
    };
  }
});

const benchmarkRecoveryParent = workflow({
  name: "bench.workload.recovery-parent",
  version: 1,
  handler: async (input: WorkflowInput): Promise<RecoveryOutput> => {
    const first = await callActivity(
      benchmarkActivity,
      { value: input.index, delayMs: input.activityDelayMs },
      { taskQueue: ACTIVITY_QUEUE }
    );
    await sleep(0);
    const second = await callActivity(
      benchmarkActivity,
      { value: input.index + 1, delayMs: input.activityDelayMs },
      { taskQueue: ACTIVITY_QUEUE }
    );
    await sleep(0);
    const third = await callActivity(
      benchmarkActivity,
      { value: input.index + 2, delayMs: input.activityDelayMs },
      { taskQueue: ACTIVITY_QUEUE }
    );
    return {
      index: input.index,
      sum: first + second + third,
      timers: 2
    };
  }
});

const benchmarkPayloadParent = workflow({
  name: "bench.workload.payload-parent",
  version: 1,
  handler: async (input: PayloadWorkflowInput): Promise<PayloadWorkflowOutput> => {
    const result = await callActivity(benchmarkPayloadActivity, input, {
      taskQueue: ACTIVITY_QUEUE
    });
    return {
      index: input.index,
      inputBytes: input.body.length,
      outputBytes: result.body.length
    };
  }
});

export function defaultBenchmarkOptions(): BenchmarkOptions {
  return {
    backend: "sqlite",
    mode: "mixed",
    workflows: DEFAULT_WORKFLOWS,
    workers: DEFAULT_WORKERS,
    batch: DEFAULT_BATCH,
    max_rounds: DEFAULT_MAX_ROUNDS,
    json: false,
    keep_db: false,
    activity_delay_ms: 0,
    workflow_offset: 0,
    child_map_items: 32,
    child_map_max_in_flight: 8,
    shards: 1,
    physical_partitions: 1,
    activation_concurrency: 1,
    activation_prefetch_limit: 1,
    activity_completion_batch: 1,
    postgres_pool_size: 10
  };
}

export function parseBenchmarkOptions(argv: readonly string[]): BenchmarkOptions {
  const parsed: Mutable<BenchmarkOptions> = { ...defaultBenchmarkOptions() };
  for (let index = 0; index < argv.length; index += 1) {
    const raw = argv[index] as string;
    const [flag, inlineValue] = raw.split("=", 2);
    const value = (name: string): string => {
      if (inlineValue !== undefined) {
        return inlineValue;
      }
      const next = argv[++index];
      if (next === undefined) {
        throw new Error(`${name} requires a value`);
      }
      return next;
    };

    switch (flag) {
      case "--backend":
      case "--provider":
        parsed.backend = parseBackend(value(flag));
        break;
      case "--mode":
        parsed.mode = parseMode(value(flag));
        break;
      case "--workflows":
        parsed.workflows = parsePositiveInteger(value(flag), flag);
        break;
      case "--workers":
        parsed.workers = parsePositiveInteger(value(flag), flag);
        break;
      case "--batch":
        parsed.batch = parsePositiveInteger(value(flag), flag);
        break;
      case "--max-rounds":
        parsed.max_rounds = parsePositiveInteger(value(flag), flag);
        break;
      case "--activity-delay-ms":
        parsed.activity_delay_ms = parseNonNegativeInteger(value(flag), flag);
        break;
      case "--workflow-offset":
        parsed.workflow_offset = parseNonNegativeInteger(value(flag), flag);
        break;
      case "--child-map-items":
        parsed.child_map_items = parsePositiveInteger(value(flag), flag);
        break;
      case "--child-map-max-in-flight":
        parsed.child_map_max_in_flight = parsePositiveInteger(value(flag), flag);
        break;
      case "--shards":
        parsed.shards = parsePositiveInteger(value(flag), flag);
        break;
      case "--physical-partitions":
        parsed.physical_partitions = parsePositiveInteger(value(flag), flag);
        break;
      case "--activation-concurrency":
        parsed.activation_concurrency = parsePositiveInteger(value(flag), flag);
        break;
      case "--activation-prefetch-limit":
        parsed.activation_prefetch_limit = parsePositiveInteger(value(flag), flag);
        break;
      case "--activity-completion-batch":
        parsed.activity_completion_batch = parsePositiveInteger(value(flag), flag);
        break;
      case "--postgres-pool-size":
        parsed.postgres_pool_size = parsePositiveInteger(value(flag), flag);
        break;
      case "--json":
        parsed.json = true;
        break;
      case "--keep-db":
        parsed.keep_db = true;
        break;
      case "--help":
      case "-h":
        throw new UsageError(usage());
      default:
        throw new Error(`unknown option: ${raw}\n${usage()}`);
    }
  }
  return parsed;
}

export async function runBenchmark(options: BenchmarkOptions): Promise<BenchmarkResult> {
  const backendHandle = await openBackend(options);
  const metrics = new BackendMetrics();
  const backend = new MeasuredBackend(backendHandle.backend, metrics);
  try {
    const postgresStatsBefore = await backendHandle.postgresStatsSnapshot?.();
    const setupStarted = performance.now();
    const registry = benchmarkRegistry();
    const startOutcome = await startWorkflows(backend, options);
    const setupFinished = performance.now();

    const workers = Array.from({ length: options.workers }, (_, index) =>
      new Worker({
        backend,
        registry,
        namespace: "default",
        workerId: `durust-benchmark-worker-${index}`,
        workflowTaskQueue: WORKFLOW_QUEUE,
        activityTaskQueue: ACTIVITY_QUEUE,
        registeredSignalNames: options.mode === "write-ceiling" ? [] : ["finish"],
        activityCompletionBatchSize: options.activity_completion_batch,
        payloadCodec: "Json"
      })
    );

    const processingStarted = setupFinished;
    let stats = emptyWorkerStats();
    let rounds = 0;
    let completed = 0;
    while (rounds < options.max_rounds) {
      rounds += 1;
      const roundStats = await drainWorkerRound(backend, workers, options);
      stats = addWorkerStats(stats, roundStats);
      completed = await verifyCompletedWorkflows(backend, startOutcome.runs, options, false);
      if (completed === options.workflows) {
        break;
      }
      if (!madeProgress(roundStats)) {
        throw new Error(
          `benchmark stalled after ${rounds} rounds: ${completed}/${options.workflows} workflows completed`
        );
      }
    }
    if (completed !== options.workflows) {
      await verifyCompletedWorkflows(backend, startOutcome.runs, options, true);
      throw new Error(
        `benchmark did not complete after ${options.max_rounds} rounds: ${completed}/${options.workflows}`
      );
    }
    const processingFinished = performance.now();
    const verifyStarted = processingFinished;
    const completedWorkflows = await verifyCompletedWorkflows(
      backend,
      startOutcome.runs,
      options,
      true
    );
    const verifyFinished = performance.now();
    const counters = countersFromStats(startOutcome, stats, options);
    const elapsedMs = verifyFinished - setupStarted;
    const setupMs = setupFinished - setupStarted;
    const processingMs = processingFinished - processingStarted;
    const verifyMs = verifyFinished - verifyStarted;
    const elapsedSeconds = Math.max(elapsedMs / 1000, Number.EPSILON);
    const processingSeconds = Math.max(processingMs / 1000, Number.EPSILON);
    const activations = counters.workflow_tasks;
    const mixedActions = mixedActionCount(counters);
    const postgresStatsAfter = await backendHandle.postgresStatsSnapshot?.();
    return {
      backend: options.backend,
      mode: options.mode,
      correct: true,
      options,
      elapsed_ms: round(elapsedMs),
      setup_ms: round(setupMs),
      processing_ms: round(processingMs),
      verify_ms: round(verifyMs),
      rounds,
      activations,
      completed_workflows: completedWorkflows,
      mixed_actions: mixedActions,
      activations_per_second: round(activations / elapsedSeconds),
      mixed_actions_per_second: round(mixedActions / elapsedSeconds),
      workflows_per_second: round(completedWorkflows / elapsedSeconds),
      processing_activations_per_second: round(activations / processingSeconds),
      processing_mixed_actions_per_second: round(mixedActions / processingSeconds),
      processing_workflows_per_second: round(completedWorkflows / processingSeconds),
      counters,
      worker_stats: workerStatsReport(stats),
      backend_metrics: metrics.report(),
      postgres_stats:
        postgresStatsBefore !== undefined && postgresStatsAfter !== undefined
          ? postgresStatsReportFromSnapshots(postgresStatsBefore, postgresStatsAfter, {
              elapsedMs,
              mixedActions,
              workflows: completedWorkflows
            })
          : null,
      resource_samples: null,
      postgres_schema: backendHandle.postgresSchema,
      db_path: options.keep_db ? backendHandle.dbPath : null,
      db_bytes: options.keep_db && backendHandle.dbPath !== null ? dbBytes(backendHandle.dbPath) : null
    };
  } finally {
    await backendHandle.cleanup();
  }
}

export function compareBenchmarkToBaseline(
  result: BenchmarkResult,
  baseline: BenchmarkBaseline
): BenchmarkThresholdComparison {
  const failures: BenchmarkThresholdFailure[] = [];
  const thresholds = baseline.thresholds;

  if (thresholds.require_correct ?? true) {
    pushEqual(failures, "correct", true, result.correct);
  }
  if (thresholds.require_profile_match ?? true) {
    for (const [key, expected] of Object.entries(baseline.profile)) {
      const actual = result.options[key as keyof BenchmarkOptions];
      pushEqual(failures, `options.${key}`, expected, actual);
    }
  }
  if (thresholds.require_exact_completed_workflows ?? true) {
    pushEqual(
      failures,
      "completed_workflows",
      baseline.result.completed_workflows,
      result.completed_workflows
    );
  }
  if (thresholds.require_exact_mixed_actions ?? true) {
    pushEqual(failures, "mixed_actions", baseline.result.mixed_actions, result.mixed_actions);
  }
  for (const key of thresholds.require_exact_counters ?? []) {
    pushEqual(
      failures,
      `counters.${key}`,
      baseline.result.counters[key],
      result.counters[key]
    );
  }
  for (const key of thresholds.require_exact_worker_stats ?? []) {
    pushEqual(
      failures,
      `worker_stats.${key}`,
      baseline.result.worker_stats?.[key],
      result.worker_stats[key]
    );
  }
  for (const name of thresholds.required_operation_names ?? []) {
    const operation = result.backend_metrics.operations[name];
    if (operation === undefined) {
      failures.push({
        path: `backend_metrics.operations.${name}`,
        expected: "present",
        actual: "missing",
        message: `required backend operation ${name} was not recorded`
      });
      continue;
    }
    if (!(thresholds.allow_operation_errors ?? false) && operation.errors !== 0) {
      failures.push({
        path: `backend_metrics.operations.${name}.errors`,
        expected: 0,
        actual: operation.errors,
        message: `backend operation ${name} recorded errors`
      });
    }
  }
  for (const name of thresholds.forbidden_operation_names ?? []) {
    const operation = result.backend_metrics.operations[name];
    if (operation === undefined) {
      continue;
    }
    failures.push({
      path: `backend_metrics.operations.${name}`,
      expected: "absent",
      actual: operation.calls,
      message: `forbidden backend operation ${name} was recorded`
    });
  }

  const minMixedRatio = thresholds.min_processing_mixed_actions_per_second_ratio;
  if (minMixedRatio !== undefined) {
    pushAtLeast(
      failures,
      "processing_mixed_actions_per_second",
      baseline.result.processing_mixed_actions_per_second * minMixedRatio,
      result.processing_mixed_actions_per_second
    );
  }
  const minWorkflowRatio = thresholds.min_processing_workflows_per_second_ratio;
  if (minWorkflowRatio !== undefined) {
    pushAtLeast(
      failures,
      "processing_workflows_per_second",
      baseline.result.processing_workflows_per_second * minWorkflowRatio,
      result.processing_workflows_per_second
    );
  }
  const maxCommitRatio = thresholds.max_workflow_task_commit_p95_ratio;
  if (maxCommitRatio !== undefined) {
    pushAtMost(
      failures,
      "backend_metrics.workflowTaskCommitLatency.p95Ms",
      baseline.result.workflow_task_commit_p95_ms * maxCommitRatio,
      result.backend_metrics.workflowTaskCommitLatency.p95Ms
    );
  }
  const maxCommitMs = thresholds.max_workflow_task_commit_p95_ms;
  if (maxCommitMs !== undefined) {
    pushAtMost(
      failures,
      "backend_metrics.workflowTaskCommitLatency.p95Ms",
      maxCommitMs,
      result.backend_metrics.workflowTaskCommitLatency.p95Ms
    );
  }
  if (thresholds.require_postgres_schema !== undefined) {
    pushEqual(
      failures,
      "postgres_schema",
      thresholds.require_postgres_schema,
      result.postgres_schema
    );
  }
  if ((thresholds.require_postgres_stats ?? false) && result.postgres_stats === null) {
    failures.push({
      path: "postgres_stats",
      expected: "present",
      actual: null,
      message: "Postgres stats were required but not reported"
    });
  }
  if (
    (thresholds.require_postgres_statement_stats ?? false) &&
    result.postgres_stats?.statementStats == null
  ) {
    failures.push({
      path: "postgres_stats.statementStats",
      expected: "present",
      actual: result.postgres_stats?.statementStats ?? null,
      message: "Postgres statement stats were required but not reported"
    });
  }

  compareOptionalPostgresAtMost(
    failures,
    baseline,
    result,
    "postgres_stats.transactionsPerMixedAction",
    thresholds.max_postgres_transactions_per_mixed_action,
    thresholds.max_postgres_transactions_per_mixed_action_ratio,
    baseline.result.postgres_stats?.transactionsPerMixedAction,
    result.postgres_stats?.transactionsPerMixedAction
  );
  compareOptionalPostgresAtMost(
    failures,
    baseline,
    result,
    "postgres_stats.statementStats.callsPerMixedAction",
    thresholds.max_postgres_statement_calls_per_mixed_action,
    thresholds.max_postgres_statement_calls_per_mixed_action_ratio,
    baseline.result.postgres_stats?.statementStats?.callsPerMixedAction,
    result.postgres_stats?.statementStats?.callsPerMixedAction
  );

  return {
    passed: failures.length === 0,
    baseline: baseline.name,
    failures
  };
}

function compareOptionalPostgresAtMost(
  failures: BenchmarkThresholdFailure[],
  baseline: BenchmarkBaseline,
  result: BenchmarkResult,
  path: string,
  absoluteMax: number | undefined,
  baselineRatio: number | undefined,
  baselineValue: number | undefined,
  actual: number | undefined
): void {
  if (absoluteMax === undefined && baselineRatio === undefined) {
    return;
  }
  if (actual === undefined) {
    failures.push({
      path,
      expected: "present",
      actual: result.postgres_stats === null ? null : undefined,
      message: `${path} was required by ${baseline.name} but not reported`
    });
    return;
  }
  if (absoluteMax !== undefined) {
    pushAtMost(failures, path, absoluteMax, actual);
  }
  if (baselineRatio === undefined) {
    return;
  }
  if (baselineValue === undefined) {
    failures.push({
      path: `baseline.result.${path}`,
      expected: "present",
      actual: undefined,
      message: `${baseline.name} is missing ${path} for ratio comparison`
    });
    return;
  }
  pushAtMost(failures, path, baselineValue * baselineRatio, actual);
}

export function postgresStatsReportFromSnapshots(
  before: PostgresBackendStatsSnapshot,
  after: PostgresBackendStatsSnapshot,
  context: {
    readonly elapsedMs: number;
    readonly mixedActions: number;
    readonly workflows: number;
  }
): PostgresStatsReport {
  const elapsedSeconds = Math.max(context.elapsedMs / 1000, Number.EPSILON);
  const mixedActions = Math.max(context.mixedActions, 0);
  const workflows = Math.max(context.workflows, 0);
  const walBytes = postgresStatsDelta(before, after, "walBytes");
  const walRecords = postgresStatsDelta(before, after, "walRecords");
  const xactCommit = postgresStatsDelta(before, after, "xactCommit");
  const xactRollback = postgresStatsDelta(before, after, "xactRollback");
  const transactions = xactCommit + xactRollback;
  const blocksRead = postgresStatsDelta(before, after, "blocksRead");
  const blocksHit = postgresStatsDelta(before, after, "blocksHit");
  const blockAccesses = blocksRead + blocksHit;
  return {
    walBytes,
    walBytesPerSecond: round(walBytes / elapsedSeconds),
    walRecords,
    walRecordsPerSecond: round(walRecords / elapsedSeconds),
    walFpi: postgresStatsDelta(before, after, "walFpi"),
    walBuffersFull: postgresStatsDelta(before, after, "walBuffersFull"),
    walWrite: postgresStatsDelta(before, after, "walWrite"),
    walSync: postgresStatsDelta(before, after, "walSync"),
    walWriteTimeMs: round(postgresStatsDelta(before, after, "walWriteTimeMs")),
    walSyncTimeMs: round(postgresStatsDelta(before, after, "walSyncTimeMs")),
    xactCommit,
    xactRollback,
    transactionsPerSecond: round(transactions / elapsedSeconds),
    transactionsPerMixedAction: mixedActions === 0 ? 0 : round(transactions / mixedActions),
    transactionsPerWorkflow: workflows === 0 ? 0 : round(transactions / workflows),
    rowsReturned: postgresStatsDelta(before, after, "rowsReturned"),
    rowsFetched: postgresStatsDelta(before, after, "rowsFetched"),
    rowsInserted: postgresStatsDelta(before, after, "rowsInserted"),
    rowsUpdated: postgresStatsDelta(before, after, "rowsUpdated"),
    rowsDeleted: postgresStatsDelta(before, after, "rowsDeleted"),
    blocksRead,
    blocksHit,
    blockCacheHitRatio: blockAccesses === 0 ? 1 : round(blocksHit / blockAccesses),
    tempFiles: postgresStatsDelta(before, after, "tempFiles"),
    tempBytes: postgresStatsDelta(before, after, "tempBytes"),
    deadlocks: postgresStatsDelta(before, after, "deadlocks"),
    blockReadTimeMs: round(postgresStatsDelta(before, after, "blockReadTimeMs")),
    blockWriteTimeMs: round(postgresStatsDelta(before, after, "blockWriteTimeMs")),
    activeTimeMs: 0,
    sessionTimeMs: 0,
    activeConnectionsAfter: after.activeConnections,
    statementStats: postgresStatementStatsReportFromSnapshots(before, after, {
      mixedActions,
      workflows
    })
  };
}

function postgresStatementStatsReportFromSnapshots(
  before: PostgresBackendStatsSnapshot,
  after: PostgresBackendStatsSnapshot,
  context: {
    readonly mixedActions: number;
    readonly workflows: number;
  }
): PostgresStatementStatsReport | null {
  if (before.statements.length === 0 && after.statements.length === 0) {
    return null;
  }
  const beforeById = new Map(before.statements.map((statement) => [statement.queryId, statement]));
  const deltas = after.statements
    .map((statement) => postgresStatementDelta(beforeById.get(statement.queryId), statement))
    .filter((statement) => statement.calls > 0 || statement.totalExecTimeMs > 0)
    .sort((left, right) =>
      right.calls === left.calls
        ? right.totalExecTimeMs - left.totalExecTimeMs
        : right.calls - left.calls
    );
  const calls = deltas.reduce((sum, statement) => sum + statement.calls, 0);
  const totalExecTimeMs = deltas.reduce(
    (sum, statement) => sum + statement.totalExecTimeMs,
    0
  );
  return {
    calls,
    callsPerMixedAction:
      context.mixedActions === 0 ? 0 : round(calls / Math.max(1, context.mixedActions)),
    callsPerWorkflow:
      context.workflows === 0 ? 0 : round(calls / Math.max(1, context.workflows)),
    totalExecTimeMs: round(totalExecTimeMs),
    topStatements: deltas.slice(0, 10).map((statement) => ({
      queryId: statement.queryId,
      calls: statement.calls,
      totalExecTimeMs: round(statement.totalExecTimeMs),
      query: statement.query
    }))
  };
}

function postgresStatementDelta(
  before: PostgresStatementStatsSnapshot | undefined,
  after: PostgresStatementStatsSnapshot
): PostgresTopStatementReport {
  return {
    queryId: after.queryId,
    calls: Math.max(0, after.calls - (before?.calls ?? 0)),
    totalExecTimeMs: Math.max(0, after.totalExecTimeMs - (before?.totalExecTimeMs ?? 0)),
    query: after.query
  };
}

async function openBackend(options: BenchmarkOptions): Promise<BackendHandle> {
  const wrapPayloadBackend = (handle: BackendHandle): BackendHandle => {
    if (options.mode !== "payload") {
      return handle;
    }
    const blobRoot = mkdtempSync(join(tmpdir(), "durust-ts-benchmark-payload-"));
    const blobStore = new LocalDirectoryBlobStore({ root: blobRoot });
    return {
      backend: new PayloadBackend({
        backend: handle.backend,
        blobStore,
        inlineThresholdBytes: 64
      }),
      dbPath: handle.dbPath,
      postgresSchema: handle.postgresSchema,
      postgresStatsSnapshot: handle.postgresStatsSnapshot,
      cleanup: async () => {
        try {
          await handle.cleanup();
        } finally {
          rmSync(blobRoot, { recursive: true, force: true });
        }
      }
    };
  };

  if (options.backend === "memory") {
    return wrapPayloadBackend({
      backend: new MemoryBackend(),
      dbPath: null,
      postgresSchema: null,
      postgresStatsSnapshot: null,
      cleanup: async () => undefined
    });
  }
  if (options.backend === "sqlite") {
    const root = mkdtempSync(join(tmpdir(), "durust-ts-benchmark-sqlite-"));
    const dbPath = join(root, "durust.db");
    const backend = new SqliteBackend({ path: dbPath });
    return wrapPayloadBackend({
      backend,
      dbPath,
      postgresSchema: null,
      postgresStatsSnapshot: null,
      cleanup: async () => {
        backend.close();
        if (!options.keep_db) {
          rmSync(root, { recursive: true, force: true });
        }
      }
    });
  }
  const url = process.env.DURUST_POSTGRES_URL;
  if (!url) {
    throw new Error("set DURUST_POSTGRES_URL to run the Postgres benchmark workload");
  }
  const tableName = `durust_ts_benchmark_${process.pid}_${Date.now()}`;
  const backend = new PostgresBackend({
    url,
    tableName,
    poolSize: options.postgres_pool_size
  });
  return wrapPayloadBackend({
    backend,
    dbPath: null,
    postgresSchema: "normalized",
    postgresStatsSnapshot: () => backend.statsSnapshot(),
    cleanup: async () => {
      if (options.keep_db) {
        await backend.close();
      } else {
        await backend.destroy();
      }
    }
  });
}

function benchmarkRegistry(): Registry {
  return new Registry()
    .registerWorkflow(benchmarkActivityParent)
    .registerWorkflow(benchmarkActivityHeartbeatParent)
    .registerWorkflow(benchmarkSignalParent)
    .registerWorkflow(benchmarkTimerParent)
    .registerWorkflow(benchmarkChildParent)
    .registerWorkflow(benchmarkParent)
    .registerWorkflow(benchmarkChild)
    .registerWorkflow(benchmarkActivityMapParent)
    .registerWorkflow(benchmarkChildMapParent)
    .registerWorkflow(benchmarkRecoveryParent)
    .registerWorkflow(benchmarkPayloadParent)
    .registerWorkflow(benchmarkWriteCeilingParent)
    .registerActivity(benchmarkPayloadActivity)
    .registerActivity(benchmarkHeartbeatActivity)
    .registerActivity(benchmarkActivity);
}

async function startWorkflows(
  backend: DurableBackend,
  options: BenchmarkOptions
): Promise<StartOutcome> {
  const client = new Client(backend, { payloadCodec: "Json" });
  const runs: StartedRun[] = [];
  let signals = 0;
  for (let localIndex = 0; localIndex < options.workflows; localIndex += 1) {
    const index = options.workflow_offset + localIndex;
    const workflowId = `${options.mode}-bench-${index}`;
    const workflowInput: WorkflowInput = {
      index,
      activityDelayMs: options.activity_delay_ms
    };
    let handle: WorkflowHandle<unknown>;
    switch (options.mode) {
      case "mixed":
        handle = await client.startWorkflow(benchmarkParent, workflowId, WORKFLOW_QUEUE, workflowInput);
        await client.sendSignal({
          workflowId,
          signal: finishSignal,
          payload: { value: index + 1000 },
          idempotencyKey: `signal-${index}`
        });
        signals += 1;
        break;
      case "activity":
        handle = await client.startWorkflow(
          benchmarkActivityParent,
          workflowId,
          WORKFLOW_QUEUE,
          workflowInput
        );
        break;
      case "activity-heartbeat":
        handle = await client.startWorkflow(
          benchmarkActivityHeartbeatParent,
          workflowId,
          WORKFLOW_QUEUE,
          workflowInput
        );
        break;
      case "signal":
        handle = await client.startWorkflow(
          benchmarkSignalParent,
          workflowId,
          WORKFLOW_QUEUE,
          workflowInput
        );
        await client.sendSignal({
          workflowId,
          signal: finishSignal,
          payload: { value: index + 1000 },
          idempotencyKey: `signal-${index}`
        });
        signals += 1;
        break;
      case "timer":
        handle = await client.startWorkflow(
          benchmarkTimerParent,
          workflowId,
          WORKFLOW_QUEUE,
          workflowInput
        );
        break;
      case "child":
        handle = await client.startWorkflow(
          benchmarkChildParent,
          workflowId,
          WORKFLOW_QUEUE,
          workflowInput
        );
        break;
      case "activity-map":
        handle = await client.startWorkflow(
          benchmarkActivityMapParent,
          workflowId,
          WORKFLOW_QUEUE,
          mapInput(options, index)
        );
        break;
      case "child-map":
        handle = await client.startWorkflow(
          benchmarkChildMapParent,
          workflowId,
          WORKFLOW_QUEUE,
          mapInput(options, index)
        );
        break;
      case "recovery":
        handle = await client.startWorkflow(
          benchmarkRecoveryParent,
          workflowId,
          WORKFLOW_QUEUE,
          workflowInput
        );
        break;
      case "payload":
        handle = await client.startWorkflow(benchmarkPayloadParent, workflowId, WORKFLOW_QUEUE, {
          ...workflowInput,
          body: payloadBody(index)
        });
        break;
      case "write-ceiling":
        handle = await client.startWorkflow(
          benchmarkWriteCeilingParent,
          workflowId,
          WORKFLOW_QUEUE,
          workflowInput
        );
        break;
    }
    runs.push({ runId: handle.runId, index });
  }
  return {
    runs,
    workflowStarts: options.workflows,
    signals
  };
}

async function drainWorkerRound(
  backend: DurableBackend,
  workers: readonly Worker[],
  options: BenchmarkOptions
): Promise<WorkerRunStats> {
  let stats = emptyWorkerStats();
  for (const worker of workers) {
    const beforeMetrics = worker.metrics();
    for (let index = 0; index < options.batch; index += 1) {
      const workflowTask = await worker.runWorkflowTaskOnce();
      if (workflowTask.kind !== "NoTask") {
        stats.workflowTasks += 1;
      }

      if (options.mode !== "write-ceiling") {
        const timers = await backend.fireDueTimers({
          namespace: "default",
          now: timestampMs(Date.now() + 60_000),
          limit: options.batch
        });
        if (timers.fired > 0) {
          stats.timersFired += timers.fired;
        }

        const activityTasks =
          options.activity_completion_batch > 1
            ? await worker.runActivityTaskBatchOnce(options.activity_completion_batch)
            : await worker.runActivityTaskOnce();
        if (activityTasks.kind !== "NoTask") {
          stats.activityTasks += activityTasks.kind === "Processed" ? activityTasks.tasks : 1;
        }
      }
    }
    const afterMetrics = worker.metrics();
    stats.workflowHistoryCacheHits +=
      afterMetrics.workflowHistoryCacheHits - beforeMetrics.workflowHistoryCacheHits;
    stats.workflowHistoryCacheMisses +=
      afterMetrics.workflowHistoryCacheMisses - beforeMetrics.workflowHistoryCacheMisses;
    stats.workflowHistoryCacheEvictions +=
      afterMetrics.workflowHistoryCacheEvictions - beforeMetrics.workflowHistoryCacheEvictions;
    stats.workflowExecutionCacheHits +=
      afterMetrics.workflowExecutionCacheHits - beforeMetrics.workflowExecutionCacheHits;
    stats.workflowExecutionCacheMisses +=
      afterMetrics.workflowExecutionCacheMisses - beforeMetrics.workflowExecutionCacheMisses;
    stats.workflowExecutionCacheEvictions +=
      afterMetrics.workflowExecutionCacheEvictions - beforeMetrics.workflowExecutionCacheEvictions;
    stats.historyStreamChunks += afterMetrics.historyStreamChunks - beforeMetrics.historyStreamChunks;
    stats.historyStreamEvents += afterMetrics.historyStreamEvents - beforeMetrics.historyStreamEvents;
  }
  return stats;
}

async function verifyCompletedWorkflows(
  backend: DurableBackend,
  runs: readonly StartedRun[],
  options: BenchmarkOptions,
  strict: boolean
): Promise<number> {
  let completed = 0;
  for (const expected of runs) {
    const history = await backend.streamHistory({
      runId: expected.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(Number.MAX_SAFE_INTEGER),
      maxEvents: 10_000,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const last = history.events.at(-1);
    if (last === undefined) {
      if (strict) {
        throw new Error(`run ${expected.runId} has empty history`);
      }
      continue;
    }
    if (last.data.kind === "WorkflowCompleted") {
      assertExpectedOutput(last.data.result, expected.index, options);
      completed += 1;
      continue;
    }
    if (!strict) {
      continue;
    }
    if (last.data.kind === "WorkflowFailed") {
      throw new Error(`run ${expected.runId} failed: ${last.data.failure.message}`);
    }
    if (last.data.kind === "WorkflowCancelled") {
      throw new Error(`run ${expected.runId} cancelled: ${last.data.reason}`);
    }
    throw new Error(`run ${expected.runId} is not terminal: ${last.eventType}`);
  }
  return completed;
}

function assertExpectedOutput(
  payload: PayloadRef<unknown>,
  index: number,
  options: BenchmarkOptions
): void {
  let actual: unknown;
  let expected: unknown;
  switch (options.mode) {
    case "mixed":
      actual = decodePayload<ParentOutput>(payload as PayloadRef<ParentOutput>);
      expected = {
        index,
        childValue: index * 10,
        signalValue: index + 1000,
        finished: true
      } satisfies ParentOutput;
      break;
    case "activity":
    case "activity-heartbeat":
      actual = decodePayload<ValueOutput>(payload as PayloadRef<ValueOutput>);
      expected = { index, value: index } satisfies ValueOutput;
      break;
    case "signal":
      actual = decodePayload<SignalOutput>(payload as PayloadRef<SignalOutput>);
      expected = { index, signalValue: index + 1000 } satisfies SignalOutput;
      break;
    case "timer":
      actual = decodePayload<TimerOutput>(payload as PayloadRef<TimerOutput>);
      expected = { index, fired: true } satisfies TimerOutput;
      break;
    case "child":
      actual = decodePayload<ValueOutput>(payload as PayloadRef<ValueOutput>);
      expected = { index, value: index * 10 } satisfies ValueOutput;
      break;
    case "activity-map":
      actual = decodePayload<ChildMapOutput>(payload as PayloadRef<ChildMapOutput>);
      expected = {
        index,
        sum: expectedActivityMapSum(index, options.child_map_items),
        items: options.child_map_items
      } satisfies ChildMapOutput;
      break;
    case "child-map":
      actual = decodePayload<ChildMapOutput>(payload as PayloadRef<ChildMapOutput>);
      expected = {
        index,
        sum: expectedChildMapSum(index, options.child_map_items),
        items: options.child_map_items
      } satisfies ChildMapOutput;
      break;
    case "recovery":
      actual = decodePayload<RecoveryOutput>(payload as PayloadRef<RecoveryOutput>);
      expected = {
        index,
        sum: index + (index + 1) + (index + 2),
        timers: 2
      } satisfies RecoveryOutput;
      break;
    case "payload": {
      actual = decodePayload<PayloadWorkflowOutput>(payload as PayloadRef<PayloadWorkflowOutput>);
      const body = payloadBody(index);
      expected = {
        index,
        inputBytes: body.length,
        outputBytes: `${body}:result`.length
      } satisfies PayloadWorkflowOutput;
      break;
    }
    case "write-ceiling":
      actual = decodePayload<ValueOutput>(payload as PayloadRef<ValueOutput>);
      expected = { index, value: index } satisfies ValueOutput;
      break;
  }
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `unexpected ${options.mode} output ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`
    );
  }
}

function childMapManifest(input: ChildMapInput) {
  const items = Array.from({ length: input.items }, (_, offset): WorkflowInput => ({
    index: indexForChildMapItem(input.index, offset),
    activityDelayMs: input.activityDelayMs
  }));
  return activityMapManifest(items, Math.max(1, items.length));
}

function activityMapInputManifest(input: ChildMapInput) {
  const items = Array.from({ length: input.items }, (_, offset) => ({
    value: indexForActivityMapItem(input.index, offset),
    delayMs: input.activityDelayMs
  }));
  return activityMapManifest(items, Math.max(1, items.length));
}

function mapInput(options: BenchmarkOptions, index: number): ChildMapInput {
  return {
    index,
    items: options.child_map_items,
    maxInFlight: options.child_map_max_in_flight,
    activityDelayMs: options.activity_delay_ms
  };
}

function payloadBody(index: number): string {
  return `payload-${index}-` + "x".repeat(16 * 1024);
}

function countersFromStats(
  start: StartOutcome,
  stats: WorkerRunStats,
  options: BenchmarkOptions
): BenchmarkCounters {
  const completed = start.runs.length;
  const base = {
    workflow_starts: start.workflowStarts,
    signals: start.signals,
    child_starts: 0,
    child_completions: 0,
    timer_handlers: 0,
    boot_activities: 0,
    activity_heartbeats: 0,
    child_activities: 0,
    finish_activities: 0,
    workflow_tasks: stats.workflowTasks,
    activity_tasks: stats.activityTasks,
    timers_fired: stats.timersFired,
    activities_timed_out: stats.activitiesTimedOut,
    child_workflow_starts_dispatched: stats.childWorkflowStartsDispatched
  };
  switch (options.mode) {
    case "mixed":
      return {
        ...base,
        child_starts: completed,
        child_completions: completed,
        timer_handlers: completed,
        boot_activities: completed,
        child_activities: completed,
        finish_activities: completed
      };
    case "activity":
      return { ...base, boot_activities: completed };
    case "activity-heartbeat":
      return { ...base, boot_activities: completed, activity_heartbeats: completed };
    case "signal":
      return base;
    case "timer":
      return { ...base, timer_handlers: completed };
    case "child":
      return {
        ...base,
        child_starts: completed,
        child_completions: completed,
        child_activities: completed
      };
    case "activity-map":
      return { ...base, boot_activities: completed * options.child_map_items };
    case "child-map": {
      const childItems = completed * options.child_map_items;
      return {
        ...base,
        child_starts: childItems,
        child_completions: childItems,
        child_activities: childItems
      };
    }
    case "recovery":
      return {
        ...base,
        timer_handlers: completed * 2,
        boot_activities: completed * 3
      };
    case "payload":
      return { ...base, boot_activities: completed };
    case "write-ceiling":
      return base;
  }
}

function mixedActionCount(counters: BenchmarkCounters): number {
  return (
    counters.workflow_starts +
    counters.signals +
    counters.child_starts +
    counters.child_completions +
    counters.timer_handlers +
    counters.boot_activities +
    counters.activity_heartbeats +
    counters.child_activities +
    counters.finish_activities
  );
}

function expectedChildMapSum(index: number, items: number): number {
  let sum = 0;
  for (let offset = 0; offset < items; offset += 1) {
    sum += indexForChildMapItem(index, offset) * 10;
  }
  return sum;
}

function expectedActivityMapSum(index: number, items: number): number {
  let sum = 0;
  for (let offset = 0; offset < items; offset += 1) {
    sum += indexForActivityMapItem(index, offset);
  }
  return sum;
}

function indexForChildMapItem(index: number, offset: number): number {
  return index * 1_000_000 + offset;
}

function indexForActivityMapItem(index: number, offset: number): number {
  return index * 1_000_000 + offset;
}

function workerStatsReport(stats: WorkerRunStats): WorkerStatsReport {
  return {
    workflowTasks: stats.workflowTasks,
    activityTasks: stats.activityTasks,
    workflowHistoryCacheHits: stats.workflowHistoryCacheHits,
    workflowHistoryCacheMisses: stats.workflowHistoryCacheMisses,
    workflowHistoryCacheEvictions: stats.workflowHistoryCacheEvictions,
    workflowExecutionCacheHits: stats.workflowExecutionCacheHits,
    workflowExecutionCacheMisses: stats.workflowExecutionCacheMisses,
    workflowExecutionCacheEvictions: stats.workflowExecutionCacheEvictions,
    historyStreamChunks: stats.historyStreamChunks,
    historyStreamEvents: stats.historyStreamEvents,
    timersFired: stats.timersFired,
    activitiesTimedOut: stats.activitiesTimedOut,
    childWorkflowStartsDispatched: stats.childWorkflowStartsDispatched
  };
}

function emptyWorkerStats(): WorkerRunStats {
  return {
    workflowTasks: 0,
    activityTasks: 0,
    workflowHistoryCacheHits: 0,
    workflowHistoryCacheMisses: 0,
    workflowHistoryCacheEvictions: 0,
    workflowExecutionCacheHits: 0,
    workflowExecutionCacheMisses: 0,
    workflowExecutionCacheEvictions: 0,
    historyStreamChunks: 0,
    historyStreamEvents: 0,
    timersFired: 0,
    activitiesTimedOut: 0,
    childWorkflowStartsDispatched: 0
  };
}

function addWorkerStats(left: WorkerRunStats, right: WorkerRunStats): WorkerRunStats {
  return {
    workflowTasks: left.workflowTasks + right.workflowTasks,
    activityTasks: left.activityTasks + right.activityTasks,
    workflowHistoryCacheHits:
      left.workflowHistoryCacheHits + right.workflowHistoryCacheHits,
    workflowHistoryCacheMisses:
      left.workflowHistoryCacheMisses + right.workflowHistoryCacheMisses,
    workflowHistoryCacheEvictions:
      left.workflowHistoryCacheEvictions + right.workflowHistoryCacheEvictions,
    workflowExecutionCacheHits:
      left.workflowExecutionCacheHits + right.workflowExecutionCacheHits,
    workflowExecutionCacheMisses:
      left.workflowExecutionCacheMisses + right.workflowExecutionCacheMisses,
    workflowExecutionCacheEvictions:
      left.workflowExecutionCacheEvictions + right.workflowExecutionCacheEvictions,
    historyStreamChunks: left.historyStreamChunks + right.historyStreamChunks,
    historyStreamEvents: left.historyStreamEvents + right.historyStreamEvents,
    timersFired: left.timersFired + right.timersFired,
    activitiesTimedOut: left.activitiesTimedOut + right.activitiesTimedOut,
    childWorkflowStartsDispatched:
      left.childWorkflowStartsDispatched + right.childWorkflowStartsDispatched
  };
}

function madeProgress(stats: WorkerRunStats): boolean {
  return (
    stats.workflowTasks > 0 ||
    stats.activityTasks > 0 ||
    stats.workflowHistoryCacheHits > 0 ||
    stats.workflowHistoryCacheMisses > 0 ||
    stats.workflowHistoryCacheEvictions > 0 ||
    stats.workflowExecutionCacheHits > 0 ||
    stats.workflowExecutionCacheMisses > 0 ||
    stats.workflowExecutionCacheEvictions > 0 ||
    stats.historyStreamChunks > 0 ||
    stats.historyStreamEvents > 0 ||
    stats.timersFired > 0 ||
    stats.activitiesTimedOut > 0 ||
    stats.childWorkflowStartsDispatched > 0
  );
}

function latencyReport(values: readonly number[]): LatencyReport {
  if (values.length === 0) {
    return { samples: 0, p50Ms: 0, p95Ms: 0, p99Ms: 0, maxMs: 0 };
  }
  const sorted = [...values].sort((left, right) => left - right);
  return {
    samples: sorted.length,
    p50Ms: round(percentile(sorted, 0.5)),
    p95Ms: round(percentile(sorted, 0.95)),
    p99Ms: round(percentile(sorted, 0.99)),
    maxMs: round(sorted.at(-1) ?? 0)
  };
}

function percentile(sorted: readonly number[], quantile: number): number {
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * quantile) - 1));
  return sorted[index] ?? 0;
}

function pushEqual(
  failures: BenchmarkThresholdFailure[],
  path: string,
  expected: unknown,
  actual: unknown
): void {
  if (actual === expected) {
    return;
  }
  failures.push({
    path,
    expected,
    actual,
    message: `${path} expected ${String(expected)}, got ${String(actual)}`
  });
}

function pushAtLeast(
  failures: BenchmarkThresholdFailure[],
  path: string,
  expectedMinimum: number,
  actual: number
): void {
  if (actual >= expectedMinimum) {
    return;
  }
  failures.push({
    path,
    expected: `>= ${round(expectedMinimum)}`,
    actual,
    message: `${path} expected at least ${round(expectedMinimum)}, got ${actual}`
  });
}

function pushAtMost(
  failures: BenchmarkThresholdFailure[],
  path: string,
  expectedMaximum: number,
  actual: number
): void {
  if (actual <= expectedMaximum) {
    return;
  }
  failures.push({
    path,
    expected: `<= ${round(expectedMaximum)}`,
    actual,
    message: `${path} expected at most ${round(expectedMaximum)}, got ${actual}`
  });
}

function postgresStatsDelta(
  before: PostgresBackendStatsSnapshot,
  after: PostgresBackendStatsSnapshot,
  key: Exclude<keyof PostgresBackendStatsSnapshot, "statements">
): number {
  return Math.max(0, after[key] - before[key]);
}

function dbBytes(path: string): number | null {
  try {
    return statSync(path).size;
  } catch {
    return null;
  }
}

function parseBackend(value: string): BenchmarkBackendName {
  if (value === "memory" || value === "sqlite" || value === "postgres") {
    return value;
  }
  throw new Error(`unsupported backend ${value}; expected memory, sqlite, or postgres`);
}

function parseMode(value: string): BenchmarkMode {
  if (
    value === "mixed" ||
    value === "activity" ||
    value === "activity-heartbeat" ||
    value === "signal" ||
    value === "timer" ||
    value === "child" ||
    value === "activity-map" ||
    value === "child-map" ||
    value === "recovery" ||
    value === "payload" ||
    value === "write-ceiling"
  ) {
    return value;
  }
  throw new Error(
    `unsupported mode ${value}; expected mixed, activity, activity-heartbeat, signal, timer, child, activity-map, child-map, recovery, payload, or write-ceiling`
  );
}

function parsePositiveInteger(value: string, flag: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${flag} must be a positive integer`);
  }
  return parsed;
}

function parseNonNegativeInteger(value: string, flag: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${flag} must be a non-negative integer`);
  }
  return parsed;
}

function round(value: number): number {
  return Math.round(value * 1000) / 1000;
}

function usage(): string {
  return [
    "usage: durust-benchmark-workload [--backend memory|sqlite|postgres]",
    "  [--mode mixed|activity|activity-heartbeat|signal|timer|child|activity-map|child-map|recovery|payload|write-ceiling]",
    "  [--workflows N] [--workers N] [--batch N]",
    "  [--child-map-items N] [--child-map-max-in-flight N] [--json]"
  ].join("\n");
}

class UsageError extends Error {}

type Mutable<T> = {
  -readonly [Key in keyof T]: T[Key];
};

async function main(): Promise<void> {
  try {
    const options = parseBenchmarkOptions(process.argv.slice(2));
    const result = await runBenchmark(options);
    if (options.json) {
      console.log(JSON.stringify(result, null, 2));
      return;
    }
    printHumanResult(result);
  } catch (error) {
    if (error instanceof UsageError) {
      console.log(error.message);
      return;
    }
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}

function printHumanResult(result: BenchmarkResult): void {
  console.log(`Durust TypeScript benchmark (${result.mode})`);
  console.log(`  backend: ${result.backend}`);
  console.log(`  workflows: ${result.completed_workflows}`);
  console.log(`  mixed actions: ${result.mixed_actions}`);
  console.log(`  elapsed ms: ${result.elapsed_ms.toFixed(2)}`);
  console.log(`  workflows/sec: ${result.workflows_per_second.toFixed(2)}`);
  console.log(`  mixed actions/sec: ${result.mixed_actions_per_second.toFixed(2)}`);
  console.log(
    `  commit p95 ms: ${result.backend_metrics.workflowTaskCommitLatency.p95Ms.toFixed(3)}`
  );
}

if (process.argv[1] !== undefined && import.meta.url === new URL(process.argv[1], "file:").href) {
  void main();
}
