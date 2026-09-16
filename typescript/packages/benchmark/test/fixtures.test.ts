import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  compareBenchmarkToBaseline,
  parseBenchmarkOptions,
  runBenchmark,
  type BenchmarkBaseline,
  type BenchmarkResult,
  type BenchmarkThresholdComparison
} from "@durust/benchmark";

interface BenchmarkContractFixture {
  readonly typescriptResult: BenchmarkResult;
  readonly typescriptBaseline: BenchmarkBaseline;
  readonly typescriptComparison: BenchmarkThresholdComparison;
  readonly typescriptPostgresStats: NonNullable<BenchmarkResult["postgres_stats"]>;
  readonly rustResult: {
    readonly backend: string;
    readonly mode: string;
    readonly correct: boolean;
    readonly options: {
      readonly workflows: number;
      readonly workers: number;
      readonly batch: number;
      readonly activityCompletionBatch: number;
    };
    readonly completedWorkflows: number;
    readonly mixedActions: number;
    readonly counters: {
      readonly workflowStarts: number;
      readonly signals: number;
      readonly childWorkflowStartsDispatched: number;
    };
    readonly workerStats: {
      readonly workflowTasks: number;
      readonly activityTasks: number;
      readonly historyStreamChunks: number;
      readonly historyStreamEvents: number;
    };
    readonly backendMetrics: {
      readonly workflowTaskCommitLatency: {
        readonly samples: number;
        readonly p95Ms: number;
      };
      readonly operations: Record<string, { readonly calls: number; readonly errors: number }>;
    };
    readonly postgresStats?: {
      readonly transactionsPerMixedAction: number;
      readonly statementStats?: {
        readonly callsPerMixedAction: number;
        readonly topStatements: readonly { readonly queryId: string; readonly calls: number }[];
      };
    };
    readonly resourceSamples?: {
      readonly samples: number;
      readonly maxProcessRssBytes: number;
    };
  };
  readonly rustComparison: {
    readonly dimensions: {
      readonly backend: string;
      readonly mode: string;
      readonly workflows: number;
      readonly workers: number;
      readonly batch: number;
    };
    readonly ratio: number;
    readonly minRatio: number;
    readonly passed: boolean;
  };
}

describe("benchmark contract fixture", () => {
  it("matches the TypeScript result, baseline, and threshold comparison vocabulary", () => {
    const fixture = loadFixture();

    expect(compareBenchmarkToBaseline(
      fixture.typescriptResult,
      fixture.typescriptBaseline
    )).toEqual(fixture.typescriptComparison);
    expect(fixture.typescriptResult.backend_metrics.workflowTaskCommitLatency).toMatchObject({
      samples: 32,
      p95Ms: 0.05
    });
    expect(fixture.typescriptResult.worker_stats).toMatchObject({
      workflowHistoryCacheHits: 0,
      workflowHistoryCacheMisses: 32,
      workflowHistoryCacheEvictions: 0,
      workflowExecutionCacheHits: 24,
      workflowExecutionCacheMisses: 8,
      workflowExecutionCacheEvictions: 0,
      historyStreamChunks: 0,
      historyStreamEvents: 0
    });
    expect(fixture.typescriptBaseline.thresholds).toMatchObject({
      require_exact_worker_stats: expect.arrayContaining([
        "workflowExecutionCacheHits",
        "workflowExecutionCacheMisses",
        "workflowExecutionCacheEvictions"
      ]),
      forbidden_operation_names: ["failActivity"]
    });
    expect(Object.keys(fixture.typescriptResult.backend_metrics.operations)).toEqual([
      "claimWorkflowTask",
      "commitWorkflowTask",
      "completeActivity"
    ]);
    expect(compareBenchmarkToBaseline({
      ...fixture.typescriptResult,
      backend_metrics: {
        ...fixture.typescriptResult.backend_metrics,
        operations: {
          ...fixture.typescriptResult.backend_metrics.operations,
          failActivity: {
            calls: 1,
            errors: 0,
            items: 1,
            totalMs: 0.01,
            itemsPerCall: 1,
            itemsPerSecond: 1000,
            latency: {
              samples: 1,
              p50Ms: 0.01,
              p95Ms: 0.01,
              p99Ms: 0.01,
              maxMs: 0.01
            }
          }
        }
      }
    }, fixture.typescriptBaseline).failures).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          path: "backend_metrics.operations.failActivity",
          expected: "absent"
        })
      ])
    );
    expect(fixture.typescriptPostgresStats).toMatchObject({
      walBytes: 4096,
      walRecords: 8,
      xactCommit: 12,
      xactRollback: 1,
      transactionsPerMixedAction: 1.3,
      blockCacheHitRatio: 0.9,
      activeConnectionsAfter: 2
    });
    expect(fixture.typescriptPostgresStats.statementStats).toMatchObject({
      calls: 8,
      callsPerMixedAction: 0.8,
      callsPerWorkflow: 4
    });
    expect(fixture.typescriptPostgresStats.statementStats?.topStatements[0]).toMatchObject({
      queryId: "select-1",
      calls: 5,
      query: "select 1"
    });
  });

  it("documents the Rust camelCase benchmark output vocabulary", () => {
    const fixture = loadFixture();

    expect(fixture.rustResult).toMatchObject({
      backend: "sqlite",
      mode: "mixed",
      correct: true,
      completedWorkflows: 4,
      mixedActions: 32
    });
    expect(fixture.rustResult.options).toMatchObject({
      workflows: 4,
      workers: 1,
      batch: 8,
      activityCompletionBatch: 1
    });
    expect(fixture.rustResult.counters).toMatchObject({
      workflowStarts: 4,
      signals: 4,
      childWorkflowStartsDispatched: 4
    });
    expect(fixture.rustResult.workerStats).toMatchObject({
      historyStreamChunks: 0,
      historyStreamEvents: 0
    });
    expect(fixture.rustResult.backendMetrics.operations.commit_workflow_tasks).toMatchObject({
      calls: 32,
      errors: 0
    });
    expect(fixture.rustResult.postgresStats?.statementStats).toMatchObject({
      callsPerMixedAction: 3,
      topStatements: [{ queryId: "fixture-query", calls: 32 }]
    });
    expect(fixture.rustResult.resourceSamples).toMatchObject({
      samples: 1,
      maxProcessRssBytes: 104857600
    });
    expect(fixture.rustComparison).toMatchObject({
      dimensions: {
        backend: "sqlite",
        mode: "mixed",
        workflows: 4,
        workers: 1,
        batch: 8
      },
      ratio: 1,
      minRatio: 0.95,
      passed: true
    });
  });
});

function loadFixture(): BenchmarkContractFixture {
  const fixtureUrl = new URL("../../../fixtures/contract/benchmark-output.json", import.meta.url);
  return JSON.parse(readFileSync(fixtureUrl, "utf8")) as BenchmarkContractFixture;
}

/**
 * Every key a runner emits, at every object level, must be in the fixture's
 * result for its runtime, so a field added on one runtime is added to the
 * vocabulary both runtimes read. The Rust twin is
 * `benchtools`' `memory_mixed_workload_completes`.
 */
function keysMissingFrom(emitted: unknown, fixture: unknown, path: string, missing: string[]): void {
  if (emitted === null || typeof emitted !== "object" || Array.isArray(emitted)) {
    return;
  }
  if (fixture === null || typeof fixture !== "object" || Array.isArray(fixture)) {
    return;
  }
  for (const [key, value] of Object.entries(emitted)) {
    if (!(key in fixture)) {
      missing.push(`${path}.${key}`);
      continue;
    }
    const expected = (fixture as Record<string, unknown>)[key];
    // `operations` is keyed by operation name; every entry shares one shape,
    // which any fixture entry pins.
    if (key === "operations" && expected !== null && typeof expected === "object") {
      const sample = Object.values(expected as Record<string, unknown>)[0];
      for (const [name, entry] of Object.entries((value ?? {}) as Record<string, unknown>)) {
        keysMissingFrom(entry, sample, `${path}.${key}.${name}`, missing);
      }
      continue;
    }
    keysMissingFrom(value, expected, `${path}.${key}`, missing);
  }
}

describe("benchmark output vocabulary", () => {
  it("emits no key the shared fixture lacks", async () => {
    const fixture = loadFixture();
    const result = await runBenchmark({
      ...parseBenchmarkOptions(["--backend", "memory", "--mode", "mixed", "--workflows", "2", "--workers", "1"])
    });
    const missing: string[] = [];
    keysMissingFrom(result, fixture.typescriptResult, "typescriptResult", missing);
    expect(missing, "keys absent from typescript/fixtures/contract/benchmark-output.json").toEqual([]);
  });
});
