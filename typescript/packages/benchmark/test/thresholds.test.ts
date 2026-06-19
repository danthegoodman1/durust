import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  compareBenchmarkToBaseline,
  defaultBenchmarkOptions,
  postgresStatsReportFromSnapshots,
  runBenchmark,
  type BenchmarkBaseline,
  type BenchmarkResult
} from "@durust/benchmark";

describe("benchmark threshold comparison", () => {
  it("passes the memory mixed smoke baseline", async () => {
    const baseline = loadBaseline("memory-mixed-smoke.json");
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "memory",
      mode: "mixed",
      workflows: 4,
      workers: 1,
      batch: 8,
      max_rounds: 200
    });

    expect(compareBenchmarkToBaseline(result, baseline)).toMatchObject({
      passed: true,
      baseline: "memory-mixed-smoke",
      failures: []
    });
  });

  it("passes the memory child-map smoke baseline", async () => {
    const baseline = loadBaseline("memory-child-map-smoke.json");
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "memory",
      mode: "child-map",
      workflows: 2,
      workers: 1,
      batch: 8,
      max_rounds: 200,
      child_map_items: 3,
      child_map_max_in_flight: 2
    });

    expect(compareBenchmarkToBaseline(result, baseline)).toMatchObject({
      passed: true,
      baseline: "memory-child-map-smoke",
      failures: []
    });
  });

  it("passes the memory activity-heartbeat smoke baseline", async () => {
    const baseline = loadBaseline("memory-activity-heartbeat-smoke.json");
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "memory",
      mode: "activity-heartbeat",
      workflows: 4,
      workers: 1,
      batch: 8,
      max_rounds: 200
    });

    expect(compareBenchmarkToBaseline(result, baseline)).toMatchObject({
      passed: true,
      baseline: "memory-activity-heartbeat-smoke",
      failures: []
    });
  });

  it("passes the memory write-ceiling smoke baseline", async () => {
    const baseline = loadBaseline("memory-write-ceiling-smoke.json");
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "memory",
      mode: "write-ceiling",
      workflows: 4,
      workers: 1,
      batch: 8,
      max_rounds: 200
    });

    expect(compareBenchmarkToBaseline(result, baseline)).toMatchObject({
      passed: true,
      baseline: "memory-write-ceiling-smoke",
      failures: []
    });
  });

  it("passes the memory mixed accepted-local baseline", async () => {
    const baseline = loadBaseline("memory-mixed-local-4-worker.json");
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "memory",
      mode: "mixed",
      workflows: 1000,
      workers: 4,
      batch: 32,
      activity_completion_batch: 1
    });

    expect(compareBenchmarkToBaseline(result, baseline)).toMatchObject({
      passed: true,
      baseline: "memory-mixed-local-4-worker",
      failures: []
    });
  });

  it.each([
    ["sqlite-mixed-local-1-worker.json", 1],
    ["sqlite-mixed-local-4-worker.json", 4]
  ] as const)("passes the %s accepted baseline", async (baselineName, workers) => {
    const baseline = loadBaseline(baselineName);
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "sqlite",
      mode: "mixed",
      workflows: 100,
      workers,
      batch: 32,
      activity_completion_batch: 1
    });

    expect(compareBenchmarkToBaseline(result, baseline)).toMatchObject({
      passed: true,
      baseline: baseline.name,
      failures: []
    });
  });

  const postgresUrl = process.env.DURUST_POSTGRES_URL;
  const itPostgres = postgresUrl === undefined ? it.skip : it;

  itPostgres("passes the env-gated Postgres mixed smoke baseline", async () => {
    const baseline = loadBaseline("postgres-mixed-smoke.json");
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "postgres",
      mode: "mixed",
      workflows: 4,
      workers: 1,
      batch: 8,
      max_rounds: 200,
      activity_completion_batch: 1,
      postgres_pool_size: 10
    });

    expect(result.postgres_schema).toBe("normalized");
    expect(result.postgres_stats).not.toBeNull();
    if (result.postgres_stats?.statementStats !== null) {
      expect(result.postgres_stats?.statementStats.calls).toBeGreaterThan(0);
    }
    expect(compareBenchmarkToBaseline(result, baseline)).toMatchObject({
      passed: true,
      baseline: "postgres-mixed-smoke",
      failures: []
    });
  });

  itPostgres(
    "passes the env-gated Postgres mixed accepted baseline",
    async () => {
      const baseline = loadBaseline("postgres-mixed-accepted.json");
      const result = await runBenchmark({
        ...defaultBenchmarkOptions(),
        backend: "postgres",
        mode: "mixed",
        workflows: 1000,
        workers: 10,
        batch: 32,
        activity_completion_batch: 32,
        postgres_pool_size: 24
      });

      expect(result.postgres_schema).toBe("normalized");
      expect(result.postgres_stats).not.toBeNull();
      expect(result.postgres_stats?.statementStats).not.toBeNull();
      expect(result.postgres_stats?.statementStats?.calls).toBeGreaterThan(0);
      expect(compareBenchmarkToBaseline(result, baseline)).toMatchObject({
        passed: true,
        baseline: "postgres-mixed-accepted",
        failures: []
      });
    },
    180_000
  );

  it("reports logical counter and latency failures with paths", async () => {
    const baseline = loadBaseline("memory-mixed-smoke.json");
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "memory",
      mode: "mixed",
      workflows: 4,
      workers: 1,
      batch: 8,
      max_rounds: 200
    });
    const regressed: BenchmarkResult = {
      ...result,
      mixed_actions: result.mixed_actions - 1,
      counters: {
        ...result.counters,
        signals: result.counters.signals - 1
      },
      backend_metrics: {
        ...result.backend_metrics,
        workflowTaskCommitLatency: {
          ...result.backend_metrics.workflowTaskCommitLatency,
          p95Ms: 10_000
        },
        operations: {
          ...result.backend_metrics.operations,
          commitWorkflowTask: {
            ...result.backend_metrics.operations.commitWorkflowTask,
            errors: 1
          }
        }
      }
    };

    const comparison = compareBenchmarkToBaseline(regressed, baseline);
    expect(comparison.passed).toBe(false);
    expect(comparison.failures.map((failure) => failure.path)).toEqual(
      expect.arrayContaining([
        "mixed_actions",
        "counters.signals",
        "backend_metrics.operations.commitWorkflowTask.errors",
        "backend_metrics.workflowTaskCommitLatency.p95Ms"
      ])
    );
  });

  it("reports forbidden backend operations with paths", async () => {
    const baseline: BenchmarkBaseline = {
      ...loadBaseline("memory-write-ceiling-smoke.json"),
      thresholds: {
        require_correct: false,
        require_profile_match: false,
        require_exact_completed_workflows: false,
        require_exact_mixed_actions: false,
        forbidden_operation_names: ["fireDueTimers"]
      }
    };
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "memory",
      mode: "timer",
      workflows: 1,
      workers: 1,
      batch: 4,
      max_rounds: 100
    });

    expect(compareBenchmarkToBaseline(result, baseline).failures).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          path: "backend_metrics.operations.fireDueTimers",
          expected: "absent"
        })
      ])
    );
  });

  it("reports Postgres accepted-stat failures with paths", async () => {
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "memory",
      mode: "mixed",
      workflows: 4,
      workers: 1,
      batch: 8,
      max_rounds: 200
    });
    const baseline: BenchmarkBaseline = {
      ...loadBaseline("memory-mixed-smoke.json"),
      result: {
        ...loadBaseline("memory-mixed-smoke.json").result,
        postgres_stats: {
          transactionsPerMixedAction: 1,
          statementStats: {
            callsPerMixedAction: 2
          }
        }
      },
      thresholds: {
        require_correct: false,
        require_profile_match: false,
        require_exact_completed_workflows: false,
        require_exact_mixed_actions: false,
        require_postgres_schema: "normalized",
        require_postgres_stats: true,
        require_postgres_statement_stats: true,
        max_postgres_transactions_per_mixed_action_ratio: 1.1,
        max_postgres_statement_calls_per_mixed_action_ratio: 1.1
      }
    };

    expect(compareBenchmarkToBaseline(result, baseline).failures.map((failure) => failure.path))
      .toEqual(
        expect.arrayContaining([
          "postgres_schema",
          "postgres_stats",
          "postgres_stats.statementStats",
          "postgres_stats.transactionsPerMixedAction",
          "postgres_stats.statementStats.callsPerMixedAction"
        ])
      );
  });

  it("derives Postgres stats deltas and rates from provider snapshots", () => {
    const before = postgresSnapshot({
      walBytes: 100,
      walRecords: 10,
      xactCommit: 20,
      xactRollback: 2,
      blocksRead: 5,
      blocksHit: 15,
      statements: [
        {
          queryId: "select-1",
          query: "select 1",
          calls: 10,
          totalExecTimeMs: 2
        },
        {
          queryId: "update-state",
          query: "update state",
          calls: 1,
          totalExecTimeMs: 5
        }
      ]
    });
    const after = postgresSnapshot({
      walBytes: 4196,
      walRecords: 18,
      walFpi: 1,
      rowsReturned: 7,
      rowsFetched: 6,
      rowsInserted: 5,
      rowsUpdated: 4,
      rowsDeleted: 3,
      xactCommit: 32,
      xactRollback: 3,
      blocksRead: 6,
      blocksHit: 24,
      tempBytes: 256,
      blockReadTimeMs: 1.5,
      activeConnections: 2,
      statements: [
        {
          queryId: "select-1",
          query: "select 1",
          calls: 15,
          totalExecTimeMs: 3.25
        },
        {
          queryId: "update-state",
          query: "update state",
          calls: 4,
          totalExecTimeMs: 7
        }
      ]
    });

    const report = postgresStatsReportFromSnapshots(before, after, {
      elapsedMs: 1000,
      mixedActions: 10,
      workflows: 2
    });

    expect(report).toMatchObject({
      walBytes: 4096,
      walBytesPerSecond: 4096,
      walRecords: 8,
      walRecordsPerSecond: 8,
      walFpi: 1,
      xactCommit: 12,
      xactRollback: 1,
      transactionsPerSecond: 13,
      transactionsPerMixedAction: 1.3,
      transactionsPerWorkflow: 6.5,
      rowsReturned: 7,
      rowsFetched: 6,
      rowsInserted: 5,
      rowsUpdated: 4,
      rowsDeleted: 3,
      blocksRead: 1,
      blocksHit: 9,
      blockCacheHitRatio: 0.9,
      tempBytes: 256,
      blockReadTimeMs: 1.5,
      activeConnectionsAfter: 2,
      statementStats: {
        calls: 8,
        callsPerMixedAction: 0.8,
        callsPerWorkflow: 4,
        totalExecTimeMs: 3.25,
        topStatements: [
          {
            queryId: "select-1",
            calls: 5,
            totalExecTimeMs: 1.25,
            query: "select 1"
          },
          {
            queryId: "update-state",
            calls: 3,
            totalExecTimeMs: 2,
            query: "update state"
          }
        ]
      }
    });
  });

  it("leaves Postgres statement stats null when pg_stat_statements is unavailable", () => {
    expect(postgresStatsReportFromSnapshots(postgresSnapshot({}), postgresSnapshot({}), {
      elapsedMs: 1000,
      mixedActions: 1,
      workflows: 1
    }).statementStats).toBeNull();
  });
});

function loadBaseline(name: string): BenchmarkBaseline {
  return JSON.parse(
    readFileSync(new URL(`../baselines/${name}`, import.meta.url), "utf8")
  ) as BenchmarkBaseline;
}

function postgresSnapshot(
  overrides: Partial<ReturnType<typeof postgresSnapshotDefaults>>
): ReturnType<typeof postgresSnapshotDefaults> {
  return {
    ...postgresSnapshotDefaults(),
    ...overrides
  };
}

function postgresSnapshotDefaults() {
  return {
    walBytes: 0,
    walRecords: 0,
    walFpi: 0,
    walBuffersFull: 0,
    walWrite: 0,
    walSync: 0,
    walWriteTimeMs: 0,
    walSyncTimeMs: 0,
    xactCommit: 0,
    xactRollback: 0,
    rowsReturned: 0,
    rowsFetched: 0,
    rowsInserted: 0,
    rowsUpdated: 0,
    rowsDeleted: 0,
    blocksRead: 0,
    blocksHit: 0,
    tempFiles: 0,
    tempBytes: 0,
    deadlocks: 0,
    blockReadTimeMs: 0,
    blockWriteTimeMs: 0,
    activeConnections: 0,
    statements: []
  };
}
