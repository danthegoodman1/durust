import { readFileSync } from "node:fs";
import { afterAll, describe, expect, it } from "vitest";
import {
  compareBenchmarkToBaseline,
  defaultBenchmarkOptions,
  postgresStatsReportFromSnapshots,
  runBenchmark,
  type BackendMetricsReport,
  type BackendOperationReport,
  type BenchmarkBaseline,
  type BenchmarkResult,
  type PostgresBackendStatsSnapshot
} from "@durust/benchmark";
import { assertPostgresAvailableWhenRequired, postgresUrlFromEnv } from "@durust/testing";

const postgresUrl = postgresUrlFromEnv();
const postgresStatementStatsRequired = process.env.DURUST_REQUIRE_POSTGRES_STATEMENT_STATS === "1";

// Every accepted baseline below gates throughput through its own
// `min_processing_*_per_second_ratio`, which is 0.1 — a machine ten times
// slower than the one that recorded it still passes. Vitest's default 5 s
// case timeout is a *tighter* wall-clock gate than that, so on a loaded CI
// runner the clock fired before the comparison ran and reported "Test timed
// out" instead of the threshold that was actually missed. This is large
// enough that the baseline comparison stays the thing that fails.
const ACCEPTED_BASELINE_TIMEOUT_MS = 120_000;

/** The baseline with its statement-statistics gates lifted, for a server that has none. */
function withoutStatementStatsThresholds(baseline: BenchmarkBaseline): BenchmarkBaseline {
  const {
    require_postgres_statement_stats: _required,
    max_postgres_statement_calls_per_mixed_action: _max,
    max_postgres_statement_calls_per_mixed_action_ratio: _ratio,
    ...thresholds
  } = baseline.thresholds;
  return { ...baseline, thresholds };
}

// The sibling of the guard in `packages/postgres/test/postgres-conformance.test.ts`,
// and it has to be at module scope for the same measured reason: a suite whose
// cases are all `it.skip` reports success, and only a module-body throw runs
// early enough to stop it.
//
// This file is the *weaker* of the two cases, and deliberately so rather than
// by oversight. Nothing in CI runs it with a database — `npm run check` leaves
// `DURUST_POSTGRES_URL` unset precisely so these throughput comparisons stay
// out of it — and the one script that does run it, `scripts/check-postgres.mjs`
// (also reached through `check:release`), already exits 1 on a missing or blank
// URL before Vitest starts. So the hole this closes is only reachable by
// invoking `npm run test:benchmark-thresholds` directly with
// `DURUST_REQUIRE_POSTGRES` set. It is here anyway, because the *reason* it is
// currently unreachable is a property of one npm script, and that is not a
// thing the next person editing CI would think to check.
assertPostgresAvailableWhenRequired(postgresUrl, "the env-gated Postgres benchmark thresholds");

/** See the identical counter in the Postgres conformance suite. */
let executedPostgresCases = 0;

afterAll(() => {
  if (postgresUrl === undefined) {
    return;
  }
  if (executedPostgresCases < 2) {
    throw new Error(
      "DURUST_POSTGRES_URL is set, so both env-gated Postgres baselines must run, but " +
        `${executedPostgresCases} did. If you filtered the run with \`-t\`, that is the cause and ` +
        "the filtered cases still passed; this check exists for the unfiltered runs CI makes, " +
        "where a silently skipped Postgres baseline would report success"
    );
  }
});

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
  }, ACCEPTED_BASELINE_TIMEOUT_MS);

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
  }, ACCEPTED_BASELINE_TIMEOUT_MS);

  const itPostgres = postgresUrl === undefined ? it.skip : it;

  itPostgres("passes the env-gated Postgres mixed smoke baseline", async () => {
    executedPostgresCases += 1;
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

    expect(result.postgres_schema).toMatch(/^durust_ts_benchmark_/);
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
      executedPostgresCases += 1;
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

      expect(result.postgres_schema).toMatch(/^durust_ts_benchmark_/);
      expect(result.postgres_stats).not.toBeNull();
      // Statement statistics need a server started with
      // `shared_preload_libraries=pg_stat_statements`, which the compose
      // fixture provides and a stock container does not. They are asserted
      // when `DURUST_REQUIRE_POSTGRES_STATEMENT_STATS=1` says the server has
      // them, with the server's own reason as the message when they are
      // missing; otherwise the case reports the reason and gates on
      // everything else.
      const statementStats = result.postgres_stats?.statementStats ?? null;
      if (postgresStatementStatsRequired) {
        expect(
          statementStats,
          result.postgres_stats?.statementStatsUnavailable ??
            "statement stats are missing and the provider recorded no reason"
        ).not.toBeNull();
        expect(statementStats?.calls).toBeGreaterThan(0);
      } else if (statementStats === null) {
        console.warn(
          `postgres statement stats not asserted: ${result.postgres_stats?.statementStatsUnavailable ?? "no reason recorded"}`
        );
      }
      const comparedBaseline =
        statementStats === null && !postgresStatementStatsRequired
          ? withoutStatementStatsThresholds(baseline)
          : baseline;
      expect(compareBenchmarkToBaseline(result, comparedBaseline)).toMatchObject({
        passed: true,
        baseline: "postgres-mixed-accepted",
        failures: []
      });
    },
    ACCEPTED_BASELINE_TIMEOUT_MS
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
            ...backendOperation(result.backend_metrics, "commitWorkflowTask"),
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

/**
 * Resolve one recorded backend operation, or throw naming the one that is gone.
 *
 * `backend_metrics.operations` is a `Record<string, …>`, so an operation the
 * benchmark stopped recording reads back as `undefined` rather than failing.
 * Spreading that `undefined` into the regressed fixture above built
 * `{ errors: 1 }` — no `calls`, no `latency` — and `compareBenchmarkToBaseline`
 * treats any present entry with `errors !== 0` as a failure, so it would still
 * emit the `backend_metrics.operations.commitWorkflowTask.errors` path the test
 * asserts on. The test would keep passing while `runBenchmark` no longer
 * measured the operation at all. Throwing here is the difference between
 * "the regression path is still reported" and "the operation still exists".
 */
function backendOperation(
  metrics: BackendMetricsReport,
  name: string
): BackendOperationReport {
  const operation = metrics.operations[name];
  if (operation === undefined) {
    const recorded = Object.keys(metrics.operations).sort().join(", ");
    throw new Error(
      `benchmark recorded no "${name}" backend operation, so this test can no longer say ` +
        `anything about it: the benchmark stopped exercising "${name}", or it was renamed. ` +
        `Recorded operations: ${recorded.length === 0 ? "(none)" : recorded}`
    );
  }
  return operation;
}

function loadBaseline(name: string): BenchmarkBaseline {
  return JSON.parse(
    readFileSync(new URL(`../baselines/${name}`, import.meta.url), "utf8")
  ) as BenchmarkBaseline;
}

function postgresSnapshot(
  overrides: Partial<PostgresBackendStatsSnapshot>
): PostgresBackendStatsSnapshot {
  return {
    ...postgresSnapshotDefaults(),
    ...overrides
  };
}

// Annotated with the real provider type on purpose, not inferred. While this
// was unannotated it silently drifted: it omitted `statementStatsUnavailable`
// entirely, so every fixture snapshot was missing a field the type declares as
// required, and `statements: []` inferred as `never[]` — which is why the four
// literal statement rows below it could not be passed in as overrides. Neither
// test asserts on `statementStatsUnavailable`, so nothing caught it; the
// annotation is what catches the next omission.
function postgresSnapshotDefaults(): PostgresBackendStatsSnapshot {
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
    statements: [],
    // `null` is the provider's "nothing went wrong" value, which is what these
    // hand-built snapshots mean: they are not modelling a failed collection.
    statementStatsUnavailable: null
  };
}
