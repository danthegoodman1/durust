import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  compareBenchmarkToBaseline,
  defaultBenchmarkOptions,
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
});

function loadBaseline(name: string): BenchmarkBaseline {
  return JSON.parse(
    readFileSync(new URL(`../baselines/${name}`, import.meta.url), "utf8")
  ) as BenchmarkBaseline;
}
