import { describe, expect, it } from "vitest";
import {
  defaultBenchmarkOptions,
  parseBenchmarkOptions,
  runBenchmark
} from "@durust/benchmark";

describe("TypeScript benchmark workload", () => {
  it("parses Rust-compatible mixed workload flags", () => {
    const options = parseBenchmarkOptions([
      "--backend",
      "postgres",
      "--mode",
      "mixed",
      "--workflows",
      "10",
      "--workers",
      "2",
      "--shards",
      "100",
      "--physical-partitions",
      "16",
      "--activation-concurrency",
      "8",
      "--activation-prefetch-limit",
      "32",
      "--batch",
      "16",
      "--activity-completion-batch",
      "4",
      "--postgres-pool-size",
      "24",
      "--json"
    ]);

    expect(options).toMatchObject({
      backend: "postgres",
      mode: "mixed",
      workflows: 10,
      workers: 2,
      shards: 100,
      physical_partitions: 16,
      activation_concurrency: 8,
      activation_prefetch_limit: 32,
      batch: 16,
      activity_completion_batch: 4,
      postgres_pool_size: 24,
      json: true
    });
  });

  it("runs the memory mixed workload and emits stable JSON fields", async () => {
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "memory",
      mode: "mixed",
      workflows: 4,
      workers: 1,
      batch: 8,
      activity_completion_batch: 2,
      max_rounds: 200
    });

    expect(result.correct).toBe(true);
    expect(result.completed_workflows).toBe(4);
    expect(result.mixed_actions).toBe(32);
    expect(result.counters.workflow_starts).toBe(4);
    expect(result.counters.signals).toBe(4);
    expect(result.counters.boot_activities).toBe(4);
    expect(result.counters.child_activities).toBe(4);
    expect(result.counters.finish_activities).toBe(4);
    expect(result.worker_stats.workflowTasks).toBeGreaterThan(0);
    expect(result.worker_stats.activityTasks).toBeGreaterThan(0);
    expect(result.backend_metrics.workflowTaskCommitLatency.samples).toBeGreaterThan(0);
    expect(result.backend_metrics.operations.commitWorkflowTask.calls).toBeGreaterThan(0);
    expect(result.backend_metrics.operations.completeActivities.calls).toBeGreaterThan(0);
  });

  it("runs the memory child-map workload with bounded fanout", async () => {
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

    expect(result.correct).toBe(true);
    expect(result.completed_workflows).toBe(2);
    expect(result.counters.child_starts).toBe(6);
    expect(result.counters.child_completions).toBe(6);
    expect(result.counters.child_activities).toBe(6);
    expect(result.mixed_actions).toBe(20);
  });

  it.each([
    ["activity", 4, { boot_activities: 2 }],
    ["signal", 4, { signals: 2 }],
    ["timer", 4, { timer_handlers: 2 }],
    ["child", 8, { child_starts: 2, child_completions: 2, child_activities: 2 }],
    ["activity-map", 8, { boot_activities: 6 }],
    ["recovery", 12, { boot_activities: 6, timer_handlers: 4 }],
    ["payload", 4, { boot_activities: 2 }]
  ] as const)("runs the memory %s workload", async (mode, mixedActions, expectedCounters) => {
    const result = await runBenchmark({
      ...defaultBenchmarkOptions(),
      backend: "memory",
      mode,
      workflows: 2,
      workers: 1,
      batch: 8,
      max_rounds: 300,
      child_map_items: 3,
      child_map_max_in_flight: 2
    });

    expect(result.correct).toBe(true);
    expect(result.completed_workflows).toBe(2);
    expect(result.mixed_actions).toBe(mixedActions);
    expect(result.counters).toMatchObject({
      workflow_starts: 2,
      ...expectedCounters
    });
    expect(result.backend_metrics.operations.claimWorkflowTask.calls).toBeGreaterThan(0);
    expect(result.backend_metrics.operations.commitWorkflowTask.calls).toBeGreaterThan(0);
  });
});
