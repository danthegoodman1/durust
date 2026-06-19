import { describe, expect, it } from "vitest";
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
  encodePayload,
  eventId,
  signal,
  sleep,
  timestampMs,
  workflow,
  workflowId,
  type DurableBackend,
  type WorkerMetricsSnapshot
} from "@durust/core";

const WORKFLOW_QUEUE = "workflows";
const ACTIVITY_QUEUE = "activities";
const LONG_SOAK_ENABLED = process.env.DURUST_LONG_SOAK === "1";
const LONG_SOAK_SEED_BASE = positiveIntEnv("DURUST_LONG_SOAK_SEED_BASE", 1_009);
const LONG_SOAK_SEEDS = positiveIntEnv("DURUST_LONG_SOAK_SEEDS", 12);
const LONG_SOAK_WORKFLOWS = positiveIntEnv("DURUST_LONG_SOAK_WORKFLOWS", 12);
const LONG_SOAK_GENERATIONS = positiveIntEnv("DURUST_LONG_SOAK_GENERATIONS", 4);
const LONG_SOAK_STEPS = positiveIntEnv("DURUST_LONG_SOAK_STEPS", 360);
const LONG_SOAK_FINAL_STEPS = positiveIntEnv("DURUST_LONG_SOAK_FINAL_STEPS", 20_000);
const LONG_SOAK_CONFLICTS = positiveIntEnv("DURUST_LONG_SOAK_CONFLICTS", 12);

interface SimInput {
  readonly value: number;
}

interface SimOutput {
  readonly value: number;
}

type SimulationNoInput = {};

const simActivity = activity({
  name: "simulation.activity",
  handler: async (input: SimInput): Promise<SimOutput> => ({
    value: input.value + 1
  })
});

const simChildWorkflow = workflow({
  name: "simulation.child",
  version: 1,
  handler: async (input: SimInput): Promise<SimOutput> => {
    const activityResult = await callActivity(
      simActivity,
      { value: input.value * 10 },
      { taskQueue: ACTIVITY_QUEUE }
    );
    return { value: activityResult.value };
  }
});

const echoSimulationWorkflow = workflow({
  name: "simulation.echo",
  version: 1,
  handler: async (input: SimInput): Promise<SimOutput> => input
});

const activitySimulationWorkflow = workflow({
  name: "simulation.activity-parent",
  version: 1,
  handler: async (input: SimInput): Promise<SimOutput> => {
    return await callActivity(
      simActivity,
      { value: input.value },
      { taskQueue: ACTIVITY_QUEUE }
    );
  }
});

const approvalSignal = signal<{ readonly value: number }>("approved");

const mixedSimulationWorkflow = workflow({
  name: "simulation.mixed-parent",
  version: 1,
  handler: async (input: SimInput): Promise<{
    readonly boot: number;
    readonly child: number;
    readonly approval: number;
    readonly finish: number;
  }> => {
    const boot = await callActivity(
      simActivity,
      { value: input.value },
      { taskQueue: ACTIVITY_QUEUE }
    );
    const child = await childWorkflow(
      simChildWorkflow,
      { value: boot.value },
      { workflowId: `simulation-child/${input.value}`, taskQueue: WORKFLOW_QUEUE }
    ).spawn();
    const childResult = await child.result();
    const approval = await approvalSignal;
    await sleep(10);
    const finish = await callActivity(
      simActivity,
      { value: approval.value + childResult.value },
      { taskQueue: ACTIVITY_QUEUE }
    );
    return {
      boot: boot.value,
      child: childResult.value,
      approval: approval.value,
      finish: finish.value
    };
  }
});

const activityMapSimulationWorkflow = workflow({
  name: "simulation.activity-map-parent",
  version: 1,
  handler: async (_input: SimulationNoInput): Promise<{ readonly sum: number }> => {
    const mapped = activityMap(simActivity, {
      inputManifest: activityMapManifest(
        [{ value: 1 }, { value: 2 }, { value: 3 }, { value: 4 }, { value: 5 }],
        2
      ),
      resultManifest: "simulation-activity-map-results",
      taskQueue: ACTIVITY_QUEUE,
      maxInFlight: 2
    });
    const results = decodeActivityMapResults(await mapped.resultManifest());
    return { sum: results.reduce((sum, result) => sum + result.value, 0) };
  }
});

const childMapSimulationWorkflow = workflow({
  name: "simulation.child-map-parent",
  version: 1,
  handler: async (_input: SimulationNoInput): Promise<{ readonly values: readonly number[] }> => {
    const mapped = childWorkflowMap(simChildWorkflow, {
      inputManifest: activityMapManifest([{ value: 1 }, { value: 2 }, { value: 3 }, { value: 4 }], 2),
      resultManifest: "simulation-child-map-results",
      workflowIdPrefix: "simulation-child-map",
      taskQueue: WORKFLOW_QUEUE,
      maxInFlight: 2
    });
    return {
      values: decodeChildWorkflowMapSuccesses<SimOutput>(await mapped.resultManifest()).map(
        (result) => result.value
      )
    };
  }
});

describe("seeded worker/provider simulations", () => {
  it("recovers a workflow task after a worker crashes with an uncommitted claim", async () => {
    let now = 1_000;
    const backend = new MemoryBackend({ nowMs: () => now });
    const registry = simulationRegistry().registerWorkflow(echoSimulationWorkflow);
    const client = new Client(backend, { payloadCodec: "Json" });
    const handle = await client.startWorkflow(
      echoSimulationWorkflow,
      workflowId("wf/simulation-crashed-workflow-claim"),
      WORKFLOW_QUEUE,
      { value: 7 }
    );

    const crashedClaim = await backend.claimWorkflowTask("crashed-workflow-worker", {
      namespace: "default",
      taskQueue: WORKFLOW_QUEUE,
      registeredWorkflowTypes: [echoSimulationWorkflow.workflowType],
      leaseDurationMs: 10
    });
    expect(crashedClaim).not.toBeNull();
    await expect(
      workerFor(backend, registry, "replacement-workflow-worker").runWorkflowTaskOnce()
    ).resolves.toEqual({ kind: "NoTask" });

    now = 1_011;
    await expect(
      workerFor(backend, registry, "replacement-workflow-worker").runWorkflowTaskOnce()
    ).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ value: 7 });
    await expect(
      backend.commitWorkflowTask(crashedClaim!.claim, {
        expectedTailEventId: eventId(1),
        appendEvents: [{ data: { kind: "WorkflowTaskStarted" } }]
      })
    ).rejects.toThrow("stale workflow task lease");
  });

  it("recovers an activity task after a worker crashes with an uncompleted claim", async () => {
    let now = 2_000;
    const backend = new MemoryBackend({ nowMs: () => now });
    const registry = simulationRegistry().registerWorkflow(activitySimulationWorkflow);
    const client = new Client(backend, { payloadCodec: "Json" });
    const handle = await client.startWorkflow(
      activitySimulationWorkflow,
      workflowId("wf/simulation-crashed-activity-claim"),
      WORKFLOW_QUEUE,
      { value: 9 }
    );

    await expect(workerFor(backend, registry, "workflow-worker-a").runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    const crashedClaim = await backend.claimActivityTask("crashed-activity-worker", {
      namespace: "default",
      taskQueue: ACTIVITY_QUEUE,
      registeredActivityNames: [simActivity.name],
      leaseDurationMs: 10
    });
    expect(crashedClaim).not.toBeNull();
    await expect(
      activityWorkerFor(backend, registry, "replacement-activity-worker").runActivityTaskOnce()
    ).resolves.toEqual({ kind: "NoTask" });

    now = 2_011;
    await expect(
      backend.completeActivity({
        claim: crashedClaim!.claim,
        result: encodePayload({ value: 10 }, { codec: "Json" })
      })
    ).rejects.toThrow("stale activity task lease");
    await expect(
      activityWorkerFor(backend, registry, "replacement-activity-worker").runActivityTaskOnce()
    ).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(workerFor(backend, registry, "workflow-worker-b").runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ value: 10 });
  });

  it("survives a replay-first fault soak with stale claims, duplicate signals, timers, and child recovery", async () => {
    let now = 5_000;
    const backend = new MemoryBackend({ nowMs: () => now });
    const registry = simulationRegistry().registerWorkflow(mixedSimulationWorkflow);
    const client = new Client(backend, { payloadCodec: "Json" });
    const workflowKey = workflowId("wf/simulation-replay-fault-soak");
    const handle = await client.startWorkflow(
      mixedSimulationWorkflow,
      workflowKey,
      WORKFLOW_QUEUE,
      { value: 21 }
    );

    const crashedWorkflowClaim = await backend.claimWorkflowTask("crashed-workflow-worker", {
      namespace: "default",
      taskQueue: WORKFLOW_QUEUE,
      registeredWorkflowTypes: [mixedSimulationWorkflow.workflowType],
      leaseDurationMs: 5
    });
    expect(crashedWorkflowClaim).not.toBeNull();
    await expect(
      workerFor(backend, registry, "workflow-worker-before-expiry", ["approved"], 5)
        .runWorkflowTaskOnce()
    ).resolves.toEqual({ kind: "NoTask" });

    now += 6;
    await expect(
      workerFor(backend, registry, "workflow-worker-after-expiry", ["approved"], 5)
        .runWorkflowTaskOnce()
    ).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(
      backend.commitWorkflowTask(crashedWorkflowClaim!.claim, {
        expectedTailEventId: eventId(1),
        appendEvents: [{ data: { kind: "WorkflowTaskStarted" } }]
      })
    ).rejects.toThrow("stale workflow task lease");

    const crashedActivityClaim = await backend.claimActivityTask("crashed-activity-worker", {
      namespace: "default",
      taskQueue: ACTIVITY_QUEUE,
      registeredActivityNames: [simActivity.name],
      leaseDurationMs: 5
    });
    expect(crashedActivityClaim).not.toBeNull();
    await expect(
      activityWorkerFor(backend, registry, "activity-worker-before-expiry", 5)
        .runActivityTaskOnce()
    ).resolves.toEqual({ kind: "NoTask" });

    now += 6;
    await expect(
      backend.completeActivity({
        claim: crashedActivityClaim!.claim,
        result: encodePayload({ value: 22 }, { codec: "Json" })
      })
    ).rejects.toThrow("stale activity task lease");
    await expect(
      activityWorkerFor(backend, registry, "activity-worker-after-expiry").runActivityTaskOnce()
    ).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });

    const driver = new SimulationDriver(101, backend, registry, ["approved"]);
    let duplicateSignalSent = false;
    await driver.runUntilTerminal({
      runId: String(handle.runId),
      maxSteps: 500,
      beforeStep: async () => {
        now += 1;
        const eventTypes = await historyEventTypes(backend, String(handle.runId));
        if (!duplicateSignalSent && eventTypes.includes("ChildWorkflowCompleted")) {
          await client.sendSignal({
            workflowId: workflowKey,
            signal: approvalSignal,
            payload: { value: 121 },
            idempotencyKey: "approved-replay-fault-soak"
          });
          await client.sendSignal({
            workflowId: workflowKey,
            signal: approvalSignal,
            payload: { value: 121 },
            idempotencyKey: "approved-replay-fault-soak"
          });
          duplicateSignalSent = true;
          driver.record("duplicate signal approved-replay-fault-soak");
        }
      }
    });

    expect(duplicateSignalSent).toBe(true);
    await expect(handle.result()).resolves.toEqual({
      boot: 22,
      child: 221,
      approval: 121,
      finish: 343
    });
    const eventTypes = await historyEventTypes(backend, String(handle.runId));
    expect(eventTypes.at(0)).toBe("WorkflowStarted");
    expect(eventTypes.at(-1)).toBe("WorkflowCompleted");
    expect(countEvents(eventTypes, "SignalConsumed")).toBe(1);
    expect(countEvents(eventTypes, "TimerFired")).toBe(1);
    expect(eventTypes).toEqual(
      expect.arrayContaining([
        "ActivityScheduled",
        "ActivityCompleted",
        "ChildWorkflowStartRequested",
        "ChildWorkflowStarted",
        "ChildWorkflowCompleted",
        "SignalConsumed",
        "TimerStarted",
        "TimerFired",
        "WorkflowCompleted"
      ])
    );
  });

  it.each([1, 7, 19, 42])(
    "completes mixed activity/signal/timer/child interleavings for seed %i",
    async (seed) => {
      const backend = new MemoryBackend();
      const registry = simulationRegistry().registerWorkflow(mixedSimulationWorkflow);
      const client = new Client(backend, { payloadCodec: "Json" });
      const handle = await client.startWorkflow(
        mixedSimulationWorkflow,
        workflowId(`wf/simulation-mixed-${seed}`),
        WORKFLOW_QUEUE,
        { value: seed }
      );
      const driver = new SimulationDriver(seed, backend, registry, ["approved"]);
      const signalAt = 1 + (seed % 9);

      await driver.runUntilTerminal({
        runId: String(handle.runId),
        maxSteps: 250,
        beforeStep: async (step) => {
          if (step === signalAt) {
            await client.sendSignal({
              workflowId: workflowId(`wf/simulation-mixed-${seed}`),
              signal: approvalSignal,
              payload: { value: seed + 100 },
              idempotencyKey: `approved-${seed}`
            });
            driver.record(`signal approved-${seed}`);
          }
        }
      });

      await expect(handle.result()).resolves.toEqual({
        boot: seed + 1,
        child: (seed + 1) * 10 + 1,
        approval: seed + 100,
        finish: seed + 100 + ((seed + 1) * 10 + 1) + 1
      });
      const eventTypes = await historyEventTypes(backend, String(handle.runId));
      expect(eventTypes).toEqual(
        expect.arrayContaining([
          "ActivityScheduled",
          "ActivityCompleted",
          "ChildWorkflowStartRequested",
          "ChildWorkflowStarted",
          "ChildWorkflowCompleted",
          "SignalConsumed",
          "TimerStarted",
          "TimerFired",
          "WorkflowCompleted"
        ])
      );
    }
  );

  it.each([3, 11, 29])(
    "completes activity-map bounded materialization for seed %i",
    async (seed) => {
      const backend = new MemoryBackend();
      const registry = simulationRegistry().registerWorkflow(activityMapSimulationWorkflow);
      const client = new Client(backend, { payloadCodec: "Json" });
      const handle = await client.startWorkflow(
        activityMapSimulationWorkflow,
        workflowId(`wf/simulation-activity-map-${seed}`),
        WORKFLOW_QUEUE,
        {}
      );
      const driver = new SimulationDriver(seed, backend, registry, []);

      await driver.runUntilTerminal({ runId: String(handle.runId), maxSteps: 250 });

      await expect(handle.result()).resolves.toEqual({ sum: 20 });
      const eventTypes = await historyEventTypes(backend, String(handle.runId));
      expect(eventTypes).toEqual([
        "WorkflowStarted",
        "ActivityMapScheduled",
        "ActivityMapCompleted",
        "WorkflowCompleted"
      ]);
    }
  );

  it.each([5, 13, 31])("completes compact child-map fanout for seed %i", async (seed) => {
    const backend = new MemoryBackend();
    const registry = simulationRegistry().registerWorkflow(childMapSimulationWorkflow);
    const client = new Client(backend, { payloadCodec: "Json" });
    const handle = await client.startWorkflow(
      childMapSimulationWorkflow,
      workflowId(`wf/simulation-child-map-${seed}`),
      WORKFLOW_QUEUE,
      {}
    );
    const driver = new SimulationDriver(seed, backend, registry, []);

    await driver.runUntilTerminal({ runId: String(handle.runId), maxSteps: 300 });

    await expect(handle.result()).resolves.toEqual({ values: [11, 21, 31, 41] });
    const eventTypes = await historyEventTypes(backend, String(handle.runId));
    expect(eventTypes).toEqual([
      "WorkflowStarted",
      "ChildWorkflowMapScheduled",
      "ChildWorkflowMapCompleted",
      "WorkflowCompleted"
    ]);
  });

  it("survives a cache-eviction replay soak across concurrent mixed workflows", async () => {
    const inner = new MemoryBackend();
    const streamRequests: Parameters<DurableBackend["streamHistory"]>[0][] = [];
    const backend = truncateWorkflowClaimPrefetch(inner, 1, streamRequests);
    const registry = simulationRegistry().registerWorkflow(mixedSimulationWorkflow);
    const client = new Client(backend, { payloadCodec: "Json" });
    const inputs = [2, 4, 6, 8] as const;
    const handles = await Promise.all(
      inputs.map(async (value) => ({
        value,
        handle: await client.startWorkflow(
          mixedSimulationWorkflow,
          workflowId(`wf/simulation-cache-soak-${value}`),
          WORKFLOW_QUEUE,
          { value }
        )
      }))
    );
    const driver = new SimulationDriver(211, backend, registry, ["approved"], {
      workflowWorkerCount: 1,
      activityWorkerCount: 2,
      workflowHistoryCacheSize: 2,
      historyFetchMaxEvents: 1
    });
    const signaled = new Set<string>();

    await driver.runUntilAllTerminal({
      runIds: handles.map(({ handle }) => String(handle.runId)),
      maxSteps: 2_000,
      beforeStep: async () => {
        for (const { value, handle } of handles) {
          const runId = String(handle.runId);
          if (signaled.has(runId)) {
            continue;
          }
          const eventTypes = await historyEventTypes(inner, runId);
          if (!eventTypes.includes("ChildWorkflowCompleted")) {
            continue;
          }
          const workflowKey = workflowId(`wf/simulation-cache-soak-${value}`);
          await client.sendSignal({
            workflowId: workflowKey,
            signal: approvalSignal,
            payload: { value: value + 200 },
            idempotencyKey: `approved-cache-soak-${value}`
          });
          await client.sendSignal({
            workflowId: workflowKey,
            signal: approvalSignal,
            payload: { value: value + 200 },
            idempotencyKey: `approved-cache-soak-${value}`
          });
          signaled.add(runId);
          driver.record(`duplicate signal approved-cache-soak-${value}`);
        }
      }
    });

    expect(signaled.size).toBe(inputs.length);
    for (const { value, handle } of handles) {
      await expect(handle.result()).resolves.toEqual({
        boot: value + 1,
        child: (value + 1) * 10 + 1,
        approval: value + 200,
        finish: value + 200 + ((value + 1) * 10 + 1) + 1
      });
      const eventTypes = await historyEventTypes(inner, String(handle.runId));
      expect(eventTypes.at(0)).toBe("WorkflowStarted");
      expect(eventTypes.at(-1)).toBe("WorkflowCompleted");
      expect(countEvents(eventTypes, "SignalConsumed")).toBe(1);
      expect(countEvents(eventTypes, "TimerFired")).toBe(1);
    }

    const metrics = driver.metrics();
    expect(metrics.workflowHistoryCacheEvictions).toBeGreaterThan(0);
    expect(metrics.workflowHistoryCacheMisses).toBeGreaterThan(0);
    expect(metrics.historyStreamChunks).toBeGreaterThan(0);
    expect(metrics.historyStreamEvents).toBeGreaterThan(0);
    expect(streamRequests.length).toBeGreaterThanOrEqual(metrics.historyStreamChunks);
  });

  it("survives multi-seed hot execution cache fault soaks with restarts, conflicts, evictions, and duplicate signals", async () => {
    const metrics = emptyWorkerMetricsSnapshot();
    let streamRequestCount = 0;
    for (const scenarioSeed of [307, 409, 613]) {
      const result = await runHotExecutionCacheSoakScenario(scenarioSeed);
      addWorkerMetrics(metrics, result.metrics);
      streamRequestCount += result.streamRequestCount;
    }
    expect(metrics.workflowExecutionCacheHits).toBeGreaterThan(0);
    expect(metrics.workflowExecutionCacheMisses).toBeGreaterThan(0);
    expect(metrics.workflowExecutionCacheEvictions).toBeGreaterThan(0);
    expect(metrics.workflowTaskConflicts).toBeGreaterThan(0);
    expect(metrics.workflowHistoryCacheMisses).toBeGreaterThan(0);
    expect(metrics.historyStreamChunks).toBeGreaterThan(0);
    expect(metrics.historyStreamEvents).toBeGreaterThan(0);
    expect(streamRequestCount).toBeGreaterThanOrEqual(metrics.historyStreamChunks);
  });
});

const describeLongSoak = LONG_SOAK_ENABLED ? describe : describe.skip;

describeLongSoak("long-running hot execution cache soak", () => {
  it("survives an extended deterministic crash/restart/fault matrix", async () => {
    const metrics = emptyWorkerMetricsSnapshot();
    let streamRequestCount = 0;
    for (let index = 0; index < LONG_SOAK_SEEDS; index += 1) {
      const result = await runHotExecutionCacheSoakScenario(LONG_SOAK_SEED_BASE + index * 101, {
        workflowCount: LONG_SOAK_WORKFLOWS,
        driverGenerations: LONG_SOAK_GENERATIONS,
        warmStepsPerGeneration: LONG_SOAK_STEPS,
        maxFinalSteps: LONG_SOAK_FINAL_STEPS,
        conflicts: LONG_SOAK_CONFLICTS
      });
      addWorkerMetrics(metrics, result.metrics);
      streamRequestCount += result.streamRequestCount;
    }

    expect(metrics.workflowExecutionCacheHits).toBeGreaterThanOrEqual(LONG_SOAK_SEEDS);
    expect(metrics.workflowExecutionCacheMisses).toBeGreaterThanOrEqual(LONG_SOAK_SEEDS);
    expect(metrics.workflowExecutionCacheEvictions).toBeGreaterThanOrEqual(LONG_SOAK_SEEDS);
    expect(metrics.workflowTaskConflicts).toBeGreaterThanOrEqual(LONG_SOAK_SEEDS);
    expect(metrics.workflowHistoryCacheMisses).toBeGreaterThanOrEqual(LONG_SOAK_SEEDS);
    expect(metrics.historyStreamChunks).toBeGreaterThanOrEqual(LONG_SOAK_SEEDS);
    expect(metrics.historyStreamEvents).toBeGreaterThanOrEqual(LONG_SOAK_SEEDS);
    expect(streamRequestCount).toBeGreaterThanOrEqual(metrics.historyStreamChunks);
  }, 120_000);
});

async function runHotExecutionCacheSoakScenario(
  scenarioSeed: number,
  options: HotExecutionCacheSoakOptions = {}
): Promise<{
  readonly metrics: WorkerMetricsSnapshot;
  readonly streamRequestCount: number;
}> {
  let now = 50_000 + scenarioSeed;
  const inner = new MemoryBackend({ nowMs: () => now });
  let conflictsRemaining = options.conflicts ?? 6;
  const conflicted = conflictWorkflowCompletions(
    inner,
    () => conflictsRemaining-- > 0
  );
  const streamRequests: Parameters<DurableBackend["streamHistory"]>[0][] = [];
  const backend = truncateWorkflowClaimPrefetch(conflicted, 1, streamRequests);
  const registry = simulationRegistry().registerWorkflow(mixedSimulationWorkflow);
  const client = new Client(backend, { payloadCodec: "Json" });
  const inputs = Array.from({ length: options.workflowCount ?? 8 }, (_, index) => scenarioSeed + index * 2);
  const handles = await Promise.all(
    inputs.map(async (value) => ({
      value,
      handle: await client.startWorkflow(
        mixedSimulationWorkflow,
        workflowId(`wf/simulation-hot-cache-soak-${scenarioSeed}-${value}`),
        WORKFLOW_QUEUE,
        { value }
      )
    }))
  );
  const signaled = new Set<string>();
  const beforeStep = (driver: SimulationDriver) => async () => {
    now += 2;
    for (const { value, handle } of handles) {
      const runId = String(handle.runId);
      if (signaled.has(runId)) {
        continue;
      }
      const eventTypes = await historyEventTypes(inner, runId);
      if (!eventTypes.includes("ChildWorkflowCompleted")) {
        continue;
      }
      const workflowKey = workflowId(`wf/simulation-hot-cache-soak-${scenarioSeed}-${value}`);
      await client.sendSignal({
        workflowId: workflowKey,
        signal: approvalSignal,
        payload: { value: value + 300 },
        idempotencyKey: `approved-hot-cache-soak-${scenarioSeed}-${value}`
      });
      await client.sendSignal({
        workflowId: workflowKey,
        signal: approvalSignal,
        payload: { value: value + 300 },
        idempotencyKey: `approved-hot-cache-soak-${scenarioSeed}-${value}`
      });
      signaled.add(runId);
      driver.record(`duplicate signal approved-hot-cache-soak-${scenarioSeed}-${value}`);
    }
  };
  const driverOptions = {
    workflowWorkerCount: 2,
    activityWorkerCount: 2,
    workflowHistoryCacheSize: 2,
    workflowExecutionCacheSize: 2,
    historyFetchMaxEvents: 1,
    leaseDurationMs: 1
  };
  const driverGenerations = options.driverGenerations ?? 3;
  const drivers = Array.from(
    { length: driverGenerations },
    (_, index) => new SimulationDriver(scenarioSeed + index * 1_000, backend, registry, ["approved"], driverOptions)
  );
  for (let index = 0; index < drivers.length - 1; index += 1) {
    const driver = drivers[index]!;
    await driver.runSteps({
      steps: options.warmStepsPerGeneration ?? 160,
      beforeStep: beforeStep(driver)
    });
  }
  const finalDriver = drivers.at(-1)!;
  await finalDriver.runUntilAllTerminal({
    runIds: handles.map(({ handle }) => String(handle.runId)),
    maxSteps: options.maxFinalSteps ?? 8_000,
    beforeStep: beforeStep(finalDriver)
  });

  expect(signaled.size).toBe(inputs.length);
  for (const { value, handle } of handles) {
    await expect(handle.result()).resolves.toEqual({
      boot: value + 1,
      child: (value + 1) * 10 + 1,
      approval: value + 300,
      finish: value + 300 + ((value + 1) * 10 + 1) + 1
    });
    const eventTypes = await historyEventTypes(inner, String(handle.runId));
    expect(eventTypes.at(0)).toBe("WorkflowStarted");
    expect(eventTypes.at(-1)).toBe("WorkflowCompleted");
    expect(countEvents(eventTypes, "WorkflowCompleted")).toBe(1);
    expect(countEvents(eventTypes, "ActivityScheduled")).toBe(2);
    expect(countEvents(eventTypes, "ActivityCompleted")).toBe(2);
    expect(countEvents(eventTypes, "ChildWorkflowStartRequested")).toBe(1);
    expect(countEvents(eventTypes, "ChildWorkflowStarted")).toBe(1);
    expect(countEvents(eventTypes, "ChildWorkflowCompleted")).toBe(1);
    expect(countEvents(eventTypes, "SignalConsumed")).toBe(1);
    expect(countEvents(eventTypes, "TimerStarted")).toBe(1);
    expect(countEvents(eventTypes, "TimerFired")).toBe(1);
  }

  const metrics = emptyWorkerMetricsSnapshot();
  for (const driver of drivers) {
    addWorkerMetrics(metrics, driver.metrics());
  }
  return { metrics, streamRequestCount: streamRequests.length };
}

interface HotExecutionCacheSoakOptions {
  readonly workflowCount?: number;
  readonly driverGenerations?: number;
  readonly warmStepsPerGeneration?: number;
  readonly maxFinalSteps?: number;
  readonly conflicts?: number;
}

function simulationRegistry(): Registry {
  return new Registry()
    .registerWorkflow(simChildWorkflow)
    .registerActivity(simActivity);
}

function workerFor(
  backend: DurableBackend,
  registry: Registry,
  workerId: string,
  signalNames: readonly string[] = [],
  leaseDurationMs = 30_000
): Worker {
  return new Worker({
    backend,
    registry,
    workerId,
    workflowTaskQueue: WORKFLOW_QUEUE,
    activityTaskQueue: ACTIVITY_QUEUE,
    registeredSignalNames: signalNames,
    leaseDurationMs,
    payloadCodec: "Json"
  });
}

function activityWorkerFor(
  backend: DurableBackend,
  registry: Registry,
  workerId: string,
  leaseDurationMs = 30_000
): Worker {
  return new Worker({
    backend,
    registry,
    workerId,
    workflowTaskQueue: WORKFLOW_QUEUE,
    activityTaskQueue: ACTIVITY_QUEUE,
    leaseDurationMs,
    payloadCodec: "Json"
  });
}

class SimulationDriver {
  readonly #rng: SeededRng;
  readonly #trace: string[] = [];
  readonly #workflowWorkers: Worker[];
  readonly #activityWorkers: Worker[];

  constructor(
    readonly seed: number,
    readonly backend: DurableBackend,
    readonly registry: Registry,
    signalNames: readonly string[],
    options: SimulationDriverOptions = {}
  ) {
    this.#rng = new SeededRng(seed);
    this.#workflowWorkers = Array.from({ length: options.workflowWorkerCount ?? 3 }, (_, index) => {
      const workerOptions = workerOptionsForSimulation({
        backend,
        registry,
        workerId: `simulation-workflow-worker-${seed}-${index}`,
        signalNames,
        workflowHistoryCacheSize: options.workflowHistoryCacheSize,
        workflowExecutionCacheSize: options.workflowExecutionCacheSize,
        historyFetchMaxEvents: options.historyFetchMaxEvents,
        leaseDurationMs: options.leaseDurationMs
      });
      return new Worker(workerOptions);
    });
    this.#activityWorkers = Array.from({ length: options.activityWorkerCount ?? 2 }, (_, index) => {
      const workerOptions = workerOptionsForSimulation({
        backend,
        registry,
        workerId: `simulation-activity-worker-${seed}-${index}`,
        signalNames,
        workflowHistoryCacheSize: options.workflowHistoryCacheSize,
        workflowExecutionCacheSize: options.workflowExecutionCacheSize,
        historyFetchMaxEvents: options.historyFetchMaxEvents,
        leaseDurationMs: options.leaseDurationMs
      });
      return new Worker(workerOptions);
    });
  }

  record(action: string): void {
    this.#trace.push(action);
  }

  async runUntilTerminal(options: {
    readonly runId: string;
    readonly maxSteps: number;
    readonly beforeStep?: (step: number) => Promise<void>;
  }): Promise<void> {
    for (let step = 0; step < options.maxSteps; step += 1) {
      await options.beforeStep?.(step);
      await this.#runRandomAction(step);
      if (await isWorkflowTerminal(this.backend, options.runId)) {
        return;
      }
    }
    throw new Error(
      `simulation seed ${this.seed} did not terminate after ${options.maxSteps} steps\n${this.#trace.join("\n")}`
    );
  }

  async runUntilAllTerminal(options: {
    readonly runIds: readonly string[];
    readonly maxSteps: number;
    readonly beforeStep?: (step: number) => Promise<void>;
  }): Promise<void> {
    for (let step = 0; step < options.maxSteps; step += 1) {
      await options.beforeStep?.(step);
      await this.#runRandomAction(step);
      const terminal = await Promise.all(
        options.runIds.map(async (runId) => await isWorkflowTerminal(this.backend, runId))
      );
      if (terminal.every(Boolean)) {
        return;
      }
    }
    throw new Error(
      `simulation seed ${this.seed} did not terminate all workflows after ${options.maxSteps} steps\n${this.#trace.join("\n")}`
    );
  }

  async runSteps(options: {
    readonly steps: number;
    readonly beforeStep?: (step: number) => Promise<void>;
  }): Promise<void> {
    for (let step = 0; step < options.steps; step += 1) {
      await options.beforeStep?.(step);
      await this.#runRandomAction(step);
    }
  }

  metrics(): WorkerMetricsSnapshot {
    const metrics = emptyWorkerMetricsSnapshot();
    for (const worker of [...this.#workflowWorkers, ...this.#activityWorkers]) {
      addWorkerMetrics(metrics, worker.metrics());
    }
    return metrics;
  }

  async #runRandomAction(step: number): Promise<void> {
    const action = this.#rng.nextInt(3);
    if (action === 0) {
      const worker = this.#workflowWorkers[this.#rng.nextInt(this.#workflowWorkers.length)];
      const outcome = await worker.runWorkflowTaskOnce();
      this.record(`${step}: workflow ${outcome.kind}`);
      return;
    }
    if (action === 1) {
      const worker = this.#activityWorkers[this.#rng.nextInt(this.#activityWorkers.length)];
      const outcome = await worker.runActivityTaskOnce();
      this.record(`${step}: activity ${outcome.kind}`);
      return;
    }
    const fired = await this.backend.fireDueTimers({
      namespace: "default",
      now: timestampMs(1_000_000 + this.seed * 1_000 + step * 100),
      limit: 16
    });
    this.record(`${step}: timers ${fired.fired}`);
  }
}

interface SimulationDriverOptions {
  readonly workflowWorkerCount?: number;
  readonly activityWorkerCount?: number;
  readonly workflowHistoryCacheSize?: number;
  readonly workflowExecutionCacheSize?: number;
  readonly historyFetchMaxEvents?: number;
  readonly leaseDurationMs?: number;
}

type MutableWorkerMetricsSnapshot = {
  -readonly [Key in keyof WorkerMetricsSnapshot]: WorkerMetricsSnapshot[Key];
};

const WORKER_METRIC_KEYS = [
  "workflowTaskClaims",
  "workflowTaskNoTasks",
  "workflowTaskCommits",
  "workflowTaskConflicts",
  "activityTaskClaims",
  "activityTaskNoTasks",
  "activityTaskCompletions",
  "activityTaskFailures",
  "activityCompletionBatches",
  "activityCompletionBatchItems",
  "workflowHistoryCacheHits",
  "workflowHistoryCacheMisses",
  "workflowHistoryCacheEvictions",
  "workflowExecutionCacheHits",
  "workflowExecutionCacheMisses",
  "workflowExecutionCacheEvictions",
  "historyStreamChunks",
  "historyStreamEvents",
  "timersFired",
  "loopErrors",
  "idleSleeps",
  "eventSinkErrors"
] as const satisfies readonly (keyof WorkerMetricsSnapshot)[];

function workerOptionsForSimulation(options: {
  readonly backend: DurableBackend;
  readonly registry: Registry;
  readonly workerId: string;
  readonly signalNames: readonly string[];
  readonly workflowHistoryCacheSize?: number;
  readonly workflowExecutionCacheSize?: number;
  readonly historyFetchMaxEvents?: number;
  readonly leaseDurationMs?: number;
}): ConstructorParameters<typeof Worker>[0] {
  return {
    backend: options.backend,
    registry: options.registry,
    workerId: options.workerId,
    workflowTaskQueue: WORKFLOW_QUEUE,
    activityTaskQueue: ACTIVITY_QUEUE,
    registeredSignalNames: options.signalNames,
    payloadCodec: "Json",
    ...(options.leaseDurationMs === undefined
      ? {}
      : { leaseDurationMs: options.leaseDurationMs }),
    ...(options.workflowHistoryCacheSize === undefined
      ? {}
      : { workflowHistoryCacheSize: options.workflowHistoryCacheSize }),
    ...(options.workflowExecutionCacheSize === undefined
      ? {}
      : { workflowExecutionCacheSize: options.workflowExecutionCacheSize }),
    ...(options.historyFetchMaxEvents === undefined
      ? {}
      : { historyFetchMaxEvents: options.historyFetchMaxEvents })
  };
}

function emptyWorkerMetricsSnapshot(): MutableWorkerMetricsSnapshot {
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

function addWorkerMetrics(
  target: MutableWorkerMetricsSnapshot,
  source: WorkerMetricsSnapshot
): void {
  for (const key of WORKER_METRIC_KEYS) {
    target[key] += source[key];
  }
}

function truncateWorkflowClaimPrefetch(
  inner: DurableBackend,
  keepEvents: number,
  streamRequests: Parameters<DurableBackend["streamHistory"]>[0][]
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "claimWorkflowTask") {
        return async (...args: Parameters<DurableBackend["claimWorkflowTask"]>) => {
          const claimed = await target.claimWorkflowTask(...args);
          return claimed === null
            ? null
            : {
                ...claimed,
                prefetchedHistory: claimed.prefetchedHistory.slice(0, keepEvents)
              };
        };
      }
      if (property === "streamHistory") {
        return async (...args: Parameters<DurableBackend["streamHistory"]>) => {
          streamRequests.push(args[0]);
          return await target.streamHistory(...args);
        };
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
}

function conflictWorkflowCompletions(
  inner: DurableBackend,
  shouldConflict: () => boolean
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "commitWorkflowTask") {
        return async (...args: Parameters<DurableBackend["commitWorkflowTask"]>) => {
          const commit = args[1];
          const completing = (commit.appendEvents ?? []).some(
            (event) => event.data.kind === "WorkflowCompleted"
          );
          if (completing && shouldConflict()) {
            return { kind: "Conflict" };
          }
          return await target.commitWorkflowTask(...args);
        };
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
}

class SeededRng {
  #state: number;

  constructor(seed: number) {
    this.#state = seed >>> 0;
  }

  nextInt(exclusiveMax: number): number {
    this.#state = (Math.imul(this.#state, 1664525) + 1013904223) >>> 0;
    return this.#state % exclusiveMax;
  }
}

async function isWorkflowTerminal(backend: DurableBackend, runId: string): Promise<boolean> {
  const events = await historyEventTypes(backend, runId);
  const last = events.at(-1);
  return last === "WorkflowCompleted" || last === "WorkflowFailed" || last === "WorkflowCancelled";
}

async function historyEventTypes(backend: DurableBackend, runId: string): Promise<readonly string[]> {
  const history = await backend.streamHistory({
    runId,
    afterEventId: eventId(0),
    upToEventId: eventId(Number.MAX_SAFE_INTEGER),
    maxEvents: 10_000,
    maxBytes: Number.MAX_SAFE_INTEGER
  });
  return history.events.map((event) => event.eventType);
}

function countEvents(eventTypes: readonly string[], eventType: string): number {
  return eventTypes.filter((value) => value === eventType).length;
}

function positiveIntEnv(name: string, defaultValue: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.length === 0) {
    return defaultValue;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}
