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
  eventId,
  signal,
  sleep,
  timestampMs,
  workflow,
  workflowId,
  type DurableBackend
} from "@durust/core";

const WORKFLOW_QUEUE = "workflows";
const ACTIVITY_QUEUE = "activities";

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
});

function simulationRegistry(): Registry {
  return new Registry()
    .registerWorkflow(simChildWorkflow)
    .registerActivity(simActivity);
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
    signalNames: readonly string[]
  ) {
    this.#rng = new SeededRng(seed);
    this.#workflowWorkers = Array.from({ length: 3 }, (_, index) =>
      new Worker({
        backend,
        registry,
        workerId: `simulation-workflow-worker-${seed}-${index}`,
        workflowTaskQueue: WORKFLOW_QUEUE,
        activityTaskQueue: ACTIVITY_QUEUE,
        registeredSignalNames: signalNames,
        payloadCodec: "Json"
      })
    );
    this.#activityWorkers = Array.from({ length: 2 }, (_, index) =>
      new Worker({
        backend,
        registry,
        workerId: `simulation-activity-worker-${seed}-${index}`,
        workflowTaskQueue: WORKFLOW_QUEUE,
        activityTaskQueue: ACTIVITY_QUEUE,
        registeredSignalNames: signalNames,
        payloadCodec: "Json"
      })
    );
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
