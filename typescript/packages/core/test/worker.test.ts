import { describe, expect, it } from "vitest";
import {
  Client,
  MemoryBackend,
  ActivityFailureError,
  ChildWorkflowFailureError,
  ChildWorkflowMapFailureError,
  Registry,
  WorkflowFailureError,
  Worker,
  activity,
  activityMap,
  activityMapManifest,
  childWorkflowMap,
  childWorkflow,
  callActivity,
  decodeActivityMapResults,
  decodeChildWorkflowMapSuccesses,
  eventId,
  namespace,
  runId,
  signal,
  sleep,
  workflow,
  workflowId,
  type DurableBackend,
  type WorkerEvent
} from "@durust/core";

interface EchoInput {
  readonly value: string;
}

interface EchoOutput {
  readonly value: string;
}

const echoWorkflow = workflow({
  name: "worker.echo",
  version: 1,
  handler: async (input: EchoInput): Promise<EchoOutput> => input
});

const quoteActivity = activity({
  name: "worker.quote",
  handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => ({
    cents: input.sku.length
  })
});

const quoteWorkflow = workflow({
  name: "worker.quote-workflow",
  version: 1,
  handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
    const quote = await callActivity(quoteActivity, { sku: input.sku }, { taskQueue: "activities" });
    return { cents: quote.cents };
  }
});

const twoQuoteWorkflow = workflow({
  name: "worker.two-quotes",
  version: 1,
  handler: async (input: { readonly first: string; readonly second: string }): Promise<{ readonly cents: number }> => {
    const first = await callActivity(
      quoteActivity,
      { sku: input.first },
      { taskQueue: "activities" }
    ).spawn();
    const second = await callActivity(
      quoteActivity,
      { sku: input.second },
      { taskQueue: "activities" }
    ).spawn();
    return {
      cents: (await first.result()).cents + (await second.result()).cents
    };
  }
});

const quoteMapWorkflow = workflow({
  name: "worker.quote-map",
  version: 1,
  handler: async (_input: {}): Promise<{ readonly totalCents: number }> => {
    const mapped = activityMap(quoteActivity, {
      inputManifest: activityMapManifest(
        [{ sku: "a" }, { sku: "abcd" }, { sku: "xy" }],
        2
      ),
      resultManifest: "quotes",
      taskQueue: "activities",
      maxInFlight: 2
    });
    const manifestRef = await mapped.resultManifest();
    const totalCents = decodeActivityMapResults(manifestRef).reduce(
      (sum, result) => sum + result.cents,
      0
    );
    return { totalCents };
  }
});

const failingActivity = activity({
  name: "worker.fail",
  handler: async (_input: {}): Promise<{ readonly ok: true }> => {
    throw new Error("activity exploded");
  }
});

const failingMapWorkflow = workflow({
  name: "worker.failing-map",
  version: 1,
  handler: async (_input: {}): Promise<{ readonly failure: string }> => {
    try {
      const mapped = activityMap(failingActivity, {
        inputManifest: activityMapManifest([{}]),
        resultManifest: "failures",
        taskQueue: "activities",
        maxInFlight: 1
      });
      await mapped.resultManifest();
      return { failure: "none" };
    } catch (error) {
      if (error instanceof ActivityFailureError) {
        return { failure: error.failure.message };
      }
      throw error;
    }
  }
});

const catchesFailureWorkflow = workflow({
  name: "worker.catches-failure",
  version: 1,
  handler: async (_input: {}): Promise<{ readonly failure: string }> => {
    try {
      await callActivity(failingActivity, {}, { taskQueue: "activities" });
      return { failure: "none" };
    } catch (error) {
      if (error instanceof ActivityFailureError) {
        return { failure: error.failure.message };
      }
      throw error;
    }
  }
});

const childEchoWorkflow = workflow({
  name: "worker.child-echo",
  version: 1,
  handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> => ({
    value: `${input.value}/child`
  })
});

const throwsWorkflow = workflow({
  name: "worker.throws",
  version: 1,
  handler: async (_input: {}): Promise<void> => {
    throw new Error("workflow exploded");
  }
});

const parentWorkflow = workflow({
  name: "worker.parent",
  version: 1,
  handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> => {
    const child = await childWorkflow(
      childEchoWorkflow,
      { value: input.value },
      { workflowId: `child/${input.value}`, taskQueue: "workflows" }
    ).spawn();
    const result = await child.result();
    return { value: result.value };
  }
});

const parentCancelWorkflow = workflow({
  name: "worker.parent-cancel",
  version: 1,
  handler: async (input: { readonly value: string }): Promise<{ readonly childRunId: string }> => {
    const child = await childWorkflow(
      childEchoWorkflow,
      { value: input.value },
      {
        workflowId: `cancel/${input.value}`,
        taskQueue: "workflows",
        parentClosePolicy: "Cancel"
      }
    ).spawn();
    return { childRunId: String(child.runId) };
  }
});

const parentAbandonWorkflow = workflow({
  name: "worker.parent-abandon",
  version: 1,
  handler: async (input: { readonly value: string }): Promise<{ readonly childRunId: string }> => {
    const child = await childWorkflow(
      childEchoWorkflow,
      { value: input.value },
      {
        workflowId: `abandon/${input.value}`,
        taskQueue: "workflows",
        parentClosePolicy: "Abandon"
      }
    ).spawn();
    return { childRunId: String(child.runId) };
  }
});

const childConflictParentWorkflow = workflow({
  name: "worker.child-conflict-parent",
  version: 1,
  handler: async (_input: {}): Promise<{ readonly errorType: string }> => {
    try {
      await childWorkflow(
        childEchoWorkflow,
        { value: "conflict" },
        { workflowId: "child/conflict", taskQueue: "workflows" }
      ).spawn();
      return { errorType: "none" };
    } catch (error) {
      if (error instanceof ChildWorkflowFailureError) {
        return { errorType: error.failure.errorType };
      }
      throw error;
    }
  }
});

const childMapWorkflow = workflow({
  name: "worker.child-map",
  version: 1,
  handler: async (_input: {}): Promise<{ readonly values: readonly string[] }> => {
    const mapped = childWorkflowMap(childEchoWorkflow, {
      inputManifest: activityMapManifest(
        [{ value: "a" }, { value: "b" }, { value: "c" }],
        2
      ),
      resultManifest: "child-values",
      workflowIdPrefix: "child-map/success",
      taskQueue: "workflows",
      maxInFlight: 2
    });
    const manifestRef = await mapped.resultManifest();
    return {
      values: decodeChildWorkflowMapSuccesses(manifestRef).map((result) => result.value)
    };
  }
});

const childMapConflictWorkflow = workflow({
  name: "worker.child-map-conflict",
  version: 1,
  handler: async (_input: {}): Promise<{ readonly errorType: string }> => {
    try {
      const mapped = childWorkflowMap(childEchoWorkflow, {
        inputManifest: activityMapManifest([{ value: "conflict" }]),
        resultManifest: "child-conflict",
        workflowIdPrefix: "child-map/conflict",
        taskQueue: "workflows",
        maxInFlight: 1
      });
      await mapped.resultManifest();
      return { errorType: "none" };
    } catch (error) {
      if (error instanceof ChildWorkflowMapFailureError) {
        return { errorType: error.failure.errorType };
      }
      throw error;
    }
  }
});

const approvalSignal = signal<{ readonly value: string }>("approved");

const signalWorkflow = workflow({
  name: "worker.signal",
  version: 1,
  handler: async (_input: {}): Promise<{ readonly value: string }> => {
    return approvalSignal;
  }
});

const timerWorkflow = workflow({
  name: "worker.timer",
  version: 1,
  handler: async (_input: {}): Promise<{ readonly fired: true }> => {
    await sleep(0);
    return { fired: true };
  }
});

describe("Worker", () => {
  it("claims and commits one immediate workflow task", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(echoWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(echoWorkflow, workflowId("wf/worker-echo"), "workflows", {
      value: "ok"
    });

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ value: "ok" });
    await expect(worker.runWorkflowTaskOnce()).resolves.toEqual({ kind: "NoTask" });
  });

  it("runs workflow and activity polling in a stoppable loop", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(quoteWorkflow).registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-loop-quote"),
      "workflows",
      { sku: "sku-1" }
    );

    const outcome = await worker.run({
      maxIterations: 8,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    });

    expect(outcome).toMatchObject({
      stopReason: "maxIterations",
      errors: 0,
      activityTasks: 1
    });
    expect(outcome.workflowTasks).toBeGreaterThanOrEqual(2);
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("records structured events and cumulative worker metrics", async () => {
    const backend = new MemoryBackend();
    const events: WorkerEvent[] = [];
    const registry = new Registry().registerWorkflow(quoteWorkflow).registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      activityCompletionBatchSize: 2,
      payloadCodec: "Json",
      onEvent: (event) => {
        events.push(event);
      }
    });
    const handle = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-metrics"),
      "workflows",
      { sku: "sku-1" }
    );

    await worker.run({
      maxIterations: 8,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    });

    await expect(handle.result()).resolves.toEqual({ cents: 5 });
    expect(events.map((event) => event.kind)).toEqual(
      expect.arrayContaining([
        "WorkflowTaskClaimed",
        "WorkflowTaskCommitted",
        "ActivityTaskClaimed",
        "ActivityCompletionBatchFlushed"
      ])
    );
    expect(worker.metrics()).toMatchObject({
      workflowTaskClaims: 2,
      workflowTaskCommits: 2,
      activityTaskClaims: 1,
      activityTaskCompletions: 1,
      activityCompletionBatches: 1,
      activityCompletionBatchItems: 1,
      loopErrors: 0,
      eventSinkErrors: 0
    });
  });

  it("isolates event sink failures from durable processing", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(echoWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json",
      onEvent: () => {
        throw new Error("metrics sink failed");
      }
    });
    const handle = await client.startWorkflow(
      echoWorkflow,
      workflowId("wf/worker-event-sink-failure"),
      "workflows",
      { value: "ok" }
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ value: "ok" });
    expect(worker.metrics()).toMatchObject({
      workflowTaskClaims: 1,
      workflowTaskCommits: 1,
      eventSinkErrors: 2
    });
  });

  it("batches successful activity completions from the worker loop", async () => {
    const inner = new MemoryBackend();
    const batchSizes: number[] = [];
    const backend = recordActivityCompletionBatches(inner, batchSizes);
    const registry = new Registry()
      .registerWorkflow(twoQuoteWorkflow)
      .registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      activityCompletionBatchSize: 2,
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      twoQuoteWorkflow,
      workflowId("wf/worker-batch-activity-completions"),
      "workflows",
      { first: "aa", second: "bbbb" }
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    const outcome = await worker.run({
      maxIterations: 4,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    });

    expect(batchSizes).toEqual([2]);
    expect(outcome.activityTasks).toBe(2);
    expect(outcome.workflowTasks).toBeGreaterThanOrEqual(1);
    await expect(handle.result()).resolves.toEqual({ cents: 6 });
  });

  it("runs due timer maintenance from the worker loop", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(timerWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      timerWorkflow,
      workflowId("wf/worker-loop-timer"),
      "workflows",
      {}
    );

    const outcome = await worker.run({
      maxIterations: 6,
      idleBackoffMs: 0,
      timerMaintenanceLimit: 8
    });

    expect(outcome.timersFired).toBe(1);
    expect(outcome.workflowTasks).toBeGreaterThanOrEqual(2);
    await expect(handle.result()).resolves.toEqual({ fired: true });
  });

  it("stops the worker loop when aborted during idle backoff", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(echoWorkflow);
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 5);

    const outcome = await worker.run({
      signal: controller.signal,
      idleBackoffMs: 100,
      maxIdleBackoffMs: 100
    });

    expect(outcome.stopReason).toBe("abort");
    expect(outcome.idleSleeps).toBeGreaterThanOrEqual(1);
  });

  it("backs off and continues after a transient worker-loop error", async () => {
    const inner = new MemoryBackend();
    const backend = failFirstWorkflowClaim(inner);
    const registry = new Registry().registerWorkflow(echoWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      echoWorkflow,
      workflowId("wf/worker-loop-transient-error"),
      "workflows",
      { value: "ok" }
    );
    const errors: unknown[] = [];

    const outcome = await worker.run({
      maxIterations: 4,
      idleBackoffMs: 0,
      errorBackoffMs: 0,
      onError: (error) => {
        errors.push(error);
      }
    });

    expect(outcome.errors).toBe(1);
    expect(errors).toHaveLength(1);
    expect(outcome.workflowTasks).toBeGreaterThanOrEqual(1);
    await expect(handle.result()).resolves.toEqual({ value: "ok" });
  });

  it("hydrates registered signal payloads before polling workflow code", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(signalWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      registeredSignalNames: ["approved"],
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      signalWorkflow,
      workflowId("wf/worker-signal"),
      "workflows",
      {}
    );
    await client.sendSignal({
      workflowId: workflowId("wf/worker-signal"),
      signal: approvalSignal,
      payload: { value: "ok" },
      idempotencyKey: "approved-1"
    });

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ value: "ok" });
  });

  it("replays activity completion and commits workflow completion", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(quoteWorkflow).registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-quote"),
      "workflows",
      { sku: "sku-1" }
    );

    await worker.runWorkflowTaskOnce();
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
    await expect(worker.runActivityTaskOnce()).resolves.toEqual({ kind: "NoTask" });
  });

  it("prefers locally registered activities after a workflow task commit", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(quoteWorkflow).registerActivity(quoteActivity);
    const remoteRegistry = new Registry().registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const workflowWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "workflow-local-activity-worker",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      maxLocalActivitiesPerWorkflowTask: 1,
      payloadCodec: "Json"
    });
    const remoteWorker = new Worker({
      backend,
      registry: remoteRegistry,
      namespace: namespace(),
      workerId: "remote-activity-worker",
      workflowTaskQueue: "unused",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-local-activity"),
      "workflows",
      { sku: "sku-1" }
    );

    await expect(workflowWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" },
      localActivityTasks: 1
    });

    const historyAfterLocal = await backend.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(historyAfterLocal.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled",
      "ActivityCompleted"
    ]);
    await expect(remoteWorker.runActivityTaskOnce()).resolves.toEqual({ kind: "NoTask" });

    await expect(workflowWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("falls back to remote activity workers when local capacity is zero", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(quoteWorkflow).registerActivity(quoteActivity);
    const remoteRegistry = new Registry().registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const workflowWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "workflow-no-local-capacity",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      maxLocalActivitiesPerWorkflowTask: 0,
      payloadCodec: "Json"
    });
    const remoteWorker = new Worker({
      backend,
      registry: remoteRegistry,
      namespace: namespace(),
      workerId: "remote-fallback-worker",
      workflowTaskQueue: "unused",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-remote-fallback"),
      "workflows",
      { sku: "sku-2" }
    );

    await expect(workflowWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" },
      localActivityTasks: 0
    });
    const historyAfterSchedule = await backend.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(historyAfterSchedule.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled"
    ]);

    await expect(remoteWorker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(workflowWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("runs an activity map with bounded provider-owned item state", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(quoteMapWorkflow).registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      quoteMapWorkflow,
      workflowId("wf/worker-quote-map"),
      "workflows",
      {}
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ totalCents: 7 });

    const history = await backend.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityMapScheduled",
      "ActivityMapCompleted",
      "WorkflowCompleted"
    ]);
  });

  it("fails an activity map compactly when a map item fails", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(failingMapWorkflow).registerActivity(failingActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      failingMapWorkflow,
      workflowId("wf/worker-failing-map"),
      "workflows",
      {}
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Failed",
      outcome: { kind: "Failed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ failure: "activity exploded" });

    const history = await backend.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityMapScheduled",
      "ActivityMapFailed",
      "WorkflowCompleted"
    ]);
  });

  it("persists activity handler failures and replays them into workflow code", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(catchesFailureWorkflow)
      .registerActivity(failingActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      catchesFailureWorkflow,
      workflowId("wf/worker-failure"),
      "workflows",
      {}
    );

    await worker.runWorkflowTaskOnce();
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Failed",
      outcome: { kind: "Failed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ failure: "activity exploded" });
  });

  it("runs a child workflow and routes its result back to the parent", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(parentWorkflow)
      .registerWorkflow(childEchoWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      parentWorkflow,
      workflowId("wf/worker-parent"),
      "workflows",
      { value: "order-1" }
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ value: "order-1/child" });
  });

  it("persists uncaught workflow handler errors as workflow failures", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(throwsWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      throwsWorkflow,
      workflowId("wf/worker-throws"),
      "workflows",
      {}
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).rejects.toMatchObject({
      name: "WorkflowFailureError",
      failure: {
        errorType: "Error",
        message: "workflow exploded"
      }
    } satisfies Partial<WorkflowFailureError>);

    const history = await backend.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "WorkflowFailed"
    ]);
  });

  it("surfaces child workflow id conflicts to parent replay", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(childConflictParentWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    await client.startWorkflow(
      childEchoWorkflow,
      workflowId("child/conflict"),
      "other-workflows",
      { value: "already-running" }
    );
    const handle = await client.startWorkflow(
      childConflictParentWorkflow,
      workflowId("wf/worker-child-conflict-parent"),
      "workflows",
      {}
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({
      errorType: "durust.child_workflow_id_conflict"
    });
  });

  it("runs a child workflow map with compact parent history and ordered results", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(childMapWorkflow)
      .registerWorkflow(childEchoWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      childMapWorkflow,
      workflowId("wf/worker-child-map"),
      "workflows",
      {}
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({
      values: ["a/child", "b/child", "c/child"]
    });

    const history = await backend.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ChildWorkflowMapScheduled",
      "ChildWorkflowMapCompleted",
      "WorkflowCompleted"
    ]);
  });

  it("materializes child workflow map items up to maxInFlight", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(childMapWorkflow)
      .registerWorkflow(childEchoWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    await client.startWorkflow(
      childMapWorkflow,
      workflowId("wf/worker-child-map-bounded"),
      "workflows",
      {}
    );

    await worker.runWorkflowTaskOnce();

    const first = await backend.claimWorkflowTask("probe-a", {
      namespace: namespace(),
      taskQueue: "workflows",
      registeredWorkflowTypes: [childEchoWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    const second = await backend.claimWorkflowTask("probe-b", {
      namespace: namespace(),
      taskQueue: "workflows",
      registeredWorkflowTypes: [childEchoWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    const third = await backend.claimWorkflowTask("probe-c", {
      namespace: namespace(),
      taskQueue: "workflows",
      registeredWorkflowTypes: [childEchoWorkflow.workflowType],
      leaseDurationMs: 30_000
    });

    expect(first?.workflowId).toBe("child-map/success/0");
    expect(second?.workflowId).toBe("child-map/success/1");
    expect(third).toBeNull();
  });

  it("fails a child workflow map compactly when a child workflow id conflicts", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(childMapConflictWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    await client.startWorkflow(
      childEchoWorkflow,
      workflowId("child-map/conflict/0"),
      "other-workflows",
      { value: "already-running" }
    );
    const handle = await client.startWorkflow(
      childMapConflictWorkflow,
      workflowId("wf/worker-child-map-conflict"),
      "workflows",
      {}
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({
      errorType: "durust.child_workflow_id_conflict"
    });

    const history = await backend.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ChildWorkflowMapScheduled",
      "ChildWorkflowMapFailed",
      "WorkflowCompleted"
    ]);
  });

  it("cancels running children when the parent closes with Cancel policy", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(parentCancelWorkflow)
      .registerWorkflow(childEchoWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      parentCancelWorkflow,
      workflowId("wf/worker-parent-cancel"),
      "workflows",
      { value: "order-2" }
    );

    await worker.runWorkflowTaskOnce();
    await worker.runWorkflowTaskOnce();
    const result = await handle.result();
    await expect(worker.runWorkflowTaskOnce()).resolves.toEqual({ kind: "NoTask" });

    const childHistory = await backend.streamHistory({
      runId: runId(result.childRunId),
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(childHistory.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "WorkflowCancelled"
    ]);
  });

  it("leaves running children claimable when the parent closes with Abandon policy", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(parentAbandonWorkflow)
      .registerWorkflow(childEchoWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      parentAbandonWorkflow,
      workflowId("wf/worker-parent-abandon"),
      "workflows",
      { value: "order-3" }
    );

    await worker.runWorkflowTaskOnce();
    await worker.runWorkflowTaskOnce();
    const result = await handle.result();
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    const childHistory = await backend.streamHistory({
      runId: runId(result.childRunId),
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(childHistory.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "WorkflowCompleted"
    ]);
  });
});

function failFirstWorkflowClaim(inner: DurableBackend): DurableBackend {
  let failed = false;
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "claimWorkflowTask") {
        return async (...args: Parameters<DurableBackend["claimWorkflowTask"]>) => {
          if (!failed) {
            failed = true;
            throw new Error("transient claim failure");
          }
          return await target.claimWorkflowTask(...args);
        };
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
}

function recordActivityCompletionBatches(
  inner: DurableBackend,
  batchSizes: number[]
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "completeActivities") {
        return async (...args: Parameters<DurableBackend["completeActivities"]>) => {
          batchSizes.push(args[0].completions.length);
          return await target.completeActivities(...args);
        };
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
}
