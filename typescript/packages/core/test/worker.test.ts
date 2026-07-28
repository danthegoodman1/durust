import v8 from "node:v8";
import vm from "node:vm";
import { describe, expect, it, vi } from "vitest";
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
  joinAll,
  decodeActivityMapResults,
  decodeChildWorkflowMapSuccesses,
  eventId,
  getVersion,
  heartbeat,
  namespace,
  runId,
  signal,
  sleep,
  workflow,
  workflowId,
  type DurableBackend,
  type HistoryEvent,
  type WorkerEvent
} from "@durust/core";
import {
  HotWorkflowExecutionDisposedError,
  REPLAY_WINDOW_LOOKAHEAD_EVENTS
} from "../src/runtime.js";

/**
 * The shape a `toMatchObject` argument actually has.
 *
 * `toMatchObject` matches *deeply* and partially, so the literal handed to it
 * is a deep partial of the asserted type. `Partial<T>` loosens only the top
 * level: under it a nested literal is still checked against the whole nested
 * type, so `satisfies Partial<WorkflowFailureError>` demanded a complete
 * `DurableFailure` — `nonRetryable` included — from a `failure` block that
 * deliberately names two of its three fields. It was a promise the clause
 * could not structurally keep, and it went unnoticed because nothing
 * type-checked this file.
 */
type DeepPartial<T> = T extends object ? { [Key in keyof T]?: DeepPartial<T[Key]> } : T;

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

const heartbeatQuoteActivity = activity({
  name: "worker.heartbeat-quote",
  handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
    await heartbeat();
    return { cents: input.sku.length };
  }
});

const heartbeatQuoteWorkflow = workflow({
  name: "worker.heartbeat-quote-workflow",
  version: 1,
  handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
    const quote = await callActivity(
      heartbeatQuoteActivity,
      { sku: input.sku },
      {
        taskQueue: "activities",
        heartbeatTimeoutMs: 30_000
      }
    );
    return { cents: quote.cents };
  }
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

const successFailureSuccessWorkflow = workflow({
  name: "worker.success-failure-success",
  version: 1,
  handler: async (_input: {}): Promise<{
    readonly cents: number;
    readonly failure: string;
  }> => {
    const first = await callActivity(
      quoteActivity,
      { sku: "aa" },
      { taskQueue: "activities" }
    ).spawn();
    const second = await callActivity(
      failingActivity,
      {},
      { taskQueue: "activities" }
    ).spawn();
    const third = await callActivity(
      quoteActivity,
      { sku: "bbbb" },
      { taskQueue: "activities" }
    ).spawn();

    const firstResult = await first.result();
    let failure = "none";
    try {
      await second.result();
    } catch (error) {
      if (error instanceof ActivityFailureError) {
        failure = error.failure.message;
      } else {
        throw error;
      }
    }
    const thirdResult = await third.result();
    return {
      cents: firstResult.cents + thirdResult.cents,
      failure
    };
  }
});

const catchesActivityTimeoutWorkflow = workflow({
  name: "worker.catches-activity-timeout",
  version: 1,
  handler: async (input: { readonly sku: string }): Promise<{ readonly errorType: string; readonly message: string }> => {
    try {
      await callActivity(
        quoteActivity,
        { sku: input.sku },
        {
          taskQueue: "activities",
          startToCloseTimeoutMs: 0
        }
      );
      return { errorType: "none", message: "none" };
    } catch (error) {
      if (error instanceof ActivityFailureError) {
        return {
          errorType: error.failure.errorType,
          message: error.failure.message
        };
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

  it("schedules an unqueued activity onto the worker's own activity task queue", async () => {
    // `callActivity` with no `taskQueue`. The runtime's fallback used to be the
    // literal "default" no matter how the worker was configured, because
    // `WorkerOptions.activityTaskQueue` reached the claim loops and nothing
    // else — so the scheduling worker put the task on a queue it never polled
    // and the run hung with no error anywhere. Rust's runtime falls back to the
    // worker's configured activity queue
    // (`ActivityOptions::with_task_queue_fallback`).
    const unqueuedWorkflow = workflow({
      name: "worker.unqueued-activity",
      version: 1,
      handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> =>
        await callActivity(quoteActivity, { sku: input.sku })
    });
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(unqueuedWorkflow)
      .registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "unqueued-activity-worker",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "worker-activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      unqueuedWorkflow,
      workflowId("wf/worker-unqueued-activity"),
      "workflows",
      { sku: "sku-1" }
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({ kind: "Committed" });
    const scheduled = await backend.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const activityScheduled = scheduled.events.find(
      (event) => event.data.kind === "ActivityScheduled"
    );
    if (activityScheduled?.data.kind !== "ActivityScheduled") {
      throw new Error("expected ActivityScheduled");
    }
    expect(activityScheduled.data.scheduled.taskQueue).toBe("worker-activities");

    // The half that matters operationally: the worker that scheduled the task
    // can claim it. Against the literal "default" this is `NoTask` and the run
    // never finishes.
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({ kind: "Completed" });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({ kind: "Committed" });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("records a timer deadline against the provider's clock rather than the epoch", async () => {
    // `HotWorkflowExecution.#nowMs` defaulted to 0 and the worker never set it,
    // so `sleep(d)` recorded `fireAt = d` — an absolute deadline measured from
    // 1970 — and only survived because a provider scan then treated every timer
    // as already due. Rust's worker takes `now` from `backend.current_time()`
    // once per prepared workflow task.
    let now = 5_000;
    const backend = new MemoryBackend({ nowMs: () => now });
    const sleeper = workflow({
      name: "worker.clock-relative-timer",
      version: 1,
      handler: async (_input: {}): Promise<{ readonly slept: true }> => {
        await sleep(1_000);
        return { slept: true };
      }
    });
    const registry = new Registry().registerWorkflow(sleeper);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "clock-relative-timer-worker",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      sleeper,
      workflowId("wf/worker-clock-relative-timer"),
      "workflows",
      {}
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({ kind: "Committed" });
    const history = await backend.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const started = history.events.find((event) => event.data.kind === "TimerStarted");
    if (started?.data.kind !== "TimerStarted") {
      throw new Error("expected TimerStarted");
    }
    expect(Number(started.data.started.fireAt)).toBe(6_000);

    // The wait carries the same instant, so the provider's own due scan agrees
    // with the recorded deadline instead of firing a timer 5 seconds early.
    await expect(
      backend.fireDueTimers({ namespace: namespace(), now: 5_999, limit: 16 })
    ).resolves.toEqual({ fired: 0 });
    now = 6_000;
    await expect(
      backend.fireDueTimers({ namespace: namespace(), now: 6_000, limit: 16 })
    ).resolves.toEqual({ fired: 1 });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({ kind: "Committed" });
    await expect(handle.result()).resolves.toEqual({ slept: true });
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

  it("allows activity handlers to record heartbeats through worker context", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(heartbeatQuoteWorkflow)
      .registerActivity(heartbeatQuoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "heartbeat-worker",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      heartbeatQuoteWorkflow,
      workflowId("wf/worker-heartbeat-quote"),
      "workflows",
      { sku: "sku-1" }
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("rejects heartbeat calls outside activity handlers", async () => {
    await expect(heartbeat()).rejects.toThrow("activity handler");
  });

  it("resumes workflow with ActivityFailureError after activity start-to-close timeout", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(catchesActivityTimeoutWorkflow)
      .registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "timeout-workflow-worker",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      catchesActivityTimeoutWorkflow,
      workflowId("wf/worker-activity-timeout"),
      "workflows",
      { sku: "sku-timeout" }
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    const claimed = await backend.claimActivityTask("stalled-activity-worker", {
      namespace: namespace(),
      taskQueue: "activities",
      registeredActivityNames: [quoteActivity.name],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();

    await expect(worker.runActivityTimeoutMaintenanceOnce()).resolves.toEqual({ timedOut: 1 });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toMatchObject({
      errorType: "ActivityTimedOut",
      message: expect.stringContaining("start-to-close timed out")
    });
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

  it("stops a batched activity loop before claiming another activity after abort", async () => {
    const inner = new MemoryBackend();
    const batchSizes: number[] = [];
    const backend = recordActivityCompletionBatches(inner, batchSizes);
    const registry = new Registry()
      .registerWorkflow(twoQuoteWorkflow)
      .registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const controller = new AbortController();
    const claimedActivityIds: string[] = [];
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
        if (event.kind === "ActivityTaskClaimed") {
          claimedActivityIds.push(event.activityId);
          controller.abort();
        }
      }
    });
    const recoveryWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-b",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      activityCompletionBatchSize: 2,
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      twoQuoteWorkflow,
      workflowId("wf/worker-batch-abort"),
      "workflows",
      { first: "aa", second: "bbbb" }
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    const stopped = await worker.run({
      signal: controller.signal,
      maxIterations: 4,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    });

    expect(stopped).toMatchObject({
      stopReason: "abort",
      activityTasks: 1,
      workflowTasks: 0,
      errors: 0
    });
    expect(claimedActivityIds).toHaveLength(1);
    expect(batchSizes).toEqual([1]);

    await expect(recoveryWorker.run({
      maxIterations: 4,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    })).resolves.toMatchObject({
      activityTasks: 1,
      workflowTasks: expect.any(Number)
    });
    await expect(handle.result()).resolves.toEqual({ cents: 6 });
  });

  it("stops a batched activity loop after a failed activity without dropping flushed completions", async () => {
    const inner = new MemoryBackend();
    const batchSizes: number[] = [];
    const backend = recordActivityCompletionBatches(inner, batchSizes);
    const registry = new Registry()
      .registerWorkflow(successFailureSuccessWorkflow)
      .registerActivity(quoteActivity)
      .registerActivity(failingActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const controller = new AbortController();
    const events: WorkerEvent[] = [];
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      activityCompletionBatchSize: 3,
      payloadCodec: "Json",
      onEvent: (event) => {
        events.push(event);
        if (event.kind === "ActivityTaskFailed") {
          controller.abort();
        }
      }
    });
    const recoveryWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-b",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      activityCompletionBatchSize: 3,
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      successFailureSuccessWorkflow,
      workflowId("wf/worker-batch-failure-abort"),
      "workflows",
      {}
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    const stopped = await worker.run({
      signal: controller.signal,
      maxIterations: 4,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    });

    expect(stopped).toMatchObject({
      stopReason: "abort",
      activityTasks: 2,
      workflowTasks: 0,
      errors: 0
    });
    expect(events.map((event) => event.kind)).toEqual(
      expect.arrayContaining(["ActivityCompletionBatchFlushed", "ActivityTaskFailed"])
    );
    expect(events.filter((event) => event.kind === "ActivityTaskClaimed")).toHaveLength(2);
    expect(batchSizes).toEqual([1]);

    await expect(recoveryWorker.run({
      maxIterations: 6,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    })).resolves.toMatchObject({
      activityTasks: 1,
      workflowTasks: expect.any(Number)
    });
    await expect(handle.result()).resolves.toEqual({
      cents: 6,
      failure: "activity exploded"
    });
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
      // Maintenance is interval-paced, so a six-pass run would finish long
      // before the first jittered scan. Zero opts out of pacing: the loop then
      // scans once per macrotask turn, independently of the task loops.
      //
      // Ordering note: maintenance used to run inside the workflow pass,
      // strictly after the workflow task, so this was ordered by construction.
      // It is now concurrent, and the assertions below depend on the unpaced
      // maintenance loop getting a turn within the workflow loop's six passes.
      maintenanceIntervalMs: 0,
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

  it("does not poll when the worker loop starts with an already-aborted signal", async () => {
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
    const controller = new AbortController();
    controller.abort();
    const handle = await client.startWorkflow(
      echoWorkflow,
      workflowId("wf/worker-pre-aborted"),
      "workflows",
      { value: "ok" }
    );

    const outcome = await worker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    });

    expect(outcome).toEqual({
      stopReason: "abort",
      iterations: 0,
      workflowTasks: 0,
      activityTasks: 0,
      timersFired: 0,
      idleSleeps: 0,
      errors: 0
    });
    expect(worker.metrics()).toMatchObject({
      workflowTaskClaims: 0,
      workflowTaskNoTasks: 0,
      activityTaskClaims: 0,
      timersFired: 0
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ value: "ok" });
  });

  it("does not start activity polling after aborting during the workflow phase", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(quoteWorkflow).registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const controller = new AbortController();
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json",
      onEvent: (event) => {
        if (event.kind === "WorkflowTaskCommitted") {
          controller.abort();
        }
      }
    });
    const activityWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "activity-worker",
      workflowTaskQueue: "unused",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-abort-before-activity-poll"),
      "workflows",
      { sku: "sku-1" }
    );

    const stopped = await worker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    });

    expect(stopped).toMatchObject({
      stopReason: "abort",
      workflowTasks: 1,
      activityTasks: 0,
      timersFired: 0,
      errors: 0
    });
    expect(worker.metrics()).toMatchObject({
      workflowTaskCommits: 1,
      activityTaskClaims: 0,
      timersFired: 0
    });
    await expect(activityWorker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("does not run timer maintenance after aborting during the activity phase", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(quoteWorkflow)
      .registerWorkflow(timerWorkflow)
      .registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const setupWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "setup-worker",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });

    const timerHandle = await client.startWorkflow(
      timerWorkflow,
      workflowId("wf/worker-abort-after-activity-timer"),
      "workflows",
      {}
    );
    await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    const quoteHandle = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-abort-after-activity-quote"),
      "workflows",
      { sku: "sku-1" }
    );
    await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    const controller = new AbortController();
    const loopWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "loop-worker",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json",
      onEvent: (event) => {
        if (event.kind === "ActivityTaskCompleted") {
          controller.abort();
        }
      }
    });

    const stopped = await loopWorker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 0,
      timerMaintenanceLimit: 8
    });

    expect(stopped).toMatchObject({
      stopReason: "abort",
      workflowTasks: 0,
      activityTasks: 1,
      timersFired: 0,
      errors: 0
    });
    expect(loopWorker.metrics()).toMatchObject({
      activityTaskCompletions: 1,
      timersFired: 0
    });

    await expect(backend.fireDueTimers({ namespace: namespace(), now: Date.now() + 60_000, limit: 8 }))
      .resolves.toEqual({ fired: 1 });
    await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(timerHandle.result()).resolves.toEqual({ fired: true });
    await expect(quoteHandle.result()).resolves.toEqual({ cents: 5 });
  });

  it("does not run activity timeout maintenance after aborting during timer maintenance", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(timerWorkflow)
      .registerWorkflow(catchesActivityTimeoutWorkflow)
      .registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const setupWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "setup-worker",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });

    const timerHandle = await client.startWorkflow(
      timerWorkflow,
      workflowId("wf/worker-abort-after-timer-maintenance-timer"),
      "workflows",
      {}
    );
    await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    const timeoutHandle = await client.startWorkflow(
      catchesActivityTimeoutWorkflow,
      workflowId("wf/worker-abort-after-timer-maintenance-timeout"),
      "workflows",
      { sku: "sku-timeout" }
    );
    await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    const claimedActivity = await backend.claimActivityTask("timeout-claimer", {
      namespace: namespace(),
      taskQueue: "activities",
      registeredActivityNames: [quoteActivity.name],
      leaseDurationMs: 30_000
    });
    expect(claimedActivity).not.toBeNull();

    const controller = new AbortController();
    const events: WorkerEvent[] = [];
    const loopWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "loop-worker",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json",
      onEvent: (event) => {
        events.push(event);
        if (event.kind === "TimersFired") {
          controller.abort();
        }
      }
    });

    const stopped = await loopWorker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 0,
      timerMaintenanceLimit: 8,
      activityTimeoutMaintenanceLimit: 8
    });

    expect(stopped).toMatchObject({
      stopReason: "abort",
      timersFired: 1,
      errors: 0
    });
    expect(events.map((event) => event.kind)).not.toContain("ActivityTasksTimedOut");
    await expect(
      backend.timeoutDueActivities({ namespace: namespace(), now: Date.now(), limit: 8 })
    ).resolves.toEqual({ timedOut: 1 });
    await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(timerHandle.result()).resolves.toEqual({ fired: true });
    await expect(timeoutHandle.result()).resolves.toMatchObject({
      errorType: "ActivityTimedOut"
    });
  });

  it("resumes a hot workflow execution after a controlled worker-loop abort", async () => {
    const trace: string[] = [];
    const abortResumeWorkflow = workflow({
      name: "worker.hot-abort-resume",
      version: 1,
      handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
        trace.push(`start:${input.sku}`);
        const quote = await callActivity(
          quoteActivity,
          { sku: input.sku },
          { taskQueue: "activities" }
        );
        trace.push(`after:${quote.cents}`);
        return { cents: quote.cents };
      }
    });
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(abortResumeWorkflow)
      .registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const controller = new AbortController();
    const workflowWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "workflow-worker",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json",
      onEvent: (event) => {
        if (event.kind === "WorkflowTaskCommitted") {
          controller.abort();
        }
      }
    });
    const activityWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "activity-worker",
      workflowTaskQueue: "unused",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      abortResumeWorkflow,
      workflowId("wf/worker-hot-abort-resume"),
      "workflows",
      { sku: "sku-1" }
    );

    const stopped = await workflowWorker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    });

    expect(stopped).toMatchObject({
      stopReason: "abort",
      workflowTasks: 1,
      activityTasks: 0,
      errors: 0
    });
    expect(trace).toEqual(["start:sku-1"]);

    await expect(activityWorker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(workflowWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    expect(trace).toEqual(["start:sku-1", "after:5"]);
    expect(workflowWorker.metrics()).toMatchObject({
      workflowExecutionCacheHits: 1,
      workflowExecutionCacheMisses: 1,
      workflowExecutionCacheEvictions: 0
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("does not emit unhandled rejections after aborting with a parked hot workflow frame", async () => {
    const trace: string[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const parkedWorkflow = workflow({
        name: "worker.hot-abort-no-unhandled",
        version: 1,
        handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
          trace.push(`start:${input.sku}`);
          const quote = await callActivity(
            quoteActivity,
            { sku: input.sku },
            { taskQueue: "activities" }
          );
          trace.push(`after:${quote.cents}`);
          return { cents: quote.cents };
        }
      });
      const backend = new MemoryBackend();
      const registry = new Registry()
        .registerWorkflow(parkedWorkflow)
        .registerActivity(quoteActivity);
      const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
      const controller = new AbortController();
      const workflowWorker = new Worker({
        backend,
        registry,
        namespace: namespace(),
        workerId: "workflow-worker",
        workflowTaskQueue: "workflows",
        payloadCodec: "Json",
        onEvent: (event) => {
          if (event.kind === "WorkflowTaskCommitted") {
            controller.abort();
          }
        }
      });
      const activityWorker = new Worker({
        backend,
        registry,
        namespace: namespace(),
        workerId: "activity-worker",
        workflowTaskQueue: "unused",
        activityTaskQueue: "activities",
        payloadCodec: "Json"
      });
      const handle = await client.startWorkflow(
        parkedWorkflow,
        workflowId("wf/worker-hot-abort-no-unhandled"),
        "workflows",
        { sku: "sku-1" }
      );

      const stopped = await workflowWorker.run({
        signal: controller.signal,
        idleBackoffMs: 0,
        errorBackoffMs: 0
      });

      expect(stopped).toMatchObject({
        stopReason: "abort",
        workflowTasks: 1,
        activityTasks: 0,
        errors: 0
      });
      expect(trace).toEqual(["start:sku-1"]);
      await flushUnhandledRejectionTurn();
      expect(unhandledRejections).toEqual([]);

      await expect(activityWorker.runActivityTaskOnce()).resolves.toMatchObject({
        kind: "Completed",
        outcome: { kind: "Completed" }
      });
      await expect(workflowWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await flushUnhandledRejectionTurn();
      expect(unhandledRejections).toEqual([]);
      expect(trace).toEqual(["start:sku-1", "after:5"]);
      await expect(handle.result()).resolves.toEqual({ cents: 5 });
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
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

  it("stops the worker loop when onError aborts immediately", async () => {
    const backend = failFirstWorkflowClaim(new MemoryBackend());
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
    const errors: unknown[] = [];

    const startedAt = performance.now();
    const outcome = await worker.run({
      signal: controller.signal,
      errorBackoffMs: 5_000,
      maxErrorBackoffMs: 5_000,
      idleBackoffMs: 0,
      onError: (error) => {
        errors.push(error);
        controller.abort();
      }
    });

    expect(performance.now() - startedAt).toBeLessThan(1_000);
    expect(outcome).toMatchObject({
      stopReason: "abort",
      workflowTasks: 0,
      activityTasks: 0,
      timersFired: 0,
      errors: 1
    });
    expect(errors).toHaveLength(1);
    expect(worker.metrics()).toMatchObject({
      loopErrors: 1,
      workflowTaskClaims: 0
    });
  });

  it("stops the worker loop when aborted during error backoff", async () => {
    const backend = failFirstWorkflowClaim(new MemoryBackend());
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
    const errors: unknown[] = [];
    setTimeout(() => controller.abort(), 5);

    const startedAt = performance.now();
    const outcome = await worker.run({
      signal: controller.signal,
      errorBackoffMs: 5_000,
      maxErrorBackoffMs: 5_000,
      idleBackoffMs: 0,
      onError: (error) => {
        errors.push(error);
      }
    });

    expect(performance.now() - startedAt).toBeLessThan(1_000);
    expect(outcome).toMatchObject({
      stopReason: "abort",
      workflowTasks: 0,
      activityTasks: 0,
      timersFired: 0,
      errors: 1
    });
    expect(errors).toHaveLength(1);
    expect(worker.metrics()).toMatchObject({
      loopErrors: 1
    });
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

  it("uses cached replay history before streaming missing workflow claim prefetch", async () => {
    const inner = new MemoryBackend();
    const streamRequests: Parameters<DurableBackend["streamHistory"]>[0][] = [];
    const backend = truncateWorkflowClaimPrefetch(inner, 1, streamRequests);
    const registry = new Registry().registerWorkflow(quoteWorkflow).registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      historyFetchMaxEvents: 1,
      workflowExecutionCacheSize: 0,
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-partial-prefetch"),
      "workflows",
      { sku: "sku-1" }
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    expect(streamRequests.map((request) => Number(request.afterEventId))).toEqual([2]);
    expect(streamRequests.map((request) => Number(request.upToEventId))).toEqual([3]);
    expect(worker.metrics()).toMatchObject({
      workflowHistoryCacheHits: 1,
      workflowHistoryCacheMisses: 1,
      workflowHistoryCacheEvictions: 0,
      historyStreamChunks: 1,
      historyStreamEvents: 1
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("evicts cached replay history when the workflow history cache is full", async () => {
    const inner = new MemoryBackend();
    const streamRequests: Parameters<DurableBackend["streamHistory"]>[0][] = [];
    const backend = truncateWorkflowClaimPrefetch(inner, 1, streamRequests);
    const registry = new Registry().registerWorkflow(quoteWorkflow).registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      historyFetchMaxEvents: 1,
      workflowHistoryCacheSize: 1,
      workflowExecutionCacheSize: 0,
      payloadCodec: "Json"
    });
    const first = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-cache-evicted-1"),
      "workflows",
      { sku: "sku-1" }
    );
    await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-cache-evicted-2"),
      "workflows",
      { sku: "sku-2" }
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    expect(streamRequests.map((request) => Number(request.afterEventId))).toEqual([1, 2]);
    expect(streamRequests.map((request) => Number(request.upToEventId))).toEqual([3, 3]);
    expect(worker.metrics()).toMatchObject({
      workflowHistoryCacheHits: 0,
      workflowHistoryCacheMisses: 3,
      workflowHistoryCacheEvictions: 2,
      historyStreamChunks: 2,
      historyStreamEvents: 2
    });
    await expect(first.result()).resolves.toEqual({ cents: 5 });
  });

  it("keeps a hot workflow execution cached between worker tasks", async () => {
    const trace: string[] = [];
    const hotQuoteWorkflow = workflow({
      name: "worker.hot-quote",
      version: 1,
      handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
        trace.push(`start:${input.sku}`);
        const quote = await callActivity(
          quoteActivity,
          { sku: input.sku },
          { taskQueue: "activities" }
        );
        trace.push(`after:${quote.cents}`);
        return { cents: quote.cents };
      }
    });
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(hotQuoteWorkflow).registerActivity(quoteActivity);
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
      hotQuoteWorkflow,
      workflowId("wf/worker-hot-quote"),
      "workflows",
      { sku: "sku-1" }
    );

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    expect(trace).toEqual(["start:sku-1", "after:5"]);
    expect(worker.metrics()).toMatchObject({
      workflowExecutionCacheHits: 1,
      workflowExecutionCacheMisses: 1,
      workflowExecutionCacheEvictions: 0
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("falls back to replay after worker restart when the hot execution cache is empty", async () => {
    const trace: string[] = [];
    const restartWorkflow = workflow({
      name: "worker.hot-restart-fallback",
      version: 1,
      handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
        trace.push(`start:${input.sku}`);
        const quote = await callActivity(
          quoteActivity,
          { sku: input.sku },
          { taskQueue: "activities" }
        );
        trace.push(`after:${quote.cents}`);
        return { cents: quote.cents };
      }
    });
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(restartWorkflow).registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const firstWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const restartedWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-b",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      restartWorkflow,
      workflowId("wf/worker-hot-restart-fallback"),
      "workflows",
      { sku: "sku-1" }
    );

    await firstWorker.runWorkflowTaskOnce();
    await firstWorker.runActivityTaskOnce();
    await expect(restartedWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    expect(trace).toEqual(["start:sku-1", "start:sku-1", "after:5"]);
    expect(restartedWorker.metrics()).toMatchObject({
      workflowExecutionCacheHits: 0,
      workflowExecutionCacheMisses: 1
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("evicts hot workflow executions when the execution cache is full", async () => {
    const trace: string[] = [];
    const hotEvictionWorkflow = workflow({
      name: "worker.hot-cache-eviction",
      version: 1,
      handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
        trace.push(`start:${input.sku}`);
        const quote = await callActivity(
          quoteActivity,
          { sku: input.sku },
          { taskQueue: "activities" }
        );
        trace.push(`after:${input.sku}:${quote.cents}`);
        return { cents: quote.cents };
      }
    });
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(hotEvictionWorkflow).registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      workflowExecutionCacheSize: 1,
      payloadCodec: "Json"
    });
    const first = await client.startWorkflow(
      hotEvictionWorkflow,
      workflowId("wf/worker-hot-cache-evicted-1"),
      "workflows",
      { sku: "one" }
    );
    await client.startWorkflow(
      hotEvictionWorkflow,
      workflowId("wf/worker-hot-cache-evicted-2"),
      "workflows",
      { sku: "two" }
    );

    await worker.runWorkflowTaskOnce();
    await worker.runWorkflowTaskOnce();
    await worker.runActivityTaskOnce();
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    expect(trace).toEqual([
      "start:one",
      "start:two",
      "start:one",
      "after:one:3"
    ]);
    expect(worker.metrics()).toMatchObject({
      workflowExecutionCacheHits: 0,
      workflowExecutionCacheMisses: 3,
      workflowExecutionCacheEvictions: 1
    });
    await expect(first.result()).resolves.toEqual({ cents: 3 });
  });

  it("invalidates a hot workflow execution after a provider commit conflict", async () => {
    let nowMs = 0;
    const inner = new MemoryBackend({ nowMs: () => nowMs });
    let conflictsRemaining = 1;
    const backend = conflictWorkflowCompletionOnce(inner, () => conflictsRemaining-- > 0);
    const trace: string[] = [];
    const conflictWorkflow = workflow({
      name: "worker.hot-cache-conflict",
      version: 1,
      handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
        trace.push(`start:${input.sku}`);
        const quote = await callActivity(
          quoteActivity,
          { sku: input.sku },
          { taskQueue: "activities" }
        );
        trace.push(`after:${quote.cents}`);
        return { cents: quote.cents };
      }
    });
    const registry = new Registry().registerWorkflow(conflictWorkflow).registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      leaseDurationMs: 1,
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      conflictWorkflow,
      workflowId("wf/worker-hot-cache-conflict"),
      "workflows",
      { sku: "sku-1" }
    );

    await worker.runWorkflowTaskOnce();
    await worker.runActivityTaskOnce();
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Conflict" }
    });
    nowMs = 2;
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    expect(trace).toEqual(["start:sku-1", "after:5", "start:sku-1", "after:5"]);
    expect(worker.metrics()).toMatchObject({
      workflowTaskConflicts: 1,
      workflowExecutionCacheHits: 1,
      workflowExecutionCacheMisses: 2
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("disposes a parked hot workflow execution after a commit conflict", async () => {
    let nowMs = 0;
    const inner = new MemoryBackend({ nowMs: () => nowMs });
    let conflictsRemaining = 1;
    const backend = conflictActivitySchedulingOnce(inner, () => conflictsRemaining-- > 0);
    const trace: string[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const conflictWorkflow = workflow({
        name: "worker.hot-dispose-conflict",
        version: 1,
        handler: disposalTracingHandler(trace)
      });
      const registry = new Registry()
        .registerWorkflow(conflictWorkflow)
        .registerActivity(quoteActivity);
      const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
      const worker = new Worker({
        backend,
        registry,
        namespace: namespace(),
        workerId: "worker-a",
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        leaseDurationMs: 1,
        payloadCodec: "Json"
      });
      const handle = await client.startWorkflow(
        conflictWorkflow,
        workflowId("wf/worker-hot-dispose-conflict"),
        "workflows",
        { sku: "sku-1" }
      );

      await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Conflict" }
      });
      await flushUnhandledRejectionTurn();
      // The conflicted execution was parked on its activity waiter. Without
      // disposal that frame stays pending for the life of the process.
      expect(trace).toEqual(["start:sku-1", "waiter:workflow task commit conflicted"]);
      expect(unhandledRejections).toEqual([]);

      nowMs = 2;
      await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
        kind: "Completed",
        outcome: { kind: "Completed" }
      });
      await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await flushUnhandledRejectionTurn();

      expect(trace).toEqual([
        "start:sku-1",
        "waiter:workflow task commit conflicted",
        "start:sku-1",
        "after:5"
      ]);
      expect(unhandledRejections).toEqual([]);
      await expect(handle.result()).resolves.toEqual({ cents: 5 });
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("disposes a parked hot workflow execution after a failed workflow task", async () => {
    const inner = new MemoryBackend();
    let failuresRemaining = 1;
    const backend = failActivitySchedulingCommitOnce(inner, () => failuresRemaining-- > 0);
    const trace: string[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const failingWorkflow = workflow({
        name: "worker.hot-dispose-task-failure",
        version: 1,
        handler: disposalTracingHandler(trace)
      });
      const registry = new Registry()
        .registerWorkflow(failingWorkflow)
        .registerActivity(quoteActivity);
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
        failingWorkflow,
        workflowId("wf/worker-hot-dispose-task-failure"),
        "workflows",
        { sku: "sku-1" }
      );

      await expect(worker.runWorkflowTaskOnce()).rejects.toThrow("commit transport failed");
      await flushUnhandledRejectionTurn();
      expect(trace).toEqual(["start:sku-1", "waiter:workflow task failed before commit"]);
      expect(unhandledRejections).toEqual([]);

      await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
        kind: "Completed",
        outcome: { kind: "Completed" }
      });
      await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await flushUnhandledRejectionTurn();

      expect(trace).toEqual([
        "start:sku-1",
        "waiter:workflow task failed before commit",
        "start:sku-1",
        "after:5"
      ]);
      expect(unhandledRejections).toEqual([]);
      await expect(handle.result()).resolves.toEqual({ cents: 5 });
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("disposes a parked hot workflow execution evicted from a full execution cache", async () => {
    const backend = new MemoryBackend();
    const trace: string[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const evictedWorkflow = workflow({
        name: "worker.hot-dispose-eviction",
        version: 1,
        handler: disposalTracingHandler(trace)
      });
      const registry = new Registry()
        .registerWorkflow(evictedWorkflow)
        .registerActivity(quoteActivity);
      const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
      const worker = new Worker({
        backend,
        registry,
        namespace: namespace(),
        workerId: "worker-a",
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        workflowExecutionCacheSize: 1,
        payloadCodec: "Json"
      });
      const first = await client.startWorkflow(
        evictedWorkflow,
        workflowId("wf/worker-hot-dispose-evicted-1"),
        "workflows",
        { sku: "one" }
      );
      await client.startWorkflow(
        evictedWorkflow,
        workflowId("wf/worker-hot-dispose-evicted-2"),
        "workflows",
        { sku: "two" }
      );

      // Driven by filling the cache to its bound: the second run's commit
      // stores an entry over the size-1 limit and evicts the first run.
      await worker.runWorkflowTaskOnce();
      await worker.runWorkflowTaskOnce();
      await flushUnhandledRejectionTurn();
      expect(trace).toEqual([
        "start:one",
        "start:two",
        "waiter:workflow execution cache eviction"
      ]);
      expect(worker.metrics()).toMatchObject({ workflowExecutionCacheEvictions: 1 });
      expect(unhandledRejections).toEqual([]);

      await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
        kind: "Completed",
        outcome: { kind: "Completed" }
      });
      await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await flushUnhandledRejectionTurn();

      expect(trace).toEqual([
        "start:one",
        "start:two",
        "waiter:workflow execution cache eviction",
        "start:one",
        "after:3"
      ]);
      expect(unhandledRejections).toEqual([]);
      await expect(first.result()).resolves.toEqual({ cents: 3 });
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("disposes a parked hot workflow execution when the execution cache is disabled", async () => {
    const backend = new MemoryBackend();
    const trace: string[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const uncachedWorkflow = workflow({
        name: "worker.hot-dispose-cache-disabled",
        version: 1,
        handler: disposalTracingHandler(trace)
      });
      const registry = new Registry()
        .registerWorkflow(uncachedWorkflow)
        .registerActivity(quoteActivity);
      const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
      const worker = new Worker({
        backend,
        registry,
        namespace: namespace(),
        workerId: "worker-a",
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        // The configuration where this fires on every committed task.
        workflowExecutionCacheSize: 0,
        payloadCodec: "Json"
      });
      const handle = await client.startWorkflow(
        uncachedWorkflow,
        workflowId("wf/worker-hot-dispose-cache-disabled"),
        "workflows",
        { sku: "sku-1" }
      );

      await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await flushUnhandledRejectionTurn();
      // The task committed, but the frame it left parked on the activity waiter
      // is never reused, so it is abandoned the moment the commit lands.
      expect(trace).toEqual(["start:sku-1", "waiter:workflow execution cache disabled"]);
      expect(unhandledRejections).toEqual([]);

      await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
        kind: "Completed",
        outcome: { kind: "Completed" }
      });
      await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await flushUnhandledRejectionTurn();

      expect(trace).toEqual([
        "start:sku-1",
        "waiter:workflow execution cache disabled",
        "start:sku-1",
        "after:5"
      ]);
      expect(worker.metrics()).toMatchObject({
        workflowExecutionCacheHits: 0,
        workflowExecutionCacheMisses: 2
      });
      expect(unhandledRejections).toEqual([]);
      await expect(handle.result()).resolves.toEqual({ cents: 5 });
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("disposes a hot workflow execution superseded by a cold replay", async () => {
    const backend = new MemoryBackend();
    const trace: string[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      // Two sequential activities, so a second worker can advance the run past
      // the point the first worker's cached frame is parked at.
      const supersededWorkflow = workflow({
        name: "worker.hot-dispose-superseded",
        version: 1,
        handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
          trace.push(`start:${input.sku}`);
          let first: { readonly cents: number };
          try {
            first = await callActivity(
              quoteActivity,
              { sku: input.sku },
              { taskQueue: "activities" }
            );
          } catch (error) {
            trace.push(
              error instanceof HotWorkflowExecutionDisposedError
                ? `waiter:${error.reason}`
                : `waiter:unexpected:${String(error)}`
            );
            throw error;
          }
          trace.push(`first:${first.cents}`);
          const second = await callActivity(
            quoteActivity,
            { sku: `${input.sku}-2` },
            { taskQueue: "activities" }
          );
          trace.push(`second:${second.cents}`);
          return { cents: first.cents + second.cents };
        }
      });
      const registry = new Registry()
        .registerWorkflow(supersededWorkflow)
        .registerActivity(quoteActivity);
      const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
      const workerOptions = {
        backend,
        registry,
        namespace: namespace(),
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        payloadCodec: "Json"
      } as const;
      const cachingWorker = new Worker({ ...workerOptions, workerId: "worker-caching" });
      const competingWorker = new Worker({ ...workerOptions, workerId: "worker-competing" });
      const handle = await client.startWorkflow(
        supersededWorkflow,
        workflowId("wf/worker-hot-dispose-superseded"),
        "workflows",
        { sku: "sku-1" }
      );

      // The caching worker schedules the first activity and keeps the frame.
      await expect(cachingWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await cachingWorker.runActivityTaskOnce();

      // A different worker takes the wake task and appends command events the
      // cached frame never saw, so it can no longer be woken hot.
      await expect(competingWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await competingWorker.runActivityTaskOnce();
      await flushUnhandledRejectionTurn();
      expect(unhandledRejections).toEqual([]);

      await expect(cachingWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await flushUnhandledRejectionTurn();

      expect(trace).toEqual([
        // caching worker, task 1
        "start:sku-1",
        // competing worker replays cold and schedules the second activity
        "start:sku-1",
        "first:5",
        // caching worker finds its entry superseded, disposes it, replays cold.
        // The replacement frame starts synchronously inside the constructor,
        // so it runs before the disposed frame's rejection microtask.
        "start:sku-1",
        "waiter:superseded by cold replay",
        "first:5",
        "second:7"
      ]);
      expect(cachingWorker.metrics()).toMatchObject({
        workflowExecutionCacheHits: 0,
        workflowExecutionCacheMisses: 2
      });
      expect(unhandledRejections).toEqual([]);
      await expect(handle.result()).resolves.toEqual({ cents: 12 });
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("does not run local activity preference after aborting during a workflow task", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(quoteWorkflow).registerActivity(quoteActivity);
    const remoteRegistry = new Registry().registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const controller = new AbortController();
    const workflowWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "workflow-local-abort-worker",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      maxLocalActivitiesPerWorkflowTask: 1,
      payloadCodec: "Json",
      onEvent: (event) => {
        if (event.kind === "WorkflowTaskClaimed") {
          controller.abort();
        }
      }
    });
    const remoteWorker = new Worker({
      backend,
      registry: remoteRegistry,
      namespace: namespace(),
      workerId: "remote-after-local-abort-worker",
      workflowTaskQueue: "unused",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/worker-local-activity-abort"),
      "workflows",
      { sku: "sku-1" }
    );

    const stopped = await workflowWorker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    });

    expect(stopped).toMatchObject({
      stopReason: "abort",
      workflowTasks: 1,
      activityTasks: 0,
      timersFired: 0,
      errors: 0
    });
    expect(workflowWorker.metrics()).toMatchObject({
      workflowTaskCommits: 1,
      activityTaskClaims: 0,
      activityTaskCompletions: 0
    });
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

  it("stops local activity preference before the next local claim after abort", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(twoQuoteWorkflow)
      .registerActivity(quoteActivity);
    const remoteRegistry = new Registry().registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const controller = new AbortController();
    const localCompleted: string[] = [];
    const workflowWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "workflow-local-abort-between-activities",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      maxLocalActivitiesPerWorkflowTask: 2,
      payloadCodec: "Json",
      onEvent: (event) => {
        if (event.kind === "ActivityTaskCompleted") {
          localCompleted.push(event.activityId);
          controller.abort();
        }
      }
    });
    const remoteWorker = new Worker({
      backend,
      registry: remoteRegistry,
      namespace: namespace(),
      workerId: "remote-after-local-between-abort",
      workflowTaskQueue: "unused",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      twoQuoteWorkflow,
      workflowId("wf/worker-local-activity-between-abort"),
      "workflows",
      { first: "aa", second: "bbbb" }
    );

    const stopped = await workflowWorker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 0
    });

    expect(stopped).toMatchObject({
      stopReason: "abort",
      workflowTasks: 1,
      activityTasks: 1,
      timersFired: 0,
      errors: 0
    });
    expect(localCompleted).toHaveLength(1);
    expect(workflowWorker.metrics()).toMatchObject({
      workflowTaskCommits: 1,
      activityTaskClaims: 1,
      activityTaskCompletions: 1
    });
    await expect(remoteWorker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    await expect(workflowWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ cents: 6 });
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
    } satisfies DeepPartial<WorkflowFailureError>);

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

describe("Worker claim release on error paths", () => {
  it("releases workflow claims when workflow registry lookup fails", async () => {
    const inner = new MemoryBackend();
    const backend = forceWorkflowClaimTypes(inner, [echoWorkflow.workflowType]);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    await client.startWorkflow(echoWorkflow, workflowId("wf/release-missing-workflow"), "workflows", {
      value: "ok"
    });
    const worker = new Worker({
      backend,
      registry: new Registry(),
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      leaseDurationMs: 30_000,
      payloadCodec: "Json"
    });

    await expect(worker.runWorkflowTaskOnce()).rejects.toThrow("workflow is not registered");

    const reclaimed = await inner.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: "workflows",
      registeredWorkflowTypes: [echoWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    expect(reclaimed?.reason).toBe("WorkflowStarted");
  });

  it("drains a claimed workflow batch when an earlier task fails", async () => {
    const unregisteredWorkflow = workflow({
      name: "worker.batch-unregistered",
      version: 1,
      handler: async (input: EchoInput): Promise<EchoOutput> => input
    });
    const inner = new MemoryBackend();
    // The wrapper claims both types so the unregistered workflow (started
    // first, so claimed first) fails registry lookup inside the batch drain
    // while its echo neighbors remain independently processable.
    const backend = withSequentialBatchClaims(inner, [
      unregisteredWorkflow.workflowType,
      echoWorkflow.workflowType
    ]);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    await client.startWorkflow(
      unregisteredWorkflow,
      workflowId("wf/batch-drain-bad"),
      "workflows",
      { value: "bad" }
    );
    const secondHandle = await client.startWorkflow(
      echoWorkflow,
      workflowId("wf/batch-drain-2"),
      "workflows",
      { value: "two" }
    );
    const thirdHandle = await client.startWorkflow(
      echoWorkflow,
      workflowId("wf/batch-drain-3"),
      "workflows",
      { value: "three" }
    );
    const worker = new Worker({
      backend,
      registry: new Registry().registerWorkflow(echoWorkflow),
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      leaseDurationMs: 30_000,
      payloadCodec: "Json"
    });

    // The first error still surfaces, but only after the whole batch drains.
    await expect(worker.runWorkflowTaskBatchOnce(3)).rejects.toThrow(
      "workflow is not registered: worker.batch-unregistered@1"
    );

    // Neighbors committed in the same call rather than stranding until lease expiry.
    await expect(secondHandle.result()).resolves.toEqual({ value: "two" });
    await expect(thirdHandle.result()).resolves.toEqual({ value: "three" });

    // The failing task's own claim was released with its wake reason preserved.
    const reclaimed = await inner.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: "workflows",
      registeredWorkflowTypes: [unregisteredWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    expect(reclaimed?.reason).toBe("WorkflowStarted");
  });

  it("releases workflow claims when live signal reads fail", async () => {
    const inner = new MemoryBackend();
    const backend = failBackendCall(inner, "readSignalInbox", new Error("signal read failed"));
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    await client.startWorkflow(echoWorkflow, workflowId("wf/release-signal-read"), "workflows", {
      value: "ok"
    });
    const worker = new Worker({
      backend,
      registry: new Registry().registerWorkflow(echoWorkflow),
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      registeredSignalNames: ["approval"],
      leaseDurationMs: 30_000,
      payloadCodec: "Json"
    });

    await expect(worker.runWorkflowTaskOnce()).rejects.toThrow("signal read failed");

    const reclaimed = await inner.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: "workflows",
      registeredWorkflowTypes: [echoWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    expect(reclaimed?.reason).toBe("WorkflowStarted");
  });

  it("releases workflow claims when replay history streaming fails", async () => {
    const inner = new MemoryBackend();
    const truncated = truncateWorkflowClaimPrefetch(inner, 0, []);
    const backend = failBackendCall(truncated, "streamHistory", new Error("stream failed"));
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    await client.startWorkflow(echoWorkflow, workflowId("wf/release-stream"), "workflows", {
      value: "ok"
    });
    const worker = new Worker({
      backend,
      registry: new Registry().registerWorkflow(echoWorkflow),
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      workflowExecutionCacheSize: 0,
      leaseDurationMs: 30_000,
      payloadCodec: "Json"
    });

    await expect(worker.runWorkflowTaskOnce()).rejects.toThrow("stream failed");

    const reclaimed = await inner.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: "workflows",
      registeredWorkflowTypes: [echoWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    expect(reclaimed?.reason).toBe("WorkflowStarted");
  });

  it("releases workflow claims when commit fails", async () => {
    const inner = new MemoryBackend();
    const backend = failBackendCall(inner, "commitWorkflowTask", new Error("commit failed"));
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    await client.startWorkflow(echoWorkflow, workflowId("wf/release-commit"), "workflows", {
      value: "ok"
    });
    const worker = new Worker({
      backend,
      registry: new Registry().registerWorkflow(echoWorkflow),
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      leaseDurationMs: 30_000,
      payloadCodec: "Json"
    });

    await expect(worker.runWorkflowTaskOnce()).rejects.toThrow("commit failed");

    const reclaimed = await inner.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: "workflows",
      registeredWorkflowTypes: [echoWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    expect(reclaimed?.reason).toBe("WorkflowStarted");
  });

  it("delays released workflow claims after nondeterminism errors", async () => {
    let now = 1_000;
    const backend = new MemoryBackend({ nowMs: () => now });
    const nondeterministicWorkflow = workflow({
      name: "worker.nondeterministic-release",
      version: 1,
      handler: async (_input: {}): Promise<{ readonly value: number }> => ({
        value: Date.now()
      })
    });
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    await client.startWorkflow(
      nondeterministicWorkflow,
      workflowId("wf/release-nondeterminism"),
      "workflows",
      {}
    );
    const worker = new Worker({
      backend,
      registry: new Registry().registerWorkflow(nondeterministicWorkflow),
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      leaseDurationMs: 30_000,
      nondeterminismRetryBackoffMs: 100,
      payloadCodec: "Json",
      // The workflow trips the guard with a bare `Date.now()`, so this case
      // needs the runtime guards regardless of the ambient NODE_ENV. They are
      // off by default in production.
      nondeterminismGuards: true
    });

    await expect(worker.runWorkflowTaskOnce()).rejects.toThrow("nondeterminism:");
    await expect(
      backend.claimWorkflowTask("worker-too-early", {
        namespace: namespace(),
        taskQueue: "workflows",
        registeredWorkflowTypes: [nondeterministicWorkflow.workflowType],
        leaseDurationMs: 30_000
      })
    ).resolves.toBeNull();

    now += 100;
    const reclaimed = await backend.claimWorkflowTask("worker-after-backoff", {
      namespace: namespace(),
      taskQueue: "workflows",
      registeredWorkflowTypes: [nondeterministicWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    expect(reclaimed?.reason).toBe("WorkflowStarted");
  });

  it("fails unregistered activity claims instead of dropping them until lease expiry", async () => {
    const inner = new MemoryBackend();
    const backend = forceActivityClaimNames(inner, [quoteActivity.name]);
    const registry = new Registry().registerWorkflow(quoteWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/release-missing-activity"),
      "workflows",
      { sku: "sku-1" }
    );
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      leaseDurationMs: 30_000,
      payloadCodec: "Json"
    });

    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Failed",
      outcome: { kind: "Failed" }
    });

    const wake = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: "workflows",
      registeredWorkflowTypes: [quoteWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    expect(wake?.reason).toBe("ActivityFailed");
  });
});

describe("Worker run loops", () => {
  it("commits a workflow task while a multi-second activity is in flight on the same worker", async () => {
    const parked = deferred<void>();
    const release = deferred<void>();
    let parkedActivityFinished = false;
    const parkedActivity = activity({
      name: "loops.parked-quote",
      handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
        parked.resolve();
        // Stands in for a multi-second activity without a multi-second sleep:
        // the handler cannot return until the test releases it.
        await release.promise;
        parkedActivityFinished = true;
        return { cents: input.sku.length };
      }
    });
    const parkedWorkflow = workflow({
      name: "loops.parked-quote-workflow",
      version: 1,
      handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
        const quote = await callActivity(
          parkedActivity,
          { sku: input.sku },
          { taskQueue: "activities" }
        );
        return { cents: quote.cents };
      }
    });

    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(parkedWorkflow)
      .registerWorkflow(echoWorkflow)
      .registerActivity(parkedActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const controller = new AbortController();
    const echoCommitted = deferred<void>();
    let parkedRunId: string | null = null;
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "loop-split-worker",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json",
      onEvent: (event) => {
        // Only a commit for some run other than the parked one proves the
        // workflow loop moved while the activity loop was stuck.
        if (event.kind === "WorkflowTaskCommitted" && String(event.runId) !== parkedRunId) {
          echoCommitted.resolve();
        }
      }
    });

    const parkedHandle = await client.startWorkflow(
      parkedWorkflow,
      workflowId("wf/loop-split-parked"),
      "workflows",
      { sku: "sku-1" }
    );
    parkedRunId = String(parkedHandle.runId);
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    const running = worker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 0,
      runTimerMaintenance: false
    });
    await parked.promise;

    const echoHandle = await client.startWorkflow(
      echoWorkflow,
      workflowId("wf/loop-split-echo"),
      "workflows",
      { value: "ok" }
    );
    await expect(
      raceDeadline(
        echoCommitted.promise.then(() => "workflow-committed"),
        2_000,
        "activity-loop-blocked-the-workflow-loop"
      )
    ).resolves.toBe("workflow-committed");
    expect(parkedActivityFinished).toBe(false);

    release.resolve();
    controller.abort();
    await running;

    await expect(echoHandle.result()).resolves.toEqual({ value: "ok" });
    expect(parkedActivityFinished).toBe(true);
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(parkedHandle.result()).resolves.toEqual({ cents: 5 });
  });

  it("completes an activity while every workflow claim fails", async () => {
    const inner = new MemoryBackend();
    const registry = new Registry().registerWorkflow(quoteWorkflow).registerActivity(quoteActivity);
    const client = new Client(inner, { namespace: namespace(), payloadCodec: "Json" });
    const setupWorker = new Worker({
      backend: inner,
      registry,
      namespace: namespace(),
      workerId: "isolation-setup-worker",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/loop-stage-isolation"),
      "workflows",
      { sku: "sku-1" }
    );
    await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    const backend = failBackendCall(
      inner,
      "claimWorkflowTask",
      new Error("workflow claim exploded")
    );
    const controller = new AbortController();
    const errors: unknown[] = [];
    const loopWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "isolation-loop-worker",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json",
      onEvent: (event) => {
        if (event.kind === "ActivityTaskCompleted") {
          controller.abort();
        }
      }
    });

    const running = loopWorker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 1,
      maxErrorBackoffMs: 1,
      runTimerMaintenance: false,
      onError: (error) => {
        errors.push(error);
      }
    });
    await expect(
      raceDeadline(
        running.then(() => "activity-completed"),
        2_000,
        "workflow-loop-error-suppressed-the-activity-loop"
      )
    ).resolves.toBe("activity-completed");
    const stopped = await running;

    expect(stopped).toMatchObject({ stopReason: "abort", activityTasks: 1, workflowTasks: 0 });
    expect(stopped.errors).toBeGreaterThanOrEqual(1);
    expect(errors.length).toBeGreaterThanOrEqual(1);
    for (const error of errors) {
      expect((error as Error).message).toBe("workflow claim exploded");
    }
    expect(loopWorker.metrics()).toMatchObject({ activityTaskCompletions: 1 });
    await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ cents: 5 });
  });

  it("paces maintenance by elapsed time rather than by workflow task count", async () => {
    const workflowTaskCount = 50;
    const maintenanceIntervalMs = 100;
    const timerScans: number[] = [];
    const timeoutScans: number[] = [];
    const backend = recordMaintenanceScans(new MemoryBackend(), timerScans, timeoutScans);
    const registry = new Registry().registerWorkflow(echoWorkflow);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const controller = new AbortController();
    const allCommitted = deferred<void>();
    let commits = 0;
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "maintenance-cadence-worker",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json",
      onEvent: (event) => {
        if (event.kind === "WorkflowTaskCommitted") {
          commits += 1;
          if (commits === workflowTaskCount) {
            allCommitted.resolve();
          }
        }
      }
    });

    for (let index = 0; index < workflowTaskCount; index += 1) {
      await client.startWorkflow(
        echoWorkflow,
        workflowId(`wf/maintenance-cadence-${index}`),
        "workflows",
        { value: `v${index}` }
      );
    }

    const startedAt = Date.now();
    const running = worker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 0,
      maintenanceIntervalMs,
      maxMaintenanceIntervalMs: maintenanceIntervalMs
    });
    await expect(
      raceDeadline(allCommitted.promise.then(() => "committed"), 5_000, "not-committed")
    ).resolves.toBe("committed");
    // The commits finish in a few milliseconds, well inside the first jittered
    // delay. Holding the run open across several intervals is what makes the
    // cadence observable at all — without it every assertion below is satisfied
    // by zero scans, which is equally true of a maintenance loop that never
    // started.
    const observationWindowMs = 700;
    await new Promise<void>((resolve) => setTimeout(resolve, observationWindowMs));
    controller.abort();
    await running;
    const elapsedMs = Date.now() - startedAt;

    // Delays are jittered over [0.5x, 1.5x) of the interval, so the scan count
    // is bracketed by elapsed time in both directions. One extra on each side
    // for the scans that can land on the opening and closing edges.
    const upperBound = Math.ceil(elapsedMs / (maintenanceIntervalMs * 0.5)) + 1;
    const lowerBound = Math.max(1, Math.floor(elapsedMs / (maintenanceIntervalMs * 1.5)) - 1);
    expect(commits).toBe(workflowTaskCount);
    expect(timerScans.length).toBeGreaterThanOrEqual(lowerBound);
    expect(timerScans.length).toBeLessThanOrEqual(upperBound);
    expect(timeoutScans.length).toBeGreaterThanOrEqual(lowerBound);
    expect(timeoutScans.length).toBeLessThanOrEqual(upperBound);
    // The O(elapsed / interval) claim, stated against the work rate it replaces.
    expect(timerScans.length).toBeLessThan(workflowTaskCount);
  });

  it("polls activities while the workflow loop is saturated with back-to-back tasks", async () => {
    // MemoryBackend settles every call on the microtask queue, so a loop that
    // makes progress every pass holds the thread unless it yields deliberately.
    // This is the converse of the parked-activity test: there the busy loop
    // awaits a real promise and yields for free.
    //
    // `setTimeout` is faked and never advanced, but `setImmediate` is left real.
    // Both halves of the handoff then become decidable instead of a race
    // against the 1 ms timer clamp: the busy loop can only give up the thread
    // through its `setImmediate` progress yield, and the idle activity loop can
    // only take it if a zero backoff resumes off the timer queue. Either half
    // reverted parks the activity loop forever rather than merely making it
    // late, so this fails deterministically instead of about a third of runs.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      await runSaturatedWorkflowLoopScenario();
    } finally {
      vi.useRealTimers();
    }
  });

  const runSaturatedWorkflowLoopScenario = async (): Promise<void> => {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(echoWorkflow)
      .registerWorkflow(quoteWorkflow)
      .registerActivity(quoteActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const controller = new AbortController();
    const activityCompleted = deferred<void>();
    const workflowBacklogDrained = deferred<void>();
    // 50 echo tasks plus the quote workflow's scheduling task; the quote
    // workflow's second task cannot run until the activity completes.
    const backlogTasks = 51;
    let commits = 0;
    let commitsWhenActivityCompleted: number | null = null;
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "saturated-workflow-loop-worker",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json",
      onEvent: (event) => {
        if (event.kind === "WorkflowTaskCommitted") {
          commits += 1;
          if (commits === backlogTasks) {
            workflowBacklogDrained.resolve();
          }
        }
        if (event.kind === "ActivityTaskCompleted") {
          commitsWhenActivityCompleted ??= commits;
          activityCompleted.resolve();
        }
      }
    });

    for (let index = 0; index < 25; index += 1) {
      await client.startWorkflow(
        echoWorkflow,
        workflowId(`wf/saturated-before-${index}`),
        "workflows",
        { value: `v${index}` }
      );
    }
    await client.startWorkflow(
      quoteWorkflow,
      workflowId("wf/saturated-quote"),
      "workflows",
      { sku: "sku-1" }
    );
    for (let index = 0; index < 25; index += 1) {
      await client.startWorkflow(
        echoWorkflow,
        workflowId(`wf/saturated-after-${index}`),
        "workflows",
        { value: `v${index}` }
      );
    }

    const running = worker.run({
      signal: controller.signal,
      idleBackoffMs: 0,
      errorBackoffMs: 0,
      runTimerMaintenance: false
    });
    await expect(
      Promise.race([
        activityCompleted.promise.then(() => "activity-ran-during-workflow-backlog"),
        workflowBacklogDrained.promise.then(() => "workflow-loop-starved-the-activity-loop")
      ])
    ).resolves.toBe("activity-ran-during-workflow-backlog");

    controller.abort();
    await running;

    expect(worker.metrics().activityTaskCompletions).toBe(1);
    expect(commitsWhenActivityCompleted).not.toBeNull();
    expect(commitsWhenActivityCompleted).toBeLessThan(backlogTasks);
  };

  it("derives a reproducible per-worker maintenance schedule from the worker id", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    try {
      const scheduleFor = async (workerIdValue: string): Promise<readonly number[]> => {
        const scans: number[] = [];
        const backend = recordMaintenanceScans(new MemoryBackend(), scans, []);
        const worker = new Worker({
          backend,
          registry: new Registry().registerWorkflow(echoWorkflow),
          namespace: namespace(),
          workerId: workerIdValue,
          workflowTaskQueue: "workflows",
          payloadCodec: "Json"
        });
        const controller = new AbortController();
        const startedAt = Date.now();
        const running = worker.run({
          signal: controller.signal,
          // Non-zero so the idle workflow loop schedules a bounded number of
          // fake timers instead of one per virtual millisecond.
          idleBackoffMs: 25,
          maxIdleBackoffMs: 25,
          maintenanceIntervalMs: 100,
          maxMaintenanceIntervalMs: 100
        });
        await vi.advanceTimersByTimeAsync(1_000);
        controller.abort();
        await vi.advanceTimersByTimeAsync(0);
        await running;
        return scans.map((at) => at - startedAt);
      };

      const first = await scheduleFor("worker-jitter-a");
      const repeat = await scheduleFor("worker-jitter-a");
      const other = await scheduleFor("worker-jitter-b");

      expect(first.length).toBeGreaterThanOrEqual(5);
      expect(repeat).toEqual(first);
      expect(other).not.toEqual(first);
      expect(other[0]).not.toBe(first[0]);
      for (const at of [...first, ...other]) {
        expect(at).toBeGreaterThanOrEqual(50);
      }
    } finally {
      vi.useRealTimers();
    }
  });

  it("leaves no loop running and no unhandled rejection after aborting mid-flight", async () => {
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const parked = deferred<void>();
      const release = deferred<void>();
      const parkedActivity = activity({
        name: "loops.shutdown-quote",
        handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
          parked.resolve();
          await release.promise;
          return { cents: input.sku.length };
        }
      });
      const parkedWorkflow = workflow({
        name: "loops.shutdown-workflow",
        version: 1,
        handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
          const quote = await callActivity(
            parkedActivity,
            { sku: input.sku },
            { taskQueue: "activities" }
          );
          return { cents: quote.cents };
        }
      });

      const calls: string[] = [];
      const backend = recordBackendCalls(new MemoryBackend(), calls);
      const registry = new Registry()
        .registerWorkflow(parkedWorkflow)
        .registerActivity(parkedActivity);
      const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
      const controller = new AbortController();
      const worker = new Worker({
        backend,
        registry,
        namespace: namespace(),
        workerId: "shutdown-worker",
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        payloadCodec: "Json"
      });
      const handle = await client.startWorkflow(
        parkedWorkflow,
        workflowId("wf/loop-shutdown"),
        "workflows",
        { sku: "sku-1" }
      );

      const running = worker.run({
        signal: controller.signal,
        idleBackoffMs: 5,
        maxIdleBackoffMs: 5,
        errorBackoffMs: 5,
        maintenanceIntervalMs: 20,
        maxMaintenanceIntervalMs: 20
      });
      await parked.promise;
      // Abort with a workflow frame parked on its activity waiter and the
      // activity itself mid-await.
      let runResolved = false;
      const settled = (): void => {
        runResolved = true;
      };
      running.then(settled, settled);
      controller.abort();

      // No loop may outlive `run()`: the activity loop is still inside its
      // handler, so `run()` must not resolve yet however long the loops are
      // given to unwind.
      await new Promise<void>((resolve) => setTimeout(resolve, 40));
      await flushUnhandledRejectionTurn();
      expect(runResolved).toBe(false);

      release.resolve();
      const stopped = await running;

      expect(stopped.stopReason).toBe("abort");
      await flushUnhandledRejectionTurn();
      expect(unhandledRejections).toEqual([]);

      const afterStop = calls.length;
      await new Promise<void>((resolve) => setTimeout(resolve, 80));
      await flushUnhandledRejectionTurn();
      expect(calls.length).toBe(afterStop);
      expect(unhandledRejections).toEqual([]);

      await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await expect(handle.result()).resolves.toEqual({ cents: 5 });
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("waits for the peer loop before failing out of run when a loop escapes its error budget", async () => {
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const parked = deferred<void>();
      const release = deferred<void>();
      const parkedActivity = activity({
        name: "loops.error-budget-quote",
        handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
          parked.resolve();
          await release.promise;
          return { cents: input.sku.length };
        }
      });
      const parkedWorkflow = workflow({
        name: "loops.error-budget-workflow",
        version: 1,
        handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
          const quote = await callActivity(
            parkedActivity,
            { sku: input.sku },
            { taskQueue: "activities" }
          );
          return { cents: quote.cents };
        }
      });

      const inner = new MemoryBackend();
      const registry = new Registry()
        .registerWorkflow(parkedWorkflow)
        .registerActivity(parkedActivity);
      const client = new Client(inner, { namespace: namespace(), payloadCodec: "Json" });
      const setupWorker = new Worker({
        backend: inner,
        registry,
        namespace: namespace(),
        workerId: "error-budget-setup-worker",
        workflowTaskQueue: "workflows",
        payloadCodec: "Json"
      });
      await client.startWorkflow(
        parkedWorkflow,
        workflowId("wf/loop-error-budget"),
        "workflows",
        { sku: "sku-1" }
      );
      await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });

      const worker = new Worker({
        backend: failBackendCall(inner, "claimWorkflowTask", new Error("workflow claim exploded")),
        registry,
        namespace: namespace(),
        workerId: "error-budget-worker",
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        payloadCodec: "Json"
      });
      const running = worker.run({
        idleBackoffMs: 0,
        errorBackoffMs: 0,
        runTimerMaintenance: false,
        onError: () => {
          throw new Error("onError exploded");
        }
      });
      let runSettled = false;
      const settled = (): void => {
        runSettled = true;
      };
      running.then(settled, settled);

      await parked.promise;
      await new Promise<void>((resolve) => setTimeout(resolve, 40));
      await flushUnhandledRejectionTurn();
      // The workflow loop's `onError` has already thrown, but the activity loop
      // is still inside its handler, so `run()` must not have settled.
      expect(runSettled).toBe(false);

      release.resolve();
      await expect(running).rejects.toThrow("onError exploded");
      await flushUnhandledRejectionTurn();
      expect(unhandledRejections).toEqual([]);
      expect(worker.metrics()).toMatchObject({ activityTaskCompletions: 1 });
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("observes a maintenance failure while an in-flight activity holds shutdown open", async () => {
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const parked = deferred<void>();
      const release = deferred<void>();
      const parkedActivity = activity({
        name: "loops.maintenance-budget-quote",
        handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
          parked.resolve();
          await release.promise;
          return { cents: input.sku.length };
        }
      });
      const parkedWorkflow = workflow({
        name: "loops.maintenance-budget-workflow",
        version: 1,
        handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
          const quote = await callActivity(
            parkedActivity,
            { sku: input.sku },
            { taskQueue: "activities" }
          );
          return { cents: quote.cents };
        }
      });

      const inner = new MemoryBackend();
      const registry = new Registry()
        .registerWorkflow(parkedWorkflow)
        .registerActivity(parkedActivity);
      const client = new Client(inner, { namespace: namespace(), payloadCodec: "Json" });
      const setupWorker = new Worker({
        backend: inner,
        registry,
        namespace: namespace(),
        workerId: "maintenance-budget-setup-worker",
        workflowTaskQueue: "workflows",
        payloadCodec: "Json"
      });
      await client.startWorkflow(
        parkedWorkflow,
        workflowId("wf/maintenance-error-budget"),
        "workflows",
        { sku: "sku-1" }
      );
      await expect(setupWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });

      const worker = new Worker({
        backend: failBackendCall(inner, "fireDueTimers", new Error("timers exploded")),
        registry,
        namespace: namespace(),
        workerId: "maintenance-error-budget-worker",
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        payloadCodec: "Json"
      });
      const running = worker.run({
        idleBackoffMs: 10,
        maxIdleBackoffMs: 10,
        errorBackoffMs: 0,
        // Late enough that the activity loop has claimed and parked before the
        // first scan fails, so the failure lands with shutdown already blocked.
        maintenanceIntervalMs: 60,
        maxMaintenanceIntervalMs: 60,
        onError: (error) => {
          throw new Error(`onError rethrew: ${(error as Error).message}`);
        }
      });
      let runSettled = false;
      const settled = (): void => {
        runSettled = true;
      };
      running.then(settled, settled);

      await parked.promise;
      // Maintenance fails inside this window, and `run()` cannot reach the
      // `await` in its `finally` while the activity handler is still parked.
      // Nothing else is watching the maintenance promise, so a handler attached
      // only at that `await` leaves the rejection unobserved for the whole
      // window — which under Node's default `--unhandled-rejections=throw`
      // kills the worker process.
      await new Promise<void>((resolve) => setTimeout(resolve, 200));
      await flushUnhandledRejectionTurn();
      expect(unhandledRejections).toEqual([]);
      expect(runSettled).toBe(false);

      release.resolve();
      // A failed maintenance loop must also stop the task loops, or `run()`
      // would carry the rejection for as long as they kept polling.
      await expect(
        raceDeadline(
          running.then(() => "resolved", (error: unknown) => (error as Error).message),
          2_000,
          "maintenance-failure-did-not-stop-the-task-loops"
        )
      ).resolves.toBe("onError rethrew: timers exploded");
      await flushUnhandledRejectionTurn();
      expect(unhandledRejections).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("reports the task-loop failure rather than the maintenance failure when both escape", async () => {
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      // Both loops must genuinely reject, which needs the workflow claim to be
      // already in flight when maintenance fails: stopping the peers cannot
      // cancel an outstanding backend call, so the workflow loop still reaches
      // its own rethrowing `onError` afterwards.
      const backend = failBackendCallAfter(
        failBackendCall(new MemoryBackend(), "fireDueTimers", new Error("timers exploded")),
        "claimWorkflowTask",
        new Error("workflow claim exploded"),
        30
      );
      const worker = new Worker({
        backend,
        registry: new Registry().registerWorkflow(echoWorkflow),
        namespace: namespace(),
        workerId: "both-budgets-fail-worker",
        workflowTaskQueue: "workflows",
        payloadCodec: "Json"
      });

      await expect(
        worker.run({
          idleBackoffMs: 0,
          errorBackoffMs: 0,
          maintenanceIntervalMs: 0,
          onError: (error) => {
            throw new Error(`onError rethrew: ${(error as Error).message}`);
          }
        })
      ).rejects.toThrow("onError rethrew: workflow claim exploded");

      await flushUnhandledRejectionTurn();
      expect(unhandledRejections).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("keeps the one-shot drivers to a single sequential pass with no maintenance", async () => {
    const cases = [
      {
        name: "runWorkflowTaskOnce",
        claim: "claimWorkflowTask",
        foreignClaim: "claimActivityTask",
        drive: async (worker: Worker) => await worker.runWorkflowTaskOnce()
      },
      {
        name: "runWorkflowTaskBatchOnce",
        claim: "claimWorkflowTask",
        foreignClaim: "claimActivityTask",
        drive: async (worker: Worker) => await worker.runWorkflowTaskBatchOnce(1)
      },
      {
        name: "runActivityTaskOnce",
        claim: "claimActivityTask",
        foreignClaim: "claimWorkflowTask",
        drive: async (worker: Worker) => await worker.runActivityTaskOnce()
      },
      {
        name: "runActivityTaskBatchOnce",
        claim: "claimActivityTask",
        foreignClaim: "claimWorkflowTask",
        drive: async (worker: Worker) => await worker.runActivityTaskBatchOnce(1)
      }
    ] as const;

    for (const testCase of cases) {
      const calls: string[] = [];
      const backend = recordBackendCalls(new MemoryBackend(), calls);
      const registry = new Registry()
        .registerWorkflow(quoteWorkflow)
        .registerActivity(quoteActivity);
      const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
      const worker = new Worker({
        backend,
        registry,
        namespace: namespace(),
        workerId: `one-shot-${testCase.name}`,
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        payloadCodec: "Json"
      });
      await client.startWorkflow(
        quoteWorkflow,
        workflowId(`wf/one-shot-${testCase.name}`),
        "workflows",
        { sku: "sku-1" }
      );
      // Stage the activity task with a driver that is itself under test only for
      // the workflow cases, so each case starts from the same durable state.
      await worker.runWorkflowTaskOnce();
      calls.length = 0;

      await testCase.drive(worker);

      expect({
        driver: testCase.name,
        claims: calls.filter((call) => call === testCase.claim).length,
        foreignClaims: calls.filter((call) => call === testCase.foreignClaim).length,
        timerScans: calls.filter((call) => call === "fireDueTimers").length,
        timeoutScans: calls.filter((call) => call === "timeoutDueActivities").length
      }).toEqual({
        driver: testCase.name,
        claims: 1,
        foreignClaims: 0,
        timerScans: 0,
        timeoutScans: 0
      });
    }
  });

  it("keeps runActivityTimeoutMaintenanceOnce to a single timeout scan", async () => {
    const calls: string[] = [];
    const backend = recordBackendCalls(new MemoryBackend(), calls);
    const worker = new Worker({
      backend,
      registry: new Registry().registerWorkflow(echoWorkflow),
      namespace: namespace(),
      workerId: "one-shot-timeout-maintenance",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });

    await expect(worker.runActivityTimeoutMaintenanceOnce()).resolves.toEqual({ timedOut: 0 });

    // `currentTime` is the scan instant, read from the provider rather than
    // from `Date.now()` so a provider driving a virtual clock decides which
    // deadlines have lapsed. The claim of this test is the *one* scan, and it
    // still holds.
    expect(calls).toEqual(["currentTime", "timeoutDueActivities"]);
  });
});

// Row 4D, exactly-once ready-event consumption over the worker's real hot-wake
// deltas. A hot wake carries only the events since the last commit, so a
// completion consumed in one task is never redelivered — the composite that
// consumed it has to own it from then on.
describe("Worker hot join settlement", () => {
  const joinQuote = activity({
    name: "worker.join-quote",
    handler: async (input: { readonly cents: number }): Promise<{ readonly cents: number }> =>
      input
  });
  const joinWorkflow = workflow({
    name: "worker.join-across-tasks",
    version: 1,
    handler: async (input: { readonly seed: number }): Promise<{ readonly total: number }> => {
      const [first, second] = await joinAll([
        callActivity(joinQuote, { cents: input.seed }, { taskQueue: "activities" }),
        callActivity(joinQuote, { cents: input.seed * 2 }, { taskQueue: "activities" })
      ]);
      return {
        total:
          (first as { readonly cents: number }).cents +
          (second as { readonly cents: number }).cents
      };
    }
  });

  it("settles a joinAll whose branches complete in separate hot tasks", async () => {
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(joinWorkflow).registerActivity(joinQuote);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "join-across-tasks",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      joinWorkflow,
      workflowId("wf/worker-join-across-tasks"),
      "workflows",
      { seed: 5 }
    );

    // Schedules both branches.
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({ kind: "Committed" });
    // One branch completes and wakes a task that can record nothing: the join
    // is still pending. It must commit anyway rather than park until its lease
    // expires.
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({ kind: "Completed" });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    // The second branch completes in a later task. The first branch's
    // completion left the runtime's ready-event index two tasks ago and is not
    // in this wake's delta, so the join has to remember it.
    await expect(worker.runActivityTaskOnce()).resolves.toMatchObject({ kind: "Completed" });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    await expect(handle.result()).resolves.toEqual({ total: 15 });
  });
});

// The one absolute limit chunked replay introduces, and its repair.
//
// `getVersion` and friends return plain values rather than thenables, so they
// cannot suspend to wait for the next chunk; they spend from the window's
// reserve. A long enough unbroken run of them exhausts it. That is refused
// rather than mis-replayed — appending a duplicate marker the rest of history
// contradicts is the failure worth preventing — and then repaired by replaying
// the run once with no reserve limit.
describe("Worker replay window reserve", () => {
  const markerActivity = activity({
    name: "worker.marker-quote",
    handler: async (input: { readonly step: number }): Promise<{ readonly step: number }> => input
  });

  // Runs a long unbroken sequence of synchronous version markers *before* its
  // first awaited durable call, which is the shape that has no chunk boundary
  // to park at.
  function markerStormWorkflow(name: string, markers: number, swallow: boolean) {
    return workflow({
      name,
      version: 1,
      handler: async (input: { readonly seed: number }): Promise<{ readonly total: number }> => {
        let total = input.seed;
        for (let index = 0; index < markers; index += 1) {
          if (swallow) {
            // A workflow that hides the refusal must still not be able to
            // commit a task built on a marker the runtime could not verify.
            try {
              total += getVersion(`worker.change-${index}`, -1, 1);
            } catch {
              total += 1000;
            }
          } else {
            total += getVersion(`worker.change-${index}`, -1, 1);
          }
        }
        const quote = await callActivity(markerActivity, { step: 1 }, { taskQueue: "activities" });
        return { total: total + quote.step };
      }
    });
  }

  async function recordThenColdReplay(
    label: string,
    markers: number,
    swallow: boolean,
    historyFetchMaxEvents: number
  ): Promise<readonly string[]> {
    const definition = markerStormWorkflow(`worker.marker-storm-${label}`, markers, swallow);
    const backend = new MemoryBackend();
    const registry = new Registry().registerWorkflow(definition).registerActivity(markerActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const recorder = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: `marker-recorder-${label}`,
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    await client.startWorkflow(
      definition,
      workflowId(`wf/worker-marker-storm-${label}`),
      "workflows",
      { seed: 1 }
    );
    // Records the markers and the scheduled activity, then completes it so a
    // cold replay has to walk the whole marker run.
    await expect(recorder.runWorkflowTaskOnce()).resolves.toMatchObject({ kind: "Committed" });
    await expect(recorder.runActivityTaskOnce()).resolves.toMatchObject({ kind: "Completed" });

    const replayer = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: `marker-replayer-${label}`,
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      historyFetchMaxEvents,
      // Cold, with nothing carried over from the recording worker.
      workflowExecutionCacheSize: 0,
      workflowHistoryCacheBytes: 0,
      payloadCodec: "Json"
    });
    const outcome = await replayer.runWorkflowTaskOnce();
    expect(outcome).toMatchObject({ kind: "Committed", outcome: { kind: "Committed" } });
    if (outcome.kind !== "Committed") {
      throw new Error("expected a committed workflow task");
    }
    const history = await backend.streamHistory({
      runId: outcome.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(1_000_000),
      maxEvents: 100_000,
      maxBytes: 100_000_000
    });
    return history.events.map((event) => `${Number(event.eventId)}:${event.eventType}`);
  }

  it("repairs a marker run that outgrows the replay window reserve", async () => {
    // Comfortably past the reserve, and past the chunk quantisation on top of
    // it: the refusal point is "reserve, rounded up by however much the last
    // chunk overshot", so the limit is a floor rather than an exact number.
    const markers = REPLAY_WINDOW_LOOKAHEAD_EVENTS * 3;
    const chunked = await recordThenColdReplay("repair", markers, false, 16);
    const unchunked = await recordThenColdReplay("whole", markers, false, 100_000);

    // The repaired replay commits, and commits exactly what an unchunked one
    // does — no duplicate markers, no missing ones.
    expect(chunked).toEqual(unchunked);
    expect(chunked.filter((entry) => entry.endsWith(":VersionMarker"))).toHaveLength(markers);
    expect(chunked.at(-1)).toContain("WorkflowCompleted");
  }, 240_000);

  it("repairs it even when the workflow swallows the refusal", async () => {
    const markers = REPLAY_WINDOW_LOOKAHEAD_EVENTS * 3;
    const chunked = await recordThenColdReplay("swallowed", markers, true, 16);
    const unchunked = await recordThenColdReplay("swallowed-whole", markers, true, 100_000);

    // The latch is what makes this true. Catching the refusal lets the handler
    // run on with an unverified marker; without a refusal that survives the
    // `catch`, the task would commit a history with duplicate markers in it.
    expect(chunked).toEqual(unchunked);
    expect(chunked.filter((entry) => entry.endsWith(":VersionMarker"))).toHaveLength(markers);
  }, 240_000);

  it("replays a marker run that fits the reserve without repairing", async () => {
    // Well inside the reserve: the priming load hands the window enough command
    // events before the handler's first marker runs, so nothing is refused and
    // the chunked replay streams normally.
    const markers = 32;
    const chunked = await recordThenColdReplay("within", markers, false, 16);
    const unchunked = await recordThenColdReplay("within-whole", markers, false, 100_000);
    expect(chunked).toEqual(unchunked);
    expect(chunked.filter((entry) => entry.endsWith(":VersionMarker"))).toHaveLength(markers);
  }, 240_000);
});

// Row 4A. Cold replay used to accumulate every event through the replay target
// into one array and hand it to the execution, so replay memory scaled with
// history length and the README's "No Event History Limit" held only for the
// provider. These measure what the replay path actually retains.
describe("Worker replay memory", () => {
  // Split across the command event and the ready event on purpose: an
  // `ActivityScheduled` carries the input and stays in the replay window until
  // the workflow matches it, an `ActivityCompleted` carries the result and sits
  // in the ready-event index until the workflow consumes it. Loading both sides
  // heavily means the assertion below reacts to either one being retained.
  const memoryPayloadBytes = 4 * 1024;
  const memoryFiller = "f".repeat(memoryPayloadBytes);
  const memoryActivity = activity({
    name: "worker.memory-quote",
    handler: async (input: {
      readonly step: number;
      readonly filler: string;
    }): Promise<{ readonly step: number; readonly filler: string }> => ({
      step: input.step,
      filler: memoryFiller
    })
  });
  const memoryWorkflow = workflow({
    name: "worker.memory-replay",
    version: 1,
    handler: async (input: { readonly steps: number }): Promise<{ readonly total: number }> => {
      let total = 0;
      for (let step = 0; step < input.steps; step += 1) {
        const quote = await callActivity(
          memoryActivity,
          { step, filler: memoryFiller },
          { taskQueue: "activities" }
        );
        total += quote.step;
      }
      return { total };
    }
  });

  // Builds a long real history, then leaves the final workflow task unrun so a
  // fresh worker has to cold-replay all of it.
  async function buildLongHistory(steps: number, runs = 1): Promise<{
    readonly backend: MemoryBackend;
    readonly registry: Registry;
    readonly historyEvents: number;
  }> {
    const backend = new MemoryBackend();
    const registry = new Registry()
      .registerWorkflow(memoryWorkflow)
      .registerActivity(memoryActivity);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const builder = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "memory-builder",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    // With more than one run the workflows are given one more activity than the
    // build drives, so none of them reaches a terminal state and each is left
    // with a claimable task for a cold replay to pick up. A single run is
    // driven exactly to its last completion instead, which keeps
    // `historyEvents` exact for the callers that predict against it.
    const activitiesPerRun = runs === 1 ? steps : steps + 1;
    for (let run = 0; run < runs; run += 1) {
      await client.startWorkflow(
        memoryWorkflow,
        workflowId(`wf/worker-memory-replay-${run}`),
        "workflows",
        { steps: activitiesPerRun }
      );
    }
    // Driven in rounds — every claimable workflow task, then every claimable
    // activity — rather than as alternating single calls. Alternating leaves
    // exactly one workflow task pending at the end no matter how many runs
    // there are, because each iteration's workflow call consumes the task the
    // previous iteration's activity woke. Rounds leave one pending per run.
    const drain = async (once: () => Promise<{ readonly kind: string }>): Promise<void> => {
      for (let guard = 0; guard < runs * 4 + 8; guard += 1) {
        if ((await once()).kind === "NoTask") {
          return;
        }
      }
      throw new Error("benchmark fixture drained more tasks than it should have");
    };
    for (let round = 0; round < steps; round += 1) {
      await drain(() => builder.runWorkflowTaskOnce());
      await drain(() => builder.runActivityTaskOnce());
    }
    return { backend, registry, historyEvents: 1 + steps * 2 };
  }

  // Replays one long history at two chunk sizes and reads the answer off both.
  //
  // Two things have to hold, and one absolute number cannot show either. First,
  // retained bytes must track `historyFetchMaxEvents` — that is the gate's
  // actual wording, and a bulk load retains the same (large) amount at every
  // chunk size, so a chunk-size sweep is what distinguishes them. Second, each
  // measurement must sit near what the window *should* cost, because a partial
  // fix that keeps a few hundred matched events still lands under any bound
  // loose enough to be set by hand.
  async function measureColdReplayRetention(
    built: Awaited<ReturnType<typeof buildLongHistory>>,
    historyFetchMaxEvents: number
  ): Promise<number> {
    const samples: number[] = [];
    let chunks = 0;
    const expectedChunks = Math.trunc(built.historyEvents / historyFetchMaxEvents);
    const sampleAtChunks = new Set([
      Math.max(1, Math.trunc(expectedChunks / 2)),
      Math.max(1, Math.trunc(expectedChunks * 0.85))
    ]);
    const backend = materializeFreshHistoryChunks(built.backend, () => {
      chunks += 1;
      if (sampleAtChunks.has(chunks)) {
        samples.push(retainedBytes());
      }
    });
    const worker = new Worker({
      backend,
      registry: built.registry,
      namespace: namespace(),
      workerId: `memory-cold-replayer-${historyFetchMaxEvents}`,
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      historyFetchMaxEvents,
      // Measured with the worker's own history cache off. It is a separate,
      // explicitly byte-bounded retention (see the cache test below); leaving it
      // on would fold its budget into this number and measure two things at once.
      workflowHistoryCacheBytes: 0,
      payloadCodec: "Json"
    });
    const baseline = retainedBytes();
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    expect(samples.length).toBeGreaterThan(0);
    return Math.max(...samples) - baseline;
  }

  // What the replay window should cost at a given chunk size: the runtime
  // refills to `REPLAY_WINDOW_LOOKAHEAD_EVENTS` of reserve and pulls one chunk
  // at a time, and this fixture splits its payload evenly between the command
  // event that sits in the window and the ready event that sits in the index.
  function predictedWindowBytes(historyFetchMaxEvents: number): number {
    return 2 * (REPLAY_WINDOW_LOOKAHEAD_EVENTS + historyFetchMaxEvents) * memoryPayloadBytes;
  }

  it("holds replay memory proportional to historyFetchMaxEvents, not to history length", async () => {
    const steps = 1800;
    const smallChunk = 32;
    const largeChunk = 384;
    const totalPayloadBytes = steps * memoryPayloadBytes * 2;

    const retainedSmall = await measureColdReplayRetention(
      await buildLongHistory(steps),
      smallChunk
    );
    const retainedLarge = await measureColdReplayRetention(
      await buildLongHistory(steps),
      largeChunk
    );

    // The history is an order of magnitude past either window.
    expect(totalPayloadBytes).toBeGreaterThan(12 * 1024 * 1024);

    // Ceiling, from the pull invariant: the window never holds more than the
    // reserve plus one chunk. This is the edge that rejects a half-finished
    // drain — keeping even a hundred extra matched events at the small chunk
    // size breaks it.
    expect(retainedSmall).toBeLessThan(predictedWindowBytes(smallChunk) * 1.25);
    expect(retainedLarge).toBeLessThan(predictedWindowBytes(largeChunk) * 1.25);

    // Floor, from the refill invariant: every awaited durable call tops the
    // window back up to the reserve, so a correct replay is always holding at
    // least that much. Without this the assertions above would also pass on a
    // measurement that had collapsed to nothing.
    const reserveFloorBytes = 2 * REPLAY_WINDOW_LOOKAHEAD_EVENTS * memoryPayloadBytes * 0.8;
    expect(retainedSmall).toBeGreaterThan(reserveFloorBytes);
    expect(retainedLarge).toBeGreaterThan(reserveFloorBytes);

    // Proportionality itself, as a slope. Each extra event of chunk size buys
    // retained bytes; a replay bounded by the run rather than by the chunk
    // retains the same amount at both sizes and this collapses to zero.
    const bytesPerChunkEvent = (retainedLarge - retainedSmall) / (largeChunk - smallChunk);
    expect(bytesPerChunkEvent).toBeGreaterThan(memoryPayloadBytes * 0.25);
  }, 240_000);

  // Measured as the *difference* between two otherwise identical replay passes,
  // one with the cache budgeted and one with it switched off.
  //
  // A single before/after reading around one replay cannot do this job: the
  // replay frees as much fixture memory as the cache retains, so the number
  // drifts with ordering and has been observed negative, which a one-sided
  // "under the budget" assertion happily accepts. Differencing two passes
  // cancels everything except the cache and gives the assertion a floor as well
  // as a ceiling.
  //
  // Many small runs rather than one big one, deliberately. A single run whose
  // history exceeds the whole budget is dropped outright — the byte bound
  // refuses to evict everything else to hold one entry — which is correct but
  // leaves nothing to measure. The shape that exercises eviction is what a
  // worker actually sees: many runs that each fit, and together do not.
  it("keeps the workflow history cache inside its retained-bytes budget", async () => {
    const runs = 8;
    const steps = 24;
    const cacheBudget = 512 * 1024;
    const historyBytes = runs * steps * memoryPayloadBytes * 2;

    const replayRetaining = async (
      workflowHistoryCacheBytes: number,
      label: string
    ): Promise<number> => {
      const built = await buildLongHistory(steps, runs);
      const worker = new Worker({
        backend: materializeFreshHistoryChunks(built.backend, () => undefined),
        registry: built.registry,
        namespace: namespace(),
        workerId: `memory-cache-${label}`,
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        historyFetchMaxEvents: 32,
        // Far more entries than the byte budget can hold, so the byte bound is
        // what has to do the work.
        workflowHistoryCacheSize: 1024,
        // A cold replay per run: the execution cache must not keep them hot, or
        // the history cache is never exercised.
        workflowExecutionCacheSize: 0,
        workflowHistoryCacheBytes,
        payloadCodec: "Json"
      });
      const baseline = retainedBytes();
      // Drains whatever the build left claimable. Counted rather than assumed:
      // the build round-robins, so how the activities landed across the runs is
      // not fixed, and only "several runs were cold-replayed" matters here.
      let commits = 0;
      for (;;) {
        const outcome = await worker.runWorkflowTaskOnce();
        if (outcome.kind === "NoTask") {
          break;
        }
        expect(outcome).toMatchObject({ kind: "Committed", outcome: { kind: "Committed" } });
        commits += 1;
      }
      const retained = retainedBytes() - baseline;
      // Referenced after the measurement so the worker and its cache cannot be
      // collected before it is taken.
      expect(commits).toBeGreaterThanOrEqual(runs);
      return retained;
    };

    const withoutCache = await replayRetaining(0, "off");
    const withBudget = await replayRetaining(cacheBudget, "budgeted");
    const cacheRetained = withBudget - withoutCache;

    // Together the runs are several times the budget, so an entry-count bound
    // alone would have kept all of them.
    expect(historyBytes).toBeGreaterThan(cacheBudget * 2);
    // Floor: the cache really did retain something, so the measurement is live
    // rather than collapsed.
    expect(cacheRetained).toBeGreaterThan(cacheBudget * 0.25);
    // Ceiling: and it stayed inside its budget, with room for the per-entry
    // bookkeeping the budget does not count.
    expect(cacheRetained).toBeLessThan(cacheBudget * 2);
  }, 240_000);

  it("commits the same events whether history arrives in one chunk or many", async () => {
    const steps = 40;
    const recorded: string[][] = [];
    for (const historyFetchMaxEvents of [4096, 1]) {
      const built = await buildLongHistory(steps);
      const worker = new Worker({
        backend: built.backend,
        registry: built.registry,
        namespace: namespace(),
        workerId: `memory-chunking-${historyFetchMaxEvents}`,
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        historyFetchMaxEvents,
        workflowExecutionCacheSize: 0,
        payloadCodec: "Json"
      });
      const outcome = await worker.runWorkflowTaskOnce();
      expect(outcome).toMatchObject({ kind: "Committed", outcome: { kind: "Committed" } });
      if (outcome.kind !== "Committed") {
        throw new Error("expected a committed workflow task");
      }
      const history = await built.backend.streamHistory({
        runId: outcome.runId,
        afterEventId: eventId(0),
        upToEventId: eventId(1_000_000),
        maxEvents: 10_000,
        maxBytes: 100_000_000
      });
      recorded.push(
        history.events.map(
          (event) => `${Number(event.eventId)}:${event.eventType}:${payloadFingerprint(event)}`
        )
      );
    }
    // Identical committed history down to the payload bytes: the chunk size is
    // a memory knob and must not be observable in what a run records.
    expect(recorded[1]).toEqual(recorded[0]);
    expect(recorded[0]?.length).toBe(1 + steps * 2 + 1);
  }, 120_000);
});

// A stable digest of everything a recorded event carries, so the chunked and
// unchunked replays are compared on their bytes rather than on their shapes.
function payloadFingerprint(event: HistoryEvent): string {
  return JSON.stringify(event.data, (_key, value: unknown) =>
    value instanceof Uint8Array ? Array.from(value) : value
  );
}

/**
 * Wraps a backend so every streamed chunk and every claim carries freshly
 * materialized events, the way a real provider that decodes rows does.
 *
 * Without this a memory assertion would be vacuous: an in-process backend hands
 * back references to the events it already holds, so retaining all of them
 * costs nothing extra and a bulk load looks identical to a chunked one.
 */
function materializeFreshHistoryChunks(
  inner: DurableBackend,
  onChunk: () => void
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "claimWorkflowTask") {
        return async (...args: Parameters<DurableBackend["claimWorkflowTask"]>) => {
          const claimed = await target.claimWorkflowTask(...args);
          return claimed === null
            ? null
            : { ...claimed, prefetchedHistory: claimed.prefetchedHistory.slice(0, 1).map(cloneHistoryEvent) };
        };
      }
      if (property === "streamHistory") {
        return async (...args: Parameters<DurableBackend["streamHistory"]>) => {
          const chunk = await target.streamHistory(...args);
          onChunk();
          return { ...chunk, events: chunk.events.map(cloneHistoryEvent) };
        };
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
}

function cloneHistoryEvent(event: HistoryEvent): HistoryEvent {
  return { ...event, data: clonePayloadsDeep(event.data) as HistoryEvent["data"] };
}

function clonePayloadsDeep(value: unknown): unknown {
  if (value === null || typeof value !== "object") {
    return value;
  }
  if (value instanceof Uint8Array) {
    return value.slice();
  }
  if (Array.isArray(value)) {
    return value.map(clonePayloadsDeep);
  }
  const cloned: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    cloned[key] = clonePayloadsDeep(item);
  }
  return cloned;
}

// Retained bytes after a forced full GC. `heapUsed` alone would miss the
// payloads entirely: their `Uint8Array` backing stores are external memory, so
// a test watching only the JS heap reports a flat line either way. Triggered
// through `vm` so the assertion works under the project's normal test command
// rather than requiring `--expose-gc` on the runner.
const forceGarbageCollection: () => void = (() => {
  v8.setFlagsFromString("--expose-gc");
  try {
    return vm.runInNewContext("gc") as () => void;
  } finally {
    v8.setFlagsFromString("--no-expose-gc");
  }
})();

function retainedBytes(): number {
  forceGarbageCollection();
  forceGarbageCollection();
  forceGarbageCollection();
  const usage = process.memoryUsage();
  return usage.heapUsed + usage.arrayBuffers;
}

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

function failBackendCall<K extends keyof DurableBackend>(
  inner: DurableBackend,
  method: K,
  error: Error
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === method) {
        return async () => {
          throw error;
        };
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
}

// Adds the optional claimWorkflowTasks batch API to a single-claim backend by
// looping claimWorkflowTask, forcing the given types past the worker's
// registry-derived claim filter so unregistered claims can reach the drain.
function withSequentialBatchClaims(
  inner: DurableBackend,
  workflowTypes: Parameters<DurableBackend["claimWorkflowTask"]>[1]["registeredWorkflowTypes"]
): DurableBackend {
  const claimWorkflowTasks = async (
    workerId: Parameters<DurableBackend["claimWorkflowTask"]>[0],
    opts: Parameters<DurableBackend["claimWorkflowTask"]>[1] & { readonly limit: number }
  ) => {
    const claimed = [];
    for (let index = 0; index < Math.max(1, Math.trunc(opts.limit)); index += 1) {
      const task = await inner.claimWorkflowTask(workerId, {
        ...opts,
        registeredWorkflowTypes: workflowTypes
      });
      if (task === null) {
        break;
      }
      claimed.push(task);
    }
    return claimed;
  };
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "claimWorkflowTasks") {
        return claimWorkflowTasks;
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
}

function forceWorkflowClaimTypes(
  inner: DurableBackend,
  workflowTypes: Parameters<DurableBackend["claimWorkflowTask"]>[1]["registeredWorkflowTypes"]
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "claimWorkflowTask") {
        return async (
          workerId: Parameters<DurableBackend["claimWorkflowTask"]>[0],
          opts: Parameters<DurableBackend["claimWorkflowTask"]>[1]
        ) => target.claimWorkflowTask(workerId, { ...opts, registeredWorkflowTypes: workflowTypes });
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
}

function forceActivityClaimNames(
  inner: DurableBackend,
  activityNames: Parameters<DurableBackend["claimActivityTask"]>[1]["registeredActivityNames"]
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "claimActivityTask") {
        return async (
          workerId: Parameters<DurableBackend["claimActivityTask"]>[0],
          opts: Parameters<DurableBackend["claimActivityTask"]>[1]
        ) => target.claimActivityTask(workerId, { ...opts, registeredActivityNames: activityNames });
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
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

function conflictWorkflowCompletionOnce(
  inner: DurableBackend,
  shouldConflict: () => boolean
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "commitWorkflowTask") {
        return async (...args: Parameters<DurableBackend["commitWorkflowTask"]>) => {
          const commit = args[1];
          const completing = (commit.appendEvents ?? [])
            .some((event) => event.data.kind === "WorkflowCompleted");
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

// A workflow that parks on an activity and records whichever disposal reason
// the runtime raises into that waiter, so a test pins the exact call site that
// abandoned it. Re-running the run from history takes the normal path.
function disposalTracingHandler(
  trace: string[]
): (input: { readonly sku: string }) => Promise<{ readonly cents: number }> {
  return async (input) => {
    trace.push(`start:${input.sku}`);
    let quote: { readonly cents: number };
    try {
      quote = await callActivity(quoteActivity, { sku: input.sku }, { taskQueue: "activities" });
    } catch (error) {
      trace.push(
        error instanceof HotWorkflowExecutionDisposedError
          ? `waiter:${error.reason}`
          : `waiter:unexpected:${String(error)}`
      );
      throw error;
    }
    trace.push(`after:${quote.cents}`);
    return { cents: quote.cents };
  };
}

// Conflicts the commit that schedules an activity, so the abandoned execution
// is parked on a durable-API waiter rather than already terminal.
function conflictActivitySchedulingOnce(
  inner: DurableBackend,
  shouldConflict: () => boolean
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "commitWorkflowTask") {
        return async (...args: Parameters<DurableBackend["commitWorkflowTask"]>) => {
          const scheduling = (args[1].scheduleActivities ?? []).length > 0;
          if (scheduling && shouldConflict()) {
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

// Throws out of the commit call itself, which is the post-prepare failure path:
// the execution is prepared and parked, and its task dies before any commit.
function failActivitySchedulingCommitOnce(
  inner: DurableBackend,
  shouldFail: () => boolean
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "commitWorkflowTask") {
        return async (...args: Parameters<DurableBackend["commitWorkflowTask"]>) => {
          const scheduling = (args[1].scheduleActivities ?? []).length > 0;
          if (scheduling && shouldFail()) {
            throw new Error("commit transport failed");
          }
          return await target.commitWorkflowTask(...args);
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

// Like `failBackendCall`, but the rejection lands only after `delayMs`, so the
// call is still outstanding while another loop fails.
function failBackendCallAfter<K extends keyof DurableBackend>(
  inner: DurableBackend,
  method: K,
  error: Error,
  delayMs: number
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === method) {
        return async () => {
          await new Promise<void>((resolve) => setTimeout(resolve, delayMs));
          throw error;
        };
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
}

// Records the wall-clock instant of every maintenance scan so a test can assert
// the cadence itself, not just the call count.
function recordMaintenanceScans(
  inner: DurableBackend,
  timerScans: number[],
  timeoutScans: number[]
): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      if (property === "fireDueTimers") {
        return async (...args: Parameters<DurableBackend["fireDueTimers"]>) => {
          timerScans.push(Date.now());
          return await target.fireDueTimers(...args);
        };
      }
      if (property === "timeoutDueActivities") {
        return async (...args: Parameters<DurableBackend["timeoutDueActivities"]>) => {
          timeoutScans.push(Date.now());
          return await target.timeoutDueActivities(...args);
        };
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  }) as DurableBackend;
}

// Appends the name of every backend method the worker calls, which is how the
// one-shot driver tests pin their single-pass shape and how the shutdown test
// proves no loop outlived `run()`.
function recordBackendCalls(inner: DurableBackend, calls: string[]): DurableBackend {
  return new Proxy(inner, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver);
      if (typeof value !== "function" || typeof property !== "string") {
        return value;
      }
      // Mutable `unknown[]`, not `readonly unknown[]`: a rest parameter is
      // always a fresh array, and `Function.prototype.apply` is typed to take
      // one under `strictBindCallApply`.
      return (...args: unknown[]) => {
        calls.push(property);
        return (value as (...callArgs: unknown[]) => unknown).apply(target, args);
      };
    }
  }) as DurableBackend;
}

function deferred<T = void>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

// Resolves to `timedOutValue` if `promise` has not settled in `timeoutMs`, so a
// coupling regression reports the invariant it broke instead of a bare timeout.
async function raceDeadline<T>(
  promise: Promise<T>,
  timeoutMs: number,
  timedOutValue: T
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<T>((resolve) => {
    timer = setTimeout(() => resolve(timedOutValue), timeoutMs);
  });
  try {
    return await Promise.race([promise, deadline]);
  } finally {
    clearTimeout(timer);
  }
}

async function flushUnhandledRejectionTurn(): Promise<void> {
  await Promise.resolve();
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}
