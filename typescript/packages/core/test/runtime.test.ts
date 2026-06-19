import { describe, expect, it } from "vitest";
import {
  MemoryBackend,
  ActivityFailureError,
  DEFAULT_VERSION,
  RetryPolicy,
  UnsupportedWorkflowVersionError,
  activity,
  activityMapManifest,
  callActivity,
  childWorkflowMap,
  continueAsNew,
  decodePayload,
  deprecatePatch,
  encodePayload,
  eventId,
  getVersion,
  join,
  joinAll,
  patched,
  namespace,
  prepareWorkflowTaskCommit,
  runId,
  select,
  selectAll,
  sideEffect,
  sleep,
  sleepUntil,
  signal,
  signalId,
  taskQueue,
  workflow,
  workflowId,
  workflowType,
  type ClaimedWorkflowTask
} from "@durust/core";

interface QuoteInput {
  readonly sku: string;
}

interface QuoteOutput {
  readonly cents: number;
}

interface CheckoutInput {
  readonly sku: string;
}

interface ApprovalSignal {
  readonly approvalId: string;
}

type TestNoInput = {};

const priceQuote = activity({
  name: "payments.price-quote",
  handler: async (input: QuoteInput): Promise<QuoteOutput> => ({
    cents: input.sku.length
  })
});

const versionActivityA = activity({
  name: "tests.version-a",
  handler: async (_input: {}): Promise<string> => "a"
});

const versionActivityB = activity({
  name: "tests.version-b",
  handler: async (_input: {}): Promise<string> => "b"
});

const fakeClaimed: ClaimedWorkflowTask = {
  runId: runId("run-1"),
  workflowId: workflowId("wf/checkout"),
  workflowType: workflowType("orders.checkout", 1),
  claim: {
    runId: runId("run-1"),
    workerId: "worker-a",
    token: 1
  },
  replayTargetEventId: eventId(1),
  reason: "WorkflowStarted",
  prefetchedHistory: []
};

describe("minimal workflow runtime", () => {
  it("rejects durable activity awaits outside workflow context", async () => {
    await expect(Promise.resolve(callActivity(priceQuote, { sku: "sku-1" }))).rejects.toThrow(
      "durust durable APIs must be awaited inside a workflow task"
    );
  });

  it("prepares a deterministic activity schedule commit when an activity is awaited", async () => {
    const checkout = workflow({
      name: "orders.checkout",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly unreachable: true }> => {
        await callActivity(priceQuote, { sku: input.sku }, {
          taskQueue: "payments",
          retry: RetryPolicy.exponential({ maxAttempts: 5 })
        });
        return { unreachable: true };
      }
    });

    const first = await prepareWorkflowTaskCommit(checkout, { sku: "sku-1" }, fakeClaimed, {
      payloadCodec: "Json"
    });
    const second = await prepareWorkflowTaskCommit(checkout, { sku: "sku-1" }, fakeClaimed, {
      payloadCodec: "Json"
    });

    expect(first).toEqual(second);
    expect(first.expectedTailEventId).toBe(eventId(1));
    expect(first.appendEvents).toHaveLength(1);
    expect(first.scheduleActivities).toHaveLength(1);

    const scheduledEvent = first.appendEvents?.[0]?.data;
    expect(scheduledEvent?.kind).toBe("ActivityScheduled");
    if (scheduledEvent?.kind !== "ActivityScheduled") {
      throw new Error("expected ActivityScheduled event");
    }
    expect(scheduledEvent.scheduled.commandId).toEqual({
      runId: runId("run-1"),
      seq: 1
    });
    expect(scheduledEvent.scheduled.activityName).toBe("payments.price-quote");
    expect(scheduledEvent.scheduled.taskQueue).toBe("payments");
    expect(scheduledEvent.scheduled.retryPolicy.maxAttempts).toBe(5);
    expect(scheduledEvent.scheduled.fingerprint).toMatchObject({
      kind: "Activity",
      name: "payments.price-quote"
    });
    expect(decodePayload<QuoteInput>(scheduledEvent.scheduled.input)).toEqual({ sku: "sku-1" });
    expect(first.scheduleActivities?.[0]).toMatchObject({
      activityId: "run-1:1",
      activityName: "payments.price-quote",
      taskQueue: "payments",
      attempt: 1
    });
  });

  it("commits prepared activity schedules through MemoryBackend history", async () => {
    const checkout = workflow({
      name: "orders.checkout",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly unreachable: true }> => {
        await callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" });
        return { unreachable: true };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/runtime"),
      workflowType: checkout.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ sku: "sku-1" }, { codec: "Json" })
    });
    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [checkout.workflowType],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected claim");
    }

    const commit = await prepareWorkflowTaskCommit(checkout, { sku: "sku-1" }, claimed, {
      payloadCodec: "Json"
    });
    const outcome = await backend.commitWorkflowTask(claimed.claim, commit);
    expect(outcome).toEqual({ kind: "Committed", newTailEventId: eventId(2) });

    const history = await backend.streamHistory({
      runId: claimed.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled"
    ]);
  });

  it("records child workflow map fingerprints and rejects changed options on replay", async () => {
    const child = workflow({
      name: "orders.child-map-item",
      version: 1,
      handler: async (input: { readonly orderId: string }): Promise<{ readonly orderId: string }> =>
        input
    });
    const inputManifest = activityMapManifest([{ orderId: "o-1" }]);
    const parent = workflow({
      name: "orders.child-map-parent",
      version: 1,
      handler: async (_input: {}): Promise<void> => {
        const mapped = childWorkflowMap(child, {
          inputManifest,
          resultManifest: "shipments",
          workflowIdPrefix: "ship",
          taskQueue: "children",
          maxInFlight: 8,
          parentClosePolicy: "Cancel",
          failureMode: "CollectAll"
        });
        await mapped.resultManifest();
      }
    });

    const first = await prepareWorkflowTaskCommit(parent, {}, fakeClaimed, {
      payloadCodec: "Json"
    });
    expect(first.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ChildWorkflowMapScheduled"
    ]);
    expect(first.scheduleChildWorkflowMaps).toHaveLength(1);
    const scheduled = first.appendEvents?.[0]?.data;
    if (scheduled?.kind !== "ChildWorkflowMapScheduled") {
      throw new Error("expected ChildWorkflowMapScheduled");
    }
    expect(scheduled.scheduled.fingerprint).toMatchObject({
      kind: "ChildWorkflowMap",
      name: "orders.child-map-item@1"
    });

    const changedParent = workflow({
      name: "orders.child-map-parent",
      version: 1,
      handler: async (_input: {}): Promise<void> => {
        const mapped = childWorkflowMap(child, {
          inputManifest,
          resultManifest: "shipments-v2",
          workflowIdPrefix: "ship",
          taskQueue: "children",
          maxInFlight: 8,
          parentClosePolicy: "Cancel",
          failureMode: "CollectAll"
        });
        await mapped.resultManifest();
      }
    });
    const replayClaim: ClaimedWorkflowTask = {
      ...fakeClaimed,
      workflowType: parent.workflowType,
      replayTargetEventId: eventId(2),
      prefetchedHistory: [
        {
          eventId: eventId(1),
          eventType: "WorkflowStarted",
          data: {
            kind: "WorkflowStarted",
            workflowType: parent.workflowType,
            input: encodePayload({}, { codec: "Json" })
          }
        },
        {
          eventId: eventId(2),
          eventType: "ChildWorkflowMapScheduled",
          data: scheduled
        }
      ]
    };

    await expect(
      prepareWorkflowTaskCommit(changedParent, {}, replayClaim, { payloadCodec: "Json" })
    ).rejects.toThrow("nondeterminism: child workflow map command fingerprint changed");
  });

  it("replays a completed activity and resumes the workflow to completion", async () => {
    const checkout = workflow({
      name: "orders.checkout",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly cents: number }> => {
        const quote = await callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" });
        return { cents: quote.cents };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/activity-complete"),
      workflowType: checkout.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ sku: "sku-1" }, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [checkout.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const scheduleCommit = await prepareWorkflowTaskCommit(checkout, { sku: "sku-1" }, firstClaim, {
      payloadCodec: "Json"
    });
    await backend.commitWorkflowTask(firstClaim.claim, scheduleCommit);

    const activityTask = await backend.claimActivityTask("activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("payments"),
      registeredActivityNames: ["payments.price-quote"],
      leaseDurationMs: 30_000
    });
    expect(activityTask).not.toBeNull();
    if (!activityTask) {
      throw new Error("expected activity task");
    }
    expect(decodePayload<QuoteInput>(activityTask.task.input)).toEqual({ sku: "sku-1" });
    await backend.completeActivity({
      claim: activityTask.claim,
      result: encodePayload<QuoteOutput>({ cents: 1234 }, { codec: "Json" })
    });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [checkout.workflowType],
      leaseDurationMs: 30_000
    });
    expect(secondClaim).not.toBeNull();
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const completionCommit = await prepareWorkflowTaskCommit(
      checkout,
      { sku: "sku-1" },
      secondClaim,
      { payloadCodec: "Json" }
    );
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    await backend.commitWorkflowTask(secondClaim.claim, completionCommit);

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled",
      "ActivityCompleted",
      "WorkflowCompleted"
    ]);
  });

  it("replays a failed activity as a catchable workflow error", async () => {
    const failingWorkflow = workflow({
      name: "orders.activity-failure",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly failure: string }> => {
        try {
          await callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" });
          return { failure: "none" };
        } catch (error) {
          if (error instanceof ActivityFailureError) {
            return { failure: error.failure.message };
          }
          throw error;
        }
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/activity-failure"),
      workflowType: failingWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ sku: "sku-1" }, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [failingWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const scheduleCommit = await prepareWorkflowTaskCommit(
      failingWorkflow,
      { sku: "sku-1" },
      firstClaim,
      { payloadCodec: "Json" }
    );
    await backend.commitWorkflowTask(firstClaim.claim, scheduleCommit);

    const activityTask = await backend.claimActivityTask("activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("payments"),
      registeredActivityNames: ["payments.price-quote"],
      leaseDurationMs: 30_000
    });
    if (!activityTask) {
      throw new Error("expected activity task");
    }
    await backend.failActivity({
      claim: activityTask.claim,
      failure: {
        errorType: "test.failure",
        message: "quote failed",
        nonRetryable: false
      }
    });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [failingWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    expect(secondClaim.reason).toBe("ActivityFailed");
    const completionCommit = await prepareWorkflowTaskCommit(
      failingWorkflow,
      { sku: "sku-1" },
      secondClaim,
      { payloadCodec: "Json" }
    );
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    await backend.commitWorkflowTask(secondClaim.claim, completionCommit);

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled",
      "ActivityFailed",
      "WorkflowCompleted"
    ]);
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(completed.result)).toEqual({ failure: "quote failed" });
  });

  it("prepares a workflow completion event when the handler returns without durable waits", async () => {
    const immediate = workflow({
      name: "orders.immediate",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly sku: string }> => ({
        sku: input.sku
      })
    });

    const commit = await prepareWorkflowTaskCommit(immediate, { sku: "sku-1" }, fakeClaimed, {
      payloadCodec: "Json"
    });

    expect(commit.appendEvents).toHaveLength(1);
    const completed = commit.appendEvents?.[0]?.data;
    expect(completed?.kind).toBe("WorkflowCompleted");
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted event");
    }
    expect(decodePayload(completed.result)).toEqual({ sku: "sku-1" });
  });

  it("continues as new by closing the current run and making a compacted run claimable", async () => {
    const continuingWorkflow = workflow({
      name: "tests.continue-as-new",
      version: 1,
      handler: async (input: { readonly count: number }): Promise<{ readonly count: number }> => {
        if (input.count < 1) {
          return continueAsNew({ count: input.count + 1 });
        }
        return { count: input.count };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/continue-as-new"),
      workflowType: continuingWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ count: 0 }, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [continuingWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const continuedCommit = await prepareWorkflowTaskCommit(
      continuingWorkflow,
      { count: 0 },
      firstClaim,
      { payloadCodec: "Json" }
    );
    expect(continuedCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowContinuedAsNew"
    ]);
    const continued = continuedCommit.appendEvents?.[0]?.data;
    if (continued?.kind !== "WorkflowContinuedAsNew") {
      throw new Error("expected WorkflowContinuedAsNew");
    }
    expect(decodePayload(continued.input)).toEqual({ count: 1 });
    await backend.commitWorkflowTask(firstClaim.claim, continuedCommit);

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [continuingWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    expect(secondClaim.runId).not.toBe(firstClaim.runId);
    expect(secondClaim.reason).toBe("WorkflowStarted");
    const secondStarted = secondClaim.prefetchedHistory[0]?.data;
    if (secondStarted?.kind !== "WorkflowStarted") {
      throw new Error("expected second WorkflowStarted");
    }
    expect(decodePayload(secondStarted.input)).toEqual({ count: 1 });

    const completionCommit = await prepareWorkflowTaskCommit(
      continuingWorkflow,
      { count: 1 },
      secondClaim,
      { payloadCodec: "Json" }
    );
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    await backend.commitWorkflowTask(secondClaim.claim, completionCommit);

    const firstHistory = await backend.streamHistory({
      runId: firstClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(firstHistory.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "WorkflowContinuedAsNew"
    ]);

    const secondHistory = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(secondHistory.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "WorkflowCompleted"
    ]);
    const secondCompleted = secondHistory.events.at(-1)?.data;
    if (secondCompleted?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(secondCompleted.result)).toEqual({ count: 1 });
  });

  it("prepares a deterministic timer wait when sleepUntil is awaited", async () => {
    const reminder = workflow({
      name: "orders.reminder",
      version: 1,
      handler: async (input: { readonly deadlineMs: number }): Promise<{ readonly done: true }> => {
        await sleepUntil(input.deadlineMs);
        return { done: true };
      }
    });

    const first = await prepareWorkflowTaskCommit(
      reminder,
      { deadlineMs: 1_000 },
      fakeClaimed,
      { payloadCodec: "Json" }
    );
    const second = await prepareWorkflowTaskCommit(
      reminder,
      { deadlineMs: 1_000 },
      fakeClaimed,
      { payloadCodec: "Json" }
    );

    expect(first).toEqual(second);
    expect(first.appendEvents?.map((event) => event.data.kind)).toEqual(["TimerStarted"]);
    expect(first.upsertWaits).toEqual([
      {
        waitId: "run-1:timer:1",
        runId: runId("run-1"),
        commandId: { runId: runId("run-1"), seq: 1 },
        kind: "Timer",
        key: "timer",
        readyAt: 1_000
      }
    ]);
    const started = first.appendEvents?.[0]?.data;
    if (started?.kind !== "TimerStarted") {
      throw new Error("expected TimerStarted");
    }
    expect(started.started.fingerprint).toEqual({
      kind: "Timer",
      name: "sleep_until",
      inputDigest: null,
      optionsDigest: "timestamp-ms:1000"
    });
  });

  it("uses deterministic runtime time for relative sleep", async () => {
    const relative = workflow({
      name: "orders.relative-sleep",
      version: 1,
      handler: async (_input: {}): Promise<void> => {
        await sleep(250);
      }
    });

    const commit = await prepareWorkflowTaskCommit(relative, {}, fakeClaimed, {
      nowMs: 10_000
    });

    expect(commit.upsertWaits?.[0]?.readyAt).toBe(10_250);
    const started = commit.appendEvents?.[0]?.data;
    if (started?.kind !== "TimerStarted") {
      throw new Error("expected TimerStarted");
    }
    expect(started.started.fingerprint).toEqual({
      kind: "Timer",
      name: "sleep",
      inputDigest: null,
      optionsDigest: "timestamp-ms:250"
    });
  });

  it("fires a due timer and resumes the workflow to completion", async () => {
    const reminder = workflow({
      name: "orders.reminder",
      version: 1,
      handler: async (input: { readonly deadlineMs: number }): Promise<{ readonly done: true }> => {
        await sleepUntil(input.deadlineMs);
        return { done: true };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/timer"),
      workflowType: reminder.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ deadlineMs: 1_000 }, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [reminder.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const timerCommit = await prepareWorkflowTaskCommit(
      reminder,
      { deadlineMs: 1_000 },
      firstClaim,
      { payloadCodec: "Json" }
    );
    await backend.commitWorkflowTask(firstClaim.claim, timerCommit);

    expect(await backend.fireDueTimers({ namespace: namespace(), now: 999, limit: 16 })).toEqual({
      fired: 0
    });
    expect(await backend.fireDueTimers({ namespace: namespace(), now: 1_000, limit: 16 })).toEqual({
      fired: 1
    });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [reminder.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    expect(secondClaim.reason).toBe("TimerFired");

    const completionCommit = await prepareWorkflowTaskCommit(
      reminder,
      { deadlineMs: 1_000 },
      secondClaim,
      { payloadCodec: "Json" }
    );
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    await backend.commitWorkflowTask(secondClaim.claim, completionCommit);

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "TimerStarted",
      "TimerFired",
      "WorkflowCompleted"
    ]);
  });

  it("registers a signal wait when no signal is available", async () => {
    const approvalWorkflow = workflow({
      name: "orders.approval",
      version: 1,
      handler: async (_input: {}): Promise<void> => {
        await signal<ApprovalSignal>("approved");
      }
    });

    const commit = await prepareWorkflowTaskCommit(approvalWorkflow, {}, fakeClaimed, {
      payloadCodec: "Json"
    });

    expect(commit.appendEvents).toEqual([]);
    expect(commit.upsertWaits).toEqual([
      {
        waitId: "run-1:signal:1",
        runId: runId("run-1"),
        commandId: { runId: runId("run-1"), seq: 1 },
        kind: "Signal",
        key: "approved",
        readyAt: null
      }
    ]);
  });

  it("consumes a live signal and resumes the workflow to completion", async () => {
    const approvalWorkflow = workflow({
      name: "orders.approval",
      version: 1,
      handler: async (_input: {}): Promise<{ readonly approvalId: string }> => {
        const approval = await signal<ApprovalSignal>("approved");
        return { approvalId: approval.approvalId };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/signal"),
      workflowType: approvalWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [approvalWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const waitCommit = await prepareWorkflowTaskCommit(approvalWorkflow, {}, firstClaim, {
      payloadCodec: "Json"
    });
    await backend.commitWorkflowTask(firstClaim.claim, waitCommit);

    expect(
      await backend.signalWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/signal"),
        signalId: signalId("sig-1"),
        signalName: "approved",
        payload: encodePayload<ApprovalSignal>({ approvalId: "a-1" }, { codec: "Json" })
      })
    ).toEqual({ kind: "Accepted" });
    expect(
      await backend.signalWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/signal"),
        signalId: signalId("sig-1"),
        signalName: "approved",
        payload: encodePayload<ApprovalSignal>({ approvalId: "a-1" }, { codec: "Json" })
      })
    ).toEqual({ kind: "Duplicate" });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [approvalWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    expect(secondClaim.reason).toBe("SignalReceived");
    const liveSignal = await backend.readSignalInbox({
      runId: secondClaim.runId,
      signalName: "approved"
    });
    expect(liveSignal).not.toBeNull();
    if (!liveSignal) {
      throw new Error("expected live signal");
    }
    expect(decodePayload<ApprovalSignal>(liveSignal.payload)).toEqual({ approvalId: "a-1" });

    const consumeCommit = await prepareWorkflowTaskCommit(approvalWorkflow, {}, secondClaim, {
      payloadCodec: "Json",
      liveSignals: [liveSignal]
    });
    expect(consumeCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "SignalConsumed",
      "WorkflowCompleted"
    ]);
    expect(consumeCommit.consumeSignals).toEqual(["sig-1"]);
    expect(consumeCommit.deleteWaits).toEqual(["run-1:signal:1"]);
    await backend.commitWorkflowTask(secondClaim.claim, consumeCommit);

    expect(
      await backend.readSignalInbox({ runId: secondClaim.runId, signalName: "approved" })
    ).toBeNull();

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "SignalConsumed",
      "WorkflowCompleted"
    ]);
  });

  it("registers join branches in deterministic object order", async () => {
    const joinedWorkflow = workflow({
      name: "orders.join",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly unreachable: true }> => {
        await join({
          quote: callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" }),
          delay: sleepUntil(1_000)
        });
        return { unreachable: true };
      }
    });

    const commit = await prepareWorkflowTaskCommit(joinedWorkflow, { sku: "sku-1" }, fakeClaimed, {
      payloadCodec: "Json"
    });

    expect(commit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled",
      "TimerStarted"
    ]);
    expect(commit.scheduleActivities).toHaveLength(1);
    expect(commit.upsertWaits).toEqual([
      {
        waitId: "run-1:timer:2",
        runId: runId("run-1"),
        commandId: { runId: runId("run-1"), seq: 2 },
        kind: "Timer",
        key: "timer",
        readyAt: 1_000
      }
    ]);
  });

  it("replays join completions even when terminal events arrive after all schedule events", async () => {
    const joinedWorkflow = workflow({
      name: "orders.join-complete",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly cents: number }> => {
        const result = await join({
          quote: callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" }),
          delay: sleepUntil(1_000)
        });
        return { cents: result.quote.cents };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/join"),
      workflowType: joinedWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ sku: "sku-1" }, { codec: "Json" })
    });
    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [joinedWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const waitCommit = await prepareWorkflowTaskCommit(joinedWorkflow, { sku: "sku-1" }, firstClaim, {
      payloadCodec: "Json"
    });
    await backend.commitWorkflowTask(firstClaim.claim, waitCommit);

    const activityTask = await backend.claimActivityTask("activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("payments"),
      registeredActivityNames: ["payments.price-quote"],
      leaseDurationMs: 30_000
    });
    if (!activityTask) {
      throw new Error("expected activity task");
    }
    await backend.completeActivity({
      claim: activityTask.claim,
      result: encodePayload<QuoteOutput>({ cents: 4321 }, { codec: "Json" })
    });
    await backend.fireDueTimers({ namespace: namespace(), now: 1_000, limit: 16 });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [joinedWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const completionCommit = await prepareWorkflowTaskCommit(
      joinedWorkflow,
      { sku: "sku-1" },
      secondClaim,
      { payloadCodec: "Json" }
    );
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    await backend.commitWorkflowTask(secondClaim.claim, completionCommit);

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled",
      "TimerStarted",
      "ActivityCompleted",
      "TimerFired",
      "WorkflowCompleted"
    ]);
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(completed.result)).toEqual({ cents: 4321 });
  });

  it("registers joinAll branches in array order", async () => {
    const joinedWorkflow = workflow({
      name: "orders.join-all",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly unreachable: true }> => {
        await joinAll([
          callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" }),
          sleepUntil(1_000)
        ] as const);
        return { unreachable: true };
      }
    });

    const commit = await prepareWorkflowTaskCommit(joinedWorkflow, { sku: "sku-1" }, fakeClaimed, {
      payloadCodec: "Json"
    });

    expect(commit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled",
      "TimerStarted"
    ]);
    const activity = commit.appendEvents?.[0]?.data;
    const timer = commit.appendEvents?.[1]?.data;
    if (activity?.kind !== "ActivityScheduled" || timer?.kind !== "TimerStarted") {
      throw new Error("expected activity then timer");
    }
    expect(activity.scheduled.commandId).toEqual({ runId: runId("run-1"), seq: 1 });
    expect(timer.started.commandId).toEqual({ runId: runId("run-1"), seq: 2 });
  });

  it("records selectAll winner metadata in array order", async () => {
    const racingWorkflow = workflow({
      name: "orders.select-all",
      version: 1,
      handler: async (_input: {}): Promise<{ readonly index: number }> => {
        const winner = await selectAll([sleepUntil(500), sleepUntil(1_000)] as const);
        return { index: winner.index };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/select-all"),
      workflowType: racingWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [racingWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const waitCommit = await prepareWorkflowTaskCommit(racingWorkflow, {}, firstClaim, {
      payloadCodec: "Json"
    });
    expect(waitCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "TimerStarted",
      "TimerStarted"
    ]);
    await backend.commitWorkflowTask(firstClaim.claim, waitCommit);

    await backend.fireDueTimers({ namespace: namespace(), now: 500, limit: 16 });
    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [racingWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const completionCommit = await prepareWorkflowTaskCommit(racingWorkflow, {}, secondClaim, {
      payloadCodec: "Json"
    });
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "SelectWinner",
      "WorkflowCompleted"
    ]);
    const winner = completionCommit.appendEvents?.[0]?.data;
    if (winner?.kind !== "SelectWinner") {
      throw new Error("expected SelectWinner");
    }
    expect(winner.winner.selectCommandId).toEqual({ runId: secondClaim.runId, seq: 3 });
    expect(winner.winner.branchOrdinal).toBe(0);
    expect(winner.winner.winningEventId).toBe(eventId(4));
    await backend.commitWorkflowTask(secondClaim.claim, completionCommit);

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "TimerStarted",
      "TimerStarted",
      "TimerFired",
      "SelectWinner",
      "WorkflowCompleted"
    ]);
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(completed.result)).toEqual({ index: 0 });
  });

  it("records a version marker and takes the patched branch for new histories", async () => {
    const versionedWorkflow = workflow({
      name: "tests.version-new",
      version: 1,
      handler: async (_input: {}): Promise<string> => {
        if (patched("replace-a-with-b")) {
          return await callActivity(versionActivityB, {}, { taskQueue: "activities" });
        }
        return await callActivity(versionActivityA, {}, { taskQueue: "activities" });
      }
    });

    const commit = await prepareWorkflowTaskCommit(versionedWorkflow, {}, fakeClaimed, {
      payloadCodec: "Json"
    });

    expect(commit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "VersionMarker",
      "ActivityScheduled"
    ]);
    const marker = commit.appendEvents?.[0]?.data;
    if (marker?.kind !== "VersionMarker") {
      throw new Error("expected VersionMarker");
    }
    expect(marker.marker).toEqual({
      commandId: { runId: runId("run-1"), seq: 1 },
      changeId: "replace-a-with-b",
      version: 1
    });
    const scheduled = commit.appendEvents?.[1]?.data;
    if (scheduled?.kind !== "ActivityScheduled") {
      throw new Error("expected ActivityScheduled");
    }
    expect(scheduled.scheduled.activityName).toBe("tests.version-b");
    expect(scheduled.scheduled.commandId).toEqual({ runId: runId("run-1"), seq: 2 });
  });

  it("returns the default version for old histories without a marker", async () => {
    const originalWorkflow = workflow({
      name: "tests.version-old-original",
      version: 1,
      handler: async (_input: {}): Promise<string> =>
        await callActivity(versionActivityA, {}, { taskQueue: "activities" })
    });
    const patchedWorkflow = workflow({
      name: "tests.version-old-patched",
      version: 1,
      handler: async (_input: {}): Promise<string> => {
        const version = getVersion("replace-a-with-b", DEFAULT_VERSION, 1);
        if (version !== DEFAULT_VERSION) {
          return await callActivity(versionActivityB, {}, { taskQueue: "activities" });
        }
        return await callActivity(versionActivityA, {}, { taskQueue: "activities" });
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/version-old"),
      workflowType: originalWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [originalWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected claim");
    }
    const originalCommit = await prepareWorkflowTaskCommit(originalWorkflow, {}, claim, {
      payloadCodec: "Json"
    });
    await backend.commitWorkflowTask(claim.claim, originalCommit);
    const history = await backend.streamHistory({
      runId: claim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const replayClaim: ClaimedWorkflowTask = {
      ...claim,
      replayTargetEventId: eventId(2),
      prefetchedHistory: history.events
    };

    const replayCommit = await prepareWorkflowTaskCommit(patchedWorkflow, {}, replayClaim, {
      payloadCodec: "Json"
    });

    expect(replayCommit.appendEvents).toEqual([]);
    expect(replayCommit.scheduleActivities).toEqual([]);
  });

  it("deprecatePatch bridges existing patched histories without adding a new marker", async () => {
    const patchedWorkflow = workflow({
      name: "tests.version-bridge-patched",
      version: 1,
      handler: async (_input: {}): Promise<string> => {
        if (patched("replace-a-with-b")) {
          return await callActivity(versionActivityB, {}, { taskQueue: "activities" });
        }
        return await callActivity(versionActivityA, {}, { taskQueue: "activities" });
      }
    });
    const deprecatedWorkflow = workflow({
      name: "tests.version-bridge-deprecated",
      version: 1,
      handler: async (_input: {}): Promise<string> => {
        deprecatePatch("replace-a-with-b");
        return await callActivity(versionActivityB, {}, { taskQueue: "activities" });
      }
    });
    const firstCommit = await prepareWorkflowTaskCommit(patchedWorkflow, {}, fakeClaimed, {
      payloadCodec: "Json"
    });
    const versionMarker = firstCommit.appendEvents?.[0]?.data;
    const activityScheduled = firstCommit.appendEvents?.[1]?.data;
    if (versionMarker?.kind !== "VersionMarker" || activityScheduled?.kind !== "ActivityScheduled") {
      throw new Error("expected version marker followed by activity schedule");
    }
    const replayClaim: ClaimedWorkflowTask = {
      ...fakeClaimed,
      replayTargetEventId: eventId(3),
      prefetchedHistory: [
        {
          eventId: eventId(1),
          eventType: "WorkflowStarted",
          data: {
            kind: "WorkflowStarted",
            workflowType: patchedWorkflow.workflowType,
            input: encodePayload({}, { codec: "Json" })
          }
        },
        {
          eventId: eventId(2),
          eventType: "VersionMarker",
          data: versionMarker
        },
        {
          eventId: eventId(3),
          eventType: "ActivityScheduled",
          data: activityScheduled
        }
      ]
    };

    const replayCommit = await prepareWorkflowTaskCommit(deprecatedWorkflow, {}, replayClaim, {
      payloadCodec: "Json"
    });

    expect(replayCommit.appendEvents).toEqual([]);
    expect(replayCommit.scheduleActivities).toEqual([]);
  });

  it("records a deprecated patch marker for new bridge histories", async () => {
    const deprecatedWorkflow = workflow({
      name: "tests.version-deprecated-new",
      version: 1,
      handler: async (_input: {}): Promise<string> => {
        deprecatePatch("replace-a-with-b");
        return await callActivity(versionActivityB, {}, { taskQueue: "activities" });
      }
    });

    const commit = await prepareWorkflowTaskCommit(deprecatedWorkflow, {}, fakeClaimed, {
      payloadCodec: "Json"
    });

    expect(commit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "DeprecatedPatchMarker",
      "ActivityScheduled"
    ]);
    const marker = commit.appendEvents?.[0]?.data;
    if (marker?.kind !== "DeprecatedPatchMarker") {
      throw new Error("expected DeprecatedPatchMarker");
    }
    expect(marker.marker).toEqual({
      commandId: { runId: runId("run-1"), seq: 1 },
      patchId: "replace-a-with-b"
    });
  });

  it("rejects unsupported recorded workflow versions", async () => {
    const minTwoWorkflow = workflow({
      name: "tests.version-min-two",
      version: 1,
      handler: async (_input: {}): Promise<string> => {
        getVersion("replace-a-with-b", 2, 2);
        return await callActivity(versionActivityB, {}, { taskQueue: "activities" });
      }
    });
    const replayClaim: ClaimedWorkflowTask = {
      ...fakeClaimed,
      replayTargetEventId: eventId(2),
      prefetchedHistory: [
        {
          eventId: eventId(1),
          eventType: "WorkflowStarted",
          data: {
            kind: "WorkflowStarted",
            workflowType: minTwoWorkflow.workflowType,
            input: encodePayload({}, { codec: "Json" })
          }
        },
        {
          eventId: eventId(2),
          eventType: "VersionMarker",
          data: {
            kind: "VersionMarker",
            marker: {
              commandId: { runId: runId("run-1"), seq: 1 },
              changeId: "replace-a-with-b",
              version: 1
            }
          }
        }
      ]
    };

    await expect(
      prepareWorkflowTaskCommit(minTwoWorkflow, {}, replayClaim, { payloadCodec: "Json" })
    ).rejects.toBeInstanceOf(UnsupportedWorkflowVersionError);
  });

  it("rejects removing a patch bridge before marked histories are gone", async () => {
    const removedWorkflow = workflow({
      name: "tests.version-removed",
      version: 1,
      handler: async (_input: {}): Promise<string> =>
        await callActivity(versionActivityB, {}, { taskQueue: "activities" })
    });
    const replayClaim: ClaimedWorkflowTask = {
      ...fakeClaimed,
      replayTargetEventId: eventId(3),
      prefetchedHistory: [
        {
          eventId: eventId(1),
          eventType: "WorkflowStarted",
          data: {
            kind: "WorkflowStarted",
            workflowType: removedWorkflow.workflowType,
            input: encodePayload({}, { codec: "Json" })
          }
        },
        {
          eventId: eventId(2),
          eventType: "VersionMarker",
          data: {
            kind: "VersionMarker",
            marker: {
              commandId: { runId: runId("run-1"), seq: 1 },
              changeId: "replace-a-with-b",
              version: 1
            }
          }
        }
      ]
    };

    await expect(
      prepareWorkflowTaskCommit(removedWorkflow, {}, replayClaim, { payloadCodec: "Json" })
    ).rejects.toThrow("nondeterminism: expected ActivityScheduled");
  });

  it("replays side effect markers without rerunning the closure", async () => {
    let counter = 0;
    const sideEffectWorkflow = workflow({
      name: "tests.side-effect",
      version: 1,
      handler: async (_input: {}): Promise<{ readonly id: string }> => {
        const id = await sideEffect("make-id", () => {
          counter += 1;
          return `side-effect-${counter}`;
        });
        await sleepUntil(1_000);
        return { id };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/side-effect"),
      workflowType: sideEffectWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [sideEffectWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const waitCommit = await prepareWorkflowTaskCommit(sideEffectWorkflow, {}, firstClaim, {
      payloadCodec: "Json"
    });
    expect(waitCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "SideEffectMarker",
      "TimerStarted"
    ]);
    expect(counter).toBe(1);
    const marker = waitCommit.appendEvents?.[0]?.data;
    if (marker?.kind !== "SideEffectMarker") {
      throw new Error("expected SideEffectMarker");
    }
    expect(decodePayload(marker.marker.value)).toBe("side-effect-1");
    await backend.commitWorkflowTask(firstClaim.claim, waitCommit);

    await backend.fireDueTimers({ namespace: namespace(), now: 1_000, limit: 16 });
    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [sideEffectWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const completionCommit = await prepareWorkflowTaskCommit(sideEffectWorkflow, {}, secondClaim, {
      payloadCodec: "Json"
    });
    expect(counter).toBe(1);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    await backend.commitWorkflowTask(secondClaim.claim, completionCommit);

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "SideEffectMarker",
      "TimerStarted",
      "TimerFired",
      "WorkflowCompleted"
    ]);
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(completed.result)).toEqual({ id: "side-effect-1" });
  });

  it("rejects side effect key changes during replay", async () => {
    const originalWorkflow = workflow({
      name: "tests.side-effect-original",
      version: 1,
      handler: async (_input: {}): Promise<string> =>
        await sideEffect("make-id", () => "side-effect-1")
    });
    const changedWorkflow = workflow({
      name: "tests.side-effect-changed",
      version: 1,
      handler: async (_input: {}): Promise<string> =>
        await sideEffect("make-other-id", () => "side-effect-2")
    });
    const firstCommit = await prepareWorkflowTaskCommit(originalWorkflow, {}, fakeClaimed, {
      payloadCodec: "Json"
    });
    const marker = firstCommit.appendEvents?.[0]?.data;
    if (marker?.kind !== "SideEffectMarker") {
      throw new Error("expected SideEffectMarker");
    }
    const replayClaim: ClaimedWorkflowTask = {
      ...fakeClaimed,
      replayTargetEventId: eventId(2),
      prefetchedHistory: [
        {
          eventId: eventId(1),
          eventType: "WorkflowStarted",
          data: {
            kind: "WorkflowStarted",
            workflowType: originalWorkflow.workflowType,
            input: encodePayload({}, { codec: "Json" })
          }
        },
        {
          eventId: eventId(2),
          eventType: "SideEffectMarker",
          data: marker
        }
      ]
    };

    await expect(
      prepareWorkflowTaskCommit(changedWorkflow, {}, replayClaim, { payloadCodec: "Json" })
    ).rejects.toThrow("nondeterminism: expected side effect make-other-id, found make-id");
  });

  it("rejects empty side effect keys", async () => {
    const invalidWorkflow = workflow({
      name: "tests.side-effect-empty",
      version: 1,
      handler: async (_input: {}): Promise<string> => await sideEffect("", () => "bad")
    });

    await expect(
      prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, { payloadCodec: "Json" })
    ).rejects.toThrow("side effect key must not be empty");
  });

  it("rejects Date.now inside workflow code", async () => {
    const invalidWorkflow = workflow({
      name: "tests.nondeterministic-date-now",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> => {
        await sideEffect("before-date", () => 1);
        return Date.now();
      }
    });

    await expect(
      prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, { payloadCodec: "Json" })
    ).rejects.toThrow("nondeterminism: Date.now() is not allowed inside workflow code");
    expect(() => Date.now()).not.toThrow();
  });

  it("rejects Math.random inside workflow code outside side effects", async () => {
    const invalidWorkflow = workflow({
      name: "tests.nondeterministic-random",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> => Math.random()
    });

    await expect(
      prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, { payloadCodec: "Json" })
    ).rejects.toThrow("nondeterminism: Math.random() is not allowed inside workflow code");
    expect(() => Math.random()).not.toThrow();
  });

  it("allows Math.random inside sideEffect and replays the recorded value", async () => {
    let replayClosureRuns = 0;
    const randomWorkflow = workflow({
      name: "tests.side-effect-random",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> =>
        await sideEffect("record-random", () => Math.random())
    });
    const firstCommit = await prepareWorkflowTaskCommit(randomWorkflow, {}, fakeClaimed, {
      payloadCodec: "Json"
    });
    const marker = firstCommit.appendEvents?.[0]?.data;
    if (marker?.kind !== "SideEffectMarker") {
      throw new Error("expected SideEffectMarker");
    }
    const recorded = decodePayload<number>(marker.marker.value);
    expect(recorded).toBeGreaterThanOrEqual(0);
    expect(recorded).toBeLessThan(1);

    const replayWorkflow = workflow({
      name: "tests.side-effect-random-replay",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> =>
        await sideEffect("record-random", () => {
          replayClosureRuns += 1;
          throw new Error("side effect replay should not rerun closure");
        })
    });
    const replayClaim: ClaimedWorkflowTask = {
      ...fakeClaimed,
      replayTargetEventId: eventId(2),
      prefetchedHistory: [
        {
          eventId: eventId(1),
          eventType: "WorkflowStarted",
          data: {
            kind: "WorkflowStarted",
            workflowType: randomWorkflow.workflowType,
            input: encodePayload({}, { codec: "Json" })
          }
        },
        {
          eventId: eventId(2),
          eventType: "SideEffectMarker",
          data: marker
        }
      ]
    };
    const replayCommit = await prepareWorkflowTaskCommit(replayWorkflow, {}, replayClaim, {
      payloadCodec: "Json"
    });

    expect(replayClosureRuns).toBe(0);
    const completed = replayCommit.appendEvents?.[0]?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(completed.result)).toBe(recorded);
  });

  it.each([
    {
      apiName: "setTimeout()",
      replacement: "durust sleep() or sleepUntil()",
      run: (): void => {
        setTimeout(() => undefined, 0);
      }
    },
    {
      apiName: "setInterval()",
      replacement: "durust sleep() or recurring workflow timers",
      run: (): void => {
        setInterval(() => undefined, 1);
      }
    },
    {
      apiName: "queueMicrotask()",
      replacement: "durust durable operations",
      run: (): void => {
        queueMicrotask(() => undefined);
      }
    }
  ])("rejects native scheduling API $apiName inside workflow code", async ({ apiName, run }) => {
    const invalidWorkflow = workflow({
      name: `tests.nondeterministic-${apiName}`,
      version: 1,
      handler: async (_input: TestNoInput): Promise<string> => {
        run();
        return "unreachable";
      }
    });

    await expect(
      prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, { payloadCodec: "Json" })
    ).rejects.toThrow(`nondeterminism: ${apiName} is not allowed inside workflow code`);
  });

  it.each([
    {
      apiName: "Promise.all()",
      replacement: "durust join() or joinAll()",
      run: async (): Promise<unknown> => await Promise.all([Promise.resolve("value")])
    },
    {
      apiName: "Promise.race()",
      replacement: "durust select() or selectAll()",
      run: async (): Promise<unknown> => await Promise.race([Promise.resolve("value")])
    },
    {
      apiName: "Promise.allSettled()",
      replacement: "durust join()/joinAll() plus explicit error handling",
      run: async (): Promise<unknown> => await Promise.allSettled([Promise.resolve("value")])
    },
    {
      apiName: "Promise.any()",
      replacement: "durust select() or selectAll()",
      run: async (): Promise<unknown> => await Promise.any([Promise.resolve("value")])
    }
  ])(
    "rejects native promise combinator $apiName inside workflow code",
    async ({ apiName, run }) => {
      const invalidWorkflow = workflow({
        name: `tests.nondeterministic-${apiName}`,
        version: 1,
        handler: async (_input: TestNoInput): Promise<unknown> => await run()
      });

      await expect(
        prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, { payloadCodec: "Json" })
      ).rejects.toThrow(`nondeterminism: ${apiName} is not allowed inside workflow code`);
    }
  );

  it("spawns an activity handle and later replays its result", async () => {
    const handleWorkflow = workflow({
      name: "tests.activity-handle",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly cents: number }> => {
        const handle = await callActivity(priceQuote, { sku: input.sku }, {
          taskQueue: "payments"
        }).spawn();
        await sleepUntil(1_000);
        const quote = await handle.result();
        return { cents: quote.cents };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/activity-handle"),
      workflowType: handleWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ sku: "sku-1" }, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [handleWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const waitCommit = await prepareWorkflowTaskCommit(
      handleWorkflow,
      { sku: "sku-1" },
      firstClaim,
      { payloadCodec: "Json" }
    );
    expect(waitCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled",
      "TimerStarted"
    ]);
    await backend.commitWorkflowTask(firstClaim.claim, waitCommit);

    const activityTask = await backend.claimActivityTask("activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("payments"),
      registeredActivityNames: ["payments.price-quote"],
      leaseDurationMs: 30_000
    });
    if (!activityTask) {
      throw new Error("expected activity task");
    }
    await backend.completeActivity({
      claim: activityTask.claim,
      result: encodePayload<QuoteOutput>({ cents: 777 }, { codec: "Json" })
    });
    await backend.fireDueTimers({ namespace: namespace(), now: 1_000, limit: 16 });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [handleWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const completionCommit = await prepareWorkflowTaskCommit(
      handleWorkflow,
      { sku: "sku-1" },
      secondClaim,
      { payloadCodec: "Json" }
    );
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    await backend.commitWorkflowTask(secondClaim.claim, completionCommit);

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled",
      "TimerStarted",
      "ActivityCompleted",
      "TimerFired",
      "WorkflowCompleted"
    ]);
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(completed.result)).toEqual({ cents: 777 });
  });

  it("records a deterministic select winner when the first branch becomes ready", async () => {
    const racingWorkflow = workflow({
      name: "orders.select",
      version: 1,
      handler: async (
        input: CheckoutInput
      ): Promise<
        | { readonly branch: "quote"; readonly cents: number }
        | { readonly branch: "delay" }
      > => {
        const winner = await select({
          quote: callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" }),
          delay: sleepUntil(1_000)
        });
        if (winner.branch === "quote") {
          return { branch: "quote", cents: winner.value.cents };
        }
        return { branch: "delay" };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/select"),
      workflowType: racingWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ sku: "sku-1" }, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [racingWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const waitCommit = await prepareWorkflowTaskCommit(
      racingWorkflow,
      { sku: "sku-1" },
      firstClaim,
      { payloadCodec: "Json" }
    );
    expect(waitCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled",
      "TimerStarted"
    ]);
    await backend.commitWorkflowTask(firstClaim.claim, waitCommit);

    const activityTask = await backend.claimActivityTask("activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("payments"),
      registeredActivityNames: ["payments.price-quote"],
      leaseDurationMs: 30_000
    });
    if (!activityTask) {
      throw new Error("expected activity task");
    }
    await backend.completeActivity({
      claim: activityTask.claim,
      result: encodePayload<QuoteOutput>({ cents: 2468 }, { codec: "Json" })
    });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [racingWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const completionCommit = await prepareWorkflowTaskCommit(
      racingWorkflow,
      { sku: "sku-1" },
      secondClaim,
      { payloadCodec: "Json" }
    );
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "SelectWinner",
      "WorkflowCompleted"
    ]);
    const winner = completionCommit.appendEvents?.[0]?.data;
    if (winner?.kind !== "SelectWinner") {
      throw new Error("expected SelectWinner");
    }
    expect(winner.winner.selectCommandId).toEqual({ runId: secondClaim.runId, seq: 3 });
    expect(winner.winner.branchOrdinal).toBe(0);
    expect(winner.winner.winningEventId).toBe(eventId(4));
    await backend.commitWorkflowTask(secondClaim.claim, completionCommit);

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled",
      "TimerStarted",
      "ActivityCompleted",
      "SelectWinner",
      "WorkflowCompleted"
    ]);
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(completed.result)).toEqual({ branch: "quote", cents: 2468 });
  });

  it("rejects replay when the recorded select winner changes", async () => {
    const racingWorkflow = workflow({
      name: "orders.select-replay",
      version: 1,
      handler: async (
        input: CheckoutInput
      ): Promise<
        | { readonly branch: "quote"; readonly cents: number }
        | { readonly branch: "delay" }
      > => {
        const winner = await select({
          quote: callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" }),
          delay: sleepUntil(1_000)
        });
        if (winner.branch === "quote") {
          return { branch: "quote", cents: winner.value.cents };
        }
        return { branch: "delay" };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/select-replay"),
      workflowType: racingWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ sku: "sku-1" }, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [racingWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const waitCommit = await prepareWorkflowTaskCommit(
      racingWorkflow,
      { sku: "sku-1" },
      firstClaim,
      { payloadCodec: "Json" }
    );
    await backend.commitWorkflowTask(firstClaim.claim, waitCommit);

    const activityTask = await backend.claimActivityTask("activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("payments"),
      registeredActivityNames: ["payments.price-quote"],
      leaseDurationMs: 30_000
    });
    if (!activityTask) {
      throw new Error("expected activity task");
    }
    await backend.completeActivity({
      claim: activityTask.claim,
      result: encodePayload<QuoteOutput>({ cents: 1357 }, { codec: "Json" })
    });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [racingWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const completionCommit = await prepareWorkflowTaskCommit(
      racingWorkflow,
      { sku: "sku-1" },
      secondClaim,
      { payloadCodec: "Json" }
    );
    await backend.commitWorkflowTask(secondClaim.claim, completionCommit);

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const badHistory = history.events.map((event) => {
      if (event.data.kind !== "SelectWinner") {
        return event;
      }
      return {
        ...event,
        data: {
          kind: "SelectWinner" as const,
          winner: {
            ...event.data.winner,
            branchOrdinal: 1
          }
        }
      };
    });
    const badClaim: ClaimedWorkflowTask = {
      ...secondClaim,
      replayTargetEventId: eventId(6),
      prefetchedHistory: badHistory
    };

    await expect(
      prepareWorkflowTaskCommit(racingWorkflow, { sku: "sku-1" }, badClaim, {
        payloadCodec: "Json"
      })
    ).rejects.toThrow("nondeterminism: select winner branch changed");
  });
});
