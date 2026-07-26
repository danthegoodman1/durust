import { Console } from "node:console";
import { Writable } from "node:stream";
import v8 from "node:v8";
import vm from "node:vm";
import { describe, expect, it } from "vitest";
import {
  Client,
  MemoryBackend,
  ActivityFailureError,
  ChildWorkflowFailureError,
  DEFAULT_VERSION,
  RetryPolicy,
  UnsupportedWorkflowVersionError,
  activity,
  activityMap,
  activityMapFingerprint,
  activityMapManifest,
  callActivity,
  childWorkflow,
  childWorkflowMap,
  continueAsNew,
  decodeActivityMapResults,
  decodeChildWorkflowMapSuccesses,
  decodePayload,
  deprecatePatch,
  encodePayload,
  eventId,
  getVersion,
  join,
  joinAll,
  patched,
  namespace,
  payloadDigest,
  publish,
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
  type ActivityHandle,
  type ActivityMapInputManifest,
  type ChildWorkflowHandle,
  type ClaimedWorkflowTask,
  type HistoryEvent,
  type PayloadRef,
  type RunId,
  type SchemaAdapter,
  type WorkflowTaskCommit
} from "@durust/core";
import { HotWorkflowExecution, HotWorkflowExecutionDisposedError } from "../src/runtime.js";
import { prepareWorkflowTaskCommit } from "@durust/testing";

// Captured at module load, before any workflow execution installs the
// determinism guards, so this is Node's real environment object rather than
// anything the guards hand out.
const originalProcessEnvObject = process.env;
const currentWorkingDirectory = process.cwd();
const nodeConsole = new Console({
  stdout: new Writable({
    write(_chunk, _encoding, callback) {
      callback();
    }
  })
});

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

const childEchoWorkflow = workflow({
  name: "orders.runtime-child-echo",
  version: 1,
  handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> => ({
    value: `${input.value}/child`
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
  prefetchedHistory: [
    {
      eventId: eventId(1),
      eventType: "WorkflowStarted",
      data: {
        kind: "WorkflowStarted",
        workflowType: workflowType("orders.checkout", 1),
        input: encodePayload({}, { codec: "Json" })
      }
    }
  ]
};

function committedTail(outcome: { readonly kind: string; readonly newTailEventId?: unknown }) {
  if (outcome.kind !== "Committed" || typeof outcome.newTailEventId !== "number") {
    throw new Error(`expected committed outcome, got ${outcome.kind}`);
  }
  return eventId(outcome.newTailEventId);
}

// Renders an event as `Kind#seq` when it carries a marker command id, so an
// out-of-order marker pair is visible in the assertion diff rather than hidden
// behind matching event kinds. Accepts both appended and recorded events.
function commandTrace(event: { readonly data: { readonly kind: string } }): string {
  const data = event.data as Record<
    string,
    { readonly commandId?: { readonly seq?: number } } | undefined
  >;
  for (const slot of ["marker", "scheduled", "requested", "consumed", "started"]) {
    const seq = data[slot]?.commandId?.seq;
    if (seq !== undefined) {
      return `${event.data.kind}#${seq}`;
    }
  }
  return event.data.kind;
}

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

  it("keeps a hot async workflow frame alive across activity completion", async () => {
    const trace: string[] = [];
    const checkout = workflow({
      name: "orders.hot-checkout",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly cents: number }> => {
        trace.push(`start:${input.sku}`);
        const quote = await callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" });
        trace.push(`after:${quote.cents}`);
        return { cents: quote.cents };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-activity"),
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
    const hot = new HotWorkflowExecution(checkout, { sku: "sku-1" }, firstClaim, {
      payloadCodec: "Json"
    });
    const scheduleCommit = await hot.nextCommit();
    expect(scheduleCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled"
    ]);
    expect(trace).toEqual(["start:sku-1"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(firstClaim.claim, scheduleCommit))
    );

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
      result: encodePayload<QuoteOutput>({ cents: 1234 }, { codec: "Json" })
    });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [checkout.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const completionCommit = await hot.advance(secondClaim);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    expect(trace).toEqual(["start:sku-1", "after:1234"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(secondClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);
  });

  it("keeps a hot async workflow frame alive across child workflow completion", async () => {
    const trace: string[] = [];
    const parent = workflow({
      name: "orders.hot-child-parent",
      version: 1,
      handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> => {
        trace.push(`before-spawn:${input.value}`);
        const child = await childWorkflow(
          childEchoWorkflow,
          { value: input.value },
          { workflowId: `child/${input.value}`, taskQueue: "workflows" }
        ).spawn();
        trace.push(`started:${child.runId}`);
        const result = await child.result();
        trace.push(`after-child:${result.value}`);
        return { value: result.value };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-child-parent"),
      workflowType: parent.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "order-1" }, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parent.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first parent claim");
    }
    const hot = new HotWorkflowExecution(parent, { value: "order-1" }, firstClaim, {
      payloadCodec: "Json"
    });
    const requestCommit = await hot.nextCommit();
    expect(requestCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ChildWorkflowStartRequested"
    ]);
    expect(requestCommit.startChildWorkflows).toHaveLength(1);
    expect(trace).toEqual(["before-spawn:order-1"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(firstClaim.claim, requestCommit))
    );

    const startedClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parent.workflowType],
      leaseDurationMs: 30_000
    });
    if (!startedClaim) {
      throw new Error("expected child-start parent claim");
    }
    const waitForResultCommit = await hot.advance(startedClaim);
    expect(waitForResultCommit.appendEvents).toEqual([]);
    expect(waitForResultCommit.startChildWorkflows).toEqual([]);
    expect(trace).toEqual(["before-spawn:order-1", "started:run-2"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(startedClaim.claim, waitForResultCommit))
    );

    const childClaim = await backend.claimWorkflowTask("worker-child", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [childEchoWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!childClaim) {
      throw new Error("expected child workflow claim");
    }
    const childCommit = await prepareWorkflowTaskCommit(
      childEchoWorkflow,
      { value: "order-1" },
      childClaim,
      { payloadCodec: "Json" }
    );
    expect(childCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    await backend.commitWorkflowTask(childClaim.claim, childCommit);

    const completedClaim = await backend.claimWorkflowTask("worker-c", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parent.workflowType],
      leaseDurationMs: 30_000
    });
    if (!completedClaim) {
      throw new Error("expected child-complete parent claim");
    }
    const completionCommit = await hot.advance(completedClaim);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    expect(trace).toEqual([
      "before-spawn:order-1",
      "started:run-2",
      "after-child:order-1/child"
    ]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(completedClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);

    const history = await backend.streamHistory({
      runId: firstClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(20),
      maxEvents: 20,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ChildWorkflowStartRequested",
      "ChildWorkflowStarted",
      "ChildWorkflowCompleted",
      "WorkflowCompleted"
    ]);
  });

  it("keeps a hot async workflow frame alive across child workflow start failure", async () => {
    const trace: string[] = [];
    const parent = workflow({
      name: "orders.hot-child-conflict-parent",
      version: 1,
      handler: async (_input: TestNoInput): Promise<{ readonly errorType: string }> => {
        trace.push("before-spawn");
        try {
          await childWorkflow(
            childEchoWorkflow,
            { value: "conflict" },
            { workflowId: "child/hot-conflict", taskQueue: "workflows" }
          ).spawn();
          return { errorType: "none" };
        } catch (error) {
          if (error instanceof ChildWorkflowFailureError) {
            trace.push(`caught:${error.failure.errorType}`);
            return { errorType: error.failure.errorType };
          }
          throw error;
        }
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("child/hot-conflict"),
      workflowType: childEchoWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "already-running" }, { codec: "Json" })
    });
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-child-conflict-parent"),
      workflowType: parent.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parent.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first parent claim");
    }
    const hot = new HotWorkflowExecution(parent, {}, firstClaim, { payloadCodec: "Json" });
    const requestCommit = await hot.nextCommit();
    expect(requestCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ChildWorkflowStartRequested"
    ]);
    expect(trace).toEqual(["before-spawn"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(firstClaim.claim, requestCommit))
    );

    const failedClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parent.workflowType],
      leaseDurationMs: 30_000
    });
    if (!failedClaim) {
      throw new Error("expected child-failed parent claim");
    }
    const completionCommit = await hot.advance(failedClaim);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    expect(trace).toEqual([
      "before-spawn",
      "caught:durust.child_workflow_id_conflict"
    ]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(failedClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);
  });

  it("keeps a hot async workflow frame alive across activity-map completion", async () => {
    const trace: string[] = [];
    const mappedWorkflow = workflow({
      name: "orders.hot-activity-map",
      version: 1,
      handler: async (_input: TestNoInput): Promise<{ readonly totalCents: number }> => {
        trace.push("before-map");
        const mapped = activityMap(priceQuote, {
          inputManifest: activityMapManifest([{ sku: "a" }, { sku: "abcd" }], 2),
          resultManifest: "quotes",
          taskQueue: "payments",
          maxInFlight: 2
        });
        const manifestRef = await mapped.resultManifest();
        const totalCents = decodeActivityMapResults<QuoteOutput>(manifestRef).reduce(
          (sum, result) => sum + result.cents,
          0
        );
        trace.push(`after-map:${totalCents}`);
        return { totalCents };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-activity-map"),
      workflowType: mappedWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [mappedWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first map claim");
    }
    const hot = new HotWorkflowExecution(mappedWorkflow, {}, firstClaim, { payloadCodec: "Json" });
    const scheduleCommit = await hot.nextCommit();
    expect(scheduleCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityMapScheduled"
    ]);
    expect(scheduleCommit.scheduleActivityMaps).toHaveLength(1);
    expect(trace).toEqual(["before-map"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(firstClaim.claim, scheduleCommit))
    );

    const firstActivity = await backend.claimActivityTask("activity-worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("payments"),
      registeredActivityNames: ["payments.price-quote"],
      leaseDurationMs: 30_000
    });
    if (!firstActivity) {
      throw new Error("expected first map activity");
    }
    await backend.completeActivity({
      claim: firstActivity.claim,
      result: encodePayload<QuoteOutput>({ cents: 100 }, { codec: "Json" })
    });
    const secondActivity = await backend.claimActivityTask("activity-worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("payments"),
      registeredActivityNames: ["payments.price-quote"],
      leaseDurationMs: 30_000
    });
    if (!secondActivity) {
      throw new Error("expected second map activity");
    }
    await backend.completeActivity({
      claim: secondActivity.claim,
      result: encodePayload<QuoteOutput>({ cents: 250 }, { codec: "Json" })
    });

    const completedClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [mappedWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!completedClaim) {
      throw new Error("expected map-complete claim");
    }
    const completionCommit = await hot.advance(completedClaim);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    expect(trace).toEqual(["before-map", "after-map:350"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(completedClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);
  });

  it("keeps a hot async workflow frame alive across child-workflow-map completion", async () => {
    const trace: string[] = [];
    const mappedWorkflow = workflow({
      name: "orders.hot-child-workflow-map",
      version: 1,
      handler: async (_input: TestNoInput): Promise<{ readonly values: readonly string[] }> => {
        trace.push("before-child-map");
        const mapped = childWorkflowMap(childEchoWorkflow, {
          inputManifest: activityMapManifest([{ value: "a" }, { value: "b" }], 2),
          resultManifest: "child-values",
          workflowIdPrefix: "child-map/hot",
          taskQueue: "workflows",
          maxInFlight: 2
        });
        const manifestRef = await mapped.resultManifest();
        const values = decodeChildWorkflowMapSuccesses<{ readonly value: string }>(manifestRef)
          .map((result) => result.value);
        trace.push(`after-child-map:${values.join(",")}`);
        return { values };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-child-workflow-map"),
      workflowType: mappedWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });

    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [mappedWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first child-map claim");
    }
    const hot = new HotWorkflowExecution(mappedWorkflow, {}, firstClaim, {
      payloadCodec: "Json"
    });
    const scheduleCommit = await hot.nextCommit();
    expect(scheduleCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ChildWorkflowMapScheduled"
    ]);
    expect(scheduleCommit.scheduleChildWorkflowMaps).toHaveLength(1);
    expect(trace).toEqual(["before-child-map"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(firstClaim.claim, scheduleCommit))
    );

    for (const value of ["a", "b"]) {
      const childClaim = await backend.claimWorkflowTask(`worker-child-${value}`, {
        namespace: namespace(),
        taskQueue: taskQueue("workflows"),
        registeredWorkflowTypes: [childEchoWorkflow.workflowType],
        leaseDurationMs: 30_000
      });
      if (!childClaim) {
        throw new Error(`expected child workflow claim for ${value}`);
      }
      const childCommit = await prepareWorkflowTaskCommit(
        childEchoWorkflow,
        { value },
        childClaim,
        { payloadCodec: "Json" }
      );
      expect(childCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
        "WorkflowCompleted"
      ]);
      await backend.commitWorkflowTask(childClaim.claim, childCommit);
    }

    const completedClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [mappedWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!completedClaim) {
      throw new Error("expected child-map-complete claim");
    }
    const completionCommit = await hot.advance(completedClaim);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    expect(trace).toEqual([
      "before-child-map",
      "after-child-map:a/child,b/child"
    ]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(completedClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);

    const history = await backend.streamHistory({
      runId: firstClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(20),
      maxEvents: 20,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ChildWorkflowMapScheduled",
      "ChildWorkflowMapCompleted",
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

  it("fails workflow code that continues as new with a non-object input", async () => {
    const invalidInputs = [
      "bad",
      null,
      ["bad"],
      () => ({ ok: false })
    ];

    for (const [index, invalidInput] of invalidInputs.entries()) {
      const invalid = workflow({
        name: `tests.continue-as-new-invalid-input-${index}`,
        version: 1,
        handler: async (_input: TestNoInput): Promise<void> =>
          continueAsNew(invalidInput as unknown as TestNoInput)
      });

      const commit = await prepareWorkflowTaskCommit(invalid, {}, fakeClaimed, {
        payloadCodec: "Json"
      });

      expect(commit.appendEvents?.map((event) => event.data.kind)).toEqual(["WorkflowFailed"]);
      const failed = commit.appendEvents?.[0]?.data;
      if (failed?.kind !== "WorkflowFailed") {
        throw new Error("expected WorkflowFailed");
      }
      expect(failed.failure.message).toBe("continueAsNew input must be a durable input object");
    }
  });

  it("fails workflow code that publishes a non-object query projection", async () => {
    const invalidProjections = [
      "bad",
      null,
      [],
      () => ({ status: "bad" })
    ];

    for (const [index, invalidProjection] of invalidProjections.entries()) {
      const invalidWorkflow = workflow({
        name: `tests.publish-invalid-root-${index}`,
        version: 1,
        handler: async (_input: TestNoInput): Promise<void> => {
          publish(invalidProjection as unknown as Record<string, unknown>);
        }
      });
      const commit = await prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json"
      });
      expect(commit.appendEvents?.map((event) => event.data.kind)).toEqual(["WorkflowFailed"]);
      const failed = commit.appendEvents?.[0]?.data;
      if (failed?.kind !== "WorkflowFailed") {
        throw new Error("expected WorkflowFailed");
      }
      expect(failed.failure.message).toBe("query projection must be a durable input object");
    }
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

  // Records a two-timer history under the given durable name and returns a
  // replay claim plus the backend, so divergence tests can replay shortened
  // workflow versions against it.
  async function recordTwoTimerHistory(durableName: string): Promise<{
    readonly backend: MemoryBackend;
    readonly replayClaim: ClaimedWorkflowTask;
  }> {
    const twoTimer = workflow({
      name: durableName,
      version: 1,
      handler: async (_input: {}): Promise<{ readonly done: true }> => {
        await sleepUntil(1_000);
        await sleepUntil(2_000);
        return { done: true };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId(`wf/${durableName}`),
      workflowType: twoTimer.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });

    for (const [worker, timerNow] of [["worker-a", 1_000], ["worker-b", 2_000]] as const) {
      const claim = await backend.claimWorkflowTask(worker, {
        namespace: namespace(),
        taskQueue: taskQueue("workflows"),
        registeredWorkflowTypes: [twoTimer.workflowType],
        leaseDurationMs: 30_000
      });
      if (!claim) {
        throw new Error(`expected claim for ${worker}`);
      }
      await backend.commitWorkflowTask(
        claim.claim,
        await prepareWorkflowTaskCommit(twoTimer, {}, claim, { payloadCodec: "Json" })
      );
      await backend.fireDueTimers({ namespace: namespace(), now: timerNow, limit: 16 });
    }

    const replayClaim = await backend.claimWorkflowTask("worker-replay", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [twoTimer.workflowType],
      leaseDurationMs: 30_000
    });
    if (!replayClaim) {
      throw new Error("expected replay claim");
    }
    return { backend, replayClaim };
  }

  async function assertHistoryHasNoTerminalEvent(
    backend: MemoryBackend,
    runId: RunId
  ): Promise<void> {
    const history = await backend.streamHistory({
      runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "TimerStarted",
      "TimerFired",
      "TimerStarted",
      "TimerFired"
    ]);
  }

  it("rejects terminal completion with leftover recorded timer commands", async () => {
    const { backend, replayClaim } = await recordTwoTimerHistory("tests.timer-shortening");
    const oneTimer = workflow({
      name: "tests.timer-shortening",
      version: 1,
      handler: async (_input: {}): Promise<{ readonly done: true }> => {
        await sleepUntil(1_000);
        return { done: true };
      }
    });

    await expect(
      prepareWorkflowTaskCommit(oneTimer, {}, replayClaim, { payloadCodec: "Json" })
    ).rejects.toThrow(
      "nondeterminism: WorkflowCompleted reached with unconsumed recorded command TimerStarted"
    );
    await assertHistoryHasNoTerminalEvent(backend, replayClaim.runId);
  });

  it("rejects terminal app failure with leftover recorded timer commands without hanging", async () => {
    const { backend, replayClaim } = await recordTwoTimerHistory("tests.timer-shortening-fail");
    const oneTimerThenThrow = workflow({
      name: "tests.timer-shortening-fail",
      version: 1,
      handler: async (_input: {}): Promise<{ readonly done: true }> => {
        await sleepUntil(1_000);
        throw new Error("app failure after shortened replay");
      }
    });

    // The divergence error is raised while recording WorkflowFailed inside the
    // handler's rejection path; it must surface as a fatal prepare error, not
    // an unhandled rejection that leaves the task hanging.
    await expect(
      prepareWorkflowTaskCommit(oneTimerThenThrow, {}, replayClaim, { payloadCodec: "Json" })
    ).rejects.toThrow(
      "nondeterminism: WorkflowFailed reached with unconsumed recorded command TimerStarted"
    );
    await assertHistoryHasNoTerminalEvent(backend, replayClaim.runId);
  });

  it("rejects continue-as-new with leftover recorded timer commands", async () => {
    const { backend, replayClaim } = await recordTwoTimerHistory("tests.timer-shortening-can");
    const oneTimerThenContinue = workflow({
      name: "tests.timer-shortening-can",
      version: 1,
      handler: async (_input: TestNoInput): Promise<void> => {
        await sleepUntil(1_000);
        continueAsNew<TestNoInput>({});
      }
    });

    await expect(
      prepareWorkflowTaskCommit(oneTimerThenContinue, {}, replayClaim, { payloadCodec: "Json" })
    ).rejects.toThrow(
      "nondeterminism: WorkflowContinuedAsNew reached with unconsumed recorded command TimerStarted"
    );
    await assertHistoryHasNoTerminalEvent(backend, replayClaim.runId);
  });

  it("keeps a hot async workflow frame alive across timer firing", async () => {
    const trace: string[] = [];
    const reminder = workflow({
      name: "orders.hot-reminder",
      version: 1,
      handler: async (input: { readonly deadlineMs: number }): Promise<{ readonly done: true }> => {
        trace.push("before-timer");
        await sleepUntil(input.deadlineMs);
        trace.push("after-timer");
        return { done: true };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-timer"),
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
    const hot = new HotWorkflowExecution(reminder, { deadlineMs: 1_000 }, firstClaim, {
      payloadCodec: "Json"
    });
    const timerCommit = await hot.nextCommit();
    expect(timerCommit.appendEvents?.map((event) => event.data.kind)).toEqual(["TimerStarted"]);
    expect(trace).toEqual(["before-timer"]);
    hot.markCommitted(committedTail(await backend.commitWorkflowTask(firstClaim.claim, timerCommit)));

    await backend.fireDueTimers({ namespace: namespace(), now: 1_000, limit: 16 });
    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [reminder.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const completionCommit = await hot.advance(secondClaim);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    expect(trace).toEqual(["before-timer", "after-timer"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(secondClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);
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

  it("uses signal schema adapters for client encoding and workflow decoding", async () => {
    const approvalSchema: SchemaAdapter<ApprovalSignal> = {
      fingerprint: "sha256:approval-signal",
      rootKind: "object",
      encode: (value) => ({ approval_id: value.approvalId }),
      decode: (value) => ({
        approvalId: (value as { readonly approval_id: string }).approval_id
      })
    };
    const approved = signal<ApprovalSignal>("schema-approved", { schema: approvalSchema });
    const approvalWorkflow = workflow({
      name: "orders.schema-approval",
      version: 1,
      handler: async (_input: {}): Promise<ApprovalSignal> => {
        return await approved;
      }
    });
    const backend = new MemoryBackend();
    const client = new Client(backend, {
      payloadCodec: "Json",
      signalIdFactory: () => "schema-sig-1"
    });

    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/schema-signal"),
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

    await client.sendSignal({
      workflowId: workflowId("wf/schema-signal"),
      signal: approved,
      payload: { approvalId: "a-schema" }
    });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [approvalWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const liveSignal = await backend.readSignalInbox({
      runId: secondClaim.runId,
      signalName: "schema-approved"
    });
    expect(liveSignal).not.toBeNull();
    if (!liveSignal) {
      throw new Error("expected live signal");
    }
    expect(liveSignal.payload.schemaFingerprint).toBe("sha256:approval-signal");
    expect(decodePayload<{ readonly approval_id: string }>(liveSignal.payload)).toEqual({
      approval_id: "a-schema"
    });

    const consumeCommit = await prepareWorkflowTaskCommit(approvalWorkflow, {}, secondClaim, {
      payloadCodec: "Json",
      liveSignals: [liveSignal]
    });
    await backend.commitWorkflowTask(secondClaim.claim, consumeCommit);

    const history = await backend.streamHistory({
      runId: secondClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const completed = history.events.find((event) => event.data.kind === "WorkflowCompleted");
    expect(completed?.data.kind).toBe("WorkflowCompleted");
    if (completed?.data.kind !== "WorkflowCompleted") {
      throw new Error("expected completed event");
    }
    expect(decodePayload<ApprovalSignal>(completed.data.result)).toEqual({
      approvalId: "a-schema"
    });
  });

  it("keeps a hot async workflow frame alive across signal delivery", async () => {
    const trace: string[] = [];
    const approvalWorkflow = workflow({
      name: "orders.hot-approval",
      version: 1,
      handler: async (_input: {}): Promise<{ readonly approvalId: string }> => {
        trace.push("waiting");
        const approval = await signal<ApprovalSignal>("approved");
        trace.push(`approved:${approval.approvalId}`);
        return { approvalId: approval.approvalId };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-signal"),
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
    const hot = new HotWorkflowExecution(approvalWorkflow, {}, firstClaim, {
      payloadCodec: "Json"
    });
    const waitCommit = await hot.nextCommit();
    expect(waitCommit.appendEvents).toEqual([]);
    expect(waitCommit.upsertWaits).toEqual([
      {
        waitId: "run-1:signal:1",
        runId: firstClaim.runId,
        commandId: { runId: firstClaim.runId, seq: 1 },
        kind: "Signal",
        key: "approved",
        readyAt: null
      }
    ]);
    expect(trace).toEqual(["waiting"]);
    hot.markCommitted(committedTail(await backend.commitWorkflowTask(firstClaim.claim, waitCommit)));

    await backend.signalWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-signal"),
      signalId: signalId("sig-hot"),
      signalName: "approved",
      payload: encodePayload<ApprovalSignal>({ approvalId: "a-hot" }, { codec: "Json" })
    });
    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [approvalWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const liveSignal = await backend.readSignalInbox({
      runId: secondClaim.runId,
      signalName: "approved"
    });
    if (!liveSignal) {
      throw new Error("expected live signal");
    }
    const completionCommit = await hot.advance(secondClaim, { liveSignals: [liveSignal] });
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "SignalConsumed",
      "WorkflowCompleted"
    ]);
    expect(completionCommit.consumeSignals).toEqual(["sig-hot"]);
    expect(completionCommit.deleteWaits).toEqual(["run-1:signal:1"]);
    expect(trace).toEqual(["waiting", "approved:a-hot"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(secondClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);
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

  it("keeps a hot async workflow frame alive across join branches", async () => {
    const trace: string[] = [];
    const joinedWorkflow = workflow({
      name: "orders.hot-join",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly cents: number }> => {
        trace.push("before-join");
        const result = await join({
          quote: callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" }),
          delay: sleepUntil(1_000)
        });
        trace.push(`after-join:${result.quote.cents}`);
        return { cents: result.quote.cents };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-join"),
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
    const hot = new HotWorkflowExecution(joinedWorkflow, { sku: "sku-1" }, firstClaim, {
      payloadCodec: "Json"
    });
    const waitCommit = await hot.nextCommit();
    expect(waitCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled",
      "TimerStarted"
    ]);
    expect(trace).toEqual(["before-join"]);
    hot.markCommitted(committedTail(await backend.commitWorkflowTask(firstClaim.claim, waitCommit)));

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
      result: encodePayload<QuoteOutput>({ cents: 8765 }, { codec: "Json" })
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
    const completionCommit = await hot.advance(secondClaim);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    expect(trace).toEqual(["before-join", "after-join:8765"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(secondClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);
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

  it("keeps a hot async workflow frame alive across joinAll branches", async () => {
    const trace: string[] = [];
    const joinedWorkflow = workflow({
      name: "orders.hot-join-all",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly cents: number }> => {
        trace.push("before-join-all");
        const result = await joinAll([
          callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" }),
          sleepUntil(1_000)
        ] as const);
        const quote = result[0] as QuoteOutput;
        trace.push(`after-join-all:${quote.cents}`);
        return { cents: quote.cents };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-join-all"),
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
    const hot = new HotWorkflowExecution(joinedWorkflow, { sku: "sku-1" }, firstClaim, {
      payloadCodec: "Json"
    });
    const waitCommit = await hot.nextCommit();
    expect(waitCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled",
      "TimerStarted"
    ]);
    expect(trace).toEqual(["before-join-all"]);
    hot.markCommitted(committedTail(await backend.commitWorkflowTask(firstClaim.claim, waitCommit)));

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
      result: encodePayload<QuoteOutput>({ cents: 9753 }, { codec: "Json" })
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
    const completionCommit = await hot.advance(secondClaim);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    expect(trace).toEqual(["before-join-all", "after-join-all:9753"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(secondClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);
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

  it("keeps a hot async workflow frame alive across selectAll winner resolution", async () => {
    const trace: string[] = [];
    const racingWorkflow = workflow({
      name: "orders.hot-select-all",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly index: number; readonly cents: number }> => {
        trace.push("before-select-all");
        const winner = await selectAll([
          callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" }),
          sleepUntil(1_000)
        ] as const);
        trace.push(`after-select-all:${winner.index}`);
        return {
          index: winner.index,
          cents: (winner.value as QuoteOutput).cents
        };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-select-all"),
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
    const hot = new HotWorkflowExecution(racingWorkflow, { sku: "sku-1" }, firstClaim, {
      payloadCodec: "Json"
    });
    const waitCommit = await hot.nextCommit();
    expect(waitCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled",
      "TimerStarted"
    ]);
    expect(trace).toEqual(["before-select-all"]);
    hot.markCommitted(committedTail(await backend.commitWorkflowTask(firstClaim.claim, waitCommit)));

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
      result: encodePayload<QuoteOutput>({ cents: 8642 }, { codec: "Json" })
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
    const completionCommit = await hot.advance(secondClaim);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "SelectWinner",
      "WorkflowCompleted"
    ]);
    const winner = completionCommit.appendEvents?.[0]?.data;
    if (winner?.kind !== "SelectWinner") {
      throw new Error("expected SelectWinner");
    }
    expect(winner.winner.branchOrdinal).toBe(0);
    expect(trace).toEqual(["before-select-all", "after-select-all:0"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(secondClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);
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

  it("fails the workflow task when a durable API is called inside a sideEffect callback", async () => {
    const reentrantWorkflow = workflow({
      name: "tests.side-effect-reentrant",
      version: 1,
      handler: async (_input: TestNoInput): Promise<string> =>
        await sideEffect("make-id", () => `id-${getVersion("side-effect-reentrant", 1, 2)}`)
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/side-effect-reentrant"),
      workflowType: reentrantWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [reentrantWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected claim");
    }

    // Commit whatever the task produces. Without the guard the task succeeds and
    // this records the poisoned pair, so a regression is exhibited as the
    // out-of-order markers in history rather than only as a missing throw.
    let commit: WorkflowTaskCommit | null = null;
    let taskError: unknown = null;
    try {
      commit = await prepareWorkflowTaskCommit(reentrantWorkflow, {}, claim, {
        payloadCodec: "Json"
      });
    } catch (error) {
      taskError = error;
    }
    if (commit !== null) {
      await backend.commitWorkflowTask(claim.claim, commit);
    }

    const history = await backend.streamHistory({
      runId: claim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map(commandTrace)).toEqual(["WorkflowStarted"]);
    expect(commit).toBeNull();
    expect(String(taskError)).toContain(
      "nondeterminism: durable APIs cannot be called from inside a sideEffect callback"
    );
    expect(String(taskError)).toContain(
      'side effect "make-id" called getVersion(side-effect-reentrant)'
    );
    // The `nondeterminism:` prefix is what makes the worker fail and retry the
    // task with its nondeterminism backoff instead of failing the workflow.
    expect(taskError).toBeInstanceOf(Error);
    expect((taskError as Error).message.startsWith("nondeterminism:")).toBe(true);
  });

  it("appends no command for a durable API rejected inside a sideEffect callback", async () => {
    const caughtWorkflow = workflow({
      name: "tests.side-effect-reentrant-caught",
      version: 1,
      handler: async (_input: TestNoInput): Promise<string> => {
        try {
          await sideEffect("make-id", () => `id-${getVersion("caught-change", 1, 2)}`);
          return "side effect unexpectedly succeeded";
        } catch (error) {
          return (error as Error).message;
        }
      }
    });

    const commit = await prepareWorkflowTaskCommit(caughtWorkflow, {}, fakeClaimed, {
      payloadCodec: "Json"
    });
    // Neither the inner VersionMarker nor the outer SideEffectMarker reaches the
    // commit: the rejected call appends nothing and the side effect never
    // records a marker it could not replay.
    expect(commit.appendEvents?.map(commandTrace)).toEqual(["WorkflowCompleted"]);
    const completed = commit.appendEvents?.[0]?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(String(decodePayload(completed.result))).toContain(
      "durable APIs cannot be called from inside a sideEffect callback"
    );
  });

  it("rejects each durable API called from inside a sideEffect callback", async () => {
    // A child workflow handle only exists once its start is recorded, so the
    // child-result case replays a history that already contains one.
    const childStarter = workflow({
      name: "tests.side-effect-reentrant-child-starter",
      version: 1,
      handler: async (_input: TestNoInput): Promise<string> => {
        const child = await childWorkflow(
          childEchoWorkflow,
          { value: "v" },
          { workflowId: "wf/reentrant-child-result", taskQueue: "workflows" }
        ).spawn();
        return String(child.runId);
      }
    });
    const startCommit = await prepareWorkflowTaskCommit(childStarter, {}, fakeClaimed, {
      payloadCodec: "Json"
    });
    const startRequested = startCommit.appendEvents?.[0]?.data;
    if (startRequested?.kind !== "ChildWorkflowStartRequested") {
      throw new Error("expected ChildWorkflowStartRequested");
    }
    const childStartedClaim: ClaimedWorkflowTask = {
      ...fakeClaimed,
      replayTargetEventId: eventId(3),
      prefetchedHistory: [
        ...fakeClaimed.prefetchedHistory,
        {
          eventId: eventId(2),
          eventType: "ChildWorkflowStartRequested",
          data: startRequested
        },
        {
          eventId: eventId(3),
          eventType: "ChildWorkflowStarted",
          data: {
            kind: "ChildWorkflowStarted",
            started: {
              commandId: startRequested.requested.commandId,
              workflowId: startRequested.requested.workflowId,
              runId: runId("run-child-1")
            }
          }
        }
      ]
    };

    let spawnedActivity: ActivityHandle<QuoteOutput> | null = null;
    let spawnedChild: ChildWorkflowHandle<{ readonly value: string }> | null = null;

    const cases: readonly {
      readonly label: string;
      readonly api: string;
      readonly call: () => void;
      // Runs inside the workflow before the side effect, for APIs that need a
      // handle the callback can reach.
      readonly setup?: () => Promise<void>;
      readonly claimed?: ClaimedWorkflowTask;
      // Commit produced when the workflow catches the rejection; defaults to a
      // bare completion because a rejected durable call appends nothing.
      readonly caughtCommit?: readonly string[];
    }[] = [
      {
        label: "get-version",
        api: "getVersion(reentrant)",
        call: () => {
          getVersion("reentrant", 1, 2);
        }
      },
      {
        label: "patched",
        api: "getVersion(reentrant)",
        call: () => {
          patched("reentrant");
        }
      },
      {
        label: "deprecate-patch",
        api: "deprecatePatch(reentrant)",
        call: () => {
          deprecatePatch("reentrant");
        }
      },
      {
        label: "publish",
        api: "publish()",
        call: () => {
          publish({ done: true });
        }
      },
      {
        label: "continue-as-new",
        api: "continueAsNew()",
        call: () => {
          continueAsNew({});
        }
      },
      {
        label: "side-effect",
        api: "sideEffect(inner)",
        call: () => {
          void sideEffect("inner", () => 1).then(() => undefined);
        }
      },
      {
        label: "call-activity",
        api: "callActivity(payments.price-quote)",
        call: () => {
          void callActivity(priceQuote, { sku: "sku-1" }, { taskQueue: "payments" }).then(
            () => undefined
          );
        }
      },
      {
        label: "sleep",
        api: "sleep()",
        call: () => {
          void sleep(1).then(() => undefined);
        }
      },
      {
        label: "signal",
        api: "signal(approval)",
        call: () => {
          void signal<ApprovalSignal>("approval").then(() => undefined);
        }
      },
      {
        label: "child-workflow",
        api: "childWorkflow(orders.runtime-child-echo)",
        call: () => {
          void childWorkflow(
            childEchoWorkflow,
            { value: "v" },
            { workflowId: "wf/reentrant-child", taskQueue: "workflows" }
          )
            .spawn()
            .then(() => undefined);
        }
      },
      {
        label: "activity-map",
        api: "activityMap(payments.price-quote)",
        call: () => {
          void activityMap(priceQuote, {
            inputManifest: activityMapManifest([{ sku: "a" }], 1),
            resultManifest: "quotes",
            taskQueue: "payments",
            maxInFlight: 1
          })
            .resultManifest()
            .then(() => undefined);
        }
      },
      {
        label: "child-workflow-map",
        api: "childWorkflowMap(orders.runtime-child-echo)",
        call: () => {
          void childWorkflowMap(childEchoWorkflow, {
            inputManifest: activityMapManifest([{ value: "a" }], 1),
            resultManifest: "echoes",
            workflowIdPrefix: "wf/reentrant-child-map",
            taskQueue: "workflows",
            maxInFlight: 1
          })
            .resultManifest()
            .then(() => undefined);
        }
      },
      {
        label: "activity-handle-result",
        api: "activityHandle.result(payments.price-quote)",
        setup: async (): Promise<void> => {
          spawnedActivity = await callActivity(
            priceQuote,
            { sku: "sku-1" },
            { taskQueue: "payments" }
          ).spawn();
        },
        call: () => {
          const handle = spawnedActivity;
          if (handle === null) {
            throw new Error("expected a spawned activity handle");
          }
          void handle.result().then(() => undefined);
        },
        caughtCommit: ["ActivityScheduled#1", "WorkflowCompleted"]
      },
      {
        label: "child-workflow-result",
        api: "childWorkflowHandle.result(orders.runtime-child-echo)",
        claimed: childStartedClaim,
        setup: async (): Promise<void> => {
          spawnedChild = await childWorkflow(
            childEchoWorkflow,
            { value: "v" },
            { workflowId: "wf/reentrant-child-result", taskQueue: "workflows" }
          ).spawn();
        },
        call: () => {
          const handle = spawnedChild;
          if (handle === null) {
            throw new Error("expected a spawned child workflow handle");
          }
          void handle.result().then(() => undefined);
        }
      }
    ];

    for (const testCase of cases) {
      const claimed = testCase.claimed ?? fakeClaimed;
      const reentrantWorkflow = workflow({
        name: `tests.side-effect-reentrant-${testCase.label}`,
        version: 1,
        handler: async (_input: TestNoInput): Promise<number> => {
          await testCase.setup?.();
          return await sideEffect("guarded", () => {
            testCase.call();
            return 1;
          });
        }
      });

      await expect(
        prepareWorkflowTaskCommit(reentrantWorkflow, {}, claimed, { payloadCodec: "Json" })
      ).rejects.toThrow(
        "nondeterminism: durable APIs cannot be called from inside a sideEffect callback; " +
          `side effect "guarded" called ${testCase.api}.`
      );

      // The same rejection, caught by the workflow, must leave nothing
      // half-appended: no command from the inner call and no side effect marker.
      const caughtWorkflow = workflow({
        name: `tests.side-effect-reentrant-caught-${testCase.label}`,
        version: 1,
        handler: async (_input: TestNoInput): Promise<string> => {
          await testCase.setup?.();
          try {
            await sideEffect("guarded", () => {
              testCase.call();
              return 1;
            });
            return "side effect unexpectedly succeeded";
          } catch (error) {
            return (error as Error).message;
          }
        }
      });
      const caughtCommit = await prepareWorkflowTaskCommit(caughtWorkflow, {}, claimed, {
        payloadCodec: "Json"
      });
      expect(caughtCommit.appendEvents?.map(commandTrace)).toEqual(
        testCase.caughtCommit ?? ["WorkflowCompleted"]
      );
    }
  });

  it("rejects a durable API re-entered while a durable command converts its values", async () => {
    // Every case re-enters the runtime from user code a durable API invokes
    // *after* it has allocated its command seq and *before* it appends its
    // event. Without the guard the inner call takes seq N+1 and appends first,
    // recording an out-of-order pair that can never replay.
    const reentrantInput = (changeId: string): { readonly sku: string } =>
      ({
        sku: "sku-1",
        // JSON encoding of a durable payload calls this.
        toJSON(): { readonly sku: string; readonly version: number } {
          return { sku: "sku-1", version: getVersion(changeId, 1, 2) };
        }
      }) as unknown as { readonly sku: string };
    // Types say these are a string and a number; at runtime a JS caller, an
    // `any`, or deserialized config can put an object here, and the fingerprint
    // template literal or the arithmetic then runs its hook inside the window.
    // Carries both hooks: a fingerprint template literal invokes `toString`,
    // while `activityOptionsDigest`'s `JSON.stringify` invokes `toJSON`.
    const reentrantText = (changeId: string): string =>
      ({
        toString(): string {
          return `q-${getVersion(changeId, 1, 2)}`;
        },
        toJSON(): string {
          return `q-${getVersion(changeId, 1, 2)}`;
        }
      }) as unknown as string;
    const reentrantNumber = (changeId: string): number =>
      ({
        valueOf(): number {
          return getVersion(changeId, 1, 2);
        }
      }) as unknown as number;

    const cases: readonly {
      readonly label: string;
      readonly frame: string;
      readonly call: (changeId: string) => void;
    }[] = [
      {
        label: "call-activity-input",
        frame: "callActivity(payments.price-quote)",
        call: (changeId) => {
          void callActivity(priceQuote, reentrantInput(changeId), {
            taskQueue: "payments"
          }).then(() => undefined);
        }
      },
      {
        label: "child-workflow-input",
        frame: "childWorkflow(orders.runtime-child-echo)",
        call: (changeId) => {
          void childWorkflow(
            childEchoWorkflow,
            reentrantInput(changeId) as unknown as { readonly value: string },
            { workflowId: "wf/reentrant-encode-child", taskQueue: "workflows" }
          )
            .spawn()
            .then(() => undefined);
        }
      },
      {
        label: "side-effect-value",
        frame: "sideEffect(recorded)",
        call: (changeId) => {
          void sideEffect("recorded", () => reentrantInput(changeId)).then(() => undefined);
        }
      },
      {
        label: "sleep-duration",
        frame: "sleep()",
        call: (changeId) => {
          void sleep(reentrantNumber(changeId)).then(() => undefined);
        }
      },
      {
        label: "activity-map-task-queue",
        frame: "activityMap(payments.price-quote)",
        call: (changeId) => {
          void activityMap(priceQuote, {
            inputManifest: activityMapManifest([{ sku: "a" }], 1),
            resultManifest: "quotes",
            taskQueue: reentrantText(changeId),
            maxInFlight: 1
          })
            .resultManifest()
            .then(() => undefined);
        }
      },
      {
        label: "child-workflow-map-task-queue",
        frame: "childWorkflowMap(orders.runtime-child-echo)",
        call: (changeId) => {
          void childWorkflowMap(childEchoWorkflow, {
            inputManifest: activityMapManifest([{ value: "a" }], 1),
            resultManifest: "echoes",
            workflowIdPrefix: "wf/reentrant-encode-map",
            taskQueue: reentrantText(changeId),
            maxInFlight: 1
          })
            .resultManifest()
            .then(() => undefined);
        }
      },
      {
        label: "publish-view",
        frame: "publish()",
        call: (changeId) => {
          publish(reentrantInput(changeId));
        }
      },
      {
        label: "continue-as-new-input",
        frame: "continueAsNew()",
        call: (changeId) => {
          continueAsNew(reentrantInput(changeId));
        }
      }
    ];

    // Collected across every case and asserted once, so a regression reports
    // all of them rather than stopping at the first.
    const commitTraces: Record<string, readonly string[]> = {};
    const taskErrors: Record<string, string> = {};

    for (const testCase of cases) {
      const changeId = `encode-${testCase.label}`;
      const caughtWorkflow = workflow({
        name: `tests.encode-reentrant-caught-${testCase.label}`,
        version: 1,
        handler: async (_input: TestNoInput): Promise<string> => {
          try {
            testCase.call(changeId);
            return "durable call unexpectedly succeeded";
          } catch (error) {
            return (error as Error).message;
          }
        }
      });
      const caughtCommit = await prepareWorkflowTaskCommit(caughtWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json"
      });
      commitTraces[testCase.label] = caughtCommit.appendEvents?.map(commandTrace) ?? [];

      const reentrantWorkflow = workflow({
        name: `tests.encode-reentrant-${testCase.label}`,
        version: 1,
        handler: async (_input: TestNoInput): Promise<string> => {
          testCase.call(changeId);
          return "durable call unexpectedly succeeded";
        }
      });
      taskErrors[testCase.label] = await prepareWorkflowTaskCommit(
        reentrantWorkflow,
        {},
        fakeClaimed,
        { payloadCodec: "Json" }
      ).then(
        (commit) => `committed ${JSON.stringify(commit.appendEvents?.map(commandTrace))}`,
        (error: unknown) => String(error)
      );
    }

    // The rejected inner call appends no VersionMarker and the outer command no
    // event of its own. Without the guard each row records the out-of-order
    // pair `[VersionMarker#N+1, <OuterCommand>#N]` instead.
    expect(commitTraces).toEqual(
      Object.fromEntries(cases.map((testCase) => [testCase.label, ["WorkflowCompleted"]]))
    );

    for (const testCase of cases) {
      expect(taskErrors[testCase.label]).toContain(
        `nondeterminism: durable APIs are not re-entrant; ` +
          `getVersion(encode-${testCase.label}) ran inside ${testCase.frame} while it was ` +
          `converting user-supplied values.`
      );
    }
  });

  it("appends no marker when the workflow output encoding re-enters a durable API", async () => {
    // `completeWorkflow` runs in the `.then` attached outside
    // `runtimeStorage.run`, so the output's `toJSON` has no AsyncLocalStorage
    // store and cannot reach the context at all — the encode guard on that path
    // is shadowed by the ALS boundary and never fires. What matters either way
    // is the observable contract asserted below: the re-entrant call appends no
    // VersionMarker ahead of the terminal event. This test holds whichever
    // mechanism stops it, so it still pins the invariant if `completeWorkflow`
    // ever moves inside the store.
    const reentrantOutputWorkflow = workflow({
      name: "tests.encode-reentrant-output",
      version: 1,
      handler: async (_input: TestNoInput): Promise<{ readonly ok: boolean }> =>
        ({
          ok: true,
          toJSON(): { readonly version: number } {
            return { version: getVersion("encode-output", 1, 2) };
          }
        }) as unknown as { readonly ok: boolean }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/encode-reentrant-output"),
      workflowType: reentrantOutputWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [reentrantOutputWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected claim");
    }

    let commit: WorkflowTaskCommit | null = null;
    let taskError: unknown = null;
    try {
      commit = await prepareWorkflowTaskCommit(reentrantOutputWorkflow, {}, claim, {
        payloadCodec: "Json"
      });
    } catch (error) {
      taskError = error;
    }
    if (commit !== null) {
      await backend.commitWorkflowTask(claim.claim, commit);
    }

    const history = await backend.streamHistory({
      runId: claim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    // No VersionMarker, and nothing appended ahead of the terminal event.
    expect(history.events.map(commandTrace)).toEqual(["WorkflowStarted", "WorkflowFailed"]);
    expect(taskError).toBeNull();
    const failed = commit?.appendEvents?.[0]?.data;
    if (failed?.kind !== "WorkflowFailed") {
      throw new Error("expected WorkflowFailed");
    }
    expect(failed.failure.message).toBe("durust durable APIs must be awaited inside a workflow task");
  });

  it("rejects parking on a hot waiter from inside a durable command's encoding", async () => {
    // The encode window is also the only place a `toJSON` could reach
    // `hotSuspendByKey`, whose unconditional `.set()` would replace the waiter
    // the workflow is already parked on for that command.
    let spawned: ActivityHandle<QuoteOutput> | null = null;
    const waiterWorkflow = workflow({
      name: "tests.encode-reentrant-hot-waiter",
      version: 1,
      handler: async (_input: TestNoInput): Promise<string> => {
        spawned = await callActivity(priceQuote, { sku: "outer" }, {
          taskQueue: "payments"
        }).spawn();
        const handle = spawned;
        void callActivity(
          priceQuote,
          {
            sku: "inner",
            toJSON(): { readonly sku: string } {
              void handle.result().then(() => undefined);
              return { sku: "inner" };
            }
          } as unknown as QuoteInput,
          { taskQueue: "payments" }
        ).then(() => undefined);
        return "unreachable";
      }
    });

    await expect(
      prepareWorkflowTaskCommit(waiterWorkflow, {}, fakeClaimed, { payloadCodec: "Json" })
    ).rejects.toThrow(
      "nondeterminism: durable APIs are not re-entrant; " +
        "activityHandle.result(payments.price-quote) ran inside callActivity(payments.price-quote) " +
        "while it was converting user-supplied values."
    );
  });

  it("allows a durable API called from a map-manifest item conversion", async () => {
    // SPEC.md §16 makes the map-manifest builders the documented exception to
    // the re-entrancy rule: `activityMapManifest` allocates no command and opens
    // no guarded window, so the item conversions the caller supplies run outside
    // it and a durable API called from one is legal. Its command is allocated
    // and appended before the map command that consumes the manifest exists.
    //
    // The item schema's `encode` is the hook that matters here rather than
    // `toJSON`: the builder encodes items with MessagePack unless told
    // otherwise, and MessagePack does not honour `toJSON`.
    let encoded = 0;
    const manifestWorkflow = workflow({
      name: "tests.manifest-item-durable-call",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> => {
        const versionedItems: SchemaAdapter<QuoteInput> = {
          fingerprint: "sha256:tests-manifest-item",
          rootKind: "object",
          encode: (item: QuoteInput): unknown => {
            encoded += 1;
            return { ...item, version: getVersion(`item-${item.sku}`, 1, 2) };
          }
        };
        const mapped = activityMap(priceQuote, {
          inputManifest: activityMapManifest([{ sku: "a" }, { sku: "b" }], {
            pageSize: 2,
            itemSchema: versionedItems
          }),
          resultManifest: "quotes",
          taskQueue: "payments",
          maxInFlight: 2
        });
        await mapped.resultManifest();
        return encoded;
      }
    });

    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/manifest-item-durable-call"),
      workflowType: manifestWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [manifestWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected claim");
    }

    // The task does not fail, and both markers are allocated in call order
    // ahead of the map command's own seq.
    const commit = await prepareWorkflowTaskCommit(manifestWorkflow, {}, claim, {
      payloadCodec: "Json"
    });
    expect(commit.appendEvents?.map(commandTrace)).toEqual([
      "VersionMarker#1",
      "VersionMarker#2",
      "ActivityMapScheduled#3"
    ]);
    expect(
      commit.appendEvents?.flatMap((event) =>
        event.data.kind === "VersionMarker"
          ? [[event.data.marker.changeId, Number(event.data.marker.commandId.seq)] as const]
          : []
      )
    ).toEqual([
      ["item-a", 1],
      ["item-b", 2]
    ]);
    expect(encoded).toBe(2);
    await backend.commitWorkflowTask(claim.claim, commit);

    // And it replays: the handler re-runs from the top on a cold replay, so the
    // conversions run again and must match the recorded commands in the same
    // order. This holds because of the replay model itself, not because any
    // runtime path re-encodes.
    const history = await backend.streamHistory({
      runId: claim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map(commandTrace)).toEqual([
      "WorkflowStarted",
      "VersionMarker#1",
      "VersionMarker#2",
      "ActivityMapScheduled#3"
    ]);
    encoded = 0;
    const replayCommit = await prepareWorkflowTaskCommit(
      manifestWorkflow,
      {},
      {
        ...claim,
        replayTargetEventId: eventId(4),
        prefetchedHistory: history.events
      },
      { payloadCodec: "Json" }
    );
    expect(replayCommit.appendEvents).toEqual([]);
    expect(encoded).toBe(2);
  });

  it("restores the durable API guard after a sideEffect callback throws", async () => {
    const recoveringWorkflow = workflow({
      name: "tests.side-effect-guard-restored",
      version: 1,
      handler: async (
        _input: TestNoInput
      ): Promise<{
        readonly caught: readonly string[];
        readonly version: number;
        readonly recorded: string;
      }> => {
        const caught: string[] = [];
        try {
          await sideEffect("boom", (): string => {
            throw new Error("callback failed");
          });
        } catch (error) {
          caught.push((error as Error).name);
        }
        try {
          await sideEffect("reentrant", () => getVersion("inner-change", 1, 2));
        } catch (error) {
          caught.push((error as Error).name);
        }
        // Latched on either throw path, both of these would fail instead.
        const version = getVersion("outer-change", 1, 2);
        const recorded = await sideEffect("after", () => "recorded");
        return { caught, version, recorded };
      }
    });

    const commit = await prepareWorkflowTaskCommit(recoveringWorkflow, {}, fakeClaimed, {
      payloadCodec: "Json"
    });
    // Seqs 1 and 2 belong to the two failed side effects, which append nothing;
    // the surviving markers keep allocation order.
    expect(commit.appendEvents?.map(commandTrace)).toEqual([
      "VersionMarker#3",
      "SideEffectMarker#4",
      "WorkflowCompleted"
    ]);
    const completed = commit.appendEvents?.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(completed.result)).toEqual({
      caught: ["Error", "Error"],
      version: 2,
      recorded: "recorded"
    });
  });

  it("does not emit unhandled rejections when a sideEffect callback returns a rejecting promise", async () => {
    const cases: readonly { readonly label: string; readonly effect: () => PromiseLike<string> }[] =
      [
        {
          label: "throw-before-await",
          effect: async (): Promise<string> => {
            throw new Error("boom-sync");
          }
        },
        {
          label: "throw-after-await",
          effect: async (): Promise<string> => {
            await Promise.resolve();
            throw new Error("boom-late");
          }
        },
        {
          label: "rejected-promise",
          effect: (): PromiseLike<string> => Promise.reject(new Error("boom-plain"))
        }
      ];

    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      for (const testCase of cases) {
        const rejectingWorkflow = workflow({
          name: `tests.side-effect-rejecting-${testCase.label}`,
          version: 1,
          handler: async (_input: TestNoInput): Promise<string> =>
            await sideEffect("async-key", testCase.effect)
        });

        await expect(
          prepareWorkflowTaskCommit(rejectingWorkflow, {}, fakeClaimed, { payloadCodec: "Json" })
        ).rejects.toThrow(
          'nondeterminism: sideEffect callback must be synchronous; side effect "async-key" ' +
            "returned a promise."
        );
        await flushUnhandledRejectionTurn();
        // Nothing else ever attaches a handler to the callback's promise, so
        // without adopting it the rejection reaches Node's default
        // --unhandled-rejections=throw and kills the worker process.
        expect(unhandledRejections).toEqual([]);
      }
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("rejects a sideEffect callback that returns a promise", async () => {
    const asyncWorkflow = workflow({
      name: "tests.side-effect-async",
      version: 1,
      handler: async (_input: TestNoInput): Promise<string> =>
        await sideEffect("async-key", () => Promise.resolve("async-value"))
    });

    await expect(
      prepareWorkflowTaskCommit(asyncWorkflow, {}, fakeClaimed, { payloadCodec: "Json" })
    ).rejects.toThrow(
      'nondeterminism: sideEffect callback must be synchronous; side effect "async-key" ' +
        "returned a promise."
    );
  });

  it("commits nothing when an async sideEffect callback calls a durable API after an await", async () => {
    let durableCallsAfterAwait = 0;
    const asyncWorkflow = workflow({
      name: "tests.side-effect-async-reentrant",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> =>
        await sideEffect("async-key", async () => {
          await Promise.resolve();
          durableCallsAfterAwait += 1;
          return getVersion("after-await", 1, 2);
        })
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/side-effect-async-reentrant"),
      workflowType: asyncWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [asyncWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected claim");
    }

    let commit: WorkflowTaskCommit | null = null;
    let taskError: unknown = null;
    try {
      commit = await prepareWorkflowTaskCommit(asyncWorkflow, {}, claim, { payloadCodec: "Json" });
    } catch (error) {
      taskError = error;
    }
    if (commit !== null) {
      await backend.commitWorkflowTask(claim.claim, commit);
    }
    await Promise.resolve();

    // The continuation after the await runs detached from the task, so the
    // re-entrancy guard cannot cover it: it has already been released by the
    // time `getVersion` runs. Rejecting the promise return is what contains the
    // damage — the task fails, so the VersionMarker that detached call appended
    // to the abandoned context never reaches a commit.
    expect(durableCallsAfterAwait).toBe(1);
    expect(commit).toBeNull();
    expect(String(taskError)).toContain(
      'nondeterminism: sideEffect callback must be synchronous; side effect "async-key"'
    );
    const history = await backend.streamHistory({
      runId: claim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map(commandTrace)).toEqual(["WorkflowStarted"]);
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
      prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json",
        nondeterminismGuards: true
      })
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
      prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json",
        nondeterminismGuards: true
      })
    ).rejects.toThrow("nondeterminism: Math.random() is not allowed inside workflow code");
    expect(() => Math.random()).not.toThrow();
  });

  // 0017 row 5C: `process.env` is not patched at runtime at all — no Proxy and
  // no accessor. An accessor that throws on every `process.env` read is a
  // false-positive generator rather than a determinism guard, because Node's own
  // `Console` reads the environment to detect colour support whenever an
  // argument is not already a string: with the accessor in place,
  // `console.log(someObject)` inside workflow code threw and named
  // `process.env`, an API the author never wrote. Guards default on in
  // development and test, which is exactly where people log.
  //
  // `durust/no-hidden-io` rejects `process.env` statically in every spelling and
  // names the right API — see determinism-lint.test.ts, "rejects every process
  // API the runtime guard no longer covers". These three tests pin the runtime
  // half of that trade so it cannot be reintroduced by accident.
  it("does not guard process.env reads inside workflow code", async () => {
    const previousEnvValue = process.env.DURUST_TEST_ENV;
    process.env.DURUST_TEST_ENV = "visible";
    try {
      const readWorkflow = workflow({
        name: "tests.unguarded-process-env-read",
        version: 1,
        handler: async (_input: TestNoInput): Promise<string | undefined> =>
          process.env.DURUST_TEST_ENV
      });

      const commit = await prepareWorkflowTaskCommit(readWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json",
        nondeterminismGuards: true
      });
      const completed = commit.appendEvents?.[0]?.data;
      if (completed?.kind !== "WorkflowCompleted") {
        throw new Error("expected WorkflowCompleted");
      }
      expect(decodePayload(completed.result)).toBe("visible");
    } finally {
      if (previousEnvValue === undefined) {
        delete process.env.DURUST_TEST_ENV;
      } else {
        process.env.DURUST_TEST_ENV = previousEnvValue;
      }
    }
  });

  // The concrete false positive that decided row 5C. `Console` detects colour
  // support whenever an argument is not already a string, and that detection
  // reads `process.env`; under the accessor guard this threw
  // `nondeterminism: process.env ...` from a line the author wrote as
  // `console.log`.
  it("lets workflow code console.log an object without tripping a guard", async () => {
    const loggingWorkflow = workflow({
      name: "tests.workflow-console-log-object",
      version: 1,
      // Node's own `Console` over a sink, not `globalThis.console`: vitest
      // replaces the global one with an interceptor that does no colour
      // detection, so this would pass even with the guard reinstated.
      handler: async (_input: TestNoInput): Promise<string> => {
        nodeConsole.log({ nested: { value: 1 } });
        return "logged";
      }
    });

    const commit = await prepareWorkflowTaskCommit(loggingWorkflow, {}, fakeClaimed, {
      payloadCodec: "Json",
      nondeterminismGuards: true
    });
    const completed = commit.appendEvents?.[0]?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload<string>(completed.result)).toBe("logged");
  });

  it("leaves process.env an ordinary data property while guards are installed", async () => {
    const installer = workflow({
      name: "tests.process-env-descriptor",
      version: 1,
      handler: async (_input: TestNoInput): Promise<string> => "installed"
    });
    await prepareWorkflowTaskCommit(installer, {}, fakeClaimed, {
      payloadCodec: "Json",
      nondeterminismGuards: true
    });

    const descriptor = Object.getOwnPropertyDescriptor(process, "env");
    expect(descriptor?.get).toBeUndefined();
    expect(descriptor?.set).toBeUndefined();
    expect(descriptor?.value).toBe(originalProcessEnvObject);
    expect(process.env).toBe(originalProcessEnvObject);
  });

  it("rejects process.cwd inside workflow code", async () => {
    const cwdWorkflow = workflow({
      name: "tests.nondeterministic-process-cwd",
      version: 1,
      handler: async (_input: TestNoInput): Promise<string> => process.cwd()
    });

    await expect(
      prepareWorkflowTaskCommit(cwdWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json",
        nondeterminismGuards: true
      })
    ).rejects.toThrow("nondeterminism: process.cwd() is not allowed inside workflow code");
    expect(() => process.cwd()).not.toThrow();
  });

  // 0017 row 5B narrowed the guarded set to the APIs whose results workflow
  // logic plausibly branches on. These four stayed patched for marginal
  // determinism value while widening the blast radius: every patched global is a
  // permanent identity change visible to every library in the host process.
  // `process.uptime()` is NOT in this list — see the guarded table above; it is
  // a monotonic clock, not introspection, and keeping it costs nothing.
  // `durust/no-native-async` rejects all four statically; this test pins that
  // the runtime no longer does, so the removal cannot be undone by accident.
  it.each([
    // chdir'ing to the directory the process is already in keeps the test inert.
    { apiName: "process.chdir()", run: (): unknown => process.chdir(currentWorkingDirectory) },
    { apiName: "process.cpuUsage()", run: (): unknown => process.cpuUsage() },
    { apiName: "process.memoryUsage()", run: (): unknown => process.memoryUsage() },
    { apiName: "process.memoryUsage.rss()", run: (): unknown => process.memoryUsage.rss() },
    { apiName: "process.resourceUsage()", run: (): unknown => process.resourceUsage() }
  ])(
    "no longer guards process-introspection API $apiName at runtime",
    async ({ apiName, run }) => {
      const introspectionWorkflow = workflow({
        name: `tests.unguarded-${apiName}`,
        version: 1,
        handler: async (_input: TestNoInput): Promise<string> => {
          run();
          return "reached";
        }
      });

      const commit = await prepareWorkflowTaskCommit(introspectionWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json",
        nondeterminismGuards: true
      });
      const completed = commit.appendEvents?.[0]?.data;
      if (completed?.kind !== "WorkflowCompleted") {
        throw new Error("expected WorkflowCompleted");
      }
      expect(decodePayload(completed.result)).toBe("reached");
    }
  );

  it.each([
    {
      apiName: "Date()",
      run: (): unknown => Date()
    },
    {
      apiName: "new Date()",
      run: (): unknown => new Date()
    },
    {
      apiName: "performance.now()",
      run: (): unknown => performance.now()
    },
    {
      apiName: "crypto.randomUUID()",
      run: (): unknown => crypto.randomUUID()
    },
    {
      apiName: "crypto.getRandomValues()",
      run: (): unknown => crypto.getRandomValues(new Uint8Array(1))
    },
    {
      apiName: "process.hrtime()",
      run: (): unknown => process.hrtime()
    },
    {
      apiName: "process.hrtime.bigint()",
      run: (): unknown => process.hrtime.bigint()
    },
    {
      // Kept against row 5B's drop list: a monotonic clock, same class as
      // performance.now() and process.hrtime(), which both stay guarded.
      apiName: "process.uptime()",
      run: (): unknown => process.uptime()
    }
  ])("rejects nondeterministic value API $apiName inside workflow code", async ({ apiName, run }) => {
    const invalidWorkflow = workflow({
      name: `tests.nondeterministic-${apiName}`,
      version: 1,
      handler: async (_input: TestNoInput): Promise<unknown> => run()
    });

    await expect(
      prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json",
        nondeterminismGuards: true
      })
    ).rejects.toThrow(`nondeterminism: ${apiName} is not allowed inside workflow code`);
  });

  it("allows deterministic Date construction inside workflow code", async () => {
    const deterministicDateWorkflow = workflow({
      name: "tests.deterministic-date-construction",
      version: 1,
      handler: async (_input: TestNoInput): Promise<string> => new Date(0).toISOString()
    });

    const commit = await prepareWorkflowTaskCommit(deterministicDateWorkflow, {}, fakeClaimed, {
      payloadCodec: "Json"
    });
    const completed = commit.appendEvents?.[0]?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(completed.result)).toBe("1970-01-01T00:00:00.000Z");
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

  it("allows value-producing nondeterministic globals inside sideEffect", async () => {
    const previousEnvValue = process.env.DURUST_TEST_ENV;
    process.env.DURUST_TEST_ENV = "recorded-env";
    let commit;
    try {
      const recordedWorkflow = workflow({
        name: "tests.side-effect-native-values",
        version: 1,
        handler: async (_input: TestNoInput): Promise<{
          readonly dateNow: number;
          readonly dateStringLength: number;
          readonly constructedDateLength: number;
          readonly performanceNow: number;
          readonly uuidLength: number;
          readonly randomByte: number;
          readonly hrtimeLength: number;
          readonly hrtimeBigintNonNegative: boolean;
          readonly cwdLength: number;
          readonly cpuUsageUser: number;
          readonly memoryUsageRss: number;
          readonly memoryUsageRssDirect: number;
          readonly resourceUsageUserCpu: number;
          readonly uptimeNonNegative: boolean;
          readonly envValue: string | undefined;
        }> =>
          await sideEffect("record-native-values", () => ({
            dateNow: Date.now(),
            dateStringLength: Date().length,
            constructedDateLength: new Date().toISOString().length,
            performanceNow: performance.now(),
            uuidLength: crypto.randomUUID().length,
            randomByte: crypto.getRandomValues(new Uint8Array(1))[0] ?? -1,
            hrtimeLength: process.hrtime().length,
            hrtimeBigintNonNegative: process.hrtime.bigint() >= 0n,
            cwdLength: process.cwd().length,
            cpuUsageUser: process.cpuUsage().user,
            memoryUsageRss: process.memoryUsage().rss,
            memoryUsageRssDirect: process.memoryUsage.rss(),
            resourceUsageUserCpu: process.resourceUsage().userCPUTime,
            uptimeNonNegative: process.uptime() >= 0,
            envValue: process.env.DURUST_TEST_ENV
          }))
      });

      commit = await prepareWorkflowTaskCommit(recordedWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json",
        nondeterminismGuards: true
      });
    } finally {
      if (previousEnvValue === undefined) {
        delete process.env.DURUST_TEST_ENV;
      } else {
        process.env.DURUST_TEST_ENV = previousEnvValue;
      }
    }
    const marker = commit.appendEvents?.[0]?.data;
    if (marker?.kind !== "SideEffectMarker") {
      throw new Error("expected SideEffectMarker");
    }
    const recorded = decodePayload<{
      readonly dateNow: number;
      readonly dateStringLength: number;
      readonly constructedDateLength: number;
      readonly performanceNow: number;
      readonly uuidLength: number;
      readonly randomByte: number;
      readonly hrtimeLength: number;
      readonly hrtimeBigintNonNegative: boolean;
      readonly cwdLength: number;
      readonly cpuUsageUser: number;
      readonly memoryUsageRss: number;
      readonly memoryUsageRssDirect: number;
      readonly resourceUsageUserCpu: number;
      readonly uptimeNonNegative: boolean;
      readonly envValue: string | undefined;
    }>(marker.marker.value);
    expect(recorded.dateNow).toBeGreaterThan(0);
    expect(recorded.dateStringLength).toBeGreaterThan(0);
    expect(recorded.constructedDateLength).toBe("1970-01-01T00:00:00.000Z".length);
    expect(recorded.performanceNow).toBeGreaterThanOrEqual(0);
    expect(recorded.uuidLength).toBe(36);
    expect(recorded.randomByte).toBeGreaterThanOrEqual(0);
    expect(recorded.hrtimeLength).toBe(2);
    expect(recorded.hrtimeBigintNonNegative).toBe(true);
    expect(recorded.cwdLength).toBeGreaterThan(0);
    expect(recorded.cpuUsageUser).toBeGreaterThanOrEqual(0);
    expect(recorded.memoryUsageRss).toBeGreaterThan(0);
    expect(recorded.memoryUsageRssDirect).toBeGreaterThan(0);
    expect(recorded.resourceUsageUserCpu).toBeGreaterThanOrEqual(0);
    expect(recorded.uptimeNonNegative).toBe(true);
    expect(recorded.envValue).toBe("recorded-env");
  });

  it.each([
    {
      apiName: "fetch()",
      run: async (): Promise<unknown> => await fetch("data:text/plain,workflow")
    },
    ...(typeof WebSocket === "function"
      ? [
          {
            apiName: "new WebSocket()",
            run: (): unknown => new WebSocket("wss://example.com")
          }
        ]
      : [])
  ])("rejects network API $apiName inside workflow code", async ({ apiName, run }) => {
    const invalidWorkflow = workflow({
      name: `tests.nondeterministic-network-${apiName}`,
      version: 1,
      handler: async (_input: TestNoInput): Promise<unknown> => await run()
    });

    await expect(
      prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json",
        nondeterminismGuards: true
      })
    ).rejects.toThrow(`nondeterminism: ${apiName} is not allowed inside workflow code`);
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
      apiName: "setImmediate()",
      replacement: "durust durable operations",
      run: (): void => {
        setImmediate(() => undefined);
      }
    },
    {
      apiName: "process.nextTick()",
      replacement: "durust durable operations",
      run: (): void => {
        process.nextTick(() => undefined);
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
      prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json",
        nondeterminismGuards: true
      })
    ).rejects.toThrow(`nondeterminism: ${apiName} is not allowed inside workflow code`);
  });

  it("reapplies the process.nextTick guard if another module replaces it", async () => {
    const savedNextTick = process.nextTick;
    Object.defineProperty(process, "nextTick", {
      configurable: true,
      writable: true,
      value: ((callback: (...args: any[]) => void, ...args: any[]) =>
        savedNextTick(callback, ...args)) as typeof process.nextTick
    });

    const invalidWorkflow = workflow({
      name: "tests.nondeterministic-reapplied-next-tick",
      version: 1,
      handler: async (_input: TestNoInput): Promise<string> => {
        process.nextTick(() => undefined);
        return "unreachable";
      }
    });

    await expect(
      prepareWorkflowTaskCommit(invalidWorkflow, {}, fakeClaimed, {
        payloadCodec: "Json",
        nondeterminismGuards: true
      })
    ).rejects.toThrow("nondeterminism: process.nextTick() is not allowed inside workflow code");
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

  it("keeps a hot async workflow frame alive across select winner resolution", async () => {
    const trace: string[] = [];
    const racingWorkflow = workflow({
      name: "orders.hot-select",
      version: 1,
      handler: async (
        input: CheckoutInput
      ): Promise<
        | { readonly branch: "quote"; readonly cents: number }
        | { readonly branch: "delay" }
      > => {
        trace.push("before-select");
        const winner = await select({
          quote: callActivity(priceQuote, { sku: input.sku }, { taskQueue: "payments" }),
          delay: sleepUntil(1_000)
        });
        trace.push(`after-select:${winner.branch}`);
        if (winner.branch === "quote") {
          return { branch: "quote", cents: winner.value.cents };
        }
        return { branch: "delay" };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-select"),
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
    const hot = new HotWorkflowExecution(racingWorkflow, { sku: "sku-1" }, firstClaim, {
      payloadCodec: "Json"
    });
    const waitCommit = await hot.nextCommit();
    expect(waitCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled",
      "TimerStarted"
    ]);
    expect(trace).toEqual(["before-select"]);
    hot.markCommitted(committedTail(await backend.commitWorkflowTask(firstClaim.claim, waitCommit)));

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
    const completionCommit = await hot.advance(secondClaim);
    expect(completionCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "SelectWinner",
      "WorkflowCompleted"
    ]);
    const winner = completionCommit.appendEvents?.[0]?.data;
    if (winner?.kind !== "SelectWinner") {
      throw new Error("expected SelectWinner");
    }
    expect(winner.winner.branchOrdinal).toBe(0);
    expect(trace).toEqual(["before-select", "after-select:quote"]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(secondClaim.claim, completionCommit))
    );
    expect(hot.closed).toBe(true);
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

  it("settles parked durable-API waiters when a hot execution is disposed", async () => {
    const trace: string[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const { hot } = await startHotExecution("tests.dispose-parked-waiter", async (_input: TestNoInput) => {
        trace.push("start");
        try {
          await callActivity(priceQuote, { sku: "sku-1" }, { taskQueue: "payments" });
        } catch (error) {
          trace.push(
            error instanceof HotWorkflowExecutionDisposedError
              ? `waiter:${error.reason}`
              : `waiter:unexpected:${String(error)}`
          );
          throw error;
        }
        trace.push("resumed");
        return "done";
      });
      const scheduleCommit = await hot.nextCommit();
      expect(scheduleCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
        "ActivityScheduled"
      ]);
      // The commit above proves the execution was live before disposal.
      expect(trace).toEqual(["start"]);

      hot.dispose("unit disposal");

      await flushUnhandledRejectionTurn();
      // The parked frame unwound instead of staying pending forever, and the
      // rejection it received names the disposal rather than a workflow fault.
      expect(trace).toEqual(["start", "waiter:unit disposal"]);
      // Disposal is observed through the error it raises, not through a getter
      // on the public surface that no production caller needs.
      await expect(hot.nextCommit()).rejects.toThrow(HotWorkflowExecutionDisposedError);
      expect(unhandledRejections).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("disposes idempotently and never produces a commit afterwards", async () => {
    const trace: string[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const { hot, claim } = await startHotExecution("tests.dispose-no-commit", async (_input: TestNoInput) => {
        trace.push("start");
        try {
          await callActivity(priceQuote, { sku: "sku-1" }, { taskQueue: "payments" });
        } catch {
          // Worst case for the no-commit invariant: the workflow swallows the
          // disposal and returns normally, so the handler chain settles
          // *fulfilled* with a terminal value while the execution is disposed.
          trace.push("swallowed");
        }
        return "done";
      });
      await hot.nextCommit();
      expect(trace).toEqual(["start"]);

      hot.dispose("first disposal");
      // Evicted, then the same run conflicts: the second call must be a no-op
      // and must not replace the recorded reason.
      hot.dispose("second disposal");
      await flushUnhandledRejectionTurn();
      expect(trace).toEqual(["start", "swallowed"]);

      await expect(hot.nextCommit()).rejects.toThrow(
        "durust: hot workflow execution disposed (first disposal)"
      );
      await expect(hot.advance(claim)).rejects.toThrow(HotWorkflowExecutionDisposedError);
      expect(() => hot.markCommitted(eventId(2))).toThrow(HotWorkflowExecutionDisposedError);
      expect(hot.closed).toBe(false);
      expect(unhandledRejections).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("makes a detached side-effect continuation inert after disposal", async () => {
    // One case per shape of context mutation a stranded frame could still
    // reach: appending a command event, starting a timer, setting the query
    // projection, recording a terminal event, and recording a marker.
    const durableCalls: readonly {
      readonly label: string;
      readonly call: () => Promise<void>;
    }[] = [
      {
        label: "callActivity",
        call: async (): Promise<void> => {
          await callActivity(priceQuote, { sku: "late" }, { taskQueue: "payments" });
        }
      },
      {
        label: "sleep",
        call: async (): Promise<void> => {
          await sleep(1_000);
        }
      },
      {
        label: "publish",
        call: async (): Promise<void> => {
          publish({ late: true });
        }
      },
      {
        label: "continueAsNew",
        call: async (): Promise<void> => {
          continueAsNew({ late: true });
        }
      },
      {
        label: "sideEffect",
        call: async (): Promise<void> => {
          await sideEffect("late", () => "late");
        }
      }
    ];

    const trace: string[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      for (const durableCall of durableCalls) {
        let releaseContinuation = (): void => {};
        const continuationGate = new Promise<void>((resolve) => {
          releaseContinuation = resolve;
        });
        let markContinuationFinished = (): void => {};
        const continuationFinished = new Promise<void>((resolve) => {
          markContinuationFinished = resolve;
        });

        const { hot } = await startHotExecution(
          `tests.dispose-detached-${durableCall.label}`,
          async (_input: TestNoInput) => {
            // An async `sideEffect` callback fails the task synchronously, but
            // its continuation survives the failure holding a live reference to
            // the context the worker is about to abandon.
            return await sideEffect("detached", async () => {
              await continuationGate;
              try {
                await durableCall.call();
                trace.push(`${durableCall.label}:accepted`);
              } catch (error) {
                trace.push(
                  error instanceof HotWorkflowExecutionDisposedError
                    ? `${durableCall.label}:rejected:${error.reason}`
                    : `${durableCall.label}:unexpected:${String(error)}`
                );
              }
              markContinuationFinished();
              return "value";
            });
          }
        );

        await expect(hot.nextCommit()).rejects.toThrow(
          "nondeterminism: sideEffect callback must be synchronous"
        );

        hot.dispose("workflow task failed before commit");
        releaseContinuation();
        // Bounded, so that losing the guard shows up as a trace diff rather
        // than a framework timeout: an unguarded continuation that parks on a
        // fresh waiter in the abandoned context never finishes at all.
        await Promise.race([
          continuationFinished,
          new Promise<void>((resolve) => {
            setTimeout(resolve, 250);
          })
        ]);
        await flushUnhandledRejectionTurn();
        await expect(hot.nextCommit()).rejects.toThrow(HotWorkflowExecutionDisposedError);
      }

      // Asserted once over the whole table so a regression shows every affected
      // API at once. Inert, not merely unobserved: each call is refused at the
      // durable-API gate, so nothing reaches the abandoned context at all.
      expect(trace).toEqual(
        durableCalls.map(
          (durableCall) => `${durableCall.label}:rejected:workflow task failed before commit`
        )
      );
      expect(unhandledRejections).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  // Row 4E. `#hotWaiters` is keyed by command id, so a second suspension on a
  // command that is already parked used to replace the first entry. The
  // displaced waiter's deferred was then unreachable — including from
  // `dispose()`, which settles waiters by walking that map — so the run hung
  // silently and stayed hung through disposal, with no side effect involved to
  // report the stall.
  it("refuses a second concurrent await on one handle and keeps the first working", async () => {
    const trace: string[] = [];
    let secondError: unknown = null;
    const doubleAwait = workflow({
      name: "orders.double-await-handle",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly cents: number }> => {
        const handle = await callActivity(
          priceQuote,
          { sku: input.sku },
          { taskQueue: "payments" }
        ).spawn();
        // Both frames start awaiting before either can settle, which is the
        // shape that used to clobber the first waiter.
        const firstAwait = handle.result().then(
          (quote) => {
            trace.push(`first:${quote.cents}`);
            return quote;
          },
          (error: unknown) => {
            trace.push("first:rejected");
            throw error;
          }
        );
        const secondAwait = handle.result().then(
          () => {
            trace.push("second:resolved");
          },
          (error: unknown) => {
            secondError = error;
            trace.push("second:rejected");
          }
        );
        await secondAwait;
        return await firstAwait;
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-double-await"),
      workflowType: doubleAwait.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ sku: "sku-1" }, { codec: "Json" })
    });
    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [doubleAwait.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const hot = new HotWorkflowExecution(doubleAwait, { sku: "sku-1" }, firstClaim, {
      payloadCodec: "Json"
    });

    // The task commits at all only because the second await was refused rather
    // than silently displacing the first. Before the fix both frames were
    // parked on deferreds nothing could settle and this never resolved.
    const scheduleCommit = await hot.nextCommit();
    expect(scheduleCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled"
    ]);
    expect(trace).toEqual(["second:rejected"]);
    expect((secondError as Error).message).toContain("is already awaited by another frame");
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(firstClaim.claim, scheduleCommit))
    );

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
      result: encodePayload<QuoteOutput>({ cents: 99 }, { codec: "Json" })
    });
    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [doubleAwait.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }

    // The retained waiter is the *first* one, and it still resolves normally.
    const completionCommit = await hot.advance(secondClaim);
    const completed = completionCommit.appendEvents?.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload<{ readonly cents: number }>(completed.result)).toEqual({ cents: 99 });
    expect(trace).toEqual(["second:rejected", "first:99"]);
  });

  // Row 4D. Reading a handle's result twice in sequence is ordinary workflow
  // code, and the ready event backing it is now consumed out of the runtime's
  // index on the first read, so the handle has to own the value afterwards.
  it("serves a second sequential read of one activity handle from the handle itself", async () => {
    const rereadHandle = workflow({
      name: "orders.handle-reread",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly cents: number }> => {
        const handle = await callActivity(
          priceQuote,
          { sku: input.sku },
          { taskQueue: "payments" }
        ).spawn();
        const first = await handle.result();
        const second = await handle.result();
        return { cents: first.cents + second.cents };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-handle-reread"),
      workflowType: rereadHandle.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ sku: "sku-1" }, { codec: "Json" })
    });
    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [rereadHandle.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const hot = new HotWorkflowExecution(rereadHandle, { sku: "sku-1" }, firstClaim, {
      payloadCodec: "Json"
    });
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(firstClaim.claim, await hot.nextCommit()))
    );
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
      result: encodePayload<QuoteOutput>({ cents: 7 }, { codec: "Json" })
    });
    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [rereadHandle.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const commit = await hot.advance(secondClaim);
    const completed = commit.appendEvents?.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload<{ readonly cents: number }>(completed.result)).toEqual({ cents: 14 });
  });

  // Row 4D's exactly-once rule, across tasks. Each branch's ready event is
  // consumed on the probe that resolves it, and the composite is re-probed on
  // every later ingest, so a branch that resolved in an earlier task must not
  // be asked to produce its value again from an index it has already left.
  //
  // Also covers a silent hang this test found: a wake that settles nothing —
  // the first of two branches completing — reported no progress at all, so
  // `nextCommit()` parked forever and the task never committed.
  it("settles a joinAll whose branches complete in separate tasks", async () => {
    const partialJoin = workflow({
      name: "orders.joinall-across-tasks",
      version: 1,
      handler: async (input: CheckoutInput): Promise<{ readonly cents: number }> => {
        const [first, second] = await joinAll([
          callActivity(priceQuote, { sku: `${input.sku}-a` }, { taskQueue: "payments" }),
          callActivity(priceQuote, { sku: `${input.sku}-b` }, { taskQueue: "payments" })
        ]);
        return {
          cents: (first as QuoteOutput).cents + (second as QuoteOutput).cents
        };
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/hot-joinall-across-tasks"),
      workflowType: partialJoin.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ sku: "sku-1" }, { codec: "Json" })
    });
    const claimWorkflow = async (worker: string): Promise<ClaimedWorkflowTask> => {
      const claimed = await backend.claimWorkflowTask(worker, {
        namespace: namespace(),
        taskQueue: taskQueue("workflows"),
        registeredWorkflowTypes: [partialJoin.workflowType],
        leaseDurationMs: 30_000
      });
      if (!claimed) {
        throw new Error(`expected a workflow claim for ${worker}`);
      }
      return claimed;
    };
    const completeOneActivity = async (cents: number): Promise<void> => {
      const task = await backend.claimActivityTask("activity-worker", {
        namespace: namespace(),
        taskQueue: taskQueue("payments"),
        registeredActivityNames: ["payments.price-quote"],
        leaseDurationMs: 30_000
      });
      if (!task) {
        throw new Error("expected an activity task");
      }
      await backend.completeActivity({
        claim: task.claim,
        result: encodePayload<QuoteOutput>({ cents }, { codec: "Json" })
      });
    };

    const firstClaim = await claimWorkflow("worker-a");
    const hot = new HotWorkflowExecution(partialJoin, { sku: "sku-1" }, firstClaim, {
      payloadCodec: "Json"
    });
    const scheduleCommit = await hot.nextCommit();
    expect(scheduleCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityScheduled",
      "ActivityScheduled"
    ]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(firstClaim.claim, scheduleCommit))
    );

    // Only the first branch completes. The join stays pending, so this task has
    // nothing to record — but it must still commit rather than park forever.
    await completeOneActivity(11);
    const partialClaim = await claimWorkflow("worker-b");
    const partialCommit = await hot.advance(partialClaim);
    expect(partialCommit.appendEvents ?? []).toEqual([]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(partialClaim.claim, partialCommit))
    );

    // The second branch completes in a later task, and the join is re-probed.
    // The first branch's completion left the runtime's index two tasks ago, so
    // the composite has to remember it.
    await completeOneActivity(31);
    const finalClaim = await claimWorkflow("worker-c");
    const finalCommit = await hot.advance(finalClaim);
    const completed = finalCommit.appendEvents?.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload<{ readonly cents: number }>(completed.result)).toEqual({ cents: 42 });
  });

  // Row 4D, the memory claim itself. Before removal-on-consume a hot workflow
  // kept one `HistoryEvent` — payload included — per ready event it had ever
  // seen, so retained memory grew with the number of activities the run had
  // completed and never came back down.
  //
  // Asserted as a *slope*, not as a band. An absolute band cannot tell flat
  // from leaky: a mutant retaining one completion in four draws a textbook
  // straight line and still lands inside any band wide enough to be robust.
  // What has to be true is that retained bytes do not grow with the number of
  // activities completed, so this runs the same workflow at two lengths and
  // measures the bytes each extra activity costs.
  it("retains no measurable memory per completed hot activity", async () => {
    const payloadBytes = 96 * 1024;
    const filler = "x".repeat(payloadBytes);

    // Retained bytes held by one hot execution after it has completed
    // `activityCount` activities, measured against the heap just before it was
    // created so the fixture itself cancels out.
    const retainedAfterActivities = async (activityCount: number): Promise<number> => {
      const manyActivities = workflow({
        name: `orders.hot-memory-soak-${activityCount}`,
        version: 1,
        handler: async (input: CheckoutInput): Promise<{ readonly total: number }> => {
          let total = 0;
          for (let index = 0; index < activityCount; index += 1) {
            const quote = await callActivity(
              priceQuote,
              { sku: `${input.sku}-${index}` },
              { taskQueue: "payments" }
            );
            total += quote.cents;
          }
          return { total };
        }
      });
      const claim = syntheticWorkflowClaim(
        manyActivities.workflowType,
        [
          {
            eventId: eventId(1),
            eventType: "WorkflowStarted",
            data: {
              kind: "WorkflowStarted",
              workflowType: manyActivities.workflowType,
              input: encodePayload({ sku: "sku" }, { codec: "Json" })
            }
          }
        ],
        eventId(1)
      );
      const baseline = retainedBytes();
      const hot = new HotWorkflowExecution(manyActivities, { sku: "sku" }, claim, {
        payloadCodec: "Json"
      });
      let tail = 1;
      for (let index = 0; index < activityCount; index += 1) {
        const commit = await (index === 0
          ? hot.nextCommit()
          : hot.advance(
              syntheticWorkflowClaim(
                manyActivities.workflowType,
                [
                  {
                    eventId: eventId(tail + 1),
                    eventType: "ActivityCompleted",
                    data: {
                      kind: "ActivityCompleted",
                      completed: {
                        commandId: { runId: claim.runId, seq: index },
                        // Big enough that retaining even a fraction of these is
                        // unmistakable next to measurement noise.
                        result: encodePayload({ cents: 1, filler }, { codec: "Json" })
                      }
                    }
                  }
                ],
                eventId(tail + 1)
              )
            ));
        const appended = commit.appendEvents?.length ?? 0;
        expect(appended).toBeGreaterThan(0);
        if (index > 0) {
          tail += 1;
        }
        tail += appended;
        hot.markCommitted(eventId(tail));
      }
      const retained = retainedBytes() - baseline;
      // Referenced after the measurement so the execution cannot be collected
      // before it is taken.
      expect(hot.closed).toBe(false);
      return retained;
    };

    const shortRun = 100;
    const longRun = 500;
    const retainedShort = await retainedAfterActivities(shortRun);
    const retainedLong = await retainedAfterActivities(longRun);
    const bytesPerActivity = (retainedLong - retainedShort) / (longRun - shortRun);

    // Retaining one completion in ten would cost ~9.8 KiB per activity here,
    // and one in four ~24 KiB. Flat costs a rounding error.
    expect(bytesPerActivity).toBeLessThan(2 * 1024);
  }, 120_000);
});

describe("map input manifest validation", () => {
  // What a map fans out over is declared by three fields that have to agree,
  // and the DSL takes any encoded object of the manifest's shape, so a
  // disagreeing one was user-reachable. It used to be found only inside the
  // provider's `commitWorkflowTask`, which fails the *task* rather than the
  // workflow and is therefore retried forever.
  async function firstCommit(
    definition: Parameters<typeof prepareWorkflowTaskCommit>[0],
    label: string
  ): Promise<WorkflowTaskCommit> {
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId(`wf/${label}`),
      workflowType: definition.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [definition.workflowType],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected claim");
    }
    return prepareWorkflowTaskCommit(definition, {}, claim, { payloadCodec: "Json" });
  }

  it("accepts an empty manifest and completes the map with no items", async () => {
    // Mapping over an empty list is a degenerate case, not an error: the map is
    // terminal the moment its descriptor exists. Pinned here at the DSL so the
    // answer is deliberate rather than an accident of the completion predicate.
    const emptyMapWorkflow = workflow({
      name: "tests.empty-activity-map",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> => {
        const mapped = activityMap(priceQuote, {
          inputManifest: activityMapManifest([]),
          resultManifest: "quotes",
          taskQueue: "payments",
          maxInFlight: 4
        });
        return decodeActivityMapResults<QuoteOutput>(await mapped.resultManifest()).length;
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/empty-activity-map"),
      workflowType: emptyMapWorkflow.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [emptyMapWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected claim");
    }
    const hot = new HotWorkflowExecution(emptyMapWorkflow, {}, claim, { payloadCodec: "Json" });
    const scheduleCommit = await hot.nextCommit();
    expect(scheduleCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "ActivityMapScheduled"
    ]);
    hot.markCommitted(
      committedTail(await backend.commitWorkflowTask(claim.claim, scheduleCommit))
    );

    // The provider completed the map inside that same commit, so the parent is
    // ready with no item ever having been scheduled.
    const noItem = await backend.claimActivityTask("map-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("payments"),
      registeredActivityNames: ["payments.price-quote"],
      leaseDurationMs: 30_000
    });
    expect(noItem).toBeNull();

    const completedClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [emptyMapWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!completedClaim) {
      throw new Error("an empty map must wake its parent");
    }
    expect(completedClaim.reason).toBe("ActivityMapCompleted");
    const finalCommit = await hot.advance(completedClaim);
    expect(finalCommit.appendEvents?.map((event) => event.data.kind)).toEqual([
      "WorkflowCompleted"
    ]);
    const completed = finalCommit.appendEvents?.[0]?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload<number>(completed.result)).toBe(0);
  });

  it("gives an activity map item the same command fingerprint when no options are supplied", async () => {
    // `retry`, `startToCloseTimeoutMs` and `heartbeatTimeoutMs` became
    // caller-supplied on `ActivityMapOptions`, matching Rust's `activity_map`
    // builder, which carries a full `ActivityOptions`. They feed the command
    // fingerprint, so the defaults have to be exactly the values that were
    // hardcoded before, or every existing workflow's map command stops
    // replaying. Two runs of the same program, one before and one after, are
    // not comparable here; the durable fingerprint is, so it is pinned.
    const defaultsWorkflow = workflow({
      name: "tests.map-options-defaults",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> => {
        const mapped = activityMap(priceQuote, {
          inputManifest: activityMapManifest([{ sku: "a" }], 1),
          resultManifest: "quotes",
          taskQueue: "payments",
          maxInFlight: 2
        });
        return decodeActivityMapResults<QuoteOutput>(await mapped.resultManifest()).length;
      }
    });
    const commit = await firstCommit(defaultsWorkflow, "map-options-defaults");
    const scheduled = commit.appendEvents?.[0]?.data;
    if (scheduled?.kind !== "ActivityMapScheduled") {
      throw new Error("expected ActivityMapScheduled");
    }
    expect(scheduled.scheduled.retryPolicy.maxAttempts).toBe(1);
    expect(scheduled.scheduled.startToCloseTimeoutMs).toBeNull();
    expect(scheduled.scheduled.heartbeatTimeoutMs).toBeNull();
    // Pinned as a literal, not recomputed from the event, so a change to any
    // default moves it. `activityOptionsDigest` hashes exactly the four values
    // the map builder now takes from the caller.
    expect(scheduled.scheduled.fingerprint.optionsDigest).toBe(
      "sha256:595b2213c1b2fc84911306d9c5f32ad68fdc4f26fa6c00ae9fc55b72c96341ff" +
        ":result=quotes:max=2"
    );
    expect(scheduled.scheduled.fingerprint).toEqual(
      activityMapFingerprint(
        "payments.price-quote",
        payloadDigest(scheduled.scheduled.inputManifest),
        "quotes",
        2,
        "sha256:595b2213c1b2fc84911306d9c5f32ad68fdc4f26fa6c00ae9fc55b72c96341ff"
      )
    );
    expect(commit.scheduleActivityMaps?.[0]?.retryPolicy.maxAttempts).toBe(1);
  });

  it("carries an activity map's retry policy and deadlines onto every item", async () => {
    // The defect this closes: the DSL hardcoded one attempt and no deadlines,
    // and a map item still gets the implicit lease-length heartbeat deadline
    // every claimed activity gets. Once map items stopped being exempt from the
    // timeout scanner, a worker that died holding an item — or an item that
    // merely outran its lease — exhausted its only attempt and failed the whole
    // map, with no way for the caller to say otherwise.
    const configuredWorkflow = workflow({
      name: "tests.map-options-configured",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> => {
        const mapped = activityMap(priceQuote, {
          inputManifest: activityMapManifest([{ sku: "a" }], 1),
          resultManifest: "quotes",
          taskQueue: "payments",
          maxInFlight: 2,
          retry: RetryPolicy.exponential({ maxAttempts: 5, initialIntervalMs: 250 }),
          startToCloseTimeoutMs: 30_000,
          heartbeatTimeoutMs: 5_000
        });
        return decodeActivityMapResults<QuoteOutput>(await mapped.resultManifest()).length;
      }
    });
    const commit = await firstCommit(configuredWorkflow, "map-options-configured");
    const task = commit.scheduleActivityMaps?.[0];
    expect(task?.retryPolicy.maxAttempts).toBe(5);
    expect(task?.retryPolicy.initialIntervalMs).toBe(250);
    expect(task?.startToCloseTimeoutMs).toBe(30_000);
    expect(task?.heartbeatTimeoutMs).toBe(5_000);

    // The configured options must move the fingerprint, or a workflow could
    // change its map's retry policy and replay against the old history.
    const defaultsCommit = await firstCommit(
      workflow({
        name: "tests.map-options-configured",
        version: 1,
        handler: async (_input: TestNoInput): Promise<number> => {
          const mapped = activityMap(priceQuote, {
            inputManifest: activityMapManifest([{ sku: "a" }], 1),
            resultManifest: "quotes",
            taskQueue: "payments",
            maxInFlight: 2
          });
          return decodeActivityMapResults<QuoteOutput>(await mapped.resultManifest()).length;
        }
      }),
      "map-options-configured-defaults"
    );
    const configuredEvent = commit.appendEvents?.[0]?.data;
    const defaultEvent = defaultsCommit.appendEvents?.[0]?.data;
    if (
      configuredEvent?.kind !== "ActivityMapScheduled" ||
      defaultEvent?.kind !== "ActivityMapScheduled"
    ) {
      throw new Error("expected ActivityMapScheduled");
    }
    expect(configuredEvent.scheduled.fingerprint.optionsDigest).not.toEqual(
      defaultEvent.scheduled.fingerprint.optionsDigest
    );
  });

  it("rejects an activity map manifest whose pages do not cover its item count", async () => {
    const brokenWorkflow = workflow({
      name: "tests.broken-activity-map-manifest",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> => {
        const mapped = activityMap(priceQuote, {
          inputManifest: encodePayload({
            itemCount: 3,
            pageLengths: [1],
            pages: [encodePayload({ items: [encodePayload({ sku: "a" }, { codec: "Json" })] }, { codec: "Json" })]
          }, { codec: "Json" }) as PayloadRef<ActivityMapInputManifest<QuoteInput>>,
          resultManifest: "quotes",
          taskQueue: "payments",
          maxInFlight: 2
        });
        return decodeActivityMapResults<QuoteOutput>(await mapped.resultManifest()).length;
      }
    });
    const commit = await firstCommit(brokenWorkflow, "broken-activity-map-manifest");
    const failed = commit.appendEvents?.[0]?.data;
    expect(failed?.kind).toBe("WorkflowFailed");
    if (failed?.kind !== "WorkflowFailed") {
      throw new Error("expected WorkflowFailed");
    }
    expect(failed.failure.message).toBe(
      "activityMap inputManifest pages cover 1 items, expected 3"
    );
    // Rejected before the command id is allocated, so nothing was scheduled.
    expect(commit.scheduleActivityMaps ?? []).toHaveLength(0);
  });

  it("rejects a child workflow map manifest with a zero-length page", async () => {
    const brokenWorkflow = workflow({
      name: "tests.broken-child-map-manifest",
      version: 1,
      handler: async (_input: TestNoInput): Promise<number> => {
        const mapped = childWorkflowMap(childEchoWorkflow, {
          inputManifest: encodePayload({
            itemCount: 0,
            pageLengths: [0],
            pages: [encodePayload({ items: [] }, { codec: "Json" })]
          }, { codec: "Json" }) as PayloadRef<ActivityMapInputManifest<{ readonly value: string }>>,
          resultManifest: "echoes",
          workflowIdPrefix: "wf/broken-child-map",
          taskQueue: "workflows",
          maxInFlight: 2
        });
        return decodeChildWorkflowMapSuccesses(await mapped.resultManifest()).length;
      }
    });
    const commit = await firstCommit(brokenWorkflow, "broken-child-map-manifest");
    const failed = commit.appendEvents?.[0]?.data;
    expect(failed?.kind).toBe("WorkflowFailed");
    if (failed?.kind !== "WorkflowFailed") {
      throw new Error("expected WorkflowFailed");
    }
    expect(failed.failure.message).toBe(
      "childWorkflowMap inputManifest page 0 must hold at least one item, got 0"
    );
    expect(commit.scheduleChildWorkflowMaps ?? []).toHaveLength(0);
  });
});

// Builds a claim without a provider so a memory test measures the runtime and
// nothing else: a `MemoryBackend` accumulates the run's history by design, and
// that growth would swamp what is being asserted.
function syntheticWorkflowClaim(
  type: ReturnType<typeof workflowType>,
  prefetchedHistory: readonly HistoryEvent[],
  replayTargetEventId: ReturnType<typeof eventId>
): ClaimedWorkflowTask {
  return {
    runId: runId("run/memory"),
    workflowId: workflowId("wf/memory"),
    workflowType: type,
    claim: {
      runId: runId("run/memory"),
      workerId: "worker-memory",
      leaseToken: "lease-memory",
      leaseExpiresAtMs: 0
    } as ClaimedWorkflowTask["claim"],
    replayTargetEventId,
    reason: "Start",
    prefetchedHistory
  };
}

// Retained bytes after a forced full GC.
//
// `heapUsed` alone is not enough: payload bytes live in `Uint8Array` backing
// stores, which V8 accounts as external memory, so a test that watched only the
// JS heap would report a flat line whether or not the payloads were retained.
// The GC is triggered through `vm` rather than requiring `--expose-gc` on the
// runner so the assertion works under the project's normal test command.
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

// Starts a hot execution over a fresh backend so a disposal test can drive the
// runtime directly, without the worker in the way.
async function startHotExecution(
  name: string,
  handler: (input: TestNoInput) => Promise<unknown>
): Promise<{
  readonly hot: HotWorkflowExecution;
  readonly claim: ClaimedWorkflowTask;
}> {
  const definition = workflow({ name, version: 1, handler });
  const backend = new MemoryBackend();
  await backend.startWorkflow({
    namespace: namespace(),
    workflowId: workflowId(`wf/${name}`),
    workflowType: definition.workflowType,
    taskQueue: taskQueue("workflows"),
    input: encodePayload({}, { codec: "Json" })
  });
  const claim = await backend.claimWorkflowTask("worker-a", {
    namespace: namespace(),
    taskQueue: taskQueue("workflows"),
    registeredWorkflowTypes: [definition.workflowType],
    leaseDurationMs: 30_000
  });
  if (!claim) {
    throw new Error(`expected a workflow claim for ${name}`);
  }
  return {
    hot: new HotWorkflowExecution(definition, {}, claim, { payloadCodec: "Json" }),
    claim
  };
}

// Lets a pending unhandled rejection reach the process listener before it is
// asserted on. Mirrors the helper in worker.test.ts.
async function flushUnhandledRejectionTurn(): Promise<void> {
  await Promise.resolve();
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}
