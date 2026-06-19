import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import {
  Client,
  RetryPolicy,
  Registry,
  Worker,
  activity,
  activityMapFingerprint,
  activityMapManifest,
  childWorkflowMapFingerprint,
  callActivity,
  commandId,
  decodePayload,
  encodePayload,
  eventId,
  namespace,
  payloadDigest,
  prepareWorkflowTaskCommit,
  taskQueue,
  workflow,
  workflowId,
  workflowType,
  type ActivityMapResultManifest,
  type ActivityMapResultPage,
  type ChildWorkflowMapResultManifest,
  type ChildWorkflowMapResultPage,
  type PayloadRef
} from "@durust/core";
import { LocalDirectoryBlobStore, PayloadBackend } from "@durust/payload";
import { basicProviderConformanceCases } from "@durust/testing";
import { SqliteBackend } from "@durust/sqlite";

const roots: string[] = [];

function tempSqlitePath(label: string): string {
  const root = mkdtempSync(join(tmpdir(), `durust-sqlite-${label}-`));
  roots.push(root);
  return join(root, "durust.db");
}

function tempRoot(label: string): string {
  const root = mkdtempSync(join(tmpdir(), `durust-${label}-`));
  roots.push(root);
  return root;
}

afterAll(() => {
  for (const root of roots) {
    rmSync(root, { recursive: true, force: true });
  }
});

describe("SqliteBackend provider conformance", () => {
  for (const conformanceCase of basicProviderConformanceCases()) {
    it(conformanceCase.name, async () => {
      let counter = 0;
      await conformanceCase.run(() => new SqliteBackend({
        path: tempSqlitePath(`conformance-${counter++}`)
      }));
    });
  }
});

describe("SqliteBackend blob-backed provider conformance", () => {
  for (const conformanceCase of basicProviderConformanceCases()) {
    it(conformanceCase.name, async () => {
      let counter = 0;
      await conformanceCase.run(() => {
        const inner = new SqliteBackend({
          path: tempSqlitePath(`payload-conformance-${counter}`)
        });
        const blobStore = new LocalDirectoryBlobStore({
          root: tempRoot(`sqlite-payload-${counter++}`),
          prefix: "objects"
        });
        return new PayloadBackend({
          backend: inner,
          blobStore,
          inlineThresholdBytes: 0
        });
      });
    });
  }
});

describe("SqliteBackend persistence", () => {
  it("recovers a started workflow after close and reopen", async () => {
    const path = tempSqlitePath("reopen-started");
    const echo = workflow({
      name: "sqlite.echo",
      version: 1,
      handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> =>
        input
    });

    const first = new SqliteBackend({ path });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-reopen"),
      workflowType: echo.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    first.close();

    const reopened = new SqliteBackend({ path });
    const claimed = await reopened.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [echo.workflowType],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected reopened claim");
    }

    const commit = await prepareWorkflowTaskCommit(
      echo,
      { value: "ok" },
      claimed,
      { payloadCodec: "Json" }
    );
    await expect(reopened.commitWorkflowTask(claimed.claim, commit)).resolves.toEqual({
      kind: "Committed",
      newTailEventId: eventId(2)
    });

    const history = await reopened.streamHistory({
      runId: claimed.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "WorkflowCompleted"
    ]);
    reopened.close();
  });

  it("recovers workflow progress across worker restart and backend reopen", async () => {
    const path = tempSqlitePath("worker-restart-activity");
    const quote = activity({
      name: "sqlite.worker.quote",
      handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => ({
        cents: input.sku.length * 100
      })
    });
    const checkout = workflow({
      name: "sqlite.worker.checkout",
      version: 1,
      handler: async (input: { readonly sku: string }): Promise<{ readonly cents: number }> => {
        return await callActivity(quote, { sku: input.sku }, { taskQueue: "activities" });
      }
    });
    const registry = new Registry().registerWorkflow(checkout).registerActivity(quote);

    const first = new SqliteBackend({ path });
    const client = new Client(first, { namespace: namespace(), payloadCodec: "Json" });
    const firstWorker = new Worker({
      backend: first,
      registry,
      namespace: namespace(),
      workerId: "sqlite-worker-a",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const handle = await client.startWorkflow(
      checkout,
      workflowId("wf/sqlite-worker-restart"),
      "workflows",
      { sku: "sku-1" }
    );

    await expect(firstWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    first.close();

    const reopened = new SqliteBackend({ path });
    const activityWorker = new Worker({
      backend: reopened,
      registry,
      namespace: namespace(),
      workerId: "sqlite-worker-b",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    await expect(activityWorker.runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    reopened.close();

    const reopenedAgain = new SqliteBackend({ path });
    const workflowWorker = new Worker({
      backend: reopenedAgain,
      registry,
      namespace: namespace(),
      workerId: "sqlite-worker-c",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    await expect(workflowWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });

    const history = await reopenedAgain.streamHistory({
      runId: handle.runId,
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
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(
      decodePayload<{ readonly cents: number }>(
        completed.result as PayloadRef<{ readonly cents: number }>
      )
    ).toEqual({ cents: 500 });
    reopenedAgain.close();
  });

  it("recovers activity-map descriptor state after close and reopen", async () => {
    const path = tempSqlitePath("reopen-activity-map");
    const parentType = workflowType("sqlite.activity-map-parent", 1);
    const inputManifest = activityMapManifest(
      [{ value: 1 }, { value: 2 }, { value: 3 }],
      2
    );

    const first = new SqliteBackend({ path });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-activity-map-reopen"),
      workflowType: parentType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await first.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parentType],
      leaseDurationMs: 30_000
    });
    expect(claim).not.toBeNull();
    if (!claim) {
      throw new Error("expected activity-map parent claim");
    }
    const scheduled = {
      commandId: commandId(claim.runId, 1),
      activityName: "sqlite.map",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      inputManifest,
      resultManifestName: "mapped",
      maxInFlight: 2,
      fingerprint: activityMapFingerprint(
        "sqlite.map",
        payloadDigest(inputManifest),
        "mapped",
        2,
        "sha256:test-options"
      )
    };
    await first.commitWorkflowTask(claim.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ActivityMapScheduled", scheduled } }],
      scheduleActivityMaps: [
        {
          mapCommandId: scheduled.commandId,
          activityName: scheduled.activityName,
          taskQueue: scheduled.taskQueue,
          retryPolicy: scheduled.retryPolicy,
          startToCloseTimeoutMs: scheduled.startToCloseTimeoutMs,
          heartbeatTimeoutMs: scheduled.heartbeatTimeoutMs,
          inputManifest: scheduled.inputManifest,
          resultManifestName: scheduled.resultManifestName,
          maxInFlight: scheduled.maxInFlight
        }
      ]
    });
    first.close();

    const reopened = new SqliteBackend({ path });
    const firstItem = await reopened.claimActivityTask("map-worker-1", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["sqlite.map"],
      leaseDurationMs: 30_000
    });
    const secondItem = await reopened.claimActivityTask("map-worker-2", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["sqlite.map"],
      leaseDurationMs: 30_000
    });
    const blockedThird = await reopened.claimActivityTask("map-worker-3", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["sqlite.map"],
      leaseDurationMs: 30_000
    });
    expect(firstItem?.task.mapItem?.itemOrdinal).toBe(0);
    expect(secondItem?.task.mapItem?.itemOrdinal).toBe(1);
    expect(blockedThird).toBeNull();
    if (!firstItem || !secondItem) {
      throw new Error("expected first two reopened activity-map items");
    }

    await reopened.completeActivity({
      claim: firstItem.claim,
      result: encodePayload({ value: 10 }, { codec: "Json" })
    });
    reopened.close();

    const reopenedAgain = new SqliteBackend({ path });
    const thirdItem = await reopenedAgain.claimActivityTask("map-worker-3", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["sqlite.map"],
      leaseDurationMs: 30_000
    });
    expect(thirdItem?.task.mapItem?.itemOrdinal).toBe(2);
    if (!thirdItem) {
      throw new Error("expected third activity-map item after reopen");
    }

    await reopenedAgain.completeActivity({
      claim: secondItem.claim,
      result: encodePayload({ value: 20 }, { codec: "Json" })
    });
    await reopenedAgain.completeActivity({
      claim: thirdItem.claim,
      result: encodePayload({ value: 30 }, { codec: "Json" })
    });

    const parentReady = await reopenedAgain.claimWorkflowTask("map-parent", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parentType],
      leaseDurationMs: 30_000
    });
    expect(parentReady?.reason).toBe("ActivityMapCompleted");
    if (!parentReady) {
      throw new Error("expected activity-map parent wake");
    }

    const history = await reopenedAgain.streamHistory({
      runId: claim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityMapScheduled",
      "ActivityMapCompleted"
    ]);
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "ActivityMapCompleted") {
      throw new Error("expected ActivityMapCompleted");
    }
    const manifest = decodePayload<ActivityMapResultManifest<{ readonly value: number }>>(
      completed.completed.resultManifest as PayloadRef<ActivityMapResultManifest<{ readonly value: number }>>
    );
    const values = manifest.pages.flatMap((pageRef) =>
      decodePayload<ActivityMapResultPage<{ readonly value: number }>>(
        pageRef as PayloadRef<ActivityMapResultPage<{ readonly value: number }>>
      ).results.map((resultRef) =>
        decodePayload<{ readonly value: number }>(
          resultRef as PayloadRef<{ readonly value: number }>
        ).value
      )
    );
    expect(values).toEqual([10, 20, 30]);
    reopenedAgain.close();
  });

  it("recovers child-workflow-map descriptor state after close and reopen", async () => {
    const path = tempSqlitePath("reopen-child-map");
    const parentType = workflowType("sqlite.child-map-parent", 1);
    const childType = workflowType("sqlite.child-map-child", 1);
    const inputManifest = activityMapManifest(
      [{ value: 1 }, { value: 2 }, { value: 3 }],
      2
    );

    const first = new SqliteBackend({ path });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-child-map-reopen"),
      workflowType: parentType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await first.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parentType],
      leaseDurationMs: 30_000
    });
    expect(claim).not.toBeNull();
    if (!claim) {
      throw new Error("expected child-map parent claim");
    }
    const scheduled = {
      commandId: commandId(claim.runId, 1),
      workflowType: childType,
      taskQueue: "child-workflows",
      inputManifest,
      resultManifestName: "child-mapped",
      workflowIdPrefix: "wf/sqlite-child-map",
      maxInFlight: 2,
      parentClosePolicy: "Cancel" as const,
      failureMode: "CollectAll" as const,
      fingerprint: childWorkflowMapFingerprint(
        childType,
        payloadDigest(inputManifest),
        "child-mapped",
        "wf/sqlite-child-map",
        2,
        "child-workflows",
        "Cancel",
        "CollectAll"
      )
    };
    await first.commitWorkflowTask(claim.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ChildWorkflowMapScheduled", scheduled } }],
      scheduleChildWorkflowMaps: [
        {
          mapCommandId: scheduled.commandId,
          workflowType: scheduled.workflowType,
          taskQueue: scheduled.taskQueue,
          inputManifest: scheduled.inputManifest,
          resultManifestName: scheduled.resultManifestName,
          workflowIdPrefix: scheduled.workflowIdPrefix,
          maxInFlight: scheduled.maxInFlight,
          parentClosePolicy: scheduled.parentClosePolicy,
          failureMode: scheduled.failureMode
        }
      ]
    });
    first.close();

    const reopened = new SqliteBackend({ path });
    const firstChild = await reopened.claimWorkflowTask("child-worker-1", {
      namespace: namespace(),
      taskQueue: taskQueue("child-workflows"),
      registeredWorkflowTypes: [childType],
      leaseDurationMs: 30_000
    });
    const secondChild = await reopened.claimWorkflowTask("child-worker-2", {
      namespace: namespace(),
      taskQueue: taskQueue("child-workflows"),
      registeredWorkflowTypes: [childType],
      leaseDurationMs: 30_000
    });
    const blockedThird = await reopened.claimWorkflowTask("child-worker-3", {
      namespace: namespace(),
      taskQueue: taskQueue("child-workflows"),
      registeredWorkflowTypes: [childType],
      leaseDurationMs: 30_000
    });
    expect(firstChild?.workflowId).toBe("wf/sqlite-child-map/0");
    expect(secondChild?.workflowId).toBe("wf/sqlite-child-map/1");
    expect(blockedThird).toBeNull();
    if (!firstChild || !secondChild) {
      throw new Error("expected first two reopened child-map items");
    }

    await reopened.commitWorkflowTask(firstChild.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [
        {
          data: {
            kind: "WorkflowCompleted",
            result: encodePayload({ value: 10 }, { codec: "Json" })
          }
        }
      ]
    });
    reopened.close();

    const reopenedAgain = new SqliteBackend({ path });
    const thirdChild = await reopenedAgain.claimWorkflowTask("child-worker-3", {
      namespace: namespace(),
      taskQueue: taskQueue("child-workflows"),
      registeredWorkflowTypes: [childType],
      leaseDurationMs: 30_000
    });
    expect(thirdChild?.workflowId).toBe("wf/sqlite-child-map/2");
    if (!thirdChild) {
      throw new Error("expected third child-map item after reopen");
    }

    await reopenedAgain.commitWorkflowTask(secondChild.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [
        {
          data: {
            kind: "WorkflowCompleted",
            result: encodePayload({ value: 20 }, { codec: "Json" })
          }
        }
      ]
    });
    await reopenedAgain.commitWorkflowTask(thirdChild.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [
        {
          data: {
            kind: "WorkflowCompleted",
            result: encodePayload({ value: 30 }, { codec: "Json" })
          }
        }
      ]
    });

    const parentReady = await reopenedAgain.claimWorkflowTask("child-map-parent", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parentType],
      leaseDurationMs: 30_000
    });
    expect(parentReady?.reason).toBe("ChildWorkflowMapCompleted");
    if (!parentReady) {
      throw new Error("expected child-map parent wake");
    }

    const history = await reopenedAgain.streamHistory({
      runId: claim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ChildWorkflowMapScheduled",
      "ChildWorkflowMapCompleted"
    ]);
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "ChildWorkflowMapCompleted") {
      throw new Error("expected ChildWorkflowMapCompleted");
    }
    const manifest = decodePayload<ChildWorkflowMapResultManifest<{ readonly value: number }>>(
      completed.completed.resultManifest as PayloadRef<ChildWorkflowMapResultManifest<{ readonly value: number }>>
    );
    const values = manifest.pages.flatMap((pageRef) =>
      decodePayload<ChildWorkflowMapResultPage<{ readonly value: number }>>(
        pageRef as PayloadRef<ChildWorkflowMapResultPage<{ readonly value: number }>>
      ).outcomes.map((outcome) => {
        expect(outcome.kind).toBe("Succeeded");
        if (outcome.kind !== "Succeeded") {
          throw new Error("expected succeeded child-map outcome");
        }
        return decodePayload<{ readonly value: number }>(
          outcome.result as PayloadRef<{ readonly value: number }>
        ).value;
      })
    );
    expect(values).toEqual([10, 20, 30]);
    reopenedAgain.close();
  });
});
