import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterAll, describe, expect, it } from "vitest";
import {
  Client,
  RetryPolicy,
  Registry,
  Worker,
  activity,
  activityFingerprint,
  activityTaskFromScheduled,
  activityMapFingerprint,
  activityMapManifest,
  childWorkflowMapFingerprint,
  callActivity,
  childWorkflow,
  commandId,
  decodePayload,
  encodePayload,
  eventId,
  namespace,
  payloadDigest,
  signal,
  signalId,
  sleep,
  sleepUntil,
  taskQueue,
  timestampMs,
  workflow,
  workflowId,
  workflowType,
  type ActivityMapResultManifest,
  type ActivityMapResultPage,
  type ChildWorkflowMapResultManifest,
  type ChildWorkflowMapResultPage,
  type PayloadRef
} from "@durust/core";
import { LocalDirectoryBlobStore, PayloadBackend, collectPayloadRefs } from "@durust/payload";
import { basicProviderConformanceCases, prepareWorkflowTaskCommit } from "@durust/testing";
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

function withRawSqlite(path: string, fn: (db: DatabaseSync) => void): void {
  const db = new DatabaseSync(path);
  try {
    fn(db);
  } finally {
    db.close();
  }
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

  it("claims and streams from normalized history when workflow-row history is invalid", async () => {
    const path = tempSqlitePath("normalized-history-authority");
    const echo = workflow({
      name: "sqlite.normalized.echo",
      version: 1,
      handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> =>
        input
    });

    const first = new SqliteBackend({ path });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-normalized-corrupt"),
      workflowType: echo.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "from-normalized" }, { codec: "Json" })
    });
    first.close();

    withRawSqlite(path, (db) => {
      db.prepare("update workflows set history = ? where workflow_id = ?")
        .run("{not-json", "wf/sqlite-normalized-corrupt");
    });

    const reopened = new SqliteBackend({ path });
    const claimed = await reopened.claimWorkflowTask("worker-normalized-history", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [echo.workflowType],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected normalized history claim");
    }
    expect(claimed.prefetchedHistory.map((event) => event.eventType)).toEqual([
      "WorkflowStarted"
    ]);

    const commit = await prepareWorkflowTaskCommit(
      echo,
      { value: "from-normalized" },
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

  it("uses workflow type projection columns before workflow claim", async () => {
    const path = tempSqlitePath("workflow-type-projection");
    const projected = workflow({
      name: "sqlite.workflow-projection",
      version: 3,
      handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> =>
        input
    });

    const first = new SqliteBackend({ path });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-workflow-projection"),
      workflowType: projected.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "projected" }, { codec: "Json" })
    });
    first.close();

    withRawSqlite(path, (db) => {
      const before = db.prepare(`
        select workflow_type_name, workflow_type_version
        from workflows where workflow_id = ?
      `).get("wf/sqlite-workflow-projection") as
        | {
            readonly workflow_type_name: string | null;
            readonly workflow_type_version: number | null;
          }
        | undefined;
      expect(before).toMatchObject({
        workflow_type_name: projected.workflowType.name,
        workflow_type_version: projected.workflowType.version
      });
    });

    const reopened = new SqliteBackend({ path });
    await expect(
      reopened.claimWorkflowTask("wrong-type-worker", {
        namespace: namespace(),
        taskQueue: taskQueue("workflows"),
        registeredWorkflowTypes: [workflowType("sqlite.workflow-projection.other", 3)],
        leaseDurationMs: 30_000
      })
    ).resolves.toBeNull();

    const claimed = await reopened.claimWorkflowTask("correct-type-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [projected.workflowType],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    expect(claimed?.workflowType).toEqual(projected.workflowType);
    reopened.close();
  });

  it("plans payload GC from normalized history when workflow-row history is invalid", async () => {
    const path = tempSqlitePath("normalized-history-payload-roots");
    const blobStore = new LocalDirectoryBlobStore({
      root: tempRoot("sqlite-normalized-history-payload-roots"),
      prefix: "objects"
    });
    const firstInner = new SqliteBackend({ path });
    const first = new PayloadBackend({
      backend: firstInner,
      blobStore,
      inlineThresholdBytes: 0
    });

    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-normalized-roots"),
      workflowType: workflowType("sqlite.normalized.roots", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "root-payload" }, { codec: "Json" })
    });
    firstInner.close();

    withRawSqlite(path, (db) => {
      db.prepare("update workflows set history = ? where workflow_id = ?")
        .run("{not-json", "wf/sqlite-normalized-roots");
    });

    const reopenedInner = new SqliteBackend({ path });
    const reopened = new PayloadBackend({
      backend: reopenedInner,
      blobStore,
      inlineThresholdBytes: 0
    });
    await expect(reopened.planGarbageCollection()).resolves.toMatchObject({
      retainedCount: 1,
      unreachableCount: 0
    });
    reopenedInner.close();
  });

  it("reads query projections from normalized rows", async () => {
    const path = tempSqlitePath("query-projection-normalized");
    const workflowTypeValue = workflowType("sqlite.query-projection-normalized", 1);
    const projection = encodePayload({ status: "projected" }, { codec: "Json" });

    const first = new SqliteBackend({ path });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-query-projection-normalized"),
      workflowType: workflowTypeValue,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await first.claimWorkflowTask("query-projection-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowTypeValue],
      leaseDurationMs: 30_000
    });
    expect(claim).not.toBeNull();
    if (!claim) {
      throw new Error("expected query projection workflow claim");
    }
    await first.commitWorkflowTask(claim.claim, {
      expectedTailEventId: eventId(1),
      queryProjection: projection
    });
    first.close();

    withRawSqlite(path, (db) => {
      expect(
        db.prepare("select 1 from query_projections where run_id = ?")
          .get(String(claim.runId))
      ).toBeDefined();
      db.prepare("update workflows set query_projection = ? where run_id = ?")
        .run("{not-json", String(claim.runId));
    });

    const reopened = new SqliteBackend({ path });
    const found = await reopened.queryWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-query-projection-normalized")
    });
    expect(found).toMatchObject({
      kind: "Found",
      projection
    });
    const rootDigests = new Set(collectPayloadRefs(await reopened.payloadRoots()).map(payloadDigest));
    expect(rootDigests.has(payloadDigest(projection))).toBe(true);
    reopened.close();
  });

  it("reads activity payload roots from input projections after task JSON corruption", async () => {
    const path = tempSqlitePath("activity-input-root-projection");
    const first = new SqliteBackend({ path });
    const activityType = "sqlite.activity-input-root.activity";
    const workflowTypeValue = workflowType("sqlite.activity-input-root.workflow", 1);
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-activity-input-root"),
      workflowType: workflowTypeValue,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const workflowClaim = await first.claimWorkflowTask("activity-root-workflow", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowTypeValue],
      leaseDurationMs: 30_000
    });
    expect(workflowClaim).not.toBeNull();
    if (!workflowClaim) {
      throw new Error("expected workflow claim");
    }
    const input = encodePayload({ value: "activity-root" }, { codec: "Json" });
    const scheduled = {
      commandId: commandId(workflowClaim.runId, 1),
      activityName: activityType,
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      input,
      fingerprint: activityFingerprint(
        activityType,
        payloadDigest(input),
        "sha256:test-options"
      )
    };
    await first.commitWorkflowTask(workflowClaim.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
      scheduleActivities: [activityTaskFromScheduled(scheduled)]
    });
    first.close();

    withRawSqlite(path, (db) => {
      db.prepare("update activities set task = ? where activity_id = ?")
        .run("{not-json", `${workflowClaim.runId}:1`);
    });

    const reopened = new SqliteBackend({ path });
    const rootDigests = new Set(collectPayloadRefs(await reopened.payloadRoots()).map(payloadDigest));
    expect(rootDigests.has(payloadDigest(input))).toBe(true);
    reopened.close();
  });

  it("uses wait projection columns before timer maintenance", async () => {
    const path = tempSqlitePath("wait-projection");
    const timerWorkflow = workflow({
      name: "sqlite.wait-projection.workflow",
      version: 1,
      handler: async (_input: { readonly value: string }): Promise<{ readonly done: true }> => {
        await sleepUntil(10);
        return { done: true };
      }
    });
    const registry = new Registry().registerWorkflow(timerWorkflow);

    const first = new SqliteBackend({ path });
    const client = new Client(first, { namespace: namespace(), payloadCodec: "Json" });
    const handle = await client.startWorkflow(
      timerWorkflow,
      workflowId("wf/sqlite-wait-projection"),
      "workflows",
      { value: "timer" }
    );
    const worker = new Worker({
      backend: first,
      registry,
      namespace: namespace(),
      workerId: "sqlite-wait-projection-worker",
      workflowTaskQueue: "workflows",
      payloadCodec: "Json"
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    first.close();

    withRawSqlite(path, (db) => {
      const before = db.prepare(`
        select namespace, command_run_id, command_seq from waits
      `).get() as
        | {
            readonly namespace: string | null;
            readonly command_run_id: string | null;
            readonly command_seq: number | null;
          }
        | undefined;
      expect(before).toMatchObject({
        namespace: "default",
        command_run_id: String(handle.runId),
        command_seq: 1
      });
    });

    const reopened = new SqliteBackend({ path });
    await expect(
      reopened.fireDueTimers({
        namespace: namespace(),
        now: timestampMs(10),
        limit: 10
      })
    ).resolves.toEqual({ fired: 1 });
    const history = await reopened.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "TimerStarted",
      "TimerFired"
    ]);
    reopened.close();

    withRawSqlite(path, (db) => {
      const remainingWait = db.prepare("select 1 from waits limit 1").get();
      expect(remainingWait).toBeUndefined();
    });
  });

  it("uses signal namespace projection before inbox consumption", async () => {
    const path = tempSqlitePath("signal-projection");
    const approval = signal<{ readonly value: number }>("sqlite.signal-projection.approved");
    const signalWorkflow = workflow({
      name: "sqlite.signal-projection.workflow",
      version: 1,
      handler: async (_input: {}): Promise<{ readonly value: number }> => {
        const payload = await approval;
        return { value: payload.value };
      }
    });
    const registry = new Registry().registerWorkflow(signalWorkflow);

    const first = new SqliteBackend({ path });
    const client = new Client(first, { namespace: namespace(), payloadCodec: "Json" });
    const handle = await client.startWorkflow(
      signalWorkflow,
      workflowId("wf/sqlite-signal-projection"),
      "workflows",
      {}
    );
    const worker = new Worker({
      backend: first,
      registry,
      namespace: namespace(),
      workerId: "sqlite-signal-projection-worker",
      workflowTaskQueue: "workflows",
      registeredSignalNames: [approval.name],
      payloadCodec: "Json"
    });
    await expect(worker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await first.signalWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-signal-projection"),
      signalId: signalId("sig-sqlite-projection"),
      signalName: approval.name,
      payload: encodePayload({ value: 7 }, { codec: "Json" })
    });
    first.close();

    withRawSqlite(path, (db) => {
      const before = db.prepare(`
        select namespace, consumed from signals where signal_id = ?
      `).get("sig-sqlite-projection") as
        | { readonly namespace: string | null; readonly consumed: number }
        | undefined;
      expect(before).toMatchObject({
        namespace: "default",
        consumed: 0
      });
    });

    const reopened = new SqliteBackend({ path });
    const reopenedWorker = new Worker({
      backend: reopened,
      registry,
      namespace: namespace(),
      workerId: "sqlite-signal-projection-reopened-worker",
      workflowTaskQueue: "workflows",
      registeredSignalNames: [approval.name],
      payloadCodec: "Json"
    });
    await expect(reopenedWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    const history = await reopened.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(
      decodePayload<{ readonly value: number }>(
        completed.result as PayloadRef<{ readonly value: number }>
      )
    ).toEqual({ value: 7 });
    reopened.close();

    withRawSqlite(path, (db) => {
      const after = db.prepare(`
        select namespace, consumed from signals where signal_id = ?
      `).get("sig-sqlite-projection") as
        | { readonly namespace: string | null; readonly consumed: number }
        | undefined;
      expect(after).toMatchObject({
        namespace: "default",
        consumed: 1
      });
    });
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

  it("recovers mixed activity signal timer progress across repeated reopen boundaries", async () => {
    const path = tempSqlitePath("mixed-reopen-recovery");
    const approvalSignal = signal<{ readonly value: number }>("sqlite.mixed.approved");
    const stepActivity = activity({
      name: "sqlite.mixed.step",
      handler: async (input: { readonly value: number }): Promise<{ readonly value: number }> => ({
        value: input.value + 1
      })
    });
    const mixed = workflow({
      name: "sqlite.mixed.recovery",
      version: 1,
      handler: async (input: { readonly value: number }): Promise<{ readonly value: number }> => {
        const first = await callActivity(
          stepActivity,
          { value: input.value },
          { taskQueue: "activities" }
        );
        const approval = await approvalSignal;
        await sleep(10);
        return await callActivity(
          stepActivity,
          { value: first.value + approval.value },
          { taskQueue: "activities" }
        );
      }
    });
    const registry = new Registry().registerWorkflow(mixed).registerActivity(stepActivity);
    const worker = (backend: SqliteBackend, workerId: string): Worker =>
      new Worker({
        backend,
        registry,
        namespace: namespace(),
        workerId,
        workflowTaskQueue: "workflows",
        activityTaskQueue: "activities",
        registeredSignalNames: [approvalSignal.name],
        payloadCodec: "Json"
      });

    const first = new SqliteBackend({ path });
    const firstClient = new Client(first, { namespace: namespace(), payloadCodec: "Json" });
    const handle = await firstClient.startWorkflow(
      mixed,
      workflowId("wf/sqlite-mixed-reopen"),
      "workflows",
      { value: 4 }
    );
    first.close();

    const scheduleFirstActivity = new SqliteBackend({ path });
    await expect(worker(scheduleFirstActivity, "sqlite-mixed-worker-1").runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    scheduleFirstActivity.close();

    const completeFirstActivity = new SqliteBackend({ path });
    await expect(worker(completeFirstActivity, "sqlite-mixed-worker-2").runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    completeFirstActivity.close();

    const sendApproval = new SqliteBackend({ path });
    await new Client(sendApproval, { namespace: namespace(), payloadCodec: "Json" }).sendSignal({
      workflowId: workflowId("wf/sqlite-mixed-reopen"),
      signal: approvalSignal,
      payload: { value: 20 },
      idempotencyKey: "approval-1"
    });
    sendApproval.close();

    const scheduleTimer = new SqliteBackend({ path });
    await expect(worker(scheduleTimer, "sqlite-mixed-worker-3").runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    scheduleTimer.close();

    const fireTimer = new SqliteBackend({ path });
    await expect(
      fireTimer.fireDueTimers({
        namespace: namespace(),
        now: timestampMs(10),
        limit: 10
      })
    ).resolves.toEqual({ fired: 1 });
    fireTimer.close();

    const scheduleSecondActivity = new SqliteBackend({ path });
    await expect(worker(scheduleSecondActivity, "sqlite-mixed-worker-4").runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    scheduleSecondActivity.close();

    const completeSecondActivity = new SqliteBackend({ path });
    await expect(worker(completeSecondActivity, "sqlite-mixed-worker-5").runActivityTaskOnce()).resolves.toMatchObject({
      kind: "Completed",
      outcome: { kind: "Completed" }
    });
    completeSecondActivity.close();

    const completeWorkflow = new SqliteBackend({ path });
    await expect(worker(completeWorkflow, "sqlite-mixed-worker-6").runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    const history = await completeWorkflow.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(20),
      maxEvents: 20,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled",
      "ActivityCompleted",
      "SignalConsumed",
      "TimerStarted",
      "TimerFired",
      "ActivityScheduled",
      "ActivityCompleted",
      "WorkflowCompleted"
    ]);
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(
      decodePayload<{ readonly value: number }>(
        completed.result as PayloadRef<{ readonly value: number }>
      )
    ).toEqual({ value: 26 });
    completeWorkflow.close();
  });

  it("recovers child workflow start and parent notification across reopen boundaries", async () => {
    const path = tempSqlitePath("child-reopen-recovery");
    const child = workflow({
      name: "sqlite.child-recovery.child",
      version: 1,
      handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> => ({
        value: `${input.value}/child`
      })
    });
    const parent = workflow({
      name: "sqlite.child-recovery.parent",
      version: 1,
      handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> => {
        const handle = await childWorkflow(
          child,
          { value: input.value },
          { workflowId: `child/${input.value}`, taskQueue: "workflows" }
        ).spawn();
        return await handle.result();
      }
    });
    const registry = new Registry().registerWorkflow(parent).registerWorkflow(child);
    const worker = (backend: SqliteBackend, workerId: string): Worker =>
      new Worker({
        backend,
        registry,
        namespace: namespace(),
        workerId,
        workflowTaskQueue: "workflows",
        payloadCodec: "Json"
      });

    const first = new SqliteBackend({ path });
    const client = new Client(first, { namespace: namespace(), payloadCodec: "Json" });
    const handle = await client.startWorkflow(
      parent,
      workflowId("wf/sqlite-child-reopen"),
      "workflows",
      { value: "order-1" }
    );
    first.close();

    for (const workerId of [
      "sqlite-child-worker-1",
      "sqlite-child-worker-2",
      "sqlite-child-worker-3",
      "sqlite-child-worker-4"
    ]) {
      const reopened = new SqliteBackend({ path });
      await expect(worker(reopened, workerId).runWorkflowTaskOnce()).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      reopened.close();
    }

    const verify = new SqliteBackend({ path });
    const history = await verify.streamHistory({
      runId: handle.runId,
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
    const completed = history.events.at(-1)?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(
      decodePayload<{ readonly value: string }>(
        completed.result as PayloadRef<{ readonly value: string }>
      )
    ).toEqual({ value: "order-1/child" });
    verify.close();
  });

  it("reclaims expired workflow and activity leases after close and reopen", async () => {
    const path = tempSqlitePath("expired-lease-reopen");
    const first = new SqliteBackend({ path });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-expired-lease"),
      workflowType: workflowType("sqlite.expired-lease", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: 1 }, { codec: "Json" })
    });
    const expiredWorkflowClaim = await first.claimWorkflowTask("expired-workflow-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("sqlite.expired-lease", 1)],
      leaseDurationMs: 0
    });
    expect(expiredWorkflowClaim).not.toBeNull();
    first.close();

    const reopened = new SqliteBackend({ path });
    const reclaimedWorkflowClaim = await reopened.claimWorkflowTask("replacement-workflow-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("sqlite.expired-lease", 1)],
      leaseDurationMs: 30_000
    });
    expect(reclaimedWorkflowClaim).not.toBeNull();
    if (!expiredWorkflowClaim || !reclaimedWorkflowClaim) {
      throw new Error("expected workflow claims");
    }
    expect(reclaimedWorkflowClaim.reason).toBe("WorkflowStarted");
    expect(reclaimedWorkflowClaim.claim.token).not.toBe(expiredWorkflowClaim.claim.token);

    const input = encodePayload({ value: 1 }, { codec: "Json" });
    const scheduled = {
      commandId: commandId(reclaimedWorkflowClaim.runId, 1),
      activityName: "sqlite.expired-lease.activity",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      input,
      fingerprint: activityFingerprint(
        "sqlite.expired-lease.activity",
        payloadDigest(input),
        "sha256:test-options"
      )
    };
    await reopened.commitWorkflowTask(reclaimedWorkflowClaim.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
      scheduleActivities: [activityTaskFromScheduled(scheduled)]
    });
    const expiredActivityClaim = await reopened.claimActivityTask("expired-activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["sqlite.expired-lease.activity"],
      leaseDurationMs: 0
    });
    expect(expiredActivityClaim).not.toBeNull();
    reopened.close();

    const reopenedAgain = new SqliteBackend({ path });
    const reclaimedActivityClaim = await reopenedAgain.claimActivityTask("replacement-activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["sqlite.expired-lease.activity"],
      leaseDurationMs: 30_000
    });
    expect(reclaimedActivityClaim).not.toBeNull();
    if (!expiredActivityClaim || !reclaimedActivityClaim) {
      throw new Error("expected activity claims");
    }
    expect(reclaimedActivityClaim.claim.token).not.toBe(expiredActivityClaim.claim.token);
    await expect(
      reopenedAgain.completeActivity({
        claim: expiredActivityClaim.claim,
        result: encodePayload({ value: 2 }, { codec: "Json" })
      })
    ).rejects.toThrow("stale activity task lease");
    await expect(
      reopenedAgain.completeActivity({
        claim: reclaimedActivityClaim.claim,
        result: encodePayload({ value: 2 }, { codec: "Json" })
      })
    ).resolves.toEqual({ kind: "Completed", eventId: eventId(3) });
    reopenedAgain.close();
  });

  it("persists refreshed activity heartbeat deadlines across close and reopen", async () => {
    const path = tempSqlitePath("heartbeat-deadline-reopen");
    let now = 1_000;
    const first = new SqliteBackend({ path, nowMs: () => now });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-heartbeat-deadline-reopen"),
      workflowType: workflowType("sqlite.heartbeat-deadline", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: 1 }, { codec: "Json" })
    });
    const workflowClaim = await first.claimWorkflowTask("heartbeat-workflow-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("sqlite.heartbeat-deadline", 1)],
      leaseDurationMs: 30_000
    });
    expect(workflowClaim).not.toBeNull();
    if (!workflowClaim) {
      throw new Error("expected heartbeat workflow claim");
    }
    const input = encodePayload({ value: 1 }, { codec: "Json" });
    const scheduled = {
      commandId: commandId(workflowClaim.runId, 1),
      activityName: "sqlite.heartbeat-deadline.activity",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: 100,
      input,
      fingerprint: activityFingerprint(
        "sqlite.heartbeat-deadline.activity",
        payloadDigest(input),
        "sha256:test-options"
      )
    };
    await first.commitWorkflowTask(workflowClaim.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
      scheduleActivities: [activityTaskFromScheduled(scheduled)]
    });
    const activityClaim = await first.claimActivityTask("heartbeat-activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["sqlite.heartbeat-deadline.activity"],
      leaseDurationMs: 30_000
    });
    expect(activityClaim).not.toBeNull();
    if (!activityClaim) {
      throw new Error("expected heartbeat activity claim");
    }
    now = 1_050;
    await expect(first.heartbeatActivity({ claim: activityClaim.claim })).resolves.toEqual({
      kind: "Recorded"
    });
    first.close();

    now = 1_149;
    const reopened = new SqliteBackend({ path, nowMs: () => now });
    await expect(
      reopened.timeoutDueActivities({ namespace: namespace(), now, limit: 8 })
    ).resolves.toEqual({ timedOut: 0 });
    now = 1_150;
    await expect(
      reopened.timeoutDueActivities({ namespace: namespace(), now, limit: 8 })
    ).resolves.toEqual({ timedOut: 1 });
    const workflowWake = await reopened.claimWorkflowTask("heartbeat-ready-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("sqlite.heartbeat-deadline", 1)],
      leaseDurationMs: 30_000
    });
    expect(workflowWake?.reason).toBe("ActivityTimedOut");
    const history = await reopened.streamHistory({
      runId: workflowClaim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled",
      "ActivityTimedOut"
    ]);
    const timedOut = history.events.at(-1)?.data;
    if (timedOut?.kind !== "ActivityTimedOut") {
      throw new Error("expected ActivityTimedOut");
    }
    expect(timedOut.timedOut.message).toContain("missed heartbeat");
    reopened.close();
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

  it("reads activity-map payload roots from item projections", async () => {
    const path = tempSqlitePath("activity-map-item-root-projection");
    const parentType = workflowType("sqlite.activity-map-item-roots", 1);
    const inputManifest = activityMapManifest([{ value: 1 }, { value: 2 }], 2);

    const first = new SqliteBackend({ path });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-activity-map-item-roots"),
      workflowType: parentType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await first.claimWorkflowTask("map-root-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parentType],
      leaseDurationMs: 30_000
    });
    expect(claim).not.toBeNull();
    if (!claim) {
      throw new Error("expected map-root workflow claim");
    }
    const scheduled = {
      commandId: commandId(claim.runId, 1),
      activityName: "sqlite.map-root",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      inputManifest,
      resultManifestName: "mapped",
      maxInFlight: 1,
      fingerprint: activityMapFingerprint(
        "sqlite.map-root",
        payloadDigest(inputManifest),
        "mapped",
        1,
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
    const firstItem = await first.claimActivityTask("map-root-activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["sqlite.map-root"],
      leaseDurationMs: 30_000
    });
    expect(firstItem?.task.mapItem?.itemOrdinal).toBe(0);
    if (!firstItem) {
      throw new Error("expected first map item");
    }
    const result = encodePayload({ value: 10 }, { codec: "Json" });
    await first.completeActivity({ claim: firstItem.claim, result });
    first.close();

    withRawSqlite(path, (db) => {
      db.prepare("update activity_maps set inputs = ?, results = ?")
        .run("{not-json", "{not-json");
    });

    const reopened = new SqliteBackend({ path });
    const rootDigests = new Set(collectPayloadRefs(await reopened.payloadRoots()).map(payloadDigest));
    expect(rootDigests.has(payloadDigest(result))).toBe(true);
    reopened.close();
  });

  it("reads child-workflow-map payload roots from item projections", async () => {
    const path = tempSqlitePath("child-map-item-root-projection");
    const parentType = workflowType("sqlite.child-map-item-roots-parent", 1);
    const childType = workflowType("sqlite.child-map-item-roots-child", 1);
    const inputManifest = activityMapManifest([{ value: 1 }, { value: 2 }], 2);

    const first = new SqliteBackend({ path });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/sqlite-child-map-item-roots"),
      workflowType: parentType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await first.claimWorkflowTask("child-map-root-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parentType],
      leaseDurationMs: 30_000
    });
    expect(claim).not.toBeNull();
    if (!claim) {
      throw new Error("expected child-map-root workflow claim");
    }
    const scheduled = {
      commandId: commandId(claim.runId, 1),
      workflowType: childType,
      taskQueue: "child-workflows",
      inputManifest,
      resultManifestName: "child-mapped",
      workflowIdPrefix: "wf/sqlite-child-map-item-roots",
      maxInFlight: 1,
      parentClosePolicy: "Cancel" as const,
      failureMode: "CollectAll" as const,
      fingerprint: childWorkflowMapFingerprint(
        childType,
        payloadDigest(inputManifest),
        "child-mapped",
        "wf/sqlite-child-map-item-roots",
        1,
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
    const firstChild = await first.claimWorkflowTask("child-map-root-child-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("child-workflows"),
      registeredWorkflowTypes: [childType],
      leaseDurationMs: 30_000
    });
    expect(firstChild?.workflowId).toBe("wf/sqlite-child-map-item-roots/0");
    if (!firstChild) {
      throw new Error("expected first child-map item");
    }
    const result = encodePayload({ value: 10 }, { codec: "Json" });
    await first.commitWorkflowTask(firstChild.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "WorkflowCompleted", result } }]
    });
    first.close();

    withRawSqlite(path, (db) => {
      db.prepare("update child_workflow_maps set inputs = ?, outcomes = ?")
        .run("{not-json", "{not-json");
    });

    const reopened = new SqliteBackend({ path });
    const rootDigests = new Set(collectPayloadRefs(await reopened.payloadRoots()).map(payloadDigest));
    expect(rootDigests.has(payloadDigest(result))).toBe(true);
    reopened.close();
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
