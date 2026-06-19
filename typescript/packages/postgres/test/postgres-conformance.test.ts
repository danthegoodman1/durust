import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Pool } from "pg";
import { afterAll, describe, expect, it } from "vitest";
import {
  Client,
  type ActivityTask,
  RetryPolicy,
  Registry,
  Worker,
  activity,
  activityFingerprint,
  activityMapFingerprint,
  activityMapManifest,
  activityTaskFromScheduled,
  callActivity,
  childWorkflow,
  childWorkflowFingerprint,
  childWorkflowMapFingerprint,
  commandId,
  decodePayload,
  encodePayload,
  eventId,
  namespace,
  payloadDigest,
  signal,
  signalId,
  sleep,
  taskQueue,
  timerFingerprint,
  timestampMs,
  waitId,
  workflow,
  workflowId,
  workflowType,
  type ActivityMapResultManifest,
  type ActivityMapResultPage,
  type ChildWorkflowMapResultManifest,
  type ChildWorkflowMapResultPage,
  type PayloadRef
} from "@durust/core";
import {
  LocalDirectoryBlobStore,
  PayloadBackend,
  collectPayloadRefs,
  encodePayloadWithStorage
} from "@durust/payload";
import { PostgresBackend } from "@durust/postgres";
import { basicProviderConformanceCases, prepareWorkflowTaskCommit } from "@durust/testing";

const postgresUrl = process.env.DURUST_POSTGRES_URL;
const describePostgres = postgresUrl === undefined ? describe.skip : describe;
const managedBackends: PostgresBackend[] = [];
const roots: string[] = [];
let tableCounter = 0;

afterAll(async () => {
  for (const backend of managedBackends.splice(0)) {
    await backend.destroy().catch(() => undefined);
  }
  for (const root of roots.splice(0)) {
    rmSync(root, { recursive: true, force: true });
  }
});

describePostgres("PostgresBackend provider conformance", () => {
  for (const conformanceCase of basicProviderConformanceCases()) {
    it(conformanceCase.name, async () => {
      await conformanceCase.run(() => trackedPostgresBackend("conformance"));
    });
  }
});

describePostgres("PostgresBackend blob-backed provider conformance", () => {
  for (const conformanceCase of basicProviderConformanceCases()) {
    it(conformanceCase.name, async () => {
      await conformanceCase.run(() => {
        const inner = trackedPostgresBackend("payload_conformance");
        const blobStore = new LocalDirectoryBlobStore({
          root: tempRoot("postgres-payload"),
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

describePostgres("PostgresBackend payload roots and GC", () => {
  it("collects wrapper-owned garbage using roots exposed by Postgres", async () => {
    const tableName = nextTableName("payload_gc");
    const inner = trackedPostgresBackendWithTable(tableName);
    const blobStore = new LocalDirectoryBlobStore({
      root: tempRoot("postgres-payload-gc"),
      prefix: "objects"
    });
    const backend = new PayloadBackend({
      backend: inner,
      blobStore,
      inlineThresholdBytes: 8
    });
    const input = encodePayload({ body: "reachable".repeat(32) }, { codec: "Json" });
    const orphan = await encodePayloadWithStorage(
      { body: "orphan".repeat(32) },
      {
        codec: "Json",
        inlineThresholdBytes: 8,
        blobStore
      }
    );
    if (orphan.kind !== "Blob") {
      throw new Error("expected orphan blob");
    }

    const started = await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-payload-gc"),
      workflowType: workflowType("postgres.payload-gc", 1),
      taskQueue: taskQueue("workflows"),
      input
    });

    await expect(backend.planGarbageCollection()).resolves.toMatchObject({
      unreachableUris: [orphan.uri],
      retainedCount: 1,
      unreachableCount: 1
    });
    await expect(backend.collectGarbage({ dryRun: false })).resolves.toMatchObject({
      deletedUris: [orphan.uri],
      deletedCount: 1
    });
    const reopenedInner = new PostgresBackend({
      url: requirePostgresUrl(),
      tableName,
      poolSize: 1
    });
    try {
      const reopened = new PayloadBackend({
        backend: reopenedInner,
        blobStore,
        inlineThresholdBytes: 8
      });
      await expect(reopened.planGarbageCollection()).resolves.toMatchObject({
        unreachableUris: [],
        retainedCount: 1,
        unreachableCount: 0
      });
    } finally {
      await reopenedInner.close();
    }

    const history = await inner.streamHistory({
      runId: started.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const startedEvent = history.events[0]?.data;
    if (startedEvent?.kind !== "WorkflowStarted" || startedEvent.input.kind !== "Blob") {
      throw new Error("expected blob-backed stored workflow input");
    }
    expect(await blobStore.list()).toEqual([startedEvent.input.uri]);
  });
});

describePostgres("PostgresBackend normalized schema", () => {
  it("creates normalized projection indexes on fresh open and reopen", async () => {
    const tableName = nextTableName("normalized_schema_indexes");
    const first = new PostgresBackend({
      url: requirePostgresUrl(),
      tableName,
      poolSize: 1
    });
    await first.statsSnapshot();
    await first.close();

    const expectedIndexNames = normalizedProjectionIndexNames(tableName);
    const firstDefinitions = await readIndexDefinitions(expectedIndexNames);
    expect([...firstDefinitions.keys()].sort()).toEqual([...expectedIndexNames].sort());
    assertNormalizedProjectionIndexDefinitions(tableName, firstDefinitions);

    const reopened = trackedPostgresBackendWithTable(tableName);
    await reopened.statsSnapshot();
    const reopenedDefinitions = await readIndexDefinitions(expectedIndexNames);
    expect([...reopenedDefinitions.keys()].sort()).toEqual([...expectedIndexNames].sort());
    assertNormalizedProjectionIndexDefinitions(tableName, reopenedDefinitions);
  });
});

describePostgres("PostgresBackend persistence", () => {
  it("recovers a started workflow after close and reopen", async () => {
    const tableName = nextTableName("reopen_started");
    const echo = workflow({
      name: "postgres.echo",
      version: 1,
      handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> =>
        input
    });

    const first = new PostgresBackend({
      url: requirePostgresUrl(),
      tableName,
      poolSize: 1
    });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-reopen"),
      workflowType: echo.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    await first.close();

    const reopened = trackedPostgresBackendWithTable(tableName);
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
  });

  it("recovers mixed activity signal timer progress across repeated reopen boundaries", async () => {
    const tableName = nextTableName("mixed_reopen_recovery");
    let now = 0;
    const opened: PostgresBackend[] = [];
    const open = (): PostgresBackend => {
      const backend = new PostgresBackend({
        url: requirePostgresUrl(),
        tableName,
        poolSize: 1,
        nowMs: () => now
      });
      opened.push(backend);
      return backend;
    };
    const cleanup = async (): Promise<void> => {
      const backend = new PostgresBackend({
        url: requirePostgresUrl(),
        tableName,
        poolSize: 1
      });
      await backend.destroy().catch(() => undefined);
    };

    const approvalSignal = signal<{ readonly value: number }>("postgres.mixed.approved");
    const stepActivity = activity({
      name: "postgres.mixed.step",
      handler: async (input: { readonly value: number }): Promise<{ readonly value: number }> => ({
        value: input.value + 1
      })
    });
    const mixed = workflow({
      name: "postgres.mixed.recovery",
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
    const worker = (backend: PostgresBackend, workerId: string): Worker =>
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

    try {
      const first = open();
      const firstClient = new Client(first, { namespace: namespace(), payloadCodec: "Json" });
      const handle = await firstClient.startWorkflow(
        mixed,
        workflowId("wf/postgres-mixed-reopen"),
        "workflows",
        { value: 4 }
      );
      await first.close();

      const scheduleFirstActivity = open();
      await expect(
        worker(scheduleFirstActivity, "postgres-mixed-worker-1").runWorkflowTaskOnce()
      ).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await scheduleFirstActivity.close();

      const completeFirstActivity = open();
      await expect(
        worker(completeFirstActivity, "postgres-mixed-worker-2").runActivityTaskOnce()
      ).resolves.toMatchObject({
        kind: "Completed",
        outcome: { kind: "Completed" }
      });
      await completeFirstActivity.close();

      const sendApproval = open();
      await new Client(sendApproval, { namespace: namespace(), payloadCodec: "Json" }).sendSignal({
        workflowId: workflowId("wf/postgres-mixed-reopen"),
        signal: approvalSignal,
        payload: { value: 20 },
        idempotencyKey: "approval-1"
      });
      await sendApproval.close();

      const scheduleTimer = open();
      await expect(
        worker(scheduleTimer, "postgres-mixed-worker-3").runWorkflowTaskOnce()
      ).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await scheduleTimer.close();

      now = 10;
      const fireTimer = open();
      await expect(
        fireTimer.fireDueTimers({
          namespace: namespace(),
          now: timestampMs(now),
          limit: 10
        })
      ).resolves.toEqual({ fired: 1 });
      await fireTimer.close();

      const scheduleSecondActivity = open();
      await expect(
        worker(scheduleSecondActivity, "postgres-mixed-worker-4").runWorkflowTaskOnce()
      ).resolves.toMatchObject({
        kind: "Committed",
        outcome: { kind: "Committed" }
      });
      await scheduleSecondActivity.close();

      const completeSecondActivity = open();
      await expect(
        worker(completeSecondActivity, "postgres-mixed-worker-5").runActivityTaskOnce()
      ).resolves.toMatchObject({
        kind: "Completed",
        outcome: { kind: "Completed" }
      });
      await completeSecondActivity.close();

      const completeWorkflow = open();
      await expect(
        worker(completeWorkflow, "postgres-mixed-worker-6").runWorkflowTaskOnce()
      ).resolves.toMatchObject({
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
      await completeWorkflow.close();
    } finally {
      for (const backend of opened.splice(0).reverse()) {
        await backend.close().catch(() => undefined);
      }
      await cleanup();
    }
  });

  it("recovers child workflow start and parent notification across reopen boundaries", async () => {
    const tableName = nextTableName("child_reopen_recovery");
    const opened: PostgresBackend[] = [];
    const open = (): PostgresBackend => {
      const backend = new PostgresBackend({
        url: requirePostgresUrl(),
        tableName,
        poolSize: 1
      });
      opened.push(backend);
      return backend;
    };
    const cleanup = async (): Promise<void> => {
      const backend = new PostgresBackend({
        url: requirePostgresUrl(),
        tableName,
        poolSize: 1
      });
      await backend.destroy().catch(() => undefined);
    };

    const child = workflow({
      name: "postgres.child-recovery.child",
      version: 1,
      handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> => ({
        value: `${input.value}/child`
      })
    });
    const parent = workflow({
      name: "postgres.child-recovery.parent",
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
    const worker = (backend: PostgresBackend, workerId: string): Worker =>
      new Worker({
        backend,
        registry,
        namespace: namespace(),
        workerId,
        workflowTaskQueue: "workflows",
        payloadCodec: "Json"
      });

    try {
      const first = open();
      const client = new Client(first, { namespace: namespace(), payloadCodec: "Json" });
      const handle = await client.startWorkflow(
        parent,
        workflowId("wf/postgres-child-reopen"),
        "workflows",
        { value: "order-1" }
      );
      await first.close();

      for (const workerId of [
        "postgres-child-worker-1",
        "postgres-child-worker-2",
        "postgres-child-worker-3",
        "postgres-child-worker-4"
      ]) {
        const reopened = open();
        await expect(worker(reopened, workerId).runWorkflowTaskOnce()).resolves.toMatchObject({
          kind: "Committed",
          outcome: { kind: "Committed" }
        });
        await reopened.close();
      }

      const verify = open();
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
      await verify.close();
    } finally {
      for (const backend of opened.splice(0).reverse()) {
        await backend.close().catch(() => undefined);
      }
      await cleanup();
    }
  });

  it("recovers activity-map descriptor state after close and reopen", async () => {
    const tableName = nextTableName("reopen_activity_map");
    const opened: PostgresBackend[] = [];
    const open = (): PostgresBackend => {
      const backend = new PostgresBackend({
        url: requirePostgresUrl(),
        tableName,
        poolSize: 1
      });
      opened.push(backend);
      return backend;
    };
    const cleanup = async (): Promise<void> => {
      const backend = new PostgresBackend({
        url: requirePostgresUrl(),
        tableName,
        poolSize: 1
      });
      await backend.destroy().catch(() => undefined);
    };

    const parentType = workflowType("postgres.activity-map-parent", 1);
    const inputManifest = activityMapManifest(
      [{ value: 1 }, { value: 2 }, { value: 3 }],
      2
    );

    try {
      const first = open();
      await first.startWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/postgres-activity-map-reopen"),
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
        activityName: "postgres.map",
        taskQueue: "activities",
        retryPolicy: RetryPolicy.none(),
        startToCloseTimeoutMs: null,
        heartbeatTimeoutMs: null,
        inputManifest,
        resultManifestName: "mapped",
        maxInFlight: 2,
        fingerprint: activityMapFingerprint(
          "postgres.map",
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
      await first.close();

      const reopened = open();
      const firstItem = await reopened.claimActivityTask("map-worker-1", {
        namespace: namespace(),
        taskQueue: taskQueue("activities"),
        registeredActivityNames: ["postgres.map"],
        leaseDurationMs: 30_000
      });
      const secondItem = await reopened.claimActivityTask("map-worker-2", {
        namespace: namespace(),
        taskQueue: taskQueue("activities"),
        registeredActivityNames: ["postgres.map"],
        leaseDurationMs: 30_000
      });
      const blockedThird = await reopened.claimActivityTask("map-worker-3", {
        namespace: namespace(),
        taskQueue: taskQueue("activities"),
        registeredActivityNames: ["postgres.map"],
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
      await reopened.close();

      const reopenedAgain = open();
      const thirdItem = await reopenedAgain.claimActivityTask("map-worker-3", {
        namespace: namespace(),
        taskQueue: taskQueue("activities"),
        registeredActivityNames: ["postgres.map"],
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
      await reopenedAgain.close();
    } finally {
      for (const backend of opened.splice(0).reverse()) {
        await backend.close().catch(() => undefined);
      }
      await cleanup();
    }
  });

  it("recovers child-workflow-map descriptor state after close and reopen", async () => {
    const tableName = nextTableName("reopen_child_map");
    const opened: PostgresBackend[] = [];
    const open = (): PostgresBackend => {
      const backend = new PostgresBackend({
        url: requirePostgresUrl(),
        tableName,
        poolSize: 1
      });
      opened.push(backend);
      return backend;
    };
    const cleanup = async (): Promise<void> => {
      const backend = new PostgresBackend({
        url: requirePostgresUrl(),
        tableName,
        poolSize: 1
      });
      await backend.destroy().catch(() => undefined);
    };

    const parentType = workflowType("postgres.child-map-parent", 1);
    const childType = workflowType("postgres.child-map-child", 1);
    const inputManifest = activityMapManifest(
      [{ value: 1 }, { value: 2 }, { value: 3 }],
      2
    );

    try {
      const first = open();
      await first.startWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/postgres-child-map-reopen"),
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
        workflowIdPrefix: "wf/postgres-child-map",
        maxInFlight: 2,
        parentClosePolicy: "Cancel" as const,
        failureMode: "CollectAll" as const,
        fingerprint: childWorkflowMapFingerprint(
          childType,
          payloadDigest(inputManifest),
          "child-mapped",
          "wf/postgres-child-map",
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
      await first.close();

      const reopened = open();
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
      expect(firstChild?.workflowId).toBe("wf/postgres-child-map/0");
      expect(secondChild?.workflowId).toBe("wf/postgres-child-map/1");
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
      await reopened.close();

      const reopenedAgain = open();
      const thirdChild = await reopenedAgain.claimWorkflowTask("child-worker-3", {
        namespace: namespace(),
        taskQueue: taskQueue("child-workflows"),
        registeredWorkflowTypes: [childType],
        leaseDurationMs: 30_000
      });
      expect(thirdChild?.workflowId).toBe("wf/postgres-child-map/2");
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
      await reopenedAgain.close();
    } finally {
      for (const backend of opened.splice(0).reverse()) {
        await backend.close().catch(() => undefined);
      }
      await cleanup();
    }
  });

  it("reclaims expired workflow and activity leases after close and reopen", async () => {
    const tableName = nextTableName("expired_lease_reopen");
    const first = new PostgresBackend({
      url: requirePostgresUrl(),
      tableName,
      poolSize: 1
    });
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-expired-lease"),
      workflowType: workflowType("postgres.expired-lease", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: 1 }, { codec: "Json" })
    });
    const expiredWorkflowClaim = await first.claimWorkflowTask("expired-workflow-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("postgres.expired-lease", 1)],
      leaseDurationMs: 0
    });
    expect(expiredWorkflowClaim).not.toBeNull();
    await first.close();

    const reopened = new PostgresBackend({
      url: requirePostgresUrl(),
      tableName,
      poolSize: 1
    });
    const reclaimedWorkflowClaim = await reopened.claimWorkflowTask("replacement-workflow-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("postgres.expired-lease", 1)],
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
      activityName: "postgres.expired-lease.activity",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      input,
      fingerprint: activityFingerprint(
        "postgres.expired-lease.activity",
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
      registeredActivityNames: ["postgres.expired-lease.activity"],
      leaseDurationMs: 0
    });
    expect(expiredActivityClaim).not.toBeNull();
    await reopened.close();

    const reopenedAgain = trackedPostgresBackendWithTable(tableName);
    const reclaimedActivityClaim = await reopenedAgain.claimActivityTask(
      "replacement-activity-worker",
      {
        namespace: namespace(),
        taskQueue: taskQueue("activities"),
        registeredActivityNames: ["postgres.expired-lease.activity"],
        leaseDurationMs: 30_000
      }
    );
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
  });

  it("persists refreshed activity heartbeat deadlines across close and reopen", async () => {
    const tableName = nextTableName("heartbeat_deadline_reopen");
    let now = 1_000;
    const first = new PostgresBackend({
      url: requirePostgresUrl(),
      tableName,
      poolSize: 1,
      nowMs: () => now
    });
    managedBackends.push(first);
    await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-heartbeat-deadline-reopen"),
      workflowType: workflowType("postgres.heartbeat-deadline", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: 1 }, { codec: "Json" })
    });
    const workflowClaim = await first.claimWorkflowTask("heartbeat-workflow-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("postgres.heartbeat-deadline", 1)],
      leaseDurationMs: 30_000
    });
    expect(workflowClaim).not.toBeNull();
    if (!workflowClaim) {
      throw new Error("expected heartbeat workflow claim");
    }
    const input = encodePayload({ value: 1 }, { codec: "Json" });
    const scheduled = {
      commandId: commandId(workflowClaim.runId, 1),
      activityName: "postgres.heartbeat-deadline.activity",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: 100,
      input,
      fingerprint: activityFingerprint(
        "postgres.heartbeat-deadline.activity",
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
      registeredActivityNames: ["postgres.heartbeat-deadline.activity"],
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
    await first.close();

    now = 1_149;
    const reopened = new PostgresBackend({
      url: requirePostgresUrl(),
      tableName,
      poolSize: 1,
      nowMs: () => now
    });
    managedBackends.push(reopened);
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
      registeredWorkflowTypes: [workflowType("postgres.heartbeat-deadline", 1)],
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
  });
});

describePostgres("PostgresBackend normalized history", () => {
  it("uses normalized workflow projections for query reads", async () => {
    const tableName = nextTableName("normalized_query_projection");
    const backend = trackedPostgresBackendWithTable(tableName);
    const queryProjection = encodePayload({ status: "ready" }, { codec: "Json" });
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-query"),
      workflowType: workflowType("postgres.normalized-query", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });

    await expect(
      backend.queryWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/postgres-normalized-query")
      })
    ).resolves.toEqual({ kind: "NoProjection" });

    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("postgres.normalized-query", 1)],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected claim");
    }

    await backend.commitWorkflowTask(claimed.claim, {
      expectedTailEventId: eventId(1),
      queryProjection
    });
    const queryProjectionRows = await readNormalizedQueryProjectionRows(tableName);
    expect(queryProjectionRows).toHaveLength(1);
    expect(queryProjectionRows[0]).toMatchObject({
      run_id: claimed.runId,
      namespace: namespace(),
      workflow_id: "wf/postgres-normalized-query"
    });
    expect(decodePayload(revivePostgresJson<PayloadRef>(queryProjectionRows[0]?.projection))).toEqual({
      status: "ready"
    });
    await clearNormalizedWorkflowRunQueryProjection(tableName, claimed.runId);

    const found = await backend.queryWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-query")
    });
    expect(found.kind).toBe("Found");
    if (found.kind !== "Found") {
      throw new Error("expected query projection");
    }
    expect(decodePayload(found.projection)).toEqual({ status: "ready" });
    await expect(
      backend.queryWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/postgres-normalized-query-missing")
      })
    ).resolves.toEqual({ kind: "NotFound" });
  });

  it("reads query projection payload roots from normalized rows", async () => {
    const tableName = nextTableName("normalized_query_payload_roots");
    const backend = trackedPostgresBackendWithTable(tableName);
    const queryProjection = encodePayload({ status: "rooted" }, { codec: "Json" });
    const type = workflowType("postgres.normalized-query-payload-roots", 1);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-query-payload-roots"),
      workflowType: type,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected claim");
    }
    await backend.commitWorkflowTask(claimed.claim, {
      expectedTailEventId: eventId(1),
      queryProjection
    });

    await clearNormalizedWorkflowRunQueryProjection(tableName, claimed.runId);

    const rootDigests = new Set(collectPayloadRefs(await backend.payloadRoots()).map(payloadDigest));
    expect(rootDigests.has(payloadDigest(queryProjection))).toBe(true);
  });

  it("queries the current run projection after continue-as-new", async () => {
    const tableName = nextTableName("normalized_query_continue");
    const backend = trackedPostgresBackendWithTable(tableName);
    const type = workflowType("postgres.normalized-query-continue", 1);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-query-continue"),
      workflowType: type,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "first" }, { codec: "Json" })
    });
    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(firstClaim).not.toBeNull();
    if (!firstClaim) {
      throw new Error("expected first claim");
    }

    await backend.commitWorkflowTask(firstClaim.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [
        {
          data: {
            kind: "WorkflowContinuedAsNew",
            input: encodePayload({ value: "second" }, { codec: "Json" })
          }
        }
      ],
      queryProjection: encodePayload({ status: "old-run" }, { codec: "Json" })
    });

    await expect(
      backend.queryWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/postgres-normalized-query-continue")
      })
    ).resolves.toEqual({ kind: "NoProjection" });

    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(secondClaim).not.toBeNull();
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const currentProjection = encodePayload({ status: "new-run" }, { codec: "Json" });
    await backend.commitWorkflowTask(secondClaim.claim, {
      expectedTailEventId: eventId(1),
      queryProjection: currentProjection
    });

    const found = await backend.queryWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-query-continue")
    });
    expect(found.kind).toBe("Found");
    if (found.kind !== "Found") {
      throw new Error("expected current query projection");
    }
    expect(decodePayload(found.projection)).toEqual({ status: "new-run" });
  });

  it("uses normalized workflow-id projection during idempotent start", async () => {
    const tableName = nextTableName("normalized_start_projection");
    const backend = trackedPostgresBackendWithTable(tableName);
    const first = await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-start-a"),
      workflowType: workflowType("postgres.normalized-start", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "a" }, { codec: "Json" })
    });

    await expect(
      backend.startWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/postgres-normalized-start-a"),
        workflowType: workflowType("postgres.normalized-start", 1),
        taskQueue: taskQueue("workflows"),
        input: encodePayload({ value: "a-again" }, { codec: "Json" })
      })
    ).resolves.toEqual({ kind: "AlreadyStarted", runId: first.runId });
    await expect(
      readNormalizedWorkflowIdRunId(
        tableName,
        String(namespace()),
        "wf/postgres-normalized-start-a"
      )
    ).resolves.toBe(String(first.runId));
  });

  it("stores workflow run queue, lease, tail, and terminal state in normalized rows", async () => {
    const tableName = nextTableName("normalized_workflow_runs");
    const backend = trackedPostgresBackendWithTable(tableName);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-workflow-runs"),
      workflowType: workflowType("postgres.normalized-workflow-runs", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });

    await expect(readNormalizedWorkflowRunRows(tableName)).resolves.toMatchObject([
      {
        workflow_id: "wf/postgres-normalized-workflow-runs",
        workflow_type_name: "postgres.normalized-workflow-runs",
        workflow_type_version: 1,
        task_queue: "workflows",
        tail_event_id: 1,
        ready_reason: "WorkflowStarted",
        claim_worker_id: null,
        claim_token: null,
        claim_reason: null,
        terminal: false
      }
    ]);

    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("postgres.normalized-workflow-runs", 1)],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected claim");
    }

    const claimedRows = await readNormalizedWorkflowRunRows(tableName);
    expect(claimedRows).toHaveLength(1);
    expect(claimedRows[0]).toMatchObject({
      tail_event_id: 1,
      ready_reason: null,
      claim_worker_id: "worker-a",
      claim_reason: "WorkflowStarted",
      terminal: false
    });
    expect(Number(claimedRows[0]?.claim_token)).toBe(claimed.claim.token);
    expect(Number(claimedRows[0]?.claim_expires_at_ms)).toBeGreaterThan(0);

    await expect(
      backend.commitWorkflowTask(claimed.claim, {
        expectedTailEventId: eventId(1),
        appendEvents: [
          {
            data: {
              kind: "WorkflowCompleted",
              result: encodePayload({ ok: true }, { codec: "Json" })
            }
          }
        ]
      })
    ).resolves.toEqual({ kind: "Committed", newTailEventId: eventId(2) });

    await expect(readNormalizedWorkflowRunRows(tableName)).resolves.toMatchObject([
      {
        tail_event_id: 2,
        ready_reason: null,
        claim_worker_id: null,
        claim_token: null,
        claim_reason: null,
        terminal: true
      }
    ]);
  });

  it("stores continued-as-new runs without requiring workflow-id uniqueness", async () => {
    const tableName = nextTableName("normalized_workflow_continue");
    const backend = trackedPostgresBackendWithTable(tableName);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-workflow-continue"),
      workflowType: workflowType("postgres.normalized-workflow-continue", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "first" }, { codec: "Json" })
    });
    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("postgres.normalized-workflow-continue", 1)],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected claim");
    }

    await expect(
      backend.commitWorkflowTask(claimed.claim, {
        expectedTailEventId: eventId(1),
        appendEvents: [
          {
            data: {
              kind: "WorkflowContinuedAsNew",
              input: encodePayload({ value: "second" }, { codec: "Json" })
            }
          }
        ]
      })
    ).resolves.toEqual({ kind: "Committed", newTailEventId: eventId(2) });

    const rows = await readNormalizedWorkflowRunRows(tableName);
    expect(rows.map((row) => [row.run_id, row.workflow_id, row.tail_event_id, row.terminal])).toEqual([
      ["run-1", "wf/postgres-normalized-workflow-continue", 2, true],
      ["run-2", "wf/postgres-normalized-workflow-continue", 1, false]
    ]);
    expect(rows[1]).toMatchObject({
      ready_reason: "WorkflowStarted",
      claim_worker_id: null
    });
  });

  it("uses the normalized workflow-run projection for claim selection", async () => {
    const tableName = nextTableName("normalized_workflow_claim_selection");
    const backend = trackedPostgresBackendWithTable(tableName);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-workflow-claim-selection"),
      workflowType: workflowType("postgres.normalized-workflow-claim-selection", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    await expect(
      backend.claimWorkflowTask("worker-a", {
        namespace: namespace(),
        taskQueue: taskQueue("workflows"),
        registeredWorkflowTypes: [workflowType("postgres.normalized-workflow-claim-selection", 1)],
        leaseDurationMs: 30_000
      })
    ).resolves.toMatchObject({
      workflowId: "wf/postgres-normalized-workflow-claim-selection",
      reason: "WorkflowStarted"
    });
  });

  it("updates only the claimed normalized workflow-run row on workflow claim", async () => {
    const tableName = nextTableName("normalized_workflow_claim_targeted_update");
    const backend = trackedPostgresBackendWithTable(tableName);
    const type = workflowType("postgres.normalized-workflow-claim-targeted-update", 1);
    const first = await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-targeted-claim-a"),
      workflowType: type,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "a" }, { codec: "Json" })
    });
    const second = await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-targeted-claim-b"),
      workflowType: type,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "b" }, { codec: "Json" })
    });
    await rewriteNormalizedWorkflowRunTaskQueue(tableName, String(second.runId), "corrupted");

    const claimed = await backend.claimWorkflowTask("targeted-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(claimed).toMatchObject({
      runId: first.runId,
      workflowId: "wf/postgres-normalized-targeted-claim-a"
    });

    const rows = await readNormalizedWorkflowRunRows(tableName);
    expect(rows.find((row) => row.run_id === String(first.runId))).toMatchObject({
      claim_worker_id: "targeted-worker",
      task_queue: "workflows"
    });
    expect(rows.find((row) => row.run_id === String(second.runId))).toMatchObject({
      claim_worker_id: null,
      task_queue: "corrupted"
    });
  });

  it("uses the normalized waits projection for due timer selection", async () => {
    const tableName = nextTableName("normalized_waits_timer_selection");
    const backend = trackedPostgresBackendWithTable(tableName);
    const type = workflowType("postgres.normalized-waits.timer-selection", 1);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-waits-timer-selection"),
      workflowType: type,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected claim");
    }

    const timerCommand = commandId(claimed.runId, 1);
    const timerWaitId = waitId(`${claimed.runId}:timer:1`);
    await backend.commitWorkflowTask(claimed.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [
        {
          data: {
            kind: "TimerStarted",
            started: {
              commandId: timerCommand,
              fireAt: timestampMs(1_000),
              fingerprint: timerFingerprint("sleep_until", timestampMs(1_000))
            }
          }
        }
      ],
      upsertWaits: [
        {
          waitId: timerWaitId,
          runId: claimed.runId,
          commandId: timerCommand,
          kind: "Timer",
          key: "timer",
          readyAt: timestampMs(1_000)
        }
      ]
    });
    await expect(
      backend.fireDueTimers({
        namespace: namespace(),
        now: timestampMs(1_000),
        limit: 16
      })
    ).resolves.toEqual({ fired: 1 });

    await expect(
      backend.claimWorkflowTask("worker-after-timer", {
        namespace: namespace(),
        taskQueue: taskQueue("workflows"),
        registeredWorkflowTypes: [type],
        leaseDurationMs: 30_000
      })
    ).resolves.toMatchObject({
      reason: "TimerFired",
      replayTargetEventId: eventId(3)
    });
  });

  it("deletes only fired normalized wait rows during timer maintenance", async () => {
    const tableName = nextTableName("normalized_waits_timer_targeted_update");
    const backend = trackedPostgresBackendWithTable(tableName);
    const type = workflowType("postgres.normalized-waits.timer-targeted-update", 1);
    const first = await startClaimAndScheduleTimer(
      backend,
      "wf/postgres-normalized-waits-targeted-a",
      type,
      timestampMs(1_000),
      1
    );
    const second = await startClaimAndScheduleTimer(
      backend,
      "wf/postgres-normalized-waits-targeted-b",
      type,
      timestampMs(2_000),
      2
    );
    await rewriteNormalizedWaitKind(tableName, second.waitId, "Signal");

    await expect(
      backend.fireDueTimers({
        namespace: namespace(),
        now: timestampMs(1_000),
        limit: 16
      })
    ).resolves.toEqual({ fired: 1 });

    const waits = await readNormalizedWaitRows(tableName);
    expect(waits.find((wait) => wait.wait_id === first.waitId)).toBeUndefined();
    expect(waits.find((wait) => wait.wait_id === second.waitId)).toMatchObject({
      kind: "Signal"
    });
  });

  it("targets normalized projection updates for simple workflow commits", async () => {
    const tableName = nextTableName("normalized_simple_commit_targeted_update");
    const backend = trackedPostgresBackendWithTable(tableName);
    const type = workflowType("postgres.normalized-simple-commit-targeted-update", 1);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-simple-commit-targeted"),
      workflowType: type,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "commit" }, { codec: "Json" })
    });
    const claimed = await backend.claimWorkflowTask("simple-commit-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected simple commit workflow claim");
    }
    const other = await startClaimAndScheduleTimer(
      backend,
      "wf/postgres-normalized-simple-commit-unrelated-wait",
      type,
      timestampMs(2_000),
      2
    );
    await rewriteNormalizedWaitKind(tableName, other.waitId, "Signal");

    const projection = encodePayload({ status: "committed" }, { codec: "Json" });
    await expect(
      backend.commitWorkflowTask(claimed.claim, {
        expectedTailEventId: eventId(1),
        appendEvents: [{ data: { kind: "WorkflowTaskStarted" } }],
        queryProjection: projection
      })
    ).resolves.toEqual({ kind: "Committed", newTailEventId: eventId(2) });

    expect((await readNormalizedWaitRows(tableName)).find((wait) => wait.wait_id === other.waitId))
      .toMatchObject({ kind: "Signal" });
    const queryProjectionRows = await readNormalizedQueryProjectionRows(tableName);
    expect(queryProjectionRows.find((row) => row.run_id === String(claimed.runId)))
      .toMatchObject({
        workflow_id: "wf/postgres-normalized-simple-commit-targeted"
      });
  });

  it("uses normalized signal projection for inbox reads", async () => {
    const tableName = nextTableName("normalized_signal_inbox");
    const backend = trackedPostgresBackendWithTable(tableName);
    const started = await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-signal-inbox"),
      workflowType: workflowType("postgres.normalized-signal-inbox", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    const payload = encodePayload({ approvalId: "a-1" }, { codec: "Json" });
    await expect(
      backend.signalWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/postgres-normalized-signal-inbox"),
        signalId: signalId("sig-normalized-inbox"),
        signalName: "approved",
        payload
      })
    ).resolves.toEqual({ kind: "Accepted" });

    const inbox = await backend.readSignalInbox({
      runId: started.runId,
      signalName: "approved"
    });

    expect(inbox).toMatchObject({
      signalId: "sig-normalized-inbox",
      signalName: "approved"
    });
    expect(decodePayload(inbox?.payload ?? encodePayload({}, { codec: "Json" }))).toEqual({
      approvalId: "a-1"
    });
  });

  it("upserts only the delivered normalized signal row", async () => {
    const tableName = nextTableName("normalized_signal_targeted_update");
    const backend = trackedPostgresBackendWithTable(tableName);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-signal-targeted-a"),
      workflowType: workflowType("postgres.normalized-signal-targeted", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "a" }, { codec: "Json" })
    });
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-signal-targeted-b"),
      workflowType: workflowType("postgres.normalized-signal-targeted", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "b" }, { codec: "Json" })
    });
    await backend.signalWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-signal-targeted-a"),
      signalId: signalId("sig-targeted-a"),
      signalName: "approved",
      payload: encodePayload({ value: "a" }, { codec: "Json" })
    });
    await rewriteNormalizedSignalConsumed(tableName, "sig-targeted-a", true);

    await expect(
      backend.signalWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/postgres-normalized-signal-targeted-b"),
        signalId: signalId("sig-targeted-b"),
        signalName: "approved",
        payload: encodePayload({ value: "b" }, { codec: "Json" })
      })
    ).resolves.toEqual({ kind: "Accepted" });

    const signals = await readNormalizedSignalRows(tableName);
    expect(signals.find((signal) => signal.signal_id === "sig-targeted-a")).toMatchObject({
      consumed: true
    });
    expect(signals.find((signal) => signal.signal_id === "sig-targeted-b")).toMatchObject({
      consumed: false
    });
  });

  it("uses the normalized current-run projection for signal target selection", async () => {
    const tableName = nextTableName("normalized_signal_target_selection");
    const backend = trackedPostgresBackendWithTable(tableName);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-signal-target"),
      workflowType: workflowType("postgres.normalized-signal-target", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });

    await expect(
      backend.startWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/postgres-normalized-signal-target"),
        workflowType: workflowType("postgres.normalized-signal-target", 1),
        taskQueue: taskQueue("workflows"),
        input: encodePayload({ value: "ok" }, { codec: "Json" })
      })
    ).resolves.toMatchObject({ kind: "AlreadyStarted" });

    await expect(
      backend.signalWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/postgres-normalized-signal-target"),
        signalId: signalId("sig-target-current-run"),
        signalName: "approved",
        payload: encodePayload({ approvalId: "a-2" }, { codec: "Json" })
      })
    ).resolves.toEqual({ kind: "Accepted" });
  });

  it("uses the normalized activity-task projection for claim selection", async () => {
    const tableName = nextTableName("normalized_activity_claim_selection");
    const backend = trackedPostgresBackendWithTable(tableName);
    const type = workflowType("postgres.normalized-activity-claim-selection", 1);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-activity-claim-selection"),
      workflowType: type,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected workflow claim");
    }

    const activityInput = encodePayload({ value: 1 }, { codec: "Json" });
    const scheduled = {
      commandId: commandId(claimed.runId, 1),
      activityName: "postgres.normalized-activity",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      input: activityInput,
      fingerprint: activityFingerprint(
        "postgres.normalized-activity",
        payloadDigest(activityInput),
        "sha256:test-options"
      )
    };
    const task = activityTaskFromScheduled(scheduled);
    await backend.commitWorkflowTask(claimed.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
      scheduleActivities: [task]
    });

    const projectionRows = await readNormalizedActivityTaskRows(tableName);
    expect(projectionRows).toHaveLength(1);
    const projection = projectionRows[0];
    const projectedTask = revivePostgresJson<ActivityTask>(projection?.task);
    expect(projectedTask).toMatchObject({
      activityId: task.activityId,
      runId: claimed.runId,
      activityName: "postgres.normalized-activity",
      taskQueue: "activities",
      attempt: 1,
      mapItem: null
    });
    expect(decodePayload(projectedTask.input)).toEqual({ value: 1 });
    expect(decodePayload(revivePostgresJson<PayloadRef>(projection?.input))).toEqual({
      value: 1
    });

    await expect(
      backend.claimActivityTask("activity-worker-a", {
        namespace: namespace(),
        taskQueue: taskQueue("activities"),
        registeredActivityNames: ["postgres.normalized-activity"],
        leaseDurationMs: 30_000
      })
    ).resolves.toMatchObject({
      task: {
        activityId: task.activityId,
        activityName: "postgres.normalized-activity"
      }
    });
  });

  it("uses the normalized activity-task projection for timeout selection", async () => {
    const tableName = nextTableName("normalized_activity_timeout_selection");
    let now = 1_000;
    const backend = new PostgresBackend({
      url: requirePostgresUrl(),
      tableName,
      poolSize: 1,
      nowMs: () => now
    });
    managedBackends.push(backend);
    const type = workflowType("postgres.normalized-activity-timeout-selection", 1);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-activity-timeout-selection"),
      workflowType: type,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected workflow claim");
    }

    const activityInput = encodePayload({ value: 1 }, { codec: "Json" });
    const scheduled = {
      commandId: commandId(claimed.runId, 1),
      activityName: "postgres.normalized-activity-timeout",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: 10,
      heartbeatTimeoutMs: null,
      input: activityInput,
      fingerprint: activityFingerprint(
        "postgres.normalized-activity-timeout",
        payloadDigest(activityInput),
        "sha256:test-options"
      )
    };
    const task = activityTaskFromScheduled(scheduled);
    await backend.commitWorkflowTask(claimed.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
      scheduleActivities: [task]
    });

    const activityClaim = await backend.claimActivityTask("activity-worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["postgres.normalized-activity-timeout"],
      leaseDurationMs: 30_000
    });
    expect(activityClaim).not.toBeNull();
    if (!activityClaim) {
      throw new Error("expected activity claim");
    }
    const claimedProjection = (await readNormalizedActivityTaskRows(tableName))[0];
    expect(claimedProjection?.activity_id).toBe(task.activityId);
    expect(Number(claimedProjection?.timeout_deadline_at_ms)).toBe(1_010);

    now = 1_011;
    await expect(
      backend.timeoutDueActivities({ namespace: namespace(), now, limit: 8 })
    ).resolves.toEqual({ timedOut: 1 });

    const workflowWake = await backend.claimWorkflowTask("ready-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(workflowWake?.reason).toBe("ActivityTimedOut");
    const history = await backend.streamHistory({
      runId: claimed.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events.map((event) => event.eventType)).toContain("ActivityTimedOut");
  });

  it("targets normalized projection updates for scalar activity completion", async () => {
    const tableName = nextTableName("normalized_activity_completion_targeted_update");
    const backend = trackedPostgresBackendWithTable(tableName);
    const type = workflowType("postgres.normalized-activity-completion-targeted", 1);
    const first = await startClaimAndScheduleActivity(
      backend,
      "wf/postgres-normalized-activity-completion-targeted-a",
      type,
      1
    );
    const second = await startClaimAndScheduleActivity(
      backend,
      "wf/postgres-normalized-activity-completion-targeted-b",
      type,
      2
    );
    const claimedActivity = await backend.claimActivityTask("activity-targeted-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["postgres.normalized-activity-completion-targeted"],
      leaseDurationMs: 30_000
    });
    expect(claimedActivity).not.toBeNull();
    if (!claimedActivity) {
      throw new Error("expected activity claim");
    }
    const untouchedActivityId =
      claimedActivity.task.activityId === first.activityId ? second.activityId : first.activityId;
    await rewriteNormalizedActivityTaskQueue(
      tableName,
      untouchedActivityId,
      "corrupted-unrelated-queue"
    );

    await expect(
      backend.completeActivity({
        claim: claimedActivity.claim,
        result: encodePayload({ value: "done" }, { codec: "Json" })
      })
    ).resolves.toEqual({ kind: "Completed", eventId: eventId(3) });

    const activityRows = await readNormalizedActivityTaskRows(tableName);
    expect(activityRows.find((row) => row.activity_id === claimedActivity.task.activityId))
      .toMatchObject({
        claim_token: null,
        terminal_event_id: 3
      });
    expect(activityRows.find((row) => row.activity_id === untouchedActivityId)).toMatchObject({
      task_queue: "corrupted-unrelated-queue"
    });
    expect(
      (await readNormalizedWorkflowRunRows(tableName)).find(
        (row) => row.run_id === String(claimedActivity.task.runId)
      )
    ).toMatchObject({
      ready_reason: "ActivityCompleted",
      tail_event_id: 3
    });
  });

  it("reads activity payload roots from the normalized input projection", async () => {
    const tableName = nextTableName("normalized_activity_input_roots");
    const backend = trackedPostgresBackendWithTable(tableName);
    const type = workflowType("postgres.normalized-activity-input-roots", 1);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-activity-input-roots"),
      workflowType: type,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected workflow claim");
    }

    const activityInput = encodePayload({ value: "activity-root" }, { codec: "Json" });
    const scheduled = {
      commandId: commandId(claimed.runId, 1),
      activityName: "postgres.normalized-activity-input-root",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      input: activityInput,
      fingerprint: activityFingerprint(
        "postgres.normalized-activity-input-root",
        payloadDigest(activityInput),
        "sha256:test-options"
      )
    };
    const task = activityTaskFromScheduled(scheduled);
    await backend.commitWorkflowTask(claimed.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
      scheduleActivities: [task]
    });

    await corruptNormalizedActivityTaskJson(tableName, task.activityId);
    await corruptNormalizedHistoryEventData(tableName, claimed.runId, 2);

    const rootDigests = new Set(collectPayloadRefs(await backend.payloadRoots()).map(payloadDigest));
    expect(rootDigests.has(payloadDigest(activityInput))).toBe(true);
  });

  it("stores activity-map and child-workflow-map state in normalized rows", async () => {
    const tableName = nextTableName("normalized_map_state_projection");
    const backend = trackedPostgresBackendWithTable(tableName);
    const type = workflowType("postgres.normalized-map-state", 1);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-map-state"),
      workflowType: type,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [type],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected workflow claim");
    }

    const activityMapInputs = activityMapManifest(
      [{ value: 1 }, { value: 2 }, { value: 3 }],
      2
    );
    const activityMapScheduled = {
      commandId: commandId(claimed.runId, 1),
      activityName: "postgres.normalized-map-activity",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      inputManifest: activityMapInputs,
      resultManifestName: "activity-map-results",
      maxInFlight: 2,
      fingerprint: activityMapFingerprint(
        "postgres.normalized-map-activity",
        payloadDigest(activityMapInputs),
        "activity-map-results",
        2,
        "sha256:test-options"
      )
    };
    const childMapInputs = activityMapManifest(
      [{ value: "a" }, { value: "b" }, { value: "c" }],
      2
    );
    const childMapType = workflowType("postgres.normalized-map-child", 1);
    const childMapScheduled = {
      commandId: commandId(claimed.runId, 2),
      workflowType: childMapType,
      taskQueue: "child-workflows",
      inputManifest: childMapInputs,
      resultManifestName: "child-map-results",
      workflowIdPrefix: "wf/postgres-normalized-map-child",
      maxInFlight: 2,
      parentClosePolicy: "Cancel" as const,
      failureMode: "CollectAll" as const,
      fingerprint: childWorkflowMapFingerprint(
        childMapType,
        payloadDigest(childMapInputs),
        "child-map-results",
        "wf/postgres-normalized-map-child",
        2,
        "child-workflows",
        "Cancel",
        "CollectAll"
      )
    };

    await backend.commitWorkflowTask(claimed.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [
        { data: { kind: "ActivityMapScheduled", scheduled: activityMapScheduled } },
        { data: { kind: "ChildWorkflowMapScheduled", scheduled: childMapScheduled } }
      ],
      scheduleActivityMaps: [
        {
          mapCommandId: activityMapScheduled.commandId,
          activityName: activityMapScheduled.activityName,
          taskQueue: activityMapScheduled.taskQueue,
          retryPolicy: activityMapScheduled.retryPolicy,
          startToCloseTimeoutMs: activityMapScheduled.startToCloseTimeoutMs,
          heartbeatTimeoutMs: activityMapScheduled.heartbeatTimeoutMs,
          inputManifest: activityMapScheduled.inputManifest,
          resultManifestName: activityMapScheduled.resultManifestName,
          maxInFlight: activityMapScheduled.maxInFlight
        }
      ],
      scheduleChildWorkflowMaps: [
        {
          mapCommandId: childMapScheduled.commandId,
          workflowType: childMapScheduled.workflowType,
          taskQueue: childMapScheduled.taskQueue,
          inputManifest: childMapScheduled.inputManifest,
          resultManifestName: childMapScheduled.resultManifestName,
          workflowIdPrefix: childMapScheduled.workflowIdPrefix,
          maxInFlight: childMapScheduled.maxInFlight,
          parentClosePolicy: childMapScheduled.parentClosePolicy,
          failureMode: childMapScheduled.failureMode
        }
      ]
    });

    const activityMaps = await readNormalizedActivityMapRows(tableName);
    expect(activityMaps).toHaveLength(1);
    const activityMap = activityMaps[0];
    expect(activityMap).toMatchObject({
      command_key: `${claimed.runId}:1`,
      run_id: claimed.runId,
      input_count: 3,
      next_ordinal: 2,
      terminal: false
    });
    expect(revivePostgresJson<number[]>(activityMap?.in_flight)).toEqual([0, 1]);
    expect(revivePostgresJson<unknown[]>(activityMap?.results)).toEqual([null, null, null]);
    expect(revivePostgresJson<{ readonly activityName: string; readonly maxInFlight: number }>(
      activityMap?.task
    )).toMatchObject({
      activityName: "postgres.normalized-map-activity",
      maxInFlight: 2
    });
    expect(payloadDigest(revivePostgresJson<PayloadRef>(activityMap?.input_manifest))).toBe(
      payloadDigest(activityMapInputs)
    );
    expect(
      revivePostgresJson(activityMap?.inputs).map((input) => decodePayload(input))
    ).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }]);
    const activityMapItems = await readNormalizedActivityMapItemRows(tableName);
    expect(activityMapItems.map((item) => ({
      command_key: item.command_key,
      item_ordinal: item.item_ordinal,
      input: decodePayload(revivePostgresJson<PayloadRef>(item.input)),
      result: item.result === null ? null : decodePayload(revivePostgresJson<PayloadRef>(item.result)),
      in_flight: item.in_flight,
      terminal: item.terminal
    }))).toEqual([
      {
        command_key: `${claimed.runId}:1`,
        item_ordinal: 0,
        input: { value: 1 },
        result: null,
        in_flight: true,
        terminal: false
      },
      {
        command_key: `${claimed.runId}:1`,
        item_ordinal: 1,
        input: { value: 2 },
        result: null,
        in_flight: true,
        terminal: false
      },
      {
        command_key: `${claimed.runId}:1`,
        item_ordinal: 2,
        input: { value: 3 },
        result: null,
        in_flight: false,
        terminal: false
      }
    ]);

    const childMaps = await readNormalizedChildWorkflowMapRows(tableName);
    expect(childMaps).toHaveLength(1);
    const childMap = childMaps[0];
    expect(childMap).toMatchObject({
      command_key: `${claimed.runId}:2`,
      run_id: claimed.runId,
      input_count: 3,
      next_ordinal: 2,
      terminal: false
    });
    expect(revivePostgresJson<number[]>(childMap?.in_flight)).toEqual([0, 1]);
    expect(revivePostgresJson<unknown[]>(childMap?.outcomes)).toEqual([null, null, null]);
    expect(revivePostgresJson<{
      readonly workflowIdPrefix: string;
      readonly maxInFlight: number;
      readonly failureMode: string;
    }>(childMap?.task)).toMatchObject({
      workflowIdPrefix: "wf/postgres-normalized-map-child",
      maxInFlight: 2,
      failureMode: "CollectAll"
    });
    expect(payloadDigest(revivePostgresJson<PayloadRef>(childMap?.input_manifest))).toBe(
      payloadDigest(childMapInputs)
    );
    expect(
      revivePostgresJson(childMap?.inputs).map((input) => decodePayload(input))
    ).toEqual([{ value: "a" }, { value: "b" }, { value: "c" }]);
    const childMapItems = await readNormalizedChildWorkflowMapItemRows(tableName);
    expect(childMapItems.map((item) => ({
      command_key: item.command_key,
      item_ordinal: item.item_ordinal,
      input: decodePayload(revivePostgresJson<PayloadRef>(item.input)),
      outcome:
        item.outcome === null
          ? null
          : revivePostgresJson<{ readonly kind: string }>(item.outcome).kind,
      in_flight: item.in_flight,
      terminal: item.terminal
    }))).toEqual([
      {
        command_key: `${claimed.runId}:2`,
        item_ordinal: 0,
        input: { value: "a" },
        outcome: null,
        in_flight: true,
        terminal: false
      },
      {
        command_key: `${claimed.runId}:2`,
        item_ordinal: 1,
        input: { value: "b" },
        outcome: null,
        in_flight: true,
        terminal: false
      },
      {
        command_key: `${claimed.runId}:2`,
        item_ordinal: 2,
        input: { value: "c" },
        outcome: null,
        in_flight: false,
        terminal: false
      }
    ]);

    const rootDigests = new Set(collectPayloadRefs(await backend.payloadRoots()).map(payloadDigest));
    expect(rootDigests.has(payloadDigest(activityMapInputs))).toBe(true);
    expect(rootDigests.has(payloadDigest(childMapInputs))).toBe(true);
    for (const item of activityMapItems) {
      expect(rootDigests.has(payloadDigest(revivePostgresJson<PayloadRef>(item.input)))).toBe(true);
    }
    for (const item of childMapItems) {
      expect(rootDigests.has(payloadDigest(revivePostgresJson<PayloadRef>(item.input)))).toBe(true);
    }

    for (const result of [{ doubled: 2 }, { doubled: 4 }]) {
      const activity = await backend.claimActivityTask("map-activity-worker", {
        namespace: namespace(),
        taskQueue: taskQueue("activities"),
        registeredActivityNames: ["postgres.normalized-map-activity"],
        leaseDurationMs: 30_000
      });
      expect(activity).not.toBeNull();
      if (!activity) {
        throw new Error("expected activity-map item claim");
      }
      await backend.completeActivity({
        claim: activity.claim,
        result: encodePayload(result, { codec: "Json" })
      });
    }
    const activityMapAfterTwo = (await readNormalizedActivityMapRows(tableName))[0];
    expect(activityMapAfterTwo).toMatchObject({
      next_ordinal: 3,
      terminal: false
    });
    expect(revivePostgresJson<number[]>(activityMapAfterTwo?.in_flight)).toEqual([2]);
    const activityResultsAfterTwo = revivePostgresJson<unknown[]>(
      activityMapAfterTwo?.results
    );
    expect(
      activityResultsAfterTwo.map((result) =>
        result === null ? null : decodePayload(result)
      )
    ).toEqual([{ doubled: 2 }, { doubled: 4 }, null]);
    const activityMapItemsAfterTwo = await readNormalizedActivityMapItemRows(tableName);
    expect(activityMapItemsAfterTwo.map((item) => ({
      item_ordinal: item.item_ordinal,
      result: item.result === null ? null : decodePayload(revivePostgresJson<PayloadRef>(item.result)),
      in_flight: item.in_flight,
      terminal: item.terminal
    }))).toEqual([
      { item_ordinal: 0, result: { doubled: 2 }, in_flight: false, terminal: true },
      { item_ordinal: 1, result: { doubled: 4 }, in_flight: false, terminal: true },
      { item_ordinal: 2, result: null, in_flight: true, terminal: false }
    ]);

    const finalActivity = await backend.claimActivityTask("map-activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["postgres.normalized-map-activity"],
      leaseDurationMs: 30_000
    });
    expect(finalActivity).not.toBeNull();
    if (!finalActivity) {
      throw new Error("expected final activity-map item claim");
    }
    await backend.completeActivity({
      claim: finalActivity.claim,
      result: encodePayload({ doubled: 6 }, { codec: "Json" })
    });
    const completedActivityMap = (await readNormalizedActivityMapRows(tableName))[0];
    expect(completedActivityMap).toMatchObject({
      next_ordinal: 3,
      terminal: true
    });
    expect(revivePostgresJson<number[]>(completedActivityMap?.in_flight)).toEqual([]);
    expect(
      revivePostgresJson<unknown[]>(completedActivityMap?.results).map((result) =>
        decodePayload(result)
      )
    ).toEqual([{ doubled: 2 }, { doubled: 4 }, { doubled: 6 }]);

    for (const result of [{ letter: "a" }, { letter: "b" }]) {
      const child = await backend.claimWorkflowTask("child-map-worker", {
        namespace: namespace(),
        taskQueue: taskQueue("child-workflows"),
        registeredWorkflowTypes: [childMapType],
        leaseDurationMs: 30_000
      });
      expect(child).not.toBeNull();
      if (!child) {
        throw new Error("expected child-map workflow claim");
      }
      await backend.commitWorkflowTask(child.claim, {
        expectedTailEventId: eventId(1),
        appendEvents: [
          {
            data: {
              kind: "WorkflowCompleted",
              result: encodePayload(result, { codec: "Json" })
            }
          }
        ]
      });
    }
    const childMapAfterTwo = (await readNormalizedChildWorkflowMapRows(tableName))[0];
    expect(childMapAfterTwo).toMatchObject({
      next_ordinal: 3,
      terminal: false
    });
    expect(revivePostgresJson<number[]>(childMapAfterTwo?.in_flight)).toEqual([2]);
    expect(
      revivePostgresJson<Array<{ readonly kind?: string; readonly result?: unknown } | null>>(
        childMapAfterTwo?.outcomes
      ).map((outcome) => (outcome === null ? null : outcome.kind))
    ).toEqual(["Succeeded", "Succeeded", null]);
    const childMapItemsAfterTwo = await readNormalizedChildWorkflowMapItemRows(tableName);
    expect(childMapItemsAfterTwo.map((item) => ({
      item_ordinal: item.item_ordinal,
      outcome:
        item.outcome === null
          ? null
          : revivePostgresJson<{ readonly kind: string }>(item.outcome).kind,
      in_flight: item.in_flight,
      terminal: item.terminal
    }))).toEqual([
      { item_ordinal: 0, outcome: "Succeeded", in_flight: false, terminal: true },
      { item_ordinal: 1, outcome: "Succeeded", in_flight: false, terminal: true },
      { item_ordinal: 2, outcome: null, in_flight: true, terminal: false }
    ]);

    const finalChild = await backend.claimWorkflowTask("child-map-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("child-workflows"),
      registeredWorkflowTypes: [childMapType],
      leaseDurationMs: 30_000
    });
    expect(finalChild).not.toBeNull();
    if (!finalChild) {
      throw new Error("expected final child-map workflow claim");
    }
    await backend.commitWorkflowTask(finalChild.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [
        {
          data: {
            kind: "WorkflowCompleted",
            result: encodePayload({ letter: "c" }, { codec: "Json" })
          }
        }
      ]
    });
    const completedChildMap = (await readNormalizedChildWorkflowMapRows(tableName))[0];
    expect(completedChildMap).toMatchObject({
      next_ordinal: 3,
      terminal: true
    });
    expect(revivePostgresJson<number[]>(completedChildMap?.in_flight)).toEqual([]);
    expect(
      revivePostgresJson<Array<{ readonly kind: string; readonly result: unknown }>>(
        completedChildMap?.outcomes
      ).map((outcome) => ({
        kind: outcome.kind,
        result: decodePayload(outcome.result)
      }))
    ).toEqual([
      { kind: "Succeeded", result: { letter: "a" } },
      { kind: "Succeeded", result: { letter: "b" } },
      { kind: "Succeeded", result: { letter: "c" } }
    ]);
  });

  it("uses normalized history for workflow claim prefetch", async () => {
    const tableName = nextTableName("normalized_claim_prefetch");
    const backend = trackedPostgresBackendWithTable(tableName);
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-claim-prefetch"),
      workflowType: workflowType("postgres.normalized-claim-prefetch", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });

    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("postgres.normalized-claim-prefetch", 1)],
      leaseDurationMs: 30_000
    });

    expect(claimed).not.toBeNull();
    expect(claimed?.prefetchedHistory.map((event) => event.eventType)).toEqual([
      "WorkflowStarted"
    ]);
    expect(claimed?.prefetchedHistory.map((event) => event.data.kind)).toEqual([
      "WorkflowStarted"
    ]);
  });

  it("streams initialized normalized history", async () => {
    const tableName = nextTableName("normalized_stream_no_state");
    const backend = trackedPostgresBackendWithTable(tableName);
    const started = await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-stream-no-state"),
      workflowType: workflowType("postgres.normalized-stream-no-state", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });

    const history = await backend.streamHistory({
      runId: started.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });

    expect(history.events.map((event) => event.eventType)).toEqual(["WorkflowStarted"]);
  });

  it("appends ordered history rows across reopen", async () => {
    const tableName = nextTableName("normalized_history_reopen");
    const echo = workflow({
      name: "postgres.normalized-history.echo",
      version: 1,
      handler: async (input: { readonly value: string }): Promise<{ readonly value: string }> =>
        input
    });

    const first = new PostgresBackend({
      url: requirePostgresUrl(),
      tableName,
      poolSize: 1
    });
    const started = await first.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-history-reopen"),
      workflowType: echo.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "ok" }, { codec: "Json" })
    });
    await first.close();

    const reopened = trackedPostgresBackendWithTable(tableName);
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

    const rows = await readNormalizedHistoryRows(tableName);
    expect(rows.map((row) => [row.run_id, row.event_id, row.event_type, row.data.kind])).toEqual([
      [String(started.runId), 1, "WorkflowStarted", "WorkflowStarted"],
      [String(started.runId), 2, "WorkflowCompleted", "WorkflowCompleted"]
    ]);
  });

  it("stores provider-generated child workflow history rows in the commit transaction", async () => {
    const tableName = nextTableName("normalized_history_child");
    const backend = trackedPostgresBackendWithTable(tableName);
    const parentType = workflowType("postgres.normalized-history.parent", 1);
    const childType = workflowType("postgres.normalized-history.child", 1);
    const childInput = encodePayload({ value: "child" }, { codec: "Json" });
    const childQueue = taskQueue("child-workflows");
    const childId = workflowId("wf/postgres-normalized-history-child");
    const started = await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-normalized-history-parent"),
      workflowType: parentType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: "parent" }, { codec: "Json" })
    });
    const claimed = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [parentType],
      leaseDurationMs: 30_000
    });
    expect(claimed).not.toBeNull();
    if (!claimed) {
      throw new Error("expected parent claim");
    }

    await expect(
      backend.commitWorkflowTask(claimed.claim, {
        expectedTailEventId: eventId(1),
        startChildWorkflows: [
          {
            commandId: commandId(started.runId, 1),
            workflowType: childType,
            workflowId: childId,
            taskQueue: childQueue,
            input: childInput,
            parentClosePolicy: "Cancel",
            fingerprint: childWorkflowFingerprint(
              childType,
              childId,
              payloadDigest(childInput),
              childQueue,
              "Cancel"
            )
          }
        ]
      })
    ).resolves.toEqual({ kind: "Committed", newTailEventId: eventId(2) });

    const rows = await readNormalizedHistoryRows(tableName);
    expect(rows.map((row) => [row.run_id, row.event_id, row.event_type, row.data.kind])).toEqual([
      [String(started.runId), 1, "WorkflowStarted", "WorkflowStarted"],
      [String(started.runId), 2, "ChildWorkflowStarted", "ChildWorkflowStarted"],
      ["run-2", 1, "WorkflowStarted", "WorkflowStarted"]
    ]);
  });
});

describePostgres("PostgresBackend stats", () => {
  it("captures statement stats when pg_stat_statements is available", async () => {
    const backend = trackedPostgresBackend("stats_snapshot");
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/postgres-stats"),
      workflowType: workflowType("postgres.stats", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: 1 }, { codec: "Json" })
    });

    const stats = await backend.statsSnapshot();
    expect(stats.activeConnections).toBeGreaterThanOrEqual(1);
    if (stats.statements.length === 0) {
      expect(stats.statements).toEqual([]);
      return;
    }

    expect(stats.statements.some((statement) => statement.calls > 0)).toBe(true);
    expect(stats.statements.some((statement) => statement.query.length > 0)).toBe(true);
  });
});

function trackedPostgresBackend(label: string): PostgresBackend {
  return trackedPostgresBackendWithTable(nextTableName(label));
}

function trackedPostgresBackendWithTable(tableName: string): PostgresBackend {
  const backend = new PostgresBackend({
    url: requirePostgresUrl(),
    tableName,
    poolSize: 1
  });
  managedBackends.push(backend);
  return backend;
}

function nextTableName(label: string): string {
  const safeLabel = label.replace(/[^a-z0-9_]/giu, "_").toLowerCase();
  return `durust_ts_${safeLabel}_${process.pid}_${tableCounter++}`;
}

function tempRoot(label: string): string {
  const root = mkdtempSync(join(tmpdir(), `durust-${label}-`));
  roots.push(root);
  return root;
}

async function startClaimAndScheduleTimer(
  backend: PostgresBackend,
  workflowIdValue: string,
  type: ReturnType<typeof workflowType>,
  fireAt: ReturnType<typeof timestampMs>,
  sequence: number
): Promise<{ readonly runId: string; readonly waitId: string }> {
  await backend.startWorkflow({
    namespace: namespace(),
    workflowId: workflowId(workflowIdValue),
    workflowType: type,
    taskQueue: taskQueue("workflows"),
    input: encodePayload({ value: workflowIdValue }, { codec: "Json" })
  });
  const claimed = await backend.claimWorkflowTask(`worker-${sequence}`, {
    namespace: namespace(),
    taskQueue: taskQueue("workflows"),
    registeredWorkflowTypes: [type],
    leaseDurationMs: 30_000
  });
  expect(claimed).not.toBeNull();
  if (!claimed) {
    throw new Error("expected timer workflow claim");
  }
  const timerCommand = commandId(claimed.runId, sequence);
  const timerWaitId = waitId(`${claimed.runId}:timer:${sequence}`);
  await backend.commitWorkflowTask(claimed.claim, {
    expectedTailEventId: eventId(1),
    appendEvents: [
      {
        data: {
          kind: "TimerStarted",
          started: {
            commandId: timerCommand,
            fireAt,
            fingerprint: timerFingerprint("sleep_until", fireAt)
          }
        }
      }
    ],
    upsertWaits: [
      {
        waitId: timerWaitId,
        runId: claimed.runId,
        commandId: timerCommand,
        kind: "Timer",
        key: "timer",
        readyAt: fireAt
      }
    ]
  });
  return { runId: String(claimed.runId), waitId: String(timerWaitId) };
}

async function startClaimAndScheduleActivity(
  backend: PostgresBackend,
  workflowIdValue: string,
  type: ReturnType<typeof workflowType>,
  sequence: number
): Promise<{ readonly runId: string; readonly activityId: string }> {
  await backend.startWorkflow({
    namespace: namespace(),
    workflowId: workflowId(workflowIdValue),
    workflowType: type,
    taskQueue: taskQueue("workflows"),
    input: encodePayload({ value: workflowIdValue }, { codec: "Json" })
  });
  const claimed = await backend.claimWorkflowTask(`activity-scheduler-${sequence}`, {
    namespace: namespace(),
    taskQueue: taskQueue("workflows"),
    registeredWorkflowTypes: [type],
    leaseDurationMs: 30_000
  });
  expect(claimed).not.toBeNull();
  if (!claimed) {
    throw new Error("expected activity workflow claim");
  }
  const activityInput = encodePayload({ value: sequence }, { codec: "Json" });
  const scheduled = {
    commandId: commandId(claimed.runId, 1),
    activityName: "postgres.normalized-activity-completion-targeted",
    taskQueue: "activities",
    retryPolicy: RetryPolicy.none(),
    startToCloseTimeoutMs: null,
    heartbeatTimeoutMs: null,
    input: activityInput,
    fingerprint: activityFingerprint(
      "postgres.normalized-activity-completion-targeted",
      payloadDigest(activityInput),
      "sha256:test-options"
    )
  };
  const task = activityTaskFromScheduled(scheduled);
  await backend.commitWorkflowTask(claimed.claim, {
    expectedTailEventId: eventId(1),
    appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
    scheduleActivities: [task]
  });
  return { runId: String(claimed.runId), activityId: task.activityId };
}

function requirePostgresUrl(): string {
  if (postgresUrl === undefined) {
    throw new Error("DURUST_POSTGRES_URL is required for Postgres tests");
  }
  return postgresUrl;
}

interface NormalizedHistoryRecord {
  readonly run_id: string;
  readonly event_id: number;
  readonly event_type: string;
  readonly data: { readonly kind?: string };
}

interface NormalizedWorkflowRunRecord {
  readonly run_id: string;
  readonly workflow_id: string;
  readonly workflow_type_name: string;
  readonly workflow_type_version: number;
  readonly task_queue: string;
  readonly tail_event_id: number;
  readonly ready_reason: string | null;
  readonly claim_worker_id: string | null;
  readonly claim_token: string | null;
  readonly claim_reason: string | null;
  readonly claim_expires_at_ms: string | null;
  readonly terminal: boolean;
}

interface NormalizedQueryProjectionRecord {
  readonly run_id: string;
  readonly namespace: string;
  readonly workflow_id: string;
  readonly projection: unknown;
}

interface NormalizedWaitRecord {
  readonly wait_id: string;
  readonly run_id: string;
  readonly kind: string;
  readonly key: string;
  readonly ready_at_ms: string | null;
}

interface NormalizedSignalRecord {
  readonly signal_id: string;
  readonly run_id: string;
  readonly signal_name: string;
  readonly consumed: boolean;
}

interface NormalizedActivityTaskRecord {
  readonly activity_id: string;
  readonly activity_name: string;
  readonly task_queue: string;
  readonly task: unknown;
  readonly input: unknown;
  readonly claim_token: string | null;
  readonly timeout_deadline_at_ms: string | null;
  readonly terminal_event_id: number | null;
}

interface NormalizedActivityMapRecord {
  readonly command_key: string;
  readonly run_id: string;
  readonly task: unknown;
  readonly input_manifest: unknown;
  readonly input_count: number;
  readonly inputs: unknown;
  readonly results: unknown;
  readonly in_flight: unknown;
  readonly next_ordinal: number;
  readonly terminal: boolean;
}

interface NormalizedActivityMapItemRecord {
  readonly command_key: string;
  readonly run_id: string;
  readonly item_ordinal: number;
  readonly input: unknown;
  readonly result: unknown | null;
  readonly in_flight: boolean;
  readonly terminal: boolean;
}

interface NormalizedChildWorkflowMapRecord {
  readonly command_key: string;
  readonly run_id: string;
  readonly task: unknown;
  readonly input_manifest: unknown;
  readonly input_count: number;
  readonly inputs: unknown;
  readonly outcomes: unknown;
  readonly in_flight: unknown;
  readonly next_ordinal: number;
  readonly terminal: boolean;
}

interface NormalizedChildWorkflowMapItemRecord {
  readonly command_key: string;
  readonly run_id: string;
  readonly item_ordinal: number;
  readonly input: unknown;
  readonly outcome: unknown | null;
  readonly in_flight: boolean;
  readonly terminal: boolean;
}

async function readNormalizedHistoryRows(
  tableName: string
): Promise<readonly NormalizedHistoryRecord[]> {
  const pool = new Pool({
    connectionString: requirePostgresUrl(),
    max: 1
  });
  try {
    const result = await pool.query<NormalizedHistoryRecord>(
      `select run_id, event_id, event_type, data
       from ${derivedQuoteIdentifier(tableName, "history_events")}
       order by run_id asc, event_id asc`
    );
    return result.rows;
  } finally {
    await pool.end();
  }
}

async function readNormalizedWorkflowRunRows(
  tableName: string
): Promise<readonly NormalizedWorkflowRunRecord[]> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<NormalizedWorkflowRunRecord>(
      `select
        run_id,
        workflow_id,
        workflow_type_name,
        workflow_type_version,
        task_queue,
        tail_event_id,
        ready_reason,
        claim_worker_id,
        claim_token,
        claim_reason,
        claim_expires_at_ms,
        terminal
       from ${derivedQuoteIdentifier(tableName, "workflow_runs")}
       order by run_id asc`
    );
    return result.rows;
  });
}

async function readNormalizedQueryProjectionRows(
  tableName: string
): Promise<readonly NormalizedQueryProjectionRecord[]> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<NormalizedQueryProjectionRecord>(
      `select run_id, namespace, workflow_id, projection
       from ${derivedQuoteIdentifier(tableName, "query_projections")}
       order by run_id asc`
    );
    return result.rows;
  });
}

async function readNormalizedWaitRows(
  tableName: string
): Promise<readonly NormalizedWaitRecord[]> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<NormalizedWaitRecord>(
      `select wait_id, run_id, kind, key, ready_at_ms
       from ${derivedQuoteIdentifier(tableName, "waits")}
       order by wait_id asc`
    );
    return result.rows;
  });
}

async function readNormalizedSignalRows(
  tableName: string
): Promise<readonly NormalizedSignalRecord[]> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<NormalizedSignalRecord>(
      `select signal_id, run_id, signal_name, consumed
       from ${derivedQuoteIdentifier(tableName, "signals")}
       order by signal_id asc`
    );
    return result.rows;
  });
}

async function readNormalizedActivityTaskRows(
  tableName: string
): Promise<readonly NormalizedActivityTaskRecord[]> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<NormalizedActivityTaskRecord>(
      `select
        activity_id,
        activity_name,
        task_queue,
        task,
        input,
        claim_token,
        timeout_deadline_at_ms,
        terminal_event_id
       from ${derivedQuoteIdentifier(tableName, "activity_tasks")}
       order by activity_id asc`
    );
    return result.rows;
  });
}

async function readNormalizedActivityMapRows(
  tableName: string
): Promise<readonly NormalizedActivityMapRecord[]> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<NormalizedActivityMapRecord>(
      `select
        command_key,
        run_id,
        task,
        input_manifest,
        input_count,
        inputs,
        results,
        in_flight,
        next_ordinal,
        terminal
       from ${derivedQuoteIdentifier(tableName, "activity_maps")}
       order by command_key asc`
    );
    return result.rows;
  });
}

async function readNormalizedActivityMapItemRows(
  tableName: string
): Promise<readonly NormalizedActivityMapItemRecord[]> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<NormalizedActivityMapItemRecord>(
      `select
        command_key,
        run_id,
        item_ordinal,
        input,
        result,
        in_flight,
        terminal
       from ${derivedQuoteIdentifier(tableName, "activity_map_items")}
       order by command_key asc, item_ordinal asc`
    );
    return result.rows;
  });
}

async function readNormalizedChildWorkflowMapRows(
  tableName: string
): Promise<readonly NormalizedChildWorkflowMapRecord[]> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<NormalizedChildWorkflowMapRecord>(
      `select
        command_key,
        run_id,
        task,
        input_manifest,
        input_count,
        inputs,
        outcomes,
        in_flight,
        next_ordinal,
        terminal
       from ${derivedQuoteIdentifier(tableName, "child_workflow_maps")}
       order by command_key asc`
    );
    return result.rows;
  });
}

async function readNormalizedChildWorkflowMapItemRows(
  tableName: string
): Promise<readonly NormalizedChildWorkflowMapItemRecord[]> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<NormalizedChildWorkflowMapItemRecord>(
      `select
        command_key,
        run_id,
        item_ordinal,
        input,
        outcome,
        in_flight,
        terminal
       from ${derivedQuoteIdentifier(tableName, "child_workflow_map_items")}
       order by command_key asc, item_ordinal asc`
    );
    return result.rows;
  });
}

async function rewriteNormalizedWaitKind(
  tableName: string,
  waitId: string,
  kind: string
): Promise<void> {
  await withPostgresPool(async (pool) => {
    await pool.query(
      `update ${derivedQuoteIdentifier(tableName, "waits")}
       set kind = $2
       where wait_id = $1`,
      [waitId, kind]
    );
  });
}

async function rewriteNormalizedSignalConsumed(
  tableName: string,
  signalId: string,
  consumed: boolean
): Promise<void> {
  await withPostgresPool(async (pool) => {
    await pool.query(
      `update ${derivedQuoteIdentifier(tableName, "signals")}
       set consumed = $2
       where signal_id = $1`,
      [signalId, consumed]
    );
  });
}

async function rewriteNormalizedActivityTaskQueue(
  tableName: string,
  activityId: string,
  taskQueueName: string
): Promise<void> {
  await withPostgresPool(async (pool) => {
    await pool.query(
      `update ${derivedQuoteIdentifier(tableName, "activity_tasks")}
       set task_queue = $2
       where activity_id = $1`,
      [activityId, taskQueueName]
    );
  });
}

async function clearNormalizedWorkflowRunQueryProjection(
  tableName: string,
  runId: string
): Promise<void> {
  await withPostgresPool(async (pool) => {
    await pool.query(
      `update ${derivedQuoteIdentifier(tableName, "workflow_runs")}
       set query_projection = null
       where run_id = $1`,
      [runId]
    );
  });
}

async function corruptNormalizedActivityTaskJson(
  tableName: string,
  activityId: string
): Promise<void> {
  await withPostgresPool(async (pool) => {
    await pool.query(
      `update ${derivedQuoteIdentifier(tableName, "activity_tasks")}
       set task = '{"corrupted": true}'::jsonb
       where activity_id = $1`,
      [activityId]
    );
  });
}

async function corruptNormalizedHistoryEventData(
  tableName: string,
  runId: string,
  eventId: number
): Promise<void> {
  await withPostgresPool(async (pool) => {
    await pool.query(
      `update ${derivedQuoteIdentifier(tableName, "history_events")}
       set data = '{"kind": "ActivityScheduled", "scheduled": {"activityName": "corrupted"}}'::jsonb
       where run_id = $1 and event_id = $2`,
      [runId, eventId]
    );
  });
}

async function readNormalizedWorkflowIdRunId(
  tableName: string,
  namespace: string,
  workflowId: string
): Promise<string | null> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<{ readonly run_id: string }>(
      `select run_id
       from ${derivedQuoteIdentifier(tableName, "workflow_ids")}
       where namespace = $1 and workflow_id = $2`,
      [namespace, workflowId]
    );
    return result.rows[0]?.run_id ?? null;
  });
}

function normalizedProjectionIndexNames(tableName: string): readonly string[] {
  return [
    "workflow_runs_unclaimed_ready_idx",
    "workflow_runs_expired_claim_idx",
    "waits_due_timer_partial_idx",
    "signals_unconsumed_inbox_idx",
    "activity_tasks_unclaimed_claim_idx",
    "activity_tasks_expired_claim_idx",
    "activity_tasks_timeout_due_idx"
  ].map((suffix) => derivedIdentifier(tableName, suffix));
}

function assertNormalizedProjectionIndexDefinitions(
  tableName: string,
  definitions: ReadonlyMap<string, string>
): void {
  expectIndexDefinition(definitions, derivedIdentifier(tableName, "workflow_runs_unclaimed_ready_idx"), [
    "where",
    "terminal = false",
    "ready_reason is not null",
    "claim_token is null"
  ]);
  expectIndexDefinition(definitions, derivedIdentifier(tableName, "workflow_runs_expired_claim_idx"), [
    "where",
    "terminal = false",
    "claim_token is not null",
    "claim_reason is not null",
    "claim_expires_at_ms is not null"
  ]);
  expectIndexDefinition(definitions, derivedIdentifier(tableName, "waits_due_timer_partial_idx"), [
    "where",
    "kind =",
    "ready_at_ms is not null"
  ]);
  expectIndexDefinition(definitions, derivedIdentifier(tableName, "signals_unconsumed_inbox_idx"), [
    "where",
    "consumed = false"
  ]);
  expectIndexDefinition(definitions, derivedIdentifier(tableName, "activity_tasks_unclaimed_claim_idx"), [
    "where",
    "terminal_event_id is null",
    "claim_token is null"
  ]);
  expectIndexDefinition(definitions, derivedIdentifier(tableName, "activity_tasks_expired_claim_idx"), [
    "where",
    "terminal_event_id is null",
    "claim_token is not null",
    "claim_expires_at_ms is not null"
  ]);
  expectIndexDefinition(definitions, derivedIdentifier(tableName, "activity_tasks_timeout_due_idx"), [
    "where",
    "terminal_event_id is null",
    "claim_token is not null",
    "map_command_key is null",
    "timeout_deadline_at_ms is not null"
  ]);
}

function expectIndexDefinition(
  definitions: ReadonlyMap<string, string>,
  indexName: string,
  expectedFragments: readonly string[]
): void {
  const definition = definitions.get(indexName);
  expect(definition, `missing index definition for ${indexName}`).toBeDefined();
  const normalized = definition?.toLowerCase() ?? "";
  for (const fragment of expectedFragments) {
    expect(normalized).toContain(fragment);
  }
}

async function readIndexDefinitions(
  indexNames: readonly string[]
): Promise<ReadonlyMap<string, string>> {
  return await withPostgresPool(async (pool) => {
    const result = await pool.query<{
      readonly indexname: string;
      readonly indexdef: string;
    }>(
      `
        select indexname, indexdef
        from pg_indexes
        where schemaname = current_schema()
          and indexname = any($1::text[])
      `,
      [indexNames]
    );
    return new Map(result.rows.map((row) => [row.indexname, row.indexdef]));
  });
}

async function rewriteNormalizedWorkflowRunTaskQueue(
  tableName: string,
  runId: string,
  taskQueue: string
): Promise<void> {
  await withPostgresPool(async (pool) => {
    await pool.query(
      `update ${derivedQuoteIdentifier(tableName, "workflow_runs")}
       set task_queue = $2
       where run_id = $1`,
      [runId, taskQueue]
    );
  });
}

async function withPostgresPool<T>(fn: (pool: Pool) => Promise<T>): Promise<T> {
  const pool = new Pool({
    connectionString: requirePostgresUrl(),
    max: 1
  });
  try {
    return await fn(pool);
  } finally {
    await pool.end();
  }
}

function quoteIdentifier(identifier: string): string {
  if (!/^[a-z_][a-z0-9_]*$/iu.test(identifier)) {
    throw new Error(`invalid Postgres identifier: ${identifier}`);
  }
  return `"${identifier.replaceAll("\"", "\"\"")}"`;
}

function derivedIdentifier(base: string, suffix: string): string {
  const suffixPart = `_${suffix}`;
  if (base.length + suffixPart.length <= 63) {
    return `${base}${suffixPart}`;
  }
  const hash = createHash("sha1").update(base).digest("hex").slice(0, 8);
  const prefixLength = Math.max(1, 63 - suffixPart.length - hash.length - 1);
  return `${base.slice(0, prefixLength)}_${hash}${suffixPart}`;
}

function derivedQuoteIdentifier(base: string, suffix: string): string {
  return quoteIdentifier(derivedIdentifier(base, suffix));
}

function revivePostgresJson<T>(value: unknown): T {
  return JSON.parse(JSON.stringify(value), (_key, nested) => {
    if (
      nested &&
      typeof nested === "object" &&
      (nested as { readonly __durustType?: unknown }).__durustType === "Uint8Array" &&
      Array.isArray((nested as { readonly data?: unknown }).data)
    ) {
      return Uint8Array.from((nested as { readonly data: readonly number[] }).data);
    }
    return nested;
  }) as T;
}
