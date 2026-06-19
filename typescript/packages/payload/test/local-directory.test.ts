import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  MemoryBackend,
  RetryPolicy,
  activityFingerprint,
  activityMapFingerprint,
  activityMapManifest,
  activityTaskFromScheduled,
  commandId,
  decodePayload,
  encodePayload,
  eventId,
  namespace,
  payloadDigest,
  taskQueue,
  workflowId,
  workflowType
} from "@durust/core";
import {
  LocalDirectoryBlobStore,
  PayloadBackend,
  collectPayloadGarbage,
  collectPayloadRefs,
  collectPayloadRefsDeep,
  decodePayloadWithStorage,
  encodePayloadWithStorage,
  hydratePayloadRef,
  planPayloadGarbageCollection
} from "@durust/payload";
import { basicProviderConformanceCases } from "@durust/testing";

describe("local-directory payload storage", () => {
  it("keeps small payloads inline and decodes them through the storage helper", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-inline-"))
    });

    const payload = await encodePayloadWithStorage(
      { orderId: "o-1" },
      {
        codec: "Json",
        inlineThresholdBytes: 1024,
        blobStore: store
      }
    );

    expect(payload.kind).toBe("Inline");
    await expect(decodePayloadWithStorage(payload, store)).resolves.toEqual({ orderId: "o-1" });
    await expect(store.list()).resolves.toEqual([]);
  });

  it("offloads large payloads and hydrates them with digest validation", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-blob-")),
      prefix: "objects"
    });

    const payload = await encodePayloadWithStorage(
      { body: "x".repeat(128) },
      {
        codec: "Json",
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );

    expect(payload.kind).toBe("Blob");
    if (payload.kind !== "Blob") {
      throw new Error("expected blob payload");
    }
    expect(payload.size).toBeGreaterThan(8);
    expect(store.owns(payload.uri)).toBe(true);
    expect(await store.list()).toEqual([payload.uri]);

    const hydrated = await hydratePayloadRef(payload, store);
    expect(hydrated.kind).toBe("Inline");
    expect(decodePayload(hydrated)).toEqual({ body: "x".repeat(128) });
  });

  it("rejects corrupt blob bytes during hydration", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-corrupt-"))
    });
    const payload = await encodePayloadWithStorage(
      { body: "x".repeat(128) },
      {
        codec: "Json",
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    if (payload.kind !== "Blob") {
      throw new Error("expected blob payload");
    }

    await writeFile(fileURLToPath(payload.uri), new TextEncoder().encode("corrupt"));

    await expect(hydratePayloadRef(payload, store)).rejects.toThrow(
      "blob payload size mismatch"
    );
  });

  it("offloads workflow start payloads while hydrated claims stay transparent", async () => {
    const inner = new MemoryBackend();
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-backend-start-"))
    });
    const backend = new PayloadBackend({
      backend: inner,
      blobStore: store,
      inlineThresholdBytes: 8
    });
    const input = encodePayload({ body: "x".repeat(128) }, { codec: "Json" });

    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/payload-start"),
      workflowType: workflowType("payload.workflow", 1),
      taskQueue: taskQueue("workflows"),
      input
    });

    const rawHistory = await inner.streamHistory({
      runId: (await backend.startWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/payload-start"),
        workflowType: workflowType("payload.workflow", 1),
        taskQueue: taskQueue("workflows"),
        input
      })).runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const rawStarted = rawHistory.events[0]?.data;
    expect(rawStarted?.kind).toBe("WorkflowStarted");
    if (rawStarted?.kind !== "WorkflowStarted") {
      throw new Error("expected raw WorkflowStarted");
    }
    expect(rawStarted.input.kind).toBe("Blob");

    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("payload.workflow", 1)],
      leaseDurationMs: 30_000
    });
    expect(claim?.prefetchedHistory[0]?.data.kind).toBe("WorkflowStarted");
    const started = claim?.prefetchedHistory[0]?.data;
    if (started?.kind !== "WorkflowStarted") {
      throw new Error("expected hydrated WorkflowStarted");
    }
    expect(started.input.kind).toBe("Inline");
    expect(decodePayload(started.input)).toEqual({ body: "x".repeat(128) });
    expect(await store.list()).toHaveLength(1);
  });

  it("offloads activity inputs and completion results across backend calls", async () => {
    const inner = new MemoryBackend();
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-backend-activity-"))
    });
    const backend = new PayloadBackend({
      backend: inner,
      blobStore: store,
      inlineThresholdBytes: 8
    });
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/payload-activity"),
      workflowType: workflowType("payload.activity-parent", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("payload.activity-parent", 1)],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected workflow claim");
    }
    const activityInput = encodePayload({ body: "i".repeat(128) }, { codec: "Json" });
    const scheduled = {
      commandId: commandId(claim.runId, 1),
      activityName: "payload.activity",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      input: activityInput,
      fingerprint: activityFingerprint(
        "payload.activity",
        payloadDigest(activityInput),
        "sha256:test-options"
      )
    };
    await backend.commitWorkflowTask(claim.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
      scheduleActivities: [activityTaskFromScheduled(scheduled)]
    });

    const activityTask = await backend.claimActivityTask("activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["payload.activity"],
      leaseDurationMs: 30_000
    });
    expect(activityTask?.task.input.kind).toBe("Inline");
    if (!activityTask) {
      throw new Error("expected hydrated activity task");
    }
    expect(decodePayload(activityTask.task.input)).toEqual({ body: "i".repeat(128) });

    await backend.completeActivity({
      claim: activityTask.claim,
      result: encodePayload({ body: "r".repeat(128) }, { codec: "Json" })
    });
    const rawHistory = await inner.streamHistory({
      runId: claim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const rawCompleted = rawHistory.events.find((event) => event.data.kind === "ActivityCompleted")
      ?.data;
    expect(rawCompleted?.kind).toBe("ActivityCompleted");
    if (rawCompleted?.kind !== "ActivityCompleted") {
      throw new Error("expected raw ActivityCompleted");
    }
    expect(rawCompleted.completed.result.kind).toBe("Blob");

    const hydratedHistory = await backend.streamHistory({
      runId: claim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const hydratedCompleted = hydratedHistory.events.find(
      (event) => event.data.kind === "ActivityCompleted"
    )?.data;
    if (hydratedCompleted?.kind !== "ActivityCompleted") {
      throw new Error("expected hydrated ActivityCompleted");
    }
    expect(hydratedCompleted.completed.result.kind).toBe("Inline");
    expect(decodePayload(hydratedCompleted.completed.result)).toEqual({ body: "r".repeat(128) });
    expect((await store.list()).length).toBeGreaterThanOrEqual(2);
  });

  it("offloads map scheduled history while keeping provider descriptor manifests usable", async () => {
    const inner = new MemoryBackend();
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-backend-map-"))
    });
    const backend = new PayloadBackend({
      backend: inner,
      blobStore: store,
      inlineThresholdBytes: 8
    });
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/payload-map"),
      workflowType: workflowType("payload.map-parent", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({}, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("payload.map-parent", 1)],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected workflow claim");
    }
    const inputManifest = activityMapManifest([
      { body: "a".repeat(64) },
      { body: "b".repeat(64) }
    ]);
    const scheduled = {
      commandId: commandId(claim.runId, 1),
      activityName: "payload.map",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      inputManifest,
      resultManifestName: "mapped",
      maxInFlight: 1,
      fingerprint: activityMapFingerprint(
        "payload.map",
        payloadDigest(inputManifest),
        "mapped",
        1,
        "sha256:test-options"
      )
    };
    await backend.commitWorkflowTask(claim.claim, {
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

    const rawHistory = await inner.streamHistory({
      runId: claim.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const rawScheduled = rawHistory.events.find((event) => event.data.kind === "ActivityMapScheduled")
      ?.data;
    expect(rawScheduled?.kind).toBe("ActivityMapScheduled");
    if (rawScheduled?.kind !== "ActivityMapScheduled") {
      throw new Error("expected raw ActivityMapScheduled");
    }
    expect(rawScheduled.scheduled.inputManifest.kind).toBe("Blob");

    const firstItem = await backend.claimActivityTask("map-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["payload.map"],
      leaseDurationMs: 30_000
    });
    expect(firstItem?.task.mapItem?.itemOrdinal).toBe(0);
    expect(firstItem?.task.input.kind).toBe("Inline");
  });

  it("collects payload refs through nested provider facts", () => {
    const first = encodePayload({ value: 1 }, { codec: "Json" });
    const second = encodePayload({ value: 2 }, { codec: "Json" });

    expect(collectPayloadRefs({
      event: { data: { result: first } },
      tasks: [{ input: second }]
    })).toEqual([first, second]);
  });

  it("plans and collects unreachable local-directory blobs after validating roots", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-gc-"))
    });
    const reachable = await encodePayloadWithStorage(
      { body: "reachable".repeat(32) },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    const orphan = await encodePayloadWithStorage(
      { body: "orphan".repeat(32) },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    if (reachable.kind !== "Blob" || orphan.kind !== "Blob") {
      throw new Error("expected blob payloads");
    }

    await expect(
      planPayloadGarbageCollection({ blobStore: store, roots: [{ payload: reachable }] })
    ).resolves.toEqual({
      reachableUris: [reachable.uri],
      unreachableUris: [orphan.uri],
      retainedCount: 1,
      unreachableCount: 1
    });

    await expect(
      collectPayloadGarbage({
        blobStore: store,
        roots: [{ payload: reachable }],
        dryRun: true
      })
    ).resolves.toMatchObject({
      deletedUris: [],
      deletedCount: 0,
      unreachableUris: [orphan.uri]
    });
    expect(await store.list()).toEqual([orphan.uri, reachable.uri].sort());

    await expect(
      collectPayloadGarbage({
        blobStore: store,
        roots: [{ payload: reachable }],
        dryRun: false
      })
    ).resolves.toMatchObject({
      deletedUris: [orphan.uri],
      deletedCount: 1
    });
    expect(await store.list()).toEqual([reachable.uri]);
  });

  it("recursively retains blob refs nested inside manifest payloads", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-gc-manifest-"))
    });
    const result = await encodePayloadWithStorage(
      { body: "result".repeat(32) },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    const page = await encodePayloadWithStorage(
      { results: [result] },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    const manifest = await encodePayloadWithStorage(
      {
        name: "nested",
        itemCount: 1,
        pageLengths: [1],
        pages: [page]
      },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    if (result.kind !== "Blob" || page.kind !== "Blob" || manifest.kind !== "Blob") {
      throw new Error("expected nested blob payloads");
    }

    await expect(collectPayloadRefsDeep(manifest, store)).resolves.toEqual([
      manifest,
      page,
      result
    ]);
    await expect(
      planPayloadGarbageCollection({ blobStore: store, roots: [manifest] })
    ).resolves.toMatchObject({
      reachableUris: [manifest.uri, page.uri, result.uri].sort(),
      unreachableUris: []
    });
  });

  it("fails garbage collection before deleting when a reachable blob is corrupt", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-gc-corrupt-"))
    });
    const reachable = await encodePayloadWithStorage(
      { body: "reachable".repeat(32) },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    const orphan = await encodePayloadWithStorage(
      { body: "orphan".repeat(32) },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    if (reachable.kind !== "Blob" || orphan.kind !== "Blob") {
      throw new Error("expected blob payloads");
    }
    await writeFile(fileURLToPath(reachable.uri), new TextEncoder().encode("corrupt"));

    await expect(
      collectPayloadGarbage({
        blobStore: store,
        roots: [reachable],
        dryRun: false
      })
    ).rejects.toThrow("blob payload size mismatch");
    expect(await store.list()).toEqual([orphan.uri, reachable.uri].sort());
  });

  it("fails garbage collection before deleting when a reachable blob is missing", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-gc-missing-"))
    });
    const reachable = await encodePayloadWithStorage(
      { body: "reachable".repeat(32) },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    const orphan = await encodePayloadWithStorage(
      { body: "orphan".repeat(32) },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    if (reachable.kind !== "Blob" || orphan.kind !== "Blob") {
      throw new Error("expected blob payloads");
    }
    await rm(fileURLToPath(reachable.uri));

    await expect(
      collectPayloadGarbage({
        blobStore: store,
        roots: [reachable],
        dryRun: false
      })
    ).rejects.toThrow();
    expect(await store.list()).toEqual([orphan.uri]);
  });

  it("collects wrapper-owned garbage using roots exposed by the inner backend", async () => {
    const inner = new MemoryBackend();
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-backend-gc-"))
    });
    const backend = new PayloadBackend({
      backend: inner,
      blobStore: store,
      inlineThresholdBytes: 8
    });
    const input = encodePayload({ body: "reachable".repeat(32) }, { codec: "Json" });
    const orphan = await encodePayloadWithStorage(
      { body: "orphan".repeat(32) },
      {
        codec: "Json",
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    if (orphan.kind !== "Blob") {
      throw new Error("expected orphan blob");
    }
    const started = await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/payload-backend-gc"),
      workflowType: workflowType("payload.gc", 1),
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
    expect(await store.list()).toEqual([startedEvent.input.uri]);
  });
});

describe("PayloadBackend provider conformance", () => {
  for (const conformanceCase of basicProviderConformanceCases()) {
    it(`blob-backed memory provider: ${conformanceCase.name}`, async () => {
      let counter = 0;
      await conformanceCase.run(() => {
        const inner = new MemoryBackend();
        const store = new LocalDirectoryBlobStore({
          root: join(tmpdir(), `durust-payload-conformance-${process.pid}-${counter++}`),
          prefix: "objects"
        });
        return new PayloadBackend({
          backend: inner,
          blobStore: store,
          inlineThresholdBytes: 0
        });
      });
    });
  }
});
