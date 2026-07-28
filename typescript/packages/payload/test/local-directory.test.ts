import { mkdtemp, rm, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import {
  Client,
  MemoryBackend,
  Registry,
  RetryPolicy,
  Worker,
  activityFingerprint,
  activityMapFingerprint,
  activityMapManifest,
  activityTaskFromScheduled,
  commandId,
  decodePayload,
  digestBytes,
  encodePayload,
  eventId,
  namespace,
  payloadDigest,
  taskQueue,
  workflow,
  workflowId,
  workflowType,
  type PayloadRef
} from "@durust/core";
import {
  DEFAULT_PAYLOAD_GC_MIN_AGE_MS,
  LocalDirectoryBlobStore,
  PayloadBackend,
  type PayloadBlobStore,
  collectPayloadGarbage,
  collectPayloadRefs,
  collectPayloadRefsDeep,
  decodePayloadWithStorage,
  encodePayloadWithStorage,
  hydratePayloadRef,
  planPayloadGarbageCollection
} from "@durust/payload";
import { basicProviderConformanceCases } from "@durust/testing";

/**
 * Cases this file actually executed, counted by the module-scope `afterEach`
 * below, which Vitest runs after every executed test in the file and not for a
 * skipped one.
 *
 * The assertion at the bottom of the file catches the hole this counter exists
 * for: a run where `PayloadBackend` is fine but its shared-conformance suite
 * collapsed to nothing — the conformance loop deleted,
 * `basicProviderConformanceCases()` returning an empty array, a `describe`
 * accidentally narrowed. Every one of those shapes reports success today,
 * because a `for` loop over zero cases registers zero tests and a suite of
 * zero tests is indistinguishable from a suite that passed; the file's own
 * hand-written payload cases would keep the run green while the provider went
 * entirely unexercised against the shared list. Ported from the Postgres
 * conformance suite, which has carried this counter since a mutation that
 * narrowed its blob-backed loop to one case still exited 0.
 *
 * Unlike the Postgres file there is nothing to skip here — the store is a
 * local temp directory — so this file needs no module-scope availability
 * throw to complement it.
 */
let executedCases = 0;

afterEach(() => {
  executedCases += 1;
});

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

  it("passes foreign-scheme refs through hydration unchanged", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-foreign-hydrate-"))
    });
    const foreign = foreignBlobRef({ value: "foreign" });

    await expect(hydratePayloadRef(foreign, store)).resolves.toEqual(foreign);
    await expect(decodePayloadWithStorage(foreign, store)).rejects.toThrow(
      "blob payload must be hydrated before decode"
    );
  });

  it("round-trips foreign-scheme refs through deep PayloadBackend hydration", async () => {
    const inner = new MemoryBackend();
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-foreign-backend-"))
    });
    const backend = new PayloadBackend({
      backend: inner,
      blobStore: store,
      inlineThresholdBytes: 1024
    });
    const foreign = foreignBlobRef({ value: "foreign" });
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/payload-foreign-ref"),
      workflowType: workflowType("payload.foreign", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ foreign }, { codec: "Json" })
    });

    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("payload.foreign", 1)],
      leaseDurationMs: 30_000
    });
    expect(claim).not.toBeNull();
    const started = claim?.prefetchedHistory[0]?.data;
    if (started?.kind !== "WorkflowStarted") {
      throw new Error("expected hydrated WorkflowStarted");
    }
    expect(
      decodePayload<{ readonly foreign: unknown }>(
        started.input as PayloadRef<{ readonly foreign: unknown }>
      ).foreign
    ).toEqual(foreign);
  });

  it("leaves foreign-scheme refs out of wrapper-owned GC reachability", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-foreign-gc-"))
    });
    const foreign = foreignBlobRef({ value: "foreign" });

    await expect(collectPayloadRefsDeep(foreign, store)).resolves.toEqual([foreign]);
    await expect(planPayloadGarbageCollection({ blobStore: store, roots: [foreign] })).resolves.toEqual({
      reachableUris: [],
      unreachableUris: [],
      retainedYoungUris: [],
      retainedCount: 0,
      unreachableCount: 0
    });
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

  it("recovers workflow progress after a blob-store hydration outage and lease expiry", async () => {
    let now = 1_000;
    const inner = new MemoryBackend({ nowMs: () => now });
    const store = new ToggleableBlobStore(
      new LocalDirectoryBlobStore({
        root: await mkdtemp(join(tmpdir(), "durust-payload-outage-"))
      })
    );
    const backend = new PayloadBackend({
      backend: inner,
      blobStore: store,
      inlineThresholdBytes: 8
    });
    const echo = workflow({
      name: "payload.outage.echo",
      version: 1,
      handler: async (input: { readonly body: string }): Promise<{ readonly body: string }> =>
        input
    });
    const registry = new Registry().registerWorkflow(echo);
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const handle = await client.startWorkflow(
      echo,
      workflowId("wf/payload-outage"),
      "workflows",
      { body: "recoverable".repeat(32) }
    );

    store.failGets = true;
    const firstWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "payload-outage-worker-a",
      workflowTaskQueue: "workflows",
      leaseDurationMs: 10,
      payloadCodec: "Json"
    });
    await expect(firstWorker.runWorkflowTaskOnce()).rejects.toThrow("blob store unavailable");

    const rawAfterOutage = await inner.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(rawAfterOutage.events.map((event) => event.eventType)).toEqual(["WorkflowStarted"]);

    const tooEarlyWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "payload-outage-worker-b",
      workflowTaskQueue: "workflows",
      leaseDurationMs: 10,
      payloadCodec: "Json"
    });
    store.failGets = false;
    await expect(tooEarlyWorker.runWorkflowTaskOnce()).resolves.toEqual({ kind: "NoTask" });

    now = 1_011;
    const recoveryWorker = new Worker({
      backend,
      registry,
      namespace: namespace(),
      workerId: "payload-outage-worker-c",
      workflowTaskQueue: "workflows",
      leaseDurationMs: 10,
      payloadCodec: "Json"
    });
    await expect(recoveryWorker.runWorkflowTaskOnce()).resolves.toMatchObject({
      kind: "Committed",
      outcome: { kind: "Committed" }
    });
    await expect(handle.result()).resolves.toEqual({ body: "recoverable".repeat(32) });

    const rawCompleted = await inner.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(rawCompleted.events.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "WorkflowCompleted"
    ]);
    expect(rawCompleted.events.every((event) => {
      if (event.data.kind === "WorkflowStarted") {
        return event.data.input.kind === "Blob";
      }
      if (event.data.kind === "WorkflowCompleted") {
        return event.data.result.kind === "Blob";
      }
      return false;
    })).toBe(true);
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
      planPayloadGarbageCollection({
        blobStore: store,
        roots: [{ payload: reachable }],
        minAgeMs: 0
      })
    ).resolves.toEqual({
      reachableUris: [reachable.uri],
      unreachableUris: [orphan.uri],
      retainedYoungUris: [],
      retainedCount: 1,
      unreachableCount: 1
    });

    await expect(
      collectPayloadGarbage({
        blobStore: store,
        roots: [{ payload: reachable }],
        minAgeMs: 0,
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
        minAgeMs: 0,
        dryRun: false
      })
    ).resolves.toMatchObject({
      deletedUris: [orphan.uri],
      deletedCount: 1
    });
    expect(await store.list()).toEqual([reachable.uri]);
  });

  it("retains in-flight uploads under the default GC grace period", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-gc-inflight-"))
    });
    const inFlight = await encodePayloadWithStorage(
      { body: "uploaded-before-commit".repeat(16) },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    if (inFlight.kind !== "Blob") {
      throw new Error("expected in-flight blob");
    }
    const uploadedAtMs = await store.lastModifiedMs(inFlight.uri);
    if (uploadedAtMs === null) {
      throw new Error("expected local blob mtime");
    }

    await expect(
      collectPayloadGarbage({
        blobStore: store,
        roots: [],
        dryRun: false,
        nowMs: uploadedAtMs + DEFAULT_PAYLOAD_GC_MIN_AGE_MS - 1
      })
    ).resolves.toMatchObject({
      deletedUris: [],
      deletedCount: 0,
      retainedYoungUris: [inFlight.uri],
      unreachableUris: []
    });
    expect(await store.list()).toEqual([inFlight.uri]);
  });

  it("allows minAgeMs zero to collect an otherwise in-flight upload", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-gc-zero-age-"))
    });
    const inFlight = await encodePayloadWithStorage(
      { body: "delete-with-zero-age".repeat(16) },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    if (inFlight.kind !== "Blob") {
      throw new Error("expected in-flight blob");
    }

    await expect(
      collectPayloadGarbage({
        blobStore: store,
        roots: [],
        dryRun: false,
        minAgeMs: 0
      })
    ).resolves.toMatchObject({
      deletedUris: [inFlight.uri],
      deletedCount: 1
    });
    expect(await store.list()).toEqual([]);
  });

  it("keeps committed blobs regardless of age", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-gc-committed-"))
    });
    const committed = await encodePayloadWithStorage(
      { body: "committed".repeat(32) },
      {
        inlineThresholdBytes: 8,
        blobStore: store
      }
    );
    if (committed.kind !== "Blob") {
      throw new Error("expected committed blob");
    }

    await expect(
      collectPayloadGarbage({
        blobStore: store,
        roots: [{ payload: committed }],
        dryRun: false,
        minAgeMs: 0
      })
    ).resolves.toMatchObject({
      deletedUris: [],
      deletedCount: 0,
      reachableUris: [committed.uri]
    });
    expect(await store.list()).toEqual([committed.uri]);
  });

  it("refreshes content-addressed local blob age on deduplicated re-put", async () => {
    const store = new LocalDirectoryBlobStore({
      root: await mkdtemp(join(tmpdir(), "durust-payload-gc-reput-"))
    });
    const value = { body: "same-content".repeat(32) };
    const first = await encodePayloadWithStorage(value, {
      inlineThresholdBytes: 8,
      blobStore: store
    });
    if (first.kind !== "Blob") {
      throw new Error("expected first blob");
    }
    await utimes(fileURLToPath(first.uri), new Date(0), new Date(0));

    await expect(
      planPayloadGarbageCollection({
        blobStore: store,
        roots: [],
        minAgeMs: 1_000,
        nowMs: 2_000
      })
    ).resolves.toMatchObject({
      unreachableUris: [first.uri],
      retainedYoungUris: []
    });

    const second = await encodePayloadWithStorage(value, {
      inlineThresholdBytes: 8,
      blobStore: store
    });
    expect(second).toEqual(first);
    const refreshedAtMs = await store.lastModifiedMs(first.uri);
    if (refreshedAtMs === null) {
      throw new Error("expected refreshed local blob mtime");
    }

    await expect(
      collectPayloadGarbage({
        blobStore: store,
        roots: [],
        dryRun: false,
        minAgeMs: 1_000,
        nowMs: refreshedAtMs + 999
      })
    ).resolves.toMatchObject({
      deletedUris: [],
      deletedCount: 0,
      retainedYoungUris: [first.uri]
    });
    expect(await store.list()).toEqual([first.uri]);
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
        minAgeMs: 0,
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
        minAgeMs: 0,
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

    await expect(backend.planGarbageCollection({ minAgeMs: 0 })).resolves.toMatchObject({
      unreachableUris: [orphan.uri],
      retainedCount: 1,
      unreachableCount: 1
    });
    await expect(backend.collectGarbage({ dryRun: false, minAgeMs: 0 })).resolves.toMatchObject({
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

class ToggleableBlobStore implements PayloadBlobStore {
  failGets = false;

  constructor(readonly inner: PayloadBlobStore) {}

  async put(bytes: Uint8Array, digest: string): Promise<string> {
    return await this.inner.put(bytes, digest);
  }

  async get(uri: string): Promise<Uint8Array> {
    if (this.failGets) {
      throw new Error("blob store unavailable");
    }
    return await this.inner.get(uri);
  }

  async delete(uri: string): Promise<void> {
    await this.inner.delete(uri);
  }

  async list(): Promise<readonly string[]> {
    return await this.inner.list();
  }

  async lastModifiedMs(uri: string): Promise<number | null> {
    return (await this.inner.lastModifiedMs?.(uri)) ?? null;
  }

  owns(uri: string): boolean {
    return this.inner.owns(uri);
  }
}

function foreignBlobRef(value: unknown) {
  const payload = encodePayload(value, { codec: "Json" });
  if (payload.kind !== "Inline") {
    throw new Error("expected inline encoded payload");
  }
  return {
    kind: "Blob" as const,
    codec: payload.codec,
    schemaFingerprint: payload.schemaFingerprint,
    compression: payload.compression,
    encryption: payload.encryption,
    digest: digestBytes(payload.bytes),
    size: payload.bytes.byteLength,
    uri: `test-custom://payloads/${digestBytes(payload.bytes).replace("sha256:", "")}`
  };
}

// Declared last on purpose: Vitest runs a file's tests in declaration order
// (nothing here sets `sequence.shuffle` or uses `.concurrent`), so every case
// above has already run and been counted by the time this executes. Its own
// `afterEach` increment lands after the assertion, so it never counts itself.
describe("PayloadBackend suite coverage", () => {
  it("executed the shared conformance list rather than registering none", () => {
    // Two assertions, and the first is the load-bearing half. A floor read
    // from the case list tracks the suite instead of going stale — but that
    // is exactly what makes an *empty* list self-consistent: `1 * 0` is a
    // floor every run clears, so a length-derived floor alone cannot fail on
    // the emptied table it is written to catch. The Postgres file had exactly
    // that gap — measured: its coverage test passed with the shared table
    // stubbed to `[]` — and carries the same non-empty assertion now. Pinning
    // the list non-empty first is what turns
    // `basicProviderConformanceCases() === []` into a failure instead of a
    // vacuous pass.
    const cases = basicProviderConformanceCases();
    expect(
      cases.length,
      "basicProviderConformanceCases() returned no cases, so the conformance loop in this file registered zero tests and this suite passed without exercising PayloadBackend against the shared list at all"
    ).toBeGreaterThan(0);
    // No multiplier here, and that is not an oversight: unlike the Postgres
    // and SQLite suites this file iterates the shared list **once**, against
    // one blob-backed memory provider, with no second pass. This file's own
    // 20 hand-written payload cases sit on top of N and are far below it on
    // their own, so narrowing the loop drops the count under the floor.
    const floor = cases.length;
    expect(
      executedCases,
      "this suite must run the whole shared conformance list against a blob-backed PayloadBackend; if you filtered the run with `-t`, that is the cause and the filtered cases still passed — this check exists for the unfiltered runs CI makes"
    ).toBeGreaterThanOrEqual(floor);
  });
});
