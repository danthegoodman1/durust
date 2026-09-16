import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import {
  decodePayload,
  encodePayload,
  eventId,
  namespace,
  taskQueue,
  workflowId,
  workflowType,
  type DurableBackend
} from "@durust/core";
import {
  assertCurrentTimeFollowsInjectedClock,
  assertPostgresAvailableWhenRequired,
  basicProviderConformanceCases,
  postgresUrlFromEnv
} from "@durust/testing";
import { NativeBackend, nativeModuleAvailable, nativeModulePath } from "@durust/native";

if (!nativeModuleAvailable()) {
  throw new Error(
    `the durust native module is missing at ${nativeModulePath()}; ` +
      "run `npm run build:native --workspace @durust/native` first"
  );
}

const postgresUrl = postgresUrlFromEnv();
assertPostgresAvailableWhenRequired(postgresUrl, "the native Postgres provider conformance suite");
const describePostgres = postgresUrl === undefined ? describe.skip : describe;

const roots: string[] = [];
const postgresBackends: NativeBackend[] = [];
let schemaCounter = 0;
let executedCases = 0;
let executedPostgresCases = 0;

function tempRoot(label: string): string {
  const root = mkdtempSync(join(tmpdir(), `durust-native-${label}-`));
  roots.push(root);
  return root;
}

function tempSqlitePath(label: string): string {
  return join(tempRoot(label.replace(/\W+/g, "-")), "durust.db");
}

/** A Postgres backend on a schema of its own, dropped after the case. */
async function postgresBackend(label: string): Promise<NativeBackend> {
  if (postgresUrl === undefined) {
    throw new Error("DURUST_POSTGRES_URL is unset");
  }
  const safeLabel = label.replace(/[^a-z0-9_]/giu, "_").toLowerCase().slice(0, 24);
  const backend = await NativeBackend.postgres(postgresUrl, {
    schema: `durust_native_${safeLabel}_${process.pid}_${schemaCounter++}`
  });
  postgresBackends.push(backend);
  return backend;
}

afterEach(async () => {
  executedCases += 1;
  for (const backend of postgresBackends.splice(0)) {
    await backend.destroy().catch(() => undefined);
  }
});

afterAll(() => {
  for (const root of roots) {
    rmSync(root, { recursive: true, force: true });
  }
});

const cases = basicProviderConformanceCases();

describe("NativeBackend.memory() provider conformance", () => {
  for (const conformanceCase of cases) {
    it(conformanceCase.name, async () => {
      await conformanceCase.run(() => NativeBackend.memory());
    });
  }
});

describe("NativeBackend.sqlite() provider conformance", () => {
  for (const conformanceCase of cases) {
    it(conformanceCase.name, async () => {
      await conformanceCase.run(() => NativeBackend.sqlite(tempSqlitePath(conformanceCase.name)));
    });
  }
});

describePostgres("NativeBackend.postgres() provider conformance", () => {
  for (const conformanceCase of cases) {
    it(conformanceCase.name, async () => {
      const backend = await postgresBackend(conformanceCase.name);
      executedPostgresCases += 1;
      await conformanceCase.run(() => backend);
    });
  }
});

describe("NativeBackend clock", () => {
  it("memory reports its configured clock from currentTime and scans against it", async () => {
    let now = 0;
    const backend = NativeBackend.memory({ nowMs: () => now });
    await assertCurrentTimeFollowsInjectedClock(backend, (ms) => {
      now = ms;
    });
  });

  it("sqlite reports its configured clock from currentTime and scans against it", async () => {
    let now = 0;
    const backend = NativeBackend.sqlite(tempSqlitePath("clock"), { nowMs: () => now });
    await assertCurrentTimeFollowsInjectedClock(backend, (ms) => {
      now = ms;
    });
  });
});

/**
 * Payload offload through the Rust payload backend: a start payload over the
 * inline threshold lands in the blob store, the worker's claim carries it
 * hydrated, the raw roots still name the blob for GC, and a sweep keeps it
 * while it is reachable.
 */
async function assertOffloadIsTransparent(backend: DurableBackend, blobDir: string): Promise<void> {
  const input = encodePayload({ body: "x".repeat(128) }, { codec: "Json" });
  await backend.startWorkflow({
    namespace: namespace(),
    workflowId: workflowId("wf/payload-start"),
    workflowType: workflowType("payload.workflow", 1),
    taskQueue: taskQueue("workflows"),
    input
  });
  expect(readdirSync(blobDir)).toHaveLength(1);

  const claim = await backend.claimWorkflowTask("worker-a", {
    namespace: namespace(),
    taskQueue: taskQueue("workflows"),
    registeredWorkflowTypes: [workflowType("payload.workflow", 1)],
    leaseDurationMs: 30_000
  });
  const started = claim?.prefetchedHistory[0]?.data;
  if (started?.kind !== "WorkflowStarted") {
    throw new Error(`expected a hydrated WorkflowStarted, got ${String(started?.kind)}`);
  }
  expect(started.input.kind).toBe("Inline");
  expect(decodePayload(started.input)).toEqual({ body: "x".repeat(128) });
  expect(claim?.replayTargetEventId).toBe(eventId(1));

  const roots = (await backend.payloadRoots()) as readonly { readonly kind: string }[];
  expect(roots.some((root) => root.kind === "Blob")).toBe(true);
}

describe("NativeBackend payload offload", () => {
  it("memory offloads start payloads to a local directory and hydrates claims", async () => {
    const root = tempRoot("payload-memory");
    const backend = NativeBackend.memory({
      payload: { inlineThresholdBytes: 8, blobStore: { kind: "LocalDirectory", root } }
    });
    await assertOffloadIsTransparent(backend, root);
    const swept = await backend.gcPayloadBlobs({ minAgeMs: 0 });
    expect(swept).toEqual({ scannedBlobs: 1, retainedBlobs: 1, deletedBlobs: 0, failedBlobs: 0 });
    expect(readdirSync(root)).toHaveLength(1);
  });

  it("sqlite offloads start payloads to a local directory and hydrates claims", async () => {
    const root = tempRoot("payload-sqlite");
    const backend = NativeBackend.sqlite(tempSqlitePath("payload"), {
      payload: { inlineThresholdBytes: 8, blobStore: { kind: "LocalDirectory", root, prefix: "blobs" } }
    });
    await assertOffloadIsTransparent(backend, join(root, "blobs"));
  });

  it("a dry-run sweep reports an unreachable blob without deleting it", async () => {
    const root = tempRoot("payload-gc");
    const backend = NativeBackend.memory({
      payload: { inlineThresholdBytes: 8, blobStore: { kind: "LocalDirectory", root } }
    });
    await assertOffloadIsTransparent(backend, root);
    const stray = join(root, "sha256:" + "0".repeat(64));
    rmSync(stray, { force: true });
    const { writeFileSync } = await import("node:fs");
    writeFileSync(stray, "orphan");
    const dryRun = await backend.gcPayloadBlobs({ dryRun: true, minAgeMs: 0 });
    expect(dryRun.scannedBlobs).toBe(2);
    expect(dryRun.retainedBlobs).toBe(1);
    expect(readdirSync(root)).toHaveLength(2);
    const swept = await backend.gcPayloadBlobs({ minAgeMs: 0 });
    expect(swept.deletedBlobs).toBe(1);
    expect(readdirSync(root)).toHaveLength(1);
  });
});

describe("NativeBackend lifecycle", () => {
  it("refuses calls after close", async () => {
    const backend = NativeBackend.memory();
    backend.close();
    await expect(backend.currentTime()).rejects.toThrow("backend is closed");
  });
});

describePostgres("NativeBackend.postgres() lifecycle", () => {
  it("destroy drops the schema so a reconnect starts empty", async () => {
    const backend = await postgresBackend("lifecycle");
    const started = await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/lifecycle"),
      workflowType: workflowType("lifecycle.workflow", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: 1 }, { codec: "Json" })
    });
    expect(started.kind).toBe("Started");
    await backend.destroy();
    await expect(backend.currentTime()).rejects.toThrow("backend is closed");
  });
});

describe("suite shape", () => {
  it("ran every shared case against the memory and SQLite providers", () => {
    expect(cases.length).toBeGreaterThan(0);
    expect(executedCases).toBeGreaterThanOrEqual(cases.length * 2);
  });

  it("ran every shared case against Postgres when a database was configured", () => {
    expect(executedPostgresCases).toBe(postgresUrl === undefined ? 0 : cases.length);
  });
});
