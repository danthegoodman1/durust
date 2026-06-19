import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import {
  encodePayload,
  eventId,
  namespace,
  prepareWorkflowTaskCommit,
  taskQueue,
  workflow,
  workflowId
} from "@durust/core";
import { LocalDirectoryBlobStore, PayloadBackend } from "@durust/payload";
import { PostgresBackend } from "@durust/postgres";
import { basicProviderConformanceCases } from "@durust/testing";

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

function requirePostgresUrl(): string {
  if (postgresUrl === undefined) {
    throw new Error("DURUST_POSTGRES_URL is required for Postgres tests");
  }
  return postgresUrl;
}
