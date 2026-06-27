import { describe, expect, it } from "vitest";
import {
  Client,
  MemoryBackend,
  decodePayload,
  eventId,
  namespace,
  publish,
  signal,
  taskQueue,
  workflow,
  workflowId
} from "@durust/core";
import type { SchemaAdapter } from "@durust/core";
import { prepareWorkflowTaskCommit } from "@durust/testing";

interface Input {
  readonly value: string;
}

interface Output {
  readonly value: string;
}

interface Approved {
  readonly approvalId: string;
}

interface EchoView {
  readonly status: string;
}

const echoWorkflow = workflow({
  name: "client.echo",
  version: 1,
  handler: async (input: Input): Promise<Output> => input
});

const queryWorkflow = workflow({
  name: "client.query",
  version: 1,
  queryStateType: {} as EchoView,
  handler: async (input: Input): Promise<Output> => {
    publish({ status: input.value });
    return input;
  }
});

const queryViewSchema: SchemaAdapter<EchoView> = {
  fingerprint: "sha256:query-view",
  rootKind: "object",
  encode: (value) => ({ wire_status: value.status }),
  decode: (value) => ({
    status: (value as { readonly wire_status: string }).wire_status
  })
};

const schemaQueryWorkflow = workflow({
  name: "client.schema-query",
  version: 1,
  queryStateType: {} as EchoView,
  queryStateSchema: queryViewSchema,
  handler: async (input: Input): Promise<Output> => {
    publish({ status: input.value });
    return input;
  }
});

describe("backend-backed Client", () => {
  it("starts workflows through the backend and preserves idempotent run identity", async () => {
    const backend = new MemoryBackend();
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });

    const first = await client.startWorkflow(echoWorkflow, workflowId("wf/client"), "workflows", {
      value: "first"
    });
    const second = await client.startWorkflow(echoWorkflow, workflowId("wf/client"), "workflows", {
      value: "first"
    });

    expect(first.runId).toBe(second.runId);
  });

  it("decodes typed workflow results from committed history", async () => {
    const backend = new MemoryBackend();
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const handle = await client.startWorkflow(echoWorkflow, workflowId("wf/result"), "workflows", {
      value: "done"
    });

    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [echoWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected workflow claim");
    }
    const commit = await prepareWorkflowTaskCommit(echoWorkflow, { value: "done" }, claim, {
      payloadCodec: "Json"
    });
    await backend.commitWorkflowTask(claim.claim, commit);

    await expect(handle.result()).resolves.toEqual({ value: "done" });
  });

  it("sends typed object signal payloads through the backend with idempotency", async () => {
    const backend = new MemoryBackend();
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const approved = signal<Approved>("approved");
    const handle = await client.startWorkflow(echoWorkflow, workflowId("wf/signal-client"), "workflows", {
      value: "waiting"
    });

    await client.sendSignal({
      workflowId: workflowId("wf/signal-client"),
      signal: approved,
      payload: { approvalId: "a-1" },
      idempotencyKey: "approval/a-1"
    });
    await client.sendSignal({
      workflowId: workflowId("wf/signal-client"),
      signal: approved,
      payload: { approvalId: "a-1" },
      idempotencyKey: "approval/a-1"
    });

    const inbox = await backend.readSignalInbox({
      runId: handle.runId,
      signalName: "approved"
    });
    expect(inbox).not.toBeNull();
    if (!inbox) {
      throw new Error("expected signal inbox record");
    }
    expect(decodePayload<Approved>(inbox.payload)).toEqual({ approvalId: "a-1" });

    const history = await backend.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(10),
      maxEvents: 10,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    expect(history.events).toHaveLength(1);
  });

  it("reports unavailable results without hidden polling", async () => {
    const backend = new MemoryBackend();
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const handle = await client.startWorkflow(echoWorkflow, workflowId("wf/pending"), "workflows", {
      value: "pending"
    });

    await expect(handle.result()).rejects.toThrow("workflow result is not available");
  });

  it("reads typed query projections published by workflow commits", async () => {
    const backend = new MemoryBackend();
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const handle = await client.startWorkflow(
      queryWorkflow,
      workflowId("wf/query"),
      "workflows",
      { value: "running" }
    );

    await expect(client.queryWorkflow(queryWorkflow, workflowId("wf/query"))).rejects.toThrow(
      "workflow query projection is not available"
    );

    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [queryWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected workflow claim");
    }
    const commit = await prepareWorkflowTaskCommit(queryWorkflow, { value: "running" }, claim, {
      payloadCodec: "Json"
    });
    await backend.commitWorkflowTask(claim.claim, commit);

    await expect(client.queryWorkflow(queryWorkflow, workflowId("wf/query"))).resolves.toEqual({
      status: "running"
    });
    await expect(handle.query()).resolves.toEqual({ status: "running" });
  });

  it("encodes query projections through workflow query-state schema", async () => {
    const backend = new MemoryBackend();
    const client = new Client(backend, { namespace: namespace(), payloadCodec: "Json" });
    const handle = await client.startWorkflow(
      schemaQueryWorkflow,
      workflowId("wf/schema-query"),
      "workflows",
      { value: "encoded-running" }
    );

    const claim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [schemaQueryWorkflow.workflowType],
      leaseDurationMs: 30_000
    });
    if (!claim) {
      throw new Error("expected workflow claim");
    }
    const commit = await prepareWorkflowTaskCommit(
      schemaQueryWorkflow,
      { value: "encoded-running" },
      claim,
      { payloadCodec: "Json" }
    );
    expect(commit.queryProjection?.schemaFingerprint).toBe("sha256:query-view");
    if (commit.queryProjection === undefined) {
      throw new Error("expected query projection payload");
    }
    expect(decodePayload<{ readonly wire_status: string }>(commit.queryProjection)).toEqual({
      wire_status: "encoded-running"
    });
    await backend.commitWorkflowTask(claim.claim, commit);

    await expect(
      client.queryWorkflow(schemaQueryWorkflow, workflowId("wf/schema-query"))
    ).resolves.toEqual({ status: "encoded-running" });
    await expect(handle.query()).resolves.toEqual({ status: "encoded-running" });
  });
});
