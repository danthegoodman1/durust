import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client, Registry, Worker, activity, callActivity, workflow } from "@durust/core";
import { NativeBackend } from "@durust/native";

interface PayloadOffloadInput {
  readonly noteId: string;
  readonly body: string;
}

interface SummarizeNoteInput {
  readonly noteId: string;
  readonly body: string;
}

interface SummarizeNoteOutput {
  readonly noteId: string;
  readonly length: number;
  readonly retainedBody: string;
}

interface PayloadOffloadOutput {
  readonly noteId: string;
  readonly length: number;
  readonly retainedBody: string;
}

interface PayloadOffloadExampleResult {
  readonly output: PayloadOffloadOutput;
  /** Files in the blob directory: content-addressed, so equal bytes share one. */
  readonly blobCount: number;
  /** Payload roots the provider holds as blob references rather than inline. */
  readonly offloadedPayloads: number;
}

const summarizeNote = activity({
  name: "examples.payload.summarize-note",
  handler: async (input: SummarizeNoteInput): Promise<SummarizeNoteOutput> => ({
    noteId: input.noteId,
    length: input.body.length,
    retainedBody: input.body
  })
});

const payloadOffloadWorkflow = workflow({
  name: "examples.payload.offload",
  version: 1,
  handler: async (input: PayloadOffloadInput): Promise<PayloadOffloadOutput> => {
    return await callActivity(
      summarizeNote,
      {
        noteId: input.noteId,
        body: input.body
      },
      { taskQueue: "activities" }
    );
  }
});

/**
 * Payload offload is a provider option: payloads over `inlineThresholdBytes`
 * go to the blob store, and the worker and client see them inline again on
 * every read. The workflow code above knows nothing about it.
 */
export async function runMemoryPayloadOffloadExample(): Promise<PayloadOffloadExampleResult> {
  const root = await mkdtemp(join(tmpdir(), "durust-example-payload-"));
  try {
    const backend = NativeBackend.memory({
      payload: {
        inlineThresholdBytes: 64,
        blobStore: { kind: "LocalDirectory", root, prefix: "objects" }
      }
    });
    const registry = new Registry()
      .registerWorkflow(payloadOffloadWorkflow)
      .registerActivity(summarizeNote);
    const client = new Client(backend, { payloadCodec: "Json" });
    const worker = new Worker({
      backend,
      registry,
      workerId: "examples-payload-worker",
      workflowTaskQueue: "workflows",
      activityTaskQueue: "activities",
      payloadCodec: "Json"
    });
    const largeBody = "payload-offload-example ".repeat(32);
    const handle = await client.startWorkflow(
      payloadOffloadWorkflow,
      "payload-offload/note-1",
      "workflows",
      {
        noteId: "note-1",
        body: largeBody
      }
    );

    await expectCommitted(worker.runWorkflowTaskOnce());
    await expectCompleted(worker.runActivityTaskOnce());
    await expectCommitted(worker.runWorkflowTaskOnce());

    const output = await handle.result();
    const roots = (await backend.payloadRoots()) as readonly { readonly kind: string }[];

    return {
      output,
      blobCount: (await readdir(join(root, "objects"))).length,
      offloadedPayloads: roots.filter((payload) => payload.kind === "Blob").length
    };
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function expectCommitted(
  outcome: Promise<Awaited<ReturnType<Worker["runWorkflowTaskOnce"]>>>
): Promise<void> {
  const resolved = await outcome;
  if (resolved.kind !== "Committed" || resolved.outcome.kind !== "Committed") {
    throw new Error("expected committed workflow task");
  }
}

async function expectCompleted(
  outcome: Promise<Awaited<ReturnType<Worker["runActivityTaskOnce"]>>>
): Promise<void> {
  const resolved = await outcome;
  if (resolved.kind !== "Completed" || resolved.outcome.kind !== "Completed") {
    throw new Error("expected completed activity task");
  }
}
