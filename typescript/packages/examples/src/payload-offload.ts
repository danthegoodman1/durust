import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  Client,
  MemoryBackend,
  Registry,
  Worker,
  activity,
  callActivity,
  eventId,
  workflow
} from "@durust/core";
import { LocalDirectoryBlobStore, PayloadBackend } from "@durust/payload";

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
  readonly blobCount: number;
  readonly payloadKinds: {
    readonly workflowInput: "Blob" | "Inline";
    readonly activityInput: "Blob" | "Inline";
    readonly activityResult: "Blob" | "Inline";
    readonly workflowResult: "Blob" | "Inline";
  };
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

export async function runMemoryPayloadOffloadExample(): Promise<PayloadOffloadExampleResult> {
  const root = await mkdtemp(join(tmpdir(), "durust-example-payload-"));
  try {
    const inner = new MemoryBackend();
    const blobStore = new LocalDirectoryBlobStore({
      root,
      prefix: "objects"
    });
    const backend = new PayloadBackend({
      backend: inner,
      blobStore,
      inlineThresholdBytes: 64
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
    const history = await inner.streamHistory({
      runId: handle.runId,
      afterEventId: eventId(0),
      upToEventId: eventId(Number.MAX_SAFE_INTEGER),
      maxEvents: Number.MAX_SAFE_INTEGER,
      maxBytes: Number.MAX_SAFE_INTEGER
    });
    const payloadKinds = payloadKindsFromHistory(history.events);

    return {
      output,
      blobCount: (await blobStore.list()).length,
      payloadKinds
    };
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function payloadKindsFromHistory(events: Awaited<ReturnType<MemoryBackend["streamHistory"]>>["events"]): PayloadOffloadExampleResult["payloadKinds"] {
  const started = events.find((event) => event.data.kind === "WorkflowStarted")?.data;
  const activityScheduled = events.find((event) => event.data.kind === "ActivityScheduled")?.data;
  const activityCompleted = events.find((event) => event.data.kind === "ActivityCompleted")?.data;
  const workflowCompleted = events.find((event) => event.data.kind === "WorkflowCompleted")?.data;
  if (
    started?.kind !== "WorkflowStarted" ||
    activityScheduled?.kind !== "ActivityScheduled" ||
    activityCompleted?.kind !== "ActivityCompleted" ||
    workflowCompleted?.kind !== "WorkflowCompleted"
  ) {
    throw new Error("expected payload offload history events");
  }
  return {
    workflowInput: started.input.kind,
    activityInput: activityScheduled.scheduled.input.kind,
    activityResult: activityCompleted.completed.result.kind,
    workflowResult: workflowCompleted.result.kind
  };
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
