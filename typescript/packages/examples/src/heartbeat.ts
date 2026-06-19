import {
  Client,
  MemoryBackend,
  Registry,
  Worker,
  activity,
  callActivity,
  eventId,
  heartbeat,
  workflow
} from "@durust/core";

interface HeartbeatExampleInput {
  readonly assetId: string;
}

interface TranscodeInput {
  readonly assetId: string;
}

interface TranscodeOutput {
  readonly assetId: string;
  readonly status: "transcoded";
}

interface HeartbeatExampleOutput {
  readonly assetId: string;
  readonly status: "transcoded";
}

interface HeartbeatExampleResult {
  readonly output: HeartbeatExampleOutput;
  readonly heartbeatOutcome: "Recorded";
  readonly events: readonly string[];
}

let heartbeatOutcome: "Recorded" | null = null;

const transcodeAsset = activity({
  name: "examples.heartbeat.transcode-asset",
  handler: async (input: TranscodeInput): Promise<TranscodeOutput> => {
    const recorded = await heartbeat();
    if (recorded.kind !== "Recorded") {
      throw new Error(`unexpected heartbeat outcome: ${recorded.kind}`);
    }
    heartbeatOutcome = recorded.kind;
    return {
      assetId: input.assetId,
      status: "transcoded"
    };
  }
});

const heartbeatWorkflow = workflow({
  name: "examples.heartbeat",
  version: 1,
  handler: async (input: HeartbeatExampleInput): Promise<HeartbeatExampleOutput> => {
    return await callActivity(
      transcodeAsset,
      { assetId: input.assetId },
      {
        taskQueue: "activities",
        heartbeatTimeoutMs: 30_000
      }
    );
  }
});

export async function runMemoryHeartbeatExample(): Promise<HeartbeatExampleResult> {
  heartbeatOutcome = null;
  const backend = new MemoryBackend();
  const registry = new Registry()
    .registerWorkflow(heartbeatWorkflow)
    .registerActivity(transcodeAsset);
  const client = new Client(backend, { payloadCodec: "Json" });
  const worker = new Worker({
    backend,
    registry,
    workerId: "examples-heartbeat-worker",
    workflowTaskQueue: "workflows",
    activityTaskQueue: "activities",
    payloadCodec: "Json"
  });

  const handle = await client.startWorkflow(
    heartbeatWorkflow,
    "heartbeat/asset-1",
    "workflows",
    { assetId: "asset-1" }
  );

  await expectCommitted(worker.runWorkflowTaskOnce());
  await expectActivityCompleted(worker.runActivityTaskOnce());
  await expectCommitted(worker.runWorkflowTaskOnce());

  const output = await handle.result();
  if (heartbeatOutcome === null) {
    throw new Error("expected activity heartbeat to record");
  }
  const history = await backend.streamHistory({
    runId: handle.runId,
    afterEventId: eventId(0),
    upToEventId: eventId(Number.MAX_SAFE_INTEGER),
    maxEvents: Number.MAX_SAFE_INTEGER,
    maxBytes: Number.MAX_SAFE_INTEGER
  });

  return {
    output,
    heartbeatOutcome,
    events: history.events.map((event) => event.eventType)
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

async function expectActivityCompleted(
  outcome: Promise<Awaited<ReturnType<Worker["runActivityTaskOnce"]>>>
): Promise<void> {
  const resolved = await outcome;
  if (resolved.kind !== "Completed" || resolved.outcome.kind !== "Completed") {
    throw new Error("expected completed activity task");
  }
}
