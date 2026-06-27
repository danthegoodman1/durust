import {
  Client,
  MemoryBackend,
  Registry,
  Worker,
  deprecatePatch,
  decodePayload,
  eventId,
  getVersion,
  patched,
  continueAsNew,
  runId,
  workflow
} from "@durust/core";
import type { PayloadRef } from "@durust/core";

interface RolloutInput {
  readonly orderId: string;
  readonly generation: number;
  readonly targetGeneration: number;
}

interface RolloutOutput {
  readonly orderId: string;
  readonly route: "v1" | "v2";
  readonly generation: number;
}

interface VersionedScoreInput {
  readonly orderId: string;
  readonly amountCents: number;
}

interface VersionedScoreOutput {
  readonly orderId: string;
  readonly algorithmVersion: number;
  readonly score: number;
}

interface DeprecatedRouteInput {
  readonly orderId: string;
}

interface DeprecatedRouteOutput {
  readonly orderId: string;
  readonly route: "v2";
}

interface VersioningExampleResult {
  readonly firstRunEvents: readonly string[];
  readonly secondRunEvents: readonly string[];
  readonly output: RolloutOutput;
}

interface VersionBridgeExampleResult {
  readonly versionedEvents: readonly string[];
  readonly deprecatedEvents: readonly string[];
  readonly versionedOutput: VersionedScoreOutput;
  readonly deprecatedOutput: DeprecatedRouteOutput;
}

const rolloutWorkflow = workflow({
  name: "examples.versioning.rollout",
  version: 1,
  handler: async (input: RolloutInput): Promise<RolloutOutput> => {
    const route = patched("route-v2") ? "v2" : "v1";
    if (input.generation < input.targetGeneration) {
      return continueAsNew({
        ...input,
        generation: input.generation + 1
      });
    }
    return {
      orderId: input.orderId,
      route,
      generation: input.generation
    };
  }
});

const versionedScoreWorkflow = workflow({
  name: "examples.versioning.score",
  version: 1,
  handler: async (input: VersionedScoreInput): Promise<VersionedScoreOutput> => {
    const algorithmVersion = getVersion("score-algorithm", 1, 2);
    return {
      orderId: input.orderId,
      algorithmVersion,
      score: input.amountCents * algorithmVersion
    };
  }
});

const deprecatedRouteWorkflow = workflow({
  name: "examples.versioning.deprecated-route",
  version: 1,
  handler: async (input: DeprecatedRouteInput): Promise<DeprecatedRouteOutput> => {
    deprecatePatch("route-v2");
    return {
      orderId: input.orderId,
      route: "v2"
    };
  }
});

export async function runMemoryVersioningExample(): Promise<VersioningExampleResult> {
  const backend = new MemoryBackend();
  const registry = new Registry().registerWorkflow(rolloutWorkflow);
  const client = new Client(backend, { payloadCodec: "Json" });
  const worker = new Worker({
    backend,
    registry,
    workerId: "examples-versioning-worker",
    workflowTaskQueue: "workflows",
    payloadCodec: "Json"
  });

  await client.startWorkflow(
    rolloutWorkflow,
    "versioning/order-1",
    "workflows",
    {
      orderId: "order-1",
      generation: 0,
      targetGeneration: 1
    }
  );

  const first = await worker.runWorkflowTaskOnce();
  if (first.kind !== "Committed") {
    throw new Error("expected first versioning workflow task to commit");
  }
  const second = await worker.runWorkflowTaskOnce();
  if (second.kind !== "Committed") {
    throw new Error("expected continued versioning workflow task to commit");
  }

  const firstHistory = await history(backend, String(first.runId));
  const secondHistory = await history(backend, String(second.runId));
  const completed = secondHistory.events.at(-1)?.data;
  if (completed?.kind !== "WorkflowCompleted") {
    throw new Error("expected continued run to complete");
  }

  return {
    firstRunEvents: firstHistory.events.map((event) => event.eventType),
    secondRunEvents: secondHistory.events.map((event) => event.eventType),
    output: decodePayload<RolloutOutput>(completed.result as PayloadRef<RolloutOutput>)
  };
}

export async function runMemoryVersionBridgeExample(): Promise<VersionBridgeExampleResult> {
  const backend = new MemoryBackend();
  const registry = new Registry()
    .registerWorkflow(versionedScoreWorkflow)
    .registerWorkflow(deprecatedRouteWorkflow);
  const client = new Client(backend, { payloadCodec: "Json" });
  const worker = new Worker({
    backend,
    registry,
    workerId: "examples-version-bridge-worker",
    workflowTaskQueue: "workflows",
    payloadCodec: "Json"
  });

  const versioned = await client.startWorkflow(
    versionedScoreWorkflow,
    "versioning/score/order-1",
    "workflows",
    {
      orderId: "order-1",
      amountCents: 100
    }
  );
  const deprecated = await client.startWorkflow(
    deprecatedRouteWorkflow,
    "versioning/deprecated-route/order-2",
    "workflows",
    {
      orderId: "order-2"
    }
  );

  await expectCommitted(worker.runWorkflowTaskOnce());
  await expectCommitted(worker.runWorkflowTaskOnce());

  return {
    versionedEvents: (await history(backend, String(versioned.runId))).events.map(
      (event) => event.eventType
    ),
    deprecatedEvents: (await history(backend, String(deprecated.runId))).events.map(
      (event) => event.eventType
    ),
    versionedOutput: await versioned.result(),
    deprecatedOutput: await deprecated.result()
  };
}

async function history(backend: MemoryBackend, runIdValue: string) {
  return await backend.streamHistory({
    runId: runId(runIdValue),
    afterEventId: eventId(0),
    upToEventId: eventId(Number.MAX_SAFE_INTEGER),
    maxEvents: Number.MAX_SAFE_INTEGER,
    maxBytes: Number.MAX_SAFE_INTEGER
  });
}

async function expectCommitted(
  outcome: Promise<Awaited<ReturnType<Worker["runWorkflowTaskOnce"]>>>
): Promise<void> {
  const resolved = await outcome;
  if (resolved.kind !== "Committed" || resolved.outcome.kind !== "Committed") {
    throw new Error("expected versioning workflow task to commit");
  }
}
