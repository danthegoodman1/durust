import {
  Client,
  MemoryBackend,
  Registry,
  Worker,
  childWorkflow,
  eventId,
  runId,
  workflow
} from "@durust/core";

interface ParentInput {
  readonly orderId: string;
}

interface ParentOutput {
  readonly childRunId: string;
}

interface ChildInput {
  readonly orderId: string;
}

interface ChildOutput {
  readonly orderId: string;
  readonly archived: true;
}

interface ParentClosePolicyExampleResult {
  readonly cancelChildEvents: readonly string[];
  readonly abandonChildEvents: readonly string[];
}

const archiveOrder = workflow({
  name: "examples.parent-close-policy.archive-order",
  version: 1,
  handler: async (input: ChildInput): Promise<ChildOutput> => ({
    orderId: input.orderId,
    archived: true
  })
});

const cancelParent = workflow({
  name: "examples.parent-close-policy.cancel-parent",
  version: 1,
  handler: async (input: ParentInput): Promise<ParentOutput> => {
    const child = await childWorkflow(
      archiveOrder,
      { orderId: input.orderId },
      {
        workflowId: `parent-close-policy/cancel/${input.orderId}`,
        taskQueue: "workflows",
        parentClosePolicy: "Cancel"
      }
    ).spawn();
    return { childRunId: String(child.runId) };
  }
});

const abandonParent = workflow({
  name: "examples.parent-close-policy.abandon-parent",
  version: 1,
  handler: async (input: ParentInput): Promise<ParentOutput> => {
    const child = await childWorkflow(
      archiveOrder,
      { orderId: input.orderId },
      {
        workflowId: `parent-close-policy/abandon/${input.orderId}`,
        taskQueue: "workflows",
        parentClosePolicy: "Abandon"
      }
    ).spawn();
    return { childRunId: String(child.runId) };
  }
});

export async function runMemoryParentClosePolicyExample(): Promise<ParentClosePolicyExampleResult> {
  const backend = new MemoryBackend();
  const registry = new Registry()
    .registerWorkflow(cancelParent)
    .registerWorkflow(abandonParent)
    .registerWorkflow(archiveOrder);
  const client = new Client(backend, { payloadCodec: "Json" });
  const worker = new Worker({
    backend,
    registry,
    workerId: "examples-parent-close-policy-worker",
    workflowTaskQueue: "workflows",
    payloadCodec: "Json"
  });

  const cancelHandle = await client.startWorkflow(
    cancelParent,
    "parent-close-policy/cancel-parent",
    "workflows",
    { orderId: "order-cancel" }
  );
  await expectCommitted(worker.runWorkflowTaskOnce());
  await expectCommitted(worker.runWorkflowTaskOnce());
  const cancelOutput = await cancelHandle.result();
  const cancelChildEvents = await eventTypes(backend, cancelOutput.childRunId);

  const abandonHandle = await client.startWorkflow(
    abandonParent,
    "parent-close-policy/abandon-parent",
    "workflows",
    { orderId: "order-abandon" }
  );
  await expectCommitted(worker.runWorkflowTaskOnce());
  await expectCommitted(worker.runWorkflowTaskOnce());
  const abandonOutput = await abandonHandle.result();
  await expectCommitted(worker.runWorkflowTaskOnce());
  const abandonChildEvents = await eventTypes(backend, abandonOutput.childRunId);

  return {
    cancelChildEvents,
    abandonChildEvents
  };
}

async function eventTypes(
  backend: MemoryBackend,
  childRunId: string
): Promise<readonly string[]> {
  const history = await backend.streamHistory({
    runId: runId(childRunId),
    afterEventId: eventId(0),
    upToEventId: eventId(Number.MAX_SAFE_INTEGER),
    maxEvents: Number.MAX_SAFE_INTEGER,
    maxBytes: Number.MAX_SAFE_INTEGER
  });
  return history.events.map((event) => event.eventType);
}

async function expectCommitted(
  outcome: Promise<Awaited<ReturnType<Worker["runWorkflowTaskOnce"]>>>
): Promise<void> {
  const resolved = await outcome;
  if (resolved.kind !== "Committed" || resolved.outcome.kind !== "Committed") {
    throw new Error("expected committed workflow task");
  }
}
