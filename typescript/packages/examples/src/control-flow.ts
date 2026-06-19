import {
  Client,
  MemoryBackend,
  Registry,
  Worker,
  activity,
  callActivity,
  eventId,
  join,
  joinAll,
  selectAll,
  sideEffect,
  signal,
  sleep,
  workflow
} from "@durust/core";

interface ControlFlowInput {
  readonly requestId: string;
  readonly orderIds: readonly string[];
  readonly approvalTimeoutMs: number;
}

interface ScoreOrderInput {
  readonly orderId: string;
}

interface ScoreOrderOutput {
  readonly orderId: string;
  readonly score: number;
}

interface ApprovalSignal {
  readonly approvalId: string;
}

interface ControlFlowOutput {
  readonly requestId: string;
  readonly batchId: string;
  readonly totalScore: number;
  readonly approvedBy: string;
}

interface ControlFlowExampleResult {
  readonly output: ControlFlowOutput;
  readonly events: readonly string[];
}

const approvalSignal = signal<ApprovalSignal>("examples.control-flow.approved");

const scoreOrder = activity({
  name: "examples.control-flow.score-order",
  handler: async (input: ScoreOrderInput): Promise<ScoreOrderOutput> => ({
    orderId: input.orderId,
    score: input.orderId.length * 10
  })
});

const controlFlowWorkflow = workflow({
  name: "examples.control-flow",
  version: 1,
  handler: async (input: ControlFlowInput): Promise<ControlFlowOutput> => {
    if (input.orderIds.length < 4) {
      throw new Error("control-flow example expects at least four orders");
    }

    const batchId = await sideEffect("batch-id", () => `batch/${input.requestId}`);
    const firstTwo = await join({
      first: callActivity(
        scoreOrder,
        { orderId: input.orderIds[0] ?? "" },
        { taskQueue: "activities" }
      ),
      second: callActivity(
        scoreOrder,
        { orderId: input.orderIds[1] ?? "" },
        { taskQueue: "activities" }
      )
    });
    const rest = await joinAll(
      input.orderIds.slice(2).map((orderId) =>
        callActivity(scoreOrder, { orderId }, { taskQueue: "activities" })
      )
    );
    const approval = await selectAll([
      approvalSignal,
      sleep(input.approvalTimeoutMs)
    ] as const);

    if (approval.index !== 0) {
      throw new Error("control-flow example expected approval before timeout");
    }

    return {
      requestId: input.requestId,
      batchId,
      totalScore:
        firstTwo.first.score +
        firstTwo.second.score +
        rest.reduce((sum, item) => sum + item.score, 0),
      approvedBy: (approval.value as ApprovalSignal).approvalId
    };
  }
});

export async function runMemoryControlFlowExample(): Promise<ControlFlowExampleResult> {
  const backend = new MemoryBackend();
  const registry = new Registry()
    .registerWorkflow(controlFlowWorkflow)
    .registerActivity(scoreOrder);
  const client = new Client(backend, { payloadCodec: "Json" });
  const worker = new Worker({
    backend,
    registry,
    workerId: "examples-control-flow-worker",
    workflowTaskQueue: "workflows",
    activityTaskQueue: "activities",
    registeredSignalNames: [approvalSignal.name],
    activityCompletionBatchSize: 2,
    payloadCodec: "Json"
  });

  const handle = await client.startWorkflow(
    controlFlowWorkflow,
    "control-flow/request-1",
    "workflows",
    {
      requestId: "request-1",
      orderIds: ["a", "bb", "ccc", "dddd"],
      approvalTimeoutMs: 60_000
    }
  );

  await expectCommitted(worker.runWorkflowTaskOnce());
  await expectProcessed(worker.runActivityTaskBatchOnce(2));
  await expectCommitted(worker.runWorkflowTaskOnce());
  await expectProcessed(worker.runActivityTaskBatchOnce(2));
  await expectCommitted(worker.runWorkflowTaskOnce());

  await client.sendSignal({
    workflowId: "control-flow/request-1",
    signal: approvalSignal,
    payload: { approvalId: "approval-1" },
    idempotencyKey: "control-flow/request-1/approved"
  });
  await expectCommitted(worker.runWorkflowTaskOnce());

  const output = await handle.result();
  const history = await backend.streamHistory({
    runId: handle.runId,
    afterEventId: eventId(0),
    upToEventId: eventId(Number.MAX_SAFE_INTEGER),
    maxEvents: Number.MAX_SAFE_INTEGER,
    maxBytes: Number.MAX_SAFE_INTEGER
  });

  return {
    output,
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

async function expectProcessed(
  outcome: Promise<Awaited<ReturnType<Worker["runActivityTaskBatchOnce"]>>>
): Promise<void> {
  const resolved = await outcome;
  if (resolved.kind !== "Processed") {
    throw new Error("expected processed activity task batch");
  }
}
