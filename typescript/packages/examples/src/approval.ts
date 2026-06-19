import {
  Client,
  MemoryBackend,
  Registry,
  Worker,
  publish,
  select,
  signal,
  sleep,
  workflow
} from "@durust/core";

interface ApprovalInput {
  readonly orderId: string;
  readonly approvalTimeoutMs: number;
}

interface ApprovalSignal {
  readonly approvalId: string;
  readonly reviewer: string;
}

interface ApprovalOutput {
  readonly orderId: string;
  readonly status: "approved" | "timed-out";
  readonly approvalId?: string;
}

interface ApprovalView {
  readonly orderId: string;
  readonly status: "waiting" | "approved" | "timed-out";
  readonly reviewer?: string;
}

interface ApprovalExampleResult {
  readonly waiting: ApprovalView;
  readonly completed: ApprovalView;
  readonly output: ApprovalOutput;
}

const approvedSignal = signal<ApprovalSignal>("approved");

const approvalWorkflow = workflow({
  name: "examples.approval.wait",
  version: 1,
  queryStateType: {} as ApprovalView,
  handler: async (input: ApprovalInput): Promise<ApprovalOutput> => {
    publish({
      orderId: input.orderId,
      status: "waiting"
    });

    const winner = await select({
      approved: approvedSignal,
      timeout: sleep(input.approvalTimeoutMs)
    });

    if (winner.branch === "approved") {
      publish({
        orderId: input.orderId,
        status: "approved",
        reviewer: winner.value.reviewer
      });
      return {
        orderId: input.orderId,
        status: "approved",
        approvalId: winner.value.approvalId
      };
    }

    publish({
      orderId: input.orderId,
      status: "timed-out"
    });
    return {
      orderId: input.orderId,
      status: "timed-out"
    };
  }
});

export async function runMemoryApprovalExample(): Promise<ApprovalExampleResult> {
  const backend = new MemoryBackend();
  const registry = new Registry().registerWorkflow(approvalWorkflow);
  const client = new Client(backend, { payloadCodec: "Json" });
  const worker = new Worker({
    backend,
    registry,
    workerId: "examples-approval-worker",
    workflowTaskQueue: "workflows",
    registeredSignalNames: ["approved"],
    payloadCodec: "Json"
  });

  const handle = await client.startWorkflow(
    approvalWorkflow,
    "approval/order-1",
    "workflows",
    {
      orderId: "order-1",
      approvalTimeoutMs: 60_000
    }
  );

  await worker.runWorkflowTaskOnce();
  const waiting = await handle.query();

  await client.sendSignal({
    workflowId: "approval/order-1",
    signal: approvedSignal,
    payload: {
      approvalId: "approval-1",
      reviewer: "ops"
    },
    idempotencyKey: "approval/order-1/approved"
  });

  await worker.runWorkflowTaskOnce();
  const completed = await handle.query();
  const output = await handle.result();

  return {
    waiting,
    completed,
    output
  };
}
