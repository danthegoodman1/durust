import {
  Client,
  MemoryBackend,
  Registry,
  RetryPolicy,
  Worker,
  activity,
  callActivity,
  eventId,
  workflow
} from "@durust/core";

interface RetryExampleInput {
  readonly requestId: string;
}

interface FlakyChargeInput {
  readonly requestId: string;
  readonly amountCents: number;
}

interface FlakyChargeOutput {
  readonly paymentId: string;
  readonly attempt: number;
}

interface RetryExampleOutput {
  readonly paymentId: string;
  readonly attempt: number;
}

interface RetryExampleResult {
  readonly output: RetryExampleOutput;
  readonly attempts: number;
  readonly earlyRetryClaim: "NoTask";
  readonly events: readonly string[];
}

let flakyChargeAttempts = 0;

const flakyCharge = activity({
  name: "examples.retry.flaky-charge",
  handler: async (input: FlakyChargeInput): Promise<FlakyChargeOutput> => {
    flakyChargeAttempts += 1;
    if (flakyChargeAttempts === 1) {
      throw new Error(`temporary payment outage for ${input.requestId}`);
    }
    return {
      paymentId: `payment/${input.requestId}/${input.amountCents}`,
      attempt: flakyChargeAttempts
    };
  }
});

const retryWorkflow = workflow({
  name: "examples.retry",
  version: 1,
  handler: async (input: RetryExampleInput): Promise<RetryExampleOutput> => {
    const payment = await callActivity(
      flakyCharge,
      { requestId: input.requestId, amountCents: 1_500 },
      {
        taskQueue: "activities",
        retry: RetryPolicy.exponential({
          initialIntervalMs: 100,
          maxIntervalMs: 100,
          maxAttempts: 2
        })
      }
    );
    return {
      paymentId: payment.paymentId,
      attempt: payment.attempt
    };
  }
});

export async function runMemoryRetryExample(): Promise<RetryExampleResult> {
  flakyChargeAttempts = 0;
  let now = 1_000;
  const backend = new MemoryBackend({ nowMs: () => now });
  const registry = new Registry()
    .registerWorkflow(retryWorkflow)
    .registerActivity(flakyCharge);
  const client = new Client(backend, { payloadCodec: "Json" });
  const worker = new Worker({
    backend,
    registry,
    workerId: "examples-retry-worker",
    workflowTaskQueue: "workflows",
    activityTaskQueue: "activities",
    payloadCodec: "Json"
  });

  const handle = await client.startWorkflow(
    retryWorkflow,
    "retry/request-1",
    "workflows",
    { requestId: "request-1" }
  );

  await expectCommitted(worker.runWorkflowTaskOnce());
  await expectRetryScheduled(worker.runActivityTaskOnce(), 2, 1_100);

  now = 1_099;
  const earlyRetryClaim = await worker.runActivityTaskOnce();
  if (earlyRetryClaim.kind !== "NoTask") {
    throw new Error("retry attempt was claimable before its retry delay");
  }

  now = 1_100;
  await expectActivityCompleted(worker.runActivityTaskOnce());
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
    attempts: flakyChargeAttempts,
    earlyRetryClaim: "NoTask",
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

async function expectRetryScheduled(
  outcome: Promise<Awaited<ReturnType<Worker["runActivityTaskOnce"]>>>,
  attempt: number,
  readyAtMs: number
): Promise<void> {
  const resolved = await outcome;
  if (
    resolved.kind !== "Failed" ||
    resolved.outcome.kind !== "RetryScheduled" ||
    resolved.outcome.attempt !== attempt ||
    resolved.outcome.readyAtMs !== readyAtMs
  ) {
    throw new Error("expected retryable activity failure to schedule a retry");
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
