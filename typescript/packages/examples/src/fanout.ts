import {
  Client,
  MemoryBackend,
  Registry,
  Worker,
  activity,
  activityMap,
  activityMapManifest,
  childWorkflowMap,
  decodeActivityMapResults,
  decodeChildWorkflowMapSuccesses,
  workflow
} from "@durust/core";

interface FanoutInput {
  readonly runName: string;
  readonly values: readonly number[];
  readonly multiplier: number;
  readonly maxInFlight: number;
}

interface FanoutOutput {
  readonly runName: string;
  readonly itemCount: number;
  readonly scaledSum: number;
  readonly squaredSum: number;
}

interface ScaleInput {
  readonly value: number;
  readonly multiplier: number;
}

interface ScaleOutput {
  readonly value: number;
  readonly scaled: number;
}

interface SquareInput {
  readonly value: number;
}

interface SquareOutput {
  readonly value: number;
  readonly squared: number;
}

const scaleNumber = activity({
  name: "examples.fanout.scale-number",
  handler: async (input: ScaleInput): Promise<ScaleOutput> => ({
    value: input.value,
    scaled: input.value * input.multiplier
  })
});

const squareNumber = workflow({
  name: "examples.fanout.square-number",
  version: 1,
  handler: async (input: SquareInput): Promise<SquareOutput> => ({
    value: input.value,
    squared: input.value * input.value
  })
});

const fanoutSummary = workflow({
  name: "examples.fanout.summary",
  version: 1,
  handler: async (input: FanoutInput): Promise<FanoutOutput> => {
    const activityMapped = activityMap(scaleNumber, {
      inputManifest: activityMapManifest(
        input.values.map((value) => ({
          value,
          multiplier: input.multiplier
        })),
        2
      ),
      resultManifest: "scaled-values",
      taskQueue: "activities",
      maxInFlight: input.maxInFlight
    });

    const childMapped = childWorkflowMap(squareNumber, {
      inputManifest: activityMapManifest(
        input.values.map((value) => ({
          value
        })),
        2
      ),
      resultManifest: "squared-values",
      workflowIdPrefix: `fanout/${input.runName}`,
      taskQueue: "workflows",
      maxInFlight: input.maxInFlight
    });

    const scaled = decodeActivityMapResults<ScaleOutput>(
      await activityMapped.resultManifest()
    );
    const squared = decodeChildWorkflowMapSuccesses<SquareOutput>(
      await childMapped.resultManifest()
    );

    return {
      runName: input.runName,
      itemCount: input.values.length,
      scaledSum: scaled.reduce((sum, item) => sum + item.scaled, 0),
      squaredSum: squared.reduce((sum, item) => sum + item.squared, 0)
    };
  }
});

export async function runMemoryFanoutExample(): Promise<FanoutOutput> {
  const backend = new MemoryBackend();
  const registry = new Registry()
    .registerWorkflow(fanoutSummary)
    .registerWorkflow(squareNumber)
    .registerActivity(scaleNumber);
  const client = new Client(backend, { payloadCodec: "Json" });
  const worker = new Worker({
    backend,
    registry,
    workerId: "examples-fanout-worker",
    workflowTaskQueue: "workflows",
    activityTaskQueue: "activities",
    activityCompletionBatchSize: 2,
    payloadCodec: "Json"
  });

  const handle = await client.startWorkflow(
    fanoutSummary,
    "fanout/run-1",
    "workflows",
    {
      runName: "run-1",
      values: [2, 3, 5, 7],
      multiplier: 10,
      maxInFlight: 2
    }
  );

  for (let index = 0; index < 32; index += 1) {
    const workflowTask = await worker.runWorkflowTaskOnce();
    const activityTask = await worker.runActivityTaskBatchOnce(2);
    if (workflowTask.kind === "NoTask" && activityTask.kind === "NoTask") {
      break;
    }
  }

  return await handle.result();
}
