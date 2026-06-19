import { describe, expect, it } from "vitest";
import { runMemoryRetryExample } from "../src/retry.js";

describe("retry example", () => {
  it("runs provider-owned activity retries against the memory backend", async () => {
    await expect(runMemoryRetryExample()).resolves.toEqual({
      output: {
        paymentId: "payment/request-1/1500",
        attempt: 2
      },
      attempts: 2,
      earlyRetryClaim: "NoTask",
      events: [
        "WorkflowStarted",
        "ActivityScheduled",
        "ActivityCompleted",
        "WorkflowCompleted"
      ]
    });
  });
});
