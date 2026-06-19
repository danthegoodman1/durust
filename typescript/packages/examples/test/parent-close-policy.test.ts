import { describe, expect, it } from "vitest";
import { runMemoryParentClosePolicyExample } from "../src/parent-close-policy.js";

describe("parent close policy example", () => {
  it("shows Cancel versus Abandon child workflow behavior against the memory backend", async () => {
    await expect(runMemoryParentClosePolicyExample()).resolves.toEqual({
      cancelChildEvents: [
        "WorkflowStarted",
        "WorkflowCancelled"
      ],
      abandonChildEvents: [
        "WorkflowStarted",
        "WorkflowCompleted"
      ]
    });
  });
});
