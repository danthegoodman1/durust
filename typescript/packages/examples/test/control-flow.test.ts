import { describe, expect, it } from "vitest";
import { runMemoryControlFlowExample } from "../src/control-flow.js";

describe("control-flow example", () => {
  it("runs join, joinAll, selectAll, and sideEffect against the memory backend", async () => {
    await expect(runMemoryControlFlowExample()).resolves.toEqual({
      output: {
        requestId: "request-1",
        batchId: "batch/request-1",
        totalScore: 100,
        approvedBy: "approval-1"
      },
      events: [
        "WorkflowStarted",
        "SideEffectMarker",
        "ActivityScheduled",
        "ActivityScheduled",
        "ActivityCompleted",
        "ActivityCompleted",
        "ActivityScheduled",
        "ActivityScheduled",
        "ActivityCompleted",
        "ActivityCompleted",
        "TimerStarted",
        "SignalConsumed",
        "SelectWinner",
        "WorkflowCompleted"
      ]
    });
  });
});
