import { describe, expect, it } from "vitest";
import { runMemoryHeartbeatExample } from "../src/heartbeat.js";

describe("heartbeat example", () => {
  it("records activity heartbeats through the public activity context", async () => {
    await expect(runMemoryHeartbeatExample()).resolves.toEqual({
      output: {
        assetId: "asset-1",
        status: "transcoded"
      },
      heartbeatOutcome: "Recorded",
      events: [
        "WorkflowStarted",
        "ActivityScheduled",
        "ActivityCompleted",
        "WorkflowCompleted"
      ]
    });
  });
});
