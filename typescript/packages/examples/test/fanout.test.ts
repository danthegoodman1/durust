import { describe, expect, it } from "vitest";
import { runMemoryFanoutExample } from "../src/fanout.js";

describe("fanout example", () => {
  it("runs activity-map and child-workflow-map fanout against the memory backend", async () => {
    await expect(runMemoryFanoutExample()).resolves.toEqual({
      runName: "run-1",
      itemCount: 4,
      scaledSum: 170,
      squaredSum: 87
    });
  });
});
