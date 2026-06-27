import { describe, expect, it } from "vitest";
import {
  runMemoryVersionBridgeExample,
  runMemoryVersioningExample
} from "../src/versioning.js";

describe("versioning example", () => {
  it("records version markers and continues as new against the memory backend", async () => {
    await expect(runMemoryVersioningExample()).resolves.toEqual({
      firstRunEvents: [
        "WorkflowStarted",
        "VersionMarker",
        "WorkflowContinuedAsNew"
      ],
      secondRunEvents: [
        "WorkflowStarted",
        "VersionMarker",
        "WorkflowCompleted"
      ],
      output: {
        orderId: "order-1",
        route: "v2",
        generation: 1
      }
    });
  });

  it("records numeric and deprecated version markers against the memory backend", async () => {
    await expect(runMemoryVersionBridgeExample()).resolves.toEqual({
      versionedEvents: [
        "WorkflowStarted",
        "VersionMarker",
        "WorkflowCompleted"
      ],
      deprecatedEvents: [
        "WorkflowStarted",
        "DeprecatedPatchMarker",
        "WorkflowCompleted"
      ],
      versionedOutput: {
        orderId: "order-1",
        algorithmVersion: 2,
        score: 200
      },
      deprecatedOutput: {
        orderId: "order-2",
        route: "v2"
      }
    });
  });
});
