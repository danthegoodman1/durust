import { describe, expect, it } from "vitest";
import { runMemoryPayloadOffloadExample } from "../src/payload-offload.js";

describe("payload offload example", () => {
  it("stores oversized durable payloads through the local-directory blob store", async () => {
    await expect(runMemoryPayloadOffloadExample()).resolves.toEqual({
      output: {
        noteId: "note-1",
        length: "payload-offload-example ".repeat(32).length,
        retainedBody: "payload-offload-example ".repeat(32)
      },
      blobCount: 2,
      payloadKinds: {
        workflowInput: "Blob",
        activityInput: "Blob",
        activityResult: "Blob",
        workflowResult: "Blob"
      }
    });
  });
});
