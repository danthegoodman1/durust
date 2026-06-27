import { describe, expect, it } from "vitest";
import { assertHistoryEventTypeMatches } from "@durust/testing";
import { eventId, newHistoryEvent } from "@durust/core";

describe("@durust/testing contract assertions", () => {
  it("accepts matching history event types", () => {
    const event = newHistoryEvent(eventId(1), { kind: "WorkflowTaskStarted" });

    expect(() => assertHistoryEventTypeMatches(event)).not.toThrow();
  });

  it("rejects mismatched history event types", () => {
    const event = {
      eventId: eventId(1),
      eventType: "WorkflowCompleted",
      data: { kind: "WorkflowTaskStarted" }
    } as const;

    expect(() => assertHistoryEventTypeMatches(event)).toThrow("history event type mismatch");
  });
});
