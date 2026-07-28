import { describe, expect, it, vi } from "vitest";
import {
  assertHistoryEventTypeMatches,
  assertLongSoakEnabledWhenRequired,
  longSoakIsEnabled,
  longSoakIsRequired,
  postgresIsRequired
} from "@durust/testing";
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

describe("@durust/testing environment switches", () => {
  // The regression this file exists for: `DURUST_LONG_SOAK=true` used to read
  // as *off*, because the parse was `=== "1"`. It skipped the soak and exited
  // 0, so nothing anywhere reported that the run had not done what it was
  // asked to do.
  it("reads `true` as on, which the previous `=== \"1\"` parse did not", () => {
    expect(longSoakIsEnabled("true")).toBe(true);
    expect(longSoakIsEnabled("1")).toBe(true);
    expect(longSoakIsEnabled("yes")).toBe(true);
  });

  it("treats empty, `0`, and `false` as off", () => {
    expect(longSoakIsEnabled("")).toBe(false);
    expect(longSoakIsEnabled("   ")).toBe(false);
    expect(longSoakIsEnabled("0")).toBe(false);
    expect(longSoakIsEnabled("false")).toBe(false);
    expect(longSoakIsEnabled("FALSE")).toBe(false);
  });

  // Not `longSoakIsEnabled(undefined)`: passing `undefined` to a defaulted
  // parameter selects the default, so that call reads `process.env` instead of
  // testing the unset branch. The first version of this file made exactly that
  // mistake and passed anyway — until the suite ran with `DURUST_*` set, where
  // it compared an environment read against a literal and failed. Stub the
  // variable away and let the default do its real job.
  it("treats an unset variable as off, without reading the ambient one", () => {
    vi.stubEnv("DURUST_LONG_SOAK", undefined);
    vi.stubEnv("DURUST_REQUIRE_LONG_SOAK", undefined);
    try {
      expect(longSoakIsEnabled()).toBe(false);
      expect(longSoakIsRequired()).toBe(false);
    } finally {
      vi.unstubAllEnvs();
    }
  });

  // An unrecognized value reads as on, so a typo runs the gated work instead
  // of silently dropping it.
  it("reads an unrecognized value as on rather than off", () => {
    expect(longSoakIsEnabled("ture")).toBe(true);
    expect(postgresIsRequired("ture")).toBe(true);
  });

  // Explicit values only — see the note above on `undefined`.
  it("gives every switch the same reading", () => {
    for (const value of ["", "   ", "0", "false", "FALSE", "1", "true", "anything"]) {
      expect(longSoakIsEnabled(value)).toBe(postgresIsRequired(value));
      expect(longSoakIsRequired(value)).toBe(postgresIsRequired(value));
    }
  });

  it("fails a required soak that is switched off", () => {
    expect(() => assertLongSoakEnabledWhenRequired(false, true)).toThrow(
      "DURUST_REQUIRE_LONG_SOAK is set, so the long soak must run"
    );
  });

  it("stays silent unless the soak is both required and off", () => {
    expect(() => assertLongSoakEnabledWhenRequired(true, true)).not.toThrow();
    expect(() => assertLongSoakEnabledWhenRequired(false, false)).not.toThrow();
    expect(() => assertLongSoakEnabledWhenRequired(true, false)).not.toThrow();
  });
});
