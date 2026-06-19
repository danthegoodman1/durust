import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  signalFingerprint,
  timerFingerprint,
  versionMarkerFingerprint,
  type CommandFingerprint,
  type HistoryEvent
} from "@durust/core";
import { assertContractFixtureEvents } from "@durust/testing";

interface CoreContractFixture {
  readonly historyEvents: readonly HistoryEvent[];
  readonly fingerprints: {
    readonly timer: CommandFingerprint;
    readonly signal: CommandFingerprint;
    readonly versionMarker: CommandFingerprint;
  };
}

describe("contract fixtures", () => {
  it("loads neutral history fixtures and validates event type derivation", () => {
    const fixture = loadFixture();

    assertContractFixtureEvents(fixture.historyEvents);
    expect(fixture.historyEvents.map((event) => event.eventType)).toEqual([
      "WorkflowStarted",
      "ActivityScheduled"
    ]);
  });

  it("matches fingerprint helpers to fixture examples", () => {
    const fixture = loadFixture();

    expect(timerFingerprint("sleep_until", 1_781_821_484_000)).toEqual(fixture.fingerprints.timer);
    expect(signalFingerprint("approved")).toEqual(fixture.fingerprints.signal);
    expect(versionMarkerFingerprint("checkout-v2", 2)).toEqual(
      fixture.fingerprints.versionMarker
    );
  });
});

function loadFixture(): CoreContractFixture {
  const fixtureUrl = new URL("../../../fixtures/contract/core-events.json", import.meta.url);
  return JSON.parse(readFileSync(fixtureUrl, "utf8")) as CoreContractFixture;
}
