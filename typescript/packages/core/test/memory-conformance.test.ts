import { afterEach, describe, expect, it } from "vitest";
import {
  MemoryBackend,
  RetryPolicy,
  activityFingerprint,
  activityTaskFromScheduled,
  commandId,
  encodePayload,
  eventId,
  namespace,
  payloadDigest,
  taskQueue,
  workflowId,
  workflowType
} from "@durust/core";
import {
  assertCurrentTimeFollowsInjectedClock,
  basicProviderConformanceCases,
  claimWorkflow,
  startTestWorkflow
} from "@durust/testing";

/**
 * Cases this file actually executed, counted by the module-scope `afterEach`
 * below, which Vitest runs after every executed test in the file and not for a
 * skipped one.
 *
 * The assertion at the bottom of the file catches the hole this counter exists
 * for: a run where `MemoryBackend` is fine but the suite collapsed to nothing
 * — the conformance loop deleted, `basicProviderConformanceCases()` returning
 * an empty array, a `describe` accidentally narrowed. Every one of those
 * shapes reports success today, because a `for` loop over zero cases registers
 * zero tests and a suite of zero tests is indistinguishable from a suite that
 * passed. Ported from the Postgres conformance suite, which has carried this
 * counter since a mutation that narrowed its blob-backed loop to one case
 * still exited 0.
 *
 * Unlike the Postgres file there is nothing to skip here — `MemoryBackend` is
 * in-process — so this file needs no module-scope availability throw to
 * complement it.
 */
let executedCases = 0;

afterEach(() => {
  executedCases += 1;
});

describe("MemoryBackend basic provider conformance", () => {
  for (const conformanceCase of basicProviderConformanceCases()) {
    it(conformanceCase.name, async () => {
      await conformanceCase.run(() => new MemoryBackend());
    });
  }
});

describe("MemoryBackend clock", () => {
  it("reports its configured clock from currentTime and scans against it", async () => {
    let now = 0;
    const backend = new MemoryBackend({ nowMs: () => now });
    await assertCurrentTimeFollowsInjectedClock(backend, (ms) => {
      now = ms;
    });
  });
});

describe("MemoryBackend retry timing", () => {
  it("delays retryable activity attempts according to retry policy", async () => {
    let now = 1_000;
    const backend = new MemoryBackend({ nowMs: () => now });
    await startTestWorkflow(backend, {
      workflowId: workflowId("wf/retry-timing"),
      workflowType: workflowType("memory.retry-timing", 1),
      input: encodePayload({ value: 1 }, { codec: "Json" })
    });
    const claim = await claimWorkflow(backend, "workflow-worker", {
      workflowTypes: [workflowType("memory.retry-timing", 1)]
    });

    const input = encodePayload({ value: 1 }, { codec: "Json" });
    const scheduled = {
      commandId: commandId(claim.runId, 1),
      activityName: "memory.retry-timing.activity",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.exponential({
        initialIntervalMs: 100,
        maxIntervalMs: 1_000,
        maxAttempts: 3,
        backoffCoefficient: 2
      }),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: null,
      input,
      fingerprint: activityFingerprint(
        "memory.retry-timing.activity",
        payloadDigest(input),
        "sha256:test-options"
      )
    };
    await backend.commitWorkflowTask(claim.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
      scheduleActivities: [activityTaskFromScheduled(scheduled)]
    });

    const first = await backend.claimActivityTask("activity-worker-1", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["memory.retry-timing.activity"],
      leaseDurationMs: 30_000
    });
    expect(first?.task.attempt).toBe(1);
    if (!first) {
      throw new Error("expected first activity attempt");
    }
    await expect(
      backend.failActivity({
        claim: first.claim,
        failure: {
          errorType: "memory.retryable",
          message: "first failure",
          nonRetryable: false
        }
      })
    ).resolves.toEqual({ kind: "RetryScheduled", attempt: 2, readyAtMs: 1_100 });

    now = 1_099;
    await expect(
      backend.claimActivityTask("activity-worker-too-early", {
        namespace: namespace(),
        taskQueue: taskQueue("activities"),
        registeredActivityNames: ["memory.retry-timing.activity"],
        leaseDurationMs: 30_000
      })
    ).resolves.toBeNull();

    now = 1_100;
    const second = await backend.claimActivityTask("activity-worker-2", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["memory.retry-timing.activity"],
      leaseDurationMs: 30_000
    });
    expect(second?.task.attempt).toBe(2);
    if (!second) {
      throw new Error("expected second activity attempt");
    }
    await expect(
      backend.failActivity({
        claim: second.claim,
        failure: {
          errorType: "memory.retryable",
          message: "second failure",
          nonRetryable: false
        }
      })
    ).resolves.toEqual({ kind: "RetryScheduled", attempt: 3, readyAtMs: 1_300 });

    now = 1_300;
    const third = await backend.claimActivityTask("activity-worker-3", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["memory.retry-timing.activity"],
      leaseDurationMs: 30_000
    });
    expect(third?.task.attempt).toBe(3);
    if (!third) {
      throw new Error("expected third activity attempt");
    }
    await expect(
      backend.failActivity({
        claim: third.claim,
        failure: {
          errorType: "memory.retryable",
          message: "terminal failure",
          nonRetryable: false
        }
      })
    ).resolves.toEqual({ kind: "Failed", eventId: eventId(3) });
  });
});

describe("MemoryBackend heartbeat timing", () => {
  it("extends heartbeat timeout deadlines when the activity records liveness", async () => {
    let now = 1_000;
    const backend = new MemoryBackend({ nowMs: () => now });
    await startTestWorkflow(backend, {
      workflowId: workflowId("wf/heartbeat-deadline"),
      workflowType: workflowType("memory.heartbeat-deadline", 1),
      input: encodePayload({ value: 1 }, { codec: "Json" })
    });
    const claim = await claimWorkflow(backend, "workflow-worker", {
      workflowTypes: [workflowType("memory.heartbeat-deadline", 1)]
    });

    const input = encodePayload({ value: 1 }, { codec: "Json" });
    const scheduled = {
      commandId: commandId(claim.runId, 1),
      activityName: "memory.heartbeat-deadline.activity",
      taskQueue: "activities",
      retryPolicy: RetryPolicy.none(),
      startToCloseTimeoutMs: null,
      heartbeatTimeoutMs: 100,
      input,
      fingerprint: activityFingerprint(
        "memory.heartbeat-deadline.activity",
        payloadDigest(input),
        "sha256:test-options"
      )
    };
    await backend.commitWorkflowTask(claim.claim, {
      expectedTailEventId: eventId(1),
      appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
      scheduleActivities: [activityTaskFromScheduled(scheduled)]
    });

    const activity = await backend.claimActivityTask("activity-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("activities"),
      registeredActivityNames: ["memory.heartbeat-deadline.activity"],
      leaseDurationMs: 30_000
    });
    expect(activity?.task.attempt).toBe(1);
    if (!activity) {
      throw new Error("expected heartbeat activity");
    }

    now = 1_099;
    await expect(
      backend.timeoutDueActivities({ namespace: namespace(), now, limit: 8 })
    ).resolves.toEqual({ timedOut: 0 });
    await expect(backend.heartbeatActivity({ claim: activity.claim })).resolves.toEqual({
      kind: "Recorded"
    });

    now = 1_198;
    await expect(
      backend.timeoutDueActivities({ namespace: namespace(), now, limit: 8 })
    ).resolves.toEqual({ timedOut: 0 });

    now = 1_199;
    await expect(
      backend.timeoutDueActivities({ namespace: namespace(), now, limit: 8 })
    ).resolves.toEqual({ timedOut: 1 });
    const workflowWake = await backend.claimWorkflowTask("workflow-worker-after-timeout", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("memory.heartbeat-deadline", 1)],
      leaseDurationMs: 30_000
    });
    expect(workflowWake?.reason).toBe("ActivityTimedOut");
  });
});

// Declared last on purpose: Vitest runs a file's tests in declaration order
// (nothing here sets `sequence.shuffle` or uses `.concurrent`), so every case
// above has already run and been counted by the time this executes. Its own
// `afterEach` increment lands after the assertion, so it never counts itself.
describe("MemoryBackend suite coverage", () => {
  it("executed the shared conformance list rather than registering none", () => {
    // Two assertions, and the first is the load-bearing half. A floor read
    // from the case list tracks the suite instead of going stale — but that
    // is exactly what makes an *empty* list self-consistent: `1 * 0` is a
    // floor every run clears, so a length-derived floor alone cannot fail on
    // the emptied table it is written to catch. The Postgres file had exactly
    // that gap — measured: its coverage test passed with the shared table
    // stubbed to `[]` — and carries the same non-empty assertion now. Pinning
    // the list non-empty first is what turns
    // `basicProviderConformanceCases() === []` into a failure instead of a
    // vacuous pass.
    const cases = basicProviderConformanceCases();
    expect(
      cases.length,
      "basicProviderConformanceCases() returned no cases, so the conformance loop in this file registered zero tests and this suite passed without exercising MemoryBackend against the shared list at all"
    ).toBeGreaterThan(0);
    // No multiplier here, and that is not an oversight: unlike the Postgres
    // and SQLite suites this file iterates the shared list **once**, plain
    // only, with no blob-backed second pass. This file's own 3
    // non-conformance cases sit on top of N and are far below it on their
    // own, so narrowing the loop drops the count under the floor.
    const floor = cases.length;
    expect(
      executedCases,
      "this suite must run the whole shared conformance list against MemoryBackend; if you filtered the run with `-t`, that is the cause and the filtered cases still passed — this check exists for the unfiltered runs CI makes"
    ).toBeGreaterThanOrEqual(floor);
  });
});
