import { describe, expect, it } from "vitest";
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
import { basicProviderConformanceCases } from "@durust/testing";

describe("MemoryBackend basic provider conformance", () => {
  for (const conformanceCase of basicProviderConformanceCases()) {
    it(conformanceCase.name, async () => {
      await conformanceCase.run(() => new MemoryBackend());
    });
  }
});

describe("MemoryBackend retry timing", () => {
  it("delays retryable activity attempts according to retry policy", async () => {
    let now = 1_000;
    const backend = new MemoryBackend({ nowMs: () => now });
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/retry-timing"),
      workflowType: workflowType("memory.retry-timing", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: 1 }, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("workflow-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("memory.retry-timing", 1)],
      leaseDurationMs: 30_000
    });
    expect(claim).not.toBeNull();
    if (!claim) {
      throw new Error("expected workflow claim");
    }

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
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/heartbeat-deadline"),
      workflowType: workflowType("memory.heartbeat-deadline", 1),
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ value: 1 }, { codec: "Json" })
    });
    const claim = await backend.claimWorkflowTask("workflow-worker", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [workflowType("memory.heartbeat-deadline", 1)],
      leaseDurationMs: 30_000
    });
    expect(claim).not.toBeNull();
    if (!claim) {
      throw new Error("expected workflow claim");
    }

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
