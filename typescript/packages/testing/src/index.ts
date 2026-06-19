import type { DurableBackend, HistoryEvent, WorkflowTaskClaim } from "@durust/core";
import {
  RetryPolicy,
  activityMapFingerprint,
  activityMapManifest,
  activityFingerprint,
  activityTaskFromScheduled,
  childWorkflowFingerprint,
  childWorkflowMapFingerprint,
  commandId,
  decodePayload,
  digestBytes,
  encodePayload,
  eventId,
  historyEventType,
  namespace,
  payloadDigest,
  signalFingerprint,
  signalId,
  taskQueue,
  timestampMs,
  timerFingerprint,
  waitId,
  workflowId,
  workflowType
} from "@durust/core";
import type { PayloadRef } from "@durust/core";
import type {
  ActivityMapInputManifest,
  ActivityMapInputPage,
  ActivityMapResultManifest,
  ActivityMapResultPage,
  ChildWorkflowMapResultManifest,
  ChildWorkflowMapResultPage
} from "@durust/core";

export function assertHistoryEventTypeMatches(event: HistoryEvent): void {
  const derived = historyEventType(event.data);
  if (event.eventType !== derived) {
    throw new Error(`history event type mismatch: expected ${event.eventType}, derived ${derived}`);
  }
}

export function assertContractFixtureEvents(events: readonly HistoryEvent[]): void {
  for (const event of events) {
    assertHistoryEventTypeMatches(event);
  }
}

export interface ProviderConformanceCase {
  readonly name: string;
  run(factory: () => DurableBackend): Promise<void>;
}

export function basicProviderConformanceCases(): readonly ProviderConformanceCase[] {
  return [
    {
      name: "start workflow is idempotent by namespace and workflow id",
      async run(factory) {
        const backend = factory();
        const request = {
          namespace: namespace(),
          workflowId: workflowId("wf/idempotent"),
          workflowType: workflowType("conformance.workflow", 1),
          taskQueue: taskQueue("workflows"),
          input: encodePayload({ value: 1 }, { codec: "Json" })
        };

        const first = await backend.startWorkflow(request);
        const second = await backend.startWorkflow(request);
        assert(first.kind === "Started", "first start should create a run");
        assert(second.kind === "AlreadyStarted", "second start should be idempotent");
        assert(first.runId === second.runId, "idempotent start should return first run id");
      }
    },
    {
      name: "claim workflow task filters by queue and registered workflow type",
      async run(factory) {
        const backend = factory();
        await backend.startWorkflow({
          namespace: namespace(),
          workflowId: workflowId("wf/claim"),
          workflowType: workflowType("conformance.workflow", 1),
          taskQueue: taskQueue("workflows"),
          input: encodePayload({ value: 1 }, { codec: "Json" })
        });

        const wrongQueue = await backend.claimWorkflowTask("worker-a", {
          namespace: namespace(),
          taskQueue: taskQueue("other"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(wrongQueue === null, "wrong task queue must not claim workflow task");

        const wrongType = await backend.claimWorkflowTask("worker-a", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("other.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(wrongType === null, "unregistered workflow type must not be claimed");

        const claimed = await backend.claimWorkflowTask("worker-a", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(claimed !== null, "matching queue and workflow type should claim task");
        assert(claimed.reason === "WorkflowStarted", "first claim reason should be WorkflowStarted");
      }
    },
    {
      name: "expired workflow task leases are reclaimable and fence old commits",
      async run(factory) {
        const backend = factory();
        await backend.startWorkflow({
          namespace: namespace(),
          workflowId: workflowId("wf/expired-workflow-lease"),
          workflowType: workflowType("conformance.workflow", 1),
          taskQueue: taskQueue("workflows"),
          input: encodePayload({ value: 1 }, { codec: "Json" })
        });

        const expired = await backend.claimWorkflowTask("expired-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 0
        });
        assert(expired !== null, "initial workflow claim should be granted");

        const reclaimed = await backend.claimWorkflowTask("replacement-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(reclaimed !== null, "expired workflow claim should be reclaimable");
        assert(
          reclaimed.reason === "WorkflowStarted",
          "reclaimed workflow task should preserve original wake reason"
        );
        assert(
          reclaimed.claim.token !== expired.claim.token,
          "reclaimed workflow task should receive a fresh claim token"
        );

        await assertRejects(
          () =>
            backend.commitWorkflowTask(expired.claim, {
              expectedTailEventId: eventId(1),
              appendEvents: [{ data: { kind: "WorkflowTaskStarted" } }]
            }),
          "stale workflow task lease"
        );

        const committed = await backend.commitWorkflowTask(reclaimed.claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            {
              data: {
                kind: "WorkflowCompleted",
                result: encodePayload({ ok: true }, { codec: "Json" })
              }
            }
          ]
        });
        assert(committed.kind === "Committed", "replacement claim should be able to commit");
      }
    },
    {
      name: "stream history is ordered and bounded",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            { data: { kind: "WorkflowTaskStarted" } },
            {
              data: {
                kind: "WorkflowCompleted",
                result: encodePayload({ ok: true }, { codec: "Json" })
              }
            }
          ]
        });

        const chunk = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(1),
          upToEventId: eventId(3),
          maxEvents: 1,
          maxBytes: Number.MAX_SAFE_INTEGER
        });

        assert(chunk.events.length === 1, "maxEvents should bound streamed history");
        assert(chunk.events[0]?.eventId === eventId(2), "history should stream in event id order");
        assert(chunk.hasMore, "bounded stream should report remaining events");
      }
    },
    {
      name: "workflow task commit appends contiguous event ids and detects stale tails",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const conflict = await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(0),
          appendEvents: [{ data: { kind: "WorkflowTaskStarted" } }]
        });
        assert(conflict.kind === "Conflict", "stale expected tail should conflict");

        const reclaimed = await backend.claimWorkflowTask("worker-b", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(reclaimed !== null, "conflicted workflow task should become claimable again");

        const committed = await backend.commitWorkflowTask(reclaimed.claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            { data: { kind: "WorkflowTaskStarted" } },
            {
              data: {
                kind: "WorkflowCompleted",
                result: encodePayload({ ok: true }, { codec: "Json" })
              }
            }
          ]
        });

        assert(
          committed.kind === "Committed" && committed.newTailEventId === eventId(3),
          "commit should append contiguous event ids"
        );

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          history.events.map((event) => event.eventId).join(",") === "1,2,3",
          "history event ids should remain contiguous"
        );
      }
    },
    {
      name: "activity task claim and completion appends a workflow wake event idempotently",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const input = encodePayload({ value: 1 }, { codec: "Json" });
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.echo",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.none(),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: null,
          input,
          fingerprint: activityFingerprint(
            "conformance.echo",
            payloadDigest(input),
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
          scheduleActivities: [activityTaskFromScheduled(scheduled)]
        });

        const wrongQueue = await backend.claimActivityTask("activity-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("other"),
          registeredActivityNames: ["conformance.echo"],
          leaseDurationMs: 30_000
        });
        assert(wrongQueue === null, "wrong activity queue must not claim task");

        const claimedActivity = await backend.claimActivityTask("activity-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.echo"],
          leaseDurationMs: 30_000
        });
        assert(claimedActivity !== null, "matching activity should be claimable");

        const completed = await backend.completeActivity({
          claim: claimedActivity.claim,
          result: encodePayload({ value: 2 }, { codec: "Json" })
        });
        assert(
          completed.kind === "Completed" && completed.eventId === eventId(3),
          "activity completion should append ActivityCompleted"
        );

        const duplicate = await backend.completeActivity({
          claim: claimedActivity.claim,
          result: encodePayload({ value: 2 }, { codec: "Json" })
        });
        assert(duplicate.kind === "AlreadyCompleted", "duplicate completion is idempotent");

        const workflowWake = await backend.claimWorkflowTask("worker-after-activity", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(workflowWake !== null, "activity completion should wake workflow");
        assert(
          workflowWake.reason === "ActivityCompleted",
          "activity completion wake should preserve reason"
        );
      }
    },
    {
      name: "expired activity task leases are reclaimable and fence old completions",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const input = encodePayload({ value: 1 }, { codec: "Json" });
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.echo",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.none(),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: null,
          input,
          fingerprint: activityFingerprint(
            "conformance.echo",
            payloadDigest(input),
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
          scheduleActivities: [activityTaskFromScheduled(scheduled)]
        });

        const expired = await backend.claimActivityTask("expired-activity-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.echo"],
          leaseDurationMs: 0
        });
        assert(expired !== null, "initial activity claim should be granted");

        const reclaimed = await backend.claimActivityTask("replacement-activity-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.echo"],
          leaseDurationMs: 30_000
        });
        assert(reclaimed !== null, "expired activity claim should be reclaimable");
        assert(
          reclaimed.claim.token !== expired.claim.token,
          "reclaimed activity task should receive a fresh claim token"
        );

        await assertRejects(
          () =>
            backend.completeActivity({
              claim: expired.claim,
              result: encodePayload({ value: 2 }, { codec: "Json" })
            }),
          "stale activity task lease"
        );

        const completed = await backend.completeActivity({
          claim: reclaimed.claim,
          result: encodePayload({ value: 2 }, { codec: "Json" })
        });
        assert(
          completed.kind === "Completed" && completed.eventId === eventId(3),
          "replacement activity claim should complete with the next workflow event"
        );
      }
    },
    {
      name: "batch activity completion reports ordered duplicate stale success and missing results",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const firstInput = encodePayload({ value: 1 }, { codec: "Json" });
        const secondInput = encodePayload({ value: 2 }, { codec: "Json" });
        const firstScheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.echo",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.none(),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: null,
          input: firstInput,
          fingerprint: activityFingerprint(
            "conformance.echo",
            payloadDigest(firstInput),
            "sha256:test-options"
          )
        };
        const secondScheduled = {
          commandId: commandId(claim.runId, 2),
          activityName: "conformance.echo",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.none(),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: null,
          input: secondInput,
          fingerprint: activityFingerprint(
            "conformance.echo",
            payloadDigest(secondInput),
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            { data: { kind: "ActivityScheduled", scheduled: firstScheduled } },
            { data: { kind: "ActivityScheduled", scheduled: secondScheduled } }
          ],
          scheduleActivities: [
            activityTaskFromScheduled(firstScheduled),
            activityTaskFromScheduled(secondScheduled)
          ]
        });

        const first = await backend.claimActivityTask("batch-worker-1", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.echo"],
          leaseDurationMs: 30_000
        });
        const second = await backend.claimActivityTask("batch-worker-2", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.echo"],
          leaseDurationMs: 30_000
        });
        assert(first !== null && second !== null, "two activities should be claimable");

        const firstCompleted = await backend.completeActivity({
          claim: first.claim,
          result: encodePayload({ value: 10 }, { codec: "Json" })
        });
        assert(
          firstCompleted.kind === "Completed" && firstCompleted.eventId === eventId(4),
          "first single completion should append event 4"
        );

        const batch = await backend.completeActivities({
          completions: [
            {
              claim: first.claim,
              result: encodePayload({ value: 10 }, { codec: "Json" })
            },
            {
              claim: {
                ...second.claim,
                token: second.claim.token + 1
              },
              result: encodePayload({ value: 20 }, { codec: "Json" })
            },
            {
              claim: second.claim,
              result: encodePayload({ value: 20 }, { codec: "Json" })
            },
            {
              claim: {
                activityId: "missing-activity",
                workerId: "missing-worker",
                token: 1
              },
              result: encodePayload({ value: 30 }, { codec: "Json" })
            }
          ]
        });

        assert(
          batch.results.map((result) => result.kind).join(",") ===
            "AlreadyCompleted,StaleLease,Completed,NotFound",
          "batch activity completion should preserve ordered per-item results"
        );
        const completed = batch.results[2];
        assert(
          completed?.kind === "Completed" && completed.eventId === eventId(5),
          "valid batch completion should append the next event"
        );

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          history.events.map((event) => event.eventType).join(",") ===
            "WorkflowStarted,ActivityScheduled,ActivityScheduled,ActivityCompleted,ActivityCompleted",
          "batch completion should only append successful completion events"
        );
      }
    },
    {
      name: "activity task failure appends a workflow wake event idempotently",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const input = encodePayload({ value: 1 }, { codec: "Json" });
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.fail",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.none(),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: null,
          input,
          fingerprint: activityFingerprint(
            "conformance.fail",
            payloadDigest(input),
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
          scheduleActivities: [activityTaskFromScheduled(scheduled)]
        });

        const claimedActivity = await backend.claimActivityTask("activity-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.fail"],
          leaseDurationMs: 30_000
        });
        assert(claimedActivity !== null, "matching activity should be claimable");

        const failure = {
          errorType: "test.activity",
          message: "activity failed",
          nonRetryable: false
        };
        const failed = await backend.failActivity({
          claim: claimedActivity.claim,
          failure
        });
        assert(
          failed.kind === "Failed" && failed.eventId === eventId(3),
          "activity failure should append ActivityFailed"
        );

        const duplicate = await backend.failActivity({
          claim: claimedActivity.claim,
          failure
        });
        assert(duplicate.kind === "AlreadyCompleted", "duplicate failure is idempotent");

        const workflowWake = await backend.claimWorkflowTask("worker-after-failure", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(workflowWake !== null, "activity failure should wake workflow");
        assert(
          workflowWake.reason === "ActivityFailed",
          "activity failure wake should preserve reason"
        );

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        const event = history.events.at(-1);
        assert(event?.data.kind === "ActivityFailed", "history should end in ActivityFailed");
        assert(
          event.data.failed.failure.message === "activity failed",
          "failure details should be durable"
        );
      }
    },
    {
      name: "retryable activity failure reschedules attempts before terminal failure",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const input = encodePayload({ value: 1 }, { codec: "Json" });
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.retry",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.exponential({
            initialIntervalMs: 0,
            maxIntervalMs: 0,
            maxAttempts: 2
          }),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: null,
          input,
          fingerprint: activityFingerprint(
            "conformance.retry",
            payloadDigest(input),
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
          scheduleActivities: [activityTaskFromScheduled(scheduled)]
        });

        const firstAttempt = await backend.claimActivityTask("activity-worker-1", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.retry"],
          leaseDurationMs: 30_000
        });
        assert(firstAttempt !== null, "first retryable activity attempt should be claimable");
        assert(firstAttempt.task.attempt === 1, "first retryable attempt should be attempt 1");

        const retry = await backend.failActivity({
          claim: firstAttempt.claim,
          failure: {
            errorType: "test.retryable",
            message: "retryable failure",
            nonRetryable: false
          }
        });
        assert(retry.kind === "RetryScheduled", "first retryable failure should schedule retry");
        assert(retry.attempt === 2, "retry should schedule attempt 2");

        const noWorkflowWake = await backend.claimWorkflowTask("worker-before-terminal-failure", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(noWorkflowWake === null, "retry scheduling should not wake workflow as failed");

        const secondAttempt = await backend.claimActivityTask("activity-worker-2", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.retry"],
          leaseDurationMs: 30_000
        });
        assert(secondAttempt !== null, "second retryable activity attempt should be claimable");
        assert(secondAttempt.task.attempt === 2, "second retryable attempt should be attempt 2");

        const terminal = await backend.failActivity({
          claim: secondAttempt.claim,
          failure: {
            errorType: "test.retryable",
            message: "terminal retry failure",
            nonRetryable: false
          }
        });
        assert(
          terminal.kind === "Failed" && terminal.eventId === eventId(3),
          "exhausted retry should append terminal ActivityFailed"
        );

        const workflowWake = await backend.claimWorkflowTask("worker-after-terminal-failure", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(workflowWake?.reason === "ActivityFailed", "terminal retry failure should wake workflow");

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          history.events.map((event) => event.eventType).join(",") ===
            "WorkflowStarted,ActivityScheduled,ActivityFailed",
          "retry attempts should not append intermediate failure events"
        );
      }
    },
    {
      name: "activity start-to-close timeout appends terminal timeout and fences completion",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const input = encodePayload({ value: 1 }, { codec: "Json" });
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.timeout",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.none(),
          startToCloseTimeoutMs: 0,
          heartbeatTimeoutMs: null,
          input,
          fingerprint: activityFingerprint(
            "conformance.timeout",
            payloadDigest(input),
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
          scheduleActivities: [activityTaskFromScheduled(scheduled)]
        });

        const activity = await backend.claimActivityTask("timeout-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.timeout"],
          leaseDurationMs: 30_000
        });
        assert(activity !== null, "timeout activity should be claimable before timeout maintenance");

        const early = await backend.timeoutDueActivities({
          namespace: namespace(),
          now: 0,
          limit: 8
        });
        assert(early.timedOut === 0, "activity should not time out before its start timestamp");

        const due = await backend.timeoutDueActivities({
          namespace: namespace(),
          now: Number.MAX_SAFE_INTEGER,
          limit: 8
        });
        assert(due.timedOut === 1, "due start-to-close timeout should append one terminal event");

        const duplicate = await backend.timeoutDueActivities({
          namespace: namespace(),
          now: Number.MAX_SAFE_INTEGER,
          limit: 8
        });
        assert(duplicate.timedOut === 0, "timeout maintenance should be idempotent");

        const late = await backend.completeActivity({
          claim: activity.claim,
          result: encodePayload({ ok: true }, { codec: "Json" })
        });
        assert(late.kind === "AlreadyCompleted", "late completion should be fenced after timeout");

        const workflowWake = await backend.claimWorkflowTask("worker-after-timeout", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(workflowWake?.reason === "ActivityTimedOut", "timeout should wake workflow");

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          history.events.map((event) => event.eventType).join(",") ===
            "WorkflowStarted,ActivityScheduled,ActivityTimedOut",
          "timeout should append compact terminal activity history"
        );
        const timedOut = history.events.at(-1);
        assert(timedOut?.data.kind === "ActivityTimedOut", "history should end in ActivityTimedOut");
        assert(
          timedOut.data.timedOut.message.includes("start-to-close timed out"),
          "timeout event should include a useful message"
        );
      }
    },
    {
      name: "activity start-to-close timeout retries before terminal workflow wake",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const input = encodePayload({ value: 1 }, { codec: "Json" });
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.timeout-retry",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.exponential({
            initialIntervalMs: 0,
            maxIntervalMs: 0,
            maxAttempts: 2,
            backoffCoefficient: 1
          }),
          startToCloseTimeoutMs: 0,
          heartbeatTimeoutMs: null,
          input,
          fingerprint: activityFingerprint(
            "conformance.timeout-retry",
            payloadDigest(input),
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
          scheduleActivities: [activityTaskFromScheduled(scheduled)]
        });

        const first = await backend.claimActivityTask("timeout-retry-worker-1", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.timeout-retry"],
          leaseDurationMs: 30_000
        });
        assert(first !== null, "first timeout retry attempt should be claimable");
        assert(first.task.attempt === 1, "first attempt should be attempt 1");

        const firstTimeout = await backend.timeoutDueActivities({
          namespace: namespace(),
          now: Date.now(),
          limit: 8
        });
        assert(firstTimeout.timedOut === 1, "first start-to-close timeout should be processed");

        const prematureWake = await backend.claimWorkflowTask("timeout-retry-premature", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(prematureWake === null, "retryable start-to-close timeout should not wake workflow early");

        const second = await backend.claimActivityTask("timeout-retry-worker-2", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.timeout-retry"],
          leaseDurationMs: 30_000
        });
        assert(second !== null, "second timeout retry attempt should be claimable");
        assert(second.task.attempt === 2, "second attempt should increment attempt");

        const secondTimeout = await backend.timeoutDueActivities({
          namespace: namespace(),
          now: Date.now(),
          limit: 8
        });
        assert(secondTimeout.timedOut === 1, "exhausted start-to-close timeout should be terminal");

        const workflowWake = await backend.claimWorkflowTask("timeout-retry-ready", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(workflowWake?.reason === "ActivityTimedOut", "exhausted timeout should wake workflow");

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          history.events.map((event) => event.eventType).join(",") ===
            "WorkflowStarted,ActivityScheduled,ActivityTimedOut",
          "retryable start-to-close timeout should append only terminal activity history"
        );
        const timedOut = history.events.at(-1);
        assert(timedOut?.data.kind === "ActivityTimedOut", "history should end in timeout");
        assert(
          timedOut.data.timedOut.message.includes("start-to-close timed out"),
          "terminal start-to-close timeout should report timeout kind"
        );
      }
    },
    {
      name: "activity heartbeat records liveness and terminal missed heartbeat",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const input = encodePayload({ value: 1 }, { codec: "Json" });
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.heartbeat",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.none(),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: 0,
          input,
          fingerprint: activityFingerprint(
            "conformance.heartbeat",
            payloadDigest(input),
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
          scheduleActivities: [activityTaskFromScheduled(scheduled)]
        });

        const activity = await backend.claimActivityTask("heartbeat-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.heartbeat"],
          leaseDurationMs: 30_000
        });
        assert(activity !== null, "heartbeat activity should be claimable");

        const recorded = await backend.heartbeatActivity({ claim: activity.claim });
        assert(recorded.kind === "Recorded", "heartbeat should record for current claim");
        await assertRejects(
          () =>
            backend.heartbeatActivity({
              claim: { ...activity.claim, token: activity.claim.token + 1 }
            }),
          "stale activity task lease"
        );

        const due = await backend.timeoutDueActivities({
          namespace: namespace(),
          now: Date.now(),
          limit: 8
        });
        assert(due.timedOut === 1, "missed heartbeat should time out the activity");

        const duplicate = await backend.timeoutDueActivities({
          namespace: namespace(),
          now: Date.now(),
          limit: 8
        });
        assert(duplicate.timedOut === 0, "heartbeat timeout should be idempotent");

        const lateHeartbeat = await backend.heartbeatActivity({ claim: activity.claim });
        assert(lateHeartbeat.kind === "AlreadyCompleted", "late heartbeat should be terminal");

        const workflowWake = await backend.claimWorkflowTask("worker-after-heartbeat-timeout", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(workflowWake?.reason === "ActivityTimedOut", "heartbeat timeout should wake workflow");

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        const timedOut = history.events.at(-1);
        assert(timedOut?.data.kind === "ActivityTimedOut", "history should end in ActivityTimedOut");
        assert(
          timedOut.data.timedOut.message.includes("missed heartbeat"),
          "heartbeat timeout event should include a useful message"
        );
      }
    },
    {
      name: "activity heartbeat timeout retries before terminal workflow wake",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const input = encodePayload({ value: 1 }, { codec: "Json" });
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.heartbeat-retry",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.exponential({
            initialIntervalMs: 0,
            maxIntervalMs: 0,
            maxAttempts: 2,
            backoffCoefficient: 1
          }),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: 0,
          input,
          fingerprint: activityFingerprint(
            "conformance.heartbeat-retry",
            payloadDigest(input),
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
          scheduleActivities: [activityTaskFromScheduled(scheduled)]
        });

        const first = await backend.claimActivityTask("heartbeat-retry-worker-1", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.heartbeat-retry"],
          leaseDurationMs: 30_000
        });
        assert(first !== null, "first heartbeat retry attempt should be claimable");
        assert(first.task.attempt === 1, "first attempt should be attempt 1");

        const firstTimeout = await backend.timeoutDueActivities({
          namespace: namespace(),
          now: Date.now(),
          limit: 8
        });
        assert(firstTimeout.timedOut === 1, "first missed heartbeat should be processed");

        const prematureWake = await backend.claimWorkflowTask("heartbeat-retry-premature", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(prematureWake === null, "retryable timeout should not wake workflow early");

        const second = await backend.claimActivityTask("heartbeat-retry-worker-2", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.heartbeat-retry"],
          leaseDurationMs: 30_000
        });
        assert(second !== null, "second heartbeat retry attempt should be claimable");
        assert(second.task.attempt === 2, "second attempt should increment attempt");

        const secondTimeout = await backend.timeoutDueActivities({
          namespace: namespace(),
          now: Date.now(),
          limit: 8
        });
        assert(secondTimeout.timedOut === 1, "exhausted missed heartbeat should be terminal");

        const workflowWake = await backend.claimWorkflowTask("heartbeat-retry-ready", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(workflowWake?.reason === "ActivityTimedOut", "exhausted timeout should wake workflow");

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          history.events.map((event) => event.eventType).join(",") ===
            "WorkflowStarted,ActivityScheduled,ActivityTimedOut",
          "retryable heartbeat timeout should append only terminal activity history"
        );
        const timedOut = history.events.at(-1);
        assert(timedOut?.data.kind === "ActivityTimedOut", "history should end in timeout");
        assert(
          timedOut.data.timedOut.message.includes("attempt 2"),
          "terminal heartbeat timeout should report exhausted attempt"
        );
      }
    },
    {
      name: "activity map materializes bounded items and writes ordered result manifest",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const inputManifest = activityMapManifest(
          [{ value: 1 }, { value: 2 }, { value: 3 }],
          2
        );
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.map",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.none(),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: null,
          inputManifest,
          resultManifestName: "mapped",
          maxInFlight: 2,
          fingerprint: activityMapFingerprint(
            "conformance.map",
            payloadDigest(inputManifest),
            "mapped",
            2,
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityMapScheduled", scheduled } }],
          scheduleActivityMaps: [
            {
              mapCommandId: scheduled.commandId,
              activityName: scheduled.activityName,
              taskQueue: scheduled.taskQueue,
              retryPolicy: scheduled.retryPolicy,
              startToCloseTimeoutMs: scheduled.startToCloseTimeoutMs,
              heartbeatTimeoutMs: scheduled.heartbeatTimeoutMs,
              inputManifest: scheduled.inputManifest,
              resultManifestName: scheduled.resultManifestName,
              maxInFlight: scheduled.maxInFlight
            }
          ]
        });

        const first = await backend.claimActivityTask("map-worker-1", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.map"],
          leaseDurationMs: 30_000
        });
        const second = await backend.claimActivityTask("map-worker-2", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.map"],
          leaseDurationMs: 30_000
        });
        const blockedThird = await backend.claimActivityTask("map-worker-3", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.map"],
          leaseDurationMs: 30_000
        });
        assert(first !== null && second !== null, "first two map items should materialize");
        assert(blockedThird === null, "maxInFlight should bound materialized map items");
        assert(first.task.mapItem?.itemOrdinal === 0, "first item ordinal should be 0");
        assert(second.task.mapItem?.itemOrdinal === 1, "second item ordinal should be 1");

        await backend.completeActivity({
          claim: first.claim,
          result: encodePayload({ value: 10 }, { codec: "Json" })
        });
        const third = await backend.claimActivityTask("map-worker-3", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.map"],
          leaseDurationMs: 30_000
        });
        assert(third !== null, "completing one map item should materialize the next");
        assert(third.task.mapItem?.itemOrdinal === 2, "third item ordinal should be 2");

        await backend.completeActivity({
          claim: second.claim,
          result: encodePayload({ value: 20 }, { codec: "Json" })
        });
        await backend.completeActivity({
          claim: third.claim,
          result: encodePayload({ value: 30 }, { codec: "Json" })
        });

        const parentReady = await backend.claimWorkflowTask("map-parent-ready", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(parentReady !== null, "completed activity map should wake parent");
        assert(
          parentReady.reason === "ActivityMapCompleted",
          "activity map wake should preserve reason"
        );
        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          history.events.map((event) => event.eventType).join(",") ===
            "WorkflowStarted,ActivityMapScheduled,ActivityMapCompleted",
          "parent history should stay compact for activity map"
        );
        const completed = history.events.at(-1)?.data;
        assert(completed?.kind === "ActivityMapCompleted", "expected ActivityMapCompleted");
        const resultManifest = decodePayload<ActivityMapResultManifest<{ readonly value: number }>>(
          completed.completed.resultManifest as PayloadRef<ActivityMapResultManifest<{ readonly value: number }>>
        );
        const values: number[] = [];
        for (const pageRef of resultManifest.pages) {
          const page = decodePayload<ActivityMapResultPage<{ readonly value: number }>>(
            pageRef as PayloadRef<ActivityMapResultPage<{ readonly value: number }>>
          );
          for (const resultRef of page.results) {
            values.push(
              decodePayload<{ readonly value: number }>(
                resultRef as PayloadRef<{ readonly value: number }>
              ).value
            );
          }
        }
        assert(values.join(",") === "10,20,30", "result manifest should preserve ordinal order");
      }
    },
    {
      name: "activity-map retryable item failure retries before terminal map failure",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const inputManifest = activityMapManifest([{ value: 1 }, { value: 2 }], 2);
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.map-retry",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.exponential({
            initialIntervalMs: 0,
            maxIntervalMs: 0,
            maxAttempts: 2
          }),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: null,
          inputManifest,
          resultManifestName: "map-retry",
          maxInFlight: 1,
          fingerprint: activityMapFingerprint(
            "conformance.map-retry",
            payloadDigest(inputManifest),
            "map-retry",
            1,
            "sha256:test-options"
          )
        };
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityMapScheduled", scheduled } }],
          scheduleActivityMaps: [
            {
              mapCommandId: scheduled.commandId,
              activityName: scheduled.activityName,
              taskQueue: scheduled.taskQueue,
              retryPolicy: scheduled.retryPolicy,
              startToCloseTimeoutMs: scheduled.startToCloseTimeoutMs,
              heartbeatTimeoutMs: scheduled.heartbeatTimeoutMs,
              inputManifest: scheduled.inputManifest,
              resultManifestName: scheduled.resultManifestName,
              maxInFlight: scheduled.maxInFlight
            }
          ]
        });

        const firstAttempt = await backend.claimActivityTask("map-retry-worker-1", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.map-retry"],
          leaseDurationMs: 30_000
        });
        assert(firstAttempt !== null, "first map retry attempt should be claimable");
        assert(firstAttempt.task.mapItem?.itemOrdinal === 0, "first map retry ordinal should be 0");
        assert(firstAttempt.task.attempt === 1, "first map retry attempt should be attempt 1");

        const retry = await backend.failActivity({
          claim: firstAttempt.claim,
          failure: {
            errorType: "test.map.retryable",
            message: "retryable map failure",
            nonRetryable: false
          }
        });
        assert(retry.kind === "RetryScheduled", "retryable map item should schedule retry");
        assert(retry.attempt === 2, "map retry should schedule attempt 2");

        const noWorkflowWake = await backend.claimWorkflowTask("worker-before-map-terminal-failure", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(noWorkflowWake === null, "map item retry should not wake parent as failed");

        const secondAttempt = await backend.claimActivityTask("map-retry-worker-2", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.map-retry"],
          leaseDurationMs: 30_000
        });
        assert(secondAttempt !== null, "second map retry attempt should be claimable");
        assert(
          secondAttempt.task.mapItem?.itemOrdinal === 0,
          "retry should keep the same map item ordinal in flight"
        );
        assert(secondAttempt.task.attempt === 2, "second map retry attempt should be attempt 2");

        const terminal = await backend.failActivity({
          claim: secondAttempt.claim,
          failure: {
            errorType: "test.map.retryable",
            message: "terminal map failure",
            nonRetryable: false
          }
        });
        assert(
          terminal.kind === "Failed" && terminal.eventId === eventId(3),
          "exhausted map item retry should append ActivityMapFailed"
        );

        const workflowWake = await backend.claimWorkflowTask("worker-after-map-terminal-failure", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(workflowWake?.reason === "ActivityMapFailed", "terminal map failure should wake parent");

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          history.events.map((event) => event.eventType).join(",") ===
            "WorkflowStarted,ActivityMapScheduled,ActivityMapFailed",
          "map retries should not append intermediate parent failure events"
        );
      }
    },
    {
      name: "timer waits fire only when due and wake the workflow",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const timerCommand = commandId(claim.runId, 1);
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            {
              data: {
                kind: "TimerStarted",
                started: {
                  commandId: timerCommand,
                  fireAt: timestampMs(1_000),
                  fingerprint: timerFingerprint("sleep_until", timestampMs(1_000))
                }
              }
            }
          ],
          upsertWaits: [
            {
              waitId: waitId(`${claim.runId}:timer:1`),
              runId: claim.runId,
              commandId: timerCommand,
              kind: "Timer",
              key: "timer",
              readyAt: timestampMs(1_000)
            }
          ]
        });

        const early = await backend.fireDueTimers({
          namespace: namespace(),
          now: timestampMs(999),
          limit: 16
        });
        assert(early.fired === 0, "timer should not fire before readyAt");

        const due = await backend.fireDueTimers({
          namespace: namespace(),
          now: timestampMs(1_000),
          limit: 16
        });
        assert(due.fired === 1, "due timer should fire exactly once");

        const duplicate = await backend.fireDueTimers({
          namespace: namespace(),
          now: timestampMs(1_000),
          limit: 16
        });
        assert(duplicate.fired === 0, "fired timer wait should be removed");

        const workflowWake = await backend.claimWorkflowTask("worker-after-timer", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(workflowWake !== null, "timer fire should wake workflow");
        assert(workflowWake.reason === "TimerFired", "timer wake should preserve reason");
        assert(workflowWake.replayTargetEventId === eventId(3), "timer fire should append event");
      }
    },
    {
      name: "signal send is idempotent and signal consumption is atomic with workflow commit",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const signalCommand = commandId(claim.runId, 1);
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          upsertWaits: [
            {
              waitId: waitId(`${claim.runId}:signal:1`),
              runId: claim.runId,
              commandId: signalCommand,
              kind: "Signal",
              key: "approved",
              readyAt: null
            }
          ]
        });

        const payload = encodePayload({ approvalId: "a-1" }, { codec: "Json" });
        const accepted = await backend.signalWorkflow({
          namespace: namespace(),
          workflowId: workflowId("wf/commit"),
          signalId: signalId("sig-conformance-1"),
          signalName: "approved",
          payload
        });
        assert(accepted.kind === "Accepted", "first signal send should be accepted");

        const duplicate = await backend.signalWorkflow({
          namespace: namespace(),
          workflowId: workflowId("wf/commit"),
          signalId: signalId("sig-conformance-1"),
          signalName: "approved",
          payload
        });
        assert(duplicate.kind === "Duplicate", "duplicate signal id should be idempotent");

        const workflowWake = await backend.claimWorkflowTask("worker-after-signal", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(workflowWake !== null, "signal should wake workflow with matching wait");
        assert(workflowWake.reason === "SignalReceived", "signal wake should preserve reason");

        const inbox = await backend.readSignalInbox({
          runId: workflowWake.runId,
          signalName: "approved"
        });
        assert(inbox !== null, "readSignalInbox should return unconsumed signal");

        await backend.commitWorkflowTask(workflowWake.claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            {
              data: {
                kind: "SignalConsumed",
                consumed: {
                  commandId: signalCommand,
                  signalId: inbox.signalId,
                  signalName: inbox.signalName,
                  payload: inbox.payload,
                  fingerprint: signalFingerprint("approved")
                }
              }
            }
          ],
          consumeSignals: [inbox.signalId],
          deleteWaits: [waitId(`${workflowWake.runId}:signal:1`)]
        });

        const consumed = await backend.readSignalInbox({
          runId: workflowWake.runId,
          signalName: "approved"
        });
        assert(consumed === null, "committed signal consumption should remove inbox visibility");
      }
    },
    {
      name: "workflow commit publishes the latest query projection atomically",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);

        const before = await backend.queryWorkflow({
          namespace: namespace(),
          workflowId: workflowId("wf/commit")
        });
        assert(before.kind === "NoProjection", "new workflow should not have a query projection");

        const projection = encodePayload({ status: "running" }, { codec: "Json" });
        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          queryProjection: projection
        });

        const after = await backend.queryWorkflow({
          namespace: namespace(),
          workflowId: workflowId("wf/commit")
        });
        assert(after.kind === "Found", "committed query projection should be readable");
        assert(
          decodePayload<{ readonly status: string }>(
            after.projection as PayloadRef<{ readonly status: string }>
          ).status === "running",
          "query projection payload should round-trip"
        );
      }
    },
    {
      name: "payload roots include durable history, queue, signal, and projection refs",
      async run(factory) {
        const backend = factory();
        const workflowInput = encodePayload({ value: "workflow-input" }, { codec: "Json" });
        await backend.startWorkflow({
          namespace: namespace(),
          workflowId: workflowId("wf/payload-roots"),
          workflowType: workflowType("conformance.workflow", 1),
          taskQueue: taskQueue("workflows"),
          input: workflowInput
        });
        const claimed = await backend.claimWorkflowTask("worker-a", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(claimed !== null, "started workflow should be claimable");

        const activityInput = encodePayload({ value: "activity-input" }, { codec: "Json" });
        const scheduled = {
          commandId: commandId(claimed.runId, 1),
          activityName: "conformance.roots",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.none(),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: null,
          input: activityInput,
          fingerprint: activityFingerprint(
            "conformance.roots",
            payloadDigest(activityInput),
            "sha256:test-options"
          )
        };
        const projection = encodePayload({ status: "projected" }, { codec: "Json" });
        await backend.commitWorkflowTask(claimed.claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
          scheduleActivities: [activityTaskFromScheduled(scheduled)],
          queryProjection: projection
        });
        const signalPayload = encodePayload({ value: "signal" }, { codec: "Json" });
        await backend.signalWorkflow({
          namespace: namespace(),
          workflowId: workflowId("wf/payload-roots"),
          signalId: signalId("roots-signal"),
          signalName: "roots",
          payload: signalPayload
        });

        const rootDigests = new Set(collectRootPayloadRefs(await backend.payloadRoots()).map(rootPayloadDigest));
        for (const payload of [workflowInput, activityInput, projection, signalPayload]) {
          assert(
            rootDigests.has(rootPayloadDigest(payload)),
            "payload roots should include committed durable payload refs"
          );
        }
      }
    },
    {
      name: "payload roots include provider-owned activity-map item refs",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const inputManifest = activityMapManifest(
          [{ value: 1 }, { value: 2 }],
          2
        );
        const inputRefs = activityMapManifestItemRefs(inputManifest);
        assert(inputRefs.length === 2, "activity-map input manifest should expose two item refs");
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          activityName: "conformance.roots.activity-map",
          taskQueue: "activities",
          retryPolicy: RetryPolicy.none(),
          startToCloseTimeoutMs: null,
          heartbeatTimeoutMs: null,
          inputManifest,
          resultManifestName: "activity-rooted",
          maxInFlight: 1,
          fingerprint: activityMapFingerprint(
            "conformance.roots.activity-map",
            payloadDigest(inputManifest),
            "activity-rooted",
            1,
            "sha256:test-options"
          )
        };

        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ActivityMapScheduled", scheduled } }],
          scheduleActivityMaps: [
            {
              mapCommandId: scheduled.commandId,
              activityName: scheduled.activityName,
              taskQueue: scheduled.taskQueue,
              retryPolicy: scheduled.retryPolicy,
              startToCloseTimeoutMs: scheduled.startToCloseTimeoutMs,
              heartbeatTimeoutMs: scheduled.heartbeatTimeoutMs,
              inputManifest: scheduled.inputManifest,
              resultManifestName: scheduled.resultManifestName,
              maxInFlight: scheduled.maxInFlight
            }
          ]
        });

        const rootsBeforeCompletion = new Set(
          collectRootPayloadRefs(await backend.payloadRoots()).map(rootPayloadDigest)
        );
        assert(
          rootsBeforeCompletion.has(rootPayloadDigest(inputRefs[1] as PayloadRef)),
          "payload roots should include unmaterialized activity-map item inputs"
        );

        const firstItem = await backend.claimActivityTask("activity-map-root-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("activities"),
          registeredActivityNames: ["conformance.roots.activity-map"],
          leaseDurationMs: 30_000
        });
        assert(firstItem !== null, "first activity-map item should be claimable");
        const result = encodePayload({ value: 10 }, { codec: "Json" });
        await backend.completeActivity({ claim: firstItem.claim, result });

        const rootsAfterCompletion = new Set(
          collectRootPayloadRefs(await backend.payloadRoots()).map(rootPayloadDigest)
        );
        assert(
          rootsAfterCompletion.has(rootPayloadDigest(result)),
          "payload roots should include in-progress activity-map item results"
        );
      }
    },
    {
      name: "payload roots include provider-owned child-workflow-map item refs",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const inputManifest = activityMapManifest(
          [{ value: 1 }, { value: 2 }],
          2
        );
        const inputRefs = activityMapManifestItemRefs(inputManifest);
        assert(inputRefs.length === 2, "child-workflow-map input manifest should expose two item refs");
        const childType = workflowType("conformance.roots.child-map-child", 1);
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          workflowType: childType,
          taskQueue: "child-workflows",
          inputManifest,
          resultManifestName: "child-rooted",
          workflowIdPrefix: "wf/child-map-roots",
          maxInFlight: 1,
          parentClosePolicy: "Cancel" as const,
          failureMode: "CollectAll" as const,
          fingerprint: childWorkflowMapFingerprint(
            childType,
            payloadDigest(inputManifest),
            "child-rooted",
            "wf/child-map-roots",
            1,
            "child-workflows",
            "Cancel",
            "CollectAll"
          )
        };

        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ChildWorkflowMapScheduled", scheduled } }],
          scheduleChildWorkflowMaps: [
            {
              mapCommandId: scheduled.commandId,
              workflowType: scheduled.workflowType,
              taskQueue: scheduled.taskQueue,
              inputManifest: scheduled.inputManifest,
              resultManifestName: scheduled.resultManifestName,
              workflowIdPrefix: scheduled.workflowIdPrefix,
              maxInFlight: scheduled.maxInFlight,
              parentClosePolicy: scheduled.parentClosePolicy,
              failureMode: scheduled.failureMode
            }
          ]
        });

        const roots = new Set(
          collectRootPayloadRefs(await backend.payloadRoots()).map(rootPayloadDigest)
        );
        assert(
          roots.has(rootPayloadDigest(inputRefs[1] as PayloadRef)),
          "payload roots should include unmaterialized child-workflow-map item inputs"
        );

        const firstChild = await backend.claimWorkflowTask("child-map-root-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("child-workflows"),
          registeredWorkflowTypes: [childType],
          leaseDurationMs: 30_000
        });
        assert(firstChild !== null, "first child-workflow-map item should be claimable");
        const result = encodePayload({ value: 10 }, { codec: "Json" });
        await backend.commitWorkflowTask(firstChild.claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "WorkflowCompleted", result } }]
        });

        const rootsAfterCompletion = new Set(
          collectRootPayloadRefs(await backend.payloadRoots()).map(rootPayloadDigest)
        );
        assert(
          rootsAfterCompletion.has(rootPayloadDigest(result)),
          "payload roots should include in-progress child-workflow-map item results"
        );
      }
    },
    {
      name: "child workflow start is durable and wakes parent while making child claimable",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const childInput = encodePayload({ value: 42 }, { codec: "Json" });
        const childType = workflowType("conformance.child", 1);
        const requested = {
          commandId: commandId(claim.runId, 1),
          workflowType: childType,
          workflowId: workflowId("wf/child"),
          taskQueue: "child-workflows",
          input: childInput,
          parentClosePolicy: "Cancel" as const,
          fingerprint: childWorkflowFingerprint(
            childType,
            workflowId("wf/child"),
            payloadDigest(childInput),
            "child-workflows",
            "Cancel"
          )
        };

        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ChildWorkflowStartRequested", requested } }],
          startChildWorkflows: [requested]
        });

        const parentReady = await backend.claimWorkflowTask("parent-after-child-start", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(parentReady !== null, "child start should wake parent");
        assert(
          parentReady.reason === "ChildWorkflowStarted",
          "parent wake should preserve child-start reason"
        );

        const childReady = await backend.claimWorkflowTask("child-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("child-workflows"),
          registeredWorkflowTypes: [childType],
          leaseDurationMs: 30_000
        });
        assert(childReady !== null, "child workflow should be claimable");
        assert(childReady.workflowId === workflowId("wf/child"), "child workflow id should match");
        const childStarted = childReady.prefetchedHistory[0]?.data;
        assert(childStarted?.kind === "WorkflowStarted", "child history should start workflow");
        assert(
          decodePayload<{ readonly value: number }>(childStarted.input as PayloadRef<{ readonly value: number }>).value === 42,
          "child input should round-trip"
        );

        const parentHistory = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          parentHistory.events.some((event) => event.data.kind === "ChildWorkflowStarted"),
          "parent history should include ChildWorkflowStarted"
        );
      }
    },
    {
      name: "child workflow map materializes bounded children and writes ordered outcome manifest",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const inputManifest = activityMapManifest(
          [{ value: 1 }, { value: 2 }, { value: 3 }],
          2
        );
        const childType = workflowType("conformance.child-map-child", 1);
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          workflowType: childType,
          taskQueue: "child-workflows",
          inputManifest,
          resultManifestName: "child-mapped",
          workflowIdPrefix: "wf/child-map",
          maxInFlight: 2,
          parentClosePolicy: "Cancel" as const,
          failureMode: "CollectAll" as const,
          fingerprint: childWorkflowMapFingerprint(
            childType,
            payloadDigest(inputManifest),
            "child-mapped",
            "wf/child-map",
            2,
            "child-workflows",
            "Cancel",
            "CollectAll"
          )
        };

        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ChildWorkflowMapScheduled", scheduled } }],
          scheduleChildWorkflowMaps: [
            {
              mapCommandId: scheduled.commandId,
              workflowType: scheduled.workflowType,
              taskQueue: scheduled.taskQueue,
              inputManifest: scheduled.inputManifest,
              resultManifestName: scheduled.resultManifestName,
              workflowIdPrefix: scheduled.workflowIdPrefix,
              maxInFlight: scheduled.maxInFlight,
              parentClosePolicy: scheduled.parentClosePolicy,
              failureMode: scheduled.failureMode
            }
          ]
        });

        const first = await backend.claimWorkflowTask("child-map-worker-1", {
          namespace: namespace(),
          taskQueue: taskQueue("child-workflows"),
          registeredWorkflowTypes: [childType],
          leaseDurationMs: 30_000
        });
        const second = await backend.claimWorkflowTask("child-map-worker-2", {
          namespace: namespace(),
          taskQueue: taskQueue("child-workflows"),
          registeredWorkflowTypes: [childType],
          leaseDurationMs: 30_000
        });
        const blockedThird = await backend.claimWorkflowTask("child-map-worker-3", {
          namespace: namespace(),
          taskQueue: taskQueue("child-workflows"),
          registeredWorkflowTypes: [childType],
          leaseDurationMs: 30_000
        });
        assert(first !== null && second !== null, "first two child map items should materialize");
        assert(blockedThird === null, "maxInFlight should bound child map materialization");
        assert(first.workflowId === workflowId("wf/child-map/0"), "first child id should be ordinal 0");
        assert(second.workflowId === workflowId("wf/child-map/1"), "second child id should be ordinal 1");

        await backend.commitWorkflowTask(first.claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            {
              data: {
                kind: "WorkflowCompleted",
                result: encodePayload({ value: 10 }, { codec: "Json" })
              }
            }
          ]
        });

        const third = await backend.claimWorkflowTask("child-map-worker-3", {
          namespace: namespace(),
          taskQueue: taskQueue("child-workflows"),
          registeredWorkflowTypes: [childType],
          leaseDurationMs: 30_000
        });
        assert(third !== null, "completing one child map item should materialize the next");
        assert(third.workflowId === workflowId("wf/child-map/2"), "third child id should be ordinal 2");

        await backend.commitWorkflowTask(second.claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            {
              data: {
                kind: "WorkflowCompleted",
                result: encodePayload({ value: 20 }, { codec: "Json" })
              }
            }
          ]
        });
        await backend.commitWorkflowTask(third.claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            {
              data: {
                kind: "WorkflowCompleted",
                result: encodePayload({ value: 30 }, { codec: "Json" })
              }
            }
          ]
        });

        const parentReady = await backend.claimWorkflowTask("child-map-parent-ready", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(parentReady !== null, "completed child workflow map should wake parent");
        assert(
          parentReady.reason === "ChildWorkflowMapCompleted",
          "child workflow map wake should preserve reason"
        );

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          history.events.map((event) => event.eventType).join(",") ===
            "WorkflowStarted,ChildWorkflowMapScheduled,ChildWorkflowMapCompleted",
          "parent history should stay compact for child workflow map"
        );
        const completed = history.events.at(-1)?.data;
        assert(completed?.kind === "ChildWorkflowMapCompleted", "expected ChildWorkflowMapCompleted");
        const resultManifest = decodePayload<ChildWorkflowMapResultManifest<{ readonly value: number }>>(
          completed.completed.resultManifest as PayloadRef<ChildWorkflowMapResultManifest<{ readonly value: number }>>
        );
        const values: number[] = [];
        for (const pageRef of resultManifest.pages) {
          const page = decodePayload<ChildWorkflowMapResultPage<{ readonly value: number }>>(
            pageRef as PayloadRef<ChildWorkflowMapResultPage<{ readonly value: number }>>
          );
          for (const outcome of page.outcomes) {
            assert(outcome.kind === "Succeeded", "all conformance child map items should succeed");
            values.push(
              decodePayload<{ readonly value: number }>(
                outcome.result as PayloadRef<{ readonly value: number }>
              ).value
            );
          }
        }
        assert(values.join(",") === "10,20,30", "outcome manifest should preserve ordinal order");
      }
    },
    {
      name: "child workflow map collect-all records item failures and still completes",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const childType = workflowType("conformance.child-map-collect", 1);
        await backend.startWorkflow({
          namespace: namespace(),
          workflowId: workflowId("wf/child-map-collect/0"),
          workflowType: childType,
          taskQueue: taskQueue("other-child-workflows"),
          input: encodePayload({ value: 0 }, { codec: "Json" })
        });

        const inputManifest = activityMapManifest(
          [{ value: 1 }, { value: 2 }],
          2
        );
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          workflowType: childType,
          taskQueue: "child-workflows",
          inputManifest,
          resultManifestName: "child-collect",
          workflowIdPrefix: "wf/child-map-collect",
          maxInFlight: 1,
          parentClosePolicy: "Cancel" as const,
          failureMode: "CollectAll" as const,
          fingerprint: childWorkflowMapFingerprint(
            childType,
            payloadDigest(inputManifest),
            "child-collect",
            "wf/child-map-collect",
            1,
            "child-workflows",
            "Cancel",
            "CollectAll"
          )
        };

        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ChildWorkflowMapScheduled", scheduled } }],
          scheduleChildWorkflowMaps: [
            {
              mapCommandId: scheduled.commandId,
              workflowType: scheduled.workflowType,
              taskQueue: scheduled.taskQueue,
              inputManifest: scheduled.inputManifest,
              resultManifestName: scheduled.resultManifestName,
              workflowIdPrefix: scheduled.workflowIdPrefix,
              maxInFlight: scheduled.maxInFlight,
              parentClosePolicy: scheduled.parentClosePolicy,
              failureMode: scheduled.failureMode
            }
          ]
        });

        const second = await backend.claimWorkflowTask("child-map-collect-worker", {
          namespace: namespace(),
          taskQueue: taskQueue("child-workflows"),
          registeredWorkflowTypes: [childType],
          leaseDurationMs: 30_000
        });
        assert(second !== null, "collect-all should materialize the non-conflicting item");
        assert(second.workflowId === workflowId("wf/child-map-collect/1"), "second child id should be ordinal 1");

        await backend.commitWorkflowTask(second.claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            {
              data: {
                kind: "WorkflowCompleted",
                result: encodePayload({ value: 20 }, { codec: "Json" })
              }
            }
          ]
        });

        const parentReady = await backend.claimWorkflowTask("child-map-collect-parent", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(parentReady !== null, "collect-all map should wake parent on completion");
        assert(parentReady.reason === "ChildWorkflowMapCompleted", "collect-all failures complete the map");

        const history = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        const completed = history.events.at(-1)?.data;
        assert(completed?.kind === "ChildWorkflowMapCompleted", "expected ChildWorkflowMapCompleted");
        assert(completed.completed.failureCount === 1, "one collect-all item should fail");
        assert(completed.completed.successCount === 1, "one collect-all item should succeed");
        const resultManifest = decodePayload<ChildWorkflowMapResultManifest<{ readonly value: number }>>(
          completed.completed.resultManifest as PayloadRef<ChildWorkflowMapResultManifest<{ readonly value: number }>>
        );
        const pageRef = resultManifest.pages[0];
        assert(pageRef !== undefined, "collect-all manifest should contain a page");
        const page = decodePayload<ChildWorkflowMapResultPage<{ readonly value: number }>>(
          pageRef as PayloadRef<ChildWorkflowMapResultPage<{ readonly value: number }>>
        );
        assert(page.outcomes[0]?.kind === "Failed", "first collect-all outcome should be failed");
        assert(page.outcomes[1]?.kind === "Succeeded", "second collect-all outcome should succeed");
      }
    },
    {
      name: "child workflow map fail-fast fails parent and cancels running siblings",
      async run(factory) {
        const { backend, claim } = await startedAndClaimed(factory);
        const inputManifest = activityMapManifest(
          [{ value: 1 }, { value: 2 }],
          2
        );
        const childType = workflowType("conformance.child-map-failfast", 1);
        const scheduled = {
          commandId: commandId(claim.runId, 1),
          workflowType: childType,
          taskQueue: "child-workflows",
          inputManifest,
          resultManifestName: "child-failfast",
          workflowIdPrefix: "wf/child-map-failfast",
          maxInFlight: 2,
          parentClosePolicy: "Cancel" as const,
          failureMode: "FailFast" as const,
          fingerprint: childWorkflowMapFingerprint(
            childType,
            payloadDigest(inputManifest),
            "child-failfast",
            "wf/child-map-failfast",
            2,
            "child-workflows",
            "Cancel",
            "FailFast"
          )
        };

        await backend.commitWorkflowTask(claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [{ data: { kind: "ChildWorkflowMapScheduled", scheduled } }],
          scheduleChildWorkflowMaps: [
            {
              mapCommandId: scheduled.commandId,
              workflowType: scheduled.workflowType,
              taskQueue: scheduled.taskQueue,
              inputManifest: scheduled.inputManifest,
              resultManifestName: scheduled.resultManifestName,
              workflowIdPrefix: scheduled.workflowIdPrefix,
              maxInFlight: scheduled.maxInFlight,
              parentClosePolicy: scheduled.parentClosePolicy,
              failureMode: scheduled.failureMode
            }
          ]
        });

        const first = await backend.claimWorkflowTask("child-map-failfast-1", {
          namespace: namespace(),
          taskQueue: taskQueue("child-workflows"),
          registeredWorkflowTypes: [childType],
          leaseDurationMs: 30_000
        });
        const second = await backend.claimWorkflowTask("child-map-failfast-2", {
          namespace: namespace(),
          taskQueue: taskQueue("child-workflows"),
          registeredWorkflowTypes: [childType],
          leaseDurationMs: 30_000
        });
        assert(first !== null && second !== null, "fail-fast should materialize both initial children");

        await backend.commitWorkflowTask(first.claim, {
          expectedTailEventId: eventId(1),
          appendEvents: [
            {
              data: {
                kind: "WorkflowFailed",
                failure: {
                  errorType: "test.child",
                  message: "child failed",
                  nonRetryable: true
                }
              }
            }
          ]
        });

        const parentReady = await backend.claimWorkflowTask("child-map-failfast-parent", {
          namespace: namespace(),
          taskQueue: taskQueue("workflows"),
          registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
          leaseDurationMs: 30_000
        });
        assert(parentReady !== null, "fail-fast child map should wake parent");
        assert(parentReady.reason === "ChildWorkflowMapFailed", "fail-fast wake should preserve failure reason");

        const parentHistory = await backend.streamHistory({
          runId: claim.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          parentHistory.events.map((event) => event.eventType).join(",") ===
            "WorkflowStarted,ChildWorkflowMapScheduled,ChildWorkflowMapFailed",
          "fail-fast parent history should stay compact"
        );

        const siblingHistory = await backend.streamHistory({
          runId: second.runId,
          afterEventId: eventId(0),
          upToEventId: eventId(10),
          maxEvents: 10,
          maxBytes: Number.MAX_SAFE_INTEGER
        });
        assert(
          siblingHistory.events.map((event) => event.eventType).join(",") ===
            "WorkflowStarted,WorkflowCancelled",
          "fail-fast should cancel running child-map siblings"
        );
      }
    }
  ];
}

async function startedAndClaimed(
  factory: () => DurableBackend
): Promise<{ backend: DurableBackend; claim: WorkflowTaskClaim }> {
  const backend = factory();
  await backend.startWorkflow({
    namespace: namespace(),
    workflowId: workflowId("wf/commit"),
    workflowType: workflowType("conformance.workflow", 1),
    taskQueue: taskQueue("workflows"),
    input: encodePayload({ value: 1 }, { codec: "Json" })
  });
  const claimed = await backend.claimWorkflowTask("worker-a", {
    namespace: namespace(),
    taskQueue: taskQueue("workflows"),
    registeredWorkflowTypes: [workflowType("conformance.workflow", 1)],
    leaseDurationMs: 30_000
  });
  assert(claimed !== null, "started workflow should be claimable");
  return { backend, claim: claimed.claim };
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

async function assertRejects(fn: () => Promise<unknown>, message: string): Promise<void> {
  try {
    await fn();
  } catch (error) {
    if (String(error).includes(message)) {
      return;
    }
    throw new Error(`expected rejection containing ${message}, got ${String(error)}`);
  }
  throw new Error(`expected rejection containing ${message}`);
}

function collectRootPayloadRefs(value: unknown, seen = new WeakSet<object>()): readonly PayloadRef[] {
  if (isPayloadRef(value)) {
    return [value];
  }
  if (value === null || typeof value !== "object" || value instanceof Uint8Array) {
    return [];
  }
  if (seen.has(value)) {
    return [];
  }
  seen.add(value);
  if (Array.isArray(value)) {
    return value.flatMap((item) => collectRootPayloadRefs(item, seen));
  }
  return Object.values(value).flatMap((nested) => collectRootPayloadRefs(nested, seen));
}

function rootPayloadDigest(payload: PayloadRef): string {
  return payload.kind === "Blob" ? payload.digest : digestBytes(payload.bytes);
}

function activityMapManifestItemRefs<Input extends object>(
  manifestRef: PayloadRef<ActivityMapInputManifest<Input>>
): readonly PayloadRef<Input>[] {
  const manifest = decodePayload<ActivityMapInputManifest<Input>>(manifestRef);
  const refs: PayloadRef<Input>[] = [];
  for (const pageRef of manifest.pages) {
    refs.push(
      ...decodePayload<ActivityMapInputPage<Input>>(
        pageRef as PayloadRef<ActivityMapInputPage<Input>>
      ).items
    );
  }
  return refs;
}

function isPayloadRef(value: unknown): value is PayloadRef {
  if (!value || typeof value !== "object") {
    return false;
  }
  const maybe = value as {
    readonly kind?: unknown;
    readonly codec?: unknown;
    readonly bytes?: unknown;
    readonly digest?: unknown;
    readonly uri?: unknown;
  };
  return (
    (maybe.kind === "Inline" && typeof maybe.codec === "string" && maybe.bytes instanceof Uint8Array) ||
    (maybe.kind === "Blob" &&
      typeof maybe.codec === "string" &&
      typeof maybe.digest === "string" &&
      typeof maybe.uri === "string")
  );
}
