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
