import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  MemoryBackend,
  RetryPolicy,
  activityFingerprint,
  activityMapFingerprint,
  activityMapManifest,
  activityTaskFromScheduled,
  commandId,
  encodePayload,
  eventId,
  namespace,
  payloadDigest,
  runId,
  taskQueue,
  workflowId,
  workflowType,
  type ActivityTaskClaim,
  type ChildWorkflowMapItemOutcome,
  type DurableFailure
} from "@durust/core";
import {
  activityOutcomeCounts,
  childCancellationReason,
  failFastFailure,
  itemRetryDelayMs,
  mapOrdinalInBounds,
  mapSlotLimit,
  outcomeCounts,
  step,
  type ItemAttemptFailureKind,
  type ItemRetryDecision,
  type MapEffect,
  type MapEvent,
  type MapState,
  type MapTransition
} from "../src/map-engine.js";

const MAP_COMMAND_ID = commandId(runId("run-1"), 7);

function activityMap(itemCount: number, maxInFlight: number): MapState {
  return {
    mapCommandId: MAP_COMMAND_ID,
    kind: "Activity",
    failureMode: "FailFast",
    itemCount,
    nextOrdinal: 0,
    inFlight: 0,
    maxInFlight,
    recordedOutcomes: 0,
    completed: false
  };
}

function childMap(
  itemCount: number,
  maxInFlight: number,
  failureMode: MapState["failureMode"]
): MapState {
  return { ...activityMap(itemCount, maxInFlight), kind: "ChildWorkflow", failureMode };
}

const ITEM_FAILURE: DurableFailure = {
  errorType: "kind",
  message: "boom",
  nonRetryable: true
};

function success(): ChildWorkflowMapItemOutcome<unknown> {
  return { kind: "Succeeded", result: encodePayload({ value: 1 }, { codec: "Json" }) };
}

function itemFailure(): ChildWorkflowMapItemOutcome<unknown> {
  return { kind: "Failed", failure: ITEM_FAILURE };
}

function itemCancelled(): ChildWorkflowMapItemOutcome<unknown> {
  return { kind: "Cancelled", reason: "stop" };
}

function completed(ordinal: number, outcome: ChildWorkflowMapItemOutcome<unknown>): MapEvent {
  return { kind: "ItemCompleted", ordinal, outcome, alreadyRecorded: false, parentTerminal: false };
}

const DESCRIPTOR_CREATED: MapEvent = { kind: "DescriptorCreated", parentTerminal: false };

/** Backoff policy every retry case uses unless it says otherwise: 1s, doubling. */
const BACKOFF = RetryPolicy.exponential({
  initialIntervalMs: 1_000,
  maxIntervalMs: 60_000,
  maxAttempts: 5,
  backoffCoefficient: 2
});

interface AttemptFailureOverrides {
  readonly attemptFailure?: ItemAttemptFailureKind;
  readonly failedAttempt?: number;
  readonly retryPolicy?: RetryPolicy;
  readonly startToCloseTimeoutMs?: number | null;
  readonly nowMs?: number;
  readonly alreadyRecorded?: boolean;
  readonly parentTerminal?: boolean;
}

/** Builder for the wide `ItemAttemptFailed` event so each case names only what it varies. */
function attemptFailed(
  ordinal: number,
  decision: ItemRetryDecision,
  overrides: AttemptFailureOverrides = {}
): MapEvent {
  return {
    kind: "ItemAttemptFailed",
    ordinal,
    failure: ITEM_FAILURE,
    attemptFailure: overrides.attemptFailure ?? "Failed",
    decision,
    failedAttempt: overrides.failedAttempt ?? 1,
    retryPolicy: overrides.retryPolicy ?? BACKOFF,
    startToCloseTimeoutMs: overrides.startToCloseTimeoutMs ?? null,
    nowMs: overrides.nowMs ?? 10_000,
    alreadyRecorded: overrides.alreadyRecorded ?? false,
    parentTerminal: overrides.parentTerminal ?? false
  };
}

const RETRY: ItemRetryDecision = { kind: "Retry", nextAttempt: 2 };
const EXHAUSTED: ItemRetryDecision = { kind: "Exhausted" };

function ok(effects: readonly MapEffect[]): MapTransition {
  return { kind: "Effects", effects };
}

const NONE: MapTransition = ok([]);

describe("map engine: materialization and maxInFlight admission", () => {
  it("admits one contiguous batch bounded by free slots at every maxInFlight boundary", () => {
    const cases: readonly {
      readonly name: string;
      readonly itemCount: number;
      readonly maxInFlight: number;
      readonly nextOrdinal: number;
      readonly inFlight: number;
      readonly expected: readonly [number, number] | null;
    }[] = [
      {
        name: "zero bound is read as one slot rather than stalling",
        itemCount: 3,
        maxInFlight: 0,
        nextOrdinal: 0,
        inFlight: 0,
        expected: [0, 1]
      },
      {
        name: "negative bound is read as one slot rather than admitting nothing",
        itemCount: 3,
        maxInFlight: -4,
        nextOrdinal: 0,
        inFlight: 0,
        expected: [0, 1]
      },
      {
        name: "bound of one admits exactly one item",
        itemCount: 3,
        maxInFlight: 1,
        nextOrdinal: 0,
        inFlight: 0,
        expected: [0, 1]
      },
      {
        name: "free slots cap the batch, not the item count",
        itemCount: 10,
        maxInFlight: 4,
        nextOrdinal: 0,
        inFlight: 0,
        expected: [0, 4]
      },
      {
        name: "item count caps the batch, not the slots",
        itemCount: 2,
        maxInFlight: 10,
        nextOrdinal: 0,
        inFlight: 0,
        expected: [0, 2]
      },
      {
        name: "exactly at the bound admits nothing",
        itemCount: 10,
        maxInFlight: 4,
        nextOrdinal: 4,
        inFlight: 4,
        expected: null
      },
      {
        name: "one below the bound admits exactly one",
        itemCount: 10,
        maxInFlight: 4,
        nextOrdinal: 4,
        inFlight: 3,
        expected: [4, 1]
      },
      {
        name: "one over the bound admits nothing and never underflows",
        itemCount: 10,
        maxInFlight: 4,
        nextOrdinal: 5,
        inFlight: 5,
        expected: null
      },
      {
        name: "fully admitted map admits nothing even with free slots",
        itemCount: 3,
        maxInFlight: 8,
        nextOrdinal: 3,
        inFlight: 0,
        expected: null
      }
    ];

    for (const testCase of cases) {
      const state: MapState = {
        ...activityMap(testCase.itemCount, testCase.maxInFlight),
        nextOrdinal: testCase.nextOrdinal,
        inFlight: testCase.inFlight
      };
      const transition = step(state, DESCRIPTOR_CREATED);
      if (testCase.expected === null) {
        expect(transition, testCase.name).toEqual(NONE);
        continue;
      }
      const [firstOrdinal, count] = testCase.expected;
      expect(transition, testCase.name).toEqual(
        ok([
          { kind: "MaterializeItems", firstOrdinal, count },
          {
            kind: "AdvanceDescriptor",
            nextOrdinal: firstOrdinal + count,
            inFlight: testCase.inFlight + count
          }
        ])
      );
    }
  });

  it("emits one range effect per batch so set-based providers issue one statement", () => {
    const transition = step(activityMap(10_000, 10_000), DESCRIPTOR_CREATED);
    expect(transition).toEqual(
      ok([
        { kind: "MaterializeItems", firstOrdinal: 0, count: 10_000 },
        { kind: "AdvanceDescriptor", nextOrdinal: 10_000, inFlight: 10_000 }
      ])
    );
  });

  it("reads the admission cursor and never scans recorded outcomes", () => {
    // Same cursor, wildly different outcome tallies: admission is identical,
    // because only the cursor and the slots decide it.
    for (const recordedOutcomes of [0, 1, 4]) {
      const state: MapState = {
        ...activityMap(5, 2),
        nextOrdinal: 2,
        inFlight: 0,
        recordedOutcomes
      };
      expect(step(state, DESCRIPTOR_CREATED), `recordedOutcomes=${recordedOutcomes}`).toEqual(
        ok([
          { kind: "MaterializeItems", firstOrdinal: 2, count: 2 },
          { kind: "AdvanceDescriptor", nextOrdinal: 4, inFlight: 2 }
        ])
      );
    }
  });

  it("completes an empty input manifest at descriptor creation, for both kinds and both parent states", () => {
    // Nothing to admit and nothing outstanding, so the map is terminal the
    // moment its descriptor exists — an empty result manifest, the parent
    // notified, the descriptor closed. `parentTerminal` is deliberately not
    // consulted: the commit creating this descriptor is the commit closing the
    // run, so rejecting would roll back a commit every provider accepts today.
    for (const state of [activityMap(0, 4), childMap(0, 4, "CollectAll")]) {
      for (const parentTerminal of [false, true]) {
        expect(
          step(state, { kind: "DescriptorCreated", parentTerminal }),
          `${state.kind} parentTerminal=${parentTerminal}`
        ).toEqual(
          ok([{ kind: "CompleteMap", itemCount: 0 }, { kind: "MarkDescriptorTerminal" }])
        );
      }
    }
  });

  it("never completes or rejects a non-empty manifest at descriptor creation, even with the parent closed", () => {
    // The guard against the empty-manifest completion firing one item early,
    // and against "schedule a map and return in one workflow task" starting to
    // roll its whole commit back.
    for (const state of [activityMap(2, 4), childMap(2, 4, "CollectAll")]) {
      for (const parentTerminal of [false, true]) {
        expect(
          step(state, { kind: "DescriptorCreated", parentTerminal }),
          `${state.kind} parentTerminal=${parentTerminal}`
        ).toEqual(
          ok([
            { kind: "MaterializeItems", firstOrdinal: 0, count: 2 },
            { kind: "AdvanceDescriptor", nextOrdinal: 2, inFlight: 2 }
          ])
        );
      }
    }
  });
});

describe("map engine: item completion", () => {
  it("releases exactly one slot and spends it on the next unadmitted ordinal", () => {
    const state: MapState = { ...activityMap(5, 2), nextOrdinal: 2, inFlight: 2 };
    expect(step(state, completed(0, success()))).toEqual(
      ok([
        { kind: "MaterializeItems", firstOrdinal: 2, count: 1 },
        { kind: "AdvanceDescriptor", nextOrdinal: 3, inFlight: 2 }
      ])
    );
  });

  it("records a per-item outcome row for child maps only", () => {
    const child: MapState = { ...childMap(5, 2, "CollectAll"), nextOrdinal: 2, inFlight: 2 };
    const childTransition = step(child, completed(0, success()));
    expect(childTransition).toEqual(
      ok([
        { kind: "RecordItemOutcome", ordinal: 0, outcome: success() },
        { kind: "MaterializeItems", firstOrdinal: 2, count: 1 },
        { kind: "AdvanceDescriptor", nextOrdinal: 3, inFlight: 2 }
      ])
    );

    const activity: MapState = { ...activityMap(5, 2), nextOrdinal: 2, inFlight: 2 };
    const activityTransition = step(activity, completed(0, success()));
    expect(activityTransition.kind).toBe("Effects");
    if (activityTransition.kind !== "Effects") {
      throw new Error("expected effects");
    }
    expect(
      activityTransition.effects.some((effect) => effect.kind === "RecordItemOutcome")
    ).toBe(false);
  });

  it("assembles the manifest and notifies the parent when the last item lands", () => {
    const state: MapState = {
      ...activityMap(3, 2),
      nextOrdinal: 3,
      inFlight: 1,
      recordedOutcomes: 2
    };
    expect(step(state, completed(2, success()))).toEqual(
      ok([{ kind: "CompleteMap", itemCount: 3 }, { kind: "MarkDescriptorTerminal" }])
    );
  });

  it("completes on exactly the last item, never one item early", () => {
    // The late direction (completing after the last item) is caught by the
    // cases above. This pins the *early* direction: an off-by-one that fires
    // the completion check one item sooner emits a short result manifest and
    // silently discards an in-flight item's result. Every state below is
    // self-consistent: admitted (`nextOrdinal`) minus recorded equals
    // `inFlight`, and the ordinal being completed is the lowest in-flight one.
    const cases: readonly {
      readonly name: string;
      readonly nextOrdinal: number;
      readonly inFlight: number;
      readonly recordedOutcomes: number;
      readonly ordinal: number;
      readonly expected: readonly MapEffect[];
    }[] = [
      {
        name: "two items short of the end, with a free slot to spend",
        nextOrdinal: 4,
        inFlight: 1,
        recordedOutcomes: 3,
        ordinal: 3,
        expected: [
          { kind: "MaterializeItems", firstOrdinal: 4, count: 1 },
          { kind: "AdvanceDescriptor", nextOrdinal: 5, inFlight: 1 }
        ]
      },
      {
        name: "two items short of the end, fully admitted so nothing to admit",
        nextOrdinal: 5,
        inFlight: 2,
        recordedOutcomes: 3,
        ordinal: 3,
        expected: []
      },
      {
        name: "the last item completes the map",
        nextOrdinal: 5,
        inFlight: 1,
        recordedOutcomes: 4,
        ordinal: 4,
        expected: [{ kind: "CompleteMap", itemCount: 5 }, { kind: "MarkDescriptorTerminal" }]
      }
    ];

    for (const testCase of cases) {
      const activity: MapState = {
        ...activityMap(5, 2),
        nextOrdinal: testCase.nextOrdinal,
        inFlight: testCase.inFlight,
        recordedOutcomes: testCase.recordedOutcomes
      };
      expect(
        step(activity, completed(testCase.ordinal, success())),
        `activity: ${testCase.name}`
      ).toEqual(ok(testCase.expected));

      const child: MapState = { ...activity, kind: "ChildWorkflow", failureMode: "CollectAll" };
      expect(step(child, completed(testCase.ordinal, success())), `child: ${testCase.name}`).toEqual(
        ok([
          { kind: "RecordItemOutcome", ordinal: testCase.ordinal, outcome: success() },
          ...testCase.expected
        ])
      );
    }
  });

  it("completes a collect-all child map once every item is terminal, however it ended", () => {
    const state: MapState = {
      ...childMap(3, 3, "CollectAll"),
      nextOrdinal: 3,
      inFlight: 1,
      recordedOutcomes: 2
    };
    expect(step(state, completed(2, itemFailure()))).toEqual(
      ok([
        { kind: "RecordItemOutcome", ordinal: 2, outcome: itemFailure() },
        { kind: "CompleteMap", itemCount: 3 },
        { kind: "MarkDescriptorTerminal" }
      ])
    );
  });
});

describe("map engine: per-item retry and exhaustion", () => {
  it("retries a retryable attempt and fails the map on the exhausting one", () => {
    const state: MapState = { ...activityMap(5, 2), nextOrdinal: 2, inFlight: 2 };
    expect(step(state, attemptFailed(0, RETRY))).toEqual(
      ok([
        {
          kind: "ScheduleItemRetry",
          ordinal: 0,
          nextAttempt: 2,
          visibleAtMs: 11_000,
          timeoutAtMs: null
        }
      ])
    );
    expect(step(state, attemptFailed(0, EXHAUSTED))).toEqual(
      ok([
        { kind: "FailMap", failure: ITEM_FAILURE },
        { kind: "AbandonPendingItems" },
        { kind: "MarkDescriptorTerminal" }
      ])
    );
  });

  it("keeps the in-flight slot across a retry so nothing else is admitted", () => {
    const state: MapState = { ...activityMap(5, 2), nextOrdinal: 2, inFlight: 2 };
    const transition = step(state, attemptFailed(0, RETRY));
    expect(transition.kind).toBe("Effects");
    if (transition.kind !== "Effects") {
      throw new Error("expected effects");
    }
    expect(
      transition.effects.some(
        (effect) => effect.kind === "MaterializeItems" || effect.kind === "AdvanceDescriptor"
      )
    ).toBe(false);
  });

  it("paces explicit failures by the backoff and leaves timeout retries immediately claimable", () => {
    const state: MapState = { ...activityMap(3, 1), nextOrdinal: 1, inFlight: 1 };
    // A lapsed deadline already paced this attempt.
    expect(
      step(state, attemptFailed(0, { kind: "Retry", nextAttempt: 3 }, { attemptFailure: "TimedOut" }))
    ).toEqual(
      ok([
        { kind: "ScheduleItemRetry", ordinal: 0, nextAttempt: 3, visibleAtMs: null, timeoutAtMs: null }
      ])
    );
    // The paced variant of the same attempt doubles per failed attempt.
    expect(
      step(state, attemptFailed(0, { kind: "Retry", nextAttempt: 4 }, { failedAttempt: 3 }))
    ).toEqual(
      ok([
        {
          kind: "ScheduleItemRetry",
          ordinal: 0,
          nextAttempt: 4,
          visibleAtMs: 10_000 + 4_000,
          timeoutAtMs: null
        }
      ])
    );
    // A policy without backoff stays immediately claimable.
    expect(
      step(state, attemptFailed(0, RETRY, { retryPolicy: RetryPolicy.none() }))
    ).toEqual(
      ok([
        { kind: "ScheduleItemRetry", ordinal: 0, nextAttempt: 2, visibleAtMs: null, timeoutAtMs: null }
      ])
    );
  });

  it("restarts the start-to-close deadline from the visibility instant, not from now", () => {
    const state: MapState = { ...activityMap(3, 1), nextOrdinal: 1, inFlight: 1 };
    // Paced retry: the deadline is measured from `now + backoff`.
    expect(step(state, attemptFailed(0, RETRY, { startToCloseTimeoutMs: 30_000 }))).toEqual(
      ok([
        {
          kind: "ScheduleItemRetry",
          ordinal: 0,
          nextAttempt: 2,
          visibleAtMs: 11_000,
          timeoutAtMs: 11_000 + 30_000
        }
      ])
    );
    // Timeout retry: immediately claimable, so the deadline is measured from `now`.
    expect(
      step(
        state,
        attemptFailed(0, RETRY, { attemptFailure: "TimedOut", startToCloseTimeoutMs: 30_000 })
      )
    ).toEqual(
      ok([
        {
          kind: "ScheduleItemRetry",
          ordinal: 0,
          nextAttempt: 2,
          visibleAtMs: null,
          timeoutAtMs: 10_000 + 30_000
        }
      ])
    );
    // An item with no start-to-close timeout gets no deadline at all.
    expect(step(state, attemptFailed(0, RETRY))).toEqual(
      ok([
        { kind: "ScheduleItemRetry", ordinal: 0, nextAttempt: 2, visibleAtMs: 11_000, timeoutAtMs: null }
      ])
    );
  });

  it("paces map item retries exactly like the plain activity retry path", async () => {
    // `MemoryBackend` still owns its own copy of the backoff for standalone
    // activities; this pins the engine's helper against it so the two cannot
    // drift before row 6D collapses them.
    const policies = [
      RetryPolicy.none(),
      RetryPolicy.exponential({
        initialIntervalMs: 100,
        maxIntervalMs: 1_000,
        maxAttempts: 4,
        backoffCoefficient: 2
      }),
      RetryPolicy.exponential({
        initialIntervalMs: 700,
        maxIntervalMs: 900,
        maxAttempts: 4,
        backoffCoefficient: 3
      })
    ];
    for (const policy of policies) {
      const nowMs = 1_000;
      const backend = new MemoryBackend({ nowMs: () => nowMs });
      await backend.startWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/map-engine-retry"),
        workflowType: workflowType("map-engine.retry", 1),
        taskQueue: taskQueue("workflows"),
        input: encodePayload({ value: 1 }, { codec: "Json" })
      });
      const claimed = await backend.claimWorkflowTask("workflow-worker", {
        namespace: namespace(),
        taskQueue: taskQueue("workflows"),
        registeredWorkflowTypes: [workflowType("map-engine.retry", 1)],
        leaseDurationMs: 30_000
      });
      if (claimed === null) {
        throw new Error("expected workflow claim");
      }
      const input = encodePayload({ value: 1 }, { codec: "Json" });
      const scheduled = {
        commandId: commandId(claimed.runId, 1),
        activityName: "map-engine.retry.activity",
        taskQueue: "activities",
        retryPolicy: policy,
        startToCloseTimeoutMs: null,
        heartbeatTimeoutMs: null,
        input,
        fingerprint: activityFingerprint(
          "map-engine.retry.activity",
          payloadDigest(input),
          "sha256:map-engine"
        )
      };
      await backend.commitWorkflowTask(claimed.claim, {
        expectedTailEventId: eventId(1),
        appendEvents: [{ data: { kind: "ActivityScheduled", scheduled } }],
        scheduleActivities: [activityTaskFromScheduled(scheduled)]
      });
      const attempt = await backend.claimActivityTask("activity-worker", {
        namespace: namespace(),
        taskQueue: taskQueue("activities"),
        registeredActivityNames: ["map-engine.retry.activity"],
        leaseDurationMs: 30_000
      });
      if (attempt === null) {
        throw new Error("expected activity claim");
      }
      const outcome = await backend.failActivity({
        claim: attempt.claim,
        failure: { errorType: "map-engine.retryable", message: "boom", nonRetryable: false }
      });
      if (outcome.kind !== "RetryScheduled") {
        // `RetryPolicy.none()` exhausts on the first attempt; the engine's
        // delay for it must still be zero so the retry would be immediate.
        expect(itemRetryDelayMs(policy, 1)).toBe(0);
        continue;
      }
      expect(outcome.readyAtMs, `policy ${JSON.stringify(policy)}`).toBe(
        nowMs + itemRetryDelayMs(policy, 1)
      );
    }
  });
});

describe("map engine: failure modes", () => {
  it("stops a fail-fast child map on the first non-success and keeps a collect-all going", () => {
    for (const outcome of [itemFailure(), itemCancelled()]) {
      const failFast: MapState = { ...childMap(5, 2, "FailFast"), nextOrdinal: 2, inFlight: 2 };
      const expectedFailure = failFastFailure(0, outcome);
      expect(expectedFailure).not.toBeNull();
      expect(step(failFast, completed(0, outcome)), `fail-fast ${outcome.kind}`).toEqual(
        ok([
          { kind: "RecordItemOutcome", ordinal: 0, outcome },
          { kind: "FailMap", failure: expectedFailure as DurableFailure },
          { kind: "AbandonPendingItems" },
          { kind: "CancelChildren", reason: childCancellationReason(MAP_COMMAND_ID) },
          { kind: "MarkDescriptorTerminal" }
        ])
      );

      const collectAll: MapState = {
        ...childMap(5, 2, "CollectAll"),
        nextOrdinal: 2,
        inFlight: 2
      };
      expect(step(collectAll, completed(0, outcome)), `collect-all ${outcome.kind}`).toEqual(
        ok([
          { kind: "RecordItemOutcome", ordinal: 0, outcome },
          { kind: "MaterializeItems", firstOrdinal: 2, count: 1 },
          { kind: "AdvanceDescriptor", nextOrdinal: 3, inFlight: 2 }
        ])
      );
    }
  });

  it("never cancels children for an activity map, which has none", () => {
    const state: MapState = { ...activityMap(5, 2), nextOrdinal: 2, inFlight: 2 };
    expect(step(state, completed(0, itemFailure()))).toEqual(
      ok([
        { kind: "FailMap", failure: ITEM_FAILURE },
        { kind: "AbandonPendingItems" },
        { kind: "MarkDescriptorTerminal" }
      ])
    );
  });
});

describe("map engine: parent cancellation and terminal absorption", () => {
  it("abandons pending items and closes the descriptor when the parent cancels mid-fanout", () => {
    for (const state of [
      { ...activityMap(5, 2), nextOrdinal: 2, inFlight: 2, recordedOutcomes: 1 },
      { ...childMap(5, 2, "CollectAll"), nextOrdinal: 2, inFlight: 2, recordedOutcomes: 1 }
    ]) {
      expect(step(state, { kind: "ParentCancelled" }), state.kind).toEqual(
        ok([{ kind: "AbandonPendingItems" }, { kind: "MarkDescriptorTerminal" }])
      );
    }
  });

  it("absorbs every later event once the descriptor is terminal", () => {
    for (const kind of ["Activity", "ChildWorkflow"] as const) {
      const terminal: MapState = {
        ...activityMap(5, 2),
        kind,
        completed: true,
        nextOrdinal: 5,
        inFlight: 0,
        recordedOutcomes: 5
      };
      const events: readonly MapEvent[] = [
        DESCRIPTOR_CREATED,
        completed(0, success()),
        completed(4, itemFailure()),
        attemptFailed(0, EXHAUSTED),
        attemptFailed(0, RETRY),
        { kind: "ParentCancelled" }
      ];
      for (const event of events) {
        expect(step(terminal, event), `${kind} absorbs ${event.kind}`).toEqual(NONE);
      }
    }
  });

  it("keeps a cancelled map absorbing, including out-of-bounds and duplicate deliveries", () => {
    const cancelled: MapState = {
      ...childMap(5, 2, "CollectAll"),
      completed: true,
      inFlight: 0,
      nextOrdinal: 2,
      recordedOutcomes: 1
    };
    expect(step(cancelled, { kind: "ParentCancelled" })).toEqual(NONE);
    expect(step(cancelled, completed(1, success()))).toEqual(NONE);
    expect(step(cancelled, completed(99, success()))).toEqual(NONE);
    expect(step(cancelled, attemptFailed(1, EXHAUSTED))).toEqual(NONE);
    expect(step(cancelled, attemptFailed(1, RETRY))).toEqual(NONE);
    expect(step(cancelled, DESCRIPTOR_CREATED)).toEqual(NONE);
  });
});

describe("map engine: idempotency", () => {
  it("releases no second slot for a duplicate terminal outcome", () => {
    const collectAll: MapState = {
      ...childMap(5, 2, "CollectAll"),
      nextOrdinal: 2,
      inFlight: 2,
      recordedOutcomes: 1
    };
    expect(
      step(collectAll, {
        kind: "ItemCompleted",
        ordinal: 0,
        outcome: success(),
        alreadyRecorded: true,
        parentTerminal: false
      })
    ).toEqual(NONE);

    // The same guard covers a duplicate failure, including a fail-fast one that
    // would otherwise re-fail an already-recorded item.
    const failFast: MapState = { ...collectAll, failureMode: "FailFast" };
    expect(
      step(failFast, {
        kind: "ItemCompleted",
        ordinal: 0,
        outcome: itemFailure(),
        alreadyRecorded: true,
        parentTerminal: false
      })
    ).toEqual(NONE);

    const activity: MapState = {
      ...activityMap(5, 2),
      nextOrdinal: 2,
      inFlight: 2,
      recordedOutcomes: 1
    };
    expect(step(activity, attemptFailed(0, EXHAUSTED, { alreadyRecorded: true }))).toEqual(NONE);
  });

  it("still reschedules a retry for an already-recorded ordinal, because a retry records nothing", () => {
    // `alreadyRecorded` guards the *outcome* write; a retry has no outcome to
    // duplicate, so the flag must not suppress it.
    const state: MapState = { ...activityMap(5, 2), nextOrdinal: 2, inFlight: 2 };
    expect(step(state, attemptFailed(0, RETRY, { alreadyRecorded: true }))).toEqual(
      ok([
        { kind: "ScheduleItemRetry", ordinal: 0, nextAttempt: 2, visibleAtMs: 11_000, timeoutAtMs: null }
      ])
    );
  });

  it("never appends a parent fact for a completion arriving after the parent closed", () => {
    const activity: MapState = {
      ...activityMap(3, 2),
      nextOrdinal: 3,
      inFlight: 1,
      recordedOutcomes: 2
    };
    expect(
      step(activity, {
        kind: "ItemCompleted",
        ordinal: 2,
        outcome: success(),
        alreadyRecorded: false,
        parentTerminal: true
      })
    ).toEqual({ kind: "Reject", reject: { kind: "TerminalParent" } });
    expect(step(activity, attemptFailed(2, EXHAUSTED, { parentTerminal: true }))).toEqual({
      kind: "Reject",
      reject: { kind: "TerminalParent" }
    });

    const child: MapState = {
      ...childMap(3, 2, "CollectAll"),
      nextOrdinal: 3,
      inFlight: 1,
      recordedOutcomes: 2
    };
    expect(
      step(child, {
        kind: "ItemCompleted",
        ordinal: 2,
        outcome: success(),
        alreadyRecorded: false,
        parentTerminal: true
      })
    ).toEqual(ok([{ kind: "RecordItemOutcome", ordinal: 2, outcome: success() }]));

    const childFailFast: MapState = { ...child, failureMode: "FailFast" };
    expect(
      step(childFailFast, {
        kind: "ItemCompleted",
        ordinal: 1,
        outcome: itemFailure(),
        alreadyRecorded: false,
        parentTerminal: true
      })
    ).toEqual(ok([{ kind: "RecordItemOutcome", ordinal: 1, outcome: itemFailure() }]));
  });

  it("never partially closes the descriptor when the parent is terminal", () => {
    const child: MapState = {
      ...childMap(3, 2, "CollectAll"),
      nextOrdinal: 3,
      inFlight: 1,
      recordedOutcomes: 2
    };
    const transition = step(child, {
      kind: "ItemCompleted",
      ordinal: 2,
      outcome: success(),
      alreadyRecorded: false,
      parentTerminal: true
    });
    expect(transition.kind).toBe("Effects");
    if (transition.kind !== "Effects") {
      throw new Error("expected effects");
    }
    expect(
      transition.effects.some(
        (effect) =>
          effect.kind === "MarkDescriptorTerminal" ||
          effect.kind === "CompleteMap" ||
          effect.kind === "FailMap"
      )
    ).toBe(false);
  });
});

describe("map engine: rejections", () => {
  it("rejects the whole transition for an ordinal outside the input manifest", () => {
    const state: MapState = { ...activityMap(3, 3), nextOrdinal: 3, inFlight: 3 };
    const outOfBounds: readonly { readonly ordinal: number; readonly event: MapEvent }[] = [
      { ordinal: 3, event: completed(3, success()) },
      { ordinal: 9, event: attemptFailed(9, EXHAUSTED) },
      { ordinal: 9, event: attemptFailed(9, RETRY) },
      { ordinal: -1, event: completed(-1, success()) }
    ];
    for (const testCase of outOfBounds) {
      expect(step(state, testCase.event), `ordinal ${testCase.ordinal}`).toEqual({
        kind: "Reject",
        reject: { kind: "OutOfBounds", ordinal: testCase.ordinal }
      });
    }
  });
});

describe("map engine: whole-fanout invariants", () => {
  it("never exceeds the bound and admits each ordinal exactly once across a full fanout", () => {
    let state = activityMap(7, 3);
    const admitted: number[] = [];
    const apply = (effects: readonly MapEffect[]): void => {
      for (const effect of effects) {
        switch (effect.kind) {
          case "MaterializeItems":
            for (let ordinal = 0; ordinal < effect.count; ordinal += 1) {
              admitted.push(effect.firstOrdinal + ordinal);
            }
            break;
          case "AdvanceDescriptor":
            state = { ...state, nextOrdinal: effect.nextOrdinal, inFlight: effect.inFlight };
            break;
          case "CompleteMap":
            break;
          case "MarkDescriptorTerminal":
            state = { ...state, completed: true, inFlight: 0 };
            break;
          default:
            throw new Error(`unexpected effect ${effect.kind}`);
        }
      }
      expect(state.inFlight).toBeLessThanOrEqual(mapSlotLimit(state));
    };

    const created = step(state, DESCRIPTOR_CREATED);
    if (created.kind !== "Effects") {
      throw new Error("expected effects");
    }
    apply(created.effects);
    for (let ordinal = 0; ordinal < 7; ordinal += 1) {
      const transition = step(state, completed(ordinal, success()));
      if (transition.kind !== "Effects") {
        throw new Error("expected effects");
      }
      state = {
        ...state,
        recordedOutcomes: state.recordedOutcomes + 1,
        inFlight: Math.max(0, state.inFlight - 1)
      };
      apply(transition.effects);
    }
    expect(state.completed).toBe(true);
    expect(admitted).toEqual([0, 1, 2, 3, 4, 5, 6]);
  });

  it("is pure: repeated steps are equal and the input state is never mutated", () => {
    const state: MapState = { ...childMap(5, 2, "CollectAll"), nextOrdinal: 2, inFlight: 2 };
    const before = structuredClone(state);
    const events: readonly MapEvent[] = [
      DESCRIPTOR_CREATED,
      completed(0, success()),
      completed(0, itemCancelled()),
      attemptFailed(0, RETRY),
      attemptFailed(0, EXHAUSTED),
      { kind: "ParentCancelled" }
    ];
    for (const event of events) {
      expect(step(state, event), event.kind).toEqual(step(state, event));
    }
    expect(state).toEqual(before);
  });
});

describe("map engine: shared tallies and persisted strings", () => {
  it("classifies every outcome variant", () => {
    expect(
      outcomeCounts([success(), itemFailure(), itemCancelled(), itemFailure(), success()])
    ).toEqual({ successCount: 2, failureCount: 2, cancellationCount: 1 });
    expect(outcomeCounts([])).toEqual({
      successCount: 0,
      failureCount: 0,
      cancellationCount: 0
    });
    expect(activityOutcomeCounts(4)).toEqual({
      successCount: 4,
      failureCount: 0,
      cancellationCount: 0
    });
  });

  it("pins the parent-visible failure strings byte for byte", () => {
    // These land in committed history, so they are pinned exactly.
    expect(failFastFailure(3, success())).toBeNull();
    expect(failFastFailure(3, itemCancelled())).toEqual({
      errorType: "durust.child_workflow_cancelled",
      message: "child workflow map item 3 was cancelled: stop",
      nonRetryable: true
    });
    expect(failFastFailure(3, itemFailure())).toEqual(ITEM_FAILURE);
    expect(childCancellationReason(MAP_COMMAND_ID)).toBe("child workflow map `run-1`:7 failed");
  });

  it("clamps the slot limit and bounds ordinals", () => {
    expect(mapSlotLimit(activityMap(3, 0))).toBe(1);
    expect(mapSlotLimit(activityMap(3, 1))).toBe(1);
    expect(mapSlotLimit(activityMap(3, 9))).toBe(9);
    const state = activityMap(3, 2);
    expect(mapOrdinalInBounds(state, 0)).toBe(true);
    expect(mapOrdinalInBounds(state, 2)).toBe(true);
    expect(mapOrdinalInBounds(state, 3)).toBe(false);
    expect(mapOrdinalInBounds(state, -1)).toBe(false);
    expect(mapOrdinalInBounds(activityMap(0, 2), 0)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// The shared transition table (`typescript/fixtures/contract/map-transitions.json`).
//
// One checked-in artefact, read by this runner and by `tests/map_transitions.rs`,
// so the Rust and TypeScript map engines cannot drift apart silently. The file
// itself documents which behaviours are deliberately excluded because the two
// runtimes are knowingly divergent there, and why the Rust runner asserts the
// `fanouts` section rather than the `transitions` one.
// ---------------------------------------------------------------------------

interface TransitionTable {
  readonly exclusions: readonly { readonly what: string; readonly why: string }[];
  readonly transitions: readonly TransitionCase[];
  readonly fanouts: readonly FanoutCase[];
}

interface TransitionCase {
  readonly name: string;
  readonly state: {
    readonly kind: MapState["kind"];
    readonly failureMode: MapState["failureMode"];
    readonly itemCount: number;
    readonly nextOrdinal: number;
    readonly inFlight: number;
    readonly maxInFlight: number;
    readonly recordedOutcomes: number;
    readonly completed: boolean;
  };
  readonly event: Record<string, unknown> & { readonly kind: MapEvent["kind"] };
  readonly expect:
    | { readonly kind: "Effects"; readonly effects: readonly Record<string, unknown>[] }
    | { readonly kind: "Reject"; readonly reject: Record<string, unknown> };
}

interface FanoutCase {
  readonly name: string;
  readonly itemCount: number;
  readonly maxInFlight: number;
  readonly steps: readonly {
    readonly action: "claim" | "complete" | "fail" | "completeAbandoned";
    readonly ordinal: number | null;
  }[];
  readonly parentHistory: readonly string[];
}

const TRANSITION_TABLE = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../../fixtures/contract/map-transitions.json", import.meta.url)),
    "utf8"
  )
) as TransitionTable;

/**
 * Backoff whose first retry is genuinely deferred, used wherever the table says
 * `"Deferred"`. The table never asserts the delay itself — the two runtimes
 * have different policy models — only that a deferred retry lands in the
 * future and an immediate one lands at `null`.
 */
const TABLE_DEFERRED_BACKOFF = RetryPolicy.exponential({
  initialIntervalMs: 5_000,
  maxIntervalMs: 60_000,
  maxAttempts: 9,
  backoffCoefficient: 2
});
const TABLE_IMMEDIATE_BACKOFF = RetryPolicy.exponential({
  initialIntervalMs: 0,
  maxIntervalMs: 0,
  maxAttempts: 9,
  backoffCoefficient: 1
});
const TABLE_NOW_MS = 1_700_000_000_000;
const TABLE_START_TO_CLOSE_MS = 30_000;

function tableState(testCase: TransitionCase): MapState {
  return { mapCommandId: MAP_COMMAND_ID, ...testCase.state };
}

function tableEvent(event: TransitionCase["event"]): MapEvent {
  if (event.kind === "DescriptorCreated") {
    return { kind: "DescriptorCreated", parentTerminal: event.parentTerminal as boolean };
  }
  if (event.kind === "ItemCompleted") {
    return {
      kind: "ItemCompleted",
      ordinal: event.ordinal as number,
      outcome: tableOutcome(event.outcome as { readonly kind: string }),
      alreadyRecorded: event.alreadyRecorded as boolean,
      parentTerminal: event.parentTerminal as boolean
    };
  }
  if (event.kind === "ItemAttemptFailed") {
    return {
      kind: "ItemAttemptFailed",
      ordinal: event.ordinal as number,
      failure: ITEM_FAILURE,
      attemptFailure: event.attemptFailure as ItemAttemptFailureKind,
      decision: event.decision as ItemRetryDecision,
      failedAttempt: event.failedAttempt as number,
      retryPolicy:
        event.retryBackoff === "Immediate" ? TABLE_IMMEDIATE_BACKOFF : TABLE_DEFERRED_BACKOFF,
      startToCloseTimeoutMs:
        event.startToCloseTimeout === "Present" ? TABLE_START_TO_CLOSE_MS : null,
      nowMs: TABLE_NOW_MS,
      alreadyRecorded: event.alreadyRecorded as boolean,
      parentTerminal: event.parentTerminal as boolean
    };
  }
  throw new Error(`the shared table excludes the ${event.kind} event`);
}

function tableOutcome(outcome: { readonly kind: string }): ChildWorkflowMapItemOutcome<unknown> {
  if (outcome.kind === "Succeeded") {
    return success();
  }
  return outcome.kind === "Failed" ? itemFailure() : itemCancelled();
}

/**
 * Project one effect onto the fields the table asserts. Payloads, failures and
 * reasons are dropped — they are language-local values, and the engines' own
 * unit tests pin them — and the two retry instants are reduced to the shape the
 * table declares normative.
 */
function tableEffect(effect: MapEffect): Record<string, unknown> {
  switch (effect.kind) {
    case "RecordItemOutcome":
      return { kind: effect.kind, ordinal: effect.ordinal, outcome: { kind: effect.outcome.kind } };
    case "MaterializeItems":
      return { kind: effect.kind, firstOrdinal: effect.firstOrdinal, count: effect.count };
    case "AdvanceDescriptor":
      return { kind: effect.kind, nextOrdinal: effect.nextOrdinal, inFlight: effect.inFlight };
    case "ScheduleItemRetry":
      return {
        kind: effect.kind,
        ordinal: effect.ordinal,
        nextAttempt: effect.nextAttempt,
        visibleAtMs: effect.visibleAtMs === null ? "Null" : "Deferred",
        timeoutAtMs: effect.timeoutAtMs === null ? "Null" : "Present"
      };
    case "CompleteMap":
      return { kind: effect.kind, itemCount: effect.itemCount };
    case "FailMap":
    case "AbandonPendingItems":
    case "CancelChildren":
    case "MarkDescriptorTerminal":
      return { kind: effect.kind };
  }
}

describe("map engine: shared transition table", () => {
  it("declares its cross-runtime exclusions", () => {
    // A table that quietly loses an exclusion would start asserting a
    // behaviour one runtime cannot match, and the failure would look like a
    // regression rather than an out-of-date exclusion list.
    expect(TRANSITION_TABLE.exclusions.map((exclusion) => exclusion.what)).toEqual([
      "ScheduleItemRetry.visibleAtMs and .timeoutAtMs values",
      "Every DescriptorCreated case where recordedOutcomes >= itemCount, including but not limited to the empty manifest",
      "The ParentCancelled event"
    ]);
    for (const exclusion of TRANSITION_TABLE.exclusions) {
      expect(exclusion.why.length).toBeGreaterThan(80);
    }
  });

  it("covers every effect the engine can emit except the excluded ones", () => {
    const emitted = new Set<string>();
    for (const testCase of TRANSITION_TABLE.transitions) {
      if (testCase.expect.kind !== "Effects") {
        continue;
      }
      for (const effect of testCase.expect.effects) {
        emitted.add(effect.kind as string);
      }
    }
    // Every `MapEffect` variant but the one only `ParentCancelled` can reach
    // on its own, which the table excludes.
    expect([...emitted].sort()).toEqual([
      "AbandonPendingItems",
      "AdvanceDescriptor",
      "CancelChildren",
      "CompleteMap",
      "FailMap",
      "MarkDescriptorTerminal",
      "MaterializeItems",
      "RecordItemOutcome",
      "ScheduleItemRetry"
    ]);
    const rejected = new Set(
      TRANSITION_TABLE.transitions
        .filter((testCase) => testCase.expect.kind === "Reject")
        .map((testCase) =>
          testCase.expect.kind === "Reject" ? (testCase.expect.reject.kind as string) : ""
        )
    );
    expect([...rejected].sort()).toEqual(["OutOfBounds", "TerminalParent"]);
  });

  it("never asserts a DescriptorCreated case the two runtimes disagree on", () => {
    for (const testCase of TRANSITION_TABLE.transitions) {
      if (testCase.event.kind !== "DescriptorCreated") {
        continue;
      }
      expect(
        testCase.state.recordedOutcomes < testCase.state.itemCount,
        `${testCase.name} is inside the excluded DescriptorCreated predicate`
      ).toBe(true);
    }
    expect(
      TRANSITION_TABLE.transitions.some((testCase) => testCase.event.kind === "ParentCancelled")
    ).toBe(false);
  });

  for (const testCase of TRANSITION_TABLE.transitions) {
    it(`table: ${testCase.name}`, () => {
      const transition = step(tableState(testCase), tableEvent(testCase.event));
      if (testCase.expect.kind === "Reject") {
        expect(transition.kind).toBe("Reject");
        if (transition.kind !== "Reject") {
          return;
        }
        expect({ ...transition.reject }).toEqual(testCase.expect.reject);
        return;
      }
      expect(transition.kind).toBe("Effects");
      if (transition.kind !== "Effects") {
        return;
      }
      expect(transition.effects.map(tableEffect)).toEqual(testCase.expect.effects);
    });
  }
});

describe("map engine: shared transition table fanouts", () => {
  for (const fanout of TRANSITION_TABLE.fanouts) {
    it(`fanout: ${fanout.name}`, async () => {
      const backend = new MemoryBackend();
      await backend.startWorkflow({
        namespace: namespace(),
        workflowId: workflowId("wf/map-table-fanout"),
        workflowType: workflowType("map-table.workflow", 1),
        taskQueue: taskQueue("workflows"),
        input: encodePayload({ value: 1 }, { codec: "Json" })
      });
      const claimed = await backend.claimWorkflowTask("fanout-scheduler", {
        namespace: namespace(),
        taskQueue: taskQueue("workflows"),
        registeredWorkflowTypes: [workflowType("map-table.workflow", 1)],
        leaseDurationMs: 30_000
      });
      if (claimed === null) {
        throw new Error("expected workflow claim");
      }
      const items = Array.from({ length: fanout.itemCount }, (_, index) => ({ value: index }));
      const inputManifest = activityMapManifest(items, 2);
      const scheduled = {
        commandId: commandId(claimed.runId, 1),
        activityName: "map-table.item",
        taskQueue: "activities",
        retryPolicy: RetryPolicy.none(),
        startToCloseTimeoutMs: null,
        heartbeatTimeoutMs: null,
        inputManifest,
        resultManifestName: "mapped",
        maxInFlight: fanout.maxInFlight,
        fingerprint: activityMapFingerprint(
          "map-table.item",
          payloadDigest(inputManifest),
          "mapped",
          fanout.maxInFlight,
          "sha256:map-table"
        )
      };
      await backend.commitWorkflowTask(claimed.claim, {
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

      const claims = new Map<number, ActivityTaskClaim>();
      for (const [index, stepCase] of fanout.steps.entries()) {
        const where = `${fanout.name} step ${index} (${stepCase.action})`;
        if (stepCase.action === "claim") {
          const task = await backend.claimActivityTask(`fanout-worker-${index}`, {
            namespace: namespace(),
            taskQueue: taskQueue("activities"),
            registeredActivityNames: ["map-table.item"],
            leaseDurationMs: 30_000
          });
          expect(task?.task.mapItem?.itemOrdinal ?? null, where).toBe(stepCase.ordinal);
          if (task !== null) {
            claims.set(task.task.mapItem?.itemOrdinal ?? -1, task.claim);
          }
          continue;
        }
        const claim = claims.get(stepCase.ordinal as number);
        if (claim === undefined) {
          throw new Error(`${where}: ordinal was never claimed`);
        }
        if (stepCase.action === "complete") {
          const outcome = await backend.completeActivity({
            claim,
            result: encodePayload({ value: stepCase.ordinal }, { codec: "Json" })
          });
          expect(outcome.kind, where).toBe("Completed");
          continue;
        }
        if (stepCase.action === "completeAbandoned") {
          const outcome = await backend.completeActivity({
            claim,
            result: encodePayload({ value: stepCase.ordinal }, { codec: "Json" })
          });
          expect(outcome.kind, where).toBe("AlreadyCompleted");
          continue;
        }
        const outcome = await backend.failActivity({
          claim,
          failure: { errorType: "map-table.fatal", message: "fatal", nonRetryable: true }
        });
        expect(outcome.kind, where).toBe("Failed");
      }

      const history = await backend.streamHistory({
        runId: claimed.runId,
        afterEventId: eventId(0),
        upToEventId: eventId(50),
        maxEvents: 50,
        maxBytes: Number.MAX_SAFE_INTEGER
      });
      expect(history.events.map((event) => event.eventType), fanout.name).toEqual(
        fanout.parentHistory
      );
    });
  }
});
