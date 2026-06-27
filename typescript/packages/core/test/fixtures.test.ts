import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  activityFingerprint,
  activityMapFingerprint,
  childWorkflowFingerprint,
  childWorkflowMapFingerprint,
  decodePayload,
  payloadRefFromJson,
  payloadRefToJson,
  signalFingerprint,
  timerFingerprint,
  toBlobRef,
  versionMarkerFingerprint,
  type CommandFingerprint,
  type ActivityTaskClaim,
  type ClaimWorkflowTaskOptions,
  type ClaimedWorkflowTask,
  type CommitOutcome,
  type CompleteActivitiesOutcome,
  type CompleteActivityOutcome,
  type CompleteActivityRequest,
  type DurableFailure,
  type FailActivityOutcome,
  type FailActivityRequest,
  type FireDueTimersOutcome,
  type FireDueTimersRequest,
  type HistoryChunk,
  type HistoryEvent,
  type HistoryEventType,
  type PayloadRefJson,
  type QueryWorkflowOutcome,
  type QueryWorkflowRequest,
  type SignalInboxRecord,
  type SignalWorkflowOutcome,
  type SignalWorkflowRequest,
  type StartWorkflowOutcome,
  type StartWorkflowRequest,
  type StreamHistoryRequest,
  type WorkflowTaskClaim,
  type WorkflowTaskCommit
} from "@durust/core";
import { assertContractFixtureEvents } from "@durust/testing";

const ALL_HISTORY_EVENT_TYPES = [
  "WorkflowStarted",
  "WorkflowCompleted",
  "WorkflowFailed",
  "WorkflowCancelled",
  "WorkflowContinuedAsNew",
  "WorkflowTaskStarted",
  "ActivityScheduled",
  "ActivityMapScheduled",
  "ActivityMapCompleted",
  "ActivityMapFailed",
  "ActivityCompleted",
  "ActivityFailed",
  "ActivityTimedOut",
  "ChildWorkflowStartRequested",
  "ChildWorkflowStarted",
  "ChildWorkflowCompleted",
  "ChildWorkflowFailed",
  "ChildWorkflowCancelled",
  "ChildWorkflowMapScheduled",
  "ChildWorkflowMapCompleted",
  "ChildWorkflowMapFailed",
  "TimerStarted",
  "TimerFired",
  "SignalConsumed",
  "SelectWinner",
  "VersionMarker",
  "DeprecatedPatchMarker",
  "SideEffectMarker"
] as const satisfies readonly HistoryEventType[];

interface CoreContractFixture {
  readonly historyEvents: readonly HistoryEvent[];
  readonly fingerprints: {
    readonly activity: CommandFingerprint;
    readonly activityMap: CommandFingerprint;
    readonly childWorkflow: CommandFingerprint;
    readonly childWorkflowMap: CommandFingerprint;
    readonly timer: CommandFingerprint;
    readonly signal: CommandFingerprint;
    readonly versionMarker: CommandFingerprint;
  };
  readonly payloadRefs: {
    readonly inlineJson: PayloadRefJson;
    readonly blobJson: PayloadRefJson;
    readonly inlineMessagePack: PayloadRefJson;
    readonly blobMessagePack: PayloadRefJson;
  };
  readonly manifestPayloads: {
    readonly activityMapInputPage: PayloadRefJson;
    readonly activityMapInputManifest: PayloadRefJson;
    readonly activityMapResultPage: PayloadRefJson;
    readonly activityMapResultManifest: PayloadRefJson;
    readonly childWorkflowMapResultPage: PayloadRefJson;
    readonly childWorkflowMapResultManifest: PayloadRefJson;
  };
  readonly durableFailures: {
    readonly retryable: DurableFailure;
    readonly nonRetryableWithDetails: DurableFailure;
  };
}

interface CheckoutFixtureInput {
  readonly orderId: string;
}

interface JsonActivityMapInputManifest {
  readonly itemCount: number;
  readonly pageLengths: readonly number[];
  readonly pages: readonly PayloadRefJson[];
}

interface JsonActivityMapInputPage {
  readonly items: readonly PayloadRefJson[];
}

interface JsonActivityMapResultManifest {
  readonly name: string;
  readonly itemCount: number;
  readonly pageLengths: readonly number[];
  readonly pages: readonly PayloadRefJson[];
}

interface JsonActivityMapResultPage {
  readonly results: readonly PayloadRefJson[];
}

type JsonChildWorkflowMapItemOutcome =
  | { readonly kind: "Succeeded"; readonly result: PayloadRefJson }
  | { readonly kind: "Failed"; readonly failure: DurableFailure }
  | { readonly kind: "Cancelled"; readonly reason: string };

interface JsonChildWorkflowMapResultManifest {
  readonly name: string;
  readonly itemCount: number;
  readonly pageLengths: readonly number[];
  readonly pages: readonly PayloadRefJson[];
}

interface JsonChildWorkflowMapResultPage {
  readonly outcomes: readonly JsonChildWorkflowMapItemOutcome[];
}

type JsonStartWorkflowRequest = Omit<StartWorkflowRequest, "input"> & {
  readonly input: PayloadRefJson;
};

type JsonSignalWorkflowRequest = Omit<SignalWorkflowRequest, "payload"> & {
  readonly payload: PayloadRefJson;
};

type JsonSignalInboxRecord = Omit<SignalInboxRecord, "payload"> & {
  readonly payload: PayloadRefJson;
};

type JsonCompleteActivityRequest = Omit<CompleteActivityRequest, "result"> & {
  readonly result: PayloadRefJson;
};

type JsonCompleteActivitiesRequest = {
  readonly completions: readonly JsonCompleteActivityRequest[];
};

type JsonFailActivityRequest = Omit<FailActivityRequest, "failure"> & {
  readonly failure: DurableFailure;
};

type JsonQueryWorkflowOutcome =
  | (Omit<Extract<QueryWorkflowOutcome, { readonly kind: "Found" }>, "projection"> & {
      readonly projection: PayloadRefJson;
    })
  | Exclude<QueryWorkflowOutcome, { readonly kind: "Found" }>;

type JsonWorkflowTaskCommit = Omit<WorkflowTaskCommit, "queryProjection"> & {
  readonly queryProjection?: PayloadRefJson;
};

interface ProviderIoFixture {
  readonly startWorkflow: {
    readonly request: JsonStartWorkflowRequest;
    readonly started: StartWorkflowOutcome;
    readonly alreadyStarted: StartWorkflowOutcome;
  };
  readonly claimWorkflowTask: {
    readonly workerId: string;
    readonly options: ClaimWorkflowTaskOptions;
    readonly claimed: ClaimedWorkflowTask;
  };
  readonly streamHistory: {
    readonly request: StreamHistoryRequest;
    readonly chunk: HistoryChunk;
  };
  readonly commitWorkflowTask: {
    readonly claim: WorkflowTaskClaim;
    readonly commit: JsonWorkflowTaskCommit;
    readonly committed: CommitOutcome;
    readonly conflict: CommitOutcome;
  };
  readonly signalWorkflow: {
    readonly request: JsonSignalWorkflowRequest;
    readonly accepted: SignalWorkflowOutcome;
    readonly duplicate: SignalWorkflowOutcome;
    readonly inboxRecord: JsonSignalInboxRecord;
  };
  readonly activityCompletion: {
    readonly claim: ActivityTaskClaim;
    readonly completeRequest: JsonCompleteActivityRequest;
    readonly completed: CompleteActivityOutcome;
    readonly alreadyCompleted: CompleteActivityOutcome;
    readonly batchRequest: JsonCompleteActivitiesRequest;
    readonly batchOutcome: CompleteActivitiesOutcome;
    readonly failRequest: JsonFailActivityRequest;
    readonly failed: FailActivityOutcome;
    readonly retryScheduled: FailActivityOutcome;
  };
  readonly timers: {
    readonly request: FireDueTimersRequest;
    readonly outcome: FireDueTimersOutcome;
  };
  readonly queryWorkflow: {
    readonly request: QueryWorkflowRequest;
    readonly found: JsonQueryWorkflowOutcome;
    readonly notFound: QueryWorkflowOutcome;
    readonly noProjection: QueryWorkflowOutcome;
  };
  readonly childDispatch: {
    readonly request: { readonly namespace: string; readonly limit: number };
    readonly outcome: { readonly dispatched: number };
  };
  readonly payloadRoots: readonly {
    readonly kind:
      | "Payload"
      | "ActivityMapInputManifest"
      | "ActivityMapResultManifest"
      | "ChildWorkflowMapResultManifest";
    readonly payload: PayloadRefJson;
  }[];
  readonly payloadGarbageCollection: {
    readonly request: { readonly dryRun: boolean };
    readonly rustProviderOutcome: {
      readonly scannedBlobs: number;
      readonly retainedBlobs: number;
      readonly deletedBlobs: number;
    };
    readonly typescriptPlan: {
      readonly reachableUris: readonly string[];
      readonly unreachableUris: readonly string[];
      readonly retainedCount: number;
      readonly unreachableCount: number;
    };
    readonly typescriptCollectOutcome: {
      readonly reachableUris: readonly string[];
      readonly unreachableUris: readonly string[];
      readonly retainedCount: number;
      readonly unreachableCount: number;
      readonly deletedUris: readonly string[];
      readonly deletedCount: number;
    };
  };
}

describe("contract fixtures", () => {
  it("loads neutral history fixtures and validates event type derivation", () => {
    const fixture = loadFixture();

    assertContractFixtureEvents(fixture.historyEvents);
    expect(fixture.historyEvents.map((event) => event.eventType)).toEqual(
      ALL_HISTORY_EVENT_TYPES
    );
    expect(fixture.historyEvents.map((event) => event.data.kind)).toEqual(
      ALL_HISTORY_EVENT_TYPES
    );
    expect(fixture.historyEvents.map((event) => Number(event.eventId))).toEqual(
      ALL_HISTORY_EVENT_TYPES.map((_, index) => index + 1)
    );
  });

  it("matches fingerprint helpers to fixture examples", () => {
    const fixture = loadFixture();

    expect(
      activityFingerprint(
        "payments.price-quote",
        "sha256:quote-input-digest",
        "sha256:activity-options"
      )
    ).toEqual(fixture.fingerprints.activity);
    expect(
      activityMapFingerprint(
        "payments.price-quote",
        "sha256:manifest",
        "partials",
        8,
        "sha256:activity-map-options"
      )
    ).toEqual(fixture.fingerprints.activityMap);
    expect(
      childWorkflowFingerprint(
        { name: "orders.ship", version: 1 },
        "ship/o-1",
        "sha256:ship-input",
        "workflows",
        "Cancel"
      )
    ).toEqual(fixture.fingerprints.childWorkflow);
    expect(
      childWorkflowMapFingerprint(
        { name: "orders.ship", version: 1 },
        "sha256:ship-manifest",
        "ship-results",
        "ship-map",
        4,
        "workflows",
        "Abandon",
        "CollectAll"
      )
    ).toEqual(fixture.fingerprints.childWorkflowMap);
    expect(timerFingerprint("sleep_until", 1_781_821_484_000)).toEqual(fixture.fingerprints.timer);
    expect(signalFingerprint("approved")).toEqual(fixture.fingerprints.signal);
    expect(versionMarkerFingerprint("checkout-v2", 2)).toEqual(
      fixture.fingerprints.versionMarker
    );
  });

  it("round-trips inline and blob payload-ref fixture JSON", () => {
    const fixture = loadFixture();

    const inline = payloadRefFromJson<{ readonly orderId: string }>(
      fixture.payloadRefs.inlineJson
    );
    expect(decodePayload(inline)).toEqual({ orderId: "o-1" });
    expect(payloadRefToJson(inline)).toEqual(fixture.payloadRefs.inlineJson);

    const blob = payloadRefFromJson(fixture.payloadRefs.blobJson);
    expect(payloadRefToJson(blob)).toEqual(fixture.payloadRefs.blobJson);

    const inlineMessagePack = payloadRefFromJson<{ readonly orderId: string }>(
      fixture.payloadRefs.inlineMessagePack
    );
    expect(decodePayload(inlineMessagePack)).toEqual({ orderId: "o-1" });
    expect(payloadRefToJson(inlineMessagePack)).toEqual(
      fixture.payloadRefs.inlineMessagePack
    );
    expect(
      payloadRefToJson(
        toBlobRef(
          inlineMessagePack,
          "s3://durust-fixtures/payloads/checkout-input.msgpack"
        )
      )
    ).toEqual(fixture.payloadRefs.blobMessagePack);

    const blobMessagePack = payloadRefFromJson(fixture.payloadRefs.blobMessagePack);
    expect(payloadRefToJson(blobMessagePack)).toEqual(
      fixture.payloadRefs.blobMessagePack
    );
  });

  it("loads durable failure fixtures with optional payload details", () => {
    const fixture = loadFixture();

    expect(fixture.durableFailures.retryable).toEqual({
      errorType: "ApplicationError",
      message: "checkout failed",
      nonRetryable: false
    });

    const detailed = fixture.durableFailures.nonRetryableWithDetails;
    expect(detailed.errorType).toBe("ValidationError");
    expect(detailed.nonRetryable).toBe(true);
    expect(detailed.details).toBeDefined();
    const details = payloadRefFromJson<{ readonly orderId: string }>(
      detailed.details as PayloadRefJson
    );
    expect(decodePayload(details)).toEqual({ orderId: "o-1" });
  });

  it("decodes manifest root and page payload fixtures", () => {
    const fixture = loadFixture();

    const inputManifest = decodeFixturePayload<JsonActivityMapInputManifest>(
      fixture.manifestPayloads.activityMapInputManifest
    );
    expect(inputManifest).toMatchObject({
      itemCount: 1,
      pageLengths: [1]
    });
    const inputPageRef = requiredAt(inputManifest.pages, 0);
    expect(inputPageRef).toEqual(fixture.manifestPayloads.activityMapInputPage);
    const inputPage = decodeFixturePayload<JsonActivityMapInputPage>(inputPageRef);
    const inputItem = decodeFixturePayload<CheckoutFixtureInput>(requiredAt(inputPage.items, 0));
    expect(inputItem).toEqual({ orderId: "o-1" });

    const resultManifest = decodeFixturePayload<JsonActivityMapResultManifest>(
      fixture.manifestPayloads.activityMapResultManifest
    );
    expect(resultManifest).toMatchObject({
      name: "partials",
      itemCount: 1,
      pageLengths: [1]
    });
    const resultPageRef = requiredAt(resultManifest.pages, 0);
    expect(resultPageRef).toEqual(fixture.manifestPayloads.activityMapResultPage);
    const resultPage = decodeFixturePayload<JsonActivityMapResultPage>(resultPageRef);
    const resultItem = decodeFixturePayload<CheckoutFixtureInput>(
      requiredAt(resultPage.results, 0)
    );
    expect(resultItem).toEqual({ orderId: "o-1" });

    const childManifest = decodeFixturePayload<JsonChildWorkflowMapResultManifest>(
      fixture.manifestPayloads.childWorkflowMapResultManifest
    );
    expect(childManifest).toMatchObject({
      name: "ship-results",
      itemCount: 3,
      pageLengths: [3]
    });
    const childPageRef = requiredAt(childManifest.pages, 0);
    expect(childPageRef).toEqual(fixture.manifestPayloads.childWorkflowMapResultPage);
    const childPage = decodeFixturePayload<JsonChildWorkflowMapResultPage>(childPageRef);
    expect(childPage.outcomes.map((outcome) => outcome.kind)).toEqual([
      "Succeeded",
      "Failed",
      "Cancelled"
    ]);
    const succeeded = requiredAt(childPage.outcomes, 0);
    expect(succeeded.kind).toBe("Succeeded");
    if (succeeded.kind === "Succeeded") {
      expect(decodeFixturePayload<CheckoutFixtureInput>(succeeded.result)).toEqual({
        orderId: "o-1"
      });
    }
    const failed = requiredAt(childPage.outcomes, 1);
    expect(failed).toEqual({
      kind: "Failed",
      failure: fixture.durableFailures.retryable
    });
    const cancelled = requiredAt(childPage.outcomes, 2);
    expect(cancelled).toEqual({
      kind: "Cancelled",
      reason: "parent cancelled"
    });
  });

  it("loads neutral provider request and response fixtures", () => {
    const fixture = loadProviderFixture();

    expect(fixture.startWorkflow.request.workflowType).toEqual({
      name: "orders.checkout",
      version: 1
    });
    expect(
      decodePayload(
        payloadRefFromJson<{ readonly orderId: string }>(fixture.startWorkflow.request.input)
      )
    ).toEqual({ orderId: "o-1" });
    expect(fixture.startWorkflow.started).toEqual({ kind: "Started", runId: "run-1" });
    expect(fixture.startWorkflow.alreadyStarted).toEqual({
      kind: "AlreadyStarted",
      runId: "run-1"
    });

    expect(fixture.claimWorkflowTask.workerId).toBe("worker-a");
    expect(fixture.claimWorkflowTask.options.leaseDurationMs).toBe(30_000);
    expect(fixture.claimWorkflowTask.claimed.reason).toBe("WorkflowStarted");
    assertContractFixtureEvents(fixture.claimWorkflowTask.claimed.prefetchedHistory);

    assertContractFixtureEvents(fixture.streamHistory.chunk.events);
    expect(fixture.streamHistory.chunk).toMatchObject({
      lastEventId: 1,
      hasMore: true
    });

    expect(fixture.commitWorkflowTask.commit.appendEvents?.[0]?.data.kind).toBe(
      "WorkflowTaskStarted"
    );
    expect(fixture.commitWorkflowTask.commit.upsertWaits?.[0]).toMatchObject({
      kind: "Timer",
      readyAt: 1_781_821_484_000
    });
    expect(fixture.commitWorkflowTask.committed).toEqual({
      kind: "Committed",
      newTailEventId: 2
    });
    expect(fixture.commitWorkflowTask.conflict).toEqual({ kind: "Conflict" });

    expect(fixture.signalWorkflow.accepted).toEqual({ kind: "Accepted" });
    expect(fixture.signalWorkflow.duplicate).toEqual({ kind: "Duplicate" });
    expect(
      decodePayload(
        payloadRefFromJson<{ readonly orderId: string }>(fixture.signalWorkflow.request.payload)
      )
    ).toEqual({ orderId: "o-1" });

    expect(fixture.activityCompletion.completed).toEqual({
      kind: "Completed",
      eventId: 11
    });
    expect(fixture.activityCompletion.batchOutcome.results.map((result) => result.kind)).toEqual([
      "Completed",
      "StaleLease",
      "NotFound"
    ]);
    expect(fixture.activityCompletion.failed).toEqual({ kind: "Failed", eventId: 12 });
    expect(fixture.activityCompletion.retryScheduled).toEqual({
      kind: "RetryScheduled",
      attempt: 2,
      readyAtMs: 1_781_821_485_000
    });

    expect(fixture.timers.outcome).toEqual({ fired: 1 });
    expect(fixture.queryWorkflow.found.kind).toBe("Found");
    if (fixture.queryWorkflow.found.kind === "Found") {
      expect(decodePayload(payloadRefFromJson(fixture.queryWorkflow.found.projection))).toEqual({
        orderId: "o-1"
      });
    }
    expect(fixture.queryWorkflow.notFound).toEqual({ kind: "NotFound" });
    expect(fixture.queryWorkflow.noProjection).toEqual({ kind: "NoProjection" });
    expect(fixture.childDispatch.outcome).toEqual({ dispatched: 1 });

    expect(fixture.payloadRoots.map((root) => root.kind)).toEqual([
      "Payload",
      "ActivityMapInputManifest"
    ]);
    expect(fixture.payloadRoots.map((root) => payloadRefFromJson(root.payload).kind)).toEqual([
      "Blob",
      "Blob"
    ]);
    expect(fixture.payloadGarbageCollection.request).toEqual({ dryRun: true });
    expect(fixture.payloadGarbageCollection.rustProviderOutcome).toEqual({
      scannedBlobs: 3,
      retainedBlobs: 2,
      deletedBlobs: 0
    });
    expect(fixture.payloadGarbageCollection.typescriptPlan).toMatchObject({
      retainedCount: 2,
      unreachableCount: 1
    });
    expect(fixture.payloadGarbageCollection.typescriptCollectOutcome).toMatchObject({
      deletedUris: [],
      deletedCount: 0
    });
  });
});

function loadFixture(): CoreContractFixture {
  const fixtureUrl = new URL("../../../fixtures/contract/core-events.json", import.meta.url);
  return JSON.parse(readFileSync(fixtureUrl, "utf8")) as CoreContractFixture;
}

function loadProviderFixture(): ProviderIoFixture {
  const fixtureUrl = new URL("../../../fixtures/contract/provider-io.json", import.meta.url);
  return JSON.parse(readFileSync(fixtureUrl, "utf8")) as ProviderIoFixture;
}

function decodeFixturePayload<T>(json: PayloadRefJson): T {
  return decodePayload(payloadRefFromJson<T>(json));
}

function requiredAt<T>(values: readonly T[], index: number): T {
  const value = values[index];
  expect(value).toBeDefined();
  return value as T;
}
