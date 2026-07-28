/**
 * The TypeScript half of the shared **behavioural** contract corpus.
 *
 * The corpus is checked in at
 * `typescript/fixtures/contract/behavioral-corpus.json` and read by two
 * runners — this one and `tests/behavioral_corpus.rs`.
 *
 * `fixtures.test.ts` pins vocabulary: event type names, fingerprint helpers,
 * payload-ref shapes, provider request and outcome names. Two runtimes can
 * agree on every one of those and still disagree about what a workflow task
 * *does*. This file pins that: each case is
 * `(workflow program, input history, live signals, now) -> commit`, the
 * expected commit is checked-in literal data, and the case is executed through
 * the real `Worker` with the commit captured by a recording backend rather
 * than rebuilt from runtime state.
 *
 * Fields that are genuinely language-local (a payload's `schemaFingerprint`, a
 * retry policy beyond `maxAttempts`, an activity `optionsDigest`, a wait id's
 * string format, `SelectWinner.branchesDigest`) are projected into a neutral
 * form by `commitJson` below and declared in the corpus's `exclusions` list
 * with the reason and the `kind` of exclusion it is. Behaviour no case reaches
 * is listed separately under `declaredGaps`, because "cannot be compared" and
 * "is not compared" are different claims.
 *
 * Where the two runtimes genuinely *behave* differently, the step carries a
 * `divergence` block: both observed commits, plus a `differences` list naming
 * every field path that differs and why. The meta-test diffs the two recorded
 * commits itself and requires the declared paths to be exactly the set that
 * differs, so a block cannot explain one cause while a second rides along
 * unlabelled.
 *
 * There is no regeneration path here on purpose. The Rust runner can rewrite
 * the corpus under `DURUST_CORPUS_REGENERATE=1`; this runner can only satisfy
 * it or fail, which is what makes the file a cross-implementation assertion
 * rather than a snapshot of one implementation.
 */
import { appendFileSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  Client,
  MemoryBackend,
  Registry,
  Worker,
  activity,
  activityMap,
  activityMapManifest,
  callActivity,
  childWorkflow,
  continueAsNew,
  decodeActivityMapResults,
  decodePayload,
  deprecatePatch,
  eventId,
  getVersion,
  namespace,
  publish,
  readMapManifestItems,
  select,
  sideEffect,
  signal,
  signalId,
  sleep,
  workflow,
  type ActivityMapInputPage,
  type ActivityTask,
  type ActivityMapTask,
  type ChildWorkflowStartRequested,
  type CommandFingerprint,
  type CommandId,
  type DurableFailure,
  type HistoryEventData,
  type NewHistoryEvent,
  type PagedManifest,
  type PayloadRef,
  type RetryPolicy,
  type RunId,
  type WaitRecord,
  type WorkflowDefinition,
  type WorkflowTaskClaim,
  type WorkflowTaskCommit,
  type CommitOutcome
} from "@durust/core";
import { fnv1a32, maintenanceJitterSource } from "../src/worker.js";
import { workerFixture } from "./support.js";

// ---------------------------------------------------------------------------
// The program catalogue.
//
// Each program exists once per language with the same name and the same
// observable behaviour. The corpus names a program; it never carries code.
// ---------------------------------------------------------------------------

const CORPUS_WORKFLOW_QUEUE = "corpus-workflows";
const CORPUS_ACTIVITY_QUEUE = "corpus-activities";

interface Value1 {
  readonly value: number;
}

interface Text1 {
  readonly text: string;
}

interface Continue1 {
  readonly remaining: number;
  readonly total: number;
}

interface Approval {
  readonly who: string;
}

interface CorpusQueryView {
  readonly status: string;
  readonly value: number;
}

const corpusDouble = activity({
  name: "corpus.double",
  handler: async (input: Value1): Promise<Value1> => ({ value: input.value * 2 })
});

const corpusChildIncrement = workflow({
  name: "corpus.child-increment",
  version: 1,
  handler: async (input: Value1): Promise<Value1> => ({ value: input.value + 1 })
});

const corpusActivityThenReturn = workflow({
  name: "corpus.activity-then-return",
  version: 1,
  handler: async (input: Value1): Promise<Value1> => {
    const doubled = await callActivity(corpusDouble, { value: input.value }, {
      taskQueue: CORPUS_ACTIVITY_QUEUE
    });
    return { value: doubled.value };
  }
});

const corpusSpawnSleepThenActivity = workflow({
  name: "corpus.spawn-sleep-then-activity",
  version: 1,
  handler: async (input: Value1): Promise<Value1> => {
    const spawned = await callActivity(corpusDouble, { value: input.value }, {
      taskQueue: CORPUS_ACTIVITY_QUEUE
    }).spawn();
    await sleep(1_000);
    const second = await callActivity(corpusDouble, { value: input.value + 1 }, {
      taskQueue: CORPUS_ACTIVITY_QUEUE
    });
    const first = await spawned.result();
    return { value: first.value + second.value };
  }
});

const corpusApproveSignal = signal<Approval>("corpus.approve");

const corpusSelectSignalOrTimer = workflow({
  name: "corpus.select-signal-or-timer",
  version: 1,
  handler: async (input: Value1): Promise<Text1> => {
    const winner = await select({
      approved: corpusApproveSignal,
      elapsed: sleep(input.value)
    });
    return {
      text: winner.branch === "approved" ? `signal:${(winner.value as Approval).who}` : "timer"
    };
  }
});

/**
 * The two-ready-branch select. Both branches resolve from real, distinct
 * history events, so a case can put either branch's event first and the winner
 * is decided purely by ready event id — which is what makes the tie-break rule
 * assertable at all. A select with one ready branch pins nothing: every
 * comparator returns the same winner.
 */
const corpusSelectActivityOrTimer = workflow({
  name: "corpus.select-activity-or-timer",
  version: 1,
  handler: async (input: Value1): Promise<Text1> => {
    const winner = await select({
      doubled: callActivity(corpusDouble, { value: input.value }, {
        taskQueue: CORPUS_ACTIVITY_QUEUE
      }),
      elapsed: sleep(1_000)
    });
    return {
      text: winner.branch === "doubled" ? `activity:${(winner.value as Value1).value}` : "timer"
    };
  }
});

const corpusSignalThenPublish = workflow({
  name: "corpus.signal-then-publish",
  version: 1,
  queryStateType: undefined as unknown as CorpusQueryView,
  handler: async (input: Value1): Promise<Value1> => {
    publish<CorpusQueryView>({ status: "waiting", value: input.value });
    const approved = await corpusApproveSignal;
    publish<CorpusQueryView>({ status: approved.who, value: input.value + 1 });
    return { value: input.value + 1 };
  }
});

/**
 * Schedules its timer in a task that runs *after* the virtual clock has moved,
 * which is the only shape that exposes the `nowMs` divergence: this worker
 * never supplies `PrepareWorkflowTaskOptions.nowMs`, so its executions compute
 * `fireAt` from 0 forever.
 */
const corpusSleepAfterSignal = workflow({
  name: "corpus.sleep-after-signal",
  version: 1,
  handler: async (input: Value1): Promise<Value1> => {
    const approved = await corpusApproveSignal;
    await sleep(input.value);
    return { value: input.value + approved.who.length };
  }
});

const corpusChildAwait = workflow({
  name: "corpus.child-await",
  version: 1,
  handler: async (input: Value1): Promise<Value1> => {
    const child = await childWorkflow(
      corpusChildIncrement,
      { value: input.value },
      { workflowId: "wf/corpus/child", taskQueue: CORPUS_WORKFLOW_QUEUE }
    ).spawn();
    const result = await child.result();
    return { value: result.value };
  }
});

const corpusActivityMap = workflow({
  name: "corpus.activity-map",
  version: 1,
  handler: async (input: Value1): Promise<Value1> => {
    const items: Value1[] = [];
    for (let index = 0; index < input.value; index += 1) {
      items.push({ value: index + 1 });
    }
    const mapped = activityMap(corpusDouble, {
      inputManifest: activityMapManifest(items),
      resultManifest: "doubled",
      taskQueue: CORPUS_ACTIVITY_QUEUE,
      maxInFlight: 2
    });
    const manifest = await mapped.resultManifest();
    const results = decodeActivityMapResults<Value1>(manifest);
    return { value: results.reduce((total, result) => total + result.value, 0) };
  }
});

const corpusMarkers = workflow({
  name: "corpus.markers",
  version: 1,
  handler: async (input: Value1): Promise<Value1> => {
    const version = getVersion("corpus.change", 1, 1);
    const tagged = await sideEffect<Value1>("corpus.tag", () => ({ value: input.value + 100 }));
    deprecatePatch("corpus.retired");
    return { value: tagged.value + Math.max(version, 0) };
  }
});

const corpusContinueAsNew = workflow({
  name: "corpus.continue-as-new",
  version: 1,
  handler: async (input: Continue1): Promise<Value1> => {
    if (input.remaining > 0) {
      return continueAsNew<Continue1>({
        remaining: input.remaining - 1,
        total: input.total + 1
      });
    }
    return { value: input.total };
  }
});

// Typed as the `Registry` itself types a heterogeneous set of workflows.
// `ReturnType<typeof workflow>` resolved the generic's `Input` to `never`, so
// the map's own values were not assignable to `registerWorkflow`, and every
// entry needed an `as never` to be stored at all.
const PROGRAMS = new Map<string, WorkflowDefinition<any, any, any, string>>([
  ["corpus.activity-then-return", corpusActivityThenReturn],
  ["corpus.spawn-sleep-then-activity", corpusSpawnSleepThenActivity],
  ["corpus.select-signal-or-timer", corpusSelectSignalOrTimer],
  ["corpus.select-activity-or-timer", corpusSelectActivityOrTimer],
  ["corpus.signal-then-publish", corpusSignalThenPublish],
  ["corpus.sleep-after-signal", corpusSleepAfterSignal],
  ["corpus.child-await", corpusChildAwait],
  ["corpus.activity-map", corpusActivityMap],
  ["corpus.markers", corpusMarkers],
  ["corpus.continue-as-new", corpusContinueAsNew]
]);

// ---------------------------------------------------------------------------
// Neutral projection of a commit. Mirrors `commit_json` in
// `tests/behavioral_corpus.rs` field for field.
// ---------------------------------------------------------------------------

const SCHEMA_PLACEHOLDER = "<schema-fingerprint>";
const ACTIVITY_OPTIONS_PLACEHOLDER = "<activity-options-digest>";
const SELECT_BRANCHES_PLACEHOLDER = "<select-branches-digest>";
const MANIFEST_DIGEST_PLACEHOLDER = "<manifest-digest>";

type Json = unknown;

function payloadJson(payload: PayloadRef): Json {
  return {
    codec: payload.codec,
    schemaFingerprint: SCHEMA_PLACEHOLDER,
    value: decodePayload<unknown>(payload)
  };
}

function optionalPayloadJson(payload: PayloadRef | undefined | null): Json {
  return payload === undefined || payload === null ? null : payloadJson(payload);
}

function activityMapManifestJson(payload: PayloadRef): Json {
  // One ref, one type. The manifest read and the item read are the same
  // payload seen through the same shape, and `readMapManifestItems` infers its
  // `Page` from the accessor below, so the ref has to be the manifest type
  // that accessor implies rather than a hand-written subset of it.
  const manifestRef = payload as PayloadRef<PagedManifest<ActivityMapInputPage<Value1>>>;
  const manifest = decodePayload(manifestRef);
  const items = readMapManifestItems(
    manifestRef,
    (page: ActivityMapInputPage<Value1>) => page.items,
    "activity map input manifest"
  );
  return {
    itemCount: manifest.itemCount,
    pageLengths: [...manifest.pageLengths],
    items: items.map((item) => payloadJson(item as PayloadRef))
  };
}

function commandIdJson(commandId: CommandId): Json {
  return { runId: String(commandId.runId), seq: Number(commandId.seq) };
}

function retryPolicyJson(policy: RetryPolicy): Json {
  return { maxAttempts: policy.maxAttempts };
}

function fingerprintJson(fingerprint: CommandFingerprint): Json {
  const optionsDigest =
    fingerprint.kind === "Activity" || fingerprint.kind === "ActivityMap"
      ? ACTIVITY_OPTIONS_PLACEHOLDER
      : fingerprint.optionsDigest;
  // A map fingerprint's input digest hashes the *manifest* bytes, and the
  // manifest wire encoding is itself excluded; the manifest's content is
  // asserted separately by `activityMapManifestJson`.
  const inputDigest =
    fingerprint.kind === "ActivityMap" || fingerprint.kind === "ChildWorkflowMap"
      ? MANIFEST_DIGEST_PLACEHOLDER
      : fingerprint.inputDigest;
  return {
    kind: fingerprint.kind,
    name: fingerprint.name,
    inputDigest,
    optionsDigest
  };
}

function failureJson(failure: DurableFailure): Json {
  return {
    errorType: failure.errorType,
    message: failure.message,
    nonRetryable: failure.nonRetryable,
    details: optionalPayloadJson(failure.details)
  };
}

function eventJson(data: HistoryEventData): Json {
  switch (data.kind) {
    case "WorkflowCompleted":
      return { type: "WorkflowCompleted", result: payloadJson(data.result) };
    case "WorkflowFailed":
      return { type: "WorkflowFailed", failure: failureJson(data.failure) };
    case "WorkflowCancelled":
      return { type: "WorkflowCancelled", reason: data.reason };
    case "WorkflowContinuedAsNew":
      return { type: "WorkflowContinuedAsNew", input: payloadJson(data.input) };
    case "ActivityScheduled":
      return {
        type: "ActivityScheduled",
        commandId: commandIdJson(data.scheduled.commandId),
        activityName: String(data.scheduled.activityName),
        taskQueue: String(data.scheduled.taskQueue),
        retryPolicy: retryPolicyJson(data.scheduled.retryPolicy),
        startToCloseTimeoutMs: data.scheduled.startToCloseTimeoutMs,
        heartbeatTimeoutMs: data.scheduled.heartbeatTimeoutMs,
        input: payloadJson(data.scheduled.input),
        fingerprint: fingerprintJson(data.scheduled.fingerprint)
      };
    case "ActivityMapScheduled":
      return {
        type: "ActivityMapScheduled",
        commandId: commandIdJson(data.scheduled.commandId),
        activityName: String(data.scheduled.activityName),
        taskQueue: String(data.scheduled.taskQueue),
        retryPolicy: retryPolicyJson(data.scheduled.retryPolicy),
        startToCloseTimeoutMs: data.scheduled.startToCloseTimeoutMs,
        heartbeatTimeoutMs: data.scheduled.heartbeatTimeoutMs,
        inputManifest: activityMapManifestJson(data.scheduled.inputManifest),
        resultManifestName: data.scheduled.resultManifestName,
        maxInFlight: data.scheduled.maxInFlight,
        fingerprint: fingerprintJson(data.scheduled.fingerprint)
      };
    case "ChildWorkflowStartRequested":
      return {
        type: "ChildWorkflowStartRequested",
        commandId: commandIdJson(data.requested.commandId),
        workflowType: {
          name: data.requested.workflowType.name,
          version: data.requested.workflowType.version
        },
        workflowId: String(data.requested.workflowId),
        taskQueue: String(data.requested.taskQueue),
        input: payloadJson(data.requested.input),
        parentClosePolicy: data.requested.parentClosePolicy,
        fingerprint: fingerprintJson(data.requested.fingerprint)
      };
    case "TimerStarted":
      return {
        type: "TimerStarted",
        commandId: commandIdJson(data.started.commandId),
        fireAtMs: Number(data.started.fireAt),
        fingerprint: fingerprintJson(data.started.fingerprint)
      };
    case "SignalConsumed":
      return {
        type: "SignalConsumed",
        commandId: commandIdJson(data.consumed.commandId),
        signalId: String(data.consumed.signalId),
        signalName: String(data.consumed.signalName),
        payload: payloadJson(data.consumed.payload),
        fingerprint: fingerprintJson(data.consumed.fingerprint)
      };
    case "SelectWinner":
      return {
        type: "SelectWinner",
        selectCommandId: commandIdJson(data.winner.selectCommandId),
        branchOrdinal: data.winner.branchOrdinal,
        winningEventId: Number(data.winner.winningEventId),
        branchesDigest: SELECT_BRANCHES_PLACEHOLDER
      };
    case "VersionMarker":
      return {
        type: "VersionMarker",
        commandId: commandIdJson(data.marker.commandId),
        changeId: data.marker.changeId,
        version: data.marker.version
      };
    case "DeprecatedPatchMarker":
      return {
        type: "DeprecatedPatchMarker",
        commandId: commandIdJson(data.marker.commandId),
        patchId: data.marker.patchId
      };
    case "SideEffectMarker":
      return {
        type: "SideEffectMarker",
        commandId: commandIdJson(data.marker.commandId),
        key: data.marker.key,
        value: payloadJson(data.marker.value)
      };
    default:
      throw new Error(
        `the behavioural corpus has no neutral encoding for ${data.kind}; add one rather than ` +
          "letting the case pass on a partial comparison"
      );
  }
}

function waitJson(wait: WaitRecord): Json {
  return {
    kind: wait.kind,
    commandId: commandIdJson(wait.commandId),
    key: wait.key,
    readyAtMs: wait.readyAt === null ? null : Number(wait.readyAt)
  };
}

/**
 * `deleteWaits` carries bare ids, so the runner parses its own format —
 * `{runId}:timer:{seq}` / `{runId}:signal:{seq}`, which is *not* Rust's
 * `{runId}:{seq}:timer` — back into the neutral triple `upsertWaits` reports.
 */
function waitIdJson(waitId: string): Json {
  const match = /^(.*):(timer|signal):(\d+)$/.exec(waitId);
  if (match === null) {
    throw new Error(`unrecognised TypeScript wait id \`${waitId}\``);
  }
  return {
    kind: match[2] === "timer" ? "Timer" : "Signal",
    commandId: { runId: match[1], seq: Number(match[3]) }
  };
}

function activityTaskJson(task: ActivityTask): Json {
  return {
    activityId: task.activityId,
    runId: String(task.runId),
    commandId: commandIdJson(task.commandId),
    activityName: String(task.activityName),
    taskQueue: String(task.taskQueue),
    retryPolicy: retryPolicyJson(task.retryPolicy),
    startToCloseTimeoutMs: task.startToCloseTimeoutMs,
    heartbeatTimeoutMs: task.heartbeatTimeoutMs,
    attempt: task.attempt,
    input: payloadJson(task.input),
    mapItem:
      task.mapItem === null
        ? null
        : {
            mapCommandId: commandIdJson(task.mapItem.mapCommandId),
            itemOrdinal: task.mapItem.itemOrdinal
          }
  };
}

function activityMapTaskJson(task: ActivityMapTask): Json {
  return {
    mapCommandId: commandIdJson(task.mapCommandId),
    activityName: String(task.activityName),
    taskQueue: String(task.taskQueue),
    retryPolicy: retryPolicyJson(task.retryPolicy),
    startToCloseTimeoutMs: task.startToCloseTimeoutMs,
    heartbeatTimeoutMs: task.heartbeatTimeoutMs,
    inputManifest: activityMapManifestJson(task.inputManifest),
    resultManifestName: task.resultManifestName,
    maxInFlight: task.maxInFlight
  };
}

function childStartJson(requested: ChildWorkflowStartRequested): Json {
  return {
    commandId: commandIdJson(requested.commandId),
    workflowType: {
      name: requested.workflowType.name,
      version: requested.workflowType.version
    },
    workflowId: String(requested.workflowId),
    taskQueue: String(requested.taskQueue),
    input: payloadJson(requested.input),
    parentClosePolicy: requested.parentClosePolicy
  };
}

function commitJson(commit: WorkflowTaskCommit): Json {
  if ((commit.scheduleChildWorkflowMaps?.length ?? 0) > 0) {
    throw new Error("no corpus case schedules a child workflow map yet");
  }
  // Mirrors the Rust runner's `cancel_commands.is_empty()` panic, and makes the
  // `WorkflowTaskCommit.cancelCommands` exclusion an assertion on *both* sides.
  // Until `cancelCommands` existed in TypeScript at all this side had nothing to
  // check, so the exclusion was declared as an assertion that only one runner
  // made. If a case ever starts cancelling a command the corpus must gain a
  // projection for it rather than silently dropping the field.
  if ((commit.cancelCommands?.length ?? 0) > 0) {
    throw new Error("no corpus case cancels a command yet");
  }
  return {
    expectedTailEventId: Number(commit.expectedTailEventId),
    appendEvents: (commit.appendEvents ?? []).map((event: NewHistoryEvent) => eventJson(event.data)),
    upsertWaits: (commit.upsertWaits ?? []).map(waitJson),
    deleteWaits: (commit.deleteWaits ?? []).map((waitId) => waitIdJson(String(waitId))),
    consumeSignals: (commit.consumeSignals ?? []).map((id) => String(id)),
    scheduleActivities: (commit.scheduleActivities ?? []).map(activityTaskJson),
    scheduleActivityMaps: (commit.scheduleActivityMaps ?? []).map(activityMapTaskJson),
    startChildWorkflows: (commit.startChildWorkflows ?? []).map(childStartJson),
    queryProjection: optionalPayloadJson(commit.queryProjection)
  };
}

// ---------------------------------------------------------------------------
// The recording backend and the virtual clock.
// ---------------------------------------------------------------------------

class VirtualClock {
  nowMs = 0;

  advance(ms: number): void {
    this.nowMs += ms;
  }
}

class RecordingBackend extends MemoryBackend {
  readonly commits: WorkflowTaskCommit[] = [];

  override async commitWorkflowTask(
    claim: WorkflowTaskClaim,
    commit: WorkflowTaskCommit
  ): Promise<CommitOutcome> {
    this.commits.push(commit);
    return super.commitWorkflowTask(claim, commit);
  }

  takeCommits(): WorkflowTaskCommit[] {
    return this.commits.splice(0, this.commits.length);
  }
}

// ---------------------------------------------------------------------------
// The corpus file.
// ---------------------------------------------------------------------------

const CORPUS_PATH = fileURLToPath(
  new URL("../../../fixtures/contract/behavioral-corpus.json", import.meta.url)
);

interface Corpus {
  readonly note: readonly string[];
  readonly exclusions: readonly {
    readonly what: string;
    readonly kind: string;
    readonly why: string;
  }[];
  readonly declaredDivergences: { readonly count: number; readonly why: string };
  readonly declaredGaps: readonly { readonly what: string; readonly why: string }[];
  readonly workerStartJitter: {
    readonly note: readonly string[];
    readonly cases: readonly {
      readonly workerId: string;
      readonly seed: number;
      readonly unitsScaled: readonly number[];
    }[];
  };
  readonly cases: readonly CorpusCase[];
}

interface CorpusCase {
  readonly name: string;
  readonly program: string;
  readonly workflowId: string;
  readonly input: Record<string, unknown>;
  readonly steps: readonly CorpusStep[];
}

interface CorpusStep {
  readonly action: string;
  readonly historyBefore?: readonly string[];
  readonly expect?: Json;
  readonly divergence?: {
    readonly differences: readonly { readonly what: string; readonly paths: readonly string[] }[];
    readonly rust: Json;
    readonly typescript: Json;
  };
  readonly ms?: number;
  readonly count?: number;
  readonly name?: string;
  readonly signalId?: string;
  readonly who?: string;
  readonly historyChunkEvents?: number;
}

const corpus = JSON.parse(readFileSync(CORPUS_PATH, "utf8")) as Corpus;

/** See the `DURUST_CORPUS_PRINT` note in the workflow-task step below. */
const PRINT_ONLY = process.env.DURUST_CORPUS_PRINT === "1";

/**
 * The projections and scope statements the runners implement, each with the
 * `kind` that says which it is: something `commitJson` actively rewrites
 * (`Projection`), something a runner actively checks (`Assertion`), or a
 * statement about what the corpus is (`Scope`). Anything the corpus simply
 * does not reach lives in `DECLARED_GAPS` instead.
 */
const DECLARED_EXCLUSIONS: readonly (readonly [string, string])[] = [
  ["PayloadRef.schemaFingerprint", "Projection"],
  ["CommandFingerprint.optionsDigest for the Activity and ActivityMap kinds", "Projection"],
  ["RetryPolicy beyond maxAttempts, including every retry delay value", "Projection"],
  ["SelectWinner.branchesDigest", "Projection"],
  ["WaitId string format", "Projection"],
  ["The activity-map manifest wire encoding", "Projection"],
  ["WorkflowTaskCommit.cancelCommands and the ParentCancelled event", "Assertion"],
  ["The execution mechanism", "Scope"]
];

/**
 * Every case this corpus holds, in order, asserted by name in both runners —
 * the same treatment {@link DECLARED_EXCLUSIONS} gets, and for the same reason.
 *
 * This runner had **no** case-count assertion at all, and the Rust runner had
 * only a floor (`cases.len() >= 12` against 13 cases). Measured before this
 * list existed: deleting cases one at a time, 8 of the 13 left the suite green
 * in both languages, including all four `select` cases — the ones whose
 * convergence this corpus was extended to pin. A case that can be deleted
 * without failing anything is a case that is not protecting anything.
 */
const DECLARED_CASES = [
  "an activity call commits one scheduled activity, and its completion closes the run",
  "a later command is appended past an unconsumed completion (hot execution)",
  "the same commits come out of a cold replay in one-event chunks",
  "a signal that arrives first wins the select, and the losing timer wait is cancelled",
  "a timer that fires first wins the select, and the losing signal wait is cancelled",
  "a select resolved with two ready branches takes the earlier ready event, not the earlier branch",
  "the same select with the activity landing first takes the activity branch",
  "a query projection is committed with the signal wait and again with its consumption",
  "a child workflow start is committed to the outbox and its completion closes the parent",
  "an activity map commits one scheduled map bounded by maxInFlight",
  "a version marker, a side effect and a deprecated patch land in one commit with the result",
  "continue-as-new commits the next run's input and nothing else",
  "a timer scheduled after the clock has moved records its deadline relative to now"
];

const DECLARED_GAPS = [
  "Double-await of a handle",
  "Offloaded (blob) payload refs",
  "Child workflow maps",
  "Workflow failure and cancellation events",
  "Activity retries",
  "Terminal-with-leftover-command divergence detection",
  "The select tie-break between two branches ready at the same event id",
  "Providers other than the two in-memory ones"
];

/**
 * Leaf-level structural diff of two commits, mirrored by `diff_paths` in the
 * Rust runner. A length or key-set mismatch reports the container and stops
 * descending, so one structural difference is one path rather than a cascade.
 *
 * The two implementations agree on every structural shape and disagree on
 * exactly one class: numeric *representation*. Rust compares
 * `serde_json::Number`, which distinguishes `1.0` from `1`, `-0.0` from `0`,
 * and `9007199254740993` from `9007199254740992`; this side compares
 * `JSON.stringify` output after `JSON.parse` has already collapsed each of
 * those pairs, so it reports no difference. Rust's path set is therefore a
 * *superset* in every disagreement, which is what makes the split safe:
 * declare the extra path and this runner rejects it as spurious, omit it and
 * Rust rejects the omission, so one runner always goes red and
 * `check:fixtures` runs both. Unreachable today — every number in every commit
 * is an integer inside the double-safe range.
 */
function diffPaths(left: Json, right: Json, path: string, out: string[]): void {
  const isPlainObject = (value: Json): value is Record<string, Json> =>
    typeof value === "object" && value !== null && !Array.isArray(value);
  if (isPlainObject(left) && isPlainObject(right)) {
    const leftKeys = Object.keys(left).sort();
    const rightKeys = Object.keys(right).sort();
    if (JSON.stringify(leftKeys) !== JSON.stringify(rightKeys)) {
      out.push(path);
      return;
    }
    for (const key of leftKeys) {
      diffPaths(left[key], right[key], path === "" ? key : `${path}.${key}`, out);
    }
    return;
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    if (left.length !== right.length) {
      out.push(path);
      return;
    }
    left.forEach((item, index) => {
      diffPaths(item, (right as Json[])[index], `${path}[${index}]`, out);
    });
    return;
  }
  if (JSON.stringify(left) !== JSON.stringify(right)) {
    out.push(path);
  }
}

// ---------------------------------------------------------------------------
// The runner.
// ---------------------------------------------------------------------------

function buildRegistry(): Registry {
  const registry = new Registry();
  for (const definition of PROGRAMS.values()) {
    registry.registerWorkflow(definition);
  }
  registry.registerWorkflow(corpusChildIncrement as never);
  registry.registerActivity(corpusDouble);
  return registry;
}

function buildWorker(backend: RecordingBackend, historyChunkEvents?: number): Worker {
  return workerFixture(backend, buildRegistry(), {
    workerId: "corpus-worker",
    workflowTaskQueue: CORPUS_WORKFLOW_QUEUE,
    activityTaskQueue: CORPUS_ACTIVITY_QUEUE,
    registeredSignalNames: ["corpus.approve"],
    ...(historyChunkEvents === undefined ? {} : { historyFetchMaxEvents: historyChunkEvents })
  });
}

async function historyTypes(backend: RecordingBackend, runId: RunId): Promise<string[]> {
  const chunk = await backend.streamHistory({
    runId,
    afterEventId: eventId(0),
    upToEventId: eventId(Number.MAX_SAFE_INTEGER),
    maxEvents: 1_000,
    maxBytes: Number.MAX_SAFE_INTEGER
  });
  return chunk.events.map((event) => String(event.eventType));
}

async function runCase(corpusCase: CorpusCase): Promise<void> {
  const clock = new VirtualClock();
  const backend = new RecordingBackend({ nowMs: () => clock.nowMs });
  const client = new Client(backend, { namespace: namespace() });
  const definition = PROGRAMS.get(corpusCase.program);
  if (definition === undefined) {
    throw new Error(`the corpus names an unknown program \`${corpusCase.program}\``);
  }
  const handle = await client.startWorkflow(
    definition as never,
    corpusCase.workflowId,
    CORPUS_WORKFLOW_QUEUE,
    corpusCase.input as never
  );
  const runId = handle.runId;
  let worker = buildWorker(backend);

  for (const [index, step] of corpusCase.steps.entries()) {
    const where = `${corpusCase.name} step ${index}`;
    switch (step.action) {
      case "workflowTask": {
        const before = await historyTypes(backend, runId);
        backend.takeCommits();
        const outcome = await worker.runWorkflowTaskOnce();
        expect(outcome.kind, `${where}: expected a claimable workflow task`).toBe("Committed");
        const commits = backend.takeCommits();
        expect(commits.length, `${where}: expected exactly one commit`).toBe(1);
        const actual = commitJson(commits[0] as WorkflowTaskCommit);
        if (PRINT_ONLY) {
          // Read-only, and never on in CI. There is no path in this file that
          // writes the corpus; this exists so a maintainer filling in a
          // `divergence.typescript` block has the observed value to paste,
          // without giving the TypeScript side a regeneration path. It skips
          // the assertions so one run reports every step rather than stopping
          // at the first mismatch.
          appendFileSync(
            process.env.DURUST_CORPUS_PRINT_FILE ?? "corpus-observed.jsonl",
            `${JSON.stringify({ where, before, actual })}\n`
          );
          break;
        }
        expect(before, `${where}: input history`).toEqual([...(step.historyBefore ?? [])]);
        const expected =
          step.divergence === undefined ? step.expect : step.divergence.typescript;
        expect(actual, `${where}: commit`).toEqual(expected);
        break;
      }
      case "otherWorkflowTask": {
        const outcome = await worker.runWorkflowTaskOnce();
        expect(outcome.kind, `${where}: expected a claimable workflow task`).toBe("Committed");
        backend.takeCommits();
        break;
      }
      case "runActivity": {
        const outcome = await worker.runActivityTaskOnce();
        expect(outcome.kind, `${where}: expected a claimable activity task`).toBe("Completed");
        break;
      }
      case "dispatchChildStarts": {
        // Rust queues child starts in a provider outbox that a worker loop
        // drains; TypeScript starts them inside the commit, so there is
        // nothing to dispatch. The step exists so the two scripts stay
        // identical.
        break;
      }
      case "advanceTime": {
        clock.advance(step.ms ?? 0);
        break;
      }
      case "fireTimers": {
        const outcome = await backend.fireDueTimers({
          namespace: namespace(),
          now: clock.nowMs,
          limit: 16
        });
        expect(outcome.fired, `${where}: fired timers`).toBe(step.count);
        break;
      }
      case "signal": {
        await backend.signalWorkflow({
          namespace: namespace(),
          workflowId: corpusCase.workflowId,
          signalId: signalId(step.signalId as string),
          signalName: step.name as string,
          payload: (await import("@durust/core")).encodePayload<Approval>({
            who: step.who as string
          })
        });
        break;
      }
      case "restartWorker": {
        worker = buildWorker(backend, step.historyChunkEvents);
        break;
      }
      default:
        throw new Error(`${where}: unknown corpus action \`${step.action}\``);
    }
  }
}

describe("behavioural contract corpus", () => {
  it("accounts for every differing field in every divergence", () => {
    let count = 0;
    for (const corpusCase of corpus.cases) {
      for (const [index, step] of corpusCase.steps.entries()) {
        if (step.divergence === undefined) {
          continue;
        }
        count += 1;
        const where = `${corpusCase.name} step ${index}`;
        expect(step.expect, `${where} carries both expect and divergence`).toBe(undefined);
        expect(
          step.divergence.differences.length,
          `${where}: no differences declared`
        ).toBeGreaterThan(0);
        const declared = new Set<string>();
        for (const difference of step.divergence.differences) {
          expect(
            difference.what.length,
            `${where}: difference \`${difference.what}\` needs a reason`
          ).toBeGreaterThan(120);
          expect(
            difference.paths.length,
            `${where}: difference \`${difference.what}\` names no field paths`
          ).toBeGreaterThan(0);
          for (const path of difference.paths) {
            declared.add(path);
          }
        }
        const actual: string[] = [];
        diffPaths(step.divergence.rust, step.divergence.typescript, "", actual);
        expect(
          actual.length,
          `${where}: records a divergence whose two sides are equal`
        ).toBeGreaterThan(0);
        expect(
          [...actual].sort(),
          `${where}: the declared difference paths must be exactly the paths that differ`
        ).toEqual([...declared].sort());
      }
    }
    // The count is declared in the corpus and asserted equal, not asserted
    // non-zero. `count > 0` encoded the assumption that some divergence would
    // always remain, which stops being true the moment the last one is
    // converged away — and it would have had to be edited under exactly the
    // pressure that makes a careless edit likely. Equality against a checked-in
    // number keeps both directions loud: deleting a block fails here, adding
    // one without declaring it fails here, and reaching zero is possible only
    // by editing the corpus on purpose.
    expect(
      corpus.declaredDivergences.why.length,
      "declaredDivergences needs a reason, not a label"
    ).toBeGreaterThan(80);
    expect(
      count,
      "the corpus records a different number of divergence blocks than it declares"
    ).toBe(corpus.declaredDivergences.count);
  });

  it("declares its cross-runtime exclusions", () => {
    expect(corpus.exclusions.map((exclusion) => [exclusion.what, exclusion.kind])).toEqual(
      DECLARED_EXCLUSIONS.map((entry) => [...entry])
    );
    for (const exclusion of corpus.exclusions) {
      expect(
        exclusion.why.length,
        `exclusion \`${exclusion.what}\` needs a reason`
      ).toBeGreaterThan(80);
    }
  });

  it("declares its cases", () => {
    expect(
      corpus.cases.map((corpusCase) => corpusCase.name),
      "a case cannot be added, removed, or renamed without saying so here and in the Rust runner's DECLARED_CASES"
    ).toEqual(DECLARED_CASES);
  });

  it("declares its gaps separately from its exclusions", () => {
    expect(corpus.declaredGaps.map((gap) => gap.what)).toEqual(DECLARED_GAPS);
    for (const gap of corpus.declaredGaps) {
      expect(gap.why.length, `gap \`${gap.what}\` needs a reason`).toBeGreaterThan(80);
    }
  });

  it("pins the per-worker maintenance jitter stream", () => {
    expect(corpus.workerStartJitter.cases.length).toBe(7);
    let sawAstral = false;
    for (const jitterCase of corpus.workerStartJitter.cases) {
      sawAstral ||= [...jitterCase.workerId].some((char) => (char.codePointAt(0) ?? 0) > 0xffff);
      expect(fnv1a32(jitterCase.workerId), `seed for \`${jitterCase.workerId}\``).toBe(
        jitterCase.seed
      );
      const next = maintenanceJitterSource(jitterCase.workerId);
      const actual = jitterCase.unitsScaled.map(() => {
        const unit = next();
        expect(unit).toBeGreaterThanOrEqual(0);
        expect(unit).toBeLessThan(1);
        return unit * 4_294_967_296;
      });
      expect(actual, `jitter stream for \`${jitterCase.workerId}\``).toEqual([
        ...jitterCase.unitsScaled
      ]);
    }
    expect(sawAstral, "the table must keep an astral worker id").toBe(true);
  });

  for (const corpusCase of corpus.cases) {
    it(corpusCase.name, async () => {
      await runCase(corpusCase);
    });
  }

  // DURUST_CORPUS_PRINT=1 turns every case into a reporter that asserts
  // nothing. That is useful exactly once — when filling in a
  // `divergence.typescript` block — and dangerous every other time, because a
  // green corpus that asserted nothing is the one outcome this file exists to
  // make impossible. So a print run is never a pass.
  it("is not a print run", () => {
    expect(
      PRINT_ONLY,
      "DURUST_CORPUS_PRINT=1 skips every commit assertion; this run proves nothing"
    ).toBe(false);
  });
});
