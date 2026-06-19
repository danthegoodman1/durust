import { AsyncLocalStorage } from "node:async_hooks";
import type {
  ActivityDefinition,
  ActivityHandle,
  ActivityInput,
  ActivityOutput,
  ActivityMapHandle,
  ActivityMapOptions,
  ActivityMapResultManifest,
  ChildWorkflowMapHandle,
  ChildWorkflowMapOptions,
  ChildWorkflowMapResultManifest,
  ChildWorkflowHandle,
  ChildWorkflowStart,
  DurableFailure,
  DurableBranch,
  DurablePromise,
  SignalDefinition,
  WorkflowDefinition,
  WorkflowInput,
  WorkflowOutput
} from "./api.js";
import type {
  ClaimedWorkflowTask,
  NewHistoryEvent,
  SignalInboxRecord,
  WaitRecord,
  WorkflowTaskCommit
} from "./backend.js";
import {
  activityFingerprint,
  activityMapFingerprint,
  childWorkflowFingerprint,
  childWorkflowMapFingerprint,
  signalFingerprint,
  timerFingerprint
} from "./fingerprint.js";
import {
  activityTaskFromScheduled,
  type ActivityMapScheduled,
  type ActivityMapTask,
  type ActivityScheduled,
  type ActivityTask,
  type ChildWorkflowMapScheduled,
  type ChildWorkflowMapTask,
  type ChildWorkflowStartRequested,
  type TimerStarted
} from "./history.js";
import type { HistoryEvent } from "./history.js";
import { RetryPolicy, type ActivityCallOptions, type ChildWorkflowOptions } from "./options.js";
import { decodePayload, digestBytes, encodePayload, payloadDigest, type CodecId, type PayloadRef, type SchemaAdapter } from "./payload.js";
import { commandId, eventId, timestampMs, waitId, type CommandId, type EventId, type RunId, type WaitId, type WorkflowId } from "./types.js";

const runtimeStorage = new AsyncLocalStorage<WorkflowRuntimeContext>();
let nondeterminismGuardsInstalled = false;
let originalDateNow: (() => number) | undefined;
let originalMathRandom: (() => number) | undefined;
let originalSetTimeout: typeof globalThis.setTimeout | undefined;
let originalSetInterval: typeof globalThis.setInterval | undefined;
let originalQueueMicrotask: typeof globalThis.queueMicrotask | undefined;
let originalPromiseAll: PromiseConstructor["all"] | undefined;
let originalPromiseRace: PromiseConstructor["race"] | undefined;
let originalPromiseAllSettled: PromiseConstructor["allSettled"] | undefined;
let originalPromiseAny: PromiseConstructor["any"] | undefined;

export interface PrepareWorkflowTaskOptions {
  readonly defaultActivityTaskQueue?: string;
  readonly defaultWorkflowTaskQueue?: string;
  readonly payloadCodec?: CodecId;
  readonly nowMs?: number;
  readonly liveSignals?: readonly SignalInboxRecord[];
}

export class WorkflowSuspended extends Error {
  constructor() {
    super("workflow task suspended on durable operation");
    this.name = "WorkflowSuspended";
  }
}

export const DEFAULT_VERSION = -1;

export class UnsupportedWorkflowVersionError extends Error {
  readonly changeId: string;
  readonly version: number;
  readonly minSupported: number;
  readonly maxSupported: number;

  constructor(changeId: string, version: number, minSupported: number, maxSupported: number) {
    super(
      `unsupported workflow version for ${changeId}: recorded ${version}, supported ${minSupported}..${maxSupported}`
    );
    this.name = "UnsupportedWorkflowVersionError";
    this.changeId = changeId;
    this.version = version;
    this.minSupported = minSupported;
    this.maxSupported = maxSupported;
  }
}

export class ActivityFailureError extends Error {
  readonly failure: DurableFailure;

  constructor(failure: DurableFailure) {
    super(failure.message);
    this.name = "ActivityFailureError";
    this.failure = failure;
  }
}

export class WorkflowFailureError extends Error {
  readonly failure: DurableFailure;

  constructor(failure: DurableFailure) {
    super(failure.message);
    this.name = "WorkflowFailureError";
    this.failure = failure;
  }
}

export class WorkflowCancelledError extends Error {
  readonly reason: string;

  constructor(reason: string) {
    super(reason);
    this.name = "WorkflowCancelledError";
    this.reason = reason;
  }
}

export class ChildWorkflowFailureError extends Error {
  readonly failure: DurableFailure;

  constructor(failure: DurableFailure) {
    super(failure.message);
    this.name = "ChildWorkflowFailureError";
    this.failure = failure;
  }
}

export class ChildWorkflowCancelledError extends Error {
  readonly reason: string;

  constructor(reason: string) {
    super(reason);
    this.name = "ChildWorkflowCancelledError";
    this.reason = reason;
  }
}

export class ChildWorkflowMapFailureError extends Error {
  readonly failure: DurableFailure;

  constructor(failure: DurableFailure) {
    super(failure.message);
    this.name = "ChildWorkflowMapFailureError";
    this.failure = failure;
  }
}

export class ContinueAsNewRequested extends Error {
  constructor() {
    super("workflow requested continue-as-new");
    this.name = "ContinueAsNewRequested";
  }
}

export function getVersion(
  changeId: string,
  minSupported: number,
  maxSupported: number
): number {
  return currentWorkflowRuntimeContext().getVersion(changeId, minSupported, maxSupported);
}

export function patched(patchId: string): boolean {
  return getVersion(patchId, DEFAULT_VERSION, 1) !== DEFAULT_VERSION;
}

export function deprecatePatch(patchId: string): void {
  currentWorkflowRuntimeContext().deprecatePatch(patchId);
}

export function sideEffect<T>(key: string, effect: () => T): PromiseLike<T> {
  return new SideEffectDurablePromise(key, effect);
}

export function publish<QueryState extends object>(view: QueryState): void {
  currentWorkflowRuntimeContext().publish(view);
}

export function continueAsNew<Input extends object>(input: Input): never {
  return currentWorkflowRuntimeContext().continueAsNew(input);
}

export async function prepareWorkflowTaskCommit<
  W extends WorkflowDefinition<any, any, any, string>
>(
  workflowDefinition: W,
  input: WorkflowInput<W>,
  claimed: ClaimedWorkflowTask,
  options: PrepareWorkflowTaskOptions = {}
): Promise<WorkflowTaskCommit> {
  installNondeterminismGuards();
  const context = new WorkflowRuntimeContext(claimed, options, workflowDefinition.inputSchema);
  try {
    const output = await runtimeStorage.run(context, () => workflowDefinition.handler(input));
    context.completeWorkflow(output as WorkflowOutput<W>, workflowDefinition);
  } catch (error) {
    if (error instanceof ContinueAsNewRequested) {
      return context.toCommit();
    }
    if (error instanceof WorkflowSuspended) {
      return context.toCommit();
    }
    if (isWorkflowTaskFatalError(error)) {
      throw error;
    }
    context.failWorkflow(durableFailureFromUnknown(error));
  }
  return context.toCommit();
}

export function currentWorkflowRuntimeContext(): WorkflowRuntimeContext {
  const context = runtimeStorage.getStore();
  if (!context) {
    throw new Error("durust durable APIs must be awaited inside a workflow task");
  }
  return context;
}

function assertNotInWorkflowRuntime(
  apiName: string,
  replacement: string,
  options: { readonly allowInSideEffect?: boolean } = {}
): void {
  const context = runtimeStorage.getStore();
  if (
    context !== undefined &&
    (!(options.allowInSideEffect ?? false) || !context.allowsNondeterministicGlobals())
  ) {
    throw new Error(
      `nondeterminism: ${apiName} is not allowed inside workflow code; use ${replacement}`
    );
  }
}

function installNondeterminismGuards(): void {
  if (nondeterminismGuardsInstalled) {
    return;
  }
  nondeterminismGuardsInstalled = true;
  originalDateNow = Date.now.bind(Date);
  originalMathRandom = Math.random.bind(Math);
  originalSetTimeout = globalThis.setTimeout.bind(globalThis);
  originalSetInterval = globalThis.setInterval.bind(globalThis);
  originalQueueMicrotask = globalThis.queueMicrotask.bind(globalThis);
  originalPromiseAll = Promise.all.bind(Promise) as PromiseConstructor["all"];
  originalPromiseRace = Promise.race.bind(Promise) as PromiseConstructor["race"];
  originalPromiseAllSettled = Promise.allSettled.bind(Promise) as PromiseConstructor["allSettled"];
  originalPromiseAny = Promise.any.bind(Promise) as PromiseConstructor["any"];
  Object.defineProperty(Date, "now", {
    configurable: true,
    writable: true,
    value: () => {
      assertNotInWorkflowRuntime("Date.now()", "workflow time APIs such as sleepUntil()", {
        allowInSideEffect: true
      });
      return originalDateNow?.() ?? 0;
    }
  });
  Object.defineProperty(Math, "random", {
    configurable: true,
    writable: true,
    value: () => {
      assertNotInWorkflowRuntime(
        "Math.random()",
        "sideEffect() for recorded nondeterministic values",
        { allowInSideEffect: true }
      );
      return originalMathRandom?.() ?? 0;
    }
  });
  Object.defineProperty(globalThis, "setTimeout", {
    configurable: true,
    writable: true,
    value: ((...args: Parameters<typeof globalThis.setTimeout>) => {
      assertNotInWorkflowRuntime("setTimeout()", "durust sleep() or sleepUntil()");
      return originalSetTimeout?.(...args);
    }) as typeof globalThis.setTimeout
  });
  Object.defineProperty(globalThis, "setInterval", {
    configurable: true,
    writable: true,
    value: ((...args: Parameters<typeof globalThis.setInterval>) => {
      assertNotInWorkflowRuntime("setInterval()", "durust sleep() or recurring workflow timers");
      return originalSetInterval?.(...args);
    }) as typeof globalThis.setInterval
  });
  Object.defineProperty(globalThis, "queueMicrotask", {
    configurable: true,
    writable: true,
    value: ((callback: VoidFunction) => {
      assertNotInWorkflowRuntime("queueMicrotask()", "durust durable operations");
      return originalQueueMicrotask?.(callback);
    }) as typeof globalThis.queueMicrotask
  });
  Object.defineProperty(Promise, "all", {
    configurable: true,
    writable: true,
    value: ((values: Iterable<unknown>) => {
      assertNotInWorkflowRuntime("Promise.all()", "durust join() or joinAll()");
      return originalPromiseAll?.(values as Iterable<unknown>);
    }) as PromiseConstructor["all"]
  });
  Object.defineProperty(Promise, "race", {
    configurable: true,
    writable: true,
    value: ((values: Iterable<unknown>) => {
      assertNotInWorkflowRuntime("Promise.race()", "durust select() or selectAll()");
      return originalPromiseRace?.(values as Iterable<unknown>);
    }) as PromiseConstructor["race"]
  });
  Object.defineProperty(Promise, "allSettled", {
    configurable: true,
    writable: true,
    value: ((values: Iterable<unknown>) => {
      assertNotInWorkflowRuntime("Promise.allSettled()", "durust join()/joinAll() plus explicit error handling");
      return originalPromiseAllSettled?.(values as Iterable<unknown>);
    }) as PromiseConstructor["allSettled"]
  });
  Object.defineProperty(Promise, "any", {
    configurable: true,
    writable: true,
    value: ((values: Iterable<unknown>) => {
      assertNotInWorkflowRuntime("Promise.any()", "durust select() or selectAll()");
      return originalPromiseAny?.(values as Iterable<unknown>);
    }) as PromiseConstructor["any"]
  });
}

export function createActivityDurablePromise<A extends ActivityDefinition<any, any, string>>(
  activityDefinition: A,
  input: ActivityInput<A>,
  options: ActivityCallOptions
): DurablePromise<ActivityOutput<A>> {
  return new ActivityDurablePromise(activityDefinition, input, options);
}

export function createActivityMapHandle<A extends ActivityDefinition<any, any, string>>(
  activityDefinition: A,
  options: ActivityMapOptions<ActivityInput<A>>
): ActivityMapHandle<ActivityOutput<A>> {
  return new RuntimeActivityMapHandle(activityDefinition, options);
}

export function createChildWorkflowMapHandle<W extends WorkflowDefinition<any, any, any, string>>(
  workflowDefinition: W,
  options: ChildWorkflowMapOptions<WorkflowInput<W>>
): ChildWorkflowMapHandle<WorkflowOutput<W>> {
  return new RuntimeChildWorkflowMapHandle(workflowDefinition, options);
}

export function createChildWorkflowStart<W extends WorkflowDefinition<any, any, any, string>>(
  workflowDefinition: W,
  input: WorkflowInput<W>,
  options: ChildWorkflowOptions
): ChildWorkflowStart<WorkflowOutput<W>> {
  return new ChildWorkflowStartDurable(workflowDefinition, input, options);
}

export interface RuntimeTimerSpec {
  readonly kind: "sleep" | "sleep_until";
  readonly durationMs?: number;
  readonly fireAt?: number;
  readonly fingerprintAt?: number;
}

export function createTimerDurablePromise(spec: RuntimeTimerSpec): DurableBranch<void> {
  return new TimerDurablePromise(spec);
}

export function createJoinDurablePromise(
  branches: Record<string, PromiseLike<unknown>>
): PromiseLike<Record<string, unknown>> {
  return new JoinDurablePromise(branches);
}

export function createJoinAllDurablePromise(
  branches: readonly PromiseLike<unknown>[]
): PromiseLike<readonly unknown[]> {
  return new JoinAllDurablePromise(branches);
}

export function createSelectDurablePromise(
  branches: Record<string, PromiseLike<unknown>>
): PromiseLike<SelectRuntimeResult> {
  return new SelectDurablePromise(branches);
}

export function createSelectAllDurablePromise(
  branches: readonly PromiseLike<unknown>[]
): PromiseLike<SelectAllRuntimeResult> {
  return new SelectAllDurablePromise(branches);
}

export function createSignalDurablePromise<
  Payload extends object,
  const Name extends string = string
>(name: Name): SignalDefinition<Payload, Name> {
  return new SignalDurablePromise<Payload, Name>(name);
}

export class WorkflowRuntimeContext {
  readonly #claimed: ClaimedWorkflowTask;
  readonly #defaultActivityTaskQueue: string;
  readonly #defaultWorkflowTaskQueue: string;
  readonly #payloadCodec: CodecId;
  readonly #workflowInputSchema: SchemaAdapter<unknown> | undefined;
  readonly #nowMs: number;
  readonly #liveSignals: SignalInboxRecord[];
  readonly #replayEvents: readonly HistoryEvent[];
  readonly #activityCompletions = new Map<string, HistoryEvent>();
  readonly #activityFailures = new Map<string, HistoryEvent>();
  readonly #activityMapCompletions = new Map<string, HistoryEvent>();
  readonly #activityMapFailures = new Map<string, HistoryEvent>();
  readonly #childStarts = new Map<string, HistoryEvent>();
  readonly #childCompletions = new Map<string, HistoryEvent>();
  readonly #childFailures = new Map<string, HistoryEvent>();
  readonly #childCancellations = new Map<string, HistoryEvent>();
  readonly #childMapCompletions = new Map<string, HistoryEvent>();
  readonly #childMapFailures = new Map<string, HistoryEvent>();
  readonly #timerFires = new Map<string, HistoryEvent>();
  #replayCursor = 0;
  #nextCommandSeq = 1;
  readonly #appendEvents: NewHistoryEvent[] = [];
  readonly #upsertWaits: WaitRecord[] = [];
  readonly #deleteWaits: WaitId[] = [];
  readonly #consumeSignals: string[] = [];
  readonly #scheduleActivities: ActivityTask[] = [];
  readonly #scheduleActivityMaps: ActivityMapTask[] = [];
  readonly #startChildWorkflows: ChildWorkflowStartRequested[] = [];
  readonly #scheduleChildWorkflowMaps: ChildWorkflowMapTask[] = [];
  #queryProjection: PayloadRef | null = null;
  #allowNondeterministicGlobalsDepth = 0;

  constructor(
    claimed: ClaimedWorkflowTask,
    options: PrepareWorkflowTaskOptions,
    workflowInputSchema?: SchemaAdapter<unknown>
  ) {
    this.#claimed = claimed;
    this.#defaultActivityTaskQueue = options.defaultActivityTaskQueue ?? "default";
    this.#defaultWorkflowTaskQueue = options.defaultWorkflowTaskQueue ?? "default";
    this.#payloadCodec = options.payloadCodec ?? "MessagePack";
    this.#workflowInputSchema = workflowInputSchema;
    this.#nowMs = options.nowMs ?? 0;
    this.#liveSignals = [...(options.liveSignals ?? [])];
    this.#replayEvents = claimed.prefetchedHistory.filter(
      (event) =>
        event.eventType !== "WorkflowStarted" &&
        event.eventType !== "ActivityCompleted" &&
        event.eventType !== "ActivityFailed" &&
        event.eventType !== "ActivityTimedOut" &&
        event.eventType !== "ActivityMapCompleted" &&
        event.eventType !== "ActivityMapFailed" &&
        event.eventType !== "ChildWorkflowMapCompleted" &&
        event.eventType !== "ChildWorkflowMapFailed" &&
        event.eventType !== "ChildWorkflowStarted" &&
        event.eventType !== "ChildWorkflowCompleted" &&
        event.eventType !== "ChildWorkflowFailed" &&
        event.eventType !== "ChildWorkflowCancelled" &&
        event.eventType !== "TimerFired"
    );
    for (const event of claimed.prefetchedHistory) {
      if (event.data.kind === "ActivityCompleted") {
        this.#activityCompletions.set(commandKey(event.data.completed.commandId), event);
      }
      if (event.data.kind === "ActivityFailed") {
        this.#activityFailures.set(commandKey(event.data.failed.commandId), event);
      }
      if (event.data.kind === "ActivityMapCompleted") {
        this.#activityMapCompletions.set(commandKey(event.data.completed.commandId), event);
      }
      if (event.data.kind === "ActivityMapFailed") {
        this.#activityMapFailures.set(commandKey(event.data.failed.commandId), event);
      }
      if (event.data.kind === "ChildWorkflowMapCompleted") {
        this.#childMapCompletions.set(commandKey(event.data.completed.commandId), event);
      }
      if (event.data.kind === "ChildWorkflowMapFailed") {
        this.#childMapFailures.set(commandKey(event.data.failed.commandId), event);
      }
      if (event.data.kind === "TimerFired") {
        this.#timerFires.set(commandKey(event.data.fired.commandId), event);
      }
      if (event.data.kind === "ChildWorkflowStarted") {
        this.#childStarts.set(commandKey(event.data.started.commandId), event);
      }
      if (event.data.kind === "ChildWorkflowCompleted") {
        this.#childCompletions.set(commandKey(event.data.completed.commandId), event);
      }
      if (event.data.kind === "ChildWorkflowFailed") {
        this.#childFailures.set(commandKey(event.data.failed.commandId), event);
      }
      if (event.data.kind === "ChildWorkflowCancelled") {
        this.#childCancellations.set(commandKey(event.data.cancelled.commandId), event);
      }
    }
  }

  allowsNondeterministicGlobals(): boolean {
    return this.#allowNondeterministicGlobalsDepth > 0;
  }

  resolveSignal<Payload extends object>(name: string): SignalResolution<Payload> {
    const id = commandId(this.#claimed.runId, this.#nextCommandSeq++);
    const fingerprint = signalFingerprint(name);
    const replayEvent = this.#peekReplayEvent();
    if (replayEvent?.data.kind === "SignalConsumed") {
      const consumed = replayEvent.data.consumed;
      if (!sameCommandId(consumed.commandId, id)) {
        throw new Error(
          `nondeterminism: expected command seq ${id.seq}, found ${consumed.commandId.seq}`
        );
      }
      if (!sameFingerprint(consumed.fingerprint, fingerprint)) {
        throw new Error("nondeterminism: signal command fingerprint changed");
      }
      this.#advanceReplay();
      return {
        kind: "Consumed",
        value: decodePayload<Payload>(consumed.payload as PayloadRef<Payload>),
        eventId: replayEvent.eventId
      };
    }

    const live = this.#takeLiveSignal(name);
    if (live) {
      const consumedEventId = this.#nextAppendEventId();
      this.#appendEvents.push({
        data: {
          kind: "SignalConsumed",
          consumed: {
            commandId: id,
            signalId: live.signalId,
            signalName: live.signalName,
            payload: live.payload,
            fingerprint
          }
        }
      });
      this.#consumeSignals.push(String(live.signalId));
      this.#deleteWaits.push(signalWaitId(id));
      return {
        kind: "Consumed",
        value: decodePayload<Payload>(live.payload as PayloadRef<Payload>),
        eventId: consumedEventId
      };
    }

    this.#upsertWaits.push({
      waitId: signalWaitId(id),
      runId: this.#claimed.runId,
      commandId: id,
      kind: "Signal",
      key: name,
      readyAt: null
    });
    return { kind: "Pending", commandId: id };
  }

  resolveTimer(spec: RuntimeTimerSpec): TimerResolution {
    const started = this.#timerStartedEvent(spec);
    const replayEvent = this.#peekReplayEvent();
    if (replayEvent !== undefined) {
      if (replayEvent.data.kind !== "TimerStarted") {
        throw new Error(
          `nondeterminism: expected TimerStarted for command ${started.commandId.seq}, found ${replayEvent.eventType}`
        );
      }
      const replayed = replayEvent.data.started;
      if (!sameCommandId(replayed.commandId, started.commandId)) {
        throw new Error(
          `nondeterminism: expected command seq ${started.commandId.seq}, found ${replayed.commandId.seq}`
        );
      }
      if (!sameFingerprint(replayed.fingerprint, started.fingerprint)) {
        throw new Error("nondeterminism: timer command fingerprint changed");
      }
      this.#advanceReplay();

      const terminal = this.#timerFires.get(commandKey(started.commandId));
      if (terminal?.data.kind === "TimerFired") {
        return { kind: "Fired", eventId: terminal.eventId };
      }

      return { kind: "Pending", commandId: started.commandId };
    }

    this.#appendEvents.push({ data: { kind: "TimerStarted", started } });
    this.#upsertWaits.push({
      waitId: timerWaitId(started.commandId),
      runId: this.#claimed.runId,
      commandId: started.commandId,
      kind: "Timer",
      key: "timer",
      readyAt: started.fireAt
    });
    return { kind: "Pending", commandId: started.commandId };
  }

  resolveActivity<A extends ActivityDefinition<any, any, string>>(
    activityDefinition: A,
    input: ActivityInput<A>,
    options: ActivityCallOptions
  ): ActivityResolution<ActivityOutput<A>> {
    const scheduled = this.#activityScheduledEvent(activityDefinition, input, options);
    const replayEvent = this.#peekReplayEvent();
    if (replayEvent !== undefined) {
      if (replayEvent.data.kind !== "ActivityScheduled") {
        throw new Error(
          `nondeterminism: expected ActivityScheduled for command ${scheduled.commandId.seq}, found ${replayEvent.eventType}`
        );
      }
      const replayed = replayEvent.data.scheduled;
      if (!sameCommandId(replayed.commandId, scheduled.commandId)) {
        throw new Error(
          `nondeterminism: expected command seq ${scheduled.commandId.seq}, found ${replayed.commandId.seq}`
        );
      }
      if (!sameFingerprint(replayed.fingerprint, scheduled.fingerprint)) {
        throw new Error(`nondeterminism: activity command fingerprint changed`);
      }
      this.#advanceReplay();

      const terminal = this.#activityCompletions.get(commandKey(scheduled.commandId));
      if (terminal?.data.kind === "ActivityCompleted") {
        return {
          kind: "Completed",
          commandId: scheduled.commandId,
          value: decodePayload<ActivityOutput<A>>(
            terminal.data.completed.result as PayloadRef<ActivityOutput<A>>,
            activityDefinition.outputSchema
          ),
          eventId: terminal.eventId
        };
      }
      const failed = this.#activityFailures.get(commandKey(scheduled.commandId));
      if (failed?.data.kind === "ActivityFailed") {
        return {
          kind: "Failed",
          commandId: scheduled.commandId,
          failure: failed.data.failed.failure,
          eventId: failed.eventId
        };
      }

      return { kind: "Pending", commandId: scheduled.commandId };
    }

    this.#appendEvents.push({
      data: { kind: "ActivityScheduled", scheduled }
    });
    this.#scheduleActivities.push(activityTaskFromScheduled(scheduled));
    return { kind: "Pending", commandId: scheduled.commandId };
  }

  resolveActivityHandleResult<A extends ActivityDefinition<any, any, string>>(
    activityDefinition: A,
    id: CommandId
  ): ActivityResolution<ActivityOutput<A>> {
    const terminal = this.#activityCompletions.get(commandKey(id));
    if (terminal?.data.kind === "ActivityCompleted") {
      return {
        kind: "Completed",
        commandId: id,
        value: decodePayload<ActivityOutput<A>>(
          terminal.data.completed.result as PayloadRef<ActivityOutput<A>>,
          activityDefinition.outputSchema
        ),
        eventId: terminal.eventId
      };
    }
    const failed = this.#activityFailures.get(commandKey(id));
    if (failed?.data.kind === "ActivityFailed") {
      return {
        kind: "Failed",
        commandId: id,
        failure: failed.data.failed.failure,
        eventId: failed.eventId
      };
    }
    return { kind: "Pending", commandId: id };
  }

  resolveActivityMap<A extends ActivityDefinition<any, any, string>>(
    activityDefinition: A,
    options: ActivityMapOptions<ActivityInput<A>>
  ): ActivityMapResolution<ActivityOutput<A>> {
    const scheduled = this.#activityMapScheduledEvent(activityDefinition, options);
    const replayEvent = this.#peekReplayEvent();
    if (replayEvent !== undefined) {
      if (replayEvent.data.kind !== "ActivityMapScheduled") {
        throw new Error(
          `nondeterminism: expected ActivityMapScheduled for command ${scheduled.commandId.seq}, found ${replayEvent.eventType}`
        );
      }
      const replayed = replayEvent.data.scheduled;
      if (!sameCommandId(replayed.commandId, scheduled.commandId)) {
        throw new Error(
          `nondeterminism: expected command seq ${scheduled.commandId.seq}, found ${replayed.commandId.seq}`
        );
      }
      if (!sameFingerprint(replayed.fingerprint, scheduled.fingerprint)) {
        throw new Error("nondeterminism: activity map command fingerprint changed");
      }
      this.#advanceReplay();

      const completed = this.#activityMapCompletions.get(commandKey(scheduled.commandId));
      if (completed?.data.kind === "ActivityMapCompleted") {
        return {
          kind: "Completed",
          commandId: scheduled.commandId,
          resultManifest: completed.data.completed.resultManifest as PayloadRef<ActivityMapResultManifest<ActivityOutput<A>>>,
          eventId: completed.eventId
        };
      }
      const failed = this.#activityMapFailures.get(commandKey(scheduled.commandId));
      if (failed?.data.kind === "ActivityMapFailed") {
        return {
          kind: "Failed",
          commandId: scheduled.commandId,
          failure: failed.data.failed.failure,
          eventId: failed.eventId
        };
      }

      return { kind: "Pending", commandId: scheduled.commandId };
    }

    this.#appendEvents.push({
      data: { kind: "ActivityMapScheduled", scheduled }
    });
    this.#scheduleActivityMaps.push(activityMapTaskFromScheduled(scheduled));
    return { kind: "Pending", commandId: scheduled.commandId };
  }

  resolveChildWorkflowMap<W extends WorkflowDefinition<any, any, any, string>>(
    workflowDefinition: W,
    options: ChildWorkflowMapOptions<WorkflowInput<W>>
  ): ChildWorkflowMapResolution<WorkflowOutput<W>> {
    const scheduled = this.#childWorkflowMapScheduledEvent(workflowDefinition, options);
    const replayEvent = this.#peekReplayEvent();
    if (replayEvent !== undefined) {
      if (replayEvent.data.kind !== "ChildWorkflowMapScheduled") {
        throw new Error(
          `nondeterminism: expected ChildWorkflowMapScheduled for command ${scheduled.commandId.seq}, found ${replayEvent.eventType}`
        );
      }
      const replayed = replayEvent.data.scheduled;
      if (!sameCommandId(replayed.commandId, scheduled.commandId)) {
        throw new Error(
          `nondeterminism: expected command seq ${scheduled.commandId.seq}, found ${replayed.commandId.seq}`
        );
      }
      if (!sameFingerprint(replayed.fingerprint, scheduled.fingerprint)) {
        throw new Error("nondeterminism: child workflow map command fingerprint changed");
      }
      this.#advanceReplay();

      const completed = this.#childMapCompletions.get(commandKey(scheduled.commandId));
      if (completed?.data.kind === "ChildWorkflowMapCompleted") {
        return {
          kind: "Completed",
          commandId: scheduled.commandId,
          resultManifest: completed.data.completed.resultManifest as PayloadRef<ChildWorkflowMapResultManifest<WorkflowOutput<W>>>,
          eventId: completed.eventId
        };
      }
      const failed = this.#childMapFailures.get(commandKey(scheduled.commandId));
      if (failed?.data.kind === "ChildWorkflowMapFailed") {
        return {
          kind: "Failed",
          commandId: scheduled.commandId,
          failure: failed.data.failed.failure,
          eventId: failed.eventId
        };
      }

      return { kind: "Pending", commandId: scheduled.commandId };
    }

    this.#appendEvents.push({
      data: { kind: "ChildWorkflowMapScheduled", scheduled }
    });
    this.#scheduleChildWorkflowMaps.push(childWorkflowMapTaskFromScheduled(scheduled));
    return { kind: "Pending", commandId: scheduled.commandId };
  }

  resolveChildWorkflowStart<W extends WorkflowDefinition<any, any, any, string>>(
    workflowDefinition: W,
    input: WorkflowInput<W>,
    options: ChildWorkflowOptions
  ): ChildWorkflowStartResolution {
    const requested = this.#childWorkflowStartRequested(workflowDefinition, input, options);
    const replayEvent = this.#peekReplayEvent();
    if (replayEvent !== undefined) {
      if (replayEvent.data.kind !== "ChildWorkflowStartRequested") {
        throw new Error(
          `nondeterminism: expected ChildWorkflowStartRequested for command ${requested.commandId.seq}, found ${replayEvent.eventType}`
        );
      }
      const replayed = replayEvent.data.requested;
      if (!sameCommandId(replayed.commandId, requested.commandId)) {
        throw new Error(
          `nondeterminism: expected command seq ${requested.commandId.seq}, found ${replayed.commandId.seq}`
        );
      }
      if (!sameFingerprint(replayed.fingerprint, requested.fingerprint)) {
        throw new Error("nondeterminism: child workflow command fingerprint changed");
      }
      this.#advanceReplay();

      const started = this.#childStarts.get(commandKey(requested.commandId));
      if (started?.data.kind === "ChildWorkflowStarted") {
        return {
          kind: "Started",
          commandId: requested.commandId,
          workflowId: started.data.started.workflowId,
          runId: started.data.started.runId,
          eventId: started.eventId
        };
      }
      const failed = this.#childFailures.get(commandKey(requested.commandId));
      if (failed?.data.kind === "ChildWorkflowFailed") {
        return {
          kind: "Failed",
          commandId: requested.commandId,
          failure: failed.data.failed.failure,
          eventId: failed.eventId
        };
      }
      return { kind: "Pending", commandId: requested.commandId, workflowId: requested.workflowId };
    }

    this.#appendEvents.push({
      data: { kind: "ChildWorkflowStartRequested", requested }
    });
    this.#startChildWorkflows.push(requested);
    return { kind: "Pending", commandId: requested.commandId, workflowId: requested.workflowId };
  }

  resolveChildWorkflowResult<W extends WorkflowDefinition<any, any, any, string>>(
    workflowDefinition: W,
    id: CommandId
  ): ChildWorkflowResultResolution<WorkflowOutput<W>> {
    const completed = this.#childCompletions.get(commandKey(id));
    if (completed?.data.kind === "ChildWorkflowCompleted") {
      return {
        kind: "Completed",
        value: decodePayload<WorkflowOutput<W>>(
          completed.data.completed.result as PayloadRef<WorkflowOutput<W>>,
          workflowDefinition.outputSchema
        ),
        eventId: completed.eventId
      };
    }
    const failed = this.#childFailures.get(commandKey(id));
    if (failed?.data.kind === "ChildWorkflowFailed") {
      return { kind: "Failed", failure: failed.data.failed.failure, eventId: failed.eventId };
    }
    const cancelled = this.#childCancellations.get(commandKey(id));
    if (cancelled?.data.kind === "ChildWorkflowCancelled") {
      return {
        kind: "Cancelled",
        reason: cancelled.data.cancelled.reason,
        eventId: cancelled.eventId
      };
    }
    return { kind: "Pending" };
  }

  getVersion(changeId: string, minSupported: number, maxSupported: number): number {
    validateVersionRange(changeId, minSupported, maxSupported);
    const replayEvent = this.#peekReplayEvent();
    if (replayEvent !== undefined) {
      if (replayEvent.data.kind === "VersionMarker") {
        const marker = replayEvent.data.marker;
        if (marker.changeId !== changeId) {
          throw new Error(
            `nondeterminism: expected VersionMarker ${changeId}, found ${marker.changeId}`
          );
        }
        const id = this.#nextCommandId();
        validateMarkerCommand(changeId, id, marker.commandId);
        this.#advanceReplay();
        return validateRecordedVersion(
          changeId,
          marker.version,
          minSupported,
          maxSupported
        );
      }
      if (replayEvent.data.kind === "DeprecatedPatchMarker") {
        throw new Error(
          `nondeterminism: expected VersionMarker ${changeId}, found DeprecatedPatchMarker ${replayEvent.data.marker.patchId}`
        );
      }
      return validateRecordedVersion(
        changeId,
        DEFAULT_VERSION,
        minSupported,
        maxSupported
      );
    }

    const id = this.#nextCommandId();
    this.#appendEvents.push({
      data: {
        kind: "VersionMarker",
        marker: {
          commandId: id,
          changeId,
          version: maxSupported
        }
      }
    });
    return maxSupported;
  }

  deprecatePatch(patchId: string): void {
    const replayEvent = this.#peekReplayEvent();
    if (replayEvent !== undefined) {
      if (replayEvent.data.kind === "VersionMarker") {
        const marker = replayEvent.data.marker;
        if (marker.changeId !== patchId) {
          throw new Error(
            `nondeterminism: expected patch marker ${patchId}, found VersionMarker ${marker.changeId}`
          );
        }
        const id = this.#nextCommandId();
        validateMarkerCommand(patchId, id, marker.commandId);
        if (marker.version <= DEFAULT_VERSION) {
          throw new UnsupportedWorkflowVersionError(
            patchId,
            marker.version,
            1,
            Number.MAX_SAFE_INTEGER
          );
        }
        this.#advanceReplay();
        return;
      }
      if (replayEvent.data.kind === "DeprecatedPatchMarker") {
        const marker = replayEvent.data.marker;
        if (marker.patchId !== patchId) {
          throw new Error(
            `nondeterminism: expected DeprecatedPatchMarker ${patchId}, found ${marker.patchId}`
          );
        }
        const id = this.#nextCommandId();
        validateMarkerCommand(patchId, id, marker.commandId);
        this.#advanceReplay();
      }
      return;
    }

    const id = this.#nextCommandId();
    this.#appendEvents.push({
      data: {
        kind: "DeprecatedPatchMarker",
        marker: {
          commandId: id,
          patchId
        }
      }
    });
  }

  resolveSideEffect<T>(key: string, effect: () => T): T {
    if (key.length === 0) {
      throw new Error("side effect key must not be empty");
    }

    const replayEvent = this.#peekReplayEvent();
    if (replayEvent !== undefined) {
      if (replayEvent.data.kind !== "SideEffectMarker") {
        throw new Error(
          `nondeterminism: expected SideEffectMarker ${key}, found ${replayEvent.eventType}`
        );
      }
      const marker = replayEvent.data.marker;
      const id = this.#nextCommandId();
      if (Number(marker.commandId.seq) !== Number(id.seq)) {
        throw new Error(
          `nondeterminism: side effect ${key} command sequence changed: expected ${id.seq}, found ${marker.commandId.seq}`
        );
      }
      if (marker.key !== key) {
        throw new Error(`nondeterminism: expected side effect ${key}, found ${marker.key}`);
      }
      this.#advanceReplay();
      return decodePayload<T>(marker.value as PayloadRef<T>);
    }

    const id = this.#nextCommandId();
    let value: T;
    this.#allowNondeterministicGlobalsDepth += 1;
    try {
      value = effect();
    } finally {
      this.#allowNondeterministicGlobalsDepth -= 1;
    }
    const payload = encodePayload(value, { codec: this.#payloadCodec });
    this.#appendEvents.push({
      data: {
        kind: "SideEffectMarker",
        marker: {
          commandId: id,
          key,
          value: payload
        }
      }
    });
    return value;
  }

  resolveSelectWinner(
    readyBranches: readonly SelectReadyBranch[],
    branchesDigest: string
  ): SelectReadyBranch {
    if (readyBranches.length === 0) {
      throw new Error("durust.select requires at least one ready branch");
    }
    const winner = [...readyBranches].sort(compareSelectReadyBranches)[0] as SelectReadyBranch;
    const selectCommandId = this.#nextCommandId();
    const replayEvent = this.#peekReplayEvent();
    if (replayEvent !== undefined) {
      if (replayEvent.data.kind !== "SelectWinner") {
        throw new Error(
          `nondeterminism: expected SelectWinner for command ${selectCommandId.seq}, found ${replayEvent.eventType}`
        );
      }
      const replayed = replayEvent.data.winner;
      if (!sameCommandId(replayed.selectCommandId, selectCommandId)) {
        throw new Error(
          `nondeterminism: expected select command seq ${selectCommandId.seq}, found ${replayed.selectCommandId.seq}`
        );
      }
      if (replayed.branchesDigest !== branchesDigest) {
        throw new Error("nondeterminism: select branches changed");
      }
      if (replayed.branchOrdinal !== winner.ordinal) {
        throw new Error("nondeterminism: select winner branch changed");
      }
      if (!sameEventId(replayed.winningEventId, winner.eventId)) {
        throw new Error("nondeterminism: select winning event changed");
      }
      this.#advanceReplay();
      return winner;
    }

    this.#appendEvents.push({
      data: {
        kind: "SelectWinner",
        winner: {
          selectCommandId,
          branchOrdinal: winner.ordinal,
          winningEventId: winner.eventId,
          branchesDigest
        }
      }
    });
    return winner;
  }

  completeWorkflow<Output>(
    output: Output,
    workflowDefinition: WorkflowDefinition<any, Output, any, string>
  ): void {
    const result = encodePayload(output, {
      codec: this.#payloadCodec,
      ...(workflowDefinition.outputSchema === undefined
        ? {}
        : { schema: workflowDefinition.outputSchema })
    });
    this.#appendEvents.push({
      data: { kind: "WorkflowCompleted", result }
    });
  }

  failWorkflow(failure: DurableFailure): void {
    this.#appendEvents.push({
      data: {
        kind: "WorkflowFailed",
        failure
      }
    });
  }

  publish<QueryState extends object>(view: QueryState): void {
    this.#queryProjection = encodePayload(view, { codec: this.#payloadCodec });
  }

  continueAsNew<Input extends object>(input: Input): never {
    const payload = encodePayload(input, {
      codec: this.#payloadCodec,
      ...(this.#workflowInputSchema === undefined
        ? {}
        : { schema: this.#workflowInputSchema as SchemaAdapter<Input> })
    });
    this.#appendEvents.push({
      data: {
        kind: "WorkflowContinuedAsNew",
        input: payload
      }
    });
    throw new ContinueAsNewRequested();
  }

  toCommit(): WorkflowTaskCommit {
    return {
      expectedTailEventId: this.#claimed.replayTargetEventId,
      appendEvents: [...this.#appendEvents],
      upsertWaits: [...this.#upsertWaits],
      deleteWaits: [...this.#deleteWaits],
      consumeSignals: [...this.#consumeSignals],
      scheduleActivities: [...this.#scheduleActivities],
      scheduleActivityMaps: [...this.#scheduleActivityMaps],
      startChildWorkflows: [...this.#startChildWorkflows],
      scheduleChildWorkflowMaps: [...this.#scheduleChildWorkflowMaps],
      ...(this.#queryProjection === null ? {} : { queryProjection: this.#queryProjection })
    };
  }

  #activityScheduledEvent<A extends ActivityDefinition<any, any, string>>(
    activityDefinition: A,
    input: ActivityInput<A>,
    options: ActivityCallOptions
  ): ActivityScheduled {
    const id = this.#nextCommandId();
    const inputRef = encodePayload(input, {
      codec: this.#payloadCodec,
      ...(activityDefinition.inputSchema === undefined ? {} : { schema: activityDefinition.inputSchema })
    });
    const taskQueue = options.taskQueue ?? this.#defaultActivityTaskQueue;
    const retryPolicy = options.retry ?? RetryPolicy.none();
    const fingerprint = activityFingerprint(
      activityDefinition.name,
      payloadDigest(inputRef),
      activityOptionsDigest({
        taskQueue,
        retryPolicy,
        startToCloseTimeoutMs: options.startToCloseTimeoutMs ?? null,
        heartbeatTimeoutMs: options.heartbeatTimeoutMs ?? null
      })
    );
    return {
      commandId: id,
      activityName: activityDefinition.name,
      taskQueue,
      retryPolicy,
      startToCloseTimeoutMs: options.startToCloseTimeoutMs ?? null,
      heartbeatTimeoutMs: options.heartbeatTimeoutMs ?? null,
      input: inputRef,
      fingerprint
    };
  }

  #activityMapScheduledEvent<A extends ActivityDefinition<any, any, string>>(
    activityDefinition: A,
    options: ActivityMapOptions<ActivityInput<A>>
  ): ActivityMapScheduled {
    const id = this.#nextCommandId();
    const taskQueue = options.taskQueue ?? this.#defaultActivityTaskQueue;
    const retryPolicy = RetryPolicy.none();
    const startToCloseTimeoutMs = null;
    const heartbeatTimeoutMs = null;
    const fingerprint = activityMapFingerprint(
      activityDefinition.name,
      payloadDigest(options.inputManifest),
      options.resultManifest,
      options.maxInFlight,
      activityOptionsDigest({
        taskQueue,
        retryPolicy,
        startToCloseTimeoutMs,
        heartbeatTimeoutMs
      })
    );
    return {
      commandId: id,
      activityName: activityDefinition.name,
      taskQueue,
      retryPolicy,
      startToCloseTimeoutMs,
      heartbeatTimeoutMs,
      inputManifest: options.inputManifest,
      resultManifestName: options.resultManifest,
      maxInFlight: options.maxInFlight,
      fingerprint
    };
  }

  #childWorkflowMapScheduledEvent<W extends WorkflowDefinition<any, any, any, string>>(
    workflowDefinition: W,
    options: ChildWorkflowMapOptions<WorkflowInput<W>>
  ): ChildWorkflowMapScheduled {
    if (options.maxInFlight <= 0 || !Number.isInteger(options.maxInFlight)) {
      throw new Error("childWorkflowMap maxInFlight must be a positive integer");
    }
    if (options.workflowIdPrefix.length === 0) {
      throw new Error("childWorkflowMap workflowIdPrefix must not be empty");
    }
    const id = this.#nextCommandId();
    const taskQueue = options.taskQueue ?? this.#defaultWorkflowTaskQueue;
    const parentClosePolicy = options.parentClosePolicy ?? "Cancel";
    const failureMode = options.failureMode ?? "FailFast";
    const fingerprint = childWorkflowMapFingerprint(
      workflowDefinition.workflowType,
      payloadDigest(options.inputManifest),
      options.resultManifest,
      options.workflowIdPrefix,
      options.maxInFlight,
      taskQueue,
      parentClosePolicy,
      failureMode
    );
    return {
      commandId: id,
      workflowType: workflowDefinition.workflowType,
      taskQueue,
      inputManifest: options.inputManifest,
      resultManifestName: options.resultManifest,
      workflowIdPrefix: options.workflowIdPrefix,
      maxInFlight: options.maxInFlight,
      parentClosePolicy,
      failureMode,
      fingerprint
    };
  }

  #childWorkflowStartRequested<W extends WorkflowDefinition<any, any, any, string>>(
    workflowDefinition: W,
    input: WorkflowInput<W>,
    options: ChildWorkflowOptions
  ): ChildWorkflowStartRequested {
    const id = this.#nextCommandId();
    const inputRef = encodePayload(input, {
      codec: this.#payloadCodec,
      ...(workflowDefinition.inputSchema === undefined
        ? {}
        : { schema: workflowDefinition.inputSchema })
    });
    const taskQueue = options.taskQueue ?? this.#defaultWorkflowTaskQueue;
    const parentClosePolicy = options.parentClosePolicy ?? "Cancel";
    const fingerprint = childWorkflowFingerprint(
      workflowDefinition.workflowType,
      options.workflowId,
      payloadDigest(inputRef),
      taskQueue,
      parentClosePolicy
    );
    return {
      commandId: id,
      workflowType: workflowDefinition.workflowType,
      workflowId: options.workflowId,
      taskQueue,
      input: inputRef,
      parentClosePolicy,
      fingerprint
    };
  }

  #timerStartedEvent(spec: RuntimeTimerSpec): TimerStarted {
    const id = this.#nextCommandId();
    const { fireAt, fingerprintAt } = resolveTimerTimes(spec, this.#nowMs);
    return {
      commandId: id,
      fireAt: timestampMs(fireAt),
      fingerprint: timerFingerprint(spec.kind, timestampMs(fingerprintAt))
    };
  }

  #peekReplayEvent(): HistoryEvent | undefined {
    return this.#replayEvents[this.#replayCursor];
  }

  #nextCommandId(): CommandId {
    return commandId(this.#claimed.runId, this.#nextCommandSeq++);
  }

  #nextAppendEventId(): EventId {
    return eventId(Number(this.#claimed.replayTargetEventId) + this.#appendEvents.length + 1);
  }

  #advanceReplay(): void {
    this.#replayCursor += 1;
  }

  #takeLiveSignal(name: string): SignalInboxRecord | undefined {
    const index = this.#liveSignals.findIndex((signal) => String(signal.signalName) === name);
    if (index < 0) {
      return undefined;
    }
    const [signal] = this.#liveSignals.splice(index, 1);
    return signal;
  }
}

type ActivityResolution<Output> =
  | { readonly kind: "Pending"; readonly commandId: CommandId }
  | {
      readonly kind: "Completed";
      readonly commandId: CommandId;
      readonly value: Output;
      readonly eventId: EventId;
    }
  | {
      readonly kind: "Failed";
      readonly commandId: CommandId;
      readonly failure: DurableFailure;
      readonly eventId: EventId;
    };

type ActivityMapResolution<Output> =
  | { readonly kind: "Pending"; readonly commandId: CommandId }
  | {
      readonly kind: "Completed";
      readonly commandId: CommandId;
      readonly resultManifest: PayloadRef<ActivityMapResultManifest<Output>>;
      readonly eventId: EventId;
    }
  | {
      readonly kind: "Failed";
      readonly commandId: CommandId;
      readonly failure: DurableFailure;
      readonly eventId: EventId;
    };

type ChildWorkflowMapResolution<Output> =
  | { readonly kind: "Pending"; readonly commandId: CommandId }
  | {
      readonly kind: "Completed";
      readonly commandId: CommandId;
      readonly resultManifest: PayloadRef<ChildWorkflowMapResultManifest<Output>>;
      readonly eventId: EventId;
    }
  | {
      readonly kind: "Failed";
      readonly commandId: CommandId;
      readonly failure: DurableFailure;
      readonly eventId: EventId;
    };

type ChildWorkflowStartResolution =
  | { readonly kind: "Pending"; readonly commandId: CommandId; readonly workflowId: WorkflowId | string }
  | {
      readonly kind: "Started";
      readonly commandId: CommandId;
      readonly workflowId: WorkflowId | string;
      readonly runId: RunId;
      readonly eventId: EventId;
    }
  | {
      readonly kind: "Failed";
      readonly commandId: CommandId;
      readonly failure: DurableFailure;
      readonly eventId: EventId;
    };

type ChildWorkflowResultResolution<Output> =
  | { readonly kind: "Pending" }
  | { readonly kind: "Completed"; readonly value: Output; readonly eventId: EventId }
  | { readonly kind: "Failed"; readonly failure: DurableFailure; readonly eventId: EventId }
  | { readonly kind: "Cancelled"; readonly reason: string; readonly eventId: EventId };

type TimerResolution =
  | { readonly kind: "Pending"; readonly commandId: CommandId }
  | { readonly kind: "Fired"; readonly eventId: EventId };

type SignalResolution<Payload> =
  | { readonly kind: "Pending"; readonly commandId: CommandId }
  | { readonly kind: "Consumed"; readonly value: Payload; readonly eventId: EventId };

interface SelectReadyBranch {
  readonly ordinal: number;
  readonly key: string;
  readonly value: unknown;
  readonly eventId: EventId;
}

interface SelectRuntimeResult {
  readonly branch: string;
  readonly value: unknown;
}

interface SelectAllRuntimeResult {
  readonly index: number;
  readonly value: unknown;
}

class ActivityDurablePromise<A extends ActivityDefinition<any, any, string>>
  implements DurablePromise<ActivityOutput<A>>
{
  readonly durableKind = "durable-promise" as const;
  readonly durableBranchKind = "activity" as const;
  readonly #activityDefinition: A;
  readonly #input: ActivityInput<A>;
  readonly #options: ActivityCallOptions;

  constructor(activityDefinition: A, input: ActivityInput<A>, options: ActivityCallOptions) {
    this.#activityDefinition = activityDefinition;
    this.#input = input;
    this.#options = options;
  }

  then<TResult1 = ActivityOutput<A>, TResult2 = never>(
    onfulfilled?: ((value: ActivityOutput<A>) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const resolution = currentWorkflowRuntimeContext().resolveActivity(
      this.#activityDefinition,
      this.#input,
      this.#options
    );
    if (resolution.kind === "Completed") {
      return Promise.resolve(
        onfulfilled
          ? onfulfilled(resolution.value)
          : (resolution.value as unknown as TResult1)
      );
    }
    if (resolution.kind === "Failed") {
      throw new ActivityFailureError(resolution.failure);
    }
    throw new WorkflowSuspended();
  }

  __durustRegisterJoinBranch(context: WorkflowRuntimeContext): JoinBranchResolution<ActivityOutput<A>> {
    const resolution = context.resolveActivity(
      this.#activityDefinition,
      this.#input,
      this.#options
    );
    if (resolution.kind === "Completed") {
      return { kind: "Ready", value: resolution.value, eventId: resolution.eventId };
    }
    if (resolution.kind === "Failed") {
      throw new ActivityFailureError(resolution.failure);
    }
    return { kind: "Pending" };
  }

  spawn(): PromiseLike<ActivityHandle<ActivityOutput<A>>> {
    return {
      then: <TResult1 = ActivityHandle<ActivityOutput<A>>, TResult2 = never>(
        onfulfilled?: ((value: ActivityHandle<ActivityOutput<A>>) => TResult1 | PromiseLike<TResult1>) | null,
        _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
      ): PromiseLike<TResult1 | TResult2> => {
        const resolution = currentWorkflowRuntimeContext().resolveActivity(
          this.#activityDefinition,
          this.#input,
          this.#options
        );
        const handle = new RuntimeActivityHandle(this.#activityDefinition, resolution.commandId);
        return Promise.resolve(onfulfilled ? onfulfilled(handle) : (handle as TResult1));
      }
    };
  }
}

class RuntimeActivityHandle<A extends ActivityDefinition<any, any, string>>
  implements ActivityHandle<ActivityOutput<A>>
{
  readonly kind = "activity-handle" as const;
  readonly #activityDefinition: A;
  readonly #commandId: CommandId;

  constructor(activityDefinition: A, commandIdValue: CommandId) {
    this.#activityDefinition = activityDefinition;
    this.#commandId = commandIdValue;
  }

  result(): PromiseLike<ActivityOutput<A>> {
    return new ActivityHandleResultDurablePromise(this.#activityDefinition, this.#commandId);
  }
}

class ActivityHandleResultDurablePromise<A extends ActivityDefinition<any, any, string>>
  implements PromiseLike<ActivityOutput<A>>
{
  readonly #activityDefinition: A;
  readonly #commandId: CommandId;

  constructor(activityDefinition: A, commandIdValue: CommandId) {
    this.#activityDefinition = activityDefinition;
    this.#commandId = commandIdValue;
  }

  then<TResult1 = ActivityOutput<A>, TResult2 = never>(
    onfulfilled?: ((value: ActivityOutput<A>) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const resolution = currentWorkflowRuntimeContext().resolveActivityHandleResult(
      this.#activityDefinition,
      this.#commandId
    );
    if (resolution.kind === "Completed") {
      return Promise.resolve(
        onfulfilled
          ? onfulfilled(resolution.value)
          : (resolution.value as unknown as TResult1)
      );
    }
    if (resolution.kind === "Failed") {
      throw new ActivityFailureError(resolution.failure);
    }
    throw new WorkflowSuspended();
  }
}

class RuntimeActivityMapHandle<A extends ActivityDefinition<any, any, string>>
  implements ActivityMapHandle<ActivityOutput<A>>
{
  readonly kind = "activity-map-handle" as const;
  readonly #activityDefinition: A;
  readonly #options: ActivityMapOptions<ActivityInput<A>>;

  constructor(activityDefinition: A, options: ActivityMapOptions<ActivityInput<A>>) {
    this.#activityDefinition = activityDefinition;
    this.#options = options;
  }

  resultManifest(): PromiseLike<PayloadRef<ActivityMapResultManifest<ActivityOutput<A>>>> {
    return new ActivityMapResultDurablePromise(this.#activityDefinition, this.#options);
  }
}

class ActivityMapResultDurablePromise<A extends ActivityDefinition<any, any, string>>
  implements PromiseLike<PayloadRef<ActivityMapResultManifest<ActivityOutput<A>>>>
{
  readonly #activityDefinition: A;
  readonly #options: ActivityMapOptions<ActivityInput<A>>;

  constructor(activityDefinition: A, options: ActivityMapOptions<ActivityInput<A>>) {
    this.#activityDefinition = activityDefinition;
    this.#options = options;
  }

  then<TResult1 = PayloadRef<ActivityMapResultManifest<ActivityOutput<A>>>, TResult2 = never>(
    onfulfilled?:
      | ((value: PayloadRef<ActivityMapResultManifest<ActivityOutput<A>>>) => TResult1 | PromiseLike<TResult1>)
      | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const resolution = currentWorkflowRuntimeContext().resolveActivityMap(
      this.#activityDefinition,
      this.#options
    );
    if (resolution.kind === "Completed") {
      return Promise.resolve(
        onfulfilled
          ? onfulfilled(resolution.resultManifest)
          : (resolution.resultManifest as unknown as TResult1)
      );
    }
    if (resolution.kind === "Failed") {
      throw new ActivityFailureError(resolution.failure);
    }
    throw new WorkflowSuspended();
  }
}

class RuntimeChildWorkflowMapHandle<W extends WorkflowDefinition<any, any, any, string>>
  implements ChildWorkflowMapHandle<WorkflowOutput<W>>
{
  readonly kind = "child-workflow-map-handle" as const;
  readonly #workflowDefinition: W;
  readonly #options: ChildWorkflowMapOptions<WorkflowInput<W>>;

  constructor(workflowDefinition: W, options: ChildWorkflowMapOptions<WorkflowInput<W>>) {
    this.#workflowDefinition = workflowDefinition;
    this.#options = options;
  }

  resultManifest(): PromiseLike<PayloadRef<ChildWorkflowMapResultManifest<WorkflowOutput<W>>>> {
    return new ChildWorkflowMapResultDurablePromise(this.#workflowDefinition, this.#options);
  }
}

class ChildWorkflowMapResultDurablePromise<W extends WorkflowDefinition<any, any, any, string>>
  implements PromiseLike<PayloadRef<ChildWorkflowMapResultManifest<WorkflowOutput<W>>>>
{
  readonly #workflowDefinition: W;
  readonly #options: ChildWorkflowMapOptions<WorkflowInput<W>>;

  constructor(workflowDefinition: W, options: ChildWorkflowMapOptions<WorkflowInput<W>>) {
    this.#workflowDefinition = workflowDefinition;
    this.#options = options;
  }

  then<TResult1 = PayloadRef<ChildWorkflowMapResultManifest<WorkflowOutput<W>>>, TResult2 = never>(
    onfulfilled?:
      | ((value: PayloadRef<ChildWorkflowMapResultManifest<WorkflowOutput<W>>>) => TResult1 | PromiseLike<TResult1>)
      | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const resolution = currentWorkflowRuntimeContext().resolveChildWorkflowMap(
      this.#workflowDefinition,
      this.#options
    );
    if (resolution.kind === "Completed") {
      return Promise.resolve(
        onfulfilled
          ? onfulfilled(resolution.resultManifest)
          : (resolution.resultManifest as unknown as TResult1)
      );
    }
    if (resolution.kind === "Failed") {
      throw new ChildWorkflowMapFailureError(resolution.failure);
    }
    throw new WorkflowSuspended();
  }
}

class ChildWorkflowStartDurable<W extends WorkflowDefinition<any, any, any, string>>
  implements ChildWorkflowStart<WorkflowOutput<W>>
{
  readonly kind = "child-workflow-start" as const;
  readonly #workflowDefinition: W;
  readonly #input: WorkflowInput<W>;
  readonly #options: ChildWorkflowOptions;

  constructor(workflowDefinition: W, input: WorkflowInput<W>, options: ChildWorkflowOptions) {
    this.#workflowDefinition = workflowDefinition;
    this.#input = input;
    this.#options = options;
  }

  spawn(): PromiseLike<ChildWorkflowHandle<WorkflowOutput<W>>> {
    return new ChildWorkflowSpawnDurablePromise(
      this.#workflowDefinition,
      this.#input,
      this.#options
    );
  }
}

class ChildWorkflowSpawnDurablePromise<W extends WorkflowDefinition<any, any, any, string>>
  implements PromiseLike<ChildWorkflowHandle<WorkflowOutput<W>>>
{
  readonly #workflowDefinition: W;
  readonly #input: WorkflowInput<W>;
  readonly #options: ChildWorkflowOptions;

  constructor(workflowDefinition: W, input: WorkflowInput<W>, options: ChildWorkflowOptions) {
    this.#workflowDefinition = workflowDefinition;
    this.#input = input;
    this.#options = options;
  }

  then<TResult1 = ChildWorkflowHandle<WorkflowOutput<W>>, TResult2 = never>(
    onfulfilled?: ((value: ChildWorkflowHandle<WorkflowOutput<W>>) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const resolution = currentWorkflowRuntimeContext().resolveChildWorkflowStart(
      this.#workflowDefinition,
      this.#input,
      this.#options
    );
    if (resolution.kind === "Started") {
      const handle = new RuntimeChildWorkflowHandle(
        this.#workflowDefinition,
        resolution.commandId,
        resolution.workflowId,
        resolution.runId
      );
      return Promise.resolve(onfulfilled ? onfulfilled(handle) : (handle as TResult1));
    }
    if (resolution.kind === "Failed") {
      throw new ChildWorkflowFailureError(resolution.failure);
    }
    throw new WorkflowSuspended();
  }
}

class RuntimeChildWorkflowHandle<W extends WorkflowDefinition<any, any, any, string>>
  implements ChildWorkflowHandle<WorkflowOutput<W>>
{
  readonly kind = "child-workflow-handle" as const;
  readonly #workflowDefinition: W;
  readonly #commandId: CommandId;
  readonly workflowId: WorkflowId | string;
  readonly runId: RunId;

  constructor(
    workflowDefinition: W,
    commandIdValue: CommandId,
    workflowIdValue: WorkflowId | string,
    runIdValue: RunId
  ) {
    this.#workflowDefinition = workflowDefinition;
    this.#commandId = commandIdValue;
    this.workflowId = workflowIdValue;
    this.runId = runIdValue;
  }

  result(): PromiseLike<WorkflowOutput<W>> {
    return new ChildWorkflowResultDurablePromise(this.#workflowDefinition, this.#commandId);
  }
}

class ChildWorkflowResultDurablePromise<W extends WorkflowDefinition<any, any, any, string>>
  implements PromiseLike<WorkflowOutput<W>>
{
  readonly #workflowDefinition: W;
  readonly #commandId: CommandId;

  constructor(workflowDefinition: W, commandIdValue: CommandId) {
    this.#workflowDefinition = workflowDefinition;
    this.#commandId = commandIdValue;
  }

  then<TResult1 = WorkflowOutput<W>, TResult2 = never>(
    onfulfilled?: ((value: WorkflowOutput<W>) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const resolution = currentWorkflowRuntimeContext().resolveChildWorkflowResult(
      this.#workflowDefinition,
      this.#commandId
    );
    if (resolution.kind === "Completed") {
      return Promise.resolve(
        onfulfilled ? onfulfilled(resolution.value) : (resolution.value as unknown as TResult1)
      );
    }
    if (resolution.kind === "Failed") {
      throw new ChildWorkflowFailureError(resolution.failure);
    }
    if (resolution.kind === "Cancelled") {
      throw new ChildWorkflowCancelledError(resolution.reason);
    }
    throw new WorkflowSuspended();
  }
}

class TimerDurablePromise implements DurableBranch<void> {
  readonly durableBranchKind = "timer" as const;
  readonly #spec: RuntimeTimerSpec;

  constructor(spec: RuntimeTimerSpec) {
    this.#spec = spec;
  }

  then<TResult1 = void, TResult2 = never>(
    onfulfilled?: ((value: void) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const resolution = currentWorkflowRuntimeContext().resolveTimer(this.#spec);
    if (resolution.kind === "Fired") {
      return Promise.resolve(onfulfilled ? onfulfilled(undefined) : (undefined as TResult1));
    }
    throw new WorkflowSuspended();
  }

  __durustRegisterJoinBranch(context: WorkflowRuntimeContext): JoinBranchResolution<void> {
    const resolution = context.resolveTimer(this.#spec);
    return resolution.kind === "Fired"
      ? { kind: "Ready", value: undefined, eventId: resolution.eventId }
      : { kind: "Pending" };
  }
}

class SignalDurablePromise<Payload extends object, const Name extends string = string>
  implements SignalDefinition<Payload, Name>
{
  readonly durableBranchKind = "signal" as const;
  readonly kind = "signal" as const;
  readonly name: Name;

  constructor(name: Name) {
    this.name = name;
  }

  then<TResult1 = Payload, TResult2 = never>(
    onfulfilled?: ((value: Payload) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const resolution = currentWorkflowRuntimeContext().resolveSignal<Payload>(this.name);
    if (resolution.kind === "Consumed") {
      return Promise.resolve(
        onfulfilled ? onfulfilled(resolution.value) : (resolution.value as unknown as TResult1)
      );
    }
    throw new WorkflowSuspended();
  }

  __durustRegisterJoinBranch(context: WorkflowRuntimeContext): JoinBranchResolution<Payload> {
    const resolution = context.resolveSignal<Payload>(this.name);
    return resolution.kind === "Consumed"
      ? { kind: "Ready", value: resolution.value, eventId: resolution.eventId }
      : { kind: "Pending" };
  }
}

class JoinDurablePromise implements PromiseLike<Record<string, unknown>> {
  readonly #branches: Record<string, PromiseLike<unknown>>;

  constructor(branches: Record<string, PromiseLike<unknown>>) {
    this.#branches = branches;
  }

  then<TResult1 = Record<string, unknown>, TResult2 = never>(
    onfulfilled?: ((value: Record<string, unknown>) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const context = currentWorkflowRuntimeContext();
    const values: Record<string, unknown> = {};
    let pending = false;
    for (const key of Object.keys(this.#branches)) {
      const branch = this.#branches[key];
      const durableBranch = asDurableJoinBranch(branch);
      const result = durableBranch.__durustRegisterJoinBranch(context);
      if (result.kind === "Pending") {
        pending = true;
      } else {
        values[key] = result.value;
      }
    }
    if (pending) {
      throw new WorkflowSuspended();
    }
    return Promise.resolve(onfulfilled ? onfulfilled(values) : (values as TResult1));
  }
}

class JoinAllDurablePromise implements PromiseLike<readonly unknown[]> {
  readonly #branches: readonly PromiseLike<unknown>[];

  constructor(branches: readonly PromiseLike<unknown>[]) {
    this.#branches = branches;
  }

  then<TResult1 = readonly unknown[], TResult2 = never>(
    onfulfilled?: ((value: readonly unknown[]) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const context = currentWorkflowRuntimeContext();
    const values: unknown[] = [];
    let pending = false;
    this.#branches.forEach((branch, index) => {
      const durableBranch = asDurableJoinBranch(branch);
      const result = durableBranch.__durustRegisterJoinBranch(context);
      if (result.kind === "Pending") {
        pending = true;
      } else {
        values[index] = result.value;
      }
    });
    if (pending) {
      throw new WorkflowSuspended();
    }
    return Promise.resolve(onfulfilled ? onfulfilled(values) : (values as TResult1));
  }
}

class SelectDurablePromise implements PromiseLike<SelectRuntimeResult> {
  readonly #branches: Record<string, PromiseLike<unknown>>;

  constructor(branches: Record<string, PromiseLike<unknown>>) {
    this.#branches = branches;
  }

  then<TResult1 = SelectRuntimeResult, TResult2 = never>(
    onfulfilled?: ((value: SelectRuntimeResult) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const context = currentWorkflowRuntimeContext();
    const keys = Object.keys(this.#branches);
    const ready: SelectReadyBranch[] = [];
    keys.forEach((key, ordinal) => {
      const branch = this.#branches[key];
      const durableBranch = asDurableJoinBranch(branch);
      const result = durableBranch.__durustRegisterJoinBranch(context);
      if (result.kind === "Ready") {
        ready.push({
          ordinal,
          key,
          value: result.value,
          eventId: result.eventId
        });
      }
    });

    if (ready.length === 0) {
      throw new WorkflowSuspended();
    }

    const winner = context.resolveSelectWinner(ready, selectBranchesDigest(keys));
    const result = { branch: winner.key, value: winner.value };
    return Promise.resolve(onfulfilled ? onfulfilled(result) : (result as TResult1));
  }
}

class SelectAllDurablePromise implements PromiseLike<SelectAllRuntimeResult> {
  readonly #branches: readonly PromiseLike<unknown>[];

  constructor(branches: readonly PromiseLike<unknown>[]) {
    this.#branches = branches;
  }

  then<TResult1 = SelectAllRuntimeResult, TResult2 = never>(
    onfulfilled?: ((value: SelectAllRuntimeResult) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const context = currentWorkflowRuntimeContext();
    const ready: SelectReadyBranch[] = [];
    this.#branches.forEach((branch, ordinal) => {
      const durableBranch = asDurableJoinBranch(branch);
      const result = durableBranch.__durustRegisterJoinBranch(context);
      if (result.kind === "Ready") {
        ready.push({
          ordinal,
          key: String(ordinal),
          value: result.value,
          eventId: result.eventId
        });
      }
    });

    if (ready.length === 0) {
      throw new WorkflowSuspended();
    }

    const keys = this.#branches.map((_, index) => String(index));
    const winner = context.resolveSelectWinner(ready, selectBranchesDigest(keys));
    const result = { index: Number(winner.key), value: winner.value };
    return Promise.resolve(onfulfilled ? onfulfilled(result) : (result as TResult1));
  }
}

class SideEffectDurablePromise<T> implements PromiseLike<T> {
  readonly #key: string;
  #effect: (() => T) | null;
  #done = false;

  constructor(key: string, effect: () => T) {
    this.#key = key;
    this.#effect = effect;
  }

  then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    if (this.#done) {
      throw new Error("side effect future polled after completion");
    }
    const effect = this.#effect;
    if (!effect) {
      throw new Error("side effect closure missing");
    }
    const value = currentWorkflowRuntimeContext().resolveSideEffect(this.#key, effect);
    this.#effect = null;
    this.#done = true;
    return Promise.resolve(onfulfilled ? onfulfilled(value) : (value as unknown as TResult1));
  }
}

type JoinBranchResolution<T> =
  | { readonly kind: "Pending" }
  | { readonly kind: "Ready"; readonly value: T; readonly eventId: EventId };

interface DurableJoinBranch<T> {
  __durustRegisterJoinBranch(context: WorkflowRuntimeContext): JoinBranchResolution<T>;
}

function asDurableJoinBranch(branch: unknown): DurableJoinBranch<unknown> {
  if (
    branch &&
    typeof branch === "object" &&
    "__durustRegisterJoinBranch" in branch &&
    typeof (branch as { __durustRegisterJoinBranch?: unknown }).__durustRegisterJoinBranch ===
      "function"
  ) {
    return branch as DurableJoinBranch<unknown>;
  }
  throw new Error("durust.join accepts only durable branches");
}

function sameCommandId(left: CommandId, right: CommandId): boolean {
  return left.runId === right.runId && left.seq === right.seq;
}

function commandKey(id: CommandId): string {
  return `${id.runId}:${id.seq}`;
}

function sameEventId(left: EventId, right: EventId): boolean {
  return Number(left) === Number(right);
}

function compareSelectReadyBranches(left: SelectReadyBranch, right: SelectReadyBranch): number {
  const eventDelta = Number(left.eventId) - Number(right.eventId);
  if (eventDelta !== 0) {
    return eventDelta;
  }
  return left.ordinal - right.ordinal;
}

function selectBranchesDigest(keys: readonly string[]): string {
  return digestBytes(JSON.stringify({ kind: "select", branches: keys }));
}

function validateVersionRange(changeId: string, minSupported: number, maxSupported: number): void {
  if (!Number.isInteger(minSupported) || !Number.isInteger(maxSupported)) {
    throw new Error(`invalid version range for ${changeId}: versions must be integers`);
  }
  if (minSupported > maxSupported) {
    throw new Error(
      `invalid version range for ${changeId}: min ${minSupported} exceeds max ${maxSupported}`
    );
  }
  if (maxSupported <= DEFAULT_VERSION) {
    throw new Error(`invalid max version for ${changeId}: ${maxSupported}`);
  }
}

function validateRecordedVersion(
  changeId: string,
  version: number,
  minSupported: number,
  maxSupported: number
): number {
  if (version < minSupported || version > maxSupported) {
    throw new UnsupportedWorkflowVersionError(
      changeId,
      version,
      minSupported,
      maxSupported
    );
  }
  return version;
}

function validateMarkerCommand(changeId: string, expected: CommandId, recorded: CommandId): void {
  if (Number(expected.seq) !== Number(recorded.seq)) {
    throw new Error(
      `nondeterminism: version marker ${changeId} command sequence changed: expected ${expected.seq}, found ${recorded.seq}`
    );
  }
}

function isWorkflowTaskFatalError(error: unknown): boolean {
  if (error instanceof UnsupportedWorkflowVersionError) {
    return true;
  }
  return (
    error instanceof Error &&
    (error.message.startsWith("nondeterminism:") ||
      error.message === "side effect key must not be empty")
  );
}

function durableFailureFromUnknown(error: unknown): DurableFailure {
  if (error instanceof ActivityFailureError) {
    return error.failure;
  }
  if (error instanceof ChildWorkflowFailureError) {
    return error.failure;
  }
  if (error instanceof ChildWorkflowMapFailureError) {
    return error.failure;
  }
  if (error instanceof ChildWorkflowCancelledError) {
    return {
      errorType: "ChildWorkflowCancelledError",
      message: error.reason,
      nonRetryable: true
    };
  }
  if (
    error &&
    typeof error === "object" &&
    "errorType" in error &&
    "message" in error &&
    typeof (error as { readonly errorType?: unknown }).errorType === "string" &&
    typeof (error as { readonly message?: unknown }).message === "string"
  ) {
    const failure = error as DurableFailure;
    return {
      errorType: failure.errorType,
      message: failure.message,
      nonRetryable: failure.nonRetryable,
      ...(failure.details === undefined ? {} : { details: failure.details })
    };
  }
  if (error instanceof Error) {
    return {
      errorType: error.name || "Error",
      message: error.message,
      nonRetryable: false
    };
  }
  return {
    errorType: "Error",
    message: String(error),
    nonRetryable: false
  };
}

function sameFingerprint(
  left: { readonly kind: string; readonly name: string; readonly inputDigest: string | null; readonly optionsDigest: string },
  right: { readonly kind: string; readonly name: string; readonly inputDigest: string | null; readonly optionsDigest: string }
): boolean {
  return (
    left.kind === right.kind &&
    left.name === right.name &&
    left.inputDigest === right.inputDigest &&
    left.optionsDigest === right.optionsDigest
  );
}

function activityOptionsDigest(options: {
  readonly taskQueue: string;
  readonly retryPolicy: RetryPolicy;
  readonly startToCloseTimeoutMs: number | null;
  readonly heartbeatTimeoutMs: number | null;
}): string {
  return digestBytes(JSON.stringify(options));
}

function activityMapTaskFromScheduled(scheduled: ActivityMapScheduled): ActivityMapTask {
  return {
    mapCommandId: scheduled.commandId,
    activityName: scheduled.activityName,
    taskQueue: scheduled.taskQueue,
    retryPolicy: scheduled.retryPolicy,
    startToCloseTimeoutMs: scheduled.startToCloseTimeoutMs,
    heartbeatTimeoutMs: scheduled.heartbeatTimeoutMs,
    inputManifest: scheduled.inputManifest,
    resultManifestName: scheduled.resultManifestName,
    maxInFlight: scheduled.maxInFlight
  };
}

function childWorkflowMapTaskFromScheduled(
  scheduled: ChildWorkflowMapScheduled
): ChildWorkflowMapTask {
  return {
    mapCommandId: scheduled.commandId,
    workflowType: scheduled.workflowType,
    taskQueue: scheduled.taskQueue,
    inputManifest: scheduled.inputManifest,
    resultManifestName: scheduled.resultManifestName,
    workflowIdPrefix: scheduled.workflowIdPrefix,
    maxInFlight: scheduled.maxInFlight,
    parentClosePolicy: scheduled.parentClosePolicy,
    failureMode: scheduled.failureMode
  };
}

function resolveTimerTimes(
  spec: RuntimeTimerSpec,
  nowMs: number
): { readonly fireAt: number; readonly fingerprintAt: number } {
  if (spec.kind === "sleep_until") {
    if (spec.fireAt === undefined || spec.fingerprintAt === undefined) {
      throw new Error("sleepUntil requires an absolute deadline");
    }
    return { fireAt: spec.fireAt, fingerprintAt: spec.fingerprintAt };
  }
  const duration = spec.durationMs ?? 0;
  return { fireAt: nowMs + duration, fingerprintAt: duration };
}

function timerWaitId(id: CommandId): WaitId {
  return waitId(`${id.runId}:timer:${id.seq}`);
}

function signalWaitId(id: CommandId): WaitId {
  return waitId(`${id.runId}:signal:${id.seq}`);
}
