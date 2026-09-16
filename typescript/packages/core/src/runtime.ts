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
  timerFingerprint,
  type CommandFingerprint
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
import { assertMapInputManifest } from "./map-manifest.js";
import { RetryPolicy, type ActivityCallOptions, type ChildWorkflowOptions } from "./options.js";
import { decodePayload, digestBytes, encodePayload, payloadDigest, type CodecId, type PayloadRef, type SchemaAdapter } from "./payload.js";
import { commandId, eventId, timestampMs, waitId, type CommandId, type DurableInput, type EventId, type RunId, type WaitId, type WorkflowId } from "./types.js";
import { assertDurableInputValue } from "./internal.js";
import { commandKey, sameCommandId } from "./provider-util.js";

const runtimeStorage = new AsyncLocalStorage<WorkflowRuntimeContext>();

// One entry per property `installNondeterminismGuards` overwrites, paired with
// the descriptor that property held *before* the first patch.
// `uninstallNondeterminismGuards` replays the ledger in reverse, which restores
// the original **identity** of every global rather than an equivalent
// replacement — host modules that captured `Date`, `setTimeout`, or `fetch` by
// reference, or that compare against them, see the same object again.
interface GuardPatch {
  readonly target: object;
  readonly key: PropertyKey;
  readonly original: PropertyDescriptor | undefined;
}

let nondeterminismGuardsInstalled = false;
const guardPatches: GuardPatch[] = [];
// The `process.nextTick` guard currently in place, so the reassert path that
// every execution construction runs can skip its work when nothing replaced it.
let installedProcessNextTickGuard: typeof process.nextTick | undefined;
let originalDateConstructor: DateConstructor | undefined;
let originalDateNow: (() => number) | undefined;
let originalMathRandom: (() => number) | undefined;
let originalPerformanceNow: (() => number) | undefined;
let originalCryptoRandomUUID: (() => `${string}-${string}-${string}-${string}-${string}`) | undefined;
let originalCryptoGetRandomValues: Crypto["getRandomValues"] | undefined;
let originalProcessHrtime: typeof process.hrtime | undefined;
let originalProcessHrtimeBigint: (() => bigint) | undefined;
let originalProcessCwd: typeof process.cwd | undefined;
let originalProcessUptime: typeof process.uptime | undefined;
let originalProcessNextTick: typeof process.nextTick | undefined;
let originalSetTimeout: typeof globalThis.setTimeout | undefined;
let originalSetInterval: typeof globalThis.setInterval | undefined;
let originalSetImmediate: typeof globalThis.setImmediate | undefined;
let originalQueueMicrotask: typeof globalThis.queueMicrotask | undefined;
let originalRequestAnimationFrame: typeof globalThis.requestAnimationFrame | undefined;
let originalRequestIdleCallback: typeof globalThis.requestIdleCallback | undefined;
let originalPromiseAll: PromiseConstructor["all"] | undefined;
let originalPromiseRace: PromiseConstructor["race"] | undefined;
let originalPromiseAllSettled: PromiseConstructor["allSettled"] | undefined;
let originalPromiseAny: PromiseConstructor["any"] | undefined;
let originalFetch: typeof globalThis.fetch | undefined;
let originalWebSocket: typeof globalThis.WebSocket | undefined;
let originalEventSource: typeof globalThis.EventSource | undefined;
let originalXMLHttpRequest: typeof globalThis.XMLHttpRequest | undefined;

/**
 * The activity task queue a `callActivity()` with no `taskQueue` resolves to
 * when the worker configured none, and the one an activity command's
 * `optionsDigest` hashes whatever the worker configured. Rust's
 * `TaskQueue::default()` is the same string.
 */
const DEFAULT_ACTIVITY_TASK_QUEUE = "default";

export interface PrepareWorkflowTaskOptions {
  readonly defaultActivityTaskQueue?: string;
  readonly defaultWorkflowTaskQueue?: string;
  readonly payloadCodec?: CodecId;
  readonly nowMs?: number;
  readonly liveSignals?: readonly SignalInboxRecord[];
  /**
   * Installs the process-global determinism guards when this execution is
   * constructed.
   *
   * The guards are a backstop, not the primary enforcement: `@durust/eslint-plugin`
   * plus `npm run lint:determinism` reject the same APIs statically at zero
   * runtime cost. Because the guards replace shared built-ins for the whole
   * process rather than for one execution, they are opt-out in development and
   * under a test harness, and opt-in in production.
   *
   * Leaving this undefined selects that default: guards install unless
   * `process.env.NODE_ENV === "production"`. `true` forces the backstop on in
   * production.
   *
   * `false` means "do not install", **not** "make sure they are off". The guards
   * are process-global, so an execution constructed with `false` still runs
   * under whatever guards an earlier execution — or an explicit
   * {@link installNondeterminismGuards} call — already put in place. Nothing
   * uninstalls on its own; {@link uninstallNondeterminismGuards} is the only way
   * to take them down.
   */
  readonly nondeterminismGuards?: boolean;
  /**
   * Streams the recorded history after a given event id, one chunk at a time.
   *
   * Supplying this puts the execution in chunked replay: `prefetchedHistory`
   * is read as the first chunk rather than as the whole history, and the
   * runtime pulls the next chunk when its replay window runs low, so replay
   * memory scales with the chunk size instead of the history length.
   *
   * Leaving it undefined keeps the whole-history contract: `prefetchedHistory`
   * must already reach `replayTargetEventId`, and a workflow that runs past
   * the end of it fails with the same "reached before replay history was fully
   * loaded" nondeterminism error as before.
   */
  readonly loadReplayHistory?: ReplayHistoryLoader;
  /**
   * How many recorded command events the replay window keeps in reserve beyond
   * whatever the current durable call needs. Defaults to
   * {@link REPLAY_WINDOW_LOOKAHEAD_EVENTS}.
   *
   * `Number.POSITIVE_INFINITY` asks for the whole recorded history, which is
   * how the worker repairs a {@link ReplayWindowOverrunError}: it re-replays
   * the run once with no reserve limit, trading this run's memory bound for
   * completing the task.
   */
  readonly replayWindowLookaheadEvents?: number;
}

/**
 * Raised when a synchronous durable marker needed a recorded event the replay
 * window had not loaded.
 *
 * `getVersion`, `patched`, and `deprecatePatch` return plain values rather than
 * thenables, so they cannot suspend to wait for the next chunk. They spend from
 * the window's reserve instead, and a long enough run of consecutive marker
 * calls with no awaited durable call between them exhausts it.
 *
 * Recoverable, not fatal, and repaired rather than reported: nothing has been
 * committed at the point this is raised and replay is deterministic, so the
 * owner can drop the execution and replay the run again with no reserve limit.
 * {@link Worker} does exactly that, once, which is why an operator does not see
 * a retry loop. The latch behind it (`toCommit()` refuses while it is set) is
 * what keeps a workflow's own `try`/`catch` from hiding the condition and
 * committing a task built on a marker the runtime could not verify.
 */
export class ReplayWindowOverrunError extends Error {
  readonly api: string;

  constructor(api: string, reserve: number) {
    super(
      `durust: ${api} needed recorded history that is not loaded yet; more than ` +
        `${reserve} consecutive synchronous durable markers ran without an awaited ` +
        `durable call between them. Raise the worker's historyFetchMaxEvents so the ` +
        `replay window carries a larger reserve.`
    );
    this.name = "ReplayWindowOverrunError";
    this.api = api;
  }
}

/**
 * Loads the recorded events after `afterEventId`, up to the claim's replay
 * target. Must return at least one event and must advance `lastEventId` past
 * `afterEventId`; the worker validates both before handing the chunk over.
 */
export type ReplayHistoryLoader = (
  afterEventId: EventId
) => Promise<{ readonly events: readonly HistoryEvent[]; readonly lastEventId: EventId }>;

/**
 * How many recorded command events the replay window keeps in reserve past
 * whatever the current durable call needs.
 *
 * It exists for the durable APIs that cannot park. `getVersion`, `patched`,
 * and `deprecatePatch` return plain values rather than thenables, so they
 * cannot suspend to wait for a chunk; every awaited durable call refills the
 * window to at least this many events, and those markers spend from that
 * reserve. A run of more than this many consecutive marker calls with no
 * awaited durable call between them exhausts it and is refused rather than
 * mis-replayed — see `#peekReplayEventForUnparkableApi`.
 *
 * Matched to the worker's default `historyFetchMaxEvents` so the steady-state
 * window is about two chunks.
 */
export const REPLAY_WINDOW_LOOKAHEAD_EVENTS = 128;

/**
 * True for the recorded events the replay cursor matches positionally.
 *
 * The complement — ready events — is indexed by command id instead and never
 * enters the replay window, so this is also the split that decides what the
 * window has to retain.
 */
export function isReplayCommandEvent(event: HistoryEvent): boolean {
  switch (event.eventType) {
    case "WorkflowStarted":
    case "ActivityCompleted":
    case "ActivityFailed":
    case "ActivityTimedOut":
    case "ActivityMapCompleted":
    case "ActivityMapFailed":
    case "ChildWorkflowMapCompleted":
    case "ChildWorkflowMapFailed":
    case "ChildWorkflowStarted":
    case "ChildWorkflowCompleted":
    case "ChildWorkflowFailed":
    case "ChildWorkflowCancelled":
    case "TimerFired":
    case "SignalConsumed":
      return false;
    default:
      return true;
  }
}

/**
 * Re-enters a durable API once the replay window has been refilled.
 *
 * The reason this is not just `gate.then(() => promise.then(...))`: a durable
 * API is allowed to refuse *synchronously* — a nondeterminism mismatch, or a
 * replayed activity failure raised as a throw. On the direct path the caller's
 * `await` sees that throw itself and turns it into a rejection. On the retry
 * path the throw happens inside a callback, and `await` ignores what a
 * thenable's `then` returns: it settles only when the handlers it passed in are
 * called. So a throw swallowed here leaves the awaiting frame parked forever
 * and puts an unowned rejection in front of Node's default
 * `--unhandled-rejections=throw`, which kills the worker.
 *
 * Handing the error to `onrejected` when there is one, and rethrowing into the
 * returned promise when there is not, makes the retry settle the caller exactly
 * the way the direct path does.
 */
function retryAfterReplayHistory<T>(
  gate: Promise<void>,
  retry: () => PromiseLike<T>,
  onrejected: ((reason: unknown) => T | PromiseLike<T>) | null | undefined
): PromiseLike<T> {
  return gate.then(() => {
    try {
      return retry();
    } catch (error: unknown) {
      if (onrejected) {
        return onrejected(error);
      }
      throw error;
    }
  });
}

const NO_PREFETCHED_HISTORY: readonly HistoryEvent[] = [];

/**
 * The claim without its event array, for the fields the context keeps for the
 * life of the run. Holding the array would pin a chunk of history — before
 * chunking, all of it — in a field nothing reads it from.
 */
function withoutPrefetchedHistory(claimed: ClaimedWorkflowTask): ClaimedWorkflowTask {
  return { ...claimed, prefetchedHistory: NO_PREFETCHED_HISTORY };
}

type HotWorkflowTaskOptions = PrepareWorkflowTaskOptions;

type HotSuspendResolution<T> =
  | { readonly kind: "Pending" }
  | { readonly kind: "Resolved"; readonly value: T }
  | { readonly kind: "Rejected"; readonly error: unknown };

interface HotWaiter {
  readonly key: string;
  // The deferred's own promise, kept so disposal can attach a handler before
  // rejecting it. See `disposeHot`.
  readonly promise: Promise<unknown>;
  tryResolve(): boolean;
  reject(error: unknown): void;
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
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

/**
 * The failure a workflow raises to close its run as failed, the TypeScript
 * spelling of a Rust workflow returning `Err(...)`. Any other value thrown out
 * of a handler is a workflow-code fault: the task is released and replayed
 * later instead of committing `WorkflowFailed`, so a redeploy recovers the run.
 */
export class WorkflowFailure extends Error implements DurableFailure {
  readonly errorType: string;
  readonly nonRetryable: boolean;
  readonly details?: PayloadRef<unknown>;

  constructor(
    message: string,
    options: {
      readonly errorType?: string;
      readonly nonRetryable?: boolean;
      readonly details?: PayloadRef<unknown>;
    } = {}
  ) {
    super(message);
    this.name = "WorkflowFailure";
    this.errorType = options.errorType ?? "WorkflowFailure";
    this.nonRetryable = options.nonRetryable ?? true;
    if (options.details !== undefined) {
      this.details = options.details;
    }
  }
}

/**
 * A workflow handler threw something that is not a durable failure. Routed
 * like nondeterminism: nothing is committed, the claim is released with the
 * nondeterminism backoff, and the next claim replays the run, so a fixed
 * redeploy recovers it. Mirrors Rust's `Error::TaskPanic`.
 */
export class WorkflowCodeError extends Error {
  constructor(cause: unknown) {
    super(`workflow task threw: ${cause instanceof Error ? cause.message : String(cause)}`, {
      cause
    });
    this.name = "WorkflowCodeError";
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

class ContinueAsNewRequested extends Error {
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

/**
 * Deterministic workflow time: the provider clock as observed by the task
 * that first evaluates this call, recorded as a side-effect marker so every
 * replay returns the same value. Each call records its own marker. The
 * TypeScript twin of `durust::now()`.
 */
export function now(): PromiseLike<number> {
  const observed = currentWorkflowRuntimeContext().nowMs();
  return new SideEffectDurablePromise("durust.now", () => observed);
}

export function publish<QueryState extends object>(view: DurableInput<QueryState>): void {
  currentWorkflowRuntimeContext().publish(view);
}

export function continueAsNew<Input extends object>(input: DurableInput<Input>): never {
  return currentWorkflowRuntimeContext().continueAsNew(input);
}

// Raised into every promise a disposed hot execution still owns: the parked
// durable-API waiters, and any later `nextCommit`/`advance`/`markCommitted`
// call. Its own class rather than a message convention, because the worker must
// be able to tell an abandoned execution apart from a workflow that genuinely
// failed: a disposal is never a `WorkflowFailed`, it means the task was dropped
// and the next claim replays the run from history.
export class HotWorkflowExecutionDisposedError extends Error {
  readonly reason: string;

  constructor(reason: string) {
    super(`durust: hot workflow execution disposed (${reason})`);
    this.name = "HotWorkflowExecutionDisposedError";
    this.reason = reason;
  }
}

export class HotWorkflowExecution {
  readonly #context: WorkflowRuntimeContext;
  #observedProgressVersion: number;
  #fatalError: unknown = null;
  #terminalCommitPending = false;
  #closed = false;
  #disposalError: HotWorkflowExecutionDisposedError | null = null;

  constructor(
    workflowDefinition: WorkflowDefinition<any, any, any, string>,
    input: object,
    claimed: ClaimedWorkflowTask,
    options: PrepareWorkflowTaskOptions = {}
  ) {
    if (shouldInstallNondeterminismGuards(options.nondeterminismGuards)) {
      installNondeterminismGuards();
    }
    this.#context = new WorkflowRuntimeContext(
      claimed,
      options,
      workflowDefinition.inputSchema,
      workflowDefinition.queryStateSchema
    );
    this.#observedProgressVersion = this.#context.hotProgressVersion();
    void runtimeStorage.run(this.#context, () => workflowDefinition.handler(input))
      .then((output: unknown) => {
        // These two early returns keep an abandoned execution's own state
        // coherent. They are deliberately *not* what stops a disposed run from
        // committing — `#assertLive()` and `toCommit()` refuse independently,
        // and removing both of these leaves the suite green, so do not read
        // them as the no-commit guard and do not delete them on finding that
        // out.
        //
        // What they prevent: a workflow that swallows the disposal and returns
        // would otherwise leave `#terminalCommitPending === true` and a
        // terminal event appended on a dead context, and
        // `completeWorkflow`'s `#assertTerminalReplayConsumed` can throw from
        // inside this `.then` — a rejection that lands in the `.catch` below,
        // which would then append a *second* terminal event to the same dead
        // context. An abandoned execution records nothing at all.
        if (this.#disposalError !== null) {
          return;
        }
        this.#context.completeWorkflow(output, workflowDefinition);
        this.#terminalCommitPending = true;
      })
      .catch((error: unknown) => {
        // Same rule on the failing side, and this is the arm that actually
        // runs: the rejection landing here is usually the disposal error
        // itself, raised into whichever durable API the workflow was parked on.
        if (this.#disposalError !== null) {
          return;
        }
        if (error instanceof ContinueAsNewRequested) {
          this.#terminalCommitPending = true;
          return;
        }
        if (isWorkflowTaskFatalError(error)) {
          this.#fatalError = error;
          this.#context.notifyHotProgress();
          return;
        }
        // A rejection that is not a durable failure is a workflow-code fault,
        // and the run keeps its progress: the task fails without committing.
        const failure = workflowRejectionFailure(error);
        if (failure === null) {
          this.#fatalError = new WorkflowCodeError(error);
          this.#context.notifyHotProgress();
          return;
        }
        // failWorkflow can itself raise fatal nondeterminism (terminal reached
        // with unconsumed recorded commands). Nothing catches a throw from this
        // .catch handler, so route it to #fatalError like other fatal errors
        // instead of leaving nextCommit() waiting forever.
        try {
          this.#context.failWorkflow(failure);
        } catch (failError: unknown) {
          this.#fatalError = failError;
          this.#context.notifyHotProgress();
          return;
        }
        this.#terminalCommitPending = true;
      });
  }

  get closed(): boolean {
    return this.#closed;
  }

  /**
   * Abandons this execution.
   *
   * The owner calls this whenever it drops its reference without committing —
   * commit conflict, a failed task, cache eviction. Rust drops the boxed future
   * and the whole tree of parked awaits dies with it; here the workflow's
   * promise chain would otherwise stay pending forever with its durable-API
   * waiters unsettled, so disposal raises {@link
   * HotWorkflowExecutionDisposedError} into each parked waiter, which unwinds
   * the handler and settles the chain.
   *
   * Idempotent: the same run can be evicted and then conflict. Terminal: a
   * disposed execution can never produce another commit, and everything its
   * abandoned frames do afterwards is inert.
   */
  dispose(reason: string): void {
    if (this.#disposalError !== null) {
      return;
    }
    this.#disposalError = new HotWorkflowExecutionDisposedError(reason);
    this.#context.disposeHot(this.#disposalError);
  }

  /**
   * Runs the workflow until it can commit, pulling replay history as it goes.
   *
   * The loop is the chunked-replay pump. Every time the workflow quiesces —
   * because it parked on a durable result, parked waiting for history it does
   * not have, or reached a terminal state — this checks whether the recorded
   * history through the replay target is fully loaded. While it is not, no
   * commit can be produced (the task would be committing against history it
   * never replayed), so it pulls the next chunk and lets the workflow run on.
   *
   * Exactly one chunk per quiescence, which is what keeps the window bounded:
   * `appendReplayHistory` reports progress itself only when the chunk woke
   * nothing, so a run blocked on a ready event several chunks ahead keeps
   * pulling, and a run that is consuming pulls only as fast as it consumes.
   *
   * Executions given a complete history and no loader skip the loop entirely
   * and behave exactly as before.
   */
  async nextCommit(): Promise<WorkflowTaskCommit> {
    this.#assertLive();
    for (;;) {
      await this.#context.waitForHotProgressAfter(this.#observedProgressVersion);
      // Re-checked after the await: disposal can land while this call is
      // parked, and the commit it would otherwise return describes a run the
      // owner has already abandoned.
      this.#assertLive();
      this.#observedProgressVersion = this.#context.hotProgressVersion();
      if (this.#fatalError !== null) {
        throw this.#fatalError;
      }
      if (!this.#context.replayHistoryComplete() && this.#context.canLoadReplayHistory()) {
        await this.#context.loadNextReplayChunk();
        this.#assertLive();
        continue;
      }
      return this.#context.toCommit();
    }
  }

  async advance(
    claimed: ClaimedWorkflowTask,
    options: HotWorkflowTaskOptions = {}
  ): Promise<WorkflowTaskCommit> {
    this.#assertLive();
    this.#context.advanceHotClaim(claimed, options);
    return this.nextCommit();
  }

  /**
   * The replay-window refusal this execution hit, if any.
   *
   * Read by the owner after a failed `nextCommit()`. The refusal is thrown into
   * workflow code, which may catch it and carry on to diverge in some other
   * way, so the error that actually surfaces is not always the overrun itself;
   * this reports the condition regardless of which error came out.
   */
  replayWindowOverrun(): ReplayWindowOverrunError | null {
    return this.#context.replayWindowOverrun();
  }

  markCommitted(newTailEventId: EventId): void {
    this.#assertLive();
    this.#context.markHotCommitAccepted(newTailEventId);
    if (this.#terminalCommitPending) {
      this.#closed = true;
    }
  }

  #assertLive(): void {
    if (this.#disposalError !== null) {
      throw this.#disposalError;
    }
    if (this.#closed) {
      throw new Error("hot workflow execution is closed");
    }
  }
}

function currentWorkflowRuntimeContext(): WorkflowRuntimeContext {
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

/**
 * Decides whether constructing a workflow execution installs the process-global
 * determinism guards.
 *
 * An explicit request always wins. Otherwise the guards install unless
 * `NODE_ENV` says the host is in production, which makes them opt-out in
 * development and under a test harness and opt-in in production. The asymmetry
 * is deliberate: the guards replace shared built-ins for the *whole process*,
 * including the host's logging, telemetry, and database code, so their cost and
 * their compatibility risk are paid by code that has nothing to do with
 * workflows. Static enforcement (`@durust/eslint-plugin` plus
 * `npm run lint:determinism`) rejects the same APIs in workflow sources before
 * the process ever starts and costs nothing at runtime, so production keeps the
 * static gate and drops the process-wide one.
 */
function shouldInstallNondeterminismGuards(requested: boolean | undefined): boolean {
  if (requested !== undefined) {
    return requested;
  }
  return !isProductionHost();
}

function isProductionHost(): boolean {
  // Reads the live `process.env` every time, and holds no captured copy of it.
  // An earlier revision consulted an environment reference captured at install
  // time; because the guarded accessor's setter reassigned that reference
  // outside the restore ledger, a host doing `process.env = {...}` while the
  // guards were up left the capture pointing at an object that uninstall then
  // discarded — after which every execution read a stale `NODE_ENV`, decided
  // "production", and silently stopped installing guards for the life of the
  // process. `process.env` is no longer patched at all, so the live read is both
  // correct and incapable of going stale.
  return process.env.NODE_ENV === "production";
}

/**
 * Replaces the nondeterministic process globals with guarded wrappers that
 * throw when called from inside workflow code.
 *
 * Idempotent, and *not* reference counted: a second call while the guards are
 * up only reasserts the `process.nextTick` patch (which other instrumentation
 * commonly overwrites) and leaves everything else alone, so it can never
 * capture a guard as its own "original". One
 * {@link uninstallNondeterminismGuards} undoes any number of installs.
 */
export function installNondeterminismGuards(): void {
  if (nondeterminismGuardsInstalled) {
    installProcessNextTickGuard({ record: false });
    return;
  }
  nondeterminismGuardsInstalled = true;
  originalDateConstructor = Date;
  originalDateNow = originalDateConstructor.now.bind(originalDateConstructor);
  originalMathRandom = Math.random.bind(Math);
  originalPerformanceNow = globalThis.performance?.now.bind(globalThis.performance);
  originalCryptoRandomUUID = globalThis.crypto?.randomUUID?.bind(globalThis.crypto);
  originalCryptoGetRandomValues = globalThis.crypto?.getRandomValues.bind(globalThis.crypto);
  originalProcessHrtime = process.hrtime.bind(process) as typeof process.hrtime;
  originalProcessHrtimeBigint = process.hrtime.bigint.bind(process.hrtime);
  originalProcessCwd = process.cwd.bind(process);
  originalProcessUptime = process.uptime.bind(process);
  originalProcessNextTick = process.nextTick.bind(process) as typeof process.nextTick;
  originalSetTimeout = globalThis.setTimeout.bind(globalThis);
  originalSetInterval = globalThis.setInterval.bind(globalThis);
  originalSetImmediate = globalThis.setImmediate?.bind(globalThis);
  originalQueueMicrotask = globalThis.queueMicrotask.bind(globalThis);
  originalRequestAnimationFrame = globalThis.requestAnimationFrame?.bind(globalThis);
  originalRequestIdleCallback = globalThis.requestIdleCallback?.bind(globalThis);
  originalPromiseAll = Promise.all.bind(Promise) as PromiseConstructor["all"];
  originalPromiseRace = Promise.race.bind(Promise) as PromiseConstructor["race"];
  originalPromiseAllSettled = Promise.allSettled.bind(Promise) as PromiseConstructor["allSettled"];
  originalPromiseAny = Promise.any.bind(Promise) as PromiseConstructor["any"];
  originalFetch = globalThis.fetch?.bind(globalThis);
  originalWebSocket = globalThis.WebSocket;
  originalEventSource = globalThis.EventSource;
  originalXMLHttpRequest = globalThis.XMLHttpRequest;
  const guardedDate = new Proxy(originalDateConstructor, {
    apply(target, thisArg, args) {
      if (args.length === 0) {
        assertNotInWorkflowRuntime("Date()", "workflow time APIs such as sleepUntil()", {
          allowInSideEffect: true
        });
      }
      return Reflect.apply(target, thisArg, args);
    },
    construct(target, args, newTarget) {
      if (args.length === 0) {
        assertNotInWorkflowRuntime("new Date()", "workflow time APIs such as sleepUntil()", {
          allowInSideEffect: true
        });
      }
      return Reflect.construct(target, args, newTarget);
    }
  });
  patchGuardedValue(globalThis, "Date", guardedDate);
  // `now` is patched on the *original* constructor, not on the proxy. A proxy
  // with only `apply` and `construct` traps forwards `defineProperty` to its
  // target, so defining `now` on `guardedDate` mutates the real `Date` anyway —
  // it just does it invisibly, outside the restore ledger, which left `Date.now`
  // permanently replaced and made a reinstall capture the previous guard as its
  // own original (unbounded recursion). Patching the target explicitly keeps the
  // same reachability, since the proxy forwards `get` too, and makes the
  // mutation restorable.
  patchGuardedValue(originalDateConstructor, "now", () => {
    assertNotInWorkflowRuntime("Date.now()", "workflow time APIs such as sleepUntil()", {
      allowInSideEffect: true
    });
    return originalDateNow?.() ?? 0;
  });
  patchGuardedValue(Math, "random", () => {
    assertNotInWorkflowRuntime(
      "Math.random()",
      "sideEffect() for recorded nondeterministic values",
      { allowInSideEffect: true }
    );
    return originalMathRandom?.() ?? 0;
  });
  if (globalThis.performance !== undefined) {
    patchGuardedValue(globalThis.performance, "now", () => {
      assertNotInWorkflowRuntime("performance.now()", "workflow time APIs such as sleepUntil()", {
        allowInSideEffect: true
      });
      return originalPerformanceNow?.() ?? 0;
    });
  }
  if (globalThis.crypto !== undefined) {
    if (typeof globalThis.crypto.randomUUID === "function") {
      patchGuardedValue(globalThis.crypto, "randomUUID", () => {
        assertNotInWorkflowRuntime(
          "crypto.randomUUID()",
          "sideEffect() for recorded nondeterministic values",
          { allowInSideEffect: true }
        );
        return originalCryptoRandomUUID?.() ?? "00000000-0000-4000-8000-000000000000";
      });
    }
    patchGuardedValue(
      globalThis.crypto,
      "getRandomValues",
      (<T extends Parameters<Crypto["getRandomValues"]>[0]>(array: T): T => {
        assertNotInWorkflowRuntime(
          "crypto.getRandomValues()",
          "sideEffect() for recorded nondeterministic values",
          { allowInSideEffect: true }
        );
        if (originalCryptoGetRandomValues === undefined) {
          return array;
        }
        return originalCryptoGetRandomValues(array) as T;
      }) as Crypto["getRandomValues"]
    );
  }
  const guardedHrtime = ((time?: [number, number]) => {
    assertNotInWorkflowRuntime("process.hrtime()", "workflow time APIs such as sleepUntil()", {
      allowInSideEffect: true
    });
    if (time === undefined) {
      return originalProcessHrtime?.() ?? [0, 0];
    }
    return originalProcessHrtime?.(time) ?? [0, 0];
  }) as typeof process.hrtime;
  guardedHrtime.bigint = (() => {
    assertNotInWorkflowRuntime(
      "process.hrtime.bigint()",
      "workflow time APIs such as sleepUntil()",
      { allowInSideEffect: true }
    );
    return originalProcessHrtimeBigint?.() ?? 0n;
  }) as typeof process.hrtime.bigint;
  patchGuardedValue(process, "hrtime", guardedHrtime);
  // `process.env` is deliberately NOT patched at all — neither wrapped in a
  // Proxy nor fronted by an accessor. 0017 row 5C took the second option its
  // scope offers ("guard the module-level accessor only, *or* drop it in favour
  // of the lint rule"), because an accessor that throws on every `process.env`
  // read is a false-positive generator, not a determinism guard:
  //
  //   - `Console` detects colour support whenever an argument is not already a
  //     string, and that detection reads `process.env`. So
  //     `console.log(someObject)` and `console.error(someError)` throw inside
  //     workflow code — reporting `process.env`, an API the author never wrote,
  //     with replacement advice that cannot be acted on. Measured, uncached:
  //     `console.log("s")` costs 0 environment reads, `console.log({...})` 2,
  //     `console.error(err)` 1. It is not `util.inspect`, `util.format`, or
  //     `JSON.stringify` — those read the environment zero times.
  //   - Guards default on in development and test, which is exactly where people
  //     log, and where an escaped-context rejection cannot even be printed
  //     because the handler's own `console.log` throws.
  //   - Any host library on a workflow's synchronous call path that reads an env
  //     var fails the whole task, and the worker then retries it with
  //     nondeterminism backoff.
  //
  // `durust/no-hidden-io` rejects `process.env` statically in every spelling —
  // direct read, `const env = process.env`, `const { X } = process.env`,
  // `const { env } = process`, `process["env"]`, `globalThis.process.env` — in
  // any linted workflow source, at zero runtime cost and with the right API
  // named in the diagnostic. That is the enforcement path; see the
  // determinism-lint suite.
  //
  // Not patching it also removes a whole class of failure this module had: the
  // accessor's setter used to reassign the module's captured environment
  // reference outside the restore ledger, so `process.env = {...}` while guards
  // were up left that capture pointing at a discarded object after uninstall.
  // `isProductionHost` reads `process.env` directly for that reason — there is
  // no captured copy left to go stale.
  patchGuardedValue(process, "cwd", (() => {
    assertNotInWorkflowRuntime(
      "process.cwd()",
      "workflow input or sideEffect() for recorded working directory",
      { allowInSideEffect: true }
    );
    return originalProcessCwd?.() ?? "";
  }) as typeof process.cwd);
  // Kept against 0017 row 5B's drop list, deliberately. `process.uptime()` is a
  // monotonic clock — the same defect class as `performance.now()` and
  // `process.hrtime()`, both of which stay guarded — not process introspection
  // like `cpuUsage`/`memoryUsage`/`resourceUsage`. Dropping it would leave three
  // monotonic clocks with two enforced and one not, and the blast-radius
  // argument that justifies the other four does not transfer: nothing's hot path
  // calls `process.uptime()`, so the guard costs nil.
  patchGuardedValue(process, "uptime", (() => {
    assertNotInWorkflowRuntime("process.uptime()", "workflow time APIs such as sleepUntil()", {
      allowInSideEffect: true
    });
    return originalProcessUptime?.() ?? 0;
  }) as typeof process.uptime);
  installProcessNextTickGuard({ record: true });
  patchGuardedValue(globalThis, "setTimeout", ((
    ...args: Parameters<typeof globalThis.setTimeout>
  ) => {
    assertNotInWorkflowRuntime("setTimeout()", "durust sleep() or sleepUntil()");
    return originalSetTimeout?.(...args);
  }) as typeof globalThis.setTimeout);
  patchGuardedValue(globalThis, "setInterval", ((
    ...args: Parameters<typeof globalThis.setInterval>
  ) => {
    assertNotInWorkflowRuntime("setInterval()", "durust sleep() or recurring workflow timers");
    return originalSetInterval?.(...args);
  }) as typeof globalThis.setInterval);
  if (originalSetImmediate !== undefined) {
    patchGuardedValue(globalThis, "setImmediate", ((
      ...args: Parameters<typeof globalThis.setImmediate>
    ) => {
      assertNotInWorkflowRuntime("setImmediate()", "durust durable operations");
      return originalSetImmediate?.(...args);
    }) as typeof globalThis.setImmediate);
  }
  patchGuardedValue(globalThis, "queueMicrotask", ((callback: VoidFunction) => {
    assertNotInWorkflowRuntime("queueMicrotask()", "durust durable operations");
    return originalQueueMicrotask?.(callback);
  }) as typeof globalThis.queueMicrotask);
  if (originalRequestAnimationFrame !== undefined) {
    patchGuardedValue(globalThis, "requestAnimationFrame", ((callback: FrameRequestCallback) => {
      assertNotInWorkflowRuntime("requestAnimationFrame()", "durust sleep() or sleepUntil()");
      return originalRequestAnimationFrame?.(callback) ?? 0;
    }) as typeof globalThis.requestAnimationFrame);
  }
  if (originalRequestIdleCallback !== undefined) {
    patchGuardedValue(globalThis, "requestIdleCallback", ((
      ...args: Parameters<typeof globalThis.requestIdleCallback>
    ) => {
      assertNotInWorkflowRuntime("requestIdleCallback()", "durust durable operations");
      return originalRequestIdleCallback?.(...args) ?? 0;
    }) as typeof globalThis.requestIdleCallback);
  }
  if (originalFetch !== undefined) {
    patchGuardedValue(globalThis, "fetch", ((...args: Parameters<typeof globalThis.fetch>) => {
      assertNotInWorkflowRuntime("fetch()", "callActivity() for external I/O");
      return originalFetch?.(...args) ?? Promise.reject(new Error("fetch is unavailable"));
    }) as typeof globalThis.fetch);
  }
  installNetworkConstructorGuard("WebSocket", originalWebSocket, "callActivity() for network I/O");
  installNetworkConstructorGuard("EventSource", originalEventSource, "callActivity() for network I/O");
  installNetworkConstructorGuard(
    "XMLHttpRequest",
    originalXMLHttpRequest,
    "callActivity() for network I/O"
  );
  patchGuardedValue(Promise, "all", ((values: Iterable<unknown>) => {
    assertNotInWorkflowRuntime("Promise.all()", "durust join() or joinAll()");
    return originalPromiseAll?.(values as Iterable<unknown>);
  }) as PromiseConstructor["all"]);
  patchGuardedValue(Promise, "race", ((values: Iterable<unknown>) => {
    assertNotInWorkflowRuntime("Promise.race()", "durust select() or selectAll()");
    return originalPromiseRace?.(values as Iterable<unknown>);
  }) as PromiseConstructor["race"]);
  patchGuardedValue(Promise, "allSettled", ((values: Iterable<unknown>) => {
    assertNotInWorkflowRuntime(
      "Promise.allSettled()",
      "durust join()/joinAll() plus explicit error handling"
    );
    return originalPromiseAllSettled?.(values as Iterable<unknown>);
  }) as PromiseConstructor["allSettled"]);
  patchGuardedValue(Promise, "any", ((values: Iterable<unknown>) => {
    assertNotInWorkflowRuntime("Promise.any()", "durust select() or selectAll()");
    return originalPromiseAny?.(values as Iterable<unknown>);
  }) as PromiseConstructor["any"]);
}

/**
 * Restores every global {@link installNondeterminismGuards} replaced and returns
 * whether anything was restored.
 *
 * Restores identity, not merely behaviour: each property gets back the exact
 * descriptor it held before the first patch, so `Date`, `setTimeout`, and
 * `fetch` are the same objects a module that loaded before the first workflow
 * already holds.
 *
 * Not reference counted: one call undoes any number of installs.
 *
 * Absolute, and this is a deliberate trade rather than a limitation. Telling a
 * later third-party wrapper apart from a guard is easy — record the guard value
 * and restore only when the property still holds it — but that "polite" unpatch
 * would leave an instrumentation wrapper in place that closes over the guard,
 * so the guard would keep throwing inside workflow code after an uninstall that
 * reported success, and `Date === originalDate` would be false. Row 5D's
 * contract is identity restoration, and only the absolute unpatch can promise
 * it. The cost: an APM agent that wraps `Date.now` *after* the worker starts
 * loses its wrapper when the guards come down.
 *
 * ## Calling this while a workflow is mid-flight
 *
 * Permitted, and it neither fails nor corrupts the run. Guards are stateless
 * call-through wrappers over captured originals; the captured originals are
 * deliberately *not* cleared here, so a wrapper already on the stack — or one a
 * caller aliased while the guards were up — keeps forwarding to the real
 * built-in instead of falling back to a placeholder. `AsyncLocalStorage`, the
 * durable APIs, and every in-flight execution are untouched.
 *
 * What does change is enforcement: from the instant this returns, workflow code
 * in *already running* executions can call `Date.now()` and friends without
 * being rejected, and any resulting replay divergence surfaces later as a
 * fingerprint mismatch rather than immediately. Uninstall on worker shutdown,
 * or between test cases, not while tasks are being served.
 */
export function uninstallNondeterminismGuards(): boolean {
  if (!nondeterminismGuardsInstalled) {
    return false;
  }
  for (let index = guardPatches.length - 1; index >= 0; index -= 1) {
    const patch = guardPatches[index];
    if (patch === undefined) {
      continue;
    }
    if (patch.original === undefined) {
      Reflect.deleteProperty(patch.target, patch.key);
      continue;
    }
    Object.defineProperty(patch.target, patch.key, patch.original);
  }
  guardPatches.length = 0;
  nondeterminismGuardsInstalled = false;
  installedProcessNextTickGuard = undefined;
  return true;
}

// Records the descriptor `key` holds today, then overwrites it. Recording
// happens exactly once per property per install, so the ledger always restores
// the pre-guard state rather than an intermediate one.
function patchGuarded(target: object, key: PropertyKey, descriptor: PropertyDescriptor): void {
  guardPatches.push({
    target,
    key,
    original: Object.getOwnPropertyDescriptor(target, key)
  });
  Object.defineProperty(target, key, descriptor);
}

function patchGuardedValue(target: object, key: PropertyKey, value: unknown): void {
  patchGuarded(target, key, { configurable: true, writable: true, value });
}

// `record: false` is the reassert path. Other instrumentation replaces
// `process.nextTick` routinely, so every install call puts the guard back on
// top — but only the first one may write the ledger, or a reassert would record
// the previous guard as the "original" and uninstall would leave it behind.
//
// The identity check makes the common reassert free. Every
// `HotWorkflowExecution` construction reaches this function, so allocating a
// closure and redefining a property on `process` unconditionally would put both
// on the per-task hot path for no benefit in the overwhelmingly common case
// where nothing replaced the guard.
function installProcessNextTickGuard(options: { readonly record: boolean }): void {
  if (process.nextTick === installedProcessNextTickGuard) {
    return;
  }
  const guarded = ((callback: (...args: any[]) => void, ...args: any[]) => {
    assertNotInWorkflowRuntime("process.nextTick()", "durust durable operations");
    return originalProcessNextTick?.(callback, ...args);
  }) as typeof process.nextTick;
  installedProcessNextTickGuard = guarded;
  if (options.record) {
    patchGuardedValue(process, "nextTick", guarded);
    return;
  }
  Object.defineProperty(process, "nextTick", {
    configurable: true,
    writable: true,
    value: guarded
  });
}

function installNetworkConstructorGuard<
  Constructor extends (new (...args: any[]) => any) | undefined
>(
  globalName: "WebSocket" | "EventSource" | "XMLHttpRequest",
  originalConstructor: Constructor,
  replacement: string
): void {
  if (originalConstructor === undefined) {
    return;
  }
  const guarded = new Proxy(originalConstructor, {
    apply(target, thisArg, args) {
      assertNotInWorkflowRuntime(`${globalName}()`, replacement);
      return Reflect.apply(target, thisArg, args);
    },
    construct(target, args, newTarget) {
      assertNotInWorkflowRuntime(`new ${globalName}()`, replacement);
      return Reflect.construct(target, args, newTarget);
    }
  });
  patchGuardedValue(globalThis, globalName, guarded);
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

interface RuntimeTimerSpec {
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
>(name: Name, payloadSchema?: SchemaAdapter<Payload>): SignalDefinition<Payload, Name> {
  return new SignalDurablePromise<Payload, Name>(name, payloadSchema);
}

class WorkflowRuntimeContext {
  #claimed: ClaimedWorkflowTask;
  readonly #defaultActivityTaskQueue: string;
  readonly #defaultWorkflowTaskQueue: string;
  readonly #payloadCodec: CodecId;
  readonly #workflowInputSchema: SchemaAdapter<unknown> | undefined;
  readonly #queryStateSchema: SchemaAdapter<unknown> | undefined;
  #nowMs: number;
  #liveSignals: SignalInboxRecord[];
  // The replay window: the recorded *command* events loaded so far that the
  // workflow has not matched yet, plus the ones it already has, which are
  // dropped on the next refill.
  //
  // A window, not the history. Replay used to hand this context every event
  // through the replay target in one array, so memory scaled with history
  // length and the README's "No Event History Limit" was true only of the
  // provider. Now the worker streams `historyFetchMaxEvents`-sized chunks and
  // the context pulls the next one when it is running low, exactly as the Rust
  // runtime's `needs_more_history_after`/`append_replay_events` pair does.
  #replayEvents: HistoryEvent[] = [];
  // Highest event id loaded into the window, which is what makes replay
  // "complete": everything through `replayTargetEventId` has been seen.
  #lastLoadedEventId: EventId;
  // Pulls the chunk after a given event id. Absent for callers that hand this
  // context a complete history up front (`prepareWorkflowTask`, and every hot
  // wake), in which case the window is never short and never refills.
  readonly #loadReplayHistory: ReplayHistoryLoader | null;
  // Frames parked at a durable API because the window ran low. Settled by
  // `appendReplayHistory`, rejected by `disposeHot`; never left unowned.
  #replayHistoryWaiters: Deferred<void>[] = [];
  // Set when a terminal event was appended before the whole recorded history
  // was loaded, so "did the workflow leave a recorded command unconsumed?"
  // could not be answered yet. `toCommit()` answers it once loading finishes.
  #pendingTerminalReplayCheck: string | null = null;
  // How many recorded command events the window keeps in reserve for the
  // durable markers that cannot park. `Infinity` means "load everything", which
  // is what a replay repaired after {@link ReplayWindowOverrunError} runs with.
  readonly #replayWindowLookahead: number;
  // Latched when a synchronous marker was refused. Separate from throwing:
  // the throw lands in workflow code, which may catch it, and a task whose
  // marker could not be verified must not commit whether or not the workflow
  // noticed.
  #replayWindowOverrun: ReplayWindowOverrunError | null = null;
  // Per-command indexes over the ready events (completions, failures, timer
  // fires, child lifecycle) loaded so far. They are the single consumption path
  // for ready events: the replay cursor skips over ready events and never hands
  // one out, so every value a workflow observes leaves through
  // `#takeReadyEvent`, which *removes* the entry.
  //
  // Removal on consume is what bounds them. Holding an entry for the lifetime
  // of the run means a hot workflow retains one `HistoryEvent` — with its
  // payload — per activity it has ever completed, which is exactly the
  // unbounded hot-path collection `AGENTS.md` forbids. What is left after a
  // task is only what that task did not consume: a spawned handle nobody has
  // awaited yet, a `select` branch that lost. Those survive across commits on
  // purpose, the way Rust carries `CachedWorkflow::unconsumed_indexes`, so a
  // run holding an unawaited handle stays hot instead of cold-replaying.
  //
  // Every value therefore has exactly one owner after it is taken. The two
  // places where a value can legitimately be asked for twice own a memo of
  // their own instead of a second index read: `RuntimeActivityHandle` and
  // `RuntimeChildWorkflowHandle` (`result()` may be awaited more than once),
  // and the join/select branch closures, which are re-probed on every ingest
  // until the whole composite settles.
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
  // A signal's consumption is recorded under the wait's command id, which may
  // be far behind the commands recorded since the wait registered, so it is a
  // ready event keyed by command like a timer fire rather than a positional
  // command event. Mirrors Rust's `ReadyEventIndexes::consumed_signals`.
  readonly #signalConsumptions = new Map<string, HistoryEvent>();
  #replayCursor = 0;
  #nextCommandSeq = 1;
  readonly #appendEvents: NewHistoryEvent[] = [];
  readonly #upsertWaits: WaitRecord[] = [];
  readonly #deleteWaits: WaitId[] = [];
  // Commands whose operational state this task withdraws. The only producer is
  // a losing `select` branch that registered an activity: its wait-less
  // counterpart is `#deleteWaits`, and the provider applies both from the same
  // commit. Mirrors Rust's `RuntimeContext::cancel_commands`, which
  // `ActivityFuture::__durust_cancel_branch` writes to.
  readonly #cancelCommands: CommandId[] = [];
  readonly #consumeSignals: string[] = [];
  readonly #scheduleActivities: ActivityTask[] = [];
  readonly #scheduleActivityMaps: ActivityMapTask[] = [];
  readonly #startChildWorkflows: ChildWorkflowStartRequested[] = [];
  readonly #scheduleChildWorkflowMaps: ChildWorkflowMapTask[] = [];
  #queryProjection: PayloadRef | null = null;
  #allowNondeterministicGlobalsDepth = 0;
  // The durable API whose user-controlled code is running right now, or null
  // when none is. Every durable API allocates its command seq before it appends
  // its event, and runs user code in between: a `sideEffect` callback, or the
  // payload conversion every other command performs — `encodePayload`, a schema
  // adapter's `encode()`, a `toJSON()`, a `valueOf()`, a fingerprint template
  // literal. A durable API called from inside that window takes the next seq
  // and appends its own event first, recording an out-of-order pair that can
  // never replay. Both frames are the same invariant and cannot overlap — the
  // side-effect callback's window closes before its value is encoded — so they
  // share one pair of fields and the gate pays one null compare.
  //
  // Two fields rather than one formatted string: entering a window happens on
  // every durable call and must cost reference stores only. The message is
  // assembled on the throwing path.
  //
  // Deliberately separate from `#allowNondeterministicGlobalsDepth`. That
  // counter relaxes the nondeterministic-globals guard; this one tightens the
  // durable-API guard. They are entered from the same place today, but they are
  // independent invariants and a future site that wants one must not silently
  // get the other.
  #userCodeFrameApi: string | null = null;
  #userCodeFrameDetail: string | null = null;
  // Set once the owning `HotWorkflowExecution` is disposed. Frames of an
  // abandoned workflow can still be scheduled after that point — a detached
  // continuation left behind by an async `sideEffect` callback is the known
  // case — so command allocation and commit production both refuse once it is
  // set, making those frames inert rather than merely unobserved.
  #disposalError: HotWorkflowExecutionDisposedError | null = null;
  readonly #hotWaiters = new Map<string, HotWaiter>();
  #hotProgressVersion = 0;
  #hotProgressWaiters: Deferred<void>[] = [];

  constructor(
    claimed: ClaimedWorkflowTask,
    options: PrepareWorkflowTaskOptions,
    workflowInputSchema?: SchemaAdapter<unknown>,
    queryStateSchema?: SchemaAdapter<unknown>
  ) {
    // Deliberately stripped of `prefetchedHistory`. The claim is kept for the
    // life of the run — it carries `runId` and `replayTargetEventId` — so
    // holding its event array here would pin the first chunk (and, before
    // chunking, the entire history) for as long as the workflow is hot, which
    // is the leak the window exists to close.
    this.#claimed = withoutPrefetchedHistory(claimed);
    this.#defaultActivityTaskQueue =
      options.defaultActivityTaskQueue ?? DEFAULT_ACTIVITY_TASK_QUEUE;
    this.#defaultWorkflowTaskQueue = options.defaultWorkflowTaskQueue ?? "default";
    this.#payloadCodec = options.payloadCodec ?? "MessagePack";
    this.#workflowInputSchema = workflowInputSchema;
    this.#queryStateSchema = queryStateSchema;
    this.#nowMs = options.nowMs ?? 0;
    this.#liveSignals = [...(options.liveSignals ?? [])];
    this.#loadReplayHistory = options.loadReplayHistory ?? null;
    this.#replayWindowLookahead =
      options.replayWindowLookaheadEvents ?? REPLAY_WINDOW_LOOKAHEAD_EVENTS;
    this.#lastLoadedEventId = eventId(0);
    this.#ingestReplayChunk(
      claimed.prefetchedHistory,
      claimed.prefetchedHistory.at(-1)?.eventId ?? eventId(0)
    );
  }

  /**
   * Folds one loaded chunk into the window.
   *
   * Drops everything the workflow has already matched before appending, which
   * is what keeps the window proportional to the chunk size instead of the
   * history length. Ready events never enter the window at all: they go into
   * the per-command indexes and leave through `#takeReadyEvent`.
   */
  #ingestReplayChunk(events: readonly HistoryEvent[], lastEventId: EventId): void {
    this.#dropMatchedReplayEvents();
    this.#ingestHistory(events);
    for (const event of events) {
      if (isReplayCommandEvent(event)) {
        this.#replayEvents.push(event);
      }
    }
    this.#advanceLoadedWatermark(lastEventId);
  }

  #dropMatchedReplayEvents(): void {
    if (this.#replayCursor > 0) {
      this.#replayEvents.splice(0, this.#replayCursor);
      this.#replayCursor = 0;
    }
  }

  #advanceLoadedWatermark(lastEventId: EventId): void {
    if (Number(lastEventId) > Number(this.#lastLoadedEventId)) {
      this.#lastLoadedEventId = lastEventId;
    }
  }

  /** The provider clock this task observed, in milliseconds. */
  nowMs(): number {
    return this.#nowMs;
  }

  /** True once every recorded event through the replay target is loaded. */
  replayHistoryComplete(): boolean {
    return Number(this.#lastLoadedEventId) >= Number(this.#claimed.replayTargetEventId);
  }

  /** The event id the next chunk must start after. */
  lastLoadedEventId(): EventId {
    return this.#lastLoadedEventId;
  }

  canLoadReplayHistory(): boolean {
    return this.#loadReplayHistory !== null;
  }

  /**
   * The gate every durable API passes through before it touches the window.
   *
   * Returns a promise to park on when the window is too short to serve the
   * caller, and `null` when it can proceed. `required` is the number of
   * recorded command events the caller may match in one uninterrupted go — one
   * for a single durable call, one per branch plus the winner marker for a
   * `select`.
   *
   * The gate reserves {@link REPLAY_WINDOW_LOOKAHEAD_EVENTS} beyond that.
   * `getVersion`, `patched`, and `deprecatePatch` are synchronous and return
   * plain values, so they cannot park; the reserve is what they consume from,
   * and it is why a run of consecutive marker calls between two awaits has a
   * documented ceiling instead of a corrupting failure mode.
   */
  replayHistoryGate(required: number): Promise<void> | null {
    if (!this.#replayWindowShort(required)) {
      return null;
    }
    if (this.#disposalError !== null) {
      return Promise.reject(this.#disposalError);
    }
    const deferred = createDeferred<void>();
    this.#replayHistoryWaiters.push(deferred);
    // Wakes the commit pump, which is what performs the load.
    this.notifyHotProgress();
    return deferred.promise;
  }

  #replayWindowShort(required: number): boolean {
    if (this.replayHistoryComplete() || this.#loadReplayHistory === null) {
      return false;
    }
    return this.#replayEvents.length - this.#replayCursor <
      required + this.#replayWindowLookahead;
  }

  /**
   * Loads the next chunk and restarts everything it unblocked.
   *
   * Called only by the commit pump in {@link HotWorkflowExecution.nextCommit},
   * so loading is serialized with commit production and never overlaps itself.
   */
  async loadNextReplayChunk(): Promise<void> {
    const loader = this.#loadReplayHistory;
    if (loader === null) {
      throw new Error(
        "durust: replay history is incomplete and this execution has no history loader"
      );
    }
    const chunk = await loader(this.#lastLoadedEventId);
    if (this.#disposalError !== null) {
      throw this.#disposalError;
    }
    this.appendReplayHistory(chunk.events, chunk.lastEventId);
  }

  appendReplayHistory(events: readonly HistoryEvent[], lastEventId: EventId): void {
    this.#ingestReplayChunk(events, lastEventId);
    const parked = this.#replayHistoryWaiters;
    this.#replayHistoryWaiters = [];
    for (const waiter of parked) {
      waiter.resolve(undefined);
    }
    const settled = this.#tryResolveHotWaiters();
    if (parked.length === 0 && settled === 0) {
      // This chunk woke nothing, so no frame is going to report progress and
      // the pump would park forever waiting for one. Report it here: the pump
      // wakes, sees replay is still incomplete, and pulls the next chunk. This
      // is the only path that keeps a run blocked on a ready event several
      // chunks ahead moving.
      this.notifyHotProgress();
    }
  }

  #ingestHistory(events: readonly HistoryEvent[]): void {
    for (const event of events) {
      if (event.data.kind === "ActivityCompleted") {
        this.#activityCompletions.set(commandKey(event.data.completed.commandId), event);
      }
      if (event.data.kind === "ActivityFailed") {
        this.#activityFailures.set(commandKey(event.data.failed.commandId), event);
      }
      if (event.data.kind === "ActivityTimedOut") {
        this.#activityFailures.set(commandKey(event.data.timedOut.commandId), event);
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
      if (event.data.kind === "SignalConsumed") {
        this.#signalConsumptions.set(commandKey(event.data.consumed.commandId), event);
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

  /**
   * Consumes an indexed ready event exactly once.
   *
   * The single removal path for {@link #activityCompletions} and its ten
   * siblings, mirroring Rust's `take_indexed`. Callers must only reach here
   * when they are about to hand the value to the workflow: a probe that may
   * discard the result — `spawn()`, which allocates the command and drops the
   * resolution — must not consume, or the handle awaited later would find
   * nothing and park forever.
   */
  #takeReadyEvent(index: Map<string, HistoryEvent>, key: string): HistoryEvent | undefined {
    const event = index.get(key);
    if (event === undefined) {
      return undefined;
    }
    index.delete(key);
    return event;
  }

  /**
   * Consumes an activity's terminal failure.
   *
   * The `failure === null` arm is a backstop, not a live path, and the comment
   * that used to claim otherwise was wrong: `#ingestHistory` files only
   * `ActivityFailed` and `ActivityTimedOut` into this index, and
   * `activityTerminalFailure` returns a failure for both, so nothing reaches
   * here without one. A retryable timeout never appears as a non-terminal entry
   * to skip past — the provider records the next attempt's outcome under the
   * same command instead.
   *
   * Kept because it is the one place that decides whether an indexed failure
   * resolves its command. If a future event kind is filed here and
   * `activityTerminalFailure` does not describe it, leaving the entry in place
   * is the safe answer; removing the arm would hand the workflow a `Failed`
   * resolution with no failure in it.
   */
  #takeActivityTerminalFailure(
    key: string
  ): { readonly event: HistoryEvent; readonly failure: DurableFailure } | undefined {
    const event = this.#activityFailures.get(key);
    if (event === undefined) {
      return undefined;
    }
    const failure = activityTerminalFailure(event);
    if (failure === null) {
      return undefined;
    }
    this.#activityFailures.delete(key);
    return { event, failure };
  }

  hotProgressVersion(): number {
    return this.#hotProgressVersion;
  }

  notifyHotProgress(): void {
    this.#hotProgressVersion += 1;
    const waiters = this.#hotProgressWaiters;
    this.#hotProgressWaiters = [];
    for (const waiter of waiters) {
      waiter.resolve(undefined);
    }
  }

  async waitForHotProgressAfter(version: number): Promise<void> {
    if (this.#hotProgressVersion > version) {
      return;
    }
    const deferred = createDeferred<void>();
    this.#hotProgressWaiters.push(deferred);
    return deferred.promise;
  }

  hotSuspend<T>(
    id: CommandId,
    resolve: () => HotSuspendResolution<T>
  ): Promise<T> {
    return this.hotSuspendByKey(commandKey(id), resolve);
  }

  hotSuspendByKey<T>(
    key: string,
    resolve: () => HotSuspendResolution<T>
  ): Promise<T> {
    const immediate = resolve();
    if (immediate.kind === "Resolved") {
      return Promise.resolve(immediate.value);
    }
    if (immediate.kind === "Rejected") {
      return Promise.reject(immediate.error);
    }

    // One parked waiter per wait key, enforced rather than assumed.
    //
    // `#hotWaiters` is keyed by command id, so a second suspension on a command
    // that is already parked used to overwrite the first entry. The displaced
    // waiter's deferred was then unreachable: nothing could resolve it, and
    // `disposeHot` — which settles waiters by walking this map — could not
    // reach it either, so the run hung silently and stayed hung through
    // disposal. Two concurrent `handle.result()` awaits on one handle are the
    // reachable case, and they involve no side effect, so nothing else in the
    // run reports the stall.
    //
    // Rejecting the *second* suspension is a choice, not a forced move, and it
    // is worth being exact about why — an earlier version of this comment
    // claimed multiple waiters were impossible because the ready event is
    // consumed exactly once. That is false: `RuntimeActivityHandle` and
    // `RuntimeChildWorkflowHandle` already own the consumed resolution in a
    // `SettledResolution` box (it is what makes a second *sequential* read
    // work), so a `Map<string, HotWaiter[]>` would serve both frames the same
    // value while retaining nothing extra. It is roughly twenty-five lines.
    //
    // The reason to reject instead is this map's job. `#hotWaiters` is what
    // `disposeHot` walks to settle every parked frame, and the bug being fixed
    // here is precisely "a waiter nobody can reach". One waiter per key keeps
    // that walk total and its removal a single `delete`; a per-key list adds a
    // second removal path whose failure mode is the same unreachable waiter.
    // The cost of refusing is small because the shape is barely reachable: the
    // composite APIs will not accept a handle-result promise as a branch, so
    // two frames can only race for one durable result through hand-written
    // `.then()` chains, which is a workflow bug that replays identically every
    // time. Serving it silently would hide it.
    //
    // Revisit freely if a real workflow wants it. The first waiter is left
    // untouched and still resolves normally.
    const existing = this.#hotWaiters.get(key);
    if (existing !== undefined) {
      return Promise.reject(
        new Error(
          `durust: durable result ${key} is already awaited by another frame; ` +
            `await it once and share the value instead of awaiting the same handle twice`
        )
      );
    }

    const deferred = createDeferred<T>();
    const waiter: HotWaiter = {
      key,
      promise: deferred.promise,
      tryResolve: () => {
        const resolution = resolve();
        if (resolution.kind === "Pending") {
          return false;
        }
        this.#hotWaiters.delete(key);
        if (resolution.kind === "Rejected") {
          deferred.reject(resolution.error);
        } else {
          deferred.resolve(resolution.value);
        }
        return true;
      },
      reject: (error) => {
        this.#hotWaiters.delete(key);
        deferred.reject(error);
      }
    };
    this.#hotWaiters.set(key, waiter);
    this.notifyHotProgress();
    return deferred.promise;
  }

  advanceHotClaim(
    claimed: ClaimedWorkflowTask,
    options: HotWorkflowTaskOptions
  ): void {
    this.#claimed = withoutPrefetchedHistory(claimed);
    this.#nowMs = options.nowMs ?? this.#nowMs;
    this.#liveSignals = [...(options.liveSignals ?? [])];
    // A hot wake carries the delta from what this execution has already
    // ingested through the new replay target, contiguous and complete — the
    // worker's `#claimWithHotWakeHistory` asserts exactly that, and admits a
    // wake only when every event in it is a ready event. Chunking the cold
    // path changed neither the delta the worker computes nor how it is
    // applied: the delta is derived from the committed tail, not from the
    // claim's prefetched array.
    //
    // Ready events only, deliberately. A hot execution produced its own
    // command events and must never re-match them, so unlike a cold chunk this
    // feeds the indexes and the loaded watermark but not the replay window.
    this.#dropMatchedReplayEvents();
    this.#ingestHistory(claimed.prefetchedHistory);
    this.#advanceLoadedWatermark(
      claimed.prefetchedHistory.at(-1)?.eventId ?? this.#lastLoadedEventId
    );
    if (this.#tryResolveHotWaiters() === 0) {
      // A wake that settles nothing still has to produce a commit.
      //
      // Reachable whenever a task is queued by an event the workflow is not
      // waiting on by itself: the first of two `joinAll` branches completing is
      // the plain case — the composite stays pending, no frame resumes, and
      // nothing calls `notifyHotProgress`. `nextCommit()` would then park
      // forever on a progress signal that can never arrive, and the task would
      // sit claimed until its lease expired instead of committing nothing and
      // releasing. Reporting progress here yields an empty commit, which is the
      // honest description of the task: history advanced, the workflow did not.
      this.notifyHotProgress();
    }
  }

  markHotCommitAccepted(newTailEventId: EventId): void {
    this.#claimed = {
      ...this.#claimed,
      replayTargetEventId: newTailEventId
    };
    // The events this commit appended are this execution's own, so history is
    // loaded through the new tail by construction. Without moving the
    // watermark the next `nextCommit()` would think the window went stale and
    // try to stream events the provider has only just accepted.
    if (Number(newTailEventId) > Number(this.#lastLoadedEventId)) {
      this.#lastLoadedEventId = newTailEventId;
    }
    this.#appendEvents.length = 0;
    this.#upsertWaits.length = 0;
    this.#deleteWaits.length = 0;
    this.#cancelCommands.length = 0;
    this.#consumeSignals.length = 0;
    this.#scheduleActivities.length = 0;
    this.#scheduleActivityMaps.length = 0;
    this.#startChildWorkflows.length = 0;
    this.#scheduleChildWorkflowMaps.length = 0;
    this.#queryProjection = null;
  }

  // Abandons this context. Called once from `HotWorkflowExecution.dispose()`.
  //
  // Rejecting the parked waiters is what settles the workflow's promise chain,
  // and `#disposalError` is what keeps the frames that resume from that
  // rejection from mutating a context nobody will commit.
  disposeHot(error: HotWorkflowExecutionDisposedError): void {
    if (this.#disposalError !== null) {
      return;
    }
    this.#disposalError = error;
    const waiters = [...this.#hotWaiters.values()];
    this.#hotWaiters.clear();
    for (const waiter of waiters) {
      // Adopt and discard before rejecting. Every current `hotSuspend*` caller
      // attaches `.then(onfulfilled, onrejected)` to this promise in the same
      // expression, so today it is always handled; that is a call-site
      // invariant, not a property of this code, and one future site returning
      // the promise raw would put an unowned rejection in front of Node's
      // default `--unhandled-rejections=throw` and kill the worker. Dropping a
      // workflow task must never fail the host.
      waiter.promise.catch(() => undefined);
      waiter.reject(error);
    }
    // Frames parked waiting for the next history chunk are settled the same
    // way. They are not in `#hotWaiters`, and the chunk that would have woken
    // them is never going to be requested now that the pump is dead, so
    // skipping them would leave the abandoned workflow's chain pending forever
    // — the exact failure disposal exists to prevent.
    const replayWaiters = this.#replayHistoryWaiters;
    this.#replayHistoryWaiters = [];
    for (const waiter of replayWaiters) {
      waiter.promise.catch(() => undefined);
      waiter.reject(error);
    }
    // Wakes a `nextCommit()` parked on progress. It re-checks disposal after
    // the await and throws instead of returning a commit.
    this.notifyHotProgress();
  }

  /** Returns how many waiters settled, which tells `appendReplayHistory`
   * whether anything will report progress on its own. */
  #tryResolveHotWaiters(): number {
    let settled = 0;
    for (const waiter of [...this.#hotWaiters.values()]) {
      try {
        if (waiter.tryResolve()) {
          settled += 1;
        }
      } catch (error) {
        waiter.reject(error);
        settled += 1;
      }
    }
    return settled;
  }

  allowsNondeterministicGlobals(): boolean {
    return this.#allowNondeterministicGlobalsDepth > 0;
  }

  // The single gate every durable API passes through: the 13 named entry
  // points assert here, and `#nextCommandId` asserts here as the backstop for
  // any command-producing path that lacks a named one. It therefore carries
  // both refusals a dead context needs.
  //
  // Disposal comes first. Once the owning execution is abandoned, a frame that
  // is still scheduled — the detached continuation an async `sideEffect`
  // callback leaves behind is the known case — must not append an event,
  // allocate a command seq, set a query projection, or park on a fresh waiter
  // in a context nobody will ever commit. Refusing at the gate makes all of
  // that inert rather than merely unobserved.
  //
  // Then: rejects a durable API called from inside a `sideEffect` callback. The
  // message is prefixed `nondeterminism:` so the existing classifiers treat it
  // as a fatal workflow-task error: the task fails and is retried with the
  // nondeterminism backoff instead of the workflow failing or the task
  // spinning. `api` names the call the workflow author made and `detail` is its
  // identifying argument; they are formatted only on the throwing path so a
  // durable call on the hot path costs one null compare and no allocation.
  #assertDurableApiAllowed(api: string, detail?: string): void {
    if (this.#disposalError !== null) {
      throw this.#disposalError;
    }
    const frameApi = this.#userCodeFrameApi;
    if (frameApi === null) {
      return;
    }
    if (frameApi === SIDE_EFFECT_CALLBACK_FRAME) {
      throw new Error(
        `nondeterminism: durable APIs cannot be called from inside a sideEffect callback; ` +
          `side effect "${this.#userCodeFrameDetail}" called ` +
          `${describeDurableApi(api, detail)}. ` +
          `Compute durable values outside the callback and pass them in.`
      );
    }
    // Name the shapes of user code that can be running inside the window rather
    // than asserting one of them. The caller is not a `sideEffect` callback
    // here: it is whatever the outer command invoked while turning user values
    // into an event.
    throw new Error(
      `nondeterminism: durable APIs are not re-entrant; ` +
        `${describeDurableApi(api, detail)} ran inside ` +
        `${describeDurableApi(frameApi, this.#userCodeFrameDetail)} while it was ` +
        `converting user-supplied values. The caller is user code that conversion invoked — ` +
        `most often a toJSON() method, a schema adapter's encode(), or a valueOf() on a value ` +
        `passed to the durable API. Compute the value first and pass in a plain value.`
    );
  }

  // Opens the window in which a durable API runs user-controlled code after
  // allocating its command seq. Every caller must close it in a `finally`, or a
  // throw from user code would latch the guard for the rest of the run.
  #beginUserCodeFrame(api: string, detail: string | null): void {
    this.#userCodeFrameApi = api;
    this.#userCodeFrameDetail = detail;
  }

  #endUserCodeFrame(): void {
    this.#userCodeFrameApi = null;
    this.#userCodeFrameDetail = null;
  }

  resolveSignal<Payload extends object>(
    name: string,
    payloadSchema?: SchemaAdapter<Payload>
  ): SignalResolution<Payload> {
    this.#assertDurableApiAllowed("signal", name);
    const id = this.#nextCommandId();
    const fingerprint = signalFingerprint(name);
    const recorded = this.#takeRecordedSignalConsumption(id, fingerprint);
    if (recorded !== undefined) {
      return {
        kind: "Consumed",
        value: decodePayload<Payload>(recorded.payload as PayloadRef<Payload>, payloadSchema),
        eventId: recorded.eventId
      };
    }

    const live = this.#takeLiveSignal(name);
    if (live) {
      const consumedEventId = this.#nextAppendEventId();
      // This append must stay above the decode below. `decodePayload` runs the
      // signal's schema `decode()`, which is user code; a durable API called
      // from there would allocate the next command seq and append its own event
      // first. Appending `SignalConsumed` first is what makes that harmless, so
      // decode sites are deliberately outside the re-entrancy frame. Moving the
      // decode above the push — to validate a payload before recording it, say
      // — reopens the out-of-order pair, and no test would catch it.
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
        value: decodePayload<Payload>(live.payload as PayloadRef<Payload>, payloadSchema),
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
    this.#assertDurableApiAllowed("sleep()");
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

      const terminal = this.#takeReadyEvent(this.#timerFires, commandKey(started.commandId));
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

  /**
   * Records or matches the `ActivityScheduled` command, and — when `consume` is
   * true — hands over the terminal ready event for it.
   *
   * `consume` is false for `spawn()`, the one caller that allocates the command
   * and throws the resolution away. Taking there would remove the completion
   * from the index that the handle's later `result()` reads, and the workflow
   * would park on a value that has already been discarded.
   */
  resolveActivity<A extends ActivityDefinition<any, any, string>>(
    activityDefinition: A,
    input: ActivityInput<A>,
    options: ActivityCallOptions,
    consume = true
  ): ActivityResolution<ActivityOutput<A>> {
    this.#assertDurableApiAllowed("callActivity", activityDefinition.name);
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

      if (!consume) {
        return { kind: "Pending", commandId: scheduled.commandId };
      }

      const terminal = this.#takeReadyEvent(
        this.#activityCompletions,
        commandKey(scheduled.commandId)
      );
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
      const failed = this.#takeActivityTerminalFailure(commandKey(scheduled.commandId));
      if (failed !== undefined) {
        return {
          kind: "Failed",
          commandId: scheduled.commandId,
          failure: failed.failure,
          eventId: failed.event.eventId
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
    this.#assertDurableApiAllowed("activityHandle.result", activityDefinition.name);
    const terminal = this.#takeReadyEvent(this.#activityCompletions, commandKey(id));
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
    const failed = this.#takeActivityTerminalFailure(commandKey(id));
    if (failed !== undefined) {
      return {
        kind: "Failed",
        commandId: id,
        failure: failed.failure,
        eventId: failed.event.eventId
      };
    }
    return { kind: "Pending", commandId: id };
  }

  resolveHotActivity<A extends ActivityDefinition<any, any, string>>(
    activityDefinition: A,
    id: CommandId
  ): HotSuspendResolution<ActivityOutput<A>> {
    const resolution = this.resolveHotActivityBranch(activityDefinition, id);
    if (resolution.kind === "Resolved") {
      return { kind: "Resolved", value: resolution.value.value };
    }
    return resolution;
  }

  resolveHotActivityBranch<A extends ActivityDefinition<any, any, string>>(
    activityDefinition: A,
    id: CommandId
  ): HotSuspendResolution<ReadyJoinBranch<ActivityOutput<A>>> {
    const resolution = this.resolveActivityHandleResult(activityDefinition, id);
    if (resolution.kind === "Completed") {
      return {
        kind: "Resolved",
        value: { kind: "Ready", value: resolution.value, eventId: resolution.eventId }
      };
    }
    if (resolution.kind === "Failed") {
      return { kind: "Rejected", error: new ActivityFailureError(resolution.failure) };
    }
    return { kind: "Pending" };
  }

  resolveHotTimer(id: CommandId): HotSuspendResolution<void> {
    const resolution = this.resolveHotTimerBranch(id);
    if (resolution.kind === "Resolved") {
      return { kind: "Resolved", value: undefined };
    }
    return resolution;
  }

  resolveHotTimerBranch(id: CommandId): HotSuspendResolution<ReadyJoinBranch<void>> {
    const terminal = this.#takeReadyEvent(this.#timerFires, commandKey(id));
    if (terminal?.data.kind === "TimerFired") {
      return {
        kind: "Resolved",
        value: { kind: "Ready", value: undefined, eventId: terminal.eventId }
      };
    }
    return { kind: "Pending" };
  }

  resolveHotSignal<Payload extends object>(
    name: string,
    id: CommandId,
    payloadSchema?: SchemaAdapter<Payload>
  ): HotSuspendResolution<Payload> {
    const resolution = this.resolveHotSignalBranch<Payload>(name, id, payloadSchema);
    if (resolution.kind === "Resolved") {
      return { kind: "Resolved", value: resolution.value.value };
    }
    return resolution;
  }

  resolveHotSignalBranch<Payload extends object>(
    name: string,
    id: CommandId,
    payloadSchema?: SchemaAdapter<Payload>
  ): HotSuspendResolution<ReadyJoinBranch<Payload>> {
    const recorded = this.#takeRecordedSignalConsumption(id, signalFingerprint(name));
    if (recorded !== undefined) {
      return {
        kind: "Resolved",
        value: {
          kind: "Ready",
          value: decodePayload<Payload>(recorded.payload as PayloadRef<Payload>, payloadSchema),
          eventId: recorded.eventId
        }
      };
    }
    const live = this.#takeLiveSignal(name);
    if (live === undefined) {
      return { kind: "Pending" };
    }
    const consumedEventId = this.#nextAppendEventId();
    // As in `resolveSignal`: this append must stay above the decode below. The
    // schema `decode()` is user code, and only appending `SignalConsumed` first
    // keeps a durable call made from it from recording its event ahead of this
    // command's own.
    this.#appendEvents.push({
      data: {
        kind: "SignalConsumed",
        consumed: {
          commandId: id,
          signalId: live.signalId,
          signalName: live.signalName,
          payload: live.payload,
          fingerprint: signalFingerprint(name)
        }
      }
    });
    this.#consumeSignals.push(String(live.signalId));
    this.#deleteWaits.push(signalWaitId(id));
    return {
      kind: "Resolved",
      value: {
        kind: "Ready",
        value: decodePayload<Payload>(live.payload as PayloadRef<Payload>, payloadSchema),
        eventId: consumedEventId
      }
    };
  }

  resolveHotActivityMapResult<A extends ActivityDefinition<any, any, string>>(
    id: CommandId
  ): HotSuspendResolution<PayloadRef<ActivityMapResultManifest<ActivityOutput<A>>>> {
    const completed = this.#takeReadyEvent(this.#activityMapCompletions, commandKey(id));
    if (completed?.data.kind === "ActivityMapCompleted") {
      return {
        kind: "Resolved",
        value: completed.data.completed.resultManifest as PayloadRef<ActivityMapResultManifest<ActivityOutput<A>>>
      };
    }
    const failed = this.#takeReadyEvent(this.#activityMapFailures, commandKey(id));
    if (failed?.data.kind === "ActivityMapFailed") {
      return { kind: "Rejected", error: new ActivityFailureError(failed.data.failed.failure) };
    }
    return { kind: "Pending" };
  }

  resolveHotChildWorkflowMapResult<W extends WorkflowDefinition<any, any, any, string>>(
    id: CommandId
  ): HotSuspendResolution<PayloadRef<ChildWorkflowMapResultManifest<WorkflowOutput<W>>>> {
    const completed = this.#takeReadyEvent(this.#childMapCompletions, commandKey(id));
    if (completed?.data.kind === "ChildWorkflowMapCompleted") {
      return {
        kind: "Resolved",
        value: completed.data.completed.resultManifest as PayloadRef<ChildWorkflowMapResultManifest<WorkflowOutput<W>>>
      };
    }
    const failed = this.#takeReadyEvent(this.#childMapFailures, commandKey(id));
    if (failed?.data.kind === "ChildWorkflowMapFailed") {
      return { kind: "Rejected", error: new ChildWorkflowMapFailureError(failed.data.failed.failure) };
    }
    return { kind: "Pending" };
  }

  resolveHotChildWorkflowStart<W extends WorkflowDefinition<any, any, any, string>>(
    workflowDefinition: W,
    id: CommandId
  ): HotSuspendResolution<ChildWorkflowHandle<WorkflowOutput<W>>> {
    const started = this.#takeReadyEvent(this.#childStarts, commandKey(id));
    if (started?.data.kind === "ChildWorkflowStarted") {
      return {
        kind: "Resolved",
        value: new RuntimeChildWorkflowHandle(
          workflowDefinition,
          id,
          started.data.started.workflowId,
          started.data.started.runId
        )
      };
    }
    const failed = this.#takeReadyEvent(this.#childFailures, commandKey(id));
    if (failed?.data.kind === "ChildWorkflowFailed") {
      return { kind: "Rejected", error: new ChildWorkflowFailureError(failed.data.failed.failure) };
    }
    return { kind: "Pending" };
  }

  resolveActivityMap<A extends ActivityDefinition<any, any, string>>(
    activityDefinition: A,
    options: ActivityMapOptions<ActivityInput<A>>
  ): ActivityMapResolution<ActivityOutput<A>> {
    this.#assertDurableApiAllowed("activityMap", activityDefinition.name);
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

      const completed = this.#takeReadyEvent(
        this.#activityMapCompletions,
        commandKey(scheduled.commandId)
      );
      if (completed?.data.kind === "ActivityMapCompleted") {
        return {
          kind: "Completed",
          commandId: scheduled.commandId,
          resultManifest: completed.data.completed.resultManifest as PayloadRef<ActivityMapResultManifest<ActivityOutput<A>>>,
          eventId: completed.eventId
        };
      }
      const failed = this.#takeReadyEvent(
        this.#activityMapFailures,
        commandKey(scheduled.commandId)
      );
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
    this.#assertDurableApiAllowed("childWorkflowMap", workflowDefinition.workflowType.name);
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

      const completed = this.#takeReadyEvent(
        this.#childMapCompletions,
        commandKey(scheduled.commandId)
      );
      if (completed?.data.kind === "ChildWorkflowMapCompleted") {
        return {
          kind: "Completed",
          commandId: scheduled.commandId,
          resultManifest: completed.data.completed.resultManifest as PayloadRef<ChildWorkflowMapResultManifest<WorkflowOutput<W>>>,
          eventId: completed.eventId
        };
      }
      const failed = this.#takeReadyEvent(
        this.#childMapFailures,
        commandKey(scheduled.commandId)
      );
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
    this.#assertDurableApiAllowed("childWorkflow", workflowDefinition.workflowType.name);
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

      const started = this.#takeReadyEvent(this.#childStarts, commandKey(requested.commandId));
      if (started?.data.kind === "ChildWorkflowStarted") {
        return {
          kind: "Started",
          commandId: requested.commandId,
          workflowId: started.data.started.workflowId,
          runId: started.data.started.runId,
          eventId: started.eventId
        };
      }
      const failed = this.#takeReadyEvent(this.#childFailures, commandKey(requested.commandId));
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
    this.#assertDurableApiAllowed(
      "childWorkflowHandle.result",
      workflowDefinition.workflowType.name
    );
    const completed = this.#takeReadyEvent(this.#childCompletions, commandKey(id));
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
    const failed = this.#takeReadyEvent(this.#childFailures, commandKey(id));
    if (failed?.data.kind === "ChildWorkflowFailed") {
      return { kind: "Failed", failure: failed.data.failed.failure, eventId: failed.eventId };
    }
    const cancelled = this.#takeReadyEvent(this.#childCancellations, commandKey(id));
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
    this.#assertDurableApiAllowed("getVersion", changeId);
    validateVersionRange(changeId, minSupported, maxSupported);
    const replayEvent = this.#peekReplayEventForUnparkableApi("getVersion");
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
    this.#assertDurableApiAllowed("deprecatePatch", patchId);
    const replayEvent = this.#peekReplayEventForUnparkableApi("deprecatePatch");
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
    this.#assertDurableApiAllowed("sideEffect", key);
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
    // The entry assert above proves no frame is open here, so clearing to null
    // in the finally restores the exact prior state.
    this.#beginUserCodeFrame(SIDE_EFFECT_CALLBACK_FRAME, key);
    this.#allowNondeterministicGlobalsDepth += 1;
    try {
      value = effect();
    } finally {
      // Cleared on the throwing path as well: a callback error is reported to
      // the workflow, which may catch it and keep using durable APIs.
      this.#allowNondeterministicGlobalsDepth -= 1;
      this.#endUserCodeFrame();
    }
    if (isThenable(value)) {
      // The marker records `value` synchronously, so an async callback would
      // record the promise itself and replay would return that instead of the
      // resolved value. Rejecting also keeps the re-entrancy guard exact: the
      // guard covers the callback's whole lifetime only when the callback
      // cannot continue past an await after `effect()` returns.
      //
      // Adopt and discard the promise first. Nothing else will ever attach a
      // handler to it, so a rejecting callback would otherwise reach
      // `unhandledRejection` and kill the worker process under Node's default
      // `--unhandled-rejections=throw`. Failing the task must not fail the host.
      void Promise.resolve(value).catch(() => undefined);
      throw new Error(
        `nondeterminism: sideEffect callback must be synchronous; side effect "${key}" ` +
          `returned a promise. Record a plain value and use an activity for asynchronous work.`
      );
    }
    // The callback's guard is already released here, but the value it returned
    // is still user-controlled and is encoded before the marker is appended, so
    // the window stays open across the encode.
    this.#beginUserCodeFrame("sideEffect", key);
    let payload: PayloadRef<T>;
    try {
      payload = encodePayload(value, { codec: this.#payloadCodec });
    } finally {
      this.#endUserCodeFrame();
    }
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

  /**
   * Reserves the `select`'s own command sequence number, before any branch
   * registers.
   *
   * This is the numbering Rust's `select!` macro produces: it calls
   * `__durust_select_ensure_command_id` at the top of every poll, so a
   * two-branch select numbers select=1, branch=2, branch=3, and it burns the
   * number even in a task where no branch is ready. TypeScript used to
   * allocate inside {@link resolveSelectWinner}, which runs only once a branch
   * *is* ready, numbering the same select branch=1, branch=2, select=3 — and
   * nothing at all in a task that parks. The command seq is durable, so the
   * two numberings meant neither runtime could replay the other's history
   * across a select; every other durable command in both runtimes already
   * allocates at initiation, and TypeScript's exception was an artefact of
   * `SelectDurablePromise.then()` inheriting the readiness condition.
   *
   * Called after the replay-history gate and before branch registration, so a
   * `then()` that parks for a longer replay window has not yet burned a
   * number and its retry is still exact.
   */
  beginSelect(): CommandId {
    return this.#nextCommandId();
  }

  resolveSelectWinner(
    selectCommandId: CommandId,
    readyBranches: readonly SelectReadyBranch[],
    branchesDigest: string
  ): SelectReadyBranch {
    if (readyBranches.length === 0) {
      throw new Error("durust.select requires at least one ready branch");
    }
    const replayEvent = this.#peekReplayEventForUnparkableApi("select");
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
      // Replay follows the recorded decision rather than recomputing it: the
      // live comparison ordered the winning fact against every other branch's
      // by arrival, which only held while no unrelated fact could be appended
      // to the run between a task's claim and its commit.
      const recorded = readyBranches.find((branch) => branch.ordinal === replayed.branchOrdinal);
      if (recorded === undefined) {
        throw new Error(
          `nondeterminism: select command ${selectCommandId.seq} recorded branch ` +
            `${replayed.branchOrdinal} as the winner, but replay never produced that branch's result`
        );
      }
      this.#advanceReplay();
      return recorded;
    }

    const winner = [...readyBranches].sort(compareSelectReadyBranches)[0] as SelectReadyBranch;
    this.#appendEvents.push({
      data: {
        kind: "SelectWinner",
        winner: {
          selectCommandId,
          branchOrdinal: winner.ordinal,
          branchesDigest
        }
      }
    });
    return winner;
  }

  /**
   * Withdraws a losing `select` branch's timer.
   *
   * The three `cancel*Branch` methods together are TypeScript's
   * `DurableSelectBranch::__durust_cancel_branch`, which Rust's `select!`
   * calls on every branch that neither won nor produced a value. Without them
   * a losing branch's operational state outlives the select: the wait sits in
   * the provider until something fires it, and a timer fired against a run
   * that has since closed appends `TimerFired` *after* the terminal event.
   *
   * Deleting a wait the provider no longer holds is a no-op in all three
   * providers, which is what makes this safe to emit on replay as well as on
   * the first pass — Rust emits it from the same `Waiting` branch state
   * whether the command was appended or matched.
   */
  cancelTimerBranch(id: CommandId): void {
    this.#deleteWaits.push(timerWaitId(id));
  }

  /**
   * Withdraws a losing `select` branch's signal wait. See `cancelTimerBranch`.
   *
   * Rust's `SignalFuture::__durust_cancel_branch` does one more thing here:
   * `abandon_live_signal`, which un-reserves the live signal its context had
   * bound to this command. That half is deliberately absent rather than
   * missed. Rust reserves a live signal to a command id and releases it on
   * cancel; TypeScript's `#takeLiveSignal` splices the record out of
   * `#liveSignals` at the moment it is consumed and reserves nothing before
   * then, so a branch still pending has bound no signal and there is nothing
   * to give back. If signal delivery ever gains a reservation step, this is
   * the site that has to gain the release.
   */
  cancelSignalBranch(id: CommandId): void {
    this.#deleteWaits.push(signalWaitId(id));
  }

  /**
   * Withdraws a losing `select` branch's activity.
   *
   * An activity has no wait to delete; its operational state is the scheduled
   * task, so the provider is told to tombstone it through `cancelCommands` —
   * exactly what Rust's `ActivityFuture::__durust_cancel_branch` does through
   * `RuntimeContext::cancel_command`. Left uncancelled the orphaned activity
   * keeps running, keeps retrying, and appends its `ActivityCompleted` to a
   * run that stopped caring about it.
   *
   * The dedup mirrors `RuntimeContext::cancel_command` and is defensive
   * rather than load-bearing: cancellation runs once, on the pass that settles
   * the select, so no branch reaches here twice today. It costs a linear scan
   * of a list that is empty on every commit but this one, and it means a
   * future second caller cannot make a provider tombstone the same command
   * twice.
   */
  cancelActivityBranch(id: CommandId): void {
    if (!this.#cancelCommands.some((existing) => sameCommandId(existing, id))) {
      this.#cancelCommands.push(id);
    }
  }

  completeWorkflow<Output>(
    output: Output,
    workflowDefinition: WorkflowDefinition<any, Output, any, string>
  ): void {
    this.#assertTerminalReplayConsumed("WorkflowCompleted");
    // No command seq is allocated here, so a durable call from the output's
    // `toJSON` cannot invert a pair. It would instead append its event after
    // the terminal-replay-consumed check already passed and ahead of the
    // terminal event, which the check exists to make impossible.
    let result: PayloadRef<Output>;
    this.#beginUserCodeFrame("workflow completion", null);
    try {
      result = encodePayload(output, {
        codec: this.#payloadCodec,
        ...(workflowDefinition.outputSchema === undefined
          ? {}
          : { schema: workflowDefinition.outputSchema })
      });
    } finally {
      this.#endUserCodeFrame();
    }
    this.#appendEvents.push({
      data: { kind: "WorkflowCompleted", result }
    });
    this.notifyHotProgress();
  }

  failWorkflow(failure: DurableFailure): void {
    this.#assertTerminalReplayConsumed("WorkflowFailed");
    this.#appendEvents.push({
      data: {
        kind: "WorkflowFailed",
        failure
      }
    });
    this.notifyHotProgress();
  }

  publish<QueryState extends object>(view: QueryState): void {
    this.#assertDurableApiAllowed("publish()");
    assertDurableInputValue(view, "query projection");
    this.#beginUserCodeFrame("publish()", null);
    try {
      this.#queryProjection = encodePayload(view, {
        codec: this.#payloadCodec,
        ...(this.#queryStateSchema === undefined
          ? {}
          : { schema: this.#queryStateSchema as SchemaAdapter<QueryState> })
      });
    } finally {
      this.#endUserCodeFrame();
    }
  }

  continueAsNew<Input extends object>(input: Input): never {
    this.#assertDurableApiAllowed("continueAsNew()");
    this.#assertTerminalReplayConsumed("WorkflowContinuedAsNew");
    assertDurableInputValue(input, "continueAsNew input");
    let payload: PayloadRef<Input>;
    this.#beginUserCodeFrame("continueAsNew()", null);
    try {
      payload = encodePayload(input, {
        codec: this.#payloadCodec,
        ...(this.#workflowInputSchema === undefined
          ? {}
          : { schema: this.#workflowInputSchema as SchemaAdapter<Input> })
      });
    } finally {
      this.#endUserCodeFrame();
    }
    this.#appendEvents.push({
      data: {
        kind: "WorkflowContinuedAsNew",
        input: payload
      }
    });
    this.notifyHotProgress();
    throw new ContinueAsNewRequested();
  }

  toCommit(): WorkflowTaskCommit {
    // Last line of the no-commit-after-disposal invariant. `nextCommit()`
    // already refuses, but this is the only function that can produce a commit
    // at all, so the invariant is enforced where it is defined.
    if (this.#disposalError !== null) {
      throw this.#disposalError;
    }
    // Refused even if the workflow caught the throw. A backstop, and an honest
    // label: no test kills this line, because every reachable path that reaches
    // here with the latch set is already caught earlier — the marker the
    // workflow skipped stays at the replay cursor, so either the next command
    // event mismatches it positionally, or the workflow reaches a terminal
    // state and the deferred `#assertTerminalReplayConsumed` finds it
    // unconsumed. It is kept because this is the only function that can produce
    // a commit, and "a task built on a marker the runtime could not verify"
    // must be refused where commits are made rather than relying on two other
    // checks to have covered every future shape of workflow code.
    if (this.#replayWindowOverrun !== null) {
      throw this.#replayWindowOverrun;
    }
    // The divergence check a terminal event could not perform when it was
    // appended, because history was still loading. The pump only calls this
    // once loading has finished, so "recorded command left unconsumed" is now
    // decidable and the run does not commit a terminal event that history
    // contradicts.
    const pendingTerminal = this.#pendingTerminalReplayCheck;
    if (pendingTerminal !== null) {
      this.#pendingTerminalReplayCheck = null;
      if (!this.replayHistoryComplete()) {
        throw new Error(
          `nondeterminism: ${pendingTerminal} reached before replay history was fully loaded`
        );
      }
      const leftover = this.#peekReplayEvent();
      if (leftover !== undefined) {
        throw new Error(
          `nondeterminism: ${pendingTerminal} reached with unconsumed recorded command ${leftover.eventType}`
        );
      }
    }
    return {
      appendEvents: [...this.#appendEvents],
      upsertWaits: [...this.#upsertWaits],
      deleteWaits: [...this.#deleteWaits],
      cancelCommands: [...this.#cancelCommands],
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
    // `encodePayload` runs the input's `toJSON`/`valueOf` and the schema
    // adapter's `encode`, and `activityOptionsDigest` stringifies a
    // user-supplied retry policy. All of it runs after seq allocation and
    // before the event is appended.
    this.#beginUserCodeFrame("callActivity", activityDefinition.name);
    try {
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
          // The caller's queue, not the resolved one. See
          // `activityOptionsDigest`.
          taskQueue: options.taskQueue ?? DEFAULT_ACTIVITY_TASK_QUEUE,
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
    } finally {
      this.#endUserCodeFrame();
    }
  }

  #activityMapScheduledEvent<A extends ActivityDefinition<any, any, string>>(
    activityDefinition: A,
    options: ActivityMapOptions<ActivityInput<A>>
  ): ActivityMapScheduled {
    // Before the command id, so a rejected manifest does not burn a command
    // sequence number and leave the next command renumbered on retry.
    assertMapInputManifest("activityMap inputManifest", options.inputManifest);
    const id = this.#nextCommandId();
    // The manifest arrives already encoded, so this builder runs less user code
    // than the others. It is guarded on the same terms anyway: the hazard is the
    // window between allocation and append, not today's contents of it.
    this.#beginUserCodeFrame("activityMap", activityDefinition.name);
    try {
      const taskQueue = options.taskQueue ?? this.#defaultActivityTaskQueue;
      // Item retry and deadlines are caller-supplied, matching Rust's
      // `activity_map` builder, which carries a full `ActivityOptions`. The
      // defaults are the values that were hardcoded here before they were
      // configurable, so a workflow that supplies none of the three produces a
      // byte-identical `activityOptionsDigest` and replays unchanged.
      const retryPolicy = options.retry ?? RetryPolicy.none();
      const startToCloseTimeoutMs = options.startToCloseTimeoutMs ?? null;
      const heartbeatTimeoutMs = options.heartbeatTimeoutMs ?? null;
      const fingerprint = activityMapFingerprint(
        activityDefinition.name,
        payloadDigest(options.inputManifest),
        options.resultManifest,
        options.maxInFlight,
        activityOptionsDigest({
          // The caller's queue, not the resolved one. See
          // `activityOptionsDigest`.
          taskQueue: options.taskQueue ?? DEFAULT_ACTIVITY_TASK_QUEUE,
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
    } finally {
      this.#endUserCodeFrame();
    }
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
    assertMapInputManifest("childWorkflowMap inputManifest", options.inputManifest);
    const id = this.#nextCommandId();
    this.#beginUserCodeFrame("childWorkflowMap", workflowDefinition.workflowType.name);
    try {
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
    } finally {
      this.#endUserCodeFrame();
    }
  }

  #childWorkflowStartRequested<W extends WorkflowDefinition<any, any, any, string>>(
    workflowDefinition: W,
    input: WorkflowInput<W>,
    options: ChildWorkflowOptions
  ): ChildWorkflowStartRequested {
    const id = this.#nextCommandId();
    this.#beginUserCodeFrame("childWorkflow", workflowDefinition.workflowType.name);
    try {
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
    } finally {
      this.#endUserCodeFrame();
    }
  }

  #timerStartedEvent(spec: RuntimeTimerSpec): TimerStarted {
    const id = this.#nextCommandId();
    // `resolveTimerTimes` reads the user-supplied duration or deadline, which
    // can carry a `valueOf()`.
    this.#beginUserCodeFrame("sleep()", null);
    try {
      const { fireAt, fingerprintAt } = resolveTimerTimes(spec, this.#nowMs);
      return {
        commandId: id,
        fireAt: timestampMs(fireAt),
        fingerprint: timerFingerprint(spec.kind, timestampMs(fingerprintAt))
      };
    } finally {
      this.#endUserCodeFrame();
    }
  }

  #peekReplayEvent(): HistoryEvent | undefined {
    return this.#replayEvents[this.#replayCursor];
  }

  /**
   * The peek for the durable APIs that cannot park.
   *
   * `getVersion`, `patched`, and `deprecatePatch` return plain values, so a
   * short window cannot suspend them the way a thenable API suspends. Reading
   * "no recorded event" as "this is a new command" would append a duplicate
   * marker that the rest of history contradicts, so an exhausted window is
   * refused here instead. `replayHistoryGate` reserves
   * {@link REPLAY_WINDOW_LOOKAHEAD_EVENTS} events at every awaited durable call
   * precisely so this cannot fire below that many consecutive marker calls.
   */
  #peekReplayEventForUnparkableApi(api: string): HistoryEvent | undefined {
    const event = this.#peekReplayEvent();
    if (event === undefined && !this.replayHistoryComplete()) {
      // Latched as well as thrown. The throw lands in workflow code, which is
      // free to catch it; the latch is what stops this task from committing on
      // a marker the runtime never verified, and what the worker reads to
      // decide to replay the run again with no reserve limit.
      const overrun = new ReplayWindowOverrunError(api, this.#replayWindowLookahead);
      this.#replayWindowOverrun ??= overrun;
      throw overrun;
    }
    return event;
  }

  /** The overrun this replay hit, if any. Read by the owner to decide whether
   * re-replaying with an unlimited reserve is worth it. */
  replayWindowOverrun(): ReplayWindowOverrunError | null {
    return this.#replayWindowOverrun;
  }

  #assertTerminalReplayConsumed(terminalKind: string): void {
    if (!this.replayHistoryComplete()) {
      if (this.#loadReplayHistory !== null) {
        // Chunked replay: whether a recorded command is left over cannot be
        // decided until the rest of history is loaded, and the pump only
        // finishes loading after this frame returns. Latch it for
        // `toCommit()`, which runs once loading is done.
        this.#pendingTerminalReplayCheck = terminalKind;
        return;
      }
      throw new Error(
        `nondeterminism: ${terminalKind} reached before replay history was fully loaded`
      );
    }
    const leftover = this.#peekReplayEvent();
    if (leftover !== undefined) {
      throw new Error(
        `nondeterminism: ${terminalKind} reached with unconsumed recorded command ${leftover.eventType}`
      );
    }
  }

  #nextCommandId(): CommandId {
    // Backstop for the durable-API re-entrancy contract. Every durable API a
    // workflow can reach asserts with a named message before it gets here, so
    // this should be unreachable; it guards the invariant at the one place that
    // defines command order, so a future command-producing path cannot record
    // an out-of-order marker by forgetting the named assert.
    this.#assertDurableApiAllowed("a durable command");
    return commandId(this.#claimed.runId, this.#nextCommandSeq++);
  }

  #nextAppendEventId(): EventId {
    return eventId(Number(this.#claimed.replayTargetEventId) + this.#appendEvents.length + 1);
  }

  #advanceReplay(): void {
    this.#replayCursor += 1;
  }

  /**
   * Consumes the recorded `SignalConsumed` for a signal command, if history
   * carries one. Indexed by command id rather than matched at the replay
   * cursor: the consumption is appended when the signal arrives, which may be
   * many commands after the wait registered, so a `select` or `join` whose
   * signal branch registered before its timer would otherwise never find it
   * on a cold replay.
   */
  #takeRecordedSignalConsumption(
    id: CommandId,
    fingerprint: CommandFingerprint
  ): { readonly payload: PayloadRef; readonly eventId: EventId } | undefined {
    const event = this.#takeReadyEvent(this.#signalConsumptions, commandKey(id));
    if (event?.data.kind !== "SignalConsumed") {
      return undefined;
    }
    const consumed = event.data.consumed;
    if (!sameFingerprint(consumed.fingerprint, fingerprint)) {
      throw new Error("nondeterminism: signal command fingerprint changed");
    }
    return { payload: consumed.payload, eventId: event.eventId };
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
    const context = currentWorkflowRuntimeContext();
    // Chunked replay: park until the window can serve this call rather than
    // reading an empty window as "no recorded command" and appending a
    // duplicate. Nothing has been mutated yet, so retrying is exact.
    const historyGate = context.replayHistoryGate(1);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    const resolution = context.resolveActivity(
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
    return context.hotSuspend(
      resolution.commandId,
      () => context.resolveHotActivity(this.#activityDefinition, resolution.commandId)
    ).then(onfulfilled, _onrejected);
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
    return {
      kind: "Pending",
      key: commandKey(resolution.commandId),
      resolve: () => context.resolveHotActivityBranch(this.#activityDefinition, resolution.commandId),
      cancel: () => context.cancelActivityBranch(resolution.commandId)
    };
  }

  spawn(): PromiseLike<ActivityHandle<ActivityOutput<A>>> {
    const spawned: PromiseLike<ActivityHandle<ActivityOutput<A>>> = {
      then: <TResult1 = ActivityHandle<ActivityOutput<A>>, TResult2 = never>(
        onfulfilled?: ((value: ActivityHandle<ActivityOutput<A>>) => TResult1 | PromiseLike<TResult1>) | null,
        _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
      ): PromiseLike<TResult1 | TResult2> => {
        const context = currentWorkflowRuntimeContext();
        // Same replay-window gate as every other command-producing call.
        const historyGate = context.replayHistoryGate(1);
        if (historyGate !== null) {
          return retryAfterReplayHistory(
            historyGate,
            () => spawned.then(onfulfilled, _onrejected),
            _onrejected
          );
        }
        // `consume: false`: this call exists to allocate or match the command,
        // and it throws the resolution away. The terminal ready event belongs
        // to whoever awaits the handle, so it must stay in the index.
        const resolution = context.resolveActivity(
          this.#activityDefinition,
          this.#input,
          this.#options,
          false
        );
        const handle = new RuntimeActivityHandle(this.#activityDefinition, resolution.commandId);
        return Promise.resolve(onfulfilled ? onfulfilled(handle) : (handle as TResult1));
      }
    };
    return spawned;
  }
}

class RuntimeActivityHandle<A extends ActivityDefinition<any, any, string>>
  implements ActivityHandle<ActivityOutput<A>>
{
  readonly kind = "activity-handle" as const;
  readonly #activityDefinition: A;
  readonly #commandId: CommandId;
  // Owns this command's terminal resolution once it has been taken out of the
  // runtime's ready-event index. `result()` hands back a fresh promise every
  // call, and awaiting the same handle twice — sequentially, across tasks, or
  // after a `join` already read it — is ordinary workflow code, so the value
  // has to live somewhere after its single consumption. It lives with the
  // handle the workflow is holding, and dies with it.
  readonly #settled: SettledResolution<ActivityResolution<ActivityOutput<A>>> = { value: null };

  constructor(activityDefinition: A, commandIdValue: CommandId) {
    this.#activityDefinition = activityDefinition;
    this.#commandId = commandIdValue;
  }

  result(): PromiseLike<ActivityOutput<A>> {
    return new ActivityHandleResultDurablePromise(
      this.#activityDefinition,
      this.#commandId,
      this.#settled
    );
  }
}

class ActivityHandleResultDurablePromise<A extends ActivityDefinition<any, any, string>>
  implements PromiseLike<ActivityOutput<A>>
{
  readonly #activityDefinition: A;
  readonly #commandId: CommandId;
  readonly #settled: SettledResolution<ActivityResolution<ActivityOutput<A>>>;

  constructor(
    activityDefinition: A,
    commandIdValue: CommandId,
    settled: SettledResolution<ActivityResolution<ActivityOutput<A>>>
  ) {
    this.#activityDefinition = activityDefinition;
    this.#commandId = commandIdValue;
    this.#settled = settled;
  }

  #resolve(context: WorkflowRuntimeContext): ActivityResolution<ActivityOutput<A>> {
    if (this.#settled.value !== null) {
      return this.#settled.value;
    }
    const resolution = context.resolveActivityHandleResult(
      this.#activityDefinition,
      this.#commandId
    );
    if (resolution.kind !== "Pending") {
      this.#settled.value = resolution;
    }
    return resolution;
  }

  then<TResult1 = ActivityOutput<A>, TResult2 = never>(
    onfulfilled?: ((value: ActivityOutput<A>) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const context = currentWorkflowRuntimeContext();
    const resolution = this.#resolve(context);
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
    return context.hotSuspend(this.#commandId, () => {
      const pending = this.#resolve(context);
      if (pending.kind === "Completed") {
        return { kind: "Resolved", value: pending.value };
      }
      if (pending.kind === "Failed") {
        return { kind: "Rejected", error: new ActivityFailureError(pending.failure) };
      }
      return { kind: "Pending" };
    }).then(onfulfilled, _onrejected);
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
    const context = currentWorkflowRuntimeContext();
    // Chunked replay: park until the window can serve this call rather than
    // reading an empty window as "no recorded command" and appending a
    // duplicate. Nothing has been mutated yet, so retrying is exact.
    const historyGate = context.replayHistoryGate(1);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    const resolution = context.resolveActivityMap(
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
    return context.hotSuspend(
      resolution.commandId,
      () => context.resolveHotActivityMapResult<A>(resolution.commandId)
    ).then(onfulfilled, _onrejected);
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
    const context = currentWorkflowRuntimeContext();
    // Chunked replay: park until the window can serve this call rather than
    // reading an empty window as "no recorded command" and appending a
    // duplicate. Nothing has been mutated yet, so retrying is exact.
    const historyGate = context.replayHistoryGate(1);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    const resolution = context.resolveChildWorkflowMap(
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
    return context.hotSuspend(
      resolution.commandId,
      () => context.resolveHotChildWorkflowMapResult<W>(resolution.commandId)
    ).then(onfulfilled, _onrejected);
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
    const context = currentWorkflowRuntimeContext();
    // Chunked replay: park until the window can serve this call rather than
    // reading an empty window as "no recorded command" and appending a
    // duplicate. Nothing has been mutated yet, so retrying is exact.
    const historyGate = context.replayHistoryGate(1);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    const resolution = context.resolveChildWorkflowStart(
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
    return context.hotSuspend(
      resolution.commandId,
      () => context.resolveHotChildWorkflowStart(
        this.#workflowDefinition,
        resolution.commandId
      )
    ).then(onfulfilled, _onrejected);
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
  // Same ownership rule as `RuntimeActivityHandle`: the child's terminal event
  // is consumed once out of the runtime's index and then belongs to the handle.
  readonly #settled: SettledResolution<ChildWorkflowResultResolution<WorkflowOutput<W>>> = {
    value: null
  };

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
    return new ChildWorkflowResultDurablePromise(
      this.#workflowDefinition,
      this.#commandId,
      this.#settled
    );
  }
}

class ChildWorkflowResultDurablePromise<W extends WorkflowDefinition<any, any, any, string>>
  implements PromiseLike<WorkflowOutput<W>>
{
  readonly #workflowDefinition: W;
  readonly #commandId: CommandId;
  readonly #settled: SettledResolution<ChildWorkflowResultResolution<WorkflowOutput<W>>>;

  constructor(
    workflowDefinition: W,
    commandIdValue: CommandId,
    settled: SettledResolution<ChildWorkflowResultResolution<WorkflowOutput<W>>>
  ) {
    this.#workflowDefinition = workflowDefinition;
    this.#commandId = commandIdValue;
    this.#settled = settled;
  }

  #resolve(context: WorkflowRuntimeContext): ChildWorkflowResultResolution<WorkflowOutput<W>> {
    if (this.#settled.value !== null) {
      return this.#settled.value;
    }
    const resolution = context.resolveChildWorkflowResult(
      this.#workflowDefinition,
      this.#commandId
    );
    if (resolution.kind !== "Pending") {
      this.#settled.value = resolution;
    }
    return resolution;
  }

  then<TResult1 = WorkflowOutput<W>, TResult2 = never>(
    onfulfilled?: ((value: WorkflowOutput<W>) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const context = currentWorkflowRuntimeContext();
    const resolution = this.#resolve(context);
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
    return context.hotSuspend(this.#commandId, () => {
      const pending = this.#resolve(context);
      if (pending.kind === "Completed") {
        return { kind: "Resolved", value: pending.value };
      }
      if (pending.kind === "Failed") {
        return { kind: "Rejected", error: new ChildWorkflowFailureError(pending.failure) };
      }
      if (pending.kind === "Cancelled") {
        return { kind: "Rejected", error: new ChildWorkflowCancelledError(pending.reason) };
      }
      return { kind: "Pending" };
    }).then(onfulfilled, _onrejected);
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
    const context = currentWorkflowRuntimeContext();
    // Chunked replay: park until the window can serve this call rather than
    // reading an empty window as "no recorded command" and appending a
    // duplicate. Nothing has been mutated yet, so retrying is exact.
    const historyGate = context.replayHistoryGate(1);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    const resolution = context.resolveTimer(this.#spec);
    if (resolution.kind === "Fired") {
      return Promise.resolve(onfulfilled ? onfulfilled(undefined) : (undefined as TResult1));
    }
    return context.hotSuspend(
      resolution.commandId,
      () => context.resolveHotTimer(resolution.commandId)
    ).then(onfulfilled, _onrejected);
  }

  __durustRegisterJoinBranch(context: WorkflowRuntimeContext): JoinBranchResolution<void> {
    const resolution = context.resolveTimer(this.#spec);
    return resolution.kind === "Fired"
      ? { kind: "Ready", value: undefined, eventId: resolution.eventId }
      : {
          kind: "Pending",
          key: commandKey(resolution.commandId),
          resolve: () => context.resolveHotTimerBranch(resolution.commandId),
          cancel: () => context.cancelTimerBranch(resolution.commandId)
        };
  }
}

class SignalDurablePromise<Payload extends object, const Name extends string = string>
  implements SignalDefinition<Payload, Name>
{
  readonly durableBranchKind = "signal" as const;
  readonly kind = "signal" as const;
  readonly name: Name;
  readonly payloadSchema?: SchemaAdapter<Payload>;

  constructor(name: Name, payloadSchema?: SchemaAdapter<Payload>) {
    this.name = name;
    if (payloadSchema !== undefined) {
      this.payloadSchema = payloadSchema;
    }
  }

  then<TResult1 = Payload, TResult2 = never>(
    onfulfilled?: ((value: Payload) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): PromiseLike<TResult1 | TResult2> {
    const context = currentWorkflowRuntimeContext();
    // Chunked replay: park until the window can serve this call rather than
    // reading an empty window as "no recorded command" and appending a
    // duplicate. Nothing has been mutated yet, so retrying is exact.
    const historyGate = context.replayHistoryGate(1);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    const resolution = context.resolveSignal<Payload>(this.name, this.payloadSchema);
    if (resolution.kind === "Consumed") {
      return Promise.resolve(
        onfulfilled ? onfulfilled(resolution.value) : (resolution.value as unknown as TResult1)
      );
    }
    return context.hotSuspend(
      resolution.commandId,
      () => context.resolveHotSignal<Payload>(
        this.name,
        resolution.commandId,
        this.payloadSchema
      )
    ).then(onfulfilled, _onrejected);
  }

  __durustRegisterJoinBranch(context: WorkflowRuntimeContext): JoinBranchResolution<Payload> {
    const resolution = context.resolveSignal<Payload>(this.name, this.payloadSchema);
    return resolution.kind === "Consumed"
      ? { kind: "Ready", value: resolution.value, eventId: resolution.eventId }
      : {
          kind: "Pending",
          key: commandKey(resolution.commandId),
          resolve: () => context.resolveHotSignalBranch<Payload>(
            this.name,
            resolution.commandId,
            this.payloadSchema
          ),
          cancel: () => context.cancelSignalBranch(resolution.commandId)
        };
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
    // Chunked replay: park until the window can serve this call rather than
    // reading an empty window as "no recorded command" and appending a
    // duplicate. Nothing has been mutated yet, so retrying is exact.
    const historyGate = context.replayHistoryGate(Object.keys(this.#branches).length);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    const values: Record<string, unknown> = {};
    const pending: PendingJoinBranch<unknown>[] = [];
    for (const key of Object.keys(this.#branches)) {
      const branch = this.#branches[key];
      const durableBranch = asDurableJoinBranch(branch);
      const result = durableBranch.__durustRegisterJoinBranch(context);
      if (result.kind === "Pending") {
        pending.push(memoizeJoinBranch({
          ...result,
          resolve: () => {
            const resolved = result.resolve();
            if (resolved.kind === "Resolved") {
              return {
                kind: "Resolved",
                value: {
                  ...resolved.value,
                  value: { key, value: resolved.value.value }
                }
              };
            }
            return resolved as HotSuspendResolution<ReadyJoinBranch<unknown>>;
          }
        }));
      } else {
        values[key] = result.value;
      }
    }
    if (pending.length > 0) {
      return context.hotSuspendByKey(
        hotCompositeWaitKey("join", pending),
        () => resolveHotJoin(values, pending)
      ).then(onfulfilled, _onrejected);
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
    // Chunked replay: park until the window can serve this call rather than
    // reading an empty window as "no recorded command" and appending a
    // duplicate. Nothing has been mutated yet, so retrying is exact.
    const historyGate = context.replayHistoryGate(this.#branches.length);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    const values: unknown[] = [];
    const pending: PendingJoinBranch<unknown>[] = [];
    this.#branches.forEach((branch, index) => {
      const durableBranch = asDurableJoinBranch(branch);
      const result = durableBranch.__durustRegisterJoinBranch(context);
      if (result.kind === "Pending") {
        pending.push(memoizeJoinBranch({
          ...result,
          resolve: () => {
            const resolved = result.resolve();
            if (resolved.kind === "Resolved") {
              return {
                kind: "Resolved",
                value: {
                  ...resolved.value,
                  value: { index, value: resolved.value.value }
                }
              };
            }
            return resolved as HotSuspendResolution<ReadyJoinBranch<unknown>>;
          }
        }));
      } else {
        values[index] = result.value;
      }
    });
    if (pending.length > 0) {
      return context.hotSuspendByKey(
        hotCompositeWaitKey("joinAll", pending),
        () => resolveHotJoinAll(values, pending)
      ).then(onfulfilled, _onrejected);
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
    // Chunked replay: park until the window can serve this call rather than
    // reading an empty window as "no recorded command" and appending a
    // duplicate. Nothing has been mutated yet, so retrying is exact.
    const historyGate = context.replayHistoryGate(Object.keys(this.#branches).length + 1);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    // Before any branch registers, matching Rust's macro. See
    // `WorkflowRuntimeContext.beginSelect`.
    const selectCommandId = context.beginSelect();
    const keys = Object.keys(this.#branches);
    const ready: SelectReadyBranch[] = [];
    const pending: PendingJoinBranch<unknown>[] = [];
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
      } else {
        pending.push(memoizeJoinBranch({
          ...result,
          resolve: () => {
            const resolved = result.resolve();
            if (resolved.kind === "Resolved") {
              return {
                kind: "Resolved",
                value: {
                  ...resolved.value,
                  value: {
                    ordinal,
                    key,
                    value: resolved.value.value,
                    eventId: resolved.value.eventId
                  }
                }
              };
            }
            return resolved as HotSuspendResolution<ReadyJoinBranch<unknown>>;
          }
        }));
      }
    });

    if (ready.length === 0) {
      return context.hotSuspendByKey(
        hotCompositeWaitKey("select", pending),
        () => resolveHotSelect(context, selectCommandId, keys, ready, pending)
      ).then(onfulfilled, _onrejected);
    }

    const winner = context.resolveSelectWinner(selectCommandId, ready, selectBranchesDigest(keys));
    // Every branch still in `pending` lost without producing a value: nothing
    // resolves here, so this list is exactly Rust's `outputs[i].is_none()`
    // losers.
    cancelLosingSelectBranches(pending);
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
    // Chunked replay: park until the window can serve this call rather than
    // reading an empty window as "no recorded command" and appending a
    // duplicate. Nothing has been mutated yet, so retrying is exact.
    const historyGate = context.replayHistoryGate(this.#branches.length + 1);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    // Before any branch registers, matching Rust's macro. See
    // `WorkflowRuntimeContext.beginSelect`.
    const selectCommandId = context.beginSelect();
    const ready: SelectReadyBranch[] = [];
    const pending: PendingJoinBranch<unknown>[] = [];
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
      } else {
        pending.push(memoizeJoinBranch({
          ...result,
          resolve: () => {
            const resolved = result.resolve();
            if (resolved.kind === "Resolved") {
              return {
                kind: "Resolved",
                value: {
                  ...resolved.value,
                  value: {
                    ordinal,
                    key: String(ordinal),
                    value: resolved.value.value,
                    eventId: resolved.value.eventId
                  }
                }
              };
            }
            return resolved as HotSuspendResolution<ReadyJoinBranch<unknown>>;
          }
        }));
      }
    });

    if (ready.length === 0) {
      const keys = this.#branches.map((_, index) => String(index));
      return context.hotSuspendByKey(
        hotCompositeWaitKey("selectAll", pending),
        () => resolveHotSelectAll(context, selectCommandId, keys, ready, pending)
      ).then(onfulfilled, _onrejected);
    }

    const keys = this.#branches.map((_, index) => String(index));
    const winner = context.resolveSelectWinner(selectCommandId, ready, selectBranchesDigest(keys));
    cancelLosingSelectBranches(pending);
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
    const context = currentWorkflowRuntimeContext();
    // Gated before the callback runs, so a retry does not run the effect twice:
    // `#effect` and `#done` are still untouched on this path.
    const historyGate = context.replayHistoryGate(1);
    if (historyGate !== null) {
      return retryAfterReplayHistory(
        historyGate,
        () => this.then(onfulfilled, _onrejected),
        _onrejected
      );
    }
    const value = context.resolveSideEffect(this.#key, effect);
    this.#effect = null;
    this.#done = true;
    return Promise.resolve(onfulfilled ? onfulfilled(value) : (value as unknown as TResult1));
  }
}

type JoinBranchResolution<T> =
  | PendingJoinBranch<T>
  | ReadyJoinBranch<T>;

interface PendingJoinBranch<T> {
  readonly kind: "Pending";
  readonly key: string;
  resolve(): HotSuspendResolution<ReadyJoinBranch<T>>;
  /**
   * Withdraws whatever this branch registered with the provider — a wait, or
   * a scheduled activity.
   *
   * Called by `select` and `selectAll` on every branch that lost without
   * producing a value, and by nothing else: `join` and `joinAll` wait for
   * every branch, so no branch of theirs is ever abandoned.
   */
  cancel(): void;
}

interface ReadyJoinBranch<T> {
  readonly kind: "Ready";
  readonly value: T;
  readonly eventId: EventId;
}

/**
 * A one-slot owner for a resolution that has already been consumed out of the
 * runtime's ready-event index.
 *
 * A mutable box rather than a field so the handle that owns the value and the
 * short-lived promises it hands out can share one slot without exposing it.
 */
interface SettledResolution<T> {
  value: T | null;
}

/**
 * Wraps a pending composite branch so it is probed until it settles and never
 * after.
 *
 * `join`, `joinAll`, `select`, and `selectAll` re-probe every branch on each
 * ingest until the whole composite settles, and a branch's probe consumes its
 * ready event. Without this memo the second pass over a branch that already
 * resolved would find its event gone and report Pending forever, so a join
 * whose branches complete in separate chunks or separate tasks would hang.
 */
function memoizeJoinBranch<T>(branch: PendingJoinBranch<T>): PendingJoinBranch<T> {
  const settled: SettledResolution<HotSuspendResolution<ReadyJoinBranch<T>>> = { value: null };
  return {
    kind: "Pending",
    key: branch.key,
    cancel: () => {
      branch.cancel();
    },
    resolve: () => {
      if (settled.value !== null) {
        return settled.value;
      }
      const resolution = branch.resolve();
      if (resolution.kind !== "Pending") {
        settled.value = resolution;
      }
      return resolution;
    }
  };
}

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

function hotCompositeWaitKey(
  kind: "join" | "joinAll" | "select" | "selectAll",
  pending: readonly PendingJoinBranch<unknown>[]
): string {
  return `${kind}:${pending.map((branch) => branch.key).join("|")}`;
}

function resolveHotJoin(
  initialValues: Record<string, unknown>,
  pending: readonly PendingJoinBranch<unknown>[]
): HotSuspendResolution<Record<string, unknown>> {
  const values = { ...initialValues };
  for (const branch of pending) {
    const resolution = branch.resolve();
    if (resolution.kind !== "Resolved") {
      return resolution as HotSuspendResolution<Record<string, unknown>>;
    }
    const joined = resolution.value.value as { readonly key: string; readonly value: unknown };
    values[joined.key] = joined.value;
  }
  return { kind: "Resolved", value: values };
}

function resolveHotJoinAll(
  initialValues: readonly unknown[],
  pending: readonly PendingJoinBranch<unknown>[]
): HotSuspendResolution<readonly unknown[]> {
  const values = [...initialValues];
  for (const branch of pending) {
    const resolution = branch.resolve();
    if (resolution.kind !== "Resolved") {
      return resolution as HotSuspendResolution<readonly unknown[]>;
    }
    const joined = resolution.value.value as { readonly index: number; readonly value: unknown };
    values[joined.index] = joined.value;
  }
  return { kind: "Resolved", value: values };
}

function resolveHotSelect(
  context: WorkflowRuntimeContext,
  selectCommandId: CommandId,
  keys: readonly string[],
  initialReady: readonly SelectReadyBranch[],
  pending: readonly PendingJoinBranch<unknown>[]
): HotSuspendResolution<SelectRuntimeResult> {
  const ready = [...initialReady];
  // The branches that are still pending on the pass that settles the select.
  // A branch that resolved is not cancelled even when it loses: it already
  // consumed its ready event, so there is nothing left with the provider to
  // withdraw. Rust draws the same line with `outputs[i].is_none()`.
  const unresolved: PendingJoinBranch<unknown>[] = [];
  for (const branch of pending) {
    const resolution = branch.resolve();
    if (resolution.kind === "Rejected") {
      return resolution as HotSuspendResolution<SelectRuntimeResult>;
    }
    if (resolution.kind === "Resolved") {
      ready.push(resolution.value.value as SelectReadyBranch);
    } else {
      unresolved.push(branch);
    }
  }
  if (ready.length === 0) {
    return { kind: "Pending" };
  }
  const winner = context.resolveSelectWinner(selectCommandId, ready, selectBranchesDigest(keys));
  cancelLosingSelectBranches(unresolved);
  return { kind: "Resolved", value: { branch: winner.key, value: winner.value } };
}

/**
 * Withdraws every branch that lost a `select` without producing a value.
 *
 * Runs *after* the winner is recorded, never before, so a select that fails
 * its nondeterminism check leaves the losing branches alone — the same order
 * Rust's macro uses, where the cancel loop sits inside the `Ok(())` arm of
 * `__durust_select_record_winner`.
 */
function cancelLosingSelectBranches(losers: readonly PendingJoinBranch<unknown>[]): void {
  for (const loser of losers) {
    loser.cancel();
  }
}

function resolveHotSelectAll(
  context: WorkflowRuntimeContext,
  selectCommandId: CommandId,
  keys: readonly string[],
  initialReady: readonly SelectReadyBranch[],
  pending: readonly PendingJoinBranch<unknown>[]
): HotSuspendResolution<SelectAllRuntimeResult> {
  const resolution = resolveHotSelect(context, selectCommandId, keys, initialReady, pending);
  if (resolution.kind !== "Resolved") {
    return resolution as HotSuspendResolution<SelectAllRuntimeResult>;
  }
  return {
    kind: "Resolved",
    value: { index: Number(resolution.value.branch), value: resolution.value.value }
  };
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
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

// Marks a `sideEffect` callback frame in `#userCodeFrameApi`, selecting that
// message instead of the value-conversion one. Compared by value against names
// passed to the gate, none of which contains a space, so it cannot collide.
const SIDE_EFFECT_CALLBACK_FRAME = "sideEffect callback";

function describeDurableApi(api: string, detail: string | null | undefined): string {
  return detail === undefined || detail === null ? api : `${api}(${detail})`;
}

function isThenable(value: unknown): boolean {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    typeof (value as { readonly then?: unknown }).then === "function"
  );
}

/**
 * The durable failure a workflow's rejection carries, or `null` when the
 * rejection is a plain throw. Propagated activity, child, and child-map
 * failures, a `WorkflowFailure`, and any `DurableFailure`-shaped value close
 * the run as failed; everything else is a workflow-code fault.
 */
export function workflowRejectionFailure(error: unknown): DurableFailure | null {
  if (
    error instanceof WorkflowFailure ||
    error instanceof ActivityFailureError ||
    error instanceof ChildWorkflowFailureError ||
    error instanceof ChildWorkflowMapFailureError ||
    error instanceof ChildWorkflowCancelledError
  ) {
    return durableFailureFromUnknown(error);
  }
  if (
    error &&
    typeof error === "object" &&
    "errorType" in error &&
    "message" in error &&
    typeof (error as { readonly errorType?: unknown }).errorType === "string" &&
    typeof (error as { readonly message?: unknown }).message === "string"
  ) {
    return durableFailureFromUnknown(error);
  }
  return null;
}

function isWorkflowTaskFatalError(error: unknown): boolean {
  if (error instanceof UnsupportedWorkflowVersionError || error instanceof WorkflowCodeError) {
    return true;
  }
  // A task-level fault, never a workflow failure: the run is fine, this
  // execution's replay window was not big enough. Routing it here keeps a
  // workflow that swallows the throw from recording `WorkflowFailed`.
  if (error instanceof ReplayWindowOverrunError) {
    return true;
  }
  return (
    error instanceof Error &&
    (error.message.startsWith("nondeterminism:") ||
      error.message === "side effect key must not be empty")
  );
}

export function durableFailureFromUnknown(error: unknown): DurableFailure {
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

function activityTerminalFailure(event: HistoryEvent): DurableFailure | null {
  if (event.data.kind === "ActivityFailed") {
    return event.data.failed.failure;
  }
  if (event.data.kind === "ActivityTimedOut") {
    return {
      errorType: "ActivityTimedOut",
      message: event.data.timedOut.message,
      nonRetryable: true
    };
  }
  return null;
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

/**
 * The digest an activity (or activity-map) command's fingerprint carries.
 *
 * `taskQueue` is the queue the **caller asked for**, defaulted to
 * {@link DEFAULT_ACTIVITY_TASK_QUEUE} — never the scheduling worker's
 * configured `activityTaskQueue`. The resolved queue still goes into
 * `ActivityScheduled.taskQueue`; only the fingerprint is narrowed.
 *
 * Folding the worker's fallback in here made a command's identity depend on
 * the configuration of whichever worker happened to schedule it: two workflow
 * workers with different activity queues fingerprinted the same unqueued
 * `callActivity()` differently, and a run scheduled by one failed replay on the
 * other with `nondeterminism: activity command fingerprint changed`. Nothing
 * about a fingerprint should be readable from worker configuration; a
 * fingerprint answers "is this the same command the workflow issued last
 * time", and the workflow issued the same call either way.
 *
 * Defaulting to the literal rather than dropping the field is what makes this
 * a *retraction* rather than a second break. `defaultActivityTaskQueue` was
 * never passed into the runtime before, so every unqueued activity ever
 * recorded hashed `"default"` here; keeping the literal leaves every one of
 * those fingerprints byte-identical, and an explicitly queued call was never
 * affected either way. `src/runtime.rs` narrows its half the same way, against
 * `TaskQueue::default()`, which is the same string.
 */
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
