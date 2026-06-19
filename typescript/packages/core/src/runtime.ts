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
import { commandId, eventId, timestampMs, waitId, type CommandId, type DurableInput, type EventId, type RunId, type WaitId, type WorkflowId } from "./types.js";

const runtimeStorage = new AsyncLocalStorage<WorkflowRuntimeContext>();
let nondeterminismGuardsInstalled = false;
let originalDateConstructor: DateConstructor | undefined;
let originalDateNow: (() => number) | undefined;
let originalMathRandom: (() => number) | undefined;
let originalPerformanceNow: (() => number) | undefined;
let originalCryptoRandomUUID: (() => `${string}-${string}-${string}-${string}-${string}`) | undefined;
let originalCryptoGetRandomValues: Crypto["getRandomValues"] | undefined;
let originalProcessHrtime: typeof process.hrtime | undefined;
let originalProcessHrtimeBigint: (() => bigint) | undefined;
let originalProcessEnv: NodeJS.ProcessEnv | undefined;
let originalProcessCwd: typeof process.cwd | undefined;
let originalProcessChdir: typeof process.chdir | undefined;
let originalProcessCpuUsage: typeof process.cpuUsage | undefined;
let originalProcessMemoryUsage: typeof process.memoryUsage | undefined;
let originalProcessNextTick: typeof process.nextTick | undefined;
let originalProcessResourceUsage: typeof process.resourceUsage | undefined;
let originalProcessUptime: typeof process.uptime | undefined;
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
let guardedProcessEnv: NodeJS.ProcessEnv | undefined;

export interface PrepareWorkflowTaskOptions {
  readonly defaultActivityTaskQueue?: string;
  readonly defaultWorkflowTaskQueue?: string;
  readonly payloadCodec?: CodecId;
  readonly nowMs?: number;
  readonly liveSignals?: readonly SignalInboxRecord[];
}

type HotWorkflowTaskOptions = PrepareWorkflowTaskOptions;

type HotSuspendResolution<T> =
  | { readonly kind: "Pending" }
  | { readonly kind: "Resolved"; readonly value: T }
  | { readonly kind: "Rejected"; readonly error: unknown };

interface HotWaiter {
  readonly key: string;
  tryResolve(): boolean;
  reject(error: unknown): void;
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

class WorkflowSuspended extends Error {
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

export function publish<QueryState extends object>(view: DurableInput<QueryState>): void {
  currentWorkflowRuntimeContext().publish(view);
}

export function continueAsNew<Input extends object>(input: DurableInput<Input>): never {
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
  const context = new WorkflowRuntimeContext(
    claimed,
    options,
    workflowDefinition.inputSchema,
    workflowDefinition.queryStateSchema
  );
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

export class HotWorkflowExecution {
  readonly #context: WorkflowRuntimeContext;
  #observedProgressVersion: number;
  #fatalError: unknown = null;
  #terminalCommitPending = false;
  #closed = false;

  constructor(
    workflowDefinition: WorkflowDefinition<any, any, any, string>,
    input: object,
    claimed: ClaimedWorkflowTask,
    options: PrepareWorkflowTaskOptions = {}
  ) {
    installNondeterminismGuards();
    this.#context = new WorkflowRuntimeContext(
      claimed,
      options,
      workflowDefinition.inputSchema,
      workflowDefinition.queryStateSchema,
      "hot"
    );
    this.#observedProgressVersion = this.#context.hotProgressVersion();
    void runtimeStorage.run(this.#context, () => workflowDefinition.handler(input))
      .then((output: unknown) => {
        this.#context.completeWorkflow(output, workflowDefinition);
        this.#terminalCommitPending = true;
      })
      .catch((error: unknown) => {
        if (error instanceof ContinueAsNewRequested) {
          this.#terminalCommitPending = true;
          return;
        }
        if (error instanceof WorkflowSuspended) {
          this.#fatalError = new Error(
            "hot workflow execution encountered an unsupported durable suspension"
          );
          this.#context.notifyHotProgress();
          return;
        }
        if (isWorkflowTaskFatalError(error)) {
          this.#fatalError = error;
          this.#context.notifyHotProgress();
          return;
        }
        this.#context.failWorkflow(durableFailureFromUnknown(error));
        this.#terminalCommitPending = true;
      });
  }

  get closed(): boolean {
    return this.#closed;
  }

  async nextCommit(): Promise<WorkflowTaskCommit> {
    if (this.#closed) {
      throw new Error("hot workflow execution is closed");
    }
    await this.#context.waitForHotProgressAfter(this.#observedProgressVersion);
    this.#observedProgressVersion = this.#context.hotProgressVersion();
    if (this.#fatalError !== null) {
      throw this.#fatalError;
    }
    return this.#context.toCommit();
  }

  async advance(
    claimed: ClaimedWorkflowTask,
    options: HotWorkflowTaskOptions = {}
  ): Promise<WorkflowTaskCommit> {
    if (this.#closed) {
      throw new Error("hot workflow execution is closed");
    }
    this.#context.advanceHotClaim(claimed, options);
    return this.nextCommit();
  }

  markCommitted(newTailEventId: EventId): void {
    if (this.#closed) {
      throw new Error("hot workflow execution is closed");
    }
    this.#context.markHotCommitAccepted(newTailEventId);
    if (this.#terminalCommitPending) {
      this.#closed = true;
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

function installNondeterminismGuards(): void {
  if (nondeterminismGuardsInstalled) {
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
  originalProcessEnv = process.env;
  originalProcessCwd = process.cwd.bind(process);
  originalProcessChdir = process.chdir.bind(process);
  originalProcessCpuUsage = process.cpuUsage.bind(process);
  originalProcessMemoryUsage = process.memoryUsage.bind(process);
  originalProcessNextTick = process.nextTick.bind(process) as typeof process.nextTick;
  originalProcessResourceUsage =
    typeof process.resourceUsage === "function"
      ? process.resourceUsage.bind(process)
      : undefined;
  originalProcessUptime = process.uptime.bind(process);
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
  Object.defineProperty(globalThis, "Date", {
    configurable: true,
    writable: true,
    value: guardedDate
  });
  Object.defineProperty(guardedDate, "now", {
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
  if (globalThis.performance !== undefined) {
    Object.defineProperty(globalThis.performance, "now", {
      configurable: true,
      writable: true,
      value: () => {
        assertNotInWorkflowRuntime("performance.now()", "workflow time APIs such as sleepUntil()", {
          allowInSideEffect: true
        });
        return originalPerformanceNow?.() ?? 0;
      }
    });
  }
  if (globalThis.crypto !== undefined) {
    if (typeof globalThis.crypto.randomUUID === "function") {
      Object.defineProperty(globalThis.crypto, "randomUUID", {
        configurable: true,
        writable: true,
        value: () => {
          assertNotInWorkflowRuntime(
            "crypto.randomUUID()",
            "sideEffect() for recorded nondeterministic values",
            { allowInSideEffect: true }
          );
          return originalCryptoRandomUUID?.() ?? "00000000-0000-4000-8000-000000000000";
        }
      });
    }
    Object.defineProperty(globalThis.crypto, "getRandomValues", {
      configurable: true,
      writable: true,
      value: (<T extends Parameters<Crypto["getRandomValues"]>[0]>(array: T): T => {
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
    });
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
  Object.defineProperty(process, "hrtime", {
    configurable: true,
    writable: true,
    value: guardedHrtime
  });
  guardedProcessEnv = createGuardedProcessEnv(originalProcessEnv);
  Object.defineProperty(process, "env", {
    configurable: true,
    enumerable: true,
    get() {
      assertNotInWorkflowRuntime(
        "process.env",
        "workflow input or sideEffect() for recorded environment values",
        { allowInSideEffect: true }
      );
      return guardedProcessEnv;
    },
    set(value: NodeJS.ProcessEnv) {
      assertNotInWorkflowRuntime(
        "process.env assignment",
        "process configuration outside workflow code"
      );
      originalProcessEnv = value;
      guardedProcessEnv = createGuardedProcessEnv(value);
    }
  });
  Object.defineProperty(process, "cwd", {
    configurable: true,
    writable: true,
    value: (() => {
      assertNotInWorkflowRuntime(
        "process.cwd()",
        "workflow input or sideEffect() for recorded working directory",
        { allowInSideEffect: true }
      );
      return originalProcessCwd?.() ?? "";
    }) as typeof process.cwd
  });
  Object.defineProperty(process, "chdir", {
    configurable: true,
    writable: true,
    value: ((directory: string) => {
      assertNotInWorkflowRuntime(
        "process.chdir()",
        "process configuration outside workflow code"
      );
      return originalProcessChdir?.(directory);
    }) as typeof process.chdir
  });
  Object.defineProperty(process, "cpuUsage", {
    configurable: true,
    writable: true,
    value: ((previousValue?: NodeJS.CpuUsage) => {
      assertProcessRuntimeStateRead("process.cpuUsage()");
      if (previousValue === undefined) {
        return originalProcessCpuUsage?.() ?? { user: 0, system: 0 };
      }
      return originalProcessCpuUsage?.(previousValue) ?? { user: 0, system: 0 };
    }) as typeof process.cpuUsage
  });
  const guardedMemoryUsage = (() => {
    assertProcessRuntimeStateRead("process.memoryUsage()");
    return originalProcessMemoryUsage?.() ?? {
      rss: 0,
      heapTotal: 0,
      heapUsed: 0,
      external: 0,
      arrayBuffers: 0
    };
  }) as typeof process.memoryUsage;
  guardedMemoryUsage.rss = (() => {
    assertProcessRuntimeStateRead("process.memoryUsage.rss()");
    return originalProcessMemoryUsage?.rss?.() ?? originalProcessMemoryUsage?.().rss ?? 0;
  }) as typeof process.memoryUsage.rss;
  Object.defineProperty(process, "memoryUsage", {
    configurable: true,
    writable: true,
    value: guardedMemoryUsage
  });
  Object.defineProperty(process, "nextTick", {
    configurable: true,
    writable: true,
    value: ((callback: (...args: any[]) => void, ...args: any[]) => {
      assertNotInWorkflowRuntime("process.nextTick()", "durust durable operations");
      return originalProcessNextTick?.(callback, ...args);
    }) as typeof process.nextTick
  });
  if (originalProcessResourceUsage !== undefined) {
    Object.defineProperty(process, "resourceUsage", {
      configurable: true,
      writable: true,
      value: (() => {
        assertProcessRuntimeStateRead("process.resourceUsage()");
        return originalProcessResourceUsage?.();
      }) as typeof process.resourceUsage
    });
  }
  Object.defineProperty(process, "uptime", {
    configurable: true,
    writable: true,
    value: (() => {
      assertProcessRuntimeStateRead("process.uptime()");
      return originalProcessUptime?.() ?? 0;
    }) as typeof process.uptime
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
  if (originalSetImmediate !== undefined) {
    Object.defineProperty(globalThis, "setImmediate", {
      configurable: true,
      writable: true,
      value: ((...args: Parameters<typeof globalThis.setImmediate>) => {
        assertNotInWorkflowRuntime("setImmediate()", "durust durable operations");
        return originalSetImmediate?.(...args);
      }) as typeof globalThis.setImmediate
    });
  }
  Object.defineProperty(globalThis, "queueMicrotask", {
    configurable: true,
    writable: true,
    value: ((callback: VoidFunction) => {
      assertNotInWorkflowRuntime("queueMicrotask()", "durust durable operations");
      return originalQueueMicrotask?.(callback);
    }) as typeof globalThis.queueMicrotask
  });
  if (originalRequestAnimationFrame !== undefined) {
    Object.defineProperty(globalThis, "requestAnimationFrame", {
      configurable: true,
      writable: true,
      value: ((callback: FrameRequestCallback) => {
        assertNotInWorkflowRuntime("requestAnimationFrame()", "durust sleep() or sleepUntil()");
        return originalRequestAnimationFrame?.(callback) ?? 0;
      }) as typeof globalThis.requestAnimationFrame
    });
  }
  if (originalRequestIdleCallback !== undefined) {
    Object.defineProperty(globalThis, "requestIdleCallback", {
      configurable: true,
      writable: true,
      value: ((...args: Parameters<typeof globalThis.requestIdleCallback>) => {
        assertNotInWorkflowRuntime("requestIdleCallback()", "durust durable operations");
        return originalRequestIdleCallback?.(...args) ?? 0;
      }) as typeof globalThis.requestIdleCallback
    });
  }
  if (originalFetch !== undefined) {
    Object.defineProperty(globalThis, "fetch", {
      configurable: true,
      writable: true,
      value: ((...args: Parameters<typeof globalThis.fetch>) => {
        assertNotInWorkflowRuntime("fetch()", "callActivity() for external I/O");
        return originalFetch?.(...args) ?? Promise.reject(new Error("fetch is unavailable"));
      }) as typeof globalThis.fetch
    });
  }
  installNetworkConstructorGuard("WebSocket", originalWebSocket, "callActivity() for network I/O");
  installNetworkConstructorGuard("EventSource", originalEventSource, "callActivity() for network I/O");
  installNetworkConstructorGuard(
    "XMLHttpRequest",
    originalXMLHttpRequest,
    "callActivity() for network I/O"
  );
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
  Object.defineProperty(globalThis, globalName, {
    configurable: true,
    writable: true,
    value: guarded
  });
}

function createGuardedProcessEnv(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  return new Proxy(env, {
    get(target, property, receiver) {
      if (typeof property === "string") {
        assertProcessEnvRead();
      }
      return Reflect.get(target, property, receiver) as string | undefined;
    },
    has(target, property) {
      if (typeof property === "string") {
        assertProcessEnvRead();
      }
      return Reflect.has(target, property);
    },
    ownKeys(target) {
      assertProcessEnvRead();
      return Reflect.ownKeys(target);
    },
    getOwnPropertyDescriptor(target, property) {
      if (typeof property === "string") {
        assertProcessEnvRead();
      }
      return Reflect.getOwnPropertyDescriptor(target, property);
    },
    set(target, property, value, receiver) {
      if (typeof property === "string") {
        assertProcessEnvMutation();
      }
      return Reflect.set(target, property, value, receiver);
    },
    deleteProperty(target, property) {
      if (typeof property === "string") {
        assertProcessEnvMutation();
      }
      return Reflect.deleteProperty(target, property);
    },
    defineProperty(target, property, descriptor) {
      if (typeof property === "string") {
        assertProcessEnvMutation();
      }
      return Reflect.defineProperty(target, property, descriptor);
    }
  });
}

function assertProcessEnvRead(): void {
  assertNotInWorkflowRuntime(
    "process.env",
    "workflow input or sideEffect() for recorded environment values",
    { allowInSideEffect: true }
  );
}

function assertProcessEnvMutation(): void {
  assertNotInWorkflowRuntime(
    "process.env mutation",
    "process configuration outside workflow code"
  );
}

function assertProcessRuntimeStateRead(apiName: string): void {
  assertNotInWorkflowRuntime(
    apiName,
    "workflow input or sideEffect() for recorded process runtime state",
    { allowInSideEffect: true }
  );
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
  readonly #mode: "replay" | "hot";
  readonly #hotWaiters = new Map<string, HotWaiter>();
  #hotProgressVersion = 0;
  #hotProgressWaiters: Deferred<void>[] = [];

  constructor(
    claimed: ClaimedWorkflowTask,
    options: PrepareWorkflowTaskOptions,
    workflowInputSchema?: SchemaAdapter<unknown>,
    queryStateSchema?: SchemaAdapter<unknown>,
    mode: "replay" | "hot" = "replay"
  ) {
    this.#claimed = claimed;
    this.#defaultActivityTaskQueue = options.defaultActivityTaskQueue ?? "default";
    this.#defaultWorkflowTaskQueue = options.defaultWorkflowTaskQueue ?? "default";
    this.#payloadCodec = options.payloadCodec ?? "MessagePack";
    this.#workflowInputSchema = workflowInputSchema;
    this.#queryStateSchema = queryStateSchema;
    this.#nowMs = options.nowMs ?? 0;
    this.#liveSignals = [...(options.liveSignals ?? [])];
    this.#mode = mode;
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
    this.#ingestHistory(claimed.prefetchedHistory);
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

  isHot(): boolean {
    return this.#mode === "hot";
  }

  hotProgressVersion(): number {
    return this.#hotProgressVersion;
  }

  notifyHotProgress(): void {
    if (!this.isHot()) {
      return;
    }
    this.#hotProgressVersion += 1;
    const waiters = this.#hotProgressWaiters;
    this.#hotProgressWaiters = [];
    for (const waiter of waiters) {
      waiter.resolve(undefined);
    }
  }

  async waitForHotProgressAfter(version: number): Promise<void> {
    if (!this.isHot()) {
      throw new Error("hot progress can only be awaited for hot workflow execution");
    }
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
    if (!this.isHot()) {
      throw new WorkflowSuspended();
    }
    const immediate = resolve();
    if (immediate.kind === "Resolved") {
      return Promise.resolve(immediate.value);
    }
    if (immediate.kind === "Rejected") {
      return Promise.reject(immediate.error);
    }

    const deferred = createDeferred<T>();
    const waiter: HotWaiter = {
      key,
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
    if (!this.isHot()) {
      throw new Error("advanceHotClaim requires hot workflow execution");
    }
    this.#claimed = claimed;
    this.#nowMs = options.nowMs ?? this.#nowMs;
    this.#liveSignals = [...(options.liveSignals ?? [])];
    this.#ingestHistory(claimed.prefetchedHistory);
    this.#tryResolveHotWaiters();
  }

  markHotCommitAccepted(newTailEventId: EventId): void {
    if (!this.isHot()) {
      throw new Error("markHotCommitAccepted requires hot workflow execution");
    }
    this.#claimed = {
      ...this.#claimed,
      replayTargetEventId: newTailEventId
    };
    this.#appendEvents.length = 0;
    this.#upsertWaits.length = 0;
    this.#deleteWaits.length = 0;
    this.#consumeSignals.length = 0;
    this.#scheduleActivities.length = 0;
    this.#scheduleActivityMaps.length = 0;
    this.#startChildWorkflows.length = 0;
    this.#scheduleChildWorkflowMaps.length = 0;
    this.#queryProjection = null;
  }

  #tryResolveHotWaiters(): void {
    for (const waiter of [...this.#hotWaiters.values()]) {
      try {
        waiter.tryResolve();
      } catch (error) {
        waiter.reject(error);
      }
    }
  }

  allowsNondeterministicGlobals(): boolean {
    return this.#allowNondeterministicGlobalsDepth > 0;
  }

  resolveSignal<Payload extends object>(
    name: string,
    payloadSchema?: SchemaAdapter<Payload>
  ): SignalResolution<Payload> {
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
        value: decodePayload<Payload>(consumed.payload as PayloadRef<Payload>, payloadSchema),
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
      const failure = failed === undefined ? null : activityTerminalFailure(failed);
      if (failed !== undefined && failure !== null) {
        return {
          kind: "Failed",
          commandId: scheduled.commandId,
          failure,
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
    const failure = failed === undefined ? null : activityTerminalFailure(failed);
    if (failed !== undefined && failure !== null) {
      return {
        kind: "Failed",
        commandId: id,
        failure,
        eventId: failed.eventId
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
    const terminal = this.#timerFires.get(commandKey(id));
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
    const live = this.#takeLiveSignal(name);
    if (live === undefined) {
      return { kind: "Pending" };
    }
    const consumedEventId = this.#nextAppendEventId();
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
    const completed = this.#activityMapCompletions.get(commandKey(id));
    if (completed?.data.kind === "ActivityMapCompleted") {
      return {
        kind: "Resolved",
        value: completed.data.completed.resultManifest as PayloadRef<ActivityMapResultManifest<ActivityOutput<A>>>
      };
    }
    const failed = this.#activityMapFailures.get(commandKey(id));
    if (failed?.data.kind === "ActivityMapFailed") {
      return { kind: "Rejected", error: new ActivityFailureError(failed.data.failed.failure) };
    }
    return { kind: "Pending" };
  }

  resolveHotChildWorkflowMapResult<W extends WorkflowDefinition<any, any, any, string>>(
    id: CommandId
  ): HotSuspendResolution<PayloadRef<ChildWorkflowMapResultManifest<WorkflowOutput<W>>>> {
    const completed = this.#childMapCompletions.get(commandKey(id));
    if (completed?.data.kind === "ChildWorkflowMapCompleted") {
      return {
        kind: "Resolved",
        value: completed.data.completed.resultManifest as PayloadRef<ChildWorkflowMapResultManifest<WorkflowOutput<W>>>
      };
    }
    const failed = this.#childMapFailures.get(commandKey(id));
    if (failed?.data.kind === "ChildWorkflowMapFailed") {
      return { kind: "Rejected", error: new ChildWorkflowMapFailureError(failed.data.failed.failure) };
    }
    return { kind: "Pending" };
  }

  resolveHotChildWorkflowStart<W extends WorkflowDefinition<any, any, any, string>>(
    workflowDefinition: W,
    id: CommandId
  ): HotSuspendResolution<ChildWorkflowHandle<WorkflowOutput<W>>> {
    const started = this.#childStarts.get(commandKey(id));
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
    const failed = this.#childFailures.get(commandKey(id));
    if (failed?.data.kind === "ChildWorkflowFailed") {
      return { kind: "Rejected", error: new ChildWorkflowFailureError(failed.data.failed.failure) };
    }
    return { kind: "Pending" };
  }

  resolveHotChildWorkflowResult<W extends WorkflowDefinition<any, any, any, string>>(
    workflowDefinition: W,
    id: CommandId
  ): HotSuspendResolution<WorkflowOutput<W>> {
    const resolution = this.resolveChildWorkflowResult(workflowDefinition, id);
    if (resolution.kind === "Completed") {
      return { kind: "Resolved", value: resolution.value };
    }
    if (resolution.kind === "Failed") {
      return { kind: "Rejected", error: new ChildWorkflowFailureError(resolution.failure) };
    }
    if (resolution.kind === "Cancelled") {
      return { kind: "Rejected", error: new ChildWorkflowCancelledError(resolution.reason) };
    }
    return { kind: "Pending" };
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
    this.notifyHotProgress();
  }

  failWorkflow(failure: DurableFailure): void {
    this.#appendEvents.push({
      data: {
        kind: "WorkflowFailed",
        failure
      }
    });
    this.notifyHotProgress();
  }

  publish<QueryState extends object>(view: QueryState): void {
    assertDurableInputValue(view, "query projection");
    this.#queryProjection = encodePayload(view, {
      codec: this.#payloadCodec,
      ...(this.#queryStateSchema === undefined
        ? {}
        : { schema: this.#queryStateSchema as SchemaAdapter<QueryState> })
    });
  }

  continueAsNew<Input extends object>(input: Input): never {
    assertDurableInputValue(input, "continueAsNew input");
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
    this.notifyHotProgress();
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
    const context = currentWorkflowRuntimeContext();
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
    if (context.isHot()) {
      return context.hotSuspend(
        resolution.commandId,
        () => context.resolveHotActivity(this.#activityDefinition, resolution.commandId)
      ).then(onfulfilled, _onrejected);
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
    return {
      kind: "Pending",
      key: commandKey(resolution.commandId),
      resolve: () => context.resolveHotActivityBranch(this.#activityDefinition, resolution.commandId)
    };
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
    const context = currentWorkflowRuntimeContext();
    const resolution = context.resolveActivityHandleResult(
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
    if (context.isHot()) {
      return context.hotSuspend(
        this.#commandId,
        () => context.resolveHotActivity(this.#activityDefinition, this.#commandId)
      ).then(onfulfilled, _onrejected);
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
    const context = currentWorkflowRuntimeContext();
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
    if (context.isHot()) {
      return context.hotSuspend(
        resolution.commandId,
        () => context.resolveHotActivityMapResult<A>(resolution.commandId)
      ).then(onfulfilled, _onrejected);
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
    const context = currentWorkflowRuntimeContext();
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
    if (context.isHot()) {
      return context.hotSuspend(
        resolution.commandId,
        () => context.resolveHotChildWorkflowMapResult<W>(resolution.commandId)
      ).then(onfulfilled, _onrejected);
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
    const context = currentWorkflowRuntimeContext();
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
    if (context.isHot()) {
      return context.hotSuspend(
        resolution.commandId,
        () => context.resolveHotChildWorkflowStart(
          this.#workflowDefinition,
          resolution.commandId
        )
      ).then(onfulfilled, _onrejected);
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
    const context = currentWorkflowRuntimeContext();
    const resolution = context.resolveChildWorkflowResult(
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
    if (context.isHot()) {
      return context.hotSuspend(
        this.#commandId,
        () => context.resolveHotChildWorkflowResult(this.#workflowDefinition, this.#commandId)
      ).then(onfulfilled, _onrejected);
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
    const context = currentWorkflowRuntimeContext();
    const resolution = context.resolveTimer(this.#spec);
    if (resolution.kind === "Fired") {
      return Promise.resolve(onfulfilled ? onfulfilled(undefined) : (undefined as TResult1));
    }
    if (context.isHot()) {
      return context.hotSuspend(
        resolution.commandId,
        () => context.resolveHotTimer(resolution.commandId)
      ).then(onfulfilled, _onrejected);
    }
    throw new WorkflowSuspended();
  }

  __durustRegisterJoinBranch(context: WorkflowRuntimeContext): JoinBranchResolution<void> {
    const resolution = context.resolveTimer(this.#spec);
    return resolution.kind === "Fired"
      ? { kind: "Ready", value: undefined, eventId: resolution.eventId }
      : {
          kind: "Pending",
          key: commandKey(resolution.commandId),
          resolve: () => context.resolveHotTimerBranch(resolution.commandId)
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
    const resolution = context.resolveSignal<Payload>(this.name, this.payloadSchema);
    if (resolution.kind === "Consumed") {
      return Promise.resolve(
        onfulfilled ? onfulfilled(resolution.value) : (resolution.value as unknown as TResult1)
      );
    }
    if (context.isHot()) {
      return context.hotSuspend(
        resolution.commandId,
        () => context.resolveHotSignal<Payload>(
          this.name,
          resolution.commandId,
          this.payloadSchema
        )
      ).then(onfulfilled, _onrejected);
    }
    throw new WorkflowSuspended();
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
          )
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
    const values: Record<string, unknown> = {};
    const pending: PendingJoinBranch<unknown>[] = [];
    for (const key of Object.keys(this.#branches)) {
      const branch = this.#branches[key];
      const durableBranch = asDurableJoinBranch(branch);
      const result = durableBranch.__durustRegisterJoinBranch(context);
      if (result.kind === "Pending") {
        pending.push({
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
        });
      } else {
        values[key] = result.value;
      }
    }
    if (pending.length > 0) {
      if (context.isHot()) {
        return context.hotSuspendByKey(
          hotCompositeWaitKey("join", pending),
          () => resolveHotJoin(values, pending)
        ).then(onfulfilled, _onrejected);
      }
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
    const pending: PendingJoinBranch<unknown>[] = [];
    this.#branches.forEach((branch, index) => {
      const durableBranch = asDurableJoinBranch(branch);
      const result = durableBranch.__durustRegisterJoinBranch(context);
      if (result.kind === "Pending") {
        pending.push({
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
        });
      } else {
        values[index] = result.value;
      }
    });
    if (pending.length > 0) {
      if (context.isHot()) {
        return context.hotSuspendByKey(
          hotCompositeWaitKey("joinAll", pending),
          () => resolveHotJoinAll(values, pending)
        ).then(onfulfilled, _onrejected);
      }
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
        pending.push({
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
        });
      }
    });

    if (ready.length === 0) {
      if (context.isHot()) {
        return context.hotSuspendByKey(
          hotCompositeWaitKey("select", pending),
          () => resolveHotSelect(context, keys, ready, pending)
        ).then(onfulfilled, _onrejected);
      }
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
        pending.push({
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
        });
      }
    });

    if (ready.length === 0) {
      if (context.isHot()) {
        const keys = this.#branches.map((_, index) => String(index));
        return context.hotSuspendByKey(
          hotCompositeWaitKey("selectAll", pending),
          () => resolveHotSelectAll(context, keys, ready, pending)
        ).then(onfulfilled, _onrejected);
      }
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
  | PendingJoinBranch<T>
  | ReadyJoinBranch<T>;

interface PendingJoinBranch<T> {
  readonly kind: "Pending";
  readonly key: string;
  resolve(): HotSuspendResolution<ReadyJoinBranch<T>>;
}

interface ReadyJoinBranch<T> {
  readonly kind: "Ready";
  readonly value: T;
  readonly eventId: EventId;
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
  keys: readonly string[],
  initialReady: readonly SelectReadyBranch[],
  pending: readonly PendingJoinBranch<unknown>[]
): HotSuspendResolution<SelectRuntimeResult> {
  const ready = [...initialReady];
  for (const branch of pending) {
    const resolution = branch.resolve();
    if (resolution.kind === "Rejected") {
      return resolution as HotSuspendResolution<SelectRuntimeResult>;
    }
    if (resolution.kind === "Resolved") {
      ready.push(resolution.value.value as SelectReadyBranch);
    }
  }
  if (ready.length === 0) {
    return { kind: "Pending" };
  }
  const winner = context.resolveSelectWinner(ready, selectBranchesDigest(keys));
  return { kind: "Resolved", value: { branch: winner.key, value: winner.value } };
}

function resolveHotSelectAll(
  context: WorkflowRuntimeContext,
  keys: readonly string[],
  initialReady: readonly SelectReadyBranch[],
  pending: readonly PendingJoinBranch<unknown>[]
): HotSuspendResolution<SelectAllRuntimeResult> {
  const resolution = resolveHotSelect(context, keys, initialReady, pending);
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

function assertDurableInputValue(value: unknown, label: string): void {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be a durable input object`);
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
