export const ruleName = "no-workflow-nondeterminism";

export type WorkflowDeterminismMessageId =
  | "activityApi"
  | "hiddenIo"
  | "nativeAsync"
  | "unknownAwait";

export interface WorkflowDeterminismViolation {
  readonly code:
    | `durust/${typeof ruleName}`
    | "durust/no-hidden-io"
    | "durust/no-native-async"
    | "durust/no-activity-api-in-workflow"
    | "durust/no-unknown-await";
  readonly messageId: WorkflowDeterminismMessageId;
  readonly message: string;
}

export interface AwaitExpressionDescriptor {
  readonly kind: "call" | "memberCall" | "durableIdentifier" | "identifier" | "other";
  readonly name: string;
  readonly displayName?: string;
}

export interface EslintRuleContext {
  report(descriptor: {
    readonly node: unknown;
    readonly messageId: WorkflowDeterminismMessageId;
    readonly data: { readonly message: string };
  }): void;
}

export interface EslintRuleModule {
  readonly meta: {
    readonly type: "problem";
    readonly docs: {
      readonly description: string;
      readonly recommended: true;
    };
    readonly schema: readonly unknown[];
    readonly messages: Record<WorkflowDeterminismMessageId, string>;
  };
  create(context: EslintRuleContext): Record<string, (node: EslintNode) => void>;
}

export interface EslintNode {
  readonly type: string;
  readonly [key: string]: unknown;
}

export interface EslintPlugin {
  readonly meta: {
    readonly name: string;
    readonly version: string;
  };
  readonly rules: Record<typeof ruleName, EslintRuleModule>;
  configs: {
    recommended: {
      readonly plugins: {
        readonly durust: EslintPlugin;
      };
      readonly rules: Record<`durust/${typeof ruleName}`, "error">;
    };
  };
}

const FORBIDDEN_MODULES = new Map([
  ["fs", "filesystem I/O is not allowed in workflow code"],
  ["node:fs", "filesystem I/O is not allowed in workflow code"],
  ["fs/promises", "filesystem I/O is not allowed in workflow code"],
  ["node:fs/promises", "filesystem I/O is not allowed in workflow code"],
  ["http", "network I/O is not allowed in workflow code"],
  ["node:http", "network I/O is not allowed in workflow code"],
  ["http2", "network I/O is not allowed in workflow code"],
  ["node:http2", "network I/O is not allowed in workflow code"],
  ["https", "network I/O is not allowed in workflow code"],
  ["node:https", "network I/O is not allowed in workflow code"],
  ["dgram", "network I/O is not allowed in workflow code"],
  ["node:dgram", "network I/O is not allowed in workflow code"],
  ["net", "network I/O is not allowed in workflow code"],
  ["node:net", "network I/O is not allowed in workflow code"],
  ["tls", "network I/O is not allowed in workflow code"],
  ["node:tls", "network I/O is not allowed in workflow code"],
  ["dns", "network I/O is not allowed in workflow code"],
  ["node:dns", "network I/O is not allowed in workflow code"],
  ["readline", "terminal I/O is not allowed in workflow code"],
  ["node:readline", "terminal I/O is not allowed in workflow code"],
  ["readline/promises", "terminal I/O is not allowed in workflow code"],
  ["node:readline/promises", "terminal I/O is not allowed in workflow code"],
  ["repl", "terminal I/O and dynamic evaluation are not allowed in workflow code"],
  ["node:repl", "terminal I/O and dynamic evaluation are not allowed in workflow code"],
  ["vm", "dynamic code evaluation is not allowed in workflow code"],
  ["node:vm", "dynamic code evaluation is not allowed in workflow code"],
  ["timers", "native timer APIs are not allowed in workflow code"],
  ["node:timers", "native timer APIs are not allowed in workflow code"],
  ["timers/promises", "native timer APIs are not allowed in workflow code"],
  ["node:timers/promises", "native timer APIs are not allowed in workflow code"],
  ["os", "host/process state reads are not allowed in workflow code"],
  ["node:os", "host/process state reads are not allowed in workflow code"],
  ["perf_hooks", "host timing APIs are not allowed in workflow code"],
  ["node:perf_hooks", "host timing APIs are not allowed in workflow code"],
  ["inspector", "debugger/process inspection APIs are not allowed in workflow code"],
  ["node:inspector", "debugger/process inspection APIs are not allowed in workflow code"],
  ["inspector/promises", "debugger/process inspection APIs are not allowed in workflow code"],
  ["node:inspector/promises", "debugger/process inspection APIs are not allowed in workflow code"],
  ["async_hooks", "native async scheduler state is not allowed in workflow code"],
  ["node:async_hooks", "native async scheduler state is not allowed in workflow code"],
  ["child_process", "child processes are not allowed in workflow code"],
  ["node:child_process", "child processes are not allowed in workflow code"],
  ["cluster", "process clustering is not allowed in workflow code"],
  ["node:cluster", "process clustering is not allowed in workflow code"],
  ["worker_threads", "worker threads are not allowed in workflow code"],
  ["node:worker_threads", "worker threads are not allowed in workflow code"],
  ["undici", "network I/O is not allowed in workflow code"]
]);

const FORBIDDEN_IMPORT_BINDINGS = new Map([
  ["crypto:generateKey", "node crypto key generation uses nondeterministic host randomness; record random values with sideEffect()"],
  ["crypto:generateKeyPair", "node crypto key-pair generation uses nondeterministic host randomness; record random values with sideEffect()"],
  ["crypto:generateKeyPairSync", "node crypto key-pair generation uses nondeterministic host randomness; record random values with sideEffect()"],
  ["crypto:pseudoRandomBytes", "node crypto random bytes are nondeterministic; record random values with sideEffect()"],
  ["crypto:randomBytes", "node crypto random bytes are nondeterministic; record random values with sideEffect()"],
  ["crypto:randomFill", "node crypto random fills are nondeterministic; record random values with sideEffect()"],
  ["crypto:randomFillSync", "node crypto random fills are nondeterministic; record random values with sideEffect()"],
  ["crypto:randomInt", "node crypto random integers are nondeterministic; record random values with sideEffect()"],
  ["crypto:randomUUID", "node crypto random UUIDs are nondeterministic; record random values with sideEffect()"],
  ["crypto:webcrypto", "node crypto webcrypto exposes nondeterministic random APIs; record random values with sideEffect()"],
  ["node:crypto:generateKey", "node crypto key generation uses nondeterministic host randomness; record random values with sideEffect()"],
  ["node:crypto:generateKeyPair", "node crypto key-pair generation uses nondeterministic host randomness; record random values with sideEffect()"],
  ["node:crypto:generateKeyPairSync", "node crypto key-pair generation uses nondeterministic host randomness; record random values with sideEffect()"],
  ["node:crypto:pseudoRandomBytes", "node crypto random bytes are nondeterministic; record random values with sideEffect()"],
  ["node:crypto:randomBytes", "node crypto random bytes are nondeterministic; record random values with sideEffect()"],
  ["node:crypto:randomFill", "node crypto random fills are nondeterministic; record random values with sideEffect()"],
  ["node:crypto:randomFillSync", "node crypto random fills are nondeterministic; record random values with sideEffect()"],
  ["node:crypto:randomInt", "node crypto random integers are nondeterministic; record random values with sideEffect()"],
  ["node:crypto:randomUUID", "node crypto random UUIDs are nondeterministic; record random values with sideEffect()"],
  ["node:crypto:webcrypto", "node crypto webcrypto exposes nondeterministic random APIs; record random values with sideEffect()"]
]);

const FORBIDDEN_NAMESPACE_IMPORT_MODULES = new Map([
  ["crypto", "crypto namespace imports expose nondeterministic random APIs; import deterministic helpers by name and record random values with sideEffect()"],
  ["node:crypto", "node crypto namespace imports expose nondeterministic random APIs; import deterministic helpers by name and record random values with sideEffect()"]
]);

const FORBIDDEN_IDENTIFIER_CALLS = new Map([
  ["Date", "use workflow time APIs such as sleepUntil()"],
  ["eval", "dynamic code evaluation is not allowed in workflow code"],
  ["Function", "dynamic code generation is not allowed in workflow code"],
  ["setTimeout", "use durust sleep() or sleepUntil()"],
  ["setInterval", "use durust sleep() or recurring workflow timers"],
  ["setImmediate", "use durust durable operations"],
  ["queueMicrotask", "use durust durable operations"],
  ["requestAnimationFrame", "use durust sleep() or sleepUntil()"],
  ["requestIdleCallback", "use durust durable operations"],
  ["fetch", "network I/O is not allowed in workflow code"]
]);

const FORBIDDEN_CONSTRUCTORS = new Map([
  ["AsyncFunction", "dynamic async code generation is not allowed in workflow code"],
  ["XMLHttpRequest", "network I/O is not allowed in workflow code"],
  ["WebSocket", "network I/O is not allowed in workflow code"],
  ["EventSource", "network I/O is not allowed in workflow code"],
  ["Function", "dynamic code generation is not allowed in workflow code"],
  ["GeneratorFunction", "dynamic generator code generation is not allowed in workflow code"],
  ["AsyncGeneratorFunction", "dynamic async generator code generation is not allowed in workflow code"],
  ["Worker", "native workers are not allowed in workflow code"],
  ["SharedWorker", "native workers are not allowed in workflow code"],
  ["MessageChannel", "native message channels are not allowed in workflow code"],
  ["BroadcastChannel", "native broadcast channels are not allowed in workflow code"],
  ["WebAssembly.Instance", "WebAssembly native execution is not allowed in workflow code"],
  ["WebAssembly.Module", "WebAssembly native code compilation is not allowed in workflow code"]
]);

const FORBIDDEN_ZERO_ARG_CONSTRUCTORS = new Map([
  ["Date", "use workflow time APIs such as sleepUntil()"]
]);

const FORBIDDEN_STATIC_CALLS = new Map([
  ["AbortSignal.timeout", "use durust sleep() or sleepUntil()"],
  ["Atomics.wait", "native blocking waits are not allowed in workflow code"],
  ["Atomics.waitAsync", "native async waits are not allowed in workflow code"],
  ["caches.delete", "browser cache storage is hidden I/O; use activities for external state"],
  ["caches.match", "browser cache storage is hidden I/O; use activities for external state"],
  ["caches.open", "browser cache storage is hidden I/O; use activities for external state"],
  ["console.assert", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.clear", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.count", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.countReset", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.debug", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.dir", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.dirxml", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.error", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.group", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.groupCollapsed", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.groupEnd", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.info", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.log", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.profile", "console profiling is hidden host state; collect profiles outside workflow code"],
  ["console.profileEnd", "console profiling is hidden host state; collect profiles outside workflow code"],
  ["console.table", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.time", "console timing is hidden host state; use workflow time APIs or worker metrics outside workflow code"],
  ["console.timeEnd", "console timing is hidden host state; use workflow time APIs or worker metrics outside workflow code"],
  ["console.timeLog", "console timing is hidden host state; use workflow time APIs or worker metrics outside workflow code"],
  ["console.timeStamp", "console timing is hidden host state; use workflow time APIs or worker metrics outside workflow code"],
  ["console.trace", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["console.warn", "console output is a replay-visible side effect; use activities or worker logs outside workflow code"],
  ["Date.now", "use workflow time APIs such as sleepUntil()"],
  ["document.querySelector", "DOM reads are hidden host state; pass values through workflow input"],
  ["document.querySelectorAll", "DOM reads are hidden host state; pass values through workflow input"],
  ["history.back", "browser history mutation is not allowed in workflow code"],
  ["history.forward", "browser history mutation is not allowed in workflow code"],
  ["history.go", "browser history mutation is not allowed in workflow code"],
  ["history.pushState", "browser history mutation is not allowed in workflow code"],
  ["history.replaceState", "browser history mutation is not allowed in workflow code"],
  ["indexedDB.deleteDatabase", "browser database access is hidden I/O; use activities for external state"],
  ["indexedDB.open", "browser database access is hidden I/O; use activities for external state"],
  ["localStorage.clear", "browser storage is hidden I/O; use activities for external state"],
  ["localStorage.getItem", "browser storage is hidden I/O; pass values through workflow input"],
  ["localStorage.removeItem", "browser storage mutation is not allowed in workflow code"],
  ["localStorage.setItem", "browser storage mutation is not allowed in workflow code"],
  ["Math.random", "use sideEffect() for recorded nondeterministic values"],
  ["navigator.clipboard.read", "clipboard access is hidden host I/O; use activities for external state"],
  ["navigator.clipboard.readText", "clipboard access is hidden host I/O; use activities for external state"],
  ["navigator.clipboard.write", "clipboard mutation is not allowed in workflow code"],
  ["navigator.clipboard.writeText", "clipboard mutation is not allowed in workflow code"],
  ["navigator.geolocation.getCurrentPosition", "geolocation is hidden host I/O; use activities for external state"],
  ["navigator.geolocation.watchPosition", "geolocation is hidden host I/O; use activities for external state"],
  ["navigator.locks.request", "native lock scheduling is not allowed in workflow code"],
  ["navigator.sendBeacon", "network I/O is not allowed in workflow code"],
  ["navigator.serviceWorker.register", "service worker mutation is not allowed in workflow code"],
  ["performance.now", "use workflow time APIs such as sleepUntil()"],
  ["location.assign", "browser navigation mutation is not allowed in workflow code"],
  ["location.reload", "browser navigation mutation is not allowed in workflow code"],
  ["location.replace", "browser navigation mutation is not allowed in workflow code"],
  ["crypto.randomUUID", "use sideEffect() for recorded nondeterministic values"],
  ["crypto.getRandomValues", "use sideEffect() for recorded nondeterministic values"],
  ["process.cwd", "pass working directory through workflow input or record it with sideEffect()"],
  ["process.abort", "process termination is not allowed in workflow code"],
  ["process.chdir", "process-global working directory changes are not allowed in workflow code"],
  ["process.cpuUsage", "process runtime usage is hidden process-global state; record it with sideEffect() if needed"],
  ["process.emitWarning", "process warnings are replay-visible side effects; log from activities or workers"],
  ["process.exit", "process termination is not allowed in workflow code"],
  ["process.hrtime", "use workflow time APIs such as sleepUntil()"],
  ["process.hrtime.bigint", "use workflow time APIs such as sleepUntil()"],
  ["process.kill", "process signalling is not allowed in workflow code"],
  ["process.memoryUsage", "process runtime usage is hidden process-global state; record it with sideEffect() if needed"],
  ["process.memoryUsage.rss", "process runtime usage is hidden process-global state; record it with sideEffect() if needed"],
  ["process.nextTick", "use durust durable operations"],
  ["process.reallyExit", "process termination is not allowed in workflow code"],
  ["process.report.getReport", "process reports are hidden process-global state; collect reports outside workflow code"],
  ["process.report.writeReport", "process report writes are hidden I/O; collect reports outside workflow code"],
  ["process.resourceUsage", "process runtime usage is hidden process-global state; record it with sideEffect() if needed"],
  ["process.stderr.end", "process stderr writes are replay-visible side effects; log from activities or workers"],
  ["process.stderr.write", "process stderr writes are replay-visible side effects; log from activities or workers"],
  ["process.stdin.read", "process stdin reads are hidden I/O; pass values through workflow input"],
  ["process.stdout.end", "process stdout writes are replay-visible side effects; log from activities or workers"],
  ["process.stdout.write", "process stdout writes are replay-visible side effects; log from activities or workers"],
  ["process.uptime", "process runtime uptime is hidden process-global state; record it with sideEffect() if needed"],
  ["Promise.all", "use durust join() or joinAll()"],
  ["Promise.race", "use durust select() or selectAll()"],
  ["Promise.allSettled", "use durust join()/joinAll() plus explicit error handling"],
  ["Promise.any", "use durust select() or selectAll()"],
  ["sessionStorage.clear", "browser storage is hidden I/O; use activities for external state"],
  ["sessionStorage.getItem", "browser storage is hidden I/O; pass values through workflow input"],
  ["sessionStorage.removeItem", "browser storage mutation is not allowed in workflow code"],
  ["sessionStorage.setItem", "browser storage mutation is not allowed in workflow code"],
  ["WebAssembly.compile", "WebAssembly native code compilation is not allowed in workflow code"],
  ["WebAssembly.compileStreaming", "WebAssembly streaming compilation is hidden host I/O; use activities for external state"],
  ["WebAssembly.instantiate", "WebAssembly native code execution is not allowed in workflow code"],
  ["WebAssembly.instantiateStreaming", "WebAssembly streaming instantiation is hidden host I/O; use activities for external state"],
  ["WebAssembly.validate", "WebAssembly native code validation is not allowed in workflow code"]
]);

const FORBIDDEN_STATIC_READS = new Map([
  [
    "document.cookie",
    "browser cookie reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "document.location",
    "browser location reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "globalThis.document",
    "DOM reads are hidden host state; pass values through workflow input"
  ],
  [
    "globalThis.location",
    "browser location reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "globalThis.localStorage",
    "browser storage reads are hidden host state; use activities for external state"
  ],
  [
    "globalThis.navigator",
    "browser navigator reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "globalThis.sessionStorage",
    "browser storage reads are hidden host state; use activities for external state"
  ],
  [
    "location.hash",
    "browser location reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "location.href",
    "browser location reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "location.search",
    "browser location reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "navigator.cookieEnabled",
    "browser navigator reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "navigator.hardwareConcurrency",
    "browser hardware reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "navigator.language",
    "browser locale reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "navigator.languages",
    "browser locale reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "navigator.onLine",
    "browser connectivity reads are hidden host state; use activities for external state"
  ],
  [
    "navigator.platform",
    "browser platform reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "navigator.userAgent",
    "browser user-agent reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "performance.timeOrigin",
    "host timing origin is hidden process-global state; record it with sideEffect() if needed"
  ],
  [
    "process.arch",
    "process identity reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "process.argv",
    "process argument reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "process.execArgv",
    "process argument reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "process.env",
    "environment variable reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "process.exitCode",
    "process exit state is hidden process-global state and must not be read or mutated in workflow code"
  ],
  [
    "process.pid",
    "process identity reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "process.platform",
    "process identity reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "process.ppid",
    "process identity reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "process.release",
    "process identity reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "process.stderr",
    "process stderr is hidden process-global I/O; log from activities or workers"
  ],
  [
    "process.stdin",
    "process stdin is hidden process-global I/O; pass values through workflow input"
  ],
  [
    "process.stdout",
    "process stdout is hidden process-global I/O; log from activities or workers"
  ],
  [
    "process.title",
    "process identity reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "process.version",
    "process identity reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "process.versions",
    "process identity reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "global.process.env",
    "environment variable reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "globalThis.process.env",
    "environment variable reads are hidden process-global state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "window.document",
    "DOM reads are hidden host state; pass values through workflow input"
  ],
  [
    "window.location",
    "browser location reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "window.localStorage",
    "browser storage reads are hidden host state; use activities for external state"
  ],
  [
    "window.navigator",
    "browser navigator reads are hidden host state; pass values through workflow input or record them with sideEffect()"
  ],
  [
    "window.sessionStorage",
    "browser storage reads are hidden host state; use activities for external state"
  ]
]);

const DURABLE_AWAIT_CALLS = new Set([
  "callActivity",
  "childWorkflow",
  "childWorkflowMap",
  "activityMap",
  "join",
  "joinAll",
  "select",
  "selectAll",
  "sideEffect",
  "signal",
  "sleep",
  "sleepUntil"
]);

const DURABLE_AWAIT_MEMBER_CALLS = new Set(["spawn", "result", "resultManifest"]);
const DURUST_CORE_MODULES = new Set(["@durust/core"]);

const ACTIVITY_ONLY_WORKFLOW_APIS = new Map([
  [
    "heartbeat",
    "heartbeat() records liveness for the currently claimed activity; call it only inside activity handlers"
  ]
]);

export function checkModuleSpecifier(moduleName: string): WorkflowDeterminismViolation | null {
  const reason = FORBIDDEN_MODULES.get(moduleName);
  if (reason === undefined) {
    return null;
  }
  return {
    code: "durust/no-hidden-io",
    messageId: "hiddenIo",
    message: `${reason}: ${moduleName}`
  };
}

export function checkImportBinding(
  moduleName: string,
  importedName: string
): WorkflowDeterminismViolation | null {
  const namespaceReason = importedName === "*" || importedName === "default"
    ? FORBIDDEN_NAMESPACE_IMPORT_MODULES.get(moduleName)
    : undefined;
  if (namespaceReason !== undefined) {
    return {
      code: "durust/no-native-async",
      messageId: "nativeAsync",
      message: `${moduleName} namespace import is not allowed in workflow code; ${namespaceReason}`
    };
  }
  const reason = FORBIDDEN_IMPORT_BINDINGS.get(`${moduleName}:${importedName}`);
  if (reason === undefined) {
    return null;
  }
  return {
    code: "durust/no-native-async",
    messageId: "nativeAsync",
    message: `${moduleName}.${importedName} import is not allowed in workflow code; ${reason}`
  };
}

export function checkIdentifierCall(name: string): WorkflowDeterminismViolation | null {
  const replacement = FORBIDDEN_IDENTIFIER_CALLS.get(name);
  if (replacement === undefined) {
    return null;
  }
  return {
    code: "durust/no-native-async",
    messageId: "nativeAsync",
    message: `${name}() is not allowed in workflow code; ${replacement}`
  };
}

export function checkIdentifierReference(name: string): WorkflowDeterminismViolation | null {
  const replacement = FORBIDDEN_IDENTIFIER_CALLS.get(name);
  if (replacement === undefined) {
    return null;
  }
  return {
    code: "durust/no-native-async",
    messageId: "nativeAsync",
    message: `${name} reference is not allowed in workflow code; ${replacement}`
  };
}

export function checkStaticCall(name: string): WorkflowDeterminismViolation | null {
  const replacement = FORBIDDEN_STATIC_CALLS.get(name);
  if (replacement === undefined) {
    return null;
  }
  return {
    code: "durust/no-native-async",
    messageId: "nativeAsync",
    message: `${name}() is not allowed in workflow code; ${replacement}`
  };
}

export function checkStaticReference(name: string): WorkflowDeterminismViolation | null {
  const replacement = FORBIDDEN_STATIC_CALLS.get(name);
  if (replacement !== undefined) {
    return {
      code: "durust/no-native-async",
      messageId: "nativeAsync",
      message: `${name} reference is not allowed in workflow code; ${replacement}`
    };
  }
  return checkStaticRead(name);
}

export function checkStaticRead(name: string): WorkflowDeterminismViolation | null {
  const reason = FORBIDDEN_STATIC_READS.get(name);
  if (reason === undefined) {
    return null;
  }
  return {
    code: "durust/no-hidden-io",
    messageId: "hiddenIo",
    message: `${name} read is not allowed in workflow code; ${reason}`
  };
}

export function checkActivityOnlyWorkflowApi(
  apiName: string,
  displayName = `${apiName}()`
): WorkflowDeterminismViolation | null {
  const reason = ACTIVITY_ONLY_WORKFLOW_APIS.get(apiName);
  if (reason === undefined) {
    return null;
  }
  return {
    code: "durust/no-activity-api-in-workflow",
    messageId: "activityApi",
    message: `${displayName} is activity-only and cannot be used in workflow code; ${reason}`
  };
}

export function checkConstructor(
  name: string,
  options: { readonly argumentCount?: number } = {}
): WorkflowDeterminismViolation | null {
  const zeroArgReplacement = FORBIDDEN_ZERO_ARG_CONSTRUCTORS.get(name);
  if (zeroArgReplacement !== undefined && (options.argumentCount ?? 0) === 0) {
    return {
      code: "durust/no-native-async",
      messageId: "nativeAsync",
      message: `new ${name}() is not allowed in workflow code; ${zeroArgReplacement}`
    };
  }
  const replacement = FORBIDDEN_CONSTRUCTORS.get(name);
  if (replacement === undefined) {
    return null;
  }
  return {
    code: "durust/no-hidden-io",
    messageId: "hiddenIo",
    message: `${name} is not allowed in workflow code; ${replacement}`
  };
}

export function checkAwaitExpression(
  descriptor: AwaitExpressionDescriptor
): WorkflowDeterminismViolation | null {
  if (descriptor.kind === "durableIdentifier") {
    return null;
  }
  if (descriptor.kind === "call" && DURABLE_AWAIT_CALLS.has(descriptor.name)) {
    return null;
  }
  if (
    descriptor.kind === "memberCall" &&
    DURABLE_AWAIT_MEMBER_CALLS.has(descriptor.name)
  ) {
    return null;
  }
  const awaited = descriptor.displayName ?? descriptor.name;
  return {
    code: "durust/no-unknown-await",
    messageId: "unknownAwait",
    message:
      `awaited expression is not a Durust durable operation: ${awaited}; ` +
      "workflow code may only await Durust durable APIs such as callActivity(), sleep(), " +
      "signal(), select(), join(), childWorkflow(...).spawn(), or durable handle result methods"
  };
}

export const noWorkflowNondeterminismRule: EslintRuleModule = {
  meta: {
    type: "problem",
    docs: {
      description: "disallow nondeterministic APIs inside Durust workflow code",
      recommended: true
    },
    schema: [],
    messages: {
      activityApi: "{{message}}",
      hiddenIo: "{{message}}",
      nativeAsync: "{{message}}",
      unknownAwait: "{{message}}"
    }
  },
  create(context) {
    const durableAwaitIdentifiers = new Set<string>();
    const activityOnlyWorkflowIdentifiers = new Map<string, string>();
    const durustCoreNamespaces = new Set<string>();

    function report(node: EslintNode, violation: WorkflowDeterminismViolation | null): void {
      if (violation === null) {
        return;
      }
      context.report({
        node,
        messageId: violation.messageId,
        data: { message: violation.message }
      });
    }

    function reportModule(node: EslintNode): void {
      const moduleName = sourceString(node);
      if (moduleName !== null) {
        report(node, checkModuleSpecifier(moduleName));
      }
    }

    function reportImportBindings(node: EslintNode): void {
      const moduleName = sourceString(node);
      if (moduleName === null || node.importKind === "type") {
        return;
      }
      const specifiers = Array.isArray(node.specifiers) ? node.specifiers : [];
      for (const specifier of specifiers) {
        const specifierNode = asNode(specifier);
        if (specifierNode === null || specifierNode.importKind === "type") {
          continue;
        }
        if (specifierNode.type === "ImportNamespaceSpecifier") {
          if (DURUST_CORE_MODULES.has(moduleName)) {
            const localName = identifierName(asNode(specifierNode.local) ?? specifierNode);
            if (localName.length > 0) {
              durustCoreNamespaces.add(localName);
            }
          }
          report(specifierNode, checkImportBinding(moduleName, "*"));
          continue;
        }
        if (specifierNode.type === "ImportDefaultSpecifier") {
          report(specifierNode, checkImportBinding(moduleName, "default"));
          continue;
        }
        if (specifierNode.type === "ImportSpecifier") {
          const imported = asNode(specifierNode.imported);
          const importedName =
            imported?.type === "Identifier"
              ? identifierName(imported)
              : imported === null
                ? null
                : literalString(imported);
          const local = asNode(specifierNode.local);
          const localName = local?.type === "Identifier" ? identifierName(local) : importedName;
          if (importedName !== null) {
            if (
              DURUST_CORE_MODULES.has(moduleName) &&
              ACTIVITY_ONLY_WORKFLOW_APIS.has(importedName) &&
              localName !== null
            ) {
              activityOnlyWorkflowIdentifiers.set(localName, importedName);
            }
            report(specifierNode, checkImportBinding(moduleName, importedName));
          }
        }
      }
    }

    return {
      VariableDeclarator(node) {
        const id = asNode(node.id);
        reportForbiddenStaticReferenceAliases(node);
        if (id?.type !== "Identifier") {
          return;
        }
        const init = asNode(node.init);
        if (init !== null && checkAwaitExpression(awaitDescriptor(init, durableAwaitIdentifiers)) === null) {
          durableAwaitIdentifiers.add(identifierName(id));
        }
      },
      ImportDeclaration(node) {
        reportModule(node);
        reportImportBindings(node);
      },
      ExportNamedDeclaration: reportModule,
      ExportAllDeclaration: reportModule,
      CallExpression(node) {
        const callee = asNode(node.callee);
        if (callee === null) {
          return;
        }
        if (callee.type === "Import") {
          const moduleName = firstStringArgument(node);
          if (moduleName !== null) {
            report(node, checkModuleSpecifier(moduleName));
          }
          return;
        }
        if (callee.type === "Identifier") {
          const name = identifierName(callee);
          report(
            node,
            checkActivityOnlyWorkflowApi(
              activityOnlyWorkflowIdentifiers.get(name) ?? "",
              `${name}()`
            )
          );
          if (name === "require") {
            const moduleName = firstStringArgument(node);
            if (moduleName !== null) {
              report(node, checkModuleSpecifier(moduleName));
            }
          }
          report(node, checkIdentifierCall(name));
          return;
        }
        if (callee.type === "MemberExpression") {
          report(node, checkDurustCoreNamespaceActivityOnlyCall(callee));
          report(node, checkStaticCall(memberExpressionName(callee)));
        }
      },
      MemberExpression(node) {
        report(node, checkStaticRead(memberExpressionName(node)));
      },
      ImportExpression(node) {
        const moduleName = sourceString(node);
        if (moduleName !== null) {
          report(node, checkModuleSpecifier(moduleName));
        }
      },
      NewExpression(node) {
        const callee = asNode(node.callee);
        if (callee?.type === "Identifier") {
          const args = Array.isArray(node.arguments) ? node.arguments : [];
          report(node, checkConstructor(identifierName(callee), { argumentCount: args.length }));
          return;
        }
        if (callee?.type === "MemberExpression") {
          const args = Array.isArray(node.arguments) ? node.arguments : [];
          report(node, checkConstructor(memberExpressionName(callee), { argumentCount: args.length }));
        }
      },
      AwaitExpression(node) {
        const argument = asNode(node.argument);
        if (argument !== null) {
          report(node, checkAwaitExpression(awaitDescriptor(argument, durableAwaitIdentifiers)));
        }
      }
    };

    function reportForbiddenStaticReferenceAliases(node: EslintNode): void {
      const init = asNode(node.init);
      if (init === null) {
        return;
      }
      if (init.type === "MemberExpression") {
        report(node, checkStaticReference(memberExpressionName(init)));
        return;
      }
      const id = asNode(node.id);
      if (init.type === "Identifier" && id?.type !== "ObjectPattern") {
        report(node, checkIdentifierReference(identifierName(init)));
        return;
      }
      if (id?.type !== "ObjectPattern" || init.type !== "Identifier") {
        return;
      }
      const objectName = identifierName(init);
      const properties = Array.isArray(id.properties) ? id.properties : [];
      for (const property of properties) {
        const propertyNode = asNode(property);
        const propertyName = objectPatternPropertyName(propertyNode);
        if (propertyName !== null) {
          report(propertyNode ?? node, checkStaticReference(`${objectName}.${propertyName}`));
        }
      }
    }

    function checkDurustCoreNamespaceActivityOnlyCall(
      node: EslintNode
    ): WorkflowDeterminismViolation | null {
      const object = asNode(node.object);
      const property = asNode(node.property);
      if (object?.type !== "Identifier" || property?.type !== "Identifier") {
        return null;
      }
      const objectName = identifierName(object);
      if (!durustCoreNamespaces.has(objectName)) {
        return null;
      }
      const apiName = identifierName(property);
      return checkActivityOnlyWorkflowApi(apiName, `${objectName}.${apiName}()`);
    }
  }
};

const plugin = {
  meta: {
    name: "@durust/eslint-plugin",
    version: "0.0.0"
  },
  rules: {
    [ruleName]: noWorkflowNondeterminismRule
  },
  configs: {} as EslintPlugin["configs"]
} as EslintPlugin;

plugin.configs.recommended = {
  plugins: {
    durust: plugin
  },
  rules: {
    "durust/no-workflow-nondeterminism": "error"
  }
};

export const rules = plugin.rules;
export const configs = plugin.configs;

export default plugin;

function sourceString(node: EslintNode): string | null {
  const source = asNode(node.source);
  return source === null ? null : literalString(source);
}

function firstStringArgument(node: EslintNode): string | null {
  const args = Array.isArray(node.arguments) ? node.arguments : [];
  const first = asNode(args[0]);
  return first === null ? null : literalString(first);
}

function memberExpressionName(node: EslintNode): string {
  const object = asNode(node.object);
  const property = asNode(node.property);
  const objectName =
    object?.type === "Identifier"
      ? identifierName(object)
      : object?.type === "MemberExpression"
        ? memberExpressionName(object)
        : "";
  const propertyName =
    property?.type === "Identifier"
      ? identifierName(property)
      : property === null
        ? ""
        : literalString(property) ?? "";
  if (objectName.length === 0 || propertyName.length === 0) {
    return "";
  }
  return `${objectName}.${propertyName}`;
}

function objectPatternPropertyName(node: EslintNode | null): string | null {
  if (node === null || node.type !== "Property") {
    return null;
  }
  const key = asNode(node.key);
  if (key?.type === "Identifier") {
    return identifierName(key);
  }
  return key === null ? null : literalString(key);
}

function awaitDescriptor(
  node: EslintNode,
  durableAwaitIdentifiers: ReadonlySet<string>
): AwaitExpressionDescriptor {
  if (node.type === "Identifier") {
    const name = identifierName(node);
    return durableAwaitIdentifiers.has(name)
      ? { kind: "durableIdentifier", name }
      : { kind: "identifier", name };
  }
  if (node.type === "CallExpression") {
    const callee = asNode(node.callee);
    if (callee?.type === "Identifier") {
      return { kind: "call", name: identifierName(callee) };
    }
    if (callee?.type === "MemberExpression") {
      const property = asNode(callee.property);
      const name = property?.type === "Identifier" ? identifierName(property) : "member";
      return {
        kind: "memberCall",
        name,
        displayName: memberExpressionDisplayName(callee)
      };
    }
    return { kind: "other", name: "call expression" };
  }
  if (node.type === "MemberExpression") {
    return {
      kind: "other",
      name: memberExpressionDisplayName(node)
    };
  }
  return { kind: "other", name: node.type };
}

function memberExpressionDisplayName(node: EslintNode): string {
  const object = asNode(node.object);
  const property = asNode(node.property);
  const propertyName =
    property?.type === "Identifier"
      ? identifierName(property)
      : property === null
        ? "member"
        : literalString(property) ?? "member";
  if (object?.type === "Identifier") {
    return `${identifierName(object)}.${propertyName}`;
  }
  if (object?.type === "MemberExpression") {
    const objectName = memberExpressionName(object);
    if (objectName.length > 0) {
      return `${objectName}.${propertyName}`;
    }
  }
  if (object?.type === "CallExpression") {
    const callee = asNode(object.callee);
    if (callee?.type === "Identifier") {
      return `${identifierName(callee)}(...).${propertyName}`;
    }
  }
  return propertyName;
}

function identifierName(node: EslintNode): string {
  return typeof node.name === "string" ? node.name : "";
}

function literalString(node: EslintNode): string | null {
  if (typeof node.value === "string") {
    return node.value;
  }
  return null;
}

function asNode(value: unknown): EslintNode | null {
  return value !== null && typeof value === "object" && "type" in value
    ? (value as EslintNode)
    : null;
}
