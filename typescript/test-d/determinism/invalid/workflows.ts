import { readFileSync } from "node:fs";
import { randomBytes as nodeRandomBytes, randomUUID as nodeRandomUUID } from "node:crypto";
import { request } from "node:http";
import { hostname } from "node:os";
import { performance as nodePerformance } from "node:perf_hooks";
import { setTimeout as nativeSleep } from "node:timers/promises";
import { Script } from "node:vm";
import { Worker } from "node:worker_threads";
import * as nodeCrypto from "node:crypto";
import { heartbeat, workflow } from "@durust/core";

interface NoInput {}

async function readFromCache(): Promise<string> {
  return "native-cache";
}

export const invalidWorkflow = workflow({
  name: "lint.invalid.workflow",
  version: 1,
  handler: async (_input: NoInput): Promise<string> => {
    const nativePromise = Promise.resolve("native");
    const timestamp = Date.now();
    const dateString = Date();
    const constructedDate = new Date();
    const monotonic = performance.now();
    const timeOrigin = performance.timeOrigin;
    const nodeMonotonic = nodePerformance.now();
    const random = Math.random();
    const randomId = crypto.randomUUID();
    const randomBytes = crypto.getRandomValues(new Uint8Array(16));
    const importedRandomId = nodeRandomUUID();
    const importedRandomBytes = nodeRandomBytes(16);
    const namespaceRandomInt = nodeCrypto.randomInt(10);
    const abortSignal = AbortSignal.timeout(1);
    const atomicBuffer = new SharedArrayBuffer(4);
    const atomicView = new Int32Array(atomicBuffer);
    const atomicWait = Atomics.wait(atomicView, 0, 0, 1);
    const highResolution = process.hrtime();
    const highResolutionBigint = process.hrtime.bigint();
    const cwd = process.cwd();
    process.chdir(cwd);
    const cpuUsage = process.cpuUsage();
    const memoryUsage = process.memoryUsage();
    const memoryUsageRss = process.memoryUsage.rss();
    const resourceUsage = process.resourceUsage();
    const uptime = process.uptime();
    const pid = process.pid;
    const argv = process.argv;
    const platform = process.platform;
    const exitCode = process.exitCode;
    const stdout = process.stdout;
    const host = hostname();
    process.nextTick(() => undefined);
    const deploymentMode = process.env.DURUST_DEPLOYMENT_MODE;
    const processEnv = process.env;
    const globalProcessEnv = globalThis.process.env;
    const { DURUST_TASK_QUEUE: taskQueueName } = process.env;
    const { env: processEnvAlias } = process;
    const userAgent = navigator.userAgent;
    const language = navigator.language;
    const online = navigator.onLine;
    const cookie = document.cookie;
    const href = location.href;
    const localValue = localStorage.getItem("durust");
    const sessionValue = sessionStorage.getItem("durust");
    const dateNow = Date.now;
    const computedTimestamp = Date["now"]();
    const computedDateNow = Date["now"];
    const { timeout: abortTimeout } = AbortSignal;
    const { log: consoleLog } = console;
    const { random: unsafeRandom } = Math;
    const computedRandom = Math["random"]();
    const { all: promiseAll } = Promise;
    const computedPromiseAll = Promise["all"];
    const { now: performanceNow } = performance;
    const { randomUUID, getRandomValues } = crypto;
    const { exit: processExit } = process;
    const hrtimeBigint = process.hrtime.bigint;
    const computedProcessEnv = process["env"];
    const computedHrtimeBigint = process["hrtime"]["bigint"];
    const evalResult = eval("1 + 1");
    const unsafeEval = eval;
    const functionResult = Function("return 1")();
    const constructedFunction = new Function("return 2");
    const script = new Script("1 + 1");
    const scriptResult = script.runInNewContext({});
    const wasmBytes = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
    const wasmCompile = WebAssembly.compile(wasmBytes);
    const wasmInstantiate = WebAssembly.instantiate(wasmBytes);
    const wasmCompileStreaming = WebAssembly.compileStreaming(Promise.resolve(new Response(wasmBytes)));
    const wasmInstantiateStreaming = WebAssembly.instantiateStreaming(Promise.resolve(new Response(wasmBytes)));
    const wasmValidate = WebAssembly.validate(wasmBytes);
    const wasmModule = new WebAssembly.Module(wasmBytes);
    const wasmInstance = new WebAssembly.Instance(wasmModule);
    const wasmCompileReference = WebAssembly.compile;
    console.log("workflow output");
    console.error("workflow error");
    console.time("workflow-timer");
    console.timeEnd("workflow-timer");
    console.trace("workflow trace");
    process.stdout.write("workflow stdout");
    process.stderr.write("workflow stderr");
    process.stdin.read();
    process.emitWarning("workflow warning");
    setTimeout(() => undefined, 1);
    setImmediate(() => undefined);
    AbortSignal.timeout(2);
    Atomics.wait(atomicView, 0, 0, 1);
    requestAnimationFrame(() => undefined);
    requestIdleCallback(() => undefined);
    queueMicrotask(() => undefined);
    navigator.sendBeacon("https://example.com");
    navigator.clipboard.readText();
    navigator.geolocation.getCurrentPosition(() => undefined);
    navigator.locks.request("durust", async () => undefined);
    navigator.serviceWorker.register("/worker.js");
    document.querySelector("body");
    history.pushState({}, "", "/next");
    location.reload();
    localStorage.setItem("durust", "value");
    sessionStorage.setItem("durust", "value");
    indexedDB.open("durust");
    caches.open("durust");
    await nativePromise;
    await nativeSleep(1);
    await Promise.resolve("resolved");
    await readFromCache();
    await Promise.race([Promise.resolve("raced")]);
    await Promise.allSettled([Promise.resolve("settled")]);
    const activityHeartbeat = await heartbeat();
    await fetch("https://example.com");
    await import("node:fs/promises");
    const datagramModule = await import("node:dgram");
    const http2Module = await import("node:http2");
    const readlineModule = await import("node:readline/promises");
    const replModule = await import("node:repl");
    const inspectorModule = await import("node:inspector");
    const clusterModule = await import("node:cluster");
    const childProcess = require("node:child_process");
    new WebSocket("wss://example.com");
    new Worker(new URL(import.meta.url));
    new SharedWorker(new URL(import.meta.url));
    new MessageChannel();
    new BroadcastChannel("durust");
    request("https://example.com");
    readFileSync(new URL(import.meta.url));
    return `${timestamp}:${dateString}:${constructedDate}:${monotonic}:${random}:${randomId}:${
      randomBytes.byteLength
    }:${importedRandomId}:${importedRandomBytes.byteLength}:${namespaceRandomInt}:${timeOrigin}:${
      nodeMonotonic
    }:${abortSignal.aborted}:${atomicWait}:${highResolution}:${highResolutionBigint}:${
      cwd
    }:${cpuUsage.user}:${memoryUsage.rss}:${memoryUsageRss}:${
      resourceUsage.userCPUTime
    }:${uptime}:${pid}:${argv.length}:${platform}:${exitCode}:${stdout}:${host}:${
      deploymentMode
    }:${processEnv}:${globalProcessEnv}:${taskQueueName}:${processEnvAlias}:${userAgent}:${
      language
    }:${online}:${cookie}:${href}:${localValue}:${sessionValue}:${dateNow}:${abortTimeout}:${
      consoleLog
    }:${unsafeRandom}:${promiseAll}:${performanceNow}:${randomUUID}:${getRandomValues}:${
      processExit
    }:${hrtimeBigint}:${computedTimestamp}:${computedDateNow}:${computedRandom}:${
      computedPromiseAll
    }:${computedProcessEnv}:${computedHrtimeBigint}:${evalResult}:${unsafeEval}:${
      functionResult
    }:${constructedFunction}:${scriptResult}:${wasmCompile}:${wasmInstantiate}:${
      wasmCompileStreaming
    }:${wasmInstantiateStreaming}:${wasmValidate}:${wasmModule}:${wasmInstance}:${
      wasmCompileReference
    }:${activityHeartbeat.kind}:${
      datagramModule
    }:${http2Module}:${readlineModule}:${replModule}:${inspectorModule}:${
      clusterModule
    }:${childProcess}`;
  }
});
