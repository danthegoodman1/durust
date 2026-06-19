import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { beforeAll, describe, expect, it } from "vitest";

const execFileAsync = promisify(execFile);
const workspaceRootUrl = new URL("../../..", import.meta.url);
const workspaceRoot = fileURLToPath(workspaceRootUrl);
const lintScript = fileURLToPath(new URL("scripts/determinism-lint.mjs", workspaceRootUrl));

describe("workflow determinism lint", () => {
  beforeAll(async () => {
    await execFileAsync("npm", ["run", "build", "--workspace", "@durust/eslint-plugin"], {
      cwd: workspaceRoot
    });
  });

  it("accepts workflow source from package configuration", async () => {
    await expect(runLint()).resolves.toMatchObject({
      stdout: "",
      stderr: ""
    });
  });

  it("accepts explicit workflow source globs", async () => {
    await expect(
      runLint("--workflow-source", "test-d/determinism/valid/**/*.ts")
    ).resolves.toMatchObject({
      stdout: "",
      stderr: ""
    });
  });

  it("rejects representative hidden IO and native async APIs", async () => {
    await expect(runLint("--workflow-source", "test-d/determinism/invalid/**/*.ts")).rejects.toMatchObject({
      stderr: expect.stringContaining("durust/no-hidden-io")
    });

    try {
      await runLint("--workflow-source", "test-d/determinism/invalid/**/*.ts");
      throw new Error("expected determinism lint to fail");
    } catch (error) {
      const stderr = String((error as { readonly stderr?: unknown }).stderr ?? "");
      expect(stderr).toContain("durust/no-activity-api-in-workflow");
      expect(stderr).toContain("durust/no-unknown-await");
      expect(stderr).toContain("heartbeat() is activity-only");
      expect(stderr).toContain("nativePromise");
      expect(stderr).toContain("Promise.resolve");
      expect(stderr).toContain("readFromCache");
      expect(stderr).toContain("node:fs");
      expect(stderr).toContain("node:crypto.randomBytes import");
      expect(stderr).toContain("node:crypto.randomUUID import");
      expect(stderr).toContain("node:crypto namespace import");
      expect(stderr).toContain("node:http");
      expect(stderr).toContain("node:os");
      expect(stderr).toContain("node:perf_hooks");
      expect(stderr).toContain("node:timers/promises");
      expect(stderr).toContain("node:worker_threads");
      expect(stderr).toContain("Date.now()");
      expect(stderr).toContain("Date()");
      expect(stderr).toContain("new Date()");
      expect(stderr).toContain("Date.now reference");
      expect(stderr).toContain("performance.now()");
      expect(stderr).toContain("performance.timeOrigin read");
      expect(stderr).toContain("performance.now reference");
      expect(stderr).toContain("Math.random()");
      expect(stderr).toContain("Math.random reference");
      expect(stderr).toContain("crypto.randomUUID()");
      expect(stderr).toContain("crypto.randomUUID reference");
      expect(stderr).toContain("crypto.getRandomValues()");
      expect(stderr).toContain("crypto.getRandomValues reference");
      expect(stderr).toContain("AbortSignal.timeout()");
      expect(stderr).toContain("AbortSignal.timeout reference");
      expect(stderr).toContain("Atomics.wait()");
      expect(stderr).toContain("process.hrtime()");
      expect(stderr).toContain("process.hrtime.bigint()");
      expect(stderr).toContain("process.hrtime.bigint reference");
      expect(stderr).toContain("process.cwd()");
      expect(stderr).toContain("process.chdir()");
      expect(stderr).toContain("process.cpuUsage()");
      expect(stderr).toContain("process.memoryUsage()");
      expect(stderr).toContain("process.memoryUsage.rss()");
      expect(stderr).toContain("process.nextTick()");
      expect(stderr).toContain("process.resourceUsage()");
      expect(stderr).toContain("process.uptime()");
      expect(stderr).toContain("process.pid read");
      expect(stderr).toContain("process.argv read");
      expect(stderr).toContain("process.platform read");
      expect(stderr).toContain("process.exitCode read");
      expect(stderr).toContain("process.stdout read");
      expect(stderr).toContain("process.env read");
      expect(stderr).toContain("globalThis.process.env read");
      expect(stderr).toContain("console.log()");
      expect(stderr).toContain("console.log reference");
      expect(stderr).toContain("console.error()");
      expect(stderr).toContain("console.time()");
      expect(stderr).toContain("console.timeEnd()");
      expect(stderr).toContain("console.trace()");
      expect(stderr).toContain("process.exit reference");
      expect(stderr).toContain("process.stdout.write()");
      expect(stderr).toContain("process.stderr.write()");
      expect(stderr).toContain("process.stdin.read()");
      expect(stderr).toContain("process.emitWarning()");
      expect(stderr).toContain("navigator.userAgent read");
      expect(stderr).toContain("navigator.language read");
      expect(stderr).toContain("navigator.onLine read");
      expect(stderr).toContain("navigator.sendBeacon()");
      expect(stderr).toContain("navigator.clipboard.readText()");
      expect(stderr).toContain("navigator.geolocation.getCurrentPosition()");
      expect(stderr).toContain("navigator.locks.request()");
      expect(stderr).toContain("navigator.serviceWorker.register()");
      expect(stderr).toContain("document.cookie read");
      expect(stderr).toContain("document.querySelector()");
      expect(stderr).toContain("location.href read");
      expect(stderr).toContain("location.reload()");
      expect(stderr).toContain("history.pushState()");
      expect(stderr).toContain("localStorage.getItem()");
      expect(stderr).toContain("localStorage.setItem()");
      expect(stderr).toContain("sessionStorage.getItem()");
      expect(stderr).toContain("sessionStorage.setItem()");
      expect(stderr).toContain("indexedDB.open()");
      expect(stderr).toContain("caches.open()");
      expect(stderr).toContain("setTimeout()");
      expect(stderr).toContain("setImmediate()");
      expect(stderr).toContain("requestAnimationFrame()");
      expect(stderr).toContain("requestIdleCallback()");
      expect(stderr).toContain("queueMicrotask()");
      expect(stderr).toContain("Promise.race()");
      expect(stderr).toContain("Promise.all reference");
      expect(stderr).toContain("Promise.allSettled()");
      expect(stderr).toContain("fetch()");
      expect(stderr).toContain("node:fs/promises");
      expect(stderr).toContain("node:dgram");
      expect(stderr).toContain("node:http2");
      expect(stderr).toContain("node:readline/promises");
      expect(stderr).toContain("node:repl");
      expect(stderr).toContain("node:vm");
      expect(stderr).toContain("node:inspector");
      expect(stderr).toContain("node:cluster");
      expect(stderr).toContain("node:child_process");
      expect(stderr).toContain("WebSocket");
      expect(stderr).toContain("Worker");
      expect(stderr).toContain("SharedWorker");
      expect(stderr).toContain("MessageChannel");
      expect(stderr).toContain("BroadcastChannel");
      expect(stderr).toContain("eval()");
      expect(stderr).toContain("eval reference");
      expect(stderr).toContain("Function()");
      expect(stderr).toContain("Function is not allowed");
      expect(stderr).toContain("WebAssembly.compile()");
      expect(stderr).toContain("WebAssembly.instantiate()");
      expect(stderr).toContain("WebAssembly.compileStreaming()");
      expect(stderr).toContain("WebAssembly.instantiateStreaming()");
      expect(stderr).toContain("WebAssembly.validate()");
      expect(stderr).toContain("WebAssembly.Module is not allowed");
      expect(stderr).toContain("WebAssembly.Instance is not allowed");
      expect(stderr).toContain("WebAssembly.compile reference");
    }
  });

  it("rejects computed string access to forbidden static APIs", async () => {
    await expect(
      runLint("--workflow-source", "test-d/determinism/computed-invalid/**/*.ts")
    ).rejects.toMatchObject({
      stderr: expect.stringContaining("Date.now()")
    });

    try {
      await runLint("--workflow-source", "test-d/determinism/computed-invalid/**/*.ts");
      throw new Error("expected determinism lint to fail");
    } catch (error) {
      const stderr = String((error as { readonly stderr?: unknown }).stderr ?? "");
      expect(stderr).toContain("Date.now()");
      expect(stderr).toContain("Math.random()");
      expect(stderr).toContain("Promise.all reference");
      expect(stderr).toContain("process.env read");
      expect(stderr).toContain("process.hrtime.bigint reference");
    }
  });
});

async function runLint(...args: string[]): Promise<{ readonly stdout: string; readonly stderr: string }> {
  return await execFileAsync(process.execPath, [lintScript, ...args], {
    cwd: workspaceRoot
  });
}
