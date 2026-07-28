import { Console } from "node:console";
import { Writable } from "node:stream";
import { types } from "node:util";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  MemoryBackend,
  decodePayload,
  encodePayload,
  eventId,
  namespace,
  runId,
  sleepUntil,
  taskQueue,
  workflow,
  workflowId,
  workflowType,
  type ClaimedWorkflowTask,
  type PayloadRef
} from "@durust/core";
import {
  HotWorkflowExecution,
  installNondeterminismGuards,
  uninstallNondeterminismGuards
} from "../src/runtime.js";

// Every global captured here is read *before* any guard installs, because the
// module body runs at import time and only a `HotWorkflowExecution` installs
// guards. These are therefore the genuine originals, which is what the identity
// assertions below need.
const pristine = {
  Date: globalThis.Date,
  // Captured as a value, not read through `pristine.Date.now` later: the guard
  // patches `now` on the real constructor, so a live read would see the guard.
  DateNow: globalThis.Date.now,
  MathRandom: Math.random,
  performanceNow: globalThis.performance.now,
  cryptoRandomUUID: globalThis.crypto.randomUUID,
  cryptoGetRandomValues: globalThis.crypto.getRandomValues,
  processHrtime: process.hrtime,
  processHrtimeBigint: process.hrtime.bigint,
  processEnv: process.env,
  processEnvDescriptor: Object.getOwnPropertyDescriptor(process, "env"),
  processCwd: process.cwd,
  processNextTick: process.nextTick,
  processChdir: process.chdir,
  processCpuUsage: process.cpuUsage,
  processMemoryUsage: process.memoryUsage,
  processResourceUsage: process.resourceUsage,
  processUptime: process.uptime,
  setTimeout: globalThis.setTimeout,
  setInterval: globalThis.setInterval,
  setImmediate: globalThis.setImmediate,
  queueMicrotask: globalThis.queueMicrotask,
  fetch: globalThis.fetch,
  WebSocket: globalThis.WebSocket,
  PromiseAll: Promise.all,
  PromiseRace: Promise.race,
  PromiseAllSettled: Promise.allSettled,
  PromiseAny: Promise.any
} as const;

const fakeClaimed: ClaimedWorkflowTask = {
  runId: runId("run-1"),
  workflowId: workflowId("wf/guards"),
  workflowType: workflowType("guards.probe", 1),
  claim: {
    runId: runId("run-1"),
    workerId: "worker-a",
    token: 1
  },
  replayTargetEventId: eventId(1),
  reason: "WorkflowStarted",
  prefetchedHistory: [
    {
      eventId: eventId(1),
      eventType: "WorkflowStarted",
      data: {
        kind: "WorkflowStarted",
        workflowType: workflowType("guards.probe", 1),
        input: encodePayload({}, { codec: "Json" })
      }
    }
  ]
};

async function runToCommit(
  handler: () => Promise<unknown>,
  options: { readonly nondeterminismGuards?: boolean } = {}
): Promise<unknown> {
  const definition = workflow({
    name: `guards.probe-${probeCounter++}`,
    version: 1,
    handler: async (_input: {}): Promise<unknown> => await handler()
  });
  const commit = await new HotWorkflowExecution(definition, {}, fakeClaimed, {
    payloadCodec: "Json",
    ...options
  }).nextCommit();
  const completed = commit.appendEvents?.[0]?.data;
  if (completed?.kind !== "WorkflowCompleted") {
    throw new Error(`expected WorkflowCompleted, got ${String(completed?.kind)}`);
  }
  return decodePayload(completed.result);
}

let probeCounter = 0;

beforeEach(() => {
  // Guards are process-global, so each case starts from a known-clean process
  // rather than from whatever the previous case left behind.
  uninstallNondeterminismGuards();
});

afterEach(() => {
  uninstallNondeterminismGuards();
});

describe("nondeterminism guard install decision", () => {
  it("installs by default under the test harness", async () => {
    const previous = process.env.NODE_ENV;
    // Set explicitly rather than read: the case is "NODE_ENV=test installs", and
    // asserting the ambient value instead would make the suite fail when it is
    // run under any other NODE_ENV.
    process.env.NODE_ENV = "test";
    try {
      await runToCommit(async () => "ok");
      expect(globalThis.Date).not.toBe(pristine.Date);
    } finally {
      process.env.NODE_ENV = previous;
    }
  });

  it("installs by default in development", async () => {
    const previous = process.env.NODE_ENV;
    process.env.NODE_ENV = "development";
    try {
      await runToCommit(async () => "ok");
      expect(globalThis.Date).not.toBe(pristine.Date);
    } finally {
      process.env.NODE_ENV = previous;
    }
  });

  it("does not install by default in production", async () => {
    const previous = process.env.NODE_ENV;
    process.env.NODE_ENV = "production";
    try {
      const result = await runToCommit(async () => Date.now());
      expect(typeof result).toBe("number");
      // Not merely "the workflow succeeded": no global was replaced at all.
      expect(globalThis.Date).toBe(pristine.Date);
      expect(Math.random).toBe(pristine.MathRandom);
      expect(globalThis.setTimeout).toBe(pristine.setTimeout);
      expect(Object.getOwnPropertyDescriptor(process, "env")?.get).toBeUndefined();
    } finally {
      process.env.NODE_ENV = previous;
    }
  });

  it("honours an explicit true in production", async () => {
    const previous = process.env.NODE_ENV;
    process.env.NODE_ENV = "production";
    try {
      await expect(
        runToCommit(async () => Date.now(), { nondeterminismGuards: true })
      ).rejects.toThrow("nondeterminism: Date.now() is not allowed inside workflow code");
      expect(globalThis.Date).not.toBe(pristine.Date);
    } finally {
      process.env.NODE_ENV = previous;
    }
  });

  it("honours an explicit false under the test harness", async () => {
    const result = await runToCommit(async () => Date.now(), { nondeterminismGuards: false });
    expect(typeof result).toBe("number");
    expect(globalThis.Date).toBe(pristine.Date);
    expect(Math.random).toBe(pristine.MathRandom);
  });

  it("re-reads NODE_ENV per execution rather than latching it at module load", async () => {
    const previous = process.env.NODE_ENV;
    process.env.NODE_ENV = "production";
    try {
      await runToCommit(async () => "ok");
      expect(globalThis.Date).toBe(pristine.Date);
      process.env.NODE_ENV = "development";
      await runToCommit(async () => "ok");
      expect(globalThis.Date).not.toBe(pristine.Date);
    } finally {
      process.env.NODE_ENV = previous;
    }
  });
});

describe("nondeterminism guards remain effective when enabled", () => {
  it("rejects Date.now() in workflow code", async () => {
    await expect(
      runToCommit(async () => Date.now(), { nondeterminismGuards: true })
    ).rejects.toThrow("nondeterminism: Date.now() is not allowed inside workflow code");
  });

  it("rejects Math.random() in workflow code", async () => {
    await expect(
      runToCommit(async () => Math.random(), { nondeterminismGuards: true })
    ).rejects.toThrow("nondeterminism: Math.random() is not allowed inside workflow code");
  });

  it("rejects process.uptime() in workflow code", async () => {
    // Kept against row 5B's drop list: a monotonic clock, not introspection.
    await expect(
      runToCommit(async () => process.uptime(), { nondeterminismGuards: true })
    ).rejects.toThrow("nondeterminism: process.uptime() is not allowed inside workflow code");
  });

  // Row 5C's runtime half: `process.env` is not patched at all. Neither a Proxy
  // nor an accessor — the property is left exactly as Node defined it.
  it("never patches process.env, in any form", async () => {
    await runToCommit(async () => "ok", { nondeterminismGuards: true });

    const descriptor = Object.getOwnPropertyDescriptor(process, "env");
    expect(descriptor?.get).toBeUndefined();
    expect(descriptor?.set).toBeUndefined();
    expect(descriptor?.value).toBe(pristine.processEnv);
    expect(descriptor?.writable).toBe(pristine.processEnvDescriptor?.writable);
    expect(descriptor?.enumerable).toBe(pristine.processEnvDescriptor?.enumerable);
    expect(descriptor?.configurable).toBe(pristine.processEnvDescriptor?.configurable);
    expect(process.env).toBe(pristine.processEnv);
    expect(types.isProxy(process.env)).toBe(false);
  });

  it("lets workflow code read and write process.env without tripping a guard", async () => {
    const previous = process.env.DURUST_GUARD_PROBE;
    try {
      const value = await runToCommit(async () => {
        process.env.DURUST_GUARD_PROBE = "written-by-workflow";
        return `${process.env.DURUST_GUARD_PROBE}:${Object.keys(process.env).length > 0}`;
      }, { nondeterminismGuards: true });
      expect(value).toBe("written-by-workflow:true");
    } finally {
      if (previous === undefined) {
        delete process.env.DURUST_GUARD_PROBE;
      } else {
        process.env.DURUST_GUARD_PROBE = previous;
      }
    }
  });

  // The concrete false positive that decided row 5C, reproduced through its
  // actual trigger. It is not `util.inspect` — that reads nothing. It is
  // `Console`'s colour-mode detection, which runs `getColorDepth()` and reads
  // `NO_COLOR`/`FORCE_COLOR`/`TERM` whenever an argument is not already a
  // string. Measured: `console.log("s")` costs 0 environment reads,
  // `console.log({...})` costs 2, `console.error(err)` costs 1.
  //
  // Under the accessor guard this threw `nondeterminism: process.env ...` from
  // a line the author wrote as `console.log`, naming an API they never touched.
  it("lets workflow code console.log objects and errors", async () => {
    // Node's own `Console`, not `globalThis.console`: vitest replaces the global
    // one with an interceptor that does no colour detection, so asserting
    // through it would pass even with the guard reinstated. A fresh `Console`
    // over a sink runs the real code path a production host runs.
    const sink = new Writable({
      write(_chunk, _encoding, callback) {
        callback();
      }
    });
    const nodeConsole = new Console({ stdout: sink, stderr: sink });

    const value = await runToCommit(async () => {
      nodeConsole.log({ a: [1, 2, 3] });
      nodeConsole.error(new Error("boom"));
      return "logged";
    }, { nondeterminismGuards: true });
    expect(value).toBe("logged");
  });

  it("rejects setTimeout and Promise.race in workflow code", async () => {
    await expect(
      runToCommit(async () => {
        setTimeout(() => undefined, 0);
        return "unreachable";
      }, { nondeterminismGuards: true })
    ).rejects.toThrow("nondeterminism: setTimeout() is not allowed inside workflow code");
    await expect(
      runToCommit(async () => await Promise.race([Promise.resolve(1)]), {
        nondeterminismGuards: true
      })
    ).rejects.toThrow("nondeterminism: Promise.race() is not allowed inside workflow code");
  });
});

describe("nondeterminism guard uninstall", () => {
  // One assertion per patched global, deliberately not a loop over a table: a
  // table that silently loses a row still passes.
  it("restores the identity of every patched global", async () => {
    await runToCommit(async () => "ok", { nondeterminismGuards: true });

    expect(globalThis.Date).not.toBe(pristine.Date);
    expect(uninstallNondeterminismGuards()).toBe(true);

    expect(globalThis.Date).toBe(pristine.Date);
    expect(globalThis.Date.now).toBe(pristine.DateNow);
    expect(Math.random).toBe(pristine.MathRandom);
    expect(globalThis.performance.now).toBe(pristine.performanceNow);
    expect(globalThis.crypto.randomUUID).toBe(pristine.cryptoRandomUUID);
    expect(globalThis.crypto.getRandomValues).toBe(pristine.cryptoGetRandomValues);
    expect(process.hrtime).toBe(pristine.processHrtime);
    expect(process.hrtime.bigint).toBe(pristine.processHrtimeBigint);
    expect(process.cwd).toBe(pristine.processCwd);
    expect(process.uptime).toBe(pristine.processUptime);
    expect(process.nextTick).toBe(pristine.processNextTick);
    expect(globalThis.setTimeout).toBe(pristine.setTimeout);
    expect(globalThis.setInterval).toBe(pristine.setInterval);
    expect(globalThis.setImmediate).toBe(pristine.setImmediate);
    expect(globalThis.queueMicrotask).toBe(pristine.queueMicrotask);
    expect(globalThis.fetch).toBe(pristine.fetch);
    expect(globalThis.WebSocket).toBe(pristine.WebSocket);
    expect(Promise.all).toBe(pristine.PromiseAll);
    expect(Promise.race).toBe(pristine.PromiseRace);
    expect(Promise.allSettled).toBe(pristine.PromiseAllSettled);
    expect(Promise.any).toBe(pristine.PromiseAny);

    // Row 5B removed these four from the guarded set, and row 5C removed
    // `process.env`; uninstall must leave them exactly as they were rather than
    // restore a patch that was never applied.
    expect(process.chdir).toBe(pristine.processChdir);
    expect(process.cpuUsage).toBe(pristine.processCpuUsage);
    expect(process.memoryUsage).toBe(pristine.processMemoryUsage);
    expect(process.resourceUsage).toBe(pristine.processResourceUsage);
    expect(process.env).toBe(pristine.processEnv);
  });

  // The per-global assertions above are the contract; this is a *bounded* safety
  // net, and the bound matters. It enumerates the objects listed below and only
  // those, so it catches an unledgered guard on any property of a host the guard
  // set already touches — the realistic mistake, since a new guard almost always
  // lands next to an existing one. `Intl.DateTimeFormat` and
  // `Date.prototype.toLocaleString` are covered, because `Intl` and
  // `Date.prototype` are listed.
  //
  // It does NOT catch a guard on a property of an object absent from the list:
  // `Reflect.ownKeys`, `JSON.stringify`, or `Temporal.Now.instant` would slip
  // past both this net and the explicit assertions above. Note that listing
  // `globalThis` is not enough for those — it pins the identity of the
  // `Reflect` and `JSON` bindings themselves, not of the properties hanging off
  // them. Enumerating every reachable object is not possible; adding a host here
  // is the price of guarding something new on it.
  it("leaves no own property of any listed host object changed", () => {
    const hosts: readonly [string, object][] = [
      ["globalThis", globalThis],
      ["process", process],
      ["Math", Math],
      ["Promise", Promise],
      ["performance", globalThis.performance],
      ["crypto", globalThis.crypto],
      ["console", console],
      ["Intl", Intl],
      ["Date", pristine.Date],
      ["Date.prototype", pristine.Date.prototype],
      ["process.env", pristine.processEnv],
      ["process.hrtime", pristine.processHrtime],
      ["process.memoryUsage", pristine.processMemoryUsage]
    ];
    const snapshot = () => {
      const entries: string[] = [];
      for (const [label, host] of hosts) {
        for (const key of Reflect.ownKeys(host)) {
          const descriptor = Object.getOwnPropertyDescriptor(host, key);
          if (descriptor === undefined) {
            continue;
          }
          const shape =
            "value" in descriptor
              ? typeof descriptor.value === "object" || typeof descriptor.value === "function"
                ? "ref"
                : "primitive"
              : "accessor";
          entries.push(`${label}.${String(key)}:${shape}`);
        }
      }
      return entries.sort();
    };
    const identities = () => {
      const seen = new Map<string, unknown>();
      for (const [label, host] of hosts) {
        for (const key of Reflect.ownKeys(host)) {
          const descriptor = Object.getOwnPropertyDescriptor(host, key);
          if (descriptor === undefined || !("value" in descriptor)) {
            continue;
          }
          seen.set(`${label}.${String(key)}`, descriptor.value);
        }
      }
      return seen;
    };

    const shapeBefore = snapshot();
    const identityBefore = identities();

    installNondeterminismGuards();
    // Sanity: the snapshot really does move, so a green result after uninstall
    // is not a vacuous comparison of two identical no-ops.
    expect(identities().get("globalThis.Date")).not.toBe(identityBefore.get("globalThis.Date"));

    uninstallNondeterminismGuards();

    expect(snapshot()).toEqual(shapeBefore);
    const identityAfter = identities();
    const changed = [...identityBefore.entries()]
      // Object.is, not !==: `globalThis.NaN` is never `===` itself.
      .filter(([key, value]) => !Object.is(identityAfter.get(key), value))
      .map(([key]) => key);
    expect(changed).toEqual([]);
  });

  // Regression for a defect that silently and permanently disabled the guards.
  // An earlier revision guarded `process.env` with an accessor whose setter
  // reassigned this module's captured environment reference *outside* the
  // restore ledger, and `isProductionHost()` read that capture. So a host doing
  // `process.env = {...}` while the guards were up — test harnesses and config
  // bootstraps do exactly this — left the capture pointing at an object that
  // uninstall then discarded. Every later execution read a stale `NODE_ENV` from
  // it, decided "production", and never installed guards again for the life of
  // the process. `process.env` is no longer patched at all, so there is no
  // capture left to go stale.
  it("keeps the install decision correct after process.env is replaced wholesale", async () => {
    const realEnv = process.env;
    const previousNodeEnv = process.env.NODE_ENV;
    try {
      process.env.NODE_ENV = "development";
      installNondeterminismGuards();

      // The exact sequence: replace the whole environment while the guards are
      // up. Because `process.env` is not patched, this replaces the real
      // property, and durust must simply follow it.
      process.env = { ...realEnv, NODE_ENV: "production" };
      expect(process.env).not.toBe(realEnv);
      expect(uninstallNondeterminismGuards()).toBe(true);

      // Uninstall must not resurrect the discarded object, and must not have
      // captured it either: the decision follows whatever `process.env` says
      // right now.
      await runToCommit(async () => "ok");
      expect(globalThis.Date).toBe(pristine.Date);

      // Put the real environment back, still saying development, and the
      // decision must flip with it. The defect this guards against latched the
      // discarded replacement here and answered "production" forever.
      process.env = realEnv;
      expect(process.env.NODE_ENV).toBe("development");
      await runToCommit(async () => "ok");
      expect(globalThis.Date).not.toBe(pristine.Date);
    } finally {
      process.env = realEnv;
      if (previousNodeEnv === undefined) {
        delete process.env.NODE_ENV;
      } else {
        process.env.NODE_ENV = previousNodeEnv;
      }
    }
  });

  it("reports whether it restored anything", () => {
    expect(uninstallNondeterminismGuards()).toBe(false);
    installNondeterminismGuards();
    expect(uninstallNondeterminismGuards()).toBe(true);
    expect(uninstallNondeterminismGuards()).toBe(false);
  });

  it("survives repeat installs without capturing a guard as its own original", () => {
    installNondeterminismGuards();
    const firstGuardedDate = globalThis.Date;
    installNondeterminismGuards();
    installNondeterminismGuards();
    // Repeat install must not re-patch anything but `process.nextTick`, or the
    // second install would record the first guard as the "original".
    expect(globalThis.Date).toBe(firstGuardedDate);

    // A single uninstall undoes any number of installs: the ledger is not
    // reference counted.
    expect(uninstallNondeterminismGuards()).toBe(true);
    expect(globalThis.Date).toBe(pristine.Date);
    expect(process.env).toBe(pristine.processEnv);
    expect(Promise.race).toBe(pristine.PromiseRace);
  });

  it("reasserts the process.nextTick guard only when something replaced it", async () => {
    installNondeterminismGuards();
    const firstGuard = process.nextTick;
    expect(firstGuard).not.toBe(pristine.processNextTick);

    // Every execution construction reaches the reassert path. Nothing replaced
    // the guard, so it must be left alone rather than reallocated and
    // redefined per task.
    await runToCommit(async () => "ok", { nondeterminismGuards: true });
    await runToCommit(async () => "ok", { nondeterminismGuards: true });
    expect(process.nextTick).toBe(firstGuard);

    // Instrumentation that wraps `process.nextTick` after the guards went up
    // gets the guard put back on top of it on the next execution.
    const foreign = ((callback: (...args: any[]) => void, ...args: any[]) =>
      firstGuard(callback, ...args)) as typeof process.nextTick;
    Object.defineProperty(process, "nextTick", {
      configurable: true,
      writable: true,
      value: foreign
    });
    await runToCommit(async () => "ok", { nondeterminismGuards: true });
    expect(process.nextTick).not.toBe(foreign);
    await expect(
      runToCommit(async () => {
        process.nextTick(() => undefined);
        return "unreachable";
      }, { nondeterminismGuards: true })
    ).rejects.toThrow("nondeterminism: process.nextTick() is not allowed inside workflow code");

    // Uninstall still restores the true original, not the foreign wrapper the
    // reassert displaced.
    uninstallNondeterminismGuards();
    expect(process.nextTick).toBe(pristine.processNextTick);
  });

  it("re-patches cleanly after an uninstall", async () => {
    installNondeterminismGuards();
    uninstallNondeterminismGuards();
    installNondeterminismGuards();

    expect(globalThis.Date).not.toBe(pristine.Date);
    await expect(
      runToCommit(async () => Date.now(), { nondeterminismGuards: true })
    ).rejects.toThrow("nondeterminism: Date.now() is not allowed inside workflow code");

    uninstallNondeterminismGuards();
    expect(globalThis.Date).toBe(pristine.Date);
    expect(globalThis.setTimeout).toBe(pristine.setTimeout);
  });

  it("keeps orphaned guard wrappers working after uninstall", () => {
    installNondeterminismGuards();
    // A host that captured the guard while it was installed — a plausible shape
    // for any library that snapshots globals at load time.
    const orphanedDateNow = globalThis.Date.now;
    const orphanedSetTimeout = globalThis.setTimeout;
    expect(orphanedDateNow).not.toBe(pristine.DateNow);

    uninstallNondeterminismGuards();

    // Uninstall deliberately does not clear the captured originals, so an
    // orphan still forwards to the real built-in instead of returning a
    // placeholder.
    expect(orphanedDateNow()).toBeGreaterThan(0);
    const handle = orphanedSetTimeout(() => undefined, 0);
    clearTimeout(handle as never);
  });

  // Mid-flight uninstall contract: safe, non-corrupting, and enforcement-only.
  it("lets an in-flight execution finish after a mid-flight uninstall", async () => {
    const trace: string[] = [];
    const reminder = workflow({
      name: "guards.mid-flight-uninstall",
      version: 1,
      handler: async (input: { readonly deadlineMs: number }): Promise<number> => {
        trace.push("before-timer");
        await sleepUntil(input.deadlineMs);
        trace.push("after-timer");
        // Rejected before the uninstall, permitted after it. That degradation is
        // the documented cost of uninstalling while tasks are in flight.
        return Date.now();
      }
    });
    const backend = new MemoryBackend();
    await backend.startWorkflow({
      namespace: namespace(),
      workflowId: workflowId("wf/mid-flight-uninstall"),
      workflowType: reminder.workflowType,
      taskQueue: taskQueue("workflows"),
      input: encodePayload({ deadlineMs: 1_000 }, { codec: "Json" })
    });
    const firstClaim = await backend.claimWorkflowTask("worker-a", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [reminder.workflowType],
      leaseDurationMs: 30_000
    });
    if (!firstClaim) {
      throw new Error("expected first claim");
    }
    const hot = new HotWorkflowExecution(reminder, { deadlineMs: 1_000 }, firstClaim, {
      payloadCodec: "Json",
      nondeterminismGuards: true
    });
    const timerCommit = await hot.nextCommit();
    expect(timerCommit.appendEvents?.map((event) => event.data.kind)).toEqual(["TimerStarted"]);
    const firstOutcome = await backend.commitWorkflowTask(firstClaim.claim, timerCommit);
    if (firstOutcome.kind !== "Committed") {
      throw new Error("expected first commit to succeed");
    }
    hot.markCommitted(firstOutcome.newTailEventId);

    // The workflow is parked on a durable timer and its guards come down now.
    expect(uninstallNondeterminismGuards()).toBe(true);
    expect(globalThis.Date).toBe(pristine.Date);

    await backend.fireDueTimers({ namespace: namespace(), now: 1_000, limit: 16 });
    const secondClaim = await backend.claimWorkflowTask("worker-b", {
      namespace: namespace(),
      taskQueue: taskQueue("workflows"),
      registeredWorkflowTypes: [reminder.workflowType],
      leaseDurationMs: 30_000
    });
    if (!secondClaim) {
      throw new Error("expected second claim");
    }
    const completionCommit = await hot.advance(secondClaim);

    // The run neither hangs nor fails: durable APIs and AsyncLocalStorage are
    // untouched by uninstall, so only detection is lost.
    expect(trace).toEqual(["before-timer", "after-timer"]);
    const completed = completionCommit.appendEvents?.[0]?.data;
    if (completed?.kind !== "WorkflowCompleted") {
      throw new Error("expected WorkflowCompleted");
    }
    expect(decodePayload(completed.result as PayloadRef<number>)).toBeGreaterThan(0);
  });
});

describe("nondeterminism guard blind spots", () => {
  // Pins today's load-order behaviour rather than asserting it is desirable.
  // The guards patch globals when the first execution is constructed, so any
  // module that captured a global at import time — which is most instrumentation
  // and many utility libraries — holds an unguarded reference forever. Uninstall
  // (row 5D) is what makes this bounded rather than permanent, but it does not
  // fix it: only static lint reaches a workflow that calls a pre-captured alias.
  it("does not guard globals captured before the first install", async () => {
    // Captured while the guards are down, exactly like a module loaded before
    // the first workflow ran.
    const preCapturedDateNow = globalThis.Date.now;
    const preCapturedSetTimeout = globalThis.setTimeout;
    const preCapturedFetch = globalThis.fetch;
    expect(preCapturedDateNow).toBe(pristine.DateNow);

    const results = (await runToCommit(async () => {
      const now = preCapturedDateNow();
      const handle = preCapturedSetTimeout(() => undefined, 0);
      clearTimeout(handle as never);
      return { now, hasFetch: typeof preCapturedFetch === "function" };
    }, { nondeterminismGuards: true })) as {
      readonly now: number;
      readonly hasFetch: boolean;
    };

    expect(results.now).toBeGreaterThan(0);
    expect(results.hasFetch).toBe(true);
    // The guards are up; the workflow simply never touched them.
    expect(globalThis.Date).not.toBe(pristine.Date);
    await expect(
      runToCommit(async () => Date.now(), { nondeterminismGuards: true })
    ).rejects.toThrow("nondeterminism: Date.now() is not allowed inside workflow code");
  });

  // AsyncLocalStorage is dynamically scoped by async resource, not lexically
  // captured by closures, so which of these two trips is not obvious. Both
  // behaviours are pinned because the second one is a real defect: the throw
  // lands in host code, after the task committed, attributed to workflow code
  // that is no longer running.
  it("does not trip on a workflow-created callback invoked from a host context", async () => {
    const escaped: (() => number)[] = [];
    // Positive control: without this, the test passes just as happily with the
    // guards deleted entirely, and asserts nothing about escaping. It proves the
    // guard is installed and does reject this exact call when the workflow store
    // really is on the stack.
    await expect(
      runToCommit(async () => {
        escaped.push(() => Date.now());
        return Date.now();
      }, { nondeterminismGuards: true })
    ).rejects.toThrow("nondeterminism: Date.now() is not allowed inside workflow code");
    expect(globalThis.Date).not.toBe(pristine.Date);

    expect(escaped).toHaveLength(1);
    // Same closure, same installed guard, invoked from the test's own context
    // with no workflow store on the stack: not rejected.
    expect(escaped[0]!()).toBeGreaterThan(0);
  });

  it("does trip on a continuation registered inside workflow code and resolved later by the host", async () => {
    let releaseHostGate: (() => void) | undefined;
    const hostGate = new Promise<void>((resolve) => {
      releaseHostGate = resolve;
    });
    let hostContinuation: Promise<number> | undefined;

    await runToCommit(async () => {
      // The workflow attaches to a host-owned promise. The `.then` callback is
      // scheduled inside the workflow's AsyncLocalStorage context and inherits
      // it, so it still sees the workflow store when the host resolves the gate
      // after the task has committed.
      hostContinuation = hostGate.then(() => Date.now());
      return "ok";
    }, { nondeterminismGuards: true });

    releaseHostGate?.();
    await expect(hostContinuation).rejects.toThrow(
      "nondeterminism: Date.now() is not allowed inside workflow code"
    );
  });
});
