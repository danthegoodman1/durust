import {
  Worker,
  WorkflowCancelledError,
  WorkflowFailureError,
  namespace,
  type DurableBackend,
  type Registry,
  type TaskQueue,
  type WorkerOptions,
  type WorkerRunOptions,
  type WorkerRunOutcome
} from "@durust/core";

/**
 * Test support for `packages/core/test` only — not part of any published
 * package, and deliberately not in `@durust/testing`.
 *
 * `@durust/testing` exists so that someone implementing a `DurableBackend`
 * outside this repository can run the conformance suite against it; the claim
 * and history fixtures there are useful to that reader. This file is the other
 * kind of helper: it encodes *this workspace's* worker-construction conventions
 * — the `workflows` queue, the `Json` codec — which are test scaffolding and not
 * something a backend author would want to inherit as public API.
 */

/**
 * Everything `new Worker` takes except the two arguments this fixture passes
 * positionally. `workerId` stays required: a fixture that invented one would
 * make every "two workers race for the same task" case pass against a single
 * identity.
 */
export type WorkerFixtureOptions = Omit<
  WorkerOptions,
  "backend" | "registry" | "workflowTaskQueue"
> & {
  readonly workflowTaskQueue?: TaskQueue | string;
};

/**
 * A `Worker` on this workspace's test defaults: the default namespace and the
 * `workflows` task queue. Those two are the only options where "unset" and the
 * default mean the same thing to `Worker` — `namespace` already falls back to
 * `default` inside it, and `workflowTaskQueue` is required, so no site can have
 * been relying on its absence.
 *
 * The spread order is load-bearing. `...options` comes *after* the defaults, so
 * deleting it does not quietly build a default worker and let 90 tests carry on
 * against the wrong configuration — it removes `workerId`, which `WorkerOptions`
 * requires, and the file stops compiling. Deleting `backend` or `registry` fails
 * the same way. There is no edit to this body that leaves it type-correct and
 * behaviourally empty.
 *
 * `activityTaskQueue` and `payloadCodec` are deliberately **not** defaulted, even
 * though nearly every caller passes the same value for both. Undefined is a
 * meaningful state for each: `activityTaskQueue` selects the `default` queue
 * fallback documented on `WorkerOptions`, and `payloadCodec` selects
 * `MessagePack`, not the `Json` the tests here mostly use. Defaulting the codec
 * was tried and reverted — `behavioral-corpus.test.ts` builds its worker without
 * one on purpose, and the fixture silently re-encoded its payloads, failing 13
 * of its cases. A default is only safe where absence carries no meaning.
 */
export function workerFixture(
  backend: DurableBackend,
  registry: Registry,
  options: WorkerFixtureOptions
): Worker {
  return new Worker({
    namespace: namespace(),
    workflowTaskQueue: "workflows",
    ...options,
    backend,
    registry
  });
}

/** The subset of a workflow handle this file needs: a readable durable outcome. */
interface SettleableHandle {
  result(): PromiseLike<unknown>;
}

/**
 * Run `worker` until `handle` has a durable outcome, then stop it and return
 * the run's stats.
 *
 * `run({ maxIterations: n })` cannot express "until the work is done", and
 * reading it that way is what made these tests flaky. The workflow and activity
 * loops run concurrently, and `#runTaskLoop` counts *every* pass against the
 * budget — an idle one that claimed nothing exactly like a productive one. So
 * `n` is a wager that the workflow loop will not spend its budget idling while
 * it waits for the activity loop, and `idleBackoffMs: 0` makes the idle passes
 * as cheap as possible to burn. Under CI load that wager loses: `records
 * structured events and cumulative worker metrics` failed on `main` with
 * "workflow result is not available" at `maxIterations: 8`, while the same
 * commit passed on a pull request and through 15 consecutive local runs.
 *
 * Waiting for the outcome removes the race rather than widening the window, so
 * `maxIterations` survives here only as a stop for a workflow that never
 * finishes at all.
 *
 * "Settled" is decided by type, not by message: a durable failure or
 * cancellation throws `WorkflowFailureError` or `WorkflowCancelledError`, and
 * every other rejection — `result()` throws a plain `Error` while no terminal
 * event is in history — means the run is still going.
 */
export async function runWorkerUntilSettled(
  worker: Worker,
  handle: SettleableHandle,
  options: Omit<WorkerRunOptions, "signal"> = {}
): Promise<WorkerRunOutcome> {
  const stop = new AbortController();
  const run = worker.run({
    maxIterations: 10_000,
    idleBackoffMs: 0,
    errorBackoffMs: 0,
    ...options,
    signal: stop.signal
  });
  try {
    while (!(await hasDurableOutcome(handle))) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  } finally {
    stop.abort();
  }
  return await run;
}

async function hasDurableOutcome(handle: SettleableHandle): Promise<boolean> {
  try {
    await handle.result();
    return true;
  } catch (error) {
    return error instanceof WorkflowFailureError || error instanceof WorkflowCancelledError;
  }
}
