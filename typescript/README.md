# Durust TypeScript

This workspace contains the TypeScript-native Durust runtime. It is a native
Node implementation of the Rust durable execution model, not an FFI wrapper.

Status: in progress. The API, providers, conformance tests, payload offload,
determinism lint, benchmark runner, and release gates are implemented in useful
slices, but the TypeScript runtime is not production-ready until the remaining
items in `../impl-plan/0014-typescript-parity.md` are complete.

## Runtime Floor

- Node.js: `>=24.0.0`
- Package manager: npm `>=11.0.0`
- Test framework: Vitest

The SQLite provider uses Node's built-in `node:sqlite` `DatabaseSync`, so the
workspace uses a conservative Node 24+ floor across packages.

## Installation

Install the Durust runtime core and the durability provider your service uses:

```bash
npm install @durust/core @durust/sqlite
```

For Postgres-backed services:

```bash
npm install @durust/core @durust/postgres
```

Add payload offload when workflow inputs, activity results, signals, query
projections, or map manifests can grow beyond the provider's inline payload
budget:

```bash
npm install @durust/payload
```

Provider authors and advanced backend test suites can install the shared
conformance package:

```bash
npm install --save-dev @durust/testing
```

Workflow packages should also enable the determinism lint:

```bash
npm install --save-dev @durust/eslint-plugin
```

The packages are intentionally split by deployment feature. `@durust/core`
contains the workflow API, worker, client, memory backend, replay runtime, and
shared backend contract. Provider packages add a concrete durability store:
`@durust/sqlite` for local single-file development and tests, or
`@durust/postgres` for the normalized SQL backend. `@durust/payload` composes
with any provider when large values need blob-backed storage. `@durust/testing`
is published for backend conformance, and `@durust/eslint-plugin` is published
for consumer workflow determinism checks. Examples and benchmark tooling remain
workspace-private.

## Packages

- `@durust/core`: public API, worker, client, history types, memory provider,
  replay/runtime machinery, and shared backend contract.
- `@durust/sqlite`: SQLite provider for local development and tests.
- `@durust/postgres`: normalized Postgres provider with append history,
  workflow/activity leases, ready indexes, waits, signals, query projections,
  activity maps, child-workflow maps, payload roots, and provider stats stored
  in SQL tables behind the shared contract.
- `@durust/payload`: payload backend wrapper plus local-directory and
  S3-compatible blob stores.
- `@durust/testing`: shared provider conformance cases.
- `@durust/eslint-plugin`: deterministic workflow lint rule.
- `@durust/benchmark`: benchmark workload runner and threshold comparison.
- `@durust/examples`: compile-checked checkout, approval, fanout,
  versioning, control-flow, retry, heartbeat, payload-offload, and
  parent-close-policy examples.

## Durable API Shape

Durust TypeScript uses explicit function definitions rather than macros or
required decorators. Workflows, activities, child workflow starts, activity
calls, and signal payloads use one durable request object.

```ts
import {
  activity,
  callActivity,
  childWorkflow,
  heartbeat,
  workflow
} from "@durust/core";

interface CheckoutInput {
  readonly orderId: string;
  readonly sku: string;
  readonly quantity: number;
}

interface CheckoutOutput {
  readonly orderId: string;
  readonly paymentId: string;
  readonly shipmentId: string;
}

interface QuoteInput {
  readonly sku: string;
  readonly quantity: number;
}

interface QuoteOutput {
  readonly amountCents: number;
}

interface ShipInput {
  readonly orderId: string;
}

interface ShipOutput {
  readonly shipmentId: string;
}

const priceQuote = activity({
  name: "payments.price-quote",
  handler: async (input: QuoteInput): Promise<QuoteOutput> => ({
    amountCents: input.sku.length * input.quantity * 100
  })
});

const transcode = activity({
  name: "media.transcode",
  handler: async (input: { readonly assetId: string }): Promise<{ readonly assetId: string }> => {
    await heartbeat();
    return { assetId: input.assetId };
  }
});

const shipOrder = workflow({
  name: "orders.ship",
  version: 1,
  handler: async (input: ShipInput): Promise<ShipOutput> => ({
    shipmentId: `shipment/${input.orderId}`
  })
});

const checkout = workflow({
  name: "orders.checkout",
  version: 1,
  handler: async (input: CheckoutInput): Promise<CheckoutOutput> => {
    const quote = await callActivity(priceQuote, {
      sku: input.sku,
      quantity: input.quantity
    });

    const shipment = await childWorkflow(
      shipOrder,
      { orderId: input.orderId },
      { workflowId: `ship/${input.orderId}`, taskQueue: "shipping" }
    ).spawn();

    const shipped = await shipment.result();
    return {
      orderId: input.orderId,
      paymentId: `payment/${quote.amountCents}`,
      shipmentId: shipped.shipmentId
    };
  }
});
```

No-input handlers still use a named empty object shape:

```ts
type NoInput = {};

const maintenance = workflow({
  name: "maintenance.compact",
  version: 1,
  handler: async (input: NoInput): Promise<void> => {
    void input;
  }
});
```

Do not use primitive inputs, tuple inputs, arrays as the root input, `null`,
`undefined`, `void`, or positional argument lists. The runtime enforces one
durable request object; compatibility of that object is an application/schema
contract. Use additive optional fields, stable serialized names, durable name
versioning, or workflow version markers for breaking schema changes.

## Worker And Client

```ts
import { Client, MemoryBackend, Registry, Worker } from "@durust/core";

const backend = new MemoryBackend();
const registry = new Registry()
  .registerWorkflow(checkout)
  .registerWorkflow(shipOrder)
  .registerActivity(priceQuote);

const worker = new Worker({
  backend,
  registry,
  workerId: "worker-1",
  workflowTaskQueue: "workflows",
  activityTaskQueue: "activities",
  payloadCodec: "Json"
});

const client = new Client(backend, { payloadCodec: "Json" });
const handle = await client.startWorkflow(
  checkout,
  "checkout/order-1",
  "workflows",
  { orderId: "order-1", sku: "sku-1", quantity: 2 }
);

for (;;) {
  const workflowTask = await worker.runWorkflowTaskOnce();
  const activityTask = await worker.runActivityTaskOnce();
  if (workflowTask.kind === "NoTask" && activityTask.kind === "NoTask") {
    break;
  }
}

const result = await handle.result();
```

Production workers should run the worker loop and provider-specific maintenance
continuously. The examples package keeps the loop explicit so tests remain
deterministic and small.

## Durable Manifest

Export a registry from a module in the package that owns your durable handlers:

```ts
import { Registry } from "@durust/core";

export const registry = new Registry()
  .registerWorkflow(checkout)
  .registerWorkflow(shipOrder)
  .registerActivity(priceQuote);
```

After building that package, write or check the reviewed manifest baseline:

```bash
durust-manifest write --module ./dist/workflows.js --out durable.manifest.json
durust-manifest check --module ./dist/workflows.js --manifest durable.manifest.json
durust-manifest diff --module ./dist/workflows.js --manifest durable.manifest.json
```

The module may export a `Registry`, a manifest object, or a function returning
either. `accept` is an alias for `write`. The manifest command is an explicit CI
guardrail: normal TypeScript compilation does not fail just because workflow or
activity inventory changed.

## Determinism

Workflow code must not use wall-clock reads, randomness, hidden file or network
I/O, native timers, native promise combinators, worker threads, child processes,
or browser network constructors. The lint gate treats computed string access
such as `Date["now"]`, `Promise["all"]`, and `process["env"]` the same as dot
access. Use Durust APIs instead:

- `sleep` and `sleepUntil` for durable time.
- `signal` and `select` or `selectAll` for deterministic waits.
- `callActivity` for side effects and external I/O.
- `childWorkflow` for durable child orchestration.
- `activityMap` and `childWorkflowMap` for bounded manifest-backed fanout.
- `getVersion`, `patched`, and `deprecatePatch` for deterministic rollout
  branches.
- `sideEffect` for recorded deterministic values.

Signals can optionally carry a schema adapter with `signal<T>("name",
{ schema })`. `Client.sendSignal` encodes through that adapter and workflow
signal awaits decode through it, while still requiring an object-shaped payload.
Workflow query projections declared with `queryStateSchema` are encoded through
that schema when workflow code calls `publish(...)` and decoded through the same
schema when clients query the workflow.

Long-running activity handlers can call `heartbeat()` when the scheduled
activity has `heartbeatTimeoutMs` configured. The provider refreshes the
claim-fenced heartbeat deadline; missed heartbeats are handled by the same
activity timeout maintenance path and use the stored retry policy before a
terminal `ActivityTimedOut` wakes the workflow.

Workflow-source lint rejects activity-only APIs such as `heartbeat()` when they
are imported or called from workflow code.

Run the determinism gate with:

```bash
npm run lint:determinism
```

Nondeterminism is a hard failure. The runtime does not restart workflows on
determinism failures as a compatibility mechanism.

### Two Gates, And Which One Runs In Production

Determinism is enforced twice, and the two gates do not cover the same ground.

**Static lint — `@durust/eslint-plugin`.** Always on, costs nothing at runtime,
names the offending API precisely. **This is the enforcement path in
production.** Point it at every file that workflow code lives in:

```json
{
  "durust": {
    "workflowSources": ["src/workflows/**/*.ts"]
  }
}
```

Its limit is the one that matters when you scope it: **it checks only the files
the globs match, and it does not follow calls into other modules.** A workflow
that calls a helper in an unlisted file gets no coverage for anything that
helper does — including a bare `Date.now()`:

```ts
// src/workflows/order.ts   (listed in workflowSources)
import { stamp } from "../util/clock.js";
export const order = workflow({ /* ... */ handler: async () => stamp() });

// src/util/clock.ts        (NOT listed -> never linted)
export const stamp = (): number => Date.now();   // reported by nothing
```

The second limit pulls against the first: the lint is **file-granular and
assumes every file it is pointed at is workflow code end to end.** Driver code
sharing a file with a workflow definition is reported as if it were inside the
handler — `new Worker(...)` and `await client.startWorkflow(...)` trip
`no-hidden-io` and `no-unknown-await`. The bundled examples in
`packages/examples/src` are written that way on purpose, for readability, and
would report ~87 diagnostics with zero real determinism violations among them.

So scope the globs like this:

- Put workflow handlers in files that contain nothing but workflow code. Keep
  worker construction, client calls, and test drivers out of them.
- Then list every helper module those handlers import, too. Widening the globs
  is cheap; missing a module is silent.

**Runtime guards.** A backstop that replaces process-global built-ins (`Date`,
`Math.random`, `crypto.*`, the timer family, `fetch`, the promise combinators)
with wrappers that throw when called inside workflow code. They catch what the
lint cannot see — including calls through unlinted modules and dynamic
dispatch — but they alter shared globals for the entire host process, so they
are **on by default in development and test, and off by default in production**:

```ts
new Worker({
  /* ... */
  nondeterminismGuards: true   // force on; omit for the NODE_ENV default
});
```

The default is `process.env.NODE_ENV !== "production"`, re-read per execution.
Set the option explicitly in either direction to override it. Hosts that enable
the guards can put every original global back — by identity, not just
behaviour — with `uninstallNondeterminismGuards()`, which is the right thing to
call on worker shutdown or between test cases. It is safe to call mid-flight,
but from that moment already-running workflows stop being checked.

Two things the runtime guards deliberately do not cover, both delegated to the
lint:

- **`process.env` is never patched**, in any form. An accessor or `Proxy` there
  taxes every environment read in the whole process, and it throws from places
  nobody wrote it: `console.log` and `console.error` detect colour support
  whenever an argument is not already a string, and that detection reads the
  environment. So `console.log(someObject)` inside workflow code would fail
  while naming `process.env`. The lint rejects `process.env` in every spelling
  instead.
- **`process.cpuUsage`, `process.memoryUsage`, `process.resourceUsage`, and
  `process.chdir` are not guarded at runtime.** They are statically rejected.

Because production runs on the lint alone, treat the `workflowSources` globs as
part of the deployment contract, not as a local convenience.

## Payloads

Payload refs hide inline versus blob-backed storage from workflow code.
Providers and wrappers normalize payload roots for workflow history, query
projections, activity maps, child workflow maps, signal inboxes, and provider
state. Shared provider conformance covers history, queue, signal, query,
activity-map item, and child-workflow-map item roots.

Use `PayloadBackend` to add blob offload around a provider:

```ts
import { PayloadBackend, LocalDirectoryBlobStore } from "@durust/payload";

const backend = new PayloadBackend({
  backend: new MemoryBackend(),
  blobStore: new LocalDirectoryBlobStore({ root: ".durust-blobs" }),
  inlineThresholdBytes: 8 * 1024
});
```

Large activity-map and child-workflow-map manifests are still ordinary payloads:
when the configured inline threshold is exceeded, the payload backend offloads
them through the same blob store path as workflow inputs, outputs, signals, and
activity results.

`activityMapManifest(items, { itemSchema, itemCodec })` encodes each durable
item payload through the optional schema adapter while keeping manifest
container payloads in the default nested-payload-safe codec. Result helpers such
as `decodeActivityMapResults(...)` and `decodeChildWorkflowMapSuccesses(...)`
also accept an optional output schema for schema-transformed result refs.

## Providers

Every provider implements `currentTime()`, and it must report the same clock
that provider's leases, deadlines and due scans read — not `Date.now()`. A
worker takes the instant it hands the runtime from there, so a provider whose
`currentTime()` disagreed with its own `fireDueTimers` would record timer
deadlines its own scan never reaches. `MemoryBackend`, `SqliteBackend` and
`PostgresBackend` all accept a `nowMs` option and report it.

A `callActivity()` with no `taskQueue` is scheduled onto the scheduling
worker's `activityTaskQueue`, and its command fingerprint does **not** depend on
that resolution: the `optionsDigest` hashes the queue the caller asked for,
defaulted to the literal `"default"`. Two workflow workers configured with
different activity queues therefore fingerprint the same unqueued call
identically, and a run scheduled by one replays on the other. Rust narrows its
half the same way, against `TaskQueue::default()`.

`MemoryBackend` is for fast tests and local simulations. It is not durable
across process restart.

`SqliteBackend` is for local development and tests. It persists append history,
active queues, leases, timers, signals, child workflow state, activity-map
state, child-workflow-map state, activity start-to-close and heartbeat timeout
metadata, and query projections in one SQLite database.
Workflow replay history is streamed from normalized `history_events` rows, and
workflow claim selection filters registered workflow types using stored workflow
type name/version columns before hydrating replay history. Query projections
live in normalized `query_projections` rows, and payload GC roots read
workflow-visible payload refs from normalized history and query projection
tables.
Activity task claim filters use queue projection columns for run id, command
key, activity name, task queue, map-item identity, availability, terminal state,
lease state, and input payload before hydrating the task record. Wait rows keep
namespace and command-id columns so timer maintenance and signal wake checks do
not scan workflow history. Activity-map and child-workflow-map item inputs,
results, outcomes, in-flight state, and terminal state live in item rows so
payload roots and map progress stay bounded.
Remaining SQLite provider facts use compact rows in the same database until the
final production-grade SQLite schema is completed.

```ts
import { SqliteBackend } from "@durust/sqlite";

const backend = new SqliteBackend({ path: "durust.db" });
```

`PostgresBackend` passes the shared conformance suite when
`DURUST_POSTGRES_URL` is set. It uses normalized SQL tables as the durable
authority: append history lives in `{table}_history_events`, current workflow
IDs in `{table}_workflow_ids`, workflow run state in `{table}_workflow_runs`,
query projections in `{table}_query_projections`, waits in `{table}_waits`,
signals in `{table}_signals`, activity tasks in `{table}_activity_tasks`, and
map descriptors/items in the activity-map and child-workflow-map tables.

Workflow and activity claims, timer maintenance, activity timeout maintenance,
signals, query reads, map progress, payload roots, and history streaming all
read and update those normalized rows in transactions. Provider counters are
stored in `{table}_counters`. Payload GC roots read from normalized history,
query, signal, activity, and map tables.

Postgres recovery tests cover repeated close/reopen boundaries for mixed
activity/signal/timer workflows, child workflow parent notification, and
activity-map plus child-workflow-map descriptor progress.

```ts
import { PostgresBackend } from "@durust/postgres";

const backend = new PostgresBackend({
  url: process.env.DURUST_POSTGRES_URL,
  tableName: "durust_state"
});
```

## Testing And Benchmarks

Run the examples:

```bash
npm run test --workspace @durust/examples
```

The examples currently cover a checkout workflow using activities plus child
workflow result handling, an approval workflow using `signal`, `sleep`,
`select`, and query projection, a fanout workflow using both `activityMap` and
`childWorkflowMap` with manifest-backed results, and a versioning workflow using
`patched`, `getVersion`, `deprecatePatch`, and `continueAsNew`. They also
include a control-flow workflow using `join`, `joinAll`, `selectAll`, and
`sideEffect`, a retry workflow using provider-owned `RetryPolicy` backoff
without intermediate parent failure history, a heartbeat workflow using the
activity-side `heartbeat()` context API, a payload-offload workflow using
`PayloadBackend` and `LocalDirectoryBlobStore`, and a parent-close-policy
workflow showing child workflow `Cancel` versus `Abandon` behavior.

Run the full TypeScript gate:

```bash
npm run check
```

That command runs:

- `npm run build`
- `npm run test`
- `npm run test:types`
- `npm run lint`
- `npm run package:dry-run`

Benchmark threshold coverage:

```bash
npm run test:benchmark-thresholds
```

The threshold suite includes smoke baselines for memory `mixed`,
`activity-heartbeat`, `child-map`, and `write-ceiling`, plus local
memory/SQLite accepted-profile guards and env-gated Postgres smoke and accepted
guards when `DURUST_POSTGRES_URL` is set.

### Current Benchmark Medians

These medians come from three runs on a shared Darwin 25.5.0 machine. Treat the
before-to-current comparison as same-machine evidence; do not use the absolute
numbers as deployment capacity. The mixed workload matches the Rust benchmark
shape: one parent workflow, three activities, one signal, one timer, one child
workflow, and final completion verification per workflow.

| Backend | Config | Processing workflows/s before -> current | Processing actions/s before -> current | Variance | Commit p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| Memory | 1000 workflows, 4 workers, batch 32 | 1581.466 -> 1650.651 (+4.4%) | 12651.729 -> 13205.207 (+4.4%) | 1.7% | 0.036 ms |
| SQLite | 100 workflows, 1 worker, batch 32 | 104.030 -> 119.274 (+14.7%) | 832.243 -> 954.188 (+14.7%) | 0.2% | 2.442 ms |
| SQLite | 100 workflows, 4 workers, batch 32 | 107.480 -> 118.531 (+10.3%) | 859.841 -> 948.247 (+10.3%) | 2.5% | 5.541 ms |
| Postgres | 1000 workflows, 10 workers, pool 24 | 192.753 current | 1542.023 current | 4.0% | 11.646 ms |

The Postgres accepted profile used
`postgres://durable:durable@127.0.0.1:55432/durable`, reported normalized schema
stats, and measured 1.015 transactions/action and 7.393 statement calls/action
for the median run. The checked-in Postgres baseline (211.629 workflows/s) has
stale metadata — the commit it records measures 12.8 workflows/s with a
9x-higher statement mix, so it was captured under untracked conditions — and a
commit-level bisect shows current code within run variance of the pre-change
code (medians 172-187 across interleaved runs). The Postgres row therefore
reports current numbers without a before-to-current comparison until the
baseline is re-recorded.

Reproduce the accepted-profile reports after building the workspace:

```bash
npm run build --workspace @durust/benchmark

node packages/benchmark/dist/index.js \
  --backend memory --mode mixed --workflows 1000 --workers 4 \
  --batch 32 --activity-completion-batch 1 --json

node packages/benchmark/dist/index.js \
  --backend sqlite --mode mixed --workflows 100 --workers 1 \
  --batch 32 --activity-completion-batch 1 --json

node packages/benchmark/dist/index.js \
  --backend sqlite --mode mixed --workflows 100 --workers 4 \
  --batch 32 --activity-completion-batch 1 --json

DURUST_POSTGRES_URL='postgres://durable:durable@127.0.0.1:55432/durable' \
  node packages/benchmark/dist/index.js \
  --backend postgres --mode mixed --workflows 1000 --workers 10 \
  --batch 32 --activity-completion-batch 32 --postgres-pool-size 24 --json
```

Run the benchmark CLI directly:

```bash
node packages/benchmark/dist/index.js \
  --backend sqlite \
  --mode mixed \
  --workflows 100 \
  --workers 4 \
  --batch 32 \
  --activity-completion-batch 1 \
  --json
```

Supported modes are `mixed`, `activity`, `activity-heartbeat`, `signal`,
`timer`, `child`, `activity-map`, `child-map`, `recovery`, `payload`, and
`write-ceiling`. The `activity-heartbeat` mode measures one activity heartbeat
recording per workflow. The `write-ceiling` mode is intentionally minimal: each
workflow starts and commits one immediate completion, which isolates provider
start/commit write overhead.

Postgres benchmarks require `DURUST_POSTGRES_URL`.

**Each Postgres benchmark run creates and drops its own database**, named
`durust_tsbench_<pid>_<ms>_<counter>` on that URL's server. It has to: every
counter the run reports except the `wal*` group comes from `pg_stat_database`
and `pg_stat_statements` filtered to `current_database()`, so sharing a database
with anything else — a conformance suite, a psql session, another benchmark —
folds that traffic into the run's own numbers. Measured on the accepted profile
under ~1.7M foreign transactions: `transactionsPerMixedAction` read **17.3**
against a ceiling of 1.174 sharing a database, and **1.06** with its own. The
name is printed to stderr the moment it is created.

`--keep-db` keeps that database instead of dropping it, and reports its name as
`postgres_database` in `--json` and in the human output. No signal handler is
installed, so an interrupt leaks the database; reclaim it with:

```bash
DURUST_POSTGRES_URL=... npm run --prefix typescript -w @durust/benchmark exec -- \
  durust-benchmark-workload --list-stale     # or --drop-stale
```

Both are server-wide, not per-run: they consider every `durust_tsbench_`
database on that server, not just this process's. `--drop-stale` never evicts a
live session — a database with a backend attached is not listed, and the drop
omits `with (force)` so a session arriving in between makes the drop fail rather
than be evicted — but an idle `--keep-db` database is indistinguishable from a
leak and will be reclaimed. On a shared server run `--list-stale` first.

Run the env-gated Postgres release checks with:

```bash
DURUST_POSTGRES_URL='postgresql://durable:durable@127.0.0.1:55432/durable' \
  npm run check:postgres
```

That command fails fast without `DURUST_POSTGRES_URL`, then runs the Postgres
provider conformance suite plus the benchmark threshold gate that includes the
Postgres mixed smoke baseline and the 1000-workflow accepted Postgres profile.

`DURUST_REQUIRE_POSTGRES=1` turns every Postgres skip into a failure. Set it
wherever a run is expected to exercise Postgres — CI does, next to its service
container — so a missing database or a dropped variable fails instead of
reporting a green run of zero coverage. Unset, the suites skip as before; `=0`
and `=false` are explicit off switches, matching the Rust suite's reading of
the same variable. A blank `DURUST_POSTGRES_URL` counts as unset, so an
expansion that produced nothing skips rather than trying to connect to the
empty string.

## Upgrading

There is no changelog yet, so breaking changes are recorded here, newest first.
A change is listed if it can break a deployment that is working today — either
its code will not compile, or its in-flight runs stop replaying.

### `callActivity()` with no `taskQueue` now runs on the worker's activity queue

**Who is affected.** A deployment whose workflow workers set
`activityTaskQueue` *and* whose workflows call `callActivity()` without naming
a queue, where some other worker polls `"default"` and runs those activities.
That shape works on 0.2.1, because the runtime ignored
`WorkerOptions.activityTaskQueue` entirely — `worker.ts` never passed it into
the runtime at all — and scheduled every unqueued activity onto the literal
queue `"default"`.

**What changes.** `ActivityScheduled.taskQueue` for an unqueued call is now the
scheduling worker's own `activityTaskQueue`, matching Rust's
`ActivityOptions::with_task_queue_fallback`. This is a fix — the far more
common outcome of the old behaviour was a run that hung forever with no error,
because the worker scheduled onto a queue nothing polled — but it moves work
between queues, so a worker that used to serve those activities on `"default"`
stops seeing them and the run makes no progress.

**The command fingerprint is *not* affected.** An earlier version of this
change also folded the resolved queue into the activity's `optionsDigest`,
which broke replay for in-flight runs. That half has been retracted: the digest
hashes the queue the caller named, defaulting to the literal `"default"`, so an
unqueued call fingerprints exactly as it did on 0.2.1 no matter how any worker
is configured. Only the routing changed.

**The repair: set the scheduling worker's `activityTaskQueue` to `"default"`,**
or give the affected calls an explicit `taskQueue`, or put a worker on the new
queue with those activities registered. The first works fleet-wide, because the
old behaviour was uniform: every unqueued activity was scheduled onto the
literal `"default"` no matter how its worker was configured. It is also free of
side effects now that the fingerprint no longer depends on it. One operational
detail, measured rather than assumed:

- **`activityTaskQueue` also decides which queue that worker *claims* from.**
  If the worker you repoint was the one serving activities that *do* name a
  queue explicitly, it stops claiming them. Leave another worker on the
  original queue, or the explicitly-queued work stalls while the repair is in
  place.

**Before upgrading**, do one of: keep the repair above ready to apply; drain
runs that have an unqueued `callActivity()` in flight; or give those calls an
explicit `taskQueue`, which both routes and fingerprints identically before and
after.

### `DurableBackend.currentTime()` is required

**Who is affected.** Anyone with a hand-written `DurableBackend`, and any
wrapper that forwards by explicit delegation rather than by `Proxy` — a
delegating wrapper does not inherit a new method, it just stops compiling.
`PayloadBackend` and `MeasuredBackend` are both of that kind and both gained a
one-line delegate in this change. A wrapper that forwards through a `Proxy`
trap needs nothing.

**What changes.** `DurableBackend` gained `currentTime(): Promise<TimestampMs>`,
which `SPEC.md` §8.1 has always listed on the backend contract and which Rust
has always had. A worker reads it once per prepared workflow task and hands it
to the runtime, so `sleep(d)` records `now + d`. Without it every timer deadline
was measured from the Unix epoch and only worked because a provider scan then
treated every timer as already due.

**Why it is required rather than optional.** An optional method with a
`Date.now()` fallback would silently give a provider that runs its own clock —
every simulation, every virtual-time test, any provider deriving time from the
database — a deadline it never scans past, and the symptom is a workflow that
stops without an error. A required method turns that into a compile error at
the one file that can fix it. Implement it by returning the same clock your
leases and due scans already use, not `Date.now()`.

## Migration Checklist

Use this checklist when moving durable code into Durust TypeScript or when
porting a Rust Durust workflow shape to TypeScript:

- Choose stable durable names for workflows, activities, and signals. Treat
  those names as persisted API identifiers, not implementation symbols.
- Convert every workflow, activity, child workflow start, activity call, map
  item, and signal payload to one named durable request object. Use a named
  empty object type for no-input handlers.
- Own schema compatibility in application code. Prefer additive optional
  fields, stable serialized names, default handling, versioned durable names,
  or workflow version markers for breaking changes.
- Replace native time, random values, filesystem/network I/O, native timers,
  native promise combinators, worker threads, and child processes inside
  workflow code with durable APIs.
- Export and review a durable manifest from the registry that owns production
  handlers. Check the manifest in CI so durable inventory changes are explicit.
- Pick the provider by deployment shape: memory for tests and simulations,
  SQLite for local single-file development, and Postgres only after the
  production-readiness gate below is satisfied.
- Wrap providers in `PayloadBackend` when large workflow inputs, activity
  payloads, signals, query projections, or map manifests can exceed the inline
  threshold. Validate blob retention and GC roots before release.
- Add deterministic replay tests, provider conformance coverage for provider
  changes, close/reopen tests for persistent providers, fault simulations for
  leases and duplicate delivery, and benchmark threshold coverage for hot paths.

## Production Readiness Checklist

Do not publish or operate the TypeScript packages as production infrastructure
until all of these are true for the target release:

- `DURUST_POSTGRES_URL=... npm run check:release` passes. This aggregate gate is
  a local pre-release convenience, not the gate. CI is: `.github/workflows/ci.yml`
  runs `npm run check` (which itself runs `check:fixtures`), the Rust half of
  every shared contract fixture, the Postgres provider conformance suite
  against a `postgres:17-alpine` service container with
  `DURUST_REQUIRE_POSTGRES=1`, and `test:soak`. A **push-triggered** release
  runs only after CI on `main` concludes successfully. A **manually dispatched**
  one does not: `release.yml`'s job condition short-circuits on
  `workflow_dispatch`, and its `bump` input defaults to `patch`, so a dispatch
  left at its defaults publishes an ordinary patch release with no CI-success
  requirement and no branch restriction. Only `bump: current` is the
  recover-a-partial-release shape the root `README.md` describes. What
  `check:release` adds on top is
  `test:benchmark-thresholds`, which CI deliberately leaves out: it compares
  throughput against baselines recorded on one developer machine, and its
  accepted-profile case asserts `pg_stat_statements` output, which needs a
  server started with `shared_preload_libraries=pg_stat_statements`. Run it on
  a controlled machine before a release; do not treat a green CI as having run
  it. Use `node scripts/check-release.mjs --dry-run` to inspect the command
  list without running the gates.
- `npm run check` passes on a clean checkout, including build, Vitest suites,
  type-negative tests, determinism lint, and package dry-run validation.
- `npm run check:fixtures` passes, proving the TypeScript neutral fixture tests
  and Rust `cargo test --test contract_fixtures` agree on history, payload,
  provider I/O, and benchmark vocabulary.
- `npm run test:soak` passes for the release candidate. The default soak enables
  `DURUST_LONG_SOAK=1` and runs the hot execution cache crash/restart/fault
  matrix. The switch is on for any value except empty, `0`, and `false`, the
  same reading as `DURUST_REQUIRE_POSTGRES`; set `DURUST_REQUIRE_LONG_SOAK=1`
  alongside it to turn a soak that is switched off into a failure rather than a
  skip. Tune `DURUST_LONG_SOAK_SEEDS`, `DURUST_LONG_SOAK_WORKFLOWS`,
  `DURUST_LONG_SOAK_GENERATIONS`, `DURUST_LONG_SOAK_STEPS`,
  `DURUST_LONG_SOAK_FINAL_STEPS`, and `DURUST_LONG_SOAK_CONFLICTS` upward for
  release-candidate burn-in.
- `DURUST_POSTGRES_URL=... npm run check:postgres` passes against the supported
  Postgres version and schema migration state.
- Memory, SQLite, Postgres, and payload-wrapped providers pass the shared
  conformance suite for every stable backend contract behavior.
- SQLite and Postgres recovery tests prove append history, active indexes,
  leases, signals, timers, child workflows, activity maps, child workflow maps,
  query projections, and payload roots survive process restart.
- Workflow determinism is enforced by source linting in production and
  additionally by runtime guards outside it, and nondeterminism remains a hard
  failure rather than a restart/compatibility mechanism. Confirm the
  `workflowSources` globs cover every module reachable from workflow code: the
  lint does not follow calls into files it was not pointed at, and it is the
  only gate running in production unless `nondeterminismGuards: true` is set.
- Payload blob storage has a durability, availability, and GC plan. GC roots
  must be read from provider-owned durable state, not reconstructed from
  application memory.
- Worker deployment config is reviewed: namespaces, task queues, registered
  workflow/activity/signal sets, lease durations, local activity capacity,
  activity completion batch size, shutdown behavior, event sinks, and metrics.
- Accepted benchmark baselines are checked in with machine/profile details for
  memory, SQLite 1-worker, SQLite 4-worker, and Postgres profiles.
- Postgres uses normalized append/index storage on the durability path, and
  strict accepted Postgres benchmarks cover the release profile.
- Production-length soak coverage exercises the final hot async workflow cache,
  crash/restart, cache eviction, stale leases, duplicate delivery, conflicts,
  timers, signals, children, and map fanout. Payload-store outage/recovery
  remains covered by focused deterministic recovery tests.

## Release Readiness

The TypeScript implementation should not be used as production infrastructure
until the production-readiness gate in `../impl-plan/0014-typescript-parity.md`
is satisfied. The npm packages are split so early adopters, provider authors,
and conformance suites can install only the pieces they need while the remaining
production-readiness work is completed. In particular, the remaining major gaps
include:

- broader public API docs and examples for every stable Rust primitive;
- running and recording the opt-in `npm run test:soak` profile for the release
  candidate.

Package dry-run validation is already wired into `npm run check`; it verifies
that publishable packages include only intended built JS, declaration files,
source maps, JSON assets, package metadata, and allowed root docs.
