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

Run the env-gated Postgres release checks with:

```bash
DURUST_POSTGRES_URL='postgresql://durable:durable@127.0.0.1:55432/durable' \
  npm run check:postgres
```

That command fails fast without `DURUST_POSTGRES_URL`, then runs the Postgres
provider conformance suite plus the benchmark threshold gate that includes the
Postgres mixed smoke baseline and the 1000-workflow accepted Postgres profile.

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

- `DURUST_POSTGRES_URL=... npm run check:release` passes. This aggregate gate
  runs the fast workspace check, cross-runtime contract fixture checks, the
  hot-cache soak profile, and the env-gated Postgres release check. Use
  `node scripts/check-release.mjs --dry-run` to inspect the command list without
  running the gates.
- `npm run check` passes on a clean checkout, including build, Vitest suites,
  type-negative tests, determinism lint, and package dry-run validation.
- `npm run check:fixtures` passes, proving the TypeScript neutral fixture tests
  and Rust `cargo test --test contract_fixtures` agree on history, payload,
  provider I/O, and benchmark vocabulary.
- `npm run test:soak` passes for the release candidate. The default soak enables
  `DURUST_LONG_SOAK=1` and runs the hot execution cache crash/restart/fault
  matrix; tune `DURUST_LONG_SOAK_SEEDS`, `DURUST_LONG_SOAK_WORKFLOWS`,
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
- Workflow determinism is enforced by runtime guards and source linting, and
  nondeterminism remains a hard failure rather than a restart/compatibility
  mechanism.
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

The TypeScript implementation should not be published or used as production
infrastructure until the production-readiness gate in
`../impl-plan/0014-typescript-parity.md` is satisfied. In particular, the
remaining major gaps include:

- broader public API docs and examples for every stable Rust primitive;
- running and recording the opt-in `npm run test:soak` profile for the release
  candidate.

Package dry-run validation is already wired into `npm run check`; it verifies
that publishable packages include only intended built JS, declaration files,
source maps, JSON assets, package metadata, and allowed root docs.

The current checked-in accepted local benchmark medians were measured on
June 19, 2026, with Node v24.15.0 on Darwin 25.5.0 arm64. The Postgres accepted
profile used PostgreSQL 16.11 from `tests/fixtures/postgres.compose.yml` with
`pg_stat_statements` loaded. Median results:

- memory mixed local 4-worker: 8859.109 mixed actions/sec, commit p95 0.012 ms.
- SQLite mixed local 1-worker: 719.137 mixed actions/sec, commit p95 2.098 ms.
- SQLite mixed local 4-worker: 842.354 mixed actions/sec, commit p95 2.104 ms.
- Postgres mixed accepted: 129.48 mixed actions/sec, commit p95 2.468 ms,
  9.257 transactions/action, 28.251 statement calls/action.
