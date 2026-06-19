---
id: 0014
title: TypeScript-native durable execution parity
status: in_progress
depends_on: [0001, 0002, 0003, 0004, 0005, 0006, 0007, 0008, 0009, 0011, 0012]
labels: [typescript, runtime, providers, conformance, simulation, benchmarks]
---

# TypeScript-Native Durable Execution Parity

Build a first-class TypeScript implementation of Durust with the same durable
execution model, provider contract, correctness posture, and benchmark
discipline as the Rust crate. This is not a thin FFI wrapper over Rust. It is a
native Node/TypeScript runtime whose public API is designed for TypeScript
first, while preserving the same history semantics, deterministic replay rules,
payload offload behavior, provider conformance, and hot-path benchmark shapes.

The TypeScript implementation may lag the Rust-only Postgres shard-native
internals in `0013` until that item exits `in_progress`, but it must not weaken
the generic provider contract to match a simpler Node implementation.

## Public API Budget

The existing composition alternatives are:

- call the Rust crate through FFI or a sidecar service;
- ask TypeScript services to use a different workflow engine;
- hand-roll idempotency, timers, signals, activity queues, child workflow
  fanout, payload manifests, and replay tests per application.

Those are insufficient for correctness and scalability. FFI or sidecars lose a
native TypeScript workflow authoring model and complicate typed activity/workflow
registration. Hand-rolled durable coordination usually grows unbounded history,
skips deterministic replay, weakens lease fencing, or stores oversized payloads
inside hot database rows.

The TypeScript API earns first-class surface area because it protects the same
invariants as the Rust API:

- one append-only replay history reconstructs workflow locals after crash,
  cache eviction, or worker restart;
- durable operations are registered in deterministic source/collection order;
- workflow-visible facts use stable workflow, activity, signal, timer, child,
  version, query, activity-map, and child-workflow-map identities;
- large fanout stays manifest-backed, with provider-owned per-item state and
  bounded parent history;
- payload refs hide inline versus blob storage from user code;
- providers pass one shared conformance suite and can optimize with batch
  claims, batch commits, and append-journal storage without changing runtime
  semantics;
- deterministic restrictions are enforced by runtime checks, lint rules, and
  replay/simulation tests rather than documentation alone.

API parity means behavioral parity, not syntactic parity. Rust macro and builder
APIs do not translate literally to TypeScript, so the TypeScript surface should
mirror durable semantics rather than Rust spelling. Prefer TypeScript-native
conventions:

- explicit function definitions instead of required decorators or macros;
- typed options objects where they read better than long builder chains;
- discriminated unions for `select` and terminal outcomes;
- native `Promise`/`async` ergonomics at the public boundary, backed by
  runtime-controlled durable thenables internally;
- schema adapters as optional type/runtime validation hooks rather than one
  required schema library;
- fluent methods only where they express a staged durable operation clearly,
  such as `spawn().result()`.

The accepted native shape is function-based and decorator-free by default:

```ts
export const priceQuote = activity({
  name: "payments.price-quote",
  handler: async (input: QuoteInput): Promise<QuoteOutput> => {
    return quote(input);
  },
});

export const checkout = workflow({
  name: "orders.checkout",
  version: 1,
  queryState: orderViewSchema.optional(),
  handler: async (input: CheckoutInput): Promise<CheckoutOutput> => {
    const quote = await callActivity(priceQuote, input.quote, {
      taskQueue: "payments",
      retry: RetryPolicy.exponential({ maxAttempts: 5 }),
    });

    const decision = await select({
      approved: signal<Approval>("approved"),
      cancel: signal<Cancel>("cancel"),
      timeout: sleepUntil(input.approvalDeadline),
    });

    if (decision.branch === "timeout") {
      throw Error.timeout("approval");
    }

    const child = await childWorkflow(shipOrder, input.ship, {
      workflowId: `ship/${input.orderId}`,
      parentClosePolicy: ParentClosePolicy.Cancel,
    }).spawn();

    const shipment = await child.result();
    return { orderId: input.orderId, shipmentId: shipment.id };
  },
});
```

Decorators may be added later only as syntax sugar over the same explicit
definitions. They must not be required for production use because TypeScript
decorator behavior and compiler settings vary across projects.

## Type Safety Contract

The TypeScript API must provide strong static typing for workflow and activity
boundaries. Type safety is part of the public API, not only editor polish.

### Forced Single Named Object Input

TypeScript translates Rust's forced single input struct rule into a forced
single named object input rule.

Adopt a forced single-input-object rule for TypeScript workflows, activities,
child workflow starts, activity calls, and signal payloads.

Every workflow and activity handler must accept exactly one input parameter. That
input must be an object-shaped durable request type, not a primitive, tuple,
array, null/undefined, void, or positional parameter list. No-input handlers
must still use a named empty input object:

```ts
type NoInput = {};

const noInputWorkflow = workflow({
  name: "maintenance.no-input",
  version: 1,
  handler: async (input: NoInput): Promise<void> => {
    void input;
  },
});
```

Accepted shape:

```ts
workflow(input: CheckoutInput): Promise<CheckoutOutput>
activity(input: ChargeCardInput): Promise<ChargeCardOutput>
```

Rejected shapes:

```ts
activity(orderId: string, amountCents: number): Promise<ChargeCardOutput>
activity(orderId: string): Promise<ChargeCardOutput>
activity(): Promise<void>
activity(input: readonly string[]): Promise<void>
```

The runtime enforces one durable request object; compatibility of that object is
an application/schema contract. Developers own backward-compatible input
evolution: additive optional fields, default handling, stable serialized field
names, and versioned durable names or workflow version markers for breaking
changes. Do not try to solve schema compatibility automatically in the runtime.

Nondeterminism remains a hard failure. Do not add "restart workflow on
determinism failure" behavior as a compatibility mechanism, and do not add
activity auto-versioning as the first answer to schema evolution.

TypeScript's structural type system cannot prove that `{}` came from a named
alias or interface, so the hard compile-time rule is "exactly one non-array
object input". Public docs, examples, tests, and lint fixtures should still use
a named request type, including `type NoInput = {}` for no-input handlers, so
the durable boundary always has an explicit schema evolution point.

Practical TypeScript enforcement:

- type-level helpers require `Input extends DurableInputObject`;
- call-site helpers validate the actual input argument type too, because
  TypeScript's `{}` empty-object spelling otherwise accepts primitives;
- handler registration requires exactly one parameter;
- compile tests reject primitive inputs including `string`, `number`, and
  `boolean`, `void`, nullish inputs, arrays, tuple-style handler inputs,
  missing-argument handlers, positional handler parameter lists, primitive
  values passed to ordinary workflow/activity/signal calls, and primitive
  values passed to named empty-object calls;
- when a runtime schema adapter is supplied, registration should reject schemas
  whose root kind is known and not `object`;
- schema-first registration may strengthen this later, but v1 must not require
  one schema library globally.

Core definition types:

```ts
export type DurableInputObject = object;

export type ActivityDefinition<Input extends DurableInputObject, Output> = {
  readonly kind: "activity";
  readonly name: string;
  readonly handler: (input: Input) => Promise<Output> | Output;
};

export type WorkflowDefinition<Input extends DurableInputObject, Output, QueryState = unknown> = {
  readonly kind: "workflow";
  readonly name: string;
  readonly version: number;
  readonly handler: (input: Input) => Promise<Output> | Output;
};
```

Typed call sites:

```ts
const quote: QuoteOutput = await callActivity(priceQuote, quoteInput);

const child = await childWorkflow(shipOrder, shipInput, {
  workflowId: `ship/${shipInput.orderId}`,
}).spawn();

const shipment: ShipmentOutput = await child.result();
```

Required invariants:

- `activity(...)` infers `Input` and `Output` from the handler unless explicit
  generic parameters are supplied, and rejects non-object or multi-parameter
  inputs.
- `workflow(...)` infers `Input`, `Output`, and optional query-state type from
  the handler and metadata, and rejects non-object or multi-parameter inputs.
- `callActivity(activityDef, input, options)` accepts only the activity's input
  type and returns a durable promise/handle for the activity's output type.
- `childWorkflow(workflowDef, input, options).spawn()` accepts only the child
  workflow's input type and returns a `ChildWorkflowHandle<Output>`.
- `Client.startWorkflow(workflowDef, workflowId, taskQueue, input)` accepts only
  the workflow's input type and returns a typed handle/result for its output.
- `queryWorkflow(workflowDef, workflowId)` returns the workflow's query-state
  type when one is declared.
- `signal<T extends DurableInputObject>(name)` and `sendSignal<T>(...)`
  preserve signal payload types at workflow and client boundaries.
- `activityMap(activityDef, options)` carries item input and item output types
  through `PayloadRef<ActivityMapInputManifest<Input>>` and
  `PayloadRef<ActivityMapResultManifest<Output>>`.
- `childWorkflowMap(workflowDef, options)` carries child input and output types
  through input manifests and success/outcome result manifests.
- Stringly typed or positional escape hatches, if any, must be clearly named as
  unsafe or advanced APIs and must not be used by examples.

Because TypeScript types erase at runtime, cross-process safety also requires
payload metadata. Schema adapters should be optional but first-class: users may
attach a runtime schema/codec adapter to a workflow, activity, signal, query
state, or manifest item type so workers can validate decoded payloads and record
schema fingerprints. The v1 API must not require one schema library globally.

Type-safety tests must use Vitest `expectTypeOf` for positive inference checks
and `@ts-expect-error` fixture files compiled with `tsc --noEmit` for negative
checks. Negative tests should cover wrong activity input, wrong child workflow
input, wrong workflow start input, wrong signal payload, wrong query-state
assignment, and mismatched activity-map/child-workflow-map manifest item types.

## Package Layout

Create a Node workspace under `typescript/` using the package manager's normal
install/add commands so manifests and lockfiles stay in sync.

Use Vitest as the TypeScript test framework for unit, replay, provider
conformance, integration, fixture, and benchmark-threshold tests. Shared
conformance helpers in `@durust/testing` should be ordinary Vitest suites that
each provider package imports and runs against its provider factory.

Initial packages:

- `@durust/core`: public API, workflow runtime, worker, client, history types,
  options, errors, manifest helpers, memory provider, and generic provider
  contract.
- `@durust/sqlite`: SQLite provider for tests and local development, with WAL
  and `synchronous=FULL`.
- `@durust/postgres`: normalized Postgres provider, batching hooks, migrations,
  and Postgres-specific benchmarks.
- `@durust/payload`: payload backend wrapper, local-directory blob store, and
  S3-compatible blob store.
- `@durust/testing`: provider conformance, replay fixtures, deterministic
  simulation harness, crash/reopen helpers, and test workload factories.
- `@durust/eslint-plugin`: deterministic workflow lint rules.
- `@durust/benchmark`: workload runner, threshold comparison, and checked-in
  baseline reader.

Keep generated files, package build output, and benchmark artifacts out of the
Rust crate's source tree except for checked-in source, tests, docs, and accepted
baseline JSON.

Current checkpoint:

- The TypeScript workspace now includes `@durust/core`, `@durust/testing`,
  `@durust/payload`, `@durust/sqlite`, `@durust/postgres`,
  `@durust/benchmark`, `@durust/eslint-plugin`, and `@durust/examples`
  packages.
- `@durust/eslint-plugin` exports a `no-workflow-nondeterminism` rule plus a
  recommended flat config. The rule flags hidden filesystem/network I/O,
  worker threads, child processes, native timer APIs, native promise
  combinators, wall-clock reads, random values, and browser network
  constructors using ESLint-style visitors.
- Root TypeScript build/test references include the plugin package, and plugin
  Vitest coverage verifies rule metadata, recommended config, helper
  classification, and visitor diagnostics.

## Cross-Language Contract

Before implementing provider code, extract the Rust runtime contract into
language-neutral fixtures:

- history event JSON fixtures for every event type;
- command fingerprint fixtures for activities, timers, signals, child
  workflows, activity maps, child workflow maps, version markers, patches,
  queries, and continue-as-new;
- payload-ref fixtures for inline MessagePack, inline JSON, blob refs, manifest
  roots, manifest pages, and durable failures;
- provider request/response fixtures for start, claim, stream, commit, signal,
  activity completion, child dispatch, query projection, payload roots, and GC;
- benchmark JSON schema fixtures for workload output and threshold comparison.

Rust and TypeScript tests must both consume these fixtures. Parity means the two
runtimes agree on durable identity, event ordering, command fingerprints,
payload-root traversal, and public benchmark vocabulary, even when each provider
uses different internal SQL.

## Implementation Slices

### 1. Workspace And Contract Fixtures

- Add `typescript/` workspace scaffolding, TypeScript config, lint/test/build
  scripts, Vitest configuration, and CI commands.
- Add language-neutral fixture directory and Rust tests that validate the
  current Rust types/fingerprints against those fixtures.
- Add TypeScript type definitions for IDs, timestamps, payload refs, history
  events, command fingerprints, retry policy, parent close policy, and workflow
  task commit structures.
- Add serialization helpers for MessagePack and JSON payload refs.

Acceptance gate:

- Rust tests remain green.
- TypeScript builds with strict settings.
- Rust and TypeScript both pass fixture round-trips for core IDs, payload refs,
  history events, and command fingerprints under Vitest.

### 2. TypeScript Public API And Registration

- Implement `workflow(...)`, `activity(...)`, `Client`, `Worker.builder(...)`,
  `Registry`, durable names, workflow versions, query-state metadata, and
  manifest export.
- Design TypeScript-native call shapes for every Rust primitive before
  implementing runtime behavior. Record why each shape is idiomatic TypeScript
  and how it maps to the existing durable command/history contract.
- Implement typed definition objects and call-site generics so activity,
  workflow, signal, query, activity-map, and child-workflow-map input/output
  types are inferred and preserved end to end.
- Enforce the single durable request object rule in public TypeScript types,
  registration checks where schema metadata is available, and negative
  compile-test fixtures.
- Implement duplicate workflow/activity identity rejection.
- Implement explicit durable names as the production path and stable derived
  default names for examples/tests.
- Add manifest write/check/diff commands in a TypeScript CLI or shared
  benchmark/utility package.
- Add README and SPEC updates showing Rust and idiomatic TypeScript versions of
  the same checkout workflow.

Acceptance gate:

- Manifest output is deterministic.
- Duplicate identity tests match Rust behavior.
- Vitest `expectTypeOf` and `tsc --noEmit` negative fixtures prove the public API
  rejects primitive inputs, missing inputs, positional inputs, array inputs,
  mismatched activity, child workflow, signal, query, and map types.
- Examples compile and run against memory.

### 3. Replay Runtime Core

- Implement workflow execution with `AsyncLocalStorage` runtime context.
- Durable APIs return custom thenables/promises that register commands without
  hidden I/O and suspend until replay/live data is available.
- Cache in-flight workflow executions between tasks; replay from the beginning
  on cache miss or conflict.
- Implement append command sequencing, command fingerprints, tail detection,
  payload hydration requests, query projection publication, and deterministic
  workflow time.
- Forbid durable APIs outside a workflow context and activity APIs outside an
  activity context.

Design constraints:

- Workflow code must not use native timers, wall-clock reads, random values,
  native `Promise.race`, native `Promise.all` over durable operations, network
  I/O, filesystem I/O, or worker threads in workflow context.
- Workflow polling must not perform provider or blob-store I/O. It may request
  hydration and suspend so the worker can perform I/O at an explicit boundary.
- Cached workflow promises are a performance layer only. Append history remains
  authoritative.

Acceptance gate:

- Replay tests cover completed workflows, pending waits, conflicts, cache
  eviction, payload hydration, query projection, and terminal command rejection.
- Lint/runtime checks catch representative nondeterministic APIs.

Current checkpoint:

- Workflow execution installs runtime guards for `Date.now()`, `Math.random()`,
  native timer scheduling (`setTimeout`, `setInterval`, `queueMicrotask`), and
  native promise combinators (`Promise.all`, `Promise.race`,
  `Promise.allSettled`, `Promise.any`). The guards throw nondeterminism failures
  only when an `AsyncLocalStorage` workflow context is active, so ordinary
  provider/worker code outside workflow execution keeps normal Node behavior.
- `sideEffect()` temporarily permits nondeterministic globals while recording a
  marker, and replay returns the recorded marker value without rerunning the
  closure.
- Vitest coverage rejects `Date.now()`, direct `Math.random()`, native timer
  APIs, and native promise combinators inside workflow code, verifies globals
  remain usable outside workflow context, and verifies `Math.random()` is
  replay-safe when wrapped in `sideEffect()`.
- A dev-only TypeScript AST scanner, `scripts/determinism-lint.mjs`, checks
  workflow source fixtures for hidden filesystem/network I/O imports, worker
  thread and child-process APIs, native timer APIs, native promise combinators,
  wall-clock reads, random values, and browser network constructors. `npm run
  check` now runs this scanner over valid workflow fixtures, and Vitest coverage
  verifies invalid fixtures fail with deterministic diagnostics.
- The scanner now imports the compiled `@durust/eslint-plugin` classifier
  helpers, so CLI diagnostics and the packaged ESLint rule share one policy
  table while keeping TypeScript AST traversal and ESLint visitor traversal
  separate.
- `scripts/determinism-lint.mjs` supports explicit source paths, explicit
  `--workflow-source`/`--workflow-source-glob` entries, and project-configured
  `workflowSources` from `durust.config.json` or `package.json`
  `durust.workflowSources`. The root workspace uses package configuration for
  its determinism lint gate.
- Remaining determinism work: extend static coverage for any remaining
  Node/browser APIs that cannot be reliably trapped with runtime guards.

### 4. Durable Operations

Implement, test, and document the public durable APIs:

- `callActivity(handler, input, options)`, activity handles, retries, timeouts,
  heartbeat API, local activity preference, and remote worker dispatch.
- `sleep`, `sleepUntil`, `signal`, signal idempotency, and atomic signal
  consumption.
- `select`, `join`, `joinAll`, and `selectAll` with deterministic branch order,
  winner recording, loser cleanup, and replay validation.
- `childWorkflow(handler, input, options).spawn().result()`, parent close
  policy, idempotent child start dispatch, and child terminal events.
- `version`, deprecated patch markers, side effects, and continue-as-new.
- `publish` and query projection reads.

Acceptance gate:

- TypeScript replay tests mirror Rust replay coverage.
- Compile/lint-fail tests cover native timers, wall-clock, random, native
  promise combinators, unknown awaits, and hidden I/O in workflow code.
- Vitest conformance tests for memory provider cover each operation.

### 5. Manifest-Backed Fanout

- Implement `activityMap(...)` with manifest creation helpers, provider-owned
  descriptors, item materialization, retries, `maxInFlight`, ordered result
  manifests, idempotent item completion, payload offload, and replay handles.
- Implement `childWorkflowMap(...)` with deterministic child IDs
  `{prefix}/{ordinal}`, fail-fast and collect-all modes, parent close policy,
  map-item child start tagging, terminal accounting, ordered outcome manifests,
  and compact parent history.
- Ensure map result manifests are hydrated through explicit async boundaries and
  offloaded by the payload backend when the inline threshold is exceeded.

Acceptance gate:

- Parent history remains compact for large maps.
- Memory provider conformance covers bounded materialization, restarts,
  duplicate dispatch/completion, fail-fast cancellation, collect-all outcomes,
  payload roots, and GC roots.

### 6. Memory Provider And Shared Conformance

- Implement an append-journal `MemoryBackend` with the full generic provider
  trait, including batch claim, batch commit, batch activity claim/completion,
  due maintenance, payload roots, and GC no-op behavior.
- Port provider conformance from Rust into `@durust/testing`.
- Add crash/replay simulations by discarding workflow caches and rebuilding
  from append history.
- Add seeded deterministic scheduler tests for stale claims, duplicate
  completions, delayed events, signal/timer/activity races, child outbox
  dispatch, activity maps, and child workflow maps.

Acceptance gate:

- `MemoryBackend` passes the full TypeScript conformance suite.
- Rust and TypeScript conformance names remain aligned so missing parity is
  visible in Vitest output.

### 7. SQLite Provider

- Implement `SqliteBackend` as the local/test durable provider with WAL,
  `synchronous=FULL`, explicit transactions, append history, active wait
  indexes, activity queues, child state, activity maps, child workflow maps,
  query projections, payload roots, and reopen recovery.
- Add provider-specific indexes and batch operations only after correctness
  tests pass.
- Keep SQLite behavior behind the generic provider contract; do not let SQLite
  transaction shape leak into public APIs.

Current checkpoint:

- `@durust/sqlite` uses Node's built-in SQLite binding with WAL,
  `synchronous=FULL`, explicit `BEGIN IMMEDIATE` transactions, provider
  conformance coverage, and close/reopen recovery tests for started workflows,
  activity-map descriptors, and child-workflow-map descriptors.
- Complex provider facts are JSON-encoded in SQLite for the first correctness
  pass. Before claiming the SQLite slice complete, replace this with the
  intended append/index table layout, add payload-root traversal, and measure
  the 1-worker and 4-worker local profiles.

Acceptance gate:

- SQLite passes the same provider conformance suite as memory.
- SQLite tests close and reopen the provider for persistence-sensitive cases.
- Local SQLite benchmark baselines are checked in for 1-worker and 4-worker
  mixed profiles.

### 8. Payload Backend And Object Storage

- Implement `PayloadBackend<B, S>` wrapper with inline/blob thresholding,
  MessagePack default codec, JSON codec support, local-directory blob store, and
  S3-compatible blob store.
- Implement payload hydration boundaries for workflow replay and public reads.
- Implement payload-root traversal through history, signals, activity tasks,
  activity maps, child workflow maps, child outbox, query projections, and
  provider-specific roots.
- Implement dry-run and mutating GC with digest/size validation.

Current checkpoint:

- `@durust/payload` has a provider-agnostic `PayloadBackend` wrapper that
  offloads oversized inline payload refs before durable writes and hydrates blob
  refs on workflow claims, activity claims, history streams, signal reads, and
  query reads.
- Local-directory blob storage has digest/size validation, and Vitest coverage
  forces offload for workflow inputs, activity task inputs, activity completion
  results, and map scheduled history. Map descriptor tasks are kept hydrated for
  current providers because descriptor materialization still decodes manifests
  synchronously during commit.
- `S3CompatibleBlobStore` implements provider-agnostic path-style S3-compatible
  object operations with AWS Signature V4 signing. A local HTTP S3-shaped Vitest
  service covers forced offload, list, hydrate, delete, URI ownership, request
  signing headers, and `PayloadBackend` workflow-start hydration.
- The shared provider conformance suite runs through `PayloadBackend` wrapped
  around `MemoryBackend` and `SqliteBackend` with `inlineThresholdBytes = 0`,
  including activity-map and child-workflow-map manifests with nested blob refs.
- `collectPayloadRefs(...)`, `collectPayloadRefsDeep(...)`,
  `planPayloadGarbageCollection(...)`, and `collectPayloadGarbage(...)` provide
  generic root traversal plus dry-run/mutating local-directory object-store GC.
  GC validates reachable blob digest/size before deleting unreachable objects
  and fails without deleting when a reachable blob is missing or corrupt.
- `MemoryBackend` and `SqliteBackend` expose `payloadRoots()`, shared provider
  conformance verifies history, queue, signal, and query roots, and
  `PayloadBackend.planGarbageCollection()` / `collectGarbage()` use those roots
  directly.
- Before claiming the payload slice complete, add provider-owned roots plus
  blob-backed conformance for Postgres.

Acceptance gate:

- Inline and blob-backed conformance runs behave identically.
- Local-directory and S3-compatible tests force offload with a tiny threshold.
- GC refuses to delete when a reachable blob is missing or corrupt.

### 9. Postgres Provider

- Implement normalized Postgres provider with schema migrations, append history,
  leases, ready queues, signal inbox, timers, activities, child outbox,
  activity maps, child workflow maps, query projections, version markers,
  payload roots, and GC roots.
- Port the proven Rust durability-path optimizations only after correctness:
  batch workflow claim, batch workflow commit, bulk history inserts, bulk
  activity scheduling, batch activity claim, batch activity completion, combined
  due maintenance, sequence-backed hot counters, and transaction-abort retries.
- Match Rust benchmark counters where practical: transactions/action,
  statements/action, commit p50/p95/p99, WAL bytes/sec, and active connections.

Current checkpoint:

- `@durust/postgres` exists as a correctness-first Postgres provider package
  using `pg`. It persists provider state in one transactionally locked text
  state row per configured table, which keeps Postgres behind the shared
  `DurableBackend` contract while normalized append/index tables are still
  pending.
- The provider supports workflow starts/claims/commits, history streaming,
  activity claims/completion/failure, timers, signals, query projections,
  child workflows, activity maps, child workflow maps, and `payloadRoots()`
  through the shared state-transition logic.
- Env-gated Vitest coverage runs shared provider conformance, blob-backed
  `PayloadBackend(PostgresBackend)` conformance with forced offload, and a
  close/reopen workflow persistence test when `DURUST_POSTGRES_URL` is set.
- Package-local workspace tests now work for `@durust/postgres`, and the root
  TS check type-checks the Postgres tests while skipping them without
  `DURUST_POSTGRES_URL`.
- Remaining work before the Postgres slice is complete: replace the state-row
  implementation with normalized append/index tables and migrations, port Rust
  durability-path optimizations, add Postgres-specific restart/fault tests, and
  add TypeScript Postgres benchmarks/baselines.

Acceptance gate:

- Postgres passes provider conformance when `DURUST_POSTGRES_URL` is set.
- Postgres benchmark outputs use the same JSON schema as Rust.
- The accepted TypeScript Postgres baseline is documented next to the Rust
  baseline with machine/profile details and known runtime differences.

### 10. Worker Runtime, Operations, And Recovery

- Implement production worker loops for workflow tasks, activity tasks, local
  activities, due maintenance, child dispatch, payload hydration, cache
  eviction, activity completion batching, graceful shutdown, and error backoff.
- Add worker metrics and structured logs for claims, commits, conflicts, stale
  leases, cache hits/misses, replay bytes/events, activity completion batches,
  and provider errors.
- Add operational docs for deployment, queues, worker registration, payload
  storage, SQLite local mode, and Postgres production mode.

Acceptance gate:

- Long-running integration tests cover worker crash, restart, cache eviction,
  provider conflict, stale leases, delayed visibility, activity worker loss,
  child dispatcher crash, and payload-store outage/recovery.
- Worker shutdown does not lose committed facts or leave uncaught workflow
  promise rejections.

Current checkpoint:

- `Worker.runWorkflowTaskOnce()` now supports explicit
  `registeredSignalNames`, reads matching live signal inbox records before
  polling workflow code, and passes them into deterministic replay preparation.
  This covers workflows that receive a signal before or during a claimed
  workflow task instead of requiring test-only manual `liveSignals` injection.
- Vitest worker coverage verifies a signal sent before polling is consumed and
  committed through the public `Worker` path.
- `Worker.run()` adds an initial production loop around the one-shot task APIs.
  It polls workflow tasks, activity tasks when an activity queue is configured,
  and due timer maintenance; supports `AbortSignal` shutdown, `maxIterations`
  for bounded tests/tools, idle backoff, transient error backoff, timer
  maintenance limits, and an `onError` hook. The loop returns aggregate
  workflow/activity/timer/error/idle counters.
- Vitest worker coverage verifies the loop completes workflow/activity
  progress, fires due timers, stops cleanly during idle backoff when aborted,
  and continues after a transient backend claim error.
- `Worker` now supports bounded local activity preference with
  `maxLocalActivitiesPerWorkflowTask`. After a successful workflow task commit,
  the worker claims and completes locally registered activities from its
  configured activity queue up to that capacity. Capacity `0` keeps the task
  available for remote activity workers.
- Vitest worker coverage verifies local activity preference consumes an
  eligible activity before a remote worker can claim it, and verifies the
  capacity-zero fallback still requires a remote activity worker.
- The provider contract now includes `completeActivities(...)` for ordered
  success-only activity completion batches. Memory, SQLite, Postgres,
  payload-wrapped providers, benchmark instrumentation, and the worker all
  implement the method.
- Shared provider conformance verifies ordered batch results for duplicate,
  stale-lease, successful, and missing activity completions. Worker coverage
  verifies `activityCompletionBatchSize` groups multiple successful activity
  completions into one provider call, and benchmark smoke coverage verifies
  `--activity-completion-batch` reaches the batch operation path.
- `Worker` now exposes a structured `onEvent` hook and cumulative
  `metrics()` snapshot for workflow claims/commits/conflicts, activity
  claims/completions/failures, activity completion batches, timer fires, loop
  errors, idle sleeps, and event sink failures. Event sink failures are counted
  without breaking durable task processing.
- Vitest worker coverage verifies normal event/metric recording and verifies
  that a failing event sink does not prevent a committed workflow from
  completing.
- SQLite persistence coverage now exercises public `Worker` recovery across
  close/reopen boundaries: one worker schedules an activity, a reopened worker
  completes the activity, and another reopened worker replays the wake and
  commits workflow completion.
- Remaining work before the worker-runtime slice is complete: cache eviction,
  child-dispatch loops, and long-running crash/restart/fault integration tests.

### 11. Benchmarks And Regression Gates

- Implement `durust-benchmark-workload` equivalent for TypeScript with modes:
  `mixed`, `activity`, `signal`, `timer`, `child`, `activity-map`,
  `child-map`, `recovery`, `payload`, and Postgres write ceiling where useful.
- Implement benchmark threshold comparison against checked-in baselines.
- Record processing-only throughput, activations/sec, mixed actions/sec,
  workflow-task commit latency, replay events/sec, provider read/write counts,
  and provider-specific counters.
- Run the same accepted profiles as Rust where possible:
  memory, SQLite 1-worker, SQLite 4-worker, Postgres 1-shard, and Postgres
  100-shard/10-worker.

Acceptance gate:

- Benchmarks produce stable JSON and fail regression thresholds in CI or a
  documented performance job.
- Any accepted TypeScript baseline below Rust by more than an explicit tolerance
  has a documented runtime reason and next optimization target.

Current checkpoint:

- `@durust/benchmark` adds a private TypeScript benchmark package with a
  `durust-benchmark-workload` bin and root Vitest alias.
- The current CLI supports `--backend memory|sqlite|postgres`, `--mode mixed`,
  `activity`, `signal`, `timer`, `child`, `activity-map`, `child-map`,
  `recovery`, and `payload`, Rust-compatible flags for the accepted mixed
  profile (`--workflows`, `--workers`, `--shards`, `--physical-partitions`,
  `--activation-concurrency`, `--activation-prefetch-limit`, `--batch`,
  `--activity-completion-batch`, `--postgres-pool-size`, `--json`), and stable
  JSON output with Rust-aligned top-level throughput/counter fields.
- The mixed workload covers workflow start, signal, child workflow start/result,
  timer firing, boot activity, child activity, finish activity, workflow tasks,
  activity tasks, and backend operation latency instrumentation.
- The child-map workload covers manifest-backed child workflow maps with bounded
  `maxInFlight` and ordered success result decoding. Additional focused modes
  cover single-activity, signal-only, timer-only, child-only, manifest-backed
  activity-map, replay-heavy recovery, and blob-backed payload-offload paths.
- Vitest benchmark smoke coverage runs small memory profiles for every
  implemented mode and parser compatibility checks. A built CLI smoke run also
  produces JSON for a tiny memory mixed profile.
- Benchmark threshold comparison exists via `compareBenchmarkToBaseline(...)`,
  checked-in smoke baselines for memory `mixed` and `child-map`, and a named
  `npm run test:benchmark-thresholds` gate. The smoke baselines validate stable
  JSON shape, logical counters, required backend operation metrics, operation
  error counts, and loose throughput/commit-latency thresholds; they are not the
  accepted machine/profile baselines.
- Remaining work before the benchmark slice is complete: add a Postgres
  write-ceiling mode if it proves useful; add strict checked-in accepted
  baselines; collect provider-specific Postgres counters; and run accepted
  memory/SQLite/Postgres profiles.

### 12. Documentation, Examples, And Release

- Update `README.md`, `SPEC.md`, and examples with side-by-side Rust and
  idiomatic TypeScript APIs for core workflows, signals, timers, select/join,
  child workflows, activity maps, child workflow maps, payload offload, query
  projection, versioning, and continue-as-new.
- Add TypeScript examples matching the Rust example set.
- Add migration and production-readiness checklists for TypeScript users.
- Publish packages only after memory, SQLite, payload, Postgres, conformance,
  replay, simulation, and benchmark gates are satisfied.

Acceptance gate:

- `npm test`, `npm run lint`, `npm run build`, TypeScript conformance,
  provider-specific Vitest suites, and benchmark threshold tests pass.
- Rust tests still pass after shared fixture/spec changes.
- Package contents are checked with dry-run publish commands before release.

Current checkpoint:

- `@durust/examples` adds a compile-checked checkout example that uses the
  idiomatic TypeScript API, forced named object inputs, activities, child
  workflow start/result handling, `Worker`, `Client`, `Registry`, and bounded
  local activity preference against the memory backend.
- Root Vitest coverage runs the checkout example end to end, so examples are
  not only type-checked.

## Correctness Gate

Every slice must leave behind deterministic tests before expanding scope:

- unit tests for local invariants;
- replay tests for command-producing workflow behavior;
- provider conformance for backend behavior;
- crash/reopen tests for persistent providers;
- seeded deterministic simulation for scheduling, leases, retries, timers,
  recovery, activity maps, child maps, and child outbox state;
- lint or compile-fail-style tests for nondeterministic workflow code;
- benchmark coverage for hot paths introduced by the slice.

Provider work is incomplete until memory, SQLite, and Postgres pass the same
shared conformance suite for the touched behavior.

Current checkpoint:

- Seeded memory worker/provider simulations now cover randomized workflow-task,
  activity-task, and virtual-timer interleavings for a mixed workflow using an
  activity, signal, timer, child workflow, and child activity.
- Additional seeded simulations cover bounded materialization for activity maps
  and compact parent history for child workflow maps.
- The simulation driver records seed traces and uses deterministic virtual time;
  remaining simulation gaps are lease fencing/stale claims, retries, worker
  crash/restart, persistent-provider recovery, child dispatcher crashes, and
  payload-store outage/recovery.

## Performance Gate

Use Rust baselines as the semantic profile reference, not as an immediate
throughput promise. The TypeScript runtime must still establish and maintain
checked-in baselines for:

- warm cached workflow happy path;
- worker crash plus streaming replay;
- local-preferred activities;
- remote-only activities;
- signal-heavy workflows;
- timer-heavy workflows;
- child workflow fanout;
- manifest-backed activity maps;
- manifest-backed child workflow maps;
- payload refs with inline and blob-backed payloads;
- SQLite local/test mode;
- Postgres normalized provider mode.

Keep an optimization only when it improves the targeted TypeScript benchmark
without weakening replay determinism, provider conformance, or payload offload
semantics. Do not port Rust SQL optimizations blindly; measure them in Node's
runtime shape and keep only proven wins.

## Production Readiness Gate

The TypeScript implementation is not production-ready until all of these are
true:

- public API docs and examples cover an idiomatic TypeScript equivalent for
  every stable Rust primitive;
- workflow determinism restrictions are enforced by lint and runtime checks;
- memory, SQLite, and Postgres providers pass conformance;
- persistent providers recover after process restart using append history;
- payload offload and GC pass inline/blob conformance;
- activity maps and child workflow maps keep parent history compact;
- worker crash, stale lease, duplicate completion, conflict, signal, timer,
  child, map, and payload-store fault simulations pass;
- benchmark baselines and regression thresholds are checked in;
- package dry-run publish verifies that npm artifacts include only intended
  source, types, docs, and generated assets.

## Open Design Decisions

Resolve these in slice 1 before runtime code expands:

- whether TypeScript providers must be storage-compatible with existing Rust
  Postgres/SQLite schemas, or only contract-compatible at the history/payload
  level;
- exact package manager and Node LTS floor;
- whether the first SQLite provider uses Node's built-in SQLite module, a native
  dependency, or an async wrapper, based on WAL/FULL durability, transaction
  control, and benchmark evidence;
- whether TypeScript manifest schema metadata uses a required user-supplied
  schema library, optional adapters, or opaque type names plus codec
  fingerprints for v1;
- exact TypeScript public call shapes for activity, child workflow, activity
  map, child workflow map, `select`, `join`, `joinAll`, and `selectAll`, with
  semantic mapping tests against Rust fixtures before implementation expands;
- whether cross-language workers may intentionally share one namespace in the
  same database for v1, or whether that is deferred until schema compatibility
  tests prove it.
