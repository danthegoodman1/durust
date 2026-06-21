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

The TypeScript `DurableBackend` may expose optional batched workflow and
activity claim hooks. The existing primitive composition is repeated
single-claim polling followed by the same replay and commit path. That remains
correct, but it is insufficient for the Postgres hot path because one worker
batch otherwise creates one transaction per claimed task and repeats history and
signal prefetch statements per task. The optional batch hooks protect bounded
transaction and statement counts per worker batch while preserving lease
fencing, deterministic claim order, bounded result sets, and provider
substitutability. They are not user-facing workflow APIs; providers without the
hook keep using the single-claim contract.

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
- `@durust/core` now ships a `durust-manifest` bin with `write`, `check`,
  `diff`, and `accept` commands. The command loads an ESM module exporting a
  `Registry`, a manifest object, or a function returning either value, then
  writes or compares a stable `durable.manifest.json` baseline.
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

Current checkpoint:

- The TypeScript contract fixture now covers every public history event type,
  every public command fingerprint helper (`Activity`, `ActivityMap`,
  `ChildWorkflow`, `ChildWorkflowMap`, `Timer`, `Signal`, and
  `VersionMarker`), plus inline JSON, inline MessagePack, blob payload refs,
  and durable failure shapes. Vitest verifies complete event-type coverage,
  event-type derivation, fingerprint helper output, inline payload decode,
  payload-ref JSON round trips, MessagePack decode, and detailed durable failure
  payloads against the neutral fixture.
- Rust integration coverage now consumes the same neutral fixture and verifies
  complete Rust event type names, command fingerprint helper output, inline JSON
  and MessagePack payload decode, blob payload-ref digest/size/URI shape, and
  durable failures with optional details against it.
- The shared core fixture now also includes neutral manifest root/page payload
  refs for activity-map input manifests, activity-map result manifests, and
  child-workflow-map outcome manifests. TypeScript and Rust both decode the
  root-to-page-to-item traversal from the same fixture while adapting only the
  runtime-specific manifest struct spelling.
- A neutral provider I/O fixture now covers shared backend vocabulary for
  workflow start/idempotent start, workflow task claim, history streaming,
  workflow task commit/conflict, signal delivery/duplicates/inbox records,
  activity completion/batch/failure/retry outcomes, timer maintenance, query
  projection, child-start dispatch counts, payload roots, and payload GC summary
  shapes. Vitest and Rust integration tests both consume this fixture, adapting
  only language-specific field spelling where the current Rust and TypeScript
  provider contracts intentionally differ.
- A neutral benchmark fixture now covers TypeScript benchmark result/baseline
  and threshold-comparison JSON, plus Rust workload result and comparison JSON
  vocabulary including camelCase counters, backend metrics, optional Postgres
  statement stats, and resource samples. The fixture also covers hot execution
  cache worker-stat fields and forbidden-operation threshold vocabulary so
  benchmark gates can assert both required and absent provider operation classes.
  `@durust/benchmark` Vitest coverage and Rust integration coverage both consume
  the fixture.
- The TypeScript workspace now exposes `npm run check:fixtures`, which runs the
  TypeScript neutral fixture tests and Rust `cargo test --test contract_fixtures`
  together. The aggregate release gate includes this command, so future shared
  fixture edits must stay accepted by both runtimes before publish.

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

Current checkpoint:

- Public TypeScript definitions preserve workflow, activity, signal, query,
  activity-map, and child-workflow-map input/output types through registration
  and call sites. Positive Vitest `expectTypeOf` coverage verifies inference for
  activity calls, child workflow starts/results, client workflow starts,
  queries, signals, joins, selects, and map handles.
- Negative `tsc --noEmit` fixtures reject primitive, missing, positional,
  array, tuple, nullish, and mismatched inputs at durable boundaries, including
  named empty-object workflow/activity/signal calls, query projection publishes,
  continue-as-new requests, and map manifest item mismatches.
- `activity(...)` and `workflow(...)` now also enforce handler arity at runtime,
  rejecting JavaScript or unsafe TypeScript handlers that do not expose exactly
  one input parameter. Registration tests cover zero-parameter and
  multi-parameter activity and workflow handlers.
- Public runtime call sites now reject JavaScript or unsafe TypeScript values
  whose durable input root is primitive, nullish, an array, or a function.
  Coverage checks activity calls, child workflow starts, workflow starts,
  signal sends, continue-as-new inputs, and manifest item encoding while
  preserving the named empty object shape.
- Signal definitions now accept optional payload schema adapters with
  `signal<T>("name", { schema })`. The runtime rejects known non-object signal
  schema roots, `Client.sendSignal` encodes through the signal schema and
  records its fingerprint, and workflow signal awaits decode through the same
  schema while preserving the existing typed `signal<T>("name")` form.
- Workflow query projections declared with `queryStateSchema` now encode
  `publish(...)` payloads through the query-state schema and record the schema
  fingerprint, matching the existing client-side query decode path. `publish`
  rejects primitive, nullish, array, and function projection values at compile
  time for TypeScript callers and at runtime for JavaScript or unsafe
  TypeScript callers, preserving the object-shaped query-state contract.
- Manifest helpers now accept optional item schema metadata:
  `activityMapManifest(items, { itemSchema, itemCodec })` encodes each item
  payload through the schema adapter and records the item schema fingerprint,
  while map result helper decoders accept optional output schemas for
  schema-transformed activity-map and child-workflow-map result refs.
- Public map scheduling helpers now reject invalid descriptor options before
  history scheduling: empty result manifest names, non-positive or non-integer
  `maxInFlight`, and empty child workflow map ID prefixes. Focused runtime
  tests cover both activity-map and child-workflow-map public call sites.

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

- Workflow execution installs runtime guards for replay-unsafe time/random APIs
  (`Date.now()`, `Date()`, zero-argument `new Date()`, `performance.now()`,
  `Math.random()`, Web Crypto random APIs, and `process.hrtime()` /
  `process.hrtime.bigint()`), native timer/scheduling APIs (`setTimeout`,
  `setInterval`, `setImmediate`, `process.nextTick`, `queueMicrotask`, and
  available browser frame or idle callbacks), native promise combinators (`Promise.all`,
  `Promise.race`, `Promise.allSettled`, `Promise.any`), `fetch()`, and available
  browser network constructors (`WebSocket`, `EventSource`, and
  `XMLHttpRequest`). Runtime guards also wrap process-global state APIs:
  workflow code cannot read, enumerate, or mutate `process.env` except for
  value-producing reads recorded through `sideEffect()`, cannot call
  `process.cwd()` except through `sideEffect()`, and cannot call
  `process.chdir()` in workflow context. Process runtime usage reads such as
  `process.cpuUsage()`, `process.memoryUsage()`, `process.resourceUsage()`, and
  `process.uptime()` are also blocked unless recorded through `sideEffect()`.
  The guards throw nondeterminism failures only when an `AsyncLocalStorage`
  workflow context is active, so ordinary provider/worker code outside workflow
  execution keeps normal Node behavior. Deterministic `new Date(timestamp)`
  construction remains allowed.
- `sideEffect()` temporarily permits nondeterministic globals while recording a
  marker, and replay returns the recorded marker value without rerunning the
  closure.
- Core runtime now includes an internal `HotWorkflowExecution` primitive that
  keeps a JavaScript async workflow frame alive across committed durable waits.
  The first slices support sequential `callActivity`, `sleep`/`sleepUntil`, and
  `signal` awaits plus durable `join`, `joinAll`, `select`, and `selectAll`
  combinators over activity/timer/signal branches, plus child workflow
  `spawn()` and handle `result()` waits. The primitive uses an explicit
  commit-acknowledgement handshake: start or resume produces a
  `WorkflowTaskCommit`, successful backend commit advances the cached execution
  tail, and later wake history resolves the still-pending await without
  reissuing commands. Focused Vitest coverage proves local workflow state before
  the first await/combinator is not rerun when an activity completes, a timer
  fires, a signal is delivered, join branches all complete, select/selectAll
  choose a winner, a child workflow starts and completes, or a child workflow
  start fails. Coverage now also proves provider-owned activity-map and
  child-workflow-map result manifests resume the same hot parent frame after map
  completion. `Worker` now uses the primitive as a bounded hot execution cache
  after successful provider commit acknowledgement, while still falling back to
  replay on cache miss, restart, stale lineage, or provider conflict.
- Vitest coverage rejects the guarded time/random APIs, native timer APIs,
  native promise combinators, and network APIs inside workflow code, verifies
  deterministic `new Date(timestamp)` still works, verifies globals remain
  usable outside workflow context, and verifies value-producing nondeterministic
  globals are replay-safe when wrapped in `sideEffect()`.
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
- The packaged ESLint rule and standalone workflow-source scanner now share a
  classifier for unknown awaits. Durable await roots such as `callActivity`,
  `sleep`, `signal`, `select`, `join`, `childWorkflow(...).spawn()`, and
  durable handle result methods are allowed, while native promises, arbitrary
  async helpers, and identifier-held native promises are reported as
  `durust/no-unknown-await`.
- The packaged ESLint rule and standalone workflow-source scanner also report
  obvious aliases of forbidden static APIs at declaration time, such as
  `const now = Date.now`, `const { random } = Math`, and
  `const { all } = Promise`, so workflow code cannot bypass the direct-call
  checks by invoking those APIs through local variables. Plugin unit tests and
  the invalid workflow-source fixture cover both member-reference and
  destructuring forms.
- Static determinism coverage now also rejects additional replay-unsafe
  time/random/native async APIs that are easy to miss in TypeScript workflow
  source: zero-argument `new Date()`, `Date()` calls, `performance.now()`,
  Web Crypto random APIs, `process.hrtime()` / `process.hrtime.bigint()`,
  `process.cwd()`, `process.chdir()`, process runtime usage calls, process
  identity/argument reads, `process.env` reads, `node:os` imports,
  `process.nextTick()`, `setImmediate()`, `requestAnimationFrame()`, and
  `requestIdleCallback()`. The shared classifier handles nested static member
  names such as `process.hrtime.bigint`, `process.memoryUsage.rss`, and
  `globalThis.process.env`, including computed string forms like `Date["now"]`,
  `Promise["all"]`, and `process["env"]`. Plugin unit tests and workflow-source
  fixtures cover direct calls, direct reads, obvious aliases, and computed
  string member access.
- Static determinism coverage now additionally rejects native timer and host
  timing modules (`node:timers`, `node:timers/promises`, `node:perf_hooks`),
  network/process/terminal/debugger modules (`node:http2`, `node:dgram`,
  `node:readline/promises`, `node:repl`, `node:inspector`, and
  `node:cluster`), native worker/message-channel constructors,
  `AbortSignal.timeout()`, `Atomics.wait()`, browser storage/cache/database
  calls, browser navigation and history mutation, DOM reads, browser
  location/cookie/navigator state reads, clipboard/geolocation/service-worker
  APIs, and `navigator.sendBeacon()`. The invalid workflow-source fixture and
  plugin unit tests pin the standalone scanner and packaged classifier to this
  broader replay-safety policy.
- Static determinism coverage now also rejects replay-visible host output and
  process-control APIs that runtime guards cannot reliably intercept, including
  `console.*` output/timing calls, `process.stdout`/`stderr` writes,
  `process.stdin` reads, process warning/report calls, and process
  termination/signalling APIs such as `process.exit()`, `process.abort()`, and
  `process.kill()`.
- Static determinism coverage now rejects named and namespace imports of Node
  crypto random/key-generation APIs from `crypto` and `node:crypto`, including
  `randomBytes`, `randomUUID`, `randomInt`, random fill helpers, key generation
  helpers, and `webcrypto`, while leaving deterministic named imports such as
  hashing helpers available for applications that need pure computations.
- Static determinism coverage now also rejects dynamic/native code execution
  surfaces that cannot be made replay-safe by runtime trapping: direct
  `eval()`, `Function()` and `new Function(...)`, aliases of those identifier
  APIs, `node:vm` / `vm` imports, and WebAssembly compile, instantiate,
  validate, streaming, module, and instance paths.
- Static determinism coverage now also rejects workflow-source calls to
  activity-only Durust APIs such as `heartbeat()`, including direct named
  imports and `@durust/core` namespace imports, so activity liveness recording
  remains confined to activity handler context.
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
- SQLite workflow replay history is streamed from normalized `history_events`
  rows, and workflow task claim selection filters registered workflow types in
  SQL before hydrating replay history.
- SQLite query projections live in `query_projections`, and payload roots read
  workflow-visible payload refs from normalized history, query, signal,
  activity, activity-map item, and child-workflow-map item rows.
- SQLite activity task claim filters use stored queue columns for namespace,
  task queue, registered activity names, availability, terminal state, lease
  state, map-item identity, and input payload before hydrating the task record.
- SQLite waits carry namespace, command-id, and ready-time columns so timer
  maintenance and signal wake checks use indexed rows instead of scanning
  workflow history.
- SQLite activity-map and child-workflow-map item inputs, results, outcomes,
  in-flight state, and terminal state live in item rows so map progress and
  payload-root traversal stay bounded.
- Remaining SQLite provider facts use compact rows in the same database until
  the final production-grade SQLite schema is completed. Before claiming the
  SQLite slice complete, replace the remaining hot payload state with the
  intended append/index table layout where it matters, and measure the
  1-worker and 4-worker local profiles.

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
  conformance verifies history, queue, signal, query, activity-map item, and
  child-workflow-map item roots, and `PayloadBackend.planGarbageCollection()` /
  `collectGarbage()` use those roots directly.
- Payload outage recovery coverage now forces a blob-store hydration failure
  after a workflow-task claim, verifies no durable commit is written, verifies a
  replacement worker cannot reclaim until the failed claim lease expires, and
  verifies recovery succeeds once the blob store is healthy again.
- Env-gated Postgres coverage runs the same forced-offload
  `PayloadBackend(PostgresBackend)` provider conformance suite and now verifies
  local-directory GC uses roots exposed by Postgres: a workflow input stored as
  a blob is retained, an orphan blob in the same store is deleted, and the raw
  Postgres history still contains the retained blob ref.
- `PostgresBackend.payloadRoots()` reads roots from normalized history, query,
  signal, activity, activity-map item, and child-workflow-map item tables.
  Env-gated coverage verifies GC planning retains reachable blobs from those
  normalized roots across provider reopen boundaries.

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

- `@durust/postgres` uses normalized SQL tables as the durable authority. It
  stores provider counters in `{table}_counters`, append history in
  `{table}_history_events`, current workflow IDs in `{table}_workflow_ids`,
  workflow run state in `{table}_workflow_runs`, query projections in
  `{table}_query_projections`, waits in `{table}_waits`, signals in
  `{table}_signals`, activity tasks in `{table}_activity_tasks`, and map
  descriptors/items in activity-map and child-workflow-map tables.
- Workflow starts, claims, commits, activity claims/completions/failures,
  heartbeats, timers, activity timeouts, signal delivery/consumption, child
  notifications, map progress, query reads, history streaming, and payload-root
  traversal all read and update normalized rows in Postgres transactions.
- The normalized schema creates partial indexes for ready and expired workflow
  claims, due timer waits, unconsumed signal inbox reads, unclaimed and expired
  activity claims, and due activity timeouts. Derived table and index names use
  stable short hash suffixes when needed so long user/test table names do not
  collide under Postgres's 63-byte identifier limit.
- Env-gated Vitest coverage runs shared provider conformance,
  blob-backed `PayloadBackend(PostgresBackend)` conformance with forced
  offload, normalized schema/index tests, normalized row tests for workflow,
  query, signal, wait, activity, and map state, and close/reopen recovery
  coverage for mixed workflows, child workflow parent notification, activity
  maps, and child-workflow maps.
- Postgres initialization best-effort installs `pg_stat_statements` when the
  database supports it, while preserving non-fatal fallback behavior for
  managed databases without extension privileges or preload. Env-gated coverage
  verifies `statsSnapshot()` after real provider writes and checks positive
  statement calls when the extension is available.
- Remaining Postgres work before release readiness: port Rust durability-path
  batching optimizations where they still apply to the normalized TypeScript
  provider, add strict accepted TypeScript Postgres benchmarks/baselines, and
  run the accepted Postgres profile at release scale.

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
  progress, fires due timers, stops cleanly during idle and error backoff when
  aborted, and continues after a transient backend claim error.
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
- Shared provider conformance now verifies workflow and activity lease expiry:
  expired workflow-task claims are reclaimable with the original wake reason,
  replacement claims receive fresh tokens, old workflow commits are fenced, and
  expired activity claims are reclaimable while old completions remain stale.
  Memory, SQLite, Postgres, and payload-wrapped providers implement internal
  lease expiry; SQLite also persists lease expiry/reason across close/reopen.
- Normal activity retry attempts are now provider-owned for memory, SQLite, and
  Postgres: retryable failures reschedule the same activity task with an
  incremented attempt without appending an intermediate parent history failure,
  exhausted retries append the terminal `ActivityFailed`, and shared provider
  conformance verifies no premature workflow wake. Memory coverage also
  verifies exponential retry delay with an injected deterministic clock.
- Normal activity start-to-close and heartbeat timeouts are now provider-owned
  for memory, SQLite, and Postgres: activity claims record an attempt start
  timestamp and heartbeat deadline, activity handlers can call `heartbeat()`
  through a claim-scoped worker context, retryable timeout attempts reschedule
  without intermediate parent history, terminal timeouts append
  `ActivityTimedOut`, and workflow replay/hot execution surfaces the timeout as
  `ActivityFailureError` with `errorType: "ActivityTimedOut"`. Shared provider
  conformance verifies start-to-close and heartbeat timeout retry behavior,
  heartbeat recording, stale heartbeat fencing, and terminal wakeup; worker
  coverage verifies activity-context heartbeat calls and outside-context
  rejection. SQLite and env-gated Postgres persistence coverage now close and
  reopen after a refreshed heartbeat, then verifies the persisted deadline is
  honored before terminal heartbeat timeout.
- Activity-map item retries use the same provider-owned attempt accounting:
  retryable item failures keep the same ordinal in flight and reschedule the
  same activity task, while exhausted retries append one compact
  `ActivityMapFailed` parent event. Shared provider conformance verifies no
  premature parent wake and no intermediate parent failure history.
- `Worker` now exposes a structured `onEvent` hook and cumulative
  `metrics()` snapshot for workflow claims/commits/conflicts, activity
  claims/completions/failures, activity completion batches, timer fires, loop
  errors, idle sleeps, and event sink failures. Event sink failures are counted
  without breaking durable task processing.
- Vitest worker coverage verifies normal event/metric recording and verifies
  that a failing event sink does not prevent a committed workflow from
  completing.
- `Worker` now treats workflow-task claim prefetch as an optimization instead
  of a correctness requirement. If the claim contains only a contiguous prefix
  of the replay history, the worker streams missing history in bounded
  `streamHistory` chunks up to the replay target before running workflow code.
  Vitest coverage truncates claim prefetch to one event and verifies multi-chunk
  replay streaming, final workflow completion, and streamed chunk/event metrics.
- `Worker` now maintains a bounded replay-history cache by run id. It fills
  partial workflow-task claim prefetch from cached contiguous history before
  calling `streamHistory`, updates the cache after committed workflow tasks, and
  exposes cache hit/miss/eviction counts through `metrics()`. Vitest coverage
  proves a cached partial-prefetch replay streams only the newly appended event,
  and proves least-recent cache eviction falls back to bounded streaming.
  Seeded simulation coverage now also runs multiple concurrent mixed workflows
  through a one-worker, two-entry replay-history cache with truncated claim
  prefetch, duplicate idempotent signals, activities, child workflows, timers,
  cache evictions, and bounded streaming metrics. This is an incremental
  replay-work reduction, not the final hot async workflow-future cache.
- `Worker` now maintains a bounded hot workflow execution cache by run id,
  separate from the replay-history cache. The worker uses a cached
  `HotWorkflowExecution` only when the new history delta contains provider-owned
  wake facts that the live frame can ingest directly; if another worker has
  committed command events for the run, the stale hot frame is evicted and the
  task cold-replays from authoritative history. Metrics now expose hot execution
  cache hits, misses, and evictions. Vitest coverage proves hot resume avoids
  rerunning pre-await workflow state, worker restart falls back to replay, cache
  capacity evicts old hot executions, provider commit conflict invalidates a hot
  frame, and seeded multi-worker simulations continue to pass with child
  workflows, signals, timers, and cache churn. Focused coverage now also stops
  a worker loop by abort signal while a hot workflow frame is parked on an
  activity wait, completes the activity on another worker, and resumes the
  original hot frame without rerunning pre-await workflow code. Shutdown
  coverage also verifies aborting with that hot frame parked on a durable wait
  does not emit an unhandled rejection before later completion resumes the same
  frame. Worker shutdown
  coverage now also verifies that an abort requested after a workflow-task
  commit stops the loop before it starts activity polling or timer maintenance
  in the same iteration, and that local activity preference is skipped when a
  loop abort is requested while the workflow task is being processed. Local
  activity preference coverage also verifies that an abort requested after one
  local activity completes stops before the next local activity claim when
  capacity remains. Coverage also verifies that an already-aborted signal
  prevents initial polling, that an abort requested after activity completion
  stops the loop before due timer maintenance can run in that iteration, and
  that an abort requested after timer maintenance fires stops the loop before
  activity-timeout maintenance can process unrelated claimed activities in the
  same iteration. Batched activity-polling shutdown coverage verifies that an
  abort requested after one batched activity is claimed lets that claimed
  activity complete and flushes its completion, but stops before claiming
  another activity in the same batch; the same coverage now also exercises a
  mixed success/failure batch, proving successful completions flush before a
  failed activity is recorded and an abort stops before the next queued activity
  claim. Separate shutdown coverage now also verifies an abort requested from
  `onError` or during transient-error backoff returns promptly instead of
  waiting out the configured error backoff.
- SQLite persistence coverage now exercises public `Worker` recovery across
  close/reopen boundaries: one worker schedules an activity, a reopened worker
  completes the activity, and another reopened worker replays the wake and
  commits workflow completion.
- SQLite persistent recovery coverage now includes a mixed workflow that
  repeatedly closes and reopens the provider between start, workflow-task
  commit, activity completion, signal delivery, timer firing, another activity
  completion, and final workflow completion. This verifies append history and
  active indexes recover together across process boundaries for activity,
  signal, timer, and replay progress.
- The current TypeScript providers do not use a separate child-start dispatcher
  or outbox loop; ordinary child workflow starts are materialized
  synchronously and atomically during parent workflow-task commit. SQLite
  recovery coverage now closes and reopens between parent child-start commit,
  child workflow execution, parent notification, and parent completion to prove
  that provider-owned child state and parent wake history recover together.
- Postgres recovery coverage now exercises the same child-start and parent
  notification close/reopen path through public `Worker` polling.
- Postgres persistent-provider recovery tests now also cover activity-map and
  child-workflow-map descriptor state across close/reopen boundaries, including
  bounded item materialization after one item completes and final ordered
  manifest decoding.
- Focused seeded fault coverage now runs hot execution cache restart, commit
  conflicts, cache evictions, duplicate signals, child workflows, timers,
  activities, truncated claim prefetch, and bounded `streamHistory` recovery
  across three deterministic seeds. Each seed uses multiple worker-driver
  generations to simulate restarts before final recovery, and verifies compact
  per-run history counts for activity, child, signal, timer, and terminal
  events.
- The workspace now exposes `npm run test:soak`, an opt-in long-running Vitest
  profile enabled by `DURUST_LONG_SOAK=1`. The default profile runs a larger
  hot execution cache crash/restart/fault matrix with configurable seed,
  workflow, generation, step, final-step, and conflict counts. Normal
  `npm run test` keeps this block skipped so the fast gate stays fast.
  Remaining work before the worker-runtime slice is complete: broaden shutdown
  behavior beyond the focused Vitest regressions and run the soak at release
  scale on release hardware.

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
  `activity`, `activity-heartbeat`, `signal`, `timer`, `child`,
  `activity-map`, `child-map`, `recovery`, `payload`, and `write-ceiling`,
  Rust-compatible flags for the accepted mixed profile (`--workflows`,
  `--workers`, `--shards`,
  `--physical-partitions`, `--activation-concurrency`,
  `--activation-prefetch-limit`, `--batch`, `--activity-completion-batch`,
  `--postgres-pool-size`, `--json`), and stable JSON output with Rust-aligned
  top-level throughput/counter fields.
- The mixed workload covers workflow start, signal, child workflow start/result,
  timer firing, boot activity, child activity, finish activity, workflow tasks,
  activity tasks, and backend operation latency instrumentation.
- The child-map workload covers manifest-backed child workflow maps with bounded
  `maxInFlight` and ordered success result decoding. Additional focused modes
  cover single-activity, activity heartbeat, signal-only, timer-only,
  child-only, manifest-backed activity-map, replay-heavy recovery, blob-backed
  payload-offload, and a write-ceiling profile that isolates provider workflow
  start plus immediate workflow-task commit overhead.
- Vitest benchmark smoke coverage runs small memory profiles for every
  implemented mode and parser compatibility checks. A built CLI smoke run also
  produces JSON for a tiny memory mixed profile.
- Benchmark threshold comparison exists via `compareBenchmarkToBaseline(...)`,
  checked-in smoke baselines for memory `mixed`, `activity-heartbeat`,
  `child-map`, and `write-ceiling`, and a named
  `npm run test:benchmark-thresholds` gate. The heartbeat smoke baseline
  requires `heartbeatActivity` provider metrics so activity heartbeat recording
  stays in benchmark coverage. The smoke baselines validate stable JSON shape,
  logical counters, required backend operation metrics, forbidden-operation
  absence where a profile is meant to isolate a path, operation error counts,
  and loose throughput/commit-latency thresholds; they are not the accepted
  machine/profile baselines.
- Benchmark worker stats now include bounded replay-history stream chunk/event
  counts and replay-history cache hit/miss/eviction counts. The checked-in
  benchmark baselines and neutral benchmark fixture require exact worker-stat
  values, so future prefetch/cache changes cannot silently add replay streaming
  work to profiles that should be fully prefetched or hide cache-behavior drift.
- TypeScript Postgres benchmark runs now report provider-specific
  `postgres_stats` instead of `null` when the Postgres backend is selected. The
  report diffs before/after `pg_stat_database` and `pg_stat_wal` snapshots into
  WAL, transaction, row, block-cache, temp-file, deadlock, and derived
  per-second/per-action/per-workflow counters. When `pg_stat_statements` is
  installed and loaded, the same report includes statement call/exec-time
  deltas, calls per mixed action/workflow, and the top statement list; otherwise
  statement stats remain `null`. The neutral benchmark fixture documents the
  TypeScript Postgres stats vocabulary.
- A 1000-workflow, 4-worker memory mixed accepted-local baseline is checked in
  alongside the smoke baselines. The threshold gate verifies exact logical
  counters, exact replay-stream worker stats, required backend operation
  coverage, operation error counts, and loose local throughput/commit-latency
  bounds.
- SQLite mixed-workload accepted-local baselines are now checked in for
  100-workflow, 1-worker and 4-worker profiles. The threshold gate runs both
  profiles with exact logical counters and required backend operation coverage,
  plus loose local throughput and workflow-task commit p95 bounds. These are
  local regression baselines, not final release performance claims.
- An env-gated Postgres mixed smoke baseline is checked in for the normalized
  provider. The threshold gate skips it unless `DURUST_POSTGRES_URL` is set,
  then verifies exact logical counters, exact replay-stream worker stats,
  required backend operation coverage, operation error counts, non-null
  Postgres stats, and positive statement calls when `pg_stat_statements` is
  available.
- The root workspace now exposes `npm run check:postgres` as the explicit
  env-gated Postgres release check. It requires `DURUST_POSTGRES_URL`, runs the
  Postgres provider conformance suite, then runs the benchmark threshold gate so
  the checked-in Postgres smoke and accepted baselines cannot be accidentally
  skipped.
- The Postgres provider now uses SQL-native durability paths for the mixed
  benchmark hot operations that fit the TypeScript provider shape: workflow
  starts, workflow claims, normal workflow-task commits, ordinary child
  workflow start and terminal notification, normal activity claims,
  success-only activity completion batches, signal delivery, and due timer
  firing. Complex paths such as activity maps, child workflow maps,
  continue-as-new, and parent-close cancellation still fall back to the
  correctness-first normalized path.
- Strict accepted benchmark baselines are checked in for the documented local
  profiles: 1000-workflow memory mixed with 4 workers, 100-workflow SQLite mixed
  with 1 worker and 4 workers, and 1000-workflow Postgres mixed with 10 workers,
  batch 32, activity completion batch 32, and pool size 24. Accepted Postgres
  threshold coverage requires normalized schema stats, statement stats, exact
  logical counters, exact replay-stream worker stats, required operation
  coverage, no backend operation errors, and bounded transactions/statement
  calls per mixed action.
- Current measured medians on June 19, 2026, Node v24.15.0, Darwin 25.5.0
  arm64: memory mixed local 4-worker 8859.109 mixed actions/sec with commit p95
  0.012 ms; SQLite mixed local 1-worker 719.137 mixed actions/sec with commit
  p95 2.098 ms; SQLite mixed local 4-worker 842.354 mixed actions/sec with
  commit p95 2.104 ms; Postgres mixed accepted 129.48 mixed actions/sec with
  commit p95 2.468 ms, 9.257 transactions/action, and 28.251 statement
  calls/action against PostgreSQL 16.11 from
  `tests/fixtures/postgres.compose.yml`.

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
- `@durust/examples` also includes a compile-checked fanout example that uses
  manifest-backed `activityMap` and `childWorkflowMap` together, decodes ordered
  result manifests, and runs against the memory backend.
- `@durust/examples` includes a compile-checked approval example that combines
  `signal`, durable `sleep`, deterministic `select`, query projection
  publishing, typed `Client.sendSignal`, and typed `WorkflowHandle.query()`.
- `@durust/examples` includes a compile-checked versioning example that combines
  `patched` version markers with `continueAsNew`, showing the compact first run
  and completed continued run through public `Worker` and `Client` paths.
- The versioning examples also cover explicit numeric `getVersion(...)`
  markers and `deprecatePatch(...)` bridge markers through public `Worker` and
  `Client` paths.
- `@durust/examples` includes a compile-checked control-flow example that
  combines `join`, `joinAll`, `selectAll`, `sideEffect`, activity fan-in,
  signal delivery, and durable timers through public `Worker` and `Client`
  paths.
- `@durust/examples` includes a compile-checked retry example that runs
  provider-owned activity retry backoff through public `Worker` and `Client`
  paths, proves the delayed retry is not claimable early, and verifies parent
  history stays compact without an intermediate `ActivityFailed` event.
- `@durust/examples` includes a compile-checked heartbeat example that calls
  activity-side `heartbeat()` through the public worker activity context,
  verifies the provider records the heartbeat, and verifies parent history stays
  compact.
- `@durust/examples` includes a compile-checked payload-offload example that
  wraps `MemoryBackend` in `PayloadBackend`, stores oversized workflow/activity
  payloads through `LocalDirectoryBlobStore`, and verifies raw history stores
  blob refs while public workflow/activity code still sees typed values.
- `@durust/examples` includes a compile-checked parent-close-policy example
  showing child workflow `Cancel` versus `Abandon` behavior through public
  `Worker` and `Client` paths.
- Root Vitest coverage runs the checkout, approval, fanout, versioning,
  control-flow, retry, heartbeat, payload-offload, and parent-close-policy
  examples end to end, so examples are not only type-checked.
- Root `npm run check` now includes `npm run package:dry-run`, which runs
  `npm pack --dry-run --json` against the publishable TypeScript workspace
  packages using an isolated temp npm cache. The validator fails if artifacts
  include source, tests, lockfiles, hidden build metadata such as
  `.tsbuildinfo`, or files outside the intended built `dist` JS/declaration
  artifact set. The validator also requires release metadata on every
  publish-surface package: MIT license, repository type `git`, and the Durust
  GitHub repository URL. The private `@durust/examples` package is intentionally
  skipped because it has no publish surface.
- `typescript/README.md` now documents the TypeScript package layout, Node/npm
  runtime floor, forced single named object input rule, worker/client usage,
  deterministic workflow restrictions, payload offload, provider status,
  benchmark commands, and current production-readiness gaps.
- `typescript/README.md` now also includes migration and production-readiness
  checklists for TypeScript users. The migration checklist covers stable
  durable names, forced single named object inputs, application-owned schema
  compatibility, durable replacements for nondeterministic workflow APIs,
  manifest review, provider selection, payload offload, and required test
  coverage. The production checklist names the release gates for `npm run
  check:release`, `npm run check`, `npm run test:soak`, env-gated Postgres
  checks, conformance, persistent recovery, determinism enforcement, payload GC,
  worker deployment configuration, accepted benchmark baselines, normalized
  Postgres storage, and production-length hot-cache soak coverage.
- The workspace and publishable package metadata now declare a conservative
  Node `>=24.0.0` runtime floor; the root workspace declares npm `>=11.0.0`
  and `packageManager: npm@11.12.1`.
- The root workspace now exposes `npm run lint` as the release-gate lint command
  and wires `npm run check` through that public alias. `npm run lint` delegates
  to the deterministic workflow-source scanner, so the acceptance gate command
  named in this plan exists directly.
- The root workspace now exposes `npm run test:soak` as the opt-in
  release-candidate soak command for the worker hot execution cache. The soak is
  skipped in ordinary Vitest runs unless `DURUST_LONG_SOAK=1` is set, and
  exposes environment variables for seed count, workflow count, worker-driver
  generations, step budgets, and injected conflict count.
- The root workspace now exposes `npm run check:release` as the aggregate
  pre-publish gate. It runs `npm run check`, `npm run check:fixtures`,
  `npm run test:soak`, and `npm run check:postgres`, fails fast when
  `DURUST_POSTGRES_URL` is missing, and supports
  `node scripts/check-release.mjs --dry-run` for local command-list
  verification.
- Vitest release-script coverage now verifies `check-release.mjs --dry-run`
  prints the aggregate command list without requiring Postgres, and verifies
  both aggregate and standalone Postgres gates fail before running provider
  work when `DURUST_POSTGRES_URL` is absent.

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
- Deterministic crash/restart simulations now cover a workflow worker that
  claims a workflow task and crashes before commit, plus an activity worker that
  claims an activity task and crashes before completion. Both cases use an
  injected provider clock to force lease expiry, verify replacement workers can
  recover progress, and verify stale old claims remain fenced.
- Deterministic replay-first fault soak coverage now combines stale workflow
  claim fencing, stale activity claim fencing, replacement worker recovery,
  duplicate idempotent signal delivery, child workflow completion, timer firing,
  and final activity completion in one seeded mixed workflow run. This exercises
  the current replay-first worker path without pretending that the TypeScript
  runtime has a workflow cache layer.
- Worker-level replay-history cache coverage now proves cache hits reduce
  backend history streaming for partial claim prefetch, and proves bounded cache
  eviction falls back to streaming instead of weakening replay correctness.
- SQLite persistent-provider recovery tests now replay a mixed activity,
  signal, timer, and second-activity workflow across repeated close/reopen
  boundaries, verifying durable history and active indexes remain consistent.
- Postgres persistent-provider recovery tests now replay the same mixed
  activity, signal, timer, and second-activity workflow across repeated
  close/reopen boundaries, verifying normalized history and projections stay
  consistent across process boundaries.
- SQLite child recovery tests cover repeated close/reopen boundaries around the
  current synchronous provider-owned child start model: parent child-start
  commit, child execution, parent notification, and parent completion.
- Additional seeded simulations cover bounded materialization for activity maps
  and compact parent history for child workflow maps.
- Provider conformance now covers expired workflow/activity lease reclamation
  and stale old-claim fencing across memory, SQLite, Postgres, and
  payload-wrapped providers; SQLite persistence tests also cover reclaiming
  expired leases after close/reopen.
- Provider conformance covers normal activity retry rescheduling and terminal
  retry failure accounting across memory, SQLite, Postgres, and payload-wrapped
  providers; a deterministic memory test covers retry backoff timing.
- Provider conformance covers normal activity start-to-close and heartbeat
  timeout maintenance, heartbeat recording, stale heartbeat fencing, timeout
  retry rescheduling, workflow wakeup, compact `ActivityTimedOut` history, and
  late completion/heartbeat fencing across memory, SQLite, Postgres, and
  payload-wrapped providers. SQLite and Postgres persistence tests also verify
  refreshed heartbeat deadlines survive close/reopen boundaries.
- Provider conformance covers activity-map item retry rescheduling and terminal
  compact map failure accounting across memory, SQLite, Postgres, and
  payload-wrapped providers.
- Payload-store outage recovery is covered with a deterministic worker-level
  test that fails blob hydration, advances lease time, and then completes the
  workflow after storage recovery.
- The simulation driver records seed traces and uses deterministic virtual time;
  bounded cache-eviction soak now covers concurrent mixed workflows with
  truncated prefetch, replay streaming, duplicate idempotent signals, child
  workflows, timers, and activities. Hot execution cache fault soak now runs
  multiple deterministic seeds with multiple simulated worker restarts, commit
  conflicts, cache evictions, truncated prefetch, duplicate signals, child
  workflows, timers, and compact history assertions. An opt-in
  `npm run test:soak` profile now scales that scenario beyond the fast default
  suite for release-candidate burn-in. Remaining fault-simulation work is to
  run and record that profile at release scale, plus add any provider-specific
  outage scenarios that need their own long soak rather than focused recovery
  tests.

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

- the aggregate `npm run check:release` gate passes with `DURUST_POSTGRES_URL`
  pointed at the supported Postgres test database;
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
- exact package manager and Node LTS floor: current decision is npm with
  `packageManager: npm@11.12.1`, npm `>=11.0.0`, and Node `>=24.0.0`;
- whether the first SQLite provider uses Node's built-in SQLite module, a native
  dependency, or an async wrapper: current decision is Node's built-in
  `node:sqlite` `DatabaseSync` with a Node 24+ floor;
- whether TypeScript manifest schema metadata uses a required user-supplied
  schema library, optional adapters, or opaque type names plus codec
  fingerprints for v1: current decision is optional schema adapters plus codec
  and schema-fingerprint metadata, with no globally required schema library;
- exact TypeScript public call shapes for activity, child workflow, activity
  map, child workflow map, `select`, `join`, `joinAll`, and `selectAll`, with
  semantic mapping tests against Rust fixtures before implementation expands;
- whether cross-language workers may intentionally share one namespace in the
  same database for v1, or whether that is deferred until schema compatibility
  tests prove it.
