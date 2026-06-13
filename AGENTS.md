# AGENTS.md

This project uses a "build it like NASA" workflow: small modules, explicit invariants, deterministic behavior, exhaustive simulation, provider conformance, and performance regression gates before broadening scope.

Durust is durable execution infrastructure. Correctness failures can duplicate external work, lose workflow progress, corrupt replay, or make recovery impossible. Treat every module as safety-critical until deterministic tests, fault simulations, and conformance checks make it boring.

## Required Reading

Before changing code, read:

1. `README.md`
2. `SPEC.md`
3. `impl-plan/README.md`
4. The active `impl-plan/*.md` item for the work you are doing
5. The module you are about to touch and its tests

If `README.md`, `SPEC.md`, `impl-plan/`, and implementation behavior disagree, update the docs or ask for direction before adding code. The README is the target DX snapshot. The spec is the design contract. `impl-plan/` is the build plan.

## Operating Rules

- Build in the order defined by `impl-plan/`.
- Do not skip ahead to integration polish while earlier correctness gates are missing.
- Keep workflow replay deterministic: no wall-clock reads, process-global randomness, nondeterministic polling order, hidden I/O, or native async scheduling in workflow execution paths.
- Use durable APIs for workflow time, sleeps, select, activities, side effects, child workflows, and version markers.
- Keep providers behind the `DurableBackend` contract. Do not let SQLite-specific behavior shape runtime semantics.
- Treat append history as authoritative. Derived indexes, caches, ready queues, leases, query projections, and map item state must be rebuildable or reconcilable from durable facts where the spec requires it.
- Write docs, plans, and comments as the chosen end-state. Do not use "V1", "temporary", "for now", "later", or similar phrasing to excuse incomplete design. If scope is intentionally deferred, put that scope boundary in `impl-plan/`, not in the design artifact.
- Prefer the smallest production-quality implementation that satisfies the current phase and its tests.
- Add abstraction only when deterministic tests, conformance gaps, benchmark evidence, or real duplication proves the need.
- Treat performance as a gate after correctness, not as permission to complicate unproven code.

## Test Coverage Requirements

Every meaningful change needs comprehensive deterministic coverage for the behavior it touches.

Required coverage by default:

- Unit tests for local invariants and edge cases.
- Table-driven tests for normal behavior and boundary conditions.
- Deterministic replay tests for command-producing workflow behavior.
- Fault tests for stale, duplicate, delayed, reordered, and concurrently committed events when relevant.
- Provider conformance tests for every `DurableBackend` behavior change.
- Deterministic simulation tests for modules that own scheduling, leases, retries, timers, recovery, activity maps, or state transitions.
- Regression tests for every bug fix, added before or alongside the fix.

If an edge case is known, plausible, or mentioned in `SPEC.md`, it should have a deterministic test.

## Deterministic Simulation

Simulation is core infrastructure, not an afterthought.

- Use seeded schedulers and virtual time.
- Print or record failing seeds and minimized traces.
- Simulate worker crashes, cache eviction, commit conflicts, stale leases, duplicate completions, timer races, signal races, delayed delivery, and reordered delivery.
- Simulate real network and storage latency where relevant to behavior: queue claim delay, activity completion delay, timer service delay, provider write latency, blob store latency, and worker heartbeat jitter.
- Keep latency simulation deterministic by deriving delays from seed, explicit trace input, or scripted profiles.
- Prefer small model checks and focused scenario simulations over giant opaque integration tests.

Any new concurrency, provider, recovery, or activity map feature should add simulation scenarios before release.

## Provider Conformance

All durability providers must pass the shared conformance suite.

Provider work is not complete until conformance covers:

- Workflow start idempotency and conflict behavior.
- Workflow and activity claim lease fencing.
- Queue and registered type/name matching.
- Bounded history streaming with tail watermarks.
- Stale workflow task commit rejection.
- Ordered event ids per run.
- Signal inbox idempotency and atomic consumption.
- Timer wake indexes without history scanning.
- Activity completion idempotency and stale lease rejection.
- Activity map descriptor creation, item materialization, retry, and `max_in_flight`.
- Child outbox idempotency and parent close policy.
- Query projection consistency.
- Inline and blob-backed payload equivalence.
- Restart recovery from append history.
- Terminal workflow command rejection.

SQLite tests must close and reopen the provider when testing persistence. In-memory tests are useful, but they do not prove durability.

## Performance Discipline

Durust has explicit throughput goals. Hot paths need benchmarks and regression tracking.

Use Criterion for performance tests that matter to runtime behavior:

- Workflow task commit overhead.
- History append and stream throughput.
- Replay throughput over small and large histories.
- Cached workflow wake/poll overhead.
- Activity claim/complete throughput.
- Signal send/consume throughput.
- Timer due scanning/wakeup throughput.
- Activity map scheduling, item materialization, and completion throughput.
- Payload inline versus blob-ref overhead.
- SQLite provider throughput for local/test baseline.

Criterion benchmarks should use stable names and checked-in baselines where practical. Regressions in hot paths need an explanation, a fix, or an explicit acceptance in the change summary.

Benchmark realistic profiles too:

- Warm cached workflow happy path.
- Recovery after worker crash.
- Local activity preferred.
- Remote activity only.
- Signal-heavy and timer-heavy workflows.
- Child workflow fanout.
- Manifest-backed activity map fanout.
- Payload refs without DB row inflation.

## Module Exit Criteria

A module is not done until it has:

- A narrow public API with documented invariants.
- Deterministic tests for normal behavior.
- Edge-case tests for every known or plausible failure mode.
- Fault/race tests for stale, duplicate, delayed, reordered, and concurrent events when relevant.
- Simulation coverage when it owns state transitions, leases, retries, timers, recovery, or scheduling.
- Provider conformance coverage when it touches backend behavior.
- Criterion benchmarks when it is on a hot path.
- No hidden I/O, global randomness, wall-clock reads, or nondeterministic scheduling in deterministic workflow/replay code.

## Change Discipline

- Keep changes scoped to the current `impl-plan` item and the user request.
- Do not introduce production behavior before its deterministic model exists.
- Do not add provider-specific shortcuts to runtime logic.
- Do not weaken replay, idempotency, fencing, or ordering invariants for convenience.
- Do not add unbounded in-memory collections to workflow, replay, provider, or activity map hot paths.
- Do not store large payloads inline when the provider payload layer should produce a blob `PayloadRef`.
- If a plan item cannot meet its gate, update the relevant `impl-plan/*.md` file with the blocker and the next smallest proof step. Update `SPEC.md` only when the design changes.

## Done Means

"Done" means the module can be trusted in isolation: invariants are explicit, behavior is deterministic, failure modes are simulated, providers pass conformance, and hot-path costs are measured when relevant.
