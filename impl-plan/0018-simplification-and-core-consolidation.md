# 0018: Simplification and Core Consolidation (Rust and TypeScript)

Follow-up: [0019](0019-durability-and-recovery-boundaries.md) records the
2026-09-16 review. Its GC/publication proof gates Phase 3B, and its atomic
memory-commit work is the next bounded step before Phase 4A–4D. It also revisits
the full-history marker fallback accepted in 1A. The remaining consolidation
work must preserve these guarantees rather than replacing transactional
protection with a timestamp grace period.

## Overarching Goal

Make Durust smaller without giving up a guarantee. A full review on
2026-09-15 (six independent reviewers, coordinator-verified where a finding
anchors a phase) found that the codebase carries the same engine many times:
the workflow-task commit, fencing, terminal cleanup, timer and timeout scans,
activity completion, child dispatch, and payload traversal exist once per
provider in each language, and the TypeScript providers are a second copy of
the Rust ones behind a different schema. Of 27,485 non-test TypeScript source
lines, 20,533 re-implement logic Rust already has; of 18,864 non-test lines in
the three Rust providers, roughly 11,900 are engine logic and 3,900 are payload
walkers, each written three times.

The duplication is not free. The review reproduced defects that live only in
one copy: a Rust run that a second `patched` call for the same change id kills
permanently; a TypeScript `select` or `join` with a signal branch that wedges on
any cold replay; a TypeScript Postgres fast path that appends activity results
after `WorkflowCompleted` and leaves cancel-policy grandchildren running; a
Rust Postgres batch commit that can persist half of an item; and a Rust payload
decorator that fails every claim on a sharded Postgres deployment.

Outcome: every defect above is fixed with a revert-verified test in the runtime
that has it, and both runtimes commit the same history for the new corpus
cases. Engine logic exists once per language, over storage primitives per
provider. Payload offload has one mechanism. The Rust/TypeScript split is
decided by a measured proof of concept rather than by the sentence in
`typescript/README.md`; the recommended direction is one Rust durability core
that TypeScript reaches through a native binding, which retires the TypeScript
providers, payload package, map engine, and most of the conformance package.
Test, benchmark, fixture, and CI infrastructure shrink to what gates something.

Non-goals: new workflow features, the Postgres shard-native roadmap (item
0013), moving the worker loop itself out of TypeScript (recorded as a decision
for after the binding is measured), and any change to map semantics. Public
APIs change only where this plan names a replacement that is at least as good.

## Implementation Principles

- Correctness before deletion. Phase 1 lands before any refactor that touches
  the same code, and every defect fix ships with a test shown to fail on revert.
- Delete the second design rather than unify it. Where two mechanisms do one
  job (provider-owned blob stores beside the payload decorator; TypeScript
  providers beside Rust providers; the TypeScript Postgres load-and-rewrite
  tier beside its SQL-native paths) the plan keeps one and removes the other.
- Engine logic lives once per language. Storage primitives live once per
  provider. `map_engine` / `map-engine.ts` (pure `(state, event) -> effects`)
  is the template for the rest of the engine.
- The commit is the contract (`SPEC.md` §1.2). Every refactor is gated by the
  behavioral corpus and the conformance suites producing byte-identical commits
  before and after.
- Measure on one machine, before and after, with the existing Criterion and
  `packages/benchmark` profiles. A regression needs an explanation or an
  explicit acceptance row.
- History-format changes are bundled into one recorded pre-1.0 breaking
  release (Phase 5 item 5G), never sprinkled across phases.
- Public API discipline per `AGENTS.md`: each new surface (native package,
  `workflowOutcome`, payload hooks, typed provider errors) records its budget.
- Deterministic drivers keep their shape: `run_until_idle` and the one-shot
  TypeScript drivers stay sequential and single-pass.

## Testing Strategy

- Rust: `cargo test --locked --workspace --all-features`. Baseline on
  2026-09-15 at `3bfaf75`: 16 binaries, 424 passed, 0 failed, 1 ignored, with
  Postgres cases skipped because no server was listening; 457 passed with
  Postgres. After the 2026-09-15 implementation pass and review: 470 passed, 0 failed,
  1 ignored with Postgres, and `cargo clippy --all-targets -D warnings
  -A clippy::result_large_err` clean. Every phase reports the same command
  with `DURUST_POSTGRES_URL` set.
- TypeScript: `npm run check` in `typescript/`. Baseline: 34 files, 655 passed,
  133 skipped (Postgres-gated); 786 passed with Postgres. After the pass and review: 838
  passed, 1 skipped, 1 failed (`thresholds.test.ts`'s Postgres case, which
  needs `pg_stat_statements` preloaded at server start; item 8B), with the
  type, lint, fixture, and package steps green. Every phase reports it with
  Postgres.
- Provider conformance is the oracle for every provider change: the 51 Rust
  scenarios through `run_conformance_scenarios!` on memory, SQLite (close and
  reopen), and Postgres; the 54 TypeScript cases through `@durust/testing` on
  all three providers.
- The behavioral corpus (`typescript/fixtures/contract/behavioral-corpus.json`)
  is the cross-runtime gate. Phase 1 widens it to SQLite-backed runs and to the
  defect cases; after that, both runners must stay identical in every phase.
- Seeded simulation (`tests/sim_worker.rs`,
  `packages/core/test/simulation.test.ts`) for every loop, lease, or recovery
  change, including the fencing race Phase 1 adds.
- Mutation checks: every new regression test is reverted against its fix once
  and the failure recorded in the ledger.
- Benchmarks: Criterion (`benches/replay_core.rs`) and `packages/benchmark`
  medians on one machine before and after each performance phase, with the
  numbers in the ledger.

> Evidence note. Rows whose evidence begins with "Reproduced" were rerun by
> the coordinator on 2026-09-15 (scratch probes under the session scratchpad,
> no repo changes). Rows citing only reviewer measurements say so; the first
> step of those items is to confirm the measurement in-repo.

---

## Phase 1: Defects the review reproduced (both runtimes)

Goal:
No workflow program can be killed or wedged by a supported idiom, no provider
appends past a terminal event or loses a commit to a concurrent scan, and the
corpus pins each fixed behavior in both runtimes.

Scope:
- 1A Rust: a second `patched` / `get_version` call for a change id already
  recorded poisons the run. `src/runtime.rs:1314-1327` consults the change
  index whenever the cursor is not on a marker; `preconsume_marker`
  (`:1427-1432`) rejects any marker at or before the loaded cursor, and the
  matched marker stays in the index (`index_from_records` `:448-456`,
  `merge_change_markers_from_history` `src/worker.rs:2852-2879`). TypeScript
  records one `VersionMarker` per call and completes. Fix: match in-window
  markers positionally and consult the index only for markers beyond
  `last_loaded_event_id`, so Rust commits the same history TypeScript does.
- 1B TypeScript: `SignalConsumed` is matched positionally (`runtime.ts:1814-1830`,
  `isReplayCommandEvent` `:230-252`) while Rust indexes it by command seq
  (`src/runtime.rs:248`, `:321-324`, `:1136`). A `select` or `join` whose signal
  branch registers before a timer wedges on cold replay. Fix: index
  `SignalConsumed` by command key with the other ready events.
- 1C Rust `PayloadBackend` forwards the four batch methods it omits
  (`claim_workflow_tasks`, `commit_workflow_tasks`, `claim_activity_tasks`,
  `run_due_maintenance`; `src/payload_backend.rs:117-488`). Postgres overrides
  all four (`src/postgres.rs:1082`, `:1145`, `:1202`, `:1219`), the worker calls
  all four (`src/worker.rs:1395`, `:1620`, `:2166`, `:2233`), and the trait
  default errors on any shard filter (`src/backend.rs:36-40`). TypeScript's
  `@durust/payload` omits the two optional batch methods and silently downgrades
  (`worker.ts:654`, `:1547`); forward them too.
- 1D Rust Postgres batch commit: non-simple items run through the scalar applier
  inside the batch transaction with no savepoint (`src/postgres.rs:2144-2170`;
  savepoints exist only on the activity path `:4119-4150`), and the applier has
  non-`Backend` exits after history and activity rows are written (`:2884`,
  `:2939-2960`, `:7445`, `:7449`). Fix: savepoint per fallback item, as the
  activity batch already does.
- 1E Rust Postgres `signal_workflow` and `start_workflow` check-then-insert
  without a conflict clause (`:3182-3227`, `:1337-1360`); a concurrent duplicate
  raises 23505, which `postgres_error` maps to `Error::Backend` and the retry
  predicate (`:6003-6009`) does not retry. Fix: `insert ... on conflict do nothing
  returning`, mapping no-row to `Duplicate` / `AlreadyStarted`.
- 1F Commit-side `consume_signals` and `delete_waits` act by id alone in all
  three Rust providers (`src/memory.rs:845-849`, `src/sqlite.rs:769-780`,
  `src/postgres.rs:3065-3080`, batch `:7047-7094`) and `upsert_waits` trusts
  `wait.run_id`. Fence all three to the claimed run; audit the TypeScript
  providers for the same shape.
- 1G TypeScript Postgres fast-path terminal commit
  (`packages/postgres/src/index.ts:1162`, `:1167`, `~:1240`) deletes waits and
  nothing else: a late plain-activity completion is accepted and appended after
  `WorkflowCompleted`, and a closing child does not cancel its own
  `Cancel`-policy children. Fix: tombstone live plain activities on terminal and
  cancel children in SQL; correct the comment at `:2733`.
- 1H TypeScript Postgres load-and-rewrite tier races the SQL-native path:
  `timeoutDueActivities` (`:2082`, scope `:313-320`) reloads and rewrites
  `workflow_runs` and `activity_tasks` on every scan with only counters locked
  (`:2909`), so a concurrent `completeActivity` loses its tail (`:5751`) and
  the run's next event is dropped by `on conflict do nothing` (`:3244`).
  Minimal fix here: lock the rows the tier rewrites and skip the rewrite when
  the scan changes nothing; Phase 7 removes the tier.
- 1I TypeScript providers accept signals to closed runs
  (`backend.ts:1015-1035`, `sqlite:992-1022`, `postgres:2142-2212`); Rust
  returns `TerminalWorkflow` (`src/memory.rs:961`, `src/sqlite.rs:945`,
  `src/postgres.rs:3216`) per `SPEC.md` §19.1.
- 1J TypeScript providers ignore `maxBytes` on history streams
  (`backend.ts:184`); Rust honors it (`src/sqlite.rs:224-253`).
- 1K Both simulations pass with stale-commit fencing removed (0017 line 875).
  Add the race only fencing can reject: A claims, the lease expires, B claims
  and commits, A commits with a correct tail and must get `StaleLease`.
- 1L Corpus: run every case on SQLite as well as memory; add a failure-and-retry
  program; add cases for 1A ("two patched calls, one id, across a task
  boundary") and 1B ("signal wins the select, a later task cold-replays").
- 1M Decision: converge the two specified divergences in `SPEC.md` §1.2 in the
  direction `PARITY.md` §3 already records (TypeScript adopts Rust's
  release-and-retry for generic workflow bugs and for output-conversion
  re-entrancy), then delete the carve-out paragraphs. Open in 0017 at lines
  199 and 211.

Out of scope:
- Performance work on the TypeScript SQLite and Postgres providers beyond the
  minimal race fix in 1H (Phase 7 decides whether those providers survive).

Completion gate:
Every item above has a test in the runtime that had the defect, shown to fail
with the fix reverted; the corpus contains the 1L cases and both runners produce
identical commits; `cargo test --all-features` and `npm run check` pass with
Postgres available.

Testing plan:
- Rust replay tests for 1A (cached and cold, marker before and after an
  activity); TypeScript `runtime.test.ts` and `worker.test.ts` cases promoted
  from the 1B probes (select-first, join-first, cache size 0 and 1024).
- Conformance: 1C `PayloadBackend<PostgresBackend, MemoryBlobStore>` with a
  shard filter and prefetch above one; 1D a batch with one valid commit and one
  map with `max_in_flight = 0`; 1E two tasks racing one `signal_id` and one
  `workflow_id`; 1F a commit naming another run's signal and wait; 1G "closing
  a run tombstones its live plain activity" and "a closing child cancels its
  Cancel-policy children"; 1H the timeout-scan-versus-completion race; 1I
  "signal to a closed run is rejected"; 1J `maxBytes` watermark. Each shared
  case runs on all three providers in its language.
- Simulation: 1K in `tests/sim_worker.rs` and `simulation.test.ts`.
- `PARITY.md` gains rows for 1A, 1B, 1I, 1J, and 1K.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Rust repeated `patched` preserves positional markers | Superseded by 0019 Phase 5: awaited marker APIs use the ordinary replay gate in both runtimes. Unlimited reload/reserve paths are removed. Repeated-id replay and the shared behavioral corpus preserve recorded command semantics. |
| Complete | Work | 1B: TypeScript signal branch cannot cold-replay | Landed: `SignalConsumed` leaves the replay window (`isReplayCommandEvent`) and is indexed by command key (`#signalConsumptions`, `#takeRecordedSignalConsumption`) in `runtime.ts`; `resolveSignal` and `resolveHotSignalBranch` consult the index before live signals, as Rust does. Tests: `worker.test.ts`, `signal branches replay by command id` (select signal-first, select timer-first, join signal-first × cache 1024 and 0); revert-verified: the two cold signal-first cases fail with the positional match restored. Corpus case `a signal that won a select whose timer registered later replays cold by command id`. |
| Complete | Work | 1C: payload decorators forward batch methods | Landed: `src/payload_backend.rs` forwards `claim_workflow_tasks`, `commit_workflow_tasks` (offloading each item), `claim_activity_tasks` (hydrating each task), and `run_due_maintenance`; `@durust/payload`'s `PayloadBackend` exposes `claimWorkflowTasks`/`claimActivityTasks` exactly when the wrapped provider does, hydrating their results. Tests: `tests/provider_conformance.rs::payload_backend_forwards_batch_claims_commits_and_maintenance`, `::payload_backend_forwards_shard_filtered_batch_claims_to_postgres` (revert-verified: fails with `workflow task shard filters require a shard-aware backend`); `packages/payload/test/batch-forwarding.test.ts` (2 cases). |
| Complete | Work | 1D: Postgres batch commit savepoints | Landed: `commit_workflow_tasks_once` wraps each scalar-path item in `savepoint durust_workflow_commit_item`, rolling back per-item errors and aborting the batch on `Error::Backend`. Test: `tests/provider_conformance.rs::postgres_batch_commit_rolls_back_a_failed_item_and_keeps_its_neighbor` (a map with `max_in_flight = 0` beside a valid timer commit); revert-verified: `left: [WorkflowStarted, ActivityMapScheduled]`. |
| Complete | Work | 1E: Postgres start/signal insert with conflict clause | Landed: `start_workflow_once` inserts `on conflict (namespace, workflow_id) do nothing` and answers `AlreadyStarted` with the winner's run id on zero rows; `signal_workflow_once` inserts `on conflict (signal_id) do nothing` and answers `Duplicate`. Test: `::postgres_concurrent_duplicate_starts_and_signals_resolve_without_error` (16 rounds of `futures::join!` pairs); revert-verified: `SQLSTATE 23505 … signals_pkey` on round 0. |
| Complete | Work | 1F: fence commit-side signal and wait mutations to the claim | Landed in all six providers: a wait naming another run is skipped, and `consume_signals`/`delete_waits` act only on rows of the claimed run (Rust `src/memory.rs`, `src/sqlite.rs`, `src/postgres.rs` scalar and batch paths with `unnest` pairs; TypeScript `backend.ts`, `sqlite/src/index.ts`, `postgres/src/index.ts` fast and loaded-state paths). Shared scenario `commit_side_signal_and_wait_mutations_are_fenced_to_the_claimed_run` (`CONFORMANCE_SCENARIOS` 51 → 52) and TypeScript shared case `commit-side signal and wait mutations are fenced to the claimed run`; revert-verified on all three providers in each language (`run b's signal must survive run a's commit`). The stray-wait cases were rewritten for the fence (`a_fenced_stray_wait_leaves_the_scan_budget_to_the_live_run`); `SPEC.md` §8.2 states the fence. |
| Complete | Work | 1G: TypeScript Postgres terminal fast path | Landed: `#commitWorkflowTaskSqlNative` hands a closing run to the loaded-state path when it has a live plain activity (`#hasLivePlainActivities`) or any `Cancel`-policy child (the `parentLink === null` condition is gone). Shared cases `closing a run tombstones its live plain activity` and `a closing child cancels its own Cancel-policy children` on all three providers; revert-verified: both fail on the old Postgres provider only, which matches the diagnosis. |
| In Progress | Work | 1H: TypeScript Postgres tier race | Landed: `timeoutDueActivities` selects `noRewriteScope` when it timed nothing out, so the steady-state scan no longer rewrites `workflow_runs` and `activity_tasks` from state loaded under only the counters lock. Missing: the due case still rewrites over a concurrent SQL-native commit; a full-table `for update` in the tier's load was rejected here because the tier and the native paths acquire row locks in different orders and would deadlock, so the remaining race is Phase 7's (tier deletion on the go path, 7G on the no-go path). No deterministic reproduction exists; the reviewer's race is recorded. |
| Complete | Work | 1I: reject signals to closed runs in TypeScript | Landed: all three providers throw `terminal workflow rejects signals` after resolving the run. Shared case `a signal to a closed run is rejected`; revert-verified on all three old providers. PARITY row 28. |
| Complete | Work | 1J: honor `maxBytes` in TypeScript providers | Landed: `boundHistoryChunk` and `historyEventPayloadBytes` in `provider-util.ts` apply Rust's rule (first event always fits, stop before the event that would exceed either bound); all three `streamHistory` bodies use it. Shared case `stream history honors maxBytes with at least one event per chunk`; revert-verified on all three old providers. PARITY row 29. |
| Complete | Test | 1K: fencing race in both simulations | Landed: `tests/sim_worker.rs::expired_lease_correct_tail_commit_is_fenced` (A claims, the lease lapses, B reclaims, A's late commit with the current tail must get `StaleLease` while B holds the claim); `simulation.test.ts`, `fences a late commit whose tail is still current once its lease was reclaimed`. Revert-verified: disabling the memory provider's token check fails each (`Ok(Committed { new_tail_event_id: EventId(2) })`; `promise resolved instead of rejecting`). PARITY row 30. |
| In Progress | Test | 1L: corpus widened | Landed: three programs in both runners and three regenerated cases (`corpus.repeated-change-id`, `corpus.signal-first-select-then-timer`, `corpus.fails`; `DECLARED_CASES` 13 → 16); the `expect` blocks are Rust's and TypeScript reproduced them. The "Workflow failure and cancellation events" gap narrowed to cancellation. Review addition: `corpus.now-twice` pins `now()` across both runtimes (`DECLARED_CASES` 17). Missing: SQLite-backed corpus runs and an activity-retry program (the "Activity retries" and "Providers other than the two in-memory ones" gaps stay declared). |
| Complete | Decision | 1M: converge the two specified divergences | Decided and landed: a run fails terminally only through a durable failure (Rust `Err(...)`; TypeScript `WorkflowFailure`, a propagated activity or child failure, or a `DurableFailure`-shaped value); any other rejection is a workflow-code fault that fails the task without committing (Rust `Error::TaskPanic`; TypeScript `WorkflowCodeError`, released with the nondeterminism backoff). Rust now encodes workflow output under the context borrow (`encode_workflow_output`), so output-conversion re-entrancy fails the task in both runtimes. API budget: `@durust/core` exports `WorkflowFailure` and `WorkflowCodeError`; Rust gains `WorkerMetrics::workflow_tasks_faulted`. Review fix: Rust `PayloadEncode`/`PayloadDecode` returned by a durable API for the workflow's own values now fail the task without committing, as the TypeScript twin already did (`tests/replay_core.rs::durable_api_refusing_a_value_fails_the_task_without_committing`, revert-verified); `SPEC.md` §4.2 and `PARITY.md` note 5 name the rule. Tests: `worker.test.ts`, `releases a workflow task whose handler threw a plain error and keeps the run`, `commits WorkflowFailed for a handler that throws a durable failure`; `runtime.test.ts` re-entrancy and validation cases; `tests/replay_core.rs::durable_call_from_output_serialize_fails_the_task_without_committing`; corpus case `a workflow that fails on purpose commits WorkflowFailed with its own failure`. Revert-verified on both sides (four TypeScript cases; the Rust case completes the run with the guard removed). `SPEC.md` §1.2, §4.2, §16 and `PARITY.md` rows 4, 5, notes 4, 5, §3 rewritten. |
| In Progress | Gate | All fixes revert-verified, corpus identical, both suites green with Postgres | `cargo test --locked --workspace --all-features` with `DURUST_POSTGRES_URL`: 470 passed, 0 failed, 1 ignored (baseline 457), clippy clean. `npm run check` with Postgres: 838 passed, 1 skipped, 1 failed (baseline 786 passed, 1 skipped, 1 failed); the one failure is `thresholds.test.ts` needing `pg_stat_statements` preloaded, an environment fact tracked under 8B; the type, lint, fixture, and package steps pass. Review 2026-09-15 (one sequential reviewer over the whole diff, read-only): three high findings, two confirmed and fixed (the budgeted overrun stall; the Rust `PayloadEncode`/`PayloadDecode` disposition), one refuted (the Postgres `liveSignals` query already selects the earliest record per name through `DISTINCT ON`; the new shared case `a claim carries the first unconsumed record of each signal name` pins it on all three providers); the `now()` corpus case and the leftover comments, the dead `registered_this_poll` parameter, and PARITY rows 8 and 27 were addressed. Missing: 1H's due-case lock (Phase 7) and 1L's SQLite runs and retry program. |

---

## Phase 2: Decide the Rust/TypeScript split by measurement

Goal:
Replace the standing assumption that TypeScript must re-implement the
durability layer with a measured decision. The recommended direction is one
Rust durability core (providers, payload offload, map engine, history encoding)
exposed to Node through a napi-rs binding, with TypeScript keeping only what
must be TypeScript: the promise-based execution mechanism, the worker loop, the
public API, the determinism guards, and the ESLint plugin.

Why this is the recommendation:
- `runtime.ts` contains no backend call; all 18 call sites are in `worker.ts`
  (14) and `api.ts` (4). The authoring model and the storage layer are already
  separated by the `DurableBackend` interface; the binding changes who
  implements the interface, not how workflows are written.
- The two implementations are already physically incompatible: Rust SQLite
  creates 14 tables and TypeScript 12 under different names; Rust Postgres is
  shard-aware (130 mentions) and TypeScript's is not (0); the corpus's own
  `exclusions` list says a history written by one runtime is not replayable by
  the other. Nothing is lost by having one storage format.
- The TypeScript providers are the weakest code in the repo (Phase 1 items
  1G-1J; Phase 7's quadratic SQLite history rewrite and the Postgres tier that
  reloads the whole database per heartbeat, measured by the reviewer at 6.35 ms
  rising to 195.7 ms after 400 unrelated runs).
- Parity today is a maintenance program: three vocabulary fixtures, a manually
  maintained 24-row ledger pinned to one commit, and conformance suites with no
  shared case list.

Scope:
- 2A Proof-of-concept crate `durust-node` (napi-rs) exposing `MemoryBackend`
  and `SqliteBackend` as a class that implements the TypeScript
  `DurableBackend` interface (19 methods), msgpack across the boundary, async
  through the binding's tokio runtime.
- 2B Drive the binding with unchanged tests: `sqlite-conformance.test.ts`,
  `memory-conformance.test.ts`, `behavioral-corpus.test.ts`, and the
  `packages/benchmark` `sqlite-mixed-local-4-worker` and `memory-mixed`
  profiles.
- 2C One benchmark runner on one machine comparing Rust `PostgresBackend`
  (1-shard and 100-shard) with TypeScript Postgres on the 10-worker, pool-24
  profile. The READMEs currently cite Rust 100-shard at 320.5 and TypeScript at
  192.8 processing workflows/s from different machines and commits, and the
  checked-in Rust Postgres baseline JSON is stale (91.9), so no comparison the
  repo can state exists today.
- 2D Distribution spike: `@napi-rs/cli` build matrix (linux x64 and arm64,
  glibc and musl; darwin x64 and arm64; win x64), optional platform packages,
  and a build-from-source fallback. Record what TypeScript SQLite users lose
  (the `node:sqlite` zero-native-dependency install) and gain (an unblocked
  event loop; `DatabaseSync` is synchronous today).
- 2D.1 Bundle SQLite in the distributed Node addon using rusqlite's existing
  `bundled` feature. Inspect built ELF/Mach-O dependencies in CI and every
  release platform job so developer-installed SQLite cannot mask a dependency.
- 2E Decision: go or no-go against the gate below, with the no-go fallback
  named (Phase 7 items 7F-7I). Record the B2 question (worker loop in Rust) as
  a separate decision to take after the binding has run in Phase 7.
- 2F API budget for `@durust/native`: what `pg.Pool` injection
  (`packages/postgres/src/index.ts:199`) becomes (connection string plus TLS
  and auth hooks), and which TypeScript `DurableBackend` methods the binding
  adds (`hydratePayload`, blob store operations, `mapEngine.step`).

Out of scope:
- Deleting any TypeScript package (Phase 7).
- Moving `worker.ts` or `runtime.ts` logic into Rust.

Completion gate:
A recorded decision backed by: the conformance and corpus suites green over the
binding; TypeScript SQLite throughput at or above today's 118.5 workflows/s
(4 workers) and memory within 20% of today's 1,650.7 workflows/s; activity
latency under load showing the event loop unblocked; per-commit binding
overhead measured below 5% of SQLite commit latency; the platform matrix
building; and the same-machine Postgres numbers from 2C.

Testing plan:
- The unchanged TypeScript conformance and corpus suites are the acceptance
  tests for the binding.
- `packages/benchmark` profiles before and after, same machine, in the ledger.
- `provider-io.json` and `core-events.json` become the binding's wire-format
  tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: `durust-node` proof of concept over memory and SQLite | `durust-node/` (napi-rs 3): `NativeBackend` with `memory()` and `sqlite(path)` factories, the 19 `DurableBackend` methods plus `claimWorkflowTasks`, `claimActivityTasks`, and `advanceTimeTo`, msgpack `Buffer`s in and out, `wire.rs` mirroring the TypeScript request and outcome shapes, an `Inner` enum dispatch because `DurableBackend` returns `impl Future`. `typescript/packages/native`: `NativeBackend implements DurableBackend` over `@msgpack/msgpack` and a `createRequire` loader, `NativeClockOptions.nowMs` (default `Date.now`) stamped into the Rust provider clock before every call, `scripts/build-native.mjs`. Wire-format tests: `tests/contract_fixtures.rs::rust_payload_refs_match_neutral_fixture_shapes` and `::rust_manifest_payload_fixtures_match_neutral_shape` read `core-events.json` through the serde shapes the binding sends. Artefact: 8,206,448 bytes release, 5,386,344 stripped. |
| Complete | Test | 2B: unchanged TypeScript suites pass over the binding | `packages/native/test/native-conformance.test.ts` runs all 50 `basicProviderConformanceCases()` over `NativeBackend.memory()` and `.sqlite()`: 47 pass per provider, 3 pinned as `it.fails` in `KNOWN_GAPS` (row "Rust-side gaps" below); a shape test fails if a gap names a case that no longer exists. `behavioral-corpus.test.ts` passes with `DURUST_CORPUS_BACKEND=native` (23 tests): the TypeScript worker runs every corpus case over the Rust memory provider and commits the fixture's history; `check-fixtures.mjs` runs that mode and CI builds the addon first. Benchmark profiles, same machine, two runs each (`--backend native-memory|native-sqlite` added to `packages/benchmark`): memory mixed 1000 workflows/4 workers, TypeScript 1711 and 1672 vs native 892 and 856 processing workflows/s; SQLite mixed 100/1, TypeScript 147 and 147 vs native 150 and 209; SQLite mixed 100/4, TypeScript 154 and 155 vs native 207 and 280. |
| Complete | Work | Rust core aligned to the pinned provider contract | Started at 21 of 50 cases per provider. Landed in the core: kind-tagged camelCase serde for `PayloadRef`, `EncryptionMetadata`, `DurableFailure`, the map manifests and `ChildWorkflowMapItemOutcome`, inline bytes through `serde_bytes`; one `RetryPolicy` model (`initial_interval_ms`, `max_interval_ms`, `max_attempts`, `backoff_coefficient`, `non_retryable_error_types`) with the shared delay formula, listed error types ending retries, and a plain activity's timed-out retry paced by it (`tests/provider_conformance.rs::memory_timeout_retry_becomes_visible_after_the_policy_backoff`); `WorkflowTaskRelease { delay }` keeping the run's wake reason; `QueryProjectionOutcome::NoProjection`; `ProviderClock` on `SqliteBackend`; start-to-close measured from the claim (the claim stamps `timeout_at`; a queued task and a waiting retry have none), which is the TypeScript rule and what lets a task whose queue wait outlasts its timeout be claimed safely; `start-to-close timed out` and `child workflow id already exists: <id>` messages. PARITY rows 32 to 36; SPEC §11 retry pacing, §8 release and query semantics, §14 stored payload shape; `cargo test --workspace --all-features` with Postgres green, clippy clean. |
| Complete | Work | Binding-side bridges the contract still needs | `durust-node/src/lib.rs`: `prefetched_history` filled from `stream_history` (256 events, 1 MiB) when the provider prefetched none; `payloadRoots` expands manifests into page and item refs (TypeScript providers expose those as objects, Rust decodes manifests during GC); `maxInFlight` validated with the TypeScript message; a superseded release reported as success. Each is a Phase 7 item to move into the core or the worker. The outbox drain the binding first ran after every commit went away with 4E: the memory and SQLite providers now start children inside the commit. |
| Complete | Decision | Rust-side gaps left open, pinned | `KNOWN_GAPS` in `native-conformance.test.ts` and PARITY §3: a claim never steals a held activity whose lease expired (Rust reclaims from the timeout scan only); an unknown activity id completes as `AlreadyCompleted` rather than `NotFound` (terminal cleanup deletes the row); a colliding child-map item is failed as it dispatches, before its batch-mate starts. Each is a Phase 7 decision, not a defect in either runtime. |
| Complete | Work | 2C: same-machine Postgres comparison with one runner | Local Docker `postgres:17-alpine` on port 5433 (default config, no `pg_stat_statements`), 1000 workflows, 10 workers, pool 24, one run each. Rust on the `benches/baselines/durust-mixed-postgres-100-shards.json` profile (16 partitions, activation concurrency 8, prefetch 32, completion batch 32): 100 shards 287.3, 1 shard 365.5 processing workflows/s. Rust on the defaults (concurrency 1, completion batch 1): 1 shard 47.8, 100 shards 43.3. TypeScript on its `postgres-mixed-accepted` profile (completion batch 32): 72.3 and 72.75. Rust's tuned profile runs 4 to 5 times TypeScript's on the same server, and 100 shards do not help on one physical database. Both workloads' JSON is in the session scratchpad, not checked in. |
| Complete | Work | 2D: distribution spike | Landed as the tinysandbox layout: `packages/native/scripts/build-native.mjs` runs `napi build --platform --release` against `durust-node/Cargo.toml` and places `durust-node.<target>.node` next to the package's `package.json`; `package.json` carries the `napi` targets, `optionalDependencies` on four `@durust/native-<target>` packages under `packages/native/npm/`, and `src/index.ts` resolves `DURUST_NATIVE_LIBRARY_PATH`, then the local build, then the platform package. `release.yml` builds on manylinux 2.34 x64 and arm64 containers and macOS x64 and arm64 runners, assembles the platform packages, and publishes them before the facade; `scripts/check-native-packages.mjs` pins the manifests to the facade in CI. |
| In Progress | Gate | 2D.1: addon SQLite portability | Release `35159960289` failed both Linux links with missing `-lsqlite3`; publish was skipped. `durust-node` enables rusqlite `bundled` without changing standalone Rust linkage. The dependency guard rejects the old addon and accepts a fresh release build in `manylinux_2_34_x86_64`; that artifact loads, creates SQLite storage, and reopens it in the container. With that artifact: full TypeScript check passes (546 tests, 1 optional S3 skip, type/lint/fixture/package checks); Rust provider conformance passes 86 tests with Postgres configured, and Clippy passes. Dedicated review found no issues. CI and all four release targets enforce the guard. Pending: hosted Linux arm64 and macOS release builds after merge. |
| Complete | Decision | 2E: go or no-go on the Rust core | **Go for all three providers.** The memory provider was first kept for throughput (TypeScript 1,700 workflows/s against 856 to 892 over the binding, one addon hop costing about 12 µs), then retired with the others in Phase 7: a test and simulation provider does not need that margin, and keeping it meant keeping the map engine and a second copy of every provider rule. Same-machine numbers after retirement are in `typescript/README.md`, "Current Benchmark Medians". |
| Complete | Decision | 2F: API budget for `@durust/native` | Constructors: `NativeBackend.memory({ nowMs? })` and `NativeBackend.sqlite(path, { nowMs? })` as built; for Phase 7 `NativeBackend.postgres({ url, schema?, poolSize?, tls?: { ca?, rejectUnauthorized? }, password?: () => Promise<string> })` in place of `pg.Pool` injection (`packages/postgres/src/index.ts:199`), because the Rust provider owns its `deadpool-postgres` pool and a `pg.Pool` cannot cross the boundary. Methods beyond `DurableBackend`: `advanceTimeTo(ms)` (built), `runDueMaintenance`, `dispatchChildWorkflowStarts` (the worker drains the outbox instead of the binding doing it per commit), `hydratePayload`, `gcPayloadBlobs`, `payloadRoots` returning `PayloadRootRef`s rather than expanded refs, and payload storage (`PayloadStorageConfig`: inline threshold, local or S3 blob store) as constructor options rather than a `PayloadBackend` wrapper. `mapEngine.step` stays inside the providers. The rest of the interface is unchanged. |
| Complete | Gate | Gate numbers recorded | Throughput, latency, overhead, corpus, conformance, and Postgres numbers are in the rows above; the platform matrix is planned, not run. |
| Complete | Review | Sequential review of Phases 2 to 8 | One reviewer, working alone, verified every finding by probe or trace: 8 findings, all fixed. High: the claim gate on lapsed start-to-close deadlines had been removed while the deadline was still stamped at schedule time, so a queued attempt could be claimed and then timed out under a running worker; now the claim stamps the deadline (the TypeScript rule), a queued attempt has none, and `tests/provider_conformance.rs::memory_start_to_close_deadline_starts_at_the_claim` pins it (revert-verified). Medium: SQLite's schedule-time stamp read the wall clock past `ProviderClock`; closed by the same change. Low: the retry-pacing doc comment; `MemoryBackend::advance_time_to` taking two locks; the binding folding every batch completion error into `NotFound` (now only a stale claim and a missing run are item outcomes, anything else fails the call); memory and SQLite returning a commit tail that predated the children the commit started (now re-read after the dispatch, as Postgres); `isProviderError` matching by class only (now by shape too); SQLite's history stream rejecting `u64::MAX` bounds (clamped). Sound: serde shapes, the retry model, release semantics, `NoProjection`, timer scans, the payload traversal, inline child starts, commit fencing, the binding, typed errors, and the rewritten tests. |

---

## Phase 3: One payload offload path in Rust

Goal:
Payload traversal exists once, offload has one mechanism, and a payload ref
costs what its bytes cost.

Scope:
- 3A One traversal. Eight hand-written matches over `HistoryEventData` walk
  payload slots today: `src/payload.rs:403` and `:555`, plus three copies each
  of `collect_history_event_payload_roots` (`src/memory.rs:3033`,
  `src/sqlite.rs:2108`, `src/postgres.rs:4905`) and
  `collect_history_event_payload_blobs` (`:3149`, `:2419`, `:4996`); the memory
  and SQLite copies differ only in a parameter name. Around them, each provider
  and the decorator re-implement the manifest page walk and reachability
  collectors (`src/memory.rs:2942-3860`, `src/sqlite.rs:1878-3400`,
  `src/postgres.rs:4552-5900`, `src/payload_backend.rs:490-1310`). Replace them
  with one slot-yielding visitor over events, commits, and roots plus one
  manifest walker; leaf operations (store, load, mark) stay per store.
- 3B Decision: `PayloadBackend<B, S>` becomes the only offload mechanism.
  Every provider owns a blob store today (`src/memory.rs:3742-3860`,
  `src/sqlite.rs:3012-3400` with `LocalDirectory`, `src/postgres.rs:5287-5900`)
  and the decorator adds a fourth; `PayloadStorageConfig::blob_store` is
  honored by SQLite only, and `PayloadBackend::with_payload_storage` clears it
  silently (`src/payload_backend.rs:96`). Providers store `PayloadRef`
  opaquely; the row-blob tables become `PayloadBlobStore` implementations. The
  SQL providers' transactional timestamp refresh (`SPEC.md` §18) remains until
  item 0019 Phase 1 proves equivalent publication/reclamation protection in the
  shared mechanism. A grace period plus pre-delete probe is insufficient.
  Public surface change: `open_with_payload_storage` / `with_payload_storage`
  shapes and `PayloadStorageConfig::blob_store`.
- 3C Cheaper refs. Reviewer-measured: `encode_payload` 193 ns against 45 ns for
  bare msgpack, of which `type_fingerprint` is 139 ns (SHA-256 plus hex plus a
  `String` per call, `src/payload.rs:169-176`, `:262-270`); an inline ref
  around a 34-byte body serializes to 196 bytes. Cache the fingerprint per
  type; stop serializing `schema_fingerprint`, `compression`, and `encryption`
  when default (bundled into 5G because stored refs change).
- 3D `s3` feature weight: 143 crates against 87 for `postgres` (`cargo tree`),
  with `rust-s3` pulling `reqwest` and `attohttpc` and both `ring` and
  `aws-lc-rs`. `S3BlobStore` uses five operations
  (`src/payload_backend.rs:1489-1611`) plus 130 lines of date parsing. Trim the
  feature set or write the five SigV4 calls on one client.
- 3E TypeScript: covered by Phase 7 on the go path (the package is deleted).
  On the no-go path, add `hydratePayload` to the TypeScript contract so replay
  hydrates lazily (`packages/payload/src/index.ts:455-461`, `:567-579` hydrate
  every prefetched blob eagerly today, and `local-directory.test.ts:206` pins
  it).

Out of scope:
- The publication/reclamation protocol belongs to item 0019 Phase 1, including
  any required `PayloadBlobStore` changes. This phase consumes that proven
  contract; URI ownership remains unchanged.

Completion gate:
One traversal in `src/payload.rs`; no provider contains a payload walker or a
blob store; inline-versus-blob equivalence, GC, and Garage S3 conformance pass;
`payload_encode_*` benches show the fingerprint cost gone.

Testing plan:
- Conformance "inline and blob-backed payload equivalence" and GC cases on
  memory, SQLite (reopen), Postgres, and `PayloadBackend` over each.
- Garage S3 conformance in CI.
- New Criterion bench `payload_encode_small`; `sqliteStoreBytes` in the
  workload report before and after.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: single payload traversal | `src/payload.rs`: `history_event_payload_slots` is the one exhaustive match over an event's payload fields (a `PayloadSlot` is a plain ref or a `ManifestKind`-tagged manifest ref); `ManifestWalk` is the one manifest walk, pull-driven so a sync caller (`manifest_refs`, `input_manifest_page_refs`) and an async caller (Postgres transactions, the decorator's blob store) load containers their own way; `history_event_payload_roots` and `history_event_payload_refs` build the root and reachability collectors on them. Deleted: the event-walking matches and per-kind manifest collectors in `src/memory.rs`, `src/sqlite.rs`, `src/postgres.rs` (roots and blobs, failure and child-map-outcome helpers, three `*_root_for_roots` and three `collect_*_manifest_ref` per provider) and the decorator's three external manifest walks. 23,508 to 22,953 lines across the five files. Guard: `payload_offload_activity_map_round_trip` and `payload_offload_child_workflow_map_round_trip` now sweep with `gc_payload_blobs` and re-read the completed map's result manifest; dropping the `ActivityMapCompleted` slot from the traversal fails `memory_provider_offloads_large_payloads_and_hydrates_public_apis` and `sqlite_provider_offloads_large_payloads_and_hydrates_after_reopen` (before those pins the whole workspace suite stayed green under that mutation). Still per provider: the normalize and hydrate manifest rewrites (`normalize_*_manifest_for_storage`, `hydrate_*_from_storage`, three each), which 3B removes with the row-blob tables. |
| In Progress | Decision | 3B: decorator is the only offload mechanism | Centralized offload remains the proposed direction, with provider-owned row stores behind a shared contract. The prior safety premise is superseded by 0019/F1: the decorator's timestamp probe and delete can lose a concurrently committed blob. Missing: 0019 Phase 1's publication/reclamation proof before replacing SQL transactional protection, then the provider migrations and constructor/API migration. |
| Complete | Work | 3C: fingerprint cached, default fields skipped | Landed: `type_fingerprint` memoizes per type (`cached_type_name_fingerprint`, keyed by the `type_name` address, value leaked once); Criterion `payload_encode_messagepack_small` 156.5 ns before, 31.2 ns after on the same machine. Decided against skipping default `schema_fingerprint`, `compression`, and `encryption` on a stored `PayloadRef`: the ref's stored shape is now the one both runtimes read (`kind`-tagged, camelCase, SPEC §14), and the TypeScript decoder expects every field present, so omitting defaults would buy a few bytes per ref at the price of a second shape. |
| In Progress | Decision | 3D: `s3` dependency weight | Measured (`cargo tree -e normal --prefix none | sort -u`): no features 54 crates, `sqlite` 54, `postgres` 96, `s3` 151, all 184. The `s3` feature adds 81 crates over the `postgres` set, every one reached only through `rust-s3`: `reqwest`, `hyper`, `tower`, `rustls` (what any async HTTP client costs); `attohttpc`, `aws-creds`, `rust-ini` (a sync credential client that `tokio-rustls-tls` pulls through `aws-creds/rustls-tls`, which no feature choice removes); both `ring` and `aws-lc-rs` with `aws-lc-sys` (two TLS backends); `sysinfo`, `quick-xml`, `time`, `md5`. Decided: replace `rust-s3` with the five SigV4 requests (`PUT`, `GET`, `HEAD`, `DELETE`, `ListObjectsV2`) on `reqwest` with `rustls` and `hmac` over `sha2`, which drops the sync client, the second TLS backend, `sysinfo`, `quick-xml`, and the 130 lines of date parsing (`Last-Modified` alone remains). Missing: the client, landing with the 3B storage pass; Garage CI conformance is its acceptance test. |
| Complete | Work | 3E: lazy hydration in TypeScript (no-go path only) | 2E decided go and 7C retired the TypeScript payload package; the binding hydrates a claim's prefetched history eagerly (`PayloadBackend::hydrate_history_events`, `durust-node/src/lib.rs` `hydrate_claim`) because the TypeScript worker reads payloads from the events it is handed, while the Rust worker keeps its lazy path (`tests/replay_core.rs::replay_hydrates_large_activity_result_only_when_workflow_observes_it`). |
| In Progress | Gate | Net lines and benches | 3A: 555 lines removed across the five payload-bearing files. 3C: `payload_encode_messagepack_small` 31.2 ns. Missing: the 3B and 3D deltas. |

---

## Phase 4: One Rust durability engine over storage primitives

Goal:
`commit_workflow_task`, fencing, terminal cleanup, timer and timeout scans,
activity completion, child dispatch, and cancellation exist once in Rust;
each provider implements storage primitives only.

Evidence:
- `DurableBackend` has 33 methods; of the 21 a provider must implement, nine
  are engine procedures each provider re-implements, and the three commit
  bodies follow the same 14-step sequence (`src/memory.rs:532-914`,
  `src/sqlite.rs:574-863`, `src/postgres.rs:2753-3164`). Reviewer estimate:
  engine logic ~2,580 lines in memory, ~3,370 in SQLite, ~5,980 in Postgres
  (including a second batch-shaped copy of ~1,650 at `:2103-2753`,
  `:6072-6540`, `:6933-7139`).
- 0017 line 930 rejected merging the providers on textual similarity (0.04 for
  the commit bodies). The SQL differs; the control flow does not.
- `map_engine.rs` already proves the pattern: pure effects applied by three
  ~150-line appliers.

Scope:
- 4A `pub(crate) trait Storage` (RPITIT, no boxing) and `Engine<S: Storage>`
  implementing `DurableBackend`; `MemoryBackend`, `SqliteBackend`, and
  `PostgresBackend` become newtypes. `DurableBackend` stays public and
  unchanged, so no API budget is spent. Primitive families: transaction and
  id allocation; runs (find, lock, insert, update, claimable, children);
  history (append, read, child seqs); waits and signals; activities; maps;
  outbox, projection, and change markers; terminal cleanup; blobs (until 3B
  removes them).
- 4B Memory first. Add the secondary indexes in the same pass: reviewer-measured
  `claim_workflow_task` scans ready runs linearly (`src/memory.rs:439`;
  5.1 ms with 100k idle runs), `dispatch_child_workflow_starts` rescans the
  outbox per row (`:1445`; 5.3 s at 50k), `fire_due_timers` scans every timer
  (`:1030`; 682 µs at 100k), and the same shape holds at `:1133`, `:1081`,
  `:971`, `:874-881`, `:112`, and `:1924-1951`. Memory is the substrate for
  the simulations and most Criterion profiles, so its scaling leaks into every
  number they produce.
- 4C SQLite. `rusqlite::Transaction` is `!Send`, so the SQLite shim drives the
  engine future to completion inside the method (it already returns
  `ready(...)`, `src/sqlite.rs:579-862`).
- 4D Postgres. Batch commit becomes plan-then-apply (lock all, decide per item
  with pure engine logic, apply set-based), replacing
  `apply_simple_workflow_task_commits_tx` (`src/postgres.rs:2476-2753`) and the
  scalar fallback that 1D patches. Scalar twins delegate to the batch form
  with one item (`:1709-1854`, `:3498-3589`, `:3263`; 26 `_inner` retry shims).
  Build schema-qualified SQL once at connect and use `prepare_cached`
  (no `prepare_cached` exists today); make the timer and timeout scans
  set-based (`fire_due_timers_tx` issues four round trips per due timer,
  `:8643-8712`); clone requests only when a retry is observed (`:2071`,
  `:2097`, `:1327`, `:1395`, `:3169`); implement `wait_for_ready` with
  LISTEN/NOTIFY or correct the `src/backend.rs:181` comment that says it is
  implemented.
- 4E Decision: built-in providers apply child starts inline, as Postgres already
  does and `SPEC.md` §8.2 permits; `dispatch_child_workflow_starts` keeps a
  default no-op for providers that need an outbox. Deletes the memory and
  SQLite dispatch machinery (reviewer estimate ~440 lines) and the plain-child
  overwrite at `src/memory.rs:836`.
- 4F The due-timer missing-run and namespace-mismatch divergences (0017 lines
  888 and 889) and the per-item timeout isolation (0017 line 786) are resolved
  once in the engine.
- 4G Test infrastructure: a `provider_tests!` macro for the 63 per-provider
  wrappers in `tests/provider_conformance.rs` (~950 lines); diff the seven
  `src/postgres/tests.rs` re-tests of conformance scenarios (~1,080 lines)
  against their twins and delete the identical ones. `force_terminal_for_tests`
  stays.

Out of scope:
- Any change to the map engine's transition table or effects.
- The shard-native roadmap (0013).

Completion gate:
Three newtype providers over one engine; the 51-scenario matrix, both
simulations, and the 1K fencing race pass on all providers; Criterion medians
for claim, commit, timer scan, and activity claim/complete within thresholds on
memory, SQLite, and Postgres; net reduction recorded (reviewer estimate −7,000
to −8,000 lines across the three files).

Testing plan:
- Conformance matrix after each provider migrates (memory, then SQLite, then
  Postgres), plus `tests/sim_worker.rs`.
- New bench: claim and timer scan with 10k idle runs on memory.
- `postgres_provider_hot_paths` and `timer_due_scan_wakeup` before and after.
- Conformance case for 4E: child starts visible in the same commit on every
  provider; parent close policy applied.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 4A: `Storage` trait and `Engine<S>` | Deferred in this pass: the three providers hold about 20,000 lines of engine-shaped code, and the rewrite is a project of its own. What this pass did instead was pull the engine's contract into one place where the providers already disagreed: 3A's payload traversal, 4E's inline child starts, and 4F's timer scan rules are now one behaviour each, which is the invariant an engine would have enforced. Missing: trait, engine, newtypes. |
| Incomplete | Work | 4B: memory provider migrated with indexes | Deferred with 4A. The one scan the binding work exposed, `dispatch_child_workflow_starts` rescanning the outbox per row, is moot: a commit starts its children itself, so the outbox holds only records a later drain may need. Missing: ready-run, timer, and activity indexes with a 10k-idle-run bench. |
| Incomplete | Work | 4C: SQLite provider migrated | Deferred with 4A. |
| Incomplete | Work | 4D: Postgres provider migrated with plan-then-apply, cached statements, set-based scans | Deferred with 4A. |
| Complete | Decision | 4E: inline child starts for built-in providers | Decided and landed for the memory and SQLite providers, which now match Postgres: `commit_workflow_task` starts every child the commit requested or admitted before it returns (`dispatch_pending_child_starts` in `src/memory.rs` and `src/sqlite.rs`, called before the commit's tail is returned; the outbox record stays as the child link and cancellation index). `dispatch_child_workflow_starts` stays on the trait as the hook a provider with a real outbox drains, and the worker keeps calling it (`tests/worker_run.rs::disabled_timer_maintenance_dispatches_child_starts_from_the_interval_loop` counts the calls through `ObservingBackend`). The corpus script's `dispatchChildStarts` step and both runners' arms are gone; `tests/replay_core.rs` and `benchtools` assert the drain metric stays at zero. The 440-line deletion of the outbox tables waits for 4B and 4C, because the record also indexes children for cancellation. |
| Complete | Work | 4F: timer-scan divergences resolved once | The memory scan selects like the SQL scans: a due wait in another namespace is never a candidate and spends no budget; a wait whose run is gone is a candidate and is deleted; a terminal run's wait is skipped without deletion (the deliberate per-row skip the SQL providers document). SQLite's scan now left-joins `workflow_instances` so a wait whose run row is gone is selected and its delete branch, dead code before, runs (`src/sqlite.rs` `fire_due_timers`). 0017 line 786 (one bad item stops the whole timeout batch) is a TypeScript provider defect, moot under 2E. |
| Complete | Decision | 4G: conformance wrappers and Postgres re-tests collapsed | Evaluated and not adopted. Of the 62 per-provider wrappers in `tests/provider_conformance.rs`, only 5 scenario triples share one shape (`provider_conformance`, `activity_map_zero_max_in_flight_is_rejected_at_descriptor_creation`, `child_workflow_map_id_collision_holds_the_in_flight_bound`, `activity_map_materializes_a_large_batch_in_one_statement`, `child_workflow_map_fail_fast_history_strings`); the rest open, reopen, or configure their provider differently. A macro over those five saves about 45 lines and would have to take all three test names as literal idents, because `PARITY.md` cites test names verbatim (the file's own rule in `run_conformance_scenarios!`). `src/postgres/tests.rs` holds 41 `postgres_*` tests named for shard leases, batch fast paths, reconnects, and schema checks; none shares a scenario name with the conformance file, so there is no identical twin to delete without reading each pair, which this pass did not do. |
| In Progress | Gate | Matrix, sims, benches, line delta | The 52-scenario matrix, both simulations, and the whole workspace suite pass on memory, SQLite, and Postgres after 4E and 4F. Missing: the 4A to 4D migration and its Criterion medians and line delta. |

---

## Phase 5: Rust execution layer

Goal:
The replay cursor holds each ready event once, waiting futures allocate
nothing per poll, one pipeline fetches chunks and one commits, `select!` has no
side channel, and the macro lints reject only what they can actually detect.

Scope:
- 5A Partition ready events at ingest. `index_events` clones every ready
  payload into the per-seq maps (`src/runtime.rs:255-329`) while
  `replay_events` keeps the original and the cursor skips it (`:748-758`);
  `consumed_replay_event_ids`, `skip_consumed_indexed_events`,
  `skip_consumed_replay_events`, and the cursor check in
  `record_indexed_ready_event_id` (`:204`, `:760-778`, `:920-933`) exist only
  for that. Command events go to the window, ready events to the indexes,
  and the four items are deleted. 0017 line 491 deferred this.
- 5B Waiting futures clone a `CommandId` (a `String`) on every poll
  (`:2272`, `:3442`, `:3567`, `:3208-3209`); `SignalFuture::poll_waiting`
  builds a fingerprint before knowing whether it hit (`:3633`); the worker adds
  two clones per pending signal per wake (`src/worker.rs:1786-1790`).
  Reviewer-measured: 8.1 allocations per pending signal waiter per cached
  wake. Store `CommandSeq` (Copy) in future state; build the fingerprint after
  the hit; consume `finishes` by value (`:1559`, `:1580`).
- 5C Read signal inboxes only when the claim reason is `SignalReceived` or the
  task registered a new signal wait (`src/worker.rs:1782-1820` reads every
  pending signal's inbox on every wake). Safe only because providers re-ready a
  run whose consumable signals remain after commit (`src/memory.rs:728`,
  `:887`; `src/sqlite.rs:830`); assert the same for Postgres in conformance
  before landing.
- 5D One chunk-fetch pipeline and one commit pipeline. `stream_history_chunk`
  / `claim_history_chunk` (`src/worker.rs:1653-1698`) duplicate the recovery
  variants (`:1700-1766`); `prefetched_claim_history_chunk` (`:2736-2775`)
  duplicates `_bounded` (`:2777-2815`); a `debug_assert!(false)` arm remains
  (`:2422-2433`). `commit_prepared_workflow_task` (`:1276-1321`) and the batch
  loop (`:2205-2301`) each write the cache decision. Route the single-task
  path through the batch loop.
- 5E `select!` passes the winning event id through a thread-local slot
  (`src/runtime.rs:717-730`, hidden functions `:2104-2114`, macro choreography
  `durust-macros/src/lib.rs:550-577`), and `select_all` makes three
  `with_context` calls per branch per poll (`:2014-2031`). Add
  `poll_branch(&mut self, cx) -> Poll<(Option<EventId>, Output)>` to
  `DurableSelectBranch` and delete the slot.
- 5F Delete `AwaitLint` (`durust-macros/src/lib.rs:909-943`) and
  `tests/ui/unknown_await.rs`: it rejects any `.await` whose base lacks a
  `durust ::` token, so an async helper that awaits only `durust::sleep`
  fails to compile, while `use tokio::time::sleep; sleep(..)` passes the path
  lint (`:866-893`). Decide the path lint's fate and record the runtime
  determinism-enforcement decision 0017 line 658 leaves open. Keep the
  `plain_join_future` type check.
- 5G History-format bundle, one recorded pre-1.0 breaking release: make
  `HistoryEvent.event_type` a method (`src/history.rs:305-309`, 22 sites keep
  it in sync); drop `SignalConsumed.fingerprint` (`:601-608`, constant per
  name); skip default `schema_fingerprint`, `compression`, `encryption` on
  `PayloadRef` (3C); decide `event_time`. `durust::now()` is documented
  (`README.md:384-392`, `SPEC.md:2606`, `:2714-2717`, lint message
  `durust-macros/src/lib.rs:875-879`) but does not exist, and the envelope has
  no timestamp to define it from. Either strike it (Phase 8 doc row) or add
  `event_time` here and define `now()` as the last replayed event's time.
- 5H `tests/replay_core.rs` rig: 143 `Worker::builder(` and 117 `Client::new(`
  sites in one 8.9K-line file; six near-identical
  `run_out_of_order_completion_before_*` helpers (`:2955-3315`,
  `:8406-8491`); no provider table. Add `tests/common/mod.rs` with a rig
  builder, a provider table, and one parameterized out-of-order helper.

Out of scope:
- The `with_context` scoped-thread-local mechanism and its two `unsafe`
  derefs (`src/runtime.rs:139`, `:188`): reviewed, covered by
  `:3958-4122` and the Miri job, and kept.

Completion gate:
`tests/replay_core.rs`, `tests/worker_run.rs`, `tests/sim_worker.rs`, and
both budget tests pass; the corpus is identical; Criterion medians for cached
wake, small and large replay, and both select benches are not worse than the
2026-09-15 baseline (cached wake 3.27 µs, small replay 6.53 µs, select
registration 2.90 µs on the review machine); the per-waiter allocation slope is
budgeted.

Testing plan:
- 5A: the `out_of_order_*` family, `tests/replay_clone_budget.rs`, and the
  unit tests at `src/runtime.rs:4155`, `:4279`, `:4324`.
- 5B: a per-waiter slope budget added to `tests/worker_hot_path_budget.rs`.
- 5C: conformance "signal arrives while claimed for an activity completion;
  consumed on the next task" on all providers.
- 5D: `src/worker.rs:3440-3519` unit tests, `recovery_defer_*` benches, the
  hot-path budget for rejected prefetch and batched commit.
- 5E: `select_*` replay tests and both select benches.
- 5F: `tests/compile_fail.rs` plus a positive UI case with an async helper.
- 5G: provider round-trip conformance; `payload_encode_messagepack_64kb`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 5A: ready events partitioned at ingest | Landed: `ReadyEventIndexes::partition_events` moves ready events into the indexes (no payload clone) and returns only command events for the window; `consumed_replay_event_ids`, `skip_consumed_indexed_events`, `record_indexed_ready_event_id`, and `is_index_consumable_ready_event` are deleted, and `peek_replay_command_event` is a plain cursor read. The `runtime::tests` cases that pinned the old skipping (`indexed_ready_events_are_skipped_when_consumed_before_the_replay_cursor`, `peek_replay_command_event_skips_unconsumed_ready_events_without_consuming_them`) pass unchanged because their assertions describe the observable contract; `tests/replay_clone_budget.rs`, `replay_core` (138), `worker_run`, `sim_worker`, and `worker_hot_path_budget` green. |
| Complete | Work | 5B: no per-poll allocations in waiting futures | Landed: `SignalFuture::poll` moves its waiting id in and out instead of cloning it; `poll_waiting` builds the fingerprint only after a hit; `request_signal` takes references and clones only for a new request; the worker moves signal names into the inbox requests. New slope in `tests/worker_hot_path_budget.rs` (`measure_cached_signal_waiter_wakes`, 2 against 10 pending waiters over 6 cached wakes): 8.2 allocations per waiter per wake before, 4.2 after, budget 5.0. |
| Complete | Decision | 5C: inbox reads gated on claim reason | Rejected after measurement. Gating carried-over waiters on `WorkflowTaskReason::SignalReceived` took the slope under 1 allocation, but it changes the commit: a signal that arrived before an activity completion is consumed in that task today (and by TypeScript, whose claims carry every live signal), and under the gate one task later. `tests/replay_core.rs::join_waits_for_signal_and_timer_branches` and two siblings pinned the difference. The remaining 4.2 allocations per waiter per wake are the inbox request's owned name and run id and the provider's answer. |
| In Progress | Work | 5D: one fetch pipeline, one commit pipeline | Landed: `cache_entry_after_commit` is the one cache decision for the single-task and batch commit paths (`src/worker.rs`). Kept: the single-task path still commits through `commit_workflow_task` rather than the batch loop, because `run_workflow_once` is the deterministic driver tests use to observe a task fault and the batch stage settles faults into counts; the recovery chunk loaders and `prefetched_claim_history_chunk` twins differ in contract (unbounded returns `None` unless the prefetch covers the window; bounded returns a prefix with `has_more`). |
| Complete | Decision | 5E: `select!` without the thread-local slot | Kept as is. The slot is `RuntimeContext::last_ready_event_id`, read through the context borrow by two hidden functions the macro calls around each branch poll; it is context state, not a thread-local. A `poll_branch` method on `DurableSelectBranch` would have to be implemented by every branch kind (activity, timer, signal, child, both maps, both result futures, `BoxSelectBranch`, `join_all`, `select_all`) and would add more code than the two functions it replaces. |
| Complete | Decision | 5F: delete `AwaitLint`; decide path lint and runtime enforcement | Decided and landed: `AwaitLint` and `tests/ui/unknown_await.rs` are deleted; the path lint (host clock, `tokio` scheduler, `rand::random`) stays as a cheap guardrail, and determinism is enforced by replay and command fingerprints (`SPEC.md` §16 already says so). Positive case `tests/ui/pass/async_helper.rs` (an async helper awaiting `durust::sleep`, plus `durust::now()`) runs through `trybuild::pass` in `tests/compile_fail.rs`. |
| Complete | Decision | 5G: history-format bundle and `event_time` | Decided and recorded as the one pre-1.0 breaking history-format release, all on this branch: `PayloadRef`, `DurableFailure`, `EncryptionMetadata`, the map manifests, and `ChildWorkflowMapItemOutcome` now serialize `kind`-tagged and camelCase with inline bytes as a byte string; `RetryPolicy` is the shared five-field model, which also changes every activity `options_digest`; `HistoryEvent.event_type` is a method over `data` (`src/history.rs`), so no constructor keeps a second copy in sync (20 construction sites and 40 reads rewritten; the SQL providers keep the `event_type` column as an index only); timeout and child-id-collision messages name their cause. `event_time` is struck: `durust::now()` is a recorded side effect (1A), so the envelope needs no timestamp. `SignalConsumed.fingerprint` stays: it is part of the shape both runtimes write and the corpus compares. Migration: none; a database written before this release is recreated, which the pre-1.0 policy allows. Provider round-trips: the offload, reopen, and GC conformance cases pass on memory, SQLite, and Postgres. |
| Incomplete | Work | 5H: `tests/replay_core.rs` rig | Deferred in this pass: the file's 143 `Worker::builder(` and 117 `Client::new(` sites vary in queues, registrations, and cache settings, so a shared rig is a per-test rewrite rather than a mechanical extraction. Missing: `tests/common/mod.rs`, line delta. |
| In Progress | Gate | Suites, corpus, benches, slope | `tests/replay_core.rs` (138), `tests/worker_run.rs`, `tests/sim_worker.rs`, and both budget tests pass; the corpus is identical (16 cases in both runners); the waiter slope is budgeted (5B). Missing: Criterion medians for cached wake, small and large replay, and both select benches against the 2026-09-15 numbers; 5G and 5H untouched. |

---

## Phase 6: TypeScript execution layer

Goal:
Every durable call shares one `then()` shape and one command matcher, the
determinism guards leave the execution path, `join` and `select` accept every
durable call as Rust does, and the worker reads live signals from the claim.

Scope:
- 6A One `DurableCall` base and one `matchOrAppendCommand`. Today: 13 `then()`
  bodies, six positional-match blocks, twelve `retryAfterReplayHistory` sites,
  paired `resolveHotX` / `resolveHotXBranch` methods (`runtime.ts:2026-2180`),
  and eleven ready-event maps with a twelve-branch `if` chain (`:1218-1228`,
  `:1427-1476`). The promise classes (935 lines) each re-implement
  gate, resolve, `hotSuspend`. Mirror Rust's `match_or_append_command`
  (`src/runtime.rs:872`) and collapse the maps to two.
- 6B `join` and `select` accept only activity, timer, and signal branches
  (`runtime.ts:3258`, `:3745`, `:3805`; `api.ts:100`); Rust accepts spawned
  and child results too, so the README's dynamic fanout and race examples
  (`README.md:410-460`) cannot be written in TypeScript, and `SelectAllResult`
  types the value as the union of all branches (`api.ts:257-260`;
  `packages/examples/src/control-flow.ts:101` casts). Falls out of 6A; type
  the winner by tuple index.
- 6C Guards out of the execution path. `installNondeterminismGuards`
  (`runtime.ts:700-955`) replaces 25 globals; the constructor decides per
  execution from `NODE_ENV` (`:462-464`, `:670-689`) and re-asserts the
  `nextTick` patch each time (`:1036-1054`). Move `:57-104` and `:639-1077` to
  `determinism-guards.ts`, keep install and uninstall exported as an opt-in
  development aid, delete the `nondeterminismGuards` option plumbing
  (`worker.ts:86`, `:371`, `:1160`), `shouldInstallNondeterminismGuards`, and
  `isProductionHost`. The ESLint plugin is the production gate; replace
  `typescript/scripts/determinism-lint.mjs` (513 lines covering one fixture
  file, `typescript/package.json:28`) with an ordinary ESLint configuration.
  Closes 0017 lines 648, 653, and 658 with a recorded decision.
- 6D `liveSignals` becomes a required field of `ClaimedWorkflowTask`
  (`backend.ts:176`); memory and SQLite fill it in the claim; delete
  `#liveSignalsForClaim` (`worker.ts:951-961`) and
  `WorkerOptions.registeredSignalNames`. Reviewer profile on the memory mixed
  profile: `readSignalInbox` is 13.8% of samples, the largest single item.
- 6E One activity execution path: `#runActivityTaskBatch`
  (`worker.ts:1543-1623`), `#runActivityTaskBatchSequential` (`:1625-1705`),
  and `runActivityTaskOnce` (`:822-880`) repeat decode, run, encode, complete.
- 6F Decision: replace the quiescence pump (`#hotProgressVersion`, eleven
  `notifyHotProgress` sites, two "notify anyway" cases `:1420-1425`,
  `:1655-1665`; 0017 rows 4G and 4H at lines 563-564 were both defects here)
  with a `process.nextTick` barrier. Measure on the memory profile; a workflow
  doing hidden real I/O would commit earlier, which the determinism contract
  already forbids.
- 6G `WorkflowHandle.result()` streams the whole history and throws while the
  run is open (`api.ts:433-457`). Add a provider `workflowOutcome(runId)`
  returning the terminal fact, with an API budget, in both runtimes.
- 6H Typed provider errors. Providers throw message strings ("stale workflow
  task lease") and the conformance suite asserts on text; Rust has
  `Error::StaleLease`, `TerminalWorkflow`, `RunNotFound`. Export error classes
  or outcome variants.
- 6I Fold in 0017 line 201: a throw inside `#prepareWorkflowTaskFromCacheOrReplay`
  leaves a poisoned cache entry neither deleted nor disposed.
- 6J A `scenario()` test helper: `worker.test.ts` constructs 71 backends and 91
  workers, `runtime.test.ts` 41 and 21, each followed by the same registry,
  client, and `startWorkflow` preamble (reviewer estimate −1,000 lines).

Out of scope:
- Changing how a hot execution parks its promise chain (`SPEC.md` §4.2).

Completion gate:
`runtime.test.ts`, `worker.test.ts`, `determinism-guards.test.ts`,
`determinism-lint.test.ts`, `simulation.test.ts`, and `api-types.test.ts`
pass; the corpus is identical; the README fanout and race examples exist in
`packages/examples` and run; the memory mixed benchmark is at or above the
2026-09-15 baseline (1,650.7 workflows/s in `typescript/README.md`).

Testing plan:
- 6A and 6B: the 85 runtime cases, the corpus, the `PARITY.md` revert rows,
  `test-d/negative/type-safety.ts`, and `api-types.test.ts` "preserves
  selectAll winner value types".
- 6C: `determinism-guards.test.ts` with explicit install; the host-cost
  microbenchmark 0017 recorded.
- 6D: conformance suite over all three providers; benchmark memory profile.
- 6E: `worker.test.ts` and `PARITY.md` rows 6, 13, 14.
- 6F: "settles a joinAll whose branches complete in separate hot tasks" and the
  disposal cases; benchmark before and after.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6A: one durable-call shape and one matcher | Deferred in this pass: a rewrite of `runtime.ts`'s promise classes and matchers under the 85 runtime cases and the corpus, which 2E's go path reshapes anyway (the TypeScript runtime stays, but its provider-facing half shrinks). Missing: refactor, suites green, corpus identical. |
| Incomplete | Work | 6B: every durable call is a join/select branch | Open and user-visible: `join` and `select` still accept activity, timer, and signal branches only, so the README's dynamic fanout and race examples cannot be written in TypeScript. Deferred with 6A, which it falls out of. Missing: registration on spawned and child handles, typed `SelectAllResult`, the two README examples ported and run. |
| Incomplete | Decision | 6C: guards out of the execution path | Deferred with a recorded reason: `determinism-guards.test.ts` drives the guards through the `nondeterminismGuards` execution option at fifteen sites, and `npm run lint` is the 513-line script rather than ESLint, so deleting the option plumbing before the ESLint replacement lands would leave `test-d/determinism/valid` as the only gate. Missing: an `eslint.config.mjs` on the plugin's `recommended` config over the workflow sources, `determinism-lint.test.ts` rewritten against ESLint's API, then the option plumbing, `shouldInstallNondeterminismGuards`, and `isProductionHost` deleted. |
| In Progress | Work | 6D: `liveSignals` required on the claim | Landed: `ClaimedWorkflowTask.liveSignals` is required and every provider fills it with the first unconsumed record per signal name at claim time (memory `#liveSignalsForRun`, SQLite query, Postgres `#readSignalInboxesForClaims` without a name filter); `WorkerOptions.registeredSignalNames`, `ClaimWorkflowTaskOptions.registeredSignalNames`, the fixture option, and `#liveSignalsForClaim` are deleted, so a worker needs no signal list to deliver signals hot. Suites green (778 passed). Missing: the memory benchmark before and after. |
| Complete | Work | 6E: one activity execution path | Landed: `#runClaimedActivity(claimed, batched, completions)` is the one path (emit, decode, run under the heartbeat context, complete or batch, fail); `runActivityTaskOnce`, `#runActivityTaskBatch`, and `#runActivityTaskBatchSequential` now differ only in how they claim. `worker.test.ts` and `simulation.test.ts` green (101). |
| Incomplete | Decision | 6F: quiescence barrier | Deferred: the memory profile runs at 1,672 to 1,711 processing workflows/s with the pump in place (2B), so the pump is not what bounds any measured profile; the decision waits for 7A, when the runtime sits over the Rust providers and the pump's interaction with async provider calls can be measured on the shape that ships. |
| Incomplete | Decision | 6G: `workflowOutcome` provider method | API budget recorded: `workflowOutcome(runId)` returning `{ kind: "Running" } | { kind: "Completed", result } | { kind: "Failed", failure } | { kind: "Cancelled", reason } | { kind: "ContinuedAsNew", nextRunId }` from the run's terminal fact, and `workflow_outcome(RunId) -> WorkflowOutcome` in Rust with the same variants; `WorkflowHandle.result()` then reads one row and throws only on `Failed` and `Cancelled`, returning `undefined` while `Running` rather than streaming history. Missing: the method on the Rust providers, the binding, and the TypeScript providers, which lands with 7A so it is written once. |
| Complete | Work | 6H: typed provider errors | Landed: `packages/core/src/provider-error.ts` exports `ProviderError` with `code: ProviderErrorCode` (`StaleWorkflowLease`, `StaleActivityLease`, `TerminalWorkflow`, `TerminalWorkflowSignal`, `WorkflowNotFound`, `InvalidMapOptions`), `isProviderError`, and one constructor per code; the memory, SQLite, and Postgres providers throw them at every refusal (28 sites), `mapRejectError` in `map-engine.ts` types the terminal map rejection, and `@durust/native` maps the addon's messages back onto the codes. The shared conformance cases assert `assertProviderError(fn, code)` at their eight refusal sites; the message text stays as it was. `npm run check` steps green (934 passed, 6 expected fail, 1 skipped, plus the Postgres thresholds env failure). |
| Complete | Work | 6I: poisoned cache entry on prepare throw | Landed: a throw from the cached `advance` evicts and disposes the entry before rethrowing, and a non-overrun throw from the cold `nextCommit` disposes the execution. Test: `worker.test.ts` `evicts and disposes the cached execution when its next task throws`; revert-verified (the retry served the poisoned entry). |
| Incomplete | Work | 6J: `scenario()` helper | Deferred in this pass: a test-only extraction across `worker.test.ts` and `runtime.test.ts`; no behaviour depends on it. Missing: helper, line delta. |
| In Progress | Gate | Suites, corpus, examples, benchmark | `runtime.test.ts`, `worker.test.ts`, `determinism-guards.test.ts`, `determinism-lint.test.ts`, `simulation.test.ts`, and `api-types.test.ts` pass (838 across the check); the corpus is identical; the memory and SQLite benchmark thresholds pass with `readSignalInbox` retired from their required operations. Missing: the README fanout and race examples (6B), the memory mixed benchmark before and after 6D, and 6A, 6C, 6F, 6G, 6H, 6J. |

---

## Phase 7: TypeScript providers on the shared core

Goal:
TypeScript users get the Rust providers, payload offload, and map engine
through the binding Phase 2 proved, and the TypeScript copies are deleted. If
Phase 2 decided no-go, the three TypeScript providers collapse onto one engine
over storage primitives instead.

Scope, go path:
- 7A Memory and SQLite through the binding. Delete `MemoryBackend`
  (`backend.ts:469-1937`) and `@durust/sqlite` (2,816 lines). Decision: what
  `@durust/testing` keeps. The binding's acceptance suite is the Rust
  conformance matrix; a trimmed provider-author suite (reviewer estimate
  ~500 lines) stays only if a third-party TypeScript provider is a supported
  use case.
- 7B Postgres through the binding, after 4D lands and 2C has recorded the
  same-machine numbers. Replace `pg.Pool` injection with the budgeted
  configuration from 2F. Delete `@durust/postgres` (6,171 lines) and its
  3,552-line conformance file.
- 7C Payload offload and the map engine through the binding. Delete
  `@durust/payload` (1,043 lines), `map-engine.ts`, and the parts of
  `provider-util.ts` that only providers used; keep the map-manifest builder
  types. Add `hydratePayload` to the TypeScript contract so replay hydrates
  lazily.
- 7D Child starts: with 4E, built-in providers apply child starts inline, so
  the TypeScript worker needs no outbox drain. If 4E is rejected, add
  `dispatchChildWorkflowStarts` to the worker's maintenance loop with the
  two-dispatch-site test `PARITY.md` §4 describes.
- 7E Retire `PARITY.md` rows 21-24 and the provider half of the ledger;
  `provider-io.json` becomes the binding's wire-format test; the Node floor
  can drop below 24 once `node:sqlite` is no longer required.

Scope, no-go path:
- 7F One `DurableEngine` over ~16 storage primitives (`MapStore`,
  `SqliteStore`, `PostgresStore`). The engine is today's `MemoryBackend` body
  with map access replaced by primitive calls; the Postgres tier already
  executes that body on loaded state. Reviewer estimate: SQLite ~2,816 to
  ~900, Postgres ~6,171 to ~2,000-2,300.
- 7G Delete the Postgres load-and-rewrite tier (`:2846-4432`, `:4432-5271`)
  by applying `map-engine.ts` effect lists as row statements for map, cancel,
  and continue-as-new commits, heartbeat, fail, and timeout.
- 7H SQLite: replace the `history` blob column (`packages/sqlite/src/index.ts:1115`,
  written at `:1409`, `:1447`, never read) and the every-event upsert in
  `#insertHistoryEvents` (`:1492-1508`) with a `tail_event_id` column and
  append-only writes. Reviewer-measured: 153 upserts per iteration at tail 101
  rising to 3,453 at tail 1,201. Bound the claim (`:476-495` has no `LIMIT`),
  add a persisted timeout deadline (`:926-931` scans every claimed activity),
  join the signal wake check (`:1639-1655`), cache prepared statements (54
  `prepare()` sites), and bound claim prefetch to `historyFetchMaxEvents` in
  all three providers (`backend.ts:570`, `sqlite:523`, `postgres:3250-3302`).
- 7I Store inline payload bytes as `bytea` rather than JSON number arrays
  (`provider-util.ts:274-280`; `postgres:5721`, `:6154`); adopt the
  `@durust/testing` fixtures inside the suite itself (54 raw
  `claimWorkflowTask` calls, 18 copies of one `encodePayload` literal).

Out of scope:
- Moving `worker.ts` into Rust (the B2 decision recorded in 2E).

Completion gate:
Go path: `@durust/core` plus `@durust/native` run the unchanged runtime,
worker, guards, simulation, corpus, and example suites; the deleted packages
are gone from the workspace and release scripts; `packages/benchmark` SQLite
and Postgres profiles meet the Phase 2 gate on the same machine. No-go path:
one engine, the 54-case suite green on all three providers, SQLite statement
count per commit constant in tail, Postgres heartbeat latency flat in
database size.

Testing plan:
- Go: the 2B suites plus `postgres-conformance.test.ts` over the binding until
  7B deletes it; `simulation.test.ts` soak over the binding's memory provider.
- No-go: race case from 1H, "heartbeat latency versus table size" assertion,
  statement-count assertion per commit, `postgres-mixed-*` baselines.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | Go or no-go inherited from 2E | Go for all three providers (2E). |
| Complete | Work | 7A: memory and SQLite via binding; `@durust/testing` scope decided | Landed: `MemoryBackend`, `map-engine.ts`, and `@durust/sqlite` are deleted; `packages/core/src/backend.ts` is the contract alone (344 lines) and `provider-util.ts` keeps the four helpers the runtime and cases read. `@durust/testing` keeps the shared cases (they are what every provider is measured against) and drops the provider-internal helpers. The three contract differences were settled by the Rust rule: `tests/provider_conformance.rs::unknown_activity_id_answers_run_not_found` (a Rust fix, revert-verified on memory), and the shared cases `an expired activity lease is reclaimed by the timeout scan and fences the old holder` and `a fail-fast child map with a colliding item id fails before its batch-mate starts`, rewritten to it. `native-conformance.test.ts` runs all 50 shared cases over memory, SQLite, and Postgres with no expected failures (159 tests). |
| Complete | Work | 7B: Postgres via binding | Landed: `durust-node` enables `postgres` and `s3`, `connectPostgres(url, options)` builds `PostgresBackendConfig` (schema, pool size, shards, partitions, timeouts) and `NativeBackend.postgres()` awaits it; `PostgresBackend` gained the injectable `ProviderClock` the other providers had (`PostgresBackendConfig::clock`), a `live_signals` reader, and a public `drop_schema` behind `destroy()`. `@durust/postgres` and its conformance file are deleted; the benchmark's Postgres counters moved to `packages/benchmark/src/postgres-stats.ts` over `pg`. Two Postgres defects the shared cases exposed are fixed and revert-verified (`child_map_id_collision_fails_the_map_in_its_scheduling_commit`): the scheduling commit published its tail only for an empty map, so a collision's failure event reused the commit's own event id, and fail-fast dispatch started every admitted item before reporting the collision. |
| Complete | Work | 7C: payload and map engine via binding | Landed: `@durust/payload` is deleted; offload is a constructor option (`payload: { inlineThresholdBytes, blobStore: LocalDirectory | S3 | Memory }`) that wraps the provider in the Rust `PayloadBackend`, with `LocalDirectoryBlobStore` added to `src/payload_backend.rs` (the SQLite provider's own local-directory offload now stores through it) and `gcPayloadBlobs` exposed. `NativeBackend payload offload` tests cover memory and SQLite offload, hydration, roots, and a dry-run sweep. The `fanouts` half of `map-transitions.json` keeps a TypeScript runner (`packages/core/test/map-fanouts.test.ts`); the `transitions` half has a Rust runner only. No `hydratePayload` was added to the contract: the binding hydrates claims eagerly (3E). |
| Complete | Work | 7D: child starts | 4E landed: every built-in provider starts children inside the commit, so the TypeScript worker needs no outbox drain and the binding carries none. |
| Complete | Doc | 7E: `PARITY.md` and fixtures retired for the provider half | Landed: rows 18, 21 to 24, and 28 cite `native-conformance.test.ts` and `map-fanouts.test.ts`; §3's three-gap bullet is gone and §4 records the provider retirement with both revert-verified Rust scenarios; `scripts/check-parity-names.mjs` resolves 107 citations. `typescript/README.md`, `packages/native/README.md`, the fixture note in `map-transitions.json`, and `check-fixtures.mjs` describe the one-provider-family layout. Node floor decision: stays at 24; nothing below it has been tested, and `node:sqlite` was the only reason the floor was named, not the only thing the floor covers. |
| Complete | Work | 7F-7I (no-go only): TypeScript engine over primitives, tier deleted, SQLite append-only history, bytea payloads | Not needed: 2E decided go. |
| Complete | Gate | Suites and benchmarks per path | `npm run check` green (build, tests, type checks, lint, fixtures, package dry run) with every test over the binding; `thresholds.test.ts` baselines re-recorded on the native providers (multi-worker baselines no longer pin cache hit and miss counts, which depend on which worker claims which task); same-machine medians of three runs, TypeScript worker, old providers -> Rust providers through the binding: memory 1757 -> 885 workflows/s, SQLite 145 -> 272 (1 worker) and 153 -> 208 (4 workers), Postgres 75 -> 132 (`typescript/README.md`). The Rust worker over the same providers moved within run variance (memory 4068 -> 4006, SQLite 343 -> 332 and 331 -> 321, tuned Postgres 308 -> 324 on five interleaved runs each), which is expected: this phase touched no Rust hot path outside map-scheduling commits. Rust workspace suite green with Postgres, clippy clean. |
| Complete | Review | Sequential review of Phase 7 | One reviewer, working alone, verified every finding by probe or trace: 8 findings, all addressed. High: the lockfile had no entries for the four unpublished platform packages, so `npm ci` refused it (`scripts/write-native-lockfile.mjs` writes them from the manifests; a real `npm ci` then succeeds, npm skipping the optional package it cannot fetch; `release.yml` writes them before its lockfile refresh and adds tarball integrity after the addons land; the release version flow was dry-run locally at 0.2.2 and restored). Medium: concurrent puts of one digest shared a temporary file in `LocalDirectoryBlobStore` (a per-put sequence number now names it; pinned by `src/payload_backend.rs::tests::local_directory_store_survives_concurrent_puts_of_one_digest`); the release job would have committed the addon binaries (`.gitignore` covers `npm/*/*.node`); the SQLite GC cutoff read the provider clock while blob ages are wall-clock (cutoff is wall-clock again, pinned by `tests/provider_conformance.rs::sqlite_gc_measures_blob_age_on_the_wall_clock`). Low: the relaxed worker tests now also assert that no claim or scan starts after the abort (`recordCallsAfterAbort`); a timed-out map item's `RetryScheduled.ready_at` is `now`, as the engine schedules it; `signalWorkflow` and `cancelWorkflow` on an unknown workflow id are now `Error::WorkflowNotFound` in Rust and the typed `WorkflowNotFound` provider error over the binding (`signal_and_cancel_of_an_unknown_workflow_are_not_found`; shared case `a signal to an unknown workflow id is rejected as not found`); stale sentences in `map-manifest.ts`, `PARITY.md` rows 21 and 31, the `map-transitions.json` exclusion note, and a benchmark comment were rewritten. |

---

## Phase 8: Tests, benchmarks, fixtures, CI, and docs

Goal:
Every gate in CI gates something, every fixture has one source of truth, and
the docs describe the code that exists.

Scope:
- 8A CI. The Rust fixture tests run three times per push: the workspace test
  (`.github/workflows/ci.yml:133`), a named step (`:140`), and `npm run check`
  (`:153`) through `typescript/scripts/check-fixtures.mjs`, which shells to
  `cargo` four times without `--locked` or `--all-features`. Keep one; split
  the single serial job into rust, typescript, and fixtures jobs; run all 18
  examples (`SPEC.md` §22.7 requires it; `:286` runs two); run clippy (0017
  line 746). Wire or delete `check:release` and `check:postgres`, which no
  workflow invokes.
- 8B Benchmarks. CI runs `cargo bench --no-run` only (`:292`);
  `tests/benchmark_thresholds.rs:424-431` proves a benchmark exists by
  substring-searching source, and `:163-420` asserts static JSON numbers
  exceed floors; the `phase6-before` Criterion baseline the README cites is
  not checked in. `benchtools/src/bin/durust-benchmark-workload.rs` (4,656
  lines) and `packages/benchmark/src/index.ts` (2,749) are twins, each with
  its own Postgres database create, sweep, and guard (472 and 302 lines plus
  guard tests) while `benches/replay_core.rs:2916` isolates by schema. Delete
  `payload_compression` and the `zstd` dev-dependency (a feature `SPEC.md` §18
  rules out); move the workload runners to schema isolation; either make the
  baselines real (nightly compare with wide tolerance) or delete the
  baseline-number tests and say benchmarks are manual. On the Phase 2 go path,
  one runner drives both runtimes.
- 8C Simulation harness. `FaultInjectingBackend` acts on two fault points
  (`src/sim.rs:552-568`, `:770-790`); the other `FaultPoint` variants drive
  in-crate models only (`:~1040-1450`), which `SPEC.md` §22.6 forbids; no
  latency injection exists although `AGENTS.md` lists it; `pub use sim::*`
  (`src/lib.rs:38`) makes ~850 harness lines public API. Delete the model
  scenarios and unused variants, add a seeded delay knob in
  `forward_faultable!`, and decide the public surface (`SimRun`,
  `FaultProfile`, `run_many_seeds` are documented in the README).
- 8D Fixtures. `tests/contract_fixtures.rs:693-1176` builds Rust values from
  JSON by string key and `tests/behavioral_corpus.rs:303-636` projects them
  back; the fixture shape matches neither serde nor any provider. Give the
  durable types one canonical serde JSON form (tagged `kind`, camelCase) so
  both runners (de)serialize directly (~800 lines). Re-export `map_engine`
  for tests so `tests/map_transitions.rs` asserts the `transitions` section
  (`:9-16` explains it cannot today). Add the key-set check
  `benchmark-output.json` lacks (0017 line 750).
- 8E `PARITY.md`: cite test names only (the file already says names are
  verbatim so they can be run), drop the baseline commit and ~40 `file:line`
  citations, and add a script that fails when a cited test no longer exists
  on either side.
- 8F Manifest tooling. `cargo-durable` cannot generate a manifest, its
  `accept` is all-or-nothing against `SPEC.md:387-390`, and `versions`
  requires `--sqlite` (`src/bin/cargo-durable.rs:93-96`), which is why the
  binary is gated on the `sqlite` feature. TypeScript `manifest-cli.ts:55-68`
  fails on any diff by string compare while Rust classifies
  (`src/manifest.rs:88-96`). Share the diff semantics (the JSON is
  language-neutral); give `versions` a Postgres path or replace it with
  `workflow_change_versions`; drop `required-features`.
- 8G Release. `scripts/release-version.mjs:7-20` moves three crates and six
  npm packages in lockstep, so a Rust-only fix republishes six packages.
  Decision: decouple the streams, unless the go path makes lockstep natural
  because the binding pins the core.
- 8H Docs. Strike or implement `durust::now()` (5G); `typescript/README.md:5-9`
  says the runtime waits on 0014, which has no open rows; rewrite the "not an
  FFI wrapper" sentence after 2E; `src/backend.rs:181` claims a LISTEN/NOTIFY
  override Postgres lacks (4D); `SPEC.md` §8.2 lists `visibility_patch`, which
  the Rust struct lacks (`src/backend.rs:384-396`); `SPEC.md` §1.2 and §4.2
  carve-outs go once 1M lands; 0017's open decision rows (lines 374, 375,
  653) are closed or moved here.

Out of scope:
- New benchmark profiles.

Completion gate:
CI runs each suite once, in parallel jobs, with examples and clippy; every
remaining benchmark gate compares against a checked-in baseline or is
documented as manual; fixtures deserialize directly in both runners and the
Rust map-transition runner asserts both sections; `PARITY.md` has a name
checker in CI; docs contain no reference to a function or override that does
not exist.

Testing plan:
- `ci.yml` run on a branch showing the job graph and total time before and
  after.
- `tests/map_transitions.rs` asserting the `transitions` section.
- The `PARITY.md` name checker failing on a renamed test in a scratch branch.
- `cargo run --example` loop green for all 18 examples.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| In Progress | Work | 8A: CI runs each suite once, examples and clippy included | Landed: the duplicate `Run shared contract fixtures (Rust)` step is gone and `typescript/scripts/check-fixtures.mjs` runs only the TypeScript halves (the Rust halves are `cargo test --workspace`); the examples step loops over all 18 (each verified locally, exit 0); a `Clippy` step runs `-D warnings -A clippy::result_large_err` after `cargo clippy --fix` plus hand fixes brought the workspace to zero lints (verified locally, exit 0). Missing: the split into parallel jobs and a CI run showing the job graph and timing; `check:release` and `check:postgres` are still unwired. |
| Complete | Decision | 8B: benchmark gates are real or declared manual | Landed: the `payload_compression` Criterion group and the `zstd` dev-dependency are gone (`SPEC.md` §18 rules compression out; `Cargo.lock` shed 29 lines). Decided: the checked-in baselines stay as floors the smoke profiles must clear, and the Postgres accepted-baseline case in `thresholds.test.ts` is declared to run only against `tests/fixtures/postgres.compose.yml` (it needs `pg_stat_statements` preloaded and fails on a stock container with a message that names the fixture, which is the loud failure the file's own guard asks for). The two workload runners keep their database lifecycles: on the go path one Rust runner will drive both runtimes through the binding, so their twin code retires with 7B rather than being merged first. |
| Incomplete | Work | 8C: simulation harness pruned, latency knob, public surface decided | Missing: deletions (reviewer estimate −600 lines), knob, decision on `pub use sim::*`. |
| In Progress | Work | 8D: canonical fixture serde form; map-transition runner complete; key-set check | Landed: `durust::map_engine` is a `#[doc(hidden)]` public module and `tests/map_transitions.rs::shared_table_transitions_replay_through_the_engine` replays all 27 `transitions` cases through `step`; the fixture's `rustRunnerBlocker` is retired. With 5G the Rust `PayloadRef` and `DurableFailure` serialize in the fixture's shape, so `tests/contract_fixtures.rs` now reads them with `serde_json::from_value` and its hand-written codec, compression, and byte-array mappers are gone. The `benchmark-output.json` key-set check exists on both sides: `benchtools`' `memory_mixed_workload_completes` and `packages/benchmark/test/fixtures.test.ts` run a small memory profile and fail on any emitted key the fixture lacks at any object level (`operations` entries compared by shape); the first run found the drift 0017 line 750 predicted (three per-mixed-action fields on every operation entry, `workflowTaskCommitShapes`, `processingBackendMetrics`, three Rust and two TypeScript option keys, `postgres_database`), now in the fixture. Missing: the canonical serde form for history events, requests, and outcomes (the Rust `HistoryEventData` stays externally tagged and snake_case for its own storage; the binding's `wire.rs` is the TypeScript-shaped twin) and the corpus projection in `tests/behavioral_corpus.rs`. |
| Complete | Work | 8E: `PARITY.md` names only plus checker | Landed: `scripts/check-parity-names.mjs` resolves every Rust `path.rs::name` citation to a `fn` in that file and every TypeScript `file.test.ts` — `title` or shared-case citation to a title in the cited file or the shared cases (101 citations resolve); renaming a cited test fails it (verified by renaming `timeout_messages_are_pinned` in a scratch copy: exit 1 naming the citation); CI runs it after the TypeScript checks. Four paraphrased citations it found are corrected. The `file:line` citations in the notes stay: the checker covers what a rename can break. |
| Incomplete | Work | 8F: manifest tooling shares one diff semantics | Missing: shared classification, `versions` path, feature gate removed. |
| Complete | Decision | 8G: release version streams | Decided: lockstep stays. On the go path `@durust/native` pins the `durust` crate it wraps, and after 7A the TypeScript packages that remain (`core`, `native`, `eslint-plugin`, `testing`) release with the core they are measured against; a Rust-only fix that changes provider behaviour is a change to what those packages ship. Decoupling would return once a package no longer depends on the core, which none will. |
| In Progress | Doc | 8H: docs match the code | Landed: `durust::now()` exists as a recorded side effect with its TypeScript twin and tests; `README.md` describes the shared retry model; `typescript/README.md` describes the Rust core direction after 2E instead of "not an FFI wrapper" and points at this plan instead of 0014; `SPEC.md` §8.2 no longer lists `visibility_patch`, §8 describes the release and query semantics, §11 the retry pacing, §14 the stored payload shape, §15 the inline child start; `src/backend.rs` `wait_for_ready` names the memory provider's notify instead of a Postgres LISTEN/NOTIFY override that does not exist. 0017's rows 374 and 375 are closed (`SPEC.md` §11 lists the history-bound `error_type` values; `Error` is `#[non_exhaustive]`). Missing: 0017 row 653 (surfacing clobbered globals from `uninstall`), which needs an API budget for a wider return type. |
| In Progress | Gate | CI once, baselines real, fixtures direct, docs true | The fixture suite runs once per language in CI, all 18 examples run, clippy gates the workspace; the Rust map-transition runner asserts both sections; `SPEC.md` and `PARITY.md` describe the landed behavior. Missing: a CI run showing the job graph, the benchmark baseline policy, the canonical fixture serde form, the `PARITY.md` name checker, and the remaining 8H edits. |

---

## Ordering and Dependencies

1. Phase 1 first. Every item is small, independent of the refactors, and
   fixes a defect a user can hit today.
2. Phase 2 runs alongside Phase 1: the proof-of-concept crate touches nothing
   the fixes touch, and its decision gates Phase 7.
3. Phase 3 before Phase 4: with offload moved entirely into the decorator, the
   engine Phase 4 writes has no payload normalization to carry.
4. Phase 4 before Phase 7B: the Postgres provider TypeScript adopts must be
   the plan-then-apply one, and the 2C measurement must be taken against it.
5. Phases 5 and 6 are independent of each other and of Phases 3 and 4; they
   can run in parallel with them once Phase 1 has landed, because the corpus
   from 1L is the gate for all of them.
6. Phase 7 after Phase 2's decision and Phase 4.
7. Phase 8 items 8A, 8E, and 8H can land at any time; 8B, 8D, and 8G follow
   the Phase 2 decision.
