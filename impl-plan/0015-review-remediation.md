# 0015: Review Remediation

## Overarching Goal

Remediate the findings from the four-track codebase review: one reproduced
replay bug that fails ordinary workflows, crash-recovery gaps that permanently
wedge runs, provider invariant drift, payload GC data loss, a self-validating
simulation harness, and misleading API/packaging surface. Outcome: workflows
survive worker crashes and out-of-order completions on all three providers, GC
cannot delete reachable data, the simulator exercises production code, and the
public surface matches reality. Non-goal: new workflow features or the Postgres
shard-native roadmap (item 0013).

## Implementation Principles

- Correctness gates before performance work, per `AGENTS.md`.
- Extract shared decision logic into `src/provider_util.rs` before fixing the
  same invariant three times.
- Every bug fix lands with its regression test in the same change.
- Provider contract changes stay generic; no provider-specific runtime
  shortcuts.
- Pre-1.0: breaking history-format changes are acceptable when recorded as a
  Decision row.

## Testing Strategy

- Deterministic replay tests for every runtime behavior change (cached, cold,
  multi-chunk, unfavorable orderings).
- Provider conformance tests for every `DurableBackend` behavior change, run
  against memory, SQLite (close/reopen), and Postgres.
- Seeded simulation scenarios for every concurrency/recovery change, driving
  the real `Worker`.
- Criterion benchmarks with checked-in baselines gate the performance phase;
  regressions need explanation or acceptance.

## Phase 1: Replay command matching correctness

Goal:
New commands schedule correctly when unconsumed ready events sit at the replay
cursor head; one live signal delivery is consumed at most once.

Scope:
- Consolidate the eleven `take_*` (`src/runtime.rs:588-860`) and twelve
  `collect_*` (`src/runtime.rs:3292-3475`) bodies into shared helpers so
  command matching becomes single-point.
- Add `peek_replay_command_event()` that skips index-consumable ready events
  (reusing the `select_can_ignore_losing_ready_event` list,
  `src/runtime.rs:1752`) and use it in all nine `poll_init`/marker paths
  (timer, activity, activity map, child map, child, side effect, signal,
  `preconsume_marker`).
- Relax `SignalFuture::poll_init` seq-mismatch and `preconsume_marker` in-chunk
  rejection for skipped ready events.
- Deduplicate live signal fulfillment: `fulfill_signal_request`
  (`src/runtime.rs:428`) rejects `signal_id`s already in
  `consume_signals`/`live_signals`; the worker fulfillment loop
  (`src/worker.rs:1071-1105`) stops handing one inbox record to multiple
  waiters.

Completion gate:
The reproduced failure (spawn activity, sleep, sleep, result —
`Nondeterminism("expected TimerStarted ... found ActivityCompleted")`) passes
cached and cold; same-name sequential and `select`/`join_all` signal waits
consume distinct deliveries; full suite green.

Testing plan:
- Replay tests for an out-of-order completion preceding each new command type
  (cached, cold, multi-chunk).
- Same-name signal tests (sequential and concurrent).
- The repro as a regression test.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: take_*/collect_* consolidation | `take_indexed`/`collect_indexed` in `src/runtime.rs`; the eleven `take_*` and twelve `collect_*` bodies are one-line wrappers, and the index maps are the single ready-event consumption path. |
| Complete | Work | 1B: command-peek helper wired into all nine sites | `peek_replay_command_event` skips index-consumable ready events without consuming them; wired into activity, activity map, child map, child, timer, and signal `poll_init`, side effect, `get_version`/`deprecate_patch`, and `record_select_winner`. Worker drops the cache entry when a task leaves loaded ready events unconsumed so the next task cold-replays with full indexes. |
| Complete | Work | 1C: live signal dedup (runtime + worker) | `fulfill_signal_request` rejects records already in `consume_signals` or handed to another waiter and returns acceptance; the worker fulfillment loop counts only accepted records as progress; select-loser cancellation releases pending live records. |
| Complete | Test | Repro regression test (spawn/sleep/sleep) | `spawn_sleep_sleep_schedules_second_timer_past_out_of_order_completion_{cached,cold,multi_chunk}` in `tests/replay_core.rs`. |
| Complete | Test | Per-command-type out-of-order ordering tests | Cached + cold multi-chunk cases for new activity, side effect, version marker, and child spawn commands; preconsume cold-replay case; two-signal select second-branch-winner cold replay (default and single-event chunks); same-name signal sequential and join dedup tests. |
| Complete | Gate | Full suite green with new tests | `cargo test` 2026-07-01: 60 lib, 99 replay_core, 25 provider_conformance, all other suites 0 failures (Postgres cases skip without `DURUST_POSTGRES_URL`). |

## Phase 2: Crash-safe claims and worker release discipline

Goal:
A worker crash between claim and commit never permanently wedges a run, an
activity, or a recovery slot.

Scope:
- Store workflow claim lease expiry in all providers: `lease_until` from
  `lease_duration` at claim, eligibility `token is null OR lease_until <= now`,
  token bump on reclaim (SQLite `src/sqlite.rs:246-304` + schema `:3178`;
  memory `src/memory.rs:269-334`; Postgres `src/postgres.rs:1364-1499`).
- Lease-based reclaim for activity tasks with no timeout and no heartbeat
  (`src/options.rs` defaults both to `None`).
- Worker: funnel the fallible awaits in `prepare_claimed_workflow_task`
  (`src/worker.rs:535, 548, 615, 619`) through the single release point at
  `:688`; RAII guard for the recovery slot
  (`try_acquire_recovery`/`release_recovery`); batch path
  (`src/worker.rs:368-394`) releases prepared-but-uncommitted claims on error
  and treats per-item commit errors as release-and-continue.

Completion gate:
Conformance test "claim, crash (drop), advance past lease, reclaim succeeds and
stale commit is fenced" passes on all three providers; no worker error path
drops a claim or recovery slot.

Testing plan:
- Lease-expiry + fencing conformance tests.
- Worker unit tests for release on each early-error path and batch-abort path.
- Timeout-less activity reclaim conformance test.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: lease columns + claim predicates per provider | `claim_lease_until_ms` column (SQLite `workflow_instances` + `ensure_column`; Postgres schema v2) and `WorkflowClaim { token, lease_until }` in memory (virtual `state.now`); claim eligibility is `token is null or lease_until <= now` with the ready reason preserved through the claim; every claim mints a fresh fencing token via `provider_util::claim_lease_until_ms`. Timeout-less activities get a lease-derived `timeout_at_ms` at claim (`activity_claim_lease_timeout_at_ms`), reclaimed through the existing timeout/retry path. |
| Complete | Work | 2B: worker single-exit release for fallible awaits | `prepare_claimed_workflow_task` is now a funnel over `prepare_claimed_workflow_task_inner`; every error escaping the inner pipeline (current_time, history chunks, change versions, hydrate, registry, poll) releases the claim at one reconciliation point. |
| Complete | Work | 2C: recovery-slot RAII guard | `RecoverySlotGuard` (Drop over `Arc<AtomicUsize>`) replaces manual `release_recovery`; no early return or `?` can leak `active_recoveries`. |
| Complete | Work | 2D: batch path release-and-continue | `run_workflow_batch_once`: prepare errors release that claim and continue; per-item commit errors release-and-continue via `release_failed_workflow_task` (delayed release for backpressure); a wholesale commit-RPC error releases every uncommitted claim; the first non-backpressure error is propagated after the batch drains. |
| Complete | Test | Lease-expiry conformance (claim, crash, reclaim, fence) | `{memory,sqlite,postgres}_workflow_lease_expiry_reclaims_and_fences_stale_holder*` (SQLite closes/reopens between claim and reclaim; memory advances virtual time): reclaim equals a fresh claim (run, reason, replay target, prefetch), stale commit/release return `StaleLease`, new holder commits. `unexpired_workflow_claim_lease_is_not_reclaimable` runs in the shared suite. Mutation check: disabling lease reclaim in memory fails the memory case. |
| Complete | Test | Timeout-less activity reclaim conformance | `timeoutless_activity_lease_expiry_reclaims_and_fences_stale_holder` in the shared suite (all three providers): no reclaim before lease expiry, reclaim as attempt 2 through `timeout_due_activities`, stale heartbeat/complete/fail all `StaleLease`. |
| Complete | Gate | No worker error path drops a claim or recovery slot | `tests/replay_core.rs`: `claim_is_released_when_current_time_fails_before_prepare`, `cold_recovery_{change_versions,hydrate}_error_releases_claim_and_recovery_slot` (with `max_concurrent_recoveries(1)`), `batch_prepare_error_releases_failed_claim_and_still_commits_neighbors`, `batch_commit_rpc_error_releases_every_claim_in_the_batch`, `batch_per_item_conflict_does_not_abort_the_rest_of_the_chunk`. Mutation checks: leaking the claim in the funnel or the slot in the guard fails these tests. |
| Complete | Decision | Postgres schema v1 -> v2 hard error, no in-place migration | `POSTGRES_SCHEMA_VERSION` bumped for `claim_lease_until_ms`; mismatched schemas error loudly at open. Pre-1.0 stance: operators drop and recreate the schema. |
| Complete | Work | 2F: builder-configurable lease durations | `workflow_task_lease_duration`/`activity_task_lease_duration` knobs (default 30s, 1s floor) threaded to all four claim sites; `activity_lease_duration_knob_bounds_default_option_activity_runtime` pins spurious-timeout vs normal-completion behavior (mutation-checked). |
| Complete | Work | 2G: SQLite ready-index migration | `ensure_index` compares the stored `sqlite_master` definition and drops+recreates on mismatch; `sqlite_reopen_recreates_legacy_ready_index_and_claims_ready_workflows` covers legacy-index reopen. |
| Incomplete | Work | 2E: heartbeat does not extend lease-derived activity deadlines | A heartbeating activity with no explicit timeouts is still reclaimed at lease expiry (`heartbeat_activity` refreshes only `heartbeat_deadline_at`). Needs: design decision (separate activity `lease_until` column per SPEC section 19 vs heartbeat-refreshed `timeout_at`), then implementation; reconcile SPEC section 19 (`lease_owner` deliberately omitted: fencing token is stronger, owner is observability only). Shapes Phase 3 provider work. |
| Incomplete | Risk | Known limitation: pre-lease SQLite rows orphaned mid-claim stay unclaimable | Rows claimed by pre-lease code with a crash before migration have token set + lease NULL (fail-safe). Acceptable pre-1.0; revisit only if migration support lands. |
| Incomplete | Test | Sim scenario: batch prepare time exceeding lease (tail commits fenced) | Deferred to Phase 5 stale-lease scenarios. |
| Incomplete | Doc | Minor follow-ups: batch error skips local-activity pass for committed neighbors (latency only); timeoutless conformance case scans with `limit: 16` (brittle to suite growth) | Needs: opportunistic cleanup in Phases 5-7. |

## Phase 3: Provider decision-logic unification

Goal:
One shared implementation for each cross-provider invariant, ending the drift
that produced the Postgres lost wakeup and four terminal-guard variants.

Scope:
- Extract into `src/provider_util.rs`: terminal-commit guard, post-commit
  ready-reason resolution (including signal-readiness recheck semantics),
  activity retry/timeout decisions, child-terminal event mapping
  (`src/postgres.rs:6246` is already pure and matches `src/sqlite.rs:3577`),
  and the reason/event-type string codecs.
- Port the `signal_wait_ready` recheck into both Postgres commit paths
  (`src/postgres.rs:3037-3060` and `:2575-2600`).
- Memory provider: validate run state before mutating in
  `complete_activity`/`fail_activity` (`src/memory.rs:1011-1027`,
  `:1085-1101`).
- Conformance tests pinning the terminal guard per rejected command type and
  the signal-between-claim-and-commit race.

Completion gate:
The four guard sites call one shared predicate; signal race conformance test
passes on all providers; no behavioral diff remains between providers for the
extracted decisions.

Testing plan:
- Conformance tests for terminal-guard-per-command-type, signal race, memory
  rollback semantics.
- Existing suite green on all providers.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 3A: shared terminal guard, ready-reason, retry, child-mapping helpers | Missing: extraction into `provider_util.rs` with call-site migration. |
| Incomplete | Work | 3B: Postgres signal recheck port (both commit paths) | Missing: recheck in scalar and batch commit. |
| Incomplete | Work | 3C: memory validate-before-mutate | Missing: reordered validation in complete/fail activity. |
| Incomplete | Test | Terminal-guard conformance per command type | Missing: conformance cases. |
| Incomplete | Test | Signal-between-claim-and-commit race conformance | Missing: conformance case. |
| Incomplete | Gate | No behavioral diff for extracted decisions | Missing: all providers pass shared cases. |

## Phase 4: Payload safety

Goal:
GC cannot delete reachable or in-flight data; custom blob stores work;
blob-backed map manifests decode.

Scope:
- GC grace period: `list_payload_blob_digests` returns last-modified
  timestamps; never delete blobs younger than the commit window
  (`src/payload_backend.rs:394-425`; same for SQLite's directory-store GC).
  Report unreadable blobs in the outcome instead of aborting the sweep.
- Mark reachability from refs without downloading every live blob
  (`src/payload_backend.rs:960-1000`).
- Invert URI scheme ownership: each provider treats every non-own scheme as
  opaque, deleting the triplicated allowlist (`src/memory.rs:3260`,
  `src/sqlite.rs:3156`, `src/postgres.rs:8574`).
- Restrict the inline hydration short-circuit to
  `PayloadHydrationKind::Payload` so inline manifest roots with blob pages
  hydrate (`src/runtime.rs:574-586`).
- Commit path: existence check instead of full blob download-and-rehash
  (`src/payload_backend.rs:1138`); normalize activity-map input manifests once
  per commit instead of twice (`:437-457`, `:665-712`).

Completion gate:
Deterministic GC race simulation (upload, GC, commit interleavings) passes;
conformance with a custom-scheme blob store passes; blob-backed activity-map
result manifests decode in a replay test.

Testing plan:
- GC race simulation.
- Custom-scheme conformance.
- Inline-root/blob-page replay test.
- Payload benchmarks confirm the commit-path download removal.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 4A: GC grace period + resilient sweep | Missing: timestamped listing and min-age filter. |
| Incomplete | Work | 4B: ref-based reachability marking | Missing: traversal without leaf downloads. |
| Incomplete | Work | 4C: URI scheme ownership inversion | Missing: per-provider own-scheme checks. |
| Incomplete | Work | 4D: hydration-kind fix for inline manifest roots | Missing: kind-scoped short-circuit. |
| Incomplete | Work | 4E: commit-path existence check + single manifest normalization | Missing: removal of double download/rebuild. |
| Incomplete | Test | GC race simulation (upload, GC, commit interleavings) | Missing: deterministic sim scenario. |
| Incomplete | Test | Custom-scheme blob store conformance | Missing: conformance case. |
| Incomplete | Test | Inline-root/blob-page replay test | Missing: replay test. |

## Phase 5: Deterministic simulation of the real worker

Goal:
The simulation harness exercises production `Worker` + provider code so
crash/race regressions of Phases 1-4 are catchable.

Scope:
- Remove wall-clock `Instant` from `MemoryBackend`: `ready_at` as `TimestampMs`
  compared to `state.now` (`src/memory.rs:96, 275-280, 3312-3318`) so
  `advance_time` controls delayed visibility.
- Fault-injecting `DurableBackend` wrapper driven by `FaultProfile` decisions;
  `SimRun` drives a real `Worker` over `MemoryBackend` per seed.
- Scenarios: crash between claim and commit, cache eviction, commit conflict,
  stale lease, duplicate completion, delayed and reordered delivery.
- Fix `run_many_seeds` collapsing seed 0 into seed 1 (`src/sim.rs:257`).

Completion gate:
Scenario suite runs production worker code and fails when the Phase 2 lease fix
is locally reverted (mutation check); failing seeds reproduce
deterministically.

Testing plan:
- Seeded scenario suite in CI.
- Mutation check documented in this file's ledger.
- Seed-0 unit test.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A: memory backend fully virtual time | Missing: `Instant` removal from `MemoryState`. |
| Incomplete | Work | 5B: fault-injecting backend wrapper | Missing: wrapper driven by `FaultProfile`. |
| Incomplete | Work | 5C: real-worker sim scenarios | Missing: crash/eviction/conflict/stale-lease/duplicate/reorder scenarios. |
| Incomplete | Work | 5D: run_many_seeds seed-0 fix | Missing: seed mixing and unit test. |
| Incomplete | Gate | Mutation check: revert lease fix locally, sim fails | Missing: documented evidence. |

## Phase 6: Provider and runtime performance

Goal:
Remove the known O(N^2) and unbounded-growth hot paths, gated by Criterion
baselines.

Scope:
- Postgres `stream_history` `LIMIT max_events + 1` (`src/postgres.rs:1697`).
- Postgres partial activity-claim index mirroring `src/sqlite.rs:3341`.
- Child-terminal dedup via indexed `command_seq` (or a rebuildable dedup table)
  replacing full-history msgpack scans (`src/sqlite.rs:3605`,
  `src/postgres.rs:6200`, memory equivalent).
- Decision: delete the write-only shard journal, dead snapshot/partition
  tables, and `snapshot_interval` (`src/postgres.rs:656-688, 2633-2679`) until
  shard-native recovery (item 0013) lands.
- Operational-row cleanup for terminal runs (activity tasks, consumed signals,
  dispatched outbox, map results).
- Runtime hot path: match-on-reference before cloning in the consolidated
  `take_*` helper; single-pass replay index build (depends on Phase 1
  consolidation).
- Carry unconsumed ready-event indexes in `CachedWorkflow` so a workflow
  holding an unawaited handle across tasks (spawn early / await late) does not
  cold-replay full history every task; Phase 1's cache drop on
  `has_unconsumed_ready_events` is correct but pays cold replay per task for
  the held-handle pattern. Benchmark the held-handle profile.
- Detection hardening: error when a workflow reaches a terminal state while
  un-replayed command events remain in loaded history (leftover command events
  at terminal are always divergence; `peek_replay_command_event` makes the
  check nearly free).

Completion gate:
Criterion baselines for replay throughput, activity claim/complete, child
fanout, and cached wake show no regressions and measured improvement on the
targeted paths; baselines updated with rationale.

Testing plan:
- Benchmark runs against checked-in baselines.
- Conformance green after schema/index changes (SQLite close/reopen).
- Row-cleanup conformance tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6A: Postgres stream_history LIMIT | Missing: bounded query. |
| Incomplete | Work | 6B: Postgres partial activity-claim index | Missing: index DDL. |
| Incomplete | Work | 6C: child-terminal dedup index | Missing: indexed dedup replacing history scans. |
| Incomplete | Decision | 6D: remove write-only shard journal + dead tables | Missing: removal or explicit deferral rationale. |
| Incomplete | Work | 6E: operational-row cleanup for terminal runs | Missing: cleanup implementation + conformance. |
| Incomplete | Work | 6F: runtime match-before-clone + single-pass index build | Missing: hot-path refactor. |
| Incomplete | Work | 6G: cached unconsumed-index carryover (held-handle cold-replay cost) | Missing: `CachedWorkflow` index carryover + held-handle benchmark. |
| Incomplete | Work | 6H: terminal-with-leftover-command-events divergence check | Missing: check + regression test. |
| Incomplete | Gate | Benchmarks show no regression, improvement on targets | Missing: Criterion evidence with baselines. |

## Phase 7: Production worker shape

Goal:
The worker runs unattended with real concurrency and bounded memory, matching
the README's promised surface.

Scope:
- `Worker::run()` production loop with graceful shutdown;
  `DurableBackend::wait_for_ready(...)` default method (returns immediately) so
  providers can replace polling (Postgres LISTEN/NOTIFY, in-memory notify)
  without a trait reshape.
- Concurrent activity execution bounded by `max_concurrent_activities`
  (JoinSet), stamping heartbeat deadlines at execution start; bounded
  pipelining for workflow tasks or honest renaming of the knobs.
- Bounded LRU workflow cache with a `max_cached_workflows` builder knob
  (`src/worker.rs:158`; `CacheEvicted` semantics already exist).

Completion gate:
The README worker-registration example compiles and runs against SQLite; cache
bound holds under a load smoke test; a slow activity no longer blocks
workflow-task progress.

Testing plan:
- Run-loop integration test with shutdown.
- Cache-bound test.
- Concurrency test with a deliberately slow activity.
- Sim scenarios re-run against the new loop.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 7A: Worker::run() loop + wait_for_ready hook | Missing: production loop and trait default method. |
| Incomplete | Work | 7B: concurrent activity execution | Missing: bounded JoinSet execution. |
| Incomplete | Work | 7C: bounded LRU workflow cache | Missing: cache bound + builder knob. |
| Incomplete | Test | README example compiles and runs as a test | Missing: integration test. |
| Incomplete | Test | Slow-activity non-blocking concurrency test | Missing: test. |

## Phase 8: API honesty and packaging

Goal:
The public surface, docs, and dependency tree state only what is real.

Scope:
- Feature-gate providers (`default = ["sqlite"]`; `postgres`, `s3` features);
  move `tempfile` to dev-dependencies; move the three benchmark bins (notably
  the 3,654-line `src/bin/durust-benchmark-workload.rs`) into a non-published
  workspace crate.
- Implement activity retry backoff (`visible_at_ms` via a shared
  `provider_util` helper) since `RetryPolicy::exponential()` is public and
  spec'd.
- `#[durust::workflow(strict)]` becomes a compile error until implemented;
  rename manifest `*_schema_hash` to reflect type-name hashing or adopt a
  structural fingerprint; rename `cargo durable manifest write` to match its
  copy behavior.
- Replace `select!` source-text digests with a stable structural digest and
  align `select_all` (Decision: breaking history change, acceptable pre-1.0).
- Reconcile `README.md` with the shipped API (post-Phase 7).

Completion gate:
Default-feature build has no Postgres/S3 deps; contract fixtures updated; docs
and examples match the shipped surface; backoff conformance test passes.

Testing plan:
- Feature-matrix CI builds.
- Backoff conformance test.
- Trybuild test for `strict`.
- Contract fixture regeneration.
- README examples compiled as tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 8A: provider feature gates + bench crate extraction | Missing: Cargo features and workspace split. |
| Incomplete | Work | 8B: activity retry backoff | Missing: `visible_at_ms` implementation + conformance. |
| Incomplete | Work | 8C: strict/manifest/CLI honesty | Missing: compile error, rename, CLI rename. |
| Incomplete | Decision | 8D: select! structural digest (breaking history change) | Missing: digest design + implementation. |
| Incomplete | Doc | 8E: README reconciliation with shipped API | Missing: README update post-Phase 7. |
| Incomplete | Gate | Default-feature build has no Postgres/S3 deps | Missing: `cargo tree` evidence. |

## Ordering and Dependencies

- Phases 1-2 fix user-facing failures and wedges first; Phase 3 removes the
  drift root cause before further provider edits; Phase 4 stops data loss;
  Phase 5 locks it all in with real simulation.
- Phase 5 depends on Phase 2 (lease semantics to assert) and the memory
  virtual-time fix; Phase 6's runtime work depends on Phase 1's consolidation;
  Phase 8's README work depends on Phase 7.
