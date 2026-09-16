# 0019: Durability and Recovery Boundaries

## Overarching Goal

Preserve atomic provider mutations, durable blob publication, deterministic
replay, lease fencing, and bounded recovery while simplifying the mechanisms
that enforce them. Review baseline: `46091e5`, 2026-09-16.

The changes address five review findings through shared boundaries: explicit
writer quiescence for destructive GC; one durable local-file publication path;
persistent transactional memory state; cooperative recovery quanta; and
awaitable markers using ordinary replay gates. No history-format migration or
new provider operation is required. Existing consolidation work remains in 0018.

## Findings and Reproductions

| ID | Finding | Original evidence | Resolution |
| --- | --- | --- | --- |
| F1 | GC can delete an old blob after another writer reuses and commits it | `gc_can_delete_a_blob_committed_after_its_final_probe` in [baseline probes](evidence/0019-review-probes.rs) deterministically leaves a committed reference unreadable | Destructive sweeps require explicitly asserted quiescent writers; online dry runs remain available |
| F2 | Local blob put acknowledges before file/directory durability barriers | `LocalDirectoryBlobStore::put_sync` on main writes and renames without syncing | Shared publication performs file and ancestor-directory syncs and propagates errors; injected operation-boundary faults exercise retries |
| F3 | A rejected memory map commit appends history and clears its claim | `failed_memory_map_commit_changes_history_and_loses_claim` contrasts memory `(2 events, lost claim)` with SQLite `(1 event, retained claim)` | Fallible mutations stage persistent table roots and publish only on success |
| F4 | Fixed recovery budgets repeatedly replay the same prefix forever | `finite_recovery_budget_repeats_the_same_prefix_forever`: ten retries, zero commits; unlimited control finishes | Budget exhaustion yields with the continuation intact and replenishes the quantum |
| F5 | Synchronous markers force whole-history replay | Main's Rust `load_cold_history` and TypeScript unlimited-reserve fallback | Markers return awaitables; both fallback mechanisms are removed |
| F6 | Memory history streaming rescans the consumed prefix | Baseline `memory_history_tail_read` grows from ~1.2 µs at 1k events to ~423 µs at 100k | Index the persistent history vector by contiguous event id; a tail read is ~93 ns at 100k |
| F7 | Postgres smoke baseline has stale exact cache counters | Main's unchanged TypeScript runtime reproduces misses=12/hits=20 against expected 8/24 | Correct three expected counters; preserve exact comparisons and speed thresholds |
| F8 | Delayed-release conformance races a 25 ms wall-clock deadline; SQL release bypasses the configured clock | CI run `35158712212` failed `hidden.is_none`; controlled-clock tests then failed visibility on both SQL providers before the fix | Compute release visibility from `ProviderClock`; test t/24/25 ms boundaries and SQLite close/reopen without sleeps |

The final reviewer also reproduced a mixed-workload stall introduced by keeping
cold continuations alive: the old preparation barrier delayed ready cached
commits until every cold replay finished. Phase 4 now commits each ready polling
round and explicitly releases pending claims on a wholesale commit failure.

Baseline probes intentionally assert the old defects and are outside test
discovery. Run them only on `46091e5` in an isolated checkout, copying the file to
`tests/review_0019_probes.rs`, then running
`cargo test --locked --all-features --test review_0019_probes -- --nocapture`.
Current regressions assert the corrected behavior instead.

## Implementation Principles

- Fix the boundary that owns the invariant instead of enumerating bad inputs.
- Share immutable records/history; copy only modified records and index paths.
- Use existing replay gates and fencing rather than a second recovery system.
- Keep SQL transaction boundaries and the existing pure map transition engine.
- Treat retained-memory tests, deterministic fault tests, and actual service
  conformance separately from clean process restarts and machine power loss.
- Record performance costs explicitly; correctness barriers cannot be removed
  merely to restore the old throughput.

## Testing Strategy

Run Rust workspace tests with mandatory Postgres and S3, TypeScript `npm run
check` with Postgres, native conformance, seeded worker simulations, clippy,
formatting, and the sqlite-free feature matrix. Compare Criterion measurements
on the same host against the isolated baseline, including database-size slopes.
Use a dedicated skeptical reviewer after direct implementation; resolve any
findings before opening a ready-for-review PR against main.

## Phase 1: Coordinate Blob Reclamation With Publication

Goal: destructive collection cannot race a writer under the supported contract.

Scope: reject destructive GC unless all writers sharing the provider/store are
stopped and drained for the entire sweep. Preserve online dry runs and recursive
manifest reachability. Remove the ineffective per-delete timestamp probe.

API budget: `writers_quiescent` / `writersQuiescent` adds an explicit deployment
precondition to the existing request; default false fails closed. Timestamp
checks and conditional object deletion cannot compose atomic exclusion with a
later provider commit. A cross-process pin protocol would require durable
writer registration, stale-pin recovery, and transactional commit validation;
that machinery is unnecessary for the chosen offline contract. No workflow API
or provider schema is added. The flag asserts quiescence; it does not stop writers.

Completion gate: an unasserted destructive sweep fails before reading roots or
calling the store; asserted offline sweeps preserve live roots across providers.

Testing plan: replay the delete/commit race at the production boundary; shared
negative conformance on memory, SQLite and Postgres; S3-compatible reachability;
native flag forwarding; SQLite reopen and recursive manifests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Explicit offline contract across providers | `PayloadGarbageCollectionRequest::validate`; all built-in providers and `PayloadBackend` validate first; native wire and TypeScript options forward the flag |
| Complete | Test | 1B: Race refusal and provider behavior | `online_gc_refuses_before_the_delete_commit_race`; shared `destructive_gc_requires_quiescence`; native payload tests; required-service Rust conformance |
| Complete | Gate | 1C: No supported concurrent destructive sweep | SPEC §18, README and native README require stopping/draining all writers; online dry-run cannot delete |
| Complete | Decision | 1D: Keep online reclamation out of this change | Offline collection needs no pin table, lease service, or extra per-upload round trip; timestamp re-probe removed |

## Phase 2: Make Local Blob Publication Durable

Goal: acknowledge local publication only after durable file and namespace barriers.

Scope: sync bytes before rename; sync the directory and every ancestor, including
new nested prefixes; apply barriers to dedup reuse too; reject corrupt existing
bytes; propagate publication failures through both SQLite and the decorator.
Local file calls remain synchronous, matching their existing executor contract.

Completion gate: every acknowledged put crosses the required barriers, and a
failed put cannot publish its reference through the provider.

Testing plan: inject failure after every production publication operation for
new and reused paths, retry after each cut, reopen, and compare bytes. Exercise
concurrent puts and SQLite/native offload. Measure new and dedup 64 KiB puts.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: Shared publication barrier | `LocalDirectoryBlobStore::publish`; `sync_all` before rename and on the full directory chain; errors propagate |
| Complete | Test | 2B: Publication cuts, dedup, corruption, concurrency | `local_publication_crash_boundaries_never_acknowledge_unflushed_data`, `local_dedup_rejects_corrupt_existing_bytes_even_with_matching_size`, `local_directory_store_survives_concurrent_puts_of_one_digest` |
| Complete | Test | 2C: Provider publication failure and reopen | `sqlite_local_blob_store_upload_failure_does_not_commit_missing_payload_ref`; local-directory/native provider conformance |
| Complete | Gate | 2D: Acknowledgement follows all barriers | Production-boundary fault tests plus inspection of shared publication; assumes the filesystem/device honors successful syncs |
| Incomplete | Risk | Actual machine power-loss validation | No disposable VM/device power-cut harness was run. Fault injection and clean reopen do not establish hardware behavior; no such claim is made |

## Phase 3: Make Memory Mutations Atomic

Goal: an error from a provider mutation leaves authoritative state and ownership
unchanged, including late map effects and child-terminal routing.

Scope: `MemoryTransaction` stages persistent ordered tables, shared immutable
records, persistent history and map results. Success swaps the roots; dropping
a failed transaction discards the staged writes. Fully validated claim/release/
heartbeat updates remain in place because no fallible work follows their first
write. Map manifests and descriptors share immutable storage. Keep the shared
map engine; a new generic SQL storage engine is not needed for this invariant.

Performance policy: memory is a development, test, and simulation engine.
Correctness, provider conformance, and one understandable rollback mechanism
take priority over peak throughput. Retain the measured transaction cost and
size-scaling benchmarks; optimize further when development or simulation cost
justifies it. Persistent providers retain their production performance gates.

Completion gate: invalid commits preserve history, projection, descriptor state,
and the original lease; a corrected same-descriptor retry succeeds.

Testing plan: shared malformed-manifest rejection/retry, late child routing
failure, seeded crashes and stale claims, native conformance, existing map retry
and terminal tests. Benchmark mutations at 1/1k/10k/100k unrelated runs.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: Shared transaction boundary | `MemoryTransaction`, `Table`, persistent `RunRecord::history`, shared map descriptors; every fallible mutator stages before writing |
| Complete | Test | 3B: Late failures and valid retries | Shared `rejected_map_commit_is_atomic` on memory/SQLite/Postgres; `a_routing_failure_rolls_back_cancellation_and_child_cleanup`; existing provider and simulation suites |
| Complete | Gate | 3C: Same-claim retry is safe | Regression checks unchanged history/no query projection, then retries the corrected manifest under the original claim and descriptor id |
| Complete | Decision | 3D: Upstream handling before engine rewrite | One rollback mechanism replaces partial-mutation exceptions; existing SQL transactions and map engine remain authoritative |
| Complete | Test | 3E: Cost slope and performance acceptance | `benches/durability_boundaries.rs`; 1/1k/10k/100k measurements in validation evidence; measured transaction overhead accepted for the memory engine's development/test role, retaining shared rollback and scaling checks |

## Phase 4: Turn Recovery Budgets Into Scheduling Quanta

Goal: finite histories make progress under finite positive replay capacity.

Scope: preserve the future, cursor, context, fixed replay target, and claim when a
quantum is exhausted; yield one executor turn and replenish it. Clamp zero
quanta to one. Admission saturation and provider backpressure retain delayed
release. Poll each batch preparation in claim order and commit ready outcomes
before resuming pending recovery quanta, even if the commit batch is not full.
A wholesale commit error explicitly releases prepared and pending claims.
Existing fencing rejects a commit if a successor takes ownership.

API budget: change the existing event/byte/chunk knobs' semantics instead of
adding options. They bound work between cooperative yields, not history length
or wall-clock read rate. Admission bounds active continuations; there is no
additional paused-future cache or lease-renewal API. Provider-level throttling
continues to own physical read-rate limits.

Completion gate: event/byte/chunk quanta smaller than one event still complete;
replay reads each event once under healthy service and stale commits remain fenced.

Testing plan: zero/one/two limits, payloads larger than byte quanta, marker chunk
boundaries, seeded crash/eviction/reordered-fact/lease races, admission saturation,
provider backpressure, and existing hot-work fairness tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: Replenished bounded continuations | `Worker::recovery_request_limits`; no release/restart on quantum exhaustion |
| Complete | Test | 4B: Liveness and read amplification | `finite_recovery_quanta_complete_even_when_smaller_than_one_event`; replay event/byte quantum tests; `budgeted_recovery_streams_markers_without_restarting` |
| Complete | Test | 4C: Failure and scheduling coverage | `tests/sim_worker.rs` runs real-worker crash, eviction, delayed/reordered facts, and lease theft with small event/byte/chunk quanta; existing backpressure and admission tests |
| Complete | Test | 4E: Cached progress during long recovery | `cached_commits_do_not_wait_for_cold_recovery_or_a_full_commit_batch` failed before the pipeline fix and passes for both claim orders and batch sizes 1/2/128; `batch_commit_failure_releases_pending_recovery_claims_and_slots` proves cleanup without lease expiry |
| Complete | Gate | 4D: Finite work progresses without raising limits | SPEC §4.6 and 0009 reconciled; old fixed-prefix repro now reaches `WorkflowCompleted` |
| Complete | Test | 4F: Deterministic delayed release | Four provider/reopen regressions use controlled clocks; SQL regressions failed before `ready_at_ms_for_delay` accepted the provider timestamp and pass after; zero-delay/overflow unit table, required-Postgres conformance, and reviewer pass |

## Phase 5: Make Version Calls Compatible With Streaming Replay

Goal: positional version checks suspend for history instead of forcing bulk replay.

Scope: awaitable `get_version`/`patched`/`deprecate_patch` and TypeScript equivalents;
ordinary replay gates; no reserve, overrun latch, or whole-history retry. Preserve
one marker per call, sequence ids, old default-version behavior, and deprecation.

API budget: a synchronous return value cannot wait for unavailable history.
Awaiting the existing marker primitive changes source syntax only, with no new
provider contract, schema or durable event. Callers must await marker decisions
before entering synchronous serialization/side-effect callbacks. API construction
still enforces re-entry guards, even if the returned future is discarded.

Completion gate: existing histories/corpus replay identically through bounded
chunks; thousands of markers neither restart replay nor expose loading as an error.

Testing plan: repeated ids, absent markers, deprecation, caught errors, unfavorable
fact order, cached/cold replay, single-event chunks, Rust allocation gates,
TypeScript retained-memory slopes, compile/type checks, examples and corpus.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 5A: Shared replay suspension | Rust marker futures and TypeScript `MarkerDurablePromise` use ordinary gates; full-load fallback paths deleted |
| Complete | Test | 5B: Compatible bounded replay | Rust replay/corpus/allocation suites; TypeScript 2,048-marker tests assert increasing read cursors and one event-zero read; repeated-id/deprecation corpus unchanged |
| Complete | Test | 5C: API migration and retained memory | TypeScript negative type fixtures; runtime callback guards; memory tests force inline payloads and retain chunk-size upper bounds and slope checks |
| Complete | Doc | 5D: Contract and earlier exceptions reconciled | README, SPEC §§4.3/15.2, examples, 0017/4F, 0018/1A |
| Complete | Gate | 5E: Final independent review and validation | Dedicated reviewer reran the mixed-workload probe after fixes and found no remaining actionable findings; required-service Rust and TypeScript suites, default features, clippy, examples and benchmark builds pass; see validation evidence |

## Additional Findings and Delivery

F6 uses the same persistent history introduced for F3; contiguous one-based event
ids support indexed bounded reads without prefix scanning. Existing bounded
stream conformance checks semantics; the new tail-read benchmark proves the cost
slope. F7 is independently reproduced against main's TypeScript runtime before
updating three exact counters. The other agent's CI fixes are included as requested.

[Validation evidence](evidence/0019-validation.md) records commands, measurements,
review outcomes, and explicit limits. Online GC pins, actual machine power-cut
validation, and broader SQL engine consolidation remain separate work.
