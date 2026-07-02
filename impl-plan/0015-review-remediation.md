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
| Complete | Work | 2A: lease columns + claim predicates per provider | `claim_lease_until_ms` column (SQLite `workflow_instances` + `ensure_column`; Postgres schema v2) and `WorkflowClaim { token, lease_until }` in memory (virtual `state.now`); claim eligibility is `token is null or lease_until <= now` with the ready reason preserved through the claim; every claim mints a fresh fencing token via `provider_util::claim_lease_until_ms`. Timeout-less activities are governed by the implicit heartbeat lease (row 2E). |
| Complete | Work | 2B: worker single-exit release for fallible awaits | `prepare_claimed_workflow_task` is now a funnel over `prepare_claimed_workflow_task_inner`; every error escaping the inner pipeline (current_time, history chunks, change versions, hydrate, registry, poll) releases the claim at one reconciliation point. |
| Complete | Work | 2C: recovery-slot RAII guard | `RecoverySlotGuard` (Drop over `Arc<AtomicUsize>`) replaces manual `release_recovery`; no early return or `?` can leak `active_recoveries`. |
| Complete | Work | 2D: batch path release-and-continue | `run_workflow_batch_once`: prepare errors release that claim and continue; per-item commit errors release-and-continue via `release_failed_workflow_task` (delayed release for backpressure); a wholesale commit-RPC error releases every uncommitted claim; the first non-backpressure error is propagated after the batch drains. |
| Complete | Test | Lease-expiry conformance (claim, crash, reclaim, fence) | `{memory,sqlite,postgres}_workflow_lease_expiry_reclaims_and_fences_stale_holder*` (SQLite closes/reopens between claim and reclaim; memory advances virtual time): reclaim equals a fresh claim (run, reason, replay target, prefetch), stale commit/release return `StaleLease`, new holder commits. `unexpired_workflow_claim_lease_is_not_reclaimable` runs in the shared suite. Mutation check: disabling lease reclaim in memory fails the memory case. |
| Complete | Test | Timeout-less activity reclaim conformance | `timeoutless_activity_lease_expiry_reclaims_and_fences_stale_holder` in the shared suite (all three providers): no reclaim before lease expiry, reclaim as attempt 2 through `timeout_due_activities`, stale heartbeat/complete/fail all `StaleLease`. |
| Complete | Gate | No worker error path drops a claim or recovery slot | `tests/replay_core.rs`: `claim_is_released_when_current_time_fails_before_prepare`, `cold_recovery_{change_versions,hydrate}_error_releases_claim_and_recovery_slot` (with `max_concurrent_recoveries(1)`), `batch_prepare_error_releases_failed_claim_and_still_commits_neighbors`, `batch_commit_rpc_error_releases_every_claim_in_the_batch`, `batch_per_item_conflict_does_not_abort_the_rest_of_the_chunk`. Mutation checks: leaking the claim in the funnel or the slot in the guard fails these tests. |
| Complete | Decision | Postgres schema v1 -> v2 hard error, no in-place migration | `POSTGRES_SCHEMA_VERSION` bumped for `claim_lease_until_ms`; mismatched schemas error loudly at open. Pre-1.0 stance: operators drop and recreate the schema. |
| Complete | Work | 2F: builder-configurable lease durations | `workflow_task_lease_duration`/`activity_task_lease_duration` knobs (default 30s, 1s floor) threaded to all four claim sites; `activity_lease_duration_knob_bounds_default_option_activity_runtime` pins spurious-timeout vs normal-completion behavior (mutation-checked). |
| Complete | Work | 2G: SQLite ready-index migration | `ensure_index` compares the stored `sqlite_master` definition and drops+recreates on mismatch; `sqlite_reopen_recreates_legacy_ready_index_and_claims_ready_workflows` covers legacy-index reopen. |
| Complete | Work | 2E: implicit heartbeat lease for timeout-less activities | Claiming a task with neither explicit timeout stamps `heartbeat_deadline = now + lease` and persists the interval as `implicit_heartbeat_ms` (memory record field on the virtual clock, SQLite `ensure_column`, Postgres schema v5→v6) instead of writing the lease into `timeout_at_ms`; `heartbeat_activity` re-arms the deadline from the explicit heartbeat timeout first, else the stored implicit interval (`provider_util::activity_heartbeat_deadline_at_ms`), so a heartbeating holder survives indefinitely and one that stops is reclaimed one lease after its last heartbeat by the existing heartbeat-deadline scan. The interval clears on retry and re-stamps at the next claim. Third timeout attribution (`activity_timeout_attribution`) persists `claim lease expired without heartbeat on attempt N` for implicit misses; the two pre-existing messages are pinned byte-for-byte (`timeout_messages_are_pinned`). Conformance: `{memory,sqlite,postgres}_heartbeating_timeoutless_activity_survives_lease_periods*` (memory `advance_time`, SQL short lease + real waits), `timeoutless_activity_reclaims_one_lease_after_heartbeats_stop`, `explicit_heartbeat_timeout_takes_precedence_over_claim_lease`, and `timeoutless_activity_batch_claim_uses_lease_as_implicit_heartbeat` (exercises the Postgres set-based unnest stamping; mutation check: stamping `None` in the batch path fails the case at the attempt-2 deadline pin) in the shared suite (all three providers); Phase 2's `timeoutless_activity_lease_expiry_reclaims_and_fences_stale_holder` passes unchanged. Mutation checks: neutering the heartbeat refresh fails the survives tests (`heartbeating holder must never be reclaimed` → `StaleLease`); neutering the claim stamping fails the reclaim tests (no deadline at all — the task wedges unclaimed, caught by `expect("lease-expired activity should be reclaimable")`). SPEC 6.3/19 and README document the three-deadline liveness model and the lease-as-heartbeat contract (`lease_owner` deliberately omitted: the fencing token is stronger; owner is observability only). |
| Incomplete | Risk | Known limitation: pre-lease SQLite rows orphaned mid-claim stay unclaimable | Rows claimed by pre-lease code with a crash before migration have token set + lease NULL (fail-safe). Same class: a mixed-version fleet on one SQLite file lets an old binary's `heartbeat_activity` null a new binary's implicit heartbeat deadline, leaving the task deadline-less until its holder exits (Postgres is protected by the schema version gate). Acceptable pre-1.0; revisit only if migration support lands. |
| Complete | Test | Sim scenario: batch prepare time exceeding lease (tail commits fenced) | `real_worker_batch_prepare_exceeding_lease_fences_tail_commits` (`tests/sim_worker.rs`, Phase 5): worker A is modeled by raw provider calls (batch-claims three runs through the real claim path, hand-commits the head), virtual time passes the lease, a real worker B reclaims and completes the tail, and A's late tail commits/releases/batch-commit RPC are all fenced as `StaleLease` without disturbing the committed histories. Worker-side reaction to fencing is unit-tested in Phase 2D. |
| Incomplete | Test | Sim scenario: real worker stalled mid-batch-prepare past its lease | The fenced-tail scenario models worker A with raw provider calls; a real `Worker` whose batch prepare stalls past the lease (deterministically via a one-shot `stream_history` hook on `FaultInjectingBackend` that advances virtual time) is untested. Needs: scenario driving both sides through real workers. |
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
| Complete | Work | 3A: shared terminal guard, ready-reason, retry, child-mapping helpers | `provider_util.rs`: `commit_has_workflow_visible_mutations` (all four guard sites), `post_commit_ready_reason` (terminal > same-commit child reason > signal recheck; all four commit paths), `activity_failure_decision`/`activity_timeout_decision`/`timed_out_by_heartbeat` (six fail/timeout sites), `child_terminal_event_data_and_reason` + `child_terminal_map_item_outcome` (all three parent-notify paths), and the persisted string codecs (`reason`, `event_type`, `wait_kind`, `marker_kind`, `parent_close_policy`) deleted from `sqlite.rs`/`postgres.rs`; `persisted_codec_strings_are_pinned` pins every stored string byte-for-byte. |
| Complete | Work | 3B: Postgres signal recheck port (both commit paths) | Scalar commit: one `signal_wait_ready` query per non-terminal commit feeding `post_commit_ready_reason` (matches SQLite's cost). Batch commit: one set-based `signal_wait_ready_run_ids` query per batch (zero when all items are terminal), merged per run before the final unnest update; round trips do not grow per item. Mutation check: forcing `signal_ready = false` in the scalar path fails `signal_between_claim_and_commit_wakes_workflow`; doing so in the batch path alone fails `signal_between_claim_and_commit_wakes_workflows_in_batch_commit`. |
| Complete | Work | 3C: memory validate-before-mutate | `complete_activity`/`fail_activity` validate run existence and terminality before touching the record or payload store; map-item outcomes mark the record completed only after the helper succeeds. Regression `memory::tests::activity_completion_against_terminal_run_fails_identically_on_every_retry` (mutation check: restoring the old order flips the retry to `AlreadyCompleted` and fails the test). |
| Complete | Decision | Terminal guard widened to every workflow-visible mutation | The four sites previously rejected four different subsets (memory: appends; SQLite: appends+child maps; Postgres scalar: appends+child starts+child maps; Postgres batch: appends+activities+waits+signals+projection). All now reject the union of the ten mutation kinds per SPEC "terminal workflow rejects new workflow-visible commands"; an empty commit remains an accepted no-op. State with a valid claim on a terminal run is not reachable through the public API (every terminal transition clears the claim), so no existing test asserted the narrower behavior. |
| Complete | Test | Terminal-guard conformance per command type | Table-driven over the shared `commit_test_support::mutating_commits` catalog with forged terminal state: `memory::tests`/`sqlite::tests::terminal_run_with_live_claim_rejects_every_mutating_commit_kind` and `postgres::tests::postgres_terminal_run_with_live_claim_rejects_every_mutating_commit_kind_when_configured` (scalar and set-based batch paths). Shared suite adds `terminal_run_fences_stale_mutating_commits_identically`: through the public API a cancelled run fences every mutation kind as `StaleLease` identically, because cancellation clears the claim before the guard can fire. |
| Complete | Test | Signal-between-claim-and-commit race conformance | Shared suite (all three providers + Postgres fixture): `signal_between_claim_and_commit_wakes_workflow` (wait created by the racing commit; real `conformance.signal-race` workflow consumes the signal and completes), `signal_during_claim_window_survives_empty_commit` (wait existed before the claim; empty commit must not erase readiness; oldest delivery consumed, second stays inboxed), `signal_between_claim_and_commit_wakes_workflows_in_batch_commit` (two runs through `commit_workflow_tasks`, exercising the Postgres set-based path). `late_activity_completion_after_cancel_is_idempotent_across_retries` pins repeated post-cancel complete/fail as `AlreadyCompleted` on every provider. |
| Complete | Gate | No behavioral diff for extracted decisions | `DURUST_POSTGRES_URL=... cargo test` 2026-07-01: 72 lib, 28 provider_conformance, 107 replay_core, all other suites green with the Postgres fixture exercised (no skips). |
| Incomplete | Risk | Residual mutate-before-validate class (reachable only via forged state) | Memory `fail_map_item` sets `map.completed` and `complete_map_item` inserts results/decrements `in_flight` before their run-terminal checks (SQL providers roll back); `fail_activity`'s retry branch precedes run validation identically in all three providers (`RetryScheduled` against a forged-terminal run). Needs: validate-first pass over map-item paths and a shared decision on the retry-branch ordering. |
| Incomplete | Test | Catalog drift tripwires | `terminal_fence_commits` in `tests/provider_conformance.rs` duplicates the in-crate catalog without a length assertion (a new `WorkflowTaskCommit` field silently skips the fence test); the batch signal-race case only exercises the Postgres set-based path while `upsert_waits` stays simple-batch-eligible. Needs: length pins / eligibility assertion, opportunistic in a later phase. |

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
- Counting blob store pins the commit-path download removal and the
  once-per-commit manifest rebuild.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: GC grace period + resilient sweep | `PayloadGarbageCollectionRequest::min_age` (default `DEFAULT_PAYLOAD_GC_MIN_AGE` = 1h; zero restores unconditional collection); `PayloadBlobStore::list_payload_blobs` returns last-modified timestamps; every GC (decorator, memory `stored_at` on the virtual clock, SQLite `created_at_ms` + directory mtime, Postgres `created_at_ms`) deletes only unreachable blobs older than the cutoff. Dedup re-puts restart the grace period: memory stores and SQL rows refresh their timestamp (Postgres via `on conflict do update`, whose row lock the GC delete's timestamp predicate re-evaluates under), local directory touches mtime; S3 skips (documented: `min_age` must exceed max upload-to-commit latency + one scan). Delete failures increment `PayloadGarbageCollectionOutcome::failed_blobs` and the sweep continues (`payload_backend_gc_records_delete_failures_and_continues`). |
| Complete | Work | 4B: ref-based reachability marking | `collect_reachable_external_payload` marks leaves from the ref digest without loading; only manifest containers load for traversal (a missing reachable container still fails the sweep per SPEC). |
| Complete | Work | 4C: URI scheme ownership inversion | The triplicated `memory-blob:// \|\| s3://` allowlist is deleted; each provider owns exactly its scheme(s) (memory `memory://payload/`, SQLite `sqlite://payload/` + `local://payload/`, Postgres `postgres://payload/`) and treats every other scheme as opaque in normalize, hydrate, roots, and GC. The decorator validates only refs `owns_payload_blob_uri` claims; unknown schemes commit opaquely and error only at hydrate/decode time (`missing_provider_blob_ref_is_rejected` pins both sides per provider). |
| Complete | Work | 4D: hydration-kind fix for inline manifest roots | `ready_payload_or_request` short-circuits inline payloads only for `PayloadHydrationKind::Payload`; manifest kinds always route through the provider manifest hydrators, which handle inline roots. |
| Complete | Work | 4E: commit-path existence check + single manifest normalization | Decorator normalize validates already-offloaded own refs with `payload_blob_exists` (S3 HEAD / map probe) instead of download-and-rehash; map input manifests rebuild once per commit through a per-commit cache shared by the history event and the operational task; `S3BlobStore::put_payload_blob` HEADs before uploading so content-addressed re-puts skip the transfer. |
| Complete | Test | GC race simulation (upload, GC, commit interleavings) | Fresh-upload window: `payload_backend_gc_grace_period_protects_in_flight_uploads` (upload, sweep retains under 1h, `min_age: 0` reproduces the pre-fix delete, committed blob always retained). Dedup window: `memory_blob_store_put_restarts_gc_grace_period_for_reused_digest` / `..._zero_grace_period_collects_reused_digest` (unit) and `sqlite_local_blob_gc_grace_period_and_dedup_mtime_refresh` (close/reopen; backdated mtime collected, in-flight file survives, re-put refreshes mtime). Virtual clock: `memory_gc_grace_period_follows_virtual_clock`. Postgres: `postgres_payload_roots_and_gc_when_configured` covers old vs young orphans and zero-grace collection. Mutation checks: removing the decorator min-age filter, the memory `stored_at` filter, the `MemoryBlobStore` refresh, or the SQLite mtime touch each fails its pinned test. |
| Complete | Test | Custom-scheme blob store conformance | `TestCustomBlobStore` (`test-custom://payload/`) via `PayloadBackend` over memory, SQLite (close/reopen), and Postgres (env-gated): public-API round trip, activity-map round trip, raw replay refs carry the scheme, zero-grace GC leaves reachable blobs, hydration works after GC/reopen. Mutation checks: restoring the memory allowlist fails the memory case with `blob payload must be hydrated...`; restoring the SQLite normalize allowlist fails the SQLite case with `missing payload blob` at commit. |
| Complete | Test | Inline-root/blob-page replay test | `inline_result_manifest_root_with_blob_item_results_hydrates_for_the_workflow`: 2 KiB threshold, ~4 KiB item results; a shape guard asserts the recorded manifest root and pages are inline while item results are blob refs, then the workflow sums the decoded lengths. Mutation check: reverting the kind-scoped short-circuit fails the test (workflow never completes; decode hits `blob payload must be hydrated...`). |
| Complete | Test | Commit-path counting assertions (4E) | `commit_validates_already_offloaded_refs_without_downloading` (`get_count == 0`, existence probes observed; reverting to the old full download fails with `get_count == 1`) and `activity_map_schedule_commit_uploads_each_manifest_blob_once` (`max_puts_for_one_digest == 1`; disabling the rebuild cache fails with 2). |
| Complete | Decision | Postgres schema v2 -> v3 for `payload_blobs.created_at_ms` | Same pre-1.0 stance as Phase 2: mismatched schemas error loudly at open; operators drop and recreate. SQLite adds the column via `ensure_column` (default 0 = pre-existing blobs are immediately past any grace period, which is correct: they are old). |
| Complete | Decision | Additive API changes + fixture updates | `PayloadGarbageCollectionRequest::min_age` (manual `Default`), `PayloadGarbageCollectionOutcome::failed_blobs`, `PayloadBlobStore::{payload_blob_exists, list_payload_blobs}` replacing `list_payload_blob_digests`. Shared contract fixture gains `minAgeMs`/`failedBlobs`; Rust readers and the TypeScript fixture test updated (TS fixture suite green). SPEC section 18 and README Payloads document the grace period, refresh semantics, S3 assumption, scheme ownership, and failure reporting. |
| Complete | Gate | Full suite green with Postgres fixture | `DURUST_POSTGRES_URL=... cargo test` 2026-07-01: 75 lib, 35 provider_conformance, 110 replay_core, all other suites 0 failures (Garage S3 case skips without env but compiles). |
| Complete | Risk | Decorator-store GC reuse window closed by per-delete re-probe | `PayloadBlobStore::payload_blob_last_modified` (defaulted to `Ok(None)` = no fresh information, so third-party stores keep compiling with their prior behavior) is implemented for `MemoryBlobStore` and `S3BlobStore` (HEAD Last-Modified, parsing both HTTP-date and ISO 8601); the decorator sweep re-probes each candidate immediately before its delete and skips (counts retained) any blob whose timestamp moved past the cutoff, so a content-addressed re-put landing between listing and delete survives. Residual window: stores without timestamp support and S3 dedup re-puts (put skips the refresh), both covered by `min_age` sizing per SPEC. Test: `gc_sweep_reprobe_skips_orphan_reput_between_listing_and_delete` (wrapper store re-puts the other sentenced digest inside the first delete — a pre-delete probe cannot observe a re-put inside that same digest's delete call — the sweep retains it, and a second sweep after aging collects it). Mutation check: disabling the re-probe deletes the just-re-put blob (`deleted_blobs` 2 ≠ 1). |
| Complete | Work | 4F: inner providers skip foreign-scheme manifest pages under inline roots | `normalize_activity_map_input_manifest_for_storage` (memory, SQLite, Postgres) passes external pages through untouched, mirroring the reachability collectors; the inner hydrate side had the same hole and skips them identically (shared `map_activity_map_input_manifest_ref` for memory/SQLite, Postgres' own hydrators), so streamed history reaches the decorator with foreign page refs intact for it to hydrate. Test: `inline_manifest_root_with_foreign_scheme_pages_completes_over_{memory,sqlite,postgres}` — `TestCustomBlobStore` via `PayloadBackend` (2 KiB threshold, ~600 B items, five per page) drives a real `activity_map` workflow through a real worker to completion, with a shape guard pinning the recorded manifest as inline root + `test-custom://` blob pages. Mutation check: reverting the memory normalize page guard reproduces `blob payload must be hydrated by the durability provider before decode` at the first commit. |
| Incomplete | Doc | Minor payload follow-ups | Postgres `created_at_ms` uses worker wall clock vs GC-host clock (NTP-academic; DB `now()` would be single-clock); SQLite mtime refresh requires write access to existing blob files (hard-errors on read-only stores); S3 put pays a HEAD per fresh upload (conditional PUT would be one round trip); `parse_iso8601_utc_ms` accepts out-of-range days (malformed-response-only). Opportunistic. |

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
| Complete | Work | 5A: memory backend fully virtual time | `Instant` is gone from `src/memory.rs`: `ready_at` is `TimestampMs` derived from `state.now + delay` and claim eligibility compares against `state.now`, so `advance_time` controls delayed visibility (recovery deferral, backpressure retry, delayed release, nondeterminism backoff). Conformance mirrors the Phase 2 lease pattern: `delayed_released_workflow_task_is_not_claimable_until_visible` is parametrized on time control (memory `advance_time`, SQLite/Postgres real sleep, SQLite reopen variant unchanged); the three replay-core deferral/backoff tests advance virtual time instead of sleeping. |
| Complete | Work | 5B: fault-injecting backend wrapper | `FaultInjectingBackend` (`src/sim.rs`, public like `SimRun`/`FaultProfile`): seeded per-call decisions via the existing `FaultProfile` machinery (`FaultPoint::BackendTransient`); transient errors fail the call before it reaches the inner backend (delayed delivery = failed read retried later); duplicate `complete_activity` requests are replayed through the inner provider's idempotency path; scripted crash-after-claim strands a live claim between claim and commit; one-shot post-claim hook lands racing appends for genuine tail conflicts; commit-conflict observation counters. Determinism pinned by `fault_injecting_backend_decisions_are_deterministic_per_seed`; crash semantics by `crash_arming_holds_the_inner_claim_until_revive`. |
| Complete | Work | 5C: real-worker sim scenarios | `tests/sim_worker.rs`: seven tests, ~960 seeded runs of real `Worker` instances over `MemoryBackend` through the wrapper, all invariants asserted from streamed history (exactly-once completion, correct outputs, contiguous event ids, no duplicate command/terminal/timer/signal events). Scenarios: crash between claim and commit (+ pre-expiry fencing probe), batch-prepare-exceeds-lease (Phase 2 ledger row), cache-eviction storm with fault-free same-seed control-run output comparison, genuine commit-conflict storm (post-claim racing completion; conflicts asserted observed), duplicate activity completions (aggregate injection assertion + per-duplicate `AlreadyCompleted`), delayed/reordered signal-timer-activity delivery, and a same-seed rerun pinning byte-identical final histories for every scenario. Suite runtime ~0.2s. |
| Complete | Work | 5D: run_many_seeds seed-0 fix | `SimRun::new` scrambles the caller's seed through SplitMix64 before it becomes scheduler/fault RNG state, so seed 0 is a distinct run and `SimRun::seed`/`SimTrace::seed` report the caller's value. Unit test `requested_seed_round_trips_and_seed_zero_is_a_distinct_run` pins distinct schedules and fault decisions for seeds 0 and 1 plus seed round-trip. No existing sim test asserted seed-specific schedules; the 2048-seed model check passes unchanged. |
| Complete | Gate | Mutation check: revert lease fix locally, sim fails | Mutation: memory `WorkflowClaim::reclaimable` neutered to `claim.is_none()` (lease expiry disabled). `real_worker_crash_between_claim_and_commit_completes_exactly_once` fails immediately with `deterministic simulation failed: seed=0 invariant=workflow_completes_after_crash message=workflow never completed after the crash and lease expiry` plus the full step trace. Predicate restored; suite green. |
| Incomplete | Doc | Minor sim follow-ups | Scripted scenarios (crash, batch-lease) branch on few seeded decisions, so their seed ranges overstate exploration (storms/reorder are where seeds vary schedules); the wrapper duplicates only the scalar `complete_activity` RPC — if the worker default switches to batch completion, the duplicate-liveness aggregate fails loudly (batch idempotency stays conformance-covered). Opportunistic. |

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
| Complete | Work | 6A: Postgres stream_history LIMIT | `stream_history_inner` (single site behind both `stream_history` and `stream_history_for_replay`) binds `limit max_events + 1`; the +1 row answers `has_more`, byte-budget truncation stays Rust-side. Chunked replay of a 1,025-event history (`history_stream_chunked_replay_postgres`, new bench) −24.9%; single 32-event chunk from 129 events (`history_stream_postgres`) −24.6%. |
| Complete | Work | 6B: Postgres partial activity-claim index | `idx_activity_tasks_claim on activity_tasks(namespace, task_queue, activity_id) where completed = false and claim_token is null`, mirroring SQLite's shape: the Phase 2 lease disjunct applies only to workflow claims (expired activity leases reclaim through `timeout_due_activities`, which nulls the token), so `claim_token is null` still exactly implies activity claim eligibility. EXPLAIN over 5,000 rows: `Limit -> LockRows -> Index Scan using idx_activity_tasks_claim (Index Cond: namespace, task_queue)`, no Sort node (ordered by `activity_id` from the index). `activity_claim_complete_postgres` −14.2%. |
| Complete | Work | 6C: child-terminal dedup index | `HistoryEventData::command_seq()` (generic accessor) persisted as a nullable indexed `history_events.command_seq` column on SQLite (`ensure_column` + one-time decode backfill of legacy child lifecycle rows) and Postgres (schema v4); dedup (`child_event_exists`, `child_terminal_event_exists`, `existing_child_*_command_ids_tx`) is an indexed `(run_id, command_seq, event_type)` lookup with zero msgpack decodes. Memory mirrors it with per-run `child_event_seqs`/`child_terminal_seqs` sets maintained by the single `RunRecord::push_history` append point; all rebuildable from history. `child_workflow_start_parent_wakeup_postgres` −33.7% (with 6D); `child_fanout_completion_sqlite` (new bench, 8 children) is flat (±1%) because WAL fsyncs dominate at that fanout — the removed cost is O(children²) decodes, which this small profile cannot surface. |
| Complete | Decision | 6D: remove write-only shard journal + dead tables | Deleted: `append_shard_journal_tx` and its CTE upsert from both commit paths, the `ShardJournal*` types, `shard_heads_pNN`/`shard_journal_pNN`/`shard_snapshots_pNN`/`history_events_pNN` table creation, `snapshot_interval` (config knob + backend field + metadata key), and the raw journal statements in the `postgres-write-ceiling` diagnostic. Rationale: the journal was written on every workflow commit but never read, snapshotted, or pruned (0013 slices 9-11 that would read it are unstarted), so it was pure per-commit overhead and unbounded growth; 0013 updated accordingly. Shard leases and commit fencing stay (`verify_shard_lease_tx` still runs; batch epoch cache retained). `workflow_task_append_commit_postgres` −25.1%. |
| Complete | Work | 6E: operational-row cleanup for terminal runs | `TerminalCleanup` (provider_util) drives all three providers: terminal transitions DELETE waits, activity tasks, map descriptors, map results, and dispatched child outbox rows (history events root every payload, so GC reachability is unaffected). Tombstone design: none needed for activities — rows exist until terminal cleanup, so complete/fail/heartbeat answer `AlreadyCompleted` from row absence (including across continue-as-new, where the old run row disappears). Signals: undelivered rows stay readable through the inbox; consumed rows (the `signal_id` dedup record) are deleted only for closed runs (further sends already fail `TerminalWorkflow`) and kept across continue-as-new so a retried consumed id stays `Duplicate`. Undispatched outbox rows survive (abandoned children may still start). SPEC 19.1 records the contract. |
| Complete | Test | 6E conformance | Shared suite (memory, SQLite, Postgres): `terminal_cleanup_answers_late_calls_and_keeps_undelivered_signals` (late heartbeat/complete/fail `AlreadyCompleted` across retries, claim scans see nothing, undelivered signal readable + still deduplicated, fresh send fails terminally), `consumed_signal_dedup_survives_continue_as_new`. Row-level pin with close/reopen: `sqlite::tests::terminal_cleanup_deletes_operational_rows_across_reopen` (all operational tables empty, only the undelivered signal row survives, history intact, late complete answers after reopen). |
| Complete | Work | 6F: runtime single-pass index build + take-without-clone | `ReadyEventIndexes::index_events` builds all twelve category maps in one pass over each chunk (was twelve passes in `RuntimeContext::new` + twelve in `append_replay_events`). `take_indexed` now removes the entry and re-files it through `Err` on pending hydration instead of cloning on every poll; `ready_payload_or_request` short-circuits inline payloads with zero copies and clones only the small blob ref when registering a hydration request (same for live signals). Clone-laziness beyond that (storing event indices) was skipped: carried indexes must outlive `replay_events` for 6G, so index entries own their values by design. `workflow_replay_large_history_memory` (new 64-timer bench) −11.2%, `workflow_cached_wake_poll_memory` −8.8%, `workflow_replay_small_history_memory` −5.2%. |
| Complete | Work | 6G: cached unconsumed-index carryover | `RuntimeContext::new` takes carried `ReadyEventIndexes`; the worker stores a committed task's unconsumed entries in `CachedWorkflow` (both scalar and batch commit paths) instead of dropping the entry. Exactly-once holds because a committed non-terminal task has always drained history to its replay target, carried entries' event ids are ≤ the commit tail, and the next chunk streams strictly after it, so no entry can be re-collected; `record_indexed_ready_event_id` already ignores ids behind the cursor. Cache is still dropped for terminal tasks, appended change markers, and provider-appended events past the runtime tail. `held_handle_spawn_then_sleeps_memory` (new bench) −64.6%. |
| Complete | Test | 6G held-handle regression tests | `held_handle_across_sleeps_completes_without_cold_replay_when_cached` and `held_handle_across_sleeps_recovers_with_one_cold_replay_after_crash` (`tests/replay_core.rs`, RecordingBackend without claim prefetch): spawn activity, 3 sleeps, `handle.result()`; exactly one from-zero history stream is allowed (initial task / crash recovery) and the run completes with the held result. Mutation check: disabling worker caching fails both with 4 from-zero replays. |
| Complete | Work | 6H: terminal-with-leftover-command-events divergence check | `reject_terminal_with_unreplayed_command_events` (worker): when a poll reaches any terminal state (completed, failed, continue-as-new — not nondeterminism errors, which already propagate), `peek_replay_command_event` is consulted and remaining unloaded chunks are streamed; a leftover command event fails the task with `Error::Nondeterminism` before any terminal event is committed, and the claim releases with the nondeterminism backoff. Unconsumed ready events stay legal (fire-and-forget). Regression tests: `terminal_with_leftover_command_events_is_nondeterminism` and `..._in_unloaded_chunks_is_nondeterminism` (two recorded timers replayed against a one-timer version; history still ends at the recorded `TimerFired`, no `WorkflowFailed`/`WorkflowCompleted` appended). |
| Complete | Decision | Postgres schema v3 -> v4 for `history_events.command_seq` + claim index | Same pre-1.0 stance as Phases 2/4: mismatched schemas error loudly at open; operators drop and recreate. SQLite migrates additively and crash-atomically: the ALTER, legacy-row backfill, and index creation run in one transaction, so a crash mid-migration rolls back the column and the next open re-runs the whole unit. Regression: `sqlite_reopen_backfills_command_seq_and_preserves_child_terminal_dedup` (legacy DB shape via raw connection; backfill populated; duplicate child-terminal notify suppressed; mutation check: neutered backfill fails the test). |
| Complete | Gate | Benchmarks: no regressions beyond noise, targets improved | Criterion `phase6-before` vs `phase6-after` medians, same session/machine: held-handle −64.6%, Postgres child wakeup −33.7%, Postgres commit −25.1%, Postgres chunked history stream −24.9%, SQLite append commit −19.6%, SQLite activity claim/complete −17.9%, SQLite 1k mixed drain −14.9%, Postgres activity claim/complete −14.2%, large-history replay −11.2%, cached wake −8.8%. Untouched paths within noise: join fanouts ±1.4%, child dispatch −1.7%, Postgres workflow claim −3.0%; `workflow_task_append_commit_memory` read +7.6% once but a rerun against the saved baseline reports "No change in performance detected" (p = 0.14). Reviewer independently reproduced the table from the saved baselines (21 benches, no omitted regressions) and reran memory/SQLite groups (append commit memory −13%, confirming the +7.6% was noise). `tests/benchmark_thresholds.rs` green; `benches/baselines/*.json` untouched (recorded workload snapshots; nothing in this phase lowers their floors). |
| Incomplete | Test | Carried blob-backed index entry across a cached task boundary | The held-handle tests use inline payloads; a carried `PayloadRef::Blob` entry consumed on a later cached task (re-registering hydration) is untested. Machinery shared with the in-task lazy-hydration path and traced sound. Opportunistic. |

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
  (`FuturesUnordered` inside the activity pass, keeping the trait free of
  `Send`-bound and runtime-flavor coupling); `activity_task_batch_size`
  stays the claim-RPC batching knob.
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
| Complete | Work | 7A: Worker::run() loop + wait_for_ready hook | `Worker::run(&mut self)` loops the pass shared with `run_until_idle` (`run_pass_once`) and parks in `DurableBackend::wait_for_ready` (new default trait method: bounded `tokio::time::sleep(max_wait)`) raced against the shutdown notify; `WorkerShutdown` (`Arc<(AtomicBool, Notify)>`, cheap `Clone`) via `Worker::shutdown_handle`/`WorkerBuilder::run`. Transient pass errors do not kill the loop; 16 consecutive failing passes surface the last error. `idle_wait(Duration)` knob (default 100ms, 5ms floor). `MemoryBackend` overrides `wait_for_ready` with a `Notify` signaled by every work-creating mutation (start/signal/cancel, commit, complete/fail activity, timer fires, activity timeouts, child dispatch, `advance_time`; maintenance notifies only when it did work so idle workers cannot wake each other in a loop). `PayloadBackend` forwards; `FaultInjectingBackend` and SQLite/Postgres keep the default sleep. `memory_wait_for_ready_wakes_parked_run_on_new_workflow` passes in ~0.03s with a 3600s `idle_wait`, which is only possible through the notify wake; sim_worker unchanged and green (one-shot drivers never call `wait_for_ready`). |
| Complete | Work | 7B: concurrent activity execution | Decision: `FuturesUnordered` within the activity pass instead of `tokio::spawn`/JoinSet (no `Send`-bound or runtime-flavor coupling). `max_concurrent_activities(n)` is now its own field and the real bound: the pass claims up to n (in `activity_task_batch_size`-sized claim RPCs, that knob unchanged as the round-trip batcher) and polls all claimed executions concurrently, each future owning its heartbeat context. Completions flush through the existing `finish_activities` batching as soon as `activity_completion_batch_size` finishes accumulate (default 1 = per-completion), so fast activities reach the backend while slow ones still run. Documented trade-off: CPU-bound activities that never yield still starve the pass. Heartbeat/lease semantics untouched (2E stays open). |
| Complete | Work | 7C: bounded LRU workflow cache | `max_cached_workflows(usize)` knob (default 10_000, floor 1); `CachedWorkflow::last_accessed_seq` from a monotonic worker counter stamped at the single insert point (`insert_cached_workflow`, used by both scalar and batch commit paths); overflow evicts the min-seq entry via an O(n) scan over the bounded map (documented; no new deps). Eviction is a drop — cold replay rebuilds, including 6G carryover indexes (held-handle tests still green). `#[doc(hidden)] Worker::cached_workflow_count` gives tests bound visibility. |
| Complete | Test | README example compiles and runs as a test | `tests/worker_run.rs::readme_shaped_worker_run_completes_workflow_on_sqlite`: SQLite, client start, one-activity workflow, `run()` joined with a completion probe on a current-thread tokio runtime, graceful stop via `WorkerShutdown`, `run()` returns Ok; every test in the file is bounded by a 30s timeout. `sqlite_run_completes_work_via_default_sleep_wait` pins the default-sleep fallback (work started while parked still completes). README/SPEC worker examples now show `.build()` + `shutdown_handle()` + `run()` (and builder `.run()` for the activity-only shape, which exists in code). |
| Complete | Test | Slow-activity non-blocking concurrency test | `parked_activity_does_not_block_fast_activity_completions`: the slow activity parks on a test-owned `Notify` (logical state, no wall-clock sleeps in asserts); three fast activities' `ActivityCompleted` events land while it holds the pass, the parked run has none, then the gate releases (`notify_one`, permit-buffered) and all four workflows complete. |
| Complete | Test | Cache bound behavioral test | `cache_bound_of_one_forces_cold_replays_for_interleaved_runs` / `cache_bound_above_run_count_keeps_interleaved_runs_cached` (`tests/replay_core.rs`, RecordingBackend without claim prefetch): three interleaved two-timer runs; bound 1 yields exactly 8 from-zero replay streams (the sole warm task is the last run's terminal task) with cache len never above 1; bound 1000 yields exactly the 3 initial streams with all 3 runs cached simultaneously. |
| Complete | Gate | Full suite green with Postgres fixture | `cargo fmt --check` clean; `DURUST_POSTGRES_URL=... cargo test` 2026-07-02: 80 lib, 116 replay_core, 38 provider_conformance, 7 sim_worker, 4 worker_run, all other suites 0 failures (Postgres cases exercised, no skips). |
| Incomplete | Work | Opportunistic: provider push wakeups (Postgres LISTEN/NOTIFY) | `wait_for_ready` gives providers the hook without a trait reshape; SQLite/Postgres currently use the default bounded sleep, so idle wake latency is `idle_wait`. Needs: Postgres LISTEN/NOTIFY (and a cross-process SQLite signal if one exists) behind the same request, plus conformance for spurious-wake and missed-notification staleness bounds. |
| Incomplete | Work | Observability for silently-retried poisoned workflows | A per-task `Nondeterminism` releases with the 60s backoff and errors the pass; `run()` swallows it (counter resets on the next empty pass), so a poisoned run retries forever with no signal. `run_until_idle` callers still see the `Err`. Needs: a stats/callback hook or tracing for repeated per-task failures. Also: `run_pass_once`'s `?` skips maintenance/activity stages on a failing workflow pass. |
| Incomplete | Work | Decouple activity execution from the run-loop pass | `execute_claimed_activities` drains all claimed executions before returning, so one long-running (even heartbeating) activity holds workflow-task processing on that worker until it finishes. Needs: spawn-based execution (requires a `Send`-bound decision) or a pass-yielding design. README perf guidance on `activity_task_batch_size` under high concurrency belongs with it. |

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
| Complete | Work | 8A: provider feature gates + bench crate extraction | `default = ["sqlite"]`; `postgres` gates `tokio-postgres`/`deadpool-postgres` + `src/postgres.rs`; `s3` gates `rust-s3` + `S3BlobStore`; `tempfile` moved to dev-dependencies; `cargo-durable` bin and `replay_core` bench carry `required-features = ["sqlite"]`. Benchmark bins moved to the unpublished `benchtools/` workspace crate (`durust-benchtools`); `tests/benchmark_thresholds.rs` repointed via `include_str!` (it never shelled out to the bins). Feature matrix green: `cargo check --no-default-features`, `cargo check`, `cargo check --features postgres,s3`, `cargo test --all-features`. |
| Complete | Work | 8B: activity retry backoff | `activity_tasks.visible_at_ms` (memory record field, SQLite `ensure_column`, Postgres schema v4→v5) set on fail-path retries via `provider_util::retry_visible_at_ms` (`now + 1s * 2^(failed_attempt-1)`, saturating); claim predicates add `(visible_at_ms is null or visible_at_ms <= now)`; the retry's start-to-close clock starts at the visibility instant so `timeout_due_activities` cannot fire on an invisible task; timeout retries stay immediate (the expired deadline already paced them). Conformance: `memory_activity_retry_backoff_follows_virtual_clock` (advance_time, 999ms/1000ms boundary), `sqlite_activity_retry_backoff_persists_across_reopen`, `postgres_activity_retry_backoff_delays_reclaim_when_configured`; math pinned by `retry_visible_at_doubles_per_failed_attempt_and_saturates`. |
| Complete | Work | 8C: strict/manifest/CLI honesty | `#[durust::workflow(strict)]` is now a compile error ("strict mode is not implemented yet..."), pinned by `tests/ui/strict_mode.rs`. Manifest fields renamed `input_schema_hash`/`output_schema_hash`/`query_state_schema_hash` → `*_type_name_hash` (serde camelCase `inputTypeNameHash` etc.) across manifest, registry, macros, tests; SPEC/README document type-identity-only detection. `cargo durable manifest write` renamed to `normalize`. |
| Complete | Decision | 8D: select! structural digest (breaking history change) | `select!` now records the plain string `select:{branch_count}` (aligned with `select_all:{count}`), replacing the concatenated branch source text. Decision: this breaks replay of pre-change histories containing `SelectWinner` events (digest mismatch surfaces as `Nondeterminism`); accepted pre-1.0. Reorder detection still holds via winner ordinal + command fingerprints (`select_branch_reorder_is_detected_on_replay` green); benign source refactors replay cleanly (`select_branch_source_refactor_replays_recorded_winner` pins digest form and cold replay through a renamed/reformatted variant). |
| Complete | Doc | 8E: README reconciliation with shipped API | README documents the cargo features (`sqlite` default, `postgres`, `s3`), the `durust-benchtools` split, real exponential backoff semantics, the honest provider list (memory/SQLite/Postgres/S3 store), `manifest normalize`, and type-name-hash semantics; compression wording verified still accurate (`CompressionId::None`, no runtime policy). SPEC updated for the renamed manifest fields, unimplemented-strict stance, select digest form, and provider-enforced retry backoff. |
| Complete | Gate | Default-feature build has no Postgres/S3 deps | `cargo tree -e normal | wc -l` = 84 with default features versus 449 with `--all-features` (the old unconditional tree, ~458 pre-split); `cargo check --no-default-features` and `--features postgres,s3` both clean. |
| Incomplete | Doc | Test tree requires the `sqlite` feature | `cargo check --no-default-features --tests` (and `--features postgres --tests`) fails: `tests/{manifest_cli,worker_run,provider_conformance,replay_core}.rs` import `SqliteBackend` unguarded. Lib/bin/bench targets are fully gated; requiring default features for the dev test suite is a deliberate stance. Needs: either gate the four files or document the requirement in CONTRIBUTING-level prose. |
| Complete | Work | Feature-matrix CI step | ci.yml gained a "Check feature matrix" step (`cargo check --no-default-features`, `--features postgres,s3`); the main test step became `cargo test --locked --workspace --all-features` (restoring compile coverage for Postgres/S3-gated tests and the benchtools member), the Garage S3 conformance step passes `--features s3`, and fmt covers the workspace via `--all`. |
| Incomplete | Test | Pin the `select_all:{count}` digest form | `select:2` is pinned by the refactor regression test; nothing pins the `select_all` digest string. Opportunistic. |

## Ordering and Dependencies

- Phases 1-2 fix user-facing failures and wedges first; Phase 3 removes the
  drift root cause before further provider edits; Phase 4 stops data loss;
  Phase 5 locks it all in with real simulation.
- Phase 5 depends on Phase 2 (lease semantics to assert) and the memory
  virtual-time fix; Phase 6's runtime work depends on Phase 1's consolidation;
  Phase 8's README work depends on Phase 7.
