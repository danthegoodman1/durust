# Cross-Runtime Parity

Durust has two implementations, Rust and TypeScript. `SPEC.md` §1.2 states what
they must agree on: the committed history and the provider contract. This file
records how that agreement is *proved* — for each cross-cutting invariant, the
named test in each language that asserts it — and which invariants are proved in
only one language.

**Baseline.** Every status, test name, and source line below is as of commit
`240d603`. Rows that depend on work not yet committed at that point say so
explicitly and name what is missing; nothing here silently describes a working
tree.

## How to read this

- **Both** means a named test exists in each language and asserts that
  invariant.
- **Revert-verified** means more, and is the bar that matters: the invariant was
  actually broken in a scratch worktree and the named test *failed*. A test that
  passes both before and after the fix it supposedly guards pins nothing, and
  reading it is not enough to tell — three of the reverts below were caught by a
  different test than the row named, and three were caught by none.
  §1's revert table gives every revert and its outcome. **Do not upgrade a row
  to revert-verified without running the revert**, and do not describe a row as
  proved when only its neighbour's test moved.
- **Gap** means one column has no test. A gap is a finding, not a footnote: an
  invariant proved on one side only is an invariant that can regress silently on
  the other. A gap row names what is missing and says why the invariant is
  believed to hold anyway, but "believed" is the operative word.
- Test names are verbatim, so they can be run. Do not paraphrase them when
  editing this file, and do not add a row for a test that has not been run.

Running a named test:

```bash
# Rust, crate unit tests (paths shown as `src/runtime.rs::runtime::tests::NAME`)
cargo test --lib -- --exact runtime::tests::NAME

# Rust, integration tests (paths shown as `tests/FILE.rs::NAME`)
cargo test --test FILE -- --exact NAME

# TypeScript, from ./typescript
npx vitest run --config vitest.config.ts packages/core/test/FILE.test.ts -t "NAME"
```

---

## 1. Invariant parity ledger

| # | Invariant | Rust test | TypeScript test | Status |
| --- | --- | --- | --- | --- |
| 1 | A durable API called from inside a side-effect callback fails the task and appends nothing | `tests/replay_core.rs::durable_api_inside_side_effect_fails_the_task_without_recording_markers`; `src/runtime.rs::runtime::tests::durable_api_inside_a_real_side_effect_closure_records_no_marker_pair` | `runtime.test.ts` — `rejects each durable API called from inside a sideEffect callback`; `fails the workflow task when a durable API is called inside a sideEffect callback` | **Both** |
| 2 | A durable API called from user conversion code run inside a command builder fails the task and appends no inverted marker pair | — **gap**, see note 2 | `runtime.test.ts` — `rejects a durable API re-entered while a durable command converts its values` | **Gap (Rust)** |
| 3 | A durable API called from a map-manifest builder's iterator adapter or item conversion is legal | `src/runtime.rs::runtime::tests::manifest_builders_run_caller_iterators_outside_the_context_borrow` — legality only. It builds both manifests and asserts the markers, but **never schedules a map command**, so the "markers land ahead of the map command" half of the §16 claim is inferred from seq allocation rather than asserted. | `runtime.test.ts` — `allows a durable API called from a map-manifest item conversion` — legality **and** ordering: it schedules the map and asserts `[VersionMarker#1, VersionMarker#2, ActivityMapScheduled#3]` | **Both** for legality; **gap (Rust)** for the ordering clause |
| 4 | A durable API called from the workflow output's conversion fails the task without committing: no marker ahead of a terminal event, and the run keeps its history | `tests/replay_core.rs::durable_call_from_output_serialize_fails_the_task_without_committing` | `runtime.test.ts` — `appends no marker when the workflow output encoding re-enters a durable API` | **Both**, converged — see note 4 |
| 5 | A fault in workflow handler code fails its task without committing and the worker keeps serving: the claim is released with the nondeterminism backoff, runs claimed in the same batch still commit, and only a durable failure closes the run as `WorkflowFailed` | `tests/worker_run.rs::panicking_workflow_fails_its_task_and_the_worker_keeps_serving`; `tests/worker_run.rs::panicking_workflow_batched_with_a_healthy_task_still_commits_its_neighbor` | `worker.test.ts` — `releases a workflow task whose handler threw a plain error and keeps the run`; `commits WorkflowFailed for a handler that throws a durable failure`; `drains a claimed workflow batch when an earlier task fails`; corpus case `a workflow that fails on purpose commits WorkflowFailed with its own failure` | **Both**, converged — see note 5 |
| 6 | A fault in activity handler code fails the activity through its retry policy, not the worker | `tests/worker_run.rs::panicking_activity_fails_its_task_and_the_retry_policy_completes_the_run`; `tests/replay_core.rs::panicking_activity_exhausts_its_retry_policy_and_records_the_panic_message` | `worker.test.ts` — `persists activity handler failures and replays them into workflow code` | **Both** |
| 7 | Reaching a terminal state with a recorded command left unconsumed is nondeterminism, including when the leftover sits in an unloaded chunk | `tests/replay_core.rs::terminal_with_leftover_command_events_is_nondeterminism`; `tests/replay_core.rs::terminal_with_leftover_command_events_in_unloaded_chunks_is_nondeterminism` | `runtime.test.ts` — `rejects terminal completion with leftover recorded timer commands`; `rejects terminal app failure with leftover recorded timer commands without hanging`; `rejects continue-as-new with leftover recorded timer commands` | **Both** |
| 8 | Exactly-once ready-event consumption. Two facets: **(a)** a consumed ready event is *removed* from its index and cannot be consumed twice, and **(b)** an unconsumed ready event never enters the replay window, so it cannot block command matching. Both Rust facets are pinned by the same single test — see note 8. | **(a) and (b)** `src/runtime.rs::runtime::tests::peek_replay_command_event_skips_unconsumed_ready_events_without_consuming_them`. Its closing `…is_none()` assertion is the only thing in the Rust suite that fails when removal reverts to a read, and its opening peek is the only thing that fails when the ready-event skip is removed. `indexed_ready_events_are_skipped_when_consumed_before_the_replay_cursor` is supporting coverage, **not a detector for either facet** (note 8). | **(a)** `worker.test.ts` — `holds replay memory proportional to historyFetchMaxEvents, not to history length`; `runtime.test.ts` — `retains no measurable memory per completed hot activity`. **(b)** `runtime.test.ts` — `serves a second sequential read of one activity handle from the handle itself`; `settles a joinAll whose branches complete in separate tasks` | **Both**, revert-verified |
| 9 | No unbounded in-workflow collections: a hot run's retained memory does not grow with the number of ready events it has consumed | — **gap**, see note 9 | `runtime.test.ts` — `retains no measurable memory per completed hot activity` | **Gap (Rust)** |
| 10 | Replay memory is bounded by chunk size, not by history length | — **gap**, see note 10 | `worker.test.ts` — `holds replay memory proportional to historyFetchMaxEvents, not to history length` | **Gap (Rust)** |
| 11 | Chunked replay commits the same history as unchunked replay, payload bytes included | `tests/replay_core.rs::out_of_order_completion_before_new_activity_command_cold_multi_chunk`; `tests/replay_core.rs::large_inline_command_payloads_replay_cold_multi_chunk` | `worker.test.ts` — `commits the same events whether history arrives in one chunk or many` | **Both** |
| 12 | An abandoned workflow execution is disposed deterministically rather than left parked forever | — **gap**, see note 12 | `worker.test.ts` — `disposes a parked hot workflow execution when facts land under its claim`; `disposes a parked hot workflow execution after a failed workflow task`; `disposes a parked hot workflow execution evicted from a full execution cache`; `disposes a parked hot workflow execution when the execution cache is disabled`; `disposes a hot workflow execution superseded by a cold replay`. `runtime.test.ts` — `settles parked durable-API waiters when a hot execution is disposed`; `disposes idempotently and never produces a commit afterwards` | **Gap (Rust)** |
| 13 | A slow activity in flight does not block workflow progress on the same worker | `tests/worker_run.rs::workflow_task_commits_while_a_multi_second_activity_is_in_flight` | `worker.test.ts` — `commits a workflow task while a multi-second activity is in flight on the same worker` | **Both** |
| 14 | A failing workflow stage does not suppress activity completions | `tests/worker_run.rs::injected_workflow_claim_failure_still_lets_an_activity_complete` | `worker.test.ts` — `completes an activity while every workflow claim fails` | **Both** |
| 15 | Maintenance load is bounded by elapsed time, not by the workflow task rate | `tests/worker_run.rs::maintenance_scans_track_elapsed_time_not_the_workflow_task_rate` | `worker.test.ts` — `paces maintenance by elapsed time rather than by workflow task count` | **Both** |
| 16 | A poisoned workflow task is counted, and counted apart from genuine history divergence | `tests/worker_run.rs::workflow_panics_and_re_entrancy_are_counted_apart_from_divergence`; `tests/worker_run.rs::repeated_nondeterministic_replays_are_counted_and_never_confused_with_panics` | — **gap**, see note 16 | **Gap (TypeScript)** |
| 17 | Nondeterministic host state is rejected in workflow code | `tests/compile_fail.rs::workflow_determinism_lints_compile_fail` | `determinism-guards.test.ts` — `rejects Date.now() in workflow code`; `rejects Math.random() in workflow code`; `rejects setTimeout and Promise.race in workflow code` | **Both**, by different mechanisms — see note 17 |
| 18 | Map fanout follows one shared transition table in both engines: admission bounded by `maxInFlight`, one replacement per released slot, a failed map abandons its siblings | `tests/map_transitions.rs::memory_replays_every_shared_table_fanout`; `tests/map_transitions.rs::sqlite_replays_every_shared_table_fanout` | `map-fanouts.test.ts` — `shared map transition table fanouts > fanout: <case>` (five cases, generated from the table, over the Rust memory provider through `@durust/native`; the `transitions` half has a Rust runner only, because the map engine lives in Rust) | **Both** |
| 19 | A worker crash between claim and commit completes the run exactly once | `tests/sim_worker.rs::real_worker_crash_between_claim_and_commit_completes_exactly_once` | `simulation.test.ts` — `recovers a workflow task after a worker crashes with an uncommitted claim` | **Both** |
| 20 | Cache eviction mid-run does not change the committed outcome | `tests/sim_worker.rs::real_worker_cache_eviction_storm_matches_fault_free_control` | `simulation.test.ts` — `survives a cache-eviction replay soak across concurrent mixed workflows` | **Both** |
| 21 | A commit that both schedules an empty map and closes its run is accepted, and no map fact lands behind the run's own terminal event — for the activity-map arm, which `terminalParent` would have rejected, and the child-map arm, which it would have let through | `tests/provider_conformance.rs::memory_provider_passes_basic_conformance`; `tests/provider_conformance.rs::sqlite_provider_passes_basic_conformance` — the `an_empty_map_scheduled_by_a_closing_commit_is_still_accepted` section | `native-conformance.test.ts` — shared case `an empty map scheduled by a closing commit is accepted and appends nothing`, over memory, SQLite, and Postgres | **Both**, revert-verified on the old TypeScript engine (dropping its `parentTerminal` branch failed the case on all three providers with `newTailEventId: 4`); both columns now run `src/map_engine.rs`, whose `DescriptorCreated` arm the shared case reaches through `@durust/native` |
| 22 | A run's waits are deleted by the same transaction that closes it (`SPEC.md` §19.1), so operational storage does not grow with closed runs and maintenance scans do not pay for them | `tests/provider_conformance.rs::memory_terminal_cleanup_deletes_a_closed_runs_waits`; `::sqlite_terminal_cleanup_deletes_a_closed_runs_waits_across_reopen`; `::postgres_terminal_cleanup_deletes_a_closed_runs_waits_when_configured`; `::postgres_closing_commits_delete_wait_rows_on_both_commit_paths_when_configured` | `native-conformance.test.ts` — shared case `terminal cleanup deletes a closed run's waits`, over memory, SQLite, and Postgres | **Both**, revert-verified on both sides. TypeScript: removing the cleanup failed the shared case on memory and SQLite (`fired 0`) and the Postgres row assertion on both commit paths. Rust: the detector is the *scan budget* — two runs, one due timer wait each, `limit: 1` — and each provider's cleanup revert failed it with `a closed run's leftover wait must not spend the due-timer scan's only slot; fired 0`, `left: 0 / right: 1`. The Postgres batch commit path is covered separately and its non-vacuity is established by mutation rather than inspection: the test cannot observe which path a commit took, but deleting **only** the batch path's `cleanup_runs_operational_state_tx` call fails it, which is possible only if the batch reached that path |
| 23 | A due-timer scan never appends `TimerFired` to a run that has already reached a terminal event | `tests/provider_conformance.rs::memory_stray_timer_wait_never_fires_against_a_closed_run`; `::sqlite_stray_timer_wait_never_fires_against_a_closed_run_across_reopen`; `::postgres_stray_timer_wait_never_fires_against_a_closed_run_when_configured` | `native-conformance.test.ts` — shared case `a stray timer wait never fires against a closed run`, over memory, SQLite, and Postgres | **Both**, revert-verified on both sides — removing each guard failed with `a closed run's timer wait must not fire; fired 1`. Rust forges the state the same way TypeScript does: the wait is committed by a **second, live run** naming the already-closed run, so it exists against a terminal run rather than being cleaned up first. Under the memory revert the second assertion reports the corruption directly — `left: [WorkflowStarted, WorkflowCompleted, TimerFired] / right: [WorkflowStarted, WorkflowCompleted]` |
| 24 | A commit's wait, signal, and wait-deletion mutations are fenced to the claimed run (`SPEC.md` §8.2): a wait forged for another run never reaches storage, so a closed run cannot acquire a stray wait through the public API, and the due-timer scan's one-slot sweep goes to the live run's timer | `tests/provider_conformance.rs::memory_stray_timer_wait_never_fires_against_a_closed_run`; `::sqlite_stray_timer_wait_never_fires_against_a_closed_run_across_reopen`; `::postgres_stray_timer_wait_never_fires_against_a_closed_run_when_configured` — all three call the shared driver `a_fenced_stray_wait_leaves_the_scan_budget_to_the_live_run`, and the Postgres case additionally asserts the absent row through `postgres_wait_row_count`; the shared scenario `commit_side_signal_and_wait_mutations_are_fenced_to_the_claimed_run` pins the signal and wait-deletion halves on every provider | `native-conformance.test.ts` — shared cases `a stray timer wait never fires against a closed run` and `commit-side signal and wait mutations are fenced to the claimed run`, over memory, SQLite, and Postgres | **Both**, revert-verified: with the Rust fence removed the shared scenario fails on all three providers with `run b's signal must survive run a's commit`. The due-timer scan still skips rather than deletes a closed run's wait (`SPEC.md` §14), but that state is no longer reachable through `DurableBackend`, so the skip is defence in depth rather than a pinned contract |

| 25 | One change id consulted on both sides of a task boundary records one `VersionMarker` per call, and the second call replays positionally on hot and cold paths | `tests/replay_core.rs::repeated_change_id_across_task_boundary_cached`; `::repeated_change_id_across_task_boundary_cold`; `::repeated_change_id_across_task_boundary_cold_single_event_chunks` | corpus case `one change id consulted on both sides of a task boundary records two markers` (the `expect` blocks are Rust's; TypeScript reproduced them) | **Both**, revert-verified: with the Rust marker index restored all three replay tests fail at the history assertion |
| 26 | A `SignalConsumed` is matched by its command id, not by its position, so a `select` or `join` whose signal branch registered before a timer replays cold | `src/runtime.rs` — `ReadyEventIndexes::consumed_signals`; corpus case `a signal that won a select whose timer registered later replays cold by command id` | `worker.test.ts` — `signal branches replay by command id` (six cases: select signal-first, select timer-first, join signal-first, each hot and cold); the same corpus case | **Both**, revert-verified: with the positional match restored the two cold signal-first cases fail |
| 27 | A change-marker API that reaches the end of a partially loaded window fails the poll and the worker replays the run with its whole history, instead of answering from a provider-side index | `tests/replay_core.rs::recorded_version_marker_beyond_loaded_window_replays_with_full_history` | `worker.test.ts` — `repairs a marker run that outgrows the replay window reserve` | **Both**; the Rust reload takes a fresh recovery budget (`tests/replay_core.rs::budgeted_recovery_reloads_the_whole_history_after_a_marker_overrun`) |
| 28 | A signal to a closed run is rejected as terminal and is not stored | `tests/provider_conformance.rs::provider_conformance` — the `terminal_cleanup_answers_late_calls_and_keeps_undelivered_signals` section (`Err(TerminalWorkflow)`) | `native-conformance.test.ts` — shared case `a signal to a closed run is rejected`, over memory, SQLite, and Postgres | **Both**, revert-verified: the TypeScript case fails on all three old providers |
| 29 | A history stream honors `maxBytes` with at least one event per chunk | `tests/provider_conformance.rs::provider_conformance` — the `stream_history_honors_bounds` section | the three conformance files — `stream history honors maxBytes with at least one event per chunk` | **Both**, revert-verified on TypeScript |
| 30 | A late commit whose claim lease was reclaimed is fenced by the claim token even when its expected tail is still current | `tests/sim_worker.rs::expired_lease_correct_tail_commit_is_fenced` | `simulation.test.ts` — `fences a late commit whose tail is still current once its lease was reclaimed` | **Both**, revert-verified: disabling the memory provider's token check fails each with `Ok(Committed { new_tail_event_id: EventId(2) })` / `promise resolved instead of rejecting` |
| 31 | A closing run tombstones its live plain activities and cancels its own `Cancel`-policy children in the same transaction, on every commit path | `tests/provider_conformance.rs::provider_conformance` — the `terminal_run_fences_stale_mutating_commits_identically` and `parent_close_policy_cancel_cancels_child` sections | `native-conformance.test.ts` — shared cases `closing a run tombstones its live plain activity` and `a closing child cancels its own Cancel-policy children`, over memory, SQLite, and Postgres | **Both**; the revert on record is the old TypeScript Postgres provider, whose fast path failed both cases; the shared cases now run over the Rust providers, so the two columns exercise one implementation |

| 32 | Retry pacing is one model in both runtimes: `{initialIntervalMs, maxIntervalMs, maxAttempts, backoffCoefficient, nonRetryableErrorTypes}`, a retry visible `min(max, initial * coefficient^(n-1))` after attempt `n` fails, a listed error type ending retries, and a plain activity's timed-out attempt paced the same way | `src/provider_util.rs::tests::retry_visible_at_doubles_per_failed_attempt_and_saturates`; `tests/provider_conformance.rs::memory_timeout_retry_becomes_visible_after_the_policy_backoff` | shared case `expired timeout-less activity lease honors nonzero retry backoff before reclaim`, run by the three conformance files and by `packages/native/test/native-conformance.test.ts` over the Rust memory and SQLite providers | **Both** |
| 33 | A release keeps the run's wake reason; `WorkflowTaskRelease` carries only the visibility delay, and a superseded release leaves the newer claim alone | `src/postgres/tests.rs::postgres_delayed_visibility_survives_reconnect_when_configured` (asserts `WorkflowStarted` after the release) | shared case `released workflow task claims are immediately reclaimable and stale releases are no-ops`, also green over the Rust providers in the native suite | **Both** |
| 34 | `query_projection` tells a missing workflow (`NotFound`) from one that has published no projection (`NoProjection`) | `tests/provider_conformance.rs::query_projection_updates_atomically_and_reads_payload_refs` | shared case `workflow commit publishes the latest query projection atomically`, also green over the Rust providers in the native suite | **Both** |
| 35 | A timeout's history message names the deadline that lapsed (`start-to-close timed out`, `missed heartbeat`), and a colliding child id fails its start with `child workflow id already exists: <id>` | `src/provider_util.rs::tests::timeout_messages_are_pinned`; `src/memory.rs`, `src/sqlite.rs`, `src/postgres.rs` at `child workflow id already exists` | shared cases `activity start-to-close timeout appends terminal timeout and fences completion`, `activity heartbeat timeout retries before terminal workflow wake`, and `child workflow map collect-all records item failures and still completes`, also green over the Rust providers in the native suite | **Both** |
| 36 | Start-to-close is measured from the claim: a task waiting in the queue has no deadline, the claim stamps `timeout_at` for the attempt it starts, and a retry's clock starts at its next claim | `tests/provider_conformance.rs::memory_start_to_close_deadline_starts_at_the_claim` (a queued attempt outlives five timeouts unclaimed, then lapses one timeout after its claim; revert-verified: stamping no deadline at claim fails it); the claim paths stamp the deadline and the schedule and retry paths store none in `src/memory.rs`, `src/sqlite.rs`, and `src/postgres.rs` | shared cases `activity start-to-close timeout appends terminal timeout and fences completion` and `a map item that misses its start-to-close deadline times out and fails its map`, green over the Rust providers in the native suite | **Both** |

| 37 | A commit starts the children it requests and the child-map items it admits before it returns, in every provider; a due-timer scan never spends budget on another namespace's waits and deletes a wait whose run is gone | `tests/replay_core.rs` asserts `child_workflow_starts_dispatched == 0` across the child and select suites and `sqlite_child_started_by_a_commit_survives_close_and_reopen`; `tests/worker_run.rs::disabled_timer_maintenance_dispatches_child_starts_from_the_interval_loop` counts the drain call the worker still makes; the corpus script no longer carries a `dispatchChildStarts` step | shared cases `child workflow start is durable and wakes parent while making child claimable`, `child workflow map materializes bounded children and writes ordered outcome manifest`, and `child workflow map collect-all records item failures and still completes`, green over the Rust providers in `packages/native/test/native-conformance.test.ts` without any drain in the binding | **Both** |

Five of thirty-seven rows are gaps: 2, 9, 10, 12 (Rust), and 16 (TypeScript).
Rows 32 to 37 closed when the Rust providers took the TypeScript provider
contract's answers while the `durust-node` binding was measured against the
shared conformance cases.
Rows 4 and 5 closed when the two runtimes converged on one disposition for a
workflow-code fault; row 24 was rewritten when the commit fence made the
forged-wait state unreachable through the public API.

### Revert verification

Every **Both** row's invariant was broken in a scratch worktree and its cited
tests re-run. A row is *revert-verified* only where the cited test failed.

| Row | Revert applied (both languages unless noted) | Rust | TypeScript |
| --- | --- | --- | --- |
| 1 | Rust: `with_context` keeps the live pointer during `f` instead of parking the sentinel. TS: the durable-API gate ignores the user-code frame. | **caught** (2/2) | **caught** (2/2) |
| 3 | Rust: both manifest builders drain the caller iterator *inside* the borrow. | **caught** | **not run** — no localized revert exists; TS legality is structural (`activityMapManifest` is a free function holding no frame), so there is nothing to switch off |
| 5 | Rust: remove `catch_unwind` from `poll_cached`. TS: (a) handler rejection not converted to `WorkflowFailed`, (b) first failing task aborts its batch. | **caught** (2/2) | **caught** (2/2) |
| 6 | Remove the activity poll's `catch_unwind` / `catch`. | **caught** (2/2) | **caught** |
| 7 | Accept the unreplayed command event at a terminal state. | **caught** (2/2) | **caught** (3/3) |
| 8 | Removal-on-consume reverted to a read; separately, ready events left in the replay window at ingest. | **caught** | **caught** (2/2) |
| 11 | Rust: an unloaded replay window is read as the replay tail. TS: the replay gate never parks for more history. | **caught** (2/2) | **survived** — `commits the same events whether history arrives in one chunk or many` stayed green; only row 10's memory test caught it |
| 13, 14 | Run the three worker loops sequentially instead of concurrently. | **caught** (2/2) | **caught** (2/2) |
| 15 | Maintenance never paces; it re-scans immediately forever. | **caught** | **caught** |
| 17 | Rust: the `#[workflow]` macro's nondeterminism lint never fires. TS: the guarded globals never throw. | **caught** | **caught** (3/3) |
| 18 | Over-admit the map by one slot (`slot_limit` / `mapSlotLimit` + 1). | **caught** (2/2) | **caught** (2 of 4 table cases; the other two are bound-insensitive by construction) |
| 19, 20 | The memory provider stops fencing stale commits (`expected_tail_event_id` / `expectedTailEventId` check disabled). | **survived** | **survived** |

**Ten of the fourteen `Both` rows are revert-verified in both languages.** Three
results are worth carrying rather than burying:

- **Row 11, TypeScript.** The test named for "chunked replay commits the same
  history as unchunked" does not detect a replay gate that never parks. Its two
  runs still agree, because that history is delivered before the workflow needs
  it either way. The defect was caught only by row 10's memory test. The row's
  TypeScript column is therefore weaker than its wording implies.
- **Rows 19 and 20.** Disabling stale-commit fencing in both memory providers was
  detected by **neither** simulation. That is one revert, not a proof that the
  sims pin nothing — but it is the revert those rows most obviously ought to
  catch, and they did not. Treat both rows as asserting that the scenarios *run
  clean*, not that they would notice this class of regression.
- **Row 3, Rust.** See the row: the legality clause is verified, the ordering
  clause is not asserted at all.

### Notes

**Note 2 — Rust has the guard but not the test.** Rust's command builders encode
their payloads *inside* the context borrow: `match_or_append_command`
(`src/runtime.rs:853`) allocates the command id and then calls the per-kind
`prepare` closure, which runs `runtime.encode_payload(...)` (for example
`src/runtime.rs:2386` on the activity path). A durable API called from a user
`Serialize` impl during that encode re-enters `with_context` and hits the
re-entrancy sentinel, so the task fails with no half-appended command. The
mechanism is real, and `src/runtime.rs::runtime::tests::nested_durable_api_call_is_rejected_instead_of_aliasing_the_context`
proves the sentinel works — but it drives a synthetic nested `with_context`, not
a `Serialize` impl that calls a durable API. No Rust test exercises the
conversion window. If a future change hoisted encoding above the borrow, the
synthetic test would stay green.

**Note 4 — converged on "fail the task".** Rust encodes the workflow output
under the context borrow (`src/runtime.rs::encode_workflow_output`), so a
durable API called from the output's `Serialize` trips the re-entrancy guard
and the task fails as `Error::TaskPanic` with nothing committed. TypeScript's
`completeWorkflow` runs outside the workflow's `AsyncLocalStorage` scope, so
the same call finds no context; the rejection is a plain `Error`, which the
disposition in note 5 routes to `WorkflowCodeError`, and again nothing is
committed. The two mechanisms differ; the commit does not: both runtimes leave
`["WorkflowStarted"]` and an open run, which the cited tests assert on each
side.

**Note 5 — one disposition in both.** Rust routes a caught panic through
`Error::TaskPanic`: nothing is committed, the claim is released with the
nondeterminism backoff, and the next claim replays the run, so a redeploy
recovers it. TypeScript now does the same for any handler rejection that is not
a durable failure, through `WorkflowCodeError`. What closes a run as
`WorkflowFailed` is the value a workflow means as its failure: `Err(...)` in
Rust; in TypeScript a `WorkflowFailure`, a propagated activity or child
failure, or any `DurableFailure`-shaped value. A durable API refusing the
workflow's own values is a code fault in both runtimes: in TypeScript that is a
plain `Error` (a malformed manifest, a non-object `continueAsNew` input, an
empty side-effect key); in Rust it is `Error::PayloadEncode` or
`Error::PayloadDecode` returned by the API, which
`fails_workflow_task_without_committing` now routes like a panic and
`WorkerMetrics::workflow_tasks_faulted` counts
(`tests/replay_core.rs::durable_api_refusing_a_value_fails_the_task_without_committing`).
`SPEC.md` §4.2 states the rule; the corpus case `a workflow that fails on
purpose commits WorkflowFailed with its own failure` pins the terminal half
across both runtimes.

**Note 8 — which test kills which revert, measured.** Both facets were checked by
reverting the behaviour in a scratch worktree and re-running, because the two
sets of tests are not interchangeable and the obvious pairing is wrong.

Reverting removal-on-consume — re-inserting the entry after a successful hydrate
in `RuntimeContext::take_indexed` (`src/runtime.rs:960`), and deleting
`index.delete(key)` from `#takeReadyEvent`
(`typescript/packages/core/src/runtime.ts:1461`) — leaves facet (b)'s tests
**green in both languages**: `indexed_ready_events_are_skipped_when_consumed_before_the_replay_cursor`
passes, and so do `serves a second sequential read of one activity handle from
the handle itself` and `settles a joinAll whose branches complete in separate
tasks`. They assert cursor skipping and handle-owned re-reads, neither of which
depends on the index shrinking.

What fails is facet (a): Rust
`peek_replay_command_event_skips_unconsumed_ready_events_without_consuming_them`
at `skipped completion must be consumable exactly once`, and TypeScript both
memory tests — `expected 98966.6425 to be less than 2048` per completed activity
(one retained 96 KiB payload each) and `expected 7827680 to be less than 1638400`
on the replay window. So exactly-once *removal* is pinned only by one Rust unit
test and by the two memory tests, and a row citing facet (b)'s tests for it
would have pinned nothing.

**Facet (b) has the same problem, and the first fix reproduced it.** Rust skips a
ready event at the cursor head through *two* independent paths: the
`is_index_consumable_ready_event` test in `peek_replay_command_event`, and
`skip_consumed_replay_events` for events already consumed. Measured, one at a
time: removing the first fails
`peek_replay_command_event_skips_unconsumed_ready_events_without_consuming_them`
and leaves `indexed_ready_events_are_skipped_when_consumed_before_the_replay_cursor`
**green**; making the second a no-op leaves **both** green. The event that second
test consumes is skippable by either mechanism, so it dies only if both go at
once — it is doubly redundant and detects neither path on its own. Both Rust
facets are therefore pinned by the single peek test, and this file said otherwise
until the revert was actually run.

**Note 9 — the removal is pinned; the memory bound is not.** Removal-on-consume
itself is asserted: `peek_replay_command_event_skips_unconsumed_ready_events_without_consuming_them`
fails when it reverts (note 8). `RuntimeContext::take_indexed`
(`src/runtime.rs:960`) removes before hydrating and `CachedWorkflow` carries only
`unconsumed_indexes` forward, so the retained set is bounded by construction. The
gap is narrower than "nothing asserts it" and is still real: **nothing in Rust
measures retained bytes**, so any accumulation that preserves consumption
semantics — a side list, a debug buffer, a second index written but never read —
passes the whole Rust suite. TypeScript's row-9 test catches that class directly,
dying at a one-in-ten retention leak.

**Note 10 — Rust's chunked replay is proved behaviourally, not by memory.** Rust
streams chunks into the poll loop and drains matched events, and the
`*_cold_multi_chunk` family proves a chunked replay commits what an unchunked one
does. There is no Rust assertion that retained or peak memory tracks
`history_chunk_events`. `tests/replay_clone_budget.rs::replaying_a_command_event_does_not_copy_its_payload`
is adjacent but different: it budgets *allocations per replayed command event*,
which would not catch a change that accumulated the whole history in one buffer.

**Note 12 — Rust gets disposal from ownership, and ownership is untested by
construction.** The worker removes a cache entry at claim time and reinserts it
only after a committed task, so an abandoned `Pin<Box<dyn Future>>` is dropped
when its owner goes out of scope; there is no waiter to settle because a Rust
durable call that never completes is simply a future nobody polls again.
`tests/replay_core.rs::cache_bound_of_one_forces_cold_replays_for_interleaved_runs`
proves the *replacement* half — an evicted run cold-replays and completes — but
nothing asserts that the abandoned execution was released. This row is a gap in
the ledger's sense (no test in one column) even though the language makes the
leak unrepresentable, and it is recorded that way rather than excused, because
"the type system covers it" is exactly the claim that stops being true when
someone stores the future somewhere else.

**Note 16 — TypeScript has no failed-workflow-task metric at all.**
`WorkerMetricsSnapshot` (`typescript/packages/core/src/worker.ts:200`) carries
claims, commits, conflicts, cache counters, stream counters, `timersFired`,
`loopErrors`, `idleSleeps`, and `eventSinkErrors` — and no counter for a workflow
task that failed. Rust's `WorkerMetrics` separates `workflow_tasks_panicked`,
`workflow_tasks_nondeterministic`, and `workflow_tasks_unsupported_version`.
TypeScript classifies a released task by string-matching
the `nondeterminism:` message prefix (`isNondeterminismError`,
`typescript/packages/core/src/worker.ts:1993`) and counts nothing, so a
permanently poisoned TypeScript run is invisible in metrics. The convergence
direction is TypeScript gaining a discriminator, not Rust dropping to
prefix-sniffing.

**Note 17 — both reject, at different times, with the same blind spot.** Rust's
`#[durust::workflow]` macro scans the handler body and refuses to compile
`Instant::now()`, `SystemTime::now()`, `rand::random()`, `tokio::spawn`,
`tokio::time::sleep`, and `tokio::select!` (`tests/ui/*.rs`). TypeScript rejects
at runtime, by patching the guarded globals, and additionally ships a static
ESLint rule. Both mechanisms see only the code they are pointed at: a
nondeterministic call in a helper function in another module is caught by
neither. Rust has no runtime backstop, which is a recorded open decision, and
TypeScript's runtime guard is off by default in production.

**Note 22 — two rows, because each detector survives the other's revert.** Row
22's cleanup makes a stray wait impossible; row 23's guard makes one harmless if
it happens anyway. They are separated because each was shown to stay green while
the other's fix was reverted — removing the cleanup left `a stray timer wait
never fires against a closed run` passing, and removing the guard left `terminal
cleanup deletes a closed run's waits` passing. One row with one detector would
have claimed coverage that neither test actually provides. Postgres passes row
22's shared conformance case with or without the cleanup, because its terminal
guard is a predicate *inside* the limited due-timer query and a leftover row is
therefore never selected; the Postgres-specific row assertion covers it, and
covers both of that provider's commit paths, which delete the rows in different
places — the SQL-native path with its own `delete`, the loaded-state path
through `#abandonWorkForClosedRun`.

The paragraph above is about **TypeScript's** Postgres provider, and the
coordinator read it as a fact about Postgres in general when briefing the Rust
work — asserting that Rust's Postgres would likewise pass row 22 with the
cleanup removed, and that its arm therefore needed a TypeScript-style special
case. Measured, that is false. `fire_due_timers_tx` (`src/postgres.rs:8611`)
selects on `namespace / kind / ready_at_ms` only and re-reads each row's run for
`terminal` afterwards — structurally SQLite's shape, not TypeScript Postgres's.
The leftover row *is* selected and *does* spend a `limit` slot, so the shared
budget detector reaches Rust's Postgres unaided, failing `fired 0 / expected 1`
with the cleanup removed. What it genuinely cannot reach is the set-based batch
commit path, which is why the separate row assertion exists and why it covers
both paths. The two runtimes' Postgres providers differ structurally here, and
the assumption that they did not was the coordinator's, corrected by
measurement.

**Note 23 — the construction is the point, and the previous one had rotted.**
Rust's guard is defence in depth on both sides now: every terminal transition
cleans the waits up first, so the state it refuses is reachable only by forging.
The TypeScript case forges it by committing a wait record naming an
already-closed run from a second, live run — the same technique as Rust's
`force_terminal` helper. This matters because the case's *previous*
construction, committing the wait in the same task that closes the run, became
vacuous the moment terminal cleanup landed: measured at `fired: 0` with the
guard removed, it would have passed for the wrong reason. The rebuild was not
tidying; it was the difference between a detector and a decoration.

Rust's cases now forge the state the same way, and the cross-revert holds on
both sides: under each of the three row-22 cleanup reverts all three row-23
tests passed, and under each of the three row-23 guard reverts all four row-22
tests passed. Six mutations, and no detector wearing two hats.

**Note 24 — found by building the detector for row 23, not by reading the
SPEC.** §14 is explicit that a wait the terminal guard refuses is "skipped, not
deleted: deleting it would hide the defect the cleanup is supposed to have
prevented." Rust's memory provider obeys it. `src/sqlite.rs:1146` and
`src/postgres.rs:8675` each run `delete from active_waits` and then `continue`,
so the evidence the SPEC wants preserved is destroyed by the very guard that
detected it — and destroyed silently, since firing zero timers is also what
correct behaviour looks like. The measurement that separates them is a second
scan after the guard has run: memory fires `0` (the wait is still there and
still refused), SQLite and Postgres fire `0` for the different reason that
there is nothing left to refuse.

Row 23's Rust cases deliberately assert only the guard's contract — no
`TimerFired`, history unchanged — and say nothing about the wait's fate, so they
do not freeze one provider's answer into the ledger while the question is open.
Whether §14 or the two providers should move is unresolved and deliberately
left so; what is not in doubt is that they disagree today, and that three
TypeScript providers were written to a description of Rust that only Rust's
memory provider satisfies.

---

## 2. Forced divergences

These cannot be closed. Neither runtime can adopt the other's answer.

**Execution mechanism.** Rust polls: each workflow task builds a fresh
`RuntimeContext` — once on the cached path and once on the cold path, both in
`Worker::prepare_claimed_workflow_task_inner` (`src/worker.rs`) — and polls a
`Pin<Box<dyn Future>>` until it blocks. TypeScript keeps a hot promise chain:
`HotWorkflowExecution` builds one `WorkflowRuntimeContext`
(`typescript/packages/core/src/runtime.ts:456`), runs the handler once inside an
`AsyncLocalStorage` scope (`:55`, `:463`), and mutates that context in place on
every later task (`advanceHotClaim`, `:1613`). Rust cannot resume a future
without polling it; JavaScript cannot poll a promise. Both produce the same
committed history. `SPEC.md` §1.2 and §4.2 state this.

**Async side-effect callbacks.** TypeScript must detect and reject a `sideEffect`
callback that returns a thenable (`runtime.test.ts` — `rejects a sideEffect
callback that returns a promise`, `does not emit unhandled rejections when a
sideEffect callback returns a rejecting promise`). Rust's
`side_effect(FnOnce() -> T)` cannot be async, so the failure mode does not exist
and there is nothing to test.

## 3. Open divergences

These are defects or undecided questions, not forced facts. Each should converge.

- Runtime determinism enforcement in Rust (ledger row 17, note 17).
- A failed-workflow-task metric in TypeScript (ledger row 16, note 16).

---

## 4. Adopt-the-better-answer list

Each runtime takes the other's better answer rather than inventing a third
design. This section tracks that list to completion. Every status below is from
reading the code and running the named test, not from the plan's prose: an item
whose implementation is present but whose behaviour nothing asserts is recorded
as open, because an unprotected adoption reverts silently.

### Rust adopts TypeScript's answer

| Item | Status | Evidence |
| --- | --- | --- |
| Worker metrics and event sink | **Landed** | `WorkerMetrics`, `Worker::metrics()`, `WorkerBuilder::on_event`, `WorkerEvent` (all `src/worker.rs`). Rust overshot its source: it separates `workflow_tasks_panicked`, `workflow_tasks_nondeterministic`, and `workflow_tasks_unsupported_version`, which TypeScript has no equivalent of (ledger row 16). Pinned by `tests/worker_run.rs::workflow_panics_and_re_entrancy_are_counted_apart_from_divergence`. |
| Exponential idle and error backoff | **Landed** | `WorkerBuilder::idle_wait`/`max_idle_wait` doubling through `next_backoff`, with a separate error budget per loop. The old flat `idle_wait` plus 16-consecutive-failure counter is gone: `MAX_CONSECUTIVE_RUN_PASS_FAILURES` no longer appears anywhere in `src/`, so a run of failing passes no longer exits `Worker::run`. Unit test `src/worker.rs::worker::tests::next_backoff_doubles_to_the_ceiling_and_leaves_zero_alone`. |
| Maintenance opt-out | **Landed, deliberately narrower than the item as written** | See "The maintenance opt-out correction" below. |
| O(1) LRU eviction | **Not landed at the baseline** | At `240d603`, `Worker::insert_cached_workflow` still selects its victim with `self.state.cache.iter().min_by_key(...)`, there is no order index, and `git log -S "cache_order" -- src/worker.rs` returns nothing. At the bound — the steady state for a busy worker — every committed task therefore scans up to `max_cached_workflows` (default 10,000) entries. An implementation exists in an uncommitted working tree, adding a `cache_order` index evicted with `pop_first()` and a matching `remove_cached_workflow`, which is TypeScript's insertion-ordered idiom in Rust shape; this row must not be marked landed on the strength of it. **Two things close the row, not one:** the index committed, *and* evidence that would fail on the scan. A correctness test cannot supply the second — both policies evict the same victim, so the suite is green either way — so it has to be a cost measurement at two cache bounds, 1,000 and 100,000, showing per-task eviction cost independent of `max_cached_workflows`. |
| Ready-event filtering at ingest | **Not landed, and not yet evaluated** | Rust still filters consumed ready events at every peek rather than removing them from the replay list at ingest. That arrangement is what `src/runtime.rs::runtime::tests::indexed_ready_events_are_skipped_when_consumed_before_the_replay_cursor` and `peek_replay_command_event_skips_unconsumed_ready_events_without_consuming_them` assert, so adopting the TypeScript shape is a behaviour-preserving change those two tests would have to be rewritten against. No decision is recorded either way. |

### TypeScript adopts Rust's answer

| Item | Status | Evidence |
| --- | --- | --- |
| Chunked replay streaming | **Landed** | `ReplayHistoryLoader` / `loadReplayHistory` (`typescript/packages/core/src/runtime.ts:141`) with a park-and-retry `replayHistoryGate(required)` (`:1350`) at every thenable durable API, and a `nextCommit()` pump that pulls one chunk per quiescence. Pinned by `worker.test.ts` — `holds replay memory proportional to historyFetchMaxEvents, not to history length` and `commits the same events whether history arrives in one chunk or many` (ledger rows 10, 11). |
| Exactly-once ready-event consumption | **Landed** | `WorkflowRuntimeContext.#takeReadyEvent` (`typescript/packages/core/src/runtime.ts:1461`) is the single removal path for all eleven ingest indexes and deletes on read, mirroring Rust's `take_indexed`, with `consume: false` for probing callers (`spawn()` allocates a command and discards the resolution) and handle-owned resolutions for legitimate re-reads. Pinned by ledger rows 8 and 9. |
| Deterministic execution disposal | **Landed, at more sites than the item named** | `HotWorkflowExecution.dispose(reason)` is called from six sites in `typescript/packages/core/src/worker.ts` — `:761` (task failed before commit), `:1015` (superseded by cold replay), `:1054` (replay window reserve exhausted), `:1361` (facts landed under the claim), `:1379` (execution cache disabled), `:1528` (cache eviction). The item named three; the supersession sites and the cache-disabled site were found while implementing it, and the cache-disabled one is the highest-volume instance. Pinned by ledger row 12. |

| Provider retirement | **Landed** | The TypeScript providers, payload package, and map engine are deleted; every provider is the Rust core behind `@durust/native` (`typescript/packages/native/src/index.ts`), and the shared cases run over memory, SQLite, and Postgres in `native-conformance.test.ts`. The three answers the two sides used to give differently were settled by the Rust rule and are pinned on both sides: an unknown activity id is `NotFound` (`tests/provider_conformance.rs::unknown_activity_id_answers_run_not_found`; shared case `batch activity completion reports ordered duplicate stale success and missing results`); a lapsed activity lease is reclaimed by the timeout scan, never by a claim (shared case `an expired activity lease is reclaimed by the timeout scan and fences the old holder`); a fail-fast child map ends at its first collision inside the scheduling commit (`tests/provider_conformance.rs::child_map_id_collision_fails_the_map_in_its_scheduling_commit`; shared case `a fail-fast child map with a colliding item id fails before its batch-mate starts`). A signal or cancellation naming an unknown workflow id is `WorkflowNotFound` on both sides (`tests/provider_conformance.rs::signal_and_cancel_of_an_unknown_workflow_are_not_found`; shared case `a signal to an unknown workflow id is rejected as not found`). Both Rust scenarios for the settled answers are revert-verified: the memory `missing_activity_outcome` short-circuit and the Postgres tail publish and fail-fast dispatch stop each fail their scenario when removed. |

Both open items are on the Rust side, and neither is closed at the baseline:
ready-event ingest filtering is unadopted and unevaluated, and O(1) eviction is
uncommitted and would still need a cost measurement to protect it. TypeScript's
list is closed: all three items landed, and disposal landed at twice the sites it
named.

### The maintenance opt-out correction

The item was written as a general maintenance opt-out, mirroring TypeScript's
`runTimerMaintenance`. It shipped as **`WorkerBuilder::run_timer_maintenance(bool)`**
— narrower on purpose, and the narrowing is the point.

A Rust worker's maintenance loop does two unrelated jobs: it scans for due timers
and expired activity deadlines, and it drains the child-workflow start outbox
(`Worker::run_maintenance_scan_once`). Only the first is optional.
`SPEC.md` §11 makes due-timer delivery a timer service's obligation and a
worker's scan a convenience; it says nothing of the kind about the child-start
outbox, and **nothing else in a deployment drains that outbox**. A general
`run_maintenance(bool)` that suppressed both would silently stop every child
workflow in the fleet, forever, in exchange for a knob whose stated purpose was
reducing provider load.

So the loop always runs; only the timer-and-deadline half is skippable.

**Pinning it takes two tests, because there are two dispatch sites.**
`run_pass_once` drains the outbox for the deterministic driver `run_until_idle`,
and `run_maintenance_scan_once` drains it for the interval loop, which is the
only site `Worker::run` reaches. A test that drives `run_until_idle` says nothing
about production. Both are covered:

- `tests/worker_run.rs::disabled_timer_maintenance_still_dispatches_child_workflow_starts`
  — the pass driver.
- `tests/worker_run.rs::disabled_timer_maintenance_dispatches_child_starts_from_the_interval_loop`
  — the interval loop. It drains `run_until_idle` against an empty queue
  *before* starting the parent it measures, so the pass driver cannot have
  dispatched the outbox row, and then completes the parent under `Worker::run`
  alone.

Verified by reverting the incident rather than by inspection: gating the child
drain in `run_maintenance_scan_once` on `run_timer_maintenance` leaves the
pass-driver test **green** and fails only the interval-loop one, with `the parent
never completed under 'run' alone`. Before the second test existed, that revert
passed the entire suite.

This has no TypeScript counterpart and needs none:
`typescript/packages/core/src/worker.ts` never calls
`dispatchChildWorkflowStarts` at all — TypeScript providers start child
workflows inline during commit — so `runTimerMaintenance: false` there suppresses
timers and activity-timeout scans and nothing else. The two options have the same
name and the same effect; they are not the same knob, and a future change that
gives TypeScript an outbox must revisit this.

The general lesson, recorded because it generalizes past this option: a worker
knob may change *when* durable work happens. It may not change *whether* durable
work that nothing else performs happens at all. `SPEC.md` §1.2 states this as a
rule.
