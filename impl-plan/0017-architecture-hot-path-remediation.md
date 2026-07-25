# 0017: Architecture and Hot-Path Remediation (Rust and TypeScript)

## Overarching Goal

Close the gaps found by a full-codebase review of both implementations across
architecture, simplicity, performance, and correctness.

Shared defects: safe workflow code can call a durable API from inside a
`side_effect`/`sideEffect` callback and permanently poison its own history in
both runtimes; both worker loops serialize workflow processing, activity
execution, and cluster maintenance into one sequential pass, so a slow activity
stalls workflow progress and maintenance load scales with work rate and worker
count; and the activity-map/child-workflow-map fanout state machine is
implemented six times (three Rust providers, three TypeScript providers).

Rust-specific defects: the `side_effect` re-entrancy is additionally aliasing UB;
one panicking workflow or activity kills the whole worker; and the replay/commit
hot path carries avoidable deep clones plus an O(n)-per-task cache eviction.

TypeScript-specific defects: cold replay bulk-loads the entire history into one
array and copies it twice per commit into a second full-history cache, which is
O(n²) over a run's lifetime and contradicts both `SPEC.md` §4.3 and the README's
"No Event History Limit" claim; the eleven ready-event ingest maps are never
consumed, so a hot workflow's memory grows without bound; abandoned hot
executions are dropped with no settlement; and running a single workflow
permanently monkey-patches ~27 process-global built-ins, including a `Proxy` on
`process.env`, for every consumer in the host process.

Outcome: workflow code cannot silently poison its own history in either runtime;
a bad workflow fails its task instead of its worker; workflow and activity
progress are independent; replay memory is bounded by chunk size in both
runtimes; map fanout semantics have one specification and one engine per
language; and a behavioral golden corpus — not just a vocabulary corpus — proves
the two runtimes agree.

Non-goals: new workflow features, the Postgres shard-native roadmap (item 0013),
any change to the durable history format or to map semantics, and unifying the
two execution models. Rust must poll futures and TypeScript cannot poll
promises, so the poll-versus-hot divergence is forced by the host languages;
this plan converges the observable contract, not the mechanism.

## Implementation Principles

- Correctness gates before performance work, per `AGENTS.md`.
- Extract the shared state machine before optimizing the same path six times.
- Every bug fix lands with its regression test in the same change.
- Deterministic drivers stay deterministic: `run_until_idle` (Rust) and the
  one-shot TS drivers keep their sequential single-pass shape; only the
  production loops gain concurrency.
- Converge on the better answer regardless of which language has it today.
  TypeScript already has worker metrics, exponential backoff, a maintenance
  opt-out, and O(1) LRU eviction; Rust already has chunked replay streaming,
  exactly-once ready-event consumption, and deterministic execution disposal.
  Each side adopts the other's, rather than inventing a third design.
- Behavioral parity is proved by a shared corpus both runtimes execute, not by
  reading both implementations.
- Provider contract changes stay generic; the shared map engine is pure decision
  logic returning effects, so sync and async providers apply it without an
  async-trait reshape in Rust and without a transaction-shape reshape in TS.
- Pre-1.0: breaking history-format changes are acceptable when recorded as a
  Decision row. This plan intends none.
- Public API discipline per `AGENTS.md`: every new knob records why composing
  existing primitives is insufficient and what scaling invariant it protects.

## Testing Strategy

- Deterministic replay tests for every runtime behavior change in both
  languages, covering cached/hot, cold, multi-chunk, and unfavorable orderings.
- Provider conformance for every backend behavior change, against memory,
  SQLite (close/reopen), and Postgres, in both languages.
- Seeded simulation driving real workers for every concurrency, recovery, or
  loop-shape change (`tests/sim_worker.rs`,
  `typescript/packages/core/test/simulation.test.ts`).
- Criterion (Rust) and `packages/benchmark` (TS) with checked-in baselines gate
  every performance phase; regressions need an explanation or explicit
  acceptance.
- Memory assertions, not just throughput: peak RSS or heap-sample bounds for
  replay of a large history, asserted in both runtimes.
- Mutation checks: every new regression test must be shown to fail when the fix
  it guards is reverted.
- The Phase 7 behavioral corpus runs in both CI jobs and must produce identical
  commits.

> Review note: the Node toolchain was not available in the review environment,
> so every TypeScript finding below is from source reading with pinned line
> references and none is backed by a measured run. Phase gates that assert TS
> performance or memory require an actual `npm run test` / benchmark run.

---

## Phase 1: Durable-API re-entrancy and workflow fault isolation (both runtimes)

Goal:
A durable API called from inside a side-effect callback fails loudly instead of
recording an unreplayable command order, and a panic or throw inside a workflow
or activity fails that task rather than the worker.

Scope:
- Rust re-entrancy. `with_context` (`src/runtime.rs:42`) does
  `unsafe { f(&mut *ptr) }` on a thread-local raw pointer; `side_effect` invokes
  the user closure inside that borrow (`src/runtime.rs:1276`), and
  `get_version`/`patched`/`deprecate_patch`/`publish`/`set_default_activity_options`/
  `continue_as_new`/`activity_map_manifest`/`child_workflow_map_manifest` are all
  synchronous and re-enter `with_context`. Two live `&mut RuntimeContext` is
  aliasing UB. Fix: take the pointer out of the cell for the duration of `f` and
  restore it after, so nesting hits the existing null check.
- Same guard for `with_activity_context` (`src/runtime.rs:88`).
- TypeScript re-entrancy — the same latent bug without the UB.
  `resolveSideEffect` (`typescript/packages/core/src/runtime.ts:1730`) allocates
  the command id at `:1756`, then runs `effect()` at `:1760`, then pushes the
  `SideEffectMarker`. A durable API called inside `effect()` allocates the next
  seq and pushes its own marker first, so history records
  `[VersionMarker(seq N+1), SideEffectMarker(seq N)]` and replay of the side
  effect peeks a `VersionMarker` and fails nondeterminism forever. Identical
  outcome to Rust. Fix: a dedicated `#activeSideEffectKey` flag that makes
  durable APIs throw, set and restored around `effect()`. Reusing the existing
  `#allowNondeterministicGlobalsDepth` counter (`:1758`/`:1762`) was considered
  and rejected: that counter *relaxes* the nondeterministic-globals guard while
  this one *tightens* the durable-API guard, so a future site wanting one would
  silently get the other, and `allowsNondeterministicGlobals()` is already read
  by `installNondeterminismGuards`, which Phase 5 reworks.
- TypeScript `sideEffect` callbacks must be synchronous. `sideEffect<T>(key,
  effect: () => T)` structurally accepts an `async` callback, and on unmodified
  `HEAD` that silently records the serialized `Promise` (`{}`) as the marker
  value while the workflow returns the resolved value — guaranteed replay
  divergence on a public API, with no warning. It is also what makes the
  re-entrancy guard inexact: `resolveSideEffect` is synchronous, so the guard is
  released at the callback's first `await` and the detached continuation runs
  with the guard clear. Reject a thenable return, and adopt-and-discard the
  promise before throwing so a rejecting callback cannot reach Node's default
  `unhandled-rejections=throw` and kill the worker.
- Rust workflow and activity fault isolation. There is no `catch_unwind`
  anywhere in `src/`: a `panic!`/`unwrap()` in a workflow unwinds through
  `poll_cached` (`src/worker.rs:1646`) and terminates `Worker::run` for the
  process, taking every cached run with it. TypeScript already survives this via
  the per-iteration `try`/`catch` (`typescript/packages/core/src/worker.ts:330`).
  Wrap the workflow poll and the activity poll in `AssertUnwindSafe` +
  `catch_unwind`, convert a caught panic into `WorkflowFailed` /
  `FailActivityRequest`, and drop the cached future.
- TypeScript hot-execution disposal. `HotWorkflowExecution`
  (`typescript/packages/core/src/runtime.ts:231`) has no dispose path; on commit
  conflict (`worker.ts:481`), on prepare error (`worker.ts:460`), and on LRU
  eviction (`worker.ts:877`) the entry is removed with a bare `Map.delete` while
  the workflow's promise chain stays pending forever with its `#hotWaiters`
  unsettled. Rust drops a boxed future deterministically and cannot leak. Add
  `dispose()` that rejects outstanding waiters and settles the handler chain, and
  call it from all three paths.
- Document the rule in `SPEC.md` §16 and §17.2: durable APIs are not re-entrant
  and must not be called from a side-effect callback; a panicking or throwing
  task fails the task, not the worker.

Out of scope:
- Making the durable APIs re-entrant. Rejecting re-entrancy is the correct
  contract in both runtimes.
- Replacing Rust's thread-local raw pointer with a safe abstraction.

Completion gate:
In both runtimes, a workflow calling a durable API from a side-effect callback
fails its task with a clear error and commits no out-of-order marker pair; a
panicking Rust workflow and activity fail their own task while `Worker::run`
keeps serving other runs — meaning healthy neighbours' **commits still land**,
including when they share a batch with the panicking task, not that the pass
itself completes (the pass tail is short-circuited today; that is row 2H); a
conflicted or evicted TS hot execution settles deterministically; Miri reports no
aliasing violation for the Rust nested-call test.

Testing plan:
- Rust unit test: nested `with_context` fails instead of aliasing.
- Rust and TS replay tests: side-effect callback calling `patched`/`getVersion`
  fails the task with no marker pair appended and history unchanged.
- Rust replay/worker tests: panicking workflow and activity; a second run
  completes afterwards on the same worker.
- TS test: conflicted and evicted hot executions settle, asserted by a
  `FinalizationRegistry`-free deterministic signal (waiters rejected).
- Miri run over `src/runtime.rs` unit tests, wired into CI if runtime allows.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Rust `with_context` re-entrancy guard | Reviewed and approved. A non-dereferenceable sentinel address parked in the slot for the duration of `f`, rather than the null the plan sketched — null cannot distinguish re-entrancy from "no context installed", and those need different messages. Two `const` assertions (`align_of >= 2`, both context types) make collision with a live pointer impossible; verified load-bearing by forcing the constant and observing `E0080` at compile time. Hot check is one `ptr.addr() < LIVE_CONTEXT_ADDR` compare, with discrimination in a `#[cold] #[inline(never)] -> !` reporter. `durable_api_inside_a_real_side_effect_closure_records_no_marker_pair` drives the genuine `SideEffectFuture` and asserts `append_events` empty, `change_markers` empty, `next_command_seq == 1`. Mutation-proven against the drift a synthetic test cannot see: hoisting `effect()` out of the borrow leaves the synthetic nested-call test green and fails only this one. |
| Complete | Work | 1B: Rust `with_activity_context` guard | Reviewed and approved. Same sentinel and `ContextRestore` `Drop` guard applied to `with_activity_context` and both `poll_with_*_context` installs, so the slot is restored on the unwinding path. `P: Copy` on the guard is load-bearing: `Copy` and `Drop` are mutually exclusive, so no instantiation can run a user destructor from `drop`, which would abort the process mid-unwind. |
| Accepted | Gate | Criterion cost of the 1A/1B guard | Measured by review after the implementer skipped it. Byte-isolated trees differing only in `src/runtime.rs`, `taskset`-pinned, interleaved A/B/A/B, two replications on different core pairs in opposite orders. `workflow_cached_wake_poll_memory` **+1.54%** (2652.5 → 2693.3 ns) and **+1.38%** (3631.3 → 3681.3 ns) on min; medians +1.70% / +1.10%. `workflow_task_append_commit_memory` +0.09%, `workflow_replay_small_history_memory` −0.27%, `workflow_replay_large_history_memory` −0.34% — all noise. **Accepted**: ~1.5% on one bench buys removal of reachable aliasing UB on the crate's most safety-critical path. Mechanism is *not* the two extra stores (~0.1% at 5–20 `with_context` calls per wake) but `ContextRestore`'s `Drop` forcing the slot live across `f`, adding unwind landing pads and changing `with_context` inlining — which is why the measurement was required rather than estimated. |
| Complete | Work | 1C: TS `resolveSideEffect` re-entrancy guard | Reviewed and approved. `#activeSideEffectKey` flag, 13 named entry-point asserts plus a backstop in `#nextCommandId()` — total, since `#nextCommandSeq` is incremented in exactly one place. `resolveSelectWinner` (`runtime.ts:1840`) allocates a seq with no named assert and surfaces via the backstop only. A rejecting thenable is adopted-and-discarded before the throw, so it cannot reach Node's default `unhandled-rejections=throw`. All 14 table cases assert their exact commit, so "no half-appended command" is proved for every guarded API. Message formatting moved to the throwing path only (~1.6–4 ns/call saved, five interleaved trials). |
| Complete | Work | 1D: Rust workflow poll panic isolation | Reviewed and approved. `catch_unwind` in `poll_cached` mapping to `Error::Nondeterminism("workflow task panicked: {msg}")`, propagated with `?` at the call site *before* `take_signal_requests`/`take_payload_hydration_requests`/`prepare_workflow_poll`, so the poisoned context is dropped unread. Cache drop needs no new code: the entry is removed at claim time (`src/worker.rs:738`) and reinserted only after a committed task — review confirmed that removal is the unconditional first statement, before any `await`. Covered on the production batch path as well as the single-task path. |
| Complete | Work | 1E: Rust activity poll panic isolation | Reviewed and approved. `catch_unwind` in `start_claimed_activity` mapping to `Error::Application(DurableFailure::new("durust.activity_panic", ...))`, which flows through the existing `ActivityFinish::Fail` arm. Deliberately *not* `Nondeterminism`: review confirmed `src/error.rs:56` maps it through `.marked_non_retryable()` and `src/provider_util.rs:199` gates on `!non_retryable`, so that choice would have silently voided the activity's retry policy. |
| Incomplete | Work | 1F: TS `HotWorkflowExecution.dispose()` | Missing: dispose rejecting `#hotWaiters`, called from `worker.ts:460`, `:481`, and `:877`. |
| Complete | Test | Rust nested-durable-API replay regression | `tests/replay_core.rs::durable_api_inside_side_effect_fails_the_task_without_recording_markers`: task fails carrying both `workflow task panicked` and `durable APIs are not re-entrant`, history byte-equal and length 1, neither half of the `[VersionMarker, SideEffectMarker]` pair present, no `workflow_change_versions` record, empty cache. |
| Complete | Test | TS nested-durable-API replay regression | 14-case table in `packages/core/test/runtime.test.ts`, each asserting its exact commit. Mutation check reproduced independently against a pristine `HEAD` tree: all cases fail exhibiting `["VersionMarker#2", "SideEffectMarker#1"]`. |
| Complete | Test | Rust panicking workflow and activity regressions | `tests/worker_run.rs`: across-passes survival, batched survival (`max_concurrent_workflow_tasks(4)` + prefetch 4, healthy neighbour commits `Some(8)` in the *same* `run_workflow_batch_once` call), activity retry-policy exhaustion. The batched case uses a separate counter-free workflow: review proved reusing the shared-counter one fails 13 runs in 15 under the default parallel runner. |
| Incomplete | Test | TS abandoned-execution settlement | Missing: conflict and eviction tests asserting waiters reject. |
| Complete | Test | Miri aliasing check | `MIRIFLAGS="-Zmiri-strict-provenance" cargo +nightly miri test --locked --lib --no-default-features -- runtime::tests` — 10 passed, wired as a CI job. Proves the UB was real and reachable: with the guard removed Miri reports `not granting access to tag ... which is strongly protected` at the `unsafe { f(&mut *ptr) }`. Scope is deliberately narrow (8 of 38 lib tests) and the CI comment says so; `--no-default-features` is forced because rusqlite's C FFI is unexecutable under Miri, and a whole-lib run under `-Zmiri-disable-isolation` was killed at 25 minutes. |
| Complete | Test | TS rejecting async side-effect callback does not kill the worker | Table-driven over all three shapes (throw-before-await, throw-after-await, plain `Promise.reject`) using the `flushUnhandledRejectionTurn()` pattern. All three exited 1 before the fix and exit 0 with zero unhandled rejections after; re-verified by review with its own repro. |
| Incomplete | Doc | SPEC re-entrancy and fault contract | Missing: `SPEC.md` §16/§17.2 text covering both runtimes, including that a `sideEffect` closure is synchronous and a thenable return is rejected; that a caught panic fails the task and is retried, not the workflow; and that a downstream crate building with `panic = "abort"` keeps the old worker-killing behaviour, because `catch_unwind` never returns `Err` there. This repo sets no `panic` profile key, so every profile here is `unwind`. |
| Incomplete | Decision | `sideEffect` callbacks are synchronous; a thenable return is rejected | Pre-1.0 public behaviour change, recorded per the Implementation Principles. Rationale: the marker is recorded synchronously, so awaiting the promise is impossible without changing the history contract; converges TypeScript onto Rust's `side_effect(FnOnce() -> T)`, which cannot be async. Two effects versus `HEAD`, both improvements: the task now fails loudly instead of recording a serialized `Promise` as the marker value, and a rejecting async callback no longer reaches Node's default `unhandled-rejections=throw` — `HEAD` reached that only *after* already recording the bad marker. Audit found no async usage in `packages/examples/src/control-flow.ts:67`, `test-d/determinism/valid/workflows.ts:37`, `typescript/README.md:279`, `README.md:729`, or `SPEC.md:2368`. Missing: the `SPEC.md` §17.2 sentence. |
| Incomplete | Decision | A caught workflow panic fails the *task*, not the *workflow* | The plan originally said "convert a caught panic into `WorkflowFailed`". Rejected after research. `Error::Nondeterminism` is not "history diverged" in this codebase — it is the fatal workflow-task error channel: `prepare_workflow_poll` (`src/worker.rs:1490`) commits `WorkflowFailed` for every workflow `Err` *except* `Nondeterminism`/`UnsupportedWorkflowVersion`, which `release_failed_workflow_task` (`:699`) re-releases with the 60 s backoff. Decisive argument: a panic can occur while replaying an already-progressed run, so committing `WorkflowFailed` would destroy committed progress (in-flight activities, timers) for a bug a redeploy fixes, with no evidence the recorded progress was wrong. Rust also already has a terminal channel for *intentional* failure — a workflow returning `Err(...)` — and making both terminal discards that distinction. Matches Temporal's `BlockWorkflow` default. Reviewer traced all three arguments to the code and could not break the decision; its mutation routing panics to `WorkflowFailed` fails both regressions at their `unwrap_err()`, with the activity test correctly unaffected. |
| Incomplete | Decision | Rust and TypeScript diverge on generic workflow-code bugs | After 1D, the Phase 1 re-entrancy case is **converged**: both runtimes fail the task, back off 60 s, and commit nothing. A *generic* bug (JS `TypeError` vs Rust `unwrap()` panic) still diverges: TS calls `failWorkflow(...)` for a terminal `WorkflowFailed`; Rust now retries the task. The divergence is real, not incidental — JS cannot distinguish a bug-throw from a business-throw and Rust can. If it is closed, the correct direction is TypeScript adopting Rust's behaviour, since terminating a run on a replay-time bug is unrecoverable. Missing: a Phase 7 corpus case pinning whichever answer is chosen. |
| Complete | Decision | Type-level rejection of async `sideEffect` callbacks: feasible, deliberately declined | Four variants tested under `tsc --strict`. An **overload pair** produces no error at all — resolution picks the first matching signature and `never` is assignable everywhere (`TS2578`). A conditional **return** type catches the assigned form but misses the statement form `await sideEffect(k, async () => v)`. A conditional **parameter** type (`() => T extends PromiseLike<unknown> ? never : T`) *does* catch every real async shape including the statement form and an explicit `Promise.resolve` return, and leaves every synchronous shape clean — so this is feasible, contrary to the first evaluation. Declined anyway on cost: it breaks naive generic forwarding (`function wrap<T>(k, f: () => T) { return sideEffect(k, f) }` fails with `TS2345`, a worse diagnostic than the runtime message), and it still misses a hand-rolled non-`PromiseLike` thenable that the runtime guard catches — so it could only supplement the runtime guard, never replace it. |

---

## Phase 2: Production worker loop shape (both runtimes)

Goal:
Both workers make workflow progress, activity progress, and maintenance
independent, and maintenance load stops scaling with work rate and worker count.

Scope:
- Rust: decouple activity execution from workflow processing. `run_pass_once`
  (`src/worker.rs:1117`) runs workflow tasks, maintenance, child dispatch, and
  activities strictly in sequence, and `execute_claimed_activities`
  (`src/worker.rs:941`) drains every claimed execution before returning — its own
  comment records that one long-running activity holds the whole pass. Split the
  worker's fields into disjoint workflow/activity/maintenance state and drive
  three loops with `futures::future::try_join3`. No `tokio::spawn`, preserving
  the 0015 Phase 7B decision to keep the trait free of `Send`-bound and
  runtime-flavor coupling. Closes the 0015 Phase 7 `Incomplete` row "Decouple
  activity execution from the run-loop pass".
- TypeScript: the same coupling exists. `#runLoopIteration`
  (`typescript/packages/core/src/worker.ts:913`) runs exactly one workflow task
  (`:928`), then activities (`:940`), then maintenance (`:951`), all sequentially
  per iteration. Split into independent loops raced under the existing
  `AbortSignal`.
- Maintenance cadence, both runtimes. Rust calls `run_due_maintenance_once`
  (`src/worker.rs:1130`) and `run_child_workflow_starts_once` (`:1139`)
  unconditionally every pass; on Postgres each is its own `retry_transaction` +
  connection acquire + BEGIN/COMMIT (`src/postgres.rs:3166`, `:1032`). TS calls
  `fireDueTimers` (`worker.ts:952`) and activity-timeout maintenance (`:966`)
  every iteration whenever `runTimerMaintenance` is true, which is the default.
  Move maintenance onto its own interval-paced loop in both: immediate re-run
  when the last scan did work, otherwise exponential backoff to a cap, with
  per-worker jitter derived deterministically from the worker id so a fleet does
  not synchronize. `SPEC.md` §11 already describes an independent timer service
  and never makes maintenance every worker's obligation.
- Rust gains the knobs TypeScript already has: a maintenance opt-out
  (`run_maintenance(bool)`, mirroring TS `runTimerMaintenance` at
  `worker.ts:314`), separate exponential idle and error backoff replacing the
  flat `idle_wait` plus 16-consecutive-failure counter (`src/worker.rs:34`,
  `:414`), and a `WorkerMetrics` snapshot plus event sink mirroring TS
  `metrics()`/`onEvent`/`onError`. The metrics surface closes the 0015 Phase 7
  `Incomplete` row "Observability for silently-retried poisoned workflows".
  API budget for the opt-out: no composition suppresses the scan because the
  cadence is hardcoded in the pass; the invariant protected is bounded provider
  load per unit of work rather than per worker pass.
- Per-stage error isolation, both runtimes. Rust's `?` on the workflow stage
  skips maintenance and activities entirely; each loop must own its error
  budget and backoff.

Out of scope:
- Postgres `LISTEN`/`NOTIFY` push wakeups (tracked in 0015 Phase 7); the
  interval-paced maintenance loop bounds idle cost without it.
- Multi-process work distribution or shard assignment (item 0013).

Completion gate:
In both runtimes a workflow task completes while a multi-second activity is in
flight on the same worker; maintenance call count per unit of work is bounded by
the interval rather than the pass rate; a failing workflow stage no longer
suppresses activity completions; `run_until_idle` and the TS one-shot drivers are
unchanged and every existing deterministic test still passes.

Testing plan:
- Both runtimes: slow activity in flight, workflow task for a different run
  commits before the activity finishes (Rust extends
  `parked_activity_does_not_block_fast_activity_completions` across the
  workflow/activity boundary).
- Both runtimes: injected workflow-claim failure still lets an activity complete.
- Both runtimes: counting test over a recording backend showing N workflow tasks
  produce O(elapsed / interval) maintenance calls, not O(N).
- Rust sim scenarios re-run against the new `run` loop, including crash and
  lease-expiry; TS `simulation.test.ts` re-run against the new loop.
- `benchtools` mixed Postgres profile before/after, reporting transaction counts
  alongside throughput, against `benches/baselines/durust-mixed-postgres.json`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Scope | Independent workflow / activity / maintenance loops in both runtimes | Missing: Rust disjoint-state split with `try_join3` and TS loop split; one-shot drivers proven unchanged. |
| Incomplete | Work | 2A: Rust activity loop decoupled from workflow loop | Missing: implementation and the cross-boundary slow-activity test. |
| Incomplete | Work | 2B: TS activity loop decoupled from workflow loop | Missing: `#runLoopIteration` split (`worker.ts:913`) and its test. |
| Incomplete | Work | 2C: interval-paced maintenance loop with deterministic jitter, both runtimes | Missing: interval knobs, worker-id-derived jitter (no global RNG), and the call-count tests. |
| Incomplete | Work | 2D: Rust maintenance opt-out knob | Missing: `run_maintenance(bool)`, recorded API budget, and a test that a disabled worker fires no timers while a peer does. |
| Incomplete | Work | 2E: Rust per-stage error isolation and exponential idle/error backoff | Missing: independent error budgets replacing `src/worker.rs:34`/`:414`. Must also settle `MAX_CONSECUTIVE_RUN_PASS_FAILURES` (`src/worker.rs:430`): a failed task returns `Err` from `run_pass_once`, so 16 consecutive all-panicking passes with no other progress exit `Worker::run` entirely, while the TypeScript loop catches, backs off, and continues forever. Second Phase 1 convergence difference; pick one behaviour for both. |
| Incomplete | Work | 2H: one failed workflow task must not short-circuit its whole pass | Found by Phase 1 review, with a probe. `run_workflow_batch_once` (`src/worker.rs:599`) does `if let Some(err) = first_error { return Err(err); }` *above* both `if committed > 0 { run_local_activities_after_workflow_tasks(committed) }` and `Ok(committed)`. Commits from healthy neighbours in the same batch do land — probe confirms `healthy completed in the SAME pass: true` — but the pass still reports `Err`, so `committed` is discarded, that batch's local activities do not run, `run_pass_once`'s `?` at `:1144` skips maintenance / child dispatch / activity execution for the pass, `stats.workflow_tasks` under-records, and `Worker::run` increments `consecutive_failures` despite real progress. `run_until_idle()` also returns `Err`, aborting any deterministic idle-loop test containing a panicking workflow. Pre-existing plumbing written for prepare/commit errors; Phase 1 newly routes *workflow-code bugs* into it, so one bad workflow gains cross-run collateral it did not have before. Missing: stop discarding `committed`, and keep one task's error from short-circuiting the pass tail. |
| Incomplete | Work | 2G: dedicated `Error::TaskPanic` variant | Phase 1 routes a caught workflow panic through `Error::Nondeterminism`, which is correct for *retry* semantics but makes a panic indistinguishable from genuine history divergence in metrics — the same gap 2F records for TypeScript, so the runtimes stay converged including in the gap. Missing: a variant in `src/error.rs` routed identically (public enum plus the exhaustive `DurableFailure::from_error` match), surfaced separately in the 2F metrics. |
| Incomplete | Work | 2F: Rust worker metrics and event sink | Missing: surface matching TS `metrics()`/`onEvent`/`onError`, plus a test asserting repeated `Nondeterminism` retries are observable. Must cover the Phase 1 re-entrancy error specifically, not only genuine history divergence: it classifies as `nondeterminism:`, so TS `#releaseFailedWorkflowTask` re-releases it with the 60 s backoff, uncapped, and it is currently indistinguishable in metrics from real divergence. |
| Incomplete | Test | Slow activity does not block workflow progress (both) | Missing: `tests/worker_run.rs` and `packages/core/test/worker.test.ts` cases. |
| Incomplete | Test | Failing workflow stage does not suppress activities (both) | Missing: fault-injected cases in both suites. |
| Incomplete | Test | Maintenance call count is time-bounded (both) | Missing: recording-backend counting tests. |
| Incomplete | Test | Sim suites green on the new loops | Missing: `tests/sim_worker.rs` against `run`, and TS `simulation.test.ts`. |
| Incomplete | Gate | Postgres mixed-workload transaction count and throughput | Missing: before/after `benchtools` run versus the checked-in baseline. |
| Incomplete | Decision | Maintenance is worker configuration, not a durability guarantee | Missing: `SPEC.md` §11 note. |
| Incomplete | Risk | Concurrent loops share one backend connection pool | Missing: sizing guidance or a bound so the activity loop cannot starve workflow commits. |

---

## Phase 3: Rust replay and commit hot path

Goal:
Remove the per-task deep clones and the O(n)-per-task cache eviction on paths the
Criterion suite already measures, and collapse the five duplicated
command-scheduling bodies into one matcher so the fix lands once.

Scope:
- Single command matcher. `poll_activity_schedule` (`src/runtime.rs:2062`),
  `ActivityMapSpawnFuture::poll_init` (`:2290`),
  `ChildWorkflowMapSpawnFuture::poll_init` (`:2574`),
  `ChildWorkflowSpawnFuture::poll_init` (`:2845`), and `TimerFuture::poll_init`
  (`:3071`) each re-implement the same protocol: block if history is unloaded,
  allocate the command seq, compute the fingerprint, then match the peeked
  event's variant/seq/fingerprint or append the command plus its side effect.
  0015 Phase 1 consolidated the `take_*` and `collect_*` sides; the schedule side
  was never consolidated and is where the remaining drift risk lives.
- Match without cloning. All nine `peek_replay_command_event().cloned()` sites
  (`src/runtime.rs:944`, `:1012`, `:1243`, `:1753`, `:2098`, `:2326`, `:2607`,
  `:2874`, `:3080`) deep-clone the whole `HistoryEvent` — including
  `ActivityScheduled.input`, `SideEffectMarker.value`, and
  `ChildWorkflowStartRequested.input` — to read a seq and a fingerprint. 0015
  Phase 6F explicitly limited its clone-laziness work to `take_indexed`; this is
  the untouched path. Consider adopting TypeScript's cleaner arrangement of
  filtering ready events out of the replay list at ingest
  (`typescript/packages/core/src/runtime.ts:921`) rather than skipping them at
  every peek.
- `split_start_event` (`src/worker.rs:1734`) deep-clones the entire first
  recovery chunk via `events.iter().skip(1).cloned()`; take the chunk by value.
- `run_workflow_batch_once` (`src/worker.rs:526`) clones every
  `WorkflowTaskCommit` — all append events, activity inputs, child start payloads
  — to build the batch, then drops the original. Nothing after the commit reads
  `task.commit`; move it out with `std::mem::take`.
- `prefetched_claim_history_chunk` (`src/worker.rs:1655`) clones every candidate
  event before validating contiguity and may discard all of them.
- Cache eviction. `insert_cached_workflow` (`src/worker.rs:1621`) picks the
  victim with an O(n) `min_by_key`. 0015 Phase 7C documented this as acceptable
  because "eviction only runs at the bound" — but at the bound is the steady
  state for a busy worker, so every committed task scans up to
  `max_cached_workflows` (default 10,000) entries. TypeScript already uses the
  correct O(1) idiom (`delete` then re-`set`, evict `keys().next()`,
  `typescript/packages/core/src/worker.ts:877`); adopt it.
- `Registry::workflow_types`/`activity_names` (`src/registry.rs:196`) allocate a
  fresh `Vec` of cloned names on every claim RPC (`src/worker.rs:461`, `:493`,
  `:875`, `:910`); the registry is immutable after `build()`, so cache both.
- `change_versions_for_loaded_history` (`src/worker.rs:1422`) clones the cached
  record vector and rebuilds a `BTreeMap` over all records on every cached task
  (`src/worker.rs:745`); keep the deduplicated map on `CachedWorkflow`.
- Consider collapsing `ReadyEventIndexes`' twelve per-kind `BTreeMap`s
  (`src/runtime.rs:143`) into one `BTreeMap<CommandSeq, CommandReadyState>`.
  Benchmark first; record a Decision either way.

Out of scope:
- Any change to the history format, fingerprints, or command-seq allocation
  order. The matcher consolidation must be behavior-preserving.

Completion gate:
Criterion shows measurable improvement on `workflow_replay_small_history_memory`,
`workflow_replay_large_history_memory`, `workflow_cached_wake_poll_memory`, and
the SQLite/Postgres append-commit benches, with no regression beyond noise; the
five schedule bodies are one helper; eviction cost is independent of
`max_cached_workflows`.

Testing plan:
- The full `tests/replay_core.rs` suite must pass unchanged — the
  behavior-preservation proof for the matcher consolidation.
- New replay tests per consolidated command kind under unfavorable orderings.
- Cache-bound microbenchmark at bounds of 1,000 and 100,000.
- Criterion against `benches/baselines/` with recorded medians.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 3A: single `match_or_append_command` helper | Missing: helper plus removal of the five bodies at `src/runtime.rs:2062`, `:2290`, `:2574`, `:2845`, `:3071`. |
| Incomplete | Work | 3B: command matching without cloning events | Missing: borrow-based comparison at the nine `.cloned()` sites, benchmarked on a large-inline-payload history. |
| Incomplete | Work | 3C: `split_start_event` takes the chunk by value | Missing: signature change at `src/worker.rs:1734` and recovery benchmark delta. |
| Incomplete | Work | 3D: batch commit moves instead of cloning | Missing: `std::mem::take` at `src/worker.rs:526` and benchmark delta. |
| Incomplete | Work | 3E: prefetch validates before cloning | Missing: reference-first validation at `src/worker.rs:1655`. |
| Incomplete | Work | 3F: O(1) cache eviction adopting the TS idiom | Missing: access index replacing the `min_by_key` scan at `src/worker.rs:1621`, plus the scaling benchmark. |
| Incomplete | Work | 3G: cached registry name lists | Missing: worker-side caching of `src/registry.rs:196`. |
| Incomplete | Work | 3H: incremental change-version merge | Missing: dedup map on `CachedWorkflow` replacing the per-task rebuild at `src/worker.rs:1422`. |
| Incomplete | Decision | Collapse `ReadyEventIndexes` to one keyed map | Missing: benchmark comparison and a recorded choice. |
| Incomplete | Decision | Filter ready events at ingest instead of skipping at peek | Missing: evaluation of the TS arrangement (`runtime.ts:921`) against Rust's carried-index requirement. |
| Incomplete | Test | Replay suite unchanged after consolidation | Missing: full `tests/replay_core.rs` green plus new per-kind cases. |
| Incomplete | Gate | Criterion medians improve on targeted paths | Missing: before/after run against `benches/baselines/`. |

---

## Phase 4: TypeScript replay memory model

Goal:
TypeScript replay memory is bounded by chunk size rather than by history length,
matching `SPEC.md` §4.3 and the README's "No Event History Limit" claim, and a
hot workflow's memory stops growing with the number of ready events it has seen.

Scope:
- Cold replay bulk-loads the whole history. `#claimWithCompleteReplayHistory`
  (`typescript/packages/core/src/worker.ts:743`) loops `streamHistory`
  accumulating `history.push(...chunk.events)` until the entire history through
  the replay target sits in one array, then hands it to `HotWorkflowExecution` as
  `prefetchedHistory`. Rust instead streams chunk-by-chunk into the poll loop
  (`needs_more_history_after`/`append_replay_events`) and drains consumed events.
  Restructure the TS cold path so the runtime pulls the next chunk when it needs
  one, and drops events it has passed.
- Full histories are cached and copied twice per commit.
  `#updateWorkflowHistoryCacheAfterCommit` (`worker.ts:805`) does
  `[...claim.prefetchedHistory]` and `#storeWorkflowHistory` (`worker.ts:861`)
  does another `[...history]` into `#workflowHistoryCache` (`worker.ts:239`,
  default 1024 entries at `:268`). A run with N events across N/k tasks copies an
  O(N) array on every task, which is O(N²/k) work and allocation over the run's
  lifetime, and the cache holds up to 1024 complete histories with all inline
  payloads. Bound the cache by total retained bytes rather than entry count, or
  remove it in favor of chunked streaming once the first item lands.
- Ready-event ingest maps are never consumed. The eleven maps at
  `typescript/packages/core/src/runtime.ts:880`–`:890` are only ever `.set()`
  (`#ingestHistory`, `:944`); the sole `.delete()` calls in the file are on
  `#hotWaiters`. `markHotCommitAccepted` (`:1063`) clears the eight append
  buffers but not the maps, so a hot workflow retains one `HistoryEvent` — with
  its payload — per ready event for the lifetime of the run. This violates the
  `AGENTS.md` rule against unbounded in-memory collections on workflow hot paths.
  Rust's `take_indexed` removes on consume; adopt that, and carry only
  unconsumed entries the way `CachedWorkflow::unconsumed_indexes` does.
- Add memory regression coverage: replaying a large history must hold heap
  proportional to the chunk size, asserted rather than assumed.

Out of scope:
- Changing the hot execution model itself. Hot execution is the right answer for
  a promise-based runtime; only its memory behavior is in scope.

Completion gate:
Replaying a workflow with a large history holds heap proportional to
`historyFetchMaxEvents`/`historyFetchMaxBytes` rather than to history length; a
hot workflow that completes many activities shows flat retained heap across
tasks; per-task history array copies are gone.

Testing plan:
- Memory test: cold-replay a synthetic large history and assert retained heap
  stays within a chunk-proportional bound.
- Memory test: hot workflow completing many activities shows flat retained heap
  across commits.
- Existing `packages/core/test/runtime.test.ts` and `worker.test.ts` green,
  proving the restructure is behavior-preserving.
- Benchmark: `packages/benchmark` recovery profile before/after.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Scope | Replay memory bounded by chunk size, not history length | Missing: chunked cold path replacing `worker.ts:743`. |
| Incomplete | Work | 4A: chunked cold replay in the TS runtime | Missing: pull-next-chunk protocol in `WorkflowRuntimeContext` and the worker path that feeds it. |
| Incomplete | Work | 4B: remove per-commit full-history copies | Missing: elimination of `[...claim.prefetchedHistory]` (`worker.ts:805`) and `[...history]` (`worker.ts:867`). |
| Incomplete | Work | 4C: bound or remove `#workflowHistoryCache` | Missing: byte-bounded cache or removal, with the entry-count default at `worker.ts:268` reconsidered. |
| Incomplete | Work | 4D: consume ready-event ingest maps | Missing: removal-on-consume for the eleven maps at `runtime.ts:880`–`:890`, mirroring Rust `take_indexed`. |
| Incomplete | Work | 4E: `hotSuspendByKey` silently clobbers concurrent waiters | Found during Phase 1 review. `hotSuspendByKey` (`runtime.ts:1060`) unconditionally does `#hotWaiters.set(commandKey(id), waiter)`, so a second suspend on the same command id replaces the first waiter and its deferred never settles — two concurrent `handle.result()` awaits on the same handle hang the run silently, with no side effect involved. Phase 1's guard blocks only the from-inside-a-callback path. Missing: detection or multi-waiter support, plus a deterministic test for two concurrent awaits on one handle. |
| Incomplete | Test | Cold-replay heap bound | Missing: memory assertion test. |
| Incomplete | Test | Hot-workflow flat retained heap | Missing: memory assertion test across many activity completions. |
| Incomplete | Test | Existing TS runtime and worker suites green | Missing: run after the restructure. |
| Incomplete | Gate | TS recovery benchmark before/after | Missing: `packages/benchmark` run; note that no TS run was possible during review. |
| Incomplete | Risk | Hot mode relies on `prefetchedHistory` deltas | Missing: confirmation that `advanceHotClaim` (`runtime.ts:1052`) still receives correct deltas once the cold path is chunked. |

---

## Phase 5: TypeScript determinism-guard blast radius

Goal:
Determinism enforcement stops permanently altering and taxing every consumer in
the host process, without weakening the guarantee for workflow code.

Scope:
- `installNondeterminismGuards` (`typescript/packages/core/src/runtime.ts:344`)
  is called from exactly one site — the `HotWorkflowExecution` constructor
  (`:244`) — and permanently replaces roughly 27 process-global built-ins with no
  uninstall path: `Date` and `Date.now`, `Math.random`, `performance.now`,
  `crypto.randomUUID`, `crypto.getRandomValues`, `process.hrtime` (+`.bigint`),
  `process.env`, `process.cwd`, `process.chdir`, `process.cpuUsage`,
  `process.memoryUsage`, `process.nextTick`, `process.resourceUsage`,
  `process.uptime`, `setTimeout`, `setInterval`, `setImmediate`,
  `queueMicrotask`, `requestAnimationFrame`, `requestIdleCallback`, `fetch`,
  `WebSocket`, `EventSource`, `XMLHttpRequest`, and `Promise.all`/`race`/
  `allSettled`/`any` (`:657`–`:688`). Every guarded call performs an
  `AsyncLocalStorage.getStore()` lookup, and `process.env` becomes a `Proxy`
  whose `get`/`has`/`ownKeys`/`getOwnPropertyDescriptor` traps
  (`:729`–`:772`) run that lookup on every property read — including reads from
  the host application, its logging and telemetry libraries, and its database
  drivers. Running one workflow imposes this on the whole process forever.
- Make the runtime guard opt-in-shaped rather than unconditional: default on in
  development and under the test harness, off in production, controlled by an
  explicit worker option. The repository already ships the zero-cost static path
  — `packages/eslint-plugin` (984 lines) plus `npm run lint:determinism` — which
  should carry the primary enforcement; the runtime guard is the backstop.
- Narrow the guarded set to genuinely determinism-critical APIs (`Date`,
  `Math.random`, `crypto.*`, the timer family, `Promise.race`/`any`) and drop the
  process-introspection guards (`cpuUsage`, `memoryUsage`, `resourceUsage`,
  `uptime`, `chdir`), which expand blast radius for marginal determinism value.
- Never `Proxy` `process.env`; guard the module-level accessor only, or drop it
  in favor of the lint rule.
- Add an uninstall path so embedding hosts and test harnesses can restore
  originals, and so the guards are not load-order-dependent (modules that
  captured `Date`, `setTimeout`, or `fetch` before the first workflow ran are
  silently unguarded today).
- Record the divergence explicitly: Rust performs no runtime determinism
  enforcement at all, relying on documentation, the manifest, and review. Decide
  and document whether that asymmetry is intended, or whether Rust should gain an
  equivalent debug-mode check.

Out of scope:
- V8 isolate or `vm` context sandboxing for workflow code. That is the
  heavyweight alternative and a separate design.

Completion gate:
A host process running a Durust worker shows no measurable regression on
`Date.now()`, `process.env` reads, and `Promise.all` outside workflow code with
guards disabled; guards remain fully effective for workflow code when enabled;
the determinism test suite passes in both modes; an uninstall path restores
original globals.

Testing plan:
- Microbenchmark: `Date.now()`, `process.env.X`, and `Promise.all` with guards
  installed versus not, reported in the plan.
- Determinism suite (`packages/core/test/determinism-lint.test.ts`,
  `test-d/determinism/**`) green with guards enabled.
- Test that guards disabled still reject nondeterminism statically via the
  eslint plugin.
- Test that uninstall restores identity of every patched global.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A: guard install becomes an explicit worker option | Missing: option plumbed to `runtime.ts:244`, defaulting on outside production. |
| Incomplete | Work | 5B: narrow the guarded global set | Missing: removal of the process-introspection guards at `runtime.ts:537`–`:585`. |
| Incomplete | Work | 5C: stop proxying `process.env` | Missing: replacement of `createGuardedProcessEnv` (`runtime.ts:729`). |
| Incomplete | Work | 5D: uninstall path | Missing: restore function and its identity test; the `nondeterminismGuardsInstalled` latch at `runtime.ts:56` is currently one-way. |
| Incomplete | Test | Host-process cost microbenchmark | Missing: measured numbers; no Node run was possible during review. |
| Incomplete | Test | Determinism suite green in both modes | Missing: runs with guards enabled and disabled. |
| Incomplete | Test | Uninstall restores original globals | Missing: identity assertions per patched global. |
| Incomplete | Risk | Load-order-dependent enforcement | Missing: assessment of modules capturing globals before the first workflow runs; today they silently bypass the guard. |
| Incomplete | Risk | `AsyncLocalStorage` context escaping into host callbacks | Missing: test for a workflow-created callback invoked later by a library, which would trip the guard spuriously. |
| Incomplete | Decision | Rust has no runtime determinism enforcement | Missing: recorded decision on whether the asymmetry is intended. |

---

## Phase 6: Shared map engine (both runtimes)

Goal:
Activity-map and child-workflow-map fanout semantics live in one tested state
machine per language, driven by one shared specification, instead of six
independent implementations.

Scope:
- The fanout state machine — item materialization, `max_in_flight` accounting,
  per-item retry and failure-mode policy, result-manifest assembly, parent
  notification — is implemented six times. Rust: `src/memory.rs:1485`, `:2037`,
  `:2101`, `:2220`, `:2257`, `:2288`, `:2336`, `:2430`; `src/sqlite.rs:4320`,
  `:4413`, `:4483`, `:4583`, `:4652`, `:4690`, `:4782`, `:4938`;
  `src/postgres.rs:7039`, `:7179`, `:7303`, `:7440`, `:7529`, `:7591`, `:7699`,
  `:7891`. TypeScript: `packages/core/src/backend.ts:1016` and `:1155`,
  `packages/sqlite/src/index.ts:1526` and `:1797`,
  `packages/postgres/src/index.ts:4177` and `:4316`. Manifest
  normalize/hydrate-for-storage is triplicated on top in Rust
  (`src/memory.rs:3165`–`:3345`, `src/sqlite.rs:2666`–`:2853`,
  `src/postgres.rs:5121`–`:5434`). 0015 Phase 3 unified the single-decision
  helpers into `provider_util`; the state machine was left out and is the largest
  remaining source of cross-provider drift in both languages.
- Extract a pure engine per language (`src/map_engine.rs`,
  `packages/core/src/map-engine.ts`), shaped like the existing `provider_util`
  decisions but covering the whole machine: given descriptor state plus one input
  event (descriptor created, item completed, item failed, item timed out, parent
  cancelled), return the ordered list of effects to apply. Effects are plain
  data, so sync providers (Rust memory and SQLite) and async providers (Rust
  Postgres, all TS providers) apply them inside their own transaction without an
  async-trait reshape.
- Each provider keeps only its storage primitives: read descriptor state, read a
  manifest page, insert item rows, insert result rows, append parent history.
- Manifest normalize/hydrate for storage becomes one generic pass over
  `PayloadRef` levels driven by each provider's existing blob put/get.
- One shared transition table, checked in as data, asserted by both a Rust test
  and a TypeScript test, so the two engines cannot drift.
- Record a Decision on whether `DurableBackend` should split into a narrow
  storage trait plus engine-provided defaults. The extraction is the substance;
  the trait split is a follow-on that should not be committed to first.

Out of scope:
- Any change to map semantics, wire format, manifest paging sizes, or the map
  task descriptor shapes.

Completion gate:
Each language has one engine shared by its three providers; conformance passes
unchanged on memory, SQLite (close/reopen), and Postgres in both languages; map
fanout benchmarks show no regression; the shared transition table is asserted by
both runtimes.

Testing plan:
- Table-driven unit tests over each pure engine covering every transition,
  including `max_in_flight` boundaries, retry exhaustion, both failure modes,
  parent cancellation mid-fanout, and empty manifests.
- Existing map conformance cases pass unchanged in both languages as the
  behavior-preservation proof.
- Mutation checks: neutering one transition must fail conformance on all three
  providers of that language simultaneously.
- Criterion `activity_map_*` / `child_workflow_map_*` and the TS equivalents.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Scope | One fanout state machine per language | Missing: `src/map_engine.rs`, `packages/core/src/map-engine.ts`, and removal of the six implementations. |
| Incomplete | Work | 6A: Rust pure engine with effect list | Missing: module, effect enum, table-driven unit tests. |
| Incomplete | Work | 6B: Rust providers apply engine effects | Missing: `memory.rs`, `sqlite.rs`, `postgres.rs` map functions replaced. |
| Incomplete | Work | 6C: TS pure engine with effect list | Missing: module and unit tests. |
| Incomplete | Work | 6D: TS providers apply engine effects | Missing: `core/backend.ts`, `sqlite`, `postgres` map functions replaced. |
| Incomplete | Work | 6E: generic manifest normalize/hydrate | Missing: one shared pass per language replacing the triplicated functions. |
| Incomplete | Work | 6F: shared transition table asserted in both languages | Missing: checked-in table plus the Rust and TS tests reading it. |
| Incomplete | Test | Map conformance unchanged, all six providers | Missing: suites green in both languages including SQLite close/reopen and Postgres. |
| Incomplete | Test | Cross-provider mutation check | Missing: proof that neutering one transition fails all three providers per language. |
| Incomplete | Gate | Map fanout benchmarks show no regression | Missing: Criterion and TS benchmark runs. |
| Incomplete | Decision | Split `DurableBackend` into storage trait plus engine defaults | Missing: decision recorded after 6A–6E land. |
| Incomplete | Risk | Postgres set-based batch paths versus per-item effects | Missing: check that batched materialization can still issue one set-based statement per effect group. |

---

## Phase 7: Cross-implementation convergence

Goal:
The two runtimes are proved equivalent where they must be, explicitly divergent
where the host languages force it, and each has adopted the other's better
answer.

Scope:
- The existing shared corpus proves vocabulary, not behavior.
  `typescript/fixtures/contract/{core-events,provider-io,benchmark-output}.json`
  is read by both `tests/contract_fixtures.rs` and
  `typescript/packages/core/test/fixtures.test.ts`, but its assertions cover
  event-type names, fingerprint helpers, payload-ref shapes, manifest shapes,
  failure shapes, and provider request/outcome vocabulary. Nothing pins
  behavior: the two runtimes could agree on every name and still diverge on
  select winner tie-breaking, map transition ordering, terminal-with-leftover-
  command handling, or signal consumption races.
- Add a behavioral golden corpus: cases of the form (workflow program identifier,
  input history, live signals, now) → (expected commit: append events in order,
  upserted and deleted waits, scheduled activities and maps, consumed signals,
  query projection). Both runtimes execute every case through their real runtime
  and must produce byte-identical commits. Seed it from the scenarios that
  already exist in both suites, then extend to the orderings `AGENTS.md` calls
  for: completions arriving before and after unrelated command events, cached/hot
  versus cold, multi-chunk.
- Maintain an explicit invariant parity ledger — a checked-in document listing
  each cross-cutting invariant (bounded replay memory, exactly-once ready-event
  consumption, no unbounded in-workflow collections, deterministic disposal of
  abandoned executions, terminal-with-leftover-command divergence detection,
  re-entrancy rejection) and, for each, the test in each language that proves it.
  A row without a test in both columns is a gap, not a footnote.
- Record the forced divergence: Rust polls futures and rebuilds `RuntimeContext`
  per task; TypeScript keeps a hot promise chain and mutates its context in
  place. Neither can adopt the other. `SPEC.md` should state that the execution
  mechanism is language-local and that only the committed history and the
  provider contract are normative.
- Adopt-the-better-answer list, tracked to completion: Rust ← TypeScript for
  worker metrics and event sink, exponential idle/error backoff, maintenance
  opt-out, O(1) LRU eviction, and ready-event filtering at ingest (Phases 2 and
  3). TypeScript ← Rust for chunked replay streaming, exactly-once ready-event
  consumption, and deterministic execution disposal (Phases 1 and 4).

Out of scope:
- Generating one implementation from the other, or extracting a shared
  cross-language core. The pure-data parts are small and the fixture corpus is
  the cheaper convergence mechanism.

Completion gate:
Every behavioral corpus case produces byte-identical commits in both runtimes and
runs in both CI jobs; the invariant parity ledger has a named test in both
columns for every row; `SPEC.md` states which parts are normative and which are
language-local.

Testing plan:
- Corpus runner in each language, wired into `check-fixtures.mjs` alongside the
  existing vocabulary tests.
- A deliberate one-sided behavior change must fail the corpus in exactly one
  language — the mutation check that proves the corpus has teeth.
- CI runs the corpus in both the Rust and TypeScript jobs.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Scope | Behavioral parity proved by execution, not by reading | Missing: the golden corpus and its two runners. |
| Incomplete | Work | 7A: behavioral golden corpus format and seed cases | Missing: schema plus cases seeded from existing scenarios in both suites. |
| Incomplete | Work | 7B: Rust corpus runner | Missing: runner executing each case through the real runtime and asserting the commit. |
| Incomplete | Work | 7C: TypeScript corpus runner | Missing: same, wired into `typescript/scripts/check-fixtures.mjs`. |
| Incomplete | Work | 7D: invariant parity ledger | Missing: checked-in document with a named test per language per invariant. |
| Incomplete | Test | Corpus mutation check | Missing: proof that a one-sided behavior change fails exactly one language. |
| Incomplete | Gate | Corpus runs in both CI jobs | Missing: `.github/workflows/ci.yml` step and the TS job equivalent. |
| Incomplete | Doc | SPEC states what is normative versus language-local | Missing: `SPEC.md` §1/§4 text on the poll-versus-hot divergence. |
| Incomplete | Doc | Adopt-the-better-answer list tracked to completion | Missing: cross-references from Phases 1–4 rows into this ledger. |

---

## Ordering and Dependencies

- Phase 1 is first and independent in both languages: it converts reachable UB,
  a silently poisoned run, a worker-killing panic, and leaked executions into
  loud, testable failures, and it is small.
- Phase 2 should follow Phase 1, because fault isolation is what makes
  independent unattended loops safe. Its Rust half imports the TypeScript worker
  shape; its TypeScript half imports interval-paced maintenance.
- Phase 3 (Rust) and Phase 4 (TypeScript) are independent of each other and can
  run in parallel; both depend on Phase 1 only for ordering discipline. Phase 3's
  matcher consolidation must land before or with its clone removal so the fix is
  written once rather than nine times.
- Phase 5 is independent and can run at any time, but its measurements should be
  taken before Phase 4 changes TypeScript memory behavior, so the two effects are
  not confounded.
- Phase 6 is independent of Phases 1–5 and is the largest item; it touches files
  disjoint from Phases 3 and 4.
- Phase 7 depends on Phases 1, 2, 4, and 6 landing, since the corpus should pin
  the converged behavior rather than the current divergence. Its ledger,
  however, should be created first and filled in as the other phases land.
