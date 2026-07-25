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

> Review note: the TypeScript findings below were originally from source reading
> only, because no Node toolchain was available when the review was written. Node
> 24 and npm are now installed, Postgres 17 is available on `:5433` via
> `DURUST_POSTGRES_URL`, and `libsqlite3-dev` is installed (without it `cargo
> test` fails to *link*, which is why only `cargo check` had been verified). Every
> phase gate below is therefore executable; findings are no longer allowed to rest
> on source reading.

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
  restore it after via a `Drop` guard, so the slot is restored on the unwinding
  path too. A non-dereferenceable sentinel is parked in the slot rather than a
  null, because null cannot distinguish re-entrancy from "no context installed"
  and the two need different messages.
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
| Complete | Work | 1F: TS `HotWorkflowExecution.dispose()` | Reviewed and approved. `HotWorkflowExecutionDisposedError` carries a `reason` naming the call site; `dispose()` / `#assertLive()` / `WorkflowRuntimeContext.disposeHot()`. Public surface added is exactly `dispose(reason)` on an already-exported class — a `disposed` getter was removed as unbudgeted API with no production consumer. The disposal refusal lives in the shared durable-API gate rather than `#nextCommandId()`: `publish()`, `continueAsNew()`, and `handle.result()` on a spawned handle all mutate the context without allocating a command seq, and the first design missed all three. No commit can survive disposal — `nextCommit()` re-checks after its `await`, and `toCommit()`, verified as the sole origin of a `WorkflowTaskCommit` in the package, refuses. |
| Complete | Work | 1F-b: two further abandoned-execution sites | Found by the 1F implementer, outside the three the plan enumerated. `worker.ts:675`/`:679` — a cached execution failing the hot-wake criteria is deleted in favour of a cold replay, abandoning its parked chain; one `dispose()` in the `if (cached !== undefined)` block covers both supersession paths. `worker.ts:848` — `#updateWorkflowExecutionCacheAfterCommit` deletes when `#workflowExecutionCacheSize === 0`, so **with the execution cache disabled every task abandons a parked execution**, making this the highest-volume instance of the bug. The `closed || cacheSize === 0` branch was split so only the cache-disabled arm disposes: a terminal execution owns no parked waiter, and disposing it would attach a misleading reason string. The split is not load-bearing for correctness but is a hot-path decision: `dispose()` constructs its error eagerly at ~1122 ns dominated by stack capture, so merging the arms would pay that on every committed terminal task under a disabled execution cache — against a memory-backend append-commit in the ~700 ns range — to settle zero waiters. Adopt-and-discard confirmed empirically reached at all five sites; review additionally instrumented `#hotWaiters.set` and found **zero** post-disposal registrations across the suite. |
| Complete | Work | 1G: TS re-entrancy guard did not cover payload encoding | Reviewed and approved. `#activeSideEffectKey` replaced by a single `#userCodeFrameApi`/`#userCodeFrameDetail` frame, non-null whenever any durable API runs user-controlled code between allocating its seq and appending its event — ten windows. Gate renamed `#assertDurableApiAllowed`, since it now carries disposal (1F), callback re-entrancy (1C), and conversion re-entrancy (1G). Mutation exhibits the inverted pair for **seven** builders in one diff. Found while writing the SPEC contract, not by review of the guard itself. |
| Complete | Decision | Reject conversion re-entrancy rather than hoisting encoding above seq allocation | Hoisting was the coordinator's preferred fix and is **wrong**; recorded because the hot-path cost will tempt someone to propose it again. Hoisting changes no fingerprint (none of the five depends on the command id) and no ordering for correct workflows, but on the broken case it converts permanent poisoning into **silent success**: the nested call takes seq N, the command takes N+1, history records them in order, and cold replay is clean because the builder re-invokes the same `toJSON` on the replay path. Review proved both halves with probes — hoisted: `["WorkflowStarted","VersionMarker#1","ActivityScheduled#2"]`, `cold replay error: null`. Then it added a Phase-3-style "skip rebuilding the event on replay" optimisation on top and re-ran against that same recorded history: `nondeterminism: expected ActivityScheduled for command 1, found VersionMarker`. So hoisting trades a loud failure at record time for a latent failure at **upgrade** time, on histories that can no longer be changed, and its correctness silently depends on an invariant nothing states — in a codebase whose Phases 3 and 4 are explicitly about deleting redundant replay-path work. |
| Accepted | Gate | Cost of the 1G conversion frame | ~1.6 ns per durable call on the *cheapest* durable ops, from the gate's extra branch alone — roughly what the Phase 1 F4 allocation fix saved, so the hot path returns to its pre-F4 cost. On the paths that actually open a window the cost is **below the measurement floor**: 3,000 `callActivity` commands per task, four interleaved trials, no-window `min=5.616/5.706/5.574/5.540 ms` versus shipped `5.402/5.473/5.560/5.514 ms` — two field writes and a `try`/`finally` disappear next to a full `encodePayload`, digest, and fingerprint. Accepted: it closes a vector that permanently destroys a run's replayability. Cheapest recovery if ever wanted is that `getVersion` consults the gate twice, once by name and once via the `#nextCommandId()` backstop — pre-existing, not introduced here. |
| Complete | Work | 1G-b: pin the decode-path ordering invariant | Review audited every `decodePayload` site against every seq allocation and append. Schema `decode()` is safe, but for a stronger reason than "record and replay order match": at every site the append or the `#advanceReplay()` *strictly precedes* the decode, so a durable call from a schema `decode()` could not invert a pair even if made. The two record-path sites are load-bearing — `runtime.ts:1314→:1330` (`resolveSignal` live) and `:1544→:1562` (`resolveHotSignalBranch`) — and nothing states or asserts the ordering. A future change that decodes before appending, e.g. to validate a payload before recording it, silently reopens exactly the vector 1G closed, and no test would catch it. Closed with a comment at each of the two sites stating the append must stay above the decode and why, naming the specific temptation (validating a payload before recording it) and the consequence (reopens the out-of-order pair with no test to catch it). A runtime assertion was evaluated and rejected: it would require inverting the gate's polarity to *require* a frame rather than forbid one — new machinery, a new state kind, and a hot-path cost on every signal consume — to catch a mistake only a source edit can introduce; and a test would assert the *absence* of an error, so it would pass both before and after someone moved the decode. A toothless test is worse than the comment. |
| Incomplete | Decision | Workflow-output conversion: TypeScript rejects what Rust permits | Found while writing the §16 contract; the first citation was wrong and review corrected it, which sharpened the finding. TypeScript opens a frame for workflow completion (`runtime.ts:2075`), so a durable API called from the output's `toJSON` fails the task. Rust does **not** merely lack that guard: the output is encoded at `src/registry.rs:102` *inside the workflow future*, which runs with the context installed by `poll_with_runtime_context` but with no borrow parked — verified, `src/worker.rs` contains zero `with_context`, and `:130`/`:152` are the client's start input and signal payload, not the output. So Rust **permits** a durable API from the workflow output's `Serialize`, appending a well-ordered marker ahead of `WorkflowCompleted` that replays cleanly, while TypeScript rejects the same program. A program written against §16 works in Rust and fails in TypeScript with no warning. The other two non-command windows have parity: Rust's `publish` (`src/runtime.rs:1396`) and `continue_as_new` (`:1279`) both encode inside `with_context`, matching TS frames `:2106` and `:2124`. Missing: a decision on which runtime moves, and a Phase 7 corpus case pinning it.
| Complete | Test | TypeScript test for the map-manifest exception | `SPEC.md` §16 now makes a *positive* claim — a durable API called from a map-manifest builder's iterator adapter or item conversion is legal, in both runtimes. Rust proves it with `manifest_builders_run_caller_iterators_outside_the_context_borrow`, whose ordering review independently confirmed through record plus cold replay. TypeScript has no such test: every `toJSON` test in the suite is a *rejection* test. Positive claims regress silently — if someone later opened a frame around `activityMapManifest` for symmetry with the other nine windows, TypeScript would start rejecting a documented-legal program and nothing would fail. Per `AGENTS.md`, anything `SPEC.md` mentions should have a deterministic test. Closed. `allows a durable API called from a map-manifest item conversion` pins all four points: the call succeeds, command order is `[VersionMarker#1, VersionMarker#2, ActivityMapScheduled#3]` with both markers ahead of the map command's seq, call order is `[["item-a", 1], ["item-b", 2]]` mirroring Rust's, and it replays — commit to a `MemoryBackend`, `streamHistory`, feed back as `prefetchedHistory` for a cold replay, which appends nothing and re-runs the conversions. **The two runtimes reach this exception through different caller code**: Rust through a lazy iterator adapter, TypeScript through the item schema adapter's `encode`. `toJSON` is *not* the TypeScript hook here — `activityMapManifest` calls `encodePayload` with no codec, so it defaults to MessagePack, which does not honour `toJSON` and rejects the method outright (`Unrecognized object: [object Function]`). `toJSON` remains reachable in the *guarded* command builders, which encode with the workflow's configured `#payloadCodec`. Worth carrying into the Phase 7 parity ledger. |
| Incomplete | Risk | Prepare-throw leaves a poisoned cache entry neither deleted nor disposed | Pre-existing, adjacent to `worker.ts:460`. When `#prepareWorkflowTaskFromCacheOrReplay` itself throws — e.g. `cached.execution.advance()` raising a fatal error — `prepared` stays `null`, so the cache entry survives and the next claim reuses it. Fixing it needs the cache key hoisted out of the `try`, which is worker-loop shape. Missing: assignment to Phase 2 alongside row 2H. |
| Complete | Test | Rust nested-durable-API replay regression | `tests/replay_core.rs::durable_api_inside_side_effect_fails_the_task_without_recording_markers`: task fails carrying both `workflow task panicked` and `durable APIs are not re-entrant`, history byte-equal and length 1, neither half of the `[VersionMarker, SideEffectMarker]` pair present, no `workflow_change_versions` record, empty cache. |
| Complete | Test | TS nested-durable-API replay regression | 14-case table in `packages/core/test/runtime.test.ts`, each asserting its exact commit. Mutation check reproduced independently against a pristine `HEAD` tree: all cases fail exhibiting `["VersionMarker#2", "SideEffectMarker#1"]`. |
| Complete | Test | Rust panicking workflow and activity regressions | `tests/worker_run.rs`: across-passes survival, batched survival (`max_concurrent_workflow_tasks(4)` + prefetch 4, healthy neighbour commits `Some(8)` in the *same* `run_workflow_batch_once` call), activity retry-policy exhaustion. The batched case uses a separate counter-free workflow: review proved reusing the shared-counter one fails 13 runs in 15 under the default parallel runner. |
| Complete | Test | TS abandoned-execution settlement | Five worker tests, one per disposal site, each asserting the disposal reason, re-claimability, correct completion, and zero unhandled rejections via `flushUnhandledRejectionTurn()`. Plus unit tests for waiter settlement, idempotence, no-commit-after-disposal — including the case where the workflow *swallows* the disposal and returns, so the chain settles fulfilled on a disposed execution — and a table-driven detached-continuation inertness test covering five mutation shapes. The two-worker cold-replay supersession test pins a real ordering fact: the replacement execution runs its handler synchronously inside the constructor, so the fresh frame's `start` precedes the disposed frame's waiter rejection. Twenty isolated runs, stable. |
| Complete | Test | Miri aliasing check | `MIRIFLAGS="-Zmiri-strict-provenance" cargo +nightly miri test --locked --lib --no-default-features -- runtime::tests` — 10 passed, wired as a CI job. Proves the UB was real and reachable: with the guard removed Miri reports `not granting access to tag ... which is strongly protected` at the `unsafe { f(&mut *ptr) }`. Scope is deliberately narrow (8 of 38 lib tests) and the CI comment says so; `--no-default-features` is forced because rusqlite's C FFI is unexecutable under Miri, and a whole-lib run under `-Zmiri-disable-isolation` was killed at 25 minutes. |
| Complete | Test | TS rejecting async side-effect callback does not kill the worker | Table-driven over all three shapes (throw-before-await, throw-after-await, plain `Promise.reject`) using the `flushUnhandledRejectionTurn()` pattern. All three exited 1 before the fix and exit 0 with zero unhandled rejections after; re-verified by review with its own repro. |
| Complete | Doc | SPEC re-entrancy and fault contract | Landed at five insertion points — §4.1 (abandoned executions settle), §4.2 (task-not-worker, the `panic = "abort"` caveat, and the stated TS/Rust divergence on generic throws), §6.3 (activity panics through the retry envelope), §16 (non-re-entrancy by shape, the map-manifest carve-out, and the workflow-output divergence), §17.2 (the closure is synchronous). Reviewed against the committed code rather than against its own citations; two factual corrections were made during review. Originally required: `SPEC.md` §16/§17.2 text covering both runtimes, including that a `sideEffect` closure is synchronous and a thenable return is rejected; that a caught panic fails the task and is retried, not the workflow; and that a downstream crate building with `panic = "abort"` keeps the old worker-killing behaviour, because `catch_unwind` never returns `Err` there. This repo sets no `panic` profile key, so every profile here is `unwind`. |
| Complete | Decision | `sideEffect` callbacks are synchronous; a thenable return is rejected | Pre-1.0 public behaviour change, recorded per the Implementation Principles. Rationale: the marker is recorded synchronously, so awaiting the promise is impossible without changing the history contract; converges TypeScript onto Rust's `side_effect(FnOnce() -> T)`, which cannot be async. Two effects versus `HEAD`, both improvements: the task now fails loudly instead of recording a serialized `Promise` as the marker value, and a rejecting async callback no longer reaches Node's default `unhandled-rejections=throw` — `HEAD` reached that only *after* already recording the bad marker. Audit found no async usage in `packages/examples/src/control-flow.ts:67`, `test-d/determinism/valid/workflows.ts:37`, `typescript/README.md:279`, `README.md:729`, or `SPEC.md:2368`. Recorded in `SPEC.md` §17.2. |
| Complete | Decision | A caught workflow panic fails the *task*, not the *workflow* | The plan originally said "convert a caught panic into `WorkflowFailed`". Rejected after research. `Error::Nondeterminism` is not "history diverged" in this codebase — it is the fatal workflow-task error channel: `prepare_workflow_poll` (`src/worker.rs:1490`) commits `WorkflowFailed` for every workflow `Err` *except* `Nondeterminism`/`UnsupportedWorkflowVersion`, which `release_failed_workflow_task` (`:699`) re-releases with the 60 s backoff. Decisive argument: a panic can occur while replaying an already-progressed run, so committing `WorkflowFailed` would destroy committed progress (in-flight activities, timers) for a bug a redeploy fixes, with no evidence the recorded progress was wrong. Rust also already has a terminal channel for *intentional* failure — a workflow returning `Err(...)` — and making both terminal discards that distinction. Matches Temporal's `BlockWorkflow` default. Reviewer traced all three arguments to the code and could not break the decision; its mutation routing panics to `WorkflowFailed` fails both regressions at their `unwrap_err()`, with the activity test correctly unaffected. |
| Incomplete | Decision | Rust and TypeScript diverge on generic workflow-code bugs | After 1D, the Phase 1 re-entrancy case is **converged**: both runtimes fail the task, back off 60 s, and commit nothing. A *generic* bug (JS `TypeError` vs Rust `unwrap()` panic) still diverges: TS calls `failWorkflow(...)` for a terminal `WorkflowFailed`; Rust now retries the task. The divergence is real, not incidental — JS cannot distinguish a bug-throw from a business-throw and Rust can. If it is closed, the correct direction is TypeScript adopting Rust's behaviour, since terminating a run on a replay-time bug is unrecoverable. Missing: a Phase 7 corpus case pinning whichever answer is chosen. |
| Complete | Decision | Type-level rejection of async `sideEffect` callbacks: feasible, deliberately declined | Four variants tested under `tsc --strict`. An **overload pair** produces no error at all — resolution picks the first matching signature and `never` is assignable everywhere (`TS2578`). A conditional **return** type catches the assigned form but misses the statement form `await sideEffect(k, async () => v)`. A conditional **parameter** type (`() => T extends PromiseLike<unknown> ? never : T`) *does* catch every real async shape including the statement form and an explicit `Promise.resolve` return, and leaves every synchronous shape clean — so this is feasible, contrary to the first evaluation. Declined anyway on cost: it breaks naive generic forwarding (`function wrap<T>(k, f: () => T) { return sideEffect(k, f) }` fails with `TS2345`, a worse diagnostic than the runtime message), and it still misses a hand-rolled non-`PromiseLike` thenable that the runtime guard catches — so it could only supplement the runtime guard, never replace it. |

Closing note — what Phase 1 did **not** solve:

Phase 1 converted silent corruption into loud failure, and every specific claim
in it holds. But "loud" currently means a log line and a retry loop, not an
alert. Re-entrancy, thenable callbacks, Rust panics, disposal, and genuine
history divergence all land in the same undifferentiated bucket: task released,
60-second backoff, retried forever, with nothing in metrics distinguishing a
permanently poisoned run from a transient one. That is the correct *behaviour* —
every one of these is recoverable by a redeploy and none justifies destroying
committed progress — but an operator still cannot see a poisoned workflow
without reading worker logs. Phase 1 made the problem tractable rather than
solving it, by giving each failure a distinguishable message; `workflow task
panicked:` is the stable marker row 2F should count separately from divergence.
Nothing in Phase 1 should be read as having closed the observability gap.

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
| Complete | Work | 2G: dedicated `Error::TaskPanic` variant | Landed with the API budget below. `poll_cached` is the only construction site; the `workflow task panicked:` prefix is preserved verbatim. Routed identically to `Nondeterminism` through one predicate `fails_workflow_task_without_committing`, which replaced three copied `matches!`. Reviewer confirmed HEAD's three sites had identical variant sets, so adding `TaskPanic` to them is a strict no-op — a panic already matched as `Nondeterminism`. |

**API budget — `Error::TaskPanic(String)`** (per `AGENTS.md` Public API Discipline)

*Composition alternatives, both rejected.* (a) Keep `Error::Nondeterminism` and classify by
the `workflow task panicked:` message prefix, as TypeScript does at
`typescript/packages/core/src/worker.ts:1367`. (b) Route panics through
`Error::Application(DurableFailure::new("durust.task_panic", ..))`. (b) is disqualified
outright: `Application` is not in the non-committing set, so it would commit
`WorkflowFailed` and destroy the committed progress of a run that panicked mid-replay —
exactly the outcome the Phase 1 decision rejected.

*Why (a) is insufficient for Rust specifically.* Rust's routing is a compiler-checked
`match` on `Error`, not a string test: `release_failed_workflow_task`, the terminal-state
check, and the non-committing branch all decide commit-versus-release by variant.
TypeScript can afford prefix-sniffing because its errors carry nothing else to match on;
Rust has an exhaustive enum, where a variant is the idiomatic compiler-enforced
classifier. Under (a) the only way for any code — in-crate or downstream — to tell a
workflow bug from history divergence is a substring test against free-form operator prose
pinned by nothing but two test assertions. Second and more seriously,
`DurableFailure::from_error` is a **durable** projection: under (a) a panic and a real
divergence both serialize as `error_type: "durust.nondeterminism"`, so a workflow bug
would be recorded in history as nondeterminism — a mis-record that outlives the process.

*Invariant protected.* Not a scaling invariant — a **recovery** invariant.
`durust.nondeterminism` tells an operator to look for a history/version mismatch and
consider a rollback; `durust.task_panic` tells them to look for a panic backtrace. Both
classes retry silently on the backoff forever, so mis-signalling costs recovery time on
precisely the runs that have no other signal.

*Provider contract change.* None. No backend method, no history format, no map semantics.

*Cost and risk.* One variant on a 20-variant enum. `Error` is **not** `#[non_exhaustive]`,
so this breaks any downstream exhaustive `match`. Pre-1.0 and permitted; a separate
decision on marking `Error` `#[non_exhaustive]` before 1.0 is recorded below. Per the
plan's converge-on-the-better-answer principle the follow-up is for TypeScript to gain an
equivalent discriminator, not for Rust to drop to prefix-sniffing.

**API budget — `WorkerRunStats::workflow_tasks_failed: usize`**

*Composition.* None available. Once a per-task fault stops failing its pass (2H), the
fault has no other representation: the crate has no `tracing`/`log`, `Worker::run` returns
`Result<()>`, and `run_workflow_batch_once` returns only a committed count. A caller cannot
derive it from history either — a poisoned task writes nothing. *Invariant protected:* that
"the pass survived the fault" is not silently confused with "there was no fault"; it is the
sole signal the deterministic drivers have, and `tests/sim_worker.rs` now depends on it to
fail seeds on divergence. *Honest limit:* invisible under `Worker::run`, which discards
per-pass stats. The doc comment says so. It does not close the production observability
gap; 2F does.

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Decision | All `durust.*` `error_type` strings are a durable contract, and none are documented | `DurableFailure.error_type` is serialized into history (`WorkflowFailed.failure`, `ActivityFailed`, child-failure propagation), so any value that can reach history is a contract by construction — `durust.activity_panic` already is, asserted *from streamed history* at `tests/replay_core.rs:1928`. `durust.nondeterminism` and `durust.task_panic` cannot reach history today (both non-committing), but nothing enforces that and `Error::with_details`/`durable_failure()` are public. `SPEC.md` documents none of them and TypeScript mirrors none. Missing: enumerate them in `SPEC.md` and mirror in TS. |
| Incomplete | Decision | Mark `Error` `#[non_exhaustive]` before 1.0 | 2G broke downstream exhaustive matches by adding a variant. Permitted pre-1.0, but the next such addition should not be a breaking change. |
| Superseded | Work | 2G (original row) | Phase 1 routes a caught workflow panic through `Error::Nondeterminism`, which is correct for *retry* semantics but makes a panic indistinguishable from genuine history divergence in metrics — the same gap 2F records for TypeScript, so the runtimes stay converged including in the gap. Missing: a variant in `src/error.rs` routed identically (public enum plus the exhaustive `DurableFailure::from_error` match), surfaced separately in the 2F metrics. |
| Incomplete | Work | 2F: Rust worker metrics and event sink | Missing: surface matching TS `metrics()`/`onEvent`/`onError`, plus a test asserting repeated `Nondeterminism` retries are observable. Must cover the Phase 1 re-entrancy error specifically, not only genuine history divergence: it classifies as `nondeterminism:`, so TS `#releaseFailedWorkflowTask` re-releases it with the 60 s backoff, uncapped, and it is currently indistinguishable in metrics from real divergence. |
| Incomplete | Risk | 2H removed the only bound on a panicking-task retry loop | **Blocker found on review, measured not argued.** `nondeterminism_retry_backoff` (`src/worker.rs:2010`) is the only timing knob in the builder without a floor — `idle_wait` has `MIN_IDLE_WAIT`, both lease durations have `MIN_TASK_LEASE_DURATION` (`:2019`/`:2027`), `history_chunk_events` has `.max(1)` (`:2001`). At HEAD a zero backoff was harmless because the failure cap bounded it; after 2H it is the sole bound. Probe with `Duration::ZERO` and one permanently-panicking workflow: HEAD's `Worker::run` exits loudly in 4 s after 16 polls, the 2H tree **never returns** — 100% CPU, killed at 24 min. Worse than a spin: with `progressed = true` every pass the loop `continue`s without awaiting anything `Pending`, so on a current-thread runtime the sibling task holding the shutdown timer is never polled again and shutdown becomes unobservable. Missing: the clamp, its regression test, and a correction to the false invariant claimed in the comment at `src/worker.rs:1204-1209`. |
| Incomplete | Risk | `workflow_tasks_failed` claims a visibility `Worker::run` does not provide | Its doc comment says a poisoned run is "visible to a driver instead of retrying forever in silence". True for `run_until_idle` callers — i.e. tests — and false for the production loop: `Worker::run` builds `WorkerRunStats::default()` per pass and discards it (`src/worker.rs:446-447`), returning `Result<()>`, and the crate has no `tracing`, no `log`, and no `eprintln!` in `src/worker.rs`. Under 2H a permanently poisoned run is therefore: nothing committed, retried every 60 s, forever, with no error, log, or metric. Row 2F owns the real surface. Missing: an honest doc comment now, and the decision recorded in 2E. |
| Incomplete | Risk | 2H silently removed a divergence-detection channel from the simulation suite | Not self-reported; found on review. `run_workflow_batch_once` now reports a poisoned batch as `Ok(0)`, **indistinguishable from idle** (HEAD returned `Err(Nondeterminism(..))`). `tests/sim_worker.rs:411` wraps that call in `tolerate_faults`, whose whole job is turning an unexpected worker error into `sim.failure("unexpected_worker_error", ..)`; a genuine replay-divergence regression there no longer produces that failure, and the cache-eviction storm scenario — built to catch cold-replay divergence — degrades to step exhaustion with no message. `drain_error` is likewise dead for divergence at `:426`, `:528`, `:622`. Suite still green, so this is latent loss of diagnostic power, which is precisely the erosion the sim exists to prevent. Missing: a restored signal, e.g. failing the seed when `workflow_tasks_failed` rises outside an injected-fault window. |
| Incomplete | Risk | Backpressure-deferred prepares are invisible to the batch driver | Pre-existing, byte-identical to HEAD, neither created nor aggravated by 2H. `Ok(PreparedWorkflowTaskOutcome::Deferred) => {}` (`src/worker.rs:569`) counts as neither committed nor failed, so a fully-backpressured batch reports `progressed = false` and `run_until_idle` can declare the worker idle with work still queued. Same shape as the bug 2H fixed, on the adjacent path. |
| Incomplete | Risk | The batch and single-task paths disagree on deferred-task accounting | Found on review. The single-task path counts a *deferred* task as **committed**: `run_claimed_workflow_task` returns `Ok(())` after a successful backpressure release, so `run_workflow_once` returns `Ok(true)` and the stage reports `committed: 1`. The batch path counts the same outcome as nothing. Pre-existing. |
| Incomplete | Risk | `PayloadDecode` during prepare is the one per-run fault that can still kill a worker | A decode failure in `prepare_claimed_workflow_task_inner` *outside* the poll releases immediately (the task-scoped predicate is false), the pass returns `Err`, `wait_for_ready` returns at once because the task is claimable again, and 16 fast passes exit `Worker::run`. Unchanged from HEAD, so not a regression — but after 2H it leaves the set inconsistent in kind: an undecodable input can kill a worker while a panic, equally a per-run bug, cannot. Belongs with 2E. |
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
- Single command matcher. `poll_activity_schedule` (`src/runtime.rs:2153`),
  `ActivityMapSpawnFuture::poll_init` (`:2386`),
  `ChildWorkflowMapSpawnFuture::poll_init` (`:2671`),
  `ChildWorkflowSpawnFuture::poll_init` (`:2942`), and `TimerFuture::poll_init`
  (`:3168`) each re-implement the same protocol: block if history is unloaded,
  allocate the command seq, compute the fingerprint, then match the peeked
  event's variant/seq/fingerprint or append the command plus its side effect.
  0015 Phase 1 consolidated the `take_*` and `collect_*` sides; the schedule side
  was never consolidated and is where the remaining drift risk lives.
- Match without cloning. All nine `peek_replay_command_event().cloned()` sites
  (`src/runtime.rs:1035`, `:1103`, `:1334`, `:1844`, `:2189`, `:2422`, `:2704`,
  `:2971`, `:3177`) deep-clone the whole `HistoryEvent` — including
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
  (`src/runtime.rs:235`) into one `BTreeMap<CommandSeq, CommandReadyState>`.
  Benchmark first; record a Decision either way.

Out of scope:
- Any change to the history format, fingerprints, or command-seq allocation
  order. The matcher consolidation must be behavior-preserving.

Completion gate:
Criterion shows measurable improvement on `workflow_replay_small_history_memory`,
`workflow_replay_large_history_memory`, and `workflow_cached_wake_poll_memory`,
with no regression beyond noise, measured against a null control; the five
schedule bodies are one helper; eviction cost is independent of
`max_cached_workflows`.

> Gate correction: the original clause also named the SQLite/Postgres
> append-commit benches. That is unreachable and has been deleted.
> `setup_claimed_workflow_for_commit` (`benches/replay_core.rs:2080`) hands
> `commit_workflow_task` a hand-built batch inside `iter_batched` setup and
> measures only the provider call — no worker, no runtime. So **no Phase 3 row
> can move them**: not 3A/3B in `src/runtime.rs`, and not 3C–3H, which are all
> in `src/worker.rs`, a file those benches never call either. Confirmed
> empirically — used as a null control, the bench moved −0.76% with a range
> straddling zero. Replacing it would need a bench that runs the worker's
> commit path.

Testing plan:
- The full `tests/replay_core.rs` suite must pass unchanged — the
  behavior-preservation proof for the matcher consolidation.
- New replay tests per consolidated command kind under unfavorable orderings.
- Cache-bound microbenchmark at bounds of 1,000 and 100,000.
- Criterion against `benches/baselines/` with recorded medians.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: single `match_or_append_command` helper | `src/runtime.rs:853` owns the whole protocol — block on unloaded history, allocate the seq, build the fingerprint, then match the recorded command event or append it with its side effect. The five hand-written copies are gone; `CommandEventKind` reproduces every divergence message byte-identically and `CommandDisposition` carries the two per-kind tails. `SignalFuture::poll_init` is deliberately excluded: signals append no command event at init, and their recorded fact is the ready event `SignalConsumed`. The five HEAD bodies had **not** drifted on guard, seq order, check order, or message text. Per-kind always-append mutation coverage now 34/30/5/4/4. |
| Complete | Work | 3B: command matching without cloning events | All nine `.cloned()` peeks replaced by borrows. `SideEffectFuture` resolves key, validity, and value in one borrow through `ReplayedSideEffect`, so a panicking user `Deserialize` can no longer convert a clean `Nondeterminism` into a task panic. Guarded by `tests/replay_clone_budget.rs`, a counting-allocator budget in its own test binary: restoring `.cloned()` costs exactly **+1.00 payload copy per command event** (5.10 → 6.10) and **+67 allocations** (577 → 644). Criterion, control-normalised: `workflow_replay_large_inline_payload_memory` −6.70% (4/4), `_small_history_` −5.59% (6/6), `_large_history_` −4.24% (6/6), `workflow_cached_wake_poll_memory` −2.40% (6/6). A third arm restoring *only* the peek clone recovers essentially the whole improvement, which is what identifies 3B rather than 3A's append reorder as the source. |
| Complete | Decision | Command matching is a per-event **allocation** win, not a payload-copy win | The row's original premise was wrong and so were the first test names, bench, and doc comment. `inline_threshold_bytes` defaults to **8 KiB**, so command payloads above it reach replay as blob refs, and `SideEffectMarker.value` is hard-capped at that same 8 KiB. Measured: at the default threshold, restoring the matcher clone costs **0.00** extra payload copies per command event; at a raised threshold, exactly **1.00**. The benefit at shipped settings is ~8 allocations per replayed command event. **Trap recorded for the next engineer:** tests and benches asserting on payload shape must read `stream_history_for_replay`, not `stream_history` — the latter hydrates blob refs back to inline and reports "inline" whatever the backend stored. The first version of the clone guard had exactly this bug and passed while proving nothing; it was caught only by mutation-testing the guard itself. |
| Incomplete | Decision | A rejected command builder still burns a command seq | All five schedule bodies allocate the seq before their fallible per-kind work, so a missing `input_manifest`, a missing `workflow_id`, an encode failure, or an options-digest failure all consume a seq no history event will carry. Pre-existing and preserved by the consolidation; unreachable in normal use because the failing future is consumed by its `.await`. Missing: a decision on validating builder inputs before allocating, which would change the seq a subsequent command receives after a caught error. |
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
| Incomplete | Work | 4E: `hotSuspendByKey` silently clobbers concurrent waiters | Found during Phase 1 review. `hotSuspendByKey` (`runtime.ts:1060`) unconditionally does `#hotWaiters.set(commandKey(id), waiter)`, so a second suspend on the same command id replaces the first waiter and its deferred never settles — two concurrent `handle.result()` awaits on the same handle hang the run silently, with no side effect involved. Phase 1's guard blocks only the from-inside-a-callback path. Constraint from row 1F: a clobbered waiter is no longer in `#hotWaiters`, so `dispose()` cannot settle it either — the silent hang survives disposal by construction, and any fix must make the clobbered waiter reachable rather than relying on disposal to clean up. Missing: detection or multi-waiter support, plus a deterministic test for two concurrent awaits on one handle. |
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
| Complete | Work | 5A: guard install becomes an explicit worker option | Default on outside production, off in production, `NODE_ENV` re-read per execution. `WorkerOptions.nondeterminismGuards` (`worker.ts:73`) is stored at `:291` and spread conditionally at `:999` — `exactOptionalPropertyTypes: true` rejects an explicit `undefined`, so omitting the property is what selects the default. Suite green in **both** modes, 495/495. Known limit, recorded not fixed: the `NODE_ENV` re-read is **one-way** — it can turn guards on later, never off — and `nondeterminismGuards: false` does not disable guards already installed, so the option is really `installIfNotAlready` and the JSDoc over-promises. |
| Complete | Work | 5B: narrow the guarded global set | Dropped `cpuUsage`, `memoryUsage`, `resourceUsage`, `uptime`, `chdir`. Kept `Promise.all`/`allSettled` beyond the plan's prose keep-list (cost measured at +0.7 ns, inside noise); the plan's **drop** list is the normative one and 5B's row agrees. |
| Complete | Work | 5C: stop proxying `process.env` | **`process.env` is no longer patched at all** — no Proxy, no accessor. The plan's second option ("or drop it in favour of the lint rule") was taken, which also removed the root cause of the staleness bug below and the `console.log` breakage. Measured cost of the Proxy it replaces: **+192/+186/+195 ns per read across three independent access shapes**, and `Object.keys(process.env)` — what every config library does — at **7.5×**. With guards on, `process.env` overhead is now **exactly zero**, confirmed against the honest baseline (a process that loaded the module and never patched). |
| Complete | Work | 5D: uninstall path | `uninstallNondeterminismGuards()` verified over four full install/uninstall cycles plus a descriptor round-trip, and exported from `packages/core/src/index.ts:22,26`. Restores **identity**, not just behaviour. The absolute unpatch is deliberate: a "polite" unpatch that restored only when `current === guardValue` would leave an instrumentation wrapper closing over the guard, so the guard would keep throwing after an uninstall that reported success *and* `Date === originalDate` would be false — strictly worse than clobbering, and permanent once the ledger is cleared. The reviewer raised this as a finding and then withdrew it on that reasoning. |
| Complete | Test | Host-process cost microbenchmark | Reproduced independently. Guards-disabled is indistinguishable from a process that loaded the module and never patched — and that baseline is *stricter* than comparing to a pristine process, which would have flattered the result by ~5.5 ns of module-graph artifact. Gate met. Guards **on** costs +4.2 ns on `Date.now` and nothing measurable elsewhere. |
| Complete | Risk | `isProductionHost()` could be permanently poisoned by a stale environment capture | **Bug introduced by this phase, found on review, fixed at the root.** The capture was never cleared by uninstall but *was* written by the guarded `process.env` setter, so dev → install → `process.env = {…NODE_ENV:"production"}` → uninstall left the real env correctly restored while the capture still said production — and **every later execution decided "production" and never installed guards again for the life of the process**. Test harnesses and config bootstraps do replace `process.env` wholesale. Fixed by removing `process.env` patching entirely (5C), so no capture exists; `isProductionHost()` reads live. Pinned by a regression test driving **both** directions, and isolated exactly: re-introducing the original defect (capture the object, read `NODE_ENV` through it) kills that one test and no other. |
| Incomplete | Risk | The static-lint gap that justifies the production default is far wider than first reported | Not the captured-alias shape — **nothing in a non-linted module is checked at all**, including bare `Date.now()`. Probe: linting the workflow alone exits 0; adding the helper it calls reports three violations. Aggravating: this repo's own `workflowSources` is `test-d/determinism/valid/**/*.ts`, **one fixture file** covering no production code, and `packages/examples/src/**` produces 20+ diagnostics — the repo cannot satisfy its own lint. `scripts/determinism-lint.mjs` is workspace-private and unpublished. A consumer who never wires up ESLint now gets **zero** determinism enforcement in production where they previously got runtime rejection, and nothing in `README.md`, `SPEC.md`, or `typescript/README.md` says so. |
| Complete | Risk | With guards on, `console.log` of anything but a plain string threw inside workflow code | Fixed by 5C. The mechanism was **misattributed twice** before being measured: not `util.inspect`, which reads nothing (`inspect`/`format`/`formatWithOptions`/`String`/`JSON.stringify` all measured at **0** env reads), but `Console`'s colour-mode detection — `getColorDepth()` → `NO_COLOR`/`FORCE_COLOR`/`TERM` — per call, uncached, whenever an argument is not already a string. So the guard fired from a line the author wrote as `console.log`, naming an API they never touched, in the two environments where guards default on. It also meant an escaped-ALS unhandled rejection **could not be logged**: the handler's own `console.log` threw from inside the escaped context. Regression tests must use a real `new Console({ stdout: sink })` — vitest replaces `globalThis.console` with an interceptor that does no colour detection, so a test written against it is **vacuous**, which the first version of these tests was. |
| Complete | Decision | Two pre-existing HEAD defects found and fixed/recorded by this phase | (a) `Object.defineProperty(guardedDate, "now", ..)` on a Proxy with only `apply`/`construct` traps **forwards to the target**, so `Date.now` was patched onto the real `Date` — invisibly, permanently, unrestorably. The `RangeError` the implementer predicted is **not** reachable on HEAD (2005 module-instance cycles produce no recursion, each cycle binding a fresh `originalDateNow`); it becomes reachable the instant an uninstall resets the latch without moving the patch, which is why the fix was required. (b) The `process.env` Proxy breaks ordinary env **writes** process-wide: `Reflect.set` on an *existing* key routes through the `defineProperty` trap with a partial descriptor, which Node rejects — so once any workflow runs, `process.env.PATH = ..` throws for the whole host, forever. A stronger argument for 5C than the 195 ns. |
| Complete | Decision | `process.uptime` stays guarded — exception taken to the plan's drop list | The implementer dissented from the plan's framing and was right on the merits: `uptime()` is a monotonic clock, the same defect class as `performance.now()` and `process.hrtime()`, **both of which were kept**. Dropping it would leave three monotonic clocks with two enforced. The blast-radius argument that justifies dropping `cpuUsage`/`memoryUsage`/`resourceUsage`/`chdir` does not transfer, since nothing's hot path calls `uptime()`. **Row 5B therefore drops four, not five.** |
| Incomplete | Risk | The determinism lint is **file-granular**, not workflow-vs-driver | Sharper than first recorded, and it constrains the whole production-default argument. `packages/examples/src/**` produces 87 diagnostics and **zero are real violations** — every one is driver code sharing a file with a workflow definition. But `heartbeat.ts:42` also flags `await heartbeat()` inside an *activity* handler, so the rule is workflow-handler-vs-**everything else**: a listed file may contain no driver code, no activity bodies, no assertion helpers, and no host imports. That pulls against the cross-module advice — to get coverage you must isolate handlers into dedicated files *and* list every helper they import, and a helper shared with host code cannot be listed without false positives. Documented in `typescript/README.md`. |
| Incomplete | Decision | Surface clobbered globals from `uninstall` rather than silently discarding them | Follow-on, not required. The absolute unpatch is correct (see 5D), but it silently discards an instrumentation wrapper installed after the worker started. A `{ restored, clobbered: [..] }` return keeps identity restoration and removes the silence, at the cost of widening a public return type — which `AGENTS.md` says needs a written budget first. |
| Complete | Test | Determinism suite green in both modes | **495 passed / 98 skipped in both**, workspace-wide, plus build, `test:types`, and `lint:determinism` clean in each. |
| Complete | Test | Uninstall restores original globals | Per-global identity assertions, deliberately not a loop, plus a bounded `Reflect.ownKeys` completeness net over 13 host objects. The net is honest about its bound: an unledgered guard on `Intl.DateTimeFormat` or `Date.prototype.toLocaleString` **is** caught, one on `Reflect` or `JSON` is not. |
| Complete | Risk | Load-order-dependent enforcement | Pinned as-is: modules capturing `Date`/`setTimeout`/`fetch` at import time hold unguarded references forever, and uninstall bounds the window without closing the hole. The test is non-vacuous — it carries a positive control asserting a *fresh* `Date.now()` still rejects, and dies under the guards-never-throw mutation. |
| Complete | Risk | `AsyncLocalStorage` context escaping into host callbacks | Both directions pinned, both non-vacuous. A closure *created* in workflow code and invoked from host context correctly does **not** trip. A continuation *registered* inside workflow code on a host-owned promise (`hostPromise.then(cb)`) **does** — after the task committed, from inside the host's promise chain, where its default Node action is process termination. Real hazard, not contrived. A fix needs `runtimeStorage.exit()` at the boundary or `enterWith(undefined)` on task completion — a design change, out of scope here. |
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
| Complete | Work | 6A: Rust pure engine with effect list | `src/map_engine.rs`: pure `step(state, event) -> Result<Vec<MapEffect>, MapReject>`, 25 table-driven tests. Reviewer independently killed 13 mutations with precise failure messages; purity, `provider_util` retry delegation (no second copy of the backoff math), and zero attributable clippy warnings all verified by running code. Approved after the five required changes below. F1 (the D5×D9 commit-rollback side effect) is now *structurally* impossible, not merely absent: `DescriptorCreated` returns `Ok(materialize(..))` where a reject is not representable on that arm, confirmed by an exhaustive 2,592-state probe plus a re-run of the real-backend commit probe on all three providers. |

The six provider bugs 6A's drift analysis uncovered are tracked as their own rows
below rather than folded into the extraction, so each lands with its own
per-provider regression test and its own commit. Phase 6's "no change to map
semantics" boundary holds: **the engine preserves every behaviour, including the
defective ones**, and each row fixes one deliberately.

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6G: empty input manifest stalls the parent forever — **Rust only** | The runtimes disagree, and the correct answer is TypeScript's. TS memory and postgres already **complete** an empty map at descriptor creation; TS sqlite appears to stall only as a symptom of row 6T1, so at the engine layer all three TS providers agree on "complete". Rust converges to that. Until it does, the two runtimes are knowingly divergent here and Phase 7's corpus must not be seeded with an empty-manifest case. Rust detail: user-reachable through the ordinary DSL: `activity_map_manifest(std::iter::empty())` → `encode_activity_map_input_manifest_with_codec` (`src/history.rs:633`) has no rejection and yields `item_count: 0`; `ActivityMapSpawnFuture::poll_init` (`src/runtime.rs:2386`) checks only for a *missing* manifest, never an empty one. Reviewer probe on memory, SQLite, and Postgres: commit succeeds (`Ok(Committed)`), descriptor inserted, nothing materialized, no terminal fact ever appended, parent never woken — `result_manifest()` blocks forever. Not a hang or an error; a silent permanent stall. Missing: the fix plus a per-provider regression test for both map kinds. |
| Incomplete | Work | 6H: Postgres inline child start under-counts `in_flight` on an id conflict | **Most serious of the six.** Reachable through the public `Client::start_workflow` API by starting a workflow whose id collides with a map child's generated id. `src/postgres.rs:7252` advances `next_ordinal` before the match; `InlineChildStartOutcome::Failed` takes no slot (`:7258`); `complete_child_workflow_map_item_tx` then releases one (`:7371`). Reviewer probe read the tables directly: descriptor `in_flight=1` with two children genuinely running. Permanent, and compounds per conflicting ordinal, so real concurrency exceeds `max_in_flight` by the conflict count. Missing: fix plus regression test. |
| Incomplete | Work | 6I: memory provider mutates before its terminal-parent check, two sites | `src/memory.rs:2364-2368` (`complete_map_item`) and `src/memory.rs:2436-2441` (`fail_map_item`) both set `completed`, write the result, and decrement `in_flight` before the `run.terminal` check returns `Err(Error::TerminalWorkflow)`. Memory has no transaction, so the writes stick; SQLite (`:4897`) and Postgres (`:7850`) reach the same error and roll back. **Reachability unproven** — memory holds one global lock, so `run.terminal` implies cleanup already ran, and all three providers answer `AlreadyCompleted` on the ordinary route. Record as latent; fix is cheap. |
| Incomplete | Work | 6J: memory activity-map `max_in_flight` is read without `.max(1)` | `src/memory.rs:1500`. `max_in_flight = 0` stalls memory forever while SQLite (`:4356`) and Postgres (`:7073`) admit one; memory's own child-map path clamps at `:2047`. Not reachable from the DSL (`src/runtime.rs:2316` and `:2413` both clamp) but reachable from any direct `commit_workflow_task` with a hand-built `ActivityMapTask` — which the conformance suite itself does. |
| Incomplete | Work | 6K: an abandoned child of a closed parent's map can **never terminate** on SQLite and Postgres | Originally recorded as a hygiene issue — memory's `let _ =` at `src/memory.rs:1895` swallowing routing errors where SQLite (`:3798`) and Postgres (`:6011`) propagate. It is a stuck-workflow bug. Under `ParentClosePolicy::Abandon` the lease-fencing barrier does not apply, and the three providers diverge on a child's terminal commit against a closed parent: memory returns `Ok(Committed)` and the child's history reaches `WorkflowCompleted`; **SQLite and Postgres both return `Err(Backend("child workflow map \`run-1\`:1 not found"))` and the child's history stops at `WorkflowStarted`** — every retry of that terminal commit hits the same missing descriptor, so the child is permanently wedged. Memory survives only *because* of the `let _ =` this row was written about. Entirely pre-existing and byte-identical before and after 6B. The fix needs a signature change plus every caller, and should be taken together with row 6M. |
| Incomplete | Work | 6M: the missing-record convention is applied to one path and not its sibling | 6B converged `fail_map_item`'s missing-descriptor case to `AlreadyCompleted` on all three providers (`memory.rs:2675`, `sqlite.rs:5166`, `postgres.rs:8102`), correctly citing the convention `complete_activity`/`fail_activity` already use. But `complete_child_workflow_map_item`'s missing-descriptor case is still a hard `Err(Backend("… not found"))` — identical situation, opposite treatment, in the same change — and that is the direct cause of 6K's wedged child. Converge both or record why the child-terminal routing path differs; **no conformance test covers either today**. |
| Incomplete | Work | 6L: `postgres.rs:7257` `Skipped` strands an ordinal | Advances `next_ordinal`, takes no slot, records no outcome, so `outcome_count` can never reach `item_count`. Requires a concurrent delete between two statements of the same transaction (`src/postgres.rs:6946`) — effectively unreachable, but unguarded, and the failure mode is an unrecoverable hang. |
| Partial | Test | Pin the two persisted map failure strings before changing them | Rust half **done**: `tests/provider_conformance.rs` +187/-0 adds per-provider expectation tables and three tests driving a real fail-fast child map through a cancelled item, asserting both `ChildWorkflowMapFailed.failure.message` and the sibling's `WorkflowCancelled.reason`. Reviewer independently re-ran three provider mutations; the middle one (reason mutated, message untouched) confirms the reason half is genuinely not masked by the message assertion. Conformance 47 → 50. Missing: the TypeScript half — TS is a **fourth** variant on both strings and nothing pins it. |
| Complete | Decision | D9 (complete empty maps at descriptor creation) is reverted | Proposed by 6A, rejected on review. Two reasons. (a) Out of scope — Phase 6 states "no change to map semantics," and the gate cannot detect the change. (b) It silently carried a second change: reviewer probe H showed a single commit that both schedules a map and closes the run is accepted today by all three providers, but under D5+D9 `DescriptorCreated { parent_terminal: true, item_count: 0 }` routes to `AbortTerminalParent`, rolling back the **whole workflow-task commit**. Non-empty maps were unaffected, so the rollback was purely a D9 side effect. Fix moves to row 6G. |
| Complete | Decision | D3/D4 adopt the SQLite/Postgres persisted-string forms | Replay-safe: fingerprints (`src/history.rs:523-610`) cover command inputs only, and `take_map_failure` (`src/runtime.rs:944`) matches on `command_id.seq` and never inspects the message, so old histories replay unchanged. Justification is *not* majority-of-three: the SQLite/Postgres forms are strictly more informative, and memory's bare `map_command_id.seq` is ambiguous across runs. TypeScript is a **fourth** variant on both strings and must converge — see Phase 7. |
| Complete | Decision | D1, D2, D5–D8, D10 stand as recorded by 6A | Reviewer spot-checked each on the merits rather than by vote and found no wrong pick. D5's activity/child asymmetry is defensible — an activity map has no per-item durable row, so aborting is the only way not to lose the result, whereas a child map's outcome row is worth keeping. |
| Complete | Decision | Memory's skip-forward admission cursor is dropped as provably dead | Ninth behaviour, missed by 6A's first pass. Memory (`src/memory.rs:2050-2052`) advanced `next_ordinal` past ordinals that already had an outcome; SQLite (`:4452`), Postgres (`:7225`), and the engine's contiguous-cursor assumption do not. Dropped rather than tolerated, because a third copy of a rule that can never fire would *hide* real corruption if a provider ever broke the invariant. `MapState::next_ordinal` now carries the precondition as a documented contract. |
| Complete | Work | `MapEffect::ScheduleItemRetry` carries the start-to-close restart | Now carries `timeout_at_ms` alongside `visible_at_ms`, restarting the clock at the **visibility** instant to match `src/memory.rs:1258-1262`, `src/sqlite.rs:1409-1424`, and `src/postgres.rs:4053-4070`. Cross-checked against `provider_util::activity_timeout_at_ms_from` over a 3×3 grid, because that helper is `#[cfg]`-gated to the SQL providers and `map_engine` compiles unconditionally. |
| Incomplete | Risk | The `next_ordinal` invariant rests on `.insert()` being a *reset* | Found on re-review; the stronger half of the proof, and it is not written down. `src/memory.rs:739` (and `:721`) reset `next_ordinal: 0` **and** `outcomes: BTreeMap::new()` in the same expression, so cursor and outcome set are only ever created together at zero or advanced together under one mutex. A plausible future "idempotency" refactor to `.entry().or_insert()` — keeping outcomes while the cursor stays put — would break it and make the engine silently re-admit completed ordinals. Missing: this sentence in the `next_ordinal` contract doc at `src/map_engine.rs:91-99`. |
| Incomplete | Risk | 6B: `MaterializeItems` on memory's activity map is an overwrite, not an upsert | The module doc's "applying a prefix and crashing is safe" claim overstates it for that provider: a prefix-crash could reset `completed`/`claim` on an already in-flight item. Pre-existing, transactional providers unaffected. Missing: either an idempotent apply in 6B or a narrowed claim in the doc. |

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 6B: Rust providers apply engine effects | All three wired; each keeps only storage primitives and applies an effect list inside its own transaction. **The plan's required cross-provider mutation check passed:** one edit in the *engine* (complete one item early) produced **14 conformance failures across all three providers simultaneously** — memory `Backend("missing result for item 1")`, SQLite and Postgres `PayloadEncode("activity map result page lengths cover 3 items, expected 2")`. Reviewer verified the 27 → 0 decision-site collapse by grep rather than by report: no admission loop, `max_in_flight` comparison, failure-mode check, terminal tally, or persisted string survives in any provider. Conformance 50 → 62. Postgres materialization is now one set-based `insert … select … from unnest(...)` per batch, replacing a per-item INSERT loop (400-item map: 1.65 s → 0.80 s). Production code grew **+450 net code lines** (+654 raw, the difference being 197 lines of effect-to-primitive doc comments): the engine removed the decisions, but each provider still needs its own dialect bridge, and sharing one would require the async-trait reshape Phase 6 rules out by name. |
| Complete | Test | New conformance tests fail on the *old* providers in the predicted per-provider pattern | The strongest artefact in this phase, and reproduced by the reviewer. Run against unmodified providers at `6bfe459`: the zero-bound test fails on **memory only** (6J); the sibling-tombstone test fails on **memory and SQLite only**, with `left: RetryScheduled { next_attempt: 2 }` vs `right: AlreadyCompleted` — the old ordering rescheduled an item onto a map that had already ended; the id-collision test fails on **Postgres only** (6H), with three concurrent children against `max_in_flight = 2`, reproduced through the public `Client::start_workflow` API. Exactly the pattern the drift analysis predicted. |
| Complete | Decision | Row 6I is closed as **absorbed**, not deferred | The engine's no-pseudo-effects contract forces ask-before-writing, so `complete_map_item` and `fail_map_item` now read `parent_terminal`, call `step`, and return `Err(Error::TerminalWorkflow)` with **zero** mutation. Both sites 6I named are gone and the defect is structurally impossible. Deliberately closed without a dedicated test: two independent review rounds failed to construct a reachable terminal-parent path — lease fencing blocks it under `ParentClosePolicy::Cancel` (all three providers return `Err(StaleLease)`), descriptor deletion under `Abandon` — so the test cannot be written. Note the original unreachability rationale named only descriptor deletion; **lease fencing is the barrier that fires under the default policy**, which matters if terminal cleanup is ever changed to retain descriptors. |
| Incomplete | Risk | Postgres batch insert should assert `rows_affected == count` | `postgres.rs:7484` uses `on conflict(activity_id) do nothing`, which is the right defensive choice — it makes materialization genuinely idempotent, better than memory's overwrite. But if the clause ever fired, D7's take-the-slot rule would have taken a slot with no task to release it and the map would stall **silently**. Unreachable inside a transaction, since materialization is once-per-ordinal by the monotonic cursor. One line turns a silent invariant violation into a loud one. |
| Complete | Work | 6C: TS pure engine with effect list | `typescript/packages/core/src/map-engine.ts` + 29 table-driven tests, 11 mutations all killed with sha256-verified restoration. `step()` returns a discriminated `MapTransition` rather than throwing, keeping the no-pseudo-effects property. TypeScript turned out **far less drifted than Rust** — the postgres provider is a near-literal copy of memory, and all genuine three-way drift reduces to one root cause (row 6T1). |

The TypeScript drift analysis found six more defects, none of them analogues of
the Rust six. Tracked here, fixed separately, exactly as 6G–6L are.

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6T1: SQLite loses every map terminal fact produced during `commitWorkflowTask` | **Most serious of the six, and the root cause of nearly all TS map drift.** `commitWorkflowTask` appends to an in-memory `state`, calls `#createActivityMap(state, task)` (`packages/sqlite/src/index.ts:493`), then saves `state` at `:508`. But `#completeActivityMapIfDone` (`:1615`), `#completeChildWorkflowMapIfDone` (`:1949`) and `#failChildWorkflowMap` (`:1982`) each re-read the workflow via `#stateForRun` (`:1187`), push their terminal event onto *that* copy, and save it — which `:508` then overwrites. Two probed triggers: an empty input manifest (terminal fact lost, parent stalls forever) and a fail-fast child map whose ordinal-0 workflow id already exists, reachable via `Client.startWorkflow` exactly like Rust 6H. Probe: memory and postgres append `ChildWorkflowMapFailed`; sqlite ends at `WorkflowStarted,ChildWorkflowMapScheduled` with the map durably `terminal=1` and **no** parent fact. |
| Incomplete | Work | 6T2: no terminal-parent guard and no abandon-on-close in any TS provider | **Largest TS↔Rust gap.** `WorkflowTaskCommit` has no `cancelCommands` field in TypeScript at all. Probed with a 4-item map, `maxInFlight=2`, parent cancelled mid-fanout: history continues **past** the terminal event — `WorkflowStarted,ActivityMapScheduled,WorkflowCancelled,ActivityMapCompleted` on memory, sqlite *and* postgres — which the terminal-commit guard at `packages/core/src/backend.ts:533` exists specifically to prevent; and completing an item on a closed run **materializes a new one** on all three, so a closed workflow keeps spawning tasks until the map drains. Rust rejects (activity) or drops the notification (child) and abandons pending items. |
| Incomplete | Work | 6T3: empty input manifest is user-reachable and unvalidated in TS | `activityMapManifest([])` (`packages/core/src/api.ts:574`) yields `itemCount: 0` with no rejection; `runtime.ts:2207`/`:2253` never check. Same root cause as Rust 6G with the **opposite** majority — see 6G. |
| Incomplete | Work | 6T4: no out-of-bounds ordinal guard in any TS provider | `map.results[ordinal] = result` (`backend.ts:1063`, `sqlite:1574`, `postgres:4224`) silently extends the array, and the `.some(r => r === null)` completion predicate skips holes — so an out-of-range ordinal completes a map with `undefined` entries. Latent today (ordinals come from materialized items). |
| Incomplete | Work | 6T5: TS map items are exempt from start-to-close and heartbeat timeouts entirely | `activityTimeoutDeadline` returns `+Infinity` whenever `task.mapItem !== null` (`backend.ts:1577`, `sqlite:2157`, `postgres:5071`), yet `ActivityMapTask` carries `startToCloseTimeoutMs`/`heartbeatTimeoutMs` (`history.ts:152`) and every materialized item copies them (`backend.ts:1034`). Stored and never enforced: a hung map item is recovered only by lease expiry. Rust's map items *are* covered by the timeout scanner. This is why the engine's `timeoutAtMs` has no TS consumer yet. |
| Incomplete | Work | 6T6: TS SQL providers store whole input and result arrays in one descriptor row | Straight `SPEC.md` §6.4/§6.5 violation — "providers must not load all inputs or results into workflow memory or a single durable row". `activity_maps.inputs`/`.results` and `child_workflow_maps.inputs`/`.outcomes` (`sqlite:1009-1053`, written at `:1470`); `normalizedActivityMapRow` (`postgres:5337`). Plus `sqlite:1489` `#replaceActivityMapItemRows` deletes and re-inserts **all N** item rows on every single item mutation — O(itemCount) writes per completion on a hot path. |
| Complete | Decision | TS rejects `maxInFlight <= 0`; Rust clamps. **Rust moves.** | Rejecting at the scheduling boundary beats silently clamping to 1, which converts a caller typo into a 10,000× throughput loss discovered in production. TS rejects at four places — both DSL entry points (`api.ts:709`, `runtime.ts:2236`) and all three providers. The TS engine does both non-contradictorily: `mapSlotLimit()` clamps so an already-existing degenerate descriptor cannot stall (covering 6J), while provider validation runs before the engine is consulted and 6D must keep it. Rust should gain the DSL rejection rather than TS dropping it. |
| Incomplete | Decision | Retry delay values are a **forced** cross-language divergence | `RetryBackoff::{None, Exponential}` with a fixed base (Rust) and `initialIntervalMs`/`maxIntervalMs`/`backoffCoefficient` (TS) are different policy models; numeric parity is impossible. Only the shape is normative: `visibleAtMs === null` ⟺ immediately claimable, and `TimedOut` ⇒ always `null`. **Phase 7's corpus must not assert `ScheduleItemRetry.visibleAtMs` values.** TS exports `itemRetryDelayMs`, pinned against `MemoryBackend`'s private `retryDelayMs` through the public API, so 6D can re-point `backend.ts:1550` at it and delete the duplicate. |
| Incomplete | Risk | 6G must carry an explicit "ignore `parentTerminal` at `DescriptorCreated`" carve-out | Rust's `DescriptorCreated` never consults `parentTerminal` today (it only materializes). When 6G converges Rust to completing empty maps, routing that completion through the normal terminal path would land the probe-H rollback on the Rust side — turning a commit that both schedules a map and closes its run from accepted into a full rollback. The TS engine already carries this carve-out and pins it with a mutation (M10). |
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
