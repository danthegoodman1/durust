# 0016: TypeScript Remediation

## Overarching Goal

Close the correctness gaps the Rust remediation (item 0015) surfaced that also
exist in the TypeScript port. An audit of all seventeen Rust fix classes found
the TS implementation already correct by construction for most (replay ready
event pre-filtering, claim lease expiry with fencing, Postgres commit-time
signal recheck, virtual-time test backend, bounded history streaming, policy
driven retry backoff, structural select digests). This item fixes what
remains. Non-goal: porting Rust-only mechanisms the TS design does not need
(child outbox dedup, provider feature gates).

## Implementation Principles

- Same correctness bar as 0015: every fix lands with a mutation-checked
  regression test in the same change.
- Keep TS/Rust behavioral parity where the SPEC defines the contract; where
  the TS design is already stronger (e.g. policy-driven backoff), keep it and
  note the divergence.
- The shared conformance suite in `packages/testing` is the cross-backend
  gate: memory, SQLite, and Postgres (env-gated) run every backend behavior
  change.

## Testing Strategy

- `cd typescript && npm run check` (build, vitest, type tests, determinism
  lint, package dry-run) is the base gate.
- Postgres conformance via its env-gated suite when a fixture is available.
- Deterministic tests only: virtual clocks, no wall-clock sleeps in asserts.

## Phase 1: Correctness-critical

Goal:
No production configuration can lose payload data, duplicate long-running
activity work, or silently commit terminal events over diverged history.

Scope:
- GC grace period: `CollectPayloadGarbageOptions` gains `minAgeMs` (default 1
  hour); blob stores expose last-modified; the sweep never deletes blobs
  younger than the cutoff, closing the upload-before-commit race
  (`packages/payload/src/index.ts:603-643`; offload at `:427` precedes
  commit). Mark reachability from refs without downloading every blob while
  in the file.
- Heartbeat renews the activity claim lease: `heartbeatActivity` extends
  `expiresAtMs` (memory `backend.ts:736-741`, sqlite `:677-686`, postgres
  `:1726-1731`) so a live heartbeating holder survives indefinitely; a
  reclaim after heartbeats stop bumps the attempt.
- Terminal-with-leftover-command-events check: `completeWorkflow`
  (`packages/core/src/runtime.ts:252-269`, `:1807-1821`) fails the task as
  nondeterminism when recorded command events remain un-replayed at any
  terminal outcome; unconsumed ready events stay legal.

Completion gate:
All three fixes mutation-checked; `npm run check` green; Postgres conformance
green when configured.

Testing plan:
- GC: in-flight upload survives a mutating sweep under default `minAgeMs`;
  `minAgeMs: 0` reproduces the pre-fix delete; committed blobs always
  retained.
- Heartbeat: heartbeating timeout-less activity survives multiple lease
  periods on all three backends; stopping heartbeats reclaims one lease later
  as the next attempt with the old holder fenced.
- Divergence: two-timer history replayed against a one-timer workflow fails
  with nondeterminism and appends no terminal event.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: GC minAgeMs grace period + ref-based marking | `packages/payload/src/index.ts` exports `DEFAULT_PAYLOAD_GC_MIN_AGE_MS`, adds optional `lastModifiedMs`, retains young/unknown-age blobs, re-probes before delete, and hydrates only container blobs during deep marking on memory/SQLite (Postgres `payloadRoots` returns bare refs that still download under the leaf-key allowlist — safe over-marking, cost only). Tests: `retains in-flight uploads under the default GC grace period`, `allows minAgeMs zero to collect an otherwise in-flight upload`, `keeps committed blobs regardless of age`, `refreshes content-addressed local blob age on deduplicated re-put`. Mutation check: forcing GC age eligibility to `true` made the in-flight upload test fail with the blob deleted. |
| Complete | Work | 1B: heartbeat extends claim lease + attempt bump on reclaim | Memory, SQLite, and Postgres activity claims persist `leaseDurationMs`; heartbeat refreshes both heartbeat deadline and `expiresAtMs`; expired lease retry paths bump attempts when retry policy allows. Shared conformance: `timeout-less activity heartbeat extends lease and expired reclaim bumps attempt` runs through memory, SQLite, Postgres, and blob-backed variants. Mutation checks: keeping `expiresAtMs` unchanged made the focused memory conformance test fail with a stale heartbeat; disabling timeout retry attempt increment made the same test fail on `expired lease reclaim should bump attempt`. |
| Complete | Work | 1C: terminal leftover-command divergence check | `packages/core/src/runtime.ts` checks terminal completed/failed/continued outcomes for unconsumed replay command events; ready events remain pre-filtered and legal, and worker replay still streams through the claimed target before runtime construction. The `failWorkflow` path (invoked in the constructor's `.catch`) routes assertion errors to `#fatalError` + hot-progress notification so divergence surfaces as clean nondeterminism instead of an unhandled-rejection hang. Regressions: `rejects terminal completion with leftover recorded timer commands`, `rejects terminal app failure with leftover recorded timer commands without hanging`, `rejects continue-as-new with leftover recorded timer commands` — all assert no terminal event lands. Mutation checks: bypassing the terminal assertion prepared `WorkflowCompleted`; removing the `.catch` guard reproduced the reviewer's hang (`Test timed out` + unhandled rejection). |
| Complete | Work | 1D: review-round fixes | SQLite expired-lease reclaim honors retry backoff (recheck `availableAtMs > now`, persist the restored row); shared conformance `expired timeout-less activity lease honors nonzero retry backoff before reclaim` pins the pacing on all backends (mutation check: disabling the skip failed both SQLite variants). S3 `lastModifiedMs` maps 404 to null (unknown age retained) instead of aborting the sweep. `#replayHistoryComplete` fails closed on empty prefetch; dead snake_case GC allowlist entries removed after grep verification. |
| Complete | Gate | npm run check green, mutation checks recorded | Final gates: `cd typescript && npm run check` passed with 28 test files passed / 1 skipped, 379 tests passed / 92 skipped, type tests, determinism lint, and package dry-run; `DURUST_POSTGRES_URL=postgres://durable:durable@127.0.0.1:55432/durable npm run check:postgres` passed Postgres conformance (89 tests, including the new shared cases) plus benchmark thresholds including Postgres smoke (14 tests). Reviewer-approved after one round (blocker: failWorkflow divergence hang; should-fix: SQLite backoff violation — both fixed and mutation-checked). |

## Phase 2: Bounded correctness and contract hardening

Goal:
Worker errors do not stall runs for a full lease; foreign-scheme payload refs
pass through opaquely; terminal runs reject mutating commits explicitly.

Scope:
- Claim release on worker error paths: add a release primitive to
  `DurableBackend` (`packages/core/src/backend.ts:40-73` has none) and
  release claims when prepare/registry/stream errors escape
  (`packages/core/src/worker.ts:405-440`).
- Foreign-scheme opacity: `hydratePayloadRef`
  (`packages/payload/src/index.ts:348-366`) passes refs whose URI the store
  does not own through untouched instead of throwing, matching GC traversal
  (`:828`) and the Rust scheme-ownership contract.
- Terminal-commit guard: shared predicate rejecting workflow-visible
  mutations against terminal runs in all three backends, with a conformance
  case per mutation kind (0014 line 499 promised this; none exists).

Completion gate:
`npm run check` green; new conformance cases pass on memory, SQLite, and
Postgres when configured.

Testing plan:
- Worker error paths release claims (fault-injecting backend wrapper).
- Custom-scheme store round trip through hydration.
- Table-driven terminal-guard conformance.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: backend release primitive + worker error-path release | `DurableBackend.releaseWorkflowTask(claim, { visibilityDelayMs })` is implemented by memory, SQLite, Postgres, and payload/benchmark wrappers. Releases are fenced by run id + worker/token, restore the held wake reason, and use `readyAtMs`/`ready_at_ms` for delayed visibility. Worker audit/fixes: workflow registry miss, live-signal read failure, hot/cold replay history streaming failure, runtime prepare/decode/nondeterminism failure, and commit RPC failure all release the workflow claim; nondeterminism and unsupported-version failures use `nondeterminismRetryBackoffMs` (default 60s), other failures release immediately. Activity audit: registered handler/decode/output errors already call `failActivity`; unregistered single, native-batch, and sequential-batch activity claims now use the same fail path instead of dropping the claim. Tests: shared conformance `released workflow task claims are immediately reclaimable and stale releases are no-ops`; worker regressions `releases workflow claims when workflow registry lookup fails`, `releases workflow claims when live signal reads fail`, `releases workflow claims when replay history streaming fails`, `releases workflow claims when commit fails`, `delays released workflow claims after nondeterminism errors`, and `fails unregistered activity claims instead of dropping them until lease expiry`. Mutation checks: removing worker release wiring makes the focused release tests see `null` until lease expiry; removing stale-token fencing lets the stale release steal the newer claim; removing delay gating makes the nondeterminism and conformance delayed-release assertions fail. Deferred interaction: this touches Postgres workflow claim readiness only; the deferred Postgres activity claim-path reclaim liveness and exhausted-attempts reclaim rows remain unchanged. |
| Complete | Work | 2B: foreign-scheme hydration pass-through | `hydratePayloadRef` returns blob refs whose URI the configured store does not own, and deep `PayloadBackend` hydration stops recursion at those pass-through refs. Owned refs still hydrate and validate bytes; decode of an unowned blob still fails with `blob payload must be hydrated before decode`. Tests: `passes foreign-scheme refs through hydration unchanged`, `round-trips foreign-scheme refs through deep PayloadBackend hydration`, and `leaves foreign-scheme refs out of wrapper-owned GC reachability`. Mutation check: removing the `owns()` pass-through reproduces the local-directory unsupported/foreign URI throw during hydration. |
| Complete | Work | 2C: terminal-commit guard (shared predicate, three backends) | `workflowTaskCommitHasWorkflowVisibleMutations` lives in `packages/core/src/backend.ts` and enumerates `appendEvents`, wait upserts/deletes, signal consumption, activity/activity-map scheduling, child start/child-map scheduling, and query projection; empty commits remain non-mutating. Memory, SQLite, and both Postgres commit paths reject workflow-visible mutations after lease fencing when a terminal run still has a forged valid claim. Shared conformance pins public behavior with `terminal workflow task commits are fenced through the public API`; provider forged-state tests `SqliteBackend terminal commit guard` and `PostgresBackend terminal commit guard` reject every shared catalog mutation and accept an empty no-op. Mutation checks: deleting the guard allows forged terminal mutation commits to append/update state; narrowing the predicate makes the shared catalog assertion fail for the omitted mutation kind; moving the guard before lease fencing changes the public stale-lease conformance behavior. |
| Complete | Work | 2D: batch drain on error | `runWorkflowTaskBatchOnce` drains every claimed task (per-task try/catch), collecting the first error and rethrowing only after the whole batch is committed, conflicted, or released, so one task's failure cannot strand batch neighbors until lease expiry (mirrors the Rust batch contract). Regression: `drains a claimed workflow batch when an earlier task fails` (batch of 3, task 1 fails registry lookup; neighbors complete in the same call, error surfaces after the drain, failed run immediately reclaimable with its wake reason). Mutation check: restoring rethrow-in-loop strands the neighbors and fails the test. |
| Complete | Gate | npm run check green on all fixes | `cd typescript && npm run check` passed: build, 28 test files passed / 1 skipped, 399 tests passed / 98 skipped, type tests, determinism lint, and package dry-run. `DURUST_POSTGRES_URL=postgres://durable:durable@127.0.0.1:55432/durable npm run check:postgres` passed Postgres provider conformance (95 tests) and benchmark thresholds including Postgres smoke (14 tests). Reviewer-approved after one round (blocker: batch neighbors stranded on error — fixed and mutation-checked). |

## Deferred (recorded, not scheduled)

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | Terminal operational-row cleanup (rows flagged forever; SQLite rewrites full history JSON per commit) | Growth/perf; mirror Rust Phase 6E when scheduled. |
| Incomplete | Work | Concurrent activity execution + push wakeups in Worker.run() | Latency/throughput; mirror Rust Phase 7. |
| Incomplete | Work | Parent-close cancel scans all open workflows with per-row history rehydration | O(open workflows) per terminal commit; needs an index. |
| Incomplete | Work | Postgres claim-path lease reclaim never persists the retry and head-of-line blocks | `claimActivityTask` recomputes `readyAtMs = now + delay` per poll so nonzero backoff never elapses through the claim path (the `timeoutDueActivities` sweep rescues it), and the `limit 1` select lets one backoff-pending expired row block claimable siblings. Liveness, not correctness. Needs: persist the restored retry state + skip-and-backfill in the select. |
| Incomplete | Work | Exhausted-attempts lease reclaim can exceed maxAttempts | When retry-after-timeout returns null at max attempts, all three backends leave the expired row claimable at the same attempt; an aggressive claimer re-stamps the implicit heartbeat deadline and starves the sweep's terminal `ActivityTimedOut`, duplicating work past `maxAttempts`. Rust reclaims only through the timeout scan (terminal-fails on exhaustion). Pre-existing, not a Phase 1 regression. Needs: scan-only reclaim or terminal-fail in the claim path + conformance case. |
| Incomplete | Doc | Minor Phase 1 follow-ups | Conformance clock control stubs global `Date.now` instead of injecting backend `nowMs` (factory signature does not allow injection); TS reports implicit-lease expiry as "missed heartbeat" where Rust distinguishes a lease-expired attribution; GC traversal infers container-vs-leaf from holding key names (typed root wrappers would be sturdier; `seenPayloads` dedup is order-sensitive but no reachable ordering today). Opportunistic. |
