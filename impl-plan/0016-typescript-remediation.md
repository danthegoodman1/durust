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
| Incomplete | Work | 1A: GC minAgeMs grace period + ref-based marking | Missing: option, store last-modified, sweep cutoff, race test. |
| Incomplete | Work | 1B: heartbeat extends claim lease + attempt bump on reclaim | Missing: three backend heartbeat paths + conformance. |
| Incomplete | Work | 1C: terminal leftover-command divergence check | Missing: runtime check + regression test. |
| Incomplete | Gate | npm run check green, mutation checks recorded | Missing: run evidence. |

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
| Incomplete | Work | 2A: backend release primitive + worker error-path release | Missing: contract addition + worker wiring + fault tests. |
| Incomplete | Work | 2B: foreign-scheme hydration pass-through | Missing: owns() check + custom-scheme test. |
| Incomplete | Work | 2C: terminal-commit guard (shared predicate, three backends) | Missing: guard + table-driven conformance. |
| Incomplete | Gate | npm run check green on all fixes | Missing: run evidence. |

## Deferred (recorded, not scheduled)

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | Terminal operational-row cleanup (rows flagged forever; SQLite rewrites full history JSON per commit) | Growth/perf; mirror Rust Phase 6E when scheduled. |
| Incomplete | Work | Concurrent activity execution + push wakeups in Worker.run() | Latency/throughput; mirror Rust Phase 7. |
| Incomplete | Work | Parent-close cancel scans all open workflows with per-row history rehydration | O(open workflows) per terminal commit; needs an index. |
| Incomplete | Work | GC marking downloads every reachable blob | Cost only; mirror Rust ref-based marking if not folded into 1A. |
