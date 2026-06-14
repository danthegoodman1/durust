---
id: 0008
title: Deterministic simulation hardening
status: complete
depends_on: [0001, 0002, 0003, 0005]
labels: [simulation, faults, shards, perf]
---

# Deterministic Simulation Hardening

Expand the deterministic simulator into the release gate for recovery,
concurrency, providers, leases, and cross-shard handoff.

## Scope

- Fault injection.
- Trace logging.
- Invariant checker.
- Many-seed CI profile.
- Aggressive crash/reorder/delay profiles.
- Shard lease simulation.
- Cross-shard outbox/inbox simulation.

## Acceptance

- Thousands of seeds pass under aggressive fault profiles.
- Failing seed prints reproducible trace.
- New concurrency and provider features add simulation scenarios before release.
- Cross-shard handoff survives duplicate, delayed, and crashed dispatchers.

## Current State

Implemented and covered:

- Public deterministic simulation harness in `src/sim.rs`:
  - `SimRun` for virtual time, seeded scheduling, trace logging, fault
    injection, and invariant checks.
  - `FaultProfile::{None, Moderate, Aggressive}` and named `FaultPoint`s for
    worker crashes, cache eviction, commit conflicts, duplicate activity
    completion, duplicate timer fire, signal storms, blob-store transient
    errors, shard lease loss, cross-shard duplicate/delayed delivery, and
    dispatcher crash points.
  - `SimTrace` and `SimFailure` so failures print the failing seed plus a
    reproducible step trace.
  - `run_many_seeds` for CI-friendly seeded profile runs.
- Existing scheduler and virtual-clock determinism tests remain covered.
- Simulation failure formatting test proves seed, invariant, message, and trace
  are present in failures.
- A many-seed aggressive profile test runs 2,048 seeds and covers:
  - worker crash storm,
  - cache eviction storm,
  - commit conflict storm,
  - activity duplicate completion,
  - timer duplicate fire,
  - signal storm,
  - blob-store transient errors,
  - shard lease loss,
  - cross-shard duplicate, delayed, and reordered delivery,
  - dispatcher crash at source outbox read, target inbox write, target apply,
    and source ack points.
- Model invariants checked by the many-seed test:
  - no duplicate workflow command commit,
  - all workflow commands eventually commit,
  - external activity/timer/signal facts are idempotent,
  - no committed payload ref exists without uploaded bytes,
  - stale shard lease owners are rejected,
  - target inbox application is idempotent,
  - target apply happens exactly once per cross-shard message,
  - source acks eventually complete.
- CI already builds benchmark targets with `cargo bench --locked --no-run`.
  Stable Criterion benchmark names cover warm cached workflow path, recovery,
  activity claim/complete, signal send/consume, timer wakeup, child fanout,
  activity-map fanout/materialization/completion, payload refs/replay/codecs,
  and SQLite single-file mixed-workflow throughput.
- Benchmark-profile test coverage proves those phase-required stable names
  remain present in `benches/replay_core.rs`; phase 0012 owns stricter timing
  gates.

Remaining follow-up outside this phase:

- Phase 0009 adds production recovery flow-control policy and provider
  backpressure behavior.
- Phase 0012 turns advisory benchmark metadata into stronger performance
  hardening and regression gates.

## Required Tests

- Worker crash storm.
- Cache eviction storm.
- Commit conflict storm.
- Activity duplicate completion.
- Timer duplicate fire.
- Signal storm.
- Blob store transient errors.
- Shard lease loss.
- Cross-shard duplicate, delayed, and reordered outbox delivery.
- Dispatcher crash at source outbox, target inbox, target apply, and source ack points.

## Performance Gate

- Criterion benchmark suite runs in CI or a documented performance job with stable names and regression history.
- Benchmarks cover warm cached workflow path, recovery, activity claim/complete, signal send/consume, timer wakeup, child fanout, activity map fanout, payload refs, and SQLite provider baseline.

## Public API Budget

- `SimRun`, `FaultProfile`, `FaultPoint`, `DispatcherCrashPoint`, `SimTrace`,
  `SimFailure`, and `run_many_seeds` are first-class test primitives because
  future concurrency, provider, recovery, and shard work needs a shared
  deterministic fault model. The API deliberately stays generic: it knows about
  fault points and invariant traces, not workflow semantics or provider storage
  details.
