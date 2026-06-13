---
id: 0008
title: Deterministic simulation hardening
status: not_started
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
