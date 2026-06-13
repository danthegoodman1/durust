---
id: 0010
title: SQLite shard-file provider
status: not_started
depends_on: [0001, 0005, 0008, 0009]
labels: [sqlite, provider, shards, outbox, conformance, benchmarks]
---

# SQLite Shard-File Provider

Add a partitioned SQLite provider layout that maps each logical shard to its
own WAL/FULL SQLite append store. This provider exists to prove local
scale-out behavior, exercise shard-local provider invariants, and simulate
cross-shard outbox/inbox handoff without relying on cross-file transactions.

The single-file SQLite provider remains the default correctness and local
development backend. The shard-file provider is a separate conformance,
simulation, and performance target.

## Scope

- `sqlite-shard-files` provider constructor/configuration.
- Provider directory management.
- Fixed `shard_count` startup validation.
- One SQLite database file per logical shard.
- Same append-journal schema and replay semantics as single-file SQLite.
- Deterministic shard routing by namespace, workflow_id, and run_id.
- Shard-local workflow task commits.
- Shard-local signal inbox, waits, activity state, child outbox, and query projection.
- Cross-shard child start, child completion, cancellation, signal routing, and activity map completion through outbox/inbox handoff.
- Dispatcher recovery across source outbox, target inbox, target apply, and source ack boundaries.
- Provider conformance registration as `sqlite-shard-files`.
- Criterion benchmarks comparing single-file SQLite and shard-file SQLite.

## Acceptance

- Single-file SQLite remains supported and remains the default SQLite backend.
- Shard-file SQLite creates and uses one WAL/FULL SQLite file per logical shard.
- A normal `commit_workflow_task` touches only the owner shard file.
- No workflow task commit requires a cross-file SQLite transaction.
- Cross-shard operations commit source-shard state and outbox atomically, then deliver to target inbox idempotently.
- Duplicate, delayed, reordered, and retried dispatch does not duplicate child starts, signal delivery, activity map completion, or parent wakeups.
- Closing and reopening the provider reconstructs every shard from durable append history and provider-owned derived state.
- Memory, single-file SQLite, and shard-file SQLite all pass the shared provider conformance suite.
- Benchmarks report comparable dimensions to `../durable-phases`: workflow count, worker count, shard count, activation concurrency, prefetch limit, commit batch size, and throughput.

## Required Tests

- Deterministic shard routing is stable across provider restart.
- Provider creates the expected shard database files.
- Mismatched `shard_count` after initialization fails with a clear error unless an explicit repartitioning tool exists.
- Workflow start, signal send, workflow task claim, and workflow task commit route to the owner shard.
- Stale shard lease owner cannot commit after another owner claims the shard.
- Same-shard child start stays on the fast shard-local path.
- Cross-shard child start uses source outbox and target inbox.
- Cross-shard child completion wakes the parent through outbox/inbox.
- Parent close policy propagates across shards through outbox/inbox.
- Signal accepted on a non-owner shard is delivered to the owner shard idempotently.
- Activity map item completion routed from a non-owner shard reaches the owner shard idempotently.
- Dispatcher crash after source outbox commit recovers.
- Dispatcher crash after target inbox write recovers.
- Dispatcher crash after target apply recovers without duplicating the target mutation.
- Dispatcher crash before source ack recovers without losing or duplicating delivery.
- SQLite close/reopen recovery verifies append history rather than in-memory state.

## Simulation Profiles

- Many-shard worker crash storm.
- Shard lease loss while dispatchers are delivering outbox messages.
- Cross-shard child fanout with duplicate dispatch.
- Cross-shard signal storm with delayed and reordered delivery.
- Activity map completion fan-in from multiple shards.
- Mixed timers, signals, child completions, and outbox dispatch under tiny history chunks.

## Performance Gate

- Criterion benchmark for shard-local workflow task commit.
- Criterion benchmark for child start outbox commit and dispatch latency.
- Criterion benchmark for cross-shard signal routing.
- Criterion benchmark for provider startup replay across many shard files.
- Compare single-file SQLite and shard-file SQLite at 1, 4, and 16 shards.
- Record whether throughput scales with shard count and explain any bottleneck.
