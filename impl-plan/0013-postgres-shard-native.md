---
id: 0013
title: Postgres shard-native scaling
status: in_progress
depends_on: [0011, 0012]
labels: [postgres, shards, leases, batching, conformance, benchmarks]
---

# Postgres Shard-Native Scaling

Extend the single public `PostgresBackend` with a shard-native execution layout
that scales by logical shard ownership, concurrent activation execution, and
batched shard-local commits. The current normalized layout remains the
conservative correctness and conformance baseline inside the same provider while
the shard-native layout proves itself.

No implementation or documentation in this phase may depend on checked-in
external reference repositories. Any external benchmark or implementation can be
used only as temporary measurement context.

## Public API Budget

Existing primitive composition would require applications to run many workers,
manually route workflow IDs to providers, maintain shard ownership, prefetch
and buffer workflow tasks out of band, and hand-roll commit coalescing. That
composition is not merely inconvenient: it can break workflow identity routing,
lose shard lease fencing, create nondeterministic polling order, overload
Postgres during recovery, and report misleading benchmark dimensions where
`batch` and `prefetch` do not reduce durable round trips.

Shard-native Postgres earns first-class configuration on `PostgresBackend`
because it protects these scaling invariants:

- workflow identity routes to one logical shard by `hash(namespace,
  workflow_id) % logical_shards`;
- shard count, physical partition count, pool sizing, schema, and timing knobs
  are stored in provider metadata and validated on startup;
- workers own shard leases before claiming or committing shard-local workflow
  tasks;
- one normal workflow-task commit touches one logical shard and appends one
  fenced shard-journal operation per batch;
- history remains append-only and streamable per run;
- snapshot plus journal-tail recovery reconstructs all operational state;
- cross-shard work is correct first and benchmarked separately from shard-local
  throughput.

The worker knobs also earn public API surface because existing user code cannot
compose them without either unbounded in-memory queues or extra durable reads:

- `max_concurrent_workflow_tasks` bounds active workflow activations per worker;
- `workflow_task_prefetch_limit` bounds claimed-but-not-yet-polled workflow
  tasks;
- `workflow_task_commit_batch_size` bounds one batch commit operation;
- `workflow_task_commit_max_delay` bounds write-combining latency;
- an optional shard ownership filter makes deployments and tests deliberately
  target only selected logical shards.

The backend contract change is generic: providers may implement batch workflow
claim and batch workflow commit, while memory, SQLite, and the normalized
Postgres layout retain default one-at-a-time implementations. Runtime semantics
do not mention Postgres tables, partitions, or shard journals.

## Scope

- `PostgresBackendConfig` shard-native fields and validation.
- Metadata validation for logical shards, physical partitions, max pool size,
  snapshot interval, schema, statement timeout, and lock timeout.
- Deterministic shard key `hash(namespace, workflow_id) % logical_shards` for
  v1 routing.
- Shard lease acquisition and heartbeat with stale-owner commit rejection.
- Shard-local in-memory projection loaded from snapshot plus journal tail.
- Batch workflow task claim and batch workflow task commit with per-item
  committed, conflict, or stale result.
- Append-only per-run history in physical partitions.
- Workflow start, signal, timer, activity completion, child lifecycle, query
  projection, version marker, payload root/GC, delayed visibility, and recovery
  behavior against shard projections.
- Benchmark dimensions for shards, activation concurrency, prefetch, batch
  size, physical partitions, pool size, and provider `postgres`.

## Storage Shape

- `meta` keys for schema version, shard layout, pool size, snapshot interval,
  and provider timeout settings
- `workflow_instances.shard_id`
- `shard_leases`
- `shard_heads_pNN`
- `shard_journal_pNN`
- `shard_snapshots_pNN`
- `history_events_pNN`
- partitioned activity lease/task tables as needed for remote activity workers

Logical shards are the scheduling and fencing unit. Physical partitions are a
storage layout detail.

## Implementation Slices

1. Update spec, plan, worker builder knobs, and generic backend batch methods
   while preserving all existing provider behavior.
2. Add `PostgresBackendConfig` metadata validation, shard hashing, shard lease
   tables, and unit tests.
3. Implement projection load from empty state, snapshot, and journal tail.
4. Implement shard-local workflow start, claim, release, stream history, and
   batch commit for the core workflow-task path.
5. Port external append paths into shard projections: signals, timers,
   activities, child lifecycle, activity maps, query projections, version
   markers, delayed visibility, payload roots, and GC.
6. Register the provider in conformance and add restart tests proving snapshot
   plus journal tail reconstructs operational state.
7. Add deterministic fault simulation for lease loss, duplicate/delayed
   commits, stale workers, worker crash, batched conflicts, timer/signal/
   activity/child ordering, cross-shard delivery, and lease transfer.
8. Add benchmark support for multi-shard `postgres` runs and accepted
   baselines.

## Benchmark Gate

The shard-native Postgres layout must exceed the normalized Postgres layout on
the same machine before it can replace it for scale-out recommendations.

Accepted baselines:

- 1 shard / 4 workers;
- 10 workers / 100 logical shards / 16 physical partitions;
- scaling sweep over shards, workers, partitions, pool size, prefetch,
  activation concurrency, and batch size until local CPU, WAL, or IO
  saturation.

Each accepted result records workflows/sec, mixed actions/sec, activations/sec,
p50/p95/p99 commit latency, WAL bytes/sec, pool wait, active connections, CPU,
and correctness counters.

## Required Tests

- Unit tests for shard hashing, metadata mismatch, shard lease fencing, journal
  CAS conflicts, snapshot restore, journal catch-up, and batch result ordering.
- Full provider conformance for memory, SQLite, and Postgres in both normalized
  and shard-native layouts.
- Replay/core tests for cached and cold replay with unfavorable ordering across
  signals, timers, child completions, activity completions, and map completions.
- Fault and simulation tests for stale shard owner commit rejection, duplicate
  batch commit behavior, crash after append before snapshot, and shard lease
  transfer preserving ready work.
- Benchmarks proving the current normalized Postgres baseline does not regress
  and the shard-native Postgres layout meets the accepted profiles above.
