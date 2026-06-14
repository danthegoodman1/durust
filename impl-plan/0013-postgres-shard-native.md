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
- shard count, physical partition count, schema, and timing knobs are stored in
  provider metadata and validated on startup;
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
- `max_concurrent_activities` bounds activity claim batching in worker loops so
  activity-heavy runs do not burn one durable claim round trip per empty worker
  slot;
- an optional shard ownership filter makes deployments and tests deliberately
  target only selected logical shards.

The backend contract change is generic: providers may implement batch workflow
claim, batch workflow commit, and batch activity claim, while memory, SQLite,
and the normalized Postgres layout retain default one-at-a-time
implementations. Runtime semantics do not mention Postgres tables, partitions,
or shard journals.

## Scope

- `PostgresBackendConfig` shard-native fields and validation.
- Metadata validation for logical shards, physical partitions, snapshot
  interval, schema, statement timeout, and lock timeout. Pool size remains a
  local client cap, not a schema compatibility invariant.
- Deterministic shard key `hash(namespace, workflow_id) % logical_shards` for
  v1 routing.
- Shard lease acquisition and heartbeat with stale-owner commit rejection.
- Shard-local in-memory projection loaded from snapshot plus journal tail.
- Batch workflow task claim and batch workflow task commit with per-item
  committed, conflict, or stale result.
- Batch activity task claim with provider-owned lease fencing and a bounded
  worker claim limit.
- Append-only per-run history in physical partitions.
- Workflow start, signal, timer, activity completion, child lifecycle, query
  projection, version marker, payload root/GC, delayed visibility, and recovery
  behavior against shard projections.
- Benchmark dimensions for shards, activation concurrency, prefetch, batch
  size, physical partitions, pool size, and provider `postgres`.

## Storage Shape

- `meta` keys for schema version, shard layout, snapshot interval, and provider
  timeout settings
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

1. **Done:** Update spec, plan, worker builder knobs, and generic backend batch
   methods while preserving all existing provider behavior.
2. **Done:** Add `PostgresBackendConfig` metadata validation, shard hashing,
   shard lease tables, and unit tests.
3. **Done:** Implement runtime prepare-then-commit workflow-task scheduling so
   prefetched workflow tasks can be committed through `commit_workflow_tasks`.
4. **Done:** Implement shard-aware Postgres batch workflow-task claim in one
   transaction, including unfiltered claim lease acquisition.
5. **Done:** Implement shard-fenced Postgres batch workflow-task commit in one
   transaction with ordered per-item committed/conflict/stale results.
6. **Done:** Append one fenced shard-journal operation per shard/lease epoch for
   each workflow-task commit batch.
7. **Done:** Add provider/unit coverage for shard hashing, metadata mismatch,
   shard-filtered claim, unfiltered shard lease acquisition, stale owner commit
   rejection, ordered batch commit results, and shard-journal batching.
8. **Done:** Add benchmark support and checked-in baselines for 1-shard
   Postgres and 100-shard/10-worker Postgres runs.
9. **Remaining:** Implement projection load from empty state, snapshot, and
   journal tail for full operational rebuild from the shard journal.
10. **Remaining:** Port external append paths into shard projections: signals,
    timers, activities, child lifecycle, activity maps, query projections,
    version markers, delayed visibility, payload roots, and GC.
11. **Remaining:** Add deterministic fault simulation for crash after append
    before snapshot, snapshot restore/journal catch-up, lease transfer preserving
    ready work, and unfavorable timer/signal/activity/child ordering under
    batched commits.

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
p50/p95/p99 workflow-task commit latency, WAL bytes/sec, active connections,
and correctness counters. Pool wait and CPU counters remain future benchmark
fields because the current local harness does not expose pool wait or host CPU
sampling.

Measured local baselines:

- 1 shard / 4 workers / 1,000 mixed workflows:
  38.46 processing workflows/sec, 307.68 processing mixed actions/sec, p50/p95/
  p99 workflow-task commit latency 4.94/7.98/8.87ms.
- 100 logical shards / 16 physical partitions / 10 workers / 1,000 mixed
  workflows / prefetch 32 / batch 32:
  89.27 processing workflows/sec, 714.17 processing mixed actions/sec, p50/p95/
  p99 workflow-task commit latency 46.86/65.82/88.11ms. This accepted baseline
  uses sequence-backed run IDs, signal receive sequences, and claim tokens,
  refreshes only selected shards instead of every shard assigned to a worker,
  and uses Postgres batch activity claim.
- Before batch activity claim, an instrumented single-process run of that same
  benchmark reached about 59.45 processing workflows/sec and 475.57 mixed
  actions/sec. Durust-side backend metrics showed underfilled workflow commit
  batches (7,953 workflow-task items across 2,232 commit calls, about 3.56
  items/call), many repeated workflow claim calls (3,078 calls for 7,953 items),
  and unbatched activity claims (3,000 claim calls).
- After batch activity claim, the single-process run reached 71.86 processing
  workflows/sec and 574.85 mixed actions/sec. Backend metrics showed 1,102
  activity claim calls for 3,000 tasks, 1,102 workflow claim calls for 7,917
  tasks, and 1,048 workflow commit calls for 7,917 tasks, or about 7.55
  workflow-task items per commit call. Postgres counters showed about 4.93k
  transactions/sec, 2.23 MiB WAL/sec, near-perfect buffer hit ratio, no temp
  files, no deadlocks, and sampled waits around WAL sync/write plus
  transaction-id/tuple locks. Commit p95/p99 remained poor at about
  147.87/270.10ms because child workflow starts still allocated `run-*` IDs by
  updating the shared `meta(run)` row inside workflow-task commit transactions.
- Moving hot Postgres counters from transactional `meta` rows to native
  sequences fixed the main single-process scaling gap. Run IDs use
  `run_id_seq`, signal inbox ordering uses `signal_seq`, and claim tokens use
  `claim_token_seq`; migration initializes each sequence from existing metadata
  and table state for compatibility. The final 100-shard checked-in baseline
  reached 89.27 processing workflows/sec and commit p95/p99 65.82/88.11ms, with
  sampled `transactionid` waits down to 9.
- Two concurrent copies of the post-batch benchmark against separate schemas
  reached about 102.48 combined processing workflows/sec and 819.86 combined
  mixed actions/sec, not 2x the 71.86 single-process baseline. Per-process
  workflow commit p95 rose to about 212-229ms, activity completion p95 rose to
  about 9.5-10.0ms, and sampled Postgres waits increased sharply for WAL
  sync/write and transaction-id locks while cache hit ratio stayed near 1.0,
  temp bytes stayed at 0, and no deadlocks occurred. This points to a fixed
  Durust-side hot-row issue plus remaining shared Postgres WAL/host pressure;
  the harness still needs explicit pool-wait and host CPU counters to identify
  the exact saturation point.
- Doubling only logical shards to 200 while keeping 10 workers, 16 physical
  partitions, and pool size 24 reached 77.84 processing workflows/sec. Max
  sampled active connections stayed at 8, so extra shards alone do not reproduce
  the two-process result.
- A fully doubled one-process shape with 2,000 workflows, 20 workers, 200
  logical shards, 32 physical partitions, and pool size 48 reached 81.91
  processing workflows/sec. Reducing that pool to 12 produced essentially the
  same throughput, 81.56 processing workflows/sec, with lower commit tail
  latency but slower pool-bound reads/replay. This rules out Postgres connection
  count as the primary missing scaling factor.
- The doubled one-process shape showed much worse same-run commit tails than
  two separate benchmark processes before the sequence fix: workflow commit
  p95/p99 about 520/1,108ms with pool 48, and sampled `transactionid` waits
  around 1,593. After the sequence fix, the same normal one-process, one-schema
  shape reached 108.36 processing workflows/sec and 866.86 processing mixed
  actions/sec with commit p95/p99 99.43/111.12ms and only 4 sampled
  `transactionid` waits. The one-process result now matches the earlier
  two-process/separate-schema control without requiring separate logical
  benchmark instances.
- A temporary one-schema executor-group diagnostic for the same doubled shape
  reached 105.47 processing workflows/sec and 843.75 processing mixed
  actions/sec after the sequence fix. Because the normal path is slightly
  faster on this workload, executor grouping was not kept as an accepted
  benchmark or fix.

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
