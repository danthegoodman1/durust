---
id: 0011
title: Postgres provider
status: completed
depends_on: [0001, 0002, 0004, 0005, 0006, 0007, 0008, 0009]
labels: [postgres, provider, leases, conformance, recovery, benchmarks]
---

# Postgres Provider

Add a production Postgres durability provider after memory and single-file
SQLite have proven the runtime/provider contract and recovery flow-control
model.

This phase is about correctness, durability, operability, and conformance. It
must include benchmarks for visibility and regression tracking, but hard
cross-provider performance tuning belongs to `0012-performance-hardening.md`.

## Reference Implementation

Use `../durable-phases` as the high-performance reference for this phase. Its
Postgres provider proves the production posture we want here: pooled async
connections, fenced append commits, shard-owned hot state, physical partition
metadata validation, bounded journal catch-up, periodic snapshots, statement and
lock timeouts, and benchmark scripts that compare happy-path throughput across
providers.

Do not copy its public API shape wholesale. Durust's current `DurableBackend`
contract remains the compatibility boundary for this phase. The reference is for
the provider invariants and performance model:

- normal workflow-task progress stays append-first and fenced;
- SQL indexes are operational visibility aids, not the only authoritative
  execution model;
- partitioning choices are durable metadata and must fail fast on mismatch;
- hot-path claim/commit work avoids broad scans and cross-partition
  transactions;
- benchmarks must be able to show whether the Rust provider is approaching the
  known high-performance implementation before phase 0011 closes.

## Scope

- Postgres provider constructor/configuration.
- Schema migrations and schema-version validation.
- Append-only workflow history with ordered event ids per run.
- Workflow task ready/claim visibility with lease fencing.
- Activity claim, heartbeat, timeout, completion, retry, and stale lease fencing.
- Signal inbox idempotency and atomic consumption.
- Timer wake indexes without history scanning.
- Query projection storage.
- Child workflow starts and parent wakeups in the same workflow-task
  transaction when the child belongs to the same Postgres store.
- Activity map descriptor, item, retry, and completion state.
- Payload refs and blob metadata compatibility with the provider payload layer.
- Generic delayed workflow task visibility and provider backpressure hooks.
- Startup/restart reconciliation from durable append history and provider-owned
  derived state.
- Provider conformance registration as `postgres`.
- Local test fixture and CI documentation for Postgres-backed tests.

## Current State

Implemented and covered:

- `PostgresBackendConfig` and `PostgresBackend` connection setup using
  `deadpool-postgres` over `tokio-postgres`, with a configurable
  `max_pool_size`.
- Configurable PostgreSQL schema name with conservative identifier validation so
  tests and deployments can isolate Durust tables without dynamic SQL injection
  risk.
- Versioned schema creation for the provider-owned tables needed by the
  `DurableBackend` contract: workflow instances, history, payload blobs,
  activity tasks, activity maps/results, waits, projections, version markers,
  and signals.
- Startup schema-version validation that fails clearly on incompatible existing
  metadata.
- Env-gated Postgres tests for migration success and incompatible version
  rejection, plus an always-on identifier validation test.
- First `DurableBackend` method slice over real Postgres transactions:
  `payload_storage_config`, `current_time`, `start_workflow`, `cancel_workflow`,
  `claim_workflow_task`, `stream_history`, `stream_history_for_replay`,
  `hydrate_payload`, `release_workflow_task`, `signal_workflow`,
  `read_signal_inbox`, `fire_due_timers`, `query_projection`, and
  `workflow_change_versions`, plus activity task claim, heartbeat, timeout,
  complete, fail, retry, and stale-lease fencing for non-map activities.
- Initial `commit_workflow_task` transaction support for claim-token fencing,
  expected-tail conflict detection, append events, scheduled activity rows,
  activity map descriptor/item materialization rows, active wait
  upserts/deletes, signal consumption marks, cancellation command operational
  cleanup, query projection writes, terminal workflow closure, continue-as-new,
  version marker indexing, and payload normalization/hydration.
- Inline child workflow start support in `commit_workflow_task`: the parent
  `ChildWorkflowStartRequested` event, child workflow row, child
  `WorkflowStarted` event, and parent `ChildWorkflowStarted` or
  `ChildWorkflowFailed` wake event commit atomically. Postgres does not maintain
  an internal child-start outbox because all of this state lives behind one
  transactional store boundary; `dispatch_child_workflow_starts` is a generic
  no-op for this provider.
- Direct workflow cancellation appends `WorkflowCancelled`, clears ready waits,
  closes uncompleted activity rows, rejects late activity completion as already
  completed, makes terminal workflows unclaimable/unsignalable, notifies a
  waiting parent, and cancels open child workflows whose parent close policy is
  `Cancel`.
- Terminal child workflow commits now append `ChildWorkflowCompleted`,
  `ChildWorkflowFailed`, or `ChildWorkflowCancelled` to the parent atomically
  with the child terminal event. Parent close policy `Cancel` and `Abandon` are
  covered for inline Postgres child starts.
- Hot timer visibility scans use `active_waits(namespace, kind, ready_at_ms,
  wait_id)` directly. Postgres no longer joins workflow rows to discover due
  timers; selected runs are locked and checked only when appending the timer
  event.
- Postgres-owned payload blob table support for large workflow-start inputs,
  with public history hydration and compact replay history covered by an
  env-gated round-trip test.
- `payload_roots` and `gc_payload_blobs` scan Postgres history, active activity
  rows, activity-map rows, signals, query projections, and provider-owned blob
  metadata transactionally. The GC path dry-runs and deletes only unreachable
  Postgres-owned blobs; wrapper/external blob GC remains owned by
  `PayloadBackend`.
- Activity maps now use Postgres-owned descriptors plus activity task rows for
  bounded item materialization, retry, completion, timeout failure, compact
  `ActivityMapCompleted`/`ActivityMapFailed` history, and blob-backed nested
  manifest hydration.
- Env-gated Postgres provider conformance is registered and passes against the
  `../durable-phases` Docker Postgres fixture. The shared child-start
  conformance assertion now validates behavior without requiring an
  outbox-specific dispatch count, so inline transactional providers remain
  valid.
- Workflow task claims now filter registered workflow types in SQL, use a
  stable ready order, and lock at most one visible row with `FOR UPDATE SKIP
  LOCKED` per claim. This keeps concurrent claimers from briefly locking the
  whole ready set.
- Restart and concurrency coverage now exercises delayed ready visibility
  across reconnect, history/query/timer/activity operational indexes across
  reconnect, concurrent workflow-task claims over the pooled provider, and stale
  workflow-task commit rejection after a replacement claim.
- Env-gated Criterion profiles exist for the phase 0011 Postgres hot paths:
  workflow claim, append/commit, bounded history streaming,
  activity claim/heartbeat/complete, signal send/consume, timer wakeup, query
  projection update/read, inline child start/parent wakeup, and activity-map
  schedule/complete.
- The Postgres benchmark harness now uses one schema per benchmark function and
  unique workflow ids per iteration, so Criterion excludes fixture teardown from
  hot-path timings. A local run against the `../durable-phases` Docker Postgres
  fixture with sample size 10 produced this baseline:

  | Hot path | p50 | p95 | p99 | Throughput |
  | --- | ---: | ---: | ---: | ---: |
  | workflow task claim | 1.12 ms | 1.45 ms | 1.52 ms | 889/s |
  | workflow task append commit | 1.53 ms | 1.74 ms | 1.79 ms | 655/s |
  | bounded history stream | 632 us | 763 us | 799 us | 1,581/s |
  | activity claim and complete | 3.08 ms | 3.35 ms | 3.39 ms | 325/s |
  | activity heartbeat | 932 us | 990 us | 994 us | 1,073/s |
  | timer due scan and wakeup | 1.78 ms | 1.84 ms | 1.85 ms | 561/s |
  | signal send and consume | 2.34 ms | 2.48 ms | 2.53 ms | 428/s |
  | query projection update | 1.25 ms | 1.43 ms | 1.49 ms | 800/s |
  | query projection read | 293 us | 331 us | 338 us | 3,413/s |
  | child start and parent wakeup | 2.72 ms | 2.86 ms | 2.87 ms | 367/s |
  | activity-map schedule and complete | 39.46 ms | 41.43 ms | 41.65 ms | 25/s |

  The activity-map profile schedules one map and completes eight materialized
  item activities per iteration.
- These DB tests require `DURUST_POSTGRES_URL`; when it is absent they skip the
  live database portion. The current slice has also been run against the
  `../durable-phases` Docker Postgres fixture.
- The production provider implementation lives in one auditable
  `src/postgres.rs` module. The env-gated Postgres test suite lives separately
  in `src/postgres/tests.rs` so test volume does not obscure the provider code.
- All current `DurableBackend` methods have Postgres implementations.

Remaining:

- None for this phase. Cross-provider tuning against the high-performance
  `../durable-phases` dimensions belongs to
  `0012-performance-hardening.md`.

## Acceptance

- Postgres passes the shared provider conformance suite.
- Postgres close/reopen and process restart tests prove recovery from durable
  append history and provider-owned derived state.
- Workflow task commits are fenced by claim token and expected tail event id.
- Activity completions, retries, heartbeats, and timeouts are idempotent and
  stale-lease safe.
- Signals, timers, query projections, child workflow lifecycle behavior, and
  activity maps match memory and SQLite semantics.
- Bounded history streaming honors `max_events` and `max_bytes`.
- Generic delayed visibility and provider backpressure behavior match the
  backend contract; Postgres does not encode workflow-semantic retry policy.
- Migrations fail clearly on incompatible schema versions.
- Benchmarks report baseline throughput and latency for hot provider paths.

## Required Tests

- Migration creates the expected schema and rejects incompatible versions.
- Workflow start idempotency and conflict behavior.
- Workflow and activity claim lease fencing.
- Stale workflow task commit rejection.
- Ordered event ids per run under concurrent commits.
- Signal inbox idempotency and atomic consumption.
- Timer wake indexes without history scanning.
- Activity heartbeat timeout and start-to-close timeout behavior.
- Activity completion idempotency and stale lease rejection.
- Activity map descriptor creation, item materialization, retry, and completion.
- Inline child start idempotency, workflow-id conflict handling, and parent
  close policy.
- Query projection consistency and overwrite behavior.
- Inline and blob-backed payload equivalence.
- Bounded history streaming under small `max_events` and `max_bytes`.
- Restart recovery from append history.
- Generic delayed workflow task visibility persists across restart.
- Provider backpressure causes generic retry/defer behavior without appending
  workflow failure.

## Simulation Profiles

- Worker crash storm against Postgres with bounded recovery budgets.
- Concurrent workflow task claims and stale commit attempts.
- Duplicate, delayed, and reordered activity completions.
- Timer, signal, child completion, and activity map fan-in under tiny history
  chunks.
- Provider backpressure while cached workflows continue to wake.

## Performance Gate

- Criterion benchmark for workflow task claim.
- Criterion benchmark for workflow task append/commit.
- Criterion benchmark for bounded history streaming.
- Criterion benchmark for activity claim/heartbeat/complete.
- Criterion benchmark for signal send/consume.
- Criterion benchmark for timer wakeup.
- Criterion benchmark for query projection update/read.
- Criterion benchmark for child workflow start and parent wakeup.
- Criterion benchmark for activity map scheduling and completion.
- Report p50/p95/p99 latency and throughput for each hot path.
