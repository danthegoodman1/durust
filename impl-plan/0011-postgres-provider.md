---
id: 0011
title: Postgres provider
status: not_started
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

## Scope

- Postgres provider constructor/configuration.
- Schema migrations and schema-version validation.
- Append-only workflow history with ordered event ids per run.
- Workflow task ready/claim visibility with lease fencing.
- Activity claim, heartbeat, timeout, completion, retry, and stale lease fencing.
- Signal inbox idempotency and atomic consumption.
- Timer wake indexes without history scanning.
- Query projection storage.
- Child workflow outbox/inbox state and parent wakeups.
- Activity map descriptor, item, retry, and completion state.
- Payload refs and blob metadata compatibility with the provider payload layer.
- Generic delayed workflow task visibility and provider backpressure hooks.
- Startup/restart reconciliation from durable append history and provider-owned
  derived state.
- Provider conformance registration as `postgres`.
- Local test fixture and CI documentation for Postgres-backed tests.

## Acceptance

- Postgres passes the shared provider conformance suite.
- Postgres close/reopen and process restart tests prove recovery from durable
  append history and provider-owned derived state.
- Workflow task commits are fenced by claim token and expected tail event id.
- Activity completions, retries, heartbeats, and timeouts are idempotent and
  stale-lease safe.
- Signals, timers, query projections, child workflow outbox/inbox, and activity
  maps match memory and SQLite semantics.
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
- Child outbox idempotency and parent close policy.
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
- Criterion benchmark for child workflow outbox dispatch and parent wakeup.
- Criterion benchmark for activity map scheduling and completion.
- Report p50/p95/p99 latency and throughput for each hot path.
