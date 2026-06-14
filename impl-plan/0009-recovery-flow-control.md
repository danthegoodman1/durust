---
id: 0009
title: Recovery flow control
status: completed
depends_on: [0001, 0008]
labels: [runtime, recovery, provider-conformance, backpressure, benchmarks]
---

# Recovery Flow Control

Add worker-level flow control and generic provider backpressure so crash
recovery, cache eviction storms, and deployment churn cannot saturate the
durability provider.

Streaming history keeps per-workflow memory bounded. This item adds the
operational controls that keep aggregate recovery read pressure bounded.

## Scope

- Worker recovery admission control.
- Worker-level recovery concurrency limit.
- Worker-level replay byte/event budgets per cold recovery attempt.
- Separate cold replay throttling from cached workflow wakes.
- Recovery prefetch limit.
- Generic provider backpressure signal.
- Generic provider retry-after or delayed visibility handling.
- Generic provider read backpressure by provider implementation.
- Provider conformance for delayed visibility and stream-budget behavior.
- Recovery storm simulation profiles.
- Recovery throughput and fairness benchmarks.

## Ownership

Worker/runtime owns semantic policy:

- How many cold recoveries may run concurrently.
- How replay byte/event budgets are divided across recovery attempts, task queues, namespaces, and workers.
- How cached workflow wakes stay ahead of cold replay when capacity is scarce.
- How long to defer or release a workflow task when recovery admission is unavailable.

Providers own storage protection:

- Honor `max_events` and `max_bytes` for every `stream_history` request.
- Enforce optional provider-wide read budgets by namespace, queue, shard, or backend instance.
- Return generic backpressure or retry-after results when saturated.
- Rate-limit startup replay and derived-index rebuild.
- Avoid workflow-semantic policy such as nondeterminism-specific retry choices.

## Acceptance

- Worker builder exposes recovery concurrency, per-attempt replay event/byte
  budgets, prefetch chunk limits, and defer-delay knobs.
- Cold replay must acquire recovery admission before streaming history.
- Cached workflow wake processing is not blocked behind cold replay saturation.
- When recovery admission is unavailable, workers release or defer workflow tasks
  through generic delayed visibility rather than holding leases idle.
- Provider backpressure is generic and does not classify recovery causes.
- Memory and SQLite providers pass conformance for delayed release visibility.
- Provider conformance covers delayed visibility and bounded history stream
  requests under budget pressure.
- Recovery storm simulation demonstrates bounded provider read pressure.
- Benchmarks report warm cached throughput and cold recovery throughput separately.

## Public API Budget

Manual composition without worker-level recovery controls would require users to
build an external poll scheduler that can distinguish cached workflow wakes from
cold replay misses, inspect each claimed task before replay, and safely release
tasks with delayed visibility when provider read capacity is unavailable. That
logic cannot be composed from activity, timer, select, join, side effect, or
payload primitives without holding leases idle or creating unbounded hot-loop
retry pressure.

The builder knobs earn their place because they protect scaling invariants on
the recovery hot path:

- `max_concurrent_recoveries` bounds in-worker cold replay work.
- `recovery_replay_event_budget` bounds replay events loaded per cold recovery
  attempt.
- `recovery_replay_byte_budget` bounds replay bytes requested per cold recovery
  attempt.
- `recovery_prefetch_chunks` bounds replay stream calls per cold recovery
  attempt.
- `recovery_defer_delay` converts unavailable capacity into generic delayed task
  visibility instead of a tight claim/release loop.
- `history_chunk_bytes` exposes the existing stream byte bound alongside
  `history_chunk_events`.

No durability provider gets workflow-semantic policy. Provider contract changes
are generic: honor stream `max_events`/`max_bytes`, store delayed visibility, and
optionally return retry-after backpressure.

## Required Tests

- Worker does not start more than configured `max_concurrent_recoveries`.
- Worker honors configured replay byte/event budgets while streaming recovery
  history.
- Cached wake can progress while cold replay is saturated.
- Recovery task is released or deferred when admission is unavailable.
- Generic provider backpressure causes retry/defer without appending workflow
  failure.
- Memory provider delayed visibility conformance.
- SQLite provider delayed visibility conformance.
- Stream history still honors `max_events` and `max_bytes` under throttling.
- SQLite close/reopen preserves generic delayed visibility where the provider persists it.

## Simulation Profiles

- Worker crash storm with thousands of recoveries.
- Cache eviction storm while cached hot workflows continue waking.
- Generic provider backpressure with many workers.
- Mixed hot cached workflows and cold replay from long histories.
- Sharded recovery storm with per-shard backpressure.
- Provider startup rebuild competing with live recovery traffic.

## Performance Gate

- Criterion benchmark for recovery admission overhead.
- Criterion benchmark for replay budget accounting.
- Criterion benchmark for cached wake latency while recovery is saturated.
- Future provider-specific soak benchmark for worker crash storm with fixed
  provider read budget.
- Report recovery throughput, warm cached throughput, provider read bytes/sec
  where available, and p95/p99 cached wake latency separately.

## Completion Notes

- `WorkerBuilder` now exposes `max_concurrent_recoveries`,
  `recovery_replay_event_budget`, `recovery_replay_byte_budget`,
  `recovery_prefetch_chunks`, `recovery_defer_delay`, and
  `history_chunk_bytes`.
- Cold replay beyond the start event acquires worker admission before streaming
  history. Budget exhaustion releases the workflow task with generic delayed
  visibility instead of holding the lease.
- Cached workflow wakes stream only events after the cached tail and bypass cold
  recovery admission.
- Providers remain generic. `Error::Backpressure { retry_after, .. }` lets any
  provider signal storage pressure; workers defer the workflow task without
  appending `WorkflowFailed`.
- Memory and SQLite conformance cover delayed visibility and stream
  `max_events`/`max_bytes`; SQLite additionally proves delayed visibility
  survives close/reopen.
- Replay regressions cover admission deferral before streaming, replay event
  budget deferral, generic provider backpressure, cached wake fairness, and the
  no-`WorkflowFailed` invariant.
- Deterministic simulation includes a recovery storm model with bounded active
  recoveries, budget deferral, provider backpressure, and cached wake progress.
- Criterion benchmark names added:
  `recovery_defer_no_admission_memory`,
  `recovery_defer_event_budget_memory`, and
  `cached_wake_with_recovery_saturated_memory`.
