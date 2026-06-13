---
id: 0009
title: Recovery flow control
status: not_started
depends_on: [0001, 0008]
labels: [runtime, recovery, provider-conformance, backpressure, benchmarks]
---

# Recovery Flow Control

Add worker-level and provider-level flow control so crash recovery, cache
eviction storms, and deployment churn cannot saturate the durability provider.

Streaming history keeps per-workflow memory bounded. This item adds the
operational controls that keep aggregate recovery read pressure bounded.

## Scope

- Worker recovery admission control.
- Worker-level recovery concurrency limit.
- Worker-level replay byte/event token buckets.
- Separate budgets for cold replay and cached workflow wakes.
- Recovery prefetch limit.
- Generic provider backpressure signal.
- Generic provider retry-after or delayed visibility handling.
- Provider-level read throughput budgets by provider implementation.
- Provider conformance for delayed visibility and stream-budget behavior.
- Recovery storm simulation profiles.
- Recovery throughput and fairness benchmarks.

## Ownership

Worker/runtime owns semantic policy:

- How many cold recoveries may run concurrently.
- How replay byte/event budgets are divided across task queues, namespaces, and workers.
- How cached workflow wakes stay ahead of cold replay when capacity is scarce.
- How long to defer or release a workflow task when recovery admission is unavailable.

Providers own storage protection:

- Honor `max_events` and `max_bytes` for every `stream_history` request.
- Enforce optional provider-wide read budgets by namespace, queue, shard, or backend instance.
- Return generic backpressure or retry-after results when saturated.
- Rate-limit startup replay and derived-index rebuild.
- Avoid workflow-semantic policy such as nondeterminism-specific retry choices.

## Acceptance

- Worker builder exposes recovery concurrency and replay throughput knobs.
- Cold replay must acquire recovery admission before streaming history.
- Cached workflow wake processing is not blocked behind cold replay saturation.
- When recovery admission is unavailable, workers release or defer workflow tasks
  through generic delayed visibility rather than holding leases idle.
- Provider backpressure is generic and does not classify recovery causes.
- Memory and SQLite providers pass conformance for delayed release visibility.
- Provider conformance covers bounded history stream requests under budget pressure.
- Recovery storm simulation demonstrates bounded provider read pressure.
- Benchmarks report warm cached throughput and cold recovery throughput separately.

## Required Tests

- Worker does not start more than configured `max_concurrent_recoveries`.
- Worker honors configured replay byte/event budgets while streaming recovery history.
- Cached wake can progress while cold replay is saturated.
- Recovery task is released or deferred when admission is unavailable.
- Provider backpressure causes retry/defer without appending workflow failure.
- Memory provider delayed visibility conformance.
- SQLite provider delayed visibility conformance.
- Stream history still honors `max_events` and `max_bytes` under throttling.
- SQLite close/reopen preserves generic delayed visibility where the provider persists it.

## Simulation Profiles

- Worker crash storm with thousands of recoveries.
- Cache eviction storm while cached hot workflows continue waking.
- Provider read-budget exhaustion with many workers.
- Mixed hot cached workflows and cold replay from long histories.
- Sharded recovery storm with per-shard read budgets.
- Provider startup rebuild competing with live recovery traffic.

## Performance Gate

- Criterion benchmark for recovery admission overhead.
- Criterion benchmark for replay token-bucket accounting.
- Criterion benchmark for cached wake latency under cold replay load.
- Criterion benchmark for provider stream throughput with budgets enabled.
- Soak benchmark for worker crash storm with fixed provider read budget.
- Report recovery throughput, warm cached throughput, provider read bytes/sec,
  and p95/p99 cached wake latency separately.
