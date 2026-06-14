---
id: 0012
title: Performance hardening
status: not_started
depends_on: [0010, 0011]
labels: [performance, benchmarks, sqlite, postgres, durable-phases, regression-gates]
---

# Performance Hardening

Make performance a dedicated release gate after the SQLite shard-file and
Postgres providers are correct, conformant, and covered by simulation. This
phase owns benchmark parity, bottleneck removal, and regression thresholds
across providers.

The benchmark reference is `../durable-phases`. Durust must meet equivalent
`../durable-phases` throughput within 5% for comparable workload dimensions
before this phase is accepted.

## Scope

- Shared benchmark harness for memory, single-file SQLite, shard-file SQLite,
  Postgres, and `../durable-phases` comparisons.
- Comparable workload dimensions: workflow count, worker count, shard/partition
  count, activation concurrency, prefetch limit, commit batch size, history
  size, payload size, and workload mix.
- Happy-path workflow execution throughput.
- Warm cached workflow wake latency.
- Cold recovery throughput and provider read pressure.
- Activity claim, heartbeat, completion, timeout, and retry throughput.
- Timer wakeup throughput.
- Signal send/consume throughput.
- Child workflow fanout and parent wakeup throughput.
- Activity map fanout/fan-in throughput.
- Payload inline/blob and codec overhead.
- Provider startup/rebuild throughput.
- Regression thresholds and documented benchmark invocation.

## Acceptance

- Benchmarks are reproducible locally with documented commands and environment
  requirements.
- Benchmark outputs include enough dimensions to compare Durust and
  `../durable-phases` honestly.
- For each equivalent benchmark, Durust throughput is no worse than 5% below
  the relevant `../durable-phases` baseline, or the phase remains open with a
  documented bottleneck and fix plan.
- Single-file SQLite, shard-file SQLite, and Postgres each have provider-specific
  bottlenecks measured and either fixed or explicitly accepted.
- Warm cached workflow latency is reported separately from cold replay recovery.
- Recovery benchmarks report provider read bytes/sec, replay events/sec, and
  p95/p99 cached wake latency under recovery load.
- CI or a documented performance job runs stable benchmark names and records
  baselines for regression tracking.

## Required Tests

- Benchmark harness validates comparable dimensions before comparing results.
- Benchmark result parser rejects missing throughput or latency metrics.
- Regression gate fails when a tracked benchmark falls below its configured
  threshold.
- Recovery benchmarks verify configured provider read budgets are honored.
- Shard-file SQLite benchmarks verify each run uses the requested shard count.
- Postgres benchmarks verify the configured connection pool and isolation
  settings are reported with results.

## Benchmark Profiles

- 1k, 10k, and 100k happy-path workflows.
- One-activity workflow execution.
- Activity-heavy workflows with heartbeat enabled and disabled.
- Timer-heavy workflows.
- Signal-heavy workflows.
- Child workflow fanout.
- Activity map fanout and completion fan-in.
- Mixed workload with activities, timers, signals, child workflows, select, and
  join.
- Recovery after worker crash with long histories.
- Cached hot workflows while cold recovery is saturated.
- Inline payloads versus blob-backed payload refs.
- SQLite shard-file runs at 1, 4, 16, and 64 shards where feasible.
- Postgres runs at multiple worker and connection-pool sizes.

## Performance Gate

- Meet `../durable-phases` throughput within 5% for every comparable benchmark
  dimension accepted for this phase.
- Record any benchmark where Durust exceeds `../durable-phases` by more than 5%
  and keep the benchmark shape as a regression target.
- Record p50/p95/p99 latency alongside throughput for every hot-path benchmark.
- Publish baseline files or documented captured output for the accepted run.
