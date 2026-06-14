---
id: 0012
title: Performance hardening
status: in_progress
depends_on: [0011]
labels: [performance, benchmarks, sqlite, postgres, regression-gates]
---

# Performance Hardening

Make performance a dedicated release gate after the Postgres provider is
correct, conformant, and covered by simulation. This phase owns benchmark
baselines, bottleneck removal, and regression thresholds across providers.

Durust must maintain checked-in benchmark baselines for accepted workload
dimensions. A benchmark is accepted only when its workload shape, provider,
worker count, shard count, activation concurrency, prefetch limit, and batch
size are recorded with the measured result.

## Scope

- Shared benchmark harness for memory, single-file SQLite, Postgres, and
  generic baseline comparisons.
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

## Current State

Implemented:

- `durust-benchmark-report`, a small Criterion sample reporter that reads
  `target/criterion/<group>/*/new/sample.json`, validates that latency sample
  data is present and well-formed, and reports p50/p95/p99 plus throughput per
  benchmark. This is the first reusable reporting primitive for the phase 0012
  regression gate.
- `durust-benchmark-compare`, a strict benchmark comparator that reads one
  candidate JSON file and one baseline JSON file, requires `correct=true`,
  validates comparable dimensions before comparing throughput, and fails when
  the candidate is below the configured throughput ratio.
- `durust-benchmark-workload`, a first JSON workload runner for Durust using a
  stable result vocabulary. The current runner covers mixed mode against
  memory, single-file SQLite, and env-gated Postgres. Mixed mode runs every root
  workflow through a boot activity, child workflow with child activity, buffered
  signal, zero-duration timer, finish activity, exact output verification, and
  semantic action counters. The runner emits `backend`, `mode`, `correct`,
  provider-specific options, nested benchmark dimensions, processing-only
  throughput, semantic action counters, and Durust worker stats, and rejects
  unsupported shard, activation-concurrency, and prefetch dimensions instead of
  reporting misleading numbers.
- `benches/baselines/durust-mixed-sqlite.json`, the first checked-in baseline
  artifact. The accepted dimension is single-file SQLite, mixed mode, 1,000
  workflows, 4 workers, shard/concurrency/prefetch dimensions set to 1, and
  batch 32. The SQLite provider keeps one configured WAL/FULL connection per
  backend instance instead of reopening a connection and reapplying pragmas for
  every operation. The captured release run completed 1,000 workflows, 8,000
  semantic mixed actions, 8,000 workflow tasks, and reports about 146.7
  processing workflows/sec and 1,173.8 processing mixed-actions/sec.
- `benches/baselines/durust-mixed-postgres.json`, the first checked-in
  env-gated Postgres baseline artifact. The accepted dimension is Postgres,
  mixed mode, 1,000 workflows, 4 workers, shard/concurrency/prefetch dimensions
  set to 1, batch 32, and pool size 8. The captured release run completed 1,000
  workflows, 8,000 semantic mixed actions, 7,973 workflow tasks, and reports
  about 41.7 processing workflows/sec and 333.3 processing mixed-actions/sec.
  Workflow task count is provider/order dependent because cold replay may
  consume multiple ready history events in one task; semantic mixed actions are
  the stable cross-provider workload dimension.
- `tests/fixtures/postgres.compose.yml`, a local Postgres fixture for env-gated
  benchmark smoke runs and future checked-in Postgres workload baselines.

Remaining:

- Add comparable benchmark support for additional modes: bare, activity,
  signal, timer, child, activity map, payload refs, recovery, and cached wake
  under recovery load.
- Add a real write-combining path for hot workflow/activity/timer/child
  progress so accepted batch dimensions represent fewer durable commits, not
  merely larger drain loops. This should stay generic at the runtime/backend
  contract boundary and preserve per-run fencing, event ordering, and crash
  safety.
- Implement `0013-postgres-shard-native.md` before accepting Postgres benchmark
  dimensions with shard count, activation concurrency, prefetch, or batch size
  above 1. The normalized Postgres layout remains the correctness baseline;
  the shard-native layout inside `PostgresBackend` owns the scale-out benchmark
  gate.
- Wire CI/performance-job guidance.
- Add checked-in baseline files or captured-output artifacts for the remaining
  accepted benchmark dimensions.

## Local Commands

Current Durust mixed SQLite workload:

```bash
cargo run --release --locked --bin durust-benchmark-workload -- \
  --backend sqlite \
  --mode mixed \
  --workflows 1000 \
  --workers 4 \
  --shards 1 \
  --activation-concurrency 1 \
  --activation-prefetch-limit 1 \
  --batch 32 \
  --json > target/durust-mixed-sqlite.json
```

Current Durust mixed Postgres workload:

```bash
docker compose -f tests/fixtures/postgres.compose.yml up -d --wait

DURUST_POSTGRES_URL=postgresql://durable:durable@127.0.0.1:55432/durable \
  cargo run --release --locked --bin durust-benchmark-workload -- \
  --backend postgres \
  --mode mixed \
  --workflows 1000 \
  --workers 4 \
  --shards 1 \
  --activation-concurrency 1 \
  --activation-prefetch-limit 1 \
  --batch 32 \
  --postgres-pool-size 8 \
  --json > target/durust-mixed-postgres.json

docker compose -f tests/fixtures/postgres.compose.yml down -v
```

Regression gate for captured JSON files:

```bash
cargo run --release --locked --bin durust-benchmark-compare -- \
  --durust target/durust-mixed-sqlite.json \
  --baseline benches/baselines/durust-mixed-sqlite.json \
  --min-ratio 0.95

cargo run --release --locked --bin durust-benchmark-compare -- \
  --durust target/durust-mixed-postgres.json \
  --baseline benches/baselines/durust-mixed-postgres.json \
  --min-ratio 0.95
```

## Acceptance

- Benchmarks are reproducible locally with documented commands and environment
  requirements.
- Benchmark outputs include enough dimensions to compare a candidate run and a
  checked-in baseline honestly.
- For each accepted benchmark, candidate throughput is no worse than 5% below
  the relevant Durust-owned baseline, or the phase remains open with a
  documented bottleneck and fix plan.
- Single-file SQLite and Postgres each have provider-specific bottlenecks
  measured and either fixed or explicitly accepted.
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
- Postgres runs at multiple worker and connection-pool sizes.

## Performance Gate

- Meet checked-in Durust baseline throughput within 5% for every benchmark
  dimension accepted for this phase.
- Record any benchmark where a candidate run exceeds the baseline by more than
  5% and keep the benchmark shape as a regression target.
- Record p50/p95/p99 latency alongside throughput for every hot-path benchmark.
- Publish baseline files or documented captured output for the accepted run.
