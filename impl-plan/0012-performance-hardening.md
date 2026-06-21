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
  count, activation concurrency, prefetch limit, commit batch size, activity
  completion batch size, history size, payload size, and workload mix.
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
  throughput, semantic action counters, Durust worker stats, per-backend-method
  latency/count/item metrics, and Postgres transaction/WAL/block/activity-wait
  counters, and statement-call density. It can also collect best-effort local
  process CPU/RSS samples when run with `--sample-resources`; resource sampling
  is opt-in so accepted throughput baselines are not perturbed by shell-based
  sampling overhead. It rejects unsupported shard, activation-concurrency, and
  prefetch dimensions instead of reporting misleading numbers.
- `durust-benchmark-workload --mode child-map`, a manifest-backed child
  workflow map workload with configurable `--child-map-items` and
  `--child-map-max-in-flight`. It exercises compact parent history, provider
  map descriptors, child start materialization, result manifest writes, and
  ordered child-map completion accounting across memory, SQLite, and Postgres.
- `durust-benchmark-workload --mode postgres-write-ceiling`, a Postgres-only
  diagnostic that exercises a comparable transactional write shape without the
  Durust runtime. It records the same Postgres stats envelope as the mixed
  workload so local runs can distinguish Rust-side bottlenecks from the
  machine's Postgres statement/write ceiling.
- `benches/baselines/durust-mixed-sqlite.json`, the checked-in 4-worker
  single-file SQLite mixed baseline. The accepted dimension is mixed mode, 1,000
  workflows, 4 workers, shard/concurrency/prefetch dimensions set to 1, and
  batch 32. The SQLite provider keeps one configured WAL/FULL connection per
  backend instance and now has ready-workflow and claimable-activity queue
  indexes. The accepted index experiment completed 1,000 workflows, 8,000
  semantic mixed actions, and 8,000 workflow tasks at a 3-run median of 206.10
  processing workflows/sec and 1,648.83 processing mixed-actions/sec.
- `benches/baselines/durust-mixed-sqlite-1-worker.json`, the checked-in
  single-worker SQLite-local mixed baseline for the same workload dimensions
  except `workers = 1`. This profile records the preferred single-file local
  shape, where the shared SQLite connection avoids multi-worker mutex/cache
  churn. The accepted index experiment measured a 3-run median of 215.63
  processing workflows/sec and 1,725.01 processing mixed-actions/sec.
- A measured SQLite follow-up kept only queue indexes. A combined maintenance
  transaction improved maintenance p95 but regressed 4-worker throughput beyond
  the gate, and SQLite batch claim overrides improved activity-claim p95 but
  regressed both 4-worker and 1-worker throughput, so both were dropped.
- `benches/baselines/durust-mixed-postgres.json`, the first checked-in
  env-gated Postgres baseline artifact. The accepted dimension is Postgres,
  mixed mode, 1,000 workflows, 4 workers, shard/concurrency/prefetch dimensions
  set to 1, batch 32, and pool size 8. The captured release run completed 1,000
  workflows, 8,000 semantic mixed actions, 7,941 workflow tasks, and reports
  about 38.5 processing workflows/sec and 307.7 processing mixed-actions/sec
  with p50/p95/p99 workflow-task commit latency 4.94/7.98/8.87ms. Workflow task
  count is provider/order dependent because cold replay may consume multiple
  ready history events in one task; semantic mixed actions are the stable
  cross-provider workload dimension.
- `benches/baselines/durust-mixed-postgres-100-shards.json`, the first
  checked-in high-shard Postgres baseline artifact. The accepted dimension is
  Postgres, mixed mode, 1,000 workflows, 10 workers, 100 logical shards, 16
  physical partitions, activation concurrency 8, prefetch 32, batch 32, and pool
  size 24. The captured release run completed 1,000 workflows, 8,000 semantic
  mixed actions, 7,874 workflow tasks, and reports about 103.7 processing
  workflows/sec and 829.9 processing mixed-actions/sec with p50/p95/p99
  workflow-task commit latency 46.77/65.91/85.08ms. Postgres reported 31,761
  total transactions, or 3.97 transactions per mixed action and 31.76
  transactions per completed workflow, plus 160,917 statement calls, or 20.11
  statements per mixed action. This run includes sequence-backed run
  IDs, signal receive sequences, and claim tokens, selected-shard-only lease
  refresh, batched activity claims, single-query history streaming, derived
  complete-history workflow change markers, and combined timer/activity
  maintenance, removing hot `meta` counter locks, idle shard lease churn,
  thousands of individual activity claim round trips, and avoidable metadata
  transactions from the sharded workload.
- Postgres durability-path transaction and statement reductions after the
  accepted sharded baseline: claimed workflow tasks carry a bounded, contiguous
  tail of prefetched history; cached wakes and cold recovery now consume that
  claim-prefetched history before asking the provider to stream. Batch workflow
  claim uses one sequence query plus one bulk lease update instead of one
  token/update pair per claimed task; shard-journal append increments and
  returns the journal sequence in one head-row upsert; batch workflow commit
  bulk verifies shard leases once per transaction; workflow-task commits bulk
  insert appended history events while preserving marker indexes; and Postgres
  batch activity completion uses a SQL-native normal-activity path with
  per-item results, falling back to scalar completion for duplicate input ids
  and activity-map items.
- The Postgres simple workflow-commit batch path is now the target mixed
  workload architecture rather than a narrow compatibility fast path. It covers
  ordinary history-only commits, direct child workflow starts, terminal
  completed/failed/cancelled events, set-based terminal operational cleanup,
  direct parent child-terminal notifications, and close-policy cancellation. It
  excludes only marker/index commits, activity-map/child-map item commits,
  cancellations, continue-as-new, and terminal commits whose parent/child
  dependency is in the same batch.
- The Rust workload runner now reports `processingBackendMetrics` separately
  from full-run backend metrics and takes Postgres/statement stat snapshots
  around the processing phase rather than setup and verification. Backend
  operation reports include call/item density per mixed action, and
  workflow-task commit shape counters classify how many commits match the
  current Postgres simple batch fast path versus child-start, terminal,
  activity-map, child-map, cancel, or other history-event fallback reasons.
- Local 100-shard mixed comparison on the current target architecture
  (1,000 workflows, 10 workers, 100 logical shards, 16 physical partitions,
  activation concurrency 8, prefetch 32, batch 32, activity-completion batch
  32, pool size 24) measured the current TypeScript runner at 1,666.03
  processing mixed-actions/sec, 1.002 transactions/action, and 7.409
  statements/action. Rust measured 1,999.51 processing mixed-actions/sec,
  0.587 transactions/action, and 5.047 statements/action on the repeated
  default 12-pass worker cadence, with a best default-equivalent 12-pass run at
  2,115.79 processing mixed-actions/sec, 0.532 transactions/action, and 4.889
  statements/action. In these Rust runs every mixed-workload workflow commit was
  simple-batch eligible; the remaining gap to a stable ceiling is runtime
  variance and non-workflow scalar paths such as signal send/read and timer
  maintenance.
- `tests/fixtures/postgres.compose.yml`, a local Postgres fixture for env-gated
  benchmark smoke runs and future checked-in Postgres workload baselines.

Remaining:

- Add comparable benchmark support for additional modes: bare, activity,
  signal, timer, child, activity map, payload refs, recovery, and cached wake
  under recovery load. Child workflow map fanout now has an initial workload
  mode; checked-in thresholds remain pending measured baselines.
- Extend target-architecture write-combining into the remaining hot scalar
  paths: signal send/read, timer maintenance, activity-map item completion,
  child-map item completion, and payload-ref materialization where the provider
  can batch without weakening per-run fencing, event ordering, or crash safety.
- Finish `0013-postgres-shard-native.md` snapshot/journal-tail rebuild before
  recommending the high-shard Postgres layout as the recovery architecture; the
  checked-in high-shard baseline currently proves scale-out for claim/commit
  batching, not full projection rebuild.
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

cargo run --release --locked --bin durust-benchmark-workload -- \
  --backend sqlite \
  --mode mixed \
  --workflows 1000 \
  --workers 1 \
  --shards 1 \
  --activation-concurrency 1 \
  --activation-prefetch-limit 1 \
  --batch 32 \
  --json > target/durust-mixed-sqlite-1-worker.json
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
- Child workflow map fanout and completion fan-in.
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
