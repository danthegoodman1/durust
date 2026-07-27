use durust::{
    Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, PostgresBackend,
    PostgresBackendConfig, RunId, ShardId, SqliteBackend, StreamHistoryRequest, Worker,
    WorkerRunStats,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_WORKFLOWS: u64 = 250;
const DEFAULT_WORKERS: usize = 4;
const DEFAULT_BATCH: usize = 32;
const DEFAULT_WORKER_ROUND_PASSES: usize = 12;
const DEFAULT_MAX_ROUNDS: usize = 10_000;
const WORKFLOW_QUEUE: &str = "workflows";
const ACTIVITY_QUEUE: &str = "activities";
static POSTGRES_SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);
static POSTGRES_DATABASE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BenchmarkBackend {
    Memory,
    Postgres,
    Sqlite,
}

impl BenchmarkBackend {
    fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Postgres => "postgres",
            Self::Sqlite => "sqlite",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkOptions {
    #[serde(skip)]
    backend: BenchmarkBackend,
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sqlite_layout: Option<String>,
    workflows: u64,
    workflow_offset: u64,
    workers: usize,
    shards: u64,
    physical_partitions: u64,
    activation_concurrency: u64,
    activation_prefetch_limit: u64,
    activity_delay_ms: u64,
    batch: usize,
    activity_completion_batch: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker_round_passes: Option<usize>,
    child_map_items: u64,
    child_map_max_in_flight: usize,
    signal_batch_width: usize,
    max_rounds: usize,
    keep_db: bool,
    list_stale: bool,
    drop_stale: bool,
    #[serde(skip_serializing_if = "is_false")]
    sample_resources: bool,
    #[serde(skip_serializing_if = "is_false")]
    force_scalar_signal_reads: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    postgres_pool_size: Option<usize>,
    json: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkCounters {
    workflow_starts: u64,
    signals: u64,
    child_starts: u64,
    child_completions: u64,
    timer_handlers: u64,
    boot_activities: u64,
    child_activities: u64,
    finish_activities: u64,
    workflow_tasks: u64,
    activity_tasks: u64,
    timers_fired: u64,
    activities_timed_out: u64,
    child_workflow_starts_dispatched: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerStatsReport {
    workflow_tasks: u64,
    activity_tasks: u64,
    timers_fired: u64,
    activities_timed_out: u64,
    child_workflow_starts_dispatched: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct LatencyReport {
    samples: u64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct BackendMetricsReport {
    workflow_task_commit_latency: LatencyReport,
    operations: BTreeMap<String, BackendOperationReport>,
    workflow_task_commit_shapes: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct BackendOperationReport {
    calls: u64,
    errors: u64,
    items: u64,
    total_ms: f64,
    items_per_call: f64,
    items_per_second: f64,
    calls_per_mixed_action: f64,
    items_per_mixed_action: f64,
    total_ms_per_mixed_action: f64,
    latency: LatencyReport,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PostgresStatsReport {
    wal_bytes: u64,
    wal_bytes_per_second: f64,
    wal_records: u64,
    wal_records_per_second: f64,
    wal_fpi: u64,
    wal_buffers_full: u64,
    wal_write: u64,
    wal_sync: u64,
    wal_write_time_ms: f64,
    wal_sync_time_ms: f64,
    xact_commit: u64,
    xact_rollback: u64,
    transactions_per_second: f64,
    transactions_per_mixed_action: f64,
    transactions_per_workflow: f64,
    rows_returned: u64,
    rows_fetched: u64,
    rows_inserted: u64,
    rows_updated: u64,
    rows_deleted: u64,
    blocks_read: u64,
    blocks_hit: u64,
    block_cache_hit_ratio: f64,
    temp_files: u64,
    temp_bytes: u64,
    deadlocks: u64,
    block_read_time_ms: f64,
    block_write_time_ms: f64,
    active_time_ms: f64,
    session_time_ms: f64,
    active_connections_after: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    activity_samples: Option<PostgresActivitySamplesReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    statement_stats: Option<PostgresStatementStatsReport>,
    /// Why `statement_stats` is absent, when it is. Present only on failure.
    ///
    /// Without this the snapshot error was dropped by `.ok()` at all four call
    /// sites, so the run reported `statementStats: null` and said nothing about
    /// the extension or the server configuration that caused it. The reader
    /// this serves is **whoever runs this binary**, from a shell or a script:
    /// they now get the reason on stderr and in the JSON.
    ///
    /// It does **not** improve the `expected null not to be null` failure in
    /// `packages/benchmark/test/thresholds.test.ts`, and an earlier version of
    /// this comment claimed it did. That assertion runs against the TypeScript
    /// `runBenchmark`, which computes its own `statementStats` and never reads
    /// this struct — measured after this change, the message there is
    /// byte-identical to before it. Fixing that needs the same treatment
    /// applied in `packages/postgres/src/index.ts`.
    #[serde(skip_serializing_if = "Option::is_none")]
    statement_stats_unavailable: Option<String>,
    /// Set when another database's backends were live during the run.
    ///
    /// Every other counter here comes from `pg_stat_database` filtered to
    /// `current_database()`, and this binary now runs in a database of its own,
    /// so those are exclusive by construction. `pg_stat_wal` has no database
    /// column at all — it is cluster-wide — so the `wal_*` fields above include
    /// writes made by anything else on the same server, and are reported rather
    /// than nulled so a reader comparing WAL figures across runs knows when
    /// they were shared.
    ///
    /// Two limits, both real. This field is only set when foreign backends are
    /// *live at snapshot time*, so it is silent on the quiet machine where a
    /// baseline would be recorded and loud afterwards — the wrong way round for
    /// warning whoever records one. And no threshold key in
    /// `packages/benchmark/src/index.ts` can reach it: that interface is
    /// closed, lists `walBytes`…`walSyncTimeMs` with no note that they are
    /// cluster-wide, and does not carry this field. A future gate on a `wal_*`
    /// key would therefore be written with nothing warning its author. The
    /// durable fix is a note on that TypeScript type, not here.
    #[serde(skip_serializing_if = "Option::is_none")]
    wal_stats_shared_with_other_databases: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PostgresStatementStatsReport {
    calls: u64,
    calls_per_mixed_action: f64,
    calls_per_workflow: f64,
    total_exec_time_ms: f64,
    top_statements: Vec<PostgresStatementReport>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PostgresStatementReport {
    query_id: String,
    calls: u64,
    total_exec_time_ms: f64,
    query: String,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct PostgresActivitySamplesReport {
    samples: u64,
    max_connections: u64,
    max_active: u64,
    max_idle: u64,
    max_waiting: u64,
    wait_event_type_counts: BTreeMap<String, u64>,
    wait_event_counts: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct ResourceSamplesReport {
    samples: u64,
    available_parallelism: u64,
    max_process_cpu_percent: f64,
    max_process_rss_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
struct PostgresStatsSnapshot {
    wal_bytes: u64,
    wal_records: u64,
    wal_fpi: u64,
    wal_buffers_full: u64,
    wal_write: u64,
    wal_sync: u64,
    wal_write_time_ms: f64,
    wal_sync_time_ms: f64,
    xact_commit: u64,
    xact_rollback: u64,
    rows_returned: u64,
    rows_fetched: u64,
    rows_inserted: u64,
    rows_updated: u64,
    rows_deleted: u64,
    blocks_read: u64,
    blocks_hit: u64,
    temp_files: u64,
    temp_bytes: u64,
    deadlocks: u64,
    block_read_time_ms: f64,
    block_write_time_ms: f64,
    active_time_ms: f64,
    session_time_ms: f64,
    active_connections: u64,
}

#[derive(Clone, Debug, Default)]
struct PostgresStatementStatsSnapshot {
    statements: BTreeMap<String, PostgresStatementStatsEntry>,
}

#[derive(Clone, Debug, Default)]
struct PostgresStatementStatsEntry {
    query: String,
    calls: u64,
    total_exec_time_ms: f64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkResult {
    backend: String,
    mode: String,
    correct: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    sqlite_layout: Option<String>,
    options: BenchmarkOptions,
    elapsed_ms: f64,
    setup_ms: f64,
    processing_ms: f64,
    verify_ms: f64,
    rounds: usize,
    activations: u64,
    completed_workflows: u64,
    mixed_actions: u64,
    activations_per_second: f64,
    mixed_actions_per_second: f64,
    workflows_per_second: f64,
    processing_activations_per_second: f64,
    processing_mixed_actions_per_second: f64,
    processing_workflows_per_second: f64,
    counters: BenchmarkCounters,
    worker_stats: WorkerStatsReport,
    backend_metrics: BackendMetricsReport,
    processing_backend_metrics: BackendMetricsReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    postgres_stats: Option<PostgresStatsReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_samples: Option<ResourceSamplesReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    postgres_schema: Option<String>,
    /// The database this run created and ran in.
    ///
    /// Before the per-run database, the schema named above lived in whatever
    /// `DURUST_POSTGRES_URL` pointed at, so `--keep-db` left something the
    /// caller could already find. It no longer does, and a kept schema inside
    /// an unnamed database is not inspectable — so the name has to be reported.
    #[serde(skip_serializing_if = "Option::is_none")]
    postgres_database: Option<String>,
    db_path: Option<String>,
    db_bytes: Option<u64>,
}

#[derive(Clone, Default)]
struct BackendMetrics {
    workflow_task_commit_latencies: Arc<Mutex<Vec<Duration>>>,
    operations: Arc<Mutex<BTreeMap<&'static str, OperationSamples>>>,
    workflow_task_commit_shapes: Arc<Mutex<BTreeMap<&'static str, u64>>>,
}

#[derive(Clone, Debug, Default)]
struct OperationSamples {
    durations: Vec<Duration>,
    total_duration: Duration,
    items: u64,
    errors: u64,
}

#[derive(Clone, Debug, Default)]
struct BackendMetricsSnapshot {
    workflow_task_commit_latencies: Vec<Duration>,
    operations: BTreeMap<&'static str, OperationSamples>,
    workflow_task_commit_shapes: BTreeMap<&'static str, u64>,
}

impl BackendMetrics {
    fn record_workflow_task_commit(&self, duration: Duration) {
        self.workflow_task_commit_latencies
            .lock()
            .expect("benchmark metrics mutex poisoned")
            .push(duration);
    }

    fn record_operation(&self, name: &'static str, duration: Duration, items: u64, success: bool) {
        let mut operations = self
            .operations
            .lock()
            .expect("benchmark metrics mutex poisoned");
        let samples = operations.entry(name).or_default();
        samples.durations.push(duration);
        samples.total_duration += duration;
        samples.items = samples.items.saturating_add(items);
        if !success {
            samples.errors = samples.errors.saturating_add(1);
        }
    }

    fn record_workflow_task_commit_shape(&self, commit: &durust::WorkflowTaskCommit) {
        let mut shapes = self
            .workflow_task_commit_shapes
            .lock()
            .expect("benchmark metrics mutex poisoned");
        *shapes.entry("total").or_default() += 1;
        let mut simple_eligible = true;
        let has_direct_child_starts = !commit.start_child_workflows.is_empty()
            && commit
                .start_child_workflows
                .iter()
                .all(|message| message.child_map_item.is_none());
        if commit
            .start_child_workflows
            .iter()
            .any(|message| message.child_map_item.is_some())
        {
            simple_eligible = false;
            *shapes.entry("ineligibleStartChildWorkflowMap").or_default() += 1;
        }
        if !commit.schedule_activity_maps.is_empty() {
            simple_eligible = false;
            *shapes.entry("ineligibleActivityMap").or_default() += 1;
        }
        if !commit.schedule_child_workflow_maps.is_empty() {
            simple_eligible = false;
            *shapes.entry("ineligibleChildWorkflowMap").or_default() += 1;
        }
        if !commit.cancel_commands.is_empty() {
            simple_eligible = false;
            *shapes.entry("ineligibleCancelCommand").or_default() += 1;
        }
        let mut terminal_events = 0_u64;
        let mut non_simple_history_events = 0_u64;
        for event in &commit.append_events {
            match &event.data {
                HistoryEventData::WorkflowCompleted { .. }
                | HistoryEventData::WorkflowFailed { .. }
                | HistoryEventData::WorkflowCancelled { .. } => {
                    terminal_events += 1;
                }
                HistoryEventData::WorkflowContinuedAsNew { .. } => {
                    simple_eligible = false;
                    *shapes.entry("ineligibleContinueAsNew").or_default() += 1;
                }
                HistoryEventData::VersionMarker(_) | HistoryEventData::DeprecatedPatchMarker(_) => {
                    non_simple_history_events += 1;
                    simple_eligible = false;
                }
                _ => {
                    // Non-terminal history-only events can use the Postgres batch fast path.
                }
            }
        }
        if terminal_events > 0 {
            *shapes.entry("postgresSimpleTerminalEvent").or_default() += 1;
            if has_direct_child_starts {
                simple_eligible = false;
                *shapes
                    .entry("ineligibleTerminalWithChildStart")
                    .or_default() += 1;
            }
        }
        if non_simple_history_events > 0 {
            *shapes.entry("ineligibleHistoryEvent").or_default() += 1;
        }
        if simple_eligible {
            *shapes.entry("postgresSimpleEligible").or_default() += 1;
        } else {
            *shapes.entry("postgresSimpleIneligible").or_default() += 1;
        }
    }

    fn snapshot(&self) -> BackendMetricsSnapshot {
        BackendMetricsSnapshot {
            workflow_task_commit_latencies: self
                .workflow_task_commit_latencies
                .lock()
                .expect("benchmark metrics mutex poisoned")
                .clone(),
            operations: self
                .operations
                .lock()
                .expect("benchmark metrics mutex poisoned")
                .clone(),
            workflow_task_commit_shapes: self
                .workflow_task_commit_shapes
                .lock()
                .expect("benchmark metrics mutex poisoned")
                .clone(),
        }
    }

    fn report(&self, mixed_actions: u64) -> BackendMetricsReport {
        let snapshot = self.snapshot();
        Self::report_from_parts(
            snapshot.workflow_task_commit_latencies,
            snapshot.operations,
            snapshot.workflow_task_commit_shapes,
            mixed_actions,
        )
    }

    fn report_between(
        &self,
        before: &BackendMetricsSnapshot,
        after: &BackendMetricsSnapshot,
        mixed_actions: u64,
    ) -> BackendMetricsReport {
        let commit_samples = after
            .workflow_task_commit_latencies
            .get(before.workflow_task_commit_latencies.len()..)
            .unwrap_or(&[])
            .to_vec();
        let operations = after
            .operations
            .iter()
            .map(|(name, samples)| {
                let before_samples = before.operations.get(name);
                let start = before_samples.map_or(0, |samples| samples.durations.len());
                let durations = samples.durations.get(start..).unwrap_or(&[]).to_vec();
                let total_duration = samples
                    .total_duration
                    .checked_sub(
                        before_samples.map_or(Duration::ZERO, |samples| samples.total_duration),
                    )
                    .unwrap_or(Duration::ZERO);
                let items = samples
                    .items
                    .saturating_sub(before_samples.map_or(0, |samples| samples.items));
                let errors = samples
                    .errors
                    .saturating_sub(before_samples.map_or(0, |samples| samples.errors));
                (
                    *name,
                    OperationSamples {
                        durations,
                        total_duration,
                        items,
                        errors,
                    },
                )
            })
            .filter(|(_, samples)| !samples.durations.is_empty())
            .collect::<BTreeMap<_, _>>();
        let shapes = after
            .workflow_task_commit_shapes
            .iter()
            .filter_map(|(name, value)| {
                let delta = value.saturating_sub(
                    before
                        .workflow_task_commit_shapes
                        .get(name)
                        .copied()
                        .unwrap_or(0),
                );
                (delta > 0).then_some((*name, delta))
            })
            .collect::<BTreeMap<_, _>>();
        Self::report_from_parts(commit_samples, operations, shapes, mixed_actions)
    }

    fn report_from_parts(
        commit_samples: Vec<Duration>,
        operation_samples: BTreeMap<&'static str, OperationSamples>,
        commit_shapes: BTreeMap<&'static str, u64>,
        mixed_actions: u64,
    ) -> BackendMetricsReport {
        let operations = operation_samples
            .iter()
            .map(|(name, samples)| {
                (
                    (*name).to_owned(),
                    operation_report(
                        samples.durations.clone(),
                        samples.total_duration,
                        samples.items,
                        samples.errors,
                        mixed_actions,
                    ),
                )
            })
            .collect();
        BackendMetricsReport {
            workflow_task_commit_latency: latency_report(commit_samples),
            operations,
            workflow_task_commit_shapes: commit_shapes
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value))
                .collect(),
        }
    }
}

#[derive(Clone)]
struct MeasuredBackend<B>
where
    B: DurableBackend,
{
    inner: B,
    metrics: BackendMetrics,
    force_scalar_signal_reads: bool,
}

impl<B> MeasuredBackend<B>
where
    B: DurableBackend,
{
    fn new(inner: B, metrics: BackendMetrics, force_scalar_signal_reads: bool) -> Self {
        Self {
            inner,
            metrics,
            force_scalar_signal_reads,
        }
    }
}

impl<B> DurableBackend for MeasuredBackend<B>
where
    B: DurableBackend,
{
    fn payload_storage_config(&self) -> durust::PayloadStorageConfig {
        self.inner.payload_storage_config()
    }

    fn start_workflow(
        &self,
        req: durust::StartWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::StartWorkflowOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.start_workflow(req).await;
            metrics.record_operation(
                "start_workflow",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn cancel_workflow(
        &self,
        req: durust::CancelWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::CancelWorkflowOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.cancel_workflow(req).await;
            metrics.record_operation(
                "cancel_workflow",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn current_time(&self) -> BoxFuture<'static, durust::Result<durust::TimestampMs>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.current_time().await;
            metrics.record_operation(
                "current_time",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn claim_workflow_task(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::ClaimedWorkflowTask>>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.claim_workflow_task(worker_id, opts).await;
            let items = result
                .as_ref()
                .ok()
                .and_then(|task| task.as_ref())
                .map_or(0, |_| 1);
            metrics.record_operation(
                "claim_workflow_task",
                started.elapsed(),
                items,
                result.is_ok(),
            );
            result
        })
    }

    fn claim_workflow_tasks(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::ClaimWorkflowTasksOptions,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::ClaimedWorkflowTask>>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.claim_workflow_tasks(worker_id, opts).await;
            let items = result.as_ref().map_or(0, |tasks| tasks.len() as u64);
            metrics.record_operation(
                "claim_workflow_tasks",
                started.elapsed(),
                items,
                result.is_ok(),
            );
            result
        })
    }

    fn stream_history(
        &self,
        req: durust::StreamHistoryRequest,
    ) -> BoxFuture<'static, durust::Result<durust::HistoryChunk>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.stream_history(req).await;
            let items = result.as_ref().map_or(0, |chunk| chunk.events.len() as u64);
            metrics.record_operation("stream_history", started.elapsed(), items, result.is_ok());
            result
        })
    }

    fn stream_history_for_replay(
        &self,
        req: durust::StreamHistoryRequest,
    ) -> BoxFuture<'static, durust::Result<durust::HistoryChunk>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.stream_history_for_replay(req).await;
            let items = result.as_ref().map_or(0, |chunk| chunk.events.len() as u64);
            metrics.record_operation(
                "stream_history_for_replay",
                started.elapsed(),
                items,
                result.is_ok(),
            );
            result
        })
    }

    fn hydrate_payload(
        &self,
        payload: durust::PayloadRef,
    ) -> BoxFuture<'static, durust::Result<durust::PayloadRef>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.hydrate_payload(payload).await;
            metrics.record_operation(
                "hydrate_payload",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn hydrate_activity_map_result_manifest(
        &self,
        payload: durust::PayloadRef,
    ) -> BoxFuture<'static, durust::Result<durust::PayloadRef>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.hydrate_activity_map_result_manifest(payload).await;
            metrics.record_operation(
                "hydrate_activity_map_result_manifest",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn commit_workflow_task(
        &self,
        claim: durust::WorkflowTaskClaim,
        batch: durust::WorkflowTaskCommit,
    ) -> BoxFuture<'static, durust::Result<durust::CommitOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            metrics.record_workflow_task_commit_shape(&batch);
            let started = Instant::now();
            let result = inner.commit_workflow_task(claim, batch).await;
            let duration = started.elapsed();
            metrics.record_workflow_task_commit(duration);
            metrics.record_operation(
                "commit_workflow_task",
                duration,
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn commit_workflow_tasks(
        &self,
        batch: durust::WorkflowTaskCommitBatch,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::WorkflowTaskCommitBatchResult>>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        let input_items = batch.commits.len() as u64;
        for input in &batch.commits {
            metrics.record_workflow_task_commit_shape(&input.commit);
        }
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.commit_workflow_tasks(batch).await;
            let duration = started.elapsed();
            metrics.record_workflow_task_commit(duration);
            let items = result
                .as_ref()
                .map_or(input_items, |results| results.len() as u64);
            metrics.record_operation("commit_workflow_tasks", duration, items, result.is_ok());
            result
        })
    }

    fn release_workflow_task(
        &self,
        claim: durust::WorkflowTaskClaim,
        release: durust::WorkflowTaskRelease,
    ) -> BoxFuture<'static, durust::Result<()>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.release_workflow_task(claim, release).await;
            metrics.record_operation(
                "release_workflow_task",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn signal_workflow(
        &self,
        req: durust::SignalWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::SignalWorkflowOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.signal_workflow(req).await;
            metrics.record_operation(
                "signal_workflow",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn read_signal_inbox(
        &self,
        req: durust::ReadSignalInboxRequest,
    ) -> BoxFuture<'static, durust::Result<Option<durust::SignalInboxRecord>>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.read_signal_inbox(req).await;
            let items = result
                .as_ref()
                .ok()
                .and_then(|record| record.as_ref())
                .map_or(0, |_| 1);
            metrics.record_operation(
                "read_signal_inbox",
                started.elapsed(),
                items,
                result.is_ok(),
            );
            result
        })
    }

    fn read_signal_inboxes(
        &self,
        req: durust::ReadSignalInboxesRequest,
    ) -> BoxFuture<'static, durust::Result<Vec<Option<durust::SignalInboxRecord>>>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        let force_scalar_signal_reads = self.force_scalar_signal_reads;
        Box::pin(async move {
            if force_scalar_signal_reads {
                let mut records = Vec::with_capacity(req.requests.len());
                for request in req.requests {
                    let started = Instant::now();
                    let result = inner.read_signal_inbox(request).await;
                    let duration = started.elapsed();
                    let items = result
                        .as_ref()
                        .ok()
                        .and_then(|record| record.as_ref())
                        .map_or(0, |_| 1);
                    metrics.record_operation("read_signal_inbox", duration, items, result.is_ok());
                    records.push(result?);
                }
                return Ok(records);
            }

            let requested = req.requests.len() as u64;
            let started = Instant::now();
            let result = inner.read_signal_inboxes(req).await;
            metrics.record_operation(
                "read_signal_inboxes",
                started.elapsed(),
                requested,
                result.is_ok(),
            );
            result
        })
    }

    fn fire_due_timers(
        &self,
        req: durust::FireDueTimersRequest,
    ) -> BoxFuture<'static, durust::Result<durust::FireDueTimersOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.fire_due_timers(req).await;
            let items = result.as_ref().map_or(0, |outcome| outcome.fired as u64);
            metrics.record_operation("fire_due_timers", started.elapsed(), items, result.is_ok());
            result
        })
    }

    fn timeout_due_activities(
        &self,
        req: durust::TimeoutDueActivitiesRequest,
    ) -> BoxFuture<'static, durust::Result<durust::TimeoutDueActivitiesOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.timeout_due_activities(req).await;
            let items = result
                .as_ref()
                .map_or(0, |outcome| outcome.timed_out as u64);
            metrics.record_operation(
                "timeout_due_activities",
                started.elapsed(),
                items,
                result.is_ok(),
            );
            result
        })
    }

    fn run_due_maintenance(
        &self,
        req: durust::RunDueMaintenanceRequest,
    ) -> BoxFuture<'static, durust::Result<durust::RunDueMaintenanceOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.run_due_maintenance(req).await;
            let items = result.as_ref().map_or(0, |outcome| {
                outcome.timers_fired as u64 + outcome.activities_timed_out as u64
            });
            metrics.record_operation(
                "run_due_maintenance",
                started.elapsed(),
                items,
                result.is_ok(),
            );
            result
        })
    }

    fn claim_activity_task(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::ClaimActivityOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::ClaimedActivityTask>>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.claim_activity_task(worker_id, opts).await;
            let items = result
                .as_ref()
                .ok()
                .and_then(|task| task.as_ref())
                .map_or(0, |_| 1);
            metrics.record_operation(
                "claim_activity_task",
                started.elapsed(),
                items,
                result.is_ok(),
            );
            result
        })
    }

    fn claim_activity_tasks(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::ClaimActivityTasksOptions,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::ClaimedActivityTask>>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.claim_activity_tasks(worker_id, opts).await;
            let items = result.as_ref().map_or(0, |tasks| tasks.len() as u64);
            metrics.record_operation(
                "claim_activity_tasks",
                started.elapsed(),
                items,
                result.is_ok(),
            );
            result
        })
    }

    fn heartbeat_activity(
        &self,
        req: durust::ActivityHeartbeatRequest,
    ) -> BoxFuture<'static, durust::Result<durust::ActivityHeartbeatOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.heartbeat_activity(req).await;
            metrics.record_operation(
                "heartbeat_activity",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn complete_activity(
        &self,
        req: durust::CompleteActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::CompleteActivityOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.complete_activity(req).await;
            metrics.record_operation(
                "complete_activity",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn complete_activity_tasks(
        &self,
        req: durust::CompleteActivityTasksRequest,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::CompleteActivityTaskBatchResult>>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        let input_items = req.completions.len() as u64;
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.complete_activity_tasks(req).await;
            let items = result
                .as_ref()
                .map_or(input_items, |results| results.len() as u64);
            metrics.record_operation(
                "complete_activity_tasks",
                started.elapsed(),
                items,
                result.is_ok(),
            );
            result
        })
    }

    fn fail_activity(
        &self,
        req: durust::FailActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::FailActivityOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.fail_activity(req).await;
            metrics.record_operation(
                "fail_activity",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn dispatch_child_workflow_starts(
        &self,
        req: durust::DispatchChildWorkflowStartsRequest,
    ) -> BoxFuture<'static, durust::Result<durust::DispatchChildWorkflowStartsOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.dispatch_child_workflow_starts(req).await;
            let items = result
                .as_ref()
                .map_or(0, |outcome| outcome.dispatched as u64);
            metrics.record_operation(
                "dispatch_child_workflow_starts",
                started.elapsed(),
                items,
                result.is_ok(),
            );
            result
        })
    }

    fn query_projection(
        &self,
        req: durust::QueryProjectionRequest,
    ) -> BoxFuture<'static, durust::Result<durust::QueryProjectionOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.query_projection(req).await;
            metrics.record_operation(
                "query_projection",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn workflow_change_versions(
        &self,
        req: durust::WorkflowChangeVersionsRequest,
    ) -> BoxFuture<'static, durust::Result<durust::WorkflowChangeVersionsOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.workflow_change_versions(req).await;
            metrics.record_operation(
                "workflow_change_versions",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn payload_roots(&self) -> BoxFuture<'static, durust::Result<durust::PayloadRootsOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.payload_roots().await;
            metrics.record_operation(
                "payload_roots",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }

    fn gc_payload_blobs(
        &self,
        req: durust::PayloadGarbageCollectionRequest,
    ) -> BoxFuture<'static, durust::Result<durust::PayloadGarbageCollectionOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.gc_payload_blobs(req).await;
            metrics.record_operation(
                "gc_payload_blobs",
                started.elapsed(),
                success_item(&result),
                result.is_ok(),
            );
            result
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkflowInput {
    index: u64,
    activity_delay_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ActivityInput {
    value: u64,
    delay_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ParentOutput {
    index: u64,
    child_value: u64,
    signal_value: u64,
    finished: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ChildMapInput {
    index: u64,
    items: u64,
    max_in_flight: usize,
    activity_delay_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ChildMapOutput {
    index: u64,
    sum: u64,
    items: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SignalBatchInput {
    index: u64,
    width: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SignalBatchOutput {
    index: u64,
    values: Vec<u64>,
}

#[durust::activity(name = "bench.workload.activity")]
async fn benchmark_activity(input: ActivityInput) -> durust::Result<u64> {
    if input.delay_ms > 0 {
        thread::sleep(Duration::from_millis(input.delay_ms));
    }
    Ok(input.value)
}

#[durust::workflow(name = "bench.workload.child", version = 1)]
async fn benchmark_child(input: WorkflowInput) -> durust::Result<u64> {
    durust::call_activity!(benchmark_activity(ActivityInput {
        value: input.index * 10,
        delay_ms: input.activity_delay_ms,
    }))
    .task_queue(ACTIVITY_QUEUE)
    .await
}

#[durust::workflow(name = "bench.workload.parent", version = 1)]
async fn benchmark_parent(input: WorkflowInput) -> durust::Result<ParentOutput> {
    let _boot = durust::call_activity!(benchmark_activity(ActivityInput {
        value: input.index,
        delay_ms: input.activity_delay_ms,
    }))
    .task_queue(ACTIVITY_QUEUE)
    .await?;

    let child = durust::child!(benchmark_child(input.clone()))
        .workflow_id(format!("mixed-child-{}", input.index))
        .spawn()
        .await?;
    let child_value = child.result().await?;

    let signal_value = durust::signal::<u64>("finish").await?;
    durust::sleep(Duration::ZERO).await?;

    let _finish = durust::call_activity!(benchmark_activity(ActivityInput {
        value: input.index + 1,
        delay_ms: input.activity_delay_ms,
    }))
    .task_queue(ACTIVITY_QUEUE)
    .await?;

    Ok(ParentOutput {
        index: input.index,
        child_value,
        signal_value,
        finished: true,
    })
}

#[durust::workflow(name = "bench.workload.child-map-parent", version = 1)]
async fn benchmark_child_map_parent(input: ChildMapInput) -> durust::Result<ChildMapOutput> {
    let items = (0..input.items)
        .map(|offset| WorkflowInput {
            index: input.index.saturating_mul(1_000_000).saturating_add(offset),
            activity_delay_ms: input.activity_delay_ms,
        })
        .collect::<Vec<_>>();
    let input_manifest = durust::child_workflow_map_manifest(items)?;
    let mapped = durust::child_workflow_map::<benchmark_child>()
        .task_queue(WORKFLOW_QUEUE)
        .workflow_id_prefix(format!("child-map-{}", input.index))
        .input_manifest(input_manifest)
        .max_in_flight(input.max_in_flight)
        .result_manifest("child-map-results")
        .spawn()
        .await?;
    let result_manifest = mapped.result_manifest().await?;
    let result_refs = durust::decode_child_workflow_map_success_refs(&result_manifest)?;
    let sum = result_refs.iter().try_fold(0_u64, |sum, payload| {
        Ok(sum.saturating_add(durust::decode_payload::<u64>(payload)?))
    })?;
    Ok(ChildMapOutput {
        index: input.index,
        sum,
        items: input.items,
    })
}

#[durust::workflow(name = "bench.workload.signal-batch", version = 1)]
async fn benchmark_signal_batch(input: SignalBatchInput) -> durust::Result<SignalBatchOutput> {
    let signals = (0..input.width)
        .map(|offset| durust::signal::<u64>(format!("signal-{offset}")))
        .collect::<Vec<_>>();
    let values = durust::join_all(signals).await?;
    Ok(SignalBatchOutput {
        index: input.index,
        values,
    })
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let options = parse_args(env::args().skip(1))?;
    // Maintenance actions, not benchmarks: they need only the URL, and they
    // must not be reachable by accident from a benchmark invocation.
    if options.list_stale || options.drop_stale {
        let admin_url = env::var("DURUST_POSTGRES_URL").map_err(|_| {
            "set DURUST_POSTGRES_URL to list or drop stale benchmark databases".to_owned()
        })?;
        let runtime = tokio_runtime()?;
        return sweep_stale_benchmark_databases(&runtime, &admin_url, options.drop_stale);
    }
    let result = match options.backend {
        BenchmarkBackend::Memory => run_memory_benchmark(options.clone())?,
        BenchmarkBackend::Postgres => run_postgres_benchmark(options.clone())?,
        BenchmarkBackend::Sqlite => run_sqlite_benchmark(options.clone())?,
    };
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&result).map_err(|err| err.to_string())?
        );
    } else {
        print_text_result(&result);
    }
    Ok(())
}

fn default_options() -> BenchmarkOptions {
    BenchmarkOptions {
        backend: BenchmarkBackend::Sqlite,
        mode: "mixed".to_owned(),
        sqlite_layout: Some("single-file".to_owned()),
        workflows: DEFAULT_WORKFLOWS,
        workflow_offset: 0,
        workers: DEFAULT_WORKERS,
        shards: 1,
        physical_partitions: 1,
        activation_concurrency: 1,
        activation_prefetch_limit: 1,
        activity_delay_ms: 0,
        batch: DEFAULT_BATCH,
        activity_completion_batch: 1,
        worker_round_passes: None,
        child_map_items: 32,
        child_map_max_in_flight: 8,
        signal_batch_width: 8,
        max_rounds: DEFAULT_MAX_ROUNDS,
        keep_db: false,
        list_stale: false,
        drop_stale: false,
        sample_resources: false,
        force_scalar_signal_reads: false,
        postgres_pool_size: None,
        json: false,
    }
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<BenchmarkOptions, String> {
    let mut options = default_options();
    let mut args = args.into_iter();
    while let Some(raw) = args.next() {
        let (flag, inline_value) = raw
            .split_once('=')
            .map_or((raw.as_str(), None), |(flag, value)| (flag, Some(value)));
        match flag {
            "--backend" | "--provider" => {
                options.backend = parse_backend(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--mode" => {
                options.mode = next_arg(&mut args, flag, inline_value)?;
            }
            "--sqlite-layout" => {
                let sqlite_layout = next_arg(&mut args, flag, inline_value)?;
                if sqlite_layout != "single-file" {
                    return Err("--sqlite-layout currently supports only `single-file`".to_owned());
                }
                options.sqlite_layout = Some(sqlite_layout);
            }
            "--workflows" => {
                options.workflows =
                    parse_positive_u64(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--workflow-offset" => {
                options.workflow_offset =
                    parse_non_negative_u64(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--workers" => {
                options.workers =
                    parse_positive_usize(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--shards" => {
                options.shards =
                    parse_positive_u64(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--physical-partitions" => {
                options.physical_partitions =
                    parse_positive_u64(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--activation-concurrency" => {
                options.activation_concurrency =
                    parse_positive_u64(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--activation-prefetch-limit" => {
                options.activation_prefetch_limit =
                    parse_positive_u64(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--activity-delay-ms" => {
                options.activity_delay_ms =
                    parse_non_negative_u64(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--batch" => {
                options.batch =
                    parse_positive_usize(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--activity-completion-batch" => {
                options.activity_completion_batch =
                    parse_positive_usize(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--worker-round-passes" => {
                options.worker_round_passes = Some(parse_positive_usize(
                    next_arg(&mut args, flag, inline_value)?,
                    flag,
                )?);
            }
            "--child-map-items" => {
                options.child_map_items =
                    parse_positive_u64(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--child-map-max-in-flight" => {
                options.child_map_max_in_flight =
                    parse_positive_usize(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--signal-batch-width" => {
                options.signal_batch_width =
                    parse_positive_usize(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--max-rounds" => {
                options.max_rounds =
                    parse_positive_usize(next_arg(&mut args, flag, inline_value)?, flag)?;
            }
            "--postgres-pool-size" => {
                options.postgres_pool_size = Some(parse_positive_usize(
                    next_arg(&mut args, flag, inline_value)?,
                    flag,
                )?);
            }
            "--keep-db" => options.keep_db = true,
            "--list-stale" => options.list_stale = true,
            "--drop-stale" => options.drop_stale = true,
            "--sample-resources" => options.sample_resources = true,
            "--force-scalar-signal-reads" => options.force_scalar_signal_reads = true,
            "--json" => options.json = true,
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    validate_supported_dimensions(&options)?;
    Ok(options)
}

fn validate_supported_dimensions(options: &BenchmarkOptions) -> Result<(), String> {
    if options.backend != BenchmarkBackend::Postgres && options.shards != 1 {
        return Err("--shards above 1 currently requires --backend postgres".to_owned());
    }
    if options.backend != BenchmarkBackend::Postgres && options.physical_partitions != 1 {
        return Err(
            "--physical-partitions above 1 currently requires --backend postgres".to_owned(),
        );
    }
    if options.backend == BenchmarkBackend::Memory && options.keep_db {
        return Err("--keep-db is only meaningful for persistent backends".to_owned());
    }
    if options.backend != BenchmarkBackend::Postgres
        && options.mode != "mixed"
        && options.mode != "child-map"
        && options.mode != "signal-batch"
    {
        return Err("only --backend postgres supports diagnostic modes".to_owned());
    }
    if options.force_scalar_signal_reads && options.mode != "signal-batch" {
        return Err(
            "--force-scalar-signal-reads is only supported with --mode signal-batch".to_owned(),
        );
    }
    match options.mode.as_str() {
        "mixed" | "child-map" | "signal-batch" | "postgres-write-ceiling" => {}
        other => {
            return Err(format!(
                "unsupported benchmark mode `{other}`; expected mixed, child-map, signal-batch, or postgres-write-ceiling"
            ));
        }
    }
    Ok(())
}

fn next_arg(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
    inline_value: Option<&str>,
) -> Result<String, String> {
    match inline_value {
        Some(value) if !value.is_empty() => Ok(value.to_owned()),
        Some(_) => Err(format!("{flag} requires a value")),
        None => args
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("{flag} requires a value")),
    }
}

fn parse_backend(value: String, flag: &str) -> Result<BenchmarkBackend, String> {
    match value.as_str() {
        "memory" => Ok(BenchmarkBackend::Memory),
        "postgres" => Ok(BenchmarkBackend::Postgres),
        "sqlite" => Ok(BenchmarkBackend::Sqlite),
        _ => Err(format!("{flag} must be memory, postgres, or sqlite")),
    }
}

fn parse_positive_u64(value: String, flag: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be a positive integer"));
    }
    Ok(parsed)
}

fn parse_non_negative_u64(value: String, flag: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn parse_positive_usize(value: String, flag: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be a positive integer"));
    }
    Ok(parsed)
}

fn usage() -> String {
    format!(
        "usage: durust-benchmark-workload [--backend sqlite|memory|postgres] [--mode mixed|child-map|signal-batch|postgres-write-ceiling] \
         [--workflows {DEFAULT_WORKFLOWS}] [--workers {DEFAULT_WORKERS}] [--shards 1] \
         [--physical-partitions 1] [--activation-concurrency 1] \
         [--activation-prefetch-limit 1] \
         [--batch {DEFAULT_BATCH}] [--activity-completion-batch 1] [--worker-round-passes {DEFAULT_WORKER_ROUND_PASSES}] \
         [--child-map-items 32] [--child-map-max-in-flight 8] [--signal-batch-width 8] \
         [--force-scalar-signal-reads] \
         [--max-rounds {DEFAULT_MAX_ROUNDS}] [--keep-db] [--sample-resources] [--json] \
         [--list-stale] [--drop-stale]\n\n\
         --list-stale lists unused databases this program generated and left behind; \
         --drop-stale reclaims them. Both are server-wide, not per-run: they consider every \
         such database on the server. Neither touches one that has a backend attached, and \
         --drop-stale never evicts a session, but an idle --keep-db database is indistinguishable \
         from a leak and will be reclaimed. On a shared server run --list-stale first."
    )
}

fn run_memory_benchmark(mut options: BenchmarkOptions) -> Result<BenchmarkResult, String> {
    options.sqlite_layout = None;
    let runtime = tokio_runtime()?;
    run_backend_benchmark(&runtime, MemoryBackend::new(), options, None, None)
}

fn run_sqlite_benchmark(mut options: BenchmarkOptions) -> Result<BenchmarkResult, String> {
    options.sqlite_layout = Some(
        options
            .sqlite_layout
            .clone()
            .unwrap_or_else(|| "single-file".to_owned()),
    );
    let runtime = tokio_runtime()?;
    let dir = tempfile::tempdir().map_err(|err| err.to_string())?;
    let db_path = dir.path().join("durust-benchmark.sqlite3");
    let backend = SqliteBackend::open(&db_path).map_err(|err| err.to_string())?;
    let result = run_backend_benchmark(
        &runtime,
        backend,
        options.clone(),
        Some(db_path.clone()),
        None,
    )?;
    if options.keep_db {
        let kept = env::current_dir()
            .map_err(|err| err.to_string())?
            .join(format!("durust-benchmark-{}.sqlite3", std::process::id()));
        fs::copy(&db_path, &kept).map_err(|err| err.to_string())?;
        let mut kept_result = result;
        kept_result.db_path = Some(kept.display().to_string());
        kept_result.db_bytes = sqlite_store_bytes(&kept).ok();
        Ok(kept_result)
    } else {
        Ok(result)
    }
}

fn run_postgres_benchmark(mut options: BenchmarkOptions) -> Result<BenchmarkResult, String> {
    options.sqlite_layout = None;
    let admin_url = env::var("DURUST_POSTGRES_URL")
        .map_err(|_| "set DURUST_POSTGRES_URL to run the Postgres benchmark workload".to_owned())?;
    let schema = postgres_benchmark_schema();
    let pool_size = options
        .postgres_pool_size
        .unwrap_or_else(|| options.workers.saturating_add(2).max(1));
    options.postgres_pool_size = Some(pool_size);
    let logical_shards = u32::try_from(options.shards)
        .map_err(|_| format!("--shards {} does not fit u32", options.shards))?;
    let physical_partitions = u32::try_from(options.physical_partitions).map_err(|_| {
        format!(
            "--physical-partitions {} does not fit u32",
            options.physical_partitions
        )
    })?;

    let runtime = tokio_runtime()?;

    // The run gets a database of its own, not just a schema of its own.
    //
    // `postgres_stats_snapshot` reads `pg_stat_database where datname =
    // current_database()` and `pg_stat_statements where dbid =
    // current_database()`, both of which count *every* client's work on that
    // database — while `mixed_actions`, the denominator of
    // `transactions_per_mixed_action`, counts only this run's. Sharing a
    // database therefore reports someone else's transactions as this run's:
    // measured, a sustained foreign load of 60,000 transactions moved the ratio
    // from 2.775 to 77.519, while after this change three contaminated runs
    // read 2.503 / 2.475 / 2.471 against quiet runs of 2.433 / 2.516 / 2.914.
    //
    // Detecting the contamination was the alternative and is strictly worse: a
    // stat that is exclusive by construction cannot be wrong, while a detector
    // has to be right about who else is connected. A per-run schema does not
    // achieve this, because no `pg_stat_*` view is per-schema — a per-run
    // database is the smallest unit that works.
    //
    // **Scope, because an earlier version of this comment overstated it.** This
    // fixes the numbers *this binary* prints. It does not fix any gate:
    // `max_postgres_transactions_per_mixed_action_ratio: 1.2` in
    // `postgres-mixed-accepted.json` is evaluated by
    // `packages/benchmark/test/thresholds.test.ts` against the **TypeScript**
    // benchmark, and nothing — not `check-postgres.mjs`, not any test — runs
    // this program and compares its output to a baseline. That benchmark builds
    // a per-run *table name* on the shared `DURUST_POSTGRES_URL`
    // (`packages/benchmark/src/index.ts:1503`) and reads `from
    // pg_stat_database where datname = current_database()`
    // (`packages/postgres/src/index.ts:409`), so the gated path is contaminated
    // exactly as it was before this change, and needs the same fix applied
    // there.
    let database = postgres_benchmark_database();
    create_postgres_database(&runtime, &admin_url, &database)?;
    // Named on stderr before anything can fail, so a run killed by a signal —
    // the one exit the guard below cannot cover — leaves the operator the name.
    eprintln!("postgres benchmark: running in database `{database}`");
    // Declared after `runtime` so it drops before it, and after nothing else,
    // so every early return below unwinds through it.
    let mut database_guard =
        BenchmarkDatabaseGuard::new(&runtime, admin_url.clone(), database.clone());
    let database_url = postgres_url_with_database(&admin_url, &database)?;

    // Dropped before the guard, so the pool's connections are gone before the
    // database is.
    let backend = runtime
        .block_on(PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(database_url.clone())
                .schema(schema.clone())
                .max_pool_size(pool_size)
                .logical_shards(logical_shards)
                .physical_partitions(physical_partitions),
        ))
        .map_err(|err| err.to_string())?;

    let mut result = if options.mode == "postgres-write-ceiling" {
        run_postgres_write_ceiling_benchmark(&runtime, &database_url, &schema, options)?
    } else {
        run_backend_benchmark(&runtime, backend, options, None, Some(database_url.clone()))?
    };
    result.postgres_schema = Some(schema.clone());
    result.postgres_database = Some(database.clone());
    if result.options.keep_db {
        database_guard.disarm();
    } else {
        // Explicit on the success path, and only then disarmed, because `Drop`
        // cannot return a `Result`: leaving cleanup to the guard here would let
        // a completed run exit 0 having leaked a database, reporting the
        // failure on stderr and nowhere the exit code can see. The guard still
        // covers what it was added for — panics and early returns — and this
        // restores the one property the closure it replaced did have.
        drop_postgres_database(&runtime, &admin_url, &database, DropDisposition::Owned)?;
        database_guard.disarm();
    }
    Ok(result)
}

fn postgres_stats_report(
    before: PostgresStatsSnapshot,
    after: PostgresStatsSnapshot,
    processing_seconds: f64,
    completed_workflows: u64,
    mixed_actions: u64,
    activity_samples: Option<PostgresActivitySamplesReport>,
    statement_stats: Option<PostgresStatementStatsReport>,
    statement_stats_unavailable: Option<String>,
    wal_stats_shared_with_other_databases: Option<String>,
) -> PostgresStatsReport {
    let wal_bytes = after.wal_bytes.saturating_sub(before.wal_bytes);
    let xact_commit = after.xact_commit.saturating_sub(before.xact_commit);
    let xact_rollback = after.xact_rollback.saturating_sub(before.xact_rollback);
    let blocks_read = after.blocks_read.saturating_sub(before.blocks_read);
    let blocks_hit = after.blocks_hit.saturating_sub(before.blocks_hit);
    let wal_records = after.wal_records.saturating_sub(before.wal_records);
    PostgresStatsReport {
        wal_bytes,
        wal_bytes_per_second: wal_bytes as f64 / processing_seconds,
        wal_records,
        wal_records_per_second: wal_records as f64 / processing_seconds,
        wal_fpi: after.wal_fpi.saturating_sub(before.wal_fpi),
        wal_buffers_full: after
            .wal_buffers_full
            .saturating_sub(before.wal_buffers_full),
        wal_write: after.wal_write.saturating_sub(before.wal_write),
        wal_sync: after.wal_sync.saturating_sub(before.wal_sync),
        wal_write_time_ms: (after.wal_write_time_ms - before.wal_write_time_ms).max(0.0),
        wal_sync_time_ms: (after.wal_sync_time_ms - before.wal_sync_time_ms).max(0.0),
        xact_commit,
        xact_rollback,
        transactions_per_second: (xact_commit + xact_rollback) as f64 / processing_seconds,
        transactions_per_mixed_action: ratio_u64(xact_commit + xact_rollback, mixed_actions),
        transactions_per_workflow: ratio_u64(xact_commit + xact_rollback, completed_workflows),
        rows_returned: after.rows_returned.saturating_sub(before.rows_returned),
        rows_fetched: after.rows_fetched.saturating_sub(before.rows_fetched),
        rows_inserted: after.rows_inserted.saturating_sub(before.rows_inserted),
        rows_updated: after.rows_updated.saturating_sub(before.rows_updated),
        rows_deleted: after.rows_deleted.saturating_sub(before.rows_deleted),
        blocks_read,
        blocks_hit,
        block_cache_hit_ratio: if blocks_read + blocks_hit == 0 {
            0.0
        } else {
            blocks_hit as f64 / (blocks_read + blocks_hit) as f64
        },
        temp_files: after.temp_files.saturating_sub(before.temp_files),
        temp_bytes: after.temp_bytes.saturating_sub(before.temp_bytes),
        deadlocks: after.deadlocks.saturating_sub(before.deadlocks),
        block_read_time_ms: (after.block_read_time_ms - before.block_read_time_ms).max(0.0),
        block_write_time_ms: (after.block_write_time_ms - before.block_write_time_ms).max(0.0),
        active_time_ms: (after.active_time_ms - before.active_time_ms).max(0.0),
        session_time_ms: (after.session_time_ms - before.session_time_ms).max(0.0),
        active_connections_after: after.active_connections,
        activity_samples,
        statement_stats,
        statement_stats_unavailable,
        wal_stats_shared_with_other_databases,
    }
}

fn run_postgres_write_ceiling_benchmark(
    runtime: &tokio::runtime::Runtime,
    database_url: &str,
    schema: &str,
    options: BenchmarkOptions,
) -> Result<BenchmarkResult, String> {
    let resource_sampler = options
        .sample_resources
        .then(|| ResourceSampler::start(Duration::from_millis(100)))
        .flatten();
    let setup_started = Instant::now();
    let operations = postgres_write_ceiling_operations(&options)?;
    runtime.block_on(setup_postgres_write_ceiling(
        database_url,
        schema,
        &options,
        operations,
    ))?;
    let setup_finished = Instant::now();

    let postgres_stats_before = postgres_stats_snapshot(runtime, database_url).ok();
    let (statement_stats_before, statement_stats_before_unavailable) =
        statement_stats_or_reason(postgres_statement_stats_snapshot(runtime, database_url));
    let foreign_backends_before = postgres_foreign_backends(runtime, database_url).unwrap_or(0);
    let activity_sampler =
        PostgresActivitySampler::start(database_url.to_owned(), Duration::from_millis(100));

    let processing_started = Instant::now();
    runtime.block_on(run_postgres_write_ceiling_operations(
        database_url,
        schema,
        &options,
        operations,
    ))?;
    let processing_finished = Instant::now();

    let activity_samples = activity_sampler.and_then(|sampler| sampler.stop().ok());
    let foreign_backends_after = postgres_foreign_backends(runtime, database_url).unwrap_or(0);
    let (statement_stats_after, statement_stats_after_unavailable) =
        statement_stats_or_reason(postgres_statement_stats_snapshot(runtime, database_url));
    let postgres_stats_after = postgres_stats_snapshot(runtime, database_url).ok();
    let statement_stats_unavailable = report_statement_stats_unavailable(
        statement_stats_before_unavailable,
        statement_stats_after_unavailable,
    );
    let wal_stats_shared =
        wal_stats_shared_message(foreign_backends_before.max(foreign_backends_after));

    let verify_started = processing_finished;
    let committed = runtime.block_on(verify_postgres_write_ceiling(
        database_url,
        schema,
        operations,
    ))?;
    let verify_finished = Instant::now();

    let elapsed_ms = duration_ms(verify_finished.duration_since(setup_started));
    let setup_ms = duration_ms(setup_finished.duration_since(setup_started));
    let processing_ms = duration_ms(processing_finished.duration_since(processing_started));
    let verify_ms = duration_ms(verify_finished.duration_since(verify_started));
    let elapsed_seconds = (elapsed_ms / 1_000.0).max(f64::EPSILON);
    let processing_seconds = (processing_ms / 1_000.0).max(f64::EPSILON);
    let operations_u64 = u64::try_from(operations).unwrap_or(u64::MAX);
    let completed_workflows = options.workflows;
    let resource_samples = resource_sampler.and_then(|sampler| sampler.stop().ok());
    let mut result = BenchmarkResult {
        backend: options.backend.as_str().to_owned(),
        mode: options.mode.clone(),
        correct: committed == operations_u64,
        sqlite_layout: None,
        options,
        elapsed_ms,
        setup_ms,
        processing_ms,
        verify_ms,
        rounds: 1,
        activations: operations_u64,
        completed_workflows,
        mixed_actions: operations_u64,
        activations_per_second: operations_u64 as f64 / elapsed_seconds,
        mixed_actions_per_second: operations_u64 as f64 / elapsed_seconds,
        workflows_per_second: completed_workflows as f64 / elapsed_seconds,
        processing_activations_per_second: operations_u64 as f64 / processing_seconds,
        processing_mixed_actions_per_second: operations_u64 as f64 / processing_seconds,
        processing_workflows_per_second: completed_workflows as f64 / processing_seconds,
        counters: BenchmarkCounters {
            workflow_tasks: operations_u64,
            ..BenchmarkCounters::default()
        },
        worker_stats: WorkerStatsReport {
            workflow_tasks: operations_u64,
            ..WorkerStatsReport::default()
        },
        backend_metrics: BackendMetricsReport::default(),
        processing_backend_metrics: BackendMetricsReport::default(),
        postgres_stats: None,
        resource_samples,
        postgres_schema: Some(schema.to_owned()),
        postgres_database: None,
        db_path: None,
        db_bytes: None,
    };
    if !result.correct {
        return Err(format!(
            "postgres write ceiling committed {committed}/{operations_u64} operations"
        ));
    }
    if let (Some(before), Some(after)) = (postgres_stats_before, postgres_stats_after) {
        result.postgres_stats = Some(postgres_stats_report(
            before,
            after,
            processing_seconds,
            result.completed_workflows,
            result.mixed_actions,
            activity_samples,
            statement_stats_before
                .zip(statement_stats_after)
                .map(|(before, after)| {
                    postgres_statement_stats_report(
                        before,
                        after,
                        result.completed_workflows,
                        result.mixed_actions,
                    )
                }),
            statement_stats_unavailable,
            wal_stats_shared,
        ));
    }
    Ok(result)
}

fn postgres_write_ceiling_operations(options: &BenchmarkOptions) -> Result<usize, String> {
    let workflows = usize::try_from(options.workflows)
        .map_err(|_| format!("workflow count {} does not fit usize", options.workflows))?;
    workflows
        .checked_mul(8)
        .ok_or_else(|| "postgres write ceiling operation count overflowed".to_owned())
}

async fn setup_postgres_write_ceiling(
    database_url: &str,
    schema: &str,
    options: &BenchmarkOptions,
    operations: usize,
) -> Result<(), String> {
    let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
        .await
        .map_err(|err| err.to_string())?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres write ceiling setup connection error: {err}");
        }
    });
    let schema = quote_benchmark_ident(schema);
    let operations_i64 = i64::try_from(operations).unwrap_or(i64::MAX);
    let shards_i32 = i32::try_from(options.shards).unwrap_or(i32::MAX).max(1);
    client
        .execute(
            &format!(
                "insert into {schema}.workflow_instances
                 (namespace, workflow_id, run_id, shard_id, workflow_name, workflow_version,
                  task_queue, current_event_id, ready_reason, ready_at_ms, workflow_claim_token,
                  terminal)
                 select 'default',
                        'raw-workflow-' || g::text,
                        'raw-run-' || g::text,
                        (g % $2::bigint)::integer,
                        'raw.workflow',
                        1,
                        $3,
                        1,
                        null,
                        0,
                        g + 1,
                        false
                 from generate_series(0::bigint, $1::bigint - 1) as g"
            ),
            &[
                &operations_i64,
                &i64::from(shards_i32),
                &WORKFLOW_QUEUE.to_owned(),
            ],
        )
        .await
        .map_err(|err| err.to_string())?;
    let lease_until = i64::MAX / 2;
    client
        .execute(
            &format!(
                "update {schema}.shard_leases
                 set owner_id = 'raw-writer',
                     lease_until_ms = $1,
                     lease_epoch = greatest(lease_epoch, 1)"
            ),
            &[&lease_until],
        )
        .await
        .map_err(|err| err.to_string())?;
    Ok(())
}

async fn run_postgres_write_ceiling_operations(
    database_url: &str,
    schema: &str,
    options: &BenchmarkOptions,
    operations: usize,
) -> Result<(), String> {
    let workers = options.workers.max(1);
    let futures = (0..workers).map(|worker_index| {
        run_postgres_write_ceiling_worker(
            database_url.to_owned(),
            schema.to_owned(),
            worker_index,
            workers,
            operations,
        )
    });
    let results = futures::future::join_all(futures).await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn run_postgres_write_ceiling_worker(
    database_url: String,
    schema: String,
    worker_index: usize,
    workers: usize,
    operations: usize,
) -> Result<(), String> {
    let (mut client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .map_err(|err| err.to_string())?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres write ceiling worker connection error: {err}");
        }
    });
    let schema = quote_benchmark_ident(&schema);
    let payload_a = vec![1_u8; 64];
    let payload_b = vec![2_u8; 64];
    for operation in (worker_index..operations).step_by(workers) {
        let run_id = format!("raw-run-{operation}");
        let tx = client.transaction().await.map_err(|err| err.to_string())?;
        let row = tx
            .query_one(
                &format!(
                    "select current_event_id, workflow_claim_token, shard_id
                     from {schema}.workflow_instances
                     where run_id = $1
                     for update"
                ),
                &[&run_id],
            )
            .await
            .map_err(|err| err.to_string())?;
        let current_event_id: i64 = row.get(0);
        let shard_id: i32 = row.get(2);
        tx.query_one(
            &format!(
                "select owner_id, lease_until_ms, lease_epoch
                 from {schema}.shard_leases
                 where shard_id = $1"
            ),
            &[&shard_id],
        )
        .await
        .map_err(|err| err.to_string())?;
        let event_ids = vec![current_event_id + 1, current_event_id + 2];
        let event_types = vec!["raw_event_a".to_owned(), "raw_event_b".to_owned()];
        let payloads = vec![payload_a.clone(), payload_b.clone()];
        tx.execute(
            &format!(
                "insert into {schema}.history_events(run_id, event_id, event_type, data)
                 select $1, event_id, event_type, data
                 from unnest($2::bigint[], $3::text[], $4::bytea[])
                      as event_rows(event_id, event_type, data)"
            ),
            &[&run_id, &event_ids, &event_types, &payloads],
        )
        .await
        .map_err(|err| err.to_string())?;
        tx.execute(
            &format!(
                "update {schema}.workflow_instances
                 set current_event_id = $1,
                     workflow_claim_token = null,
                     terminal = false,
                     ready_reason = null,
                     ready_at_ms = 0
                 where run_id = $2"
            ),
            &[&(current_event_id + 2), &run_id],
        )
        .await
        .map_err(|err| err.to_string())?;
        tx.commit().await.map_err(|err| err.to_string())?;
    }
    Ok(())
}

async fn verify_postgres_write_ceiling(
    database_url: &str,
    schema: &str,
    operations: usize,
) -> Result<u64, String> {
    let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
        .await
        .map_err(|err| err.to_string())?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres write ceiling verify connection error: {err}");
        }
    });
    let schema = quote_benchmark_ident(schema);
    let operations_i64 = i64::try_from(operations).unwrap_or(i64::MAX);
    let committed = client
        .query_one(
            &format!(
                "select count(*)
                 from {schema}.workflow_instances
                 where run_id like 'raw-run-%'
                   and current_event_id = 3
                   and workflow_claim_token is null"
            ),
            &[],
        )
        .await
        .map_err(|err| err.to_string())?
        .get::<_, i64>(0);
    if committed != operations_i64 {
        return Ok(u64::try_from(committed).unwrap_or(0));
    }
    Ok(u64::try_from(committed).unwrap_or(0))
}

fn quote_benchmark_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn run_backend_benchmark<B>(
    runtime: &tokio::runtime::Runtime,
    backend: B,
    options: BenchmarkOptions,
    db_path: Option<PathBuf>,
    postgres_stats_database_url: Option<String>,
) -> Result<BenchmarkResult, String>
where
    B: DurableBackend,
{
    let metrics = BackendMetrics::default();
    let backend = MeasuredBackend::new(backend, metrics.clone(), options.force_scalar_signal_reads);
    let resource_sampler = options
        .sample_resources
        .then(|| ResourceSampler::start(Duration::from_millis(100)))
        .flatten();
    let setup_started = Instant::now();
    let start_outcome = runtime.block_on(start_workflows(&backend, &options))?;
    let setup_finished = Instant::now();
    let setup_metrics = metrics.snapshot();
    let postgres_stats_before = postgres_stats_database_url
        .as_deref()
        .and_then(|database_url| postgres_stats_snapshot(runtime, database_url).ok());
    let (statement_stats_before, statement_stats_before_unavailable) =
        match postgres_stats_database_url.as_deref() {
            Some(database_url) => {
                statement_stats_or_reason(postgres_statement_stats_snapshot(runtime, database_url))
            }
            None => (None, None),
        };
    let foreign_backends_before = postgres_stats_database_url
        .as_deref()
        .and_then(|database_url| postgres_foreign_backends(runtime, database_url).ok())
        .unwrap_or(0);
    let activity_sampler = postgres_stats_database_url
        .clone()
        .and_then(|database_url| {
            PostgresActivitySampler::start(database_url, Duration::from_millis(100))
        });

    let shared_worker_runtime = options.backend == BenchmarkBackend::Postgres;
    let mut workers = (0..options.workers)
        .map(|worker_index| {
            benchmark_worker_slot(
                backend.clone(),
                worker_index,
                shared_worker_runtime,
                &options,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    let processing_started = setup_finished;
    let mut rounds = 0;
    let mut stats = WorkerRunStats::default();
    let nominal_workflow_tasks = nominal_workflow_task_target(&options)?;
    loop {
        if rounds >= options.max_rounds {
            return Err(format!(
                "benchmark did not reach nominal workflow task target after {} rounds: {}/{}",
                options.max_rounds, stats.workflow_tasks, nominal_workflow_tasks
            ));
        }
        rounds += 1;
        let round_stats = drain_worker_round(
            runtime,
            &mut workers,
            options
                .worker_round_passes
                .unwrap_or(DEFAULT_WORKER_ROUND_PASSES.min(options.batch).max(1)),
            shared_worker_runtime,
        )?;
        let made_progress = round_stats != WorkerRunStats::default();
        stats = add_worker_stats(stats, round_stats);
        if stats.workflow_tasks >= nominal_workflow_tasks {
            break;
        }
        if !made_progress {
            match runtime.block_on(verify_completed_workflows(
                &backend,
                &start_outcome.runs,
                &options,
            )) {
                Ok(completed_workflows) if completed_workflows == options.workflows => break,
                Ok(completed_workflows) => {
                    return Err(format!(
                        "benchmark stalled after {rounds} rounds: {}/{} workflow tasks processed, {completed_workflows}/{} workflows completed",
                        stats.workflow_tasks, nominal_workflow_tasks, options.workflows
                    ));
                }
                Err(err) => {
                    return Err(format!(
                        "benchmark stalled after {rounds} rounds: {}/{} workflow tasks processed: {err}",
                        stats.workflow_tasks, nominal_workflow_tasks
                    ));
                }
            }
        }
    }
    let processing_finished = Instant::now();
    let processing_metrics = metrics.snapshot();
    let activity_samples = activity_sampler.and_then(|sampler| sampler.stop().ok());
    let (statement_stats_after, statement_stats_after_unavailable) =
        match postgres_stats_database_url.as_deref() {
            Some(database_url) => {
                statement_stats_or_reason(postgres_statement_stats_snapshot(runtime, database_url))
            }
            None => (None, None),
        };
    let foreign_backends_after = postgres_stats_database_url
        .as_deref()
        .and_then(|database_url| postgres_foreign_backends(runtime, database_url).ok())
        .unwrap_or(0);
    let statement_stats_unavailable = report_statement_stats_unavailable(
        statement_stats_before_unavailable,
        statement_stats_after_unavailable,
    );
    let wal_stats_shared =
        wal_stats_shared_message(foreign_backends_before.max(foreign_backends_after));
    let postgres_stats_after = postgres_stats_database_url
        .as_deref()
        .and_then(|database_url| postgres_stats_snapshot(runtime, database_url).ok());

    let verify_started = processing_finished;
    let completed_workflows = runtime.block_on(verify_completed_workflows(
        &backend,
        &start_outcome.runs,
        &options,
    ))?;
    if completed_workflows != options.workflows {
        return Err(format!(
            "benchmark completed {completed_workflows}/{} workflows",
            options.workflows
        ));
    }
    assert_workload_stats(&stats, &options)?;
    let verify_finished = Instant::now();

    let counters = counters_from_stats(&start_outcome, &stats, &options);
    let elapsed_ms = duration_ms(verify_finished.duration_since(setup_started));
    let setup_ms = duration_ms(setup_finished.duration_since(setup_started));
    let processing_ms = duration_ms(processing_finished.duration_since(processing_started));
    let verify_ms = duration_ms(verify_finished.duration_since(verify_started));
    let elapsed_seconds = (elapsed_ms / 1_000.0).max(f64::EPSILON);
    let processing_seconds = (processing_ms / 1_000.0).max(f64::EPSILON);
    let activations = counters.workflow_tasks;
    let mixed_actions = mixed_action_count(&counters);
    let db_bytes = db_path
        .as_ref()
        .and_then(|path| sqlite_store_bytes(path).ok())
        .filter(|_| options.keep_db);
    let resource_samples = resource_sampler.and_then(|sampler| sampler.stop().ok());
    let mut result = BenchmarkResult {
        backend: options.backend.as_str().to_owned(),
        mode: options.mode.clone(),
        correct: true,
        sqlite_layout: options.sqlite_layout.clone(),
        options,
        elapsed_ms,
        setup_ms,
        processing_ms,
        verify_ms,
        rounds,
        activations,
        completed_workflows,
        mixed_actions,
        activations_per_second: activations as f64 / elapsed_seconds,
        mixed_actions_per_second: mixed_actions as f64 / elapsed_seconds,
        workflows_per_second: completed_workflows as f64 / elapsed_seconds,
        processing_activations_per_second: activations as f64 / processing_seconds,
        processing_mixed_actions_per_second: mixed_actions as f64 / processing_seconds,
        processing_workflows_per_second: completed_workflows as f64 / processing_seconds,
        counters,
        worker_stats: WorkerStatsReport::from(stats),
        backend_metrics: metrics.report(mixed_actions),
        processing_backend_metrics: metrics.report_between(
            &setup_metrics,
            &processing_metrics,
            mixed_actions,
        ),
        postgres_stats: None,
        resource_samples,
        postgres_schema: None,
        postgres_database: None,
        db_path: None,
        db_bytes,
    };
    if let (Some(before), Some(after)) = (postgres_stats_before, postgres_stats_after) {
        result.postgres_stats = Some(postgres_stats_report(
            before,
            after,
            processing_seconds,
            result.completed_workflows,
            result.mixed_actions,
            activity_samples,
            statement_stats_before
                .zip(statement_stats_after)
                .map(|(before, after)| {
                    postgres_statement_stats_report(
                        before,
                        after,
                        result.completed_workflows,
                        result.mixed_actions,
                    )
                }),
            statement_stats_unavailable,
            wal_stats_shared,
        ));
    }
    Ok(result)
}

fn nominal_workflow_task_target(options: &BenchmarkOptions) -> Result<usize, String> {
    let workflows = usize::try_from(options.workflows)
        .map_err(|_| format!("workflow count {} does not fit usize", options.workflows))?;
    let per_workflow = match options.mode.as_str() {
        "mixed" => 8,
        "signal-batch" => 1,
        "child-map" => {
            let items = usize::try_from(options.child_map_items).map_err(|_| {
                format!(
                    "--child-map-items {} does not fit usize",
                    options.child_map_items
                )
            })?;
            items
                .checked_mul(2)
                .and_then(|tasks| tasks.checked_add(2))
                .ok_or_else(|| "nominal child-map workflow task count overflowed".to_owned())?
        }
        other => return Err(format!("unsupported benchmark mode `{other}`")),
    };
    workflows
        .checked_mul(per_workflow)
        .ok_or_else(|| "nominal workflow task count overflowed".to_owned())
}

struct StartOutcome {
    runs: Vec<ExpectedRun>,
    workflow_starts: u64,
    signals: u64,
}

struct ExpectedRun {
    run_id: RunId,
    index: u64,
}

async fn start_workflows<B>(backend: &B, options: &BenchmarkOptions) -> Result<StartOutcome, String>
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let mut runs = Vec::with_capacity(options.workflows as usize);
    let mut signals = 0_u64;
    for local_index in 0..options.workflows {
        let index = options.workflow_offset + local_index;
        let workflow_id = format!("{}-bench-{index}", options.mode);
        let run_id = match options.mode.as_str() {
            "mixed" => {
                let input = WorkflowInput {
                    index,
                    activity_delay_ms: options.activity_delay_ms,
                };
                let run_id = client
                    .start_workflow::<benchmark_parent>(&workflow_id, WORKFLOW_QUEUE, input)
                    .await
                    .map_err(|err| err.to_string())?;
                client
                    .signal_workflow(
                        &workflow_id,
                        "finish",
                        format!("signal-{index}"),
                        index + 1_000,
                    )
                    .await
                    .map_err(|err| err.to_string())?;
                signals += 1;
                run_id
            }
            "child-map" => {
                let input = ChildMapInput {
                    index,
                    items: options.child_map_items,
                    max_in_flight: options.child_map_max_in_flight,
                    activity_delay_ms: options.activity_delay_ms,
                };
                client
                    .start_workflow::<benchmark_child_map_parent>(
                        &workflow_id,
                        WORKFLOW_QUEUE,
                        input,
                    )
                    .await
                    .map_err(|err| err.to_string())?
            }
            "signal-batch" => {
                let input = SignalBatchInput {
                    index,
                    width: options.signal_batch_width,
                };
                let run_id = client
                    .start_workflow::<benchmark_signal_batch>(&workflow_id, WORKFLOW_QUEUE, input)
                    .await
                    .map_err(|err| err.to_string())?;
                for offset in 0..options.signal_batch_width {
                    client
                        .signal_workflow(
                            &workflow_id,
                            format!("signal-{offset}"),
                            format!("signal-batch-{index}-{offset}"),
                            signal_batch_value(index, offset),
                        )
                        .await
                        .map_err(|err| err.to_string())?;
                    signals += 1;
                }
                run_id
            }
            other => return Err(format!("unsupported benchmark mode `{other}`")),
        };
        runs.push(ExpectedRun { run_id, index });
    }
    Ok(StartOutcome {
        runs,
        workflow_starts: options.workflows,
        signals,
    })
}

fn benchmark_worker_slot<B>(
    backend: B,
    worker_index: usize,
    shared_worker_runtime: bool,
    options: &BenchmarkOptions,
) -> Result<WorkerSlot<B>, String>
where
    B: DurableBackend,
{
    let mut builder = Worker::builder(backend)
        .worker_id(format!("durust-benchmark-worker-{worker_index}"))
        .workflow_task_queue(WORKFLOW_QUEUE)
        .activity_task_queue(ACTIVITY_QUEUE)
        .max_concurrent_workflow_tasks(usize::try_from(options.activation_concurrency).map_err(
            |_| {
                format!(
                    "--activation-concurrency {} does not fit usize",
                    options.activation_concurrency
                )
            },
        )?)
        .workflow_task_prefetch_limit(usize::try_from(options.activation_prefetch_limit).map_err(
            |_| {
                format!(
                    "--activation-prefetch-limit {} does not fit usize",
                    options.activation_prefetch_limit
                )
            },
        )?)
        .workflow_task_commit_batch_size(options.batch)
        .activity_task_batch_size(options.batch)
        .max_concurrent_activities(options.batch)
        .activity_completion_batch_size(options.activity_completion_batch)
        .register_workflow(benchmark_parent)
        .register_workflow(benchmark_child_map_parent)
        .register_workflow(benchmark_signal_batch)
        .register_workflow(benchmark_child)
        .register_activity(benchmark_activity);
    if options.backend == BenchmarkBackend::Postgres && options.shards > 1 {
        builder = builder.workflow_task_shard_filter(worker_shards(
            worker_index,
            options.workers,
            options.shards,
        )?);
    }
    let worker = builder.build();
    Ok(WorkerSlot {
        worker,
        runtime: if shared_worker_runtime {
            None
        } else {
            Some(tokio_runtime()?)
        },
    })
}

struct WorkerSlot<B>
where
    B: DurableBackend,
{
    worker: Worker<B>,
    runtime: Option<tokio::runtime::Runtime>,
}

fn drain_worker_round<B>(
    runtime: &tokio::runtime::Runtime,
    workers: &mut [WorkerSlot<B>],
    batch: usize,
    shared_worker_runtime: bool,
) -> Result<WorkerRunStats, String>
where
    B: DurableBackend,
{
    if shared_worker_runtime {
        let results = runtime.block_on(async {
            futures::future::join_all(
                workers
                    .iter_mut()
                    .map(|slot| run_worker_batch(&mut slot.worker, batch)),
            )
            .await
        });
        let mut stats = WorkerRunStats::default();
        for worker_stats in results {
            stats = add_worker_stats(stats, worker_stats.map_err(|err| err.to_string())?);
        }
        return Ok(stats);
    }

    thread::scope(|scope| {
        let handles = workers
            .iter_mut()
            .map(|slot| {
                scope.spawn(move || {
                    let runtime = slot
                        .runtime
                        .as_mut()
                        .expect("threaded benchmark worker runtime");
                    runtime.block_on(run_worker_batch(&mut slot.worker, batch))
                })
            })
            .collect::<Vec<_>>();
        let mut stats = WorkerRunStats::default();
        for handle in handles {
            let worker_stats = handle
                .join()
                .map_err(|_| "benchmark worker panicked".to_owned())?
                .map_err(|err| err.to_string())?;
            stats = add_worker_stats(stats, worker_stats);
        }
        Ok(stats)
    })
}

fn tokio_runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())
}

fn worker_shards(worker_index: usize, workers: usize, shards: u64) -> Result<Vec<ShardId>, String> {
    let worker_index = u64::try_from(worker_index)
        .map_err(|_| format!("worker index {worker_index} does not fit u64"))?;
    let workers =
        u64::try_from(workers).map_err(|_| format!("worker count {workers} does not fit u64"))?;
    let mut assigned = Vec::new();
    for shard in 0..shards {
        if shard % workers == worker_index {
            assigned.push(ShardId(
                u32::try_from(shard).map_err(|_| format!("shard {shard} does not fit u32"))?,
            ));
        }
    }
    Ok(assigned)
}

async fn run_worker_batch<B>(worker: &mut Worker<B>, batch: usize) -> durust::Result<WorkerRunStats>
where
    B: DurableBackend,
{
    let mut stats = WorkerRunStats::default();
    for _ in 0..batch {
        let workflow_tasks = worker.run_workflow_batch_once().await?;
        if workflow_tasks == 0 {
            break;
        }
        stats.workflow_tasks += workflow_tasks;
    }

    let maintenance = worker.run_due_maintenance_once().await?;
    if maintenance.timers_fired > 0 {
        stats.timers_fired += maintenance.timers_fired;
    }
    if maintenance.activities_timed_out > 0 {
        stats.activities_timed_out += maintenance.activities_timed_out;
    }

    let child_starts = worker.run_child_workflow_starts_once().await?;
    if child_starts > 0 {
        stats.child_workflow_starts_dispatched += child_starts;
    }

    let activity_tasks = worker.run_activity_batch_once().await?;
    if activity_tasks > 0 {
        stats.activity_tasks += activity_tasks;
    }

    Ok(stats)
}

async fn verify_completed_workflows<B>(
    backend: &B,
    runs: &[ExpectedRun],
    options: &BenchmarkOptions,
) -> Result<u64, String>
where
    B: DurableBackend,
{
    let mut completed = 0_u64;
    for expected in runs {
        let history = backend
            .stream_history(StreamHistoryRequest {
                run_id: expected.run_id.clone(),
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(i64::MAX as u64),
                max_events: 10_000,
                max_bytes: usize::MAX,
            })
            .await
            .map_err(|err| err.to_string())?;
        let Some(last) = history.events.last() else {
            return Err(format!("run {:?} has empty history", expected.run_id));
        };
        match &last.data {
            HistoryEventData::WorkflowCompleted { result } => {
                match options.mode.as_str() {
                    "mixed" => {
                        let output = durust::decode_payload::<ParentOutput>(result)
                            .map_err(|err| err.to_string())?;
                        let expected_output = ParentOutput {
                            index: expected.index,
                            child_value: expected.index * 10,
                            signal_value: expected.index + 1_000,
                            finished: true,
                        };
                        if output != expected_output {
                            return Err(format!(
                                "run {:?} completed with unexpected output {output:?}, expected {expected_output:?}",
                                expected.run_id
                            ));
                        }
                    }
                    "child-map" => {
                        let output = durust::decode_payload::<ChildMapOutput>(result)
                            .map_err(|err| err.to_string())?;
                        let expected_output = ChildMapOutput {
                            index: expected.index,
                            sum: expected_child_map_sum(expected.index, options.child_map_items),
                            items: options.child_map_items,
                        };
                        if output != expected_output {
                            return Err(format!(
                                "run {:?} completed with unexpected child-map output {output:?}, expected {expected_output:?}",
                                expected.run_id
                            ));
                        }
                    }
                    "signal-batch" => {
                        let output = durust::decode_payload::<SignalBatchOutput>(result)
                            .map_err(|err| err.to_string())?;
                        let expected_output = SignalBatchOutput {
                            index: expected.index,
                            values: (0..options.signal_batch_width)
                                .map(|offset| signal_batch_value(expected.index, offset))
                                .collect(),
                        };
                        if output != expected_output {
                            return Err(format!(
                                "run {:?} completed with unexpected signal-batch output {output:?}, expected {expected_output:?}",
                                expected.run_id
                            ));
                        }
                    }
                    other => return Err(format!("unsupported benchmark mode `{other}`")),
                }
                completed += 1;
            }
            HistoryEventData::WorkflowFailed { failure } => {
                return Err(format!("run {:?} failed: {failure:?}", expected.run_id));
            }
            HistoryEventData::WorkflowCancelled { reason } => {
                return Err(format!("run {:?} cancelled: {reason}", expected.run_id));
            }
            other => {
                return Err(format!(
                    "run {:?} is not terminal: {other:?}",
                    expected.run_id
                ));
            }
        }
    }
    Ok(completed)
}

fn assert_workload_stats(stats: &WorkerRunStats, options: &BenchmarkOptions) -> Result<(), String> {
    let workflows = options.workflows;
    if stats.workflow_tasks < workflows as usize {
        return Err(format!(
            "expected at least one workflow task per workflow, got {stats:?}"
        ));
    }
    if options.mode != "signal-batch" && stats.activity_tasks == 0 {
        return Err(format!("expected activity work, got {stats:?}"));
    }
    if options.mode == "mixed" && stats.timers_fired == 0 {
        return Err(format!("expected timer work, got {stats:?}"));
    }
    Ok(())
}

fn counters_from_stats(
    start: &StartOutcome,
    stats: &WorkerRunStats,
    options: &BenchmarkOptions,
) -> BenchmarkCounters {
    let completed = start.runs.len() as u64;
    let mut counters = BenchmarkCounters {
        workflow_starts: start.workflow_starts,
        signals: start.signals,
        workflow_tasks: stats.workflow_tasks as u64,
        activity_tasks: stats.activity_tasks as u64,
        timers_fired: stats.timers_fired as u64,
        activities_timed_out: stats.activities_timed_out as u64,
        child_workflow_starts_dispatched: stats.child_workflow_starts_dispatched as u64,
        ..BenchmarkCounters::default()
    };
    match options.mode.as_str() {
        "mixed" => {
            counters.child_starts = completed;
            counters.child_completions = completed;
            counters.timer_handlers = completed;
            counters.boot_activities = completed;
            counters.child_activities = completed;
            counters.finish_activities = completed;
        }
        "child-map" => {
            counters.child_starts = completed.saturating_mul(options.child_map_items);
            counters.child_completions = counters.child_starts;
            counters.child_activities = counters.child_starts;
        }
        "signal-batch" => {}
        _ => {}
    }
    counters
}

fn expected_child_map_sum(index: u64, items: u64) -> u64 {
    (0..items).fold(0_u64, |sum, offset| {
        sum.saturating_add(
            index
                .saturating_mul(1_000_000)
                .saturating_add(offset)
                .saturating_mul(10),
        )
    })
}

fn signal_batch_value(index: u64, offset: usize) -> u64 {
    let offset = u64::try_from(offset).unwrap_or(u64::MAX);
    index.saturating_mul(1_000_000).saturating_add(offset)
}

impl From<WorkerRunStats> for WorkerStatsReport {
    fn from(stats: WorkerRunStats) -> Self {
        Self {
            workflow_tasks: stats.workflow_tasks as u64,
            activity_tasks: stats.activity_tasks as u64,
            timers_fired: stats.timers_fired as u64,
            activities_timed_out: stats.activities_timed_out as u64,
            child_workflow_starts_dispatched: stats.child_workflow_starts_dispatched as u64,
        }
    }
}

fn add_worker_stats(mut left: WorkerRunStats, right: WorkerRunStats) -> WorkerRunStats {
    left.workflow_tasks += right.workflow_tasks;
    left.activity_tasks += right.activity_tasks;
    left.timers_fired += right.timers_fired;
    left.activities_timed_out += right.activities_timed_out;
    left.child_workflow_starts_dispatched += right.child_workflow_starts_dispatched;
    left
}

fn mixed_action_count(counters: &BenchmarkCounters) -> u64 {
    counters.workflow_starts
        + counters.signals
        + counters.child_starts
        + counters.child_completions
        + counters.timer_handlers
        + counters.boot_activities
        + counters.child_activities
        + counters.finish_activities
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn ratio_u64(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn success_item<T>(result: &durust::Result<T>) -> u64 {
    u64::from(result.is_ok())
}

fn operation_report(
    samples: Vec<Duration>,
    total_duration: Duration,
    items: u64,
    errors: u64,
    mixed_actions: u64,
) -> BackendOperationReport {
    let latency = latency_report(samples);
    let total_seconds = total_duration.as_secs_f64();
    let mixed_actions = mixed_actions.max(1) as f64;
    BackendOperationReport {
        calls: latency.samples,
        errors,
        items,
        total_ms: duration_ms(total_duration),
        items_per_call: if latency.samples == 0 {
            0.0
        } else {
            items as f64 / latency.samples as f64
        },
        items_per_second: if total_seconds <= f64::EPSILON {
            0.0
        } else {
            items as f64 / total_seconds
        },
        calls_per_mixed_action: latency.samples as f64 / mixed_actions,
        items_per_mixed_action: items as f64 / mixed_actions,
        total_ms_per_mixed_action: duration_ms(total_duration) / mixed_actions,
        latency,
    }
}

fn latency_report(mut samples: Vec<Duration>) -> LatencyReport {
    if samples.is_empty() {
        return LatencyReport::default();
    }
    samples.sort_unstable();
    let p50 = percentile_duration(&samples, 0.50);
    let p95 = percentile_duration(&samples, 0.95);
    let p99 = percentile_duration(&samples, 0.99);
    let max = *samples.last().expect("latency samples are not empty");
    LatencyReport {
        samples: samples.len() as u64,
        p50_ms: duration_ms(p50),
        p95_ms: duration_ms(p95),
        p99_ms: duration_ms(p99),
        max_ms: duration_ms(max),
    }
}

fn percentile_duration(samples: &[Duration], percentile: f64) -> Duration {
    let index = ((samples.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    samples[index]
}

fn sqlite_store_bytes(path: &PathBuf) -> std::io::Result<u64> {
    let mut total = 0;
    for path in [
        path.clone(),
        path.with_extension("sqlite3-wal"),
        path.with_extension("sqlite3-shm"),
    ] {
        match fs::metadata(path) {
            Ok(metadata) => total += metadata.len(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(total)
}

/// Every generated benchmark database name starts with this, and
/// [`validate_benchmark_database_name`] refuses to create or drop anything that
/// does not. `drop database` has unbounded blast radius; the guard exists so
/// the only names this program can ever pass to it are ones it generated.
const POSTGRES_BENCHMARK_DATABASE_PREFIX: &str = "durust_benchdb_";

/// What to do when `pg_stat_statements` is missing. Named here once so the
/// message a user hits and the fixture that fixes it cannot drift apart, and so
/// the fixture is reachable by grep from the error rather than only from prose
/// in `impl-plan/`.
const PG_STAT_STATEMENTS_PRELOAD_HINT: &str = "pg_stat_statements has to be loaded at server start, which `create extension` cannot do on \
     its own: run the server with `-c shared_preload_libraries=pg_stat_statements`. The \
     checked-in fixture does exactly that — `docker compose -f tests/fixtures/postgres.compose.yml \
     up -d --wait`, then DURUST_POSTGRES_URL=postgres://durable:durable@127.0.0.1:55432/durable";

fn statement_stats_unavailable_message(reason: &str) -> String {
    format!("pg_stat_statements snapshot failed ({reason}). {PG_STAT_STATEMENTS_PRELOAD_HINT}")
}

/// Keeps a failed statement-stats snapshot's reason instead of discarding it.
///
/// This replaced a bare `.ok()` at four call sites. The `.ok()` turned a
/// diagnosis into `None`, and `None` is indistinguishable from "not requested".
fn statement_stats_or_reason(
    snapshot: Result<PostgresStatementStatsSnapshot, String>,
) -> (Option<PostgresStatementStatsSnapshot>, Option<String>) {
    match snapshot {
        Ok(snapshot) => (Some(snapshot), None),
        Err(reason) => (None, Some(statement_stats_unavailable_message(&reason))),
    }
}

/// Reports the reason once, on the way past, so a run that loses its statement
/// stats says so on stderr as well as in its JSON.
fn report_statement_stats_unavailable(
    before: Option<String>,
    after: Option<String>,
) -> Option<String> {
    let reason = before.or(after)?;
    eprintln!("postgres benchmark: {reason}");
    Some(reason)
}

fn wal_stats_shared_message(foreign_backends: u64) -> Option<String> {
    (foreign_backends > 0).then(|| {
        format!(
            "pg_stat_wal is cluster-wide and {foreign_backends} backend(s) on other databases \
             were live during this run, so the wal* fields include their writes; every other \
             counter comes from pg_stat_database filtered to this run's own database and is \
             unaffected"
        )
    })
}

/// Rewrites the database component of a Postgres connection URL, keeping the
/// scheme, credentials, host, port and query parameters.
///
/// Needed because the benchmark now runs in a database it creates, while
/// `create database` and `drop database` must be issued from a connection to
/// some *other* database — so two URLs are in play for one run.
///
/// The authority is taken as everything up to the first `/`, `?` or `#`, which
/// is what RFC 3986 says and what every in-spec Postgres URL satisfies. A
/// password containing one of those characters **unescaped** would split in the
/// wrong place; percent-encoding it, which the URL syntax requires anyway,
/// parses correctly.
fn postgres_url_with_database(database_url: &str, database: &str) -> Result<String, String> {
    let Some(scheme_end) = database_url.find("://") else {
        return Err(format!(
            "DURUST_POSTGRES_URL must be a `postgres://…` URL so the benchmark can run in its own \
             database, got `{database_url}`"
        ));
    };
    let authority_start = scheme_end + 3;
    let rest = &database_url[authority_start..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let tail = &rest[authority_end..];
    let query_start = tail.find(['?', '#']).unwrap_or(tail.len());
    Ok(format!(
        "{}{}/{}{}",
        &database_url[..authority_start],
        &rest[..authority_end],
        database,
        &tail[query_start..]
    ))
}

/// The one thing standing between a generated name and `drop database`.
fn validate_benchmark_database_name(database: &str) -> Result<(), String> {
    let shaped = database.starts_with(POSTGRES_BENCHMARK_DATABASE_PREFIX)
        && database.len() > POSTGRES_BENCHMARK_DATABASE_PREFIX.len()
        && database.len() <= 63
        && database
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    if shaped {
        return Ok(());
    }
    Err(format!(
        "refusing to create or drop database `{database}`: this program only ever touches names it \
         generated itself, which start with `{POSTGRES_BENCHMARK_DATABASE_PREFIX}`, contain only \
         [a-z0-9_], and fit in 63 bytes"
    ))
}

fn postgres_benchmark_database() -> String {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let counter = POSTGRES_DATABASE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "{POSTGRES_BENCHMARK_DATABASE_PREFIX}{}_{}_{}",
        std::process::id(),
        micros,
        counter
    )
}

fn postgres_admin_client(
    database_url: String,
) -> impl std::future::Future<Output = Result<tokio_postgres::Client, String>> {
    async move {
        let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .map_err(|err| err.to_string())?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("postgres benchmark admin connection error: {err}");
            }
        });
        Ok(client)
    }
}

/// Drops the run's database when it goes out of scope, panic included.
///
/// The closure this replaced ran on the `Ok` and `Err` paths only, so a panic
/// anywhere in the benchmark unwound straight past it and leaked a whole
/// database. That is heavier than the leaked *schema* the pre-per-run-database
/// code could leave, and this program has no listing or reclaim path, so it
/// falls to a human.
///
/// It covers **panics and early returns**. The success path drops the database
/// explicitly with `?` before disarming, because `Drop` cannot return a
/// `Result` and a run that completed should not exit 0 having leaked one.
///
/// **Signals are deliberately still uncovered.** Nothing runs on the way out of
/// a SIGINT, and the alternative — installing a handler — puts a global,
/// process-wide side effect into a benchmark harness and takes over Ctrl-C from
/// the async work it supervises. It would also still miss SIGKILL, OOM and
/// power loss. So the answer is recovery rather than interception: the name is
/// printed to stderr the moment the database is created, it is in the report as
/// `postgresDatabase`, and `--drop-stale` reclaims anything that survived by
/// any route at all.
struct BenchmarkDatabaseGuard<'a> {
    runtime: &'a tokio::runtime::Runtime,
    admin_url: String,
    database: String,
    armed: bool,
}

impl<'a> BenchmarkDatabaseGuard<'a> {
    fn new(runtime: &'a tokio::runtime::Runtime, admin_url: String, database: String) -> Self {
        Self {
            runtime,
            admin_url,
            database,
            armed: true,
        }
    }

    /// `--keep-db` is the only way a run's database should outlive it.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BenchmarkDatabaseGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Err(err) = drop_postgres_database(
            self.runtime,
            &self.admin_url,
            &self.database,
            DropDisposition::Owned,
        ) {
            eprintln!(
                "postgres benchmark: could not drop its own database `{}` ({err}); \
                 drop it by hand, nothing else will",
                self.database
            );
        }
    }
}

/// Quotes a Postgres identifier.
///
/// `create database` and `drop database` interpolate a name into SQL, and an
/// unquoted identifier is **case-folded to lowercase** by the server. Before
/// this, correctness of those two statements rested entirely on
/// `validate_benchmark_database_name` happening to forbid uppercase: a name
/// that reached `drop database` unquoted and contained a capital would fold to
/// a different name and, with `if exists`, silently succeed having dropped
/// nothing. That coupling was invisible and load-bearing. Quoting removes it,
/// leaving the validator as defence in depth rather than the only thing
/// standing between the program and the wrong database.
fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Lists databases this program may have left behind **and that nobody is
/// using**, without dropping anything.
///
/// Three independent filters, and the third is the one that makes the word
/// "stale" mean something:
///
/// - `like` on the generated prefix. `_` is a wildcard in `like`, so both
///   underscores are escaped. This is a **listing** filter and nothing more; no
///   pattern is ever handed to `drop database`, and every candidate must still
///   pass the validator individually.
/// - `datname <> current_database()`, so a sweep run from inside a benchmark
///   database never lists itself. This is **redundant** given the liveness
///   check below — the connection doing the listing is itself a backend
///   attached to `current_database()`, so the `not exists` already excludes it,
///   and removing this line alone leaves every test green. It is kept as a
///   second barrier because the operation it guards is unrecoverable, and
///   recorded as redundant so nobody mistakes it for the thing doing the work.
///   Removing *both* does make a sweep list itself, which is what pins the
///   property.
/// - **`pg_stat_activity` must show no backend attached.** Without this, "stale"
///   was inferred from a name and nothing else, so a benchmark running *right
///   now* was listed as reclaimable — and, because the drop used `with
///   (force)`, reclaiming it force-disconnected a live run and deleted its
///   database out from under it. A name proves who created something; only the
///   server can say whether it is in use.
///
/// The check is a snapshot, so something can still connect between here and the
/// drop. That race is closed on the other side: the sweep drops without `with
/// (force)`, so a connection arriving in the window makes the drop fail rather
/// than evict.
fn list_stale_benchmark_databases(
    runtime: &tokio::runtime::Runtime,
    admin_url: &str,
) -> Result<Vec<String>, String> {
    let admin_url = admin_url.to_owned();
    runtime.block_on(async move {
        let client = postgres_admin_client(admin_url).await?;
        let rows = client
            .query(
                r"select datname from pg_database
                  where datname like 'durust\_benchdb\_%'
                    and datname <> current_database()
                    and not exists (
                      select 1 from pg_stat_activity a where a.datname = pg_database.datname
                    )
                  order by datname",
                &[],
            )
            .await
            .map_err(|err| err.to_string())?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect())
    })
}

/// Splits listed candidates into the ones this program can prove it generated
/// and the ones it cannot.
///
/// Pure, so the refusal path is testable without a server. A name that matched
/// the listing filter but fails the validator is **kept**, not dropped, and
/// reported — `drop database` is unrecoverable and this program is the only
/// thing in the repository that issues it.
fn partition_stale_benchmark_databases(
    candidates: Vec<String>,
) -> (Vec<String>, Vec<(String, String)>) {
    let mut droppable = Vec::new();
    let mut refused = Vec::new();
    for candidate in candidates {
        match validate_benchmark_database_name(&candidate) {
            Ok(()) => droppable.push(candidate),
            Err(reason) => refused.push((candidate, reason)),
        }
    }
    (droppable, refused)
}

/// The decision half of the sweep, over a caller-supplied candidate list.
///
/// Separate from the listing so it can be tested on named fixtures rather than
/// on whatever the server happens to be holding. Every candidate is validated
/// individually and dropped by exact name; nothing here takes a pattern.
fn sweep_benchmark_databases(
    runtime: &tokio::runtime::Runtime,
    admin_url: &str,
    candidates: Vec<String>,
    drop_them: bool,
) -> Result<(), String> {
    let (droppable, refused) = partition_stale_benchmark_databases(candidates);
    for (name, reason) in &refused {
        eprintln!("leaving `{name}` alone: {reason}");
    }
    if droppable.is_empty() {
        println!("no stale benchmark databases");
    }
    let mut failures = Vec::new();
    for name in &droppable {
        if !drop_them {
            println!("{name}");
            continue;
        }
        // One validated name at a time, by exact name, and never with `force`:
        // see `DropDisposition::Foreign`.
        match drop_postgres_database(runtime, admin_url, name, DropDisposition::Foreign) {
            Ok(()) => println!("dropped {name}"),
            Err(err) => {
                eprintln!("could not drop `{name}`, leaving it: {err}");
                failures.push(name.clone());
            }
        }
    }
    if !refused.is_empty() {
        println!(
            "{} name(s) matched the listing filter but could not be proved generated by this \
             program; they were left in place",
            refused.len()
        );
    }
    if failures.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{} database(s) could not be dropped and were left in place: {}",
        failures.len(),
        failures.join(", ")
    ))
}

/// `--list-stale` and `--drop-stale`.
///
/// Closes the leak class no `Drop` guard and no signal handler can reach —
/// SIGKILL, OOM, power loss, and `2>/dev/null` plus an interrupt, where even
/// the announced name is lost.
///
/// **This is server-wide.** It considers every database on the server whose
/// name this program could have generated, not only this run's. It will not
/// touch one that is in use — see `list_stale_benchmark_databases` for the
/// liveness filter and `DropDisposition::Foreign` for the race — but an idle
/// `--keep-db` database someone left for inspection is exactly what it is
/// designed to reclaim, and it cannot tell that apart from a leak. On a shared
/// server, run `--list-stale` first.
fn sweep_stale_benchmark_databases(
    runtime: &tokio::runtime::Runtime,
    admin_url: &str,
    drop_them: bool,
) -> Result<(), String> {
    let candidates = list_stale_benchmark_databases(runtime, admin_url)?;
    sweep_benchmark_databases(runtime, admin_url, candidates, drop_them)
}

fn create_postgres_database(
    runtime: &tokio::runtime::Runtime,
    admin_url: &str,
    database: &str,
) -> Result<(), String> {
    validate_benchmark_database_name(database)?;
    let admin_url = admin_url.to_owned();
    let database = database.to_owned();
    runtime.block_on(async move {
        let client = postgres_admin_client(admin_url).await?;
        client
            .batch_execute(&format!(
                "create database {}",
                quote_postgres_identifier(&database)
            ))
            .await
            .map_err(|err| {
                format!(
                    "could not create the benchmark's own database `{database}` ({err}). The role \
                     needs CREATEDB; without it the benchmark has to share pg_stat_database with \
                     everything else on that database, which is what this exists to avoid"
                )
            })
    })
}

/// Whether a drop may evict the sessions attached to its target.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DropDisposition {
    /// The caller created this database in this process and is finished with
    /// it, so any surviving session is its own pool and may be terminated.
    Owned,
    /// The caller did not create this database and cannot know who is using it.
    /// A drop that would evict someone must fail instead.
    Foreign,
}

fn drop_postgres_database(
    runtime: &tokio::runtime::Runtime,
    admin_url: &str,
    database: &str,
    disposition: DropDisposition,
) -> Result<(), String> {
    validate_benchmark_database_name(database)?;
    let admin_url = admin_url.to_owned();
    let database = database.to_owned();
    runtime.block_on(async move {
        let client = postgres_admin_client(admin_url).await?;
        let quoted = quote_postgres_identifier(&database);
        if disposition == DropDisposition::Foreign {
            // No `with (force)`: if anything is attached, Postgres refuses with
            // `database "..." is being accessed by other users` and the caller
            // leaves it alone. This is what closes the listing's race — the
            // liveness check there is a snapshot, and a session that arrives
            // after it must survive.
            return client
                .batch_execute(&format!("drop database if exists {quoted}"))
                .await
                .map_err(|err| err.to_string());
        }
        // `with (force)` needs PostgreSQL 13; the plain form is the fallback,
        // and it fails loudly if a pooled connection outlived the run.
        if client
            .batch_execute(&format!("drop database if exists {quoted} with (force)"))
            .await
            .is_ok()
        {
            return Ok(());
        }
        client
            .batch_execute(&format!("drop database if exists {quoted}"))
            .await
            .map_err(|err| err.to_string())
    })
}

/// Backends attached to databases other than this connection's own.
///
/// `pg_stat_database` is filtered to `current_database()` and the run owns that
/// database, so only `pg_stat_wal` — which has no database column — can be
/// contaminated, and only by these.
fn postgres_foreign_backends(
    runtime: &tokio::runtime::Runtime,
    database_url: &str,
) -> Result<u64, String> {
    let database_url = database_url.to_owned();
    runtime.block_on(async move {
        let client = postgres_admin_client(database_url).await?;
        let row = client
            .query_one(
                "select count(*) from pg_stat_activity
                 where datname is not null and datname <> current_database()",
                &[],
            )
            .await
            .map_err(|err| err.to_string())?;
        Ok(u64::try_from(row.get::<_, i64>(0)).unwrap_or(0))
    })
}

fn postgres_benchmark_schema() -> String {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let counter = POSTGRES_SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "durust_benchmark_{}_{}_{}",
        std::process::id(),
        micros,
        counter
    )
}

struct ResourceSampler {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<ResourceSamplesReport>,
}

impl ResourceSampler {
    fn start(interval: Duration) -> Option<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("durust-resource-sampler".to_owned())
            .spawn(move || resource_sampler_loop(interval, thread_stop))
            .ok()?;
        Some(Self { stop, handle })
    }

    fn stop(self) -> Result<ResourceSamplesReport, String> {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .join()
            .map_err(|_| "resource sampler panicked".to_owned())
    }
}

fn resource_sampler_loop(interval: Duration, stop: Arc<AtomicBool>) -> ResourceSamplesReport {
    let mut report = ResourceSamplesReport {
        available_parallelism: std::thread::available_parallelism()
            .map(|parallelism| parallelism.get() as u64)
            .unwrap_or(0),
        ..ResourceSamplesReport::default()
    };
    while !stop.load(Ordering::Relaxed) {
        if let Some(sample) = process_resource_sample() {
            report.samples += 1;
            report.max_process_cpu_percent = report.max_process_cpu_percent.max(sample.cpu_percent);
            report.max_process_rss_bytes = report.max_process_rss_bytes.max(sample.rss_bytes);
        }
        thread::sleep(interval);
    }
    report
}

#[derive(Clone, Copy, Debug)]
struct ProcessResourceSample {
    cpu_percent: f64,
    rss_bytes: u64,
}

fn process_resource_sample() -> Option<ProcessResourceSample> {
    let output = Command::new("ps")
        .args([
            "-o",
            "rss=",
            "-o",
            "pcpu=",
            "-p",
            &std::process::id().to_string(),
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let mut parts = stdout.split_whitespace();
    let rss_kib = parts.next()?.parse::<u64>().ok()?;
    let cpu_percent = parts.next()?.parse::<f64>().ok()?;
    Some(ProcessResourceSample {
        cpu_percent,
        rss_bytes: rss_kib.saturating_mul(1024),
    })
}

struct PostgresActivitySampler {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<Result<PostgresActivitySamplesReport, String>>,
}

impl PostgresActivitySampler {
    fn start(database_url: String, interval: Duration) -> Option<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("durust-postgres-activity-sampler".to_owned())
            .spawn(move || postgres_activity_sampler_loop(database_url, interval, thread_stop))
            .ok()?;
        Some(Self { stop, handle })
    }

    fn stop(self) -> Result<PostgresActivitySamplesReport, String> {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .join()
            .map_err(|_| "postgres activity sampler panicked".to_owned())?
    }
}

fn postgres_activity_sampler_loop(
    database_url: String,
    interval: Duration,
    stop: Arc<AtomicBool>,
) -> Result<PostgresActivitySamplesReport, String> {
    let runtime = tokio_runtime()?;
    runtime.block_on(async move {
        let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .map_err(|err| err.to_string())?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("postgres activity sampler connection error: {err}");
            }
        });

        let mut report = PostgresActivitySamplesReport::default();
        while !stop.load(Ordering::Relaxed) {
            let rows = client
                .query(
                    "select state,
                            coalesce(wait_event_type, ''),
                            coalesce(wait_event, '')
                     from pg_stat_activity
                     where datname = current_database()
                       and pid <> pg_backend_pid()",
                    &[],
                )
                .await
                .map_err(|err| err.to_string())?;

            let mut active = 0_u64;
            let mut idle = 0_u64;
            let mut waiting = 0_u64;
            for row in &rows {
                let state: Option<String> = row.get(0);
                let wait_event_type: String = row.get(1);
                let wait_event: String = row.get(2);
                match state.as_deref() {
                    Some("active") => active += 1,
                    Some("idle") => idle += 1,
                    _ => {}
                }
                if !wait_event_type.is_empty() {
                    waiting += 1;
                    *report
                        .wait_event_type_counts
                        .entry(wait_event_type)
                        .or_default() += 1;
                }
                if !wait_event.is_empty() {
                    *report.wait_event_counts.entry(wait_event).or_default() += 1;
                }
            }
            report.samples += 1;
            report.max_connections = report.max_connections.max(rows.len() as u64);
            report.max_active = report.max_active.max(active);
            report.max_idle = report.max_idle.max(idle);
            report.max_waiting = report.max_waiting.max(waiting);

            tokio::time::sleep(interval).await;
        }
        Ok(report)
    })
}

fn postgres_stats_snapshot(
    runtime: &tokio::runtime::Runtime,
    database_url: &str,
) -> Result<PostgresStatsSnapshot, String> {
    let database_url = database_url.to_owned();
    runtime.block_on(async move {
        let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .map_err(|err| err.to_string())?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("postgres benchmark stats connection error: {err}");
            }
        });
        let wal_bytes = client
            .query_one(
                "select wal_records::bigint,
                        wal_fpi::bigint,
                        wal_bytes::bigint,
                        wal_buffers_full::bigint,
                        wal_write::bigint,
                        wal_sync::bigint,
                        coalesce(wal_write_time, 0)::double precision,
                        coalesce(wal_sync_time, 0)::double precision
                 from pg_stat_wal",
                &[],
            )
            .await
            .map_err(|err| err.to_string())?;
        let database = client
            .query_one(
                "select xact_commit::bigint,
                        xact_rollback::bigint,
                        blks_read::bigint,
                        blks_hit::bigint,
                        tup_returned::bigint,
                        tup_fetched::bigint,
                        tup_inserted::bigint,
                        tup_updated::bigint,
                        tup_deleted::bigint,
                        conflicts::bigint,
                        temp_files::bigint,
                        temp_bytes::bigint,
                        deadlocks::bigint,
                        coalesce(blk_read_time, 0)::double precision,
                        coalesce(blk_write_time, 0)::double precision,
                        coalesce(session_time, 0)::double precision,
                        coalesce(active_time, 0)::double precision
                 from pg_stat_database
                 where datname = current_database()",
                &[],
            )
            .await
            .map_err(|err| err.to_string())?;
        let active_connections = client
            .query_one(
                "select count(*) from pg_stat_activity where datname = current_database()",
                &[],
            )
            .await
            .map_err(|err| err.to_string())?
            .get::<_, i64>(0);
        Ok(PostgresStatsSnapshot {
            wal_records: row_u64(&wal_bytes, 0),
            wal_fpi: row_u64(&wal_bytes, 1),
            wal_bytes: row_u64(&wal_bytes, 2),
            wal_buffers_full: row_u64(&wal_bytes, 3),
            wal_write: row_u64(&wal_bytes, 4),
            wal_sync: row_u64(&wal_bytes, 5),
            wal_write_time_ms: wal_bytes.get(6),
            wal_sync_time_ms: wal_bytes.get(7),
            xact_commit: row_u64(&database, 0),
            xact_rollback: row_u64(&database, 1),
            blocks_read: row_u64(&database, 2),
            blocks_hit: row_u64(&database, 3),
            rows_returned: row_u64(&database, 4),
            rows_fetched: row_u64(&database, 5),
            rows_inserted: row_u64(&database, 6),
            rows_updated: row_u64(&database, 7),
            rows_deleted: row_u64(&database, 8),
            temp_files: row_u64(&database, 10),
            temp_bytes: row_u64(&database, 11),
            deadlocks: row_u64(&database, 12),
            block_read_time_ms: database.get(13),
            block_write_time_ms: database.get(14),
            session_time_ms: database.get(15),
            active_time_ms: database.get(16),
            active_connections: u64::try_from(active_connections).unwrap_or(0),
        })
    })
}

fn postgres_statement_stats_snapshot(
    runtime: &tokio::runtime::Runtime,
    database_url: &str,
) -> Result<PostgresStatementStatsSnapshot, String> {
    let database_url = database_url.to_owned();
    runtime.block_on(async move {
        let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .map_err(|err| err.to_string())?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("postgres statement stats connection error: {err}");
            }
        });
        if client
            .batch_execute("create extension if not exists pg_stat_statements")
            .await
            .is_err()
        {
            return Err("pg_stat_statements extension is unavailable".to_owned());
        }
        let rows = client
            .query(
                "select coalesce(queryid::text, md5(query)) as query_id,
                        regexp_replace(left(query, 240), '\\s+', ' ', 'g') as query,
                        calls::bigint,
                        total_exec_time::double precision
                 from pg_stat_statements
                 where dbid = (select oid from pg_database where datname = current_database())",
                &[],
            )
            .await
            .map_err(|err| err.to_string())?;
        let statements = rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    PostgresStatementStatsEntry {
                        query: row.get(1),
                        calls: row_u64(&row, 2),
                        total_exec_time_ms: row.get(3),
                    },
                )
            })
            .collect();
        Ok(PostgresStatementStatsSnapshot { statements })
    })
}

fn postgres_statement_stats_report(
    before: PostgresStatementStatsSnapshot,
    after: PostgresStatementStatsSnapshot,
    completed_workflows: u64,
    mixed_actions: u64,
) -> PostgresStatementStatsReport {
    let mut calls = 0_u64;
    let mut total_exec_time_ms = 0.0_f64;
    let mut top_statements = Vec::new();
    for (query_id, after_entry) in after.statements {
        let before_entry = before.statements.get(&query_id);
        let call_delta = after_entry
            .calls
            .saturating_sub(before_entry.map_or(0, |entry| entry.calls));
        if call_delta == 0 {
            continue;
        }
        let time_delta = (after_entry.total_exec_time_ms
            - before_entry.map_or(0.0, |entry| entry.total_exec_time_ms))
        .max(0.0);
        calls = calls.saturating_add(call_delta);
        total_exec_time_ms += time_delta;
        top_statements.push(PostgresStatementReport {
            query_id,
            calls: call_delta,
            total_exec_time_ms: time_delta,
            query: after_entry.query,
        });
    }
    top_statements.sort_by(|left, right| {
        right
            .calls
            .cmp(&left.calls)
            .then_with(|| right.total_exec_time_ms.total_cmp(&left.total_exec_time_ms))
    });
    top_statements.truncate(10);
    PostgresStatementStatsReport {
        calls,
        calls_per_mixed_action: ratio_u64(calls, mixed_actions),
        calls_per_workflow: ratio_u64(calls, completed_workflows),
        total_exec_time_ms,
        top_statements,
    }
}

fn row_u64(row: &tokio_postgres::Row, idx: usize) -> u64 {
    u64::try_from(row.get::<_, i64>(idx)).unwrap_or(0)
}

fn print_text_result(result: &BenchmarkResult) {
    println!("Durust benchmark workload");
    println!("  backend: {}", result.backend);
    if let Some(layout) = &result.sqlite_layout {
        println!("  SQLite layout: {layout}");
    }
    println!("  mode: {}", result.mode);
    println!("  workflows: {}", result.options.workflows);
    println!("  workers: {}", result.options.workers);
    println!("  shards: {}", result.options.shards);
    println!(
        "  physical partitions: {}",
        result.options.physical_partitions
    );
    println!(
        "  activation concurrency: {}",
        result.options.activation_concurrency
    );
    println!(
        "  activation prefetch limit: {}",
        result.options.activation_prefetch_limit
    );
    println!("  batch: {}", result.options.batch);
    println!(
        "  activity completion batch: {}",
        result.options.activity_completion_batch
    );
    println!("  rounds: {}", result.rounds);
    println!(
        "  elapsed: {:.2}ms ({:.2}ms setup, {:.2}ms processing, {:.2}ms verify)",
        result.elapsed_ms, result.setup_ms, result.processing_ms, result.verify_ms
    );
    println!();
    println!("Processing-only throughput:");
    println!(
        "  workflows/sec: {:.2}",
        result.processing_workflows_per_second
    );
    println!(
        "  activations/sec: {:.2}",
        result.processing_activations_per_second
    );
    println!(
        "  mixed actions/sec: {:.2}",
        result.processing_mixed_actions_per_second
    );
    println!();
    println!("Commit latency:");
    println!(
        "  workflow task commits: {} samples, p50 {:.3}ms, p95 {:.3}ms, p99 {:.3}ms, max {:.3}ms",
        result.backend_metrics.workflow_task_commit_latency.samples,
        result.backend_metrics.workflow_task_commit_latency.p50_ms,
        result.backend_metrics.workflow_task_commit_latency.p95_ms,
        result.backend_metrics.workflow_task_commit_latency.p99_ms,
        result.backend_metrics.workflow_task_commit_latency.max_ms
    );
    print_backend_metrics_summary(
        "Processing backend operation latency",
        &result.processing_backend_metrics,
    );
    print_backend_metrics_summary(
        "Full-run backend operation latency",
        &result.backend_metrics,
    );
    if let Some(postgres) = &result.postgres_stats {
        println!();
        println!("Postgres stats:");
        println!("  WAL bytes: {}", postgres.wal_bytes);
        println!("  WAL bytes/sec: {:.2}", postgres.wal_bytes_per_second);
        println!("  WAL records/sec: {:.2}", postgres.wal_records_per_second);
        println!(
            "  transactions/sec: {:.2}",
            postgres.transactions_per_second
        );
        println!(
            "  transactions/action: {:.3}",
            postgres.transactions_per_mixed_action
        );
        if let Some(statements) = &postgres.statement_stats {
            println!(
                "  statements/action: {:.3}",
                statements.calls_per_mixed_action
            );
        }
        println!(
            "  block cache hit ratio: {:.4}",
            postgres.block_cache_hit_ratio
        );
        println!("  temp bytes: {}", postgres.temp_bytes);
        println!("  deadlocks: {}", postgres.deadlocks);
        if let Some(samples) = &postgres.activity_samples {
            println!(
                "  activity samples: {}, max connections {}, max active {}, max waiting {}",
                samples.samples, samples.max_connections, samples.max_active, samples.max_waiting
            );
        }
        println!(
            "  active connections after: {}",
            postgres.active_connections_after
        );
    }
    if let Some(samples) = &result.resource_samples {
        println!();
        println!("Process resources:");
        println!("  samples: {}", samples.samples);
        println!("  available parallelism: {}", samples.available_parallelism);
        println!("  max process CPU: {:.1}%", samples.max_process_cpu_percent);
        println!("  max process RSS: {} bytes", samples.max_process_rss_bytes);
    }
    println!();
    println!("Counters:");
    println!("  workflow starts: {}", result.counters.workflow_starts);
    println!("  signals: {}", result.counters.signals);
    println!("  child starts: {}", result.counters.child_starts);
    println!("  child completions: {}", result.counters.child_completions);
    println!("  timer handlers: {}", result.counters.timer_handlers);
    println!("  boot activities: {}", result.counters.boot_activities);
    println!("  child activities: {}", result.counters.child_activities);
    println!("  finish activities: {}", result.counters.finish_activities);
    println!();
    println!("Worker stats:");
    println!("  workflow tasks: {}", result.worker_stats.workflow_tasks);
    println!("  activity tasks: {}", result.worker_stats.activity_tasks);
    println!("  timers fired: {}", result.worker_stats.timers_fired);
    println!(
        "  child starts dispatched: {}",
        result.worker_stats.child_workflow_starts_dispatched
    );
    if let Some(path) = &result.db_path {
        println!();
        println!("SQLite store kept:");
        println!("  path: {path}");
        println!("  size: {} bytes", result.db_bytes.unwrap_or(0));
    }
    if let Some(schema) = &result.postgres_schema {
        println!();
        println!("Postgres schema:");
        println!("  name: {schema}");
        if let Some(database) = &result.postgres_database {
            println!("  database: {database}");
        }
        if result.options.keep_db {
            println!("  kept: true");
        }
    }
}

fn print_backend_metrics_summary(label: &str, metrics: &BackendMetricsReport) {
    if !metrics.operations.is_empty() {
        let mut operations = metrics.operations.iter().collect::<Vec<_>>();
        operations.sort_by(|(_, left), (_, right)| {
            right
                .total_ms
                .partial_cmp(&left.total_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        println!();
        println!("{label}:");
        for (name, operation) in operations.into_iter().take(8) {
            println!(
                "  {name}: calls {}, items {}, calls/action {:.3}, items/call {:.2}, total {:.2}ms, p95 {:.3}ms",
                operation.calls,
                operation.items,
                operation.calls_per_mixed_action,
                operation.items_per_call,
                operation.total_ms,
                operation.latency.p95_ms
            );
        }
    }
    if !metrics.workflow_task_commit_shapes.is_empty() {
        println!();
        println!("{label} commit shapes:");
        for (name, count) in &metrics.workflow_task_commit_shapes {
            println!("  {name}: {count}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_postgres_snapshot() -> PostgresStatsSnapshot {
        PostgresStatsSnapshot {
            wal_records: 0,
            wal_fpi: 0,
            wal_bytes: 0,
            wal_buffers_full: 0,
            wal_write: 0,
            wal_sync: 0,
            wal_write_time_ms: 0.0,
            wal_sync_time_ms: 0.0,
            xact_commit: 0,
            xact_rollback: 0,
            blocks_read: 0,
            blocks_hit: 0,
            rows_returned: 0,
            rows_fetched: 0,
            rows_inserted: 0,
            rows_updated: 0,
            rows_deleted: 0,
            temp_files: 0,
            temp_bytes: 0,
            deadlocks: 0,
            block_read_time_ms: 0.0,
            block_write_time_ms: 0.0,
            session_time_ms: 0.0,
            active_time_ms: 0.0,
            active_connections: 0,
        }
    }

    fn probe_postgres_url_or_skip() -> Option<String> {
        if let Ok(url) = std::env::var("DURUST_POSTGRES_URL") {
            if !url.trim().is_empty() {
                return Some(url);
            }
        }
        let required = match std::env::var("DURUST_REQUIRE_POSTGRES") {
            Ok(value) => {
                let value = value.trim();
                !(value.is_empty() || value == "0" || value.eq_ignore_ascii_case("false"))
            }
            Err(_) => false,
        };
        assert!(
            !required,
            "DURUST_REQUIRE_POSTGRES is set, so the benchtools database-guard test must run, \
             but DURUST_POSTGRES_URL is unset or empty"
        );
        eprintln!("skipping benchtools database guard test; set DURUST_POSTGRES_URL");
        None
    }

    fn database_exists(runtime: &tokio::runtime::Runtime, admin_url: &str, database: &str) -> bool {
        let admin_url = admin_url.to_owned();
        let database = database.to_owned();
        runtime.block_on(async move {
            let client = postgres_admin_client(admin_url).await.unwrap();
            client
                .query_one(
                    "select count(*) from pg_database where datname = $1",
                    &[&database],
                )
                .await
                .unwrap()
                .get::<_, i64>(0)
                > 0
        })
    }

    #[test]
    fn the_stale_sweep_refuses_every_name_it_cannot_prove_it_generated() {
        let candidates = vec![
            postgres_benchmark_database(),
            // All of these can reach the sweep: `like 'durust\_benchdb\_%'` is a
            // listing filter, not a proof of origin.
            "durust_benchdb_UPPER".to_owned(),
            "durust_benchdb_".to_owned(),
            "durust_benchdb_1; drop database durust".to_owned(),
            "durust_benchdb_wgw-hyphen".to_owned(),
            "not_durust_benchdb_1".to_owned(),
            format!("durust_benchdb_{}", "x".repeat(64)),
        ];
        let (droppable, refused) = partition_stale_benchmark_databases(candidates);
        assert_eq!(
            droppable.len(),
            1,
            "only the generated name may be dropped, got {droppable:?}"
        );
        assert!(droppable[0].starts_with(POSTGRES_BENCHMARK_DATABASE_PREFIX));
        assert_eq!(refused.len(), 6, "refused {refused:?}");
        for (name, reason) in &refused {
            assert!(
                reason.contains("refusing to create or drop database"),
                "`{name}` must be refused by name, got: {reason}"
            );
        }
    }

    #[test]
    fn the_sweep_drops_a_generated_database_and_spares_a_lookalike() {
        let Some(url) = probe_postgres_url_or_skip() else {
            return;
        };
        let runtime = tokio_runtime().unwrap();
        let generated = postgres_benchmark_database();
        create_postgres_database(&runtime, &url, &generated).unwrap();

        // Matches the listing filter, fails the validator: exactly the case the
        // sweep must see and refuse. Created with raw SQL because
        // `create_postgres_database` would — correctly — refuse to make it.
        let lookalike = "durust_benchdb_NotOurs";
        let raw = |sql: String| {
            let url = url.clone();
            runtime.block_on(async move {
                postgres_admin_client(url)
                    .await
                    .unwrap()
                    .batch_execute(&sql)
                    .await
            })
        };
        let _ = raw(format!(r#"drop database if exists "{lookalike}""#));
        raw(format!(r#"create database "{lookalike}""#)).unwrap();

        // Only this test's two fixtures. Calling the server-wide
        // `sweep_stale_benchmark_databases` here made `cargo test` reclaim
        // every idle benchmark database on the server, which on a shared box is
        // the normal condition rather than the edge case. The decision logic is
        // what is under test; its reach is not.
        sweep_benchmark_databases(
            &runtime,
            &url,
            vec![generated.clone(), lookalike.to_owned()],
            true,
        )
        .unwrap();

        assert!(
            !database_exists(&runtime, &url, &generated),
            "the sweep must reclaim a database it can prove it generated: `{generated}` survived"
        );
        assert!(
            database_exists(&runtime, &url, lookalike),
            "the sweep dropped `{lookalike}`, which it cannot prove it generated; `drop \
             database` is unrecoverable and the listing filter is not a proof of origin"
        );

        // Reclaim the fixture by its exact literal name, not by pattern.
        raw(format!(r#"drop database "{lookalike}""#)).unwrap();
    }

    #[test]
    fn the_sweep_refuses_to_evict_a_live_session_rather_than_forcing_it() {
        let Some(url) = probe_postgres_url_or_skip() else {
            return;
        };
        let runtime = tokio_runtime().unwrap();
        let database = postgres_benchmark_database();
        create_postgres_database(&runtime, &url, &database).unwrap();
        let database_url = postgres_url_with_database(&url, &database).unwrap();
        let live = runtime
            .block_on(postgres_admin_client(database_url))
            .unwrap();

        // Handed straight to the decision half, bypassing the listing: this is
        // the race the liveness filter cannot close, where a session arrives
        // after the snapshot. The drop must fail rather than evict.
        let outcome = sweep_benchmark_databases(&runtime, &url, vec![database.clone()], true);

        assert!(
            outcome.is_err(),
            "dropping a foreign database with a live session must fail and be reported, not \
             succeed quietly"
        );
        assert!(
            database_exists(&runtime, &url, &database),
            "the sweep evicted a live session and deleted `{database}`. `with (force)` belongs \
             to the guard, which owns the database it created; on the sweep it turns reclaiming \
             a leak into killing someone's running benchmark"
        );
        // The session is still usable, which is the property that matters to
        // whoever is running that benchmark.
        runtime
            .block_on(live.query_one("select 1", &[]))
            .expect("the live session must survive a refused sweep");

        drop(live);
        drop_postgres_database(&runtime, &url, &database, DropDisposition::Owned).unwrap();
    }

    #[test]
    fn a_database_with_a_live_backend_is_never_listed_as_stale() {
        let Some(url) = probe_postgres_url_or_skip() else {
            return;
        };
        let runtime = tokio_runtime().unwrap();
        let database = postgres_benchmark_database();
        create_postgres_database(&runtime, &url, &database).unwrap();
        let database_url = postgres_url_with_database(&url, &database).unwrap();

        // Hold a session open against it, which is what a running benchmark
        // looks like to the server.
        let live = runtime
            .block_on(postgres_admin_client(database_url))
            .unwrap();
        let listed = list_stale_benchmark_databases(&runtime, &url).unwrap();
        assert!(
            !listed.contains(&database),
            "`{database}` has a live backend and was listed as stale. A name says who created \
             a database, never whether someone is using it — and the sweep would have \
             force-disconnected a running benchmark and deleted it. Listed: {listed:?}"
        );

        // Release it, and it becomes reclaimable.
        drop(live);
        let mut listed_after = Vec::new();
        for _ in 0..50 {
            listed_after = list_stale_benchmark_databases(&runtime, &url).unwrap();
            if listed_after.contains(&database) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            listed_after.contains(&database),
            "once nothing is attached, `{database}` must become reclaimable: {listed_after:?}"
        );
        drop_postgres_database(&runtime, &url, &database, DropDisposition::Owned).unwrap();
    }

    #[test]
    fn a_sweep_run_from_inside_a_benchmark_database_does_not_list_itself() {
        let Some(url) = probe_postgres_url_or_skip() else {
            return;
        };
        let runtime = tokio_runtime().unwrap();
        let database = postgres_benchmark_database();
        create_postgres_database(&runtime, &url, &database).unwrap();
        // Point the sweep at the benchmark database itself, which is the only
        // configuration in which `datname <> current_database()` does anything.
        let database_url = postgres_url_with_database(&url, &database).unwrap();
        let listed = list_stale_benchmark_databases(&runtime, &database_url).unwrap();
        assert!(
            !listed.contains(&database),
            "a sweep run from inside `{database}` listed itself: {listed:?}"
        );
        drop_postgres_database(&runtime, &url, &database, DropDisposition::Owned).unwrap();
    }

    #[test]
    fn the_listing_filter_treats_prefix_underscores_as_literals() {
        let Some(url) = probe_postgres_url_or_skip() else {
            return;
        };
        let runtime = tokio_runtime().unwrap();
        // `_` is a `like` wildcard; unescaped, this name matches the pattern.
        let wildcard_match = "durustxbenchdby_probe";
        let raw = |sql: String| {
            let url = url.clone();
            runtime.block_on(async move {
                postgres_admin_client(url)
                    .await
                    .unwrap()
                    .batch_execute(&sql)
                    .await
            })
        };
        let _ = raw(format!(r#"drop database if exists "{wildcard_match}""#));
        raw(format!(r#"create database "{wildcard_match}""#)).unwrap();
        let listed = list_stale_benchmark_databases(&runtime, &url).unwrap();
        let contains = listed.contains(&wildcard_match.to_owned());
        raw(format!(r#"drop database "{wildcard_match}""#)).unwrap();
        assert!(
            !contains,
            "`{wildcard_match}` was listed: the underscores in the prefix must be escaped in \
             the `like`, or the filter matches a whole family of unrelated names"
        );
    }

    #[test]
    fn a_panicking_run_still_drops_the_database_it_created() {
        let Some(url) = probe_postgres_url_or_skip() else {
            return;
        };
        let runtime = tokio_runtime().unwrap();
        let database = postgres_benchmark_database();
        create_postgres_database(&runtime, &url, &database).unwrap();
        assert!(database_exists(&runtime, &url, &database));

        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = BenchmarkDatabaseGuard::new(&runtime, url.clone(), database.clone());
            panic!("benchmark exploded mid-run");
        }));
        assert!(panicked.is_err(), "the probe must actually panic");

        assert!(
            !database_exists(&runtime, &url, &database),
            "a panic leaked the run's whole database: `{database}` survived. Only `--keep-db` \
             may leave one behind, and nothing in this program can list or reclaim it"
        );
    }

    #[test]
    fn keeping_the_database_disarms_the_guard() {
        let Some(url) = probe_postgres_url_or_skip() else {
            return;
        };
        let runtime = tokio_runtime().unwrap();
        let database = postgres_benchmark_database();
        create_postgres_database(&runtime, &url, &database).unwrap();
        {
            let mut guard = BenchmarkDatabaseGuard::new(&runtime, url.clone(), database.clone());
            guard.disarm();
        }
        let kept = database_exists(&runtime, &url, &database);
        // Reclaim it either way: this test exists to prove `--keep-db` works,
        // not to leave evidence of it lying around.
        drop_postgres_database(&runtime, &url, &database, DropDisposition::Owned).unwrap();
        assert!(
            kept,
            "--keep-db must leave the database in place for inspection"
        );
    }

    #[test]
    fn a_failed_statement_stats_snapshot_keeps_its_reason_and_names_the_preload_fixture() {
        let (snapshot, reason) = statement_stats_or_reason(Err(
            "pg_stat_statements extension is unavailable".to_owned(),
        ));
        assert!(snapshot.is_none());
        let reason = reason.expect(
            "a failed pg_stat_statements snapshot must report why; dropping it with `.ok()` is \
             what made `statementStats: null` unexplainable",
        );
        assert!(
            reason.contains("pg_stat_statements extension is unavailable"),
            "the server's own error must survive, got: {reason}"
        );
        assert!(
            reason.contains("shared_preload_libraries=pg_stat_statements"),
            "the reason must name the setting that fixes it, got: {reason}"
        );
        assert!(
            reason.contains("tests/fixtures/postgres.compose.yml"),
            "the reason must name the checked-in fixture that supplies the preload, got: {reason}"
        );
    }

    #[test]
    fn a_successful_statement_stats_snapshot_reports_no_reason() {
        let (snapshot, reason) = statement_stats_or_reason(Ok(PostgresStatementStatsSnapshot {
            statements: BTreeMap::new(),
        }));
        assert!(snapshot.is_some());
        assert_eq!(reason, None, "a working snapshot must not carry an excuse");
    }

    #[test]
    fn the_statement_stats_reason_reaches_the_serialised_report() {
        let report = postgres_stats_report(
            empty_postgres_snapshot(),
            empty_postgres_snapshot(),
            1.0,
            1,
            1,
            None,
            None,
            Some("pg_stat_statements snapshot failed".to_owned()),
            None,
        );
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(
            json.get("statementStatsUnavailable")
                .and_then(serde_json::Value::as_str),
            Some("pg_stat_statements snapshot failed"),
            "the reason must be readable by whoever consumes the JSON, not only on stderr"
        );
        let quiet = postgres_stats_report(
            empty_postgres_snapshot(),
            empty_postgres_snapshot(),
            1.0,
            1,
            1,
            None,
            None,
            None,
            None,
        );
        let quiet = serde_json::to_value(&quiet).unwrap();
        assert!(
            quiet.get("statementStatsUnavailable").is_none(),
            "a healthy run must not emit the field at all"
        );
    }

    #[test]
    fn wal_stats_are_flagged_only_when_another_database_was_active() {
        assert_eq!(wal_stats_shared_message(0), None);
        let shared = wal_stats_shared_message(3).expect(
            "pg_stat_wal is cluster-wide, so live backends on other databases must be recorded",
        );
        assert!(shared.contains("3 backend(s)"), "got: {shared}");
        assert!(shared.contains("pg_stat_wal"), "got: {shared}");
    }

    #[test]
    fn a_benchmark_database_url_keeps_everything_but_the_database_name() {
        assert_eq!(
            postgres_url_with_database(
                "postgres://postgres:postgres@localhost:5433/durust",
                "durust_benchdb_1_2_3"
            )
            .unwrap(),
            "postgres://postgres:postgres@localhost:5433/durust_benchdb_1_2_3"
        );
        assert_eq!(
            postgres_url_with_database(
                "postgres://user:pw@db.example:5432/durust?sslmode=require&connect_timeout=5",
                "durust_benchdb_1_2_3"
            )
            .unwrap(),
            "postgres://user:pw@db.example:5432/durust_benchdb_1_2_3?sslmode=require&connect_timeout=5"
        );
        assert_eq!(
            postgres_url_with_database("postgres://localhost", "durust_benchdb_1_2_3").unwrap(),
            "postgres://localhost/durust_benchdb_1_2_3"
        );
        assert_eq!(
            postgres_url_with_database(
                "postgres://localhost?sslmode=disable",
                "durust_benchdb_1_2_3"
            )
            .unwrap(),
            "postgres://localhost/durust_benchdb_1_2_3?sslmode=disable"
        );
        let err =
            postgres_url_with_database("/var/run/postgresql", "durust_benchdb_1_2_3").unwrap_err();
        assert!(err.contains("must be a `postgres://"), "got: {err}");
    }

    #[test]
    fn only_names_this_program_generated_can_be_created_or_dropped() {
        validate_benchmark_database_name(&postgres_benchmark_database())
            .expect("a generated name must pass its own guard");
        for rejected in [
            "durust",
            "postgres",
            "template1",
            "",
            "durust_benchdb_",
            "durust_benchdb_1; drop database durust",
            "durust_benchdb_UPPER",
            "bench_durust_1",
            // Pins `starts_with` against `contains`: the prefix is embedded,
            // not leading. Unreachable through the listing filter today, which
            // anchors, but the validator's own error text promises "start
            // with".
            "not_durust_benchdb_1",
        ] {
            let Err(err) = validate_benchmark_database_name(rejected) else {
                panic!(
                    "`{rejected}` was accepted as a benchmark database name; this guard is the \
                     only thing between a generated name and `drop database`"
                );
            };
            assert!(
                err.contains("refusing to create or drop database"),
                "`{rejected}` must be refused by name, got: {err}"
            );
        }
    }

    #[test]
    fn parses_default_sqlite_options() {
        let options = parse_args(Vec::<String>::new()).unwrap();
        assert_eq!(options.backend, BenchmarkBackend::Sqlite);
        assert_eq!(options.mode, "mixed");
        assert_eq!(options.workflows, DEFAULT_WORKFLOWS);
        assert_eq!(options.shards, 1);
        assert_eq!(options.physical_partitions, 1);
        assert_eq!(options.activation_concurrency, 1);
        assert_eq!(options.activation_prefetch_limit, 1);
        assert_eq!(options.activity_completion_batch, 1);
        assert_eq!(options.signal_batch_width, 8);
        assert!(!options.force_scalar_signal_reads);
    }

    #[test]
    fn rejects_shards_for_non_postgres_backends() {
        let err = parse_args(["--shards".to_owned(), "2".to_owned()]).unwrap_err();
        assert!(err.contains("requires --backend postgres"));

        let err = parse_args(["--physical-partitions".to_owned(), "2".to_owned()]).unwrap_err();
        assert!(err.contains("requires --backend postgres"));
    }

    #[test]
    fn parses_postgres_options_without_requiring_database() {
        let options = parse_args([
            "--backend".to_owned(),
            "postgres".to_owned(),
            "--postgres-pool-size".to_owned(),
            "9".to_owned(),
            "--shards".to_owned(),
            "100".to_owned(),
            "--physical-partitions".to_owned(),
            "16".to_owned(),
            "--activation-concurrency".to_owned(),
            "4".to_owned(),
            "--activation-prefetch-limit".to_owned(),
            "8".to_owned(),
            "--activity-completion-batch".to_owned(),
            "7".to_owned(),
            "--worker-round-passes".to_owned(),
            "1".to_owned(),
        ])
        .unwrap();
        assert_eq!(options.backend, BenchmarkBackend::Postgres);
        assert_eq!(options.postgres_pool_size, Some(9));
        assert_eq!(options.shards, 100);
        assert_eq!(options.physical_partitions, 16);
        assert_eq!(options.activation_concurrency, 4);
        assert_eq!(options.activation_prefetch_limit, 8);
        assert_eq!(options.activity_completion_batch, 7);
        assert_eq!(options.worker_round_passes, Some(1));
    }

    #[test]
    fn parses_postgres_write_ceiling_mode() {
        let options = parse_args([
            "--backend".to_owned(),
            "postgres".to_owned(),
            "--mode".to_owned(),
            "postgres-write-ceiling".to_owned(),
            "--workflows".to_owned(),
            "4".to_owned(),
            "--keep-db".to_owned(),
            "--sample-resources".to_owned(),
        ])
        .unwrap();
        assert_eq!(options.backend, BenchmarkBackend::Postgres);
        assert_eq!(options.mode, "postgres-write-ceiling");
        assert!(options.keep_db);
        assert!(options.sample_resources);
        assert_eq!(postgres_write_ceiling_operations(&options).unwrap(), 32);
    }

    #[test]
    fn parses_signal_batch_mode_and_scalar_comparison_flag() {
        let options = parse_args([
            "--backend".to_owned(),
            "postgres".to_owned(),
            "--mode".to_owned(),
            "signal-batch".to_owned(),
            "--signal-batch-width".to_owned(),
            "4".to_owned(),
            "--force-scalar-signal-reads".to_owned(),
        ])
        .unwrap();
        assert_eq!(options.backend, BenchmarkBackend::Postgres);
        assert_eq!(options.mode, "signal-batch");
        assert_eq!(options.signal_batch_width, 4);
        assert!(options.force_scalar_signal_reads);

        let err = parse_args([
            "--backend".to_owned(),
            "postgres".to_owned(),
            "--mode".to_owned(),
            "mixed".to_owned(),
            "--force-scalar-signal-reads".to_owned(),
        ])
        .unwrap_err();
        assert!(err.contains("only supported with --mode signal-batch"));
    }

    #[test]
    fn rejects_diagnostic_modes_for_non_postgres_backends() {
        let err = parse_args([
            "--backend".to_owned(),
            "sqlite".to_owned(),
            "--mode".to_owned(),
            "postgres-write-ceiling".to_owned(),
        ])
        .unwrap_err();
        assert!(err.contains("only --backend postgres supports diagnostic modes"));
    }

    #[test]
    fn mixed_workload_has_nominal_workflow_task_target() {
        let mut options = default_options();
        options.workflows = 4;
        assert_eq!(nominal_workflow_task_target(&options).unwrap(), 32);

        options.mode = "signal-batch".to_owned();
        assert_eq!(nominal_workflow_task_target(&options).unwrap(), 4);

        options.mode = "mixed".to_owned();
        options.workflows = u64::MAX;
        let err = nominal_workflow_task_target(&options).unwrap_err();
        assert!(err.contains("does not fit usize") || err.contains("overflowed"));
    }

    #[test]
    fn memory_mixed_workload_completes() {
        let mut options = default_options();
        options.backend = BenchmarkBackend::Memory;
        options.workflows = 4;
        options.workers = 2;
        options.batch = 16;
        options.max_rounds = 100;

        let result = run_memory_benchmark(options).unwrap();
        assert!(result.correct);
        assert_eq!(result.completed_workflows, 4);
        assert_eq!(result.counters.workflow_starts, 4);
        assert_eq!(result.counters.signals, 4);
        assert_eq!(result.counters.child_starts, 4);
        assert_eq!(result.counters.child_completions, 4);
        assert_eq!(result.counters.timer_handlers, 4);
        assert_eq!(result.counters.boot_activities, 4);
        assert_eq!(result.counters.child_activities, 4);
        assert_eq!(result.counters.finish_activities, 4);
        assert_eq!(result.mixed_actions, 32);
        assert_eq!(result.worker_stats.workflow_tasks, 32);
        assert!(result.worker_stats.activity_tasks >= 12);
        assert!(result.worker_stats.child_workflow_starts_dispatched >= 4);
        assert!(result.worker_stats.timers_fired >= 4);
        assert!(
            result
                .backend_metrics
                .operations
                .contains_key("commit_workflow_task")
                || result
                    .backend_metrics
                    .operations
                    .contains_key("commit_workflow_tasks"),
            "benchmark results should include per-backend-method instrumentation"
        );
    }

    #[test]
    fn sqlite_mixed_workload_completes() {
        let mut options = default_options();
        options.workflows = 4;
        options.workers = 2;
        options.batch = 16;
        options.max_rounds = 100;

        let result = run_sqlite_benchmark(options).unwrap();
        assert!(result.correct);
        assert_eq!(result.completed_workflows, 4);
        assert_eq!(result.counters.workflow_starts, 4);
        assert_eq!(result.counters.signals, 4);
        assert_eq!(result.counters.child_starts, 4);
        assert_eq!(result.counters.child_completions, 4);
        assert_eq!(result.mixed_actions, 32);
        assert_eq!(result.worker_stats.workflow_tasks, 32);
    }

    #[test]
    fn memory_signal_batch_workload_records_requested_slots() {
        let mut options = default_options();
        options.backend = BenchmarkBackend::Memory;
        options.mode = "signal-batch".to_owned();
        options.workflows = 4;
        options.workers = 2;
        options.batch = 16;
        options.signal_batch_width = 3;
        options.max_rounds = 100;

        let result = run_memory_benchmark(options).unwrap();
        assert!(result.correct);
        assert_eq!(result.completed_workflows, 4);
        assert_eq!(result.counters.workflow_starts, 4);
        assert_eq!(result.counters.signals, 12);
        assert_eq!(result.mixed_actions, 16);
        assert_eq!(result.worker_stats.workflow_tasks, 4);
        let read = result
            .processing_backend_metrics
            .operations
            .get("read_signal_inboxes")
            .expect("signal batch read metrics");
        assert_eq!(read.calls, 4);
        assert_eq!(read.items, 12);
        assert_eq!(read.items_per_call, 3.0);
    }
}
