use durust::{
    Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, PostgresBackend,
    PostgresBackendConfig, RunId, ShardId, SqliteBackend, StreamHistoryRequest, Worker,
    WorkerRunStats,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_WORKFLOWS: u64 = 250;
const DEFAULT_WORKERS: usize = 4;
const DEFAULT_BATCH: usize = 32;
const DEFAULT_MAX_ROUNDS: usize = 10_000;
const WORKFLOW_QUEUE: &str = "workflows";
const ACTIVITY_QUEUE: &str = "activities";

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
    max_rounds: usize,
    keep_db: bool,
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
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PostgresStatsReport {
    wal_bytes: u64,
    wal_bytes_per_second: f64,
    active_connections_after: u64,
}

#[derive(Clone, Copy, Debug)]
struct PostgresStatsSnapshot {
    wal_bytes: u64,
    active_connections: u64,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    postgres_stats: Option<PostgresStatsReport>,
    db_path: Option<String>,
    db_bytes: Option<u64>,
}

#[derive(Clone, Default)]
struct BackendMetrics {
    workflow_task_commit_latencies: Arc<Mutex<Vec<Duration>>>,
}

impl BackendMetrics {
    fn record_workflow_task_commit(&self, duration: Duration) {
        self.workflow_task_commit_latencies
            .lock()
            .expect("benchmark metrics mutex poisoned")
            .push(duration);
    }

    fn report(&self) -> BackendMetricsReport {
        let samples = self
            .workflow_task_commit_latencies
            .lock()
            .expect("benchmark metrics mutex poisoned")
            .clone();
        BackendMetricsReport {
            workflow_task_commit_latency: latency_report(samples),
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
}

impl<B> MeasuredBackend<B>
where
    B: DurableBackend,
{
    fn new(inner: B, metrics: BackendMetrics) -> Self {
        Self { inner, metrics }
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
        self.inner.start_workflow(req)
    }

    fn cancel_workflow(
        &self,
        req: durust::CancelWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::CancelWorkflowOutcome>> {
        self.inner.cancel_workflow(req)
    }

    fn current_time(&self) -> BoxFuture<'static, durust::Result<durust::TimestampMs>> {
        self.inner.current_time()
    }

    fn claim_workflow_task(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::ClaimedWorkflowTask>>> {
        self.inner.claim_workflow_task(worker_id, opts)
    }

    fn claim_workflow_tasks(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::ClaimWorkflowTasksOptions,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::ClaimedWorkflowTask>>> {
        self.inner.claim_workflow_tasks(worker_id, opts)
    }

    fn stream_history(
        &self,
        req: durust::StreamHistoryRequest,
    ) -> BoxFuture<'static, durust::Result<durust::HistoryChunk>> {
        self.inner.stream_history(req)
    }

    fn stream_history_for_replay(
        &self,
        req: durust::StreamHistoryRequest,
    ) -> BoxFuture<'static, durust::Result<durust::HistoryChunk>> {
        self.inner.stream_history_for_replay(req)
    }

    fn hydrate_payload(
        &self,
        payload: durust::PayloadRef,
    ) -> BoxFuture<'static, durust::Result<durust::PayloadRef>> {
        self.inner.hydrate_payload(payload)
    }

    fn hydrate_activity_map_result_manifest(
        &self,
        payload: durust::PayloadRef,
    ) -> BoxFuture<'static, durust::Result<durust::PayloadRef>> {
        self.inner.hydrate_activity_map_result_manifest(payload)
    }

    fn commit_workflow_task(
        &self,
        claim: durust::WorkflowTaskClaim,
        batch: durust::WorkflowTaskCommit,
    ) -> BoxFuture<'static, durust::Result<durust::CommitOutcome>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.commit_workflow_task(claim, batch).await;
            metrics.record_workflow_task_commit(started.elapsed());
            result
        })
    }

    fn commit_workflow_tasks(
        &self,
        batch: durust::WorkflowTaskCommitBatch,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::WorkflowTaskCommitBatchResult>>> {
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.commit_workflow_tasks(batch).await;
            metrics.record_workflow_task_commit(started.elapsed());
            result
        })
    }

    fn release_workflow_task(
        &self,
        claim: durust::WorkflowTaskClaim,
        release: durust::WorkflowTaskRelease,
    ) -> BoxFuture<'static, durust::Result<()>> {
        self.inner.release_workflow_task(claim, release)
    }

    fn signal_workflow(
        &self,
        req: durust::SignalWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::SignalWorkflowOutcome>> {
        self.inner.signal_workflow(req)
    }

    fn read_signal_inbox(
        &self,
        req: durust::ReadSignalInboxRequest,
    ) -> BoxFuture<'static, durust::Result<Option<durust::SignalInboxRecord>>> {
        self.inner.read_signal_inbox(req)
    }

    fn fire_due_timers(
        &self,
        req: durust::FireDueTimersRequest,
    ) -> BoxFuture<'static, durust::Result<durust::FireDueTimersOutcome>> {
        self.inner.fire_due_timers(req)
    }

    fn timeout_due_activities(
        &self,
        req: durust::TimeoutDueActivitiesRequest,
    ) -> BoxFuture<'static, durust::Result<durust::TimeoutDueActivitiesOutcome>> {
        self.inner.timeout_due_activities(req)
    }

    fn claim_activity_task(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::ClaimActivityOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::ClaimedActivityTask>>> {
        self.inner.claim_activity_task(worker_id, opts)
    }

    fn heartbeat_activity(
        &self,
        req: durust::ActivityHeartbeatRequest,
    ) -> BoxFuture<'static, durust::Result<durust::ActivityHeartbeatOutcome>> {
        self.inner.heartbeat_activity(req)
    }

    fn complete_activity(
        &self,
        req: durust::CompleteActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::CompleteActivityOutcome>> {
        self.inner.complete_activity(req)
    }

    fn fail_activity(
        &self,
        req: durust::FailActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::FailActivityOutcome>> {
        self.inner.fail_activity(req)
    }

    fn dispatch_child_workflow_starts(
        &self,
        req: durust::DispatchChildWorkflowStartsRequest,
    ) -> BoxFuture<'static, durust::Result<durust::DispatchChildWorkflowStartsOutcome>> {
        self.inner.dispatch_child_workflow_starts(req)
    }

    fn query_projection(
        &self,
        req: durust::QueryProjectionRequest,
    ) -> BoxFuture<'static, durust::Result<durust::QueryProjectionOutcome>> {
        self.inner.query_projection(req)
    }

    fn workflow_change_versions(
        &self,
        req: durust::WorkflowChangeVersionsRequest,
    ) -> BoxFuture<'static, durust::Result<durust::WorkflowChangeVersionsOutcome>> {
        self.inner.workflow_change_versions(req)
    }

    fn payload_roots(&self) -> BoxFuture<'static, durust::Result<durust::PayloadRootsOutcome>> {
        self.inner.payload_roots()
    }

    fn gc_payload_blobs(
        &self,
        req: durust::PayloadGarbageCollectionRequest,
    ) -> BoxFuture<'static, durust::Result<durust::PayloadGarbageCollectionOutcome>> {
        self.inner.gc_payload_blobs(req)
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

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let options = parse_args(env::args().skip(1))?;
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
        max_rounds: DEFAULT_MAX_ROUNDS,
        keep_db: false,
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
                if options.mode != "mixed" {
                    return Err("--mode currently supports only `mixed`".to_owned());
                }
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
    if options.backend != BenchmarkBackend::Sqlite && options.keep_db {
        return Err("--keep-db is only meaningful for --backend sqlite".to_owned());
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
        "usage: durust-benchmark-workload [--backend sqlite|memory|postgres] [--mode mixed] \
         [--workflows {DEFAULT_WORKFLOWS}] [--workers {DEFAULT_WORKERS}] [--shards 1] \
         [--physical-partitions 1] [--activation-concurrency 1] \
         [--activation-prefetch-limit 1] \
         [--batch {DEFAULT_BATCH}] [--max-rounds {DEFAULT_MAX_ROUNDS}] [--json]"
    )
}

fn run_memory_benchmark(mut options: BenchmarkOptions) -> Result<BenchmarkResult, String> {
    options.sqlite_layout = None;
    let runtime = tokio_runtime()?;
    run_backend_benchmark(&runtime, MemoryBackend::new(), options, None)
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
    let result = run_backend_benchmark(&runtime, backend, options.clone(), Some(db_path.clone()))?;
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
    let database_url = env::var("DURUST_POSTGRES_URL")
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
    let backend = runtime
        .block_on(PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(database_url.clone())
                .schema(schema.clone())
                .max_pool_size(pool_size)
                .logical_shards(logical_shards)
                .physical_partitions(physical_partitions),
        ))
        .map_err(|err| err.to_string())?;

    let postgres_stats_before = postgres_stats_snapshot(&runtime, &database_url).ok();
    let mut result = run_backend_benchmark(&runtime, backend, options, None);
    let postgres_stats_after = postgres_stats_snapshot(&runtime, &database_url).ok();
    if let (Ok(result), Some(before), Some(after)) =
        (&mut result, postgres_stats_before, postgres_stats_after)
    {
        let wal_bytes = after.wal_bytes.saturating_sub(before.wal_bytes);
        let processing_seconds = (result.processing_ms / 1_000.0).max(f64::EPSILON);
        result.postgres_stats = Some(PostgresStatsReport {
            wal_bytes,
            wal_bytes_per_second: wal_bytes as f64 / processing_seconds,
            active_connections_after: after.active_connections,
        });
    }
    let cleanup = drop_postgres_schema(&runtime, &database_url, &schema);
    match (result, cleanup) {
        (Ok(result), Ok(())) => Ok(result),
        (Ok(_), Err(err)) => Err(err),
        (Err(err), _) => Err(err),
    }
}

fn run_backend_benchmark<B>(
    runtime: &tokio::runtime::Runtime,
    backend: B,
    options: BenchmarkOptions,
    db_path: Option<PathBuf>,
) -> Result<BenchmarkResult, String>
where
    B: DurableBackend,
{
    let metrics = BackendMetrics::default();
    let backend = MeasuredBackend::new(backend, metrics.clone());
    let setup_started = Instant::now();
    let start_outcome = runtime.block_on(start_workflows(&backend, &options))?;
    let setup_finished = Instant::now();

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
        let round_stats =
            drain_worker_round(runtime, &mut workers, options.batch, shared_worker_runtime)?;
        let made_progress = round_stats != WorkerRunStats::default();
        stats = add_worker_stats(stats, round_stats);
        if stats.workflow_tasks >= nominal_workflow_tasks {
            break;
        }
        if !made_progress {
            match runtime.block_on(verify_completed_workflows(&backend, &start_outcome.runs)) {
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

    let verify_started = processing_finished;
    let completed_workflows =
        runtime.block_on(verify_completed_workflows(&backend, &start_outcome.runs))?;
    if completed_workflows != options.workflows {
        return Err(format!(
            "benchmark completed {completed_workflows}/{} workflows",
            options.workflows
        ));
    }
    assert_mixed_stats(&stats, options.workflows)?;
    let verify_finished = Instant::now();

    let counters = counters_from_stats(&start_outcome, &stats);
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
    Ok(BenchmarkResult {
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
        backend_metrics: metrics.report(),
        postgres_stats: None,
        db_path: None,
        db_bytes,
    })
}

fn nominal_workflow_task_target(options: &BenchmarkOptions) -> Result<usize, String> {
    if options.mode != "mixed" {
        return Err(format!("unsupported benchmark mode `{}`", options.mode));
    }
    let workflows = usize::try_from(options.workflows)
        .map_err(|_| format!("workflow count {} does not fit usize", options.workflows))?;
    workflows
        .checked_mul(8)
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
        let workflow_id = format!("mixed-bench-{index}");
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
        .register_workflow(benchmark_parent)
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
        let mut progressed = false;

        let workflow_tasks = worker.run_workflow_batch_once().await?;
        if workflow_tasks > 0 {
            stats.workflow_tasks += workflow_tasks;
            progressed = true;
        }
        let timers_fired = worker.run_timers_once().await?;
        if timers_fired > 0 {
            stats.timers_fired += timers_fired;
            progressed = true;
        }
        let activities_timed_out = worker.run_activity_timeouts_once().await?;
        if activities_timed_out > 0 {
            stats.activities_timed_out += activities_timed_out;
            progressed = true;
        }
        let child_starts = worker.run_child_workflow_starts_once().await?;
        if child_starts > 0 {
            stats.child_workflow_starts_dispatched += child_starts;
            progressed = true;
        }
        if worker.run_activity_once().await? {
            stats.activity_tasks += 1;
            progressed = true;
        }

        if !progressed {
            break;
        }
    }
    Ok(stats)
}

async fn verify_completed_workflows<B>(backend: &B, runs: &[ExpectedRun]) -> Result<u64, String>
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

fn assert_mixed_stats(stats: &WorkerRunStats, workflows: u64) -> Result<(), String> {
    if stats.workflow_tasks < workflows as usize {
        return Err(format!(
            "expected at least one workflow task per workflow, got {stats:?}"
        ));
    }
    if stats.activity_tasks == 0 {
        return Err(format!("expected activity work, got {stats:?}"));
    }
    if stats.timers_fired == 0 {
        return Err(format!("expected timer work, got {stats:?}"));
    }
    Ok(())
}

fn counters_from_stats(start: &StartOutcome, stats: &WorkerRunStats) -> BenchmarkCounters {
    let completed = start.runs.len() as u64;
    BenchmarkCounters {
        workflow_starts: start.workflow_starts,
        signals: start.signals,
        child_starts: completed,
        child_completions: completed,
        timer_handlers: completed,
        boot_activities: completed,
        child_activities: completed,
        finish_activities: completed,
        workflow_tasks: stats.workflow_tasks as u64,
        activity_tasks: stats.activity_tasks as u64,
        timers_fired: stats.timers_fired as u64,
        activities_timed_out: stats.activities_timed_out as u64,
        child_workflow_starts_dispatched: stats.child_workflow_starts_dispatched as u64,
    }
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

fn postgres_benchmark_schema() -> String {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    format!("durust_benchmark_{}_{}", std::process::id(), micros)
}

fn drop_postgres_schema(
    runtime: &tokio::runtime::Runtime,
    database_url: &str,
    schema: &str,
) -> Result<(), String> {
    let database_url = database_url.to_owned();
    let schema = schema.to_owned();
    runtime.block_on(async move {
        let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .map_err(|err| err.to_string())?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("postgres benchmark cleanup connection error: {err}");
            }
        });
        client
            .batch_execute(&format!("drop schema if exists {schema} cascade"))
            .await
            .map_err(|err| err.to_string())
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
            .query_one("select wal_bytes::bigint from pg_stat_wal", &[])
            .await
            .map_err(|err| err.to_string())?
            .get::<_, i64>(0);
        let active_connections = client
            .query_one(
                "select count(*) from pg_stat_activity where datname = current_database()",
                &[],
            )
            .await
            .map_err(|err| err.to_string())?
            .get::<_, i64>(0);
        Ok(PostgresStatsSnapshot {
            wal_bytes: u64::try_from(wal_bytes).unwrap_or(0),
            active_connections: u64::try_from(active_connections).unwrap_or(0),
        })
    })
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
    if let Some(postgres) = &result.postgres_stats {
        println!();
        println!("Postgres stats:");
        println!("  WAL bytes: {}", postgres.wal_bytes);
        println!("  WAL bytes/sec: {:.2}", postgres.wal_bytes_per_second);
        println!(
            "  active connections after: {}",
            postgres.active_connections_after
        );
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
        ])
        .unwrap();
        assert_eq!(options.backend, BenchmarkBackend::Postgres);
        assert_eq!(options.postgres_pool_size, Some(9));
        assert_eq!(options.shards, 100);
        assert_eq!(options.physical_partitions, 16);
        assert_eq!(options.activation_concurrency, 4);
        assert_eq!(options.activation_prefetch_limit, 8);
    }

    #[test]
    fn mixed_workload_has_nominal_workflow_task_target() {
        let mut options = default_options();
        options.workflows = 4;
        assert_eq!(nominal_workflow_task_target(&options).unwrap(), 32);

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
}
