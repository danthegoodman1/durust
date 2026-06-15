use crate::{
    ActivityFailed, ActivityHeartbeatOutcome, ActivityHeartbeatRequest, ActivityId,
    ActivityMapInputManifest, ActivityMapInputPage, ActivityMapItem, ActivityMapResultManifest,
    ActivityMapResultPage, ActivityMapTask, ActivityTask, ActivityTaskClaim, CancelWorkflowOutcome,
    CancelWorkflowRequest, ChildStartOutboxMessage, ClaimActivityOptions,
    ClaimActivityTasksOptions, ClaimWorkflowTaskOptions, ClaimWorkflowTasksOptions,
    ClaimedActivityTask, ClaimedWorkflowTask, CommandId, CommandSeq, CommitOutcome,
    CompleteActivityOutcome, CompleteActivityRequest, CompleteActivityTaskBatchResult,
    CompleteActivityTasksRequest, DispatchChildWorkflowStartsOutcome,
    DispatchChildWorkflowStartsRequest, DurableBackend, DurableFailure, Error, EventId,
    FailActivityOutcome, FailActivityRequest, FireDueTimersOutcome, FireDueTimersRequest,
    HistoryChunk, HistoryEvent, HistoryEventData, HistoryEventType, Namespace, ParentClosePolicy,
    PayloadBlob, PayloadGarbageCollectionOutcome, PayloadGarbageCollectionRequest, PayloadRef,
    PayloadRootRef, PayloadRootsOutcome, PayloadStorageConfig, QueryProjectionOutcome,
    QueryProjectionRequest, ReadSignalInboxRequest, Result, RunDueMaintenanceOutcome,
    RunDueMaintenanceRequest, RunId, ShardId, SignalInboxRecord, SignalWorkflowOutcome,
    SignalWorkflowRequest, StartWorkflowOutcome, StartWorkflowRequest, TimeoutDueActivitiesOutcome,
    TimeoutDueActivitiesRequest, TimestampMs, WaitKind, WorkerId, WorkflowChangeMarkerKind,
    WorkflowChangeVersionRecord, WorkflowChangeVersionStatus, WorkflowChangeVersionsOutcome,
    WorkflowChangeVersionsRequest, WorkflowId, WorkflowTaskClaim, WorkflowTaskCommit,
    WorkflowTaskReason, WorkflowType, activity_map_input_at, digest_bytes,
    encode_activity_map_result_manifest_with_codec, event_payload_len, is_terminal,
};
use deadpool_postgres::{
    Manager, ManagerConfig, Object as PooledPostgresClient, Pool, RecyclingMethod, Runtime,
};
use futures::future::{BoxFuture, ready};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::{NoTls, Transaction};

const POSTGRES_SCHEMA_VERSION: i64 = 1;
const DEFAULT_SCHEMA: &str = "durust";
const DEFAULT_MAX_POOL_SIZE: usize = 16;
const DEFAULT_LOGICAL_SHARDS: u32 = 1;
const DEFAULT_PHYSICAL_PARTITIONS: u32 = 1;
const DEFAULT_SNAPSHOT_INTERVAL: u64 = 10_000;
const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const POSTGRES_TRANSACTION_RETRY_ATTEMPTS: usize = 3;
const POSTGRES_TRANSACTION_RETRY_BASE_DELAY: Duration = Duration::from_millis(2);
const CLAIM_HISTORY_PREFETCH_EVENTS: usize = 16;
const CLAIM_HISTORY_PREFETCH_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ShardJournalOperation {
    kind: ShardJournalOperationKind,
    entries: Vec<ShardJournalEntry>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum ShardJournalOperationKind {
    WorkflowTaskBatch,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ShardJournalEntry {
    run_id: RunId,
    expected_tail_event_id: EventId,
    new_tail_event_id: EventId,
    result: ShardJournalCommitResult,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum ShardJournalCommitResult {
    Committed {
        appended_events: usize,
        terminal: bool,
        ready_reason: Option<String>,
    },
    Conflict,
}

#[derive(Clone, Debug)]
struct AppliedWorkflowTaskCommit {
    outcome: CommitOutcome,
    shard_id: i32,
    lease_epoch: i64,
    journal_entry: ShardJournalEntry,
}

struct LockedActivityCompletion {
    task: ActivityTask,
    claim_token: Option<i64>,
    completed: bool,
}

struct WorkflowCompletionState {
    current_event_id: EventId,
    terminal: bool,
}

struct NormalActivityCompletionCandidate {
    input_index: usize,
    activity_id: ActivityId,
    run_id: RunId,
    command_id: CommandId,
    result: PayloadRef,
}

#[derive(Clone, Debug)]
pub struct PostgresBackendConfig {
    database_url: String,
    schema: String,
    payload_config: PayloadStorageConfig,
    max_pool_size: usize,
    logical_shards: u32,
    physical_partitions: u32,
    snapshot_interval: u64,
    statement_timeout: Duration,
    lock_timeout: Duration,
}

impl PostgresBackendConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            schema: DEFAULT_SCHEMA.to_owned(),
            payload_config: PayloadStorageConfig::default(),
            max_pool_size: DEFAULT_MAX_POOL_SIZE,
            logical_shards: DEFAULT_LOGICAL_SHARDS,
            physical_partitions: DEFAULT_PHYSICAL_PARTITIONS,
            snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
            statement_timeout: DEFAULT_STATEMENT_TIMEOUT,
            lock_timeout: DEFAULT_LOCK_TIMEOUT,
        }
    }

    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    pub fn payload_storage(mut self, payload_config: PayloadStorageConfig) -> Self {
        self.payload_config = payload_config;
        self
    }

    pub fn max_pool_size(mut self, max_pool_size: usize) -> Self {
        self.max_pool_size = max_pool_size.max(1);
        self
    }

    pub fn logical_shards(mut self, logical_shards: u32) -> Self {
        self.logical_shards = logical_shards.max(1);
        self
    }

    pub fn physical_partitions(mut self, physical_partitions: u32) -> Self {
        self.physical_partitions = physical_partitions.max(1);
        self
    }

    pub fn snapshot_interval(mut self, snapshot_interval: u64) -> Self {
        self.snapshot_interval = snapshot_interval.max(1);
        self
    }

    pub fn statement_timeout(mut self, timeout: Duration) -> Self {
        self.statement_timeout = timeout;
        self
    }

    pub fn lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
    }
}

#[derive(Clone, Debug)]
pub struct PostgresBackend {
    pool: Pool,
    schema: String,
    payload_config: PayloadStorageConfig,
    logical_shards: u32,
    physical_partitions: u32,
    snapshot_interval: u64,
    statement_timeout: Duration,
    lock_timeout: Duration,
}

impl PostgresBackend {
    pub async fn connect(database_url: impl AsRef<str>) -> Result<Self> {
        Self::connect_with_config(PostgresBackendConfig::new(database_url.as_ref())).await
    }

    pub async fn connect_with_payload_storage(
        database_url: impl AsRef<str>,
        payload_config: PayloadStorageConfig,
    ) -> Result<Self> {
        Self::connect_with_config(
            PostgresBackendConfig::new(database_url.as_ref()).payload_storage(payload_config),
        )
        .await
    }

    pub async fn connect_with_config(config: PostgresBackendConfig) -> Result<Self> {
        validate_identifier(&config.schema)?;
        let mut pg_config: tokio_postgres::Config = config
            .database_url
            .parse()
            .map_err(|err| Error::Backend(format!("postgres database URL parse error: {err}")))?;
        let postgres_options = format!(
            "-c statement_timeout={} -c lock_timeout={}",
            duration_millis_i64(config.statement_timeout),
            duration_millis_i64(config.lock_timeout),
        );
        pg_config.options(&postgres_options);
        let manager = Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(config.max_pool_size.max(1))
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|err| Error::Backend(format!("postgres pool build error: {err}")))?;

        let backend = Self {
            pool,
            schema: config.schema,
            payload_config: config.payload_config,
            logical_shards: config.logical_shards.max(1),
            physical_partitions: config.physical_partitions.max(1),
            snapshot_interval: config.snapshot_interval.max(1),
            statement_timeout: config.statement_timeout,
            lock_timeout: config.lock_timeout,
        };
        backend.migrate().await?;
        Ok(backend)
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn logical_shards(&self) -> u32 {
        self.logical_shards
    }

    pub fn physical_partitions(&self) -> u32 {
        self.physical_partitions
    }

    pub fn shard_for_workflow(&self, namespace: &Namespace, workflow_id: &WorkflowId) -> ShardId {
        shard_for_workflow(namespace, workflow_id, self.logical_shards)
    }

    pub async fn schema_version(&self) -> Result<u32> {
        let client = self.client().await?;
        let row = client
            .query_one(
                &format!(
                    "select value from {}.meta where key = 'schema_version'",
                    quote_ident(&self.schema)
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        let value: i64 = row.get(0);
        u32::try_from(value).map_err(|_| {
            Error::Backend(format!(
                "postgres schema `{}` has invalid schema version {value}",
                self.schema
            ))
        })
    }

    async fn migrate(&self) -> Result<()> {
        let schema = quote_ident(&self.schema);
        let client = self.client().await?;
        client
            .batch_execute(&format!(
                "
                create schema if not exists {schema};

                create table if not exists {schema}.meta (
                    key text primary key,
                    value bigint not null
                );
                "
            ))
            .await
            .map_err(postgres_error)?;

        let existing = client
            .query_opt(
                &format!("select value from {schema}.meta where key = 'schema_version'"),
                &[],
            )
            .await
            .map_err(postgres_error)?
            .map(|row| row.get::<_, i64>(0));
        if let Some(version) = existing {
            if version != POSTGRES_SCHEMA_VERSION {
                return Err(Error::Backend(format!(
                    "postgres schema `{}` has version {version}, expected {POSTGRES_SCHEMA_VERSION}",
                    self.schema
                )));
            }
        }

        client
            .batch_execute(&format!(
                "
                begin;

                create sequence if not exists {schema}.claim_token_seq;
                create sequence if not exists {schema}.run_id_seq;
                create sequence if not exists {schema}.signal_seq;

                create table if not exists {schema}.workflow_instances (
                    namespace text not null,
                    workflow_id text not null,
                    run_id text primary key,
                    shard_id integer not null default 0,
                    workflow_name text not null,
                    workflow_version integer not null,
                    task_queue text not null,
                    current_event_id bigint not null,
                    ready_reason text,
                    ready_at_ms bigint not null default 0,
                    workflow_claim_token bigint,
                    terminal boolean not null,
                    parent_run_id text,
                    parent_command_seq bigint,
                    parent_close_policy text,
                    unique(namespace, workflow_id)
                );

                alter table {schema}.workflow_instances
                    add column if not exists shard_id integer not null default 0;

                create table if not exists {schema}.history_events (
                    run_id text not null,
                    event_id bigint not null,
                    event_type text not null,
                    data bytea not null,
                    primary key(run_id, event_id)
                );

                create table if not exists {schema}.payload_blobs (
                    digest text primary key,
                    codec text not null,
                    schema_fingerprint text not null,
                    compression text not null,
                    encryption bytea,
                    size bigint not null,
                    bytes bytea
                );

                create table if not exists {schema}.activity_tasks (
                    activity_id text primary key,
                    namespace text not null,
                    run_id text not null,
                    activity_name text not null,
                    task_queue text not null,
                    task bytea not null,
                    claim_token bigint,
                    completed boolean not null,
                    timeout_at_ms bigint,
                    heartbeat_deadline_at_ms bigint
                );

                create table if not exists {schema}.activity_maps (
                    map_command_id text primary key,
                    namespace text not null,
                    run_id text not null,
                    command_seq bigint not null,
                    task bytea not null,
                    item_count bigint not null,
                    next_ordinal bigint not null,
                    in_flight bigint not null,
                    completed boolean not null
                );

                create table if not exists {schema}.activity_map_results (
                    map_command_id text not null,
                    item_ordinal bigint not null,
                    result bytea not null,
                    primary key(map_command_id, item_ordinal)
                );

                create table if not exists {schema}.active_waits (
                    wait_id text primary key,
                    namespace text not null,
                    run_id text not null,
                    command_seq bigint not null,
                    kind text not null,
                    wait_key text not null,
                    ready_at_ms bigint
                );

                create index if not exists idx_active_waits_timer_due
                    on {schema}.active_waits(namespace, kind, ready_at_ms, wait_id);

                create index if not exists idx_active_waits_signal
                    on {schema}.active_waits(run_id, kind, wait_key);

                create table if not exists {schema}.query_projections (
                    namespace text not null,
                    workflow_id text not null,
                    run_id text not null,
                    event_id bigint not null,
                    payload bytea not null,
                    primary key(namespace, workflow_id)
                );

                create table if not exists {schema}.workflow_change_versions (
                    namespace text not null,
                    workflow_id text not null,
                    workflow_name text not null,
                    workflow_version integer not null,
                    run_id text not null,
                    change_id text not null,
                    version integer not null,
                    marker_kind text not null,
                    command_seq bigint not null,
                    first_event_id bigint not null,
                    last_seen_at_ms bigint not null,
                    primary key(run_id, change_id)
                );

                create index if not exists idx_workflow_change_versions_change
                    on {schema}.workflow_change_versions(namespace, change_id, run_id);

                create index if not exists idx_workflow_change_versions_workflow
                    on {schema}.workflow_change_versions(namespace, workflow_id, change_id);

                create index if not exists idx_workflow_instances_ready
                    on {schema}.workflow_instances(namespace, task_queue, ready_at_ms, run_id)
                    where ready_reason is not null
                      and workflow_claim_token is null
                      and terminal = false;

                create index if not exists idx_workflow_instances_ready_shard
                    on {schema}.workflow_instances(namespace, shard_id, task_queue, ready_at_ms, run_id)
                    where ready_reason is not null
                      and workflow_claim_token is null
                      and terminal = false;

                create table if not exists {schema}.signals (
                    signal_id text primary key,
                    namespace text not null,
                    run_id text not null,
                    signal_name text not null,
                    payload bytea not null,
                    received_sequence bigint not null,
                    consumed boolean not null
                );

                create index if not exists idx_signals_inbox
                    on {schema}.signals(run_id, signal_name, consumed, received_sequence);

                create index if not exists idx_activity_tasks_timeout_due
                    on {schema}.activity_tasks(namespace, completed, timeout_at_ms, activity_id);

                create index if not exists idx_activity_tasks_heartbeat_due
                    on {schema}.activity_tasks(namespace, completed, heartbeat_deadline_at_ms, activity_id);

                insert into {schema}.meta(key, value)
                values ('schema_version', {POSTGRES_SCHEMA_VERSION})
                on conflict(key) do update set value = excluded.value;

                with existing(value) as (
                    select greatest(
                        coalesce((select value from {schema}.meta where key = 'claim'), 0),
                        coalesce((select max(workflow_claim_token) from {schema}.workflow_instances), 0),
                        coalesce((select max(claim_token) from {schema}.activity_tasks), 0)
                    )
                )
                select setval('{schema}.claim_token_seq'::regclass, greatest(value, 1), value > 0)
                from existing
                where not (select is_called from {schema}.claim_token_seq);

                with existing(value) as (
                    select greatest(
                        coalesce((select value from {schema}.meta where key = 'run'), 0),
                        coalesce((
                            select max(substring(run_id from '^run-([0-9]+)$')::bigint)
                            from {schema}.workflow_instances
                        ), 0)
                    )
                )
                select setval('{schema}.run_id_seq'::regclass, greatest(value, 1), value > 0)
                from existing
                where not (select is_called from {schema}.run_id_seq);

                with existing(value) as (
                    select greatest(
                        coalesce((select value from {schema}.meta where key = 'signal'), 0),
                        coalesce((select max(received_sequence) from {schema}.signals), 0)
                    )
                )
                select setval('{schema}.signal_seq'::regclass, greatest(value, 1), value > 0)
                from existing
                where not (select is_called from {schema}.signal_seq);

                commit;
                "
            ))
            .await
            .map_err(postgres_error)?;

        self.validate_or_insert_provider_metadata(&client).await?;
        self.ensure_shard_native_tables(&client).await
    }

    async fn validate_or_insert_provider_metadata(
        &self,
        client: &PooledPostgresClient,
    ) -> Result<()> {
        let schema = self.schema_sql();
        let expected = BTreeMap::from([
            ("logical_shards", i64::from(self.logical_shards)),
            ("physical_partitions", i64::from(self.physical_partitions)),
            (
                "snapshot_interval",
                i64::try_from(self.snapshot_interval).unwrap_or(i64::MAX),
            ),
            (
                "statement_timeout_ms",
                duration_millis_i64(self.statement_timeout),
            ),
            ("lock_timeout_ms", duration_millis_i64(self.lock_timeout)),
        ]);

        for (key, expected_value) in expected {
            let row = client
                .query_opt(
                    &format!("select value from {schema}.meta where key = $1"),
                    &[&key],
                )
                .await
                .map_err(postgres_error)?;
            match row {
                Some(row) => {
                    let actual: i64 = row.get(0);
                    if actual != expected_value {
                        return Err(Error::Backend(format!(
                            "postgres schema `{}` metadata mismatch for `{key}`: stored {actual}, configured {expected_value}",
                            self.schema
                        )));
                    }
                }
                None => {
                    client
                        .execute(
                            &format!("insert into {schema}.meta(key, value) values ($1, $2)"),
                            &[&key, &expected_value],
                        )
                        .await
                        .map_err(postgres_error)?;
                }
            }
        }

        Ok(())
    }

    async fn ensure_shard_native_tables(&self, client: &PooledPostgresClient) -> Result<()> {
        let schema = self.schema_sql();
        client
            .batch_execute(&format!(
                "
                create table if not exists {schema}.shard_leases (
                    shard_id integer primary key,
                    owner_id text,
                    lease_epoch bigint not null default 0,
                    lease_until_ms bigint
                );
                "
            ))
            .await
            .map_err(postgres_error)?;

        for partition in 0..self.physical_partitions {
            let suffix = partition_suffix(partition, self.physical_partitions);
            client
                .batch_execute(&format!(
                    "
                    create table if not exists {schema}.shard_heads_{suffix} (
                        shard_id integer primary key,
                        journal_seq bigint not null,
                        snapshot_seq bigint not null,
                        updated_at_ms bigint not null
                    );

                    create table if not exists {schema}.shard_journal_{suffix} (
                        shard_id integer not null,
                        journal_seq bigint not null,
                        lease_epoch bigint not null,
                        operation bytea not null,
                        appended_at_ms bigint not null,
                        primary key(shard_id, journal_seq)
                    );

                    create table if not exists {schema}.shard_snapshots_{suffix} (
                        shard_id integer not null,
                        snapshot_seq bigint not null,
                        journal_seq bigint not null,
                        snapshot bytea not null,
                        created_at_ms bigint not null,
                        primary key(shard_id, snapshot_seq)
                    );

                    create table if not exists {schema}.history_events_{suffix} (
                        run_id text not null,
                        event_id bigint not null,
                        event_type text not null,
                        data bytea not null,
                        primary key(run_id, event_id)
                    );
                    "
                ))
                .await
                .map_err(postgres_error)?;
        }

        for shard_id in 0..self.logical_shards {
            client
                .execute(
                    &format!(
                        "insert into {schema}.shard_leases(shard_id, lease_epoch)
                         values ($1, 0)
                         on conflict(shard_id) do nothing"
                    ),
                    &[&(i32::try_from(shard_id).unwrap_or(i32::MAX))],
                )
                .await
                .map_err(postgres_error)?;
        }
        Ok(())
    }

    async fn refresh_shard_leases_tx(
        &self,
        tx: &Transaction<'_>,
        worker_id: &WorkerId,
        shards: &[ShardId],
        lease_duration: Duration,
        now_ms: i64,
    ) -> Result<Vec<ShardId>> {
        let schema = self.schema_sql();
        let lease_until_ms = now_ms.saturating_add(duration_millis_i64(lease_duration));
        if shards.is_empty() {
            return Ok(Vec::new());
        }
        let shard_ids = shards
            .iter()
            .map(|shard| i32::try_from(shard.0).unwrap_or(i32::MAX))
            .collect::<Vec<_>>();
        let rows = tx
            .query(
                &format!(
                    "with requested(shard_id, ordinal) as (
                         select shard_id, ordinal
                         from unnest($4::integer[]) with ordinality as requested(shard_id, ordinal)
                     ),
                     lockable as (
                         select leases.shard_id, requested.ordinal
                         from {schema}.shard_leases leases
                         join requested on requested.shard_id = leases.shard_id
                         where leases.owner_id is null
                            or leases.owner_id = $1
                            or leases.lease_until_ms <= $2
                         order by leases.shard_id asc
                         for update of leases
                     ),
                     updated as (
                         update {schema}.shard_leases leases
                         set owner_id = $1,
                             lease_epoch = case
                                 when leases.owner_id = $1 and leases.lease_until_ms > $2 then leases.lease_epoch
                                 else leases.lease_epoch + 1
                             end,
                             lease_until_ms = $3
                         from lockable
                         where leases.shard_id = lockable.shard_id
                         returning leases.shard_id, lockable.ordinal
                     )
                     select shard_id
                     from updated
                     order by ordinal asc"
                ),
                &[&worker_id.0, &now_ms, &lease_until_ms, &shard_ids],
            )
            .await
            .map_err(postgres_error)?;
        Ok(rows
            .into_iter()
            .map(|row| ShardId(u32::try_from(row.get::<_, i32>(0)).unwrap_or(u32::MAX)))
            .collect())
    }

    async fn verify_shard_lease_tx(
        &self,
        tx: &Transaction<'_>,
        worker_id: &WorkerId,
        shard_id: i32,
    ) -> Result<i64> {
        if self.logical_shards <= 1 {
            return Ok(0);
        }
        let schema = self.schema_sql();
        let now_ms = unix_epoch_millis();
        let Some(row) = tx
            .query_opt(
                &format!(
                    "select owner_id, lease_until_ms, lease_epoch
                     from {schema}.shard_leases
                     where shard_id = $1"
                ),
                &[&shard_id],
            )
            .await
            .map_err(postgres_error)?
        else {
            return Err(Error::StaleLease);
        };
        let owner_id: Option<String> = row.get(0);
        let lease_until_ms: Option<i64> = row.get(1);
        let lease_epoch: i64 = row.get(2);
        if owner_id.as_deref() == Some(worker_id.0.as_str())
            && lease_until_ms.is_some_and(|lease_until_ms| lease_until_ms > now_ms)
        {
            Ok(lease_epoch)
        } else {
            Err(Error::StaleLease)
        }
    }

    #[cfg(test)]
    async fn force_schema_version_for_tests(&self, version: i64) -> Result<()> {
        let client = self.client().await?;
        client
            .execute(
                &format!(
                    "update {}.meta set value = $1 where key = 'schema_version'",
                    quote_ident(&self.schema)
                ),
                &[&version],
            )
            .await
            .map_err(postgres_error)?;
        Ok(())
    }

    #[cfg(test)]
    async fn drop_schema_for_tests(&self) -> Result<()> {
        let client = self.client().await?;
        client
            .batch_execute(&format!(
                "drop schema if exists {} cascade",
                quote_ident(&self.schema)
            ))
            .await
            .map_err(postgres_error)
    }
}

impl DurableBackend for PostgresBackend {
    fn payload_storage_config(&self) -> PayloadStorageConfig {
        self.payload_config.clone()
    }

    fn start_workflow(
        &self,
        req: StartWorkflowRequest,
    ) -> BoxFuture<'static, Result<StartWorkflowOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.start_workflow_inner(req).await })
    }

    fn cancel_workflow(
        &self,
        req: CancelWorkflowRequest,
    ) -> BoxFuture<'static, Result<CancelWorkflowOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.cancel_workflow_inner(req).await })
    }

    fn current_time(&self) -> BoxFuture<'static, Result<TimestampMs>> {
        Box::pin(ready(Ok(TimestampMs(unix_epoch_millis()))))
    }

    fn claim_workflow_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedWorkflowTask>>> {
        let backend = self.clone();
        Box::pin(async move { backend.claim_workflow_task_inner(worker_id, opts).await })
    }

    fn claim_workflow_tasks(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTasksOptions,
    ) -> BoxFuture<'static, Result<Vec<ClaimedWorkflowTask>>> {
        let backend = self.clone();
        Box::pin(async move { backend.claim_workflow_tasks_inner(worker_id, opts).await })
    }

    fn stream_history(
        &self,
        req: crate::StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        let backend = self.clone();
        Box::pin(async move { backend.stream_history_inner(req, true).await })
    }

    fn stream_history_for_replay(
        &self,
        req: crate::StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        let backend = self.clone();
        Box::pin(async move { backend.stream_history_inner(req, false).await })
    }

    fn hydrate_payload(&self, payload: PayloadRef) -> BoxFuture<'static, Result<PayloadRef>> {
        let backend = self.clone();
        Box::pin(async move { backend.hydrate_payload_from_storage(payload).await })
    }

    fn hydrate_activity_map_result_manifest(
        &self,
        payload: PayloadRef,
    ) -> BoxFuture<'static, Result<PayloadRef>> {
        let backend = self.clone();
        Box::pin(async move {
            backend
                .hydrate_activity_map_result_manifest_from_storage(payload)
                .await
        })
    }

    fn commit_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> BoxFuture<'static, Result<CommitOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.commit_workflow_task_inner(claim, batch).await })
    }

    fn commit_workflow_tasks(
        &self,
        batch: crate::WorkflowTaskCommitBatch,
    ) -> BoxFuture<'static, Result<Vec<crate::WorkflowTaskCommitBatchResult>>> {
        let backend = self.clone();
        Box::pin(async move { backend.commit_workflow_tasks_inner(batch).await })
    }

    fn release_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        release: crate::WorkflowTaskRelease,
    ) -> BoxFuture<'static, Result<()>> {
        let backend = self.clone();
        Box::pin(async move { backend.release_workflow_task_inner(claim, release).await })
    }

    fn signal_workflow(
        &self,
        req: SignalWorkflowRequest,
    ) -> BoxFuture<'static, Result<SignalWorkflowOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.signal_workflow_inner(req).await })
    }

    fn read_signal_inbox(
        &self,
        req: ReadSignalInboxRequest,
    ) -> BoxFuture<'static, Result<Option<SignalInboxRecord>>> {
        let backend = self.clone();
        Box::pin(async move { backend.read_signal_inbox_inner(req).await })
    }

    fn fire_due_timers(
        &self,
        req: FireDueTimersRequest,
    ) -> BoxFuture<'static, Result<FireDueTimersOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.fire_due_timers_inner(req).await })
    }

    fn timeout_due_activities(
        &self,
        req: TimeoutDueActivitiesRequest,
    ) -> BoxFuture<'static, Result<TimeoutDueActivitiesOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.timeout_due_activities_inner(req).await })
    }

    fn run_due_maintenance(
        &self,
        req: RunDueMaintenanceRequest,
    ) -> BoxFuture<'static, Result<RunDueMaintenanceOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.run_due_maintenance_inner(req).await })
    }

    fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedActivityTask>>> {
        let backend = self.clone();
        Box::pin(async move { backend.claim_activity_task_inner(worker_id, opts).await })
    }

    fn claim_activity_tasks(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityTasksOptions,
    ) -> BoxFuture<'static, Result<Vec<ClaimedActivityTask>>> {
        let backend = self.clone();
        Box::pin(async move { backend.claim_activity_tasks_inner(worker_id, opts).await })
    }

    fn heartbeat_activity(
        &self,
        req: ActivityHeartbeatRequest,
    ) -> BoxFuture<'static, Result<ActivityHeartbeatOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.heartbeat_activity_inner(req).await })
    }

    fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> BoxFuture<'static, Result<CompleteActivityOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.complete_activity_inner(req).await })
    }

    fn complete_activity_tasks(
        &self,
        req: CompleteActivityTasksRequest,
    ) -> BoxFuture<'static, Result<Vec<CompleteActivityTaskBatchResult>>> {
        let backend = self.clone();
        Box::pin(async move { backend.complete_activity_tasks_inner(req).await })
    }

    fn fail_activity(
        &self,
        req: FailActivityRequest,
    ) -> BoxFuture<'static, Result<FailActivityOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.fail_activity_inner(req).await })
    }

    fn dispatch_child_workflow_starts(
        &self,
        _req: DispatchChildWorkflowStartsRequest,
    ) -> BoxFuture<'static, Result<DispatchChildWorkflowStartsOutcome>> {
        Box::pin(ready(Ok(DispatchChildWorkflowStartsOutcome {
            dispatched: 0,
        })))
    }

    fn query_projection(
        &self,
        req: QueryProjectionRequest,
    ) -> BoxFuture<'static, Result<QueryProjectionOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.query_projection_inner(req).await })
    }

    fn workflow_change_versions(
        &self,
        req: WorkflowChangeVersionsRequest,
    ) -> BoxFuture<'static, Result<WorkflowChangeVersionsOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.workflow_change_versions_inner(req).await })
    }

    fn payload_roots(&self) -> BoxFuture<'static, Result<PayloadRootsOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.payload_roots_inner().await })
    }

    fn gc_payload_blobs(
        &self,
        req: PayloadGarbageCollectionRequest,
    ) -> BoxFuture<'static, Result<PayloadGarbageCollectionOutcome>> {
        let backend = self.clone();
        Box::pin(async move { backend.gc_payload_blobs_inner(req).await })
    }
}

impl PostgresBackend {
    async fn retry_transaction<T, F, Fut>(&self, _operation: &'static str, mut body: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt = 0usize;
        loop {
            match body().await {
                Err(err)
                    if is_retryable_postgres_transaction_abort(&err)
                        && attempt + 1 < POSTGRES_TRANSACTION_RETRY_ATTEMPTS =>
                {
                    let shift = u32::try_from(attempt).unwrap_or(u32::MAX).min(10);
                    let multiplier = 1_u32.checked_shl(shift).unwrap_or(1);
                    tokio::time::sleep(POSTGRES_TRANSACTION_RETRY_BASE_DELAY * multiplier).await;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    async fn start_workflow_inner(
        &self,
        req: StartWorkflowRequest,
    ) -> Result<StartWorkflowOutcome> {
        self.retry_transaction("start_workflow", || {
            let req = req.clone();
            async move { self.start_workflow_once(req).await }
        })
        .await
    }

    async fn start_workflow_once(&self, req: StartWorkflowRequest) -> Result<StartWorkflowOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        if let Some(row) = tx
            .query_opt(
                &format!(
                    "select run_id from {schema}.workflow_instances where namespace = $1 and workflow_id = $2"
                ),
                &[&req.namespace.0, &req.workflow_id.0],
            )
            .await
            .map_err(postgres_error)?
        {
            let run_id: String = row.get(0);
            tx.commit().await.map_err(postgres_error)?;
            return Ok(StartWorkflowOutcome::AlreadyStarted {
                run_id: RunId::new(run_id),
            });
        }

        let input = self
            .normalize_payload_for_storage_tx(&tx, req.input)
            .await?;
        let run_id = next_run_id(&tx, &schema).await?;
        let shard_id = i32::try_from(self.shard_for_workflow(&req.namespace, &req.workflow_id).0)
            .unwrap_or(i32::MAX);
        let start = HistoryEventData::WorkflowStarted {
            workflow_type: req.workflow_type.clone(),
            input,
        };
        tx.execute(
            &format!(
                "insert into {schema}.workflow_instances
                 (namespace, workflow_id, run_id, shard_id, workflow_name, workflow_version, task_queue,
                  current_event_id, ready_reason, ready_at_ms, workflow_claim_token, terminal,
                  parent_run_id, parent_command_seq, parent_close_policy)
                 values ($1, $2, $3, $4, $5, $6, $7, 1, $8, 0, null, false, null, null, null)"
            ),
            &[
                &req.namespace.0,
                &req.workflow_id.0,
                &run_id.0,
                &shard_id,
                &req.workflow_type.name,
                &(i32::try_from(req.workflow_type.version).unwrap_or(i32::MAX)),
                &req.task_queue.0,
                &reason_to_str(&WorkflowTaskReason::WorkflowStarted),
            ],
        )
        .await
        .map_err(postgres_error)?;
        insert_history_event(&tx, &schema, &run_id, EventId(1), start).await?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(StartWorkflowOutcome::Started { run_id })
    }

    async fn cancel_workflow_inner(
        &self,
        req: CancelWorkflowRequest,
    ) -> Result<CancelWorkflowOutcome> {
        self.retry_transaction("cancel_workflow", || {
            let req = req.clone();
            async move { self.cancel_workflow_once(req).await }
        })
        .await
    }

    async fn cancel_workflow_once(
        &self,
        req: CancelWorkflowRequest,
    ) -> Result<CancelWorkflowOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let Some(row) = tx
            .query_opt(
                &format!(
                    "select run_id, current_event_id, terminal
                     from {schema}.workflow_instances
                     where namespace = $1 and workflow_id = $2
                     for update"
                ),
                &[&req.namespace.0, &req.workflow_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            return Err(Error::Backend(format!(
                "workflow `{}` was not found",
                req.workflow_id.0
            )));
        };
        let run_id = RunId::new(row.get::<_, String>(0));
        let tail = EventId(u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX));
        let terminal: bool = row.get(2);
        if terminal {
            tx.commit().await.map_err(postgres_error)?;
            return Ok(CancelWorkflowOutcome::AlreadyTerminal { run_id });
        }

        let event_id = tail.next();
        let terminal_event = HistoryEventData::WorkflowCancelled { reason: req.reason };
        insert_history_event(&tx, &schema, &run_id, event_id, terminal_event.clone()).await?;
        cleanup_run_operational_state_tx(&tx, &schema, &run_id).await?;
        tx.execute(
            &format!(
                "update {schema}.workflow_instances
                 set current_event_id = $1,
                     workflow_claim_token = null,
                     terminal = true,
                     ready_reason = null,
                     ready_at_ms = 0
                 where run_id = $2"
            ),
            &[&i64::try_from(event_id.0).unwrap_or(i64::MAX), &run_id.0],
        )
        .await
        .map_err(postgres_error)?;
        handle_terminal_run_tx(&tx, &schema, &run_id, &terminal_event).await?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(CancelWorkflowOutcome::Cancelled { run_id, event_id })
    }

    async fn claim_workflow_task_inner(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
    ) -> Result<Option<ClaimedWorkflowTask>> {
        self.claim_workflow_task_inner_filtered(worker_id, opts, None)
            .await
    }

    async fn claim_workflow_tasks_inner(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTasksOptions,
    ) -> Result<Vec<ClaimedWorkflowTask>> {
        self.retry_transaction("claim_workflow_tasks", || {
            let worker_id = worker_id.clone();
            let opts = opts.clone();
            async move { self.claim_workflow_tasks_once(worker_id, opts).await }
        })
        .await
    }

    async fn claim_workflow_tasks_once(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTasksOptions,
    ) -> Result<Vec<ClaimedWorkflowTask>> {
        if opts.limit == 0 || opts.claim.registered_workflow_types.is_empty() {
            return Ok(Vec::new());
        }
        let registered_names = opts
            .claim
            .registered_workflow_types
            .iter()
            .map(|workflow_type| workflow_type.name.clone())
            .collect::<Vec<_>>();
        let registered_versions = opts
            .claim
            .registered_workflow_types
            .iter()
            .map(|workflow_type| i32::try_from(workflow_type.version).unwrap_or(i32::MAX))
            .collect::<Vec<_>>();
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let now_ms = unix_epoch_millis();
        let shard_ids = match opts.shard_filter {
            Some(shards) => {
                if shards.is_empty() {
                    tx.commit().await.map_err(postgres_error)?;
                    return Ok(Vec::new());
                }
                Some(
                    shards
                        .into_iter()
                        .map(|shard| i32::try_from(shard.0).unwrap_or(i32::MAX))
                        .collect::<Vec<_>>(),
                )
            }
            None => None,
        };

        let rows = tx
            .query(
                &format!(
                    "select run_id, workflow_id, workflow_name, workflow_version, current_event_id, ready_reason, shard_id
                     from {schema}.workflow_instances
                     where namespace = $1
                       and task_queue = $2
                       and ready_reason is not null
                       and ready_at_ms <= $3
                       and workflow_claim_token is null
                       and terminal = false
                       and ($6::integer[] is null or shard_id = any($6::integer[]))
                       and (workflow_name, workflow_version) in (
                         select registered.workflow_name, registered.workflow_version
                         from unnest($4::text[], $5::integer[])
                              as registered(workflow_name, workflow_version)
                       )
                     order by ready_at_ms asc, run_id asc
                     limit $7
                     for update skip locked"
                ),
                &[
                    &opts.claim.namespace.0,
                    &opts.claim.task_queue.0,
                    &now_ms,
                    &registered_names,
                    &registered_versions,
                    &shard_ids,
                    &i64::try_from(opts.limit).unwrap_or(i64::MAX),
                ],
            )
            .await
            .map_err(postgres_error)?;

        let mut selected = rows
            .into_iter()
            .map(|row| {
                let workflow_type = WorkflowType::new(
                    row.get::<_, String>(2),
                    u32::try_from(row.get::<_, i32>(3)).unwrap_or(0),
                );
                Ok((
                    RunId::new(row.get::<_, String>(0)),
                    crate::WorkflowId::new(row.get::<_, String>(1)),
                    workflow_type,
                    EventId(row.get::<_, i64>(4).try_into().unwrap_or(u64::MAX)),
                    reason_from_str(&row.get::<_, String>(5))?,
                    row.get::<_, i32>(6),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        if selected.is_empty() {
            tx.commit().await.map_err(postgres_error)?;
            return Ok(Vec::new());
        }

        if self.logical_shards > 1 {
            let unique_shards = selected
                .iter()
                .map(|(_, _, _, _, _, shard_id)| {
                    ShardId(u32::try_from(*shard_id).unwrap_or(u32::MAX))
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let owned = self
                .refresh_shard_leases_tx(
                    &tx,
                    &worker_id,
                    &unique_shards,
                    opts.claim.lease_duration,
                    now_ms,
                )
                .await?
                .into_iter()
                .collect::<BTreeSet<_>>();
            selected.retain(|(_, _, _, _, _, shard_id)| {
                owned.contains(&ShardId(u32::try_from(*shard_id).unwrap_or(u32::MAX)))
            });
            if selected.is_empty() {
                tx.commit().await.map_err(postgres_error)?;
                return Ok(Vec::new());
            }
        }

        let history_targets = selected
            .iter()
            .map(|(run_id, _, _, tail, _, _)| (run_id.clone(), *tail))
            .collect::<Vec<_>>();
        let prefetched_histories = self
            .prefetch_claim_histories_tx(&tx, &schema, &history_targets)
            .await?;

        let token_rows = tx
            .query(
                &format!(
                    "select nextval('{schema}.claim_token_seq'::regclass)
                     from generate_series(1::bigint, $1::bigint)"
                ),
                &[&i64::try_from(selected.len()).unwrap_or(i64::MAX)],
            )
            .await
            .map_err(postgres_error)?;
        let tokens = token_rows
            .into_iter()
            .map(|row| {
                let token: i64 = row.get(0);
                u64::try_from(token).map_err(|_| {
                    Error::Backend(format!(
                        "postgres claim token sequence returned invalid value {token}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let run_ids = selected
            .iter()
            .map(|(run_id, _, _, _, _, _)| run_id.0.clone())
            .collect::<Vec<_>>();
        let token_values = tokens
            .iter()
            .map(|token| i64::try_from(*token).unwrap_or(i64::MAX))
            .collect::<Vec<_>>();
        tx.execute(
            &format!(
                "update {schema}.workflow_instances workflows
                 set workflow_claim_token = claimed.claim_token,
                     ready_reason = null,
                     ready_at_ms = 0
                 from unnest($1::text[], $2::bigint[]) as claimed(run_id, claim_token)
                 where workflows.run_id = claimed.run_id"
            ),
            &[&run_ids, &token_values],
        )
        .await
        .map_err(postgres_error)?;

        let mut claimed = Vec::with_capacity(selected.len());
        for ((run_id, workflow_id, workflow_type, tail, reason, _), token) in
            selected.into_iter().zip(tokens.into_iter())
        {
            let prefetched_history = prefetched_histories
                .get(&run_id)
                .cloned()
                .unwrap_or_default();
            claimed.push(ClaimedWorkflowTask {
                run_id: run_id.clone(),
                workflow_id,
                workflow_type,
                claim: WorkflowTaskClaim {
                    run_id,
                    worker_id: worker_id.clone(),
                    token,
                },
                replay_target_event_id: tail,
                reason,
                prefetched_history,
            });
        }

        tx.commit().await.map_err(postgres_error)?;
        Ok(claimed)
    }

    async fn claim_workflow_task_inner_filtered(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
        shard_filter: Option<Vec<ShardId>>,
    ) -> Result<Option<ClaimedWorkflowTask>> {
        self.retry_transaction("claim_workflow_task", || {
            let worker_id = worker_id.clone();
            let opts = opts.clone();
            let shard_filter = shard_filter.clone();
            async move {
                self.claim_workflow_task_once_filtered(worker_id, opts, shard_filter)
                    .await
            }
        })
        .await
    }

    async fn claim_workflow_task_once_filtered(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
        shard_filter: Option<Vec<ShardId>>,
    ) -> Result<Option<ClaimedWorkflowTask>> {
        if opts.registered_workflow_types.is_empty() {
            return Ok(None);
        }
        let registered_names = opts
            .registered_workflow_types
            .iter()
            .map(|workflow_type| workflow_type.name.clone())
            .collect::<Vec<_>>();
        let registered_versions = opts
            .registered_workflow_types
            .iter()
            .map(|workflow_type| i32::try_from(workflow_type.version).unwrap_or(i32::MAX))
            .collect::<Vec<_>>();
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let now_ms = unix_epoch_millis();
        let shard_ids = match shard_filter {
            Some(shards) => {
                if shards.is_empty() {
                    tx.commit().await.map_err(postgres_error)?;
                    return Ok(None);
                }
                Some(
                    shards
                        .into_iter()
                        .map(|shard| i32::try_from(shard.0).unwrap_or(i32::MAX))
                        .collect::<Vec<_>>(),
                )
            }
            None => None,
        };
        let row = tx
            .query_opt(
                &format!(
                    "select run_id, workflow_id, workflow_name, workflow_version, current_event_id, ready_reason, shard_id
                     from {schema}.workflow_instances
                     where namespace = $1
                       and task_queue = $2
                       and ready_reason is not null
                       and ready_at_ms <= $3
                       and workflow_claim_token is null
                       and terminal = false
                       and ($6::integer[] is null or shard_id = any($6::integer[]))
                       and (workflow_name, workflow_version) in (
                         select registered.workflow_name, registered.workflow_version
                         from unnest($4::text[], $5::integer[])
                              as registered(workflow_name, workflow_version)
                       )
                     order by ready_at_ms asc, run_id asc
                     limit 1
                     for update skip locked"
                ),
                &[
                    &opts.namespace.0,
                    &opts.task_queue.0,
                    &now_ms,
                    &registered_names,
                    &registered_versions,
                    &shard_ids,
                ],
            )
            .await
            .map_err(postgres_error)?;

        let selected = row
            .map(|row| {
                let workflow_type = WorkflowType::new(
                    row.get::<_, String>(2),
                    u32::try_from(row.get::<_, i32>(3)).unwrap_or(0),
                );
                Ok((
                    RunId::new(row.get::<_, String>(0)),
                    crate::WorkflowId::new(row.get::<_, String>(1)),
                    workflow_type,
                    EventId(row.get::<_, i64>(4).try_into().unwrap_or(u64::MAX)),
                    reason_from_str(&row.get::<_, String>(5))?,
                    row.get::<_, i32>(6),
                ))
            })
            .transpose()?;

        let Some((run_id, workflow_id, workflow_type, tail, reason, selected_shard_id)) = selected
        else {
            tx.commit().await.map_err(postgres_error)?;
            return Ok(None);
        };
        if self.logical_shards > 1 {
            let selected_shard = ShardId(u32::try_from(selected_shard_id).unwrap_or(u32::MAX));
            let owned = self
                .refresh_shard_leases_tx(
                    &tx,
                    &worker_id,
                    &[selected_shard],
                    opts.lease_duration,
                    now_ms,
                )
                .await?;
            if owned.is_empty() {
                tx.commit().await.map_err(postgres_error)?;
                return Ok(None);
            }
        }
        let token = next_claim_token(&tx, &schema).await?;
        tx.execute(
            &format!(
                "update {schema}.workflow_instances
                 set workflow_claim_token = $1, ready_reason = null, ready_at_ms = 0
                 where run_id = $2"
            ),
            &[&i64::try_from(token).unwrap_or(i64::MAX), &run_id.0],
        )
        .await
        .map_err(postgres_error)?;
        let prefetched_history = self
            .prefetch_claim_histories_tx(&tx, &schema, &[(run_id.clone(), tail)])
            .await?
            .remove(&run_id)
            .unwrap_or_default();
        tx.commit().await.map_err(postgres_error)?;
        Ok(Some(ClaimedWorkflowTask {
            run_id: run_id.clone(),
            workflow_id,
            workflow_type,
            claim: WorkflowTaskClaim {
                run_id,
                worker_id,
                token,
            },
            replay_target_event_id: tail,
            reason,
            prefetched_history,
        }))
    }

    async fn stream_history_inner(
        &self,
        req: crate::StreamHistoryRequest,
        hydrate: bool,
    ) -> Result<HistoryChunk> {
        let schema = self.schema_sql();
        let client = self.client().await?;
        let rows = client
            .query(
                &format!(
                    "select event_id, event_type, data
                     from {schema}.history_events
                     where run_id = $1 and event_id > $2 and event_id <= $3
                     order by event_id asc"
                ),
                &[
                    &req.run_id.0,
                    &i64::try_from(req.after_event_id.0).unwrap_or(i64::MAX),
                    &i64::try_from(req.up_to_event_id.0).unwrap_or(i64::MAX),
                ],
            )
            .await
            .map_err(postgres_error)?;
        let max_events = req.max_events.max(1);
        let max_bytes = req.max_bytes.max(1);
        let mut events = Vec::new();
        let mut bytes = 0usize;
        let mut consumed_rows = 0usize;
        for row in &rows {
            let event_id = EventId(row.get::<_, i64>(0).try_into().unwrap_or(u64::MAX));
            let event_type = row.get::<_, String>(1);
            let blob = row.get::<_, Vec<u8>>(2);
            let mut data: HistoryEventData = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            let event_bytes = event_payload_len(&data).max(1);
            if !events.is_empty() && (events.len() >= max_events || bytes + event_bytes > max_bytes)
            {
                break;
            }
            consumed_rows += 1;
            if hydrate {
                data = self.hydrate_history_event_from_storage(data).await?;
            }
            bytes += event_bytes;
            events.push(HistoryEvent {
                event_id,
                event_type: event_type_from_str(&event_type)?,
                data,
            });
            if events.len() >= max_events {
                break;
            }
        }

        let last_event_id = events
            .last()
            .map(|event| event.event_id)
            .unwrap_or(req.after_event_id);
        let has_more = consumed_rows < rows.len();
        Ok(HistoryChunk {
            events,
            last_event_id,
            has_more,
        })
    }

    async fn prefetch_claim_histories_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
        targets: &[(RunId, EventId)],
    ) -> Result<BTreeMap<RunId, Vec<HistoryEvent>>> {
        if targets.is_empty() {
            return Ok(BTreeMap::new());
        }

        let run_ids = targets
            .iter()
            .map(|(run_id, _)| run_id.0.clone())
            .collect::<Vec<_>>();
        let tail_event_ids = targets
            .iter()
            .map(|(_, tail)| i64::try_from(tail.0).unwrap_or(i64::MAX))
            .collect::<Vec<_>>();
        let rows = tx
            .query(
                &format!(
                    "with targets(run_id, tail_event_id) as (
                         select run_id, tail_event_id
                         from unnest($1::text[], $2::bigint[])
                              as targets(run_id, tail_event_id)
                     )
                     select t.run_id, h.event_id, h.event_type, h.data
                     from targets t
                     join lateral (
                         select event_id, event_type, data
                         from {schema}.history_events h
                         where h.run_id = t.run_id
                           and h.event_id <= t.tail_event_id
                         order by h.event_id desc
                         limit $3
                     ) h on true
                     order by t.run_id asc, h.event_id asc"
                ),
                &[
                    &run_ids,
                    &tail_event_ids,
                    &i64::try_from(CLAIM_HISTORY_PREFETCH_EVENTS).unwrap_or(i64::MAX),
                ],
            )
            .await
            .map_err(postgres_error)?;

        let mut by_run = BTreeMap::<RunId, Vec<HistoryEvent>>::new();
        let mut bytes_by_run = BTreeMap::<RunId, usize>::new();
        for row in rows {
            let run_id = RunId::new(row.get::<_, String>(0));
            let event_id = EventId(row.get::<_, i64>(1).try_into().unwrap_or(u64::MAX));
            let event_type = row.get::<_, String>(2);
            let blob = row.get::<_, Vec<u8>>(3);
            let data: HistoryEventData = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            let event_bytes = event_payload_len(&data).max(1);
            let events = by_run.entry(run_id.clone()).or_default();
            let bytes = bytes_by_run.entry(run_id).or_default();
            events.push(HistoryEvent {
                event_id,
                event_type: event_type_from_str(&event_type)?,
                data,
            });
            *bytes = bytes.saturating_add(event_bytes);
        }

        for (run_id, events) in &mut by_run {
            let mut bytes = *bytes_by_run.get(run_id).unwrap_or(&0);
            while events.len() > 1 && bytes > CLAIM_HISTORY_PREFETCH_BYTES {
                let removed = events.remove(0);
                bytes = bytes.saturating_sub(event_payload_len(&removed.data).max(1));
            }
        }

        Ok(by_run)
    }

    async fn release_workflow_task_inner(
        &self,
        claim: WorkflowTaskClaim,
        release: crate::WorkflowTaskRelease,
    ) -> Result<()> {
        self.retry_transaction("release_workflow_task", || {
            let claim = claim.clone();
            let release = release.clone();
            async move { self.release_workflow_task_once(claim, release).await }
        })
        .await
    }

    async fn release_workflow_task_once(
        &self,
        claim: WorkflowTaskClaim,
        release: crate::WorkflowTaskRelease,
    ) -> Result<()> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let Some(row) = tx
            .query_opt(
                &format!(
                    "select workflow_claim_token, terminal
                     from {schema}.workflow_instances
                     where run_id = $1
                     for update"
                ),
                &[&claim.run_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            return Err(Error::RunNotFound(claim.run_id));
        };
        let claim_token: Option<i64> = row.get(0);
        let terminal: bool = row.get(1);
        if claim_token != Some(i64::try_from(claim.token).unwrap_or(i64::MAX)) {
            return Err(Error::StaleLease);
        }
        let ready_reason = (!terminal).then(|| reason_to_str(&release.reason));
        let ready_at_ms = if terminal {
            0
        } else {
            ready_at_ms_for_delay(release.delay)
        };
        tx.execute(
            &format!(
                "update {schema}.workflow_instances
                 set workflow_claim_token = null, ready_reason = $1, ready_at_ms = $2
                 where run_id = $3"
            ),
            &[&ready_reason, &ready_at_ms, &claim.run_id.0],
        )
        .await
        .map_err(postgres_error)?;
        tx.commit().await.map_err(postgres_error)
    }

    async fn commit_workflow_task_inner(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> Result<CommitOutcome> {
        self.retry_transaction("commit_workflow_task", || {
            let claim = claim.clone();
            let batch = batch.clone();
            async move { self.commit_workflow_task_once(claim, batch).await }
        })
        .await
    }

    async fn commit_workflow_task_once(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> Result<CommitOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let applied = self
            .apply_workflow_task_commit_tx(&tx, &schema, claim, batch, None)
            .await?;
        self.append_shard_journal_tx(
            &tx,
            applied.shard_id,
            applied.lease_epoch,
            ShardJournalOperation {
                kind: ShardJournalOperationKind::WorkflowTaskBatch,
                entries: vec![applied.journal_entry],
            },
        )
        .await?;
        let outcome = applied.outcome;
        tx.commit().await.map_err(postgres_error)?;
        Ok(outcome)
    }

    async fn commit_workflow_tasks_inner(
        &self,
        batch: crate::WorkflowTaskCommitBatch,
    ) -> Result<Vec<crate::WorkflowTaskCommitBatchResult>> {
        self.retry_transaction("commit_workflow_tasks", || {
            let batch = batch.clone();
            async move { self.commit_workflow_tasks_once(batch).await }
        })
        .await
    }

    async fn commit_workflow_tasks_once(
        &self,
        batch: crate::WorkflowTaskCommitBatch,
    ) -> Result<Vec<crate::WorkflowTaskCommitBatchResult>> {
        if batch.commits.is_empty() {
            return Ok(Vec::new());
        }

        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let mut results = Vec::with_capacity(batch.commits.len());
        let mut journal_by_shard = BTreeMap::<(i32, i64), Vec<ShardJournalEntry>>::new();
        let mut lease_epoch_cache = BTreeMap::<(WorkerId, i32), i64>::new();

        for input in batch.commits {
            let claim = input.claim;
            match self
                .apply_workflow_task_commit_tx(
                    &tx,
                    &schema,
                    claim.clone(),
                    input.commit,
                    Some(&mut lease_epoch_cache),
                )
                .await
            {
                Ok(applied) => {
                    journal_by_shard
                        .entry((applied.shard_id, applied.lease_epoch))
                        .or_default()
                        .push(applied.journal_entry);
                    results.push(crate::WorkflowTaskCommitBatchResult {
                        claim,
                        result: Ok(applied.outcome),
                    });
                }
                Err(err @ Error::Backend(_)) => return Err(err),
                Err(err) => {
                    results.push(crate::WorkflowTaskCommitBatchResult {
                        claim,
                        result: Err(err),
                    });
                }
            }
        }

        for ((shard_id, lease_epoch), entries) in journal_by_shard {
            self.append_shard_journal_tx(
                &tx,
                shard_id,
                lease_epoch,
                ShardJournalOperation {
                    kind: ShardJournalOperationKind::WorkflowTaskBatch,
                    entries,
                },
            )
            .await?;
        }

        tx.commit().await.map_err(postgres_error)?;
        Ok(results)
    }

    async fn append_shard_journal_tx(
        &self,
        tx: &Transaction<'_>,
        shard_id: i32,
        lease_epoch: i64,
        operation: ShardJournalOperation,
    ) -> Result<u64> {
        let shard_id_u32 = u32::try_from(shard_id).map_err(|_| {
            Error::Backend(format!(
                "invalid postgres shard id {shard_id} for journal append"
            ))
        })?;
        let partition = shard_id_u32 % self.physical_partitions.max(1);
        let suffix = partition_suffix(partition, self.physical_partitions);
        let operation_blob = rmp_serde::to_vec_named(&operation)
            .map_err(|err| Error::PayloadEncode(err.to_string()))?;
        let schema = self.schema_sql();
        let now_ms = unix_epoch_millis();

        let row = tx
            .query_one(
                &format!(
                    "insert into {schema}.shard_heads_{suffix}
                     (shard_id, journal_seq, snapshot_seq, updated_at_ms)
                     values ($1, 1, 0, $2)
                     on conflict(shard_id) do update set
                        journal_seq = shard_heads_{suffix}.journal_seq + 1,
                        updated_at_ms = excluded.updated_at_ms
                     returning journal_seq"
                ),
                &[&shard_id, &now_ms],
            )
            .await
            .map_err(postgres_error)?;
        let next_seq = u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX);
        let next_seq_i64 = i64::try_from(next_seq).unwrap_or(i64::MAX);
        tx.execute(
            &format!(
                "insert into {schema}.shard_journal_{suffix}
                 (shard_id, journal_seq, lease_epoch, operation, appended_at_ms)
                 values ($1, $2, $3, $4, $5)"
            ),
            &[
                &shard_id,
                &next_seq_i64,
                &lease_epoch,
                &operation_blob,
                &now_ms,
            ],
        )
        .await
        .map_err(postgres_error)?;
        Ok(next_seq)
    }

    async fn apply_workflow_task_commit_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
        mut lease_epoch_cache: Option<&mut BTreeMap<(WorkerId, i32), i64>>,
    ) -> Result<AppliedWorkflowTaskCommit> {
        let Some(row) = tx
            .query_opt(
                &format!(
                    "select current_event_id, workflow_claim_token, terminal, namespace, workflow_id,
                            shard_id, workflow_name, workflow_version
                     from {schema}.workflow_instances
                     where run_id = $1
                     for update"
                ),
                &[&claim.run_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            return Err(Error::RunNotFound(claim.run_id));
        };
        let current_tail_i64: i64 = row.get(0);
        let claim_token: Option<i64> = row.get(1);
        let terminal: bool = row.get(2);
        let namespace: String = row.get(3);
        let workflow_id: String = row.get(4);
        let shard_id: i32 = row.get(5);
        let workflow_name: String = row.get(6);
        let workflow_version: i32 = row.get(7);
        if claim_token != Some(i64::try_from(claim.token).unwrap_or(i64::MAX)) {
            return Err(Error::StaleLease);
        }
        let lease_epoch = match lease_epoch_cache.as_deref_mut() {
            Some(cache) => {
                let key = (claim.worker_id.clone(), shard_id);
                if let Some(lease_epoch) = cache.get(&key).copied() {
                    lease_epoch
                } else {
                    let lease_epoch = self
                        .verify_shard_lease_tx(tx, &claim.worker_id, shard_id)
                        .await?;
                    cache.insert(key, lease_epoch);
                    lease_epoch
                }
            }
            None => {
                self.verify_shard_lease_tx(tx, &claim.worker_id, shard_id)
                    .await?
            }
        };
        let current_tail = EventId(u64::try_from(current_tail_i64).unwrap_or(u64::MAX));
        let expected_tail_event_id = batch.expected_tail_event_id;
        if current_tail != expected_tail_event_id {
            tx.execute(
                &format!(
                    "update {schema}.workflow_instances
                     set workflow_claim_token = null, ready_reason = $1, ready_at_ms = 0
                     where run_id = $2"
                ),
                &[
                    &reason_to_str(&WorkflowTaskReason::CacheEvicted),
                    &claim.run_id.0,
                ],
            )
            .await
            .map_err(postgres_error)?;
            return Ok(AppliedWorkflowTaskCommit {
                outcome: CommitOutcome::Conflict,
                shard_id,
                lease_epoch,
                journal_entry: ShardJournalEntry {
                    run_id: claim.run_id,
                    expected_tail_event_id,
                    new_tail_event_id: current_tail,
                    result: ShardJournalCommitResult::Conflict,
                },
            });
        }
        if terminal && (!batch.append_events.is_empty() || !batch.start_child_workflows.is_empty())
        {
            return Err(Error::TerminalWorkflow);
        }

        let mut append_events = Vec::with_capacity(batch.append_events.len());
        for event in batch.append_events {
            append_events.push(crate::NewHistoryEvent::new(
                self.normalize_history_event_for_storage_tx(&tx, event.data)
                    .await?,
            ));
        }
        let mut schedule_activities = Vec::with_capacity(batch.schedule_activities.len());
        for task in batch.schedule_activities {
            schedule_activities.push(
                self.normalize_activity_task_for_storage_tx(&tx, task)
                    .await?,
            );
        }
        let mut schedule_activity_maps = Vec::with_capacity(batch.schedule_activity_maps.len());
        for task in batch.schedule_activity_maps {
            schedule_activity_maps.push(
                self.normalize_activity_map_task_for_storage_tx(&tx, task)
                    .await?,
            );
        }
        let mut start_child_workflows = Vec::with_capacity(batch.start_child_workflows.len());
        for message in batch.start_child_workflows {
            start_child_workflows.push(
                self.normalize_child_start_message_for_storage_tx(&tx, message)
                    .await?,
            );
        }
        let query_projection = match batch.query_projection {
            Some(payload) => Some(self.normalize_payload_for_storage_tx(&tx, payload).await?),
            None => None,
        };

        let mut next_event_id = current_tail;
        let mut became_terminal = false;
        let mut terminal_event = None;
        let mut ready_after_commit = None;
        let mut append_history = Vec::with_capacity(append_events.len());
        for event in append_events {
            next_event_id = next_event_id.next();
            if is_terminal(&event.data) {
                became_terminal = true;
                terminal_event = Some(event.data.clone());
            }
            append_history.push((next_event_id, event.data));
        }
        insert_history_events(&tx, schema, &claim.run_id, &append_history).await?;
        let marker_context = WorkflowChangeMarkerContext {
            namespace: &namespace,
            workflow_id: &workflow_id,
            workflow_name: &workflow_name,
            workflow_version,
        };
        for (event_id, data) in &append_history {
            index_workflow_change_marker_with_context(
                &tx,
                schema,
                &claim.run_id,
                *event_id,
                data,
                &marker_context,
            )
            .await?;
        }

        for message in start_child_workflows {
            if terminal
                || (became_terminal && message.parent_close_policy == ParentClosePolicy::Cancel)
            {
                continue;
            }
            let child_start = if child_event_exists_tx(&tx, &schema, &message.command_id).await? {
                InlineChildStartOutcome::Skipped
            } else {
                let child_shard_id = i32::try_from(
                    self.shard_for_workflow(
                        &Namespace::new(namespace.clone()),
                        &message.workflow_id,
                    )
                    .0,
                )
                .unwrap_or(i32::MAX);
                start_child_workflow_inline_tx(&tx, &schema, &namespace, child_shard_id, &message)
                    .await?
            };
            if terminal || became_terminal {
                continue;
            }
            let (event_data, reason) = match child_start {
                InlineChildStartOutcome::Started(child_run_id) => (
                    HistoryEventData::ChildWorkflowStarted(crate::ChildWorkflowStarted {
                        command_id: message.command_id.clone(),
                        workflow_id: message.workflow_id.clone(),
                        run_id: child_run_id,
                    }),
                    WorkflowTaskReason::ChildWorkflowStarted,
                ),
                InlineChildStartOutcome::Failed(failure) => (
                    HistoryEventData::ChildWorkflowFailed(crate::ChildWorkflowFailed {
                        command_id: message.command_id.clone(),
                        failure,
                    }),
                    WorkflowTaskReason::ChildWorkflowFailed,
                ),
                InlineChildStartOutcome::Skipped => continue,
            };
            next_event_id = next_event_id.next();
            insert_history_event(tx, schema, &claim.run_id, next_event_id, event_data).await?;
            ready_after_commit = Some(reason);
        }

        for task in schedule_activities {
            let task_blob = rmp_serde::to_vec_named(&task)
                .map_err(|err| Error::PayloadEncode(err.to_string()))?;
            tx.execute(
                &format!(
                    "insert into {schema}.activity_tasks
                     (activity_id, namespace, run_id, activity_name, task_queue, task,
                      claim_token, completed, timeout_at_ms, heartbeat_deadline_at_ms)
                     values ($1, $2, $3, $4, $5, $6, null, false, $7, null)"
                ),
                &[
                    &task.activity_id.0,
                    &namespace,
                    &task.run_id.0,
                    &task.activity_name.0,
                    &task.task_queue.0,
                    &task_blob,
                    &activity_timeout_at_ms(task.start_to_close_timeout),
                ],
            )
            .await
            .map_err(postgres_error)?;
        }

        for map_task in schedule_activity_maps {
            insert_activity_map_tx(self, &tx, &schema, &namespace, &map_task).await?;
            materialize_activity_map_items_tx(self, &tx, &schema, &map_task.map_command_id).await?;
        }

        for wait in batch.upsert_waits {
            tx.execute(
                &format!(
                    "insert into {schema}.active_waits
                     (wait_id, namespace, run_id, command_seq, kind, wait_key, ready_at_ms)
                     values ($1, $2, $3, $4, $5, $6, $7)
                     on conflict(wait_id) do update set
                        namespace = excluded.namespace,
                        run_id = excluded.run_id,
                        command_seq = excluded.command_seq,
                        kind = excluded.kind,
                        wait_key = excluded.wait_key,
                        ready_at_ms = excluded.ready_at_ms"
                ),
                &[
                    &wait.wait_id.0,
                    &namespace,
                    &wait.run_id.0,
                    &i64::try_from(wait.command_id.seq.0).unwrap_or(i64::MAX),
                    &wait_kind_to_str(&wait.kind),
                    &wait.key,
                    &wait.ready_at.map(|ready_at| ready_at.0),
                ],
            )
            .await
            .map_err(postgres_error)?;
        }

        for signal_id in batch.consume_signals {
            tx.execute(
                &format!("update {schema}.signals set consumed = true where signal_id = $1"),
                &[&signal_id.0],
            )
            .await
            .map_err(postgres_error)?;
        }

        for wait_id in batch.delete_waits {
            tx.execute(
                &format!("delete from {schema}.active_waits where wait_id = $1"),
                &[&wait_id.0],
            )
            .await
            .map_err(postgres_error)?;
        }

        for command_id in batch.cancel_commands {
            cancel_command_operational_state_tx(&tx, &schema, &command_id).await?;
        }

        if let Some(payload) = query_projection {
            let payload_blob = rmp_serde::to_vec_named(&payload)
                .map_err(|err| Error::PayloadEncode(err.to_string()))?;
            tx.execute(
                &format!(
                    "insert into {schema}.query_projections
                     (namespace, workflow_id, run_id, event_id, payload)
                     values ($1, $2, $3, $4, $5)
                     on conflict(namespace, workflow_id) do update set
                        run_id = excluded.run_id,
                        event_id = excluded.event_id,
                        payload = excluded.payload"
                ),
                &[
                    &namespace,
                    &workflow_id,
                    &claim.run_id.0,
                    &i64::try_from(next_event_id.0).unwrap_or(i64::MAX),
                    &payload_blob,
                ],
            )
            .await
            .map_err(postgres_error)?;
        }

        let terminal_after_commit = terminal || became_terminal;
        if terminal_after_commit {
            cleanup_run_operational_state_tx(&tx, &schema, &claim.run_id).await?;
            if let Some(event @ HistoryEventData::WorkflowContinuedAsNew { .. }) =
                terminal_event.clone()
            {
                continue_run_as_new_tx(&tx, &schema, &claim.run_id, event).await?;
                return Ok(AppliedWorkflowTaskCommit {
                    outcome: CommitOutcome::Committed {
                        new_tail_event_id: next_event_id,
                    },
                    shard_id,
                    lease_epoch,
                    journal_entry: ShardJournalEntry {
                        run_id: claim.run_id,
                        expected_tail_event_id,
                        new_tail_event_id: next_event_id,
                        result: ShardJournalCommitResult::Committed {
                            appended_events: usize::try_from(
                                next_event_id.0.saturating_sub(current_tail.0),
                            )
                            .unwrap_or(usize::MAX),
                            terminal: terminal_after_commit,
                            ready_reason: None,
                        },
                    },
                });
            }
            if let Some(event) = terminal_event {
                handle_terminal_run_tx(&tx, &schema, &claim.run_id, &event).await?;
            }
        }
        let ready_reason = if terminal_after_commit {
            None
        } else {
            ready_after_commit.as_ref().map(reason_to_str)
        };
        tx.execute(
            &format!(
                "update {schema}.workflow_instances
                 set current_event_id = $1,
                     workflow_claim_token = null,
                     terminal = $2,
                     ready_reason = $3,
                     ready_at_ms = 0
                 where run_id = $4"
            ),
            &[
                &i64::try_from(next_event_id.0).unwrap_or(i64::MAX),
                &terminal_after_commit,
                &ready_reason,
                &claim.run_id.0,
            ],
        )
        .await
        .map_err(postgres_error)?;
        Ok(AppliedWorkflowTaskCommit {
            outcome: CommitOutcome::Committed {
                new_tail_event_id: next_event_id,
            },
            shard_id,
            lease_epoch,
            journal_entry: ShardJournalEntry {
                run_id: claim.run_id,
                expected_tail_event_id,
                new_tail_event_id: next_event_id,
                result: ShardJournalCommitResult::Committed {
                    appended_events: usize::try_from(
                        next_event_id.0.saturating_sub(current_tail.0),
                    )
                    .unwrap_or(usize::MAX),
                    terminal: terminal_after_commit,
                    ready_reason: ready_reason.map(str::to_owned),
                },
            },
        })
    }

    async fn signal_workflow_inner(
        &self,
        req: SignalWorkflowRequest,
    ) -> Result<SignalWorkflowOutcome> {
        self.retry_transaction("signal_workflow", || {
            let req = req.clone();
            async move { self.signal_workflow_once(req).await }
        })
        .await
    }

    async fn signal_workflow_once(
        &self,
        req: SignalWorkflowRequest,
    ) -> Result<SignalWorkflowOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        if tx
            .query_opt(
                &format!("select 1 from {schema}.signals where signal_id = $1 limit 1"),
                &[&req.signal_id.0],
            )
            .await
            .map_err(postgres_error)?
            .is_some()
        {
            tx.commit().await.map_err(postgres_error)?;
            return Ok(SignalWorkflowOutcome::Duplicate);
        }

        let Some(row) = tx
            .query_opt(
                &format!(
                    "select run_id, terminal
                     from {schema}.workflow_instances
                     where namespace = $1 and workflow_id = $2
                     for update"
                ),
                &[&req.namespace.0, &req.workflow_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            return Err(Error::Backend(format!(
                "workflow `{}` was not found",
                req.workflow_id.0
            )));
        };
        let run_id = RunId::new(row.get::<_, String>(0));
        let terminal: bool = row.get(1);
        if terminal {
            return Err(Error::TerminalWorkflow);
        }

        let received_sequence = next_signal_sequence(&tx, &schema).await?;
        let payload_ref = self
            .normalize_payload_for_storage_tx(&tx, req.payload)
            .await?;
        let payload = rmp_serde::to_vec_named(&payload_ref)
            .map_err(|err| Error::PayloadEncode(err.to_string()))?;
        tx.execute(
            &format!(
                "insert into {schema}.signals
                 (signal_id, namespace, run_id, signal_name, payload, received_sequence, consumed)
                 values ($1, $2, $3, $4, $5, $6, false)"
            ),
            &[
                &req.signal_id.0,
                &req.namespace.0,
                &run_id.0,
                &req.signal_name.0,
                &payload,
                &i64::try_from(received_sequence).unwrap_or(i64::MAX),
            ],
        )
        .await
        .map_err(postgres_error)?;

        if signal_wait_ready(&tx, &schema, &run_id).await? {
            tx.execute(
                &format!(
                    "update {schema}.workflow_instances
                     set ready_reason = $1, ready_at_ms = 0
                     where run_id = $2 and terminal = false"
                ),
                &[
                    &reason_to_str(&WorkflowTaskReason::SignalReceived),
                    &run_id.0,
                ],
            )
            .await
            .map_err(postgres_error)?;
        }

        tx.commit().await.map_err(postgres_error)?;
        Ok(SignalWorkflowOutcome::Accepted)
    }

    async fn read_signal_inbox_inner(
        &self,
        req: ReadSignalInboxRequest,
    ) -> Result<Option<SignalInboxRecord>> {
        let schema = self.schema_sql();
        let row = {
            let client = self.client().await?;
            client
                .query_opt(
                    &format!(
                        "select signal_id, signal_name, payload
                         from {schema}.signals
                         where run_id = $1 and signal_name = $2 and consumed = false
                         order by received_sequence asc
                         limit 1"
                    ),
                    &[&req.run_id.0, &req.signal_name.0],
                )
                .await
                .map_err(postgres_error)?
        };
        let Some((signal_id, signal_name, payload)) = row
            .map(|row| {
                let payload: Vec<u8> = row.get(2);
                let payload: PayloadRef = rmp_serde::from_slice(&payload)
                    .map_err(|err| Error::PayloadDecode(err.to_string()))?;
                Ok((row.get::<_, String>(0), row.get::<_, String>(1), payload))
            })
            .transpose()?
        else {
            return Ok(None);
        };
        let payload = self.hydrate_payload_from_storage(payload).await?;
        Ok(Some(SignalInboxRecord {
            signal_id: crate::SignalId::new(signal_id),
            signal_name: crate::SignalName::new(signal_name),
            payload,
        }))
    }

    async fn fire_due_timers_inner(
        &self,
        req: FireDueTimersRequest,
    ) -> Result<FireDueTimersOutcome> {
        self.retry_transaction("fire_due_timers", || {
            let req = req.clone();
            async move { self.fire_due_timers_once(req).await }
        })
        .await
    }

    async fn fire_due_timers_once(
        &self,
        req: FireDueTimersRequest,
    ) -> Result<FireDueTimersOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let fired = fire_due_timers_tx(&tx, &schema, req).await?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(FireDueTimersOutcome { fired })
    }

    async fn timeout_due_activities_inner(
        &self,
        req: TimeoutDueActivitiesRequest,
    ) -> Result<TimeoutDueActivitiesOutcome> {
        self.retry_transaction("timeout_due_activities", || {
            let req = req.clone();
            async move { self.timeout_due_activities_once(req).await }
        })
        .await
    }

    async fn timeout_due_activities_once(
        &self,
        req: TimeoutDueActivitiesRequest,
    ) -> Result<TimeoutDueActivitiesOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let timed_out = timeout_due_activities_tx(&tx, &schema, req).await?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(TimeoutDueActivitiesOutcome { timed_out })
    }

    async fn run_due_maintenance_inner(
        &self,
        req: RunDueMaintenanceRequest,
    ) -> Result<RunDueMaintenanceOutcome> {
        self.retry_transaction("run_due_maintenance", || {
            let req = req.clone();
            async move { self.run_due_maintenance_once(req).await }
        })
        .await
    }

    async fn run_due_maintenance_once(
        &self,
        req: RunDueMaintenanceRequest,
    ) -> Result<RunDueMaintenanceOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let now = req.now;
        let timers_fired = fire_due_timers_tx(
            &tx,
            &schema,
            FireDueTimersRequest {
                namespace: req.namespace.clone(),
                now,
                limit: req.timer_limit,
            },
        )
        .await?;
        let activities_timed_out = timeout_due_activities_tx(
            &tx,
            &schema,
            TimeoutDueActivitiesRequest {
                namespace: req.namespace,
                now,
                limit: req.activity_limit,
            },
        )
        .await?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(RunDueMaintenanceOutcome {
            timers_fired,
            activities_timed_out,
        })
    }

    async fn claim_activity_task_inner(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> Result<Option<ClaimedActivityTask>> {
        self.retry_transaction("claim_activity_task", || {
            let worker_id = worker_id.clone();
            let opts = opts.clone();
            async move { self.claim_activity_task_once(worker_id, opts).await }
        })
        .await
    }

    async fn claim_activity_task_once(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> Result<Option<ClaimedActivityTask>> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let now = unix_epoch_millis();
        let registered_activity_names = opts
            .registered_activity_names
            .iter()
            .map(|name| name.0.clone())
            .collect::<Vec<_>>();
        let row = tx
            .query_opt(
                &format!(
                    "select activity_id, task
                     from {schema}.activity_tasks
                     where namespace = $1
                       and task_queue = $2
                       and activity_name = any($3::text[])
                       and completed = false
                       and claim_token is null
                       and (timeout_at_ms is null or timeout_at_ms > $4)
                     order by activity_id asc
                     limit 1
                     for update skip locked"
                ),
                &[
                    &opts.namespace.0,
                    &opts.task_queue.0,
                    &registered_activity_names,
                    &now,
                ],
            )
            .await
            .map_err(postgres_error)?;
        let Some(row) = row else {
            tx.commit().await.map_err(postgres_error)?;
            return Ok(None);
        };
        let activity_id = ActivityId(row.get::<_, String>(0));
        let task_blob: Vec<u8> = row.get(1);
        let task: ActivityTask = rmp_serde::from_slice(&task_blob)
            .map_err(|err| Error::PayloadDecode(err.to_string()))?;
        let task = self
            .hydrate_activity_task_from_storage_tx(&tx, task)
            .await?;
        let token = next_claim_token(&tx, &schema).await?;
        tx.execute(
            &format!(
                "update {schema}.activity_tasks
                 set claim_token = $1, heartbeat_deadline_at_ms = $2
                 where activity_id = $3"
            ),
            &[
                &i64::try_from(token).unwrap_or(i64::MAX),
                &activity_timeout_at_ms(task.heartbeat_timeout),
                &activity_id.0,
            ],
        )
        .await
        .map_err(postgres_error)?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(Some(ClaimedActivityTask {
            task,
            claim: ActivityTaskClaim {
                activity_id,
                worker_id,
                token,
            },
        }))
    }

    async fn claim_activity_tasks_inner(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityTasksOptions,
    ) -> Result<Vec<ClaimedActivityTask>> {
        self.retry_transaction("claim_activity_tasks", || {
            let worker_id = worker_id.clone();
            let opts = opts.clone();
            async move { self.claim_activity_tasks_once(worker_id, opts).await }
        })
        .await
    }

    async fn claim_activity_tasks_once(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityTasksOptions,
    ) -> Result<Vec<ClaimedActivityTask>> {
        if opts.limit == 0 || opts.claim.registered_activity_names.is_empty() {
            return Ok(Vec::new());
        }

        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let now = unix_epoch_millis();
        let registered_activity_names = opts
            .claim
            .registered_activity_names
            .iter()
            .map(|name| name.0.clone())
            .collect::<Vec<_>>();
        let rows = tx
            .query(
                &format!(
                    "select activity_id, task
                     from {schema}.activity_tasks
                     where namespace = $1
                       and task_queue = $2
                       and activity_name = any($3::text[])
                       and completed = false
                       and claim_token is null
                       and (timeout_at_ms is null or timeout_at_ms > $4)
                     order by activity_id asc
                     limit $5
                     for update skip locked"
                ),
                &[
                    &opts.claim.namespace.0,
                    &opts.claim.task_queue.0,
                    &registered_activity_names,
                    &now,
                    &i64::try_from(opts.limit).unwrap_or(i64::MAX),
                ],
            )
            .await
            .map_err(postgres_error)?;

        if rows.is_empty() {
            tx.commit().await.map_err(postgres_error)?;
            return Ok(Vec::new());
        }

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let activity_id = ActivityId(row.get::<_, String>(0));
            let task_blob: Vec<u8> = row.get(1);
            let task: ActivityTask = rmp_serde::from_slice(&task_blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            let task = self
                .hydrate_activity_task_from_storage_tx(&tx, task)
                .await?;
            tasks.push((activity_id, task));
        }

        let token_rows = tx
            .query(
                &format!(
                    "select nextval('{schema}.claim_token_seq'::regclass)
                     from generate_series(1::bigint, $1::bigint)"
                ),
                &[&i64::try_from(tasks.len()).unwrap_or(i64::MAX)],
            )
            .await
            .map_err(postgres_error)?;
        let tokens = token_rows
            .into_iter()
            .map(|row| {
                let token: i64 = row.get(0);
                u64::try_from(token).map_err(|_| {
                    Error::Backend(format!(
                        "postgres claim token sequence returned invalid value {token}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let activity_ids = tasks
            .iter()
            .map(|(activity_id, _)| activity_id.0.clone())
            .collect::<Vec<_>>();
        let token_values = tokens
            .iter()
            .map(|token| i64::try_from(*token).unwrap_or(i64::MAX))
            .collect::<Vec<_>>();
        let heartbeat_deadlines = tasks
            .iter()
            .map(|(_, task)| activity_timeout_at_ms(task.heartbeat_timeout).unwrap_or(-1))
            .collect::<Vec<_>>();
        tx.execute(
            &format!(
                "update {schema}.activity_tasks tasks
                 set claim_token = claimed.claim_token,
                     heartbeat_deadline_at_ms = nullif(claimed.heartbeat_deadline_at_ms, -1)
                 from unnest($1::text[], $2::bigint[], $3::bigint[])
                      as claimed(activity_id, claim_token, heartbeat_deadline_at_ms)
                 where tasks.activity_id = claimed.activity_id"
            ),
            &[&activity_ids, &token_values, &heartbeat_deadlines],
        )
        .await
        .map_err(postgres_error)?;
        tx.commit().await.map_err(postgres_error)?;

        Ok(tasks
            .into_iter()
            .zip(tokens.into_iter())
            .map(|((activity_id, task), token)| ClaimedActivityTask {
                task,
                claim: ActivityTaskClaim {
                    activity_id,
                    worker_id: worker_id.clone(),
                    token,
                },
            })
            .collect())
    }

    async fn heartbeat_activity_inner(
        &self,
        req: ActivityHeartbeatRequest,
    ) -> Result<ActivityHeartbeatOutcome> {
        self.retry_transaction("heartbeat_activity", || {
            let req = req.clone();
            async move { self.heartbeat_activity_once(req).await }
        })
        .await
    }

    async fn heartbeat_activity_once(
        &self,
        req: ActivityHeartbeatRequest,
    ) -> Result<ActivityHeartbeatOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let Some(row) = tx
            .query_opt(
                &format!(
                    "select task, claim_token, completed
                     from {schema}.activity_tasks
                     where activity_id = $1
                     for update"
                ),
                &[&req.claim.activity_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            return Err(Error::Backend(format!(
                "activity `{}` not found",
                req.claim.activity_id.0
            )));
        };
        let task_blob: Vec<u8> = row.get(0);
        let claim_token: Option<i64> = row.get(1);
        let completed: bool = row.get(2);
        if completed {
            tx.commit().await.map_err(postgres_error)?;
            return Ok(ActivityHeartbeatOutcome::AlreadyCompleted);
        }
        if claim_token != Some(i64::try_from(req.claim.token).unwrap_or(i64::MAX)) {
            return Err(Error::StaleLease);
        }

        let task: ActivityTask = rmp_serde::from_slice(&task_blob)
            .map_err(|err| Error::PayloadDecode(err.to_string()))?;
        tx.execute(
            &format!(
                "update {schema}.activity_tasks
                 set heartbeat_deadline_at_ms = $1
                 where activity_id = $2"
            ),
            &[
                &activity_timeout_at_ms(task.heartbeat_timeout),
                &req.claim.activity_id.0,
            ],
        )
        .await
        .map_err(postgres_error)?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(ActivityHeartbeatOutcome::Recorded)
    }

    async fn complete_activity_inner(
        &self,
        req: CompleteActivityRequest,
    ) -> Result<CompleteActivityOutcome> {
        self.retry_transaction("complete_activity", || {
            let req = req.clone();
            async move { self.complete_activity_once(req).await }
        })
        .await
    }

    async fn complete_activity_once(
        &self,
        req: CompleteActivityRequest,
    ) -> Result<CompleteActivityOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let outcome = self.complete_activity_tx(&tx, &schema, req).await?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(outcome)
    }

    async fn complete_activity_tasks_inner(
        &self,
        req: CompleteActivityTasksRequest,
    ) -> Result<Vec<CompleteActivityTaskBatchResult>> {
        self.retry_transaction("complete_activity_tasks", || {
            let req = req.clone();
            async move { self.complete_activity_tasks_once(req).await }
        })
        .await
    }

    async fn complete_activity_tasks_once(
        &self,
        req: CompleteActivityTasksRequest,
    ) -> Result<Vec<CompleteActivityTaskBatchResult>> {
        if req.completions.is_empty() {
            return Ok(Vec::new());
        }

        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();

        if has_duplicate_activity_completion_ids(&req.completions) {
            let results = self
                .complete_activity_tasks_scalar_tx(&tx, &schema, req.completions)
                .await?;
            tx.commit().await.map_err(postgres_error)?;
            return Ok(results);
        }

        let activity_ids = req
            .completions
            .iter()
            .map(|completion| completion.claim.activity_id.0.clone())
            .collect::<Vec<_>>();
        let rows = tx
            .query(
                &format!(
                    "select activity_id, task, claim_token, completed
                     from {schema}.activity_tasks
                     where activity_id = any($1::text[])
                     order by activity_id asc
                     for update"
                ),
                &[&activity_ids],
            )
            .await
            .map_err(postgres_error)?;
        let mut locked = BTreeMap::<String, LockedActivityCompletion>::new();
        for row in rows {
            let activity_id: String = row.get(0);
            let task_blob: Vec<u8> = row.get(1);
            let task: ActivityTask = rmp_serde::from_slice(&task_blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            locked.insert(
                activity_id,
                LockedActivityCompletion {
                    task,
                    claim_token: row.get(2),
                    completed: row.get(3),
                },
            );
        }

        let mut result_slots = std::iter::repeat_with(|| None)
            .take(req.completions.len())
            .collect::<Vec<Option<Result<CompleteActivityOutcome>>>>();
        let mut pending_indices = Vec::new();
        let mut has_valid_map_item = false;

        for (index, completion) in req.completions.iter().enumerate() {
            let Some(row) = locked.get(&completion.claim.activity_id.0) else {
                result_slots[index] = Some(Err(Error::Backend(format!(
                    "activity `{}` not found",
                    completion.claim.activity_id.0
                ))));
                continue;
            };
            if row.completed {
                result_slots[index] = Some(Ok(CompleteActivityOutcome::AlreadyCompleted));
                continue;
            }
            if row.claim_token != Some(i64::try_from(completion.claim.token).unwrap_or(i64::MAX)) {
                result_slots[index] = Some(Err(Error::StaleLease));
                continue;
            }
            if row.task.map_item.is_some() {
                has_valid_map_item = true;
                break;
            }
            pending_indices.push(index);
        }

        if has_valid_map_item {
            let results = self
                .complete_activity_tasks_scalar_tx(&tx, &schema, req.completions)
                .await?;
            tx.commit().await.map_err(postgres_error)?;
            return Ok(results);
        }

        if !pending_indices.is_empty() {
            let run_ids = pending_indices
                .iter()
                .filter_map(|index| {
                    locked
                        .get(&req.completions[*index].claim.activity_id.0)
                        .map(|row| row.task.run_id.0.clone())
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let rows = tx
                .query(
                    &format!(
                        "select run_id, current_event_id, terminal
                         from {schema}.workflow_instances
                         where run_id = any($1::text[])
                         order by run_id asc
                         for update"
                    ),
                    &[&run_ids],
                )
                .await
                .map_err(postgres_error)?;
            let mut workflows = BTreeMap::<String, WorkflowCompletionState>::new();
            for row in rows {
                workflows.insert(
                    row.get(0),
                    WorkflowCompletionState {
                        current_event_id: EventId(
                            u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX),
                        ),
                        terminal: row.get(2),
                    },
                );
            }

            let mut candidates = Vec::new();
            for index in pending_indices {
                let completion = &req.completions[index];
                let row = locked
                    .get(&completion.claim.activity_id.0)
                    .expect("pending activity row should be locked");
                let Some(workflow) = workflows.get(&row.task.run_id.0) else {
                    result_slots[index] = Some(Err(Error::RunNotFound(row.task.run_id.clone())));
                    continue;
                };
                if workflow.terminal {
                    result_slots[index] = Some(Err(Error::TerminalWorkflow));
                    continue;
                }
                candidates.push(NormalActivityCompletionCandidate {
                    input_index: index,
                    activity_id: completion.claim.activity_id.clone(),
                    run_id: row.task.run_id.clone(),
                    command_id: row.task.command_id.clone(),
                    result: completion.result.clone(),
                });
            }

            if !candidates.is_empty() {
                let mut next_event_ids = workflows
                    .iter()
                    .map(|(run_id, state)| (run_id.clone(), state.current_event_id))
                    .collect::<BTreeMap<_, _>>();
                let mut updated_tails = BTreeMap::<String, EventId>::new();
                let mut history_events = Vec::with_capacity(candidates.len());
                let mut completed_activity_ids = Vec::with_capacity(candidates.len());

                for candidate in candidates {
                    let tail = next_event_ids
                        .get_mut(&candidate.run_id.0)
                        .expect("candidate run should have workflow state");
                    let event_id = tail.next();
                    *tail = event_id;
                    updated_tails.insert(candidate.run_id.0.clone(), event_id);

                    let result = self
                        .normalize_payload_for_storage_tx(&tx, candidate.result)
                        .await?;
                    history_events.push((
                        candidate.run_id.clone(),
                        event_id,
                        HistoryEventData::ActivityCompleted(crate::ActivityCompleted {
                            command_id: candidate.command_id,
                            result,
                        }),
                    ));
                    completed_activity_ids.push(candidate.activity_id.0);
                    result_slots[candidate.input_index] =
                        Some(Ok(CompleteActivityOutcome::Completed { event_id }));
                }

                let history_inserts = history_events
                    .iter()
                    .map(|(run_id, event_id, data)| HistoryEventInsert {
                        run_id,
                        event_id: *event_id,
                        data,
                    })
                    .collect::<Vec<_>>();
                insert_history_event_rows(&tx, &schema, &history_inserts).await?;

                let update_run_ids = updated_tails.keys().cloned().collect::<Vec<_>>();
                let update_event_ids = updated_tails
                    .values()
                    .map(|event_id| i64::try_from(event_id.0).unwrap_or(i64::MAX))
                    .collect::<Vec<_>>();
                tx.execute(
                    &format!(
                        "with updates(run_id, event_id) as (
                             select run_id, event_id
                             from unnest($1::text[], $2::bigint[]) as updates(run_id, event_id)
                         )
                         update {schema}.workflow_instances as workflows
                         set current_event_id = updates.event_id,
                             ready_reason = $3,
                             ready_at_ms = 0
                         from updates
                         where workflows.run_id = updates.run_id"
                    ),
                    &[
                        &update_run_ids,
                        &update_event_ids,
                        &reason_to_str(&WorkflowTaskReason::ActivityCompleted),
                    ],
                )
                .await
                .map_err(postgres_error)?;

                tx.execute(
                    &format!(
                        "update {schema}.activity_tasks
                         set completed = true,
                             heartbeat_deadline_at_ms = null
                         where activity_id = any($1::text[])"
                    ),
                    &[&completed_activity_ids],
                )
                .await
                .map_err(postgres_error)?;
            }
        }

        let results = req
            .completions
            .into_iter()
            .enumerate()
            .map(|(index, completion)| CompleteActivityTaskBatchResult {
                claim: completion.claim,
                result: result_slots[index]
                    .take()
                    .expect("batch activity completion should fill every result slot"),
            })
            .collect();
        tx.commit().await.map_err(postgres_error)?;
        Ok(results)
    }

    async fn complete_activity_tasks_scalar_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
        completions: Vec<CompleteActivityRequest>,
    ) -> Result<Vec<CompleteActivityTaskBatchResult>> {
        let mut results = Vec::with_capacity(completions.len());
        for completion in completions {
            tx.batch_execute("savepoint durust_complete_activity_item")
                .await
                .map_err(postgres_error)?;
            let claim = completion.claim.clone();
            match self.complete_activity_tx(tx, schema, completion).await {
                Ok(outcome) => {
                    tx.batch_execute("release savepoint durust_complete_activity_item")
                        .await
                        .map_err(postgres_error)?;
                    results.push(CompleteActivityTaskBatchResult {
                        claim,
                        result: Ok(outcome),
                    });
                }
                Err(err) if is_activity_completion_item_error(&err) => {
                    tx.batch_execute("rollback to savepoint durust_complete_activity_item")
                        .await
                        .map_err(postgres_error)?;
                    tx.batch_execute("release savepoint durust_complete_activity_item")
                        .await
                        .map_err(postgres_error)?;
                    results.push(CompleteActivityTaskBatchResult {
                        claim,
                        result: Err(err),
                    });
                }
                Err(err) => {
                    let _ = tx
                        .batch_execute("rollback to savepoint durust_complete_activity_item")
                        .await;
                    let _ = tx
                        .batch_execute("release savepoint durust_complete_activity_item")
                        .await;
                    return Err(err);
                }
            }
        }
        Ok(results)
    }

    async fn complete_activity_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
        req: CompleteActivityRequest,
    ) -> Result<CompleteActivityOutcome> {
        let Some(row) = tx
            .query_opt(
                &format!(
                    "select task, claim_token, completed
                     from {schema}.activity_tasks
                     where activity_id = $1
                     for update"
                ),
                &[&req.claim.activity_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            return Err(Error::Backend(format!(
                "activity `{}` not found",
                req.claim.activity_id.0
            )));
        };
        let task_blob: Vec<u8> = row.get(0);
        let claim_token: Option<i64> = row.get(1);
        let completed: bool = row.get(2);
        if completed {
            return Ok(CompleteActivityOutcome::AlreadyCompleted);
        }
        if claim_token != Some(i64::try_from(req.claim.token).unwrap_or(i64::MAX)) {
            return Err(Error::StaleLease);
        }
        let task: ActivityTask = rmp_serde::from_slice(&task_blob)
            .map_err(|err| Error::PayloadDecode(err.to_string()))?;
        if let Some(map_item) = task.map_item.clone() {
            let result = self
                .normalize_payload_for_storage_tx(&tx, req.result)
                .await?;
            let outcome = complete_map_item_tx(
                self,
                &tx,
                &schema,
                task,
                map_item,
                result,
                &req.claim.activity_id,
            )
            .await?;
            return Ok(outcome);
        }
        let result = self
            .normalize_payload_for_storage_tx(&tx, req.result)
            .await?;
        let Some(run_row) = tx
            .query_opt(
                &format!(
                    "select current_event_id, terminal
                     from {schema}.workflow_instances
                     where run_id = $1
                     for update"
                ),
                &[&task.run_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            return Err(Error::RunNotFound(task.run_id));
        };
        let tail = EventId(u64::try_from(run_row.get::<_, i64>(0)).unwrap_or(u64::MAX));
        let terminal: bool = run_row.get(1);
        if terminal {
            return Err(Error::TerminalWorkflow);
        }
        let event_id = tail.next();
        insert_history_event(
            &tx,
            &schema,
            &task.run_id,
            event_id,
            HistoryEventData::ActivityCompleted(crate::ActivityCompleted {
                command_id: task.command_id,
                result,
            }),
        )
        .await?;
        tx.execute(
            &format!(
                "update {schema}.workflow_instances
                 set current_event_id = $1, ready_reason = $2, ready_at_ms = 0
                 where run_id = $3"
            ),
            &[
                &i64::try_from(event_id.0).unwrap_or(i64::MAX),
                &reason_to_str(&WorkflowTaskReason::ActivityCompleted),
                &task.run_id.0,
            ],
        )
        .await
        .map_err(postgres_error)?;
        tx.execute(
            &format!(
                "update {schema}.activity_tasks
                 set completed = true,
                     heartbeat_deadline_at_ms = null
                 where activity_id = $1"
            ),
            &[&req.claim.activity_id.0],
        )
        .await
        .map_err(postgres_error)?;
        Ok(CompleteActivityOutcome::Completed { event_id })
    }

    async fn fail_activity_inner(&self, req: FailActivityRequest) -> Result<FailActivityOutcome> {
        self.retry_transaction("fail_activity", || {
            let req = req.clone();
            async move { self.fail_activity_once(req).await }
        })
        .await
    }

    async fn fail_activity_once(&self, req: FailActivityRequest) -> Result<FailActivityOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let Some(row) = tx
            .query_opt(
                &format!(
                    "select task, claim_token, completed
                     from {schema}.activity_tasks
                     where activity_id = $1
                     for update"
                ),
                &[&req.claim.activity_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            return Err(Error::Backend(format!(
                "activity `{}` not found",
                req.claim.activity_id.0
            )));
        };
        let task_blob: Vec<u8> = row.get(0);
        let claim_token: Option<i64> = row.get(1);
        let completed: bool = row.get(2);
        if completed {
            tx.commit().await.map_err(postgres_error)?;
            return Ok(FailActivityOutcome::AlreadyCompleted);
        }
        if claim_token != Some(i64::try_from(req.claim.token).unwrap_or(i64::MAX)) {
            return Err(Error::StaleLease);
        }
        let task: ActivityTask = rmp_serde::from_slice(&task_blob)
            .map_err(|err| Error::PayloadDecode(err.to_string()))?;
        if should_retry_activity(&task) && !req.failure.non_retryable {
            let mut retry_task = task.clone();
            retry_task.attempt = retry_task.attempt.saturating_add(1);
            let retry_blob = rmp_serde::to_vec_named(&retry_task)
                .map_err(|err| Error::PayloadEncode(err.to_string()))?;
            tx.execute(
                &format!(
                    "update {schema}.activity_tasks
                     set task = $1,
                         claim_token = null,
                         timeout_at_ms = $2,
                         heartbeat_deadline_at_ms = null
                     where activity_id = $3"
                ),
                &[
                    &retry_blob,
                    &activity_timeout_at_ms(retry_task.start_to_close_timeout),
                    &req.claim.activity_id.0,
                ],
            )
            .await
            .map_err(postgres_error)?;
            tx.commit().await.map_err(postgres_error)?;
            return Ok(FailActivityOutcome::RetryScheduled {
                next_attempt: retry_task.attempt,
            });
        }

        let failure = self
            .normalize_failure_for_storage_tx(&tx, req.failure)
            .await?;
        if let Some(map_item) = task.map_item.clone() {
            let outcome = fail_map_item_tx(
                &tx,
                &schema,
                task,
                map_item,
                failure,
                &req.claim.activity_id,
            )
            .await?;
            tx.commit().await.map_err(postgres_error)?;
            return Ok(outcome);
        }
        let Some(run_row) = tx
            .query_opt(
                &format!(
                    "select current_event_id, terminal
                     from {schema}.workflow_instances
                     where run_id = $1
                     for update"
                ),
                &[&task.run_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            return Err(Error::RunNotFound(task.run_id));
        };
        let tail = EventId(u64::try_from(run_row.get::<_, i64>(0)).unwrap_or(u64::MAX));
        let terminal: bool = run_row.get(1);
        if terminal {
            return Err(Error::TerminalWorkflow);
        }
        let event_id = tail.next();
        insert_history_event(
            &tx,
            &schema,
            &task.run_id,
            event_id,
            HistoryEventData::ActivityFailed(ActivityFailed {
                command_id: task.command_id,
                failure,
            }),
        )
        .await?;
        tx.execute(
            &format!(
                "update {schema}.workflow_instances
                 set current_event_id = $1, ready_reason = $2, ready_at_ms = 0
                 where run_id = $3"
            ),
            &[
                &i64::try_from(event_id.0).unwrap_or(i64::MAX),
                &reason_to_str(&WorkflowTaskReason::ActivityFailed),
                &task.run_id.0,
            ],
        )
        .await
        .map_err(postgres_error)?;
        tx.execute(
            &format!(
                "update {schema}.activity_tasks
                 set completed = true,
                     heartbeat_deadline_at_ms = null
                 where activity_id = $1"
            ),
            &[&req.claim.activity_id.0],
        )
        .await
        .map_err(postgres_error)?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(FailActivityOutcome::Failed { event_id })
    }

    async fn query_projection_inner(
        &self,
        req: QueryProjectionRequest,
    ) -> Result<QueryProjectionOutcome> {
        let schema = self.schema_sql();
        let row = {
            let client = self.client().await?;
            client
                .query_opt(
                    &format!(
                        "select run_id, event_id, payload
                         from {schema}.query_projections
                         where namespace = $1 and workflow_id = $2"
                    ),
                    &[&req.namespace.0, &req.workflow_id.0],
                )
                .await
                .map_err(postgres_error)?
        };
        let Some(row) = row else {
            return Ok(QueryProjectionOutcome::NotFound);
        };
        let payload_blob: Vec<u8> = row.get(2);
        let payload: PayloadRef = rmp_serde::from_slice(&payload_blob)
            .map_err(|err| Error::PayloadDecode(err.to_string()))?;
        let payload = self.hydrate_payload_from_storage(payload).await?;
        Ok(QueryProjectionOutcome::Found {
            run_id: RunId::new(row.get::<_, String>(0)),
            event_id: EventId(u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX)),
            payload,
        })
    }

    async fn workflow_change_versions_inner(
        &self,
        req: WorkflowChangeVersionsRequest,
    ) -> Result<WorkflowChangeVersionsOutcome> {
        let schema = self.schema_sql();
        let workflow_id = req.workflow_id.map(|workflow_id| workflow_id.0);
        let run_id = req.run_id.map(|run_id| run_id.0);
        let client = self.client().await?;
        let rows = client
            .query(
                &format!(
                    "select c.namespace,
                            c.workflow_id,
                            c.workflow_name,
                            c.workflow_version,
                            c.run_id,
                            c.change_id,
                            c.version,
                            c.marker_kind,
                            c.command_seq,
                            c.first_event_id,
                            c.last_seen_at_ms,
                            i.terminal
                     from {schema}.workflow_change_versions c
                     join {schema}.workflow_instances i on i.run_id = c.run_id
                     where c.namespace = $1
                       and ($2::text is null or c.workflow_id = $2)
                       and ($3::text is null or c.run_id = $3)
                       and ($4::text is null or c.change_id = $4)
                     order by c.workflow_id asc, c.run_id asc, c.change_id asc"
                ),
                &[&req.namespace.0, &workflow_id, &run_id, &req.change_id],
            )
            .await
            .map_err(postgres_error)?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let terminal: bool = row.get(11);
            records.push(WorkflowChangeVersionRecord {
                namespace: crate::Namespace::new(row.get::<_, String>(0)),
                workflow_id: crate::WorkflowId::new(row.get::<_, String>(1)),
                workflow_type: WorkflowType::new(
                    row.get::<_, String>(2),
                    u32::try_from(row.get::<_, i32>(3)).unwrap_or(0),
                ),
                run_id: RunId::new(row.get::<_, String>(4)),
                change_id: row.get(5),
                version: row.get(6),
                marker_kind: marker_kind_from_str(&row.get::<_, String>(7))?,
                command_seq: CommandSeq(u64::try_from(row.get::<_, i64>(8)).unwrap_or(u64::MAX)),
                first_event_id: EventId(u64::try_from(row.get::<_, i64>(9)).unwrap_or(u64::MAX)),
                last_seen_at: TimestampMs(row.get(10)),
                status: if terminal {
                    WorkflowChangeVersionStatus::Closed
                } else {
                    WorkflowChangeVersionStatus::Open
                },
            });
        }
        Ok(WorkflowChangeVersionsOutcome { records })
    }

    async fn payload_roots_inner(&self) -> Result<PayloadRootsOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let roots = self.collect_payload_roots_tx(&tx, &schema).await?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(PayloadRootsOutcome { roots })
    }

    async fn gc_payload_blobs_inner(
        &self,
        req: PayloadGarbageCollectionRequest,
    ) -> Result<PayloadGarbageCollectionOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let mut reachable = BTreeSet::new();
        self.collect_reachable_payload_blobs_tx(&tx, &schema, &mut reachable)
            .await?;
        let rows = tx
            .query(
                &format!("select digest from {schema}.payload_blobs order by digest asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        let all_digests = rows
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<BTreeSet<_>>();
        let scanned_blobs = all_digests.len();
        let retained_blobs = all_digests
            .iter()
            .filter(|digest| reachable.contains(*digest))
            .count();
        let garbage = all_digests
            .into_iter()
            .filter(|digest| !reachable.contains(digest))
            .collect::<Vec<_>>();
        let deleted_blobs = garbage.len();
        if !req.dry_run {
            for digest in garbage {
                tx.execute(
                    &format!("delete from {schema}.payload_blobs where digest = $1"),
                    &[&digest],
                )
                .await
                .map_err(postgres_error)?;
            }
        }
        tx.commit().await.map_err(postgres_error)?;
        Ok(PayloadGarbageCollectionOutcome {
            scanned_blobs,
            retained_blobs,
            deleted_blobs,
        })
    }

    async fn collect_payload_roots_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
    ) -> Result<Vec<PayloadRootRef>> {
        let mut roots = Vec::new();
        let rows = tx
            .query(
                &format!(
                    "select data from {schema}.history_events order by run_id asc, event_id asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let data: HistoryEventData = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_history_event_payload_roots_tx(tx, &data, &mut roots)
                .await?;
        }

        let rows = tx
            .query(
                &format!("select task from {schema}.activity_tasks order by activity_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let task: ActivityTask = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::Payload(task.input));
        }

        let rows = tx
            .query(
                &format!("select task from {schema}.activity_maps order by map_command_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let task: ActivityMapTask = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::ActivityMapInputManifest(
                self.activity_map_input_root_for_roots_tx(tx, task.input_manifest)
                    .await?,
            ));
        }

        let rows = tx
            .query(
                &format!(
                    "select result
                     from {schema}.activity_map_results
                     order by map_command_id asc, item_ordinal asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let result: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::Payload(result));
        }

        let rows = tx
            .query(
                &format!("select payload from {schema}.signals order by signal_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let payload: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::Payload(payload));
        }

        let rows = tx
            .query(
                &format!(
                    "select payload
                     from {schema}.query_projections
                     order by namespace asc, workflow_id asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let payload: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::Payload(payload));
        }

        Ok(roots)
    }

    async fn collect_reachable_payload_blobs_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        let rows = tx
            .query(
                &format!(
                    "select data from {schema}.history_events order by run_id asc, event_id asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let data: HistoryEventData = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_history_event_payload_blobs_tx(tx, &data, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!("select task from {schema}.activity_tasks order by activity_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let task: ActivityTask = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_payload_blob_ref_tx(tx, &task.input, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!("select task from {schema}.activity_maps order by map_command_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let task: ActivityMapTask = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_activity_map_input_manifest_ref_tx(tx, &task.input_manifest, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!(
                    "select result
                     from {schema}.activity_map_results
                     order by map_command_id asc, item_ordinal asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let result: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_payload_blob_ref_tx(tx, &result, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!("select payload from {schema}.signals order by signal_id asc"),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let payload: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_payload_blob_ref_tx(tx, &payload, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!(
                    "select payload
                     from {schema}.query_projections
                     order by namespace asc, workflow_id asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let payload: PayloadRef = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_payload_blob_ref_tx(tx, &payload, reachable)
                .await?;
        }

        Ok(())
    }

    async fn collect_history_event_payload_roots_tx(
        &self,
        tx: &Transaction<'_>,
        data: &HistoryEventData,
        roots: &mut Vec<PayloadRootRef>,
    ) -> Result<()> {
        match data {
            HistoryEventData::WorkflowStarted { input, .. }
            | HistoryEventData::WorkflowContinuedAsNew { input } => {
                roots.push(PayloadRootRef::Payload(input.clone()));
            }
            HistoryEventData::WorkflowCompleted { result } => {
                roots.push(PayloadRootRef::Payload(result.clone()));
            }
            HistoryEventData::WorkflowFailed { failure } => {
                collect_failure_payload_roots(failure, roots);
            }
            HistoryEventData::ActivityScheduled(scheduled) => {
                roots.push(PayloadRootRef::Payload(scheduled.input.clone()));
            }
            HistoryEventData::ActivityMapScheduled(scheduled) => {
                roots.push(PayloadRootRef::ActivityMapInputManifest(
                    self.activity_map_input_root_for_roots_tx(tx, scheduled.input_manifest.clone())
                        .await?,
                ));
            }
            HistoryEventData::ActivityMapCompleted(completed) => {
                roots.push(PayloadRootRef::ActivityMapResultManifest(
                    self.activity_map_result_root_for_roots_tx(
                        tx,
                        completed.result_manifest.clone(),
                    )
                    .await?,
                ));
            }
            HistoryEventData::ActivityMapFailed(failed) => {
                collect_failure_payload_roots(&failed.failure, roots);
            }
            HistoryEventData::ActivityCompleted(completed) => {
                roots.push(PayloadRootRef::Payload(completed.result.clone()));
            }
            HistoryEventData::ActivityFailed(failed) => {
                collect_failure_payload_roots(&failed.failure, roots);
            }
            HistoryEventData::ChildWorkflowStartRequested(requested) => {
                roots.push(PayloadRootRef::Payload(requested.input.clone()));
            }
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                roots.push(PayloadRootRef::Payload(completed.result.clone()));
            }
            HistoryEventData::ChildWorkflowFailed(failed) => {
                collect_failure_payload_roots(&failed.failure, roots);
            }
            HistoryEventData::SignalConsumed(signal) => {
                roots.push(PayloadRootRef::Payload(signal.payload.clone()));
            }
            HistoryEventData::SideEffectMarker(marker) => {
                crate::payload::validate_side_effect_marker(marker)?;
            }
            HistoryEventData::WorkflowCancelled { .. }
            | HistoryEventData::WorkflowTaskStarted
            | HistoryEventData::ActivityTimedOut(_)
            | HistoryEventData::ChildWorkflowStarted(_)
            | HistoryEventData::ChildWorkflowCancelled(_)
            | HistoryEventData::TimerStarted(_)
            | HistoryEventData::TimerFired(_)
            | HistoryEventData::SelectWinner(_)
            | HistoryEventData::VersionMarker(_)
            | HistoryEventData::DeprecatedPatchMarker(_) => {}
        }
        Ok(())
    }

    async fn collect_history_event_payload_blobs_tx(
        &self,
        tx: &Transaction<'_>,
        data: &HistoryEventData,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        match data {
            HistoryEventData::WorkflowStarted { input, .. }
            | HistoryEventData::WorkflowContinuedAsNew { input } => {
                self.collect_payload_blob_ref_tx(tx, input, reachable).await
            }
            HistoryEventData::WorkflowCompleted { result } => {
                self.collect_payload_blob_ref_tx(tx, result, reachable)
                    .await
            }
            HistoryEventData::WorkflowFailed { failure } => {
                self.collect_failure_payload_blobs_tx(tx, failure, reachable)
                    .await
            }
            HistoryEventData::ActivityScheduled(scheduled) => {
                self.collect_payload_blob_ref_tx(tx, &scheduled.input, reachable)
                    .await
            }
            HistoryEventData::ActivityMapScheduled(scheduled) => {
                self.collect_activity_map_input_manifest_ref_tx(
                    tx,
                    &scheduled.input_manifest,
                    reachable,
                )
                .await
            }
            HistoryEventData::ActivityMapCompleted(completed) => {
                self.collect_activity_map_result_manifest_ref_tx(
                    tx,
                    &completed.result_manifest,
                    reachable,
                )
                .await
            }
            HistoryEventData::ActivityMapFailed(failed) => {
                self.collect_failure_payload_blobs_tx(tx, &failed.failure, reachable)
                    .await
            }
            HistoryEventData::ActivityCompleted(completed) => {
                self.collect_payload_blob_ref_tx(tx, &completed.result, reachable)
                    .await
            }
            HistoryEventData::ActivityFailed(failed) => {
                self.collect_failure_payload_blobs_tx(tx, &failed.failure, reachable)
                    .await
            }
            HistoryEventData::ChildWorkflowStartRequested(requested) => {
                self.collect_payload_blob_ref_tx(tx, &requested.input, reachable)
                    .await
            }
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                self.collect_payload_blob_ref_tx(tx, &completed.result, reachable)
                    .await
            }
            HistoryEventData::ChildWorkflowFailed(failed) => {
                self.collect_failure_payload_blobs_tx(tx, &failed.failure, reachable)
                    .await
            }
            HistoryEventData::SignalConsumed(signal) => {
                self.collect_payload_blob_ref_tx(tx, &signal.payload, reachable)
                    .await
            }
            HistoryEventData::SideEffectMarker(marker) => {
                crate::payload::validate_side_effect_marker(marker)
            }
            HistoryEventData::WorkflowCancelled { .. }
            | HistoryEventData::WorkflowTaskStarted
            | HistoryEventData::ActivityTimedOut(_)
            | HistoryEventData::ChildWorkflowStarted(_)
            | HistoryEventData::ChildWorkflowCancelled(_)
            | HistoryEventData::TimerStarted(_)
            | HistoryEventData::TimerFired(_)
            | HistoryEventData::SelectWinner(_)
            | HistoryEventData::VersionMarker(_)
            | HistoryEventData::DeprecatedPatchMarker(_) => Ok(()),
        }
    }

    async fn collect_failure_payload_blobs_tx(
        &self,
        tx: &Transaction<'_>,
        failure: &DurableFailure,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        if let Some(details) = &failure.details {
            self.collect_payload_blob_ref_tx(tx, details, reachable)
                .await?;
        }
        Ok(())
    }

    async fn collect_payload_blob_ref_tx(
        &self,
        tx: &Transaction<'_>,
        payload: &PayloadRef,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        let PayloadRef::Blob { digest, uri, .. } = payload else {
            return Ok(());
        };
        if uri.starts_with("postgres://payload/") {
            self.load_payload_blob_tx(tx, payload).await?;
        } else if !is_opaque_external_payload_ref(payload) {
            self.load_payload_blob_tx(tx, payload).await?;
        }
        reachable.insert(digest.clone());
        Ok(())
    }

    async fn activity_map_input_root_for_roots_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        self.hydrate_activity_map_input_manifest_from_storage_tx(tx, payload)
            .await
    }

    async fn activity_map_result_root_for_roots_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        self.hydrate_activity_map_result_manifest_from_storage_tx(tx, payload)
            .await
    }

    async fn collect_activity_map_input_manifest_ref_tx(
        &self,
        tx: &Transaction<'_>,
        payload: &PayloadRef,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        self.collect_payload_blob_ref_tx(tx, payload, reachable)
            .await?;
        if is_opaque_external_payload_ref(payload) {
            return Ok(());
        }
        let manifest_payload = self
            .hydrate_payload_from_storage_tx(tx, payload.clone())
            .await?;
        let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
        for page in manifest.pages {
            self.collect_payload_blob_ref_tx(tx, &page, reachable)
                .await?;
            if is_opaque_external_payload_ref(&page) {
                continue;
            }
            let page_payload = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let page: ActivityMapInputPage = crate::decode_payload(&page_payload)?;
            for item in page.items {
                self.collect_payload_blob_ref_tx(tx, &item, reachable)
                    .await?;
            }
        }
        Ok(())
    }

    async fn collect_activity_map_result_manifest_ref_tx(
        &self,
        tx: &Transaction<'_>,
        payload: &PayloadRef,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        self.collect_payload_blob_ref_tx(tx, payload, reachable)
            .await?;
        if is_opaque_external_payload_ref(payload) {
            return Ok(());
        }
        let manifest_payload = self
            .hydrate_payload_from_storage_tx(tx, payload.clone())
            .await?;
        let manifest: ActivityMapResultManifest = crate::decode_payload(&manifest_payload)?;
        for page in manifest.pages {
            self.collect_payload_blob_ref_tx(tx, &page, reachable)
                .await?;
            if is_opaque_external_payload_ref(&page) {
                continue;
            }
            let page_payload = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let page: ActivityMapResultPage = crate::decode_payload(&page_payload)?;
            for result in page.results {
                self.collect_payload_blob_ref_tx(tx, &result, reachable)
                    .await?;
            }
        }
        Ok(())
    }

    async fn normalize_payload_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        match payload {
            PayloadRef::Inline {
                codec,
                schema_fingerprint,
                compression,
                encryption,
                bytes,
            } if bytes.len() > self.payload_config.inline_threshold_bytes => {
                let digest = digest_bytes(&bytes);
                let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                let encryption_blob = encode_encryption_metadata(&encryption)?;
                let schema = self.schema_sql();
                tx.execute(
                    &format!(
                        "insert into {schema}.payload_blobs
                         (digest, codec, schema_fingerprint, compression, encryption, size, bytes)
                         values ($1, $2, $3, $4, $5, $6, $7)
                         on conflict(digest) do nothing"
                    ),
                    &[
                        &digest,
                        &codec_to_str(codec),
                        &schema_fingerprint.0,
                        &compression_to_str(compression),
                        &encryption_blob,
                        &i64::try_from(size).unwrap_or(i64::MAX),
                        &bytes,
                    ],
                )
                .await
                .map_err(postgres_error)?;
                Ok(PayloadRef::Blob {
                    codec,
                    schema_fingerprint,
                    compression,
                    encryption,
                    digest: digest.clone(),
                    size,
                    uri: format!("postgres://payload/{digest}"),
                })
            }
            payload @ PayloadRef::Inline { .. } => Ok(payload),
            payload @ PayloadRef::Blob { .. } => {
                if !is_opaque_external_payload_ref(&payload) {
                    self.load_payload_blob_tx(tx, &payload).await?;
                }
                Ok(payload)
            }
        }
    }

    async fn hydrate_payload_from_storage(&self, payload: PayloadRef) -> Result<PayloadRef> {
        match payload {
            payload @ PayloadRef::Inline { .. } => Ok(payload),
            payload @ PayloadRef::Blob { .. } if is_opaque_external_payload_ref(&payload) => {
                Ok(payload)
            }
            payload @ PayloadRef::Blob { .. } => {
                let PayloadRef::Blob {
                    codec,
                    schema_fingerprint,
                    compression,
                    encryption,
                    ..
                } = &payload
                else {
                    unreachable!();
                };
                let blob = self.load_payload_blob(&payload).await?;
                Ok(PayloadRef::Inline {
                    codec: *codec,
                    schema_fingerprint: schema_fingerprint.clone(),
                    compression: *compression,
                    encryption: encryption.clone(),
                    bytes: blob.bytes,
                })
            }
        }
    }

    async fn hydrate_payload_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        match payload {
            payload @ PayloadRef::Inline { .. } => Ok(payload),
            payload @ PayloadRef::Blob { .. } if is_opaque_external_payload_ref(&payload) => {
                Ok(payload)
            }
            payload @ PayloadRef::Blob { .. } => {
                let PayloadRef::Blob {
                    codec,
                    schema_fingerprint,
                    compression,
                    encryption,
                    ..
                } = &payload
                else {
                    unreachable!();
                };
                let blob = self.load_payload_blob_tx(tx, &payload).await?;
                Ok(PayloadRef::Inline {
                    codec: *codec,
                    schema_fingerprint: schema_fingerprint.clone(),
                    compression: *compression,
                    encryption: encryption.clone(),
                    bytes: blob.bytes,
                })
            }
        }
    }

    async fn normalize_activity_map_input_manifest_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
            let mut items = Vec::with_capacity(page.items.len());
            for item in page.items {
                items.push(self.normalize_payload_for_storage_tx(tx, item).await?);
            }
            page.items = items;
            let page = crate::encode_payload_with_codec(&page, self.payload_config.codec)?;
            pages.push(self.normalize_payload_for_storage_tx(tx, page).await?);
        }
        manifest.pages = pages;
        let root = crate::encode_payload_with_codec(&manifest, self.payload_config.codec)?;
        self.normalize_payload_for_storage_tx(tx, root).await
    }

    async fn normalize_activity_map_result_manifest_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
            let mut results = Vec::with_capacity(page.results.len());
            for result in page.results {
                results.push(self.normalize_payload_for_storage_tx(tx, result).await?);
            }
            page.results = results;
            let page = crate::encode_payload_with_codec(&page, self.payload_config.codec)?;
            pages.push(self.normalize_payload_for_storage_tx(tx, page).await?);
        }
        manifest.pages = pages;
        let root = crate::encode_payload_with_codec(&manifest, self.payload_config.codec)?;
        self.normalize_payload_for_storage_tx(tx, root).await
    }

    async fn hydrate_activity_map_input_manifest_from_storage(
        &self,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage(payload).await?;
        let root_codec = root.codec();
        let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage(page).await?;
            let page_codec = page.codec();
            let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
            let mut items = Vec::with_capacity(page.items.len());
            for item in page.items {
                items.push(self.hydrate_payload_from_storage(item).await?);
            }
            page.items = items;
            pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
        }
        manifest.pages = pages;
        crate::encode_payload_with_codec(&manifest, root_codec)
    }

    async fn hydrate_activity_map_result_manifest_from_storage(
        &self,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage(payload).await?;
        let root_codec = root.codec();
        let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage(page).await?;
            let page_codec = page.codec();
            let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
            let mut results = Vec::with_capacity(page.results.len());
            for result in page.results {
                results.push(self.hydrate_payload_from_storage(result).await?);
            }
            page.results = results;
            pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
        }
        manifest.pages = pages;
        crate::encode_payload_with_codec(&manifest, root_codec)
    }

    async fn hydrate_activity_map_input_manifest_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let root_codec = root.codec();
        let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let page_codec = page.codec();
            let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
            let mut items = Vec::with_capacity(page.items.len());
            for item in page.items {
                items.push(self.hydrate_payload_from_storage_tx(tx, item).await?);
            }
            page.items = items;
            pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
        }
        manifest.pages = pages;
        crate::encode_payload_with_codec(&manifest, root_codec)
    }

    async fn hydrate_activity_map_result_manifest_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_opaque_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let root_codec = root.codec();
        let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let page_codec = page.codec();
            let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
            let mut results = Vec::with_capacity(page.results.len());
            for result in page.results {
                results.push(self.hydrate_payload_from_storage_tx(tx, result).await?);
            }
            page.results = results;
            pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
        }
        manifest.pages = pages;
        crate::encode_payload_with_codec(&manifest, root_codec)
    }

    async fn normalize_history_event_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        data: HistoryEventData,
    ) -> Result<HistoryEventData> {
        match data {
            HistoryEventData::WorkflowStarted {
                workflow_type,
                input,
            } => Ok(HistoryEventData::WorkflowStarted {
                workflow_type,
                input: self.normalize_payload_for_storage_tx(tx, input).await?,
            }),
            HistoryEventData::WorkflowCompleted { result } => {
                Ok(HistoryEventData::WorkflowCompleted {
                    result: self.normalize_payload_for_storage_tx(tx, result).await?,
                })
            }
            HistoryEventData::WorkflowFailed { failure } => Ok(HistoryEventData::WorkflowFailed {
                failure: self.normalize_failure_for_storage_tx(tx, failure).await?,
            }),
            HistoryEventData::WorkflowContinuedAsNew { input } => {
                Ok(HistoryEventData::WorkflowContinuedAsNew {
                    input: self.normalize_payload_for_storage_tx(tx, input).await?,
                })
            }
            HistoryEventData::ActivityScheduled(mut scheduled) => {
                scheduled.input = self
                    .normalize_payload_for_storage_tx(tx, scheduled.input)
                    .await?;
                Ok(HistoryEventData::ActivityScheduled(scheduled))
            }
            HistoryEventData::ActivityMapScheduled(mut scheduled) => {
                scheduled.input_manifest = self
                    .normalize_activity_map_input_manifest_for_storage_tx(
                        tx,
                        scheduled.input_manifest,
                    )
                    .await?;
                Ok(HistoryEventData::ActivityMapScheduled(scheduled))
            }
            HistoryEventData::ActivityMapCompleted(mut completed) => {
                completed.result_manifest = self
                    .normalize_activity_map_result_manifest_for_storage_tx(
                        tx,
                        completed.result_manifest,
                    )
                    .await?;
                Ok(HistoryEventData::ActivityMapCompleted(completed))
            }
            HistoryEventData::ActivityMapFailed(mut failed) => {
                failed.failure = self
                    .normalize_failure_for_storage_tx(tx, failed.failure)
                    .await?;
                Ok(HistoryEventData::ActivityMapFailed(failed))
            }
            HistoryEventData::ActivityCompleted(mut completed) => {
                completed.result = self
                    .normalize_payload_for_storage_tx(tx, completed.result)
                    .await?;
                Ok(HistoryEventData::ActivityCompleted(completed))
            }
            HistoryEventData::ActivityFailed(mut failed) => {
                failed.failure = self
                    .normalize_failure_for_storage_tx(tx, failed.failure)
                    .await?;
                Ok(HistoryEventData::ActivityFailed(failed))
            }
            HistoryEventData::ChildWorkflowStartRequested(mut requested) => {
                requested.input = self
                    .normalize_payload_for_storage_tx(tx, requested.input)
                    .await?;
                Ok(HistoryEventData::ChildWorkflowStartRequested(requested))
            }
            HistoryEventData::ChildWorkflowCompleted(mut completed) => {
                completed.result = self
                    .normalize_payload_for_storage_tx(tx, completed.result)
                    .await?;
                Ok(HistoryEventData::ChildWorkflowCompleted(completed))
            }
            HistoryEventData::ChildWorkflowFailed(mut failed) => {
                failed.failure = self
                    .normalize_failure_for_storage_tx(tx, failed.failure)
                    .await?;
                Ok(HistoryEventData::ChildWorkflowFailed(failed))
            }
            HistoryEventData::SignalConsumed(mut signal) => {
                signal.payload = self
                    .normalize_payload_for_storage_tx(tx, signal.payload)
                    .await?;
                Ok(HistoryEventData::SignalConsumed(signal))
            }
            other => Ok(other),
        }
    }

    async fn normalize_activity_task_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut task: crate::ActivityTask,
    ) -> Result<crate::ActivityTask> {
        task.input = self
            .normalize_payload_for_storage_tx(tx, task.input)
            .await?;
        Ok(task)
    }

    async fn normalize_activity_map_task_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut task: ActivityMapTask,
    ) -> Result<ActivityMapTask> {
        task.input_manifest = self
            .normalize_activity_map_input_manifest_for_storage_tx(tx, task.input_manifest)
            .await?;
        Ok(task)
    }

    async fn normalize_child_start_message_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut message: ChildStartOutboxMessage,
    ) -> Result<ChildStartOutboxMessage> {
        message.input = self
            .normalize_payload_for_storage_tx(tx, message.input)
            .await?;
        Ok(message)
    }

    async fn hydrate_activity_task_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut task: ActivityTask,
    ) -> Result<ActivityTask> {
        task.input = self.hydrate_payload_from_storage_tx(tx, task.input).await?;
        Ok(task)
    }

    async fn normalize_failure_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut failure: DurableFailure,
    ) -> Result<DurableFailure> {
        if let Some(details) = failure.details.take() {
            failure.details = Some(self.normalize_payload_for_storage_tx(tx, details).await?);
        }
        Ok(failure)
    }

    async fn hydrate_history_event_from_storage(
        &self,
        data: HistoryEventData,
    ) -> Result<HistoryEventData> {
        match data {
            HistoryEventData::WorkflowStarted {
                workflow_type,
                input,
            } => Ok(HistoryEventData::WorkflowStarted {
                workflow_type,
                input: self.hydrate_payload_from_storage(input).await?,
            }),
            HistoryEventData::WorkflowCompleted { result } => {
                Ok(HistoryEventData::WorkflowCompleted {
                    result: self.hydrate_payload_from_storage(result).await?,
                })
            }
            HistoryEventData::WorkflowFailed { failure } => Ok(HistoryEventData::WorkflowFailed {
                failure: self.hydrate_failure_from_storage(failure).await?,
            }),
            HistoryEventData::WorkflowContinuedAsNew { input } => {
                Ok(HistoryEventData::WorkflowContinuedAsNew {
                    input: self.hydrate_payload_from_storage(input).await?,
                })
            }
            HistoryEventData::ActivityScheduled(mut scheduled) => {
                scheduled.input = self.hydrate_payload_from_storage(scheduled.input).await?;
                Ok(HistoryEventData::ActivityScheduled(scheduled))
            }
            HistoryEventData::ActivityMapScheduled(mut scheduled) => {
                scheduled.input_manifest = self
                    .hydrate_activity_map_input_manifest_from_storage(scheduled.input_manifest)
                    .await?;
                Ok(HistoryEventData::ActivityMapScheduled(scheduled))
            }
            HistoryEventData::ActivityMapCompleted(mut completed) => {
                completed.result_manifest = self
                    .hydrate_activity_map_result_manifest_from_storage(completed.result_manifest)
                    .await?;
                Ok(HistoryEventData::ActivityMapCompleted(completed))
            }
            HistoryEventData::ActivityMapFailed(mut failed) => {
                failed.failure = self.hydrate_failure_from_storage(failed.failure).await?;
                Ok(HistoryEventData::ActivityMapFailed(failed))
            }
            HistoryEventData::ActivityCompleted(mut completed) => {
                completed.result = self.hydrate_payload_from_storage(completed.result).await?;
                Ok(HistoryEventData::ActivityCompleted(completed))
            }
            HistoryEventData::ActivityFailed(mut failed) => {
                failed.failure = self.hydrate_failure_from_storage(failed.failure).await?;
                Ok(HistoryEventData::ActivityFailed(failed))
            }
            HistoryEventData::ChildWorkflowStartRequested(mut requested) => {
                requested.input = self.hydrate_payload_from_storage(requested.input).await?;
                Ok(HistoryEventData::ChildWorkflowStartRequested(requested))
            }
            HistoryEventData::ChildWorkflowCompleted(mut completed) => {
                completed.result = self.hydrate_payload_from_storage(completed.result).await?;
                Ok(HistoryEventData::ChildWorkflowCompleted(completed))
            }
            HistoryEventData::ChildWorkflowFailed(mut failed) => {
                failed.failure = self.hydrate_failure_from_storage(failed.failure).await?;
                Ok(HistoryEventData::ChildWorkflowFailed(failed))
            }
            HistoryEventData::SignalConsumed(mut signal) => {
                signal.payload = self.hydrate_payload_from_storage(signal.payload).await?;
                Ok(HistoryEventData::SignalConsumed(signal))
            }
            other => Ok(other),
        }
    }

    async fn hydrate_failure_from_storage(
        &self,
        mut failure: DurableFailure,
    ) -> Result<DurableFailure> {
        if let Some(details) = failure.details.take() {
            failure.details = Some(self.hydrate_payload_from_storage(details).await?);
        }
        Ok(failure)
    }

    async fn load_payload_blob_tx(
        &self,
        tx: &Transaction<'_>,
        payload: &PayloadRef,
    ) -> Result<PayloadBlob> {
        let PayloadRef::Blob {
            codec: ref_codec,
            schema_fingerprint: ref_schema_fingerprint,
            compression: ref_compression,
            encryption: ref_encryption,
            digest,
            size,
            uri: _,
        } = payload
        else {
            return Err(Error::PayloadDecode(
                "inline payload does not reference blob storage".to_owned(),
            ));
        };
        let schema = self.schema_sql();
        let row = tx
            .query_opt(
                &format!(
                    "select codec, schema_fingerprint, compression, encryption, size, bytes
                     from {schema}.payload_blobs
                     where digest = $1"
                ),
                &[digest],
            )
            .await
            .map_err(postgres_error)?
            .ok_or_else(|| Error::PayloadDecode(format!("missing payload blob `{digest}`")))?;
        decode_payload_blob_row(
            payload,
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
            *ref_codec,
            ref_schema_fingerprint,
            *ref_compression,
            ref_encryption,
            digest,
            *size,
        )
    }

    async fn load_payload_blob(&self, payload: &PayloadRef) -> Result<PayloadBlob> {
        let PayloadRef::Blob {
            codec: ref_codec,
            schema_fingerprint: ref_schema_fingerprint,
            compression: ref_compression,
            encryption: ref_encryption,
            digest,
            size,
            uri: _,
        } = payload
        else {
            return Err(Error::PayloadDecode(
                "inline payload does not reference blob storage".to_owned(),
            ));
        };
        let schema = self.schema_sql();
        let client = self.client().await?;
        let row = client
            .query_opt(
                &format!(
                    "select codec, schema_fingerprint, compression, encryption, size, bytes
                     from {schema}.payload_blobs
                     where digest = $1"
                ),
                &[digest],
            )
            .await
            .map_err(postgres_error)?
            .ok_or_else(|| Error::PayloadDecode(format!("missing payload blob `{digest}`")))?;
        decode_payload_blob_row(
            payload,
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
            *ref_codec,
            ref_schema_fingerprint,
            *ref_compression,
            ref_encryption,
            digest,
            *size,
        )
    }

    fn schema_sql(&self) -> String {
        quote_ident(&self.schema)
    }

    async fn client(&self) -> Result<PooledPostgresClient> {
        self.pool
            .get()
            .await
            .map_err(|err| Error::Backend(format!("postgres pool checkout error: {err}")))
    }
}

fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return Err(Error::Backend(
            "postgres schema identifier must not be empty".to_owned(),
        ));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(Error::Backend(format!(
            "postgres schema identifier `{identifier}` must start with an ASCII letter or underscore"
        )));
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        return Err(Error::Backend(format!(
            "postgres schema identifier `{identifier}` must contain only ASCII letters, digits, or underscores"
        )));
    }
    Ok(())
}

fn shard_for_workflow(
    namespace: &Namespace,
    workflow_id: &WorkflowId,
    logical_shards: u32,
) -> ShardId {
    let mut hasher = Sha256::new();
    hasher.update(namespace.0.as_bytes());
    hasher.update([0]);
    hasher.update(workflow_id.0.as_bytes());
    let digest = hasher.finalize();
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&digest[..8]);
    ShardId((u64::from_be_bytes(prefix) % u64::from(logical_shards.max(1))) as u32)
}

fn partition_suffix(partition: u32, physical_partitions: u32) -> String {
    let width = physical_partitions.saturating_sub(1).max(1).ilog10() as usize + 1;
    format!("p{partition:0width$}")
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn postgres_error(err: tokio_postgres::Error) -> Error {
    if let Some(db_error) = err.as_db_error() {
        return Error::Backend(format!(
            "postgres error SQLSTATE {}: {}{}{}",
            db_error.code().code(),
            db_error.message(),
            db_error
                .detail()
                .map(|detail| format!(" detail: {detail}"))
                .unwrap_or_default(),
            db_error
                .constraint()
                .map(|constraint| format!(" constraint: {constraint}"))
                .unwrap_or_default()
        ));
    }
    Error::Backend(format!("postgres error: {err}"))
}

fn is_retryable_postgres_transaction_abort(err: &Error) -> bool {
    matches!(
        err,
        Error::Backend(message)
            if message.contains("SQLSTATE 40P01") || message.contains("SQLSTATE 40001")
    )
}

fn ready_at_ms_for_delay(delay: Duration) -> i64 {
    if delay.is_zero() {
        0
    } else {
        unix_epoch_millis().saturating_add(duration_millis_i64(delay))
    }
}

fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn activity_timeout_at_ms(timeout: Option<Duration>) -> Option<i64> {
    activity_timeout_at_ms_from(TimestampMs(unix_epoch_millis()), timeout)
}

fn activity_timeout_at_ms_from(now: TimestampMs, timeout: Option<Duration>) -> Option<i64> {
    timeout.map(|timeout| now.0.saturating_add(duration_millis_i64(timeout)))
}

fn timeout_message(activity_id: &ActivityId, attempt: u32, heartbeat: bool) -> String {
    if heartbeat {
        format!(
            "activity `{}` missed heartbeat on attempt {}",
            activity_id.0,
            attempt.max(1)
        )
    } else {
        format!(
            "activity `{}` timed out on attempt {}",
            activity_id.0,
            attempt.max(1)
        )
    }
}

fn should_retry_activity(task: &ActivityTask) -> bool {
    task.attempt < task.retry_policy.max_attempts.max(1)
}

fn wait_kind_to_str(kind: &WaitKind) -> &'static str {
    match kind {
        WaitKind::Timer => "timer",
        WaitKind::Signal => "signal",
    }
}

async fn signal_wait_ready(tx: &Transaction<'_>, schema: &str, run_id: &RunId) -> Result<bool> {
    Ok(tx
        .query_opt(
            &format!(
                "select 1
                 from {schema}.active_waits w
                 join {schema}.signals s on s.run_id = w.run_id
                    and s.signal_name = w.wait_key
                    and s.consumed = false
                 where w.run_id = $1 and w.kind = $2
                 limit 1"
            ),
            &[&run_id.0, &wait_kind_to_str(&WaitKind::Signal)],
        )
        .await
        .map_err(postgres_error)?
        .is_some())
}

async fn cleanup_run_operational_state_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
) -> Result<()> {
    tx.execute(
        &format!("delete from {schema}.active_waits where run_id = $1"),
        &[&run_id.0],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 claim_token = null,
                 heartbeat_deadline_at_ms = null
             where run_id = $1"
        ),
        &[&run_id.0],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!(
            "update {schema}.activity_maps
             set completed = true, in_flight = 0
             where run_id = $1"
        ),
        &[&run_id.0],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn handle_terminal_run_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    notify_parent_of_child_terminal_tx(tx, schema, run_id, terminal_event).await?;
    cancel_children_for_parent_tx(tx, schema, run_id).await?;
    Ok(())
}

async fn continue_run_as_new_tx(
    tx: &Transaction<'_>,
    schema: &str,
    old_run_id: &RunId,
    event: HistoryEventData,
) -> Result<()> {
    let HistoryEventData::WorkflowContinuedAsNew { input } = event else {
        return Ok(());
    };
    let Some(row) = tx
        .query_opt(
            &format!(
                "select workflow_name, workflow_version
                 from {schema}.workflow_instances
                 where run_id = $1"
            ),
            &[&old_run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(old_run_id.clone()));
    };
    let workflow_type = WorkflowType::new(
        row.get::<_, String>(0),
        u32::try_from(row.get::<_, i32>(1)).unwrap_or(0),
    );
    let new_run_id = next_run_id(tx, schema).await?;
    insert_history_event(
        tx,
        schema,
        &new_run_id,
        EventId(1),
        HistoryEventData::WorkflowStarted {
            workflow_type,
            input,
        },
    )
    .await?;
    tx.execute(
        &format!(
            "update {schema}.workflow_instances
             set run_id = $1,
                 current_event_id = 1,
                 ready_reason = $2,
                 ready_at_ms = 0,
                 workflow_claim_token = null,
                 terminal = false
             where run_id = $3"
        ),
        &[
            &new_run_id.0,
            &reason_to_str(&WorkflowTaskReason::WorkflowStarted),
            &old_run_id.0,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn notify_parent_of_child_terminal_tx(
    tx: &Transaction<'_>,
    schema: &str,
    child_run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    let Some(row) = tx
        .query_opt(
            &format!(
                "select parent_run_id, parent_command_seq
                 from {schema}.workflow_instances
                 where run_id = $1"
            ),
            &[&child_run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(child_run_id.clone()));
    };
    let parent_run_id: Option<String> = row.get(0);
    let parent_command_seq: Option<i64> = row.get(1);
    let Some((parent_run_id, parent_command_seq)) = parent_run_id
        .zip(parent_command_seq)
        .and_then(|(run_id, seq)| Some((RunId::new(run_id), CommandSeq(u64::try_from(seq).ok()?))))
    else {
        return Ok(());
    };
    let command_id = CommandId {
        run_id: parent_run_id.clone(),
        seq: parent_command_seq,
    };
    if child_terminal_event_exists_tx(tx, schema, &command_id).await? {
        return Ok(());
    }
    let Some(row) = tx
        .query_opt(
            &format!(
                "select current_event_id, terminal
                 from {schema}.workflow_instances
                 where run_id = $1
                 for update"
            ),
            &[&parent_run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(());
    };
    let parent_tail = EventId(u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX));
    let parent_terminal: bool = row.get(1);
    if parent_terminal {
        return Ok(());
    }
    let event_id = parent_tail.next();
    let (event_data, reason) = match terminal_event {
        HistoryEventData::WorkflowCompleted { result } => (
            HistoryEventData::ChildWorkflowCompleted(crate::ChildWorkflowCompleted {
                command_id,
                result: result.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowCompleted,
        ),
        HistoryEventData::WorkflowFailed { failure } => (
            HistoryEventData::ChildWorkflowFailed(crate::ChildWorkflowFailed {
                command_id,
                failure: failure.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowFailed,
        ),
        HistoryEventData::WorkflowCancelled { reason } => (
            HistoryEventData::ChildWorkflowCancelled(crate::ChildWorkflowCancelled {
                command_id,
                reason: reason.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowCancelled,
        ),
        _ => return Ok(()),
    };
    insert_history_event(tx, schema, &parent_run_id, event_id, event_data).await?;
    set_workflow_ready_tx(tx, schema, &parent_run_id, event_id, reason).await
}

async fn child_terminal_event_exists_tx(
    tx: &Transaction<'_>,
    schema: &str,
    command_id: &CommandId,
) -> Result<bool> {
    let child_event_types = vec![
        event_type_to_str(&HistoryEventType::ChildWorkflowCompleted),
        event_type_to_str(&HistoryEventType::ChildWorkflowFailed),
        event_type_to_str(&HistoryEventType::ChildWorkflowCancelled),
    ];
    let rows = tx
        .query(
            &format!(
                "select data
                 from {schema}.history_events
                 where run_id = $1
                   and event_type = any($2::text[])"
            ),
            &[&command_id.run_id.0, &child_event_types],
        )
        .await
        .map_err(postgres_error)?;
    for row in rows {
        let blob: Vec<u8> = row.get(0);
        let event: HistoryEventData =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        let matches = match event {
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                completed.command_id == *command_id
            }
            HistoryEventData::ChildWorkflowFailed(failed) => failed.command_id == *command_id,
            HistoryEventData::ChildWorkflowCancelled(cancelled) => {
                cancelled.command_id == *command_id
            }
            _ => false,
        };
        if matches {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn cancel_children_for_parent_tx(
    tx: &Transaction<'_>,
    schema: &str,
    parent_run_id: &RunId,
) -> Result<()> {
    let rows = tx
        .query(
            &format!(
                "select run_id, current_event_id
                 from {schema}.workflow_instances
                 where parent_run_id = $1
                   and parent_close_policy = $2
                   and terminal = false
                 order by run_id asc
                 for update"
            ),
            &[
                &parent_run_id.0,
                &parent_close_policy_to_str(ParentClosePolicy::Cancel),
            ],
        )
        .await
        .map_err(postgres_error)?;
    let children = rows
        .into_iter()
        .map(|row| {
            (
                RunId::new(row.get::<_, String>(0)),
                EventId(u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX)),
            )
        })
        .collect::<Vec<_>>();
    for (child_run_id, tail) in children {
        let event_id = tail.next();
        insert_history_event(
            tx,
            schema,
            &child_run_id,
            event_id,
            HistoryEventData::WorkflowCancelled {
                reason: format!("parent workflow `{parent_run_id}` closed"),
            },
        )
        .await?;
        cleanup_run_operational_state_tx(tx, schema, &child_run_id).await?;
        tx.execute(
            &format!(
                "update {schema}.workflow_instances
                 set current_event_id = $1,
                     workflow_claim_token = null,
                     terminal = true,
                     ready_reason = null,
                     ready_at_ms = 0
                 where run_id = $2"
            ),
            &[
                &i64::try_from(event_id.0).unwrap_or(i64::MAX),
                &child_run_id.0,
            ],
        )
        .await
        .map_err(postgres_error)?;
    }
    Ok(())
}

async fn cancel_command_operational_state_tx(
    tx: &Transaction<'_>,
    schema: &str,
    command_id: &CommandId,
) -> Result<()> {
    let activity_id = ActivityId::new(command_id);
    let map_prefix = format!("{}:map:%", activity_id.0);
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 claim_token = null,
                 heartbeat_deadline_at_ms = null
             where activity_id = $1 or activity_id like $2"
        ),
        &[&activity_id.0, &map_prefix],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!(
            "update {schema}.activity_maps
             set completed = true, in_flight = 0
             where map_command_id = $1"
        ),
        &[&map_command_key(command_id)],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn set_workflow_ready_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    event_id: EventId,
    reason: WorkflowTaskReason,
) -> Result<()> {
    tx.execute(
        &format!(
            "update {schema}.workflow_instances
             set current_event_id = $1, ready_reason = $2, ready_at_ms = 0
             where run_id = $3"
        ),
        &[
            &i64::try_from(event_id.0).unwrap_or(i64::MAX),
            &reason_to_str(&reason),
            &run_id.0,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

fn unix_epoch_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

async fn next_sequence_value(tx: &Transaction<'_>, schema: &str, sequence: &str) -> Result<u64> {
    let row = tx
        .query_one(
            &format!("select nextval('{schema}.{sequence}'::regclass)"),
            &[],
        )
        .await
        .map_err(postgres_error)?;
    let value: i64 = row.get(0);
    u64::try_from(value).map_err(|_| {
        Error::Backend(format!(
            "postgres sequence `{sequence}` returned invalid value {value}"
        ))
    })
}

async fn next_run_id(tx: &Transaction<'_>, schema: &str) -> Result<RunId> {
    Ok(RunId::new(format!(
        "run-{}",
        next_sequence_value(tx, schema, "run_id_seq").await?
    )))
}

fn has_duplicate_activity_completion_ids(completions: &[CompleteActivityRequest]) -> bool {
    let mut seen = BTreeSet::new();
    completions
        .iter()
        .any(|completion| !seen.insert(completion.claim.activity_id.0.as_str()))
}

fn is_activity_completion_item_error(err: &Error) -> bool {
    match err {
        Error::StaleLease | Error::RunNotFound(_) | Error::TerminalWorkflow => true,
        Error::Backend(message) => {
            message.starts_with("activity `") && message.ends_with(" not found")
        }
        _ => false,
    }
}

async fn next_signal_sequence(tx: &Transaction<'_>, schema: &str) -> Result<u64> {
    next_sequence_value(tx, schema, "signal_seq").await
}

async fn next_claim_token(tx: &Transaction<'_>, schema: &str) -> Result<u64> {
    next_sequence_value(tx, schema, "claim_token_seq").await
}

async fn insert_history_event(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    event_id: EventId,
    data: HistoryEventData,
) -> Result<()> {
    let event_type = event_type_to_str(&data.event_type());
    let blob =
        rmp_serde::to_vec_named(&data).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        &format!(
            "insert into {schema}.history_events(run_id, event_id, event_type, data)
             values ($1, $2, $3, $4)"
        ),
        &[
            &run_id.0,
            &i64::try_from(event_id.0).unwrap_or(i64::MAX),
            &event_type,
            &blob,
        ],
    )
    .await
    .map_err(postgres_error)?;
    index_workflow_change_marker(tx, schema, run_id, event_id, &data).await?;
    Ok(())
}

async fn insert_history_events(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    events: &[(EventId, HistoryEventData)],
) -> Result<()> {
    let rows = events
        .iter()
        .map(|(event_id, data)| HistoryEventInsert {
            run_id,
            event_id: *event_id,
            data,
        })
        .collect::<Vec<_>>();
    insert_history_event_rows(tx, schema, &rows).await
}

struct HistoryEventInsert<'a> {
    run_id: &'a RunId,
    event_id: EventId,
    data: &'a HistoryEventData,
}

async fn insert_history_event_rows(
    tx: &Transaction<'_>,
    schema: &str,
    events: &[HistoryEventInsert<'_>],
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let run_ids = events
        .iter()
        .map(|event| event.run_id.0.clone())
        .collect::<Vec<_>>();
    let event_ids = events
        .iter()
        .map(|event| i64::try_from(event.event_id.0).unwrap_or(i64::MAX))
        .collect::<Vec<_>>();
    let event_types = events
        .iter()
        .map(|event| event_type_to_str(&event.data.event_type()).to_owned())
        .collect::<Vec<_>>();
    let payloads = events
        .iter()
        .map(|event| rmp_serde::to_vec_named(event.data))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| Error::PayloadEncode(err.to_string()))?;

    tx.execute(
        &format!(
            "insert into {schema}.history_events(run_id, event_id, event_type, data)
             select run_id, event_id, event_type, data
             from unnest($1::text[], $2::bigint[], $3::text[], $4::bytea[])
                  as event_rows(run_id, event_id, event_type, data)"
        ),
        &[&run_ids, &event_ids, &event_types, &payloads],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

enum InlineChildStartOutcome {
    Started(RunId),
    Failed(DurableFailure),
    Skipped,
}

async fn start_child_workflow_inline_tx(
    tx: &Transaction<'_>,
    schema: &str,
    namespace: &str,
    shard_id: i32,
    message: &ChildStartOutboxMessage,
) -> Result<InlineChildStartOutcome> {
    let run_id = next_run_id(tx, schema).await?;
    let inserted = tx
        .query_opt(
            &format!(
                "insert into {schema}.workflow_instances
                 (namespace, workflow_id, run_id, shard_id, workflow_name, workflow_version, task_queue,
                  current_event_id, ready_reason, ready_at_ms, workflow_claim_token, terminal,
                  parent_run_id, parent_command_seq, parent_close_policy)
                 values ($1, $2, $3, $4, $5, $6, $7, 1, $8, 0, null, false, $9, $10, $11)
                 on conflict(namespace, workflow_id) do nothing
                 returning run_id"
            ),
            &[
                &namespace,
                &message.workflow_id.0,
                &run_id.0,
                &shard_id,
                &message.workflow_type.name,
                &(i32::try_from(message.workflow_type.version).unwrap_or(i32::MAX)),
                &message.task_queue.0,
                &reason_to_str(&WorkflowTaskReason::WorkflowStarted),
                &message.command_id.run_id.0,
                &i64::try_from(message.command_id.seq.0).unwrap_or(i64::MAX),
                &parent_close_policy_to_str(message.parent_close_policy),
            ],
        )
        .await
        .map_err(postgres_error)?;

    if inserted.is_some() {
        insert_history_event(
            tx,
            schema,
            &run_id,
            EventId(1),
            HistoryEventData::WorkflowStarted {
                workflow_type: message.workflow_type.clone(),
                input: message.input.clone(),
            },
        )
        .await?;
        return Ok(InlineChildStartOutcome::Started(run_id));
    }

    let Some(row) = tx
        .query_opt(
            &format!(
                "select run_id, parent_run_id, parent_command_seq
                 from {schema}.workflow_instances
                 where namespace = $1 and workflow_id = $2
                 for update"
            ),
            &[&namespace, &message.workflow_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(InlineChildStartOutcome::Skipped);
    };
    let existing_run_id = RunId::new(row.get::<_, String>(0));
    let parent_run_id: Option<String> = row.get(1);
    let parent_command_seq: Option<i64> = row.get(2);
    let same_child = parent_run_id.as_deref() == Some(message.command_id.run_id.0.as_str())
        && parent_command_seq.and_then(|seq| u64::try_from(seq).ok())
            == Some(message.command_id.seq.0);
    if same_child {
        return Ok(InlineChildStartOutcome::Started(existing_run_id));
    }

    Ok(InlineChildStartOutcome::Failed(
        DurableFailure::non_retryable(
            "durust.child_workflow_id_conflict",
            format!("workflow id `{}` is already started", message.workflow_id),
        ),
    ))
}

async fn child_event_exists_tx(
    tx: &Transaction<'_>,
    schema: &str,
    command_id: &CommandId,
) -> Result<bool> {
    let child_event_types = vec![
        event_type_to_str(&HistoryEventType::ChildWorkflowStarted),
        event_type_to_str(&HistoryEventType::ChildWorkflowCompleted),
        event_type_to_str(&HistoryEventType::ChildWorkflowFailed),
        event_type_to_str(&HistoryEventType::ChildWorkflowCancelled),
    ];
    let rows = tx
        .query(
            &format!(
                "select data
                 from {schema}.history_events
                 where run_id = $1
                   and event_type = any($2::text[])"
            ),
            &[&command_id.run_id.0, &child_event_types],
        )
        .await
        .map_err(postgres_error)?;
    for row in rows {
        let blob: Vec<u8> = row.get(0);
        let event: HistoryEventData =
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
        let matches = match event {
            HistoryEventData::ChildWorkflowStarted(started) => started.command_id == *command_id,
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                completed.command_id == *command_id
            }
            HistoryEventData::ChildWorkflowFailed(failed) => failed.command_id == *command_id,
            HistoryEventData::ChildWorkflowCancelled(cancelled) => {
                cancelled.command_id == *command_id
            }
            _ => false,
        };
        if matches {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn insert_activity_map_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    namespace: &str,
    map_task: &ActivityMapTask,
) -> Result<()> {
    let manifest_payload = backend
        .hydrate_activity_map_input_manifest_from_storage_tx(tx, map_task.input_manifest.clone())
        .await?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
    let task_blob =
        rmp_serde::to_vec_named(map_task).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        &format!(
            "insert into {schema}.activity_maps
             (map_command_id, namespace, run_id, command_seq, task, item_count,
              next_ordinal, in_flight, completed)
             values ($1, $2, $3, $4, $5, $6, 0, 0, false)
             on conflict(map_command_id) do nothing"
        ),
        &[
            &map_command_key(&map_task.map_command_id),
            &namespace,
            &map_task.map_command_id.run_id.0,
            &i64::try_from(map_task.map_command_id.seq.0).unwrap_or(i64::MAX),
            &task_blob,
            &i64::try_from(manifest.item_count).unwrap_or(i64::MAX),
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn materialize_activity_map_items_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
) -> Result<()> {
    let key = map_command_key(map_command_id);
    let Some(row) = tx
        .query_opt(
            &format!(
                "select namespace, task, item_count, next_ordinal, in_flight, completed
                 from {schema}.activity_maps
                 where map_command_id = $1
                 for update"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(());
    };
    let namespace: String = row.get(0);
    let task_blob: Vec<u8> = row.get(1);
    let item_count = u64::try_from(row.get::<_, i64>(2)).unwrap_or(u64::MAX);
    let mut next_ordinal = u64::try_from(row.get::<_, i64>(3)).unwrap_or(u64::MAX);
    let mut in_flight = u64::try_from(row.get::<_, i64>(4)).unwrap_or(u64::MAX);
    let completed: bool = row.get(5);
    if completed {
        return Ok(());
    }

    let task: ActivityMapTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let max_in_flight = u64::try_from(task.max_in_flight.max(1)).unwrap_or(u64::MAX);
    let manifest_payload = backend
        .hydrate_activity_map_input_manifest_from_storage_tx(tx, task.input_manifest.clone())
        .await?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;

    while in_flight < max_in_flight && next_ordinal < item_count {
        let input = activity_map_input_at(&manifest, next_ordinal)?;
        let activity_id = ActivityId::map_item(map_command_id, next_ordinal);
        let item_task = ActivityTask {
            activity_id: activity_id.clone(),
            run_id: map_command_id.run_id.clone(),
            command_id: map_command_id.clone(),
            activity_name: task.activity_name.clone(),
            task_queue: task.task_queue.clone(),
            retry_policy: task.retry_policy.clone(),
            start_to_close_timeout: task.start_to_close_timeout,
            heartbeat_timeout: task.heartbeat_timeout,
            attempt: 1,
            input,
            map_item: Some(ActivityMapItem {
                map_command_id: map_command_id.clone(),
                item_ordinal: next_ordinal,
            }),
        };
        let item_task = backend
            .normalize_activity_task_for_storage_tx(tx, item_task)
            .await?;
        let item_blob = rmp_serde::to_vec_named(&item_task)
            .map_err(|err| Error::PayloadEncode(err.to_string()))?;
        tx.execute(
            &format!(
                "insert into {schema}.activity_tasks
                 (activity_id, namespace, run_id, activity_name, task_queue, task,
                  claim_token, completed, timeout_at_ms, heartbeat_deadline_at_ms)
                 values ($1, $2, $3, $4, $5, $6, null, false, $7, null)
                 on conflict(activity_id) do nothing"
            ),
            &[
                &activity_id.0,
                &namespace,
                &item_task.run_id.0,
                &item_task.activity_name.0,
                &item_task.task_queue.0,
                &item_blob,
                &activity_timeout_at_ms(item_task.start_to_close_timeout),
            ],
        )
        .await
        .map_err(postgres_error)?;
        next_ordinal = next_ordinal.saturating_add(1);
        in_flight = in_flight.saturating_add(1);
    }

    tx.execute(
        &format!(
            "update {schema}.activity_maps
             set next_ordinal = $1, in_flight = $2
             where map_command_id = $3"
        ),
        &[
            &i64::try_from(next_ordinal).unwrap_or(i64::MAX),
            &i64::try_from(in_flight).unwrap_or(i64::MAX),
            &key,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn complete_map_item_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    task: ActivityTask,
    map_item: ActivityMapItem,
    result: PayloadRef,
    activity_id: &ActivityId,
) -> Result<CompleteActivityOutcome> {
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 heartbeat_deadline_at_ms = null
             where activity_id = $1"
        ),
        &[&activity_id.0],
    )
    .await
    .map_err(postgres_error)?;

    let key = map_command_key(&map_item.map_command_id);
    let Some(row) = tx
        .query_opt(
            &format!(
                "select task, item_count, completed
                 from {schema}.activity_maps
                 where map_command_id = $1
                 for update"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::Backend(format!(
            "activity map `{}`:{} not found",
            map_item.map_command_id.run_id, map_item.map_command_id.seq.0
        )));
    };
    let task_blob: Vec<u8> = row.get(0);
    let item_count = u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX);
    let completed: bool = row.get(2);
    if completed {
        return Ok(CompleteActivityOutcome::AlreadyCompleted);
    }
    if map_item.item_ordinal >= item_count {
        return Err(Error::Backend(format!(
            "activity map item ordinal {} out of bounds",
            map_item.item_ordinal
        )));
    }

    let result_blob =
        rmp_serde::to_vec_named(&result).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    let inserted = tx
        .query_opt(
            &format!(
                "insert into {schema}.activity_map_results(map_command_id, item_ordinal, result)
                 values ($1, $2, $3)
                 on conflict(map_command_id, item_ordinal) do nothing
                 returning item_ordinal"
            ),
            &[
                &key,
                &i64::try_from(map_item.item_ordinal).unwrap_or(i64::MAX),
                &result_blob,
            ],
        )
        .await
        .map_err(postgres_error)?
        .is_some();
    if inserted {
        tx.execute(
            &format!(
                "update {schema}.activity_maps
                 set in_flight = case when in_flight > 0 then in_flight - 1 else 0 end
                 where map_command_id = $1"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?;
    }

    let success_count = tx
        .query_one(
            &format!(
                "select count(*)
                 from {schema}.activity_map_results
                 where map_command_id = $1"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?
        .get::<_, i64>(0);
    let success_count = u64::try_from(success_count).unwrap_or(u64::MAX);

    if success_count < item_count {
        materialize_activity_map_items_tx(backend, tx, schema, &map_item.map_command_id).await?;
        let tail = tx
            .query_opt(
                &format!(
                    "select current_event_id
                     from {schema}.workflow_instances
                     where run_id = $1"
                ),
                &[&task.run_id.0],
            )
            .await
            .map_err(postgres_error)?
            .map(|row| EventId(u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX)))
            .unwrap_or(EventId::ZERO);
        return Ok(CompleteActivityOutcome::Completed { event_id: tail });
    }

    let map_task: ActivityMapTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let input_manifest_payload = backend
        .hydrate_activity_map_input_manifest_from_storage_tx(tx, map_task.input_manifest.clone())
        .await?;
    let input_manifest: ActivityMapInputManifest = crate::decode_payload(&input_manifest_payload)?;
    let result_refs = activity_map_results_tx(tx, schema, &key).await?;
    let result_manifest = encode_activity_map_result_manifest_with_codec(
        map_task.result_manifest_name,
        result_refs,
        &input_manifest.page_lengths,
        backend.payload_config.codec,
    )?;
    let result_manifest = backend
        .normalize_activity_map_result_manifest_for_storage_tx(tx, result_manifest)
        .await?;
    let Some(row) = tx
        .query_opt(
            &format!(
                "select current_event_id, terminal
                 from {schema}.workflow_instances
                 where run_id = $1
                 for update"
            ),
            &[&task.run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(task.run_id));
    };
    let tail = EventId(u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX));
    let terminal: bool = row.get(1);
    if terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = tail.next();
    let item_count_usize = usize::try_from(item_count).unwrap_or(usize::MAX);
    let success_count_usize = usize::try_from(success_count).unwrap_or(usize::MAX);
    insert_history_event(
        tx,
        schema,
        &task.run_id,
        event_id,
        HistoryEventData::ActivityMapCompleted(crate::ActivityMapCompleted {
            command_id: map_item.map_command_id,
            result_manifest,
            item_count: item_count_usize,
            success_count: success_count_usize,
            failure_count: 0,
        }),
    )
    .await?;
    tx.execute(
        &format!(
            "update {schema}.activity_maps
             set completed = true, in_flight = 0
             where map_command_id = $1"
        ),
        &[&key],
    )
    .await
    .map_err(postgres_error)?;
    set_workflow_ready_tx(
        tx,
        schema,
        &task.run_id,
        event_id,
        WorkflowTaskReason::ActivityMapCompleted,
    )
    .await?;
    Ok(CompleteActivityOutcome::Completed { event_id })
}

async fn fail_map_item_tx(
    tx: &Transaction<'_>,
    schema: &str,
    task: ActivityTask,
    map_item: ActivityMapItem,
    failure: DurableFailure,
    activity_id: &ActivityId,
) -> Result<FailActivityOutcome> {
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 heartbeat_deadline_at_ms = null
             where activity_id = $1"
        ),
        &[&activity_id.0],
    )
    .await
    .map_err(postgres_error)?;

    let key = map_command_key(&map_item.map_command_id);
    let Some(row) = tx
        .query_opt(
            &format!(
                "select completed
                 from {schema}.activity_maps
                 where map_command_id = $1
                 for update"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::Backend(format!(
            "activity map `{}`:{} not found",
            map_item.map_command_id.run_id, map_item.map_command_id.seq.0
        )));
    };
    let completed: bool = row.get(0);
    if completed {
        return Ok(FailActivityOutcome::AlreadyCompleted);
    }

    let Some(row) = tx
        .query_opt(
            &format!(
                "select current_event_id, terminal
                 from {schema}.workflow_instances
                 where run_id = $1
                 for update"
            ),
            &[&task.run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(task.run_id));
    };
    let tail = EventId(u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX));
    let terminal: bool = row.get(1);
    if terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = tail.next();
    insert_history_event(
        tx,
        schema,
        &task.run_id,
        event_id,
        HistoryEventData::ActivityMapFailed(crate::ActivityMapFailed {
            command_id: map_item.map_command_id.clone(),
            failure,
        }),
    )
    .await?;
    tx.execute(
        &format!(
            "update {schema}.activity_maps
             set completed = true, in_flight = 0
             where map_command_id = $1"
        ),
        &[&key],
    )
    .await
    .map_err(postgres_error)?;
    let map_prefix = format!(
        "{}:{}:map:%",
        map_item.map_command_id.run_id, map_item.map_command_id.seq.0
    );
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 claim_token = null,
                 heartbeat_deadline_at_ms = null
             where activity_id like $1"
        ),
        &[&map_prefix],
    )
    .await
    .map_err(postgres_error)?;
    set_workflow_ready_tx(
        tx,
        schema,
        &task.run_id,
        event_id,
        WorkflowTaskReason::ActivityMapFailed,
    )
    .await?;
    Ok(FailActivityOutcome::Failed { event_id })
}

async fn activity_map_results_tx(
    tx: &Transaction<'_>,
    schema: &str,
    map_command_key: &str,
) -> Result<Vec<PayloadRef>> {
    let rows = tx
        .query(
            &format!(
                "select result
                 from {schema}.activity_map_results
                 where map_command_id = $1
                 order by item_ordinal asc"
            ),
            &[&map_command_key],
        )
        .await
        .map_err(postgres_error)?;
    rows.into_iter()
        .map(|row| {
            let blob: Vec<u8> = row.get(0);
            rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))
        })
        .collect()
}

async fn fire_due_timers_tx(
    tx: &Transaction<'_>,
    schema: &str,
    req: FireDueTimersRequest,
) -> Result<usize> {
    let rows = tx
        .query(
            &format!(
                "select wait_id, run_id, command_seq
                 from {schema}.active_waits
                 where namespace = $1
                   and kind = $2
                   and ready_at_ms is not null
                   and ready_at_ms <= $3
                 order by ready_at_ms asc, wait_id asc
                 limit $4
                 for update skip locked"
            ),
            &[
                &req.namespace.0,
                &wait_kind_to_str(&WaitKind::Timer),
                &req.now.0,
                &i64::try_from(req.limit.max(1)).unwrap_or(i64::MAX),
            ],
        )
        .await
        .map_err(postgres_error)?;

    let due = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                RunId::new(row.get::<_, String>(1)),
                CommandSeq(u64::try_from(row.get::<_, i64>(2)).unwrap_or(u64::MAX)),
            )
        })
        .collect::<Vec<_>>();

    let mut fired = 0usize;
    for (wait_id, run_id, command_seq) in due {
        let Some(row) = tx
            .query_opt(
                &format!(
                    "select current_event_id, terminal
                     from {schema}.workflow_instances
                     where run_id = $1
                     for update"
                ),
                &[&run_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            tx.execute(
                &format!("delete from {schema}.active_waits where wait_id = $1"),
                &[&wait_id],
            )
            .await
            .map_err(postgres_error)?;
            continue;
        };
        let tail = EventId(u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX));
        let terminal: bool = row.get(1);
        if terminal {
            tx.execute(
                &format!("delete from {schema}.active_waits where wait_id = $1"),
                &[&wait_id],
            )
            .await
            .map_err(postgres_error)?;
            continue;
        }

        let event_id = tail.next();
        insert_history_event(
            tx,
            schema,
            &run_id,
            event_id,
            HistoryEventData::TimerFired(crate::TimerFired {
                command_id: CommandId {
                    run_id: run_id.clone(),
                    seq: command_seq,
                },
                fired_at: req.now,
            }),
        )
        .await?;
        tx.execute(
            &format!(
                "update {schema}.workflow_instances
                 set current_event_id = $1, ready_reason = $2, ready_at_ms = 0
                 where run_id = $3"
            ),
            &[
                &i64::try_from(event_id.0).unwrap_or(i64::MAX),
                &reason_to_str(&WorkflowTaskReason::TimerFired),
                &run_id.0,
            ],
        )
        .await
        .map_err(postgres_error)?;
        tx.execute(
            &format!("delete from {schema}.active_waits where wait_id = $1"),
            &[&wait_id],
        )
        .await
        .map_err(postgres_error)?;
        fired += 1;
    }
    Ok(fired)
}

async fn timeout_due_activities_tx(
    tx: &Transaction<'_>,
    schema: &str,
    req: TimeoutDueActivitiesRequest,
) -> Result<usize> {
    let rows = tx
        .query(
            &format!(
                "select activity_id
                 from {schema}.activity_tasks
                 where namespace = $1
                   and completed = false
                   and (
                     (timeout_at_ms is not null and timeout_at_ms <= $2)
                     or
                     (heartbeat_deadline_at_ms is not null and heartbeat_deadline_at_ms <= $2)
                   )
                 order by least(
                     coalesce(timeout_at_ms, 9223372036854775807),
                     coalesce(heartbeat_deadline_at_ms, 9223372036854775807)
                   ) asc,
                   activity_id asc
                 limit $3
                 for update skip locked"
            ),
            &[
                &req.namespace.0,
                &req.now.0,
                &i64::try_from(req.limit.max(1)).unwrap_or(i64::MAX),
            ],
        )
        .await
        .map_err(postgres_error)?;
    let activity_ids = rows
        .into_iter()
        .map(|row| ActivityId(row.get::<_, String>(0)))
        .collect::<Vec<_>>();

    let mut timed_out = 0usize;
    for activity_id in activity_ids {
        if timeout_activity_tx(tx, schema, activity_id, req.now).await? {
            timed_out += 1;
        }
    }
    Ok(timed_out)
}

async fn timeout_activity_tx(
    tx: &Transaction<'_>,
    schema: &str,
    activity_id: ActivityId,
    now: TimestampMs,
) -> Result<bool> {
    let Some(row) = tx
        .query_opt(
            &format!(
                "select task, completed, timeout_at_ms, heartbeat_deadline_at_ms
                 from {schema}.activity_tasks
                 where activity_id = $1
                 for update"
            ),
            &[&activity_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(false);
    };
    let task_blob: Vec<u8> = row.get(0);
    let completed: bool = row.get(1);
    let timeout_at_ms: Option<i64> = row.get(2);
    let heartbeat_deadline_at_ms: Option<i64> = row.get(3);
    let start_timeout_due = timeout_at_ms.is_some_and(|timeout_at_ms| timeout_at_ms <= now.0);
    let heartbeat_timeout_due = heartbeat_deadline_at_ms.is_some_and(|deadline| deadline <= now.0);
    if completed || !(start_timeout_due || heartbeat_timeout_due) {
        return Ok(false);
    }
    let timed_out_by_heartbeat = heartbeat_timeout_due && !start_timeout_due;

    let task: ActivityTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    if should_retry_activity(&task) {
        let mut retry_task = task.clone();
        retry_task.attempt = retry_task.attempt.saturating_add(1);
        let retry_blob = rmp_serde::to_vec_named(&retry_task)
            .map_err(|err| Error::PayloadEncode(err.to_string()))?;
        tx.execute(
            &format!(
                "update {schema}.activity_tasks
                 set task = $1,
                     claim_token = null,
                     timeout_at_ms = $2,
                     heartbeat_deadline_at_ms = null
                 where activity_id = $3"
            ),
            &[
                &retry_blob,
                &activity_timeout_at_ms_from(now, retry_task.start_to_close_timeout),
                &activity_id.0,
            ],
        )
        .await
        .map_err(postgres_error)?;
        return Ok(true);
    }

    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 heartbeat_deadline_at_ms = null
             where activity_id = $1"
        ),
        &[&activity_id.0],
    )
    .await
    .map_err(postgres_error)?;

    if let Some(map_item) = task.map_item.clone() {
        fail_map_item_tx(
            tx,
            schema,
            task.clone(),
            map_item,
            DurableFailure::new(
                "durust.activity_timed_out",
                timeout_message(&activity_id, task.attempt, timed_out_by_heartbeat),
            ),
            &activity_id,
        )
        .await?;
        return Ok(true);
    }

    let Some(run_row) = tx
        .query_opt(
            &format!(
                "select current_event_id, terminal
                 from {schema}.workflow_instances
                 where run_id = $1
                 for update"
            ),
            &[&task.run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(task.run_id));
    };
    let tail = EventId(u64::try_from(run_row.get::<_, i64>(0)).unwrap_or(u64::MAX));
    let terminal: bool = run_row.get(1);
    if terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = tail.next();
    insert_history_event(
        tx,
        schema,
        &task.run_id,
        event_id,
        HistoryEventData::ActivityTimedOut(crate::ActivityTimedOut {
            command_id: task.command_id,
            message: timeout_message(&activity_id, task.attempt, timed_out_by_heartbeat),
        }),
    )
    .await?;
    tx.execute(
        &format!(
            "update {schema}.workflow_instances
             set current_event_id = $1, ready_reason = $2, ready_at_ms = 0
             where run_id = $3"
        ),
        &[
            &i64::try_from(event_id.0).unwrap_or(i64::MAX),
            &reason_to_str(&WorkflowTaskReason::ActivityTimedOut),
            &task.run_id.0,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(true)
}

async fn index_workflow_change_marker(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    event_id: EventId,
    data: &HistoryEventData,
) -> Result<()> {
    let Some(marker) = workflow_change_marker_fields(data) else {
        return Ok(());
    };
    let Some(row) = tx
        .query_opt(
            &format!(
                "select namespace, workflow_id, workflow_name, workflow_version
                 from {schema}.workflow_instances
                 where run_id = $1"
            ),
            &[&run_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Err(Error::RunNotFound(run_id.clone()));
    };
    let namespace: String = row.get(0);
    let workflow_id: String = row.get(1);
    let workflow_name: String = row.get(2);
    let workflow_version: i32 = row.get(3);
    let context = WorkflowChangeMarkerContext {
        namespace: &namespace,
        workflow_id: &workflow_id,
        workflow_name: &workflow_name,
        workflow_version,
    };
    index_workflow_change_marker_record(tx, schema, run_id, event_id, marker, &context).await
}

struct WorkflowChangeMarkerContext<'a> {
    namespace: &'a str,
    workflow_id: &'a str,
    workflow_name: &'a str,
    workflow_version: i32,
}

struct WorkflowChangeMarkerFields {
    change_id: String,
    version: i32,
    marker_kind: WorkflowChangeMarkerKind,
    command_seq: CommandSeq,
}

fn workflow_change_marker_fields(data: &HistoryEventData) -> Option<WorkflowChangeMarkerFields> {
    match data {
        HistoryEventData::VersionMarker(marker) => Some(WorkflowChangeMarkerFields {
            change_id: marker.change_id.clone(),
            version: marker.version,
            marker_kind: WorkflowChangeMarkerKind::Version,
            command_seq: marker.command_id.seq,
        }),
        HistoryEventData::DeprecatedPatchMarker(marker) => Some(WorkflowChangeMarkerFields {
            change_id: marker.patch_id.clone(),
            version: 1,
            marker_kind: WorkflowChangeMarkerKind::DeprecatedPatch,
            command_seq: marker.command_id.seq,
        }),
        _ => None,
    }
}

async fn index_workflow_change_marker_with_context(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    event_id: EventId,
    data: &HistoryEventData,
    context: &WorkflowChangeMarkerContext<'_>,
) -> Result<()> {
    let Some(marker) = workflow_change_marker_fields(data) else {
        return Ok(());
    };
    index_workflow_change_marker_record(tx, schema, run_id, event_id, marker, context).await
}

async fn index_workflow_change_marker_record(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    event_id: EventId,
    marker: WorkflowChangeMarkerFields,
    context: &WorkflowChangeMarkerContext<'_>,
) -> Result<()> {
    let marker_kind = marker_kind_to_str(marker.marker_kind);
    let command_seq = i64::try_from(marker.command_seq.0).unwrap_or(i64::MAX);
    let first_event_id = i64::try_from(event_id.0).unwrap_or(i64::MAX);
    let last_seen_at_ms = unix_epoch_millis();
    tx.execute(
        &format!(
            "insert into {schema}.workflow_change_versions
             (namespace, workflow_id, workflow_name, workflow_version, run_id, change_id,
              version, marker_kind, command_seq, first_event_id, last_seen_at_ms)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             on conflict(run_id, change_id) do update set
                version = excluded.version,
                marker_kind = excluded.marker_kind,
                command_seq = excluded.command_seq,
                first_event_id = excluded.first_event_id,
                last_seen_at_ms = excluded.last_seen_at_ms"
        ),
        &[
            &context.namespace,
            &context.workflow_id,
            &context.workflow_name,
            &context.workflow_version,
            &run_id.0,
            &marker.change_id,
            &marker.version,
            &marker_kind,
            &command_seq,
            &first_event_id,
            &last_seen_at_ms,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

fn is_opaque_external_payload_ref(payload: &PayloadRef) -> bool {
    matches!(payload, PayloadRef::Blob { uri, .. } if uri.starts_with("memory-blob://payload/") || uri.starts_with("s3://"))
}

fn collect_failure_payload_roots(failure: &DurableFailure, roots: &mut Vec<PayloadRootRef>) {
    if let Some(details) = &failure.details {
        roots.push(PayloadRootRef::Payload(details.clone()));
    }
}

fn decode_payload_blob_row(
    _payload: &PayloadRef,
    row_codec: String,
    row_schema_fingerprint: String,
    row_compression: String,
    encryption_blob: Option<Vec<u8>>,
    stored_size: i64,
    bytes: Vec<u8>,
    ref_codec: crate::CodecId,
    ref_schema_fingerprint: &crate::SchemaFingerprint,
    ref_compression: crate::CompressionId,
    ref_encryption: &Option<crate::EncryptionMetadata>,
    digest: &str,
    size: u64,
) -> Result<PayloadBlob> {
    let actual_digest = digest_bytes(&bytes);
    if actual_digest != digest {
        return Err(Error::PayloadDecode(format!(
            "payload blob digest mismatch: expected `{digest}`, got `{actual_digest}`"
        )));
    }
    let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual_size != size || u64::try_from(stored_size).unwrap_or(u64::MAX) != size {
        return Err(Error::PayloadDecode(format!(
            "payload blob size mismatch: expected {size}, got {actual_size}"
        )));
    }
    let blob = PayloadBlob {
        codec: codec_from_str(&row_codec)?,
        schema_fingerprint: crate::SchemaFingerprint(row_schema_fingerprint),
        compression: compression_from_str(&row_compression)?,
        encryption: decode_encryption_metadata(encryption_blob)?,
        bytes,
    };
    if blob.codec != ref_codec
        || blob.schema_fingerprint != *ref_schema_fingerprint
        || blob.compression != ref_compression
        || blob.encryption != *ref_encryption
    {
        return Err(Error::PayloadDecode(format!(
            "payload blob metadata mismatch for `{digest}`"
        )));
    }
    Ok(blob)
}

fn encode_encryption_metadata(
    encryption: &Option<crate::EncryptionMetadata>,
) -> Result<Option<Vec<u8>>> {
    encryption
        .as_ref()
        .map(|metadata| {
            rmp_serde::to_vec_named(metadata).map_err(|err| Error::PayloadEncode(err.to_string()))
        })
        .transpose()
}

fn decode_encryption_metadata(blob: Option<Vec<u8>>) -> Result<Option<crate::EncryptionMetadata>> {
    blob.map(|blob| {
        rmp_serde::from_slice(&blob).map_err(|err| Error::PayloadDecode(err.to_string()))
    })
    .transpose()
}

fn codec_to_str(codec: crate::CodecId) -> &'static str {
    match codec {
        crate::CodecId::MessagePack => "messagepack",
        crate::CodecId::Json => "json",
        crate::CodecId::Protobuf => "protobuf",
    }
}

fn codec_from_str(value: &str) -> Result<crate::CodecId> {
    match value {
        "messagepack" => Ok(crate::CodecId::MessagePack),
        "json" => Ok(crate::CodecId::Json),
        "protobuf" => Ok(crate::CodecId::Protobuf),
        other => Err(Error::PayloadDecode(format!(
            "unknown payload codec `{other}`"
        ))),
    }
}

fn compression_to_str(compression: crate::CompressionId) -> &'static str {
    match compression {
        crate::CompressionId::None => "none",
    }
}

fn compression_from_str(value: &str) -> Result<crate::CompressionId> {
    match value {
        "none" => Ok(crate::CompressionId::None),
        other => Err(Error::PayloadDecode(format!(
            "unknown payload compression `{other}`"
        ))),
    }
}

fn reason_to_str(reason: &WorkflowTaskReason) -> &'static str {
    match reason {
        WorkflowTaskReason::WorkflowStarted => "workflow_started",
        WorkflowTaskReason::ActivityCompleted => "activity_completed",
        WorkflowTaskReason::ActivityFailed => "activity_failed",
        WorkflowTaskReason::ActivityTimedOut => "activity_timed_out",
        WorkflowTaskReason::ActivityMapCompleted => "activity_map_completed",
        WorkflowTaskReason::ActivityMapFailed => "activity_map_failed",
        WorkflowTaskReason::ChildWorkflowStarted => "child_workflow_started",
        WorkflowTaskReason::ChildWorkflowCompleted => "child_workflow_completed",
        WorkflowTaskReason::ChildWorkflowFailed => "child_workflow_failed",
        WorkflowTaskReason::ChildWorkflowCancelled => "child_workflow_cancelled",
        WorkflowTaskReason::TimerFired => "timer_fired",
        WorkflowTaskReason::SignalReceived => "signal_received",
        WorkflowTaskReason::CacheEvicted => "cache_evicted",
    }
}

fn reason_from_str(value: &str) -> Result<WorkflowTaskReason> {
    match value {
        "workflow_started" => Ok(WorkflowTaskReason::WorkflowStarted),
        "activity_completed" => Ok(WorkflowTaskReason::ActivityCompleted),
        "activity_failed" => Ok(WorkflowTaskReason::ActivityFailed),
        "activity_timed_out" => Ok(WorkflowTaskReason::ActivityTimedOut),
        "activity_map_completed" => Ok(WorkflowTaskReason::ActivityMapCompleted),
        "activity_map_failed" => Ok(WorkflowTaskReason::ActivityMapFailed),
        "child_workflow_started" => Ok(WorkflowTaskReason::ChildWorkflowStarted),
        "child_workflow_completed" => Ok(WorkflowTaskReason::ChildWorkflowCompleted),
        "child_workflow_failed" => Ok(WorkflowTaskReason::ChildWorkflowFailed),
        "child_workflow_cancelled" => Ok(WorkflowTaskReason::ChildWorkflowCancelled),
        "timer_fired" => Ok(WorkflowTaskReason::TimerFired),
        "signal_received" => Ok(WorkflowTaskReason::SignalReceived),
        "cache_evicted" => Ok(WorkflowTaskReason::CacheEvicted),
        other => Err(Error::Backend(format!(
            "unknown workflow task reason `{other}`"
        ))),
    }
}

fn parent_close_policy_to_str(policy: ParentClosePolicy) -> &'static str {
    match policy {
        ParentClosePolicy::Cancel => "cancel",
        ParentClosePolicy::Abandon => "abandon",
    }
}

fn map_command_key(command_id: &CommandId) -> String {
    format!("{}:{}", command_id.run_id, command_id.seq.0)
}

fn marker_kind_to_str(kind: WorkflowChangeMarkerKind) -> &'static str {
    match kind {
        WorkflowChangeMarkerKind::Version => "version",
        WorkflowChangeMarkerKind::DeprecatedPatch => "deprecated_patch",
    }
}

fn marker_kind_from_str(value: &str) -> Result<WorkflowChangeMarkerKind> {
    match value {
        "version" => Ok(WorkflowChangeMarkerKind::Version),
        "deprecated_patch" => Ok(WorkflowChangeMarkerKind::DeprecatedPatch),
        other => Err(Error::Backend(format!(
            "unknown workflow change marker kind `{other}`"
        ))),
    }
}

fn event_type_to_str(event_type: &HistoryEventType) -> &'static str {
    match event_type {
        HistoryEventType::WorkflowStarted => "workflow_started",
        HistoryEventType::WorkflowCompleted => "workflow_completed",
        HistoryEventType::WorkflowFailed => "workflow_failed",
        HistoryEventType::WorkflowCancelled => "workflow_cancelled",
        HistoryEventType::WorkflowContinuedAsNew => "workflow_continued_as_new",
        HistoryEventType::WorkflowTaskStarted => "workflow_task_started",
        HistoryEventType::ActivityScheduled => "activity_scheduled",
        HistoryEventType::ActivityMapScheduled => "activity_map_scheduled",
        HistoryEventType::ActivityMapCompleted => "activity_map_completed",
        HistoryEventType::ActivityMapFailed => "activity_map_failed",
        HistoryEventType::ActivityCompleted => "activity_completed",
        HistoryEventType::ActivityFailed => "activity_failed",
        HistoryEventType::ActivityTimedOut => "activity_timed_out",
        HistoryEventType::ChildWorkflowStartRequested => "child_workflow_start_requested",
        HistoryEventType::ChildWorkflowStarted => "child_workflow_started",
        HistoryEventType::ChildWorkflowCompleted => "child_workflow_completed",
        HistoryEventType::ChildWorkflowFailed => "child_workflow_failed",
        HistoryEventType::ChildWorkflowCancelled => "child_workflow_cancelled",
        HistoryEventType::TimerStarted => "timer_started",
        HistoryEventType::TimerFired => "timer_fired",
        HistoryEventType::SignalConsumed => "signal_consumed",
        HistoryEventType::SelectWinner => "select_winner",
        HistoryEventType::VersionMarker => "version_marker",
        HistoryEventType::DeprecatedPatchMarker => "deprecated_patch_marker",
        HistoryEventType::SideEffectMarker => "side_effect_marker",
    }
}

fn event_type_from_str(value: &str) -> Result<HistoryEventType> {
    match value {
        "workflow_started" => Ok(HistoryEventType::WorkflowStarted),
        "workflow_completed" => Ok(HistoryEventType::WorkflowCompleted),
        "workflow_failed" => Ok(HistoryEventType::WorkflowFailed),
        "workflow_cancelled" => Ok(HistoryEventType::WorkflowCancelled),
        "workflow_continued_as_new" => Ok(HistoryEventType::WorkflowContinuedAsNew),
        "workflow_task_started" => Ok(HistoryEventType::WorkflowTaskStarted),
        "activity_scheduled" => Ok(HistoryEventType::ActivityScheduled),
        "activity_map_scheduled" => Ok(HistoryEventType::ActivityMapScheduled),
        "activity_map_completed" => Ok(HistoryEventType::ActivityMapCompleted),
        "activity_map_failed" => Ok(HistoryEventType::ActivityMapFailed),
        "activity_completed" => Ok(HistoryEventType::ActivityCompleted),
        "activity_failed" => Ok(HistoryEventType::ActivityFailed),
        "activity_timed_out" => Ok(HistoryEventType::ActivityTimedOut),
        "child_workflow_start_requested" => Ok(HistoryEventType::ChildWorkflowStartRequested),
        "child_workflow_started" => Ok(HistoryEventType::ChildWorkflowStarted),
        "child_workflow_completed" => Ok(HistoryEventType::ChildWorkflowCompleted),
        "child_workflow_failed" => Ok(HistoryEventType::ChildWorkflowFailed),
        "child_workflow_cancelled" => Ok(HistoryEventType::ChildWorkflowCancelled),
        "timer_started" => Ok(HistoryEventType::TimerStarted),
        "timer_fired" => Ok(HistoryEventType::TimerFired),
        "signal_consumed" => Ok(HistoryEventType::SignalConsumed),
        "select_winner" => Ok(HistoryEventType::SelectWinner),
        "version_marker" => Ok(HistoryEventType::VersionMarker),
        "deprecated_patch_marker" => Ok(HistoryEventType::DeprecatedPatchMarker),
        "side_effect_marker" => Ok(HistoryEventType::SideEffectMarker),
        other => Err(Error::Backend(format!("unknown event type `{other}`"))),
    }
}

#[cfg(test)]
mod tests;
