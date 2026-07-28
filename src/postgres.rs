use crate::map_engine::{
    ACTIVITY_MAP_LABEL, CHILD_WORKFLOW_MAP_LABEL, ItemAttemptFailureKind, ItemRetryDecision,
    MapEffect, MapEvent, MapKind, MapReject, MapState, activity_outcome_counts,
    map_command_cancelled_reason, outcome_counts, validate_map_slot_bound,
};
use crate::provider_util::{
    ActivityFailureDecision, TerminalCleanup, activity_claim_implicit_heartbeat_ms,
    activity_failure_decision, activity_heartbeat_deadline_at_ms, activity_timeout_at_ms,
    activity_timeout_at_ms_from, activity_timeout_attribution, activity_timeout_decision,
    child_terminal_event_data_and_reason, child_terminal_map_item_outcome, claim_lease_until_ms,
    codec_from_str, codec_to_str, commit_has_workflow_visible_mutations, compression_from_str,
    compression_to_str, decode_encryption_metadata, duration_millis_i64,
    encode_encryption_metadata, event_type_from_str, event_type_to_str, marker_kind_from_str,
    marker_kind_to_str, parent_close_policy_to_str, payload_gc_cutoff_ms, post_commit_ready_reason,
    ready_at_ms_for_delay, reason_from_str, reason_to_str, retry_visible_at_ms, timeout_message,
    unix_epoch_millis, wait_kind_to_str,
};
use crate::{
    ActivityFailed, ActivityHeartbeatOutcome, ActivityHeartbeatRequest, ActivityId,
    ActivityMapInputManifest, ActivityMapInputPage, ActivityMapItem, ActivityMapResultManifest,
    ActivityMapResultPage, ActivityMapTask, ActivityTask, ActivityTaskClaim, CancelWorkflowOutcome,
    CancelWorkflowRequest, ChildStartOutboxMessage, ChildWorkflowMapFailureMode,
    ChildWorkflowMapItem, ChildWorkflowMapItemOutcome, ChildWorkflowMapTask, ClaimActivityOptions,
    ClaimActivityTasksOptions, ClaimWorkflowTaskOptions, ClaimWorkflowTasksOptions,
    ClaimedActivityTask, ClaimedWorkflowTask, CommandId, CommandSeq, CommitOutcome,
    CompleteActivityOutcome, CompleteActivityRequest, CompleteActivityTaskBatchResult,
    CompleteActivityTasksRequest, DispatchChildWorkflowStartsOutcome,
    DispatchChildWorkflowStartsRequest, DurableBackend, DurableFailure, Error, EventId,
    FailActivityOutcome, FailActivityRequest, FireDueTimersOutcome, FireDueTimersRequest,
    HistoryChunk, HistoryEvent, HistoryEventData, HistoryEventType, Namespace, ParentClosePolicy,
    PayloadBlob, PayloadGarbageCollectionOutcome, PayloadGarbageCollectionRequest, PayloadRef,
    PayloadRootRef, PayloadRootsOutcome, PayloadStorageConfig, QueryProjectionOutcome,
    QueryProjectionRequest, ReadSignalInboxRequest, ReadSignalInboxesRequest, Result,
    RunDueMaintenanceOutcome, RunDueMaintenanceRequest, RunId, ShardId, SignalInboxRecord,
    SignalWorkflowOutcome, SignalWorkflowRequest, StartWorkflowOutcome, StartWorkflowRequest,
    TimeoutDueActivitiesOutcome, TimeoutDueActivitiesRequest, TimestampMs, WaitKind, WorkerId,
    WorkflowChangeMarkerKind, WorkflowChangeVersionRecord, WorkflowChangeVersionStatus,
    WorkflowChangeVersionsOutcome, WorkflowChangeVersionsRequest, WorkflowId, WorkflowTaskClaim,
    WorkflowTaskCommit, WorkflowTaskReason, WorkflowType, activity_map_input_at, digest_bytes,
    encode_activity_map_result_manifest_with_codec,
    encode_child_workflow_map_result_manifest_with_codec, event_payload_len, is_terminal,
};
use deadpool_postgres::{
    Manager, ManagerConfig, Object as PooledPostgresClient, Pool, RecyclingMethod, Runtime,
};
use futures::future::{BoxFuture, ready};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;
use tokio_postgres::{NoTls, Transaction};

/// One descriptor's worth of `PostgresBackend::repair_stalled_empty_maps`. Every
/// failure it can produce is the caller's to swallow, which is why it is a
/// separate function rather than an inline block.
async fn repair_one_stalled_empty_map_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    kind: MapKind,
    map_command_id: &CommandId,
) -> Result<()> {
    // Re-read under the write transaction, which locks the descriptor row
    // `for update`: another process may have repaired the same one between the
    // probe and here, and the engine's terminal-absorbing rule then makes this
    // a no-op.
    let stepped = match kind {
        MapKind::Activity => activity_map_state_tx(tx, schema, map_command_id)
            .await?
            .map(|(state, namespace, task)| (state, namespace, MapTask::Activity(task))),
        MapKind::ChildWorkflow => child_workflow_map_state_tx(tx, schema, map_command_id)
            .await?
            .map(|(state, namespace, task)| (state, namespace, MapTask::ChildWorkflow(task))),
    };
    let Some((state, namespace, task)) = stepped else {
        return Ok(());
    };
    // A descriptor whose run row is gone is skipped, not raised; see
    // `SqliteBackend::repair_stalled_empty_maps`.
    let Some((_, parent_terminal)) =
        parent_tail_and_terminal_tx(tx, schema, &map_command_id.run_id).await?
    else {
        return Ok(());
    };
    step_map_tx(
        backend,
        tx,
        schema,
        &state,
        &namespace,
        &task,
        MapEvent::DescriptorCreated { parent_terminal },
    )
    .await?;
    Ok(())
}

/// `meta` key recording that the empty-map upgrade repair has already run
/// against this database. Both SQL providers use the same key so the two
/// repairs stay recognisably one mechanism.
const EMPTY_MAP_REPAIR_MARKER: &str = "empty_map_repair_done";

/// `meta` key counting descriptors the empty-map upgrade repair could not act
/// on. The pass records itself either way, so it will not revisit them; the
/// count is where an operator finds out that it did not.
const EMPTY_MAP_REPAIR_SKIPPED: &str = "empty_map_repair_skipped";

const POSTGRES_SCHEMA_VERSION: i64 = 6;
const DEFAULT_SCHEMA: &str = "durust";
const DEFAULT_MAX_POOL_SIZE: usize = 16;
const DEFAULT_LOGICAL_SHARDS: u32 = 1;
const DEFAULT_PHYSICAL_PARTITIONS: u32 = 1;
const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const POSTGRES_TRANSACTION_RETRY_ATTEMPTS: usize = 3;
const POSTGRES_TRANSACTION_RETRY_BASE_DELAY: Duration = Duration::from_millis(2);
const CLAIM_HISTORY_PREFETCH_EVENTS: usize = 16;
const CLAIM_HISTORY_PREFETCH_BYTES: usize = 256 * 1024;

struct LockedWorkflowCommitRow {
    current_tail: EventId,
    claim_token: Option<i64>,
    terminal: bool,
    namespace: String,
    workflow_id: String,
    shard_id: i32,
}

struct PreparedSimpleWorkflowCommit {
    input_index: usize,
    claim: WorkflowTaskClaim,
    next_event_id: EventId,
    namespace: String,
    workflow_id: String,
    append_history: Vec<(EventId, HistoryEventData)>,
    schedule_activities: Vec<ActivityTask>,
    start_child_workflows: Vec<ChildStartOutboxMessage>,
    upsert_waits: Vec<crate::WaitRecord>,
    consume_signals: Vec<crate::SignalId>,
    delete_waits: Vec<crate::WaitId>,
    query_projection: Option<PayloadRef>,
    terminal_event: Option<HistoryEventData>,
    ready_reason: Option<WorkflowTaskReason>,
}

struct PreparedSimpleChildStart {
    commit_index: usize,
    message: ChildStartOutboxMessage,
    proposed_run_id: RunId,
    child_shard_id: i32,
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
            statement_timeout: config.statement_timeout,
            lock_timeout: config.lock_timeout,
        };
        let empty_map_repair_done = backend.migrate().await?;
        if !empty_map_repair_done {
            backend.repair_stalled_empty_maps().await?;
        }
        Ok(backend)
    }

    /// Upgrade repair: complete maps that a database written before empty input
    /// manifests completed at descriptor creation left permanently stalled.
    /// See `SqliteBackend::repair_stalled_empty_maps` for the full rationale,
    /// including why every per-descriptor failure is skipped rather than raised
    /// and why the skips are counted into `meta` instead of discarded.
    ///
    /// One-shot: `migrate()` reads the `meta` marker in the query it already
    /// issues for `schema_version`, so a database that has been repaired pays
    /// **no** round trip here — this function is not called at all. That
    /// matters because the probe's predicate has no index behind it and the
    /// tables it scans are sized by the number of concurrently open maps.
    ///
    /// The probe is ordered, and that is load-bearing rather than tidiness: the
    /// loop takes `for update` on each descriptor in probe order, and
    /// `synchronize_seqscans` (on by default) deliberately starts concurrent
    /// sequential scans at different positions once a table passes roughly a
    /// quarter of `shared_buffers`. Two processes connecting for the first time
    /// could otherwise lock in opposite orders, and one would take
    /// `deadlock detected` — on the connect path, where it fails construction.
    /// SQLite is immune to this because `BEGIN IMMEDIATE` serialises writers.
    ///
    /// This runs straight after `migrate()` inside `connect_with_config`, not
    /// inside a readiness chain, so it cannot await the future it is
    /// resolving — the deadlock the equivalent TypeScript repair had to fix.
    async fn repair_stalled_empty_maps(&self) -> Result<()> {
        let schema = self.schema_sql();
        // One statement, not one per table: this sits on the connect path, and
        // every round trip here lengthens the window a caller sees before the
        // backend is usable.
        let rows = {
            let client = self.client().await?;
            client
                .query(
                    &format!(
                        "select false as is_child, run_id, command_seq from {schema}.activity_maps
                          where item_count = 0 and completed = false
                         union all
                         select true, run_id, command_seq from {schema}.child_workflow_maps
                          where item_count = 0 and completed = false
                         order by 1, 2, 3"
                    ),
                    &[],
                )
                .await
                .map_err(postgres_error)?
        };
        let stalled = rows
            .into_iter()
            .map(|row| {
                let kind = if row.get::<_, bool>(0) {
                    MapKind::ChildWorkflow
                } else {
                    MapKind::Activity
                };
                (
                    kind,
                    CommandId {
                        run_id: RunId::new(row.get::<_, String>(1)),
                        seq: CommandSeq(u64::try_from(row.get::<_, i64>(2)).unwrap_or(u64::MAX)),
                    },
                )
            })
            .collect::<Vec<_>>();

        let mut client = self.client().await?;
        let mut skipped = 0_u64;
        for (kind, map_command_id) in stalled {
            // One transaction per descriptor, so a skip is a rollback of that
            // descriptor alone; see the SQLite half.
            let tx = client.transaction().await.map_err(postgres_error)?;
            let repaired =
                match repair_one_stalled_empty_map_tx(self, &tx, &schema, kind, &map_command_id)
                    .await
                {
                    Ok(()) => tx.commit().await.map_err(postgres_error),
                    Err(err) => {
                        // Rolled back explicitly rather than by drop, so the
                        // rollback's own failure is observed here too.
                        let _ = tx.rollback().await;
                        Err(err)
                    }
                };
            if repaired.is_err() {
                skipped += 1;
            }
        }

        let tx = client.transaction().await.map_err(postgres_error)?;
        tx.execute(
            &format!(
                "insert into {schema}.meta(key, value) values ('{EMPTY_MAP_REPAIR_MARKER}', 1)
                 on conflict(key) do update set value = excluded.value"
            ),
            &[],
        )
        .await
        .map_err(postgres_error)?;
        if skipped > 0 {
            tx.execute(
                &format!(
                    "insert into {schema}.meta(key, value)
                     values ('{EMPTY_MAP_REPAIR_SKIPPED}', $1)
                     on conflict(key) do update set value = excluded.value"
                ),
                &[&i64::try_from(skipped).unwrap_or(i64::MAX)],
            )
            .await
            .map_err(postgres_error)?;
        }
        tx.commit().await.map_err(postgres_error)
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

    /// Runs the schema migration and reports whether the empty-map upgrade
    /// repair still has to run against this database.
    async fn migrate(&self) -> Result<bool> {
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

        // One statement for both keys: `schema_version` gates the migration and
        // `empty_map_repair_done` gates the upgrade repair, so a steady-state
        // connect pays no round trip for the repair at all.
        let meta = client
            .query(
                &format!(
                    "select key, value from {schema}.meta
                     where key in ('schema_version', '{EMPTY_MAP_REPAIR_MARKER}')"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        let meta_value = |wanted: &str| {
            meta.iter()
                .find(|row| row.get::<_, String>(0) == wanted)
                .map(|row| row.get::<_, i64>(1))
        };
        let empty_map_repair_done = meta_value(EMPTY_MAP_REPAIR_MARKER).unwrap_or(0) > 0;
        let existing = meta_value("schema_version");
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
                    claim_lease_until_ms bigint,
                    terminal boolean not null,
                    parent_run_id text,
                    parent_command_seq bigint,
                    parent_close_policy text,
                    parent_child_map_ordinal bigint,
                    unique(namespace, workflow_id)
                );

                alter table {schema}.workflow_instances
                    add column if not exists shard_id integer not null default 0;

                alter table {schema}.workflow_instances
                    add column if not exists parent_child_map_ordinal bigint;

                alter table {schema}.workflow_instances
                    add column if not exists claim_lease_until_ms bigint;

                create table if not exists {schema}.history_events (
                    run_id text not null,
                    event_id bigint not null,
                    event_type text not null,
                    command_seq bigint,
                    data bytea not null,
                    primary key(run_id, event_id)
                );

                create index if not exists idx_history_events_command_seq
                    on {schema}.history_events(run_id, command_seq)
                    where command_seq is not null;

                create table if not exists {schema}.payload_blobs (
                    digest text primary key,
                    codec text not null,
                    schema_fingerprint text not null,
                    compression text not null,
                    encryption bytea,
                    size bigint not null,
                    bytes bytea,
                    created_at_ms bigint not null default 0
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
                    heartbeat_deadline_at_ms bigint,
                    implicit_heartbeat_ms bigint,
                    visible_at_ms bigint
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

                create table if not exists {schema}.child_workflow_maps (
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

                create table if not exists {schema}.child_workflow_map_results (
                    map_command_id text not null,
                    item_ordinal bigint not null,
                    outcome bytea not null,
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
                      and terminal = false;

                create index if not exists idx_workflow_instances_ready_shard
                    on {schema}.workflow_instances(namespace, shard_id, task_queue, ready_at_ms, run_id)
                    where ready_reason is not null
                      and terminal = false;

                -- Serves `cancel_child_workflow_map_children_tx`, which finds a
                -- map's children by parent link. Without it that query is a
                -- sequential scan of every workflow instance, and it runs on
                -- two hot paths: cancelling a command and applying a fail-fast
                -- map effect. Measured on a table shaped like this one, 20
                -- matching children throughout: 0.246 ms at 1k rows, 1.210 ms
                -- at 10k, 8.637 ms at 100k, with `Rows Removed by Filter`
                -- tracking the row count. With this index, 0.140 ms at 1k and
                -- 0.193 ms at 100k — the growth is gone.
                --
                -- The predicate and the key deliberately name only columns that
                -- never change after insert. `parent_run_id` and
                -- `parent_command_seq` are written once by `start_workflow` and
                -- updated by none of the 20 `update workflow_instances`
                -- statements in this file, so an ordinary instance update —
                -- claim token, `current_event_id`, `ready_reason`, `terminal` —
                -- changes no indexed column and stays eligible for a HOT
                -- update, paying nothing for this index. Measured against the
                -- counterfactual: as shipped it costs 0 non-HOT updates, and
                -- adding `terminal = false` to the predicate — which looks
                -- tighter — costs all 3,023 of them, because the terminal
                -- transition would move the row out of the index.
                --
                -- The residual write cost is one 2-column btree insert per
                -- child workflow. That has **not** been measured, and the
                -- honest reason is that it is below what the harness can
                -- resolve: an interleaved A/B of the `mixed` profile at n=24,
                -- in which 400 of 800 inserted instances carried a parent so
                -- the index was genuinely populated, bounds the effect at
                -- [-1.14%, +2.03%] — while the expected effect is ~400 btree
                -- inserts across a ~570 ms run, a few tenths of a percent.
                -- Resolving that needs n on the order of 900. What the
                -- experiment establishes is a bound, not a null: no regression
                -- above ~2%.
                create index if not exists idx_workflow_instances_parent
                    on {schema}.workflow_instances(parent_run_id, parent_command_seq)
                    where parent_run_id is not null;

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

                create index if not exists idx_activity_tasks_claim
                    on {schema}.activity_tasks(namespace, task_queue, activity_id)
                    where completed = false
                      and claim_token is null;

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
        self.ensure_shard_leases(&client).await?;
        Ok(empty_map_repair_done)
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

    // Shard leases fence claims and commits today. Journal, snapshot, and
    // partitioned history storage arrive with shard-native recovery
    // (impl-plan item 0013); until something reads them, no such tables or
    // writes exist.
    async fn ensure_shard_leases(&self, client: &PooledPostgresClient) -> Result<()> {
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

    async fn verify_shard_leases_tx(
        &self,
        tx: &Transaction<'_>,
        lease_keys: &BTreeSet<(WorkerId, i32)>,
    ) -> Result<BTreeMap<(WorkerId, i32), i64>> {
        if lease_keys.is_empty() {
            return Ok(BTreeMap::new());
        }
        if self.logical_shards <= 1 {
            return Ok(lease_keys
                .iter()
                .map(|key| (key.clone(), 0))
                .collect::<BTreeMap<_, _>>());
        }
        let schema = self.schema_sql();
        let now_ms = unix_epoch_millis();
        let shard_ids = lease_keys
            .iter()
            .map(|(_, shard_id)| *shard_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let rows = tx
            .query(
                &format!(
                    "select shard_id, owner_id, lease_until_ms, lease_epoch
                     from {schema}.shard_leases
                     where shard_id = any($1::integer[])"
                ),
                &[&shard_ids],
            )
            .await
            .map_err(postgres_error)?;
        let rows_by_shard = rows
            .into_iter()
            .map(|row| {
                let shard_id: i32 = row.get(0);
                let owner_id: Option<String> = row.get(1);
                let lease_until_ms: Option<i64> = row.get(2);
                let lease_epoch: i64 = row.get(3);
                (shard_id, (owner_id, lease_until_ms, lease_epoch))
            })
            .collect::<BTreeMap<_, _>>();
        let mut leases = BTreeMap::new();
        for key @ (worker_id, shard_id) in lease_keys {
            let Some((owner_id, lease_until_ms, lease_epoch)) = rows_by_shard.get(shard_id) else {
                continue;
            };
            if owner_id.as_deref() == Some(worker_id.0.as_str())
                && lease_until_ms.is_some_and(|lease_until_ms| lease_until_ms > now_ms)
            {
                leases.insert(key.clone(), *lease_epoch);
            }
        }
        Ok(leases)
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

    fn hydrate_child_workflow_map_result_manifest(
        &self,
        payload: PayloadRef,
    ) -> BoxFuture<'static, Result<PayloadRef>> {
        let backend = self.clone();
        Box::pin(async move {
            backend
                .hydrate_child_workflow_map_result_manifest_from_storage(payload)
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

    fn read_signal_inboxes(
        &self,
        req: ReadSignalInboxesRequest,
    ) -> BoxFuture<'static, Result<Vec<Option<SignalInboxRecord>>>> {
        let backend = self.clone();
        Box::pin(async move { backend.read_signal_inboxes_inner(req).await })
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
    async fn retry_transaction<T, F, Fut>(&self, mut body: F) -> Result<T>
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
        self.retry_transaction(|| {
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
        self.retry_transaction(|| {
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
        cleanup_run_operational_state_tx(&tx, &schema, &run_id, TerminalCleanup::Closed).await?;
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
        handle_terminal_run_tx(self, &tx, &schema, &run_id, &terminal_event).await?;
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
        self.retry_transaction(|| {
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

        // A held claim blocks reclaiming only while its lease is unexpired; a
        // null lease keeps the row unclaimable, which fails safe.
        let rows = tx
            .query(
                &format!(
                    "select run_id, workflow_id, workflow_name, workflow_version, current_event_id, ready_reason, shard_id
                     from {schema}.workflow_instances
                     where namespace = $1
                       and task_queue = $2
                       and ready_reason is not null
                       and ready_at_ms <= $3
                       and (workflow_claim_token is null or claim_lease_until_ms <= $3)
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
        // The ready reason and visibility stay on the rows while claimed so a
        // reclaim after lease expiry hands out the same task a fresh claim
        // would; commit, conflict, and release overwrite them.
        tx.execute(
            &format!(
                "update {schema}.workflow_instances workflows
                 set workflow_claim_token = claimed.claim_token,
                     claim_lease_until_ms = $3
                 from unnest($1::text[], $2::bigint[]) as claimed(run_id, claim_token)
                 where workflows.run_id = claimed.run_id"
            ),
            &[
                &run_ids,
                &token_values,
                &claim_lease_until_ms(TimestampMs(now_ms), opts.claim.lease_duration),
            ],
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
        self.retry_transaction(|| {
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
                       and (workflow_claim_token is null or claim_lease_until_ms <= $3)
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
                 set workflow_claim_token = $1, claim_lease_until_ms = $2
                 where run_id = $3"
            ),
            &[
                &i64::try_from(token).unwrap_or(i64::MAX),
                &claim_lease_until_ms(TimestampMs(now_ms), opts.lease_duration),
                &run_id.0,
            ],
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
        let max_events = req.max_events.max(1);
        let max_bytes = req.max_bytes.max(1);
        // Fetch one row past the event budget so `has_more` is answered
        // without materializing the whole remaining history; byte-budget
        // truncation stays Rust-side because row sizes are not known in SQL.
        let row_limit = i64::try_from(max_events.saturating_add(1)).unwrap_or(i64::MAX);
        let rows = client
            .query(
                &format!(
                    "select event_id, event_type, data
                     from {schema}.history_events
                     where run_id = $1 and event_id > $2 and event_id <= $3
                     order by event_id asc
                     limit $4"
                ),
                &[
                    &req.run_id.0,
                    &i64::try_from(req.after_event_id.0).unwrap_or(i64::MAX),
                    &i64::try_from(req.up_to_event_id.0).unwrap_or(i64::MAX),
                    &row_limit,
                ],
            )
            .await
            .map_err(postgres_error)?;
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
        self.retry_transaction(|| {
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
        self.retry_transaction(|| {
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
        let outcome = self
            .apply_workflow_task_commit_tx(&tx, &schema, claim, batch, None)
            .await?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(outcome)
    }

    async fn commit_workflow_tasks_inner(
        &self,
        batch: crate::WorkflowTaskCommitBatch,
    ) -> Result<Vec<crate::WorkflowTaskCommitBatchResult>> {
        self.retry_transaction(|| {
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
        let commits = batch.commits;
        let mut results = (0..commits.len()).map(|_| None).collect::<Vec<_>>();
        let mut lease_epoch_cache = BTreeMap::<(WorkerId, i32), i64>::new();
        let run_counts = commits.iter().fold(BTreeMap::new(), |mut counts, input| {
            *counts.entry(input.claim.run_id.clone()).or_insert(0usize) += 1;
            counts
        });
        let terminal_dependency_run_ids = self
            .workflow_batch_terminal_dependency_run_ids_tx(&tx, &schema, &commits)
            .await?;
        let simple_indices = commits
            .iter()
            .enumerate()
            .filter_map(|(index, input)| {
                (run_counts.get(&input.claim.run_id) == Some(&1)
                    && !terminal_dependency_run_ids.contains(&input.claim.run_id)
                    && postgres_simple_batch_commit_eligible(&input.commit))
                .then_some(index)
            })
            .collect::<Vec<_>>();

        if simple_indices.len() > 1 {
            for (index, result) in self
                .apply_simple_workflow_task_commits_tx(&tx, &schema, &commits, &simple_indices)
                .await?
            {
                match result {
                    Ok(outcome) => {
                        results[index] = Some(Ok(outcome));
                    }
                    Err(err @ Error::Backend(_)) => return Err(err),
                    Err(err) => {
                        results[index] = Some(Err(err));
                    }
                }
            }
        }

        for (index, input) in commits.iter().enumerate() {
            if results[index].is_some() {
                continue;
            }
            let claim = input.claim.clone();
            match self
                .apply_workflow_task_commit_tx(
                    &tx,
                    &schema,
                    claim.clone(),
                    input.commit.clone(),
                    Some(&mut lease_epoch_cache),
                )
                .await
            {
                Ok(outcome) => {
                    results[index] = Some(Ok(outcome));
                }
                Err(err @ Error::Backend(_)) => return Err(err),
                Err(err) => {
                    results[index] = Some(Err(err));
                }
            }
        }

        let mut batch_results = Vec::with_capacity(commits.len());
        for (index, input) in commits.iter().enumerate() {
            batch_results.push(crate::WorkflowTaskCommitBatchResult {
                claim: input.claim.clone(),
                result: results[index].take().unwrap_or_else(|| {
                    Err(Error::Backend(format!(
                        "postgres workflow commit batch item {index} was not evaluated"
                    )))
                }),
            });
        }

        tx.commit().await.map_err(postgres_error)?;
        Ok(batch_results)
    }

    async fn workflow_batch_terminal_dependency_run_ids_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
        commits: &[crate::WorkflowTaskCommitInput],
    ) -> Result<BTreeSet<RunId>> {
        let terminal_run_ids = commits
            .iter()
            .filter(|input| postgres_simple_batch_commit_has_terminal_event(&input.commit))
            .map(|input| input.claim.run_id.clone())
            .collect::<BTreeSet<_>>();
        if terminal_run_ids.is_empty() {
            return Ok(BTreeSet::new());
        }
        let batch_run_ids = commits
            .iter()
            .map(|input| input.claim.run_id.clone())
            .collect::<BTreeSet<_>>();
        let batch_run_id_values = batch_run_ids
            .iter()
            .map(|run_id| run_id.0.clone())
            .collect::<Vec<_>>();
        let rows = tx
            .query(
                &format!(
                    "select run_id, parent_run_id
                     from {schema}.workflow_instances
                     where run_id = any($1::text[])"
                ),
                &[&batch_run_id_values],
            )
            .await
            .map_err(postgres_error)?;
        let mut dependency_run_ids = BTreeSet::new();
        for row in rows {
            let run_id = RunId::new(row.get::<_, String>(0));
            let parent_run_id = row.get::<_, Option<String>>(1).map(RunId::new);
            if terminal_run_ids.contains(&run_id)
                && parent_run_id
                    .as_ref()
                    .is_some_and(|parent_run_id| batch_run_ids.contains(parent_run_id))
            {
                dependency_run_ids.insert(run_id.clone());
            }
            if parent_run_id
                .as_ref()
                .is_some_and(|parent_run_id| terminal_run_ids.contains(parent_run_id))
            {
                dependency_run_ids.extend(parent_run_id);
            }
        }
        Ok(dependency_run_ids)
    }

    async fn apply_child_starts_for_simple_commits_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
        commits: &mut [PreparedSimpleWorkflowCommit],
    ) -> Result<()> {
        let child_start_count = commits
            .iter()
            .map(|commit| commit.start_child_workflows.len())
            .sum::<usize>();
        if child_start_count == 0 {
            return Ok(());
        }
        let proposed_run_ids = next_run_ids(tx, schema, child_start_count).await?;
        let mut proposed_run_ids = proposed_run_ids.into_iter();
        let mut starts = Vec::with_capacity(child_start_count);
        for (commit_index, commit) in commits.iter().enumerate() {
            for message in &commit.start_child_workflows {
                let child_shard_id = i32::try_from(
                    self.shard_for_workflow(
                        &Namespace::new(commit.namespace.clone()),
                        &message.workflow_id,
                    )
                    .0,
                )
                .unwrap_or(i32::MAX);
                let proposed_run_id = proposed_run_ids.next().ok_or_else(|| {
                    Error::Backend("postgres child start run id allocation underflow".to_owned())
                })?;
                starts.push(PreparedSimpleChildStart {
                    commit_index,
                    message: message.clone(),
                    proposed_run_id,
                    child_shard_id,
                });
            }
        }

        let parent_run_id_values = starts
            .iter()
            .map(|start| start.message.command_id.run_id.0.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let existing_child_events =
            existing_child_command_ids_tx(tx, schema, &parent_run_id_values).await?;

        let insertable = starts
            .iter()
            .filter(|start| !existing_child_events.contains(&start.message.command_id))
            .collect::<Vec<_>>();
        let mut inserted_children = BTreeMap::<(String, String), RunId>::new();
        if !insertable.is_empty() {
            let namespaces = insertable
                .iter()
                .map(|start| commits[start.commit_index].namespace.clone())
                .collect::<Vec<_>>();
            let workflow_ids = insertable
                .iter()
                .map(|start| start.message.workflow_id.0.clone())
                .collect::<Vec<_>>();
            let run_ids = insertable
                .iter()
                .map(|start| start.proposed_run_id.0.clone())
                .collect::<Vec<_>>();
            let shard_ids = insertable
                .iter()
                .map(|start| start.child_shard_id)
                .collect::<Vec<_>>();
            let workflow_names = insertable
                .iter()
                .map(|start| start.message.workflow_type.name.clone())
                .collect::<Vec<_>>();
            let workflow_versions = insertable
                .iter()
                .map(|start| i32::try_from(start.message.workflow_type.version).unwrap_or(i32::MAX))
                .collect::<Vec<_>>();
            let task_queues = insertable
                .iter()
                .map(|start| start.message.task_queue.0.clone())
                .collect::<Vec<_>>();
            let parent_run_ids = insertable
                .iter()
                .map(|start| start.message.command_id.run_id.0.clone())
                .collect::<Vec<_>>();
            let parent_command_seqs = insertable
                .iter()
                .map(|start| i64::try_from(start.message.command_id.seq.0).unwrap_or(i64::MAX))
                .collect::<Vec<_>>();
            let parent_close_policies = insertable
                .iter()
                .map(|start| {
                    parent_close_policy_to_str(start.message.parent_close_policy).to_owned()
                })
                .collect::<Vec<_>>();
            let parent_child_map_ordinals = insertable
                .iter()
                .map(|start| {
                    start
                        .message
                        .child_map_item
                        .as_ref()
                        .map(|item| i64::try_from(item.item_ordinal).unwrap_or(i64::MAX))
                })
                .collect::<Vec<_>>();
            let rows = tx
                .query(
                    &format!(
                        "insert into {schema}.workflow_instances
                         (namespace, workflow_id, run_id, shard_id, workflow_name, workflow_version,
                          task_queue, current_event_id, ready_reason, ready_at_ms,
                          workflow_claim_token, terminal, parent_run_id, parent_command_seq,
                          parent_close_policy, parent_child_map_ordinal)
                         select namespace, workflow_id, run_id, shard_id, workflow_name,
                                workflow_version, task_queue, 1, $12, 0, null, false,
                                parent_run_id, parent_command_seq, parent_close_policy,
                                parent_child_map_ordinal
                         from unnest($1::text[], $2::text[], $3::text[], $4::integer[],
                                     $5::text[], $6::integer[], $7::text[], $8::text[],
                                     $9::bigint[], $10::text[], $11::bigint[])
                              as child_rows(namespace, workflow_id, run_id, shard_id,
                                            workflow_name, workflow_version, task_queue,
                                            parent_run_id, parent_command_seq,
                                            parent_close_policy, parent_child_map_ordinal)
                         on conflict(namespace, workflow_id) do nothing
                         returning namespace, workflow_id, run_id"
                    ),
                    &[
                        &namespaces,
                        &workflow_ids,
                        &run_ids,
                        &shard_ids,
                        &workflow_names,
                        &workflow_versions,
                        &task_queues,
                        &parent_run_ids,
                        &parent_command_seqs,
                        &parent_close_policies,
                        &parent_child_map_ordinals,
                        &reason_to_str(&WorkflowTaskReason::WorkflowStarted),
                    ],
                )
                .await
                .map_err(postgres_error)?;
            for row in rows {
                inserted_children.insert(
                    (row.get::<_, String>(0), row.get::<_, String>(1)),
                    RunId::new(row.get::<_, String>(2)),
                );
            }
        }

        let conflict_keys = insertable
            .iter()
            .filter_map(|start| {
                let key = (
                    commits[start.commit_index].namespace.clone(),
                    start.message.workflow_id.0.clone(),
                );
                (!inserted_children.contains_key(&key)).then_some(key)
            })
            .collect::<BTreeSet<_>>();
        let existing_children =
            existing_child_workflows_for_keys_tx(tx, schema, &conflict_keys).await?;

        let mut child_history = Vec::<(RunId, HistoryEventData)>::new();
        for start in starts {
            if existing_child_events.contains(&start.message.command_id) {
                continue;
            }
            let namespace = commits[start.commit_index].namespace.clone();
            let key = (namespace, start.message.workflow_id.0.clone());
            let outcome = if let Some(run_id) = inserted_children.get(&key) {
                child_history.push((
                    run_id.clone(),
                    HistoryEventData::WorkflowStarted {
                        workflow_type: start.message.workflow_type.clone(),
                        input: start.message.input.clone(),
                    },
                ));
                InlineChildStartOutcome::Started(run_id.clone())
            } else if let Some(existing) = existing_children.get(&key) {
                let expected_map_ordinal = start
                    .message
                    .child_map_item
                    .as_ref()
                    .map(|item| i64::try_from(item.item_ordinal).unwrap_or(i64::MAX));
                let same_child = existing.parent_run_id.as_deref()
                    == Some(start.message.command_id.run_id.0.as_str())
                    && existing
                        .parent_command_seq
                        .and_then(|seq| u64::try_from(seq).ok())
                        == Some(start.message.command_id.seq.0)
                    && existing.parent_child_map_ordinal == expected_map_ordinal;
                if same_child {
                    InlineChildStartOutcome::Started(existing.run_id.clone())
                } else {
                    InlineChildStartOutcome::Failed(DurableFailure::non_retryable(
                        "durust.child_workflow_id_conflict",
                        format!(
                            "workflow id `{}` is already started",
                            start.message.workflow_id
                        ),
                    ))
                }
            } else {
                InlineChildStartOutcome::Vanished
            };
            let (event_data, reason) =
                child_start_outcome_event_and_reason(&start.message, outcome)?;
            let commit = &mut commits[start.commit_index];
            commit.next_event_id = commit.next_event_id.next();
            commit
                .append_history
                .push((commit.next_event_id, event_data));
            commit.ready_reason = Some(reason);
        }

        let child_history_rows = child_history
            .iter()
            .map(|(run_id, data)| HistoryEventInsert {
                run_id,
                event_id: EventId(1),
                data,
            })
            .collect::<Vec<_>>();
        insert_history_event_rows(tx, schema, &child_history_rows).await
    }

    async fn apply_simple_workflow_task_commits_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
        commits: &[crate::WorkflowTaskCommitInput],
        indices: &[usize],
    ) -> Result<Vec<(usize, Result<CommitOutcome>)>> {
        let run_id_values = indices
            .iter()
            .map(|index| commits[*index].claim.run_id.0.clone())
            .collect::<Vec<_>>();
        let rows = tx
            .query(
                &format!(
                    "select run_id, current_event_id, workflow_claim_token, terminal,
                            namespace, workflow_id, shard_id
                     from {schema}.workflow_instances
                     where run_id = any($1::text[])
                     for update"
                ),
                &[&run_id_values],
            )
            .await
            .map_err(postgres_error)?;
        let mut locked = BTreeMap::<RunId, LockedWorkflowCommitRow>::new();
        for row in rows {
            let run_id = RunId::new(row.get::<_, String>(0));
            locked.insert(
                run_id,
                LockedWorkflowCommitRow {
                    current_tail: EventId(u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX)),
                    claim_token: row.get(2),
                    terminal: row.get(3),
                    namespace: row.get(4),
                    workflow_id: row.get(5),
                    shard_id: row.get(6),
                },
            );
        }

        let mut item_results = Vec::with_capacity(indices.len());
        let mut prepared = Vec::<PreparedSimpleWorkflowCommit>::new();
        let mut conflict_updates = Vec::<(RunId, EventId)>::new();
        let lease_keys = indices
            .iter()
            .filter_map(|index| {
                let input = &commits[*index];
                let row = locked.get(&input.claim.run_id)?;
                (row.claim_token == Some(i64::try_from(input.claim.token).unwrap_or(i64::MAX)))
                    .then(|| (input.claim.worker_id.clone(), row.shard_id))
            })
            .collect::<BTreeSet<_>>();
        let lease_epochs = self.verify_shard_leases_tx(tx, &lease_keys).await?;
        for index in indices {
            let input = &commits[*index];
            let claim = input.claim.clone();
            let Some(row) = locked.get(&claim.run_id) else {
                item_results.push((*index, Err(Error::RunNotFound(claim.run_id))));
                continue;
            };
            if row.claim_token != Some(i64::try_from(claim.token).unwrap_or(i64::MAX)) {
                item_results.push((*index, Err(Error::StaleLease)));
                continue;
            }
            let lease_key = (claim.worker_id.clone(), row.shard_id);
            if !lease_epochs.contains_key(&lease_key) {
                item_results.push((*index, Err(Error::StaleLease)));
                continue;
            }
            let expected_tail_event_id = input.commit.expected_tail_event_id;
            if row.current_tail != expected_tail_event_id {
                conflict_updates.push((claim.run_id.clone(), row.current_tail));
                item_results.push((*index, Ok(CommitOutcome::Conflict)));
                continue;
            }
            if row.terminal && commit_has_workflow_visible_mutations(&input.commit) {
                item_results.push((*index, Err(Error::TerminalWorkflow)));
                continue;
            }

            let mut next_event_id = row.current_tail;
            let mut append_history = Vec::with_capacity(input.commit.append_events.len());
            let mut terminal_event = None;
            for event in &input.commit.append_events {
                let data = self
                    .normalize_history_event_for_storage_tx(tx, event.data.clone())
                    .await?;
                if postgres_simple_batch_terminal_event_eligible(&data) {
                    terminal_event = Some(data.clone());
                }
                next_event_id = next_event_id.next();
                append_history.push((next_event_id, data));
            }
            let mut schedule_activities =
                Vec::with_capacity(input.commit.schedule_activities.len());
            for task in &input.commit.schedule_activities {
                schedule_activities.push(
                    self.normalize_activity_task_for_storage_tx(tx, task.clone())
                        .await?,
                );
            }
            let mut start_child_workflows =
                Vec::with_capacity(input.commit.start_child_workflows.len());
            for message in &input.commit.start_child_workflows {
                start_child_workflows.push(
                    self.normalize_child_start_message_for_storage_tx(tx, message.clone())
                        .await?,
                );
            }
            let query_projection = match &input.commit.query_projection {
                Some(payload) => Some(
                    self.normalize_payload_for_storage_tx(tx, payload.clone())
                        .await?,
                ),
                None => None,
            };

            prepared.push(PreparedSimpleWorkflowCommit {
                input_index: *index,
                claim,
                next_event_id,
                namespace: row.namespace.clone(),
                workflow_id: row.workflow_id.clone(),
                append_history,
                schedule_activities,
                start_child_workflows,
                upsert_waits: input.commit.upsert_waits.clone(),
                consume_signals: input.commit.consume_signals.clone(),
                delete_waits: input.commit.delete_waits.clone(),
                query_projection,
                terminal_event,
                ready_reason: None,
            });
        }

        if !conflict_updates.is_empty() {
            let run_ids = conflict_updates
                .iter()
                .map(|(run_id, _)| run_id.0.clone())
                .collect::<Vec<_>>();
            tx.execute(
                &format!(
                    "update {schema}.workflow_instances workflows
                     set workflow_claim_token = null,
                         ready_reason = $2,
                         ready_at_ms = 0
                     where workflows.run_id = any($1::text[])"
                ),
                &[&run_ids, &reason_to_str(&WorkflowTaskReason::CacheEvicted)],
            )
            .await
            .map_err(postgres_error)?;
        }

        self.apply_child_starts_for_simple_commits_tx(tx, schema, &mut prepared)
            .await?;

        let history_rows = prepared
            .iter()
            .flat_map(|commit| {
                commit
                    .append_history
                    .iter()
                    .map(|(event_id, data)| HistoryEventInsert {
                        run_id: &commit.claim.run_id,
                        event_id: *event_id,
                        data,
                    })
            })
            .collect::<Vec<_>>();
        insert_history_event_rows(tx, schema, &history_rows).await?;

        insert_activity_task_rows_for_simple_commits_tx(tx, schema, &prepared).await?;
        upsert_wait_rows_for_simple_commits_tx(tx, schema, &prepared).await?;
        mark_signal_rows_consumed_for_simple_commits_tx(tx, schema, &prepared).await?;
        delete_wait_rows_for_simple_commits_tx(tx, schema, &prepared).await?;
        upsert_query_projection_rows_for_simple_commits_tx(tx, schema, &prepared).await?;

        let terminal_runs = prepared
            .iter()
            .filter_map(|commit| {
                commit
                    .terminal_event
                    .as_ref()
                    .map(|event| (commit.claim.run_id.clone(), event.clone()))
            })
            .collect::<Vec<_>>();
        if !terminal_runs.is_empty() {
            let terminal_run_ids = terminal_runs
                .iter()
                .map(|(run_id, _)| run_id.clone())
                .collect::<Vec<_>>();
            // Simple-batch terminal events are only completed/failed/cancelled
            // (continue-as-new falls back to the scalar path), so this is
            // always a closed-run cleanup.
            cleanup_runs_operational_state_tx(
                tx,
                schema,
                &terminal_run_ids,
                TerminalCleanup::Closed,
            )
            .await?;
            handle_terminal_runs_tx(self, tx, schema, &terminal_runs).await?;
        }

        // Recompute signal readiness for every committed non-terminal run now
        // that wait upserts, wait deletes, and signal consumption are applied;
        // a signal delivered while a task was claimed must re-mark its run
        // instead of being erased by the final update below. One set-based
        // query covers the whole batch.
        let signal_ready_run_ids = {
            let candidate_run_ids = prepared
                .iter()
                .filter(|commit| commit.terminal_event.is_none())
                .map(|commit| commit.claim.run_id.0.clone())
                .collect::<Vec<_>>();
            signal_wait_ready_run_ids(tx, schema, &candidate_run_ids).await?
        };
        for commit in &mut prepared {
            commit.ready_reason = post_commit_ready_reason(
                commit.terminal_event.is_some(),
                commit.ready_reason.take(),
                signal_ready_run_ids.contains(&commit.claim.run_id.0),
            );
        }

        if !prepared.is_empty() {
            let run_ids = prepared
                .iter()
                .map(|commit| commit.claim.run_id.0.clone())
                .collect::<Vec<_>>();
            let event_ids = prepared
                .iter()
                .map(|commit| i64::try_from(commit.next_event_id.0).unwrap_or(i64::MAX))
                .collect::<Vec<_>>();
            let terminal_flags = prepared
                .iter()
                .map(|commit| commit.terminal_event.is_some())
                .collect::<Vec<_>>();
            let ready_reasons = prepared
                .iter()
                .map(|commit| {
                    commit
                        .ready_reason
                        .as_ref()
                        .map(|reason| reason_to_str(reason).to_owned())
                })
                .collect::<Vec<_>>();
            tx.execute(
                &format!(
                    "update {schema}.workflow_instances workflows
                     set current_event_id = updates.current_event_id,
                         workflow_claim_token = null,
                         terminal = updates.terminal,
                         ready_reason = updates.ready_reason,
                         ready_at_ms = 0
                     from unnest($1::text[], $2::bigint[], $3::boolean[], $4::text[])
                          as updates(run_id, current_event_id, terminal, ready_reason)
                     where workflows.run_id = updates.run_id"
                ),
                &[&run_ids, &event_ids, &terminal_flags, &ready_reasons],
            )
            .await
            .map_err(postgres_error)?;
        }

        for commit in prepared {
            item_results.push((
                commit.input_index,
                Ok(CommitOutcome::Committed {
                    new_tail_event_id: commit.next_event_id,
                }),
            ));
        }
        Ok(item_results)
    }

    async fn apply_workflow_task_commit_tx(
        &self,
        tx: &Transaction<'_>,
        schema: &str,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
        mut lease_epoch_cache: Option<&mut BTreeMap<(WorkerId, i32), i64>>,
    ) -> Result<CommitOutcome> {
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
        // Shard-lease fencing: the verification must run even though its
        // epoch has no consumer; the per-batch cache avoids re-verifying one
        // (worker, shard) pair per item.
        match lease_epoch_cache.as_deref_mut() {
            Some(cache) => {
                let key = (claim.worker_id.clone(), shard_id);
                // The entry API cannot hold a borrow across the verify await.
                #[allow(clippy::map_entry)]
                if !cache.contains_key(&key) {
                    let lease_epoch = self
                        .verify_shard_lease_tx(tx, &claim.worker_id, shard_id)
                        .await?;
                    cache.insert(key, lease_epoch);
                }
            }
            None => {
                self.verify_shard_lease_tx(tx, &claim.worker_id, shard_id)
                    .await?;
            }
        }
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
            return Ok(CommitOutcome::Conflict);
        }
        if terminal && commit_has_workflow_visible_mutations(&batch) {
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
        let mut schedule_child_workflow_maps =
            Vec::with_capacity(batch.schedule_child_workflow_maps.len());
        for task in batch.schedule_child_workflow_maps {
            schedule_child_workflow_maps.push(
                self.normalize_child_workflow_map_task_for_storage_tx(&tx, task)
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
            // "This child event already exists" short-circuits here rather than
            // borrowing an `InlineChildStartOutcome`. It used to reuse the
            // variant that also means "the row vanished mid-transaction", and
            // the two were then dropped by the same arm — so the unreachable
            // one was silent.
            if child_event_exists_tx(&tx, &schema, &message.command_id).await? {
                continue;
            }
            let child_shard_id = i32::try_from(
                self.shard_for_workflow(&Namespace::new(namespace.clone()), &message.workflow_id)
                    .0,
            )
            .unwrap_or(i32::MAX);
            let child_start =
                start_child_workflow_inline_tx(&tx, &schema, &namespace, child_shard_id, &message)
                    .await?;
            if terminal || became_terminal {
                continue;
            }
            let (event_data, reason) = child_start_outcome_event_and_reason(&message, child_start)?;
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

        // The query projection records the history point the workflow task
        // itself observed, so it keeps the tail reached before the map loops
        // even when a map completed during this commit appends past it. Memory
        // has no separate cursor to drift, so capturing this is what keeps the
        // three providers stamping the same event id.
        let projection_event_id = next_event_id;
        let mut commit_tail_published = false;

        for map_task in schedule_activity_maps {
            insert_activity_map_tx(self, &tx, &schema, &namespace, &map_task).await?;
            if let Some((state, map_namespace, task)) =
                activity_map_state_tx(tx, schema, &map_task.map_command_id).await?
            {
                publish_commit_tail_before_map_append_tx(
                    tx,
                    schema,
                    &claim.run_id,
                    next_event_id,
                    &state,
                    &mut commit_tail_published,
                )
                .await?;
                if let Some(event_id) = step_map_tx(
                    self,
                    &tx,
                    &schema,
                    &state,
                    &map_namespace,
                    &MapTask::Activity(task),
                    MapEvent::DescriptorCreated {
                        parent_terminal: became_terminal,
                    },
                )
                .await?
                {
                    next_event_id = event_id;
                    ready_after_commit = Some(WorkflowTaskReason::ActivityMapCompleted);
                }
            }
        }

        for map_task in schedule_child_workflow_maps {
            insert_child_workflow_map_tx(self, &tx, &schema, &namespace, &map_task).await?;
            if let Some((state, map_namespace, task)) =
                child_workflow_map_state_tx(tx, schema, &map_task.map_command_id).await?
            {
                let map_namespace = if map_namespace.is_empty() {
                    namespace.clone()
                } else {
                    map_namespace
                };
                publish_commit_tail_before_map_append_tx(
                    tx,
                    schema,
                    &claim.run_id,
                    next_event_id,
                    &state,
                    &mut commit_tail_published,
                )
                .await?;
                if let Some(event_id) = step_map_tx(
                    self,
                    &tx,
                    &schema,
                    &state,
                    &map_namespace,
                    &MapTask::ChildWorkflow(task),
                    MapEvent::DescriptorCreated {
                        parent_terminal: became_terminal,
                    },
                )
                .await?
                {
                    next_event_id = event_id;
                    ready_after_commit = Some(WorkflowTaskReason::ChildWorkflowMapCompleted);
                }
            }
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
            cancel_command_operational_state_tx(self, &tx, &schema, &command_id).await?;
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
                    &i64::try_from(projection_event_id.0).unwrap_or(i64::MAX),
                    &payload_blob,
                ],
            )
            .await
            .map_err(postgres_error)?;
        }

        let terminal_after_commit = terminal || became_terminal;
        if terminal_after_commit {
            let cleanup = terminal_event
                .as_ref()
                .map(TerminalCleanup::for_terminal_event)
                .unwrap_or(TerminalCleanup::Closed);
            cleanup_run_operational_state_tx(&tx, &schema, &claim.run_id, cleanup).await?;
            if let Some(event @ HistoryEventData::WorkflowContinuedAsNew { .. }) =
                terminal_event.clone()
            {
                continue_run_as_new_tx(&tx, &schema, &claim.run_id, event).await?;
                return Ok(CommitOutcome::Committed {
                    new_tail_event_id: next_event_id,
                });
            }
            if let Some(event) = terminal_event {
                handle_terminal_run_tx(self, &tx, &schema, &claim.run_id, &event).await?;
            }
        }
        // Recompute signal readiness now that this commit's wait upserts,
        // wait deletes, and signal consumption are applied; a signal delivered
        // while the task was claimed must re-mark the run instead of being
        // erased by this update.
        let signal_ready =
            !terminal_after_commit && signal_wait_ready(&tx, schema, &claim.run_id).await?;
        let ready_reason =
            post_commit_ready_reason(terminal_after_commit, ready_after_commit, signal_ready);
        let ready_reason = ready_reason.as_ref().map(reason_to_str);
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
        Ok(CommitOutcome::Committed {
            new_tail_event_id: next_event_id,
        })
    }

    async fn signal_workflow_inner(
        &self,
        req: SignalWorkflowRequest,
    ) -> Result<SignalWorkflowOutcome> {
        self.retry_transaction(|| {
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

    async fn read_signal_inboxes_inner(
        &self,
        req: ReadSignalInboxesRequest,
    ) -> Result<Vec<Option<SignalInboxRecord>>> {
        if req.requests.is_empty() {
            return Ok(Vec::new());
        }

        let run_ids = req
            .requests
            .iter()
            .map(|request| request.run_id.0.clone())
            .collect::<Vec<_>>();
        let signal_names = req
            .requests
            .iter()
            .map(|request| request.signal_name.0.clone())
            .collect::<Vec<_>>();
        let schema = self.schema_sql();
        let rows = {
            let client = self.client().await?;
            client
                .query(
                    &format!(
                        "with requests as (
                             select request_index, run_id, signal_name
                             from unnest($1::text[], $2::text[])
                                  with ordinality as request_rows(run_id, signal_name, request_index)
                         )
                         select requests.request_index,
                                signal_rows.signal_id,
                                signal_rows.signal_name,
                                signal_rows.payload
                         from requests
                         left join lateral (
                             select signal_id, signal_name, payload
                             from {schema}.signals
                             where run_id = requests.run_id
                               and signal_name = requests.signal_name
                               and consumed = false
                             order by received_sequence asc
                             limit 1
                         ) signal_rows on true
                         order by requests.request_index asc"
                    ),
                    &[&run_ids, &signal_names],
                )
                .await
                .map_err(postgres_error)?
        };

        let mut records = vec![None; req.requests.len()];
        for row in rows {
            let request_index: i64 = row.get(0);
            let Some(signal_id) = row.get::<_, Option<String>>(1) else {
                continue;
            };
            let signal_name: String = row
                .get::<_, Option<String>>(2)
                .ok_or_else(|| Error::PayloadDecode("signal row missing signal_name".to_owned()))?;
            let payload: Vec<u8> = row
                .get::<_, Option<Vec<u8>>>(3)
                .ok_or_else(|| Error::PayloadDecode("signal row missing payload".to_owned()))?;
            let payload: PayloadRef = rmp_serde::from_slice(&payload)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            let payload = self.hydrate_payload_from_storage(payload).await?;
            let zero_based = request_index
                .checked_sub(1)
                .and_then(|index| usize::try_from(index).ok())
                .ok_or_else(|| {
                    Error::Backend(format!(
                        "postgres returned invalid signal request index {request_index}"
                    ))
                })?;
            let slot = records.get_mut(zero_based).ok_or_else(|| {
                Error::Backend(format!(
                    "postgres returned out-of-range signal request index {request_index}"
                ))
            })?;
            *slot = Some(SignalInboxRecord {
                signal_id: crate::SignalId::new(signal_id),
                signal_name: crate::SignalName::new(signal_name),
                payload,
            });
        }

        Ok(records)
    }

    async fn fire_due_timers_inner(
        &self,
        req: FireDueTimersRequest,
    ) -> Result<FireDueTimersOutcome> {
        self.retry_transaction(|| {
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
        self.retry_transaction(|| {
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
        let timed_out = timeout_due_activities_tx(self, &tx, &schema, req).await?;
        tx.commit().await.map_err(postgres_error)?;
        Ok(TimeoutDueActivitiesOutcome { timed_out })
    }

    async fn run_due_maintenance_inner(
        &self,
        req: RunDueMaintenanceRequest,
    ) -> Result<RunDueMaintenanceOutcome> {
        self.retry_transaction(|| {
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
            self,
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
        self.retry_transaction(|| {
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
                       and (visible_at_ms is null or visible_at_ms <= $4)
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
        // Tasks without explicit timeouts get the lease as an implicit
        // heartbeat interval; explicit deadlines stay authoritative and the
        // stored timeout_at_ms is untouched by the claim.
        let implicit_heartbeat_ms = activity_claim_implicit_heartbeat_ms(
            task.start_to_close_timeout,
            task.heartbeat_timeout,
            opts.lease_duration,
        );
        tx.execute(
            &format!(
                "update {schema}.activity_tasks
                 set claim_token = $1,
                     heartbeat_deadline_at_ms = $2,
                     implicit_heartbeat_ms = $3
                 where activity_id = $4"
            ),
            &[
                &i64::try_from(token).unwrap_or(i64::MAX),
                &activity_heartbeat_deadline_at_ms(
                    TimestampMs(now),
                    task.heartbeat_timeout,
                    implicit_heartbeat_ms,
                ),
                &implicit_heartbeat_ms,
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
        self.retry_transaction(|| {
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
                       and (visible_at_ms is null or visible_at_ms <= $4)
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
        // Tasks without explicit timeouts get the lease as an implicit
        // heartbeat interval; explicit deadlines stay authoritative and the
        // stored timeout_at_ms is untouched by the claim (-1 marks "none" in
        // the unnest arrays).
        let implicit_heartbeats = tasks
            .iter()
            .map(|(_, task)| {
                activity_claim_implicit_heartbeat_ms(
                    task.start_to_close_timeout,
                    task.heartbeat_timeout,
                    opts.claim.lease_duration,
                )
                .unwrap_or(-1)
            })
            .collect::<Vec<_>>();
        let heartbeat_deadlines = tasks
            .iter()
            .zip(implicit_heartbeats.iter())
            .map(|((_, task), implicit)| {
                activity_heartbeat_deadline_at_ms(
                    TimestampMs(now),
                    task.heartbeat_timeout,
                    (*implicit >= 0).then_some(*implicit),
                )
                .unwrap_or(-1)
            })
            .collect::<Vec<_>>();
        tx.execute(
            &format!(
                "update {schema}.activity_tasks tasks
                 set claim_token = claimed.claim_token,
                     heartbeat_deadline_at_ms = nullif(claimed.heartbeat_deadline_at_ms, -1),
                     implicit_heartbeat_ms = nullif(claimed.implicit_heartbeat_ms, -1)
                 from unnest($1::text[], $2::bigint[], $3::bigint[], $4::bigint[])
                      as claimed(activity_id, claim_token, heartbeat_deadline_at_ms, implicit_heartbeat_ms)
                 where tasks.activity_id = claimed.activity_id"
            ),
            &[
                &activity_ids,
                &token_values,
                &heartbeat_deadlines,
                &implicit_heartbeats,
            ],
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
        self.retry_transaction(|| {
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
                    "select task, claim_token, completed, implicit_heartbeat_ms
                     from {schema}.activity_tasks
                     where activity_id = $1
                     for update"
                ),
                &[&req.claim.activity_id.0],
            )
            .await
            .map_err(postgres_error)?
        else {
            // Activity rows exist until their run's terminal cleanup deletes
            // them, so a missing row is a completed activity.
            tx.commit().await.map_err(postgres_error)?;
            return Ok(ActivityHeartbeatOutcome::AlreadyCompleted);
        };
        let task_blob: Vec<u8> = row.get(0);
        let claim_token: Option<i64> = row.get(1);
        let completed: bool = row.get(2);
        let implicit_heartbeat_ms: Option<i64> = row.get(3);
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
                &activity_heartbeat_deadline_at_ms(
                    TimestampMs(unix_epoch_millis()),
                    task.heartbeat_timeout,
                    implicit_heartbeat_ms,
                ),
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
        self.retry_transaction(|| {
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
        self.retry_transaction(|| {
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
                // Missing row means the run's terminal cleanup deleted it.
                result_slots[index] = Some(Ok(CompleteActivityOutcome::AlreadyCompleted));
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
                             heartbeat_deadline_at_ms = null,
                             implicit_heartbeat_ms = null
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
            // Missing row means the run's terminal cleanup deleted it.
            return Ok(CompleteActivityOutcome::AlreadyCompleted);
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
                     heartbeat_deadline_at_ms = null,
                     implicit_heartbeat_ms = null
                 where activity_id = $1"
            ),
            &[&req.claim.activity_id.0],
        )
        .await
        .map_err(postgres_error)?;
        Ok(CompleteActivityOutcome::Completed { event_id })
    }

    async fn fail_activity_inner(&self, req: FailActivityRequest) -> Result<FailActivityOutcome> {
        self.retry_transaction(|| {
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
            // Missing row means the run's terminal cleanup deleted it.
            tx.commit().await.map_err(postgres_error)?;
            return Ok(FailActivityOutcome::AlreadyCompleted);
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
        let decision = activity_failure_decision(&task, req.failure.non_retryable);

        // Map items take the engine route for *both* verdicts: a retry of an
        // item whose map already ended must not reschedule anything, and only
        // the engine knows whether the map ended.
        if let Some(map_item) = task.map_item.clone() {
            let decision = ItemRetryDecision::from(decision);
            // Only an exhausting attempt persists its failure, so a retried
            // attempt never offloads a payload the map discards.
            let failure = if matches!(decision, ItemRetryDecision::Exhausted) {
                self.normalize_failure_for_storage_tx(&tx, req.failure)
                    .await?
            } else {
                req.failure
            };
            let now = TimestampMs(unix_epoch_millis());
            let outcome = fail_map_item_tx(
                self,
                &tx,
                &schema,
                task,
                map_item,
                failure,
                ItemAttemptFailureKind::Failed,
                decision,
                now,
            )
            .await?;
            if !matches!(outcome, FailActivityOutcome::RetryScheduled { .. }) {
                tx.execute(
                    &format!(
                        "update {schema}.activity_tasks
                         set completed = true,
                             heartbeat_deadline_at_ms = null,
                             implicit_heartbeat_ms = null
                         where activity_id = $1"
                    ),
                    &[&req.claim.activity_id.0],
                )
                .await
                .map_err(postgres_error)?;
            }
            tx.commit().await.map_err(postgres_error)?;
            return Ok(outcome);
        }

        if let ActivityFailureDecision::Retry { next_attempt } = decision {
            let mut retry_task = task.clone();
            retry_task.attempt = next_attempt;
            let retry_blob = rmp_serde::to_vec_named(&retry_task)
                .map_err(|err| Error::PayloadEncode(err.to_string()))?;
            // The retry backoff delays visibility; the start-to-close clock
            // restarts at the visibility instant so the timeout scanner
            // cannot fire on a task that was never claimable.
            let now = TimestampMs(unix_epoch_millis());
            let visible_at_ms = retry_visible_at_ms(&task.retry_policy, task.attempt, now);
            let visible_from = visible_at_ms.map(TimestampMs).unwrap_or(now);
            tx.execute(
                &format!(
                    "update {schema}.activity_tasks
                     set task = $1,
                         claim_token = null,
                         timeout_at_ms = $2,
                         heartbeat_deadline_at_ms = null,
                         implicit_heartbeat_ms = null,
                         visible_at_ms = $3
                     where activity_id = $4"
                ),
                &[
                    &retry_blob,
                    &activity_timeout_at_ms_from(visible_from, retry_task.start_to_close_timeout),
                    &visible_at_ms,
                    &req.claim.activity_id.0,
                ],
            )
            .await
            .map_err(postgres_error)?;
            tx.commit().await.map_err(postgres_error)?;
            return Ok(FailActivityOutcome::RetryScheduled { next_attempt });
        }

        let failure = self
            .normalize_failure_for_storage_tx(&tx, req.failure)
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
                     heartbeat_deadline_at_ms = null,
                     implicit_heartbeat_ms = null
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
        // A commit racing this sweep may reuse an existing unreachable blob
        // (content-addressed dedup) without inserting a row. Two mechanisms
        // close that window: the grace period skips young rows, and the
        // delete's timestamp predicate re-evaluates under the row lock the
        // reusing commit's `on conflict do update` touch takes, so a blob
        // touched between the scan and the delete survives.
        let cutoff = payload_gc_cutoff_ms(unix_epoch_millis(), req.min_age);
        let rows = tx
            .query(
                &format!(
                    "select digest, created_at_ms from {schema}.payload_blobs
                     order by digest asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        let all_blobs = rows
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
            .collect::<BTreeMap<_, _>>();
        let scanned_blobs = all_blobs.len();
        let garbage = all_blobs
            .into_iter()
            .filter(|(digest, created_at_ms)| {
                !reachable.contains(digest) && *created_at_ms <= cutoff
            })
            .map(|(digest, _)| digest)
            .collect::<Vec<_>>();
        let mut deleted_blobs = garbage.len();
        if !req.dry_run {
            deleted_blobs = 0;
            for digest in garbage {
                deleted_blobs += tx
                    .execute(
                        &format!(
                            "delete from {schema}.payload_blobs
                             where digest = $1 and created_at_ms <= $2"
                        ),
                        &[&digest, &cutoff],
                    )
                    .await
                    .map_err(postgres_error)? as usize;
            }
        }
        tx.commit().await.map_err(postgres_error)?;
        Ok(PayloadGarbageCollectionOutcome {
            scanned_blobs,
            retained_blobs: scanned_blobs - deleted_blobs,
            deleted_blobs,
            failed_blobs: 0,
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
                &format!(
                    "select task from {schema}.child_workflow_maps order by map_command_id asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let task: ChildWorkflowMapTask = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            roots.push(PayloadRootRef::ActivityMapInputManifest(
                self.activity_map_input_root_for_roots_tx(tx, task.input_manifest)
                    .await?,
            ));
        }

        let rows = tx
            .query(
                &format!(
                    "select outcome
                     from {schema}.child_workflow_map_results
                     order by map_command_id asc, item_ordinal asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let outcome: ChildWorkflowMapItemOutcome = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            collect_child_workflow_map_outcome_payload_roots(&outcome, &mut roots);
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
                &format!(
                    "select task from {schema}.child_workflow_maps order by map_command_id asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let task: ChildWorkflowMapTask = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_activity_map_input_manifest_ref_tx(tx, &task.input_manifest, reachable)
                .await?;
        }

        let rows = tx
            .query(
                &format!(
                    "select outcome
                     from {schema}.child_workflow_map_results
                     order by map_command_id asc, item_ordinal asc"
                ),
                &[],
            )
            .await
            .map_err(postgres_error)?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let outcome: ChildWorkflowMapItemOutcome = rmp_serde::from_slice(&blob)
                .map_err(|err| Error::PayloadDecode(err.to_string()))?;
            self.collect_child_workflow_map_outcome_payload_blobs_tx(tx, &outcome, reachable)
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
            HistoryEventData::ChildWorkflowMapScheduled(scheduled) => {
                roots.push(PayloadRootRef::ActivityMapInputManifest(
                    self.activity_map_input_root_for_roots_tx(tx, scheduled.input_manifest.clone())
                        .await?,
                ));
            }
            HistoryEventData::ChildWorkflowMapCompleted(completed) => {
                roots.push(PayloadRootRef::ChildWorkflowMapResultManifest(
                    self.child_workflow_map_result_root_for_roots_tx(
                        tx,
                        completed.result_manifest.clone(),
                    )
                    .await?,
                ));
            }
            HistoryEventData::ChildWorkflowMapFailed(failed) => {
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
            HistoryEventData::ChildWorkflowMapScheduled(scheduled) => {
                self.collect_activity_map_input_manifest_ref_tx(
                    tx,
                    &scheduled.input_manifest,
                    reachable,
                )
                .await
            }
            HistoryEventData::ChildWorkflowMapCompleted(completed) => {
                self.collect_child_workflow_map_result_manifest_ref_tx(
                    tx,
                    &completed.result_manifest,
                    reachable,
                )
                .await
            }
            HistoryEventData::ChildWorkflowMapFailed(failed) => {
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
        if is_postgres_payload_uri(uri) {
            self.load_payload_blob_tx(tx, payload, false).await?;
        }
        reachable.insert(digest.clone());
        Ok(())
    }

    async fn activity_map_input_root_for_roots_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_external_payload_ref(&payload) {
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
        if is_external_payload_ref(&payload) {
            return Ok(payload);
        }
        self.hydrate_activity_map_result_manifest_from_storage_tx(tx, payload)
            .await
    }

    async fn child_workflow_map_result_root_for_roots_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_external_payload_ref(&payload) {
            return Ok(payload);
        }
        self.hydrate_child_workflow_map_result_manifest_from_storage_tx(tx, payload)
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
        if is_external_payload_ref(payload) {
            return Ok(());
        }
        let manifest_payload = self
            .hydrate_payload_from_storage_tx(tx, payload.clone())
            .await?;
        let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
        for page in manifest.pages {
            self.collect_payload_blob_ref_tx(tx, &page, reachable)
                .await?;
            if is_external_payload_ref(&page) {
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
        if is_external_payload_ref(payload) {
            return Ok(());
        }
        let manifest_payload = self
            .hydrate_payload_from_storage_tx(tx, payload.clone())
            .await?;
        let manifest: ActivityMapResultManifest = crate::decode_payload(&manifest_payload)?;
        for page in manifest.pages {
            self.collect_payload_blob_ref_tx(tx, &page, reachable)
                .await?;
            if is_external_payload_ref(&page) {
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

    async fn collect_child_workflow_map_result_manifest_ref_tx(
        &self,
        tx: &Transaction<'_>,
        payload: &PayloadRef,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        self.collect_payload_blob_ref_tx(tx, payload, reachable)
            .await?;
        if is_external_payload_ref(payload) {
            return Ok(());
        }
        let manifest_payload = self
            .hydrate_payload_from_storage_tx(tx, payload.clone())
            .await?;
        let manifest: crate::ChildWorkflowMapResultManifest =
            crate::decode_payload(&manifest_payload)?;
        for page in manifest.pages {
            self.collect_payload_blob_ref_tx(tx, &page, reachable)
                .await?;
            if is_external_payload_ref(&page) {
                continue;
            }
            let page_payload = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let page: crate::ChildWorkflowMapResultPage = crate::decode_payload(&page_payload)?;
            for outcome in page.outcomes {
                match outcome {
                    crate::ChildWorkflowMapItemOutcome::Succeeded { result } => {
                        self.collect_payload_blob_ref_tx(tx, &result, reachable)
                            .await?;
                    }
                    crate::ChildWorkflowMapItemOutcome::Failed { failure } => {
                        self.collect_failure_payload_blobs_tx(tx, &failure, reachable)
                            .await?;
                    }
                    crate::ChildWorkflowMapItemOutcome::Cancelled { .. } => {}
                }
            }
        }
        Ok(())
    }

    async fn collect_child_workflow_map_outcome_payload_blobs_tx(
        &self,
        tx: &Transaction<'_>,
        outcome: &ChildWorkflowMapItemOutcome,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        match outcome {
            ChildWorkflowMapItemOutcome::Succeeded { result } => {
                self.collect_payload_blob_ref_tx(tx, result, reachable)
                    .await?;
            }
            ChildWorkflowMapItemOutcome::Failed { failure } => {
                self.collect_failure_payload_blobs_tx(tx, failure, reachable)
                    .await?;
            }
            ChildWorkflowMapItemOutcome::Cancelled { .. } => {}
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
                // The conflict arm is `do update` rather than `do nothing` on
                // purpose: refreshing `created_at_ms` restarts the GC grace
                // period for a reused blob AND takes a row lock, so a
                // concurrent GC delete serializes against this commit and
                // re-evaluates its timestamp predicate under the refreshed
                // value instead of deleting a row this commit references.
                tx.execute(
                    &format!(
                        "insert into {schema}.payload_blobs
                         (digest, codec, schema_fingerprint, compression, encryption, size, bytes,
                          created_at_ms)
                         values ($1, $2, $3, $4, $5, $6, $7, $8)
                         on conflict(digest) do update set created_at_ms = excluded.created_at_ms"
                    ),
                    &[
                        &digest,
                        &codec_to_str(codec),
                        &schema_fingerprint.0,
                        &compression_to_str(compression),
                        &encryption_blob,
                        &i64::try_from(size).unwrap_or(i64::MAX),
                        &bytes,
                        &unix_epoch_millis(),
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
                // Only refs with this provider's scheme are validated against
                // its store; every other scheme is opaque and persists
                // unchanged.
                if matches!(&payload, PayloadRef::Blob { uri, .. } if is_postgres_payload_uri(uri))
                {
                    self.load_payload_blob_tx(tx, &payload, true).await?;
                }
                Ok(payload)
            }
        }
    }

    async fn hydrate_payload_from_storage(&self, payload: PayloadRef) -> Result<PayloadRef> {
        match payload {
            payload @ PayloadRef::Inline { .. } => Ok(payload),
            payload @ PayloadRef::Blob { .. } if is_external_payload_ref(&payload) => Ok(payload),
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
                let blob = self.load_payload_blob(&payload, false).await?;
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
            payload @ PayloadRef::Blob { .. } if is_external_payload_ref(&payload) => Ok(payload),
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
                let blob = self.load_payload_blob_tx(tx, &payload, false).await?;
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
        if is_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            // A foreign-scheme page under an inline root is opaque: the
            // owning layer normalized its items before this commit, so it
            // passes through untouched (mirroring the reachability
            // collectors' external-page skip).
            if is_external_payload_ref(&page) {
                pages.push(page);
                continue;
            }
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
        if is_external_payload_ref(&payload) {
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

    async fn normalize_child_workflow_map_result_manifest_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let mut manifest: crate::ChildWorkflowMapResultManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let mut page: crate::ChildWorkflowMapResultPage = crate::decode_payload(&page)?;
            let mut outcomes = Vec::with_capacity(page.outcomes.len());
            for outcome in page.outcomes {
                outcomes.push(
                    self.normalize_child_workflow_map_outcome_for_storage_tx(tx, outcome)
                        .await?,
                );
            }
            page.outcomes = outcomes;
            let page = crate::encode_payload_with_codec(&page, self.payload_config.codec)?;
            pages.push(self.normalize_payload_for_storage_tx(tx, page).await?);
        }
        manifest.pages = pages;
        let root = crate::encode_payload_with_codec(&manifest, self.payload_config.codec)?;
        self.normalize_payload_for_storage_tx(tx, root).await
    }

    async fn normalize_child_workflow_map_outcome_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        outcome: ChildWorkflowMapItemOutcome,
    ) -> Result<ChildWorkflowMapItemOutcome> {
        match outcome {
            ChildWorkflowMapItemOutcome::Succeeded { result } => {
                Ok(ChildWorkflowMapItemOutcome::Succeeded {
                    result: self.normalize_payload_for_storage_tx(tx, result).await?,
                })
            }
            ChildWorkflowMapItemOutcome::Failed { failure } => {
                Ok(ChildWorkflowMapItemOutcome::Failed {
                    failure: self.normalize_failure_for_storage_tx(tx, failure).await?,
                })
            }
            ChildWorkflowMapItemOutcome::Cancelled { reason } => {
                Ok(ChildWorkflowMapItemOutcome::Cancelled { reason })
            }
        }
    }

    async fn hydrate_activity_map_input_manifest_from_storage(
        &self,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage(payload).await?;
        let root_codec = root.codec();
        let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            // A foreign-scheme page under an inline root is opaque here; the
            // owning layer hydrates it.
            if is_external_payload_ref(&page) {
                pages.push(page);
                continue;
            }
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
        if is_external_payload_ref(&payload) {
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

    async fn hydrate_child_workflow_map_result_manifest_from_storage(
        &self,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage(payload).await?;
        let root_codec = root.codec();
        let mut manifest: crate::ChildWorkflowMapResultManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage(page).await?;
            let page_codec = page.codec();
            let mut page: crate::ChildWorkflowMapResultPage = crate::decode_payload(&page)?;
            let mut outcomes = Vec::with_capacity(page.outcomes.len());
            for outcome in page.outcomes {
                outcomes.push(
                    self.hydrate_child_workflow_map_outcome_from_storage(outcome)
                        .await?,
                );
            }
            page.outcomes = outcomes;
            pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
        }
        manifest.pages = pages;
        crate::encode_payload_with_codec(&manifest, root_codec)
    }

    async fn hydrate_child_workflow_map_outcome_from_storage(
        &self,
        outcome: crate::ChildWorkflowMapItemOutcome,
    ) -> Result<crate::ChildWorkflowMapItemOutcome> {
        match outcome {
            crate::ChildWorkflowMapItemOutcome::Succeeded { result } => {
                Ok(crate::ChildWorkflowMapItemOutcome::Succeeded {
                    result: self.hydrate_payload_from_storage(result).await?,
                })
            }
            crate::ChildWorkflowMapItemOutcome::Failed { mut failure } => {
                if let Some(details) = failure.details.take() {
                    failure.details = Some(self.hydrate_payload_from_storage(details).await?);
                }
                Ok(crate::ChildWorkflowMapItemOutcome::Failed { failure })
            }
            crate::ChildWorkflowMapItemOutcome::Cancelled { reason } => {
                Ok(crate::ChildWorkflowMapItemOutcome::Cancelled { reason })
            }
        }
    }

    async fn hydrate_activity_map_input_manifest_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let root_codec = root.codec();
        let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            // A foreign-scheme page under an inline root is opaque here; the
            // owning layer hydrates it.
            if is_external_payload_ref(&page) {
                pages.push(page);
                continue;
            }
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
        if is_external_payload_ref(&payload) {
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

    async fn hydrate_child_workflow_map_result_manifest_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        payload: PayloadRef,
    ) -> Result<PayloadRef> {
        if is_external_payload_ref(&payload) {
            return Ok(payload);
        }
        let root = self.hydrate_payload_from_storage_tx(tx, payload).await?;
        let root_codec = root.codec();
        let mut manifest: crate::ChildWorkflowMapResultManifest = crate::decode_payload(&root)?;
        let mut pages = Vec::with_capacity(manifest.pages.len());
        for page in manifest.pages {
            let page = self.hydrate_payload_from_storage_tx(tx, page).await?;
            let page_codec = page.codec();
            let mut page: crate::ChildWorkflowMapResultPage = crate::decode_payload(&page)?;
            let mut outcomes = Vec::with_capacity(page.outcomes.len());
            for outcome in page.outcomes {
                outcomes.push(
                    self.hydrate_child_workflow_map_outcome_from_storage_tx(tx, outcome)
                        .await?,
                );
            }
            page.outcomes = outcomes;
            pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
        }
        manifest.pages = pages;
        crate::encode_payload_with_codec(&manifest, root_codec)
    }

    async fn hydrate_child_workflow_map_outcome_from_storage_tx(
        &self,
        tx: &Transaction<'_>,
        outcome: crate::ChildWorkflowMapItemOutcome,
    ) -> Result<crate::ChildWorkflowMapItemOutcome> {
        match outcome {
            crate::ChildWorkflowMapItemOutcome::Succeeded { result } => {
                Ok(crate::ChildWorkflowMapItemOutcome::Succeeded {
                    result: self.hydrate_payload_from_storage_tx(tx, result).await?,
                })
            }
            crate::ChildWorkflowMapItemOutcome::Failed { mut failure } => {
                if let Some(details) = failure.details.take() {
                    failure.details =
                        Some(self.hydrate_payload_from_storage_tx(tx, details).await?);
                }
                Ok(crate::ChildWorkflowMapItemOutcome::Failed { failure })
            }
            crate::ChildWorkflowMapItemOutcome::Cancelled { reason } => {
                Ok(crate::ChildWorkflowMapItemOutcome::Cancelled { reason })
            }
        }
    }

    async fn normalize_history_event_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        data: HistoryEventData,
    ) -> Result<HistoryEventData> {
        let mut rewriter = PostgresNormalizeRewriter { backend: self, tx };
        crate::payload::rewrite_history_event_payloads(&mut rewriter, data).await
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

    async fn normalize_child_workflow_map_task_for_storage_tx(
        &self,
        tx: &Transaction<'_>,
        mut task: ChildWorkflowMapTask,
    ) -> Result<ChildWorkflowMapTask> {
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
        let mut rewriter = PostgresHydrateRewriter { backend: self };
        crate::payload::rewrite_history_event_payloads(&mut rewriter, data).await
    }

    async fn load_payload_blob_tx(
        &self,
        tx: &Transaction<'_>,
        payload: &PayloadRef,
        require_schema_fingerprint_match: bool,
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
            require_schema_fingerprint_match,
        )
    }

    async fn load_payload_blob(
        &self,
        payload: &PayloadRef,
        require_schema_fingerprint_match: bool,
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
            require_schema_fingerprint_match,
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

/// Set-based form of `signal_wait_ready` for batch commits: the runs among
/// `run_id_values` that have a live signal wait matching an unconsumed signal.
async fn signal_wait_ready_run_ids(
    tx: &Transaction<'_>,
    schema: &str,
    run_id_values: &[String],
) -> Result<BTreeSet<String>> {
    if run_id_values.is_empty() {
        return Ok(BTreeSet::new());
    }
    let rows = tx
        .query(
            &format!(
                "select distinct w.run_id
                 from {schema}.active_waits w
                 join {schema}.signals s on s.run_id = w.run_id
                    and s.signal_name = w.wait_key
                    and s.consumed = false
                 where w.run_id = any($1::text[]) and w.kind = $2"
            ),
            &[&run_id_values, &wait_kind_to_str(&WaitKind::Signal)],
        )
        .await
        .map_err(postgres_error)?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect())
}

async fn cleanup_run_operational_state_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    cleanup: TerminalCleanup,
) -> Result<()> {
    cleanup_runs_operational_state_tx(tx, schema, std::slice::from_ref(run_id), cleanup).await
}

// Deletes the terminal runs' operational rows; see `TerminalCleanup` for the
// contract (history stays authoritative, missing activity rows answer late
// calls as `AlreadyCompleted`, signal rows survive continue-as-new).
async fn cleanup_runs_operational_state_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_ids: &[RunId],
    cleanup: TerminalCleanup,
) -> Result<()> {
    if run_ids.is_empty() {
        return Ok(());
    }
    let run_id_values = run_ids
        .iter()
        .map(|run_id| run_id.0.clone())
        .collect::<Vec<_>>();
    tx.execute(
        &format!("delete from {schema}.active_waits where run_id = any($1::text[])"),
        &[&run_id_values],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!("delete from {schema}.activity_tasks where run_id = any($1::text[])"),
        &[&run_id_values],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!(
            "delete from {schema}.activity_map_results
             where map_command_id in (
                 select map_command_id from {schema}.activity_maps
                 where run_id = any($1::text[])
             )"
        ),
        &[&run_id_values],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!("delete from {schema}.activity_maps where run_id = any($1::text[])"),
        &[&run_id_values],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!(
            "delete from {schema}.child_workflow_map_results
             where map_command_id in (
                 select map_command_id from {schema}.child_workflow_maps
                 where run_id = any($1::text[])
             )"
        ),
        &[&run_id_values],
    )
    .await
    .map_err(postgres_error)?;
    tx.execute(
        &format!("delete from {schema}.child_workflow_maps where run_id = any($1::text[])"),
        &[&run_id_values],
    )
    .await
    .map_err(postgres_error)?;
    if cleanup.deletes_consumed_signals() {
        // Unconsumed deliveries stay readable through the inbox after the run
        // closes; only the consumed dedup rows go.
        tx.execute(
            &format!(
                "delete from {schema}.signals
                 where run_id = any($1::text[]) and consumed = true"
            ),
            &[&run_id_values],
        )
        .await
        .map_err(postgres_error)?;
    }
    Ok(())
}

async fn handle_terminal_run_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    handle_terminal_runs_tx(
        backend,
        tx,
        schema,
        &[(run_id.clone(), terminal_event.clone())],
    )
    .await
}

async fn handle_terminal_runs_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    terminal_runs: &[(RunId, HistoryEventData)],
) -> Result<()> {
    notify_parents_of_child_terminals_tx(backend, tx, schema, terminal_runs).await?;
    let run_ids = terminal_runs
        .iter()
        .map(|(run_id, _)| run_id.clone())
        .collect::<Vec<_>>();
    cancel_children_for_parents_tx(tx, schema, &run_ids).await
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

#[derive(Clone)]
struct DirectChildTerminalNotification {
    parent_run_id: RunId,
    command_id: CommandId,
    terminal_event: HistoryEventData,
}

async fn notify_parents_of_child_terminals_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    terminal_runs: &[(RunId, HistoryEventData)],
) -> Result<()> {
    if terminal_runs.is_empty() {
        return Ok(());
    }
    let terminal_events = terminal_runs
        .iter()
        .map(|(run_id, event)| (run_id.clone(), event.clone()))
        .collect::<BTreeMap<_, _>>();
    let run_ids = terminal_runs
        .iter()
        .map(|(run_id, _)| run_id.0.clone())
        .collect::<Vec<_>>();
    let rows = tx
        .query(
            &format!(
                "select run_id, parent_run_id, parent_command_seq, parent_child_map_ordinal
                 from {schema}.workflow_instances
                 where run_id = any($1::text[])
                   and parent_run_id is not null"
            ),
            &[&run_ids],
        )
        .await
        .map_err(postgres_error)?;
    let mut direct_notifications = Vec::new();
    for row in rows {
        let child_run_id = RunId::new(row.get::<_, String>(0));
        let Some(terminal_event) = terminal_events.get(&child_run_id).cloned() else {
            continue;
        };
        let parent_run_id: Option<String> = row.get(1);
        let parent_command_seq: Option<i64> = row.get(2);
        let parent_child_map_ordinal: Option<i64> = row.get(3);
        let Some((parent_run_id, parent_command_seq)) = parent_run_id
            .zip(parent_command_seq)
            .and_then(|(run_id, seq)| {
                Some((RunId::new(run_id), CommandSeq(u64::try_from(seq).ok()?)))
            })
        else {
            continue;
        };
        let command_id = CommandId {
            run_id: parent_run_id.clone(),
            seq: parent_command_seq,
        };
        if let Some(item_ordinal) =
            parent_child_map_ordinal.and_then(|ordinal| u64::try_from(ordinal).ok())
        {
            let Some(outcome) = child_terminal_map_item_outcome(&terminal_event) else {
                continue;
            };
            complete_child_workflow_map_item_tx(
                backend,
                tx,
                schema,
                ChildWorkflowMapItem {
                    map_command_id: command_id,
                    item_ordinal,
                },
                outcome,
            )
            .await?;
            continue;
        }
        direct_notifications.push(DirectChildTerminalNotification {
            parent_run_id,
            command_id,
            terminal_event,
        });
    }
    notify_direct_parents_of_child_terminals_tx(tx, schema, direct_notifications).await
}

async fn notify_direct_parents_of_child_terminals_tx(
    tx: &Transaction<'_>,
    schema: &str,
    notifications: Vec<DirectChildTerminalNotification>,
) -> Result<()> {
    if notifications.is_empty() {
        return Ok(());
    }
    let parent_run_ids = notifications
        .iter()
        .map(|notification| notification.parent_run_id.clone())
        .collect::<BTreeSet<_>>();
    let parent_run_id_values = parent_run_ids
        .iter()
        .map(|run_id| run_id.0.clone())
        .collect::<Vec<_>>();
    let existing_command_ids =
        existing_child_terminal_command_ids_tx(tx, schema, &parent_run_id_values).await?;
    let rows = tx
        .query(
            &format!(
                "select run_id, current_event_id, terminal
                 from {schema}.workflow_instances
                 where run_id = any($1::text[])
                 for update"
            ),
            &[&parent_run_id_values],
        )
        .await
        .map_err(postgres_error)?;
    let mut parent_tails = rows
        .into_iter()
        .map(|row| {
            (
                RunId::new(row.get::<_, String>(0)),
                (
                    EventId(u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX)),
                    row.get::<_, bool>(2),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut parent_events = Vec::<(RunId, EventId, HistoryEventData)>::new();
    let mut parent_updates = BTreeMap::<RunId, (EventId, WorkflowTaskReason)>::new();
    for notification in notifications {
        if existing_command_ids.contains(&notification.command_id) {
            continue;
        }
        let Some((tail, terminal)) = parent_tails.get_mut(&notification.parent_run_id) else {
            continue;
        };
        if *terminal {
            continue;
        }
        let Some((event_data, reason)) = child_terminal_event_data_and_reason(
            notification.command_id,
            &notification.terminal_event,
        ) else {
            continue;
        };
        let event_id = tail.next();
        *tail = event_id;
        parent_updates.insert(notification.parent_run_id.clone(), (event_id, reason));
        parent_events.push((notification.parent_run_id, event_id, event_data));
    }
    let history_rows = parent_events
        .iter()
        .map(|(run_id, event_id, data)| HistoryEventInsert {
            run_id,
            event_id: *event_id,
            data,
        })
        .collect::<Vec<_>>();
    insert_history_event_rows(tx, schema, &history_rows).await?;
    if parent_updates.is_empty() {
        return Ok(());
    }
    let run_ids = parent_updates
        .keys()
        .map(|run_id| run_id.0.clone())
        .collect::<Vec<_>>();
    let event_ids = parent_updates
        .values()
        .map(|(event_id, _)| i64::try_from(event_id.0).unwrap_or(i64::MAX))
        .collect::<Vec<_>>();
    let reasons = parent_updates
        .values()
        .map(|(_, reason)| reason_to_str(reason).to_owned())
        .collect::<Vec<_>>();
    tx.execute(
        &format!(
            "update {schema}.workflow_instances workflows
             set current_event_id = updates.current_event_id,
                 ready_reason = updates.ready_reason,
                 ready_at_ms = 0
             from unnest($1::text[], $2::bigint[], $3::text[])
                  as updates(run_id, current_event_id, ready_reason)
             where workflows.run_id = updates.run_id"
        ),
        &[&run_ids, &event_ids, &reasons],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn existing_child_terminal_command_ids_tx(
    tx: &Transaction<'_>,
    schema: &str,
    parent_run_id_values: &[String],
) -> Result<BTreeSet<CommandId>> {
    let child_event_types = vec![
        event_type_to_str(&HistoryEventType::ChildWorkflowCompleted),
        event_type_to_str(&HistoryEventType::ChildWorkflowFailed),
        event_type_to_str(&HistoryEventType::ChildWorkflowCancelled),
    ];
    existing_child_command_ids_by_types_tx(tx, schema, parent_run_id_values, child_event_types)
        .await
}

async fn cancel_children_for_parents_tx(
    tx: &Transaction<'_>,
    schema: &str,
    parent_run_ids: &[RunId],
) -> Result<()> {
    if parent_run_ids.is_empty() {
        return Ok(());
    }
    let parent_run_id_values = parent_run_ids
        .iter()
        .map(|run_id| run_id.0.clone())
        .collect::<Vec<_>>();
    let rows = tx
        .query(
            &format!(
                "select run_id, current_event_id, parent_run_id
                 from {schema}.workflow_instances
                 where parent_run_id = any($1::text[])
                   and parent_close_policy = $2
                   and terminal = false
                 order by run_id asc
                 for update"
            ),
            &[
                &parent_run_id_values,
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
                RunId::new(row.get::<_, String>(2)),
            )
        })
        .collect::<Vec<_>>();
    if children.is_empty() {
        return Ok(());
    }
    let child_events = children
        .iter()
        .map(|(child_run_id, tail, parent_run_id)| {
            (
                child_run_id.clone(),
                tail.next(),
                HistoryEventData::WorkflowCancelled {
                    reason: format!("parent workflow `{parent_run_id}` closed"),
                },
            )
        })
        .collect::<Vec<_>>();
    let history_rows = child_events
        .iter()
        .map(|(run_id, event_id, data)| HistoryEventInsert {
            run_id,
            event_id: *event_id,
            data,
        })
        .collect::<Vec<_>>();
    insert_history_event_rows(tx, schema, &history_rows).await?;
    let child_run_ids = children
        .iter()
        .map(|(child_run_id, _, _)| child_run_id.clone())
        .collect::<Vec<_>>();
    cleanup_runs_operational_state_tx(tx, schema, &child_run_ids, TerminalCleanup::Closed).await?;
    let child_run_id_values = child_events
        .iter()
        .map(|(run_id, _, _)| run_id.0.clone())
        .collect::<Vec<_>>();
    let event_ids = child_events
        .iter()
        .map(|(_, event_id, _)| i64::try_from(event_id.0).unwrap_or(i64::MAX))
        .collect::<Vec<_>>();
    tx.execute(
        &format!(
            "update {schema}.workflow_instances workflows
             set current_event_id = updates.current_event_id,
                 workflow_claim_token = null,
                 terminal = true,
                 ready_reason = null,
                 ready_at_ms = 0
             from unnest($1::text[], $2::bigint[]) as updates(run_id, current_event_id)
             where workflows.run_id = updates.run_id"
        ),
        &[&child_run_id_values, &event_ids],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn cancel_command_operational_state_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    command_id: &CommandId,
) -> Result<()> {
    let activity_id = ActivityId::new(command_id);
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 claim_token = null,
                 heartbeat_deadline_at_ms = null,
                 implicit_heartbeat_ms = null
             where activity_id = $1"
        ),
        &[&activity_id.0],
    )
    .await
    .map_err(postgres_error)?;
    // A cancelled map is a terminal map: the engine's `ParentCancelled`
    // transition tombstones its pending work and closes the descriptor, so
    // this path cannot drift from the fail-fast one.
    if let Some((state, namespace, task)) = activity_map_state_tx(tx, schema, command_id).await? {
        step_map_tx(
            backend,
            tx,
            schema,
            &state,
            &namespace,
            &MapTask::Activity(task),
            MapEvent::ParentCancelled,
        )
        .await?;
    }
    if let Some((state, namespace, task)) =
        child_workflow_map_state_tx(tx, schema, command_id).await?
    {
        if !state.completed {
            // Withdrawing the command on a still-live run means the
            // `ParentClosePolicy` path never runs, so without this the children
            // the map already started keep running with nothing waiting for
            // them. The engine cannot emit this: `ParentCancelled`'s other
            // producer is a run reaching a terminal event, where the children
            // belong to the close policy, which is free to abandon them.
            //
            // Children first, descriptor last, matching the fail-fast effect
            // order. That is a consistency choice, not a constraint: child
            // cancellation reads the child's parent link and terminal flag and
            // never the descriptor's state.
            cancel_child_workflow_map_children_tx(
                tx,
                schema,
                command_id,
                &map_command_cancelled_reason(command_id),
            )
            .await?;
        }
        step_map_tx(
            backend,
            tx,
            schema,
            &state,
            &namespace,
            &MapTask::ChildWorkflow(task),
            MapEvent::ParentCancelled,
        )
        .await?;
    }
    Ok(())
}

/// Postgres half of `sqlite::publish_commit_tail_before_map_append`; see there
/// for why this is conditional on the engine's completion predicate rather than
/// on a map merely being scheduled.
async fn publish_commit_tail_before_map_append_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    tail: EventId,
    state: &MapState,
    published: &mut bool,
) -> Result<()> {
    if *published || state.recorded_outcomes < state.item_count {
        return Ok(());
    }
    tx.execute(
        &format!(
            "update {schema}.workflow_instances
             set current_event_id = $1
             where run_id = $2"
        ),
        &[&i64::try_from(tail.0).unwrap_or(i64::MAX), &run_id.0],
    )
    .await
    .map_err(postgres_error)?;
    *published = true;
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

async fn next_run_ids(tx: &Transaction<'_>, schema: &str, count: usize) -> Result<Vec<RunId>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let rows = tx
        .query(
            &format!(
                "select nextval('{schema}.run_id_seq'::regclass)
                 from generate_series(1::bigint, $1::bigint)"
            ),
            &[&i64::try_from(count).unwrap_or(i64::MAX)],
        )
        .await
        .map_err(postgres_error)?;
    rows.into_iter()
        .map(|row| {
            let value: i64 = row.get(0);
            u64::try_from(value)
                .map(|value| RunId::new(format!("run-{value}")))
                .map_err(|_| {
                    Error::Backend(format!(
                        "postgres run id sequence returned invalid value {value}"
                    ))
                })
        })
        .collect()
}

fn has_duplicate_activity_completion_ids(completions: &[CompleteActivityRequest]) -> bool {
    let mut seen = BTreeSet::new();
    completions
        .iter()
        .any(|completion| !seen.insert(completion.claim.activity_id.0.as_str()))
}

fn is_activity_completion_item_error(err: &Error) -> bool {
    matches!(
        err,
        Error::StaleLease | Error::RunNotFound(_) | Error::TerminalWorkflow
    )
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
    let command_seq = data
        .command_seq()
        .map(|seq| i64::try_from(seq.0).unwrap_or(i64::MAX));
    let blob =
        rmp_serde::to_vec_named(&data).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        &format!(
            "insert into {schema}.history_events(run_id, event_id, event_type, command_seq, data)
             values ($1, $2, $3, $4, $5)"
        ),
        &[
            &run_id.0,
            &i64::try_from(event_id.0).unwrap_or(i64::MAX),
            &event_type,
            &command_seq,
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
    let command_seqs = events
        .iter()
        .map(|event| {
            event
                .data
                .command_seq()
                .map(|seq| i64::try_from(seq.0).unwrap_or(i64::MAX))
        })
        .collect::<Vec<_>>();
    let payloads = events
        .iter()
        .map(|event| rmp_serde::to_vec_named(event.data))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| Error::PayloadEncode(err.to_string()))?;

    tx.execute(
        &format!(
            "insert into {schema}.history_events(run_id, event_id, event_type, command_seq, data)
             select run_id, event_id, event_type, command_seq, data
             from unnest($1::text[], $2::bigint[], $3::text[], $4::bigint[], $5::bytea[])
                  as event_rows(run_id, event_id, event_type, command_seq, data)"
        ),
        &[&run_ids, &event_ids, &event_types, &command_seqs, &payloads],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

/// Which commits the set-based batch path can take, and — for `cancel_commands`
/// specifically — why the bail is left as it is.
///
/// **The decision is to leave it. The first version of this comment justified
/// that with a claim that was false, and the correction is the useful part.**
///
/// The claim was that Rust has no cost that grows with database size on this
/// path, every statement being a primary-key lookup, unlike TypeScript's
/// `canUseSqlNativeWorkflowCommit`, whose fallback rebuilds normalized state
/// over whole tables (9.5x at 10 runs, 42x at 200, recorded at
/// `packages/postgres/src/index.ts:5467`). Two statements *are* primary-key
/// lookups — `activity_map_state_tx` and `child_workflow_map_state_tx` both key
/// on `map_command_id`, a `primary key` column. But when the cancelled command
/// **is** a live child-workflow map, they return `Some` and
/// `cancel_child_workflow_map_children_tx` runs, and that query finds children
/// by `parent_run_id`/`parent_command_seq`. Until `idx_workflow_instances_parent`
/// was added next to the other indexes above, no index covered those columns
/// and it was a sequential scan of every workflow instance: measured 0.246 ms
/// at 1k rows, 1.210 ms at 10k, 8.637 ms at 100k, `Rows Removed by Filter`
/// tracking the table. Linear, on a hot path, and shared with the fail-fast map
/// effect applier — so it was never specific to cancellation.
///
/// The benchmark that produced the ratios below **did not reach that branch**:
/// its cancels named a command sequence that no map had ever used, so both
/// descriptor lookups missed and the children query never ran. What those
/// numbers measure is therefore the fixed cost of dropping one commit off the
/// set-based path onto the scalar one, which is real and is the common case,
/// but they were not evidence about scaling and the earlier version of this
/// comment presented them as if they were. They read flat across a 3,000-row
/// change because at that size the scan is a fraction of a millisecond against
/// a ~1.8 ms batch — below the noise, not absent.
///
/// The measurement that stands, batches of 32 commits, medians of 10 timed
/// batches after 4 warmup rounds, interleaved A/B in one process:
///
/// - every commit carrying a cancel: 19.0x / 20.5x / 18.7x at 0 extra runs and
///   19.2x / 18.1x / 21.1x at 3000;
/// - one cancel in 32, which is what a losing `select` branch produces:
///   1.63x / 1.67x / 1.70x at 0 and 1.63x / 1.68x / 1.65x at 3000.
///
/// So a losing-select cancel costs about one extra scalar commit, ~1.1 ms
/// against a ~1.8 ms batch. The bail stays for two reasons. Narrowing it to
/// admit plain-activity cancels would not remove the children query, because
/// that query runs whenever the cancelled command really is a map, on this path
/// and on the fail-fast one alike — the growth was never the bail's to fix, and
/// the index is what fixes it. And narrowing needs a lookup to tell an activity
/// id from a map descriptor id, which the commit alone cannot do, placing it on
/// the hottest path in the provider where a wrong answer silently skips a map
/// cancellation. Revisit if a workload appears where most commits in a batch
/// carry cancels; there the 19x figure applies, not the 1.65x one.
fn postgres_simple_batch_commit_eligible(commit: &WorkflowTaskCommit) -> bool {
    let has_terminal_event = postgres_simple_batch_commit_has_terminal_event(commit);
    commit.schedule_activity_maps.is_empty()
        && commit.schedule_child_workflow_maps.is_empty()
        && commit
            .start_child_workflows
            .iter()
            .all(|message| message.child_map_item.is_none())
        && !(has_terminal_event && !commit.start_child_workflows.is_empty())
        && commit.cancel_commands.is_empty()
        && commit
            .append_events
            .iter()
            .all(|event| postgres_simple_batch_history_event_eligible(&event.data))
}

fn postgres_simple_batch_commit_has_terminal_event(commit: &WorkflowTaskCommit) -> bool {
    commit
        .append_events
        .iter()
        .any(|event| postgres_simple_batch_terminal_event_eligible(&event.data))
}

fn postgres_simple_batch_history_event_eligible(data: &HistoryEventData) -> bool {
    postgres_simple_batch_terminal_event_eligible(data)
        || (!is_terminal(data)
            && !matches!(
                data,
                HistoryEventData::VersionMarker(_) | HistoryEventData::DeprecatedPatchMarker(_)
            ))
}

fn postgres_simple_batch_terminal_event_eligible(data: &HistoryEventData) -> bool {
    matches!(
        data,
        HistoryEventData::WorkflowCompleted { .. }
            | HistoryEventData::WorkflowFailed { .. }
            | HistoryEventData::WorkflowCancelled { .. }
    )
}

async fn insert_activity_task_rows_for_simple_commits_tx(
    tx: &Transaction<'_>,
    schema: &str,
    commits: &[PreparedSimpleWorkflowCommit],
) -> Result<()> {
    let mut activity_ids = Vec::new();
    let mut namespaces = Vec::new();
    let mut run_ids = Vec::new();
    let mut activity_names = Vec::new();
    let mut task_queues = Vec::new();
    let mut task_blobs = Vec::new();
    let mut timeout_at_ms = Vec::new();
    for commit in commits {
        for task in &commit.schedule_activities {
            activity_ids.push(task.activity_id.0.clone());
            namespaces.push(commit.namespace.clone());
            run_ids.push(task.run_id.0.clone());
            activity_names.push(task.activity_name.0.clone());
            task_queues.push(task.task_queue.0.clone());
            task_blobs.push(
                rmp_serde::to_vec_named(task)
                    .map_err(|err| Error::PayloadEncode(err.to_string()))?,
            );
            timeout_at_ms.push(activity_timeout_at_ms(task.start_to_close_timeout));
        }
    }
    if activity_ids.is_empty() {
        return Ok(());
    }
    tx.execute(
        &format!(
            "insert into {schema}.activity_tasks
             (activity_id, namespace, run_id, activity_name, task_queue, task,
              claim_token, completed, timeout_at_ms, heartbeat_deadline_at_ms)
             select activity_id, namespace, run_id, activity_name, task_queue, task,
                    null, false, timeout_at_ms, null
             from unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
                         $6::bytea[], $7::bigint[])
                  as task_rows(activity_id, namespace, run_id, activity_name, task_queue, task,
                               timeout_at_ms)"
        ),
        &[
            &activity_ids,
            &namespaces,
            &run_ids,
            &activity_names,
            &task_queues,
            &task_blobs,
            &timeout_at_ms,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn upsert_wait_rows_for_simple_commits_tx(
    tx: &Transaction<'_>,
    schema: &str,
    commits: &[PreparedSimpleWorkflowCommit],
) -> Result<()> {
    let mut wait_ids = Vec::new();
    let mut namespaces = Vec::new();
    let mut run_ids = Vec::new();
    let mut command_seqs = Vec::new();
    let mut kinds = Vec::new();
    let mut keys = Vec::new();
    let mut ready_at_ms = Vec::new();
    for commit in commits {
        for wait in &commit.upsert_waits {
            wait_ids.push(wait.wait_id.0.clone());
            namespaces.push(commit.namespace.clone());
            run_ids.push(wait.run_id.0.clone());
            command_seqs.push(i64::try_from(wait.command_id.seq.0).unwrap_or(i64::MAX));
            kinds.push(wait_kind_to_str(&wait.kind).to_owned());
            keys.push(wait.key.clone());
            ready_at_ms.push(wait.ready_at.map(|ready_at| ready_at.0));
        }
    }
    if wait_ids.is_empty() {
        return Ok(());
    }
    tx.execute(
        &format!(
            "insert into {schema}.active_waits
             (wait_id, namespace, run_id, command_seq, kind, wait_key, ready_at_ms)
             select wait_id, namespace, run_id, command_seq, kind, wait_key, ready_at_ms
             from unnest($1::text[], $2::text[], $3::text[], $4::bigint[],
                         $5::text[], $6::text[], $7::bigint[])
                  as wait_rows(wait_id, namespace, run_id, command_seq, kind, wait_key,
                               ready_at_ms)
             on conflict(wait_id) do update set
                namespace = excluded.namespace,
                run_id = excluded.run_id,
                command_seq = excluded.command_seq,
                kind = excluded.kind,
                wait_key = excluded.wait_key,
                ready_at_ms = excluded.ready_at_ms"
        ),
        &[
            &wait_ids,
            &namespaces,
            &run_ids,
            &command_seqs,
            &kinds,
            &keys,
            &ready_at_ms,
        ],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn mark_signal_rows_consumed_for_simple_commits_tx(
    tx: &Transaction<'_>,
    schema: &str,
    commits: &[PreparedSimpleWorkflowCommit],
) -> Result<()> {
    let signal_ids = commits
        .iter()
        .flat_map(|commit| {
            commit
                .consume_signals
                .iter()
                .map(|signal_id| signal_id.0.clone())
        })
        .collect::<Vec<_>>();
    if signal_ids.is_empty() {
        return Ok(());
    }
    tx.execute(
        &format!("update {schema}.signals set consumed = true where signal_id = any($1::text[])"),
        &[&signal_ids],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn delete_wait_rows_for_simple_commits_tx(
    tx: &Transaction<'_>,
    schema: &str,
    commits: &[PreparedSimpleWorkflowCommit],
) -> Result<()> {
    let wait_ids = commits
        .iter()
        .flat_map(|commit| commit.delete_waits.iter().map(|wait_id| wait_id.0.clone()))
        .collect::<Vec<_>>();
    if wait_ids.is_empty() {
        return Ok(());
    }
    tx.execute(
        &format!("delete from {schema}.active_waits where wait_id = any($1::text[])"),
        &[&wait_ids],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn upsert_query_projection_rows_for_simple_commits_tx(
    tx: &Transaction<'_>,
    schema: &str,
    commits: &[PreparedSimpleWorkflowCommit],
) -> Result<()> {
    let mut namespaces = Vec::new();
    let mut workflow_ids = Vec::new();
    let mut run_ids = Vec::new();
    let mut event_ids = Vec::new();
    let mut payloads = Vec::new();
    for commit in commits {
        let Some(payload) = &commit.query_projection else {
            continue;
        };
        namespaces.push(commit.namespace.clone());
        workflow_ids.push(commit.workflow_id.clone());
        run_ids.push(commit.claim.run_id.0.clone());
        event_ids.push(i64::try_from(commit.next_event_id.0).unwrap_or(i64::MAX));
        payloads.push(
            rmp_serde::to_vec_named(payload)
                .map_err(|err| Error::PayloadEncode(err.to_string()))?,
        );
    }
    if namespaces.is_empty() {
        return Ok(());
    }
    tx.execute(
        &format!(
            "insert into {schema}.query_projections
             (namespace, workflow_id, run_id, event_id, payload)
             select namespace, workflow_id, run_id, event_id, payload
             from unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::bytea[])
                  as projection_rows(namespace, workflow_id, run_id, event_id, payload)
             on conflict(namespace, workflow_id) do update set
                run_id = excluded.run_id,
                event_id = excluded.event_id,
                payload = excluded.payload"
        ),
        &[&namespaces, &workflow_ids, &run_ids, &event_ids, &payloads],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

enum InlineChildStartOutcome {
    Started(RunId),
    Failed(DurableFailure),
    /// The `workflow_instances` insert conflicted and the follow-up re-read
    /// found nothing: something deleted that row, and committed, between two
    /// statements of this transaction.
    ///
    /// Named for what it *is*, not "skipped", because the previous name was
    /// shared with the ordinary "this child event already exists" case and the
    /// two were then handled together — silently. Nothing in this crate deletes
    /// a `workflow_instances` row, so this is unreachable without an external
    /// writer, but every caller must raise it rather than continue: the child
    /// was not started and no outcome was recorded, so a plain child start
    /// leaves its parent waiting forever and a map item strands its ordinal.
    Vanished,
}

/// The error every `InlineChildStartOutcome::Vanished` site raises, written
/// once so the three call sites cannot drift or quietly stop raising.
///
/// Failing the transaction is also the recovery: the commit rolls back and its
/// retry re-runs the insert, which now finds no conflicting row and starts the
/// child properly.
fn vanished_child_start_error(message: &ChildStartOutboxMessage) -> Error {
    match &message.child_map_item {
        Some(item) => Error::Backend(format!(
            "child workflow map `{}`:{} item {} could not be started: \
             workflow instance `{}` was deleted mid-transaction",
            item.map_command_id.run_id,
            item.map_command_id.seq.0,
            item.item_ordinal,
            message.workflow_id
        )),
        None => Error::Backend(format!(
            "child workflow `{}`:{} could not be started: \
             workflow instance `{}` was deleted mid-transaction",
            message.command_id.run_id, message.command_id.seq.0, message.workflow_id
        )),
    }
}

struct ExistingChildWorkflowRow {
    run_id: RunId,
    parent_run_id: Option<String>,
    parent_command_seq: Option<i64>,
    parent_child_map_ordinal: Option<i64>,
}

fn child_start_outcome_event_and_reason(
    message: &ChildStartOutboxMessage,
    outcome: InlineChildStartOutcome,
) -> Result<(HistoryEventData, WorkflowTaskReason)> {
    match outcome {
        InlineChildStartOutcome::Started(child_run_id) => Ok((
            HistoryEventData::ChildWorkflowStarted(crate::ChildWorkflowStarted {
                command_id: message.command_id.clone(),
                workflow_id: message.workflow_id.clone(),
                run_id: child_run_id,
            }),
            WorkflowTaskReason::ChildWorkflowStarted,
        )),
        InlineChildStartOutcome::Failed(failure) => Ok((
            HistoryEventData::ChildWorkflowFailed(crate::ChildWorkflowFailed {
                command_id: message.command_id.clone(),
                failure,
            }),
            WorkflowTaskReason::ChildWorkflowFailed,
        )),
        InlineChildStartOutcome::Vanished => Err(vanished_child_start_error(message)),
    }
}

async fn existing_child_workflows_for_keys_tx(
    tx: &Transaction<'_>,
    schema: &str,
    keys: &BTreeSet<(String, String)>,
) -> Result<BTreeMap<(String, String), ExistingChildWorkflowRow>> {
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    let namespaces = keys
        .iter()
        .map(|(namespace, _)| namespace.clone())
        .collect::<Vec<_>>();
    let workflow_ids = keys
        .iter()
        .map(|(_, workflow_id)| workflow_id.clone())
        .collect::<Vec<_>>();
    let rows = tx
        .query(
            &format!(
                "select workflows.namespace, workflows.workflow_id, workflows.run_id,
                        workflows.parent_run_id, workflows.parent_command_seq,
                        workflows.parent_child_map_ordinal
                 from {schema}.workflow_instances workflows
                 join unnest($1::text[], $2::text[]) as keys(namespace, workflow_id)
                   on keys.namespace = workflows.namespace
                  and keys.workflow_id = workflows.workflow_id
                 for update"
            ),
            &[&namespaces, &workflow_ids],
        )
        .await
        .map_err(postgres_error)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                (row.get::<_, String>(0), row.get::<_, String>(1)),
                ExistingChildWorkflowRow {
                    run_id: RunId::new(row.get::<_, String>(2)),
                    parent_run_id: row.get(3),
                    parent_command_seq: row.get(4),
                    parent_child_map_ordinal: row.get(5),
                },
            )
        })
        .collect())
}

async fn existing_child_command_ids_tx(
    tx: &Transaction<'_>,
    schema: &str,
    parent_run_id_values: &[String],
) -> Result<BTreeSet<CommandId>> {
    let child_event_types = vec![
        event_type_to_str(&HistoryEventType::ChildWorkflowStarted),
        event_type_to_str(&HistoryEventType::ChildWorkflowCompleted),
        event_type_to_str(&HistoryEventType::ChildWorkflowFailed),
        event_type_to_str(&HistoryEventType::ChildWorkflowCancelled),
    ];
    existing_child_command_ids_by_types_tx(tx, schema, parent_run_id_values, child_event_types)
        .await
}

// Reads the indexed `command_seq` column instead of decoding event payloads;
// child lifecycle events within one run identify their command by sequence
// alone, so (run_id, command_seq) reconstructs the command id.
async fn existing_child_command_ids_by_types_tx(
    tx: &Transaction<'_>,
    schema: &str,
    parent_run_id_values: &[String],
    child_event_types: Vec<&'static str>,
) -> Result<BTreeSet<CommandId>> {
    if parent_run_id_values.is_empty() {
        return Ok(BTreeSet::new());
    }
    let rows = tx
        .query(
            &format!(
                "select run_id, command_seq
                 from {schema}.history_events
                 where run_id = any($1::text[])
                   and event_type = any($2::text[])
                   and command_seq is not null"
            ),
            &[&parent_run_id_values, &child_event_types],
        )
        .await
        .map_err(postgres_error)?;
    let mut command_ids = BTreeSet::new();
    for row in rows {
        let run_id = RunId::new(row.get::<_, String>(0));
        let seq = u64::try_from(row.get::<_, i64>(1)).unwrap_or(u64::MAX);
        command_ids.insert(CommandId {
            run_id,
            seq: CommandSeq(seq),
        });
    }
    Ok(command_ids)
}

async fn start_child_workflow_inline_tx(
    tx: &Transaction<'_>,
    schema: &str,
    namespace: &str,
    shard_id: i32,
    message: &ChildStartOutboxMessage,
) -> Result<InlineChildStartOutcome> {
    let run_id = next_run_id(tx, schema).await?;
    let parent_child_map_ordinal = message
        .child_map_item
        .as_ref()
        .map(|item| i64::try_from(item.item_ordinal).unwrap_or(i64::MAX));
    let inserted = tx
        .query_opt(
            &format!(
                "insert into {schema}.workflow_instances
                 (namespace, workflow_id, run_id, shard_id, workflow_name, workflow_version, task_queue,
                  current_event_id, ready_reason, ready_at_ms, workflow_claim_token, terminal,
                  parent_run_id, parent_command_seq, parent_close_policy, parent_child_map_ordinal)
                 values ($1, $2, $3, $4, $5, $6, $7, 1, $8, 0, null, false, $9, $10, $11, $12)
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
                &parent_child_map_ordinal,
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
                "select run_id, parent_run_id, parent_command_seq, parent_child_map_ordinal
                 from {schema}.workflow_instances
                 where namespace = $1 and workflow_id = $2
                 for update"
            ),
            &[&namespace, &message.workflow_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(InlineChildStartOutcome::Vanished);
    };
    let existing_run_id = RunId::new(row.get::<_, String>(0));
    let parent_run_id: Option<String> = row.get(1);
    let parent_command_seq: Option<i64> = row.get(2);
    let parent_map_ordinal: Option<i64> = row.get(3);
    let expected_map_ordinal = message
        .child_map_item
        .as_ref()
        .map(|item| i64::try_from(item.item_ordinal).unwrap_or(i64::MAX));
    let same_child = parent_run_id.as_deref() == Some(message.command_id.run_id.0.as_str())
        && parent_command_seq.and_then(|seq| u64::try_from(seq).ok())
            == Some(message.command_id.seq.0)
        && parent_map_ordinal == expected_map_ordinal;
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
    let row = tx
        .query_opt(
            &format!(
                "select 1
                 from {schema}.history_events
                 where run_id = $1
                   and command_seq = $2
                   and event_type = any($3::text[])
                 limit 1"
            ),
            &[
                &command_id.run_id.0,
                &i64::try_from(command_id.seq.0).unwrap_or(i64::MAX),
                &child_event_types,
            ],
        )
        .await
        .map_err(postgres_error)?;
    Ok(row.is_some())
}

async fn insert_activity_map_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    namespace: &str,
    map_task: &ActivityMapTask,
) -> Result<()> {
    validate_map_slot_bound(ACTIVITY_MAP_LABEL, map_task.max_in_flight)?;
    let manifest_payload = backend
        .hydrate_activity_map_input_manifest_from_storage_tx(tx, map_task.input_manifest.clone())
        .await?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
    let task_blob =
        rmp_serde::to_vec_named(map_task).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    let inserted = tx
        .execute(
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
    // `do nothing` makes the insert genuinely idempotent, which is the right
    // defensive choice — but it also means a conflicting row would leave this
    // function reporting success against a descriptor it did not write. That
    // became consequential when `DescriptorCreated` gained the ability to append
    // a history fact: Postgres would step a *stale* descriptor where SQLite's
    // plain insert raises. Unreachable behind `expected_tail_event_id`, which
    // fences a replayed commit before it gets here, so this is a tripwire for an
    // invariant violation rather than a race to be handled.
    if inserted != 1 {
        return Err(Error::Backend(format!(
            "map descriptor `{}`:{} already exists: a workflow task commit re-scheduled a map \
             whose descriptor was never deleted",
            map_task.map_command_id.run_id, map_task.map_command_id.seq.0
        )));
    }
    Ok(())
}

/// The activity-map descriptor as [`crate::map_engine`] sees it, plus the
/// decoded task the effect appliers need. The row is locked `for update`, so
/// every transition against one descriptor serializes. `None` when the row is
/// gone, which means the run's terminal cleanup deleted it.
async fn activity_map_state_tx(
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
) -> Result<Option<(MapState, String, ActivityMapTask)>> {
    let key = map_command_key(map_command_id);
    let Some(row) = tx
        .query_opt(
            &format!(
                "select m.namespace, m.task, m.item_count, m.next_ordinal, m.in_flight,
                        m.completed,
                        (select count(*) from {schema}.activity_map_results r
                          where r.map_command_id = m.map_command_id)
                 from {schema}.activity_maps m
                 where m.map_command_id = $1
                 for update"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(None);
    };
    let namespace: String = row.get(0);
    let task_blob: Vec<u8> = row.get(1);
    let task: ActivityMapTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let state = MapState {
        map_command_id: map_command_id.clone(),
        kind: MapKind::Activity,
        // An activity map is always fail-fast; the field is inert for it.
        failure_mode: ChildWorkflowMapFailureMode::FailFast,
        item_count: u64::try_from(row.get::<_, i64>(2)).unwrap_or(u64::MAX),
        next_ordinal: u64::try_from(row.get::<_, i64>(3)).unwrap_or(u64::MAX),
        in_flight: u64::try_from(row.get::<_, i64>(4)).unwrap_or(u64::MAX),
        max_in_flight: task.max_in_flight,
        recorded_outcomes: u64::try_from(row.get::<_, i64>(6)).unwrap_or(u64::MAX),
        completed: row.get(5),
    };
    Ok(Some((state, namespace, task)))
}

/// The child-workflow-map descriptor as [`crate::map_engine`] sees it.
async fn child_workflow_map_state_tx(
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
) -> Result<Option<(MapState, String, ChildWorkflowMapTask)>> {
    let key = map_command_key(map_command_id);
    let Some(row) = tx
        .query_opt(
            &format!(
                "select m.namespace, m.task, m.item_count, m.next_ordinal, m.in_flight,
                        m.completed,
                        (select count(*) from {schema}.child_workflow_map_results r
                          where r.map_command_id = m.map_command_id)
                 from {schema}.child_workflow_maps m
                 where m.map_command_id = $1
                 for update"
            ),
            &[&key],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(None);
    };
    let namespace: String = row.get(0);
    let task_blob: Vec<u8> = row.get(1);
    let task: ChildWorkflowMapTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let state = MapState {
        map_command_id: map_command_id.clone(),
        kind: MapKind::ChildWorkflow,
        failure_mode: task.failure_mode,
        item_count: u64::try_from(row.get::<_, i64>(2)).unwrap_or(u64::MAX),
        next_ordinal: u64::try_from(row.get::<_, i64>(3)).unwrap_or(u64::MAX),
        in_flight: u64::try_from(row.get::<_, i64>(4)).unwrap_or(u64::MAX),
        max_in_flight: task.max_in_flight,
        recorded_outcomes: u64::try_from(row.get::<_, i64>(6)).unwrap_or(u64::MAX),
        completed: row.get(5),
    };
    Ok(Some((state, namespace, task)))
}

/// A rejected transition never reaches storage: the caller returns this error
/// and the transaction rolls back, so no effect from the same event lands.
fn map_reject_error(kind: MapKind, reject: MapReject) -> Error {
    match reject {
        MapReject::OutOfBounds { ordinal } => match kind {
            MapKind::Activity => {
                Error::Backend(format!("activity map item ordinal {ordinal} out of bounds"))
            }
            MapKind::ChildWorkflow => Error::Backend(format!(
                "child workflow map item ordinal {ordinal} out of bounds"
            )),
        },
        MapReject::TerminalParent => Error::TerminalWorkflow,
    }
}

/// Everything an effect applier needs that the effect list does not carry.
enum MapTask {
    Activity(ActivityMapTask),
    ChildWorkflow(ChildWorkflowMapTask),
}

impl MapTask {
    fn kind(&self) -> MapKind {
        match self {
            Self::Activity(_) => MapKind::Activity,
            Self::ChildWorkflow(_) => MapKind::ChildWorkflow,
        }
    }

    fn input_manifest(&self) -> &PayloadRef {
        match self {
            Self::Activity(task) => &task.input_manifest,
            Self::ChildWorkflow(task) => &task.input_manifest,
        }
    }

    fn table(&self) -> &'static str {
        match self {
            Self::Activity(_) => "activity_maps",
            Self::ChildWorkflow(_) => "child_workflow_maps",
        }
    }
}

/// Run one map transition: project the descriptor, ask the engine, apply the
/// effects it returns inside the caller's transaction. Returns the parent
/// event the transition appended, if any.
async fn step_map_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    state: &MapState,
    namespace: &str,
    task: &MapTask,
    event: MapEvent,
) -> Result<Option<EventId>> {
    let effects = crate::map_engine::step(state, event)
        .map_err(|reject| map_reject_error(state.kind, reject))?;
    apply_map_effects_tx(
        backend,
        tx,
        schema,
        &state.map_command_id,
        namespace,
        task,
        effects,
    )
    .await
}

/// Apply an engine effect list in order. Each arm is one storage primitive; no
/// arm decides anything the engine already decided.
async fn apply_map_effects_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
    namespace: &str,
    task: &MapTask,
    effects: Vec<MapEffect>,
) -> Result<Option<EventId>> {
    let key = map_command_key(map_command_id);
    let mut appended = None;
    // Inline child starts that lost an id race. Their ordinals already took a
    // slot (D7: materialization always takes the slot), so each is routed back
    // through the engine as a failed item once the batch is fully applied,
    // which is what releases the slot exactly once.
    let mut id_conflicts = Vec::new();
    for effect in effects {
        match effect {
            MapEffect::RecordItemOutcome { ordinal, outcome } => {
                let outcome = backend
                    .normalize_child_workflow_map_outcome_for_storage_tx(tx, outcome)
                    .await?;
                let blob = rmp_serde::to_vec_named(&outcome)
                    .map_err(|err| Error::PayloadEncode(err.to_string()))?;
                tx.execute(
                    &format!(
                        "insert into {schema}.child_workflow_map_results
                           (map_command_id, item_ordinal, outcome)
                         values ($1, $2, $3)
                         on conflict(map_command_id, item_ordinal) do nothing"
                    ),
                    &[&key, &i64::try_from(ordinal).unwrap_or(i64::MAX), &blob],
                )
                .await
                .map_err(postgres_error)?;
            }
            MapEffect::MaterializeItems {
                first_ordinal,
                count,
            } => {
                id_conflicts.extend(
                    insert_map_item_batch_tx(
                        backend,
                        tx,
                        schema,
                        map_command_id,
                        namespace,
                        task,
                        first_ordinal,
                        count,
                    )
                    .await?,
                );
            }
            MapEffect::AdvanceDescriptor {
                next_ordinal,
                in_flight,
            } => {
                let table = task.table();
                tx.execute(
                    &format!(
                        "update {schema}.{table}
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
            }
            MapEffect::ScheduleItemRetry {
                ordinal,
                next_attempt,
                visible_at_ms,
                timeout_at_ms,
            } => {
                schedule_map_item_retry_tx(
                    tx,
                    schema,
                    map_command_id,
                    ordinal,
                    next_attempt,
                    visible_at_ms,
                    timeout_at_ms,
                )
                .await?;
            }
            MapEffect::CompleteMap { item_count } => {
                appended = Some(
                    complete_map_tx(backend, tx, schema, map_command_id, task, item_count).await?,
                );
            }
            MapEffect::FailMap { failure } => {
                appended = Some(
                    fail_map_tx(backend, tx, schema, map_command_id, task.kind(), failure).await?,
                );
            }
            MapEffect::AbandonPendingItems => {
                abandon_pending_map_items_tx(tx, schema, map_command_id, task.kind()).await?;
            }
            MapEffect::CancelChildren { reason } => {
                cancel_child_workflow_map_children_tx(tx, schema, map_command_id, &reason).await?;
            }
            MapEffect::MarkDescriptorTerminal => {
                let table = task.table();
                tx.execute(
                    &format!(
                        "update {schema}.{table}
                         set completed = true, in_flight = 0
                         where map_command_id = $1"
                    ),
                    &[&key],
                )
                .await
                .map_err(postgres_error)?;
            }
        }
    }

    for (ordinal, failure) in id_conflicts {
        Box::pin(complete_child_workflow_map_item_tx(
            backend,
            tx,
            schema,
            ChildWorkflowMapItem {
                map_command_id: map_command_id.clone(),
                item_ordinal: ordinal,
            },
            ChildWorkflowMapItemOutcome::Failed { failure },
        ))
        .await?;
    }
    Ok(appended)
}

/// Storage primitive behind [`MapEffect::MaterializeItems`]: read the manifest
/// entries for `[first_ordinal, first_ordinal + count)` and admit them.
///
/// An activity map admits the whole range with **one** set-based insert over
/// `unnest`, which is the entire reason the effect carries a contiguous range
/// instead of one ordinal: a 10,000-item batch is one statement, not 10,000
/// round trips. A child map starts one real workflow per ordinal — an insert
/// plus its `WorkflowStarted` event plus an id-collision check — so it stays
/// per item and returns the ordinals whose id was already taken.
#[allow(clippy::too_many_arguments)]
async fn insert_map_item_batch_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
    namespace: &str,
    task: &MapTask,
    first_ordinal: u64,
    count: u64,
) -> Result<Vec<(u64, DurableFailure)>> {
    let manifest_payload = backend
        .hydrate_activity_map_input_manifest_from_storage_tx(tx, task.input_manifest().clone())
        .await?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
    let ordinals = first_ordinal..first_ordinal.saturating_add(count);

    match task {
        MapTask::Activity(map_task) => {
            let mut activity_ids: Vec<String> = Vec::new();
            let mut task_blobs: Vec<Vec<u8>> = Vec::new();
            for item_ordinal in ordinals {
                let input = activity_map_input_at(&manifest, item_ordinal)?;
                let activity_id = ActivityId::map_item(map_command_id, item_ordinal);
                let item_task = ActivityTask {
                    activity_id: activity_id.clone(),
                    run_id: map_command_id.run_id.clone(),
                    command_id: map_command_id.clone(),
                    activity_name: map_task.activity_name.clone(),
                    task_queue: map_task.task_queue.clone(),
                    retry_policy: map_task.retry_policy.clone(),
                    start_to_close_timeout: map_task.start_to_close_timeout,
                    heartbeat_timeout: map_task.heartbeat_timeout,
                    attempt: 1,
                    input,
                    map_item: Some(ActivityMapItem {
                        map_command_id: map_command_id.clone(),
                        item_ordinal,
                    }),
                };
                let item_task = backend
                    .normalize_activity_task_for_storage_tx(tx, item_task)
                    .await?;
                task_blobs.push(
                    rmp_serde::to_vec_named(&item_task)
                        .map_err(|err| Error::PayloadEncode(err.to_string()))?,
                );
                activity_ids.push(activity_id.0);
            }
            if activity_ids.is_empty() {
                return Ok(Vec::new());
            }
            // Every column but `activity_id` and `task` is identical across a
            // map's items, so the batch needs exactly two arrays.
            tx.execute(
                &format!(
                    "insert into {schema}.activity_tasks
                     (activity_id, namespace, run_id, activity_name, task_queue, task,
                      claim_token, completed, timeout_at_ms, heartbeat_deadline_at_ms)
                     select item.activity_id, $1, $2, $3, $4, item.task, null, false, $5, null
                     from unnest($6::text[], $7::bytea[]) as item(activity_id, task)
                     on conflict(activity_id) do nothing"
                ),
                &[
                    &namespace,
                    &map_command_id.run_id.0,
                    &map_task.activity_name.0,
                    &map_task.task_queue.0,
                    &activity_timeout_at_ms(map_task.start_to_close_timeout),
                    &activity_ids,
                    &task_blobs,
                ],
            )
            .await
            .map_err(postgres_error)?;
            Ok(Vec::new())
        }
        MapTask::ChildWorkflow(map_task) => {
            let mut conflicts = Vec::new();
            for item_ordinal in ordinals {
                let input = activity_map_input_at(&manifest, item_ordinal)?;
                let message = ChildStartOutboxMessage {
                    command_id: map_command_id.clone(),
                    workflow_type: map_task.workflow_type.clone(),
                    workflow_id: WorkflowId::new(format!(
                        "{}/{}",
                        map_task.workflow_id_prefix, item_ordinal
                    )),
                    task_queue: map_task.task_queue.clone(),
                    input,
                    parent_close_policy: map_task.parent_close_policy,
                    child_map_item: Some(ChildWorkflowMapItem {
                        map_command_id: map_command_id.clone(),
                        item_ordinal,
                    }),
                };
                let message = backend
                    .normalize_child_start_message_for_storage_tx(tx, message)
                    .await?;
                let child_shard_id = i32::try_from(
                    backend
                        .shard_for_workflow(
                            &Namespace::new(namespace.to_owned()),
                            &message.workflow_id,
                        )
                        .0,
                )
                .unwrap_or(i32::MAX);
                let start =
                    start_child_workflow_inline_tx(tx, schema, namespace, child_shard_id, &message)
                        .await?;
                match start {
                    InlineChildStartOutcome::Started(_) => {}
                    InlineChildStartOutcome::Failed(failure) => {
                        conflicts.push((item_ordinal, failure));
                    }
                    // Materialization has already taken this ordinal's slot
                    // (D7), so ignoring the outcome would leave an ordinal with
                    // no child, no outcome and no slot release: `outcome_count`
                    // could never reach `item_count` and the map would hang
                    // forever with nothing to observe. See
                    // `InlineChildStartOutcome::Vanished`.
                    InlineChildStartOutcome::Vanished => {
                        return Err(vanished_child_start_error(&message));
                    }
                }
            }
            Ok(conflicts)
        }
    }
}

/// [`MapEffect::ScheduleItemRetry`]: release the claim, clear both heartbeat
/// fields, and restamp the deadlines of one item activity task.
async fn schedule_map_item_retry_tx(
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
    ordinal: u64,
    next_attempt: u32,
    visible_at_ms: Option<i64>,
    timeout_at_ms: Option<i64>,
) -> Result<()> {
    let activity_id = ActivityId::map_item(map_command_id, ordinal);
    let Some(row) = tx
        .query_opt(
            &format!("select task from {schema}.activity_tasks where activity_id = $1"),
            &[&activity_id.0],
        )
        .await
        .map_err(postgres_error)?
    else {
        return Ok(());
    };
    let task_blob: Vec<u8> = row.get(0);
    let mut task: ActivityTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    task.attempt = next_attempt;
    let task_blob =
        rmp_serde::to_vec_named(&task).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set task = $1,
                 claim_token = null,
                 visible_at_ms = $2,
                 timeout_at_ms = $3,
                 heartbeat_deadline_at_ms = null,
                 implicit_heartbeat_ms = null
             where activity_id = $4"
        ),
        &[&task_blob, &visible_at_ms, &timeout_at_ms, &activity_id.0],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

/// [`MapEffect::AbandonPendingItems`]: tombstone every not-yet-terminal item
/// task of a map that is over, so neither the claim path nor the timeout
/// scanner can resurrect one. The claim-time guard that skips items of a
/// completed map stays as well: a claim already in flight when the map ended
/// must not change the answer. A child map has nothing to tombstone here —
/// this provider starts map children inline rather than through the outbox, so
/// it has no undispatched item starts.
async fn abandon_pending_map_items_tx(
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
    kind: MapKind,
) -> Result<()> {
    if kind != MapKind::Activity {
        return Ok(());
    }
    let map_prefix = format!("{}:map:%", ActivityId::new(map_command_id).0);
    tx.execute(
        &format!(
            "update {schema}.activity_tasks
             set completed = true,
                 claim_token = null,
                 heartbeat_deadline_at_ms = null,
                 implicit_heartbeat_ms = null
             where activity_id like $1 and completed = false"
        ),
        &[&map_prefix],
    )
    .await
    .map_err(postgres_error)?;
    Ok(())
}

async fn insert_child_workflow_map_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    namespace: &str,
    map_task: &ChildWorkflowMapTask,
) -> Result<()> {
    validate_map_slot_bound(CHILD_WORKFLOW_MAP_LABEL, map_task.max_in_flight)?;
    let manifest_payload = backend
        .hydrate_activity_map_input_manifest_from_storage_tx(tx, map_task.input_manifest.clone())
        .await?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&manifest_payload)?;
    let task_blob =
        rmp_serde::to_vec_named(map_task).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    let inserted = tx
        .execute(
            &format!(
                "insert into {schema}.child_workflow_maps
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
    // `do nothing` makes the insert genuinely idempotent, which is the right
    // defensive choice — but it also means a conflicting row would leave this
    // function reporting success against a descriptor it did not write. That
    // became consequential when `DescriptorCreated` gained the ability to append
    // a history fact: Postgres would step a *stale* descriptor where SQLite's
    // plain insert raises. Unreachable behind `expected_tail_event_id`, which
    // fences a replayed commit before it gets here, so this is a tripwire for an
    // invariant violation rather than a race to be handled.
    if inserted != 1 {
        return Err(Error::Backend(format!(
            "map descriptor `{}`:{} already exists: a workflow task commit re-scheduled a map \
             whose descriptor was never deleted",
            map_task.map_command_id.run_id, map_task.map_command_id.seq.0
        )));
    }
    Ok(())
}

async fn complete_child_workflow_map_item_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    map_item: ChildWorkflowMapItem,
    outcome: ChildWorkflowMapItemOutcome,
) -> Result<()> {
    let Some((state, namespace, map_task)) =
        child_workflow_map_state_tx(tx, schema, &map_item.map_command_id).await?
    else {
        // Missing descriptor means the parent run's terminal cleanup deleted
        // it, exactly as a missing activity record does on `complete_activity`
        // and `fail_activity`, and as a missing activity-map descriptor does on
        // `fail_map_item`. It is not an error: under
        // `ParentClosePolicy::Abandon` a child of a closed parent's map keeps
        // running by design and still has to be able to *finish*. Raising here
        // rolled the child's own terminal commit back, and every retry hit the
        // same missing descriptor, so the child could never terminate.
        return Ok(());
    };
    let already_recorded = tx
        .query_opt(
            &format!(
                "select 1 from {schema}.child_workflow_map_results
                 where map_command_id = $1 and item_ordinal = $2"
            ),
            &[
                &map_command_key(&map_item.map_command_id),
                &i64::try_from(map_item.item_ordinal).unwrap_or(i64::MAX),
            ],
        )
        .await
        .map_err(postgres_error)?
        .is_some();
    let parent_terminal =
        parent_run_terminal_tx(tx, schema, &map_item.map_command_id.run_id).await?;
    step_map_tx(
        backend,
        tx,
        schema,
        &state,
        &namespace,
        &MapTask::ChildWorkflow(map_task),
        MapEvent::ItemCompleted {
            ordinal: map_item.item_ordinal,
            outcome,
            already_recorded,
            parent_terminal,
        },
    )
    .await?;
    Ok(())
}

/// Whether the map's parent run is already closed. A missing run is an error
/// rather than "closed": the descriptor exists, so the run should too. The row
/// is locked because the transition that follows may append to its history.
async fn parent_run_terminal_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
) -> Result<bool> {
    let Some((_, terminal)) = parent_tail_and_terminal_tx(tx, schema, run_id).await? else {
        return Err(Error::RunNotFound(run_id.clone()));
    };
    Ok(terminal)
}

async fn parent_tail_and_terminal_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
) -> Result<Option<(EventId, bool)>> {
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
        return Ok(None);
    };
    Ok(Some((
        EventId(u64::try_from(row.get::<_, i64>(0)).unwrap_or(u64::MAX)),
        row.get(1),
    )))
}

/// [`MapEffect::CompleteMap`]: assemble the result manifest in ascending
/// ordinal order over the input manifest's page boundaries, append the
/// terminal success fact to the parent, and wake it.
async fn complete_map_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
    task: &MapTask,
    item_count: u64,
) -> Result<EventId> {
    let key = map_command_key(map_command_id);
    let input_manifest_payload = backend
        .hydrate_activity_map_input_manifest_from_storage_tx(tx, task.input_manifest().clone())
        .await?;
    let input_manifest: ActivityMapInputManifest = crate::decode_payload(&input_manifest_payload)?;
    let item_count = usize::try_from(item_count).unwrap_or(usize::MAX);
    let (result_manifest, counts) = match task {
        MapTask::Activity(map_task) => {
            let result_refs = activity_map_results_tx(tx, schema, &key).await?;
            let manifest = encode_activity_map_result_manifest_with_codec(
                map_task.result_manifest_name.clone(),
                result_refs,
                &input_manifest.page_lengths,
                backend.payload_config.codec,
            )?;
            (
                backend
                    .normalize_activity_map_result_manifest_for_storage_tx(tx, manifest)
                    .await?,
                activity_outcome_counts(item_count),
            )
        }
        MapTask::ChildWorkflow(map_task) => {
            let outcomes = child_workflow_map_outcomes_tx(tx, schema, &key).await?;
            let counts = outcome_counts(&outcomes);
            let manifest = encode_child_workflow_map_result_manifest_with_codec(
                map_task.result_manifest_name.clone(),
                outcomes,
                &input_manifest.page_lengths,
                backend.payload_config.codec,
            )?;
            (
                backend
                    .normalize_child_workflow_map_result_manifest_for_storage_tx(tx, manifest)
                    .await?,
                counts,
            )
        }
    };
    let (data, reason) = match task.kind() {
        MapKind::Activity => (
            HistoryEventData::ActivityMapCompleted(crate::ActivityMapCompleted {
                command_id: map_command_id.clone(),
                result_manifest,
                item_count,
                success_count: counts.success_count,
                failure_count: counts.failure_count,
            }),
            WorkflowTaskReason::ActivityMapCompleted,
        ),
        MapKind::ChildWorkflow => (
            HistoryEventData::ChildWorkflowMapCompleted(crate::ChildWorkflowMapCompleted {
                command_id: map_command_id.clone(),
                result_manifest,
                item_count,
                success_count: counts.success_count,
                failure_count: counts.failure_count,
                cancellation_count: counts.cancellation_count,
            }),
            WorkflowTaskReason::ChildWorkflowMapCompleted,
        ),
    };
    append_map_terminal_event_tx(tx, schema, &map_command_id.run_id, data, reason).await
}

/// [`MapEffect::FailMap`]: append the terminal failure fact to the parent and
/// wake it.
async fn fail_map_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
    kind: MapKind,
    failure: DurableFailure,
) -> Result<EventId> {
    let failure = backend
        .normalize_failure_for_storage_tx(tx, failure)
        .await?;
    let (data, reason) = match kind {
        MapKind::Activity => (
            HistoryEventData::ActivityMapFailed(crate::ActivityMapFailed {
                command_id: map_command_id.clone(),
                failure,
            }),
            WorkflowTaskReason::ActivityMapFailed,
        ),
        MapKind::ChildWorkflow => (
            HistoryEventData::ChildWorkflowMapFailed(crate::ChildWorkflowMapFailed {
                command_id: map_command_id.clone(),
                failure,
            }),
            WorkflowTaskReason::ChildWorkflowMapFailed,
        ),
    };
    append_map_terminal_event_tx(tx, schema, &map_command_id.run_id, data, reason).await
}

/// Append a terminal map fact to the parent and mark it ready. The engine has
/// already rejected the closed-parent case, so reaching here with a terminal
/// run would be an engine/provider disagreement, not a routine race.
async fn append_map_terminal_event_tx(
    tx: &Transaction<'_>,
    schema: &str,
    run_id: &RunId,
    data: HistoryEventData,
    reason: WorkflowTaskReason,
) -> Result<EventId> {
    let Some((tail, terminal)) = parent_tail_and_terminal_tx(tx, schema, run_id).await? else {
        return Err(Error::RunNotFound(run_id.clone()));
    };
    if terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = tail.next();
    insert_history_event(tx, schema, run_id, event_id, data).await?;
    set_workflow_ready_tx(tx, schema, run_id, event_id, reason).await?;
    Ok(event_id)
}

/// [`MapEffect::CancelChildren`]: cancel every already-running, not-yet-
/// terminal child of this map with the engine's reason.
async fn cancel_child_workflow_map_children_tx(
    tx: &Transaction<'_>,
    schema: &str,
    map_command_id: &CommandId,
    reason: &str,
) -> Result<()> {
    let rows = tx
        .query(
            &format!(
                "select run_id, current_event_id
                 from {schema}.workflow_instances
                 where parent_run_id = $1
                   and parent_command_seq = $2
                   and parent_child_map_ordinal is not null
                   and terminal = false
                   and not exists (
                     select 1
                     from {schema}.history_events h
                     where h.run_id = workflow_instances.run_id
                       and h.event_type = any($3::text[])
                   )
                 order by run_id asc
                 for update"
            ),
            &[
                &map_command_id.run_id.0,
                &i64::try_from(map_command_id.seq.0).unwrap_or(i64::MAX),
                &vec![
                    event_type_to_str(&HistoryEventType::WorkflowCompleted),
                    event_type_to_str(&HistoryEventType::WorkflowFailed),
                    event_type_to_str(&HistoryEventType::WorkflowCancelled),
                    event_type_to_str(&HistoryEventType::WorkflowContinuedAsNew),
                ],
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
                reason: reason.to_owned(),
            },
        )
        .await?;
        cleanup_run_operational_state_tx(tx, schema, &child_run_id, TerminalCleanup::Closed)
            .await?;
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

async fn child_workflow_map_outcomes_tx(
    tx: &Transaction<'_>,
    schema: &str,
    map_command_key: &str,
) -> Result<Vec<ChildWorkflowMapItemOutcome>> {
    let rows = tx
        .query(
            &format!(
                "select outcome
                 from {schema}.child_workflow_map_results
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
                 heartbeat_deadline_at_ms = null,
                 implicit_heartbeat_ms = null
             where activity_id = $1"
        ),
        &[&activity_id.0],
    )
    .await
    .map_err(postgres_error)?;

    let key = map_command_key(&map_item.map_command_id);
    // Unlike `fail_map_item` and `complete_child_workflow_map_item`, a missing
    // descriptor stays a hard error here, and the asymmetry is deliberate.
    // Those two answer "already handled" because the state is reachable: a
    // child run outlives its parent's cleanup under
    // `ParentClosePolicy::Abandon`, and an item task can outlive its
    // descriptor. An activity-map item cannot: the run's terminal cleanup
    // deletes the item's own activity record and the descriptor together, and
    // this path already returned `AlreadyCompleted` when that record was gone.
    // So reaching here means a live item task with no descriptor — corruption,
    // not a race — and this completion carries a *result* that answering
    // "already handled" would silently discard.
    let Some((state, namespace, map_task)) =
        activity_map_state_tx(tx, schema, &map_item.map_command_id).await?
    else {
        return Err(Error::Backend(format!(
            "activity map `{}`:{} not found",
            map_item.map_command_id.run_id, map_item.map_command_id.seq.0
        )));
    };
    if state.completed {
        return Ok(CompleteActivityOutcome::AlreadyCompleted);
    }
    let parent_terminal = parent_run_terminal_tx(tx, schema, &task.run_id).await?;

    // An activity map's result row is its own storage primitive rather than an
    // effect, so `already_recorded` is whether the row is already there.
    // `state.recorded_outcomes` was read before this insert, which is exactly
    // the count the engine wants: the tally *excluding* this event's outcome.
    let result_blob =
        rmp_serde::to_vec_named(&result).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    let already_recorded = tx
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
        .is_none();

    let appended = step_map_tx(
        backend,
        tx,
        schema,
        &state,
        &namespace,
        &MapTask::Activity(map_task),
        MapEvent::ItemCompleted {
            ordinal: map_item.item_ordinal,
            // The result payload rides the result table, not the outcome; the
            // engine only needs to know this was a success.
            outcome: ChildWorkflowMapItemOutcome::Succeeded { result },
            already_recorded,
            parent_terminal,
        },
    )
    .await?;

    let event_id = match appended {
        Some(event_id) => event_id,
        None => tx
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
            .unwrap_or(EventId::ZERO),
    };
    Ok(CompleteActivityOutcome::Completed { event_id })
}

/// One activity-map item attempt ended. `decision` is the shared activity
/// retry verdict; the engine turns it into either a rescheduled attempt or the
/// map's terminal failure.
#[allow(clippy::too_many_arguments)]
async fn fail_map_item_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    task: ActivityTask,
    map_item: ActivityMapItem,
    failure: DurableFailure,
    kind: ItemAttemptFailureKind,
    decision: ItemRetryDecision,
    now: TimestampMs,
) -> Result<FailActivityOutcome> {
    let Some((state, namespace, map_task)) =
        activity_map_state_tx(tx, schema, &map_item.map_command_id).await?
    else {
        return Ok(FailActivityOutcome::AlreadyCompleted);
    };
    if state.completed {
        return Ok(FailActivityOutcome::AlreadyCompleted);
    }
    let already_recorded = tx
        .query_opt(
            &format!(
                "select 1 from {schema}.activity_map_results
                 where map_command_id = $1 and item_ordinal = $2"
            ),
            &[
                &map_command_key(&map_item.map_command_id),
                &i64::try_from(map_item.item_ordinal).unwrap_or(i64::MAX),
            ],
        )
        .await
        .map_err(postgres_error)?
        .is_some();
    let parent_terminal = parent_run_terminal_tx(tx, schema, &task.run_id).await?;
    let appended = step_map_tx(
        backend,
        tx,
        schema,
        &state,
        &namespace,
        &MapTask::Activity(map_task),
        MapEvent::ItemAttemptFailed {
            ordinal: map_item.item_ordinal,
            failure,
            kind,
            decision,
            failed_attempt: task.attempt,
            retry_policy: task.retry_policy.clone(),
            start_to_close_timeout: task.start_to_close_timeout,
            now,
            already_recorded,
            parent_terminal,
        },
    )
    .await?;
    match (decision, appended) {
        (ItemRetryDecision::Retry { next_attempt }, _) => {
            Ok(FailActivityOutcome::RetryScheduled { next_attempt })
        }
        (ItemRetryDecision::Exhausted, Some(event_id)) => {
            Ok(FailActivityOutcome::Failed { event_id })
        }
        (ItemRetryDecision::Exhausted, None) => Ok(FailActivityOutcome::AlreadyCompleted),
    }
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
        // Skipped, not deleted (`SPEC.md` §14). A wait that outlived its run's
        // terminal event is a defect, not a state to handle: §19.1 requires the
        // terminal commit to delete the run's waits in the same transaction, so
        // this row can only exist because that cleanup did not run. Deleting it
        // here would hide the defect the cleanup was supposed to have prevented
        // — the scan would silently repair the symptom once and leave no trace,
        // and `only_the_live_runs_timer_spends_the_due_scan_budget` would stop
        // detecting a broken cleanup, because a leftover that disappears after
        // one sweep stops contending for the scan's budget. The guard's job is
        // only to refuse the append: a `TimerFired` past a terminal event is a
        // history every replay, audit and cleanup path assumes cannot exist.
        //
        // Deliberately a per-row skip rather than a `terminal = false`
        // predicate on the selecting query: the predicate (TypeScript's
        // Postgres provider takes that route) would keep a leftover out of the
        // scan's `limit` budget, but it would also make this provider blind to
        // a terminal cleanup that stopped deleting waits, which is what the
        // budget case above actually tests. The starvation the predicate would
        // avoid is reachable only once that invariant is already broken.
        if terminal {
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
    backend: &PostgresBackend,
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
        if timeout_activity_tx(backend, tx, schema, activity_id, req.now).await? {
            timed_out += 1;
        }
    }
    Ok(timed_out)
}

async fn timeout_activity_tx(
    backend: &PostgresBackend,
    tx: &Transaction<'_>,
    schema: &str,
    activity_id: ActivityId,
    now: TimestampMs,
) -> Result<bool> {
    let Some(row) = tx
        .query_opt(
            &format!(
                "select task, completed, timeout_at_ms, heartbeat_deadline_at_ms,
                        implicit_heartbeat_ms
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
    let implicit_heartbeat_ms: Option<i64> = row.get(4);
    let start_timeout_due = timeout_at_ms.is_some_and(|timeout_at_ms| timeout_at_ms <= now.0);
    let heartbeat_timeout_due = heartbeat_deadline_at_ms.is_some_and(|deadline| deadline <= now.0);
    if completed || !(start_timeout_due || heartbeat_timeout_due) {
        return Ok(false);
    }
    let attribution = activity_timeout_attribution(
        start_timeout_due,
        heartbeat_timeout_due,
        implicit_heartbeat_ms.is_some(),
    );

    let task: ActivityTask =
        rmp_serde::from_slice(&task_blob).map_err(|err| Error::PayloadDecode(err.to_string()))?;
    let decision = activity_timeout_decision(&task);

    // A map item's lapsed deadline is an engine event, not a local reschedule:
    // the engine owns both the retry and the map's terminal failure, and it is
    // the only thing that knows whether the map is over.
    if let Some(map_item) = task.map_item.clone() {
        let outcome = fail_map_item_tx(
            backend,
            tx,
            schema,
            task.clone(),
            map_item,
            DurableFailure::new(
                "durust.activity_timed_out",
                timeout_message(&activity_id, task.attempt, attribution),
            ),
            ItemAttemptFailureKind::TimedOut,
            ItemRetryDecision::from(decision),
            now,
        )
        .await?;
        if !matches!(outcome, FailActivityOutcome::RetryScheduled { .. }) {
            tx.execute(
                &format!(
                    "update {schema}.activity_tasks
                     set completed = true,
                         heartbeat_deadline_at_ms = null,
                         implicit_heartbeat_ms = null
                     where activity_id = $1"
                ),
                &[&activity_id.0],
            )
            .await
            .map_err(postgres_error)?;
        }
        return Ok(true);
    }

    if let ActivityFailureDecision::Retry { next_attempt } = decision {
        // Timeout retries carry no backoff: the expired deadline already
        // paced this attempt, and delaying crash recovery further would only
        // add latency.
        let mut retry_task = task.clone();
        retry_task.attempt = next_attempt;
        let retry_blob = rmp_serde::to_vec_named(&retry_task)
            .map_err(|err| Error::PayloadEncode(err.to_string()))?;
        tx.execute(
            &format!(
                "update {schema}.activity_tasks
                 set task = $1,
                     claim_token = null,
                     timeout_at_ms = $2,
                     heartbeat_deadline_at_ms = null,
                     implicit_heartbeat_ms = null,
                     visible_at_ms = null
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
                 heartbeat_deadline_at_ms = null,
                 implicit_heartbeat_ms = null
             where activity_id = $1"
        ),
        &[&activity_id.0],
    )
    .await
    .map_err(postgres_error)?;

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
            message: timeout_message(&activity_id, task.attempt, attribution),
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

fn is_postgres_payload_uri(uri: &str) -> bool {
    uri.starts_with("postgres://payload/")
}

// Every blob ref this provider did not mint is opaque: it belongs to whatever
// layer owns its scheme (a `PayloadBackend` blob store), so the provider never
// hydrates, validates, or garbage-collects it.
fn is_external_payload_ref(payload: &PayloadRef) -> bool {
    matches!(payload, PayloadRef::Blob { uri, .. } if !is_postgres_payload_uri(uri))
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
    _ref_schema_fingerprint: &crate::SchemaFingerprint,
    ref_compression: crate::CompressionId,
    ref_encryption: &Option<crate::EncryptionMetadata>,
    digest: &str,
    size: u64,
    require_schema_fingerprint_match: bool,
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
        || (require_schema_fingerprint_match && blob.schema_fingerprint != *_ref_schema_fingerprint)
        || blob.compression != ref_compression
        || blob.encryption != *ref_encryption
    {
        return Err(Error::PayloadDecode(format!(
            "payload blob metadata mismatch for `{digest}`"
        )));
    }
    Ok(blob)
}

// Rewriters bind the shared `rewrite_history_event_payloads` visitor to Postgres's
// in-transaction normalize and connection-scoped hydrate leaf operations.
struct PostgresNormalizeRewriter<'a, 'tx> {
    backend: &'a PostgresBackend,
    tx: &'a Transaction<'tx>,
}

impl crate::payload::PayloadRewrite for PostgresNormalizeRewriter<'_, '_> {
    async fn payload(&mut self, payload: PayloadRef) -> Result<PayloadRef> {
        self.backend
            .normalize_payload_for_storage_tx(self.tx, payload)
            .await
    }

    async fn activity_map_input_manifest(&mut self, manifest: PayloadRef) -> Result<PayloadRef> {
        self.backend
            .normalize_activity_map_input_manifest_for_storage_tx(self.tx, manifest)
            .await
    }

    async fn activity_map_result_manifest(&mut self, manifest: PayloadRef) -> Result<PayloadRef> {
        self.backend
            .normalize_activity_map_result_manifest_for_storage_tx(self.tx, manifest)
            .await
    }

    async fn child_workflow_map_result_manifest(
        &mut self,
        manifest: PayloadRef,
    ) -> Result<PayloadRef> {
        self.backend
            .normalize_child_workflow_map_result_manifest_for_storage_tx(self.tx, manifest)
            .await
    }
}

struct PostgresHydrateRewriter<'a> {
    backend: &'a PostgresBackend,
}

impl crate::payload::PayloadRewrite for PostgresHydrateRewriter<'_> {
    async fn payload(&mut self, payload: PayloadRef) -> Result<PayloadRef> {
        self.backend.hydrate_payload_from_storage(payload).await
    }

    async fn activity_map_input_manifest(&mut self, manifest: PayloadRef) -> Result<PayloadRef> {
        self.backend
            .hydrate_activity_map_input_manifest_from_storage(manifest)
            .await
    }

    async fn activity_map_result_manifest(&mut self, manifest: PayloadRef) -> Result<PayloadRef> {
        self.backend
            .hydrate_activity_map_result_manifest_from_storage(manifest)
            .await
    }

    async fn child_workflow_map_result_manifest(
        &mut self,
        manifest: PayloadRef,
    ) -> Result<PayloadRef> {
        self.backend
            .hydrate_child_workflow_map_result_manifest_from_storage(manifest)
            .await
    }
}

fn map_command_key(command_id: &CommandId) -> String {
    format!("{}:{}", command_id.run_id, command_id.seq.0)
}

fn collect_child_workflow_map_outcome_payload_roots(
    outcome: &ChildWorkflowMapItemOutcome,
    roots: &mut Vec<PayloadRootRef>,
) {
    match outcome {
        ChildWorkflowMapItemOutcome::Succeeded { result } => {
            roots.push(PayloadRootRef::Payload(result.clone()));
        }
        ChildWorkflowMapItemOutcome::Failed { failure } => {
            collect_failure_payload_roots(failure, roots);
        }
        ChildWorkflowMapItemOutcome::Cancelled { .. } => {}
    }
}

#[cfg(test)]
mod tests;
