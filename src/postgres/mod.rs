use crate::{
    ActivityFailed, ActivityHeartbeatOutcome, ActivityHeartbeatRequest, ActivityId,
    ActivityMapInputManifest, ActivityMapInputPage, ActivityMapItem, ActivityMapResultManifest,
    ActivityMapResultPage, ActivityMapTask, ActivityTask, ActivityTaskClaim, CancelWorkflowOutcome,
    CancelWorkflowRequest, ChildStartOutboxMessage, ClaimActivityOptions, ClaimWorkflowTaskOptions,
    ClaimedActivityTask, ClaimedWorkflowTask, CommandId, CommandSeq, CommitOutcome,
    CompleteActivityOutcome, CompleteActivityRequest, DispatchChildWorkflowStartsOutcome,
    DispatchChildWorkflowStartsRequest, DurableBackend, DurableFailure, Error, EventId,
    FailActivityOutcome, FailActivityRequest, FireDueTimersOutcome, FireDueTimersRequest,
    HistoryChunk, HistoryEvent, HistoryEventData, HistoryEventType, ParentClosePolicy, PayloadBlob,
    PayloadGarbageCollectionOutcome, PayloadGarbageCollectionRequest, PayloadRef, PayloadRootRef,
    PayloadRootsOutcome, PayloadStorageConfig, QueryProjectionOutcome, QueryProjectionRequest,
    ReadSignalInboxRequest, Result, RunId, SignalInboxRecord, SignalWorkflowOutcome,
    SignalWorkflowRequest, StartWorkflowOutcome, StartWorkflowRequest, TimeoutDueActivitiesOutcome,
    TimeoutDueActivitiesRequest, TimestampMs, WaitKind, WorkerId, WorkflowChangeMarkerKind,
    WorkflowChangeVersionRecord, WorkflowChangeVersionStatus, WorkflowChangeVersionsOutcome,
    WorkflowChangeVersionsRequest, WorkflowTaskClaim, WorkflowTaskCommit, WorkflowTaskReason,
    WorkflowType, activity_map_input_at, digest_bytes,
    encode_activity_map_result_manifest_with_codec, event_payload_len, is_terminal,
};
use deadpool_postgres::{
    Manager, ManagerConfig, Object as PooledPostgresClient, Pool, RecyclingMethod, Runtime,
};
use futures::future::{BoxFuture, ready};
use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::NoTls;

mod helpers;
mod payloads;

use helpers::*;

const POSTGRES_SCHEMA_VERSION: i64 = 1;
const DEFAULT_SCHEMA: &str = "durust";
const DEFAULT_MAX_POOL_SIZE: usize = 16;

#[derive(Clone, Debug)]
pub struct PostgresBackendConfig {
    database_url: String,
    schema: String,
    payload_config: PayloadStorageConfig,
    max_pool_size: usize,
}

impl PostgresBackendConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            schema: DEFAULT_SCHEMA.to_owned(),
            payload_config: PayloadStorageConfig::default(),
            max_pool_size: DEFAULT_MAX_POOL_SIZE,
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
}

#[derive(Clone, Debug)]
pub struct PostgresBackend {
    pool: Pool,
    schema: String,
    payload_config: PayloadStorageConfig,
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
        let pg_config = config
            .database_url
            .parse()
            .map_err(|err| Error::Backend(format!("postgres database URL parse error: {err}")))?;
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
        };
        backend.migrate().await?;
        Ok(backend)
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn payload_storage_config(&self) -> PayloadStorageConfig {
        self.payload_config.clone()
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

                create table if not exists {schema}.workflow_instances (
                    namespace text not null,
                    workflow_id text not null,
                    run_id text primary key,
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

                commit;
                "
            ))
            .await
            .map_err(postgres_error)
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

    fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedActivityTask>>> {
        let backend = self.clone();
        Box::pin(async move { backend.claim_activity_task_inner(worker_id, opts).await })
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
    async fn start_workflow_inner(
        &self,
        req: StartWorkflowRequest,
    ) -> Result<StartWorkflowOutcome> {
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
        let run_id = RunId::new(format!("run-{}", next_counter(&tx, &schema, "run").await?));
        let start = HistoryEventData::WorkflowStarted {
            workflow_type: req.workflow_type.clone(),
            input,
        };
        tx.execute(
            &format!(
                "insert into {schema}.workflow_instances
                 (namespace, workflow_id, run_id, workflow_name, workflow_version, task_queue,
                  current_event_id, ready_reason, ready_at_ms, workflow_claim_token, terminal,
                  parent_run_id, parent_command_seq, parent_close_policy)
                 values ($1, $2, $3, $4, $5, $6, 1, $7, 0, null, false, null, null, null)"
            ),
            &[
                &req.namespace.0,
                &req.workflow_id.0,
                &run_id.0,
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
        let row = tx
            .query_opt(
                &format!(
                    "select run_id, workflow_id, workflow_name, workflow_version, current_event_id, ready_reason
                     from {schema}.workflow_instances
                     where namespace = $1
                       and task_queue = $2
                       and ready_reason is not null
                       and ready_at_ms <= $3
                       and workflow_claim_token is null
                       and terminal = false
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
                ))
            })
            .transpose()?;

        let Some((run_id, workflow_id, workflow_type, tail, reason)) = selected else {
            tx.commit().await.map_err(postgres_error)?;
            return Ok(None);
        };
        let token = next_counter(&tx, &schema, "claim").await?;
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
        }))
    }

    async fn stream_history_inner(
        &self,
        req: crate::StreamHistoryRequest,
        hydrate: bool,
    ) -> Result<HistoryChunk> {
        let schema = self.schema_sql();
        let rows = {
            let client = self.client().await?;
            client
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
                .map_err(postgres_error)?
        };
        let max_events = req.max_events.max(1);
        let max_bytes = req.max_bytes.max(1);
        let mut events = Vec::new();
        let mut bytes = 0usize;
        for row in rows {
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
        let has_more = {
            let client = self.client().await?;
            client
                .query_opt(
                    &format!(
                        "select 1 from {schema}.history_events
                         where run_id = $1 and event_id > $2 and event_id <= $3
                         limit 1"
                    ),
                    &[
                        &req.run_id.0,
                        &i64::try_from(last_event_id.0).unwrap_or(i64::MAX),
                        &i64::try_from(req.up_to_event_id.0).unwrap_or(i64::MAX),
                    ],
                )
                .await
                .map_err(postgres_error)?
                .is_some()
        };
        Ok(HistoryChunk {
            events,
            last_event_id,
            has_more,
        })
    }

    async fn release_workflow_task_inner(
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
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
        let Some(row) = tx
            .query_opt(
                &format!(
                    "select current_event_id, workflow_claim_token, terminal, namespace, workflow_id
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
        if claim_token != Some(i64::try_from(claim.token).unwrap_or(i64::MAX)) {
            return Err(Error::StaleLease);
        }
        let current_tail = EventId(u64::try_from(current_tail_i64).unwrap_or(u64::MAX));
        if current_tail != batch.expected_tail_event_id {
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
            tx.commit().await.map_err(postgres_error)?;
            return Ok(CommitOutcome::Conflict);
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
        for event in append_events {
            next_event_id = next_event_id.next();
            if is_terminal(&event.data) {
                became_terminal = true;
                terminal_event = Some(event.data.clone());
            }
            insert_history_event(&tx, &schema, &claim.run_id, next_event_id, event.data).await?;
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
                start_child_workflow_inline_tx(&tx, &schema, &namespace, &message).await?
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
            insert_history_event(&tx, &schema, &claim.run_id, next_event_id, event_data).await?;
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
                tx.commit().await.map_err(postgres_error)?;
                return Ok(CommitOutcome::Committed {
                    new_tail_event_id: next_event_id,
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
        tx.commit().await.map_err(postgres_error)?;
        Ok(CommitOutcome::Committed {
            new_tail_event_id: next_event_id,
        })
    }

    async fn signal_workflow_inner(
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

        let received_sequence = next_counter(&tx, &schema, "signal").await?;
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
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
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
                &tx,
                &schema,
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

        tx.commit().await.map_err(postgres_error)?;
        Ok(FireDueTimersOutcome { fired })
    }

    async fn timeout_due_activities_inner(
        &self,
        req: TimeoutDueActivitiesRequest,
    ) -> Result<TimeoutDueActivitiesOutcome> {
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(postgres_error)?;
        let schema = self.schema_sql();
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
            if timeout_activity_tx(&tx, &schema, activity_id, req.now).await? {
                timed_out += 1;
            }
        }

        tx.commit().await.map_err(postgres_error)?;
        Ok(TimeoutDueActivitiesOutcome { timed_out })
    }

    async fn claim_activity_task_inner(
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
        let token = next_counter(&tx, &schema, "claim").await?;
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

    async fn heartbeat_activity_inner(
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
            tx.commit().await.map_err(postgres_error)?;
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
        tx.commit().await.map_err(postgres_error)?;
        Ok(CompleteActivityOutcome::Completed { event_id })
    }

    async fn fail_activity_inner(&self, req: FailActivityRequest) -> Result<FailActivityOutcome> {
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

#[cfg(test)]
mod tests;
