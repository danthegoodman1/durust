//! The Node binding: one class over the Rust providers, speaking the
//! TypeScript provider contract in msgpack. Every method takes and returns a
//! msgpack `Buffer` whose shape is the TypeScript request or outcome type,
//! encoded by `wire`.

mod wire;

#[cfg(feature = "sqlite")]
use durust::SqliteBackend;
use durust::provider::{
    DurableBackend, LocalDirectoryBlobStore, MemoryBlobStore, PayloadBackend, PayloadBlobStore,
    S3BlobStore, S3BlobStoreConfig, SignalInboxRecord,
};
use durust::{MemoryBackend, RunId};
#[cfg(feature = "postgres")]
use durust::{PostgresBackend, PostgresBackendConfig};
use futures::future::{BoxFuture, ready};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::{Arc, Mutex};
#[cfg(feature = "postgres")]
use std::time::Duration;

/// The provider behind a `NativeBackend`. `DurableBackend` is `Clone`, so it
/// cannot be a trait object; this trait carries the calls the binding makes,
/// forwarded from any provider, plain or wrapped in `PayloadBackend`.
trait Provider: Send + Sync {
    fn current_time(&self) -> BoxFuture<'static, durust::Result<durust::TimestampMs>>;
    fn start_workflow(
        &self,
        req: durust::provider::StartWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::StartWorkflowOutcome>>;
    fn claim_workflow_task(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::provider::ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::provider::ClaimedWorkflowTask>>>;
    fn claim_workflow_tasks(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::provider::ClaimWorkflowTasksOptions,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::provider::ClaimedWorkflowTask>>>;
    fn stream_history(
        &self,
        req: durust::provider::StreamHistoryRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::HistoryChunk>>;
    fn commit_workflow_task(
        &self,
        claim: durust::provider::WorkflowTaskClaim,
        commit: durust::provider::WorkflowTaskCommit,
    ) -> BoxFuture<'static, durust::Result<durust::EventId>>;
    fn release_workflow_task(
        &self,
        claim: durust::provider::WorkflowTaskClaim,
        release: durust::provider::WorkflowTaskRelease,
    ) -> BoxFuture<'static, durust::Result<()>>;
    fn claim_activity_task(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::provider::ClaimActivityOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::provider::ClaimedActivityTask>>>;
    fn claim_activity_tasks(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::provider::ClaimActivityTasksOptions,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::provider::ClaimedActivityTask>>>;
    fn complete_activity(
        &self,
        req: durust::provider::CompleteActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::CompleteActivityOutcome>>;
    fn complete_activity_tasks(
        &self,
        req: durust::provider::CompleteActivityTasksRequest,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::provider::CompleteActivityTaskBatchResult>>>;
    fn fail_activity(
        &self,
        req: durust::provider::FailActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::FailActivityOutcome>>;
    fn heartbeat_activity(
        &self,
        req: durust::provider::ActivityHeartbeatRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::ActivityHeartbeatOutcome>>;
    fn fire_due_timers(
        &self,
        req: durust::provider::FireDueTimersRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::FireDueTimersOutcome>>;
    fn timeout_due_activities(
        &self,
        req: durust::provider::TimeoutDueActivitiesRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::TimeoutDueActivitiesOutcome>>;
    fn signal_workflow(
        &self,
        req: durust::provider::SignalWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::SignalWorkflowOutcome>>;
    fn read_signal_inbox(
        &self,
        req: durust::provider::ReadSignalInboxRequest,
    ) -> BoxFuture<'static, durust::Result<Option<SignalInboxRecord>>>;
    fn query_projection(
        &self,
        req: durust::provider::QueryProjectionRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::QueryProjectionOutcome>>;
    fn payload_roots(
        &self,
    ) -> BoxFuture<'static, durust::Result<durust::provider::PayloadRootsOutcome>>;
    fn gc_payload_blobs(
        &self,
        req: durust::provider::PayloadGarbageCollectionRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::PayloadGarbageCollectionOutcome>>;
    /// Every undelivered signal of a run, one per signal name in arrival
    /// order: the snapshot a claimed workflow task carries in the TypeScript
    /// contract.
    fn live_signals(
        &self,
        run_id: RunId,
    ) -> BoxFuture<'static, durust::Result<Vec<SignalInboxRecord>>>;
    /// Deletes the provider's storage where that is a schema of its own
    /// (Postgres); the memory and SQLite providers have nothing to delete
    /// beyond what dropping them or their file does.
    fn destroy(&self) -> BoxFuture<'static, durust::Result<()>>;
    /// A claim as the TypeScript worker reads it: prefetched history with
    /// every offloaded payload inline.
    fn hydrate_claim(
        &self,
        task: durust::provider::ClaimedWorkflowTask,
    ) -> BoxFuture<'static, durust::Result<durust::provider::ClaimedWorkflowTask>>;
}

/// The calls outside the `DurableBackend` trait that each concrete provider
/// answers its own way. Plain providers hold nothing offloaded, so their
/// claims are already what the worker reads.
trait ProviderExtras {
    fn live_signals(
        &self,
        run_id: RunId,
    ) -> BoxFuture<'static, durust::Result<Vec<SignalInboxRecord>>>;
    fn destroy(&self) -> BoxFuture<'static, durust::Result<()>>;
    fn hydrate_claim(
        &self,
        task: durust::provider::ClaimedWorkflowTask,
    ) -> BoxFuture<'static, durust::Result<durust::provider::ClaimedWorkflowTask>> {
        Box::pin(ready(Ok(task)))
    }
}

impl ProviderExtras for MemoryBackend {
    fn live_signals(
        &self,
        run_id: RunId,
    ) -> BoxFuture<'static, durust::Result<Vec<SignalInboxRecord>>> {
        Box::pin(ready(MemoryBackend::live_signals(self, &run_id)))
    }

    fn destroy(&self) -> BoxFuture<'static, durust::Result<()>> {
        Box::pin(ready(Ok(())))
    }
}

#[cfg(feature = "sqlite")]
impl ProviderExtras for SqliteBackend {
    fn live_signals(
        &self,
        run_id: RunId,
    ) -> BoxFuture<'static, durust::Result<Vec<SignalInboxRecord>>> {
        Box::pin(ready(SqliteBackend::live_signals(self, &run_id)))
    }

    fn destroy(&self) -> BoxFuture<'static, durust::Result<()>> {
        Box::pin(ready(Ok(())))
    }
}

#[cfg(feature = "postgres")]
impl ProviderExtras for PostgresBackend {
    fn live_signals(
        &self,
        run_id: RunId,
    ) -> BoxFuture<'static, durust::Result<Vec<SignalInboxRecord>>> {
        let backend = self.clone();
        Box::pin(async move { backend.live_signals(&run_id).await })
    }

    fn destroy(&self) -> BoxFuture<'static, durust::Result<()>> {
        let backend = self.clone();
        Box::pin(async move { backend.drop_schema().await })
    }
}

impl<B, S> ProviderExtras for PayloadBackend<B, S>
where
    B: DurableBackend + ProviderExtras,
    S: PayloadBlobStore,
{
    fn live_signals(
        &self,
        run_id: RunId,
    ) -> BoxFuture<'static, durust::Result<Vec<SignalInboxRecord>>> {
        let inner = self.inner().live_signals(run_id);
        let this = self.clone();
        Box::pin(async move {
            let mut hydrated = Vec::new();
            for record in inner.await? {
                hydrated.push(SignalInboxRecord {
                    payload: this.hydrate_payload(record.payload).await?,
                    ..record
                });
            }
            Ok(hydrated)
        })
    }

    fn destroy(&self) -> BoxFuture<'static, durust::Result<()>> {
        self.inner().destroy()
    }

    fn hydrate_claim(
        &self,
        task: durust::provider::ClaimedWorkflowTask,
    ) -> BoxFuture<'static, durust::Result<durust::provider::ClaimedWorkflowTask>> {
        let hydrated = self.hydrate_history_events(task.prefetched_history);
        Box::pin(async move {
            Ok(durust::provider::ClaimedWorkflowTask {
                prefetched_history: hydrated.await?,
                ..task
            })
        })
    }
}

impl<B> Provider for B
where
    B: DurableBackend + ProviderExtras,
{
    fn current_time(&self) -> BoxFuture<'static, durust::Result<durust::TimestampMs>> {
        DurableBackend::current_time(self)
    }

    fn start_workflow(
        &self,
        req: durust::provider::StartWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::StartWorkflowOutcome>> {
        DurableBackend::start_workflow(self, req)
    }

    fn claim_workflow_task(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::provider::ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::provider::ClaimedWorkflowTask>>> {
        DurableBackend::claim_workflow_task(self, worker_id, opts)
    }

    fn claim_workflow_tasks(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::provider::ClaimWorkflowTasksOptions,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::provider::ClaimedWorkflowTask>>> {
        DurableBackend::claim_workflow_tasks(self, worker_id, opts)
    }

    fn stream_history(
        &self,
        req: durust::provider::StreamHistoryRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::HistoryChunk>> {
        DurableBackend::stream_history(self, req)
    }

    fn commit_workflow_task(
        &self,
        claim: durust::provider::WorkflowTaskClaim,
        commit: durust::provider::WorkflowTaskCommit,
    ) -> BoxFuture<'static, durust::Result<durust::EventId>> {
        DurableBackend::commit_workflow_task(self, claim, commit)
    }

    fn release_workflow_task(
        &self,
        claim: durust::provider::WorkflowTaskClaim,
        release: durust::provider::WorkflowTaskRelease,
    ) -> BoxFuture<'static, durust::Result<()>> {
        DurableBackend::release_workflow_task(self, claim, release)
    }

    fn claim_activity_task(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::provider::ClaimActivityOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::provider::ClaimedActivityTask>>> {
        DurableBackend::claim_activity_task(self, worker_id, opts)
    }

    fn claim_activity_tasks(
        &self,
        worker_id: durust::WorkerId,
        opts: durust::provider::ClaimActivityTasksOptions,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::provider::ClaimedActivityTask>>> {
        DurableBackend::claim_activity_tasks(self, worker_id, opts)
    }

    fn complete_activity(
        &self,
        req: durust::provider::CompleteActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::CompleteActivityOutcome>> {
        DurableBackend::complete_activity(self, req)
    }

    fn complete_activity_tasks(
        &self,
        req: durust::provider::CompleteActivityTasksRequest,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::provider::CompleteActivityTaskBatchResult>>>
    {
        DurableBackend::complete_activity_tasks(self, req)
    }

    fn fail_activity(
        &self,
        req: durust::provider::FailActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::FailActivityOutcome>> {
        DurableBackend::fail_activity(self, req)
    }

    fn heartbeat_activity(
        &self,
        req: durust::provider::ActivityHeartbeatRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::ActivityHeartbeatOutcome>> {
        DurableBackend::heartbeat_activity(self, req)
    }

    fn fire_due_timers(
        &self,
        req: durust::provider::FireDueTimersRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::FireDueTimersOutcome>> {
        DurableBackend::fire_due_timers(self, req)
    }

    fn timeout_due_activities(
        &self,
        req: durust::provider::TimeoutDueActivitiesRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::TimeoutDueActivitiesOutcome>> {
        DurableBackend::timeout_due_activities(self, req)
    }

    fn signal_workflow(
        &self,
        req: durust::provider::SignalWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::SignalWorkflowOutcome>> {
        DurableBackend::signal_workflow(self, req)
    }

    fn read_signal_inbox(
        &self,
        req: durust::provider::ReadSignalInboxRequest,
    ) -> BoxFuture<'static, durust::Result<Option<SignalInboxRecord>>> {
        DurableBackend::read_signal_inbox(self, req)
    }

    fn query_projection(
        &self,
        req: durust::provider::QueryProjectionRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::QueryProjectionOutcome>> {
        DurableBackend::query_projection(self, req)
    }

    fn payload_roots(
        &self,
    ) -> BoxFuture<'static, durust::Result<durust::provider::PayloadRootsOutcome>> {
        DurableBackend::payload_roots(self)
    }

    fn gc_payload_blobs(
        &self,
        req: durust::provider::PayloadGarbageCollectionRequest,
    ) -> BoxFuture<'static, durust::Result<durust::provider::PayloadGarbageCollectionOutcome>> {
        DurableBackend::gc_payload_blobs(self, req)
    }

    fn live_signals(
        &self,
        run_id: RunId,
    ) -> BoxFuture<'static, durust::Result<Vec<SignalInboxRecord>>> {
        ProviderExtras::live_signals(self, run_id)
    }

    fn destroy(&self) -> BoxFuture<'static, durust::Result<()>> {
        ProviderExtras::destroy(self)
    }

    fn hydrate_claim(
        &self,
        task: durust::provider::ClaimedWorkflowTask,
    ) -> BoxFuture<'static, durust::Result<durust::provider::ClaimedWorkflowTask>> {
        ProviderExtras::hydrate_claim(self, task)
    }
}

/// Wraps a provider in `PayloadBackend` over the blob store the options name,
/// or returns it plain when the options name none.
fn with_payload<B>(inner: B, payload: Option<wire::PayloadOptions>) -> Result<Arc<dyn Provider>>
where
    B: DurableBackend + ProviderExtras,
{
    let Some(payload) = payload else {
        return Ok(Arc::new(inner));
    };
    let mut config = durust::PayloadStorageConfig::default();
    if let Some(threshold) = payload.inline_threshold_bytes {
        config.inline_threshold_bytes = threshold;
    }
    Ok(match payload.blob_store {
        wire::BlobStore::Memory => Arc::new(PayloadBackend::with_payload_storage(
            inner,
            MemoryBlobStore::new(),
            config,
        )),
        wire::BlobStore::LocalDirectory { root, prefix } => {
            Arc::new(PayloadBackend::with_payload_storage(
                inner,
                LocalDirectoryBlobStore::new(root, prefix.as_deref().unwrap_or("")),
                config,
            ))
        }
        wire::BlobStore::S3 {
            bucket,
            endpoint,
            region,
            prefix,
            access_key_id,
            secret_access_key,
        } => {
            let store = S3BlobStore::new(S3BlobStoreConfig {
                bucket,
                endpoint,
                region,
                prefix: prefix.unwrap_or_default(),
                access_key_id,
                secret_access_key,
            })
            .map_err(|err| provider_error(err, false))?;
            Arc::new(PayloadBackend::with_payload_storage(inner, store, config))
        }
    })
}

/// A claim as the TypeScript worker expects it: history up to the replay
/// target, prefetched here when the provider prefetched none (the worker
/// streams anything past these bounds), with offloaded payloads inline.
const PREFETCH_MAX_EVENTS: usize = 256;
const PREFETCH_MAX_BYTES: usize = 1 << 20;

async fn with_prefetched_history(
    backend: &dyn Provider,
    mut task: durust::provider::ClaimedWorkflowTask,
) -> durust::Result<durust::provider::ClaimedWorkflowTask> {
    if task.prefetched_history.is_empty() && task.replay_target_event_id.0 > 0 {
        let req = durust::provider::StreamHistoryRequest {
            run_id: task.run_id.clone(),
            after_event_id: durust::EventId::ZERO,
            up_to_event_id: task.replay_target_event_id,
            max_events: PREFETCH_MAX_EVENTS,
            max_bytes: PREFETCH_MAX_BYTES,
        };
        let chunk = backend.stream_history(req).await?;
        task.prefetched_history = chunk.events;
    }
    backend.hydrate_claim(task).await
}

/// Every payload reference a manifest reaches: the manifest, its pages, and
/// the items or results inside each inline page. The TypeScript contract
/// lists those as roots; the Rust providers list the manifest and decode it
/// when they collect garbage.
fn push_manifest_roots<M, P>(
    manifest: durust::PayloadRef,
    pages: impl Fn(&M) -> &[durust::PayloadRef],
    items: impl Fn(P) -> Vec<durust::PayloadRef>,
    roots: &mut Vec<durust::PayloadRef>,
) where
    M: serde::de::DeserializeOwned,
    P: serde::de::DeserializeOwned,
{
    if let Ok(decoded) = durust::decode_payload::<M>(&manifest) {
        for page in pages(&decoded) {
            if let Ok(decoded_page) = durust::decode_payload::<P>(page) {
                roots.extend(items(decoded_page));
            }
            roots.push(page.clone());
        }
    }
    roots.push(manifest);
}

fn decode<T: serde::de::DeserializeOwned>(buffer: &Buffer) -> Result<T> {
    rmp_serde::from_slice(buffer.as_ref()).map_err(|err| {
        Error::new(
            Status::InvalidArg,
            format!("durust-node: request decode failed: {err}"),
        )
    })
}

fn decode_options<T: serde::de::DeserializeOwned + Default>(buffer: Option<Buffer>) -> Result<T> {
    buffer.as_ref().map_or_else(|| Ok(T::default()), decode)
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Buffer> {
    rmp_serde::to_vec_named(value)
        .map(Buffer::from)
        .map_err(|err| Error::from_reason(format!("durust-node: response encode failed: {err}")))
}

/// Maps a provider error onto the message the TypeScript providers throw for
/// the same condition, so the shared conformance suite reads the binding like
/// any other provider.
fn provider_error(err: durust::Error, activity: bool) -> Error {
    let message = match &err {
        durust::Error::StaleLease if activity => "stale activity task lease".to_owned(),
        durust::Error::StaleLease => "stale workflow task lease".to_owned(),
        durust::Error::TerminalWorkflow => {
            "terminal workflow rejects workflow-visible mutations".to_owned()
        }
        durust::Error::RunNotFound(run_id) => format!("workflow not found: {run_id}"),
        durust::Error::WorkflowNotFound(workflow_id) => {
            format!("workflow not found: {workflow_id}")
        }
        other => other.to_string(),
    };
    Error::from_reason(message)
}

/// An activity call's error: an id naming a run the provider never had is
/// reported against the activity, as the TypeScript providers do.
fn activity_error(err: durust::Error, activity_id: &str) -> Error {
    match err {
        durust::Error::RunNotFound(_) => {
            Error::from_reason(format!("activity task not found: {activity_id}"))
        }
        other => provider_error(other, true),
    }
}

/// How `advance_time_to` reaches the provider's clock: the memory provider
/// owns a virtual clock, and SQLite and Postgres take a caller-driven one.
enum Clock {
    Memory(MemoryBackend),
    Manual(durust::provider::ProviderClock),
}

#[napi]
pub struct NativeBackend {
    provider: Mutex<Option<Arc<dyn Provider>>>,
    clock: Clock,
}

impl NativeBackend {
    fn new(provider: Arc<dyn Provider>, clock: Clock) -> Self {
        Self {
            provider: Mutex::new(Some(provider)),
            clock,
        }
    }

    fn provider(&self) -> Result<Arc<dyn Provider>> {
        self.provider
            .lock()
            .map_err(|_| Error::from_reason("durust-node: backend mutex poisoned"))?
            .clone()
            .ok_or_else(|| Error::from_reason("durust-node: backend is closed"))
    }
}

/// The Rust Postgres provider. Connecting runs the schema migration, so the
/// constructor is a free async function rather than a factory.
#[cfg(feature = "postgres")]
#[napi]
pub async fn connect_postgres(url: String, options: Option<Buffer>) -> Result<NativeBackend> {
    let options: wire::PostgresOptions = decode_options(options)?;
    let mut config = PostgresBackendConfig::new(url);
    if let Some(schema) = options.schema {
        config = config.schema(schema);
    }
    if let Some(size) = options.max_pool_size {
        config = config.max_pool_size(size);
    }
    if let Some(shards) = options.logical_shards {
        config = config.logical_shards(shards);
    }
    if let Some(partitions) = options.physical_partitions {
        config = config.physical_partitions(partitions);
    }
    if let Some(ms) = options.statement_timeout_ms {
        config = config.statement_timeout(Duration::from_millis(ms));
    }
    if let Some(ms) = options.lock_timeout_ms {
        config = config.lock_timeout(Duration::from_millis(ms));
    }
    let clock = durust::provider::ProviderClock::manual(durust::TimestampMs(0));
    let backend = PostgresBackend::connect_with_config(config.clock(clock.clone()))
        .await
        .map_err(|err| provider_error(err, false))?;
    Ok(NativeBackend::new(
        with_payload(backend, options.payload)?,
        Clock::Manual(clock),
    ))
}

#[cfg(feature = "sqlite")]
#[napi]
impl NativeBackend {
    /// The Rust SQLite provider over the database file at `path`, on a
    /// caller-driven clock.
    #[napi(factory)]
    pub fn sqlite(path: String, options: Option<Buffer>) -> Result<Self> {
        let options: wire::BackendOptions = decode_options(options)?;
        let clock = durust::provider::ProviderClock::manual(durust::TimestampMs(0));
        let backend = SqliteBackend::open_with_clock(
            path,
            durust::PayloadStorageConfig::default(),
            clock.clone(),
        )
        .map_err(|err| provider_error(err, false))?;
        Ok(Self::new(
            with_payload(backend, options.payload)?,
            Clock::Manual(clock),
        ))
    }
}

#[napi]
impl NativeBackend {
    /// The Rust in-memory provider, on its own virtual clock.
    #[napi(factory)]
    pub fn memory(options: Option<Buffer>) -> Result<Self> {
        let options: wire::BackendOptions = decode_options(options)?;
        let backend = MemoryBackend::new();
        Ok(Self::new(
            with_payload(backend.clone(), options.payload)?,
            Clock::Memory(backend),
        ))
    }

    /// Moves the provider's clock forward to `now_ms`; earlier readings
    /// leave it where it is.
    #[napi]
    pub fn advance_time_to(&self, now_ms: i64) {
        let now = durust::TimestampMs(now_ms);
        match &self.clock {
            Clock::Memory(backend) => backend.advance_time_to(now),
            Clock::Manual(clock) => clock.advance_to(now),
        }
    }

    /// Drops the provider's own storage (the Postgres schema) and closes the
    /// backend.
    #[napi]
    pub async fn destroy(&self) -> Result<()> {
        let backend = self.provider()?;
        backend
            .destroy()
            .await
            .map_err(|err| provider_error(err, false))?;
        self.close();
        Ok(())
    }

    /// Releases the provider: Postgres connections close once every call in
    /// flight has returned, and later calls report the backend as closed.
    #[napi]
    pub fn close(&self) {
        if let Ok(mut slot) = self.provider.lock() {
            slot.take();
        }
    }

    #[napi]
    pub async fn current_time(&self) -> Result<i64> {
        let backend = self.provider()?;
        let now = backend
            .current_time()
            .await
            .map_err(|err| provider_error(err, false))?;
        Ok(now.0)
    }

    #[napi]
    pub async fn start_workflow(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::StartWorkflowRequest = decode(&req)?;
        let backend = self.provider()?;
        let outcome = backend
            .start_workflow(req.into())
            .await
            .map_err(|err| provider_error(err, false))?;
        encode(&wire::StartWorkflowOutcome::from(outcome))
    }

    #[napi]
    pub async fn claim_workflow_task(&self, worker_id: String, opts: Buffer) -> Result<Buffer> {
        let opts: wire::ClaimWorkflowTaskOptions = decode(&opts)?;
        let backend = self.provider()?;
        let claimed = backend
            .claim_workflow_task(durust::WorkerId::new(worker_id), opts.into())
            .await
            .map_err(|err| provider_error(err, false))?;
        let claimed = match claimed {
            Some(task) => {
                let task = with_prefetched_history(backend.as_ref(), task)
                    .await
                    .map_err(|err| provider_error(err, false))?;
                let live_signals = backend
                    .live_signals(task.run_id.clone())
                    .await
                    .map_err(|err| provider_error(err, false))?;
                Some(wire::ClaimedWorkflowTask::from_claimed(task, live_signals))
            }
            None => None,
        };
        encode(&claimed)
    }

    #[napi]
    pub async fn claim_workflow_tasks(&self, worker_id: String, opts: Buffer) -> Result<Buffer> {
        let opts: wire::ClaimWorkflowBatchOptions = decode(&opts)?;
        let backend = self.provider()?;
        let claimed = backend
            .claim_workflow_tasks(durust::WorkerId::new(worker_id), opts.into())
            .await
            .map_err(|err| provider_error(err, false))?;
        let mut tasks = Vec::with_capacity(claimed.len());
        for task in claimed {
            let task = with_prefetched_history(backend.as_ref(), task)
                .await
                .map_err(|err| provider_error(err, false))?;
            let live_signals = backend
                .live_signals(task.run_id.clone())
                .await
                .map_err(|err| provider_error(err, false))?;
            tasks.push(wire::ClaimedWorkflowTask::from_claimed(task, live_signals));
        }
        encode(&tasks)
    }

    #[napi]
    pub async fn stream_history(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::StreamHistoryRequest = decode(&req)?;
        let backend = self.provider()?;
        let chunk = backend
            .stream_history(req.into())
            .await
            .map_err(|err| provider_error(err, false))?;
        encode(&wire::HistoryChunk::from(chunk))
    }

    #[napi]
    pub async fn commit_workflow_task(&self, claim: Buffer, commit: Buffer) -> Result<Buffer> {
        let claim: wire::WorkflowTaskClaim = decode(&claim)?;
        let commit: wire::WorkflowTaskCommit = decode(&commit)?;
        if commit
            .schedule_activity_maps
            .iter()
            .flatten()
            .any(|map| map.max_in_flight == 0)
        {
            return Err(Error::from_reason(
                "activity map maxInFlight must be a positive integer",
            ));
        }
        if commit
            .schedule_child_workflow_maps
            .iter()
            .flatten()
            .any(|map| map.max_in_flight == 0)
        {
            return Err(Error::from_reason(
                "child workflow map maxInFlight must be a positive integer",
            ));
        }
        let backend = self.provider()?;
        let new_tail_event_id = backend
            .commit_workflow_task(claim.into(), commit.into())
            .await
            .map_err(|err| provider_error(err, false))?;
        encode(&new_tail_event_id.0)
    }

    #[napi]
    pub async fn release_workflow_task(&self, claim: Buffer, options: Buffer) -> Result<()> {
        let claim: wire::WorkflowTaskClaim = decode(&claim)?;
        let options: wire::ReleaseWorkflowTaskOptions = decode(&options)?;
        let backend = self.provider()?;
        // A release with a superseded token leaves the newer claim alone; the
        // TypeScript contract reports that as success rather than an error.
        match backend
            .release_workflow_task(claim.into(), options.into())
            .await
        {
            Ok(()) | Err(durust::Error::StaleLease) => Ok(()),
            Err(err) => Err(provider_error(err, false)),
        }
    }

    #[napi]
    pub async fn claim_activity_task(&self, worker_id: String, opts: Buffer) -> Result<Buffer> {
        let opts: wire::ClaimActivityOptions = decode(&opts)?;
        let backend = self.provider()?;
        let claimed = backend
            .claim_activity_task(durust::WorkerId::new(worker_id), opts.into())
            .await
            .map_err(|err| provider_error(err, true))?;
        encode(&claimed.map(wire::ClaimedActivityTask::from))
    }

    #[napi]
    pub async fn claim_activity_tasks(&self, worker_id: String, opts: Buffer) -> Result<Buffer> {
        let opts: wire::ClaimActivityBatchOptions = decode(&opts)?;
        let backend = self.provider()?;
        let claimed = backend
            .claim_activity_tasks(durust::WorkerId::new(worker_id), opts.into())
            .await
            .map_err(|err| provider_error(err, true))?;
        encode(
            &claimed
                .into_iter()
                .map(wire::ClaimedActivityTask::from)
                .collect::<Vec<_>>(),
        )
    }

    #[napi]
    pub async fn complete_activity(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::CompleteActivityRequest = decode(&req)?;
        let activity_id = req.claim.activity_id.clone();
        let backend = self.provider()?;
        let outcome = backend
            .complete_activity(req.into())
            .await
            .map_err(|err| activity_error(err, &activity_id))?;
        encode(&wire::CompleteActivityOutcome::from(outcome))
    }

    #[napi]
    pub async fn complete_activities(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::CompleteActivitiesRequest = decode(&req)?;
        let backend = self.provider()?;
        let results = backend
            .complete_activity_tasks(req.into())
            .await
            .map_err(|err| provider_error(err, true))?;
        let outcome = wire::CompleteActivitiesOutcome::from_results(results)
            .map_err(|err| provider_error(err, true))?;
        encode(&outcome)
    }

    #[napi]
    pub async fn fail_activity(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::FailActivityRequest = decode(&req)?;
        let activity_id = req.claim.activity_id.clone();
        let backend = self.provider()?;
        let outcome = backend
            .fail_activity(req.into())
            .await
            .map_err(|err| activity_error(err, &activity_id))?;
        encode(&wire::FailActivityOutcome::from(outcome))
    }

    #[napi]
    pub async fn heartbeat_activity(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::ActivityHeartbeatRequest = decode(&req)?;
        let activity_id = req.claim.activity_id.clone();
        let backend = self.provider()?;
        let outcome = backend
            .heartbeat_activity(req.into())
            .await
            .map_err(|err| activity_error(err, &activity_id))?;
        encode(&wire::ActivityHeartbeatOutcome::from(outcome))
    }

    #[napi]
    pub async fn fire_due_timers(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::FireDueTimersRequest = decode(&req)?;
        let backend = self.provider()?;
        let outcome = backend
            .fire_due_timers(req.into())
            .await
            .map_err(|err| provider_error(err, false))?;
        encode(&wire::FireDueTimersOutcome {
            fired: outcome.fired,
        })
    }

    #[napi]
    pub async fn timeout_due_activities(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::TimeoutDueActivitiesRequest = decode(&req)?;
        let backend = self.provider()?;
        let outcome = backend
            .timeout_due_activities(req.into())
            .await
            .map_err(|err| provider_error(err, true))?;
        encode(&wire::TimeoutDueActivitiesOutcome {
            timed_out: outcome.timed_out,
        })
    }

    #[napi]
    pub async fn signal_workflow(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::SignalWorkflowRequest = decode(&req)?;
        let backend = self.provider()?;
        let outcome = backend
            .signal_workflow(req.into())
            .await
            .map_err(|err| match err {
                durust::Error::TerminalWorkflow => {
                    Error::from_reason("terminal workflow rejects signals")
                }
                other => provider_error(other, false),
            })?;
        encode(&wire::SignalWorkflowOutcome::from(outcome))
    }

    #[napi]
    pub async fn read_signal_inbox(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::ReadSignalInboxRequest = decode(&req)?;
        let backend = self.provider()?;
        let record = backend
            .read_signal_inbox(req.into())
            .await
            .map_err(|err| provider_error(err, false))?;
        encode(&record.map(wire::SignalInboxRecord::from))
    }

    #[napi]
    pub async fn query_workflow(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::QueryWorkflowRequest = decode(&req)?;
        let backend = self.provider()?;
        let outcome = backend
            .query_projection(req.into())
            .await
            .map_err(|err| provider_error(err, false))?;
        encode(&wire::QueryWorkflowOutcome::from(outcome))
    }

    #[napi]
    pub async fn gc_payload_blobs(&self, req: Buffer) -> Result<Buffer> {
        let req: wire::PayloadGcRequest = decode(&req)?;
        let backend = self.provider()?;
        let outcome = backend
            .gc_payload_blobs(req.into())
            .await
            .map_err(|err| provider_error(err, false))?;
        encode(&wire::PayloadGcOutcome::from(outcome))
    }

    #[napi]
    pub async fn payload_roots(&self) -> Result<Buffer> {
        let backend = self.provider()?;
        let outcome = backend
            .payload_roots()
            .await
            .map_err(|err| provider_error(err, false))?;
        let mut roots = Vec::with_capacity(outcome.roots.len());
        for root in outcome.roots {
            match root {
                durust::provider::PayloadRootRef::Payload(payload) => roots.push(payload),
                durust::provider::PayloadRootRef::ActivityMapInputManifest(manifest) => {
                    push_manifest_roots::<
                        durust::provider::ActivityMapInputManifest,
                        durust::provider::ActivityMapInputPage,
                    >(manifest, |m| &m.pages, |page| page.items, &mut roots)
                }
                durust::provider::PayloadRootRef::ActivityMapResultManifest(manifest) => {
                    push_manifest_roots::<
                        durust::provider::ActivityMapResultManifest,
                        durust::provider::ActivityMapResultPage,
                    >(manifest, |m| &m.pages, |page| page.results, &mut roots)
                }
                durust::provider::PayloadRootRef::ChildWorkflowMapResultManifest(manifest) => {
                    push_manifest_roots::<
                        durust::provider::ChildWorkflowMapResultManifest,
                        durust::provider::ChildWorkflowMapResultPage,
                    >(
                        manifest,
                        |m| &m.pages,
                        |page| {
                            page.outcomes
                                .into_iter()
                                .filter_map(|outcome| match outcome {
                                    durust::provider::ChildWorkflowMapItemOutcome::Succeeded {
                                        result,
                                    } => Some(result),
                                    durust::provider::ChildWorkflowMapItemOutcome::Failed {
                                        failure,
                                    } => failure.details,
                                    durust::provider::ChildWorkflowMapItemOutcome::Cancelled {
                                        ..
                                    } => None,
                                })
                                .collect()
                        },
                        &mut roots,
                    )
                }
            }
        }
        encode(&roots)
    }
}
