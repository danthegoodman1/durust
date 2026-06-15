use crate::{
    ActivityMapInputManifest, ActivityMapInputPage, ActivityMapResultManifest,
    ActivityMapResultPage, ActivityScheduled, ActivityTask, CancelWorkflowOutcome,
    CancelWorkflowRequest, ChildStartOutboxMessage, ChildWorkflowCompleted, ChildWorkflowFailed,
    ChildWorkflowStartRequested, ClaimActivityOptions, ClaimWorkflowTaskOptions,
    ClaimedActivityTask, ClaimedWorkflowTask, CommitOutcome, CompleteActivityOutcome,
    CompleteActivityRequest, CompleteActivityTaskBatchResult, CompleteActivityTasksRequest,
    DispatchChildWorkflowStartsOutcome, DispatchChildWorkflowStartsRequest, DurableBackend,
    DurableFailure, Error, FailActivityOutcome, FailActivityRequest, FireDueTimersOutcome,
    FireDueTimersRequest, HistoryChunk, HistoryEvent, HistoryEventData, PayloadBlob,
    PayloadGarbageCollectionOutcome, PayloadGarbageCollectionRequest, PayloadRef, PayloadRootRef,
    PayloadRootsOutcome, PayloadStorageConfig, QueryProjectionOutcome, QueryProjectionRequest,
    ReadSignalInboxRequest, Result, SignalConsumed, SignalInboxRecord, SignalWorkflowOutcome,
    SignalWorkflowRequest, StartWorkflowOutcome, StartWorkflowRequest, TimeoutDueActivitiesOutcome,
    TimeoutDueActivitiesRequest, WorkerId, WorkflowChangeVersionsOutcome,
    WorkflowChangeVersionsRequest, WorkflowTaskClaim, WorkflowTaskCommit, WorkflowTaskRelease,
    digest_bytes,
};
use futures::future::{BoxFuture, ready};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

pub trait PayloadBlobStore: Clone + Send + Sync + 'static {
    fn put_payload_blob(
        &self,
        digest: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<String>>;

    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, Result<Vec<u8>>>;

    fn list_payload_blob_digests(&self) -> BoxFuture<'static, Result<BTreeSet<String>>>;

    fn delete_payload_blob(&self, digest: String) -> BoxFuture<'static, Result<()>>;

    fn owns_payload_blob_uri(&self, uri: &str) -> bool;
}

#[derive(Clone, Debug)]
pub struct PayloadBackend<B, S>
where
    B: DurableBackend,
    S: PayloadBlobStore,
{
    inner: B,
    blob_store: S,
    payload_config: PayloadStorageConfig,
}

impl<B, S> PayloadBackend<B, S>
where
    B: DurableBackend,
    S: PayloadBlobStore,
{
    pub fn new(inner: B, blob_store: S) -> Self {
        Self::with_payload_storage(inner, blob_store, PayloadStorageConfig::default())
    }

    pub fn with_payload_storage(
        inner: B,
        blob_store: S,
        mut payload_config: PayloadStorageConfig,
    ) -> Self {
        payload_config.blob_store = None;
        Self {
            inner,
            blob_store,
            payload_config,
        }
    }

    pub fn inner(&self) -> &B {
        &self.inner
    }

    pub fn blob_store(&self) -> &S {
        &self.blob_store
    }

    pub fn into_inner(self) -> B {
        self.inner
    }
}

impl<B, S> DurableBackend for PayloadBackend<B, S>
where
    B: DurableBackend,
    S: PayloadBlobStore,
{
    fn payload_storage_config(&self) -> PayloadStorageConfig {
        self.payload_config.clone()
    }

    fn start_workflow(
        &self,
        req: StartWorkflowRequest,
    ) -> BoxFuture<'static, Result<StartWorkflowOutcome>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        let config = self.payload_config.clone();
        Box::pin(async move {
            let input = normalize_payload_ref(&blob_store, &config, req.input).await?;
            inner
                .start_workflow(StartWorkflowRequest { input, ..req })
                .await
        })
    }

    fn cancel_workflow(
        &self,
        req: CancelWorkflowRequest,
    ) -> BoxFuture<'static, Result<CancelWorkflowOutcome>> {
        self.inner.cancel_workflow(req)
    }

    fn current_time(&self) -> BoxFuture<'static, Result<crate::TimestampMs>> {
        self.inner.current_time()
    }

    fn claim_workflow_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedWorkflowTask>>> {
        self.inner.claim_workflow_task(worker_id, opts)
    }

    fn stream_history(
        &self,
        req: crate::StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        Box::pin(async move {
            let chunk = inner.stream_history(req).await?;
            let mut events = Vec::with_capacity(chunk.events.len());
            for event in chunk.events {
                events.push(hydrate_history_event(&blob_store, event).await?);
            }
            Ok(HistoryChunk { events, ..chunk })
        })
    }

    fn stream_history_for_replay(
        &self,
        req: crate::StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        self.inner.stream_history_for_replay(req)
    }

    fn hydrate_payload(&self, payload: PayloadRef) -> BoxFuture<'static, Result<PayloadRef>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        Box::pin(async move {
            let payload = inner.hydrate_payload(payload).await?;
            if matches!(&payload, PayloadRef::Blob { uri, .. } if blob_store.owns_payload_blob_uri(uri))
            {
                hydrate_payload_ref(&blob_store, payload).await
            } else {
                Ok(payload)
            }
        })
    }

    fn hydrate_activity_map_result_manifest(
        &self,
        payload: PayloadRef,
    ) -> BoxFuture<'static, Result<PayloadRef>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        Box::pin(async move {
            let payload = inner.hydrate_activity_map_result_manifest(payload).await?;
            hydrate_activity_map_result_manifest(&blob_store, payload).await
        })
    }

    fn commit_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> BoxFuture<'static, Result<CommitOutcome>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        let config = self.payload_config.clone();
        Box::pin(async move {
            let batch = normalize_workflow_task_commit(&blob_store, &config, batch).await?;
            inner.commit_workflow_task(claim, batch).await
        })
    }

    fn release_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        release: WorkflowTaskRelease,
    ) -> BoxFuture<'static, Result<()>> {
        self.inner.release_workflow_task(claim, release)
    }

    fn signal_workflow(
        &self,
        req: SignalWorkflowRequest,
    ) -> BoxFuture<'static, Result<SignalWorkflowOutcome>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        let config = self.payload_config.clone();
        Box::pin(async move {
            let payload = normalize_payload_ref(&blob_store, &config, req.payload).await?;
            inner
                .signal_workflow(SignalWorkflowRequest { payload, ..req })
                .await
        })
    }

    fn read_signal_inbox(
        &self,
        req: ReadSignalInboxRequest,
    ) -> BoxFuture<'static, Result<Option<SignalInboxRecord>>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        Box::pin(async move {
            let Some(record) = inner.read_signal_inbox(req).await? else {
                return Ok(None);
            };
            let payload = hydrate_payload_ref(&blob_store, record.payload).await?;
            Ok(Some(SignalInboxRecord { payload, ..record }))
        })
    }

    fn fire_due_timers(
        &self,
        req: FireDueTimersRequest,
    ) -> BoxFuture<'static, Result<FireDueTimersOutcome>> {
        self.inner.fire_due_timers(req)
    }

    fn timeout_due_activities(
        &self,
        req: TimeoutDueActivitiesRequest,
    ) -> BoxFuture<'static, Result<TimeoutDueActivitiesOutcome>> {
        self.inner.timeout_due_activities(req)
    }

    fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedActivityTask>>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        Box::pin(async move {
            let Some(claimed) = inner.claim_activity_task(worker_id, opts).await? else {
                return Ok(None);
            };
            let task = hydrate_activity_task(&blob_store, claimed.task).await?;
            Ok(Some(ClaimedActivityTask { task, ..claimed }))
        })
    }

    fn heartbeat_activity(
        &self,
        req: crate::ActivityHeartbeatRequest,
    ) -> BoxFuture<'static, Result<crate::ActivityHeartbeatOutcome>> {
        self.inner.heartbeat_activity(req)
    }

    fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> BoxFuture<'static, Result<CompleteActivityOutcome>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        let config = self.payload_config.clone();
        Box::pin(async move {
            let result = normalize_payload_ref(&blob_store, &config, req.result).await?;
            inner
                .complete_activity(CompleteActivityRequest { result, ..req })
                .await
        })
    }

    fn complete_activity_tasks(
        &self,
        req: CompleteActivityTasksRequest,
    ) -> BoxFuture<'static, Result<Vec<CompleteActivityTaskBatchResult>>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        let config = self.payload_config.clone();
        Box::pin(async move {
            let mut completions = Vec::with_capacity(req.completions.len());
            for completion in req.completions {
                let result = normalize_payload_ref(&blob_store, &config, completion.result).await?;
                completions.push(CompleteActivityRequest {
                    result,
                    ..completion
                });
            }
            inner
                .complete_activity_tasks(CompleteActivityTasksRequest { completions })
                .await
        })
    }

    fn fail_activity(
        &self,
        req: FailActivityRequest,
    ) -> BoxFuture<'static, Result<FailActivityOutcome>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        let config = self.payload_config.clone();
        Box::pin(async move {
            let failure = normalize_failure(&blob_store, &config, req.failure).await?;
            inner
                .fail_activity(FailActivityRequest { failure, ..req })
                .await
        })
    }

    fn dispatch_child_workflow_starts(
        &self,
        req: DispatchChildWorkflowStartsRequest,
    ) -> BoxFuture<'static, Result<DispatchChildWorkflowStartsOutcome>> {
        self.inner.dispatch_child_workflow_starts(req)
    }

    fn query_projection(
        &self,
        req: QueryProjectionRequest,
    ) -> BoxFuture<'static, Result<QueryProjectionOutcome>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        Box::pin(async move {
            match inner.query_projection(req).await? {
                QueryProjectionOutcome::Found {
                    run_id,
                    event_id,
                    payload,
                } => Ok(QueryProjectionOutcome::Found {
                    run_id,
                    event_id,
                    payload: hydrate_payload_ref(&blob_store, payload).await?,
                }),
                QueryProjectionOutcome::NotFound => Ok(QueryProjectionOutcome::NotFound),
            }
        })
    }

    fn workflow_change_versions(
        &self,
        req: WorkflowChangeVersionsRequest,
    ) -> BoxFuture<'static, Result<WorkflowChangeVersionsOutcome>> {
        self.inner.workflow_change_versions(req)
    }

    fn payload_roots(&self) -> BoxFuture<'static, Result<PayloadRootsOutcome>> {
        self.inner.payload_roots()
    }

    fn gc_payload_blobs(
        &self,
        req: PayloadGarbageCollectionRequest,
    ) -> BoxFuture<'static, Result<PayloadGarbageCollectionOutcome>> {
        let inner = self.inner.clone();
        let blob_store = self.blob_store.clone();
        Box::pin(async move {
            let roots = inner.payload_roots().await?;
            let external_blobs = blob_store.list_payload_blob_digests().await?;
            let mut reachable = BTreeSet::new();
            collect_reachable_external_blobs(&blob_store, roots.roots, &mut reachable).await?;
            reachable.retain(|digest| external_blobs.contains(digest));
            let garbage = external_blobs
                .iter()
                .filter(|digest| !reachable.contains(*digest))
                .cloned()
                .collect::<Vec<_>>();
            let inner_outcome = inner.gc_payload_blobs(req.clone()).await?;
            if !req.dry_run {
                for digest in &garbage {
                    blob_store.delete_payload_blob(digest.clone()).await?;
                }
            }
            Ok(PayloadGarbageCollectionOutcome {
                scanned_blobs: inner_outcome
                    .scanned_blobs
                    .saturating_add(external_blobs.len()),
                retained_blobs: inner_outcome.retained_blobs.saturating_add(reachable.len()),
                deleted_blobs: inner_outcome.deleted_blobs.saturating_add(garbage.len()),
            })
        })
    }
}

async fn normalize_workflow_task_commit<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    batch: WorkflowTaskCommit,
) -> Result<WorkflowTaskCommit>
where
    S: PayloadBlobStore,
{
    let mut append_events = Vec::with_capacity(batch.append_events.len());
    for event in batch.append_events {
        append_events.push(crate::NewHistoryEvent {
            data: normalize_history_event(blob_store, config, event.data).await?,
        });
    }

    let mut schedule_activities = Vec::with_capacity(batch.schedule_activities.len());
    for task in batch.schedule_activities {
        schedule_activities.push(normalize_activity_task(blob_store, config, task).await?);
    }

    let mut schedule_activity_maps = Vec::with_capacity(batch.schedule_activity_maps.len());
    for task in batch.schedule_activity_maps {
        schedule_activity_maps.push(normalize_activity_map_task(blob_store, config, task).await?);
    }

    let mut start_child_workflows = Vec::with_capacity(batch.start_child_workflows.len());
    for message in batch.start_child_workflows {
        start_child_workflows
            .push(normalize_child_start_message(blob_store, config, message).await?);
    }

    let query_projection = match batch.query_projection {
        Some(payload) => Some(normalize_payload_ref(blob_store, config, payload).await?),
        None => None,
    };

    Ok(WorkflowTaskCommit {
        append_events,
        schedule_activities,
        schedule_activity_maps,
        start_child_workflows,
        query_projection,
        ..batch
    })
}

async fn normalize_history_event<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    data: HistoryEventData,
) -> Result<HistoryEventData>
where
    S: PayloadBlobStore,
{
    Ok(match data {
        HistoryEventData::WorkflowStarted {
            workflow_type,
            input,
        } => HistoryEventData::WorkflowStarted {
            workflow_type,
            input: normalize_payload_ref(blob_store, config, input).await?,
        },
        HistoryEventData::WorkflowCompleted { result } => HistoryEventData::WorkflowCompleted {
            result: normalize_payload_ref(blob_store, config, result).await?,
        },
        HistoryEventData::WorkflowFailed { failure } => HistoryEventData::WorkflowFailed {
            failure: normalize_failure(blob_store, config, failure).await?,
        },
        HistoryEventData::WorkflowContinuedAsNew { input } => {
            HistoryEventData::WorkflowContinuedAsNew {
                input: normalize_payload_ref(blob_store, config, input).await?,
            }
        }
        HistoryEventData::ActivityScheduled(scheduled) => HistoryEventData::ActivityScheduled(
            normalize_activity_scheduled(blob_store, config, scheduled).await?,
        ),
        HistoryEventData::ActivityMapScheduled(mut scheduled) => {
            scheduled.input_manifest =
                normalize_activity_map_input_manifest(blob_store, config, scheduled.input_manifest)
                    .await?;
            HistoryEventData::ActivityMapScheduled(scheduled)
        }
        HistoryEventData::ActivityMapCompleted(mut completed) => {
            completed.result_manifest = normalize_activity_map_result_manifest(
                blob_store,
                config,
                completed.result_manifest,
            )
            .await?;
            HistoryEventData::ActivityMapCompleted(completed)
        }
        HistoryEventData::ActivityMapFailed(mut failed) => {
            failed.failure = normalize_failure(blob_store, config, failed.failure).await?;
            HistoryEventData::ActivityMapFailed(failed)
        }
        HistoryEventData::ActivityCompleted(mut completed) => {
            completed.result = normalize_payload_ref(blob_store, config, completed.result).await?;
            HistoryEventData::ActivityCompleted(completed)
        }
        HistoryEventData::ActivityFailed(mut failed) => {
            failed.failure = normalize_failure(blob_store, config, failed.failure).await?;
            HistoryEventData::ActivityFailed(failed)
        }
        HistoryEventData::ChildWorkflowStartRequested(requested) => {
            HistoryEventData::ChildWorkflowStartRequested(
                normalize_child_start_requested(blob_store, config, requested).await?,
            )
        }
        HistoryEventData::ChildWorkflowCompleted(completed) => {
            HistoryEventData::ChildWorkflowCompleted(
                normalize_child_completed(blob_store, config, completed).await?,
            )
        }
        HistoryEventData::ChildWorkflowFailed(failed) => HistoryEventData::ChildWorkflowFailed(
            normalize_child_failed(blob_store, config, failed).await?,
        ),
        HistoryEventData::SignalConsumed(signal) => {
            HistoryEventData::SignalConsumed(normalize_signal(blob_store, config, signal).await?)
        }
        HistoryEventData::SideEffectMarker(marker) => {
            crate::payload::validate_side_effect_marker(&marker)?;
            HistoryEventData::SideEffectMarker(marker)
        }
        data => data,
    })
}

async fn hydrate_history_event<S>(blob_store: &S, event: HistoryEvent) -> Result<HistoryEvent>
where
    S: PayloadBlobStore,
{
    Ok(HistoryEvent {
        data: hydrate_history_event_data(blob_store, event.data).await?,
        ..event
    })
}

async fn hydrate_history_event_data<S>(
    blob_store: &S,
    data: HistoryEventData,
) -> Result<HistoryEventData>
where
    S: PayloadBlobStore,
{
    Ok(match data {
        HistoryEventData::WorkflowStarted {
            workflow_type,
            input,
        } => HistoryEventData::WorkflowStarted {
            workflow_type,
            input: hydrate_payload_ref(blob_store, input).await?,
        },
        HistoryEventData::WorkflowCompleted { result } => HistoryEventData::WorkflowCompleted {
            result: hydrate_payload_ref(blob_store, result).await?,
        },
        HistoryEventData::WorkflowFailed { failure } => HistoryEventData::WorkflowFailed {
            failure: hydrate_failure(blob_store, failure).await?,
        },
        HistoryEventData::WorkflowContinuedAsNew { input } => {
            HistoryEventData::WorkflowContinuedAsNew {
                input: hydrate_payload_ref(blob_store, input).await?,
            }
        }
        HistoryEventData::ActivityScheduled(scheduled) => HistoryEventData::ActivityScheduled(
            hydrate_activity_scheduled(blob_store, scheduled).await?,
        ),
        HistoryEventData::ActivityMapScheduled(mut scheduled) => {
            scheduled.input_manifest =
                hydrate_activity_map_input_manifest(blob_store, scheduled.input_manifest).await?;
            HistoryEventData::ActivityMapScheduled(scheduled)
        }
        HistoryEventData::ActivityMapCompleted(mut completed) => {
            completed.result_manifest =
                hydrate_activity_map_result_manifest(blob_store, completed.result_manifest).await?;
            HistoryEventData::ActivityMapCompleted(completed)
        }
        HistoryEventData::ActivityMapFailed(mut failed) => {
            failed.failure = hydrate_failure(blob_store, failed.failure).await?;
            HistoryEventData::ActivityMapFailed(failed)
        }
        HistoryEventData::ActivityCompleted(mut completed) => {
            completed.result = hydrate_payload_ref(blob_store, completed.result).await?;
            HistoryEventData::ActivityCompleted(completed)
        }
        HistoryEventData::ActivityFailed(mut failed) => {
            failed.failure = hydrate_failure(blob_store, failed.failure).await?;
            HistoryEventData::ActivityFailed(failed)
        }
        HistoryEventData::ChildWorkflowStartRequested(requested) => {
            HistoryEventData::ChildWorkflowStartRequested(
                hydrate_child_start_requested(blob_store, requested).await?,
            )
        }
        HistoryEventData::ChildWorkflowCompleted(completed) => {
            HistoryEventData::ChildWorkflowCompleted(
                hydrate_child_completed(blob_store, completed).await?,
            )
        }
        HistoryEventData::ChildWorkflowFailed(failed) => {
            HistoryEventData::ChildWorkflowFailed(hydrate_child_failed(blob_store, failed).await?)
        }
        HistoryEventData::SignalConsumed(signal) => {
            HistoryEventData::SignalConsumed(hydrate_signal(blob_store, signal).await?)
        }
        HistoryEventData::SideEffectMarker(marker) => {
            crate::payload::validate_side_effect_marker(&marker)?;
            HistoryEventData::SideEffectMarker(marker)
        }
        data => data,
    })
}

async fn normalize_activity_scheduled<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    mut scheduled: ActivityScheduled,
) -> Result<ActivityScheduled>
where
    S: PayloadBlobStore,
{
    scheduled.input = normalize_payload_ref(blob_store, config, scheduled.input).await?;
    Ok(scheduled)
}

async fn hydrate_activity_scheduled<S>(
    blob_store: &S,
    mut scheduled: ActivityScheduled,
) -> Result<ActivityScheduled>
where
    S: PayloadBlobStore,
{
    scheduled.input = hydrate_payload_ref(blob_store, scheduled.input).await?;
    Ok(scheduled)
}

async fn normalize_activity_task<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    mut task: ActivityTask,
) -> Result<ActivityTask>
where
    S: PayloadBlobStore,
{
    task.input = normalize_payload_ref(blob_store, config, task.input).await?;
    Ok(task)
}

async fn hydrate_activity_task<S>(blob_store: &S, mut task: ActivityTask) -> Result<ActivityTask>
where
    S: PayloadBlobStore,
{
    task.input = hydrate_payload_ref(blob_store, task.input).await?;
    Ok(task)
}

async fn normalize_activity_map_task<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    mut task: crate::ActivityMapTask,
) -> Result<crate::ActivityMapTask>
where
    S: PayloadBlobStore,
{
    task.input_manifest = normalize_activity_map_input_manifest_for_operations(
        blob_store,
        config,
        task.input_manifest,
    )
    .await?;
    Ok(task)
}

async fn normalize_child_start_message<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    mut message: ChildStartOutboxMessage,
) -> Result<ChildStartOutboxMessage>
where
    S: PayloadBlobStore,
{
    message.input = normalize_payload_ref(blob_store, config, message.input).await?;
    Ok(message)
}

async fn normalize_child_start_requested<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    mut requested: ChildWorkflowStartRequested,
) -> Result<ChildWorkflowStartRequested>
where
    S: PayloadBlobStore,
{
    requested.input = normalize_payload_ref(blob_store, config, requested.input).await?;
    Ok(requested)
}

async fn hydrate_child_start_requested<S>(
    blob_store: &S,
    mut requested: ChildWorkflowStartRequested,
) -> Result<ChildWorkflowStartRequested>
where
    S: PayloadBlobStore,
{
    requested.input = hydrate_payload_ref(blob_store, requested.input).await?;
    Ok(requested)
}

async fn normalize_child_completed<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    mut completed: ChildWorkflowCompleted,
) -> Result<ChildWorkflowCompleted>
where
    S: PayloadBlobStore,
{
    completed.result = normalize_payload_ref(blob_store, config, completed.result).await?;
    Ok(completed)
}

async fn hydrate_child_completed<S>(
    blob_store: &S,
    mut completed: ChildWorkflowCompleted,
) -> Result<ChildWorkflowCompleted>
where
    S: PayloadBlobStore,
{
    completed.result = hydrate_payload_ref(blob_store, completed.result).await?;
    Ok(completed)
}

async fn normalize_child_failed<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    mut failed: ChildWorkflowFailed,
) -> Result<ChildWorkflowFailed>
where
    S: PayloadBlobStore,
{
    failed.failure = normalize_failure(blob_store, config, failed.failure).await?;
    Ok(failed)
}

async fn hydrate_child_failed<S>(
    blob_store: &S,
    mut failed: ChildWorkflowFailed,
) -> Result<ChildWorkflowFailed>
where
    S: PayloadBlobStore,
{
    failed.failure = hydrate_failure(blob_store, failed.failure).await?;
    Ok(failed)
}

async fn normalize_signal<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    mut signal: SignalConsumed,
) -> Result<SignalConsumed>
where
    S: PayloadBlobStore,
{
    signal.payload = normalize_payload_ref(blob_store, config, signal.payload).await?;
    Ok(signal)
}

async fn hydrate_signal<S>(blob_store: &S, mut signal: SignalConsumed) -> Result<SignalConsumed>
where
    S: PayloadBlobStore,
{
    signal.payload = hydrate_payload_ref(blob_store, signal.payload).await?;
    Ok(signal)
}

async fn normalize_failure<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    mut failure: DurableFailure,
) -> Result<DurableFailure>
where
    S: PayloadBlobStore,
{
    if let Some(details) = failure.details.take() {
        failure.details = Some(normalize_payload_ref(blob_store, config, details).await?);
    }
    Ok(failure)
}

async fn hydrate_failure<S>(blob_store: &S, mut failure: DurableFailure) -> Result<DurableFailure>
where
    S: PayloadBlobStore,
{
    if let Some(details) = failure.details.take() {
        failure.details = Some(hydrate_payload_ref(blob_store, details).await?);
    }
    Ok(failure)
}

async fn normalize_activity_map_input_manifest<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef>
where
    S: PayloadBlobStore,
{
    let root = hydrate_payload_ref(blob_store, payload).await?;
    let root_codec = root.codec();
    let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
    let mut pages = Vec::with_capacity(manifest.pages.len());
    for page in manifest.pages {
        let page = hydrate_payload_ref(blob_store, page).await?;
        let page_codec = page.codec();
        let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
        let mut items = Vec::with_capacity(page.items.len());
        for item in page.items {
            items.push(normalize_payload_ref(blob_store, config, item).await?);
        }
        page.items = items;
        pages.push(
            normalize_payload_ref(
                blob_store,
                config,
                crate::encode_payload_with_codec(&page, page_codec)?,
            )
            .await?,
        );
    }
    manifest.pages = pages;
    normalize_payload_ref(
        blob_store,
        config,
        crate::encode_payload_with_codec(&manifest, root_codec)?,
    )
    .await
}

async fn normalize_activity_map_input_manifest_for_operations<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef>
where
    S: PayloadBlobStore,
{
    let root = hydrate_payload_ref(blob_store, payload).await?;
    let root_codec = root.codec();
    let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
    let mut pages = Vec::with_capacity(manifest.pages.len());
    for page in manifest.pages {
        let page = hydrate_payload_ref(blob_store, page).await?;
        let page_codec = page.codec();
        let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
        let mut items = Vec::with_capacity(page.items.len());
        for item in page.items {
            items.push(normalize_payload_ref(blob_store, config, item).await?);
        }
        page.items = items;
        pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
    }
    manifest.pages = pages;
    crate::encode_payload_with_codec(&manifest, root_codec)
}

async fn hydrate_activity_map_input_manifest<S>(
    blob_store: &S,
    payload: PayloadRef,
) -> Result<PayloadRef>
where
    S: PayloadBlobStore,
{
    let root = hydrate_payload_ref(blob_store, payload).await?;
    let root_codec = root.codec();
    let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
    let mut pages = Vec::with_capacity(manifest.pages.len());
    for page in manifest.pages {
        let page = hydrate_payload_ref(blob_store, page).await?;
        let page_codec = page.codec();
        let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
        let mut items = Vec::with_capacity(page.items.len());
        for item in page.items {
            items.push(hydrate_payload_ref(blob_store, item).await?);
        }
        page.items = items;
        pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
    }
    manifest.pages = pages;
    crate::encode_payload_with_codec(&manifest, root_codec)
}

async fn normalize_activity_map_result_manifest<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef>
where
    S: PayloadBlobStore,
{
    let root = hydrate_payload_ref(blob_store, payload).await?;
    let root_codec = root.codec();
    let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
    let mut pages = Vec::with_capacity(manifest.pages.len());
    for page in manifest.pages {
        let page = hydrate_payload_ref(blob_store, page).await?;
        let page_codec = page.codec();
        let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
        let mut results = Vec::with_capacity(page.results.len());
        for result in page.results {
            results.push(normalize_payload_ref(blob_store, config, result).await?);
        }
        page.results = results;
        pages.push(
            normalize_payload_ref(
                blob_store,
                config,
                crate::encode_payload_with_codec(&page, page_codec)?,
            )
            .await?,
        );
    }
    manifest.pages = pages;
    normalize_payload_ref(
        blob_store,
        config,
        crate::encode_payload_with_codec(&manifest, root_codec)?,
    )
    .await
}

async fn hydrate_activity_map_result_manifest<S>(
    blob_store: &S,
    payload: PayloadRef,
) -> Result<PayloadRef>
where
    S: PayloadBlobStore,
{
    let root = hydrate_payload_ref(blob_store, payload).await?;
    let root_codec = root.codec();
    let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
    let mut pages = Vec::with_capacity(manifest.pages.len());
    for page in manifest.pages {
        let page = hydrate_payload_ref(blob_store, page).await?;
        let page_codec = page.codec();
        let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
        let mut results = Vec::with_capacity(page.results.len());
        for result in page.results {
            results.push(hydrate_payload_ref(blob_store, result).await?);
        }
        page.results = results;
        pages.push(crate::encode_payload_with_codec(&page, page_codec)?);
    }
    manifest.pages = pages;
    crate::encode_payload_with_codec(&manifest, root_codec)
}

async fn collect_reachable_external_blobs<S>(
    blob_store: &S,
    roots: Vec<PayloadRootRef>,
    reachable: &mut BTreeSet<String>,
) -> Result<()>
where
    S: PayloadBlobStore,
{
    for root in roots {
        match root {
            PayloadRootRef::Payload(payload) => {
                collect_reachable_external_payload(blob_store, &payload, reachable).await?;
            }
            PayloadRootRef::ActivityMapInputManifest(payload) => {
                collect_reachable_external_input_manifest(blob_store, payload, reachable).await?;
            }
            PayloadRootRef::ActivityMapResultManifest(payload) => {
                collect_reachable_external_result_manifest(blob_store, payload, reachable).await?;
            }
        }
    }
    Ok(())
}

async fn collect_reachable_external_payload<S>(
    blob_store: &S,
    payload: &PayloadRef,
    reachable: &mut BTreeSet<String>,
) -> Result<()>
where
    S: PayloadBlobStore,
{
    let PayloadRef::Blob { digest, uri, .. } = payload else {
        return Ok(());
    };
    if !blob_store.owns_payload_blob_uri(uri) {
        return Ok(());
    }
    load_payload_blob(blob_store, payload).await?;
    reachable.insert(digest.clone());
    Ok(())
}

async fn load_external_container<S>(
    blob_store: &S,
    payload: PayloadRef,
    reachable: &mut BTreeSet<String>,
    context: &str,
) -> Result<PayloadRef>
where
    S: PayloadBlobStore,
{
    let (digest, uri) = match &payload {
        PayloadRef::Inline { .. } => return Ok(payload),
        PayloadRef::Blob { digest, uri, .. } => (digest.clone(), uri.clone()),
    };
    if !blob_store.owns_payload_blob_uri(&uri) {
        return Err(Error::PayloadDecode(format!(
            "{context} references a non-wrapper payload blob `{uri}`"
        )));
    }
    let hydrated = hydrate_payload_ref(blob_store, payload).await?;
    reachable.insert(digest);
    Ok(hydrated)
}

async fn collect_reachable_external_input_manifest<S>(
    blob_store: &S,
    payload: PayloadRef,
    reachable: &mut BTreeSet<String>,
) -> Result<()>
where
    S: PayloadBlobStore,
{
    let root = load_external_container(
        blob_store,
        payload,
        reachable,
        "activity map input manifest root",
    )
    .await?;
    let manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
    for page in manifest.pages {
        let page = load_external_container(
            blob_store,
            page,
            reachable,
            "activity map input manifest page",
        )
        .await?;
        let page: ActivityMapInputPage = crate::decode_payload(&page)?;
        for item in page.items {
            collect_reachable_external_payload(blob_store, &item, reachable).await?;
        }
    }
    Ok(())
}

async fn collect_reachable_external_result_manifest<S>(
    blob_store: &S,
    payload: PayloadRef,
    reachable: &mut BTreeSet<String>,
) -> Result<()>
where
    S: PayloadBlobStore,
{
    let root = load_external_container(
        blob_store,
        payload,
        reachable,
        "activity map result manifest root",
    )
    .await?;
    let manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
    for page in manifest.pages {
        let page = load_external_container(
            blob_store,
            page,
            reachable,
            "activity map result manifest page",
        )
        .await?;
        let page: ActivityMapResultPage = crate::decode_payload(&page)?;
        for result in page.results {
            collect_reachable_external_payload(blob_store, &result, reachable).await?;
        }
    }
    Ok(())
}

async fn normalize_payload_ref<S>(
    blob_store: &S,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef>
where
    S: PayloadBlobStore,
{
    match payload {
        PayloadRef::Inline {
            codec,
            schema_fingerprint,
            compression,
            encryption,
            bytes,
        } if bytes.len() > config.inline_threshold_bytes => {
            let digest = digest_bytes(&bytes);
            let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let uri = blob_store.put_payload_blob(digest.clone(), bytes).await?;
            Ok(PayloadRef::Blob {
                codec,
                schema_fingerprint,
                compression,
                encryption,
                digest,
                size,
                uri,
            })
        }
        payload @ PayloadRef::Inline { .. } => Ok(payload),
        payload @ PayloadRef::Blob { .. } => {
            load_payload_blob(blob_store, &payload).await?;
            Ok(payload)
        }
    }
}

async fn hydrate_payload_ref<S>(blob_store: &S, payload: PayloadRef) -> Result<PayloadRef>
where
    S: PayloadBlobStore,
{
    match payload {
        payload @ PayloadRef::Inline { .. } => Ok(payload),
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
            let blob = load_payload_blob(blob_store, &payload).await?;
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

async fn load_payload_blob<S>(blob_store: &S, payload: &PayloadRef) -> Result<PayloadBlob>
where
    S: PayloadBlobStore,
{
    let PayloadRef::Blob {
        codec,
        schema_fingerprint,
        compression,
        encryption,
        digest,
        size,
        ..
    } = payload
    else {
        return Err(Error::PayloadDecode(
            "inline payload does not reference blob storage".to_owned(),
        ));
    };
    let bytes = blob_store.get_payload_blob(digest.clone()).await?;
    validate_payload_blob_bytes(digest, *size, &bytes)?;
    Ok(PayloadBlob {
        codec: *codec,
        schema_fingerprint: schema_fingerprint.clone(),
        compression: *compression,
        encryption: encryption.clone(),
        bytes,
    })
}

fn validate_payload_blob_bytes(digest: &str, expected_size: u64, bytes: &[u8]) -> Result<()> {
    let actual_digest = digest_bytes(bytes);
    if actual_digest != digest {
        return Err(Error::PayloadDecode(format!(
            "payload blob digest mismatch: expected `{digest}`, got `{actual_digest}`"
        )));
    }
    let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual_size != expected_size {
        return Err(Error::PayloadDecode(format!(
            "payload blob size mismatch: expected {expected_size}, got {actual_size}"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct MemoryBlobStore {
    blobs: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn payload_blob_count(&self) -> usize {
        self.blobs
            .lock()
            .expect("memory blob store mutex poisoned")
            .len()
    }
}

impl PayloadBlobStore for MemoryBlobStore {
    fn put_payload_blob(
        &self,
        digest: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<String>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            validate_payload_blob_bytes(
                &digest,
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                &bytes,
            )?;
            let mut blobs = blobs.lock().expect("memory blob store mutex poisoned");
            if let Some(existing) = blobs.get(&digest) {
                validate_payload_blob_bytes(
                    &digest,
                    u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                    existing,
                )?;
            } else {
                blobs.insert(digest.clone(), bytes);
            }
            Ok(memory_blob_uri(&digest))
        })
    }

    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, Result<Vec<u8>>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            blobs
                .lock()
                .expect("memory blob store mutex poisoned")
                .get(&digest)
                .cloned()
                .ok_or_else(|| Error::PayloadDecode(format!("missing payload blob `{digest}`")))
        })
    }

    fn list_payload_blob_digests(&self) -> BoxFuture<'static, Result<BTreeSet<String>>> {
        let blobs = self.blobs.clone();
        Box::pin(ready(Ok(blobs
            .lock()
            .expect("memory blob store mutex poisoned")
            .keys()
            .cloned()
            .collect())))
    }

    fn delete_payload_blob(&self, digest: String) -> BoxFuture<'static, Result<()>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            blobs
                .lock()
                .expect("memory blob store mutex poisoned")
                .remove(&digest);
            Ok(())
        })
    }

    fn owns_payload_blob_uri(&self, uri: &str) -> bool {
        uri.starts_with("memory-blob://payload/")
    }
}

fn memory_blob_uri(digest: &str) -> String {
    format!("memory-blob://payload/{digest}")
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3BlobStoreConfig {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub prefix: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Clone)]
pub struct S3BlobStore {
    bucket: Arc<s3::Bucket>,
    bucket_name: String,
    prefix: String,
}

impl S3BlobStore {
    pub fn new(config: S3BlobStoreConfig) -> Result<Self> {
        let credentials = s3::creds::Credentials::new(
            Some(config.access_key_id.as_str()),
            Some(config.secret_access_key.as_str()),
            None,
            None,
            None,
        )
        .map_err(|err| Error::Backend(format!("S3 payload store credentials error: {err}")))?;
        let region = s3::Region::Custom {
            region: config.region.clone(),
            endpoint: config.endpoint,
        };
        let bucket = s3::Bucket::new(&config.bucket, region, credentials)
            .map_err(s3_backend_error)?
            .with_path_style();
        Ok(Self {
            bucket: Arc::new(*bucket),
            bucket_name: config.bucket,
            prefix: normalize_s3_prefix(&config.prefix),
        })
    }

    pub fn garage(config: S3BlobStoreConfig) -> Result<Self> {
        Self::new(config)
    }
}

impl PayloadBlobStore for S3BlobStore {
    fn put_payload_blob(
        &self,
        digest: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<String>> {
        let bucket = self.bucket.clone();
        let bucket_name = self.bucket_name.clone();
        let key = s3_key(&self.prefix, &digest);
        Box::pin(async move {
            validate_payload_blob_bytes(
                &digest,
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                &bytes,
            )?;
            let response = bucket
                .put_object(&key, &bytes)
                .await
                .map_err(s3_backend_error)?;
            require_s3_success("put payload blob", response.status_code())?;
            Ok(s3_blob_uri(&bucket_name, &key))
        })
    }

    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, Result<Vec<u8>>> {
        let bucket = self.bucket.clone();
        let key = s3_key(&self.prefix, &digest);
        Box::pin(async move {
            let response = bucket
                .get_object(&key)
                .await
                .map_err(|err| s3_payload_decode_error("read payload blob", err))?;
            require_s3_success("read payload blob", response.status_code())?;
            Ok(response.to_vec())
        })
    }

    fn list_payload_blob_digests(&self) -> BoxFuture<'static, Result<BTreeSet<String>>> {
        let bucket = self.bucket.clone();
        let prefix = self.prefix.clone();
        Box::pin(async move {
            let results = bucket
                .list(prefix.clone(), None)
                .await
                .map_err(s3_backend_error)?;
            let mut digests = BTreeSet::new();
            for page in results {
                for object in page.contents {
                    if let Some(digest) = digest_from_s3_key(&prefix, &object.key) {
                        digests.insert(digest);
                    }
                }
            }
            Ok(digests)
        })
    }

    fn delete_payload_blob(&self, digest: String) -> BoxFuture<'static, Result<()>> {
        let bucket = self.bucket.clone();
        let key = s3_key(&self.prefix, &digest);
        Box::pin(async move {
            let response = bucket.delete_object(&key).await.map_err(s3_backend_error)?;
            require_s3_success("delete payload blob", response.status_code())?;
            Ok(())
        })
    }

    fn owns_payload_blob_uri(&self, uri: &str) -> bool {
        let bucket_prefix = format!("s3://{}/", self.bucket_name);
        let Some(key) = uri.strip_prefix(&bucket_prefix) else {
            return false;
        };
        digest_from_s3_key(&self.prefix, key).is_some()
    }
}

fn normalize_s3_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_owned()
}

fn s3_key(prefix: &str, digest: &str) -> String {
    if prefix.is_empty() {
        digest.to_owned()
    } else {
        format!("{prefix}/{digest}")
    }
}

fn digest_from_s3_key(prefix: &str, key: &str) -> Option<String> {
    let digest = if prefix.is_empty() {
        key
    } else {
        key.strip_prefix(prefix)?.strip_prefix('/')?
    };
    digest.starts_with("sha256:").then(|| digest.to_owned())
}

fn s3_blob_uri(bucket: &str, key: &str) -> String {
    format!("s3://{bucket}/{key}")
}

fn require_s3_success(operation: &str, status: u16) -> Result<()> {
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(Error::Backend(format!(
            "{operation} failed with S3 status {status}"
        )))
    }
}

fn s3_backend_error(err: s3::error::S3Error) -> Error {
    Error::Backend(format!("S3 payload store error: {err}"))
}

fn s3_payload_decode_error(operation: &str, err: s3::error::S3Error) -> Error {
    Error::PayloadDecode(format!("{operation} failed: {err}"))
}
