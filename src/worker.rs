use crate::{
    ActivityHeartbeatRequest, ClaimActivityOptions, ClaimWorkflowTaskOptions,
    CompleteActivityRequest, DurableBackend, Error, EventId, FailActivityRequest,
    FireDueTimersRequest, HistoryEvent, HistoryEventData, Namespace, NewHistoryEvent,
    ReadSignalInboxRequest, Registry, Result, RunId, StartWorkflowRequest, TaskQueue,
    TimeoutDueActivitiesRequest, WorkerId, Workflow, WorkflowChangeVersionsRequest, WorkflowId,
    WorkflowTaskCommit, WorkflowTaskReason, WorkflowTaskRelease, poll_with_activity_context,
    poll_with_runtime_context,
};
use futures::Future;
use serde::Serialize;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerRunOptions {
    pub max_iterations: usize,
}

impl Default for WorkerRunOptions {
    fn default() -> Self {
        Self {
            max_iterations: 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkerRunStats {
    pub workflow_tasks: usize,
    pub activity_tasks: usize,
    pub timers_fired: usize,
    pub activities_timed_out: usize,
    pub child_workflow_starts_dispatched: usize,
}

pub struct Client<B>
where
    B: DurableBackend,
{
    backend: B,
    namespace: Namespace,
    payload_codec: crate::CodecId,
}

impl<B> Client<B>
where
    B: DurableBackend,
{
    pub fn new(backend: B) -> Self {
        let payload_codec = backend.payload_storage_config().codec;
        Self {
            backend,
            namespace: Namespace::default(),
            payload_codec,
        }
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Namespace::new(namespace);
        self
    }

    pub async fn start_workflow<W>(
        &self,
        workflow_id: impl Into<String>,
        task_queue: impl Into<String>,
        input: W::Input,
    ) -> Result<RunId>
    where
        W: Workflow,
    {
        let outcome = self
            .backend
            .start_workflow(StartWorkflowRequest {
                namespace: self.namespace.clone(),
                workflow_id: WorkflowId::new(workflow_id),
                workflow_type: W::workflow_type(),
                task_queue: TaskQueue::new(task_queue),
                input: crate::encode_payload_with_codec(&input, self.payload_codec)?,
            })
            .await?;
        Ok(outcome.run_id().clone())
    }

    pub async fn signal_workflow<T>(
        &self,
        workflow_id: impl Into<String>,
        signal_name: impl Into<String>,
        signal_id: impl Into<String>,
        payload: T,
    ) -> Result<crate::SignalWorkflowOutcome>
    where
        T: Serialize,
    {
        self.backend
            .signal_workflow(crate::SignalWorkflowRequest {
                namespace: self.namespace.clone(),
                workflow_id: WorkflowId::new(workflow_id),
                signal_id: crate::SignalId::new(signal_id),
                signal_name: crate::SignalName::new(signal_name),
                payload: crate::encode_payload_with_codec(&payload, self.payload_codec)?,
            })
            .await
    }

    pub async fn cancel_workflow(
        &self,
        workflow_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<crate::CancelWorkflowOutcome> {
        self.backend
            .cancel_workflow(crate::CancelWorkflowRequest {
                namespace: self.namespace.clone(),
                workflow_id: WorkflowId::new(workflow_id),
                reason: reason.into(),
            })
            .await
    }

    pub async fn query_projection<W>(
        &self,
        workflow_id: impl Into<String>,
    ) -> Result<Option<W::QueryState>>
    where
        W: Workflow,
    {
        match self
            .backend
            .query_projection(crate::QueryProjectionRequest {
                namespace: self.namespace.clone(),
                workflow_id: WorkflowId::new(workflow_id),
            })
            .await?
        {
            crate::QueryProjectionOutcome::Found { payload, .. } => {
                Ok(Some(crate::decode_payload::<W::QueryState>(&payload)?))
            }
            crate::QueryProjectionOutcome::NotFound => Ok(None),
        }
    }
}

pub struct Worker<B>
where
    B: DurableBackend,
{
    backend: B,
    namespace: Namespace,
    worker_id: WorkerId,
    workflow_task_queue: TaskQueue,
    activity_task_queue: TaskQueue,
    registry: Registry,
    cache: BTreeMap<RunId, CachedWorkflow>,
    history_chunk_events: usize,
    history_chunk_bytes: usize,
    payload_codec: crate::CodecId,
    nondeterminism_retry_backoff: Duration,
    recovery_flow_control: RecoveryFlowControl,
    active_recoveries: usize,
    max_local_activities_per_workflow_task: usize,
    completed_local_activity_tasks: usize,
}

struct CachedWorkflow {
    future: Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
    last_event_id: EventId,
    next_command_seq: u64,
    default_activity_options: crate::ActivityOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RecoveryFlowControl {
    max_concurrent_recoveries: usize,
    replay_event_budget: usize,
    replay_byte_budget: usize,
    prefetch_chunks: usize,
    defer_delay: Duration,
}

impl Default for RecoveryFlowControl {
    fn default() -> Self {
        Self {
            max_concurrent_recoveries: usize::MAX,
            replay_event_budget: usize::MAX,
            replay_byte_budget: usize::MAX,
            prefetch_chunks: usize::MAX,
            defer_delay: Duration::from_millis(100),
        }
    }
}

#[derive(Clone, Debug)]
struct RecoveryReplayBudget {
    remaining_events: usize,
    remaining_bytes: usize,
    remaining_chunks: usize,
}

impl RecoveryReplayBudget {
    fn new(flow_control: RecoveryFlowControl) -> Self {
        Self {
            remaining_events: flow_control.replay_event_budget,
            remaining_bytes: flow_control.replay_byte_budget,
            remaining_chunks: flow_control.prefetch_chunks,
        }
    }

    fn next_request_limits(
        &self,
        chunk_events: usize,
        chunk_bytes: usize,
    ) -> Option<(usize, usize)> {
        if self.remaining_events == 0 || self.remaining_bytes == 0 || self.remaining_chunks == 0 {
            return None;
        }

        Some((
            chunk_events.min(self.remaining_events),
            chunk_bytes.min(self.remaining_bytes),
        ))
    }

    fn record_chunk(&mut self, chunk: &crate::HistoryChunk) {
        self.remaining_chunks = self.remaining_chunks.saturating_sub(1);
        self.remaining_events = self.remaining_events.saturating_sub(chunk.events.len());
        let bytes = chunk
            .events
            .iter()
            .map(|event| crate::runtime::event_payload_len(&event.data).max(1))
            .sum::<usize>();
        self.remaining_bytes = self.remaining_bytes.saturating_sub(bytes);
    }
}

enum WorkflowTaskOutcome {
    Finished(Option<CachedWorkflow>),
    Deferred,
}

enum WorkflowPollOutcome {
    Ready(Poll<Result<crate::PayloadRef>>),
    Deferred,
}

impl<B> Worker<B>
where
    B: DurableBackend,
{
    pub fn builder(backend: B) -> WorkerBuilder<B> {
        WorkerBuilder {
            backend,
            namespace: Namespace::default(),
            worker_id: WorkerId::new("worker"),
            workflow_task_queue: TaskQueue::default(),
            activity_task_queue: TaskQueue::default(),
            registry: Registry::default(),
            history_chunk_events: 128,
            history_chunk_bytes: 256 * 1024,
            nondeterminism_retry_backoff: Duration::from_secs(60),
            recovery_flow_control: RecoveryFlowControl::default(),
            max_local_activities_per_workflow_task: 0,
        }
    }

    pub async fn run_workflow_once(&mut self) -> Result<bool> {
        let claim = self
            .backend
            .claim_workflow_task(
                self.worker_id.clone(),
                ClaimWorkflowTaskOptions {
                    namespace: self.namespace.clone(),
                    task_queue: self.workflow_task_queue.clone(),
                    registered_workflow_types: self.registry.workflow_types(),
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await?;

        let Some(claimed) = claim else {
            return Ok(false);
        };

        let run_id = claimed.run_id.clone();
        let claim_for_release = claimed.claim.clone();
        let cached = self.cache.remove(&claimed.run_id);
        let now = self.backend.current_time().await?;
        let change_versions = self
            .backend
            .workflow_change_versions(WorkflowChangeVersionsRequest {
                namespace: self.namespace.clone(),
                workflow_id: None,
                run_id: Some(claimed.run_id.clone()),
                change_id: None,
            })
            .await?
            .records;
        let entry_result = if let Some(mut cached) = cached {
            let chunk = self
                .stream_history_chunk(
                    claimed.run_id.clone(),
                    cached.last_event_id,
                    claimed.replay_target_event_id,
                )
                .await;
            match chunk {
                Ok(chunk) => {
                    let mut context = crate::runtime::RuntimeContext::new(
                        claimed.run_id.clone(),
                        self.workflow_task_queue.clone(),
                        self.activity_task_queue.clone(),
                        self.payload_codec,
                        now,
                        chunk.events,
                        cached.default_activity_options,
                        cached.next_command_seq,
                        chunk.last_event_id,
                        claimed.replay_target_event_id,
                        change_versions.clone(),
                    );
                    let poll = self
                        .poll_until_history_blocked_or_ready(
                            &claimed.run_id,
                            &mut cached.future,
                            &mut context,
                            claimed.replay_target_event_id,
                            None,
                        )
                        .await;
                    match poll {
                        Ok(poll) => match poll {
                            WorkflowPollOutcome::Ready(poll) => self
                                .finish_workflow_poll(claimed, cached.future, context, poll)
                                .await
                                .map(WorkflowTaskOutcome::Finished),
                            WorkflowPollOutcome::Deferred => Ok(WorkflowTaskOutcome::Deferred),
                        },
                        Err(err) => Err(err),
                    }
                }
                Err(err) => Err(err),
            }
        } else {
            let is_recovery = claimed.replay_target_event_id > EventId(1);
            if is_recovery && !self.try_acquire_recovery() {
                self.defer_workflow_task(claimed.claim, self.recovery_flow_control.defer_delay)
                    .await
            } else {
                let mut recovery_budget =
                    is_recovery.then(|| RecoveryReplayBudget::new(self.recovery_flow_control));
                let first_chunk = match recovery_budget.as_mut() {
                    Some(budget) => {
                        self.stream_recovery_history_chunk(
                            claimed.run_id.clone(),
                            EventId::ZERO,
                            claimed.replay_target_event_id,
                            budget,
                        )
                        .await
                    }
                    None => self
                        .stream_history_chunk(
                            claimed.run_id.clone(),
                            EventId::ZERO,
                            claimed.replay_target_event_id,
                        )
                        .await
                        .map(Some),
                };
                let result = match first_chunk {
                    Ok(Some(first_chunk)) => {
                        let last_loaded_event_id = first_chunk.last_event_id;
                        match split_start_event(&first_chunk.events) {
                            Err(err) => Err(err),
                            Ok((input, replay_events)) => {
                                let input = self.hydrate_payload_for_decode(input).await?;
                                match self.registry.workflow(&claimed.workflow_type) {
                                    None => Err(Error::WorkflowNotRegistered(
                                        claimed.workflow_type.clone(),
                                    )),
                                    Some(registration) => {
                                        let mut future =
                                            registration.run(input, self.payload_codec);
                                        let mut context = crate::runtime::RuntimeContext::new(
                                            claimed.run_id.clone(),
                                            self.workflow_task_queue.clone(),
                                            self.activity_task_queue.clone(),
                                            self.payload_codec,
                                            now,
                                            replay_events,
                                            crate::ActivityOptions::default(),
                                            0,
                                            last_loaded_event_id,
                                            claimed.replay_target_event_id,
                                            change_versions.clone(),
                                        );
                                        let poll = self
                                            .poll_until_history_blocked_or_ready(
                                                &claimed.run_id,
                                                &mut future,
                                                &mut context,
                                                claimed.replay_target_event_id,
                                                recovery_budget.as_mut(),
                                            )
                                            .await;
                                        match poll {
                                            Ok(WorkflowPollOutcome::Ready(poll)) => self
                                                .finish_workflow_poll(
                                                    claimed, future, context, poll,
                                                )
                                                .await
                                                .map(WorkflowTaskOutcome::Finished),
                                            Ok(WorkflowPollOutcome::Deferred) => {
                                                self.defer_workflow_task(
                                                    claimed.claim,
                                                    self.recovery_flow_control.defer_delay,
                                                )
                                                .await
                                            }
                                            Err(err) => Err(err),
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        self.defer_workflow_task(
                            claimed.claim,
                            self.recovery_flow_control.defer_delay,
                        )
                        .await
                    }
                    Err(err) => Err(err),
                };
                if is_recovery {
                    self.release_recovery();
                }
                result
            }
        };

        let entry = match entry_result {
            Ok(entry) => entry,
            Err(err) => {
                if let Error::Backpressure { retry_after, .. } = &err {
                    let delay = self.backpressure_delay(*retry_after);
                    let _ = self
                        .backend
                        .release_workflow_task(
                            claim_for_release,
                            WorkflowTaskRelease::delayed(WorkflowTaskReason::CacheEvicted, delay),
                        )
                        .await;
                    return Ok(true);
                }
                let release = if matches!(
                    &err,
                    Error::Nondeterminism(_) | Error::UnsupportedWorkflowVersion { .. }
                ) {
                    WorkflowTaskRelease::delayed(
                        WorkflowTaskReason::CacheEvicted,
                        self.nondeterminism_retry_backoff,
                    )
                } else {
                    WorkflowTaskRelease::immediate(WorkflowTaskReason::CacheEvicted)
                };
                let _ = self
                    .backend
                    .release_workflow_task(claim_for_release, release)
                    .await;
                return Err(err);
            }
        };

        let WorkflowTaskOutcome::Finished(entry) = entry else {
            return Ok(true);
        };

        if let Some(entry) = entry {
            self.cache.insert(run_id, entry);
        }
        self.run_local_activities_after_workflow_task().await?;
        Ok(true)
    }

    pub async fn run_activity_once(&mut self) -> Result<bool> {
        let claim = self
            .backend
            .claim_activity_task(
                self.worker_id.clone(),
                ClaimActivityOptions {
                    namespace: self.namespace.clone(),
                    task_queue: self.activity_task_queue.clone(),
                    registered_activity_names: self.registry.activity_names(),
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await?;

        let Some(claimed) = claim else {
            return Ok(false);
        };

        let registration = self
            .registry
            .activity(&claimed.task.activity_name)
            .ok_or_else(|| Error::ActivityNotRegistered(claimed.task.activity_name.clone()))?;
        let heartbeat_backend = self.backend.clone();
        let heartbeat_claim = claimed.claim.clone();
        let activity_context = crate::runtime::ActivityRuntimeContext::new(move || {
            let backend = heartbeat_backend.clone();
            let claim = heartbeat_claim.clone();
            Box::pin(async move {
                backend
                    .heartbeat_activity(ActivityHeartbeatRequest { claim })
                    .await
            })
        });
        let mut future = registration.run(claimed.task.input, self.payload_codec);
        let result = std::future::poll_fn(|cx| {
            poll_with_activity_context(&activity_context, || future.as_mut().poll(cx))
        })
        .await;

        match result {
            Ok(result) => {
                self.backend
                    .complete_activity(CompleteActivityRequest {
                        claim: claimed.claim,
                        result,
                    })
                    .await?;
            }
            Err(err) => {
                self.backend
                    .fail_activity(FailActivityRequest {
                        claim: claimed.claim,
                        failure: err.durable_failure(),
                    })
                    .await?;
            }
        }
        Ok(true)
    }

    pub async fn run_timers_once(&mut self) -> Result<usize> {
        let now = self.backend.current_time().await?;
        let outcome = self
            .backend
            .fire_due_timers(FireDueTimersRequest {
                namespace: self.namespace.clone(),
                now,
                limit: 1024,
            })
            .await?;
        Ok(outcome.fired)
    }

    pub async fn run_activity_timeouts_once(&mut self) -> Result<usize> {
        let now = self.backend.current_time().await?;
        let outcome = self
            .backend
            .timeout_due_activities(TimeoutDueActivitiesRequest {
                namespace: self.namespace.clone(),
                now,
                limit: 1024,
            })
            .await?;
        Ok(outcome.timed_out)
    }

    pub async fn run_child_workflow_starts_once(&mut self) -> Result<usize> {
        let outcome = self
            .backend
            .dispatch_child_workflow_starts(crate::DispatchChildWorkflowStartsRequest {
                namespace: self.namespace.clone(),
                limit: 1024,
            })
            .await?;
        Ok(outcome.dispatched)
    }

    pub async fn run_until_idle(&mut self) -> Result<WorkerRunStats> {
        self.run_until_idle_with(WorkerRunOptions::default()).await
    }

    pub async fn run_until_idle_with(&mut self, opts: WorkerRunOptions) -> Result<WorkerRunStats> {
        let mut stats = WorkerRunStats::default();
        for _ in 0..opts.max_iterations {
            let mut progressed = false;

            if self.run_workflow_once().await? {
                stats.workflow_tasks += 1;
                progressed = true;
            }
            let local_activity_tasks = self.take_completed_local_activity_tasks();
            if local_activity_tasks > 0 {
                stats.activity_tasks += local_activity_tasks;
                progressed = true;
            }
            let timers_fired = self.run_timers_once().await?;
            if timers_fired > 0 {
                stats.timers_fired += timers_fired;
                progressed = true;
            }
            let activities_timed_out = self.run_activity_timeouts_once().await?;
            if activities_timed_out > 0 {
                stats.activities_timed_out += activities_timed_out;
                progressed = true;
            }
            let child_starts = self.run_child_workflow_starts_once().await?;
            if child_starts > 0 {
                stats.child_workflow_starts_dispatched += child_starts;
                progressed = true;
            }
            if self.run_activity_once().await? {
                stats.activity_tasks += 1;
                progressed = true;
            }

            if !progressed {
                return Ok(stats);
            }
        }

        Err(Error::Backend(format!(
            "worker did not become idle within {} iterations",
            opts.max_iterations
        )))
    }

    async fn stream_history_chunk(
        &self,
        run_id: RunId,
        after_event_id: EventId,
        up_to_event_id: EventId,
    ) -> Result<crate::HistoryChunk> {
        if after_event_id >= up_to_event_id {
            return Ok(crate::HistoryChunk {
                events: Vec::new(),
                last_event_id: after_event_id,
                has_more: false,
            });
        }
        self.backend
            .stream_history_for_replay(crate::StreamHistoryRequest {
                run_id,
                after_event_id,
                up_to_event_id,
                max_events: self.history_chunk_events,
                max_bytes: self.history_chunk_bytes,
            })
            .await
    }

    async fn stream_recovery_history_chunk(
        &self,
        run_id: RunId,
        after_event_id: EventId,
        up_to_event_id: EventId,
        budget: &mut RecoveryReplayBudget,
    ) -> Result<Option<crate::HistoryChunk>> {
        if after_event_id >= up_to_event_id {
            return Ok(Some(crate::HistoryChunk {
                events: Vec::new(),
                last_event_id: after_event_id,
                has_more: false,
            }));
        }

        let Some((max_events, max_bytes)) =
            budget.next_request_limits(self.history_chunk_events, self.history_chunk_bytes)
        else {
            return Ok(None);
        };

        let chunk = self
            .backend
            .stream_history_for_replay(crate::StreamHistoryRequest {
                run_id,
                after_event_id,
                up_to_event_id,
                max_events,
                max_bytes,
            })
            .await?;
        budget.record_chunk(&chunk);
        Ok(Some(chunk))
    }

    async fn poll_until_history_blocked_or_ready(
        &self,
        run_id: &RunId,
        future: &mut Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
        context: &mut crate::runtime::RuntimeContext,
        replay_target_event_id: EventId,
        mut recovery_budget: Option<&mut RecoveryReplayBudget>,
    ) -> Result<WorkflowPollOutcome> {
        loop {
            let poll = poll_cached(future, context);
            let signal_requests = context.take_signal_requests();
            if !signal_requests.is_empty() {
                let mut fulfilled = false;
                for request in signal_requests {
                    let signal = self
                        .backend
                        .read_signal_inbox(ReadSignalInboxRequest {
                            run_id: run_id.clone(),
                            signal_name: request.signal_name,
                        })
                        .await?;
                    let signal = signal.map(|signal| crate::runtime::SignalInboxRecordForRuntime {
                        signal_id: signal.signal_id,
                        signal_name: signal.signal_name,
                        payload: signal.payload,
                    });
                    fulfilled |= signal.is_some();
                    context.fulfill_signal_request(request.command_id, signal);
                }
                if fulfilled {
                    continue;
                }
            }
            let payload_requests = context.take_payload_hydration_requests();
            if !payload_requests.is_empty() {
                for request in payload_requests {
                    let hydrated = match request.kind {
                        crate::runtime::PayloadHydrationKind::Payload => {
                            self.backend
                                .hydrate_payload(request.payload.clone())
                                .await?
                        }
                        crate::runtime::PayloadHydrationKind::ActivityMapResultManifest => {
                            self.backend
                                .hydrate_activity_map_result_manifest(request.payload.clone())
                                .await?
                        }
                    };
                    context.fulfill_payload_hydration(request, hydrated)?;
                }
                continue;
            }
            let Some(after_event_id) = context.needs_more_history_after() else {
                return Ok(WorkflowPollOutcome::Ready(poll));
            };
            let chunk = match recovery_budget.as_deref_mut() {
                Some(budget) => {
                    let Some(chunk) = self
                        .stream_recovery_history_chunk(
                            run_id.clone(),
                            after_event_id,
                            replay_target_event_id,
                            budget,
                        )
                        .await?
                    else {
                        return Ok(WorkflowPollOutcome::Deferred);
                    };
                    chunk
                }
                None => {
                    self.stream_history_chunk(
                        run_id.clone(),
                        after_event_id,
                        replay_target_event_id,
                    )
                    .await?
                }
            };
            if chunk.events.is_empty() && after_event_id < replay_target_event_id {
                return Err(Error::Backend(format!(
                    "history stream ended at event {after_event_id} before replay target {replay_target_event_id}"
                )));
            }
            context.append_replay_events(chunk.events, chunk.last_event_id);
        }
    }

    async fn defer_workflow_task(
        &self,
        claim: crate::WorkflowTaskClaim,
        delay: Duration,
    ) -> Result<WorkflowTaskOutcome> {
        self.backend
            .release_workflow_task(
                claim,
                WorkflowTaskRelease::delayed(WorkflowTaskReason::CacheEvicted, delay),
            )
            .await?;
        Ok(WorkflowTaskOutcome::Deferred)
    }

    fn try_acquire_recovery(&mut self) -> bool {
        if self.active_recoveries >= self.recovery_flow_control.max_concurrent_recoveries {
            return false;
        }
        self.active_recoveries += 1;
        true
    }

    fn release_recovery(&mut self) {
        self.active_recoveries = self.active_recoveries.saturating_sub(1);
    }

    fn backpressure_delay(&self, retry_after: Duration) -> Duration {
        if retry_after == Duration::ZERO {
            self.recovery_flow_control.defer_delay
        } else {
            retry_after
        }
    }

    async fn hydrate_payload_for_decode(
        &self,
        payload: crate::PayloadRef,
    ) -> Result<crate::PayloadRef> {
        let payload = self.backend.hydrate_payload(payload).await?;
        if matches!(payload, crate::PayloadRef::Blob { .. }) {
            return Err(Error::PayloadDecode(
                "backend returned an unresolved blob for workflow input".to_owned(),
            ));
        }
        Ok(payload)
    }

    async fn finish_workflow_poll(
        &mut self,
        claimed: crate::ClaimedWorkflowTask,
        future: Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
        context: crate::runtime::RuntimeContext,
        poll: Poll<Result<crate::PayloadRef>>,
    ) -> Result<Option<CachedWorkflow>> {
        let next_command_seq = context.next_command_seq();
        let parts = context.into_commit_parts();
        let default_activity_options = parts.default_activity_options.clone();
        let mut append_events = parts.append_events;
        let mut terminal = false;

        match poll {
            Poll::Ready(Ok(result)) => {
                append_events.push(NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
                    result,
                }));
                terminal = true;
            }
            Poll::Ready(Err(err)) => {
                if let Error::ContinueAsNew { input } = err {
                    append_events.push(NewHistoryEvent::new(
                        HistoryEventData::WorkflowContinuedAsNew { input },
                    ));
                    terminal = true;
                } else if matches!(
                    err,
                    Error::Nondeterminism(_) | Error::UnsupportedWorkflowVersion { .. }
                ) {
                    return Err(err);
                } else {
                    append_events.push(NewHistoryEvent::new(HistoryEventData::WorkflowFailed {
                        failure: err.durable_failure(),
                    }));
                    terminal = true;
                }
            }
            Poll::Pending => {}
        }
        let runtime_appended_tail = EventId(
            claimed
                .replay_target_event_id
                .0
                .saturating_add(u64::try_from(append_events.len()).unwrap_or(u64::MAX)),
        );

        let commit = self
            .backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: claimed.replay_target_event_id,
                    append_events,
                    upsert_waits: parts.upsert_waits,
                    schedule_activities: parts.schedule_activities,
                    schedule_activity_maps: parts.schedule_activity_maps,
                    start_child_workflows: parts.start_child_workflows,
                    consume_signals: parts.consume_signals,
                    delete_waits: parts.delete_waits,
                    cancel_commands: parts.cancel_commands,
                    query_projection: parts.query_projection,
                },
            )
            .await?;
        let crate::CommitOutcome::Committed {
            new_tail_event_id: last_event_id,
        } = commit
        else {
            return Ok(None);
        };

        if terminal {
            return Ok(None);
        }
        if last_event_id > runtime_appended_tail {
            return Ok(None);
        }

        Ok(Some(CachedWorkflow {
            future,
            last_event_id,
            next_command_seq,
            default_activity_options,
        }))
    }

    async fn run_local_activities_after_workflow_task(&mut self) -> Result<()> {
        if self.max_local_activities_per_workflow_task == 0 {
            return Ok(());
        }
        for _ in 0..self.max_local_activities_per_workflow_task {
            if self.run_activity_once().await? {
                self.completed_local_activity_tasks =
                    self.completed_local_activity_tasks.saturating_add(1);
            } else {
                break;
            }
        }
        Ok(())
    }

    fn take_completed_local_activity_tasks(&mut self) -> usize {
        let completed = self.completed_local_activity_tasks;
        self.completed_local_activity_tasks = 0;
        completed
    }
}

fn poll_cached(
    future: &mut Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
    context: &mut crate::runtime::RuntimeContext,
) -> Poll<Result<crate::PayloadRef>> {
    let waker = futures::task::noop_waker();
    let mut task_context = std::task::Context::from_waker(&waker);
    poll_with_runtime_context(context, || future.as_mut().poll(&mut task_context))
}

fn split_start_event(events: &[HistoryEvent]) -> Result<(crate::PayloadRef, Vec<HistoryEvent>)> {
    let Some(first) = events.first() else {
        return Err(Error::Backend(
            "claimed workflow task without WorkflowStarted event".to_owned(),
        ));
    };
    let HistoryEventData::WorkflowStarted { input, .. } = &first.data else {
        return Err(Error::Backend(
            "first workflow history event was not WorkflowStarted".to_owned(),
        ));
    };
    Ok((input.clone(), events.iter().skip(1).cloned().collect()))
}

pub struct WorkerBuilder<B>
where
    B: DurableBackend,
{
    backend: B,
    namespace: Namespace,
    worker_id: WorkerId,
    workflow_task_queue: TaskQueue,
    activity_task_queue: TaskQueue,
    registry: Registry,
    history_chunk_events: usize,
    history_chunk_bytes: usize,
    nondeterminism_retry_backoff: Duration,
    recovery_flow_control: RecoveryFlowControl,
    max_local_activities_per_workflow_task: usize,
}

impl<B> WorkerBuilder<B>
where
    B: DurableBackend,
{
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Namespace::new(namespace);
        self
    }

    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = WorkerId::new(worker_id);
        self
    }

    pub fn workflow_task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.workflow_task_queue = TaskQueue::new(task_queue);
        self
    }

    pub fn activity_task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.activity_task_queue = TaskQueue::new(task_queue);
        self
    }

    pub fn history_chunk_events(mut self, max_events: usize) -> Self {
        self.history_chunk_events = max_events.max(1);
        self
    }

    pub fn history_chunk_bytes(mut self, max_bytes: usize) -> Self {
        self.history_chunk_bytes = max_bytes.max(1);
        self
    }

    pub fn nondeterminism_retry_backoff(mut self, backoff: Duration) -> Self {
        self.nondeterminism_retry_backoff = backoff;
        self
    }

    pub fn max_concurrent_recoveries(mut self, limit: usize) -> Self {
        self.recovery_flow_control.max_concurrent_recoveries = limit;
        self
    }

    pub fn recovery_replay_event_budget(mut self, max_events: usize) -> Self {
        self.recovery_flow_control.replay_event_budget = max_events;
        self
    }

    pub fn recovery_replay_byte_budget(mut self, max_bytes: usize) -> Self {
        self.recovery_flow_control.replay_byte_budget = max_bytes;
        self
    }

    pub fn recovery_prefetch_chunks(mut self, max_chunks: usize) -> Self {
        self.recovery_flow_control.prefetch_chunks = max_chunks;
        self
    }

    pub fn recovery_defer_delay(mut self, delay: Duration) -> Self {
        self.recovery_flow_control.defer_delay = delay;
        self
    }

    pub fn max_local_activities_per_workflow_task(mut self, limit: usize) -> Self {
        self.max_local_activities_per_workflow_task = limit;
        self
    }

    pub fn register_workflow<W>(mut self, _workflow: W) -> Self
    where
        W: Workflow + Default,
    {
        self = self
            .try_register_workflow(_workflow)
            .expect("duplicate workflow registration");
        self
    }

    pub fn try_register_workflow<W>(mut self, _workflow: W) -> Result<Self>
    where
        W: Workflow + Default,
    {
        self.registry.register_workflow::<W>()?;
        Ok(self)
    }

    pub fn register_activity<A>(mut self, _activity: A) -> Self
    where
        A: crate::Activity + Default,
    {
        self = self
            .try_register_activity(_activity)
            .expect("duplicate activity registration");
        self
    }

    pub fn try_register_activity<A>(mut self, _activity: A) -> Result<Self>
    where
        A: crate::Activity + Default,
    {
        self.registry.register_activity::<A>()?;
        Ok(self)
    }

    pub fn build(self) -> Worker<B> {
        let payload_codec = self.backend.payload_storage_config().codec;
        Worker {
            backend: self.backend,
            namespace: self.namespace,
            worker_id: self.worker_id,
            workflow_task_queue: self.workflow_task_queue,
            activity_task_queue: self.activity_task_queue,
            registry: self.registry,
            cache: BTreeMap::new(),
            history_chunk_events: self.history_chunk_events,
            history_chunk_bytes: self.history_chunk_bytes,
            payload_codec,
            nondeterminism_retry_backoff: self.nondeterminism_retry_backoff,
            recovery_flow_control: self.recovery_flow_control,
            active_recoveries: 0,
            max_local_activities_per_workflow_task: self.max_local_activities_per_workflow_task,
            completed_local_activity_tasks: 0,
        }
    }
}
