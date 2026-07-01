use crate::{
    ActivityHeartbeatRequest, ClaimActivityOptions, ClaimActivityTasksOptions,
    ClaimWorkflowTaskOptions, ClaimWorkflowTasksOptions, CompleteActivityRequest, DurableBackend,
    Error, EventId, FailActivityRequest, FireDueTimersRequest, HistoryEvent, HistoryEventData,
    Namespace, NewHistoryEvent, ReadSignalInboxRequest, ReadSignalInboxesRequest, Registry, Result,
    RunDueMaintenanceRequest, RunId, ShardId, StartWorkflowRequest, TaskQueue,
    TimeoutDueActivitiesRequest, TimestampMs, WorkerId, Workflow, WorkflowChangeMarkerKind,
    WorkflowChangeVersionRecord, WorkflowChangeVersionStatus, WorkflowChangeVersionsRequest,
    WorkflowId, WorkflowTaskCommit, WorkflowTaskReason, WorkflowTaskRelease,
    poll_with_activity_context, poll_with_runtime_context,
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
    workflow_task_concurrency: WorkflowTaskConcurrency,
    recovery_flow_control: RecoveryFlowControl,
    active_recoveries: usize,
    activity_task_batch_size: usize,
    activity_completion_batch_size: usize,
    max_local_activities_per_workflow_task: usize,
    completed_local_activity_tasks: usize,
}

struct CachedWorkflow {
    future: Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
    last_event_id: EventId,
    next_command_seq: u64,
    default_activity_options: crate::ActivityOptions,
    change_versions: Vec<WorkflowChangeVersionRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkflowTaskConcurrency {
    max_concurrent_workflow_tasks: usize,
    prefetch_limit: usize,
    commit_batch_size: usize,
    shard_filter: Option<Vec<ShardId>>,
}

impl Default for WorkflowTaskConcurrency {
    fn default() -> Self {
        Self {
            max_concurrent_workflow_tasks: 1,
            prefetch_limit: 1,
            commit_batch_size: 1,
            shard_filter: None,
        }
    }
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

enum PreparedWorkflowTaskOutcome {
    Prepared(PreparedWorkflowTask),
    Deferred,
}

struct PreparedWorkflowTask {
    run_id: RunId,
    claim: crate::WorkflowTaskClaim,
    commit: WorkflowTaskCommit,
    future: Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
    runtime_appended_tail: EventId,
    next_command_seq: u64,
    default_activity_options: crate::ActivityOptions,
    change_versions: Vec<WorkflowChangeVersionRecord>,
    appended_change_marker: bool,
    terminal: bool,
}

enum WorkflowPollOutcome {
    Ready(Poll<Result<crate::PayloadRef>>),
    Deferred,
}

#[derive(Clone)]
enum ActivityFinish {
    Complete(crate::CompleteActivityRequest),
    Fail(crate::FailActivityRequest),
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
            workflow_task_concurrency: WorkflowTaskConcurrency::default(),
            recovery_flow_control: RecoveryFlowControl::default(),
            activity_task_batch_size: 1,
            activity_completion_batch_size: 1,
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

        self.run_claimed_workflow_task(claimed).await?;
        Ok(true)
    }

    pub async fn run_workflow_batch_once(&mut self) -> Result<usize> {
        let limit = self
            .workflow_task_concurrency
            .prefetch_limit
            .min(self.workflow_task_concurrency.max_concurrent_workflow_tasks)
            .max(1);
        if limit == 1 && self.workflow_task_concurrency.shard_filter.is_none() {
            return self.run_workflow_once().await.map(usize::from);
        }

        let claimed = self
            .backend
            .claim_workflow_tasks(
                self.worker_id.clone(),
                ClaimWorkflowTasksOptions {
                    claim: ClaimWorkflowTaskOptions {
                        namespace: self.namespace.clone(),
                        task_queue: self.workflow_task_queue.clone(),
                        registered_workflow_types: self.registry.workflow_types(),
                        lease_duration: Duration::from_secs(30),
                    },
                    limit,
                    shard_filter: self.workflow_task_concurrency.shard_filter.clone(),
                },
            )
            .await?;
        if claimed.is_empty() {
            return Ok(0);
        }

        let mut prepared = Vec::with_capacity(claimed.len());
        for task in claimed {
            match self.prepare_claimed_workflow_task(task).await? {
                PreparedWorkflowTaskOutcome::Prepared(task) => prepared.push(task),
                PreparedWorkflowTaskOutcome::Deferred => {}
            }
        }
        if prepared.is_empty() {
            return Ok(0);
        }

        let mut committed = 0usize;
        for chunk in prepared.chunks_mut(self.workflow_task_concurrency.commit_batch_size.max(1)) {
            let commits = chunk
                .iter()
                .map(|task| crate::WorkflowTaskCommitInput {
                    claim: task.claim.clone(),
                    commit: task.commit.clone(),
                })
                .collect::<Vec<_>>();
            let results = self
                .backend
                .commit_workflow_tasks(crate::WorkflowTaskCommitBatch { commits })
                .await?;
            for (task, result) in chunk.iter_mut().zip(results.into_iter()) {
                let crate::CommitOutcome::Committed {
                    new_tail_event_id: last_event_id,
                } = result.result?
                else {
                    continue;
                };
                committed += 1;
                if task.terminal
                    || task.appended_change_marker
                    || last_event_id > task.runtime_appended_tail
                {
                    continue;
                }
                let future = std::mem::replace(
                    &mut task.future,
                    Box::pin(std::future::ready(Err(Error::Backend(
                        "committed workflow future was already moved".to_owned(),
                    )))),
                );
                self.cache.insert(
                    task.run_id.clone(),
                    CachedWorkflow {
                        future,
                        last_event_id,
                        next_command_seq: task.next_command_seq,
                        default_activity_options: task.default_activity_options.clone(),
                        change_versions: task.change_versions.clone(),
                    },
                );
            }
        }

        if committed > 0 {
            self.run_local_activities_after_workflow_tasks(committed)
                .await?;
        }
        Ok(committed)
    }

    async fn run_claimed_workflow_task(
        &mut self,
        claimed: crate::ClaimedWorkflowTask,
    ) -> Result<()> {
        let run_id = claimed.run_id.clone();
        let prepared = match self.prepare_claimed_workflow_task(claimed).await? {
            PreparedWorkflowTaskOutcome::Prepared(prepared) => prepared,
            PreparedWorkflowTaskOutcome::Deferred => return Ok(()),
        };
        // The single-task path commits one prepared task immediately, mirroring the
        // batched commit loop in `run_workflow_batch_once` with a batch of one.
        let claim_for_release = prepared.claim.clone();
        let entry = match self.commit_prepared_workflow_task(prepared).await {
            Ok(entry) => entry,
            Err(err) => {
                return self
                    .release_failed_workflow_task(claim_for_release, err)
                    .await;
            }
        };
        if let Some(entry) = entry {
            self.cache.insert(run_id, entry);
        }
        self.run_local_activities_after_workflow_tasks(1).await?;
        Ok(())
    }

    // Commits a single prepared task and decides whether its future stays cached,
    // factored out so the single-claim path reuses the same logic the batch loop
    // applies per committed task.
    async fn commit_prepared_workflow_task(
        &mut self,
        prepared: PreparedWorkflowTask,
    ) -> Result<Option<CachedWorkflow>> {
        let runtime_appended_tail = prepared.runtime_appended_tail;
        let terminal = prepared.terminal;
        let appended_change_marker = prepared.appended_change_marker;
        let next_command_seq = prepared.next_command_seq;
        let default_activity_options = prepared.default_activity_options.clone();
        let change_versions = prepared.change_versions.clone();
        let future = prepared.future;
        let commit = self
            .backend
            .commit_workflow_task(prepared.claim, prepared.commit)
            .await?;
        let crate::CommitOutcome::Committed {
            new_tail_event_id: last_event_id,
        } = commit
        else {
            return Ok(None);
        };

        if terminal || appended_change_marker || last_event_id > runtime_appended_tail {
            return Ok(None);
        }

        Ok(Some(CachedWorkflow {
            future,
            last_event_id,
            next_command_seq,
            default_activity_options,
            change_versions,
        }))
    }

    // Releases a claim whose commit failed, swallowing backpressure (the task will
    // be retried after the delay) and propagating every other error.
    async fn release_failed_workflow_task(
        &self,
        claim: crate::WorkflowTaskClaim,
        err: Error,
    ) -> Result<()> {
        if let Error::Backpressure { retry_after, .. } = &err {
            let delay = self.backpressure_delay(*retry_after);
            let _ = self
                .backend
                .release_workflow_task(
                    claim,
                    WorkflowTaskRelease::delayed(WorkflowTaskReason::CacheEvicted, delay),
                )
                .await;
            return Ok(());
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
        let _ = self.backend.release_workflow_task(claim, release).await;
        Err(err)
    }

    async fn prepare_claimed_workflow_task(
        &mut self,
        claimed: crate::ClaimedWorkflowTask,
    ) -> Result<PreparedWorkflowTaskOutcome> {
        let claim_for_release = claimed.claim.clone();
        let cached = self.cache.remove(&claimed.run_id);
        let now = self.backend.current_time().await?;
        let entry_result = if let Some(mut cached) = cached {
            let chunk = self
                .claim_history_chunk(&claimed, cached.last_event_id)
                .await;
            match chunk {
                Ok(chunk) => {
                    let change_versions = self
                        .change_versions_for_loaded_history(
                            &claimed,
                            &chunk,
                            Some(cached.change_versions.clone()),
                        )
                        .await?;
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
                            &claimed,
                            &mut cached.future,
                            &mut context,
                            claimed.replay_target_event_id,
                            None,
                        )
                        .await;
                    match poll {
                        Ok(WorkflowPollOutcome::Ready(poll)) => self
                            .prepare_workflow_poll(
                                claimed,
                                cached.future,
                                context,
                                poll,
                                change_versions,
                            )
                            .await
                            .map(PreparedWorkflowTaskOutcome::Prepared),
                        Ok(WorkflowPollOutcome::Deferred) => {
                            Ok(PreparedWorkflowTaskOutcome::Deferred)
                        }
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
                    .map(|_| PreparedWorkflowTaskOutcome::Deferred)
            } else {
                let mut recovery_budget =
                    is_recovery.then(|| RecoveryReplayBudget::new(self.recovery_flow_control));
                let first_chunk = match recovery_budget.as_mut() {
                    Some(budget) => {
                        self.claim_recovery_history_chunk(&claimed, EventId::ZERO, budget)
                            .await
                    }
                    None => self
                        .claim_history_chunk(&claimed, EventId::ZERO)
                        .await
                        .map(Some),
                };
                let result = match first_chunk {
                    Ok(Some(first_chunk)) => {
                        let last_loaded_event_id = first_chunk.last_event_id;
                        let change_versions = self
                            .change_versions_for_loaded_history(&claimed, &first_chunk, None)
                            .await?;
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
                                                &claimed,
                                                &mut future,
                                                &mut context,
                                                claimed.replay_target_event_id,
                                                recovery_budget.as_mut(),
                                            )
                                            .await;
                                        match poll {
                                            Ok(WorkflowPollOutcome::Ready(poll)) => self
                                                .prepare_workflow_poll(
                                                    claimed,
                                                    future,
                                                    context,
                                                    poll,
                                                    change_versions,
                                                )
                                                .await
                                                .map(PreparedWorkflowTaskOutcome::Prepared),
                                            Ok(WorkflowPollOutcome::Deferred) => self
                                                .defer_workflow_task(
                                                    claimed.claim,
                                                    self.recovery_flow_control.defer_delay,
                                                )
                                                .await
                                                .map(|_| PreparedWorkflowTaskOutcome::Deferred),
                                            Err(err) => Err(err),
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => self
                        .defer_workflow_task(claimed.claim, self.recovery_flow_control.defer_delay)
                        .await
                        .map(|_| PreparedWorkflowTaskOutcome::Deferred),
                    Err(err) => Err(err),
                };
                if is_recovery {
                    self.release_recovery();
                }
                result
            }
        };

        match entry_result {
            Ok(entry) => Ok(entry),
            Err(err) => {
                self.release_failed_workflow_task(claim_for_release, err)
                    .await?;
                Ok(PreparedWorkflowTaskOutcome::Deferred)
            }
        }
    }

    pub async fn run_activity_once(&mut self) -> Result<bool> {
        let claimed = self
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

        let Some(claimed) = claimed else {
            return Ok(false);
        };

        self.run_claimed_activity(claimed).await?;
        Ok(true)
    }

    pub async fn run_activity_batch_once(&mut self) -> Result<usize> {
        let limit = self.activity_task_batch_size.max(1);
        if limit == 1 {
            return self.run_activity_once().await.map(usize::from);
        }

        let claimed = self
            .backend
            .claim_activity_tasks(
                self.worker_id.clone(),
                ClaimActivityTasksOptions {
                    claim: ClaimActivityOptions {
                        namespace: self.namespace.clone(),
                        task_queue: self.activity_task_queue.clone(),
                        registered_activity_names: self.registry.activity_names(),
                        lease_duration: Duration::from_secs(30),
                    },
                    limit,
                },
            )
            .await?;
        if claimed.is_empty() {
            return Ok(0);
        }

        let mut completed = 0usize;
        if self.activity_completion_batch_size <= 1 {
            for task in claimed {
                self.run_claimed_activity(task).await?;
                completed += 1;
            }
        } else {
            let mut finishes = Vec::with_capacity(claimed.len());
            for task in claimed {
                finishes.push(self.prepare_activity_finish(task).await?);
                completed += 1;
            }
            self.finish_activities(finishes).await?;
        }
        Ok(completed)
    }

    async fn run_claimed_activity(&mut self, claimed: crate::ClaimedActivityTask) -> Result<()> {
        let finish = self.prepare_activity_finish(claimed).await?;
        self.finish_activity(finish).await
    }

    async fn prepare_activity_finish(
        &mut self,
        claimed: crate::ClaimedActivityTask,
    ) -> Result<ActivityFinish> {
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

        Ok(match result {
            Ok(result) => ActivityFinish::Complete(CompleteActivityRequest {
                claim: claimed.claim,
                result,
            }),
            Err(err) => ActivityFinish::Fail(FailActivityRequest {
                claim: claimed.claim,
                failure: err.durable_failure(),
            }),
        })
    }

    async fn finish_activity(&self, finish: ActivityFinish) -> Result<()> {
        match finish {
            ActivityFinish::Complete(req) => {
                self.backend.complete_activity(req).await?;
            }
            ActivityFinish::Fail(req) => {
                self.backend.fail_activity(req).await?;
            }
        }
        Ok(())
    }

    async fn finish_activities(&self, finishes: Vec<ActivityFinish>) -> Result<()> {
        let batch_size = self.activity_completion_batch_size.max(1);
        for chunk in finishes.chunks(batch_size) {
            if chunk
                .iter()
                .all(|finish| matches!(finish, ActivityFinish::Complete(_)))
            {
                let completions = chunk
                    .iter()
                    .filter_map(|finish| match finish {
                        ActivityFinish::Complete(req) => Some(req.clone()),
                        ActivityFinish::Fail(_) => None,
                    })
                    .collect::<Vec<_>>();
                let results = self
                    .backend
                    .complete_activity_tasks(crate::CompleteActivityTasksRequest { completions })
                    .await?;
                for result in results {
                    result.result?;
                }
            } else {
                for finish in chunk {
                    self.finish_activity(finish.clone()).await?;
                }
            }
        }
        Ok(())
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

    pub async fn run_due_maintenance_once(&mut self) -> Result<crate::RunDueMaintenanceOutcome> {
        let now = self.backend.current_time().await?;
        self.backend
            .run_due_maintenance(RunDueMaintenanceRequest {
                namespace: self.namespace.clone(),
                now,
                timer_limit: 1024,
                activity_limit: 1024,
            })
            .await
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

            let workflow_tasks = self.run_workflow_batch_once().await?;
            if workflow_tasks > 0 {
                stats.workflow_tasks += workflow_tasks;
                progressed = true;
            }
            let local_activity_tasks = self.take_completed_local_activity_tasks();
            if local_activity_tasks > 0 {
                stats.activity_tasks += local_activity_tasks;
                progressed = true;
            }
            let maintenance = self.run_due_maintenance_once().await?;
            if maintenance.timers_fired > 0 {
                stats.timers_fired += maintenance.timers_fired;
                progressed = true;
            }
            if maintenance.activities_timed_out > 0 {
                stats.activities_timed_out += maintenance.activities_timed_out;
                progressed = true;
            }
            let child_starts = self.run_child_workflow_starts_once().await?;
            if child_starts > 0 {
                stats.child_workflow_starts_dispatched += child_starts;
                progressed = true;
            }
            let activity_tasks = self.run_activity_batch_once().await?;
            if activity_tasks > 0 {
                stats.activity_tasks += activity_tasks;
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

    async fn claim_history_chunk(
        &self,
        claimed: &crate::ClaimedWorkflowTask,
        after_event_id: EventId,
    ) -> Result<crate::HistoryChunk> {
        if after_event_id >= claimed.replay_target_event_id {
            return Ok(crate::HistoryChunk {
                events: Vec::new(),
                last_event_id: after_event_id,
                has_more: false,
            });
        }
        if let Some(chunk) = prefetched_claim_history_chunk(claimed, after_event_id) {
            return Ok(chunk);
        }
        self.stream_history_chunk(
            claimed.run_id.clone(),
            after_event_id,
            claimed.replay_target_event_id,
        )
        .await
    }

    async fn claim_recovery_history_chunk(
        &self,
        claimed: &crate::ClaimedWorkflowTask,
        after_event_id: EventId,
        budget: &mut RecoveryReplayBudget,
    ) -> Result<Option<crate::HistoryChunk>> {
        if after_event_id >= claimed.replay_target_event_id {
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
        if let Some(chunk) =
            prefetched_claim_history_chunk_bounded(claimed, after_event_id, max_events, max_bytes)
        {
            budget.record_chunk(&chunk);
            return Ok(Some(chunk));
        }
        self.stream_recovery_history_chunk(
            claimed.run_id.clone(),
            after_event_id,
            claimed.replay_target_event_id,
            budget,
        )
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
        claimed: &crate::ClaimedWorkflowTask,
        future: &mut Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
        context: &mut crate::runtime::RuntimeContext,
        replay_target_event_id: EventId,
        mut recovery_budget: Option<&mut RecoveryReplayBudget>,
    ) -> Result<WorkflowPollOutcome> {
        loop {
            let poll = poll_cached(future, context);
            let signal_requests = context.take_signal_requests();
            if !signal_requests.is_empty() {
                let inbox_requests = signal_requests
                    .iter()
                    .map(|request| ReadSignalInboxRequest {
                        run_id: run_id.clone(),
                        signal_name: request.signal_name.clone(),
                    })
                    .collect::<Vec<_>>();
                let signals = self
                    .backend
                    .read_signal_inboxes(ReadSignalInboxesRequest {
                        requests: inbox_requests,
                    })
                    .await?;
                if signals.len() != signal_requests.len() {
                    return Err(Error::Backend(format!(
                        "backend returned {} signal inbox records for {} requests",
                        signals.len(),
                        signal_requests.len()
                    )));
                }
                let mut fulfilled = false;
                for (request, signal) in signal_requests.into_iter().zip(signals) {
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
                        crate::runtime::PayloadHydrationKind::ChildWorkflowMapResultManifest => {
                            self.backend
                                .hydrate_child_workflow_map_result_manifest(request.payload.clone())
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
                        .claim_recovery_history_chunk(claimed, after_event_id, budget)
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
    ) -> Result<()> {
        self.backend
            .release_workflow_task(
                claim,
                WorkflowTaskRelease::delayed(WorkflowTaskReason::CacheEvicted, delay),
            )
            .await
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

    async fn change_versions_for_loaded_history(
        &self,
        claimed: &crate::ClaimedWorkflowTask,
        chunk: &crate::HistoryChunk,
        cached: Option<Vec<WorkflowChangeVersionRecord>>,
    ) -> Result<Vec<WorkflowChangeVersionRecord>> {
        if chunk.has_more {
            return Ok(self
                .backend
                .workflow_change_versions(WorkflowChangeVersionsRequest {
                    namespace: self.namespace.clone(),
                    workflow_id: None,
                    run_id: Some(claimed.run_id.clone()),
                    change_id: None,
                })
                .await?
                .records);
        }

        let mut records = cached.unwrap_or_default();
        records.extend(change_versions_from_history(
            &self.namespace,
            &claimed.workflow_id,
            &claimed.workflow_type,
            &claimed.run_id,
            &chunk.events,
        ));
        let mut by_change_id = BTreeMap::new();
        for record in records {
            by_change_id.insert(record.change_id.clone(), record);
        }
        Ok(by_change_id.into_values().collect())
    }

    async fn prepare_workflow_poll(
        &mut self,
        claimed: crate::ClaimedWorkflowTask,
        future: Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
        context: crate::runtime::RuntimeContext,
        poll: Poll<Result<crate::PayloadRef>>,
        change_versions: Vec<WorkflowChangeVersionRecord>,
    ) -> Result<PreparedWorkflowTask> {
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
        let appended_change_marker = append_events.iter().any(|event| {
            matches!(
                event.data,
                HistoryEventData::VersionMarker(_) | HistoryEventData::DeprecatedPatchMarker(_)
            )
        });

        Ok(PreparedWorkflowTask {
            run_id: claimed.run_id,
            claim: claimed.claim,
            commit: WorkflowTaskCommit {
                expected_tail_event_id: claimed.replay_target_event_id,
                append_events,
                upsert_waits: parts.upsert_waits,
                schedule_activities: parts.schedule_activities,
                schedule_activity_maps: parts.schedule_activity_maps,
                schedule_child_workflow_maps: parts.schedule_child_workflow_maps,
                start_child_workflows: parts.start_child_workflows,
                consume_signals: parts.consume_signals,
                delete_waits: parts.delete_waits,
                cancel_commands: parts.cancel_commands,
                query_projection: parts.query_projection,
            },
            future,
            runtime_appended_tail,
            next_command_seq,
            default_activity_options,
            change_versions,
            appended_change_marker,
            terminal,
        })
    }

    async fn run_local_activities_after_workflow_tasks(
        &mut self,
        workflow_tasks: usize,
    ) -> Result<()> {
        if self.max_local_activities_per_workflow_task == 0 {
            return Ok(());
        }
        let limit = self
            .max_local_activities_per_workflow_task
            .saturating_mul(workflow_tasks.max(1));
        for _ in 0..limit {
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

fn prefetched_claim_history_chunk(
    claimed: &crate::ClaimedWorkflowTask,
    after_event_id: EventId,
) -> Option<crate::HistoryChunk> {
    if after_event_id >= claimed.replay_target_event_id {
        return Some(crate::HistoryChunk {
            events: Vec::new(),
            last_event_id: after_event_id,
            has_more: false,
        });
    }

    let events = claimed
        .prefetched_history
        .iter()
        .filter(|event| {
            event.event_id > after_event_id && event.event_id <= claimed.replay_target_event_id
        })
        .cloned()
        .collect::<Vec<_>>();
    let first = events.first()?;
    let last = events.last()?;
    if first.event_id != after_event_id.next() || last.event_id != claimed.replay_target_event_id {
        return None;
    }
    if events
        .windows(2)
        .any(|pair| pair[1].event_id != pair[0].event_id.next())
    {
        return None;
    }
    let last_event_id = last.event_id;
    Some(crate::HistoryChunk {
        events,
        last_event_id,
        has_more: false,
    })
}

fn prefetched_claim_history_chunk_bounded(
    claimed: &crate::ClaimedWorkflowTask,
    after_event_id: EventId,
    max_events: usize,
    max_bytes: usize,
) -> Option<crate::HistoryChunk> {
    if after_event_id >= claimed.replay_target_event_id {
        return Some(crate::HistoryChunk {
            events: Vec::new(),
            last_event_id: after_event_id,
            has_more: false,
        });
    }
    let max_events = max_events.max(1);
    let max_bytes = max_bytes.max(1);
    let mut next_event_id = after_event_id.next();
    let mut bytes = 0usize;
    let mut events = Vec::new();
    for event in claimed.prefetched_history.iter().filter(|event| {
        event.event_id > after_event_id && event.event_id <= claimed.replay_target_event_id
    }) {
        if event.event_id != next_event_id {
            break;
        }
        let event_bytes = crate::runtime::event_payload_len(&event.data).max(1);
        if !events.is_empty() && (events.len() >= max_events || bytes + event_bytes > max_bytes) {
            break;
        }
        events.push(event.clone());
        bytes = bytes.saturating_add(event_bytes);
        next_event_id = event.event_id.next();
    }
    let last_event_id = events.last()?.event_id;
    Some(crate::HistoryChunk {
        events,
        last_event_id,
        has_more: last_event_id < claimed.replay_target_event_id,
    })
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

fn change_versions_from_history(
    namespace: &Namespace,
    workflow_id: &WorkflowId,
    workflow_type: &crate::WorkflowType,
    run_id: &RunId,
    events: &[HistoryEvent],
) -> Vec<WorkflowChangeVersionRecord> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::VersionMarker(marker) => Some(WorkflowChangeVersionRecord {
                namespace: namespace.clone(),
                workflow_id: workflow_id.clone(),
                workflow_type: workflow_type.clone(),
                run_id: run_id.clone(),
                change_id: marker.change_id.clone(),
                version: marker.version,
                marker_kind: WorkflowChangeMarkerKind::Version,
                status: WorkflowChangeVersionStatus::Open,
                command_seq: marker.command_id.seq,
                first_event_id: event.event_id,
                last_seen_at: TimestampMs(0),
            }),
            HistoryEventData::DeprecatedPatchMarker(marker) => Some(WorkflowChangeVersionRecord {
                namespace: namespace.clone(),
                workflow_id: workflow_id.clone(),
                workflow_type: workflow_type.clone(),
                run_id: run_id.clone(),
                change_id: marker.patch_id.clone(),
                version: 1,
                marker_kind: WorkflowChangeMarkerKind::DeprecatedPatch,
                status: WorkflowChangeVersionStatus::Open,
                command_seq: marker.command_id.seq,
                first_event_id: event.event_id,
                last_seen_at: TimestampMs(0),
            }),
            _ => None,
        })
        .collect()
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
    workflow_task_concurrency: WorkflowTaskConcurrency,
    recovery_flow_control: RecoveryFlowControl,
    activity_task_batch_size: usize,
    activity_completion_batch_size: usize,
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

    pub fn max_concurrent_workflow_tasks(mut self, limit: usize) -> Self {
        self.workflow_task_concurrency.max_concurrent_workflow_tasks = limit.max(1);
        self
    }

    pub fn workflow_task_prefetch_limit(mut self, limit: usize) -> Self {
        self.workflow_task_concurrency.prefetch_limit = limit.max(1);
        self
    }

    pub fn workflow_task_commit_batch_size(mut self, limit: usize) -> Self {
        self.workflow_task_concurrency.commit_batch_size = limit.max(1);
        self
    }

    pub fn workflow_task_shard_filter(mut self, shards: impl IntoIterator<Item = ShardId>) -> Self {
        let shards = shards.into_iter().collect::<Vec<_>>();
        self.workflow_task_concurrency.shard_filter = (!shards.is_empty()).then_some(shards);
        self
    }

    pub fn activity_task_batch_size(mut self, limit: usize) -> Self {
        self.activity_task_batch_size = limit.max(1);
        self
    }

    pub fn max_concurrent_activities(mut self, limit: usize) -> Self {
        self.activity_task_batch_size = limit.max(1);
        self
    }

    pub fn activity_completion_batch_size(mut self, limit: usize) -> Self {
        self.activity_completion_batch_size = limit.max(1);
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
            workflow_task_concurrency: self.workflow_task_concurrency,
            recovery_flow_control: self.recovery_flow_control,
            active_recoveries: 0,
            activity_task_batch_size: self.activity_task_batch_size,
            activity_completion_batch_size: self.activity_completion_batch_size,
            max_local_activities_per_workflow_task: self.max_local_activities_per_workflow_task,
            completed_local_activity_tasks: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClaimedWorkflowTask, HistoryEvent, HistoryEventData, HistoryEventType, WorkflowTaskClaim,
        WorkflowTaskReason,
    };

    fn event(event_id: u64) -> HistoryEvent {
        HistoryEvent {
            event_id: EventId(event_id),
            event_type: HistoryEventType::WorkflowTaskStarted,
            data: HistoryEventData::WorkflowTaskStarted,
        }
    }

    fn claimed(
        prefetched_history: Vec<HistoryEvent>,
        replay_target_event_id: u64,
    ) -> ClaimedWorkflowTask {
        ClaimedWorkflowTask {
            run_id: RunId::new("run"),
            workflow_id: WorkflowId::new("workflow"),
            workflow_type: crate::WorkflowType::new("test.workflow", 1),
            claim: WorkflowTaskClaim {
                run_id: RunId::new("run"),
                worker_id: WorkerId::new("worker"),
                token: 1,
            },
            replay_target_event_id: EventId(replay_target_event_id),
            reason: WorkflowTaskReason::WorkflowStarted,
            prefetched_history,
        }
    }

    #[test]
    fn prefetched_claim_history_chunk_uses_complete_contiguous_tail() {
        let claimed = claimed(vec![event(2), event(3), event(4)], 4);

        let chunk = prefetched_claim_history_chunk(&claimed, EventId(1)).unwrap();

        assert_eq!(chunk.last_event_id, EventId(4));
        assert!(!chunk.has_more);
        assert_eq!(
            chunk
                .events
                .iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>(),
            vec![EventId(2), EventId(3), EventId(4)]
        );
    }

    #[test]
    fn prefetched_claim_history_chunk_rejects_missing_start_or_target() {
        let missing_start = claimed(vec![event(3), event(4)], 4);
        assert!(prefetched_claim_history_chunk(&missing_start, EventId(1)).is_none());

        let missing_target = claimed(vec![event(2), event(3)], 4);
        assert!(prefetched_claim_history_chunk(&missing_target, EventId(1)).is_none());
    }

    #[test]
    fn prefetched_claim_history_chunk_rejects_internal_gaps() {
        let claimed = claimed(vec![event(2), event(4)], 4);

        assert!(prefetched_claim_history_chunk(&claimed, EventId(1)).is_none());
    }
}
