use crate::{
    ClaimActivityOptions, ClaimWorkflowTaskOptions, CompleteActivityRequest, DurableBackend, Error,
    EventId, FailActivityRequest, FireDueTimersRequest, HistoryEvent, HistoryEventData, Namespace,
    NewHistoryEvent, ReadSignalInboxRequest, Registry, Result, RunId, StartWorkflowRequest,
    TaskQueue, TimeoutDueActivitiesRequest, WorkerId, Workflow, WorkflowId, WorkflowTaskCommit,
    WorkflowTaskReason, WorkflowTaskRelease, conflict_to_error, poll_with_runtime_context,
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
}

impl<B> Client<B>
where
    B: DurableBackend,
{
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            namespace: Namespace::default(),
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
                input: crate::encode_payload(&input)?,
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
                payload: crate::encode_payload(&payload)?,
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
    nondeterminism_retry_backoff: Duration,
    max_local_activities_per_workflow_task: usize,
    completed_local_activity_tasks: usize,
}

struct CachedWorkflow {
    future: Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
    last_event_id: EventId,
    next_command_seq: u64,
    default_activity_options: crate::ActivityOptions,
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
                        now,
                        chunk.events,
                        cached.default_activity_options,
                        cached.next_command_seq,
                        chunk.last_event_id,
                        claimed.replay_target_event_id,
                    );
                    let poll = self
                        .poll_until_history_blocked_or_ready(
                            &claimed.run_id,
                            &mut cached.future,
                            &mut context,
                            claimed.replay_target_event_id,
                        )
                        .await;
                    match poll {
                        Ok(poll) => {
                            self.finish_workflow_poll(claimed, cached.future, context, poll)
                                .await
                        }
                        Err(err) => Err(err),
                    }
                }
                Err(err) => Err(err),
            }
        } else {
            let first_chunk = self
                .stream_history_chunk(
                    claimed.run_id.clone(),
                    EventId::ZERO,
                    claimed.replay_target_event_id,
                )
                .await;
            match first_chunk {
                Ok(first_chunk) => {
                    let last_loaded_event_id = first_chunk.last_event_id;
                    match split_start_event(&first_chunk.events) {
                        Err(err) => Err(err),
                        Ok((input, replay_events)) => {
                            match self.registry.workflow(&claimed.workflow_type) {
                                None => {
                                    Err(Error::WorkflowNotRegistered(claimed.workflow_type.clone()))
                                }
                                Some(registration) => {
                                    let mut future = registration.run(input);
                                    let mut context = crate::runtime::RuntimeContext::new(
                                        claimed.run_id.clone(),
                                        self.workflow_task_queue.clone(),
                                        self.activity_task_queue.clone(),
                                        now,
                                        replay_events,
                                        crate::ActivityOptions::default(),
                                        0,
                                        last_loaded_event_id,
                                        claimed.replay_target_event_id,
                                    );
                                    let poll = self
                                        .poll_until_history_blocked_or_ready(
                                            &claimed.run_id,
                                            &mut future,
                                            &mut context,
                                            claimed.replay_target_event_id,
                                        )
                                        .await;
                                    match poll {
                                        Ok(poll) => {
                                            self.finish_workflow_poll(
                                                claimed, future, context, poll,
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
                Err(err) => Err(err),
            }
        };

        let entry = match entry_result {
            Ok(entry) => entry,
            Err(err) => {
                let release = if matches!(&err, Error::Nondeterminism(_)) {
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
        match registration.run(claimed.task.input).await {
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
            .stream_history(crate::StreamHistoryRequest {
                run_id,
                after_event_id,
                up_to_event_id,
                max_events: self.history_chunk_events,
                max_bytes: self.history_chunk_bytes,
            })
            .await
    }

    async fn poll_until_history_blocked_or_ready(
        &self,
        run_id: &RunId,
        future: &mut Pin<Box<dyn Future<Output = Result<crate::PayloadRef>> + Send>>,
        context: &mut crate::runtime::RuntimeContext,
        replay_target_event_id: EventId,
    ) -> Result<Poll<Result<crate::PayloadRef>>> {
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
            let Some(after_event_id) = context.needs_more_history_after() else {
                return Ok(poll);
            };
            let chunk = self
                .stream_history_chunk(run_id.clone(), after_event_id, replay_target_event_id)
                .await?;
            if chunk.events.is_empty() && after_event_id < replay_target_event_id {
                return Err(Error::Backend(format!(
                    "history stream ended at event {after_event_id} before replay target {replay_target_event_id}"
                )));
            }
            context.append_replay_events(chunk.events, chunk.last_event_id);
        }
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
                if matches!(err, Error::Nondeterminism(_)) {
                    return Err(err);
                }
                append_events.push(NewHistoryEvent::new(HistoryEventData::WorkflowFailed {
                    failure: err.durable_failure(),
                }));
                terminal = true;
            }
            Poll::Pending => {}
        }

        let last_event_id = conflict_to_error(
            self.backend
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
                .await?,
        )?;

        if terminal {
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

    pub fn nondeterminism_retry_backoff(mut self, backoff: Duration) -> Self {
        self.nondeterminism_retry_backoff = backoff;
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
            nondeterminism_retry_backoff: self.nondeterminism_retry_backoff,
            max_local_activities_per_workflow_task: self.max_local_activities_per_workflow_task,
            completed_local_activity_tasks: 0,
        }
    }
}
