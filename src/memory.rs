use crate::{
    ActivityId, ActivityTask, ActivityTaskClaim, ClaimActivityOptions, ClaimWorkflowTaskOptions,
    ClaimedActivityTask, ClaimedWorkflowTask, CommitOutcome, CompleteActivityOutcome,
    CompleteActivityRequest, DurableBackend, Error, EventId, HistoryChunk, HistoryEvent,
    HistoryEventData, Namespace, Result, RunId, StartWorkflowOutcome, StartWorkflowRequest,
    WorkflowId, WorkflowTaskClaim, WorkflowTaskCommit, WorkflowTaskReason, event_payload_len,
    is_terminal,
};
use futures::future::{BoxFuture, ready};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
pub struct MemoryBackend {
    state: Arc<Mutex<MemoryState>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default)]
struct MemoryState {
    next_run_id: u64,
    next_claim_token: u64,
    workflow_ids: BTreeMap<(Namespace, WorkflowId), RunId>,
    runs: BTreeMap<RunId, RunRecord>,
    activities: BTreeMap<ActivityId, ActivityRecord>,
}

struct RunRecord {
    namespace: Namespace,
    workflow_id: WorkflowId,
    workflow_type: crate::WorkflowType,
    task_queue: crate::TaskQueue,
    history: Vec<HistoryEvent>,
    ready: Option<WorkflowTaskReason>,
    ready_at: Option<Instant>,
    workflow_claim: Option<u64>,
    terminal: bool,
}

struct ActivityRecord {
    task: ActivityTask,
    claim: Option<u64>,
    completed: bool,
}

impl DurableBackend for MemoryBackend {
    fn start_workflow(
        &self,
        req: StartWorkflowRequest,
    ) -> BoxFuture<'static, Result<StartWorkflowOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        if let Some(run_id) = state
            .workflow_ids
            .get(&(req.namespace.clone(), req.workflow_id.clone()))
            .cloned()
        {
            return Box::pin(ready(Ok(StartWorkflowOutcome::AlreadyStarted { run_id })));
        }

        state.next_run_id += 1;
        let run_id = RunId::new(format!("run-{}", state.next_run_id));
        let start = HistoryEvent {
            event_id: EventId(1),
            event_type: crate::HistoryEventType::WorkflowStarted,
            data: HistoryEventData::WorkflowStarted {
                workflow_type: req.workflow_type.clone(),
                input: req.input,
            },
        };
        state.workflow_ids.insert(
            (req.namespace.clone(), req.workflow_id.clone()),
            run_id.clone(),
        );
        state.runs.insert(
            run_id.clone(),
            RunRecord {
                namespace: req.namespace,
                workflow_id: req.workflow_id,
                workflow_type: req.workflow_type,
                task_queue: req.task_queue,
                history: vec![start],
                ready: Some(WorkflowTaskReason::WorkflowStarted),
                ready_at: None,
                workflow_claim: None,
                terminal: false,
            },
        );

        Box::pin(ready(Ok(StartWorkflowOutcome::Started { run_id })))
    }

    fn claim_workflow_task(
        &self,
        worker_id: crate::WorkerId,
        opts: ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedWorkflowTask>>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let now = Instant::now();
        let Some(run_id) = state.runs.iter().find_map(|(run_id, run)| {
            let matches = run.namespace == opts.namespace
                && run.task_queue == opts.task_queue
                && run.ready.is_some()
                && run.ready_at.is_none_or(|ready_at| ready_at <= now)
                && !run.terminal
                && opts
                    .registered_workflow_types
                    .iter()
                    .any(|workflow_type| workflow_type == &run.workflow_type);
            matches.then(|| run_id.clone())
        }) else {
            return Box::pin(ready(Ok(None)));
        };

        state.next_claim_token += 1;
        let token = state.next_claim_token;
        let run = state
            .runs
            .get_mut(&run_id)
            .expect("run id selected from runs map");
        run.workflow_claim = Some(token);
        let reason = run
            .ready
            .clone()
            .expect("ready reason selected from ready run");
        run.ready = None;
        run.ready_at = None;
        let replay_target_event_id = run
            .history
            .last()
            .map(|event| event.event_id)
            .unwrap_or(EventId::ZERO);

        Box::pin(ready(Ok(Some(ClaimedWorkflowTask {
            run_id: run_id.clone(),
            workflow_id: run.workflow_id.clone(),
            workflow_type: run.workflow_type.clone(),
            claim: WorkflowTaskClaim {
                run_id,
                worker_id,
                token,
            },
            replay_target_event_id,
            reason,
        }))))
    }

    fn stream_history(
        &self,
        req: crate::StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        let Some(run) = state.runs.get(&req.run_id) else {
            return Box::pin(ready(Err(Error::RunNotFound(req.run_id))));
        };

        let max_events = req.max_events.max(1);
        let max_bytes = req.max_bytes.max(1);
        let mut bytes = 0usize;
        let mut events = Vec::new();
        for event in run.history.iter().filter(|event| {
            event.event_id > req.after_event_id && event.event_id <= req.up_to_event_id
        }) {
            let event_bytes = event_payload_len(&event.data).max(1);
            if !events.is_empty() && (events.len() >= max_events || bytes + event_bytes > max_bytes)
            {
                break;
            }
            bytes += event_bytes;
            events.push(event.clone());
            if events.len() >= max_events {
                break;
            }
        }

        let last_event_id = events
            .last()
            .map(|event| event.event_id)
            .unwrap_or(req.after_event_id);
        let has_more = run
            .history
            .iter()
            .any(|event| event.event_id > last_event_id && event.event_id <= req.up_to_event_id);

        Box::pin(ready(Ok(HistoryChunk {
            events,
            last_event_id,
            has_more,
        })))
    }

    fn commit_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> BoxFuture<'static, Result<CommitOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let scheduled = batch.schedule_activities;
        let next_event_id = {
            let Some(run) = state.runs.get_mut(&claim.run_id) else {
                return Box::pin(ready(Err(Error::RunNotFound(claim.run_id))));
            };
            if run.workflow_claim != Some(claim.token) {
                return Box::pin(ready(Err(Error::StaleLease)));
            }
            let current_tail = run
                .history
                .last()
                .map(|event| event.event_id)
                .unwrap_or(EventId::ZERO);
            if current_tail != batch.expected_tail_event_id {
                run.workflow_claim = None;
                run.ready = Some(WorkflowTaskReason::CacheEvicted);
                run.ready_at = None;
                return Box::pin(ready(Ok(CommitOutcome::Conflict)));
            }
            if run.terminal && !batch.append_events.is_empty() {
                return Box::pin(ready(Err(Error::TerminalWorkflow)));
            }

            let mut next_event_id = current_tail;
            let mut terminal = false;
            for new_event in batch.append_events {
                next_event_id = next_event_id.next();
                terminal |= is_terminal(&new_event.data);
                run.history.push(HistoryEvent {
                    event_id: next_event_id,
                    event_type: new_event.data.event_type(),
                    data: new_event.data,
                });
            }

            run.workflow_claim = None;
            if terminal {
                run.terminal = true;
                run.ready = None;
                run.ready_at = None;
            }

            next_event_id
        };

        for task in scheduled {
            state.activities.insert(
                task.activity_id.clone(),
                ActivityRecord {
                    task,
                    claim: None,
                    completed: false,
                },
            );
        }

        Box::pin(ready(Ok(CommitOutcome::Committed {
            new_tail_event_id: next_event_id,
        })))
    }

    fn release_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        release: crate::WorkflowTaskRelease,
    ) -> BoxFuture<'static, Result<()>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let Some(run) = state.runs.get_mut(&claim.run_id) else {
            return Box::pin(ready(Err(Error::RunNotFound(claim.run_id))));
        };
        if run.workflow_claim != Some(claim.token) {
            return Box::pin(ready(Err(Error::StaleLease)));
        }
        run.workflow_claim = None;
        if !run.terminal {
            run.ready = Some(release.reason);
            run.ready_at = ready_at_for_delay(release.delay);
        } else {
            run.ready_at = None;
        }
        Box::pin(ready(Ok(())))
    }

    fn claim_activity_task(
        &self,
        worker_id: crate::WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedActivityTask>>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let Some(activity_id) = state.activities.iter().find_map(|(activity_id, record)| {
            let matches = !record.completed
                && record.claim.is_none()
                && record.task.task_queue == opts.task_queue
                && opts
                    .registered_activity_names
                    .iter()
                    .any(|name| name == &record.task.activity_name);
            matches.then(|| activity_id.clone())
        }) else {
            return Box::pin(ready(Ok(None)));
        };

        state.next_claim_token += 1;
        let token = state.next_claim_token;
        let record = state
            .activities
            .get_mut(&activity_id)
            .expect("activity id selected from activities map");
        record.claim = Some(token);
        Box::pin(ready(Ok(Some(ClaimedActivityTask {
            task: record.task.clone(),
            claim: ActivityTaskClaim {
                activity_id,
                worker_id,
                token,
            },
        }))))
    }

    fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> BoxFuture<'static, Result<CompleteActivityOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let Some(record) = state.activities.get_mut(&req.claim.activity_id) else {
            return Box::pin(ready(Err(Error::Backend(format!(
                "activity `{}` not found",
                req.claim.activity_id.0
            )))));
        };
        if record.completed {
            return Box::pin(ready(Ok(CompleteActivityOutcome::AlreadyCompleted)));
        }
        if record.claim != Some(req.claim.token) {
            return Box::pin(ready(Err(Error::StaleLease)));
        }

        record.completed = true;
        let task = record.task.clone();
        let Some(run) = state.runs.get_mut(&task.run_id) else {
            return Box::pin(ready(Err(Error::RunNotFound(task.run_id))));
        };
        if run.terminal {
            return Box::pin(ready(Err(Error::TerminalWorkflow)));
        }
        let event_id = run
            .history
            .last()
            .map(|event| event.event_id.next())
            .unwrap_or(EventId(1));
        run.history.push(HistoryEvent {
            event_id,
            event_type: crate::HistoryEventType::ActivityCompleted,
            data: HistoryEventData::ActivityCompleted(crate::ActivityCompleted {
                command_id: task.command_id,
                result: req.result,
            }),
        });
        run.ready = Some(WorkflowTaskReason::ActivityCompleted);
        run.ready_at = None;

        Box::pin(ready(Ok(CompleteActivityOutcome::Completed { event_id })))
    }
}

fn ready_at_for_delay(delay: Duration) -> Option<Instant> {
    if delay.is_zero() {
        None
    } else {
        let now = Instant::now();
        Some(now.checked_add(delay).unwrap_or(now))
    }
}
