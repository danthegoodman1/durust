use crate::{
    ActivityId, ActivityMapInputManifest, ActivityMapItem, ActivityMapTask, ActivityTask,
    ActivityTaskClaim, CancelWorkflowOutcome, CancelWorkflowRequest, ChildStartOutboxMessage,
    ClaimActivityOptions, ClaimWorkflowTaskOptions, ClaimedActivityTask, ClaimedWorkflowTask,
    CommitOutcome, CompleteActivityOutcome, CompleteActivityRequest,
    DispatchChildWorkflowStartsOutcome, DispatchChildWorkflowStartsRequest, DurableBackend, Error,
    EventId, FailActivityOutcome, FailActivityRequest, FireDueTimersOutcome, FireDueTimersRequest,
    HistoryChunk, HistoryEvent, HistoryEventData, Namespace, ParentClosePolicy,
    ReadSignalInboxRequest, Result, RunId, SignalId, SignalInboxRecord, SignalWorkflowOutcome,
    SignalWorkflowRequest, StartWorkflowOutcome, StartWorkflowRequest, TimeoutDueActivitiesOutcome,
    TimeoutDueActivitiesRequest, TimestampMs, WaitId, WaitKind, WaitRecord,
    WorkflowChangeMarkerKind, WorkflowChangeVersionRecord, WorkflowChangeVersionStatus,
    WorkflowChangeVersionsOutcome, WorkflowChangeVersionsRequest, WorkflowId, WorkflowTaskClaim,
    WorkflowTaskCommit, WorkflowTaskReason, activity_map_input_at,
    encode_activity_map_result_manifest, event_payload_len, is_terminal,
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

    pub fn advance_time(&self, duration: std::time::Duration) {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        state.now = TimestampMs(
            state
                .now
                .0
                .saturating_add(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)),
        );
    }
}

#[derive(Default)]
struct MemoryState {
    now: TimestampMs,
    next_run_id: u64,
    next_claim_token: u64,
    next_signal_sequence: u64,
    workflow_ids: BTreeMap<(Namespace, WorkflowId), RunId>,
    runs: BTreeMap<RunId, RunRecord>,
    activities: BTreeMap<ActivityId, ActivityRecord>,
    activity_maps: BTreeMap<crate::CommandId, ActivityMapRecord>,
    child_outbox: BTreeMap<String, ChildOutboxRecord>,
    waits: BTreeMap<WaitId, WaitRecord>,
    signals: BTreeMap<SignalId, SignalRecord>,
    query_projections: BTreeMap<(Namespace, WorkflowId), QueryProjectionRecord>,
    workflow_change_versions: BTreeMap<(RunId, String), WorkflowChangeVersionRecord>,
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
    parent: Option<ChildParentLink>,
}

#[derive(Clone)]
struct ChildParentLink {
    parent_run_id: RunId,
    command_id: crate::CommandId,
    parent_close_policy: ParentClosePolicy,
}

struct ChildOutboxRecord {
    message: ChildStartOutboxMessage,
    dispatched: bool,
    child_run_id: Option<RunId>,
}

struct ActivityRecord {
    task: ActivityTask,
    claim: Option<u64>,
    completed: bool,
    timeout_at: Option<TimestampMs>,
    heartbeat_deadline_at: Option<TimestampMs>,
}

struct ActivityMapRecord {
    task: ActivityMapTask,
    input_manifest: ActivityMapInputManifest,
    results: BTreeMap<u64, crate::PayloadRef>,
    next_ordinal: u64,
    in_flight: usize,
    completed: bool,
}

struct SignalRecord {
    run_id: RunId,
    signal_name: crate::SignalName,
    payload: crate::PayloadRef,
    received_sequence: u64,
    consumed: bool,
}

struct QueryProjectionRecord {
    run_id: RunId,
    event_id: EventId,
    payload: crate::PayloadRef,
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
                parent: None,
            },
        );

        Box::pin(ready(Ok(StartWorkflowOutcome::Started { run_id })))
    }

    fn cancel_workflow(
        &self,
        req: CancelWorkflowRequest,
    ) -> BoxFuture<'static, Result<CancelWorkflowOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let Some(run_id) = state
            .workflow_ids
            .get(&(req.namespace.clone(), req.workflow_id.clone()))
            .cloned()
        else {
            return Box::pin(ready(Err(Error::Backend(format!(
                "workflow `{}` was not found",
                req.workflow_id.0
            )))));
        };
        let terminal_event = HistoryEventData::WorkflowCancelled { reason: req.reason };
        let event_id = {
            let Some(run) = state.runs.get_mut(&run_id) else {
                return Box::pin(ready(Err(Error::RunNotFound(run_id))));
            };
            if run.terminal {
                return Box::pin(ready(Ok(CancelWorkflowOutcome::AlreadyTerminal {
                    run_id: run_id.clone(),
                })));
            }

            let event_id = run
                .history
                .last()
                .map(|event| event.event_id.next())
                .unwrap_or(EventId(1));
            run.history.push(HistoryEvent {
                event_id,
                event_type: crate::HistoryEventType::WorkflowCancelled,
                data: terminal_event.clone(),
            });
            run.terminal = true;
            run.ready = None;
            run.ready_at = None;
            run.workflow_claim = None;
            event_id
        };
        cleanup_run_operational_state(&mut state, &run_id);
        handle_terminal_run(&mut state, &run_id, &terminal_event);

        Box::pin(ready(Ok(CancelWorkflowOutcome::Cancelled {
            run_id,
            event_id,
        })))
    }

    fn current_time(&self) -> BoxFuture<'static, Result<TimestampMs>> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        Box::pin(ready(Ok(state.now)))
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
                && run.workflow_claim.is_none()
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
        let scheduled_maps = batch.schedule_activity_maps;
        let mut decoded_maps = Vec::with_capacity(scheduled_maps.len());
        for map_task in scheduled_maps {
            let manifest: ActivityMapInputManifest =
                match crate::decode_payload(&map_task.input_manifest) {
                    Ok(manifest) => manifest,
                    Err(err) => return Box::pin(ready(Err(err))),
                };
            decoded_maps.push((map_task, manifest));
        }
        let upsert_waits = batch.upsert_waits;
        let consume_signals = batch.consume_signals;
        let delete_waits = batch.delete_waits;
        let cancel_commands = batch.cancel_commands;
        let start_child_workflows = batch.start_child_workflows;
        let query_projection = batch.query_projection;
        let mut terminal_event = None;
        let mut projection_update = None;
        let mut change_version_updates = Vec::new();
        let now = state.now;
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
                let data = new_event.data;
                terminal |= is_terminal(&data);
                if is_terminal(&data) {
                    terminal_event = Some(data.clone());
                }
                if let Some(record) =
                    change_version_record_for_run(run, &claim.run_id, next_event_id, &data, now)
                {
                    change_version_updates.push(record);
                }
                run.history.push(HistoryEvent {
                    event_id: next_event_id,
                    event_type: data.event_type(),
                    data,
                });
            }

            run.workflow_claim = None;
            if terminal {
                run.terminal = true;
                run.ready = None;
                run.ready_at = None;
            }
            if let Some(payload) = query_projection {
                projection_update = Some((
                    run.namespace.clone(),
                    run.workflow_id.clone(),
                    QueryProjectionRecord {
                        run_id: claim.run_id.clone(),
                        event_id: next_event_id,
                        payload,
                    },
                ));
            }

            (next_event_id, terminal)
        };

        for task in scheduled {
            let timeout_at = activity_timeout_at(state.now, task.start_to_close_timeout);
            state.activities.insert(
                task.activity_id.clone(),
                ActivityRecord {
                    task,
                    claim: None,
                    completed: false,
                    timeout_at,
                    heartbeat_deadline_at: None,
                },
            );
        }
        for (map_task, manifest) in decoded_maps {
            state.activity_maps.insert(
                map_task.map_command_id.clone(),
                ActivityMapRecord {
                    task: map_task.clone(),
                    input_manifest: manifest,
                    results: BTreeMap::new(),
                    next_ordinal: 0,
                    in_flight: 0,
                    completed: false,
                },
            );
            if let Err(err) = materialize_activity_map_items(&mut state, &map_task.map_command_id) {
                return Box::pin(ready(Err(err)));
            }
        }
        for message in start_child_workflows {
            state.child_outbox.insert(
                child_outbox_id(&message.command_id),
                ChildOutboxRecord {
                    message,
                    dispatched: false,
                    child_run_id: None,
                },
            );
        }
        for wait in upsert_waits {
            state.waits.insert(wait.wait_id.clone(), wait);
        }
        for signal_id in consume_signals {
            if let Some(signal) = state.signals.get_mut(&signal_id) {
                signal.consumed = true;
            }
        }
        for wait_id in delete_waits {
            state.waits.remove(&wait_id);
        }
        for command_id in cancel_commands {
            cancel_command_operational_state(&mut state, &command_id);
        }
        if next_event_id.1 {
            cleanup_run_operational_state(&mut state, &claim.run_id);
            if let Some(event) = terminal_event {
                handle_terminal_run(&mut state, &claim.run_id, &event);
            }
        }
        let signal_wait_ready = state.waits.values().any(|wait| {
            wait.run_id == claim.run_id
                && wait.kind == WaitKind::Signal
                && state.signals.values().any(|signal| {
                    signal.run_id == wait.run_id
                        && signal.signal_name.0 == wait.key
                        && !signal.consumed
                })
        });
        if signal_wait_ready {
            if let Some(run) = state.runs.get_mut(&claim.run_id) {
                if !run.terminal {
                    run.ready = Some(WorkflowTaskReason::SignalReceived);
                    run.ready_at = None;
                }
            }
        }
        if let Some((namespace, workflow_id, projection)) = projection_update {
            state
                .query_projections
                .insert((namespace, workflow_id), projection);
        }
        for record in change_version_updates {
            state
                .workflow_change_versions
                .insert((record.run_id.clone(), record.change_id.clone()), record);
        }

        Box::pin(ready(Ok(CommitOutcome::Committed {
            new_tail_event_id: next_event_id.0,
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

    fn signal_workflow(
        &self,
        req: SignalWorkflowRequest,
    ) -> BoxFuture<'static, Result<SignalWorkflowOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        if state.signals.contains_key(&req.signal_id) {
            return Box::pin(ready(Ok(SignalWorkflowOutcome::Duplicate)));
        }
        let Some(run_id) = state
            .workflow_ids
            .get(&(req.namespace.clone(), req.workflow_id.clone()))
            .cloned()
        else {
            return Box::pin(ready(Err(Error::Backend(format!(
                "workflow `{}` was not found",
                req.workflow_id.0
            )))));
        };
        if state.runs.get(&run_id).is_some_and(|run| run.terminal) {
            return Box::pin(ready(Err(Error::TerminalWorkflow)));
        }
        state.next_signal_sequence += 1;
        let received_sequence = state.next_signal_sequence;
        state.signals.insert(
            req.signal_id.clone(),
            SignalRecord {
                run_id: run_id.clone(),
                signal_name: req.signal_name.clone(),
                payload: req.payload,
                received_sequence,
                consumed: false,
            },
        );
        let has_wait = state.waits.values().any(|wait| {
            wait.run_id == run_id && wait.kind == WaitKind::Signal && wait.key == req.signal_name.0
        });
        if has_wait {
            if let Some(run) = state.runs.get_mut(&run_id) {
                if !run.terminal && run.workflow_claim.is_none() {
                    run.ready = Some(WorkflowTaskReason::SignalReceived);
                    run.ready_at = None;
                }
            }
        }
        Box::pin(ready(Ok(SignalWorkflowOutcome::Accepted)))
    }

    fn read_signal_inbox(
        &self,
        req: ReadSignalInboxRequest,
    ) -> BoxFuture<'static, Result<Option<SignalInboxRecord>>> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        let signal = state
            .signals
            .iter()
            .filter(|(_, signal)| {
                signal.run_id == req.run_id
                    && signal.signal_name == req.signal_name
                    && !signal.consumed
            })
            .min_by_key(|(_, signal)| signal.received_sequence)
            .map(|(signal_id, signal)| SignalInboxRecord {
                signal_id: signal_id.clone(),
                signal_name: signal.signal_name.clone(),
                payload: signal.payload.clone(),
            });
        Box::pin(ready(Ok(signal)))
    }

    fn fire_due_timers(
        &self,
        req: FireDueTimersRequest,
    ) -> BoxFuture<'static, Result<FireDueTimersOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let due = state
            .waits
            .iter()
            .filter(|(_, wait)| {
                wait.kind == WaitKind::Timer
                    && wait.ready_at.is_some_and(|ready_at| ready_at <= req.now)
            })
            .take(req.limit.max(1))
            .map(|(wait_id, wait)| (wait_id.clone(), wait.clone()))
            .collect::<Vec<_>>();
        let mut fired = 0usize;
        for (wait_id, wait) in due {
            let Some(run) = state.runs.get_mut(&wait.run_id) else {
                state.waits.remove(&wait_id);
                continue;
            };
            if run.namespace != req.namespace || run.terminal {
                continue;
            }
            let event_id = run
                .history
                .last()
                .map(|event| event.event_id.next())
                .unwrap_or(EventId(1));
            run.history.push(HistoryEvent {
                event_id,
                event_type: crate::HistoryEventType::TimerFired,
                data: HistoryEventData::TimerFired(crate::TimerFired {
                    command_id: wait.command_id,
                    fired_at: req.now,
                }),
            });
            run.ready = Some(WorkflowTaskReason::TimerFired);
            run.ready_at = None;
            state.waits.remove(&wait_id);
            fired += 1;
        }
        Box::pin(ready(Ok(FireDueTimersOutcome { fired })))
    }

    fn timeout_due_activities(
        &self,
        req: TimeoutDueActivitiesRequest,
    ) -> BoxFuture<'static, Result<TimeoutDueActivitiesOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let due = state
            .activities
            .iter()
            .filter(|(_, record)| {
                !record.completed
                    && activity_due_at(record).is_some_and(|due_at| due_at <= req.now)
                    && state
                        .runs
                        .get(&record.task.run_id)
                        .is_some_and(|run| run.namespace == req.namespace && !run.terminal)
            })
            .take(req.limit.max(1))
            .map(|(activity_id, _)| activity_id.clone())
            .collect::<Vec<_>>();

        let mut timed_out = 0usize;
        for activity_id in due {
            match timeout_activity(&mut state, &activity_id, req.now) {
                Ok(true) => timed_out += 1,
                Ok(false) => {}
                Err(err) => return Box::pin(ready(Err(err))),
            }
        }

        Box::pin(ready(Ok(TimeoutDueActivitiesOutcome { timed_out })))
    }

    fn claim_activity_task(
        &self,
        worker_id: crate::WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, Result<Option<ClaimedActivityTask>>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let mut selected = None;
        for (activity_id, record) in &state.activities {
            if record.completed
                || record.claim.is_some()
                || record.task.task_queue != opts.task_queue
                || state
                    .runs
                    .get(&record.task.run_id)
                    .is_none_or(|run| run.terminal || run.namespace != opts.namespace)
                || activity_due_at(record).is_some_and(|due_at| due_at <= state.now)
                || !opts
                    .registered_activity_names
                    .iter()
                    .any(|name| name == &record.task.activity_name)
            {
                continue;
            }
            if let Some(map_item) = &record.task.map_item {
                if state
                    .activity_maps
                    .get(&map_item.map_command_id)
                    .is_some_and(|map| map.completed)
                {
                    continue;
                }
            }
            selected = Some(activity_id.clone());
            break;
        }
        let Some(activity_id) = selected else {
            return Box::pin(ready(Ok(None)));
        };

        state.next_claim_token += 1;
        let token = state.next_claim_token;
        let now = state.now;
        let record = state
            .activities
            .get_mut(&activity_id)
            .expect("activity id selected from activities map");
        record.claim = Some(token);
        record.heartbeat_deadline_at = activity_timeout_at(now, record.task.heartbeat_timeout);
        Box::pin(ready(Ok(Some(ClaimedActivityTask {
            task: record.task.clone(),
            claim: ActivityTaskClaim {
                activity_id,
                worker_id,
                token,
            },
        }))))
    }

    fn heartbeat_activity(
        &self,
        req: crate::ActivityHeartbeatRequest,
    ) -> BoxFuture<'static, Result<crate::ActivityHeartbeatOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let now = state.now;
        let Some(record) = state.activities.get_mut(&req.claim.activity_id) else {
            return Box::pin(ready(Err(Error::Backend(format!(
                "activity `{}` not found",
                req.claim.activity_id.0
            )))));
        };
        if record.completed {
            return Box::pin(ready(Ok(crate::ActivityHeartbeatOutcome::AlreadyCompleted)));
        }
        if record.claim != Some(req.claim.token) {
            return Box::pin(ready(Err(Error::StaleLease)));
        }

        record.heartbeat_deadline_at = activity_timeout_at(now, record.task.heartbeat_timeout);
        Box::pin(ready(Ok(crate::ActivityHeartbeatOutcome::Recorded)))
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
        if let Some(map_item) = task.map_item.clone() {
            return Box::pin(ready(complete_map_item(
                &mut state, task, map_item, req.result,
            )));
        }
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

    fn fail_activity(
        &self,
        req: FailActivityRequest,
    ) -> BoxFuture<'static, Result<FailActivityOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let now = state.now;
        let Some(record) = state.activities.get_mut(&req.claim.activity_id) else {
            return Box::pin(ready(Err(Error::Backend(format!(
                "activity `{}` not found",
                req.claim.activity_id.0
            )))));
        };
        if record.completed {
            return Box::pin(ready(Ok(FailActivityOutcome::AlreadyCompleted)));
        }
        if record.claim != Some(req.claim.token) {
            return Box::pin(ready(Err(Error::StaleLease)));
        }

        let task = record.task.clone();
        if should_retry_activity(&task) && !req.failure.non_retryable {
            record.task.attempt = record.task.attempt.saturating_add(1);
            record.claim = None;
            record.timeout_at = activity_timeout_at(now, record.task.start_to_close_timeout);
            record.heartbeat_deadline_at = None;
            return Box::pin(ready(Ok(FailActivityOutcome::RetryScheduled {
                next_attempt: record.task.attempt,
            })));
        }

        record.completed = true;
        if let Some(map_item) = task.map_item.clone() {
            return Box::pin(ready(fail_map_item(
                &mut state,
                task,
                map_item,
                req.failure,
            )));
        }
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
            event_type: crate::HistoryEventType::ActivityFailed,
            data: HistoryEventData::ActivityFailed(crate::ActivityFailed {
                command_id: task.command_id,
                failure: req.failure,
            }),
        });
        run.ready = Some(WorkflowTaskReason::ActivityFailed);
        run.ready_at = None;

        Box::pin(ready(Ok(FailActivityOutcome::Failed { event_id })))
    }

    fn dispatch_child_workflow_starts(
        &self,
        req: DispatchChildWorkflowStartsRequest,
    ) -> BoxFuture<'static, Result<DispatchChildWorkflowStartsOutcome>> {
        let result = (|| {
            let mut state = self.state.lock().expect("memory backend mutex poisoned");
            let mut dispatched = 0usize;
            let limit = req.limit.max(1);
            while dispatched < limit {
                let Some(outbox_id) = state.child_outbox.iter().find_map(|(outbox_id, record)| {
                    (!record.dispatched
                        && state
                            .runs
                            .get(&record.message.command_id.run_id)
                            .is_some_and(|run| run.namespace == req.namespace))
                    .then(|| outbox_id.clone())
                }) else {
                    break;
                };
                dispatch_child_start(&mut state, &outbox_id)?;
                dispatched += 1;
            }
            Ok(DispatchChildWorkflowStartsOutcome { dispatched })
        })();
        Box::pin(ready(result))
    }

    fn query_projection(
        &self,
        req: crate::QueryProjectionRequest,
    ) -> BoxFuture<'static, Result<crate::QueryProjectionOutcome>> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        let outcome = state
            .query_projections
            .get(&(req.namespace, req.workflow_id))
            .map(|projection| crate::QueryProjectionOutcome::Found {
                run_id: projection.run_id.clone(),
                event_id: projection.event_id,
                payload: projection.payload.clone(),
            })
            .unwrap_or(crate::QueryProjectionOutcome::NotFound);
        Box::pin(ready(Ok(outcome)))
    }

    fn workflow_change_versions(
        &self,
        req: WorkflowChangeVersionsRequest,
    ) -> BoxFuture<'static, Result<WorkflowChangeVersionsOutcome>> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        let mut records = Vec::new();
        for record in state.workflow_change_versions.values() {
            if record.namespace != req.namespace {
                continue;
            }
            if req
                .workflow_id
                .as_ref()
                .is_some_and(|workflow_id| workflow_id != &record.workflow_id)
            {
                continue;
            }
            if req
                .run_id
                .as_ref()
                .is_some_and(|run_id| run_id != &record.run_id)
            {
                continue;
            }
            if req
                .change_id
                .as_ref()
                .is_some_and(|change_id| change_id != &record.change_id)
            {
                continue;
            }
            let mut record = record.clone();
            record.status = state
                .runs
                .get(&record.run_id)
                .map(|run| {
                    if run.terminal {
                        WorkflowChangeVersionStatus::Closed
                    } else {
                        WorkflowChangeVersionStatus::Open
                    }
                })
                .unwrap_or(WorkflowChangeVersionStatus::Closed);
            records.push(record);
        }
        records.sort_by(|left, right| {
            (
                left.workflow_id.0.as_str(),
                left.run_id.0.as_str(),
                left.change_id.as_str(),
            )
                .cmp(&(
                    right.workflow_id.0.as_str(),
                    right.run_id.0.as_str(),
                    right.change_id.as_str(),
                ))
        });
        Box::pin(ready(Ok(WorkflowChangeVersionsOutcome { records })))
    }
}

fn materialize_activity_map_items(
    state: &mut MemoryState,
    map_command_id: &crate::CommandId,
) -> Result<()> {
    let Some(map) = state.activity_maps.get_mut(map_command_id) else {
        return Ok(());
    };
    if map.completed {
        return Ok(());
    }

    while map.in_flight < map.task.max_in_flight
        && (map.next_ordinal as usize) < map.input_manifest.item_count
    {
        let item_ordinal = map.next_ordinal;
        let input = activity_map_input_at(&map.input_manifest, item_ordinal)?;
        let activity_id = ActivityId::map_item(map_command_id, item_ordinal);
        let timeout_at = activity_timeout_at(state.now, map.task.start_to_close_timeout);
        map.next_ordinal += 1;
        map.in_flight += 1;
        state.activities.insert(
            activity_id.clone(),
            ActivityRecord {
                task: ActivityTask {
                    activity_id,
                    run_id: map_command_id.run_id.clone(),
                    command_id: map_command_id.clone(),
                    activity_name: map.task.activity_name.clone(),
                    task_queue: map.task.task_queue.clone(),
                    retry_policy: map.task.retry_policy.clone(),
                    start_to_close_timeout: map.task.start_to_close_timeout,
                    heartbeat_timeout: map.task.heartbeat_timeout,
                    attempt: 1,
                    input,
                    map_item: Some(ActivityMapItem {
                        map_command_id: map_command_id.clone(),
                        item_ordinal,
                    }),
                },
                claim: None,
                completed: false,
                timeout_at,
                heartbeat_deadline_at: None,
            },
        );
    }
    Ok(())
}

fn change_version_record_for_run(
    run: &RunRecord,
    run_id: &RunId,
    event_id: EventId,
    data: &HistoryEventData,
    now: TimestampMs,
) -> Option<WorkflowChangeVersionRecord> {
    let (change_id, version, marker_kind, command_seq) = match data {
        HistoryEventData::VersionMarker(marker) => (
            marker.change_id.clone(),
            marker.version,
            WorkflowChangeMarkerKind::Version,
            marker.command_id.seq,
        ),
        HistoryEventData::DeprecatedPatchMarker(marker) => (
            marker.patch_id.clone(),
            1,
            WorkflowChangeMarkerKind::DeprecatedPatch,
            marker.command_id.seq,
        ),
        _ => return None,
    };
    Some(WorkflowChangeVersionRecord {
        namespace: run.namespace.clone(),
        workflow_id: run.workflow_id.clone(),
        workflow_type: run.workflow_type.clone(),
        run_id: run_id.clone(),
        change_id,
        version,
        marker_kind,
        status: if run.terminal {
            WorkflowChangeVersionStatus::Closed
        } else {
            WorkflowChangeVersionStatus::Open
        },
        command_seq,
        first_event_id: event_id,
        last_seen_at: now,
    })
}

fn cleanup_run_operational_state(state: &mut MemoryState, run_id: &RunId) {
    state.waits.retain(|_, wait| &wait.run_id != run_id);
    for record in state.activities.values_mut() {
        if &record.task.run_id == run_id {
            record.completed = true;
            record.claim = None;
            record.heartbeat_deadline_at = None;
        }
    }
    for map in state.activity_maps.values_mut() {
        if &map.task.map_command_id.run_id == run_id {
            map.completed = true;
            map.in_flight = 0;
        }
    }
}

fn dispatch_child_start(state: &mut MemoryState, outbox_id: &str) -> Result<()> {
    let message = {
        let Some(record) = state.child_outbox.get(outbox_id) else {
            return Ok(());
        };
        if record.dispatched {
            return Ok(());
        }
        record.message.clone()
    };

    let parent_terminal = state
        .runs
        .get(&message.command_id.run_id)
        .map(|run| run.terminal)
        .unwrap_or(true);
    if parent_terminal && message.parent_close_policy == ParentClosePolicy::Cancel {
        if let Some(record) = state.child_outbox.get_mut(outbox_id) {
            record.dispatched = true;
        }
        return Ok(());
    }

    let child_run_id = match state
        .workflow_ids
        .get(&(
            state
                .runs
                .get(&message.command_id.run_id)
                .ok_or_else(|| Error::RunNotFound(message.command_id.run_id.clone()))?
                .namespace
                .clone(),
            message.workflow_id.clone(),
        ))
        .cloned()
    {
        Some(existing_run_id) => {
            let same_child = state
                .runs
                .get(&existing_run_id)
                .and_then(|run| run.parent.as_ref())
                .is_some_and(|parent| parent.command_id == message.command_id);
            if !same_child {
                append_child_start_failed(
                    state,
                    &message.command_id,
                    crate::DurableFailure::non_retryable(
                        "durust.child_workflow_id_conflict",
                        format!("workflow id `{}` is already started", message.workflow_id),
                    ),
                )?;
                if let Some(record) = state.child_outbox.get_mut(outbox_id) {
                    record.dispatched = true;
                }
                return Ok(());
            }
            existing_run_id
        }
        None => start_child_run(state, &message)?,
    };

    append_child_started(state, &message, child_run_id.clone())?;
    if let Some(record) = state.child_outbox.get_mut(outbox_id) {
        record.dispatched = true;
        record.child_run_id = Some(child_run_id);
    }
    Ok(())
}

fn start_child_run(state: &mut MemoryState, message: &ChildStartOutboxMessage) -> Result<RunId> {
    let parent_run = state
        .runs
        .get(&message.command_id.run_id)
        .ok_or_else(|| Error::RunNotFound(message.command_id.run_id.clone()))?;
    state.next_run_id += 1;
    let run_id = RunId::new(format!("run-{}", state.next_run_id));
    let start = HistoryEvent {
        event_id: EventId(1),
        event_type: crate::HistoryEventType::WorkflowStarted,
        data: HistoryEventData::WorkflowStarted {
            workflow_type: message.workflow_type.clone(),
            input: message.input.clone(),
        },
    };
    state.workflow_ids.insert(
        (parent_run.namespace.clone(), message.workflow_id.clone()),
        run_id.clone(),
    );
    state.runs.insert(
        run_id.clone(),
        RunRecord {
            namespace: parent_run.namespace.clone(),
            workflow_id: message.workflow_id.clone(),
            workflow_type: message.workflow_type.clone(),
            task_queue: message.task_queue.clone(),
            history: vec![start],
            ready: Some(WorkflowTaskReason::WorkflowStarted),
            ready_at: None,
            workflow_claim: None,
            terminal: false,
            parent: Some(ChildParentLink {
                parent_run_id: message.command_id.run_id.clone(),
                command_id: message.command_id.clone(),
                parent_close_policy: message.parent_close_policy,
            }),
        },
    );
    Ok(run_id)
}

fn append_child_started(
    state: &mut MemoryState,
    message: &ChildStartOutboxMessage,
    child_run_id: RunId,
) -> Result<()> {
    if child_event_exists(state, &message.command_id) {
        return Ok(());
    }
    let Some(parent) = state.runs.get_mut(&message.command_id.run_id) else {
        return Err(Error::RunNotFound(message.command_id.run_id.clone()));
    };
    if parent.terminal {
        return Ok(());
    }
    let event_id = parent
        .history
        .last()
        .map(|event| event.event_id.next())
        .unwrap_or(EventId(1));
    parent.history.push(HistoryEvent {
        event_id,
        event_type: crate::HistoryEventType::ChildWorkflowStarted,
        data: HistoryEventData::ChildWorkflowStarted(crate::ChildWorkflowStarted {
            command_id: message.command_id.clone(),
            workflow_id: message.workflow_id.clone(),
            run_id: child_run_id,
        }),
    });
    parent.ready = Some(WorkflowTaskReason::ChildWorkflowStarted);
    parent.ready_at = None;
    Ok(())
}

fn append_child_start_failed(
    state: &mut MemoryState,
    command_id: &crate::CommandId,
    failure: crate::DurableFailure,
) -> Result<()> {
    if child_event_exists(state, command_id) {
        return Ok(());
    }
    let Some(parent) = state.runs.get_mut(&command_id.run_id) else {
        return Err(Error::RunNotFound(command_id.run_id.clone()));
    };
    if parent.terminal {
        return Ok(());
    }
    let event_id = parent
        .history
        .last()
        .map(|event| event.event_id.next())
        .unwrap_or(EventId(1));
    parent.history.push(HistoryEvent {
        event_id,
        event_type: crate::HistoryEventType::ChildWorkflowFailed,
        data: HistoryEventData::ChildWorkflowFailed(crate::ChildWorkflowFailed {
            command_id: command_id.clone(),
            failure,
        }),
    });
    parent.ready = Some(WorkflowTaskReason::ChildWorkflowFailed);
    parent.ready_at = None;
    Ok(())
}

fn child_event_exists(state: &MemoryState, command_id: &crate::CommandId) -> bool {
    state.runs.get(&command_id.run_id).is_some_and(|run| {
        run.history.iter().any(|event| match &event.data {
            HistoryEventData::ChildWorkflowStarted(started) => started.command_id == *command_id,
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                completed.command_id == *command_id
            }
            HistoryEventData::ChildWorkflowFailed(failed) => failed.command_id == *command_id,
            HistoryEventData::ChildWorkflowCancelled(cancelled) => {
                cancelled.command_id == *command_id
            }
            _ => false,
        })
    })
}

fn handle_terminal_run(state: &mut MemoryState, run_id: &RunId, terminal_event: &HistoryEventData) {
    notify_parent_of_child_terminal(state, run_id, terminal_event);
    cancel_children_for_parent(state, run_id);
}

fn notify_parent_of_child_terminal(
    state: &mut MemoryState,
    child_run_id: &RunId,
    terminal_event: &HistoryEventData,
) {
    let Some(parent) = state
        .runs
        .get(child_run_id)
        .and_then(|run| run.parent.clone())
    else {
        return;
    };
    if child_terminal_event_exists(state, &parent.command_id) {
        return;
    }
    let Some(parent_run) = state.runs.get_mut(&parent.parent_run_id) else {
        return;
    };
    if parent_run.terminal {
        return;
    }
    let event_id = parent_run
        .history
        .last()
        .map(|event| event.event_id.next())
        .unwrap_or(EventId(1));
    let (event_type, data, reason) = match terminal_event {
        HistoryEventData::WorkflowCompleted { result } => (
            crate::HistoryEventType::ChildWorkflowCompleted,
            HistoryEventData::ChildWorkflowCompleted(crate::ChildWorkflowCompleted {
                command_id: parent.command_id,
                result: result.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowCompleted,
        ),
        HistoryEventData::WorkflowFailed { failure } => (
            crate::HistoryEventType::ChildWorkflowFailed,
            HistoryEventData::ChildWorkflowFailed(crate::ChildWorkflowFailed {
                command_id: parent.command_id,
                failure: failure.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowFailed,
        ),
        HistoryEventData::WorkflowCancelled { reason } => (
            crate::HistoryEventType::ChildWorkflowCancelled,
            HistoryEventData::ChildWorkflowCancelled(crate::ChildWorkflowCancelled {
                command_id: parent.command_id,
                reason: reason.clone(),
            }),
            WorkflowTaskReason::ChildWorkflowCancelled,
        ),
        _ => return,
    };
    parent_run.history.push(HistoryEvent {
        event_id,
        event_type,
        data,
    });
    parent_run.ready = Some(reason);
    parent_run.ready_at = None;
}

fn child_terminal_event_exists(state: &MemoryState, command_id: &crate::CommandId) -> bool {
    state.runs.get(&command_id.run_id).is_some_and(|run| {
        run.history.iter().any(|event| match &event.data {
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                completed.command_id == *command_id
            }
            HistoryEventData::ChildWorkflowFailed(failed) => failed.command_id == *command_id,
            HistoryEventData::ChildWorkflowCancelled(cancelled) => {
                cancelled.command_id == *command_id
            }
            _ => false,
        })
    })
}

fn cancel_children_for_parent(state: &mut MemoryState, parent_run_id: &RunId) {
    for record in state.child_outbox.values_mut() {
        if record.message.command_id.run_id == *parent_run_id
            && record.message.parent_close_policy == ParentClosePolicy::Cancel
            && record.child_run_id.is_none()
        {
            record.dispatched = true;
        }
    }

    let children = state
        .runs
        .iter()
        .filter_map(|(run_id, run)| {
            run.parent
                .as_ref()
                .is_some_and(|parent| {
                    parent.parent_run_id == *parent_run_id
                        && parent.parent_close_policy == ParentClosePolicy::Cancel
                        && !run.terminal
                })
                .then(|| run_id.clone())
        })
        .collect::<Vec<_>>();
    for child_run_id in children {
        let terminal_event = HistoryEventData::WorkflowCancelled {
            reason: format!("parent workflow `{parent_run_id}` closed"),
        };
        if let Some(child) = state.runs.get_mut(&child_run_id) {
            let event_id = child
                .history
                .last()
                .map(|event| event.event_id.next())
                .unwrap_or(EventId(1));
            child.history.push(HistoryEvent {
                event_id,
                event_type: crate::HistoryEventType::WorkflowCancelled,
                data: terminal_event.clone(),
            });
            child.terminal = true;
            child.ready = None;
            child.ready_at = None;
            child.workflow_claim = None;
        }
        cleanup_run_operational_state(state, &child_run_id);
    }
}

fn cancel_command_operational_state(state: &mut MemoryState, command_id: &crate::CommandId) {
    for record in state.activities.values_mut() {
        let matches_activity = record.task.command_id == *command_id;
        let matches_map_item = record
            .task
            .map_item
            .as_ref()
            .is_some_and(|item| item.map_command_id == *command_id);
        if matches_activity || matches_map_item {
            record.completed = true;
            record.claim = None;
            record.heartbeat_deadline_at = None;
        }
    }
    if let Some(map) = state.activity_maps.get_mut(command_id) {
        map.completed = true;
        map.in_flight = 0;
    }
    if let Some(record) = state.child_outbox.get_mut(&child_outbox_id(command_id)) {
        record.dispatched = true;
    }
}

fn child_outbox_id(command_id: &crate::CommandId) -> String {
    format!("{}:{}:child-start", command_id.run_id, command_id.seq.0)
}

fn complete_map_item(
    state: &mut MemoryState,
    task: ActivityTask,
    map_item: ActivityMapItem,
    result: crate::PayloadRef,
) -> Result<CompleteActivityOutcome> {
    let mut completed_map = None;
    {
        let Some(map) = state.activity_maps.get_mut(&map_item.map_command_id) else {
            return Err(Error::Backend(format!(
                "activity map `{}`:{} not found",
                map_item.map_command_id.run_id, map_item.map_command_id.seq.0
            )));
        };
        if map.completed {
            return Ok(CompleteActivityOutcome::AlreadyCompleted);
        }
        let index = usize::try_from(map_item.item_ordinal).unwrap_or(usize::MAX);
        if index >= map.input_manifest.item_count {
            return Err(Error::Backend(format!(
                "activity map item ordinal {} out of bounds",
                map_item.item_ordinal
            )));
        }
        if let std::collections::btree_map::Entry::Vacant(entry) =
            map.results.entry(map_item.item_ordinal)
        {
            entry.insert(result);
            map.in_flight = map.in_flight.saturating_sub(1);
        }
        if map.results.len() == map.input_manifest.item_count {
            map.completed = true;
            let results = (0..map.input_manifest.item_count)
                .map(|ordinal| {
                    map.results
                        .get(&(ordinal as u64))
                        .cloned()
                        .ok_or_else(|| Error::Backend(format!("missing result for item {ordinal}")))
                })
                .collect::<Result<Vec<_>>>()?;
            let result_manifest = encode_activity_map_result_manifest(
                map.task.result_manifest_name.clone(),
                results,
                &map.input_manifest.page_lengths,
            )?;
            completed_map = Some((result_manifest, map.input_manifest.item_count));
        }
    }

    if completed_map.is_none() {
        materialize_activity_map_items(state, &map_item.map_command_id)?;
    }

    let event_id = if let Some((result_manifest, item_count)) = completed_map {
        let Some(run) = state.runs.get_mut(&task.run_id) else {
            return Err(Error::RunNotFound(task.run_id));
        };
        if run.terminal {
            return Err(Error::TerminalWorkflow);
        }
        let event_id = run
            .history
            .last()
            .map(|event| event.event_id.next())
            .unwrap_or(EventId(1));
        run.history.push(HistoryEvent {
            event_id,
            event_type: crate::HistoryEventType::ActivityMapCompleted,
            data: HistoryEventData::ActivityMapCompleted(crate::ActivityMapCompleted {
                command_id: map_item.map_command_id,
                result_manifest,
                item_count,
                success_count: item_count,
                failure_count: 0,
            }),
        });
        run.ready = Some(WorkflowTaskReason::ActivityMapCompleted);
        run.ready_at = None;
        event_id
    } else {
        state
            .runs
            .get(&task.run_id)
            .and_then(|run| run.history.last().map(|event| event.event_id))
            .unwrap_or(EventId::ZERO)
    };

    Ok(CompleteActivityOutcome::Completed { event_id })
}

fn fail_map_item(
    state: &mut MemoryState,
    task: ActivityTask,
    map_item: ActivityMapItem,
    failure: crate::DurableFailure,
) -> Result<FailActivityOutcome> {
    if let Some(map) = state.activity_maps.get_mut(&map_item.map_command_id) {
        if map.completed {
            return Ok(FailActivityOutcome::AlreadyCompleted);
        }
        map.completed = true;
        map.in_flight = map.in_flight.saturating_sub(1);
    }
    let Some(run) = state.runs.get_mut(&task.run_id) else {
        return Err(Error::RunNotFound(task.run_id));
    };
    if run.terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = run
        .history
        .last()
        .map(|event| event.event_id.next())
        .unwrap_or(EventId(1));
    run.history.push(HistoryEvent {
        event_id,
        event_type: crate::HistoryEventType::ActivityMapFailed,
        data: HistoryEventData::ActivityMapFailed(crate::ActivityMapFailed {
            command_id: map_item.map_command_id,
            failure,
        }),
    });
    run.ready = Some(WorkflowTaskReason::ActivityMapFailed);
    run.ready_at = None;
    Ok(FailActivityOutcome::Failed { event_id })
}

fn timeout_activity(
    state: &mut MemoryState,
    activity_id: &ActivityId,
    now: TimestampMs,
) -> Result<bool> {
    let timed_out_task = {
        let Some(record) = state.activities.get_mut(activity_id) else {
            return Ok(false);
        };
        if record.completed || !activity_due_at(record).is_some_and(|due_at| due_at <= now) {
            return Ok(false);
        }
        let timed_out_by_heartbeat = record
            .heartbeat_deadline_at
            .is_some_and(|deadline| deadline <= now)
            && !record
                .timeout_at
                .is_some_and(|timeout_at| timeout_at <= now);

        let task = record.task.clone();
        if should_retry_activity(&task) {
            record.task.attempt = record.task.attempt.saturating_add(1);
            record.claim = None;
            record.timeout_at = activity_timeout_at(now, record.task.start_to_close_timeout);
            record.heartbeat_deadline_at = None;
            return Ok(true);
        }

        record.completed = true;
        (task, timed_out_by_heartbeat)
    };

    let (timed_out_task, timed_out_by_heartbeat) = timed_out_task;
    if let Some(map_item) = timed_out_task.map_item.clone() {
        fail_map_item(
            state,
            timed_out_task.clone(),
            map_item,
            crate::DurableFailure::new(
                "durust.activity_timed_out",
                timeout_message(activity_id, timed_out_task.attempt, timed_out_by_heartbeat),
            ),
        )?;
        return Ok(true);
    }

    let Some(run) = state.runs.get_mut(&timed_out_task.run_id) else {
        return Err(Error::RunNotFound(timed_out_task.run_id));
    };
    if run.terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = run
        .history
        .last()
        .map(|event| event.event_id.next())
        .unwrap_or(EventId(1));
    run.history.push(HistoryEvent {
        event_id,
        event_type: crate::HistoryEventType::ActivityTimedOut,
        data: HistoryEventData::ActivityTimedOut(crate::ActivityTimedOut {
            command_id: timed_out_task.command_id,
            message: timeout_message(activity_id, timed_out_task.attempt, timed_out_by_heartbeat),
        }),
    });
    run.ready = Some(WorkflowTaskReason::ActivityTimedOut);
    run.ready_at = None;
    Ok(true)
}

fn should_retry_activity(task: &ActivityTask) -> bool {
    task.attempt < task.retry_policy.max_attempts.max(1)
}

fn activity_timeout_at(now: TimestampMs, timeout: Option<Duration>) -> Option<TimestampMs> {
    timeout.map(|timeout| {
        TimestampMs(
            now.0
                .saturating_add(i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX)),
        )
    })
}

fn activity_due_at(record: &ActivityRecord) -> Option<TimestampMs> {
    match (record.timeout_at, record.heartbeat_deadline_at) {
        (Some(timeout_at), Some(heartbeat_deadline_at)) => {
            Some(timeout_at.min(heartbeat_deadline_at))
        }
        (Some(timeout_at), None) => Some(timeout_at),
        (None, Some(heartbeat_deadline_at)) => Some(heartbeat_deadline_at),
        (None, None) => None,
    }
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

fn ready_at_for_delay(delay: Duration) -> Option<Instant> {
    if delay.is_zero() {
        None
    } else {
        let now = Instant::now();
        Some(now.checked_add(delay).unwrap_or(now))
    }
}
