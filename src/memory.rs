use crate::map_engine::{
    ACTIVITY_MAP_LABEL, CHILD_WORKFLOW_MAP_LABEL, ItemAttemptFailureKind, ItemRetryDecision,
    MapEffect, MapEvent, MapKind, MapReject, MapState, activity_outcome_counts,
    map_command_cancelled_reason, outcome_counts, validate_map_slot_bound,
};
use crate::payload::ManifestKind;
use crate::provider_util::{
    ActivityFailureDecision, TerminalCleanup, activity_claim_implicit_heartbeat_ms,
    activity_failure_decision, activity_heartbeat_deadline_at_ms, activity_timeout_attribution,
    activity_timeout_decision, child_terminal_event_data_and_reason,
    child_terminal_map_item_outcome, claim_lease_until_ms, commit_has_workflow_visible_mutations,
    payload_gc_cutoff_ms, post_commit_ready_reason, retry_visible_at_ms, timeout_message,
};
use crate::{
    ActivityId, ActivityMapInputManifest, ActivityMapInputPage, ActivityMapItem,
    ActivityMapResultManifest, ActivityMapResultPage, ActivityMapTask, ActivityTask,
    ActivityTaskClaim, CancelWorkflowOutcome, CancelWorkflowRequest, ChildStartOutboxMessage,
    ChildWorkflowMapFailureMode, ChildWorkflowMapItem, ChildWorkflowMapItemOutcome,
    ChildWorkflowMapTask, ClaimActivityOptions, ClaimWorkflowTaskOptions, ClaimedActivityTask,
    ClaimedWorkflowTask, CompleteActivityOutcome, CompleteActivityRequest,
    DispatchChildWorkflowStartsOutcome, DispatchChildWorkflowStartsRequest, DurableBackend, Error,
    EventId, FailActivityOutcome, FailActivityRequest, FireDueTimersOutcome, FireDueTimersRequest,
    HistoryChunk, HistoryEvent, HistoryEventData, Namespace, ParentClosePolicy, PayloadBlob,
    PayloadRef, PayloadRootRef, PayloadRootsOutcome, PayloadStorageConfig, ReadSignalInboxRequest,
    Result, RunId, SignalId, SignalInboxRecord, SignalWorkflowOutcome, SignalWorkflowRequest,
    StartWorkflowOutcome, StartWorkflowRequest, TimeoutDueActivitiesOutcome,
    TimeoutDueActivitiesRequest, TimestampMs, WaitId, WaitKind, WaitRecord,
    WorkflowChangeMarkerKind, WorkflowChangeVersionRecord, WorkflowChangeVersionStatus,
    WorkflowChangeVersionsOutcome, WorkflowChangeVersionsRequest, WorkflowId, WorkflowTaskClaim,
    WorkflowTaskCommit, WorkflowTaskReason, activity_map_input_at, digest_bytes,
    encode_activity_map_result_manifest_with_codec,
    encode_child_workflow_map_result_manifest_with_codec, event_payload_len, is_terminal,
};
use futures::future::{BoxFuture, ready};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub struct MemoryBackend {
    state: Arc<Mutex<MemoryState>>,
    payload_config: PayloadStorageConfig,
    // Signaled by every mutation that can create worker-visible work so
    // `wait_for_ready` waiters wake immediately instead of sleeping out
    // their `max_wait`.
    work_notify: Arc<tokio::sync::Notify>,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::with_payload_storage(PayloadStorageConfig::default())
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_payload_storage(payload_config: PayloadStorageConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState::default())),
            payload_config,
            work_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn notify_work(&self) {
        self.work_notify.notify_waiters();
    }

    pub fn payload_blob_count(&self) -> usize {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        state.payload_blobs.len()
    }

    /// Moves the virtual clock forward to `now`; an earlier instant is ignored,
    /// since leases and timers were already judged against the current one.
    pub fn advance_time_to(&self, now: TimestampMs) {
        let moved = {
            let mut state = self.state.lock().expect("memory backend mutex poisoned");
            if now.0 > state.now.0 {
                state.now = now;
                true
            } else {
                false
            }
        };
        if moved {
            self.notify_work();
        }
    }

    /// The first unconsumed inbox record of every signal name the run holds,
    /// in received order: what a claim carries in the TypeScript contract.
    pub fn live_signals(&self, run_id: &RunId) -> Result<Vec<SignalInboxRecord>> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        let mut ordered = state
            .signals
            .iter()
            .filter(|(_, signal)| signal.run_id == *run_id && !signal.consumed)
            .collect::<Vec<_>>();
        ordered.sort_by_key(|(_, signal)| signal.received_sequence);
        let mut seen = std::collections::BTreeSet::new();
        let mut records = Vec::new();
        for (signal_id, signal) in ordered {
            if !seen.insert(signal.signal_name.clone()) {
                continue;
            }
            records.push(SignalInboxRecord {
                signal_id: signal_id.clone(),
                signal_name: signal.signal_name.clone(),
                payload: hydrate_payload_from_storage(&state, signal.payload.clone())?,
            });
        }
        Ok(records)
    }

    pub fn advance_time(&self, duration: std::time::Duration) {
        {
            let mut state = self.state.lock().expect("memory backend mutex poisoned");
            state.now = TimestampMs(
                state
                    .now
                    .0
                    .saturating_add(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)),
            );
        }
        // Delayed releases and due timers may have become visible.
        self.notify_work();
    }

    /// Shared body of [`DurableBackend::stream_history`] and
    /// [`DurableBackend::stream_history_for_replay`]. `hydrate` is the whole
    /// difference between them: the public read resolves blob refs, the replay
    /// read hands them back untouched so the worker pays for only what it
    /// decodes. Passing `false` on the public path is a silent storage bug, not
    /// a compile error — `memory_provider_replay_stream_keeps_large_payloads_
    /// lazy_until_explicit_hydration` and three other payload tests are what
    /// catch it.
    fn stream_history_inner(
        &self,
        req: crate::StreamHistoryRequest,
        hydrate: bool,
    ) -> Result<HistoryChunk> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        let Some(run) = state.runs.get(&req.run_id) else {
            return Err(Error::RunNotFound(req.run_id));
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
            let mut event = event.clone();
            if hydrate {
                event.data = hydrate_history_event_from_storage(&state, event.data)?;
            }
            events.push(event);
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

        Ok(HistoryChunk {
            events,
            last_event_id,
            has_more,
        })
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
    child_workflow_maps: BTreeMap<crate::CommandId, ChildWorkflowMapRecord>,
    child_outbox: BTreeMap<String, ChildOutboxRecord>,
    waits: BTreeMap<WaitId, WaitRecord>,
    signals: BTreeMap<SignalId, SignalRecord>,
    query_projections: BTreeMap<(Namespace, WorkflowId), QueryProjectionRecord>,
    workflow_change_versions: BTreeMap<(RunId, String), WorkflowChangeVersionRecord>,
    payload_blobs: BTreeMap<String, StoredPayloadBlob>,
}

// `stored_at` follows the virtual clock and is refreshed whenever a
// content-addressed store call reuses the blob, so the GC grace period
// (`PayloadGarbageCollectionRequest::min_age`) protects blobs that an
// in-flight commit is about to reference.
struct StoredPayloadBlob {
    blob: PayloadBlob,
    stored_at: TimestampMs,
}

struct RunRecord {
    namespace: Namespace,
    workflow_id: WorkflowId,
    workflow_type: crate::WorkflowType,
    task_queue: crate::TaskQueue,
    history: Vec<HistoryEvent>,
    // Command seqs with a child lifecycle event (started/terminal) in this
    // run's history, and the terminal-only subset. Kept so child start and
    // terminal notification dedup are lookups instead of history scans;
    // rebuildable from history.
    child_event_seqs: BTreeSet<u64>,
    child_terminal_seqs: BTreeSet<u64>,
    ready: Option<WorkflowTaskReason>,
    // Virtual-clock visibility deadline for delayed releases; `advance_time`
    // controls when a deferred task becomes claimable again.
    ready_at: Option<TimestampMs>,
    workflow_claim: Option<WorkflowClaim>,
    terminal: bool,
    parent: Option<ChildParentLink>,
}

impl RunRecord {
    // The single append point for run history so the child dedup indexes
    // cannot drift from the events actually stored.
    fn push_history(&mut self, event: HistoryEvent) {
        match &event.data {
            HistoryEventData::ChildWorkflowStarted(started) => {
                self.child_event_seqs.insert(started.command_id.seq.0);
            }
            HistoryEventData::ChildWorkflowCompleted(completed) => {
                self.child_event_seqs.insert(completed.command_id.seq.0);
                self.child_terminal_seqs.insert(completed.command_id.seq.0);
            }
            HistoryEventData::ChildWorkflowFailed(failed) => {
                self.child_event_seqs.insert(failed.command_id.seq.0);
                self.child_terminal_seqs.insert(failed.command_id.seq.0);
            }
            HistoryEventData::ChildWorkflowCancelled(cancelled) => {
                self.child_event_seqs.insert(cancelled.command_id.seq.0);
                self.child_terminal_seqs.insert(cancelled.command_id.seq.0);
            }
            _ => {}
        }
        self.history.push(event);
    }
}

// Lease expiry is compared against the virtual clock (`state.now`) so
// deterministic simulations can expire claims with `advance_time`.
struct WorkflowClaim {
    token: u64,
    lease_until: TimestampMs,
    /// The run's history tail when this claim was handed out, which is also the
    /// worker's replay target. At commit it answers whether facts landed under
    /// the claim that the task never saw, so their wake reason is kept rather
    /// than consumed with the task.
    tail_at_claim: EventId,
}

impl WorkflowClaim {
    fn holds(claim: &Option<WorkflowClaim>, token: u64) -> bool {
        claim.as_ref().is_some_and(|claim| claim.token == token)
    }

    fn reclaimable(claim: &Option<WorkflowClaim>, now: TimestampMs) -> bool {
        claim.as_ref().is_none_or(|claim| claim.lease_until <= now)
    }
}

#[derive(Clone)]
struct ChildParentLink {
    parent_run_id: RunId,
    command_id: crate::CommandId,
    parent_close_policy: ParentClosePolicy,
    child_map_item: Option<ChildWorkflowMapItem>,
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
    /// Lease-as-heartbeat interval stamped at claim when the task has no
    /// explicit timeouts; heartbeat refreshes re-derive the deadline from it.
    /// Cleared on retry, re-stamped at the next claim.
    implicit_heartbeat_ms: Option<i64>,
    /// Retry-backoff visibility: the task is not claimable before this
    /// instant. `None` means immediately visible.
    visible_at: Option<TimestampMs>,
}

struct ActivityMapRecord {
    task: ActivityMapTask,
    input_manifest: ActivityMapInputManifest,
    results: BTreeMap<u64, crate::PayloadRef>,
    next_ordinal: u64,
    in_flight: usize,
    completed: bool,
}

struct ChildWorkflowMapRecord {
    task: ChildWorkflowMapTask,
    input_manifest: ActivityMapInputManifest,
    outcomes: BTreeMap<u64, ChildWorkflowMapItemOutcome>,
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

/// A late call for an activity record that is gone: terminal cleanup deleted
/// it, so the activity is over, unless the id names a run this provider
/// never started.
fn missing_activity_outcome(state: &MemoryState, activity_id: &ActivityId) -> Result<()> {
    let run_id = crate::provider_util::activity_run_id(activity_id);
    if state.runs.contains_key(&run_id) {
        Ok(())
    } else {
        Err(Error::RunNotFound(run_id))
    }
}

impl DurableBackend for MemoryBackend {
    fn payload_storage_config(&self) -> PayloadStorageConfig {
        self.payload_config.clone()
    }

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

        let input = match normalize_payload_for_storage(&mut state, &self.payload_config, req.input)
        {
            Ok(input) => input,
            Err(err) => return Box::pin(ready(Err(err))),
        };
        state.next_run_id += 1;
        let run_id = RunId::new(format!("run-{}", state.next_run_id));
        let start = HistoryEvent {
            event_id: EventId(1),
            data: HistoryEventData::WorkflowStarted {
                workflow_type: req.workflow_type.clone(),
                input,
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
                child_event_seqs: BTreeSet::new(),
                child_terminal_seqs: BTreeSet::new(),
                ready: Some(WorkflowTaskReason::WorkflowStarted),
                ready_at: None,
                workflow_claim: None,
                terminal: false,
                parent: None,
            },
        );

        drop(state);
        self.notify_work();
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
            return Box::pin(ready(Err(Error::WorkflowNotFound(req.workflow_id.clone()))));
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
            run.push_history(HistoryEvent {
                event_id,
                data: terminal_event.clone(),
            });
            run.terminal = true;
            run.ready = None;
            run.ready_at = None;
            run.workflow_claim = None;
            event_id
        };
        cleanup_run_operational_state(&mut state, &run_id, TerminalCleanup::Closed);
        let config = self.payload_config.clone();
        // The cancellation itself is already durable and this provider has no
        // transaction to undo it, so a routing failure must not be reported as
        // if nothing happened: the SQL providers roll back, where an error
        // truthfully means the run was *not* cancelled, and a bare error here
        // would mean the opposite on the same API. The message says which,
        // because the outcome type cannot. A retry answers `AlreadyTerminal`,
        // which is then consistent with it rather than contradicting it.
        if let Err(err) = handle_terminal_run(&mut state, &config, &run_id, &terminal_event) {
            return Box::pin(ready(Err(Error::Backend(format!(
                "workflow run `{run_id}` was cancelled, but routing its terminal \
                 outcome to its parent failed: {err}"
            )))));
        }

        drop(state);
        self.notify_work();
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
        let now = state.now;
        let Some(run_id) = state.runs.iter().find_map(|(run_id, run)| {
            let matches = run.namespace == opts.namespace
                && run.task_queue == opts.task_queue
                && run.ready.is_some()
                && run.ready_at.is_none_or(|ready_at| ready_at <= now)
                && WorkflowClaim::reclaimable(&run.workflow_claim, now)
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
        // The ready reason stays on the run while claimed so a reclaim after
        // lease expiry hands out the same task a fresh claim would; commit,
        // conflict, and release overwrite it.
        let reason = run
            .ready
            .clone()
            .expect("ready reason selected from ready run");
        let replay_target_event_id = run
            .history
            .last()
            .map(|event| event.event_id)
            .unwrap_or(EventId::ZERO);
        run.workflow_claim = Some(WorkflowClaim {
            token,
            lease_until: TimestampMs(claim_lease_until_ms(now, opts.lease_duration)),
            tail_at_claim: replay_target_event_id,
        });
        let prefetched_history = run
            .history
            .iter()
            .rev()
            .take(16)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

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
            prefetched_history,
        }))))
    }

    fn stream_history(
        &self,
        req: crate::StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        Box::pin(ready(self.stream_history_inner(req, true)))
    }

    fn stream_history_for_replay(
        &self,
        req: crate::StreamHistoryRequest,
    ) -> BoxFuture<'static, Result<HistoryChunk>> {
        Box::pin(ready(self.stream_history_inner(req, false)))
    }

    fn hydrate_payload(&self, payload: PayloadRef) -> BoxFuture<'static, Result<PayloadRef>> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        Box::pin(ready(hydrate_payload_from_storage(&state, payload)))
    }

    fn hydrate_activity_map_result_manifest(
        &self,
        payload: PayloadRef,
    ) -> BoxFuture<'static, Result<PayloadRef>> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        Box::pin(ready(hydrate_activity_map_result_manifest_from_storage(
            &state, payload,
        )))
    }

    fn commit_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> BoxFuture<'static, Result<EventId>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        {
            let Some(run) = state.runs.get_mut(&claim.run_id) else {
                return Box::pin(ready(Err(Error::RunNotFound(claim.run_id))));
            };
            // The claim token is the fence. Facts appended while this task was
            // held (activity results, fired timers, child terminals) never
            // enter the replay window, so they do not invalidate it.
            if !WorkflowClaim::holds(&run.workflow_claim, claim.token) {
                return Box::pin(ready(Err(Error::StaleLease)));
            }
            if run.terminal && commit_has_workflow_visible_mutations(&batch) {
                return Box::pin(ready(Err(Error::TerminalWorkflow)));
            }
        }

        // Validated before this commit touches anything. The SQL providers reject
        // the same commits inside their descriptor-insert primitive and roll
        // back, but this provider has no transaction, so a rejection raised any
        // later than here would leave `ActivityMapScheduled` already appended to
        // a run whose map was never created.
        //
        // Two checks, both of which the SQL providers make at the same point. A
        // zero `max_in_flight` is a caller typo the boundary rejects rather than
        // clamps. A descriptor that already exists is an invariant violation:
        // SQLite raises on its unique index and Postgres on its
        // `on conflict do nothing` row count, while this provider's
        // `BTreeMap::insert` would *replace* it and re-step
        // `DescriptorCreated` — appending a second terminal fact for the same
        // command id when the map is empty. The insert must stay a replace, not
        // an upsert, because `MapState::next_ordinal`'s contract depends on the
        // cursor and the outcome set being created together at zero; so the
        // duplicate is rejected here instead.
        let mut scheduled_map_command_ids = BTreeSet::new();
        for (label, map_command_id, max_in_flight, exists) in batch
            .schedule_activity_maps
            .iter()
            .map(|task| {
                (
                    ACTIVITY_MAP_LABEL,
                    &task.map_command_id,
                    task.max_in_flight,
                    state.activity_maps.contains_key(&task.map_command_id),
                )
            })
            .chain(batch.schedule_child_workflow_maps.iter().map(|task| {
                (
                    CHILD_WORKFLOW_MAP_LABEL,
                    &task.map_command_id,
                    task.max_in_flight,
                    state.child_workflow_maps.contains_key(&task.map_command_id),
                )
            }))
        {
            if let Err(err) = validate_map_slot_bound(label, max_in_flight) {
                return Box::pin(ready(Err(err)));
            }
            if exists || !scheduled_map_command_ids.insert(map_command_id.clone()) {
                return Box::pin(ready(Err(Error::Backend(format!(
                    "map descriptor `{}`:{} already exists: a workflow task commit re-scheduled \
                     a map whose descriptor was never deleted",
                    map_command_id.run_id, map_command_id.seq.0
                )))));
            }
        }

        let config = self.payload_config.clone();
        let append_events =
            match normalize_history_events_for_storage(&mut state, &config, batch.append_events) {
                Ok(events) => events,
                Err(err) => return Box::pin(ready(Err(err))),
            };
        let scheduled = match normalize_activity_tasks_for_storage(
            &mut state,
            &config,
            batch.schedule_activities,
        ) {
            Ok(tasks) => tasks,
            Err(err) => return Box::pin(ready(Err(err))),
        };
        let mut decoded_maps = Vec::with_capacity(batch.schedule_activity_maps.len());
        for map_task in batch.schedule_activity_maps {
            let map_task =
                match normalize_activity_map_task_for_storage(&mut state, &config, map_task) {
                    Ok(map_task) => map_task,
                    Err(err) => return Box::pin(ready(Err(err))),
                };
            let manifest_payload = match hydrate_activity_map_input_manifest_from_storage(
                &state,
                map_task.input_manifest.clone(),
            ) {
                Ok(payload) => payload,
                Err(err) => return Box::pin(ready(Err(err))),
            };
            let manifest: ActivityMapInputManifest = match crate::decode_payload(&manifest_payload)
            {
                Ok(manifest) => manifest,
                Err(err) => return Box::pin(ready(Err(err))),
            };
            decoded_maps.push((map_task, manifest));
        }
        let mut decoded_child_maps = Vec::with_capacity(batch.schedule_child_workflow_maps.len());
        for map_task in batch.schedule_child_workflow_maps {
            let map_task = match normalize_child_workflow_map_task_for_storage(
                &mut state, &config, map_task,
            ) {
                Ok(map_task) => map_task,
                Err(err) => return Box::pin(ready(Err(err))),
            };
            let manifest_payload = match hydrate_activity_map_input_manifest_from_storage(
                &state,
                map_task.input_manifest.clone(),
            ) {
                Ok(payload) => payload,
                Err(err) => return Box::pin(ready(Err(err))),
            };
            let manifest: ActivityMapInputManifest = match crate::decode_payload(&manifest_payload)
            {
                Ok(manifest) => manifest,
                Err(err) => return Box::pin(ready(Err(err))),
            };
            decoded_child_maps.push((map_task, manifest));
        }
        let upsert_waits = batch.upsert_waits;
        let consume_signals = batch.consume_signals;
        let delete_waits = batch.delete_waits;
        let cancel_commands = batch.cancel_commands;
        let start_child_workflows = match normalize_child_start_messages_for_storage(
            &mut state,
            &config,
            batch.start_child_workflows,
        ) {
            Ok(messages) => messages,
            Err(err) => return Box::pin(ready(Err(err))),
        };
        let query_projection = match batch
            .query_projection
            .map(|payload| normalize_payload_for_storage(&mut state, &config, payload))
            .transpose()
        {
            Ok(payload) => payload,
            Err(err) => return Box::pin(ready(Err(err))),
        };
        let mut terminal_event = None;
        let mut projection_update = None;
        let mut change_version_updates = Vec::new();
        let now = state.now;
        let (next_event_id, unobserved_fact_reason) = {
            let Some(run) = state.runs.get_mut(&claim.run_id) else {
                return Box::pin(ready(Err(Error::RunNotFound(claim.run_id))));
            };
            let current_tail = run
                .history
                .last()
                .map(|event| event.event_id)
                .unwrap_or(EventId::ZERO);

            let mut next_event_id = current_tail;
            let mut terminal = false;
            for new_event in append_events {
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
                run.push_history(HistoryEvent {
                    event_id: next_event_id,
                    data,
                });
            }

            // Read before the claim is cleared: facts that landed under this
            // claim are ones the task never saw, so their wake reason is
            // carried past the clear below and re-applied instead of being
            // consumed with the task.
            let tail_at_claim = run
                .workflow_claim
                .as_ref()
                .map_or(EventId::ZERO, |claim| claim.tail_at_claim);
            let unobserved_fact_reason = (tail_at_claim < current_tail)
                .then_some(run.ready.clone())
                .flatten();
            run.workflow_claim = None;
            // Commit consumes the claimed task's readiness (the reason stays
            // on the run while claimed so lease-expiry reclaims see it); the
            // signal recheck below re-marks the run if consumable signals
            // remain, mirroring the SQL providers' commit update.
            run.ready = None;
            run.ready_at = None;
            if terminal {
                run.terminal = true;
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

            ((next_event_id, terminal), unobserved_fact_reason)
        };
        // A map scheduled with an empty input manifest is terminal at
        // descriptor creation, so its terminal fact is appended by this very
        // commit. The commit's tail and its post-commit ready reason have to
        // account for it, or the caller's cached tail is one behind and the
        // parent is never woken.
        let mut map_tail_event_id = None;
        let mut map_ready_reason = None;

        for task in scheduled {
            state.activities.insert(
                task.activity_id.clone(),
                ActivityRecord {
                    task,
                    claim: None,
                    completed: false,
                    // The start-to-close clock starts at the claim.
                    timeout_at: None,
                    heartbeat_deadline_at: None,
                    implicit_heartbeat_ms: None,
                    visible_at: None,
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
            let Some(map_state) = activity_map_state(&state, &map_task.map_command_id) else {
                continue;
            };
            let parent_terminal = next_event_id.1;
            match step_map(
                &mut state,
                &config,
                map_state,
                MapEvent::DescriptorCreated { parent_terminal },
            ) {
                Ok(Some(event_id)) => {
                    map_tail_event_id = Some(event_id);
                    map_ready_reason = Some(WorkflowTaskReason::ActivityMapCompleted);
                }
                Ok(None) => {}
                Err(err) => return Box::pin(ready(Err(err))),
            }
        }
        for (map_task, manifest) in decoded_child_maps {
            state.child_workflow_maps.insert(
                map_task.map_command_id.clone(),
                ChildWorkflowMapRecord {
                    task: map_task.clone(),
                    input_manifest: manifest,
                    outcomes: BTreeMap::new(),
                    next_ordinal: 0,
                    in_flight: 0,
                    completed: false,
                },
            );
            let Some(map_state) = child_workflow_map_state(&state, &map_task.map_command_id) else {
                continue;
            };
            let parent_terminal = next_event_id.1;
            match step_map(
                &mut state,
                &config,
                map_state,
                MapEvent::DescriptorCreated { parent_terminal },
            ) {
                Ok(Some(event_id)) => {
                    map_tail_event_id = Some(event_id);
                    map_ready_reason = Some(WorkflowTaskReason::ChildWorkflowMapCompleted);
                }
                Ok(None) => {}
                Err(err) => return Box::pin(ready(Err(err))),
            }
        }
        for message in start_child_workflows {
            state.child_outbox.insert(
                child_outbox_id(&message),
                ChildOutboxRecord {
                    message,
                    dispatched: false,
                    child_run_id: None,
                },
            );
        }
        // Fenced to the claimed run: a wait naming another run is skipped, and
        // a signal or wait belonging to another run is left untouched, so no
        // commit can act on state it does not own.
        for wait in upsert_waits {
            if wait.run_id != claim.run_id {
                continue;
            }
            state.waits.insert(wait.wait_id.clone(), wait);
        }
        for signal_id in consume_signals {
            if let Some(signal) = state.signals.get_mut(&signal_id)
                && signal.run_id == claim.run_id
            {
                signal.consumed = true;
            }
        }
        for wait_id in delete_waits {
            if state
                .waits
                .get(&wait_id)
                .is_some_and(|wait| wait.run_id == claim.run_id)
            {
                state.waits.remove(&wait_id);
            }
        }
        for command_id in cancel_commands {
            if let Err(err) = cancel_command_operational_state(&mut state, &config, &command_id) {
                return Box::pin(ready(Err(err)));
            }
        }
        if next_event_id.1 {
            let cleanup = terminal_event
                .as_ref()
                .map(TerminalCleanup::for_terminal_event)
                .unwrap_or(TerminalCleanup::Closed);
            cleanup_run_operational_state(&mut state, &claim.run_id, cleanup);
            if let Some(event) = terminal_event {
                if matches!(event, HistoryEventData::WorkflowContinuedAsNew { .. }) {
                    continue_run_as_new(&mut state, &claim.run_id, event);
                } else if let Err(err) =
                    handle_terminal_run(&mut state, &config, &claim.run_id, &event)
                {
                    return Box::pin(ready(Err(err)));
                }
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
        let terminal_after_commit = state.runs.get(&claim.run_id).is_none_or(|run| run.terminal);
        // An empty map completed at descriptor creation is the one same-commit
        // reason this bookkeeping can produce, and only the signal recheck can
        // otherwise re-mark the run; the child starts dispatched below set
        // their own reason afterwards.
        if let Some(reason) = post_commit_ready_reason(
            terminal_after_commit,
            map_ready_reason,
            signal_wait_ready,
            unobserved_fact_reason,
        ) && let Some(run) = state.runs.get_mut(&claim.run_id)
        {
            run.ready = Some(reason);
            run.ready_at = None;
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
        // The commit starts the children it requested and the map items it
        // admitted before it returns, as the Postgres provider does. The
        // returned tail predates their `ChildWorkflowStarted` events, which
        // the worker ingests like any other provider append.
        let namespace = state
            .runs
            .get(&claim.run_id)
            .map(|run| run.namespace.clone());
        if let Some(namespace) = namespace
            && let Err(err) =
                dispatch_pending_child_starts(&mut state, &config, &namespace, usize::MAX)
        {
            return Box::pin(ready(Err(err)));
        }
        // The tail the caller learns includes what the dispatch appended, as
        // it does on Postgres.
        let new_tail_event_id = state
            .runs
            .get(&claim.run_id)
            .and_then(|run| run.history.last())
            .map(|event| event.event_id)
            .unwrap_or_else(|| map_tail_event_id.unwrap_or(next_event_id.0));

        drop(state);
        self.notify_work();
        Box::pin(ready(Ok(new_tail_event_id)))
    }

    fn release_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        release: crate::WorkflowTaskRelease,
    ) -> BoxFuture<'static, Result<()>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let now = state.now;
        let Some(run) = state.runs.get_mut(&claim.run_id) else {
            return Box::pin(ready(Err(Error::RunNotFound(claim.run_id))));
        };
        if !WorkflowClaim::holds(&run.workflow_claim, claim.token) {
            return Box::pin(ready(Err(Error::StaleLease)));
        }
        run.workflow_claim = None;
        let became_ready = !run.terminal;
        if became_ready {
            run.ready_at = ready_at_for_delay(now, release.delay);
        } else {
            run.ready_at = None;
        }
        drop(state);
        if became_ready {
            self.notify_work();
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
            return Box::pin(ready(Err(Error::WorkflowNotFound(req.workflow_id.clone()))));
        };
        if state.runs.get(&run_id).is_some_and(|run| run.terminal) {
            return Box::pin(ready(Err(Error::TerminalWorkflow)));
        }
        let payload =
            match normalize_payload_for_storage(&mut state, &self.payload_config, req.payload) {
                Ok(payload) => payload,
                Err(err) => return Box::pin(ready(Err(err))),
            };
        state.next_signal_sequence += 1;
        let received_sequence = state.next_signal_sequence;
        state.signals.insert(
            req.signal_id.clone(),
            SignalRecord {
                run_id: run_id.clone(),
                signal_name: req.signal_name.clone(),
                payload,
                received_sequence,
                consumed: false,
            },
        );
        let has_wait = state.waits.values().any(|wait| {
            wait.run_id == run_id && wait.kind == WaitKind::Signal && wait.key == req.signal_name.0
        });
        if has_wait
            && let Some(run) = state.runs.get_mut(&run_id)
            && !run.terminal
            && run.workflow_claim.is_none()
        {
            run.ready = Some(WorkflowTaskReason::SignalReceived);
            run.ready_at = None;
        }
        drop(state);
        self.notify_work();
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
            .map(|(signal_id, signal)| {
                let payload = hydrate_payload_from_storage(&state, signal.payload.clone())?;
                Ok(SignalInboxRecord {
                    signal_id: signal_id.clone(),
                    signal_name: signal.signal_name.clone(),
                    payload,
                })
            })
            .transpose();
        match signal {
            Ok(signal) => Box::pin(ready(Ok(signal))),
            Err(err) => Box::pin(ready(Err(err))),
        }
    }

    fn fire_due_timers(
        &self,
        req: FireDueTimersRequest,
    ) -> BoxFuture<'static, Result<FireDueTimersOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        // Selected the way the SQL providers select: a due timer wait whose
        // run is in another namespace is never a candidate, one whose run is
        // gone is a candidate so the loop below deletes it, and the budget
        // counts candidates only.
        let due = state
            .waits
            .iter()
            .filter(|(_, wait)| {
                wait.kind == WaitKind::Timer
                    && wait.ready_at.is_some_and(|ready_at| ready_at <= req.now)
                    && state
                        .runs
                        .get(&wait.run_id)
                        .is_none_or(|run| run.namespace == req.namespace)
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
            // Skipped, not deleted, for the reason the SQL providers give.
            if run.terminal {
                continue;
            }
            let event_id = run
                .history
                .last()
                .map(|event| event.event_id.next())
                .unwrap_or(EventId(1));
            run.push_history(HistoryEvent {
                event_id,
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
        drop(state);
        // Guarded so idle maintenance passes do not wake other waiters and
        // spin them against each other.
        if fired > 0 {
            self.notify_work();
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

        let config = self.payload_config.clone();
        let mut timed_out = 0usize;
        for activity_id in due {
            match timeout_activity(&mut state, &config, &activity_id, req.now) {
                Ok(true) => timed_out += 1,
                Ok(false) => {}
                Err(err) => return Box::pin(ready(Err(err))),
            }
        }

        drop(state);
        if timed_out > 0 {
            self.notify_work();
        }
        Box::pin(ready(Ok(TimeoutDueActivitiesOutcome { timed_out })))
    }

    fn wait_for_ready(&self, req: crate::WaitForReadyRequest) -> BoxFuture<'static, Result<()>> {
        let notify = Arc::clone(&self.work_notify);
        Box::pin(async move {
            // Race the notification against the bounded sleep: a mutation
            // that lands between the caller's last claim check and this
            // registration is missed, so the sleep caps the staleness.
            let notified = std::pin::pin!(notify.notified());
            let sleep = std::pin::pin!(tokio::time::sleep(req.max_wait));
            futures::future::select(notified, sleep).await;
            Ok(())
        })
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
                || record
                    .visible_at
                    .is_some_and(|visible_at| visible_at > state.now)
                || !opts
                    .registered_activity_names
                    .iter()
                    .any(|name| name == &record.task.activity_name)
            {
                continue;
            }
            if let Some(map_item) = &record.task.map_item
                && state
                    .activity_maps
                    .get(&map_item.map_command_id)
                    .is_some_and(|map| map.completed)
            {
                continue;
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
        let task = {
            let record = state
                .activities
                .get_mut(&activity_id)
                .expect("activity id selected from activities map");
            record.claim = Some(token);
            // Start-to-close is measured from the claim, as the TypeScript
            // providers measure it: a task waiting in the queue has no
            // deadline, and the scan only ever times out a claimed attempt.
            record.timeout_at = activity_timeout_at(now, record.task.start_to_close_timeout);
            record.implicit_heartbeat_ms = activity_claim_implicit_heartbeat_ms(
                record.task.start_to_close_timeout,
                record.task.heartbeat_timeout,
                opts.lease_duration,
            );
            record.heartbeat_deadline_at = activity_heartbeat_deadline_at_ms(
                now,
                record.task.heartbeat_timeout,
                record.implicit_heartbeat_ms,
            )
            .map(TimestampMs);
            record.task.clone()
        };
        let task = match hydrate_activity_task_from_storage(&state, task) {
            Ok(task) => task,
            Err(err) => return Box::pin(ready(Err(err))),
        };
        Box::pin(ready(Ok(Some(ClaimedActivityTask {
            task,
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
            return Box::pin(ready(
                missing_activity_outcome(&state, &req.claim.activity_id)
                    .map(|()| crate::ActivityHeartbeatOutcome::AlreadyCompleted),
            ));
        };
        if record.completed {
            return Box::pin(ready(Ok(crate::ActivityHeartbeatOutcome::AlreadyCompleted)));
        }
        if record.claim != Some(req.claim.token) {
            return Box::pin(ready(Err(Error::StaleLease)));
        }

        record.heartbeat_deadline_at = activity_heartbeat_deadline_at_ms(
            now,
            record.task.heartbeat_timeout,
            record.implicit_heartbeat_ms,
        )
        .map(TimestampMs);
        Box::pin(ready(Ok(crate::ActivityHeartbeatOutcome::Recorded)))
    }

    fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> BoxFuture<'static, Result<CompleteActivityOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let task = {
            let Some(record) = state.activities.get_mut(&req.claim.activity_id) else {
                return Box::pin(ready(
                    missing_activity_outcome(&state, &req.claim.activity_id)
                        .map(|()| CompleteActivityOutcome::AlreadyCompleted),
                ));
            };
            if record.completed {
                return Box::pin(ready(Ok(CompleteActivityOutcome::AlreadyCompleted)));
            }
            if record.claim != Some(req.claim.token) {
                return Box::pin(ready(Err(Error::StaleLease)));
            }
            record.task.clone()
        };
        let config = self.payload_config.clone();
        if let Some(map_item) = task.map_item.clone() {
            let result = match normalize_payload_for_storage(&mut state, &config, req.result) {
                Ok(result) => result,
                Err(err) => return Box::pin(ready(Err(err))),
            };
            let outcome = match complete_map_item(&mut state, &config, task, map_item, result) {
                Ok(outcome) => outcome,
                Err(err) => return Box::pin(ready(Err(err))),
            };
            if let Some(record) = state.activities.get_mut(&req.claim.activity_id) {
                record.completed = true;
            }
            drop(state);
            self.notify_work();
            return Box::pin(ready(Ok(outcome)));
        }
        // Validate the run before touching the record or payload store so a
        // rejected completion leaves the record retryable and every retry
        // returns the same error, matching the SQL providers' transactional
        // rollback.
        if let Some(run) = state.runs.get(&task.run_id) {
            if run.terminal {
                return Box::pin(ready(Err(Error::TerminalWorkflow)));
            }
        } else {
            return Box::pin(ready(Err(Error::RunNotFound(task.run_id))));
        }
        let result = match normalize_payload_for_storage(&mut state, &config, req.result) {
            Ok(result) => result,
            Err(err) => return Box::pin(ready(Err(err))),
        };
        if let Some(record) = state.activities.get_mut(&req.claim.activity_id) {
            record.completed = true;
        }
        let Some(run) = state.runs.get_mut(&task.run_id) else {
            return Box::pin(ready(Err(Error::RunNotFound(task.run_id))));
        };
        let event_id = run
            .history
            .last()
            .map(|event| event.event_id.next())
            .unwrap_or(EventId(1));
        run.push_history(HistoryEvent {
            event_id,
            data: HistoryEventData::ActivityCompleted(crate::ActivityCompleted {
                command_id: task.command_id,
                result,
            }),
        });
        run.ready = Some(WorkflowTaskReason::ActivityCompleted);
        run.ready_at = None;

        drop(state);
        self.notify_work();
        Box::pin(ready(Ok(CompleteActivityOutcome::Completed { event_id })))
    }

    fn fail_activity(
        &self,
        req: FailActivityRequest,
    ) -> BoxFuture<'static, Result<FailActivityOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let now = state.now;
        let task = {
            let Some(record) = state.activities.get_mut(&req.claim.activity_id) else {
                return Box::pin(ready(
                    missing_activity_outcome(&state, &req.claim.activity_id)
                        .map(|()| FailActivityOutcome::AlreadyCompleted),
                ));
            };
            if record.completed {
                return Box::pin(ready(Ok(FailActivityOutcome::AlreadyCompleted)));
            }
            if record.claim != Some(req.claim.token) {
                return Box::pin(ready(Err(Error::StaleLease)));
            }
            record.task.clone()
        };
        let decision = activity_failure_decision(&task, &req.failure);

        // Map items take the engine route for *both* verdicts: a retry of an
        // item whose map already ended must not reschedule anything, and only
        // the engine knows whether the map ended.
        let config = self.payload_config.clone();
        if let Some(map_item) = task.map_item.clone() {
            let decision = ItemRetryDecision::from(decision);
            // Only an exhausting attempt persists its failure, so a retried
            // attempt never offloads a payload the map will discard.
            let failure = if matches!(decision, ItemRetryDecision::Exhausted) {
                match normalize_failure_for_storage(&mut state, &config, req.failure) {
                    Ok(failure) => failure,
                    Err(err) => return Box::pin(ready(Err(err))),
                }
            } else {
                req.failure
            };
            let outcome = match fail_map_item(
                &mut state,
                &config,
                task,
                map_item,
                failure,
                ItemAttemptFailureKind::Failed,
                decision,
                now,
            ) {
                Ok(outcome) => outcome,
                Err(err) => return Box::pin(ready(Err(err))),
            };
            if !matches!(outcome, FailActivityOutcome::RetryScheduled { .. })
                && let Some(record) = state.activities.get_mut(&req.claim.activity_id)
            {
                record.completed = true;
            }
            drop(state);
            self.notify_work();
            return Box::pin(ready(Ok(outcome)));
        }

        if let ActivityFailureDecision::Retry { next_attempt } = decision {
            let Some(record) = state.activities.get_mut(&req.claim.activity_id) else {
                return Box::pin(ready(Err(Error::Backend(format!(
                    "activity `{}` not found",
                    req.claim.activity_id.0
                )))));
            };
            record.task.attempt = next_attempt;
            record.claim = None;
            // The retry backoff delays visibility; the start-to-close clock
            // restarts at the visibility instant so the timeout scanner
            // cannot fire on a task that was never claimable.
            let visible_at = retry_visible_at_ms(&task.retry_policy, task.attempt, now);
            record.visible_at = visible_at.map(TimestampMs);
            record.timeout_at = None;
            record.heartbeat_deadline_at = None;
            record.implicit_heartbeat_ms = None;
            drop(state);
            self.notify_work();
            return Box::pin(ready(Ok(FailActivityOutcome::RetryScheduled {
                next_attempt,
                ready_at: visible_at.map(TimestampMs).unwrap_or(now),
            })));
        }

        // Validate the run before touching the record or payload store so a
        // rejected failure leaves the record retryable and every retry returns
        // the same error, matching the SQL providers' transactional rollback.
        if let Some(run) = state.runs.get(&task.run_id) {
            if run.terminal {
                return Box::pin(ready(Err(Error::TerminalWorkflow)));
            }
        } else {
            return Box::pin(ready(Err(Error::RunNotFound(task.run_id))));
        }
        let failure = match normalize_failure_for_storage(&mut state, &config, req.failure) {
            Ok(failure) => failure,
            Err(err) => return Box::pin(ready(Err(err))),
        };
        if let Some(record) = state.activities.get_mut(&req.claim.activity_id) {
            record.completed = true;
        }
        let Some(run) = state.runs.get_mut(&task.run_id) else {
            return Box::pin(ready(Err(Error::RunNotFound(task.run_id))));
        };
        let event_id = run
            .history
            .last()
            .map(|event| event.event_id.next())
            .unwrap_or(EventId(1));
        run.push_history(HistoryEvent {
            event_id,
            data: HistoryEventData::ActivityFailed(crate::ActivityFailed {
                command_id: task.command_id,
                failure,
            }),
        });
        run.ready = Some(WorkflowTaskReason::ActivityFailed);
        run.ready_at = None;

        drop(state);
        self.notify_work();
        Box::pin(ready(Ok(FailActivityOutcome::Failed { event_id })))
    }

    fn dispatch_child_workflow_starts(
        &self,
        req: DispatchChildWorkflowStartsRequest,
    ) -> BoxFuture<'static, Result<DispatchChildWorkflowStartsOutcome>> {
        let result = (|| {
            let mut state = self.state.lock().expect("memory backend mutex poisoned");
            let config = self.payload_config.clone();
            let dispatched =
                dispatch_pending_child_starts(&mut state, &config, &req.namespace, req.limit)?;
            Ok(DispatchChildWorkflowStartsOutcome { dispatched })
        })();
        if matches!(&result, Ok(outcome) if outcome.dispatched > 0) {
            self.notify_work();
        }
        Box::pin(ready(result))
    }

    fn query_projection(
        &self,
        req: crate::QueryProjectionRequest,
    ) -> BoxFuture<'static, Result<crate::QueryProjectionOutcome>> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        let key = (req.namespace, req.workflow_id);
        let outcome = match state.query_projections.get(&key) {
            Some(projection) => {
                match hydrate_payload_from_storage(&state, projection.payload.clone()) {
                    Ok(payload) => crate::QueryProjectionOutcome::Found {
                        run_id: projection.run_id.clone(),
                        event_id: projection.event_id,
                        payload,
                    },
                    Err(err) => return Box::pin(ready(Err(err))),
                }
            }
            None if state.workflow_ids.contains_key(&key) => {
                crate::QueryProjectionOutcome::NoProjection
            }
            None => crate::QueryProjectionOutcome::NotFound,
        };
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

    fn payload_roots(&self) -> BoxFuture<'static, Result<PayloadRootsOutcome>> {
        let state = self.state.lock().expect("memory backend mutex poisoned");
        Box::pin(ready(
            collect_payload_roots(&state).map(|roots| PayloadRootsOutcome { roots }),
        ))
    }

    fn gc_payload_blobs(
        &self,
        req: crate::PayloadGarbageCollectionRequest,
    ) -> BoxFuture<'static, Result<crate::PayloadGarbageCollectionOutcome>> {
        let mut state = self.state.lock().expect("memory backend mutex poisoned");
        let scanned_blobs = state.payload_blobs.len();
        let mut reachable = BTreeSet::new();
        if let Err(err) = collect_reachable_payload_blobs(&state, &mut reachable) {
            return Box::pin(ready(Err(err)));
        }
        // Grace period against the virtual clock: an unreachable-but-young
        // blob may belong to an in-flight commit.
        let cutoff = payload_gc_cutoff_ms(state.now.0, req.min_age);
        let deleted_blobs = state
            .payload_blobs
            .iter()
            .filter(|(digest, record)| !reachable.contains(*digest) && record.stored_at.0 <= cutoff)
            .count();
        let retained_blobs = scanned_blobs - deleted_blobs;
        if !req.dry_run {
            state
                .payload_blobs
                .retain(|digest, record| reachable.contains(digest) || record.stored_at.0 > cutoff);
        }
        Box::pin(ready(Ok(crate::PayloadGarbageCollectionOutcome {
            scanned_blobs,
            retained_blobs,
            deleted_blobs,
            failed_blobs: 0,
        })))
    }
}

/// Storage primitive behind [`MapEffect::MaterializeItems`] for an activity
/// map: read the manifest entries for `[first_ordinal, first_ordinal + count)`
/// and insert one item activity task per ordinal. *How many* ordinals to admit
/// and *from where* is [`crate::map_engine`]'s decision, never this function's.
fn insert_activity_map_item_batch(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    map_command_id: &crate::CommandId,
    first_ordinal: u64,
    count: u64,
) -> Result<()> {
    let mut tasks = Vec::new();
    {
        let Some(map) = state.activity_maps.get(map_command_id) else {
            return Ok(());
        };
        for item_ordinal in first_ordinal..first_ordinal.saturating_add(count) {
            let input = activity_map_input_at(&map.input_manifest, item_ordinal)?;
            let activity_id = ActivityId::map_item(map_command_id, item_ordinal);
            tasks.push((
                activity_id.clone(),
                None,
                ActivityTask {
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
            ));
        }
    }

    for (activity_id, timeout_at, task) in tasks {
        let task = normalize_activity_task_for_storage(state, config, task)?;
        // `or_insert`, never `insert`. This provider has no transaction, so a
        // materialization batch that fails part-way leaves its prefix applied
        // and a later event re-drives the same range; overwriting would reset
        // `completed` and the claim of an item that is already in flight. That
        // is what makes the engine's "applying a prefix and crashing is safe"
        // claim true here and not merely true of the SQL providers.
        state
            .activities
            .entry(activity_id)
            .or_insert(ActivityRecord {
                task,
                claim: None,
                completed: false,
                timeout_at,
                heartbeat_deadline_at: None,
                implicit_heartbeat_ms: None,
                visible_at: None,
            });
    }
    Ok(())
}

/// The activity-map descriptor as [`crate::map_engine`] sees it. `None` when
/// the descriptor is gone, which means the run's terminal cleanup deleted it.
fn activity_map_state(state: &MemoryState, map_command_id: &crate::CommandId) -> Option<MapState> {
    let map = state.activity_maps.get(map_command_id)?;
    Some(MapState {
        map_command_id: map_command_id.clone(),
        kind: MapKind::Activity,
        // An activity map is always fail-fast; the field is inert for it.
        failure_mode: ChildWorkflowMapFailureMode::FailFast,
        item_count: map.input_manifest.item_count as u64,
        next_ordinal: map.next_ordinal,
        in_flight: map.in_flight as u64,
        max_in_flight: map.task.max_in_flight,
        recorded_outcomes: map.results.len() as u64,
        completed: map.completed,
    })
}

/// The child-workflow-map descriptor as [`crate::map_engine`] sees it.
fn child_workflow_map_state(
    state: &MemoryState,
    map_command_id: &crate::CommandId,
) -> Option<MapState> {
    let map = state.child_workflow_maps.get(map_command_id)?;
    Some(MapState {
        map_command_id: map_command_id.clone(),
        kind: MapKind::ChildWorkflow,
        failure_mode: map.task.failure_mode,
        item_count: map.input_manifest.item_count as u64,
        next_ordinal: map.next_ordinal,
        in_flight: map.in_flight as u64,
        max_in_flight: map.task.max_in_flight,
        recorded_outcomes: map.outcomes.len() as u64,
        completed: map.completed,
    })
}

/// A rejected transition never reaches the storage layer: the caller returns
/// this error, and because every map path validates before it mutates, the
/// descriptor and the parent history are untouched.
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

/// Run one map transition: project the descriptor, ask the engine, apply the
/// effects it returns. Returns the parent event the transition appended, if
/// any. Every map path in this provider goes through here.
fn step_map(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    map_state: MapState,
    event: MapEvent,
) -> Result<Option<EventId>> {
    let kind = map_state.kind;
    let map_command_id = map_state.map_command_id.clone();
    let effects = crate::map_engine::step(&map_state, event)
        .map_err(|reject| map_reject_error(kind, reject))?;
    apply_map_effects(state, config, &map_command_id, kind, effects)
}

/// Apply an engine effect list in order. Each arm is one storage primitive;
/// no arm decides anything the engine already decided.
fn apply_map_effects(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    map_command_id: &crate::CommandId,
    kind: MapKind,
    effects: Vec<MapEffect>,
) -> Result<Option<EventId>> {
    let mut appended = None;
    for effect in effects {
        match effect {
            MapEffect::RecordItemOutcome { ordinal, outcome } => {
                if let Some(map) = state.child_workflow_maps.get_mut(map_command_id) {
                    map.outcomes.insert(ordinal, outcome);
                }
            }
            MapEffect::MaterializeItems {
                first_ordinal,
                count,
            } => match kind {
                MapKind::Activity => insert_activity_map_item_batch(
                    state,
                    config,
                    map_command_id,
                    first_ordinal,
                    count,
                )?,
                MapKind::ChildWorkflow => insert_child_workflow_map_item_batch(
                    state,
                    config,
                    map_command_id,
                    first_ordinal,
                    count,
                )?,
            },
            MapEffect::AdvanceDescriptor {
                next_ordinal,
                in_flight,
            } => {
                let in_flight = usize::try_from(in_flight).unwrap_or(usize::MAX);
                match kind {
                    MapKind::Activity => {
                        if let Some(map) = state.activity_maps.get_mut(map_command_id) {
                            map.next_ordinal = next_ordinal;
                            map.in_flight = in_flight;
                        }
                    }
                    MapKind::ChildWorkflow => {
                        if let Some(map) = state.child_workflow_maps.get_mut(map_command_id) {
                            map.next_ordinal = next_ordinal;
                            map.in_flight = in_flight;
                        }
                    }
                }
            }
            MapEffect::ScheduleItemRetry {
                ordinal,
                next_attempt,
                visible_at_ms,
                timeout_at_ms: _,
            } => {
                let activity_id = ActivityId::map_item(map_command_id, ordinal);
                if let Some(record) = state.activities.get_mut(&activity_id) {
                    record.task.attempt = next_attempt;
                    record.claim = None;
                    record.visible_at = visible_at_ms.map(TimestampMs);
                    // The engine's `timeout_at_ms` has no consumer here: the
                    // start-to-close clock starts at the next claim.
                    record.timeout_at = None;
                    record.heartbeat_deadline_at = None;
                    record.implicit_heartbeat_ms = None;
                }
            }
            MapEffect::CompleteMap { item_count } => {
                appended = Some(complete_map(
                    state,
                    config,
                    map_command_id,
                    kind,
                    item_count,
                )?);
            }
            MapEffect::FailMap { failure } => {
                appended = Some(fail_map(state, config, map_command_id, kind, failure)?);
            }
            MapEffect::AbandonPendingItems => {
                abandon_pending_map_items(state, map_command_id, kind);
            }
            MapEffect::CancelChildren { reason } => {
                cancel_child_workflow_map_children(state, map_command_id, &reason);
            }
            MapEffect::MarkDescriptorTerminal => match kind {
                MapKind::Activity => {
                    if let Some(map) = state.activity_maps.get_mut(map_command_id) {
                        map.completed = true;
                        map.in_flight = 0;
                    }
                }
                MapKind::ChildWorkflow => {
                    if let Some(map) = state.child_workflow_maps.get_mut(map_command_id) {
                        map.completed = true;
                        map.in_flight = 0;
                    }
                }
            },
        }
    }
    Ok(appended)
}

/// [`MapEffect::AbandonPendingItems`]: tombstone every not-yet-terminal item
/// task and every undispatched item start of a map that is over, so neither
/// the claim path nor the timeout scanner can resurrect one. The claim-time
/// guard that skips items of a completed map stays as well: a claim already in
/// flight when the map ended must not change the answer.
fn abandon_pending_map_items(
    state: &mut MemoryState,
    map_command_id: &crate::CommandId,
    kind: MapKind,
) {
    match kind {
        MapKind::Activity => {
            for record in state.activities.values_mut() {
                let is_item = record
                    .task
                    .map_item
                    .as_ref()
                    .is_some_and(|item| item.map_command_id == *map_command_id);
                if is_item && !record.completed {
                    record.completed = true;
                    record.claim = None;
                    record.heartbeat_deadline_at = None;
                    record.implicit_heartbeat_ms = None;
                }
            }
        }
        MapKind::ChildWorkflow => {
            for record in state.child_outbox.values_mut() {
                let is_item = record
                    .message
                    .child_map_item
                    .as_ref()
                    .is_some_and(|item| item.map_command_id == *map_command_id);
                if is_item && record.child_run_id.is_none() {
                    record.dispatched = true;
                }
            }
        }
    }
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

// Deletes the terminal run's operational records; see `TerminalCleanup` for
// the contract (history stays authoritative, missing activity records answer
// late calls as `AlreadyCompleted`, signal records survive continue-as-new).
// Undispatched child outbox records stay: an abandoned child may still start
// after its parent closes.
fn cleanup_run_operational_state(
    state: &mut MemoryState,
    run_id: &RunId,
    cleanup: TerminalCleanup,
) {
    state.waits.retain(|_, wait| &wait.run_id != run_id);
    state
        .activities
        .retain(|_, record| &record.task.run_id != run_id);
    state
        .activity_maps
        .retain(|_, map| &map.task.map_command_id.run_id != run_id);
    state
        .child_workflow_maps
        .retain(|_, map| &map.task.map_command_id.run_id != run_id);
    state
        .child_outbox
        .retain(|_, record| !(record.dispatched && &record.message.command_id.run_id == run_id));
    if cleanup.deletes_consumed_signals() {
        // Unconsumed deliveries stay readable through the inbox after the run
        // closes; only the consumed dedup records go.
        state
            .signals
            .retain(|_, signal| !(signal.consumed && &signal.run_id == run_id));
    }
}

/// Starts every undispatched child of the namespace's runs, at most `limit`
/// of them, and returns how many it started. A commit calls this before it
/// returns, so the outbox only carries what a later drain may still need.
fn dispatch_pending_child_starts(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    namespace: &Namespace,
    limit: usize,
) -> Result<usize> {
    let limit = limit.max(1);
    let mut dispatched = 0usize;
    while dispatched < limit {
        let pending = state
            .child_outbox
            .iter()
            .filter(|(_, record)| {
                !record.dispatched
                    && state
                        .runs
                        .get(&record.message.command_id.run_id)
                        .is_some_and(|run| run.namespace == *namespace)
            })
            .map(|(outbox_id, _)| outbox_id.clone())
            .take(limit - dispatched)
            .collect::<Vec<_>>();
        if pending.is_empty() {
            break;
        }
        for outbox_id in pending {
            dispatch_child_start(state, config, &outbox_id)?;
            dispatched += 1;
        }
    }
    Ok(dispatched)
}

fn dispatch_child_start(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    outbox_id: &str,
) -> Result<()> {
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
                .is_some_and(|parent| {
                    parent.command_id == message.command_id
                        && parent.child_map_item == message.child_map_item
                });
            if !same_child {
                let failure = crate::DurableFailure::non_retryable(
                    "durust.child_workflow_id_conflict",
                    format!("child workflow id already exists: {}", message.workflow_id),
                );
                if let Some(map_item) = message.child_map_item.clone() {
                    complete_child_workflow_map_item(
                        state,
                        config,
                        map_item,
                        ChildWorkflowMapItemOutcome::Failed { failure },
                    )?;
                } else {
                    append_child_start_failed(state, &message.command_id, failure)?;
                }
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
            child_event_seqs: BTreeSet::new(),
            child_terminal_seqs: BTreeSet::new(),
            ready: Some(WorkflowTaskReason::WorkflowStarted),
            ready_at: None,
            workflow_claim: None,
            terminal: false,
            parent: Some(ChildParentLink {
                parent_run_id: message.command_id.run_id.clone(),
                command_id: message.command_id.clone(),
                parent_close_policy: message.parent_close_policy,
                child_map_item: message.child_map_item.clone(),
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
    if message.child_map_item.is_some() {
        return Ok(());
    }
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
    parent.push_history(HistoryEvent {
        event_id,
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
    parent.push_history(HistoryEvent {
        event_id,
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
    state
        .runs
        .get(&command_id.run_id)
        .is_some_and(|run| run.child_event_seqs.contains(&command_id.seq.0))
}

/// Both halves of closing a run: route its terminal outcome to its parent, and
/// close out its own children.
///
/// The two are independent obligations and **both always run**. Short-circuiting
/// on the routing error with `?` looked natural and was wrong: this provider has
/// no transaction, so the run is already terminal by the time either half is
/// reached, and abandoning the second half left the closed run's children still
/// executing with nothing waiting for them — work that previously always
/// happened, because the routing result used to be discarded entirely.
///
/// The routing error is reported after the cancellation, never instead of it.
/// Every error still reachable here is an invariant violation rather than a
/// race — a missing descriptor is answered as already-handled by
/// [`complete_child_workflow_map_item`] — so it must stay loud.
fn handle_terminal_run(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    let routed = notify_parent_of_child_terminal(state, config, run_id, terminal_event);
    cancel_children_for_parent(state, run_id);
    routed
}

fn continue_run_as_new(state: &mut MemoryState, old_run_id: &RunId, event: HistoryEventData) {
    let HistoryEventData::WorkflowContinuedAsNew { input } = event else {
        return;
    };
    let Some(old_run) = state.runs.get(old_run_id) else {
        return;
    };
    let namespace = old_run.namespace.clone();
    let workflow_id = old_run.workflow_id.clone();
    let workflow_type = old_run.workflow_type.clone();
    let task_queue = old_run.task_queue.clone();
    let parent = old_run.parent.clone();
    state.next_run_id += 1;
    let new_run_id = RunId::new(format!("run-{}", state.next_run_id));
    let start = HistoryEvent {
        event_id: EventId(1),
        data: HistoryEventData::WorkflowStarted {
            workflow_type: workflow_type.clone(),
            input,
        },
    };
    state
        .workflow_ids
        .insert((namespace.clone(), workflow_id.clone()), new_run_id.clone());
    state.runs.insert(
        new_run_id,
        RunRecord {
            namespace,
            workflow_id,
            workflow_type,
            task_queue,
            history: vec![start],
            child_event_seqs: BTreeSet::new(),
            child_terminal_seqs: BTreeSet::new(),
            ready: Some(WorkflowTaskReason::WorkflowStarted),
            ready_at: None,
            workflow_claim: None,
            terminal: false,
            parent,
        },
    );
}

/// Route a child's terminal event to its parent. Errors propagate: this
/// provider used to discard the map branch's result with `let _ =`, which was
/// the only reason a routing failure here did not wedge the child the way it
/// did on the SQL providers. Discarding it also hid the failure, so the
/// missing-descriptor case is answered explicitly in
/// [`complete_child_workflow_map_item`] and everything else is reported.
fn notify_parent_of_child_terminal(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    child_run_id: &RunId,
    terminal_event: &HistoryEventData,
) -> Result<()> {
    let Some(parent) = state
        .runs
        .get(child_run_id)
        .and_then(|run| run.parent.clone())
    else {
        return Ok(());
    };
    if let Some(map_item) = parent.child_map_item.clone() {
        let Some(outcome) = child_terminal_map_item_outcome(terminal_event) else {
            return Ok(());
        };
        return complete_child_workflow_map_item(state, config, map_item, outcome);
    }
    if child_terminal_event_exists(state, &parent.command_id) {
        return Ok(());
    }
    let Some(parent_run) = state.runs.get_mut(&parent.parent_run_id) else {
        return Ok(());
    };
    if parent_run.terminal {
        return Ok(());
    }
    let event_id = parent_run
        .history
        .last()
        .map(|event| event.event_id.next())
        .unwrap_or(EventId(1));
    let Some((data, reason)) =
        child_terminal_event_data_and_reason(parent.command_id, terminal_event)
    else {
        return Ok(());
    };
    parent_run.push_history(HistoryEvent { event_id, data });
    parent_run.ready = Some(reason);
    parent_run.ready_at = None;
    Ok(())
}

fn child_terminal_event_exists(state: &MemoryState, command_id: &crate::CommandId) -> bool {
    state
        .runs
        .get(&command_id.run_id)
        .is_some_and(|run| run.child_terminal_seqs.contains(&command_id.seq.0))
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
        .filter(|&(_run_id, run)| {
            run.parent.as_ref().is_some_and(|parent| {
                parent.parent_run_id == *parent_run_id
                    && parent.parent_close_policy == ParentClosePolicy::Cancel
                    && !run.terminal
            })
        })
        .map(|(run_id, _run)| run_id.clone())
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
            child.push_history(HistoryEvent {
                event_id,
                data: terminal_event.clone(),
            });
            child.terminal = true;
            child.ready = None;
            child.ready_at = None;
            child.workflow_claim = None;
        }
        cleanup_run_operational_state(state, &child_run_id, TerminalCleanup::Closed);
    }
}

fn cancel_command_operational_state(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    command_id: &crate::CommandId,
) -> Result<()> {
    for record in state.activities.values_mut() {
        if record.task.command_id == *command_id && record.task.map_item.is_none() {
            record.completed = true;
            record.claim = None;
            record.heartbeat_deadline_at = None;
            record.implicit_heartbeat_ms = None;
        }
    }
    // A cancelled map is a terminal map: the engine's `ParentCancelled`
    // transition tombstones its pending work and closes the descriptor, so
    // this path cannot drift from the fail-fast one.
    if let Some(map_state) = activity_map_state(state, command_id) {
        step_map(state, config, map_state, MapEvent::ParentCancelled)?;
    }
    if let Some(map_state) = child_workflow_map_state(state, command_id) {
        if !map_state.completed {
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
            cancel_child_workflow_map_children(
                state,
                command_id,
                &map_command_cancelled_reason(command_id),
            );
        }
        step_map(state, config, map_state, MapEvent::ParentCancelled)?;
    }
    for record in state.child_outbox.values_mut() {
        if record.message.command_id == *command_id && record.message.child_map_item.is_none() {
            record.dispatched = true;
        }
    }
    if let Some(record) = state
        .child_outbox
        .get_mut(&child_outbox_id_for_command(command_id))
    {
        record.dispatched = true;
    }
    Ok(())
}

fn child_outbox_id(message: &ChildStartOutboxMessage) -> String {
    if let Some(item) = &message.child_map_item {
        return format!(
            "{}:{}:child-map:{}",
            item.map_command_id.run_id, item.map_command_id.seq.0, item.item_ordinal
        );
    }
    child_outbox_id_for_command(&message.command_id)
}

fn child_outbox_id_for_command(command_id: &crate::CommandId) -> String {
    format!("{}:{}:child-start", command_id.run_id, command_id.seq.0)
}

/// Storage primitive behind [`MapEffect::MaterializeItems`] for a child map:
/// enqueue one child-start outbox entry per ordinal in
/// `[first_ordinal, first_ordinal + count)`.
fn insert_child_workflow_map_item_batch(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    map_command_id: &crate::CommandId,
    first_ordinal: u64,
    count: u64,
) -> Result<()> {
    for item_ordinal in first_ordinal..first_ordinal.saturating_add(count) {
        let Some((task, input)) = ({
            let Some(map) = state.child_workflow_maps.get(map_command_id) else {
                return Ok(());
            };
            let input = activity_map_input_at(&map.input_manifest, item_ordinal)?;
            Some((map.task.clone(), input))
        }) else {
            return Ok(());
        };
        let message = ChildStartOutboxMessage {
            command_id: map_command_id.clone(),
            workflow_type: task.workflow_type.clone(),
            workflow_id: crate::WorkflowId::new(format!(
                "{}/{}",
                task.workflow_id_prefix, item_ordinal
            )),
            task_queue: task.task_queue.clone(),
            input,
            parent_close_policy: task.parent_close_policy,
            child_map_item: Some(ChildWorkflowMapItem {
                map_command_id: map_command_id.clone(),
                item_ordinal,
            }),
        };
        let message = crate::payload::map_child_start_payloads(message, &mut |payload| {
            normalize_payload_for_storage(state, config, payload)
        })?;
        state
            .child_outbox
            .entry(child_outbox_id(&message))
            .or_insert(ChildOutboxRecord {
                message,
                dispatched: false,
                child_run_id: None,
            });
    }
    Ok(())
}

fn complete_child_workflow_map_item(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    map_item: ChildWorkflowMapItem,
    outcome: ChildWorkflowMapItemOutcome,
) -> Result<()> {
    let Some(map_state) = child_workflow_map_state(state, &map_item.map_command_id) else {
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
    let already_recorded = state
        .child_workflow_maps
        .get(&map_item.map_command_id)
        .is_some_and(|map| map.outcomes.contains_key(&map_item.item_ordinal));
    let parent_terminal = parent_run_terminal(state, &map_item.map_command_id.run_id)?;
    step_map(
        state,
        config,
        map_state,
        MapEvent::ItemCompleted {
            ordinal: map_item.item_ordinal,
            outcome,
            already_recorded,
            parent_terminal,
        },
    )?;
    Ok(())
}

/// Whether the map's parent run is already closed. A missing run is an error
/// rather than "closed": the descriptor exists, so the run should too.
fn parent_run_terminal(state: &MemoryState, run_id: &RunId) -> Result<bool> {
    state
        .runs
        .get(run_id)
        .map(|run| run.terminal)
        .ok_or_else(|| Error::RunNotFound(run_id.clone()))
}

/// [`MapEffect::CompleteMap`]: assemble the result manifest in ascending
/// ordinal order over the input manifest's page boundaries, append the
/// terminal success fact to the parent, and wake it.
fn complete_map(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    map_command_id: &crate::CommandId,
    kind: MapKind,
    item_count: u64,
) -> Result<EventId> {
    let item_count = usize::try_from(item_count).unwrap_or(usize::MAX);
    let (result_manifest, counts) = match kind {
        MapKind::Activity => {
            let Some(map) = state.activity_maps.get(map_command_id) else {
                return Err(Error::Backend(format!(
                    "activity map `{}`:{} not found",
                    map_command_id.run_id, map_command_id.seq.0
                )));
            };
            let results = (0..item_count)
                .map(|ordinal| {
                    map.results
                        .get(&(ordinal as u64))
                        .cloned()
                        .ok_or_else(|| Error::Backend(format!("missing result for item {ordinal}")))
                })
                .collect::<Result<Vec<_>>>()?;
            let manifest = encode_activity_map_result_manifest_with_codec(
                map.task.result_manifest_name.clone(),
                results,
                &map.input_manifest.page_lengths,
                config.codec,
            )?;
            let manifest =
                normalize_activity_map_result_manifest_for_storage(state, config, manifest)?;
            (manifest, activity_outcome_counts(item_count))
        }
        MapKind::ChildWorkflow => {
            let Some(map) = state.child_workflow_maps.get(map_command_id) else {
                return Err(Error::Backend(format!(
                    "child workflow map `{}`:{} not found",
                    map_command_id.run_id, map_command_id.seq.0
                )));
            };
            let outcomes = (0..item_count)
                .map(|ordinal| {
                    map.outcomes.get(&(ordinal as u64)).cloned().ok_or_else(|| {
                        Error::Backend(format!(
                            "missing child workflow map outcome for item {ordinal}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let counts = outcome_counts(&outcomes);
            let manifest = encode_child_workflow_map_result_manifest_with_codec(
                map.task.result_manifest_name.clone(),
                outcomes,
                &map.input_manifest.page_lengths,
                config.codec,
            )?;
            let manifest =
                normalize_child_workflow_map_result_manifest_for_storage(state, config, manifest)?;
            (manifest, counts)
        }
    };

    let (data, reason) = match kind {
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
    append_map_terminal_event(state, &map_command_id.run_id, data, reason)
}

/// [`MapEffect::FailMap`]: append the terminal failure fact to the parent and
/// wake it.
fn fail_map(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    map_command_id: &crate::CommandId,
    kind: MapKind,
    failure: crate::DurableFailure,
) -> Result<EventId> {
    let failure = normalize_failure_for_storage(state, config, failure)?;
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
    append_map_terminal_event(state, &map_command_id.run_id, data, reason)
}

/// Append a terminal map fact to the parent and mark it ready. The engine has
/// already rejected the closed-parent case, so reaching here with a terminal
/// run would be an engine/provider disagreement, not a routine race.
fn append_map_terminal_event(
    state: &mut MemoryState,
    run_id: &RunId,
    data: HistoryEventData,
    reason: WorkflowTaskReason,
) -> Result<EventId> {
    let Some(parent) = state.runs.get_mut(run_id) else {
        return Err(Error::RunNotFound(run_id.clone()));
    };
    if parent.terminal {
        return Err(Error::TerminalWorkflow);
    }
    let event_id = parent
        .history
        .last()
        .map(|event| event.event_id.next())
        .unwrap_or(EventId(1));
    parent.push_history(HistoryEvent { event_id, data });
    parent.ready = Some(reason);
    parent.ready_at = None;
    Ok(event_id)
}

/// [`MapEffect::CancelChildren`]: cancel every already-running, not-yet-
/// terminal child of this map with the engine's reason. Undispatched starts
/// are tombstoned by [`MapEffect::AbandonPendingItems`], which the engine
/// always emits first.
fn cancel_child_workflow_map_children(
    state: &mut MemoryState,
    map_command_id: &crate::CommandId,
    reason: &str,
) {
    let children = state
        .runs
        .iter()
        .filter_map(|(run_id, run)| {
            run.parent
                .as_ref()
                .and_then(|parent| parent.child_map_item.as_ref())
                .is_some_and(|item| item.map_command_id == *map_command_id)
                .then_some((run_id.clone(), run.terminal))
        })
        .filter_map(|(run_id, terminal)| (!terminal).then_some(run_id))
        .collect::<Vec<_>>();
    for child_run_id in children {
        if let Some(child) = state.runs.get_mut(&child_run_id) {
            let event_id = child
                .history
                .last()
                .map(|event| event.event_id.next())
                .unwrap_or(EventId(1));
            child.push_history(HistoryEvent {
                event_id,
                data: HistoryEventData::WorkflowCancelled {
                    reason: reason.to_owned(),
                },
            });
            child.terminal = true;
            child.ready = None;
            child.ready_at = None;
            child.workflow_claim = None;
        }
        cleanup_run_operational_state(state, &child_run_id, TerminalCleanup::Closed);
    }
}

fn complete_map_item(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    task: ActivityTask,
    map_item: ActivityMapItem,
    result: crate::PayloadRef,
) -> Result<CompleteActivityOutcome> {
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
    let Some(map_state) = activity_map_state(state, &map_item.map_command_id) else {
        return Err(Error::Backend(format!(
            "activity map `{}`:{} not found",
            map_item.map_command_id.run_id, map_item.map_command_id.seq.0
        )));
    };
    if map_state.completed {
        return Ok(CompleteActivityOutcome::AlreadyCompleted);
    }
    if !map_state.ordinal_in_bounds(map_item.item_ordinal) {
        return Err(map_reject_error(
            MapKind::Activity,
            MapReject::OutOfBounds {
                ordinal: map_item.item_ordinal,
            },
        ));
    }
    let already_recorded = state
        .activity_maps
        .get(&map_item.map_command_id)
        .is_some_and(|map| map.results.contains_key(&map_item.item_ordinal));
    let parent_terminal = parent_run_terminal(state, &task.run_id)?;

    // Ask before writing. An activity map's result row is its own storage
    // primitive rather than an effect, so it must not be written for a
    // transition the engine rejects — this provider has no transaction to roll
    // the write back.
    let effects = crate::map_engine::step(
        &map_state,
        MapEvent::ItemCompleted {
            ordinal: map_item.item_ordinal,
            // The result payload rides the result table, not the outcome; the
            // engine only needs to know this was a success.
            outcome: ChildWorkflowMapItemOutcome::Succeeded {
                result: result.clone(),
            },
            already_recorded,
            parent_terminal,
        },
    )
    .map_err(|reject| map_reject_error(MapKind::Activity, reject))?;
    if !already_recorded && let Some(map) = state.activity_maps.get_mut(&map_item.map_command_id) {
        map.results.insert(map_item.item_ordinal, result);
    }
    let appended = apply_map_effects(
        state,
        config,
        &map_item.map_command_id,
        MapKind::Activity,
        effects,
    )?;

    let event_id = appended.unwrap_or_else(|| {
        state
            .runs
            .get(&task.run_id)
            .and_then(|run| run.history.last().map(|event| event.event_id))
            .unwrap_or(EventId::ZERO)
    });
    Ok(CompleteActivityOutcome::Completed { event_id })
}

/// One activity-map item attempt ended. `decision` is the shared activity
/// retry verdict; the engine turns it into either a rescheduled attempt or the
/// map's terminal failure.
#[allow(clippy::too_many_arguments)]
fn fail_map_item(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    task: ActivityTask,
    map_item: ActivityMapItem,
    failure: crate::DurableFailure,
    kind: ItemAttemptFailureKind,
    decision: ItemRetryDecision,
    now: TimestampMs,
) -> Result<FailActivityOutcome> {
    let Some(map_state) = activity_map_state(state, &map_item.map_command_id) else {
        return Ok(FailActivityOutcome::AlreadyCompleted);
    };
    if map_state.completed {
        return Ok(FailActivityOutcome::AlreadyCompleted);
    }
    let already_recorded = state
        .activity_maps
        .get(&map_item.map_command_id)
        .is_some_and(|map| map.results.contains_key(&map_item.item_ordinal));
    let parent_terminal = parent_run_terminal(state, &task.run_id)?;
    let appended = step_map(
        state,
        config,
        map_state,
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
    )?;
    match (decision, appended) {
        // A timed-out attempt retries at once; a failed one waits out the
        // policy's backoff, as `MapEffect::ScheduleItemRetry` schedules them.
        (ItemRetryDecision::Retry { next_attempt }, _) => Ok(FailActivityOutcome::RetryScheduled {
            next_attempt,
            ready_at: if kind == ItemAttemptFailureKind::TimedOut {
                now
            } else {
                retry_visible_at_ms(&task.retry_policy, task.attempt, now)
                    .map(TimestampMs)
                    .unwrap_or(now)
            },
        }),
        (ItemRetryDecision::Exhausted, Some(event_id)) => {
            Ok(FailActivityOutcome::Failed { event_id })
        }
        (ItemRetryDecision::Exhausted, None) => Ok(FailActivityOutcome::AlreadyCompleted),
    }
}

fn timeout_activity(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
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
        let heartbeat_due = record
            .heartbeat_deadline_at
            .is_some_and(|deadline| deadline <= now);
        let start_to_close_due = record
            .timeout_at
            .is_some_and(|timeout_at| timeout_at <= now);
        let attribution = activity_timeout_attribution(
            start_to_close_due,
            heartbeat_due,
            record.implicit_heartbeat_ms.is_some(),
        );

        let task = record.task.clone();
        let decision = activity_timeout_decision(&task);
        // A map item's lapsed deadline is an engine event, not a local
        // reschedule: the engine owns both the retry and the map's terminal
        // failure, and it is the only thing that knows whether the map is over.
        if task.map_item.is_none() {
            if let ActivityFailureDecision::Retry { next_attempt } = decision {
                // The stored policy paces a timed-out attempt's retry the way
                // it paces a failed one, and the start-to-close clock restarts
                // when the retry becomes visible.
                let visible_at = retry_visible_at_ms(&task.retry_policy, task.attempt, now);
                record.task.attempt = next_attempt;
                record.claim = None;
                record.visible_at = visible_at.map(TimestampMs);
                record.timeout_at = None;
                record.heartbeat_deadline_at = None;
                record.implicit_heartbeat_ms = None;
                return Ok(true);
            }
            record.completed = true;
        }
        (task, attribution, decision)
    };

    let (timed_out_task, attribution, decision) = timed_out_task;
    if let Some(map_item) = timed_out_task.map_item.clone() {
        let outcome = fail_map_item(
            state,
            config,
            timed_out_task.clone(),
            map_item,
            crate::DurableFailure::new(
                "durust.activity_timed_out",
                timeout_message(activity_id, timed_out_task.attempt, attribution),
            ),
            ItemAttemptFailureKind::TimedOut,
            ItemRetryDecision::from(decision),
            now,
        )?;
        if !matches!(outcome, FailActivityOutcome::RetryScheduled { .. })
            && let Some(record) = state.activities.get_mut(activity_id)
        {
            record.completed = true;
        }
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
    run.push_history(HistoryEvent {
        event_id,
        data: HistoryEventData::ActivityTimedOut(crate::ActivityTimedOut {
            command_id: timed_out_task.command_id,
            message: timeout_message(activity_id, timed_out_task.attempt, attribution),
        }),
    });
    run.ready = Some(WorkflowTaskReason::ActivityTimedOut);
    run.ready_at = None;
    Ok(true)
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

fn normalize_history_events_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    events: Vec<crate::NewHistoryEvent>,
) -> Result<Vec<crate::NewHistoryEvent>> {
    events
        .into_iter()
        .map(|event| {
            Ok(crate::NewHistoryEvent {
                data: normalize_history_event_for_storage(state, config, event.data)?,
            })
        })
        .collect()
}

fn collect_reachable_payload_blobs(
    state: &MemoryState,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    let mut load = |payload: &PayloadRef| -> Result<Option<PayloadRef>> {
        if is_external_payload_ref(payload) {
            Ok(None)
        } else {
            hydrate_payload_from_storage(state, payload.clone()).map(Some)
        }
    };
    let mut mark = |payload: &PayloadRef| collect_payload_blob_ref(state, payload, reachable);
    for run in state.runs.values() {
        for event in &run.history {
            crate::payload::history_event_payload_refs(&event.data, &mut load, &mut mark)?;
        }
    }
    for record in state.activities.values() {
        mark(&record.task.input)?;
    }
    for map in state.activity_maps.values() {
        for payload in crate::payload::manifest_refs(
            ManifestKind::ActivityMapInput,
            &map.task.input_manifest,
            &mut load,
        )? {
            mark(&payload)?;
        }
        for payload in crate::payload::input_manifest_page_refs(&map.input_manifest, &mut load)? {
            mark(&payload)?;
        }
        for result in map.results.values() {
            mark(result)?;
        }
    }
    for map in state.child_workflow_maps.values() {
        for payload in crate::payload::manifest_refs(
            ManifestKind::ActivityMapInput,
            &map.task.input_manifest,
            &mut load,
        )? {
            mark(&payload)?;
        }
        for payload in crate::payload::input_manifest_page_refs(&map.input_manifest, &mut load)? {
            mark(&payload)?;
        }
        for outcome in map.outcomes.values() {
            if let Some(payload) = crate::payload::child_workflow_map_outcome_payload(outcome) {
                mark(payload)?;
            }
        }
    }
    for record in state.child_outbox.values() {
        mark(&record.message.input)?;
    }
    for signal in state.signals.values() {
        mark(&signal.payload)?;
    }
    for projection in state.query_projections.values() {
        mark(&projection.payload)?;
    }
    Ok(())
}

fn collect_payload_roots(state: &MemoryState) -> Result<Vec<PayloadRootRef>> {
    let mut roots = Vec::new();
    let mut hydrate_manifest =
        |kind: ManifestKind, payload: &PayloadRef| hydrate_manifest_root(state, kind, payload);
    for run in state.runs.values() {
        for event in &run.history {
            crate::payload::history_event_payload_roots(
                &event.data,
                &mut roots,
                &mut hydrate_manifest,
            )?;
        }
    }
    for record in state.activities.values() {
        roots.push(PayloadRootRef::Payload(record.task.input.clone()));
    }
    for map in state.activity_maps.values() {
        roots.push(PayloadRootRef::ActivityMapInputManifest(hydrate_manifest(
            ManifestKind::ActivityMapInput,
            &map.task.input_manifest,
        )?));
        for result in map.results.values() {
            roots.push(PayloadRootRef::Payload(result.clone()));
        }
    }
    for map in state.child_workflow_maps.values() {
        roots.push(PayloadRootRef::ActivityMapInputManifest(hydrate_manifest(
            ManifestKind::ActivityMapInput,
            &map.task.input_manifest,
        )?));
        for outcome in map.outcomes.values() {
            if let Some(payload) = crate::payload::child_workflow_map_outcome_payload(outcome) {
                roots.push(PayloadRootRef::Payload(payload.clone()));
            }
        }
    }
    for record in state.child_outbox.values() {
        roots.push(PayloadRootRef::Payload(record.message.input.clone()));
    }
    for signal in state.signals.values() {
        roots.push(PayloadRootRef::Payload(signal.payload.clone()));
    }
    for projection in state.query_projections.values() {
        roots.push(PayloadRootRef::Payload(projection.payload.clone()));
    }
    Ok(roots)
}

/// A manifest root with its bytes in place when this provider holds them.
fn hydrate_manifest_root(
    state: &MemoryState,
    kind: ManifestKind,
    payload: &PayloadRef,
) -> Result<PayloadRef> {
    if is_external_payload_ref(payload) {
        return Ok(payload.clone());
    }
    match kind {
        ManifestKind::ActivityMapInput => {
            hydrate_activity_map_input_manifest_from_storage(state, payload.clone())
        }
        ManifestKind::ActivityMapResult => {
            hydrate_activity_map_result_manifest_from_storage(state, payload.clone())
        }
        ManifestKind::ChildWorkflowMapResult => {
            hydrate_child_workflow_map_result_manifest_from_storage(state, payload.clone())
        }
    }
}

fn collect_payload_blob_ref(
    state: &MemoryState,
    payload: &PayloadRef,
    reachable: &mut BTreeSet<String>,
) -> Result<()> {
    if let PayloadRef::Blob { digest, uri, .. } = payload {
        if is_memory_payload_uri(uri) {
            verify_payload_blob(state, payload, false)?;
        }
        reachable.insert(digest.clone());
    }
    Ok(())
}

fn normalize_history_event_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    data: HistoryEventData,
) -> Result<HistoryEventData> {
    match data {
        HistoryEventData::ActivityMapScheduled(mut scheduled) => {
            if !is_external_payload_ref(&scheduled.input_manifest) {
                scheduled.input_manifest = normalize_activity_map_input_manifest_for_storage(
                    state,
                    config,
                    scheduled.input_manifest,
                )?;
            }
            Ok(HistoryEventData::ActivityMapScheduled(scheduled))
        }
        HistoryEventData::ActivityMapCompleted(mut completed) => {
            if !is_external_payload_ref(&completed.result_manifest) {
                completed.result_manifest = normalize_activity_map_result_manifest_for_storage(
                    state,
                    config,
                    completed.result_manifest,
                )?;
            }
            Ok(HistoryEventData::ActivityMapCompleted(completed))
        }
        HistoryEventData::ChildWorkflowMapScheduled(mut scheduled) => {
            if !is_external_payload_ref(&scheduled.input_manifest) {
                scheduled.input_manifest = normalize_activity_map_input_manifest_for_storage(
                    state,
                    config,
                    scheduled.input_manifest,
                )?;
            }
            Ok(HistoryEventData::ChildWorkflowMapScheduled(scheduled))
        }
        HistoryEventData::ChildWorkflowMapCompleted(mut completed) => {
            if !is_external_payload_ref(&completed.result_manifest) {
                completed.result_manifest =
                    normalize_child_workflow_map_result_manifest_for_storage(
                        state,
                        config,
                        completed.result_manifest,
                    )?;
            }
            Ok(HistoryEventData::ChildWorkflowMapCompleted(completed))
        }
        data => crate::payload::map_history_event_payloads(data, &mut |payload| {
            normalize_payload_for_storage(state, config, payload)
        }),
    }
}

fn hydrate_history_event_from_storage(
    state: &MemoryState,
    data: HistoryEventData,
) -> Result<HistoryEventData> {
    match data {
        HistoryEventData::ActivityMapScheduled(mut scheduled) => {
            if !is_external_payload_ref(&scheduled.input_manifest) {
                scheduled.input_manifest = hydrate_activity_map_input_manifest_from_storage(
                    state,
                    scheduled.input_manifest,
                )?;
            }
            Ok(HistoryEventData::ActivityMapScheduled(scheduled))
        }
        HistoryEventData::ActivityMapCompleted(mut completed) => {
            if !is_external_payload_ref(&completed.result_manifest) {
                completed.result_manifest = hydrate_activity_map_result_manifest_from_storage(
                    state,
                    completed.result_manifest,
                )?;
            }
            Ok(HistoryEventData::ActivityMapCompleted(completed))
        }
        HistoryEventData::ChildWorkflowMapScheduled(mut scheduled) => {
            if !is_external_payload_ref(&scheduled.input_manifest) {
                scheduled.input_manifest = hydrate_activity_map_input_manifest_from_storage(
                    state,
                    scheduled.input_manifest,
                )?;
            }
            Ok(HistoryEventData::ChildWorkflowMapScheduled(scheduled))
        }
        HistoryEventData::ChildWorkflowMapCompleted(mut completed) => {
            if !is_external_payload_ref(&completed.result_manifest) {
                completed.result_manifest =
                    hydrate_child_workflow_map_result_manifest_from_storage(
                        state,
                        completed.result_manifest,
                    )?;
            }
            Ok(HistoryEventData::ChildWorkflowMapCompleted(completed))
        }
        data => crate::payload::map_history_event_payloads(data, &mut |payload| {
            hydrate_payload_from_storage(state, payload)
        }),
    }
}

fn normalize_activity_tasks_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    tasks: Vec<ActivityTask>,
) -> Result<Vec<ActivityTask>> {
    tasks
        .into_iter()
        .map(|task| normalize_activity_task_for_storage(state, config, task))
        .collect()
}

fn normalize_activity_task_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    task: ActivityTask,
) -> Result<ActivityTask> {
    crate::payload::map_activity_task_payloads(task, &mut |payload| {
        normalize_payload_for_storage(state, config, payload)
    })
}

fn hydrate_activity_task_from_storage(
    state: &MemoryState,
    task: ActivityTask,
) -> Result<ActivityTask> {
    crate::payload::map_activity_task_payloads(task, &mut |payload| {
        hydrate_payload_from_storage(state, payload)
    })
}

fn normalize_activity_map_task_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    mut task: ActivityMapTask,
) -> Result<ActivityMapTask> {
    task.input_manifest =
        normalize_activity_map_input_manifest_for_storage(state, config, task.input_manifest)?;
    Ok(task)
}

fn normalize_child_workflow_map_task_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    mut task: ChildWorkflowMapTask,
) -> Result<ChildWorkflowMapTask> {
    task.input_manifest =
        normalize_activity_map_input_manifest_for_storage(state, config, task.input_manifest)?;
    Ok(task)
}

fn normalize_child_start_messages_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    messages: Vec<ChildStartOutboxMessage>,
) -> Result<Vec<ChildStartOutboxMessage>> {
    messages
        .into_iter()
        .map(|message| {
            crate::payload::map_child_start_payloads(message, &mut |payload| {
                normalize_payload_for_storage(state, config, payload)
            })
        })
        .collect()
}

fn normalize_failure_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    failure: crate::DurableFailure,
) -> Result<crate::DurableFailure> {
    crate::payload::map_failure_payloads(failure, &mut |payload| {
        normalize_payload_for_storage(state, config, payload)
    })
}

fn normalize_activity_map_input_manifest_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let root = hydrate_payload_from_storage(state, payload)?;
    let mut manifest: ActivityMapInputManifest = crate::decode_payload(&root)?;
    manifest.pages = manifest
        .pages
        .into_iter()
        .map(|page| {
            // A foreign-scheme page under an inline root is opaque: the
            // owning layer normalized its items before this commit, so it
            // passes through untouched (mirroring the reachability
            // collectors' external-page skip).
            if is_external_payload_ref(&page) {
                return Ok(page);
            }
            let page = hydrate_payload_from_storage(state, page)?;
            let mut page: ActivityMapInputPage = crate::decode_payload(&page)?;
            page.items = page
                .items
                .into_iter()
                .map(|payload| normalize_payload_for_storage(state, config, payload))
                .collect::<Result<Vec<_>>>()?;
            normalize_payload_for_storage(
                state,
                config,
                crate::encode_payload_with_codec(&page, config.codec)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    normalize_payload_for_storage(
        state,
        config,
        crate::encode_payload_with_codec(&manifest, config.codec)?,
    )
}

fn normalize_activity_map_result_manifest_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let root = hydrate_payload_from_storage(state, payload)?;
    let mut manifest: ActivityMapResultManifest = crate::decode_payload(&root)?;
    manifest.pages = manifest
        .pages
        .into_iter()
        .map(|page| {
            let page = hydrate_payload_from_storage(state, page)?;
            let mut page: ActivityMapResultPage = crate::decode_payload(&page)?;
            page.results = page
                .results
                .into_iter()
                .map(|payload| normalize_payload_for_storage(state, config, payload))
                .collect::<Result<Vec<_>>>()?;
            normalize_payload_for_storage(
                state,
                config,
                crate::encode_payload_with_codec(&page, config.codec)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    normalize_payload_for_storage(
        state,
        config,
        crate::encode_payload_with_codec(&manifest, config.codec)?,
    )
}

fn normalize_child_workflow_map_result_manifest_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let root = hydrate_payload_from_storage(state, payload)?;
    let mut manifest: crate::ChildWorkflowMapResultManifest = crate::decode_payload(&root)?;
    manifest.pages = manifest
        .pages
        .into_iter()
        .map(|page| {
            let page = hydrate_payload_from_storage(state, page)?;
            let mut page: crate::ChildWorkflowMapResultPage = crate::decode_payload(&page)?;
            page.outcomes = page
                .outcomes
                .into_iter()
                .map(|outcome| {
                    normalize_child_workflow_map_outcome_for_storage(state, config, outcome)
                })
                .collect::<Result<Vec<_>>>()?;
            normalize_payload_for_storage(
                state,
                config,
                crate::encode_payload_with_codec(&page, config.codec)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    normalize_payload_for_storage(
        state,
        config,
        crate::encode_payload_with_codec(&manifest, config.codec)?,
    )
}

fn normalize_child_workflow_map_outcome_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    outcome: ChildWorkflowMapItemOutcome,
) -> Result<ChildWorkflowMapItemOutcome> {
    match outcome {
        ChildWorkflowMapItemOutcome::Succeeded { result } => {
            Ok(ChildWorkflowMapItemOutcome::Succeeded {
                result: normalize_payload_for_storage(state, config, result)?,
            })
        }
        ChildWorkflowMapItemOutcome::Failed { failure } => {
            Ok(ChildWorkflowMapItemOutcome::Failed {
                failure: normalize_failure_for_storage(state, config, failure)?,
            })
        }
        ChildWorkflowMapItemOutcome::Cancelled { reason } => {
            Ok(ChildWorkflowMapItemOutcome::Cancelled { reason })
        }
    }
}

fn hydrate_activity_map_input_manifest_from_storage(
    state: &MemoryState,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let mut load_container = |payload| hydrate_payload_from_storage(state, payload);
    let mut hydrate_leaf = |payload| hydrate_payload_from_storage(state, payload);
    let mut finish_container = Ok;
    crate::payload::map_activity_map_input_manifest_ref(
        payload,
        &mut load_container,
        &mut hydrate_leaf,
        &mut finish_container,
    )
}

fn hydrate_activity_map_result_manifest_from_storage(
    state: &MemoryState,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let mut load_container = |payload| hydrate_payload_from_storage(state, payload);
    let mut hydrate_leaf = |payload| hydrate_payload_from_storage(state, payload);
    let mut finish_container = Ok;
    crate::payload::map_activity_map_result_manifest_ref(
        payload,
        &mut load_container,
        &mut hydrate_leaf,
        &mut finish_container,
    )
}

fn hydrate_child_workflow_map_result_manifest_from_storage(
    state: &MemoryState,
    payload: PayloadRef,
) -> Result<PayloadRef> {
    let root = hydrate_payload_from_storage(state, payload)?;
    let mut manifest: crate::ChildWorkflowMapResultManifest = crate::decode_payload(&root)?;
    manifest.pages = manifest
        .pages
        .into_iter()
        .map(|page| {
            let page = hydrate_payload_from_storage(state, page)?;
            let mut page: crate::ChildWorkflowMapResultPage = crate::decode_payload(&page)?;
            page.outcomes = page
                .outcomes
                .into_iter()
                .map(|outcome| hydrate_child_workflow_map_outcome_from_storage(state, outcome))
                .collect::<Result<Vec<_>>>()?;
            crate::encode_payload_with_codec(&page, root.codec())
        })
        .collect::<Result<Vec<_>>>()?;
    crate::encode_payload_with_codec(&manifest, root.codec())
}

fn hydrate_child_workflow_map_outcome_from_storage(
    state: &MemoryState,
    outcome: ChildWorkflowMapItemOutcome,
) -> Result<ChildWorkflowMapItemOutcome> {
    match outcome {
        ChildWorkflowMapItemOutcome::Succeeded { result } => {
            Ok(ChildWorkflowMapItemOutcome::Succeeded {
                result: hydrate_payload_from_storage(state, result)?,
            })
        }
        ChildWorkflowMapItemOutcome::Failed { mut failure } => {
            if let Some(details) = failure.details.take() {
                failure.details = Some(hydrate_payload_from_storage(state, details)?);
            }
            Ok(ChildWorkflowMapItemOutcome::Failed { failure })
        }
        ChildWorkflowMapItemOutcome::Cancelled { reason } => {
            Ok(ChildWorkflowMapItemOutcome::Cancelled { reason })
        }
    }
}

fn normalize_payload_for_storage(
    state: &mut MemoryState,
    config: &PayloadStorageConfig,
    payload: PayloadRef,
) -> Result<PayloadRef> {
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
            let now = state.now;
            state
                .payload_blobs
                .entry(digest.clone())
                // A content-addressed reuse keeps the first blob's metadata
                // but restarts the GC grace period for it.
                .and_modify(|record| record.stored_at = now)
                .or_insert_with(|| StoredPayloadBlob {
                    blob: PayloadBlob {
                        codec,
                        schema_fingerprint: schema_fingerprint.clone(),
                        compression,
                        encryption: encryption.clone(),
                        bytes: bytes.clone(),
                    },
                    stored_at: now,
                });
            Ok(PayloadRef::Blob {
                codec,
                schema_fingerprint,
                compression,
                encryption,
                digest: digest.clone(),
                size,
                uri: format!("memory://payload/{digest}"),
            })
        }
        payload @ PayloadRef::Inline { .. } => Ok(payload),
        payload @ PayloadRef::Blob { .. } => {
            // Only refs with this provider's scheme are validated against its
            // store; every other scheme is opaque and persists unchanged.
            if matches!(&payload, PayloadRef::Blob { uri, .. } if is_memory_payload_uri(uri)) {
                verify_payload_blob(state, &payload, true)?;
            }
            Ok(payload)
        }
    }
}

fn hydrate_payload_from_storage(state: &MemoryState, payload: PayloadRef) -> Result<PayloadRef> {
    match payload {
        payload @ PayloadRef::Inline { .. } => Ok(payload),
        payload @ PayloadRef::Blob { .. } => {
            if matches!(&payload, PayloadRef::Blob { uri, .. } if !is_memory_payload_uri(uri)) {
                return Ok(payload);
            }
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
            let blob = verify_payload_blob(state, &payload, false)?;
            Ok(PayloadRef::Inline {
                codec: *codec,
                schema_fingerprint: schema_fingerprint.clone(),
                compression: *compression,
                encryption: encryption.clone(),
                bytes: blob.bytes.clone(),
            })
        }
    }
}

fn is_memory_payload_uri(uri: &str) -> bool {
    uri.starts_with("memory://payload/")
}

// Every blob ref this provider did not mint is opaque: it belongs to whatever
// layer owns its scheme (a `PayloadBackend` blob store), so the provider never
// hydrates, validates, or garbage-collects it.
fn is_external_payload_ref(payload: &PayloadRef) -> bool {
    matches!(payload, PayloadRef::Blob { uri, .. } if !is_memory_payload_uri(uri))
}

fn verify_payload_blob<'a>(
    state: &'a MemoryState,
    payload: &PayloadRef,
    require_schema_fingerprint_match: bool,
) -> Result<&'a PayloadBlob> {
    let PayloadRef::Blob {
        codec,
        schema_fingerprint: _schema_fingerprint,
        compression,
        encryption,
        digest,
        size,
        uri: _,
    } = payload
    else {
        return Err(Error::PayloadDecode(
            "inline payload does not reference blob storage".to_owned(),
        ));
    };
    let Some(StoredPayloadBlob { blob, .. }) = state.payload_blobs.get(digest) else {
        return Err(Error::PayloadDecode(format!(
            "missing payload blob `{digest}`"
        )));
    };
    if blob.codec != *codec
        || (require_schema_fingerprint_match && blob.schema_fingerprint != *_schema_fingerprint)
        || blob.compression != *compression
        || blob.encryption != *encryption
    {
        return Err(Error::PayloadDecode(format!(
            "payload blob metadata mismatch for `{digest}`"
        )));
    }
    let actual_digest = digest_bytes(&blob.bytes);
    if &actual_digest != digest {
        return Err(Error::PayloadDecode(format!(
            "payload blob digest mismatch: expected `{digest}`, got `{actual_digest}`"
        )));
    }
    let actual_size = u64::try_from(blob.bytes.len()).unwrap_or(u64::MAX);
    if actual_size != *size {
        return Err(Error::PayloadDecode(format!(
            "payload blob size mismatch: expected {size}, got {actual_size}"
        )));
    }
    Ok(blob)
}

fn ready_at_for_delay(now: TimestampMs, delay: Duration) -> Option<TimestampMs> {
    if delay.is_zero() {
        None
    } else {
        let delay_ms = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
        Some(TimestampMs(now.0.saturating_add(delay_ms)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider_util::commit_test_support;
    use crate::{CodecId, CompressionId, SchemaFingerprint};
    use futures::executor::block_on;

    async fn start_and_claim(
        backend: &MemoryBackend,
        workflow_id: &str,
        queue: &str,
    ) -> ClaimedWorkflowTask {
        let workflow_type = crate::WorkflowType::new("tests.memory-terminal-guard", 1);
        backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: WorkflowId::new(workflow_id),
                workflow_type: workflow_type.clone(),
                task_queue: crate::TaskQueue::new(queue),
                input: crate::encode_payload(&0_u64).unwrap(),
            })
            .await
            .unwrap();
        backend
            .claim_workflow_task(
                crate::WorkerId::new("memory-terminal-guard"),
                ClaimWorkflowTaskOptions {
                    namespace: Namespace::default(),
                    task_queue: crate::TaskQueue::new(queue),
                    registered_workflow_types: vec![workflow_type],
                    lease_duration: Duration::from_secs(30),
                    shard_filter: None,
                },
            )
            .await
            .unwrap()
            .expect("claimable workflow task")
    }

    fn force_terminal(backend: &MemoryBackend, run_id: &RunId) {
        let mut state = backend.state.lock().unwrap();
        state.runs.get_mut(run_id).unwrap().terminal = true;
    }

    /// A routing failure while closing a run must not abandon the rest of the
    /// close, and must not read as "nothing happened".
    ///
    /// Removing `notify_parent_of_child_terminal`'s `let _ =` swallow was
    /// right, but propagating it with `?` from `handle_terminal_run` made the
    /// close *partial*: this provider has no transaction, so the run was
    /// already terminal, and skipping `cancel_children_for_parent` left the
    /// closed run's own children executing forever — work that always happened
    /// while the error was being discarded.
    ///
    /// Driven by corrupting the parent map's outcome table, because every
    /// routing error still reachable is an invariant violation rather than a
    /// race: a bogus out-of-range outcome makes the descriptor's recorded count
    /// reach `item_count` while ordinal 1 has no outcome, so assembling the
    /// result manifest fails inside `CompleteMap`.
    #[test]
    fn a_routing_failure_while_closing_a_run_still_closes_its_children() {
        block_on(async {
            let backend = MemoryBackend::new();
            let parent = start_and_claim(&backend, "wf/route-fail", "route-fail-q").await;
            let map_command_id = crate::CommandId {
                run_id: parent.run_id.clone(),
                seq: crate::CommandSeq(1),
            };
            let input_manifest = crate::encode_activity_map_input_manifest(
                (0..2_u64)
                    .map(|value| crate::encode_payload(&value).unwrap())
                    .collect(),
                2,
            )
            .unwrap();
            let map_task = crate::ChildWorkflowMapTask {
                map_command_id: map_command_id.clone(),
                workflow_type: crate::WorkflowType::new("tests.memory-terminal-guard", 1),
                task_queue: crate::TaskQueue::new("route-fail-children"),
                input_manifest,
                result_manifest_name: "results".to_owned(),
                workflow_id_prefix: "wf/route-fail/item".to_owned(),
                max_in_flight: 2,
                parent_close_policy: ParentClosePolicy::Cancel,
                failure_mode: crate::ChildWorkflowMapFailureMode::CollectAll,
            };
            backend
                .commit_workflow_task(
                    parent.claim,
                    WorkflowTaskCommit {
                        schedule_child_workflow_maps: vec![map_task],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap();
            backend
                .dispatch_child_workflow_starts(crate::DispatchChildWorkflowStartsRequest {
                    namespace: Namespace::default(),
                    limit: 16,
                })
                .await
                .unwrap();

            // Item 0 starts a child of its own, so the close has real work to
            // abandon if it short-circuits.
            let item0 = backend
                .claim_workflow_task(
                    crate::WorkerId::new("route-fail-item-0"),
                    ClaimWorkflowTaskOptions {
                        namespace: Namespace::default(),
                        task_queue: crate::TaskQueue::new("route-fail-children"),
                        registered_workflow_types: vec![crate::WorkflowType::new(
                            "tests.memory-terminal-guard",
                            1,
                        )],
                        lease_duration: Duration::from_secs(30),
                        shard_filter: None,
                    },
                )
                .await
                .unwrap()
                .expect("first child map item");
            let item0_run_id = item0.run_id.clone();
            backend
                .commit_workflow_task(
                    item0.claim,
                    WorkflowTaskCommit {
                        start_child_workflows: vec![ChildStartOutboxMessage {
                            command_id: crate::CommandId {
                                run_id: item0_run_id.clone(),
                                seq: crate::CommandSeq(1),
                            },
                            workflow_type: crate::WorkflowType::new(
                                "tests.memory-terminal-guard",
                                1,
                            ),
                            workflow_id: WorkflowId::new("wf/route-fail/grandchild"),
                            task_queue: crate::TaskQueue::new("route-fail-grandchildren"),
                            input: crate::encode_payload(&0_u64).unwrap(),
                            parent_close_policy: ParentClosePolicy::Cancel,
                            child_map_item: None,
                        }],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap();
            backend
                .dispatch_child_workflow_starts(crate::DispatchChildWorkflowStartsRequest {
                    namespace: Namespace::default(),
                    limit: 16,
                })
                .await
                .unwrap();
            let grandchild_run_id = {
                let state = backend.state.lock().unwrap();
                state
                    .workflow_ids
                    .get(&(
                        Namespace::default(),
                        WorkflowId::new("wf/route-fail/grandchild"),
                    ))
                    .cloned()
                    .expect("the grandchild was started")
            };

            // Corrupt the descriptor: an outcome at an ordinal the manifest does
            // not have, so the recorded count reaches `item_count` while ordinal
            // 1 stays missing.
            {
                let mut state = backend.state.lock().unwrap();
                state
                    .child_workflow_maps
                    .get_mut(&map_command_id)
                    .expect("child map descriptor")
                    .outcomes
                    .insert(
                        99,
                        crate::ChildWorkflowMapItemOutcome::Succeeded {
                            result: crate::encode_payload(&0_u64).unwrap(),
                        },
                    );
            }

            let err = backend
                .cancel_workflow(crate::CancelWorkflowRequest {
                    namespace: Namespace::default(),
                    workflow_id: WorkflowId::new("wf/route-fail/item/0"),
                    reason: "operator".to_owned(),
                })
                .await
                .expect_err("the corrupted descriptor must make routing fail");
            let Error::Backend(message) = &err else {
                panic!("expected a backend error, got {err:?}");
            };
            assert!(
                message.contains("was cancelled, but routing its terminal outcome")
                    && message.contains("missing child workflow map outcome for item 1"),
                "the error must say the cancellation happened and why routing failed, got \
                 `{message}`"
            );

            let state = backend.state.lock().unwrap();
            // The cancellation itself is complete...
            let item0 = state.runs.get(&item0_run_id).expect("item 0 run");
            assert!(item0.terminal);
            assert!(matches!(
                item0.history.last().map(|event| event.event_type()),
                Some(crate::HistoryEventType::WorkflowCancelled)
            ));
            // ...and so is the half that used to be skipped.
            let grandchild = state.runs.get(&grandchild_run_id).expect("grandchild run");
            assert!(
                grandchild.terminal,
                "a routing failure must not leave the closed run's own children running"
            );
            assert!(matches!(
                grandchild.history.last().map(|event| event.event_type()),
                Some(crate::HistoryEventType::WorkflowCancelled)
            ));
        });
    }

    #[test]
    fn terminal_run_with_live_claim_rejects_every_mutating_commit_kind() {
        block_on(async {
            // Every terminal transition clears the workflow claim, so the
            // guard is defense-in-depth: forge the terminal flag while a valid
            // claim and matching tail survive, then require each mutation kind
            // to be rejected (SPEC: "terminal workflow rejects new
            // workflow-visible commands").
            let backend = MemoryBackend::new();
            let claimed = start_and_claim(&backend, "wf/memory-terminal-guard", "guard-q").await;
            force_terminal(&backend, &claimed.run_id);

            for (kind, commit) in commit_test_support::mutating_commits(&claimed.run_id) {
                let err = backend
                    .commit_workflow_task(claimed.claim.clone(), commit)
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, Error::TerminalWorkflow),
                    "commit kind `{kind}` should be rejected as TerminalWorkflow, got {err:?}"
                );
            }
            // The rejection must not consume the claim, and a fully empty
            // commit stays an accepted no-op against the terminal run.
            let outcome = backend
                .commit_workflow_task(
                    claimed.claim.clone(),
                    WorkflowTaskCommit {
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(outcome, EventId(1));
        });
    }

    #[test]
    fn activity_completion_against_terminal_run_fails_identically_on_every_retry() {
        block_on(async {
            // Regression: complete/fail used to mark the record completed
            // before validating the run, so the first call returned
            // TerminalWorkflow but a retry returned AlreadyCompleted. The SQL
            // providers roll the whole transaction back; memory must validate
            // first so every retry sees the same error.
            let backend = MemoryBackend::new();
            let claimed =
                start_and_claim(&backend, "wf/memory-terminal-activity", "guard-aq").await;
            let command_id = crate::CommandId {
                run_id: claimed.run_id.clone(),
                seq: crate::CommandSeq(1),
            };
            let mut task = commit_test_support::activity_task(&claimed.run_id, &command_id);
            task.task_queue = crate::TaskQueue::new("guard-aq-activities");
            task.retry_policy.max_attempts = 1;
            backend
                .commit_workflow_task(
                    claimed.claim.clone(),
                    WorkflowTaskCommit {
                        schedule_activities: vec![task],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap();
            let activity = backend
                .claim_activity_task(
                    crate::WorkerId::new("memory-terminal-activity"),
                    ClaimActivityOptions {
                        namespace: Namespace::default(),
                        task_queue: crate::TaskQueue::new("guard-aq-activities"),
                        registered_activity_names: vec![crate::ActivityName::new(
                            "tests.guard-activity",
                        )],
                        lease_duration: Duration::from_secs(30),
                    },
                )
                .await
                .unwrap()
                .expect("claimable activity task");
            // Forge the racing state directly: run terminal while the
            // activity record is still claimed and uncompleted (cleanup on
            // real terminal transitions completes the record atomically).
            force_terminal(&backend, &claimed.run_id);

            for attempt in 0..2 {
                let err = backend
                    .complete_activity(CompleteActivityRequest {
                        claim: activity.claim.clone(),
                        result: crate::encode_payload(&1_u64).unwrap(),
                    })
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, Error::TerminalWorkflow),
                    "complete retry {attempt} should stay TerminalWorkflow, got {err:?}"
                );
            }
            for attempt in 0..2 {
                let err = backend
                    .fail_activity(FailActivityRequest {
                        claim: activity.claim.clone(),
                        failure: crate::DurableFailure::new("tests.boom", "boom"),
                    })
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, Error::TerminalWorkflow),
                    "fail retry {attempt} should stay TerminalWorkflow, got {err:?}"
                );
            }
        });
    }

    #[test]
    fn content_addressed_payload_blobs_allow_distinct_schema_fingerprints() {
        let config = PayloadStorageConfig::new().inline_threshold_bytes(0);
        let bytes = vec![0x2a];
        let mut state = MemoryState::default();

        let first = normalize_payload_for_storage(
            &mut state,
            &config,
            PayloadRef::Inline {
                codec: CodecId::MessagePack,
                schema_fingerprint: SchemaFingerprint("schema:first".to_owned()),
                compression: CompressionId::None,
                encryption: None,
                bytes: bytes.clone(),
            },
        )
        .unwrap();
        let second = normalize_payload_for_storage(
            &mut state,
            &config,
            PayloadRef::Inline {
                codec: CodecId::MessagePack,
                schema_fingerprint: SchemaFingerprint("schema:second".to_owned()),
                compression: CompressionId::None,
                encryption: None,
                bytes,
            },
        )
        .unwrap();

        assert_eq!(state.payload_blobs.len(), 1);
        let first_hydrated = hydrate_payload_from_storage(&state, first).unwrap();
        let second_hydrated = hydrate_payload_from_storage(&state, second).unwrap();
        assert!(matches!(
            first_hydrated,
            PayloadRef::Inline {
                schema_fingerprint: SchemaFingerprint(ref schema),
                ..
            } if schema == "schema:first"
        ));
        assert!(matches!(
            second_hydrated,
            PayloadRef::Inline {
                schema_fingerprint: SchemaFingerprint(ref schema),
                ..
            } if schema == "schema:second"
        ));
    }
}
