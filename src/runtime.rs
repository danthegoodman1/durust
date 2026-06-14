use crate::{
    Activity, ActivityMapCompleted, ActivityMapScheduled, ActivityMapTask, ActivityOptions,
    ActivityScheduled, ActivityTask, ChildStartOutboxMessage, ChildWorkflowCompleted,
    ChildWorkflowStarted, CommandFingerprint, CommandId, CommandSeq, DeprecatedPatchMarker, Error,
    HistoryEvent, HistoryEventData, NewHistoryEvent, ParentClosePolicy, PayloadRef, Result, RunId,
    SelectWinner, SideEffectMarker, SignalConsumed, SignalId, SignalName, TaskQueue, TimerFired,
    TimerStarted, TimestampMs, VersionMarker, WaitId, WaitKind, WaitRecord, Workflow,
    WorkflowChangeMarkerKind, WorkflowChangeVersionRecord, WorkflowId, activity_fingerprint,
    activity_map_fingerprint, child_workflow_fingerprint, command_id, payload_digest,
    signal_fingerprint, timer_fingerprint,
};
use futures::future::BoxFuture;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

thread_local! {
    static CURRENT_CONTEXT: Cell<*mut RuntimeContext> = const { Cell::new(std::ptr::null_mut()) };
    static CURRENT_ACTIVITY_CONTEXT: Cell<*const ActivityRuntimeContext> = const { Cell::new(std::ptr::null()) };
}

pub(crate) fn poll_with_runtime_context<F, T>(
    context: &mut RuntimeContext,
    poll: F,
) -> Poll<Result<T>>
where
    F: FnOnce() -> Poll<Result<T>>,
{
    CURRENT_CONTEXT.with(|slot| {
        let previous = slot.replace(context as *mut RuntimeContext);
        let result = poll();
        slot.set(previous);
        result
    })
}

fn with_context<T>(f: impl FnOnce(&mut RuntimeContext) -> T) -> T {
    CURRENT_CONTEXT.with(|slot| {
        let ptr = slot.get();
        assert!(
            !ptr.is_null(),
            "durust durable APIs must be polled inside a workflow task"
        );
        // The worker installs the pointer only for the duration of one poll and
        // does not move the RuntimeContext during that scope.
        unsafe { f(&mut *ptr) }
    })
}

pub(crate) struct ActivityRuntimeContext {
    heartbeat:
        Box<dyn Fn() -> BoxFuture<'static, Result<crate::ActivityHeartbeatOutcome>> + Send + Sync>,
}

impl ActivityRuntimeContext {
    pub(crate) fn new(
        heartbeat: impl Fn() -> BoxFuture<'static, Result<crate::ActivityHeartbeatOutcome>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            heartbeat: Box::new(heartbeat),
        }
    }
}

pub(crate) fn poll_with_activity_context<F, T>(
    context: &ActivityRuntimeContext,
    poll: F,
) -> Poll<Result<T>>
where
    F: FnOnce() -> Poll<Result<T>>,
{
    CURRENT_ACTIVITY_CONTEXT.with(|slot| {
        let previous = slot.replace(context as *const ActivityRuntimeContext);
        let result = poll();
        slot.set(previous);
        result
    })
}

fn with_activity_context<T>(f: impl FnOnce(&ActivityRuntimeContext) -> T) -> T {
    CURRENT_ACTIVITY_CONTEXT.with(|slot| {
        let ptr = slot.get();
        assert!(
            !ptr.is_null(),
            "durust activity APIs must be polled inside an activity task"
        );
        // The worker installs this pointer only while polling one activity task.
        unsafe { f(&*ptr) }
    })
}

#[derive(Debug)]
pub(crate) struct RuntimeContext {
    run_id: RunId,
    worker_workflow_task_queue: TaskQueue,
    worker_activity_task_queue: TaskQueue,
    payload_codec: crate::CodecId,
    default_activity_options: ActivityOptions,
    now: TimestampMs,
    replay_events: Vec<HistoryEvent>,
    replay_cursor: usize,
    last_loaded_event_id: crate::EventId,
    replay_target_event_id: crate::EventId,
    consumed_replay_event_ids: BTreeSet<crate::EventId>,
    needs_more_history: bool,
    last_ready_event_id: Option<crate::EventId>,
    next_command_seq: u64,
    completions: BTreeMap<CommandSeq, (crate::EventId, PayloadRef)>,
    failures: BTreeMap<CommandSeq, (crate::EventId, ActivityTerminalError)>,
    map_completions: BTreeMap<CommandSeq, (crate::EventId, ActivityMapCompleted)>,
    map_failures: BTreeMap<CommandSeq, (crate::EventId, crate::DurableFailure)>,
    child_starts: BTreeMap<CommandSeq, (crate::EventId, ChildWorkflowStarted)>,
    child_completions: BTreeMap<CommandSeq, (crate::EventId, ChildWorkflowCompleted)>,
    child_failures: BTreeMap<CommandSeq, (crate::EventId, crate::DurableFailure)>,
    child_cancellations: BTreeMap<CommandSeq, (crate::EventId, String)>,
    timers: BTreeMap<CommandSeq, (crate::EventId, TimerFired)>,
    consumed_signals: BTreeMap<CommandSeq, (crate::EventId, SignalConsumed)>,
    live_signals: BTreeMap<CommandSeq, SignalInboxRecordForRuntime>,
    payload_hydration_requests: BTreeMap<String, PayloadHydrationRequest>,
    hydrated_payloads: BTreeMap<String, PayloadRef>,
    change_markers: BTreeMap<String, RuntimeChangeMarker>,
    preconsumed_change_markers: BTreeMap<CommandSeq, RuntimeChangeMarker>,
    signal_requests: Vec<LiveSignalRequest>,
    append_events: Vec<NewHistoryEvent>,
    upsert_waits: Vec<WaitRecord>,
    schedule_activities: Vec<ActivityTask>,
    schedule_activity_maps: Vec<ActivityMapTask>,
    start_child_workflows: Vec<ChildStartOutboxMessage>,
    consume_signals: Vec<SignalId>,
    delete_waits: Vec<WaitId>,
    cancel_commands: Vec<CommandId>,
    query_projection: Option<PayloadRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuntimeChangeMarker {
    pub command_id: CommandId,
    pub change_id: String,
    pub version: i32,
    pub marker_kind: WorkflowChangeMarkerKind,
    pub event_id: crate::EventId,
}

impl RuntimeChangeMarker {
    fn from_record(record: WorkflowChangeVersionRecord) -> Self {
        Self {
            command_id: command_id(&record.run_id, record.command_seq.0),
            change_id: record.change_id,
            version: record.version,
            marker_kind: record.marker_kind,
            event_id: record.first_event_id,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LiveSignalRequest {
    pub command_id: CommandId,
    pub signal_name: SignalName,
}

#[derive(Clone, Debug)]
pub(crate) struct SignalInboxRecordForRuntime {
    pub signal_id: SignalId,
    pub signal_name: SignalName,
    pub payload: PayloadRef,
}

pub(crate) struct RuntimeCommitParts {
    pub append_events: Vec<NewHistoryEvent>,
    pub upsert_waits: Vec<WaitRecord>,
    pub schedule_activities: Vec<ActivityTask>,
    pub schedule_activity_maps: Vec<ActivityMapTask>,
    pub start_child_workflows: Vec<ChildStartOutboxMessage>,
    pub consume_signals: Vec<SignalId>,
    pub delete_waits: Vec<WaitId>,
    pub cancel_commands: Vec<CommandId>,
    pub query_projection: Option<PayloadRef>,
    pub default_activity_options: ActivityOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PayloadHydrationKind {
    Payload,
    ActivityMapResultManifest,
}

#[derive(Clone, Debug)]
pub(crate) struct PayloadHydrationRequest {
    pub kind: PayloadHydrationKind,
    pub payload: PayloadRef,
}

impl PayloadHydrationRequest {
    fn payload(payload: PayloadRef) -> Self {
        Self {
            kind: PayloadHydrationKind::Payload,
            payload,
        }
    }

    fn activity_map_result_manifest(payload: PayloadRef) -> Self {
        Self {
            kind: PayloadHydrationKind::ActivityMapResultManifest,
            payload,
        }
    }

    pub(crate) fn key(&self) -> String {
        payload_hydration_key(self.kind, &self.payload)
    }
}

fn payload_hydration_key(kind: PayloadHydrationKind, payload: &PayloadRef) -> String {
    let kind = match kind {
        PayloadHydrationKind::Payload => "payload",
        PayloadHydrationKind::ActivityMapResultManifest => "activity-map-result-manifest",
    };
    match payload {
        PayloadRef::Inline { bytes, .. } => {
            format!("{kind}:inline:{}", crate::digest_bytes(bytes))
        }
        PayloadRef::Blob {
            codec,
            schema_fingerprint,
            compression,
            encryption,
            digest,
            size,
            uri,
        } => format!(
            "{kind}:blob:{codec:?}:{}:{compression:?}:{encryption:?}:{digest}:{size}:{uri}",
            schema_fingerprint.0
        ),
    }
}

#[derive(Clone, Debug)]
enum ActivityTerminalError {
    Failed(crate::DurableFailure),
    TimedOut(String),
}

impl ActivityTerminalError {
    fn into_error(self) -> Error {
        match self {
            Self::Failed(failure) => Error::ActivityFailed(failure),
            Self::TimedOut(message) => Error::ActivityTimedOut(message),
        }
    }
}

impl RuntimeContext {
    pub(crate) fn new(
        run_id: RunId,
        worker_workflow_task_queue: TaskQueue,
        default_activity_task_queue: TaskQueue,
        payload_codec: crate::CodecId,
        now: TimestampMs,
        replay_events: Vec<HistoryEvent>,
        default_activity_options: ActivityOptions,
        next_command_seq: u64,
        last_loaded_event_id: crate::EventId,
        replay_target_event_id: crate::EventId,
        change_versions: Vec<WorkflowChangeVersionRecord>,
    ) -> Self {
        let completions = collect_completions(&replay_events);
        let failures = collect_failures(&replay_events);
        let map_completions = collect_map_completions(&replay_events);
        let map_failures = collect_map_failures(&replay_events);
        let child_starts = collect_child_starts(&replay_events);
        let child_completions = collect_child_completions(&replay_events);
        let child_failures = collect_child_failures(&replay_events);
        let child_cancellations = collect_child_cancellations(&replay_events);
        let timers = collect_timers(&replay_events);
        let consumed_signals = collect_consumed_signals(&replay_events);
        let change_markers = change_versions
            .into_iter()
            .map(RuntimeChangeMarker::from_record)
            .map(|marker| (marker.change_id.clone(), marker))
            .collect();

        Self {
            run_id,
            worker_workflow_task_queue,
            worker_activity_task_queue: default_activity_task_queue,
            payload_codec,
            default_activity_options,
            now,
            replay_events,
            replay_cursor: 0,
            last_loaded_event_id,
            replay_target_event_id,
            consumed_replay_event_ids: BTreeSet::new(),
            needs_more_history: false,
            last_ready_event_id: None,
            next_command_seq,
            completions,
            failures,
            map_completions,
            map_failures,
            child_starts,
            child_completions,
            child_failures,
            child_cancellations,
            timers,
            consumed_signals,
            live_signals: BTreeMap::new(),
            payload_hydration_requests: BTreeMap::new(),
            hydrated_payloads: BTreeMap::new(),
            change_markers,
            preconsumed_change_markers: BTreeMap::new(),
            signal_requests: Vec::new(),
            append_events: Vec::new(),
            upsert_waits: Vec::new(),
            schedule_activities: Vec::new(),
            schedule_activity_maps: Vec::new(),
            start_child_workflows: Vec::new(),
            consume_signals: Vec::new(),
            delete_waits: Vec::new(),
            cancel_commands: Vec::new(),
            query_projection: None,
        }
    }

    pub(crate) fn into_commit_parts(self) -> RuntimeCommitParts {
        RuntimeCommitParts {
            append_events: self.append_events,
            upsert_waits: self.upsert_waits,
            schedule_activities: self.schedule_activities,
            schedule_activity_maps: self.schedule_activity_maps,
            start_child_workflows: self.start_child_workflows,
            consume_signals: self.consume_signals,
            delete_waits: self.delete_waits,
            cancel_commands: self.cancel_commands,
            query_projection: self.query_projection,
            default_activity_options: self.default_activity_options,
        }
    }

    pub(crate) fn next_command_seq(&self) -> u64 {
        self.next_command_seq
    }

    fn encode_payload<T>(&self, value: &T) -> Result<PayloadRef>
    where
        T: serde::Serialize + ?Sized,
    {
        crate::encode_payload_with_codec(value, self.payload_codec)
    }

    pub(crate) fn needs_more_history_after(&mut self) -> Option<crate::EventId> {
        if self.needs_more_history {
            self.needs_more_history = false;
            Some(self.last_loaded_event_id)
        } else {
            None
        }
    }

    pub(crate) fn append_replay_events(
        &mut self,
        events: Vec<HistoryEvent>,
        last_loaded_event_id: crate::EventId,
    ) {
        if self.replay_cursor > 0 {
            self.replay_events.drain(..self.replay_cursor);
            self.replay_cursor = 0;
        }
        self.completions.extend(collect_completions(&events));
        self.failures.extend(collect_failures(&events));
        self.map_completions
            .extend(collect_map_completions(&events));
        self.map_failures.extend(collect_map_failures(&events));
        self.child_starts.extend(collect_child_starts(&events));
        self.child_completions
            .extend(collect_child_completions(&events));
        self.child_failures.extend(collect_child_failures(&events));
        self.child_cancellations
            .extend(collect_child_cancellations(&events));
        self.timers.extend(collect_timers(&events));
        self.consumed_signals
            .extend(collect_consumed_signals(&events));
        self.replay_events.extend(events);
        self.last_loaded_event_id = last_loaded_event_id;
    }

    pub(crate) fn take_signal_requests(&mut self) -> Vec<LiveSignalRequest> {
        std::mem::take(&mut self.signal_requests)
    }

    pub(crate) fn fulfill_signal_request(
        &mut self,
        command_id: CommandId,
        signal: Option<SignalInboxRecordForRuntime>,
    ) {
        if let Some(signal) = signal {
            self.live_signals.insert(command_id.seq, signal);
        }
    }

    pub(crate) fn take_payload_hydration_requests(&mut self) -> Vec<PayloadHydrationRequest> {
        let requests = self.payload_hydration_requests.values().cloned().collect();
        self.payload_hydration_requests.clear();
        requests
    }

    pub(crate) fn fulfill_payload_hydration(
        &mut self,
        request: PayloadHydrationRequest,
        hydrated: PayloadRef,
    ) -> Result<()> {
        if matches!(hydrated, PayloadRef::Blob { .. }) {
            return Err(Error::PayloadDecode(
                "backend returned an unresolved blob for an observed replay payload".to_owned(),
            ));
        }
        self.hydrated_payloads.insert(request.key(), hydrated);
        Ok(())
    }

    fn next_command_id(&mut self) -> CommandId {
        self.next_command_seq += 1;
        command_id(&self.run_id, self.next_command_seq)
    }

    fn record_ready_event_id(&mut self, event_id: crate::EventId) {
        self.last_ready_event_id = Some(event_id);
    }

    fn record_next_appended_ready_event_id(&mut self) {
        let offset = u64::try_from(self.append_events.len()).unwrap_or(u64::MAX);
        self.last_ready_event_id = Some(crate::EventId(
            self.replay_target_event_id.0.saturating_add(offset),
        ));
    }

    fn take_last_ready_event_id(&mut self) -> Option<crate::EventId> {
        self.last_ready_event_id.take()
    }

    fn cancel_command(&mut self, command_id: CommandId) {
        if !self
            .cancel_commands
            .iter()
            .any(|existing| existing == &command_id)
        {
            self.cancel_commands.push(command_id);
        }
    }

    fn peek_replay_event(&mut self) -> Option<&HistoryEvent> {
        self.skip_consumed_replay_events();
        self.replay_events.get(self.replay_cursor)
    }

    fn skip_consumed_replay_events(&mut self) {
        loop {
            let start = self.replay_cursor;
            self.skip_consumed_indexed_events();
            self.skip_preconsumed_change_markers();
            if self.replay_cursor == start {
                break;
            }
        }
    }

    fn skip_consumed_indexed_events(&mut self) {
        while let Some(event) = self.replay_events.get(self.replay_cursor) {
            if !self.consumed_replay_event_ids.remove(&event.event_id) {
                break;
            }
            self.replay_cursor += 1;
        }
    }

    fn skip_preconsumed_change_markers(&mut self) {
        while let Some(event) = self.replay_events.get(self.replay_cursor) {
            let marker = match &event.data {
                HistoryEventData::VersionMarker(marker) => Some((
                    marker.command_id.seq,
                    marker.command_id.clone(),
                    marker.change_id.as_str(),
                    marker.version,
                    WorkflowChangeMarkerKind::Version,
                )),
                HistoryEventData::DeprecatedPatchMarker(marker) => Some((
                    marker.command_id.seq,
                    marker.command_id.clone(),
                    marker.patch_id.as_str(),
                    1,
                    WorkflowChangeMarkerKind::DeprecatedPatch,
                )),
                _ => None,
            };
            let Some((seq, command_id, change_id, version, marker_kind)) = marker else {
                break;
            };
            let Some(preconsumed) = self.preconsumed_change_markers.get(&seq) else {
                break;
            };
            let matches = preconsumed.command_id == command_id
                && preconsumed.change_id == change_id
                && preconsumed.version == version
                && preconsumed.marker_kind == marker_kind
                && preconsumed.event_id == event.event_id;
            if !matches {
                break;
            }
            self.preconsumed_change_markers.remove(&seq);
            self.replay_cursor += 1;
        }
    }

    fn at_replay_tail(&self) -> bool {
        self.replay_cursor >= self.replay_events.len()
            && self.last_loaded_event_id >= self.replay_target_event_id
    }

    fn request_more_history_if_available(&mut self) -> bool {
        if self.last_loaded_event_id < self.replay_target_event_id {
            self.needs_more_history = true;
            true
        } else {
            false
        }
    }

    fn advance_replay(&mut self) {
        self.replay_cursor += 1;
    }

    fn record_indexed_ready_event_id(&mut self, event_id: crate::EventId) {
        self.consumed_replay_event_ids.insert(event_id);
        self.record_ready_event_id(event_id);
    }

    fn ready_payload_or_request(&mut self, request: PayloadHydrationRequest) -> Option<PayloadRef> {
        if matches!(request.payload, PayloadRef::Inline { .. }) {
            return Some(request.payload);
        }
        let key = request.key();
        if let Some(hydrated) = self.hydrated_payloads.remove(&key) {
            return Some(hydrated);
        }
        self.payload_hydration_requests
            .entry(key)
            .or_insert(request);
        None
    }

    fn take_completion(&mut self, command_id: &CommandId) -> Option<PayloadRef> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::ActivityCompleted(completed) = event.data {
                if completed.command_id.seq == command_id.seq {
                    let result = self.ready_payload_or_request(
                        PayloadHydrationRequest::payload(completed.result),
                    )?;
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.completions.remove(&command_id.seq);
                    return Some(result);
                }
            }
        }
        let (event_id, result) = self.completions.get(&command_id.seq).cloned()?;
        let result = self.ready_payload_or_request(PayloadHydrationRequest::payload(result))?;
        self.completions.remove(&command_id.seq);
        self.record_indexed_ready_event_id(event_id);
        Some(result)
    }

    fn take_failure(&mut self, command_id: &CommandId) -> Option<ActivityTerminalError> {
        if let Some(event) = self.peek_replay_event().cloned() {
            match event.data {
                HistoryEventData::ActivityFailed(failed)
                    if failed.command_id.seq == command_id.seq =>
                {
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.failures.remove(&command_id.seq);
                    return Some(ActivityTerminalError::Failed(failed.failure));
                }
                HistoryEventData::ActivityTimedOut(timed_out)
                    if timed_out.command_id.seq == command_id.seq =>
                {
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.failures.remove(&command_id.seq);
                    return Some(ActivityTerminalError::TimedOut(timed_out.message));
                }
                _ => {}
            }
        }
        self.failures
            .remove(&command_id.seq)
            .map(|(event_id, failure)| {
                self.record_indexed_ready_event_id(event_id);
                failure
            })
    }

    fn take_timer(&mut self, command_id: &CommandId) -> Option<TimerFired> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::TimerFired(fired) = event.data {
                if fired.command_id.seq == command_id.seq {
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.timers.remove(&command_id.seq);
                    return Some(fired);
                }
            }
        }
        self.timers
            .remove(&command_id.seq)
            .map(|(event_id, fired)| {
                self.record_indexed_ready_event_id(event_id);
                fired
            })
    }

    fn take_map_completion(&mut self, command_id: &CommandId) -> Option<ActivityMapCompleted> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::ActivityMapCompleted(completed) = event.data {
                if completed.command_id.seq == command_id.seq {
                    let mut completed = completed;
                    completed.result_manifest = self.ready_payload_or_request(
                        PayloadHydrationRequest::activity_map_result_manifest(
                            completed.result_manifest,
                        ),
                    )?;
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.map_completions.remove(&command_id.seq);
                    return Some(completed);
                }
            }
        }
        let (event_id, mut completed) = self.map_completions.get(&command_id.seq).cloned()?;
        completed.result_manifest = self.ready_payload_or_request(
            PayloadHydrationRequest::activity_map_result_manifest(completed.result_manifest),
        )?;
        self.map_completions.remove(&command_id.seq);
        self.record_indexed_ready_event_id(event_id);
        Some(completed)
    }

    fn take_map_failure(&mut self, command_id: &CommandId) -> Option<crate::DurableFailure> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::ActivityMapFailed(failed) = event.data {
                if failed.command_id.seq == command_id.seq {
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.map_failures.remove(&command_id.seq);
                    return Some(failed.failure);
                }
            }
        }
        self.map_failures
            .remove(&command_id.seq)
            .map(|(event_id, failure)| {
                self.record_indexed_ready_event_id(event_id);
                failure
            })
    }

    fn take_child_started(&mut self, command_id: &CommandId) -> Option<ChildWorkflowStarted> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::ChildWorkflowStarted(started) = event.data {
                if started.command_id.seq == command_id.seq {
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.child_starts.remove(&command_id.seq);
                    return Some(started);
                }
            }
        }
        self.child_starts
            .remove(&command_id.seq)
            .map(|(event_id, started)| {
                self.record_indexed_ready_event_id(event_id);
                started
            })
    }

    fn take_child_completion(&mut self, command_id: &CommandId) -> Option<ChildWorkflowCompleted> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::ChildWorkflowCompleted(completed) = event.data {
                if completed.command_id.seq == command_id.seq {
                    let mut completed = completed;
                    completed.result = self.ready_payload_or_request(
                        PayloadHydrationRequest::payload(completed.result),
                    )?;
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.child_completions.remove(&command_id.seq);
                    return Some(completed);
                }
            }
        }
        let (event_id, mut completed) = self.child_completions.get(&command_id.seq).cloned()?;
        completed.result =
            self.ready_payload_or_request(PayloadHydrationRequest::payload(completed.result))?;
        self.child_completions.remove(&command_id.seq);
        self.record_indexed_ready_event_id(event_id);
        Some(completed)
    }

    fn take_child_failure(&mut self, command_id: &CommandId) -> Option<crate::DurableFailure> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::ChildWorkflowFailed(failed) = event.data {
                if failed.command_id.seq == command_id.seq {
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.child_failures.remove(&command_id.seq);
                    return Some(failed.failure);
                }
            }
        }
        self.child_failures
            .remove(&command_id.seq)
            .map(|(event_id, failure)| {
                self.record_indexed_ready_event_id(event_id);
                failure
            })
    }

    fn take_child_cancellation(&mut self, command_id: &CommandId) -> Option<String> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::ChildWorkflowCancelled(cancelled) = event.data {
                if cancelled.command_id.seq == command_id.seq {
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.child_cancellations.remove(&command_id.seq);
                    return Some(cancelled.reason);
                }
            }
        }
        self.child_cancellations
            .remove(&command_id.seq)
            .map(|(event_id, reason)| {
                self.record_indexed_ready_event_id(event_id);
                reason
            })
    }

    fn take_live_signal(&mut self, command_id: &CommandId) -> Option<SignalInboxRecordForRuntime> {
        let mut signal = self.live_signals.get(&command_id.seq).cloned()?;
        signal.payload =
            self.ready_payload_or_request(PayloadHydrationRequest::payload(signal.payload))?;
        self.live_signals.remove(&command_id.seq);
        Some(signal)
    }

    fn take_consumed_signal(&mut self, command_id: &CommandId) -> Option<SignalConsumed> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::SignalConsumed(consumed) = event.data {
                if consumed.command_id.seq == command_id.seq {
                    let mut consumed = consumed;
                    consumed.payload = self.ready_payload_or_request(
                        PayloadHydrationRequest::payload(consumed.payload),
                    )?;
                    self.advance_replay();
                    self.record_ready_event_id(event.event_id);
                    self.consumed_signals.remove(&command_id.seq);
                    return Some(consumed);
                }
            }
        }
        let (event_id, mut consumed) = self.consumed_signals.get(&command_id.seq).cloned()?;
        consumed.payload =
            self.ready_payload_or_request(PayloadHydrationRequest::payload(consumed.payload))?;
        self.consumed_signals.remove(&command_id.seq);
        self.record_indexed_ready_event_id(event_id);
        Some(consumed)
    }

    fn request_signal(&mut self, command_id: CommandId, signal_name: SignalName) {
        if !self
            .signal_requests
            .iter()
            .any(|request| request.command_id.seq == command_id.seq)
        {
            self.signal_requests.push(LiveSignalRequest {
                command_id,
                signal_name,
            });
        }
    }

    fn effective_activity_options(&self, overrides: ActivityOptions) -> ActivityOptions {
        self.default_activity_options
            .clone()
            .merge_overrides(overrides)
            .with_task_queue_fallback(self.worker_activity_task_queue.clone())
    }

    fn get_version(
        &mut self,
        change_id: String,
        min_supported: i32,
        max_supported: i32,
    ) -> Result<i32> {
        validate_version_range(&change_id, min_supported, max_supported)?;

        if let Some(event) = self.peek_replay_event().cloned() {
            match event.data {
                HistoryEventData::VersionMarker(marker) => {
                    if marker.change_id != change_id {
                        return Err(Error::Nondeterminism(format!(
                            "expected VersionMarker `{change_id}`, found `{}`",
                            marker.change_id
                        )));
                    }
                    let command_id = self.next_command_id();
                    validate_marker_command(&change_id, &command_id, &marker.command_id)?;
                    self.advance_replay();
                    return validate_recorded_version(
                        change_id,
                        marker.version,
                        min_supported,
                        max_supported,
                    );
                }
                HistoryEventData::DeprecatedPatchMarker(marker) => {
                    return Err(Error::Nondeterminism(format!(
                        "expected VersionMarker `{change_id}`, found DeprecatedPatchMarker `{}`",
                        marker.patch_id
                    )));
                }
                _ => {
                    if self.change_markers.contains_key(&change_id) {
                        return Err(Error::Nondeterminism(format!(
                            "version marker `{change_id}` moved relative to command history"
                        )));
                    }
                }
            }
        }

        if let Some(marker) = self.change_markers.get(&change_id).cloned() {
            if marker.marker_kind != WorkflowChangeMarkerKind::Version {
                return Err(Error::Nondeterminism(format!(
                    "expected VersionMarker `{change_id}`, found DeprecatedPatchMarker"
                )));
            }
            self.preconsume_marker(&change_id, &marker)?;
            return validate_recorded_version(
                change_id,
                marker.version,
                min_supported,
                max_supported,
            );
        }

        if self.at_replay_tail() {
            let command_id = self.next_command_id();
            let marker = VersionMarker {
                command_id,
                change_id,
                version: max_supported,
            };
            self.append_events
                .push(NewHistoryEvent::new(HistoryEventData::VersionMarker(
                    marker,
                )));
            return Ok(max_supported);
        }

        validate_recorded_version(change_id, DEFAULT_VERSION, min_supported, max_supported)
    }

    fn deprecate_patch(&mut self, patch_id: String) -> Result<()> {
        if let Some(event) = self.peek_replay_event().cloned() {
            match event.data {
                HistoryEventData::VersionMarker(marker) => {
                    if marker.change_id != patch_id {
                        return Err(Error::Nondeterminism(format!(
                            "expected patch marker `{patch_id}`, found VersionMarker `{}`",
                            marker.change_id
                        )));
                    }
                    let command_id = self.next_command_id();
                    validate_marker_command(&patch_id, &command_id, &marker.command_id)?;
                    if marker.version <= DEFAULT_VERSION {
                        return Err(Error::UnsupportedWorkflowVersion {
                            change_id: patch_id,
                            version: marker.version,
                            min_supported: 1,
                            max_supported: i32::MAX,
                        });
                    }
                    self.advance_replay();
                    return Ok(());
                }
                HistoryEventData::DeprecatedPatchMarker(marker) => {
                    if marker.patch_id != patch_id {
                        return Err(Error::Nondeterminism(format!(
                            "expected DeprecatedPatchMarker `{patch_id}`, found `{}`",
                            marker.patch_id
                        )));
                    }
                    let command_id = self.next_command_id();
                    validate_marker_command(&patch_id, &command_id, &marker.command_id)?;
                    self.advance_replay();
                    return Ok(());
                }
                _ => {
                    if self.change_markers.contains_key(&patch_id) {
                        return Err(Error::Nondeterminism(format!(
                            "patch marker `{patch_id}` moved relative to command history"
                        )));
                    }
                }
            }
        }

        if let Some(marker) = self.change_markers.get(&patch_id).cloned() {
            match marker.marker_kind {
                WorkflowChangeMarkerKind::Version => {
                    if marker.version <= DEFAULT_VERSION {
                        return Err(Error::UnsupportedWorkflowVersion {
                            change_id: patch_id,
                            version: marker.version,
                            min_supported: 1,
                            max_supported: i32::MAX,
                        });
                    }
                }
                WorkflowChangeMarkerKind::DeprecatedPatch => {}
            }
            self.preconsume_marker(&patch_id, &marker)?;
            return Ok(());
        }

        if self.at_replay_tail() {
            let command_id = self.next_command_id();
            self.append_events.push(NewHistoryEvent::new(
                HistoryEventData::DeprecatedPatchMarker(DeprecatedPatchMarker {
                    command_id,
                    patch_id,
                }),
            ));
        }

        Ok(())
    }

    fn preconsume_marker(&mut self, change_id: &str, marker: &RuntimeChangeMarker) -> Result<()> {
        if marker.event_id <= self.last_loaded_event_id {
            return Err(Error::Nondeterminism(format!(
                "change marker `{change_id}` was indexed before loaded history cursor"
            )));
        }
        let command_id = self.next_command_id();
        validate_marker_command(change_id, &command_id, &marker.command_id)?;
        self.preconsumed_change_markers
            .insert(command_id.seq, marker.clone());
        Ok(())
    }
}

fn validate_version_range(change_id: &str, min_supported: i32, max_supported: i32) -> Result<()> {
    if min_supported > max_supported {
        return Err(Error::Backend(format!(
            "invalid version range for `{change_id}`: min {min_supported} exceeds max {max_supported}"
        )));
    }
    if max_supported <= DEFAULT_VERSION {
        return Err(Error::Backend(format!(
            "invalid max version for `{change_id}`: {max_supported}"
        )));
    }
    Ok(())
}

fn validate_recorded_version(
    change_id: String,
    version: i32,
    min_supported: i32,
    max_supported: i32,
) -> Result<i32> {
    if version < min_supported || version > max_supported {
        return Err(Error::UnsupportedWorkflowVersion {
            change_id,
            version,
            min_supported,
            max_supported,
        });
    }
    Ok(version)
}

fn validate_marker_command(
    change_id: &str,
    expected: &CommandId,
    recorded: &CommandId,
) -> Result<()> {
    if recorded.seq != expected.seq {
        return Err(Error::Nondeterminism(format!(
            "version marker `{change_id}` command sequence changed: expected {}, found {}",
            expected.seq.0, recorded.seq.0
        )));
    }
    Ok(())
}

impl<A> Unpin for ActivityFuture<A> where A: Activity {}

pub fn activity_call<A>(input: A::Input) -> ActivityFuture<A>
where
    A: Activity,
{
    ActivityFuture {
        input: Some(input),
        options: ActivityOptions::default(),
        state: ActivityFutureState::Init,
        _activity: std::marker::PhantomData,
    }
}

pub fn set_default_activity_options(options: ActivityOptions) {
    with_context(|runtime| {
        runtime.default_activity_options = options;
    });
}

pub const DEFAULT_VERSION: i32 = -1;

pub fn get_version(
    change_id: impl Into<String>,
    min_supported: i32,
    max_supported: i32,
) -> Result<i32> {
    with_context(|runtime| runtime.get_version(change_id.into(), min_supported, max_supported))
}

pub fn patched(patch_id: impl Into<String>) -> Result<bool> {
    Ok(get_version(patch_id, DEFAULT_VERSION, 1)? != DEFAULT_VERSION)
}

pub fn deprecate_patch(patch_id: impl Into<String>) -> Result<()> {
    with_context(|runtime| runtime.deprecate_patch(patch_id.into()))
}

pub fn continue_as_new<T, I>(input: I) -> Result<T>
where
    I: serde::Serialize,
{
    let input = with_context(|runtime| runtime.encode_payload(&input))?;
    Err(Error::ContinueAsNew { input })
}

pub fn side_effect<T, F>(key: impl Into<String>, effect: F) -> SideEffectFuture<T, F>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
    F: FnOnce() -> T,
{
    SideEffectFuture {
        key: key.into(),
        effect: Some(effect),
        done: false,
        _value: std::marker::PhantomData,
    }
}

pub struct SideEffectFuture<T, F>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
    F: FnOnce() -> T,
{
    key: String,
    effect: Option<F>,
    done: bool,
    _value: std::marker::PhantomData<T>,
}

impl<T, F> Unpin for SideEffectFuture<T, F>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
    F: FnOnce() -> T,
{
}

impl<T, F> Future for SideEffectFuture<T, F>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
    F: FnOnce() -> T,
{
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_context(|runtime| {
            if self.done {
                return Poll::Ready(Err(Error::Nondeterminism(
                    "side effect future polled after completion".to_owned(),
                )));
            }
            if self.key.is_empty() {
                return Poll::Ready(Err(Error::PayloadEncode(
                    "side effect key must not be empty".to_owned(),
                )));
            }

            if let Some(event) = runtime.peek_replay_event().cloned() {
                let HistoryEventData::SideEffectMarker(marker) = event.data else {
                    return Poll::Ready(Err(Error::Nondeterminism(format!(
                        "expected SideEffectMarker `{}`, found {:?}",
                        self.key, event.event_type
                    ))));
                };
                let command_id = runtime.next_command_id();
                if marker.command_id.seq != command_id.seq {
                    return Poll::Ready(Err(Error::Nondeterminism(format!(
                        "side effect `{}` command sequence changed: expected {}, found {}",
                        self.key, command_id.seq.0, marker.command_id.seq.0
                    ))));
                }
                if marker.key != self.key {
                    return Poll::Ready(Err(Error::Nondeterminism(format!(
                        "expected side effect `{}`, found `{}`",
                        self.key, marker.key
                    ))));
                }
                if let Err(err) = crate::validate_side_effect_marker(&marker) {
                    return Poll::Ready(Err(err));
                }
                runtime.advance_replay();
                self.done = true;
                Poll::Ready(crate::decode_payload(&marker.value))
            } else if runtime.at_replay_tail() {
                let command_id = runtime.next_command_id();
                let Some(effect) = self.effect.take() else {
                    return Poll::Ready(Err(Error::Nondeterminism(
                        "side effect closure missing".to_owned(),
                    )));
                };
                let value = effect();
                let payload = match runtime.encode_payload(&value) {
                    Ok(payload) => payload,
                    Err(err) => return Poll::Ready(Err(err)),
                };
                if let Err(err) = crate::validate_inline_side_effect_payload(&payload) {
                    return Poll::Ready(Err(err));
                }
                runtime.append_events.push(NewHistoryEvent::new(
                    HistoryEventData::SideEffectMarker(SideEffectMarker {
                        command_id,
                        key: self.key.clone(),
                        value: payload,
                    }),
                ));
                self.done = true;
                Poll::Ready(Ok(value))
            } else {
                runtime.request_more_history_if_available();
                Poll::Pending
            }
        })
    }
}

pub fn publish<T>(view: &T) -> Result<()>
where
    T: serde::Serialize + ?Sized,
{
    with_context(|runtime| {
        runtime.query_projection = Some(runtime.encode_payload(view)?);
        Ok(())
    })
}

pub fn heartbeat_activity() -> ActivityHeartbeatFuture {
    ActivityHeartbeatFuture {
        state: ActivityHeartbeatState::Init,
    }
}

enum ActivityHeartbeatState {
    Init,
    Waiting(BoxFuture<'static, Result<crate::ActivityHeartbeatOutcome>>),
    Done,
}

pub struct ActivityHeartbeatFuture {
    state: ActivityHeartbeatState,
}

impl Unpin for ActivityHeartbeatFuture {}

impl Future for ActivityHeartbeatFuture {
    type Output = Result<crate::ActivityHeartbeatOutcome>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match &mut self.state {
                ActivityHeartbeatState::Init => {
                    let heartbeat = with_activity_context(|context| (context.heartbeat)());
                    self.state = ActivityHeartbeatState::Waiting(heartbeat);
                }
                ActivityHeartbeatState::Waiting(heartbeat) => {
                    let poll = Pin::new(heartbeat).poll(cx);
                    if poll.is_ready() {
                        self.state = ActivityHeartbeatState::Done;
                    }
                    return poll;
                }
                ActivityHeartbeatState::Done => {
                    return Poll::Ready(Err(Error::Backend(
                        "activity heartbeat future polled after completion".to_owned(),
                    )));
                }
            }
        }
    }
}

pub trait DurableSelectBranch: Future + Unpin {
    #[doc(hidden)]
    fn __durust_cancel_branch(&self);
}

pub trait DurableJoinBranch: Future + Unpin {}

pub trait DurableBranchExt: DurableSelectBranch + Sized {
    fn map_ok<T, U, M>(self, mapper: M) -> MapOkBranch<Self, M>
    where
        Self: Future<Output = Result<T>>,
        M: FnOnce(T) -> U,
    {
        MapOkBranch {
            branch: self,
            mapper: Some(mapper),
        }
    }

    fn boxed<T>(self) -> BoxSelectBranch<T>
    where
        Self: Future<Output = Result<T>> + Send + 'static,
        T: 'static,
    {
        BoxSelectBranch {
            branch: Box::new(self),
        }
    }
}

impl<B> DurableBranchExt for B where B: DurableSelectBranch + Sized {}

pub struct MapOkBranch<B, M> {
    branch: B,
    mapper: Option<M>,
}

impl<B, M> Unpin for MapOkBranch<B, M>
where
    B: DurableSelectBranch,
    M: Unpin,
{
}

impl<B, M, T, U> Future for MapOkBranch<B, M>
where
    B: DurableSelectBranch + Future<Output = Result<T>>,
    M: FnOnce(T) -> U + Unpin,
{
    type Output = Result<U>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.branch).poll(cx) {
            Poll::Ready(Ok(value)) => {
                let mapper = self
                    .mapper
                    .take()
                    .expect("durust map_ok branch polled after completion");
                Poll::Ready(Ok(mapper(value)))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<B, M, T, U> DurableSelectBranch for MapOkBranch<B, M>
where
    B: DurableSelectBranch + Future<Output = Result<T>>,
    M: FnOnce(T) -> U + Unpin,
{
    fn __durust_cancel_branch(&self) {
        self.branch.__durust_cancel_branch();
    }
}

impl<B, M, T, U> DurableJoinBranch for MapOkBranch<B, M>
where
    B: DurableSelectBranch + DurableJoinBranch + Future<Output = Result<T>>,
    M: FnOnce(T) -> U + Unpin,
{
}

pub struct BoxSelectBranch<T> {
    branch: Box<dyn DurableSelectBranch<Output = Result<T>> + Send>,
}

impl<T> Unpin for BoxSelectBranch<T> {}

impl<T> Future for BoxSelectBranch<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.branch).poll(cx)
    }
}

impl<T> DurableSelectBranch for BoxSelectBranch<T> {
    fn __durust_cancel_branch(&self) {
        self.branch.__durust_cancel_branch();
    }
}

impl<T> DurableJoinBranch for BoxSelectBranch<T> {}

pub fn join_all<I, B, T>(branches: I) -> JoinAllFuture<B, T>
where
    I: IntoIterator<Item = B>,
    B: DurableJoinBranch<Output = Result<T>>,
    T: Unpin,
{
    let branches = branches.into_iter().map(Some).collect::<Vec<Option<B>>>();
    let mut outputs = Vec::with_capacity(branches.len());
    outputs.resize_with(branches.len(), || None);
    JoinAllFuture {
        branches,
        outputs,
        done: false,
    }
}

pub struct JoinAllFuture<B, T>
where
    B: DurableJoinBranch<Output = Result<T>>,
    T: Unpin,
{
    branches: Vec<Option<B>>,
    outputs: Vec<Option<T>>,
    done: bool,
}

impl<B, T> Unpin for JoinAllFuture<B, T>
where
    B: DurableJoinBranch<Output = Result<T>>,
    T: Unpin,
{
}

impl<B, T> Future for JoinAllFuture<B, T>
where
    B: DurableJoinBranch<Output = Result<T>>,
    T: Unpin,
{
    type Output = Result<Vec<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.done {
            return Poll::Ready(Err(Error::Nondeterminism(
                "join_all future polled after completion".to_owned(),
            )));
        }
        if self.branches.is_empty() {
            self.done = true;
            return Poll::Ready(Ok(Vec::new()));
        }

        let mut made_progress = false;
        for index in 0..self.branches.len() {
            if self.outputs[index].is_some() {
                continue;
            }
            let Some(branch) = self.branches[index].as_mut() else {
                continue;
            };
            match Pin::new(branch).poll(cx) {
                Poll::Ready(Ok(value)) => {
                    self.outputs[index] = Some(value);
                    self.branches[index] = None;
                    made_progress = true;
                }
                Poll::Ready(Err(err)) => {
                    self.done = true;
                    return Poll::Ready(Err(err));
                }
                Poll::Pending => {}
            }
        }

        if self.outputs.iter().all(Option::is_some) {
            self.done = true;
            let outputs = self
                .outputs
                .iter_mut()
                .map(|output| output.take().expect("join_all output missing"))
                .collect();
            return Poll::Ready(Ok(outputs));
        }
        if made_progress {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectAllWinner<T> {
    pub branch_index: usize,
    pub value: T,
}

impl<T> SelectAllWinner<T> {
    pub fn into_value(self) -> T {
        self.value
    }
}

pub fn select_all<I, B, T>(branches: I) -> SelectAllFuture<B, T>
where
    I: IntoIterator<Item = B>,
    B: DurableSelectBranch<Output = Result<T>>,
    T: Unpin,
{
    let branches = branches.into_iter().map(Some).collect::<Vec<Option<B>>>();
    let mut outputs = Vec::with_capacity(branches.len());
    outputs.resize_with(branches.len(), || None);
    SelectAllFuture {
        branches_digest: format!("select_all:{}", branches.len()),
        branches,
        outputs,
        command_id: None,
        done: false,
    }
}

pub struct SelectAllFuture<B, T>
where
    B: DurableSelectBranch<Output = Result<T>>,
    T: Unpin,
{
    branches: Vec<Option<B>>,
    outputs: Vec<Option<(crate::EventId, Result<T>)>>,
    command_id: Option<CommandId>,
    branches_digest: String,
    done: bool,
}

impl<B, T> Unpin for SelectAllFuture<B, T>
where
    B: DurableSelectBranch<Output = Result<T>>,
    T: Unpin,
{
}

impl<B, T> Future for SelectAllFuture<B, T>
where
    B: DurableSelectBranch<Output = Result<T>>,
    T: Unpin,
{
    type Output = Result<SelectAllWinner<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.done {
            return Poll::Ready(Err(Error::Nondeterminism(
                "select_all future polled after completion".to_owned(),
            )));
        }
        if self.branches.is_empty() {
            self.done = true;
            return Poll::Ready(Err(Error::Backend(
                "select_all requires at least one durable branch".to_owned(),
            )));
        }
        if self.command_id.is_none() {
            self.command_id = Some(with_context(|runtime| runtime.next_command_id()));
        }

        for index in 0..self.branches.len() {
            if self.outputs[index].is_some() {
                continue;
            }
            let Some(branch) = self.branches[index].as_mut() else {
                continue;
            };
            with_context(|runtime| {
                runtime.take_last_ready_event_id();
            });
            match Pin::new(branch).poll(cx) {
                Poll::Ready(Ok(value)) => {
                    let event_id = with_context(|runtime| runtime.take_last_ready_event_id())
                        .unwrap_or(crate::EventId::ZERO);
                    self.outputs[index] = Some((event_id, Ok(value)));
                }
                Poll::Ready(Err(err)) => {
                    if let Some(event_id) =
                        with_context(|runtime| runtime.take_last_ready_event_id())
                    {
                        self.outputs[index] = Some((event_id, Err(err)));
                    } else {
                        self.done = true;
                        return Poll::Ready(Err(err));
                    }
                }
                Poll::Pending => {}
            }
        }

        let mut winner: Option<(usize, crate::EventId)> = None;
        for (index, output) in self.outputs.iter().enumerate() {
            if let Some((event_id, _)) = output {
                match winner {
                    Some((winner_index, winner_event_id))
                        if (winner_event_id, winner_index) <= (*event_id, index) => {}
                    _ => winner = Some((index, *event_id)),
                }
            }
        }
        let Some((winner_index, winning_event_id)) = winner else {
            return Poll::Pending;
        };
        let command_id = self
            .command_id
            .as_ref()
            .expect("select_all command id initialized")
            .clone();
        match record_select_winner(
            &command_id,
            winner_index as u32,
            winning_event_id,
            &self.branches_digest,
        ) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => {
                self.done = true;
                Poll::Ready(Err(err))
            }
            Poll::Ready(Ok(())) => {
                for index in 0..self.branches.len() {
                    if index != winner_index && self.outputs[index].is_none() {
                        if let Some(branch) = self.branches[index].as_ref() {
                            branch.__durust_cancel_branch();
                        }
                    }
                }
                self.done = true;
                let (_, value) = self.outputs[winner_index]
                    .take()
                    .expect("select_all winner completed without output");
                match value {
                    Ok(value) => Poll::Ready(Ok(SelectAllWinner {
                        branch_index: winner_index,
                        value,
                    })),
                    Err(err) => Poll::Ready(Err(err)),
                }
            }
        }
    }
}

#[doc(hidden)]
pub fn __durust_join_assert_branch<F>(_: &F)
where
    F: DurableJoinBranch,
{
}

#[doc(hidden)]
pub fn __durust_select_ensure_command_id(command_id: &mut Option<CommandId>) {
    if command_id.is_none() {
        *command_id = Some(with_context(|runtime| runtime.next_command_id()));
    }
}

#[doc(hidden)]
pub fn __durust_select_clear_ready_event_id() {
    with_context(|runtime| {
        runtime.take_last_ready_event_id();
    });
}

#[doc(hidden)]
pub fn __durust_select_take_ready_event_id() -> Option<crate::EventId> {
    with_context(|runtime| runtime.take_last_ready_event_id())
}

#[doc(hidden)]
pub fn __durust_select_record_winner(
    select_command_id: &CommandId,
    branch_ordinal: u32,
    winning_event_id: crate::EventId,
    branches_digest: &str,
) -> Poll<Result<()>> {
    record_select_winner(
        select_command_id,
        branch_ordinal,
        winning_event_id,
        branches_digest,
    )
}

fn record_select_winner(
    select_command_id: &CommandId,
    branch_ordinal: u32,
    winning_event_id: crate::EventId,
    branches_digest: &str,
) -> Poll<Result<()>> {
    with_context(|runtime| {
        while let Some(event) = runtime.peek_replay_event().cloned() {
            match event.data {
                HistoryEventData::SelectWinner(winner) => {
                    if winner.select_command_id.seq != select_command_id.seq {
                        return Poll::Ready(Err(Error::Nondeterminism(format!(
                            "expected SelectWinner command {}, found {}",
                            select_command_id.seq.0, winner.select_command_id.seq.0
                        ))));
                    }
                    if winner.branches_digest != branches_digest {
                        return Poll::Ready(Err(Error::Nondeterminism(format!(
                            "select branch order changed for command {}",
                            select_command_id.seq.0
                        ))));
                    }
                    if winner.branch_ordinal != branch_ordinal {
                        return Poll::Ready(Err(Error::Nondeterminism(format!(
                            "select winner changed for command {}: recorded {}, observed {}",
                            select_command_id.seq.0, winner.branch_ordinal, branch_ordinal
                        ))));
                    }
                    if winner.winning_event_id != winning_event_id {
                        return Poll::Ready(Err(Error::Nondeterminism(format!(
                            "select winning event changed for command {}: recorded {}, observed {}",
                            select_command_id.seq.0, winner.winning_event_id, winning_event_id
                        ))));
                    }
                    runtime.advance_replay();
                    return Poll::Ready(Ok(()));
                }
                other if select_can_ignore_losing_ready_event(&other) => {
                    runtime.advance_replay();
                }
                _ => break,
            }
        }
        if runtime.request_more_history_if_available() {
            return Poll::Pending;
        }
        runtime
            .append_events
            .push(NewHistoryEvent::new(HistoryEventData::SelectWinner(
                SelectWinner {
                    select_command_id: select_command_id.clone(),
                    branch_ordinal,
                    winning_event_id,
                    branches_digest: branches_digest.to_owned(),
                },
            )));
        Poll::Ready(Ok(()))
    })
}

fn select_can_ignore_losing_ready_event(data: &HistoryEventData) -> bool {
    matches!(
        data,
        HistoryEventData::ActivityCompleted(_)
            | HistoryEventData::ActivityFailed(_)
            | HistoryEventData::ActivityTimedOut(_)
            | HistoryEventData::ActivityMapCompleted(_)
            | HistoryEventData::ActivityMapFailed(_)
            | HistoryEventData::ChildWorkflowStarted(_)
            | HistoryEventData::ChildWorkflowCompleted(_)
            | HistoryEventData::ChildWorkflowFailed(_)
            | HistoryEventData::ChildWorkflowCancelled(_)
            | HistoryEventData::TimerFired(_)
            | HistoryEventData::SignalConsumed(_)
    )
}

pub struct ActivityFuture<A>
where
    A: Activity,
{
    input: Option<A::Input>,
    options: ActivityOptions,
    state: ActivityFutureState,
    _activity: std::marker::PhantomData<A>,
}

impl<A> ActivityFuture<A>
where
    A: Activity,
{
    pub fn task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.options = self.options.task_queue(task_queue);
        self
    }

    pub fn retry(mut self, retry_policy: crate::RetryPolicy) -> Self {
        self.options = self.options.retry(retry_policy);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.options = self.options.timeout(timeout);
        self
    }

    pub fn heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.options = self.options.heartbeat_timeout(timeout);
        self
    }

    pub fn spawn(self) -> ActivitySpawnFuture<A> {
        ActivitySpawnFuture {
            input: self.input,
            options: self.options,
            state: ActivitySpawnState::Init,
            _activity: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
enum ActivityFutureState {
    Init,
    Waiting(CommandId),
    Done,
}

impl<A> Future for ActivityFuture<A>
where
    A: Activity,
{
    type Output = Result<A::Output>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_context(|runtime| match &self.state {
            ActivityFutureState::Init => self.poll_init(runtime),
            ActivityFutureState::Waiting(command_id) => {
                let command_id = command_id.clone();
                self.poll_waiting(runtime, &command_id)
            }
            ActivityFutureState::Done => Poll::Ready(Err(Error::Nondeterminism(
                "activity future polled after completion".to_owned(),
            ))),
        })
    }
}

impl<A> DurableSelectBranch for ActivityFuture<A>
where
    A: Activity,
{
    fn __durust_cancel_branch(&self) {
        if let ActivityFutureState::Waiting(command_id) = &self.state {
            with_context(|runtime| runtime.cancel_command(command_id.clone()));
        }
    }
}

impl<A> DurableJoinBranch for ActivityFuture<A> where A: Activity {}

impl<A> ActivityFuture<A>
where
    A: Activity,
{
    fn poll_init(&mut self, runtime: &mut RuntimeContext) -> Poll<Result<A::Output>> {
        let command_id =
            match poll_activity_schedule::<A>(&mut self.input, self.options.clone(), runtime) {
                Poll::Ready(Ok(command_id)) => command_id,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            };
        self.state = ActivityFutureState::Waiting(command_id.clone());
        self.poll_waiting(runtime, &command_id)
    }

    fn poll_waiting(
        &mut self,
        runtime: &mut RuntimeContext,
        command_id: &CommandId,
    ) -> Poll<Result<A::Output>> {
        if let Some(result) = runtime.take_completion(command_id) {
            self.state = ActivityFutureState::Done;
            return Poll::Ready(crate::decode_payload::<A::Output>(&result));
        }
        if let Some(error) = runtime.take_failure(command_id) {
            self.state = ActivityFutureState::Done;
            return Poll::Ready(Err(error.into_error()));
        }

        runtime.request_more_history_if_available();
        Poll::Pending
    }
}

#[derive(Debug)]
enum ActivitySpawnState {
    Init,
    Done,
}

pub struct ActivitySpawnFuture<A>
where
    A: Activity,
{
    input: Option<A::Input>,
    options: ActivityOptions,
    state: ActivitySpawnState,
    _activity: std::marker::PhantomData<A>,
}

impl<A> Unpin for ActivitySpawnFuture<A> where A: Activity {}

#[derive(Debug)]
pub struct ActivityHandle<A>
where
    A: Activity,
{
    command_id: CommandId,
    _activity: std::marker::PhantomData<A>,
}

impl<A> ActivityHandle<A>
where
    A: Activity,
{
    pub fn result(self) -> ActivityResultFuture<A> {
        ActivityResultFuture {
            command_id: self.command_id,
            done: false,
            _activity: std::marker::PhantomData,
        }
    }
}

impl<A> Future for ActivitySpawnFuture<A>
where
    A: Activity,
{
    type Output = Result<ActivityHandle<A>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_context(|runtime| match self.state {
            ActivitySpawnState::Init => {
                let options = self.options.clone();
                match poll_activity_schedule::<A>(&mut self.input, options, runtime) {
                    Poll::Ready(Ok(command_id)) => {
                        self.state = ActivitySpawnState::Done;
                        Poll::Ready(Ok(ActivityHandle {
                            command_id,
                            _activity: std::marker::PhantomData,
                        }))
                    }
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                    Poll::Pending => Poll::Pending,
                }
            }
            ActivitySpawnState::Done => Poll::Ready(Err(Error::Nondeterminism(
                "activity spawn future polled after completion".to_owned(),
            ))),
        })
    }
}

impl<A> DurableJoinBranch for ActivitySpawnFuture<A> where A: Activity {}

pub struct ActivityResultFuture<A>
where
    A: Activity,
{
    command_id: CommandId,
    done: bool,
    _activity: std::marker::PhantomData<A>,
}

impl<A> Unpin for ActivityResultFuture<A> where A: Activity {}

impl<A> Future for ActivityResultFuture<A>
where
    A: Activity,
{
    type Output = Result<A::Output>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_context(|runtime| {
            if self.done {
                return Poll::Ready(Err(Error::Nondeterminism(
                    "activity result future polled after completion".to_owned(),
                )));
            }
            if let Some(result) = runtime.take_completion(&self.command_id) {
                self.done = true;
                return Poll::Ready(crate::decode_payload::<A::Output>(&result));
            }
            if let Some(error) = runtime.take_failure(&self.command_id) {
                self.done = true;
                return Poll::Ready(Err(error.into_error()));
            }
            runtime.request_more_history_if_available();
            Poll::Pending
        })
    }
}

impl<A> DurableSelectBranch for ActivityResultFuture<A>
where
    A: Activity,
{
    fn __durust_cancel_branch(&self) {
        with_context(|runtime| runtime.cancel_command(self.command_id.clone()));
    }
}

impl<A> DurableJoinBranch for ActivityResultFuture<A> where A: Activity {}

fn poll_activity_schedule<A>(
    input: &mut Option<A::Input>,
    options: ActivityOptions,
    runtime: &mut RuntimeContext,
) -> Poll<Result<CommandId>>
where
    A: Activity,
{
    if runtime.peek_replay_event().is_none() && !runtime.at_replay_tail() {
        runtime.request_more_history_if_available();
        return Poll::Pending;
    }

    let command_id = runtime.next_command_id();
    let options = runtime.effective_activity_options(options);
    let task_queue = options
        .task_queue
        .clone()
        .expect("effective activity options include task queue fallback");
    let retry_policy = options.effective_retry_policy();
    let fingerprint_options = ActivityOptions {
        task_queue: Some(task_queue.clone()),
        retry_policy: Some(retry_policy.clone()),
        start_to_close_timeout: options.start_to_close_timeout,
        heartbeat_timeout: options.heartbeat_timeout,
    };
    let activity_input = input
        .as_ref()
        .expect("activity input exists before schedule");
    let input_ref = runtime.encode_payload(activity_input)?;
    let fingerprint = activity_fingerprint(
        A::activity_name(),
        payload_digest(&input_ref),
        fingerprint_options.digest()?,
    );

    if let Some(event) = runtime.peek_replay_event().cloned() {
        let HistoryEventData::ActivityScheduled(scheduled) = event.data else {
            return Poll::Ready(Err(Error::Nondeterminism(format!(
                "expected ActivityScheduled for command {}, found {:?}",
                command_id.seq.0, event.event_type
            ))));
        };
        if scheduled.command_id.seq != command_id.seq {
            return Poll::Ready(Err(Error::Nondeterminism(format!(
                "expected command seq {}, found {}",
                command_id.seq.0, scheduled.command_id.seq.0
            ))));
        }
        if scheduled.fingerprint != fingerprint {
            return Poll::Ready(Err(Error::Nondeterminism(format!(
                "activity command fingerprint changed for command {}",
                command_id.seq.0
            ))));
        }
        runtime.advance_replay();
        *input = None;
        return Poll::Ready(Ok(command_id));
    }

    let scheduled = ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: A::activity_name(),
        task_queue,
        retry_policy,
        start_to_close_timeout: options.start_to_close_timeout,
        heartbeat_timeout: options.heartbeat_timeout,
        input: input_ref,
        fingerprint,
    };
    runtime
        .append_events
        .push(NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
            scheduled.clone(),
        )));
    runtime
        .schedule_activities
        .push(ActivityTask::from_scheduled(&scheduled));
    *input = None;
    Poll::Ready(Ok(command_id))
}

pub fn activity_map<A>(_activity: A) -> ActivityMapBuilder<A>
where
    A: Activity,
{
    ActivityMapBuilder {
        options: ActivityOptions::default(),
        input_manifest: None,
        result_manifest_name: "results".to_owned(),
        max_in_flight: 1,
        _activity: std::marker::PhantomData,
    }
}

pub fn activity_map_manifest<T>(items: impl IntoIterator<Item = T>) -> Result<PayloadRef>
where
    T: serde::Serialize,
{
    activity_map_manifest_with_page_size(items, crate::ACTIVITY_MAP_MANIFEST_PAGE_SIZE)
}

pub fn activity_map_manifest_with_page_size<T>(
    items: impl IntoIterator<Item = T>,
    page_size: usize,
) -> Result<PayloadRef>
where
    T: serde::Serialize,
{
    with_context(|runtime| {
        let items = items
            .into_iter()
            .map(|item| runtime.encode_payload(&item))
            .collect::<Result<Vec<_>>>()?;
        crate::encode_activity_map_input_manifest_with_codec(
            items,
            page_size,
            runtime.payload_codec,
        )
    })
}

pub struct ActivityMapBuilder<A>
where
    A: Activity,
{
    options: ActivityOptions,
    input_manifest: Option<PayloadRef>,
    result_manifest_name: String,
    max_in_flight: usize,
    _activity: std::marker::PhantomData<A>,
}

impl<A> ActivityMapBuilder<A>
where
    A: Activity,
{
    pub fn task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.options = self.options.task_queue(task_queue);
        self
    }

    pub fn retry(mut self, retry_policy: crate::RetryPolicy) -> Self {
        self.options = self.options.retry(retry_policy);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.options = self.options.timeout(timeout);
        self
    }

    pub fn heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.options = self.options.heartbeat_timeout(timeout);
        self
    }

    pub fn input_manifest(mut self, input_manifest: PayloadRef) -> Self {
        self.input_manifest = Some(input_manifest);
        self
    }

    pub fn result_manifest(mut self, name: impl Into<String>) -> Self {
        self.result_manifest_name = name.into();
        self
    }

    pub fn max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = max_in_flight.max(1);
        self
    }

    pub fn spawn(self) -> ActivityMapSpawnFuture<A> {
        ActivityMapSpawnFuture {
            options: self.options,
            input_manifest: self.input_manifest,
            result_manifest_name: self.result_manifest_name,
            max_in_flight: self.max_in_flight,
            state: ActivityMapSpawnState::Init,
            _activity: std::marker::PhantomData,
        }
    }
}

pub struct ActivityMapSpawnFuture<A>
where
    A: Activity,
{
    options: ActivityOptions,
    input_manifest: Option<PayloadRef>,
    result_manifest_name: String,
    max_in_flight: usize,
    state: ActivityMapSpawnState,
    _activity: std::marker::PhantomData<A>,
}

impl<A> Unpin for ActivityMapSpawnFuture<A> where A: Activity {}

#[derive(Debug)]
enum ActivityMapSpawnState {
    Init,
    Done,
}

#[derive(Clone, Debug)]
pub struct ActivityMapHandle {
    command_id: CommandId,
}

impl ActivityMapHandle {
    pub fn result_manifest(&self) -> ActivityMapResultFuture {
        ActivityMapResultFuture {
            command_id: self.command_id.clone(),
            done: false,
        }
    }
}

impl<A> Future for ActivityMapSpawnFuture<A>
where
    A: Activity,
{
    type Output = Result<ActivityMapHandle>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_context(|runtime| match self.state {
            ActivityMapSpawnState::Init => self.poll_init(runtime),
            ActivityMapSpawnState::Done => Poll::Ready(Err(Error::Nondeterminism(
                "activity map spawn future polled after completion".to_owned(),
            ))),
        })
    }
}

impl<A> ActivityMapSpawnFuture<A>
where
    A: Activity,
{
    fn poll_init(&mut self, runtime: &mut RuntimeContext) -> Poll<Result<ActivityMapHandle>> {
        if runtime.peek_replay_event().is_none() && !runtime.at_replay_tail() {
            runtime.request_more_history_if_available();
            return Poll::Pending;
        }

        let command_id = runtime.next_command_id();
        let input_manifest = match self.input_manifest.clone() {
            Some(input_manifest) => input_manifest,
            None => {
                return Poll::Ready(Err(Error::Backend(
                    "activity_map requires input_manifest".to_owned(),
                )));
            }
        };
        let options = runtime.effective_activity_options(self.options.clone());
        let task_queue = options
            .task_queue
            .clone()
            .expect("effective activity options include task queue fallback");
        let retry_policy = options.effective_retry_policy();
        let fingerprint_options = ActivityOptions {
            task_queue: Some(task_queue.clone()),
            retry_policy: Some(retry_policy.clone()),
            start_to_close_timeout: options.start_to_close_timeout,
            heartbeat_timeout: options.heartbeat_timeout,
        };
        let max_in_flight = self.max_in_flight.max(1);
        let fingerprint = activity_map_fingerprint(
            A::activity_name(),
            payload_digest(&input_manifest),
            self.result_manifest_name.clone(),
            max_in_flight,
            fingerprint_options.digest()?,
        );

        if let Some(event) = runtime.peek_replay_event().cloned() {
            let HistoryEventData::ActivityMapScheduled(scheduled) = event.data else {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "expected ActivityMapScheduled for command {}, found {:?}",
                    command_id.seq.0, event.event_type
                ))));
            };
            if scheduled.command_id.seq != command_id.seq {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "expected command seq {}, found {}",
                    command_id.seq.0, scheduled.command_id.seq.0
                ))));
            }
            if scheduled.fingerprint != fingerprint {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "activity map command fingerprint changed for command {}",
                    command_id.seq.0
                ))));
            }
            runtime.advance_replay();
            self.state = ActivityMapSpawnState::Done;
            return Poll::Ready(Ok(ActivityMapHandle { command_id }));
        }

        let scheduled = ActivityMapScheduled {
            command_id: command_id.clone(),
            activity_name: A::activity_name(),
            task_queue,
            retry_policy,
            start_to_close_timeout: options.start_to_close_timeout,
            heartbeat_timeout: options.heartbeat_timeout,
            input_manifest: input_manifest.clone(),
            result_manifest_name: self.result_manifest_name.clone(),
            max_in_flight,
            fingerprint,
        };
        runtime.append_events.push(NewHistoryEvent::new(
            HistoryEventData::ActivityMapScheduled(scheduled.clone()),
        ));
        runtime.schedule_activity_maps.push(ActivityMapTask {
            map_command_id: command_id.clone(),
            activity_name: scheduled.activity_name,
            task_queue: scheduled.task_queue,
            retry_policy: scheduled.retry_policy,
            start_to_close_timeout: scheduled.start_to_close_timeout,
            heartbeat_timeout: scheduled.heartbeat_timeout,
            input_manifest,
            result_manifest_name: scheduled.result_manifest_name,
            max_in_flight,
        });
        self.state = ActivityMapSpawnState::Done;
        Poll::Ready(Ok(ActivityMapHandle { command_id }))
    }
}

impl<A> DurableJoinBranch for ActivityMapSpawnFuture<A> where A: Activity {}

pub struct ActivityMapResultFuture {
    command_id: CommandId,
    done: bool,
}

impl Unpin for ActivityMapResultFuture {}

impl Future for ActivityMapResultFuture {
    type Output = Result<PayloadRef>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_context(|runtime| {
            if self.done {
                return Poll::Ready(Err(Error::Nondeterminism(
                    "activity map result future polled after completion".to_owned(),
                )));
            }
            if let Some(completed) = runtime.take_map_completion(&self.command_id) {
                self.done = true;
                return Poll::Ready(Ok(completed.result_manifest));
            }
            if let Some(failure) = runtime.take_map_failure(&self.command_id) {
                self.done = true;
                return Poll::Ready(Err(Error::ActivityFailed(failure)));
            }
            runtime.request_more_history_if_available();
            Poll::Pending
        })
    }
}

impl DurableJoinBranch for ActivityMapResultFuture {}

pub fn child_workflow<W>(input: W::Input) -> ChildWorkflowBuilder<W>
where
    W: Workflow,
{
    ChildWorkflowBuilder {
        input: Some(input),
        workflow_id: None,
        task_queue: None,
        parent_close_policy: ParentClosePolicy::Cancel,
        _workflow: std::marker::PhantomData,
    }
}

pub struct ChildWorkflowBuilder<W>
where
    W: Workflow,
{
    input: Option<W::Input>,
    workflow_id: Option<WorkflowId>,
    task_queue: Option<TaskQueue>,
    parent_close_policy: ParentClosePolicy,
    _workflow: std::marker::PhantomData<W>,
}

impl<W> ChildWorkflowBuilder<W>
where
    W: Workflow,
{
    pub fn workflow_id(mut self, workflow_id: impl Into<String>) -> Self {
        self.workflow_id = Some(WorkflowId::new(workflow_id));
        self
    }

    pub fn task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.task_queue = Some(TaskQueue::new(task_queue));
        self
    }

    pub fn parent_close_policy(mut self, parent_close_policy: ParentClosePolicy) -> Self {
        self.parent_close_policy = parent_close_policy;
        self
    }

    pub fn spawn(self) -> ChildWorkflowSpawnFuture<W> {
        ChildWorkflowSpawnFuture {
            input: self.input,
            workflow_id: self.workflow_id,
            task_queue: self.task_queue,
            parent_close_policy: self.parent_close_policy,
            state: ChildWorkflowSpawnState::Init,
            _workflow: std::marker::PhantomData,
        }
    }
}

pub struct ChildWorkflowSpawnFuture<W>
where
    W: Workflow,
{
    input: Option<W::Input>,
    workflow_id: Option<WorkflowId>,
    task_queue: Option<TaskQueue>,
    parent_close_policy: ParentClosePolicy,
    state: ChildWorkflowSpawnState,
    _workflow: std::marker::PhantomData<W>,
}

impl<W> Unpin for ChildWorkflowSpawnFuture<W> where W: Workflow {}

#[derive(Debug)]
enum ChildWorkflowSpawnState {
    Init,
    Waiting(CommandId, WorkflowId),
    Done,
}

#[derive(Clone, Debug)]
pub struct ChildWorkflowHandle<W>
where
    W: Workflow,
{
    command_id: CommandId,
    workflow_id: WorkflowId,
    run_id: RunId,
    _workflow: std::marker::PhantomData<W>,
}

impl<W> ChildWorkflowHandle<W>
where
    W: Workflow,
{
    pub fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn result(&self) -> ChildWorkflowResultFuture<W> {
        ChildWorkflowResultFuture {
            command_id: self.command_id.clone(),
            done: false,
            _workflow: std::marker::PhantomData,
        }
    }
}

impl<W> Future for ChildWorkflowSpawnFuture<W>
where
    W: Workflow,
{
    type Output = Result<ChildWorkflowHandle<W>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_context(|runtime| match &self.state {
            ChildWorkflowSpawnState::Init => self.poll_init(runtime),
            ChildWorkflowSpawnState::Waiting(command_id, workflow_id) => {
                let command_id = command_id.clone();
                let workflow_id = workflow_id.clone();
                self.poll_waiting(runtime, &command_id, workflow_id)
            }
            ChildWorkflowSpawnState::Done => Poll::Ready(Err(Error::Nondeterminism(
                "child workflow spawn future polled after completion".to_owned(),
            ))),
        })
    }
}

impl<W> DurableSelectBranch for ChildWorkflowSpawnFuture<W>
where
    W: Workflow,
{
    fn __durust_cancel_branch(&self) {
        if let ChildWorkflowSpawnState::Waiting(command_id, _) = &self.state {
            with_context(|runtime| runtime.cancel_command(command_id.clone()));
        }
    }
}

impl<W> DurableJoinBranch for ChildWorkflowSpawnFuture<W> where W: Workflow {}

impl<W> ChildWorkflowSpawnFuture<W>
where
    W: Workflow,
{
    fn poll_init(&mut self, runtime: &mut RuntimeContext) -> Poll<Result<ChildWorkflowHandle<W>>> {
        if runtime.peek_replay_event().is_none() && !runtime.at_replay_tail() {
            runtime.request_more_history_if_available();
            return Poll::Pending;
        }

        let command_id = runtime.next_command_id();
        let Some(workflow_id) = self.workflow_id.clone() else {
            return Poll::Ready(Err(Error::Backend(
                "child workflow requires workflow_id".to_owned(),
            )));
        };
        let task_queue = self
            .task_queue
            .clone()
            .unwrap_or_else(|| runtime.worker_workflow_task_queue.clone());
        let input = self
            .input
            .as_ref()
            .expect("child workflow input exists before schedule");
        let input_ref = runtime.encode_payload(input)?;
        let fingerprint = child_workflow_fingerprint(
            W::workflow_type(),
            workflow_id.clone(),
            payload_digest(&input_ref),
            task_queue.clone(),
            self.parent_close_policy,
        );

        if let Some(event) = runtime.peek_replay_event().cloned() {
            let HistoryEventData::ChildWorkflowStartRequested(requested) = event.data else {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "expected ChildWorkflowStartRequested for command {}, found {:?}",
                    command_id.seq.0, event.event_type
                ))));
            };
            if requested.command_id.seq != command_id.seq {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "expected command seq {}, found {}",
                    command_id.seq.0, requested.command_id.seq.0
                ))));
            }
            if requested.fingerprint != fingerprint {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "child workflow command fingerprint changed for command {}",
                    command_id.seq.0
                ))));
            }
            runtime.advance_replay();
            if let Some(started) = runtime.take_child_started(&command_id) {
                self.state = ChildWorkflowSpawnState::Done;
                return Poll::Ready(Ok(ChildWorkflowHandle {
                    command_id,
                    workflow_id: started.workflow_id,
                    run_id: started.run_id,
                    _workflow: std::marker::PhantomData,
                }));
            }
            if let Some(failure) = runtime.take_child_failure(&command_id) {
                self.state = ChildWorkflowSpawnState::Done;
                return Poll::Ready(Err(Error::ChildWorkflowFailed(failure)));
            }
            self.state = ChildWorkflowSpawnState::Waiting(command_id, workflow_id);
            runtime.request_more_history_if_available();
            return Poll::Pending;
        }

        let requested = crate::ChildWorkflowStartRequested {
            command_id: command_id.clone(),
            workflow_type: W::workflow_type(),
            workflow_id: workflow_id.clone(),
            task_queue,
            input: input_ref,
            parent_close_policy: self.parent_close_policy,
            fingerprint,
        };
        runtime.append_events.push(NewHistoryEvent::new(
            HistoryEventData::ChildWorkflowStartRequested(requested.clone()),
        ));
        runtime
            .start_child_workflows
            .push(ChildStartOutboxMessage::from_requested(&requested));
        self.input = None;
        self.state = ChildWorkflowSpawnState::Waiting(command_id, workflow_id);
        Poll::Pending
    }

    fn poll_waiting(
        &mut self,
        runtime: &mut RuntimeContext,
        command_id: &CommandId,
        workflow_id: WorkflowId,
    ) -> Poll<Result<ChildWorkflowHandle<W>>> {
        if let Some(started) = runtime.take_child_started(command_id) {
            self.state = ChildWorkflowSpawnState::Done;
            return Poll::Ready(Ok(ChildWorkflowHandle {
                command_id: command_id.clone(),
                workflow_id: started.workflow_id,
                run_id: started.run_id,
                _workflow: std::marker::PhantomData,
            }));
        }
        if let Some(failure) = runtime.take_child_failure(command_id) {
            self.state = ChildWorkflowSpawnState::Done;
            return Poll::Ready(Err(Error::ChildWorkflowFailed(failure)));
        }

        self.state = ChildWorkflowSpawnState::Waiting(command_id.clone(), workflow_id);
        runtime.request_more_history_if_available();
        Poll::Pending
    }
}

pub struct ChildWorkflowResultFuture<W>
where
    W: Workflow,
{
    command_id: CommandId,
    done: bool,
    _workflow: std::marker::PhantomData<W>,
}

impl<W> Unpin for ChildWorkflowResultFuture<W> where W: Workflow {}

impl<W> Future for ChildWorkflowResultFuture<W>
where
    W: Workflow,
{
    type Output = Result<W::Output>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_context(|runtime| {
            if self.done {
                return Poll::Ready(Err(Error::Nondeterminism(
                    "child workflow result future polled after completion".to_owned(),
                )));
            }
            if let Some(completed) = runtime.take_child_completion(&self.command_id) {
                self.done = true;
                return Poll::Ready(crate::decode_payload::<W::Output>(&completed.result));
            }
            if let Some(failure) = runtime.take_child_failure(&self.command_id) {
                self.done = true;
                return Poll::Ready(Err(Error::ChildWorkflowFailed(failure)));
            }
            if let Some(reason) = runtime.take_child_cancellation(&self.command_id) {
                self.done = true;
                return Poll::Ready(Err(Error::ChildWorkflowCancelled(reason)));
            }
            runtime.request_more_history_if_available();
            Poll::Pending
        })
    }
}

impl<W> DurableSelectBranch for ChildWorkflowResultFuture<W>
where
    W: Workflow,
{
    fn __durust_cancel_branch(&self) {}
}

impl<W> DurableJoinBranch for ChildWorkflowResultFuture<W> where W: Workflow {}

pub fn sleep(duration: Duration) -> TimerFuture {
    TimerFuture {
        timer: TimerSpec::After(duration),
        state: TimerFutureState::Init,
    }
}

pub fn sleep_until(deadline: SystemTime) -> TimerFuture {
    TimerFuture {
        timer: TimerSpec::At(system_time_to_timestamp(deadline)),
        state: TimerFutureState::Init,
    }
}

pub struct TimerFuture {
    timer: TimerSpec,
    state: TimerFutureState,
}

impl Unpin for TimerFuture {}

#[derive(Clone, Copy, Debug)]
enum TimerSpec {
    After(Duration),
    At(TimestampMs),
}

#[derive(Debug)]
enum TimerFutureState {
    Init,
    Waiting(CommandId),
    Done,
}

impl Future for TimerFuture {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_context(|runtime| match &self.state {
            TimerFutureState::Init => self.poll_init(runtime),
            TimerFutureState::Waiting(command_id) => {
                let command_id = command_id.clone();
                self.poll_waiting(runtime, &command_id)
            }
            TimerFutureState::Done => Poll::Ready(Err(Error::Nondeterminism(
                "timer future polled after completion".to_owned(),
            ))),
        })
    }
}

impl DurableSelectBranch for TimerFuture {
    fn __durust_cancel_branch(&self) {
        if let TimerFutureState::Waiting(command_id) = &self.state {
            with_context(|runtime| runtime.delete_waits.push(timer_wait_id(command_id)));
        }
    }
}

impl DurableJoinBranch for TimerFuture {}

impl TimerFuture {
    fn poll_init(&mut self, runtime: &mut RuntimeContext) -> Poll<Result<()>> {
        if runtime.peek_replay_event().is_none() && !runtime.at_replay_tail() {
            runtime.request_more_history_if_available();
            return Poll::Pending;
        }

        let command_id = runtime.next_command_id();
        let (fingerprint, fire_at) = self.timer.fingerprint_and_fire_at(runtime.now);

        if let Some(event) = runtime.peek_replay_event().cloned() {
            let HistoryEventData::TimerStarted(started) = event.data else {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "expected TimerStarted for command {}, found {:?}",
                    command_id.seq.0, event.event_type
                ))));
            };
            if started.command_id.seq != command_id.seq {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "expected command seq {}, found {}",
                    command_id.seq.0, started.command_id.seq.0
                ))));
            }
            if started.fingerprint != fingerprint {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "timer command fingerprint changed for command {}",
                    command_id.seq.0
                ))));
            }
            runtime.advance_replay();

            if runtime.take_timer(&command_id).is_some() {
                self.state = TimerFutureState::Done;
                return Poll::Ready(Ok(()));
            }

            self.state = TimerFutureState::Waiting(command_id);
            runtime.request_more_history_if_available();
            return Poll::Pending;
        }

        let started = TimerStarted {
            command_id: command_id.clone(),
            fire_at,
            fingerprint,
        };
        runtime
            .append_events
            .push(NewHistoryEvent::new(HistoryEventData::TimerStarted(
                started,
            )));
        runtime.upsert_waits.push(WaitRecord {
            wait_id: timer_wait_id(&command_id),
            run_id: runtime.run_id.clone(),
            command_id: command_id.clone(),
            kind: WaitKind::Timer,
            key: "timer".to_owned(),
            ready_at: Some(fire_at),
        });
        self.state = TimerFutureState::Waiting(command_id);
        Poll::Pending
    }

    fn poll_waiting(
        &mut self,
        runtime: &mut RuntimeContext,
        command_id: &CommandId,
    ) -> Poll<Result<()>> {
        if runtime.take_timer(command_id).is_some() {
            self.state = TimerFutureState::Done;
            return Poll::Ready(Ok(()));
        }

        runtime.request_more_history_if_available();
        Poll::Pending
    }
}

impl TimerSpec {
    fn fingerprint_and_fire_at(self, now: TimestampMs) -> (crate::CommandFingerprint, TimestampMs) {
        match self {
            TimerSpec::After(duration) => {
                let duration_ms = duration_millis_i64(duration);
                (
                    timer_fingerprint("sleep", TimestampMs(duration_ms)),
                    TimestampMs(now.0.saturating_add(duration_ms)),
                )
            }
            TimerSpec::At(deadline) => (timer_fingerprint("sleep_until", deadline), deadline),
        }
    }
}

pub fn signal<T>(signal_name: impl Into<String>) -> SignalFuture<T> {
    SignalFuture {
        signal_name: SignalName::new(signal_name),
        state: SignalFutureState::Init,
        _output: std::marker::PhantomData,
    }
}

pub struct SignalFuture<T> {
    signal_name: SignalName,
    state: SignalFutureState,
    _output: std::marker::PhantomData<T>,
}

impl<T> Unpin for SignalFuture<T> {}

#[derive(Debug)]
enum SignalFutureState {
    Init,
    Waiting(CommandId),
    Done,
}

impl<T> Future for SignalFuture<T>
where
    T: serde::de::DeserializeOwned,
{
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_context(|runtime| match &self.state {
            SignalFutureState::Init => self.poll_init(runtime),
            SignalFutureState::Waiting(command_id) => {
                let command_id = command_id.clone();
                self.poll_waiting(runtime, &command_id)
            }
            SignalFutureState::Done => Poll::Ready(Err(Error::Nondeterminism(
                "signal future polled after completion".to_owned(),
            ))),
        })
    }
}

impl<T> DurableSelectBranch for SignalFuture<T>
where
    T: serde::de::DeserializeOwned,
{
    fn __durust_cancel_branch(&self) {
        if let SignalFutureState::Waiting(command_id) = &self.state {
            with_context(|runtime| runtime.delete_waits.push(signal_wait_id(command_id)));
        }
    }
}

impl<T> DurableJoinBranch for SignalFuture<T> where T: serde::de::DeserializeOwned {}

impl<T> SignalFuture<T>
where
    T: serde::de::DeserializeOwned,
{
    fn poll_init(&mut self, runtime: &mut RuntimeContext) -> Poll<Result<T>> {
        if runtime.peek_replay_event().is_none() && !runtime.at_replay_tail() {
            runtime.request_more_history_if_available();
            return Poll::Pending;
        }

        let command_id = runtime.next_command_id();
        let fingerprint = signal_fingerprint(self.signal_name.clone());

        if let Some(consumed) = runtime.take_consumed_signal(&command_id) {
            self.state = SignalFutureState::Done;
            return Poll::Ready(decode_consumed_signal(
                command_id.seq,
                &fingerprint,
                consumed,
            ));
        }

        if let Some(event) = runtime.peek_replay_event().cloned() {
            if let HistoryEventData::SignalConsumed(consumed) = event.data {
                if consumed.command_id.seq != command_id.seq {
                    return Poll::Ready(Err(Error::Nondeterminism(format!(
                        "expected command seq {}, found {}",
                        command_id.seq.0, consumed.command_id.seq.0
                    ))));
                }
                self.state = SignalFutureState::Waiting(command_id);
                runtime.request_more_history_if_available();
                return Poll::Pending;
            }

            self.register_wait(runtime, &command_id);
            runtime.request_more_history_if_available();
            self.state = SignalFutureState::Waiting(command_id);
            return Poll::Pending;
        }

        self.register_wait(runtime, &command_id);
        self.state = SignalFutureState::Waiting(command_id);
        Poll::Pending
    }

    fn poll_waiting(
        &mut self,
        runtime: &mut RuntimeContext,
        command_id: &CommandId,
    ) -> Poll<Result<T>> {
        let fingerprint = signal_fingerprint(self.signal_name.clone());
        if let Some(consumed) = runtime.take_consumed_signal(command_id) {
            self.state = SignalFutureState::Done;
            return Poll::Ready(decode_consumed_signal(
                command_id.seq,
                &fingerprint,
                consumed,
            ));
        }
        if let Some(event) = runtime.peek_replay_event().cloned() {
            if let HistoryEventData::SignalConsumed(consumed) = event.data {
                if consumed.command_id.seq == command_id.seq {
                    runtime.request_more_history_if_available();
                    return Poll::Pending;
                }
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "expected command seq {}, found {}",
                    command_id.seq.0, consumed.command_id.seq.0
                ))));
            }
        }
        if let Some(signal) = runtime.take_live_signal(command_id) {
            runtime.consume_signals.push(signal.signal_id.clone());
            runtime.delete_waits.push(signal_wait_id(command_id));
            runtime
                .append_events
                .push(NewHistoryEvent::new(HistoryEventData::SignalConsumed(
                    SignalConsumed {
                        command_id: command_id.clone(),
                        signal_id: signal.signal_id,
                        signal_name: signal.signal_name,
                        payload: signal.payload.clone(),
                        fingerprint,
                    },
                )));
            runtime.record_next_appended_ready_event_id();
            self.state = SignalFutureState::Done;
            return Poll::Ready(crate::decode_payload::<T>(&signal.payload));
        }

        runtime.request_signal(command_id.clone(), self.signal_name.clone());
        runtime.request_more_history_if_available();
        Poll::Pending
    }

    fn register_wait(&self, runtime: &mut RuntimeContext, command_id: &CommandId) {
        runtime.upsert_waits.push(WaitRecord {
            wait_id: signal_wait_id(command_id),
            run_id: runtime.run_id.clone(),
            command_id: command_id.clone(),
            kind: WaitKind::Signal,
            key: self.signal_name.0.clone(),
            ready_at: None,
        });
        runtime.request_signal(command_id.clone(), self.signal_name.clone());
    }
}

fn decode_consumed_signal<T>(
    command_seq: CommandSeq,
    fingerprint: &CommandFingerprint,
    consumed: SignalConsumed,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    if consumed.fingerprint != *fingerprint {
        return Err(Error::Nondeterminism(format!(
            "signal command fingerprint changed for command {}",
            command_seq.0
        )));
    }
    crate::decode_payload::<T>(&consumed.payload)
}

fn collect_completions(
    events: &[HistoryEvent],
) -> BTreeMap<CommandSeq, (crate::EventId, PayloadRef)> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ActivityCompleted(completed) => Some((
                completed.command_id.seq,
                (event.event_id, completed.result.clone()),
            )),
            _ => None,
        })
        .collect()
}

fn collect_failures(
    events: &[HistoryEvent],
) -> BTreeMap<CommandSeq, (crate::EventId, ActivityTerminalError)> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ActivityFailed(failed) => Some((
                failed.command_id.seq,
                (
                    event.event_id,
                    ActivityTerminalError::Failed(failed.failure.clone()),
                ),
            )),
            HistoryEventData::ActivityTimedOut(timed_out) => Some((
                timed_out.command_id.seq,
                (
                    event.event_id,
                    ActivityTerminalError::TimedOut(timed_out.message.clone()),
                ),
            )),
            _ => None,
        })
        .collect()
}

fn collect_map_completions(
    events: &[HistoryEvent],
) -> BTreeMap<CommandSeq, (crate::EventId, ActivityMapCompleted)> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ActivityMapCompleted(completed) => Some((
                completed.command_id.seq,
                (event.event_id, completed.clone()),
            )),
            _ => None,
        })
        .collect()
}

fn collect_map_failures(
    events: &[HistoryEvent],
) -> BTreeMap<CommandSeq, (crate::EventId, crate::DurableFailure)> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ActivityMapFailed(failed) => Some((
                failed.command_id.seq,
                (event.event_id, failed.failure.clone()),
            )),
            _ => None,
        })
        .collect()
}

fn collect_child_starts(
    events: &[HistoryEvent],
) -> BTreeMap<CommandSeq, (crate::EventId, ChildWorkflowStarted)> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowStarted(started) => {
                Some((started.command_id.seq, (event.event_id, started.clone())))
            }
            _ => None,
        })
        .collect()
}

fn collect_child_completions(
    events: &[HistoryEvent],
) -> BTreeMap<CommandSeq, (crate::EventId, ChildWorkflowCompleted)> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowCompleted(completed) => Some((
                completed.command_id.seq,
                (event.event_id, completed.clone()),
            )),
            _ => None,
        })
        .collect()
}

fn collect_child_failures(
    events: &[HistoryEvent],
) -> BTreeMap<CommandSeq, (crate::EventId, crate::DurableFailure)> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowFailed(failed) => Some((
                failed.command_id.seq,
                (event.event_id, failed.failure.clone()),
            )),
            _ => None,
        })
        .collect()
}

fn collect_child_cancellations(
    events: &[HistoryEvent],
) -> BTreeMap<CommandSeq, (crate::EventId, String)> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowCancelled(cancelled) => Some((
                cancelled.command_id.seq,
                (event.event_id, cancelled.reason.clone()),
            )),
            _ => None,
        })
        .collect()
}

fn collect_timers(events: &[HistoryEvent]) -> BTreeMap<CommandSeq, (crate::EventId, TimerFired)> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::TimerFired(fired) => {
                Some((fired.command_id.seq, (event.event_id, fired.clone())))
            }
            _ => None,
        })
        .collect()
}

fn collect_consumed_signals(
    events: &[HistoryEvent],
) -> BTreeMap<CommandSeq, (crate::EventId, SignalConsumed)> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::SignalConsumed(consumed) => {
                Some((consumed.command_id.seq, (event.event_id, consumed.clone())))
            }
            _ => None,
        })
        .collect()
}

pub(crate) fn is_terminal(data: &HistoryEventData) -> bool {
    matches!(
        data,
        HistoryEventData::WorkflowCompleted { .. }
            | HistoryEventData::WorkflowFailed { .. }
            | HistoryEventData::WorkflowCancelled { .. }
            | HistoryEventData::WorkflowContinuedAsNew { .. }
    )
}

pub(crate) fn event_payload_len(data: &HistoryEventData) -> usize {
    match data {
        HistoryEventData::WorkflowStarted { input, .. } => input.encoded_len(),
        HistoryEventData::WorkflowCompleted { result } => result.encoded_len(),
        HistoryEventData::WorkflowFailed { failure } => event_failure_len(failure),
        HistoryEventData::WorkflowCancelled { reason } => reason.len(),
        HistoryEventData::WorkflowContinuedAsNew { input } => input.encoded_len(),
        HistoryEventData::WorkflowTaskStarted => 0,
        HistoryEventData::ActivityScheduled(scheduled) => scheduled.input.encoded_len(),
        HistoryEventData::ActivityMapScheduled(scheduled) => scheduled.input_manifest.encoded_len(),
        HistoryEventData::ActivityMapCompleted(completed) => {
            completed.result_manifest.encoded_len()
        }
        HistoryEventData::ActivityMapFailed(failed) => event_failure_len(&failed.failure),
        HistoryEventData::ActivityCompleted(completed) => completed.result.encoded_len(),
        HistoryEventData::ActivityFailed(failed) => event_failure_len(&failed.failure),
        HistoryEventData::ActivityTimedOut(timed_out) => timed_out.message.len(),
        HistoryEventData::ChildWorkflowStartRequested(requested) => {
            requested.workflow_type.name.len()
                + requested.workflow_id.0.len()
                + requested.task_queue.0.len()
                + requested.input.encoded_len()
                + requested.fingerprint.options_digest.len()
        }
        HistoryEventData::ChildWorkflowStarted(started) => {
            started.workflow_id.0.len() + started.run_id.0.len()
        }
        HistoryEventData::ChildWorkflowCompleted(completed) => completed.result.encoded_len(),
        HistoryEventData::ChildWorkflowFailed(failed) => event_failure_len(&failed.failure),
        HistoryEventData::ChildWorkflowCancelled(cancelled) => cancelled.reason.len(),
        HistoryEventData::TimerStarted(_) | HistoryEventData::TimerFired(_) => 16,
        HistoryEventData::SignalConsumed(signal) => signal.payload.encoded_len(),
        HistoryEventData::SelectWinner(winner) => winner.branches_digest.len() + 32,
        HistoryEventData::VersionMarker(marker) => marker.change_id.len() + 16,
        HistoryEventData::DeprecatedPatchMarker(marker) => marker.patch_id.len() + 16,
        HistoryEventData::SideEffectMarker(marker) => marker.key.len() + marker.value.encoded_len(),
    }
}

fn event_failure_len(failure: &crate::DurableFailure) -> usize {
    failure.error_type.len()
        + failure.message.len()
        + usize::from(failure.non_retryable)
        + failure
            .details
            .as_ref()
            .map(PayloadRef::encoded_len)
            .unwrap_or_default()
}

fn timer_wait_id(command_id: &CommandId) -> WaitId {
    WaitId::new(format!("{}:{}:timer", command_id.run_id, command_id.seq.0))
}

fn signal_wait_id(command_id: &CommandId) -> WaitId {
    WaitId::new(format!("{}:{}:signal", command_id.run_id, command_id.seq.0))
}

fn system_time_to_timestamp(value: SystemTime) -> TimestampMs {
    TimestampMs(
        i64::try_from(
            value
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX),
    )
}

fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ActivityCompleted, ActivityFailed, ActivityMapCompleted, ActivityMapFailed,
        ActivityTimedOut, ChildWorkflowCancelled, ChildWorkflowCompleted, ChildWorkflowFailed,
        ChildWorkflowStarted, CodecId, DurableFailure, EventId, HistoryEventType, TimerStarted,
    };

    #[test]
    fn indexed_ready_events_are_skipped_when_consumed_before_the_replay_cursor() {
        assert_indexed_ready_event_skips(
            "activity_completed",
            |command_id| {
                HistoryEventData::ActivityCompleted(ActivityCompleted {
                    command_id,
                    result: payload(&22_u64),
                })
            },
            |runtime, command_id| runtime.take_completion(command_id).is_some(),
        );
        assert_indexed_ready_event_skips(
            "activity_failed",
            |command_id| {
                HistoryEventData::ActivityFailed(ActivityFailed {
                    command_id,
                    failure: failure("boom"),
                })
            },
            |runtime, command_id| runtime.take_failure(command_id).is_some(),
        );
        assert_indexed_ready_event_skips(
            "activity_timed_out",
            |command_id| {
                HistoryEventData::ActivityTimedOut(ActivityTimedOut {
                    command_id,
                    message: "timeout".to_owned(),
                })
            },
            |runtime, command_id| runtime.take_failure(command_id).is_some(),
        );
        assert_indexed_ready_event_skips(
            "activity_map_completed",
            |command_id| {
                HistoryEventData::ActivityMapCompleted(ActivityMapCompleted {
                    command_id,
                    result_manifest: payload(&"map-result"),
                    item_count: 2,
                    success_count: 2,
                    failure_count: 0,
                })
            },
            |runtime, command_id| runtime.take_map_completion(command_id).is_some(),
        );
        assert_indexed_ready_event_skips(
            "activity_map_failed",
            |command_id| {
                HistoryEventData::ActivityMapFailed(ActivityMapFailed {
                    command_id,
                    failure: failure("map failed"),
                })
            },
            |runtime, command_id| runtime.take_map_failure(command_id).is_some(),
        );
        assert_indexed_ready_event_skips(
            "child_workflow_started",
            |command_id| {
                HistoryEventData::ChildWorkflowStarted(ChildWorkflowStarted {
                    command_id,
                    workflow_id: WorkflowId::new("wf/child"),
                    run_id: RunId::new("run/child"),
                })
            },
            |runtime, command_id| runtime.take_child_started(command_id).is_some(),
        );
        assert_indexed_ready_event_skips(
            "child_workflow_completed",
            |command_id| {
                HistoryEventData::ChildWorkflowCompleted(ChildWorkflowCompleted {
                    command_id,
                    result: payload(&44_u64),
                })
            },
            |runtime, command_id| runtime.take_child_completion(command_id).is_some(),
        );
        assert_indexed_ready_event_skips(
            "child_workflow_failed",
            |command_id| {
                HistoryEventData::ChildWorkflowFailed(ChildWorkflowFailed {
                    command_id,
                    failure: failure("child failed"),
                })
            },
            |runtime, command_id| runtime.take_child_failure(command_id).is_some(),
        );
        assert_indexed_ready_event_skips(
            "child_workflow_cancelled",
            |command_id| {
                HistoryEventData::ChildWorkflowCancelled(ChildWorkflowCancelled {
                    command_id,
                    reason: "cancelled".to_owned(),
                })
            },
            |runtime, command_id| runtime.take_child_cancellation(command_id).is_some(),
        );
        assert_indexed_ready_event_skips(
            "timer_fired",
            |command_id| {
                HistoryEventData::TimerFired(TimerFired {
                    command_id,
                    fired_at: TimestampMs(10),
                })
            },
            |runtime, command_id| runtime.take_timer(command_id).is_some(),
        );
        assert_indexed_ready_event_skips(
            "signal_consumed",
            |command_id| {
                HistoryEventData::SignalConsumed(SignalConsumed {
                    command_id,
                    signal_id: SignalId::new("signal/1"),
                    signal_name: SignalName::new("ready"),
                    payload: payload(&"ready"),
                    fingerprint: signal_fingerprint(SignalName::new("ready")),
                })
            },
            |runtime, command_id| runtime.take_consumed_signal(command_id).is_some(),
        );
    }

    fn assert_indexed_ready_event_skips(
        case_name: &str,
        indexed_event: impl FnOnce(CommandId) -> HistoryEventData,
        consume_indexed: impl FnOnce(&mut RuntimeContext, &CommandId) -> bool,
    ) {
        let run_id = RunId::new(format!("run/{case_name}"));
        let first_command_id = command_id(&run_id, 1);
        let indexed_command_id = command_id(&run_id, 2);
        let after_skipped_command_id = command_id(&run_id, 3);
        let first_event = HistoryEventData::ActivityCompleted(ActivityCompleted {
            command_id: first_command_id.clone(),
            result: payload(&11_u64),
        });
        let after_skipped = HistoryEventData::TimerStarted(TimerStarted {
            command_id: after_skipped_command_id,
            fire_at: TimestampMs(10),
            fingerprint: timer_fingerprint("sleep", TimestampMs(10)),
        });
        let mut runtime = runtime_with_history(
            run_id,
            vec![
                event(1, first_event),
                event(2, indexed_event(indexed_command_id.clone())),
                event(3, after_skipped),
            ],
        );

        assert!(
            consume_indexed(&mut runtime, &indexed_command_id),
            "{case_name} indexed event should be consumable before the cursor reaches it"
        );
        assert!(
            runtime.take_completion(&first_command_id).is_some(),
            "{case_name} should still consume the in-cursor event"
        );

        let next = runtime
            .peek_replay_event()
            .unwrap_or_else(|| panic!("{case_name} should skip the consumed indexed event"));
        assert!(
            matches!(next.data, HistoryEventData::TimerStarted(_)),
            "{case_name} should skip consumed ready event and continue at the next unconsumed event, found {:?}",
            next.event_type
        );
        assert_eq!(next.event_id, EventId(3));
    }

    fn runtime_with_history(run_id: RunId, events: Vec<HistoryEvent>) -> RuntimeContext {
        RuntimeContext::new(
            run_id,
            TaskQueue::new("workflows"),
            TaskQueue::new("activities"),
            CodecId::MessagePack,
            TimestampMs(0),
            events,
            ActivityOptions::default(),
            0,
            EventId(3),
            EventId(3),
            Vec::new(),
        )
    }

    fn event(event_id: u64, data: HistoryEventData) -> HistoryEvent {
        let event_type: HistoryEventType = data.event_type();
        HistoryEvent {
            event_id: EventId(event_id),
            event_type,
            data,
        }
    }

    fn payload<T: serde::Serialize + ?Sized>(value: &T) -> PayloadRef {
        crate::encode_payload(value).expect("test payload should encode")
    }

    fn failure(message: &str) -> DurableFailure {
        DurableFailure::new("tests.failure", message)
    }
}
