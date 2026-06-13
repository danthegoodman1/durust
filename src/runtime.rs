use crate::{
    Activity, ActivityMapCompleted, ActivityMapScheduled, ActivityMapTask, ActivityOptions,
    ActivityScheduled, ActivityTask, CommandId, CommandSeq, Error, HistoryEvent, HistoryEventData,
    NewHistoryEvent, PayloadRef, Result, RunId, SignalConsumed, SignalId, SignalName, TaskQueue,
    TimerFired, TimerStarted, TimestampMs, WaitId, WaitKind, WaitRecord, activity_fingerprint,
    activity_map_fingerprint, command_id, encode_activity_map_input_manifest, payload_digest,
    signal_fingerprint, timer_fingerprint,
};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

thread_local! {
    static CURRENT_CONTEXT: Cell<*mut RuntimeContext> = const { Cell::new(std::ptr::null_mut()) };
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

#[derive(Debug)]
pub(crate) struct RuntimeContext {
    run_id: RunId,
    worker_activity_task_queue: TaskQueue,
    default_activity_options: ActivityOptions,
    now: TimestampMs,
    replay_events: Vec<HistoryEvent>,
    replay_cursor: usize,
    last_loaded_event_id: crate::EventId,
    replay_target_event_id: crate::EventId,
    needs_more_history: bool,
    next_command_seq: u64,
    completions: BTreeMap<CommandSeq, PayloadRef>,
    failures: BTreeMap<CommandSeq, ActivityTerminalError>,
    map_completions: BTreeMap<CommandSeq, ActivityMapCompleted>,
    map_failures: BTreeMap<CommandSeq, String>,
    timers: BTreeMap<CommandSeq, TimerFired>,
    live_signals: BTreeMap<CommandSeq, SignalInboxRecordForRuntime>,
    signal_requests: Vec<LiveSignalRequest>,
    append_events: Vec<NewHistoryEvent>,
    upsert_waits: Vec<WaitRecord>,
    schedule_activities: Vec<ActivityTask>,
    schedule_activity_maps: Vec<ActivityMapTask>,
    consume_signals: Vec<SignalId>,
    delete_waits: Vec<WaitId>,
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
    pub consume_signals: Vec<SignalId>,
    pub delete_waits: Vec<WaitId>,
    pub default_activity_options: ActivityOptions,
}

#[derive(Clone, Debug)]
enum ActivityTerminalError {
    Failed(String),
    TimedOut(String),
}

impl ActivityTerminalError {
    fn into_error(self) -> Error {
        match self {
            Self::Failed(message) => Error::ActivityFailed(message),
            Self::TimedOut(message) => Error::ActivityTimedOut(message),
        }
    }
}

impl RuntimeContext {
    pub(crate) fn new(
        run_id: RunId,
        default_activity_task_queue: TaskQueue,
        now: TimestampMs,
        replay_events: Vec<HistoryEvent>,
        default_activity_options: ActivityOptions,
        next_command_seq: u64,
        last_loaded_event_id: crate::EventId,
        replay_target_event_id: crate::EventId,
    ) -> Self {
        let completions = collect_completions(&replay_events);
        let failures = collect_failures(&replay_events);
        let map_completions = collect_map_completions(&replay_events);
        let map_failures = collect_map_failures(&replay_events);
        let timers = collect_timers(&replay_events);

        Self {
            run_id,
            worker_activity_task_queue: default_activity_task_queue,
            default_activity_options,
            now,
            replay_events,
            replay_cursor: 0,
            last_loaded_event_id,
            replay_target_event_id,
            needs_more_history: false,
            next_command_seq,
            completions,
            failures,
            map_completions,
            map_failures,
            timers,
            live_signals: BTreeMap::new(),
            signal_requests: Vec::new(),
            append_events: Vec::new(),
            upsert_waits: Vec::new(),
            schedule_activities: Vec::new(),
            schedule_activity_maps: Vec::new(),
            consume_signals: Vec::new(),
            delete_waits: Vec::new(),
        }
    }

    pub(crate) fn into_commit_parts(self) -> RuntimeCommitParts {
        RuntimeCommitParts {
            append_events: self.append_events,
            upsert_waits: self.upsert_waits,
            schedule_activities: self.schedule_activities,
            schedule_activity_maps: self.schedule_activity_maps,
            consume_signals: self.consume_signals,
            delete_waits: self.delete_waits,
            default_activity_options: self.default_activity_options,
        }
    }

    pub(crate) fn next_command_seq(&self) -> u64 {
        self.next_command_seq
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
        self.timers.extend(collect_timers(&events));
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

    fn next_command_id(&mut self) -> CommandId {
        self.next_command_seq += 1;
        command_id(&self.run_id, self.next_command_seq)
    }

    fn peek_replay_event(&self) -> Option<&HistoryEvent> {
        self.replay_events.get(self.replay_cursor)
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

    fn take_completion(&mut self, command_id: &CommandId) -> Option<PayloadRef> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::ActivityCompleted(completed) = event.data {
                if completed.command_id.seq == command_id.seq {
                    self.advance_replay();
                    self.completions.remove(&command_id.seq);
                    return Some(completed.result);
                }
            }
        }
        self.completions.remove(&command_id.seq)
    }

    fn take_failure(&mut self, command_id: &CommandId) -> Option<ActivityTerminalError> {
        if let Some(event) = self.peek_replay_event().cloned() {
            match event.data {
                HistoryEventData::ActivityFailed(failed)
                    if failed.command_id.seq == command_id.seq =>
                {
                    self.advance_replay();
                    self.failures.remove(&command_id.seq);
                    return Some(ActivityTerminalError::Failed(failed.message));
                }
                HistoryEventData::ActivityTimedOut(timed_out)
                    if timed_out.command_id.seq == command_id.seq =>
                {
                    self.advance_replay();
                    self.failures.remove(&command_id.seq);
                    return Some(ActivityTerminalError::TimedOut(timed_out.message));
                }
                _ => {}
            }
        }
        self.failures.remove(&command_id.seq)
    }

    fn take_timer(&mut self, command_id: &CommandId) -> Option<TimerFired> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::TimerFired(fired) = event.data {
                if fired.command_id.seq == command_id.seq {
                    self.advance_replay();
                    self.timers.remove(&command_id.seq);
                    return Some(fired);
                }
            }
        }
        self.timers.remove(&command_id.seq)
    }

    fn take_map_completion(&mut self, command_id: &CommandId) -> Option<ActivityMapCompleted> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::ActivityMapCompleted(completed) = event.data {
                if completed.command_id.seq == command_id.seq {
                    self.advance_replay();
                    self.map_completions.remove(&command_id.seq);
                    return Some(completed);
                }
            }
        }
        self.map_completions.remove(&command_id.seq)
    }

    fn take_map_failure(&mut self, command_id: &CommandId) -> Option<String> {
        if let Some(event) = self.peek_replay_event().cloned() {
            if let HistoryEventData::ActivityMapFailed(failed) = event.data {
                if failed.command_id.seq == command_id.seq {
                    self.advance_replay();
                    self.map_failures.remove(&command_id.seq);
                    return Some(failed.message);
                }
            }
        }
        self.map_failures.remove(&command_id.seq)
    }

    fn take_live_signal(&mut self, command_id: &CommandId) -> Option<SignalInboxRecordForRuntime> {
        self.live_signals.remove(&command_id.seq)
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

impl<A> ActivityFuture<A>
where
    A: Activity,
{
    fn poll_init(&mut self, runtime: &mut RuntimeContext) -> Poll<Result<A::Output>> {
        if runtime.peek_replay_event().is_none() && !runtime.at_replay_tail() {
            runtime.request_more_history_if_available();
            return Poll::Pending;
        }

        let command_id = runtime.next_command_id();
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
        };
        let input = self
            .input
            .as_ref()
            .expect("activity input exists before schedule");
        let input_ref = crate::encode_payload(input)?;
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

            if let Some(next) = runtime.peek_replay_event().cloned() {
                match next.data {
                    HistoryEventData::ActivityCompleted(completed)
                        if completed.command_id.seq == command_id.seq =>
                    {
                        runtime.advance_replay();
                        self.state = ActivityFutureState::Done;
                        return Poll::Ready(crate::decode_payload::<A::Output>(&completed.result));
                    }
                    HistoryEventData::ActivityFailed(failed)
                        if failed.command_id.seq == command_id.seq =>
                    {
                        runtime.advance_replay();
                        self.state = ActivityFutureState::Done;
                        return Poll::Ready(Err(Error::ActivityFailed(failed.message)));
                    }
                    HistoryEventData::ActivityTimedOut(timed_out)
                        if timed_out.command_id.seq == command_id.seq =>
                    {
                        runtime.advance_replay();
                        self.state = ActivityFutureState::Done;
                        return Poll::Ready(Err(Error::ActivityTimedOut(timed_out.message)));
                    }
                    _ => {}
                }
            }

            self.state = ActivityFutureState::Waiting(command_id);
            runtime.request_more_history_if_available();
            return Poll::Pending;
        }

        let scheduled = ActivityScheduled {
            command_id: command_id.clone(),
            activity_name: A::activity_name(),
            task_queue,
            retry_policy,
            start_to_close_timeout: options.start_to_close_timeout,
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
        self.input = None;
        self.state = ActivityFutureState::Waiting(command_id);
        Poll::Pending
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
    let items = items
        .into_iter()
        .map(|item| crate::encode_payload(&item))
        .collect::<Result<Vec<_>>>()?;
    encode_activity_map_input_manifest(items, page_size)
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
            input_manifest,
            result_manifest_name: scheduled.result_manifest_name,
            max_in_flight,
        });
        self.state = ActivityMapSpawnState::Done;
        Poll::Ready(Ok(ActivityMapHandle { command_id }))
    }
}

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
            if let Some(message) = runtime.take_map_failure(&self.command_id) {
                self.done = true;
                return Poll::Ready(Err(Error::ActivityFailed(message)));
            }
            runtime.request_more_history_if_available();
            Poll::Pending
        })
    }
}

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

            if let Some(next) = runtime.peek_replay_event().cloned() {
                if let HistoryEventData::TimerFired(fired) = next.data {
                    if fired.command_id.seq == command_id.seq {
                        runtime.advance_replay();
                        self.state = TimerFutureState::Done;
                        return Poll::Ready(Ok(()));
                    }
                }
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

        if let Some(event) = runtime.peek_replay_event().cloned() {
            let HistoryEventData::SignalConsumed(consumed) = event.data else {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "expected SignalConsumed for command {}, found {:?}",
                    command_id.seq.0, event.event_type
                ))));
            };
            if consumed.command_id.seq != command_id.seq {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "expected command seq {}, found {}",
                    command_id.seq.0, consumed.command_id.seq.0
                ))));
            }
            if consumed.fingerprint != fingerprint {
                return Poll::Ready(Err(Error::Nondeterminism(format!(
                    "signal command fingerprint changed for command {}",
                    command_id.seq.0
                ))));
            }
            runtime.advance_replay();
            self.state = SignalFutureState::Done;
            return Poll::Ready(crate::decode_payload::<T>(&consumed.payload));
        }

        runtime.upsert_waits.push(WaitRecord {
            wait_id: signal_wait_id(&command_id),
            run_id: runtime.run_id.clone(),
            command_id: command_id.clone(),
            kind: WaitKind::Signal,
            key: self.signal_name.0.clone(),
            ready_at: None,
        });
        runtime.request_signal(command_id.clone(), self.signal_name.clone());
        self.state = SignalFutureState::Waiting(command_id);
        Poll::Pending
    }

    fn poll_waiting(
        &mut self,
        runtime: &mut RuntimeContext,
        command_id: &CommandId,
    ) -> Poll<Result<T>> {
        if let Some(signal) = runtime.take_live_signal(command_id) {
            let fingerprint = signal_fingerprint(signal.signal_name.clone());
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
            self.state = SignalFutureState::Done;
            return Poll::Ready(crate::decode_payload::<T>(&signal.payload));
        }

        runtime.request_signal(command_id.clone(), self.signal_name.clone());
        runtime.request_more_history_if_available();
        Poll::Pending
    }
}

fn collect_completions(events: &[HistoryEvent]) -> BTreeMap<CommandSeq, PayloadRef> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ActivityCompleted(completed) => {
                Some((completed.command_id.seq, completed.result.clone()))
            }
            _ => None,
        })
        .collect()
}

fn collect_failures(events: &[HistoryEvent]) -> BTreeMap<CommandSeq, ActivityTerminalError> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ActivityFailed(failed) => Some((
                failed.command_id.seq,
                ActivityTerminalError::Failed(failed.message.clone()),
            )),
            HistoryEventData::ActivityTimedOut(timed_out) => Some((
                timed_out.command_id.seq,
                ActivityTerminalError::TimedOut(timed_out.message.clone()),
            )),
            _ => None,
        })
        .collect()
}

fn collect_map_completions(events: &[HistoryEvent]) -> BTreeMap<CommandSeq, ActivityMapCompleted> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ActivityMapCompleted(completed) => {
                Some((completed.command_id.seq, completed.clone()))
            }
            _ => None,
        })
        .collect()
}

fn collect_map_failures(events: &[HistoryEvent]) -> BTreeMap<CommandSeq, String> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::ActivityMapFailed(failed) => {
                Some((failed.command_id.seq, failed.message.clone()))
            }
            _ => None,
        })
        .collect()
}

fn collect_timers(events: &[HistoryEvent]) -> BTreeMap<CommandSeq, TimerFired> {
    events
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::TimerFired(fired) => Some((fired.command_id.seq, fired.clone())),
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
    )
}

pub(crate) fn event_payload_len(data: &HistoryEventData) -> usize {
    match data {
        HistoryEventData::WorkflowStarted { input, .. } => input.encoded_len(),
        HistoryEventData::WorkflowCompleted { result } => result.encoded_len(),
        HistoryEventData::WorkflowFailed { message } => message.len(),
        HistoryEventData::WorkflowCancelled { reason } => reason.len(),
        HistoryEventData::WorkflowTaskStarted => 0,
        HistoryEventData::ActivityScheduled(scheduled) => scheduled.input.encoded_len(),
        HistoryEventData::ActivityMapScheduled(scheduled) => scheduled.input_manifest.encoded_len(),
        HistoryEventData::ActivityMapCompleted(completed) => {
            completed.result_manifest.encoded_len()
        }
        HistoryEventData::ActivityMapFailed(failed) => failed.message.len(),
        HistoryEventData::ActivityCompleted(completed) => completed.result.encoded_len(),
        HistoryEventData::ActivityFailed(failed) => failed.message.len(),
        HistoryEventData::ActivityTimedOut(timed_out) => timed_out.message.len(),
        HistoryEventData::TimerStarted(_) | HistoryEventData::TimerFired(_) => 16,
        HistoryEventData::SignalConsumed(signal) => signal.payload.encoded_len(),
    }
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
