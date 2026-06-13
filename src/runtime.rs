use crate::{
    Activity, ActivityScheduled, ActivityTask, CommandId, CommandSeq, Error, HistoryEvent,
    HistoryEventData, NewHistoryEvent, PayloadRef, Result, RunId, TaskQueue, activity_fingerprint,
    command_id, payload_digest,
};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

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
    default_activity_task_queue: TaskQueue,
    replay_events: Vec<HistoryEvent>,
    replay_cursor: usize,
    last_loaded_event_id: crate::EventId,
    replay_target_event_id: crate::EventId,
    needs_more_history: bool,
    next_command_seq: u64,
    completions: BTreeMap<CommandSeq, PayloadRef>,
    append_events: Vec<NewHistoryEvent>,
    schedule_activities: Vec<ActivityTask>,
}

impl RuntimeContext {
    pub(crate) fn new(
        run_id: RunId,
        default_activity_task_queue: TaskQueue,
        replay_events: Vec<HistoryEvent>,
        next_command_seq: u64,
        last_loaded_event_id: crate::EventId,
        replay_target_event_id: crate::EventId,
    ) -> Self {
        let completions = collect_completions(&replay_events);

        Self {
            run_id,
            default_activity_task_queue,
            replay_events,
            replay_cursor: 0,
            last_loaded_event_id,
            replay_target_event_id,
            needs_more_history: false,
            next_command_seq,
            completions,
            append_events: Vec::new(),
            schedule_activities: Vec::new(),
        }
    }

    pub(crate) fn into_commit_parts(self) -> (Vec<NewHistoryEvent>, Vec<ActivityTask>) {
        (self.append_events, self.schedule_activities)
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
        self.replay_events.extend(events);
        self.last_loaded_event_id = last_loaded_event_id;
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
        self.completions.remove(&command_id.seq)
    }
}

impl<A> Unpin for ActivityFuture<A> where A: Activity {}

pub fn activity_call<A>(input: A::Input) -> ActivityFuture<A>
where
    A: Activity,
{
    ActivityFuture {
        input: Some(input),
        task_queue: None,
        state: ActivityFutureState::Init,
        _activity: std::marker::PhantomData,
    }
}

pub struct ActivityFuture<A>
where
    A: Activity,
{
    input: Option<A::Input>,
    task_queue: Option<TaskQueue>,
    state: ActivityFutureState,
    _activity: std::marker::PhantomData<A>,
}

impl<A> ActivityFuture<A>
where
    A: Activity,
{
    pub fn task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.task_queue = Some(TaskQueue::new(task_queue));
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
        let task_queue = self
            .task_queue
            .clone()
            .unwrap_or_else(|| runtime.default_activity_task_queue.clone());
        let input = self
            .input
            .as_ref()
            .expect("activity input exists before schedule");
        let input_ref = crate::encode_payload(input)?;
        let fingerprint = activity_fingerprint(A::activity_name(), payload_digest(&input_ref));

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
                if let HistoryEventData::ActivityCompleted(completed) = next.data {
                    if completed.command_id.seq == command_id.seq {
                        runtime.advance_replay();
                        self.state = ActivityFutureState::Done;
                        return Poll::Ready(crate::decode_payload::<A::Output>(&completed.result));
                    }
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

pub(crate) fn is_terminal(data: &HistoryEventData) -> bool {
    matches!(
        data,
        HistoryEventData::WorkflowCompleted { .. } | HistoryEventData::WorkflowFailed { .. }
    )
}

pub(crate) fn event_payload_len(data: &HistoryEventData) -> usize {
    match data {
        HistoryEventData::WorkflowStarted { input, .. } => input.encoded_len(),
        HistoryEventData::WorkflowCompleted { result } => result.encoded_len(),
        HistoryEventData::WorkflowFailed { message } => message.len(),
        HistoryEventData::WorkflowTaskStarted => 0,
        HistoryEventData::ActivityScheduled(scheduled) => scheduled.input.encoded_len(),
        HistoryEventData::ActivityCompleted(completed) => completed.result.encoded_len(),
    }
}
