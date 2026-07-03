use crate::{CommitOutcome, CompleteActivityOutcome, DurableBackend, Error, PayloadStorageConfig};
use futures::future::BoxFuture;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VirtualInstant {
    millis: u64,
}

impl VirtualInstant {
    pub fn from_millis(millis: u64) -> Self {
        Self { millis }
    }

    pub fn as_millis(self) -> u64 {
        self.millis
    }
}

#[derive(Clone, Debug, Default)]
pub struct VirtualClock {
    now: VirtualInstant,
}

impl VirtualClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn now(&self) -> VirtualInstant {
        self.now
    }

    pub fn advance(&mut self, duration: Duration) -> VirtualInstant {
        let millis = duration.as_millis().try_into().unwrap_or(u64::MAX);
        self.now.millis = self.now.millis.saturating_add(millis);
        self.now
    }

    pub fn advance_to(&mut self, instant: VirtualInstant) -> VirtualInstant {
        if instant > self.now {
            self.now = instant;
        }
        self.now
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimTaskId(u64);

impl SimTaskId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimStep {
    pub task_id: SimTaskId,
    pub label: String,
}

#[derive(Clone, Debug)]
pub struct SeededScheduler {
    rng: XorShift64,
    next_task_id: u64,
    ready: VecDeque<SimStep>,
    delayed: BTreeMap<VirtualInstant, Vec<SimStep>>,
}

impl SeededScheduler {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: XorShift64::new(seed),
            next_task_id: 0,
            ready: VecDeque::new(),
            delayed: BTreeMap::new(),
        }
    }

    pub fn spawn(&mut self, label: impl Into<String>) -> SimTaskId {
        self.next_task_id += 1;
        let task_id = SimTaskId(self.next_task_id);
        self.ready.push_back(SimStep {
            task_id,
            label: label.into(),
        });
        task_id
    }

    pub fn spawn_at(&mut self, at: VirtualInstant, label: impl Into<String>) -> SimTaskId {
        self.next_task_id += 1;
        let task_id = SimTaskId(self.next_task_id);
        self.delayed.entry(at).or_default().push(SimStep {
            task_id,
            label: label.into(),
        });
        task_id
    }

    pub fn wake_due(&mut self, now: VirtualInstant) {
        let due = self
            .delayed
            .keys()
            .copied()
            .take_while(|instant| instant <= &now)
            .collect::<Vec<_>>();
        for instant in due {
            if let Some(mut steps) = self.delayed.remove(&instant) {
                steps.sort_by_key(|step| step.task_id);
                self.ready.extend(steps);
            }
        }
    }

    pub fn next_step(&mut self) -> Option<SimStep> {
        if self.ready.is_empty() {
            return None;
        }
        let index = (self.rng.next() as usize) % self.ready.len();
        self.ready.remove(index)
    }

    pub fn next_delayed_at(&self) -> Option<VirtualInstant> {
        self.delayed.keys().next().copied()
    }

    pub fn is_idle(&self) -> bool {
        self.ready.is_empty() && self.delayed.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FaultProfile {
    #[default]
    None,
    Moderate,
    Aggressive,
}

impl FaultProfile {
    fn should_inject(self, rng: &mut XorShift64, point: FaultPoint) -> bool {
        match self {
            Self::None => false,
            Self::Moderate => rng.next() % point.moderate_denominator() == 0,
            Self::Aggressive => rng.next() % point.aggressive_denominator() == 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultPoint {
    WorkerCrash,
    CacheEviction,
    CommitConflict,
    ActivityDuplicateCompletion,
    TimerDuplicateFire,
    SignalStorm,
    BlobStoreTransient,
    BackendTransient,
    ProviderBackpressure,
    RecoveryBudgetExhausted,
    ShardLeaseLoss,
    CrossShardDuplicateDelivery,
    CrossShardDelayedDelivery,
    DispatcherCrash(DispatcherCrashPoint),
}

impl FaultPoint {
    fn moderate_denominator(self) -> u64 {
        match self {
            Self::SignalStorm => 5,
            Self::CrossShardDelayedDelivery => 4,
            Self::DispatcherCrash(_) => 5,
            Self::ProviderBackpressure | Self::RecoveryBudgetExhausted => 4,
            Self::BackendTransient => 10,
            _ => 6,
        }
    }

    fn aggressive_denominator(self) -> u64 {
        match self {
            Self::SignalStorm
            | Self::CrossShardDuplicateDelivery
            | Self::CrossShardDelayedDelivery
            | Self::ProviderBackpressure
            | Self::RecoveryBudgetExhausted => 2,
            Self::DispatcherCrash(_) => 3,
            Self::BackendTransient => 4,
            _ => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DispatcherCrashPoint {
    SourceOutboxRead,
    TargetInboxWrite,
    TargetApply,
    SourceAck,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEntry {
    pub step: u64,
    pub now_ms: u64,
    pub task_id: Option<u64>,
    pub label: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimTrace {
    pub seed: u64,
    pub entries: Vec<TraceEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimFailure {
    pub seed: u64,
    pub invariant: String,
    pub message: String,
    pub trace: SimTrace,
}

impl fmt::Display for SimFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "deterministic simulation failed: seed={} invariant={} message={}",
            self.seed, self.invariant, self.message
        )?;
        writeln!(f, "trace:")?;
        for entry in &self.trace.entries {
            writeln!(
                f,
                "  step={} now_ms={} task={:?} {} {}",
                entry.step, entry.now_ms, entry.task_id, entry.label, entry.detail
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for SimFailure {}

#[derive(Clone, Debug)]
pub struct SimRun {
    seed: u64,
    fault_profile: FaultProfile,
    clock: VirtualClock,
    scheduler: SeededScheduler,
    fault_rng: XorShift64,
    trace: SimTrace,
    step_count: u64,
    current_task_id: Option<SimTaskId>,
}

impl SimRun {
    pub fn new(seed: u64) -> Self {
        // Scramble the caller's seed before it becomes RNG state so seed 0 is
        // usable (XorShift state cannot be zero) and every requested seed maps
        // to a distinct schedule, while `SimRun::seed` and failure reports
        // keep the caller's value for reproduction.
        let mut mix = seed;
        let scheduler_seed = splitmix64(&mut mix);
        let fault_seed = splitmix64(&mut mix);
        Self {
            seed,
            fault_profile: FaultProfile::None,
            clock: VirtualClock::new(),
            scheduler: SeededScheduler::new(scheduler_seed),
            fault_rng: XorShift64::new(fault_seed),
            trace: SimTrace {
                seed,
                entries: Vec::new(),
            },
            step_count: 0,
            current_task_id: None,
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn fault_profile(&self) -> FaultProfile {
        self.fault_profile
    }

    pub fn with_fault_profile(mut self, fault_profile: FaultProfile) -> Self {
        self.fault_profile = fault_profile;
        self
    }

    pub fn trace(&self) -> &SimTrace {
        &self.trace
    }

    pub fn now(&self) -> VirtualInstant {
        self.clock.now()
    }

    pub fn schedule(&mut self, label: impl Into<String>) -> SimTaskId {
        let label = label.into();
        let task_id = self.scheduler.spawn(label.clone());
        self.record(
            "schedule",
            format!("task={} label={label}", task_id.as_u64()),
        );
        task_id
    }

    pub fn schedule_after(&mut self, delay: Duration, label: impl Into<String>) -> SimTaskId {
        let label = label.into();
        let at = VirtualInstant::from_millis(
            self.clock
                .now()
                .as_millis()
                .saturating_add(delay.as_millis().try_into().unwrap_or(u64::MAX)),
        );
        let task_id = self.scheduler.spawn_at(at, label.clone());
        self.record(
            "schedule_after",
            format!(
                "task={} at_ms={} label={label}",
                task_id.as_u64(),
                at.as_millis()
            ),
        );
        task_id
    }

    pub fn record(&mut self, label: impl Into<String>, detail: impl Into<String>) {
        self.trace.entries.push(TraceEntry {
            step: self.step_count,
            now_ms: self.clock.now().as_millis(),
            task_id: self.current_task_id.map(SimTaskId::as_u64),
            label: label.into(),
            detail: detail.into(),
        });
    }

    pub fn inject(&mut self, point: FaultPoint) -> bool {
        let hit = self.fault_profile.should_inject(&mut self.fault_rng, point);
        if hit {
            self.record("fault", format!("{point:?}"));
        }
        hit
    }

    pub fn ensure(
        &self,
        invariant: impl Into<String>,
        condition: bool,
        message: impl Into<String>,
    ) -> Result<(), SimFailure> {
        if condition {
            Ok(())
        } else {
            Err(self.failure(invariant, message))
        }
    }

    pub fn failure(&self, invariant: impl Into<String>, message: impl Into<String>) -> SimFailure {
        SimFailure {
            seed: self.seed,
            invariant: invariant.into(),
            message: message.into(),
            trace: self.trace.clone(),
        }
    }

    pub fn run_until_idle<F>(&mut self, max_steps: usize, mut on_step: F) -> Result<(), SimFailure>
    where
        F: FnMut(&mut Self, SimStep) -> Result<(), SimFailure>,
    {
        let start_step_count = self.step_count;
        loop {
            self.scheduler.wake_due(self.clock.now());
            let step = match self.scheduler.next_step() {
                Some(step) => step,
                None => match self.scheduler.next_delayed_at() {
                    Some(next) => {
                        self.clock.advance_to(next);
                        self.record("clock", format!("advance_to_ms={}", next.as_millis()));
                        continue;
                    }
                    None => return Ok(()),
                },
            };

            self.step_count = self.step_count.saturating_add(1);
            if self.step_count.saturating_sub(start_step_count) as usize > max_steps {
                return Err(self.failure(
                    "max_steps",
                    format!("simulation exceeded {max_steps} steps"),
                ));
            }

            let previous = self.current_task_id.replace(step.task_id);
            self.record("step", step.label.clone());
            on_step(self, step)?;
            self.current_task_id = previous;
        }
    }

    pub fn is_idle(&self) -> bool {
        self.scheduler.is_idle()
    }
}

type ClaimHook = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

/// Deterministic fault-injecting `DurableBackend` decorator for simulation.
///
/// Wraps any backend (the deterministic target is `MemoryBackend`) and makes
/// seeded per-call fault decisions before forwarding:
///
/// - Transient errors on any fallible method, decided by the `FaultProfile`
///   from a seeded RNG. A failed call never reaches the inner backend, so a
///   "delayed delivery" is simply a read that fails now and succeeds when the
///   caller retries.
/// - Duplicate activity completions: the identical `complete_activity`
///   request is replayed against the inner backend, exercising provider
///   idempotency; the duplicate outcome is recorded for assertions.
/// - Scripted worker crashes: after the next successful workflow-task claim
///   every call fails until `revive`, so a real `Worker` can be "killed"
///   between claim and commit while the inner backend keeps the claim.
/// - A one-shot post-claim hook, so a scenario can commit racing work (e.g.
///   an activity completion) between a worker's claim and its commit to force
///   genuine tail conflicts.
///
/// Determinism contract: callers drive the backend serially (the worker is
/// single-threaded), every fallible call consumes RNG state in call order,
/// and hooks run synchronously inside the call that triggered them. Same
/// seed + same call sequence => same decisions. The wrapper never mutates
/// inner state directly; it only fails, replays, or forwards calls.
#[derive(Clone)]
pub struct FaultInjectingBackend<B>
where
    B: DurableBackend,
{
    inner: B,
    state: Arc<Mutex<FaultInjectorState>>,
}

struct FaultInjectorState {
    rng: XorShift64,
    profile: FaultProfile,
    crashed: bool,
    crash_after_next_workflow_claim: bool,
    after_workflow_claim_hook: Option<ClaimHook>,
    injected_faults: u64,
    duplicated_completions: u64,
    duplicate_completion_outcomes: Vec<CompleteActivityOutcome>,
    observed_commit_conflicts: u64,
}

impl<B> FaultInjectingBackend<B>
where
    B: DurableBackend,
{
    pub fn new(inner: B, seed: u64, profile: FaultProfile) -> Self {
        // Seed the wrapper from SplitMix64 output 3: a same-seed `SimRun`
        // consumes outputs 1 (scheduler) and 2 (fault RNG), so the wrapper's
        // decision stream must start later in the sequence to stay
        // decorrelated from both.
        let mut mix = seed;
        let _ = splitmix64(&mut mix);
        let _ = splitmix64(&mut mix);
        let rng_seed = splitmix64(&mut mix);
        Self {
            inner,
            state: Arc::new(Mutex::new(FaultInjectorState {
                rng: XorShift64::new(rng_seed),
                profile,
                crashed: false,
                crash_after_next_workflow_claim: false,
                after_workflow_claim_hook: None,
                injected_faults: 0,
                duplicated_completions: 0,
                duplicate_completion_outcomes: Vec::new(),
                observed_commit_conflicts: 0,
            })),
        }
    }

    /// Stops profile-driven fault decisions (crash state is separate; see
    /// `revive`). Scenarios use this for a convergence phase in which the
    /// system must quiesce and every workflow must finish.
    pub fn disable_faults(&self) {
        self.lock().profile = FaultProfile::None;
    }

    /// Arms a one-shot crash: after the next workflow-task claim succeeds,
    /// every call fails until `revive`. The inner backend keeps the claim,
    /// which is exactly the state a worker crash between claim and commit
    /// leaves behind.
    pub fn crash_after_next_workflow_claim(&self) {
        self.lock().crash_after_next_workflow_claim = true;
    }

    /// Clears the crashed state (and any armed crash), modeling a replacement
    /// worker reaching the backend.
    pub fn revive(&self) {
        let mut state = self.lock();
        state.crashed = false;
        state.crash_after_next_workflow_claim = false;
    }

    pub fn is_crashed(&self) -> bool {
        self.lock().crashed
    }

    /// One-shot hook that runs immediately after the next successful
    /// workflow-task claim, before the claim is returned to the caller.
    /// Racing appends committed here (through the inner backend's public
    /// API) land between the worker's claim and its commit.
    pub fn on_next_workflow_claim<F>(&self, hook: F)
    where
        F: FnOnce() -> BoxFuture<'static, ()> + Send + 'static,
    {
        self.lock().after_workflow_claim_hook = Some(Box::new(hook));
    }

    pub fn injected_faults(&self) -> u64 {
        self.lock().injected_faults
    }

    pub fn duplicated_completions(&self) -> u64 {
        self.lock().duplicated_completions
    }

    pub fn duplicate_completion_outcomes(&self) -> Vec<CompleteActivityOutcome> {
        self.lock().duplicate_completion_outcomes.clone()
    }

    pub fn observed_commit_conflicts(&self) -> u64 {
        self.lock().observed_commit_conflicts
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FaultInjectorState> {
        self.state.lock().expect("fault injector mutex poisoned")
    }

    // One deterministic decision per fallible call: crashed backends fail
    // without consuming RNG state; live ones draw once for the transient
    // fault decision.
    fn fault(&self, op: &'static str) -> Option<Error> {
        let mut state = self.lock();
        if state.crashed {
            return Some(Error::Backend(format!(
                "simulated worker crash: backend unreachable during {op}"
            )));
        }
        let state = &mut *state;
        if state
            .profile
            .should_inject(&mut state.rng, FaultPoint::BackendTransient)
        {
            state.injected_faults += 1;
            return Some(Error::Backend(format!(
                "injected transient backend fault during {op}"
            )));
        }
        None
    }

    fn decide(&self, point: FaultPoint) -> bool {
        let mut state = self.lock();
        if state.crashed {
            return false;
        }
        let state = &mut *state;
        state.profile.should_inject(&mut state.rng, point)
    }

    // Runs crash arming and the one-shot post-claim hook after a successful
    // claim; shared by the single and batch claim paths.
    fn after_workflow_claim(&self) -> Option<ClaimHook> {
        let mut state = self.lock();
        if state.crash_after_next_workflow_claim {
            state.crash_after_next_workflow_claim = false;
            state.crashed = true;
        }
        state.after_workflow_claim_hook.take()
    }
}

/// Errors produced by `FaultInjectingBackend` (as opposed to real backend
/// errors, which a scenario should treat as bugs).
pub fn is_injected_fault(err: &Error) -> bool {
    matches!(
        err,
        Error::Backend(message)
            if message.starts_with("injected transient backend fault")
                || message.starts_with("simulated worker crash")
    )
}

macro_rules! forward_faultable {
    ($(fn $method:ident($($arg:ident: $arg_ty:ty),*) -> $out:ty;)*) => {
        $(
            fn $method(&self, $($arg: $arg_ty),*) -> BoxFuture<'static, crate::Result<$out>> {
                if let Some(err) = self.fault(stringify!($method)) {
                    return Box::pin(futures::future::ready(Err(err)));
                }
                self.inner.$method($($arg),*)
            }
        )*
    };
}

impl<B> DurableBackend for FaultInjectingBackend<B>
where
    B: DurableBackend,
{
    fn payload_storage_config(&self) -> PayloadStorageConfig {
        self.inner.payload_storage_config()
    }

    forward_faultable! {
        fn start_workflow(req: crate::StartWorkflowRequest) -> crate::StartWorkflowOutcome;
        fn cancel_workflow(req: crate::CancelWorkflowRequest) -> crate::CancelWorkflowOutcome;
        fn current_time() -> crate::TimestampMs;
        fn stream_history(req: crate::StreamHistoryRequest) -> crate::HistoryChunk;
        fn stream_history_for_replay(req: crate::StreamHistoryRequest) -> crate::HistoryChunk;
        fn hydrate_payload(payload: crate::PayloadRef) -> crate::PayloadRef;
        fn hydrate_activity_map_result_manifest(payload: crate::PayloadRef) -> crate::PayloadRef;
        fn hydrate_child_workflow_map_result_manifest(
            payload: crate::PayloadRef
        ) -> crate::PayloadRef;
        fn release_workflow_task(
            claim: crate::WorkflowTaskClaim,
            release: crate::WorkflowTaskRelease
        ) -> ();
        fn signal_workflow(req: crate::SignalWorkflowRequest) -> crate::SignalWorkflowOutcome;
        fn read_signal_inbox(
            req: crate::ReadSignalInboxRequest
        ) -> Option<crate::SignalInboxRecord>;
        fn read_signal_inboxes(
            req: crate::ReadSignalInboxesRequest
        ) -> Vec<Option<crate::SignalInboxRecord>>;
        fn fire_due_timers(req: crate::FireDueTimersRequest) -> crate::FireDueTimersOutcome;
        fn timeout_due_activities(
            req: crate::TimeoutDueActivitiesRequest
        ) -> crate::TimeoutDueActivitiesOutcome;
        fn run_due_maintenance(
            req: crate::RunDueMaintenanceRequest
        ) -> crate::RunDueMaintenanceOutcome;
        fn claim_activity_task(
            worker_id: crate::WorkerId,
            opts: crate::ClaimActivityOptions
        ) -> Option<crate::ClaimedActivityTask>;
        fn claim_activity_tasks(
            worker_id: crate::WorkerId,
            opts: crate::ClaimActivityTasksOptions
        ) -> Vec<crate::ClaimedActivityTask>;
        fn heartbeat_activity(
            req: crate::ActivityHeartbeatRequest
        ) -> crate::ActivityHeartbeatOutcome;
        fn fail_activity(req: crate::FailActivityRequest) -> crate::FailActivityOutcome;
        fn dispatch_child_workflow_starts(
            req: crate::DispatchChildWorkflowStartsRequest
        ) -> crate::DispatchChildWorkflowStartsOutcome;
        fn query_projection(
            req: crate::QueryProjectionRequest
        ) -> crate::QueryProjectionOutcome;
        fn workflow_change_versions(
            req: crate::WorkflowChangeVersionsRequest
        ) -> crate::WorkflowChangeVersionsOutcome;
        fn payload_roots() -> crate::PayloadRootsOutcome;
        fn gc_payload_blobs(
            req: crate::PayloadGarbageCollectionRequest
        ) -> crate::PayloadGarbageCollectionOutcome;
    }

    fn claim_workflow_task(
        &self,
        worker_id: crate::WorkerId,
        opts: crate::ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, crate::Result<Option<crate::ClaimedWorkflowTask>>> {
        if let Some(err) = self.fault("claim_workflow_task") {
            return Box::pin(futures::future::ready(Err(err)));
        }
        let inner = self.inner.clone();
        let this = self.clone();
        Box::pin(async move {
            let claimed = inner.claim_workflow_task(worker_id, opts).await?;
            if claimed.is_some()
                && let Some(hook) = this.after_workflow_claim()
            {
                hook().await;
            }
            Ok(claimed)
        })
    }

    fn claim_workflow_tasks(
        &self,
        worker_id: crate::WorkerId,
        opts: crate::ClaimWorkflowTasksOptions,
    ) -> BoxFuture<'static, crate::Result<Vec<crate::ClaimedWorkflowTask>>> {
        if let Some(err) = self.fault("claim_workflow_tasks") {
            return Box::pin(futures::future::ready(Err(err)));
        }
        let inner = self.inner.clone();
        let this = self.clone();
        Box::pin(async move {
            let claimed = inner.claim_workflow_tasks(worker_id, opts).await?;
            if !claimed.is_empty()
                && let Some(hook) = this.after_workflow_claim()
            {
                hook().await;
            }
            Ok(claimed)
        })
    }

    fn commit_workflow_task(
        &self,
        claim: crate::WorkflowTaskClaim,
        batch: crate::WorkflowTaskCommit,
    ) -> BoxFuture<'static, crate::Result<CommitOutcome>> {
        if let Some(err) = self.fault("commit_workflow_task") {
            return Box::pin(futures::future::ready(Err(err)));
        }
        let inner = self.inner.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let outcome = inner.commit_workflow_task(claim, batch).await?;
            if outcome == CommitOutcome::Conflict {
                state
                    .lock()
                    .expect("fault injector mutex poisoned")
                    .observed_commit_conflicts += 1;
            }
            Ok(outcome)
        })
    }

    fn commit_workflow_tasks(
        &self,
        batch: crate::WorkflowTaskCommitBatch,
    ) -> BoxFuture<'static, crate::Result<Vec<crate::WorkflowTaskCommitBatchResult>>> {
        if let Some(err) = self.fault("commit_workflow_tasks") {
            return Box::pin(futures::future::ready(Err(err)));
        }
        let inner = self.inner.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let results = inner.commit_workflow_tasks(batch).await?;
            let conflicts = results
                .iter()
                .filter(|result| result.result == Ok(CommitOutcome::Conflict))
                .count() as u64;
            if conflicts > 0 {
                state
                    .lock()
                    .expect("fault injector mutex poisoned")
                    .observed_commit_conflicts += conflicts;
            }
            Ok(results)
        })
    }

    fn complete_activity(
        &self,
        req: crate::CompleteActivityRequest,
    ) -> BoxFuture<'static, crate::Result<CompleteActivityOutcome>> {
        if let Some(err) = self.fault("complete_activity") {
            return Box::pin(futures::future::ready(Err(err)));
        }
        let duplicate = self.decide(FaultPoint::ActivityDuplicateCompletion);
        let inner = self.inner.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let first = inner.complete_activity(req.clone()).await?;
            if duplicate {
                // Replay the identical request; the provider must report the
                // duplicate idempotently without appending a second event.
                let second = inner.complete_activity(req).await?;
                let mut state = state.lock().expect("fault injector mutex poisoned");
                state.duplicated_completions += 1;
                state.duplicate_completion_outcomes.push(second);
            }
            Ok(first)
        })
    }

    fn complete_activity_tasks(
        &self,
        req: crate::CompleteActivityTasksRequest,
    ) -> BoxFuture<'static, crate::Result<Vec<crate::CompleteActivityTaskBatchResult>>> {
        if let Some(err) = self.fault("complete_activity_tasks") {
            return Box::pin(futures::future::ready(Err(err)));
        }
        self.inner.complete_activity_tasks(req)
    }
}

pub fn run_many_seeds<F>(
    first_seed: u64,
    count: usize,
    fault_profile: FaultProfile,
    mut scenario: F,
) -> Result<(), SimFailure>
where
    F: FnMut(&mut SimRun) -> Result<(), SimFailure>,
{
    for offset in 0..count {
        let seed = first_seed.saturating_add(offset as u64);
        let mut run = SimRun::new(seed).with_fault_profile(fault_profile);
        run.record(
            "seed",
            format!("start seed={seed} profile={fault_profile:?}"),
        );
        scenario(&mut run)?;
        run.ensure(
            "scheduler_idle",
            run.is_idle(),
            "scenario left pending tasks",
        )?;
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

// SplitMix64: sequential calls yield independent well-mixed seeds, so small
// consecutive caller seeds (0, 1, 2, ...) produce decorrelated RNG states.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn same_seed_produces_same_schedule() {
        fn trace(seed: u64) -> Vec<String> {
            let mut scheduler = SeededScheduler::new(seed);
            scheduler.spawn("a");
            scheduler.spawn("b");
            scheduler.spawn("c");
            std::iter::from_fn(|| scheduler.next_step().map(|step| step.label)).collect()
        }

        assert_eq!(trace(123), trace(123));
        assert_ne!(trace(123), trace(456));
    }

    #[test]
    fn virtual_clock_wakes_due_steps_in_stable_order() {
        let mut clock = VirtualClock::new();
        let mut scheduler = SeededScheduler::new(7);
        let later = clock.advance(Duration::from_millis(10));
        scheduler.spawn_at(later, "second");
        scheduler.spawn_at(later, "third");
        scheduler.spawn_at(VirtualInstant { millis: 5 }, "first");

        scheduler.wake_due(VirtualInstant { millis: 5 });
        assert_eq!(scheduler.next_step().unwrap().label, "first");
        assert!(scheduler.next_step().is_none());

        scheduler.wake_due(later);
        let labels =
            std::iter::from_fn(|| scheduler.next_step().map(|step| step.label)).collect::<Vec<_>>();
        assert_eq!(labels, vec!["second", "third"]);
    }

    #[test]
    fn requested_seed_round_trips_and_seed_zero_is_a_distinct_run() {
        fn schedule(seed: u64) -> Vec<String> {
            let mut sim = SimRun::new(seed);
            for i in 0..8 {
                sim.schedule(format!("task:{i}"));
            }
            let mut labels = Vec::new();
            sim.run_until_idle(64, |_, step| {
                labels.push(step.label);
                Ok(())
            })
            .unwrap();
            labels
        }

        fn fault_decisions(seed: u64) -> Vec<bool> {
            let mut sim = SimRun::new(seed).with_fault_profile(FaultProfile::Aggressive);
            (0..64)
                .map(|_| sim.inject(FaultPoint::WorkerCrash))
                .collect()
        }

        // Seed 0 must report itself (not seed 1) and produce its own schedule
        // and fault decisions instead of collapsing into seed 1's run.
        assert_eq!(SimRun::new(0).seed(), 0);
        assert_eq!(SimRun::new(0).trace().seed, 0);
        assert_eq!(schedule(0), schedule(0));
        assert_ne!(schedule(0), schedule(1));
        assert_ne!(fault_decisions(0), fault_decisions(1));
    }

    #[test]
    fn fault_injecting_backend_decisions_are_deterministic_per_seed() {
        use futures::executor::block_on;

        fn decision_trace(seed: u64) -> Vec<bool> {
            let backend = FaultInjectingBackend::new(
                crate::MemoryBackend::new(),
                seed,
                FaultProfile::Aggressive,
            );
            (0..64)
                .map(|_| block_on(backend.current_time()).is_err())
                .collect()
        }

        assert_eq!(decision_trace(7), decision_trace(7));
        assert_ne!(decision_trace(7), decision_trace(8));
        // The aggressive profile must actually fail some calls while letting
        // others through to the inner backend.
        let trace = decision_trace(7);
        assert!(trace.iter().any(|failed| *failed));
        assert!(trace.iter().any(|failed| !*failed));

        // The wrapper's stream must be decorrelated from a same-seed
        // `SimRun`'s fault stream: identical fault point (same denominator),
        // identical seed, different decisions.
        let mut sim = SimRun::new(7).with_fault_profile(FaultProfile::Aggressive);
        let sim_trace: Vec<bool> = (0..64)
            .map(|_| sim.inject(FaultPoint::BackendTransient))
            .collect();
        assert_ne!(trace, sim_trace);
    }

    #[test]
    fn crash_arming_holds_the_inner_claim_until_revive() {
        use futures::executor::block_on;

        let inner = crate::MemoryBackend::new();
        let backend = FaultInjectingBackend::new(inner.clone(), 3, FaultProfile::None);
        block_on(backend.start_workflow(crate::StartWorkflowRequest {
            namespace: crate::Namespace::default(),
            workflow_id: crate::WorkflowId::new("wf/sim-crash"),
            workflow_type: crate::WorkflowType::new("sim.crash", 1),
            task_queue: crate::TaskQueue::new("sim-crash-queue"),
            input: crate::encode_payload(&1_u64).unwrap(),
        }))
        .unwrap();
        let opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: crate::TaskQueue::new("sim-crash-queue"),
            registered_workflow_types: vec![crate::WorkflowType::new("sim.crash", 1)],
            lease_duration: Duration::from_secs(1),
        };

        backend.crash_after_next_workflow_claim();
        let claimed =
            block_on(backend.claim_workflow_task(crate::WorkerId::new("w1"), opts.clone()))
                .unwrap()
                .expect("workflow task");
        assert!(backend.is_crashed());
        assert!(is_injected_fault(
            &block_on(backend.current_time()).unwrap_err()
        ));

        // The inner backend still holds the crashed worker's claim, so the
        // run is invisible until the lease expires.
        assert!(
            block_on(inner.claim_workflow_task(crate::WorkerId::new("w2"), opts.clone()))
                .unwrap()
                .is_none()
        );

        backend.revive();
        assert!(block_on(backend.current_time()).is_ok());
        inner.advance_time(Duration::from_secs(2));
        let reclaimed = block_on(backend.claim_workflow_task(crate::WorkerId::new("w2"), opts))
            .unwrap()
            .expect("reclaim after lease expiry");
        assert_ne!(reclaimed.claim.token, claimed.claim.token);
    }

    #[test]
    fn simulation_failure_display_includes_seed_and_trace() {
        let mut sim = SimRun::new(42);
        sim.record("scenario", "started");
        let failure = sim.failure("history_order", "event id regressed");
        let rendered = failure.to_string();
        assert!(rendered.contains("seed=42"));
        assert!(rendered.contains("history_order"));
        assert!(rendered.contains("event id regressed"));
        assert!(rendered.contains("scenario started"));
    }

    #[test]
    fn many_seed_aggressive_fault_profiles_preserve_model_invariants() {
        run_many_seeds(1, 2_048, FaultProfile::Aggressive, |sim| {
            simulate_worker_cache_and_commit_conflict_storm(sim)?;
            simulate_external_event_idempotency_storm(sim)?;
            simulate_blob_store_transient_errors(sim)?;
            simulate_shard_lease_and_cross_shard_handoff(sim)?;
            simulate_recovery_flow_control_storm(sim)?;
            Ok(())
        })
        .unwrap();
    }

    #[derive(Default)]
    struct WorkerStorm {
        committed: BTreeSet<u64>,
        attempts: BTreeMap<u64, u32>,
    }

    fn simulate_worker_cache_and_commit_conflict_storm(sim: &mut SimRun) -> Result<(), SimFailure> {
        const COMMANDS: u64 = 12;
        sim.record("scenario", "worker_cache_commit_conflict_storm");
        sim.schedule("workflow:1");
        let mut state = WorkerStorm::default();

        sim.run_until_idle(4_000, |sim, step| {
            let command = suffix(&step.label, "workflow:").ok_or_else(|| {
                sim.failure("parse_step", format!("unknown step `{}`", step.label))
            })?;
            let attempts = state.attempts.entry(command).or_default();
            *attempts += 1;

            if *attempts <= 8 && sim.inject(FaultPoint::WorkerCrash) {
                sim.schedule_after(Duration::from_millis(1), step.label);
                return Ok(());
            }
            if *attempts <= 8 && sim.inject(FaultPoint::CacheEviction) {
                sim.schedule_after(Duration::from_millis(1), step.label);
                return Ok(());
            }
            if *attempts <= 8 && sim.inject(FaultPoint::CommitConflict) {
                sim.schedule_after(Duration::from_millis(1), step.label);
                return Ok(());
            }

            sim.ensure(
                "single_commit_per_command",
                state.committed.insert(command),
                format!("command {command} committed twice"),
            )?;
            sim.record("commit", format!("workflow_command={command}"));
            if command < COMMANDS {
                sim.schedule(format!("workflow:{}", command + 1));
            }
            Ok(())
        })?;

        sim.ensure(
            "all_commands_committed",
            state.committed.len() == COMMANDS as usize,
            format!("committed {} of {COMMANDS} commands", state.committed.len()),
        )
    }

    #[derive(Default)]
    struct ExternalEvents {
        activities: BTreeSet<u64>,
        timers: BTreeSet<u64>,
        signals: BTreeSet<u64>,
        attempts: BTreeMap<String, u32>,
        duplicate_attempts: u64,
    }

    fn simulate_external_event_idempotency_storm(sim: &mut SimRun) -> Result<(), SimFailure> {
        const EVENTS: u64 = 8;
        sim.record("scenario", "external_event_idempotency_storm");
        for id in 1..=EVENTS {
            sim.schedule(format!("activity:{id}"));
            sim.schedule(format!("timer:{id}"));
            sim.schedule(format!("signal:{id}"));
        }
        let mut state = ExternalEvents::default();
        state.duplicate_attempts += 3;
        sim.schedule_after(Duration::from_millis(1), "activity:1");
        sim.schedule_after(Duration::from_millis(1), "timer:1");
        sim.schedule_after(Duration::from_millis(1), "signal:1");

        sim.run_until_idle(4_000, |sim, step| {
            let attempts = state.attempts.entry(step.label.clone()).or_default();
            *attempts += 1;
            let fault_budget = *attempts <= 4;
            if let Some(id) = suffix(&step.label, "activity:") {
                if fault_budget && sim.inject(FaultPoint::ActivityDuplicateCompletion) {
                    state.duplicate_attempts += 1;
                    sim.schedule_after(Duration::from_millis(1), step.label.clone());
                }
                state.activities.insert(id);
            } else if let Some(id) = suffix(&step.label, "timer:") {
                if fault_budget && sim.inject(FaultPoint::TimerDuplicateFire) {
                    state.duplicate_attempts += 1;
                    sim.schedule_after(Duration::from_millis(1), step.label.clone());
                }
                state.timers.insert(id);
            } else if let Some(id) = suffix(&step.label, "signal:") {
                if fault_budget && sim.inject(FaultPoint::SignalStorm) {
                    state.duplicate_attempts += 1;
                    sim.schedule_after(Duration::from_millis(1), step.label.clone());
                    sim.schedule_after(Duration::from_millis(2), step.label.clone());
                }
                state.signals.insert(id);
            } else {
                return Err(sim.failure("known_external_event", step.label));
            }
            Ok(())
        })?;

        sim.ensure(
            "activity_completion_idempotency",
            state.activities.len() == EVENTS as usize,
            "activity completions were lost or duplicated",
        )?;
        sim.ensure(
            "timer_fire_idempotency",
            state.timers.len() == EVENTS as usize,
            "timer fires were lost or duplicated",
        )?;
        sim.ensure(
            "signal_idempotency",
            state.signals.len() == EVENTS as usize,
            "signals were lost or duplicated",
        )?;
        sim.ensure(
            "faults_exercised",
            state.duplicate_attempts > 0,
            "duplicate external event faults did not execute",
        )
    }

    #[derive(Default)]
    struct RecoveryFlowModel {
        active_recoveries: usize,
        max_active_recoveries: usize,
        recovery_attempts: BTreeMap<u64, u32>,
        recovery_chunks_read: BTreeMap<u64, u32>,
        completed_recoveries: BTreeSet<u64>,
        completed_cached_wakes: BTreeSet<u64>,
        deferred_recoveries: u64,
        backpressure_retries: u64,
    }

    fn simulate_recovery_flow_control_storm(sim: &mut SimRun) -> Result<(), SimFailure> {
        const RECOVERIES: u64 = 10;
        const CACHED_WAKES: u64 = 6;
        const MAX_ACTIVE: usize = 2;
        const CHUNKS_PER_ATTEMPT: u32 = 2;
        const REQUIRED_CHUNKS: u32 = 5;

        sim.record("scenario", "recovery_flow_control_storm");
        for id in 1..=RECOVERIES {
            sim.schedule(format!("recovery-start:{id}"));
        }
        for id in 1..=CACHED_WAKES {
            sim.schedule(format!("cached-wake:{id}"));
        }

        let mut state = RecoveryFlowModel::default();
        sim.run_until_idle(8_000, |sim, step| {
            if let Some(id) = suffix(&step.label, "cached-wake:") {
                state.completed_cached_wakes.insert(id);
                sim.record("cached_wake", format!("id={id}"));
                return Ok(());
            }

            if let Some(id) = suffix(&step.label, "recovery-start:") {
                let attempts = state.recovery_attempts.entry(id).or_default();
                *attempts += 1;
                if state.active_recoveries >= MAX_ACTIVE {
                    state.deferred_recoveries += 1;
                    sim.schedule_after(Duration::from_millis(2), step.label);
                    return Ok(());
                }

                state.active_recoveries += 1;
                state.max_active_recoveries =
                    state.max_active_recoveries.max(state.active_recoveries);
                sim.ensure(
                    "bounded_active_recoveries",
                    state.active_recoveries <= MAX_ACTIVE,
                    format!("active recoveries exceeded {MAX_ACTIVE}"),
                )?;
                sim.schedule_after(Duration::from_millis(1), format!("recovery-read:{id}"));
                return Ok(());
            }

            let id = suffix(&step.label, "recovery-read:").ok_or_else(|| {
                sim.failure("parse_step", format!("unknown step `{}`", step.label))
            })?;
            let fault_budget = state
                .recovery_attempts
                .get(&id)
                .copied()
                .unwrap_or_default()
                <= 8;

            if fault_budget && sim.inject(FaultPoint::ProviderBackpressure) {
                state.backpressure_retries += 1;
                state.active_recoveries = state.active_recoveries.saturating_sub(1);
                sim.schedule_after(Duration::from_millis(3), format!("recovery-start:{id}"));
                return Ok(());
            }

            let chunks = state.recovery_chunks_read.entry(id).or_default();
            *chunks = chunks.saturating_add(CHUNKS_PER_ATTEMPT);
            state.active_recoveries = state.active_recoveries.saturating_sub(1);
            if *chunks < REQUIRED_CHUNKS
                || (fault_budget && sim.inject(FaultPoint::RecoveryBudgetExhausted))
            {
                state.deferred_recoveries += 1;
                sim.schedule_after(Duration::from_millis(2), format!("recovery-start:{id}"));
                return Ok(());
            }

            state.completed_recoveries.insert(id);
            sim.record("recovery_complete", format!("id={id} chunks={chunks}"));
            Ok(())
        })?;

        sim.ensure(
            "recovery_limit_exercised",
            state.max_active_recoveries == MAX_ACTIVE,
            format!(
                "max active recoveries was {}, expected {MAX_ACTIVE}",
                state.max_active_recoveries
            ),
        )?;
        sim.ensure(
            "recovery_deferral_exercised",
            state.deferred_recoveries > 0,
            "no recovery was deferred under flow control",
        )?;
        sim.ensure(
            "provider_backpressure_exercised",
            state.backpressure_retries > 0,
            "provider backpressure retry path did not execute",
        )?;
        sim.ensure(
            "cached_wakes_not_starved",
            state.completed_cached_wakes.len() == CACHED_WAKES as usize,
            "cached wakes were starved behind cold recovery",
        )?;
        sim.ensure(
            "recoveries_eventually_complete",
            state.completed_recoveries.len() == RECOVERIES as usize,
            "not all cold recoveries completed",
        )
    }

    #[derive(Default)]
    struct BlobStoreModel {
        attempts: BTreeMap<u64, u32>,
        uploaded: BTreeSet<u64>,
        committed_refs: BTreeSet<u64>,
    }

    fn simulate_blob_store_transient_errors(sim: &mut SimRun) -> Result<(), SimFailure> {
        const PAYLOADS: u64 = 10;
        sim.record("scenario", "blob_store_transient_errors");
        for id in 1..=PAYLOADS {
            sim.schedule(format!("payload:{id}"));
        }
        let mut state = BlobStoreModel::default();

        sim.run_until_idle(4_000, |sim, step| {
            let payload = suffix(&step.label, "payload:").ok_or_else(|| {
                sim.failure("parse_step", format!("unknown step `{}`", step.label))
            })?;
            let attempts = state.attempts.entry(payload).or_default();
            *attempts += 1;
            if *attempts <= 8 && sim.inject(FaultPoint::BlobStoreTransient) {
                sim.schedule_after(Duration::from_millis(1), step.label);
                return Ok(());
            }

            state.uploaded.insert(payload);
            sim.record("blob_uploaded", format!("payload={payload}"));
            sim.ensure(
                "payload_uploaded_before_commit",
                state.uploaded.contains(&payload),
                format!("payload {payload} committed before upload"),
            )?;
            state.committed_refs.insert(payload);
            Ok(())
        })?;

        sim.ensure(
            "no_missing_committed_payload_ref",
            state.committed_refs.is_subset(&state.uploaded),
            "committed payload ref without uploaded bytes",
        )?;
        sim.ensure(
            "all_payloads_committed",
            state.committed_refs.len() == PAYLOADS as usize,
            "not all payloads committed",
        )
    }

    #[derive(Default)]
    struct CrossShardModel {
        dispatch_attempts: BTreeMap<u64, u32>,
        inbox: BTreeSet<u64>,
        apply_counts: BTreeMap<u64, u32>,
        source_acks: BTreeSet<u64>,
        stale_lease_rejections: u64,
        delayed_or_duplicate_delivery: u64,
    }

    fn simulate_shard_lease_and_cross_shard_handoff(sim: &mut SimRun) -> Result<(), SimFailure> {
        const MESSAGES: u64 = 8;
        sim.record("scenario", "shard_lease_and_cross_shard_handoff");
        sim.schedule("stale-lease-commit:1");
        let mut state = CrossShardModel::default();
        for id in 1..=MESSAGES {
            sim.schedule(format!("dispatch:{id}"));
        }
        state.delayed_or_duplicate_delivery += 1;
        sim.schedule_after(Duration::from_millis(1), "dispatch:1");

        sim.run_until_idle(6_000, |sim, step| {
            if suffix(&step.label, "stale-lease-commit:").is_some() {
                let _ = sim.inject(FaultPoint::ShardLeaseLoss);
                state.stale_lease_rejections += 1;
                sim.record("lease_reject", "owner=old epoch=1 current_epoch=2");
                return Ok(());
            }

            let message = suffix(&step.label, "dispatch:").ok_or_else(|| {
                sim.failure("parse_step", format!("unknown step `{}`", step.label))
            })?;
            let attempts = state.dispatch_attempts.entry(message).or_default();
            *attempts += 1;
            let fault_budget = *attempts <= 10;

            if fault_budget
                && sim.inject(FaultPoint::DispatcherCrash(
                    DispatcherCrashPoint::SourceOutboxRead,
                ))
            {
                sim.schedule_after(Duration::from_millis(1), step.label);
                return Ok(());
            }
            if fault_budget && sim.inject(FaultPoint::CrossShardDuplicateDelivery) {
                state.delayed_or_duplicate_delivery += 1;
                sim.schedule_after(Duration::from_millis(1), step.label.clone());
            }
            if fault_budget && sim.inject(FaultPoint::CrossShardDelayedDelivery) {
                state.delayed_or_duplicate_delivery += 1;
                sim.schedule_after(Duration::from_millis(3), step.label);
                return Ok(());
            }

            state.inbox.insert(message);
            if fault_budget
                && sim.inject(FaultPoint::DispatcherCrash(
                    DispatcherCrashPoint::TargetInboxWrite,
                ))
            {
                sim.schedule_after(Duration::from_millis(1), format!("dispatch:{message}"));
                return Ok(());
            }

            if !state.apply_counts.contains_key(&message) {
                state.apply_counts.insert(message, 1);
                sim.record("target_apply", format!("message={message}"));
            }
            if fault_budget
                && sim.inject(FaultPoint::DispatcherCrash(
                    DispatcherCrashPoint::TargetApply,
                ))
            {
                sim.schedule_after(Duration::from_millis(1), format!("dispatch:{message}"));
                return Ok(());
            }

            if fault_budget
                && sim.inject(FaultPoint::DispatcherCrash(DispatcherCrashPoint::SourceAck))
            {
                sim.schedule_after(Duration::from_millis(1), format!("dispatch:{message}"));
                return Ok(());
            }
            state.source_acks.insert(message);
            Ok(())
        })?;

        sim.ensure(
            "stale_shard_lease_rejected",
            state.stale_lease_rejections > 0,
            "stale shard lease owner did not attempt a rejected commit",
        )?;
        sim.ensure(
            "cross_shard_faults_exercised",
            state.delayed_or_duplicate_delivery > 0,
            "cross-shard duplicate/delay faults did not execute",
        )?;
        for id in 1..=MESSAGES {
            sim.ensure(
                "target_inbox_idempotent",
                state.inbox.contains(&id),
                format!("message {id} missing from target inbox"),
            )?;
            sim.ensure(
                "target_apply_once",
                state.apply_counts.get(&id).copied() == Some(1),
                format!("message {id} target apply count was not exactly one"),
            )?;
            sim.ensure(
                "source_ack_eventual",
                state.source_acks.contains(&id),
                format!("message {id} was not source-acked"),
            )?;
        }
        Ok(())
    }

    fn suffix(label: &str, prefix: &str) -> Option<u64> {
        label.strip_prefix(prefix)?.parse().ok()
    }
}
