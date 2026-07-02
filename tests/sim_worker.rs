//! Deterministic simulations that drive the real `Worker` over the real
//! `MemoryBackend` through the seeded `FaultInjectingBackend`.
//!
//! Unlike the model simulations in `src/sim.rs`, every scenario here executes
//! production claim/prepare/commit/replay code and asserts its invariants
//! from durable state (streamed history), never from model bookkeeping. All
//! timing derives from virtual time: the sim clock advances through scheduled
//! steps and the memory backend's clock is synced to it, so leases, timers,
//! delayed releases, and backoffs are fully controlled per seed.

use durust::{
    ClaimActivityOptions, ClaimWorkflowTaskOptions, ClaimWorkflowTasksOptions, Client,
    CommitOutcome, CompleteActivityOutcome, CompleteActivityRequest, DurableBackend, EventId,
    FaultInjectingBackend, FaultPoint, FaultProfile, HistoryEvent, HistoryEventData, MemoryBackend,
    Namespace, NewHistoryEvent, RunId, SimFailure, SimRun, TaskQueue, Worker, WorkerId,
    WorkflowTaskCommit, WorkflowType, is_injected_fault, run_many_seeds,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Num {
    value: u64,
}

fn num(value: u64) -> Num {
    Num { value }
}

#[durust::activity(name = "sim.double")]
async fn sim_double(input: Num) -> durust::Result<u64> {
    Ok(input.value * 2)
}

// Single activity then arithmetic; the exact output pins exactly-once
// execution across crashes and stale-lease fencing.
#[durust::workflow(name = "sim.pipeline", version = 1)]
async fn sim_pipeline(input: Num) -> durust::Result<u64> {
    let doubled = durust::call_activity!(sim_double(Num { value: input.value }))
        .task_queue("sim-activities")
        .retry(durust::RetryPolicy::exponential().max_attempts(10))
        .await?;
    Ok(doubled + 1)
}

// Multi-task lifecycle: activity, timer, signal, second activity. Output:
// ((2v + bump) * 2) + 1 with bump = 5.
#[durust::workflow(name = "sim.lifecycle", version = 1)]
async fn sim_lifecycle(input: Num) -> durust::Result<u64> {
    let doubled = durust::call_activity!(sim_double(Num { value: input.value }))
        .task_queue("sim-activities")
        .retry(durust::RetryPolicy::exponential().max_attempts(10))
        .await?;
    durust::sleep(Duration::from_millis(50)).await?;
    let bump = durust::signal::<u64>("bump").await?;
    let total = durust::call_activity!(sim_double(Num {
        value: doubled + bump,
    }))
    .task_queue("sim-activities")
    .retry(durust::RetryPolicy::exponential().max_attempts(10))
    .await?;
    Ok(total + 1)
}

// Timer and activity race in one join, with a per-run activity queue so the
// conflict scenario can hold exactly this run's activity completion and land
// it between a claim and its commit. Output: 2v + 3.
#[durust::workflow(name = "sim.conflict", version = 1)]
async fn sim_conflict(input: Num) -> durust::Result<u64> {
    let queue = format!("sim-conflict-activities-{}", input.value);
    let (doubled, _) = durust::join!(
        durust::call_activity!(sim_double(Num { value: input.value }))
            .task_queue(queue)
            .retry(durust::RetryPolicy::exponential().max_attempts(10)),
        durust::sleep(Duration::from_millis(50)),
    )
    .await?;
    Ok(doubled + 3)
}

// Three sequential activities: every completion is a duplication candidate.
// Output: 8v.
#[durust::workflow(name = "sim.chain", version = 1)]
async fn sim_chain(input: Num) -> durust::Result<u64> {
    let first = durust::call_activity!(sim_double(Num { value: input.value }))
        .task_queue("sim-activities")
        .retry(durust::RetryPolicy::exponential().max_attempts(10))
        .await?;
    let second = durust::call_activity!(sim_double(Num { value: first }))
        .task_queue("sim-activities")
        .retry(durust::RetryPolicy::exponential().max_attempts(10))
        .await?;
    durust::call_activity!(sim_double(Num { value: second }))
        .task_queue("sim-activities")
        .retry(durust::RetryPolicy::exponential().max_attempts(10))
        .await
}

// Two signals, an activity, and a timer joined in one task so deliveries can
// arrive in adversarial orders across tasks. Output: alpha * 1_000_000 +
// beta * 1_000 + 2v.
#[durust::workflow(name = "sim.reorder", version = 1)]
async fn sim_reorder(input: Num) -> durust::Result<u64> {
    let (alpha, beta, doubled, _) = durust::join!(
        durust::signal::<u64>("alpha"),
        durust::signal::<u64>("beta"),
        durust::call_activity!(sim_double(Num { value: input.value }))
            .task_queue("sim-activities")
            .retry(durust::RetryPolicy::exponential().max_attempts(10)),
        durust::sleep(Duration::from_millis(40)),
    )
    .await?;
    Ok(alpha * 1_000_000 + beta * 1_000 + doubled)
}

type SimBackend = FaultInjectingBackend<MemoryBackend>;

/// One workflow under test: id, run, and the exact output that proves
/// exactly-once execution.
struct RunUnderTest {
    workflow_id: String,
    run_id: RunId,
    expected_output: u64,
}

struct ScenarioOutcome {
    histories: BTreeMap<String, Vec<HistoryEvent>>,
    injected_faults: u64,
    duplicated_completions: u64,
    observed_commit_conflicts: u64,
    // Cache-eviction worker rebuilds performed by the storm driver; only the
    // eviction scenario feeds this.
    worker_rebuilds: u64,
}

struct SimEnv {
    inner: MemoryBackend,
    backend: SimBackend,
    synced_ms: u64,
}

impl SimEnv {
    fn new(sim: &SimRun) -> Self {
        let inner = MemoryBackend::new();
        let backend = FaultInjectingBackend::new(inner.clone(), sim.seed(), sim.fault_profile());
        Self {
            inner,
            backend,
            synced_ms: 0,
        }
    }

    // The memory backend's virtual clock follows the sim clock, so leases,
    // timers, and delayed releases expire exactly when the schedule says.
    fn sync_clock(&mut self, sim: &SimRun) {
        let now = sim.now().as_millis();
        if now > self.synced_ms {
            self.inner
                .advance_time(Duration::from_millis(now - self.synced_ms));
            self.synced_ms = now;
        }
    }
}

fn build_worker(backend: &SimBackend, worker_id: &str) -> Worker<SimBackend> {
    Worker::builder(backend.clone())
        .worker_id(worker_id)
        .workflow_task_queue("sim-workflows")
        .activity_task_queue("sim-activities")
        // Tiny chunks force cold replays to span multiple history chunks.
        .history_chunk_events(3)
        .workflow_task_lease_duration(Duration::from_secs(1))
        .activity_task_lease_duration(Duration::from_secs(1))
        .register_workflow(sim_pipeline)
        .register_workflow(sim_lifecycle)
        .register_workflow(sim_chain)
        .register_workflow(sim_reorder)
        .register_activity(sim_double)
        .build()
}

fn workflow_claim_options(queue: &str, workflow_type: &str) -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(queue),
        registered_workflow_types: vec![WorkflowType::new(workflow_type, 1)],
        lease_duration: Duration::from_secs(1),
    }
}

fn suffix(label: &str, prefix: &str) -> Option<u64> {
    label.strip_prefix(prefix)?.parse().ok()
}

fn run_history(inner: &MemoryBackend, run_id: &RunId) -> Vec<HistoryEvent> {
    block_on(inner.stream_history(durust::StreamHistoryRequest {
        run_id: run_id.clone(),
        after_event_id: EventId::ZERO,
        up_to_event_id: EventId(u64::MAX),
        max_events: 100_000,
        max_bytes: usize::MAX,
    }))
    .expect("stream history from the inner backend")
    .events
}

fn decoded_output(history: &[HistoryEvent]) -> Option<u64> {
    history.iter().find_map(|event| match &event.data {
        HistoryEventData::WorkflowCompleted { result } => {
            durust::decode_payload::<u64>(result).ok()
        }
        _ => None,
    })
}

fn is_completed(inner: &MemoryBackend, run_id: &RunId) -> bool {
    run_history(inner, run_id)
        .iter()
        .any(|event| matches!(event.data, HistoryEventData::WorkflowCompleted { .. }))
}

fn all_completed(inner: &MemoryBackend, runs: &[RunUnderTest]) -> bool {
    runs.iter().all(|run| is_completed(inner, &run.run_id))
}

// Worker steps under fault injection: injected transient errors and fencing
// rejections are expected outcomes to retry around; anything else is a real
// bug and fails the seed.
fn tolerate_faults<T>(
    sim: &mut SimRun,
    result: durust::Result<T>,
) -> Result<Option<T>, SimFailure> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(err) if is_injected_fault(&err) || matches!(err, durust::Error::StaleLease) => {
            sim.record("tolerated_fault", err.to_string());
            Ok(None)
        }
        Err(err) => Err(sim.failure("unexpected_worker_error", err.to_string())),
    }
}

// Durable-state invariants shared by every scenario: contiguous event ids,
// exactly one WorkflowCompleted, and no duplicated command events (per
// command id, one schedule, one terminal activity event, one timer fire, one
// consumption per signal id).
fn ensure_history_invariants(
    sim: &SimRun,
    label: &str,
    history: &[HistoryEvent],
) -> Result<(), SimFailure> {
    sim.ensure(
        "history_non_empty",
        !history.is_empty(),
        format!("{label}: empty history"),
    )?;
    for (index, event) in history.iter().enumerate() {
        sim.ensure(
            "contiguous_event_ids",
            event.event_id == EventId(index as u64 + 1),
            format!("{label}: event id {} at index {index}", event.event_id.0),
        )?;
    }
    let completions = history
        .iter()
        .filter(|event| matches!(event.data, HistoryEventData::WorkflowCompleted { .. }))
        .count();
    sim.ensure(
        "exactly_one_completion",
        completions == 1,
        format!("{label}: {completions} WorkflowCompleted events"),
    )?;

    let mut seen = BTreeSet::new();
    for event in history {
        let keys: Vec<String> = match &event.data {
            HistoryEventData::WorkflowStarted { .. } => vec!["workflow-started".to_owned()],
            HistoryEventData::ActivityScheduled(e) => {
                vec![format!("activity-scheduled:{:?}", e.command_id)]
            }
            HistoryEventData::ActivityCompleted(e) => {
                vec![format!("activity-terminal:{:?}", e.command_id)]
            }
            HistoryEventData::ActivityFailed(e) => {
                vec![format!("activity-terminal:{:?}", e.command_id)]
            }
            HistoryEventData::ActivityTimedOut(e) => {
                vec![format!("activity-terminal:{:?}", e.command_id)]
            }
            HistoryEventData::TimerStarted(e) => {
                vec![format!("timer-started:{:?}", e.command_id)]
            }
            HistoryEventData::TimerFired(e) => vec![format!("timer-fired:{:?}", e.command_id)],
            HistoryEventData::SignalConsumed(e) => vec![
                format!("signal-consumed:{:?}", e.command_id),
                format!("signal-id:{:?}", e.signal_id),
            ],
            HistoryEventData::SelectWinner(e) => {
                vec![format!("select-winner:{:?}", e.select_command_id)]
            }
            _ => Vec::new(),
        };
        for key in keys {
            sim.ensure(
                "unique_command_events",
                seen.insert(key.clone()),
                format!("{label}: duplicate event {key}"),
            )?;
        }
    }
    Ok(())
}

fn verify_runs(
    sim: &SimRun,
    env: &SimEnv,
    runs: &[RunUnderTest],
) -> Result<BTreeMap<String, Vec<HistoryEvent>>, SimFailure> {
    let mut histories = BTreeMap::new();
    for run in runs {
        let history = run_history(&env.inner, &run.run_id);
        ensure_history_invariants(sim, &run.workflow_id, &history)?;
        let output = decoded_output(&history);
        sim.ensure(
            "correct_output",
            output == Some(run.expected_output),
            format!(
                "{}: output {output:?}, expected {}",
                run.workflow_id, run.expected_output
            ),
        )?;
        histories.insert(run.workflow_id.clone(), history);
    }
    Ok(histories)
}

fn scenario_outcome(
    sim: &SimRun,
    env: &SimEnv,
    runs: &[RunUnderTest],
) -> Result<ScenarioOutcome, SimFailure> {
    Ok(ScenarioOutcome {
        histories: verify_runs(sim, env, runs)?,
        injected_faults: env.backend.injected_faults(),
        duplicated_completions: env.backend.duplicated_completions(),
        observed_commit_conflicts: env.backend.observed_commit_conflicts(),
        worker_rebuilds: 0,
    })
}

/// One client send driven through the fault-injecting backend during a storm;
/// injected failures reschedule the send, modeling delayed delivery.
struct StormSignal {
    workflow_id: String,
    signal_name: &'static str,
    signal_id: String,
    payload: u64,
    at: Duration,
}

const STORM_TICKS: u64 = 40;
const MAX_DRAINS: u64 = 12;

// Shared storm driver: seeded interleaving of worker polls, maintenance
// ticks, and client sends under fault injection, then a fault-free
// convergence phase in which every workflow must finish. Returns how many
// cache-eviction worker rebuilds fired.
fn run_storm(
    sim: &mut SimRun,
    env: &mut SimEnv,
    runs: &[RunUnderTest],
    signals: Vec<StormSignal>,
    evictions: bool,
) -> Result<u64, SimFailure> {
    let mut worker = build_worker(&env.backend, "sim-storm-worker");
    let mut worker_rebuilds = 0_u64;
    let signal_client = Client::new(env.backend.clone());
    for (index, signal) in signals.iter().enumerate() {
        sim.schedule_after(signal.at, format!("send:{index}"));
    }
    sim.schedule("tick:0");

    sim.run_until_idle(4_000, |sim, step| {
        env.sync_clock(sim);
        if let Some(index) = suffix(&step.label, "send:") {
            let signal = &signals[index as usize];
            match block_on(signal_client.signal_workflow(
                signal.workflow_id.clone(),
                signal.signal_name,
                signal.signal_id.clone(),
                signal.payload,
            )) {
                Ok(_) => {}
                Err(err) if is_injected_fault(&err) => {
                    sim.record("delayed_send", step.label.clone());
                    sim.schedule_after(Duration::from_millis(15), step.label);
                }
                Err(err) => return Err(sim.failure("signal_send_error", err.to_string())),
            }
            return Ok(());
        }
        if let Some(tick) = suffix(&step.label, "tick:") {
            if evictions && sim.inject(FaultPoint::CacheEviction) {
                // Dropping the worker discards its workflow cache; the next
                // task on every cached run is a cold replay.
                worker = build_worker(&env.backend, "sim-storm-worker");
                worker_rebuilds += 1;
            }
            tolerate_faults(sim, block_on(worker.run_workflow_batch_once()))?;
            tolerate_faults(sim, block_on(worker.run_due_maintenance_once()))?;
            tolerate_faults(sim, block_on(worker.run_activity_batch_once()))?;
            if all_completed(&env.inner, runs) {
                return Ok(());
            }
            if tick + 1 < STORM_TICKS {
                sim.schedule_after(Duration::from_millis(25), format!("tick:{}", tick + 1));
            } else {
                env.backend.disable_faults();
                sim.schedule_after(Duration::from_millis(300), "drain:0".to_owned());
            }
            return Ok(());
        }
        if let Some(round) = suffix(&step.label, "drain:") {
            if let Err(err) = block_on(worker.run_until_idle()) {
                return Err(sim.failure("drain_error", err.to_string()));
            }
            if all_completed(&env.inner, runs) {
                return Ok(());
            }
            if round >= MAX_DRAINS {
                return Err(sim.failure(
                    "storm_converges",
                    "workflows did not complete after the fault window closed",
                ));
            }
            sim.schedule_after(Duration::from_millis(300), format!("drain:{}", round + 1));
            return Ok(());
        }
        Err(sim.failure("unknown_step", step.label))
    })?;
    Ok(worker_rebuilds)
}

// Scenario 1: worker crash between claim and commit. The armed crash lets the
// real worker claim through the real provider, then kills every subsequent
// call, so the inner backend keeps the claim exactly as a mid-task crash
// would leave it. A fresh worker must reclaim after lease expiry and complete
// the workflow exactly once. This is the Phase 2 lease-fix mutation target.
fn crash_between_claim_and_commit_scenario(
    sim: &mut SimRun,
) -> Result<ScenarioOutcome, SimFailure> {
    let mut env = SimEnv::new(sim);
    let client = Client::new(env.inner.clone());
    let run_id =
        block_on(client.start_workflow::<sim_pipeline>("wf/sim-crash", "sim-workflows", num(21)))
            .expect("start workflow");
    let runs = [RunUnderTest {
        workflow_id: "wf/sim-crash".to_owned(),
        run_id: run_id.clone(),
        expected_output: 43,
    }];

    // Seeded crash point: the claim of the first workflow task (nothing
    // scheduled yet) or of the second (activity already completed).
    let crash_round = sim.seed() % 2;
    let mut worker = Some(build_worker(&env.backend, "sim-crash-worker"));
    let mut crashed = false;
    let mut probed = false;
    let mut drains = 0_u64;

    sim.schedule("round:0");
    sim.run_until_idle(2_000, |sim, step| {
        env.sync_clock(sim);
        if let Some(round) = suffix(&step.label, "round:") {
            let active = worker.as_mut().expect("worker alive before crash");
            if round == crash_round {
                env.backend.crash_after_next_workflow_claim();
                match block_on(active.run_workflow_once()) {
                    Err(err) if is_injected_fault(&err) => {}
                    Err(err) => return Err(sim.failure("crash_error_kind", err.to_string())),
                    Ok(progressed) => {
                        return Err(sim.failure(
                            "crash_must_interrupt",
                            format!(
                                "run_workflow_once returned Ok({progressed}) under an armed crash"
                            ),
                        ));
                    }
                }
                // The crash drops the worker (cache, prepared commit and all)
                // while the inner backend keeps the claim.
                worker = None;
                crashed = true;
                env.backend.revive();
                sim.schedule_after(Duration::from_millis(200), "probe");
                sim.schedule_after(Duration::from_millis(1_200), "drain");
                return Ok(());
            }
            if let Err(err) = block_on(active.run_workflow_once()) {
                return Err(sim.failure("pre_crash_workflow_error", err.to_string()));
            }
            if let Err(err) = block_on(active.run_activity_once()) {
                return Err(sim.failure("pre_crash_activity_error", err.to_string()));
            }
            sim.schedule_after(Duration::from_millis(10), format!("round:{}", round + 1));
            return Ok(());
        }
        if step.label == "probe" {
            // Before the lease expires, the crashed worker's claim still
            // fences the run from fresh workers.
            let hidden = block_on(env.backend.claim_workflow_task(
                WorkerId::new("sim-crash-probe"),
                workflow_claim_options("sim-workflows", "sim.pipeline"),
            ))
            .map_err(|err| sim.failure("probe_error", err.to_string()))?;
            sim.ensure(
                "lease_fences_until_expiry",
                hidden.is_none(),
                "run was claimable before the crashed worker's lease expired",
            )?;
            probed = true;
            return Ok(());
        }
        if step.label == "drain" {
            let mut replacement = build_worker(&env.backend, "sim-crash-replacement");
            if let Err(err) = block_on(replacement.run_until_idle()) {
                return Err(sim.failure("post_crash_drain_error", err.to_string()));
            }
            if is_completed(&env.inner, &run_id) {
                return Ok(());
            }
            drains += 1;
            if drains > 8 {
                return Err(sim.failure(
                    "workflow_completes_after_crash",
                    "workflow never completed after the crash and lease expiry",
                ));
            }
            sim.schedule_after(Duration::from_millis(500), "drain");
            return Ok(());
        }
        Err(sim.failure("unknown_step", step.label))
    })?;

    sim.ensure("crash_exercised", crashed, "the armed crash never fired")?;
    sim.ensure("lease_probe_ran", probed, "the pre-expiry probe never ran")?;
    scenario_outcome(sim, &env, &runs)
}

// Scenario 4 (Phase 2 deferred ledger row): worker A claims a whole batch,
// commits only the head in time, and virtual time passes the lease
// mid-prepare. Worker B reclaims and completes the tail; A's late tail
// commits and releases are fenced as stale.
fn batch_prepare_exceeds_lease_scenario(sim: &mut SimRun) -> Result<ScenarioOutcome, SimFailure> {
    let mut env = SimEnv::new(sim);
    let client = Client::new(env.inner.clone());
    let mut runs = Vec::new();
    for index in 0..3_u64 {
        let workflow_id = format!("wf/sim-stale-{index}");
        let value = 10 + index;
        let run_id = block_on(client.start_workflow::<sim_pipeline>(
            workflow_id.clone(),
            "sim-workflows",
            num(value),
        ))
        .expect("start workflow");
        runs.push(RunUnderTest {
            workflow_id,
            run_id,
            // The head is completed by worker A's in-time commit; the tail
            // runs complete through worker B's real execution.
            expected_output: if index == 0 { 555 } else { value * 2 + 1 },
        });
    }

    // Worker A claims the batch through the real provider claim path.
    let claims = block_on(env.backend.claim_workflow_tasks(
        WorkerId::new("sim-stale-a"),
        ClaimWorkflowTasksOptions {
            claim: workflow_claim_options("sim-workflows", "sim.pipeline"),
            limit: 3,
            shard_filter: None,
        },
    ))
    .expect("batch claim");
    sim.ensure(
        "whole_batch_claimed",
        claims.len() == 3,
        format!("claimed {} of 3 tasks", claims.len()),
    )?;

    // The head commit lands within the lease.
    let head = &claims[0];
    let committed = block_on(env.backend.commit_workflow_task(
        head.claim.clone(),
        WorkflowTaskCommit {
            expected_tail_event_id: head.replay_target_event_id,
            append_events: vec![NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
                result: durust::encode_payload(&555_u64).unwrap(),
            })],
            ..WorkflowTaskCommit::default()
        },
    ))
    .map_err(|err| sim.failure("head_commit_error", err.to_string()))?;
    sim.ensure(
        "head_commit_lands_in_time",
        matches!(committed, CommitOutcome::Committed { .. }),
        format!("head commit outcome {committed:?}"),
    )?;

    sim.schedule_after(Duration::from_millis(1_200), "reclaim");
    sim.run_until_idle(1_000, |sim, step| {
        env.sync_clock(sim);
        if step.label != "reclaim" {
            return Err(sim.failure("unknown_step", step.label));
        }
        // Worker B reclaims the expired tail leases and completes the runs
        // through real execution.
        let mut worker_b = build_worker(&env.backend, "sim-stale-b");
        if let Err(err) = block_on(worker_b.run_until_idle()) {
            return Err(sim.failure("reclaim_drain_error", err.to_string()));
        }
        for run in &runs[1..] {
            sim.ensure(
                "tail_runs_complete_via_reclaim",
                is_completed(&env.inner, &run.run_id),
                format!("{} did not complete after reclaim", run.workflow_id),
            )?;
        }

        // A's late tail commits (with real append payloads) and releases must
        // be fenced and must not disturb the completed histories.
        for stale in &claims[1..] {
            let late_commit = block_on(env.backend.commit_workflow_task(
                stale.claim.clone(),
                WorkflowTaskCommit {
                    expected_tail_event_id: stale.replay_target_event_id,
                    append_events: vec![NewHistoryEvent::new(
                        HistoryEventData::WorkflowCompleted {
                            result: durust::encode_payload(&999_u64).unwrap(),
                        },
                    )],
                    ..WorkflowTaskCommit::default()
                },
            ));
            sim.ensure(
                "late_commit_fenced",
                matches!(late_commit, Err(durust::Error::StaleLease)),
                format!("late commit outcome {late_commit:?}"),
            )?;
            let late_release = block_on(env.backend.release_workflow_task(
                stale.claim.clone(),
                durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::CacheEvicted),
            ));
            sim.ensure(
                "late_release_fenced",
                matches!(late_release, Err(durust::Error::StaleLease)),
                format!("late release outcome {late_release:?}"),
            )?;
        }
        // The batch commit RPC path is fenced identically.
        let batch_results = block_on(
            env.backend
                .commit_workflow_tasks(durust::WorkflowTaskCommitBatch {
                    commits: claims[1..]
                        .iter()
                        .map(|stale| durust::WorkflowTaskCommitInput {
                            claim: stale.claim.clone(),
                            commit: WorkflowTaskCommit {
                                expected_tail_event_id: stale.replay_target_event_id,
                                ..WorkflowTaskCommit::default()
                            },
                        })
                        .collect(),
                }),
        )
        .map_err(|err| sim.failure("late_batch_commit_error", err.to_string()))?;
        for result in batch_results {
            sim.ensure(
                "late_batch_commit_fenced",
                matches!(result.result, Err(durust::Error::StaleLease)),
                format!("late batch commit result {:?}", result.result),
            )?;
        }
        Ok(())
    })?;

    scenario_outcome(sim, &env, &runs)
}

// Scenario 2: cache eviction storm. Workflows progress across tasks while the
// worker cache is randomly dropped between tasks, interleaved with signals,
// timers, and activities under transient backend faults.
fn cache_eviction_storm_scenario(sim: &mut SimRun) -> Result<ScenarioOutcome, SimFailure> {
    let mut env = SimEnv::new(sim);
    let client = Client::new(env.inner.clone());
    let mut runs = Vec::new();
    let mut signals = Vec::new();
    for index in 0..3_u64 {
        let workflow_id = format!("wf/sim-evict-{index}");
        let value = index + 1;
        let run_id = block_on(client.start_workflow::<sim_lifecycle>(
            workflow_id.clone(),
            "sim-workflows",
            num(value),
        ))
        .expect("start workflow");
        runs.push(RunUnderTest {
            workflow_id: workflow_id.clone(),
            run_id,
            expected_output: (value * 2 + 5) * 2 + 1,
        });
        signals.push(StormSignal {
            workflow_id,
            signal_name: "bump",
            signal_id: format!("bump-{index}"),
            payload: 5,
            at: Duration::from_millis(60 + index * 5),
        });
    }

    let worker_rebuilds = run_storm(sim, &mut env, &runs, signals, true)?;
    let mut outcome = scenario_outcome(sim, &env, &runs)?;
    outcome.worker_rebuilds = worker_rebuilds;
    Ok(outcome)
}

// Scenario 3: commit conflict storm. A racing activity completion lands
// between the worker's claim and its commit (through the one-shot post-claim
// hook), moving the history tail and forcing a genuine commit conflict. The
// conflicted commit must not append anything; the retried task completes the
// workflow exactly once.
fn commit_conflict_storm_scenario(sim: &mut SimRun) -> Result<ScenarioOutcome, SimFailure> {
    let mut env = SimEnv::new(sim);
    let client = Client::new(env.inner.clone());
    let values = [4_u64, 9_u64];
    let mut runs = Vec::new();
    let mut workers = Vec::new();
    let mut held: Vec<Option<CompleteActivityRequest>> = vec![None, None];
    for (index, value) in values.iter().enumerate() {
        let workflow_id = format!("wf/sim-conflict-{index}");
        let run_id = block_on(client.start_workflow::<sim_conflict>(
            workflow_id.clone(),
            format!("sim-conflict-workflows-{index}"),
            num(*value),
        ))
        .expect("start workflow");
        runs.push(RunUnderTest {
            workflow_id,
            run_id,
            expected_output: value * 2 + 3,
        });
        workers.push(
            Worker::builder(env.backend.clone())
                .worker_id(format!("sim-conflict-worker-{index}"))
                .workflow_task_queue(format!("sim-conflict-workflows-{index}"))
                .activity_task_queue(format!("sim-conflict-activities-{value}"))
                .history_chunk_events(3)
                .workflow_task_lease_duration(Duration::from_secs(1))
                .activity_task_lease_duration(Duration::from_secs(1))
                .register_workflow(sim_conflict)
                .register_activity(sim_double)
                .build(),
        );
    }

    // Both per-workflow chains are scheduled at once; the seeded scheduler
    // interleaves them.
    sim.schedule("open:0");
    sim.schedule("open:1");
    sim.run_until_idle(2_000, |sim, step| {
        env.sync_clock(sim);
        let (phase, index) = step
            .label
            .split_once(':')
            .and_then(|(phase, index)| Some((phase, index.parse::<usize>().ok()?)))
            .ok_or_else(|| sim.failure("unknown_step", step.label.clone()))?;
        let value = values[index];
        match phase {
            "open" => {
                // Task 1 commits ActivityScheduled + TimerStarted.
                if let Err(err) = block_on(workers[index].run_workflow_once()) {
                    return Err(sim.failure("open_error", err.to_string()));
                }
                // A racing activity worker claims this run's activity and
                // holds the completion.
                let claimed = block_on(env.inner.claim_activity_task(
                    WorkerId::new("sim-conflict-racer"),
                    ClaimActivityOptions {
                        namespace: Namespace::default(),
                        task_queue: TaskQueue::new(format!("sim-conflict-activities-{value}")),
                        registered_activity_names: vec![durust::ActivityName::new("sim.double")],
                        lease_duration: Duration::from_secs(5),
                    },
                ))
                .map_err(|err| sim.failure("racer_claim_error", err.to_string()))?
                .ok_or_else(|| sim.failure("racer_claim_missing", "no activity to hold"))?;
                held[index] = Some(CompleteActivityRequest {
                    claim: claimed.claim,
                    result: durust::encode_payload(&(value * 2)).unwrap(),
                });
                sim.schedule_after(Duration::from_millis(60), format!("race:{index}"));
                Ok(())
            }
            "race" => {
                // Fire the due timer so the run is claimable again.
                if let Err(err) = block_on(workers[index].run_due_maintenance_once()) {
                    return Err(sim.failure("maintenance_error", err.to_string()));
                }
                // Arm the racing completion and poll: the completion lands
                // between the claim and the commit, moving the tail.
                let inner = env.inner.clone();
                let request = held[index].take().expect("held activity completion");
                env.backend.on_next_workflow_claim(move || {
                    Box::pin(async move {
                        inner
                            .complete_activity(request)
                            .await
                            .expect("racing activity completion");
                    })
                });
                let conflicts_before = env.backend.observed_commit_conflicts();
                if let Err(err) = block_on(workers[index].run_workflow_once()) {
                    return Err(sim.failure("race_poll_error", err.to_string()));
                }
                sim.ensure(
                    "genuine_tail_conflict",
                    env.backend.observed_commit_conflicts() > conflicts_before,
                    "racing completion did not force a commit conflict",
                )?;
                sim.schedule_after(Duration::from_millis(5), format!("finish:{index}"));
                Ok(())
            }
            "finish" => {
                if let Err(err) = block_on(workers[index].run_workflow_once()) {
                    return Err(sim.failure("finish_error", err.to_string()));
                }
                if !is_completed(&env.inner, &runs[index].run_id) {
                    sim.schedule_after(Duration::from_millis(20), format!("finish:{index}"));
                }
                Ok(())
            }
            _ => Err(sim.failure("unknown_step", step.label.clone())),
        }
    })?;

    let outcome = scenario_outcome(sim, &env, &runs)?;
    sim.ensure(
        "conflicts_exercised",
        outcome.observed_commit_conflicts >= runs.len() as u64,
        format!(
            "observed {} conflicts for {} runs",
            outcome.observed_commit_conflicts,
            runs.len()
        ),
    )?;
    Ok(outcome)
}

// Scenario 5: duplicate activity completions. The wrapper replays identical
// completion requests at seeded points; providers must record a single
// ActivityCompleted per command and answer the duplicate idempotently.
fn duplicate_completion_storm_scenario(sim: &mut SimRun) -> Result<ScenarioOutcome, SimFailure> {
    let mut env = SimEnv::new(sim);
    let client = Client::new(env.inner.clone());
    let mut runs = Vec::new();
    for (index, value) in [3_u64, 5_u64].into_iter().enumerate() {
        let workflow_id = format!("wf/sim-dup-{index}");
        let run_id = block_on(client.start_workflow::<sim_chain>(
            workflow_id.clone(),
            "sim-workflows",
            num(value),
        ))
        .expect("start workflow");
        runs.push(RunUnderTest {
            workflow_id,
            run_id,
            expected_output: value * 8,
        });
    }

    let _ = run_storm(sim, &mut env, &runs, Vec::new(), false)?;
    let outcome = scenario_outcome(sim, &env, &runs)?;
    for duplicate in env.backend.duplicate_completion_outcomes() {
        sim.ensure(
            "duplicate_completion_idempotent",
            duplicate == CompleteActivityOutcome::AlreadyCompleted,
            format!("duplicate completion outcome {duplicate:?}"),
        )?;
    }
    Ok(outcome)
}

// Scenario 6: delayed and reordered delivery. Signals, activity completions,
// and timer fires arrive in seed-dependent orders (same-instant sends are
// ordered by the seeded scheduler; injected faults delay deliveries into
// later ticks).
fn delayed_reordered_delivery_scenario(sim: &mut SimRun) -> Result<ScenarioOutcome, SimFailure> {
    let mut env = SimEnv::new(sim);
    let client = Client::new(env.inner.clone());
    let mut runs = Vec::new();
    let mut signals = Vec::new();
    for (index, value) in [6_u64, 8_u64].into_iter().enumerate() {
        let workflow_id = format!("wf/sim-reorder-{index}");
        let run_id = block_on(client.start_workflow::<sim_reorder>(
            workflow_id.clone(),
            "sim-workflows",
            num(value),
        ))
        .expect("start workflow");
        runs.push(RunUnderTest {
            workflow_id: workflow_id.clone(),
            run_id,
            expected_output: 7 * 1_000_000 + 9 * 1_000 + value * 2,
        });
        // Same-instant sends: the seeded scheduler picks their order.
        signals.push(StormSignal {
            workflow_id: workflow_id.clone(),
            signal_name: "alpha",
            signal_id: format!("alpha-{index}"),
            payload: 7,
            at: Duration::from_millis(10),
        });
        signals.push(StormSignal {
            workflow_id,
            signal_name: "beta",
            signal_id: format!("beta-{index}"),
            payload: 9,
            at: Duration::from_millis(10),
        });
    }

    let _ = run_storm(sim, &mut env, &runs, signals, false)?;
    scenario_outcome(sim, &env, &runs)
}

type ScenarioFn = fn(&mut SimRun) -> Result<ScenarioOutcome, SimFailure>;

fn run_scenario_once(scenario: ScenarioFn, seed: u64, profile: FaultProfile) -> ScenarioOutcome {
    let mut sim = SimRun::new(seed).with_fault_profile(profile);
    sim.record("seed", format!("start seed={seed} profile={profile:?}"));
    let outcome = scenario(&mut sim).unwrap_or_else(|failure| panic!("{failure}"));
    assert!(sim.is_idle(), "scenario left pending steps (seed={seed})");
    outcome
}

#[test]
fn real_worker_crash_between_claim_and_commit_completes_exactly_once() {
    run_many_seeds(0, 256, FaultProfile::None, |sim| {
        crash_between_claim_and_commit_scenario(sim).map(|_| ())
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
}

#[test]
fn real_worker_batch_prepare_exceeding_lease_fences_tail_commits() {
    run_many_seeds(0, 128, FaultProfile::None, |sim| {
        batch_prepare_exceeds_lease_scenario(sim).map(|_| ())
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
}

#[test]
fn real_worker_cache_eviction_storm_matches_fault_free_control() {
    let mut total_faults = 0_u64;
    let mut total_rebuilds = 0_u64;
    run_many_seeds(0, 64, FaultProfile::Aggressive, |sim| {
        let storm = cache_eviction_storm_scenario(sim)?;
        total_faults += storm.injected_faults;
        total_rebuilds += storm.worker_rebuilds;

        // Fault-free control run with the same seed: identical schedule,
        // no evictions, no injected faults. Outputs must match exactly.
        let mut control_sim = SimRun::new(sim.seed());
        let control = cache_eviction_storm_scenario(&mut control_sim)?;
        for (workflow_id, storm_history) in &storm.histories {
            let control_history = control
                .histories
                .get(workflow_id)
                .ok_or_else(|| sim.failure("control_missing_run", workflow_id.clone()))?;
            sim.ensure(
                "storm_output_matches_control",
                decoded_output(storm_history) == decoded_output(control_history),
                format!(
                    "{workflow_id}: storm output {:?} != control output {:?}",
                    decoded_output(storm_history),
                    decoded_output(control_history)
                ),
            )?;
        }
        Ok(())
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
    assert!(
        total_faults > 0,
        "the aggressive profile never injected a fault across the seed range"
    );
    assert!(
        total_rebuilds > 0,
        "no cache-eviction worker rebuild fired across the seed range"
    );
}

#[test]
fn real_worker_commit_conflict_storm_never_duplicates_history() {
    run_many_seeds(0, 128, FaultProfile::None, |sim| {
        commit_conflict_storm_scenario(sim).map(|_| ())
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
}

#[test]
fn real_worker_duplicate_activity_completions_are_idempotent() {
    let mut total_duplicates = 0_u64;
    run_many_seeds(0, 192, FaultProfile::Aggressive, |sim| {
        let outcome = duplicate_completion_storm_scenario(sim)?;
        total_duplicates += outcome.duplicated_completions;
        Ok(())
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
    assert!(
        total_duplicates > 0,
        "no duplicate completion was injected across the seed range"
    );
}

#[test]
fn real_worker_delayed_and_reordered_delivery_completes_correctly() {
    run_many_seeds(0, 192, FaultProfile::Aggressive, |sim| {
        delayed_reordered_delivery_scenario(sim).map(|_| ())
    })
    .unwrap_or_else(|failure| panic!("{failure}"));
}

// Scenario 7: every scenario re-run with the same seed must produce
// byte-identical final histories, event by event.
#[test]
fn same_seed_reruns_produce_identical_histories() {
    let scenarios: [(&str, FaultProfile, ScenarioFn); 6] = [
        (
            "crash",
            FaultProfile::None,
            crash_between_claim_and_commit_scenario,
        ),
        (
            "stale-batch",
            FaultProfile::None,
            batch_prepare_exceeds_lease_scenario,
        ),
        (
            "eviction",
            FaultProfile::Aggressive,
            cache_eviction_storm_scenario,
        ),
        (
            "conflict",
            FaultProfile::None,
            commit_conflict_storm_scenario,
        ),
        (
            "duplicate",
            FaultProfile::Aggressive,
            duplicate_completion_storm_scenario,
        ),
        (
            "reorder",
            FaultProfile::Aggressive,
            delayed_reordered_delivery_scenario,
        ),
    ];
    for (name, profile, scenario) in scenarios {
        for seed in [0, 7, 23] {
            let first = run_scenario_once(scenario, seed, profile);
            let second = run_scenario_once(scenario, seed, profile);
            assert_eq!(
                first.histories, second.histories,
                "scenario {name} seed {seed} produced different histories across reruns"
            );
        }
    }
}
