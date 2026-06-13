use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use durust::{
    ActivityName, ActivityScheduled, ActivityTask, ClaimWorkflowTaskOptions, ClaimedWorkflowTask,
    Client, CommitOutcome, DurableBackend, EventId, HistoryEventData, MemoryBackend, Namespace,
    NewHistoryEvent, TaskQueue, Worker, WorkerId, WorkflowTaskCommit, WorkflowType,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BenchInput {
    value: u64,
}

#[durust::activity(name = "bench.double")]
async fn double(input: BenchInput) -> durust::Result<u64> {
    Ok(input.value * 2)
}

#[durust::workflow(name = "bench.double-plus-one", version = 1)]
async fn double_plus_one(input: u64) -> durust::Result<u64> {
    let doubled = durust::call_activity!(double(BenchInput { value: input }))
        .task_queue("activities")
        .await?;
    Ok(doubled + 1)
}

fn workflow_task_schedule(c: &mut Criterion) {
    c.bench_function("workflow_task_schedule_activity_memory", |b| {
        b.iter_batched(
            setup_started_worker,
            |(mut worker, _backend)| {
                block_on(async {
                    worker.run_workflow_once().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn workflow_task_claim(c: &mut Criterion) {
    c.bench_function("workflow_task_claim_memory", |b| {
        b.iter_batched(
            setup_claimable_workflow,
            |(backend, worker_id, opts)| {
                block_on(async {
                    let claimed = backend.claim_workflow_task(worker_id, opts).await.unwrap();
                    assert!(claimed.is_some());
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn workflow_task_append_commit(c: &mut Criterion) {
    c.bench_function("workflow_task_append_commit_memory", |b| {
        b.iter_batched(
            setup_claimed_workflow_for_commit,
            |state| {
                block_on(async {
                    let outcome = state
                        .backend
                        .commit_workflow_task(state.claimed.claim, state.batch)
                        .await
                        .unwrap();
                    assert!(matches!(outcome, CommitOutcome::Committed { .. }));
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn cached_wake_poll(c: &mut Criterion) {
    c.bench_function("workflow_cached_wake_poll_memory", |b| {
        b.iter_batched(
            setup_completed_activity,
            |(mut worker, _backend)| {
                block_on(async {
                    worker.run_workflow_once().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn crash_replay(c: &mut Criterion) {
    c.bench_function("workflow_replay_small_history_memory", |b| {
        b.iter_batched(
            setup_completed_activity,
            |(_cached_worker, backend)| {
                block_on(async {
                    let mut recovered = worker(backend);
                    recovered.run_workflow_once().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn setup_started_worker() -> (Worker<MemoryBackend>, MemoryBackend) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<double_plus_one>("bench/workflow", "workflows", 10)
            .await
            .unwrap();
        (worker(backend.clone()), backend)
    })
}

fn setup_completed_activity() -> (Worker<MemoryBackend>, MemoryBackend) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<double_plus_one>("bench/workflow", "workflows", 10)
            .await
            .unwrap();
        let mut worker = worker(backend.clone());
        worker.run_workflow_once().await.unwrap();
        worker.run_activity_once().await.unwrap();
        (worker, backend)
    })
}

fn setup_claimable_workflow() -> (MemoryBackend, WorkerId, ClaimWorkflowTaskOptions) {
    block_on(create_claimable_workflow())
}

async fn create_claimable_workflow() -> (MemoryBackend, WorkerId, ClaimWorkflowTaskOptions) {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    client
        .start_workflow::<double_plus_one>("bench/claim", "workflows", 10)
        .await
        .unwrap();
    (
        backend,
        WorkerId::new("bench-claim-worker"),
        claim_workflow_options(),
    )
}

struct AppendCommitBenchState {
    backend: MemoryBackend,
    claimed: ClaimedWorkflowTask,
    batch: WorkflowTaskCommit,
}

fn setup_claimed_workflow_for_commit() -> AppendCommitBenchState {
    block_on(async {
        let (backend, worker_id, opts) = create_claimable_workflow().await;
        let claimed = backend
            .claim_workflow_task(worker_id, opts)
            .await
            .unwrap()
            .expect("claimable workflow task");
        let input = durust::encode_payload(&BenchInput { value: 10 }).unwrap();
        let scheduled = ActivityScheduled {
            command_id: durust::command_id(&claimed.run_id, 0),
            activity_name: ActivityName::new("bench.double"),
            task_queue: TaskQueue::new("activities"),
            fingerprint: durust::activity_fingerprint(
                ActivityName::new("bench.double"),
                durust::payload_digest(&input),
            ),
            input,
        };
        let activity_task = ActivityTask::from_scheduled(&scheduled);
        AppendCommitBenchState {
            backend,
            claimed,
            batch: WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                    scheduled,
                ))],
                schedule_activities: vec![activity_task],
                delete_waits: Vec::new(),
            },
        }
    })
}

fn claim_workflow_options() -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("workflows"),
        registered_workflow_types: vec![WorkflowType::new("bench.double-plus-one", 1)],
        lease_duration: Duration::from_secs(30),
    }
}

fn worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(double_plus_one)
        .register_activity(double)
        .build()
}

criterion_group!(
    benches,
    workflow_task_schedule,
    workflow_task_claim,
    workflow_task_append_commit,
    cached_wake_poll,
    crash_replay
);
criterion_main!(benches);
