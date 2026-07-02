use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use durust::{
    ActivityMapTask, ActivityName, ActivityScheduled, ActivityTask, ClaimActivityOptions,
    ClaimWorkflowTaskOptions, ClaimedWorkflowTask, Client, CommitOutcome, CompleteActivityRequest,
    DurableBackend, DurableBranchExt, EventId, FireDueTimersRequest, HistoryEventData,
    MemoryBackend, Namespace, NewHistoryEvent, PayloadBlobStore, PayloadStorageConfig,
    PostgresBackend, PostgresBackendConfig, SignalWorkflowRequest, TaskQueue, TimestampMs,
    WaitKind, WaitRecord, Worker, WorkerId, WorkflowTaskCommit, WorkflowType,
};
use durust::{BoxSelectBranch, SqliteBackend, WorkerRunOptions, WorkerRunStats};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::env;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const SQLITE_SINGLE_FILE_WORKFLOWS: usize = 1_000;
const SQLITE_SINGLE_FILE_WORKERS: usize = 4;
const SQLITE_DRAIN_MAX_ITERATIONS: usize = 50_000;
static POSTGRES_BENCH_SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BenchInput {
    value: u64,
}

fn bench_input(value: u64) -> BenchInput {
    BenchInput { value }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LargePayload {
    body: String,
}

#[durust::activity(name = "bench.double")]
async fn double(input: BenchInput) -> durust::Result<u64> {
    Ok(input.value * 2)
}

#[durust::workflow(name = "bench.double-plus-one", version = 1)]
async fn double_plus_one(input: BenchInput) -> durust::Result<u64> {
    let input = input.value;
    let doubled = durust::call_activity!(double(BenchInput { value: input }))
        .task_queue("activities")
        .await?;
    Ok(doubled + 1)
}

#[durust::workflow(name = "bench.large-payload-then-timer", version = 1)]
async fn large_payload_then_timer(input: LargePayload) -> durust::Result<usize> {
    let len = input.body.len();
    durust::sleep(Duration::ZERO).await?;
    Ok(len)
}

#[durust::workflow(name = "bench.join-four-activities", version = 1)]
async fn join_four_activities(input: BenchInput) -> durust::Result<u64> {
    let input = input.value;
    let (first, second, third, fourth) = durust::join!(
        durust::call_activity!(double(BenchInput { value: input })).task_queue("activities"),
        durust::call_activity!(double(BenchInput { value: input + 1 })).task_queue("activities"),
        durust::call_activity!(double(BenchInput { value: input + 2 })).task_queue("activities"),
        durust::call_activity!(double(BenchInput { value: input + 3 })).task_queue("activities"),
    )
    .await?;
    Ok(first + second + third + fourth)
}

#[durust::workflow(name = "bench.select-all-activities", version = 1)]
async fn select_all_activities(input: BenchInput) -> durust::Result<u64> {
    let input = input.value;
    let mut branches = Vec::new();
    for offset in 0..4_u64 {
        let handle = durust::call_activity!(double(BenchInput {
            value: input + offset,
        }))
        .task_queue("activities")
        .spawn()
        .await?;
        branches.push(handle.result());
    }
    let winner = durust::select_all(branches).await?;
    Ok(winner.value)
}

#[durust::workflow(name = "bench.join-all-activities", version = 1)]
async fn join_all_activities(input: BenchInput) -> durust::Result<u64> {
    let input = input.value;
    let mut branches = Vec::new();
    for offset in 0..4_u64 {
        let handle = durust::call_activity!(double(BenchInput {
            value: input + offset,
        }))
        .task_queue("activities")
        .spawn()
        .await?;
        branches.push(handle.result());
    }
    let results = durust::join_all(branches).await?;
    Ok(results.into_iter().sum())
}

#[durust::workflow(name = "bench.version-branch", version = 1)]
async fn version_branch(input: BenchInput) -> durust::Result<u64> {
    let input = input.value;
    if durust::patched("bench-double-v2")? {
        durust::call_activity!(double(BenchInput { value: input + 1 }))
            .task_queue("activities")
            .await
    } else {
        durust::call_activity!(double(BenchInput { value: input }))
            .task_queue("activities")
            .await
    }
}

#[durust::workflow(name = "bench.child-double", version = 1)]
async fn child_double(input: BenchInput) -> durust::Result<u64> {
    let input = input.value;
    Ok(input * 2)
}

#[durust::workflow(name = "bench.child-start", version = 1)]
async fn child_start(input: BenchInput) -> durust::Result<()> {
    let input = input.value;
    let _child = durust::child!(child_double(bench_input(input)))
        .workflow_id(format!("bench/child/{input}"))
        .spawn()
        .await?;
    Ok(())
}

#[durust::workflow(name = "bench.select-signal-timer", version = 1)]
async fn select_signal_timer(input: BenchInput) -> durust::Result<String> {
    let input = input.value;
    let outcome = durust::select! {
        signal = durust::signal::<String>("ready") => {
            format!("signal:{}", signal?)
        }
        timer = durust::sleep(Duration::from_millis(input)) => {
            timer?;
            "timer".to_owned()
        }
    };
    Ok(outcome)
}

#[durust::workflow(name = "bench.select-all-mixed", version = 1)]
async fn select_all_mixed(input: BenchInput) -> durust::Result<String> {
    let input = input.value;
    let activity = durust::call_activity!(double(BenchInput { value: input }))
        .task_queue("activities")
        .spawn()
        .await?;
    let child = durust::child!(child_double(bench_input(input + 10_000)))
        .workflow_id(format!("bench/mixed-child/{input}"))
        .parent_close_policy(durust::ParentClosePolicy::Abandon)
        .spawn()
        .await?;

    let branches: Vec<BoxSelectBranch<String>> = vec![
        activity
            .result()
            .map_ok(|value| format!("activity:{value}"))
            .boxed(),
        child
            .result()
            .map_ok(|value| format!("child:{value}"))
            .boxed(),
        durust::sleep(Duration::ZERO)
            .map_ok(|_| "timer".to_owned())
            .boxed(),
    ];
    let winner = durust::select_all(branches).await?;
    Ok(format!("{}:{}", winner.branch_index, winner.value))
}

const LARGE_HISTORY_TIMERS: u64 = 64;
const HELD_HANDLE_SLEEPS: u64 = 16;
const CHILD_FANOUT_CHILDREN: u64 = 8;

#[durust::workflow(name = "bench.timer-loop-then-signal", version = 1)]
async fn timer_loop_then_signal(input: BenchInput) -> durust::Result<String> {
    let _input = input.value;
    for _ in 0..LARGE_HISTORY_TIMERS {
        durust::sleep(Duration::ZERO).await?;
    }
    durust::signal::<String>("after").await
}

#[durust::workflow(name = "bench.held-handle", version = 1)]
async fn held_handle_activity_then_sleeps(input: BenchInput) -> durust::Result<u64> {
    let input = input.value;
    let handle = durust::call_activity!(double(BenchInput { value: input }))
        .task_queue("activities")
        .spawn()
        .await?;
    for _ in 0..HELD_HANDLE_SLEEPS {
        durust::sleep(Duration::ZERO).await?;
    }
    handle.result().await
}

#[durust::workflow(name = "bench.child-fanout", version = 1)]
async fn child_fanout(input: BenchInput) -> durust::Result<u64> {
    let input = input.value;
    let mut results = Vec::new();
    for offset in 0..CHILD_FANOUT_CHILDREN {
        let child = durust::child!(child_double(bench_input(input + offset)))
            .workflow_id(format!("bench/fanout-child/{offset}"))
            .spawn()
            .await?;
        results.push(child.result());
    }
    let results = durust::join_all(results).await?;
    Ok(results.into_iter().sum())
}

#[durust::workflow(name = "bench.select-then-wait", version = 1)]
async fn select_then_wait(input: BenchInput) -> durust::Result<String> {
    let input = input.value;
    let first = durust::select! {
        signal = durust::signal::<String>("ready") => {
            format!("signal:{}", signal?)
        }
        timer = durust::sleep(Duration::from_millis(input)) => {
            timer?;
            "timer".to_owned()
        }
    };
    let after = durust::signal::<String>("after").await?;
    Ok(format!("{first}:{after}"))
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

    c.bench_function("workflow_one_activity_e2e_sqlite", |b| {
        b.iter_batched(
            setup_started_sqlite_worker,
            |(_dir, mut worker)| {
                block_on(async {
                    let stats = worker.run_until_idle().await.unwrap();
                    assert_eq!(stats.workflow_tasks, 2);
                    assert_eq!(stats.activity_tasks, 1);
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

    c.bench_function("workflow_task_claim_sqlite", |b| {
        b.iter_batched(
            setup_claimable_workflow_sqlite,
            |(_dir, backend, worker_id, opts)| {
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

    c.bench_function("workflow_task_append_commit_sqlite", |b| {
        b.iter_batched(
            setup_claimed_workflow_for_commit_sqlite,
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

    c.bench_function("workflow_replay_large_history_memory", |b| {
        b.iter_batched(
            setup_large_history_replay,
            |backend| {
                block_on(async {
                    let mut recovered = large_history_worker(backend);
                    assert!(recovered.run_workflow_once().await.unwrap());
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn held_handle_wake(c: &mut Criterion) {
    c.bench_function("held_handle_spawn_then_sleeps_memory", |b| {
        b.iter_batched(
            setup_held_handle_workflow,
            |backend| {
                block_on(async {
                    let mut worker = held_handle_worker(backend);
                    let stats = worker.run_until_idle().await.unwrap();
                    assert_eq!(stats.activity_tasks, 1);
                    assert_eq!(stats.timers_fired, HELD_HANDLE_SLEEPS as usize);
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn child_fanout_completion(c: &mut Criterion) {
    c.bench_function("child_fanout_completion_memory", |b| {
        b.iter_batched(
            setup_child_fanout_memory,
            |backend| {
                block_on(async {
                    let mut worker = child_fanout_worker(backend);
                    let stats = worker.run_until_idle().await.unwrap();
                    assert_eq!(
                        stats.child_workflow_starts_dispatched,
                        CHILD_FANOUT_CHILDREN as usize
                    );
                });
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("child_fanout_completion_sqlite", |b| {
        b.iter_batched(
            setup_child_fanout_sqlite,
            |(_dir, backend)| {
                block_on(async {
                    let mut worker = child_fanout_sqlite_worker(backend);
                    let stats = worker.run_until_idle().await.unwrap();
                    assert_eq!(
                        stats.child_workflow_starts_dispatched,
                        CHILD_FANOUT_CHILDREN as usize
                    );
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn recovery_flow_control(c: &mut Criterion) {
    c.bench_function("recovery_defer_no_admission_memory", |b| {
        b.iter_batched(
            setup_completed_activity,
            |(cached_worker, backend)| {
                drop(cached_worker);
                block_on(async {
                    let mut recovered = Worker::builder(backend)
                        .workflow_task_queue("workflows")
                        .activity_task_queue("activities")
                        .max_concurrent_recoveries(0)
                        .register_workflow(double_plus_one)
                        .register_activity(double)
                        .build();
                    assert!(recovered.run_workflow_once().await.unwrap());
                });
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("recovery_defer_event_budget_memory", |b| {
        b.iter_batched(
            setup_completed_activity,
            |(cached_worker, backend)| {
                drop(cached_worker);
                block_on(async {
                    let mut recovered = Worker::builder(backend)
                        .workflow_task_queue("workflows")
                        .activity_task_queue("activities")
                        .history_chunk_events(1)
                        .recovery_replay_event_budget(1)
                        .register_workflow(double_plus_one)
                        .register_activity(double)
                        .build();
                    assert!(recovered.run_workflow_once().await.unwrap());
                });
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("cached_wake_with_recovery_saturated_memory", |b| {
        b.iter_batched(
            setup_completed_activity_with_recovery_saturation,
            |(mut worker, _backend)| {
                block_on(async {
                    assert!(worker.run_workflow_once().await.unwrap());
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn select_registration(c: &mut Criterion) {
    c.bench_function("select_registration_memory", |b| {
        b.iter_batched(
            setup_select_registration_worker,
            |(mut worker, _backend)| {
                block_on(async {
                    worker.run_workflow_once().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn select_replay(c: &mut Criterion) {
    c.bench_function("select_replay_recorded_winner_memory", |b| {
        b.iter_batched(
            setup_select_replay,
            |backend| {
                block_on(async {
                    let mut worker = select_replay_worker(backend);
                    worker.run_workflow_once().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn bounded_join_fanout(c: &mut Criterion) {
    c.bench_function("bounded_join_fanout_memory", |b| {
        b.iter_batched(
            setup_join_fanout_worker,
            |(mut worker, _backend)| {
                block_on(async {
                    worker.run_workflow_once().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn join_all_activity_fanout(c: &mut Criterion) {
    c.bench_function("join_all_activity_fanout_memory", |b| {
        b.iter_batched(
            setup_join_all_fanout_worker,
            |(mut worker, _backend)| {
                block_on(async {
                    worker.run_workflow_once().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn select_all_activity_race(c: &mut Criterion) {
    c.bench_function("select_all_activity_race_memory", |b| {
        b.iter_batched(
            setup_select_all_activity_race,
            |(mut worker, _backend)| {
                block_on(async {
                    worker.run_workflow_once().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn child_start_dispatch(c: &mut Criterion) {
    c.bench_function("child_start_dispatch_memory", |b| {
        b.iter_batched(
            setup_child_start_outbox,
            |backend| {
                block_on(async {
                    let outcome = backend
                        .dispatch_child_workflow_starts(
                            durust::DispatchChildWorkflowStartsRequest {
                                namespace: Namespace::default(),
                                limit: 16,
                            },
                        )
                        .await
                        .unwrap();
                    assert_eq!(outcome.dispatched, 1);
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn projection_update(c: &mut Criterion) {
    c.bench_function("query_projection_update_memory", |b| {
        b.iter_batched(
            setup_claimed_projection_update,
            |(backend, claimed, payload)| {
                block_on(async {
                    let outcome = backend
                        .commit_workflow_task(
                            claimed.claim,
                            WorkflowTaskCommit {
                                expected_tail_event_id: EventId(1),
                                append_events: Vec::new(),
                                upsert_waits: Vec::new(),
                                schedule_activities: Vec::new(),
                                schedule_activity_maps: Vec::new(),
                                schedule_child_workflow_maps: Vec::new(),
                                start_child_workflows: Vec::new(),
                                consume_signals: Vec::new(),
                                delete_waits: Vec::new(),
                                cancel_commands: Vec::new(),
                                query_projection: Some(payload),
                            },
                        )
                        .await
                        .unwrap();
                    assert!(matches!(outcome, CommitOutcome::Committed { .. }));
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn projection_read(c: &mut Criterion) {
    c.bench_function("query_projection_read_memory", |b| {
        b.iter_batched(
            setup_projection_read,
            |(backend, req)| {
                block_on(async {
                    let outcome = backend.query_projection(req).await.unwrap();
                    assert!(matches!(
                        outcome,
                        durust::QueryProjectionOutcome::Found { .. }
                    ));
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn version_marker_replay(c: &mut Criterion) {
    c.bench_function("version_marker_lookup_replay_memory", |b| {
        b.iter_batched(
            setup_version_marker_replay,
            |backend| {
                block_on(async {
                    let mut worker = version_replay_worker(backend);
                    worker.run_workflow_once().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn activity_claim_complete(c: &mut Criterion) {
    c.bench_function("activity_claim_complete_memory", |b| {
        b.iter_batched(
            setup_scheduled_activity,
            |(backend, worker_id, opts)| {
                block_on(async {
                    let claimed = backend
                        .claim_activity_task(worker_id, opts)
                        .await
                        .unwrap()
                        .expect("activity task");
                    let completed = backend
                        .complete_activity(CompleteActivityRequest {
                            claim: claimed.claim,
                            result: durust::encode_payload(&20_u64).unwrap(),
                        })
                        .await
                        .unwrap();
                    assert!(matches!(
                        completed,
                        durust::CompleteActivityOutcome::Completed { .. }
                    ));
                });
            },
            BatchSize::SmallInput,
        );
    });

    c.bench_function("activity_claim_complete_sqlite", |b| {
        b.iter_batched(
            setup_scheduled_activity_sqlite,
            |(_dir, backend, worker_id, opts)| {
                block_on(async {
                    let claimed = backend
                        .claim_activity_task(worker_id, opts)
                        .await
                        .unwrap()
                        .expect("activity task");
                    let completed = backend
                        .complete_activity(CompleteActivityRequest {
                            claim: claimed.claim,
                            result: durust::encode_payload(&20_u64).unwrap(),
                        })
                        .await
                        .unwrap();
                    assert!(matches!(
                        completed,
                        durust::CompleteActivityOutcome::Completed { .. }
                    ));
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn sqlite_single_file_mixed_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite_single_file_throughput");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(SQLITE_SINGLE_FILE_WORKFLOWS as u64));
    group.bench_function("drain_1000_mixed_workflows_4_workers", |b| {
        b.iter_batched(
            setup_sqlite_single_file_mixed_workflows,
            |(_dir, backend)| {
                let stats =
                    drain_sqlite_workers_concurrently(backend.clone(), SQLITE_SINGLE_FILE_WORKERS);
                assert_mixed_sqlite_stats(stats);
                block_on(async {
                    let mut idle_check = sqlite_mixed_worker(backend, SQLITE_SINGLE_FILE_WORKERS);
                    let idle_stats = idle_check
                        .run_until_idle_with(WorkerRunOptions {
                            max_iterations: SQLITE_DRAIN_MAX_ITERATIONS,
                        })
                        .await
                        .unwrap();
                    assert_eq!(idle_stats, WorkerRunStats::default());
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn postgres_provider_hot_paths(c: &mut Criterion) {
    let Some(database_url) = postgres_benchmark_url() else {
        return;
    };
    let mut group = c.benchmark_group("postgres_provider_hot_paths");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("workflow_task_claim_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "claim",
                iters,
                |fixture, iteration| {
                    start_postgres_workflow(fixture, "claim", iteration);
                },
                |fixture, values| {
                    fixture.runtime.block_on(async {
                        for _ in values {
                            let claimed = fixture
                                .backend
                                .claim_workflow_task(
                                    WorkerId::new("bench-postgres-claim-worker"),
                                    claim_workflow_options(),
                                )
                                .await
                                .unwrap();
                            assert!(claimed.is_some());
                        }
                    });
                },
            )
        });
    });

    group.bench_function("workflow_task_append_commit_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "commit",
                iters,
                setup_postgres_claimed_workflow_for_commit,
                |fixture, values| {
                    fixture.runtime.block_on(async {
                        for (claimed, batch) in values {
                            let outcome = fixture
                                .backend
                                .commit_workflow_task(claimed.claim, batch)
                                .await
                                .unwrap();
                            assert!(matches!(outcome, CommitOutcome::Committed { .. }));
                        }
                    });
                },
            )
        });
    });

    group.bench_function("history_stream_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "history",
                iters,
                setup_postgres_history_stream,
                |fixture, run_ids| {
                    fixture.runtime.block_on(async {
                        for run_id in run_ids {
                            let chunk = fixture
                                .backend
                                .stream_history_for_replay(durust::StreamHistoryRequest {
                                    run_id,
                                    after_event_id: EventId::ZERO,
                                    up_to_event_id: EventId(129),
                                    max_events: 32,
                                    max_bytes: usize::MAX,
                                })
                                .await
                                .unwrap();
                            assert_eq!(chunk.events.len(), 32);
                            assert!(chunk.has_more);
                        }
                    });
                },
            )
        });
    });

    group.bench_function("history_stream_chunked_replay_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "history_chunked",
                iters,
                setup_postgres_large_history_stream,
                |fixture, run_ids| {
                    fixture.runtime.block_on(async {
                        for run_id in run_ids {
                            let mut after = EventId::ZERO;
                            let mut total = 0usize;
                            loop {
                                let chunk = fixture
                                    .backend
                                    .stream_history_for_replay(durust::StreamHistoryRequest {
                                        run_id: run_id.clone(),
                                        after_event_id: after,
                                        up_to_event_id: EventId(
                                            POSTGRES_CHUNKED_HISTORY_EVENTS + 1,
                                        ),
                                        max_events: 128,
                                        max_bytes: usize::MAX,
                                    })
                                    .await
                                    .unwrap();
                                total += chunk.events.len();
                                after = chunk.last_event_id;
                                if !chunk.has_more {
                                    break;
                                }
                            }
                            assert_eq!(
                                total,
                                usize::try_from(POSTGRES_CHUNKED_HISTORY_EVENTS).unwrap() + 1
                            );
                        }
                    });
                },
            )
        });
    });

    group.bench_function("activity_claim_complete_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "activity",
                iters,
                setup_postgres_scheduled_activity,
                |fixture, values| {
                    fixture.runtime.block_on(async {
                        for _ in values {
                            let claimed = fixture
                                .backend
                                .claim_activity_task(
                                    WorkerId::new("bench-postgres-activity-worker"),
                                    claim_activity_options("activities"),
                                )
                                .await
                                .unwrap()
                                .expect("activity task");
                            let completed = fixture
                                .backend
                                .complete_activity(CompleteActivityRequest {
                                    claim: claimed.claim,
                                    result: durust::encode_payload(&20_u64).unwrap(),
                                })
                                .await
                                .unwrap();
                            assert!(matches!(
                                completed,
                                durust::CompleteActivityOutcome::Completed { .. }
                            ));
                        }
                    });
                },
            )
        });
    });

    group.bench_function("activity_heartbeat_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "heartbeat",
                iters,
                setup_postgres_claimed_heartbeat_activity,
                |fixture, claims| {
                    fixture.runtime.block_on(async {
                        for claim in claims {
                            let outcome = fixture
                                .backend
                                .heartbeat_activity(durust::ActivityHeartbeatRequest { claim })
                                .await
                                .unwrap();
                            assert_eq!(outcome, durust::ActivityHeartbeatOutcome::Recorded);
                        }
                    });
                },
            )
        });
    });

    group.bench_function("timer_due_scan_wakeup_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "timer",
                iters,
                setup_postgres_due_timer,
                |fixture, values| {
                    fixture.runtime.block_on(async {
                        for _ in values {
                            let fired = fixture
                                .backend
                                .fire_due_timers(FireDueTimersRequest {
                                    namespace: Namespace::default(),
                                    now: TimestampMs(10),
                                    limit: 1,
                                })
                                .await
                                .unwrap();
                            assert_eq!(fired.fired, 1);
                        }
                    });
                },
            )
        });
    });

    group.bench_function("signal_send_consume_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "signal",
                iters,
                setup_postgres_signal_wait,
                |fixture, values| {
                    fixture.runtime.block_on(async {
                        for (run_id, workflow_id, signal_id) in values {
                            let outcome = fixture
                                .backend
                                .signal_workflow(SignalWorkflowRequest {
                                    namespace: Namespace::default(),
                                    workflow_id,
                                    signal_id: signal_id.clone(),
                                    signal_name: durust::SignalName::new("ready"),
                                    payload: durust::encode_payload(&"ready").unwrap(),
                                })
                                .await
                                .unwrap();
                            assert_eq!(outcome, durust::SignalWorkflowOutcome::Accepted);
                            let inbox = fixture
                                .backend
                                .read_signal_inbox(durust::ReadSignalInboxRequest {
                                    run_id,
                                    signal_name: durust::SignalName::new("ready"),
                                })
                                .await
                                .unwrap()
                                .expect("signal inbox record");
                            assert_eq!(inbox.signal_id, signal_id);
                        }
                    });
                },
            )
        });
    });

    group.bench_function("query_projection_update_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "projection_update",
                iters,
                setup_postgres_claimed_projection_update,
                |fixture, values| {
                    fixture.runtime.block_on(async {
                        for (claimed, payload) in values {
                            let outcome = fixture
                                .backend
                                .commit_workflow_task(
                                    claimed.claim,
                                    WorkflowTaskCommit {
                                        expected_tail_event_id: EventId(1),
                                        append_events: Vec::new(),
                                        upsert_waits: Vec::new(),
                                        schedule_activities: Vec::new(),
                                        schedule_activity_maps: Vec::new(),
                                        schedule_child_workflow_maps: Vec::new(),
                                        start_child_workflows: Vec::new(),
                                        consume_signals: Vec::new(),
                                        delete_waits: Vec::new(),
                                        cancel_commands: Vec::new(),
                                        query_projection: Some(payload),
                                    },
                                )
                                .await
                                .unwrap();
                            assert!(matches!(outcome, CommitOutcome::Committed { .. }));
                        }
                    });
                },
            )
        });
    });

    group.bench_function("query_projection_read_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "projection_read",
                iters,
                setup_postgres_projection_read,
                |fixture, requests| {
                    fixture.runtime.block_on(async {
                        for req in requests {
                            let outcome = fixture.backend.query_projection(req).await.unwrap();
                            assert!(matches!(
                                outcome,
                                durust::QueryProjectionOutcome::Found { .. }
                            ));
                        }
                    });
                },
            )
        });
    });

    group.bench_function("child_workflow_start_parent_wakeup_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "child",
                iters,
                setup_postgres_child_start,
                |fixture, values| {
                    fixture.runtime.block_on(async {
                        for (claimed, batch) in values {
                            let outcome = fixture
                                .backend
                                .commit_workflow_task(claimed.claim, batch)
                                .await
                                .unwrap();
                            assert_eq!(
                                outcome,
                                CommitOutcome::Committed {
                                    new_tail_event_id: EventId(3)
                                }
                            );
                        }
                    });
                },
            )
        });
    });

    group.bench_function("activity_map_schedule_complete_postgres", |b| {
        let database_url = database_url.clone();
        b.iter_custom(|iters| {
            measure_postgres_bench(
                database_url.clone(),
                "activity_map",
                iters,
                setup_postgres_claimed_activity_map_workflow,
                |fixture, values| {
                    fixture.runtime.block_on(async {
                        for (iteration, (claimed, map_task, scheduled)) in
                            values.into_iter().enumerate()
                        {
                            let outcome = fixture
                                .backend
                                .commit_workflow_task(
                                    claimed.claim,
                                    WorkflowTaskCommit {
                                        expected_tail_event_id: EventId(1),
                                        append_events: vec![NewHistoryEvent::new(
                                            HistoryEventData::ActivityMapScheduled(scheduled),
                                        )],
                                        upsert_waits: Vec::new(),
                                        schedule_activities: Vec::new(),
                                        schedule_activity_maps: vec![map_task],
                                        schedule_child_workflow_maps: Vec::new(),
                                        start_child_workflows: Vec::new(),
                                        consume_signals: Vec::new(),
                                        delete_waits: Vec::new(),
                                        cancel_commands: Vec::new(),
                                        query_projection: None,
                                    },
                                )
                                .await
                                .unwrap();
                            assert!(matches!(outcome, CommitOutcome::Committed { .. }));
                            for value in 0..8_u64 {
                                let claimed = fixture
                                    .backend
                                    .claim_activity_task(
                                        WorkerId::new(format!(
                                            "bench-postgres-map-worker-{iteration}-{value}"
                                        )),
                                        claim_activity_options("activities"),
                                    )
                                    .await
                                    .unwrap()
                                    .expect("map item task");
                                let completed = fixture
                                    .backend
                                    .complete_activity(CompleteActivityRequest {
                                        claim: claimed.claim,
                                        result: durust::encode_payload(&(value * 2)).unwrap(),
                                    })
                                    .await
                                    .unwrap();
                                assert!(matches!(
                                    completed,
                                    durust::CompleteActivityOutcome::Completed { .. }
                                ));
                            }
                        }
                    });
                },
            )
        });
    });

    group.finish();
}

fn activity_heartbeat(c: &mut Criterion) {
    c.bench_function("activity_heartbeat_memory", |b| {
        b.iter_batched(
            setup_claimed_heartbeat_activity,
            |(backend, claim)| {
                block_on(async {
                    let outcome = backend
                        .heartbeat_activity(durust::ActivityHeartbeatRequest { claim })
                        .await
                        .unwrap();
                    assert_eq!(outcome, durust::ActivityHeartbeatOutcome::Recorded);
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn timer_due_scan_wakeup(c: &mut Criterion) {
    c.bench_function("timer_due_scan_wakeup_memory", |b| {
        b.iter_batched(
            setup_due_timer,
            |backend| {
                block_on(async {
                    let fired = backend
                        .fire_due_timers(FireDueTimersRequest {
                            namespace: Namespace::default(),
                            now: TimestampMs(10),
                            limit: 1024,
                        })
                        .await
                        .unwrap();
                    assert_eq!(fired.fired, 1);
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn signal_send_consume(c: &mut Criterion) {
    c.bench_function("signal_send_consume_memory", |b| {
        b.iter_batched(
            setup_signal_wait,
            |(backend, run_id)| {
                block_on(async {
                    let signal_id = durust::SignalId::new("bench/signal");
                    let outcome = backend
                        .signal_workflow(SignalWorkflowRequest {
                            namespace: Namespace::default(),
                            workflow_id: durust::WorkflowId::new("bench/signal"),
                            signal_id: signal_id.clone(),
                            signal_name: durust::SignalName::new("ready"),
                            payload: durust::encode_payload(&"ready").unwrap(),
                        })
                        .await
                        .unwrap();
                    assert_eq!(outcome, durust::SignalWorkflowOutcome::Accepted);
                    let claimed = backend
                        .claim_workflow_task(
                            WorkerId::new("bench-signal-consumer"),
                            claim_workflow_options(),
                        )
                        .await
                        .unwrap()
                        .expect("signal-ready workflow task");
                    let inbox = backend
                        .read_signal_inbox(durust::ReadSignalInboxRequest {
                            run_id,
                            signal_name: durust::SignalName::new("ready"),
                        })
                        .await
                        .unwrap()
                        .expect("signal inbox record");
                    let commit = backend
                        .commit_workflow_task(
                            claimed.claim,
                            WorkflowTaskCommit {
                                expected_tail_event_id: EventId(1),
                                append_events: Vec::new(),
                                upsert_waits: Vec::new(),
                                schedule_activities: Vec::new(),
                                schedule_activity_maps: Vec::new(),
                                schedule_child_workflow_maps: Vec::new(),
                                start_child_workflows: Vec::new(),
                                consume_signals: vec![inbox.signal_id],
                                delete_waits: Vec::new(),
                                cancel_commands: Vec::new(),
                                query_projection: None,
                            },
                        )
                        .await
                        .unwrap();
                    assert!(matches!(commit, CommitOutcome::Committed { .. }));
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn activity_map_materialize(c: &mut Criterion) {
    c.bench_function("activity_map_materialize_memory", |b| {
        b.iter_batched(
            setup_claimed_activity_map_workflow,
            |(backend, claimed, map_task, scheduled)| {
                block_on(async {
                    let outcome = backend
                        .commit_workflow_task(
                            claimed.claim,
                            WorkflowTaskCommit {
                                expected_tail_event_id: EventId(1),
                                append_events: vec![NewHistoryEvent::new(
                                    HistoryEventData::ActivityMapScheduled(scheduled),
                                )],
                                upsert_waits: Vec::new(),
                                schedule_activities: Vec::new(),
                                schedule_activity_maps: vec![map_task],
                                schedule_child_workflow_maps: Vec::new(),
                                start_child_workflows: Vec::new(),
                                consume_signals: Vec::new(),
                                delete_waits: Vec::new(),
                                cancel_commands: Vec::new(),
                                query_projection: None,
                            },
                        )
                        .await
                        .unwrap();
                    assert!(matches!(outcome, CommitOutcome::Committed { .. }));
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn activity_map_item_complete(c: &mut Criterion) {
    c.bench_function("activity_map_item_complete_memory", |b| {
        b.iter_batched(
            setup_materialized_activity_map,
            |(backend, worker_id, opts)| {
                block_on(async {
                    let claimed = backend
                        .claim_activity_task(worker_id, opts)
                        .await
                        .unwrap()
                        .expect("map item task");
                    let completed = backend
                        .complete_activity(CompleteActivityRequest {
                            claim: claimed.claim,
                            result: durust::encode_payload(&20_u64).unwrap(),
                        })
                        .await
                        .unwrap();
                    assert!(matches!(
                        completed,
                        durust::CompleteActivityOutcome::Completed { .. }
                    ));
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn child_workflow_map_materialize(c: &mut Criterion) {
    c.bench_function("child_workflow_map_materialize_memory", |b| {
        b.iter_batched(
            setup_claimed_child_workflow_map_workflow,
            |(backend, claimed, map_task, scheduled)| {
                block_on(async {
                    let outcome = backend
                        .commit_workflow_task(
                            claimed.claim,
                            WorkflowTaskCommit {
                                expected_tail_event_id: EventId(1),
                                append_events: vec![NewHistoryEvent::new(
                                    HistoryEventData::ChildWorkflowMapScheduled(scheduled),
                                )],
                                upsert_waits: Vec::new(),
                                schedule_activities: Vec::new(),
                                schedule_activity_maps: Vec::new(),
                                schedule_child_workflow_maps: vec![map_task],
                                start_child_workflows: Vec::new(),
                                consume_signals: Vec::new(),
                                delete_waits: Vec::new(),
                                cancel_commands: Vec::new(),
                                query_projection: None,
                            },
                        )
                        .await
                        .unwrap();
                    assert!(matches!(outcome, CommitOutcome::Committed { .. }));
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn child_workflow_map_item_complete(c: &mut Criterion) {
    c.bench_function("child_workflow_map_item_complete_memory", |b| {
        b.iter_batched(
            setup_materialized_child_workflow_map,
            |(backend, child_claim)| {
                block_on(async {
                    let outcome = backend
                        .commit_workflow_task(
                            child_claim.claim,
                            WorkflowTaskCommit {
                                expected_tail_event_id: EventId(1),
                                append_events: vec![NewHistoryEvent::new(
                                    HistoryEventData::WorkflowCompleted {
                                        result: durust::encode_payload(&20_u64).unwrap(),
                                    },
                                )],
                                upsert_waits: Vec::new(),
                                schedule_activities: Vec::new(),
                                schedule_activity_maps: Vec::new(),
                                schedule_child_workflow_maps: Vec::new(),
                                start_child_workflows: Vec::new(),
                                consume_signals: Vec::new(),
                                delete_waits: Vec::new(),
                                cancel_commands: Vec::new(),
                                query_projection: None,
                            },
                        )
                        .await
                        .unwrap();
                    assert!(matches!(outcome, CommitOutcome::Committed { .. }));
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn payload_codec(c: &mut Criterion) {
    let payload = large_payload();
    c.bench_function("payload_encode_messagepack_64kb", |b| {
        b.iter(|| durust::encode_payload(black_box(&payload)).unwrap());
    });
    let messagepack = durust::encode_payload(&payload).unwrap();
    c.bench_function("payload_decode_messagepack_64kb", |b| {
        b.iter(|| durust::decode_payload::<LargePayload>(black_box(&messagepack)).unwrap());
    });

    c.bench_function("payload_encode_json_64kb", |b| {
        b.iter(|| {
            durust::encode_payload_with_codec(black_box(&payload), durust::CodecId::Json).unwrap()
        });
    });
    let json = durust::encode_payload_with_codec(&payload, durust::CodecId::Json).unwrap();
    c.bench_function("payload_decode_json_64kb", |b| {
        b.iter(|| black_box(&json).decode_json::<LargePayload>().unwrap());
    });
}

fn payload_compression(c: &mut Criterion) {
    let repetitive = encoded_payload_bytes(&large_payload());
    let mixed = encoded_payload_bytes(&mixed_large_payload());
    let repetitive_compressed = zstd::bulk::compress(&repetitive, 3).unwrap();
    let mixed_compressed = zstd::bulk::compress(&mixed, 3).unwrap();

    let mut group = c.benchmark_group("payload_compression_64kb");
    group.throughput(Throughput::Bytes(repetitive.len() as u64));
    group.bench_function("zstd_compress_repetitive_messagepack", |b| {
        b.iter(|| zstd::bulk::compress(black_box(&repetitive), 3).unwrap());
    });
    group.bench_function("zstd_decompress_repetitive_messagepack", |b| {
        b.iter(|| {
            zstd::bulk::decompress(black_box(&repetitive_compressed), repetitive.len()).unwrap()
        });
    });
    group.bench_function("zstd_compress_mixed_messagepack", |b| {
        b.iter(|| zstd::bulk::compress(black_box(&mixed), 3).unwrap());
    });
    group.bench_function("zstd_decompress_mixed_messagepack", |b| {
        b.iter(|| zstd::bulk::decompress(black_box(&mixed_compressed), mixed.len()).unwrap());
    });
    group.finish();
}

fn payload_garage_object_store(c: &mut Criterion) {
    let Some(config) = garage_config_from_env() else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store = durust::S3BlobStore::garage(config).unwrap();
    runtime
        .block_on(store.list_payload_blobs())
        .expect("Garage S3 benchmark store must be reachable");

    let bytes = encoded_payload_bytes(&large_payload());
    let digest = durust::digest_bytes(&bytes);
    runtime
        .block_on(store.put_payload_blob(digest.clone(), bytes.clone()))
        .unwrap();

    let mut group = c.benchmark_group("payload_garage_object_store_64kb");
    group.throughput(Throughput::Bytes(bytes.len() as u64));
    group.bench_function("get_existing_blob", |b| {
        b.iter(|| {
            let bytes = runtime
                .block_on(store.get_payload_blob(black_box(digest.clone())))
                .unwrap();
            black_box(bytes);
        });
    });

    let mut sequence = 0_u64;
    group.bench_function("put_unique_blob", |b| {
        b.iter_batched(
            || {
                sequence = sequence.saturating_add(1);
                let mut bytes = bytes.clone();
                bytes.extend_from_slice(&sequence.to_le_bytes());
                let digest = durust::digest_bytes(&bytes);
                (digest, bytes)
            },
            |(digest, bytes)| {
                let uri = runtime
                    .block_on(store.put_payload_blob(black_box(digest), black_box(bytes)))
                    .unwrap();
                black_box(uri);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn payload_provider_refs(c: &mut Criterion) {
    c.bench_function("payload_inline_history_stream_memory_64kb", |b| {
        b.iter_batched(
            || setup_payload_history(usize::MAX),
            |(backend, run_id)| {
                block_on(async {
                    let chunk = backend
                        .stream_history(durust::StreamHistoryRequest {
                            run_id,
                            after_event_id: EventId::ZERO,
                            up_to_event_id: EventId(1),
                            max_events: 100,
                            max_bytes: usize::MAX,
                        })
                        .await
                        .unwrap();
                    assert_eq!(chunk.events.len(), 1);
                });
            },
            BatchSize::SmallInput,
        );
    });
    c.bench_function("payload_blob_history_stream_memory_64kb", |b| {
        b.iter_batched(
            || setup_payload_history(1),
            |(backend, run_id)| {
                block_on(async {
                    let chunk = backend
                        .stream_history(durust::StreamHistoryRequest {
                            run_id,
                            after_event_id: EventId::ZERO,
                            up_to_event_id: EventId(1),
                            max_events: 100,
                            max_bytes: usize::MAX,
                        })
                        .await
                        .unwrap();
                    assert_eq!(chunk.events.len(), 1);
                    assert!(backend.payload_blob_count() > 0);
                });
            },
            BatchSize::SmallInput,
        );
    });
}

fn payload_replay(c: &mut Criterion) {
    c.bench_function("workflow_replay_large_payload_inline_memory_64kb", |b| {
        b.iter_batched(
            || setup_large_payload_timer_replay(usize::MAX),
            |(backend, run_id)| {
                block_on(async {
                    assert_eq!(backend.payload_blob_count(), 0);
                    let mut recovered = large_payload_replay_worker(backend.clone());
                    assert!(recovered.run_workflow_once().await.unwrap());
                    assert_large_payload_workflow_completed(&backend, &run_id).await;
                });
            },
            BatchSize::SmallInput,
        );
    });
    c.bench_function("workflow_replay_large_payload_blob_memory_64kb", |b| {
        b.iter_batched(
            || setup_large_payload_timer_replay(1),
            |(backend, run_id)| {
                block_on(async {
                    assert!(backend.payload_blob_count() > 0);
                    let mut recovered = large_payload_replay_worker(backend.clone());
                    assert!(recovered.run_workflow_once().await.unwrap());
                    assert_large_payload_workflow_completed(&backend, &run_id).await;
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
            .start_workflow::<double_plus_one>("bench/workflow", "workflows", bench_input(10))
            .await
            .unwrap();
        (worker(backend.clone()), backend)
    })
}

fn setup_started_sqlite_worker() -> (tempfile::TempDir, Worker<SqliteBackend>) {
    block_on(async {
        let (dir, backend) = sqlite_backend();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<double_plus_one>("bench/workflow", "workflows", bench_input(10))
            .await
            .unwrap();
        (dir, sqlite_worker(backend))
    })
}

fn setup_completed_activity() -> (Worker<MemoryBackend>, MemoryBackend) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<double_plus_one>("bench/workflow", "workflows", bench_input(10))
            .await
            .unwrap();
        let mut worker = worker(backend.clone());
        worker.run_workflow_once().await.unwrap();
        worker.run_activity_once().await.unwrap();
        (worker, backend)
    })
}

fn setup_completed_activity_with_recovery_saturation() -> (Worker<MemoryBackend>, MemoryBackend) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<double_plus_one>("bench/workflow", "workflows", bench_input(10))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .max_concurrent_recoveries(0)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        worker.run_workflow_once().await.unwrap();
        worker.run_activity_once().await.unwrap();
        (worker, backend)
    })
}

fn setup_select_registration_worker() -> (Worker<MemoryBackend>, MemoryBackend) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<select_signal_timer>(
                "bench/select-registration",
                "workflows",
                bench_input(10),
            )
            .await
            .unwrap();
        (select_worker(backend.clone()), backend)
    })
}

fn setup_select_replay() -> MemoryBackend {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<select_then_wait>("bench/select-replay", "workflows", bench_input(10))
            .await
            .unwrap();
        let mut worker = select_replay_worker(backend.clone());
        worker.run_workflow_once().await.unwrap();
        backend.advance_time(Duration::from_millis(10));
        worker.run_timers_once().await.unwrap();
        worker.run_workflow_once().await.unwrap();
        drop(worker);
        client
            .signal_workflow(
                "bench/select-replay",
                "after",
                "bench/select-replay/after",
                "done",
            )
            .await
            .unwrap();
        backend
    })
}

fn setup_large_history_replay() -> MemoryBackend {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<timer_loop_then_signal>(
                "bench/large-history",
                "workflows",
                bench_input(1),
            )
            .await
            .unwrap();
        let mut worker = large_history_worker(backend.clone());
        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.timers_fired, LARGE_HISTORY_TIMERS as usize);
        drop(worker);
        client
            .signal_workflow(
                "bench/large-history",
                "after",
                "bench/large-history/after",
                "done",
            )
            .await
            .unwrap();
        backend
    })
}

fn large_history_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .register_workflow(timer_loop_then_signal)
        .build()
}

fn setup_held_handle_workflow() -> MemoryBackend {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<held_handle_activity_then_sleeps>(
                "bench/held-handle",
                "workflows",
                bench_input(21),
            )
            .await
            .unwrap();
        backend
    })
}

fn held_handle_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(held_handle_activity_then_sleeps)
        .register_activity(double)
        .build()
}

fn setup_child_fanout_memory() -> MemoryBackend {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<child_fanout>("bench/child-fanout", "workflows", bench_input(5))
            .await
            .unwrap();
        backend
    })
}

fn child_fanout_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .register_workflow(child_fanout)
        .register_workflow(child_double)
        .build()
}

fn setup_child_fanout_sqlite() -> (tempfile::TempDir, SqliteBackend) {
    block_on(async {
        let (dir, backend) = sqlite_backend();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<child_fanout>("bench/child-fanout", "workflows", bench_input(5))
            .await
            .unwrap();
        (dir, backend)
    })
}

fn child_fanout_sqlite_worker(backend: SqliteBackend) -> Worker<SqliteBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .register_workflow(child_fanout)
        .register_workflow(child_double)
        .build()
}

fn setup_child_start_outbox() -> MemoryBackend {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<child_start>("bench/child-parent", "workflows", bench_input(42))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(child_start)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());
        backend
    })
}

fn setup_version_marker_replay() -> MemoryBackend {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<version_branch>("bench/version-branch", "workflows", bench_input(10))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(version_branch)
            .register_activity(double)
            .build();
        worker.run_workflow_once().await.unwrap();
        worker.run_activity_once().await.unwrap();
        backend
    })
}

fn setup_join_fanout_worker() -> (Worker<MemoryBackend>, MemoryBackend) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<join_four_activities>(
                "bench/join-fanout",
                "workflows",
                bench_input(10),
            )
            .await
            .unwrap();
        (join_worker(backend.clone()), backend)
    })
}

fn setup_join_all_fanout_worker() -> (Worker<MemoryBackend>, MemoryBackend) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<join_all_activities>(
                "bench/join-all-fanout",
                "workflows",
                bench_input(10),
            )
            .await
            .unwrap();
        (join_all_worker(backend.clone()), backend)
    })
}

fn setup_select_all_activity_race() -> (Worker<MemoryBackend>, MemoryBackend) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<select_all_activities>(
                "bench/select-all",
                "workflows",
                bench_input(10),
            )
            .await
            .unwrap();
        let mut worker = select_all_worker(backend.clone());
        worker.run_workflow_once().await.unwrap();
        let claimed = backend
            .claim_activity_task(
                WorkerId::new("bench-select-all-activity"),
                claim_activity_options("activities"),
            )
            .await
            .unwrap()
            .expect("select_all activity");
        backend
            .complete_activity(CompleteActivityRequest {
                claim: claimed.claim,
                result: durust::encode_payload(&20_u64).unwrap(),
            })
            .await
            .unwrap();
        (worker, backend)
    })
}

fn setup_claimed_projection_update() -> (MemoryBackend, ClaimedWorkflowTask, durust::PayloadRef) {
    block_on(async {
        let (backend, worker_id, opts) = create_claimable_workflow().await;
        let claimed = backend
            .claim_workflow_task(worker_id, opts)
            .await
            .unwrap()
            .expect("workflow task");
        let payload = durust::encode_payload(&BenchInput { value: 10 }).unwrap();
        (backend, claimed, payload)
    })
}

fn setup_projection_read() -> (MemoryBackend, durust::QueryProjectionRequest) {
    let (backend, claimed, payload) = setup_claimed_projection_update();
    block_on(async {
        backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: EventId(1),
                    append_events: Vec::new(),
                    upsert_waits: Vec::new(),
                    schedule_activities: Vec::new(),
                    schedule_activity_maps: Vec::new(),
                    schedule_child_workflow_maps: Vec::new(),
                    start_child_workflows: Vec::new(),
                    consume_signals: Vec::new(),
                    delete_waits: Vec::new(),
                    cancel_commands: Vec::new(),
                    query_projection: Some(payload),
                },
            )
            .await
            .unwrap();
        (
            backend,
            durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("bench/claim"),
            },
        )
    })
}

fn setup_claimable_workflow() -> (MemoryBackend, WorkerId, ClaimWorkflowTaskOptions) {
    block_on(create_claimable_workflow())
}

async fn create_claimable_workflow() -> (MemoryBackend, WorkerId, ClaimWorkflowTaskOptions) {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    client
        .start_workflow::<double_plus_one>("bench/claim", "workflows", bench_input(10))
        .await
        .unwrap();
    (
        backend,
        WorkerId::new("bench-claim-worker"),
        claim_workflow_options(),
    )
}

fn setup_claimable_workflow_sqlite() -> (
    tempfile::TempDir,
    SqliteBackend,
    WorkerId,
    ClaimWorkflowTaskOptions,
) {
    block_on(create_claimable_workflow_sqlite())
}

async fn create_claimable_workflow_sqlite() -> (
    tempfile::TempDir,
    SqliteBackend,
    WorkerId,
    ClaimWorkflowTaskOptions,
) {
    let (dir, backend) = sqlite_backend();
    let client = Client::new(backend.clone());
    client
        .start_workflow::<double_plus_one>("bench/claim", "workflows", bench_input(10))
        .await
        .unwrap();
    (
        dir,
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

struct SqliteAppendCommitBenchState {
    _dir: tempfile::TempDir,
    backend: SqliteBackend,
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
            retry_policy: durust::RetryPolicy::none(),
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            fingerprint: durust::activity_fingerprint(
                ActivityName::new("bench.double"),
                durust::payload_digest(&input),
                "sha256:bench-options".to_owned(),
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
                upsert_waits: Vec::new(),
                schedule_activities: vec![activity_task],
                schedule_activity_maps: Vec::new(),
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        }
    })
}

fn setup_claimed_workflow_for_commit_sqlite() -> SqliteAppendCommitBenchState {
    block_on(async {
        let (dir, backend, worker_id, opts) = create_claimable_workflow_sqlite().await;
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
            retry_policy: durust::RetryPolicy::none(),
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            fingerprint: durust::activity_fingerprint(
                ActivityName::new("bench.double"),
                durust::payload_digest(&input),
                "sha256:bench-options".to_owned(),
            ),
            input,
        };
        let activity_task = ActivityTask::from_scheduled(&scheduled);
        SqliteAppendCommitBenchState {
            _dir: dir,
            backend,
            claimed,
            batch: WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                    scheduled,
                ))],
                upsert_waits: Vec::new(),
                schedule_activities: vec![activity_task],
                schedule_activity_maps: Vec::new(),
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        }
    })
}

fn setup_scheduled_activity() -> (MemoryBackend, WorkerId, ClaimActivityOptions) {
    let state = setup_claimed_workflow_for_commit();
    block_on(async {
        state
            .backend
            .commit_workflow_task(state.claimed.claim, state.batch)
            .await
            .unwrap();
        (
            state.backend,
            WorkerId::new("bench-activity-worker"),
            claim_activity_options("activities"),
        )
    })
}

fn setup_scheduled_activity_sqlite() -> (
    tempfile::TempDir,
    SqliteBackend,
    WorkerId,
    ClaimActivityOptions,
) {
    let state = setup_claimed_workflow_for_commit_sqlite();
    block_on(async {
        state
            .backend
            .commit_workflow_task(state.claimed.claim, state.batch)
            .await
            .unwrap();
        (
            state._dir,
            state.backend,
            WorkerId::new("bench-activity-worker"),
            claim_activity_options("activities"),
        )
    })
}

fn setup_claimed_heartbeat_activity() -> (MemoryBackend, durust::ActivityTaskClaim) {
    block_on(async {
        let (backend, worker_id, opts) = create_claimable_workflow().await;
        let claimed = backend
            .claim_workflow_task(worker_id, opts)
            .await
            .unwrap()
            .expect("workflow task");
        let input = durust::encode_payload(&BenchInput { value: 10 }).unwrap();
        let scheduled = ActivityScheduled {
            command_id: durust::command_id(&claimed.run_id, 1),
            activity_name: ActivityName::new("bench.double"),
            task_queue: TaskQueue::new("activities"),
            retry_policy: durust::RetryPolicy::none(),
            start_to_close_timeout: None,
            heartbeat_timeout: Some(Duration::from_secs(30)),
            fingerprint: durust::activity_fingerprint(
                ActivityName::new("bench.double"),
                durust::payload_digest(&input),
                "sha256:bench-heartbeat-options".to_owned(),
            ),
            input,
        };
        backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: EventId(1),
                    append_events: vec![NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                        scheduled.clone(),
                    ))],
                    upsert_waits: Vec::new(),
                    schedule_activities: vec![ActivityTask::from_scheduled(&scheduled)],
                    schedule_activity_maps: Vec::new(),
                    schedule_child_workflow_maps: Vec::new(),
                    start_child_workflows: Vec::new(),
                    consume_signals: Vec::new(),
                    delete_waits: Vec::new(),
                    cancel_commands: Vec::new(),
                    query_projection: None,
                },
            )
            .await
            .unwrap();
        let activity = backend
            .claim_activity_task(
                WorkerId::new("bench-heartbeat-worker"),
                claim_activity_options("activities"),
            )
            .await
            .unwrap()
            .expect("heartbeat activity");
        (backend, activity.claim)
    })
}

fn setup_due_timer() -> MemoryBackend {
    block_on(async {
        let (backend, worker_id, opts) = create_claimable_workflow().await;
        let claimed = backend
            .claim_workflow_task(worker_id, opts)
            .await
            .unwrap()
            .expect("workflow task");
        let command_id = durust::command_id(&claimed.run_id, 1);
        backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: EventId(1),
                    append_events: vec![NewHistoryEvent::new(HistoryEventData::TimerStarted(
                        durust::TimerStarted {
                            command_id: command_id.clone(),
                            fire_at: TimestampMs(10),
                            fingerprint: durust::timer_fingerprint("sleep", TimestampMs(10)),
                        },
                    ))],
                    upsert_waits: vec![WaitRecord {
                        wait_id: durust::WaitId::new(format!(
                            "{}:{}:timer",
                            command_id.run_id, command_id.seq.0
                        )),
                        run_id: command_id.run_id.clone(),
                        command_id,
                        kind: WaitKind::Timer,
                        key: "timer".to_owned(),
                        ready_at: Some(TimestampMs(10)),
                    }],
                    schedule_activities: Vec::new(),
                    schedule_activity_maps: Vec::new(),
                    schedule_child_workflow_maps: Vec::new(),
                    start_child_workflows: Vec::new(),
                    consume_signals: Vec::new(),
                    delete_waits: Vec::new(),
                    cancel_commands: Vec::new(),
                    query_projection: None,
                },
            )
            .await
            .unwrap();
        backend
    })
}

fn setup_signal_wait() -> (MemoryBackend, durust::RunId) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("bench/signal", "workflows", bench_input(10))
            .await
            .unwrap();
        let claimed = backend
            .claim_workflow_task(
                WorkerId::new("bench-signal-worker"),
                claim_workflow_options(),
            )
            .await
            .unwrap()
            .expect("workflow task");
        let command_id = durust::command_id(&claimed.run_id, 1);
        backend
            .commit_workflow_task(
                claimed.claim.clone(),
                WorkflowTaskCommit {
                    expected_tail_event_id: EventId(1),
                    append_events: Vec::new(),
                    upsert_waits: vec![WaitRecord {
                        wait_id: durust::WaitId::new(format!(
                            "{}:{}:signal",
                            command_id.run_id, command_id.seq.0
                        )),
                        run_id: command_id.run_id.clone(),
                        command_id,
                        kind: WaitKind::Signal,
                        key: "ready".to_owned(),
                        ready_at: None,
                    }],
                    schedule_activities: Vec::new(),
                    schedule_activity_maps: Vec::new(),
                    schedule_child_workflow_maps: Vec::new(),
                    start_child_workflows: Vec::new(),
                    consume_signals: Vec::new(),
                    delete_waits: Vec::new(),
                    cancel_commands: Vec::new(),
                    query_projection: None,
                },
            )
            .await
            .unwrap();
        (backend, run_id)
    })
}

fn setup_claimed_activity_map_workflow() -> (
    MemoryBackend,
    ClaimedWorkflowTask,
    ActivityMapTask,
    durust::ActivityMapScheduled,
) {
    block_on(async {
        let (backend, worker_id, opts) = create_claimable_workflow().await;
        let claimed = backend
            .claim_workflow_task(worker_id, opts)
            .await
            .unwrap()
            .expect("workflow task");
        let command_id = durust::command_id(&claimed.run_id, 1);
        let input_manifest = activity_map_input_manifest(128);
        let activity_name = ActivityName::new("bench.double");
        let task_queue = TaskQueue::new("activities");
        let retry_policy = durust::RetryPolicy::none();
        let scheduled = durust::ActivityMapScheduled {
            command_id: command_id.clone(),
            activity_name: activity_name.clone(),
            task_queue: task_queue.clone(),
            retry_policy: retry_policy.clone(),
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            input_manifest: input_manifest.clone(),
            result_manifest_name: "bench-results".to_owned(),
            max_in_flight: 64,
            fingerprint: durust::activity_map_fingerprint(
                activity_name.clone(),
                durust::payload_digest(&input_manifest),
                "bench-results".to_owned(),
                64,
                "sha256:bench-options".to_owned(),
            ),
        };
        let map_task = ActivityMapTask {
            map_command_id: command_id,
            activity_name,
            task_queue,
            retry_policy,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            input_manifest,
            result_manifest_name: "bench-results".to_owned(),
            max_in_flight: 64,
        };
        (backend, claimed, map_task, scheduled)
    })
}

fn setup_materialized_activity_map() -> (MemoryBackend, WorkerId, ClaimActivityOptions) {
    let (backend, claimed, map_task, scheduled) = setup_claimed_activity_map_workflow();
    block_on(async {
        backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: EventId(1),
                    append_events: vec![NewHistoryEvent::new(
                        HistoryEventData::ActivityMapScheduled(scheduled),
                    )],
                    upsert_waits: Vec::new(),
                    schedule_activities: Vec::new(),
                    schedule_activity_maps: vec![map_task],
                    schedule_child_workflow_maps: Vec::new(),
                    start_child_workflows: Vec::new(),
                    consume_signals: Vec::new(),
                    delete_waits: Vec::new(),
                    cancel_commands: Vec::new(),
                    query_projection: None,
                },
            )
            .await
            .unwrap();
        (
            backend,
            WorkerId::new("bench-map-worker"),
            claim_activity_options("activities"),
        )
    })
}

fn setup_claimed_child_workflow_map_workflow() -> (
    MemoryBackend,
    ClaimedWorkflowTask,
    durust::ChildWorkflowMapTask,
    durust::ChildWorkflowMapScheduled,
) {
    block_on(async {
        let (backend, worker_id, opts) = create_claimable_workflow().await;
        let claimed = backend
            .claim_workflow_task(worker_id, opts)
            .await
            .unwrap()
            .expect("workflow task");
        let command_id = durust::command_id(&claimed.run_id, 1);
        let input_manifest = child_workflow_map_input_manifest(128);
        let workflow_type = WorkflowType::new("bench.child-double", 1);
        let task_queue = TaskQueue::new("workflows");
        let workflow_id_prefix = format!("bench/child-map/{}", claimed.run_id.0);
        let parent_close_policy = durust::ParentClosePolicy::Cancel;
        let failure_mode = durust::ChildWorkflowMapFailureMode::FailFast;
        let scheduled = durust::ChildWorkflowMapScheduled {
            command_id: command_id.clone(),
            workflow_type: workflow_type.clone(),
            task_queue: task_queue.clone(),
            input_manifest: input_manifest.clone(),
            result_manifest_name: "bench-results".to_owned(),
            workflow_id_prefix: workflow_id_prefix.clone(),
            max_in_flight: 64,
            parent_close_policy,
            failure_mode,
            fingerprint: durust::child_workflow_map_fingerprint(
                workflow_type.clone(),
                durust::payload_digest(&input_manifest),
                "bench-results".to_owned(),
                workflow_id_prefix.clone(),
                64,
                task_queue.clone(),
                parent_close_policy,
                failure_mode,
            ),
        };
        let map_task = durust::ChildWorkflowMapTask {
            map_command_id: command_id,
            workflow_type,
            task_queue,
            input_manifest,
            result_manifest_name: "bench-results".to_owned(),
            workflow_id_prefix,
            max_in_flight: 64,
            parent_close_policy,
            failure_mode,
        };
        (backend, claimed, map_task, scheduled)
    })
}

fn setup_materialized_child_workflow_map() -> (MemoryBackend, ClaimedWorkflowTask) {
    let (backend, claimed, map_task, scheduled) = setup_claimed_child_workflow_map_workflow();
    block_on(async {
        backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: EventId(1),
                    append_events: vec![NewHistoryEvent::new(
                        HistoryEventData::ChildWorkflowMapScheduled(scheduled),
                    )],
                    upsert_waits: Vec::new(),
                    schedule_activities: Vec::new(),
                    schedule_activity_maps: Vec::new(),
                    schedule_child_workflow_maps: vec![map_task],
                    start_child_workflows: Vec::new(),
                    consume_signals: Vec::new(),
                    delete_waits: Vec::new(),
                    cancel_commands: Vec::new(),
                    query_projection: None,
                },
            )
            .await
            .unwrap();
        let dispatched = backend
            .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
                namespace: Namespace::default(),
                limit: 64,
            })
            .await
            .unwrap();
        assert_eq!(dispatched.dispatched, 64);
        let child_claim = backend
            .claim_workflow_task(
                WorkerId::new("bench-child-map-worker"),
                child_workflow_claim_options(),
            )
            .await
            .unwrap()
            .expect("child workflow task");
        (backend, child_claim)
    })
}

fn activity_map_input_manifest(items: u64) -> durust::PayloadRef {
    let inputs = (0..items)
        .map(|value| durust::encode_payload(&BenchInput { value }).unwrap())
        .collect::<Vec<_>>();
    durust::encode_activity_map_input_manifest(inputs, 32).unwrap()
}

fn child_workflow_map_input_manifest(items: u64) -> durust::PayloadRef {
    let inputs = (0..items)
        .map(|value| durust::encode_payload(&BenchInput { value }).unwrap())
        .collect::<Vec<_>>();
    durust::encode_activity_map_input_manifest(inputs, 32).unwrap()
}

fn large_payload() -> LargePayload {
    LargePayload {
        body: "x".repeat(64 * 1024),
    }
}

fn mixed_large_payload() -> LargePayload {
    let mut state = 0x1234_5678_u32;
    let mut body = String::with_capacity(64 * 1024);
    for _ in 0..64 * 1024 {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        body.push(char::from(b' ' + (state % 95) as u8));
    }
    LargePayload { body }
}

fn encoded_payload_bytes(payload: &LargePayload) -> Vec<u8> {
    durust::encode_payload(payload)
        .unwrap()
        .inline_bytes()
        .unwrap()
        .to_vec()
}

fn garage_config_from_env() -> Option<durust::S3BlobStoreConfig> {
    let endpoint = env::var("DURUST_GARAGE_ENDPOINT").ok()?;
    let bucket = env::var("DURUST_GARAGE_BUCKET").ok()?;
    let access_key_id = env::var("DURUST_GARAGE_ACCESS_KEY_ID").ok()?;
    let secret_access_key = env::var("DURUST_GARAGE_SECRET_ACCESS_KEY").ok()?;
    let region = env::var("DURUST_GARAGE_REGION").unwrap_or_else(|_| "garage".to_owned());
    let prefix = env::var("DURUST_GARAGE_PREFIX").unwrap_or_else(|_| "bench/payloads".to_owned());
    Some(durust::S3BlobStoreConfig {
        bucket,
        endpoint,
        region,
        prefix,
        access_key_id,
        secret_access_key,
    })
}

fn setup_payload_history(inline_threshold_bytes: usize) -> (MemoryBackend, durust::RunId) {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            PayloadStorageConfig::new().inline_threshold_bytes(inline_threshold_bytes),
        );
        let outcome = backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("bench/payload-history"),
                workflow_type: WorkflowType::new("bench.double-plus-one", 1),
                task_queue: TaskQueue::new("workflows"),
                input: durust::encode_payload(&large_payload()).unwrap(),
            })
            .await
            .unwrap();
        (backend, outcome.run_id().clone())
    })
}

fn setup_large_payload_timer_replay(
    inline_threshold_bytes: usize,
) -> (MemoryBackend, durust::RunId) {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            PayloadStorageConfig::new().inline_threshold_bytes(inline_threshold_bytes),
        );
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<large_payload_then_timer>(
                "bench/large-payload-replay",
                "workflows",
                large_payload(),
            )
            .await
            .unwrap();
        let mut worker = large_payload_worker(backend.clone());
        assert!(worker.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::ZERO);
        assert_eq!(worker.run_timers_once().await.unwrap(), 1);
        (backend, run_id)
    })
}

async fn assert_large_payload_workflow_completed(backend: &MemoryBackend, run_id: &durust::RunId) {
    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1_000),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowCompleted { result } =
        &history.last().expect("workflow terminal").data
    else {
        panic!("large payload replay workflow did not complete");
    };
    assert_eq!(durust::decode_payload::<usize>(result).unwrap(), 64 * 1024);
}

fn claim_workflow_options() -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("workflows"),
        registered_workflow_types: vec![WorkflowType::new("bench.double-plus-one", 1)],
        lease_duration: Duration::from_secs(30),
    }
}

fn child_workflow_claim_options() -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("workflows"),
        registered_workflow_types: vec![WorkflowType::new("bench.child-double", 1)],
        lease_duration: Duration::from_secs(30),
    }
}

fn claim_activity_options(task_queue: &str) -> ClaimActivityOptions {
    ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(task_queue),
        registered_activity_names: vec![ActivityName::new("bench.double")],
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

fn sqlite_backend() -> (tempfile::TempDir, SqliteBackend) {
    let dir = tempfile::tempdir().unwrap();
    let backend = SqliteBackend::open(dir.path().join("bench.sqlite3")).unwrap();
    (dir, backend)
}

fn sqlite_worker(backend: SqliteBackend) -> Worker<SqliteBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(double_plus_one)
        .register_activity(double)
        .build()
}

fn large_payload_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .register_workflow(large_payload_then_timer)
        .build()
}

fn large_payload_replay_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .history_chunk_events(1)
        .register_workflow(large_payload_then_timer)
        .build()
}

fn setup_sqlite_single_file_mixed_workflows() -> (tempfile::TempDir, SqliteBackend) {
    block_on(async {
        let (dir, backend) = sqlite_backend();
        start_sqlite_mixed_workflows(&backend, SQLITE_SINGLE_FILE_WORKFLOWS).await;
        (dir, backend)
    })
}

async fn start_sqlite_mixed_workflows(backend: &SqliteBackend, workflows: usize) {
    let client = Client::new(backend.clone());
    for index in 0..workflows {
        let input = index as u64;
        match index % 5 {
            0 => {
                client
                    .start_workflow::<double_plus_one>(
                        format!("bench/sqlite-mixed/double/{index}"),
                        "workflows",
                        bench_input(input),
                    )
                    .await
                    .unwrap();
            }
            1 => {
                client
                    .start_workflow::<join_all_activities>(
                        format!("bench/sqlite-mixed/join-all/{index}"),
                        "workflows",
                        bench_input(input),
                    )
                    .await
                    .unwrap();
            }
            2 => {
                client
                    .start_workflow::<select_all_activities>(
                        format!("bench/sqlite-mixed/select-all/{index}"),
                        "workflows",
                        bench_input(input),
                    )
                    .await
                    .unwrap();
            }
            3 => {
                client
                    .start_workflow::<child_start>(
                        format!("bench/sqlite-mixed/child-start/{index}"),
                        "workflows",
                        bench_input(input),
                    )
                    .await
                    .unwrap();
            }
            _ => {
                client
                    .start_workflow::<select_all_mixed>(
                        format!("bench/sqlite-mixed/mixed/{index}"),
                        "workflows",
                        bench_input(input),
                    )
                    .await
                    .unwrap();
            }
        };
    }
}

fn drain_sqlite_workers_concurrently(
    backend: SqliteBackend,
    worker_count: usize,
) -> WorkerRunStats {
    let handles = (0..worker_count)
        .map(|worker_index| {
            let backend = backend.clone();
            thread::spawn(move || {
                let mut worker = sqlite_mixed_worker(backend, worker_index);
                block_on(async {
                    worker
                        .run_until_idle_with(WorkerRunOptions {
                            max_iterations: SQLITE_DRAIN_MAX_ITERATIONS,
                        })
                        .await
                        .unwrap()
                })
            })
        })
        .collect::<Vec<_>>();

    handles
        .into_iter()
        .map(|handle| handle.join().expect("SQLite benchmark worker panicked"))
        .fold(WorkerRunStats::default(), add_worker_stats)
}

fn sqlite_mixed_worker(backend: SqliteBackend, worker_index: usize) -> Worker<SqliteBackend> {
    Worker::builder(backend)
        .worker_id(format!("sqlite-single-file-worker-{worker_index}"))
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(double_plus_one)
        .register_workflow(join_all_activities)
        .register_workflow(select_all_activities)
        .register_workflow(child_start)
        .register_workflow(child_double)
        .register_workflow(select_all_mixed)
        .register_activity(double)
        .build()
}

struct PostgresBenchFixture {
    runtime: tokio::runtime::Runtime,
    database_url: String,
    schema: String,
    backend: PostgresBackend,
}

fn postgres_benchmark_url() -> Option<String> {
    env::var("DURUST_POSTGRES_URL").ok()
}

fn postgres_bench_schema(prefix: &str) -> String {
    let counter = POSTGRES_BENCH_SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("durust_bench_{prefix}_{}_{}", std::process::id(), counter)
}

fn postgres_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn measure_postgres_bench<T, Setup, Measure>(
    database_url: String,
    prefix: &str,
    iters: u64,
    mut setup: Setup,
    measure: Measure,
) -> Duration
where
    Setup: FnMut(&PostgresBenchFixture, u64) -> T,
    Measure: FnOnce(&PostgresBenchFixture, Vec<T>),
{
    let fixture = setup_postgres_backend(database_url, prefix);
    let values = (0..iters)
        .map(|iteration| setup(&fixture, iteration))
        .collect::<Vec<_>>();
    let start = Instant::now();
    measure(&fixture, values);
    let elapsed = start.elapsed();
    finish_postgres_bench(fixture);
    elapsed
}

fn setup_postgres_backend(database_url: String, prefix: &str) -> PostgresBenchFixture {
    let runtime = postgres_runtime();
    let schema = postgres_bench_schema(prefix);
    let backend = runtime
        .block_on(PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(database_url.clone())
                .schema(schema.clone())
                .max_pool_size(8),
        ))
        .unwrap();
    PostgresBenchFixture {
        runtime,
        database_url,
        schema,
        backend,
    }
}

fn finish_postgres_bench(fixture: PostgresBenchFixture) {
    let PostgresBenchFixture {
        runtime,
        database_url,
        schema,
        backend,
    } = fixture;
    drop(backend);
    runtime.block_on(drop_postgres_schema(&database_url, &schema));
}

async fn drop_postgres_schema(database_url: &str, schema: &str) {
    let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "drop schema if exists {} cascade",
            quote_postgres_identifier(schema)
        ))
        .await
        .unwrap();
    connection.abort();
}

fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn postgres_workflow_id(prefix: &str, schema: &str, iteration: u64) -> durust::WorkflowId {
    durust::WorkflowId::new(format!("bench/postgres/{prefix}/{schema}/{iteration}"))
}

fn start_postgres_workflow(
    fixture: &PostgresBenchFixture,
    prefix: &str,
    iteration: u64,
) -> (durust::WorkflowId, durust::RunId) {
    let workflow_id = postgres_workflow_id(prefix, &fixture.schema, iteration);
    let outcome = fixture
        .runtime
        .block_on(
            fixture
                .backend
                .start_workflow(durust::StartWorkflowRequest {
                    namespace: Namespace::default(),
                    workflow_id: workflow_id.clone(),
                    workflow_type: WorkflowType::new("bench.double-plus-one", 1),
                    task_queue: TaskQueue::new("workflows"),
                    input: durust::encode_payload(&10_u64).unwrap(),
                }),
        )
        .unwrap();
    (workflow_id, outcome.run_id().clone())
}

fn claim_postgres_workflow_task(
    fixture: &PostgresBenchFixture,
    worker_id: impl Into<String>,
) -> ClaimedWorkflowTask {
    fixture
        .runtime
        .block_on(
            fixture
                .backend
                .claim_workflow_task(WorkerId::new(worker_id), claim_workflow_options()),
        )
        .unwrap()
        .expect("workflow task")
}

fn setup_postgres_claimed_workflow_for_commit(
    fixture: &PostgresBenchFixture,
    iteration: u64,
) -> (ClaimedWorkflowTask, WorkflowTaskCommit) {
    start_postgres_workflow(fixture, "commit", iteration);
    let claimed = claim_postgres_workflow_task(fixture, "bench-postgres-commit-worker");
    let input = durust::encode_payload(&BenchInput { value: 10 }).unwrap();
    let scheduled = ActivityScheduled {
        command_id: durust::command_id(&claimed.run_id, 1),
        activity_name: ActivityName::new("bench.double"),
        task_queue: TaskQueue::new("activities"),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("bench.double"),
            durust::payload_digest(&input),
            "sha256:bench-options".to_owned(),
        ),
        input,
    };
    let activity_task = ActivityTask::from_scheduled(&scheduled);
    (
        claimed,
        WorkflowTaskCommit {
            expected_tail_event_id: EventId(1),
            append_events: vec![NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                scheduled,
            ))],
            upsert_waits: Vec::new(),
            schedule_activities: vec![activity_task],
            schedule_activity_maps: Vec::new(),
            schedule_child_workflow_maps: Vec::new(),
            start_child_workflows: Vec::new(),
            consume_signals: Vec::new(),
            delete_waits: Vec::new(),
            cancel_commands: Vec::new(),
            query_projection: None,
        },
    )
}

fn setup_postgres_scheduled_activity(fixture: &PostgresBenchFixture, iteration: u64) {
    let (claimed, batch) = setup_postgres_claimed_workflow_for_commit(fixture, iteration);
    fixture
        .runtime
        .block_on(fixture.backend.commit_workflow_task(claimed.claim, batch))
        .unwrap();
}

fn setup_postgres_claimed_heartbeat_activity(
    fixture: &PostgresBenchFixture,
    iteration: u64,
) -> durust::ActivityTaskClaim {
    start_postgres_workflow(fixture, "heartbeat", iteration);
    let claimed = claim_postgres_workflow_task(fixture, "bench-postgres-heartbeat-workflow-worker");
    let input = durust::encode_payload(&BenchInput { value: 10 }).unwrap();
    let scheduled = ActivityScheduled {
        command_id: durust::command_id(&claimed.run_id, 1),
        activity_name: ActivityName::new("bench.double"),
        task_queue: TaskQueue::new("activities"),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: Some(Duration::from_secs(30)),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("bench.double"),
            durust::payload_digest(&input),
            "sha256:bench-heartbeat-options".to_owned(),
        ),
        input,
    };
    fixture
        .runtime
        .block_on(fixture.backend.commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                    scheduled.clone(),
                ))],
                upsert_waits: Vec::new(),
                schedule_activities: vec![ActivityTask::from_scheduled(&scheduled)],
                schedule_activity_maps: Vec::new(),
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        ))
        .unwrap();
    let activity = fixture
        .runtime
        .block_on(fixture.backend.claim_activity_task(
            WorkerId::new(format!("bench-postgres-heartbeat-worker-{iteration}")),
            claim_activity_options("activities"),
        ))
        .unwrap()
        .expect("heartbeat activity");
    activity.claim
}

fn setup_postgres_due_timer(fixture: &PostgresBenchFixture, iteration: u64) {
    start_postgres_workflow(fixture, "timer", iteration);
    let claimed = claim_postgres_workflow_task(fixture, "bench-postgres-timer-worker");
    let command_id = durust::command_id(&claimed.run_id, 1);
    fixture
        .runtime
        .block_on(fixture.backend.commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![NewHistoryEvent::new(HistoryEventData::TimerStarted(
                    durust::TimerStarted {
                        command_id: command_id.clone(),
                        fire_at: TimestampMs(10),
                        fingerprint: durust::timer_fingerprint("sleep", TimestampMs(10)),
                    },
                ))],
                upsert_waits: vec![WaitRecord {
                    wait_id: durust::WaitId::new(format!(
                        "{}:{}:timer",
                        command_id.run_id, command_id.seq.0
                    )),
                    run_id: command_id.run_id.clone(),
                    command_id,
                    kind: WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(TimestampMs(10)),
                }],
                schedule_activities: Vec::new(),
                schedule_activity_maps: Vec::new(),
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        ))
        .unwrap();
}

fn setup_postgres_signal_wait(
    fixture: &PostgresBenchFixture,
    iteration: u64,
) -> (durust::RunId, durust::WorkflowId, durust::SignalId) {
    let (workflow_id, _) = start_postgres_workflow(fixture, "signal", iteration);
    let claimed = claim_postgres_workflow_task(fixture, "bench-postgres-signal-worker");
    let command_id = durust::command_id(&claimed.run_id, 1);
    fixture
        .runtime
        .block_on(fixture.backend.commit_workflow_task(
            claimed.claim.clone(),
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: Vec::new(),
                upsert_waits: vec![WaitRecord {
                    wait_id: durust::WaitId::new(format!(
                        "{}:{}:signal",
                        command_id.run_id, command_id.seq.0
                    )),
                    run_id: command_id.run_id.clone(),
                    command_id,
                    kind: WaitKind::Signal,
                    key: "ready".to_owned(),
                    ready_at: None,
                }],
                schedule_activities: Vec::new(),
                schedule_activity_maps: Vec::new(),
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        ))
        .unwrap();
    (
        claimed.run_id,
        workflow_id,
        durust::SignalId::new(format!("bench/signal/{}/{}", fixture.schema, iteration)),
    )
}

fn setup_postgres_claimed_projection_update(
    fixture: &PostgresBenchFixture,
    iteration: u64,
) -> (ClaimedWorkflowTask, durust::PayloadRef) {
    start_postgres_workflow(fixture, "projection_update", iteration);
    let claimed = claim_postgres_workflow_task(fixture, "bench-postgres-projection-update-worker");
    (
        claimed,
        durust::encode_payload(&BenchInput { value: 10 }).unwrap(),
    )
}

fn setup_postgres_projection_read(
    fixture: &PostgresBenchFixture,
    iteration: u64,
) -> durust::QueryProjectionRequest {
    let (workflow_id, _) = start_postgres_workflow(fixture, "projection_read", iteration);
    let claimed = claim_postgres_workflow_task(fixture, "bench-postgres-projection-read-worker");
    let payload = durust::encode_payload(&BenchInput { value: 10 }).unwrap();
    fixture
        .runtime
        .block_on(fixture.backend.commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: Vec::new(),
                upsert_waits: Vec::new(),
                schedule_activities: Vec::new(),
                schedule_activity_maps: Vec::new(),
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: Some(payload),
            },
        ))
        .unwrap();
    durust::QueryProjectionRequest {
        namespace: Namespace::default(),
        workflow_id,
    }
}

fn setup_postgres_history_stream(fixture: &PostgresBenchFixture, iteration: u64) -> durust::RunId {
    start_postgres_workflow(fixture, "history", iteration);
    let claimed = claim_postgres_workflow_task(fixture, "bench-postgres-history-worker");
    let events = (0..128)
        .map(|_| NewHistoryEvent::new(HistoryEventData::WorkflowTaskStarted))
        .collect::<Vec<_>>();
    fixture
        .runtime
        .block_on(fixture.backend.commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: events,
                upsert_waits: Vec::new(),
                schedule_activities: Vec::new(),
                schedule_activity_maps: Vec::new(),
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        ))
        .unwrap();
    claimed.run_id
}

const POSTGRES_CHUNKED_HISTORY_EVENTS: u64 = 1024;

fn setup_postgres_large_history_stream(
    fixture: &PostgresBenchFixture,
    iteration: u64,
) -> durust::RunId {
    start_postgres_workflow(fixture, "history_chunked", iteration);
    let claimed = claim_postgres_workflow_task(fixture, "bench-postgres-history-chunked-worker");
    let events = (0..POSTGRES_CHUNKED_HISTORY_EVENTS)
        .map(|_| NewHistoryEvent::new(HistoryEventData::WorkflowTaskStarted))
        .collect::<Vec<_>>();
    fixture
        .runtime
        .block_on(fixture.backend.commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: events,
                upsert_waits: Vec::new(),
                schedule_activities: Vec::new(),
                schedule_activity_maps: Vec::new(),
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        ))
        .unwrap();
    claimed.run_id
}

fn setup_postgres_child_start(
    fixture: &PostgresBenchFixture,
    iteration: u64,
) -> (ClaimedWorkflowTask, WorkflowTaskCommit) {
    start_postgres_workflow(fixture, "child_parent", iteration);
    let claimed = claim_postgres_workflow_task(fixture, "bench-postgres-child-worker");
    let command_id = durust::command_id(&claimed.run_id, 1);
    let input = durust::encode_payload(&10_u64).unwrap();
    let workflow_type = WorkflowType::new("bench.child-double", 1);
    let workflow_id = postgres_workflow_id("child_target", &fixture.schema, iteration);
    let task_queue = TaskQueue::new("workflows");
    let requested = durust::ChildWorkflowStartRequested {
        command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        workflow_id: workflow_id.clone(),
        task_queue: task_queue.clone(),
        input: input.clone(),
        parent_close_policy: durust::ParentClosePolicy::Cancel,
        fingerprint: durust::child_workflow_fingerprint(
            workflow_type,
            workflow_id,
            durust::payload_digest(&input),
            task_queue,
            durust::ParentClosePolicy::Cancel,
        ),
    };
    (
        claimed,
        WorkflowTaskCommit {
            expected_tail_event_id: EventId(1),
            append_events: vec![NewHistoryEvent::new(
                HistoryEventData::ChildWorkflowStartRequested(requested.clone()),
            )],
            upsert_waits: Vec::new(),
            schedule_activities: Vec::new(),
            schedule_activity_maps: Vec::new(),
            schedule_child_workflow_maps: Vec::new(),
            start_child_workflows: vec![durust::ChildStartOutboxMessage::from_requested(
                &requested,
            )],
            consume_signals: Vec::new(),
            delete_waits: Vec::new(),
            cancel_commands: Vec::new(),
            query_projection: None,
        },
    )
}

fn setup_postgres_claimed_activity_map_workflow(
    fixture: &PostgresBenchFixture,
    iteration: u64,
) -> (
    ClaimedWorkflowTask,
    ActivityMapTask,
    durust::ActivityMapScheduled,
) {
    start_postgres_workflow(fixture, "activity_map", iteration);
    let claimed = claim_postgres_workflow_task(fixture, "bench-postgres-map-workflow-worker");
    let command_id = durust::command_id(&claimed.run_id, 1);
    let input_manifest = activity_map_input_manifest(8);
    let activity_name = ActivityName::new("bench.double");
    let task_queue = TaskQueue::new("activities");
    let retry_policy = durust::RetryPolicy::none();
    let scheduled = durust::ActivityMapScheduled {
        command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "bench-results".to_owned(),
        max_in_flight: 8,
        fingerprint: durust::activity_map_fingerprint(
            activity_name.clone(),
            durust::payload_digest(&input_manifest),
            "bench-results".to_owned(),
            8,
            "sha256:bench-options".to_owned(),
        ),
    };
    let map_task = ActivityMapTask {
        map_command_id: command_id,
        activity_name,
        task_queue,
        retry_policy,
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest,
        result_manifest_name: "bench-results".to_owned(),
        max_in_flight: 8,
    };
    (claimed, map_task, scheduled)
}

fn add_worker_stats(mut left: WorkerRunStats, right: WorkerRunStats) -> WorkerRunStats {
    left.workflow_tasks += right.workflow_tasks;
    left.activity_tasks += right.activity_tasks;
    left.timers_fired += right.timers_fired;
    left.activities_timed_out += right.activities_timed_out;
    left.child_workflow_starts_dispatched += right.child_workflow_starts_dispatched;
    left
}

fn assert_mixed_sqlite_stats(stats: WorkerRunStats) {
    assert!(
        stats.workflow_tasks >= SQLITE_SINGLE_FILE_WORKFLOWS,
        "expected at least one workflow task per started workflow, got {stats:?}"
    );
    assert!(
        stats.activity_tasks >= SQLITE_SINGLE_FILE_WORKFLOWS,
        "expected activity work from double and join_all workflows, got {stats:?}"
    );
    assert!(
        stats.child_workflow_starts_dispatched >= SQLITE_SINGLE_FILE_WORKFLOWS / 5,
        "expected child dispatch work from child workflows, got {stats:?}"
    );
    assert!(
        stats.timers_fired > 0,
        "expected timer work from mixed select workflows, got {stats:?}"
    );
}

fn select_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .register_workflow(select_signal_timer)
        .build()
}

fn select_replay_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .history_chunk_events(1)
        .register_workflow(select_then_wait)
        .build()
}

fn join_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(join_four_activities)
        .register_activity(double)
        .build()
}

fn join_all_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(join_all_activities)
        .register_activity(double)
        .build()
}

fn select_all_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .register_workflow(select_all_activities)
        .build()
}

fn version_replay_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .history_chunk_events(1)
        .register_workflow(version_branch)
        .register_activity(double)
        .build()
}

criterion_group!(
    benches,
    workflow_task_schedule,
    workflow_task_claim,
    workflow_task_append_commit,
    cached_wake_poll,
    crash_replay,
    held_handle_wake,
    child_fanout_completion,
    recovery_flow_control,
    select_registration,
    select_replay,
    bounded_join_fanout,
    join_all_activity_fanout,
    select_all_activity_race,
    child_start_dispatch,
    projection_update,
    projection_read,
    version_marker_replay,
    activity_claim_complete,
    sqlite_single_file_mixed_throughput,
    postgres_provider_hot_paths,
    activity_heartbeat,
    payload_codec,
    payload_compression,
    payload_garage_object_store,
    payload_provider_refs,
    payload_replay,
    timer_due_scan_wakeup,
    signal_send_consume,
    activity_map_materialize,
    activity_map_item_complete,
    child_workflow_map_materialize,
    child_workflow_map_item_complete
);
criterion_main!(benches);
