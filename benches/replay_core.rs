use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use durust::{
    ActivityMapTask, ActivityName, ActivityScheduled, ActivityTask, ClaimActivityOptions,
    ClaimWorkflowTaskOptions, ClaimedWorkflowTask, Client, CommitOutcome, CompleteActivityRequest,
    DurableBackend, EventId, FireDueTimersRequest, HistoryEventData, MemoryBackend, Namespace,
    NewHistoryEvent, SignalWorkflowRequest, TaskQueue, TimestampMs, WaitKind, WaitRecord, Worker,
    WorkerId, WorkflowTaskCommit, WorkflowType,
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

#[durust::workflow(name = "bench.join-four-activities", version = 1)]
async fn join_four_activities(input: u64) -> durust::Result<u64> {
    let (first, second, third, fourth) = durust::join!(
        durust::call_activity!(double(BenchInput { value: input })).task_queue("activities"),
        durust::call_activity!(double(BenchInput { value: input + 1 })).task_queue("activities"),
        durust::call_activity!(double(BenchInput { value: input + 2 })).task_queue("activities"),
        durust::call_activity!(double(BenchInput { value: input + 3 })).task_queue("activities"),
    )
    .await?;
    Ok(first + second + third + fourth)
}

#[durust::workflow(name = "bench.select-signal-timer", version = 1)]
async fn select_signal_timer(input: u64) -> durust::Result<String> {
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

#[durust::workflow(name = "bench.select-then-wait", version = 1)]
async fn select_then_wait(input: u64) -> durust::Result<String> {
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

fn setup_select_registration_worker() -> (Worker<MemoryBackend>, MemoryBackend) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<select_signal_timer>("bench/select-registration", "workflows", 10)
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
            .start_workflow::<select_then_wait>("bench/select-replay", "workflows", 10)
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

fn setup_join_fanout_worker() -> (Worker<MemoryBackend>, MemoryBackend) {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<join_four_activities>("bench/join-fanout", "workflows", 10)
            .await
            .unwrap();
        (join_worker(backend.clone()), backend)
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
            retry_policy: durust::RetryPolicy::none(),
            start_to_close_timeout: None,
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
            .start_workflow::<double_plus_one>("bench/signal", "workflows", 10)
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

fn activity_map_input_manifest(items: u64) -> durust::PayloadRef {
    let inputs = (0..items)
        .map(|value| durust::encode_payload(&BenchInput { value }).unwrap())
        .collect::<Vec<_>>();
    durust::encode_activity_map_input_manifest(inputs, 32).unwrap()
}

fn claim_workflow_options() -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("workflows"),
        registered_workflow_types: vec![WorkflowType::new("bench.double-plus-one", 1)],
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

criterion_group!(
    benches,
    workflow_task_schedule,
    workflow_task_claim,
    workflow_task_append_commit,
    cached_wake_poll,
    crash_replay,
    select_registration,
    select_replay,
    bounded_join_fanout,
    projection_update,
    projection_read,
    activity_claim_complete,
    timer_due_scan_wakeup,
    signal_send_consume,
    activity_map_materialize,
    activity_map_item_complete
);
criterion_main!(benches);
