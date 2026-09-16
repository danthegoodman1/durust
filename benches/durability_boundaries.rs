use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use durust::provider::*;
use durust::*;
use futures::executor::block_on;
use std::time::Duration;

fn mutation_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_transaction_unrelated_runs");
    for runs in [1, 1_000, 10_000, 100_000] {
        let backend = MemoryBackend::new();
        block_on(async {
            for index in 0..runs {
                backend
                    .start_workflow(StartWorkflowRequest {
                        namespace: Namespace::default(),
                        workflow_id: WorkflowId::new(format!("scaling/{index}")),
                        workflow_type: WorkflowType::new("scaling", 1),
                        task_queue: TaskQueue::new(if index == 0 { "target" } else { "unrelated" }),
                        input: encode_payload(&vec![0_u8; 128]).unwrap(),
                    })
                    .await
                    .unwrap();
            }
        });
        group.bench_with_input(BenchmarkId::from_parameter(runs), &runs, |b, _| {
            b.iter(|| {
                block_on(async {
                    let task = backend
                        .claim_workflow_task(
                            WorkerId::new("scaling"),
                            ClaimWorkflowTaskOptions {
                                namespace: Namespace::default(),
                                task_queue: TaskQueue::new("target"),
                                registered_workflow_types: vec![WorkflowType::new("scaling", 1)],
                                lease_duration: Duration::from_secs(30),
                                shard_filter: None,
                            },
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    backend
                        .release_workflow_task(task.claim, WorkflowTaskRelease::immediate())
                        .await
                        .unwrap();
                })
            });
        });
    }
    group.finish();
}

fn commit_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_commit_unrelated_runs");
    for runs in [1, 1_000, 10_000, 100_000] {
        let backend = MemoryBackend::new();
        block_on(async {
            for index in 0..runs {
                backend
                    .start_workflow(StartWorkflowRequest {
                        namespace: Namespace::default(),
                        workflow_id: WorkflowId::new(format!("scaling/{index}")),
                        workflow_type: WorkflowType::new("scaling", 1),
                        task_queue: TaskQueue::new(if index == 0 { "target" } else { "unrelated" }),
                        input: encode_payload(&vec![0_u8; 128]).unwrap(),
                    })
                    .await
                    .unwrap();
            }
            // Keep one inbox record live so each commit remains claimable.
            backend
                .signal_workflow(SignalWorkflowRequest {
                    namespace: Namespace::default(),
                    workflow_id: WorkflowId::new("scaling/0"),
                    signal_id: SignalId::new("pending"),
                    signal_name: SignalName::new("pending"),
                    payload: encode_payload(&0_u64).unwrap(),
                })
                .await
                .unwrap();
        });
        group.bench_with_input(BenchmarkId::from_parameter(runs), &runs, |b, _| {
            b.iter(|| {
                block_on(async {
                    let task = backend
                        .claim_workflow_task(
                            WorkerId::new("scaling"),
                            ClaimWorkflowTaskOptions {
                                namespace: Namespace::default(),
                                task_queue: TaskQueue::new("target"),
                                registered_workflow_types: vec![WorkflowType::new("scaling", 1)],
                                lease_duration: Duration::from_secs(30),
                                shard_filter: None,
                            },
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    let wait = WaitRecord {
                        wait_id: WaitId::new("pending"),
                        run_id: task.run_id.clone(),
                        command_id: CommandId {
                            run_id: task.run_id,
                            seq: CommandSeq(1),
                        },
                        kind: WaitKind::Signal,
                        key: "pending".into(),
                        ready_at: None,
                    };
                    backend
                        .commit_workflow_task(
                            task.claim,
                            WorkflowTaskCommit {
                                upsert_waits: vec![wait],
                                query_projection: Some(encode_payload(&1_u64).unwrap()),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                })
            });
        });
    }
    group.finish();
}

fn local_publication(c: &mut Criterion) {
    let bytes = vec![17_u8; 64 * 1024];
    let digest = digest_bytes(&bytes);
    c.bench_function("payload_local_directory_new_64k", |b| {
        b.iter_batched(
            || tempfile::tempdir().unwrap(),
            |dir| {
                let store = LocalDirectoryBlobStore::new(dir.path(), "nested/payloads");
                store.put_sync(&digest, &bytes).unwrap();
            },
            BatchSize::PerIteration,
        );
    });
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDirectoryBlobStore::new(dir.path(), "nested/payloads");
    store.put_sync(&digest, &bytes).unwrap();
    c.bench_function("payload_local_directory_dedup_64k", |b| {
        b.iter(|| store.put_sync(&digest, &bytes).unwrap());
    });
}

fn tail_history_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_history_tail_read");
    for length in [1_000_u64, 10_000, 100_000] {
        let backend = MemoryBackend::new();
        let run_id = block_on(async {
            let run_id = backend
                .start_workflow(StartWorkflowRequest {
                    namespace: Namespace::default(),
                    workflow_id: WorkflowId::new("history"),
                    workflow_type: WorkflowType::new("scaling", 1),
                    task_queue: TaskQueue::new("target"),
                    input: encode_payload(&0_u64).unwrap(),
                })
                .await
                .unwrap()
                .run_id()
                .clone();
            let claim = backend
                .claim_workflow_task(
                    WorkerId::new("history"),
                    ClaimWorkflowTaskOptions {
                        namespace: Namespace::default(),
                        task_queue: TaskQueue::new("target"),
                        registered_workflow_types: vec![WorkflowType::new("scaling", 1)],
                        lease_duration: Duration::from_secs(30),
                        shard_filter: None,
                    },
                )
                .await
                .unwrap()
                .unwrap()
                .claim;
            backend
                .commit_workflow_task(
                    claim,
                    WorkflowTaskCommit {
                        append_events: (1..=length)
                            .map(|seq| NewHistoryEvent {
                                data: HistoryEventData::VersionMarker(VersionMarker {
                                    command_id: CommandId {
                                        run_id: run_id.clone(),
                                        seq: CommandSeq(seq),
                                    },
                                    change_id: format!("marker-{seq}"),
                                    version: 1,
                                }),
                            })
                            .collect(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            run_id
        });
        group.bench_with_input(
            BenchmarkId::from_parameter(length),
            &length,
            |b, &length| {
                b.iter(|| {
                    block_on(backend.stream_history_for_replay(StreamHistoryRequest {
                        run_id: run_id.clone(),
                        after_event_id: EventId(length),
                        up_to_event_id: EventId(length + 1),
                        max_events: 1,
                        max_bytes: usize::MAX,
                    }))
                    .unwrap()
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    mutation_scaling,
    commit_scaling,
    local_publication,
    tail_history_read
);
criterion_main!(benches);
