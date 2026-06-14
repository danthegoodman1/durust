use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

fn postgres_url_from_env() -> Option<String> {
    std::env::var("DURUST_POSTGRES_URL").ok()
}

fn test_schema(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("durust_{prefix}_{}_{}", std::process::id(), millis)
}

fn block_on_tokio<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn workflow_id_for_shard(
    backend: &PostgresBackend,
    namespace: &Namespace,
    shard_id: ShardId,
) -> crate::WorkflowId {
    for attempt in 0..10_000_u32 {
        let workflow_id = crate::WorkflowId::new(format!("wf/shard-{}/{}", shard_id.0, attempt));
        if backend.shard_for_workflow(namespace, &workflow_id) == shard_id {
            return workflow_id;
        }
    }
    panic!("could not find workflow id for shard {shard_id}");
}

async fn start_inline_child_for_tests(
    backend: &PostgresBackend,
    prefix: &str,
    parent_close_policy: ParentClosePolicy,
) -> (
    RunId,
    CommandId,
    RunId,
    crate::ClaimWorkflowTaskOptions,
    crate::ClaimWorkflowTaskOptions,
) {
    let parent_workflow_type = WorkflowType::new("postgres.parent", 1);
    let child_workflow_type = WorkflowType::new("postgres.child", 1);
    let parent_queue = crate::TaskQueue::new(format!("{prefix}-parent-workflows"));
    let child_queue = crate::TaskQueue::new(format!("{prefix}-child-workflows"));
    let parent_started = backend
        .start_workflow(crate::StartWorkflowRequest {
            namespace: crate::Namespace::default(),
            workflow_id: crate::WorkflowId::new(format!("wf/{prefix}-parent")),
            workflow_type: parent_workflow_type.clone(),
            task_queue: parent_queue.clone(),
            input: crate::encode_payload(&format!("{prefix}-parent-input")).unwrap(),
        })
        .await
        .unwrap();
    let parent_run_id = parent_started.run_id().clone();
    let parent_claim_opts = crate::ClaimWorkflowTaskOptions {
        namespace: crate::Namespace::default(),
        task_queue: parent_queue,
        registered_workflow_types: vec![parent_workflow_type],
        lease_duration: Duration::from_secs(30),
    };
    let child_claim_opts = crate::ClaimWorkflowTaskOptions {
        namespace: crate::Namespace::default(),
        task_queue: child_queue.clone(),
        registered_workflow_types: vec![child_workflow_type.clone()],
        lease_duration: Duration::from_secs(30),
    };
    let parent = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-parent-starter")),
            parent_claim_opts.clone(),
        )
        .await
        .unwrap()
        .expect("parent workflow task");
    let command_id = CommandId {
        run_id: parent_run_id.clone(),
        seq: CommandSeq(1),
    };
    let child_workflow_id = crate::WorkflowId::new(format!("wf/{prefix}-child"));
    let child_input = crate::encode_payload(&format!("{prefix}-child-input")).unwrap();
    let requested = crate::ChildWorkflowStartRequested {
        command_id: command_id.clone(),
        workflow_type: child_workflow_type.clone(),
        workflow_id: child_workflow_id.clone(),
        task_queue: child_queue,
        input: child_input.clone(),
        parent_close_policy,
        fingerprint: crate::child_workflow_fingerprint(
            child_workflow_type,
            child_workflow_id,
            crate::payload_digest(&child_input),
            child_claim_opts.task_queue.clone(),
            parent_close_policy,
        ),
    };
    assert_eq!(
        backend
            .commit_workflow_task(
                parent.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: EventId(1),
                    append_events: vec![crate::NewHistoryEvent::new(
                        HistoryEventData::ChildWorkflowStartRequested(requested.clone()),
                    )],
                    start_child_workflows: vec![crate::ChildStartOutboxMessage::from_requested(
                        &requested
                    )],
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap(),
        CommitOutcome::Committed {
            new_tail_event_id: EventId(3)
        }
    );
    let history = backend
        .stream_history(crate::StreamHistoryRequest {
            run_id: parent_run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(3),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap();
    let child_run_id = history
        .events
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowStarted(started) => Some(started.run_id.clone()),
            _ => None,
        })
        .expect("child started");
    (
        parent_run_id,
        command_id,
        child_run_id,
        parent_claim_opts,
        child_claim_opts,
    )
}

#[test]
fn postgres_schema_migration_runs_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres schema migration test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("schema_migration");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url).schema(schema.clone()),
        )
        .await
        .unwrap();
        assert_eq!(backend.schema(), schema);
        assert_eq!(backend.schema_version().await.unwrap(), 1);
        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_shard_key_is_stable_and_bounded() {
    let namespace = Namespace::new("default");
    let workflow_id = WorkflowId::new("jobs/word-count");
    let first = shard_for_workflow(&namespace, &workflow_id, 100);
    let second = shard_for_workflow(&namespace, &workflow_id, 100);
    assert_eq!(first, second);
    assert!(first.0 < 100);
}

#[test]
fn postgres_shard_key_uses_namespace() {
    let workflow_id = WorkflowId::new("same-id");
    let a = shard_for_workflow(&Namespace::new("a"), &workflow_id, 4096);
    let b = shard_for_workflow(&Namespace::new("b"), &workflow_id, 4096);
    assert_ne!(a, b);
}

#[test]
fn postgres_partition_suffix_width_tracks_partition_count() {
    assert_eq!(partition_suffix(0, 1), "p0");
    assert_eq!(partition_suffix(3, 16), "p03");
    assert_eq!(partition_suffix(12, 128), "p012");
}

#[test]
fn postgres_shard_metadata_is_validated_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres shard metadata test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("shard_metadata");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone())
                .schema(schema.clone())
                .max_pool_size(8)
                .logical_shards(16)
                .physical_partitions(4),
        )
        .await
        .unwrap();
        assert_eq!(backend.logical_shards(), 16);
        assert_eq!(backend.physical_partitions(), 4);

        let client = backend.client().await.unwrap();
        let row = client
            .query_one(
                &format!("select count(*) from {}.shard_leases", quote_ident(&schema)),
                &[],
            )
            .await
            .unwrap();
        let shard_rows: i64 = row.get(0);
        assert_eq!(shard_rows, 16);

        let err = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema.clone())
                .max_pool_size(8)
                .logical_shards(32)
                .physical_partitions(4),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("metadata mismatch for `logical_shards`"),
            "unexpected error: {err}"
        );
        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_batch_claim_honors_shard_filter_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres shard-filter claim test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("shard_filter");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema.clone())
                .logical_shards(8)
                .physical_partitions(2),
        )
        .await
        .unwrap();
        let namespace = Namespace::default();
        let target_shard = ShardId(3);
        let other_shard = ShardId(4);
        let target_workflow_id = workflow_id_for_shard(&backend, &namespace, target_shard);
        let other_workflow_id = workflow_id_for_shard(&backend, &namespace, other_shard);
        let workflow_type = WorkflowType::new("postgres.sharded", 1);
        let queue = crate::TaskQueue::new("postgres-sharded-workflows");

        backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: namespace.clone(),
                workflow_id: target_workflow_id.clone(),
                workflow_type: workflow_type.clone(),
                task_queue: queue.clone(),
                input: crate::encode_payload(&"target").unwrap(),
            })
            .await
            .unwrap();
        backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: namespace.clone(),
                workflow_id: other_workflow_id,
                workflow_type: workflow_type.clone(),
                task_queue: queue.clone(),
                input: crate::encode_payload(&"other").unwrap(),
            })
            .await
            .unwrap();

        let claimed = backend
            .claim_workflow_tasks(
                WorkerId::new("postgres-shard-filter-worker"),
                crate::ClaimWorkflowTasksOptions {
                    claim: crate::ClaimWorkflowTaskOptions {
                        namespace,
                        task_queue: queue,
                        registered_workflow_types: vec![workflow_type],
                        lease_duration: Duration::from_secs(30),
                    },
                    limit: 8,
                    shard_filter: Some(vec![target_shard]),
                },
            )
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].workflow_id, target_workflow_id);
        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_stale_shard_owner_cannot_commit_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres stale shard owner test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("stale_shard_owner");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema.clone())
                .logical_shards(8)
                .physical_partitions(2),
        )
        .await
        .unwrap();
        let namespace = Namespace::default();
        let target_shard = ShardId(3);
        let workflow_id = workflow_id_for_shard(&backend, &namespace, target_shard);
        let workflow_type = WorkflowType::new("postgres.shard-fence", 1);
        let queue = crate::TaskQueue::new("postgres-shard-fence-workflows");

        backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: namespace.clone(),
                workflow_id,
                workflow_type: workflow_type.clone(),
                task_queue: queue.clone(),
                input: crate::encode_payload(&"input").unwrap(),
            })
            .await
            .unwrap();
        let claimed = backend
            .claim_workflow_tasks(
                WorkerId::new("old-owner"),
                crate::ClaimWorkflowTasksOptions {
                    claim: crate::ClaimWorkflowTaskOptions {
                        namespace,
                        task_queue: queue,
                        registered_workflow_types: vec![workflow_type],
                        lease_duration: Duration::from_secs(30),
                    },
                    limit: 1,
                    shard_filter: Some(vec![target_shard]),
                },
            )
            .await
            .unwrap()
            .pop()
            .expect("claimed shard-filtered workflow");

        let client = backend.client().await.unwrap();
        client
            .execute(
                &format!(
                    "update {}.shard_leases
                     set owner_id = 'new-owner',
                         lease_epoch = lease_epoch + 1,
                         lease_until_ms = $1
                     where shard_id = $2",
                    quote_ident(&schema)
                ),
                &[
                    &unix_epoch_millis().saturating_add(60_000),
                    &(i32::try_from(target_shard.0).unwrap_or(i32::MAX)),
                ],
            )
            .await
            .unwrap();

        let err = backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: claimed.replay_target_event_id,
                    append_events: vec![crate::NewHistoryEvent::new(
                        HistoryEventData::WorkflowCompleted {
                            result: crate::encode_payload(&"done").unwrap(),
                        },
                    )],
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::StaleLease));
        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_claim_without_filter_acquires_shard_lease_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres unfiltered shard lease test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("unfiltered_shard_claim");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema.clone())
                .logical_shards(8)
                .physical_partitions(2),
        )
        .await
        .unwrap();
        let namespace = Namespace::default();
        let workflow_id = crate::WorkflowId::new("wf/unfiltered-shard-claim");
        let workflow_type = WorkflowType::new("postgres.unfiltered-shard", 1);
        let queue = crate::TaskQueue::new("postgres-unfiltered-shard-workflows");
        let shard_id = backend.shard_for_workflow(&namespace, &workflow_id);

        backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: namespace.clone(),
                workflow_id,
                workflow_type: workflow_type.clone(),
                task_queue: queue.clone(),
                input: crate::encode_payload(&"input").unwrap(),
            })
            .await
            .unwrap();
        let claimed = backend
            .claim_workflow_task(
                WorkerId::new("unfiltered-shard-worker"),
                crate::ClaimWorkflowTaskOptions {
                    namespace,
                    task_queue: queue,
                    registered_workflow_types: vec![workflow_type],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("unfiltered claim should acquire selected shard");

        let outcome = backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: claimed.replay_target_event_id,
                    append_events: vec![crate::NewHistoryEvent::new(
                        HistoryEventData::WorkflowCompleted {
                            result: crate::encode_payload(&"done").unwrap(),
                        },
                    )],
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            CommitOutcome::Committed {
                new_tail_event_id: EventId(2)
            }
        );

        let suffix = partition_suffix(
            shard_id.0 % backend.physical_partitions(),
            backend.physical_partitions(),
        );
        let client = backend.client().await.unwrap();
        let journal_rows: i64 = client
            .query_one(
                &format!(
                    "select count(*) from {}.shard_journal_{suffix} where shard_id = $1",
                    quote_ident(&schema)
                ),
                &[&(i32::try_from(shard_id.0).unwrap_or(i32::MAX))],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(journal_rows, 1);
        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_batch_commit_appends_one_shard_journal_operation_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres batch journal test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("batch_journal");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema.clone())
                .logical_shards(8)
                .physical_partitions(2),
        )
        .await
        .unwrap();
        let namespace = Namespace::default();
        let target_shard = ShardId(5);
        let workflow_type = WorkflowType::new("postgres.batch-journal", 1);
        let queue = crate::TaskQueue::new("postgres-batch-journal-workflows");
        let first_workflow_id = workflow_id_for_shard(&backend, &namespace, target_shard);
        let second_workflow_id = {
            let mut suffix = 10_000_u32;
            loop {
                let workflow_id =
                    crate::WorkflowId::new(format!("wf/shard-{}/{}", target_shard.0, suffix));
                if workflow_id != first_workflow_id
                    && backend.shard_for_workflow(&namespace, &workflow_id) == target_shard
                {
                    break workflow_id;
                }
                suffix += 1;
            }
        };

        for (workflow_id, input) in [(first_workflow_id, "first"), (second_workflow_id, "second")] {
            backend
                .start_workflow(crate::StartWorkflowRequest {
                    namespace: namespace.clone(),
                    workflow_id,
                    workflow_type: workflow_type.clone(),
                    task_queue: queue.clone(),
                    input: crate::encode_payload(&input).unwrap(),
                })
                .await
                .unwrap();
        }

        let claimed = backend
            .claim_workflow_tasks(
                WorkerId::new("batch-journal-worker"),
                crate::ClaimWorkflowTasksOptions {
                    claim: crate::ClaimWorkflowTaskOptions {
                        namespace,
                        task_queue: queue,
                        registered_workflow_types: vec![workflow_type],
                        lease_duration: Duration::from_secs(30),
                    },
                    limit: 2,
                    shard_filter: Some(vec![target_shard]),
                },
            )
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);

        let results = backend
            .commit_workflow_tasks(crate::WorkflowTaskCommitBatch {
                commits: claimed
                    .into_iter()
                    .map(|claimed| crate::WorkflowTaskCommitInput {
                        claim: claimed.claim,
                        commit: WorkflowTaskCommit {
                            expected_tail_event_id: claimed.replay_target_event_id,
                            append_events: vec![crate::NewHistoryEvent::new(
                                HistoryEventData::WorkflowCompleted {
                                    result: crate::encode_payload(&"done").unwrap(),
                                },
                            )],
                            ..WorkflowTaskCommit::default()
                        },
                    })
                    .collect(),
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        for result in results {
            assert_eq!(
                result.result.unwrap(),
                CommitOutcome::Committed {
                    new_tail_event_id: EventId(2)
                }
            );
        }

        let suffix = partition_suffix(
            target_shard.0 % backend.physical_partitions(),
            backend.physical_partitions(),
        );
        let client = backend.client().await.unwrap();
        let rows = client
            .query(
                &format!(
                    "select operation from {}.shard_journal_{suffix} where shard_id = $1 order by journal_seq asc",
                    quote_ident(&schema)
                ),
                &[&(i32::try_from(target_shard.0).unwrap_or(i32::MAX))],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let operation_blob: Vec<u8> = rows[0].get(0);
        let operation: ShardJournalOperation = rmp_serde::from_slice(&operation_blob).unwrap();
        assert!(matches!(
            operation.kind,
            ShardJournalOperationKind::WorkflowTaskBatch
        ));
        assert_eq!(operation.entries.len(), 2);
        assert!(operation.entries.iter().all(|entry| matches!(
            entry.result,
            ShardJournalCommitResult::Committed {
                appended_events: 1,
                terminal: true,
                ready_reason: None,
            }
        )));
        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_rejects_incompatible_schema_version_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres incompatible schema test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("schema_version_mismatch");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
        )
        .await
        .unwrap();
        backend.force_schema_version_for_tests(999).await.unwrap();

        let err = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url).schema(schema.clone()),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("has version 999"),
            "unexpected error: {err}"
        );
        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_schema_identifier_is_validated() {
    block_on_tokio(async {
        let err = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new("postgresql://localhost/unused").schema("bad-schema"),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("must contain only ASCII"));
    });
}

#[test]
fn postgres_core_workflow_visibility_round_trip_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres core workflow test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("core_workflow");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema)
                .payload_storage(PayloadStorageConfig::new().inline_threshold_bytes(1)),
        )
        .await
        .unwrap();
        let workflow_id = crate::WorkflowId::new("wf/postgres-core");
        let workflow_type = WorkflowType::new("postgres.core", 1);
        let queue = crate::TaskQueue::new("postgres-core-workflows");
        let input_value = "postgres-core-input".repeat(8);
        let started = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: workflow_id.clone(),
                workflow_type: workflow_type.clone(),
                task_queue: queue.clone(),
                input: crate::encode_payload(&input_value).unwrap(),
            })
            .await
            .unwrap();
        let run_id = started.run_id().clone();
        let duplicate = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: workflow_id.clone(),
                workflow_type: workflow_type.clone(),
                task_queue: queue.clone(),
                input: crate::encode_payload(&"ignored").unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            duplicate,
            crate::StartWorkflowOutcome::AlreadyStarted {
                run_id: run_id.clone()
            }
        );

        let public_history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id: run_id.clone(),
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(1),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        let HistoryEventData::WorkflowStarted { input, .. } = &public_history.events[0].data else {
            panic!("expected workflow start event");
        };
        assert!(matches!(input, PayloadRef::Inline { .. }));
        assert_eq!(crate::decode_payload::<String>(input).unwrap(), input_value);

        let replay_history = backend
            .stream_history_for_replay(crate::StreamHistoryRequest {
                run_id: run_id.clone(),
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(1),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        let HistoryEventData::WorkflowStarted { input, .. } = &replay_history.events[0].data else {
            panic!("expected workflow start event");
        };
        assert!(matches!(input, PayloadRef::Blob { .. }));
        let hydrated = backend.hydrate_payload(input.clone()).await.unwrap();
        assert_eq!(
            crate::decode_payload::<String>(&hydrated).unwrap(),
            input_value
        );

        let wrong_queue = backend
            .claim_workflow_task(
                WorkerId::new("postgres-core-wrong-queue"),
                crate::ClaimWorkflowTaskOptions {
                    namespace: crate::Namespace::default(),
                    task_queue: crate::TaskQueue::new("wrong"),
                    registered_workflow_types: vec![workflow_type.clone()],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(wrong_queue.is_none());

        let claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: queue,
            registered_workflow_types: vec![workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let claimed = backend
            .claim_workflow_task(WorkerId::new("postgres-core-worker-a"), claim_opts.clone())
            .await
            .unwrap()
            .expect("workflow task");
        assert_eq!(claimed.run_id, run_id);
        assert_eq!(claimed.replay_target_event_id, EventId(1));
        assert_eq!(claimed.reason, WorkflowTaskReason::WorkflowStarted);
        let double_claim = backend
            .claim_workflow_task(WorkerId::new("postgres-core-worker-b"), claim_opts.clone())
            .await
            .unwrap();
        assert!(double_claim.is_none());

        backend
            .release_workflow_task(
                claimed.claim,
                crate::WorkflowTaskRelease::delayed(
                    WorkflowTaskReason::CacheEvicted,
                    Duration::from_millis(25),
                ),
            )
            .await
            .unwrap();
        let hidden = backend
            .claim_workflow_task(WorkerId::new("postgres-core-worker-c"), claim_opts.clone())
            .await
            .unwrap();
        assert!(hidden.is_none());
        tokio::time::sleep(Duration::from_millis(40)).await;
        let visible = backend
            .claim_workflow_task(WorkerId::new("postgres-core-worker-d"), claim_opts.clone())
            .await
            .unwrap()
            .expect("workflow task visible after delayed release");

        let marker_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(10),
        };
        let signal_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(11),
        };
        let timer_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(12),
        };
        let activity_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(13),
        };
        let signal_wait_id = crate::WaitId::new(format!("{}:11:signal", run_id.0));
        let timer_wait_id = crate::WaitId::new(format!("{}:12:timer", run_id.0));
        let query_value = "postgres-query-state".repeat(8);
        let timer_fire_at = TimestampMs(1);
        let activity_queue = crate::TaskQueue::new("postgres-core-activities");
        let command_fingerprint =
            |kind: crate::CommandKind, name: &str| crate::CommandFingerprint {
                kind,
                name: name.to_owned(),
                input_digest: None,
                options_digest: "test".to_owned(),
            };
        let activity_input = crate::encode_payload(&"postgres-activity-input".repeat(8)).unwrap();
        let activity_scheduled = crate::ActivityScheduled {
            command_id: activity_command_id.clone(),
            activity_name: crate::ActivityName::new("postgres.echo"),
            task_queue: activity_queue.clone(),
            retry_policy: crate::RetryPolicy::none(),
            start_to_close_timeout: Some(Duration::from_secs(30)),
            heartbeat_timeout: Some(Duration::from_secs(30)),
            input: activity_input,
            fingerprint: command_fingerprint(crate::CommandKind::Activity, "postgres.echo"),
        };
        let commit = backend
            .commit_workflow_task(
                visible.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: visible.replay_target_event_id,
                    append_events: vec![
                        crate::NewHistoryEvent::new(HistoryEventData::VersionMarker(
                            crate::VersionMarker {
                                command_id: marker_command_id.clone(),
                                change_id: "postgres-change".to_owned(),
                                version: 2,
                            },
                        )),
                        crate::NewHistoryEvent::new(HistoryEventData::TimerStarted(
                            crate::TimerStarted {
                                command_id: timer_command_id.clone(),
                                fire_at: timer_fire_at,
                                fingerprint: command_fingerprint(
                                    crate::CommandKind::Timer,
                                    "timer",
                                ),
                            },
                        )),
                        crate::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                            activity_scheduled.clone(),
                        )),
                    ],
                    schedule_activities: vec![ActivityTask::from_scheduled(&activity_scheduled)],
                    upsert_waits: vec![
                        crate::WaitRecord {
                            wait_id: signal_wait_id.clone(),
                            run_id: run_id.clone(),
                            command_id: signal_command_id.clone(),
                            kind: WaitKind::Signal,
                            key: "postgres-signal".to_owned(),
                            ready_at: None,
                        },
                        crate::WaitRecord {
                            wait_id: timer_wait_id.clone(),
                            run_id: run_id.clone(),
                            command_id: timer_command_id.clone(),
                            kind: WaitKind::Timer,
                            key: "timer".to_owned(),
                            ready_at: Some(timer_fire_at),
                        },
                    ],
                    query_projection: Some(crate::encode_payload(&query_value).unwrap()),
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            commit,
            CommitOutcome::Committed {
                new_tail_event_id: EventId(4)
            }
        );

        let projection = backend
            .query_projection(crate::QueryProjectionRequest {
                namespace: crate::Namespace::default(),
                workflow_id: workflow_id.clone(),
            })
            .await
            .unwrap();
        let QueryProjectionOutcome::Found {
            run_id: projected_run_id,
            event_id: projected_event_id,
            payload: projected_payload,
        } = projection
        else {
            panic!("expected query projection");
        };
        assert_eq!(projected_run_id, run_id);
        assert_eq!(projected_event_id, EventId(4));
        assert_eq!(
            crate::decode_payload::<String>(&projected_payload).unwrap(),
            query_value
        );

        let versions = backend
            .workflow_change_versions(crate::WorkflowChangeVersionsRequest {
                namespace: crate::Namespace::default(),
                workflow_id: Some(workflow_id.clone()),
                run_id: Some(run_id.clone()),
                change_id: Some("postgres-change".to_owned()),
            })
            .await
            .unwrap();
        assert_eq!(versions.records.len(), 1);
        assert_eq!(versions.records[0].version, 2);
        assert_eq!(versions.records[0].first_event_id, EventId(2));
        assert_eq!(
            versions.records[0].status,
            WorkflowChangeVersionStatus::Open
        );

        let activity_opts = ClaimActivityOptions {
            namespace: crate::Namespace::default(),
            task_queue: activity_queue,
            registered_activity_names: vec![crate::ActivityName::new("postgres.echo")],
            lease_duration: Duration::from_secs(30),
        };
        let activity = backend
            .claim_activity_task(
                WorkerId::new("postgres-core-activity-worker-a"),
                activity_opts.clone(),
            )
            .await
            .unwrap()
            .expect("activity task");
        assert_eq!(
            activity.task.activity_id,
            ActivityId::new(&activity_command_id)
        );
        assert_eq!(activity.task.attempt, 1);
        assert_eq!(
            crate::decode_payload::<String>(&activity.task.input).unwrap(),
            "postgres-activity-input".repeat(8)
        );
        assert_eq!(
            backend
                .heartbeat_activity(ActivityHeartbeatRequest {
                    claim: activity.claim.clone(),
                })
                .await
                .unwrap(),
            ActivityHeartbeatOutcome::Recorded
        );
        let activity_output = "postgres-activity-output".repeat(8);
        assert_eq!(
            backend
                .complete_activity(CompleteActivityRequest {
                    claim: activity.claim.clone(),
                    result: crate::encode_payload(&activity_output).unwrap(),
                })
                .await
                .unwrap(),
            CompleteActivityOutcome::Completed {
                event_id: EventId(5)
            }
        );
        assert_eq!(
            backend
                .complete_activity(CompleteActivityRequest {
                    claim: activity.claim,
                    result: crate::encode_payload(&"ignored").unwrap(),
                })
                .await
                .unwrap(),
            CompleteActivityOutcome::AlreadyCompleted
        );
        let no_activity = backend
            .claim_activity_task(
                WorkerId::new("postgres-core-activity-worker-b"),
                activity_opts,
            )
            .await
            .unwrap();
        assert!(no_activity.is_none());
        let activity_ready = backend
            .claim_workflow_task(WorkerId::new("postgres-core-worker-e"), claim_opts.clone())
            .await
            .unwrap()
            .expect("activity completion should wake workflow");
        assert_eq!(activity_ready.replay_target_event_id, EventId(5));
        assert_eq!(activity_ready.reason, WorkflowTaskReason::ActivityCompleted);
        assert_eq!(
            backend
                .commit_workflow_task(
                    activity_ready.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: activity_ready.replay_target_event_id,
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(5)
            }
        );

        let signal_payload_value = "postgres-signal-payload".repeat(8);
        let signal = backend
            .signal_workflow(crate::SignalWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: workflow_id.clone(),
                signal_id: crate::SignalId::new("postgres-signal-1"),
                signal_name: crate::SignalName::new("postgres-signal"),
                payload: crate::encode_payload(&signal_payload_value).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(signal, SignalWorkflowOutcome::Accepted);
        let duplicate_signal = backend
            .signal_workflow(crate::SignalWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: workflow_id.clone(),
                signal_id: crate::SignalId::new("postgres-signal-1"),
                signal_name: crate::SignalName::new("postgres-signal"),
                payload: crate::encode_payload(&"ignored").unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(duplicate_signal, SignalWorkflowOutcome::Duplicate);
        let inbox = backend
            .read_signal_inbox(crate::ReadSignalInboxRequest {
                run_id: run_id.clone(),
                signal_name: crate::SignalName::new("postgres-signal"),
            })
            .await
            .unwrap()
            .expect("signal inbox record");
        assert_eq!(inbox.signal_id, crate::SignalId::new("postgres-signal-1"));
        assert_eq!(
            crate::decode_payload::<String>(&inbox.payload).unwrap(),
            signal_payload_value
        );
        let signal_claim = backend
            .claim_workflow_task(WorkerId::new("postgres-core-worker-f"), claim_opts.clone())
            .await
            .unwrap()
            .expect("signal should wake workflow");
        assert_eq!(signal_claim.replay_target_event_id, EventId(5));
        assert_eq!(signal_claim.reason, WorkflowTaskReason::SignalReceived);
        let signal_commit = backend
            .commit_workflow_task(
                signal_claim.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: signal_claim.replay_target_event_id,
                    append_events: vec![crate::NewHistoryEvent::new(
                        HistoryEventData::SignalConsumed(crate::SignalConsumed {
                            command_id: signal_command_id,
                            signal_id: inbox.signal_id.clone(),
                            signal_name: inbox.signal_name.clone(),
                            payload: inbox.payload.clone(),
                            fingerprint: command_fingerprint(
                                crate::CommandKind::Signal,
                                "postgres-signal",
                            ),
                        }),
                    )],
                    consume_signals: vec![inbox.signal_id],
                    delete_waits: vec![signal_wait_id],
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            signal_commit,
            CommitOutcome::Committed {
                new_tail_event_id: EventId(6)
            }
        );

        let timer_outcome = backend
            .fire_due_timers(crate::FireDueTimersRequest {
                namespace: crate::Namespace::default(),
                now: timer_fire_at,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(timer_outcome, FireDueTimersOutcome { fired: 1 });
        let timer_claim = backend
            .claim_workflow_task(WorkerId::new("postgres-core-worker-g"), claim_opts.clone())
            .await
            .unwrap()
            .expect("timer should wake workflow");
        assert_eq!(timer_claim.replay_target_event_id, EventId(7));
        assert_eq!(timer_claim.reason, WorkflowTaskReason::TimerFired);

        let output_value = "postgres-core-output".repeat(8);
        let commit = backend
            .commit_workflow_task(
                timer_claim.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: timer_claim.replay_target_event_id,
                    append_events: vec![crate::NewHistoryEvent::new(
                        HistoryEventData::WorkflowCompleted {
                            result: crate::encode_payload(&output_value).unwrap(),
                        },
                    )],
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            commit,
            CommitOutcome::Committed {
                new_tail_event_id: EventId(8)
            }
        );

        let completed_history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id: run_id.clone(),
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(8),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        assert_eq!(completed_history.events.len(), 8);
        let HistoryEventData::ActivityCompleted(completed) = &completed_history.events[4].data
        else {
            panic!("expected activity completed event");
        };
        assert_eq!(
            crate::decode_payload::<String>(&completed.result).unwrap(),
            activity_output
        );
        let HistoryEventData::WorkflowCompleted { result } = &completed_history.events[7].data
        else {
            panic!("expected workflow completed event");
        };
        assert!(matches!(result, PayloadRef::Inline { .. }));
        assert_eq!(
            crate::decode_payload::<String>(result).unwrap(),
            output_value
        );

        let terminal_claim = backend
            .claim_workflow_task(WorkerId::new("postgres-core-worker-h"), claim_opts)
            .await
            .unwrap();
        assert!(terminal_claim.is_none());

        let versions = backend
            .workflow_change_versions(crate::WorkflowChangeVersionsRequest {
                namespace: crate::Namespace::default(),
                workflow_id: Some(workflow_id),
                run_id: Some(run_id.clone()),
                change_id: Some("postgres-change".to_owned()),
            })
            .await
            .unwrap();
        assert_eq!(
            versions.records[0].status,
            WorkflowChangeVersionStatus::Closed
        );

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_child_start_is_inline_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres child start test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("child_start_inline");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema)
                .payload_storage(PayloadStorageConfig::new().inline_threshold_bytes(1)),
        )
        .await
        .unwrap();
        let parent_workflow_id = crate::WorkflowId::new("wf/postgres-child-parent");
        let parent_workflow_type = WorkflowType::new("postgres.parent", 1);
        let parent_queue = crate::TaskQueue::new("postgres-parent-workflows");
        let started = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: parent_workflow_id.clone(),
                workflow_type: parent_workflow_type.clone(),
                task_queue: parent_queue.clone(),
                input: crate::encode_payload(&"parent-input").unwrap(),
            })
            .await
            .unwrap();
        let parent_run_id = started.run_id().clone();
        let parent_claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: parent_queue,
            registered_workflow_types: vec![parent_workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let parent = backend
            .claim_workflow_task(
                WorkerId::new("postgres-child-start-parent"),
                parent_claim_opts.clone(),
            )
            .await
            .unwrap()
            .expect("parent workflow task");

        let command_id = CommandId {
            run_id: parent_run_id.clone(),
            seq: CommandSeq(1),
        };
        let child_workflow_type = WorkflowType::new("postgres.child", 1);
        let child_workflow_id = crate::WorkflowId::new("wf/postgres-child-inline");
        let child_queue = crate::TaskQueue::new("postgres-child-workflows");
        let child_input_value = "postgres-child-input".repeat(8);
        let child_input = crate::encode_payload(&child_input_value).unwrap();
        let requested = crate::ChildWorkflowStartRequested {
            command_id: command_id.clone(),
            workflow_type: child_workflow_type.clone(),
            workflow_id: child_workflow_id.clone(),
            task_queue: child_queue.clone(),
            input: child_input.clone(),
            parent_close_policy: ParentClosePolicy::Cancel,
            fingerprint: crate::child_workflow_fingerprint(
                child_workflow_type.clone(),
                child_workflow_id.clone(),
                crate::payload_digest(&child_input),
                child_queue.clone(),
                ParentClosePolicy::Cancel,
            ),
        };
        let commit = backend
            .commit_workflow_task(
                parent.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: EventId(1),
                    append_events: vec![crate::NewHistoryEvent::new(
                        HistoryEventData::ChildWorkflowStartRequested(requested.clone()),
                    )],
                    start_child_workflows: vec![crate::ChildStartOutboxMessage::from_requested(
                        &requested,
                    )],
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            commit,
            CommitOutcome::Committed {
                new_tail_event_id: EventId(3)
            }
        );
        assert_eq!(
            backend
                .dispatch_child_workflow_starts(crate::DispatchChildWorkflowStartsRequest {
                    namespace: crate::Namespace::default(),
                    limit: 16,
                })
                .await
                .unwrap(),
            DispatchChildWorkflowStartsOutcome { dispatched: 0 }
        );

        let parent_ready = backend
            .claim_workflow_task(
                WorkerId::new("postgres-child-start-parent-ready"),
                parent_claim_opts,
            )
            .await
            .unwrap()
            .expect("parent woken by inline child start");
        assert_eq!(
            parent_ready.reason,
            WorkflowTaskReason::ChildWorkflowStarted
        );
        assert_eq!(parent_ready.replay_target_event_id, EventId(3));
        let parent_history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id: parent_run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(3),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        let HistoryEventData::ChildWorkflowStartRequested(stored_request) =
            &parent_history.events[1].data
        else {
            panic!("expected child start request");
        };
        assert_eq!(
            crate::decode_payload::<String>(&stored_request.input).unwrap(),
            child_input_value
        );
        let HistoryEventData::ChildWorkflowStarted(started_child) = &parent_history.events[2].data
        else {
            panic!("expected child started event");
        };
        assert_eq!(started_child.command_id, command_id);
        assert_eq!(started_child.workflow_id, child_workflow_id);

        let child = backend
            .claim_workflow_task(
                WorkerId::new("postgres-child-start-child"),
                crate::ClaimWorkflowTaskOptions {
                    namespace: crate::Namespace::default(),
                    task_queue: child_queue,
                    registered_workflow_types: vec![child_workflow_type],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("child workflow task");
        assert_eq!(child.workflow_id, child_workflow_id);
        assert_eq!(child.run_id, started_child.run_id);
        assert_eq!(child.reason, WorkflowTaskReason::WorkflowStarted);
        let child_history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id: child.run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(1),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        let HistoryEventData::WorkflowStarted { input, .. } = &child_history.events[0].data else {
            panic!("expected child workflow start event");
        };
        assert_eq!(
            crate::decode_payload::<String>(input).unwrap(),
            child_input_value
        );

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_child_start_conflict_records_failure_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres child conflict test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("child_start_conflict");
        let backend =
            PostgresBackend::connect_with_config(PostgresBackendConfig::new(url).schema(schema))
                .await
                .unwrap();
        let child_workflow_id = crate::WorkflowId::new("wf/postgres-child-conflict");
        let child_workflow_type = WorkflowType::new("postgres.child", 1);
        backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: child_workflow_id.clone(),
                workflow_type: child_workflow_type.clone(),
                task_queue: crate::TaskQueue::new("postgres-existing-child-workflows"),
                input: crate::encode_payload(&"existing-child").unwrap(),
            })
            .await
            .unwrap();

        let parent_workflow_type = WorkflowType::new("postgres.parent", 1);
        let parent_queue = crate::TaskQueue::new("postgres-conflict-parent-workflows");
        let parent_started = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: crate::WorkflowId::new("wf/postgres-conflict-parent"),
                workflow_type: parent_workflow_type.clone(),
                task_queue: parent_queue.clone(),
                input: crate::encode_payload(&"parent-input").unwrap(),
            })
            .await
            .unwrap();
        let parent_run_id = parent_started.run_id().clone();
        let parent_claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: parent_queue,
            registered_workflow_types: vec![parent_workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let parent = backend
            .claim_workflow_task(
                WorkerId::new("postgres-child-conflict-parent"),
                parent_claim_opts.clone(),
            )
            .await
            .unwrap()
            .expect("parent workflow task");

        let child_queue = crate::TaskQueue::new("postgres-conflict-child-workflows");
        let input = crate::encode_payload(&"new-child").unwrap();
        let command_id = CommandId {
            run_id: parent_run_id.clone(),
            seq: CommandSeq(1),
        };
        let requested = crate::ChildWorkflowStartRequested {
            command_id: command_id.clone(),
            workflow_type: child_workflow_type.clone(),
            workflow_id: child_workflow_id,
            task_queue: child_queue.clone(),
            input: input.clone(),
            parent_close_policy: ParentClosePolicy::Cancel,
            fingerprint: crate::child_workflow_fingerprint(
                child_workflow_type,
                crate::WorkflowId::new("wf/postgres-child-conflict"),
                crate::payload_digest(&input),
                child_queue,
                ParentClosePolicy::Cancel,
            ),
        };
        assert_eq!(
            backend
                .commit_workflow_task(
                    parent.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![crate::NewHistoryEvent::new(
                            HistoryEventData::ChildWorkflowStartRequested(requested.clone()),
                        )],
                        start_child_workflows: vec![
                            crate::ChildStartOutboxMessage::from_requested(&requested)
                        ],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(3)
            }
        );

        let parent_ready = backend
            .claim_workflow_task(
                WorkerId::new("postgres-child-conflict-parent-ready"),
                parent_claim_opts,
            )
            .await
            .unwrap()
            .expect("parent woken by child conflict");
        assert_eq!(parent_ready.reason, WorkflowTaskReason::ChildWorkflowFailed);
        let parent_history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id: parent_run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(3),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        let HistoryEventData::ChildWorkflowFailed(failed) = &parent_history.events[2].data else {
            panic!("expected child workflow failed event");
        };
        assert_eq!(failed.command_id, command_id);
        assert_eq!(
            failed.failure.error_type,
            "durust.child_workflow_id_conflict"
        );
        assert!(failed.failure.non_retryable);

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_child_completion_routes_to_parent_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres child completion test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("child_completion");
        let backend =
            PostgresBackend::connect_with_config(PostgresBackendConfig::new(url).schema(schema))
                .await
                .unwrap();
        let (parent_run_id, command_id, child_run_id, parent_claim_opts, child_claim_opts) =
            start_inline_child_for_tests(
                &backend,
                "postgres-child-completion",
                ParentClosePolicy::Cancel,
            )
            .await;
        let child = backend
            .claim_workflow_task(
                WorkerId::new("postgres-child-completion-child"),
                child_claim_opts,
            )
            .await
            .unwrap()
            .expect("child workflow task");
        assert_eq!(child.run_id, child_run_id);
        let child_result = "postgres-child-result".repeat(4);
        assert_eq!(
            backend
                .commit_workflow_task(
                    child.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![crate::NewHistoryEvent::new(
                            HistoryEventData::WorkflowCompleted {
                                result: crate::encode_payload(&child_result).unwrap(),
                            },
                        )],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(2)
            }
        );
        let parent = backend
            .claim_workflow_task(
                WorkerId::new("postgres-child-completion-parent"),
                parent_claim_opts,
            )
            .await
            .unwrap()
            .expect("parent woken by child completion");
        assert_eq!(parent.reason, WorkflowTaskReason::ChildWorkflowCompleted);
        assert_eq!(parent.replay_target_event_id, EventId(4));
        let parent_history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id: parent_run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(4),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        let HistoryEventData::ChildWorkflowCompleted(completed) = &parent_history.events[3].data
        else {
            panic!("expected child completion event");
        };
        assert_eq!(completed.command_id, command_id);
        assert_eq!(
            crate::decode_payload::<String>(&completed.result).unwrap(),
            child_result
        );

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_parent_close_policy_is_applied_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres parent close test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("parent_close_policy");
        let backend =
            PostgresBackend::connect_with_config(PostgresBackendConfig::new(url).schema(schema))
                .await
                .unwrap();
        let (
            _cancel_parent_run_id,
            _cancel_command_id,
            cancel_child_run_id,
            cancel_parent_opts,
            cancel_child_opts,
        ) = start_inline_child_for_tests(
            &backend,
            "postgres-parent-close-cancel",
            ParentClosePolicy::Cancel,
        )
        .await;
        let cancel_parent = backend
            .claim_workflow_task(
                WorkerId::new("postgres-parent-close-cancel-parent"),
                cancel_parent_opts,
            )
            .await
            .unwrap()
            .expect("cancel parent ready");
        assert_eq!(
            backend
                .commit_workflow_task(
                    cancel_parent.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(3),
                        append_events: vec![crate::NewHistoryEvent::new(
                            HistoryEventData::WorkflowCompleted {
                                result: crate::encode_payload(&()).unwrap(),
                            },
                        )],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(4)
            }
        );
        let cancelled_child_claim = backend
            .claim_workflow_task(
                WorkerId::new("postgres-parent-close-cancel-child"),
                cancel_child_opts,
            )
            .await
            .unwrap();
        assert!(cancelled_child_claim.is_none());
        let cancelled_child_history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id: cancel_child_run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(10),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        assert!(
            cancelled_child_history
                .events
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowCancelled { .. }))
        );

        let (
            _abandon_parent_run_id,
            _abandon_command_id,
            abandon_child_run_id,
            abandon_parent_opts,
            abandon_child_opts,
        ) = start_inline_child_for_tests(
            &backend,
            "postgres-parent-close-abandon",
            ParentClosePolicy::Abandon,
        )
        .await;
        let abandon_parent = backend
            .claim_workflow_task(
                WorkerId::new("postgres-parent-close-abandon-parent"),
                abandon_parent_opts,
            )
            .await
            .unwrap()
            .expect("abandon parent ready");
        assert_eq!(
            backend
                .commit_workflow_task(
                    abandon_parent.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(3),
                        append_events: vec![crate::NewHistoryEvent::new(
                            HistoryEventData::WorkflowCompleted {
                                result: crate::encode_payload(&()).unwrap(),
                            },
                        )],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(4)
            }
        );
        let abandoned_child = backend
            .claim_workflow_task(
                WorkerId::new("postgres-parent-close-abandon-child"),
                abandon_child_opts,
            )
            .await
            .unwrap()
            .expect("abandoned child remains claimable");
        assert_eq!(abandoned_child.run_id, abandon_child_run_id);

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_cancel_workflow_cleans_operational_state_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres cancellation test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("cancel_workflow");
        let backend =
            PostgresBackend::connect_with_config(PostgresBackendConfig::new(url).schema(schema))
                .await
                .unwrap();
        let workflow_id = crate::WorkflowId::new("wf/postgres-cancel");
        let workflow_type = WorkflowType::new("postgres.cancel", 1);
        let workflow_queue = crate::TaskQueue::new("postgres-cancel-workflows");
        let activity_queue = crate::TaskQueue::new("postgres-cancel-activities");
        let started = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: workflow_id.clone(),
                workflow_type: workflow_type.clone(),
                task_queue: workflow_queue.clone(),
                input: crate::encode_payload(&"cancel-input").unwrap(),
            })
            .await
            .unwrap();
        let run_id = started.run_id().clone();
        let claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: workflow_queue,
            registered_workflow_types: vec![workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let claimed = backend
            .claim_workflow_task(WorkerId::new("postgres-cancel-worker"), claim_opts.clone())
            .await
            .unwrap()
            .expect("workflow task");
        let timer_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(1),
        };
        let activity_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(2),
        };
        let command_fingerprint =
            |kind: crate::CommandKind, name: &str| crate::CommandFingerprint {
                kind,
                name: name.to_owned(),
                input_digest: None,
                options_digest: "test".to_owned(),
            };
        let fire_at = TimestampMs(1);
        let scheduled = crate::ActivityScheduled {
            command_id: activity_command_id.clone(),
            activity_name: crate::ActivityName::new("postgres.cancel-activity"),
            task_queue: activity_queue.clone(),
            retry_policy: crate::RetryPolicy::none(),
            start_to_close_timeout: Some(Duration::from_secs(30)),
            heartbeat_timeout: Some(Duration::from_secs(30)),
            input: crate::encode_payload(&"activity-input").unwrap(),
            fingerprint: command_fingerprint(
                crate::CommandKind::Activity,
                "postgres.cancel-activity",
            ),
        };
        assert_eq!(
            backend
                .commit_workflow_task(
                    claimed.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![
                            crate::NewHistoryEvent::new(HistoryEventData::TimerStarted(
                                crate::TimerStarted {
                                    command_id: timer_command_id.clone(),
                                    fire_at,
                                    fingerprint: command_fingerprint(
                                        crate::CommandKind::Timer,
                                        "timer",
                                    ),
                                },
                            )),
                            crate::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                                scheduled.clone(),
                            )),
                        ],
                        upsert_waits: vec![crate::WaitRecord {
                            wait_id: crate::WaitId::new(format!("{}:1:timer", run_id.0)),
                            run_id: run_id.clone(),
                            command_id: timer_command_id,
                            kind: WaitKind::Timer,
                            key: "timer".to_owned(),
                            ready_at: Some(fire_at),
                        }],
                        schedule_activities: vec![ActivityTask::from_scheduled(&scheduled)],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(3)
            }
        );
        let activity_opts = ClaimActivityOptions {
            namespace: crate::Namespace::default(),
            task_queue: activity_queue.clone(),
            registered_activity_names: vec![crate::ActivityName::new("postgres.cancel-activity")],
            lease_duration: Duration::from_secs(30),
        };
        let activity = backend
            .claim_activity_task(
                WorkerId::new("postgres-cancel-activity-worker"),
                activity_opts.clone(),
            )
            .await
            .unwrap()
            .expect("activity task");
        assert_eq!(
            backend
                .cancel_workflow(CancelWorkflowRequest {
                    namespace: crate::Namespace::default(),
                    workflow_id: workflow_id.clone(),
                    reason: "operator cancelled".to_owned(),
                })
                .await
                .unwrap(),
            CancelWorkflowOutcome::Cancelled {
                run_id: run_id.clone(),
                event_id: EventId(4)
            }
        );
        assert_eq!(
            backend
                .cancel_workflow(CancelWorkflowRequest {
                    namespace: crate::Namespace::default(),
                    workflow_id: workflow_id.clone(),
                    reason: "duplicate".to_owned(),
                })
                .await
                .unwrap(),
            CancelWorkflowOutcome::AlreadyTerminal {
                run_id: run_id.clone()
            }
        );
        assert!(
            backend
                .claim_workflow_task(WorkerId::new("postgres-cancel-reclaim"), claim_opts)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            backend
                .fire_due_timers(crate::FireDueTimersRequest {
                    namespace: crate::Namespace::default(),
                    now: fire_at,
                    limit: 10,
                })
                .await
                .unwrap(),
            FireDueTimersOutcome { fired: 0 }
        );
        assert!(
            backend
                .claim_activity_task(
                    WorkerId::new("postgres-cancel-leftover-activity"),
                    activity_opts,
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            backend
                .complete_activity(CompleteActivityRequest {
                    claim: activity.claim,
                    result: crate::encode_payload(&"late").unwrap(),
                })
                .await
                .unwrap(),
            CompleteActivityOutcome::AlreadyCompleted
        );
        assert!(matches!(
            backend
                .signal_workflow(crate::SignalWorkflowRequest {
                    namespace: crate::Namespace::default(),
                    workflow_id,
                    signal_id: crate::SignalId::new("postgres-cancel-signal"),
                    signal_name: crate::SignalName::new("cancelled"),
                    payload: crate::encode_payload(&"ignored").unwrap(),
                })
                .await,
            Err(Error::TerminalWorkflow)
        ));
        let history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(10),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        assert_eq!(history.events.len(), 4);
        assert!(matches!(
            history.events[3].data,
            HistoryEventData::WorkflowCancelled { .. }
        ));

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_payload_roots_and_gc_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres payload GC test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("payload_gc");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema.clone())
                .payload_storage(PayloadStorageConfig::new().inline_threshold_bytes(1)),
        )
        .await
        .unwrap();
        let workflow_id = crate::WorkflowId::new("wf/postgres-payload-gc");
        let workflow_type = WorkflowType::new("postgres.payload-gc", 1);
        let queue = crate::TaskQueue::new("postgres-payload-gc-workflows");
        let input_value = "postgres-payload-gc-input".repeat(8);
        let started = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id,
                workflow_type,
                task_queue: queue,
                input: crate::encode_payload(&input_value).unwrap(),
            })
            .await
            .unwrap();
        let run_id = started.run_id().clone();

        let roots = backend.payload_roots().await.unwrap();
        assert!(roots.roots.iter().any(|root| {
            matches!(
                root.payload(),
                PayloadRef::Blob { uri, .. } if uri.starts_with("postgres://payload/")
            )
        }));

        let orphan_bytes = b"postgres unreachable payload".to_vec();
        let orphan_digest = digest_bytes(&orphan_bytes);
        let client = backend.client().await.unwrap();
        let orphan_codec = "messagepack".to_owned();
        let orphan_schema = "test.orphan".to_owned();
        let orphan_compression = "none".to_owned();
        client
            .execute(
                &format!(
                    "insert into {}.payload_blobs
                         (digest, codec, schema_fingerprint, compression, encryption, size, bytes)
                         values ($1, $2, $3, $4, null, $5, $6)",
                    quote_ident(&schema)
                ),
                &[
                    &orphan_digest,
                    &orphan_codec,
                    &orphan_schema,
                    &orphan_compression,
                    &i64::try_from(orphan_bytes.len()).unwrap_or(i64::MAX),
                    &orphan_bytes,
                ],
            )
            .await
            .unwrap();

        let dry_run = backend
            .gc_payload_blobs(PayloadGarbageCollectionRequest { dry_run: true })
            .await
            .unwrap();
        assert_eq!(dry_run.deleted_blobs, 1);
        assert!(dry_run.retained_blobs >= 1);
        let collected = backend
            .gc_payload_blobs(PayloadGarbageCollectionRequest { dry_run: false })
            .await
            .unwrap();
        assert_eq!(collected.deleted_blobs, dry_run.deleted_blobs);
        let after = backend
            .gc_payload_blobs(PayloadGarbageCollectionRequest { dry_run: true })
            .await
            .unwrap();
        assert_eq!(after.deleted_blobs, 0);

        let history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(1),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        let HistoryEventData::WorkflowStarted { input, .. } = &history.events[0].data else {
            panic!("expected workflow start");
        };
        assert_eq!(crate::decode_payload::<String>(input).unwrap(), input_value);

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_cancel_commands_clean_activity_state_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres cancel command test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("cancel_commands");
        let backend =
            PostgresBackend::connect_with_config(PostgresBackendConfig::new(url).schema(schema))
                .await
                .unwrap();
        let workflow_type = WorkflowType::new("postgres.cancel-command", 1);
        let workflow_queue = crate::TaskQueue::new("postgres-cancel-command-workflows");
        let activity_queue = crate::TaskQueue::new("postgres-cancel-command-activities");
        let started = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: crate::WorkflowId::new("wf/postgres-cancel-command"),
                workflow_type: workflow_type.clone(),
                task_queue: workflow_queue.clone(),
                input: crate::encode_payload(&"cancel-command-input").unwrap(),
            })
            .await
            .unwrap();
        let run_id = started.run_id().clone();
        let workflow_claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: workflow_queue,
            registered_workflow_types: vec![workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let first_claim = backend
            .claim_workflow_task(
                WorkerId::new("postgres-cancel-command-scheduler"),
                workflow_claim_opts.clone(),
            )
            .await
            .unwrap()
            .expect("workflow task");
        let activity_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(1),
        };
        let timer_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(2),
        };
        let fingerprint = |kind: crate::CommandKind, name: &str| crate::CommandFingerprint {
            kind,
            name: name.to_owned(),
            input_digest: None,
            options_digest: "test".to_owned(),
        };
        let scheduled = crate::ActivityScheduled {
            command_id: activity_command_id.clone(),
            activity_name: crate::ActivityName::new("postgres.cancel-command-activity"),
            task_queue: activity_queue.clone(),
            retry_policy: crate::RetryPolicy::none(),
            start_to_close_timeout: Some(Duration::from_secs(30)),
            heartbeat_timeout: Some(Duration::from_secs(30)),
            input: crate::encode_payload(&"activity-input").unwrap(),
            fingerprint: fingerprint(
                crate::CommandKind::Activity,
                "postgres.cancel-command-activity",
            ),
        };
        let fire_at = TimestampMs(1);
        assert_eq!(
            backend
                .commit_workflow_task(
                    first_claim.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![
                            crate::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                                scheduled.clone(),
                            )),
                            crate::NewHistoryEvent::new(HistoryEventData::TimerStarted(
                                crate::TimerStarted {
                                    command_id: timer_command_id.clone(),
                                    fire_at,
                                    fingerprint: fingerprint(crate::CommandKind::Timer, "timer"),
                                },
                            )),
                        ],
                        schedule_activities: vec![ActivityTask::from_scheduled(&scheduled)],
                        upsert_waits: vec![crate::WaitRecord {
                            wait_id: crate::WaitId::new(format!("{}:2:timer", run_id.0)),
                            run_id: run_id.clone(),
                            command_id: timer_command_id,
                            kind: WaitKind::Timer,
                            key: "timer".to_owned(),
                            ready_at: Some(fire_at),
                        }],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(3)
            }
        );
        let activity_opts = ClaimActivityOptions {
            namespace: crate::Namespace::default(),
            task_queue: activity_queue,
            registered_activity_names: vec![crate::ActivityName::new(
                "postgres.cancel-command-activity",
            )],
            lease_duration: Duration::from_secs(30),
        };
        let activity = backend
            .claim_activity_task(
                WorkerId::new("postgres-cancel-command-activity"),
                activity_opts.clone(),
            )
            .await
            .unwrap()
            .expect("activity task");
        assert_eq!(
            activity.task.activity_id,
            ActivityId::new(&activity_command_id)
        );
        assert_eq!(
            backend
                .fire_due_timers(crate::FireDueTimersRequest {
                    namespace: crate::Namespace::default(),
                    now: fire_at,
                    limit: 10,
                })
                .await
                .unwrap(),
            FireDueTimersOutcome { fired: 1 }
        );
        let timer_claim = backend
            .claim_workflow_task(
                WorkerId::new("postgres-cancel-command-timer"),
                workflow_claim_opts,
            )
            .await
            .unwrap()
            .expect("timer wake");
        assert_eq!(timer_claim.replay_target_event_id, EventId(4));
        assert_eq!(
            backend
                .commit_workflow_task(
                    timer_claim.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(4),
                        cancel_commands: vec![activity_command_id],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(4)
            }
        );
        assert!(
            backend
                .claim_activity_task(
                    WorkerId::new("postgres-cancel-command-leftover"),
                    activity_opts,
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            backend
                .complete_activity(CompleteActivityRequest {
                    claim: activity.claim,
                    result: crate::encode_payload(&"late").unwrap(),
                })
                .await
                .unwrap(),
            CompleteActivityOutcome::AlreadyCompleted
        );

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_continue_as_new_starts_claimable_next_run_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres continue-as-new test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("continue_as_new");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema)
                .payload_storage(PayloadStorageConfig::new().inline_threshold_bytes(1)),
        )
        .await
        .unwrap();
        let workflow_id = crate::WorkflowId::new("wf/postgres-continue");
        let workflow_type = WorkflowType::new("postgres.continue", 1);
        let workflow_queue = crate::TaskQueue::new("postgres-continue-workflows");
        let started = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: workflow_id.clone(),
                workflow_type: workflow_type.clone(),
                task_queue: workflow_queue.clone(),
                input: crate::encode_payload(&"first-input").unwrap(),
            })
            .await
            .unwrap();
        let first_run_id = started.run_id().clone();
        let claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: workflow_queue,
            registered_workflow_types: vec![workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let claimed = backend
            .claim_workflow_task(WorkerId::new("postgres-continue-first"), claim_opts.clone())
            .await
            .unwrap()
            .expect("first workflow task");
        let next_input_value = "postgres-continued-input".repeat(8);
        assert_eq!(
            backend
                .commit_workflow_task(
                    claimed.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![crate::NewHistoryEvent::new(
                            HistoryEventData::WorkflowContinuedAsNew {
                                input: crate::encode_payload(&next_input_value).unwrap(),
                            },
                        )],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(2)
            }
        );

        let old_history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id: first_run_id.clone(),
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(100),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        assert_eq!(old_history.events.len(), 2);
        assert!(matches!(
            old_history.events[1].data,
            HistoryEventData::WorkflowContinuedAsNew { .. }
        ));

        let next = backend
            .claim_workflow_task(WorkerId::new("postgres-continue-next"), claim_opts)
            .await
            .unwrap()
            .expect("continued workflow task");
        assert_ne!(next.run_id, first_run_id);
        assert_eq!(next.workflow_id, workflow_id);
        assert_eq!(next.reason, WorkflowTaskReason::WorkflowStarted);
        assert_eq!(next.replay_target_event_id, EventId(1));
        let next_history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id: next.run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(1),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        let HistoryEventData::WorkflowStarted { input, .. } = &next_history.events[0].data else {
            panic!("expected continued run start");
        };
        assert_eq!(
            crate::decode_payload::<String>(input).unwrap(),
            next_input_value
        );

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_activity_map_completes_with_blob_backed_manifest_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres activity map test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("activity_map");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema)
                .payload_storage(PayloadStorageConfig::new().inline_threshold_bytes(1)),
        )
        .await
        .unwrap();
        let workflow_id = crate::WorkflowId::new("wf/postgres-activity-map");
        let workflow_type = WorkflowType::new("postgres.map", 1);
        let workflow_queue = crate::TaskQueue::new("postgres-map-workflows");
        let activity_queue = crate::TaskQueue::new("postgres-map-activities");
        let started = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id,
                workflow_type: workflow_type.clone(),
                task_queue: workflow_queue.clone(),
                input: crate::encode_payload(&"map-input").unwrap(),
            })
            .await
            .unwrap();
        let run_id = started.run_id().clone();
        let claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: workflow_queue,
            registered_workflow_types: vec![workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let claimed = backend
            .claim_workflow_task(WorkerId::new("postgres-map-scheduler"), claim_opts.clone())
            .await
            .unwrap()
            .expect("workflow task");
        let command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(1),
        };
        let input_manifest = crate::encode_activity_map_input_manifest(
            [1_u64, 2, 3]
                .into_iter()
                .map(|value| crate::encode_payload(&value).unwrap())
                .collect(),
            2,
        )
        .unwrap();
        let activity_name = crate::ActivityName::new("postgres.map-activity");
        let retry_policy = crate::RetryPolicy::none();
        let map_task = ActivityMapTask {
            map_command_id: command_id.clone(),
            activity_name: activity_name.clone(),
            task_queue: activity_queue.clone(),
            retry_policy: retry_policy.clone(),
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            input_manifest: input_manifest.clone(),
            result_manifest_name: "postgres-map-results".to_owned(),
            max_in_flight: 2,
        };
        let scheduled = crate::ActivityMapScheduled {
            command_id: command_id.clone(),
            activity_name: activity_name.clone(),
            task_queue: activity_queue.clone(),
            retry_policy,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            input_manifest: input_manifest.clone(),
            result_manifest_name: "postgres-map-results".to_owned(),
            max_in_flight: 2,
            fingerprint: crate::activity_map_fingerprint(
                activity_name.clone(),
                crate::payload_digest(&input_manifest),
                "postgres-map-results".to_owned(),
                2,
                "test".to_owned(),
            ),
        };
        assert_eq!(
            backend
                .commit_workflow_task(
                    claimed.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![crate::NewHistoryEvent::new(
                            HistoryEventData::ActivityMapScheduled(scheduled),
                        )],
                        schedule_activity_maps: vec![map_task],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(2)
            }
        );

        let activity_opts = ClaimActivityOptions {
            namespace: crate::Namespace::default(),
            task_queue: activity_queue,
            registered_activity_names: vec![activity_name],
            lease_duration: Duration::from_secs(30),
        };
        let first = backend
            .claim_activity_task(
                WorkerId::new("postgres-map-worker-1"),
                activity_opts.clone(),
            )
            .await
            .unwrap()
            .expect("first map item");
        let second = backend
            .claim_activity_task(
                WorkerId::new("postgres-map-worker-2"),
                activity_opts.clone(),
            )
            .await
            .unwrap()
            .expect("second map item");
        assert_eq!(first.task.map_item.as_ref().unwrap().item_ordinal, 0);
        assert_eq!(second.task.map_item.as_ref().unwrap().item_ordinal, 1);
        assert!(
            backend
                .claim_activity_task(
                    WorkerId::new("postgres-map-worker-hidden"),
                    activity_opts.clone(),
                )
                .await
                .unwrap()
                .is_none()
        );

        assert_eq!(
            backend
                .complete_activity(CompleteActivityRequest {
                    claim: first.claim,
                    result: crate::encode_payload(&10_u64).unwrap(),
                })
                .await
                .unwrap(),
            CompleteActivityOutcome::Completed {
                event_id: EventId(2)
            }
        );
        let third = backend
            .claim_activity_task(
                WorkerId::new("postgres-map-worker-3"),
                activity_opts.clone(),
            )
            .await
            .unwrap()
            .expect("third map item");
        assert_eq!(third.task.map_item.as_ref().unwrap().item_ordinal, 2);
        backend
            .complete_activity(CompleteActivityRequest {
                claim: third.claim,
                result: crate::encode_payload(&30_u64).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            backend
                .complete_activity(CompleteActivityRequest {
                    claim: second.claim,
                    result: crate::encode_payload(&20_u64).unwrap(),
                })
                .await
                .unwrap(),
            CompleteActivityOutcome::Completed {
                event_id: EventId(3)
            }
        );

        let ready = backend
            .claim_workflow_task(WorkerId::new("postgres-map-ready"), claim_opts)
            .await
            .unwrap()
            .expect("workflow task after map completion");
        assert_eq!(ready.reason, WorkflowTaskReason::ActivityMapCompleted);
        assert_eq!(ready.replay_target_event_id, EventId(3));
        let history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(3),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        assert_eq!(history.events.len(), 3);
        let HistoryEventData::ActivityMapCompleted(completed) = &history.events[2].data else {
            panic!("expected activity map completion");
        };
        assert_eq!(completed.item_count, 3);
        assert_eq!(completed.success_count, 3);
        assert_eq!(completed.failure_count, 0);
        let results = crate::decode_activity_map_result_refs(&completed.result_manifest)
            .unwrap()
            .into_iter()
            .map(|payload| crate::decode_payload::<u64>(&payload).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results, vec![10, 20, 30]);

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_delayed_visibility_survives_reconnect_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres delayed reconnect test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("delayed_reconnect");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone())
                .schema(schema.clone())
                .max_pool_size(2),
        )
        .await
        .unwrap();
        let workflow_type = WorkflowType::new("postgres.delayed", 1);
        let queue = crate::TaskQueue::new("postgres-delayed-workflows");
        backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: crate::WorkflowId::new("wf/postgres-delayed-reconnect"),
                workflow_type: workflow_type.clone(),
                task_queue: queue.clone(),
                input: crate::encode_payload(&"delayed").unwrap(),
            })
            .await
            .unwrap();
        let claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: queue,
            registered_workflow_types: vec![workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let claimed = backend
            .claim_workflow_task(WorkerId::new("postgres-delayed-first"), claim_opts.clone())
            .await
            .unwrap()
            .expect("workflow task");
        backend
            .release_workflow_task(
                claimed.claim,
                crate::WorkflowTaskRelease::delayed(
                    WorkflowTaskReason::CacheEvicted,
                    Duration::from_millis(75),
                ),
            )
            .await
            .unwrap();
        drop(backend);

        let restarted = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone())
                .schema(schema.clone())
                .max_pool_size(2),
        )
        .await
        .unwrap();
        let hidden = restarted
            .claim_workflow_task(WorkerId::new("postgres-delayed-hidden"), claim_opts.clone())
            .await
            .unwrap();
        assert!(hidden.is_none());
        tokio::time::sleep(Duration::from_millis(90)).await;
        let visible = restarted
            .claim_workflow_task(WorkerId::new("postgres-delayed-visible"), claim_opts)
            .await
            .unwrap()
            .expect("workflow task visible after reconnect delay");
        assert_eq!(visible.reason, WorkflowTaskReason::CacheEvicted);

        restarted.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_reconnect_preserves_history_and_operational_indexes_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres reconnect recovery test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("reconnect_recovery");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone())
                .schema(schema.clone())
                .payload_storage(PayloadStorageConfig::new().inline_threshold_bytes(1))
                .max_pool_size(4),
        )
        .await
        .unwrap();
        let workflow_type = WorkflowType::new("postgres.reconnect", 1);
        let workflow_queue = crate::TaskQueue::new("postgres-reconnect-workflows");
        let activity_queue = crate::TaskQueue::new("postgres-reconnect-activities");
        let started = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id: crate::WorkflowId::new("wf/postgres-reconnect"),
                workflow_type: workflow_type.clone(),
                task_queue: workflow_queue.clone(),
                input: crate::encode_payload(&"reconnect-input").unwrap(),
            })
            .await
            .unwrap();
        let run_id = started.run_id().clone();
        let claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: workflow_queue,
            registered_workflow_types: vec![workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let claimed = backend
            .claim_workflow_task(
                WorkerId::new("postgres-reconnect-scheduler"),
                claim_opts.clone(),
            )
            .await
            .unwrap()
            .expect("workflow task");
        let timer_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(1),
        };
        let activity_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(2),
        };
        let fire_at = TimestampMs(1);
        let activity_input = "postgres-reconnect-activity-input".repeat(8);
        let activity_scheduled = crate::ActivityScheduled {
            command_id: activity_command_id.clone(),
            activity_name: crate::ActivityName::new("postgres.reconnect-activity"),
            task_queue: activity_queue.clone(),
            retry_policy: crate::RetryPolicy::none(),
            start_to_close_timeout: Some(Duration::from_secs(30)),
            heartbeat_timeout: None,
            input: crate::encode_payload(&activity_input).unwrap(),
            fingerprint: crate::CommandFingerprint {
                kind: crate::CommandKind::Activity,
                name: "postgres.reconnect-activity".to_owned(),
                input_digest: None,
                options_digest: "test".to_owned(),
            },
        };
        assert_eq!(
            backend
                .commit_workflow_task(
                    claimed.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![
                            crate::NewHistoryEvent::new(HistoryEventData::TimerStarted(
                                crate::TimerStarted {
                                    command_id: timer_command_id.clone(),
                                    fire_at,
                                    fingerprint: crate::CommandFingerprint {
                                        kind: crate::CommandKind::Timer,
                                        name: "timer".to_owned(),
                                        input_digest: None,
                                        options_digest: "test".to_owned(),
                                    },
                                },
                            )),
                            crate::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                                activity_scheduled.clone(),
                            )),
                        ],
                        upsert_waits: vec![crate::WaitRecord {
                            wait_id: crate::WaitId::new(format!("{}:1:timer", run_id.0)),
                            run_id: run_id.clone(),
                            command_id: timer_command_id,
                            kind: WaitKind::Timer,
                            key: "timer".to_owned(),
                            ready_at: Some(fire_at),
                        }],
                        schedule_activities: vec![ActivityTask::from_scheduled(
                            &activity_scheduled,
                        )],
                        query_projection: Some(crate::encode_payload(&"reconnect-view").unwrap()),
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(3)
            }
        );
        drop(backend);

        let restarted = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone())
                .schema(schema.clone())
                .payload_storage(PayloadStorageConfig::new().inline_threshold_bytes(1))
                .max_pool_size(4),
        )
        .await
        .unwrap();
        let history = restarted
            .stream_history(crate::StreamHistoryRequest {
                run_id: run_id.clone(),
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(100),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        assert_eq!(history.events.len(), 3);
        assert!(matches!(
            history.events[1].data,
            HistoryEventData::TimerStarted(_)
        ));
        assert!(matches!(
            history.events[2].data,
            HistoryEventData::ActivityScheduled(_)
        ));

        let projection = restarted
            .query_projection(crate::QueryProjectionRequest {
                namespace: crate::Namespace::default(),
                workflow_id: crate::WorkflowId::new("wf/postgres-reconnect"),
            })
            .await
            .unwrap();
        let QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected query projection after reconnect");
        };
        assert_eq!(
            crate::decode_payload::<String>(&payload).unwrap(),
            "reconnect-view"
        );

        assert_eq!(
            restarted
                .fire_due_timers(crate::FireDueTimersRequest {
                    namespace: crate::Namespace::default(),
                    now: fire_at,
                    limit: 8,
                })
                .await
                .unwrap(),
            FireDueTimersOutcome { fired: 1 }
        );
        let activity = restarted
            .claim_activity_task(
                WorkerId::new("postgres-reconnect-activity-worker"),
                ClaimActivityOptions {
                    namespace: crate::Namespace::default(),
                    task_queue: activity_queue,
                    registered_activity_names: vec![crate::ActivityName::new(
                        "postgres.reconnect-activity",
                    )],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("activity task after reconnect");
        assert_eq!(
            crate::decode_payload::<String>(&activity.task.input).unwrap(),
            activity_input
        );

        restarted.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_concurrent_claims_are_unique_and_stale_commits_are_rejected_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres concurrent claim test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("concurrent_claims");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url)
                .schema(schema)
                .max_pool_size(8),
        )
        .await
        .unwrap();
        let workflow_type = WorkflowType::new("postgres.concurrent", 1);
        let queue = crate::TaskQueue::new("postgres-concurrent-workflows");
        for index in 0..16_u64 {
            backend
                .start_workflow(crate::StartWorkflowRequest {
                    namespace: crate::Namespace::default(),
                    workflow_id: crate::WorkflowId::new(format!("wf/postgres-concurrent-{index}")),
                    workflow_type: workflow_type.clone(),
                    task_queue: queue.clone(),
                    input: crate::encode_payload(&index).unwrap(),
                })
                .await
                .unwrap();
        }
        let claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: queue,
            registered_workflow_types: vec![workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let mut handles = Vec::new();
        for index in 0..16_u64 {
            let backend = backend.clone();
            let claim_opts = claim_opts.clone();
            handles.push(tokio::spawn(async move {
                backend
                    .claim_workflow_task(
                        WorkerId::new(format!("postgres-concurrent-worker-{index}")),
                        claim_opts,
                    )
                    .await
                    .unwrap()
                    .expect("workflow task")
            }));
        }
        let mut claims = Vec::new();
        for handle in handles {
            claims.push(handle.await.unwrap());
        }
        let unique_run_ids = claims
            .iter()
            .map(|claim| claim.run_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique_run_ids.len(), 16);
        assert!(
            backend
                .claim_workflow_task(
                    WorkerId::new("postgres-concurrent-empty"),
                    claim_opts.clone(),
                )
                .await
                .unwrap()
                .is_none()
        );

        let stale_claim = claims.remove(0);
        backend
            .release_workflow_task(
                stale_claim.claim.clone(),
                crate::WorkflowTaskRelease::immediate(WorkflowTaskReason::CacheEvicted),
            )
            .await
            .unwrap();
        let replacement = backend
            .claim_workflow_task(WorkerId::new("postgres-concurrent-replacement"), claim_opts)
            .await
            .unwrap()
            .expect("replacement workflow task");
        let stale = backend
            .commit_workflow_task(
                stale_claim.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: stale_claim.replay_target_event_id,
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(stale, Error::StaleLease));
        assert_eq!(
            backend
                .commit_workflow_task(
                    replacement.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: replacement.replay_target_event_id,
                        append_events: vec![crate::NewHistoryEvent::new(
                            HistoryEventData::WorkflowCompleted {
                                result: crate::encode_payload(&()).unwrap(),
                            },
                        )],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(2)
            }
        );

        backend.drop_schema_for_tests().await.unwrap();
    });
}

#[test]
fn postgres_activity_retry_failure_and_timeout_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres activity lifecycle test; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = test_schema("activity_lifecycle");
        let backend =
            PostgresBackend::connect_with_config(PostgresBackendConfig::new(url).schema(schema))
                .await
                .unwrap();
        let workflow_id = crate::WorkflowId::new("wf/postgres-activity-lifecycle");
        let workflow_type = WorkflowType::new("postgres.activity", 1);
        let workflow_queue = crate::TaskQueue::new("postgres-activity-workflows");
        let activity_queue = crate::TaskQueue::new("postgres-activity-tasks");
        let started = backend
            .start_workflow(crate::StartWorkflowRequest {
                namespace: crate::Namespace::default(),
                workflow_id,
                workflow_type: workflow_type.clone(),
                task_queue: workflow_queue.clone(),
                input: crate::encode_payload(&"activity-lifecycle").unwrap(),
            })
            .await
            .unwrap();
        let run_id = started.run_id().clone();
        let claim_opts = crate::ClaimWorkflowTaskOptions {
            namespace: crate::Namespace::default(),
            task_queue: workflow_queue,
            registered_workflow_types: vec![workflow_type],
            lease_duration: Duration::from_secs(30),
        };
        let claimed = backend
            .claim_workflow_task(
                WorkerId::new("postgres-activity-scheduler"),
                claim_opts.clone(),
            )
            .await
            .unwrap()
            .expect("workflow task");
        let command_fingerprint =
            |kind: crate::CommandKind, name: &str| crate::CommandFingerprint {
                kind,
                name: name.to_owned(),
                input_digest: None,
                options_digest: "test".to_owned(),
            };
        let retry_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(1),
        };
        let timeout_command_id = CommandId {
            run_id: run_id.clone(),
            seq: CommandSeq(2),
        };
        let retry_scheduled = crate::ActivityScheduled {
            command_id: retry_command_id.clone(),
            activity_name: crate::ActivityName::new("postgres.retry"),
            task_queue: activity_queue.clone(),
            retry_policy: crate::RetryPolicy::exponential().max_attempts(2),
            start_to_close_timeout: Some(Duration::from_secs(30)),
            heartbeat_timeout: None,
            input: crate::encode_payload(&"retry-input").unwrap(),
            fingerprint: command_fingerprint(crate::CommandKind::Activity, "postgres.retry"),
        };
        let timeout_scheduled = crate::ActivityScheduled {
            command_id: timeout_command_id.clone(),
            activity_name: crate::ActivityName::new("postgres.timeout"),
            task_queue: activity_queue.clone(),
            retry_policy: crate::RetryPolicy::none(),
            start_to_close_timeout: Some(Duration::from_millis(1)),
            heartbeat_timeout: None,
            input: crate::encode_payload(&"timeout-input").unwrap(),
            fingerprint: command_fingerprint(crate::CommandKind::Activity, "postgres.timeout"),
        };
        assert_eq!(
            backend
                .commit_workflow_task(
                    claimed.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![
                            crate::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                                retry_scheduled.clone(),
                            )),
                            crate::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                                timeout_scheduled.clone(),
                            )),
                        ],
                        schedule_activities: vec![
                            ActivityTask::from_scheduled(&retry_scheduled),
                            ActivityTask::from_scheduled(&timeout_scheduled),
                        ],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(3)
            }
        );

        let retry_activity_opts = ClaimActivityOptions {
            namespace: crate::Namespace::default(),
            task_queue: activity_queue.clone(),
            registered_activity_names: vec![crate::ActivityName::new("postgres.retry")],
            lease_duration: Duration::from_secs(30),
        };
        let first = backend
            .claim_activity_task(
                WorkerId::new("postgres-retry-worker-a"),
                retry_activity_opts.clone(),
            )
            .await
            .unwrap()
            .expect("retry activity first attempt");
        assert_eq!(first.task.activity_id, ActivityId::new(&retry_command_id));
        assert_eq!(first.task.attempt, 1);
        assert_eq!(
            backend
                .fail_activity(FailActivityRequest {
                    claim: first.claim.clone(),
                    failure: DurableFailure::new("test.retry", "retry me"),
                })
                .await
                .unwrap(),
            FailActivityOutcome::RetryScheduled { next_attempt: 2 }
        );
        assert!(matches!(
            backend
                .fail_activity(FailActivityRequest {
                    claim: first.claim,
                    failure: DurableFailure::new("test.retry", "stale"),
                })
                .await,
            Err(Error::StaleLease)
        ));
        let second = backend
            .claim_activity_task(
                WorkerId::new("postgres-retry-worker-b"),
                retry_activity_opts.clone(),
            )
            .await
            .unwrap()
            .expect("retry activity second attempt");
        assert_eq!(second.task.attempt, 2);
        assert_eq!(
            backend
                .fail_activity(FailActivityRequest {
                    claim: second.claim,
                    failure: DurableFailure::non_retryable("test.permanent", "permanent"),
                })
                .await
                .unwrap(),
            FailActivityOutcome::Failed {
                event_id: EventId(4)
            }
        );
        let no_retry = backend
            .claim_activity_task(
                WorkerId::new("postgres-retry-worker-c"),
                retry_activity_opts,
            )
            .await
            .unwrap();
        assert!(no_retry.is_none());
        let failed_ready = backend
            .claim_workflow_task(WorkerId::new("postgres-failed-ready"), claim_opts.clone())
            .await
            .unwrap()
            .expect("activity failure should wake workflow");
        assert_eq!(failed_ready.replay_target_event_id, EventId(4));
        assert_eq!(failed_ready.reason, WorkflowTaskReason::ActivityFailed);
        assert_eq!(
            backend
                .commit_workflow_task(
                    failed_ready.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(4),
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(4)
            }
        );

        let timeout_outcome = backend
            .timeout_due_activities(TimeoutDueActivitiesRequest {
                namespace: crate::Namespace::default(),
                now: TimestampMs(unix_epoch_millis().saturating_add(60_000)),
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(
            timeout_outcome,
            TimeoutDueActivitiesOutcome { timed_out: 1 }
        );
        let timed_out_ready = backend
            .claim_workflow_task(WorkerId::new("postgres-timeout-ready"), claim_opts)
            .await
            .unwrap()
            .expect("activity timeout should wake workflow");
        assert_eq!(timed_out_ready.replay_target_event_id, EventId(5));
        assert_eq!(timed_out_ready.reason, WorkflowTaskReason::ActivityTimedOut);

        let history = backend
            .stream_history(crate::StreamHistoryRequest {
                run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(5),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap()
            .events;
        assert_eq!(history.len(), 5);
        let HistoryEventData::ActivityFailed(failed) = &history[3].data else {
            panic!("expected activity failed event");
        };
        assert_eq!(failed.failure.message, "permanent");
        assert!(failed.failure.non_retryable);
        let HistoryEventData::ActivityTimedOut(timed_out) = &history[4].data else {
            panic!("expected activity timed out event");
        };
        assert_eq!(timed_out.command_id, timeout_command_id);

        backend.drop_schema_for_tests().await.unwrap();
    });
}
