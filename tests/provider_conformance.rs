use durust::{
    ActivityMapInputManifest, ActivityMapResultManifest, ActivityMapTask, ActivityName,
    ClaimActivityOptions, ClaimWorkflowTaskOptions, Client, CommitOutcome, CompleteActivityRequest,
    DurableBackend, Error, EventId, FailActivityRequest, HistoryEventData, MemoryBackend,
    Namespace, Registry, SqliteBackend, TaskQueue, Worker, WorkerId, WorkflowTaskCommit,
    WorkflowType,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    value: u64,
}

#[durust::activity(name = "conformance.echo")]
async fn echo(input: Input) -> durust::Result<u64> {
    Ok(input.value)
}

#[durust::workflow(name = "conformance.workflow", version = 1)]
async fn workflow(input: u64) -> durust::Result<u64> {
    durust::call_activity!(echo(Input { value: input })).await
}

mod default_name_handlers {
    #[durust::activity]
    pub async fn default_activity(_: ()) -> durust::Result<()> {
        Ok(())
    }

    #[durust::workflow(version = 1)]
    pub async fn default_workflow(_: ()) -> durust::Result<()> {
        Ok(())
    }
}

#[test]
fn memory_provider_passes_basic_conformance() {
    block_on(provider_conformance(MemoryBackend::new()));
}

#[test]
fn sqlite_provider_passes_basic_conformance() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("conformance.sqlite3")).unwrap();
        provider_conformance(backend).await;
    });
}

#[test]
fn registry_rejects_duplicate_handler_identities() {
    let mut registry = Registry::default();
    registry.register_workflow::<workflow>().unwrap();
    let err = registry.register_workflow::<workflow>().unwrap_err();
    assert!(matches!(err, Error::DuplicateWorkflow(_)));

    registry.register_activity::<echo>().unwrap();
    let err = registry.register_activity::<echo>().unwrap_err();
    assert!(matches!(err, Error::DuplicateActivity(_)));
}

#[test]
fn worker_builder_exposes_fallible_duplicate_registration() {
    let builder = Worker::builder(MemoryBackend::new())
        .try_register_workflow(workflow)
        .unwrap();
    let result = builder.try_register_workflow(workflow);
    assert!(matches!(result, Err(Error::DuplicateWorkflow(_))));

    let builder = Worker::builder(MemoryBackend::new())
        .try_register_activity(echo)
        .unwrap();
    let result = builder.try_register_activity(echo);
    assert!(matches!(result, Err(Error::DuplicateActivity(_))));
}

#[test]
fn registry_generates_manifest_metadata_from_handlers() {
    let mut registry = Registry::default();
    registry.register_workflow::<workflow>().unwrap();
    registry.register_activity::<echo>().unwrap();

    let manifest = registry.manifest();
    assert_eq!(manifest.workflows.len(), 1);
    assert_eq!(manifest.workflows[0].name, "conformance.workflow");
    assert_eq!(manifest.workflows[0].version, 1);
    assert!(
        manifest.workflows[0]
            .rust_path
            .ends_with("provider_conformance::workflow")
    );
    assert!(manifest.workflows[0].input_type.ends_with("u64"));
    assert!(
        manifest.workflows[0]
            .input_schema_hash
            .starts_with("sha256:")
    );

    assert_eq!(manifest.activities.len(), 1);
    assert_eq!(manifest.activities[0].name, "conformance.echo");
    assert!(
        manifest.activities[0]
            .input_type
            .ends_with("provider_conformance::Input")
    );
    assert!(
        manifest.activities[0]
            .output_schema_hash
            .starts_with("sha256:")
    );
}

#[test]
fn macros_export_manifest_metadata_for_linked_handlers() {
    let manifest = durust::exported_manifest();

    let workflow_export = manifest
        .workflows
        .iter()
        .find(|entry| entry.name == "conformance.workflow" && entry.version == 1)
        .expect("workflow export");
    assert!(
        workflow_export
            .rust_path
            .ends_with("provider_conformance::workflow")
    );
    assert_eq!(
        workflow_export.input_type,
        <workflow as durust::Workflow>::input_type_name()
    );
    assert_eq!(
        workflow_export.input_schema_hash,
        durust::type_fingerprint::<<workflow as durust::Workflow>::Input>()
    );

    let activity = manifest
        .activities
        .iter()
        .find(|activity| activity.name == "conformance.echo")
        .expect("activity export");
    assert!(activity.rust_path.ends_with("provider_conformance::echo"));
    assert_eq!(
        activity.output_type,
        <echo as durust::Activity>::output_type_name()
    );
    assert_eq!(
        activity.output_schema_hash,
        durust::type_fingerprint::<<echo as durust::Activity>::Output>()
    );
}

#[test]
fn default_durable_names_include_package_module_and_function() {
    assert_eq!(
        <default_name_handlers::default_activity as durust::Activity>::NAME,
        "durust::provider_conformance::default_name_handlers::default_activity"
    );
    assert_eq!(
        <default_name_handlers::default_workflow as durust::Workflow>::NAME,
        "durust::provider_conformance::default_name_handlers::default_workflow"
    );
}

async fn provider_conformance<B>(backend: B)
where
    B: DurableBackend,
{
    start_workflow_is_idempotent(backend.clone()).await;
    workflow_claim_filters_by_queue_and_registered_type(backend.clone()).await;
    stream_history_honors_bounds(backend.clone()).await;
    released_workflow_task_is_claimable_again(backend.clone()).await;
    delayed_released_workflow_task_is_not_claimable_until_visible(backend.clone()).await;
    query_projection_updates_atomically_and_reads_payload_refs(backend.clone()).await;
    signal_inbox_is_idempotent_ordered_and_consumed_by_commit(backend.clone()).await;
    timer_waits_fire_only_when_due_and_make_workflow_claimable(backend.clone()).await;
    activity_retry_reschedules_until_max_attempts(backend.clone()).await;
    non_retryable_activity_failure_skips_retry_and_wakes_workflow(backend.clone()).await;
    activity_timeout_retries_until_max_attempts_then_wakes_workflow(backend.clone()).await;
    cancel_commands_clear_activity_tasks(backend.clone()).await;
    activity_map_materializes_bounded_items_and_writes_result_manifest(backend.clone()).await;
    activity_map_failure_suppresses_remaining_items_and_wakes_workflow(backend.clone()).await;
    workflow_cancel_cleans_waits_activities_and_activity_maps(backend.clone()).await;
    stale_workflow_task_commit_conflicts(backend.clone()).await;
    activity_claim_filters_and_stale_completion_is_rejected(backend).await;
}

async fn workflow_claim_filters_by_queue_and_registered_type<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/claim-filter", "claim-filter-workflows", 9)
        .await
        .unwrap();

    let wrong_queue = backend
        .claim_workflow_task(
            WorkerId::new("wrong-queue-worker"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("other-workflows"),
                registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    assert!(wrong_queue.is_none());

    let wrong_type = backend
        .claim_workflow_task(
            WorkerId::new("wrong-type-worker"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("claim-filter-workflows"),
                registered_workflow_types: vec![WorkflowType::new("other.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    assert!(wrong_type.is_none());

    let matched = backend
        .claim_workflow_task(
            WorkerId::new("matched-worker"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("claim-filter-workflows"),
                registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    assert!(matched.is_some());
}

async fn start_workflow_is_idempotent<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let first = client
        .start_workflow::<workflow>("wf/idempotent", "idempotent-workflows", 1)
        .await
        .unwrap();
    let second = client
        .start_workflow::<workflow>("wf/idempotent", "idempotent-workflows", 1)
        .await
        .unwrap();
    assert_eq!(first, second);
}

async fn stream_history_honors_bounds<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/stream", "stream-workflows", 2)
        .await
        .unwrap();
    let start_only = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap();
    assert_eq!(start_only.events.len(), 1);
    assert!(!start_only.has_more);

    let mut worker = worker(backend.clone(), "stream-workflows", "stream-activities");
    worker.run_workflow_once().await.unwrap();
    let one_event = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(2),
            max_events: 1,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap();
    assert_eq!(one_event.events.len(), 1);
    assert!(one_event.has_more);
}

async fn stale_workflow_task_commit_conflicts<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/stale-commit", "stale-workflows", 3)
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("worker"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("stale-workflows"),
                registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("workflow task");
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId::ZERO,
                append_events: Vec::new(),
                upsert_waits: Vec::new(),
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
    assert_eq!(outcome, CommitOutcome::Conflict);
}

async fn released_workflow_task_is_claimable_again<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/release", "release-workflows", 5)
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("release-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("worker-a"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    backend
        .release_workflow_task(
            claimed.claim,
            durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::CacheEvicted),
        )
        .await
        .unwrap();

    let reclaimed = backend
        .claim_workflow_task(WorkerId::new("worker-b"), claim_opts)
        .await
        .unwrap();
    assert!(reclaimed.is_some());
}

async fn delayed_released_workflow_task_is_not_claimable_until_visible<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/delayed-release", "delayed-release-workflows", 5)
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("delayed-release-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("worker-a"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    backend
        .release_workflow_task(
            claimed.claim,
            durust::WorkflowTaskRelease::delayed(
                durust::WorkflowTaskReason::CacheEvicted,
                Duration::from_millis(25),
            ),
        )
        .await
        .unwrap();

    let hidden = backend
        .claim_workflow_task(WorkerId::new("worker-b"), claim_opts.clone())
        .await
        .unwrap();
    assert!(hidden.is_none());

    std::thread::sleep(Duration::from_millis(40));
    let visible = backend
        .claim_workflow_task(WorkerId::new("worker-c"), claim_opts)
        .await
        .unwrap();
    assert!(visible.is_some());
}

async fn query_projection_updates_atomically_and_reads_payload_refs<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/query-raw", "query-raw-workflows", 5)
        .await
        .unwrap();
    let req = durust::QueryProjectionRequest {
        namespace: Namespace::default(),
        workflow_id: durust::WorkflowId::new("wf/query-raw"),
    };
    assert_eq!(
        backend.query_projection(req.clone()).await.unwrap(),
        durust::QueryProjectionOutcome::NotFound
    );

    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("query-raw-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("query-raw-worker"), claim_opts)
        .await
        .unwrap()
        .expect("workflow task");
    assert_eq!(
        backend.query_projection(req.clone()).await.unwrap(),
        durust::QueryProjectionOutcome::NotFound
    );
    let stale_payload = durust::encode_payload(&"stale").unwrap();
    let conflict = backend
        .commit_workflow_task(
            claimed.claim.clone(),
            WorkflowTaskCommit {
                expected_tail_event_id: EventId::ZERO,
                append_events: Vec::new(),
                upsert_waits: Vec::new(),
                schedule_activities: Vec::new(),
                schedule_activity_maps: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: Some(stale_payload),
            },
        )
        .await
        .unwrap();
    assert_eq!(conflict, CommitOutcome::Conflict);
    assert_eq!(
        backend.query_projection(req.clone()).await.unwrap(),
        durust::QueryProjectionOutcome::NotFound
    );

    let reclaimed = backend
        .claim_workflow_task(
            WorkerId::new("query-raw-reclaimer"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("query-raw-workflows"),
                registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("workflow task after conflict");
    let blob_payload = durust::PayloadRef::Blob {
        codec: durust::CodecId::MessagePack,
        schema_fingerprint: durust::SchemaFingerprint("sha256:blob".to_owned()),
        compression: durust::CompressionId::None,
        encryption: None,
        digest: "sha256:projection".to_owned(),
        size: 128,
        uri: "memory://projection".to_owned(),
    };
    let committed = backend
        .commit_workflow_task(
            reclaimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: Vec::new(),
                upsert_waits: Vec::new(),
                schedule_activities: Vec::new(),
                schedule_activity_maps: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: Some(blob_payload.clone()),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        committed,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(1)
        }
    );
    assert_eq!(
        backend.query_projection(req).await.unwrap(),
        durust::QueryProjectionOutcome::Found {
            run_id: claimed.run_id,
            event_id: EventId(1),
            payload: blob_payload,
        }
    );
}

async fn signal_inbox_is_idempotent_ordered_and_consumed_by_commit<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/signal-inbox", "signal-inbox-workflows", 5)
        .await
        .unwrap();
    let accepted = client
        .signal_workflow("wf/signal-inbox", "ready", "signal/inbox/1", "first")
        .await
        .unwrap();
    assert_eq!(accepted, durust::SignalWorkflowOutcome::Accepted);
    let duplicate = client
        .signal_workflow("wf/signal-inbox", "ready", "signal/inbox/1", "duplicate")
        .await
        .unwrap();
    assert_eq!(duplicate, durust::SignalWorkflowOutcome::Duplicate);
    let second = client
        .signal_workflow("wf/signal-inbox", "ready", "signal/inbox/2", "second")
        .await
        .unwrap();
    assert_eq!(second, durust::SignalWorkflowOutcome::Accepted);

    let first_inbox = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id: run_id.clone(),
            signal_name: durust::SignalName::new("ready"),
        })
        .await
        .unwrap()
        .expect("first signal");
    assert_eq!(
        first_inbox.signal_id,
        durust::SignalId::new("signal/inbox/1")
    );
    assert_eq!(
        durust::decode_payload::<String>(&first_inbox.payload).unwrap(),
        "first"
    );

    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("signal-consumer"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("signal-inbox-workflows"),
                registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("workflow task");
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: Vec::new(),
                upsert_waits: Vec::new(),
                schedule_activities: Vec::new(),
                schedule_activity_maps: Vec::new(),
                consume_signals: vec![first_inbox.signal_id],
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(1)
        }
    );

    let second_inbox = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id,
            signal_name: durust::SignalName::new("ready"),
        })
        .await
        .unwrap()
        .expect("second signal");
    assert_eq!(
        second_inbox.signal_id,
        durust::SignalId::new("signal/inbox/2")
    );
}

async fn timer_waits_fire_only_when_due_and_make_workflow_claimable<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/timer-wait", "timer-workflows", 5)
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("timer-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("timer-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let now = backend.current_time().await.unwrap();
    let fire_at = durust::TimestampMs(now.0.saturating_add(50));
    let command_id = durust::command_id(&claimed.run_id, 1);
    let wait_id = durust::WaitId::new(format!("{}:{}:timer", command_id.run_id, command_id.seq.0));
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::TimerStarted(durust::TimerStarted {
                        command_id: command_id.clone(),
                        fire_at,
                        fingerprint: durust::timer_fingerprint("sleep", durust::TimestampMs(50)),
                    }),
                )],
                upsert_waits: vec![durust::WaitRecord {
                    wait_id,
                    run_id: command_id.run_id.clone(),
                    command_id: command_id.clone(),
                    kind: durust::WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(fire_at),
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
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let early = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(early.fired, 0);
    let hidden = backend
        .claim_workflow_task(WorkerId::new("timer-too-early"), claim_opts.clone())
        .await
        .unwrap();
    assert!(hidden.is_none());

    let due = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: fire_at,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(due.fired, 1);
    let duplicate = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: fire_at,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(duplicate.fired, 0);

    let ready = backend
        .claim_workflow_task(WorkerId::new("timer-ready"), claim_opts)
        .await
        .unwrap()
        .expect("timer-fired workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::TimerFired);
}

async fn activity_retry_reschedules_until_max_attempts<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/activity-retry", "retry-workflows", 5)
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("retry-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("retry-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let retry_policy = durust::RetryPolicy::exponential().max_attempts(2);
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("retry-activities"),
        retry_policy,
        start_to_close_timeout: None,
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityScheduled(scheduled.clone()),
                )],
                upsert_waits: Vec::new(),
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                schedule_activity_maps: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        )
        .await
        .unwrap();

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("retry-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(WorkerId::new("retry-worker-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(first.task.attempt, 1);
    let retried = backend
        .fail_activity(FailActivityRequest {
            claim: first.claim,
            failure: durust::DurableFailure::new("test.transient", "transient"),
        })
        .await
        .unwrap();
    assert_eq!(
        retried,
        durust::FailActivityOutcome::RetryScheduled { next_attempt: 2 }
    );
    let not_ready = backend
        .claim_workflow_task(WorkerId::new("retry-not-ready"), claim_opts.clone())
        .await
        .unwrap();
    assert!(not_ready.is_none());

    let second = backend
        .claim_activity_task(WorkerId::new("retry-worker-2"), activity_opts)
        .await
        .unwrap()
        .expect("second attempt");
    assert_eq!(second.task.attempt, 2);
    let failed = backend
        .fail_activity(FailActivityRequest {
            claim: second.claim,
            failure: durust::DurableFailure::new("test.permanent", "permanent"),
        })
        .await
        .unwrap();
    assert_eq!(
        failed,
        durust::FailActivityOutcome::Failed {
            event_id: EventId(3)
        }
    );
    let ready = backend
        .claim_workflow_task(WorkerId::new("retry-ready"), claim_opts)
        .await
        .unwrap()
        .expect("activity failed workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityFailed);

    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    assert_eq!(history.len(), 3);
    assert!(matches!(
        history[1].data,
        HistoryEventData::ActivityScheduled(_)
    ));
    let HistoryEventData::ActivityFailed(failed) = &history[2].data else {
        panic!("expected final ActivityFailed event");
    };
    assert_eq!(failed.failure.message, "permanent");
    assert!(!failed.failure.non_retryable);
}

async fn non_retryable_activity_failure_skips_retry_and_wakes_workflow<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/activity-non-retryable", "non-retryable-workflows", 5)
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("non-retryable-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("non-retryable-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let retry_policy = durust::RetryPolicy::exponential().max_attempts(5);
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("non-retryable-activities"),
        retry_policy,
        start_to_close_timeout: None,
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityScheduled(scheduled.clone()),
                )],
                upsert_waits: Vec::new(),
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                schedule_activity_maps: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        )
        .await
        .unwrap();

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("non-retryable-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(
            WorkerId::new("non-retryable-worker-1"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(first.task.attempt, 1);
    let failure = durust::DurableFailure::non_retryable("test.validation", "validation failed");
    let failed = backend
        .fail_activity(FailActivityRequest {
            claim: first.claim,
            failure: failure.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        failed,
        durust::FailActivityOutcome::Failed {
            event_id: EventId(3)
        }
    );
    let no_retry = backend
        .claim_activity_task(WorkerId::new("non-retryable-worker-2"), activity_opts)
        .await
        .unwrap();
    assert!(no_retry.is_none());

    let ready = backend
        .claim_workflow_task(WorkerId::new("non-retryable-ready"), claim_opts)
        .await
        .unwrap()
        .expect("activity failed workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityFailed);

    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::ActivityFailed(failed) = &history[2].data else {
        panic!("expected final ActivityFailed event");
    };
    assert_eq!(failed.failure, failure);
}

async fn activity_timeout_retries_until_max_attempts_then_wakes_workflow<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/activity-timeout", "timeout-workflows", 5)
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("timeout-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("timeout-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let retry_policy = durust::RetryPolicy::exponential().max_attempts(2);
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("timeout-activities"),
        retry_policy,
        start_to_close_timeout: Some(Duration::from_millis(10)),
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityScheduled(scheduled.clone()),
                )],
                upsert_waits: Vec::new(),
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                schedule_activity_maps: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        )
        .await
        .unwrap();

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("timeout-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let after_schedule = backend.current_time().await.unwrap();
    let early = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_schedule.0.saturating_add(5)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(early.timed_out, 0);

    let first = backend
        .claim_activity_task(WorkerId::new("timeout-worker-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(first.task.attempt, 1);
    let retry = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_schedule.0.saturating_add(20)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(retry.timed_out, 1);
    let not_ready = backend
        .claim_workflow_task(WorkerId::new("timeout-not-ready"), claim_opts.clone())
        .await
        .unwrap();
    assert!(not_ready.is_none());
    let stale_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim,
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_completion, Error::StaleLease));

    let second = backend
        .claim_activity_task(WorkerId::new("timeout-worker-2"), activity_opts)
        .await
        .unwrap()
        .expect("second attempt");
    assert_eq!(second.task.attempt, 2);
    let final_timeout = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_schedule.0.saturating_add(40)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(final_timeout.timed_out, 1);
    let duplicate_timeout = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_schedule.0.saturating_add(50)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(duplicate_timeout.timed_out, 0);
    let late_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        late_completion,
        durust::CompleteActivityOutcome::AlreadyCompleted
    );

    let ready = backend
        .claim_workflow_task(WorkerId::new("timeout-ready"), claim_opts)
        .await
        .unwrap()
        .expect("activity timed-out workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityTimedOut);

    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    assert_eq!(history.len(), 3);
    assert!(matches!(
        history[1].data,
        HistoryEventData::ActivityScheduled(_)
    ));
    let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
        panic!("expected final ActivityTimedOut event");
    };
    assert!(timed_out.message.contains("timed out"));
}

async fn cancel_commands_clear_activity_tasks<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/cancel-command", "cancel-command-workflows", 5)
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("cancel-command-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let activity_command = durust::command_id(&run_id, 1);
    let timer_command = durust::command_id(&run_id, 2);
    let activity_input = durust::encode_payload(&Input { value: 5 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: activity_command.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("activities"),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        input: activity_input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&activity_input),
            "sha256:cancel-command-options".to_owned(),
        ),
    };
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("cancel-command-scheduler"),
            claim_opts.clone(),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![
                    durust::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                        scheduled.clone(),
                    )),
                    durust::NewHistoryEvent::new(HistoryEventData::TimerStarted(
                        durust::TimerStarted {
                            command_id: timer_command.clone(),
                            fire_at: durust::TimestampMs(10),
                            fingerprint: durust::timer_fingerprint(
                                "sleep",
                                durust::TimestampMs(10),
                            ),
                        },
                    )),
                ],
                upsert_waits: vec![durust::WaitRecord {
                    wait_id: durust::WaitId::new(format!(
                        "{}:{}:timer",
                        timer_command.run_id, timer_command.seq.0
                    )),
                    run_id: timer_command.run_id.clone(),
                    command_id: timer_command.clone(),
                    kind: durust::WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(durust::TimestampMs(10)),
                }],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                schedule_activity_maps: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        )
        .await
        .unwrap();
    assert!(matches!(outcome, CommitOutcome::Committed { .. }));

    let claimed_activity = backend
        .claim_activity_task(
            WorkerId::new("cancel-command-activity"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("activities"),
                registered_activity_names: vec![ActivityName::new("conformance.echo")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("activity task");
    let fired = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(10),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(fired.fired, 1);

    let claimed = backend
        .claim_workflow_task(WorkerId::new("cancel-command-selector"), claim_opts)
        .await
        .unwrap()
        .expect("timer-ready workflow task");
    assert_eq!(claimed.replay_target_event_id, EventId(4));
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(4),
                append_events: Vec::new(),
                upsert_waits: Vec::new(),
                schedule_activities: Vec::new(),
                schedule_activity_maps: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: vec![activity_command],
                query_projection: None,
            },
        )
        .await
        .unwrap();
    assert!(matches!(outcome, CommitOutcome::Committed { .. }));

    let late_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: claimed_activity.claim,
            result: durust::encode_payload(&5_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        late_completion,
        durust::CompleteActivityOutcome::AlreadyCompleted
    );
}

async fn activity_map_materializes_bounded_items_and_writes_result_manifest<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/activity-map", "map-workflows", 5)
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("map-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("map-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        [1_u64, 2, 3]
            .into_iter()
            .map(|value| durust::encode_payload(&Input { value }).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let decoded_input_manifest: ActivityMapInputManifest =
        durust::decode_payload(&input_manifest).unwrap();
    assert_eq!(decoded_input_manifest.item_count, 3);
    assert_eq!(decoded_input_manifest.page_lengths, vec![2, 1]);
    assert_eq!(decoded_input_manifest.pages.len(), 2);
    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new("map-activities");
    let retry_policy = durust::RetryPolicy::none();
    let map_task = ActivityMapTask {
        map_command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "mapped".to_owned(),
        max_in_flight: 2,
    };
    let fingerprint = durust::activity_map_fingerprint(
        activity_name.clone(),
        durust::payload_digest(&input_manifest),
        "mapped".to_owned(),
        2,
        "sha256:test-options".to_owned(),
    );
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityMapScheduled(durust::ActivityMapScheduled {
                        command_id: command_id.clone(),
                        activity_name,
                        task_queue,
                        retry_policy,
                        start_to_close_timeout: None,
                        input_manifest: input_manifest.clone(),
                        result_manifest_name: "mapped".to_owned(),
                        max_in_flight: 2,
                        fingerprint,
                    }),
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
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("map-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(WorkerId::new("mapper-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first map item");
    let second = backend
        .claim_activity_task(WorkerId::new("mapper-2"), activity_opts.clone())
        .await
        .unwrap()
        .expect("second map item");
    let hidden_by_max_in_flight = backend
        .claim_activity_task(WorkerId::new("mapper-3"), activity_opts.clone())
        .await
        .unwrap();
    assert!(hidden_by_max_in_flight.is_none());

    assert_map_item(&first.task, 0, 1);
    assert_map_item(&second.task, 1, 2);
    let non_terminal = backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim.clone(),
            result: durust::encode_payload(&10_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        non_terminal,
        durust::CompleteActivityOutcome::Completed {
            event_id: EventId(2)
        }
    );

    let third = backend
        .claim_activity_task(WorkerId::new("mapper-3"), activity_opts.clone())
        .await
        .unwrap()
        .expect("third map item after one completion");
    assert_map_item(&third.task, 2, 3);

    backend
        .complete_activity(CompleteActivityRequest {
            claim: third.claim.clone(),
            result: durust::encode_payload(&30_u64).unwrap(),
        })
        .await
        .unwrap();
    let final_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim.clone(),
            result: durust::encode_payload(&20_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        final_completion,
        durust::CompleteActivityOutcome::Completed {
            event_id: EventId(3)
        }
    );
    let duplicate = backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&20_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(duplicate, durust::CompleteActivityOutcome::AlreadyCompleted);

    let no_leftover_items = backend
        .claim_activity_task(WorkerId::new("mapper-leftover"), activity_opts)
        .await
        .unwrap();
    assert!(no_leftover_items.is_none());

    let ready = backend
        .claim_workflow_task(WorkerId::new("map-ready"), claim_opts)
        .await
        .unwrap()
        .expect("map-completed workflow task");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ActivityMapCompleted
    );

    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    assert_eq!(history.len(), 3);
    assert!(matches!(
        history[1].data,
        HistoryEventData::ActivityMapScheduled(_)
    ));
    let HistoryEventData::ActivityMapCompleted(completed) = &history[2].data else {
        panic!("expected compact ActivityMapCompleted event");
    };
    assert_eq!(completed.item_count, 3);
    assert_eq!(completed.success_count, 3);
    assert_eq!(completed.failure_count, 0);
    let manifest: ActivityMapResultManifest =
        durust::decode_payload(&completed.result_manifest).unwrap();
    assert_eq!(manifest.name, "mapped");
    assert_eq!(manifest.item_count, 3);
    assert_eq!(manifest.page_lengths, vec![2, 1]);
    assert_eq!(manifest.pages.len(), 2);
    let result_refs = durust::decode_activity_map_result_refs(&completed.result_manifest).unwrap();
    let values = result_refs
        .iter()
        .map(|payload| durust::decode_payload::<u64>(payload).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values, vec![10, 20, 30]);
}

async fn activity_map_failure_suppresses_remaining_items_and_wakes_workflow<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/activity-map-failure", "map-failure-workflows", 5)
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("map-failure-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("map-failure-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        [1_u64, 2, 3]
            .into_iter()
            .map(|value| durust::encode_payload(&Input { value }).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new("map-failure-activities");
    let retry_policy = durust::RetryPolicy::exponential().max_attempts(2);
    let map_task = ActivityMapTask {
        map_command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "mapped".to_owned(),
        max_in_flight: 2,
    };
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityMapScheduled(durust::ActivityMapScheduled {
                        command_id: command_id.clone(),
                        activity_name,
                        task_queue,
                        retry_policy,
                        start_to_close_timeout: None,
                        input_manifest: input_manifest.clone(),
                        result_manifest_name: "mapped".to_owned(),
                        max_in_flight: 2,
                        fingerprint: durust::activity_map_fingerprint(
                            ActivityName::new("conformance.echo"),
                            durust::payload_digest(&input_manifest),
                            "mapped".to_owned(),
                            2,
                            "sha256:test-options".to_owned(),
                        ),
                    }),
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
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("map-failure-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(WorkerId::new("failing-mapper-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first map item");
    let second = backend
        .claim_activity_task(WorkerId::new("failing-mapper-2"), activity_opts.clone())
        .await
        .unwrap()
        .expect("second map item");

    let retried = backend
        .fail_activity(FailActivityRequest {
            claim: first.claim,
            failure: durust::DurableFailure::new(
                "test.map_transient",
                "transient map item failure",
            ),
        })
        .await
        .unwrap();
    assert_eq!(
        retried,
        durust::FailActivityOutcome::RetryScheduled { next_attempt: 2 }
    );
    let not_ready = backend
        .claim_workflow_task(WorkerId::new("map-retry-not-ready"), claim_opts.clone())
        .await
        .unwrap();
    assert!(not_ready.is_none());

    let retry = backend
        .claim_activity_task(WorkerId::new("failing-mapper-retry"), activity_opts.clone())
        .await
        .unwrap()
        .expect("retried map item");
    assert_map_item(&retry.task, 0, 1);
    assert_eq!(retry.task.attempt, 2);
    let failed = backend
        .fail_activity(FailActivityRequest {
            claim: retry.claim,
            failure: durust::DurableFailure::new("test.map_failed", "map item failed"),
        })
        .await
        .unwrap();
    assert_eq!(
        failed,
        durust::FailActivityOutcome::Failed {
            event_id: EventId(3)
        }
    );

    let stale_in_flight_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&20_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        stale_in_flight_completion,
        durust::CompleteActivityOutcome::AlreadyCompleted
    );
    let no_leftover_items = backend
        .claim_activity_task(WorkerId::new("failing-mapper-leftover"), activity_opts)
        .await
        .unwrap();
    assert!(no_leftover_items.is_none());

    let ready = backend
        .claim_workflow_task(WorkerId::new("map-failed-ready"), claim_opts)
        .await
        .unwrap()
        .expect("map-failed workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityMapFailed);

    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    assert_eq!(history.len(), 3);
    let HistoryEventData::ActivityMapFailed(failed) = &history[2].data else {
        panic!("expected compact ActivityMapFailed event");
    };
    assert_eq!(failed.failure.message, "map item failed");
}

async fn workflow_cancel_cleans_waits_activities_and_activity_maps<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/cancel-cleanup", "cancel-workflows", 5)
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("cancel-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("cancel-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");

    let now = backend.current_time().await.unwrap();
    let fire_at = durust::TimestampMs(now.0.saturating_add(50));
    let timer_command = durust::command_id(&run_id, 1);
    let activity_command = durust::command_id(&run_id, 2);
    let map_command = durust::command_id(&run_id, 3);
    let activity_input = durust::encode_payload(&Input { value: 7 }).unwrap();
    let retry_policy = durust::RetryPolicy::none();
    let scheduled_activity = durust::ActivityScheduled {
        command_id: activity_command.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("cancel-activities"),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        input: activity_input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&activity_input),
            "sha256:test-options".to_owned(),
        ),
    };
    let input_manifest = durust::encode_activity_map_input_manifest(
        [1_u64, 2]
            .into_iter()
            .map(|value| durust::encode_payload(&Input { value }).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let map_task = ActivityMapTask {
        map_command_id: map_command.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("cancel-activities"),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "cancelled".to_owned(),
        max_in_flight: 2,
    };
    let wait_id = durust::WaitId::new(format!(
        "{}:{}:timer",
        timer_command.run_id, timer_command.seq.0
    ));
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![
                    durust::NewHistoryEvent::new(HistoryEventData::TimerStarted(
                        durust::TimerStarted {
                            command_id: timer_command.clone(),
                            fire_at,
                            fingerprint: durust::timer_fingerprint(
                                "sleep",
                                durust::TimestampMs(50),
                            ),
                        },
                    )),
                    durust::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                        scheduled_activity.clone(),
                    )),
                    durust::NewHistoryEvent::new(HistoryEventData::ActivityMapScheduled(
                        durust::ActivityMapScheduled {
                            command_id: map_command.clone(),
                            activity_name: ActivityName::new("conformance.echo"),
                            task_queue: TaskQueue::new("cancel-activities"),
                            retry_policy,
                            start_to_close_timeout: None,
                            input_manifest: input_manifest.clone(),
                            result_manifest_name: "cancelled".to_owned(),
                            max_in_flight: 2,
                            fingerprint: durust::activity_map_fingerprint(
                                ActivityName::new("conformance.echo"),
                                durust::payload_digest(&input_manifest),
                                "cancelled".to_owned(),
                                2,
                                "sha256:test-options".to_owned(),
                            ),
                        },
                    )),
                ],
                upsert_waits: vec![durust::WaitRecord {
                    wait_id,
                    run_id: run_id.clone(),
                    command_id: timer_command,
                    kind: durust::WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(fire_at),
                }],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(
                    &scheduled_activity,
                )],
                schedule_activity_maps: vec![map_task],
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(4)
        }
    );

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("cancel-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let ordinary = backend
        .claim_activity_task(
            WorkerId::new("cancel-activity-worker"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("ordinary activity");
    assert!(ordinary.task.map_item.is_none());
    let map_item = backend
        .claim_activity_task(WorkerId::new("cancel-map-worker"), activity_opts.clone())
        .await
        .unwrap()
        .expect("map activity");
    assert_map_item(&map_item.task, 0, 1);

    let cancelled = client
        .cancel_workflow("wf/cancel-cleanup", "operator cancelled")
        .await
        .unwrap();
    assert_eq!(
        cancelled,
        durust::CancelWorkflowOutcome::Cancelled {
            run_id: run_id.clone(),
            event_id: EventId(5)
        }
    );
    let duplicate_cancel = client
        .cancel_workflow("wf/cancel-cleanup", "duplicate")
        .await
        .unwrap();
    assert_eq!(
        duplicate_cancel,
        durust::CancelWorkflowOutcome::AlreadyTerminal {
            run_id: run_id.clone()
        }
    );

    let workflow_after_cancel = backend
        .claim_workflow_task(WorkerId::new("cancel-workflow-claim"), claim_opts)
        .await
        .unwrap();
    assert!(workflow_after_cancel.is_none());
    let timer_after_cancel = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: fire_at,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(timer_after_cancel.fired, 0);
    let activity_after_cancel = backend
        .claim_activity_task(WorkerId::new("cancel-leftover-worker"), activity_opts)
        .await
        .unwrap();
    assert!(activity_after_cancel.is_none());

    let late_ordinary_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: ordinary.claim,
            result: durust::encode_payload(&7_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        late_ordinary_completion,
        durust::CompleteActivityOutcome::AlreadyCompleted
    );
    let late_map_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: map_item.claim,
            result: durust::encode_payload(&2_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        late_map_completion,
        durust::CompleteActivityOutcome::AlreadyCompleted
    );
    let signal_after_cancel = client
        .signal_workflow("wf/cancel-cleanup", "ready", "signal/cancelled", "ignored")
        .await;
    assert!(matches!(signal_after_cancel, Err(Error::TerminalWorkflow)));

    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    assert_eq!(history.len(), 5);
    assert!(matches!(history[1].data, HistoryEventData::TimerStarted(_)));
    assert!(matches!(
        history[2].data,
        HistoryEventData::ActivityScheduled(_)
    ));
    assert!(matches!(
        history[3].data,
        HistoryEventData::ActivityMapScheduled(_)
    ));
    assert!(matches!(
        history[4].data,
        HistoryEventData::WorkflowCancelled { .. }
    ));
    assert!(!history.iter().any(|event| matches!(
        event.data,
        HistoryEventData::TimerFired(_)
            | HistoryEventData::ActivityCompleted(_)
            | HistoryEventData::ActivityMapCompleted(_)
            | HistoryEventData::ActivityMapFailed(_)
            | HistoryEventData::WorkflowFailed { .. }
    )));
}

async fn activity_claim_filters_and_stale_completion_is_rejected<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/activity-filter", "activity-workflows", 4)
        .await
        .unwrap();
    let mut workflow_worker = worker(backend.clone(), "activity-workflows", "activity-activities");
    workflow_worker.run_workflow_once().await.unwrap();

    let unmatched = backend
        .claim_activity_task(
            WorkerId::new("wrong-activity-worker"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("activity-activities"),
                registered_activity_names: vec![ActivityName::new("other.activity")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    assert!(unmatched.is_none());

    let claimed = backend
        .claim_activity_task(
            WorkerId::new("activity-worker"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("activity-activities"),
                registered_activity_names: vec![ActivityName::new("conformance.echo")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("activity task");
    let mut stale_claim = claimed.claim.clone();
    stale_claim.token += 1;
    let err = backend
        .complete_activity(CompleteActivityRequest {
            claim: stale_claim,
            result: durust::encode_payload(&4u64).unwrap(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::StaleLease));

    let completed = backend
        .complete_activity(CompleteActivityRequest {
            claim: claimed.claim.clone(),
            result: durust::encode_payload(&4u64).unwrap(),
        })
        .await
        .unwrap();
    assert!(matches!(
        completed,
        durust::CompleteActivityOutcome::Completed { .. }
    ));
    let duplicate = backend
        .complete_activity(CompleteActivityRequest {
            claim: claimed.claim,
            result: durust::encode_payload(&4u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(duplicate, durust::CompleteActivityOutcome::AlreadyCompleted);
}

fn worker<B>(backend: B, workflow_queue: &str, activity_queue: &str) -> Worker<B>
where
    B: DurableBackend,
{
    Worker::builder(backend)
        .workflow_task_queue(workflow_queue)
        .activity_task_queue(activity_queue)
        .register_workflow(workflow)
        .register_activity(echo)
        .build()
}

fn assert_map_item(task: &durust::ActivityTask, item_ordinal: u64, expected_input: u64) {
    let map_item = task.map_item.as_ref().expect("map item metadata");
    assert_eq!(map_item.item_ordinal, item_ordinal);
    assert_eq!(
        durust::decode_payload::<Input>(&task.input).unwrap().value,
        expected_input
    );
}
