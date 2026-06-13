use durust::{
    ActivityName, ClaimActivityOptions, ClaimWorkflowTaskOptions, Client, CommitOutcome,
    CompleteActivityRequest, DurableBackend, Error, EventId, MemoryBackend, Namespace, Registry,
    SqliteBackend, TaskQueue, Worker, WorkerId, WorkflowTaskCommit, WorkflowType,
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
                schedule_activities: Vec::new(),
                delete_waits: Vec::new(),
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
    let mut stale_claim = claimed.claim;
    stale_claim.token += 1;
    let err = backend
        .complete_activity(CompleteActivityRequest {
            claim: stale_claim,
            result: durust::encode_payload(&4u64).unwrap(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::StaleLease));
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
