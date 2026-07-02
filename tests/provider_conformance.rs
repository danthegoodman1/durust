use durust::{
    ActivityMapInputManifest, ActivityMapResultManifest, ActivityMapTask, ActivityName,
    ChildWorkflowMapTask, ClaimActivityOptions, ClaimActivityTasksOptions,
    ClaimWorkflowTaskOptions, ClaimWorkflowTasksOptions, Client, CommitOutcome,
    CompleteActivityRequest, CompleteActivityTasksRequest, DurableBackend, Error, EventId,
    FailActivityRequest, HistoryEventData, MemoryBackend, Namespace, NewHistoryEvent,
    PayloadBackend, PayloadBlobStore, PostgresBackend, PostgresBackendConfig, Registry,
    SqliteBackend, TaskQueue, Worker, WorkerId, WorkflowTaskCommit, WorkflowTaskCommitBatch,
    WorkflowTaskCommitInput, WorkflowType,
};
use futures::executor::block_on;
use futures::future::{BoxFuture, ready};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    value: u64,
}

fn input(value: u64) -> Input {
    Input { value }
}

fn postgres_url_from_env() -> Option<String> {
    env::var("DURUST_POSTGRES_URL").ok()
}

fn postgres_test_schema(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("durust_{prefix}_{}_{}", std::process::id(), millis)
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

#[durust::activity(name = "conformance.echo")]
async fn echo(input: Input) -> durust::Result<u64> {
    Ok(input.value)
}

#[durust::workflow(name = "conformance.workflow", version = 1)]
async fn workflow(input: Input) -> durust::Result<u64> {
    durust::call_activity!(echo(Input { value: input.value })).await
}

#[durust::workflow(name = "conformance.signal-race", version = 1)]
async fn signal_race_workflow(_: Input) -> durust::Result<String> {
    durust::signal::<String>("go").await
}

mod default_name_handlers {
    #[durust::activity]
    pub async fn default_activity(_: DefaultInput) -> durust::Result<()> {
        Ok(())
    }

    #[durust::workflow(version = 1)]
    pub async fn default_workflow(_: DefaultInput) -> durust::Result<()> {
        Ok(())
    }

    #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
    pub struct DefaultInput {}
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
fn postgres_provider_passes_basic_conformance_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres provider conformance; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = postgres_test_schema("conformance");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
        )
        .await
        .unwrap();
        provider_conformance(backend).await;
        drop_postgres_schema(&url, &schema).await;
    });
}

#[test]
fn sqlite_activity_heartbeat_deadline_persists_across_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("heartbeat-reopen.sqlite3");
        let backend = SqliteBackend::open(&path).unwrap();
        let (run_id, claim_opts, activity_opts) = schedule_heartbeat_activity(
            backend.clone(),
            "wf/sqlite-heartbeat-reopen",
            "sqlite-heartbeat-reopen-workflows",
            "sqlite-heartbeat-reopen-activities",
            durust::RetryPolicy::exponential().max_attempts(1),
        )
        .await;

        let activity = backend
            .claim_activity_task(WorkerId::new("sqlite-heartbeat-worker"), activity_opts)
            .await
            .unwrap()
            .expect("heartbeat activity");
        assert_eq!(activity.task.attempt, 1);
        let claimed_at = backend.current_time().await.unwrap();
        drop(backend);

        let reopened = SqliteBackend::open(&path).unwrap();
        let outcome = reopened
            .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
                namespace: Namespace::default(),
                now: durust::TimestampMs(claimed_at.0.saturating_add(500)),
                limit: 16,
            })
            .await
            .unwrap();
        assert_eq!(outcome.timed_out, 1);

        let ready = reopened
            .claim_workflow_task(WorkerId::new("sqlite-heartbeat-ready"), claim_opts)
            .await
            .unwrap()
            .expect("workflow task after heartbeat timeout");
        assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityTimedOut);

        let history = reopened
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
        let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
            panic!("expected ActivityTimedOut event after reopen");
        };
        assert!(timed_out.message.contains("missed heartbeat"));
    });
}

#[test]
fn memory_workflow_lease_expiry_reclaims_and_fences_stale_holder() {
    block_on(async {
        let backend = MemoryBackend::new();
        let advance_backend = backend.clone();
        workflow_lease_expiry_reclaims_and_fences_stale_holder(
            backend,
            "wf/memory-lease-expiry",
            "memory-lease-expiry-workflows",
            Duration::from_secs(30),
            |backend| async move { backend },
            move || async move {
                advance_backend.advance_time(Duration::from_secs(31));
            },
        )
        .await;
    });
}

#[test]
fn sqlite_workflow_lease_expiry_reclaims_and_fences_stale_holder_across_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease-expiry.sqlite3");
        let backend = SqliteBackend::open(&path).unwrap();
        workflow_lease_expiry_reclaims_and_fences_stale_holder(
            backend,
            "wf/sqlite-lease-expiry",
            "sqlite-lease-expiry-workflows",
            Duration::from_millis(50),
            move |backend| async move {
                drop(backend);
                SqliteBackend::open(&path).unwrap()
            },
            || async { std::thread::sleep(Duration::from_millis(120)) },
        )
        .await;
    });
}

#[test]
fn postgres_workflow_lease_expiry_reclaims_and_fences_stale_holder_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres lease expiry conformance; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = postgres_test_schema("lease_expiry");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
        )
        .await
        .unwrap();
        workflow_lease_expiry_reclaims_and_fences_stale_holder(
            backend,
            "wf/postgres-lease-expiry",
            "postgres-lease-expiry-workflows",
            Duration::from_millis(50),
            |backend| async move { backend },
            || async { tokio::time::sleep(Duration::from_millis(120)).await },
        )
        .await;
        drop_postgres_schema(&url, &schema).await;
    });
}

#[test]
fn memory_delayed_released_workflow_task_visibility_follows_virtual_clock() {
    block_on(async {
        let backend = MemoryBackend::new();
        let advance_backend = backend.clone();
        delayed_released_workflow_task_is_not_claimable_until_visible(
            backend,
            "wf/memory-delayed-release",
            "memory-delayed-release-workflows",
            move || async move {
                advance_backend.advance_time(Duration::from_millis(40));
            },
        )
        .await;
    });
}

#[test]
fn sqlite_delayed_released_workflow_task_is_not_claimable_until_visible() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("delayed-release.sqlite3")).unwrap();
        delayed_released_workflow_task_is_not_claimable_until_visible(
            backend,
            "wf/sqlite-delayed-release",
            "sqlite-delayed-release-workflows",
            || async { std::thread::sleep(Duration::from_millis(40)) },
        )
        .await;
    });
}

#[test]
fn postgres_delayed_released_workflow_task_is_not_claimable_until_visible_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres delayed release conformance; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = postgres_test_schema("delayed_release");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
        )
        .await
        .unwrap();
        delayed_released_workflow_task_is_not_claimable_until_visible(
            backend,
            "wf/postgres-delayed-release",
            "postgres-delayed-release-workflows",
            || async { tokio::time::sleep(Duration::from_millis(40)).await },
        )
        .await;
        drop_postgres_schema(&url, &schema).await;
    });
}

#[test]
fn sqlite_delayed_workflow_task_visibility_persists_across_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delayed-visibility.sqlite3");
        let backend = SqliteBackend::open(&path).unwrap();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<workflow>(
                "wf/sqlite-delayed-visibility",
                "sqlite-delayed-visibility-workflows",
                input(5),
            )
            .await
            .unwrap();
        let claim_opts = ClaimWorkflowTaskOptions {
            namespace: Namespace::default(),
            task_queue: TaskQueue::new("sqlite-delayed-visibility-workflows"),
            registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
            lease_duration: Duration::from_secs(30),
        };
        let claimed = backend
            .claim_workflow_task(WorkerId::new("sqlite-delayed-worker-a"), claim_opts.clone())
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
        drop(backend);

        let reopened = SqliteBackend::open(&path).unwrap();
        let hidden = reopened
            .claim_workflow_task(WorkerId::new("sqlite-delayed-worker-b"), claim_opts.clone())
            .await
            .unwrap();
        assert!(hidden.is_none());

        std::thread::sleep(Duration::from_millis(40));
        let visible = reopened
            .claim_workflow_task(WorkerId::new("sqlite-delayed-worker-c"), claim_opts)
            .await
            .unwrap();
        assert!(visible.is_some());
    });
}

#[test]
fn memory_provider_offloads_large_payloads_and_hydrates_public_apis() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/memory-payload-offload",
            "memory-payload-workflows",
            "memory-payload-activities",
        )
        .await;
        payload_offload_child_workflow_round_trip(backend.clone(), "memory").await;
        payload_offload_child_workflow_map_round_trip(backend.clone(), "memory").await;
        payload_offload_activity_map_round_trip(backend.clone(), "memory").await;
        let _ = payload_gc_removes_unreachable_projection_blob(backend.clone(), "memory").await;
        assert!(backend.payload_blob_count() >= 4);
    });
}

#[test]
fn memory_provider_replay_stream_keeps_large_payloads_lazy_until_explicit_hydration() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let run_id = start_large_payload_workflow(
            backend.clone(),
            "wf/memory-lazy-replay-payload",
            "memory-lazy-replay-workflows",
        )
        .await;
        assert_replay_stream_payload_hydrates_explicitly(backend, run_id).await;
    });
}

#[test]
fn memory_provider_keeps_side_effect_marker_inline_when_threshold_would_offload() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        assert_side_effect_marker_stays_inline_for_replay_stream(
            backend.clone(),
            "wf/memory-side-effect-inline",
            "memory-side-effect-workflows",
        )
        .await;
        assert_eq!(backend.payload_blob_count(), 0);
    });
}

#[test]
fn payload_backend_offloads_large_payloads_and_hydrates_memory_provider_apis() {
    block_on(async {
        let blob_store = durust::MemoryBlobStore::new();
        let backend = PayloadBackend::with_payload_storage(
            MemoryBackend::new(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/payload-backend-memory-offload",
            "payload-backend-memory-workflows",
            "payload-backend-memory-activities",
        )
        .await;
        payload_offload_child_workflow_round_trip(backend.clone(), "payload-backend-memory").await;
        payload_offload_child_workflow_map_round_trip(backend.clone(), "payload-backend-memory")
            .await;
        payload_offload_activity_map_round_trip(backend.clone(), "payload-backend-memory").await;
        let _ =
            payload_gc_removes_unreachable_projection_blob(backend, "payload-backend-memory").await;
        assert!(blob_store.payload_blob_count() >= 8);
    });
}

#[test]
fn sqlite_provider_offloads_large_payloads_and_hydrates_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload-offload.sqlite3");
        let config = durust::PayloadStorageConfig::new().inline_threshold_bytes(1);
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        let run_id = payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/sqlite-payload-offload",
            "sqlite-payload-workflows",
            "sqlite-payload-activities",
        )
        .await;
        payload_offload_child_workflow_round_trip(backend.clone(), "sqlite").await;
        payload_offload_child_workflow_map_round_trip(backend.clone(), "sqlite").await;
        payload_offload_activity_map_round_trip(backend.clone(), "sqlite").await;
        let (gc_workflow_id, gc_projection) =
            payload_gc_removes_unreachable_projection_blob(backend.clone(), "sqlite").await;
        let blob_count = backend.payload_blob_count().unwrap();
        assert!(blob_count >= 12);
        drop(backend);

        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        assert_eq!(reopened.payload_blob_count().unwrap(), blob_count);
        let projection = reopened
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(gc_workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected retained projection after reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            gc_projection
        );
        let history = reopened
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
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected hydrated workflow start");
        };
        assert_eq!(
            durust::decode_payload::<String>(input).unwrap(),
            large_payload("workflow-input")
        );
    });
}

#[test]
fn sqlite_provider_replay_stream_keeps_large_payloads_lazy_until_explicit_hydration_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lazy-replay-payload.sqlite3");
        let config = durust::PayloadStorageConfig::new().inline_threshold_bytes(1);
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        let run_id = start_large_payload_workflow(
            backend.clone(),
            "wf/sqlite-lazy-replay-payload",
            "sqlite-lazy-replay-workflows",
        )
        .await;
        drop(backend);

        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        assert_replay_stream_payload_hydrates_explicitly(reopened, run_id).await;
    });
}

#[test]
fn sqlite_provider_keeps_side_effect_marker_inline_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("side-effect-inline.sqlite3");
        let config = durust::PayloadStorageConfig::new().inline_threshold_bytes(1);
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        assert_side_effect_marker_stays_inline_for_replay_stream(
            backend.clone(),
            "wf/sqlite-side-effect-inline",
            "sqlite-side-effect-workflows",
        )
        .await;
        assert_eq!(backend.payload_blob_count().unwrap(), 0);
        drop(backend);

        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        assert_side_effect_marker_stays_inline_in_existing_history(
            reopened,
            "wf/sqlite-side-effect-inline",
        )
        .await;
    });
}

#[test]
fn payload_backend_wraps_sqlite_and_hydrates_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload-wrapper.sqlite3");
        let blob_store = durust::MemoryBlobStore::new();
        let config = durust::PayloadStorageConfig::new().inline_threshold_bytes(1);
        let backend = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store.clone(),
            config.clone(),
        );
        let run_id = payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/payload-backend-sqlite-offload",
            "payload-backend-sqlite-workflows",
            "payload-backend-sqlite-activities",
        )
        .await;
        payload_offload_child_workflow_map_round_trip(backend.clone(), "payload-backend-sqlite")
            .await;
        payload_offload_activity_map_round_trip(backend.clone(), "payload-backend-sqlite").await;
        let (gc_workflow_id, gc_projection) =
            payload_gc_removes_unreachable_projection_blob(backend, "payload-backend-sqlite").await;
        assert!(blob_store.payload_blob_count() >= 8);

        let reopened = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store,
            config,
        );
        let history = reopened
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
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected hydrated workflow start after wrapper reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(input).unwrap(),
            large_payload("workflow-input")
        );
        let projection = reopened
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(gc_workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected retained wrapper projection after reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            gc_projection
        );
    });
}

#[test]
fn payload_backend_over_sqlite_passes_garage_s3_conformance_when_configured() {
    block_on_tokio(async {
        let Some(garage) = garage_config_from_env() else {
            eprintln!("skipping Garage S3 conformance; set DURUST_GARAGE_* env vars");
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload-wrapper-garage.sqlite3");
        let blob_store = durust::S3BlobStore::garage(garage).unwrap();
        wait_for_blob_store(&blob_store).await;
        let config = durust::PayloadStorageConfig::new().inline_threshold_bytes(1);
        let backend = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store.clone(),
            config.clone(),
        );
        let run_id = payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/payload-backend-garage-offload",
            "payload-backend-garage-workflows",
            "payload-backend-garage-activities",
        )
        .await;
        payload_offload_child_workflow_round_trip(backend.clone(), "payload-backend-garage").await;
        payload_offload_child_workflow_map_round_trip(backend.clone(), "payload-backend-garage")
            .await;
        payload_offload_activity_map_round_trip(backend.clone(), "payload-backend-garage").await;
        let (gc_workflow_id, gc_projection) =
            payload_gc_removes_unreachable_projection_blob(backend, "payload-backend-garage").await;
        let external_blobs = durust::PayloadBlobStore::list_payload_blobs(&blob_store)
            .await
            .unwrap();
        assert!(external_blobs.len() >= 8);

        let reopened = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store,
            config,
        );
        let history = reopened
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
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected hydrated workflow start after Garage wrapper reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(input).unwrap(),
            large_payload("workflow-input")
        );
        let projection = reopened
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(gc_workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected retained Garage projection after reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            gc_projection
        );
    });
}

#[test]
fn payload_backend_s3_upload_failure_does_not_commit_missing_payload_ref() {
    block_on_tokio(async {
        let inner = MemoryBackend::new();
        let blob_store = durust::S3BlobStore::garage(durust::S3BlobStoreConfig {
            bucket: "durust-payloads".to_owned(),
            endpoint: "http://127.0.0.1:9".to_owned(),
            region: "garage".to_owned(),
            prefix: "payloads".to_owned(),
            access_key_id: "GK0123456789abcdef0123456789abcdef".to_owned(),
            secret_access_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
        })
        .unwrap();
        let backend = PayloadBackend::with_payload_storage(
            inner.clone(),
            blob_store,
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let err = backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("wf/payload-backend-s3-upload-failure"),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new("workflows"),
                input: durust::encode_payload(&large_payload("workflow-input")).unwrap(),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Backend(message) if message.contains("S3 payload store error"))
        );
        let claim = inner
            .claim_workflow_task(
                WorkerId::new("payload-backend-s3-upload-failure-worker"),
                ClaimWorkflowTaskOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("workflows"),
                    registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(claim.is_none());
    });
}

fn block_on_tokio<F>(future: F) -> F::Output
where
    F: Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn garage_config_from_env() -> Option<durust::S3BlobStoreConfig> {
    let endpoint = env::var("DURUST_GARAGE_ENDPOINT").ok()?;
    let bucket = env::var("DURUST_GARAGE_BUCKET").ok()?;
    let access_key_id = env::var("DURUST_GARAGE_ACCESS_KEY_ID").ok()?;
    let secret_access_key = env::var("DURUST_GARAGE_SECRET_ACCESS_KEY").ok()?;
    let region = env::var("DURUST_GARAGE_REGION").unwrap_or_else(|_| "garage".to_owned());
    let prefix = env::var("DURUST_GARAGE_PREFIX").unwrap_or_else(|_| "payloads".to_owned());
    Some(durust::S3BlobStoreConfig {
        bucket,
        endpoint,
        region,
        prefix,
        access_key_id,
        secret_access_key,
    })
}

async fn wait_for_blob_store<S>(blob_store: &S)
where
    S: durust::PayloadBlobStore,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_error = None;
    while Instant::now() < deadline {
        match blob_store.list_payload_blobs().await {
            Ok(_) => return,
            Err(err) => {
                last_error = Some(err);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    panic!("Garage S3 blob store did not become ready: {last_error:?}");
}

#[test]
fn payload_backend_upload_failure_does_not_commit_missing_payload_ref() {
    block_on(async {
        let inner = MemoryBackend::new();
        let backend = PayloadBackend::with_payload_storage(
            inner.clone(),
            FailingBlobStore,
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let err = backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("wf/payload-backend-upload-failure"),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new("workflows"),
                input: durust::encode_payload(&large_payload("workflow-input")).unwrap(),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Backend(message) if message.contains("intentional blob upload failure"))
        );
        let claim = inner
            .claim_workflow_task(
                WorkerId::new("payload-backend-upload-failure-worker"),
                ClaimWorkflowTaskOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("workflows"),
                    registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(claim.is_none());
    });
}

#[derive(Clone, Debug)]
struct FailingBlobStore;

impl durust::PayloadBlobStore for FailingBlobStore {
    fn put_payload_blob(
        &self,
        _digest: String,
        _bytes: Vec<u8>,
    ) -> BoxFuture<'static, durust::Result<String>> {
        Box::pin(ready(Err(Error::Backend(
            "intentional blob upload failure".to_owned(),
        ))))
    }

    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<Vec<u8>>> {
        Box::pin(ready(Err(Error::PayloadDecode(format!(
            "missing payload blob `{digest}`"
        )))))
    }

    fn payload_blob_exists(&self, _digest: String) -> BoxFuture<'static, durust::Result<bool>> {
        Box::pin(ready(Ok(false)))
    }

    fn list_payload_blobs(
        &self,
    ) -> BoxFuture<'static, durust::Result<BTreeMap<String, durust::TimestampMs>>> {
        Box::pin(ready(Ok(BTreeMap::new())))
    }

    fn delete_payload_blob(&self, _digest: String) -> BoxFuture<'static, durust::Result<()>> {
        Box::pin(ready(Ok(())))
    }

    fn owns_payload_blob_uri(&self, _uri: &str) -> bool {
        false
    }
}

// A blob store with a scheme none of the built-in providers know about. Inner
// providers must treat its refs as opaque; only this store hydrates or
// garbage-collects them.
#[derive(Clone, Debug, Default)]
struct TestCustomBlobStore {
    blobs: Arc<Mutex<BTreeMap<String, (Vec<u8>, durust::TimestampMs)>>>,
}

impl TestCustomBlobStore {
    fn blob_count(&self) -> usize {
        self.blobs.lock().unwrap().len()
    }
}

fn wall_clock_ms() -> durust::TimestampMs {
    durust::TimestampMs(
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX),
    )
}

impl durust::PayloadBlobStore for TestCustomBlobStore {
    fn put_payload_blob(
        &self,
        digest: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, durust::Result<String>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            let now = wall_clock_ms();
            let mut blobs = blobs.lock().unwrap();
            match blobs.get_mut(&digest) {
                Some(record) => record.1 = now,
                None => {
                    blobs.insert(digest.clone(), (bytes, now));
                }
            }
            Ok(format!("test-custom://payload/{digest}"))
        })
    }

    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<Vec<u8>>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            blobs
                .lock()
                .unwrap()
                .get(&digest)
                .map(|(bytes, _)| bytes.clone())
                .ok_or_else(|| Error::PayloadDecode(format!("missing payload blob `{digest}`")))
        })
    }

    fn payload_blob_exists(&self, digest: String) -> BoxFuture<'static, durust::Result<bool>> {
        let blobs = self.blobs.clone();
        Box::pin(async move { Ok(blobs.lock().unwrap().contains_key(&digest)) })
    }

    fn list_payload_blobs(
        &self,
    ) -> BoxFuture<'static, durust::Result<BTreeMap<String, durust::TimestampMs>>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            Ok(blobs
                .lock()
                .unwrap()
                .iter()
                .map(|(digest, (_, last_modified))| (digest.clone(), *last_modified))
                .collect())
        })
    }

    fn delete_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<()>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            blobs.lock().unwrap().remove(&digest);
            Ok(())
        })
    }

    fn owns_payload_blob_uri(&self, uri: &str) -> bool {
        uri.starts_with("test-custom://payload/")
    }
}

// Bug B pin: a custom-scheme blob store must work over any inner provider.
// Against the pre-fix scheme allowlist this fails at the first commit because
// the inner provider tries to resolve `test-custom://` refs from its own
// store.
async fn custom_scheme_blob_store_round_trips_and_survives_gc<B>(inner: B, prefix: &str)
where
    B: DurableBackend,
{
    let blob_store = TestCustomBlobStore::default();
    let backend = PayloadBackend::with_payload_storage(
        inner,
        blob_store.clone(),
        durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
    );
    let run_id = payload_offload_public_api_round_trip(
        backend.clone(),
        &format!("wf/{prefix}-custom-scheme"),
        &format!("{prefix}-custom-scheme-workflows"),
        &format!("{prefix}-custom-scheme-activities"),
    )
    .await;
    payload_offload_activity_map_round_trip(backend.clone(), &format!("{prefix}-custom")).await;
    assert!(blob_store.blob_count() >= 4);

    // Raw replay refs carry the custom scheme end to end.
    let raw_events = backend
        .stream_history_for_replay(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &raw_events[0].data else {
        panic!("expected raw workflow start event");
    };
    assert!(
        matches!(input, durust::PayloadRef::Blob { uri, .. } if uri.starts_with("test-custom://payload/")),
        "raw workflow input should be a custom-scheme blob ref, got {input:?}"
    );

    // GC with a zero grace period must still leave every reachable
    // custom-scheme blob alone.
    let before = blob_store.blob_count();
    let outcome = backend
        .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .await
        .unwrap();
    assert_eq!(outcome.failed_blobs, 0);
    assert_eq!(blob_store.blob_count(), before - outcome.deleted_blobs);

    // Hydration after GC proves reachable blobs survived.
    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
        panic!("expected hydrated workflow start event");
    };
    assert_eq!(
        durust::decode_payload::<String>(input).unwrap(),
        large_payload("workflow-input")
    );
}

#[test]
fn custom_scheme_blob_store_works_over_memory_provider() {
    block_on(async {
        custom_scheme_blob_store_round_trips_and_survives_gc(MemoryBackend::new(), "memory").await;
    });
}

#[test]
fn custom_scheme_blob_store_works_over_sqlite_provider() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("custom-scheme.sqlite3");
        let blob_store = TestCustomBlobStore::default();
        let backend = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let run_id = payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/sqlite-custom-scheme",
            "sqlite-custom-scheme-workflows",
            "sqlite-custom-scheme-activities",
        )
        .await;
        drop(backend);

        // Reopen: the persisted custom-scheme refs must hydrate through the
        // custom store and survive GC with a zero grace period.
        let reopened = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let outcome = reopened
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.failed_blobs, 0);
        let history = reopened
            .stream_history(durust::StreamHistoryRequest {
                run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(1),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap()
            .events;
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected hydrated workflow start after reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(input).unwrap(),
            large_payload("workflow-input")
        );
    });
}

#[test]
fn custom_scheme_blob_store_works_over_postgres_provider_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres custom-scheme conformance; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = postgres_test_schema("custom_scheme");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
        )
        .await
        .unwrap();
        custom_scheme_blob_store_round_trips_and_survives_gc(backend, "postgres").await;
        drop_postgres_schema(&url, &schema).await;
    });
}

// Bug A pin, fresh-upload window: every write path uploads its blob before the
// commit that makes it reachable, so GC must never delete an
// unreachable-but-young blob. `min_age: 0` reproduces the pre-fix behavior;
// the default grace period keeps the in-flight upload alive until its commit
// lands, after which reachability protects it unconditionally.
#[test]
fn payload_backend_gc_grace_period_protects_in_flight_uploads() {
    block_on(async {
        let blob_store = durust::MemoryBlobStore::new();
        let backend = PayloadBackend::with_payload_storage(
            MemoryBackend::new(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let value = large_payload("gc-race-input");
        let payload = durust::encode_payload(&value).unwrap();
        let durust::PayloadRef::Inline { bytes, .. } = payload.clone() else {
            panic!("freshly encoded payload should be inline");
        };
        let digest = durust::digest_bytes(&bytes);

        // The in-flight window: uploaded, not yet referenced by any commit.
        blob_store
            .put_payload_blob(digest.clone(), bytes.clone())
            .await
            .unwrap();
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 0);
        assert_eq!(outcome.scanned_blobs, 1);
        blob_store
            .get_payload_blob(digest.clone())
            .await
            .expect("grace period must protect the in-flight upload");

        // Zero grace period restores the pre-fix delete-anything-unreachable
        // behavior: the same window now loses the blob.
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 1);
        blob_store
            .get_payload_blob(digest.clone())
            .await
            .expect_err("zero grace period deletes the uncommitted upload");

        // Upload again and land the commit; reachability now protects the
        // blob at any grace period.
        blob_store
            .put_payload_blob(digest.clone(), bytes)
            .await
            .unwrap();
        backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("wf/gc-race-committed"),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new("gc-race-workflows"),
                input: payload,
            })
            .await
            .unwrap();
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 0);
        blob_store
            .get_payload_blob(digest)
            .await
            .expect("committed blob must always survive GC");
    });
}

// Drives one projection replacement against the single-event conformance
// workflow: wakes the run with a signal when `signal_seq > 0`, claims it, and
// commits a projection holding `value` plus a re-armed signal wait so the next
// replacement can wake the run again. Replacing a projection is the simplest
// way to turn an offloaded blob into garbage.
async fn commit_projection_replacement<B>(
    backend: &B,
    workflow_id: &str,
    queue: &str,
    signal_seq: u64,
    value: &str,
) where
    B: DurableBackend,
{
    // Idempotent re-start resolves the run id; the tiny input stays inline so
    // it cannot perturb blob-store contents.
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new(queue),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let mut consume_signals = Vec::new();
    if signal_seq > 0 {
        backend
            .signal_workflow(durust::SignalWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(workflow_id),
                signal_id: durust::SignalId::new(format!("{workflow_id}/replace/{signal_seq}")),
                signal_name: durust::SignalName::new("replace"),
                payload: durust::encode_payload(&signal_seq).unwrap(),
            })
            .await
            .unwrap();
        let inbox = backend
            .read_signal_inbox(durust::ReadSignalInboxRequest {
                run_id: run_id.clone(),
                signal_name: durust::SignalName::new("replace"),
            })
            .await
            .unwrap()
            .expect("replacement signal");
        consume_signals.push(inbox.signal_id);
    }
    let claimed = claim_conformance_workflow(
        backend,
        &format!("{workflow_id}-projection-{signal_seq}"),
        queue,
    )
    .await;
    let command_id = durust::command_id(&run_id, 1);
    let wait_id = durust::WaitId::new(format!("{}:{}:signal", command_id.run_id, command_id.seq.0));
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                consume_signals,
                upsert_waits: vec![durust::WaitRecord {
                    wait_id,
                    run_id,
                    command_id,
                    kind: durust::WaitKind::Signal,
                    key: "replace".to_owned(),
                    ready_at: None,
                }],
                ..projection_only_commit(durust::encode_payload(&value.to_owned()).unwrap())
            },
        )
        .await
        .unwrap();
}

// Bug A pin, memory provider: the provider-internal store follows the virtual
// clock, so deterministic tests control blob age with `advance_time`. Old
// unreachable blobs are collected, young ones survive the grace period, and
// `min_age: 0` collects unconditionally.
#[test]
fn memory_gc_grace_period_follows_virtual_clock() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let workflow_id = "wf/memory-gc-grace";
        let queue = "memory-gc-grace-workflows";
        backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(workflow_id),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new(queue),
                input: durust::encode_payload(&large_payload("memory-gc-input")).unwrap(),
            })
            .await
            .unwrap();
        commit_projection_replacement(&backend, workflow_id, queue, 0, "projection-old").await;
        commit_projection_replacement(&backend, workflow_id, queue, 1, "projection-mid").await;

        // The replaced projection blob is unreachable but young: the default
        // grace period retains it because it could belong to an in-flight
        // commit.
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 0);

        // Two virtual hours later the same blob is old garbage.
        backend.advance_time(Duration::from_secs(2 * 60 * 60));
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.deleted_blobs, 1,
            "projection-old blob aged past the grace period"
        );

        // A replacement after the clock advance leaves young garbage again:
        // default grace retains it, zero grace collects it.
        commit_projection_replacement(&backend, workflow_id, queue, 2, "projection-new").await;
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.deleted_blobs, 1,
            "projection-mid blob aged past the grace period while projection-new's predecessor stayed protected"
        );
        commit_projection_replacement(&backend, workflow_id, queue, 3, "projection-final").await;
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.deleted_blobs, 0,
            "projection-new blob is young garbage the grace period retains"
        );
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.deleted_blobs, 1,
            "zero grace period collects the young garbage"
        );

        // The live projection and workflow input always survive.
        let projection = backend
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected live projection");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            "projection-final"
        );
    });
}

// Bug A pin, SQLite local-directory store (close/reopen): directory blobs are
// written before their transaction commits, so GC ages them by file mtime.
// Content-addressed re-puts refresh the mtime so a blob a new in-flight commit
// deduplicated against regains its full grace period.
#[test]
fn sqlite_local_blob_gc_grace_period_and_dedup_mtime_refresh() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let object_dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gc-grace.sqlite3");
        let config = durust::PayloadStorageConfig::new()
            .inline_threshold_bytes(1)
            .blob_store(durust::BlobStoreConfig::LocalDirectory {
                root: object_dir.path().to_path_buf(),
                prefix: "payloads".to_owned(),
            });
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        let workflow_id = "wf/sqlite-gc-grace";
        let queue = "sqlite-gc-grace-workflows";
        backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(workflow_id),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new(queue),
                input: durust::encode_payload(&large_payload("sqlite-gc-input")).unwrap(),
            })
            .await
            .unwrap();
        commit_projection_replacement(&backend, workflow_id, queue, 0, "projection-old").await;
        commit_projection_replacement(&backend, workflow_id, queue, 1, "projection-live").await;

        let blob_dir = object_dir.path().join("payloads");
        let garbage_digest = durust::digest_bytes(
            durust::encode_payload(&"projection-old".to_owned())
                .unwrap()
                .inline_bytes()
                .unwrap(),
        );
        let garbage_path = blob_dir.join(&garbage_digest);
        assert!(garbage_path.exists());

        // Young unreachable garbage survives the default grace period.
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 0);
        assert!(garbage_path.exists());

        // Backdated past the grace period the same file is collected.
        let two_hours_ago = SystemTime::now() - Duration::from_secs(2 * 60 * 60);
        fs::File::options()
            .write(true)
            .open(&garbage_path)
            .unwrap()
            .set_modified(two_hours_ago)
            .unwrap();
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 1);
        assert_eq!(outcome.failed_blobs, 0);
        assert!(!garbage_path.exists());

        // Fresh-upload window: a file written by an in-flight commit (upload
        // happens before the transaction commits) survives the grace period
        // and dies only under min_age zero.
        let in_flight = durust::encode_payload(&large_payload("sqlite-in-flight")).unwrap();
        let in_flight_bytes = in_flight.inline_bytes().unwrap().to_vec();
        let in_flight_digest = durust::digest_bytes(&in_flight_bytes);
        let in_flight_path = blob_dir.join(&in_flight_digest);
        fs::write(&in_flight_path, &in_flight_bytes).unwrap();
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 0);
        assert!(in_flight_path.exists());
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 1);
        assert!(!in_flight_path.exists());

        // Dedup window: a commit that reuses an existing old blob must refresh
        // its mtime, restarting the grace period for the reused content.
        let reused_value = "projection-reused";
        let reused_digest = durust::digest_bytes(
            durust::encode_payload(&reused_value.to_owned())
                .unwrap()
                .inline_bytes()
                .unwrap(),
        );
        let reused_path = blob_dir.join(&reused_digest);
        fs::write(
            &reused_path,
            durust::encode_payload(&reused_value.to_owned())
                .unwrap()
                .inline_bytes()
                .unwrap(),
        )
        .unwrap();
        fs::File::options()
            .write(true)
            .open(&reused_path)
            .unwrap()
            .set_modified(two_hours_ago)
            .unwrap();
        commit_projection_replacement(&backend, workflow_id, queue, 2, reused_value).await;
        let refreshed = fs::metadata(&reused_path).unwrap().modified().unwrap();
        assert!(
            refreshed.duration_since(two_hours_ago).unwrap_or_default()
                > Duration::from_secs(60 * 60),
            "content-addressed re-put must refresh the blob mtime"
        );
        drop(backend);

        // Close/reopen: the refreshed blob is now reachable (live projection)
        // and hydrates from disk.
        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        let outcome = reopened
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.failed_blobs, 0);
        assert!(reused_path.exists());
        let projection = reopened
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected reopened projection");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            reused_value
        );
    });
}

// A delete failure on one garbage blob must not abort the sweep: the failure
// is recorded in the outcome and the remaining garbage is still collected.
#[derive(Clone, Debug)]
struct PoisonedDeleteBlobStore {
    inner: durust::MemoryBlobStore,
    poisoned_digest: String,
}

impl durust::PayloadBlobStore for PoisonedDeleteBlobStore {
    fn put_payload_blob(
        &self,
        digest: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, durust::Result<String>> {
        self.inner.put_payload_blob(digest, bytes)
    }

    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<Vec<u8>>> {
        self.inner.get_payload_blob(digest)
    }

    fn payload_blob_exists(&self, digest: String) -> BoxFuture<'static, durust::Result<bool>> {
        self.inner.payload_blob_exists(digest)
    }

    fn list_payload_blobs(
        &self,
    ) -> BoxFuture<'static, durust::Result<BTreeMap<String, durust::TimestampMs>>> {
        self.inner.list_payload_blobs()
    }

    fn delete_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<()>> {
        if digest == self.poisoned_digest {
            return Box::pin(ready(Err(Error::Backend(
                "intentional blob delete failure".to_owned(),
            ))));
        }
        self.inner.delete_payload_blob(digest)
    }

    fn owns_payload_blob_uri(&self, uri: &str) -> bool {
        self.inner.owns_payload_blob_uri(uri)
    }
}

#[test]
fn payload_backend_gc_records_delete_failures_and_continues() {
    block_on(async {
        let poisoned_payload = durust::encode_payload(&large_payload("poisoned-garbage")).unwrap();
        let poisoned_bytes = poisoned_payload.inline_bytes().unwrap().to_vec();
        let poisoned_digest = durust::digest_bytes(&poisoned_bytes);
        let deletable_payload =
            durust::encode_payload(&large_payload("deletable-garbage")).unwrap();
        let deletable_bytes = deletable_payload.inline_bytes().unwrap().to_vec();
        let deletable_digest = durust::digest_bytes(&deletable_bytes);

        let blob_store = PoisonedDeleteBlobStore {
            inner: durust::MemoryBlobStore::new(),
            poisoned_digest: poisoned_digest.clone(),
        };
        let backend = PayloadBackend::with_payload_storage(
            MemoryBackend::new(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        blob_store
            .put_payload_blob(poisoned_digest.clone(), poisoned_bytes)
            .await
            .unwrap();
        blob_store
            .put_payload_blob(deletable_digest.clone(), deletable_bytes)
            .await
            .unwrap();

        // Dry run reports both as would-delete without attempting deletes.
        let dry_run = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: true,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(dry_run.deleted_blobs, 2);
        assert_eq!(dry_run.failed_blobs, 0);

        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 1);
        assert_eq!(outcome.failed_blobs, 1);
        blob_store
            .get_payload_blob(deletable_digest)
            .await
            .expect_err("healthy garbage must still be deleted");
        blob_store
            .get_payload_blob(poisoned_digest)
            .await
            .expect("failed delete leaves the blob for the next sweep");
    });
}

#[test]
fn sqlite_provider_offloads_large_payloads_to_local_blob_store_and_gc_collects_objects() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let object_dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload-local-objects.sqlite3");
        let config = durust::PayloadStorageConfig::new()
            .inline_threshold_bytes(1)
            .blob_store(durust::BlobStoreConfig::LocalDirectory {
                root: object_dir.path().to_path_buf(),
                prefix: "payloads".to_owned(),
            });
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        let run_id = payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/sqlite-local-payload-offload",
            "sqlite-local-payload-workflows",
            "sqlite-local-payload-activities",
        )
        .await;
        let (gc_workflow_id, gc_projection) =
            payload_gc_removes_unreachable_projection_blob(backend.clone(), "sqlite-local").await;
        let object_count = local_blob_file_count(object_dir.path(), "payloads");
        assert!(object_count >= 4);
        assert_eq!(backend.payload_blob_count().unwrap(), object_count);

        fs::write(object_dir.path().join("payloads").join("orphan"), b"orphan").unwrap();
        let dry_run = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: true,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert!(dry_run.deleted_blobs >= 1);
        let collected = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(collected.deleted_blobs, dry_run.deleted_blobs);
        assert!(!object_dir.path().join("payloads").join("orphan").exists());
        drop(backend);

        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        let projection = reopened
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(gc_workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected retained projection after local object-store reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            gc_projection
        );
        let history = reopened
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
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected hydrated workflow start from local object store");
        };
        assert_eq!(
            durust::decode_payload::<String>(input).unwrap(),
            large_payload("workflow-input")
        );
    });
}

#[test]
fn sqlite_local_blob_store_upload_failure_does_not_commit_missing_payload_ref() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let root_file = dir.path().join("not-a-directory");
        fs::write(&root_file, b"not a directory").unwrap();
        let path = dir.path().join("payload-upload-failure.sqlite3");
        let config = durust::PayloadStorageConfig::new()
            .inline_threshold_bytes(1)
            .blob_store(durust::BlobStoreConfig::LocalDirectory {
                root: root_file,
                prefix: "payloads".to_owned(),
            });
        let backend = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        let err = backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("wf/sqlite-local-upload-failure"),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new("workflows"),
                input: durust::encode_payload(&large_payload("workflow-input")).unwrap(),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Backend(message) if message.contains("failed to create local payload blob directory"))
        );
        let claim = backend
            .claim_workflow_task(
                WorkerId::new("sqlite-local-upload-failure-worker"),
                ClaimWorkflowTaskOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("workflows"),
                    registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(claim.is_none());
    });
}

#[test]
fn memory_provider_json_codec_round_trips_nested_activity_map_payloads() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new()
                .codec(durust::CodecId::Json)
                .inline_threshold_bytes(1),
        );
        payload_json_activity_map_round_trip(backend, "memory-json").await;
    });
}

#[test]
fn sqlite_provider_json_codec_round_trips_nested_activity_map_payloads_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload-json-map.sqlite3");
        let config = durust::PayloadStorageConfig::new()
            .codec(durust::CodecId::Json)
            .inline_threshold_bytes(1);
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        let run_id = payload_json_activity_map_round_trip(backend, "sqlite-json").await;

        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        let history = stream_history(&reopened, run_id).await;
        let HistoryEventData::ActivityMapCompleted(completed) = &history[2].data else {
            panic!("expected persisted JSON activity map completion after reopen");
        };
        assert_eq!(completed.result_manifest.codec(), durust::CodecId::Json);
        let results = durust::decode_activity_map_result_refs(&completed.result_manifest).unwrap();
        assert_eq!(
            results
                .iter()
                .map(|payload| payload.codec())
                .collect::<Vec<_>>(),
            vec![durust::CodecId::Json, durust::CodecId::Json]
        );
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
    assert!(
        manifest.workflows[0]
            .input_type
            .ends_with("provider_conformance::Input")
    );
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
    query_projection_updates_atomically_and_reads_payload_refs(backend.clone()).await;
    missing_provider_blob_ref_is_rejected(backend.clone()).await;
    provider_blob_ref_metadata_mismatch_is_rejected(backend.clone()).await;
    workflow_change_version_index_tracks_markers_and_open_status(backend.clone()).await;
    continue_as_new_closes_current_run_and_starts_claimable_next_run(backend.clone()).await;
    signal_inbox_is_idempotent_ordered_and_consumed_by_commit(backend.clone()).await;
    signal_between_claim_and_commit_wakes_workflow(backend.clone()).await;
    signal_during_claim_window_survives_empty_commit(backend.clone()).await;
    signal_between_claim_and_commit_wakes_workflows_in_batch_commit(backend.clone()).await;
    terminal_run_fences_stale_mutating_commits_identically(backend.clone()).await;
    late_activity_completion_after_cancel_is_idempotent_across_retries(backend.clone()).await;
    terminal_cleanup_answers_late_calls_and_keeps_undelivered_signals(backend.clone()).await;
    consumed_signal_dedup_survives_continue_as_new(backend.clone()).await;
    timer_waits_fire_only_when_due_and_make_workflow_claimable(backend.clone()).await;
    activity_retry_reschedules_until_max_attempts(backend.clone()).await;
    non_retryable_activity_failure_skips_retry_and_wakes_workflow(backend.clone()).await;
    activity_timeout_retries_until_max_attempts_then_wakes_workflow(backend.clone()).await;
    activity_heartbeat_extends_deadline_and_rejects_stale_claim(backend.clone()).await;
    activity_heartbeat_timeout_retries_until_max_attempts_then_wakes_workflow(backend.clone())
        .await;
    cancel_commands_clear_activity_tasks(backend.clone()).await;
    child_start_dispatch_is_idempotent_and_wakes_parent(backend.clone()).await;
    child_completion_routes_to_parent(backend.clone()).await;
    child_start_conflict_records_failure(backend.clone()).await;
    parent_close_policy_cancel_cancels_child(backend.clone()).await;
    parent_close_policy_abandon_leaves_child_running(backend.clone()).await;
    activity_map_materializes_bounded_items_and_writes_result_manifest(backend.clone()).await;
    activity_map_failure_suppresses_remaining_items_and_wakes_workflow(backend.clone()).await;
    child_workflow_map_materializes_bounded_children_and_writes_result_manifest(backend.clone())
        .await;
    child_workflow_map_fail_fast_cancels_in_flight_children(backend.clone()).await;
    child_workflow_map_collect_all_records_ordered_outcomes(backend.clone()).await;
    workflow_cancel_cleans_waits_activities_and_activity_maps(backend.clone()).await;
    stale_workflow_task_commit_conflicts(backend.clone()).await;
    batch_workflow_task_claim_and_commit_results_are_ordered(backend.clone()).await;
    batch_activity_completion_reports_ordered_duplicate_and_stale_results(backend.clone()).await;
    activity_claim_filters_and_stale_completion_is_rejected(backend.clone()).await;
    unexpired_workflow_claim_lease_is_not_reclaimable(backend.clone()).await;
    // Runs last: its timeout scan uses a far-future `now` that must not
    // disturb other cases' pending activities.
    timeoutless_activity_lease_expiry_reclaims_and_fences_stale_holder(backend).await;
}

async fn start_large_payload_workflow<B>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
) -> durust::RunId
where
    B: DurableBackend,
{
    backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new(workflow_queue),
            input: durust::encode_payload(&large_payload("workflow-input")).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone()
}

async fn assert_replay_stream_payload_hydrates_explicitly<B>(backend: B, run_id: durust::RunId)
where
    B: DurableBackend,
{
    let public_history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &public_history[0].data else {
        panic!("expected public workflow start");
    };
    assert!(matches!(input, durust::PayloadRef::Inline { .. }));
    assert_eq!(
        durust::decode_payload::<String>(input).unwrap(),
        large_payload("workflow-input")
    );

    let replay_history = backend
        .stream_history_for_replay(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &replay_history[0].data else {
        panic!("expected replay workflow start");
    };
    assert!(matches!(input, durust::PayloadRef::Blob { .. }));
    let hydrated = backend.hydrate_payload(input.clone()).await.unwrap();
    assert!(matches!(hydrated, durust::PayloadRef::Inline { .. }));
    assert_eq!(
        durust::decode_payload::<String>(&hydrated).unwrap(),
        large_payload("workflow-input")
    );
}

async fn assert_side_effect_marker_stays_inline_for_replay_stream<B>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
) where
    B: DurableBackend,
{
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.side-effect-marker", 1),
            task_queue: TaskQueue::new(workflow_queue),
            input: durust::encode_payload(&()).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(workflow_queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.side-effect-marker", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("side-effect-marker-commit"), claim_opts)
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let value = durust::encode_payload(&"side-effect-value-that-exceeds-threshold").unwrap();
    assert!(value.encoded_len() > 1);

    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::SideEffectMarker(durust::SideEffectMarker {
                        command_id,
                        key: "make-id".to_owned(),
                        value,
                    }),
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        durust::CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    assert_side_effect_marker_stays_inline_in_existing_history(backend, workflow_id).await;
}

async fn assert_side_effect_marker_stays_inline_in_existing_history<B>(
    backend: B,
    workflow_id: &str,
) where
    B: DurableBackend,
{
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.side-effect-marker", 1),
            task_queue: TaskQueue::new("unused"),
            input: durust::encode_payload(&()).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let replay_history = backend
        .stream_history_for_replay(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(2),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::SideEffectMarker(marker) = &replay_history[1].data else {
        panic!("expected side effect marker in replay history");
    };
    assert_eq!(marker.key, "make-id");
    assert!(matches!(marker.value, durust::PayloadRef::Inline { .. }));
    assert_eq!(
        durust::decode_payload::<String>(&marker.value).unwrap(),
        "side-effect-value-that-exceeds-threshold"
    );
}

async fn payload_offload_public_api_round_trip<B>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    activity_queue: &str,
) -> durust::RunId
where
    B: DurableBackend,
{
    let workflow_input = large_payload("workflow-input");
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new(workflow_queue),
            input: durust::encode_payload(&workflow_input).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();

    let start_history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &start_history[0].data else {
        panic!("expected workflow start");
    };
    assert_eq!(
        durust::decode_payload::<String>(input).unwrap(),
        workflow_input
    );
    assert!(matches!(input, durust::PayloadRef::Inline { .. }));

    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(workflow_queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("payload-offload-workflow"), claim_opts)
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let activity_input = large_payload("activity-input");
    let activity_payload = durust::encode_payload(&activity_input).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new(activity_queue),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: activity_payload.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&activity_payload),
            "sha256:payload-offload-options".to_owned(),
        ),
    };
    let projection = large_payload("projection");
    let outcome = backend
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
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: Some(durust::encode_payload(&projection).unwrap()),
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

    let query = backend
        .query_projection(durust::QueryProjectionRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
        })
        .await
        .unwrap();
    let durust::QueryProjectionOutcome::Found { payload, .. } = query else {
        panic!("expected query projection");
    };
    assert_eq!(
        durust::decode_payload::<String>(&payload).unwrap(),
        projection
    );
    assert!(matches!(payload, durust::PayloadRef::Inline { .. }));

    let activity = backend
        .claim_activity_task(
            WorkerId::new("payload-offload-activity"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new(activity_queue),
                registered_activity_names: vec![ActivityName::new("conformance.echo")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("activity task");
    assert_eq!(
        durust::decode_payload::<String>(&activity.task.input).unwrap(),
        activity_input
    );
    assert!(matches!(
        activity.task.input,
        durust::PayloadRef::Inline { .. }
    ));

    let activity_result = large_payload("activity-result");
    let completed = backend
        .complete_activity(CompleteActivityRequest {
            claim: activity.claim,
            result: durust::encode_payload(&activity_result).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        completed,
        durust::CompleteActivityOutcome::Completed {
            event_id: EventId(3)
        }
    );

    let signal_payload = large_payload("signal");
    let accepted = backend
        .signal_workflow(durust::SignalWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            signal_id: durust::SignalId::new(format!("{workflow_id}/signal/1")),
            signal_name: durust::SignalName::new("payload"),
            payload: durust::encode_payload(&signal_payload).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(accepted, durust::SignalWorkflowOutcome::Accepted);
    let signal = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id: run_id.clone(),
            signal_name: durust::SignalName::new("payload"),
        })
        .await
        .unwrap()
        .expect("signal payload");
    assert_eq!(
        durust::decode_payload::<String>(&signal.payload).unwrap(),
        signal_payload
    );
    assert!(matches!(signal.payload, durust::PayloadRef::Inline { .. }));
    let signal_batch = backend
        .read_signal_inboxes(durust::ReadSignalInboxesRequest {
            requests: vec![
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("missing"),
                },
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("payload"),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(signal_batch.len(), 2);
    assert!(signal_batch[0].is_none());
    let batched_signal = signal_batch[1].as_ref().expect("batched signal payload");
    assert_eq!(
        durust::decode_payload::<String>(&batched_signal.payload).unwrap(),
        signal_payload
    );
    assert!(matches!(
        batched_signal.payload,
        durust::PayloadRef::Inline { .. }
    ));

    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::ActivityScheduled(scheduled) = &history[1].data else {
        panic!("expected activity scheduled event");
    };
    assert_eq!(
        durust::decode_payload::<String>(&scheduled.input).unwrap(),
        activity_input
    );
    let HistoryEventData::ActivityCompleted(completed) = &history[2].data else {
        panic!("expected activity completed event");
    };
    assert_eq!(
        durust::decode_payload::<String>(&completed.result).unwrap(),
        activity_result
    );

    run_id
}

fn large_payload(label: &str) -> String {
    format!("{label}:{}", "x".repeat(64))
}

fn local_blob_file_count(root: &std::path::Path, prefix: &str) -> usize {
    let dir = root.join(prefix);
    fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("failed to list local blob dir `{}`: {err}", dir.display()))
        .filter_map(|entry| {
            let entry = entry.unwrap();
            let is_file = entry.file_type().unwrap().is_file();
            let name = entry.file_name().to_string_lossy().into_owned();
            (is_file && !name.contains(".tmp-")).then_some(())
        })
        .count()
}

async fn payload_gc_removes_unreachable_projection_blob<B>(
    backend: B,
    prefix: &str,
) -> (String, String)
where
    B: DurableBackend,
{
    let workflow_id = format!("wf/{prefix}-payload-gc");
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new("payload-gc-workflows"),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("payload-gc-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let first_claim = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-gc-first")),
            claim_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first projection workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let wait_id = durust::WaitId::new(format!("{}:{}:signal", command_id.run_id, command_id.seq.0));
    let first_projection = large_payload(&format!("{prefix}-projection-old"));
    backend
        .commit_workflow_task(
            first_claim.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: Vec::new(),
                upsert_waits: vec![durust::WaitRecord {
                    wait_id,
                    run_id: run_id.clone(),
                    command_id,
                    kind: durust::WaitKind::Signal,
                    key: "replace".to_owned(),
                    ready_at: None,
                }],
                schedule_activities: Vec::new(),
                schedule_activity_maps: Vec::new(),
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
                consume_signals: Vec::new(),
                delete_waits: Vec::new(),
                cancel_commands: Vec::new(),
                query_projection: Some(durust::encode_payload(&first_projection).unwrap()),
            },
        )
        .await
        .unwrap();

    backend
        .signal_workflow(durust::SignalWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
            signal_id: durust::SignalId::new(format!("{workflow_id}/replace")),
            signal_name: durust::SignalName::new("replace"),
            payload: durust::encode_payload(&large_payload(&format!("{prefix}-wake"))).unwrap(),
        })
        .await
        .unwrap();
    let inbox = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id: run_id.clone(),
            signal_name: durust::SignalName::new("replace"),
        })
        .await
        .unwrap()
        .expect("replacement signal");
    let second_claim = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-gc-second")),
            claim_opts,
        )
        .await
        .unwrap()
        .expect("second projection workflow task");
    let second_projection = large_payload(&format!("{prefix}-projection-new"));
    backend
        .commit_workflow_task(
            second_claim.claim,
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
                query_projection: Some(durust::encode_payload(&second_projection).unwrap()),
            },
        )
        .await
        .unwrap();

    let dry_run = backend
        .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
            dry_run: true,
            min_age: Duration::ZERO,
        })
        .await
        .unwrap();
    assert!(
        dry_run.scanned_blobs > dry_run.retained_blobs,
        "expected garbage: scanned={}, retained={}, deleted={}",
        dry_run.scanned_blobs,
        dry_run.retained_blobs,
        dry_run.deleted_blobs
    );
    assert!(
        dry_run.deleted_blobs > 0,
        "expected deletions: scanned={}, retained={}, deleted={}",
        dry_run.scanned_blobs,
        dry_run.retained_blobs,
        dry_run.deleted_blobs
    );

    let collected = backend
        .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .await
        .unwrap();
    assert_eq!(collected.deleted_blobs, dry_run.deleted_blobs);

    let after = backend
        .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
            dry_run: true,
            min_age: Duration::ZERO,
        })
        .await
        .unwrap();
    assert_eq!(after.deleted_blobs, 0);

    let projection = backend
        .query_projection(durust::QueryProjectionRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
        })
        .await
        .unwrap();
    let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
        panic!("expected retained projection");
    };
    assert_eq!(
        durust::decode_payload::<String>(&payload).unwrap(),
        second_projection
    );
    (workflow_id, second_projection)
}

async fn payload_offload_child_workflow_round_trip<B>(backend: B, prefix: &str)
where
    B: DurableBackend,
{
    let parent_workflow_id = format!("wf/{prefix}-payload-child-parent");
    let child_workflow_id = format!("wf/{prefix}-payload-child-child");
    let parent_run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&parent_workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new("payload-child-parent-workflows"),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let parent = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-parent")),
            workflow_claim_opts("payload-child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent workflow task");
    let command_id = durust::command_id(&parent_run_id, 1);
    let child_input = large_payload("child-input");
    let input = durust::encode_payload(&child_input).unwrap();
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let workflow_id = durust::WorkflowId::new(&child_workflow_id);
    let task_queue = TaskQueue::new("payload-child-workflows");
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
    backend
        .commit_workflow_task(
            parent.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
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
        .await
        .unwrap();
    backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();

    let child = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child")),
            workflow_claim_opts("payload-child-workflows"),
        )
        .await
        .unwrap()
        .expect("child workflow task");
    let child_history = stream_history(&backend, child.run_id.clone()).await;
    let HistoryEventData::WorkflowStarted { input, .. } = &child_history[0].data else {
        panic!("expected child start");
    };
    assert_eq!(
        durust::decode_payload::<String>(input).unwrap(),
        child_input
    );

    let child_result = large_payload("child-result");
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCompleted {
                        result: durust::encode_payload(&child_result).unwrap(),
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
    let parent_ready = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-parent-ready")),
            workflow_claim_opts("payload-child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent completion wake");
    assert_eq!(
        parent_ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowCompleted
    );
    let parent_history = stream_history(&backend, parent_run_id).await;
    let requested = parent_history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowStartRequested(requested) => Some(requested),
            _ => None,
        })
        .expect("child start request");
    assert_eq!(
        durust::decode_payload::<String>(&requested.input).unwrap(),
        large_payload("child-input")
    );
    let completed = parent_history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowCompleted(completed) => Some(completed),
            _ => None,
        })
        .expect("child completion");
    assert_eq!(
        durust::decode_payload::<String>(&completed.result).unwrap(),
        child_result
    );
}

async fn payload_offload_child_workflow_map_round_trip<B>(backend: B, prefix: &str)
where
    B: DurableBackend,
{
    let workflow_id = format!("wf/{prefix}-payload-child-map");
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new("payload-child-map-parent-workflows"),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-map-scheduler")),
            workflow_claim_opts("payload-child-map-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        [
            "child-map-input-0",
            "child-map-input-1",
            "child-map-input-2",
        ]
        .into_iter()
        .map(|label| durust::encode_payload(&large_payload(label)).unwrap())
        .collect(),
        1,
    )
    .unwrap();
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let task_queue = TaskQueue::new("payload-child-map-workflows");
    let workflow_id_prefix = format!("wf/{prefix}-payload-child-map/item");
    let result_manifest_name = "payload-child-map-results".to_owned();
    let map_task = ChildWorkflowMapTask {
        map_command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        input_manifest: input_manifest.clone(),
        result_manifest_name: result_manifest_name.clone(),
        workflow_id_prefix: workflow_id_prefix.clone(),
        max_in_flight: 2,
        parent_close_policy: durust::ParentClosePolicy::Cancel,
        failure_mode: durust::ChildWorkflowMapFailureMode::FailFast,
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ChildWorkflowMapScheduled(
                        durust::ChildWorkflowMapScheduled {
                            command_id: command_id.clone(),
                            workflow_type: workflow_type.clone(),
                            task_queue: task_queue.clone(),
                            input_manifest: input_manifest.clone(),
                            result_manifest_name: result_manifest_name.clone(),
                            workflow_id_prefix: workflow_id_prefix.clone(),
                            max_in_flight: 2,
                            parent_close_policy: durust::ParentClosePolicy::Cancel,
                            failure_mode: durust::ChildWorkflowMapFailureMode::FailFast,
                            fingerprint: durust::child_workflow_map_fingerprint(
                                workflow_type,
                                durust::payload_digest(&input_manifest),
                                result_manifest_name,
                                workflow_id_prefix,
                                2,
                                task_queue,
                                durust::ParentClosePolicy::Cancel,
                                durust::ChildWorkflowMapFailureMode::FailFast,
                            ),
                        },
                    ),
                )],
                schedule_child_workflow_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    dispatch_child_map_starts(&backend).await;
    let child_opts = workflow_claim_opts("payload-child-map-workflows");
    let first = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-map-0")),
            child_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first child map workflow");
    let second = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-map-1")),
            child_opts.clone(),
        )
        .await
        .unwrap()
        .expect("second child map workflow");
    assert_child_map_started_with_input(&backend, &first, "child-map-input-0").await;
    assert_child_map_started_with_input(&backend, &second, "child-map-input-1").await;

    complete_child_run_string(&backend, first, &large_payload("child-map-result-0")).await;
    dispatch_child_map_starts(&backend).await;
    let third = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-map-2")),
            child_opts,
        )
        .await
        .unwrap()
        .expect("third child map workflow");
    assert_child_map_started_with_input(&backend, &third, "child-map-input-2").await;
    complete_child_run_string(&backend, third, &large_payload("child-map-result-2")).await;
    complete_child_run_string(&backend, second, &large_payload("child-map-result-1")).await;

    let history = stream_history(&backend, run_id).await;
    let HistoryEventData::ChildWorkflowMapScheduled(scheduled) = &history[1].data else {
        panic!("expected child workflow map scheduled event");
    };
    let manifest: ActivityMapInputManifest =
        durust::decode_payload(&scheduled.input_manifest).unwrap();
    assert_eq!(manifest.item_count, 3);
    for (ordinal, page) in manifest.pages.iter().enumerate() {
        let page: durust::ActivityMapInputPage = durust::decode_payload(page).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            durust::decode_payload::<String>(&page.items[0]).unwrap(),
            large_payload(&format!("child-map-input-{ordinal}"))
        );
    }

    let HistoryEventData::ChildWorkflowMapCompleted(completed) = &history[2].data else {
        panic!("expected child workflow map completed event");
    };
    let results =
        durust::decode_child_workflow_map_success_refs(&completed.result_manifest).unwrap();
    let values = results
        .iter()
        .map(|payload| durust::decode_payload::<String>(payload).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            large_payload("child-map-result-0"),
            large_payload("child-map-result-1"),
            large_payload("child-map-result-2")
        ]
    );
}

async fn assert_child_map_started_with_input<B>(
    backend: &B,
    child: &durust::ClaimedWorkflowTask,
    expected_label: &str,
) where
    B: DurableBackend,
{
    let history = stream_history(backend, child.run_id.clone()).await;
    let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
        panic!("expected child workflow start");
    };
    assert_eq!(
        durust::decode_payload::<String>(input).unwrap(),
        large_payload(expected_label)
    );
}

async fn complete_child_run_string<B>(backend: &B, child: durust::ClaimedWorkflowTask, value: &str)
where
    B: DurableBackend,
{
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: child.replay_target_event_id,
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCompleted {
                        result: durust::encode_payload(&value.to_owned()).unwrap(),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
}

async fn payload_offload_activity_map_round_trip<B>(backend: B, prefix: &str)
where
    B: DurableBackend,
{
    let workflow_id = format!("wf/{prefix}-payload-map");
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new("payload-map-workflows"),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-map-scheduler")),
            workflow_claim_opts("payload-map-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        ["map-input-0", "map-input-1", "map-input-2"]
            .into_iter()
            .map(|label| durust::encode_payload(&large_payload(label)).unwrap())
            .collect(),
        1,
    )
    .unwrap();
    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new("payload-map-activities");
    let retry_policy = durust::RetryPolicy::none();
    let map_task = ActivityMapTask {
        map_command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "payload-results".to_owned(),
        max_in_flight: 2,
    };
    backend
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
                        heartbeat_timeout: None,
                        input_manifest: input_manifest.clone(),
                        result_manifest_name: "payload-results".to_owned(),
                        max_in_flight: 2,
                        fingerprint: durust::activity_map_fingerprint(
                            ActivityName::new("conformance.echo"),
                            durust::payload_digest(&input_manifest),
                            "payload-results".to_owned(),
                            2,
                            "sha256:payload-map-options".to_owned(),
                        ),
                    }),
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

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("payload-map-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(
            WorkerId::new(format!("{prefix}-payload-map-0")),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first map task");
    let second = backend
        .claim_activity_task(
            WorkerId::new(format!("{prefix}-payload-map-1")),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("second map task");
    assert_string_map_item(&first.task, 0, "map-input-0");
    assert_string_map_item(&second.task, 1, "map-input-1");

    backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim,
            result: durust::encode_payload(&large_payload("map-result-0")).unwrap(),
        })
        .await
        .unwrap();
    let third = backend
        .claim_activity_task(
            WorkerId::new(format!("{prefix}-payload-map-2")),
            activity_opts,
        )
        .await
        .unwrap()
        .expect("third map task");
    assert_string_map_item(&third.task, 2, "map-input-2");
    backend
        .complete_activity(CompleteActivityRequest {
            claim: third.claim,
            result: durust::encode_payload(&large_payload("map-result-2")).unwrap(),
        })
        .await
        .unwrap();
    backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&large_payload("map-result-1")).unwrap(),
        })
        .await
        .unwrap();

    let history = stream_history(&backend, run_id).await;
    let HistoryEventData::ActivityMapScheduled(scheduled) = &history[1].data else {
        panic!("expected activity map scheduled event");
    };
    let manifest: ActivityMapInputManifest =
        durust::decode_payload(&scheduled.input_manifest).unwrap();
    assert_eq!(manifest.item_count, 3);
    for (ordinal, page) in manifest.pages.iter().enumerate() {
        let page: durust::ActivityMapInputPage = durust::decode_payload(page).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            durust::decode_payload::<String>(&page.items[0]).unwrap(),
            large_payload(&format!("map-input-{ordinal}"))
        );
    }

    let HistoryEventData::ActivityMapCompleted(completed) = &history[2].data else {
        panic!("expected activity map completed event");
    };
    let results = durust::decode_activity_map_result_refs(&completed.result_manifest).unwrap();
    let values = results
        .iter()
        .map(|payload| durust::decode_payload::<String>(payload).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            large_payload("map-result-0"),
            large_payload("map-result-1"),
            large_payload("map-result-2")
        ]
    );
}

async fn payload_json_activity_map_round_trip<B>(backend: B, prefix: &str) -> durust::RunId
where
    B: DurableBackend,
{
    assert_eq!(
        backend.payload_storage_config().codec,
        durust::CodecId::Json
    );
    let workflow_id = format!("wf/{prefix}-payload-map");
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new("payload-json-map-workflows"),
            input: json_payload(&0_u64),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let start_history = stream_history(&backend, run_id.clone()).await;
    let HistoryEventData::WorkflowStarted { input, .. } = &start_history[0].data else {
        panic!("expected JSON workflow start");
    };
    assert_eq!(input.codec(), durust::CodecId::Json);

    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-json-map-scheduler")),
            workflow_claim_opts("payload-json-map-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest_with_codec(
        ["json-map-input-0", "json-map-input-1"]
            .into_iter()
            .map(|label| json_payload(&large_payload(label)))
            .collect(),
        1,
        durust::CodecId::Json,
    )
    .unwrap();
    assert_eq!(input_manifest.codec(), durust::CodecId::Json);

    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new("payload-json-map-activities");
    let retry_policy = durust::RetryPolicy::none();
    let map_task = ActivityMapTask {
        map_command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "payload-json-results".to_owned(),
        max_in_flight: 2,
    };
    backend
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
                        heartbeat_timeout: None,
                        input_manifest: input_manifest.clone(),
                        result_manifest_name: "payload-json-results".to_owned(),
                        max_in_flight: 2,
                        fingerprint: durust::activity_map_fingerprint(
                            ActivityName::new("conformance.echo"),
                            durust::payload_digest(&input_manifest),
                            "payload-json-results".to_owned(),
                            2,
                            "sha256:payload-json-map-options".to_owned(),
                        ),
                    }),
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

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("payload-json-map-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(
            WorkerId::new(format!("{prefix}-payload-json-map-0")),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first JSON map task");
    let second = backend
        .claim_activity_task(
            WorkerId::new(format!("{prefix}-payload-json-map-1")),
            activity_opts,
        )
        .await
        .unwrap()
        .expect("second JSON map task");
    assert_eq!(first.task.input.codec(), durust::CodecId::Json);
    assert_eq!(second.task.input.codec(), durust::CodecId::Json);
    assert_eq!(
        durust::decode_payload::<String>(&first.task.input).unwrap(),
        large_payload("json-map-input-0")
    );
    assert_eq!(
        durust::decode_payload::<String>(&second.task.input).unwrap(),
        large_payload("json-map-input-1")
    );

    backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim,
            result: json_payload(&large_payload("json-map-result-0")),
        })
        .await
        .unwrap();
    backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: json_payload(&large_payload("json-map-result-1")),
        })
        .await
        .unwrap();

    let history = stream_history(&backend, run_id.clone()).await;
    let HistoryEventData::ActivityMapScheduled(scheduled) = &history[1].data else {
        panic!("expected JSON activity map scheduled event");
    };
    assert_eq!(scheduled.input_manifest.codec(), durust::CodecId::Json);
    let manifest: ActivityMapInputManifest =
        durust::decode_payload(&scheduled.input_manifest).unwrap();
    for page in &manifest.pages {
        assert_eq!(page.codec(), durust::CodecId::Json);
        let page: durust::ActivityMapInputPage = durust::decode_payload(page).unwrap();
        assert_eq!(page.items[0].codec(), durust::CodecId::Json);
    }

    let HistoryEventData::ActivityMapCompleted(completed) = &history[2].data else {
        panic!("expected JSON activity map completed event");
    };
    assert_eq!(completed.result_manifest.codec(), durust::CodecId::Json);
    let result_manifest: ActivityMapResultManifest =
        durust::decode_payload(&completed.result_manifest).unwrap();
    for page in &result_manifest.pages {
        assert_eq!(page.codec(), durust::CodecId::Json);
        let page: durust::ActivityMapResultPage = durust::decode_payload(page).unwrap();
        assert_eq!(page.results[0].codec(), durust::CodecId::Json);
    }
    let results = durust::decode_activity_map_result_refs(&completed.result_manifest).unwrap();
    assert_eq!(
        results
            .iter()
            .map(|payload| durust::decode_payload::<String>(payload).unwrap())
            .collect::<Vec<_>>(),
        vec![
            large_payload("json-map-result-0"),
            large_payload("json-map-result-1"),
        ]
    );

    run_id
}

fn json_payload<T>(value: &T) -> durust::PayloadRef
where
    T: Serialize + ?Sized,
{
    durust::encode_payload_with_codec(value, durust::CodecId::Json).unwrap()
}

fn assert_string_map_item(task: &durust::ActivityTask, ordinal: u64, label: &str) {
    let map_item = task.map_item.as_ref().expect("map item metadata");
    assert_eq!(map_item.item_ordinal, ordinal);
    assert_eq!(
        durust::decode_payload::<String>(&task.input).unwrap(),
        large_payload(label)
    );
    assert!(matches!(task.input, durust::PayloadRef::Inline { .. }));
}

async fn workflow_claim_filters_by_queue_and_registered_type<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/claim-filter", "claim-filter-workflows", input(9))
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
        .start_workflow::<workflow>("wf/idempotent", "idempotent-workflows", input(1))
        .await
        .unwrap();
    let second = client
        .start_workflow::<workflow>("wf/idempotent", "idempotent-workflows", input(1))
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
        .start_workflow::<workflow>("wf/stream", "stream-workflows", input(2))
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
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(2),
            max_events: 1,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap();
    assert_eq!(one_event.events.len(), 1);
    assert!(one_event.has_more);

    let one_event_by_byte_budget = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(2),
            max_events: 100,
            max_bytes: 1,
        })
        .await
        .unwrap();
    assert_eq!(one_event_by_byte_budget.events.len(), 1);
    assert!(one_event_by_byte_budget.has_more);
}

async fn stale_workflow_task_commit_conflicts<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/stale-commit", "stale-workflows", input(3))
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
    assert_eq!(outcome, CommitOutcome::Conflict);
}

async fn batch_workflow_task_claim_and_commit_results_are_ordered<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/batch-commit-a", "batch-workflows", input(11))
        .await
        .unwrap();
    client
        .start_workflow::<workflow>("wf/batch-commit-b", "batch-workflows", input(12))
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("batch-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let mut claimed = backend
        .claim_workflow_tasks(
            WorkerId::new("batch-worker"),
            ClaimWorkflowTasksOptions {
                claim: claim_opts,
                limit: 2,
                shard_filter: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);
    let first = claimed.remove(0);
    let second = claimed.remove(0);
    let completion = WorkflowTaskCommit {
        expected_tail_event_id: first.replay_target_event_id,
        append_events: vec![NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
            result: durust::encode_payload(&first.run_id.0).unwrap(),
        })],
        ..WorkflowTaskCommit::default()
    };
    let stale = WorkflowTaskCommit {
        expected_tail_event_id: EventId::ZERO,
        ..WorkflowTaskCommit::default()
    };
    let results = backend
        .commit_workflow_tasks(WorkflowTaskCommitBatch {
            commits: vec![
                WorkflowTaskCommitInput {
                    claim: first.claim.clone(),
                    commit: completion,
                },
                WorkflowTaskCommitInput {
                    claim: second.claim.clone(),
                    commit: stale,
                },
            ],
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].claim.run_id, first.run_id);
    assert_eq!(
        results[0].result,
        Ok(CommitOutcome::Committed {
            new_tail_event_id: EventId(2),
        })
    );
    assert_eq!(results[1].claim.run_id, second.run_id);
    assert_eq!(results[1].result, Ok(CommitOutcome::Conflict));
}

async fn released_workflow_task_is_claimable_again<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/release", "release-workflows", input(5))
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

// Parametrized on how time passes because delayed visibility is a clock
// comparison: memory follows the virtual clock (`advance_time`) while the SQL
// providers compare against the wall clock and must really sleep.
async fn delayed_released_workflow_task_is_not_claimable_until_visible<B, Advance, AdvanceFut>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    advance_past_delay: Advance,
) where
    B: DurableBackend,
    Advance: FnOnce() -> AdvanceFut,
    AdvanceFut: Future<Output = ()>,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>(workflow_id, workflow_queue, input(5))
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(workflow_queue),
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

    advance_past_delay().await;
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
        .start_workflow::<workflow>("wf/query-raw", "query-raw-workflows", input(5))
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
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
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
    let projection_payload = durust::encode_payload(&"visible").unwrap();
    let committed = backend
        .commit_workflow_task(
            reclaimed.claim,
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
                query_projection: Some(projection_payload.clone()),
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
            payload: projection_payload,
        }
    );
}

// Reads back the provider-minted blob ref for a freshly started large-input
// workflow so tests can derive the provider's own URI scheme without
// hardcoding it. The input exceeds the default inline threshold so it offloads
// under any payload configuration.
async fn provider_offloaded_input_ref<B>(
    backend: &B,
    workflow_id: &str,
    workflow_queue: &str,
) -> durust::PayloadRef
where
    B: DurableBackend,
{
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new(workflow_queue),
            input: durust::encode_payload(&format!("{workflow_id}:{}", "x".repeat(64 * 1024)))
                .unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let raw_events = backend
        .stream_history_for_replay(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &raw_events[0].data else {
        panic!("expected workflow start event");
    };
    assert!(
        matches!(input, durust::PayloadRef::Blob { .. }),
        "large workflow input should be provider-offloaded"
    );
    input.clone()
}

async fn claim_conformance_workflow<B>(
    backend: &B,
    worker: &str,
    queue: &str,
) -> durust::ClaimedWorkflowTask
where
    B: DurableBackend,
{
    backend
        .claim_workflow_task(
            WorkerId::new(worker),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new(queue),
                registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("workflow task")
}

fn projection_only_commit(payload: durust::PayloadRef) -> WorkflowTaskCommit {
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
    }
}

async fn missing_provider_blob_ref_is_rejected<B>(backend: B)
where
    B: DurableBackend,
{
    // A ref carrying the provider's own scheme must be validated at commit
    // time: a digest missing from the provider's store rejects the commit.
    let source_ref = provider_offloaded_input_ref(
        &backend,
        "wf/missing-blob-source",
        "missing-blob-source-workflows",
    )
    .await;
    let durust::PayloadRef::Blob {
        codec,
        schema_fingerprint,
        compression,
        encryption,
        digest,
        size,
        uri,
    } = source_ref
    else {
        unreachable!();
    };
    let missing = durust::PayloadRef::Blob {
        codec,
        schema_fingerprint: schema_fingerprint.clone(),
        compression,
        encryption: encryption.clone(),
        digest: "sha256:missing".to_owned(),
        size,
        uri: uri.replace(digest.as_str(), "sha256:missing"),
    };
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/missing-blob", "missing-blob-workflows", input(5))
        .await
        .unwrap();
    let claimed =
        claim_conformance_workflow(&backend, "missing-blob-worker", "missing-blob-workflows").await;
    let err = backend
        .commit_workflow_task(claimed.claim, projection_only_commit(missing))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::PayloadDecode(message) if message.contains("missing payload blob"))
    );

    // A scheme the provider does not own is opaque: the commit persists the
    // ref unchanged and provider hydration returns it as-is. Only a decorating
    // payload layer that owns the scheme may resolve it.
    let foreign = durust::PayloadRef::Blob {
        codec,
        schema_fingerprint,
        compression,
        encryption,
        digest: "sha256:foreign".to_owned(),
        size,
        uri: "test-unknown://payload/sha256:foreign".to_owned(),
    };
    client
        .start_workflow::<workflow>(
            "wf/foreign-scheme-blob",
            "foreign-scheme-blob-workflows",
            input(5),
        )
        .await
        .unwrap();
    let claimed = claim_conformance_workflow(
        &backend,
        "foreign-scheme-blob-worker",
        "foreign-scheme-blob-workflows",
    )
    .await;
    backend
        .commit_workflow_task(claimed.claim, projection_only_commit(foreign.clone()))
        .await
        .unwrap();
    let projection = backend
        .query_projection(durust::QueryProjectionRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new("wf/foreign-scheme-blob"),
        })
        .await
        .unwrap();
    let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
        panic!("expected opaque foreign-scheme projection");
    };
    assert_eq!(payload, foreign);
    let hydrated = backend.hydrate_payload(payload).await.unwrap();
    assert_eq!(hydrated, foreign);
}

async fn provider_blob_ref_metadata_mismatch_is_rejected<B>(backend: B)
where
    B: DurableBackend,
{
    // Metadata validation applies to refs carrying the provider's own scheme;
    // the source ref supplies that scheme with its real digest.
    let source_ref = provider_offloaded_input_ref(
        &backend,
        "wf/blob-metadata-source",
        "blob-metadata-source-workflows",
    )
    .await;
    let durust::PayloadRef::Blob {
        codec,
        schema_fingerprint,
        compression,
        encryption,
        digest,
        size,
        uri,
    } = source_ref
    else {
        unreachable!();
    };

    let cases = [
        (
            "schema",
            durust::PayloadRef::Blob {
                codec,
                schema_fingerprint: durust::SchemaFingerprint("sha256:mismatched".to_owned()),
                compression,
                encryption: encryption.clone(),
                digest: digest.clone(),
                size,
                uri: uri.clone(),
            },
        ),
        (
            "codec",
            durust::PayloadRef::Blob {
                codec: durust::CodecId::Json,
                schema_fingerprint: schema_fingerprint.clone(),
                compression,
                encryption: encryption.clone(),
                digest: digest.clone(),
                size,
                uri: uri.clone(),
            },
        ),
    ];

    for (case, mismatched) in cases {
        let workflow_id = format!("wf/blob-metadata-mismatch/{case}");
        let run_id = backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(&workflow_id),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new("blob-metadata-mismatch-workflows"),
                input: durust::encode_payload(&0_u64).unwrap(),
            })
            .await
            .unwrap()
            .run_id()
            .clone();
        let claimed = backend
            .claim_workflow_task(
                WorkerId::new(format!("blob-metadata-mismatch-{case}")),
                ClaimWorkflowTaskOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("blob-metadata-mismatch-workflows"),
                    registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("workflow task");
        assert_eq!(claimed.run_id, run_id);
        let err = backend
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
                    query_projection: Some(mismatched),
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::PayloadDecode(message) if message.contains("payload blob metadata mismatch")),
            "unexpected error for {case} mismatch: {err:?}"
        );
    }
}

async fn workflow_change_version_index_tracks_markers_and_open_status<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/version-index", "version-index-workflows", input(5))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("version-index-worker"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("version-index-workflows"),
                registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::VersionMarker(durust::VersionMarker {
                        command_id: command_id.clone(),
                        change_id: "replace-a-with-b".to_owned(),
                        version: 1,
                    }),
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
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let open = backend
        .workflow_change_versions(durust::WorkflowChangeVersionsRequest {
            namespace: Namespace::default(),
            workflow_id: None,
            run_id: Some(run_id.clone()),
            change_id: Some("replace-a-with-b".to_owned()),
        })
        .await
        .unwrap();
    assert!(!open.safe_to_remove());
    assert_eq!(open.records.len(), 1);
    let record = &open.records[0];
    assert_eq!(
        record.workflow_id,
        durust::WorkflowId::new("wf/version-index")
    );
    assert_eq!(
        record.workflow_type,
        WorkflowType::new("conformance.workflow", 1)
    );
    assert_eq!(record.run_id, run_id);
    assert_eq!(record.change_id, "replace-a-with-b");
    assert_eq!(record.version, 1);
    assert_eq!(
        record.marker_kind,
        durust::WorkflowChangeMarkerKind::Version
    );
    assert_eq!(record.status, durust::WorkflowChangeVersionStatus::Open);
    assert_eq!(record.command_seq, durust::CommandSeq(1));
    assert_eq!(record.first_event_id, EventId(2));

    client
        .cancel_workflow("wf/version-index", "conformance close")
        .await
        .unwrap();
    let closed = backend
        .workflow_change_versions(durust::WorkflowChangeVersionsRequest {
            namespace: Namespace::default(),
            workflow_id: None,
            run_id: Some(run_id),
            change_id: Some("replace-a-with-b".to_owned()),
        })
        .await
        .unwrap();
    assert!(closed.safe_to_remove());
    assert_eq!(closed.records.len(), 1);
    assert_eq!(
        closed.records[0].status,
        durust::WorkflowChangeVersionStatus::Closed
    );
}

async fn continue_as_new_closes_current_run_and_starts_claimable_next_run<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let first_run_id = client
        .start_workflow::<workflow>("wf/continue-conformance", "continue-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("continue-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("continue-worker"), claim_opts.clone())
        .await
        .unwrap()
        .expect("initial workflow task");
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowContinuedAsNew {
                        input: durust::encode_payload(&7_u64).unwrap(),
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
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let old_history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: first_run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    assert_eq!(old_history.len(), 2);
    assert!(matches!(
        old_history[1].data,
        HistoryEventData::WorkflowContinuedAsNew { .. }
    ));

    let next = backend
        .claim_workflow_task(WorkerId::new("continue-next-worker"), claim_opts)
        .await
        .unwrap()
        .expect("continued workflow task");
    assert_ne!(next.run_id, first_run_id);
    assert_eq!(
        next.workflow_id,
        durust::WorkflowId::new("wf/continue-conformance")
    );
    assert_eq!(next.reason, durust::WorkflowTaskReason::WorkflowStarted);
    assert_eq!(next.replay_target_event_id, EventId(1));
    let new_history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: next.run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    assert_eq!(new_history.len(), 1);
    let HistoryEventData::WorkflowStarted { input, .. } = &new_history[0].data else {
        panic!("expected new run start");
    };
    assert_eq!(durust::decode_payload::<u64>(input).unwrap(), 7);
}

async fn signal_inbox_is_idempotent_ordered_and_consumed_by_commit<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/signal-inbox", "signal-inbox-workflows", input(5))
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
    let other = client
        .signal_workflow("wf/signal-inbox", "other", "signal/inbox/other", "other")
        .await
        .unwrap();
    assert_eq!(other, durust::SignalWorkflowOutcome::Accepted);

    let batch = backend
        .read_signal_inboxes(durust::ReadSignalInboxesRequest {
            requests: vec![
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("ready"),
                },
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("missing"),
                },
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("other"),
                },
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("ready"),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(batch.len(), 4);
    let first_batch = batch[0].as_ref().expect("first ready signal");
    assert_eq!(
        first_batch.signal_id,
        durust::SignalId::new("signal/inbox/1")
    );
    assert_eq!(
        durust::decode_payload::<String>(&first_batch.payload).unwrap(),
        "first"
    );
    assert!(batch[1].is_none());
    let other_batch = batch[2].as_ref().expect("other signal");
    assert_eq!(
        other_batch.signal_id,
        durust::SignalId::new("signal/inbox/other")
    );
    assert_eq!(
        durust::decode_payload::<String>(&other_batch.payload).unwrap(),
        "other"
    );
    assert_eq!(
        batch[3].as_ref().expect("repeated ready request").signal_id,
        durust::SignalId::new("signal/inbox/1")
    );

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
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
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
            run_id: run_id.clone(),
            signal_name: durust::SignalName::new("ready"),
        })
        .await
        .unwrap()
        .expect("second signal");
    assert_eq!(
        second_inbox.signal_id,
        durust::SignalId::new("signal/inbox/2")
    );

    let post_consume_batch = backend
        .read_signal_inboxes(durust::ReadSignalInboxesRequest {
            requests: vec![
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("ready"),
                },
                durust::ReadSignalInboxRequest {
                    run_id,
                    signal_name: durust::SignalName::new("other"),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(
        post_consume_batch[0]
            .as_ref()
            .expect("second ready signal")
            .signal_id,
        durust::SignalId::new("signal/inbox/2")
    );
    assert_eq!(
        post_consume_batch[1]
            .as_ref()
            .expect("other signal remains unconsumed")
            .signal_id,
        durust::SignalId::new("signal/inbox/other")
    );
}

fn signal_race_claim_opts(queue: &str) -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.signal-race", 1)],
        lease_duration: Duration::from_secs(30),
    }
}

/// The commit the signal-race workflow's first task produces: no appends, one
/// signal wait registered for command seq 1 on signal name `go`.
fn signal_wait_commit(run_id: &durust::RunId, expected_tail: EventId) -> WorkflowTaskCommit {
    let command_id = durust::command_id(run_id, 1);
    WorkflowTaskCommit {
        expected_tail_event_id: expected_tail,
        upsert_waits: vec![durust::WaitRecord {
            wait_id: durust::WaitId::new(format!(
                "{}:{}:signal",
                command_id.run_id, command_id.seq.0
            )),
            run_id: run_id.clone(),
            command_id,
            kind: durust::WaitKind::Signal,
            key: "go".to_owned(),
            ready_at: None,
        }],
        ..WorkflowTaskCommit::default()
    }
}

/// Runs a real worker until idle and asserts the signal-race run finished by
/// consuming a signal and completing with the expected payload.
async fn assert_signal_race_run_completes<B>(
    backend: &B,
    queue: &str,
    run_id: durust::RunId,
    expected_result: &str,
) where
    B: DurableBackend,
{
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue(queue)
        .activity_task_queue(queue)
        .register_workflow(signal_race_workflow)
        .build();
    worker.run_until_idle().await.unwrap();

    let history = stream_history(backend, run_id).await;
    assert_eq!(
        history.len(),
        3,
        "expected WorkflowStarted, SignalConsumed, WorkflowCompleted; got {history:?}"
    );
    assert!(matches!(
        history[1].data,
        HistoryEventData::SignalConsumed(_)
    ));
    let HistoryEventData::WorkflowCompleted { result } = &history[2].data else {
        panic!("expected WorkflowCompleted, got {:?}", history[2].data);
    };
    assert_eq!(
        durust::decode_payload::<String>(result).unwrap(),
        expected_result
    );
}

/// A signal delivered after a task is claimed but before its commit registers
/// the signal wait must leave the run immediately claimable with
/// `SignalReceived`; the commit's ready-reason recomputation is what prevents
/// the delivery from being lost until an unrelated event pokes the run.
async fn signal_between_claim_and_commit_wakes_workflow<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<signal_race_workflow>(
            "wf/signal-race-new-wait",
            "signal-race-new-wait",
            input(1),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("signal-race-claimer"),
            signal_race_claim_opts("signal-race-new-wait"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    assert_eq!(claimed.reason, durust::WorkflowTaskReason::WorkflowStarted);

    // The race: the signal lands while the task is claimed and the wait it
    // matches is only created by the claimed task's commit below.
    let accepted = client
        .signal_workflow(
            "wf/signal-race-new-wait",
            "go",
            "signal/race-new-wait/1",
            "raced-hello",
        )
        .await
        .unwrap();
    assert_eq!(accepted, durust::SignalWorkflowOutcome::Accepted);

    let outcome = backend
        .commit_workflow_task(claimed.claim, signal_wait_commit(&run_id, EventId(1)))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(1)
        }
    );

    let woken = backend
        .claim_workflow_task(
            WorkerId::new("signal-race-waker"),
            signal_race_claim_opts("signal-race-new-wait"),
        )
        .await
        .unwrap()
        .expect("run should be immediately claimable after the racing signal");
    assert_eq!(woken.reason, durust::WorkflowTaskReason::SignalReceived);
    backend
        .release_workflow_task(
            woken.claim,
            durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::SignalReceived),
        )
        .await
        .unwrap();

    // The real workflow consumes the raced signal on its next task.
    assert_signal_race_run_completes(
        &backend,
        "signal-race-new-wait",
        run_id.clone(),
        "raced-hello",
    )
    .await;
    let inbox = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id,
            signal_name: durust::SignalName::new("go"),
        })
        .await
        .unwrap();
    assert!(inbox.is_none(), "raced signal should be consumed");
}

/// A signal delivered during the claim window for a wait that already existed
/// before the claim must survive a commit that carries no mutations: the
/// commit's unconditional ready-reason write would otherwise erase the wakeup
/// the delivery recorded.
async fn signal_during_claim_window_survives_empty_commit<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<signal_race_workflow>(
            "wf/signal-race-existing-wait",
            "signal-race-existing-wait",
            input(1),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("signal-existing-scheduler"),
            signal_race_claim_opts("signal-race-existing-wait"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    backend
        .commit_workflow_task(claimed.claim, signal_wait_commit(&run_id, EventId(1)))
        .await
        .unwrap();
    // Blocked on the signal: not claimable until a delivery arrives.
    assert!(
        backend
            .claim_workflow_task(
                WorkerId::new("signal-existing-early"),
                signal_race_claim_opts("signal-race-existing-wait"),
            )
            .await
            .unwrap()
            .is_none()
    );

    let first = client
        .signal_workflow(
            "wf/signal-race-existing-wait",
            "go",
            "signal/race-existing/1",
            "first",
        )
        .await
        .unwrap();
    assert_eq!(first, durust::SignalWorkflowOutcome::Accepted);
    let woken = backend
        .claim_workflow_task(
            WorkerId::new("signal-existing-claimer"),
            signal_race_claim_opts("signal-race-existing-wait"),
        )
        .await
        .unwrap()
        .expect("signal delivery should wake the run");
    assert_eq!(woken.reason, durust::WorkflowTaskReason::SignalReceived);

    // Second delivery lands while the task is claimed, matching the
    // pre-existing wait; the claimed task then commits nothing (a spurious
    // wake), which must not erase the pending delivery's readiness.
    let second = client
        .signal_workflow(
            "wf/signal-race-existing-wait",
            "go",
            "signal/race-existing/2",
            "second",
        )
        .await
        .unwrap();
    assert_eq!(second, durust::SignalWorkflowOutcome::Accepted);
    let outcome = backend
        .commit_workflow_task(
            woken.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                ..WorkflowTaskCommit::default()
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

    let rewoken = backend
        .claim_workflow_task(
            WorkerId::new("signal-existing-rewake"),
            signal_race_claim_opts("signal-race-existing-wait"),
        )
        .await
        .unwrap()
        .expect("pending signals must keep the run claimable after an empty commit");
    assert_eq!(rewoken.reason, durust::WorkflowTaskReason::SignalReceived);
    backend
        .release_workflow_task(
            rewoken.claim,
            durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::SignalReceived),
        )
        .await
        .unwrap();

    // The real workflow consumes the oldest delivery; the second stays in the
    // inbox for the (now terminal) run.
    assert_signal_race_run_completes(
        &backend,
        "signal-race-existing-wait",
        run_id.clone(),
        "first",
    )
    .await;
    let remaining = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id,
            signal_name: durust::SignalName::new("go"),
        })
        .await
        .unwrap()
        .expect("second delivery stays unconsumed");
    assert_eq!(
        remaining.signal_id,
        durust::SignalId::new("signal/race-existing/2")
    );
}

/// The claim-window signal race applied to a multi-run `commit_workflow_tasks`
/// batch: every committed run with a pending consumable signal must come out
/// claimable, exercising the set-based batch commit path on providers that
/// have one.
async fn signal_between_claim_and_commit_wakes_workflows_in_batch_commit<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let queue = "signal-race-batch";
    let mut claims = Vec::new();
    for index in 0..2 {
        let workflow_id = format!("wf/signal-race-batch/{index}");
        let run_id = client
            .start_workflow::<signal_race_workflow>(&workflow_id, queue, input(index))
            .await
            .unwrap();
        let claimed = backend
            .claim_workflow_task(
                WorkerId::new(format!("signal-batch-claimer-{index}")),
                signal_race_claim_opts(queue),
            )
            .await
            .unwrap()
            .expect("workflow task");
        assert_eq!(claimed.run_id, run_id);
        let accepted = client
            .signal_workflow(
                &workflow_id,
                "go",
                format!("signal/race-batch/{index}"),
                format!("batch-{index}"),
            )
            .await
            .unwrap();
        assert_eq!(accepted, durust::SignalWorkflowOutcome::Accepted);
        claims.push(claimed);
    }

    let results = backend
        .commit_workflow_tasks(WorkflowTaskCommitBatch {
            commits: claims
                .iter()
                .map(|claimed| WorkflowTaskCommitInput {
                    claim: claimed.claim.clone(),
                    commit: signal_wait_commit(&claimed.run_id, EventId(1)),
                })
                .collect(),
        })
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    for result in &results {
        assert_eq!(
            *result.result.as_ref().unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(1)
            }
        );
    }

    let mut woken = Vec::new();
    for index in 0..2 {
        let task = backend
            .claim_workflow_task(
                WorkerId::new(format!("signal-batch-waker-{index}")),
                signal_race_claim_opts(queue),
            )
            .await
            .unwrap()
            .expect("both committed runs should be claimable after racing signals");
        assert_eq!(task.reason, durust::WorkflowTaskReason::SignalReceived);
        woken.push(task);
    }
    let woken_run_ids = woken
        .iter()
        .map(|task| task.run_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(woken_run_ids.len(), 2, "each run wakes exactly once");
    for task in woken {
        backend
            .release_workflow_task(
                task.claim,
                durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::SignalReceived),
            )
            .await
            .unwrap();
    }

    for (index, claimed) in claims.into_iter().enumerate() {
        assert_signal_race_run_completes(
            &backend,
            queue,
            claimed.run_id,
            &format!("batch-{index}"),
        )
        .await;
    }
}

/// Once a run is terminal every mutation kind in a stale holder's commit is
/// rejected identically across providers. Terminal transitions clear the
/// claim, so the fencing check fires first and the rejection is `StaleLease`
/// for every kind; the deeper `TerminalWorkflow` guard is pinned per provider
/// with forged state in the provider unit suites.
async fn terminal_run_fences_stale_mutating_commits_identically<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<signal_race_workflow>(
            "wf/terminal-fence",
            "terminal-fence-workflows",
            input(1),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("terminal-fence-claimer"),
            signal_race_claim_opts("terminal-fence-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let cancelled = client
        .cancel_workflow("wf/terminal-fence", "fence test")
        .await
        .unwrap();
    let durust::CancelWorkflowOutcome::Cancelled { event_id, .. } = cancelled else {
        panic!("expected cancellation, got {cancelled:?}");
    };

    for (kind, commit) in terminal_fence_commits(&run_id, event_id) {
        let err = backend
            .commit_workflow_task(claimed.claim.clone(), commit)
            .await
            .expect_err(kind);
        assert!(
            matches!(err, Error::StaleLease),
            "stale `{kind}` commit against the cancelled run should fence as StaleLease, got {err:?}"
        );
    }
}

/// One commit per workflow-visible mutation kind, aimed at the post-cancel
/// tail so only claim fencing (not a tail conflict) decides the outcome.
fn terminal_fence_commits(
    run_id: &durust::RunId,
    expected_tail: EventId,
) -> Vec<(&'static str, WorkflowTaskCommit)> {
    let command_id = durust::command_id(run_id, 900);
    let base = WorkflowTaskCommit {
        expected_tail_event_id: expected_tail,
        ..WorkflowTaskCommit::default()
    };
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("terminal-fence-activities"),
        retry_policy: durust::RetryPolicy::exponential().max_attempts(1),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-options".to_owned(),
        ),
    };
    vec![
        (
            "append_events",
            WorkflowTaskCommit {
                append_events: vec![NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
                    result: durust::encode_payload(&0_u64).unwrap(),
                })],
                ..base.clone()
            },
        ),
        (
            "schedule_activities",
            WorkflowTaskCommit {
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..base.clone()
            },
        ),
        (
            "upsert_waits",
            WorkflowTaskCommit {
                upsert_waits: vec![durust::WaitRecord {
                    wait_id: durust::WaitId::new(format!("{run_id}:900:signal")),
                    run_id: run_id.clone(),
                    command_id: command_id.clone(),
                    kind: durust::WaitKind::Signal,
                    key: "go".to_owned(),
                    ready_at: None,
                }],
                ..base.clone()
            },
        ),
        (
            "consume_signals",
            WorkflowTaskCommit {
                consume_signals: vec![durust::SignalId::new("terminal-fence-signal")],
                ..base.clone()
            },
        ),
        (
            "delete_waits",
            WorkflowTaskCommit {
                delete_waits: vec![durust::WaitId::new(format!("{run_id}:900:signal"))],
                ..base.clone()
            },
        ),
        (
            "start_child_workflows",
            WorkflowTaskCommit {
                start_child_workflows: vec![durust::ChildStartOutboxMessage {
                    command_id: command_id.clone(),
                    workflow_id: durust::WorkflowId::new(format!("{run_id}/fence-child")),
                    workflow_type: WorkflowType::new("conformance.signal-race", 1),
                    task_queue: TaskQueue::new("terminal-fence-workflows"),
                    input: durust::encode_payload(&Input { value: 0 }).unwrap(),
                    parent_close_policy: durust::ParentClosePolicy::Cancel,
                    child_map_item: None,
                }],
                ..base.clone()
            },
        ),
        (
            "schedule_activity_maps",
            WorkflowTaskCommit {
                schedule_activity_maps: vec![durust::ActivityMapTask {
                    map_command_id: command_id.clone(),
                    activity_name: ActivityName::new("conformance.echo"),
                    task_queue: TaskQueue::new("terminal-fence-activities"),
                    input_manifest: durust::encode_payload(&0_u64).unwrap(),
                    result_manifest_name: "results".to_owned(),
                    max_in_flight: 1,
                    retry_policy: durust::RetryPolicy::exponential().max_attempts(1),
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                }],
                ..base.clone()
            },
        ),
        (
            "schedule_child_workflow_maps",
            WorkflowTaskCommit {
                schedule_child_workflow_maps: vec![ChildWorkflowMapTask {
                    map_command_id: command_id.clone(),
                    workflow_type: WorkflowType::new("conformance.signal-race", 1),
                    task_queue: TaskQueue::new("terminal-fence-workflows"),
                    input_manifest: durust::encode_payload(&0_u64).unwrap(),
                    result_manifest_name: "results".to_owned(),
                    workflow_id_prefix: format!("{run_id}/fence-child-map"),
                    max_in_flight: 1,
                    parent_close_policy: durust::ParentClosePolicy::Cancel,
                    failure_mode: durust::ChildWorkflowMapFailureMode::FailFast,
                }],
                ..base.clone()
            },
        ),
        (
            "cancel_commands",
            WorkflowTaskCommit {
                cancel_commands: vec![command_id],
                ..base.clone()
            },
        ),
        (
            "query_projection",
            WorkflowTaskCommit {
                query_projection: Some(durust::encode_payload(&0_u64).unwrap()),
                ..base
            },
        ),
    ]
}

/// Terminal cleanup deletes the run's operational rows, so late activity
/// calls (heartbeat included) must answer `AlreadyCompleted` from row
/// absence across retries, claim scans must not see the terminal run's
/// tasks, undelivered signals must stay readable through the inbox, and
/// their `signal_id` dedup must survive while new sends fail terminally.
async fn terminal_cleanup_answers_late_calls_and_keeps_undelivered_signals<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(
            "wf/terminal-cleanup",
            "terminal-cleanup-workflows",
            input(5),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("terminal-cleanup-scheduler"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("terminal-cleanup-workflows"),
                registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_payload = durust::encode_payload(&Input { value: 3 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("terminal-cleanup-activities"),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: Some(Duration::from_secs(30)),
        input: input_payload.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input_payload),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                    scheduled.clone(),
                ))],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("terminal-cleanup-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let activity = backend
        .claim_activity_task(
            WorkerId::new("terminal-cleanup-worker"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("activity task");
    // An undelivered signal lands before the run closes.
    let undelivered = client
        .signal_workflow(
            "wf/terminal-cleanup",
            "go",
            "signal/terminal-cleanup/undelivered",
            "pending",
        )
        .await
        .unwrap();
    assert_eq!(undelivered, durust::SignalWorkflowOutcome::Accepted);

    client
        .cancel_workflow("wf/terminal-cleanup", "terminal cleanup test")
        .await
        .unwrap();

    // Late calls answer idempotently from row absence, on every retry.
    for attempt in 0..2 {
        let heartbeat = backend
            .heartbeat_activity(durust::ActivityHeartbeatRequest {
                claim: activity.claim.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            heartbeat,
            durust::ActivityHeartbeatOutcome::AlreadyCompleted,
            "heartbeat retry {attempt}"
        );
        let completed = backend
            .complete_activity(CompleteActivityRequest {
                claim: activity.claim.clone(),
                result: durust::encode_payload(&3_u64).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            completed,
            durust::CompleteActivityOutcome::AlreadyCompleted,
            "complete retry {attempt}"
        );
        let failed = backend
            .fail_activity(FailActivityRequest {
                claim: activity.claim.clone(),
                failure: durust::DurableFailure::new("test.late", "late failure"),
            })
            .await
            .unwrap();
        assert_eq!(
            failed,
            durust::FailActivityOutcome::AlreadyCompleted,
            "fail retry {attempt}"
        );
    }

    // Claim scans no longer see the terminal run's tasks.
    assert!(
        backend
            .claim_activity_task(WorkerId::new("terminal-cleanup-leftover"), activity_opts)
            .await
            .unwrap()
            .is_none()
    );

    // The undelivered signal stays readable and its dedup record survives;
    // a genuinely new send fails terminally.
    let inboxed = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id: run_id.clone(),
            signal_name: durust::SignalName::new("go"),
        })
        .await
        .unwrap()
        .expect("undelivered signal survives terminal cleanup");
    assert_eq!(
        inboxed.signal_id,
        durust::SignalId::new("signal/terminal-cleanup/undelivered")
    );
    let duplicate = client
        .signal_workflow(
            "wf/terminal-cleanup",
            "go",
            "signal/terminal-cleanup/undelivered",
            "pending",
        )
        .await
        .unwrap();
    assert_eq!(duplicate, durust::SignalWorkflowOutcome::Duplicate);
    let fresh = client
        .signal_workflow(
            "wf/terminal-cleanup",
            "go",
            "signal/terminal-cleanup/fresh",
            "rejected",
        )
        .await;
    assert!(matches!(fresh, Err(durust::Error::TerminalWorkflow)));
}

/// Consumed signal rows are the `signal_id` dedup record, and continue-as-new
/// keeps accepting sends under the same workflow id, so cleanup after a
/// continue-as-new transition must retain them: a retried send of an already
/// consumed id must stay `Duplicate` instead of delivering again to the next
/// run.
async fn consumed_signal_dedup_survives_continue_as_new<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/can-signal-dedup", "can-signal-workflows", input(2))
        .await
        .unwrap();
    let consumed_signal_id = durust::SignalId::new("signal/can-dedup/consumed");
    let accepted = client
        .signal_workflow(
            "wf/can-signal-dedup",
            "go",
            consumed_signal_id.0.clone(),
            "first",
        )
        .await
        .unwrap();
    assert_eq!(accepted, durust::SignalWorkflowOutcome::Accepted);

    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("can-signal-scheduler"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("can-signal-workflows"),
                registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let signal_payload = durust::encode_payload(&"first").unwrap();
    // One commit consumes the delivery and continues the run as new.
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![
                    NewHistoryEvent::new(HistoryEventData::SignalConsumed(
                        durust::SignalConsumed {
                            command_id: command_id.clone(),
                            signal_id: consumed_signal_id.clone(),
                            signal_name: durust::SignalName::new("go"),
                            payload: signal_payload,
                            fingerprint: durust::signal_fingerprint(durust::SignalName::new("go")),
                        },
                    )),
                    NewHistoryEvent::new(HistoryEventData::WorkflowContinuedAsNew {
                        input: durust::encode_payload(&Input { value: 3 }).unwrap(),
                    }),
                ],
                consume_signals: vec![consumed_signal_id.clone()],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    // The retried send of the consumed id must stay deduplicated instead of
    // delivering a second time to the next run.
    let retried = client
        .signal_workflow(
            "wf/can-signal-dedup",
            "go",
            consumed_signal_id.0.clone(),
            "first",
        )
        .await
        .unwrap();
    assert_eq!(retried, durust::SignalWorkflowOutcome::Duplicate);

    // The continued run is live under the same workflow id: fresh sends land.
    let fresh = client
        .signal_workflow("wf/can-signal-dedup", "go", "signal/can-dedup/next", "next")
        .await
        .unwrap();
    assert_eq!(fresh, durust::SignalWorkflowOutcome::Accepted);
}

/// Late completion and failure of an activity whose run was cancelled must be
/// idempotently absorbed, and repeated retries must keep returning the same
/// outcome on every provider (cancellation deletes the run's activity rows
/// atomically with the terminal transition; absence answers late calls).
async fn late_activity_completion_after_cancel_is_idempotent_across_retries<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(
            "wf/late-completion-cancel",
            "late-completion-workflows",
            input(5),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("late-completion-scheduler"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("late-completion-workflows"),
                registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_payload = durust::encode_payload(&Input { value: 9 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("late-completion-activities"),
        retry_policy: durust::RetryPolicy::exponential().max_attempts(2),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: input_payload.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input_payload),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                    scheduled.clone(),
                ))],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    let activity = backend
        .claim_activity_task(
            WorkerId::new("late-completion-worker"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("late-completion-activities"),
                registered_activity_names: vec![ActivityName::new("conformance.echo")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("activity task");

    client
        .cancel_workflow("wf/late-completion-cancel", "late completion test")
        .await
        .unwrap();

    // Retried completions and failures after cancellation must return the
    // same idempotent outcome every time; a provider that mutates before
    // validating would flip between error kinds across retries.
    for attempt in 0..2 {
        let completed = backend
            .complete_activity(CompleteActivityRequest {
                claim: activity.claim.clone(),
                result: durust::encode_payload(&9_u64).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            completed,
            durust::CompleteActivityOutcome::AlreadyCompleted,
            "complete retry {attempt}"
        );
        let failed = backend
            .fail_activity(FailActivityRequest {
                claim: activity.claim.clone(),
                failure: durust::DurableFailure::new("test.late", "late failure"),
            })
            .await
            .unwrap();
        assert_eq!(
            failed,
            durust::FailActivityOutcome::AlreadyCompleted,
            "fail retry {attempt}"
        );
    }

    // The cancelled run's history is untouched by the late attempts.
    let history = stream_history(&backend, run_id).await;
    assert!(matches!(
        history.last().map(|event| &event.data),
        Some(HistoryEventData::WorkflowCancelled { .. })
    ));
}

async fn timer_waits_fire_only_when_due_and_make_workflow_claimable<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/timer-wait", "timer-workflows", input(5))
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
        .start_workflow::<workflow>("wf/activity-retry", "retry-workflows", input(5))
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
        heartbeat_timeout: None,
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
        .start_workflow::<workflow>(
            "wf/activity-non-retryable",
            "non-retryable-workflows",
            input(5),
        )
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
        heartbeat_timeout: None,
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
        .start_workflow::<workflow>("wf/activity-timeout", "timeout-workflows", input(5))
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
        start_to_close_timeout: Some(Duration::from_secs(1)),
        heartbeat_timeout: None,
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
            now: durust::TimestampMs(after_schedule.0.saturating_add(100)),
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
            now: durust::TimestampMs(after_schedule.0.saturating_add(1_200)),
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
            now: durust::TimestampMs(after_schedule.0.saturating_add(2_400)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(final_timeout.timed_out, 1);
    let duplicate_timeout = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_schedule.0.saturating_add(2_500)),
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

async fn activity_heartbeat_extends_deadline_and_rejects_stale_claim<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, claim_opts, activity_opts) = schedule_heartbeat_activity(
        backend.clone(),
        "wf/activity-heartbeat-extend",
        "heartbeat-extend-workflows",
        "heartbeat-extend-activities",
        durust::RetryPolicy::exponential().max_attempts(1),
    )
    .await;

    let activity = backend
        .claim_activity_task(WorkerId::new("heartbeat-worker-1"), activity_opts)
        .await
        .unwrap()
        .expect("heartbeat attempt");
    assert_eq!(activity.task.attempt, 1);

    let claimed_at = backend.current_time().await.unwrap();
    let early = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(claimed_at.0.saturating_add(100)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(early.timed_out, 0);

    let recorded = backend
        .heartbeat_activity(durust::ActivityHeartbeatRequest {
            claim: activity.claim.clone(),
        })
        .await
        .unwrap();
    assert_eq!(recorded, durust::ActivityHeartbeatOutcome::Recorded);

    let mut stale_claim = activity.claim.clone();
    stale_claim.token = stale_claim.token.saturating_add(1);
    let stale = backend
        .heartbeat_activity(durust::ActivityHeartbeatRequest { claim: stale_claim })
        .await
        .unwrap_err();
    assert!(matches!(stale, Error::StaleLease));

    let heartbeat_at = backend.current_time().await.unwrap();
    let still_early = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(heartbeat_at.0.saturating_add(100)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(still_early.timed_out, 0);

    let final_timeout = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(heartbeat_at.0.saturating_add(500)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(final_timeout.timed_out, 1);

    let ready = backend
        .claim_workflow_task(WorkerId::new("heartbeat-ready"), claim_opts)
        .await
        .unwrap()
        .expect("heartbeat timeout workflow task");
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
    let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
        panic!("expected final ActivityTimedOut event");
    };
    assert!(timed_out.message.contains("missed heartbeat on attempt 1"));
}

async fn activity_heartbeat_timeout_retries_until_max_attempts_then_wakes_workflow<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, claim_opts, activity_opts) = schedule_heartbeat_activity(
        backend.clone(),
        "wf/activity-heartbeat-retry",
        "heartbeat-retry-workflows",
        "heartbeat-retry-activities",
        durust::RetryPolicy::exponential().max_attempts(2),
    )
    .await;

    let first = backend
        .claim_activity_task(
            WorkerId::new("heartbeat-retry-worker-1"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first heartbeat attempt");
    assert_eq!(first.task.attempt, 1);
    let first_claimed_at = backend.current_time().await.unwrap();
    let retry = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(first_claimed_at.0.saturating_add(500)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(retry.timed_out, 1);
    let not_ready = backend
        .claim_workflow_task(
            WorkerId::new("heartbeat-retry-not-ready"),
            claim_opts.clone(),
        )
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
        .claim_activity_task(WorkerId::new("heartbeat-retry-worker-2"), activity_opts)
        .await
        .unwrap()
        .expect("second heartbeat attempt");
    assert_eq!(second.task.attempt, 2);
    let second_claimed_at = backend.current_time().await.unwrap();
    let final_timeout = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(second_claimed_at.0.saturating_add(500)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(final_timeout.timed_out, 1);

    let ready = backend
        .claim_workflow_task(WorkerId::new("heartbeat-retry-ready"), claim_opts)
        .await
        .unwrap()
        .expect("heartbeat timeout workflow task");
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
    let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
        panic!("expected final ActivityTimedOut event");
    };
    assert!(timed_out.message.contains("missed heartbeat on attempt 2"));
}

async fn schedule_heartbeat_activity<B>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    activity_queue: &str,
    retry_policy: durust::RetryPolicy,
) -> (
    durust::RunId,
    ClaimWorkflowTaskOptions,
    ClaimActivityOptions,
)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(workflow_id, workflow_queue, input(5))
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(workflow_queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("heartbeat-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new(activity_queue),
        retry_policy,
        start_to_close_timeout: Some(Duration::from_secs(10)),
        heartbeat_timeout: Some(Duration::from_millis(200)),
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-heartbeat-options".to_owned(),
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
    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(activity_queue),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    (run_id, claim_opts, activity_opts)
}

async fn unexpired_workflow_claim_lease_is_not_reclaimable<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/lease-unexpired", "lease-unexpired-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("lease-unexpired-workflows"),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("lease-unexpired-holder"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");

    // The holder's lease is nowhere near expiry, so the task must not be
    // handed out to another worker.
    let blocked = backend
        .claim_workflow_task(WorkerId::new("lease-unexpired-thief"), claim_opts.clone())
        .await
        .unwrap();
    assert!(blocked.is_none());

    // Releasing the claim restores normal claimability.
    backend
        .release_workflow_task(
            claimed.claim,
            durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::CacheEvicted),
        )
        .await
        .unwrap();
    let reclaimed = backend
        .claim_workflow_task(WorkerId::new("lease-unexpired-after-release"), claim_opts)
        .await
        .unwrap();
    assert!(reclaimed.is_some());
}

// Claims a workflow task, "crashes" the holder (drops it without commit or
// release), advances time past the lease, and verifies the task is reclaimed
// with the identical state a fresh claim would produce while every operation
// from the dead holder is fenced as stale. `crash` lets SQLite close and
// reopen the database between claim and reclaim; `advance_past_lease` is
// virtual time for the memory provider and a real sleep for SQL providers,
// whose claim scans read their own wall clock.
async fn workflow_lease_expiry_reclaims_and_fences_stale_holder<
    B,
    Crash,
    CrashFut,
    Advance,
    AdvanceFut,
>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    lease_duration: Duration,
    crash: Crash,
    advance_past_lease: Advance,
) where
    B: DurableBackend,
    Crash: FnOnce(B) -> CrashFut,
    CrashFut: Future<Output = B>,
    Advance: FnOnce() -> AdvanceFut,
    AdvanceFut: Future<Output = ()>,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(workflow_id, workflow_queue, input(5))
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(workflow_queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration,
    };
    let original = backend
        .claim_workflow_task(WorkerId::new("lease-crash-holder"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");

    let backend = crash(backend).await;
    advance_past_lease().await;

    let reclaimed = backend
        .claim_workflow_task(WorkerId::new("lease-crash-reclaimer"), claim_opts)
        .await
        .unwrap()
        .expect("expired lease should be reclaimable");
    // The reclaim must look exactly like a fresh claim of the same task,
    // under a new fencing token.
    assert_eq!(reclaimed.run_id, run_id);
    assert_eq!(reclaimed.reason, original.reason);
    assert_eq!(
        reclaimed.replay_target_event_id,
        original.replay_target_event_id
    );
    assert_eq!(
        reclaimed
            .prefetched_history
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
        original
            .prefetched_history
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
    );
    assert_ne!(reclaimed.claim.token, original.claim.token);

    // The dead holder is fenced: its commit and release are rejected as stale
    // and leave the new claim untouched.
    let stale_commit = backend
        .commit_workflow_task(
            original.claim.clone(),
            WorkflowTaskCommit {
                expected_tail_event_id: original.replay_target_event_id,
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(stale_commit, Error::StaleLease));
    let stale_release = backend
        .release_workflow_task(
            original.claim,
            durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::CacheEvicted),
        )
        .await
        .unwrap_err();
    assert!(matches!(stale_release, Error::StaleLease));

    let outcome = backend
        .commit_workflow_task(
            reclaimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: reclaimed.replay_target_event_id,
                append_events: vec![NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
                    result: durust::encode_payload(&5_u64).unwrap(),
                })],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(outcome, CommitOutcome::Committed { .. }));
}

async fn schedule_timeoutless_activity<B>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    activity_queue: &str,
) -> (
    durust::RunId,
    ClaimWorkflowTaskOptions,
    ClaimActivityOptions,
)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(workflow_id, workflow_queue, input(5))
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(workflow_queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("timeoutless-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new(activity_queue),
        retry_policy: durust::RetryPolicy::exponential().max_attempts(2),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-timeoutless-options".to_owned(),
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
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(activity_queue),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    (run_id, claim_opts, activity_opts)
}

// An activity with neither a start-to-close timeout nor a heartbeat timeout
// (the `ActivityOptions` defaults) must be reclaimable after its claim lease
// expires, through the same timeout/retry path explicit deadlines use.
async fn timeoutless_activity_lease_expiry_reclaims_and_fences_stale_holder<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, claim_opts, activity_opts) = schedule_timeoutless_activity(
        backend.clone(),
        "wf/timeoutless-activity-lease",
        "timeoutless-lease-workflows",
        "timeoutless-lease-activities",
    )
    .await;

    let first = backend
        .claim_activity_task(WorkerId::new("timeoutless-worker-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(first.task.attempt, 1);
    let claimed_at = backend.current_time().await.unwrap();

    // While the lease is unexpired the timeout scan leaves the claim alone and
    // the task is not claimable by anyone else.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(claimed_at.0.saturating_add(100)),
            limit: 16,
        })
        .await
        .unwrap();
    let blocked = backend
        .claim_activity_task(
            WorkerId::new("timeoutless-worker-blocked"),
            activity_opts.clone(),
        )
        .await
        .unwrap();
    assert!(blocked.is_none());

    // Past the 30s claim lease the timeout scan reclaims the task through the
    // existing retry machinery, making it claimable as attempt 2.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(claimed_at.0.saturating_add(31_000)),
            limit: 16,
        })
        .await
        .unwrap();
    let second = backend
        .claim_activity_task(WorkerId::new("timeoutless-worker-2"), activity_opts)
        .await
        .unwrap()
        .expect("lease-expired activity should be reclaimable");
    assert_eq!(second.task.attempt, 2);

    // Every operation from the crashed holder is fenced as stale.
    let stale_heartbeat = backend
        .heartbeat_activity(durust::ActivityHeartbeatRequest {
            claim: first.claim.clone(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_heartbeat, Error::StaleLease));
    let stale_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim.clone(),
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_completion, Error::StaleLease));
    let stale_failure = backend
        .fail_activity(FailActivityRequest {
            claim: first.claim,
            failure: durust::DurableFailure::new("conformance.crashed", "stale holder"),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_failure, Error::StaleLease));

    // The new holder completes normally and the workflow wakes up.
    let completed = backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap();
    assert!(matches!(
        completed,
        durust::CompleteActivityOutcome::Completed { .. }
    ));
    let ready = backend
        .claim_workflow_task(WorkerId::new("timeoutless-ready"), claim_opts)
        .await
        .unwrap()
        .expect("activity completion workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityCompleted);
    assert_eq!(ready.run_id, run_id);
}

async fn cancel_commands_clear_activity_tasks<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/cancel-command", "cancel-command-workflows", input(5))
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
        heartbeat_timeout: None,
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
                schedule_child_workflow_maps: Vec::new(),
                start_child_workflows: Vec::new(),
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

async fn child_start_dispatch_is_idempotent_and_wakes_parent<B>(backend: B)
where
    B: DurableBackend,
{
    let (parent_run_id, command_id) = schedule_child_start(
        backend.clone(),
        "wf/child-dispatch-parent",
        "wf/child-dispatch-child",
        durust::ParentClosePolicy::Cancel,
    )
    .await;

    let dispatched = backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();
    assert!(
        dispatched.dispatched <= 1,
        "providers may dispatch one queued child start or inline it during commit"
    );
    let duplicate = backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(duplicate.dispatched, 0);

    let parent_ready = backend
        .claim_workflow_task(
            WorkerId::new("child-start-parent-ready"),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent woken by child start");
    assert_eq!(
        parent_ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowStarted
    );

    let child_ready = backend
        .claim_workflow_task(
            WorkerId::new("child-start-child-ready"),
            workflow_claim_opts("child-workflows"),
        )
        .await
        .unwrap()
        .expect("child workflow started");
    assert_eq!(
        child_ready.workflow_id,
        durust::WorkflowId::new("wf/child-dispatch-child")
    );

    let history = stream_history(&backend, parent_run_id).await;
    assert!(history.iter().any(|event| matches!(
        &event.data,
        HistoryEventData::ChildWorkflowStarted(started)
            if started.command_id == command_id
    )));
}

async fn child_completion_routes_to_parent<B>(backend: B)
where
    B: DurableBackend,
{
    let (parent_run_id, command_id) = schedule_child_start(
        backend.clone(),
        "wf/child-completion-parent",
        "wf/child-completion-child",
        durust::ParentClosePolicy::Cancel,
    )
    .await;
    backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();
    let child = backend
        .claim_workflow_task(
            WorkerId::new("child-completion-worker"),
            workflow_claim_opts("child-workflows"),
        )
        .await
        .unwrap()
        .expect("child workflow task");
    let result = durust::encode_payload(&99_u64).unwrap();
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCompleted {
                        result: result.clone(),
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

    let parent_ready = backend
        .claim_workflow_task(
            WorkerId::new("child-completion-parent-ready"),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent woken by child completion");
    assert_eq!(
        parent_ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowCompleted
    );
    let history = stream_history(&backend, parent_run_id).await;
    let completed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowCompleted(completed)
                if completed.command_id == command_id =>
            {
                Some(completed)
            }
            _ => None,
        })
        .expect("child completion event");
    assert_eq!(
        durust::decode_payload::<u64>(&completed.result).unwrap(),
        99
    );
}

async fn child_start_conflict_records_failure<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>(
            "wf/child-conflict-child",
            "conflict-child-workflows",
            input(1),
        )
        .await
        .unwrap();
    let (parent_run_id, _command_id) = schedule_child_start(
        backend.clone(),
        "wf/child-conflict-parent",
        "wf/child-conflict-child",
        durust::ParentClosePolicy::Cancel,
    )
    .await;
    backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();

    let history = stream_history(&backend, parent_run_id).await;
    let failed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowFailed(failed) => Some(failed),
            _ => None,
        })
        .expect("child start conflict failure");
    assert_eq!(
        failed.failure.error_type,
        "durust.child_workflow_id_conflict"
    );
    assert!(failed.failure.non_retryable);
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("child-conflict-parent-ready"),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap();
    assert!(claimed.is_some());
}

async fn parent_close_policy_cancel_cancels_child<B>(backend: B)
where
    B: DurableBackend,
{
    let (parent_run_id, _command_id) = schedule_child_start(
        backend.clone(),
        "wf/child-cancel-parent",
        "wf/child-cancel-child",
        durust::ParentClosePolicy::Cancel,
    )
    .await;
    backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();
    let parent = backend
        .claim_workflow_task(
            WorkerId::new("child-cancel-parent-ready"),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent ready after child start");
    backend
        .commit_workflow_task(
            parent.claim,
            terminal_parent_commit(parent.replay_target_event_id),
        )
        .await
        .unwrap();

    let child_claim = backend
        .claim_workflow_task(
            WorkerId::new("child-cancel-claim"),
            workflow_claim_opts("child-workflows"),
        )
        .await
        .unwrap();
    assert!(child_claim.is_none());

    let parent_history = stream_history(&backend, parent_run_id).await;
    let child_run_id = parent_history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowStarted(started) => Some(started.run_id.clone()),
            _ => None,
        })
        .expect("child started");
    let child_history = stream_history(&backend, child_run_id).await;
    assert!(
        child_history
            .iter()
            .any(|event| matches!(event.data, HistoryEventData::WorkflowCancelled { .. }))
    );
}

async fn parent_close_policy_abandon_leaves_child_running<B>(backend: B)
where
    B: DurableBackend,
{
    schedule_child_start(
        backend.clone(),
        "wf/child-abandon-parent",
        "wf/child-abandon-child",
        durust::ParentClosePolicy::Abandon,
    )
    .await;
    backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();
    let parent = backend
        .claim_workflow_task(
            WorkerId::new("child-abandon-parent-ready"),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent ready after child start");
    backend
        .commit_workflow_task(
            parent.claim,
            terminal_parent_commit(parent.replay_target_event_id),
        )
        .await
        .unwrap();

    let child_claim = backend
        .claim_workflow_task(
            WorkerId::new("child-abandon-claim"),
            workflow_claim_opts("child-workflows"),
        )
        .await
        .unwrap();
    assert!(child_claim.is_some());
}

async fn schedule_child_start<B>(
    backend: B,
    parent_workflow_id: &str,
    child_workflow_id: &str,
    parent_close_policy: durust::ParentClosePolicy,
) -> (durust::RunId, durust::CommandId)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let parent_run_id = client
        .start_workflow::<workflow>(parent_workflow_id, "child-parent-workflows", input(1))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{parent_workflow_id}-scheduler")),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent workflow task");
    let command_id = durust::command_id(&parent_run_id, 1);
    let input = durust::encode_payload(&7_u64).unwrap();
    let workflow_type = durust::WorkflowType::new("conformance.workflow", 1);
    let workflow_id = durust::WorkflowId::new(child_workflow_id);
    let task_queue = TaskQueue::new("child-workflows");
    let requested = durust::ChildWorkflowStartRequested {
        command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        workflow_id: workflow_id.clone(),
        task_queue: task_queue.clone(),
        input: input.clone(),
        parent_close_policy,
        fingerprint: durust::child_workflow_fingerprint(
            workflow_type,
            workflow_id,
            durust::payload_digest(&input),
            task_queue,
            parent_close_policy,
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
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
        .await
        .unwrap();
    (parent_run_id, command_id)
}

async fn schedule_child_workflow_map<B>(
    backend: B,
    parent_workflow_id: &str,
    parent_queue: &str,
    child_queue: &str,
    workflow_id_prefix: &str,
    failure_mode: durust::ChildWorkflowMapFailureMode,
    parent_close_policy: durust::ParentClosePolicy,
    max_in_flight: usize,
) -> (
    durust::RunId,
    durust::CommandId,
    ClaimWorkflowTaskOptions,
    ClaimWorkflowTaskOptions,
)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let parent_run_id = client
        .start_workflow::<workflow>(parent_workflow_id, parent_queue, input(1))
        .await
        .unwrap();
    let parent_opts = workflow_claim_opts(parent_queue);
    let child_opts = workflow_claim_opts(child_queue);
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{parent_workflow_id}-child-map-scheduler")),
            parent_opts.clone(),
        )
        .await
        .unwrap()
        .expect("parent workflow task");
    let command_id = durust::command_id(&parent_run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        [1_u64, 2, 3]
            .into_iter()
            .map(|value| durust::encode_payload(&value).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let task_queue = TaskQueue::new(child_queue);
    let result_manifest_name = "child-map-results".to_owned();
    let map_task = ChildWorkflowMapTask {
        map_command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        input_manifest: input_manifest.clone(),
        result_manifest_name: result_manifest_name.clone(),
        workflow_id_prefix: workflow_id_prefix.to_owned(),
        max_in_flight,
        parent_close_policy,
        failure_mode,
    };
    let scheduled = durust::ChildWorkflowMapScheduled {
        command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        input_manifest: input_manifest.clone(),
        result_manifest_name: result_manifest_name.clone(),
        workflow_id_prefix: workflow_id_prefix.to_owned(),
        max_in_flight,
        parent_close_policy,
        failure_mode,
        fingerprint: durust::child_workflow_map_fingerprint(
            workflow_type,
            durust::payload_digest(&input_manifest),
            result_manifest_name,
            workflow_id_prefix.to_owned(),
            max_in_flight,
            task_queue,
            parent_close_policy,
            failure_mode,
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ChildWorkflowMapScheduled(scheduled),
                )],
                schedule_child_workflow_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    (parent_run_id, command_id, parent_opts, child_opts)
}

async fn dispatch_child_map_starts<B>(backend: &B)
where
    B: DurableBackend,
{
    for _ in 0..4 {
        let dispatched = backend
            .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
                namespace: Namespace::default(),
                limit: 16,
            })
            .await
            .unwrap();
        if dispatched.dispatched == 0 {
            return;
        }
    }
}

async fn complete_child_run<B>(backend: &B, child: durust::ClaimedWorkflowTask, value: u64)
where
    B: DurableBackend,
{
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: child.replay_target_event_id,
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCompleted {
                        result: durust::encode_payload(&value).unwrap(),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
}

async fn fail_child_run<B>(
    backend: &B,
    child: durust::ClaimedWorkflowTask,
    error_type: &str,
    message: &str,
) where
    B: DurableBackend,
{
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: child.replay_target_event_id,
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowFailed {
                        failure: durust::DurableFailure::new(error_type, message),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
}

fn assert_compact_child_workflow_map_parent_history(history: &[durust::HistoryEvent]) {
    assert!(
        history
            .iter()
            .any(|event| matches!(event.data, HistoryEventData::ChildWorkflowMapScheduled(_))),
        "expected child workflow map scheduled event"
    );
    assert!(
        !history.iter().any(|event| matches!(
            event.data,
            HistoryEventData::ChildWorkflowStarted(_)
                | HistoryEventData::ChildWorkflowCompleted(_)
                | HistoryEventData::ChildWorkflowFailed(_)
                | HistoryEventData::ChildWorkflowCancelled(_)
        )),
        "child workflow map parent history must stay compact"
    );
}

fn workflow_claim_opts(task_queue: &str) -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(task_queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    }
}

fn terminal_parent_commit(expected_tail_event_id: EventId) -> WorkflowTaskCommit {
    WorkflowTaskCommit {
        expected_tail_event_id,
        append_events: vec![durust::NewHistoryEvent::new(
            HistoryEventData::WorkflowCompleted {
                result: durust::encode_payload(&()).unwrap(),
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
    }
}

async fn stream_history<B>(backend: &B, run_id: durust::RunId) -> Vec<durust::HistoryEvent>
where
    B: DurableBackend,
{
    backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events
}

async fn activity_map_materializes_bounded_items_and_writes_result_manifest<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/activity-map", "map-workflows", input(5))
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
        heartbeat_timeout: None,
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
                        heartbeat_timeout: None,
                        input_manifest: input_manifest.clone(),
                        result_manifest_name: "mapped".to_owned(),
                        max_in_flight: 2,
                        fingerprint,
                    }),
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
        .start_workflow::<workflow>("wf/activity-map-failure", "map-failure-workflows", input(5))
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
        heartbeat_timeout: None,
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
                        heartbeat_timeout: None,
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

async fn child_workflow_map_materializes_bounded_children_and_writes_result_manifest<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, _command_id, parent_opts, child_opts) = schedule_child_workflow_map(
        backend.clone(),
        "wf/child-map-success",
        "child-map-success-parent",
        "child-map-success-children",
        "wf/child-map-success/item",
        durust::ChildWorkflowMapFailureMode::FailFast,
        durust::ParentClosePolicy::Cancel,
        2,
    )
    .await;

    dispatch_child_map_starts(&backend).await;
    let first = backend
        .claim_workflow_task(WorkerId::new("child-map-success-0"), child_opts.clone())
        .await
        .unwrap()
        .expect("first child map item");
    let second = backend
        .claim_workflow_task(WorkerId::new("child-map-success-1"), child_opts.clone())
        .await
        .unwrap()
        .expect("second child map item");
    assert_eq!(
        first.workflow_id,
        durust::WorkflowId::new("wf/child-map-success/item/0")
    );
    assert_eq!(
        second.workflow_id,
        durust::WorkflowId::new("wf/child-map-success/item/1")
    );
    let hidden = backend
        .claim_workflow_task(
            WorkerId::new("child-map-success-hidden"),
            child_opts.clone(),
        )
        .await
        .unwrap();
    assert!(hidden.is_none());

    complete_child_run(&backend, first, 10).await;
    dispatch_child_map_starts(&backend).await;
    let third = backend
        .claim_workflow_task(WorkerId::new("child-map-success-2"), child_opts.clone())
        .await
        .unwrap()
        .expect("third child map item after one completion");
    assert_eq!(
        third.workflow_id,
        durust::WorkflowId::new("wf/child-map-success/item/2")
    );

    complete_child_run(&backend, third, 30).await;
    let not_ready = backend
        .claim_workflow_task(
            WorkerId::new("child-map-success-parent-not-ready"),
            parent_opts.clone(),
        )
        .await
        .unwrap();
    assert!(not_ready.is_none());
    complete_child_run(&backend, second, 20).await;

    let ready = backend
        .claim_workflow_task(WorkerId::new("child-map-success-parent-ready"), parent_opts)
        .await
        .unwrap()
        .expect("parent ready after child map completion");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowMapCompleted
    );

    let history = stream_history(&backend, run_id).await;
    assert_compact_child_workflow_map_parent_history(&history);
    let completed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowMapCompleted(completed) => Some(completed),
            _ => None,
        })
        .expect("child workflow map completed event");
    assert_eq!(completed.item_count, 3);
    assert_eq!(completed.success_count, 3);
    assert_eq!(completed.failure_count, 0);
    assert_eq!(completed.cancellation_count, 0);
    let refs = durust::decode_child_workflow_map_success_refs(&completed.result_manifest).unwrap();
    let values = refs
        .iter()
        .map(|payload| durust::decode_payload::<u64>(payload).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values, vec![10, 20, 30]);
}

async fn child_workflow_map_fail_fast_cancels_in_flight_children<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, _command_id, parent_opts, child_opts) = schedule_child_workflow_map(
        backend.clone(),
        "wf/child-map-fail-fast",
        "child-map-fail-fast-parent",
        "child-map-fail-fast-children",
        "wf/child-map-fail-fast/item",
        durust::ChildWorkflowMapFailureMode::FailFast,
        durust::ParentClosePolicy::Cancel,
        2,
    )
    .await;

    dispatch_child_map_starts(&backend).await;
    let first = backend
        .claim_workflow_task(WorkerId::new("child-map-fail-fast-0"), child_opts.clone())
        .await
        .unwrap()
        .expect("first child map item");
    let second = backend
        .claim_workflow_task(WorkerId::new("child-map-fail-fast-1"), child_opts.clone())
        .await
        .unwrap()
        .expect("second child map item");

    fail_child_run(&backend, first, "test.child_failed", "child failed").await;
    let ready = backend
        .claim_workflow_task(
            WorkerId::new("child-map-fail-fast-parent-ready"),
            parent_opts,
        )
        .await
        .unwrap()
        .expect("parent ready after child map failure");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowMapFailed
    );

    let no_more_children = backend
        .claim_workflow_task(WorkerId::new("child-map-fail-fast-no-more"), child_opts)
        .await
        .unwrap();
    assert!(no_more_children.is_none());
    let second_history = stream_history(&backend, second.run_id).await;
    assert!(
        second_history
            .iter()
            .any(|event| matches!(event.data, HistoryEventData::WorkflowCancelled { .. }))
    );

    let history = stream_history(&backend, run_id).await;
    assert_compact_child_workflow_map_parent_history(&history);
    let failed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowMapFailed(failed) => Some(failed),
            _ => None,
        })
        .expect("child workflow map failed event");
    assert_eq!(failed.failure.error_type, "test.child_failed");
}

async fn child_workflow_map_collect_all_records_ordered_outcomes<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, _command_id, parent_opts, child_opts) = schedule_child_workflow_map(
        backend.clone(),
        "wf/child-map-collect-all",
        "child-map-collect-all-parent",
        "child-map-collect-all-children",
        "wf/child-map-collect-all/item",
        durust::ChildWorkflowMapFailureMode::CollectAll,
        durust::ParentClosePolicy::Cancel,
        2,
    )
    .await;

    dispatch_child_map_starts(&backend).await;
    let first = backend
        .claim_workflow_task(WorkerId::new("child-map-collect-all-0"), child_opts.clone())
        .await
        .unwrap()
        .expect("first child map item");
    let second = backend
        .claim_workflow_task(WorkerId::new("child-map-collect-all-1"), child_opts.clone())
        .await
        .unwrap()
        .expect("second child map item");
    fail_child_run(&backend, first, "test.collect_failed", "collect failure").await;
    dispatch_child_map_starts(&backend).await;
    let third = backend
        .claim_workflow_task(WorkerId::new("child-map-collect-all-2"), child_opts)
        .await
        .unwrap()
        .expect("third collect-all child map item");
    complete_child_run(&backend, second, 20).await;
    complete_child_run(&backend, third, 30).await;

    let ready = backend
        .claim_workflow_task(
            WorkerId::new("child-map-collect-all-parent-ready"),
            parent_opts,
        )
        .await
        .unwrap()
        .expect("parent ready after collect-all child map completion");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowMapCompleted
    );

    let history = stream_history(&backend, run_id).await;
    assert_compact_child_workflow_map_parent_history(&history);
    let completed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowMapCompleted(completed) => Some(completed),
            _ => None,
        })
        .expect("collect-all child workflow map completed event");
    assert_eq!(completed.item_count, 3);
    assert_eq!(completed.success_count, 2);
    assert_eq!(completed.failure_count, 1);
    assert_eq!(completed.cancellation_count, 0);
    let outcomes = durust::decode_child_workflow_map_outcomes(&completed.result_manifest).unwrap();
    assert_eq!(outcomes.len(), 3);
    match &outcomes[0] {
        durust::ChildWorkflowMapItemOutcome::Failed { failure } => {
            assert_eq!(failure.error_type, "test.collect_failed");
        }
        other => panic!("expected failed first outcome, got {other:?}"),
    }
    match &outcomes[1] {
        durust::ChildWorkflowMapItemOutcome::Succeeded { result } => {
            assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 20);
        }
        other => panic!("expected successful second outcome, got {other:?}"),
    }
    match &outcomes[2] {
        durust::ChildWorkflowMapItemOutcome::Succeeded { result } => {
            assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 30);
        }
        other => panic!("expected successful third outcome, got {other:?}"),
    }
}

async fn workflow_cancel_cleans_waits_activities_and_activity_maps<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/cancel-cleanup", "cancel-workflows", input(5))
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
        heartbeat_timeout: None,
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
        heartbeat_timeout: None,
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
                            heartbeat_timeout: None,
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
        .start_workflow::<workflow>("wf/activity-filter", "activity-workflows", input(4))
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

async fn batch_activity_completion_reports_ordered_duplicate_and_stale_results<B>(backend: B)
where
    B: DurableBackend,
{
    let workflow_type = WorkflowType::new("conformance.batch-activity", 1);
    let workflow_queue = TaskQueue::new("batch-activity-workflows");
    let activity_queue = TaskQueue::new("batch-activity-activities");
    let activity_name = ActivityName::new("conformance.echo");
    let started = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new("wf/batch-activity-completion"),
            workflow_type: workflow_type.clone(),
            task_queue: workflow_queue.clone(),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap();
    let run_id = started.run_id().clone();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: workflow_queue,
        registered_workflow_types: vec![workflow_type],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("batch-activity-scheduler"), claim_opts)
        .await
        .unwrap()
        .expect("workflow task");
    let schedules = (0..2_u64)
        .map(|index| {
            let input = durust::encode_payload(&Input { value: index }).unwrap();
            durust::ActivityScheduled {
                command_id: durust::CommandId {
                    run_id: run_id.clone(),
                    seq: durust::CommandSeq(index + 1),
                },
                activity_name: activity_name.clone(),
                task_queue: activity_queue.clone(),
                retry_policy: durust::RetryPolicy::none(),
                start_to_close_timeout: Some(Duration::from_secs(30)),
                heartbeat_timeout: None,
                fingerprint: durust::activity_fingerprint(
                    activity_name.clone(),
                    durust::payload_digest(&input),
                    format!("batch-activity-{index}"),
                ),
                input,
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: claimed.replay_target_event_id,
                    append_events: schedules
                        .iter()
                        .cloned()
                        .map(
                            |scheduled| NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                                scheduled,
                            ))
                        )
                        .collect(),
                    schedule_activities: schedules
                        .iter()
                        .map(durust::ActivityTask::from_scheduled)
                        .collect(),
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap(),
        CommitOutcome::Committed {
            new_tail_event_id: EventId(3)
        }
    );

    let mut claimed_activities = backend
        .claim_activity_tasks(
            WorkerId::new("batch-activity-worker"),
            ClaimActivityTasksOptions {
                claim: ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: activity_queue,
                    registered_activity_names: vec![activity_name],
                    lease_duration: Duration::from_secs(30),
                },
                limit: 2,
            },
        )
        .await
        .unwrap();
    claimed_activities
        .sort_by(|left, right| left.task.activity_id.0.cmp(&right.task.activity_id.0));
    assert_eq!(claimed_activities.len(), 2);

    assert_eq!(
        backend
            .complete_activity(CompleteActivityRequest {
                claim: claimed_activities[0].claim.clone(),
                result: durust::encode_payload(&1_u64).unwrap(),
            })
            .await
            .unwrap(),
        durust::CompleteActivityOutcome::Completed {
            event_id: EventId(4)
        }
    );

    let mut stale_claim = claimed_activities[1].claim.clone();
    stale_claim.token = stale_claim.token.saturating_add(1);
    let results = backend
        .complete_activity_tasks(CompleteActivityTasksRequest {
            completions: vec![
                CompleteActivityRequest {
                    claim: claimed_activities[0].claim.clone(),
                    result: durust::encode_payload(&10_u64).unwrap(),
                },
                CompleteActivityRequest {
                    claim: stale_claim,
                    result: durust::encode_payload(&20_u64).unwrap(),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].result.as_ref().unwrap(),
        &durust::CompleteActivityOutcome::AlreadyCompleted
    );
    assert!(matches!(results[1].result, Err(Error::StaleLease)));

    assert_eq!(
        backend
            .complete_activity(CompleteActivityRequest {
                claim: claimed_activities[1].claim.clone(),
                result: durust::encode_payload(&2_u64).unwrap(),
            })
            .await
            .unwrap(),
        durust::CompleteActivityOutcome::Completed {
            event_id: EventId(5)
        }
    );
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
