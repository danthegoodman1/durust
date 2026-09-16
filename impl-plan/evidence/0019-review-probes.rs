// Review probes for 46091e5. These assert the observed defect, not the fix.
use durust::provider::*;
use durust::*;
use futures::{executor::block_on, future::BoxFuture};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, UNIX_EPOCH},
};

fn request(id: &str, input: PayloadRef) -> StartWorkflowRequest {
    StartWorkflowRequest {
        namespace: Namespace::default(),
        workflow_id: WorkflowId::new(id),
        workflow_type: WorkflowType::new("review.probe", 1),
        task_queue: TaskQueue::new("workflows"),
        input,
    }
}

#[derive(Clone)]
struct CommitInsideDelete {
    store: LocalDirectoryBlobStore,
    backend: MemoryBackend,
    pending: Arc<Mutex<Option<StartWorkflowRequest>>>,
    committed: Arc<Mutex<Option<RunId>>>,
}
impl PayloadBlobStore for CommitInsideDelete {
    fn put_payload_blob(
        &self,
        digest: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<String>> {
        self.store.put_payload_blob(digest, bytes)
    }
    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, Result<Vec<u8>>> {
        self.store.get_payload_blob(digest)
    }
    fn payload_blob_exists(&self, digest: String) -> BoxFuture<'static, Result<bool>> {
        self.store.payload_blob_exists(digest)
    }
    fn list_payload_blobs(&self) -> BoxFuture<'static, Result<BTreeMap<String, TimestampMs>>> {
        self.store.list_payload_blobs()
    }
    fn payload_blob_last_modified(
        &self,
        digest: String,
    ) -> BoxFuture<'static, Result<Option<TimestampMs>>> {
        self.store.payload_blob_last_modified(digest)
    }
    fn owns_payload_blob_uri(&self, uri: &str) -> bool {
        self.store.owns_payload_blob_uri(uri)
    }
    fn delete_payload_blob(&self, digest: String) -> BoxFuture<'static, Result<()>> {
        let this = self.clone();
        Box::pin(async move {
            let req = this.pending.lock().unwrap().take();
            if let Some(req) = req {
                let writer = PayloadBackend::with_payload_storage(
                    this.backend.clone(),
                    this.store.clone(),
                    PayloadStorageConfig::new().inline_threshold_bytes(1),
                );
                let outcome = writer.start_workflow(req).await?;
                *this.committed.lock().unwrap() = Some(outcome.run_id().clone());
            }
            this.store.delete_payload_blob(digest).await
        })
    }
}

#[test]
fn gc_can_delete_a_blob_committed_after_its_final_probe() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalDirectoryBlobStore::new(dir.path(), "");
        let payload = encode_payload(&vec![7_u8; 256]).unwrap();
        let PayloadRef::Inline { bytes, .. } = &payload else {
            unreachable!()
        };
        let digest = digest_bytes(bytes);
        store
            .put_payload_blob(digest.clone(), bytes.clone())
            .await
            .unwrap();
        std::fs::File::options()
            .write(true)
            .open(dir.path().join(&digest))
            .unwrap()
            .set_modified(UNIX_EPOCH)
            .unwrap();
        let inner = MemoryBackend::new();
        let committed = Arc::new(Mutex::new(None));
        let wrapped_store = CommitInsideDelete {
            store: store.clone(),
            backend: inner.clone(),
            pending: Arc::new(Mutex::new(Some(request("gc-race", payload)))),
            committed: committed.clone(),
        };
        let gc = PayloadBackend::new(inner.clone(), wrapped_store);
        let outcome = gc
            .gc_payload_blobs(PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        let run_id = committed.lock().unwrap().clone().unwrap();
        let raw = inner
            .stream_history_for_replay(StreamHistoryRequest {
                run_id: run_id.clone(),
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(1),
                max_events: 1,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap();
        assert_eq!(raw.events.len(), 1, "the competing start committed");
        let reader = PayloadBackend::new(inner, store.clone());
        let result = reader
            .stream_history(StreamHistoryRequest {
                run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(1),
                max_events: 1,
                max_bytes: usize::MAX,
            })
            .await;
        assert_eq!(outcome.deleted_blobs, 1);
        assert!(!store.payload_blob_exists(digest).await.unwrap());
        assert!(result.is_err());
        eprintln!(
            "GC race: committed WorkflowStarted survives, its input blob does not; read = {result:?}"
        );
    });
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    value: u64,
}
#[durust::activity(name = "review.double")]
async fn double(input: Input) -> Result<u64> {
    let value = input.value * 2;
    Ok(value)
}
#[durust::workflow(name = "review.recovery", version = 1)]
async fn recovery(input: Input) -> Result<u64> {
    let value = durust::call_activity!(double(input)).await?;
    Ok(value)
}

#[test]
fn finite_recovery_budget_repeats_the_same_prefix_forever() {
    block_on(async {
        let backend = MemoryBackend::new();
        let run_id = Client::new(backend.clone())
            .start_workflow::<recovery>("recovery", "workflows", Input { value: 3 })
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(recovery)
            .register_activity(double)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());
        assert!(worker.run_activity_once().await.unwrap());
        drop(worker);
        let mut bounded = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(recovery)
            .recovery_replay_event_budget(2)
            .recovery_defer_delay(Duration::from_millis(1))
            .build();
        for _ in 0..10 {
            assert!(bounded.run_workflow_once().await.unwrap());
            backend.advance_time(Duration::from_millis(2));
        }
        let req = StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        };
        let stalled = backend.stream_history(req.clone()).await.unwrap();
        assert_eq!(stalled.events.len(), 3);
        assert_eq!(bounded.metrics().workflow_tasks_committed, 0);
        let mut unbounded = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(recovery)
            .build();
        assert!(unbounded.run_workflow_once().await.unwrap());
        let resumed = backend.stream_history(req).await.unwrap();
        assert!(matches!(
            resumed.events.last().unwrap().data,
            HistoryEventData::WorkflowCompleted { .. }
        ));
        eprintln!(
            "Recovery budget: 10 eligible retries, 0 commits, history length 3; unbounded control completes immediately"
        );
    });
}

async fn failed_map_commit<B: DurableBackend>(backend: B) -> (usize, bool) {
    let run_id = backend
        .start_workflow(request("map", encode_payload(&Input { value: 1 }).unwrap()))
        .await
        .unwrap()
        .run_id()
        .clone();
    let claim = backend
        .claim_workflow_task(
            WorkerId::new("probe"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("workflows"),
                registered_workflow_types: vec![WorkflowType::new("review.probe", 1)],
                lease_duration: Duration::from_secs(30),
                shard_filter: None,
            },
        )
        .await
        .unwrap()
        .unwrap()
        .claim;
    let command_id = CommandId {
        run_id: run_id.clone(),
        seq: CommandSeq(1),
    };
    let manifest = encode_payload(&ActivityMapInputManifest {
        item_count: 1,
        page_lengths: vec![1],
        pages: vec![],
    })
    .unwrap();
    let scheduled = ActivityMapScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("review.double"),
        task_queue: TaskQueue::new("default"),
        retry_policy: RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: manifest.clone(),
        result_manifest_name: "results".into(),
        max_in_flight: 1,
        fingerprint: CommandFingerprint {
            kind: CommandKind::ActivityMap,
            name: "review.double".into(),
            input_digest: None,
            options_digest: "probe".into(),
        },
    };
    let commit = WorkflowTaskCommit {
        append_events: vec![NewHistoryEvent {
            data: HistoryEventData::ActivityMapScheduled(scheduled),
        }],
        schedule_activity_maps: vec![ActivityMapTask {
            map_command_id: command_id,
            activity_name: ActivityName::new("review.double"),
            task_queue: TaskQueue::new("default"),
            retry_policy: RetryPolicy::none(),
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            input_manifest: manifest,
            result_manifest_name: "results".into(),
            max_in_flight: 1,
        }],
        ..Default::default()
    };
    let failure = backend
        .commit_workflow_task(claim.clone(), commit)
        .await
        .unwrap_err();
    let history = backend
        .stream_history(StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap();
    let held = backend
        .release_workflow_task(claim, WorkflowTaskRelease::immediate())
        .await
        .is_ok();
    eprintln!(
        "Failed map: error = {failure}, history length = {}, original claim held = {held}",
        history.events.len()
    );
    (history.events.len(), held)
}

#[test]
fn failed_memory_map_commit_changes_history_and_loses_claim() {
    block_on(async {
        assert_eq!(failed_map_commit(MemoryBackend::new()).await, (2, false));
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            failed_map_commit(SqliteBackend::open(dir.path().join("probe.sqlite")).unwrap()).await,
            (1, true)
        );
    });
}
