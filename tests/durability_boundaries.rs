//! Regression tests for publication, commit atomicity, and recovery boundaries.
use durust::provider::*;
use durust::*;
use futures::{executor::block_on, future::BoxFuture};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
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
fn online_gc_refuses_before_the_delete_commit_race() {
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
        let error = gc
            .gc_payload_blobs(PayloadGarbageCollectionRequest::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("quiescent"), "{error}");
        assert!(
            committed.lock().unwrap().is_none(),
            "collector must refuse before invoking a delete"
        );
        assert!(store.payload_blob_exists(digest).await.unwrap());
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
fn finite_recovery_quanta_complete_even_when_smaller_than_one_event() {
    for limit in [0, 1, 2] {
        block_on(async {
            let backend = MemoryBackend::new();
            let run_id = Client::new(backend.clone())
                .start_workflow::<recovery>("recovery", "workflows", Input { value: 3 })
                .await
                .unwrap();
            let mut recorder = Worker::builder(backend.clone())
                .workflow_task_queue("workflows")
                .register_workflow(recovery)
                .register_activity(double)
                .build();
            assert!(recorder.run_workflow_once().await.unwrap());
            assert!(recorder.run_activity_once().await.unwrap());
            drop(recorder);
            let mut bounded = Worker::builder(backend.clone())
                .workflow_task_queue("workflows")
                .register_workflow(recovery)
                .history_chunk_events(1)
                .recovery_replay_event_budget(limit)
                .recovery_replay_byte_budget(limit)
                .recovery_prefetch_chunks(limit)
                .build();
            assert!(bounded.run_workflow_once().await.unwrap());
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
            assert!(
                matches!(
                    history.events.last().unwrap().data,
                    HistoryEventData::WorkflowCompleted { .. }
                ),
                "quantum={limit}"
            );
            assert_eq!(bounded.metrics().workflow_tasks_committed, 1);
            assert_eq!(bounded.metrics().workflow_tasks_deferred, 0);
        });
    }
}

#[durust::workflow(name = "review.cold-markers", version = 1)]
async fn cold_markers(_: Input) -> Result<()> {
    for i in 0..100 {
        durust::get_version(format!("m{i}"), 1, 1).await?;
    }
    durust::signal::<()>("go").await?;
    Ok(())
}

#[durust::workflow(name = "review.hot-signal", version = 1)]
async fn hot_signal(_: Input) -> Result<()> {
    durust::signal::<()>("go").await?;
    Ok(())
}

#[test]
fn cached_commits_do_not_wait_for_cold_recovery_or_a_full_commit_batch() {
    use std::{
        future::Future,
        task::{Context, Waker},
    };
    for commit_batch_size in [1, 2, 128] {
        for hot_first in [false, true] {
            block_on(async {
                let backend = MemoryBackend::new();
                let client = Client::new(backend.clone());
                let mut worker = Worker::builder(backend.clone())
                    .workflow_task_queue("workflows")
                    .history_chunk_events(1)
                    .recovery_replay_event_budget(1)
                    .recovery_replay_byte_budget(1)
                    .recovery_prefetch_chunks(1)
                    .max_concurrent_workflow_tasks(2)
                    .workflow_task_prefetch_limit(2)
                    .workflow_task_commit_batch_size(commit_batch_size)
                    .register_workflow(cold_markers)
                    .register_workflow(hot_signal)
                    .build();
                let mut hot = None;
                let mut cold = None;
                for start_hot in [hot_first, !hot_first] {
                    if start_hot {
                        hot = Some(
                            client
                                .start_workflow::<hot_signal>(
                                    "hot",
                                    "workflows",
                                    Input { value: 0 },
                                )
                                .await
                                .unwrap(),
                        );
                        assert!(worker.run_workflow_once().await.unwrap());
                    } else {
                        cold = Some(
                            client
                                .start_workflow::<cold_markers>(
                                    "cold",
                                    "workflows",
                                    Input { value: 0 },
                                )
                                .await
                                .unwrap(),
                        );
                        let mut recorder = Worker::builder(backend.clone())
                            .workflow_task_queue("workflows")
                            .register_workflow(cold_markers)
                            .build();
                        assert!(recorder.run_workflow_once().await.unwrap());
                    }
                }
                for id in ["cold", "hot"] {
                    client.signal_workflow(id, "go", id, ()).await.unwrap();
                }
                let mut batch = Box::pin(worker.run_workflow_batch_once());
                let mut cx = Context::from_waker(Waker::noop());
                for _ in 0..10 {
                    assert!(batch.as_mut().poll(&mut cx).is_pending());
                }
                for (run_id, should_complete) in [(hot.unwrap(), true), (cold.unwrap(), false)] {
                    let history = backend
                        .stream_history(StreamHistoryRequest {
                            run_id,
                            after_event_id: EventId::ZERO,
                            up_to_event_id: EventId(1000),
                            max_events: 1000,
                            max_bytes: usize::MAX,
                        })
                        .await
                        .unwrap();
                    assert_eq!(
                        history.events.iter().any(|event| matches!(
                            event.data,
                            HistoryEventData::WorkflowCompleted { .. }
                        )),
                        should_complete,
                        "hot_first={hot_first}, commit_batch_size={commit_batch_size}"
                    );
                }
                assert_eq!(batch.await.unwrap(), 2);
            });
        }
    }
}
