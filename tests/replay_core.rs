use durust::{
    ActivityName, ClaimActivityOptions, ClaimWorkflowTaskOptions, Client, CompleteActivityRequest,
    DurableBackend, EventId, HistoryEventData, MemoryBackend, Namespace, SqliteBackend, TaskQueue,
    Worker, WorkerId, WorkflowType,
};
use futures::executor::block_on;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct NumberInput {
    value: u64,
}

#[durust::activity(name = "tests.double")]
async fn double(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value * 2)
}

#[durust::workflow(name = "tests.double-plus-one", version = 1)]
async fn double_plus_one(input: u64) -> durust::Result<u64> {
    let doubled = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .await?;
    Ok(doubled + 1)
}

#[durust::workflow(name = "tests.double-plus-one", version = 1)]
async fn double_plus_one_changed(input: u64) -> durust::Result<u64> {
    let doubled = durust::call_activity!(double(NumberInput { value: input + 1 }))
        .task_queue("activities")
        .await?;
    Ok(doubled + 1)
}

#[test]
fn simple_workflow_schedules_activity_and_completes_from_cache() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/simple", "workflows", 20)
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("worker-a")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        assert!(worker.run_activity_once().await.unwrap());
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(
            history[0].data,
            HistoryEventData::WorkflowStarted { .. }
        ));
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        assert!(matches!(
            history[2].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 41);
    });
}

#[test]
fn worker_loop_runs_workflow_and_activity_until_idle() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/loop", "workflows", 8)
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("loop-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 2);
        assert_eq!(stats.activity_tasks, 1);

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 17);
    });
}

#[test]
fn worker_crash_recovers_by_streaming_history() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/replay", "workflows", 7)
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .worker_id("worker-before-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);

        let mut recovered_worker = Worker::builder(backend.clone())
            .worker_id("worker-after-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("workflow did not complete after replay");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 15);
    });
}

#[test]
fn recovery_fetches_history_incrementally_in_configured_chunks() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/chunked-replay", "workflows", 5)
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);
        backend.clear_stream_requests();

        let mut recovered_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        let requests = backend.stream_requests();
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|request| request.max_events == 1));
        assert_eq!(
            requests
                .iter()
                .map(|request| request.after_event_id)
                .collect::<Vec<_>>(),
            vec![EventId::ZERO, EventId(1), EventId(2)]
        );
        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
    });
}

#[test]
fn cached_workflow_wake_streams_only_events_after_cached_tail() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/cached-wake", "workflows", 6)
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());
        assert!(worker.run_activity_once().await.unwrap());
        backend.clear_stream_requests();

        assert!(worker.run_workflow_once().await.unwrap());

        let requests = backend.stream_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].after_event_id, EventId(2));
        assert_eq!(requests[0].up_to_event_id, EventId(3));
        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
    });
}

#[test]
fn replay_detects_changed_activity_input_without_appending_failure() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/nondeterminism", "workflows", 7)
            .await
            .unwrap();
        let mut original_worker = Worker::builder(backend.clone())
            .worker_id("worker-original")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(original_worker.run_workflow_once().await.unwrap());
        assert!(original_worker.run_activity_once().await.unwrap());
        drop(original_worker);

        let mut changed_worker = Worker::builder(backend.clone())
            .worker_id("worker-changed")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(double_plus_one_changed)
            .register_activity(double)
            .build();
        let err = changed_worker.run_workflow_once().await.unwrap_err();
        assert!(matches!(err, durust::Error::Nondeterminism(_)));

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 3);
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
        );

        let immediately_claimable = backend
            .claim_workflow_task(
                WorkerId::new("after-nondeterminism"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(immediately_claimable.is_none());
    });
}

#[test]
fn configured_nondeterminism_backoff_releases_workflow_after_delay() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<double_plus_one>("wf/nondeterminism-backoff", "workflows", 7)
            .await
            .unwrap();
        let mut original_worker = Worker::builder(backend.clone())
            .worker_id("worker-original")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(original_worker.run_workflow_once().await.unwrap());
        assert!(original_worker.run_activity_once().await.unwrap());
        drop(original_worker);

        let mut changed_worker = Worker::builder(backend.clone())
            .worker_id("worker-changed")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .nondeterminism_retry_backoff(Duration::from_millis(25))
            .register_workflow(double_plus_one_changed)
            .register_activity(double)
            .build();
        let err = changed_worker.run_workflow_once().await.unwrap_err();
        assert!(matches!(err, durust::Error::Nondeterminism(_)));

        let hidden = backend
            .claim_workflow_task(
                WorkerId::new("before-backoff"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(hidden.is_none());

        std::thread::sleep(Duration::from_millis(40));
        let visible = backend
            .claim_workflow_task(
                WorkerId::new("after-backoff"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(visible.is_some());
    });
}

#[test]
fn provider_claims_only_registered_workflow_and_activity_types() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<double_plus_one>("wf/filtering", "workflows", 1)
            .await
            .unwrap();

        let unmatched = backend
            .claim_workflow_task(
                WorkerId::new("worker"),
                ClaimWorkflowTaskOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("workflows"),
                    registered_workflow_types: Vec::new(),
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(unmatched.is_none());

        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());

        let unmatched_activity = backend
            .claim_activity_task(
                WorkerId::new("activity-worker"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("other.activity")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(unmatched_activity.is_none());
    });
}

#[test]
fn sqlite_backend_recovers_after_close_and_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("durust.sqlite3");
        let backend = SqliteBackend::open(&db_path).unwrap();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/sqlite-replay", "workflows", 11)
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .worker_id("sqlite-before-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);
        drop(backend);

        let reopened = SqliteBackend::open(&db_path).unwrap();
        let mut recovered_worker = Worker::builder(reopened.clone())
            .worker_id("sqlite-after-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&reopened, &run_id).await;
        assert_eq!(history.len(), 4);
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("SQLite workflow did not complete after reopen replay");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 23);
    });
}

#[test]
fn sqlite_worker_loop_runs_until_idle() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("durust-loop.sqlite3");
        let backend = SqliteBackend::open(&db_path).unwrap();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/sqlite-loop", "workflows", 13)
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("sqlite-loop-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 2);
        assert_eq!(stats.activity_tasks, 1);

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("SQLite workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 27);
    });
}

async fn stream_all<B>(backend: &B, run_id: &durust::RunId) -> Vec<durust::HistoryEvent>
where
    B: DurableBackend,
{
    backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1_000_000),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events
}

fn double_plus_one_claim_options() -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("workflows"),
        registered_workflow_types: vec![WorkflowType::new("tests.double-plus-one", 1)],
        lease_duration: Duration::from_secs(30),
    }
}

#[derive(Clone)]
struct RecordingBackend {
    inner: MemoryBackend,
    stream_requests: Arc<Mutex<Vec<durust::StreamHistoryRequest>>>,
}

impl RecordingBackend {
    fn new(inner: MemoryBackend) -> Self {
        Self {
            inner,
            stream_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn clear_stream_requests(&self) {
        self.stream_requests.lock().unwrap().clear();
    }

    fn stream_requests(&self) -> Vec<durust::StreamHistoryRequest> {
        self.stream_requests.lock().unwrap().clone()
    }
}

impl DurableBackend for RecordingBackend {
    fn start_workflow(
        &self,
        req: durust::StartWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::StartWorkflowOutcome>> {
        self.inner.start_workflow(req)
    }

    fn claim_workflow_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::ClaimedWorkflowTask>>> {
        self.inner.claim_workflow_task(worker_id, opts)
    }

    fn stream_history(
        &self,
        req: durust::StreamHistoryRequest,
    ) -> BoxFuture<'static, durust::Result<durust::HistoryChunk>> {
        self.stream_requests.lock().unwrap().push(req.clone());
        self.inner.stream_history(req)
    }

    fn commit_workflow_task(
        &self,
        claim: durust::WorkflowTaskClaim,
        batch: durust::WorkflowTaskCommit,
    ) -> BoxFuture<'static, durust::Result<durust::CommitOutcome>> {
        self.inner.commit_workflow_task(claim, batch)
    }

    fn release_workflow_task(
        &self,
        claim: durust::WorkflowTaskClaim,
        release: durust::WorkflowTaskRelease,
    ) -> BoxFuture<'static, durust::Result<()>> {
        self.inner.release_workflow_task(claim, release)
    }

    fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::ClaimedActivityTask>>> {
        self.inner.claim_activity_task(worker_id, opts)
    }

    fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::CompleteActivityOutcome>> {
        self.inner.complete_activity(req)
    }
}
