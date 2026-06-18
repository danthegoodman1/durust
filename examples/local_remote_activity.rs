use durust::{
    ActivityName, ClaimActivityOptions, Client, DurableBackend, EventId, HistoryEventData,
    MemoryBackend, Namespace, TaskQueue, Worker, WorkerId,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NumberInput {
    value: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DoubleWorkflowInput {
    value: u64,
}

#[durust::activity(name = "examples.local-remote-double")]
async fn double(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value * 2)
}

#[durust::workflow(name = "examples.local-remote-activity", version = 1)]
async fn double_workflow(input: DoubleWorkflowInput) -> durust::Result<u64> {
    durust::call_activity!(double(NumberInput { value: input.value }))
        .task_queue("compute")
        .await
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("local/remote activity example");
        println!("local={}, remote={}", output.0, output.1);
    });
}

async fn run_example() -> durust::Result<(u64, u64)> {
    let local = run_with_local_preference().await?;
    let remote = run_with_remote_fallback().await?;
    Ok((local, remote))
}

async fn run_with_local_preference() -> durust::Result<u64> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<double_workflow>(
            "local-activity/1",
            "workflows",
            DoubleWorkflowInput { value: 21 },
        )
        .await?;
    let mut workflow_worker = Worker::builder(backend.clone())
        .worker_id("workflow-worker")
        .workflow_task_queue("workflows")
        .activity_task_queue("compute")
        .register_workflow(double_workflow)
        .register_activity(double)
        .max_local_activities_per_workflow_task(1)
        .build();

    workflow_worker.run_workflow_once().await?;
    let remote_claim = backend
        .claim_activity_task(
            WorkerId::new("remote-worker"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("compute"),
                registered_activity_names: vec![ActivityName::new("examples.local-remote-double")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await?;
    assert!(remote_claim.is_none());
    workflow_worker.run_workflow_once().await?;

    completed_result(&backend, &run_id).await
}

async fn run_with_remote_fallback() -> durust::Result<u64> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<double_workflow>(
            "remote-activity/1",
            "workflows",
            DoubleWorkflowInput { value: 21 },
        )
        .await?;
    let mut workflow_worker = Worker::builder(backend.clone())
        .worker_id("workflow-worker")
        .workflow_task_queue("workflows")
        .activity_task_queue("compute")
        .register_workflow(double_workflow)
        .register_activity(double)
        .max_local_activities_per_workflow_task(0)
        .build();
    let mut remote_worker = Worker::builder(backend.clone())
        .worker_id("remote-worker")
        .workflow_task_queue("unused")
        .activity_task_queue("compute")
        .register_activity(double)
        .build();

    workflow_worker.run_workflow_once().await?;
    assert!(remote_worker.run_activity_once().await?);
    workflow_worker.run_workflow_once().await?;

    completed_result(&backend, &run_id).await
}

async fn completed_result(backend: &MemoryBackend, run_id: &durust::RunId) -> durust::Result<u64> {
    let history = stream_history(backend, run_id).await?;
    assert_eq!(history.len(), 4);
    assert!(matches!(
        history[1].data,
        HistoryEventData::ActivityScheduled(_)
    ));
    assert!(matches!(
        history[2].data,
        HistoryEventData::ActivityCompleted(_)
    ));
    let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
        return Err(durust::Error::Backend(
            "activity workflow did not complete".to_owned(),
        ));
    };
    durust::decode_payload(result)
}

async fn stream_history(
    backend: &MemoryBackend,
    run_id: &durust::RunId,
) -> durust::Result<Vec<durust::HistoryEvent>> {
    Ok(backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1_000),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await?
        .events)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_local_and_remote_activity_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, (42, 42));
    }
}
