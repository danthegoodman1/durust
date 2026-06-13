use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NumberInput {
    value: u64,
}

#[durust::activity(name = "examples.join-double")]
async fn double(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value * 2)
}

#[durust::workflow(name = "examples.join-activities", version = 1)]
async fn join_activities(input: u64) -> durust::Result<u64> {
    let (left, right) = durust::join!(
        durust::call_activity!(double(NumberInput { value: input })).task_queue("activities"),
        durust::call_activity!(double(NumberInput { value: input + 1 })).task_queue("activities"),
    )
    .await?;
    Ok(left + right)
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("join activities example");
        println!("{output}");
    });
}

async fn run_example() -> durust::Result<u64> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<join_activities>("join/1", "workflows", 20)
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(join_activities)
        .register_activity(double)
        .build();

    worker.run_workflow_once().await?;
    let scheduled = stream_history(&backend, &run_id).await?;
    assert_eq!(scheduled.len(), 3);
    assert!(matches!(
        scheduled[1].data,
        HistoryEventData::ActivityScheduled(_)
    ));
    assert!(matches!(
        scheduled[2].data,
        HistoryEventData::ActivityScheduled(_)
    ));

    worker.run_activity_once().await?;
    worker.run_activity_once().await?;
    worker.run_workflow_once().await?;

    let history = stream_history(&backend, &run_id).await?;
    assert_eq!(history.len(), 6);
    let HistoryEventData::WorkflowCompleted { result } = &history[5].data else {
        return Err(durust::Error::Backend(
            "join workflow did not complete".to_owned(),
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
    fn runs_join_activities_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, 82);
    }
}
