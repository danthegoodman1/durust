use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NumberInput {
    value: u64,
}

#[durust::activity(name = "examples.square")]
async fn square(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value * input.value)
}

#[durust::workflow(name = "examples.activity-map", version = 1)]
async fn sum_squares(input: Vec<u64>) -> durust::Result<u64> {
    let manifest =
        durust::activity_map_manifest(input.into_iter().map(|value| NumberInput { value }))?;
    let mapped = durust::activity_map(square)
        .task_queue("mappers")
        .input_manifest(manifest)
        .max_in_flight(2)
        .result_manifest("squares")
        .spawn()
        .await?;
    let result_manifest = mapped.result_manifest().await?;
    let result_refs = durust::decode_activity_map_result_refs(&result_manifest)?;
    result_refs.iter().try_fold(0_u64, |sum, payload| {
        Ok(sum + durust::decode_payload::<u64>(payload)?)
    })
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("activity map example");
        println!("{output}");
    });
}

async fn run_example() -> durust::Result<u64> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<sum_squares>("activity-map/1", "workflows", vec![2, 3, 4])
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .activity_task_queue("mappers")
        .register_workflow(sum_squares)
        .register_activity(square)
        .build();

    worker.run_until_idle().await?;

    let history = stream_history(&backend, &run_id).await?;
    assert_eq!(history.len(), 4);
    assert!(matches!(
        history[1].data,
        HistoryEventData::ActivityMapScheduled(_)
    ));
    assert!(matches!(
        history[2].data,
        HistoryEventData::ActivityMapCompleted(_)
    ));
    let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
        return Err(durust::Error::Backend(
            "activity map workflow did not complete".to_owned(),
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
    fn runs_activity_map_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, 29);
    }
}
