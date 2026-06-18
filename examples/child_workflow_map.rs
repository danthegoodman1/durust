use durust::{Client, DurableBackend, HistoryEventData, MemoryBackend, Worker};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ItemInput {
    value: u64,
}

#[durust::workflow(name = "examples.child-map-item", version = 1)]
async fn child_map_item(input: ItemInput) -> durust::Result<u64> {
    Ok(input.value * input.value)
}

#[durust::workflow(name = "examples.child-workflow-map", version = 1)]
async fn sum_child_workflow_map(values: Vec<u64>) -> durust::Result<u64> {
    let input_manifest =
        durust::child_workflow_map_manifest(values.into_iter().map(|value| ItemInput { value }))?;
    let mapped = durust::child_workflow_map::<child_map_item>()
        .task_queue("workflows")
        .workflow_id_prefix("child-map/items")
        .input_manifest(input_manifest)
        .max_in_flight(2)
        .result_manifest("squares")
        .spawn()
        .await?;
    let result_manifest = mapped.result_manifest().await?;
    let result_refs = durust::decode_child_workflow_map_success_refs(&result_manifest)?;
    result_refs.iter().try_fold(0_u64, |sum, payload| {
        Ok(sum + durust::decode_payload::<u64>(payload)?)
    })
}

fn main() -> durust::Result<()> {
    futures::executor::block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<sum_child_workflow_map>(
                "examples/child-workflow-map",
                "workflows",
                vec![2, 3, 4],
            )
            .await?;

        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(sum_child_workflow_map)
            .register_workflow(child_map_item)
            .build();
        worker.run_until_idle().await?;

        let history = backend
            .stream_history(durust::StreamHistoryRequest {
                run_id,
                after_event_id: durust::EventId::ZERO,
                up_to_event_id: durust::EventId(100),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await?
            .events;
        let Some(HistoryEventData::WorkflowCompleted { result }) =
            history.last().map(|event| &event.data)
        else {
            return Err(durust::Error::Backend(
                "example workflow did not complete".to_owned(),
            ));
        };
        println!(
            "sum of squares: {}",
            durust::decode_payload::<u64>(&result)?
        );
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_child_workflow_map_example() {
        main().unwrap();
    }
}
