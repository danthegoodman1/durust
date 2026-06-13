use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct JobInput {
    remaining_batches: u32,
    processed: u64,
}

#[durust::workflow(name = "examples.continue-as-new", version = 1)]
async fn batch_job(input: JobInput) -> durust::Result<u64> {
    if input.remaining_batches > 0 {
        return durust::continue_as_new(JobInput {
            remaining_batches: input.remaining_batches - 1,
            processed: input.processed + 1,
        });
    }
    Ok(input.processed)
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("continue-as-new example");
        println!("{output}");
    });
}

async fn run_example() -> durust::Result<u64> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let first_run_id = client
        .start_workflow::<batch_job>(
            "job/continue",
            "workflows",
            JobInput {
                remaining_batches: 2,
                processed: 0,
            },
        )
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .register_workflow(batch_job)
        .build();

    worker.run_workflow_once().await?;
    let second_run_id = client
        .start_workflow::<batch_job>(
            "job/continue",
            "workflows",
            JobInput {
                remaining_batches: 99,
                processed: 99,
            },
        )
        .await?;
    worker.run_workflow_once().await?;
    let final_run_id = client
        .start_workflow::<batch_job>(
            "job/continue",
            "workflows",
            JobInput {
                remaining_batches: 99,
                processed: 99,
            },
        )
        .await?;
    worker.run_workflow_once().await?;

    assert_ne!(first_run_id, second_run_id);
    assert_ne!(second_run_id, final_run_id);

    let first_history = stream_history(&backend, &first_run_id).await?;
    assert_eq!(first_history.len(), 2);
    assert!(matches!(
        first_history[1].data,
        HistoryEventData::WorkflowContinuedAsNew { .. }
    ));

    let final_history = stream_history(&backend, &final_run_id).await?;
    assert_eq!(final_history.len(), 2);
    let HistoryEventData::WorkflowCompleted { result } = &final_history[1].data else {
        return Err(durust::Error::Backend(
            "final continued run did not complete".to_owned(),
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
    fn runs_continue_as_new_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, 2);
    }
}
