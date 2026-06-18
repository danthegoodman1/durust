use durust::{
    Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, PayloadStorageConfig, Worker,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Document {
    id: String,
    body: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DocumentView {
    id: String,
    bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DocumentSummary {
    id: String,
    bytes: usize,
}

#[durust::activity(name = "examples.summarize-document")]
async fn summarize_document(input: Document) -> durust::Result<DocumentSummary> {
    Ok(DocumentSummary {
        id: input.id,
        bytes: input.body.len(),
    })
}

#[durust::workflow(
    name = "examples.payload-offload",
    version = 1,
    query_state = DocumentView
)]
async fn process_document(input: Document) -> durust::Result<DocumentSummary> {
    durust::publish(&DocumentView {
        id: input.id.clone(),
        bytes: input.body.len(),
    })?;
    durust::call_activity!(summarize_document(input)).await
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("payload offload example");
        println!("{} {}", output.id, output.bytes);
    });
}

async fn run_example() -> durust::Result<DocumentSummary> {
    let backend =
        MemoryBackend::with_payload_storage(PayloadStorageConfig::new().inline_threshold_bytes(1));
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<process_document>(
            "document/large",
            "workflows",
            Document {
                id: "large".to_owned(),
                body: "x".repeat(512),
            },
        )
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .activity_task_queue("workflows")
        .register_workflow(process_document)
        .register_activity(summarize_document)
        .build();

    worker.run_until_idle().await?;
    assert!(backend.payload_blob_count() > 0);

    let history = stream_history(&backend, &run_id).await?;
    let HistoryEventData::WorkflowCompleted { result } =
        &history.last().expect("terminal event").data
    else {
        return Err(durust::Error::Backend(
            "payload offload workflow did not complete".to_owned(),
        ));
    };
    let hydrated = backend.hydrate_payload(result.clone()).await?;
    durust::decode_payload(&hydrated)
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
    fn runs_payload_offload_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(
            output,
            DocumentSummary {
                id: "large".to_owned(),
                bytes: 512
            }
        );
    }
}
