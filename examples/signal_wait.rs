use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SignalWaitInput {}

#[durust::workflow(name = "examples.signal-wait", version = 1)]
async fn wait_for_signal(_: SignalWaitInput) -> durust::Result<String> {
    durust::signal::<String>("ready").await
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("signal wait example");
        println!("{output}");
    });
}

async fn run_example() -> durust::Result<String> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<wait_for_signal>("signal/1", "workflows", SignalWaitInput {})
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .register_workflow(wait_for_signal)
        .build();

    worker.run_workflow_once().await?;
    client
        .signal_workflow("signal/1", "ready", "signal/1/ready", "released".to_owned())
        .await?;
    worker.run_workflow_once().await?;

    let history = stream_history(&backend, &run_id).await?;
    assert_eq!(history.len(), 3);
    assert!(matches!(
        history[1].data,
        HistoryEventData::SignalConsumed(_)
    ));
    let HistoryEventData::WorkflowCompleted { result } = &history[2].data else {
        return Err(durust::Error::Backend(
            "signal workflow did not complete".to_owned(),
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
    fn runs_signal_wait_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, "released");
    }
}
