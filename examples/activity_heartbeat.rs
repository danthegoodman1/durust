use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::thread;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RenderInput {
    frames: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RenderWorkflowInput {
    frames: u64,
}

#[durust::activity(name = "examples.render")]
async fn render(input: RenderInput) -> durust::Result<u64> {
    for _ in 0..3 {
        thread::sleep(Duration::from_secs(1));
        durust::heartbeat_activity().await?;
    }
    Ok(input.frames)
}

#[durust::workflow(name = "examples.activity-heartbeat", version = 1)]
async fn render_workflow(input: RenderWorkflowInput) -> durust::Result<u64> {
    durust::call_activity!(render(RenderInput {
        frames: input.frames
    }))
    .task_queue("render")
    .heartbeat_timeout(Duration::from_secs(2))
    .await
}

fn main() {
    block_on(async {
        let (result, history_len) = run_example().await.expect("activity heartbeat example");
        println!("rendered={result}, history_entries={history_len}");
    });
}

async fn run_example() -> durust::Result<(u64, usize)> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<render_workflow>(
            "render/1",
            "workflows",
            RenderWorkflowInput { frames: 120 },
        )
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .activity_task_queue("render")
        .register_workflow(render_workflow)
        .register_activity(render)
        .build();

    worker.run_until_idle().await?;
    let history = stream_history(&backend, &run_id).await?;
    assert_eq!(history.len(), 4);
    let HistoryEventData::ActivityScheduled(scheduled) = &history[1].data else {
        return Err(durust::Error::Backend(
            "expected ActivityScheduled event".to_owned(),
        ));
    };
    assert_eq!(scheduled.heartbeat_timeout, Some(Duration::from_secs(2)));
    let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
        return Err(durust::Error::Backend(
            "workflow did not complete".to_owned(),
        ));
    };
    Ok((durust::decode_payload(result)?, history.len()))
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
    fn runs_activity_heartbeat_example() {
        let (result, history_len) = block_on(run_example()).unwrap();
        assert_eq!(result, 120);
        assert_eq!(history_len, 4);
    }
}
