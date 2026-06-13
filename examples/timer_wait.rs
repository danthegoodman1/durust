use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;
use std::time::Duration;

#[durust::workflow(name = "examples.timer-wait", version = 1)]
async fn sleep_then_return(input: u64) -> durust::Result<u64> {
    durust::sleep(Duration::from_millis(input)).await?;
    Ok(input + 1)
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("timer wait example");
        println!("{output}");
    });
}

async fn run_example() -> durust::Result<u64> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<sleep_then_return>("timer/1", "workflows", 50)
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .register_workflow(sleep_then_return)
        .build();

    worker.run_workflow_once().await?;
    assert_eq!(worker.run_timers_once().await?, 0);
    backend.advance_time(Duration::from_millis(50));
    assert_eq!(worker.run_timers_once().await?, 1);
    worker.run_workflow_once().await?;

    let history = stream_history(&backend, &run_id).await?;
    assert_eq!(history.len(), 4);
    assert!(matches!(history[1].data, HistoryEventData::TimerStarted(_)));
    assert!(matches!(history[2].data, HistoryEventData::TimerFired(_)));
    let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
        return Err(durust::Error::Backend(
            "timer workflow did not complete".to_owned(),
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
    fn runs_timer_wait_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, 51);
    }
}
