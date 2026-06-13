use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;

#[durust::activity(name = "examples.charge-v1")]
async fn charge_v1(_: ()) -> durust::Result<String> {
    Ok("charge-v1".to_owned())
}

#[durust::activity(name = "examples.charge-v2")]
async fn charge_v2(_: ()) -> durust::Result<String> {
    Ok("charge-v2".to_owned())
}

#[durust::workflow(name = "examples.versioned-charge", version = 1)]
async fn charge_workflow(_: ()) -> durust::Result<String> {
    if durust::patched("charge-v2")? {
        durust::call_activity!(charge_v2(()))
            .task_queue("activities")
            .await
    } else {
        durust::call_activity!(charge_v1(()))
            .task_queue("activities")
            .await
    }
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("version branch example");
        println!("{output}");
    });
}

async fn run_example() -> durust::Result<String> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<charge_workflow>("charge/1", "workflows", ())
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(charge_workflow)
        .register_activity(charge_v1)
        .register_activity(charge_v2)
        .build();

    worker.run_until_idle().await?;

    let history = stream_history(&backend, &run_id).await?;
    assert_eq!(history.len(), 5);
    let HistoryEventData::VersionMarker(marker) = &history[1].data else {
        return Err(durust::Error::Backend(
            "version branch did not record a marker".to_owned(),
        ));
    };
    assert_eq!(marker.change_id, "charge-v2");
    assert_eq!(marker.version, 1);

    let HistoryEventData::WorkflowCompleted { result } = &history[4].data else {
        return Err(durust::Error::Backend(
            "version branch workflow did not complete".to_owned(),
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
    fn runs_version_branch_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, "charge-v2");
    }
}
