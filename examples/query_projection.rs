use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OrderView {
    status: String,
    total_cents: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OrderInput {
    total_cents: u64,
}

#[durust::workflow(name = "examples.query-projection", version = 1, query_state = OrderView)]
async fn order_workflow(input: OrderInput) -> durust::Result<u64> {
    durust::publish(&OrderView {
        status: "created".to_owned(),
        total_cents: input.total_cents,
    })?;
    let status = durust::signal::<String>("status").await?;
    durust::publish(&OrderView {
        status,
        total_cents: input.total_cents,
    })?;
    Ok(input.total_cents)
}

#[durust::query(workflow = order_workflow)]
fn order_status(view: &OrderView) -> String {
    view.status.clone()
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("query projection example");
        println!("{output}");
    });
}

async fn run_example() -> durust::Result<String> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<order_workflow>("order/1", "workflows", OrderInput { total_cents: 4_200 })
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .register_workflow(order_workflow)
        .build();

    assert!(
        client
            .query_projection::<order_workflow>("order/1")
            .await?
            .is_none()
    );
    worker.run_workflow_once().await?;
    let view = client
        .query_projection::<order_workflow>("order/1")
        .await?
        .expect("created projection");
    assert_eq!(order_status(&view), "created");

    client
        .signal_workflow("order/1", "status", "order/1/status/paid", "paid")
        .await?;
    worker.run_workflow_once().await?;

    let history = stream_history(&backend, &run_id).await?;
    assert!(matches!(
        history.last().expect("terminal event").data,
        HistoryEventData::WorkflowCompleted { .. }
    ));
    let view = client
        .query_projection::<order_workflow>("order/1")
        .await?
        .expect("paid projection");
    Ok(order_status(&view))
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
    fn runs_query_projection_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, "paid");
    }
}
