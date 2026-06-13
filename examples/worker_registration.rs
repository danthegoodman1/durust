use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChargeInput {
    cents: u64,
}

#[durust::activity(name = "examples.charge-card")]
async fn charge_card(input: ChargeInput) -> durust::Result<String> {
    Ok(format!("charge:{}", input.cents))
}

#[durust::workflow(name = "examples.checkout", version = 1)]
async fn checkout(cents: u64) -> durust::Result<String> {
    durust::call_activity!(charge_card(ChargeInput { cents }))
        .task_queue("payments")
        .await
}

fn main() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<checkout>("order/1", "orders", 4200)
            .await
            .expect("start checkout");

        let mut workflow_worker = Worker::builder(backend.clone())
            .worker_id("workflow-worker")
            .workflow_task_queue("orders")
            .activity_task_queue("local-unused")
            .register_workflow(checkout)
            .build();
        let mut payment_worker = Worker::builder(backend.clone())
            .worker_id("payment-worker")
            .workflow_task_queue("unused")
            .activity_task_queue("payments")
            .register_activity(charge_card)
            .build();

        workflow_worker
            .run_until_idle()
            .await
            .expect("schedule payment");
        payment_worker
            .run_until_idle()
            .await
            .expect("complete payment remotely");
        workflow_worker
            .run_until_idle()
            .await
            .expect("complete checkout");

        let result = workflow_result(&backend, &run_id).await;
        println!("{result}");
    });
}

async fn workflow_result(backend: &MemoryBackend, run_id: &durust::RunId) -> String {
    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1_000),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .expect("stream history")
        .events;
    let completed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::WorkflowCompleted { result } => Some(result),
            _ => None,
        })
        .expect("workflow completed");
    durust::decode_payload(completed).expect("decode result")
}
