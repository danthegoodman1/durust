use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GreetingInput {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HelloInput {
    name: String,
}

#[durust::activity(name = "examples.greet")]
async fn greet(input: GreetingInput) -> durust::Result<String> {
    Ok(format!("hello, {}", input.name))
}

#[durust::workflow(name = "examples.hello-activity", version = 1)]
async fn hello(input: HelloInput) -> durust::Result<String> {
    durust::call_activity!(greet(GreetingInput { name: input.name }))
        .task_queue("activities")
        .await
}

fn main() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<hello>(
                "hello/1",
                "workflows",
                HelloInput {
                    name: "durust".to_owned(),
                },
            )
            .await
            .expect("start workflow");
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(hello)
            .register_activity(greet)
            .build();

        worker.run_until_idle().await.expect("run worker");

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
