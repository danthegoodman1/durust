use durust::{
    ActivityName, ClaimActivityOptions, Client, CompleteActivityRequest, DurableBackend, EventId,
    HistoryEventData, MemoryBackend, TaskQueue, Worker, WorkerId,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkItem {
    id: String,
    value: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CollectWorkInput {
    items: Vec<WorkItem>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WorkOutput {
    id: String,
    doubled: u64,
}

#[durust::activity(name = "examples.join-all-work-item")]
async fn work_item(input: WorkItem) -> durust::Result<WorkOutput> {
    Ok(WorkOutput {
        id: input.id,
        doubled: input.value * 2,
    })
}

#[durust::workflow(name = "examples.activity-spawn-join-all", version = 1)]
async fn collect_work(input: CollectWorkInput) -> durust::Result<String> {
    let mut branches = Vec::new();
    for item in input.items {
        let handle = durust::call_activity!(work_item(item))
            .task_queue("workers")
            .spawn()
            .await?;
        branches.push(handle.result());
    }

    let outputs = durust::join_all(branches).await?;
    Ok(outputs
        .into_iter()
        .map(|output| format!("{}={}", output.id, output.doubled))
        .collect::<Vec<_>>()
        .join(","))
}

fn main() {
    block_on(async {
        let output = run_example()
            .await
            .expect("activity spawn join_all example");
        println!("{output}");
    });
}

async fn run_example() -> durust::Result<String> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<collect_work>(
            "join-all/1",
            "workflows",
            CollectWorkInput {
                items: vec![
                    WorkItem {
                        id: "a".to_owned(),
                        value: 10,
                    },
                    WorkItem {
                        id: "b".to_owned(),
                        value: 11,
                    },
                    WorkItem {
                        id: "c".to_owned(),
                        value: 12,
                    },
                ],
            },
        )
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .register_workflow(collect_work)
        .build();

    worker.run_workflow_once().await?;
    let activity_opts = ClaimActivityOptions {
        namespace: durust::Namespace::default(),
        task_queue: TaskQueue::new("workers"),
        registered_activity_names: vec![ActivityName::new("examples.join-all-work-item")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(WorkerId::new("join-all-worker-a"), activity_opts.clone())
        .await?
        .expect("first spawned activity");
    let second = backend
        .claim_activity_task(WorkerId::new("join-all-worker-b"), activity_opts.clone())
        .await?
        .expect("second spawned activity");
    let third = backend
        .claim_activity_task(WorkerId::new("join-all-worker-c"), activity_opts)
        .await?
        .expect("third spawned activity");

    for (claim, output) in [
        (
            third.claim,
            WorkOutput {
                id: "c".to_owned(),
                doubled: 24,
            },
        ),
        (
            first.claim,
            WorkOutput {
                id: "a".to_owned(),
                doubled: 20,
            },
        ),
        (
            second.claim,
            WorkOutput {
                id: "b".to_owned(),
                doubled: 22,
            },
        ),
    ] {
        backend
            .complete_activity(CompleteActivityRequest {
                claim,
                result: durust::encode_payload(&output)?,
            })
            .await?;
    }
    worker.run_workflow_once().await?;

    let history = stream_history(&backend, &run_id).await?;
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(event.data, HistoryEventData::ActivityScheduled(_)))
            .count(),
        3
    );
    assert!(
        !history
            .iter()
            .any(|event| matches!(event.data, HistoryEventData::SelectWinner(_)))
    );
    let HistoryEventData::WorkflowCompleted { result } =
        &history.last().expect("workflow terminal").data
    else {
        return Err(durust::Error::Backend(
            "join_all workflow did not complete".to_owned(),
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
    fn runs_activity_spawn_join_all_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, "a=20,b=22,c=24");
    }
}
