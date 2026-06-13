use durust::{Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, Worker};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Approval {
    reviewer: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Cancel {
    reason: String,
}

enum ApprovalDecision {
    Approved(Approval),
    Cancelled(Cancel),
    TimedOut,
}

#[durust::workflow(name = "examples.select-approval", version = 1)]
async fn approval_workflow(deadline_ms: u64) -> durust::Result<String> {
    let decision = durust::select! {
        approval = durust::signal::<Approval>("approved") => {
            ApprovalDecision::Approved(approval?)
        }

        cancel = durust::signal::<Cancel>("cancel") => {
            ApprovalDecision::Cancelled(cancel?)
        }

        timer = durust::sleep(Duration::from_millis(deadline_ms)) => {
            timer?;
            ApprovalDecision::TimedOut
        }
    };

    match decision {
        ApprovalDecision::Approved(approval) => Ok(format!("approved:{}", approval.reviewer)),
        ApprovalDecision::Cancelled(cancel) => Ok(format!("cancelled:{}", cancel.reason)),
        ApprovalDecision::TimedOut => Ok("timed-out".to_owned()),
    }
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("select approval example");
        println!("{output}");
    });
}

async fn run_example() -> durust::Result<String> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<approval_workflow>("approval/1", "workflows", 30_000)
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .register_workflow(approval_workflow)
        .build();

    worker.run_workflow_once().await?;
    client
        .signal_workflow(
            "approval/1",
            "approved",
            "approval/1/approved",
            Approval {
                reviewer: "sam".to_owned(),
            },
        )
        .await?;
    worker.run_workflow_once().await?;

    let history = stream_history(&backend, &run_id).await?;
    assert_eq!(history.len(), 5);
    assert!(matches!(history[1].data, HistoryEventData::TimerStarted(_)));
    assert!(matches!(
        history[2].data,
        HistoryEventData::SignalConsumed(_)
    ));
    let HistoryEventData::SelectWinner(winner) = &history[3].data else {
        return Err(durust::Error::Backend(
            "approval select did not record a winner".to_owned(),
        ));
    };
    assert_eq!(winner.branch_ordinal, 0);
    let HistoryEventData::WorkflowCompleted { result } = &history[4].data else {
        return Err(durust::Error::Backend(
            "approval workflow did not complete".to_owned(),
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
    fn runs_select_approval_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, "approved:sam");
    }
}
