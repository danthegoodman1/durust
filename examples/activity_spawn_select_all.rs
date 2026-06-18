use durust::{
    ActivityName, BoxSelectBranch, ClaimActivityOptions, Client, CompleteActivityRequest,
    DurableBackend, DurableBranchExt, EventId, HistoryEventData, MemoryBackend, TaskQueue, Worker,
    WorkerId,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Candidate {
    id: String,
    score: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FirstScoreInput {
    candidates: Vec<Candidate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScoredCandidate {
    id: String,
    score: u64,
}

#[durust::activity(name = "examples.score-candidate")]
async fn score_candidate(candidate: Candidate) -> durust::Result<u64> {
    Ok(candidate.score)
}

#[durust::workflow(name = "examples.activity-spawn-select-all", version = 1)]
async fn first_score(input: FirstScoreInput) -> durust::Result<String> {
    let mut branches: Vec<BoxSelectBranch<ScoredCandidate>> = Vec::new();
    for candidate in input.candidates {
        let id = candidate.id.clone();
        let handle = durust::call_activity!(score_candidate(candidate))
            .task_queue("scoring")
            .spawn()
            .await?;
        branches.push(
            handle
                .result()
                .map_ok(move |score| ScoredCandidate { id, score })
                .boxed(),
        );
    }

    let winner = durust::select_all(branches).await?;
    Ok(format!(
        "{}:{}:{}",
        winner.branch_index, winner.value.id, winner.value.score
    ))
}

fn main() {
    block_on(async {
        let output = run_example()
            .await
            .expect("activity spawn select_all example");
        println!("{output}");
    });
}

async fn run_example() -> durust::Result<String> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<first_score>(
            "score/1",
            "workflows",
            FirstScoreInput {
                candidates: vec![
                    Candidate {
                        id: "slow-a".to_owned(),
                        score: 10,
                    },
                    Candidate {
                        id: "fast-b".to_owned(),
                        score: 99,
                    },
                    Candidate {
                        id: "slow-c".to_owned(),
                        score: 30,
                    },
                ],
            },
        )
        .await?;
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .register_workflow(first_score)
        .build();

    worker.run_workflow_once().await?;
    let activity_opts = ClaimActivityOptions {
        namespace: durust::Namespace::default(),
        task_queue: TaskQueue::new("scoring"),
        registered_activity_names: vec![ActivityName::new("examples.score-candidate")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(WorkerId::new("score-worker-a"), activity_opts.clone())
        .await?
        .expect("first spawned activity");
    let second = backend
        .claim_activity_task(WorkerId::new("score-worker-b"), activity_opts.clone())
        .await?
        .expect("second spawned activity");
    let third = backend
        .claim_activity_task(WorkerId::new("score-worker-c"), activity_opts.clone())
        .await?
        .expect("third spawned activity");

    backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&99_u64)?,
        })
        .await?;
    worker.run_workflow_once().await?;

    for claim in [first.claim, third.claim] {
        let late = backend
            .complete_activity(CompleteActivityRequest {
                claim,
                result: durust::encode_payload(&0_u64)?,
            })
            .await?;
        assert_eq!(late, durust::CompleteActivityOutcome::AlreadyCompleted);
    }

    let history = stream_history(&backend, &run_id).await?;
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(event.data, HistoryEventData::ActivityScheduled(_)))
            .count(),
        3
    );
    assert!(matches!(
        history[history.len() - 2].data,
        HistoryEventData::SelectWinner(_)
    ));
    let HistoryEventData::WorkflowCompleted { result } =
        &history.last().expect("workflow terminal").data
    else {
        return Err(durust::Error::Backend(
            "select_all workflow did not complete".to_owned(),
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
    fn runs_activity_spawn_select_all_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, "1:fast-b:99");
    }
}
