use durust::{
    Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, PayloadRef, Worker,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WordCountInput {
    chunks: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkInput {
    chunk: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PartitionOutput {
    manifest_ref: PayloadRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReduceInput {
    manifest_ref: PayloadRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WordCountOutput {
    words: u64,
}

#[durust::activity(name = "examples.partition-input")]
async fn partition_input(input: WordCountInput) -> durust::Result<PartitionOutput> {
    let manifest_ref =
        durust::activity_map_manifest(input.chunks.into_iter().map(|chunk| WorkInput { chunk }))?;
    Ok(PartitionOutput { manifest_ref })
}

#[durust::activity(name = "examples.count-words")]
async fn count_words(input: WorkInput) -> durust::Result<u64> {
    Ok(input.chunk.split_whitespace().count() as u64)
}

#[durust::activity(name = "examples.reduce-word-count")]
async fn reduce_word_count(input: ReduceInput) -> durust::Result<WordCountOutput> {
    let result_refs = durust::decode_activity_map_result_refs(&input.manifest_ref)?;
    let words = result_refs.iter().try_fold(0_u64, |sum, payload| {
        Ok(sum + durust::decode_payload::<u64>(payload)?)
    })?;
    Ok(WordCountOutput { words })
}

#[durust::workflow(name = "examples.word-count", version = 1)]
async fn word_count(input: WordCountInput) -> durust::Result<WordCountOutput> {
    let partitions = durust::call_activity!(partition_input(input))
        .task_queue("storage")
        .await?;
    let mapped = durust::activity_map(count_words)
        .task_queue("mappers")
        .input_manifest(partitions.manifest_ref)
        .max_in_flight(2)
        .result_manifest("partials")
        .spawn()
        .await?;
    let partials = mapped.result_manifest().await?;
    durust::call_activity!(reduce_word_count(ReduceInput {
        manifest_ref: partials,
    }))
    .task_queue("reducers")
    .await
}

fn main() {
    block_on(async {
        let output = run_example().await.expect("map reduce example");
        println!("{}", output.words);
    });
}

async fn run_example() -> durust::Result<WordCountOutput> {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<word_count>(
            "word-count/1",
            "jobs",
            WordCountInput {
                chunks: vec![
                    "durable rust".to_owned(),
                    "workflow replay scales".to_owned(),
                    "append only history".to_owned(),
                ],
            },
        )
        .await?;

    let mut workflow_worker = Worker::builder(backend.clone())
        .worker_id("workflow-worker")
        .workflow_task_queue("jobs")
        .activity_task_queue("unused")
        .register_workflow(word_count)
        .build();
    let mut storage_worker = Worker::builder(backend.clone())
        .worker_id("storage-worker")
        .workflow_task_queue("unused")
        .activity_task_queue("storage")
        .register_activity(partition_input)
        .build();
    let mut mapper_worker = Worker::builder(backend.clone())
        .worker_id("mapper-worker")
        .workflow_task_queue("unused")
        .activity_task_queue("mappers")
        .register_activity(count_words)
        .build();
    let mut reducer_worker = Worker::builder(backend.clone())
        .worker_id("reducer-worker")
        .workflow_task_queue("unused")
        .activity_task_queue("reducers")
        .register_activity(reduce_word_count)
        .build();

    drive_until_idle(
        &mut workflow_worker,
        &mut storage_worker,
        &mut mapper_worker,
        &mut reducer_worker,
    )
    .await?;

    let history = stream_history(&backend, &run_id).await?;
    assert_eq!(history.len(), 8);
    assert!(matches!(
        history[1].data,
        HistoryEventData::ActivityScheduled(_)
    ));
    assert!(matches!(
        history[2].data,
        HistoryEventData::ActivityCompleted(_)
    ));
    assert!(matches!(
        history[3].data,
        HistoryEventData::ActivityMapScheduled(_)
    ));
    assert!(matches!(
        history[4].data,
        HistoryEventData::ActivityMapCompleted(_)
    ));
    assert!(matches!(
        history[5].data,
        HistoryEventData::ActivityScheduled(_)
    ));
    assert!(matches!(
        history[6].data,
        HistoryEventData::ActivityCompleted(_)
    ));
    let HistoryEventData::WorkflowCompleted { result } = &history[7].data else {
        return Err(durust::Error::Backend(
            "map reduce workflow did not complete".to_owned(),
        ));
    };
    durust::decode_payload(result)
}

async fn drive_until_idle(
    workflow_worker: &mut Worker<MemoryBackend>,
    storage_worker: &mut Worker<MemoryBackend>,
    mapper_worker: &mut Worker<MemoryBackend>,
    reducer_worker: &mut Worker<MemoryBackend>,
) -> durust::Result<()> {
    for _ in 0..32 {
        let mut progressed = false;
        progressed |= workflow_worker.run_workflow_once().await?;
        progressed |= storage_worker.run_activity_once().await?;
        progressed |= mapper_worker.run_activity_once().await?;
        progressed |= reducer_worker.run_activity_once().await?;
        if !progressed {
            return Ok(());
        }
    }
    Err(durust::Error::Backend(
        "example workers did not become idle".to_owned(),
    ))
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
    fn runs_map_reduce_example() {
        let output = block_on(run_example()).unwrap();
        assert_eq!(output, WordCountOutput { words: 8 });
    }
}
