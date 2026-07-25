//! The Rust half of the shared map-fanout transition table.
//!
//! The table is checked in at `typescript/fixtures/contract/map-transitions.json`
//! and read by two runners — this one and
//! `typescript/packages/core/test/map-engine.test.ts` — so the Rust and
//! TypeScript map engines cannot drift apart silently.
//!
//! **What this runner asserts, and what it does not.** The table's
//! `transitions` section is single-step engine cases: descriptor state plus one
//! event in, an ordered effect list out. This file cannot assert them.
//! `src/map_engine.rs` is a private module (`mod map_engine;` in `src/lib.rs`,
//! with `pub(crate)` items) and a Cargo integration test only sees the crate's
//! public API, so `map_engine::step` is not nameable from here. Exposing it —
//! a test-only re-export in `src/lib.rs` — turns this file into the same table
//! walk the TypeScript runner already performs; the fixture records that as
//! `rustRunnerBlocker`.
//!
//! What is asserted here by execution is the table's `fanouts` section: whole
//! fanouts scripted purely in terms of the public provider API, which the
//! TypeScript runner replays against `MemoryBackend` too. Those cases pin the
//! parts of the machine both runtimes must agree on and both can reach — the
//! `max_in_flight` admission sequence, one replacement admitted per released
//! slot, and a failed map that abandons its siblings rather than leaving them
//! completable.
//!
//! Deliberately excluded, and asserted to stay excluded: retry delay *values*
//! (the two runtimes have different policy models), every `DescriptorCreated`
//! case where `recorded_outcomes >= item_count` (the runtimes disagree until
//! the plan row that converges Rust lands), and `ParentCancelled` (no
//! TypeScript producer). The fixture states each exclusion and its reason.

use durust::{
    ActivityMapTask, ActivityName, ClaimActivityOptions, ClaimWorkflowTaskOptions, Client,
    CompleteActivityOutcome, CompleteActivityRequest, DurableBackend, EventId, FailActivityOutcome,
    FailActivityRequest, HistoryEventData, MemoryBackend, Namespace, SqliteBackend, TaskQueue,
    WorkerId, WorkflowTaskCommit, WorkflowType,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Item {
    value: u64,
}

#[durust::activity(name = "map-table.item")]
async fn item_activity(input: Item) -> durust::Result<u64> {
    Ok(input.value)
}

#[durust::workflow(name = "map-table.workflow", version = 1)]
async fn map_table_workflow(input: Item) -> durust::Result<u64> {
    durust::call_activity!(item_activity(Item { value: input.value })).await
}

#[derive(Debug, Deserialize)]
struct TransitionTable {
    exclusions: Vec<Exclusion>,
    transitions: Vec<serde_json::Value>,
    fanouts: Vec<Fanout>,
}

#[derive(Debug, Deserialize)]
struct Exclusion {
    what: String,
    why: String,
}

#[derive(Debug, Deserialize)]
struct Fanout {
    name: String,
    #[serde(rename = "itemCount")]
    item_count: u64,
    #[serde(rename = "maxInFlight")]
    max_in_flight: usize,
    steps: Vec<FanoutStep>,
    #[serde(rename = "parentHistory")]
    parent_history: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FanoutStep {
    action: String,
    ordinal: Option<u64>,
}

fn load_table() -> TransitionTable {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/typescript/fixtures/contract/map-transitions.json"
    );
    serde_json::from_str(&fs::read_to_string(path).expect("shared map transition table"))
        .expect("shared map transition table is valid JSON")
}

/// The exclusions are the table's contract with the runtimes it cannot make
/// agree. Losing one would silently start asserting a behaviour one runtime
/// cannot match, and the failure would read as a regression rather than as an
/// out-of-date exclusion list.
#[test]
fn shared_table_declares_its_cross_runtime_exclusions() {
    let table = load_table();
    let declared: Vec<&str> = table
        .exclusions
        .iter()
        .map(|exclusion| exclusion.what.as_str())
        .collect();
    assert_eq!(
        declared,
        vec![
            "ScheduleItemRetry.visibleAtMs and .timeoutAtMs values",
            "Every DescriptorCreated case where recordedOutcomes >= itemCount, including but not limited to the empty manifest",
            "The ParentCancelled event",
        ],
    );
    for exclusion in &table.exclusions {
        assert!(
            exclusion.why.len() > 80,
            "exclusion `{}` needs a reason, not a label",
            exclusion.what
        );
    }
}

/// The single-step section must never assert a case inside a declared
/// exclusion, in either runner. This runner cannot execute those cases, but it
/// can hold the file to its own contract.
#[test]
fn shared_table_never_asserts_an_excluded_transition() {
    let table = load_table();
    assert!(!table.transitions.is_empty());
    for case in &table.transitions {
        let event_kind = case["event"]["kind"].as_str().expect("event kind");
        assert_ne!(
            event_kind, "ParentCancelled",
            "the table excludes the ParentCancelled event"
        );
        if event_kind == "DescriptorCreated" {
            let recorded = case["state"]["recordedOutcomes"]
                .as_u64()
                .expect("recorded");
            let item_count = case["state"]["itemCount"].as_u64().expect("itemCount");
            assert!(
                recorded < item_count,
                "`{}` is inside the excluded DescriptorCreated predicate",
                case["name"]
            );
        }
        // Retry instants are shape-only on both sides.
        if let Some(effects) = case["expect"]["effects"].as_array() {
            for effect in effects {
                if effect["kind"] == "ScheduleItemRetry" {
                    assert!(
                        effect["visibleAtMs"].is_string() && effect["timeoutAtMs"].is_string(),
                        "`{}` asserts a retry instant as a value, which the runtimes cannot match",
                        case["name"]
                    );
                }
            }
        }
    }
}

#[test]
fn memory_replays_every_shared_table_fanout() {
    block_on(async {
        for fanout in load_table().fanouts {
            replay_fanout(MemoryBackend::new(), &fanout).await;
        }
    });
}

#[test]
fn sqlite_replays_every_shared_table_fanout() {
    block_on(async {
        for fanout in load_table().fanouts {
            let dir = tempfile::tempdir().unwrap();
            let backend = SqliteBackend::open(dir.path().join("map-transitions.sqlite3")).unwrap();
            replay_fanout(backend, &fanout).await;
        }
    });
}

async fn replay_fanout<B>(backend: B, fanout: &Fanout)
where
    B: DurableBackend,
{
    let name = &fanout.name;
    let run_id = schedule_map(&backend, fanout).await;
    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("map-table-activities"),
        registered_activity_names: vec![ActivityName::new("map-table.item")],
        lease_duration: Duration::from_secs(30),
    };

    let mut claims = BTreeMap::new();
    for (index, step) in fanout.steps.iter().enumerate() {
        let where_ = format!("{name} step {index} ({})", step.action);
        match step.action.as_str() {
            "claim" => {
                let claimed = backend
                    .claim_activity_task(
                        WorkerId::new(format!("fanout-worker-{index}")),
                        activity_opts.clone(),
                    )
                    .await
                    .unwrap();
                let ordinal = claimed
                    .as_ref()
                    .and_then(|task| task.task.map_item.as_ref())
                    .map(|item| item.item_ordinal);
                assert_eq!(ordinal, step.ordinal, "{where_}");
                if let Some(task) = claimed {
                    claims.insert(
                        task.task.map_item.as_ref().expect("map item").item_ordinal,
                        task.claim,
                    );
                }
            }
            "complete" | "completeAbandoned" => {
                let ordinal = step.ordinal.expect("terminal step names an ordinal");
                let claim = claims
                    .get(&ordinal)
                    .expect("ordinal was never claimed")
                    .clone();
                let outcome = backend
                    .complete_activity(CompleteActivityRequest {
                        claim,
                        result: durust::encode_payload(&ordinal).unwrap(),
                    })
                    .await
                    .unwrap();
                if step.action == "complete" {
                    assert!(
                        matches!(outcome, CompleteActivityOutcome::Completed { .. }),
                        "{where_}: {outcome:?}"
                    );
                } else {
                    // The map ended while this item was in flight, so the
                    // engine's `AbandonPendingItems` tombstoned it and its work
                    // has nowhere to land.
                    assert!(
                        matches!(outcome, CompleteActivityOutcome::AlreadyCompleted),
                        "{where_}: {outcome:?}"
                    );
                }
            }
            "fail" => {
                let ordinal = step.ordinal.expect("terminal step names an ordinal");
                let claim = claims
                    .get(&ordinal)
                    .expect("ordinal was never claimed")
                    .clone();
                let outcome = backend
                    .fail_activity(FailActivityRequest {
                        claim,
                        failure: durust::DurableFailure::non_retryable("map-table.fatal", "fatal"),
                    })
                    .await
                    .unwrap();
                assert!(
                    matches!(outcome, FailActivityOutcome::Failed { .. }),
                    "{where_}: {outcome:?}"
                );
            }
            other => panic!("{where_}: unknown fanout action `{other}`"),
        }
    }

    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(50),
            max_events: 50,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap();
    let types: Vec<String> = history
        .events
        .iter()
        .map(|event| format!("{:?}", event.data.event_type()))
        .collect();
    assert_eq!(types, fanout.parent_history, "{name}");
}

async fn schedule_map<B>(backend: &B, fanout: &Fanout) -> durust::RunId
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<map_table_workflow>(
            "wf/map-table-fanout",
            "map-table-workflows",
            Item { value: 1 },
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("fanout-scheduler"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("map-table-workflows"),
                registered_workflow_types: vec![WorkflowType::new("map-table.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        (0..fanout.item_count)
            .map(|value| durust::encode_payload(&Item { value }).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let activity_name = ActivityName::new("map-table.item");
    let task_queue = TaskQueue::new("map-table-activities");
    let retry_policy = durust::RetryPolicy::none();
    let fingerprint = durust::activity_map_fingerprint(
        activity_name.clone(),
        durust::payload_digest(&input_manifest),
        "mapped".to_owned(),
        fanout.max_in_flight,
        "sha256:map-table".to_owned(),
    );
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityMapScheduled(durust::ActivityMapScheduled {
                        command_id: command_id.clone(),
                        activity_name: activity_name.clone(),
                        task_queue: task_queue.clone(),
                        retry_policy: retry_policy.clone(),
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                        input_manifest: input_manifest.clone(),
                        result_manifest_name: "mapped".to_owned(),
                        max_in_flight: fanout.max_in_flight,
                        fingerprint,
                    }),
                )],
                schedule_activity_maps: vec![ActivityMapTask {
                    map_command_id: command_id,
                    activity_name,
                    task_queue,
                    retry_policy,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                    input_manifest,
                    result_manifest_name: "mapped".to_owned(),
                    max_in_flight: fanout.max_in_flight,
                }],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    run_id
}
