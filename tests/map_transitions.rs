//! The Rust half of the shared map-fanout transition table.
//!
//! The table is checked in at `typescript/fixtures/contract/map-transitions.json`
//! and read by two runners — this one and
//! `typescript/packages/core/test/map-engine.test.ts` — so the Rust and
//! TypeScript map engines cannot drift apart silently.
//!
//! Both sections are asserted. The `transitions` section is single-step engine
//! cases, descriptor state plus one event in and an ordered effect list out,
//! replayed through `durust::map_engine::step` (a `#[doc(hidden)]` module
//! exposed for this runner) with the same projection the TypeScript runner
//! applies. The `fanouts` section is whole fanouts scripted purely in terms of
//! the public provider API, which the TypeScript runner replays against
//! `MemoryBackend` too. Those cases pin the parts of the machine both runtimes
//! must agree on and both can reach — the `max_in_flight` admission sequence,
//! one replacement admitted per released slot, and a failed map that abandons
//! its siblings rather than leaving them completable.
//!
//! Deliberately excluded, and asserted to stay excluded: retry delay *values*
//! (the two runtimes have different policy models) and `ParentCancelled`
//! (whose transition is only half the behaviour). The fixture states each
//! exclusion and its reason.
//!
//! A third exclusion — every `DescriptorCreated` case where
//! `recorded_outcomes >= item_count` — has been **retired**. It was written
//! when TypeScript completed such a map at descriptor creation and Rust
//! stalled; both engines now complete it, and both drop the parent
//! notification when the same commit closed the parent run. The table asserts
//! the predicate instead of excluding it, and
//! `shared_table_asserts_the_retired_descriptor_created_predicate` refuses a
//! silent slide back: deleting the cases fails here, and re-adding the
//! exclusion fails the list above.

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

/// How many cases each section of the shared table holds. Every assertion in
/// this file about either section runs inside a `for` loop, and a loop over an
/// empty collection asserts nothing while still reporting `ok` — so these
/// counts are the floor that keeps the loops from going quiet.
///
/// The hole they close is narrow and real. Neither `fanouts` nor `transitions`
/// carries `#[serde(default)]`, so deleting the *key* fails to deserialize and
/// every test here fails loudly. But `"fanouts": []` deserializes fine:
/// emptying the array left `memory_replays_every_shared_table_fanout` and
/// `sqlite_replays_every_shared_table_fanout` passing with an unchanged test
/// count, replaying nothing, in `0.00s` instead of `0.04s`. That is the only
/// difference an emptied table produced.
///
/// The counts are exact rather than `> 0` on purpose. The table is checked in,
/// so it changes only deliberately, and an exact count also catches the case a
/// non-empty check cannot: cases quietly deleted to make a failure go away.
/// Adding or retiring a case is expected to move the number here in the same
/// commit — that edit is the point, not an obstacle.
const SHARED_TABLE_FANOUTS: usize = 5;
const SHARED_TABLE_TRANSITIONS: usize = 27;

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
    assert_eq!(
        table.transitions.len(),
        SHARED_TABLE_TRANSITIONS,
        "the shared table's single-step section changed size; every loop over \
         `transitions` in this file asserts nothing on the cases that are no \
         longer there, so update SHARED_TABLE_TRANSITIONS deliberately or put \
         the cases back"
    );
    for case in &table.transitions {
        let event_kind = case["event"]["kind"].as_str().expect("event kind");
        assert_ne!(
            event_kind, "ParentCancelled",
            "the table excludes the ParentCancelled event"
        );
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

/// The other half of retiring an exclusion: the table has to actually assert
/// the predicate that used to be excluded, or the retirement is a deletion
/// dressed up as a convergence.
///
/// Both parent states are required. `parentTerminal: false` is the arm both
/// engines already agreed on once Rust stopped stalling; `parentTerminal:
/// true` is the arm TypeScript moved on — it used to emit `CompleteMap`, which
/// appended the map's terminal fact *behind* the run's own terminal event.
#[test]
fn shared_table_asserts_the_retired_descriptor_created_predicate() {
    let table = load_table();
    let mut seen_parent_terminal: Vec<bool> = Vec::new();
    for case in &table.transitions {
        if case["event"]["kind"] != "DescriptorCreated" {
            continue;
        }
        let recorded = case["state"]["recordedOutcomes"]
            .as_u64()
            .expect("recorded");
        let item_count = case["state"]["itemCount"].as_u64().expect("itemCount");
        if recorded < item_count {
            continue;
        }
        let parent_terminal = case["event"]["parentTerminal"]
            .as_bool()
            .expect("parentTerminal");
        if !seen_parent_terminal.contains(&parent_terminal) {
            seen_parent_terminal.push(parent_terminal);
        }
    }
    seen_parent_terminal.sort_unstable();
    assert_eq!(
        seen_parent_terminal,
        vec![false, true],
        "the table must assert the DescriptorCreated predicate it stopped excluding, \
         for a closed parent as well as an open one"
    );
}

/// One engine step per table case, projected onto the fields the table
/// declares normative exactly as `tableEffect` does in
/// `typescript/packages/core/test/map-engine.test.ts`: payloads, failures, and
/// reasons are dropped, and the two retry instants reduce to whether they are
/// set.
#[test]
fn shared_table_transitions_replay_through_the_engine() {
    use durust::map_engine::{
        ItemAttemptFailureKind, ItemRetryDecision, MapEffect, MapEvent, MapKind, MapReject,
        MapState, step,
    };
    use serde_json::{Value, json};

    let table = load_table();
    assert_eq!(table.transitions.len(), SHARED_TABLE_TRANSITIONS);
    let map_command_id = durust::command_id(&durust::RunId::new("run-1"), 7);
    let failure = durust::DurableFailure::non_retryable("table.item", "item failed");
    let now = durust::TimestampMs(1_700_000_000_000);

    let outcome = |value: &Value| match value["kind"].as_str().expect("outcome kind") {
        "Succeeded" => durust::ChildWorkflowMapItemOutcome::Succeeded {
            result: durust::encode_payload(&1_u64).unwrap(),
        },
        "Failed" => durust::ChildWorkflowMapItemOutcome::Failed {
            failure: failure.clone(),
        },
        "Cancelled" => durust::ChildWorkflowMapItemOutcome::Cancelled {
            reason: "cancelled".to_owned(),
        },
        other => panic!("unknown outcome kind `{other}`"),
    };
    let project = |effect: &MapEffect| -> Value {
        match effect {
            MapEffect::RecordItemOutcome { ordinal, outcome } => json!({
                "kind": "RecordItemOutcome",
                "ordinal": ordinal,
                "outcome": { "kind": match outcome {
                    durust::ChildWorkflowMapItemOutcome::Succeeded { .. } => "Succeeded",
                    durust::ChildWorkflowMapItemOutcome::Failed { .. } => "Failed",
                    durust::ChildWorkflowMapItemOutcome::Cancelled { .. } => "Cancelled",
                } },
            }),
            MapEffect::MaterializeItems {
                first_ordinal,
                count,
            } => json!({
                "kind": "MaterializeItems", "firstOrdinal": first_ordinal, "count": count,
            }),
            MapEffect::AdvanceDescriptor {
                next_ordinal,
                in_flight,
            } => json!({
                "kind": "AdvanceDescriptor", "nextOrdinal": next_ordinal, "inFlight": in_flight,
            }),
            MapEffect::ScheduleItemRetry {
                ordinal,
                next_attempt,
                visible_at_ms,
                timeout_at_ms,
            } => {
                json!({
                    "kind": "ScheduleItemRetry",
                    "ordinal": ordinal,
                    "nextAttempt": next_attempt,
                    "visibleAtMs": if visible_at_ms.is_some() { "Deferred" } else { "Null" },
                    "timeoutAtMs": if timeout_at_ms.is_some() { "Present" } else { "Null" },
                })
            }
            MapEffect::CompleteMap { item_count } => json!({
                "kind": "CompleteMap", "itemCount": item_count,
            }),
            MapEffect::FailMap { .. } => json!({ "kind": "FailMap" }),
            MapEffect::AbandonPendingItems => json!({ "kind": "AbandonPendingItems" }),
            MapEffect::CancelChildren { .. } => json!({ "kind": "CancelChildren" }),
            MapEffect::MarkDescriptorTerminal => json!({ "kind": "MarkDescriptorTerminal" }),
        }
    };

    for case in &table.transitions {
        let name = case["name"].as_str().expect("case name");
        let state_json = &case["state"];
        let state = MapState {
            map_command_id: map_command_id.clone(),
            kind: match state_json["kind"].as_str().expect("state kind") {
                "Activity" => MapKind::Activity,
                "ChildWorkflow" => MapKind::ChildWorkflow,
                other => panic!("{name}: unknown map kind `{other}`"),
            },
            failure_mode: match state_json["failureMode"].as_str().expect("failure mode") {
                "FailFast" => durust::ChildWorkflowMapFailureMode::FailFast,
                "CollectAll" => durust::ChildWorkflowMapFailureMode::CollectAll,
                other => panic!("{name}: unknown failure mode `{other}`"),
            },
            item_count: state_json["itemCount"].as_u64().expect("itemCount"),
            next_ordinal: state_json["nextOrdinal"].as_u64().expect("nextOrdinal"),
            in_flight: state_json["inFlight"].as_u64().expect("inFlight"),
            max_in_flight: state_json["maxInFlight"].as_u64().expect("maxInFlight") as usize,
            recorded_outcomes: state_json["recordedOutcomes"]
                .as_u64()
                .expect("recordedOutcomes"),
            completed: state_json["completed"].as_bool().expect("completed"),
        };
        let event_json = &case["event"];
        let event = match event_json["kind"].as_str().expect("event kind") {
            "DescriptorCreated" => MapEvent::DescriptorCreated {
                parent_terminal: event_json["parentTerminal"]
                    .as_bool()
                    .expect("parentTerminal"),
            },
            "ItemCompleted" => MapEvent::ItemCompleted {
                ordinal: event_json["ordinal"].as_u64().expect("ordinal"),
                outcome: outcome(&event_json["outcome"]),
                already_recorded: event_json["alreadyRecorded"]
                    .as_bool()
                    .expect("alreadyRecorded"),
                parent_terminal: event_json["parentTerminal"]
                    .as_bool()
                    .expect("parentTerminal"),
            },
            "ItemAttemptFailed" => MapEvent::ItemAttemptFailed {
                ordinal: event_json["ordinal"].as_u64().expect("ordinal"),
                failure: failure.clone(),
                kind: match event_json["attemptFailure"]
                    .as_str()
                    .expect("attemptFailure")
                {
                    "Failed" => ItemAttemptFailureKind::Failed,
                    "TimedOut" => ItemAttemptFailureKind::TimedOut,
                    other => panic!("{name}: unknown attempt failure `{other}`"),
                },
                decision: match event_json["decision"]["kind"]
                    .as_str()
                    .expect("decision kind")
                {
                    "Retry" => ItemRetryDecision::Retry {
                        next_attempt: event_json["decision"]["nextAttempt"]
                            .as_u64()
                            .expect("nextAttempt") as u32,
                    },
                    "Exhausted" => ItemRetryDecision::Exhausted,
                    other => panic!("{name}: unknown decision `{other}`"),
                },
                failed_attempt: event_json["failedAttempt"].as_u64().expect("failedAttempt") as u32,
                retry_policy: match event_json["retryBackoff"].as_str().expect("retryBackoff") {
                    "Immediate" => durust::RetryPolicy::none().max_attempts(9),
                    "Deferred" => durust::RetryPolicy::exponential().max_attempts(9),
                    other => panic!("{name}: unknown backoff `{other}`"),
                },
                start_to_close_timeout: match event_json["startToCloseTimeout"]
                    .as_str()
                    .expect("startToCloseTimeout")
                {
                    "Present" => Some(Duration::from_millis(30_000)),
                    "Null" => None,
                    other => panic!("{name}: unknown timeout marker `{other}`"),
                },
                now,
                already_recorded: event_json["alreadyRecorded"]
                    .as_bool()
                    .expect("alreadyRecorded"),
                parent_terminal: event_json["parentTerminal"]
                    .as_bool()
                    .expect("parentTerminal"),
            },
            other => panic!("{name}: the shared table excludes the {other} event"),
        };

        let expect = &case["expect"];
        match step(&state, event) {
            Ok(effects) => {
                assert_eq!(expect["kind"], "Effects", "{name}: expected a rejection");
                let projected = effects.iter().map(project).collect::<Vec<_>>();
                assert_eq!(Value::Array(projected), expect["effects"], "{name}");
            }
            Err(reject) => {
                assert_eq!(expect["kind"], "Reject", "{name}: expected effects");
                let projected = match reject {
                    MapReject::OutOfBounds { ordinal } => {
                        json!({ "kind": "OutOfBounds", "ordinal": ordinal })
                    }
                    MapReject::TerminalParent => json!({ "kind": "TerminalParent" }),
                };
                assert_eq!(projected, expect["reject"], "{name}");
            }
        }
    }
}

#[test]
fn memory_replays_every_shared_table_fanout() {
    block_on(async {
        let mut replayed = 0usize;
        for fanout in load_table().fanouts {
            replay_fanout(MemoryBackend::new(), &fanout).await;
            replayed += 1;
        }
        assert_replayed_every_fanout(replayed);
    });
}

#[test]
fn sqlite_replays_every_shared_table_fanout() {
    block_on(async {
        let mut replayed = 0usize;
        for fanout in load_table().fanouts {
            let dir = tempfile::tempdir().unwrap();
            let backend = SqliteBackend::open(dir.path().join("map-transitions.sqlite3")).unwrap();
            replay_fanout(backend, &fanout).await;
            replayed += 1;
        }
        assert_replayed_every_fanout(replayed);
    });
}

/// The floor for both replay tests: counted inside the loop and checked after
/// it, so the count is what each test actually executed rather than what the
/// file claims to contain. A replay test that replays nothing is not a passing
/// test — see `SHARED_TABLE_FANOUTS`.
fn assert_replayed_every_fanout(replayed: usize) {
    assert_eq!(
        replayed, SHARED_TABLE_FANOUTS,
        "this test replayed {replayed} of the shared table's \
         {SHARED_TABLE_FANOUTS} fanouts; a replay loop that runs fewer times \
         than the table has cases still reports `ok` while asserting nothing, \
         so restore the missing fanouts or move SHARED_TABLE_FANOUTS \
         deliberately"
    );
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
