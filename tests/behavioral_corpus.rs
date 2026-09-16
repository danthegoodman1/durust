//! The Rust half of the shared **behavioural** contract corpus.
//!
//! The corpus is checked in at
//! `typescript/fixtures/contract/behavioral-corpus.json` and read by two
//! runners — this one and
//! `typescript/packages/core/test/behavioral-corpus.test.ts`.
//!
//! **What it proves that the vocabulary fixtures do not.**
//! `core-events.json`, `provider-io.json`, and `benchmark-output.json` pin
//! *names*: event types, fingerprint helpers, payload-ref shapes, provider
//! request and outcome vocabulary. Two runtimes can agree on every one of
//! those names and still disagree about what a workflow task *does* — which
//! `select` branch wins a tie, whether a completion sitting unconsumed at the
//! replay cursor blocks the next command, whether a signal is consumed in the
//! task that observes it or the one after. This corpus pins that: each case is
//! `(workflow program, input history, live signals, now) -> commit`, the
//! commit is checked-in literal data, and both runtimes must reproduce it by
//! executing the case through their real worker.
//!
//! **The commit is captured, not reconstructed.** `RecordingBackend` wraps
//! `MemoryBackend` and records every `WorkflowTaskCommit` the worker hands to
//! `commit_workflow_task`/`commit_workflow_tasks`, then forwards it unchanged.
//! Nothing in the runner rebuilds a commit from the runtime's own state, so a
//! behavioural change in the runtime shows up as a corpus diff rather than
//! being re-derived into agreement.
//!
//! **Normalisation.** A commit cannot be compared field-for-field: several of
//! its fields are genuinely language-local (Rust's `RetryPolicy` is
//! `{backoff, max_attempts}`, TypeScript's is
//! `{initialIntervalMs, maxIntervalMs, maxAttempts, backoffCoefficient,
//! nonRetryableErrorTypes}`; a payload's `schemaFingerprint` hashes a Rust
//! type name on one side and a schema adapter or the literal `"unknown"` on
//! the other). Every such field is projected into a neutral form by
//! `commit_json` below, and every projection is declared in the corpus's
//! `exclusions` list with the reason. The `corpus_declares_its_exclusions`
//! test holds the file to that list so an exclusion cannot be quietly widened.
//!
//! Deliberately *not* excluded, and therefore asserted: the ordered append
//! list, the scheduled activity/map/child payloads, wait upserts and deletes
//! by kind and command, consumed signal ids, the query projection, the select
//! winner's branch ordinal and winning event id, and `expected_tail_event_id`.
//!
//! **Divergences are recorded, not normalised.** A step whose two runtimes
//! genuinely behave differently carries a `divergence` block — *both* observed
//! commits plus a `differences` list naming every field path that differs and
//! why — instead of one shared `expect`. Each runner asserts its own side, so
//! the difference stays visible as literal data and stays under test.
//!
//! `corpus_divergence_reasons_account_for_every_differing_field` is the part
//! that matters: it diffs the two recorded commits itself and requires the
//! declared paths to be *exactly* the set that differs. A block whose prose
//! explains one cause while a second cause rides along unlabelled fails. That
//! is not hypothetical — it is how the select cases were found to be carrying
//! two independent divergences (command-sequence allocation order, and
//! TypeScript never cancelling a losing branch's wait) under one heading.
//!
//! **Authoring.** The `expect` blocks were produced by this runner under
//! `DURUST_CORPUS_REGENERATE=1` and are checked in. The TypeScript runner has
//! no regeneration path: it can satisfy the file or fail. That asymmetry is
//! what makes the corpus a cross-implementation assertion rather than a
//! snapshot of one implementation.
//!
//! The asymmetry argument only holds while a regenerate run cannot be mistaken
//! for a passing one, which is what `is_not_a_regenerate_run` enforces:
//! `DURUST_CORPUS_REGENERATE=1` otherwise makes this runner silently rewrite
//! the file the *other* runner is measured against and then report `ok`, so a
//! single environment variable turned the half with teeth into a snapshot
//! writer that always passes. The TypeScript runner's `is not a print run` is
//! the same guard for its own escape hatch.

use durust::{
    ActivityMapInputManifest, Client, CodecId, DurableBackend, EventId, MemoryBackend, PayloadRef,
    TimestampMs, Worker, WorkflowTaskCommit,
};
use futures::executor::block_on;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CORPUS_WORKFLOW_QUEUE: &str = "corpus-workflows";
const CORPUS_ACTIVITY_QUEUE: &str = "corpus-activities";

// ---------------------------------------------------------------------------
// The program catalogue.
//
// Each program exists once per language with the same name and the same
// observable behaviour. The corpus names a program; it never carries code.
// Inputs and outputs are objects on both sides, because a TypeScript durable
// input must be an object and a bare scalar would encode differently anyway.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Value1 {
    value: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Text1 {
    text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Continue1 {
    remaining: u64,
    total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Approval {
    who: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CorpusQueryView {
    status: String,
    value: u64,
}

#[durust::activity(name = "corpus.double")]
async fn corpus_double(input: Value1) -> durust::Result<Value1> {
    Ok(Value1 {
        value: input.value * 2,
    })
}

#[durust::workflow(name = "corpus.child-increment", version = 1)]
async fn corpus_child_increment(input: Value1) -> durust::Result<Value1> {
    Ok(Value1 {
        value: input.value + 1,
    })
}

#[durust::workflow(name = "corpus.activity-then-return", version = 1)]
async fn corpus_activity_then_return(input: Value1) -> durust::Result<Value1> {
    let doubled = durust::call_activity!(corpus_double(Value1 { value: input.value }))
        .task_queue(CORPUS_ACTIVITY_QUEUE)
        .await?;
    Ok(Value1 {
        value: doubled.value,
    })
}

#[durust::workflow(name = "corpus.spawn-sleep-then-activity", version = 1)]
async fn corpus_spawn_sleep_then_activity(input: Value1) -> durust::Result<Value1> {
    let spawned = durust::call_activity!(corpus_double(Value1 { value: input.value }))
        .task_queue(CORPUS_ACTIVITY_QUEUE)
        .spawn()
        .await?;
    durust::sleep(Duration::from_millis(1_000)).await?;
    let second = durust::call_activity!(corpus_double(Value1 {
        value: input.value + 1
    }))
    .task_queue(CORPUS_ACTIVITY_QUEUE)
    .await?;
    let first = spawned.result().await?;
    Ok(Value1 {
        value: first.value + second.value,
    })
}

#[durust::workflow(name = "corpus.select-signal-or-timer", version = 1)]
async fn corpus_select_signal_or_timer(input: Value1) -> durust::Result<Text1> {
    let text = durust::select! {
        approved = durust::signal::<Approval>("corpus.approve") => {
            format!("signal:{}", approved?.who)
        }
        elapsed = durust::sleep(Duration::from_millis(input.value)) => {
            elapsed?;
            "timer".to_owned()
        }
    };
    Ok(Text1 { text })
}

/// The two-ready-branch select. Both branches resolve from real, distinct
/// history events, so a case can put either branch's event first and the
/// winner is decided purely by ready event id — which is what makes the
/// tie-break rule assertable at all. A select with one ready branch pins
/// nothing: every comparator returns the same winner.
#[durust::workflow(name = "corpus.select-activity-or-timer", version = 1)]
async fn corpus_select_activity_or_timer(input: Value1) -> durust::Result<Text1> {
    let text = durust::select! {
        doubled = durust::call_activity!(corpus_double(Value1 { value: input.value }))
            .task_queue(CORPUS_ACTIVITY_QUEUE) => {
            format!("activity:{}", doubled?.value)
        }
        elapsed = durust::sleep(Duration::from_millis(1_000)) => {
            elapsed?;
            "timer".to_owned()
        }
    };
    Ok(Text1 { text })
}

#[durust::workflow(name = "corpus.signal-then-publish", version = 1, query_state = CorpusQueryView)]
async fn corpus_signal_then_publish(input: Value1) -> durust::Result<Value1> {
    durust::publish(&CorpusQueryView {
        status: "waiting".to_owned(),
        value: input.value,
    })?;
    let approved = durust::signal::<Approval>("corpus.approve").await?;
    durust::publish(&CorpusQueryView {
        status: approved.who,
        value: input.value + 1,
    })?;
    Ok(Value1 {
        value: input.value + 1,
    })
}

/// Schedules its timer in a task that runs *after* the virtual clock has
/// moved, which is the only shape that exposes the `nowMs` divergence: the
/// TypeScript worker never supplies `PrepareWorkflowTaskOptions.nowMs`, so its
/// executions compute `fireAt` from 0 forever.
#[durust::workflow(name = "corpus.sleep-after-signal", version = 1)]
async fn corpus_sleep_after_signal(input: Value1) -> durust::Result<Value1> {
    let approved = durust::signal::<Approval>("corpus.approve").await?;
    durust::sleep(Duration::from_millis(input.value)).await?;
    Ok(Value1 {
        value: input.value + u64::try_from(approved.who.len()).unwrap_or(0),
    })
}

#[durust::workflow(name = "corpus.child-await", version = 1)]
async fn corpus_child_await(input: Value1) -> durust::Result<Value1> {
    let child = durust::child!(corpus_child_increment(Value1 { value: input.value }))
        .workflow_id("wf/corpus/child")
        .task_queue(CORPUS_WORKFLOW_QUEUE)
        .spawn()
        .await?;
    let result = child.result().await?;
    Ok(Value1 {
        value: result.value,
    })
}

#[durust::workflow(name = "corpus.activity-map", version = 1)]
async fn corpus_activity_map(input: Value1) -> durust::Result<Value1> {
    let input_manifest =
        durust::activity_map_manifest((0..input.value).map(|value| Value1 { value: value + 1 }))?;
    let mapped = durust::activity_map(corpus_double)
        .task_queue(CORPUS_ACTIVITY_QUEUE)
        .input_manifest(input_manifest)
        .max_in_flight(2)
        .result_manifest("doubled")
        .spawn()
        .await?;
    let manifest = mapped.result_manifest().await?;
    let refs = durust::decode_activity_map_result_refs(&manifest)?;
    let mut total = 0;
    for payload in &refs {
        total += durust::decode_payload::<Value1>(payload)?.value;
    }
    Ok(Value1 { value: total })
}

#[durust::workflow(name = "corpus.markers", version = 1)]
async fn corpus_markers(input: Value1) -> durust::Result<Value1> {
    let version = durust::get_version("corpus.change", 1, 1)?;
    let tagged = durust::side_effect("corpus.tag", move || Value1 {
        value: input.value + 100,
    })
    .await?;
    durust::deprecate_patch("corpus.retired")?;
    Ok(Value1 {
        value: tagged.value + u64::try_from(version).unwrap_or(0),
    })
}

/// One change id consulted on both sides of a task boundary: each call records
/// its own marker, so the second task appends a second `VersionMarker`.
#[durust::workflow(name = "corpus.repeated-change-id", version = 1)]
async fn corpus_repeated_change_id(input: Value1) -> durust::Result<Value1> {
    let first = durust::patched("corpus.repeat")?;
    let doubled = durust::call_activity!(corpus_double(Value1 { value: input.value }))
        .task_queue(CORPUS_ACTIVITY_QUEUE)
        .await?;
    let second = durust::patched("corpus.repeat")?;
    Ok(Value1 {
        value: doubled.value + u64::from(first) + u64::from(second),
    })
}

/// The signal branch registers before the timer, and a later task replays the
/// settled select: the recorded `SignalConsumed` sits behind the timer's
/// command event, so it must be matched by command id, not by position.
#[durust::workflow(name = "corpus.signal-first-select-then-timer", version = 1)]
async fn corpus_signal_first_select_then_timer(input: Value1) -> durust::Result<Text1> {
    let text = durust::select! {
        approved = durust::signal::<Approval>("corpus.approve") => {
            format!("signal:{}", approved?.who)
        }
        elapsed = durust::sleep(Duration::from_millis(input.value)) => {
            elapsed?;
            "timer".to_owned()
        }
    };
    durust::sleep(Duration::from_millis(10)).await?;
    Ok(Text1 { text })
}

/// A workflow that fails on purpose commits `WorkflowFailed` with the failure
/// it named; both runtimes record the same terminal event.
#[durust::workflow(name = "corpus.fails", version = 1)]
async fn corpus_fails(input: Value1) -> durust::Result<Value1> {
    if input.value > 0 {
        return Err(durust::Error::non_retryable(
            "corpus.rejected",
            "rejected on purpose",
        ));
    }
    Ok(input)
}

/// `now()` records the provider clock as a side-effect marker; the second call
/// observes the advanced clock and both values are replayed as recorded.
#[durust::workflow(name = "corpus.now-twice", version = 1)]
async fn corpus_now_twice(input: Value1) -> durust::Result<Value1> {
    let first = durust::now().await?;
    durust::sleep(Duration::from_millis(input.value)).await?;
    let second = durust::now().await?;
    Ok(Value1 {
        value: u64::try_from(second.0 - first.0).unwrap_or(0),
    })
}

#[durust::workflow(name = "corpus.continue-as-new", version = 1)]
async fn corpus_continue_as_new(input: Continue1) -> durust::Result<Value1> {
    if input.remaining > 0 {
        return durust::continue_as_new(Continue1 {
            remaining: input.remaining - 1,
            total: input.total + 1,
        });
    }
    Ok(Value1 { value: input.total })
}

// ---------------------------------------------------------------------------
// Neutral projection of a commit.
// ---------------------------------------------------------------------------

/// Stand-in for `PayloadRef::schema_fingerprint`. Rust hashes
/// `std::any::type_name::<T>()`; TypeScript hashes a schema adapter's
/// fingerprint or the literal `"unknown"`. Neither can produce the other's.
const SCHEMA_PLACEHOLDER: &str = "<schema-fingerprint>";
/// Stand-in for the activity/activity-map `options_digest`. Rust hashes
/// MessagePack of `ActivityOptions` (snake_case, `Duration` as `{secs,nanos}`);
/// TypeScript hashes `JSON.stringify` of a camelCase object.
const ACTIVITY_OPTIONS_PLACEHOLDER: &str = "<activity-options-digest>";
/// Stand-in for `SelectWinner::branches_digest`. Rust records `select:{n}` /
/// `select_all:{n}`; TypeScript records a SHA-256 over the branch *keys*,
/// which Rust's positional macro does not have.
const SELECT_BRANCHES_PLACEHOLDER: &str = "<select-branches-digest>";
/// Stand-in for a map fingerprint's `input_digest`, which hashes the encoded
/// manifest rather than any item value.
const MANIFEST_DIGEST_PLACEHOLDER: &str = "<manifest-digest>";

fn codec_name(codec: CodecId) -> &'static str {
    match codec {
        CodecId::Json => "Json",
        CodecId::MessagePack => "MessagePack",
    }
}

/// A plain user payload: the codec plus the decoded value. `schema_fingerprint`
/// is dropped (see `SCHEMA_PLACEHOLDER`); the encoded bytes are not compared
/// directly because they carry no information the decoded value does not,
/// while `schema_fingerprint` would drag the whole ref into a divergence.
fn payload_json(payload: &PayloadRef) -> Value {
    let decoded: Value = durust::decode_payload(payload)
        .expect("corpus payloads are inline and decode to a neutral JSON value");
    json!({
        "codec": codec_name(payload.codec()),
        "schemaFingerprint": SCHEMA_PLACEHOLDER,
        "value": decoded,
    })
}

fn optional_payload_json(payload: Option<&PayloadRef>) -> Value {
    payload.map_or(Value::Null, payload_json)
}

/// An activity-map input manifest, summarised by *content* rather than by its
/// wire bytes. Rust serialises the manifest with snake_case field names and an
/// externally tagged `PayloadRef`; TypeScript serialises camelCase with an
/// internally tagged one. The layout (item count, page lengths) and the item
/// values are the parts both runtimes must agree on, and they are what a
/// worker actually reads.
fn activity_map_manifest_json(payload: &PayloadRef) -> Value {
    let manifest: ActivityMapInputManifest =
        durust::decode_payload(payload).expect("activity map input manifest decodes");
    let items: Vec<Value> = (0..manifest.item_count)
        .map(|ordinal| {
            payload_json(
                &durust::activity_map_input_at(&manifest, ordinal as u64)
                    .expect("activity map manifest covers every ordinal"),
            )
        })
        .collect();
    json!({
        "itemCount": manifest.item_count,
        "pageLengths": manifest.page_lengths,
        "items": items,
    })
}

fn command_id_json(command_id: &durust::CommandId) -> Value {
    json!({ "runId": command_id.run_id.0, "seq": command_id.seq.0 })
}

/// Only `max_attempts` survives. The two retry-policy models share no other
/// field, and the plan already records the delay values as a forced
/// divergence.
fn retry_policy_json(policy: &durust::RetryPolicy) -> Value {
    json!({ "maxAttempts": policy.max_attempts })
}

fn duration_ms_json(duration: Option<Duration>) -> Value {
    duration.map_or(Value::Null, |duration| {
        json!(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
    })
}

fn fingerprint_json(fingerprint: &durust::CommandFingerprint) -> Value {
    let options_digest = match fingerprint.kind {
        durust::CommandKind::Activity | durust::CommandKind::ActivityMap => {
            ACTIVITY_OPTIONS_PLACEHOLDER.to_owned()
        }
        _ => fingerprint.options_digest.clone(),
    };
    // A map fingerprint's input digest hashes the *manifest* bytes, and the
    // manifest wire encoding is itself excluded; the manifest's content is
    // asserted separately by `activity_map_manifest_json`.
    let input_digest = match fingerprint.kind {
        durust::CommandKind::ActivityMap | durust::CommandKind::ChildWorkflowMap => {
            Some(MANIFEST_DIGEST_PLACEHOLDER.to_owned())
        }
        _ => fingerprint.input_digest.clone(),
    };
    json!({
        "kind": format!("{:?}", fingerprint.kind),
        "name": fingerprint.name,
        "inputDigest": input_digest,
        "optionsDigest": options_digest,
    })
}

fn event_json(data: &durust::HistoryEventData) -> Value {
    use durust::HistoryEventData as E;
    match data {
        E::WorkflowCompleted { result } => json!({
            "type": "WorkflowCompleted",
            "result": payload_json(result),
        }),
        E::WorkflowFailed { failure } => json!({
            "type": "WorkflowFailed",
            "failure": failure_json(failure),
        }),
        E::WorkflowCancelled { reason } => json!({
            "type": "WorkflowCancelled",
            "reason": reason,
        }),
        E::WorkflowContinuedAsNew { input } => json!({
            "type": "WorkflowContinuedAsNew",
            "input": payload_json(input),
        }),
        E::ActivityScheduled(scheduled) => json!({
            "type": "ActivityScheduled",
            "commandId": command_id_json(&scheduled.command_id),
            "activityName": scheduled.activity_name.0,
            "taskQueue": scheduled.task_queue.0,
            "retryPolicy": retry_policy_json(&scheduled.retry_policy),
            "startToCloseTimeoutMs": duration_ms_json(scheduled.start_to_close_timeout),
            "heartbeatTimeoutMs": duration_ms_json(scheduled.heartbeat_timeout),
            "input": payload_json(&scheduled.input),
            "fingerprint": fingerprint_json(&scheduled.fingerprint),
        }),
        E::ActivityMapScheduled(scheduled) => json!({
            "type": "ActivityMapScheduled",
            "commandId": command_id_json(&scheduled.command_id),
            "activityName": scheduled.activity_name.0,
            "taskQueue": scheduled.task_queue.0,
            "retryPolicy": retry_policy_json(&scheduled.retry_policy),
            "startToCloseTimeoutMs": duration_ms_json(scheduled.start_to_close_timeout),
            "heartbeatTimeoutMs": duration_ms_json(scheduled.heartbeat_timeout),
            "inputManifest": activity_map_manifest_json(&scheduled.input_manifest),
            "resultManifestName": scheduled.result_manifest_name,
            "maxInFlight": scheduled.max_in_flight,
            "fingerprint": fingerprint_json(&scheduled.fingerprint),
        }),
        E::ChildWorkflowStartRequested(requested) => json!({
            "type": "ChildWorkflowStartRequested",
            "commandId": command_id_json(&requested.command_id),
            "workflowType": {
                "name": requested.workflow_type.name,
                "version": requested.workflow_type.version,
            },
            "workflowId": requested.workflow_id.0,
            "taskQueue": requested.task_queue.0,
            "input": payload_json(&requested.input),
            "parentClosePolicy": format!("{:?}", requested.parent_close_policy),
            "fingerprint": fingerprint_json(&requested.fingerprint),
        }),
        E::TimerStarted(started) => json!({
            "type": "TimerStarted",
            "commandId": command_id_json(&started.command_id),
            "fireAtMs": started.fire_at.0,
            "fingerprint": fingerprint_json(&started.fingerprint),
        }),
        E::SignalConsumed(consumed) => json!({
            "type": "SignalConsumed",
            "commandId": command_id_json(&consumed.command_id),
            "signalId": consumed.signal_id.0,
            "signalName": consumed.signal_name.0,
            "payload": payload_json(&consumed.payload),
            "fingerprint": fingerprint_json(&consumed.fingerprint),
        }),
        E::SelectWinner(winner) => json!({
            "type": "SelectWinner",
            "selectCommandId": command_id_json(&winner.select_command_id),
            "branchOrdinal": winner.branch_ordinal,
            "branchesDigest": SELECT_BRANCHES_PLACEHOLDER,
        }),
        E::VersionMarker(marker) => json!({
            "type": "VersionMarker",
            "commandId": command_id_json(&marker.command_id),
            "changeId": marker.change_id,
            "version": marker.version,
        }),
        E::DeprecatedPatchMarker(marker) => json!({
            "type": "DeprecatedPatchMarker",
            "commandId": command_id_json(&marker.command_id),
            "patchId": marker.patch_id,
        }),
        E::SideEffectMarker(marker) => json!({
            "type": "SideEffectMarker",
            "commandId": command_id_json(&marker.command_id),
            "key": marker.key,
            "value": payload_json(&marker.value),
        }),
        other => panic!(
            "the behavioural corpus has no neutral encoding for {:?}; add one rather than \
             letting the case pass on a partial comparison",
            other.event_type()
        ),
    }
}

fn failure_json(failure: &durust::DurableFailure) -> Value {
    json!({
        "errorType": failure.error_type,
        "message": failure.message,
        "nonRetryable": failure.non_retryable,
        "details": optional_payload_json(failure.details.as_ref()),
    })
}

/// A wait, keyed by what it *is* rather than by the provider key string. The
/// two runtimes format the key differently (`{run}:{seq}:timer` in Rust,
/// `{run}:timer:{seq}` in TypeScript), which is opaque within a store but not
/// comparable across one.
fn wait_json(wait: &durust::WaitRecord) -> Value {
    json!({
        "kind": format!("{:?}", wait.kind),
        "commandId": command_id_json(&wait.command_id),
        "key": wait.key,
        "readyAtMs": wait.ready_at.map_or(Value::Null, |ready_at| json!(ready_at.0)),
    })
}

/// `delete_waits` carries bare ids, so the runner parses its own format back
/// into the same neutral triple `upsert_waits` reports.
fn wait_id_json(wait_id: &durust::WaitId) -> Value {
    let raw = &wait_id.0;
    let (prefix, kind) = raw
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("unrecognised Rust wait id `{raw}`"));
    let (run_id, seq) = prefix
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("unrecognised Rust wait id `{raw}`"));
    json!({
        "kind": match kind {
            "timer" => "Timer",
            "signal" => "Signal",
            other => panic!("unrecognised Rust wait id kind `{other}`"),
        },
        "commandId": { "runId": run_id, "seq": seq.parse::<u64>().expect("wait id seq") },
    })
}

fn activity_task_json(task: &durust::ActivityTask) -> Value {
    json!({
        "activityId": task.activity_id.0,
        "runId": task.run_id.0,
        "commandId": command_id_json(&task.command_id),
        "activityName": task.activity_name.0,
        "taskQueue": task.task_queue.0,
        "retryPolicy": retry_policy_json(&task.retry_policy),
        "startToCloseTimeoutMs": duration_ms_json(task.start_to_close_timeout),
        "heartbeatTimeoutMs": duration_ms_json(task.heartbeat_timeout),
        "attempt": task.attempt,
        "input": payload_json(&task.input),
        "mapItem": task.map_item.as_ref().map_or(Value::Null, |item| {
            json!({
                "mapCommandId": command_id_json(&item.map_command_id),
                "itemOrdinal": item.item_ordinal,
            })
        }),
    })
}

fn activity_map_task_json(task: &durust::ActivityMapTask) -> Value {
    json!({
        "mapCommandId": command_id_json(&task.map_command_id),
        "activityName": task.activity_name.0,
        "taskQueue": task.task_queue.0,
        "retryPolicy": retry_policy_json(&task.retry_policy),
        "startToCloseTimeoutMs": duration_ms_json(task.start_to_close_timeout),
        "heartbeatTimeoutMs": duration_ms_json(task.heartbeat_timeout),
        "inputManifest": activity_map_manifest_json(&task.input_manifest),
        "resultManifestName": task.result_manifest_name,
        "maxInFlight": task.max_in_flight,
    })
}

fn child_start_json(message: &durust::ChildStartOutboxMessage) -> Value {
    json!({
        "commandId": command_id_json(&message.command_id),
        "workflowType": {
            "name": message.workflow_type.name,
            "version": message.workflow_type.version,
        },
        "workflowId": message.workflow_id.0,
        "taskQueue": message.task_queue.0,
        "input": payload_json(&message.input),
        "parentClosePolicy": format!("{:?}", message.parent_close_policy),
    })
}

fn commit_json(commit: &WorkflowTaskCommit) -> Value {
    // Rust-only field: `WorkflowTaskCommit` has no `cancelCommands` in
    // TypeScript at all, so a corpus case that produced one would be asserting
    // something one runtime cannot express. Checked rather than ignored.
    assert!(
        commit.cancel_commands.is_empty(),
        "the behavioural corpus excludes cancel_commands: no TypeScript producer exists"
    );
    assert!(
        commit.schedule_child_workflow_maps.is_empty(),
        "no corpus case schedules a child workflow map yet; add a neutral encoding first"
    );
    json!({
        "appendEvents": commit
            .append_events
            .iter()
            .map(|event| event_json(&event.data))
            .collect::<Vec<_>>(),
        "upsertWaits": commit.upsert_waits.iter().map(wait_json).collect::<Vec<_>>(),
        "deleteWaits": commit.delete_waits.iter().map(wait_id_json).collect::<Vec<_>>(),
        "consumeSignals": commit
            .consume_signals
            .iter()
            .map(|signal_id| signal_id.0.clone())
            .collect::<Vec<_>>(),
        "scheduleActivities": commit
            .schedule_activities
            .iter()
            .map(activity_task_json)
            .collect::<Vec<_>>(),
        "scheduleActivityMaps": commit
            .schedule_activity_maps
            .iter()
            .map(activity_map_task_json)
            .collect::<Vec<_>>(),
        "startChildWorkflows": commit
            .start_child_workflows
            .iter()
            .map(child_start_json)
            .collect::<Vec<_>>(),
        "queryProjection": optional_payload_json(commit.query_projection.as_ref()),
    })
}

// ---------------------------------------------------------------------------
// The recording backend.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RecordingBackend {
    inner: MemoryBackend,
    commits: Arc<Mutex<Vec<WorkflowTaskCommit>>>,
}

impl RecordingBackend {
    fn new() -> Self {
        Self {
            inner: MemoryBackend::new(),
            commits: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record(&self, commit: &WorkflowTaskCommit) {
        self.commits
            .lock()
            .expect("corpus commit log")
            .push(commit.clone());
    }

    fn take_commits(&self) -> Vec<WorkflowTaskCommit> {
        std::mem::take(&mut *self.commits.lock().expect("corpus commit log"))
    }

    fn advance_time(&self, duration: Duration) {
        self.inner.advance_time(duration);
    }
}

macro_rules! forward_to_inner {
    ($(fn $method:ident($($arg:ident: $arg_ty:ty),*) -> $out:ty;)*) => {
        $(
            fn $method(&self, $($arg: $arg_ty),*) -> BoxFuture<'static, durust::Result<$out>> {
                self.inner.$method($($arg),*)
            }
        )*
    };
}

impl DurableBackend for RecordingBackend {
    fn payload_storage_config(&self) -> durust::PayloadStorageConfig {
        self.inner.payload_storage_config()
    }

    fn commit_workflow_task(
        &self,
        claim: durust::WorkflowTaskClaim,
        commit: WorkflowTaskCommit,
    ) -> BoxFuture<'static, durust::Result<durust::EventId>> {
        self.record(&commit);
        self.inner.commit_workflow_task(claim, commit)
    }

    fn commit_workflow_tasks(
        &self,
        batch: durust::WorkflowTaskCommitBatch,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::WorkflowTaskCommitBatchResult>>> {
        for input in &batch.commits {
            self.record(&input.commit);
        }
        self.inner.commit_workflow_tasks(batch)
    }

    forward_to_inner! {
        fn start_workflow(req: durust::StartWorkflowRequest) -> durust::StartWorkflowOutcome;
        fn cancel_workflow(req: durust::CancelWorkflowRequest) -> durust::CancelWorkflowOutcome;
        fn current_time() -> durust::TimestampMs;
        fn claim_workflow_task(
            worker_id: durust::WorkerId,
            opts: durust::ClaimWorkflowTaskOptions
        ) -> Option<durust::ClaimedWorkflowTask>;
        fn claim_workflow_tasks(
            worker_id: durust::WorkerId,
            opts: durust::ClaimWorkflowTasksOptions
        ) -> Vec<durust::ClaimedWorkflowTask>;
        fn stream_history(req: durust::StreamHistoryRequest) -> durust::HistoryChunk;
        fn stream_history_for_replay(req: durust::StreamHistoryRequest) -> durust::HistoryChunk;
        fn hydrate_payload(payload: durust::PayloadRef) -> durust::PayloadRef;
        fn hydrate_activity_map_result_manifest(
            payload: durust::PayloadRef
        ) -> durust::PayloadRef;
        fn hydrate_child_workflow_map_result_manifest(
            payload: durust::PayloadRef
        ) -> durust::PayloadRef;
        fn release_workflow_task(
            claim: durust::WorkflowTaskClaim,
            release: durust::WorkflowTaskRelease
        ) -> ();
        fn signal_workflow(req: durust::SignalWorkflowRequest) -> durust::SignalWorkflowOutcome;
        fn read_signal_inbox(
            req: durust::ReadSignalInboxRequest
        ) -> Option<durust::SignalInboxRecord>;
        fn read_signal_inboxes(
            req: durust::ReadSignalInboxesRequest
        ) -> Vec<Option<durust::SignalInboxRecord>>;
        fn fire_due_timers(req: durust::FireDueTimersRequest) -> durust::FireDueTimersOutcome;
        fn timeout_due_activities(
            req: durust::TimeoutDueActivitiesRequest
        ) -> durust::TimeoutDueActivitiesOutcome;
        fn run_due_maintenance(
            req: durust::RunDueMaintenanceRequest
        ) -> durust::RunDueMaintenanceOutcome;
        fn wait_for_ready(req: durust::WaitForReadyRequest) -> ();
        fn claim_activity_task(
            worker_id: durust::WorkerId,
            opts: durust::ClaimActivityOptions
        ) -> Option<durust::ClaimedActivityTask>;
        fn claim_activity_tasks(
            worker_id: durust::WorkerId,
            opts: durust::ClaimActivityTasksOptions
        ) -> Vec<durust::ClaimedActivityTask>;
        fn heartbeat_activity(
            req: durust::ActivityHeartbeatRequest
        ) -> durust::ActivityHeartbeatOutcome;
        fn complete_activity(
            req: durust::CompleteActivityRequest
        ) -> durust::CompleteActivityOutcome;
        fn complete_activity_tasks(
            req: durust::CompleteActivityTasksRequest
        ) -> Vec<durust::CompleteActivityTaskBatchResult>;
        fn fail_activity(req: durust::FailActivityRequest) -> durust::FailActivityOutcome;
        fn dispatch_child_workflow_starts(
            req: durust::DispatchChildWorkflowStartsRequest
        ) -> durust::DispatchChildWorkflowStartsOutcome;
        fn query_projection(req: durust::QueryProjectionRequest) -> durust::QueryProjectionOutcome;
        fn workflow_change_versions(
            req: durust::WorkflowChangeVersionsRequest
        ) -> durust::WorkflowChangeVersionsOutcome;
        fn payload_roots() -> durust::PayloadRootsOutcome;
        fn gc_payload_blobs(
            req: durust::PayloadGarbageCollectionRequest
        ) -> durust::PayloadGarbageCollectionOutcome;
    }
}

// ---------------------------------------------------------------------------
// The corpus file.
// ---------------------------------------------------------------------------

const CORPUS_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/typescript/fixtures/contract/behavioral-corpus.json"
);

fn load_corpus() -> Value {
    serde_json::from_str(&std::fs::read_to_string(CORPUS_PATH).expect("behavioural corpus"))
        .expect("behavioural corpus is valid JSON")
}

/// The projections and scope statements the runners implement, each with the
/// `kind` that says which it is. "Cannot be compared" and "is not compared"
/// are different claims and are not allowed to look alike: an exclusion is
/// something `commit_json` actively rewrites (`Projection`) or actively checks
/// (`Assertion`) or a statement about what the corpus is (`Scope`). Anything
/// the corpus simply does not reach lives in `DECLARED_GAPS` instead.
const DECLARED_EXCLUSIONS: [(&str, &str); 8] = [
    ("PayloadRef.schemaFingerprint", "Projection"),
    (
        "CommandFingerprint.optionsDigest for the Activity and ActivityMap kinds",
        "Projection",
    ),
    (
        "RetryPolicy beyond maxAttempts, including every retry delay value",
        "Projection",
    ),
    ("SelectWinner.branchesDigest", "Projection"),
    ("WaitId string format", "Projection"),
    ("The activity-map manifest wire encoding", "Projection"),
    (
        "WorkflowTaskCommit.cancelCommands and the ParentCancelled event",
        "Assertion",
    ),
    ("The execution mechanism", "Scope"),
];

/// Behaviour no case reaches. Listed and asserted in order so a gap cannot be
/// quietly dropped or quietly acquired, and kept apart from the exclusions so
/// a reader can tell "the runtimes cannot be compared here" from "nobody
/// looked here yet".
const DECLARED_GAPS: [&str; 8] = [
    "Double-await of a handle",
    "Offloaded (blob) payload refs",
    "Child workflow maps",
    "Workflow cancellation events",
    "Activity retries",
    "Terminal-with-leftover-command divergence detection",
    "The select tie-break between two branches ready at the same event id",
    "Providers other than the two in-memory ones",
];

#[test]
fn corpus_declares_its_exclusions() {
    let corpus = load_corpus();
    let declared: Vec<(&str, &str)> = corpus["exclusions"]
        .as_array()
        .expect("corpus exclusions")
        .iter()
        .map(|exclusion| {
            (
                exclusion["what"].as_str().expect("exclusion what"),
                exclusion["kind"].as_str().expect("exclusion kind"),
            )
        })
        .collect();
    assert_eq!(declared, DECLARED_EXCLUSIONS.to_vec());
    for exclusion in corpus["exclusions"].as_array().expect("exclusions") {
        let why = exclusion["why"].as_str().expect("exclusion why");
        assert!(
            why.len() > 80,
            "exclusion `{}` needs a reason, not a label",
            exclusion["what"]
        );
    }
}

#[test]
fn corpus_declares_its_gaps_separately_from_its_exclusions() {
    let corpus = load_corpus();
    let declared: Vec<&str> = corpus["declaredGaps"]
        .as_array()
        .expect("corpus declaredGaps")
        .iter()
        .map(|gap| gap["what"].as_str().expect("gap what"))
        .collect();
    assert_eq!(declared, DECLARED_GAPS.to_vec());
    for gap in corpus["declaredGaps"].as_array().expect("declaredGaps") {
        let why = gap["why"].as_str().expect("gap why");
        assert!(
            why.len() > 80,
            "gap `{}` needs a reason, not a label",
            gap["what"]
        );
    }
}

/// Every case this corpus holds, in order, asserted by name in both runners —
/// the same treatment [`DECLARED_EXCLUSIONS`] gets, and for the same reason.
///
/// A floor (`cases.len() >= 12` against 13 cases) is not a tripwire. Measured
/// before this list existed: deleting cases one at a time, **8 of the 13 left
/// the suite green in both languages**, including all four `select` cases —
/// the ones whose convergence this corpus was extended to pin. A case that can
/// be deleted without failing anything is a case that is not protecting
/// anything.
const DECLARED_CASES: [&str; 17] = [
    "an activity call commits one scheduled activity, and its completion closes the run",
    "a later command is appended past an unconsumed completion (hot execution)",
    "the same commits come out of a cold replay in one-event chunks",
    "a signal that arrives first wins the select, and the losing timer wait is cancelled",
    "a timer that fires first wins the select, and the losing signal wait is cancelled",
    "a select resolved with two ready branches takes the earlier ready event, not the earlier branch",
    "the same select with the activity landing first takes the activity branch",
    "a query projection is committed with the signal wait and again with its consumption",
    "a child workflow start is committed with the parent's task and its completion closes the parent",
    "an activity map commits one scheduled map bounded by maxInFlight",
    "a version marker, a side effect and a deprecated patch land in one commit with the result",
    "continue-as-new commits the next run's input and nothing else",
    "a timer scheduled after the clock has moved records its deadline relative to now",
    "one change id consulted on both sides of a task boundary records two markers",
    "a signal that won a select whose timer registered later replays cold by command id",
    "a workflow that fails on purpose commits WorkflowFailed with its own failure",
    "now() records the provider clock once per call and replays it as recorded",
];

#[test]
fn corpus_declares_its_cases() {
    let corpus = load_corpus();
    let declared: Vec<&str> = corpus["cases"]
        .as_array()
        .expect("corpus cases")
        .iter()
        .map(|case| case["name"].as_str().expect("case name"))
        .collect();
    assert_eq!(
        declared,
        DECLARED_CASES.to_vec(),
        "a case cannot be added, removed, or renamed without saying so here and in the \
         TypeScript runner's DECLARED_CASES"
    );
}

/// A corpus whose expectations are all empty would pass in both runtimes and
/// prove nothing. This is the floor: every case commits something, and the
/// corpus as a whole reaches every commit field the runners can encode.
#[test]
fn corpus_cases_assert_something() {
    let corpus = load_corpus();
    let cases = corpus["cases"].as_array().expect("corpus cases");
    assert_eq!(
        cases.len(),
        DECLARED_CASES.len(),
        "the corpus case count must match the declared list"
    );

    let mut seen_fields: Vec<&str> = Vec::new();
    let mut seen_events: Vec<String> = Vec::new();
    for case in cases {
        let mut asserted = 0;
        for step in case["steps"].as_array().expect("case steps") {
            let expect = match (step.get("expect"), step.get("divergence")) {
                (Some(expect), None) => expect,
                (None, Some(divergence)) => &divergence["rust"],
                (None, None) => continue,
                (Some(_), Some(_)) => {
                    panic!("`{}` carries both expect and divergence", case["name"])
                }
            };
            asserted += 1;
            for (field, value) in expect.as_object().expect("expect object") {
                let non_empty = match value {
                    Value::Array(items) => !items.is_empty(),
                    Value::Null => false,
                    _ => true,
                };
                if non_empty && field != "expectedTailEventId" && !seen_fields.contains(&&**field) {
                    seen_fields.push(field);
                }
            }
            for event in expect["appendEvents"].as_array().expect("appendEvents") {
                let event_type = event["type"].as_str().expect("event type").to_owned();
                if !seen_events.contains(&event_type) {
                    seen_events.push(event_type);
                }
            }
        }
        assert!(asserted > 0, "case `{}` asserts no commit", case["name"]);
    }

    seen_fields.sort_unstable();
    assert_eq!(
        seen_fields,
        vec![
            "appendEvents",
            "consumeSignals",
            "deleteWaits",
            "queryProjection",
            "scheduleActivities",
            "scheduleActivityMaps",
            "startChildWorkflows",
            "upsertWaits",
        ],
        "every commit field the runners encode must be exercised by some case"
    );

    seen_events.sort();
    assert_eq!(
        seen_events,
        vec![
            "ActivityMapScheduled",
            "ActivityScheduled",
            "ChildWorkflowStartRequested",
            "DeprecatedPatchMarker",
            "SelectWinner",
            "SideEffectMarker",
            "SignalConsumed",
            "TimerStarted",
            "VersionMarker",
            "WorkflowCompleted",
            "WorkflowContinuedAsNew",
            "WorkflowFailed",
        ],
    );
}

/// A `divergence` block records a place where the two runtimes genuinely
/// behave differently, by holding *both* observed commits plus a `differences`
/// list naming the field paths each cause explains.
///
/// The union of the declared paths must be exactly the set of paths that
/// actually differ. That is the check with teeth: a prose reason can explain
/// one cause convincingly while a second cause rides along in the data
/// unlabelled, and only a diff of the recorded commits notices.
#[test]
fn corpus_divergence_reasons_account_for_every_differing_field() {
    let corpus = load_corpus();
    let mut count = 0;
    for case in corpus["cases"].as_array().expect("cases") {
        for (index, step) in case["steps"].as_array().expect("steps").iter().enumerate() {
            let Some(divergence) = step.get("divergence") else {
                continue;
            };
            count += 1;
            let where_ = format!("{} step {index}", case["name"]);
            let mut declared: Vec<String> = Vec::new();
            let differences = divergence["differences"]
                .as_array()
                .expect("divergence differences");
            assert!(!differences.is_empty(), "{where_}: no differences declared");
            for difference in differences {
                let what = difference["what"].as_str().expect("difference what");
                assert!(
                    what.len() > 120,
                    "{where_}: difference `{what}` needs a reason, not a label"
                );
                let paths = difference["paths"].as_array().expect("difference paths");
                assert!(
                    !paths.is_empty(),
                    "{where_}: difference `{what}` names no field paths"
                );
                for path in paths {
                    let path = path.as_str().expect("path").to_owned();
                    if !declared.contains(&path) {
                        declared.push(path);
                    }
                }
            }
            let mut actual = Vec::new();
            diff_paths(
                &divergence["rust"],
                &divergence["typescript"],
                String::new(),
                &mut actual,
            );
            assert!(
                !actual.is_empty(),
                "{where_}: records a divergence whose two sides are equal; make it a shared expect"
            );
            declared.sort();
            actual.sort();
            assert_eq!(
                actual, declared,
                "{where_}: the declared difference paths must be exactly the paths that differ"
            );
        }
    }
    // The count is declared in the corpus and asserted equal, not asserted
    // non-zero. `count > 0` encoded the assumption that some divergence would
    // always remain, which stops being true the moment the last one is
    // converged away — and it would have had to be edited under exactly the
    // pressure that makes a careless edit likely. Equality against a checked-in
    // number keeps both directions loud: deleting a block fails here, adding
    // one without declaring it fails here, and reaching zero is possible only
    // by editing the corpus on purpose.
    let declared = &corpus["declaredDivergences"];
    let why = declared["why"].as_str().expect("declaredDivergences why");
    assert!(
        why.len() > 80,
        "declaredDivergences needs a reason, not a label"
    );
    assert_eq!(
        u64::try_from(count).expect("divergence count fits u64"),
        declared["count"]
            .as_u64()
            .expect("declaredDivergences count"),
        "the corpus records a different number of divergence blocks than it declares"
    );
}

/// Leaf-level structural diff of two commits, mirrored by `diffPaths` in the
/// TypeScript runner. A length or key-set mismatch reports the container and
/// stops descending, so one structural difference is one path rather than a
/// cascade.
///
/// The two implementations agree on every structural shape and disagree on
/// exactly one class: numeric *representation*. This side compares
/// `serde_json::Number`, which distinguishes `1.0` from `1`, `-0.0` from `0`,
/// and `9007199254740993` from `9007199254740992`; the TypeScript side
/// compares `JSON.stringify` output after `JSON.parse` has already collapsed
/// each of those pairs. This side's path set is therefore a *superset* in
/// every disagreement, which is what makes the split safe: declare the extra
/// path and TypeScript rejects it as spurious, omit it and this runner rejects
/// the omission, so one runner always goes red and `check:fixtures` runs both.
/// Unreachable today — every number in every commit is an integer inside the
/// double-safe range.
fn diff_paths(left: &Value, right: &Value, path: String, out: &mut Vec<String>) {
    match (left, right) {
        (Value::Object(left_map), Value::Object(right_map)) => {
            let left_keys: Vec<&String> = left_map.keys().collect();
            let right_keys: Vec<&String> = right_map.keys().collect();
            if left_keys != right_keys {
                out.push(path);
                return;
            }
            for key in left_keys {
                let child = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                diff_paths(&left_map[key], &right_map[key], child, out);
            }
        }
        (Value::Array(left_items), Value::Array(right_items)) => {
            if left_items.len() != right_items.len() {
                out.push(path);
                return;
            }
            for (index, (left_item, right_item)) in
                left_items.iter().zip(right_items.iter()).enumerate()
            {
                diff_paths(left_item, right_item, format!("{path}[{index}]"), out);
            }
        }
        _ => {
            if left != right {
                out.push(path);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The runner.
// ---------------------------------------------------------------------------

fn build_worker(
    backend: RecordingBackend,
    history_chunk_events: Option<usize>,
) -> Worker<RecordingBackend> {
    let mut builder = Worker::builder(backend)
        .worker_id("corpus-worker")
        .workflow_task_queue(CORPUS_WORKFLOW_QUEUE)
        .activity_task_queue(CORPUS_ACTIVITY_QUEUE)
        .register_workflow(corpus_activity_then_return)
        .register_workflow(corpus_spawn_sleep_then_activity)
        .register_workflow(corpus_select_signal_or_timer)
        .register_workflow(corpus_select_activity_or_timer)
        .register_workflow(corpus_signal_then_publish)
        .register_workflow(corpus_sleep_after_signal)
        .register_workflow(corpus_child_await)
        .register_workflow(corpus_child_increment)
        .register_workflow(corpus_activity_map)
        .register_workflow(corpus_markers)
        .register_workflow(corpus_continue_as_new)
        .register_workflow(corpus_repeated_change_id)
        .register_workflow(corpus_signal_first_select_then_timer)
        .register_workflow(corpus_fails)
        .register_workflow(corpus_now_twice)
        .register_activity(corpus_double);
    if let Some(chunk_events) = history_chunk_events {
        builder = builder.history_chunk_events(chunk_events);
    }
    builder.build()
}

async fn start_case(backend: &RecordingBackend, case: &Value) -> durust::RunId {
    let client = Client::new(backend.clone());
    let workflow_id = case["workflowId"].as_str().expect("case workflowId");
    let input = &case["input"];
    match case["program"].as_str().expect("case program") {
        "corpus.activity-then-return" => {
            client
                .start_workflow::<corpus_activity_then_return>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.spawn-sleep-then-activity" => {
            client
                .start_workflow::<corpus_spawn_sleep_then_activity>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.select-signal-or-timer" => {
            client
                .start_workflow::<corpus_select_signal_or_timer>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.select-activity-or-timer" => {
            client
                .start_workflow::<corpus_select_activity_or_timer>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.signal-then-publish" => {
            client
                .start_workflow::<corpus_signal_then_publish>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.sleep-after-signal" => {
            client
                .start_workflow::<corpus_sleep_after_signal>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.child-await" => {
            client
                .start_workflow::<corpus_child_await>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.activity-map" => {
            client
                .start_workflow::<corpus_activity_map>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.markers" => {
            client
                .start_workflow::<corpus_markers>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.repeated-change-id" => {
            client
                .start_workflow::<corpus_repeated_change_id>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.signal-first-select-then-timer" => {
            client
                .start_workflow::<corpus_signal_first_select_then_timer>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.now-twice" => {
            client
                .start_workflow::<corpus_now_twice>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.fails" => {
            client
                .start_workflow::<corpus_fails>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Value1 input"),
                )
                .await
        }
        "corpus.continue-as-new" => {
            client
                .start_workflow::<corpus_continue_as_new>(
                    workflow_id,
                    CORPUS_WORKFLOW_QUEUE,
                    serde_json::from_value(input.clone()).expect("Continue1 input"),
                )
                .await
        }
        other => panic!("the corpus names an unknown program `{other}`"),
    }
    .expect("corpus case starts")
}

async fn history_types(backend: &RecordingBackend, run_id: &durust::RunId) -> Vec<String> {
    let chunk = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(u64::MAX),
            max_events: 1_000,
            max_bytes: usize::MAX,
        })
        .await
        .expect("corpus history");
    chunk
        .events
        .iter()
        .map(|event| format!("{:?}", event.data.event_type()))
        .collect()
}

struct ObservedTask {
    history_before: Vec<String>,
    commit: Value,
}

async fn run_case(case: &Value, observed: &mut Vec<ObservedTask>, regenerate: bool) {
    let name = case["name"].as_str().expect("case name");
    let backend = RecordingBackend::new();
    let run_id = start_case(&backend, case).await;
    let mut worker = build_worker(backend.clone(), None);

    for (index, step) in case["steps"]
        .as_array()
        .expect("case steps")
        .iter()
        .enumerate()
    {
        let where_ = format!("{name} step {index}");
        match step["action"].as_str().expect("step action") {
            "workflowTask" => {
                let before = history_types(&backend, &run_id).await;
                backend.take_commits();
                assert!(
                    worker.run_workflow_once().await.expect("workflow task"),
                    "{where_}: expected a claimable workflow task"
                );
                let commits = backend.take_commits();
                assert_eq!(commits.len(), 1, "{where_}: expected exactly one commit");
                let actual = commit_json(&commits[0]);
                if !regenerate {
                    let expected_before: Vec<String> = step["historyBefore"]
                        .as_array()
                        .expect("historyBefore")
                        .iter()
                        .map(|value| value.as_str().expect("history type").to_owned())
                        .collect();
                    assert_eq!(before, expected_before, "{where_}: input history");
                    // A step either carries one shared `expect`, or — where the
                    // two runtimes genuinely behave differently — a `divergence`
                    // block holding both observed commits. Each runner asserts
                    // its own side of a divergence, so the difference is
                    // recorded as literal data instead of normalised away.
                    let expected = step
                        .get("divergence")
                        .map_or(&step["expect"], |divergence| &divergence["rust"]);
                    assert_eq!(
                        &actual,
                        expected,
                        "{where_}: commit\n  actual: {}\nexpected: {}",
                        serde_json::to_string_pretty(&actual).unwrap(),
                        serde_json::to_string_pretty(expected).unwrap(),
                    );
                }
                observed.push(ObservedTask {
                    history_before: before,
                    commit: actual,
                });
            }
            "otherWorkflowTask" => {
                assert!(
                    worker.run_workflow_once().await.expect("workflow task"),
                    "{where_}: expected a claimable workflow task"
                );
                backend.take_commits();
            }
            "runActivity" => {
                assert!(
                    worker.run_activity_once().await.expect("activity task"),
                    "{where_}: expected a claimable activity task"
                );
            }
            "advanceTime" => {
                backend.advance_time(Duration::from_millis(
                    step["ms"].as_u64().expect("advanceTime ms"),
                ));
            }
            "fireTimers" => {
                let outcome = backend
                    .fire_due_timers(durust::FireDueTimersRequest {
                        namespace: durust::Namespace::default(),
                        now: current_time(&backend).await,
                        limit: 16,
                    })
                    .await
                    .expect("fire timers");
                assert_eq!(
                    u64::try_from(outcome.fired).unwrap(),
                    step["count"].as_u64().expect("count"),
                    "{where_}: fired timers"
                );
            }
            "signal" => {
                let client = Client::new(backend.clone());
                let outcome = client
                    .signal_workflow(
                        case["workflowId"].as_str().expect("workflowId"),
                        step["name"].as_str().expect("signal name"),
                        step["signalId"].as_str().expect("signal id"),
                        Approval {
                            who: step["who"].as_str().expect("signal who").to_owned(),
                        },
                    )
                    .await
                    .expect("signal");
                assert_eq!(outcome, durust::SignalWorkflowOutcome::Accepted, "{where_}");
            }
            "restartWorker" => {
                // Drops the cached execution so the next task cold-replays.
                drop(worker);
                worker = build_worker(
                    backend.clone(),
                    step.get("historyChunkEvents")
                        .and_then(Value::as_u64)
                        .map(|events| events as usize),
                );
            }
            other => panic!("{where_}: unknown corpus action `{other}`"),
        }
    }
}

async fn current_time(backend: &RecordingBackend) -> TimestampMs {
    backend.current_time().await.expect("current time")
}

/// The Rust twin of the TypeScript runner's `is not a print run`, and the
/// stronger of the two: `DURUST_CORPUS_PRINT=1` only stops that runner
/// asserting, while `DURUST_CORPUS_REGENERATE=1` makes this one *rewrite the
/// file the other runner is measured against* and then pass. One environment
/// variable turned the Rust half from a cross-implementation assertion into a
/// snapshot writer that cannot fail — and the corpus's own header argues that
/// the asymmetry between a runner that can regenerate and one that cannot is
/// what gives the file its teeth. That argument only holds while a regenerate
/// run is never mistaken for a passing one.
///
/// Demonstrated before this existed: with a case's `expect` mutated to a wrong
/// commit, `DURUST_CORPUS_REGENERATE=1 cargo test --test behavioral_corpus`
/// reported `ok` and silently corrected the file.
#[test]
fn is_not_a_regenerate_run() {
    assert!(
        !regenerate_requested(),
        "DURUST_CORPUS_REGENERATE=1 rewrites the corpus instead of asserting it; this run proves \
         nothing. Re-run without it to check the regenerated file in."
    );
}

fn regenerate_requested() -> bool {
    std::env::var("DURUST_CORPUS_REGENERATE").as_deref() == Ok("1")
}

#[test]
fn every_corpus_case_reproduces_its_recorded_commits() {
    // `DURUST_CORPUS_REGENERATE=1 cargo test --test behavioral_corpus` rewrites
    // the `historyBefore` and `expect` blocks in place. Deliberately gated and
    // never run in CI: the corpus is only worth anything as checked-in literal
    // data that the *other* runtime has to satisfy without having produced it,
    // and the TypeScript runner has no regeneration path at all.
    let regenerate = regenerate_requested();
    let corpus = load_corpus();
    let cases = corpus["cases"].as_array().expect("corpus cases").clone();
    let mut regenerated = Vec::new();
    block_on(async {
        for case in &cases {
            let mut observed = Vec::new();
            run_case(case, &mut observed, regenerate).await;
            regenerated.push(observed);
        }
    });

    if regenerate {
        let mut corpus = corpus;
        for (case_index, case) in corpus["cases"]
            .as_array_mut()
            .expect("cases")
            .iter_mut()
            .enumerate()
        {
            let mut observed = regenerated[case_index].iter();
            for step in case["steps"].as_array_mut().expect("steps") {
                if step["action"] == "workflowTask" {
                    let task = observed.next().expect("observed commit");
                    step["historyBefore"] = json!(task.history_before);
                    if step.get("divergence").is_some() {
                        step["divergence"]["rust"] = task.commit.clone();
                    } else {
                        step["expect"] = task.commit.clone();
                    }
                }
            }
        }
        std::fs::write(
            CORPUS_PATH,
            format!("{}\n", serde_json::to_string_pretty(&corpus).unwrap()),
        )
        .expect("write corpus");
        // The test that performed the write is the test that reports it.
        // `is_not_a_regenerate_run` already fails the run, but it is a
        // *different* test, so this one was reporting `ok` immediately after
        // overwriting the file the other runner is measured against. Nothing
        // here prevents the write — the authoring workflow needs it, and an
        // unwanted rewrite is recoverable from git, which is the real safety
        // net — but no test that regenerated the corpus should be able to say
        // it passed.
        panic!(
            "DURUST_CORPUS_REGENERATE=1 rewrote {CORPUS_PATH}; review the diff and re-run \
             without the variable to check it in"
        );
    }
}
