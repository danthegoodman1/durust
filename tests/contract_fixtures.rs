use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckoutInput {
    order_id: String,
}

#[test]
fn rust_event_type_names_match_neutral_history_fixture() {
    let fixture = load_fixture();
    let events = fixture["historyEvents"]
        .as_array()
        .expect("fixture historyEvents should be an array");

    let expected = all_history_event_types();
    assert_eq!(events.len(), expected.len());

    for (index, (event, expected_type)) in events.iter().zip(expected).enumerate() {
        let event_id = event["eventId"]
            .as_u64()
            .expect("history fixture eventId should be an integer");
        let event_type = event["eventType"]
            .as_str()
            .expect("history fixture eventType should be a string");
        let data_kind = event["data"]["kind"]
            .as_str()
            .expect("history fixture data.kind should be a string");

        assert_eq!(event_id, u64::try_from(index + 1).unwrap());
        assert_eq!(
            serde_json::to_value(expected_type).unwrap(),
            json!(event_type)
        );
        assert_eq!(data_kind, event_type);
    }

    let started_input = payload_ref_from_fixture_json(&events[0]["data"]["input"]);
    let started = durust::provider::HistoryEventData::WorkflowStarted {
        workflow_type: durust::WorkflowType::new("orders.checkout", 1),
        input: started_input,
    };
    assert_eq!(
        started.event_type(),
        durust::provider::HistoryEventType::WorkflowStarted,
    );
}

#[test]
fn rust_fingerprint_helpers_match_neutral_fixture() {
    let fixture = load_fixture();
    let fingerprints = &fixture["fingerprints"];

    assert_eq!(
        fingerprint_fixture_json(durust::provider::activity_fingerprint(
            durust::ActivityName::new("payments.price-quote"),
            "sha256:quote-input-digest".to_owned(),
            "sha256:activity-options".to_owned(),
        )),
        fingerprints["activity"]
    );

    assert_eq!(
        fingerprint_fixture_json(durust::provider::activity_map_fingerprint(
            durust::ActivityName::new("payments.price-quote"),
            "sha256:manifest".to_owned(),
            "partials".to_owned(),
            8,
            "sha256:activity-map-options".to_owned(),
        )),
        fingerprints["activityMap"]
    );

    assert_eq!(
        fingerprint_fixture_json(durust::provider::child_workflow_fingerprint(
            durust::WorkflowType::new("orders.ship", 1),
            durust::WorkflowId::new("ship/o-1"),
            "sha256:ship-input".to_owned(),
            durust::TaskQueue::new("workflows"),
            durust::provider::ParentClosePolicy::Cancel,
        )),
        fingerprints["childWorkflow"]
    );

    assert_eq!(
        fingerprint_fixture_json(durust::provider::child_workflow_map_fingerprint(
            durust::WorkflowType::new("orders.ship", 1),
            "sha256:ship-manifest".to_owned(),
            "ship-results".to_owned(),
            "ship-map".to_owned(),
            4,
            durust::TaskQueue::new("workflows"),
            durust::provider::ParentClosePolicy::Abandon,
            durust::provider::ChildWorkflowMapFailureMode::CollectAll,
        )),
        fingerprints["childWorkflowMap"]
    );

    assert_eq!(
        fingerprint_fixture_json(durust::provider::timer_fingerprint(
            "sleep_until",
            durust::TimestampMs(1_781_821_484_000),
        )),
        fingerprints["timer"]
    );

    assert_eq!(
        fingerprint_fixture_json(durust::provider::signal_fingerprint(
            durust::SignalName::new("approved")
        )),
        fingerprints["signal"]
    );

    assert_eq!(
        fingerprint_fixture_json(durust::provider::version_marker_fingerprint(
            "checkout-v2",
            2
        )),
        fingerprints["versionMarker"]
    );
}

#[test]
fn rust_payload_refs_match_neutral_fixture_shapes() {
    let fixture = load_fixture();
    let inline = payload_ref_from_fixture_json(&fixture["payloadRefs"]["inlineJson"]);
    let blob = payload_ref_from_fixture_json(&fixture["payloadRefs"]["blobJson"]);
    let inline_messagepack =
        payload_ref_from_fixture_json(&fixture["payloadRefs"]["inlineMessagePack"]);
    let blob_messagepack =
        payload_ref_from_fixture_json(&fixture["payloadRefs"]["blobMessagePack"]);

    assert_eq!(
        durust::decode_payload::<CheckoutInput>(&inline).unwrap(),
        CheckoutInput {
            order_id: "o-1".to_owned()
        }
    );
    assert_eq!(
        inline
            .to_blob_ref("s3://durust-fixtures/payloads/checkout-input.bin".to_owned())
            .unwrap(),
        blob
    );

    assert_eq!(
        durust::decode_payload::<CheckoutInput>(&inline_messagepack).unwrap(),
        CheckoutInput {
            order_id: "o-1".to_owned()
        }
    );
    assert_eq!(
        inline_messagepack
            .to_blob_ref("s3://durust-fixtures/payloads/checkout-input.msgpack".to_owned())
            .unwrap(),
        blob_messagepack
    );
}

#[test]
fn rust_manifest_payload_fixtures_match_neutral_shape() {
    let fixture = load_fixture();
    let manifests = &fixture["manifestPayloads"];

    let input_manifest = decode_json_payload_value(&manifests["activityMapInputManifest"]);
    assert_eq!(input_manifest["itemCount"], json!(1));
    assert_eq!(input_manifest["pageLengths"], json!([1]));
    assert_eq!(
        input_manifest["pages"][0],
        manifests["activityMapInputPage"]
    );
    let input_page = decode_json_payload_value(&input_manifest["pages"][0]);
    assert_eq!(
        decode_checkout_from_payload_value(&input_page["items"][0]),
        CheckoutInput {
            order_id: "o-1".to_owned()
        }
    );

    let result_manifest = decode_json_payload_value(&manifests["activityMapResultManifest"]);
    assert_eq!(result_manifest["name"], json!("partials"));
    assert_eq!(result_manifest["itemCount"], json!(1));
    assert_eq!(result_manifest["pageLengths"], json!([1]));
    assert_eq!(
        result_manifest["pages"][0],
        manifests["activityMapResultPage"]
    );
    let result_page = decode_json_payload_value(&result_manifest["pages"][0]);
    assert_eq!(
        decode_checkout_from_payload_value(&result_page["results"][0]),
        CheckoutInput {
            order_id: "o-1".to_owned()
        }
    );

    let child_manifest = decode_json_payload_value(&manifests["childWorkflowMapResultManifest"]);
    assert_eq!(child_manifest["name"], json!("ship-results"));
    assert_eq!(child_manifest["itemCount"], json!(3));
    assert_eq!(child_manifest["pageLengths"], json!([3]));
    assert_eq!(
        child_manifest["pages"][0],
        manifests["childWorkflowMapResultPage"]
    );
    let child_page = decode_json_payload_value(&child_manifest["pages"][0]);
    let outcomes = child_page["outcomes"]
        .as_array()
        .expect("child workflow map outcomes should be an array");
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| outcome["kind"].as_str().expect("outcome kind"))
            .collect::<Vec<_>>(),
        vec!["Succeeded", "Failed", "Cancelled"]
    );
    assert_eq!(
        decode_checkout_from_payload_value(&outcomes[0]["result"]),
        CheckoutInput {
            order_id: "o-1".to_owned()
        }
    );
    assert_eq!(
        durable_failure_from_fixture_json(&outcomes[1]["failure"]),
        durust::DurableFailure::new("ApplicationError", "checkout failed")
    );
    assert_eq!(outcomes[2]["reason"], json!("parent cancelled"));
}

#[test]
fn rust_durable_failures_match_neutral_fixture_shapes() {
    let fixture = load_fixture();
    let retryable = durable_failure_from_fixture_json(&fixture["durableFailures"]["retryable"]);
    assert_eq!(
        retryable,
        durust::DurableFailure::new("ApplicationError", "checkout failed")
    );

    let detailed =
        durable_failure_from_fixture_json(&fixture["durableFailures"]["nonRetryableWithDetails"]);
    assert_eq!(detailed.error_type, "ValidationError");
    assert_eq!(detailed.message, "invalid checkout request");
    assert!(detailed.non_retryable);
    assert_eq!(
        durust::decode_payload::<CheckoutInput>(
            detailed
                .details
                .as_ref()
                .expect("detailed fixture failure should include details")
        )
        .unwrap(),
        CheckoutInput {
            order_id: "o-1".to_owned()
        }
    );
}

#[test]
fn rust_provider_io_fixture_matches_backend_contract_vocabulary() {
    let fixture = load_provider_fixture();

    let start_request = start_workflow_request_from_fixture(&fixture["startWorkflow"]["request"]);
    assert_eq!(start_request.namespace, durust::Namespace::default());
    assert_eq!(
        start_request.workflow_id,
        durust::WorkflowId::new("wf/provider")
    );
    assert_eq!(
        start_request.workflow_type,
        durust::WorkflowType::new("orders.checkout", 1)
    );
    assert_eq!(
        start_request.task_queue,
        durust::TaskQueue::new("workflows")
    );
    assert_eq!(
        durust::decode_payload::<CheckoutInput>(&start_request.input).unwrap(),
        CheckoutInput {
            order_id: "o-1".to_owned()
        }
    );
    assert_start_workflow_outcome(
        &fixture["startWorkflow"]["started"],
        durust::provider::StartWorkflowOutcome::Started {
            run_id: durust::RunId::new("run-1"),
        },
    );
    assert_start_workflow_outcome(
        &fixture["startWorkflow"]["alreadyStarted"],
        durust::provider::StartWorkflowOutcome::AlreadyStarted {
            run_id: durust::RunId::new("run-1"),
        },
    );

    let claim_options =
        claim_workflow_task_options_from_fixture(&fixture["claimWorkflowTask"]["options"]);
    assert_eq!(claim_options.namespace, durust::Namespace::default());
    assert_eq!(
        claim_options.task_queue,
        durust::TaskQueue::new("workflows")
    );
    assert_eq!(claim_options.lease_duration, Duration::from_millis(30_000));
    assert_eq!(
        claim_options.registered_workflow_types,
        vec![durust::WorkflowType::new("orders.checkout", 1)]
    );
    let claimed = claimed_workflow_task_from_fixture(&fixture["claimWorkflowTask"]["claimed"]);
    assert_eq!(claimed.run_id, durust::RunId::new("run-1"));
    assert_eq!(claimed.claim.token, 7);
    assert_eq!(claimed.replay_target_event_id, durust::EventId(1));
    assert_eq!(
        claimed.reason,
        durust::provider::WorkflowTaskReason::WorkflowStarted
    );
    assert_eq!(claimed.prefetched_history.len(), 1);

    let stream_request = stream_history_request_from_fixture(&fixture["streamHistory"]["request"]);
    assert_eq!(stream_request.run_id, durust::RunId::new("run-1"));
    assert_eq!(stream_request.after_event_id, durust::EventId(0));
    assert_eq!(stream_request.up_to_event_id, durust::EventId(28));
    assert_eq!(stream_request.max_events, 128);
    assert_eq!(stream_request.max_bytes, 1_048_576);
    assert_eq!(fixture["streamHistory"]["chunk"]["lastEventId"], json!(1));
    assert_eq!(fixture["streamHistory"]["chunk"]["hasMore"], json!(true));

    let workflow_claim = workflow_task_claim_from_fixture(&fixture["commitWorkflowTask"]["claim"]);
    assert_eq!(workflow_claim.worker_id, durust::WorkerId::new("worker-a"));
    let commit = workflow_task_commit_from_fixture(&fixture["commitWorkflowTask"]["commit"]);
    assert!(matches!(
        commit.append_events.first().map(|event| &event.data),
        Some(durust::provider::HistoryEventData::WorkflowTaskStarted)
    ));
    assert_eq!(
        commit.upsert_waits[0].kind,
        durust::provider::WaitKind::Timer
    );
    assert_eq!(
        commit.upsert_waits[0].ready_at,
        Some(durust::TimestampMs(1_781_821_484_000))
    );
    assert_eq!(
        commit.delete_waits,
        vec![durust::WaitId::new("run-1:signal:6")]
    );
    assert_eq!(
        commit.consume_signals,
        vec![durust::SignalId::new("signal-1")]
    );
    assert_eq!(
        durust::decode_payload::<CheckoutInput>(
            commit
                .query_projection
                .as_ref()
                .expect("provider fixture commit should include query projection")
        )
        .unwrap(),
        CheckoutInput {
            order_id: "o-1".to_owned()
        }
    );
    assert_eq!(
        durust::EventId(u64_field(&fixture["commitWorkflowTask"], "newTailEventId")),
        durust::EventId(2)
    );

    let signal_request =
        signal_workflow_request_from_fixture(&fixture["signalWorkflow"]["request"]);
    assert_eq!(signal_request.signal_id, durust::SignalId::new("signal-1"));
    assert_eq!(
        signal_request.signal_name,
        durust::SignalName::new("approved")
    );
    assert_signal_workflow_outcome(
        &fixture["signalWorkflow"]["accepted"],
        durust::provider::SignalWorkflowOutcome::Accepted,
    );
    assert_signal_workflow_outcome(
        &fixture["signalWorkflow"]["duplicate"],
        durust::provider::SignalWorkflowOutcome::Duplicate,
    );
    let inbox = signal_inbox_record_from_fixture(&fixture["signalWorkflow"]["inboxRecord"]);
    assert_eq!(inbox.signal_name, durust::SignalName::new("approved"));

    let complete_request =
        complete_activity_request_from_fixture(&fixture["activityCompletion"]["completeRequest"]);
    assert_eq!(
        complete_request.claim.activity_id,
        durust::ActivityId("run-1:1".to_owned())
    );
    assert_complete_activity_outcome(
        &fixture["activityCompletion"]["completed"],
        durust::provider::CompleteActivityOutcome::Completed {
            event_id: durust::EventId(11),
        },
    );
    assert_complete_activity_outcome(
        &fixture["activityCompletion"]["alreadyCompleted"],
        durust::provider::CompleteActivityOutcome::AlreadyCompleted,
    );
    let fail_request =
        fail_activity_request_from_fixture(&fixture["activityCompletion"]["failRequest"]);
    assert_eq!(fail_request.failure.error_type, "ActivityError");
    assert_fail_activity_outcome(
        &fixture["activityCompletion"]["failed"],
        durust::provider::FailActivityOutcome::Failed {
            event_id: durust::EventId(12),
        },
    );
    assert_fail_activity_outcome(
        &fixture["activityCompletion"]["retryScheduled"],
        durust::provider::FailActivityOutcome::RetryScheduled {
            next_attempt: 2,
            ready_at: durust::TimestampMs(0),
        },
    );

    let timer_request = fire_due_timers_request_from_fixture(&fixture["timers"]["request"]);
    assert_eq!(timer_request.now, durust::TimestampMs(1_781_821_484_000));
    assert_eq!(timer_request.limit, 64);
    assert_eq!(
        durust::provider::FireDueTimersOutcome { fired: 1 },
        durust::provider::FireDueTimersOutcome {
            fired: fixture["timers"]["outcome"]["fired"].as_u64().unwrap() as usize
        }
    );

    let query_request = query_projection_request_from_fixture(&fixture["queryWorkflow"]["request"]);
    assert_eq!(
        query_request.workflow_id,
        durust::WorkflowId::new("wf/provider")
    );
    let query_found = query_projection_outcome_from_fixture(&fixture["queryWorkflow"]["found"]);
    assert!(matches!(
        query_found,
        durust::provider::QueryProjectionOutcome::Found {
            run_id: durust::RunId(ref run),
            event_id: durust::EventId(2),
            ..
        } if run == "run-1"
    ));
    assert_eq!(
        query_projection_outcome_from_fixture(&fixture["queryWorkflow"]["notFound"]),
        durust::provider::QueryProjectionOutcome::NotFound
    );

    let dispatch_request =
        dispatch_child_workflow_starts_request_from_fixture(&fixture["childDispatch"]["request"]);
    assert_eq!(dispatch_request.limit, 32);
    assert_eq!(
        durust::provider::DispatchChildWorkflowStartsOutcome { dispatched: 1 },
        durust::provider::DispatchChildWorkflowStartsOutcome {
            dispatched: fixture["childDispatch"]["outcome"]["dispatched"]
                .as_u64()
                .unwrap() as usize
        }
    );

    let payload_roots = payload_roots_from_fixture(&fixture["payloadRoots"]);
    assert_eq!(payload_roots.roots.len(), 2);
    assert!(matches!(
        payload_roots.roots[0],
        durust::provider::PayloadRootRef::Payload(_)
    ));
    assert!(matches!(
        payload_roots.roots[1],
        durust::provider::PayloadRootRef::ActivityMapInputManifest(_)
    ));
    let gc_request =
        payload_gc_request_from_fixture(&fixture["payloadGarbageCollection"]["request"]);
    assert!(gc_request.dry_run);
    assert!(gc_request.writers_quiescent);
    assert_eq!(
        gc_request.min_age,
        durust::provider::DEFAULT_PAYLOAD_GC_MIN_AGE
    );
    assert_eq!(
        payload_gc_outcome_from_fixture(
            &fixture["payloadGarbageCollection"]["rustProviderOutcome"]
        ),
        durust::provider::PayloadGarbageCollectionOutcome {
            scanned_blobs: 3,
            retained_blobs: 2,
            deleted_blobs: 0,
            failed_blobs: 0,
        }
    );
}

#[test]
fn rust_benchmark_fixture_matches_shared_output_vocabulary() {
    let fixture = load_benchmark_fixture();
    let rust_result = &fixture["rustResult"];
    assert_eq!(rust_result["backend"], json!("sqlite"));
    assert_eq!(rust_result["mode"], json!("mixed"));
    assert_eq!(rust_result["correct"], json!(true));
    assert_eq!(rust_result["sqliteLayout"], json!("single-file"));

    let rust_options = &rust_result["options"];
    assert_eq!(rust_options["workflows"], json!(4));
    assert_eq!(rust_options["workers"], json!(1));
    assert_eq!(rust_options["batch"], json!(8));
    assert_eq!(rust_options["activityCompletionBatch"], json!(1));
    assert_eq!(rust_options["activationConcurrency"], json!(1));
    assert_eq!(rust_options["activationPrefetchLimit"], json!(1));

    assert_eq!(rust_result["completedWorkflows"], json!(4));
    assert_eq!(rust_result["mixedActions"], json!(32));
    assert_positive_f64(&rust_result["processingWorkflowsPerSecond"]);
    assert_positive_f64(&rust_result["processingMixedActionsPerSecond"]);

    let rust_counters = &rust_result["counters"];
    for field in [
        "workflowStarts",
        "signals",
        "childStarts",
        "childCompletions",
        "timerHandlers",
        "bootActivities",
        "childActivities",
        "finishActivities",
    ] {
        assert_eq!(rust_counters[field], json!(4), "counter {field} drifted");
    }
    assert_eq!(rust_counters["workflowTasks"], json!(32));
    assert_eq!(rust_counters["activityTasks"], json!(12));
    assert_eq!(rust_counters["childWorkflowStartsDispatched"], json!(4));

    let rust_worker_stats = &rust_result["workerStats"];
    assert_eq!(rust_worker_stats["workflowTasks"], json!(32));
    assert_eq!(rust_worker_stats["activityTasks"], json!(12));
    assert_eq!(rust_worker_stats["historyStreamChunks"], json!(0));
    assert_eq!(rust_worker_stats["historyStreamEvents"], json!(0));

    let rust_metrics = &rust_result["backendMetrics"];
    assert_eq!(
        rust_metrics["workflowTaskCommitLatency"]["samples"],
        json!(32)
    );
    assert_positive_f64(&rust_metrics["workflowTaskCommitLatency"]["p95Ms"]);
    assert_eq!(
        rust_metrics["operations"]["commit_workflow_tasks"]["calls"],
        json!(32)
    );
    assert_eq!(
        rust_metrics["operations"]["commit_workflow_tasks"]["errors"],
        json!(0)
    );

    let postgres_stats = &rust_result["postgresStats"];
    assert_eq!(postgres_stats["transactionsPerMixedAction"], json!(1));
    assert_eq!(
        postgres_stats["statementStats"]["callsPerMixedAction"],
        json!(3)
    );
    assert_eq!(
        postgres_stats["statementStats"]["topStatements"][0]["queryId"],
        json!("fixture-query")
    );
    assert_eq!(rust_result["resourceSamples"]["samples"], json!(1));

    let rust_comparison = &fixture["rustComparison"];
    assert_eq!(rust_comparison["dimensions"]["backend"], json!("sqlite"));
    assert_eq!(rust_comparison["ratio"], json!(1));
    assert_eq!(rust_comparison["minRatio"], json!(0.95));
    assert_eq!(rust_comparison["passed"], json!(true));

    let ts_result = &fixture["typescriptResult"];
    assert_eq!(ts_result["backend"], json!("memory"));
    assert_eq!(ts_result["completed_workflows"], json!(4));
    assert_eq!(ts_result["mixed_actions"], json!(32));
    assert_eq!(ts_result["counters"]["workflow_starts"], json!(4));
    assert_eq!(
        ts_result["worker_stats"]["workflowHistoryCacheHits"],
        json!(0)
    );
    assert_eq!(
        ts_result["worker_stats"]["workflowHistoryCacheMisses"],
        json!(32)
    );
    assert_eq!(
        ts_result["worker_stats"]["workflowHistoryCacheEvictions"],
        json!(0)
    );
    assert_eq!(
        ts_result["worker_stats"]["workflowExecutionCacheHits"],
        json!(24)
    );
    assert_eq!(
        ts_result["worker_stats"]["workflowExecutionCacheMisses"],
        json!(8)
    );
    assert_eq!(
        ts_result["worker_stats"]["workflowExecutionCacheEvictions"],
        json!(0)
    );
    assert_eq!(ts_result["worker_stats"]["historyStreamChunks"], json!(0));
    assert_eq!(ts_result["worker_stats"]["historyStreamEvents"], json!(0));
    assert_eq!(
        ts_result["backend_metrics"]["operations"]["commitWorkflowTask"]["calls"],
        json!(32)
    );
    let ts_thresholds = &fixture["typescriptBaseline"]["thresholds"];
    assert_eq!(
        ts_thresholds["require_exact_worker_stats"],
        json!([
            "workflowHistoryCacheHits",
            "workflowHistoryCacheMisses",
            "workflowHistoryCacheEvictions",
            "workflowExecutionCacheHits",
            "workflowExecutionCacheMisses",
            "workflowExecutionCacheEvictions",
            "historyStreamChunks",
            "historyStreamEvents"
        ])
    );
    assert_eq!(
        ts_thresholds["forbidden_operation_names"],
        json!(["failActivity"])
    );
    assert_eq!(
        fixture["typescriptComparison"],
        json!({
            "passed": true,
            "baseline": "contract-memory-mixed",
            "failures": []
        })
    );

    let ts_postgres_stats = &fixture["typescriptPostgresStats"];
    assert_eq!(ts_postgres_stats["walBytes"], json!(4096));
    assert_eq!(ts_postgres_stats["walRecords"], json!(8));
    assert_eq!(ts_postgres_stats["xactCommit"], json!(12));
    assert_eq!(ts_postgres_stats["xactRollback"], json!(1));
    assert_eq!(ts_postgres_stats["transactionsPerMixedAction"], json!(1.3));
    assert_eq!(ts_postgres_stats["blockCacheHitRatio"], json!(0.9));
    assert_eq!(ts_postgres_stats["activeConnectionsAfter"], json!(2));
    assert_eq!(ts_postgres_stats["statementStats"]["calls"], json!(8));
    assert_eq!(
        ts_postgres_stats["statementStats"]["callsPerMixedAction"],
        json!(0.8)
    );
    assert_eq!(
        ts_postgres_stats["statementStats"]["topStatements"][0]["queryId"],
        json!("select-1")
    );
}

fn load_fixture() -> Value {
    serde_json::from_str(include_str!(
        "../typescript/fixtures/contract/core-events.json"
    ))
    .expect("core contract fixture should be valid JSON")
}

fn load_provider_fixture() -> Value {
    serde_json::from_str(include_str!(
        "../typescript/fixtures/contract/provider-io.json"
    ))
    .expect("provider contract fixture should be valid JSON")
}

fn load_benchmark_fixture() -> Value {
    serde_json::from_str(include_str!(
        "../typescript/fixtures/contract/benchmark-output.json"
    ))
    .expect("benchmark contract fixture should be valid JSON")
}

fn all_history_event_types() -> [durust::provider::HistoryEventType; 28] {
    [
        durust::provider::HistoryEventType::WorkflowStarted,
        durust::provider::HistoryEventType::WorkflowCompleted,
        durust::provider::HistoryEventType::WorkflowFailed,
        durust::provider::HistoryEventType::WorkflowCancelled,
        durust::provider::HistoryEventType::WorkflowContinuedAsNew,
        durust::provider::HistoryEventType::WorkflowTaskStarted,
        durust::provider::HistoryEventType::ActivityScheduled,
        durust::provider::HistoryEventType::ActivityMapScheduled,
        durust::provider::HistoryEventType::ActivityMapCompleted,
        durust::provider::HistoryEventType::ActivityMapFailed,
        durust::provider::HistoryEventType::ActivityCompleted,
        durust::provider::HistoryEventType::ActivityFailed,
        durust::provider::HistoryEventType::ActivityTimedOut,
        durust::provider::HistoryEventType::ChildWorkflowStartRequested,
        durust::provider::HistoryEventType::ChildWorkflowStarted,
        durust::provider::HistoryEventType::ChildWorkflowCompleted,
        durust::provider::HistoryEventType::ChildWorkflowFailed,
        durust::provider::HistoryEventType::ChildWorkflowCancelled,
        durust::provider::HistoryEventType::ChildWorkflowMapScheduled,
        durust::provider::HistoryEventType::ChildWorkflowMapCompleted,
        durust::provider::HistoryEventType::ChildWorkflowMapFailed,
        durust::provider::HistoryEventType::TimerStarted,
        durust::provider::HistoryEventType::TimerFired,
        durust::provider::HistoryEventType::SignalConsumed,
        durust::provider::HistoryEventType::SelectWinner,
        durust::provider::HistoryEventType::VersionMarker,
        durust::provider::HistoryEventType::DeprecatedPatchMarker,
        durust::provider::HistoryEventType::SideEffectMarker,
    ]
}

fn start_workflow_request_from_fixture(value: &Value) -> durust::provider::StartWorkflowRequest {
    durust::provider::StartWorkflowRequest {
        namespace: durust::Namespace::new(string_field(value, "namespace")),
        workflow_id: durust::WorkflowId::new(string_field(value, "workflowId")),
        workflow_type: workflow_type_from_fixture(&value["workflowType"]),
        task_queue: durust::TaskQueue::new(string_field(value, "taskQueue")),
        input: payload_ref_from_fixture_json(&value["input"]),
    }
}

fn assert_start_workflow_outcome(value: &Value, expected: durust::provider::StartWorkflowOutcome) {
    match (string_field(value, "kind").as_str(), expected) {
        ("Started", durust::provider::StartWorkflowOutcome::Started { run_id })
        | ("AlreadyStarted", durust::provider::StartWorkflowOutcome::AlreadyStarted { run_id }) => {
            assert_eq!(durust::RunId::new(string_field(value, "runId")), run_id);
        }
        (actual, expected) => panic!("unexpected start outcome {actual} for {expected:?}"),
    }
}

fn claim_workflow_task_options_from_fixture(
    value: &Value,
) -> durust::provider::ClaimWorkflowTaskOptions {
    let registered_workflow_types = value["registeredWorkflowTypes"]
        .as_array()
        .expect("registeredWorkflowTypes should be an array")
        .iter()
        .map(workflow_type_from_fixture)
        .collect();
    durust::provider::ClaimWorkflowTaskOptions {
        namespace: durust::Namespace::new(string_field(value, "namespace")),
        task_queue: durust::TaskQueue::new(string_field(value, "taskQueue")),
        registered_workflow_types,
        lease_duration: Duration::from_millis(u64_field(value, "leaseDurationMs")),
        shard_filter: None,
    }
}

fn claimed_workflow_task_from_fixture(value: &Value) -> durust::provider::ClaimedWorkflowTask {
    let prefetched_history = value["prefetchedHistory"]
        .as_array()
        .expect("prefetchedHistory should be an array")
        .iter()
        .map(history_event_from_fixture)
        .collect();
    durust::provider::ClaimedWorkflowTask {
        run_id: durust::RunId::new(string_field(value, "runId")),
        workflow_id: durust::WorkflowId::new(string_field(value, "workflowId")),
        workflow_type: workflow_type_from_fixture(&value["workflowType"]),
        claim: workflow_task_claim_from_fixture(&value["claim"]),
        replay_target_event_id: durust::EventId(u64_field(value, "replayTargetEventId")),
        reason: workflow_task_reason_from_fixture(&value["reason"]),
        prefetched_history,
    }
}

fn stream_history_request_from_fixture(value: &Value) -> durust::provider::StreamHistoryRequest {
    durust::provider::StreamHistoryRequest {
        run_id: durust::RunId::new(string_field(value, "runId")),
        after_event_id: durust::EventId(u64_field(value, "afterEventId")),
        up_to_event_id: durust::EventId(u64_field(value, "upToEventId")),
        max_events: usize_field(value, "maxEvents"),
        max_bytes: usize_field(value, "maxBytes"),
    }
}

fn workflow_task_claim_from_fixture(value: &Value) -> durust::provider::WorkflowTaskClaim {
    durust::provider::WorkflowTaskClaim {
        run_id: durust::RunId::new(string_field(value, "runId")),
        worker_id: durust::WorkerId::new(string_field(value, "workerId")),
        token: u64_field(value, "token"),
    }
}

fn workflow_task_commit_from_fixture(value: &Value) -> durust::provider::WorkflowTaskCommit {
    let append_events = value["appendEvents"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(new_history_event_from_fixture)
        .collect();
    let upsert_waits = value["upsertWaits"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(wait_record_from_fixture)
        .collect();
    let delete_waits = value["deleteWaits"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|wait| durust::WaitId::new(wait.as_str().expect("deleteWaits entry")))
        .collect();
    let consume_signals = value["consumeSignals"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|signal| durust::SignalId::new(signal.as_str().expect("consumeSignals entry")))
        .collect();

    durust::provider::WorkflowTaskCommit {
        append_events,
        upsert_waits,
        delete_waits,
        consume_signals,
        query_projection: value
            .get("queryProjection")
            .map(payload_ref_from_fixture_json),
        ..Default::default()
    }
}

fn signal_workflow_request_from_fixture(value: &Value) -> durust::provider::SignalWorkflowRequest {
    durust::provider::SignalWorkflowRequest {
        namespace: durust::Namespace::new(string_field(value, "namespace")),
        workflow_id: durust::WorkflowId::new(string_field(value, "workflowId")),
        signal_id: durust::SignalId::new(string_field(value, "signalId")),
        signal_name: durust::SignalName::new(string_field(value, "signalName")),
        payload: payload_ref_from_fixture_json(&value["payload"]),
    }
}

fn assert_signal_workflow_outcome(
    value: &Value,
    expected: durust::provider::SignalWorkflowOutcome,
) {
    match (string_field(value, "kind").as_str(), expected) {
        ("Accepted", durust::provider::SignalWorkflowOutcome::Accepted)
        | ("Duplicate", durust::provider::SignalWorkflowOutcome::Duplicate) => {}
        (actual, expected) => panic!("unexpected signal outcome {actual} for {expected:?}"),
    }
}

fn signal_inbox_record_from_fixture(value: &Value) -> durust::provider::SignalInboxRecord {
    durust::provider::SignalInboxRecord {
        signal_id: durust::SignalId::new(string_field(value, "signalId")),
        signal_name: durust::SignalName::new(string_field(value, "signalName")),
        payload: payload_ref_from_fixture_json(&value["payload"]),
    }
}

fn complete_activity_request_from_fixture(
    value: &Value,
) -> durust::provider::CompleteActivityRequest {
    durust::provider::CompleteActivityRequest {
        claim: activity_task_claim_from_fixture(&value["claim"]),
        result: payload_ref_from_fixture_json(&value["result"]),
    }
}

fn assert_complete_activity_outcome(
    value: &Value,
    expected: durust::provider::CompleteActivityOutcome,
) {
    match (string_field(value, "kind").as_str(), expected) {
        ("Completed", durust::provider::CompleteActivityOutcome::Completed { event_id }) => {
            assert_eq!(durust::EventId(u64_field(value, "eventId")), event_id);
        }
        ("AlreadyCompleted", durust::provider::CompleteActivityOutcome::AlreadyCompleted) => {}
        (actual, expected) => {
            panic!("unexpected complete activity outcome {actual} for {expected:?}");
        }
    }
}

fn fail_activity_request_from_fixture(value: &Value) -> durust::provider::FailActivityRequest {
    durust::provider::FailActivityRequest {
        claim: activity_task_claim_from_fixture(&value["claim"]),
        failure: durable_failure_from_fixture_json(&value["failure"]),
    }
}

fn assert_fail_activity_outcome(value: &Value, expected: durust::provider::FailActivityOutcome) {
    match (string_field(value, "kind").as_str(), expected) {
        ("Failed", durust::provider::FailActivityOutcome::Failed { event_id }) => {
            assert_eq!(durust::EventId(u64_field(value, "eventId")), event_id);
        }
        (
            "RetryScheduled",
            durust::provider::FailActivityOutcome::RetryScheduled { next_attempt, .. },
        ) => {
            assert_eq!(u32_field(value, "attempt"), next_attempt);
        }
        ("AlreadyCompleted", durust::provider::FailActivityOutcome::AlreadyCompleted) => {}
        (actual, expected) => panic!("unexpected fail activity outcome {actual} for {expected:?}"),
    }
}

fn fire_due_timers_request_from_fixture(value: &Value) -> durust::provider::FireDueTimersRequest {
    durust::provider::FireDueTimersRequest {
        namespace: durust::Namespace::new(string_field(value, "namespace")),
        now: durust::TimestampMs(i64_field(value, "now")),
        limit: usize_field(value, "limit"),
    }
}

fn query_projection_request_from_fixture(
    value: &Value,
) -> durust::provider::QueryProjectionRequest {
    durust::provider::QueryProjectionRequest {
        namespace: durust::Namespace::new(string_field(value, "namespace")),
        workflow_id: durust::WorkflowId::new(string_field(value, "workflowId")),
    }
}

fn query_projection_outcome_from_fixture(
    value: &Value,
) -> durust::provider::QueryProjectionOutcome {
    match string_field(value, "kind").as_str() {
        "Found" => durust::provider::QueryProjectionOutcome::Found {
            run_id: durust::RunId::new(string_field(value, "runId")),
            event_id: durust::EventId(u64_field(value, "eventId")),
            payload: payload_ref_from_fixture_json(&value["projection"]),
        },
        "NotFound" => durust::provider::QueryProjectionOutcome::NotFound,
        other => panic!("unsupported Rust query projection fixture outcome {other}"),
    }
}

fn dispatch_child_workflow_starts_request_from_fixture(
    value: &Value,
) -> durust::provider::DispatchChildWorkflowStartsRequest {
    durust::provider::DispatchChildWorkflowStartsRequest {
        namespace: durust::Namespace::new(string_field(value, "namespace")),
        limit: usize_field(value, "limit"),
    }
}

fn payload_roots_from_fixture(value: &Value) -> durust::provider::PayloadRootsOutcome {
    let roots = value
        .as_array()
        .expect("payloadRoots should be an array")
        .iter()
        .map(|root| {
            let payload = payload_ref_from_fixture_json(&root["payload"]);
            match string_field(root, "kind").as_str() {
                "Payload" => durust::provider::PayloadRootRef::Payload(payload),
                "ActivityMapInputManifest" => {
                    durust::provider::PayloadRootRef::ActivityMapInputManifest(payload)
                }
                "ActivityMapResultManifest" => {
                    durust::provider::PayloadRootRef::ActivityMapResultManifest(payload)
                }
                "ChildWorkflowMapResultManifest" => {
                    durust::provider::PayloadRootRef::ChildWorkflowMapResultManifest(payload)
                }
                other => panic!("unsupported payload root kind {other}"),
            }
        })
        .collect();
    durust::provider::PayloadRootsOutcome { roots }
}

fn payload_gc_request_from_fixture(
    value: &Value,
) -> durust::provider::PayloadGarbageCollectionRequest {
    durust::provider::PayloadGarbageCollectionRequest {
        writers_quiescent: value["writersQuiescent"]
            .as_bool()
            .expect("GC writersQuiescent"),
        dry_run: value["dryRun"].as_bool().expect("GC dryRun"),
        min_age: Duration::from_millis(value["minAgeMs"].as_u64().expect("GC minAgeMs")),
    }
}

fn payload_gc_outcome_from_fixture(
    value: &Value,
) -> durust::provider::PayloadGarbageCollectionOutcome {
    durust::provider::PayloadGarbageCollectionOutcome {
        scanned_blobs: usize_field(value, "scannedBlobs"),
        retained_blobs: usize_field(value, "retainedBlobs"),
        deleted_blobs: usize_field(value, "deletedBlobs"),
        failed_blobs: usize_field(value, "failedBlobs"),
    }
}

fn activity_task_claim_from_fixture(value: &Value) -> durust::provider::ActivityTaskClaim {
    durust::provider::ActivityTaskClaim {
        activity_id: durust::ActivityId(string_field(value, "activityId")),
        worker_id: durust::WorkerId::new(string_field(value, "workerId")),
        token: u64_field(value, "token"),
    }
}

fn wait_record_from_fixture(value: &Value) -> durust::provider::WaitRecord {
    durust::provider::WaitRecord {
        wait_id: durust::WaitId::new(string_field(value, "waitId")),
        run_id: durust::RunId::new(string_field(value, "runId")),
        command_id: command_id_from_fixture(&value["commandId"]),
        kind: wait_kind_from_fixture(&value["kind"]),
        key: string_field(value, "key"),
        ready_at: value.get("readyAt").and_then(|ready_at| {
            if ready_at.is_null() {
                None
            } else {
                Some(durust::TimestampMs(
                    ready_at.as_i64().expect("readyAt should be i64"),
                ))
            }
        }),
    }
}

fn new_history_event_from_fixture(value: &Value) -> durust::provider::NewHistoryEvent {
    durust::provider::NewHistoryEvent::new(history_event_data_from_fixture(&value["data"]))
}

fn history_event_from_fixture(value: &Value) -> durust::provider::HistoryEvent {
    let data = history_event_data_from_fixture(&value["data"]);
    durust::provider::HistoryEvent {
        event_id: durust::EventId(u64_field(value, "eventId")),
        data,
    }
}

fn history_event_data_from_fixture(value: &Value) -> durust::provider::HistoryEventData {
    match string_field(value, "kind").as_str() {
        "WorkflowStarted" => durust::provider::HistoryEventData::WorkflowStarted {
            workflow_type: workflow_type_from_fixture(&value["workflowType"]),
            input: payload_ref_from_fixture_json(&value["input"]),
        },
        "WorkflowTaskStarted" => durust::provider::HistoryEventData::WorkflowTaskStarted,
        other => panic!("fixture adapter does not construct history event data kind {other}"),
    }
}

fn workflow_type_from_fixture(value: &Value) -> durust::WorkflowType {
    durust::WorkflowType::new(string_field(value, "name"), u32_field(value, "version"))
}

fn command_id_from_fixture(value: &Value) -> durust::CommandId {
    durust::provider::command_id(
        &durust::RunId::new(string_field(value, "runId")),
        u64_field(value, "seq"),
    )
}

fn workflow_task_reason_from_fixture(value: &Value) -> durust::provider::WorkflowTaskReason {
    match value
        .as_str()
        .expect("workflow task reason should be string")
    {
        "WorkflowStarted" => durust::provider::WorkflowTaskReason::WorkflowStarted,
        "ActivityCompleted" => durust::provider::WorkflowTaskReason::ActivityCompleted,
        "ActivityFailed" => durust::provider::WorkflowTaskReason::ActivityFailed,
        "ActivityTimedOut" => durust::provider::WorkflowTaskReason::ActivityTimedOut,
        "ActivityMapCompleted" => durust::provider::WorkflowTaskReason::ActivityMapCompleted,
        "ActivityMapFailed" => durust::provider::WorkflowTaskReason::ActivityMapFailed,
        "ChildWorkflowStarted" => durust::provider::WorkflowTaskReason::ChildWorkflowStarted,
        "ChildWorkflowCompleted" => durust::provider::WorkflowTaskReason::ChildWorkflowCompleted,
        "ChildWorkflowFailed" => durust::provider::WorkflowTaskReason::ChildWorkflowFailed,
        "ChildWorkflowCancelled" => durust::provider::WorkflowTaskReason::ChildWorkflowCancelled,
        "ChildWorkflowMapCompleted" => {
            durust::provider::WorkflowTaskReason::ChildWorkflowMapCompleted
        }
        "ChildWorkflowMapFailed" => durust::provider::WorkflowTaskReason::ChildWorkflowMapFailed,
        "TimerFired" => durust::provider::WorkflowTaskReason::TimerFired,
        "SignalReceived" => durust::provider::WorkflowTaskReason::SignalReceived,
        "CacheEvicted" => durust::provider::WorkflowTaskReason::CacheEvicted,
        other => panic!("unsupported workflow task reason {other}"),
    }
}

fn wait_kind_from_fixture(value: &Value) -> durust::provider::WaitKind {
    match value.as_str().expect("wait kind should be string") {
        "Timer" => durust::provider::WaitKind::Timer,
        "Signal" => durust::provider::WaitKind::Signal,
        other => panic!("unsupported wait kind {other}"),
    }
}

fn fingerprint_fixture_json(fingerprint: durust::provider::CommandFingerprint) -> Value {
    json!({
        "kind": fingerprint.kind,
        "name": fingerprint.name,
        "inputDigest": fingerprint.input_digest,
        "optionsDigest": fingerprint.options_digest,
    })
}

/// The fixture's payload refs and failures are the canonical serde form of
/// the Rust types (`kind`-tagged, camelCase, bytes as an array of byte
/// values in JSON), so they deserialize directly.
fn payload_ref_from_fixture_json(value: &Value) -> durust::PayloadRef {
    serde_json::from_value(value.clone()).expect("fixture payload ref should deserialize")
}

fn durable_failure_from_fixture_json(value: &Value) -> durust::DurableFailure {
    serde_json::from_value(value.clone()).expect("fixture durable failure should deserialize")
}

fn decode_json_payload_value(value: &Value) -> Value {
    let payload = payload_ref_from_fixture_json(value);
    durust::decode_payload::<Value>(&payload).expect("fixture JSON payload should decode")
}

fn decode_checkout_from_payload_value(value: &Value) -> CheckoutInput {
    let payload = payload_ref_from_fixture_json(value);
    durust::decode_payload::<CheckoutInput>(&payload)
        .expect("fixture checkout payload should decode")
}

fn string_field(value: &Value, field: &str) -> String {
    value[field]
        .as_str()
        .unwrap_or_else(|| panic!("{field} should be a string"))
        .to_owned()
}

fn u64_field(value: &Value, field: &str) -> u64 {
    value[field]
        .as_u64()
        .unwrap_or_else(|| panic!("{field} should be u64"))
}

fn i64_field(value: &Value, field: &str) -> i64 {
    value[field]
        .as_i64()
        .unwrap_or_else(|| panic!("{field} should be i64"))
}

fn usize_field(value: &Value, field: &str) -> usize {
    usize::try_from(u64_field(value, field)).unwrap_or_else(|_| panic!("{field} should fit usize"))
}

fn u32_field(value: &Value, field: &str) -> u32 {
    u32::try_from(u64_field(value, field)).unwrap_or_else(|_| panic!("{field} should fit u32"))
}

fn assert_positive_f64(value: &Value) {
    let number = value.as_f64().expect("fixture field should be f64");
    assert!(number > 0.0, "fixture field should be positive");
}
