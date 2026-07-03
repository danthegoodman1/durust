use durust::{
    ClaimWorkflowTaskOptions, DurableBackend, DurableManifest, EventId, HistoryEventData,
    ManifestActivity, ManifestWorkflow, Namespace, NewHistoryEvent, SqliteBackend,
    StartWorkflowOutcome, StartWorkflowRequest, TaskQueue, VersionMarker, WorkerId,
    WorkflowChangeMarkerKind, WorkflowId, WorkflowTaskCommit, WorkflowType, write_manifest,
};
use futures::executor::block_on;
use std::process::Command;

#[test]
fn manifest_check_exits_nonzero_for_ci_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("durable.manifest.json");
    let current = dir.path().join("durable.manifest.current.json");
    write_manifest(
        &baseline,
        &DurableManifest {
            workflows: vec![workflow("orders.checkout", 1, "hash:input", "hash:output")],
            activities: Vec::new(),
        },
    )
    .unwrap();
    write_manifest(&current, &DurableManifest::default()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cargo-durable"))
        .args([
            "durable",
            "manifest",
            "check",
            "--baseline",
            baseline.to_str().unwrap(),
            "--current",
            current.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("vanished workflow: orders.checkout@1"));
}

#[test]
fn manifest_normalize_reserializes_current_manifest_to_output() {
    let dir = tempfile::tempdir().unwrap();
    let current = dir.path().join("durable.manifest.current.json");
    let output_path = dir.path().join("normalized.manifest.json");
    let manifest = DurableManifest {
        workflows: vec![workflow("orders.checkout", 1, "hash:input", "hash:output")],
        activities: vec![activity(
            "payments.charge",
            "hash:charge-in",
            "hash:charge-out",
        )],
    };
    write_manifest(&current, &manifest).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cargo-durable"))
        .args([
            "durable",
            "manifest",
            "normalize",
            "--current",
            current.to_str().unwrap(),
            "--output",
            output_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(durust::read_manifest(&output_path).unwrap(), manifest);
}

#[test]
fn manifest_diff_prints_changes_and_exits_nonzero_for_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("durable.manifest.json");
    let current = dir.path().join("durable.manifest.current.json");
    write_manifest(
        &baseline,
        &DurableManifest {
            workflows: vec![workflow("orders.checkout", 1, "hash:input", "hash:output")],
            activities: Vec::new(),
        },
    )
    .unwrap();
    write_manifest(
        &current,
        &DurableManifest {
            workflows: vec![workflow(
                "orders.checkout",
                1,
                "hash:changed",
                "hash:output",
            )],
            activities: Vec::new(),
        },
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cargo-durable"))
        .args([
            "durable",
            "manifest",
            "diff",
            "--baseline",
            baseline.to_str().unwrap(),
            "--current",
            current.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("workflow schema changed: orders.checkout@1"));
}

#[test]
fn manifest_accept_updates_baseline_after_yes() {
    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("durable.manifest.json");
    let current = dir.path().join("durable.manifest.current.json");
    write_manifest(&baseline, &DurableManifest::default()).unwrap();
    let current_manifest = DurableManifest {
        workflows: vec![workflow(
            "orders.checkout",
            2,
            "hash:new-input",
            "hash:new-output",
        )],
        activities: vec![activity(
            "payments.charge",
            "hash:charge-in",
            "hash:charge-out",
        )],
    };
    write_manifest(&current, &current_manifest).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cargo-durable"))
        .args([
            "durable",
            "manifest",
            "accept",
            "--baseline",
            baseline.to_str().unwrap(),
            "--current",
            current.to_str().unwrap(),
            "--yes",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert_eq!(durust::read_manifest(&baseline).unwrap(), current_manifest);
}

#[test]
fn versions_safe_to_remove_queries_sqlite_marker_index() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("versions.sqlite3");
        let backend = SqliteBackend::open(&db_path).unwrap();
        let outcome = backend
            .start_workflow(StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: WorkflowId::new("wf/version-cli"),
                workflow_type: WorkflowType::new("cli.versioned", 1),
                task_queue: TaskQueue::new("workflows"),
                input: durust::encode_payload(&()).unwrap(),
            })
            .await
            .unwrap();
        let StartWorkflowOutcome::Started { run_id } = outcome else {
            panic!("expected started workflow");
        };
        let claimed = backend
            .claim_workflow_task(
                WorkerId::new("version-cli-worker"),
                ClaimWorkflowTaskOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("workflows"),
                    registered_workflow_types: vec![WorkflowType::new("cli.versioned", 1)],
                    lease_duration: std::time::Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("workflow task");
        let command_id = durust::command_id(&run_id, 1);
        backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: EventId(1),
                    append_events: vec![NewHistoryEvent::new(HistoryEventData::VersionMarker(
                        VersionMarker {
                            command_id,
                            change_id: "cli-change".to_owned(),
                            version: 1,
                        },
                    ))],
                    upsert_waits: Vec::new(),
                    schedule_activities: Vec::new(),
                    schedule_activity_maps: Vec::new(),
                    schedule_child_workflow_maps: Vec::new(),
                    start_child_workflows: Vec::new(),
                    consume_signals: Vec::new(),
                    delete_waits: Vec::new(),
                    cancel_commands: Vec::new(),
                    query_projection: None,
                },
            )
            .await
            .unwrap();
        let records = backend
            .workflow_change_versions(durust::WorkflowChangeVersionsRequest {
                namespace: Namespace::default(),
                workflow_id: None,
                run_id: Some(run_id.clone()),
                change_id: Some("cli-change".to_owned()),
            })
            .await
            .unwrap();
        assert_eq!(
            records.records[0].marker_kind,
            WorkflowChangeMarkerKind::Version
        );

        let open = Command::new(env!("CARGO_BIN_EXE_cargo-durable"))
            .args([
                "durable",
                "versions",
                "safe-to-remove",
                "--sqlite",
                db_path.to_str().unwrap(),
                "--change-id",
                "cli-change",
            ])
            .output()
            .unwrap();
        assert!(!open.status.success());
        assert!(String::from_utf8(open.stderr).unwrap().contains("not safe"));

        backend
            .cancel_workflow(durust::CancelWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: WorkflowId::new("wf/version-cli"),
                reason: "done".to_owned(),
            })
            .await
            .unwrap();

        let closed = Command::new(env!("CARGO_BIN_EXE_cargo-durable"))
            .args([
                "durable",
                "versions",
                "safe-to-remove",
                "--sqlite",
                db_path.to_str().unwrap(),
                "--change-id",
                "cli-change",
            ])
            .output()
            .unwrap();
        assert!(closed.status.success());
        assert!(
            String::from_utf8(closed.stdout)
                .unwrap()
                .contains("safe to remove")
        );
    });
}

fn workflow(
    name: &str,
    version: u32,
    input_type_name_hash: &str,
    output_type_name_hash: &str,
) -> ManifestWorkflow {
    ManifestWorkflow {
        name: name.to_owned(),
        version,
        rust_path: "crate::workflow".to_owned(),
        input_type: "Input".to_owned(),
        output_type: "Output".to_owned(),
        query_state_type: None,
        input_type_name_hash: input_type_name_hash.to_owned(),
        output_type_name_hash: output_type_name_hash.to_owned(),
        query_state_type_name_hash: None,
    }
}

fn activity(
    name: &str,
    input_type_name_hash: &str,
    output_type_name_hash: &str,
) -> ManifestActivity {
    ManifestActivity {
        name: name.to_owned(),
        rust_path: "crate::activity".to_owned(),
        input_type: "Input".to_owned(),
        output_type: "Output".to_owned(),
        input_type_name_hash: input_type_name_hash.to_owned(),
        output_type_name_hash: output_type_name_hash.to_owned(),
    }
}
