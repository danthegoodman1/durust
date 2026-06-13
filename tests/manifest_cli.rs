use durust::{DurableManifest, ManifestActivity, ManifestWorkflow, write_manifest};
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
fn manifest_write_materializes_current_manifest_to_output() {
    let dir = tempfile::tempdir().unwrap();
    let current = dir.path().join("durable.manifest.current.json");
    let output_path = dir.path().join("written.manifest.json");
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
            "write",
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

fn workflow(
    name: &str,
    version: u32,
    input_schema_hash: &str,
    output_schema_hash: &str,
) -> ManifestWorkflow {
    ManifestWorkflow {
        name: name.to_owned(),
        version,
        rust_path: "crate::workflow".to_owned(),
        input_type: "Input".to_owned(),
        output_type: "Output".to_owned(),
        input_schema_hash: input_schema_hash.to_owned(),
        output_schema_hash: output_schema_hash.to_owned(),
    }
}

fn activity(name: &str, input_schema_hash: &str, output_schema_hash: &str) -> ManifestActivity {
    ManifestActivity {
        name: name.to_owned(),
        rust_path: "crate::activity".to_owned(),
        input_type: "Input".to_owned(),
        output_type: "Output".to_owned(),
        input_schema_hash: input_schema_hash.to_owned(),
        output_schema_hash: output_schema_hash.to_owned(),
    }
}
