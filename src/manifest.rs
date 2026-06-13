use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DurableManifest {
    #[serde(default)]
    pub workflows: Vec<ManifestWorkflow>,
    #[serde(default)]
    pub activities: Vec<ManifestActivity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestWorkflow {
    pub name: String,
    pub version: u32,
    pub rust_path: String,
    pub input_type: String,
    pub output_type: String,
    pub input_schema_hash: String,
    pub output_schema_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestActivity {
    pub name: String,
    pub rust_path: String,
    pub input_type: String,
    pub output_type: String,
    pub input_schema_hash: String,
    pub output_schema_hash: String,
}

#[derive(Clone, Copy)]
pub enum DurableExport {
    Workflow(fn() -> ManifestWorkflow),
    Activity(fn() -> ManifestActivity),
}

inventory::collect!(DurableExport);

pub fn exported_manifest() -> DurableManifest {
    let mut manifest = DurableManifest::default();
    for export in inventory::iter::<DurableExport> {
        match *export {
            DurableExport::Workflow(factory) => manifest.workflows.push(factory()),
            DurableExport::Activity(factory) => manifest.activities.push(factory()),
        }
    }

    manifest
        .workflows
        .sort_by(|left, right| workflow_key(left).cmp(&workflow_key(right)));
    manifest
        .activities
        .sort_by(|left, right| left.name.cmp(&right.name));
    manifest
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManifestDiff {
    pub added_workflows: Vec<String>,
    pub vanished_workflows: Vec<String>,
    pub changed_workflow_schemas: Vec<String>,
    pub moved_workflow_rust_paths: Vec<String>,
    pub added_activities: Vec<String>,
    pub vanished_activities: Vec<String>,
    pub changed_activity_schemas: Vec<String>,
    pub moved_activity_rust_paths: Vec<String>,
    pub duplicate_workflows: Vec<String>,
    pub duplicate_activities: Vec<String>,
}

impl ManifestDiff {
    pub fn has_ci_conflicts(&self) -> bool {
        !self.vanished_workflows.is_empty()
            || !self.changed_workflow_schemas.is_empty()
            || !self.vanished_activities.is_empty()
            || !self.changed_activity_schemas.is_empty()
            || !self.duplicate_workflows.is_empty()
            || !self.duplicate_activities.is_empty()
    }

    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    pub fn summary_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        push_lines(&mut lines, "added workflow", &self.added_workflows);
        push_lines(&mut lines, "vanished workflow", &self.vanished_workflows);
        push_lines(
            &mut lines,
            "workflow schema changed",
            &self.changed_workflow_schemas,
        );
        push_lines(
            &mut lines,
            "workflow rust path moved",
            &self.moved_workflow_rust_paths,
        );
        push_lines(&mut lines, "added activity", &self.added_activities);
        push_lines(&mut lines, "vanished activity", &self.vanished_activities);
        push_lines(
            &mut lines,
            "activity schema changed",
            &self.changed_activity_schemas,
        );
        push_lines(
            &mut lines,
            "activity rust path moved",
            &self.moved_activity_rust_paths,
        );
        push_lines(&mut lines, "duplicate workflow", &self.duplicate_workflows);
        push_lines(&mut lines, "duplicate activity", &self.duplicate_activities);
        if lines.is_empty() {
            lines.push("manifest unchanged".to_owned());
        }
        lines
    }
}

pub fn read_manifest(path: impl AsRef<Path>) -> Result<DurableManifest> {
    let bytes = fs::read(path.as_ref()).map_err(|err| {
        Error::Backend(format!(
            "failed to read manifest `{}`: {err}",
            path.as_ref().display()
        ))
    })?;
    serde_json::from_slice(&bytes).map_err(|err| {
        Error::PayloadDecode(format!(
            "failed to parse manifest `{}`: {err}",
            path.as_ref().display()
        ))
    })
}

pub fn write_manifest(path: impl AsRef<Path>, manifest: &DurableManifest) -> Result<()> {
    let bytes =
        serde_json::to_vec_pretty(manifest).map_err(|err| Error::PayloadEncode(err.to_string()))?;
    fs::write(path.as_ref(), bytes).map_err(|err| {
        Error::Backend(format!(
            "failed to write manifest `{}`: {err}",
            path.as_ref().display()
        ))
    })
}

pub fn diff_manifests(baseline: &DurableManifest, current: &DurableManifest) -> ManifestDiff {
    let mut diff = ManifestDiff {
        duplicate_workflows: duplicate_workflows(current),
        duplicate_activities: duplicate_activities(current),
        ..ManifestDiff::default()
    };

    let baseline_workflows = baseline
        .workflows
        .iter()
        .map(|workflow| (workflow_key(workflow), workflow))
        .collect::<BTreeMap<_, _>>();
    let current_workflows = current
        .workflows
        .iter()
        .map(|workflow| (workflow_key(workflow), workflow))
        .collect::<BTreeMap<_, _>>();

    for (key, old) in &baseline_workflows {
        match current_workflows.get(key) {
            None => diff.vanished_workflows.push(key.clone()),
            Some(new) => {
                if old.input_schema_hash != new.input_schema_hash
                    || old.output_schema_hash != new.output_schema_hash
                    || old.input_type != new.input_type
                    || old.output_type != new.output_type
                {
                    diff.changed_workflow_schemas.push(key.clone());
                }
                if old.rust_path != new.rust_path {
                    diff.moved_workflow_rust_paths.push(key.clone());
                }
            }
        }
    }
    for key in current_workflows.keys() {
        if !baseline_workflows.contains_key(key) {
            diff.added_workflows.push(key.clone());
        }
    }

    let baseline_activities = baseline
        .activities
        .iter()
        .map(|activity| (activity.name.clone(), activity))
        .collect::<BTreeMap<_, _>>();
    let current_activities = current
        .activities
        .iter()
        .map(|activity| (activity.name.clone(), activity))
        .collect::<BTreeMap<_, _>>();

    for (key, old) in &baseline_activities {
        match current_activities.get(key) {
            None => diff.vanished_activities.push(key.clone()),
            Some(new) => {
                if old.input_schema_hash != new.input_schema_hash
                    || old.output_schema_hash != new.output_schema_hash
                    || old.input_type != new.input_type
                    || old.output_type != new.output_type
                {
                    diff.changed_activity_schemas.push(key.clone());
                }
                if old.rust_path != new.rust_path {
                    diff.moved_activity_rust_paths.push(key.clone());
                }
            }
        }
    }
    for key in current_activities.keys() {
        if !baseline_activities.contains_key(key) {
            diff.added_activities.push(key.clone());
        }
    }

    diff
}

pub fn check_manifest(
    baseline: &DurableManifest,
    current: &DurableManifest,
) -> Result<ManifestDiff> {
    let diff = diff_manifests(baseline, current);
    if diff.has_ci_conflicts() {
        return Err(Error::Backend(format!(
            "manifest check failed:\n{}",
            diff.summary_lines().join("\n")
        )));
    }
    Ok(diff)
}

fn workflow_key(workflow: &ManifestWorkflow) -> String {
    format!("{}@{}", workflow.name, workflow.version)
}

fn duplicate_workflows(manifest: &DurableManifest) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for workflow in &manifest.workflows {
        let key = workflow_key(workflow);
        if !seen.insert(key.clone()) {
            duplicates.insert(key);
        }
    }
    duplicates.into_iter().collect()
}

fn duplicate_activities(manifest: &DurableManifest) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for activity in &manifest.activities {
        if !seen.insert(activity.name.clone()) {
            duplicates.insert(activity.name.clone());
        }
    }
    duplicates.into_iter().collect()
}

fn push_lines(lines: &mut Vec<String>, label: &str, values: &[String]) {
    for value in values {
        lines.push(format!("{label}: {value}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_fails_for_vanished_workflow_identity() {
        let baseline = DurableManifest {
            workflows: vec![workflow("orders.checkout", 1, "hash:a", "hash:b")],
            activities: Vec::new(),
        };
        let current = DurableManifest::default();

        let err = check_manifest(&baseline, &current).unwrap_err();
        assert!(err.to_string().contains("vanished workflow"));
    }

    #[test]
    fn check_fails_for_schema_change_without_version_bump() {
        let baseline = DurableManifest {
            workflows: vec![workflow("orders.checkout", 1, "hash:a", "hash:b")],
            activities: vec![activity("payments.charge", "hash:c", "hash:d")],
        };
        let current = DurableManifest {
            workflows: vec![workflow("orders.checkout", 1, "hash:changed", "hash:b")],
            activities: vec![activity("payments.charge", "hash:c", "hash:changed")],
        };

        let err = check_manifest(&baseline, &current).unwrap_err();
        assert!(err.to_string().contains("workflow schema changed"));
        assert!(err.to_string().contains("activity schema changed"));
    }

    #[test]
    fn check_allows_added_identity_and_rust_path_move() {
        let baseline = DurableManifest {
            workflows: vec![workflow("orders.checkout", 1, "hash:a", "hash:b")],
            activities: Vec::new(),
        };
        let mut moved = workflow("orders.checkout", 1, "hash:a", "hash:b");
        moved.rust_path = "new::path".to_owned();
        let current = DurableManifest {
            workflows: vec![moved, workflow("orders.checkout", 2, "hash:c", "hash:d")],
            activities: Vec::new(),
        };

        let diff = check_manifest(&baseline, &current).unwrap();
        assert_eq!(
            diff.moved_workflow_rust_paths,
            vec!["orders.checkout@1".to_owned()]
        );
        assert_eq!(diff.added_workflows, vec!["orders.checkout@2".to_owned()]);
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
}
