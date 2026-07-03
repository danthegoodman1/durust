use durust::{
    DurableBackend, Namespace, RunId, SqliteBackend, WorkflowChangeVersionStatus,
    WorkflowChangeVersionsRequest, WorkflowId, check_manifest, diff_manifests, read_manifest,
    write_manifest,
};
use futures::executor::block_on;
use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "durable") {
        args.remove(0);
    }
    let Some(command) = args.first().map(String::as_str) else {
        return Err(usage());
    };
    match command {
        "manifest" => run_manifest(&args),
        "versions" => run_versions(&args),
        _ => Err(usage()),
    }
}

fn run_manifest(args: &[String]) -> Result<(), String> {
    let Some(action) = args.get(1).map(String::as_str) else {
        return Err(usage());
    };

    let baseline = value_after(args, "--baseline").unwrap_or("durable.manifest.json");
    let current = value_after(args, "--current").unwrap_or("durable.manifest.current.json");
    let output = value_after(args, "--output").unwrap_or(current);

    match action {
        "normalize" => {
            let current = read_manifest(current).map_err(|err| err.to_string())?;
            write_manifest(output, &current).map_err(|err| err.to_string())
        }
        "check" => {
            let baseline = read_manifest(baseline).map_err(|err| err.to_string())?;
            let current = read_manifest(current).map_err(|err| err.to_string())?;
            let diff = check_manifest(&baseline, &current).map_err(|err| err.to_string())?;
            for line in diff.summary_lines() {
                println!("{line}");
            }
            Ok(())
        }
        "diff" => {
            let baseline = read_manifest(baseline).map_err(|err| err.to_string())?;
            let current = read_manifest(current).map_err(|err| err.to_string())?;
            let diff = diff_manifests(&baseline, &current);
            for line in diff.summary_lines() {
                println!("{line}");
            }
            if diff.has_ci_conflicts() {
                Err("manifest diff contains CI conflicts".to_owned())
            } else {
                Ok(())
            }
        }
        "accept" => {
            let current_manifest = read_manifest(current).map_err(|err| err.to_string())?;
            let yes = args.iter().any(|arg| arg == "--yes");
            if !yes {
                let baseline_manifest = read_manifest(baseline).unwrap_or_default();
                let diff = diff_manifests(&baseline_manifest, &current_manifest);
                for line in diff.summary_lines() {
                    println!("{line}");
                }
                return Err("pass --yes to accept the current manifest".to_owned());
            }
            write_manifest(baseline, &current_manifest).map_err(|err| err.to_string())
        }
        _ => Err(usage()),
    }
}

fn run_versions(args: &[String]) -> Result<(), String> {
    let Some(action) = args.get(1).map(String::as_str) else {
        return Err(usage());
    };
    let sqlite_path = value_after(args, "--sqlite")
        .ok_or_else(|| "versions commands require --sqlite <path>".to_owned())?;
    let backend = SqliteBackend::open(sqlite_path).map_err(|err| err.to_string())?;
    let namespace = Namespace::new(value_after(args, "--namespace").unwrap_or("default"));
    let req = WorkflowChangeVersionsRequest {
        namespace,
        workflow_id: value_after(args, "--workflow-id").map(WorkflowId::new),
        run_id: value_after(args, "--run-id").map(RunId::new),
        change_id: value_after(args, "--change-id").map(str::to_owned),
    };
    let outcome = block_on(backend.workflow_change_versions(req)).map_err(|err| err.to_string())?;

    match action {
        "list" | "check" => {
            for record in &outcome.records {
                println!(
                    "{} {} {}@{} run={} change={} version={} kind={:?} status={:?} event={}",
                    record.namespace,
                    record.workflow_id,
                    record.workflow_type.name,
                    record.workflow_type.version,
                    record.run_id,
                    record.change_id,
                    record.version,
                    record.marker_kind,
                    record.status,
                    record.first_event_id
                );
            }
            if action == "check" && outcome.records.is_empty() {
                Err("no matching workflow change versions found".to_owned())
            } else {
                Ok(())
            }
        }
        "safe-to-remove" => {
            if value_after(args, "--change-id").is_none() {
                return Err("safe-to-remove requires --change-id <id>".to_owned());
            }
            if outcome.safe_to_remove() {
                println!("safe to remove: no open workflow runs reference this change");
                Ok(())
            } else {
                let open = outcome
                    .records
                    .iter()
                    .filter(|record| record.status == WorkflowChangeVersionStatus::Open)
                    .count();
                Err(format!(
                    "not safe to remove: {open} open workflow run(s) reference this change"
                ))
            }
        }
        _ => Err(usage()),
    }
}

fn value_after<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.windows(2)
        .find_map(|pair| (pair[0] == flag).then_some(pair[1].as_str()))
}

fn usage() -> String {
    "usage: cargo durable manifest <normalize|check|diff|accept> [--baseline durable.manifest.json] [--current durable.manifest.current.json] [--output durable.manifest.current.json] [--yes]\n       cargo durable versions <list|check|safe-to-remove> --sqlite <path> [--namespace default] [--workflow-id id] [--run-id id] [--change-id id]".to_owned()
}
