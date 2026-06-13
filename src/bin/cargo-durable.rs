use durust::{check_manifest, diff_manifests, read_manifest, write_manifest};
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
    if command != "manifest" {
        return Err(usage());
    }
    let Some(action) = args.get(1).map(String::as_str) else {
        return Err(usage());
    };

    let baseline = value_after(&args, "--baseline").unwrap_or("durable.manifest.json");
    let current = value_after(&args, "--current").unwrap_or("durable.manifest.current.json");
    let output = value_after(&args, "--output").unwrap_or(current);

    match action {
        "write" => {
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

fn value_after<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.windows(2)
        .find_map(|pair| (pair[0] == flag).then_some(pair[1].as_str()))
}

fn usage() -> String {
    "usage: cargo durable manifest <write|check|diff|accept> [--baseline durable.manifest.json] [--current durable.manifest.current.json] [--output durable.manifest.current.json] [--yes]".to_owned()
}
