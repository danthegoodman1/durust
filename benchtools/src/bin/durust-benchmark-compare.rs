use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::PathBuf;

const DEFAULT_MIN_RATIO: f64 = 0.95;

#[derive(Clone, Debug, PartialEq)]
struct Options {
    durust: PathBuf,
    baseline: PathBuf,
    min_ratio: Ratio,
    json: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Ratio(f64);

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct BenchmarkDimensions {
    provider: String,
    mode: String,
    workflows: u64,
    workers: u64,
    shards: u64,
    activation_concurrency: u64,
    activation_prefetch_limit: u64,
    batch: u64,
    physical_partitions: Option<u64>,
    sqlite_layout: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkComparison {
    dimensions: BenchmarkDimensions,
    durust_workflows_per_second: f64,
    baseline_workflows_per_second: f64,
    ratio: f64,
    min_ratio: f64,
    passed: bool,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let options = parse_args(env::args().skip(1))?;
    let comparison = compare_files(&options)?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&comparison).map_err(|err| err.to_string())?
        );
    } else {
        println!(
            "{} {}: Durust {:.2} workflows/s, baseline {:.2} workflows/s, ratio {:.3}",
            comparison.dimensions.provider,
            comparison.dimensions.mode,
            comparison.durust_workflows_per_second,
            comparison.baseline_workflows_per_second,
            comparison.ratio
        );
    }
    if comparison.passed {
        Ok(())
    } else {
        Err(format!(
            "Durust benchmark ratio {:.3} is below required {:.3}",
            comparison.ratio, comparison.min_ratio
        ))
    }
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Options, String> {
    let mut durust = None;
    let mut baseline = None;
    let mut min_ratio = Ratio(DEFAULT_MIN_RATIO);
    let mut json = false;
    let mut args = args.into_iter();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--durust" => durust = Some(PathBuf::from(next_arg(&mut args, &flag)?)),
            "--baseline" => baseline = Some(PathBuf::from(next_arg(&mut args, &flag)?)),
            "--min-ratio" => {
                let value = next_arg(&mut args, &flag)?;
                let parsed = value
                    .parse::<f64>()
                    .map_err(|_| format!("{flag} must be a number"))?;
                if !(parsed.is_finite() && parsed > 0.0) {
                    return Err(format!("{flag} must be positive"));
                }
                min_ratio = Ratio(parsed);
            }
            "--json" => json = true,
            "--help" | "-h" => {
                return Err(
                    "usage: durust-benchmark-compare --durust PATH --baseline PATH [--min-ratio 0.95] [--json]"
                        .to_owned(),
                );
            }
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    Ok(Options {
        durust: durust.ok_or_else(|| "--durust is required".to_owned())?,
        baseline: baseline.ok_or_else(|| "--baseline is required".to_owned())?,
        min_ratio,
        json,
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn compare_files(options: &Options) -> Result<BenchmarkComparison, String> {
    let durust = read_result(&options.durust, "Durust")?;
    let baseline = read_result(&options.baseline, "baseline")?;
    compare_results(durust, baseline, options.min_ratio.0)
}

fn read_result(path: &PathBuf, label: &str) -> Result<BenchmarkResult, String> {
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read `{}`: {err}", path.display()))?;
    parse_result(&raw, label)
}

fn parse_result(raw: &str, label: &str) -> Result<BenchmarkResult, String> {
    let value: Value = serde_json::from_str(raw).map_err(|err| format!("{label} JSON: {err}"))?;
    if value.get("correct").and_then(Value::as_bool) != Some(true) {
        return Err(format!("{label} result must have correct=true"));
    }
    let dimensions = BenchmarkDimensions {
        provider: string_field(&value, &["backend", "provider"])
            .or_else(|| string_field(value.get("options")?, &["provider", "backend"]))
            .ok_or_else(|| format!("{label} result missing provider/backend"))?,
        mode: string_field(&value, &["mode"])
            .or_else(|| string_field(value.get("options")?, &["mode"]))
            .ok_or_else(|| format!("{label} result missing mode"))?,
        workflows: integer_dimension(&value, "workflows", label)?,
        workers: integer_dimension(&value, "workers", label)?,
        shards: integer_dimension(&value, "shards", label)?,
        activation_concurrency: integer_dimension(&value, "activationConcurrency", label)?,
        activation_prefetch_limit: integer_dimension(&value, "activationPrefetchLimit", label)?,
        batch: integer_dimension(&value, "batch", label)?,
        physical_partitions: optional_integer_dimension(&value, "physicalPartitions")?,
        sqlite_layout: string_field(&value, &["sqliteLayout"])
            .or_else(|| string_field(value.get("options")?, &["sqliteLayout"])),
    };
    let workflows_per_second = number_field(
        &value,
        &["processingWorkflowsPerSecond", "workflowsPerSecond"],
    )
    .ok_or_else(|| {
        format!("{label} result missing processingWorkflowsPerSecond/workflowsPerSecond")
    })?;
    if !(workflows_per_second.is_finite() && workflows_per_second > 0.0) {
        return Err(format!(
            "{label} workflows-per-second must be positive, got {workflows_per_second}"
        ));
    }
    Ok(BenchmarkResult {
        dimensions,
        workflows_per_second,
    })
}

fn compare_results(
    durust: BenchmarkResult,
    baseline: BenchmarkResult,
    min_ratio: f64,
) -> Result<BenchmarkComparison, String> {
    if durust.dimensions != baseline.dimensions {
        return Err(format!(
            "benchmark dimensions differ: Durust {:?}, baseline {:?}",
            durust.dimensions, baseline.dimensions
        ));
    }
    let ratio = durust.workflows_per_second / baseline.workflows_per_second;
    Ok(BenchmarkComparison {
        dimensions: durust.dimensions,
        durust_workflows_per_second: durust.workflows_per_second,
        baseline_workflows_per_second: baseline.workflows_per_second,
        ratio,
        min_ratio,
        passed: ratio >= min_ratio,
    })
}

#[derive(Clone, Debug, PartialEq)]
struct BenchmarkResult {
    dimensions: BenchmarkDimensions,
    workflows_per_second: f64,
}

fn integer_dimension(value: &Value, key: &str, label: &str) -> Result<u64, String> {
    optional_integer_dimension(value, key)?.ok_or_else(|| {
        format!("{label} result missing comparable dimension `{key}` in top-level or options")
    })
}

fn optional_integer_dimension(value: &Value, key: &str) -> Result<Option<u64>, String> {
    let raw = value
        .get(key)
        .or_else(|| value.get("options").and_then(|options| options.get(key)));
    match raw {
        None => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("dimension `{key}` must be an unsigned integer")),
    }
}

fn string_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(str::to_owned)
}

fn number_field(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_f64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result_json(score: f64) -> String {
        format!(
            r#"{{
                "backend": "postgres",
                "mode": "mixed",
                "correct": true,
                "options": {{
                    "workflows": 1000,
                    "workers": 4,
                    "shards": 4,
                    "activationConcurrency": 8,
                    "activationPrefetchLimit": 32,
                    "batch": 64,
                    "physicalPartitions": 4
                }},
                "processingWorkflowsPerSecond": {score}
            }}"#
        )
    }

    #[test]
    fn compares_matching_benchmark_dimensions() {
        let durust = parse_result(&result_json(950.0), "Durust").unwrap();
        let baseline = parse_result(&result_json(1000.0), "baseline").unwrap();
        let comparison = compare_results(durust, baseline, 0.95).unwrap();
        assert!(comparison.passed);
        assert_eq!(comparison.ratio, 0.95);
    }

    #[test]
    fn fails_when_ratio_is_below_threshold() {
        let durust = parse_result(&result_json(940.0), "Durust").unwrap();
        let baseline = parse_result(&result_json(1000.0), "baseline").unwrap();
        let comparison = compare_results(durust, baseline, 0.95).unwrap();
        assert!(!comparison.passed);
    }

    #[test]
    fn rejects_mismatched_dimensions_before_comparing() {
        let durust = parse_result(&result_json(1000.0), "Durust").unwrap();
        let mut baseline = parse_result(&result_json(1000.0), "baseline").unwrap();
        baseline.dimensions.workers = 8;
        assert!(
            compare_results(durust, baseline, 0.95)
                .unwrap_err()
                .contains("dimensions differ")
        );
    }

    #[test]
    fn rejects_missing_correct_or_throughput() {
        assert!(
            parse_result(r#"{"correct":false}"#, "Durust")
                .unwrap_err()
                .contains("correct=true")
        );
        let mut value: Value = serde_json::from_str(&result_json(1000.0)).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("processingWorkflowsPerSecond");
        assert!(
            parse_result(&value.to_string(), "Durust")
                .unwrap_err()
                .contains("workflowsPerSecond")
        );
    }

    #[test]
    fn reads_workflows_per_second_fallback() {
        let raw = result_json(1000.0).replace(
            "\"processingWorkflowsPerSecond\": 1000",
            "\"workflowsPerSecond\": 1000",
        );
        let result = parse_result(&raw, "Durust").unwrap();
        assert_eq!(result.workflows_per_second, 1000.0);
    }
}
