use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Options {
    criterion_dir: PathBuf,
    group: String,
    required: Vec<String>,
    json: bool,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkReport {
    group: String,
    benchmarks: Vec<BenchmarkMetric>,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkMetric {
    name: String,
    p50_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
    throughput_per_second: f64,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let options = parse_args(env::args().skip(1))?;
    let report = collect_report(&options)?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|err| err.to_string())?
        );
    } else {
        print_text_report(&report);
    }
    Ok(())
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Options, String> {
    let mut criterion_dir = PathBuf::from("target/criterion");
    let mut group = None;
    let mut required = Vec::new();
    let mut json = false;
    let mut args = args.into_iter();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--criterion-dir" => {
                criterion_dir = PathBuf::from(next_arg(&mut args, &flag)?);
            }
            "--group" => {
                group = Some(next_arg(&mut args, &flag)?);
            }
            "--require" => {
                required.push(next_arg(&mut args, &flag)?);
            }
            "--json" => {
                json = true;
            }
            "--help" | "-h" => {
                return Err(
                    "usage: durust-benchmark-report --group GROUP [--criterion-dir PATH] [--require NAME] [--json]"
                        .to_owned(),
                );
            }
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    let group = group.ok_or_else(|| "--group is required".to_owned())?;
    Ok(Options {
        criterion_dir,
        group,
        required,
        json,
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn collect_report(options: &Options) -> Result<BenchmarkReport, String> {
    let group_dir = options.criterion_dir.join(&options.group);
    let mut benchmarks = Vec::new();
    for entry in fs::read_dir(&group_dir)
        .map_err(|err| format!("failed to read `{}`: {err}", group_dir.display()))?
    {
        let entry = entry.map_err(|err| err.to_string())?;
        let file_type = entry.file_type().map_err(|err| err.to_string())?;
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let sample_path = entry.path().join("new").join("sample.json");
        if !sample_path.exists() {
            continue;
        }
        benchmarks.push(metric_from_sample(&name, &sample_path)?);
    }
    benchmarks.sort_by(|left, right| left.name.cmp(&right.name));
    for required in &options.required {
        if !benchmarks
            .iter()
            .any(|benchmark| benchmark.name == *required)
        {
            return Err(format!(
                "required benchmark `{required}` missing from group `{}`",
                options.group
            ));
        }
    }
    if benchmarks.is_empty() {
        return Err(format!(
            "no Criterion samples found under `{}`",
            group_dir.display()
        ));
    }
    Ok(BenchmarkReport {
        group: options.group.clone(),
        benchmarks,
    })
}

fn metric_from_sample(name: &str, sample_path: &Path) -> Result<BenchmarkMetric, String> {
    let sample = fs::read_to_string(sample_path)
        .map_err(|err| format!("failed to read `{}`: {err}", sample_path.display()))?;
    let mut per_iteration_ns = per_iteration_ns(&sample)
        .map_err(|err| format!("invalid `{}`: {err}", sample_path.display()))?;
    per_iteration_ns.sort_by(|left, right| left.total_cmp(right));
    let p50_ns = quantile(&per_iteration_ns, 0.50);
    let p95_ns = quantile(&per_iteration_ns, 0.95);
    let p99_ns = quantile(&per_iteration_ns, 0.99);
    Ok(BenchmarkMetric {
        name: name.to_owned(),
        p50_ns,
        p95_ns,
        p99_ns,
        throughput_per_second: 1_000_000_000.0 / p50_ns,
    })
}

fn per_iteration_ns(sample_json: &str) -> Result<Vec<f64>, String> {
    let value: Value = serde_json::from_str(sample_json).map_err(|err| err.to_string())?;
    let iters = number_array(&value, "iters")?;
    let times = number_array(&value, "times")?;
    if iters.is_empty() {
        return Err("iters must not be empty".to_owned());
    }
    if iters.len() != times.len() {
        return Err(format!(
            "iters/times length mismatch: {} != {}",
            iters.len(),
            times.len()
        ));
    }
    iters
        .into_iter()
        .zip(times)
        .map(|(iters, time)| {
            if !iters.is_finite() || iters <= 0.0 {
                return Err(format!("iteration count must be positive, got {iters}"));
            }
            if !time.is_finite() || time <= 0.0 {
                return Err(format!("sample time must be positive, got {time}"));
            }
            Ok(time / iters)
        })
        .collect()
}

fn number_array(value: &Value, key: &str) -> Result<Vec<f64>, String> {
    let array = value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("missing numeric array `{key}`"))?;
    array
        .iter()
        .map(|value| {
            value
                .as_f64()
                .ok_or_else(|| format!("`{key}` contains a non-number"))
        })
        .collect()
}

fn quantile(sorted_values: &[f64], quantile: f64) -> f64 {
    debug_assert!(!sorted_values.is_empty());
    let position = (sorted_values.len() - 1) as f64 * quantile;
    let low = position.floor() as usize;
    let high = position.ceil() as usize;
    if low == high {
        sorted_values[low]
    } else {
        let fraction = position - low as f64;
        sorted_values[low] + (sorted_values[high] - sorted_values[low]) * fraction
    }
}

fn print_text_report(report: &BenchmarkReport) {
    println!("group: {}", report.group);
    for benchmark in &report.benchmarks {
        println!(
            "{} p50={} p95={} p99={} throughput={:.0}/s",
            benchmark.name,
            format_ns(benchmark.p50_ns),
            format_ns(benchmark.p95_ns),
            format_ns(benchmark.p99_ns),
            benchmark.throughput_per_second
        );
    }
}

fn format_ns(ns: f64) -> String {
    if ns >= 1_000_000.0 {
        format!("{:.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:.0} us", ns / 1_000.0)
    } else {
        format!("{ns:.0} ns")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_criterion_samples_into_per_iteration_latency() {
        let values = per_iteration_ns(r#"{"iters":[10,20,30],"times":[1000,5000,12000]}"#).unwrap();
        assert_eq!(values, vec![100.0, 250.0, 400.0]);
    }

    #[test]
    fn rejects_missing_or_mismatched_sample_metrics() {
        assert!(
            per_iteration_ns(r#"{"iters":[1]}"#)
                .unwrap_err()
                .contains("times")
        );
        assert!(
            per_iteration_ns(r#"{"iters":[1,2],"times":[3]}"#)
                .unwrap_err()
                .contains("length mismatch")
        );
        assert!(
            per_iteration_ns(r#"{"iters":[0],"times":[3]}"#)
                .unwrap_err()
                .contains("positive")
        );
    }

    #[test]
    fn reports_required_benchmark_presence() {
        let temp = tempfile::tempdir().unwrap();
        let sample_dir = temp.path().join("group").join("bench").join("new");
        fs::create_dir_all(&sample_dir).unwrap();
        fs::write(
            sample_dir.join("sample.json"),
            r#"{"iters":[1,2,3],"times":[10,30,60]}"#,
        )
        .unwrap();
        let report = collect_report(&Options {
            criterion_dir: temp.path().to_path_buf(),
            group: "group".to_owned(),
            required: vec!["bench".to_owned()],
            json: true,
        })
        .unwrap();
        assert_eq!(report.benchmarks.len(), 1);
        assert_eq!(report.benchmarks[0].name, "bench");
        assert!(
            collect_report(&Options {
                criterion_dir: temp.path().to_path_buf(),
                group: "group".to_owned(),
                required: vec!["missing".to_owned()],
                json: true,
            })
            .unwrap_err()
            .contains("required benchmark")
        );
    }

    #[test]
    fn quantile_interpolates_between_samples() {
        let values = [10.0, 20.0, 30.0, 40.0];
        assert_eq!(quantile(&values, 0.50), 25.0);
        assert_eq!(quantile(&values, 0.95), 38.5);
    }
}
