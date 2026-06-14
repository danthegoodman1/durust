use serde_json::Value;

#[test]
fn payload_thresholds_are_sane_and_reference_existing_benchmarks() {
    let thresholds: Value =
        serde_json::from_str(include_str!("../benches/payload_thresholds.json"))
            .expect("payload threshold metadata should be valid JSON");
    assert_eq!(thresholds["schema"], 1);
    assert_eq!(thresholds["unit"], "nanoseconds_per_iteration");

    let bench_source = include_str!("../benches/replay_core.rs");
    let benchmarks = thresholds["benchmarks"]
        .as_array()
        .expect("payload thresholds should contain benchmark entries");
    assert!(!benchmarks.is_empty());

    for benchmark in benchmarks {
        let name = benchmark["name"]
            .as_str()
            .expect("benchmark threshold should include a name");
        assert!(
            benchmark_exists(bench_source, name),
            "threshold references missing benchmark `{name}`"
        );

        let local_baseline = positive_u64(benchmark, "local_baseline_ns", name);
        let warn = positive_u64(benchmark, "warn_above_ns", name);
        let fail = positive_u64(benchmark, "fail_above_ns", name);
        assert!(
            local_baseline <= warn && warn <= fail,
            "threshold ordering should be baseline <= warn <= fail for `{name}`"
        );
    }
}

fn benchmark_exists(source: &str, name: &str) -> bool {
    if let Some((group, function)) = name.split_once('/') {
        source.contains(&format!("benchmark_group(\"{group}\")"))
            && source.contains(&format!("bench_function(\"{function}\""))
    } else {
        source.contains(&format!("bench_function(\"{name}\""))
    }
}

fn positive_u64(benchmark: &Value, field: &str, name: &str) -> u64 {
    let value = benchmark[field]
        .as_u64()
        .unwrap_or_else(|| panic!("benchmark `{name}` should include numeric `{field}`"));
    assert!(
        value > 0,
        "benchmark `{name}` field `{field}` must be positive"
    );
    value
}
