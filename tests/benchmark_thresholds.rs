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

#[test]
fn phase_0008_benchmark_profiles_have_stable_names() {
    let bench_source = include_str!("../benches/replay_core.rs");
    let required = [
        ("warm cached workflow", "workflow_cached_wake_poll_memory"),
        ("recovery", "workflow_replay_small_history_memory"),
        ("activity claim complete", "activity_claim_complete_memory"),
        ("signal send consume", "signal_send_consume_memory"),
        ("timer wakeup", "timer_due_scan_wakeup_memory"),
        ("child workflow dispatch", "child_start_dispatch_memory"),
        ("activity map fanout", "activity_map_materialize_memory"),
        (
            "activity map completion",
            "activity_map_item_complete_memory",
        ),
        ("payload refs", "payload_blob_history_stream_memory_64kb"),
        (
            "payload replay",
            "workflow_replay_large_payload_blob_memory_64kb",
        ),
        ("sqlite baseline", "workflow_one_activity_e2e_sqlite"),
        (
            "sqlite mixed throughput",
            "sqlite_single_file_throughput/drain_1000_mixed_workflows_4_workers",
        ),
    ];

    for (profile, benchmark) in required {
        assert!(
            benchmark_exists(bench_source, benchmark),
            "phase 0008 benchmark profile `{profile}` is missing benchmark `{benchmark}`"
        );
    }
}

#[test]
fn phase_0009_recovery_flow_control_benchmark_profiles_have_stable_names() {
    let bench_source = include_str!("../benches/replay_core.rs");
    let required = [
        (
            "cold recovery admission",
            "recovery_defer_no_admission_memory",
        ),
        (
            "cold recovery replay budget",
            "recovery_defer_event_budget_memory",
        ),
        (
            "cached wake under saturation",
            "cached_wake_with_recovery_saturated_memory",
        ),
    ];

    for (profile, benchmark) in required {
        assert!(
            benchmark_exists(bench_source, benchmark),
            "phase 0009 benchmark profile `{profile}` is missing benchmark `{benchmark}`"
        );
    }
}

#[test]
fn phase_0011_postgres_provider_benchmark_profiles_have_stable_names() {
    let bench_source = include_str!("../benches/replay_core.rs");
    let required = [
        (
            "workflow task claim",
            "postgres_provider_hot_paths/workflow_task_claim_postgres",
        ),
        (
            "workflow task append commit",
            "postgres_provider_hot_paths/workflow_task_append_commit_postgres",
        ),
        (
            "bounded history streaming",
            "postgres_provider_hot_paths/history_stream_postgres",
        ),
        (
            "activity claim complete",
            "postgres_provider_hot_paths/activity_claim_complete_postgres",
        ),
        (
            "activity heartbeat",
            "postgres_provider_hot_paths/activity_heartbeat_postgres",
        ),
        (
            "signal send consume",
            "postgres_provider_hot_paths/signal_send_consume_postgres",
        ),
        (
            "timer wakeup",
            "postgres_provider_hot_paths/timer_due_scan_wakeup_postgres",
        ),
        (
            "query projection update",
            "postgres_provider_hot_paths/query_projection_update_postgres",
        ),
        (
            "query projection read",
            "postgres_provider_hot_paths/query_projection_read_postgres",
        ),
        (
            "child workflow start",
            "postgres_provider_hot_paths/child_workflow_start_parent_wakeup_postgres",
        ),
        (
            "activity map scheduling and completion",
            "postgres_provider_hot_paths/activity_map_schedule_complete_postgres",
        ),
    ];

    for (profile, benchmark) in required {
        assert!(
            benchmark_exists(bench_source, benchmark),
            "phase 0011 benchmark profile `{profile}` is missing benchmark `{benchmark}`"
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
