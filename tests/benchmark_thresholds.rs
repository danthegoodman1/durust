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

#[test]
fn phase_0012_mixed_sqlite_baseline_is_dimensioned_and_semantic() {
    let baseline: Value = serde_json::from_str(include_str!(
        "../benches/baselines/durust-mixed-sqlite.json"
    ))
    .expect("mixed SQLite benchmark baseline should be valid JSON");
    assert_eq!(baseline["backend"], "sqlite");
    assert_eq!(baseline["mode"], "mixed");
    assert_eq!(baseline["correct"], true);
    assert_eq!(baseline["sqliteLayout"], "single-file");

    let options = &baseline["options"];
    assert_eq!(options["workflows"], 1000);
    assert_eq!(options["workers"], 4);
    assert_eq!(options["shards"], 1);
    assert_eq!(options["activationConcurrency"], 1);
    assert_eq!(options["activationPrefetchLimit"], 1);
    assert_eq!(options["batch"], 32);
    assert_eq!(options["activityCompletionBatch"], 1);

    assert_eq!(baseline["completedWorkflows"], 1000);
    assert_eq!(baseline["activations"], 8000);
    assert_eq!(baseline["mixedActions"], 8000);
    assert!(
        positive_f64(&baseline, "processingWorkflowsPerSecond") >= 100.0,
        "SQLite mixed baseline should stay above the post-hardening throughput floor"
    );
    assert!(
        positive_f64(&baseline, "processingMixedActionsPerSecond") >= 800.0,
        "SQLite mixed action baseline should stay above the post-hardening throughput floor"
    );

    let counters = &baseline["counters"];
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
        assert_eq!(counters[field], 1000, "semantic counter `{field}` drifted");
    }
    assert_eq!(counters["workflowTasks"], 8000);
    assert_eq!(counters["activityTasks"], 3000);
    assert_eq!(counters["timersFired"], 1000);
    assert_eq!(counters["childWorkflowStartsDispatched"], 1000);

    let workload_source = include_str!("../src/bin/durust-benchmark-workload.rs");
    assert!(workload_source.contains("bench.workload.parent"));
    assert!(workload_source.contains("bench.workload.child"));
    assert!(workload_source.contains("bench.workload.activity"));

    let compare_source = include_str!("../src/bin/durust-benchmark-compare.rs");
    assert!(compare_source.contains("processingWorkflowsPerSecond"));
    assert!(compare_source.contains("benchmark dimensions differ"));

    assert!(workload_source.contains("postgres-write-ceiling"));
    assert!(workload_source.contains("ResourceSamplesReport"));
}

#[test]
fn phase_0012_mixed_postgres_baseline_is_dimensioned_and_semantic() {
    let baseline: Value = serde_json::from_str(include_str!(
        "../benches/baselines/durust-mixed-postgres.json"
    ))
    .expect("mixed Postgres benchmark baseline should be valid JSON");
    assert_eq!(baseline["backend"], "postgres");
    assert_eq!(baseline["mode"], "mixed");
    assert_eq!(baseline["correct"], true);

    let options = &baseline["options"];
    assert_eq!(options["workflows"], 1000);
    assert_eq!(options["workers"], 4);
    assert_eq!(options["shards"], 1);
    assert_eq!(options["activationConcurrency"], 1);
    assert_eq!(options["activationPrefetchLimit"], 1);
    assert_eq!(options["batch"], 32);
    assert_eq!(options["activityCompletionBatch"], 1);
    assert_eq!(options["postgresPoolSize"], 8);

    assert_eq!(baseline["completedWorkflows"], 1000);
    assert_eq!(baseline["mixedActions"], 8000);
    positive_f64(&baseline, "processingWorkflowsPerSecond");
    positive_f64(&baseline, "processingMixedActionsPerSecond");
    assert!(
        baseline["backendMetrics"]["workflowTaskCommitLatency"]["p95Ms"]
            .as_f64()
            .unwrap()
            > 0.0
    );
    assert!(baseline["postgresStats"]["walBytes"].as_u64().unwrap() > 0);

    let counters = &baseline["counters"];
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
        assert_eq!(counters[field], 1000, "semantic counter `{field}` drifted");
    }
    assert!(
        counters["workflowTasks"].as_u64().unwrap() <= 8000,
        "Postgres may coalesce ready events during cold replay, but must not exceed the nominal task target"
    );
    assert_eq!(counters["activityTasks"], 3000);
    assert_eq!(counters["timersFired"], 1000);
}

#[test]
fn phase_0013_mixed_postgres_sharded_baseline_is_dimensioned_and_semantic() {
    let baseline: Value = serde_json::from_str(include_str!(
        "../benches/baselines/durust-mixed-postgres-100-shards.json"
    ))
    .expect("mixed sharded Postgres benchmark baseline should be valid JSON");
    assert_eq!(baseline["backend"], "postgres");
    assert_eq!(baseline["mode"], "mixed");
    assert_eq!(baseline["correct"], true);

    let options = &baseline["options"];
    assert_eq!(options["workflows"], 1000);
    assert_eq!(options["workers"], 10);
    assert_eq!(options["shards"], 100);
    assert_eq!(options["physicalPartitions"], 16);
    assert_eq!(options["activationConcurrency"], 8);
    assert_eq!(options["activationPrefetchLimit"], 32);
    assert_eq!(options["batch"], 32);
    assert_eq!(options["activityCompletionBatch"], 32);
    assert_eq!(options["postgresPoolSize"], 24);

    assert_eq!(baseline["completedWorkflows"], 1000);
    assert_eq!(baseline["mixedActions"], 8000);
    assert!(
        positive_f64(&baseline, "processingWorkflowsPerSecond") >= 100.0,
        "sharded Postgres baseline should prove scale-out improvement over the normalized baseline"
    );
    positive_f64(&baseline, "processingMixedActionsPerSecond");
    assert!(
        baseline["backendMetrics"]["workflowTaskCommitLatency"]["samples"]
            .as_u64()
            .unwrap()
            < baseline["counters"]["workflowTasks"].as_u64().unwrap(),
        "batching should reduce workflow task commit calls below workflow task count"
    );
    assert!(baseline["postgresStats"]["walBytes"].as_u64().unwrap() > 0);
    let tx_per_action = baseline["postgresStats"]["transactionsPerMixedAction"]
        .as_f64()
        .expect("sharded Postgres baseline should report transactions per mixed action");
    assert!(
        tx_per_action > 0.0 && tx_per_action <= 3.8,
        "sharded Postgres baseline should stay below the accepted transaction budget, got {tx_per_action}"
    );
    let tx_per_workflow = baseline["postgresStats"]["transactionsPerWorkflow"]
        .as_f64()
        .expect("sharded Postgres baseline should report transactions per workflow");
    assert!(
        tx_per_workflow > 0.0 && tx_per_workflow <= 30.0,
        "sharded Postgres baseline should stay below the accepted transaction budget, got {tx_per_workflow}"
    );
    let statement_stats = &baseline["postgresStats"]["statementStats"];
    let statement_calls_per_action = statement_stats["callsPerMixedAction"]
        .as_f64()
        .expect("sharded Postgres baseline should report statement calls per mixed action");
    assert!(
        statement_calls_per_action > 0.0 && statement_calls_per_action <= 13.5,
        "sharded Postgres baseline should stay below the accepted statement budget, got {statement_calls_per_action}"
    );
    assert!(
        !statement_stats["topStatements"]
            .as_array()
            .unwrap()
            .is_empty(),
        "sharded Postgres baseline should include pg_stat_statements top statements"
    );
    assert!(
        baseline["backendMetrics"]["operations"]["workflow_change_versions"].is_null(),
        "normal complete-history workflow tasks should derive change markers without a provider metadata query"
    );
    assert!(
        baseline["backendMetrics"]["operations"]["run_due_maintenance"]["calls"]
            .as_u64()
            .unwrap()
            > 0,
        "mixed benchmark should exercise the combined timer/activity maintenance path"
    );

    let counters = &baseline["counters"];
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
        assert_eq!(counters[field], 1000, "semantic counter `{field}` drifted");
    }
    assert_eq!(counters["activityTasks"], 3000);
    assert_eq!(counters["timersFired"], 1000);
}

fn benchmark_exists(source: &str, name: &str) -> bool {
    if let Some((group, function)) = name.split_once('/') {
        source.contains(&format!("benchmark_group(\"{group}\")"))
            && source.contains(&format!("bench_function(\"{function}\""))
    } else {
        source.contains(&format!("bench_function(\"{name}\""))
    }
}

fn positive_f64(value: &Value, field: &str) -> f64 {
    let parsed = value[field]
        .as_f64()
        .unwrap_or_else(|| panic!("benchmark baseline should include numeric `{field}`"));
    assert!(
        parsed.is_finite() && parsed > 0.0,
        "benchmark baseline field `{field}` must be positive"
    );
    parsed
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
