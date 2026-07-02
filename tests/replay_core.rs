use durust::{
    ActivityName, BoxSelectBranch, ClaimActivityOptions, ClaimWorkflowTaskOptions, Client,
    CompleteActivityRequest, DurableBackend, DurableBranchExt, EventId, HistoryEventData,
    MemoryBackend, Namespace, PostgresBackend, PostgresBackendConfig, SqliteBackend, TaskQueue,
    Worker, WorkerId, WorkflowType,
};
use futures::executor::block_on;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static FLAKY_ATTEMPTS: Mutex<u32> = Mutex::new(0);
static SIDE_EFFECT_COUNTER: Mutex<u64> = Mutex::new(0);

fn postgres_url_from_env() -> Option<String> {
    env::var("DURUST_POSTGRES_URL").ok()
}

fn postgres_test_schema(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("durust_{prefix}_{}_{}", std::process::id(), millis)
}

async fn drop_postgres_schema(database_url: &str, schema: &str) {
    let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "drop schema if exists {} cascade",
            quote_postgres_identifier(schema)
        ))
        .await
        .unwrap();
    connection.abort();
}

fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn block_on_tokio<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct NumberInput {
    value: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct UnitInput {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ValuesInput {
    values: Vec<u64>,
}

fn number(value: u64) -> NumberInput {
    NumberInput { value }
}

fn unit() -> UnitInput {
    UnitInput {}
}

fn values(values: Vec<u64>) -> ValuesInput {
    ValuesInput { values }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct QueryView {
    status: String,
    value: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ContinueInput {
    remaining: u32,
    total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LargePayload {
    bytes: Vec<u8>,
}

#[durust::activity(name = "tests.double")]
async fn double(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value * 2)
}

#[durust::activity(name = "tests.map-double")]
async fn map_double(input: NumberInput) -> durust::Result<u64> {
    Ok(input.value * 2)
}

#[durust::activity(name = "tests.fail")]
async fn fail_activity(_: UnitInput) -> durust::Result<u64> {
    Err(durust::Error::Backend("boom".to_owned()))
}

#[durust::activity(name = "tests.non-retryable")]
async fn non_retryable_activity(_: UnitInput) -> durust::Result<u64> {
    Err(durust::Error::non_retryable(
        "tests.validation",
        "validation failed",
    ))
}

#[durust::activity(name = "tests.flaky")]
async fn flaky_activity(_: UnitInput) -> durust::Result<u64> {
    let mut attempts = FLAKY_ATTEMPTS.lock().unwrap();
    *attempts += 1;
    if *attempts == 1 {
        Err(durust::Error::Backend("transient".to_owned()))
    } else {
        Ok(7)
    }
}

#[durust::activity(name = "tests.heartbeat")]
async fn heartbeat_activity_test(input: NumberInput) -> durust::Result<u64> {
    durust::heartbeat_activity().await?;
    durust::heartbeat_activity().await?;
    Ok(input.value * 2)
}

#[durust::activity(name = "tests.large-payload-result")]
async fn large_payload_result(_: UnitInput) -> durust::Result<LargePayload> {
    Ok(LargePayload {
        bytes: vec![7; 64 * 1024],
    })
}

#[durust::activity(name = "tests.version-a")]
async fn version_activity_a(_: UnitInput) -> durust::Result<String> {
    Ok("a".to_owned())
}

#[durust::activity(name = "tests.version-b")]
async fn version_activity_b(_: UnitInput) -> durust::Result<String> {
    Ok("b".to_owned())
}

#[durust::workflow(name = "tests.double-plus-one", version = 1)]
async fn double_plus_one(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    let doubled = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .await?;
    Ok(doubled + 1)
}

#[durust::workflow(name = "tests.version-branch", version = 1)]
async fn version_original(_: UnitInput) -> durust::Result<String> {
    durust::call_activity!(version_activity_a(UnitInput {}))
        .task_queue("activities")
        .await
}

#[durust::workflow(name = "tests.version-branch", version = 1)]
async fn version_patched(_: UnitInput) -> durust::Result<String> {
    if durust::patched("replace-a-with-b")? {
        durust::call_activity!(version_activity_b(UnitInput {}))
            .task_queue("activities")
            .await
    } else {
        durust::call_activity!(version_activity_a(UnitInput {}))
            .task_queue("activities")
            .await
    }
}

#[durust::workflow(name = "tests.version-branch", version = 1)]
async fn version_min_two(_: UnitInput) -> durust::Result<String> {
    let _ = durust::get_version("replace-a-with-b", 2, 2)?;
    durust::call_activity!(version_activity_b(UnitInput {}))
        .task_queue("activities")
        .await
}

#[durust::workflow(name = "tests.version-branch", version = 1)]
async fn version_deprecated(_: UnitInput) -> durust::Result<String> {
    durust::deprecate_patch("replace-a-with-b")?;
    durust::call_activity!(version_activity_b(UnitInput {}))
        .task_queue("activities")
        .await
}

#[durust::workflow(name = "tests.version-branch", version = 1)]
async fn version_removed(_: UnitInput) -> durust::Result<String> {
    durust::call_activity!(version_activity_b(UnitInput {}))
        .task_queue("activities")
        .await
}

#[durust::workflow(name = "tests.child-double", version = 1)]
async fn child_double_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    Ok(input * 2)
}

#[durust::workflow(name = "tests.child-triple", version = 1)]
async fn child_triple_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    Ok(input * 3)
}

#[durust::workflow(name = "tests.child-spawn-wait", version = 1)]
async fn child_spawn_wait_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    let child = durust::child!(child_double_workflow(number(input)))
        .workflow_id(format!("wf/child-spawn-wait/{input}"))
        .spawn()
        .await?;
    child.result().await
}

#[durust::workflow(name = "tests.postgres-inline-child-signal-timer", version = 1)]
async fn postgres_inline_child_signal_timer(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let child = durust::child!(child_double_workflow(number(input)))
        .workflow_id(format!("wf/postgres-inline-child-signal-timer/{input}"))
        .spawn()
        .await?;
    let child_value = child.result().await?;
    let signal_value = durust::signal::<String>("ready").await?;
    durust::sleep(Duration::ZERO).await?;
    Ok(format!("{child_value}:{signal_value}"))
}

#[durust::workflow(name = "tests.child-spawn-abandon", version = 1)]
async fn child_spawn_abandon_workflow(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let child = durust::child!(child_double_workflow(number(input)))
        .workflow_id(format!("wf/child-spawn-abandon/{input}"))
        .parent_close_policy(durust::ParentClosePolicy::Abandon)
        .spawn()
        .await?;
    Ok(child.run_id().0.clone())
}

#[durust::workflow(name = "tests.child-spawn-cancel", version = 1)]
async fn child_spawn_cancel_workflow(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let child = durust::child!(child_double_workflow(number(input)))
        .workflow_id(format!("wf/child-spawn-cancel/{input}"))
        .parent_close_policy(durust::ParentClosePolicy::Cancel)
        .spawn()
        .await?;
    Ok(child.run_id().0.clone())
}

#[durust::workflow(name = "tests.select-child-result", version = 1)]
async fn select_child_result_workflow(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let child = durust::child!(child_double_workflow(number(input)))
        .workflow_id(format!("wf/select-child-result/{input}"))
        .spawn()
        .await?;
    let outcome = durust::select! {
        result = child.result() => {
            format!("child:{}", result?)
        }
        timer = durust::sleep(Duration::from_secs(60)) => {
            timer?;
            "timer".to_owned()
        }
    };
    Ok(outcome)
}

#[durust::workflow(name = "tests.select-timer-before-child-result", version = 1)]
async fn select_timer_before_child_result_workflow(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let child = durust::child!(child_double_workflow(number(input)))
        .workflow_id(format!("wf/select-timer-before-child-result/{input}"))
        .parent_close_policy(durust::ParentClosePolicy::Abandon)
        .spawn()
        .await?;
    let outcome = durust::select! {
        result = child.result() => {
            format!("child:{}", result?)
        }
        timer = durust::sleep(Duration::ZERO) => {
            timer?;
            format!("timer:{}", child.run_id().0)
        }
    };
    Ok(outcome)
}

#[durust::workflow(name = "tests.activity-spawn-await-later", version = 1)]
async fn activity_spawn_await_later_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    let first = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .spawn()
        .await?;
    let _second = durust::call_activity!(double(NumberInput { value: input + 1 }))
        .task_queue("activities")
        .spawn()
        .await?;
    first.result().await
}

#[durust::workflow(name = "tests.sleep-before-large-activity-result", version = 1)]
async fn sleep_before_large_activity_result(_: UnitInput) -> durust::Result<usize> {
    let handle = durust::call_activity!(large_payload_result(UnitInput {}))
        .task_queue("activities")
        .spawn()
        .await?;
    durust::sleep(Duration::from_secs(1)).await?;
    let payload = handle.result().await?;
    Ok(payload.bytes.len())
}

#[durust::workflow(name = "tests.side-effect-then-sleep", version = 1)]
async fn side_effect_then_sleep_workflow(_: UnitInput) -> durust::Result<String> {
    let value = durust::side_effect("make-id", || {
        let mut counter = SIDE_EFFECT_COUNTER.lock().unwrap();
        *counter += 1;
        format!("side-effect-{}", *counter)
    })
    .await?;
    durust::sleep(Duration::from_secs(1)).await?;
    Ok(value)
}

#[durust::workflow(name = "tests.oversized-side-effect", version = 1)]
async fn oversized_side_effect_workflow(_: UnitInput) -> durust::Result<()> {
    let _: String = durust::side_effect("too-large", || {
        "x".repeat(durust::MAX_SIDE_EFFECT_PAYLOAD_BYTES + 1)
    })
    .await?;
    Ok(())
}

#[durust::workflow(name = "tests.select-all-activity-handles", version = 1)]
async fn select_all_activity_handles_workflow(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let mut branches = Vec::new();
    for offset in 0..3_u64 {
        let handle = durust::call_activity!(double(NumberInput {
            value: input + offset,
        }))
        .task_queue("activities")
        .spawn()
        .await?;
        branches.push(handle.result().boxed());
    }
    let winner = durust::select_all(branches).await?;
    Ok(format!("{}:{}", winner.branch_index, winner.value))
}

#[durust::workflow(name = "tests.select-all-mixed-branches", version = 1)]
async fn select_all_mixed_branches_workflow(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let activity = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .spawn()
        .await?;
    let child = durust::child!(child_double_workflow(number(input + 10)))
        .workflow_id(format!("wf/select-all-mixed-child/{input}"))
        .parent_close_policy(durust::ParentClosePolicy::Abandon)
        .spawn()
        .await?;

    let branches: Vec<BoxSelectBranch<String>> = vec![
        activity
            .result()
            .map_ok(|value| format!("activity:{value}"))
            .boxed(),
        child
            .result()
            .map_ok(|value| format!("child:{value}"))
            .boxed(),
        durust::sleep(Duration::ZERO)
            .map_ok(|_| "timer".to_owned())
            .boxed(),
    ];
    let winner = durust::select_all(branches).await?;
    Ok(format!("{}:{}", winner.branch_index, winner.value))
}

#[durust::workflow(name = "tests.join-all-activity-handles", version = 1)]
async fn join_all_activity_handles_workflow(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let mut branches = Vec::new();
    for offset in 0..3_u64 {
        let handle = durust::call_activity!(double(NumberInput {
            value: input + offset,
        }))
        .task_queue("activities")
        .spawn()
        .await?;
        branches.push(handle.result());
    }
    let results = durust::join_all(branches).await?;
    Ok(results
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(","))
}

#[durust::workflow(name = "tests.join-all-mixed-branches", version = 1)]
async fn join_all_mixed_branches_workflow(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let activity = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .spawn()
        .await?;
    let branches: Vec<BoxSelectBranch<String>> = vec![
        activity
            .result()
            .map_ok(|value| format!("activity:{value}"))
            .boxed(),
        durust::sleep(Duration::ZERO)
            .map_ok(|_| "timer".to_owned())
            .boxed(),
    ];
    let results = durust::join_all(branches).await?;
    Ok(results.join("|"))
}

#[durust::workflow(name = "tests.child-first-select-then-timer", version = 1)]
async fn child_first_select_then_timer_workflow(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let activity = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .spawn()
        .await?;
    let child = durust::child!(child_double_workflow(number(input + 10)))
        .workflow_id(format!("wf/child-first-select/{input}"))
        .spawn()
        .await?;
    let branches: Vec<BoxSelectBranch<String>> = vec![
        child
            .result()
            .map_ok(|value| format!("child:{value}"))
            .boxed(),
        activity
            .result()
            .map_ok(|value| format!("activity:{value}"))
            .boxed(),
    ];
    let winner = durust::select_all(branches).await?;
    durust::sleep(Duration::ZERO).await?;
    Ok(format!("{}:{}", winner.branch_index, winner.value))
}

#[durust::workflow(name = "tests.timer-first-select-then-timer", version = 1)]
async fn timer_first_select_then_timer_workflow(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let activity = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .spawn()
        .await?;
    let branches: Vec<BoxSelectBranch<String>> = vec![
        durust::sleep(Duration::ZERO)
            .map_ok(|_| "timer".to_owned())
            .boxed(),
        activity
            .result()
            .map_ok(|value| format!("activity:{value}"))
            .boxed(),
    ];
    let winner = durust::select_all(branches).await?;
    durust::sleep(Duration::ZERO).await?;
    Ok(format!("{}:{}", winner.branch_index, winner.value))
}

#[durust::workflow(name = "tests.join-two-activities", version = 1)]
async fn join_two_activities(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    let (left, right) = durust::join!(
        durust::call_activity!(double(NumberInput { value: input })).task_queue("activities"),
        durust::call_activity!(double(NumberInput { value: input + 1 })).task_queue("activities"),
    )
    .await?;
    Ok(left + right)
}

#[durust::workflow(name = "tests.join-four-activities", version = 1)]
async fn join_four_activities(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    let (first, second, third, fourth) = durust::join!(
        durust::call_activity!(double(NumberInput { value: input })).task_queue("activities"),
        durust::call_activity!(double(NumberInput { value: input + 1 })).task_queue("activities"),
        durust::call_activity!(double(NumberInput { value: input + 2 })).task_queue("activities"),
        durust::call_activity!(double(NumberInput { value: input + 3 })).task_queue("activities"),
    )
    .await?;
    Ok(first + second + third + fourth)
}

#[durust::workflow(name = "tests.sequential-two-activities", version = 1)]
async fn sequential_two_activities(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    let first = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .await?;
    let second = durust::call_activity!(double(NumberInput { value: input + 1 }))
        .task_queue("activities")
        .await?;
    Ok(first + second)
}

#[durust::workflow(name = "tests.join-signal-timer", version = 1)]
async fn join_signal_timer(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let (signal, _) = durust::join!(
        durust::signal::<String>("ready"),
        durust::sleep(Duration::from_millis(input)),
    )
    .await?;
    Ok(signal)
}

#[durust::workflow(name = "tests.join-signal-timer-then-timer", version = 1)]
async fn join_signal_timer_then_timer(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let (signal, _) = durust::join!(
        durust::signal::<String>("ready"),
        durust::sleep(Duration::from_millis(input)),
    )
    .await?;
    durust::sleep(Duration::ZERO).await?;
    Ok(signal)
}

#[durust::workflow(name = "tests.select-signal-timer", version = 1)]
async fn select_signal_timer(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let outcome = durust::select! {
        signal = durust::signal::<String>("ready") => {
            format!("signal:{}", signal?)
        }
        timer = durust::sleep(Duration::from_millis(input)) => {
            timer?;
            "timer".to_owned()
        }
    };
    Ok(outcome)
}

#[durust::workflow(name = "tests.select-activity-timer", version = 1)]
async fn select_activity_timer(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    let outcome = durust::select! {
        activity = durust::call_activity!(double(NumberInput { value: input })).task_queue("activities") => {
            activity?
        }
        timer = durust::sleep(Duration::from_millis(10)) => {
            timer?;
            0
        }
    };
    Ok(outcome)
}

#[durust::workflow(name = "tests.select-timer-before-activity", version = 1)]
async fn select_timer_before_activity(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let outcome = durust::select! {
        activity = durust::call_activity!(double(NumberInput { value: input })).task_queue("activities") => {
            format!("activity:{}", activity?)
        }
        timer = durust::sleep(Duration::from_millis(10)) => {
            timer?;
            "timer".to_owned()
        }
    };
    Ok(outcome)
}

#[durust::workflow(name = "tests.select-same-tick-timers", version = 1)]
async fn select_same_tick_timers(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let outcome = durust::select! {
        left = durust::sleep(Duration::from_millis(input)) => {
            left?;
            "left".to_owned()
        }
        right = durust::sleep(Duration::from_millis(input)) => {
            right?;
            "right".to_owned()
        }
    };
    Ok(outcome)
}

#[durust::workflow(name = "tests.select-fourth-signal", version = 1)]
async fn select_fourth_signal(_: UnitInput) -> durust::Result<String> {
    let outcome = durust::select! {
        first = durust::sleep(Duration::from_secs(1)) => {
            first?;
            "first".to_owned()
        }
        second = durust::sleep(Duration::from_secs(2)) => {
            second?;
            "second".to_owned()
        }
        third = durust::sleep(Duration::from_secs(3)) => {
            third?;
            "third".to_owned()
        }
        signal = durust::signal::<String>("ready") => {
            format!("signal:{}", signal?)
        }
    };
    Ok(outcome)
}

#[durust::workflow(name = "tests.select-two-signals", version = 1)]
async fn select_two_signals(_: UnitInput) -> durust::Result<String> {
    let outcome = durust::select! {
        left = durust::signal::<String>("left") => {
            format!("left:{}", left?)
        }
        right = durust::signal::<String>("right") => {
            format!("right:{}", right?)
        }
    };
    Ok(outcome)
}

#[durust::workflow(name = "tests.select-reorder", version = 1)]
async fn select_then_wait(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let first = durust::select! {
        signal = durust::signal::<String>("ready") => {
            format!("signal:{}", signal?)
        }
        timer = durust::sleep(Duration::from_millis(input)) => {
            timer?;
            "timer".to_owned()
        }
    };
    let after = durust::signal::<String>("after").await?;
    Ok(format!("{first}:{after}"))
}

#[durust::workflow(name = "tests.select-reorder", version = 1)]
async fn select_then_wait_reordered(input: NumberInput) -> durust::Result<String> {
    let input = input.value;
    let first = durust::select! {
        timer = durust::sleep(Duration::from_millis(input)) => {
            timer?;
            "timer".to_owned()
        }
        signal = durust::signal::<String>("ready") => {
            format!("signal:{}", signal?)
        }
    };
    let after = durust::signal::<String>("after").await?;
    Ok(format!("{first}:{after}"))
}

#[durust::workflow(name = "tests.failing-activity", version = 1)]
async fn failing_activity_workflow(_: UnitInput) -> durust::Result<u64> {
    durust::call_activity!(fail_activity(UnitInput {})).await
}

#[durust::workflow(name = "tests.retry-activity", version = 1)]
async fn retry_activity_workflow(_: UnitInput) -> durust::Result<u64> {
    durust::call_activity!(flaky_activity(UnitInput {}))
        .retry(durust::RetryPolicy::exponential().max_attempts(2))
        .await
}

#[durust::workflow(name = "tests.non-retryable-activity", version = 1)]
async fn non_retryable_activity_workflow(_: UnitInput) -> durust::Result<u64> {
    durust::call_activity!(non_retryable_activity(UnitInput {}))
        .retry(durust::RetryPolicy::exponential().max_attempts(5))
        .await
}

#[durust::workflow(name = "tests.timeout-activity", version = 1)]
async fn timeout_activity_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .timeout(Duration::from_millis(10))
        .await
}

#[durust::workflow(name = "tests.heartbeat-activity", version = 1)]
async fn heartbeat_activity_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    durust::call_activity!(heartbeat_activity_test(NumberInput { value: input }))
        .task_queue("activities")
        .heartbeat_timeout(Duration::from_secs(30))
        .await
}

#[durust::workflow(name = "tests.double-plus-one", version = 1)]
async fn double_plus_one_changed(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    let doubled = durust::call_activity!(double(NumberInput { value: input + 1 }))
        .task_queue("activities")
        .await?;
    Ok(doubled + 1)
}

#[durust::workflow(name = "tests.default-activity-options", version = 1)]
async fn default_activity_options_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    durust::set_default_activity_options(
        durust::ActivityOptions::new()
            .task_queue("preferred-activities")
            .retry(durust::RetryPolicy::exponential().max_attempts(5)),
    );
    durust::call_activity!(double(NumberInput { value: input })).await
}

#[durust::workflow(name = "tests.override-activity-options", version = 1)]
async fn override_activity_options_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    durust::set_default_activity_options(
        durust::ActivityOptions::new()
            .task_queue("default-activities")
            .retry(durust::RetryPolicy::exponential().max_attempts(5)),
    );
    durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("override-activities")
        .retry(durust::RetryPolicy::none())
        .await
}

#[durust::workflow(name = "tests.cached-default-activity-options", version = 1)]
async fn cached_default_activity_options_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    durust::set_default_activity_options(
        durust::ActivityOptions::new()
            .task_queue("sticky-activities")
            .retry(durust::RetryPolicy::exponential().max_attempts(7)),
    );
    let first = durust::call_activity!(double(NumberInput { value: input })).await?;
    durust::call_activity!(double(NumberInput { value: first })).await
}

#[durust::workflow(name = "tests.query-projection", version = 1, query_state = QueryView)]
async fn query_projection_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    durust::publish(&QueryView {
        status: "started".to_owned(),
        value: input,
    })?;
    let signal = durust::signal::<String>("advance").await?;
    durust::publish(&QueryView {
        status: signal,
        value: input + 1,
    })?;
    Ok(input + 1)
}

#[durust::query(workflow = query_projection_workflow)]
fn query_status(view: &QueryView) -> String {
    view.status.clone()
}

#[durust::workflow(name = "tests.provider-json-codec", version = 1, query_state = QueryView)]
async fn provider_json_codec_workflow(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    durust::publish(&QueryView {
        status: "started".to_owned(),
        value: input,
    })?;
    let doubled = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("json-activities")
        .await?;
    let signal = durust::signal::<String>("advance").await?;
    durust::publish(&QueryView {
        status: signal,
        value: doubled,
    })?;
    Ok(doubled + 1)
}

#[durust::workflow(name = "tests.continue-as-new", version = 1)]
async fn continue_as_new_workflow(input: ContinueInput) -> durust::Result<u64> {
    if input.remaining > 0 {
        return durust::continue_as_new(ContinueInput {
            remaining: input.remaining - 1,
            total: input.total + 1,
        });
    }
    Ok(input.total)
}

#[durust::workflow(name = "tests.continue-query", version = 1, query_state = QueryView)]
async fn continue_query_workflow(input: ContinueInput) -> durust::Result<u64> {
    if input.remaining > 0 {
        durust::publish(&QueryView {
            status: "continuing".to_owned(),
            value: input.total,
        })?;
        return durust::continue_as_new(ContinueInput {
            remaining: input.remaining - 1,
            total: input.total + 1,
        });
    }
    durust::publish(&QueryView {
        status: "done".to_owned(),
        value: input.total,
    })?;
    Ok(input.total)
}

#[durust::workflow(name = "tests.continued-child", version = 1)]
async fn continued_child_workflow(input: ContinueInput) -> durust::Result<u64> {
    if input.remaining > 0 {
        return durust::continue_as_new(ContinueInput {
            remaining: input.remaining - 1,
            total: input.total + 1,
        });
    }
    Ok(input.total)
}

#[durust::workflow(name = "tests.parent-waits-continued-child", version = 1)]
async fn parent_waits_continued_child(input: ContinueInput) -> durust::Result<u64> {
    let child = durust::child!(continued_child_workflow(input))
        .workflow_id("wf/continued-child")
        .spawn()
        .await?;
    child.result().await
}

#[durust::workflow(name = "tests.sleep-then-return", version = 1)]
async fn sleep_then_return(input: NumberInput) -> durust::Result<u64> {
    let input = input.value;
    durust::sleep(Duration::from_millis(input)).await?;
    Ok(input + 1)
}

#[durust::workflow(name = "tests.await-signal", version = 1)]
async fn await_signal(_: UnitInput) -> durust::Result<String> {
    durust::signal::<String>("ready").await
}

#[durust::workflow(name = "tests.activity-map-sum", version = 1)]
async fn activity_map_sum(input: ValuesInput) -> durust::Result<u64> {
    let input_manifest =
        durust::activity_map_manifest(input.values.into_iter().map(|value| NumberInput { value }))?;
    let mapped = durust::activity_map(map_double)
        .task_queue("map-activities")
        .input_manifest(input_manifest)
        .max_in_flight(2)
        .result_manifest("doubled")
        .spawn()
        .await?;
    let result_manifest = mapped.result_manifest().await?;
    let result_refs = durust::decode_activity_map_result_refs(&result_manifest)?;
    result_refs.iter().try_fold(0_u64, |sum, payload| {
        Ok(sum + durust::decode_payload::<u64>(payload)?)
    })
}

macro_rules! child_workflow_map_sum_body {
    (
        $input:expr,
        $workflow:ty,
        $workflow_id_prefix:expr,
        $task_queue:expr,
        $max_in_flight:expr,
        $parent_close_policy:expr,
        $failure_mode:expr,
        $input_offset:expr $(,)?
    ) => {{
        let input_manifest =
            durust::child_workflow_map_manifest($input.into_iter().map(|value| NumberInput {
                value: value + $input_offset,
            }))?;
        let mapped = durust::child_workflow_map::<$workflow>()
            .task_queue($task_queue)
            .workflow_id_prefix($workflow_id_prefix)
            .input_manifest(input_manifest)
            .max_in_flight($max_in_flight)
            .result_manifest("child-doubled")
            .parent_close_policy($parent_close_policy)
            .failure_mode($failure_mode)
            .spawn()
            .await?;
        let result_manifest = mapped.result_manifest().await?;
        let result_refs = durust::decode_child_workflow_map_success_refs(&result_manifest)?;
        result_refs.iter().try_fold(0_u64, |sum, payload| {
            Ok(sum + durust::decode_payload::<u64>(payload)?)
        })
    }};
}

#[durust::workflow(name = "tests.child-workflow-map-sum", version = 1)]
async fn child_workflow_map_sum(input: ValuesInput) -> durust::Result<u64> {
    let input = input.values;
    child_workflow_map_sum_body!(
        input,
        child_double_workflow,
        "wf/child-workflow-map-sum/item",
        "workflows",
        2,
        durust::ParentClosePolicy::Cancel,
        durust::ChildWorkflowMapFailureMode::FailFast,
        0,
    )
}

#[durust::workflow(name = "tests.child-workflow-map-sum", version = 1)]
async fn child_workflow_map_sum_changed_child_type(input: ValuesInput) -> durust::Result<u64> {
    let input = input.values;
    child_workflow_map_sum_body!(
        input,
        child_triple_workflow,
        "wf/child-workflow-map-sum/item",
        "workflows",
        2,
        durust::ParentClosePolicy::Cancel,
        durust::ChildWorkflowMapFailureMode::FailFast,
        0,
    )
}

#[durust::workflow(name = "tests.child-workflow-map-sum", version = 1)]
async fn child_workflow_map_sum_changed_input_manifest(input: ValuesInput) -> durust::Result<u64> {
    let input = input.values;
    child_workflow_map_sum_body!(
        input,
        child_double_workflow,
        "wf/child-workflow-map-sum/item",
        "workflows",
        2,
        durust::ParentClosePolicy::Cancel,
        durust::ChildWorkflowMapFailureMode::FailFast,
        1,
    )
}

#[durust::workflow(name = "tests.child-workflow-map-sum", version = 1)]
async fn child_workflow_map_sum_changed_prefix(input: ValuesInput) -> durust::Result<u64> {
    let input = input.values;
    child_workflow_map_sum_body!(
        input,
        child_double_workflow,
        "wf/child-workflow-map-sum/changed-item",
        "workflows",
        2,
        durust::ParentClosePolicy::Cancel,
        durust::ChildWorkflowMapFailureMode::FailFast,
        0,
    )
}

#[durust::workflow(name = "tests.child-workflow-map-sum", version = 1)]
async fn child_workflow_map_sum_changed_task_queue(input: ValuesInput) -> durust::Result<u64> {
    let input = input.values;
    child_workflow_map_sum_body!(
        input,
        child_double_workflow,
        "wf/child-workflow-map-sum/item",
        "other-workflows",
        2,
        durust::ParentClosePolicy::Cancel,
        durust::ChildWorkflowMapFailureMode::FailFast,
        0,
    )
}

#[durust::workflow(name = "tests.child-workflow-map-sum", version = 1)]
async fn child_workflow_map_sum_changed_max_in_flight(input: ValuesInput) -> durust::Result<u64> {
    let input = input.values;
    child_workflow_map_sum_body!(
        input,
        child_double_workflow,
        "wf/child-workflow-map-sum/item",
        "workflows",
        3,
        durust::ParentClosePolicy::Cancel,
        durust::ChildWorkflowMapFailureMode::FailFast,
        0,
    )
}

#[durust::workflow(name = "tests.child-workflow-map-sum", version = 1)]
async fn child_workflow_map_sum_changed_parent_close_policy(
    input: ValuesInput,
) -> durust::Result<u64> {
    let input = input.values;
    child_workflow_map_sum_body!(
        input,
        child_double_workflow,
        "wf/child-workflow-map-sum/item",
        "workflows",
        2,
        durust::ParentClosePolicy::Abandon,
        durust::ChildWorkflowMapFailureMode::FailFast,
        0,
    )
}

#[durust::workflow(name = "tests.child-workflow-map-sum", version = 1)]
async fn child_workflow_map_sum_changed_failure_mode(input: ValuesInput) -> durust::Result<u64> {
    let input = input.values;
    child_workflow_map_sum_body!(
        input,
        child_double_workflow,
        "wf/child-workflow-map-sum/item",
        "workflows",
        2,
        durust::ParentClosePolicy::Cancel,
        durust::ChildWorkflowMapFailureMode::CollectAll,
        0,
    )
}

// The reproduced replay bug: after the spawned activity completes before the
// first timer fires, the replay chunk is [ActivityCompleted, TimerFired] and
// the second sleep must schedule past the unconsumed completion at the cursor.
#[durust::workflow(name = "tests.spawn-sleep-sleep", version = 1)]
async fn spawn_sleep_sleep_workflow(input: NumberInput) -> durust::Result<u64> {
    let value = input.value;
    let handle = durust::call_activity!(double(NumberInput { value }))
        .task_queue("activities")
        .spawn()
        .await?;
    durust::sleep(Duration::from_secs(1)).await?;
    durust::sleep(Duration::from_secs(1)).await?;
    handle.result().await
}

#[durust::workflow(name = "tests.spawn-sleep-then-activity", version = 1)]
async fn spawn_sleep_then_activity_workflow(input: NumberInput) -> durust::Result<u64> {
    let value = input.value;
    let handle = durust::call_activity!(double(NumberInput { value }))
        .task_queue("activities")
        .spawn()
        .await?;
    durust::sleep(Duration::from_secs(1)).await?;
    let second = durust::call_activity!(double(NumberInput { value: value + 1 }))
        .task_queue("activities")
        .await?;
    let first = handle.result().await?;
    Ok(first + second)
}

#[durust::workflow(name = "tests.spawn-sleep-then-side-effect", version = 1)]
async fn spawn_sleep_then_side_effect_workflow(input: NumberInput) -> durust::Result<String> {
    let value = input.value;
    let handle = durust::call_activity!(double(NumberInput { value }))
        .task_queue("activities")
        .spawn()
        .await?;
    durust::sleep(Duration::from_secs(1)).await?;
    let tag: String = durust::side_effect("post-sleep-tag", || "tagged".to_owned()).await?;
    let first = handle.result().await?;
    Ok(format!("{tag}:{first}"))
}

#[durust::workflow(name = "tests.spawn-sleep-then-version", version = 1)]
async fn spawn_sleep_then_version_workflow(input: NumberInput) -> durust::Result<u64> {
    let value = input.value;
    let handle = durust::call_activity!(double(NumberInput { value }))
        .task_queue("activities")
        .spawn()
        .await?;
    durust::sleep(Duration::from_secs(1)).await?;
    let version = durust::get_version("post-sleep-change", 1, 1)?;
    durust::sleep(Duration::from_secs(1)).await?;
    let first = handle.result().await?;
    Ok(first + version as u64)
}

#[durust::workflow(name = "tests.spawn-sleep-then-child", version = 1)]
async fn spawn_sleep_then_child_workflow(input: NumberInput) -> durust::Result<u64> {
    let value = input.value;
    let handle = durust::call_activity!(double(NumberInput { value }))
        .task_queue("activities")
        .spawn()
        .await?;
    durust::sleep(Duration::from_secs(1)).await?;
    let child = durust::child!(child_double_workflow(number(value + 1)))
        .workflow_id(format!("wf/spawn-sleep-then-child/{value}"))
        .spawn()
        .await?;
    let child_result = child.result().await?;
    let first = handle.result().await?;
    Ok(first + child_result)
}

#[durust::workflow(name = "tests.select-two-signals-then-signal", version = 1)]
async fn select_two_signals_then_signal_workflow(_: UnitInput) -> durust::Result<String> {
    let first = durust::select! {
        left = durust::signal::<String>("left") => {
            format!("left:{}", left?)
        }
        right = durust::signal::<String>("right") => {
            format!("right:{}", right?)
        }
    };
    let after = durust::signal::<String>("after").await?;
    Ok(format!("{first}:{after}"))
}

#[durust::workflow(name = "tests.same-signal-twice", version = 1)]
async fn same_signal_twice_workflow(_: UnitInput) -> durust::Result<String> {
    let first = durust::signal::<String>("gate").await?;
    let second = durust::signal::<String>("gate").await?;
    Ok(format!("{first}:{second}"))
}

#[durust::workflow(name = "tests.same-signal-join", version = 1)]
async fn same_signal_join_workflow(_: UnitInput) -> durust::Result<String> {
    let (first, second) = durust::join!(
        durust::signal::<String>("gate"),
        durust::signal::<String>("gate"),
    )
    .await?;
    Ok(format!("{first}:{second}"))
}

#[durust::workflow(name = "tests.signal-fingerprint", version = 1)]
async fn signal_gate_then_after_workflow(_: UnitInput) -> durust::Result<String> {
    let gate = durust::signal::<String>("gate").await?;
    let after = durust::signal::<String>("after").await?;
    Ok(format!("{gate}:{after}"))
}

#[durust::workflow(name = "tests.signal-fingerprint", version = 1)]
async fn signal_door_then_after_workflow(_: UnitInput) -> durust::Result<String> {
    let door = durust::signal::<String>("door").await?;
    let after = durust::signal::<String>("after").await?;
    Ok(format!("{door}:{after}"))
}

#[test]
fn simple_workflow_schedules_activity_and_completes_from_cache() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/simple", "workflows", number(20))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("worker-a")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        assert!(worker.run_activity_once().await.unwrap());
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(
            history[0].data,
            HistoryEventData::WorkflowStarted { .. }
        ));
        let HistoryEventData::ActivityScheduled(scheduled) = &history[1].data else {
            panic!("workflow did not schedule activity");
        };
        assert_eq!(scheduled.heartbeat_timeout, None);
        assert!(matches!(
            history[2].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 41);
    });
}

// Exercises the concurrent/batched workflow-task fork. With prefetch and commit
// batching above one, `run_until_idle` drives `run_workflow_batch_once`, which
// claims, prepares, and batch-commits several runs' tasks together rather than
// taking the single-claim shortcut. Both callers now share one prepare/commit
// path, so this guards the previously untested batched fork against regressions.
#[test]
fn batched_workflow_tasks_commit_multiple_runs_together() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let mut run_ids = Vec::new();
        for index in 0..3u32 {
            let run_id = client
                .start_workflow::<double_plus_one>(
                    format!("wf/batch-{index}"),
                    "workflows",
                    number(20),
                )
                .await
                .unwrap();
            run_ids.push(run_id);
        }

        let mut worker = Worker::builder(backend.clone())
            .worker_id("batch-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .max_concurrent_workflow_tasks(4)
            .workflow_task_prefetch_limit(4)
            .workflow_task_commit_batch_size(4)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();

        // The first batch must claim, prepare, and commit all three runs' schedule
        // tasks together. The single-claim fallback (taken only when the effective
        // limit collapses to one) could commit at most one per call, so committing 3
        // here proves the batched fork actually ran.
        let scheduled = worker.run_workflow_batch_once().await.unwrap();
        assert_eq!(
            scheduled, 3,
            "all three schedule tasks should commit in one batch"
        );
        let stats = worker.run_until_idle().await.unwrap();
        // The first batch plus the three completion tasks account for every commit.
        assert_eq!(scheduled + stats.workflow_tasks, 6);

        for run_id in &run_ids {
            let history = stream_all(&backend, run_id).await;
            let HistoryEventData::WorkflowCompleted { result } = &history[history.len() - 1].data
            else {
                panic!("batched workflow run did not complete");
            };
            assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 41);
        }
    });
}

#[test]
fn replay_hydrates_large_activity_result_only_when_workflow_observes_it() {
    block_on(async {
        let inner = MemoryBackend::new();
        let blob_store = CountingBlobStore::default();
        let backend = durust::PayloadBackend::with_payload_storage(
            inner.clone(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1024),
        );
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<sleep_before_large_activity_result>(
                "wf/lazy-large-activity-result",
                "workflows",
                unit(),
            )
            .await
            .unwrap();

        let mut worker = lazy_payload_worker(backend.clone(), "lazy-worker-a");
        assert!(worker.run_workflow_once().await.unwrap());
        assert!(worker.run_activity_once().await.unwrap());
        assert_eq!(
            blob_store.get_count(),
            0,
            "activity completion upload should not read the blob"
        );

        let mut replay_before_timer = lazy_payload_worker(backend.clone(), "lazy-worker-b");
        assert!(replay_before_timer.run_workflow_once().await.unwrap());
        assert_eq!(
            blob_store.get_count(),
            0,
            "cold replay streamed the completed activity result but the workflow was still blocked on the timer"
        );

        inner.advance_time(Duration::from_secs(1));
        assert_eq!(replay_before_timer.run_timers_once().await.unwrap(), 1);

        let mut replay_after_timer = lazy_payload_worker(backend.clone(), "lazy-worker-c");
        assert!(replay_after_timer.run_workflow_once().await.unwrap());
        assert_eq!(
            blob_store.get_count(),
            1,
            "the activity result should hydrate exactly when handle.result() observes it"
        );

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } = &history[5].data else {
            panic!("workflow did not complete after timer and activity result");
        };
        assert_eq!(durust::decode_payload::<usize>(result).unwrap(), 64 * 1024);
    });
}

#[test]
fn side_effect_replays_recorded_marker_without_rerunning_closure() {
    block_on(async {
        *SIDE_EFFECT_COUNTER.lock().unwrap() = 0;
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<side_effect_then_sleep_workflow>(
                "wf/side-effect-replay",
                "workflows",
                unit(),
            )
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .worker_id("side-effect-first")
            .workflow_task_queue("workflows")
            .register_workflow(side_effect_then_sleep_workflow)
            .build();

        assert!(first_worker.run_workflow_once().await.unwrap());
        assert_eq!(*SIDE_EFFECT_COUNTER.lock().unwrap(), 1);

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::SideEffectMarker(marker) = &history[1].data else {
            panic!("workflow did not record a side effect marker");
        };
        assert!(matches!(marker.value, durust::PayloadRef::Inline { .. }));
        assert_eq!(backend.payload_blob_count(), 0);

        backend.advance_time(Duration::from_secs(1));
        let mut recovered_worker = Worker::builder(backend.clone())
            .worker_id("side-effect-recovered")
            .workflow_task_queue("workflows")
            .register_workflow(side_effect_then_sleep_workflow)
            .build();
        assert_eq!(recovered_worker.run_timers_once().await.unwrap(), 1);
        assert!(recovered_worker.run_workflow_once().await.unwrap());
        assert_eq!(*SIDE_EFFECT_COUNTER.lock().unwrap(), 1);

        let history = stream_all(&backend, &run_id).await;
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
        );
        let HistoryEventData::WorkflowCompleted { result } = &history[4].data else {
            panic!("side effect workflow did not complete after replay");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "side-effect-1"
        );
    });
}

#[test]
fn oversized_side_effect_fails_without_recording_marker() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<oversized_side_effect_workflow>(
                "wf/oversized-side-effect",
                "workflows",
                unit(),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("oversized-side-effect-worker")
            .workflow_task_queue("workflows")
            .register_workflow(oversized_side_effect_workflow)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 1);

        let history = stream_all(&backend, &run_id).await;
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::SideEffectMarker(_)))
        );
        let HistoryEventData::WorkflowFailed { failure } = &history[1].data else {
            panic!("oversized side effect should fail the workflow task");
        };
        assert_eq!(failure.error_type, "durust.payload_encode");
        assert!(failure.message.contains("side effect payload"));
    });
}

#[test]
fn activity_can_heartbeat_through_worker_context() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<heartbeat_activity_workflow>(
                "wf/heartbeat-activity",
                "workflows",
                number(20),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("heartbeat-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(heartbeat_activity_workflow)
            .register_activity(heartbeat_activity_test)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 2);
        assert_eq!(stats.activity_tasks, 1);

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        let HistoryEventData::ActivityScheduled(scheduled) = &history[1].data else {
            panic!("workflow did not schedule activity");
        };
        assert_eq!(scheduled.heartbeat_timeout, Some(Duration::from_secs(30)));
        assert!(matches!(
            history[2].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 40);
    });
}

#[test]
fn child_workflow_spawn_and_wait_completes_from_public_api() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<child_spawn_wait_workflow>(
                "wf/child-wait-parent",
                "workflows",
                number(11),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(child_spawn_wait_workflow)
            .register_workflow(child_double_workflow)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert!(stats.child_workflow_starts_dispatched >= 1);

        let history = stream_all(&backend, &run_id).await;
        assert!(history.iter().any(|event| {
            matches!(event.data, HistoryEventData::ChildWorkflowStartRequested(_))
        }));
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ChildWorkflowStarted(_)))
        );
        assert!(
            history
                .iter()
                .any(|event| { matches!(event.data, HistoryEventData::ChildWorkflowCompleted(_)) })
        );
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("parent history").data
        else {
            panic!("parent workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 22);
    });
}

#[test]
fn postgres_inline_child_wake_does_not_advance_cache_past_unobserved_events_when_configured() {
    block_on_tokio(async {
        let Some(url) = postgres_url_from_env() else {
            eprintln!("skipping Postgres inline child cache regression; set DURUST_POSTGRES_URL");
            return;
        };
        let schema = postgres_test_schema("inline_child_cache");
        let backend = PostgresBackend::connect_with_config(
            PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
        )
        .await
        .unwrap();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<postgres_inline_child_signal_timer>(
                "wf/postgres-inline-child-cache",
                "workflows",
                number(11),
            )
            .await
            .unwrap();
        client
            .signal_workflow(
                "wf/postgres-inline-child-cache",
                "ready",
                "signal/postgres-inline-child-cache/ready",
                "go",
            )
            .await
            .unwrap();

        let mut worker = Worker::builder(backend.clone())
            .worker_id("postgres-inline-child-cache-worker")
            .workflow_task_queue("workflows")
            .register_workflow(postgres_inline_child_signal_timer)
            .register_workflow(child_double_workflow)
            .build();
        let stats = worker.run_until_idle().await.unwrap();
        assert!(
            stats.workflow_tasks >= 4,
            "worker should make progress through parent, child, signal, and timer wakes"
        );

        let history = stream_all(&backend, &run_id).await;
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ChildWorkflowCompleted(_)))
        );
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::SignalConsumed(_)))
        );
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::TimerFired(_)))
        );
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("parent history").data
        else {
            panic!("parent workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "22:go");

        drop_postgres_schema(&url, &schema).await;
    });
}

#[test]
fn child_workflow_abandon_lets_child_continue_after_parent_exit() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<child_spawn_abandon_workflow>(
                "wf/child-abandon-parent",
                "workflows",
                number(12),
            )
            .await
            .unwrap();
        let mut parent_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(child_spawn_abandon_workflow)
            .build();
        parent_worker.run_until_idle().await.unwrap();

        let parent_history = stream_all(&backend, &run_id).await;
        let child_run_id = parent_history
            .iter()
            .find_map(|event| match &event.data {
                HistoryEventData::ChildWorkflowStarted(started) => Some(started.run_id.clone()),
                _ => None,
            })
            .expect("child started");
        assert!(matches!(
            parent_history.last().expect("parent terminal").data,
            HistoryEventData::WorkflowCompleted { .. }
        ));

        let mut child_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(child_double_workflow)
            .build();
        assert!(child_worker.run_workflow_once().await.unwrap());
        let child_history = stream_all(&backend, &child_run_id).await;
        assert!(matches!(
            child_history.last().expect("child terminal").data,
            HistoryEventData::WorkflowCompleted { .. }
        ));
    });
}

#[test]
fn child_workflow_cancel_policy_cancels_child_on_parent_exit() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<child_spawn_cancel_workflow>(
                "wf/child-cancel-parent",
                "workflows",
                number(13),
            )
            .await
            .unwrap();
        let mut parent_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(child_spawn_cancel_workflow)
            .build();
        parent_worker.run_until_idle().await.unwrap();

        let parent_history = stream_all(&backend, &run_id).await;
        let child_run_id = parent_history
            .iter()
            .find_map(|event| match &event.data {
                HistoryEventData::ChildWorkflowStarted(started) => Some(started.run_id.clone()),
                _ => None,
            })
            .expect("child started");
        let child_claim = backend
            .claim_workflow_task(
                WorkerId::new("cancelled-child-worker"),
                ClaimWorkflowTaskOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("workflows"),
                    registered_workflow_types: vec![WorkflowType::new("tests.child-double", 1)],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(child_claim.is_none());
        let child_history = stream_all(&backend, &child_run_id).await;
        assert!(
            child_history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowCancelled { .. }))
        );
    });
}

#[test]
fn child_workflow_result_can_win_select() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_child_result_workflow>(
                "wf/select-child-result-parent",
                "workflows",
                number(14),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(select_child_result_workflow)
            .register_workflow(child_double_workflow)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert!(stats.child_workflow_starts_dispatched >= 1);

        let history = stream_all(&backend, &run_id).await;
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ChildWorkflowCompleted(_)))
        );
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::TimerFired(_)))
        );
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("parent terminal").data
        else {
            panic!("parent workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "child:28"
        );
    });
}

#[test]
fn losing_child_workflow_result_select_branch_does_not_cancel_child() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_timer_before_child_result_workflow>(
                "wf/select-child-result-loses-parent",
                "workflows",
                number(15),
            )
            .await
            .unwrap();
        let mut parent_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(select_timer_before_child_result_workflow)
            .build();

        let stats = parent_worker.run_until_idle().await.unwrap();
        assert!(stats.child_workflow_starts_dispatched >= 1);

        let parent_history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } =
            &parent_history.last().expect("parent terminal").data
        else {
            panic!("parent workflow did not complete");
        };
        let result = durust::decode_payload::<String>(result).unwrap();
        let child_run_id = result
            .strip_prefix("timer:")
            .map(durust::RunId::new)
            .expect("timer branch should win before child is claimed");
        assert!(
            !parent_history
                .iter()
                .any(|event| { matches!(event.data, HistoryEventData::ChildWorkflowCancelled(_)) })
        );

        let mut child_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(child_double_workflow)
            .build();
        assert!(child_worker.run_workflow_once().await.unwrap());
        let child_history = stream_all(&backend, &child_run_id).await;
        assert!(matches!(
            child_history.last().expect("child terminal").data,
            HistoryEventData::WorkflowCompleted { .. }
        ));
    });
}

#[test]
fn activity_spawn_handle_launches_before_result_is_awaited() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<activity_spawn_await_later_workflow>(
                "wf/activity-spawn-await-later",
                "workflows",
                number(10),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(activity_spawn_await_later_workflow)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let history = stream_all(&backend, &run_id).await;
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(event.data, HistoryEventData::ActivityScheduled(_)))
                .count(),
            2
        );
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ActivityCompleted(_)))
        );

        let activity_opts = ClaimActivityOptions {
            namespace: Namespace::default(),
            task_queue: TaskQueue::new("activities"),
            registered_activity_names: vec![ActivityName::new("tests.double")],
            lease_duration: Duration::from_secs(30),
        };
        let first = backend
            .claim_activity_task(WorkerId::new("spawn-worker-1"), activity_opts.clone())
            .await
            .unwrap()
            .expect("first spawned activity");
        let second = backend
            .claim_activity_task(WorkerId::new("spawn-worker-2"), activity_opts.clone())
            .await
            .unwrap()
            .expect("second spawned activity");
        assert_ne!(first.task.command_id, second.task.command_id);

        backend
            .complete_activity(CompleteActivityRequest {
                claim: first.claim,
                result: durust::encode_payload(&20_u64).unwrap(),
            })
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());

        let late_second = backend
            .complete_activity(CompleteActivityRequest {
                claim: second.claim,
                result: durust::encode_payload(&22_u64).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            late_second,
            durust::CompleteActivityOutcome::AlreadyCompleted
        );

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("workflow terminal").data
        else {
            panic!("workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 20);
    });
}

#[test]
fn select_all_races_spawned_activity_handles_and_cancels_pending_losers() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_all_activity_handles_workflow>(
                "wf/select-all-activity-handles",
                "workflows",
                number(10),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(select_all_activity_handles_workflow)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let activity_opts = ClaimActivityOptions {
            namespace: Namespace::default(),
            task_queue: TaskQueue::new("activities"),
            registered_activity_names: vec![ActivityName::new("tests.double")],
            lease_duration: Duration::from_secs(30),
        };
        let first = backend
            .claim_activity_task(WorkerId::new("race-worker-1"), activity_opts.clone())
            .await
            .unwrap()
            .expect("first spawned activity");
        let second = backend
            .claim_activity_task(WorkerId::new("race-worker-2"), activity_opts.clone())
            .await
            .unwrap()
            .expect("second spawned activity");
        let third = backend
            .claim_activity_task(WorkerId::new("race-worker-3"), activity_opts.clone())
            .await
            .unwrap()
            .expect("third spawned activity");

        backend
            .complete_activity(CompleteActivityRequest {
                claim: second.claim,
                result: durust::encode_payload(&22_u64).unwrap(),
            })
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::SelectWinner(_)))
        );
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("workflow terminal").data
        else {
            panic!("workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "1:22");

        for claim in [first.claim, third.claim] {
            let late = backend
                .complete_activity(CompleteActivityRequest {
                    claim,
                    result: durust::encode_payload(&999_u64).unwrap(),
                })
                .await
                .unwrap();
            assert_eq!(late, durust::CompleteActivityOutcome::AlreadyCompleted);
        }
        assert!(
            backend
                .claim_activity_task(WorkerId::new("race-worker-4"), activity_opts)
                .await
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn select_all_can_mix_activity_child_and_timer_branches() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_all_mixed_branches_workflow>(
                "wf/select-all-mixed-branches",
                "workflows",
                number(8),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(select_all_mixed_branches_workflow)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert!(stats.child_workflow_starts_dispatched >= 1);
        assert!(stats.timers_fired >= 1);

        let history = stream_all(&backend, &run_id).await;
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ActivityScheduled(_)))
        );
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ChildWorkflowStarted(_)))
        );
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::TimerFired(_)))
        );
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("workflow terminal").data
        else {
            panic!("workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "2:timer");
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ChildWorkflowCancelled(_)))
        );

        let remaining_activity = backend
            .claim_activity_task(
                WorkerId::new("mixed-activity-worker"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("tests.double")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(remaining_activity.is_none());
    });
}

#[test]
fn replay_skips_child_start_consumed_out_of_order_before_later_timer_command() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_all_mixed_branches_workflow>(
                "wf/out-of-order-child-start",
                "workflows",
                number(8),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(select_all_mixed_branches_workflow)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let activity = backend
            .claim_activity_task(
                WorkerId::new("out-of-order-child-start-activity"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("tests.double")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("scheduled activity");
        backend
            .complete_activity(CompleteActivityRequest {
                claim: activity.claim,
                result: durust::encode_payload(&16_u64).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(worker.run_child_workflow_starts_once().await.unwrap(), 1);

        let history = stream_all(&backend, &run_id).await;
        assert!(matches!(
            history[3].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        assert!(matches!(
            history[4].data,
            HistoryEventData::ChildWorkflowStarted(_)
        ));

        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("workflow terminal").data
        else {
            panic!("workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "0:activity:16"
        );
    });
}

#[test]
fn replay_skips_child_completion_consumed_out_of_order_before_later_timer_command() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<child_first_select_then_timer_workflow>(
                "wf/out-of-order-child-completion",
                "workflows",
                number(9),
            )
            .await
            .unwrap();
        let mut parent_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(child_first_select_then_timer_workflow)
            .build();
        let mut child_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(child_double_workflow)
            .build();

        assert!(parent_worker.run_workflow_once().await.unwrap());
        let activity = backend
            .claim_activity_task(
                WorkerId::new("out-of-order-child-completion-activity"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("tests.double")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("scheduled activity");
        backend
            .complete_activity(CompleteActivityRequest {
                claim: activity.claim,
                result: durust::encode_payload(&18_u64).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            parent_worker
                .run_child_workflow_starts_once()
                .await
                .unwrap(),
            1
        );
        assert!(child_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert!(matches!(
            history[3].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        assert!(matches!(
            history[4].data,
            HistoryEventData::ChildWorkflowStarted(_)
        ));
        assert!(matches!(
            history[5].data,
            HistoryEventData::ChildWorkflowCompleted(_)
        ));

        assert!(parent_worker.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::ZERO);
        assert_eq!(parent_worker.run_timers_once().await.unwrap(), 1);
        assert!(parent_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("workflow terminal").data
        else {
            panic!("workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "1:activity:18"
        );
    });
}

#[test]
fn replay_skips_timer_fired_consumed_out_of_order_before_later_timer_command() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<timer_first_select_then_timer_workflow>(
                "wf/out-of-order-timer-fired",
                "workflows",
                number(7),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(timer_first_select_then_timer_workflow)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let activity = backend
            .claim_activity_task(
                WorkerId::new("out-of-order-timer-activity"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("tests.double")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("scheduled activity");
        backend
            .complete_activity(CompleteActivityRequest {
                claim: activity.claim,
                result: durust::encode_payload(&14_u64).unwrap(),
            })
            .await
            .unwrap();
        backend.advance_time(Duration::ZERO);
        assert_eq!(worker.run_timers_once().await.unwrap(), 1);

        let history = stream_all(&backend, &run_id).await;
        assert!(matches!(
            history[3].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        assert!(matches!(history[4].data, HistoryEventData::TimerFired(_)));

        assert!(worker.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::ZERO);
        assert_eq!(worker.run_timers_once().await.unwrap(), 1);
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("workflow terminal").data
        else {
            panic!("workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "1:activity:14"
        );
    });
}

fn out_of_order_worker<W>(
    backend: MemoryBackend,
    workflow: W,
    chunk_events: Option<usize>,
) -> Worker<MemoryBackend>
where
    W: durust::Workflow + Default,
{
    let mut builder = Worker::builder(backend)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(workflow)
        .register_activity(double);
    if let Some(chunk_events) = chunk_events {
        builder = builder.history_chunk_events(chunk_events);
    }
    builder.build()
}

// Drives the verified repro ordering for the spawn/sleep workflows: the first
// workflow task commits ActivityScheduled(cmd 1) + TimerStarted(cmd 2), the
// activity completes, and the timer fires, so the run's next replay chunk is
// [ActivityCompleted, TimerFired] with the unconsumed completion sitting at
// the replay cursor head when the post-sleep command schedules.
async fn drive_completion_before_timer_fired(
    backend: &MemoryBackend,
    worker: &mut Worker<MemoryBackend>,
    run_id: &durust::RunId,
) {
    assert!(worker.run_workflow_once().await.unwrap());
    assert!(worker.run_activity_once().await.unwrap());
    backend.advance_time(Duration::from_secs(1));
    assert_eq!(worker.run_timers_once().await.unwrap(), 1);

    let history = stream_all(backend, run_id).await;
    assert!(matches!(
        history[3].data,
        HistoryEventData::ActivityCompleted(_)
    ));
    assert!(matches!(history[4].data, HistoryEventData::TimerFired(_)));
}

// `cold_chunk_events` = None runs the critical task against the scheduling
// worker's cached future; Some(n) drops that worker first so a fresh worker
// cold-replays the full history in n-event chunks.
async fn run_spawn_sleep_sleep_out_of_order_case(cold_chunk_events: Option<usize>) {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<spawn_sleep_sleep_workflow>(
            "wf/spawn-sleep-sleep",
            "workflows",
            number(21),
        )
        .await
        .unwrap();
    let mut worker = out_of_order_worker(backend.clone(), spawn_sleep_sleep_workflow, None);
    drive_completion_before_timer_fired(&backend, &mut worker, &run_id).await;
    if let Some(chunk_events) = cold_chunk_events {
        drop(worker);
        worker = out_of_order_worker(
            backend.clone(),
            spawn_sleep_sleep_workflow,
            Some(chunk_events),
        );
    }

    // The critical task: the second sleep's TimerStarted must be scheduled
    // past the unconsumed ActivityCompleted at the replay cursor head instead
    // of failing with Nondeterminism("expected TimerStarted ... found
    // ActivityCompleted").
    assert!(worker.run_workflow_once().await.unwrap());
    let history = stream_all(&backend, &run_id).await;
    assert!(matches!(history[5].data, HistoryEventData::TimerStarted(_)));

    backend.advance_time(Duration::from_secs(1));
    assert_eq!(worker.run_timers_once().await.unwrap(), 1);
    assert!(worker.run_workflow_once().await.unwrap());

    let history = stream_all(&backend, &run_id).await;
    let HistoryEventData::WorkflowCompleted { result } = &history.last().unwrap().data else {
        panic!("spawn-sleep-sleep workflow did not complete");
    };
    assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 42);
}

#[test]
fn spawn_sleep_sleep_schedules_second_timer_past_out_of_order_completion_cached() {
    block_on(run_spawn_sleep_sleep_out_of_order_case(None));
}

#[test]
fn spawn_sleep_sleep_schedules_second_timer_past_out_of_order_completion_cold() {
    block_on(run_spawn_sleep_sleep_out_of_order_case(Some(100)));
}

#[test]
fn spawn_sleep_sleep_schedules_second_timer_past_out_of_order_completion_multi_chunk() {
    block_on(run_spawn_sleep_sleep_out_of_order_case(Some(1)));
}

async fn run_out_of_order_completion_before_new_activity_case(cold_chunk_events: Option<usize>) {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<spawn_sleep_then_activity_workflow>(
            "wf/spawn-sleep-then-activity",
            "workflows",
            number(10),
        )
        .await
        .unwrap();
    let mut worker = out_of_order_worker(backend.clone(), spawn_sleep_then_activity_workflow, None);
    drive_completion_before_timer_fired(&backend, &mut worker, &run_id).await;
    if let Some(chunk_events) = cold_chunk_events {
        drop(worker);
        worker = out_of_order_worker(
            backend.clone(),
            spawn_sleep_then_activity_workflow,
            Some(chunk_events),
        );
    }

    // The second activity's ActivityScheduled must schedule past the
    // unconsumed first completion at the cursor head.
    assert!(worker.run_workflow_once().await.unwrap());
    let history = stream_all(&backend, &run_id).await;
    assert!(matches!(
        history[5].data,
        HistoryEventData::ActivityScheduled(_)
    ));

    assert!(worker.run_activity_once().await.unwrap());
    assert!(worker.run_workflow_once().await.unwrap());

    let history = stream_all(&backend, &run_id).await;
    let HistoryEventData::WorkflowCompleted { result } = &history.last().unwrap().data else {
        panic!("spawn-sleep-then-activity workflow did not complete");
    };
    // 2*10 from the spawned activity plus 2*11 from the sequential one.
    assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 42);
}

#[test]
fn out_of_order_completion_before_new_activity_command_cached() {
    block_on(run_out_of_order_completion_before_new_activity_case(None));
}

#[test]
fn out_of_order_completion_before_new_activity_command_cold_multi_chunk() {
    block_on(run_out_of_order_completion_before_new_activity_case(Some(
        1,
    )));
}

async fn run_out_of_order_completion_before_side_effect_case(cold_chunk_events: Option<usize>) {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<spawn_sleep_then_side_effect_workflow>(
            "wf/spawn-sleep-then-side-effect",
            "workflows",
            number(10),
        )
        .await
        .unwrap();
    let mut worker =
        out_of_order_worker(backend.clone(), spawn_sleep_then_side_effect_workflow, None);
    drive_completion_before_timer_fired(&backend, &mut worker, &run_id).await;
    if let Some(chunk_events) = cold_chunk_events {
        drop(worker);
        worker = out_of_order_worker(
            backend.clone(),
            spawn_sleep_then_side_effect_workflow,
            Some(chunk_events),
        );
    }

    // The side effect marker records past the unconsumed completion; the
    // pending activity result then resolves through the index, completing the
    // workflow in the same task.
    assert!(worker.run_workflow_once().await.unwrap());

    let history = stream_all(&backend, &run_id).await;
    assert!(matches!(
        history[5].data,
        HistoryEventData::SideEffectMarker(_)
    ));
    let HistoryEventData::WorkflowCompleted { result } = &history.last().unwrap().data else {
        panic!("spawn-sleep-then-side-effect workflow did not complete");
    };
    assert_eq!(
        durust::decode_payload::<String>(result).unwrap(),
        "tagged:20"
    );
}

#[test]
fn out_of_order_completion_before_side_effect_cached() {
    block_on(run_out_of_order_completion_before_side_effect_case(None));
}

#[test]
fn out_of_order_completion_before_side_effect_cold_multi_chunk() {
    block_on(run_out_of_order_completion_before_side_effect_case(Some(1)));
}

async fn run_out_of_order_completion_before_version_marker_case(cold_chunk_events: Option<usize>) {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<spawn_sleep_then_version_workflow>(
            "wf/spawn-sleep-then-version",
            "workflows",
            number(10),
        )
        .await
        .unwrap();
    let mut worker = out_of_order_worker(backend.clone(), spawn_sleep_then_version_workflow, None);
    drive_completion_before_timer_fired(&backend, &mut worker, &run_id).await;
    if let Some(chunk_events) = cold_chunk_events {
        drop(worker);
        worker = out_of_order_worker(
            backend.clone(),
            spawn_sleep_then_version_workflow,
            Some(chunk_events),
        );
    }

    // get_version must record its marker past the unconsumed completion.
    assert!(worker.run_workflow_once().await.unwrap());
    let history = stream_all(&backend, &run_id).await;
    assert!(matches!(
        history[5].data,
        HistoryEventData::VersionMarker(_)
    ));
    assert!(matches!(history[6].data, HistoryEventData::TimerStarted(_)));

    backend.advance_time(Duration::from_secs(1));
    assert_eq!(worker.run_timers_once().await.unwrap(), 1);
    assert!(worker.run_workflow_once().await.unwrap());

    let history = stream_all(&backend, &run_id).await;
    let HistoryEventData::WorkflowCompleted { result } = &history.last().unwrap().data else {
        panic!("spawn-sleep-then-version workflow did not complete");
    };
    // 2*10 from the spawned activity plus version 1.
    assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 21);
}

#[test]
fn out_of_order_completion_before_version_marker_cached() {
    block_on(run_out_of_order_completion_before_version_marker_case(None));
}

#[test]
fn out_of_order_completion_before_version_marker_cold_multi_chunk() {
    block_on(run_out_of_order_completion_before_version_marker_case(
        Some(1),
    ));
}

// Crashes after the version marker committed, so the cold replay preconsumes
// the marker from the provider's change-version index while an out-of-order
// completion sits at the cursor head and the marker event is still unloaded.
#[test]
fn recorded_version_marker_preconsumes_past_out_of_order_completion_on_cold_replay() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<spawn_sleep_then_version_workflow>(
                "wf/spawn-sleep-then-version-preconsume",
                "workflows",
                number(10),
            )
            .await
            .unwrap();
        let mut worker =
            out_of_order_worker(backend.clone(), spawn_sleep_then_version_workflow, None);
        drive_completion_before_timer_fired(&backend, &mut worker, &run_id).await;
        assert!(worker.run_workflow_once().await.unwrap());
        drop(worker);

        backend.advance_time(Duration::from_secs(1));
        let mut replay_worker =
            out_of_order_worker(backend.clone(), spawn_sleep_then_version_workflow, Some(1));
        assert_eq!(replay_worker.run_timers_once().await.unwrap(), 1);
        assert!(replay_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } = &history.last().unwrap().data else {
            panic!("spawn-sleep-then-version workflow did not complete after cold replay");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 21);
    });
}

async fn run_out_of_order_completion_before_child_spawn_case(cold_chunk_events: Option<usize>) {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<spawn_sleep_then_child_workflow>(
            "wf/spawn-sleep-then-child",
            "workflows",
            number(10),
        )
        .await
        .unwrap();
    let build_worker = |chunk_events: Option<usize>| {
        let mut builder = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(spawn_sleep_then_child_workflow)
            .register_workflow(child_double_workflow)
            .register_activity(double);
        if let Some(chunk_events) = chunk_events {
            builder = builder.history_chunk_events(chunk_events);
        }
        builder.build()
    };
    let mut worker = build_worker(None);
    drive_completion_before_timer_fired(&backend, &mut worker, &run_id).await;
    if let Some(chunk_events) = cold_chunk_events {
        drop(worker);
        worker = build_worker(Some(chunk_events));
    }

    // The child start request must schedule past the unconsumed completion.
    assert!(worker.run_workflow_once().await.unwrap());
    let history = stream_all(&backend, &run_id).await;
    assert!(matches!(
        history[5].data,
        HistoryEventData::ChildWorkflowStartRequested(_)
    ));

    let stats = worker.run_until_idle().await.unwrap();
    assert!(stats.child_workflow_starts_dispatched >= 1);

    let history = stream_all(&backend, &run_id).await;
    let HistoryEventData::WorkflowCompleted { result } = &history.last().unwrap().data else {
        panic!("spawn-sleep-then-child workflow did not complete");
    };
    // 2*10 from the spawned activity plus 2*11 from the child workflow.
    assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 42);
}

#[test]
fn out_of_order_completion_before_child_spawn_cached() {
    block_on(run_out_of_order_completion_before_child_spawn_case(None));
}

#[test]
fn out_of_order_completion_before_child_spawn_cold_multi_chunk() {
    block_on(run_out_of_order_completion_before_child_spawn_case(Some(1)));
}

// Cold replay of a two-signal select where the SECOND branch won: the
// recorded SignalConsumed for the later branch sits at the cursor head when
// the first branch replays, and must be skipped instead of reported as a
// command sequence mismatch.
async fn run_cold_replay_of_second_branch_signal_select_case(chunk_events: usize) {
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<select_two_signals_then_signal_workflow>(
            "wf/select-two-signals-then-signal",
            "workflows",
            unit(),
        )
        .await
        .unwrap();
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .register_workflow(select_two_signals_then_signal_workflow)
        .build();

    assert!(worker.run_workflow_once().await.unwrap());
    client
        .signal_workflow(
            "wf/select-two-signals-then-signal",
            "right",
            "signal/select-two/right",
            "go",
        )
        .await
        .unwrap();
    assert!(worker.run_workflow_once().await.unwrap());

    let history = stream_all(&backend, &run_id).await;
    assert!(matches!(
        history[1].data,
        HistoryEventData::SignalConsumed(_)
    ));
    assert!(matches!(history[2].data, HistoryEventData::SelectWinner(_)));
    drop(worker);

    client
        .signal_workflow(
            "wf/select-two-signals-then-signal",
            "after",
            "signal/select-two/after",
            "done",
        )
        .await
        .unwrap();
    let mut replay_worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .history_chunk_events(chunk_events)
        .register_workflow(select_two_signals_then_signal_workflow)
        .build();
    assert!(replay_worker.run_workflow_once().await.unwrap());

    let history = stream_all(&backend, &run_id).await;
    let HistoryEventData::WorkflowCompleted { result } = &history.last().unwrap().data else {
        panic!("select-two-signals workflow did not complete after cold replay");
    };
    assert_eq!(
        durust::decode_payload::<String>(result).unwrap(),
        "right:go:done"
    );
}

#[test]
fn cold_replay_of_two_signal_select_with_second_branch_winner() {
    block_on(run_cold_replay_of_second_branch_signal_select_case(100));
}

#[test]
fn cold_replay_of_two_signal_select_with_second_branch_winner_multi_chunk() {
    block_on(run_cold_replay_of_second_branch_signal_select_case(1));
}

fn consumed_signal_ids(history: &[durust::HistoryEvent]) -> Vec<durust::SignalId> {
    history
        .iter()
        .filter_map(|event| match &event.data {
            HistoryEventData::SignalConsumed(consumed) => Some(consumed.signal_id.clone()),
            _ => None,
        })
        .collect()
}

// One live delivery must satisfy at most one waiter: signal consumption only
// commits with the task, so the second sequential wait re-reads the same
// inbox record mid-task and must stay pending until a distinct delivery.
#[test]
fn one_signal_delivery_is_consumed_once_by_sequential_same_name_waits() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<same_signal_twice_workflow>(
                "wf/same-signal-twice",
                "workflows",
                unit(),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(same_signal_twice_workflow)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        client
            .signal_workflow("wf/same-signal-twice", "gate", "gate/1", "one")
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(
            consumed_signal_ids(&history),
            vec![durust::SignalId::new("gate/1")],
            "one delivery must produce exactly one SignalConsumed"
        );
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowCompleted { .. })),
            "second wait must stay pending until a distinct delivery"
        );

        client
            .signal_workflow("wf/same-signal-twice", "gate", "gate/2", "two")
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(
            consumed_signal_ids(&history),
            vec![
                durust::SignalId::new("gate/1"),
                durust::SignalId::new("gate/2")
            ],
            "each wait must consume a distinct delivery"
        );
        let HistoryEventData::WorkflowCompleted { result } = &history.last().unwrap().data else {
            panic!("same-signal-twice workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "one:two");
    });
}

// Concurrent same-name waiters in one poll batch both read the same
// min-sequence inbox record; exactly one branch may resolve per delivery.
#[test]
fn one_signal_delivery_resolves_exactly_one_concurrent_same_name_waiter() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<same_signal_join_workflow>("wf/same-signal-join", "workflows", unit())
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(same_signal_join_workflow)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        client
            .signal_workflow("wf/same-signal-join", "gate", "gate/1", "one")
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(
            consumed_signal_ids(&history),
            vec![durust::SignalId::new("gate/1")],
            "one delivery must resolve exactly one of the joined waiters"
        );
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowCompleted { .. })),
            "join must stay pending until the second delivery"
        );

        client
            .signal_workflow("wf/same-signal-join", "gate", "gate/2", "two")
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(
            consumed_signal_ids(&history),
            vec![
                durust::SignalId::new("gate/1"),
                durust::SignalId::new("gate/2")
            ],
            "the joined waiters must consume distinct deliveries"
        );
        let HistoryEventData::WorkflowCompleted { result } = &history.last().unwrap().data else {
            panic!("same-signal-join workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "one:two");
    });
}

// With the seq-mismatch checks removed from SignalFuture, the fingerprint
// comparison in decode_consumed_signal is the only guard that detects a
// changed signal name at a recorded consumption's command position. Replaying
// a recorded "gate" consumption against code awaiting "door" at the same
// position must fail as nondeterminism, and the task must be released
// without appending a WorkflowFailed terminal.
#[test]
fn changed_signal_name_at_recorded_consumption_is_detected_as_nondeterminism() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<signal_gate_then_after_workflow>(
                "wf/signal-fingerprint-changed",
                "workflows",
                unit(),
            )
            .await
            .unwrap();
        let mut original_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(signal_gate_then_after_workflow)
            .build();

        // Record SignalConsumed("gate") at command 1, then leave the run
        // non-terminal blocked on the "after" wait.
        assert!(original_worker.run_workflow_once().await.unwrap());
        client
            .signal_workflow(
                "wf/signal-fingerprint-changed",
                "gate",
                "signal/fingerprint/gate",
                "go",
            )
            .await
            .unwrap();
        assert!(original_worker.run_workflow_once().await.unwrap());
        drop(original_worker);

        let history = stream_all(&backend, &run_id).await;
        assert!(matches!(
            history[1].data,
            HistoryEventData::SignalConsumed(_)
        ));

        // Wake the run so the changed worker cold-replays the recorded
        // consumption against code awaiting "door" at the same position.
        client
            .signal_workflow(
                "wf/signal-fingerprint-changed",
                "after",
                "signal/fingerprint/after",
                "done",
            )
            .await
            .unwrap();
        let mut changed_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(signal_door_then_after_workflow)
            .nondeterminism_retry_backoff(Duration::from_millis(25))
            .build();
        let err = changed_worker.run_workflow_once().await.unwrap_err();
        assert!(matches!(err, durust::Error::Nondeterminism(_)));

        // Nondeterminism releases the task for retry against fixed code; it
        // must not fail the workflow.
        let history = stream_all(&backend, &run_id).await;
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
        );
    });
}

#[test]
fn join_all_collects_spawned_activity_results_in_input_order_after_crash() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<join_all_activity_handles_workflow>(
                "wf/join-all-activity-handles",
                "workflows",
                number(10),
            )
            .await
            .unwrap();
        let mut scheduling_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(join_all_activity_handles_workflow)
            .build();

        assert!(scheduling_worker.run_workflow_once().await.unwrap());
        let activity_opts = ClaimActivityOptions {
            namespace: Namespace::default(),
            task_queue: TaskQueue::new("activities"),
            registered_activity_names: vec![ActivityName::new("tests.double")],
            lease_duration: Duration::from_secs(30),
        };
        let first = backend
            .claim_activity_task(WorkerId::new("join-all-worker-1"), activity_opts.clone())
            .await
            .unwrap()
            .expect("first join_all activity");
        let second = backend
            .claim_activity_task(WorkerId::new("join-all-worker-2"), activity_opts.clone())
            .await
            .unwrap()
            .expect("second join_all activity");
        let third = backend
            .claim_activity_task(WorkerId::new("join-all-worker-3"), activity_opts)
            .await
            .unwrap()
            .expect("third join_all activity");

        for (claim, result) in [
            (third.claim, 24_u64),
            (first.claim, 20_u64),
            (second.claim, 22_u64),
        ] {
            backend
                .complete_activity(CompleteActivityRequest {
                    claim,
                    result: durust::encode_payload(&result).unwrap(),
                })
                .await
                .unwrap();
        }
        drop(scheduling_worker);

        let mut replay_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(join_all_activity_handles_workflow)
            .build();
        assert!(replay_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(event.data, HistoryEventData::ActivityScheduled(_)))
                .count(),
            3
        );
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(event.data, HistoryEventData::ActivityCompleted(_)))
                .count(),
            3
        );
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::SelectWinner(_)))
        );
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("workflow terminal").data
        else {
            panic!("workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "20,22,24"
        );
    });
}

#[test]
fn join_all_can_collect_boxed_mixed_durable_branches() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<join_all_mixed_branches_workflow>(
                "wf/join-all-mixed-branches",
                "workflows",
                number(9),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(join_all_mixed_branches_workflow)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::ZERO);
        assert_eq!(worker.run_timers_once().await.unwrap(), 1);
        let activity = backend
            .claim_activity_task(
                WorkerId::new("join-all-mixed-activity"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("tests.double")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("mixed join_all activity");
        backend
            .complete_activity(CompleteActivityRequest {
                claim: activity.claim,
                result: durust::encode_payload(&18_u64).unwrap(),
            })
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::TimerFired(_)))
        );
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("workflow terminal").data
        else {
            panic!("workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "activity:18|timer"
        );
    });
}

#[test]
fn cancelling_pending_workflow_cleans_activity_without_workflow_failure() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/cancel-pending", "workflows", number(20))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let claimed_activity = backend
            .claim_activity_task(
                WorkerId::new("late-worker"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("tests.double")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("activity task");

        let cancelled = client
            .cancel_workflow("wf/cancel-pending", "test cancellation")
            .await
            .unwrap();
        assert_eq!(
            cancelled,
            durust::CancelWorkflowOutcome::Cancelled {
                run_id: run_id.clone(),
                event_id: EventId(3)
            }
        );
        let late_completion = backend
            .complete_activity(CompleteActivityRequest {
                claim: claimed_activity.claim,
                result: durust::encode_payload(&40_u64).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            late_completion,
            durust::CompleteActivityOutcome::AlreadyCompleted
        );
        assert!(!worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 3);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        assert!(matches!(
            history[2].data,
            HistoryEventData::WorkflowCancelled { .. }
        ));
        assert!(!history.iter().any(|event| matches!(
            event.data,
            HistoryEventData::ActivityCompleted(_)
                | HistoryEventData::WorkflowCompleted { .. }
                | HistoryEventData::WorkflowFailed { .. }
        )));
    });
}

#[test]
fn join_registers_all_branches_before_waiting() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<join_two_activities>("wf/join-register", "workflows", number(10))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(join_two_activities)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 3);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        assert!(matches!(
            history[2].data,
            HistoryEventData::ActivityScheduled(_)
        ));

        let activity_opts = ClaimActivityOptions {
            namespace: Namespace::default(),
            task_queue: TaskQueue::new("activities"),
            registered_activity_names: vec![ActivityName::new("tests.double")],
            lease_duration: Duration::from_secs(30),
        };
        let first = backend
            .claim_activity_task(WorkerId::new("join-worker-1"), activity_opts.clone())
            .await
            .unwrap()
            .expect("first joined activity");
        let second = backend
            .claim_activity_task(WorkerId::new("join-worker-2"), activity_opts)
            .await
            .unwrap()
            .expect("second joined activity");
        assert_ne!(first.task.command_id.seq, second.task.command_id.seq);
    });
}

#[test]
fn sequential_awaits_do_not_register_later_activity_before_waiting() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<sequential_two_activities>(
                "wf/sequential-awaits",
                "workflows",
                number(10),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(sequential_two_activities)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 2);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
    });
}

#[test]
fn join_accepts_more_than_three_branches() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<join_four_activities>("wf/join-four", "workflows", number(10))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(join_four_activities)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 5);
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(event.data, HistoryEventData::ActivityScheduled(_)))
                .count(),
            4
        );
    });
}

#[test]
fn join_waits_for_signal_and_timer_branches() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<join_signal_timer>("wf/join-signal-timer", "workflows", number(10))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(join_signal_timer)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        client
            .signal_workflow("wf/join-signal-timer", "ready", "signal/join/1", "joined")
            .await
            .unwrap();
        backend.advance_time(Duration::from_millis(10));
        assert_eq!(worker.run_timers_once().await.unwrap(), 1);
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 5);
        assert!(matches!(history[1].data, HistoryEventData::TimerStarted(_)));
        assert!(matches!(history[2].data, HistoryEventData::TimerFired(_)));
        assert!(matches!(
            history[3].data,
            HistoryEventData::SignalConsumed(_)
        ));
        let HistoryEventData::WorkflowCompleted { result } = &history[4].data else {
            panic!("join signal/timer workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "joined");
    });
}

#[test]
fn join_replays_signal_consumed_after_timer_fired_before_later_timer_command() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<join_signal_timer_then_timer>(
                "wf/join-signal-timer-replay-order",
                "workflows",
                number(10),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(join_signal_timer_then_timer)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        client
            .signal_workflow(
                "wf/join-signal-timer-replay-order",
                "ready",
                "signal/join/replay-order",
                "joined",
            )
            .await
            .unwrap();
        backend.advance_time(Duration::from_millis(10));
        assert_eq!(worker.run_timers_once().await.unwrap(), 1);
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 5);
        assert!(matches!(history[1].data, HistoryEventData::TimerStarted(_)));
        assert!(matches!(history[2].data, HistoryEventData::TimerFired(_)));
        assert!(matches!(
            history[3].data,
            HistoryEventData::SignalConsumed(_)
        ));
        assert!(matches!(history[4].data, HistoryEventData::TimerStarted(_)));

        drop(worker);
        let mut replay_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .history_chunk_events(1)
            .register_workflow(join_signal_timer_then_timer)
            .build();
        backend.advance_time(Duration::ZERO);
        assert_eq!(replay_worker.run_timers_once().await.unwrap(), 1);
        assert!(replay_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("workflow terminal").data
        else {
            panic!("workflow did not complete after replay");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "joined");
    });
}

#[test]
fn join_replays_interleaved_branch_completions_after_crash() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<join_two_activities>("wf/join-replay", "workflows", number(10))
            .await
            .unwrap();
        let mut scheduling_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(join_two_activities)
            .register_activity(double)
            .build();

        assert!(scheduling_worker.run_workflow_once().await.unwrap());
        let activity_opts = ClaimActivityOptions {
            namespace: Namespace::default(),
            task_queue: TaskQueue::new("activities"),
            registered_activity_names: vec![ActivityName::new("tests.double")],
            lease_duration: Duration::from_secs(30),
        };
        let first = backend
            .claim_activity_task(WorkerId::new("join-worker-1"), activity_opts.clone())
            .await
            .unwrap()
            .expect("first joined activity");
        let second = backend
            .claim_activity_task(WorkerId::new("join-worker-2"), activity_opts)
            .await
            .unwrap()
            .expect("second joined activity");
        backend
            .complete_activity(CompleteActivityRequest {
                claim: second.claim,
                result: durust::encode_payload(&22_u64).unwrap(),
            })
            .await
            .unwrap();
        backend
            .complete_activity(CompleteActivityRequest {
                claim: first.claim,
                result: durust::encode_payload(&20_u64).unwrap(),
            })
            .await
            .unwrap();
        drop(scheduling_worker);

        let mut replay_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(join_two_activities)
            .register_activity(double)
            .build();
        assert!(replay_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 6);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        assert!(matches!(
            history[2].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        assert!(matches!(
            history[3].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        assert!(matches!(
            history[4].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        let HistoryEventData::WorkflowCompleted { result } = &history[5].data else {
            panic!("join workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 42);
    });
}

#[test]
fn select_chooses_earliest_ready_event_before_lexical_order() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_timer_before_activity>(
                "wf/select-event-order",
                "workflows",
                number(20),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(select_timer_before_activity)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let claimed_activity = backend
            .claim_activity_task(
                WorkerId::new("activity-after-timer"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("tests.double")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("activity task");
        backend.advance_time(Duration::from_millis(10));
        assert_eq!(worker.run_timers_once().await.unwrap(), 1);
        backend
            .complete_activity(CompleteActivityRequest {
                claim: claimed_activity.claim,
                result: durust::encode_payload(&40_u64).unwrap(),
            })
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 7);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        assert!(matches!(history[2].data, HistoryEventData::TimerStarted(_)));
        assert!(matches!(history[3].data, HistoryEventData::TimerFired(_)));
        assert!(matches!(
            history[4].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        let HistoryEventData::SelectWinner(winner) = &history[5].data else {
            panic!("expected SelectWinner");
        };
        assert_eq!(winner.branch_ordinal, 1);
        assert_eq!(winner.winning_event_id, EventId(4));
        let HistoryEventData::WorkflowCompleted { result } = &history[6].data else {
            panic!("select workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "timer");
    });
}

#[test]
fn select_same_tick_timer_race_is_deterministic() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_same_tick_timers>(
                "wf/select-same-tick",
                "workflows",
                number(10),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(select_same_tick_timers)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::from_millis(10));
        assert_eq!(worker.run_timers_once().await.unwrap(), 2);
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 7);
        assert!(matches!(history[1].data, HistoryEventData::TimerStarted(_)));
        assert!(matches!(history[2].data, HistoryEventData::TimerStarted(_)));
        assert!(matches!(history[3].data, HistoryEventData::TimerFired(_)));
        assert!(matches!(history[4].data, HistoryEventData::TimerFired(_)));
        let HistoryEventData::SelectWinner(winner) = &history[5].data else {
            panic!("expected SelectWinner");
        };
        assert_eq!(winner.branch_ordinal, 0);
        assert_eq!(winner.winning_event_id, EventId(4));
        let HistoryEventData::WorkflowCompleted { result } = &history[6].data else {
            panic!("select workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "left");
    });
}

#[test]
fn select_signal_winner_cancels_losing_timer_wait() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_signal_timer>("wf/select-signal", "workflows", number(50))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(select_signal_timer)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        client
            .signal_workflow("wf/select-signal", "ready", "signal/select/1", "go")
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::from_millis(50));
        assert_eq!(worker.run_timers_once().await.unwrap(), 0);

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 5);
        assert!(matches!(history[1].data, HistoryEventData::TimerStarted(_)));
        assert!(matches!(
            history[2].data,
            HistoryEventData::SignalConsumed(_)
        ));
        let HistoryEventData::SelectWinner(winner) = &history[3].data else {
            panic!("expected SelectWinner");
        };
        assert_eq!(winner.branch_ordinal, 0);
        let HistoryEventData::WorkflowCompleted { result } = &history[4].data else {
            panic!("select workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "signal:go"
        );
    });
}

#[test]
fn worker_batches_multiple_live_signal_requests_from_one_poll() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_two_signals>("wf/select-two-signals", "workflows", unit())
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(select_two_signals)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        client
            .signal_workflow(
                "wf/select-two-signals",
                "right",
                "signal/select/right",
                "go",
            )
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());

        let batch_requests = backend.signal_batch_requests();
        assert!(
            batch_requests
                .iter()
                .any(|request| request.requests.len() == 2),
            "expected one batched signal inbox read with two requests, got {batch_requests:?}"
        );

        let history = stream_all(&backend.inner, &run_id).await;
        assert_eq!(history.len(), 4);
        let HistoryEventData::SignalConsumed(consumed) = &history[1].data else {
            panic!("expected SignalConsumed");
        };
        assert_eq!(
            consumed.signal_id,
            durust::SignalId::new("signal/select/right")
        );
        let HistoryEventData::SelectWinner(winner) = &history[2].data else {
            panic!("expected SelectWinner");
        };
        assert_eq!(winner.branch_ordinal, 1);
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("select workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "right:go"
        );
    });
}

#[test]
fn select_timer_winner_cancels_in_flight_activity() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_activity_timer>(
                "wf/select-activity-timer",
                "workflows",
                number(20),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(select_activity_timer)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let claimed_activity = backend
            .claim_activity_task(
                WorkerId::new("late-activity-worker"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("tests.double")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("activity task");
        backend.advance_time(Duration::from_millis(10));
        assert_eq!(worker.run_timers_once().await.unwrap(), 1);
        assert!(worker.run_workflow_once().await.unwrap());

        let late_completion = backend
            .complete_activity(CompleteActivityRequest {
                claim: claimed_activity.claim,
                result: durust::encode_payload(&40_u64).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            late_completion,
            durust::CompleteActivityOutcome::AlreadyCompleted
        );

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 6);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        assert!(matches!(history[2].data, HistoryEventData::TimerStarted(_)));
        assert!(matches!(history[3].data, HistoryEventData::TimerFired(_)));
        let HistoryEventData::SelectWinner(winner) = &history[4].data else {
            panic!("expected SelectWinner");
        };
        assert_eq!(winner.branch_ordinal, 1);
        let HistoryEventData::WorkflowCompleted { result } = &history[5].data else {
            panic!("select workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 0);
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ActivityCompleted(_)))
        );
    });
}

#[test]
fn select_accepts_more_than_three_branches() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_fourth_signal>("wf/select-four", "workflows", unit())
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(select_fourth_signal)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        client
            .signal_workflow("wf/select-four", "ready", "signal/select/four", "go")
            .await
            .unwrap();
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 7);
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(event.data, HistoryEventData::TimerStarted(_)))
                .count(),
            3
        );
        let HistoryEventData::SelectWinner(winner) = &history[5].data else {
            panic!("expected SelectWinner");
        };
        assert_eq!(winner.branch_ordinal, 3);
        let HistoryEventData::WorkflowCompleted { result } = &history[6].data else {
            panic!("select workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "signal:go"
        );
    });
}

#[test]
fn select_replays_recorded_winner_after_worker_crash() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_then_wait>("wf/select-replay-winner", "workflows", number(10))
            .await
            .unwrap();
        let mut original_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(select_then_wait)
            .build();

        assert!(original_worker.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::from_millis(10));
        assert_eq!(original_worker.run_timers_once().await.unwrap(), 1);
        assert!(original_worker.run_workflow_once().await.unwrap());
        drop(original_worker);

        client
            .signal_workflow(
                "wf/select-replay-winner",
                "after",
                "signal/select/replay-after",
                "done",
            )
            .await
            .unwrap();
        let mut replay_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .history_chunk_events(1)
            .register_workflow(select_then_wait)
            .build();
        assert!(replay_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(event.data, HistoryEventData::SelectWinner(_)))
                .count(),
            1
        );
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("completed event").data
        else {
            panic!("select replay workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "timer:done"
        );
    });
}

#[test]
fn select_branch_reorder_is_detected_on_replay() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<select_then_wait>("wf/select-reorder", "workflows", number(10))
            .await
            .unwrap();
        let mut original_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(select_then_wait)
            .build();

        assert!(original_worker.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::from_millis(10));
        assert_eq!(original_worker.run_timers_once().await.unwrap(), 1);
        assert!(original_worker.run_workflow_once().await.unwrap());
        drop(original_worker);

        client
            .signal_workflow("wf/select-reorder", "after", "signal/select/after", "done")
            .await
            .unwrap();
        let mut changed_worker = Worker::builder(backend)
            .workflow_task_queue("workflows")
            .register_workflow(select_then_wait_reordered)
            .nondeterminism_retry_backoff(Duration::from_millis(25))
            .build();
        let err = changed_worker.run_workflow_once().await.unwrap_err();
        assert!(matches!(err, durust::Error::Nondeterminism(_)));
    });
}

#[test]
fn get_version_returns_default_for_old_history_without_marker() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<version_original>("wf/version-old", "workflows", unit())
            .await
            .unwrap();
        let mut old_worker = version_worker(backend.clone(), version_original);
        assert!(old_worker.run_workflow_once().await.unwrap());
        assert!(old_worker.run_activity_once().await.unwrap());
        drop(old_worker);

        let mut patched_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(version_patched)
            .register_activity(version_activity_a)
            .register_activity(version_activity_b)
            .build();
        assert!(patched_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::VersionMarker(_)))
        );
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("expected workflow completion");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "a");
    });
}

#[test]
fn patched_records_marker_and_takes_new_branch_for_new_history() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<version_patched>("wf/version-new", "workflows", unit())
            .await
            .unwrap();
        let mut worker = version_worker(backend.clone(), version_patched);
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::VersionMarker(marker) = &history[1].data else {
            panic!("expected VersionMarker");
        };
        assert_eq!(marker.change_id, "replace-a-with-b");
        assert_eq!(marker.version, 1);
        assert_eq!(marker.command_id.seq, durust::CommandSeq(1));
        let scheduled = scheduled_activity(&history, 2);
        assert_eq!(
            scheduled.activity_name,
            ActivityName::new("tests.version-b")
        );
        assert_eq!(scheduled.command_id.seq, durust::CommandSeq(2));

        let versions = backend
            .workflow_change_versions(durust::WorkflowChangeVersionsRequest {
                namespace: Namespace::default(),
                workflow_id: None,
                run_id: Some(run_id),
                change_id: Some("replace-a-with-b".to_owned()),
            })
            .await
            .unwrap();
        assert_eq!(versions.records.len(), 1);
        assert_eq!(
            versions.records[0].marker_kind,
            durust::WorkflowChangeMarkerKind::Version
        );
        assert!(!versions.safe_to_remove());
    });
}

#[test]
fn recorded_version_is_stable_across_streamed_replay() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<version_patched>("wf/version-replay", "workflows", unit())
            .await
            .unwrap();
        let mut first_worker = version_worker(backend.clone(), version_patched);
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);

        let mut replay_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(version_patched)
            .register_activity(version_activity_a)
            .register_activity(version_activity_b)
            .build();
        assert!(replay_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(event.data, HistoryEventData::VersionMarker(_)))
                .count(),
            1
        );
        let HistoryEventData::WorkflowCompleted { result } = &history[4].data else {
            panic!("expected workflow completion");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "b");
    });
}

#[test]
fn unsupported_min_version_aborts_task_without_workflow_failed() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<version_patched>("wf/version-unsupported", "workflows", unit())
            .await
            .unwrap();
        let mut first_worker = version_worker(backend.clone(), version_patched);
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);

        let mut min_two_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .nondeterminism_retry_backoff(Duration::from_millis(25))
            .register_workflow(version_min_two)
            .register_activity(version_activity_b)
            .build();
        let err = min_two_worker.run_workflow_once().await.unwrap_err();
        assert!(matches!(
            err,
            durust::Error::UnsupportedWorkflowVersion { .. }
        ));
        let history = stream_all(&backend, &run_id).await;
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
        );
    });
}

#[test]
fn deprecate_patch_bridges_existing_patched_histories() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<version_patched>("wf/version-deprecated", "workflows", unit())
            .await
            .unwrap();
        let mut first_worker = version_worker(backend.clone(), version_patched);
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);

        let mut deprecated_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(version_deprecated)
            .register_activity(version_activity_b)
            .build();
        assert!(deprecated_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::DeprecatedPatchMarker(_)))
        );
        let HistoryEventData::WorkflowCompleted { result } = &history[4].data else {
            panic!("expected workflow completion");
        };
        assert_eq!(durust::decode_payload::<String>(result).unwrap(), "b");
    });
}

#[test]
fn deprecate_patch_records_bridge_marker_for_new_histories() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<version_deprecated>("wf/version-deprecated-new", "workflows", unit())
            .await
            .unwrap();
        let mut worker = version_worker(backend.clone(), version_deprecated);
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::DeprecatedPatchMarker(marker) = &history[1].data else {
            panic!("expected DeprecatedPatchMarker");
        };
        assert_eq!(marker.patch_id, "replace-a-with-b");
        assert_eq!(marker.command_id.seq, durust::CommandSeq(1));
        let scheduled = scheduled_activity(&history, 2);
        assert_eq!(
            scheduled.activity_name,
            ActivityName::new("tests.version-b")
        );

        let versions = backend
            .workflow_change_versions(durust::WorkflowChangeVersionsRequest {
                namespace: Namespace::default(),
                workflow_id: None,
                run_id: Some(run_id),
                change_id: Some("replace-a-with-b".to_owned()),
            })
            .await
            .unwrap();
        assert_eq!(
            versions.records[0].marker_kind,
            durust::WorkflowChangeMarkerKind::DeprecatedPatch
        );
    });
}

#[test]
fn removing_patch_bridge_too_early_is_nondeterministic() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<version_patched>("wf/version-removed-too-early", "workflows", unit())
            .await
            .unwrap();
        let mut first_worker = version_worker(backend.clone(), version_patched);
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);

        let mut removed_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .nondeterminism_retry_backoff(Duration::from_millis(25))
            .register_workflow(version_removed)
            .register_activity(version_activity_b)
            .build();
        let err = removed_worker.run_workflow_once().await.unwrap_err();
        assert!(matches!(err, durust::Error::Nondeterminism(_)));
        let history = stream_all(&backend, &run_id).await;
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
        );
    });
}

#[test]
fn workflow_default_activity_options_apply_to_scheduled_activity() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<default_activity_options_workflow>(
                "wf/default-activity-options",
                "workflows",
                number(10),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("preferred-activities")
            .register_workflow(default_activity_options_workflow)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let scheduled = scheduled_activity(&history, 1);
        assert_eq!(scheduled.task_queue, TaskQueue::new("preferred-activities"));
        assert_eq!(
            scheduled.retry_policy,
            durust::RetryPolicy::exponential().max_attempts(5)
        );

        let unclaimable_on_worker_fallback = backend
            .claim_activity_task(
                WorkerId::new("fallback-worker"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("tests.double")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(unclaimable_on_worker_fallback.is_none());
    });
}

#[test]
fn per_call_activity_options_override_workflow_defaults() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<override_activity_options_workflow>(
                "wf/override-activity-options",
                "workflows",
                number(10),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("fallback-activities")
            .register_workflow(override_activity_options_workflow)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let scheduled = scheduled_activity(&history, 1);
        assert_eq!(scheduled.task_queue, TaskQueue::new("override-activities"));
        assert_eq!(scheduled.retry_policy, durust::RetryPolicy::none());
    });
}

#[test]
fn durust_errors_are_serializable_with_failure_details() {
    let error = durust::Error::non_retryable("tests.validation", "validation failed")
        .with_details(&NumberInput { value: 42 })
        .unwrap();
    let bytes = rmp_serde::to_vec_named(&error).unwrap();
    let decoded: durust::Error = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(decoded, error);

    let durust::Error::Application(failure) = decoded else {
        panic!("expected application failure");
    };
    assert!(failure.non_retryable);
    let details = failure.details.expect("failure details");
    assert_eq!(
        durust::decode_payload::<NumberInput>(&details).unwrap(),
        NumberInput { value: 42 }
    );
}

#[test]
fn workflow_default_activity_options_survive_cached_wake_and_crash_replay() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<cached_default_activity_options_workflow>(
                "wf/cached-default-activity-options",
                "workflows",
                number(4),
            )
            .await
            .unwrap();
        let mut cached_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("sticky-activities")
            .register_workflow(cached_default_activity_options_workflow)
            .register_activity(double)
            .build();

        assert!(cached_worker.run_workflow_once().await.unwrap());
        assert!(cached_worker.run_activity_once().await.unwrap());
        assert!(cached_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let second = scheduled_activity(&history, 3);
        assert_eq!(second.task_queue, TaskQueue::new("sticky-activities"));
        assert_eq!(
            second.retry_policy,
            durust::RetryPolicy::exponential().max_attempts(7)
        );

        drop(cached_worker);
        let mut replay_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("sticky-activities")
            .history_chunk_events(1)
            .register_workflow(cached_default_activity_options_workflow)
            .register_activity(double)
            .build();
        assert!(replay_worker.run_activity_once().await.unwrap());
        assert!(replay_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 6);
        let HistoryEventData::WorkflowCompleted { result } = &history[5].data else {
            panic!("workflow did not complete after replay");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 16);
    });
}

#[test]
fn query_projection_reads_latest_committed_publish_without_replay() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<query_projection_workflow>(
                "wf/query-projection",
                "workflows",
                number(41),
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .query_projection::<query_projection_workflow>("wf/query-projection")
                .await
                .unwrap(),
            None
        );

        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(query_projection_workflow)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());

        let view = client
            .query_projection::<query_projection_workflow>("wf/query-projection")
            .await
            .unwrap()
            .expect("committed projection");
        assert_eq!(
            view,
            QueryView {
                status: "started".to_owned(),
                value: 41,
            }
        );
        assert_eq!(query_status(&view), "started");

        client
            .signal_workflow(
                "wf/query-projection",
                "advance",
                "signal/query/advance",
                "done",
            )
            .await
            .unwrap();
        let claimed = backend
            .claim_workflow_task(
                WorkerId::new("query-concurrent-reader"),
                ClaimWorkflowTaskOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("workflows"),
                    registered_workflow_types: vec![WorkflowType::new("tests.query-projection", 1)],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("signal-ready workflow task");
        let still_committed = client
            .query_projection::<query_projection_workflow>("wf/query-projection")
            .await
            .unwrap()
            .expect("committed projection");
        assert_eq!(still_committed.status, "started");
        backend
            .release_workflow_task(
                claimed.claim,
                durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::CacheEvicted),
            )
            .await
            .unwrap();

        assert!(worker.run_workflow_once().await.unwrap());
        let view = client
            .query_projection::<query_projection_workflow>("wf/query-projection")
            .await
            .unwrap()
            .expect("updated projection");
        assert_eq!(
            view,
            QueryView {
                status: "done".to_owned(),
                value: 42,
            }
        );

        let history = stream_all(&backend, &run_id).await;
        assert!(matches!(
            history.last().expect("terminal event").data,
            HistoryEventData::WorkflowCompleted { .. }
        ));
    });
}

#[test]
fn provider_configured_json_codec_round_trips_typed_runtime_apis() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new().codec(durust::CodecId::Json),
        );
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<provider_json_codec_workflow>(
                "wf/provider-json-codec",
                "json-workflows",
                number(21),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("json-workflows")
            .activity_task_queue("json-activities")
            .register_workflow(provider_json_codec_workflow)
            .register_activity(double)
            .build();

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected WorkflowStarted");
        };
        assert_eq!(input.codec(), durust::CodecId::Json);
        assert_eq!(
            durust::decode_payload::<NumberInput>(input).unwrap(),
            NumberInput { value: 21 }
        );

        assert!(worker.run_workflow_once().await.unwrap());
        let query = backend
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("wf/provider-json-codec"),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = query else {
            panic!("expected started query projection");
        };
        assert_eq!(payload.codec(), durust::CodecId::Json);
        assert_eq!(
            durust::decode_payload::<QueryView>(&payload).unwrap(),
            QueryView {
                status: "started".to_owned(),
                value: 21,
            }
        );
        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::ActivityScheduled(scheduled) = &history[1].data else {
            panic!("expected ActivityScheduled");
        };
        assert_eq!(scheduled.input.codec(), durust::CodecId::Json);
        assert_eq!(
            durust::decode_payload::<NumberInput>(&scheduled.input).unwrap(),
            NumberInput { value: 21 }
        );

        assert!(worker.run_activity_once().await.unwrap());
        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::ActivityCompleted(completed) = &history[2].data else {
            panic!("expected ActivityCompleted");
        };
        assert_eq!(completed.result.codec(), durust::CodecId::Json);
        assert_eq!(
            durust::decode_payload::<u64>(&completed.result).unwrap(),
            42
        );

        assert!(worker.run_workflow_once().await.unwrap());
        client
            .signal_workflow(
                "wf/provider-json-codec",
                "advance",
                "signal/provider-json-codec/advance",
                "done",
            )
            .await
            .unwrap();
        let signal = backend
            .read_signal_inbox(durust::ReadSignalInboxRequest {
                run_id: run_id.clone(),
                signal_name: durust::SignalName::new("advance"),
            })
            .await
            .unwrap()
            .expect("signal payload");
        assert_eq!(signal.payload.codec(), durust::CodecId::Json);
        assert_eq!(
            durust::decode_payload::<String>(&signal.payload).unwrap(),
            "done"
        );

        assert!(worker.run_workflow_once().await.unwrap());
        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::SignalConsumed(consumed) = &history[3].data else {
            panic!("expected SignalConsumed");
        };
        assert_eq!(consumed.payload.codec(), durust::CodecId::Json);
        let HistoryEventData::WorkflowCompleted { result } = &history[4].data else {
            panic!("expected WorkflowCompleted");
        };
        assert_eq!(result.codec(), durust::CodecId::Json);
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 43);

        let view = client
            .query_projection::<provider_json_codec_workflow>("wf/provider-json-codec")
            .await
            .unwrap()
            .expect("updated projection");
        assert_eq!(
            view,
            QueryView {
                status: "done".to_owned(),
                value: 42,
            }
        );
    });
}

#[test]
fn continue_as_new_starts_fresh_run_with_compacted_input() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let first_run_id = client
            .start_workflow::<continue_as_new_workflow>(
                "wf/continue-as-new",
                "workflows",
                ContinueInput {
                    remaining: 2,
                    total: 10,
                },
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(continue_as_new_workflow)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let second_run_id = client
            .start_workflow::<continue_as_new_workflow>(
                "wf/continue-as-new",
                "workflows",
                ContinueInput {
                    remaining: 99,
                    total: 99,
                },
            )
            .await
            .unwrap();
        assert_ne!(first_run_id, second_run_id);

        assert!(worker.run_workflow_once().await.unwrap());
        let third_run_id = client
            .start_workflow::<continue_as_new_workflow>(
                "wf/continue-as-new",
                "workflows",
                ContinueInput {
                    remaining: 99,
                    total: 99,
                },
            )
            .await
            .unwrap();
        assert_ne!(second_run_id, third_run_id);

        assert!(worker.run_workflow_once().await.unwrap());
        assert!(!worker.run_workflow_once().await.unwrap());

        let first_history = stream_all(&backend, &first_run_id).await;
        assert_eq!(first_history.len(), 2);
        let HistoryEventData::WorkflowContinuedAsNew { input } = &first_history[1].data else {
            panic!("expected first run to continue as new");
        };
        assert_eq!(
            durust::decode_payload::<ContinueInput>(input).unwrap(),
            ContinueInput {
                remaining: 1,
                total: 11
            }
        );

        let final_history = stream_all(&backend, &third_run_id).await;
        assert_eq!(final_history.len(), 2);
        let HistoryEventData::WorkflowStarted { input, .. } = &final_history[0].data else {
            panic!("expected compacted start input");
        };
        assert_eq!(
            durust::decode_payload::<ContinueInput>(input).unwrap(),
            ContinueInput {
                remaining: 0,
                total: 12
            }
        );
        let HistoryEventData::WorkflowCompleted { result } = &final_history[1].data else {
            panic!("expected final run completion");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 12);
    });
}

#[test]
fn query_projection_survives_until_continued_run_publishes_replacement() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<continue_query_workflow>(
                "wf/continue-query",
                "workflows",
                ContinueInput {
                    remaining: 1,
                    total: 20,
                },
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(continue_query_workflow)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let continuing = client
            .query_projection::<continue_query_workflow>("wf/continue-query")
            .await
            .unwrap()
            .expect("projection from continued run");
        assert_eq!(
            continuing,
            QueryView {
                status: "continuing".to_owned(),
                value: 20,
            }
        );

        assert!(worker.run_workflow_once().await.unwrap());
        let done = client
            .query_projection::<continue_query_workflow>("wf/continue-query")
            .await
            .unwrap()
            .expect("replacement projection");
        assert_eq!(
            done,
            QueryView {
                status: "done".to_owned(),
                value: 21,
            }
        );
    });
}

#[test]
fn parent_waits_for_child_that_continues_as_new() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let parent_run_id = client
            .start_workflow::<parent_waits_continued_child>(
                "wf/parent-continued-child",
                "workflows",
                ContinueInput {
                    remaining: 1,
                    total: 30,
                },
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(parent_waits_continued_child)
            .register_workflow(continued_child_workflow)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.child_workflow_starts_dispatched, 1);
        assert!(stats.workflow_tasks >= 4);

        let parent_history = stream_all(&backend, &parent_run_id).await;
        assert!(
            parent_history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ChildWorkflowCompleted(_)))
        );
        let HistoryEventData::WorkflowCompleted { result } =
            &parent_history.last().expect("parent terminal").data
        else {
            panic!("parent did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 31);
    });
}

#[test]
fn timer_fires_after_virtual_time_and_replays_after_worker_crash() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<sleep_then_return>("wf/timer-recovery", "workflows", number(50))
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(sleep_then_return)
            .build();

        assert!(first_worker.run_workflow_once().await.unwrap());
        assert_eq!(first_worker.run_timers_once().await.unwrap(), 0);
        backend.advance_time(Duration::from_millis(49));
        assert_eq!(first_worker.run_timers_once().await.unwrap(), 0);
        drop(first_worker);

        backend.advance_time(Duration::from_millis(1));
        let mut recovered_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .history_chunk_events(1)
            .register_workflow(sleep_then_return)
            .build();
        assert_eq!(recovered_worker.run_timers_once().await.unwrap(), 1);
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(history[1].data, HistoryEventData::TimerStarted(_)));
        assert!(matches!(history[2].data, HistoryEventData::TimerFired(_)));
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("timer workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 51);
    });
}

#[test]
fn failed_activity_records_failure_and_workflow_failure_on_replay() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<failing_activity_workflow>("wf/activity-failure", "workflows", unit())
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(failing_activity_workflow)
            .register_activity(fail_activity)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        assert!(worker.run_activity_once().await.unwrap());
        drop(worker);

        let mut replay_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(failing_activity_workflow)
            .register_activity(fail_activity)
            .build();
        assert!(replay_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        let HistoryEventData::ActivityFailed(failed) = &history[2].data else {
            panic!("expected ActivityFailed");
        };
        assert!(failed.failure.message.contains("boom"));
        let HistoryEventData::WorkflowFailed { failure } = &history[3].data else {
            panic!("expected WorkflowFailed");
        };
        assert!(failure.message.contains("boom"));
    });
}

#[test]
fn retryable_activity_failure_does_not_append_failure_history() {
    block_on(async {
        *FLAKY_ATTEMPTS.lock().unwrap() = 0;
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<retry_activity_workflow>("wf/activity-retry", "workflows", unit())
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(retry_activity_workflow)
            .register_activity(flaky_activity)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 2);
        assert_eq!(stats.activity_tasks, 2);
        assert_eq!(*FLAKY_ATTEMPTS.lock().unwrap(), 2);

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        assert!(matches!(
            history[2].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        assert!(!history.iter().any(|event| matches!(
            event.data,
            HistoryEventData::ActivityFailed(_) | HistoryEventData::WorkflowFailed { .. }
        )));
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("retry workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 7);
    });
}

#[test]
fn non_retryable_activity_failure_skips_retries_and_restores_failure() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<non_retryable_activity_workflow>(
                "wf/activity-non-retryable",
                "workflows",
                unit(),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(non_retryable_activity_workflow)
            .register_activity(non_retryable_activity)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 2);
        assert_eq!(stats.activity_tasks, 1);

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        let HistoryEventData::ActivityFailed(failed) = &history[2].data else {
            panic!("expected ActivityFailed");
        };
        assert_eq!(failed.failure.error_type, "tests.validation");
        assert_eq!(failed.failure.message, "validation failed");
        assert!(failed.failure.non_retryable);
        let HistoryEventData::WorkflowFailed { failure } = &history[3].data else {
            panic!("expected WorkflowFailed");
        };
        assert_eq!(failure.error_type, "tests.validation");
        assert_eq!(failure.message, "validation failed");
        assert!(failure.non_retryable);
    });
}

#[test]
fn activity_timeout_records_timeout_and_fails_workflow_on_replay() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<timeout_activity_workflow>(
                "wf/activity-timeout",
                "workflows",
                number(5),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(timeout_activity_workflow)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        backend.advance_time(Duration::from_millis(9));
        assert_eq!(worker.run_activity_timeouts_once().await.unwrap(), 0);
        backend.advance_time(Duration::from_millis(1));
        assert_eq!(worker.run_activity_timeouts_once().await.unwrap(), 1);
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
            panic!("expected ActivityTimedOut");
        };
        assert!(timed_out.message.contains("timed out"));
        let HistoryEventData::WorkflowFailed { failure } = &history[3].data else {
            panic!("expected WorkflowFailed");
        };
        assert!(failure.message.contains("timed out"));
    });
}

#[test]
fn signal_before_wait_buffers_and_completes_without_extra_task() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<await_signal>("wf/signal-before", "workflows", unit())
            .await
            .unwrap();
        let outcome = client
            .signal_workflow("wf/signal-before", "ready", "signal-before-1", "buffered")
            .await
            .unwrap();
        assert_eq!(outcome, durust::SignalWorkflowOutcome::Accepted);
        let duplicate = client
            .signal_workflow("wf/signal-before", "ready", "signal-before-1", "ignored")
            .await
            .unwrap();
        assert_eq!(duplicate, durust::SignalWorkflowOutcome::Duplicate);

        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(await_signal)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 3);
        assert!(matches!(
            history[1].data,
            HistoryEventData::SignalConsumed(_)
        ));
        let HistoryEventData::WorkflowCompleted { result } = &history[2].data else {
            panic!("signal workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "buffered"
        );
    });
}

#[test]
fn signal_after_wait_wakes_and_consumes_atomically() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<await_signal>("wf/signal-after", "workflows", unit())
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .register_workflow(await_signal)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        let waiting_history = stream_all(&backend, &run_id).await;
        assert_eq!(waiting_history.len(), 1);

        let outcome = client
            .signal_workflow("wf/signal-after", "ready", "signal-after-1", "delivered")
            .await
            .unwrap();
        assert_eq!(outcome, durust::SignalWorkflowOutcome::Accepted);
        assert!(worker.run_workflow_once().await.unwrap());
        assert!(!worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 3);
        let HistoryEventData::SignalConsumed(consumed) = &history[1].data else {
            panic!("expected SignalConsumed");
        };
        assert_eq!(consumed.signal_id, durust::SignalId::new("signal-after-1"));
        let HistoryEventData::WorkflowCompleted { result } = &history[2].data else {
            panic!("signal workflow did not complete");
        };
        assert_eq!(
            durust::decode_payload::<String>(result).unwrap(),
            "delivered"
        );
    });
}

#[test]
fn worker_loop_runs_workflow_and_activity_until_idle() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/loop", "workflows", number(8))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("loop-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 2);
        assert_eq!(stats.activity_tasks, 1);

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 17);
    });
}

#[test]
fn configured_local_activity_preference_runs_before_remote_worker_can_claim() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/local-activity", "workflows", number(5))
            .await
            .unwrap();
        let mut workflow_worker = Worker::builder(backend.clone())
            .worker_id("workflow-with-local-activity")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .max_local_activities_per_workflow_task(1)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        let mut remote_worker = Worker::builder(backend.clone())
            .worker_id("remote-activity-worker")
            .workflow_task_queue("unused")
            .activity_task_queue("activities")
            .register_activity(double)
            .build();

        assert!(workflow_worker.run_workflow_once().await.unwrap());
        let history_after_local = stream_all(&backend, &run_id).await;
        assert_eq!(history_after_local.len(), 3);
        assert!(matches!(
            history_after_local[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        assert!(matches!(
            history_after_local[2].data,
            HistoryEventData::ActivityCompleted(_)
        ));
        assert!(!remote_worker.run_activity_once().await.unwrap());

        assert!(workflow_worker.run_workflow_once().await.unwrap());
        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("workflow did not complete after local activity");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 11);
    });
}

#[test]
fn zero_local_activity_capacity_falls_back_to_remote_worker() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/remote-fallback", "workflows", number(6))
            .await
            .unwrap();
        let mut workflow_worker = Worker::builder(backend.clone())
            .worker_id("workflow-without-local-capacity")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .max_local_activities_per_workflow_task(0)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        let mut remote_worker = Worker::builder(backend.clone())
            .worker_id("remote-fallback-worker")
            .workflow_task_queue("unused")
            .activity_task_queue("activities")
            .register_activity(double)
            .build();

        assert!(workflow_worker.run_workflow_once().await.unwrap());
        let history_after_schedule = stream_all(&backend, &run_id).await;
        assert_eq!(history_after_schedule.len(), 2);
        assert!(matches!(
            history_after_schedule[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));
        assert!(remote_worker.run_activity_once().await.unwrap());
        assert!(workflow_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("workflow did not complete after remote fallback");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 13);
    });
}

#[test]
fn activity_map_workflow_runs_with_compact_history() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<activity_map_sum>(
                "wf/activity-map-sum",
                "workflows",
                values(vec![1, 2, 3]),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("map-activities")
            .register_workflow(activity_map_sum)
            .register_activity(map_double)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 2);
        assert_eq!(stats.activity_tasks, 3);

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityMapScheduled(_)
        ));
        assert!(matches!(
            history[2].data,
            HistoryEventData::ActivityMapCompleted(_)
        ));
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ActivityCompleted(_)))
        );
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("activity map workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 12);
    });
}

#[test]
fn child_workflow_map_runs_with_compact_parent_history() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<child_workflow_map_sum>(
                "wf/child-workflow-map-sum",
                "workflows",
                values(vec![1, 2, 3]),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(child_workflow_map_sum)
            .register_workflow(child_double_workflow)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 5);
        assert_eq!(stats.activity_tasks, 0);

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ChildWorkflowMapScheduled(_)
        ));
        assert!(matches!(
            history[2].data,
            HistoryEventData::ChildWorkflowMapCompleted(_)
        ));
        assert!(!history.iter().any(|event| matches!(
            event.data,
            HistoryEventData::ChildWorkflowStarted(_)
                | HistoryEventData::ChildWorkflowCompleted(_)
                | HistoryEventData::ChildWorkflowFailed(_)
                | HistoryEventData::ChildWorkflowCancelled(_)
        )));
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("child workflow map workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 12);
    });
}

#[test]
fn child_workflow_map_replay_rejects_changed_fingerprint_fields() {
    block_on(async {
        assert_child_workflow_map_replay_change_is_nondeterministic(
            "child-type",
            child_workflow_map_sum_changed_child_type,
        )
        .await;
        assert_child_workflow_map_replay_change_is_nondeterministic(
            "input-manifest",
            child_workflow_map_sum_changed_input_manifest,
        )
        .await;
        assert_child_workflow_map_replay_change_is_nondeterministic(
            "prefix",
            child_workflow_map_sum_changed_prefix,
        )
        .await;
        assert_child_workflow_map_replay_change_is_nondeterministic(
            "task-queue",
            child_workflow_map_sum_changed_task_queue,
        )
        .await;
        assert_child_workflow_map_replay_change_is_nondeterministic(
            "max-in-flight",
            child_workflow_map_sum_changed_max_in_flight,
        )
        .await;
        assert_child_workflow_map_replay_change_is_nondeterministic(
            "parent-close-policy",
            child_workflow_map_sum_changed_parent_close_policy,
        )
        .await;
        assert_child_workflow_map_replay_change_is_nondeterministic(
            "failure-mode",
            child_workflow_map_sum_changed_failure_mode,
        )
        .await;
    });
}

async fn assert_child_workflow_map_replay_change_is_nondeterministic<W>(
    case: &str,
    changed_workflow: W,
) where
    W: durust::Workflow<Input = ValuesInput, Output = u64> + Default,
{
    let backend = MemoryBackend::new();
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<child_workflow_map_sum>(
            format!("wf/child-workflow-map-replay/{case}"),
            "workflows",
            values(vec![1, 2]),
        )
        .await
        .unwrap();
    let mut original_worker = Worker::builder(backend.clone())
        .worker_id(format!("original-child-map-{case}"))
        .workflow_task_queue("workflows")
        .register_workflow(child_workflow_map_sum)
        .build();
    assert!(original_worker.run_workflow_once().await.unwrap());

    let after_schedule = stream_all(&backend, &run_id).await;
    assert_eq!(after_schedule.len(), 2);
    assert!(matches!(
        after_schedule[1].data,
        HistoryEventData::ChildWorkflowMapScheduled(_)
    ));

    assert_eq!(
        original_worker
            .run_child_workflow_starts_once()
            .await
            .unwrap(),
        2
    );
    drop(original_worker);

    let mut child_worker = Worker::builder(backend.clone())
        .worker_id(format!("child-map-child-{case}"))
        .workflow_task_queue("workflows")
        .register_workflow(child_double_workflow)
        .build();
    let child_stats = child_worker.run_until_idle().await.unwrap();
    assert_eq!(child_stats.workflow_tasks, 2);
    drop(child_worker);

    let before_changed_replay = stream_all(&backend, &run_id).await;
    assert!(
        before_changed_replay
            .iter()
            .any(|event| { matches!(event.data, HistoryEventData::ChildWorkflowMapCompleted(_)) })
    );
    assert!(
        !before_changed_replay
            .iter()
            .any(|event| matches!(event.data, HistoryEventData::WorkflowCompleted { .. }))
    );

    let mut changed_worker = Worker::builder(backend.clone())
        .worker_id(format!("changed-child-map-{case}"))
        .workflow_task_queue("workflows")
        .history_chunk_events(1)
        .register_workflow(changed_workflow)
        .build();
    let err = changed_worker.run_workflow_once().await.unwrap_err();
    assert!(
        matches!(err, durust::Error::Nondeterminism(_)),
        "{case} should be nondeterministic, got {err:?}"
    );

    let history = stream_all(&backend, &run_id).await;
    assert!(
        !history
            .iter()
            .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
    );
    assert!(
        !history
            .iter()
            .any(|event| matches!(event.data, HistoryEventData::WorkflowCompleted { .. }))
    );
}

#[test]
fn configured_local_activity_preference_applies_to_activity_map_items() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<activity_map_sum>(
                "wf/local-activity-map",
                "workflows",
                values(vec![1, 2, 3]),
            )
            .await
            .unwrap();
        let mut workflow_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("map-activities")
            .max_local_activities_per_workflow_task(2)
            .register_workflow(activity_map_sum)
            .register_activity(map_double)
            .build();

        assert!(workflow_worker.run_workflow_once().await.unwrap());
        let remote_item = backend
            .claim_activity_task(
                WorkerId::new("remote-map-worker"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("map-activities"),
                    registered_activity_names: vec![ActivityName::new("tests.map-double")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap()
            .expect("remaining map item after local slots");
        let map_item = remote_item.task.map_item.as_ref().expect("map item");
        assert_eq!(map_item.item_ordinal, 2);
        assert_eq!(
            durust::decode_payload::<NumberInput>(&remote_item.task.input)
                .unwrap()
                .value,
            3
        );

        backend
            .complete_activity(CompleteActivityRequest {
                claim: remote_item.claim,
                result: durust::encode_payload(&6_u64).unwrap(),
            })
            .await
            .unwrap();
        assert!(workflow_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("activity map workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 12);
    });
}

#[test]
fn worker_crash_recovers_by_streaming_history() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/replay", "workflows", number(7))
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .worker_id("worker-before-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);

        let mut recovered_worker = Worker::builder(backend.clone())
            .worker_id("worker-after-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("workflow did not complete after replay");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 15);
    });
}

#[test]
fn recovery_replays_prefetched_history_in_configured_chunks() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/chunked-replay", "workflows", number(5))
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);
        backend.clear_stream_requests();

        let mut recovered_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        assert!(backend.stream_requests().is_empty());
        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
    });
}

#[test]
fn cached_workflow_wake_uses_prefetched_tail() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/cached-wake", "workflows", number(6))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());
        assert!(worker.run_activity_once().await.unwrap());
        backend.clear_stream_requests();

        assert!(worker.run_workflow_once().await.unwrap());

        assert!(backend.stream_requests().is_empty());
        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
    });
}

#[test]
fn cached_workflow_wake_ignores_cold_recovery_saturation() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>(
                "wf/cached-recovery-saturated",
                "workflows",
                number(6),
            )
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .max_concurrent_recoveries(0)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();

        assert!(worker.run_workflow_once().await.unwrap());
        assert!(worker.run_activity_once().await.unwrap());
        backend.clear_stream_requests();

        assert!(worker.run_workflow_once().await.unwrap());

        assert!(backend.stream_requests().is_empty());
        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
    });
}

#[test]
fn cold_recovery_defers_before_streaming_when_admission_is_unavailable() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/recovery-admission", "workflows", number(7))
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);
        backend.clear_stream_requests();

        let mut recovered_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .max_concurrent_recoveries(0)
            .recovery_defer_delay(Duration::from_millis(25))
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        assert!(backend.stream_requests().is_empty());
        let hidden = backend
            .claim_workflow_task(
                WorkerId::new("before-recovery-admission-delay"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(hidden.is_none());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 3);
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
        );

        std::thread::sleep(Duration::from_millis(40));
        let visible = backend
            .claim_workflow_task(
                WorkerId::new("after-recovery-admission-delay"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(visible.is_some());
    });
}

#[test]
fn cold_recovery_event_budget_defers_without_appending_failure() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/recovery-event-budget", "workflows", number(5))
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);
        backend.clear_stream_requests();

        let mut recovered_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .recovery_replay_event_budget(1)
            .recovery_defer_delay(Duration::from_millis(25))
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        assert!(backend.stream_requests().is_empty());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 3);
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
        );

        let hidden = backend
            .claim_workflow_task(
                WorkerId::new("before-recovery-budget-delay"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(hidden.is_none());
    });
}

#[test]
fn cold_recovery_byte_budget_clamps_stream_request_and_defers() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/recovery-byte-budget", "workflows", number(5))
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);
        backend.clear_stream_requests();

        let mut recovered_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_bytes(usize::MAX)
            .recovery_replay_byte_budget(1)
            .recovery_defer_delay(Duration::from_millis(25))
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        assert!(backend.stream_requests().is_empty());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 3);
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
        );
    });
}

#[test]
fn provider_backpressure_defers_cold_recovery_without_workflow_failure() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new()).without_claim_prefetch();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/recovery-backpressure", "workflows", number(9))
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);
        backend.clear_stream_requests();
        backend.backpressure_next_replay_stream(Duration::from_millis(25));

        let mut recovered_worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 3);
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
        );

        let hidden = backend
            .claim_workflow_task(
                WorkerId::new("before-provider-backpressure-delay"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(hidden.is_none());

        std::thread::sleep(Duration::from_millis(40));
        let visible = backend
            .claim_workflow_task(
                WorkerId::new("after-provider-backpressure-delay"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(visible.is_some());
    });
}

// Builds a two-slot batch worker so run_workflow_batch_once takes the batched
// claim/prepare/commit path instead of delegating to run_workflow_once.
fn batch_error_worker(backend: RecordingBackend) -> Worker<RecordingBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .max_concurrent_workflow_tasks(2)
        .workflow_task_prefetch_limit(2)
        .workflow_task_commit_batch_size(2)
        .register_workflow(double_plus_one)
        .register_activity(double)
        .build()
}

#[test]
fn claim_is_released_when_current_time_fails_before_prepare() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/current-time-error", "workflows", number(9))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();

        backend.fail_next_current_time();
        let err = worker.run_workflow_once().await.unwrap_err();
        assert!(matches!(err, durust::Error::Backend(_)));

        // The failed prepare released its claim immediately: the task is
        // claimable again without waiting for lease expiry.
        let reclaimed = backend
            .claim_workflow_task(
                WorkerId::new("after-current-time-error"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap()
            .expect("claim released by failed prepare");
        backend
            .release_workflow_task(
                reclaimed.claim,
                durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::CacheEvicted),
            )
            .await
            .unwrap();

        worker.run_until_idle().await.unwrap();
        let history = stream_all(&backend, &run_id).await;
        assert!(matches!(
            history.last().unwrap().data,
            HistoryEventData::WorkflowCompleted { .. }
        ));
    });
}

// Drives a workflow through its first task and activity with one worker, then
// injects `inject` before a fresh worker's cold recovery and asserts the error
// surfaced without stranding the claim or the single recovery slot: the same
// worker must afterwards recover and complete the run unaided.
async fn cold_recovery_error_releases_claim_and_recovery_slot(
    inject: impl FnOnce(&RecordingBackend),
    chunk_events: usize,
) {
    let backend = RecordingBackend::new(MemoryBackend::new());
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<double_plus_one>("wf/cold-recovery-error", "workflows", number(9))
        .await
        .unwrap();
    let mut first_worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(double_plus_one)
        .register_activity(double)
        .build();
    assert!(first_worker.run_workflow_once().await.unwrap());
    assert!(first_worker.run_activity_once().await.unwrap());
    drop(first_worker);

    let mut recovered_worker = Worker::builder(backend.clone())
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .history_chunk_events(chunk_events)
        .max_concurrent_recoveries(1)
        .recovery_defer_delay(Duration::from_millis(1))
        .register_workflow(double_plus_one)
        .register_activity(double)
        .build();

    inject(&backend);
    let err = recovered_worker.run_workflow_once().await.unwrap_err();
    assert!(matches!(err, durust::Error::Backend(_)));

    // A leaked claim would keep the run unclaimable (its lease never expires
    // under virtual time) and a leaked recovery slot would defer every future
    // cold recovery, so completing here proves both were released.
    recovered_worker.run_until_idle().await.unwrap();
    let history = stream_all(&backend, &run_id).await;
    assert!(matches!(
        history.last().unwrap().data,
        HistoryEventData::WorkflowCompleted { .. }
    ));
}

#[test]
fn cold_recovery_change_versions_error_releases_claim_and_recovery_slot() {
    block_on(async {
        // A one-event chunk keeps `has_more` true so the prepare pipeline
        // queries the change-version index, which is where the fault fires.
        cold_recovery_error_releases_claim_and_recovery_slot(
            |backend| backend.fail_next_change_versions(),
            1,
        )
        .await;
    });
}

#[test]
fn cold_recovery_hydrate_error_releases_claim_and_recovery_slot() {
    block_on(async {
        cold_recovery_error_releases_claim_and_recovery_slot(
            |backend| backend.fail_hydrate_payload_calls(1),
            128,
        )
        .await;
    });
}

#[test]
fn batch_prepare_error_releases_failed_claim_and_still_commits_neighbors() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_a = client
            .start_workflow::<double_plus_one>("wf/batch-prepare-a", "workflows", number(1))
            .await
            .unwrap();
        let run_b = client
            .start_workflow::<double_plus_one>("wf/batch-prepare-b", "workflows", number(2))
            .await
            .unwrap();
        let mut worker = batch_error_worker(backend.clone());

        // The first claimed task (run a, lowest run id) fails its input
        // hydration during prepare; its batch neighbor must still commit.
        backend.fail_hydrate_payload_calls(1);
        let err = worker.run_workflow_batch_once().await.unwrap_err();
        assert!(matches!(err, durust::Error::Backend(_)));

        let history_a = stream_all(&backend, &run_a).await;
        assert_eq!(history_a.len(), 1);
        let history_b = stream_all(&backend, &run_b).await;
        assert_eq!(history_b.len(), 2);
        assert!(matches!(
            history_b[1].data,
            HistoryEventData::ActivityScheduled(_)
        ));

        // The failed task's claim was released, so the worker finishes both
        // workflows without any lease expiry.
        worker.run_until_idle().await.unwrap();
        for run_id in [&run_a, &run_b] {
            let history = stream_all(&backend, run_id).await;
            assert!(matches!(
                history.last().unwrap().data,
                HistoryEventData::WorkflowCompleted { .. }
            ));
        }
    });
}

#[test]
fn batch_commit_rpc_error_releases_every_claim_in_the_batch() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_a = client
            .start_workflow::<double_plus_one>("wf/batch-rpc-a", "workflows", number(1))
            .await
            .unwrap();
        let run_b = client
            .start_workflow::<double_plus_one>("wf/batch-rpc-b", "workflows", number(2))
            .await
            .unwrap();
        let mut worker = batch_error_worker(backend.clone());

        backend.fail_next_commit_batch();
        let err = worker.run_workflow_batch_once().await.unwrap_err();
        assert!(matches!(err, durust::Error::Backend(_)));

        // Nothing committed, and both claims were released.
        for run_id in [&run_a, &run_b] {
            assert_eq!(stream_all(&backend, run_id).await.len(), 1);
        }
        worker.run_until_idle().await.unwrap();
        for run_id in [&run_a, &run_b] {
            let history = stream_all(&backend, run_id).await;
            assert!(matches!(
                history.last().unwrap().data,
                HistoryEventData::WorkflowCompleted { .. }
            ));
        }
    });
}

#[test]
fn activity_lease_duration_knob_bounds_default_option_activity_runtime() {
    block_on(async {
        // An activity with default options (no timeout, no heartbeat, no
        // retries) whose virtual runtime exceeds the worker's configured
        // activity lease is reclaimed mid-flight: the maintenance scan turns
        // it into a permanent ActivityTimedOut, the late completion is
        // idempotently rejected, and the workflow observes the timeout.
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/lease-too-short", "workflows", number(4))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .activity_task_lease_duration(Duration::from_secs(1))
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());
        backend.advance_and_scan_before_activity_completion(Duration::from_secs(2));
        assert!(worker.run_activity_once().await.unwrap());
        worker.run_until_idle().await.unwrap();
        let history = stream_all(&backend, &run_id).await;
        assert!(
            history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ActivityTimedOut(_)))
        );
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ActivityCompleted(_)))
        );
        assert!(matches!(
            history.last().unwrap().data,
            HistoryEventData::WorkflowFailed { .. }
        ));

        // The same activity under a lease longer than its runtime completes
        // normally and the workflow finishes.
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/lease-long-enough", "workflows", number(4))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .activity_task_lease_duration(Duration::from_secs(60))
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());
        backend.advance_and_scan_before_activity_completion(Duration::from_secs(2));
        assert!(worker.run_activity_once().await.unwrap());
        worker.run_until_idle().await.unwrap();
        let history = stream_all(&backend, &run_id).await;
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::ActivityTimedOut(_)))
        );
        assert!(matches!(
            history.last().unwrap().data,
            HistoryEventData::WorkflowCompleted { .. }
        ));
    });
}

#[test]
fn batch_per_item_conflict_does_not_abort_the_rest_of_the_chunk() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_a = client
            .start_workflow::<double_plus_one>("wf/batch-conflict-a", "workflows", number(1))
            .await
            .unwrap();
        let run_b = client
            .start_workflow::<double_plus_one>("wf/batch-conflict-b", "workflows", number(2))
            .await
            .unwrap();
        let mut worker = batch_error_worker(backend.clone());

        backend.conflict_batch_commit_for_run(run_a.clone());
        let committed = worker.run_workflow_batch_once().await.unwrap();
        assert_eq!(committed, 1);

        let history_a = stream_all(&backend, &run_a).await;
        assert_eq!(history_a.len(), 1);
        let history_b = stream_all(&backend, &run_b).await;
        assert_eq!(history_b.len(), 2);

        worker.run_until_idle().await.unwrap();
        for run_id in [&run_a, &run_b] {
            let history = stream_all(&backend, run_id).await;
            assert!(matches!(
                history.last().unwrap().data,
                HistoryEventData::WorkflowCompleted { .. }
            ));
        }
    });
}

#[test]
fn replay_detects_changed_activity_input_without_appending_failure() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/nondeterminism", "workflows", number(7))
            .await
            .unwrap();
        let mut original_worker = Worker::builder(backend.clone())
            .worker_id("worker-original")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(original_worker.run_workflow_once().await.unwrap());
        assert!(original_worker.run_activity_once().await.unwrap());
        drop(original_worker);

        let mut changed_worker = Worker::builder(backend.clone())
            .worker_id("worker-changed")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(double_plus_one_changed)
            .register_activity(double)
            .build();
        let err = changed_worker.run_workflow_once().await.unwrap_err();
        assert!(matches!(err, durust::Error::Nondeterminism(_)));

        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 3);
        assert!(
            !history
                .iter()
                .any(|event| matches!(event.data, HistoryEventData::WorkflowFailed { .. }))
        );

        let immediately_claimable = backend
            .claim_workflow_task(
                WorkerId::new("after-nondeterminism"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(immediately_claimable.is_none());
    });
}

#[test]
fn configured_nondeterminism_backoff_releases_workflow_after_delay() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<double_plus_one>("wf/nondeterminism-backoff", "workflows", number(7))
            .await
            .unwrap();
        let mut original_worker = Worker::builder(backend.clone())
            .worker_id("worker-original")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(original_worker.run_workflow_once().await.unwrap());
        assert!(original_worker.run_activity_once().await.unwrap());
        drop(original_worker);

        let mut changed_worker = Worker::builder(backend.clone())
            .worker_id("worker-changed")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .nondeterminism_retry_backoff(Duration::from_millis(25))
            .register_workflow(double_plus_one_changed)
            .register_activity(double)
            .build();
        let err = changed_worker.run_workflow_once().await.unwrap_err();
        assert!(matches!(err, durust::Error::Nondeterminism(_)));

        let hidden = backend
            .claim_workflow_task(
                WorkerId::new("before-backoff"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(hidden.is_none());

        std::thread::sleep(Duration::from_millis(40));
        let visible = backend
            .claim_workflow_task(
                WorkerId::new("after-backoff"),
                double_plus_one_claim_options(),
            )
            .await
            .unwrap();
        assert!(visible.is_some());
    });
}

#[test]
fn provider_claims_only_registered_workflow_and_activity_types() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<double_plus_one>("wf/filtering", "workflows", number(1))
            .await
            .unwrap();

        let unmatched = backend
            .claim_workflow_task(
                WorkerId::new("worker"),
                ClaimWorkflowTaskOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("workflows"),
                    registered_workflow_types: Vec::new(),
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(unmatched.is_none());

        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());

        let unmatched_activity = backend
            .claim_activity_task(
                WorkerId::new("activity-worker"),
                ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: TaskQueue::new("activities"),
                    registered_activity_names: vec![ActivityName::new("other.activity")],
                    lease_duration: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(unmatched_activity.is_none());
    });
}

#[test]
fn sqlite_backend_recovers_after_close_and_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("durust.sqlite3");
        let backend = SqliteBackend::open(&db_path).unwrap();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/sqlite-replay", "workflows", number(11))
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .worker_id("sqlite-before-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);
        drop(backend);

        let reopened = SqliteBackend::open(&db_path).unwrap();
        let mut recovered_worker = Worker::builder(reopened.clone())
            .worker_id("sqlite-after-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        let history = stream_all(&reopened, &run_id).await;
        assert_eq!(history.len(), 4);
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("SQLite workflow did not complete after reopen replay");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 23);
    });
}

#[test]
fn sqlite_continue_as_new_recovers_after_close_and_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("durust-continue.sqlite3");
        let backend = SqliteBackend::open(&db_path).unwrap();
        let client = Client::new(backend.clone());
        let first_run_id = client
            .start_workflow::<continue_as_new_workflow>(
                "wf/sqlite-continue",
                "workflows",
                ContinueInput {
                    remaining: 1,
                    total: 4,
                },
            )
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .worker_id("sqlite-continue-before-reopen")
            .workflow_task_queue("workflows")
            .register_workflow(continue_as_new_workflow)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        drop(first_worker);
        drop(backend);

        let reopened = SqliteBackend::open(&db_path).unwrap();
        let reopened_client = Client::new(reopened.clone());
        let continued_run_id = reopened_client
            .start_workflow::<continue_as_new_workflow>(
                "wf/sqlite-continue",
                "workflows",
                ContinueInput {
                    remaining: 99,
                    total: 99,
                },
            )
            .await
            .unwrap();
        assert_ne!(first_run_id, continued_run_id);

        let mut recovered_worker = Worker::builder(reopened.clone())
            .worker_id("sqlite-continue-after-reopen")
            .workflow_task_queue("workflows")
            .history_chunk_events(1)
            .register_workflow(continue_as_new_workflow)
            .build();
        assert!(recovered_worker.run_workflow_once().await.unwrap());

        let first_history = stream_all(&reopened, &first_run_id).await;
        assert!(matches!(
            first_history[1].data,
            HistoryEventData::WorkflowContinuedAsNew { .. }
        ));
        let continued_history = stream_all(&reopened, &continued_run_id).await;
        assert_eq!(continued_history.len(), 2);
        let HistoryEventData::WorkflowStarted { input, .. } = &continued_history[0].data else {
            panic!("expected continued run start");
        };
        assert_eq!(
            durust::decode_payload::<ContinueInput>(input).unwrap(),
            ContinueInput {
                remaining: 0,
                total: 5
            }
        );
        let HistoryEventData::WorkflowCompleted { result } = &continued_history[1].data else {
            panic!("expected continued run completion");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 5);
    });
}

#[test]
fn sqlite_activity_map_recovers_after_close_and_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("durust-map.sqlite3");
        let backend = SqliteBackend::open(&db_path).unwrap();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<activity_map_sum>(
                "wf/sqlite-map-recovery",
                "workflows",
                values(vec![2, 4, 6]),
            )
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .worker_id("sqlite-map-before-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("map-activities")
            .register_workflow(activity_map_sum)
            .register_activity(map_double)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        assert!(first_worker.run_activity_once().await.unwrap());
        drop(first_worker);
        drop(backend);

        let reopened = SqliteBackend::open(&db_path).unwrap();
        let mut recovered_worker = Worker::builder(reopened.clone())
            .worker_id("sqlite-map-after-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("map-activities")
            .history_chunk_events(1)
            .register_workflow(activity_map_sum)
            .register_activity(map_double)
            .build();
        let stats = recovered_worker.run_until_idle().await.unwrap();
        assert_eq!(stats.activity_tasks, 2);
        assert_eq!(stats.workflow_tasks, 1);

        let history = stream_all(&reopened, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ActivityMapScheduled(_)
        ));
        assert!(matches!(
            history[2].data,
            HistoryEventData::ActivityMapCompleted(_)
        ));
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("SQLite map workflow did not complete after reopen replay");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 24);
    });
}

#[test]
fn sqlite_child_workflow_map_recovers_after_close_and_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("durust-child-map.sqlite3");
        let backend = SqliteBackend::open(&db_path).unwrap();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<child_workflow_map_sum>(
                "wf/sqlite-child-map-recovery",
                "workflows",
                values(vec![2, 4, 6]),
            )
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .worker_id("sqlite-child-map-before-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(child_workflow_map_sum)
            .register_workflow(child_double_workflow)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        drop(first_worker);
        drop(backend);

        let reopened = SqliteBackend::open(&db_path).unwrap();
        let mut recovered_worker = Worker::builder(reopened.clone())
            .worker_id("sqlite-child-map-after-crash")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .history_chunk_events(1)
            .register_workflow(child_workflow_map_sum)
            .register_workflow(child_double_workflow)
            .build();
        let stats = recovered_worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 4);
        assert_eq!(stats.activity_tasks, 0);

        let history = stream_all(&reopened, &run_id).await;
        assert_eq!(history.len(), 4);
        assert!(matches!(
            history[1].data,
            HistoryEventData::ChildWorkflowMapScheduled(_)
        ));
        assert!(matches!(
            history[2].data,
            HistoryEventData::ChildWorkflowMapCompleted(_)
        ));
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("SQLite child map workflow did not complete after reopen replay");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 24);
    });
}

#[test]
fn sqlite_child_outbox_recovers_after_close_and_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("durust-child.sqlite3");
        let backend = SqliteBackend::open(&db_path).unwrap();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<child_spawn_wait_workflow>(
                "wf/sqlite-child-recovery",
                "workflows",
                number(14),
            )
            .await
            .unwrap();
        let mut first_worker = Worker::builder(backend.clone())
            .worker_id("sqlite-child-before-crash")
            .workflow_task_queue("workflows")
            .register_workflow(child_spawn_wait_workflow)
            .register_workflow(child_double_workflow)
            .build();
        assert!(first_worker.run_workflow_once().await.unwrap());
        drop(first_worker);
        drop(backend);

        let reopened = SqliteBackend::open(&db_path).unwrap();
        let mut recovered_worker = Worker::builder(reopened.clone())
            .worker_id("sqlite-child-after-crash")
            .workflow_task_queue("workflows")
            .history_chunk_events(1)
            .register_workflow(child_spawn_wait_workflow)
            .register_workflow(child_double_workflow)
            .build();
        let stats = recovered_worker.run_until_idle().await.unwrap();
        assert!(stats.child_workflow_starts_dispatched >= 1);

        let history = stream_all(&reopened, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } =
            &history.last().expect("parent terminal").data
        else {
            panic!("SQLite child workflow did not complete after reopen");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 28);
    });
}

#[test]
fn sqlite_worker_loop_runs_until_idle() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("durust-loop.sqlite3");
        let backend = SqliteBackend::open(&db_path).unwrap();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/sqlite-loop", "workflows", number(13))
            .await
            .unwrap();
        let mut worker = Worker::builder(backend.clone())
            .worker_id("sqlite-loop-worker")
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 2);
        assert_eq!(stats.activity_tasks, 1);

        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("SQLite workflow did not complete");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 27);
    });
}

#[test]
fn worker_drops_cache_and_retries_after_workflow_task_commit_conflict() {
    block_on(async {
        let inner = MemoryBackend::new();
        let backend = RecordingBackend::new(inner);
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/commit-conflict", "workflows", number(11))
            .await
            .unwrap();
        backend.conflict_next_commit();

        let mut worker = Worker::builder(backend.clone())
            .workflow_task_queue("workflows")
            .activity_task_queue("activities")
            .register_workflow(double_plus_one)
            .register_activity(double)
            .build();
        assert!(worker.run_workflow_once().await.unwrap());
        assert_eq!(stream_all(&backend, &run_id).await.len(), 1);

        let stats = worker.run_until_idle().await.unwrap();
        assert_eq!(stats.workflow_tasks, 2);
        assert_eq!(stats.activity_tasks, 1);
        let history = stream_all(&backend, &run_id).await;
        let HistoryEventData::WorkflowCompleted { result } = &history[3].data else {
            panic!("workflow did not complete after retry");
        };
        assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 23);
    });
}

async fn stream_all<B>(backend: &B, run_id: &durust::RunId) -> Vec<durust::HistoryEvent>
where
    B: DurableBackend,
{
    backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1_000_000),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events
}

fn lazy_payload_worker(
    backend: durust::PayloadBackend<MemoryBackend, CountingBlobStore>,
    worker_id: &str,
) -> Worker<durust::PayloadBackend<MemoryBackend, CountingBlobStore>> {
    Worker::builder(backend)
        .worker_id(worker_id)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(sleep_before_large_activity_result)
        .register_activity(large_payload_result)
        .build()
}

#[derive(Clone, Default)]
struct CountingBlobStore {
    blobs: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    gets: Arc<Mutex<usize>>,
}

impl CountingBlobStore {
    fn get_count(&self) -> usize {
        *self.gets.lock().unwrap()
    }
}

impl durust::PayloadBlobStore for CountingBlobStore {
    fn put_payload_blob(
        &self,
        digest: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, durust::Result<String>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            blobs.lock().unwrap().insert(digest.clone(), bytes);
            Ok(format!("memory-blob://payload/{digest}"))
        })
    }

    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<Vec<u8>>> {
        let blobs = self.blobs.clone();
        let gets = self.gets.clone();
        Box::pin(async move {
            *gets.lock().unwrap() += 1;
            blobs
                .lock()
                .unwrap()
                .get(&digest)
                .cloned()
                .ok_or_else(|| durust::Error::PayloadDecode(format!("missing blob `{digest}`")))
        })
    }

    fn list_payload_blob_digests(&self) -> BoxFuture<'static, durust::Result<BTreeSet<String>>> {
        let blobs = self.blobs.clone();
        Box::pin(async move { Ok(blobs.lock().unwrap().keys().cloned().collect()) })
    }

    fn delete_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<()>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            blobs.lock().unwrap().remove(&digest);
            Ok(())
        })
    }

    fn owns_payload_blob_uri(&self, uri: &str) -> bool {
        uri.starts_with("memory-blob://payload/")
    }
}

fn version_worker<W>(backend: MemoryBackend, workflow: W) -> Worker<MemoryBackend>
where
    W: durust::Workflow + Default,
{
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(workflow)
        .register_activity(version_activity_a)
        .register_activity(version_activity_b)
        .build()
}

fn scheduled_activity(
    history: &[durust::HistoryEvent],
    index: usize,
) -> &durust::ActivityScheduled {
    let HistoryEventData::ActivityScheduled(scheduled) = &history[index].data else {
        panic!("expected ActivityScheduled at history index {index}");
    };
    scheduled
}

fn double_plus_one_claim_options() -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("workflows"),
        registered_workflow_types: vec![WorkflowType::new("tests.double-plus-one", 1)],
        lease_duration: Duration::from_secs(30),
    }
}

#[derive(Clone)]
struct RecordingBackend {
    inner: MemoryBackend,
    stream_requests: Arc<Mutex<Vec<durust::StreamHistoryRequest>>>,
    signal_batch_requests: Arc<Mutex<Vec<durust::ReadSignalInboxesRequest>>>,
    conflict_next_commit: Arc<Mutex<bool>>,
    backpressure_next_replay_stream: Arc<Mutex<Option<Duration>>>,
    fail_next_current_time: Arc<Mutex<bool>>,
    fail_next_change_versions: Arc<Mutex<bool>>,
    hydrate_failures_remaining: Arc<Mutex<u32>>,
    fail_next_commit_batch: Arc<Mutex<bool>>,
    conflict_batch_commit_run: Arc<Mutex<Option<durust::RunId>>>,
    advance_before_activity_completion: Arc<Mutex<Option<Duration>>>,
    claim_prefetch_enabled: bool,
}

impl RecordingBackend {
    fn new(inner: MemoryBackend) -> Self {
        Self {
            inner,
            stream_requests: Arc::new(Mutex::new(Vec::new())),
            signal_batch_requests: Arc::new(Mutex::new(Vec::new())),
            conflict_next_commit: Arc::new(Mutex::new(false)),
            backpressure_next_replay_stream: Arc::new(Mutex::new(None)),
            fail_next_current_time: Arc::new(Mutex::new(false)),
            fail_next_change_versions: Arc::new(Mutex::new(false)),
            hydrate_failures_remaining: Arc::new(Mutex::new(0)),
            fail_next_commit_batch: Arc::new(Mutex::new(false)),
            conflict_batch_commit_run: Arc::new(Mutex::new(None)),
            advance_before_activity_completion: Arc::new(Mutex::new(None)),
            claim_prefetch_enabled: true,
        }
    }

    fn without_claim_prefetch(mut self) -> Self {
        self.claim_prefetch_enabled = false;
        self
    }

    fn clear_stream_requests(&self) {
        self.stream_requests.lock().unwrap().clear();
    }

    fn stream_requests(&self) -> Vec<durust::StreamHistoryRequest> {
        self.stream_requests.lock().unwrap().clone()
    }

    fn signal_batch_requests(&self) -> Vec<durust::ReadSignalInboxesRequest> {
        self.signal_batch_requests.lock().unwrap().clone()
    }

    fn conflict_next_commit(&self) {
        *self.conflict_next_commit.lock().unwrap() = true;
    }

    fn backpressure_next_replay_stream(&self, retry_after: Duration) {
        *self.backpressure_next_replay_stream.lock().unwrap() = Some(retry_after);
    }

    fn fail_next_current_time(&self) {
        *self.fail_next_current_time.lock().unwrap() = true;
    }

    fn fail_next_change_versions(&self) {
        *self.fail_next_change_versions.lock().unwrap() = true;
    }

    fn fail_hydrate_payload_calls(&self, count: u32) {
        *self.hydrate_failures_remaining.lock().unwrap() = count;
    }

    fn fail_next_commit_batch(&self) {
        *self.fail_next_commit_batch.lock().unwrap() = true;
    }

    fn conflict_batch_commit_for_run(&self, run_id: durust::RunId) {
        *self.conflict_batch_commit_run.lock().unwrap() = Some(run_id);
    }

    // Simulates an activity whose execution outlives `advance` of virtual time
    // while the maintenance scanner keeps running elsewhere: before the next
    // completion is applied, the clock moves and due activities are timed out.
    fn advance_and_scan_before_activity_completion(&self, advance: Duration) {
        *self.advance_before_activity_completion.lock().unwrap() = Some(advance);
    }

    fn take_flag(flag: &Arc<Mutex<bool>>) -> bool {
        let mut flag = flag.lock().unwrap();
        std::mem::take(&mut *flag)
    }
}

impl DurableBackend for RecordingBackend {
    fn start_workflow(
        &self,
        req: durust::StartWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::StartWorkflowOutcome>> {
        self.inner.start_workflow(req)
    }

    fn cancel_workflow(
        &self,
        req: durust::CancelWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::CancelWorkflowOutcome>> {
        self.inner.cancel_workflow(req)
    }

    fn current_time(&self) -> BoxFuture<'static, durust::Result<durust::TimestampMs>> {
        if Self::take_flag(&self.fail_next_current_time) {
            return Box::pin(async {
                Err(durust::Error::Backend(
                    "injected current_time failure".to_owned(),
                ))
            });
        }
        self.inner.current_time()
    }

    fn claim_workflow_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::ClaimedWorkflowTask>>> {
        let prefetch_enabled = self.claim_prefetch_enabled;
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut claimed = inner.claim_workflow_task(worker_id, opts).await?;
            if !prefetch_enabled {
                if let Some(claimed) = &mut claimed {
                    claimed.prefetched_history.clear();
                }
            }
            Ok(claimed)
        })
    }

    fn stream_history(
        &self,
        req: durust::StreamHistoryRequest,
    ) -> BoxFuture<'static, durust::Result<durust::HistoryChunk>> {
        self.stream_requests.lock().unwrap().push(req.clone());
        self.inner.stream_history(req)
    }

    fn hydrate_payload(
        &self,
        payload: durust::PayloadRef,
    ) -> BoxFuture<'static, durust::Result<durust::PayloadRef>> {
        {
            let mut remaining = self.hydrate_failures_remaining.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Box::pin(async {
                    Err(durust::Error::Backend(
                        "injected hydrate failure".to_owned(),
                    ))
                });
            }
        }
        self.inner.hydrate_payload(payload)
    }

    fn stream_history_for_replay(
        &self,
        req: durust::StreamHistoryRequest,
    ) -> BoxFuture<'static, durust::Result<durust::HistoryChunk>> {
        self.stream_requests.lock().unwrap().push(req.clone());
        let retry_after = self.backpressure_next_replay_stream.lock().unwrap().take();
        if let Some(retry_after) = retry_after {
            return Box::pin(async move {
                Err(durust::Error::backpressure(
                    "recording backend replay stream budget exhausted",
                    retry_after,
                ))
            });
        }
        self.inner.stream_history_for_replay(req)
    }

    fn commit_workflow_task(
        &self,
        claim: durust::WorkflowTaskClaim,
        batch: durust::WorkflowTaskCommit,
    ) -> BoxFuture<'static, durust::Result<durust::CommitOutcome>> {
        let should_conflict = {
            let mut conflict_next_commit = self.conflict_next_commit.lock().unwrap();
            let should_conflict = *conflict_next_commit;
            *conflict_next_commit = false;
            should_conflict
        };
        if should_conflict {
            let inner = self.inner.clone();
            return Box::pin(async move {
                inner
                    .release_workflow_task(
                        claim,
                        durust::WorkflowTaskRelease::immediate(
                            durust::WorkflowTaskReason::CacheEvicted,
                        ),
                    )
                    .await?;
                Ok(durust::CommitOutcome::Conflict)
            });
        }
        self.inner.commit_workflow_task(claim, batch)
    }

    // Overridden (instead of relying on the default per-item loop) so tests
    // can fail the whole batch RPC or fabricate a per-item conflict while the
    // other items commit for real.
    fn commit_workflow_tasks(
        &self,
        batch: durust::WorkflowTaskCommitBatch,
    ) -> BoxFuture<'static, durust::Result<Vec<durust::WorkflowTaskCommitBatchResult>>> {
        if Self::take_flag(&self.fail_next_commit_batch) {
            return Box::pin(async {
                Err(durust::Error::Backend(
                    "injected batch commit failure".to_owned(),
                ))
            });
        }
        let conflict_run = self.conflict_batch_commit_run.lock().unwrap().take();
        let backend = self.clone();
        Box::pin(async move {
            let mut results = Vec::with_capacity(batch.commits.len());
            for input in batch.commits {
                let claim = input.claim;
                if conflict_run.as_ref() == Some(&claim.run_id) {
                    // Mirror a real conflict: the provider releases the claim
                    // as part of reporting it.
                    backend
                        .inner
                        .release_workflow_task(
                            claim.clone(),
                            durust::WorkflowTaskRelease::immediate(
                                durust::WorkflowTaskReason::CacheEvicted,
                            ),
                        )
                        .await?;
                    results.push(durust::WorkflowTaskCommitBatchResult {
                        claim,
                        result: Ok(durust::CommitOutcome::Conflict),
                    });
                    continue;
                }
                let result = backend
                    .commit_workflow_task(claim.clone(), input.commit)
                    .await;
                results.push(durust::WorkflowTaskCommitBatchResult { claim, result });
            }
            Ok(results)
        })
    }

    fn release_workflow_task(
        &self,
        claim: durust::WorkflowTaskClaim,
        release: durust::WorkflowTaskRelease,
    ) -> BoxFuture<'static, durust::Result<()>> {
        self.inner.release_workflow_task(claim, release)
    }

    fn signal_workflow(
        &self,
        req: durust::SignalWorkflowRequest,
    ) -> BoxFuture<'static, durust::Result<durust::SignalWorkflowOutcome>> {
        self.inner.signal_workflow(req)
    }

    fn read_signal_inbox(
        &self,
        req: durust::ReadSignalInboxRequest,
    ) -> BoxFuture<'static, durust::Result<Option<durust::SignalInboxRecord>>> {
        self.inner.read_signal_inbox(req)
    }

    fn read_signal_inboxes(
        &self,
        req: durust::ReadSignalInboxesRequest,
    ) -> BoxFuture<'static, durust::Result<Vec<Option<durust::SignalInboxRecord>>>> {
        self.signal_batch_requests.lock().unwrap().push(req.clone());
        self.inner.read_signal_inboxes(req)
    }

    fn fire_due_timers(
        &self,
        req: durust::FireDueTimersRequest,
    ) -> BoxFuture<'static, durust::Result<durust::FireDueTimersOutcome>> {
        self.inner.fire_due_timers(req)
    }

    fn timeout_due_activities(
        &self,
        req: durust::TimeoutDueActivitiesRequest,
    ) -> BoxFuture<'static, durust::Result<durust::TimeoutDueActivitiesOutcome>> {
        self.inner.timeout_due_activities(req)
    }

    fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::ClaimedActivityTask>>> {
        self.inner.claim_activity_task(worker_id, opts)
    }

    fn heartbeat_activity(
        &self,
        req: durust::ActivityHeartbeatRequest,
    ) -> BoxFuture<'static, durust::Result<durust::ActivityHeartbeatOutcome>> {
        self.inner.heartbeat_activity(req)
    }

    fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::CompleteActivityOutcome>> {
        let advance = self
            .advance_before_activity_completion
            .lock()
            .unwrap()
            .take();
        let inner = self.inner.clone();
        Box::pin(async move {
            if let Some(advance) = advance {
                inner.advance_time(advance);
                let now = inner.current_time().await?;
                inner
                    .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
                        namespace: Namespace::default(),
                        now,
                        limit: 16,
                    })
                    .await?;
            }
            inner.complete_activity(req).await
        })
    }

    fn fail_activity(
        &self,
        req: durust::FailActivityRequest,
    ) -> BoxFuture<'static, durust::Result<durust::FailActivityOutcome>> {
        self.inner.fail_activity(req)
    }

    fn dispatch_child_workflow_starts(
        &self,
        req: durust::DispatchChildWorkflowStartsRequest,
    ) -> BoxFuture<'static, durust::Result<durust::DispatchChildWorkflowStartsOutcome>> {
        self.inner.dispatch_child_workflow_starts(req)
    }

    fn query_projection(
        &self,
        req: durust::QueryProjectionRequest,
    ) -> BoxFuture<'static, durust::Result<durust::QueryProjectionOutcome>> {
        self.inner.query_projection(req)
    }

    fn workflow_change_versions(
        &self,
        req: durust::WorkflowChangeVersionsRequest,
    ) -> BoxFuture<'static, durust::Result<durust::WorkflowChangeVersionsOutcome>> {
        if Self::take_flag(&self.fail_next_change_versions) {
            return Box::pin(async {
                Err(durust::Error::Backend(
                    "injected change versions failure".to_owned(),
                ))
            });
        }
        self.inner.workflow_change_versions(req)
    }

    fn payload_roots(&self) -> BoxFuture<'static, durust::Result<durust::PayloadRootsOutcome>> {
        self.inner.payload_roots()
    }

    fn gc_payload_blobs(
        &self,
        req: durust::PayloadGarbageCollectionRequest,
    ) -> BoxFuture<'static, durust::Result<durust::PayloadGarbageCollectionOutcome>> {
        self.inner.gc_payload_blobs(req)
    }
}
