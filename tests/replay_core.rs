use durust::{
    ActivityName, BoxSelectBranch, ClaimActivityOptions, ClaimWorkflowTaskOptions, Client,
    CompleteActivityRequest, DurableBackend, DurableBranchExt, EventId, HistoryEventData,
    MemoryBackend, Namespace, SqliteBackend, TaskQueue, Worker, WorkerId, WorkflowType,
};
use futures::executor::block_on;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static FLAKY_ATTEMPTS: Mutex<u32> = Mutex::new(0);
static SIDE_EFFECT_COUNTER: Mutex<u64> = Mutex::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct NumberInput {
    value: u64,
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
async fn fail_activity(_: ()) -> durust::Result<u64> {
    Err(durust::Error::Backend("boom".to_owned()))
}

#[durust::activity(name = "tests.non-retryable")]
async fn non_retryable_activity(_: ()) -> durust::Result<u64> {
    Err(durust::Error::non_retryable(
        "tests.validation",
        "validation failed",
    ))
}

#[durust::activity(name = "tests.flaky")]
async fn flaky_activity(_: ()) -> durust::Result<u64> {
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
async fn large_payload_result(_: ()) -> durust::Result<LargePayload> {
    Ok(LargePayload {
        bytes: vec![7; 64 * 1024],
    })
}

#[durust::activity(name = "tests.version-a")]
async fn version_activity_a(_: ()) -> durust::Result<String> {
    Ok("a".to_owned())
}

#[durust::activity(name = "tests.version-b")]
async fn version_activity_b(_: ()) -> durust::Result<String> {
    Ok("b".to_owned())
}

#[durust::workflow(name = "tests.double-plus-one", version = 1)]
async fn double_plus_one(input: u64) -> durust::Result<u64> {
    let doubled = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .await?;
    Ok(doubled + 1)
}

#[durust::workflow(name = "tests.version-branch", version = 1)]
async fn version_original(_: ()) -> durust::Result<String> {
    durust::call_activity!(version_activity_a(()))
        .task_queue("activities")
        .await
}

#[durust::workflow(name = "tests.version-branch", version = 1)]
async fn version_patched(_: ()) -> durust::Result<String> {
    if durust::patched("replace-a-with-b")? {
        durust::call_activity!(version_activity_b(()))
            .task_queue("activities")
            .await
    } else {
        durust::call_activity!(version_activity_a(()))
            .task_queue("activities")
            .await
    }
}

#[durust::workflow(name = "tests.version-branch", version = 1)]
async fn version_min_two(_: ()) -> durust::Result<String> {
    let _ = durust::get_version("replace-a-with-b", 2, 2)?;
    durust::call_activity!(version_activity_b(()))
        .task_queue("activities")
        .await
}

#[durust::workflow(name = "tests.version-branch", version = 1)]
async fn version_deprecated(_: ()) -> durust::Result<String> {
    durust::deprecate_patch("replace-a-with-b")?;
    durust::call_activity!(version_activity_b(()))
        .task_queue("activities")
        .await
}

#[durust::workflow(name = "tests.version-branch", version = 1)]
async fn version_removed(_: ()) -> durust::Result<String> {
    durust::call_activity!(version_activity_b(()))
        .task_queue("activities")
        .await
}

#[durust::workflow(name = "tests.child-double", version = 1)]
async fn child_double_workflow(input: u64) -> durust::Result<u64> {
    Ok(input * 2)
}

#[durust::workflow(name = "tests.child-spawn-wait", version = 1)]
async fn child_spawn_wait_workflow(input: u64) -> durust::Result<u64> {
    let child = durust::child!(child_double_workflow(input))
        .workflow_id(format!("wf/child-spawn-wait/{input}"))
        .spawn()
        .await?;
    child.result().await
}

#[durust::workflow(name = "tests.child-spawn-abandon", version = 1)]
async fn child_spawn_abandon_workflow(input: u64) -> durust::Result<String> {
    let child = durust::child!(child_double_workflow(input))
        .workflow_id(format!("wf/child-spawn-abandon/{input}"))
        .parent_close_policy(durust::ParentClosePolicy::Abandon)
        .spawn()
        .await?;
    Ok(child.run_id().0.clone())
}

#[durust::workflow(name = "tests.child-spawn-cancel", version = 1)]
async fn child_spawn_cancel_workflow(input: u64) -> durust::Result<String> {
    let child = durust::child!(child_double_workflow(input))
        .workflow_id(format!("wf/child-spawn-cancel/{input}"))
        .parent_close_policy(durust::ParentClosePolicy::Cancel)
        .spawn()
        .await?;
    Ok(child.run_id().0.clone())
}

#[durust::workflow(name = "tests.select-child-result", version = 1)]
async fn select_child_result_workflow(input: u64) -> durust::Result<String> {
    let child = durust::child!(child_double_workflow(input))
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
async fn select_timer_before_child_result_workflow(input: u64) -> durust::Result<String> {
    let child = durust::child!(child_double_workflow(input))
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
async fn activity_spawn_await_later_workflow(input: u64) -> durust::Result<u64> {
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
async fn sleep_before_large_activity_result(_: ()) -> durust::Result<usize> {
    let handle = durust::call_activity!(large_payload_result(()))
        .task_queue("activities")
        .spawn()
        .await?;
    durust::sleep(Duration::from_secs(1)).await?;
    let payload = handle.result().await?;
    Ok(payload.bytes.len())
}

#[durust::workflow(name = "tests.side-effect-then-sleep", version = 1)]
async fn side_effect_then_sleep_workflow(_: ()) -> durust::Result<String> {
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
async fn oversized_side_effect_workflow(_: ()) -> durust::Result<()> {
    let _: String = durust::side_effect("too-large", || {
        "x".repeat(durust::MAX_SIDE_EFFECT_PAYLOAD_BYTES + 1)
    })
    .await?;
    Ok(())
}

#[durust::workflow(name = "tests.select-all-activity-handles", version = 1)]
async fn select_all_activity_handles_workflow(input: u64) -> durust::Result<String> {
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
async fn select_all_mixed_branches_workflow(input: u64) -> durust::Result<String> {
    let activity = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .spawn()
        .await?;
    let child = durust::child!(child_double_workflow(input + 10))
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
async fn join_all_activity_handles_workflow(input: u64) -> durust::Result<String> {
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
async fn join_all_mixed_branches_workflow(input: u64) -> durust::Result<String> {
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
async fn child_first_select_then_timer_workflow(input: u64) -> durust::Result<String> {
    let activity = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .spawn()
        .await?;
    let child = durust::child!(child_double_workflow(input + 10))
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
async fn timer_first_select_then_timer_workflow(input: u64) -> durust::Result<String> {
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
async fn join_two_activities(input: u64) -> durust::Result<u64> {
    let (left, right) = durust::join!(
        durust::call_activity!(double(NumberInput { value: input })).task_queue("activities"),
        durust::call_activity!(double(NumberInput { value: input + 1 })).task_queue("activities"),
    )
    .await?;
    Ok(left + right)
}

#[durust::workflow(name = "tests.join-four-activities", version = 1)]
async fn join_four_activities(input: u64) -> durust::Result<u64> {
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
async fn sequential_two_activities(input: u64) -> durust::Result<u64> {
    let first = durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .await?;
    let second = durust::call_activity!(double(NumberInput { value: input + 1 }))
        .task_queue("activities")
        .await?;
    Ok(first + second)
}

#[durust::workflow(name = "tests.join-signal-timer", version = 1)]
async fn join_signal_timer(input: u64) -> durust::Result<String> {
    let (signal, _) = durust::join!(
        durust::signal::<String>("ready"),
        durust::sleep(Duration::from_millis(input)),
    )
    .await?;
    Ok(signal)
}

#[durust::workflow(name = "tests.join-signal-timer-then-timer", version = 1)]
async fn join_signal_timer_then_timer(input: u64) -> durust::Result<String> {
    let (signal, _) = durust::join!(
        durust::signal::<String>("ready"),
        durust::sleep(Duration::from_millis(input)),
    )
    .await?;
    durust::sleep(Duration::ZERO).await?;
    Ok(signal)
}

#[durust::workflow(name = "tests.select-signal-timer", version = 1)]
async fn select_signal_timer(input: u64) -> durust::Result<String> {
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
async fn select_activity_timer(input: u64) -> durust::Result<u64> {
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
async fn select_timer_before_activity(input: u64) -> durust::Result<String> {
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
async fn select_same_tick_timers(input: u64) -> durust::Result<String> {
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
async fn select_fourth_signal(_: ()) -> durust::Result<String> {
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

#[durust::workflow(name = "tests.select-reorder", version = 1)]
async fn select_then_wait(input: u64) -> durust::Result<String> {
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
async fn select_then_wait_reordered(input: u64) -> durust::Result<String> {
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
async fn failing_activity_workflow(_: ()) -> durust::Result<u64> {
    durust::call_activity!(fail_activity(())).await
}

#[durust::workflow(name = "tests.retry-activity", version = 1)]
async fn retry_activity_workflow(_: ()) -> durust::Result<u64> {
    durust::call_activity!(flaky_activity(()))
        .retry(durust::RetryPolicy::exponential().max_attempts(2))
        .await
}

#[durust::workflow(name = "tests.non-retryable-activity", version = 1)]
async fn non_retryable_activity_workflow(_: ()) -> durust::Result<u64> {
    durust::call_activity!(non_retryable_activity(()))
        .retry(durust::RetryPolicy::exponential().max_attempts(5))
        .await
}

#[durust::workflow(name = "tests.timeout-activity", version = 1)]
async fn timeout_activity_workflow(input: u64) -> durust::Result<u64> {
    durust::call_activity!(double(NumberInput { value: input }))
        .task_queue("activities")
        .timeout(Duration::from_millis(10))
        .await
}

#[durust::workflow(name = "tests.heartbeat-activity", version = 1)]
async fn heartbeat_activity_workflow(input: u64) -> durust::Result<u64> {
    durust::call_activity!(heartbeat_activity_test(NumberInput { value: input }))
        .task_queue("activities")
        .heartbeat_timeout(Duration::from_secs(30))
        .await
}

#[durust::workflow(name = "tests.double-plus-one", version = 1)]
async fn double_plus_one_changed(input: u64) -> durust::Result<u64> {
    let doubled = durust::call_activity!(double(NumberInput { value: input + 1 }))
        .task_queue("activities")
        .await?;
    Ok(doubled + 1)
}

#[durust::workflow(name = "tests.default-activity-options", version = 1)]
async fn default_activity_options_workflow(input: u64) -> durust::Result<u64> {
    durust::set_default_activity_options(
        durust::ActivityOptions::new()
            .task_queue("preferred-activities")
            .retry(durust::RetryPolicy::exponential().max_attempts(5)),
    );
    durust::call_activity!(double(NumberInput { value: input })).await
}

#[durust::workflow(name = "tests.override-activity-options", version = 1)]
async fn override_activity_options_workflow(input: u64) -> durust::Result<u64> {
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
async fn cached_default_activity_options_workflow(input: u64) -> durust::Result<u64> {
    durust::set_default_activity_options(
        durust::ActivityOptions::new()
            .task_queue("sticky-activities")
            .retry(durust::RetryPolicy::exponential().max_attempts(7)),
    );
    let first = durust::call_activity!(double(NumberInput { value: input })).await?;
    durust::call_activity!(double(NumberInput { value: first })).await
}

#[durust::workflow(name = "tests.query-projection", version = 1, query_state = QueryView)]
async fn query_projection_workflow(input: u64) -> durust::Result<u64> {
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
async fn provider_json_codec_workflow(input: u64) -> durust::Result<u64> {
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
async fn sleep_then_return(input: u64) -> durust::Result<u64> {
    durust::sleep(Duration::from_millis(input)).await?;
    Ok(input + 1)
}

#[durust::workflow(name = "tests.await-signal", version = 1)]
async fn await_signal(_: ()) -> durust::Result<String> {
    durust::signal::<String>("ready").await
}

#[durust::workflow(name = "tests.activity-map-sum", version = 1)]
async fn activity_map_sum(input: Vec<u64>) -> durust::Result<u64> {
    let input_manifest =
        durust::activity_map_manifest(input.into_iter().map(|value| NumberInput { value }))?;
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

#[test]
fn simple_workflow_schedules_activity_and_completes_from_cache() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/simple", "workflows", 20)
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
                (),
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
                (),
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
                (),
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
            .start_workflow::<heartbeat_activity_workflow>("wf/heartbeat-activity", "workflows", 20)
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
            .start_workflow::<child_spawn_wait_workflow>("wf/child-wait-parent", "workflows", 11)
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
fn child_workflow_abandon_lets_child_continue_after_parent_exit() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<child_spawn_abandon_workflow>(
                "wf/child-abandon-parent",
                "workflows",
                12,
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
                13,
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
                14,
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
                15,
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
                10,
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
                10,
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
                8,
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
                8,
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
                9,
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
                7,
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

#[test]
fn join_all_collects_spawned_activity_results_in_input_order_after_crash() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<join_all_activity_handles_workflow>(
                "wf/join-all-activity-handles",
                "workflows",
                10,
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
                9,
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
            .start_workflow::<double_plus_one>("wf/cancel-pending", "workflows", 20)
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
            .start_workflow::<join_two_activities>("wf/join-register", "workflows", 10)
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
            .start_workflow::<sequential_two_activities>("wf/sequential-awaits", "workflows", 10)
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
            .start_workflow::<join_four_activities>("wf/join-four", "workflows", 10)
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
            .start_workflow::<join_signal_timer>("wf/join-signal-timer", "workflows", 10)
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
                10,
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
            .start_workflow::<join_two_activities>("wf/join-replay", "workflows", 10)
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
                20,
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
            .start_workflow::<select_same_tick_timers>("wf/select-same-tick", "workflows", 10)
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
            .start_workflow::<select_signal_timer>("wf/select-signal", "workflows", 50)
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
fn select_timer_winner_cancels_in_flight_activity() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<select_activity_timer>("wf/select-activity-timer", "workflows", 20)
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
            .start_workflow::<select_fourth_signal>("wf/select-four", "workflows", ())
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
            .start_workflow::<select_then_wait>("wf/select-replay-winner", "workflows", 10)
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
            .start_workflow::<select_then_wait>("wf/select-reorder", "workflows", 10)
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
            .start_workflow::<version_original>("wf/version-old", "workflows", ())
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
            .start_workflow::<version_patched>("wf/version-new", "workflows", ())
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
            .start_workflow::<version_patched>("wf/version-replay", "workflows", ())
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
            .start_workflow::<version_patched>("wf/version-unsupported", "workflows", ())
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
            .start_workflow::<version_patched>("wf/version-deprecated", "workflows", ())
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
            .start_workflow::<version_deprecated>("wf/version-deprecated-new", "workflows", ())
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
            .start_workflow::<version_patched>("wf/version-removed-too-early", "workflows", ())
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
                10,
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
                10,
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
                4,
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
            .start_workflow::<query_projection_workflow>("wf/query-projection", "workflows", 41)
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
                21,
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
        assert_eq!(durust::decode_payload::<u64>(input).unwrap(), 21);

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
            .start_workflow::<sleep_then_return>("wf/timer-recovery", "workflows", 50)
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
            .start_workflow::<failing_activity_workflow>("wf/activity-failure", "workflows", ())
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
            .start_workflow::<retry_activity_workflow>("wf/activity-retry", "workflows", ())
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
                (),
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
            .start_workflow::<timeout_activity_workflow>("wf/activity-timeout", "workflows", 5)
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
            .start_workflow::<await_signal>("wf/signal-before", "workflows", ())
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
            .start_workflow::<await_signal>("wf/signal-after", "workflows", ())
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
            .start_workflow::<double_plus_one>("wf/loop", "workflows", 8)
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
            .start_workflow::<double_plus_one>("wf/local-activity", "workflows", 5)
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
            .start_workflow::<double_plus_one>("wf/remote-fallback", "workflows", 6)
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
            .start_workflow::<activity_map_sum>("wf/activity-map-sum", "workflows", vec![1, 2, 3])
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
fn configured_local_activity_preference_applies_to_activity_map_items() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<activity_map_sum>("wf/local-activity-map", "workflows", vec![1, 2, 3])
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
            .start_workflow::<double_plus_one>("wf/replay", "workflows", 7)
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
fn recovery_fetches_history_incrementally_in_configured_chunks() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/chunked-replay", "workflows", 5)
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

        let requests = backend.stream_requests();
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|request| request.max_events == 1));
        assert_eq!(
            requests
                .iter()
                .map(|request| request.after_event_id)
                .collect::<Vec<_>>(),
            vec![EventId::ZERO, EventId(1), EventId(2)]
        );
        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
    });
}

#[test]
fn cached_workflow_wake_streams_only_events_after_cached_tail() {
    block_on(async {
        let backend = RecordingBackend::new(MemoryBackend::new());
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/cached-wake", "workflows", 6)
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

        let requests = backend.stream_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].after_event_id, EventId(2));
        assert_eq!(requests[0].up_to_event_id, EventId(3));
        let history = stream_all(&backend, &run_id).await;
        assert_eq!(history.len(), 4);
    });
}

#[test]
fn replay_detects_changed_activity_input_without_appending_failure() {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<double_plus_one>("wf/nondeterminism", "workflows", 7)
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
            .start_workflow::<double_plus_one>("wf/nondeterminism-backoff", "workflows", 7)
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
            .start_workflow::<double_plus_one>("wf/filtering", "workflows", 1)
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
            .start_workflow::<double_plus_one>("wf/sqlite-replay", "workflows", 11)
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
                vec![2, 4, 6],
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
                14,
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
            .start_workflow::<double_plus_one>("wf/sqlite-loop", "workflows", 13)
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
            .start_workflow::<double_plus_one>("wf/commit-conflict", "workflows", 11)
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
    conflict_next_commit: Arc<Mutex<bool>>,
}

impl RecordingBackend {
    fn new(inner: MemoryBackend) -> Self {
        Self {
            inner,
            stream_requests: Arc::new(Mutex::new(Vec::new())),
            conflict_next_commit: Arc::new(Mutex::new(false)),
        }
    }

    fn clear_stream_requests(&self) {
        self.stream_requests.lock().unwrap().clear();
    }

    fn stream_requests(&self) -> Vec<durust::StreamHistoryRequest> {
        self.stream_requests.lock().unwrap().clone()
    }

    fn conflict_next_commit(&self) {
        *self.conflict_next_commit.lock().unwrap() = true;
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
        self.inner.current_time()
    }

    fn claim_workflow_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
    ) -> BoxFuture<'static, durust::Result<Option<durust::ClaimedWorkflowTask>>> {
        self.inner.claim_workflow_task(worker_id, opts)
    }

    fn stream_history(
        &self,
        req: durust::StreamHistoryRequest,
    ) -> BoxFuture<'static, durust::Result<durust::HistoryChunk>> {
        self.stream_requests.lock().unwrap().push(req.clone());
        self.inner.stream_history(req)
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
        self.inner.complete_activity(req)
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
