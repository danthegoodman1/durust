use durust::{
    ActivityMapInputManifest, ActivityMapResultManifest, ActivityMapTask, ActivityName,
    ChildWorkflowMapTask, ClaimActivityOptions, ClaimActivityTasksOptions,
    ClaimWorkflowTaskOptions, ClaimWorkflowTasksOptions, Client, CommitOutcome,
    CompleteActivityRequest, CompleteActivityTasksRequest, DurableBackend, Error, EventId,
    FailActivityRequest, HistoryEventData, MemoryBackend, Namespace, NewHistoryEvent,
    PayloadBackend, PayloadBlobStore, Registry, SqliteBackend, TaskQueue, Worker, WorkerId,
    WorkflowTaskCommit, WorkflowTaskCommitBatch, WorkflowTaskCommitInput, WorkflowType,
};
#[cfg(feature = "postgres")]
use durust::{PostgresBackend, PostgresBackendConfig};
use futures::executor::block_on;
use futures::future::{BoxFuture, ready};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::future::Future;
use std::sync::{Arc, Mutex};
#[cfg(feature = "s3")]
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    value: u64,
}

fn input(value: u64) -> Input {
    Input { value }
}

/// The URL of the Postgres test database, or `None` when this run is not
/// expected to have one.
///
/// Every Postgres test here is environment-gated, and libtest **captures**
/// `eprintln!` on a passing test, so with `DURUST_POSTGRES_URL` unset the whole
/// Postgres suite reports `ok` and its skip notices are invisible unless the
/// run also passes `--nocapture`. That is how these tests ran vacuously in CI.
///
/// `DURUST_REQUIRE_POSTGRES` closes the hole: when it is set, a missing URL is
/// a panic instead of a skip, so a run that is *supposed* to exercise Postgres
/// fails loudly if the database or the variable goes away. CI sets it next to
/// its service container; a developer with no database leaves it unset and
/// still gets the skip.
#[cfg(feature = "postgres")]
fn postgres_url_or_skip(what: &str) -> Option<String> {
    if let Ok(url) = std::env::var("DURUST_POSTGRES_URL") {
        if !url.trim().is_empty() {
            return Some(url);
        }
    }
    assert!(
        !postgres_is_required(),
        "DURUST_REQUIRE_POSTGRES is set, so `{what}` must run, \
         but DURUST_POSTGRES_URL is unset or empty"
    );
    eprintln!("skipping {what}; set DURUST_POSTGRES_URL");
    None
}

/// `DURUST_REQUIRE_POSTGRES` can only fail a Postgres test that was compiled.
///
/// Every Postgres test in this workspace sits behind `#[cfg(feature =
/// "postgres")]`, so a run that drops the feature contains no Postgres tests at
/// all and the require flag has nothing to fire on — the same vacuous pass the
/// flag exists to abolish, one level up. This test is outside the `cfg`, so it
/// is the one Postgres assertion that survives the feature being switched off.
///
/// Two honest limits on what it covers. It lives in an **integration** target,
/// so it cannot fire for a `--lib`-only invocation; it protects
/// `--workspace` and `--test provider_conformance` runs, which is what CI uses.
/// And `cargo test --workspace` without `--all-features` still enables
/// `postgres` on the lib anyway, through feature unification with
/// `benchtools`, whose dependency on `durust` names that feature — so the
/// scenario this guards is narrower than "someone forgot `--all-features`".
#[test]
fn postgres_feature_is_enabled_when_postgres_is_required() {
    if !postgres_is_required() {
        return;
    }
    assert!(
        cfg!(feature = "postgres"),
        "DURUST_REQUIRE_POSTGRES is set, but this binary was built without the `postgres` \
         feature, so every Postgres test was compiled out and the run proves nothing. Add \
         `--features postgres` or `--all-features`."
    );
}

/// Deliberately outside `#[cfg(feature = "postgres")]`: the test above needs it
/// in a build that has no Postgres support at all.
fn postgres_is_required() -> bool {
    env_flag_is_on("DURUST_REQUIRE_POSTGRES")
}

/// `DURUST_REQUIRE_GARAGE` can only fail a Garage test that was compiled, and
/// the Garage test lives behind `#[cfg(feature = "s3")]` — so a build without
/// that feature contains no Garage test and the flag has nothing to fire on.
///
/// That hole is wider here than it is for Postgres, because CI's Garage step
/// selects a *single* test by name filter. `cargo test <filter>` that matches
/// nothing prints `0 passed` and **exits 0** (measured, not assumed): drop the
/// feature, or rename the conformance test, and the step stays green having
/// run no S3 at all. Two independent ways to pass vacuously, and the container
/// coming up healthy disguises both.
///
/// So this test sits outside the `cfg`, and CI filters on the substring
/// `garage` rather than the full test name. This test's own name contains it,
/// so the filter always matches at least one test, that test always compiles,
/// and it fails when the feature is gone. A filter that can never match zero
/// tests is the part that makes the rest of the guard reachable.
#[test]
fn garage_s3_feature_is_enabled_when_garage_is_required() {
    if !garage_is_required() {
        return;
    }
    assert!(
        cfg!(feature = "s3"),
        "DURUST_REQUIRE_GARAGE is set, but this binary was built without the `s3` feature, so \
         the Garage conformance test was compiled out and the run proves nothing. Add \
         `--features s3` or `--all-features`."
    );
}

/// Deliberately outside `#[cfg(feature = "s3")]`: the test above needs it in a
/// build that has no S3 support at all.
fn garage_is_required() -> bool {
    env_flag_is_on("DURUST_REQUIRE_GARAGE")
}

/// The on/off reading of a `DURUST_REQUIRE_*` switch, shared by every flag in
/// this file so that no two of them can drift apart.
///
/// On for any value except unset, empty, `0`, and `false` (case-insensitive).
/// That an unrecognized value reads as *on* is the deliberate part: a typo in
/// `DURUST_REQUIRE_GARAGE=ture` runs the gated work rather than silently
/// dropping it. For a switch whose only job is to stop a suite passing
/// vacuously, failing toward more coverage is the sole safe direction.
fn env_flag_is_on(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) => {
            let value = value.trim();
            !(value.is_empty() || value == "0" || value.eq_ignore_ascii_case("false"))
        }
        Err(_) => false,
    }
}

#[cfg(feature = "postgres")]
fn postgres_test_schema(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("durust_{prefix}_{}_{}", std::process::id(), millis)
}

#[cfg(feature = "postgres")]
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

#[cfg(feature = "postgres")]
fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Connects a Postgres backend bound to a fresh, uniquely named schema and runs
/// `body` against it, or skips exactly as a hand-written `postgres_url_or_skip`
/// block does.
///
/// The skip is still a skip and `DURUST_REQUIRE_POSTGRES` is still armed,
/// because the decision is still `postgres_url_or_skip`'s, called with the
/// caller's own `what` string: with `DURUST_POSTGRES_URL` unset the test returns
/// early having done nothing, and with `DURUST_REQUIRE_POSTGRES` set that same
/// call panics naming the caller. Nothing about that gate moved into this
/// function; only the connect boilerplate did.
///
/// Teardown deliberately reproduces what the inline blocks did rather than
/// improving on it: the schema is dropped after a body that *returns*, and is
/// **left behind** when the body panics, because a sequential
/// `drop_postgres_schema` at the end of an async block never ran on the panic
/// path either. Catching the unwind to drop it would change which server-side
/// state a failing run leaves for inspection, which is a behaviour change and
/// not this refactor's business.
#[cfg(feature = "postgres")]
async fn with_postgres_schema<F, Fut, R>(what: &str, prefix: &str, body: F) -> PostgresRun<R>
where
    F: FnOnce(PostgresBackend, String, String) -> Fut,
    Fut: Future<Output = R>,
{
    let Some(url) = postgres_url_or_skip(what) else {
        return PostgresRun::SkippedNoPostgres;
    };
    let schema = postgres_test_schema(prefix);
    let backend = PostgresBackend::connect_with_config(
        PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
    )
    .await
    .unwrap();
    let ran = body(backend, url.clone(), schema.clone()).await;
    drop_postgres_schema(&url, &schema).await;
    PostgresRun::Ran(ran)
}

/// What `with_postgres_schema` did — as a value the compiler will not let that
/// function fabricate.
///
/// Folding 22 tests behind one helper concentrates their failure mode as well as
/// their boilerplate. Were the `body(..)` call simply deleted from
/// `with_postgres_schema`, all 22 would report `ok` having exercised nothing, and
/// rustc would say only that a parameter went unused — a warning, in a suite that
/// already has some. That is this repository's own recurring defect, moved up one
/// level, and it did not exist while each test carried its own inline body.
///
/// `R` closes it. A type parameter cannot be conjured, so the single way to
/// produce `Ran(R)` is to call `body`. Deleting the call stops compiling; keeping
/// the shortcut means writing `SkippedNoPostgres` unconditionally, which is a
/// deliberate line that says what it does rather than an absence nobody reviews.
///
/// Not `#[must_use]`, deliberately: the tests are right to ignore this, and 22
/// `let _ =` bindings would only train people to ignore it. `dead_code` is
/// allowed for the same reason — the payload exists to constrain this file's
/// helper, not to be read back.
#[cfg(feature = "postgres")]
#[allow(dead_code)]
enum PostgresRun<R> {
    Ran(R),
    SkippedNoPostgres,
}

/// `with_postgres_schema` for the majority of tests that never name the URL or
/// the schema after connecting.
#[cfg(feature = "postgres")]
async fn with_postgres<F, Fut, R>(what: &str, prefix: &str, body: F) -> PostgresRun<R>
where
    F: FnOnce(PostgresBackend) -> Fut,
    Fut: Future<Output = R>,
{
    with_postgres_schema(what, prefix, |backend, _url, _schema| body(backend)).await
}

#[durust::activity(name = "conformance.echo")]
async fn echo(input: Input) -> durust::Result<u64> {
    Ok(input.value)
}

#[durust::workflow(name = "conformance.workflow", version = 1)]
async fn workflow(input: Input) -> durust::Result<u64> {
    durust::call_activity!(echo(Input { value: input.value })).await
}

#[durust::workflow(name = "conformance.signal-race", version = 1)]
async fn signal_race_workflow(_: Input) -> durust::Result<String> {
    durust::signal::<String>("go").await
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PageItem {
    label: String,
}

#[durust::activity(name = "conformance.page-item-len")]
async fn page_item_len(input: PageItem) -> durust::Result<u64> {
    Ok(input.label.len() as u64)
}

// Items are ~600 bytes each so a page of five overflows a 2 KiB decorator
// threshold (page offloaded to the custom scheme) while the manifest root —
// a handful of page refs — stays inline.
#[durust::workflow(name = "conformance.foreign-page-map", version = 1)]
async fn foreign_page_map_workflow(input: Input) -> durust::Result<u64> {
    let items = (0..input.value).map(|ordinal| PageItem {
        label: format!("{ordinal:0>600}"),
    });
    let input_manifest = durust::activity_map_manifest(items)?;
    let mapped = durust::activity_map(page_item_len)
        .task_queue("foreign-page-map-activities")
        .input_manifest(input_manifest)
        .max_in_flight(2)
        .result_manifest("page-item-lens")
        .spawn()
        .await?;
    let result_manifest = mapped.result_manifest().await?;
    let result_refs = durust::decode_activity_map_result_refs(&result_manifest)?;
    result_refs.iter().try_fold(0_u64, |sum, payload| {
        Ok(sum + durust::decode_payload::<u64>(payload)?)
    })
}

mod default_name_handlers {
    #[durust::activity]
    pub async fn default_activity(_: DefaultInput) -> durust::Result<()> {
        Ok(())
    }

    #[durust::workflow(version = 1)]
    pub async fn default_workflow(_: DefaultInput) -> durust::Result<()> {
        Ok(())
    }

    #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
    pub struct DefaultInput {}
}

#[test]
fn memory_provider_passes_basic_conformance() {
    block_on(provider_conformance(MemoryBackend::new()));
}

#[test]
fn sqlite_provider_passes_basic_conformance() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("conformance.sqlite3")).unwrap();
        provider_conformance(backend).await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_provider_passes_basic_conformance_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres provider conformance",
        "conformance",
        |backend| async move {
            provider_conformance(backend).await;
        },
    ));
}

#[test]
fn sqlite_activity_heartbeat_deadline_persists_across_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("heartbeat-reopen.sqlite3");
        let backend = SqliteBackend::open(&path).unwrap();
        let (run_id, claim_opts, activity_opts) = schedule_heartbeat_activity(
            backend.clone(),
            "wf/sqlite-heartbeat-reopen",
            "sqlite-heartbeat-reopen-workflows",
            "sqlite-heartbeat-reopen-activities",
            durust::RetryPolicy::exponential().max_attempts(1),
        )
        .await;

        let activity = backend
            .claim_activity_task(WorkerId::new("sqlite-heartbeat-worker"), activity_opts)
            .await
            .unwrap()
            .expect("heartbeat activity");
        assert_eq!(activity.task.attempt, 1);
        let claimed_at = backend.current_time().await.unwrap();
        drop(backend);

        let reopened = SqliteBackend::open(&path).unwrap();
        let outcome = reopened
            .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
                namespace: Namespace::default(),
                now: durust::TimestampMs(claimed_at.0.saturating_add(500)),
                limit: 16,
            })
            .await
            .unwrap();
        assert_eq!(outcome.timed_out, 1);

        let ready = reopened
            .claim_workflow_task(WorkerId::new("sqlite-heartbeat-ready"), claim_opts)
            .await
            .unwrap()
            .expect("workflow task after heartbeat timeout");
        assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityTimedOut);

        let history = stream_history(&reopened, run_id).await;
        let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
            panic!("expected ActivityTimedOut event after reopen");
        };
        assert!(timed_out.message.contains("missed heartbeat"));
    });
}

#[test]
fn memory_workflow_lease_expiry_reclaims_and_fences_stale_holder() {
    block_on(async {
        let backend = MemoryBackend::new();
        let advance_backend = backend.clone();
        workflow_lease_expiry_reclaims_and_fences_stale_holder(
            backend,
            "wf/memory-lease-expiry",
            "memory-lease-expiry-workflows",
            Duration::from_secs(30),
            |backend| async move { backend },
            move || async move {
                advance_backend.advance_time(Duration::from_secs(31));
            },
        )
        .await;
    });
}

#[test]
fn sqlite_workflow_lease_expiry_reclaims_and_fences_stale_holder_across_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease-expiry.sqlite3");
        let backend = SqliteBackend::open(&path).unwrap();
        workflow_lease_expiry_reclaims_and_fences_stale_holder(
            backend,
            "wf/sqlite-lease-expiry",
            "sqlite-lease-expiry-workflows",
            Duration::from_millis(50),
            move |backend| async move {
                drop(backend);
                SqliteBackend::open(&path).unwrap()
            },
            || async { std::thread::sleep(Duration::from_millis(120)) },
        )
        .await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_workflow_lease_expiry_reclaims_and_fences_stale_holder_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres lease expiry conformance",
        "lease_expiry",
        |backend| async move {
            workflow_lease_expiry_reclaims_and_fences_stale_holder(
                backend,
                "wf/postgres-lease-expiry",
                "postgres-lease-expiry-workflows",
                Duration::from_millis(50),
                |backend| async move { backend },
                || async { tokio::time::sleep(Duration::from_millis(120)).await },
            )
            .await;
        },
    ));
}

#[test]
fn memory_delayed_released_workflow_task_visibility_follows_virtual_clock() {
    block_on(async {
        let backend = MemoryBackend::new();
        let advance_backend = backend.clone();
        delayed_released_workflow_task_is_not_claimable_until_visible(
            backend,
            "wf/memory-delayed-release",
            "memory-delayed-release-workflows",
            move || async move {
                advance_backend.advance_time(Duration::from_millis(40));
            },
        )
        .await;
    });
}

#[test]
fn sqlite_delayed_released_workflow_task_is_not_claimable_until_visible() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("delayed-release.sqlite3")).unwrap();
        delayed_released_workflow_task_is_not_claimable_until_visible(
            backend,
            "wf/sqlite-delayed-release",
            "sqlite-delayed-release-workflows",
            || async { std::thread::sleep(Duration::from_millis(40)) },
        )
        .await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_delayed_released_workflow_task_is_not_claimable_until_visible_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres delayed release conformance",
        "delayed_release",
        |backend| async move {
            delayed_released_workflow_task_is_not_claimable_until_visible(
                backend,
                "wf/postgres-delayed-release",
                "postgres-delayed-release-workflows",
                || async { tokio::time::sleep(Duration::from_millis(40)).await },
            )
            .await;
        },
    ));
}

#[test]
fn sqlite_delayed_workflow_task_visibility_persists_across_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delayed-visibility.sqlite3");
        let backend = SqliteBackend::open(&path).unwrap();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<workflow>(
                "wf/sqlite-delayed-visibility",
                "sqlite-delayed-visibility-workflows",
                input(5),
            )
            .await
            .unwrap();
        let claim_opts = workflow_claim_opts("sqlite-delayed-visibility-workflows");
        let claimed = backend
            .claim_workflow_task(WorkerId::new("sqlite-delayed-worker-a"), claim_opts.clone())
            .await
            .unwrap()
            .expect("workflow task");
        backend
            .release_workflow_task(
                claimed.claim,
                durust::WorkflowTaskRelease::delayed(
                    durust::WorkflowTaskReason::CacheEvicted,
                    Duration::from_millis(25),
                ),
            )
            .await
            .unwrap();
        drop(backend);

        let reopened = SqliteBackend::open(&path).unwrap();
        let hidden = reopened
            .claim_workflow_task(WorkerId::new("sqlite-delayed-worker-b"), claim_opts.clone())
            .await
            .unwrap();
        assert!(hidden.is_none());

        std::thread::sleep(Duration::from_millis(40));
        let visible = reopened
            .claim_workflow_task(WorkerId::new("sqlite-delayed-worker-c"), claim_opts)
            .await
            .unwrap();
        assert!(visible.is_some());
    });
}

#[test]
fn memory_activity_retry_backoff_follows_virtual_clock() {
    block_on(async {
        let backend = MemoryBackend::new();
        let advance_backend = backend.clone();
        exponential_backoff_hides_retry_until_visible(
            backend.clone(),
            backend,
            "memory",
            move |activity_opts| async move {
                // Pin the exact visibility boundary: one millisecond before
                // the 1s first-attempt backoff the retry is still hidden.
                advance_backend.advance_time(Duration::from_millis(999));
                let hidden = advance_backend
                    .claim_activity_task(WorkerId::new("memory-backoff-boundary"), activity_opts)
                    .await
                    .unwrap();
                assert!(hidden.is_none(), "retry visible before its backoff elapsed");
                advance_backend.advance_time(Duration::from_millis(1));
            },
        )
        .await;
    });
}

#[test]
fn sqlite_activity_retry_backoff_persists_across_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retry-backoff.sqlite3");
        let backend = SqliteBackend::open(&path).unwrap();
        // The retry side runs on a reopened provider so the visibility
        // deadline is proven to live in the database, not in memory.
        let reopened = SqliteBackend::open(&path).unwrap();
        exponential_backoff_hides_retry_until_visible(backend, reopened, "sqlite", |_| async {
            std::thread::sleep(Duration::from_millis(1_000));
        })
        .await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_activity_retry_backoff_delays_reclaim_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres retry backoff conformance",
        "retry_backoff",
        |backend| async move {
            exponential_backoff_hides_retry_until_visible(
                backend.clone(),
                backend,
                "postgres",
                |_| async {
                    tokio::time::sleep(Duration::from_millis(1_000)).await;
                },
            )
            .await;
        },
    ));
}

// Shared exponential-backoff suite: a failing activity's retry is hidden from
// claims until `now + base * 2^(attempt - 1)`, claimable afterwards, and the
// workflow still completes on the retried attempt. `pass_backoff` moves the
// provider clock past the 1s first-attempt backoff (virtual time for memory,
// wall-clock waiting for the SQL providers); `retry_backend` performs every
// post-failure call so SQLite can prove the persisted deadline across a
// reopen.
async fn exponential_backoff_hides_retry_until_visible<B, R, F, Fut>(
    backend: B,
    retry_backend: R,
    prefix: &str,
    pass_backoff: F,
) where
    B: DurableBackend,
    R: DurableBackend,
    F: FnOnce(ClaimActivityOptions) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let client = Client::new(backend.clone());
    let workflow_queue = format!("{prefix}-backoff-workflows");
    let activity_queue = format!("{prefix}-backoff-activities");
    let run_id = client
        .start_workflow::<workflow>(format!("wf/{prefix}-backoff"), &workflow_queue, input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts(&workflow_queue);
    let claimed = backend
        .claim_workflow_task(WorkerId::new("backoff-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new(&activity_queue),
        retry_policy: durust::RetryPolicy::exponential().max_attempts(2),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityScheduled(scheduled.clone()),
                )],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(&activity_queue),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(WorkerId::new("backoff-worker-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(first.task.attempt, 1);
    assert_eq!(
        backend
            .fail_activity(FailActivityRequest {
                claim: first.claim,
                failure: durust::DurableFailure::new("test.transient", "transient"),
            })
            .await
            .unwrap(),
        durust::FailActivityOutcome::RetryScheduled { next_attempt: 2 }
    );

    // The scheduled retry exists but is hidden until its backoff elapses.
    let hidden = retry_backend
        .claim_activity_task(WorkerId::new("backoff-worker-2"), activity_opts.clone())
        .await
        .unwrap();
    assert!(hidden.is_none(), "retry visible before its backoff elapsed");

    pass_backoff(activity_opts.clone()).await;

    // Wall clocks are inexact, so the SQL providers poll for the deadline;
    // memory succeeds on the first claim because virtual time has passed it.
    let mut second = None;
    for _ in 0..200 {
        if let Some(claimed) = retry_backend
            .claim_activity_task(WorkerId::new("backoff-worker-3"), activity_opts.clone())
            .await
            .unwrap()
        {
            second = Some(claimed);
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let second = second.expect("retry should become claimable after its backoff");
    assert_eq!(second.task.attempt, 2);
    retry_backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap();

    // The workflow wakes on the retried completion and still runs to a
    // terminal state.
    let ready = retry_backend
        .claim_workflow_task(WorkerId::new("backoff-finisher"), claim_opts)
        .await
        .unwrap()
        .expect("activity completion should wake workflow");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityCompleted);
    retry_backend
        .commit_workflow_task(
            ready.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: ready.replay_target_event_id,
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCompleted {
                        result: durust::encode_payload(&9_u64).unwrap(),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    let history = stream_history(&retry_backend, run_id).await;
    assert!(matches!(
        history.last().expect("terminal event").data,
        HistoryEventData::WorkflowCompleted { .. }
    ));
    // The retried attempt left no failure event behind.
    assert!(!history.iter().any(|event| matches!(
        event.data,
        HistoryEventData::ActivityFailed(_) | HistoryEventData::ActivityTimedOut(_)
    )));
}

#[test]
fn memory_provider_offloads_large_payloads_and_hydrates_public_apis() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/memory-payload-offload",
            "memory-payload-workflows",
            "memory-payload-activities",
        )
        .await;
        payload_offload_child_workflow_round_trip(backend.clone(), "memory").await;
        payload_offload_child_workflow_map_round_trip(backend.clone(), "memory").await;
        payload_offload_activity_map_round_trip(backend.clone(), "memory").await;
        let _ = payload_gc_removes_unreachable_projection_blob(backend.clone(), "memory").await;
        assert!(backend.payload_blob_count() >= 4);
    });
}

#[test]
fn memory_provider_replay_stream_keeps_large_payloads_lazy_until_explicit_hydration() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let run_id = start_large_payload_workflow(
            backend.clone(),
            "wf/memory-lazy-replay-payload",
            "memory-lazy-replay-workflows",
        )
        .await;
        assert_replay_stream_payload_hydrates_explicitly(backend, run_id).await;
    });
}

#[test]
fn memory_provider_keeps_side_effect_marker_inline_when_threshold_would_offload() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        assert_side_effect_marker_stays_inline_for_replay_stream(
            backend.clone(),
            "wf/memory-side-effect-inline",
            "memory-side-effect-workflows",
        )
        .await;
        assert_eq!(backend.payload_blob_count(), 0);
    });
}

#[test]
fn payload_backend_offloads_large_payloads_and_hydrates_memory_provider_apis() {
    block_on(async {
        let blob_store = durust::MemoryBlobStore::new();
        let backend = PayloadBackend::with_payload_storage(
            MemoryBackend::new(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/payload-backend-memory-offload",
            "payload-backend-memory-workflows",
            "payload-backend-memory-activities",
        )
        .await;
        payload_offload_child_workflow_round_trip(backend.clone(), "payload-backend-memory").await;
        payload_offload_child_workflow_map_round_trip(backend.clone(), "payload-backend-memory")
            .await;
        payload_offload_activity_map_round_trip(backend.clone(), "payload-backend-memory").await;
        let _ =
            payload_gc_removes_unreachable_projection_blob(backend, "payload-backend-memory").await;
        assert!(blob_store.payload_blob_count() >= 8);
    });
}

#[test]
fn sqlite_provider_offloads_large_payloads_and_hydrates_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload-offload.sqlite3");
        let config = durust::PayloadStorageConfig::new().inline_threshold_bytes(1);
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        let run_id = payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/sqlite-payload-offload",
            "sqlite-payload-workflows",
            "sqlite-payload-activities",
        )
        .await;
        payload_offload_child_workflow_round_trip(backend.clone(), "sqlite").await;
        payload_offload_child_workflow_map_round_trip(backend.clone(), "sqlite").await;
        payload_offload_activity_map_round_trip(backend.clone(), "sqlite").await;
        let (gc_workflow_id, gc_projection) =
            payload_gc_removes_unreachable_projection_blob(backend.clone(), "sqlite").await;
        let blob_count = backend.payload_blob_count().unwrap();
        assert!(blob_count >= 12);
        drop(backend);

        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        assert_eq!(reopened.payload_blob_count().unwrap(), blob_count);
        let projection = reopened
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(gc_workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected retained projection after reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            gc_projection
        );
        let history = stream_history(&reopened, run_id).await;
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected hydrated workflow start");
        };
        assert_eq!(
            durust::decode_payload::<String>(input).unwrap(),
            large_payload("workflow-input")
        );
    });
}

#[test]
fn sqlite_provider_replay_stream_keeps_large_payloads_lazy_until_explicit_hydration_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lazy-replay-payload.sqlite3");
        let config = durust::PayloadStorageConfig::new().inline_threshold_bytes(1);
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        let run_id = start_large_payload_workflow(
            backend.clone(),
            "wf/sqlite-lazy-replay-payload",
            "sqlite-lazy-replay-workflows",
        )
        .await;
        drop(backend);

        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        assert_replay_stream_payload_hydrates_explicitly(reopened, run_id).await;
    });
}

#[test]
fn sqlite_provider_keeps_side_effect_marker_inline_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("side-effect-inline.sqlite3");
        let config = durust::PayloadStorageConfig::new().inline_threshold_bytes(1);
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        assert_side_effect_marker_stays_inline_for_replay_stream(
            backend.clone(),
            "wf/sqlite-side-effect-inline",
            "sqlite-side-effect-workflows",
        )
        .await;
        assert_eq!(backend.payload_blob_count().unwrap(), 0);
        drop(backend);

        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        assert_side_effect_marker_stays_inline_in_existing_history(
            reopened,
            "wf/sqlite-side-effect-inline",
        )
        .await;
    });
}

#[test]
fn payload_backend_wraps_sqlite_and_hydrates_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload-wrapper.sqlite3");
        let blob_store = durust::MemoryBlobStore::new();
        let config = durust::PayloadStorageConfig::new().inline_threshold_bytes(1);
        let backend = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store.clone(),
            config.clone(),
        );
        let run_id = payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/payload-backend-sqlite-offload",
            "payload-backend-sqlite-workflows",
            "payload-backend-sqlite-activities",
        )
        .await;
        payload_offload_child_workflow_map_round_trip(backend.clone(), "payload-backend-sqlite")
            .await;
        payload_offload_activity_map_round_trip(backend.clone(), "payload-backend-sqlite").await;
        let (gc_workflow_id, gc_projection) =
            payload_gc_removes_unreachable_projection_blob(backend, "payload-backend-sqlite").await;
        assert!(blob_store.payload_blob_count() >= 8);

        let reopened = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store,
            config,
        );
        let history = stream_history(&reopened, run_id).await;
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected hydrated workflow start after wrapper reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(input).unwrap(),
            large_payload("workflow-input")
        );
        let projection = reopened
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(gc_workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected retained wrapper projection after reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            gc_projection
        );
    });
}

#[cfg(feature = "s3")]
#[test]
fn payload_backend_over_sqlite_passes_garage_s3_conformance_when_configured() {
    block_on_tokio(async {
        let Some(garage) = garage_config_or_skip("Garage S3 conformance") else {
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload-wrapper-garage.sqlite3");
        let blob_store = durust::S3BlobStore::garage(garage).unwrap();
        wait_for_blob_store(&blob_store).await;
        let config = durust::PayloadStorageConfig::new().inline_threshold_bytes(1);
        let backend = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store.clone(),
            config.clone(),
        );
        let run_id = payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/payload-backend-garage-offload",
            "payload-backend-garage-workflows",
            "payload-backend-garage-activities",
        )
        .await;
        payload_offload_child_workflow_round_trip(backend.clone(), "payload-backend-garage").await;
        payload_offload_child_workflow_map_round_trip(backend.clone(), "payload-backend-garage")
            .await;
        payload_offload_activity_map_round_trip(backend.clone(), "payload-backend-garage").await;
        let (gc_workflow_id, gc_projection) =
            payload_gc_removes_unreachable_projection_blob(backend, "payload-backend-garage").await;
        let external_blobs = durust::PayloadBlobStore::list_payload_blobs(&blob_store)
            .await
            .unwrap();
        assert!(external_blobs.len() >= 8);

        let reopened = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store,
            config,
        );
        let history = stream_history(&reopened, run_id).await;
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected hydrated workflow start after Garage wrapper reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(input).unwrap(),
            large_payload("workflow-input")
        );
        let projection = reopened
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(gc_workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected retained Garage projection after reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            gc_projection
        );
    });
}

#[cfg(feature = "s3")]
#[test]
fn payload_backend_s3_upload_failure_does_not_commit_missing_payload_ref() {
    block_on_tokio(async {
        let inner = MemoryBackend::new();
        let blob_store = durust::S3BlobStore::garage(durust::S3BlobStoreConfig {
            bucket: "durust-payloads".to_owned(),
            endpoint: "http://127.0.0.1:9".to_owned(),
            region: "garage".to_owned(),
            prefix: "payloads".to_owned(),
            access_key_id: "GK0123456789abcdef0123456789abcdef".to_owned(),
            secret_access_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
        })
        .unwrap();
        let backend = PayloadBackend::with_payload_storage(
            inner.clone(),
            blob_store,
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let err = backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("wf/payload-backend-s3-upload-failure"),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new("workflows"),
                input: durust::encode_payload(&large_payload("workflow-input")).unwrap(),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Backend(message) if message.contains("S3 payload store error"))
        );
        let claim = inner
            .claim_workflow_task(
                WorkerId::new("payload-backend-s3-upload-failure-worker"),
                workflow_claim_opts("workflows"),
            )
            .await
            .unwrap();
        assert!(claim.is_none());
    });
}

fn block_on_tokio<F>(future: F) -> F::Output
where
    F: Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/// The Garage connection settings, or `None` when this run is not expected to
/// have a Garage.
///
/// Same shape and same reason as `postgres_url_or_skip`: libtest captures
/// `eprintln!` on a passing test, so with the variables unset this returns
/// `None`, the caller returns early, and the run reports `ok. 1 passed` having
/// touched no S3 at all. `DURUST_REQUIRE_GARAGE` turns that into a panic.
///
/// The panic names the variables that are actually missing rather than the
/// `DURUST_GARAGE_*` family, because a dropped `DURUST_GARAGE_BUCKET` and a
/// Garage container that never came up are different failures and should not
/// print the same sentence.
///
/// Blank is missing. The four required reads used `env::var(..).ok()?`, which
/// accepts `DURUST_GARAGE_ENDPOINT=""` as a value and hands an empty endpoint
/// to the client; the Postgres helper above has always rejected empty, and
/// there is no reason for the two to disagree about what "set" means.
#[cfg(feature = "s3")]
fn garage_config_or_skip(what: &str) -> Option<durust::S3BlobStoreConfig> {
    const REQUIRED: [&str; 4] = [
        "DURUST_GARAGE_ENDPOINT",
        "DURUST_GARAGE_BUCKET",
        "DURUST_GARAGE_ACCESS_KEY_ID",
        "DURUST_GARAGE_SECRET_ACCESS_KEY",
    ];
    let missing: Vec<&str> = REQUIRED
        .into_iter()
        .filter(|name| non_empty_env(name).is_none())
        .collect();
    if !missing.is_empty() {
        let missing = missing.join(", ");
        assert!(
            !garage_is_required(),
            "DURUST_REQUIRE_GARAGE is set, so `{what}` must run, \
             but these are unset or empty: {missing}"
        );
        eprintln!("skipping {what}; set {missing}");
        return None;
    }
    Some(durust::S3BlobStoreConfig {
        bucket: non_empty_env("DURUST_GARAGE_BUCKET").expect("checked above"),
        endpoint: non_empty_env("DURUST_GARAGE_ENDPOINT").expect("checked above"),
        region: env::var("DURUST_GARAGE_REGION").unwrap_or_else(|_| "garage".to_owned()),
        prefix: env::var("DURUST_GARAGE_PREFIX").unwrap_or_else(|_| "payloads".to_owned()),
        access_key_id: non_empty_env("DURUST_GARAGE_ACCESS_KEY_ID").expect("checked above"),
        secret_access_key: non_empty_env("DURUST_GARAGE_SECRET_ACCESS_KEY").expect("checked above"),
    })
}

#[cfg(feature = "s3")]
fn non_empty_env(name: &str) -> Option<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

#[cfg(feature = "s3")]
async fn wait_for_blob_store<S>(blob_store: &S)
where
    S: durust::PayloadBlobStore,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_error = None;
    while Instant::now() < deadline {
        match blob_store.list_payload_blobs().await {
            Ok(_) => return,
            Err(err) => {
                last_error = Some(err);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    panic!("Garage S3 blob store did not become ready: {last_error:?}");
}

#[test]
fn payload_backend_upload_failure_does_not_commit_missing_payload_ref() {
    block_on(async {
        let inner = MemoryBackend::new();
        let backend = PayloadBackend::with_payload_storage(
            inner.clone(),
            FailingBlobStore,
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let err = backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("wf/payload-backend-upload-failure"),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new("workflows"),
                input: durust::encode_payload(&large_payload("workflow-input")).unwrap(),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Backend(message) if message.contains("intentional blob upload failure"))
        );
        let claim = inner
            .claim_workflow_task(
                WorkerId::new("payload-backend-upload-failure-worker"),
                workflow_claim_opts("workflows"),
            )
            .await
            .unwrap();
        assert!(claim.is_none());
    });
}

#[derive(Clone, Debug)]
struct FailingBlobStore;

impl durust::PayloadBlobStore for FailingBlobStore {
    fn put_payload_blob(
        &self,
        _digest: String,
        _bytes: Vec<u8>,
    ) -> BoxFuture<'static, durust::Result<String>> {
        Box::pin(ready(Err(Error::Backend(
            "intentional blob upload failure".to_owned(),
        ))))
    }

    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<Vec<u8>>> {
        Box::pin(ready(Err(Error::PayloadDecode(format!(
            "missing payload blob `{digest}`"
        )))))
    }

    fn payload_blob_exists(&self, _digest: String) -> BoxFuture<'static, durust::Result<bool>> {
        Box::pin(ready(Ok(false)))
    }

    fn list_payload_blobs(
        &self,
    ) -> BoxFuture<'static, durust::Result<BTreeMap<String, durust::TimestampMs>>> {
        Box::pin(ready(Ok(BTreeMap::new())))
    }

    fn delete_payload_blob(&self, _digest: String) -> BoxFuture<'static, durust::Result<()>> {
        Box::pin(ready(Ok(())))
    }

    fn owns_payload_blob_uri(&self, _uri: &str) -> bool {
        false
    }
}

// A blob store with a scheme none of the built-in providers know about. Inner
// providers must treat its refs as opaque; only this store hydrates or
// garbage-collects them.
#[derive(Clone, Debug, Default)]
struct TestCustomBlobStore {
    blobs: Arc<Mutex<BTreeMap<String, (Vec<u8>, durust::TimestampMs)>>>,
}

impl TestCustomBlobStore {
    fn blob_count(&self) -> usize {
        self.blobs.lock().unwrap().len()
    }
}

fn wall_clock_ms() -> durust::TimestampMs {
    durust::TimestampMs(
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX),
    )
}

impl durust::PayloadBlobStore for TestCustomBlobStore {
    fn put_payload_blob(
        &self,
        digest: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, durust::Result<String>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            let now = wall_clock_ms();
            let mut blobs = blobs.lock().unwrap();
            match blobs.get_mut(&digest) {
                Some(record) => record.1 = now,
                None => {
                    blobs.insert(digest.clone(), (bytes, now));
                }
            }
            Ok(format!("test-custom://payload/{digest}"))
        })
    }

    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<Vec<u8>>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            blobs
                .lock()
                .unwrap()
                .get(&digest)
                .map(|(bytes, _)| bytes.clone())
                .ok_or_else(|| Error::PayloadDecode(format!("missing payload blob `{digest}`")))
        })
    }

    fn payload_blob_exists(&self, digest: String) -> BoxFuture<'static, durust::Result<bool>> {
        let blobs = self.blobs.clone();
        Box::pin(async move { Ok(blobs.lock().unwrap().contains_key(&digest)) })
    }

    fn list_payload_blobs(
        &self,
    ) -> BoxFuture<'static, durust::Result<BTreeMap<String, durust::TimestampMs>>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            Ok(blobs
                .lock()
                .unwrap()
                .iter()
                .map(|(digest, (_, last_modified))| (digest.clone(), *last_modified))
                .collect())
        })
    }

    fn delete_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<()>> {
        let blobs = self.blobs.clone();
        Box::pin(async move {
            blobs.lock().unwrap().remove(&digest);
            Ok(())
        })
    }

    fn owns_payload_blob_uri(&self, uri: &str) -> bool {
        uri.starts_with("test-custom://payload/")
    }
}

// Bug B pin: a custom-scheme blob store must work over any inner provider.
// Against the pre-fix scheme allowlist this fails at the first commit because
// the inner provider tries to resolve `test-custom://` refs from its own
// store.
async fn custom_scheme_blob_store_round_trips_and_survives_gc<B>(inner: B, prefix: &str)
where
    B: DurableBackend,
{
    let blob_store = TestCustomBlobStore::default();
    let backend = PayloadBackend::with_payload_storage(
        inner,
        blob_store.clone(),
        durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
    );
    let run_id = payload_offload_public_api_round_trip(
        backend.clone(),
        &format!("wf/{prefix}-custom-scheme"),
        &format!("{prefix}-custom-scheme-workflows"),
        &format!("{prefix}-custom-scheme-activities"),
    )
    .await;
    payload_offload_activity_map_round_trip(backend.clone(), &format!("{prefix}-custom")).await;
    assert!(blob_store.blob_count() >= 4);

    // Raw replay refs carry the custom scheme end to end.
    let raw_events = backend
        .stream_history_for_replay(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &raw_events[0].data else {
        panic!("expected raw workflow start event");
    };
    assert!(
        matches!(input, durust::PayloadRef::Blob { uri, .. } if uri.starts_with("test-custom://payload/")),
        "raw workflow input should be a custom-scheme blob ref, got {input:?}"
    );

    // GC with a zero grace period must still leave every reachable
    // custom-scheme blob alone.
    let before = blob_store.blob_count();
    let outcome = backend
        .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .await
        .unwrap();
    assert_eq!(outcome.failed_blobs, 0);
    assert_eq!(blob_store.blob_count(), before - outcome.deleted_blobs);

    // Hydration after GC proves reachable blobs survived.
    let history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
        panic!("expected hydrated workflow start event");
    };
    assert_eq!(
        durust::decode_payload::<String>(input).unwrap(),
        large_payload("workflow-input")
    );
}

// 4F pin: a decorator-owned (foreign-scheme) blob page under an inline
// manifest root must commit and run to completion on every inner provider.
// Against the pre-fix normalize functions the first commit fails with "blob
// payload must be hydrated by the durability provider before decode" (the
// inner provider tries to decode the foreign page) and the task poison-loops.
async fn inline_root_foreign_page_activity_map_completes<B>(inner: B, prefix: &str)
where
    B: DurableBackend,
{
    let blob_store = TestCustomBlobStore::default();
    let backend = PayloadBackend::with_payload_storage(
        inner,
        blob_store.clone(),
        durust::PayloadStorageConfig::new().inline_threshold_bytes(2048),
    );
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<foreign_page_map_workflow>(
            &format!("wf/{prefix}-foreign-page-map"),
            "foreign-page-map-workflows",
            input(5),
        )
        .await
        .unwrap();

    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue("foreign-page-map-workflows")
        .activity_task_queue("foreign-page-map-activities")
        .register_workflow(foreign_page_map_workflow)
        .register_activity(page_item_len)
        .build();
    worker.run_until_idle().await.unwrap();

    // Shape guard: the raw recorded manifest must be an inline root holding
    // at least one foreign-scheme blob page, or this test stops exercising
    // the guarded path.
    let raw_events = backend
        .stream_history_for_replay(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let scheduled = raw_events
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ActivityMapScheduled(scheduled) => Some(scheduled),
            _ => None,
        })
        .expect("activity map scheduled event");
    assert!(
        matches!(&scheduled.input_manifest, durust::PayloadRef::Inline { .. }),
        "manifest root must stay inline, got {:?}",
        scheduled.input_manifest
    );
    let manifest: ActivityMapInputManifest =
        durust::decode_payload(&scheduled.input_manifest).unwrap();
    assert!(!manifest.pages.is_empty());
    for page in &manifest.pages {
        assert!(
            matches!(page, durust::PayloadRef::Blob { uri, .. } if uri.starts_with("test-custom://payload/")),
            "pages must be foreign-scheme blobs, got {page:?}"
        );
    }

    // The run completed with the decoded item lengths, proving materialized
    // map items and result assembly worked over the foreign pages.
    let history = stream_history(&backend, run_id).await;
    let HistoryEventData::WorkflowCompleted { result } = &history.last().unwrap().data else {
        panic!(
            "expected WorkflowCompleted, got {:?}",
            history.last().unwrap().data
        );
    };
    assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 5 * 600);
}

#[test]
fn inline_manifest_root_with_foreign_scheme_pages_completes_over_memory_provider() {
    block_on(async {
        inline_root_foreign_page_activity_map_completes(MemoryBackend::new(), "memory").await;
    });
}

#[test]
fn inline_manifest_root_with_foreign_scheme_pages_completes_over_sqlite_provider() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("foreign-page.sqlite3")).unwrap();
        inline_root_foreign_page_activity_map_completes(backend, "sqlite").await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn inline_manifest_root_with_foreign_scheme_pages_completes_over_postgres_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres foreign-page conformance",
        "foreign_page",
        |backend| async move {
            inline_root_foreign_page_activity_map_completes(backend, "postgres").await;
        },
    ));
}

#[test]
fn custom_scheme_blob_store_works_over_memory_provider() {
    block_on(async {
        custom_scheme_blob_store_round_trips_and_survives_gc(MemoryBackend::new(), "memory").await;
    });
}

#[test]
fn custom_scheme_blob_store_works_over_sqlite_provider() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("custom-scheme.sqlite3");
        let blob_store = TestCustomBlobStore::default();
        let backend = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let run_id = payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/sqlite-custom-scheme",
            "sqlite-custom-scheme-workflows",
            "sqlite-custom-scheme-activities",
        )
        .await;
        drop(backend);

        // Reopen: the persisted custom-scheme refs must hydrate through the
        // custom store and survive GC with a zero grace period.
        let reopened = PayloadBackend::with_payload_storage(
            SqliteBackend::open(&path).unwrap(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let outcome = reopened
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.failed_blobs, 0);
        let history = reopened
            .stream_history(durust::StreamHistoryRequest {
                run_id,
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(1),
                max_events: 100,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap()
            .events;
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected hydrated workflow start after reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(input).unwrap(),
            large_payload("workflow-input")
        );
    });
}

#[cfg(feature = "postgres")]
#[test]
fn custom_scheme_blob_store_works_over_postgres_provider_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres custom-scheme conformance",
        "custom_scheme",
        |backend| async move {
            custom_scheme_blob_store_round_trips_and_survives_gc(backend, "postgres").await;
        },
    ));
}

// Bug A pin, fresh-upload window: every write path uploads its blob before the
// commit that makes it reachable, so GC must never delete an
// unreachable-but-young blob. `min_age: 0` reproduces the pre-fix behavior;
// the default grace period keeps the in-flight upload alive until its commit
// lands, after which reachability protects it unconditionally.
#[test]
fn payload_backend_gc_grace_period_protects_in_flight_uploads() {
    block_on(async {
        let blob_store = durust::MemoryBlobStore::new();
        let backend = PayloadBackend::with_payload_storage(
            MemoryBackend::new(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let value = large_payload("gc-race-input");
        let payload = durust::encode_payload(&value).unwrap();
        let durust::PayloadRef::Inline { bytes, .. } = payload.clone() else {
            panic!("freshly encoded payload should be inline");
        };
        let digest = durust::digest_bytes(&bytes);

        // The in-flight window: uploaded, not yet referenced by any commit.
        blob_store
            .put_payload_blob(digest.clone(), bytes.clone())
            .await
            .unwrap();
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 0);
        assert_eq!(outcome.scanned_blobs, 1);
        blob_store
            .get_payload_blob(digest.clone())
            .await
            .expect("grace period must protect the in-flight upload");

        // Zero grace period restores the pre-fix delete-anything-unreachable
        // behavior: the same window now loses the blob.
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 1);
        blob_store
            .get_payload_blob(digest.clone())
            .await
            .expect_err("zero grace period deletes the uncommitted upload");

        // Upload again and land the commit; reachability now protects the
        // blob at any grace period.
        blob_store
            .put_payload_blob(digest.clone(), bytes)
            .await
            .unwrap();
        backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("wf/gc-race-committed"),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new("gc-race-workflows"),
                input: payload,
            })
            .await
            .unwrap();
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 0);
        blob_store
            .get_payload_blob(digest)
            .await
            .expect("committed blob must always survive GC");
    });
}

// Drives one projection replacement against the single-event conformance
// workflow: wakes the run with a signal when `signal_seq > 0`, claims it, and
// commits a projection holding `value` plus a re-armed signal wait so the next
// replacement can wake the run again. Replacing a projection is the simplest
// way to turn an offloaded blob into garbage.
async fn commit_projection_replacement<B>(
    backend: &B,
    workflow_id: &str,
    queue: &str,
    signal_seq: u64,
    value: &str,
) where
    B: DurableBackend,
{
    // Idempotent re-start resolves the run id; the tiny input stays inline so
    // it cannot perturb blob-store contents.
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new(queue),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let mut consume_signals = Vec::new();
    if signal_seq > 0 {
        backend
            .signal_workflow(durust::SignalWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(workflow_id),
                signal_id: durust::SignalId::new(format!("{workflow_id}/replace/{signal_seq}")),
                signal_name: durust::SignalName::new("replace"),
                payload: durust::encode_payload(&signal_seq).unwrap(),
            })
            .await
            .unwrap();
        let inbox = backend
            .read_signal_inbox(durust::ReadSignalInboxRequest {
                run_id: run_id.clone(),
                signal_name: durust::SignalName::new("replace"),
            })
            .await
            .unwrap()
            .expect("replacement signal");
        consume_signals.push(inbox.signal_id);
    }
    let claimed = claim_conformance_workflow(
        backend,
        &format!("{workflow_id}-projection-{signal_seq}"),
        queue,
    )
    .await;
    let command_id = durust::command_id(&run_id, 1);
    let wait_id = durust::WaitId::new(format!("{}:{}:signal", command_id.run_id, command_id.seq.0));
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                consume_signals,
                upsert_waits: vec![durust::WaitRecord {
                    wait_id,
                    run_id,
                    command_id,
                    kind: durust::WaitKind::Signal,
                    key: "replace".to_owned(),
                    ready_at: None,
                }],
                ..projection_only_commit(durust::encode_payload(&value.to_owned()).unwrap())
            },
        )
        .await
        .unwrap();
}

// Bug A pin, memory provider: the provider-internal store follows the virtual
// clock, so deterministic tests control blob age with `advance_time`. Old
// unreachable blobs are collected, young ones survive the grace period, and
// `min_age: 0` collects unconditionally.
#[test]
fn memory_gc_grace_period_follows_virtual_clock() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        let workflow_id = "wf/memory-gc-grace";
        let queue = "memory-gc-grace-workflows";
        backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(workflow_id),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new(queue),
                input: durust::encode_payload(&large_payload("memory-gc-input")).unwrap(),
            })
            .await
            .unwrap();
        commit_projection_replacement(&backend, workflow_id, queue, 0, "projection-old").await;
        commit_projection_replacement(&backend, workflow_id, queue, 1, "projection-mid").await;

        // The replaced projection blob is unreachable but young: the default
        // grace period retains it because it could belong to an in-flight
        // commit.
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 0);

        // Two virtual hours later the same blob is old garbage.
        backend.advance_time(Duration::from_secs(2 * 60 * 60));
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.deleted_blobs, 1,
            "projection-old blob aged past the grace period"
        );

        // A replacement after the clock advance leaves young garbage again:
        // default grace retains it, zero grace collects it.
        commit_projection_replacement(&backend, workflow_id, queue, 2, "projection-new").await;
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.deleted_blobs, 1,
            "projection-mid blob aged past the grace period while projection-new's predecessor stayed protected"
        );
        commit_projection_replacement(&backend, workflow_id, queue, 3, "projection-final").await;
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.deleted_blobs, 0,
            "projection-new blob is young garbage the grace period retains"
        );
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.deleted_blobs, 1,
            "zero grace period collects the young garbage"
        );

        // The live projection and workflow input always survive.
        let projection = backend
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected live projection");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            "projection-final"
        );
    });
}

// Bug A pin, SQLite local-directory store (close/reopen): directory blobs are
// written before their transaction commits, so GC ages them by file mtime.
// Content-addressed re-puts refresh the mtime so a blob a new in-flight commit
// deduplicated against regains its full grace period.
#[test]
fn sqlite_local_blob_gc_grace_period_and_dedup_mtime_refresh() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let object_dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gc-grace.sqlite3");
        let config = durust::PayloadStorageConfig::new()
            .inline_threshold_bytes(1)
            .blob_store(durust::BlobStoreConfig::LocalDirectory {
                root: object_dir.path().to_path_buf(),
                prefix: "payloads".to_owned(),
            });
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        let workflow_id = "wf/sqlite-gc-grace";
        let queue = "sqlite-gc-grace-workflows";
        backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(workflow_id),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new(queue),
                input: durust::encode_payload(&large_payload("sqlite-gc-input")).unwrap(),
            })
            .await
            .unwrap();
        commit_projection_replacement(&backend, workflow_id, queue, 0, "projection-old").await;
        commit_projection_replacement(&backend, workflow_id, queue, 1, "projection-live").await;

        let blob_dir = object_dir.path().join("payloads");
        let garbage_digest = durust::digest_bytes(
            durust::encode_payload(&"projection-old".to_owned())
                .unwrap()
                .inline_bytes()
                .unwrap(),
        );
        let garbage_path = blob_dir.join(&garbage_digest);
        assert!(garbage_path.exists());

        // Young unreachable garbage survives the default grace period.
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 0);
        assert!(garbage_path.exists());

        // Backdated past the grace period the same file is collected.
        let two_hours_ago = SystemTime::now() - Duration::from_secs(2 * 60 * 60);
        fs::File::options()
            .write(true)
            .open(&garbage_path)
            .unwrap()
            .set_modified(two_hours_ago)
            .unwrap();
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 1);
        assert_eq!(outcome.failed_blobs, 0);
        assert!(!garbage_path.exists());

        // Fresh-upload window: a file written by an in-flight commit (upload
        // happens before the transaction commits) survives the grace period
        // and dies only under min_age zero.
        let in_flight = durust::encode_payload(&large_payload("sqlite-in-flight")).unwrap();
        let in_flight_bytes = in_flight.inline_bytes().unwrap().to_vec();
        let in_flight_digest = durust::digest_bytes(&in_flight_bytes);
        let in_flight_path = blob_dir.join(&in_flight_digest);
        fs::write(&in_flight_path, &in_flight_bytes).unwrap();
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 0);
        assert!(in_flight_path.exists());
        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 1);
        assert!(!in_flight_path.exists());

        // Dedup window: a commit that reuses an existing old blob must refresh
        // its mtime, restarting the grace period for the reused content.
        let reused_value = "projection-reused";
        let reused_digest = durust::digest_bytes(
            durust::encode_payload(&reused_value.to_owned())
                .unwrap()
                .inline_bytes()
                .unwrap(),
        );
        let reused_path = blob_dir.join(&reused_digest);
        fs::write(
            &reused_path,
            durust::encode_payload(&reused_value.to_owned())
                .unwrap()
                .inline_bytes()
                .unwrap(),
        )
        .unwrap();
        fs::File::options()
            .write(true)
            .open(&reused_path)
            .unwrap()
            .set_modified(two_hours_ago)
            .unwrap();
        commit_projection_replacement(&backend, workflow_id, queue, 2, reused_value).await;
        let refreshed = fs::metadata(&reused_path).unwrap().modified().unwrap();
        assert!(
            refreshed.duration_since(two_hours_ago).unwrap_or_default()
                > Duration::from_secs(60 * 60),
            "content-addressed re-put must refresh the blob mtime"
        );
        drop(backend);

        // Close/reopen: the refreshed blob is now reachable (live projection)
        // and hydrates from disk.
        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        let outcome = reopened
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.failed_blobs, 0);
        assert!(reused_path.exists());
        let projection = reopened
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected reopened projection");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            reused_value
        );
    });
}

// A delete failure on one garbage blob must not abort the sweep: the failure
// is recorded in the outcome and the remaining garbage is still collected.
#[derive(Clone, Debug)]
struct PoisonedDeleteBlobStore {
    inner: durust::MemoryBlobStore,
    poisoned_digest: String,
}

impl durust::PayloadBlobStore for PoisonedDeleteBlobStore {
    fn put_payload_blob(
        &self,
        digest: String,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, durust::Result<String>> {
        self.inner.put_payload_blob(digest, bytes)
    }

    fn get_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<Vec<u8>>> {
        self.inner.get_payload_blob(digest)
    }

    fn payload_blob_exists(&self, digest: String) -> BoxFuture<'static, durust::Result<bool>> {
        self.inner.payload_blob_exists(digest)
    }

    fn list_payload_blobs(
        &self,
    ) -> BoxFuture<'static, durust::Result<BTreeMap<String, durust::TimestampMs>>> {
        self.inner.list_payload_blobs()
    }

    fn delete_payload_blob(&self, digest: String) -> BoxFuture<'static, durust::Result<()>> {
        if digest == self.poisoned_digest {
            return Box::pin(ready(Err(Error::Backend(
                "intentional blob delete failure".to_owned(),
            ))));
        }
        self.inner.delete_payload_blob(digest)
    }

    fn owns_payload_blob_uri(&self, uri: &str) -> bool {
        self.inner.owns_payload_blob_uri(uri)
    }
}

#[test]
fn payload_backend_gc_records_delete_failures_and_continues() {
    block_on(async {
        let poisoned_payload = durust::encode_payload(&large_payload("poisoned-garbage")).unwrap();
        let poisoned_bytes = poisoned_payload.inline_bytes().unwrap().to_vec();
        let poisoned_digest = durust::digest_bytes(&poisoned_bytes);
        let deletable_payload =
            durust::encode_payload(&large_payload("deletable-garbage")).unwrap();
        let deletable_bytes = deletable_payload.inline_bytes().unwrap().to_vec();
        let deletable_digest = durust::digest_bytes(&deletable_bytes);

        let blob_store = PoisonedDeleteBlobStore {
            inner: durust::MemoryBlobStore::new(),
            poisoned_digest: poisoned_digest.clone(),
        };
        let backend = PayloadBackend::with_payload_storage(
            MemoryBackend::new(),
            blob_store.clone(),
            durust::PayloadStorageConfig::new().inline_threshold_bytes(1),
        );
        blob_store
            .put_payload_blob(poisoned_digest.clone(), poisoned_bytes)
            .await
            .unwrap();
        blob_store
            .put_payload_blob(deletable_digest.clone(), deletable_bytes)
            .await
            .unwrap();

        // Dry run reports both as would-delete without attempting deletes.
        let dry_run = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: true,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(dry_run.deleted_blobs, 2);
        assert_eq!(dry_run.failed_blobs, 0);

        let outcome = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(outcome.deleted_blobs, 1);
        assert_eq!(outcome.failed_blobs, 1);
        blob_store
            .get_payload_blob(deletable_digest)
            .await
            .expect_err("healthy garbage must still be deleted");
        blob_store
            .get_payload_blob(poisoned_digest)
            .await
            .expect("failed delete leaves the blob for the next sweep");
    });
}

#[test]
fn sqlite_provider_offloads_large_payloads_to_local_blob_store_and_gc_collects_objects() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let object_dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload-local-objects.sqlite3");
        let config = durust::PayloadStorageConfig::new()
            .inline_threshold_bytes(1)
            .blob_store(durust::BlobStoreConfig::LocalDirectory {
                root: object_dir.path().to_path_buf(),
                prefix: "payloads".to_owned(),
            });
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        let run_id = payload_offload_public_api_round_trip(
            backend.clone(),
            "wf/sqlite-local-payload-offload",
            "sqlite-local-payload-workflows",
            "sqlite-local-payload-activities",
        )
        .await;
        let (gc_workflow_id, gc_projection) =
            payload_gc_removes_unreachable_projection_blob(backend.clone(), "sqlite-local").await;
        let object_count = local_blob_file_count(object_dir.path(), "payloads");
        assert!(object_count >= 4);
        assert_eq!(backend.payload_blob_count().unwrap(), object_count);

        fs::write(object_dir.path().join("payloads").join("orphan"), b"orphan").unwrap();
        let dry_run = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: true,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert!(dry_run.deleted_blobs >= 1);
        let collected = backend
            .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
                dry_run: false,
                min_age: Duration::ZERO,
            })
            .await
            .unwrap();
        assert_eq!(collected.deleted_blobs, dry_run.deleted_blobs);
        assert!(!object_dir.path().join("payloads").join("orphan").exists());
        drop(backend);

        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        let projection = reopened
            .query_projection(durust::QueryProjectionRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(gc_workflow_id),
            })
            .await
            .unwrap();
        let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
            panic!("expected retained projection after local object-store reopen");
        };
        assert_eq!(
            durust::decode_payload::<String>(&payload).unwrap(),
            gc_projection
        );
        let history = stream_history(&reopened, run_id).await;
        let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
            panic!("expected hydrated workflow start from local object store");
        };
        assert_eq!(
            durust::decode_payload::<String>(input).unwrap(),
            large_payload("workflow-input")
        );
    });
}

#[test]
fn sqlite_local_blob_store_upload_failure_does_not_commit_missing_payload_ref() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let root_file = dir.path().join("not-a-directory");
        fs::write(&root_file, b"not a directory").unwrap();
        let path = dir.path().join("payload-upload-failure.sqlite3");
        let config = durust::PayloadStorageConfig::new()
            .inline_threshold_bytes(1)
            .blob_store(durust::BlobStoreConfig::LocalDirectory {
                root: root_file,
                prefix: "payloads".to_owned(),
            });
        let backend = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        let err = backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new("wf/sqlite-local-upload-failure"),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new("workflows"),
                input: durust::encode_payload(&large_payload("workflow-input")).unwrap(),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Backend(message) if message.contains("failed to create local payload blob directory"))
        );
        let claim = backend
            .claim_workflow_task(
                WorkerId::new("sqlite-local-upload-failure-worker"),
                workflow_claim_opts("workflows"),
            )
            .await
            .unwrap();
        assert!(claim.is_none());
    });
}

#[test]
fn memory_provider_json_codec_round_trips_nested_activity_map_payloads() {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            durust::PayloadStorageConfig::new()
                .codec(durust::CodecId::Json)
                .inline_threshold_bytes(1),
        );
        payload_json_activity_map_round_trip(backend, "memory-json").await;
    });
}

#[test]
fn sqlite_provider_json_codec_round_trips_nested_activity_map_payloads_after_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payload-json-map.sqlite3");
        let config = durust::PayloadStorageConfig::new()
            .codec(durust::CodecId::Json)
            .inline_threshold_bytes(1);
        let backend = SqliteBackend::open_with_payload_storage(&path, config.clone()).unwrap();
        let run_id = payload_json_activity_map_round_trip(backend, "sqlite-json").await;

        let reopened = SqliteBackend::open_with_payload_storage(&path, config).unwrap();
        let history = stream_history(&reopened, run_id).await;
        let HistoryEventData::ActivityMapCompleted(completed) = &history[2].data else {
            panic!("expected persisted JSON activity map completion after reopen");
        };
        assert_eq!(completed.result_manifest.codec(), durust::CodecId::Json);
        let results = durust::decode_activity_map_result_refs(&completed.result_manifest).unwrap();
        assert_eq!(
            results
                .iter()
                .map(|payload| payload.codec())
                .collect::<Vec<_>>(),
            vec![durust::CodecId::Json, durust::CodecId::Json]
        );
    });
}

#[test]
fn registry_rejects_duplicate_handler_identities() {
    let mut registry = Registry::default();
    registry.register_workflow::<workflow>().unwrap();
    let err = registry.register_workflow::<workflow>().unwrap_err();
    assert!(matches!(err, Error::DuplicateWorkflow(_)));

    registry.register_activity::<echo>().unwrap();
    let err = registry.register_activity::<echo>().unwrap_err();
    assert!(matches!(err, Error::DuplicateActivity(_)));
}

#[test]
fn worker_builder_exposes_fallible_duplicate_registration() {
    let builder = Worker::builder(MemoryBackend::new())
        .try_register_workflow(workflow)
        .unwrap();
    let result = builder.try_register_workflow(workflow);
    assert!(matches!(result, Err(Error::DuplicateWorkflow(_))));

    let builder = Worker::builder(MemoryBackend::new())
        .try_register_activity(echo)
        .unwrap();
    let result = builder.try_register_activity(echo);
    assert!(matches!(result, Err(Error::DuplicateActivity(_))));
}

#[test]
fn registry_generates_manifest_metadata_from_handlers() {
    let mut registry = Registry::default();
    registry.register_workflow::<workflow>().unwrap();
    registry.register_activity::<echo>().unwrap();

    let manifest = registry.manifest();
    assert_eq!(manifest.workflows.len(), 1);
    assert_eq!(manifest.workflows[0].name, "conformance.workflow");
    assert_eq!(manifest.workflows[0].version, 1);
    assert!(
        manifest.workflows[0]
            .rust_path
            .ends_with("provider_conformance::workflow")
    );
    assert!(
        manifest.workflows[0]
            .input_type
            .ends_with("provider_conformance::Input")
    );
    assert!(
        manifest.workflows[0]
            .input_type_name_hash
            .starts_with("sha256:")
    );

    assert_eq!(manifest.activities.len(), 1);
    assert_eq!(manifest.activities[0].name, "conformance.echo");
    assert!(
        manifest.activities[0]
            .input_type
            .ends_with("provider_conformance::Input")
    );
    assert!(
        manifest.activities[0]
            .output_type_name_hash
            .starts_with("sha256:")
    );
}

#[test]
fn macros_export_manifest_metadata_for_linked_handlers() {
    let manifest = durust::exported_manifest();

    let workflow_export = manifest
        .workflows
        .iter()
        .find(|entry| entry.name == "conformance.workflow" && entry.version == 1)
        .expect("workflow export");
    assert!(
        workflow_export
            .rust_path
            .ends_with("provider_conformance::workflow")
    );
    assert_eq!(
        workflow_export.input_type,
        <workflow as durust::Workflow>::input_type_name()
    );
    assert_eq!(
        workflow_export.input_type_name_hash,
        durust::type_fingerprint::<<workflow as durust::Workflow>::Input>()
    );

    let activity = manifest
        .activities
        .iter()
        .find(|activity| activity.name == "conformance.echo")
        .expect("activity export");
    assert!(activity.rust_path.ends_with("provider_conformance::echo"));
    assert_eq!(
        activity.output_type,
        <echo as durust::Activity>::output_type_name()
    );
    assert_eq!(
        activity.output_type_name_hash,
        durust::type_fingerprint::<<echo as durust::Activity>::Output>()
    );
}

#[test]
fn default_durable_names_include_package_module_and_function() {
    assert_eq!(
        <default_name_handlers::default_activity as durust::Activity>::NAME,
        "durust::provider_conformance::default_name_handlers::default_activity"
    );
    assert_eq!(
        <default_name_handlers::default_workflow as durust::Workflow>::NAME,
        "durust::provider_conformance::default_name_handlers::default_workflow"
    );
}

/// How many scenarios [`provider_conformance`] runs against every provider.
///
/// The aggregator is a straight-line list of calls and nothing downstream
/// counts them, so deleting one used to be invisible: the three provider tests
/// keep passing, the test count is unchanged (the scenarios are plain `async
/// fn`s, not `#[test]`s), and the only trace is a `dead_code` warning that
/// nothing enforces — CI runs neither `-D warnings` nor clippy. That matters
/// because `PARITY.md` cites individual entries in this list by name, row 21's
/// Rust column being
/// `an_empty_map_scheduled_by_a_closing_commit_is_still_accepted`; a deleted
/// call would leave the row pointing at coverage that no longer runs.
///
/// [`run_conformance_scenarios`] counts what it expands and this number is the
/// floor. Removing a scenario is meant to move it in the same commit.
const CONFORMANCE_SCENARIOS: usize = 51;

/// Runs each named scenario against a clone of `backend`, in order, and
/// returns how many it ran.
///
/// The scenarios stay listed as bare `fn` idents and no name is assembled from
/// fragments: `PARITY.md` names tests and scenarios in this file verbatim, so
/// every identifier a row cites has to remain greppable as a literal here.
macro_rules! run_conformance_scenarios {
    ($backend:expr, $($scenario:ident),+ $(,)?) => {{
        let mut ran = 0usize;
        $(
            $scenario($backend.clone()).await;
            ran += 1;
        )+
        ran
    }};
}

async fn provider_conformance<B>(backend: B)
where
    B: DurableBackend,
{
    let ran = run_conformance_scenarios!(
        backend,
        start_workflow_is_idempotent,
        workflow_claim_filters_by_queue_and_registered_type,
        stream_history_honors_bounds,
        released_workflow_task_is_claimable_again,
        query_projection_updates_atomically_and_reads_payload_refs,
        missing_provider_blob_ref_is_rejected,
        provider_blob_ref_metadata_mismatch_is_rejected,
        workflow_change_version_index_tracks_markers_and_open_status,
        continue_as_new_closes_current_run_and_starts_claimable_next_run,
        signal_inbox_is_idempotent_ordered_and_consumed_by_commit,
        signal_between_claim_and_commit_wakes_workflow,
        signal_during_claim_window_survives_empty_commit,
        signal_between_claim_and_commit_wakes_workflows_in_batch_commit,
        terminal_run_fences_stale_mutating_commits_identically,
        late_activity_completion_after_cancel_is_idempotent_across_retries,
        terminal_cleanup_answers_late_calls_and_keeps_undelivered_signals,
        consumed_signal_dedup_survives_continue_as_new,
        timer_waits_fire_only_when_due_and_make_workflow_claimable,
        activity_retry_reschedules_until_max_attempts,
        non_retryable_activity_failure_skips_retry_and_wakes_workflow,
        activity_timeout_retries_until_max_attempts_then_wakes_workflow,
        activity_heartbeat_extends_deadline_and_rejects_stale_claim,
        activity_heartbeat_timeout_retries_until_max_attempts_then_wakes_workflow,
        cancel_commands_clear_activity_tasks,
        child_start_dispatch_is_idempotent_and_wakes_parent,
        child_completion_routes_to_parent,
        child_start_conflict_records_failure,
        parent_close_policy_cancel_cancels_child,
        parent_close_policy_abandon_leaves_child_running,
        activity_map_materializes_bounded_items_and_writes_result_manifest,
        activity_map_failure_suppresses_remaining_items_and_wakes_workflow,
        child_workflow_map_materializes_bounded_children_and_writes_result_manifest,
        child_workflow_map_fail_fast_cancels_in_flight_children,
        child_workflow_map_collect_all_records_ordered_outcomes,
        child_workflow_map_command_cancellation_cancels_started_children,
        abandoned_child_of_closed_map_parent_can_still_terminate,
        child_workflow_map_zero_max_in_flight_is_rejected_at_descriptor_creation,
        a_commit_scheduling_one_map_twice_is_rejected,
        empty_input_manifest_completes_at_descriptor_creation,
        an_empty_map_scheduled_by_a_closing_commit_is_still_accepted,
        one_commit_completing_two_empty_maps_keeps_its_event_ids_contiguous,
        workflow_cancel_cleans_waits_activities_and_activity_maps,
        stale_workflow_task_commit_conflicts,
        batch_workflow_task_claim_and_commit_results_are_ordered,
        batch_activity_completion_reports_ordered_duplicate_and_stale_results,
        activity_claim_filters_and_stale_completion_is_rejected,
        unexpired_workflow_claim_lease_is_not_reclaimable,
        // Run last: their timeout scans use far-future `now`s that must not
        // disturb other cases' pending activities.
        timeoutless_activity_lease_expiry_reclaims_and_fences_stale_holder,
        timeoutless_activity_reclaims_one_lease_after_heartbeats_stop,
        timeoutless_activity_batch_claim_uses_lease_as_implicit_heartbeat,
        explicit_heartbeat_timeout_takes_precedence_over_claim_lease,
    );
    assert_eq!(
        ran, CONFORMANCE_SCENARIOS,
        "this provider ran {ran} conformance scenarios, not \
         {CONFORMANCE_SCENARIOS}; a scenario dropped from the list stops \
         running against every provider without failing a test or changing the \
         test count, and `PARITY.md` cites entries in this list by name, so \
         restore it or move CONFORMANCE_SCENARIOS deliberately"
    );
}

async fn start_large_payload_workflow<B>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
) -> durust::RunId
where
    B: DurableBackend,
{
    backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new(workflow_queue),
            input: durust::encode_payload(&large_payload("workflow-input")).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone()
}

async fn assert_replay_stream_payload_hydrates_explicitly<B>(backend: B, run_id: durust::RunId)
where
    B: DurableBackend,
{
    let public_history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &public_history[0].data else {
        panic!("expected public workflow start");
    };
    assert!(matches!(input, durust::PayloadRef::Inline { .. }));
    assert_eq!(
        durust::decode_payload::<String>(input).unwrap(),
        large_payload("workflow-input")
    );

    let replay_history = backend
        .stream_history_for_replay(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &replay_history[0].data else {
        panic!("expected replay workflow start");
    };
    assert!(matches!(input, durust::PayloadRef::Blob { .. }));
    let hydrated = backend.hydrate_payload(input.clone()).await.unwrap();
    assert!(matches!(hydrated, durust::PayloadRef::Inline { .. }));
    assert_eq!(
        durust::decode_payload::<String>(&hydrated).unwrap(),
        large_payload("workflow-input")
    );
}

async fn assert_side_effect_marker_stays_inline_for_replay_stream<B>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
) where
    B: DurableBackend,
{
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.side-effect-marker", 1),
            task_queue: TaskQueue::new(workflow_queue),
            input: durust::encode_payload(&()).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(workflow_queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.side-effect-marker", 1)],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("side-effect-marker-commit"), claim_opts)
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let value = durust::encode_payload(&"side-effect-value-that-exceeds-threshold").unwrap();
    assert!(value.encoded_len() > 1);

    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::SideEffectMarker(durust::SideEffectMarker {
                        command_id,
                        key: "make-id".to_owned(),
                        value,
                    }),
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        durust::CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    assert_side_effect_marker_stays_inline_in_existing_history(backend, workflow_id).await;
}

async fn assert_side_effect_marker_stays_inline_in_existing_history<B>(
    backend: B,
    workflow_id: &str,
) where
    B: DurableBackend,
{
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.side-effect-marker", 1),
            task_queue: TaskQueue::new("unused"),
            input: durust::encode_payload(&()).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let replay_history = backend
        .stream_history_for_replay(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(2),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::SideEffectMarker(marker) = &replay_history[1].data else {
        panic!("expected side effect marker in replay history");
    };
    assert_eq!(marker.key, "make-id");
    assert!(matches!(marker.value, durust::PayloadRef::Inline { .. }));
    assert_eq!(
        durust::decode_payload::<String>(&marker.value).unwrap(),
        "side-effect-value-that-exceeds-threshold"
    );
}

async fn payload_offload_public_api_round_trip<B>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    activity_queue: &str,
) -> durust::RunId
where
    B: DurableBackend,
{
    let workflow_input = large_payload("workflow-input");
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new(workflow_queue),
            input: durust::encode_payload(&workflow_input).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();

    let start_history = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &start_history[0].data else {
        panic!("expected workflow start");
    };
    assert_eq!(
        durust::decode_payload::<String>(input).unwrap(),
        workflow_input
    );
    assert!(matches!(input, durust::PayloadRef::Inline { .. }));

    let claim_opts = workflow_claim_opts(workflow_queue);
    let claimed = backend
        .claim_workflow_task(WorkerId::new("payload-offload-workflow"), claim_opts)
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let activity_input = large_payload("activity-input");
    let activity_payload = durust::encode_payload(&activity_input).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new(activity_queue),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: activity_payload.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&activity_payload),
            "sha256:payload-offload-options".to_owned(),
        ),
    };
    let projection = large_payload("projection");
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityScheduled(scheduled.clone()),
                )],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                query_projection: Some(durust::encode_payload(&projection).unwrap()),
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let query = backend
        .query_projection(durust::QueryProjectionRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
        })
        .await
        .unwrap();
    let durust::QueryProjectionOutcome::Found { payload, .. } = query else {
        panic!("expected query projection");
    };
    assert_eq!(
        durust::decode_payload::<String>(&payload).unwrap(),
        projection
    );
    assert!(matches!(payload, durust::PayloadRef::Inline { .. }));

    let activity = backend
        .claim_activity_task(
            WorkerId::new("payload-offload-activity"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new(activity_queue),
                registered_activity_names: vec![ActivityName::new("conformance.echo")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("activity task");
    assert_eq!(
        durust::decode_payload::<String>(&activity.task.input).unwrap(),
        activity_input
    );
    assert!(matches!(
        activity.task.input,
        durust::PayloadRef::Inline { .. }
    ));

    let activity_result = large_payload("activity-result");
    let completed = backend
        .complete_activity(CompleteActivityRequest {
            claim: activity.claim,
            result: durust::encode_payload(&activity_result).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        completed,
        durust::CompleteActivityOutcome::Completed {
            event_id: EventId(3)
        }
    );

    let signal_payload = large_payload("signal");
    let accepted = backend
        .signal_workflow(durust::SignalWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            signal_id: durust::SignalId::new(format!("{workflow_id}/signal/1")),
            signal_name: durust::SignalName::new("payload"),
            payload: durust::encode_payload(&signal_payload).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(accepted, durust::SignalWorkflowOutcome::Accepted);
    let signal = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id: run_id.clone(),
            signal_name: durust::SignalName::new("payload"),
        })
        .await
        .unwrap()
        .expect("signal payload");
    assert_eq!(
        durust::decode_payload::<String>(&signal.payload).unwrap(),
        signal_payload
    );
    assert!(matches!(signal.payload, durust::PayloadRef::Inline { .. }));
    let signal_batch = backend
        .read_signal_inboxes(durust::ReadSignalInboxesRequest {
            requests: vec![
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("missing"),
                },
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("payload"),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(signal_batch.len(), 2);
    assert!(signal_batch[0].is_none());
    let batched_signal = signal_batch[1].as_ref().expect("batched signal payload");
    assert_eq!(
        durust::decode_payload::<String>(&batched_signal.payload).unwrap(),
        signal_payload
    );
    assert!(matches!(
        batched_signal.payload,
        durust::PayloadRef::Inline { .. }
    ));

    let history = stream_history(&backend, run_id.clone()).await;
    let HistoryEventData::ActivityScheduled(scheduled) = &history[1].data else {
        panic!("expected activity scheduled event");
    };
    assert_eq!(
        durust::decode_payload::<String>(&scheduled.input).unwrap(),
        activity_input
    );
    let HistoryEventData::ActivityCompleted(completed) = &history[2].data else {
        panic!("expected activity completed event");
    };
    assert_eq!(
        durust::decode_payload::<String>(&completed.result).unwrap(),
        activity_result
    );

    run_id
}

fn large_payload(label: &str) -> String {
    format!("{label}:{}", "x".repeat(64))
}

fn local_blob_file_count(root: &std::path::Path, prefix: &str) -> usize {
    let dir = root.join(prefix);
    fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("failed to list local blob dir `{}`: {err}", dir.display()))
        .filter_map(|entry| {
            let entry = entry.unwrap();
            let is_file = entry.file_type().unwrap().is_file();
            let name = entry.file_name().to_string_lossy().into_owned();
            (is_file && !name.contains(".tmp-")).then_some(())
        })
        .count()
}

async fn payload_gc_removes_unreachable_projection_blob<B>(
    backend: B,
    prefix: &str,
) -> (String, String)
where
    B: DurableBackend,
{
    let workflow_id = format!("wf/{prefix}-payload-gc");
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new("payload-gc-workflows"),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let claim_opts = workflow_claim_opts("payload-gc-workflows");
    let first_claim = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-gc-first")),
            claim_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first projection workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let wait_id = durust::WaitId::new(format!("{}:{}:signal", command_id.run_id, command_id.seq.0));
    let first_projection = large_payload(&format!("{prefix}-projection-old"));
    backend
        .commit_workflow_task(
            first_claim.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                upsert_waits: vec![durust::WaitRecord {
                    wait_id,
                    run_id: run_id.clone(),
                    command_id,
                    kind: durust::WaitKind::Signal,
                    key: "replace".to_owned(),
                    ready_at: None,
                }],
                query_projection: Some(durust::encode_payload(&first_projection).unwrap()),
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    backend
        .signal_workflow(durust::SignalWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
            signal_id: durust::SignalId::new(format!("{workflow_id}/replace")),
            signal_name: durust::SignalName::new("replace"),
            payload: durust::encode_payload(&large_payload(&format!("{prefix}-wake"))).unwrap(),
        })
        .await
        .unwrap();
    let inbox = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id: run_id.clone(),
            signal_name: durust::SignalName::new("replace"),
        })
        .await
        .unwrap()
        .expect("replacement signal");
    let second_claim = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-gc-second")),
            claim_opts,
        )
        .await
        .unwrap()
        .expect("second projection workflow task");
    let second_projection = large_payload(&format!("{prefix}-projection-new"));
    backend
        .commit_workflow_task(
            second_claim.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                consume_signals: vec![inbox.signal_id],
                query_projection: Some(durust::encode_payload(&second_projection).unwrap()),
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    let dry_run = backend
        .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
            dry_run: true,
            min_age: Duration::ZERO,
        })
        .await
        .unwrap();
    assert!(
        dry_run.scanned_blobs > dry_run.retained_blobs,
        "expected garbage: scanned={}, retained={}, deleted={}",
        dry_run.scanned_blobs,
        dry_run.retained_blobs,
        dry_run.deleted_blobs
    );
    assert!(
        dry_run.deleted_blobs > 0,
        "expected deletions: scanned={}, retained={}, deleted={}",
        dry_run.scanned_blobs,
        dry_run.retained_blobs,
        dry_run.deleted_blobs
    );

    let collected = backend
        .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
            dry_run: false,
            min_age: Duration::ZERO,
        })
        .await
        .unwrap();
    assert_eq!(collected.deleted_blobs, dry_run.deleted_blobs);

    let after = backend
        .gc_payload_blobs(durust::PayloadGarbageCollectionRequest {
            dry_run: true,
            min_age: Duration::ZERO,
        })
        .await
        .unwrap();
    assert_eq!(after.deleted_blobs, 0);

    let projection = backend
        .query_projection(durust::QueryProjectionRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
        })
        .await
        .unwrap();
    let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
        panic!("expected retained projection");
    };
    assert_eq!(
        durust::decode_payload::<String>(&payload).unwrap(),
        second_projection
    );
    (workflow_id, second_projection)
}

async fn payload_offload_child_workflow_round_trip<B>(backend: B, prefix: &str)
where
    B: DurableBackend,
{
    let parent_workflow_id = format!("wf/{prefix}-payload-child-parent");
    let child_workflow_id = format!("wf/{prefix}-payload-child-child");
    let parent_run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&parent_workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new("payload-child-parent-workflows"),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let parent = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-parent")),
            workflow_claim_opts("payload-child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent workflow task");
    let command_id = durust::command_id(&parent_run_id, 1);
    let child_input = large_payload("child-input");
    let input = durust::encode_payload(&child_input).unwrap();
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let workflow_id = durust::WorkflowId::new(&child_workflow_id);
    let task_queue = TaskQueue::new("payload-child-workflows");
    let requested = durust::ChildWorkflowStartRequested {
        command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        workflow_id: workflow_id.clone(),
        task_queue: task_queue.clone(),
        input: input.clone(),
        parent_close_policy: durust::ParentClosePolicy::Cancel,
        fingerprint: durust::child_workflow_fingerprint(
            workflow_type,
            workflow_id,
            durust::payload_digest(&input),
            task_queue,
            durust::ParentClosePolicy::Cancel,
        ),
    };
    backend
        .commit_workflow_task(
            parent.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ChildWorkflowStartRequested(requested.clone()),
                )],
                start_child_workflows: vec![durust::ChildStartOutboxMessage::from_requested(
                    &requested,
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();

    let child = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child")),
            workflow_claim_opts("payload-child-workflows"),
        )
        .await
        .unwrap()
        .expect("child workflow task");
    let child_history = stream_history(&backend, child.run_id.clone()).await;
    let HistoryEventData::WorkflowStarted { input, .. } = &child_history[0].data else {
        panic!("expected child start");
    };
    assert_eq!(
        durust::decode_payload::<String>(input).unwrap(),
        child_input
    );

    let child_result = large_payload("child-result");
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCompleted {
                        result: durust::encode_payload(&child_result).unwrap(),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    let parent_ready = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-parent-ready")),
            workflow_claim_opts("payload-child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent completion wake");
    assert_eq!(
        parent_ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowCompleted
    );
    let parent_history = stream_history(&backend, parent_run_id).await;
    let requested = parent_history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowStartRequested(requested) => Some(requested),
            _ => None,
        })
        .expect("child start request");
    assert_eq!(
        durust::decode_payload::<String>(&requested.input).unwrap(),
        large_payload("child-input")
    );
    let completed = parent_history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowCompleted(completed) => Some(completed),
            _ => None,
        })
        .expect("child completion");
    assert_eq!(
        durust::decode_payload::<String>(&completed.result).unwrap(),
        child_result
    );
}

async fn payload_offload_child_workflow_map_round_trip<B>(backend: B, prefix: &str)
where
    B: DurableBackend,
{
    let workflow_id = format!("wf/{prefix}-payload-child-map");
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new("payload-child-map-parent-workflows"),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-map-scheduler")),
            workflow_claim_opts("payload-child-map-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        [
            "child-map-input-0",
            "child-map-input-1",
            "child-map-input-2",
        ]
        .into_iter()
        .map(|label| durust::encode_payload(&large_payload(label)).unwrap())
        .collect(),
        1,
    )
    .unwrap();
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let task_queue = TaskQueue::new("payload-child-map-workflows");
    let workflow_id_prefix = format!("wf/{prefix}-payload-child-map/item");
    let result_manifest_name = "payload-child-map-results".to_owned();
    let map_task = ChildWorkflowMapTask {
        map_command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        input_manifest: input_manifest.clone(),
        result_manifest_name: result_manifest_name.clone(),
        workflow_id_prefix: workflow_id_prefix.clone(),
        max_in_flight: 2,
        parent_close_policy: durust::ParentClosePolicy::Cancel,
        failure_mode: durust::ChildWorkflowMapFailureMode::FailFast,
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ChildWorkflowMapScheduled(
                        durust::ChildWorkflowMapScheduled {
                            command_id: command_id.clone(),
                            workflow_type: workflow_type.clone(),
                            task_queue: task_queue.clone(),
                            input_manifest: input_manifest.clone(),
                            result_manifest_name: result_manifest_name.clone(),
                            workflow_id_prefix: workflow_id_prefix.clone(),
                            max_in_flight: 2,
                            parent_close_policy: durust::ParentClosePolicy::Cancel,
                            failure_mode: durust::ChildWorkflowMapFailureMode::FailFast,
                            fingerprint: durust::child_workflow_map_fingerprint(
                                workflow_type,
                                durust::payload_digest(&input_manifest),
                                result_manifest_name,
                                workflow_id_prefix,
                                2,
                                task_queue,
                                durust::ParentClosePolicy::Cancel,
                                durust::ChildWorkflowMapFailureMode::FailFast,
                            ),
                        },
                    ),
                )],
                schedule_child_workflow_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    dispatch_child_map_starts(&backend).await;
    let child_opts = workflow_claim_opts("payload-child-map-workflows");
    let first = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-map-0")),
            child_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first child map workflow");
    let second = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-map-1")),
            child_opts.clone(),
        )
        .await
        .unwrap()
        .expect("second child map workflow");
    assert_child_map_started_with_input(&backend, &first, "child-map-input-0").await;
    assert_child_map_started_with_input(&backend, &second, "child-map-input-1").await;

    complete_child_run_string(&backend, first, &large_payload("child-map-result-0")).await;
    dispatch_child_map_starts(&backend).await;
    let third = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-child-map-2")),
            child_opts,
        )
        .await
        .unwrap()
        .expect("third child map workflow");
    assert_child_map_started_with_input(&backend, &third, "child-map-input-2").await;
    complete_child_run_string(&backend, third, &large_payload("child-map-result-2")).await;
    complete_child_run_string(&backend, second, &large_payload("child-map-result-1")).await;

    let history = stream_history(&backend, run_id).await;
    let HistoryEventData::ChildWorkflowMapScheduled(scheduled) = &history[1].data else {
        panic!("expected child workflow map scheduled event");
    };
    let manifest: ActivityMapInputManifest =
        durust::decode_payload(&scheduled.input_manifest).unwrap();
    assert_eq!(manifest.item_count, 3);
    for (ordinal, page) in manifest.pages.iter().enumerate() {
        let page: durust::ActivityMapInputPage = durust::decode_payload(page).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            durust::decode_payload::<String>(&page.items[0]).unwrap(),
            large_payload(&format!("child-map-input-{ordinal}"))
        );
    }

    let HistoryEventData::ChildWorkflowMapCompleted(completed) = &history[2].data else {
        panic!("expected child workflow map completed event");
    };
    let results =
        durust::decode_child_workflow_map_success_refs(&completed.result_manifest).unwrap();
    let values = results
        .iter()
        .map(|payload| durust::decode_payload::<String>(payload).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            large_payload("child-map-result-0"),
            large_payload("child-map-result-1"),
            large_payload("child-map-result-2")
        ]
    );
}

async fn assert_child_map_started_with_input<B>(
    backend: &B,
    child: &durust::ClaimedWorkflowTask,
    expected_label: &str,
) where
    B: DurableBackend,
{
    let history = stream_history(backend, child.run_id.clone()).await;
    let HistoryEventData::WorkflowStarted { input, .. } = &history[0].data else {
        panic!("expected child workflow start");
    };
    assert_eq!(
        durust::decode_payload::<String>(input).unwrap(),
        large_payload(expected_label)
    );
}

async fn complete_child_run_string<B>(backend: &B, child: durust::ClaimedWorkflowTask, value: &str)
where
    B: DurableBackend,
{
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: child.replay_target_event_id,
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCompleted {
                        result: durust::encode_payload(&value.to_owned()).unwrap(),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
}

async fn payload_offload_activity_map_round_trip<B>(backend: B, prefix: &str)
where
    B: DurableBackend,
{
    let workflow_id = format!("wf/{prefix}-payload-map");
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new("payload-map-workflows"),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-map-scheduler")),
            workflow_claim_opts("payload-map-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        ["map-input-0", "map-input-1", "map-input-2"]
            .into_iter()
            .map(|label| durust::encode_payload(&large_payload(label)).unwrap())
            .collect(),
        1,
    )
    .unwrap();
    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new("payload-map-activities");
    let retry_policy = durust::RetryPolicy::none();
    let map_task = ActivityMapTask {
        map_command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "payload-results".to_owned(),
        max_in_flight: 2,
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityMapScheduled(durust::ActivityMapScheduled {
                        command_id: command_id.clone(),
                        activity_name,
                        task_queue,
                        retry_policy,
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                        input_manifest: input_manifest.clone(),
                        result_manifest_name: "payload-results".to_owned(),
                        max_in_flight: 2,
                        fingerprint: durust::activity_map_fingerprint(
                            ActivityName::new("conformance.echo"),
                            durust::payload_digest(&input_manifest),
                            "payload-results".to_owned(),
                            2,
                            "sha256:payload-map-options".to_owned(),
                        ),
                    }),
                )],
                schedule_activity_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("payload-map-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(
            WorkerId::new(format!("{prefix}-payload-map-0")),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first map task");
    let second = backend
        .claim_activity_task(
            WorkerId::new(format!("{prefix}-payload-map-1")),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("second map task");
    assert_string_map_item(&first.task, 0, "map-input-0");
    assert_string_map_item(&second.task, 1, "map-input-1");

    backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim,
            result: durust::encode_payload(&large_payload("map-result-0")).unwrap(),
        })
        .await
        .unwrap();
    let third = backend
        .claim_activity_task(
            WorkerId::new(format!("{prefix}-payload-map-2")),
            activity_opts,
        )
        .await
        .unwrap()
        .expect("third map task");
    assert_string_map_item(&third.task, 2, "map-input-2");
    backend
        .complete_activity(CompleteActivityRequest {
            claim: third.claim,
            result: durust::encode_payload(&large_payload("map-result-2")).unwrap(),
        })
        .await
        .unwrap();
    backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&large_payload("map-result-1")).unwrap(),
        })
        .await
        .unwrap();

    let history = stream_history(&backend, run_id).await;
    let HistoryEventData::ActivityMapScheduled(scheduled) = &history[1].data else {
        panic!("expected activity map scheduled event");
    };
    let manifest: ActivityMapInputManifest =
        durust::decode_payload(&scheduled.input_manifest).unwrap();
    assert_eq!(manifest.item_count, 3);
    for (ordinal, page) in manifest.pages.iter().enumerate() {
        let page: durust::ActivityMapInputPage = durust::decode_payload(page).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            durust::decode_payload::<String>(&page.items[0]).unwrap(),
            large_payload(&format!("map-input-{ordinal}"))
        );
    }

    let HistoryEventData::ActivityMapCompleted(completed) = &history[2].data else {
        panic!("expected activity map completed event");
    };
    let results = durust::decode_activity_map_result_refs(&completed.result_manifest).unwrap();
    let values = results
        .iter()
        .map(|payload| durust::decode_payload::<String>(payload).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            large_payload("map-result-0"),
            large_payload("map-result-1"),
            large_payload("map-result-2")
        ]
    );
}

async fn payload_json_activity_map_round_trip<B>(backend: B, prefix: &str) -> durust::RunId
where
    B: DurableBackend,
{
    assert_eq!(
        backend.payload_storage_config().codec,
        durust::CodecId::Json
    );
    let workflow_id = format!("wf/{prefix}-payload-map");
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(&workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new("payload-json-map-workflows"),
            input: json_payload(&0_u64),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let start_history = stream_history(&backend, run_id.clone()).await;
    let HistoryEventData::WorkflowStarted { input, .. } = &start_history[0].data else {
        panic!("expected JSON workflow start");
    };
    assert_eq!(input.codec(), durust::CodecId::Json);

    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-payload-json-map-scheduler")),
            workflow_claim_opts("payload-json-map-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest_with_codec(
        ["json-map-input-0", "json-map-input-1"]
            .into_iter()
            .map(|label| json_payload(&large_payload(label)))
            .collect(),
        1,
        durust::CodecId::Json,
    )
    .unwrap();
    assert_eq!(input_manifest.codec(), durust::CodecId::Json);

    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new("payload-json-map-activities");
    let retry_policy = durust::RetryPolicy::none();
    let map_task = ActivityMapTask {
        map_command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "payload-json-results".to_owned(),
        max_in_flight: 2,
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityMapScheduled(durust::ActivityMapScheduled {
                        command_id: command_id.clone(),
                        activity_name,
                        task_queue,
                        retry_policy,
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                        input_manifest: input_manifest.clone(),
                        result_manifest_name: "payload-json-results".to_owned(),
                        max_in_flight: 2,
                        fingerprint: durust::activity_map_fingerprint(
                            ActivityName::new("conformance.echo"),
                            durust::payload_digest(&input_manifest),
                            "payload-json-results".to_owned(),
                            2,
                            "sha256:payload-json-map-options".to_owned(),
                        ),
                    }),
                )],
                schedule_activity_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("payload-json-map-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(
            WorkerId::new(format!("{prefix}-payload-json-map-0")),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first JSON map task");
    let second = backend
        .claim_activity_task(
            WorkerId::new(format!("{prefix}-payload-json-map-1")),
            activity_opts,
        )
        .await
        .unwrap()
        .expect("second JSON map task");
    assert_eq!(first.task.input.codec(), durust::CodecId::Json);
    assert_eq!(second.task.input.codec(), durust::CodecId::Json);
    assert_eq!(
        durust::decode_payload::<String>(&first.task.input).unwrap(),
        large_payload("json-map-input-0")
    );
    assert_eq!(
        durust::decode_payload::<String>(&second.task.input).unwrap(),
        large_payload("json-map-input-1")
    );

    backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim,
            result: json_payload(&large_payload("json-map-result-0")),
        })
        .await
        .unwrap();
    backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: json_payload(&large_payload("json-map-result-1")),
        })
        .await
        .unwrap();

    let history = stream_history(&backend, run_id.clone()).await;
    let HistoryEventData::ActivityMapScheduled(scheduled) = &history[1].data else {
        panic!("expected JSON activity map scheduled event");
    };
    assert_eq!(scheduled.input_manifest.codec(), durust::CodecId::Json);
    let manifest: ActivityMapInputManifest =
        durust::decode_payload(&scheduled.input_manifest).unwrap();
    for page in &manifest.pages {
        assert_eq!(page.codec(), durust::CodecId::Json);
        let page: durust::ActivityMapInputPage = durust::decode_payload(page).unwrap();
        assert_eq!(page.items[0].codec(), durust::CodecId::Json);
    }

    let HistoryEventData::ActivityMapCompleted(completed) = &history[2].data else {
        panic!("expected JSON activity map completed event");
    };
    assert_eq!(completed.result_manifest.codec(), durust::CodecId::Json);
    let result_manifest: ActivityMapResultManifest =
        durust::decode_payload(&completed.result_manifest).unwrap();
    for page in &result_manifest.pages {
        assert_eq!(page.codec(), durust::CodecId::Json);
        let page: durust::ActivityMapResultPage = durust::decode_payload(page).unwrap();
        assert_eq!(page.results[0].codec(), durust::CodecId::Json);
    }
    let results = durust::decode_activity_map_result_refs(&completed.result_manifest).unwrap();
    assert_eq!(
        results
            .iter()
            .map(|payload| durust::decode_payload::<String>(payload).unwrap())
            .collect::<Vec<_>>(),
        vec![
            large_payload("json-map-result-0"),
            large_payload("json-map-result-1"),
        ]
    );

    run_id
}

fn json_payload<T>(value: &T) -> durust::PayloadRef
where
    T: Serialize + ?Sized,
{
    durust::encode_payload_with_codec(value, durust::CodecId::Json).unwrap()
}

fn assert_string_map_item(task: &durust::ActivityTask, ordinal: u64, label: &str) {
    let map_item = task.map_item.as_ref().expect("map item metadata");
    assert_eq!(map_item.item_ordinal, ordinal);
    assert_eq!(
        durust::decode_payload::<String>(&task.input).unwrap(),
        large_payload(label)
    );
    assert!(matches!(task.input, durust::PayloadRef::Inline { .. }));
}

async fn workflow_claim_filters_by_queue_and_registered_type<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/claim-filter", "claim-filter-workflows", input(9))
        .await
        .unwrap();

    let wrong_queue = backend
        .claim_workflow_task(
            WorkerId::new("wrong-queue-worker"),
            workflow_claim_opts("other-workflows"),
        )
        .await
        .unwrap();
    assert!(wrong_queue.is_none());

    let wrong_type = backend
        .claim_workflow_task(
            WorkerId::new("wrong-type-worker"),
            ClaimWorkflowTaskOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("claim-filter-workflows"),
                registered_workflow_types: vec![WorkflowType::new("other.workflow", 1)],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    assert!(wrong_type.is_none());

    let matched = backend
        .claim_workflow_task(
            WorkerId::new("matched-worker"),
            workflow_claim_opts("claim-filter-workflows"),
        )
        .await
        .unwrap();
    assert!(matched.is_some());
}

async fn start_workflow_is_idempotent<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let first = client
        .start_workflow::<workflow>("wf/idempotent", "idempotent-workflows", input(1))
        .await
        .unwrap();
    let second = client
        .start_workflow::<workflow>("wf/idempotent", "idempotent-workflows", input(1))
        .await
        .unwrap();
    assert_eq!(first, second);
}

async fn stream_history_honors_bounds<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/stream", "stream-workflows", input(2))
        .await
        .unwrap();
    let start_only = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap();
    assert_eq!(start_only.events.len(), 1);
    assert!(!start_only.has_more);

    let mut worker = worker(backend.clone(), "stream-workflows", "stream-activities");
    worker.run_workflow_once().await.unwrap();
    let one_event = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id: run_id.clone(),
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(2),
            max_events: 1,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap();
    assert_eq!(one_event.events.len(), 1);
    assert!(one_event.has_more);

    let one_event_by_byte_budget = backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(2),
            max_events: 100,
            max_bytes: 1,
        })
        .await
        .unwrap();
    assert_eq!(one_event_by_byte_budget.events.len(), 1);
    assert!(one_event_by_byte_budget.has_more);
}

async fn stale_workflow_task_commit_conflicts<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/stale-commit", "stale-workflows", input(3))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("worker"),
            workflow_claim_opts("stale-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId::ZERO,
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Conflict);
}

async fn batch_workflow_task_claim_and_commit_results_are_ordered<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/batch-commit-a", "batch-workflows", input(11))
        .await
        .unwrap();
    client
        .start_workflow::<workflow>("wf/batch-commit-b", "batch-workflows", input(12))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("batch-workflows");
    let mut claimed = backend
        .claim_workflow_tasks(
            WorkerId::new("batch-worker"),
            ClaimWorkflowTasksOptions {
                claim: claim_opts,
                limit: 2,
                shard_filter: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);
    let first = claimed.remove(0);
    let second = claimed.remove(0);
    let completion = WorkflowTaskCommit {
        expected_tail_event_id: first.replay_target_event_id,
        append_events: vec![NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
            result: durust::encode_payload(&first.run_id.0).unwrap(),
        })],
        ..WorkflowTaskCommit::default()
    };
    let stale = WorkflowTaskCommit {
        expected_tail_event_id: EventId::ZERO,
        ..WorkflowTaskCommit::default()
    };
    let results = backend
        .commit_workflow_tasks(WorkflowTaskCommitBatch {
            commits: vec![
                WorkflowTaskCommitInput {
                    claim: first.claim.clone(),
                    commit: completion,
                },
                WorkflowTaskCommitInput {
                    claim: second.claim.clone(),
                    commit: stale,
                },
            ],
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].claim.run_id, first.run_id);
    assert_eq!(
        results[0].result,
        Ok(CommitOutcome::Committed {
            new_tail_event_id: EventId(2),
        })
    );
    assert_eq!(results[1].claim.run_id, second.run_id);
    assert_eq!(results[1].result, Ok(CommitOutcome::Conflict));
}

async fn released_workflow_task_is_claimable_again<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/release", "release-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("release-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("worker-a"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    backend
        .release_workflow_task(
            claimed.claim,
            durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::CacheEvicted),
        )
        .await
        .unwrap();

    let reclaimed = backend
        .claim_workflow_task(WorkerId::new("worker-b"), claim_opts)
        .await
        .unwrap();
    assert!(reclaimed.is_some());
}

// Parametrized on how time passes because delayed visibility is a clock
// comparison: memory follows the virtual clock (`advance_time`) while the SQL
// providers compare against the wall clock and must really sleep.
async fn delayed_released_workflow_task_is_not_claimable_until_visible<B, Advance, AdvanceFut>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    advance_past_delay: Advance,
) where
    B: DurableBackend,
    Advance: FnOnce() -> AdvanceFut,
    AdvanceFut: Future<Output = ()>,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>(workflow_id, workflow_queue, input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts(workflow_queue);
    let claimed = backend
        .claim_workflow_task(WorkerId::new("worker-a"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    backend
        .release_workflow_task(
            claimed.claim,
            durust::WorkflowTaskRelease::delayed(
                durust::WorkflowTaskReason::CacheEvicted,
                Duration::from_millis(25),
            ),
        )
        .await
        .unwrap();

    let hidden = backend
        .claim_workflow_task(WorkerId::new("worker-b"), claim_opts.clone())
        .await
        .unwrap();
    assert!(hidden.is_none());

    advance_past_delay().await;
    let visible = backend
        .claim_workflow_task(WorkerId::new("worker-c"), claim_opts)
        .await
        .unwrap();
    assert!(visible.is_some());
}

async fn query_projection_updates_atomically_and_reads_payload_refs<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/query-raw", "query-raw-workflows", input(5))
        .await
        .unwrap();
    let req = durust::QueryProjectionRequest {
        namespace: Namespace::default(),
        workflow_id: durust::WorkflowId::new("wf/query-raw"),
    };
    assert_eq!(
        backend.query_projection(req.clone()).await.unwrap(),
        durust::QueryProjectionOutcome::NotFound
    );

    let claim_opts = workflow_claim_opts("query-raw-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("query-raw-worker"), claim_opts)
        .await
        .unwrap()
        .expect("workflow task");
    assert_eq!(
        backend.query_projection(req.clone()).await.unwrap(),
        durust::QueryProjectionOutcome::NotFound
    );
    let stale_payload = durust::encode_payload(&"stale").unwrap();
    let conflict = backend
        .commit_workflow_task(
            claimed.claim.clone(),
            WorkflowTaskCommit {
                expected_tail_event_id: EventId::ZERO,
                query_projection: Some(stale_payload),
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(conflict, CommitOutcome::Conflict);
    assert_eq!(
        backend.query_projection(req.clone()).await.unwrap(),
        durust::QueryProjectionOutcome::NotFound
    );

    let reclaimed = backend
        .claim_workflow_task(
            WorkerId::new("query-raw-reclaimer"),
            workflow_claim_opts("query-raw-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task after conflict");
    let projection_payload = durust::encode_payload(&"visible").unwrap();
    let committed = backend
        .commit_workflow_task(
            reclaimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                query_projection: Some(projection_payload.clone()),
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        committed,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(1)
        }
    );
    assert_eq!(
        backend.query_projection(req).await.unwrap(),
        durust::QueryProjectionOutcome::Found {
            run_id: claimed.run_id,
            event_id: EventId(1),
            payload: projection_payload,
        }
    );
}

// Reads back the provider-minted blob ref for a freshly started large-input
// workflow so tests can derive the provider's own URI scheme without
// hardcoding it. The input exceeds the default inline threshold so it offloads
// under any payload configuration.
async fn provider_offloaded_input_ref<B>(
    backend: &B,
    workflow_id: &str,
    workflow_queue: &str,
) -> durust::PayloadRef
where
    B: DurableBackend,
{
    let run_id = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new(workflow_id),
            workflow_type: WorkflowType::new("conformance.workflow", 1),
            task_queue: TaskQueue::new(workflow_queue),
            input: durust::encode_payload(&format!("{workflow_id}:{}", "x".repeat(64 * 1024)))
                .unwrap(),
        })
        .await
        .unwrap()
        .run_id()
        .clone();
    let raw_events = backend
        .stream_history_for_replay(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(1),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events;
    let HistoryEventData::WorkflowStarted { input, .. } = &raw_events[0].data else {
        panic!("expected workflow start event");
    };
    assert!(
        matches!(input, durust::PayloadRef::Blob { .. }),
        "large workflow input should be provider-offloaded"
    );
    input.clone()
}

async fn claim_conformance_workflow<B>(
    backend: &B,
    worker: &str,
    queue: &str,
) -> durust::ClaimedWorkflowTask
where
    B: DurableBackend,
{
    backend
        .claim_workflow_task(WorkerId::new(worker), workflow_claim_opts(queue))
        .await
        .unwrap()
        .expect("workflow task")
}

fn projection_only_commit(payload: durust::PayloadRef) -> WorkflowTaskCommit {
    WorkflowTaskCommit {
        expected_tail_event_id: EventId(1),
        query_projection: Some(payload),
        ..WorkflowTaskCommit::default()
    }
}

async fn missing_provider_blob_ref_is_rejected<B>(backend: B)
where
    B: DurableBackend,
{
    // A ref carrying the provider's own scheme must be validated at commit
    // time: a digest missing from the provider's store rejects the commit.
    let source_ref = provider_offloaded_input_ref(
        &backend,
        "wf/missing-blob-source",
        "missing-blob-source-workflows",
    )
    .await;
    let durust::PayloadRef::Blob {
        codec,
        schema_fingerprint,
        compression,
        encryption,
        digest,
        size,
        uri,
    } = source_ref
    else {
        unreachable!();
    };
    let missing = durust::PayloadRef::Blob {
        codec,
        schema_fingerprint: schema_fingerprint.clone(),
        compression,
        encryption: encryption.clone(),
        digest: "sha256:missing".to_owned(),
        size,
        uri: uri.replace(digest.as_str(), "sha256:missing"),
    };
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/missing-blob", "missing-blob-workflows", input(5))
        .await
        .unwrap();
    let claimed =
        claim_conformance_workflow(&backend, "missing-blob-worker", "missing-blob-workflows").await;
    let err = backend
        .commit_workflow_task(claimed.claim, projection_only_commit(missing))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::PayloadDecode(message) if message.contains("missing payload blob"))
    );

    // A scheme the provider does not own is opaque: the commit persists the
    // ref unchanged and provider hydration returns it as-is. Only a decorating
    // payload layer that owns the scheme may resolve it.
    let foreign = durust::PayloadRef::Blob {
        codec,
        schema_fingerprint,
        compression,
        encryption,
        digest: "sha256:foreign".to_owned(),
        size,
        uri: "test-unknown://payload/sha256:foreign".to_owned(),
    };
    client
        .start_workflow::<workflow>(
            "wf/foreign-scheme-blob",
            "foreign-scheme-blob-workflows",
            input(5),
        )
        .await
        .unwrap();
    let claimed = claim_conformance_workflow(
        &backend,
        "foreign-scheme-blob-worker",
        "foreign-scheme-blob-workflows",
    )
    .await;
    backend
        .commit_workflow_task(claimed.claim, projection_only_commit(foreign.clone()))
        .await
        .unwrap();
    let projection = backend
        .query_projection(durust::QueryProjectionRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new("wf/foreign-scheme-blob"),
        })
        .await
        .unwrap();
    let durust::QueryProjectionOutcome::Found { payload, .. } = projection else {
        panic!("expected opaque foreign-scheme projection");
    };
    assert_eq!(payload, foreign);
    let hydrated = backend.hydrate_payload(payload).await.unwrap();
    assert_eq!(hydrated, foreign);
}

async fn provider_blob_ref_metadata_mismatch_is_rejected<B>(backend: B)
where
    B: DurableBackend,
{
    // Metadata validation applies to refs carrying the provider's own scheme;
    // the source ref supplies that scheme with its real digest.
    let source_ref = provider_offloaded_input_ref(
        &backend,
        "wf/blob-metadata-source",
        "blob-metadata-source-workflows",
    )
    .await;
    let durust::PayloadRef::Blob {
        codec,
        schema_fingerprint,
        compression,
        encryption,
        digest,
        size,
        uri,
    } = source_ref
    else {
        unreachable!();
    };

    let cases = [
        (
            "schema",
            durust::PayloadRef::Blob {
                codec,
                schema_fingerprint: durust::SchemaFingerprint("sha256:mismatched".to_owned()),
                compression,
                encryption: encryption.clone(),
                digest: digest.clone(),
                size,
                uri: uri.clone(),
            },
        ),
        (
            "codec",
            durust::PayloadRef::Blob {
                codec: durust::CodecId::Json,
                schema_fingerprint: schema_fingerprint.clone(),
                compression,
                encryption: encryption.clone(),
                digest: digest.clone(),
                size,
                uri: uri.clone(),
            },
        ),
    ];

    for (case, mismatched) in cases {
        let workflow_id = format!("wf/blob-metadata-mismatch/{case}");
        let run_id = backend
            .start_workflow(durust::StartWorkflowRequest {
                namespace: Namespace::default(),
                workflow_id: durust::WorkflowId::new(&workflow_id),
                workflow_type: WorkflowType::new("conformance.workflow", 1),
                task_queue: TaskQueue::new("blob-metadata-mismatch-workflows"),
                input: durust::encode_payload(&0_u64).unwrap(),
            })
            .await
            .unwrap()
            .run_id()
            .clone();
        let claimed = backend
            .claim_workflow_task(
                WorkerId::new(format!("blob-metadata-mismatch-{case}")),
                workflow_claim_opts("blob-metadata-mismatch-workflows"),
            )
            .await
            .unwrap()
            .expect("workflow task");
        assert_eq!(claimed.run_id, run_id);
        let err = backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: EventId(1),
                    query_projection: Some(mismatched),
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::PayloadDecode(message) if message.contains("payload blob metadata mismatch")),
            "unexpected error for {case} mismatch: {err:?}"
        );
    }
}

async fn workflow_change_version_index_tracks_markers_and_open_status<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/version-index", "version-index-workflows", input(5))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("version-index-worker"),
            workflow_claim_opts("version-index-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::VersionMarker(durust::VersionMarker {
                        command_id: command_id.clone(),
                        change_id: "replace-a-with-b".to_owned(),
                        version: 1,
                    }),
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let open = backend
        .workflow_change_versions(durust::WorkflowChangeVersionsRequest {
            namespace: Namespace::default(),
            workflow_id: None,
            run_id: Some(run_id.clone()),
            change_id: Some("replace-a-with-b".to_owned()),
        })
        .await
        .unwrap();
    assert!(!open.safe_to_remove());
    assert_eq!(open.records.len(), 1);
    let record = &open.records[0];
    assert_eq!(
        record.workflow_id,
        durust::WorkflowId::new("wf/version-index")
    );
    assert_eq!(
        record.workflow_type,
        WorkflowType::new("conformance.workflow", 1)
    );
    assert_eq!(record.run_id, run_id);
    assert_eq!(record.change_id, "replace-a-with-b");
    assert_eq!(record.version, 1);
    assert_eq!(
        record.marker_kind,
        durust::WorkflowChangeMarkerKind::Version
    );
    assert_eq!(record.status, durust::WorkflowChangeVersionStatus::Open);
    assert_eq!(record.command_seq, durust::CommandSeq(1));
    assert_eq!(record.first_event_id, EventId(2));

    client
        .cancel_workflow("wf/version-index", "conformance close")
        .await
        .unwrap();
    let closed = backend
        .workflow_change_versions(durust::WorkflowChangeVersionsRequest {
            namespace: Namespace::default(),
            workflow_id: None,
            run_id: Some(run_id),
            change_id: Some("replace-a-with-b".to_owned()),
        })
        .await
        .unwrap();
    assert!(closed.safe_to_remove());
    assert_eq!(closed.records.len(), 1);
    assert_eq!(
        closed.records[0].status,
        durust::WorkflowChangeVersionStatus::Closed
    );
}

async fn continue_as_new_closes_current_run_and_starts_claimable_next_run<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let first_run_id = client
        .start_workflow::<workflow>("wf/continue-conformance", "continue-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("continue-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("continue-worker"), claim_opts.clone())
        .await
        .unwrap()
        .expect("initial workflow task");
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowContinuedAsNew {
                        input: durust::encode_payload(&7_u64).unwrap(),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let old_history = stream_history(&backend, first_run_id.clone()).await;
    assert_eq!(old_history.len(), 2);
    assert!(matches!(
        old_history[1].data,
        HistoryEventData::WorkflowContinuedAsNew { .. }
    ));

    let next = backend
        .claim_workflow_task(WorkerId::new("continue-next-worker"), claim_opts)
        .await
        .unwrap()
        .expect("continued workflow task");
    assert_ne!(next.run_id, first_run_id);
    assert_eq!(
        next.workflow_id,
        durust::WorkflowId::new("wf/continue-conformance")
    );
    assert_eq!(next.reason, durust::WorkflowTaskReason::WorkflowStarted);
    assert_eq!(next.replay_target_event_id, EventId(1));
    let new_history = stream_history(&backend, next.run_id).await;
    assert_eq!(new_history.len(), 1);
    let HistoryEventData::WorkflowStarted { input, .. } = &new_history[0].data else {
        panic!("expected new run start");
    };
    assert_eq!(durust::decode_payload::<u64>(input).unwrap(), 7);
}

async fn signal_inbox_is_idempotent_ordered_and_consumed_by_commit<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/signal-inbox", "signal-inbox-workflows", input(5))
        .await
        .unwrap();
    let accepted = client
        .signal_workflow("wf/signal-inbox", "ready", "signal/inbox/1", "first")
        .await
        .unwrap();
    assert_eq!(accepted, durust::SignalWorkflowOutcome::Accepted);
    let duplicate = client
        .signal_workflow("wf/signal-inbox", "ready", "signal/inbox/1", "duplicate")
        .await
        .unwrap();
    assert_eq!(duplicate, durust::SignalWorkflowOutcome::Duplicate);
    let second = client
        .signal_workflow("wf/signal-inbox", "ready", "signal/inbox/2", "second")
        .await
        .unwrap();
    assert_eq!(second, durust::SignalWorkflowOutcome::Accepted);
    let other = client
        .signal_workflow("wf/signal-inbox", "other", "signal/inbox/other", "other")
        .await
        .unwrap();
    assert_eq!(other, durust::SignalWorkflowOutcome::Accepted);

    let batch = backend
        .read_signal_inboxes(durust::ReadSignalInboxesRequest {
            requests: vec![
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("ready"),
                },
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("missing"),
                },
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("other"),
                },
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("ready"),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(batch.len(), 4);
    let first_batch = batch[0].as_ref().expect("first ready signal");
    assert_eq!(
        first_batch.signal_id,
        durust::SignalId::new("signal/inbox/1")
    );
    assert_eq!(
        durust::decode_payload::<String>(&first_batch.payload).unwrap(),
        "first"
    );
    assert!(batch[1].is_none());
    let other_batch = batch[2].as_ref().expect("other signal");
    assert_eq!(
        other_batch.signal_id,
        durust::SignalId::new("signal/inbox/other")
    );
    assert_eq!(
        durust::decode_payload::<String>(&other_batch.payload).unwrap(),
        "other"
    );
    assert_eq!(
        batch[3].as_ref().expect("repeated ready request").signal_id,
        durust::SignalId::new("signal/inbox/1")
    );

    let first_inbox = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id: run_id.clone(),
            signal_name: durust::SignalName::new("ready"),
        })
        .await
        .unwrap()
        .expect("first signal");
    assert_eq!(
        first_inbox.signal_id,
        durust::SignalId::new("signal/inbox/1")
    );
    assert_eq!(
        durust::decode_payload::<String>(&first_inbox.payload).unwrap(),
        "first"
    );

    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("signal-consumer"),
            workflow_claim_opts("signal-inbox-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                consume_signals: vec![first_inbox.signal_id],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(1)
        }
    );

    let second_inbox = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id: run_id.clone(),
            signal_name: durust::SignalName::new("ready"),
        })
        .await
        .unwrap()
        .expect("second signal");
    assert_eq!(
        second_inbox.signal_id,
        durust::SignalId::new("signal/inbox/2")
    );

    let post_consume_batch = backend
        .read_signal_inboxes(durust::ReadSignalInboxesRequest {
            requests: vec![
                durust::ReadSignalInboxRequest {
                    run_id: run_id.clone(),
                    signal_name: durust::SignalName::new("ready"),
                },
                durust::ReadSignalInboxRequest {
                    run_id,
                    signal_name: durust::SignalName::new("other"),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(
        post_consume_batch[0]
            .as_ref()
            .expect("second ready signal")
            .signal_id,
        durust::SignalId::new("signal/inbox/2")
    );
    assert_eq!(
        post_consume_batch[1]
            .as_ref()
            .expect("other signal remains unconsumed")
            .signal_id,
        durust::SignalId::new("signal/inbox/other")
    );
}

fn signal_race_claim_opts(queue: &str) -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.signal-race", 1)],
        lease_duration: Duration::from_secs(30),
    }
}

/// The commit the signal-race workflow's first task produces: no appends, one
/// signal wait registered for command seq 1 on signal name `go`.
fn signal_wait_commit(run_id: &durust::RunId, expected_tail: EventId) -> WorkflowTaskCommit {
    let command_id = durust::command_id(run_id, 1);
    WorkflowTaskCommit {
        expected_tail_event_id: expected_tail,
        upsert_waits: vec![durust::WaitRecord {
            wait_id: durust::WaitId::new(format!(
                "{}:{}:signal",
                command_id.run_id, command_id.seq.0
            )),
            run_id: run_id.clone(),
            command_id,
            kind: durust::WaitKind::Signal,
            key: "go".to_owned(),
            ready_at: None,
        }],
        ..WorkflowTaskCommit::default()
    }
}

/// Runs a real worker until idle and asserts the signal-race run finished by
/// consuming a signal and completing with the expected payload.
async fn assert_signal_race_run_completes<B>(
    backend: &B,
    queue: &str,
    run_id: durust::RunId,
    expected_result: &str,
) where
    B: DurableBackend,
{
    let mut worker = Worker::builder(backend.clone())
        .workflow_task_queue(queue)
        .activity_task_queue(queue)
        .register_workflow(signal_race_workflow)
        .build();
    worker.run_until_idle().await.unwrap();

    let history = stream_history(backend, run_id).await;
    assert_eq!(
        history.len(),
        3,
        "expected WorkflowStarted, SignalConsumed, WorkflowCompleted; got {history:?}"
    );
    assert!(matches!(
        history[1].data,
        HistoryEventData::SignalConsumed(_)
    ));
    let HistoryEventData::WorkflowCompleted { result } = &history[2].data else {
        panic!("expected WorkflowCompleted, got {:?}", history[2].data);
    };
    assert_eq!(
        durust::decode_payload::<String>(result).unwrap(),
        expected_result
    );
}

/// A signal delivered after a task is claimed but before its commit registers
/// the signal wait must leave the run immediately claimable with
/// `SignalReceived`; the commit's ready-reason recomputation is what prevents
/// the delivery from being lost until an unrelated event pokes the run.
async fn signal_between_claim_and_commit_wakes_workflow<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<signal_race_workflow>(
            "wf/signal-race-new-wait",
            "signal-race-new-wait",
            input(1),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("signal-race-claimer"),
            signal_race_claim_opts("signal-race-new-wait"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    assert_eq!(claimed.reason, durust::WorkflowTaskReason::WorkflowStarted);

    // The race: the signal lands while the task is claimed and the wait it
    // matches is only created by the claimed task's commit below.
    let accepted = client
        .signal_workflow(
            "wf/signal-race-new-wait",
            "go",
            "signal/race-new-wait/1",
            "raced-hello",
        )
        .await
        .unwrap();
    assert_eq!(accepted, durust::SignalWorkflowOutcome::Accepted);

    let outcome = backend
        .commit_workflow_task(claimed.claim, signal_wait_commit(&run_id, EventId(1)))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(1)
        }
    );

    let woken = backend
        .claim_workflow_task(
            WorkerId::new("signal-race-waker"),
            signal_race_claim_opts("signal-race-new-wait"),
        )
        .await
        .unwrap()
        .expect("run should be immediately claimable after the racing signal");
    assert_eq!(woken.reason, durust::WorkflowTaskReason::SignalReceived);
    backend
        .release_workflow_task(
            woken.claim,
            durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::SignalReceived),
        )
        .await
        .unwrap();

    // The real workflow consumes the raced signal on its next task.
    assert_signal_race_run_completes(
        &backend,
        "signal-race-new-wait",
        run_id.clone(),
        "raced-hello",
    )
    .await;
    let inbox = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id,
            signal_name: durust::SignalName::new("go"),
        })
        .await
        .unwrap();
    assert!(inbox.is_none(), "raced signal should be consumed");
}

/// A signal delivered during the claim window for a wait that already existed
/// before the claim must survive a commit that carries no mutations: the
/// commit's unconditional ready-reason write would otherwise erase the wakeup
/// the delivery recorded.
async fn signal_during_claim_window_survives_empty_commit<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<signal_race_workflow>(
            "wf/signal-race-existing-wait",
            "signal-race-existing-wait",
            input(1),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("signal-existing-scheduler"),
            signal_race_claim_opts("signal-race-existing-wait"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    backend
        .commit_workflow_task(claimed.claim, signal_wait_commit(&run_id, EventId(1)))
        .await
        .unwrap();
    // Blocked on the signal: not claimable until a delivery arrives.
    assert!(
        backend
            .claim_workflow_task(
                WorkerId::new("signal-existing-early"),
                signal_race_claim_opts("signal-race-existing-wait"),
            )
            .await
            .unwrap()
            .is_none()
    );

    let first = client
        .signal_workflow(
            "wf/signal-race-existing-wait",
            "go",
            "signal/race-existing/1",
            "first",
        )
        .await
        .unwrap();
    assert_eq!(first, durust::SignalWorkflowOutcome::Accepted);
    let woken = backend
        .claim_workflow_task(
            WorkerId::new("signal-existing-claimer"),
            signal_race_claim_opts("signal-race-existing-wait"),
        )
        .await
        .unwrap()
        .expect("signal delivery should wake the run");
    assert_eq!(woken.reason, durust::WorkflowTaskReason::SignalReceived);

    // Second delivery lands while the task is claimed, matching the
    // pre-existing wait; the claimed task then commits nothing (a spurious
    // wake), which must not erase the pending delivery's readiness.
    let second = client
        .signal_workflow(
            "wf/signal-race-existing-wait",
            "go",
            "signal/race-existing/2",
            "second",
        )
        .await
        .unwrap();
    assert_eq!(second, durust::SignalWorkflowOutcome::Accepted);
    let outcome = backend
        .commit_workflow_task(
            woken.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(1)
        }
    );

    let rewoken = backend
        .claim_workflow_task(
            WorkerId::new("signal-existing-rewake"),
            signal_race_claim_opts("signal-race-existing-wait"),
        )
        .await
        .unwrap()
        .expect("pending signals must keep the run claimable after an empty commit");
    assert_eq!(rewoken.reason, durust::WorkflowTaskReason::SignalReceived);
    backend
        .release_workflow_task(
            rewoken.claim,
            durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::SignalReceived),
        )
        .await
        .unwrap();

    // The real workflow consumes the oldest delivery; the second stays in the
    // inbox for the (now terminal) run.
    assert_signal_race_run_completes(
        &backend,
        "signal-race-existing-wait",
        run_id.clone(),
        "first",
    )
    .await;
    let remaining = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id,
            signal_name: durust::SignalName::new("go"),
        })
        .await
        .unwrap()
        .expect("second delivery stays unconsumed");
    assert_eq!(
        remaining.signal_id,
        durust::SignalId::new("signal/race-existing/2")
    );
}

/// The claim-window signal race applied to a multi-run `commit_workflow_tasks`
/// batch: every committed run with a pending consumable signal must come out
/// claimable, exercising the set-based batch commit path on providers that
/// have one.
async fn signal_between_claim_and_commit_wakes_workflows_in_batch_commit<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let queue = "signal-race-batch";
    let mut claims = Vec::new();
    for index in 0..2 {
        let workflow_id = format!("wf/signal-race-batch/{index}");
        let run_id = client
            .start_workflow::<signal_race_workflow>(&workflow_id, queue, input(index))
            .await
            .unwrap();
        let claimed = backend
            .claim_workflow_task(
                WorkerId::new(format!("signal-batch-claimer-{index}")),
                signal_race_claim_opts(queue),
            )
            .await
            .unwrap()
            .expect("workflow task");
        assert_eq!(claimed.run_id, run_id);
        let accepted = client
            .signal_workflow(
                &workflow_id,
                "go",
                format!("signal/race-batch/{index}"),
                format!("batch-{index}"),
            )
            .await
            .unwrap();
        assert_eq!(accepted, durust::SignalWorkflowOutcome::Accepted);
        claims.push(claimed);
    }

    let results = backend
        .commit_workflow_tasks(WorkflowTaskCommitBatch {
            commits: claims
                .iter()
                .map(|claimed| WorkflowTaskCommitInput {
                    claim: claimed.claim.clone(),
                    commit: signal_wait_commit(&claimed.run_id, EventId(1)),
                })
                .collect(),
        })
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    for result in &results {
        assert_eq!(
            *result.result.as_ref().unwrap(),
            CommitOutcome::Committed {
                new_tail_event_id: EventId(1)
            }
        );
    }

    let mut woken = Vec::new();
    for index in 0..2 {
        let task = backend
            .claim_workflow_task(
                WorkerId::new(format!("signal-batch-waker-{index}")),
                signal_race_claim_opts(queue),
            )
            .await
            .unwrap()
            .expect("both committed runs should be claimable after racing signals");
        assert_eq!(task.reason, durust::WorkflowTaskReason::SignalReceived);
        woken.push(task);
    }
    let woken_run_ids = woken
        .iter()
        .map(|task| task.run_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(woken_run_ids.len(), 2, "each run wakes exactly once");
    for task in woken {
        backend
            .release_workflow_task(
                task.claim,
                durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::SignalReceived),
            )
            .await
            .unwrap();
    }

    for (index, claimed) in claims.into_iter().enumerate() {
        assert_signal_race_run_completes(
            &backend,
            queue,
            claimed.run_id,
            &format!("batch-{index}"),
        )
        .await;
    }
}

/// Once a run is terminal every mutation kind in a stale holder's commit is
/// rejected identically across providers. Terminal transitions clear the
/// claim, so the fencing check fires first and the rejection is `StaleLease`
/// for every kind; the deeper `TerminalWorkflow` guard is pinned per provider
/// with forged state in the provider unit suites.
async fn terminal_run_fences_stale_mutating_commits_identically<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<signal_race_workflow>(
            "wf/terminal-fence",
            "terminal-fence-workflows",
            input(1),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("terminal-fence-claimer"),
            signal_race_claim_opts("terminal-fence-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let cancelled = client
        .cancel_workflow("wf/terminal-fence", "fence test")
        .await
        .unwrap();
    let durust::CancelWorkflowOutcome::Cancelled { event_id, .. } = cancelled else {
        panic!("expected cancellation, got {cancelled:?}");
    };

    for (kind, commit) in terminal_fence_commits(&run_id, event_id) {
        let err = backend
            .commit_workflow_task(claimed.claim.clone(), commit)
            .await
            .expect_err(kind);
        assert!(
            matches!(err, Error::StaleLease),
            "stale `{kind}` commit against the cancelled run should fence as StaleLease, got {err:?}"
        );
    }
}

/// One commit per workflow-visible mutation kind, aimed at the post-cancel
/// tail so only claim fencing (not a tail conflict) decides the outcome.
fn terminal_fence_commits(
    run_id: &durust::RunId,
    expected_tail: EventId,
) -> Vec<(&'static str, WorkflowTaskCommit)> {
    let command_id = durust::command_id(run_id, 900);
    let base = WorkflowTaskCommit {
        expected_tail_event_id: expected_tail,
        ..WorkflowTaskCommit::default()
    };
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("terminal-fence-activities"),
        retry_policy: durust::RetryPolicy::exponential().max_attempts(1),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-options".to_owned(),
        ),
    };
    vec![
        (
            "append_events",
            WorkflowTaskCommit {
                append_events: vec![NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
                    result: durust::encode_payload(&0_u64).unwrap(),
                })],
                ..base.clone()
            },
        ),
        (
            "schedule_activities",
            WorkflowTaskCommit {
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..base.clone()
            },
        ),
        (
            "upsert_waits",
            WorkflowTaskCommit {
                upsert_waits: vec![durust::WaitRecord {
                    wait_id: durust::WaitId::new(format!("{run_id}:900:signal")),
                    run_id: run_id.clone(),
                    command_id: command_id.clone(),
                    kind: durust::WaitKind::Signal,
                    key: "go".to_owned(),
                    ready_at: None,
                }],
                ..base.clone()
            },
        ),
        (
            "consume_signals",
            WorkflowTaskCommit {
                consume_signals: vec![durust::SignalId::new("terminal-fence-signal")],
                ..base.clone()
            },
        ),
        (
            "delete_waits",
            WorkflowTaskCommit {
                delete_waits: vec![durust::WaitId::new(format!("{run_id}:900:signal"))],
                ..base.clone()
            },
        ),
        (
            "start_child_workflows",
            WorkflowTaskCommit {
                start_child_workflows: vec![durust::ChildStartOutboxMessage {
                    command_id: command_id.clone(),
                    workflow_id: durust::WorkflowId::new(format!("{run_id}/fence-child")),
                    workflow_type: WorkflowType::new("conformance.signal-race", 1),
                    task_queue: TaskQueue::new("terminal-fence-workflows"),
                    input: durust::encode_payload(&Input { value: 0 }).unwrap(),
                    parent_close_policy: durust::ParentClosePolicy::Cancel,
                    child_map_item: None,
                }],
                ..base.clone()
            },
        ),
        (
            "schedule_activity_maps",
            WorkflowTaskCommit {
                schedule_activity_maps: vec![durust::ActivityMapTask {
                    map_command_id: command_id.clone(),
                    activity_name: ActivityName::new("conformance.echo"),
                    task_queue: TaskQueue::new("terminal-fence-activities"),
                    input_manifest: durust::encode_payload(&0_u64).unwrap(),
                    result_manifest_name: "results".to_owned(),
                    max_in_flight: 1,
                    retry_policy: durust::RetryPolicy::exponential().max_attempts(1),
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                }],
                ..base.clone()
            },
        ),
        (
            "schedule_child_workflow_maps",
            WorkflowTaskCommit {
                schedule_child_workflow_maps: vec![ChildWorkflowMapTask {
                    map_command_id: command_id.clone(),
                    workflow_type: WorkflowType::new("conformance.signal-race", 1),
                    task_queue: TaskQueue::new("terminal-fence-workflows"),
                    input_manifest: durust::encode_payload(&0_u64).unwrap(),
                    result_manifest_name: "results".to_owned(),
                    workflow_id_prefix: format!("{run_id}/fence-child-map"),
                    max_in_flight: 1,
                    parent_close_policy: durust::ParentClosePolicy::Cancel,
                    failure_mode: durust::ChildWorkflowMapFailureMode::FailFast,
                }],
                ..base.clone()
            },
        ),
        (
            "cancel_commands",
            WorkflowTaskCommit {
                cancel_commands: vec![command_id],
                ..base.clone()
            },
        ),
        (
            "query_projection",
            WorkflowTaskCommit {
                query_projection: Some(durust::encode_payload(&0_u64).unwrap()),
                ..base
            },
        ),
    ]
}

/// Terminal cleanup deletes the run's operational rows, so late activity
/// calls (heartbeat included) must answer `AlreadyCompleted` from row
/// absence across retries, claim scans must not see the terminal run's
/// tasks, undelivered signals must stay readable through the inbox, and
/// their `signal_id` dedup must survive while new sends fail terminally.
async fn terminal_cleanup_answers_late_calls_and_keeps_undelivered_signals<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(
            "wf/terminal-cleanup",
            "terminal-cleanup-workflows",
            input(5),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("terminal-cleanup-scheduler"),
            workflow_claim_opts("terminal-cleanup-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_payload = durust::encode_payload(&Input { value: 3 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("terminal-cleanup-activities"),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: Some(Duration::from_secs(30)),
        input: input_payload.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input_payload),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                    scheduled.clone(),
                ))],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("terminal-cleanup-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let activity = backend
        .claim_activity_task(
            WorkerId::new("terminal-cleanup-worker"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("activity task");
    // An undelivered signal lands before the run closes.
    let undelivered = client
        .signal_workflow(
            "wf/terminal-cleanup",
            "go",
            "signal/terminal-cleanup/undelivered",
            "pending",
        )
        .await
        .unwrap();
    assert_eq!(undelivered, durust::SignalWorkflowOutcome::Accepted);

    client
        .cancel_workflow("wf/terminal-cleanup", "terminal cleanup test")
        .await
        .unwrap();

    // Late calls answer idempotently from row absence, on every retry.
    for attempt in 0..2 {
        let heartbeat = backend
            .heartbeat_activity(durust::ActivityHeartbeatRequest {
                claim: activity.claim.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            heartbeat,
            durust::ActivityHeartbeatOutcome::AlreadyCompleted,
            "heartbeat retry {attempt}"
        );
        let completed = backend
            .complete_activity(CompleteActivityRequest {
                claim: activity.claim.clone(),
                result: durust::encode_payload(&3_u64).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            completed,
            durust::CompleteActivityOutcome::AlreadyCompleted,
            "complete retry {attempt}"
        );
        let failed = backend
            .fail_activity(FailActivityRequest {
                claim: activity.claim.clone(),
                failure: durust::DurableFailure::new("test.late", "late failure"),
            })
            .await
            .unwrap();
        assert_eq!(
            failed,
            durust::FailActivityOutcome::AlreadyCompleted,
            "fail retry {attempt}"
        );
    }

    // Claim scans no longer see the terminal run's tasks.
    assert!(
        backend
            .claim_activity_task(WorkerId::new("terminal-cleanup-leftover"), activity_opts)
            .await
            .unwrap()
            .is_none()
    );

    // The undelivered signal stays readable and its dedup record survives;
    // a genuinely new send fails terminally.
    let inboxed = backend
        .read_signal_inbox(durust::ReadSignalInboxRequest {
            run_id: run_id.clone(),
            signal_name: durust::SignalName::new("go"),
        })
        .await
        .unwrap()
        .expect("undelivered signal survives terminal cleanup");
    assert_eq!(
        inboxed.signal_id,
        durust::SignalId::new("signal/terminal-cleanup/undelivered")
    );
    let duplicate = client
        .signal_workflow(
            "wf/terminal-cleanup",
            "go",
            "signal/terminal-cleanup/undelivered",
            "pending",
        )
        .await
        .unwrap();
    assert_eq!(duplicate, durust::SignalWorkflowOutcome::Duplicate);
    let fresh = client
        .signal_workflow(
            "wf/terminal-cleanup",
            "go",
            "signal/terminal-cleanup/fresh",
            "rejected",
        )
        .await;
    assert!(matches!(fresh, Err(durust::Error::TerminalWorkflow)));
}

/// Consumed signal rows are the `signal_id` dedup record, and continue-as-new
/// keeps accepting sends under the same workflow id, so cleanup after a
/// continue-as-new transition must retain them: a retried send of an already
/// consumed id must stay `Duplicate` instead of delivering again to the next
/// run.
async fn consumed_signal_dedup_survives_continue_as_new<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/can-signal-dedup", "can-signal-workflows", input(2))
        .await
        .unwrap();
    let consumed_signal_id = durust::SignalId::new("signal/can-dedup/consumed");
    let accepted = client
        .signal_workflow(
            "wf/can-signal-dedup",
            "go",
            consumed_signal_id.0.clone(),
            "first",
        )
        .await
        .unwrap();
    assert_eq!(accepted, durust::SignalWorkflowOutcome::Accepted);

    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("can-signal-scheduler"),
            workflow_claim_opts("can-signal-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let signal_payload = durust::encode_payload(&"first").unwrap();
    // One commit consumes the delivery and continues the run as new.
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![
                    NewHistoryEvent::new(HistoryEventData::SignalConsumed(
                        durust::SignalConsumed {
                            command_id: command_id.clone(),
                            signal_id: consumed_signal_id.clone(),
                            signal_name: durust::SignalName::new("go"),
                            payload: signal_payload,
                            fingerprint: durust::signal_fingerprint(durust::SignalName::new("go")),
                        },
                    )),
                    NewHistoryEvent::new(HistoryEventData::WorkflowContinuedAsNew {
                        input: durust::encode_payload(&Input { value: 3 }).unwrap(),
                    }),
                ],
                consume_signals: vec![consumed_signal_id.clone()],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    // The retried send of the consumed id must stay deduplicated instead of
    // delivering a second time to the next run.
    let retried = client
        .signal_workflow(
            "wf/can-signal-dedup",
            "go",
            consumed_signal_id.0.clone(),
            "first",
        )
        .await
        .unwrap();
    assert_eq!(retried, durust::SignalWorkflowOutcome::Duplicate);

    // The continued run is live under the same workflow id: fresh sends land.
    let fresh = client
        .signal_workflow("wf/can-signal-dedup", "go", "signal/can-dedup/next", "next")
        .await
        .unwrap();
    assert_eq!(fresh, durust::SignalWorkflowOutcome::Accepted);
}

/// Late completion and failure of an activity whose run was cancelled must be
/// idempotently absorbed, and repeated retries must keep returning the same
/// outcome on every provider (cancellation deletes the run's activity rows
/// atomically with the terminal transition; absence answers late calls).
async fn late_activity_completion_after_cancel_is_idempotent_across_retries<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(
            "wf/late-completion-cancel",
            "late-completion-workflows",
            input(5),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("late-completion-scheduler"),
            workflow_claim_opts("late-completion-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_payload = durust::encode_payload(&Input { value: 9 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("late-completion-activities"),
        retry_policy: durust::RetryPolicy::exponential().max_attempts(2),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: input_payload.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input_payload),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                    scheduled.clone(),
                ))],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    let activity = backend
        .claim_activity_task(
            WorkerId::new("late-completion-worker"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("late-completion-activities"),
                registered_activity_names: vec![ActivityName::new("conformance.echo")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("activity task");

    client
        .cancel_workflow("wf/late-completion-cancel", "late completion test")
        .await
        .unwrap();

    // Retried completions and failures after cancellation must return the
    // same idempotent outcome every time; a provider that mutates before
    // validating would flip between error kinds across retries.
    for attempt in 0..2 {
        let completed = backend
            .complete_activity(CompleteActivityRequest {
                claim: activity.claim.clone(),
                result: durust::encode_payload(&9_u64).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            completed,
            durust::CompleteActivityOutcome::AlreadyCompleted,
            "complete retry {attempt}"
        );
        let failed = backend
            .fail_activity(FailActivityRequest {
                claim: activity.claim.clone(),
                failure: durust::DurableFailure::new("test.late", "late failure"),
            })
            .await
            .unwrap();
        assert_eq!(
            failed,
            durust::FailActivityOutcome::AlreadyCompleted,
            "fail retry {attempt}"
        );
    }

    // The cancelled run's history is untouched by the late attempts.
    let history = stream_history(&backend, run_id).await;
    assert!(matches!(
        history.last().map(|event| &event.data),
        Some(HistoryEventData::WorkflowCancelled { .. })
    ));
}

async fn timer_waits_fire_only_when_due_and_make_workflow_claimable<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/timer-wait", "timer-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("timer-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("timer-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let now = backend.current_time().await.unwrap();
    let fire_at = durust::TimestampMs(now.0.saturating_add(50));
    let command_id = durust::command_id(&claimed.run_id, 1);
    let wait_id = durust::WaitId::new(format!("{}:{}:timer", command_id.run_id, command_id.seq.0));
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::TimerStarted(durust::TimerStarted {
                        command_id: command_id.clone(),
                        fire_at,
                        fingerprint: durust::timer_fingerprint("sleep", durust::TimestampMs(50)),
                    }),
                )],
                upsert_waits: vec![durust::WaitRecord {
                    wait_id,
                    run_id: command_id.run_id.clone(),
                    command_id: command_id.clone(),
                    kind: durust::WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(fire_at),
                }],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let early = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(early.fired, 0);
    let hidden = backend
        .claim_workflow_task(WorkerId::new("timer-too-early"), claim_opts.clone())
        .await
        .unwrap();
    assert!(hidden.is_none());

    let due = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: fire_at,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(due.fired, 1);
    let duplicate = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: fire_at,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(duplicate.fired, 0);

    let ready = backend
        .claim_workflow_task(WorkerId::new("timer-ready"), claim_opts)
        .await
        .unwrap()
        .expect("timer-fired workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::TimerFired);
}

async fn activity_retry_reschedules_until_max_attempts<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/activity-retry", "retry-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("retry-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("retry-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    // No backoff: this suite pins retry bookkeeping (attempt counts, history
    // silence, terminal failure), not retry pacing, and must stay immediate
    // for every provider clock. Backoff pacing has its own conformance tests.
    let retry_policy = durust::RetryPolicy::none().max_attempts(2);
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("retry-activities"),
        retry_policy,
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityScheduled(scheduled.clone()),
                )],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("retry-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(WorkerId::new("retry-worker-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(first.task.attempt, 1);
    let retried = backend
        .fail_activity(FailActivityRequest {
            claim: first.claim,
            failure: durust::DurableFailure::new("test.transient", "transient"),
        })
        .await
        .unwrap();
    assert_eq!(
        retried,
        durust::FailActivityOutcome::RetryScheduled { next_attempt: 2 }
    );
    let not_ready = backend
        .claim_workflow_task(WorkerId::new("retry-not-ready"), claim_opts.clone())
        .await
        .unwrap();
    assert!(not_ready.is_none());

    let second = backend
        .claim_activity_task(WorkerId::new("retry-worker-2"), activity_opts)
        .await
        .unwrap()
        .expect("second attempt");
    assert_eq!(second.task.attempt, 2);
    let failed = backend
        .fail_activity(FailActivityRequest {
            claim: second.claim,
            failure: durust::DurableFailure::new("test.permanent", "permanent"),
        })
        .await
        .unwrap();
    assert_eq!(
        failed,
        durust::FailActivityOutcome::Failed {
            event_id: EventId(3)
        }
    );
    let ready = backend
        .claim_workflow_task(WorkerId::new("retry-ready"), claim_opts)
        .await
        .unwrap()
        .expect("activity failed workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityFailed);

    let history = stream_history(&backend, run_id).await;
    assert_eq!(history.len(), 3);
    assert!(matches!(
        history[1].data,
        HistoryEventData::ActivityScheduled(_)
    ));
    let HistoryEventData::ActivityFailed(failed) = &history[2].data else {
        panic!("expected final ActivityFailed event");
    };
    assert_eq!(failed.failure.message, "permanent");
    assert!(!failed.failure.non_retryable);
}

async fn non_retryable_activity_failure_skips_retry_and_wakes_workflow<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(
            "wf/activity-non-retryable",
            "non-retryable-workflows",
            input(5),
        )
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("non-retryable-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("non-retryable-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let retry_policy = durust::RetryPolicy::exponential().max_attempts(5);
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("non-retryable-activities"),
        retry_policy,
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityScheduled(scheduled.clone()),
                )],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("non-retryable-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(
            WorkerId::new("non-retryable-worker-1"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(first.task.attempt, 1);
    let failure = durust::DurableFailure::non_retryable("test.validation", "validation failed");
    let failed = backend
        .fail_activity(FailActivityRequest {
            claim: first.claim,
            failure: failure.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        failed,
        durust::FailActivityOutcome::Failed {
            event_id: EventId(3)
        }
    );
    let no_retry = backend
        .claim_activity_task(WorkerId::new("non-retryable-worker-2"), activity_opts)
        .await
        .unwrap();
    assert!(no_retry.is_none());

    let ready = backend
        .claim_workflow_task(WorkerId::new("non-retryable-ready"), claim_opts)
        .await
        .unwrap()
        .expect("activity failed workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityFailed);

    let history = stream_history(&backend, run_id).await;
    let HistoryEventData::ActivityFailed(failed) = &history[2].data else {
        panic!("expected final ActivityFailed event");
    };
    assert_eq!(failed.failure, failure);
}

async fn activity_timeout_retries_until_max_attempts_then_wakes_workflow<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/activity-timeout", "timeout-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("timeout-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("timeout-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let retry_policy = durust::RetryPolicy::exponential().max_attempts(2);
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("timeout-activities"),
        retry_policy,
        start_to_close_timeout: Some(Duration::from_secs(1)),
        heartbeat_timeout: None,
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityScheduled(scheduled.clone()),
                )],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("timeout-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let after_schedule = backend.current_time().await.unwrap();
    let early = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_schedule.0.saturating_add(100)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(early.timed_out, 0);

    let first = backend
        .claim_activity_task(WorkerId::new("timeout-worker-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(first.task.attempt, 1);
    let retry = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_schedule.0.saturating_add(1_200)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(retry.timed_out, 1);
    let not_ready = backend
        .claim_workflow_task(WorkerId::new("timeout-not-ready"), claim_opts.clone())
        .await
        .unwrap();
    assert!(not_ready.is_none());
    let stale_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim,
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_completion, Error::StaleLease));

    let second = backend
        .claim_activity_task(WorkerId::new("timeout-worker-2"), activity_opts)
        .await
        .unwrap()
        .expect("second attempt");
    assert_eq!(second.task.attempt, 2);
    let final_timeout = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_schedule.0.saturating_add(2_400)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(final_timeout.timed_out, 1);
    let duplicate_timeout = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_schedule.0.saturating_add(2_500)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(duplicate_timeout.timed_out, 0);
    let late_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        late_completion,
        durust::CompleteActivityOutcome::AlreadyCompleted
    );

    let ready = backend
        .claim_workflow_task(WorkerId::new("timeout-ready"), claim_opts)
        .await
        .unwrap()
        .expect("activity timed-out workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityTimedOut);

    let history = stream_history(&backend, run_id).await;
    assert_eq!(history.len(), 3);
    assert!(matches!(
        history[1].data,
        HistoryEventData::ActivityScheduled(_)
    ));
    let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
        panic!("expected final ActivityTimedOut event");
    };
    assert!(timed_out.message.contains("timed out"));
}

async fn activity_heartbeat_extends_deadline_and_rejects_stale_claim<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, claim_opts, activity_opts) = schedule_heartbeat_activity(
        backend.clone(),
        "wf/activity-heartbeat-extend",
        "heartbeat-extend-workflows",
        "heartbeat-extend-activities",
        durust::RetryPolicy::exponential().max_attempts(1),
    )
    .await;

    let activity = backend
        .claim_activity_task(WorkerId::new("heartbeat-worker-1"), activity_opts)
        .await
        .unwrap()
        .expect("heartbeat attempt");
    assert_eq!(activity.task.attempt, 1);

    let claimed_at = backend.current_time().await.unwrap();
    let early = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(claimed_at.0.saturating_add(100)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(early.timed_out, 0);

    let recorded = backend
        .heartbeat_activity(durust::ActivityHeartbeatRequest {
            claim: activity.claim.clone(),
        })
        .await
        .unwrap();
    assert_eq!(recorded, durust::ActivityHeartbeatOutcome::Recorded);

    let mut stale_claim = activity.claim.clone();
    stale_claim.token = stale_claim.token.saturating_add(1);
    let stale = backend
        .heartbeat_activity(durust::ActivityHeartbeatRequest { claim: stale_claim })
        .await
        .unwrap_err();
    assert!(matches!(stale, Error::StaleLease));

    let heartbeat_at = backend.current_time().await.unwrap();
    let still_early = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(heartbeat_at.0.saturating_add(100)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(still_early.timed_out, 0);

    let final_timeout = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(heartbeat_at.0.saturating_add(500)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(final_timeout.timed_out, 1);

    let ready = backend
        .claim_workflow_task(WorkerId::new("heartbeat-ready"), claim_opts)
        .await
        .unwrap()
        .expect("heartbeat timeout workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityTimedOut);

    let history = stream_history(&backend, run_id).await;
    let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
        panic!("expected final ActivityTimedOut event");
    };
    assert!(timed_out.message.contains("missed heartbeat on attempt 1"));
}

async fn activity_heartbeat_timeout_retries_until_max_attempts_then_wakes_workflow<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, claim_opts, activity_opts) = schedule_heartbeat_activity(
        backend.clone(),
        "wf/activity-heartbeat-retry",
        "heartbeat-retry-workflows",
        "heartbeat-retry-activities",
        durust::RetryPolicy::exponential().max_attempts(2),
    )
    .await;

    let first = backend
        .claim_activity_task(
            WorkerId::new("heartbeat-retry-worker-1"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first heartbeat attempt");
    assert_eq!(first.task.attempt, 1);
    let first_claimed_at = backend.current_time().await.unwrap();
    let retry = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(first_claimed_at.0.saturating_add(500)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(retry.timed_out, 1);
    let not_ready = backend
        .claim_workflow_task(
            WorkerId::new("heartbeat-retry-not-ready"),
            claim_opts.clone(),
        )
        .await
        .unwrap();
    assert!(not_ready.is_none());
    let stale_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim,
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_completion, Error::StaleLease));

    let second = backend
        .claim_activity_task(WorkerId::new("heartbeat-retry-worker-2"), activity_opts)
        .await
        .unwrap()
        .expect("second heartbeat attempt");
    assert_eq!(second.task.attempt, 2);
    let second_claimed_at = backend.current_time().await.unwrap();
    let final_timeout = backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(second_claimed_at.0.saturating_add(500)),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(final_timeout.timed_out, 1);

    let ready = backend
        .claim_workflow_task(WorkerId::new("heartbeat-retry-ready"), claim_opts)
        .await
        .unwrap()
        .expect("heartbeat timeout workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityTimedOut);

    let history = stream_history(&backend, run_id).await;
    assert_eq!(history.len(), 3);
    let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
        panic!("expected final ActivityTimedOut event");
    };
    assert!(timed_out.message.contains("missed heartbeat on attempt 2"));
}

async fn schedule_heartbeat_activity<B>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    activity_queue: &str,
    retry_policy: durust::RetryPolicy,
) -> (
    durust::RunId,
    ClaimWorkflowTaskOptions,
    ClaimActivityOptions,
)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(workflow_id, workflow_queue, input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts(workflow_queue);
    let claimed = backend
        .claim_workflow_task(WorkerId::new("heartbeat-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new(activity_queue),
        retry_policy,
        start_to_close_timeout: Some(Duration::from_secs(10)),
        heartbeat_timeout: Some(Duration::from_millis(200)),
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-heartbeat-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityScheduled(scheduled.clone()),
                )],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(activity_queue),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    (run_id, claim_opts, activity_opts)
}

async fn unexpired_workflow_claim_lease_is_not_reclaimable<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/lease-unexpired", "lease-unexpired-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("lease-unexpired-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("lease-unexpired-holder"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");

    // The holder's lease is nowhere near expiry, so the task must not be
    // handed out to another worker.
    let blocked = backend
        .claim_workflow_task(WorkerId::new("lease-unexpired-thief"), claim_opts.clone())
        .await
        .unwrap();
    assert!(blocked.is_none());

    // Releasing the claim restores normal claimability.
    backend
        .release_workflow_task(
            claimed.claim,
            durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::CacheEvicted),
        )
        .await
        .unwrap();
    let reclaimed = backend
        .claim_workflow_task(WorkerId::new("lease-unexpired-after-release"), claim_opts)
        .await
        .unwrap();
    assert!(reclaimed.is_some());
}

// Claims a workflow task, "crashes" the holder (drops it without commit or
// release), advances time past the lease, and verifies the task is reclaimed
// with the identical state a fresh claim would produce while every operation
// from the dead holder is fenced as stale. `crash` lets SQLite close and
// reopen the database between claim and reclaim; `advance_past_lease` is
// virtual time for the memory provider and a real sleep for SQL providers,
// whose claim scans read their own wall clock.
async fn workflow_lease_expiry_reclaims_and_fences_stale_holder<
    B,
    Crash,
    CrashFut,
    Advance,
    AdvanceFut,
>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    lease_duration: Duration,
    crash: Crash,
    advance_past_lease: Advance,
) where
    B: DurableBackend,
    Crash: FnOnce(B) -> CrashFut,
    CrashFut: Future<Output = B>,
    Advance: FnOnce() -> AdvanceFut,
    AdvanceFut: Future<Output = ()>,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(workflow_id, workflow_queue, input(5))
        .await
        .unwrap();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(workflow_queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration,
    };
    let original = backend
        .claim_workflow_task(WorkerId::new("lease-crash-holder"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");

    let backend = crash(backend).await;
    advance_past_lease().await;

    let reclaimed = backend
        .claim_workflow_task(WorkerId::new("lease-crash-reclaimer"), claim_opts)
        .await
        .unwrap()
        .expect("expired lease should be reclaimable");
    // The reclaim must look exactly like a fresh claim of the same task,
    // under a new fencing token.
    assert_eq!(reclaimed.run_id, run_id);
    assert_eq!(reclaimed.reason, original.reason);
    assert_eq!(
        reclaimed.replay_target_event_id,
        original.replay_target_event_id
    );
    assert_eq!(
        reclaimed
            .prefetched_history
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
        original
            .prefetched_history
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
    );
    assert_ne!(reclaimed.claim.token, original.claim.token);

    // The dead holder is fenced: its commit and release are rejected as stale
    // and leave the new claim untouched.
    let stale_commit = backend
        .commit_workflow_task(
            original.claim.clone(),
            WorkflowTaskCommit {
                expected_tail_event_id: original.replay_target_event_id,
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(stale_commit, Error::StaleLease));
    let stale_release = backend
        .release_workflow_task(
            original.claim,
            durust::WorkflowTaskRelease::immediate(durust::WorkflowTaskReason::CacheEvicted),
        )
        .await
        .unwrap_err();
    assert!(matches!(stale_release, Error::StaleLease));

    let outcome = backend
        .commit_workflow_task(
            reclaimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: reclaimed.replay_target_event_id,
                append_events: vec![NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
                    result: durust::encode_payload(&5_u64).unwrap(),
                })],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(outcome, CommitOutcome::Committed { .. }));
}

async fn schedule_timeoutless_activity<B>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    activity_queue: &str,
) -> (
    durust::RunId,
    ClaimWorkflowTaskOptions,
    ClaimActivityOptions,
)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(workflow_id, workflow_queue, input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts(workflow_queue);
    let claimed = backend
        .claim_workflow_task(WorkerId::new("timeoutless-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input = durust::encode_payload(&Input { value: 9 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: command_id.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new(activity_queue),
        retry_policy: durust::RetryPolicy::exponential().max_attempts(2),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&input),
            "sha256:test-timeoutless-options".to_owned(),
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityScheduled(scheduled.clone()),
                )],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(activity_queue),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    (run_id, claim_opts, activity_opts)
}

// An activity with neither a start-to-close timeout nor a heartbeat timeout
// (the `ActivityOptions` defaults) must be reclaimable after its claim lease
// expires, through the same timeout/retry path explicit deadlines use.
async fn timeoutless_activity_lease_expiry_reclaims_and_fences_stale_holder<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, claim_opts, activity_opts) = schedule_timeoutless_activity(
        backend.clone(),
        "wf/timeoutless-activity-lease",
        "timeoutless-lease-workflows",
        "timeoutless-lease-activities",
    )
    .await;

    let first = backend
        .claim_activity_task(WorkerId::new("timeoutless-worker-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(first.task.attempt, 1);
    let claimed_at = backend.current_time().await.unwrap();

    // While the lease is unexpired the timeout scan leaves the claim alone and
    // the task is not claimable by anyone else.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(claimed_at.0.saturating_add(100)),
            limit: 16,
        })
        .await
        .unwrap();
    let blocked = backend
        .claim_activity_task(
            WorkerId::new("timeoutless-worker-blocked"),
            activity_opts.clone(),
        )
        .await
        .unwrap();
    assert!(blocked.is_none());

    // Past the 30s claim lease the timeout scan reclaims the task through the
    // existing retry machinery, making it claimable as attempt 2.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(claimed_at.0.saturating_add(31_000)),
            limit: 16,
        })
        .await
        .unwrap();
    let second = backend
        .claim_activity_task(WorkerId::new("timeoutless-worker-2"), activity_opts)
        .await
        .unwrap()
        .expect("lease-expired activity should be reclaimable");
    assert_eq!(second.task.attempt, 2);

    // Every operation from the crashed holder is fenced as stale.
    let stale_heartbeat = backend
        .heartbeat_activity(durust::ActivityHeartbeatRequest {
            claim: first.claim.clone(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_heartbeat, Error::StaleLease));
    let stale_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim.clone(),
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_completion, Error::StaleLease));
    let stale_failure = backend
        .fail_activity(FailActivityRequest {
            claim: first.claim,
            failure: durust::DurableFailure::new("conformance.crashed", "stale holder"),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_failure, Error::StaleLease));

    // The new holder completes normally and the workflow wakes up.
    let completed = backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap();
    assert!(matches!(
        completed,
        durust::CompleteActivityOutcome::Completed { .. }
    ));
    let ready = backend
        .claim_workflow_task(WorkerId::new("timeoutless-ready"), claim_opts)
        .await
        .unwrap()
        .expect("activity completion workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityCompleted);
    assert_eq!(ready.run_id, run_id);
}

// A timeout-less activity whose holder heartbeats and then stops must be
// reclaimed exactly one lease after the last heartbeat: the heartbeat re-arms
// the implicit lease-as-heartbeat deadline, so the scan leaves the claim
// alone just short of it and reclaims reliably once it lapses. The terminal
// miss records the lease-flavored attribution.
async fn timeoutless_activity_reclaims_one_lease_after_heartbeats_stop<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, claim_opts, activity_opts) = schedule_timeoutless_activity(
        backend.clone(),
        "wf/timeoutless-heartbeat-stop",
        "timeoutless-hb-stop-workflows",
        "timeoutless-hb-stop-activities",
    )
    .await;
    let lease_ms = i64::try_from(activity_opts.lease_duration.as_millis()).unwrap();

    let first = backend
        .claim_activity_task(
            WorkerId::new("timeoutless-hb-stop-worker-1"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(first.task.attempt, 1);

    let before_heartbeat = backend.current_time().await.unwrap();
    let recorded = backend
        .heartbeat_activity(durust::ActivityHeartbeatRequest {
            claim: first.claim.clone(),
        })
        .await
        .unwrap();
    assert_eq!(recorded, durust::ActivityHeartbeatOutcome::Recorded);
    let after_heartbeat = backend.current_time().await.unwrap();

    // The refreshed deadline sits at least one lease past the pre-heartbeat
    // instant, so a scan just short of that must leave the claim alone.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(before_heartbeat.0.saturating_add(lease_ms - 1)),
            limit: 16,
        })
        .await
        .unwrap();
    let blocked = backend
        .claim_activity_task(
            WorkerId::new("timeoutless-hb-stop-blocked"),
            activity_opts.clone(),
        )
        .await
        .unwrap();
    assert!(
        blocked.is_none(),
        "claim must hold until one lease past the last heartbeat"
    );

    // One lease after the last heartbeat the holder counts as crashed and
    // the scan reclaims the task as attempt 2.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_heartbeat.0.saturating_add(lease_ms + 1)),
            limit: 16,
        })
        .await
        .unwrap();
    let second = backend
        .claim_activity_task(
            WorkerId::new("timeoutless-hb-stop-worker-2"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("stopped heartbeats must reclaim one lease after the last one");
    assert_eq!(second.task.attempt, 2);

    // Every operation from the stopped holder is fenced as stale.
    let stale_heartbeat = backend
        .heartbeat_activity(durust::ActivityHeartbeatRequest {
            claim: first.claim.clone(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_heartbeat, Error::StaleLease));
    let stale_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim.clone(),
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_completion, Error::StaleLease));
    let stale_failure = backend
        .fail_activity(FailActivityRequest {
            claim: first.claim,
            failure: durust::DurableFailure::new("conformance.crashed", "stale holder"),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_failure, Error::StaleLease));

    // Attempt 2 exhausts the retry budget without heartbeating; the terminal
    // miss persists the lease-expiry attribution rather than a
    // missed-heartbeat or start-to-close message.
    let second_claimed_at = backend.current_time().await.unwrap();
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(second_claimed_at.0.saturating_add(lease_ms + 1)),
            limit: 16,
        })
        .await
        .unwrap();
    let ready = backend
        .claim_workflow_task(WorkerId::new("timeoutless-hb-stop-ready"), claim_opts)
        .await
        .unwrap()
        .expect("terminal timeout workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityTimedOut);
    assert_eq!(ready.run_id, run_id);
    let history = stream_history(&backend, run_id).await;
    let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
        panic!("expected ActivityTimedOut event, got {:?}", history[2].data);
    };
    assert!(
        timed_out
            .message
            .contains("claim lease expired without heartbeat on attempt 2"),
        "implicit deadline misses need the lease attribution, got `{}`",
        timed_out.message
    );
}

// The batch claim RPC must stamp the implicit lease-as-heartbeat state
// exactly like the scalar claim: Postgres routes it through a set-based
// unnest update with -1 sentinels while memory/SQLite loop the scalar path.
// A swapped unnest array or a broken sentinel decode either stamps a garbage
// deadline (reclaimed immediately, tripping the blocked assertion) or none at
// all (never reclaimed, tripping the reclaim assertion), so the boundary
// scans pin both sides.
async fn timeoutless_activity_batch_claim_uses_lease_as_implicit_heartbeat<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, claim_opts, activity_opts) = schedule_timeoutless_activity(
        backend.clone(),
        "wf/timeoutless-batch-claim",
        "timeoutless-batch-claim-workflows",
        "timeoutless-batch-claim-activities",
    )
    .await;
    let lease_ms = i64::try_from(activity_opts.lease_duration.as_millis()).unwrap();

    let mut claimed = backend
        .claim_activity_tasks(
            WorkerId::new("timeoutless-batch-worker-1"),
            ClaimActivityTasksOptions {
                claim: activity_opts.clone(),
                limit: 4,
            },
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let first = claimed.remove(0);
    assert_eq!(first.task.attempt, 1);

    let before_heartbeat = backend.current_time().await.unwrap();
    let recorded = backend
        .heartbeat_activity(durust::ActivityHeartbeatRequest {
            claim: first.claim.clone(),
        })
        .await
        .unwrap();
    assert_eq!(recorded, durust::ActivityHeartbeatOutcome::Recorded);
    let after_heartbeat = backend.current_time().await.unwrap();

    // Just short of one lease past the last heartbeat the claim holds: the
    // heartbeat re-armed the batch-stamped implicit deadline.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(before_heartbeat.0.saturating_add(lease_ms - 1)),
            limit: 16,
        })
        .await
        .unwrap();
    let blocked = backend
        .claim_activity_tasks(
            WorkerId::new("timeoutless-batch-blocked"),
            ClaimActivityTasksOptions {
                claim: activity_opts.clone(),
                limit: 4,
            },
        )
        .await
        .unwrap();
    assert!(
        blocked.is_empty(),
        "batch-claimed implicit deadline must hold until one lease past the last heartbeat"
    );

    // One lease after the last heartbeat the task reclaims as attempt 2,
    // again through the batch claim RPC; the old holder is fenced.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_heartbeat.0.saturating_add(lease_ms + 1)),
            limit: 16,
        })
        .await
        .unwrap();
    let before_second_claim = backend.current_time().await.unwrap();
    let mut reclaimed = backend
        .claim_activity_tasks(
            WorkerId::new("timeoutless-batch-worker-2"),
            ClaimActivityTasksOptions {
                claim: activity_opts.clone(),
                limit: 4,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        reclaimed.len(),
        1,
        "batch-claimed timeout-less task must reclaim one lease after the last heartbeat"
    );
    let second = reclaimed.remove(0);
    assert_eq!(second.task.attempt, 2);
    let after_second_claim = backend.current_time().await.unwrap();
    let stale_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim,
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap_err();
    assert!(matches!(stale_completion, Error::StaleLease));

    // Attempt 2 never heartbeats, so the deadline observed below is exactly
    // what the batch claim stamped: it must hold just short of one lease and
    // lapse just past it. A batch claim that stamps no deadline (or a garbage
    // one) fails one of these two boundaries. The lapse exhausts the retry
    // budget and records the lease attribution.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(before_second_claim.0.saturating_add(lease_ms - 1)),
            limit: 16,
        })
        .await
        .unwrap();
    let blocked = backend
        .claim_activity_tasks(
            WorkerId::new("timeoutless-batch-blocked-2"),
            ClaimActivityTasksOptions {
                claim: activity_opts,
                limit: 4,
            },
        )
        .await
        .unwrap();
    assert!(
        blocked.is_empty(),
        "batch claim must stamp the implicit deadline one full lease ahead"
    );
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_second_claim.0.saturating_add(lease_ms + 1)),
            limit: 16,
        })
        .await
        .unwrap();
    let ready = backend
        .claim_workflow_task(WorkerId::new("timeoutless-batch-ready"), claim_opts)
        .await
        .unwrap()
        .expect("terminal timeout workflow task (batch claim must stamp a deadline)");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityTimedOut);
    assert_eq!(ready.run_id, run_id);
    let history = stream_history(&backend, run_id).await;
    let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
        panic!("expected ActivityTimedOut event, got {:?}", history[2].data);
    };
    assert!(
        timed_out
            .message
            .contains("claim lease expired without heartbeat on attempt 2"),
        "batch-claimed implicit misses need the lease attribution, got `{}`",
        timed_out.message
    );
}

// An explicit heartbeat timeout stays authoritative over the claim lease: an
// activity with a 200ms heartbeat timeout under a 30s lease is reclaimed on
// the 200ms cadence when heartbeats stop, and the miss is attributed to the
// heartbeat, not the lease.
async fn explicit_heartbeat_timeout_takes_precedence_over_claim_lease<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, claim_opts, activity_opts) = schedule_heartbeat_activity(
        backend.clone(),
        "wf/explicit-heartbeat-precedence",
        "explicit-hb-precedence-workflows",
        "explicit-hb-precedence-activities",
        durust::RetryPolicy::exponential().max_attempts(1),
    )
    .await;
    assert_eq!(activity_opts.lease_duration, Duration::from_secs(30));

    let first = backend
        .claim_activity_task(
            WorkerId::new("explicit-hb-precedence-worker"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("heartbeat activity");
    assert_eq!(first.task.attempt, 1);

    let before_heartbeat = backend.current_time().await.unwrap();
    let recorded = backend
        .heartbeat_activity(durust::ActivityHeartbeatRequest {
            claim: first.claim.clone(),
        })
        .await
        .unwrap();
    assert_eq!(recorded, durust::ActivityHeartbeatOutcome::Recorded);
    let after_heartbeat = backend.current_time().await.unwrap();

    // Just short of the 200ms explicit cadence the claim holds.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(before_heartbeat.0.saturating_add(199)),
            limit: 16,
        })
        .await
        .unwrap();
    let blocked = backend
        .claim_activity_task(
            WorkerId::new("explicit-hb-precedence-blocked"),
            activity_opts.clone(),
        )
        .await
        .unwrap();
    assert!(blocked.is_none());

    // 201ms after the last heartbeat — far inside the 30s lease — the
    // explicit cadence reclaims the task; the exhausted retry budget makes
    // the miss terminal with the missed-heartbeat attribution.
    backend
        .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(after_heartbeat.0.saturating_add(201)),
            limit: 16,
        })
        .await
        .unwrap();
    let ready = backend
        .claim_workflow_task(WorkerId::new("explicit-hb-precedence-ready"), claim_opts)
        .await
        .unwrap()
        .expect("heartbeat timeout workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityTimedOut);
    assert_eq!(ready.run_id, run_id);
    let history = stream_history(&backend, run_id).await;
    let HistoryEventData::ActivityTimedOut(timed_out) = &history[2].data else {
        panic!("expected ActivityTimedOut event, got {:?}", history[2].data);
    };
    assert!(
        timed_out.message.contains("missed heartbeat on attempt 1"),
        "explicit heartbeat misses keep the heartbeat attribution, got `{}`",
        timed_out.message
    );
}

// A timeout-less activity whose holder heartbeats faithfully must survive
// past the original claim lease indefinitely: each heartbeat re-arms the
// implicit lease-as-heartbeat deadline. `pass_partial_lease` moves provider
// time a large fraction of the lease per cycle (virtual time for memory,
// wall-clock waiting for the SQL providers), so three cycles put provider
// time well past the deadline stamped at claim.
async fn heartbeating_timeoutless_activity_survives_lease_periods<B, F, Fut>(
    backend: B,
    workflow_id: &str,
    workflow_queue: &str,
    activity_queue: &str,
    lease: Duration,
    pass_partial_lease: F,
) where
    B: DurableBackend,
    F: Fn() -> Fut,
    Fut: Future<Output = ()>,
{
    let (run_id, claim_opts, mut activity_opts) =
        schedule_timeoutless_activity(backend.clone(), workflow_id, workflow_queue, activity_queue)
            .await;
    activity_opts.lease_duration = lease;
    let lease_ms = i64::try_from(lease.as_millis()).unwrap();

    let claimed = backend
        .claim_activity_task(WorkerId::new("hb-timeoutless-worker"), activity_opts)
        .await
        .unwrap()
        .expect("first attempt");
    assert_eq!(claimed.task.attempt, 1);
    let claimed_at = backend.current_time().await.unwrap();

    for _ in 0..3 {
        pass_partial_lease().await;
        // The scan runs before this cycle's heartbeat, so it observes the
        // deadline as re-armed by the previous heartbeat only.
        let now = backend.current_time().await.unwrap();
        backend
            .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
                namespace: Namespace::default(),
                now,
                limit: 16,
            })
            .await
            .unwrap();
        let recorded = backend
            .heartbeat_activity(durust::ActivityHeartbeatRequest {
                claim: claimed.claim.clone(),
            })
            .await
            .expect("heartbeating holder must never be reclaimed");
        assert_eq!(recorded, durust::ActivityHeartbeatOutcome::Recorded);
    }
    let now = backend.current_time().await.unwrap();
    assert!(
        now.0 > claimed_at.0.saturating_add(lease_ms),
        "test must outlive the deadline stamped at claim time"
    );

    // The original holder completes normally: exactly one attempt ran and no
    // timeout was recorded.
    let completed = backend
        .complete_activity(CompleteActivityRequest {
            claim: claimed.claim,
            result: durust::encode_payload(&9_u64).unwrap(),
        })
        .await
        .unwrap();
    assert!(matches!(
        completed,
        durust::CompleteActivityOutcome::Completed { .. }
    ));
    let ready = backend
        .claim_workflow_task(WorkerId::new("hb-timeoutless-ready"), claim_opts)
        .await
        .unwrap()
        .expect("activity completion workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityCompleted);
    assert_eq!(ready.run_id, run_id);
    let history = stream_history(&backend, run_id).await;
    assert!(
        history
            .iter()
            .all(|event| !matches!(event.data, HistoryEventData::ActivityTimedOut(_))),
        "no timeout may fire for a heartbeating holder: {history:?}"
    );
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(event.data, HistoryEventData::ActivityCompleted(_)))
            .count(),
        1,
        "exactly one attempt completes"
    );
}

#[test]
fn memory_heartbeating_timeoutless_activity_survives_lease_periods_on_virtual_clock() {
    block_on(async {
        let backend = MemoryBackend::new();
        let advance_backend = backend.clone();
        heartbeating_timeoutless_activity_survives_lease_periods(
            backend,
            "wf/memory-hb-timeoutless",
            "memory-hb-timeoutless-workflows",
            "memory-hb-timeoutless-activities",
            Duration::from_secs(30),
            move || {
                let advance_backend = advance_backend.clone();
                async move {
                    advance_backend.advance_time(Duration::from_secs(20));
                }
            },
        )
        .await;
    });
}

#[test]
fn sqlite_heartbeating_timeoutless_activity_survives_lease_periods() {
    block_on_tokio(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("hb-timeoutless.sqlite3")).unwrap();
        heartbeating_timeoutless_activity_survives_lease_periods(
            backend,
            "wf/sqlite-hb-timeoutless",
            "sqlite-hb-timeoutless-workflows",
            "sqlite-hb-timeoutless-activities",
            Duration::from_millis(500),
            || async { tokio::time::sleep(Duration::from_millis(200)).await },
        )
        .await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_heartbeating_timeoutless_activity_survives_lease_periods_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres heartbeat lease conformance",
        "hb_timeoutless",
        |backend| async move {
            heartbeating_timeoutless_activity_survives_lease_periods(
                backend,
                "wf/postgres-hb-timeoutless",
                "postgres-hb-timeoutless-workflows",
                "postgres-hb-timeoutless-activities",
                Duration::from_millis(500),
                || async { tokio::time::sleep(Duration::from_millis(200)).await },
            )
            .await;
        },
    ));
}

async fn cancel_commands_clear_activity_tasks<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/cancel-command", "cancel-command-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("cancel-command-workflows");
    let activity_command = durust::command_id(&run_id, 1);
    let timer_command = durust::command_id(&run_id, 2);
    let activity_input = durust::encode_payload(&Input { value: 5 }).unwrap();
    let scheduled = durust::ActivityScheduled {
        command_id: activity_command.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("activities"),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: activity_input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&activity_input),
            "sha256:cancel-command-options".to_owned(),
        ),
    };
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("cancel-command-scheduler"),
            claim_opts.clone(),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![
                    durust::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                        scheduled.clone(),
                    )),
                    durust::NewHistoryEvent::new(HistoryEventData::TimerStarted(
                        durust::TimerStarted {
                            command_id: timer_command.clone(),
                            fire_at: durust::TimestampMs(10),
                            fingerprint: durust::timer_fingerprint(
                                "sleep",
                                durust::TimestampMs(10),
                            ),
                        },
                    )),
                ],
                upsert_waits: vec![durust::WaitRecord {
                    wait_id: durust::WaitId::new(format!(
                        "{}:{}:timer",
                        timer_command.run_id, timer_command.seq.0
                    )),
                    run_id: timer_command.run_id.clone(),
                    command_id: timer_command.clone(),
                    kind: durust::WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(durust::TimestampMs(10)),
                }],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(&scheduled)],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(outcome, CommitOutcome::Committed { .. }));

    let claimed_activity = backend
        .claim_activity_task(
            WorkerId::new("cancel-command-activity"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("activities"),
                registered_activity_names: vec![ActivityName::new("conformance.echo")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("activity task");
    let fired = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(10),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(fired.fired, 1);

    let claimed = backend
        .claim_workflow_task(WorkerId::new("cancel-command-selector"), claim_opts)
        .await
        .unwrap()
        .expect("timer-ready workflow task");
    assert_eq!(claimed.replay_target_event_id, EventId(4));
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(4),
                cancel_commands: vec![activity_command],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(outcome, CommitOutcome::Committed { .. }));

    let late_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: claimed_activity.claim,
            result: durust::encode_payload(&5_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        late_completion,
        durust::CompleteActivityOutcome::AlreadyCompleted
    );
}

async fn child_start_dispatch_is_idempotent_and_wakes_parent<B>(backend: B)
where
    B: DurableBackend,
{
    let (parent_run_id, command_id) = schedule_child_start(
        backend.clone(),
        "wf/child-dispatch-parent",
        "wf/child-dispatch-child",
        durust::ParentClosePolicy::Cancel,
    )
    .await;

    let dispatched = backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();
    assert!(
        dispatched.dispatched <= 1,
        "providers may dispatch one queued child start or inline it during commit"
    );
    let duplicate = backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(duplicate.dispatched, 0);

    let parent_ready = backend
        .claim_workflow_task(
            WorkerId::new("child-start-parent-ready"),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent woken by child start");
    assert_eq!(
        parent_ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowStarted
    );

    let child_ready = backend
        .claim_workflow_task(
            WorkerId::new("child-start-child-ready"),
            workflow_claim_opts("child-workflows"),
        )
        .await
        .unwrap()
        .expect("child workflow started");
    assert_eq!(
        child_ready.workflow_id,
        durust::WorkflowId::new("wf/child-dispatch-child")
    );

    let history = stream_history(&backend, parent_run_id).await;
    assert!(history.iter().any(|event| matches!(
        &event.data,
        HistoryEventData::ChildWorkflowStarted(started)
            if started.command_id == command_id
    )));
}

async fn child_completion_routes_to_parent<B>(backend: B)
where
    B: DurableBackend,
{
    let (parent_run_id, command_id) = schedule_child_start(
        backend.clone(),
        "wf/child-completion-parent",
        "wf/child-completion-child",
        durust::ParentClosePolicy::Cancel,
    )
    .await;
    backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();
    let child = backend
        .claim_workflow_task(
            WorkerId::new("child-completion-worker"),
            workflow_claim_opts("child-workflows"),
        )
        .await
        .unwrap()
        .expect("child workflow task");
    let result = durust::encode_payload(&99_u64).unwrap();
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCompleted {
                        result: result.clone(),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    let parent_ready = backend
        .claim_workflow_task(
            WorkerId::new("child-completion-parent-ready"),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent woken by child completion");
    assert_eq!(
        parent_ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowCompleted
    );
    let history = stream_history(&backend, parent_run_id).await;
    let completed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowCompleted(completed)
                if completed.command_id == command_id =>
            {
                Some(completed)
            }
            _ => None,
        })
        .expect("child completion event");
    assert_eq!(
        durust::decode_payload::<u64>(&completed.result).unwrap(),
        99
    );
}

async fn child_start_conflict_records_failure<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>(
            "wf/child-conflict-child",
            "conflict-child-workflows",
            input(1),
        )
        .await
        .unwrap();
    let (parent_run_id, _command_id) = schedule_child_start(
        backend.clone(),
        "wf/child-conflict-parent",
        "wf/child-conflict-child",
        durust::ParentClosePolicy::Cancel,
    )
    .await;
    backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();

    let history = stream_history(&backend, parent_run_id).await;
    let failed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowFailed(failed) => Some(failed),
            _ => None,
        })
        .expect("child start conflict failure");
    assert_eq!(
        failed.failure.error_type,
        "durust.child_workflow_id_conflict"
    );
    assert!(failed.failure.non_retryable);
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("child-conflict-parent-ready"),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap();
    assert!(claimed.is_some());
}

async fn parent_close_policy_cancel_cancels_child<B>(backend: B)
where
    B: DurableBackend,
{
    let (parent_run_id, _command_id) = schedule_child_start(
        backend.clone(),
        "wf/child-cancel-parent",
        "wf/child-cancel-child",
        durust::ParentClosePolicy::Cancel,
    )
    .await;
    backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();
    let parent = backend
        .claim_workflow_task(
            WorkerId::new("child-cancel-parent-ready"),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent ready after child start");
    backend
        .commit_workflow_task(
            parent.claim,
            terminal_parent_commit(parent.replay_target_event_id),
        )
        .await
        .unwrap();

    let child_claim = backend
        .claim_workflow_task(
            WorkerId::new("child-cancel-claim"),
            workflow_claim_opts("child-workflows"),
        )
        .await
        .unwrap();
    assert!(child_claim.is_none());

    let parent_history = stream_history(&backend, parent_run_id).await;
    let child_run_id = parent_history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowStarted(started) => Some(started.run_id.clone()),
            _ => None,
        })
        .expect("child started");
    let child_history = stream_history(&backend, child_run_id).await;
    assert!(
        child_history
            .iter()
            .any(|event| matches!(event.data, HistoryEventData::WorkflowCancelled { .. }))
    );
}

async fn parent_close_policy_abandon_leaves_child_running<B>(backend: B)
where
    B: DurableBackend,
{
    schedule_child_start(
        backend.clone(),
        "wf/child-abandon-parent",
        "wf/child-abandon-child",
        durust::ParentClosePolicy::Abandon,
    )
    .await;
    backend
        .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
            namespace: Namespace::default(),
            limit: 16,
        })
        .await
        .unwrap();
    let parent = backend
        .claim_workflow_task(
            WorkerId::new("child-abandon-parent-ready"),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent ready after child start");
    backend
        .commit_workflow_task(
            parent.claim,
            terminal_parent_commit(parent.replay_target_event_id),
        )
        .await
        .unwrap();

    let child_claim = backend
        .claim_workflow_task(
            WorkerId::new("child-abandon-claim"),
            workflow_claim_opts("child-workflows"),
        )
        .await
        .unwrap();
    assert!(child_claim.is_some());
}

async fn schedule_child_start<B>(
    backend: B,
    parent_workflow_id: &str,
    child_workflow_id: &str,
    parent_close_policy: durust::ParentClosePolicy,
) -> (durust::RunId, durust::CommandId)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let parent_run_id = client
        .start_workflow::<workflow>(parent_workflow_id, "child-parent-workflows", input(1))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{parent_workflow_id}-scheduler")),
            workflow_claim_opts("child-parent-workflows"),
        )
        .await
        .unwrap()
        .expect("parent workflow task");
    let command_id = durust::command_id(&parent_run_id, 1);
    let input = durust::encode_payload(&7_u64).unwrap();
    let workflow_type = durust::WorkflowType::new("conformance.workflow", 1);
    let workflow_id = durust::WorkflowId::new(child_workflow_id);
    let task_queue = TaskQueue::new("child-workflows");
    let requested = durust::ChildWorkflowStartRequested {
        command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        workflow_id: workflow_id.clone(),
        task_queue: task_queue.clone(),
        input: input.clone(),
        parent_close_policy,
        fingerprint: durust::child_workflow_fingerprint(
            workflow_type,
            workflow_id,
            durust::payload_digest(&input),
            task_queue,
            parent_close_policy,
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ChildWorkflowStartRequested(requested.clone()),
                )],
                start_child_workflows: vec![durust::ChildStartOutboxMessage::from_requested(
                    &requested,
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    (parent_run_id, command_id)
}

async fn schedule_child_workflow_map<B>(
    backend: B,
    parent_workflow_id: &str,
    parent_queue: &str,
    child_queue: &str,
    workflow_id_prefix: &str,
    failure_mode: durust::ChildWorkflowMapFailureMode,
    parent_close_policy: durust::ParentClosePolicy,
    max_in_flight: usize,
) -> (
    durust::RunId,
    durust::CommandId,
    ClaimWorkflowTaskOptions,
    ClaimWorkflowTaskOptions,
    durust::Result<CommitOutcome>,
)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let parent_run_id = client
        .start_workflow::<workflow>(parent_workflow_id, parent_queue, input(1))
        .await
        .unwrap();
    let parent_opts = workflow_claim_opts(parent_queue);
    let child_opts = workflow_claim_opts(child_queue);
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{parent_workflow_id}-child-map-scheduler")),
            parent_opts.clone(),
        )
        .await
        .unwrap()
        .expect("parent workflow task");
    let command_id = durust::command_id(&parent_run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        [1_u64, 2, 3]
            .into_iter()
            .map(|value| durust::encode_payload(&value).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let task_queue = TaskQueue::new(child_queue);
    let result_manifest_name = "child-map-results".to_owned();
    let map_task = ChildWorkflowMapTask {
        map_command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        input_manifest: input_manifest.clone(),
        result_manifest_name: result_manifest_name.clone(),
        workflow_id_prefix: workflow_id_prefix.to_owned(),
        max_in_flight,
        parent_close_policy,
        failure_mode,
    };
    let scheduled = durust::ChildWorkflowMapScheduled {
        command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        input_manifest: input_manifest.clone(),
        result_manifest_name: result_manifest_name.clone(),
        workflow_id_prefix: workflow_id_prefix.to_owned(),
        max_in_flight,
        parent_close_policy,
        failure_mode,
        fingerprint: durust::child_workflow_map_fingerprint(
            workflow_type,
            durust::payload_digest(&input_manifest),
            result_manifest_name,
            workflow_id_prefix.to_owned(),
            max_in_flight,
            task_queue,
            parent_close_policy,
            failure_mode,
        ),
    };
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ChildWorkflowMapScheduled(scheduled),
                )],
                schedule_child_workflow_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await;
    (parent_run_id, command_id, parent_opts, child_opts, outcome)
}

async fn dispatch_child_map_starts<B>(backend: &B)
where
    B: DurableBackend,
{
    for _ in 0..4 {
        let dispatched = backend
            .dispatch_child_workflow_starts(durust::DispatchChildWorkflowStartsRequest {
                namespace: Namespace::default(),
                limit: 16,
            })
            .await
            .unwrap();
        if dispatched.dispatched == 0 {
            return;
        }
    }
}

async fn complete_child_run<B>(backend: &B, child: durust::ClaimedWorkflowTask, value: u64)
where
    B: DurableBackend,
{
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: child.replay_target_event_id,
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCompleted {
                        result: durust::encode_payload(&value).unwrap(),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
}

async fn fail_child_run<B>(
    backend: &B,
    child: durust::ClaimedWorkflowTask,
    error_type: &str,
    message: &str,
) where
    B: DurableBackend,
{
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: child.replay_target_event_id,
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowFailed {
                        failure: durust::DurableFailure::new(error_type, message),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
}

async fn cancel_child_run<B>(backend: &B, child: durust::ClaimedWorkflowTask, reason: &str)
where
    B: DurableBackend,
{
    backend
        .commit_workflow_task(
            child.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: child.replay_target_event_id,
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCancelled {
                        reason: reason.to_owned(),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
}

/// The two strings a fail-fast child workflow map writes into durable history:
/// the parent's `ChildWorkflowMapFailed.failure.message` when the item that
/// stopped the map was *cancelled*, and the `reason` on every sibling child's
/// `WorkflowCancelled`.
///
/// Both were byte-for-byte different across the providers before row 6B, and
/// nothing else in either language's suite asserted them, so "conformance
/// passes unchanged" was vacuous for exactly the two behaviours Phase 6's
/// shared map engine converges (Decisions D3/D4). The per-provider tables were
/// added first so the convergence would show up as a diff here instead of as a
/// silent change to persisted history. **This is that diff.**
///
/// One table now, because all three providers read these strings out of
/// `map_engine::fail_fast_failure` and `map_engine::child_cancellation_reason`
/// instead of formatting their own. Both converged forms are the SQL
/// providers': the message names the item ordinal, and the cancellation reason
/// qualifies the command with its run so it is unambiguous across runs. The
/// in-memory provider's bare child reason and seq-only cancellation reason are
/// gone.
struct FailFastHistoryStrings {
    /// `(item ordinal, child cancellation reason) -> parent-visible message`.
    cancelled_item_message: fn(u64, &str) -> String,
    /// `map command id -> sibling WorkflowCancelled.reason`.
    sibling_cancellation_reason: fn(&durust::CommandId) -> String,
}

/// The converged forms. Produced once by `src/map_engine.rs` and pinned there
/// byte-for-byte by `parent_visible_failure_strings_are_pinned`; asserted here
/// against the durable history all three providers actually write.
const FAIL_FAST_HISTORY_STRINGS: FailFastHistoryStrings = FailFastHistoryStrings {
    cancelled_item_message: |ordinal, reason| {
        format!("child workflow map item {ordinal} was cancelled: {reason}")
    },
    sibling_cancellation_reason: |command_id| {
        format!(
            "child workflow map `{}`:{} failed",
            command_id.run_id, command_id.seq.0
        )
    },
};

async fn child_workflow_map_fail_fast_history_strings<B>(
    backend: B,
    expected: &FailFastHistoryStrings,
) where
    B: DurableBackend,
{
    let workflow_id_prefix = "wf/child-map-fail-fast-strings/item";
    let (run_id, command_id, parent_opts, child_opts, scheduled) = schedule_child_workflow_map(
        backend.clone(),
        "wf/child-map-fail-fast-strings",
        "child-map-fail-fast-strings-parent",
        "child-map-fail-fast-strings-children",
        workflow_id_prefix,
        durust::ChildWorkflowMapFailureMode::FailFast,
        durust::ParentClosePolicy::Cancel,
        2,
    )
    .await;
    scheduled.expect("scheduling a valid child map commits");

    dispatch_child_map_starts(&backend).await;
    let first = backend
        .claim_workflow_task(
            WorkerId::new("child-map-fail-fast-strings-0"),
            child_opts.clone(),
        )
        .await
        .unwrap()
        .expect("first child map item");
    let second = backend
        .claim_workflow_task(WorkerId::new("child-map-fail-fast-strings-1"), child_opts)
        .await
        .unwrap()
        .expect("second child map item");
    let cancelled_ordinal: u64 = first
        .workflow_id
        .0
        .strip_prefix(&format!("{workflow_id_prefix}/"))
        .expect("map child ids are `{prefix}/{ordinal}`")
        .parse()
        .expect("map child ordinal is numeric");
    let sibling_run_id = second.run_id.clone();

    // A *cancelled* item, not a failed one: the cancelled arm is the only
    // place a provider synthesizes the parent-visible failure itself.
    cancel_child_run(&backend, first, "child stopped").await;

    let ready = backend
        .claim_workflow_task(
            WorkerId::new("child-map-fail-fast-strings-parent"),
            parent_opts,
        )
        .await
        .unwrap()
        .expect("parent ready after the map failed fast");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowMapFailed
    );

    let history = stream_history(&backend, run_id).await;
    let failed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowMapFailed(failed) => Some(failed),
            _ => None,
        })
        .expect("child workflow map failed event");
    assert_eq!(failed.failure.error_type, "durust.child_workflow_cancelled");
    assert!(failed.failure.non_retryable);
    assert_eq!(
        failed.failure.message,
        (expected.cancelled_item_message)(cancelled_ordinal, "child stopped"),
    );

    let sibling_history = stream_history(&backend, sibling_run_id).await;
    let sibling_reason = sibling_history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::WorkflowCancelled { reason } => Some(reason.clone()),
            _ => None,
        })
        .expect("sibling child cancelled by the fail-fast map");
    assert_eq!(
        sibling_reason,
        (expected.sibling_cancellation_reason)(&command_id),
    );
}

#[test]
fn memory_child_workflow_map_fail_fast_history_strings_are_pinned() {
    block_on(child_workflow_map_fail_fast_history_strings(
        MemoryBackend::new(),
        &FAIL_FAST_HISTORY_STRINGS,
    ));
}

#[test]
fn sqlite_child_workflow_map_fail_fast_history_strings_are_pinned() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("fail-fast-strings.sqlite3")).unwrap();
        child_workflow_map_fail_fast_history_strings(backend, &FAIL_FAST_HISTORY_STRINGS).await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_child_workflow_map_fail_fast_history_strings_are_pinned_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres fail-fast history strings",
        "failfaststrings",
        |backend| async move {
            child_workflow_map_fail_fast_history_strings(backend, &FAIL_FAST_HISTORY_STRINGS).await;
        },
    ));
}

fn assert_compact_child_workflow_map_parent_history(history: &[durust::HistoryEvent]) {
    assert!(
        history
            .iter()
            .any(|event| matches!(event.data, HistoryEventData::ChildWorkflowMapScheduled(_))),
        "expected child workflow map scheduled event"
    );
    assert!(
        !history.iter().any(|event| matches!(
            event.data,
            HistoryEventData::ChildWorkflowStarted(_)
                | HistoryEventData::ChildWorkflowCompleted(_)
                | HistoryEventData::ChildWorkflowFailed(_)
                | HistoryEventData::ChildWorkflowCancelled(_)
        )),
        "child workflow map parent history must stay compact"
    );
}

fn workflow_claim_opts(task_queue: &str) -> ClaimWorkflowTaskOptions {
    ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new(task_queue),
        registered_workflow_types: vec![WorkflowType::new("conformance.workflow", 1)],
        lease_duration: Duration::from_secs(30),
    }
}

fn terminal_parent_commit(expected_tail_event_id: EventId) -> WorkflowTaskCommit {
    WorkflowTaskCommit {
        expected_tail_event_id,
        append_events: vec![durust::NewHistoryEvent::new(
            HistoryEventData::WorkflowCompleted {
                result: durust::encode_payload(&()).unwrap(),
            },
        )],
        ..WorkflowTaskCommit::default()
    }
}

async fn stream_history<B>(backend: &B, run_id: durust::RunId) -> Vec<durust::HistoryEvent>
where
    B: DurableBackend,
{
    backend
        .stream_history(durust::StreamHistoryRequest {
            run_id,
            after_event_id: EventId::ZERO,
            up_to_event_id: EventId(100),
            max_events: 100,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .events
}

async fn activity_map_materializes_bounded_items_and_writes_result_manifest<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/activity-map", "map-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("map-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("map-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        [1_u64, 2, 3]
            .into_iter()
            .map(|value| durust::encode_payload(&Input { value }).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let decoded_input_manifest: ActivityMapInputManifest =
        durust::decode_payload(&input_manifest).unwrap();
    assert_eq!(decoded_input_manifest.item_count, 3);
    assert_eq!(decoded_input_manifest.page_lengths, vec![2, 1]);
    assert_eq!(decoded_input_manifest.pages.len(), 2);
    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new("map-activities");
    let retry_policy = durust::RetryPolicy::none();
    let map_task = ActivityMapTask {
        map_command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "mapped".to_owned(),
        max_in_flight: 2,
    };
    let fingerprint = durust::activity_map_fingerprint(
        activity_name.clone(),
        durust::payload_digest(&input_manifest),
        "mapped".to_owned(),
        2,
        "sha256:test-options".to_owned(),
    );
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityMapScheduled(durust::ActivityMapScheduled {
                        command_id: command_id.clone(),
                        activity_name,
                        task_queue,
                        retry_policy,
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                        input_manifest: input_manifest.clone(),
                        result_manifest_name: "mapped".to_owned(),
                        max_in_flight: 2,
                        fingerprint,
                    }),
                )],
                schedule_activity_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("map-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(WorkerId::new("mapper-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first map item");
    let second = backend
        .claim_activity_task(WorkerId::new("mapper-2"), activity_opts.clone())
        .await
        .unwrap()
        .expect("second map item");
    let hidden_by_max_in_flight = backend
        .claim_activity_task(WorkerId::new("mapper-3"), activity_opts.clone())
        .await
        .unwrap();
    assert!(hidden_by_max_in_flight.is_none());

    assert_map_item(&first.task, 0, 1);
    assert_map_item(&second.task, 1, 2);
    let non_terminal = backend
        .complete_activity(CompleteActivityRequest {
            claim: first.claim.clone(),
            result: durust::encode_payload(&10_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        non_terminal,
        durust::CompleteActivityOutcome::Completed {
            event_id: EventId(2)
        }
    );

    let third = backend
        .claim_activity_task(WorkerId::new("mapper-3"), activity_opts.clone())
        .await
        .unwrap()
        .expect("third map item after one completion");
    assert_map_item(&third.task, 2, 3);

    backend
        .complete_activity(CompleteActivityRequest {
            claim: third.claim.clone(),
            result: durust::encode_payload(&30_u64).unwrap(),
        })
        .await
        .unwrap();
    let final_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim.clone(),
            result: durust::encode_payload(&20_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        final_completion,
        durust::CompleteActivityOutcome::Completed {
            event_id: EventId(3)
        }
    );
    let duplicate = backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&20_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(duplicate, durust::CompleteActivityOutcome::AlreadyCompleted);

    let no_leftover_items = backend
        .claim_activity_task(WorkerId::new("mapper-leftover"), activity_opts)
        .await
        .unwrap();
    assert!(no_leftover_items.is_none());

    let ready = backend
        .claim_workflow_task(WorkerId::new("map-ready"), claim_opts)
        .await
        .unwrap()
        .expect("map-completed workflow task");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ActivityMapCompleted
    );

    let history = stream_history(&backend, run_id).await;
    assert_eq!(history.len(), 3);
    assert!(matches!(
        history[1].data,
        HistoryEventData::ActivityMapScheduled(_)
    ));
    let HistoryEventData::ActivityMapCompleted(completed) = &history[2].data else {
        panic!("expected compact ActivityMapCompleted event");
    };
    assert_eq!(completed.item_count, 3);
    assert_eq!(completed.success_count, 3);
    assert_eq!(completed.failure_count, 0);
    let manifest: ActivityMapResultManifest =
        durust::decode_payload(&completed.result_manifest).unwrap();
    assert_eq!(manifest.name, "mapped");
    assert_eq!(manifest.item_count, 3);
    assert_eq!(manifest.page_lengths, vec![2, 1]);
    assert_eq!(manifest.pages.len(), 2);
    let result_refs = durust::decode_activity_map_result_refs(&completed.result_manifest).unwrap();
    let values = result_refs
        .iter()
        .map(|payload| durust::decode_payload::<u64>(payload).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values, vec![10, 20, 30]);
}

async fn activity_map_failure_suppresses_remaining_items_and_wakes_workflow<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/activity-map-failure", "map-failure-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("map-failure-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("map-failure-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        [1_u64, 2, 3]
            .into_iter()
            .map(|value| durust::encode_payload(&Input { value }).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new("map-failure-activities");
    // No backoff: the map-failure suite reclaims the retried item
    // immediately; backoff pacing has its own conformance tests.
    let retry_policy = durust::RetryPolicy::none().max_attempts(2);
    let map_task = ActivityMapTask {
        map_command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "mapped".to_owned(),
        max_in_flight: 2,
    };
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityMapScheduled(durust::ActivityMapScheduled {
                        command_id: command_id.clone(),
                        activity_name,
                        task_queue,
                        retry_policy,
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                        input_manifest: input_manifest.clone(),
                        result_manifest_name: "mapped".to_owned(),
                        max_in_flight: 2,
                        fingerprint: durust::activity_map_fingerprint(
                            ActivityName::new("conformance.echo"),
                            durust::payload_digest(&input_manifest),
                            "mapped".to_owned(),
                            2,
                            "sha256:test-options".to_owned(),
                        ),
                    }),
                )],
                schedule_activity_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        }
    );

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("map-failure-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let first = backend
        .claim_activity_task(WorkerId::new("failing-mapper-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first map item");
    let second = backend
        .claim_activity_task(WorkerId::new("failing-mapper-2"), activity_opts.clone())
        .await
        .unwrap()
        .expect("second map item");

    let retried = backend
        .fail_activity(FailActivityRequest {
            claim: first.claim,
            failure: durust::DurableFailure::new(
                "test.map_transient",
                "transient map item failure",
            ),
        })
        .await
        .unwrap();
    assert_eq!(
        retried,
        durust::FailActivityOutcome::RetryScheduled { next_attempt: 2 }
    );
    let not_ready = backend
        .claim_workflow_task(WorkerId::new("map-retry-not-ready"), claim_opts.clone())
        .await
        .unwrap();
    assert!(not_ready.is_none());

    let retry = backend
        .claim_activity_task(WorkerId::new("failing-mapper-retry"), activity_opts.clone())
        .await
        .unwrap()
        .expect("retried map item");
    assert_map_item(&retry.task, 0, 1);
    assert_eq!(retry.task.attempt, 2);
    let failed = backend
        .fail_activity(FailActivityRequest {
            claim: retry.claim,
            failure: durust::DurableFailure::new("test.map_failed", "map item failed"),
        })
        .await
        .unwrap();
    assert_eq!(
        failed,
        durust::FailActivityOutcome::Failed {
            event_id: EventId(3)
        }
    );

    let stale_in_flight_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: second.claim,
            result: durust::encode_payload(&20_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        stale_in_flight_completion,
        durust::CompleteActivityOutcome::AlreadyCompleted
    );
    let no_leftover_items = backend
        .claim_activity_task(WorkerId::new("failing-mapper-leftover"), activity_opts)
        .await
        .unwrap();
    assert!(no_leftover_items.is_none());

    let ready = backend
        .claim_workflow_task(WorkerId::new("map-failed-ready"), claim_opts)
        .await
        .unwrap()
        .expect("map-failed workflow task");
    assert_eq!(ready.reason, durust::WorkflowTaskReason::ActivityMapFailed);

    let history = stream_history(&backend, run_id).await;
    assert_eq!(history.len(), 3);
    let HistoryEventData::ActivityMapFailed(failed) = &history[2].data else {
        panic!("expected compact ActivityMapFailed event");
    };
    assert_eq!(failed.failure.message, "map item failed");
}

async fn child_workflow_map_materializes_bounded_children_and_writes_result_manifest<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, _command_id, parent_opts, child_opts, scheduled) = schedule_child_workflow_map(
        backend.clone(),
        "wf/child-map-success",
        "child-map-success-parent",
        "child-map-success-children",
        "wf/child-map-success/item",
        durust::ChildWorkflowMapFailureMode::FailFast,
        durust::ParentClosePolicy::Cancel,
        2,
    )
    .await;
    scheduled.expect("scheduling a valid child map commits");

    dispatch_child_map_starts(&backend).await;
    let first = backend
        .claim_workflow_task(WorkerId::new("child-map-success-0"), child_opts.clone())
        .await
        .unwrap()
        .expect("first child map item");
    let second = backend
        .claim_workflow_task(WorkerId::new("child-map-success-1"), child_opts.clone())
        .await
        .unwrap()
        .expect("second child map item");
    assert_eq!(
        first.workflow_id,
        durust::WorkflowId::new("wf/child-map-success/item/0")
    );
    assert_eq!(
        second.workflow_id,
        durust::WorkflowId::new("wf/child-map-success/item/1")
    );
    let hidden = backend
        .claim_workflow_task(
            WorkerId::new("child-map-success-hidden"),
            child_opts.clone(),
        )
        .await
        .unwrap();
    assert!(hidden.is_none());

    complete_child_run(&backend, first, 10).await;
    dispatch_child_map_starts(&backend).await;
    let third = backend
        .claim_workflow_task(WorkerId::new("child-map-success-2"), child_opts.clone())
        .await
        .unwrap()
        .expect("third child map item after one completion");
    assert_eq!(
        third.workflow_id,
        durust::WorkflowId::new("wf/child-map-success/item/2")
    );

    complete_child_run(&backend, third, 30).await;
    let not_ready = backend
        .claim_workflow_task(
            WorkerId::new("child-map-success-parent-not-ready"),
            parent_opts.clone(),
        )
        .await
        .unwrap();
    assert!(not_ready.is_none());
    complete_child_run(&backend, second, 20).await;

    let ready = backend
        .claim_workflow_task(WorkerId::new("child-map-success-parent-ready"), parent_opts)
        .await
        .unwrap()
        .expect("parent ready after child map completion");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowMapCompleted
    );

    let history = stream_history(&backend, run_id).await;
    assert_compact_child_workflow_map_parent_history(&history);
    let completed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowMapCompleted(completed) => Some(completed),
            _ => None,
        })
        .expect("child workflow map completed event");
    assert_eq!(completed.item_count, 3);
    assert_eq!(completed.success_count, 3);
    assert_eq!(completed.failure_count, 0);
    assert_eq!(completed.cancellation_count, 0);
    let refs = durust::decode_child_workflow_map_success_refs(&completed.result_manifest).unwrap();
    let values = refs
        .iter()
        .map(|payload| durust::decode_payload::<u64>(payload).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values, vec![10, 20, 30]);
}

async fn child_workflow_map_fail_fast_cancels_in_flight_children<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, _command_id, parent_opts, child_opts, scheduled) = schedule_child_workflow_map(
        backend.clone(),
        "wf/child-map-fail-fast",
        "child-map-fail-fast-parent",
        "child-map-fail-fast-children",
        "wf/child-map-fail-fast/item",
        durust::ChildWorkflowMapFailureMode::FailFast,
        durust::ParentClosePolicy::Cancel,
        2,
    )
    .await;
    scheduled.expect("scheduling a valid child map commits");

    dispatch_child_map_starts(&backend).await;
    let first = backend
        .claim_workflow_task(WorkerId::new("child-map-fail-fast-0"), child_opts.clone())
        .await
        .unwrap()
        .expect("first child map item");
    let second = backend
        .claim_workflow_task(WorkerId::new("child-map-fail-fast-1"), child_opts.clone())
        .await
        .unwrap()
        .expect("second child map item");

    fail_child_run(&backend, first, "test.child_failed", "child failed").await;
    let ready = backend
        .claim_workflow_task(
            WorkerId::new("child-map-fail-fast-parent-ready"),
            parent_opts,
        )
        .await
        .unwrap()
        .expect("parent ready after child map failure");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowMapFailed
    );

    let no_more_children = backend
        .claim_workflow_task(WorkerId::new("child-map-fail-fast-no-more"), child_opts)
        .await
        .unwrap();
    assert!(no_more_children.is_none());
    let second_history = stream_history(&backend, second.run_id).await;
    assert!(
        second_history
            .iter()
            .any(|event| matches!(event.data, HistoryEventData::WorkflowCancelled { .. }))
    );

    let history = stream_history(&backend, run_id).await;
    assert_compact_child_workflow_map_parent_history(&history);
    let failed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowMapFailed(failed) => Some(failed),
            _ => None,
        })
        .expect("child workflow map failed event");
    assert_eq!(failed.failure.error_type, "test.child_failed");
}

async fn child_workflow_map_collect_all_records_ordered_outcomes<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, _command_id, parent_opts, child_opts, scheduled) = schedule_child_workflow_map(
        backend.clone(),
        "wf/child-map-collect-all",
        "child-map-collect-all-parent",
        "child-map-collect-all-children",
        "wf/child-map-collect-all/item",
        durust::ChildWorkflowMapFailureMode::CollectAll,
        durust::ParentClosePolicy::Cancel,
        2,
    )
    .await;
    scheduled.expect("scheduling a valid child map commits");

    dispatch_child_map_starts(&backend).await;
    let first = backend
        .claim_workflow_task(WorkerId::new("child-map-collect-all-0"), child_opts.clone())
        .await
        .unwrap()
        .expect("first child map item");
    let second = backend
        .claim_workflow_task(WorkerId::new("child-map-collect-all-1"), child_opts.clone())
        .await
        .unwrap()
        .expect("second child map item");
    fail_child_run(&backend, first, "test.collect_failed", "collect failure").await;
    dispatch_child_map_starts(&backend).await;
    let third = backend
        .claim_workflow_task(WorkerId::new("child-map-collect-all-2"), child_opts)
        .await
        .unwrap()
        .expect("third collect-all child map item");
    complete_child_run(&backend, second, 20).await;
    complete_child_run(&backend, third, 30).await;

    let ready = backend
        .claim_workflow_task(
            WorkerId::new("child-map-collect-all-parent-ready"),
            parent_opts,
        )
        .await
        .unwrap()
        .expect("parent ready after collect-all child map completion");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowMapCompleted
    );

    let history = stream_history(&backend, run_id).await;
    assert_compact_child_workflow_map_parent_history(&history);
    let completed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowMapCompleted(completed) => Some(completed),
            _ => None,
        })
        .expect("collect-all child workflow map completed event");
    assert_eq!(completed.item_count, 3);
    assert_eq!(completed.success_count, 2);
    assert_eq!(completed.failure_count, 1);
    assert_eq!(completed.cancellation_count, 0);
    let outcomes = durust::decode_child_workflow_map_outcomes(&completed.result_manifest).unwrap();
    assert_eq!(outcomes.len(), 3);
    match &outcomes[0] {
        durust::ChildWorkflowMapItemOutcome::Failed { failure } => {
            assert_eq!(failure.error_type, "test.collect_failed");
        }
        other => panic!("expected failed first outcome, got {other:?}"),
    }
    match &outcomes[1] {
        durust::ChildWorkflowMapItemOutcome::Succeeded { result } => {
            assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 20);
        }
        other => panic!("expected successful second outcome, got {other:?}"),
    }
    match &outcomes[2] {
        durust::ChildWorkflowMapItemOutcome::Succeeded { result } => {
            assert_eq!(durust::decode_payload::<u64>(result).unwrap(), 30);
        }
        other => panic!("expected successful third outcome, got {other:?}"),
    }
}

/// Row 6G: a map scheduled with an *empty* input manifest completes at
/// descriptor creation instead of stalling the parent forever.
///
/// `activity_map_manifest(std::iter::empty())` is an ordinary DSL call and
/// yields `item_count: 0`. Nothing was ever materialized, so no later event
/// could reach the completion check: the commit succeeded, the descriptor was
/// inserted, and `result_manifest()` blocked forever on all three providers.
/// Not a hang or an error — a silent permanent stall. TypeScript already
/// completes such a map at descriptor creation and Rust converges onto it.
///
/// The terminal fact is appended by the *same* commit that schedules the map,
/// so this also pins the two things that break when a provider forgets that:
/// the returned `new_tail_event_id` and the run's post-commit ready reason.
/// Both map kinds, because they take different routes through the engine's
/// `CompleteMap` applier (result table versus outcome table).
async fn empty_input_manifest_completes_at_descriptor_creation<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let activity_queue = TaskQueue::new("empty-map-activities");
    let activity_name = ActivityName::new("conformance.echo");
    let empty_manifest = durust::encode_activity_map_input_manifest(Vec::new(), 2).unwrap();

    let run_id = client
        .start_workflow::<workflow>("wf/empty-activity-map", "empty-map-workflows", input(1))
        .await
        .unwrap();
    let parent_opts = workflow_claim_opts("empty-map-workflows");
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("empty-activity-map-scheduler"),
            parent_opts.clone(),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let scheduled = durust::ActivityMapScheduled {
        command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: activity_queue.clone(),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: empty_manifest.clone(),
        result_manifest_name: "empty".to_owned(),
        max_in_flight: 2,
        fingerprint: durust::activity_map_fingerprint(
            activity_name.clone(),
            durust::payload_digest(&empty_manifest),
            "empty".to_owned(),
            2,
            "sha256:test-options".to_owned(),
        ),
    };
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityMapScheduled(scheduled),
                )],
                schedule_activity_maps: vec![ActivityMapTask {
                    map_command_id: command_id.clone(),
                    activity_name,
                    task_queue: activity_queue,
                    retry_policy: durust::RetryPolicy::none(),
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                    input_manifest: empty_manifest,
                    result_manifest_name: "empty".to_owned(),
                    max_in_flight: 2,
                }],
                query_projection: Some(durust::encode_payload(&"empty-map").unwrap()),
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(3)
        },
        "the empty map's terminal fact is appended by the scheduling commit, \
         so the returned tail must count it"
    );
    // The projection records the history point the workflow task itself
    // observed, so it stays at the scheduling event on every provider even
    // though the map's terminal fact landed after it in the same commit.
    match backend
        .query_projection(durust::QueryProjectionRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new("wf/empty-activity-map"),
        })
        .await
        .unwrap()
    {
        durust::QueryProjectionOutcome::Found { event_id, .. } => {
            assert_eq!(event_id, EventId(2))
        }
        other => panic!("expected a stored query projection, got {other:?}"),
    }
    let history = stream_history(&backend, run_id.clone()).await;
    assert_eq!(
        history
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            durust::HistoryEventType::WorkflowStarted,
            durust::HistoryEventType::ActivityMapScheduled,
            durust::HistoryEventType::ActivityMapCompleted,
        ],
    );
    let completed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ActivityMapCompleted(completed) => Some(completed),
            _ => None,
        })
        .expect("empty activity map completed event");
    assert_eq!(completed.item_count, 0);
    assert_eq!(completed.success_count, 0);
    assert_eq!(completed.failure_count, 0);
    assert!(
        durust::decode_activity_map_result_refs(&completed.result_manifest)
            .unwrap()
            .is_empty()
    );
    let ready = backend
        .claim_workflow_task(WorkerId::new("empty-activity-map-ready"), parent_opts)
        .await
        .unwrap()
        .expect("the parent of an empty map must be woken by the scheduling commit");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ActivityMapCompleted
    );
    assert_eq!(ready.replay_target_event_id, EventId(3));
    backend
        .commit_workflow_task(ready.claim, terminal_parent_commit(EventId(3)))
        .await
        .unwrap();

    // The child-workflow-map half. Same shape, different terminal applier.
    let empty_manifest = durust::encode_activity_map_input_manifest(Vec::new(), 2).unwrap();
    let child_queue = TaskQueue::new("empty-child-map-children");
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let child_run_id = client
        .start_workflow::<workflow>("wf/empty-child-map", "empty-child-map-workflows", input(1))
        .await
        .unwrap();
    let parent_opts = workflow_claim_opts("empty-child-map-workflows");
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("empty-child-map-scheduler"),
            parent_opts.clone(),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&child_run_id, 1);
    let map_task = ChildWorkflowMapTask {
        map_command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: child_queue.clone(),
        input_manifest: empty_manifest.clone(),
        result_manifest_name: "empty".to_owned(),
        workflow_id_prefix: "wf/empty-child-map/item".to_owned(),
        max_in_flight: 2,
        parent_close_policy: durust::ParentClosePolicy::Cancel,
        failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
    };
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ChildWorkflowMapScheduled(
                        durust::ChildWorkflowMapScheduled {
                            command_id: command_id.clone(),
                            workflow_type: workflow_type.clone(),
                            task_queue: child_queue.clone(),
                            input_manifest: empty_manifest.clone(),
                            result_manifest_name: "empty".to_owned(),
                            workflow_id_prefix: "wf/empty-child-map/item".to_owned(),
                            max_in_flight: 2,
                            parent_close_policy: durust::ParentClosePolicy::Cancel,
                            failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
                            fingerprint: durust::child_workflow_map_fingerprint(
                                workflow_type,
                                durust::payload_digest(&empty_manifest),
                                "empty".to_owned(),
                                "wf/empty-child-map/item".to_owned(),
                                2,
                                child_queue,
                                durust::ParentClosePolicy::Cancel,
                                durust::ChildWorkflowMapFailureMode::CollectAll,
                            ),
                        },
                    ),
                )],
                schedule_child_workflow_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(3)
        },
    );
    let history = stream_history(&backend, child_run_id).await;
    assert_eq!(
        history
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            durust::HistoryEventType::WorkflowStarted,
            durust::HistoryEventType::ChildWorkflowMapScheduled,
            durust::HistoryEventType::ChildWorkflowMapCompleted,
        ],
    );
    let completed = history
        .iter()
        .find_map(|event| match &event.data {
            HistoryEventData::ChildWorkflowMapCompleted(completed) => Some(completed),
            _ => None,
        })
        .expect("empty child workflow map completed event");
    assert_eq!(completed.item_count, 0);
    assert_eq!(completed.cancellation_count, 0);
    assert!(
        durust::decode_child_workflow_map_outcomes(&completed.result_manifest)
            .unwrap()
            .is_empty()
    );
    let ready = backend
        .claim_workflow_task(WorkerId::new("empty-child-map-ready"), parent_opts)
        .await
        .unwrap()
        .expect("the parent of an empty child map must be woken by the scheduling commit");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowMapCompleted
    );
    backend
        .commit_workflow_task(ready.claim, terminal_parent_commit(EventId(3)))
        .await
        .unwrap();
}

/// The carve-out 6G is required to keep: `DescriptorCreated` never rejects.
///
/// A workflow that spawns a map and returns without awaiting it produces a
/// single commit that both schedules the map and closes the run. Every
/// provider accepts that commit today. Routing the new empty-map completion
/// through the ordinary terminal path would answer `TerminalWorkflow` for an
/// activity map and roll the **whole workflow-task commit** back — the change
/// that got the equivalent TypeScript proposal (D9) reverted, and the reason
/// the TypeScript engine pins this with its own mutation.
///
/// The commit stays accepted, and no map fact lands behind the run's own
/// terminal event: the closed parent has nobody to notify and the descriptor
/// is deleted by the same commit's terminal cleanup, so appending would only
/// corrupt the history every replay and audit reads.
async fn an_empty_map_scheduled_by_a_closing_commit_is_still_accepted<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(
            "wf/empty-map-closing-commit",
            "empty-map-closing-workflows",
            input(1),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("empty-map-closing-scheduler"),
            workflow_claim_opts("empty-map-closing-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let empty_manifest = durust::encode_activity_map_input_manifest(Vec::new(), 2).unwrap();
    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new("empty-map-closing-activities");
    let scheduled = durust::ActivityMapScheduled {
        command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: empty_manifest.clone(),
        result_manifest_name: "empty".to_owned(),
        max_in_flight: 2,
        fingerprint: durust::activity_map_fingerprint(
            activity_name.clone(),
            durust::payload_digest(&empty_manifest),
            "empty".to_owned(),
            2,
            "sha256:test-options".to_owned(),
        ),
    };
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![
                    durust::NewHistoryEvent::new(HistoryEventData::ActivityMapScheduled(scheduled)),
                    durust::NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
                        result: durust::encode_payload(&()).unwrap(),
                    }),
                ],
                schedule_activity_maps: vec![ActivityMapTask {
                    map_command_id: command_id,
                    activity_name,
                    task_queue,
                    retry_policy: durust::RetryPolicy::none(),
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                    input_manifest: empty_manifest,
                    result_manifest_name: "empty".to_owned(),
                    max_in_flight: 2,
                }],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .expect("a commit that schedules an empty map and closes its run must stay accepted");
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(3)
        },
    );
    assert_eq!(
        stream_history(&backend, run_id)
            .await
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            durust::HistoryEventType::WorkflowStarted,
            durust::HistoryEventType::ActivityMapScheduled,
            durust::HistoryEventType::WorkflowCompleted,
        ],
        "no map fact may land behind the run's own terminal event",
    );

    // The child-map half. It reaches the carve-out down a different arm — an
    // activity map is the kind `terminal_parent` would have *rejected*, a child
    // map the kind it would have let through — so covering only the activity
    // map leaves the arm that actually produces effects untested.
    let child_run_id = client
        .start_workflow::<workflow>(
            "wf/empty-child-map-closing-commit",
            "empty-map-closing-workflows",
            input(1),
        )
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("empty-child-map-closing-scheduler"),
            workflow_claim_opts("empty-map-closing-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&child_run_id, 1);
    let empty_manifest = durust::encode_activity_map_input_manifest(Vec::new(), 2).unwrap();
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let task_queue = TaskQueue::new("empty-map-closing-children");
    let prefix = "wf/empty-map-closing/item".to_owned();
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![
                    durust::NewHistoryEvent::new(HistoryEventData::ChildWorkflowMapScheduled(
                        durust::ChildWorkflowMapScheduled {
                            command_id: command_id.clone(),
                            workflow_type: workflow_type.clone(),
                            task_queue: task_queue.clone(),
                            input_manifest: empty_manifest.clone(),
                            result_manifest_name: "empty".to_owned(),
                            workflow_id_prefix: prefix.clone(),
                            max_in_flight: 2,
                            parent_close_policy: durust::ParentClosePolicy::Cancel,
                            failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
                            fingerprint: durust::child_workflow_map_fingerprint(
                                workflow_type.clone(),
                                durust::payload_digest(&empty_manifest),
                                "empty".to_owned(),
                                prefix.clone(),
                                2,
                                task_queue.clone(),
                                durust::ParentClosePolicy::Cancel,
                                durust::ChildWorkflowMapFailureMode::CollectAll,
                            ),
                        },
                    )),
                    durust::NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
                        result: durust::encode_payload(&()).unwrap(),
                    }),
                ],
                schedule_child_workflow_maps: vec![ChildWorkflowMapTask {
                    map_command_id: command_id,
                    workflow_type,
                    task_queue,
                    input_manifest: empty_manifest,
                    result_manifest_name: "empty".to_owned(),
                    workflow_id_prefix: prefix,
                    max_in_flight: 2,
                    parent_close_policy: durust::ParentClosePolicy::Cancel,
                    failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
                }],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .expect("a commit that schedules an empty child map and closes its run stays accepted");
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(3)
        },
    );
    assert_eq!(
        stream_history(&backend, child_run_id)
            .await
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            durust::HistoryEventType::WorkflowStarted,
            durust::HistoryEventType::ChildWorkflowMapScheduled,
            durust::HistoryEventType::WorkflowCompleted,
        ],
        "no map fact may land behind the run's own terminal event",
    );
}

/// One commit, both empty map kinds, and a query projection: the one case where
/// all four provider-side pieces of 6G interact.
///
/// The tail must count *both* map facts, the two facts must take contiguous
/// event ids after the two scheduling events, the projection must stay at the
/// tail the workflow task itself observed rather than following the maps, and
/// the post-commit ready reason must be the last map stepped. Each piece is
/// pinned on its own elsewhere; nothing pinned them together, and the tail
/// pre-publish is deliberately once-per-commit, so a second completing map
/// exercises a path the single-map cases never reach.
async fn one_commit_completing_two_empty_maps_keeps_its_event_ids_contiguous<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/two-empty-maps", "two-empty-map-workflows", input(1))
        .await
        .unwrap();
    let parent_opts = workflow_claim_opts("two-empty-map-workflows");
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("two-empty-maps-scheduler"),
            parent_opts.clone(),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let activity_command = durust::command_id(&run_id, 1);
    let child_command = durust::command_id(&run_id, 2);
    let empty_manifest = durust::encode_activity_map_input_manifest(Vec::new(), 2).unwrap();
    let activity_name = ActivityName::new("conformance.echo");
    let activity_queue = TaskQueue::new("two-empty-map-activities");
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let child_queue = TaskQueue::new("two-empty-map-children");
    let prefix = "wf/two-empty-maps/item".to_owned();

    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![
                    durust::NewHistoryEvent::new(HistoryEventData::ActivityMapScheduled(
                        durust::ActivityMapScheduled {
                            command_id: activity_command.clone(),
                            activity_name: activity_name.clone(),
                            task_queue: activity_queue.clone(),
                            retry_policy: durust::RetryPolicy::none(),
                            start_to_close_timeout: None,
                            heartbeat_timeout: None,
                            input_manifest: empty_manifest.clone(),
                            result_manifest_name: "empty".to_owned(),
                            max_in_flight: 2,
                            fingerprint: durust::activity_map_fingerprint(
                                activity_name.clone(),
                                durust::payload_digest(&empty_manifest),
                                "empty".to_owned(),
                                2,
                                "sha256:test-options".to_owned(),
                            ),
                        },
                    )),
                    durust::NewHistoryEvent::new(HistoryEventData::ChildWorkflowMapScheduled(
                        durust::ChildWorkflowMapScheduled {
                            command_id: child_command.clone(),
                            workflow_type: workflow_type.clone(),
                            task_queue: child_queue.clone(),
                            input_manifest: empty_manifest.clone(),
                            result_manifest_name: "empty".to_owned(),
                            workflow_id_prefix: prefix.clone(),
                            max_in_flight: 2,
                            parent_close_policy: durust::ParentClosePolicy::Cancel,
                            failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
                            fingerprint: durust::child_workflow_map_fingerprint(
                                workflow_type.clone(),
                                durust::payload_digest(&empty_manifest),
                                "empty".to_owned(),
                                prefix.clone(),
                                2,
                                child_queue.clone(),
                                durust::ParentClosePolicy::Cancel,
                                durust::ChildWorkflowMapFailureMode::CollectAll,
                            ),
                        },
                    )),
                ],
                schedule_activity_maps: vec![ActivityMapTask {
                    map_command_id: activity_command,
                    activity_name,
                    task_queue: activity_queue,
                    retry_policy: durust::RetryPolicy::none(),
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                    input_manifest: empty_manifest.clone(),
                    result_manifest_name: "empty".to_owned(),
                    max_in_flight: 2,
                }],
                schedule_child_workflow_maps: vec![ChildWorkflowMapTask {
                    map_command_id: child_command,
                    workflow_type,
                    task_queue: child_queue,
                    input_manifest: empty_manifest,
                    result_manifest_name: "empty".to_owned(),
                    workflow_id_prefix: prefix,
                    max_in_flight: 2,
                    parent_close_policy: durust::ParentClosePolicy::Cancel,
                    failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
                }],
                query_projection: Some(durust::encode_payload(&"two-empty-maps").unwrap()),
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(5)
        },
    );
    let history = stream_history(&backend, run_id).await;
    assert_eq!(
        history
            .iter()
            .map(|event| (event.event_id, event.event_type))
            .collect::<Vec<_>>(),
        vec![
            (EventId(1), durust::HistoryEventType::WorkflowStarted),
            (EventId(2), durust::HistoryEventType::ActivityMapScheduled),
            (
                EventId(3),
                durust::HistoryEventType::ChildWorkflowMapScheduled
            ),
            (EventId(4), durust::HistoryEventType::ActivityMapCompleted),
            (
                EventId(5),
                durust::HistoryEventType::ChildWorkflowMapCompleted
            ),
        ],
        "two maps completing in one commit must take contiguous ids after the scheduling events",
    );
    match backend
        .query_projection(durust::QueryProjectionRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new("wf/two-empty-maps"),
        })
        .await
        .unwrap()
    {
        durust::QueryProjectionOutcome::Found { event_id, .. } => {
            assert_eq!(
                event_id,
                EventId(3),
                "the projection stays at the append tail"
            )
        }
        other => panic!("expected a stored query projection, got {other:?}"),
    }
    let ready = backend
        .claim_workflow_task(WorkerId::new("two-empty-maps-ready"), parent_opts)
        .await
        .unwrap()
        .expect("the parent is woken by the maps its own commit completed");
    assert_eq!(
        ready.reason,
        durust::WorkflowTaskReason::ChildWorkflowMapCompleted,
        "the last map stepped names the wake reason",
    );
    assert_eq!(ready.replay_target_event_id, EventId(5));
    backend
        .commit_workflow_task(ready.claim, terminal_parent_commit(EventId(5)))
        .await
        .unwrap();
}

/// Rows 6K and 6M: a child of a *closed* parent's map under
/// `ParentClosePolicy::Abandon` must still be able to reach a terminal state.
///
/// The parent's terminal cleanup deletes the map descriptor, but an abandoned
/// child keeps running by design and eventually commits its own terminal event.
/// That commit routes through `complete_child_workflow_map_item`, which used to
/// raise `Backend("child workflow map \`run\`:seq not found")` on SQLite and
/// Postgres — rolling the child's terminal commit back, permanently, because
/// every retry found the same missing descriptor. The in-memory provider
/// survived only because it discarded the routing result with `let _ =`.
///
/// The missing descriptor now answers the same way a missing activity record
/// does on `complete_activity`/`fail_activity` and a missing activity-map
/// descriptor does on `fail_map_item`: already handled, not an error. Both
/// terminal shapes are driven, because they take different routes through the
/// engine (`Succeeded` and `Failed`).
async fn abandoned_child_of_closed_map_parent_can_still_terminate<B>(backend: B)
where
    B: DurableBackend,
{
    let (_parent_run_id, _command_id, _parent_opts, child_opts, scheduled) =
        schedule_child_workflow_map(
            backend.clone(),
            "wf/child-map-abandon",
            "child-map-abandon-parent",
            "child-map-abandon-children",
            "wf/child-map-abandon/item",
            durust::ChildWorkflowMapFailureMode::CollectAll,
            durust::ParentClosePolicy::Abandon,
            2,
        )
        .await;
    scheduled.expect("scheduling a valid child map commits");

    dispatch_child_map_starts(&backend).await;
    let first = backend
        .claim_workflow_task(WorkerId::new("child-map-abandon-0"), child_opts.clone())
        .await
        .unwrap()
        .expect("first child map item");
    let second = backend
        .claim_workflow_task(WorkerId::new("child-map-abandon-1"), child_opts)
        .await
        .unwrap()
        .expect("second child map item");

    let client = Client::new(backend.clone());
    client
        .cancel_workflow("wf/child-map-abandon", "parent closed mid-fanout")
        .await
        .unwrap();

    let first_run_id = first.run_id.clone();
    let second_run_id = second.run_id.clone();
    complete_child_run(&backend, first, 10).await;
    fail_child_run(&backend, second, "test.abandoned", "abandoned child failed").await;

    for (label, run_id, expected) in [
        (
            "completed",
            first_run_id,
            durust::HistoryEventType::WorkflowCompleted,
        ),
        (
            "failed",
            second_run_id,
            durust::HistoryEventType::WorkflowFailed,
        ),
    ] {
        let history = stream_history(&backend, run_id).await;
        let types = history
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>();
        assert!(
            types.contains(&expected),
            "the {label} abandoned child must reach {expected:?}, got {types:?}"
        );
    }
}

/// Withdrawing a child-map command must cancel the children it already
/// started.
///
/// `MapEvent::ParentCancelled` deliberately carries no
/// `MapEffect::CancelChildren`: its *other* producer is a run reaching a
/// terminal event, where the children belong to `ParentClosePolicy`, which is
/// free to abandon them. So the cancellation belongs to the provider at the
/// `cancel_commands` call site, and before this landed every Rust provider
/// tombstoned only the map's *undispatched* outbox rows — the children already
/// running kept running with nothing waiting for them, forever.
///
/// Drives a select-loser-shaped commit: schedule the map alongside a timer,
/// fire the timer to make the parent claimable again, then withdraw the map
/// command in the next commit.
async fn child_workflow_map_command_cancellation_cancels_started_children<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let parent_run_id = client
        .start_workflow::<workflow>("wf/child-map-cancel", "child-map-cancel-parent", input(1))
        .await
        .unwrap();
    let parent_opts = workflow_claim_opts("child-map-cancel-parent");
    let child_opts = workflow_claim_opts("child-map-cancel-children");
    let map_command_id = durust::command_id(&parent_run_id, 1);
    let timer_command_id = durust::command_id(&parent_run_id, 2);

    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("child-map-cancel-scheduler"),
            parent_opts.clone(),
        )
        .await
        .unwrap()
        .expect("parent workflow task");
    let input_manifest = durust::encode_activity_map_input_manifest(
        [1_u64, 2, 3]
            .into_iter()
            .map(|value| durust::encode_payload(&value).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let task_queue = TaskQueue::new("child-map-cancel-children");
    let workflow_id_prefix = "wf/child-map-cancel/item".to_owned();
    let result_manifest_name = "child-map-cancel-results".to_owned();
    let map_task = ChildWorkflowMapTask {
        map_command_id: map_command_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        input_manifest: input_manifest.clone(),
        result_manifest_name: result_manifest_name.clone(),
        workflow_id_prefix: workflow_id_prefix.clone(),
        max_in_flight: 2,
        parent_close_policy: durust::ParentClosePolicy::Cancel,
        failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
    };
    let scheduled = durust::ChildWorkflowMapScheduled {
        command_id: map_command_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        input_manifest: input_manifest.clone(),
        result_manifest_name: result_manifest_name.clone(),
        workflow_id_prefix: workflow_id_prefix.clone(),
        max_in_flight: 2,
        parent_close_policy: durust::ParentClosePolicy::Cancel,
        failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
        fingerprint: durust::child_workflow_map_fingerprint(
            workflow_type,
            durust::payload_digest(&input_manifest),
            result_manifest_name,
            workflow_id_prefix,
            2,
            task_queue,
            durust::ParentClosePolicy::Cancel,
            durust::ChildWorkflowMapFailureMode::CollectAll,
        ),
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![
                    durust::NewHistoryEvent::new(HistoryEventData::ChildWorkflowMapScheduled(
                        scheduled,
                    )),
                    durust::NewHistoryEvent::new(HistoryEventData::TimerStarted(
                        durust::TimerStarted {
                            command_id: timer_command_id.clone(),
                            fire_at: durust::TimestampMs(10),
                            fingerprint: durust::timer_fingerprint(
                                "sleep",
                                durust::TimestampMs(10),
                            ),
                        },
                    )),
                ],
                upsert_waits: vec![durust::WaitRecord {
                    wait_id: durust::WaitId::new(format!(
                        "{}:{}:timer",
                        timer_command_id.run_id, timer_command_id.seq.0
                    )),
                    run_id: parent_run_id.clone(),
                    command_id: timer_command_id,
                    kind: durust::WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(durust::TimestampMs(10)),
                }],
                schedule_child_workflow_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    dispatch_child_map_starts(&backend).await;
    let first = backend
        .claim_workflow_task(WorkerId::new("child-map-cancel-0"), child_opts.clone())
        .await
        .unwrap()
        .expect("first child map item");
    let second = backend
        .claim_workflow_task(WorkerId::new("child-map-cancel-1"), child_opts.clone())
        .await
        .unwrap()
        .expect("second child map item");
    let started = [first.run_id.clone(), second.run_id.clone()];

    let fired = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(10),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(fired.fired, 1);
    let ready = backend
        .claim_workflow_task(WorkerId::new("child-map-cancel-selector"), parent_opts)
        .await
        .unwrap()
        .expect("timer-ready parent workflow task");
    backend
        .commit_workflow_task(
            ready.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: ready.replay_target_event_id,
                cancel_commands: vec![map_command_id.clone()],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();

    let expected_reason = format!(
        "child workflow map `{}`:{} cancelled",
        map_command_id.run_id, map_command_id.seq.0
    );
    for (ordinal, child_run_id) in started.into_iter().enumerate() {
        let history = stream_history(&backend, child_run_id).await;
        let reason = history
            .iter()
            .find_map(|event| match &event.data {
                HistoryEventData::WorkflowCancelled { reason } => Some(reason.clone()),
                _ => None,
            })
            .unwrap_or_else(|| {
                panic!(
                    "child {ordinal} of a cancelled map must not be orphaned, got {:?}",
                    history
                        .iter()
                        .map(|event| event.event_type)
                        .collect::<Vec<_>>()
                )
            });
        assert_eq!(reason, expected_reason);
    }

    // The withdrawn command's remaining, never-dispatched item stays
    // tombstoned, and no cancelled child becomes claimable again.
    dispatch_child_map_starts(&backend).await;
    assert!(
        backend
            .claim_workflow_task(WorkerId::new("child-map-cancel-none"), child_opts)
            .await
            .unwrap()
            .is_none(),
        "a withdrawn map must leave no claimable child"
    );

    // Cancelling a command appends no parent-visible map fact: the commit that
    // carried the cancellation already recorded it.
    let parent_history = stream_history(&backend, parent_run_id).await;
    assert!(
        !parent_history.iter().any(|event| matches!(
            event.data,
            HistoryEventData::ChildWorkflowMapCompleted(_)
                | HistoryEventData::ChildWorkflowMapFailed(_)
        )),
        "a withdrawn map command appends no terminal map fact"
    );
}

async fn workflow_cancel_cleans_waits_activities_and_activity_maps<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/cancel-cleanup", "cancel-workflows", input(5))
        .await
        .unwrap();
    let claim_opts = workflow_claim_opts("cancel-workflows");
    let claimed = backend
        .claim_workflow_task(WorkerId::new("cancel-scheduler"), claim_opts.clone())
        .await
        .unwrap()
        .expect("workflow task");

    let now = backend.current_time().await.unwrap();
    let fire_at = durust::TimestampMs(now.0.saturating_add(50));
    let timer_command = durust::command_id(&run_id, 1);
    let activity_command = durust::command_id(&run_id, 2);
    let map_command = durust::command_id(&run_id, 3);
    let activity_input = durust::encode_payload(&Input { value: 7 }).unwrap();
    let retry_policy = durust::RetryPolicy::none();
    let scheduled_activity = durust::ActivityScheduled {
        command_id: activity_command.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("cancel-activities"),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input: activity_input.clone(),
        fingerprint: durust::activity_fingerprint(
            ActivityName::new("conformance.echo"),
            durust::payload_digest(&activity_input),
            "sha256:test-options".to_owned(),
        ),
    };
    let input_manifest = durust::encode_activity_map_input_manifest(
        [1_u64, 2]
            .into_iter()
            .map(|value| durust::encode_payload(&Input { value }).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let map_task = ActivityMapTask {
        map_command_id: map_command.clone(),
        activity_name: ActivityName::new("conformance.echo"),
        task_queue: TaskQueue::new("cancel-activities"),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "cancelled".to_owned(),
        max_in_flight: 2,
    };
    let wait_id = durust::WaitId::new(format!(
        "{}:{}:timer",
        timer_command.run_id, timer_command.seq.0
    ));
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![
                    durust::NewHistoryEvent::new(HistoryEventData::TimerStarted(
                        durust::TimerStarted {
                            command_id: timer_command.clone(),
                            fire_at,
                            fingerprint: durust::timer_fingerprint(
                                "sleep",
                                durust::TimestampMs(50),
                            ),
                        },
                    )),
                    durust::NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                        scheduled_activity.clone(),
                    )),
                    durust::NewHistoryEvent::new(HistoryEventData::ActivityMapScheduled(
                        durust::ActivityMapScheduled {
                            command_id: map_command.clone(),
                            activity_name: ActivityName::new("conformance.echo"),
                            task_queue: TaskQueue::new("cancel-activities"),
                            retry_policy,
                            start_to_close_timeout: None,
                            heartbeat_timeout: None,
                            input_manifest: input_manifest.clone(),
                            result_manifest_name: "cancelled".to_owned(),
                            max_in_flight: 2,
                            fingerprint: durust::activity_map_fingerprint(
                                ActivityName::new("conformance.echo"),
                                durust::payload_digest(&input_manifest),
                                "cancelled".to_owned(),
                                2,
                                "sha256:test-options".to_owned(),
                            ),
                        },
                    )),
                ],
                upsert_waits: vec![durust::WaitRecord {
                    wait_id,
                    run_id: run_id.clone(),
                    command_id: timer_command,
                    kind: durust::WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(fire_at),
                }],
                schedule_activities: vec![durust::ActivityTask::from_scheduled(
                    &scheduled_activity,
                )],
                schedule_activity_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(4)
        }
    );

    let activity_opts = ClaimActivityOptions {
        namespace: Namespace::default(),
        task_queue: TaskQueue::new("cancel-activities"),
        registered_activity_names: vec![ActivityName::new("conformance.echo")],
        lease_duration: Duration::from_secs(30),
    };
    let ordinary = backend
        .claim_activity_task(
            WorkerId::new("cancel-activity-worker"),
            activity_opts.clone(),
        )
        .await
        .unwrap()
        .expect("ordinary activity");
    assert!(ordinary.task.map_item.is_none());
    let map_item = backend
        .claim_activity_task(WorkerId::new("cancel-map-worker"), activity_opts.clone())
        .await
        .unwrap()
        .expect("map activity");
    assert_map_item(&map_item.task, 0, 1);

    let cancelled = client
        .cancel_workflow("wf/cancel-cleanup", "operator cancelled")
        .await
        .unwrap();
    assert_eq!(
        cancelled,
        durust::CancelWorkflowOutcome::Cancelled {
            run_id: run_id.clone(),
            event_id: EventId(5)
        }
    );
    let duplicate_cancel = client
        .cancel_workflow("wf/cancel-cleanup", "duplicate")
        .await
        .unwrap();
    assert_eq!(
        duplicate_cancel,
        durust::CancelWorkflowOutcome::AlreadyTerminal {
            run_id: run_id.clone()
        }
    );

    let workflow_after_cancel = backend
        .claim_workflow_task(WorkerId::new("cancel-workflow-claim"), claim_opts)
        .await
        .unwrap();
    assert!(workflow_after_cancel.is_none());
    let timer_after_cancel = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: fire_at,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(timer_after_cancel.fired, 0);
    let activity_after_cancel = backend
        .claim_activity_task(WorkerId::new("cancel-leftover-worker"), activity_opts)
        .await
        .unwrap();
    assert!(activity_after_cancel.is_none());

    let late_ordinary_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: ordinary.claim,
            result: durust::encode_payload(&7_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        late_ordinary_completion,
        durust::CompleteActivityOutcome::AlreadyCompleted
    );
    let late_map_completion = backend
        .complete_activity(CompleteActivityRequest {
            claim: map_item.claim,
            result: durust::encode_payload(&2_u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(
        late_map_completion,
        durust::CompleteActivityOutcome::AlreadyCompleted
    );
    let signal_after_cancel = client
        .signal_workflow("wf/cancel-cleanup", "ready", "signal/cancelled", "ignored")
        .await;
    assert!(matches!(signal_after_cancel, Err(Error::TerminalWorkflow)));

    let history = stream_history(&backend, run_id).await;
    assert_eq!(history.len(), 5);
    assert!(matches!(history[1].data, HistoryEventData::TimerStarted(_)));
    assert!(matches!(
        history[2].data,
        HistoryEventData::ActivityScheduled(_)
    ));
    assert!(matches!(
        history[3].data,
        HistoryEventData::ActivityMapScheduled(_)
    ));
    assert!(matches!(
        history[4].data,
        HistoryEventData::WorkflowCancelled { .. }
    ));
    assert!(!history.iter().any(|event| matches!(
        event.data,
        HistoryEventData::TimerFired(_)
            | HistoryEventData::ActivityCompleted(_)
            | HistoryEventData::ActivityMapCompleted(_)
            | HistoryEventData::ActivityMapFailed(_)
            | HistoryEventData::WorkflowFailed { .. }
    )));
}

async fn activity_claim_filters_and_stale_completion_is_rejected<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    client
        .start_workflow::<workflow>("wf/activity-filter", "activity-workflows", input(4))
        .await
        .unwrap();
    let mut workflow_worker = worker(backend.clone(), "activity-workflows", "activity-activities");
    workflow_worker.run_workflow_once().await.unwrap();

    let unmatched = backend
        .claim_activity_task(
            WorkerId::new("wrong-activity-worker"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("activity-activities"),
                registered_activity_names: vec![ActivityName::new("other.activity")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    assert!(unmatched.is_none());

    let claimed = backend
        .claim_activity_task(
            WorkerId::new("activity-worker"),
            ClaimActivityOptions {
                namespace: Namespace::default(),
                task_queue: TaskQueue::new("activity-activities"),
                registered_activity_names: vec![ActivityName::new("conformance.echo")],
                lease_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap()
        .expect("activity task");
    let mut stale_claim = claimed.claim.clone();
    stale_claim.token += 1;
    let err = backend
        .complete_activity(CompleteActivityRequest {
            claim: stale_claim,
            result: durust::encode_payload(&4u64).unwrap(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::StaleLease));

    let completed = backend
        .complete_activity(CompleteActivityRequest {
            claim: claimed.claim.clone(),
            result: durust::encode_payload(&4u64).unwrap(),
        })
        .await
        .unwrap();
    assert!(matches!(
        completed,
        durust::CompleteActivityOutcome::Completed { .. }
    ));
    let duplicate = backend
        .complete_activity(CompleteActivityRequest {
            claim: claimed.claim,
            result: durust::encode_payload(&4u64).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(duplicate, durust::CompleteActivityOutcome::AlreadyCompleted);
}

async fn batch_activity_completion_reports_ordered_duplicate_and_stale_results<B>(backend: B)
where
    B: DurableBackend,
{
    let workflow_type = WorkflowType::new("conformance.batch-activity", 1);
    let workflow_queue = TaskQueue::new("batch-activity-workflows");
    let activity_queue = TaskQueue::new("batch-activity-activities");
    let activity_name = ActivityName::new("conformance.echo");
    let started = backend
        .start_workflow(durust::StartWorkflowRequest {
            namespace: Namespace::default(),
            workflow_id: durust::WorkflowId::new("wf/batch-activity-completion"),
            workflow_type: workflow_type.clone(),
            task_queue: workflow_queue.clone(),
            input: durust::encode_payload(&0_u64).unwrap(),
        })
        .await
        .unwrap();
    let run_id = started.run_id().clone();
    let claim_opts = ClaimWorkflowTaskOptions {
        namespace: Namespace::default(),
        task_queue: workflow_queue,
        registered_workflow_types: vec![workflow_type],
        lease_duration: Duration::from_secs(30),
    };
    let claimed = backend
        .claim_workflow_task(WorkerId::new("batch-activity-scheduler"), claim_opts)
        .await
        .unwrap()
        .expect("workflow task");
    let schedules = (0..2_u64)
        .map(|index| {
            let input = durust::encode_payload(&Input { value: index }).unwrap();
            durust::ActivityScheduled {
                command_id: durust::CommandId {
                    run_id: run_id.clone(),
                    seq: durust::CommandSeq(index + 1),
                },
                activity_name: activity_name.clone(),
                task_queue: activity_queue.clone(),
                retry_policy: durust::RetryPolicy::none(),
                start_to_close_timeout: Some(Duration::from_secs(30)),
                heartbeat_timeout: None,
                fingerprint: durust::activity_fingerprint(
                    activity_name.clone(),
                    durust::payload_digest(&input),
                    format!("batch-activity-{index}"),
                ),
                input,
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        backend
            .commit_workflow_task(
                claimed.claim,
                WorkflowTaskCommit {
                    expected_tail_event_id: claimed.replay_target_event_id,
                    append_events: schedules
                        .iter()
                        .cloned()
                        .map(
                            |scheduled| NewHistoryEvent::new(HistoryEventData::ActivityScheduled(
                                scheduled,
                            ))
                        )
                        .collect(),
                    schedule_activities: schedules
                        .iter()
                        .map(durust::ActivityTask::from_scheduled)
                        .collect(),
                    ..WorkflowTaskCommit::default()
                },
            )
            .await
            .unwrap(),
        CommitOutcome::Committed {
            new_tail_event_id: EventId(3)
        }
    );

    let mut claimed_activities = backend
        .claim_activity_tasks(
            WorkerId::new("batch-activity-worker"),
            ClaimActivityTasksOptions {
                claim: ClaimActivityOptions {
                    namespace: Namespace::default(),
                    task_queue: activity_queue,
                    registered_activity_names: vec![activity_name],
                    lease_duration: Duration::from_secs(30),
                },
                limit: 2,
            },
        )
        .await
        .unwrap();
    claimed_activities
        .sort_by(|left, right| left.task.activity_id.0.cmp(&right.task.activity_id.0));
    assert_eq!(claimed_activities.len(), 2);

    assert_eq!(
        backend
            .complete_activity(CompleteActivityRequest {
                claim: claimed_activities[0].claim.clone(),
                result: durust::encode_payload(&1_u64).unwrap(),
            })
            .await
            .unwrap(),
        durust::CompleteActivityOutcome::Completed {
            event_id: EventId(4)
        }
    );

    let mut stale_claim = claimed_activities[1].claim.clone();
    stale_claim.token = stale_claim.token.saturating_add(1);
    let results = backend
        .complete_activity_tasks(CompleteActivityTasksRequest {
            completions: vec![
                CompleteActivityRequest {
                    claim: claimed_activities[0].claim.clone(),
                    result: durust::encode_payload(&10_u64).unwrap(),
                },
                CompleteActivityRequest {
                    claim: stale_claim,
                    result: durust::encode_payload(&20_u64).unwrap(),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].result.as_ref().unwrap(),
        &durust::CompleteActivityOutcome::AlreadyCompleted
    );
    assert!(matches!(results[1].result, Err(Error::StaleLease)));

    assert_eq!(
        backend
            .complete_activity(CompleteActivityRequest {
                claim: claimed_activities[1].claim.clone(),
                result: durust::encode_payload(&2_u64).unwrap(),
            })
            .await
            .unwrap(),
        durust::CompleteActivityOutcome::Completed {
            event_id: EventId(5)
        }
    );
}

fn worker<B>(backend: B, workflow_queue: &str, activity_queue: &str) -> Worker<B>
where
    B: DurableBackend,
{
    Worker::builder(backend)
        .workflow_task_queue(workflow_queue)
        .activity_task_queue(activity_queue)
        .register_workflow(workflow)
        .register_activity(echo)
        .build()
}

fn assert_map_item(task: &durust::ActivityTask, item_ordinal: u64, expected_input: u64) {
    let map_item = task.map_item.as_ref().expect("map item metadata");
    assert_eq!(map_item.item_ordinal, item_ordinal);
    assert_eq!(
        durust::decode_payload::<Input>(&task.input).unwrap().value,
        expected_input
    );
}

// ---------------------------------------------------------------------------
// Row 6B: behaviours that changed when the three providers started applying
// `src/map_engine.rs`'s effect list instead of each running its own copy of
// the fanout state machine. Every case below is driven through the public
// `DurableBackend` surface on all three providers, because the point of the
// extraction is that they now answer identically.
// ---------------------------------------------------------------------------

/// Schedule an activity map with a caller-chosen `max_in_flight` and item
/// count and return the map command id plus the activity claim options its
/// items are claimable with.
async fn schedule_activity_map<B>(
    backend: &B,
    workflow_id: &str,
    workflow_queue: &str,
    activity_queue: &str,
    item_count: u64,
    max_in_flight: usize,
    retry_policy: durust::RetryPolicy,
    start_to_close_timeout: Option<Duration>,
) -> (
    durust::RunId,
    durust::CommandId,
    ClaimActivityOptions,
    durust::Result<CommitOutcome>,
)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>(workflow_id, workflow_queue, input(1))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{workflow_id}-map-scheduler")),
            workflow_claim_opts(workflow_queue),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        (0..item_count)
            .map(|value| durust::encode_payload(&Input { value }).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new(activity_queue);
    let map_task = ActivityMapTask {
        map_command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: retry_policy.clone(),
        start_to_close_timeout,
        heartbeat_timeout: None,
        input_manifest: input_manifest.clone(),
        result_manifest_name: "mapped".to_owned(),
        max_in_flight,
    };
    let fingerprint = durust::activity_map_fingerprint(
        activity_name.clone(),
        durust::payload_digest(&input_manifest),
        "mapped".to_owned(),
        max_in_flight,
        "sha256:test-options".to_owned(),
    );
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityMapScheduled(durust::ActivityMapScheduled {
                        command_id: command_id.clone(),
                        activity_name: activity_name.clone(),
                        task_queue: task_queue.clone(),
                        retry_policy,
                        start_to_close_timeout,
                        heartbeat_timeout: None,
                        input_manifest,
                        result_manifest_name: "mapped".to_owned(),
                        max_in_flight,
                        fingerprint,
                    }),
                )],
                schedule_activity_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await;
    (
        run_id,
        command_id,
        ClaimActivityOptions {
            namespace: Namespace::default(),
            task_queue,
            registered_activity_names: vec![activity_name],
            lease_duration: Duration::from_secs(30),
        },
        outcome,
    )
}

/// Row 6J: `max_in_flight: 0` is rejected at descriptor creation on every
/// provider, not clamped to one and not stalled.
///
/// Two behaviours converged here. The in-memory provider read `max_in_flight`
/// without `.max(1)`, so a zero bound materialized nothing and the map stalled
/// forever while SQLite and Postgres admitted one item. `MapState::slot_limit`
/// clamps once for everyone, which removed the stall — but clamping alone turns
/// a caller typo into a silent 10,000x throughput loss, so the bound is now
/// rejected where it is still the caller's: the DSL builders *and* every
/// provider's descriptor-creation primitive, matching TypeScript.
///
/// The provider half is not redundant with the DSL half. This case never
/// touches the builders: it hands `commit_workflow_task` a `max_in_flight: 0`
/// `ActivityMapTask` directly, which is exactly what a non-DSL caller — another
/// language's client, a test harness, a custom runtime — does.
///
/// The engine's clamp is untouched and is not in tension with this: it runs
/// against descriptors that already exist, this runs only as one is created.
/// `map_engine::materialization_admits_one_batch_bounded_by_free_slots` covers
/// the clamp, which after this change is reachable only from a descriptor
/// persisted before the rejection landed.
async fn activity_map_zero_max_in_flight_is_rejected_at_descriptor_creation<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, _command_id, activity_opts, scheduled) = schedule_activity_map(
        &backend,
        "wf/map-zero-bound",
        "map-zero-bound-workflows",
        "map-zero-bound-activities",
        3,
        0,
        durust::RetryPolicy::none(),
        None,
    )
    .await;

    match scheduled.expect_err("a zero bound must be rejected at descriptor creation") {
        durust::Error::Application(failure) => {
            assert_eq!(failure.error_type, "durust.invalid_map_options");
            assert_eq!(
                failure.message,
                "activity_map max_in_flight must be a positive integer"
            );
            assert!(failure.non_retryable);
        }
        other => panic!("expected a non-retryable application error, got {other:?}"),
    }

    // The rejection is the whole commit's: no descriptor, no scheduled event,
    // and nothing claimable. A provider that rejected *after* materializing
    // would pass the assertion above and still leak work.
    assert_eq!(
        stream_history(&backend, run_id)
            .await
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![durust::HistoryEventType::WorkflowStarted],
    );
    assert!(
        backend
            .claim_activity_task(WorkerId::new("zero-bound-1"), activity_opts)
            .await
            .unwrap()
            .is_none(),
        "a rejected map must materialize no item"
    );
}

/// The child-workflow-map half of the zero-bound rejection.
///
/// Not redundant with the activity-map case: the check lives in a *different*
/// primitive on every provider — memory's pre-pass arm, `insert_child_workflow_map`,
/// `insert_child_workflow_map_tx` — and deleting exactly those three left the
/// entire suite green, because nothing drove a child map with a zero bound.
async fn child_workflow_map_zero_max_in_flight_is_rejected_at_descriptor_creation<B>(backend: B)
where
    B: DurableBackend,
{
    let (run_id, _command_id, _parent_opts, child_opts, scheduled) = schedule_child_workflow_map(
        backend.clone(),
        "wf/child-map-zero-bound",
        "child-map-zero-bound-parent",
        "child-map-zero-bound-children",
        "wf/child-map-zero-bound/item",
        durust::ChildWorkflowMapFailureMode::CollectAll,
        durust::ParentClosePolicy::Cancel,
        0,
    )
    .await;

    match scheduled.expect_err("a zero child-map bound must be rejected at descriptor creation") {
        durust::Error::Application(failure) => {
            assert_eq!(failure.error_type, "durust.invalid_map_options");
            assert_eq!(
                failure.message,
                "child_workflow_map max_in_flight must be a positive integer"
            );
            assert!(failure.non_retryable);
        }
        other => panic!("expected a non-retryable application error, got {other:?}"),
    }
    assert_eq!(
        stream_history(&backend, run_id)
            .await
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![durust::HistoryEventType::WorkflowStarted],
    );
    dispatch_child_map_starts(&backend).await;
    assert!(
        backend
            .claim_workflow_task(WorkerId::new("child-map-zero-bound-none"), child_opts)
            .await
            .unwrap()
            .is_none(),
        "a rejected map must start no child"
    );
}

/// A commit that schedules the same map command twice is rejected by every
/// provider.
///
/// SQLite raises on its unique index and Postgres on the `on conflict do
/// nothing` row count. Memory's `BTreeMap::insert` is a *replace*, so it used to
/// overwrite the descriptor and re-step `DescriptorCreated` — appending a second
/// `ActivityMapCompleted` for one command id when the map is empty. The insert
/// has to stay a replace (`MapState::next_ordinal`'s contract depends on the
/// cursor and outcome set being created together at zero), so memory rejects the
/// duplicate ahead of every mutation instead.
async fn a_commit_scheduling_one_map_twice_is_rejected<B>(backend: B)
where
    B: DurableBackend,
{
    let client = Client::new(backend.clone());
    let run_id = client
        .start_workflow::<workflow>("wf/duplicate-map", "duplicate-map-workflows", input(1))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("duplicate-map-scheduler"),
            workflow_claim_opts("duplicate-map-workflows"),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&run_id, 1);
    let empty_manifest = durust::encode_activity_map_input_manifest(Vec::new(), 2).unwrap();
    let activity_name = ActivityName::new("conformance.echo");
    let task_queue = TaskQueue::new("duplicate-map-activities");
    let map_task = ActivityMapTask {
        map_command_id: command_id.clone(),
        activity_name: activity_name.clone(),
        task_queue: task_queue.clone(),
        retry_policy: durust::RetryPolicy::none(),
        start_to_close_timeout: None,
        heartbeat_timeout: None,
        input_manifest: empty_manifest.clone(),
        result_manifest_name: "empty".to_owned(),
        max_in_flight: 2,
    };
    let err = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ActivityMapScheduled(durust::ActivityMapScheduled {
                        command_id: command_id.clone(),
                        activity_name,
                        task_queue,
                        retry_policy: durust::RetryPolicy::none(),
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                        input_manifest: empty_manifest.clone(),
                        result_manifest_name: "empty".to_owned(),
                        max_in_flight: 2,
                        fingerprint: durust::activity_map_fingerprint(
                            ActivityName::new("conformance.echo"),
                            durust::payload_digest(&empty_manifest),
                            "empty".to_owned(),
                            2,
                            "sha256:test-options".to_owned(),
                        ),
                    }),
                )],
                schedule_activity_maps: vec![map_task.clone(), map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .expect_err("one map command may only be scheduled once");
    assert!(
        !matches!(err, durust::Error::TerminalWorkflow),
        "the duplicate must be named, not mistaken for a terminal run: {err:?}"
    );
    // Nothing applied: the second descriptor would otherwise have appended a
    // second terminal fact for the same command id.
    assert_eq!(
        stream_history(&backend, run_id)
            .await
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![durust::HistoryEventType::WorkflowStarted],
    );
}

#[test]
fn memory_activity_map_zero_max_in_flight_is_rejected_at_descriptor_creation() {
    block_on(
        activity_map_zero_max_in_flight_is_rejected_at_descriptor_creation(MemoryBackend::new()),
    );
}

#[test]
fn sqlite_activity_map_zero_max_in_flight_is_rejected_at_descriptor_creation() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("map-zero-bound.sqlite3");
        let backend = SqliteBackend::open(&path).unwrap();
        activity_map_zero_max_in_flight_is_rejected_at_descriptor_creation(backend).await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_activity_map_zero_max_in_flight_is_rejected_at_descriptor_creation_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres zero-bound map conformance",
        "mapzerobound",
        |backend| async move {
            activity_map_zero_max_in_flight_is_rejected_at_descriptor_creation(backend).await;
        },
    ));
}

/// Behaviour change: a failed activity map tombstones its still-pending
/// sibling items (`MapEffect::AbandonPendingItems`).
///
/// Postgres already bulk-tombstoned on failure (`postgres.rs:7891`); the
/// in-memory and SQLite providers did not and relied on the claim-time guard
/// alone, so a sibling item stayed claimable-shaped in storage and the timeout
/// scanner could still *retry* it — rescheduling work for a map that was over.
/// The engine emits `AbandonPendingItems` on every terminal failure, so all
/// three now tombstone. The claim-time guard stays: it still has to answer for
/// an item claimed before the map ended.
async fn activity_map_failure_tombstones_pending_sibling_items<B>(backend: B)
where
    B: DurableBackend,
{
    let (_run_id, _command_id, activity_opts, scheduled) = schedule_activity_map(
        &backend,
        "wf/map-abandon",
        "map-abandon-workflows",
        "map-abandon-activities",
        4,
        2,
        // A policy that *would* retry and a deadline that *would* fire: under
        // the pre-6B in-memory and SQLite providers the sibling below stayed
        // live after the map failed, so its next failure rescheduled it and
        // the timeout scanner rescheduled it again.
        durust::RetryPolicy {
            backoff: durust::RetryBackoff::None,
            max_attempts: 5,
        },
        Some(Duration::from_secs(1)),
    )
    .await;
    scheduled.expect("scheduling a valid map commits");

    let first = backend
        .claim_activity_task(WorkerId::new("abandon-1"), activity_opts.clone())
        .await
        .unwrap()
        .expect("first map item");
    let sibling = backend
        .claim_activity_task(WorkerId::new("abandon-2"), activity_opts.clone())
        .await
        .unwrap()
        .expect("second map item");

    // Fail the first item non-retryably: the map is fail-fast, so it ends.
    let outcome = backend
        .fail_activity(FailActivityRequest {
            claim: first.claim,
            failure: durust::DurableFailure::non_retryable("boom", "item 0 failed"),
        })
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        durust::FailActivityOutcome::Failed { .. }
    ));

    // The sibling is tombstoned, so its own terminal call is a no-op rather
    // than a second slot release or a reschedule onto a dead map.
    assert_eq!(
        backend
            .fail_activity(FailActivityRequest {
                claim: sibling.claim.clone(),
                failure: durust::DurableFailure::new("retryable", "would have rescheduled"),
            })
            .await
            .unwrap(),
        durust::FailActivityOutcome::AlreadyCompleted,
        "a retryable failure on a map that is over must not reschedule the item",
    );
    assert_eq!(
        backend
            .complete_activity(CompleteActivityRequest {
                claim: sibling.claim,
                result: durust::encode_payload(&1_u64).unwrap(),
            })
            .await
            .unwrap(),
        durust::CompleteActivityOutcome::AlreadyCompleted,
    );

    // Nothing of this map is claimable any more, and the timeout scanner has
    // nothing left to resurrect.
    assert!(
        backend
            .claim_activity_task(WorkerId::new("abandon-3"), activity_opts)
            .await
            .unwrap()
            .is_none(),
    );
    assert_eq!(
        backend
            .timeout_due_activities(durust::TimeoutDueActivitiesRequest {
                namespace: Namespace::default(),
                now: durust::TimestampMs(i64::MAX / 2),
                limit: 32,
            })
            .await
            .unwrap()
            .timed_out,
        0,
        "no item of a finished map may time out and reschedule",
    );
}

#[test]
fn memory_activity_map_failure_tombstones_pending_sibling_items() {
    block_on(activity_map_failure_tombstones_pending_sibling_items(
        MemoryBackend::new(),
    ));
}

#[test]
fn sqlite_activity_map_failure_tombstones_pending_sibling_items() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("map-abandon.sqlite3");
        {
            let backend = SqliteBackend::open(&path).unwrap();
            activity_map_failure_tombstones_pending_sibling_items(backend).await;
        }
        // Close and reopen: the tombstones must be durable, not in-process
        // state.
        let reopened = SqliteBackend::open(&path).unwrap();
        assert!(
            reopened
                .claim_activity_task(
                    WorkerId::new("abandon-reopened"),
                    ClaimActivityOptions {
                        namespace: Namespace::default(),
                        task_queue: TaskQueue::new("map-abandon-activities"),
                        registered_activity_names: vec![ActivityName::new("conformance.echo")],
                        lease_duration: Duration::from_secs(30),
                    },
                )
                .await
                .unwrap()
                .is_none(),
        );
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_activity_map_failure_tombstones_pending_sibling_items_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres map abandon conformance",
        "mapabandon",
        |backend| async move {
            activity_map_failure_tombstones_pending_sibling_items(backend).await;
        },
    ));
}

/// Behaviour change 6H: a child-map item whose generated workflow id is
/// already taken takes its `max_in_flight` slot like every other admitted
/// ordinal, so real concurrency never exceeds the bound.
///
/// Postgres starts map children inline. `materialize_child_workflow_map_items_tx`
/// advanced `next_ordinal` before the match and `InlineChildStartOutcome::Failed`
/// took no slot (`postgres.rs:7252`, `:7258`), but the failure was then routed
/// through `complete_child_workflow_map_item_tx`, which *released* one
/// (`:7371`). Net −1 slot per conflicting ordinal, permanently, compounding.
/// The engine's D7 rule is that materialization always takes the slot and
/// exactly one terminal outcome releases it, so the conflict is now routed
/// back as an ordinary failed item after the batch is applied.
///
/// The collision is reachable through the public `Client::start_workflow`
/// API: any caller that starts a workflow named `{prefix}/{ordinal}` before
/// the map materializes that ordinal produces it.
async fn child_workflow_map_id_collision_holds_the_in_flight_bound<B>(backend: B)
where
    B: DurableBackend,
{
    let prefix = "wf/child-map-collision/item";
    let parent_queue = "child-map-collision-parent";
    let child_queue = "child-map-collision-children";
    let client = Client::new(backend.clone());

    // The squatter: a workflow started through the ordinary public API whose
    // id happens to be the one ordinal 1 will want. It lives on its own task
    // queue so it never shows up as a map child below.
    client
        .start_workflow::<workflow>(
            &format!("{prefix}/1"),
            "child-map-collision-squatter",
            input(99),
        )
        .await
        .unwrap();

    let parent_run_id = client
        .start_workflow::<workflow>("wf/child-map-collision", parent_queue, input(1))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new("child-map-collision-scheduler"),
            workflow_claim_opts(parent_queue),
        )
        .await
        .unwrap()
        .expect("parent workflow task");
    let command_id = durust::command_id(&parent_run_id, 1);
    let input_manifest = durust::encode_activity_map_input_manifest(
        (0..4_u64)
            .map(|value| durust::encode_payload(&value).unwrap())
            .collect(),
        2,
    )
    .unwrap();
    let workflow_type = WorkflowType::new("conformance.workflow", 1);
    let task_queue = TaskQueue::new(child_queue);
    let max_in_flight = 2;
    let map_task = ChildWorkflowMapTask {
        map_command_id: command_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        input_manifest: input_manifest.clone(),
        result_manifest_name: "collision-results".to_owned(),
        workflow_id_prefix: prefix.to_owned(),
        max_in_flight,
        parent_close_policy: durust::ParentClosePolicy::Abandon,
        // CollectAll, so the conflicting ordinal is recorded as a failed item
        // and the map keeps materializing instead of stopping at the first
        // non-success.
        failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
    };
    backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ChildWorkflowMapScheduled(
                        durust::ChildWorkflowMapScheduled {
                            command_id: command_id.clone(),
                            workflow_type: workflow_type.clone(),
                            task_queue: task_queue.clone(),
                            input_manifest: input_manifest.clone(),
                            result_manifest_name: "collision-results".to_owned(),
                            workflow_id_prefix: prefix.to_owned(),
                            max_in_flight,
                            parent_close_policy: durust::ParentClosePolicy::Abandon,
                            failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
                            fingerprint: durust::child_workflow_map_fingerprint(
                                workflow_type,
                                durust::payload_digest(&input_manifest),
                                "collision-results".to_owned(),
                                prefix.to_owned(),
                                max_in_flight,
                                task_queue,
                                durust::ParentClosePolicy::Abandon,
                                durust::ChildWorkflowMapFailureMode::CollectAll,
                            ),
                        },
                    ),
                )],
                schedule_child_workflow_maps: vec![map_task],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    dispatch_child_map_starts(&backend).await;

    // Every map child that actually exists right now. The bound is two, and
    // the conflicting ordinal must have consumed one of them and then given it
    // back exactly once — never given back a slot it never took.
    let mut started = BTreeSet::new();
    for index in 0..8 {
        let Some(child) = backend
            .claim_workflow_task(
                WorkerId::new(format!("child-map-collision-{index}")),
                workflow_claim_opts(child_queue),
            )
            .await
            .unwrap()
        else {
            break;
        };
        started.insert(child.workflow_id.0.clone());
    }
    assert_eq!(
        started,
        BTreeSet::from([format!("{prefix}/0"), format!("{prefix}/2")]),
        "an id collision must not admit an extra child past `max_in_flight`",
    );
}

#[test]
fn memory_child_workflow_map_id_collision_holds_the_in_flight_bound() {
    block_on(child_workflow_map_id_collision_holds_the_in_flight_bound(
        MemoryBackend::new(),
    ));
}

#[test]
fn sqlite_child_workflow_map_id_collision_holds_the_in_flight_bound() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("child-map-collision.sqlite3")).unwrap();
        child_workflow_map_id_collision_holds_the_in_flight_bound(backend).await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_child_workflow_map_id_collision_holds_the_in_flight_bound_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres child map collision conformance",
        "childmapcollision",
        |backend| async move {
            child_workflow_map_id_collision_holds_the_in_flight_bound(backend).await;
        },
    ));
}

/// The materialization effect is a contiguous *range*, and Postgres turns each
/// range into exactly one set-based statement rather than `count` round trips.
///
/// `map_engine::materialization_is_one_range_effect_per_batch` pins the engine
/// half (a 10,000-item batch is still one `MaterializeItems`); this pins the
/// provider half end to end at a size where a per-item loop would be obvious.
/// Postgres issues
///
/// ```sql
/// insert into <schema>.activity_tasks (...)
/// select item.activity_id, $1, $2, $3, $4, item.task, null, false, $5, null
/// from unnest($6::text[], $7::bytea[]) as item(activity_id, task)
/// on conflict(activity_id) do nothing
/// ```
///
/// once for the whole range, because every column except `activity_id` and
/// `task` is identical across one map's items.
async fn activity_map_materializes_a_large_batch_in_one_statement<B>(backend: B)
where
    B: DurableBackend,
{
    const ITEMS: u64 = 400;
    let (_run_id, _command_id, activity_opts, scheduled) = schedule_activity_map(
        &backend,
        "wf/map-batch",
        "map-batch-workflows",
        "map-batch-activities",
        ITEMS,
        ITEMS as usize,
        durust::RetryPolicy::none(),
        None,
    )
    .await;
    scheduled.expect("scheduling a valid map commits");

    let mut ordinals = BTreeSet::new();
    for index in 0..ITEMS + 1 {
        let Some(task) = backend
            .claim_activity_task(
                WorkerId::new(format!("batch-{index}")),
                activity_opts.clone(),
            )
            .await
            .unwrap()
        else {
            break;
        };
        let map_item = task.task.map_item.as_ref().expect("map item metadata");
        assert_eq!(
            durust::decode_payload::<Input>(&task.task.input)
                .unwrap()
                .value,
            map_item.item_ordinal,
            "each row of the batch carries its own manifest input",
        );
        assert!(
            ordinals.insert(map_item.item_ordinal),
            "the batch must admit each ordinal exactly once",
        );
    }
    assert_eq!(
        ordinals,
        (0..ITEMS).collect::<BTreeSet<_>>(),
        "one batch admits the whole contiguous range",
    );
}

#[test]
fn memory_activity_map_materializes_a_large_batch_in_one_statement() {
    block_on(activity_map_materializes_a_large_batch_in_one_statement(
        MemoryBackend::new(),
    ));
}

#[test]
fn sqlite_activity_map_materializes_a_large_batch_in_one_statement() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(dir.path().join("map-batch.sqlite3")).unwrap();
        activity_map_materializes_a_large_batch_in_one_statement(backend).await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_activity_map_materializes_a_large_batch_in_one_statement_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres map batch conformance",
        "mapbatch",
        |backend| async move {
            activity_map_materializes_a_large_batch_in_one_statement(backend).await;
        },
    ));
}

/// Row 6L: a child-map item whose `workflow_instances` row is absent after the
/// insert already conflicted must fail loudly instead of stranding an ordinal.
///
/// `start_child_workflow_inline_tx` inserts with `on conflict … do nothing` and
/// then re-reads the row `for update`. `InlineChildStartOutcome::Skipped` is the
/// state where the insert conflicted *and* the re-read found nothing — a
/// committed delete of that row between two statements of the same transaction.
/// It used to be matched together with `Started` and ignored. Materialization
/// had already taken the ordinal's slot, so an ignored `Skipped` left an ordinal
/// with no child, no outcome and no slot release: `outcome_count` could never
/// reach `item_count` and the map hung forever with nothing to observe.
///
/// **Reachability, stated honestly.** Nothing in this crate ever deletes a
/// `workflow_instances` row — `grep` finds no `delete from … workflow_instances`
/// in `src/postgres.rs` — so the real-world race needs an external writer, and
/// no in-process test can interleave a second connection between two statements
/// of a transaction it does not drive. This test therefore reaches the branch
/// by the one other route that makes an insert return no row without a
/// conflict: a `before insert` trigger that returns `null` for exactly one
/// workflow id. The mechanism is contrived; the branch, the message and the
/// rollback are the behaviour under test.
///
/// The second half is the point of choosing an error over a silent skip: the
/// failed transaction rolls back, and the *same* commit replayed without the
/// trigger starts the child normally. The guard is a recovery path, not just a
/// louder hang.
#[cfg(feature = "postgres")]
#[test]
fn postgres_child_map_item_vanishing_mid_transaction_fails_loudly_when_configured() {
    block_on_tokio(with_postgres_schema(
        "Postgres vanishing child map item test",
        "mapvanish",
        |backend, url, schema| async move {
            let prefix = "wf/map-vanish/item";
            let parent_queue = "map-vanish-parent";
            let child_queue = "map-vanish-children";
            let client = Client::new(backend.clone());
            let parent_run_id = client
                .start_workflow::<workflow>("wf/map-vanish", parent_queue, input(1))
                .await
                .unwrap();
            let claimed = backend
                .claim_workflow_task(
                    WorkerId::new("map-vanish-scheduler"),
                    workflow_claim_opts(parent_queue),
                )
                .await
                .unwrap()
                .expect("parent workflow task");
            let command_id = durust::command_id(&parent_run_id, 1);
            let input_manifest = durust::encode_activity_map_input_manifest(
                (0..2_u64)
                    .map(|value| durust::encode_payload(&value).unwrap())
                    .collect(),
                2,
            )
            .unwrap();
            let workflow_type = WorkflowType::new("conformance.workflow", 1);
            let task_queue = TaskQueue::new(child_queue);
            let map_task = ChildWorkflowMapTask {
                map_command_id: command_id.clone(),
                workflow_type: workflow_type.clone(),
                task_queue: task_queue.clone(),
                input_manifest: input_manifest.clone(),
                result_manifest_name: "vanish-results".to_owned(),
                workflow_id_prefix: prefix.to_owned(),
                max_in_flight: 2,
                parent_close_policy: durust::ParentClosePolicy::Abandon,
                failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
            };
            let commit = || WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ChildWorkflowMapScheduled(
                        durust::ChildWorkflowMapScheduled {
                            command_id: command_id.clone(),
                            workflow_type: workflow_type.clone(),
                            task_queue: task_queue.clone(),
                            input_manifest: input_manifest.clone(),
                            result_manifest_name: "vanish-results".to_owned(),
                            workflow_id_prefix: prefix.to_owned(),
                            max_in_flight: 2,
                            parent_close_policy: durust::ParentClosePolicy::Abandon,
                            failure_mode: durust::ChildWorkflowMapFailureMode::CollectAll,
                            fingerprint: durust::child_workflow_map_fingerprint(
                                workflow_type.clone(),
                                durust::payload_digest(&input_manifest),
                                "vanish-results".to_owned(),
                                prefix.to_owned(),
                                2,
                                task_queue.clone(),
                                durust::ParentClosePolicy::Abandon,
                                durust::ChildWorkflowMapFailureMode::CollectAll,
                            ),
                        },
                    ),
                )],
                schedule_child_workflow_maps: vec![map_task.clone()],
                ..WorkflowTaskCommit::default()
            };

            run_postgres_sql(
            &url,
            &format!(
                "create function {schema}.durust_test_swallow_insert() returns trigger as $fn$ \
                 begin if new.workflow_id = '{prefix}/0' then return null; end if; return new; end; \
                 $fn$ language plpgsql; \
                 create trigger durust_test_swallow_insert before insert on \
                 {schema}.workflow_instances for each row \
                 execute function {schema}.durust_test_swallow_insert();",
                schema = quote_postgres_identifier(&schema),
                prefix = prefix,
            ),
        )
        .await;

            let err = backend
                .commit_workflow_task(claimed.claim.clone(), commit())
                .await
                .expect_err("a vanished child map item must not be silently skipped");
            match &err {
                durust::Error::Backend(message) => assert_eq!(
                    message,
                    &format!(
                        "child workflow map `{}`:{} item 0 could not be started: \
                     workflow instance `{prefix}/0` was deleted mid-transaction",
                        command_id.run_id, command_id.seq.0
                    )
                ),
                other => panic!("expected a backend error naming the ordinal, got {other:?}"),
            }
            // The whole commit rolled back, so nothing half-materialized: no
            // descriptor, no scheduled event, no partial fanout.
            assert_eq!(
                stream_history(&backend, parent_run_id.clone())
                    .await
                    .iter()
                    .map(|event| event.event_type)
                    .collect::<Vec<_>>(),
                vec![durust::HistoryEventType::WorkflowStarted],
            );

            run_postgres_sql(
                &url,
                &format!(
                    "drop trigger durust_test_swallow_insert on {schema}.workflow_instances;",
                    schema = quote_postgres_identifier(&schema),
                ),
            )
            .await;
            backend
                .commit_workflow_task(claimed.claim, commit())
                .await
                .expect("the retried commit starts the child the trigger had swallowed");
            dispatch_child_map_starts(&backend).await;
            let mut started = BTreeSet::new();
            for index in 0..4 {
                let Some(child) = backend
                    .claim_workflow_task(
                        WorkerId::new(format!("map-vanish-{index}")),
                        workflow_claim_opts(child_queue),
                    )
                    .await
                    .unwrap()
                else {
                    break;
                };
                started.insert(child.workflow_id.0.clone());
            }
            assert_eq!(
                started,
                BTreeSet::from([format!("{prefix}/0"), format!("{prefix}/1")]),
            );
        },
    ));
}

/// The plain child-start sibling of the case above: `InlineChildStartOutcome`'s
/// vanished-row state used to share a variant with "this child event already
/// exists", and Postgres's commit path dropped both with the same `continue`.
///
/// The consequence was worse than the map one it sat next to. Postgres starts
/// plain children *inline*, so a silently dropped start appended no
/// `ChildWorkflowStarted`, no `ChildWorkflowFailed`, and left no outbox row to
/// retry from — the parent simply waited forever with nothing to observe. The
/// variant is now `Vanished` and every site raises through one helper.
///
/// Reached the same contrived way as the map case, and for the same reason: no
/// in-process test can commit a `workflow_instances` delete between two
/// statements of a transaction it does not drive.
#[cfg(feature = "postgres")]
#[test]
fn postgres_plain_child_start_vanishing_mid_transaction_fails_loudly_when_configured() {
    block_on_tokio(with_postgres_schema(
        "Postgres vanishing child start test",
        "childvanish",
        |backend, url, schema| async move {
            let child_id = "wf/child-vanish/child";
            let client = Client::new(backend.clone());
            let parent_run_id = client
                .start_workflow::<workflow>("wf/child-vanish", "child-vanish-parent", input(1))
                .await
                .unwrap();
            let claimed = backend
                .claim_workflow_task(
                    WorkerId::new("child-vanish-scheduler"),
                    workflow_claim_opts("child-vanish-parent"),
                )
                .await
                .unwrap()
                .expect("parent workflow task");
            let command_id = durust::command_id(&parent_run_id, 1);
            let commit = || WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::ChildWorkflowStartRequested(
                        durust::ChildWorkflowStartRequested {
                            command_id: command_id.clone(),
                            workflow_type: WorkflowType::new("conformance.workflow", 1),
                            workflow_id: durust::WorkflowId::new(child_id),
                            task_queue: TaskQueue::new("child-vanish-children"),
                            input: durust::encode_payload(&Input { value: 1 }).unwrap(),
                            parent_close_policy: durust::ParentClosePolicy::Abandon,
                            fingerprint: durust::child_workflow_fingerprint(
                                WorkflowType::new("conformance.workflow", 1),
                                durust::WorkflowId::new(child_id),
                                durust::payload_digest(
                                    &durust::encode_payload(&Input { value: 1 }).unwrap(),
                                ),
                                TaskQueue::new("child-vanish-children"),
                                durust::ParentClosePolicy::Abandon,
                            ),
                        },
                    ),
                )],
                start_child_workflows: vec![durust::ChildStartOutboxMessage {
                    command_id: command_id.clone(),
                    workflow_type: WorkflowType::new("conformance.workflow", 1),
                    workflow_id: durust::WorkflowId::new(child_id),
                    task_queue: TaskQueue::new("child-vanish-children"),
                    input: durust::encode_payload(&Input { value: 1 }).unwrap(),
                    parent_close_policy: durust::ParentClosePolicy::Abandon,
                    child_map_item: None,
                }],
                ..WorkflowTaskCommit::default()
            };

            run_postgres_sql(
            &url,
            &format!(
                "create function {schema}.durust_test_swallow_child() returns trigger as $fn$ \
                 begin if new.workflow_id = '{child_id}' then return null; end if; return new; end; \
                 $fn$ language plpgsql; \
                 create trigger durust_test_swallow_child before insert on \
                 {schema}.workflow_instances for each row \
                 execute function {schema}.durust_test_swallow_child();",
                schema = quote_postgres_identifier(&schema),
                child_id = child_id,
            ),
        )
        .await;

            let err = backend
                .commit_workflow_task(claimed.claim.clone(), commit())
                .await
                .expect_err("a vanished plain child start must not be silently dropped");
            match &err {
                durust::Error::Backend(message) => assert_eq!(
                    message,
                    &format!(
                        "child workflow `{}`:{} could not be started: \
                     workflow instance `{child_id}` was deleted mid-transaction",
                        command_id.run_id, command_id.seq.0
                    )
                ),
                other => panic!("expected a backend error naming the child, got {other:?}"),
            }
            assert_eq!(
                stream_history(&backend, parent_run_id.clone())
                    .await
                    .iter()
                    .map(|event| event.event_type)
                    .collect::<Vec<_>>(),
                vec![durust::HistoryEventType::WorkflowStarted],
                "the whole commit must roll back",
            );

            run_postgres_sql(
                &url,
                &format!(
                    "drop trigger durust_test_swallow_child on {schema}.workflow_instances;",
                    schema = quote_postgres_identifier(&schema),
                ),
            )
            .await;
            backend
                .commit_workflow_task(claimed.claim, commit())
                .await
                .expect("the retried commit starts the child the trigger had swallowed");
            assert!(
                stream_history(&backend, parent_run_id)
                    .await
                    .iter()
                    .any(|event| matches!(event.data, HistoryEventData::ChildWorkflowStarted(_))),
                "the retry appends the started fact the first attempt could not",
            );
        },
    ));
}

/// Row 6G's upgrade half: a database written *before* empty manifests completed
/// at descriptor creation keeps its stalled maps unless something re-steps them.
///
/// `DescriptorCreated` is only stepped as a descriptor is created, so the fix
/// never reaches a descriptor that already exists, and there is no operator
/// recovery short of hand-writing the terminal history event. Both SQL
/// providers therefore repair at open.
///
/// The stalled state is reconstructed rather than synthesized: an empty map is
/// scheduled through the ordinary commit path, and then only the *completion*
/// is undone — the terminal event deleted, the tail rewound, `completed` reset.
/// That is byte-for-byte the row a pre-upgrade database holds, because the
/// descriptor was written by the same code either way.
#[cfg(feature = "postgres")]
#[test]
fn postgres_repairs_a_pre_upgrade_stalled_empty_map_at_open_when_configured() {
    block_on_tokio(with_postgres_schema(
        "Postgres empty-map upgrade repair test",
        "maprepair",
        |backend, url, schema| async move {
            let quoted = quote_postgres_identifier(&schema);

            let (run_id, _command_id, _opts, scheduled) = schedule_activity_map(
                &backend,
                "wf/map-repair",
                "map-repair-workflows",
                "map-repair-activities",
                0,
                2,
                durust::RetryPolicy::none(),
                None,
            )
            .await;
            scheduled.expect("the empty map completes when it is scheduled");
            assert_eq!(stream_history(&backend, run_id.clone()).await.len(), 3);

            // Rewind to the pre-upgrade shape: descriptor open, no terminal fact,
            // tail back at the scheduling event, parent asleep.
            run_postgres_sql(
                &url,
                &format!(
                    "delete from {quoted}.history_events \
                   where run_id = '{run_id}' and event_type = 'activity_map_completed'; \
                 update {quoted}.workflow_instances \
                   set current_event_id = 2, ready_reason = null where run_id = '{run_id}'; \
                 update {quoted}.activity_maps set completed = false where run_id = '{run_id}'; \
                 delete from {quoted}.meta where key = 'empty_map_repair_done';",
                    run_id = run_id.0,
                ),
            )
            .await;
            assert_eq!(
                stream_history(&backend, run_id.clone()).await.len(),
                2,
                "the rewind must reproduce the stall"
            );
            assert!(
                backend
                    .claim_workflow_task(
                        WorkerId::new("map-repair-stalled"),
                        workflow_claim_opts("map-repair-workflows"),
                    )
                    .await
                    .unwrap()
                    .is_none(),
                "a pre-upgrade stalled map leaves its parent asleep"
            );

            let reopened = PostgresBackend::connect_with_config(
                PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
            )
            .await
            .unwrap();
            assert_eq!(
                stream_history(&reopened, run_id.clone())
                    .await
                    .iter()
                    .map(|event| event.event_type)
                    .collect::<Vec<_>>(),
                vec![
                    durust::HistoryEventType::WorkflowStarted,
                    durust::HistoryEventType::ActivityMapScheduled,
                    durust::HistoryEventType::ActivityMapCompleted,
                ],
            );
            let woken = reopened
                .claim_workflow_task(
                    WorkerId::new("map-repair-woken"),
                    workflow_claim_opts("map-repair-workflows"),
                )
                .await
                .unwrap()
                .expect("the repaired map wakes its parent");
            assert_eq!(
                woken.reason,
                durust::WorkflowTaskReason::ActivityMapCompleted
            );
            assert_eq!(woken.replay_target_event_id, EventId(3));

            // One-shot: the repair records itself in `meta`, and `migrate` reads that
            // marker in the query it already issues for `schema_version`, so every
            // later connect pays nothing for the unindexed descriptor scan.
            let again = PostgresBackend::connect_with_config(
                PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
            )
            .await
            .unwrap();
            assert_eq!(stream_history(&again, run_id).await.len(), 3);
            assert_eq!(
                postgres_scalar(
                    &url,
                    &format!("select value from {quoted}.meta where key = 'empty_map_repair_done'"),
                )
                .await,
                Some(1),
            );
        },
    ));
}

/// A descriptor this pass cannot interpret at all must not stop the process from
/// starting either — the other half of the class the doc comment claims.
///
/// A `task` blob that does not decode aborted `connect_with_config` outright.
/// The population this pass exists to process is by definition rows written by
/// an older binary, so cross-version serialization skew, a partial restore or
/// plain corruption all land here, and the failure is unrecoverable without
/// hand-editing the database because the process cannot start to fix itself.
///
/// The repairable descriptor beside it is still repaired, which is what makes
/// this per-descriptor isolation rather than "give up on the first error", and
/// the skip is counted rather than discarded.
#[cfg(feature = "postgres")]
#[test]
fn postgres_undecodable_stalled_map_is_skipped_not_fatal_when_configured() {
    block_on_tokio(with_postgres_schema(
        "Postgres undecodable map repair test",
        "mapundec",
        |backend, url, schema| async move {
            let quoted = quote_postgres_identifier(&schema);

            let (run_id, _command_id, _opts, scheduled) = schedule_activity_map(
                &backend,
                "wf/map-undec",
                "map-undec-workflows",
                "map-undec-activities",
                0,
                2,
                durust::RetryPolicy::none(),
                None,
            )
            .await;
            scheduled.expect("the empty map completes when it is scheduled");

            // Rewind to the pre-upgrade stall, then add a second stalled descriptor
            // whose task blob is not an `ActivityMapTask`. Its run row exists, so it
            // fails inside the pass rather than before it.
            run_postgres_sql(
                &url,
                &format!(
                    "delete from {quoted}.history_events \
                   where run_id = '{run_id}' and event_type = 'activity_map_completed'; \
                 update {quoted}.workflow_instances \
                   set current_event_id = 2, ready_reason = null where run_id = '{run_id}'; \
                 update {quoted}.activity_maps set completed = false where run_id = '{run_id}'; \
                 delete from {quoted}.meta where key = 'empty_map_repair_done'; \
                 insert into {quoted}.activity_maps \
                   (map_command_id, namespace, run_id, command_seq, task, item_count, \
                    next_ordinal, in_flight, completed) \
                 values ('{run_id}:9', 'default', '{run_id}', 9, '\\x00'::bytea, 0, 0, 0, false);",
                    run_id = run_id.0,
                ),
            )
            .await;

            let reopened = PostgresBackend::connect_with_config(
                PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
            )
            .await
            .expect("an undecodable descriptor must not abort backend construction");
            assert_eq!(
                stream_history(&reopened, run_id)
                    .await
                    .iter()
                    .map(|event| event.event_type)
                    .collect::<Vec<_>>(),
                vec![
                    durust::HistoryEventType::WorkflowStarted,
                    durust::HistoryEventType::ActivityMapScheduled,
                    durust::HistoryEventType::ActivityMapCompleted,
                ],
                "the repairable descriptor beside the bad one is still repaired",
            );
            assert_eq!(
                postgres_scalar(
                    &url,
                    &format!("select value from {quoted}.meta where key = 'empty_map_repair_done'"),
                )
                .await,
                Some(1),
                "the pass still records itself, so it does not re-scan forever",
            );
            assert_eq!(
                postgres_scalar(
                    &url,
                    &format!(
                        "select value from {quoted}.meta where key = 'empty_map_repair_skipped'"
                    ),
                )
                .await,
                Some(1),
                "and what it could not act on is counted, not discarded",
            );
        },
    ));
}

/// A descriptor whose `workflow_instances` row is gone must not stop the
/// process from starting.
///
/// Neither schema carries a foreign key from a descriptor to its run, so
/// ordinary retention pruning — `delete from workflow_instances where …`, the
/// most common maintenance anyone performs on a workflow store — leaves exactly
/// this orphan, as does a partial restore. Before the repair existed the row
/// was inert because nothing read it. Routing it through `parent_run_terminal`
/// raised `RunNotFound` from inside `connect_with_config`: no worker could
/// start, there was no flag to skip the repair, and every restart repeated it.
#[cfg(feature = "postgres")]
#[test]
fn postgres_orphaned_stalled_map_does_not_refuse_to_start_when_configured() {
    block_on_tokio(with_postgres_schema(
        "Postgres orphaned map repair test",
        "maporphan",
        |backend, url, schema| async move {
            let quoted = quote_postgres_identifier(&schema);

            let (run_id, _command_id, _opts, scheduled) = schedule_activity_map(
                &backend,
                "wf/map-orphan",
                "map-orphan-workflows",
                "map-orphan-activities",
                0,
                2,
                durust::RetryPolicy::none(),
                None,
            )
            .await;
            scheduled.expect("the empty map completes when it is scheduled");

            // Pre-upgrade stall, then prune the run out from under the descriptor.
            run_postgres_sql(
                &url,
                &format!(
                    "delete from {quoted}.history_events \
                   where run_id = '{run_id}' and event_type = 'activity_map_completed'; \
                 update {quoted}.activity_maps set completed = false where run_id = '{run_id}'; \
                 delete from {quoted}.meta where key = 'empty_map_repair_done'; \
                 delete from {quoted}.workflow_instances where run_id = '{run_id}';",
                    run_id = run_id.0,
                ),
            )
            .await;

            let reopened = PostgresBackend::connect_with_config(
                PostgresBackendConfig::new(url.clone()).schema(schema.clone()),
            )
            .await
            .expect("an orphaned descriptor must not abort backend construction");
            // Started, and the orphan is left exactly as it was found.
            assert_eq!(
                postgres_scalar(
                    &url,
                    &format!(
                        "select count(*) from {quoted}.activity_maps \
                     where run_id = '{run_id}' and completed = false"
                    ),
                )
                .await,
                Some(1),
                "an unrepairable descriptor is left untouched",
            );
            assert_eq!(
                postgres_scalar(
                    &url,
                    &format!("select value from {quoted}.meta where key = 'empty_map_repair_done'"),
                )
                .await,
                Some(1),
                "the pass still completes, so it does not re-scan on every connect",
            );
            drop(reopened);
        },
    ));
}

#[cfg(feature = "postgres")]
async fn postgres_scalar(database_url: &str, sql: &str) -> Option<i64> {
    let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    let value = client
        .query_opt(sql, &[])
        .await
        .unwrap()
        .map(|row| row.get::<_, i64>(0));
    connection.abort();
    value
}

/// Postgres starts plain children *inline*, so one commit can append both a
/// `ChildWorkflowStarted` and an empty map's terminal fact — and the map loop
/// assigns the post-commit ready reason after the child-start loop has already
/// set one.
///
/// The later assignment wins, so the reason names the last fact this commit
/// appended. That is the answer memory and SQLite give too — they route the
/// child through an outbox, so a map fact is the only same-commit reason they
/// can produce — which makes the overwrite a convergence rather than a
/// Postgres quirk. Nothing asserted either answer; the reason is not durable,
/// but it decides whether the parent is woken for the right cause, and silently
/// flipping it would be invisible.
#[cfg(feature = "postgres")]
#[test]
fn postgres_same_commit_map_completion_names_the_ready_reason_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres same-commit ready reason test",
        "mapreadyreason",
        |backend| async move {
            let client = Client::new(backend.clone());
            let run_id = client
                .start_workflow::<workflow>("wf/reason", "reason-parent", input(1))
                .await
                .unwrap();
            let parent_opts = workflow_claim_opts("reason-parent");
            let claimed = backend
                .claim_workflow_task(WorkerId::new("reason-scheduler"), parent_opts.clone())
                .await
                .unwrap()
                .expect("workflow task");
            let child_command = durust::command_id(&run_id, 1);
            let map_command = durust::command_id(&run_id, 2);
            let child_input = durust::encode_payload(&Input { value: 1 }).unwrap();
            let empty_manifest = durust::encode_activity_map_input_manifest(Vec::new(), 2).unwrap();
            let activity_name = ActivityName::new("conformance.echo");
            let activity_queue = TaskQueue::new("reason-activities");

            backend
                .commit_workflow_task(
                    claimed.claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![
                            durust::NewHistoryEvent::new(
                                HistoryEventData::ChildWorkflowStartRequested(
                                    durust::ChildWorkflowStartRequested {
                                        command_id: child_command.clone(),
                                        workflow_type: WorkflowType::new("conformance.workflow", 1),
                                        workflow_id: durust::WorkflowId::new("wf/reason/child"),
                                        task_queue: TaskQueue::new("reason-children"),
                                        input: child_input.clone(),
                                        parent_close_policy: durust::ParentClosePolicy::Abandon,
                                        fingerprint: durust::child_workflow_fingerprint(
                                            WorkflowType::new("conformance.workflow", 1),
                                            durust::WorkflowId::new("wf/reason/child"),
                                            durust::payload_digest(&child_input),
                                            TaskQueue::new("reason-children"),
                                            durust::ParentClosePolicy::Abandon,
                                        ),
                                    },
                                ),
                            ),
                            durust::NewHistoryEvent::new(HistoryEventData::ActivityMapScheduled(
                                durust::ActivityMapScheduled {
                                    command_id: map_command.clone(),
                                    activity_name: activity_name.clone(),
                                    task_queue: activity_queue.clone(),
                                    retry_policy: durust::RetryPolicy::none(),
                                    start_to_close_timeout: None,
                                    heartbeat_timeout: None,
                                    input_manifest: empty_manifest.clone(),
                                    result_manifest_name: "empty".to_owned(),
                                    max_in_flight: 2,
                                    fingerprint: durust::activity_map_fingerprint(
                                        activity_name.clone(),
                                        durust::payload_digest(&empty_manifest),
                                        "empty".to_owned(),
                                        2,
                                        "sha256:test-options".to_owned(),
                                    ),
                                },
                            )),
                        ],
                        start_child_workflows: vec![durust::ChildStartOutboxMessage {
                            command_id: child_command,
                            workflow_type: WorkflowType::new("conformance.workflow", 1),
                            workflow_id: durust::WorkflowId::new("wf/reason/child"),
                            task_queue: TaskQueue::new("reason-children"),
                            input: child_input,
                            parent_close_policy: durust::ParentClosePolicy::Abandon,
                            child_map_item: None,
                        }],
                        schedule_activity_maps: vec![ActivityMapTask {
                            map_command_id: map_command,
                            activity_name,
                            task_queue: activity_queue,
                            retry_policy: durust::RetryPolicy::none(),
                            start_to_close_timeout: None,
                            heartbeat_timeout: None,
                            input_manifest: empty_manifest,
                            result_manifest_name: "empty".to_owned(),
                            max_in_flight: 2,
                        }],
                        ..WorkflowTaskCommit::default()
                    },
                )
                .await
                .unwrap();

            assert_eq!(
                stream_history(&backend, run_id)
                    .await
                    .iter()
                    .map(|event| event.event_type)
                    .collect::<Vec<_>>(),
                vec![
                    durust::HistoryEventType::WorkflowStarted,
                    durust::HistoryEventType::ChildWorkflowStartRequested,
                    durust::HistoryEventType::ActivityMapScheduled,
                    durust::HistoryEventType::ChildWorkflowStarted,
                    durust::HistoryEventType::ActivityMapCompleted,
                ],
            );
            let ready = backend
                .claim_workflow_task(WorkerId::new("reason-ready"), parent_opts)
                .await
                .unwrap()
                .expect("the parent is woken by its own commit");
            assert_eq!(
                ready.reason,
                durust::WorkflowTaskReason::ActivityMapCompleted,
                "the last fact this commit appended names the reason, not the inline child start",
            );
        },
    ));
}

/// Postgres's descriptor insert is `on conflict(map_command_id) do nothing`,
/// which makes it genuinely idempotent — and would therefore report success
/// against a descriptor it did not write. That became consequential when
/// `DescriptorCreated` gained the ability to append a history fact: without the
/// row-count tripwire Postgres steps the **stale** descriptor and appends a
/// spurious terminal fact, where SQLite's plain insert raises.
///
/// Unreachable behind `expected_tail_event_id`, which fences a replayed commit
/// before it reaches the insert, so the stale row is built by contrivance — but
/// by *rewinding a real commit*, the way the repair tests do, so the descriptor
/// is well-formed and the assertion is behavioural rather than textual. A
/// hand-written placeholder blob would make the mutation die on a decode error
/// instead, which is the same user-visible outcome with and without the guard
/// and therefore proves nothing.
///
/// Both halves, matching row 6L's: the stale descriptor is rejected, and
/// removing it lets the same commit through.
#[cfg(feature = "postgres")]
#[test]
fn postgres_stale_map_descriptor_is_rejected_not_silently_reused_when_configured() {
    block_on_tokio(with_postgres_schema(
        "Postgres stale map descriptor test",
        "stalemapdesc",
        |backend, url, schema| async move {
            let quoted = quote_postgres_identifier(&schema);

            // A real descriptor, written by the ordinary commit path, then rewound
            // so the run is back at the scheduling event with the descriptor still
            // present. That is a well-formed stale row.
            let (run_id, command_id, _opts, scheduled) = schedule_activity_map(
                &backend,
                "wf/stale-desc",
                "stale-desc-workflows",
                "stale-desc-activities",
                0,
                2,
                durust::RetryPolicy::none(),
                None,
            )
            .await;
            scheduled.expect("the empty map completes when it is scheduled");
            run_postgres_sql(
                &url,
                &format!(
                    "delete from {quoted}.history_events \
                   where run_id = '{run_id}' and event_type = 'activity_map_completed'; \
                 update {quoted}.workflow_instances \
                   set current_event_id = 1, ready_reason = 'workflow_started' \
                   where run_id = '{run_id}'; \
                 delete from {quoted}.history_events \
                   where run_id = '{run_id}' and event_type = 'activity_map_scheduled'; \
                 update {quoted}.activity_maps set completed = false where run_id = '{run_id}';",
                    run_id = run_id.0,
                ),
            )
            .await;

            let commit = |claim| {
                let empty_manifest =
                    durust::encode_activity_map_input_manifest(Vec::new(), 2).unwrap();
                let activity_name = ActivityName::new("conformance.echo");
                let task_queue = TaskQueue::new("stale-desc-activities");
                backend.commit_workflow_task(
                    claim,
                    WorkflowTaskCommit {
                        expected_tail_event_id: EventId(1),
                        append_events: vec![durust::NewHistoryEvent::new(
                            HistoryEventData::ActivityMapScheduled(durust::ActivityMapScheduled {
                                command_id: command_id.clone(),
                                activity_name: activity_name.clone(),
                                task_queue: task_queue.clone(),
                                retry_policy: durust::RetryPolicy::none(),
                                start_to_close_timeout: None,
                                heartbeat_timeout: None,
                                input_manifest: empty_manifest.clone(),
                                result_manifest_name: "mapped".to_owned(),
                                max_in_flight: 2,
                                fingerprint: durust::activity_map_fingerprint(
                                    activity_name.clone(),
                                    durust::payload_digest(&empty_manifest),
                                    "mapped".to_owned(),
                                    2,
                                    "sha256:test-options".to_owned(),
                                ),
                            }),
                        )],
                        schedule_activity_maps: vec![ActivityMapTask {
                            map_command_id: command_id.clone(),
                            activity_name,
                            task_queue,
                            retry_policy: durust::RetryPolicy::none(),
                            start_to_close_timeout: None,
                            heartbeat_timeout: None,
                            input_manifest: empty_manifest,
                            result_manifest_name: "mapped".to_owned(),
                            max_in_flight: 2,
                        }],
                        ..WorkflowTaskCommit::default()
                    },
                )
            };

            let claimed = backend
                .claim_workflow_task(
                    WorkerId::new("stale-desc-scheduler"),
                    workflow_claim_opts("stale-desc-workflows"),
                )
                .await
                .unwrap()
                .expect("workflow task");
            let claim = claimed.claim.clone();
            commit(claim)
                .await
                .expect_err("a stale descriptor must not be silently reused");
            // The behaviour, not the message: without the tripwire this commit is
            // accepted and appends an `ActivityMapCompleted` for a descriptor it
            // never created. The stale row is left open precisely so the engine's
            // terminal-absorbing rule cannot mask that — a `completed` stale row
            // would produce no effects and the mutation would look harmless.
            assert_eq!(
                stream_history(&backend, run_id.clone())
                    .await
                    .iter()
                    .map(|event| event.event_type)
                    .collect::<Vec<_>>(),
                vec![durust::HistoryEventType::WorkflowStarted],
                "the whole commit must roll back, appending nothing",
            );

            // Recovery: with the stale row gone the same commit goes through and
            // the map completes exactly once.
            run_postgres_sql(
                &url,
                &format!(
                    "delete from {quoted}.activity_maps where run_id = '{run_id}';",
                    run_id = run_id.0,
                ),
            )
            .await;
            commit(claimed.claim)
                .await
                .expect("the retried commit creates the descriptor the stale row blocked");
            assert_eq!(
                stream_history(&backend, run_id)
                    .await
                    .iter()
                    .map(|event| event.event_type)
                    .collect::<Vec<_>>(),
                vec![
                    durust::HistoryEventType::WorkflowStarted,
                    durust::HistoryEventType::ActivityMapScheduled,
                    durust::HistoryEventType::ActivityMapCompleted,
                ],
            );
        },
    ));
}

#[cfg(feature = "postgres")]
async fn run_postgres_sql(database_url: &str, sql: &str) {
    let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    client.batch_execute(sql).await.unwrap();
    connection.abort();
}

/// The closing commit both halves of the terminal-wait coverage are built on:
/// it starts a timer, records that timer's wait, and closes the run in one
/// transaction, so the commit creates a wait and makes it unreachable at the
/// same instant. Shared with the Postgres batch-path case so both of that
/// provider's commit paths apply a byte-identical commit.
fn timer_wait_closing_commit(
    run_id: &durust::RunId,
    fire_at: durust::TimestampMs,
) -> (durust::WaitId, WorkflowTaskCommit) {
    let command_id = durust::command_id(run_id, 1);
    let wait_id = durust::WaitId::new(format!("{}:{}:timer", command_id.run_id, command_id.seq.0));
    let commit = WorkflowTaskCommit {
        expected_tail_event_id: EventId(1),
        append_events: vec![
            durust::NewHistoryEvent::new(HistoryEventData::TimerStarted(durust::TimerStarted {
                command_id: command_id.clone(),
                fire_at,
                fingerprint: durust::timer_fingerprint("sleep_until", fire_at),
            })),
            durust::NewHistoryEvent::new(HistoryEventData::WorkflowCompleted {
                result: durust::encode_payload(&()).unwrap(),
            }),
        ],
        upsert_waits: vec![durust::WaitRecord {
            wait_id: wait_id.clone(),
            run_id: run_id.clone(),
            command_id,
            kind: durust::WaitKind::Timer,
            key: "timer".to_owned(),
            ready_at: Some(fire_at),
        }],
        ..WorkflowTaskCommit::default()
    };
    (wait_id, commit)
}

/// A run's waits are deleted by the same transaction that closes it
/// (`SPEC.md` §19.1), so operational storage does not grow with closed runs and
/// maintenance scans do not pay for them.
///
/// The observable consequence is starvation, not corruption — corruption is
/// what the due-timer terminal guard prevents, and that is pinned by
/// `a_stray_timer_wait_never_fires`. A leftover wait is
/// still selected by the due scan, still spends one of its `limit` slots, and
/// is only then refused by the guard — which leaves it in place (`SPEC.md` §14;
/// see `a_skipped_stray_wait_still_spends_the_next_scans_budget`), so it spends
/// a slot on every later sweep too and a fleet's dead waits crowd out the
/// timers that could actually fire. Two runs, one due wait each,
/// `limit: 1`: with the cleanup the live run's timer fires, and without it the
/// closed run's wait takes the only slot.
///
/// Every Rust provider selects the leftover row before it checks the run, so
/// this is a real detector on all three. (The equivalent TypeScript case is
/// blind on Postgres, whose scan carries `terminal = false` as a predicate
/// *inside* the limited query; the Rust Postgres scan filters only on
/// namespace, kind and `ready_at_ms`, then re-reads each row's run.)
///
/// Each provider gets its own backend rather than a case in the shared
/// `provider_conformance` list, because this is a *budget* observation: the
/// single scan slot has to be contended by exactly these two runs. On the
/// shared backend an unrelated case's surviving timer wait can take the slot
/// instead — and under the very mutation this case exists to catch, terminal
/// cleanup stops running for every earlier case too, so the shared backend
/// accumulates precisely the leftovers that would make the failure
/// unattributable.
async fn close_a_run_holding_a_due_timer_wait<B>(
    backend: &B,
    prefix: &str,
) -> (durust::RunId, durust::WaitId)
where
    B: DurableBackend,
{
    let queue = format!("{prefix}-terminal-cleanup-workflows");
    let run_id = Client::new(backend.clone())
        .start_workflow::<workflow>(format!("wf/{prefix}-closed-timer"), queue.clone(), input(1))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-closing-timer-scheduler")),
            workflow_claim_opts(&queue),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let (wait_id, commit) = timer_wait_closing_commit(&run_id, durust::TimestampMs(1_000));
    let outcome = backend
        .commit_workflow_task(claimed.claim, commit)
        .await
        .expect("a commit that starts a timer and closes its own run must be accepted");
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(3)
        },
    );
    (run_id, wait_id)
}

async fn only_the_live_runs_timer_spends_the_due_scan_budget<B>(
    backend: &B,
    prefix: &str,
    closed_wait_id: &durust::WaitId,
) where
    B: DurableBackend,
{
    let queue = format!("{prefix}-terminal-cleanup-workflows");
    let live_run_id = Client::new(backend.clone())
        .start_workflow::<workflow>(format!("wf/{prefix}-live-timer"), queue.clone(), input(1))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-live-timer-scheduler")),
            workflow_claim_opts(&queue),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let command_id = durust::command_id(&live_run_id, 1);
    let live_wait_id =
        durust::WaitId::new(format!("{}:{}:timer", command_id.run_id, command_id.seq.0));
    // The closed run's leftover has to be picked first under both selection
    // orders in play — key order in the memory provider's wait map, `order by
    // ready_at_ms asc, wait_id asc` in the SQL providers — or it never contends
    // for the slot and this case cannot fail. The earlier `ready_at` covers the
    // SQL providers; this covers the memory provider, and fails loudly if run
    // id generation ever stops sorting the first-started run first.
    assert!(
        closed_wait_id.0 < live_wait_id.0,
        "the closed run's leftover wait must sort before the live run's, or it never \
         contends for the scan slot: `{}` vs `{}`",
        closed_wait_id.0,
        live_wait_id.0,
    );
    let fire_at = durust::TimestampMs(2_000);
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::TimerStarted(durust::TimerStarted {
                        command_id: command_id.clone(),
                        fire_at,
                        fingerprint: durust::timer_fingerprint("sleep_until", fire_at),
                    }),
                )],
                upsert_waits: vec![durust::WaitRecord {
                    wait_id: live_wait_id,
                    run_id: live_run_id.clone(),
                    command_id,
                    kind: durust::WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(fire_at),
                }],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        },
    );

    let fired = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(10_000),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        fired.fired, 1,
        "a closed run's leftover wait must not spend the due-timer scan's only slot; fired {}",
        fired.fired,
    );
    assert_eq!(
        stream_history(backend, live_run_id)
            .await
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            durust::HistoryEventType::WorkflowStarted,
            durust::HistoryEventType::TimerStarted,
            durust::HistoryEventType::TimerFired,
        ],
        "the live run's timer is the one that must have fired",
    );
}

#[test]
fn memory_terminal_cleanup_deletes_a_closed_runs_waits() {
    block_on(async {
        let backend = MemoryBackend::new();
        let (_, closed_wait_id) =
            close_a_run_holding_a_due_timer_wait(&backend, "memory-cleanup-waits").await;
        only_the_live_runs_timer_spends_the_due_scan_budget(
            &backend,
            "memory-cleanup-waits",
            &closed_wait_id,
        )
        .await;
    });
}

#[test]
fn sqlite_terminal_cleanup_deletes_a_closed_runs_waits_across_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("terminal-cleanup-waits.sqlite3");
        let backend = SqliteBackend::open(&path).unwrap();
        let (_, closed_wait_id) =
            close_a_run_holding_a_due_timer_wait(&backend, "sqlite-cleanup-waits").await;
        drop(backend);

        // Reopened before the scan: the delete has to be on disk, or a
        // restarted provider inherits the dead wait it was supposed to have
        // dropped and the storage bound is only true for one process.
        let reopened = SqliteBackend::open(&path).unwrap();
        only_the_live_runs_timer_spends_the_due_scan_budget(
            &reopened,
            "sqlite-cleanup-waits",
            &closed_wait_id,
        )
        .await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_terminal_cleanup_deletes_a_closed_runs_waits_when_configured() {
    block_on_tokio(with_postgres(
        "Postgres terminal wait cleanup",
        "cleanupwaits",
        |backend| async move {
            let (_, closed_wait_id) =
                close_a_run_holding_a_due_timer_wait(&backend, "pg-cleanup-waits").await;
            only_the_live_runs_timer_spends_the_due_scan_budget(
                &backend,
                "pg-cleanup-waits",
                &closed_wait_id,
            )
            .await;
        },
    ));
}

/// Postgres is the one provider with two commit implementations: the scalar
/// `commit_workflow_task` and the set-based path a multi-run
/// `commit_workflow_tasks` batch takes when more than one of its commits is
/// simple-batch eligible. Both must delete a closed run's wait rows, and the
/// budget case above can only reach the scalar one.
///
/// Asserted on the rows themselves rather than on a scan, because that is the
/// §19.1 invariant directly — operational storage does not grow with closed
/// runs — and because a row count cannot be satisfied by a scan that merely
/// skips what it finds.
///
/// Non-vacuity is established by mutation, not by inspection: this test cannot
/// see which path the batch took, but deleting the batch path's cleanup call
/// alone fails it, which is only possible if the batch reached that path.
#[cfg(feature = "postgres")]
#[test]
fn postgres_closing_commits_delete_wait_rows_on_both_commit_paths_when_configured() {
    block_on_tokio(with_postgres_schema(
        "Postgres terminal wait row cleanup",
        "waitrows",
        |backend, url, schema| async move {
            let (scalar_run_id, _) =
                close_a_run_holding_a_due_timer_wait(&backend, "pg-wait-rows").await;
            assert_eq!(
                postgres_wait_row_count(&url, &schema, &scalar_run_id).await,
                0,
                "the scalar commit path must delete the closed run's wait rows",
            );

            // Two runs, one commit each, both simple-batch eligible (no maps, no
            // child starts, no cancels, and only timer/terminal events), so the
            // batch routes through the set-based apply instead of falling back to
            // the scalar path per item.
            let queue = "pg-wait-rows-batch-workflows";
            let client = Client::new(backend.clone());
            let mut claims = Vec::new();
            for index in 0..2 {
                let run_id = client
                    .start_workflow::<workflow>(
                        format!("wf/pg-wait-rows-batch/{index}"),
                        queue,
                        input(1),
                    )
                    .await
                    .unwrap();
                let claimed = backend
                    .claim_workflow_task(
                        WorkerId::new(format!("pg-wait-rows-batch-{index}")),
                        workflow_claim_opts(queue),
                    )
                    .await
                    .unwrap()
                    .expect("workflow task");
                assert_eq!(claimed.run_id, run_id);
                claims.push(claimed);
            }
            let results = backend
                .commit_workflow_tasks(WorkflowTaskCommitBatch {
                    commits: claims
                        .iter()
                        .map(|claimed| WorkflowTaskCommitInput {
                            claim: claimed.claim.clone(),
                            commit: timer_wait_closing_commit(
                                &claimed.run_id,
                                durust::TimestampMs(1_000),
                            )
                            .1,
                        })
                        .collect(),
                })
                .await
                .unwrap();
            assert_eq!(results.len(), 2);
            for result in &results {
                assert_eq!(
                    *result.result.as_ref().unwrap(),
                    CommitOutcome::Committed {
                        new_tail_event_id: EventId(3)
                    },
                );
            }
            for claimed in &claims {
                assert_eq!(
                    postgres_wait_row_count(&url, &schema, &claimed.run_id).await,
                    0,
                    "the set-based batch commit path must delete closed run `{}`'s wait rows",
                    claimed.run_id,
                );
            }
        },
    ));
}

#[cfg(feature = "postgres")]
async fn postgres_wait_row_count(database_url: &str, schema: &str, run_id: &durust::RunId) -> i64 {
    let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one(
            &format!(
                "select count(*) from {}.active_waits where run_id = $1",
                quote_postgres_identifier(schema)
            ),
            &[&run_id.0],
        )
        .await
        .unwrap();
    connection.abort();
    row.get(0)
}

/// A due-timer scan never appends `TimerFired` to a run that has already
/// reached a terminal event (`SPEC.md` §14 timers, §19.1 cleanup).
///
/// Terminal cleanup deletes a closed run's waits, so a stray wait against a
/// closed run is state that cleanup could not reach. This case forges exactly
/// that state and requires the provider to refuse it: a `TimerFired` appended
/// after the terminal event corrupts a history every replay, audit and cleanup
/// path assumes is finished, and the run is not even resurrected by it — the
/// next claim still refuses a closed run, so the only outcome is the
/// corruption.
///
/// **How the state is forged, and why it has to be.** The obvious
/// construction — commit the wait in the same task that closes the run — is
/// vacuous, because that is precisely the commit terminal cleanup deletes the
/// wait from; the scan would find nothing and the case would pass with the
/// guard deleted. It is forged here by committing the wait from a *second,
/// live* run after the first has closed, naming the closed run in the record.
/// Every provider stores the record's own `run_id`, so the row outlives a
/// cleanup that already ran. This is the integration-level form of the
/// `force_terminal` helpers the provider unit tests use, which poke the
/// terminal flag directly because every real terminal transition would have
/// cleaned up first.
///
/// `only_the_live_runs_timer_spends_the_due_scan_budget` pins the cleanup;
/// this pins the guard behind it. Reverting either fix leaves the other case
/// green, which is the point of having both.
///
/// What this case asserts is the guard's contract — no `TimerFired` past the
/// terminal event. The skipped wait's *fate* is a separate contract, asserted
/// separately by `a_skipped_stray_wait_still_spends_the_next_scans_budget`,
/// which every caller of this helper runs next.
async fn forge_a_stray_timer_wait_against_a_closed_run<B>(
    backend: &B,
    prefix: &str,
) -> durust::RunId
where
    B: DurableBackend,
{
    let queue = format!("{prefix}-stray-wait-workflows");
    let client = Client::new(backend.clone());
    let closed_run_id = client
        .start_workflow::<workflow>(format!("wf/{prefix}-stray-closed"), queue.clone(), input(1))
        .await
        .unwrap();
    let closing = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-stray-closer")),
            workflow_claim_opts(&queue),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let outcome = backend
        .commit_workflow_task(
            closing.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::WorkflowCompleted {
                        result: durust::encode_payload(&()).unwrap(),
                    },
                )],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        },
    );

    client
        .start_workflow::<workflow>(
            format!("wf/{prefix}-stray-injector"),
            queue.clone(),
            input(1),
        )
        .await
        .unwrap();
    let injector = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-stray-injector")),
            workflow_claim_opts(&queue),
        )
        .await
        .unwrap()
        .expect("workflow task");
    let stray_command_id = durust::command_id(&closed_run_id, 1);
    let injected = backend
        .commit_workflow_task(
            injector.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                upsert_waits: vec![durust::WaitRecord {
                    wait_id: durust::WaitId::new(format!(
                        "{}:{}:timer",
                        stray_command_id.run_id, stray_command_id.seq.0
                    )),
                    run_id: closed_run_id.clone(),
                    command_id: stray_command_id,
                    kind: durust::WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(durust::TimestampMs(1_000)),
                }],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .expect("the forging commit is a live run's own commit and must be accepted");
    assert_eq!(
        injected,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(1)
        },
    );
    closed_run_id
}

async fn a_stray_timer_wait_never_fires<B>(backend: &B, closed_run_id: durust::RunId)
where
    B: DurableBackend,
{
    let fired = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(10_000),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(
        fired.fired, 0,
        "a closed run's timer wait must not fire; fired {}",
        fired.fired,
    );
    assert_eq!(
        stream_history(backend, closed_run_id)
            .await
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            durust::HistoryEventType::WorkflowStarted,
            durust::HistoryEventType::WorkflowCompleted,
        ],
        "nothing may be appended past the run's own terminal event",
    );
}

/// A wait the due-timer scan skipped for the terminal guard is *skipped, not
/// deleted* (`SPEC.md` §14): "deleting it would hide the defect the cleanup is
/// supposed to have prevented".
///
/// Asserted through the scan's own budget, because that is the one consequence
/// of the row's survival that every provider exposes through `DurableBackend`
/// alone: a wait the scan skipped is still there for the next sweep to select,
/// so it still spends one of that sweep's `limit` slots; a wait the scan
/// deleted does not. The caller has already run one full sweep over the stray
/// (`a_stray_timer_wait_never_fires`), so on a provider that deletes, the row
/// is already gone before this starts and the live run's timer takes the slot.
///
/// This runs against the *same* forged state as the guard case rather than
/// forging its own, so the two cases assert two different contracts about one
/// row: the guard refuses the append, and the row survives the refusal.
///
/// The starvation being pinned here is deliberate, and is not a licence for
/// leftovers to accumulate. It is reachable only once §19.1's terminal cleanup
/// has already failed — every `TerminalCleanup` variant deletes the run's waits
/// unconditionally — and it is exactly what makes
/// `only_the_live_runs_timer_spends_the_due_scan_budget` a detector of that
/// failure. A provider that pushed the terminal check into the selecting query
/// as a predicate would keep the leftover out of the budget and go blind on
/// that case instead; TypeScript's Postgres provider makes that trade, and no
/// Rust provider does.
///
/// Non-vacuity is asserted rather than assumed: the live run's timer must fire
/// on the unbudgeted sweep at the end, or `fired == 0` above would be equally
/// satisfied by a live wait that was never due, never committed, or committed
/// against the wrong run.
async fn a_skipped_stray_wait_still_spends_the_next_scans_budget<B>(
    backend: &B,
    prefix: &str,
    closed_run_id: &durust::RunId,
) where
    B: DurableBackend,
{
    let queue = format!("{prefix}-stray-wait-workflows");
    let live_run_id = Client::new(backend.clone())
        .start_workflow::<workflow>(format!("wf/{prefix}-stray-live"), queue.clone(), input(1))
        .await
        .unwrap();
    let claimed = backend
        .claim_workflow_task(
            WorkerId::new(format!("{prefix}-stray-live-scheduler")),
            workflow_claim_opts(&queue),
        )
        .await
        .unwrap()
        .expect("workflow task");
    assert_eq!(
        claimed.run_id, live_run_id,
        "the live run must be the task claimed here, or the timer below is committed against \
         the wrong run and the final sweep proves nothing",
    );
    let command_id = durust::command_id(&live_run_id, 1);
    let live_wait_id =
        durust::WaitId::new(format!("{}:{}:timer", command_id.run_id, command_id.seq.0));
    let stray_command_id = durust::command_id(closed_run_id, 1);
    let stray_wait_id = durust::WaitId::new(format!(
        "{}:{}:timer",
        stray_command_id.run_id, stray_command_id.seq.0
    ));
    // The stray has to be selected first under both selection orders in play —
    // key order in the memory provider's wait map, `order by ready_at_ms asc,
    // wait_id asc` in the SQL providers — or it never contends for the single
    // slot and this case cannot fail. The earlier `ready_at` (1_000 against the
    // 2_000 below) covers the SQL providers; this covers the memory provider,
    // and fails loudly if run id generation ever stops sorting the
    // first-started run first.
    assert!(
        stray_wait_id.0 < live_wait_id.0,
        "the closed run's stray wait must sort before the live run's, or it never contends \
         for the scan slot: `{}` vs `{}`",
        stray_wait_id.0,
        live_wait_id.0,
    );
    let fire_at = durust::TimestampMs(2_000);
    let outcome = backend
        .commit_workflow_task(
            claimed.claim,
            WorkflowTaskCommit {
                expected_tail_event_id: EventId(1),
                append_events: vec![durust::NewHistoryEvent::new(
                    HistoryEventData::TimerStarted(durust::TimerStarted {
                        command_id: command_id.clone(),
                        fire_at,
                        fingerprint: durust::timer_fingerprint("sleep_until", fire_at),
                    }),
                )],
                upsert_waits: vec![durust::WaitRecord {
                    wait_id: live_wait_id,
                    run_id: live_run_id.clone(),
                    command_id,
                    kind: durust::WaitKind::Timer,
                    key: "timer".to_owned(),
                    ready_at: Some(fire_at),
                }],
                ..WorkflowTaskCommit::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            new_tail_event_id: EventId(2)
        },
    );

    let budgeted = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(10_000),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        budgeted.fired, 0,
        "the stray wait the previous sweep skipped must still be there to take this sweep's \
         only slot; a scan that deleted it instead would have let the live run's timer through. \
         fired {}",
        budgeted.fired,
    );

    let unbudgeted = backend
        .fire_due_timers(durust::FireDueTimersRequest {
            namespace: Namespace::default(),
            now: durust::TimestampMs(10_000),
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(
        unbudgeted.fired, 1,
        "the live run's timer was due all along and must fire once the budget is not the \
         binding constraint; fired {}",
        unbudgeted.fired,
    );
    assert_eq!(
        stream_history(backend, live_run_id)
            .await
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            durust::HistoryEventType::WorkflowStarted,
            durust::HistoryEventType::TimerStarted,
            durust::HistoryEventType::TimerFired,
        ],
        "the live run's timer is the one that must have fired",
    );
}

#[test]
fn memory_stray_timer_wait_never_fires_against_a_closed_run() {
    block_on(async {
        let backend = MemoryBackend::new();
        let closed_run_id =
            forge_a_stray_timer_wait_against_a_closed_run(&backend, "memory-stray").await;
        a_stray_timer_wait_never_fires(&backend, closed_run_id.clone()).await;
        a_skipped_stray_wait_still_spends_the_next_scans_budget(
            &backend,
            "memory-stray",
            &closed_run_id,
        )
        .await;
    });
}

#[test]
fn sqlite_stray_timer_wait_never_fires_against_a_closed_run_across_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stray-timer-wait.sqlite3");
        let backend = SqliteBackend::open(&path).unwrap();
        let closed_run_id =
            forge_a_stray_timer_wait_against_a_closed_run(&backend, "sqlite-stray").await;
        drop(backend);

        // Reopened before the scan, so the forged row and the closed run's
        // terminal flag both come off disk: a guard that only holds for the
        // process that wrote the row would not protect a restarted fleet.
        let reopened = SqliteBackend::open(&path).unwrap();
        a_stray_timer_wait_never_fires(&reopened, closed_run_id.clone()).await;
        a_skipped_stray_wait_still_spends_the_next_scans_budget(
            &reopened,
            "sqlite-stray",
            &closed_run_id,
        )
        .await;
    });
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_stray_timer_wait_never_fires_against_a_closed_run_when_configured() {
    block_on_tokio(with_postgres_schema(
        "Postgres stray timer wait guard",
        "straywait",
        |backend, url, schema| async move {
            let closed_run_id =
                forge_a_stray_timer_wait_against_a_closed_run(&backend, "pg-stray").await;
            a_stray_timer_wait_never_fires(&backend, closed_run_id.clone()).await;
            // The row itself, directly, on the one provider whose storage this test
            // file can read: the scan that just refused to fire it must also have
            // left it alone. The budget case below reaches the same conclusion
            // through the trait alone, on every provider.
            assert_eq!(
                postgres_wait_row_count(&url, &schema, &closed_run_id).await,
                1,
                "the sweep that skipped the stray wait must not have deleted it (`SPEC.md` §14)",
            );
            a_skipped_stray_wait_still_spends_the_next_scans_budget(
                &backend,
                "pg-stray",
                &closed_run_id,
            )
            .await;
        },
    ));
}
