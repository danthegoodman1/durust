# Durust

[Docs.rs](https://docs.rs/durust) | [Crates.io](https://crates.io/crates/durust)

Durust is a durable workflow runtime for Rust services.

Write async Rust workflows that survive crashes, restarts, timers, signals,
long waits, child workflows, version rollouts, and large fanout.

The workflow `name` is the durable identity stored in history and task indexes.
Keep it stable across Rust function renames or module moves; use `version` for
intentional workflow type changes.

```rust
#[durust::workflow(name = "orders.checkout", version = 1, query_state = OrderView)]
pub async fn checkout(input: CheckoutInput) -> durust::Result<CheckoutOutput> {
    let quote = durust::call_activity!(price_quote(input.quote()))
        .retry(RetryPolicy::exponential().max_attempts(5))
        .await?;

    let decision = durust::select! {
        approval = durust::signal::<Approval>("approved") => {
            ApprovalDecision::Approved(approval?)
        }

        cancel = durust::signal::<Cancel>("cancel") => {
            ApprovalDecision::Cancelled(cancel?)
        }

        _ = durust::sleep_until(input.approval_deadline) => {
            ApprovalDecision::TimedOut
        }
    };

    let approval = match decision {
        ApprovalDecision::Approved(approval) => approval,
        ApprovalDecision::Cancelled(cancel) => return Err(cancel.into()),
        ApprovalDecision::TimedOut => return Err(durust::Error::timeout("approval")),
    };

    let payment = durust::call_activity!(charge_card(input.charge(quote, approval)))
        .task_queue("payments")
        .idempotency_key(("charge", &input.order_id))
        .await?;

    let child = durust::child!(ship_order(input.ship(payment.id.clone())))
        .workflow_id(format!("ship/{}", input.order_id))
        .parent_close_policy(ParentClosePolicy::Cancel)
        .spawn()
        .await?;

    let shipment = child.result().await?;

    Ok(CheckoutOutput {
        order_id: input.order_id,
        payment_id: payment.id,
        shipment_id: shipment.id,
    })
}
```

## Contents

- [Why Durust](#why-durust)
- [How It Works](#how-it-works)
- [What Makes It Different](#what-makes-it-different)
  - [Workflow Cache First](#workflow-cache-first)
  - [Append-Journal Durability](#append-journal-durability)
  - [No Event History Limit](#no-event-history-limit)
  - [First-Class Map Reduce](#first-class-map-reduce)
  - [Payload Handling Is Provider-Owned](#payload-handling-is-provider-owned)
- [Worker Registration](#worker-registration)
- [Core Patterns](#core-patterns)
  - [Signals, Timers, And Select](#signals-timers-and-select)
  - [Workflow Time](#workflow-time)
  - [Bounded Fanout With Join](#bounded-fanout-with-join)
  - [Dynamic Fanout With Join All](#dynamic-fanout-with-join-all)
  - [Dynamic Races With Select All](#dynamic-races-with-select-all)
  - [Child Workflow: Spawn And Wait](#child-workflow-spawn-and-wait)
  - [Child Workflow: Spawn And Abandon](#child-workflow-spawn-and-abandon)
  - [Query Projection](#query-projection)
  - [Version Branches](#version-branches)
  - [Map Reduce](#map-reduce)
  - [Continue As New](#continue-as-new)
- [Payloads](#payloads)
- [Recovery Model](#recovery-model)
  - [The Commit Fence](#the-commit-fence)
- [Determinism](#determinism)
- [Durability Providers](#durability-providers)
  - [Cargo Features](#cargo-features)
- [Benchmarks](#benchmarks)
- [Upgrading](#upgrading)
- [Release Automation](#release-automation)
- [Examples](#examples)

## Why Durust

- Workflows are normal async Rust functions.
- Local variables stay in memory while a workflow is hot on a worker.
- Recovery reconstructs locals by streaming append-only history.
- Providers optimize persistence with append-journal writes and derived indexes.
- Payload storage is handled by providers, not workflow code.
- SQLite works for tests and local development.

Durust is built for services that need durable coordination, Rust control flow,
local worker performance, and provider choice.

## How It Works

Durust separates workflow execution from durability:

```text
workflow code
  ordinary async Rust with durable APIs

workflow cache
  pinned Rust futures kept alive until terminal state when possible

append history
  ordered facts needed to recover locals after crash or eviction

active indexes
  timers, signals, activity tasks, child completions, leases, ready queues

payload provider
  stores compact payload refs for values too large for hot rows
```

The happy path is fast because the workflow future remains hot in memory. When a
worker crashes or evicts a workflow, Durust recreates the future and streams
history in bounded chunks until it reaches the claimed tail.

## What Makes It Different

### Workflow Cache First

The workflow future stays alive on the worker until it completes, fails, is
cancelled, or is evicted. Most steady-state progress happens against a hot
in-memory future.

Durability is still authoritative. The cache is only a performance layer.

### Append-Journal Durability

Providers optimize accepted mutations around append-only writes and
derived indexes.

This keeps the happy path friendly to high-throughput providers.

### No Event History Limit

Workflow history is segmented and streamed. Long histories do not need to be
loaded as one row or one buffer.

Recovery time still grows with replay distance. Use `continue_as_new` when a
workflow wants to cap recovery latency.

### First-Class Map Reduce

Large fanout uses paged manifests.

Durust records compact map operation facts in workflow history. Per-item leases,
retries, progress, and result writes live in provider-owned map/activity state.

### Payload Handling Is Provider-Owned

Workflow code passes typed values. Providers serialize those values and may keep
large encoded payloads out of hot history and index rows.

Application code sees the same workflow, activity, signal, child, and query APIs
regardless of where the encoded bytes live.

## Worker Registration

Workflow workers and activity workers can run in the same process or on
different machines.

```rust
let mut worker = durust::Worker::builder(backend.clone())
    .namespace("prod")
    .worker_id("orders-a")
    .workflow_task_queue("orders")
    .register_workflow(checkout)
    .activity_task_queue("payments")
    .register_activity(price_quote)
    .register_activity(charge_card)
    .max_local_activities_per_workflow_task(64)
    .max_cached_workflows(10_000)
    .max_concurrent_workflow_tasks(256)
    .max_concurrent_activities(512)
    .activity_completion_batch_size(32)
    .build();

let shutdown = worker.shutdown_handle();
worker.run().await?;
```

`Worker::run` loops work passes until `shutdown.shutdown()` is called from
another task, parking in the provider's `wait_for_ready` while idle. The
throughput knobs:

- `max_cached_workflows` bounds the in-memory workflow future cache (LRU);
  evicted runs cold-replay from history on their next task.
- `max_concurrent_workflow_tasks` bounds how many workflow tasks one pass
  claims and pipelines through commit.
- `max_concurrent_activities` bounds how many claimed activities execute
  concurrently within one activity pass.
- `activity_task_batch_size` sets how many tasks one claim RPC requests (the
  per-RPC size is the min of both activity knobs); raise it under high
  concurrency to issue fewer, larger claim RPCs instead of single-task ones.
- `activity_completion_batch_size` batches activity completion RPCs.

Activity-only workers are just workers that register activities and poll an
activity queue (`WorkerBuilder::run` builds and runs in one step):

```rust
durust::Worker::builder(backend.clone())
    .namespace("prod")
    .worker_id("payment-activities-a")
    .activity_task_queue("payments")
    .register_activity(charge_card)
    .run()
    .await?;
```

Handlers annotated with `#[durust::workflow]` and `#[durust::activity]` also
export manifest metadata for the binary that links them. Use
`durust::exported_manifest()` with `durust::write_manifest(...)` to materialize a
current `durable.manifest.json` candidate for review, and the `cargo durable
manifest <normalize|check|diff|accept>` CLI to normalize, gate, and accept it.
The manifest's `*TypeNameHash` fields fingerprint Rust type names: they catch a
handler switching input/output types, not fields changing inside a same-named
type.

Workflow and activity handlers take exactly one named input struct. Wrap scalar,
tuple, collection, and no-input cases in an explicit request type so durable
inputs can evolve by adding named fields without changing the handler shape:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NoInput {}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChargeInput {
    pub order_id: String,
    pub amount_cents: u64,
}
```

When evolving an input type, prefer additive fields that old history payloads can
deserialize. Optional fields should normally use `Option<T>` plus Serde defaults:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChargeInput {
    pub order_id: String,
    pub amount_cents: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_scope: Option<String>,
}
```

With this shape, workflows or activities started before the field existed replay
with `idempotency_scope: None`, while new callers can set it explicitly. For
non-optional fields, give Serde a deterministic default with `#[serde(default)]`
only when that default preserves the old semantics. Breaking input changes should
use the normal Durust versioning tools rather than relying on replay to reinterpret
old payloads.

If an activity is registered locally on the workflow worker and
`max_local_activities_per_workflow_task` has available slots, Durust executes
that activity in the workflow worker process before remote workers can claim
remaining queued work. Set the local limit to `0` to leave all activity tasks for
remote workers polling the selected task queue.

Workflow code can set defaults for later activity calls:

```rust
durust::set_default_activity_options(
    durust::ActivityOptions::new()
        .task_queue("payments")
        .retry(durust::RetryPolicy::exponential().max_attempts(5))
        .timeout(std::time::Duration::from_secs(30)),
);

let quote = durust::call_activity!(price_quote(input.quote())).await?;
let charge = durust::call_activity!(charge_card(input.charge(quote)))
    .task_queue("high-priority-payments")
    .await?;
```

Defaults are workflow-local and can be changed with normal deterministic control
flow. Per-call options override the current defaults for that call.

Long-running activities can opt into heartbeat enforcement:

```rust
let result = durust::call_activity!(transcode(input))
    .task_queue("media")
    .heartbeat_timeout(std::time::Duration::from_secs(10))
    .await?;
```

The heartbeat timeout is disabled by default. When enabled, the provider starts
the heartbeat deadline when the activity task is claimed and refreshes it when
the activity calls `durust::heartbeat_activity().await?`. A missed heartbeat
uses the same retry policy as other activity timeouts.

Activities with neither a start-to-close timeout nor a heartbeat timeout use
their claim lease as an implicit heartbeat interval: a timeout-less activity
that outlives the worker's activity lease (default 30s) must heartbeat, and
each heartbeat keeps the claim alive for one more lease. One that stops
heartbeating — a hung or crashed worker — is reclaimed and retried one lease
after its last heartbeat.

`RetryPolicy::exponential()` paces retries with provider-enforced backoff: a
failed attempt's retry becomes claimable
`min(max_interval, initial_interval * backoff_coefficient^(failed_attempt - 1))`
after the failure (one second doubling up to a minute by default), so a
fast-failing activity cannot hot-loop; `.initial_interval(..)`,
`.max_interval(..)`, `.backoff_coefficient(..)`, and
`.non_retryable_error_types(..)` tune it, and the same fields drive the
TypeScript runtime. `RetryPolicy::none()` disables both retries and pacing. A
timed-out attempt's retry is paced the same way.

Activities return serializable Durust errors. A retry policy is skipped when the
activity returns a non-retryable application error:

```rust
return Err(durust::Error::non_retryable(
    "orders.invalid-address",
    "shipping address is not serviceable",
));
```

The durable failure stores a stable error type, message, optional encoded
details, and the non-retryable flag so replay can restore the same failure
metadata.

## Core Patterns

### Signals, Timers, And Select

`durust::select!` races durable operations and returns the winning branch
value. Use an enum when the caller needs to know which branch won.

```rust
enum ApprovalDecision {
    Approved(Approval),
    Cancelled(Cancel),
    TimedOut,
}

let decision = durust::select! {
    approval = durust::signal::<Approval>("approved") => {
        ApprovalDecision::Approved(approval?)
    }

    cancel = durust::signal::<Cancel>("cancel") => {
        ApprovalDecision::Cancelled(cancel?)
    }

    _ = durust::sleep_until(deadline) => {
        ApprovalDecision::TimedOut
    }
};
```

A signal named `"cancel"` is just an application signal. It only cancels the
workflow if your code maps it to a cancellation or terminal error.

External cancellation is a client operation:

```rust
client
    .cancel_workflow("order/123", "customer requested cancellation")
    .await?;
```

Cancellation records a terminal workflow fact and the provider atomically clears
derived waits, activity tasks, and activity-map item state for that run. Late
activity completions are idempotent and do not append workflow failure history.

### Workflow Time

Workflow code reads deterministic time from Durust:

```rust
let started_at = durust::now().await?;
let deadline = TimestampMs(started_at.0 + 30 * 60 * 1_000);

durust::sleep_until(deadline).await;
```

`durust::now()` is workflow time: the provider clock as the task observed it,
recorded in durable history as a side-effect marker so replay returns the same
value. Each call records its own marker. Use `durust::sleep(...)` or
`durust::sleep_until(...)` for timers.

### Bounded Fanout With Join

Use `join!` when the workflow has a bounded number of durable operations to
launch and collect.

```rust
let (quote, inventory) = durust::join!(
    durust::call_activity!(price_quote(input.quote())).task_queue("pricing"),
    durust::call_activity!(reserve_inventory(input.items)).task_queue("inventory"),
)
.await?;
```

Plain Rust futures are lazy. Creating variables and awaiting them one by one is
not a concurrent durable launch. Use `join!` for bounded fanout.

### Dynamic Fanout With Join All

Use `join_all` when the workflow learns a bounded set of durable operations at
runtime and needs every result.

```rust
let mut branches = Vec::new();
for item in items {
    let handle = durust::call_activity!(work_item(item))
        .task_queue("workers")
        .spawn()
        .await?;
    branches.push(handle.result());
}

let outputs = durust::join_all(branches).await?;
```

`join_all` registers and polls branches in vector order and returns results in
that same order, even if completions arrive out of order. It is still a bounded
workflow-level primitive; use `activity_map` for very large collect-all fanout.

### Dynamic Races With Select All

Use activity spawn handles when the workflow learns a bounded set of activities
at runtime and needs to launch them before awaiting any one result.

```rust
let mut branches = Vec::new();
for (index, item) in items.into_iter().enumerate() {
    let handle = durust::call_activity!(score_item(item))
        .task_queue("scoring")
        .spawn()
        .await?;
    branches.push(
        handle
            .result()
            .map_ok(move |score| ScoredItem { index, score })
            .boxed(),
    );
}

let winner = durust::select_all(branches).await?;
```

`spawn().await` emits an `ActivityScheduled` command immediately; the backend
makes it durable in the same atomic workflow-task commit. `select_all` picks the
ready branch with the earliest history event id, using vector order as the
tie-break. Pending activity losers are cancelled. Child-result losers are not
cancelled unless parent close policy later cancels them.

For very large collect-all fanout, prefer `activity_map`.

### Child Workflow: Spawn And Wait

```rust
let child = durust::child!(ship_order(input))
    .workflow_id(format!("ship/{}", input.order_id))
    .parent_close_policy(ParentClosePolicy::Cancel)
    .spawn()
    .await?;

let shipment = child.result().await?;
```

`spawn().await` resolves after the child start is durable. `result().await`
waits for child completion.

### Child Workflow: Spawn And Abandon

Use `Abandon` when the parent may exit while the child continues independently.

```rust
let receipt = durust::child!(send_receipt(input))
    .workflow_id(format!("receipt/{}", input.order_id))
    .parent_close_policy(ParentClosePolicy::Abandon)
    .spawn()
    .await?;

durust::publish(&OrderView {
    receipt_run_id: Some(receipt.run_id().clone()),
    ..view
})?;
```

Children are cancelled on parent terminal state by default. Orphaning is
explicit.

### Query Projection

Queries read the latest committed projection. They do not replay workflow code.

```rust
#[derive(Serialize, Deserialize)]
pub struct OrderView {
    pub status: OrderStatus,
    pub payment_id: Option<PaymentId>,
}

durust::publish(&view)?;

#[durust::query(workflow = checkout)]
pub fn status(view: &OrderView) -> OrderStatus {
    view.status.clone()
}

let view = client
    .query_projection::<checkout>("order/123")
    .await?
    .expect("projection published");
let status = status(&view);
```

### Version Branches

Use version markers when changing command-producing workflow code.

```rust
if durust::patched("new-payment-flow")? {
    durust::call_activity!(charge_v2(input)).await?;
} else {
    durust::call_activity!(charge_v1(input)).await?;
}
```

The marker lets one worker binary run both old and new open workflows.
After no open workflow needs the old branch, keep a bridge while removing the
branch body:

```rust
durust::deprecate_patch("new-payment-flow")?;
durust::call_activity!(charge_v2(input)).await?;
```

Removing the bridge before marked histories are gone is detected during replay.

### Map Reduce

For large fanout, use manifest-backed maps. The workflow never holds all inputs
or outputs in memory.

```rust
#[durust::workflow(name = "jobs.word-count", version = 1)]
pub async fn word_count(input: WordCountInput) -> durust::Result<WordCountOutput> {
    let partitions = durust::call_activity!(partition_input(input.source_ref))
        .task_queue("storage")
        .await?;

    let mapped = durust::activity_map(do_work)
        .task_queue("mappers")
        .input_manifest(partitions.manifest_ref)
        .max_in_flight(10_000)
        .result_manifest("partials")
        .spawn()
        .await?;

    let partials = mapped.result_manifest().await?;

    let output = durust::call_activity!(reduce_manifest(partials))
        .task_queue("reducers")
        .await?;

    Ok(WordCountOutput {
        output_ref: output.output_ref,
    })
}
```

On the happy path this workflow writes eight history events total:
`WorkflowStarted`, partition activity scheduled/completed, map
scheduled/completed, reduce activity scheduled/completed, and
`WorkflowCompleted`. The map does not add one history event per manifest item;
per-item leases, retries, and results stay in provider-owned map state.
Input and result manifest refs point to small root manifests whose pages are
separate payload refs, so providers do not need one large row for every map item
or result.

`do_work` is the activity Durust runs once per manifest item:

```rust
#[durust::activity(name = "jobs.do-work")]
pub async fn do_work(input: WorkInput) -> durust::Result<WorkOutput> {
    let item = blob::read(input.item_ref).await?;
    let partial = count_words(item)?;
    let partial_ref = blob::write(partial).await?;

    Ok(WorkOutput { partial_ref })
}
```

`activity_map` manages:

```text
manifest paging
max_in_flight
per-item leases
per-item retries
progress counters
result manifest writes
bounded workflow history
```

Workflow history stays compact by recording the map operation.

When each map item needs durable multi-step orchestration, use
`child_workflow_map` instead of recording one child lifecycle in the parent per
item:

```rust
let mapped = durust::child_workflow_map::<ProcessPartitionWorkflow>()
    .task_queue("partition-workers")
    .workflow_id_prefix(format!("word-count/{}/partitions", input.job_id))
    .input_manifest(partitions.manifest_ref)
    .max_in_flight(256)
    .result_manifest("partition-results")
    .spawn()
    .await?;

let partials = mapped.result_manifest().await?;
```

Child workflow maps use deterministic child workflow ids of the form
`{workflow_id_prefix}/{ordinal}`. The parent history records only
`ChildWorkflowMapScheduled` and a terminal map event; provider-owned map item
state tracks child starts, completions, cancellation, and ordered result
manifests. The input and result manifests use the normal `PayloadRef` path, so
large manifests are offloaded by the configured payload backend when they exceed
the inline threshold.

### Continue As New

Use `continue_as_new` to cap recovery latency for workflows that naturally run
for a long time.

```rust
if processed_batches >= 10_000 {
    return durust::continue_as_new(JobInput {
        cursor: next_cursor,
        accumulated_ref,
    });
}
```

The current run records `WorkflowContinuedAsNew`, and the provider starts a new
run with the same workflow id and compacted input. The new run begins with a
fresh history, so later recovery replays only the compacted state.

## Payloads

Durust APIs use typed inputs and outputs. History and indexes record compact
payload references so backends can keep hot persistence paths small without
changing workflow code.

For application code, the rule is simple: pass serializable request and response
types through workflows, activities, signals, children, and queries. For truly
large domain data, pass application-level object references through those types
and let activities read or write the external data.

Codec choices, inline thresholds, blob-store integrations, and provider test
fixtures are provider implementation details. They are part of the durability
contract, not the workflow API.

The default durable codec is MessagePack. Providers may opt into JSON for
debugging or export; typed client, workflow, activity, signal, child, map, and
query APIs use the provider-configured codec for new payloads, while replay
decodes each stored payload by its recorded codec.

Payload refs include compression metadata for forward compatibility, but Durust
does not choose a runtime compression policy today. New payloads are written
with `CompressionId::None`; keep compression decisions outside the runtime until
benchmark and deployment evidence justifies a specific provider option.

In tests or local providers, force small inline thresholds to exercise the blob
path, or choose JSON explicitly:

```rust
let backend = durust::MemoryBackend::with_payload_storage(
    durust::PayloadStorageConfig::new().inline_threshold_bytes(1024),
);

let backend = durust::SqliteBackend::open_with_payload_storage(
    "durust.sqlite3",
    durust::PayloadStorageConfig::new().inline_threshold_bytes(1024),
)?;

let backend = durust::SqliteBackend::open_with_payload_storage(
    "durust.sqlite3",
    durust::PayloadStorageConfig::new()
        .inline_threshold_bytes(1024)
        .blob_store(durust::BlobStoreConfig::LocalDirectory {
            root: "durust-payloads".into(),
            prefix: "payloads".to_owned(),
        }),
)?;

let backend = durust::PayloadBackend::with_payload_storage(
    durust::SqliteBackend::open("durust.sqlite3")?,
    durust::S3BlobStore::garage(durust::S3BlobStoreConfig {
        bucket: "durust-payloads".to_owned(),
        endpoint: "http://127.0.0.1:3900".to_owned(),
        region: "garage".to_owned(),
        prefix: "payloads".to_owned(),
        access_key_id: "garage-access-key".to_owned(),
        secret_access_key: "garage-secret-key".to_owned(),
    })?,
    durust::PayloadStorageConfig::new().inline_threshold_bytes(1024),
);

let debug_backend = durust::MemoryBackend::with_payload_storage(
    durust::PayloadStorageConfig::new().codec(durust::CodecId::Json),
);
```

The provider validates blob digests and hydrates payloads before returning them
through public workflow history, activity tasks, signals, and query projections.
Worker replay uses a separate raw history stream so large blob refs stay compact
until workflow code actually observes the payload. The worker then hydrates that
payload at an explicit async boundary before polling the workflow again; replay
polling itself does not hide database or object-store I/O.

Side-effect markers are the deliberate exception: `durust::side_effect(...).await`
records a small inline value, capped at 8 KiB, and is never offloaded. Use
activities or ordinary payload refs for larger values.

The SQLite local-directory store is content-addressed and keeps large encoded
bytes outside hot SQLite rows. For S3-compatible object stores such as Garage,
use `PayloadBackend` with `S3BlobStore` (behind the `s3` cargo feature) so the
async object-store implementation works across durability providers instead of
being duplicated inside each provider. Blob URI
ownership is exclusive: each provider resolves only refs carrying its own
scheme and persists every other scheme opaquely, so custom `PayloadBlobStore`
implementations work over any inner provider.

Providers also expose dry-run-capable payload GC that removes blobs no longer
reachable from durable history or operational indexes; `PayloadBackend` applies
the same contract to its external object store by asking the inner provider for
generic payload roots and deleting only wrapper-owned unreachable objects.
Because blobs upload before the commit that makes them reachable, GC never
deletes a blob younger than `PayloadGarbageCollectionRequest::min_age` (default
one hour); stores that can cheaply refresh a blob's timestamp on a
content-addressed re-put do so, while S3 skips the refresh and relies on the
grace period exceeding the worst upload-to-commit latency plus one GC scan.
Delete failures are recorded in the outcome and the sweep continues.

To run the local Garage-backed S3 conformance test:

```bash
docker compose -f tests/fixtures/garage.compose.yml up -d
DURUST_GARAGE_ENDPOINT=http://127.0.0.1:3900 \
DURUST_GARAGE_BUCKET=durust-payloads \
DURUST_GARAGE_REGION=garage \
DURUST_GARAGE_PREFIX=local/payloads \
DURUST_GARAGE_ACCESS_KEY_ID=GK0123456789abcdef0123456789abcdef \
DURUST_GARAGE_SECRET_ACCESS_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
DURUST_REQUIRE_GARAGE=1 \
cargo test --features s3 --test provider_conformance garage -- --nocapture
docker compose -f tests/fixtures/garage.compose.yml down -v
```

`DURUST_REQUIRE_GARAGE` makes a missing or blank `DURUST_GARAGE_*` variable a
failure instead of a skip, and it names the variables that are actually
missing. Leave it unset if you have no Garage: the test then skips and the run
still passes. It is on for any value except empty, `0`, and `false`, so a typo
runs the test rather than quietly dropping it.

The filter is the substring `garage` rather than the full test name, and that
matters more than it looks. `cargo test` with a filter matching nothing prints
`0 passed` and exits 0, so naming the single conformance test meant a run
without `--features s3` was green having executed nothing. `garage` also
matches `garage_s3_feature_is_enabled_when_garage_is_required`, which compiles
unconditionally and fails when the feature is absent — so the filter can never
select zero tests.

## Recovery Model

Recovery is streaming and bounded:

```text
claim workflow task -> get replay_target_event_id
stream history chunks up to that target
recreate the workflow future
durable APIs consume recorded facts
switch to live mode at the tail
```

A claimed batch prepares its runs concurrently: each run's history read,
payload hydration, and cold replay are independent, so they overlap instead of
queueing behind one another. `max_concurrent_workflow_tasks` bounds that
overlap, and `max_concurrent_payload_hydrations` bounds the blob reads one task
issues while resuming a fanout.

Cold recovery is flow-controlled by the worker. A worker can cap concurrent
cold recoveries, clamp each recovery attempt by replay events, replay bytes, and
history chunks, then defer the workflow task through generic delayed visibility
when capacity is unavailable. Cached workflow wakes stay on the fast path and do
not wait behind cold replay saturation. Durability providers remain generic:
they honor stream bounds and may return retry-after backpressure without knowing
why the workflow is replaying.

Unconsumed signals, pending timers, activity leases, and ready rows are live
operational indexes. They are not streamed as replay history until workflow code
observes a committed fact.

### The Commit Fence

A workflow task's claim token is its whole fence. A provider applies the commit
while that token still owns the run and rejects it otherwise; there is no
separate check on the run's history tail.

That works because every event replay matches positionally is written by the
claim holder. The exceptions — `cancel_workflow` and the two parent-close paths
— revoke the claim in the same transaction, so the token catches them. Facts
appended concurrently by activity workers, timer sweeps, and child dispatch are
looked up by command sequence and never enter the replay window, so they cannot
invalidate a task that never saw them.

A fact that lands under a claim keeps the run ready, so the next task picks it
up. Providers record the tail they handed out with each claim and compare it at
commit to decide that, rather than taking the worker's word for it.

## Determinism

Workflow code must be deterministic. Durust provides a best-effort
compile-time lint for obvious mistakes:

```text
tokio::time::sleep      -> durust::sleep
std::time::Instant::now -> durust::now
tokio::select!          -> durust::select!
tokio::spawn            -> durust::spawn or durust::join!
random values           -> durust::side_effect(...).await
network/db calls        -> durust::call_activity!
```

Replay and command fingerprints remain the correctness backstop.

Runtime and provider fault tests use the deterministic simulator primitives in
`durust::testing` (`SimRun`, `FaultProfile`, and `run_many_seeds`) so failures
report a seed and trace that can be replayed locally. `FaultInjectingBackend`
wraps any `DurableBackend` with seeded per-call fault decisions (transient errors,
duplicated activity completions, scripted worker crashes) so simulations drive
the real `Worker` over the real in-memory provider under fault injection; the
memory provider's clock is fully virtual (`advance_time`), so leases, timers,
delayed visibility, and retry backoffs are simulation-controlled.

## Durability Providers

Durability is a provider trait, not a database mandate. The trait and every
request and outcome type it exchanges with the runtime live in
`durust::provider`; workflow code needs none of them.

A defaulted method on `DurableBackend` must stay correct when `self` is a
wrapper around another backend, since that is the case the compiler cannot
check. The batch and convenience methods default to driving `self`'s own
single-item method, which stays correct through any number of wrappers.
`payload_storage_config` and `hydrate_payload` have no default, so a wrapper
that forgets them fails to build rather than silently reporting the wrong
config or handing back unresolved blob refs.

Providers must support:

```text
append-journal history
bounded history streaming
active wait indexes
workflow and activity leases
signal inboxes
activity map state
child workflow map state
child workflow starts and parent notifications
query projections
payload refs
idempotency
provider conformance tests
```

Durust includes:

```text
memory provider for fast tests (always available)
SQLite provider for local development and conformance (`sqlite` feature, default)
Postgres provider (`postgres` feature)
S3-compatible payload blob store (`s3` feature)
```

### Cargo Features

```toml
[dependencies]
durust = "0.1"                                            # memory + SQLite
durust = { version = "0.1", features = ["postgres"] }     # + Postgres
durust = { version = "0.1", features = ["s3"] }           # + S3BlobStore
durust = { version = "0.1", features = ["testing"] }      # + durust::testing
durust = { version = "0.1", default-features = false }    # memory only
```

- `sqlite` (default) gates `SqliteBackend` and the `cargo-durable` CLI binary.
- `postgres` gates `PostgresBackend` and its `tokio-postgres`/`deadpool-postgres`
  dependencies.
- `s3` gates `S3BlobStore`; the `PayloadBackend` decorator and the SQLite
  local-directory blob store are always available.
- `testing` gates `durust::testing`, the deterministic simulation harness
  (`SimRun`, `FaultProfile`, `FaultInjectingBackend`). Test scaffolding, so it
  is opt-in rather than linked into every production binary.

The benchmark workload, comparison, and reporting binaries live in the
unpublished `durust-benchtools` workspace crate
(`cargo run -p durust-benchtools --bin durust-benchmark-workload`), so library
consumers never compile them.

## Benchmarks

These numbers are local medians from three runs on a shared Darwin 25.5.0
machine. They are useful for close comparisons on the same machine, not as
portable capacity claims. The mixed workload starts each parent workflow, runs
three activities, sends one signal, fires one timer, starts and completes one
child workflow, then verifies completion. The TypeScript implementation has its
own benchmark section in [`typescript/README.md`](typescript/README.md).

Mixed workload medians:

| Backend | Config | Processing workflows/s | Processing actions/s | Variance | Commit p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| SQLite | 1000 workflows, 4 workers, batch 32 | 226.82 | 1814.52 | 9.6% | 3.769 ms |
| SQLite | 1000 workflows, 1 worker, batch 32 | 224.78 | 1798.21 | 4.2% | 0.402 ms |
| Postgres | 1000 workflows, 4 workers, pool 8 | 47.94 | 383.52 | 9.9% | 7.912 ms |
| Postgres | 1000 workflows, 100 shard leases, 10 workers, pool 24 | 320.50 | 2563.98 | 7.3% | 15.838 ms |

The 100-shard Postgres profile measures batched normalized Postgres operation
under 100 shard leases, not shard-journal recovery.

Criterion headline medians, current versus the saved `phase6-before` baseline:

| Benchmark | Current median | Delta |
| --- | ---: | ---: |
| Cached wake poll, memory | 4.0 us | -3.1% |
| Replay small history, memory | 8.1 us | -4.5% |
| Replay large history, memory | 45.9 us | -10.3% |
| Activity claim/complete, memory | 1.5 us | +0.4% |
| Activity claim/complete, SQLite | 2.1 ms | -43.2% |
| Activity claim/complete, Postgres | 3.4 ms | -17.8% |
| Workflow append commit, memory | 1.0 us | -3.9% |
| Workflow append commit, SQLite | 0.428 ms | -20.1% |
| Workflow append commit, Postgres | 2.1 ms | -26.7% |
| Held handle across sleeps, memory | 75.2 us | -62.5% |
| Child fanout completion, memory | 131.4 us | -2.7% |
| Child fanout completion, SQLite | 14.7 ms | -0.0% |
| Child start dispatch, memory | 1.6 us | +8.5% |
| Child parent wakeup, Postgres | 3.1 ms | -32.2% |
| History stream, Postgres | 0.339 ms | -22.7% |
| Chunked history replay stream, Postgres | 4.3 ms | -25.7% |

Reproduce the mixed workload reports with release benchtools:

```bash
cargo build --release -p durust-benchtools

cargo run --release -p durust-benchtools --bin durust-benchmark-workload -- \
  --backend sqlite --mode mixed --sqlite-layout single-file \
  --workflows 1000 --workers 4 --shards 1 --physical-partitions 1 \
  --activation-concurrency 1 --activation-prefetch-limit 1 \
  --batch 32 --activity-completion-batch 1 --max-rounds 10000 --json

cargo run --release -p durust-benchtools --bin durust-benchmark-workload -- \
  --backend sqlite --mode mixed --sqlite-layout single-file \
  --workflows 1000 --workers 1 --shards 1 --physical-partitions 1 \
  --activation-concurrency 1 --activation-prefetch-limit 1 \
  --batch 32 --activity-completion-batch 1 --max-rounds 10000 --json

DURUST_POSTGRES_URL='postgres://durable:durable@127.0.0.1:55432/durable' \
  cargo run --release -p durust-benchtools --bin durust-benchmark-workload -- \
  --backend postgres --mode mixed --workflows 1000 --workers 4 \
  --shards 1 --physical-partitions 1 --activation-concurrency 1 \
  --activation-prefetch-limit 1 --batch 32 --activity-completion-batch 1 \
  --postgres-pool-size 8 --max-rounds 10000 --json

DURUST_POSTGRES_URL='postgres://durable:durable@127.0.0.1:55432/durable' \
  cargo run --release -p durust-benchtools --bin durust-benchmark-workload -- \
  --backend postgres --mode mixed --workflows 1000 --workers 10 \
  --shards 100 --physical-partitions 16 --activation-concurrency 8 \
  --activation-prefetch-limit 32 --batch 32 --activity-completion-batch 32 \
  --postgres-pool-size 24 --max-rounds 10000 --json
```

Compare a captured mixed workload report with its checked-in baseline:

```bash
cargo run --release -p durust-benchtools --bin durust-benchmark-compare -- \
  --durust target/benchmark-runs/rust/durust-mixed-postgres-median.json \
  --baseline benches/baselines/durust-mixed-postgres.json
```

Run the scoped Criterion comparison against a saved baseline:

```bash
DURUST_POSTGRES_URL='postgres://durable:durable@127.0.0.1:55432/durable' \
  cargo bench --bench replay_core -- --baseline phase6-before \
  '^(workflow_cached_wake_poll_memory|workflow_replay_(small|large)_history_memory|held_handle_spawn_then_sleeps_memory|child_fanout_completion_(memory|sqlite)|child_start_dispatch_memory|activity_claim_complete_(memory|sqlite)|workflow_task_append_commit_(memory|sqlite)|postgres_provider_hot_paths/(workflow_task_append_commit|history_stream|history_stream_chunked_replay|activity_claim_complete|child_workflow_start_parent_wakeup)_postgres)$'
```

## Upgrading

There is no changelog yet, so breaking changes to the Rust crate are recorded
here, newest first. A change is listed if it can break a deployment that is
working today — either its code will not compile, or its in-flight runs stop
replaying. `typescript/README.md` keeps the same ledger for the TypeScript
packages; a change that breaks both is written in both, because the affected
reader only reads one.

### The commit fence is the claim token, and `SelectWinner` drops its event id

**Who is affected.** Every deployment. In-flight runs whose history contains a
`SelectWinner` event cannot be replayed by this version, and every third-party
`DurableBackend` implementation must be updated to compile.

**What changes.** Three things move together, because the first is what made
the second necessary and the third is what made it possible.

`WorkflowTaskCommit` loses `expected_tail_event_id`, and a provider fences a
commit on the claim token alone. A fact appended to the run while a workflow
task was claimed — an activity result, a fired timer, a child terminal — no
longer voids that task. `commit_workflow_task` returns the run's new
`EventId` directly, and `CommitOutcome`, `Error::Conflict`, `conflict_to_error`,
`WorkerEvent::WorkflowTaskConflicted` and `WorkerMetrics::workflow_task_conflicts`
are gone: a commit either lands or reports why it could not.

`SelectWinner` loses `winning_event_id`. Replay follows the recorded
`branch_ordinal` instead of recomputing which branch won, so it no longer
depends on the absolute event id of the winning fact. This is the history-format
break: a history recorded by 0.2.1 that contains a `SelectWinner` will fail to
decode.

Providers gain a `claim_tail_event_id` column on `workflow_instances`, recorded
when a claim is handed out and compared at commit so a fact that arrived under
the claim keeps the run ready. The SQLite and Postgres providers migrate it
automatically on open.

**What to do.** Drain in-flight runs before upgrading, or accept that runs with
a recorded `SelectWinner` will not replay. Third-party providers: delete the
tail comparison, return the new tail from `commit_workflow_task`, record the
claim tail, and implement `payload_storage_config` and `hydrate_payload`, which
no longer have defaults.

### An unqueued activity no longer fingerprints the worker's activity queue

**Who is affected.** Any deployment whose workflow workers set
`.activity_task_queue(...)` to anything other than `"default"` *and* whose
workflows schedule an activity without naming a queue. That means **both**
`durust::call_activity!` and `durust::activity_map(...)`: they share one
fingerprint helper (`activity_fingerprint_options`), so they moved together and
must be repaired together. Auditing only `call_activity!` leaves every
in-flight run with an unqueued `activity_map` broken. The
[Worker Registration](#worker-registration) example above —
`.activity_task_queue("payments")` — is exactly the affected worker shape, so
treat this as the common case rather than the exotic one. A worker left on the
default activity queue is unaffected, byte for byte.

**What changes.** The task queue an activity resolves to used to be folded into
the command's `options_digest`, and the resolution includes the scheduling
worker's `activity_task_queue` fallback. So a command's identity was readable
from the configuration of whichever worker happened to schedule it: two
workflow workers with different activity queues fingerprinted the same call
differently, and a run scheduled by one could not be replayed by the other. The
digest now hashes the queue the **caller** named, defaulting to `"default"`
when the call names none. The activity is still *scheduled onto* the worker's
queue; only the fingerprint stopped depending on it.

Measured on the two formulas, for an unqueued activity with default options.
`src/runtime.rs` and `src/options.rs` are byte-identical at `58672bf` (0.2.0)
and `c04b2a0` (0.2.1), so both releases recorded the left-hand column, and an
`activity_map` moves the same way:

| worker `activity_task_queue` | recorded by 0.2.0 and 0.2.1 | recorded now |
| --- | --- | --- |
| `default` | `sha256:619ac156…` | `sha256:619ac156…` |
| `activities` | `sha256:6ceafc6e…` | `sha256:619ac156…` |
| `queue-a` | `sha256:c41e6973…` | `sha256:619ac156…` |

**The consequence.** In-flight runs of the affected shape fail their next
replay. Both messages, captured from the code rather than reconstructed — grep
your logs for either:

```text
nondeterministic replay: activity command fingerprint changed for command 1
nondeterministic replay: activity map command fingerprint changed for command 1
```

(The trailing number is the command sequence, so it varies.) These are Rust's
strings. TypeScript renders the same condition as
`nondeterminism: activity command fingerprint changed`, which matches nothing
in a Rust log.

**The repair is a source change, not a configuration change.** No worker
setting reproduces the old digest, because the old digest was a function of the
worker's own queue. Name the queue explicitly at every unqueued site instead —
**both kinds**:

```rust
// Was: implicit, fingerprinted with the worker's `activity_task_queue`.
durust::call_activity!(price_quote(input)).await?;

// Now: explicit, and fingerprints exactly as the old implicit form did on a
// worker configured with `.activity_task_queue("activities")`.
durust::call_activity!(price_quote(input))
    .task_queue("activities")
    .await?;

// The same edit is required on unqueued maps, which are affected identically.
durust::activity_map(map_chunk)
    .task_queue("activities")
    .input_manifest(manifest_ref)
    .max_in_flight(100)
    .result_manifest("partials")
    .spawn()
    .await?;
```

Verified rather than asserted: under the new code an explicit
`.task_queue("activities")` produces `sha256:6ceafc6e…`, the same digest a
0.2.0 or 0.2.1 worker configured with `activity_task_queue("activities")`
recorded for the implicit call. Affected runs then replay and complete. Making
the queue explicit is worth keeping afterwards — it is what makes the
fingerprint independent of deployment topology.

**Before upgrading**, do one of: give every unqueued `call_activity!` *and*
`activity_map` site an explicit `.task_queue(...)` matching the worker queue it
was scheduled onto, and deploy that together with this version; or drain runs
with an unqueued activity of either kind in flight.

**Why this is not deferred.** The behaviour it removes is not a one-time break
but a permanent latent one: while the resolved queue sits inside the digest,
*every future change* to a worker's activity queue silently breaks replay for
runs in flight, and a fleet whose workers disagree can never replay one
another's runs at all. One documented break ends an unbounded series of
undocumented ones.

## Release Automation

Pull requests run the CI test workflow only, with a read-only `GITHUB_TOKEN`.
The repository's fork pull request workflow approval policy is
`first_time_contributors`, so GitHub holds workflow runs when the PR author or
event actor has not previously had a commit or pull request merged into this
repository until a user with write access approves the run.

Pushes to `main` run CI first. When push-triggered CI on `main` succeeds, the
release workflow publishes a lockstep release for the Rust crates and TypeScript
packages. It bumps the patch version by default, or bumps the minor or major
version when the triggering commit message contains `#minor` or `#major`. It
commits the updated manifests and lockfiles back to `main` with
`[skip release]`, then publishes `durust-macros`, `durust`, and the public
`@durust/*` npm packages. The `durust-node` addon behind `@durust/native` is
built on four platform runners (Linux x64 and arm64 in manylinux 2.34
containers, macOS x64 and arm64) and shipped as one `@durust/native-<target>`
package each, published before the facade that depends on them.

Manual dispatch can publish the `current` checked-in version without creating a
new version commit. This is only for recovering a partially published release.

The repository must define a `CARGO_REGISTRY_TOKEN` secret, and branch
protection must allow the GitHub Actions token to push the generated release
commit. npm packages use trusted publishing, so each public `@durust/*` package
must trust the `danthegoodman1/durust` repository's `release.yml` workflow on
the `main` branch.

## Examples

The [`examples/`](examples/) directory is the canonical reference for common
patterns. Each example is small, runnable, and copyable into a new project.

- [`hello_activity.rs`](examples/hello_activity.rs)
- [`worker_registration.rs`](examples/worker_registration.rs)
- [`signal_wait.rs`](examples/signal_wait.rs)
- [`timer_wait.rs`](examples/timer_wait.rs)
- [`select_approval.rs`](examples/select_approval.rs)
- [`join_activities.rs`](examples/join_activities.rs)
- [`activity_spawn_join_all.rs`](examples/activity_spawn_join_all.rs)
- [`activity_spawn_select_all.rs`](examples/activity_spawn_select_all.rs)
- [`version_branch.rs`](examples/version_branch.rs)
- [`continue_as_new.rs`](examples/continue_as_new.rs)
- [`payload_offload.rs`](examples/payload_offload.rs)
- [`child_workflows.rs`](examples/child_workflows.rs)
- [`query_projection.rs`](examples/query_projection.rs)
- [`local_remote_activity.rs`](examples/local_remote_activity.rs)
- [`activity_map.rs`](examples/activity_map.rs)
- [`child_workflow_map.rs`](examples/child_workflow_map.rs)
- [`map_reduce.rs`](examples/map_reduce.rs)
