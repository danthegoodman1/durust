# Durust

Durust is a durable workflow runtime for Rust services.

Write async Rust workflows that survive crashes, restarts, timers, signals,
long waits, child workflows, version rollouts, and large fanout.

```rust
#[durust::workflow(name = "orders.checkout", version = 1, query_state = OrderView)]
pub async fn checkout(input: CheckoutInput) -> durust::Result<CheckoutOutput> {
    let quote = durust::activity!(price_quote(input.quote()))
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

    let payment = durust::activity!(charge_card(input.charge(quote, approval)))
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
  - [Payload Offloading Is Provider-Owned](#payload-offloading-is-provider-owned)
- [Worker Registration](#worker-registration)
- [Core Patterns](#core-patterns)
  - [Signals, Timers, And Select](#signals-timers-and-select)
  - [Workflow Time](#workflow-time)
  - [Bounded Fanout With Join](#bounded-fanout-with-join)
  - [Child Workflow: Spawn And Wait](#child-workflow-spawn-and-wait)
  - [Child Workflow: Spawn And Abandon](#child-workflow-spawn-and-abandon)
  - [Query Projection](#query-projection)
  - [Version Branches](#version-branches)
  - [Map Reduce](#map-reduce)
  - [Continue As New](#continue-as-new)
- [Payloads And Blob Storage](#payloads-and-blob-storage)
- [Recovery Model](#recovery-model)
- [Determinism](#determinism)
- [Durability Providers](#durability-providers)
- [Examples](#examples)

## Why Durust

- Workflows are normal async Rust functions.
- Local variables stay in memory while a workflow is hot on a worker.
- Recovery reconstructs locals by streaming append-only history.
- Providers optimize persistence with append-journal writes and derived indexes.
- Large payloads are hidden behind provider-owned `PayloadRef` handling.
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
  inline small payloads, offload large payloads to blob storage such as S3
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

### Payload Offloading Is Provider-Owned

Workflow code passes typed values. The provider chooses inline storage or blob
storage such as S3.

The durability provider owns serialization, compression, encryption, blob
upload/download, and `PayloadRef` persistence. Application code sees the same
API either way.

## Worker Registration

Workflow workers and activity workers can run in the same process or on
different machines.

```rust
let worker = durust::Worker::builder(backend.clone())
    .namespace("prod")
    .worker_id("orders-a")
    .workflow_task_queue("orders")
    .register_workflow(checkout)
    .activity_task_queue("payments")
    .register_activity(price_quote)
    .register_activity(charge_card)
    .max_cached_workflows(10_000)
    .max_concurrent_workflow_tasks(256)
    .max_concurrent_activities(512)
    .run()
    .await?;
```

Activity-only workers are just workers that register activities and poll an
activity queue:

```rust
durust::Worker::builder(backend.clone())
    .namespace("prod")
    .worker_id("payment-activities-a")
    .activity_task_queue("payments")
    .register_activity(charge_card)
    .run()
    .await?;
```

If an activity is registered locally on the workflow worker and local capacity is
available, Durust prefers local execution. Otherwise remote workers polling
the selected task queue can claim the task.

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

### Workflow Time

Workflow code reads deterministic time from Durust:

```rust
let started_at = durust::now();
let deadline = started_at + Duration::from_minutes(30);

durust::sleep_until(deadline).await;
```

`durust::now()` is workflow time. It is recorded in durable history and returns
the same value during replay. Use `durust::sleep(...)` or
`durust::sleep_until(...)` for timers.

### Bounded Fanout With Join

Use `join!` when the workflow has a bounded number of durable operations to
launch and collect.

```rust
let (quote, inventory) = durust::join!(
    durust::activity!(price_quote(input.quote())).task_queue("pricing"),
    durust::activity!(reserve_inventory(input.items)).task_queue("inventory"),
)
.await?;
```

Plain Rust futures are lazy. Creating variables and awaiting them one by one is
not a concurrent durable launch. Use `join!` for bounded fanout.

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
```

### Version Branches

Use version markers when changing command-producing workflow code.

```rust
if durust::patched("new-payment-flow")? {
    durust::activity!(charge_v2(input)).await?;
} else {
    durust::activity!(charge_v1(input)).await?;
}
```

The marker lets one worker binary run both old and new open workflows.

### Map Reduce

For large fanout, use manifest-backed maps. The workflow never holds all inputs
or outputs in memory.

```rust
#[durust::workflow(name = "jobs.word-count", version = 1, query_state = JobView)]
pub async fn word_count(input: WordCountInput) -> durust::Result<WordCountOutput> {
    let partitions = durust::activity!(partition_input(input.source_ref))
        .task_queue("storage")
        .await?;

    let mapped = durust::activity_map(do_work)
        .task_queue("mappers")
        .input_manifest(partitions.manifest_ref)
        .max_in_flight(10_000)
        .result_manifest("partials")
        .spawn()
        .await?;

    durust::publish(&JobView {
        status: JobStatus::Mapping,
        map_run_id: Some(mapped.id().clone()),
        ..JobView::default()
    })?;

    let partials = mapped.result_manifest().await?;

    let output = durust::activity!(reduce_manifest(partials))
        .task_queue("reducers")
        .await?;

    Ok(WordCountOutput {
        output_ref: output.output_ref,
    })
}
```

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

### Continue As New

Use `continue_as_new` to cap recovery latency for workflows that naturally run
for a long time.

```rust
if durust::history().event_count() > 100_000 {
    return durust::continue_as_new(JobInput {
        cursor: next_cursor,
        accumulated_ref,
    });
}
```

## Payloads And Blob Storage

Durust APIs use typed inputs and outputs. Providers decide whether encoded bytes
are inline or offloaded.

```text
default codec -> MessagePack via Serde
debug/export codec -> JSON
small payload -> inline PayloadRef
large payload -> blob PayloadRef, often S3 or another object store
```

This keeps hot DB rows small and lets providers optimize write throughput.
Providers offload payloads above roughly `4 KiB` to `16 KiB`, depending on the
backing store.

Provider config controls the inline threshold and blob store. The SQLite provider
can use an S3-compatible store such as Garage for local tests.

## Recovery Model

Recovery is streaming and bounded:

```text
claim workflow task -> get replay_target_event_id
stream history chunks up to that target
recreate the workflow future
durable APIs consume recorded facts
switch to live mode at the tail
```

Unconsumed signals, pending timers, activity leases, and ready rows are live
operational indexes. They are not streamed as replay history until workflow code
observes a committed fact.

## Determinism

Workflow code must be deterministic. Durust provides a best-effort
compile-time lint for obvious mistakes:

```text
tokio::time::sleep      -> durust::sleep
std::time::Instant::now -> durust::now
tokio::select!          -> durust::select!
tokio::spawn            -> durust::spawn or durust::join!
random values           -> durust::side_effect!
network/db calls        -> durust::activity!
```

Replay and command fingerprints remain the correctness backstop.

## Durability Providers

Durability is a provider trait, not a database mandate.

Providers must support:

```text
append-journal history
bounded history streaming
active wait indexes
workflow and activity leases
signal inboxes
activity map state
query projections
payload refs
idempotency
provider conformance tests
```

Durust includes:

```text
memory provider for fast tests
SQLite provider for local development and conformance
production-oriented provider examples
```

SQLite is included for local development, testing, and provider conformance.

## Examples

The [`examples/`](examples/) directory is the canonical reference for common
patterns. Each example is small, runnable, and copyable into a new project.
