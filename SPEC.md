# Plan: Replay-First Durable Rust Runtime

The new foundation should be:

```text
Normal Rust workflow function
        ↓
Durable APIs append commands and facts
        ↓
History is streamed during recovery
        ↓
Workflow locals are reconstructed by replay
        ↓
Live workflow future stays cached in memory
        ↓
No macro-lowered continuation frames
```

This gets you Temporal-style ergonomics without asking a proc macro to serialize arbitrary Rust locals. Temporal’s own model is based on deterministic workflow code, replay, workflow-safe alternatives for time/concurrency, and history markers for versioning; the Go SDK docs explicitly describe workflows as procedural coordination code whose state is recreated on another worker, with restrictions such as deterministic execution, using SDK time/sleep/select equivalents, and avoiding randomized map iteration. ([Go Packages][1])

`../durable-phases` is useful prior art for the durability posture: keep the happy-path persistence path append-only so durable commits are cheap and backend implementations can optimize around their native log/write model. The deliberate difference here is execution state. This runtime should keep the live workflow future cached on a worker while the workflow is running, and only reconstruct locals by replay after worker loss, cache eviction, or explicit recovery. Recovery from durable storage happens on cache miss or failure, not at every wait boundary.

The core accepted tradeoff becomes:

```text
Recovery memory: bounded
DB row size: bounded
Payload materialization: bounded/lazy
DX: high
Recovery time: grows with replay distance
```

That is a good trade.

---

# 1. New Architecture

## 1.1 Core components

```text
Workflow history
  Append-only replay facts and command events.
  Segmented and streamed.
  Required for recovery.

Active wait indexes
  Operational indexes for timers, signals, activities, children, cancellation.
  Used to wake workflows without scanning history.

Workflow cache
  In-memory pinned Rust futures.
  Used for fast steady-state progress.
  Kept until terminal state when possible.
  Droppable under cache pressure, worker shutdown, or recovery.

Payload store
  Inline small payloads.
  Offload large inputs/results/signals/query state to blob storage.
  Hidden behind provider-owned PayloadRef handling.

Query projection store
  Latest committed query/read-model payload.
  Queries do not require replay.

Audit log
  Optional/debuggable.
  Not necessarily the same as replay history.
```

The replay history is authoritative. The in-memory workflow future is only a cache, but it is an important performance cache: a running workflow should normally stay alive on the worker until it completes, fails, or is cancelled.

---

# 2. Public DX Target

The user writes normal-looking async Rust:

```rust
#[durust::workflow(name = "order", version = 1, query_state = OrderView)]
pub async fn order(input: OrderInput) -> durust::Result<OrderOutput> {
    let mut view = OrderView::new(&input);
    durust::publish(&view)?;

    let quote = durust::call_activity!(price_quote(QuoteInput {
        sku: input.sku.clone(),
        qty: input.qty,
    }))
    .retry(RetryPolicy::exponential().max_attempts(5))
    .await?;

    view.status = OrderStatus::Quoted;
    view.quote = Some(quote.summary());
    durust::publish(&view)?;

    let approval = durust::select! {
        approval = durust::signal::<Approval>("approved") => approval?,
        _ = durust::sleep_until(input.deadline) => {
            return Err(durust::Error::timeout("approval timed out"));
        }
    };

    let payment = durust::call_activity!(charge_card(ChargeInput {
        order_id: input.order_id.clone(),
        amount: quote.total,
        approval_id: approval.id.clone(),
    }))
    .idempotency_key(("charge", &input.order_id))
    .await?;

    let child = durust::child!(ship_order(ShipInput {
        order_id: input.order_id.clone(),
        payment_id: payment.id.clone(),
    }))
    .workflow_id(format!("ship/{}", input.order_id))
    .spawn()
    .await?;

    let shipment = child.result().await?;

    Ok(OrderOutput {
        order_id: input.order_id,
        payment_id: payment.id,
        shipment_id: shipment.id,
    })
}
```

No `WorkflowCtx` parameter is required. Durable APIs use task-local workflow context internally.

The `#[workflow]` macro should **not** lower the function into continuation frames. It should only generate:

```text
workflow registration
typed workflow stub
input/output metadata
workflow name/version metadata
durable manifest metadata
determinism lint scanning
```

## 2.1 Durable type identity

Workflow and activity identity must be stable and collision-resistant.

```rust
pub struct WorkflowType {
    pub name: DurableName,
    pub version: u32,
}

pub struct ActivityType {
    pub name: DurableName,
}
```

Durable names are what get stored in history, tasks, indexes, and child-start requests. They are not Rust symbol names.

Recommended production style is an explicit durable name:

```rust
#[durust::workflow(name = "orders.order", version = 1)]
pub async fn order(input: OrderInput) -> durust::Result<OrderOutput> {
    // ...
}

#[durust::activity(name = "payments.charge-card")]
pub async fn charge_card(input: ChargeInput) -> durust::Result<ChargeOutput> {
    // ...
}
```

If `name` is omitted, the macro may derive a default from the fully qualified Rust path:

```text
{cargo_package_name}::{module_path}::{function_name}
```

For example:

```text
order_service::workflows::checkout::order
order_service::workflows::returns::order
```

Those two functions have the same Rust function name, but different durable names because the module path is included.

Default names are convenient for tests and examples, but explicit names are safer for shipped workflows because Rust module refactors would otherwise change the durable identity. Strict mode may require explicit names for workflows and activities.

Collision rules:

```text
workflow registry key = namespace + workflow_name + workflow_version
activity registry key = namespace + activity_name
two handlers with the same registry key on one worker are an error
two generated registrations with the same registry key in one binary are an error when detectable
same function name in different modules is allowed when durable names differ
same durable name for different code is not allowed without an explicit versioning plan
```

The macro should also record the Rust source path as diagnostic metadata:

```text
rust_path = {cargo_package_name}::{module_path}::{function_name}
```

`rust_path` is for logs, errors, and debugging. Replay and provider matching must use only the durable workflow/activity identity.

## 2.2 Durable manifest

The macros should emit current workflow/activity metadata, and a CLI should manage a checked-in durable manifest baseline:

```text
durable.manifest.json
```

The manifest records every workflow and activity exported by the crate:

```json
{
  "workflows": [
    {
      "name": "orders.order",
      "version": 1,
      "rustPath": "order_service::workflows::checkout::order",
      "inputType": "order_service::types::OrderInput",
      "outputType": "order_service::types::OrderOutput",
      "inputSchemaHash": "sha256:...",
      "outputSchemaHash": "sha256:..."
    }
  ],
  "activities": [
    {
      "name": "payments.charge-card",
      "rustPath": "order_service::activities::payments::charge_card",
      "inputType": "order_service::types::ChargeInput",
      "outputType": "order_service::types::ChargeOutput",
      "inputSchemaHash": "sha256:...",
      "outputSchemaHash": "sha256:..."
    }
  ]
}
```

The `#[workflow]` and `#[activity]` macros submit linked handler metadata into
Durust's manifest inventory. A binary or test harness that links the handlers can
call `durust::exported_manifest()` to materialize the current metadata, then
write it with `durust::write_manifest(...)`. This keeps metadata generation tied
to compiled handler code while leaving manifest review and CI failure policy to
the explicit cargo helper.

CLI:

```text
cargo durable manifest write
cargo durable manifest check
cargo durable manifest diff
cargo durable manifest accept
```

Normal local compilation should not fail just because the manifest changed. If a checked-in manifest exists, the macro/build integration may emit warnings for obvious drift, but the explicit CLI is the source of hard failures.

`manifest check` should exit 1 in CI when:

```text
a workflow identity vanished
an activity identity vanished
a workflow input/output schema changed without a version bump
an activity input/output schema changed without an explicit compatibility decision
an implicit durable name changed because a function moved modules
two exported handlers collide on the same durable identity
```

Intentional changes should be accepted through the tool, not by hand-editing random generated output:

```text
cargo durable manifest accept --all
cargo durable manifest accept --workflow orders.order --version 2
cargo durable manifest accept --activity payments.charge-card
```

Accepting updates the local baseline to the current code after the developer has decided the change is safe. The tool should show the diff before writing unless `--yes` is passed.

Moving a function with an explicit durable name updates `rustPath` but preserves durable identity. Moving a function that relies on the default Rust-path-derived name appears as one removed identity and one added identity. `manifest check` fails until the developer restores the old path, adds an explicit durable name, or accepts the new baseline with an intentional migration plan.

The manifest check is a deployment guardrail. Runtime correctness still depends on durable names, versions, replay, and command fingerprints.

## 2.3 Supported workflow feature surface

Durust supports the durable coordination features directly inside normal workflow code:

```text
activities
manifest-backed activity maps
signals
projection queries updated from workflow code and read outside workflow execution
version/patch branches with normal if/match
child workflow spawn and wait
child workflow spawn and parent exit
timers
select over durable async operations
join/fanout over durable async operations
continue-as-new
```

The intended DX is ordinary Rust control flow around durable futures:

```rust
if durust::patched("new-payment-flow")? {
    durust::call_activity!(charge_v2(input)).await?;
} else {
    durust::call_activity!(charge_v1(input)).await?;
}

let outcome = durust::select! {
    approval = durust::signal::<Approval>("approved") => {
        ApprovalOutcome::Approved(approval?)
    }

    cancel = durust::signal::<Cancel>("cancel") => {
        ApprovalOutcome::Cancelled(cancel?)
    }

    _ = durust::sleep_until(deadline) => {
        ApprovalOutcome::TimedOut
    }
};

match outcome {
    ApprovalOutcome::Approved(approval) => approve(approval).await?,
    ApprovalOutcome::Cancelled(cancel) => return Err(cancel.into()),
    ApprovalOutcome::TimedOut => return Err(durust::Error::timeout("approval")),
}

let child = durust::child!(ship_order(input))
    .parent_close_policy(ParentClosePolicy::Cancel)
    .spawn()
    .await?;

let shipment = child.result().await?;
```

No feature should require users to write a state machine, manually inspect history, or pass an explicit workflow context. Durable APIs should preserve deterministic command order while letting users express coordination with `if`, `match`, loops, `select!`, and `join!`.

## 2.4 Developer example suite

Every core pattern should have a minimal, runnable example. Examples are part of the product surface, not optional demos.

Rules:

```text
one concept per example
small inputs and outputs
explicit worker registration
explicit client start/signal/query calls
no unrelated framework code
comments explain the durable behavior being demonstrated
each example has an integration test or snapshot test
```

Required examples:

```text
hello activity
worker registration with separate workflow and activity workers
signal wait
timer wait
select over approval, cancellation signal, and timeout
join over bounded parallel activities
query projection with publish
version branch with patched/get_version
child workflow spawn and wait
child workflow spawn and abandon
parent close policy cancellation
manifest-backed activity map
map reduce with partition, map manifest, and reduce manifest
continue-as-new for long histories
payload offloading through provider-owned PayloadRef handling
recovery after worker crash
SQLite provider setup for local testing
```

Example layout:

```text
examples/
  hello_activity.rs
  worker_registration.rs
  signal_wait.rs
  timer_wait.rs
  select_approval.rs
  join_activities.rs
  query_projection.rs
  version_branch.rs
  child_wait.rs
  child_abandon.rs
  parent_close_cancel.rs
  activity_map.rs
  map_reduce.rs
  continue_as_new.rs
  payload_offload.rs
  crash_recovery.rs
  sqlite_provider.rs
```

Each example should be copyable into a new project with minimal edits. If an API cannot be explained cleanly in one small example, revisit the API before expanding documentation.

---

# 3. Execution Model

## 3.1 Normal steady-state execution

When a workflow is hot on a worker:

```text
1. Worker has a pinned Rust Future for the workflow.
2. Durable activity/timer/signal/child futures return Pending when waiting.
3. Backend appends command events and active waits.
4. Activity/signal/timer/child completions append replay facts.
5. Worker polls the cached future again.
6. Local Rust variables remain in memory.
```

So in the common path, there is no replay and no local-state serialization.

Example:

```rust
let mut x = 0;

for i in 0..10 {
    x += durust::call_activity!(compute(i)).await?;
}
```

During normal execution, `x` is just a Rust local inside the pinned future.

## 3.2 Recovery execution

If the worker crashes or the workflow cache evicts the future:

```text
1. Load WorkflowStarted input.
2. Recreate the Rust future by calling the workflow function from the beginning.
3. Stream replay history in small chunks.
4. Durable APIs consume recorded facts instead of scheduling new work.
5. Rust code naturally reconstructs locals.
6. When the replay cursor reaches the tail, execution switches to live mode.
```

For the loop:

```rust
let mut x = 0;

for i in 0..10 {
    x += durust::call_activity!(compute(i)).await?;
}
```

Recovery streams:

```text
ActivityCompleted compute/0 -> returns 5  -> x = 5
ActivityCompleted compute/1 -> returns 7  -> x = 12
ActivityCompleted compute/2 -> returns 9  -> x = 21
...
```

Only the current locals and a small history buffer are in memory.

---

# 4. Workflow Worker Design

## 4.1 Workflow cache

```rust
pub struct WorkflowCacheEntry {
    pub run_id: RunId,
    pub workflow_type: WorkflowType,
    pub last_replayed_event_id: EventId,
    pub last_committed_event_id: EventId,
    pub future: Pin<Box<dyn WorkflowFuture>>,
    pub current_waits: Vec<WaitId>,
    pub last_accessed_at: Instant,
}
```

Properties:

```text
Cache is an optimization.
Cache entries are evictable.
Eviction never loses durable state.
A cache miss causes streaming replay.
Replay happens on cache miss, not at every durable wait boundary.
Keep non-terminal workflows resident while cache limits allow.
```

## 4.2 Worker loop

```text
1. Claim ready workflow task.
2. Check workflow cache.
3. If cached and current, feed new events into context.
4. If missing/stale, recreate future and stream replay from event 1.
5. Poll workflow future until:
   - it blocks on durable waits
   - it completes
   - it fails
   - it needs more history
6. Commit generated commands/facts atomically.
7. Keep future cached if still running.
```

## 4.3 Replay stream backpressure

The replay cursor must not bulk-load history.

```rust
pub trait HistoryStream {
    async fn next_chunk(
        &mut self,
        after: EventId,
        max_events: usize,
        max_bytes: usize,
    ) -> durust::Result<HistoryChunk>;
}
```

The workflow poll loop should support this state:

```rust
enum PollOutcome {
    BlockedOnDurableWait,
    Completed,
    Failed,
    NeedsMoreHistory { after: EventId },
    HasNewCommands,
}
```

If replay needs more history, the driver fetches the next segment and polls again.

## 4.4 Worker registration and task queues

Workers are local processes that register code they can execute and poll durable task queues. Workflow workers and activity workers may be the same process or separate processes.

Registration is local capability registration, not durable schema mutation:

```rust
let worker = durust::Worker::builder(backend.clone())
    .namespace("prod")
    .worker_id("orders-a")
    .workflow_task_queue("orders")
    .register_workflow(order)
    .activity_task_queue("payments")
    .register_activity(price_quote)
    .register_activity(charge_card)
    .max_local_activities_per_workflow_task(64)
    .max_cached_workflows(10_000)
    .max_concurrent_workflow_tasks(256)
    .max_concurrent_activities(512)
    .run()
    .await?;
```

Workflow-only worker:

```rust
durust::Worker::builder(backend.clone())
    .namespace("prod")
    .worker_id("order-workflows-a")
    .workflow_task_queue("orders")
    .register_workflow(order)
    .run()
    .await?;
```

Activity-only worker on another machine:

```rust
durust::Worker::builder(backend.clone())
    .namespace("prod")
    .worker_id("payment-activities-a")
    .activity_task_queue("payments")
    .register_activity(price_quote)
    .register_activity(charge_card)
    .run()
    .await?;
```

Client start chooses the workflow task queue:

```rust
let handle = durust::Client::new(backend.clone())
    .start(order(input))
    .workflow_id("order/123")
    .task_queue("orders")
    .await?;
```

Activity calls choose an activity task queue, either from activity metadata or per-call override:

```rust
let payment = durust::call_activity!(charge_card(input))
    .task_queue("payments")
    .await?;
```

Workflow code may set workflow-local defaults for subsequent activity calls:

```rust
durust::set_default_activity_options(
    durust::ActivityOptions::new()
        .task_queue("payments")
        .retry(durust::RetryPolicy::exponential().max_attempts(5))
        .timeout(std::time::Duration::from_secs(30))
        .heartbeat_timeout(std::time::Duration::from_secs(10)),
);
```

Defaults are part of deterministic workflow execution. They may be changed with
normal workflow control flow, apply only to later activity calls in that
workflow, and are superseded by explicit per-call options.

Queue matching rules:

```text
workflow tasks are claimed by workers polling the workflow's task_queue
activity tasks are claimed by workers polling the activity's task_queue
workers must only claim activity tasks whose activity_name is registered locally
workers must only claim workflow tasks whose workflow_type is registered locally
task queues are logical names, not process identities
multiple worker processes may poll the same task queue
one process may poll workflow queues, activity queues, or both
```

Local activity preference uses the same registration data. If a workflow worker
also has the requested activity registered for the selected activity task queue
and `max_local_activities_per_workflow_task` has available slots, it should
execute locally after the workflow task commit. Otherwise the scheduled activity
task remains available to remote workers polling that queue. Setting the local
limit to `0` disables local activity preference and forces remote dispatch.

Worker builder registration should fail before polling starts if two handlers register the same durable workflow identity or activity identity on the same worker. A worker that receives a task for an unregistered type should reject or release it without executing user code; provider conformance should ensure that such tasks can be claimed by a capable worker later.

## 4.5 Recovery stream API

Recovery should stream committed history in bounded chunks. It should not request one event row at a time in the normal path, and it should not bulk-load an entire workflow history.

Claiming a workflow task establishes the replay target:

```rust
pub struct ClaimedWorkflowTask {
    pub run_id: RunId,
    pub workflow_type: WorkflowType,
    pub claim: WorkflowTaskClaim,
    pub replay_target_event_id: EventId,
    pub reason: WorkflowTaskReason,
}
```

The worker replays up to `replay_target_event_id`:

```rust
pub struct StreamHistoryRequest {
    pub run_id: RunId,
    pub after_event_id: EventId,
    pub up_to_event_id: EventId,
    pub max_events: usize,
    pub max_bytes: usize,
}

pub struct HistoryChunk {
    pub events: Vec<HistoryEvent>,
    pub last_event_id: EventId,
    pub has_more: bool,
}
```

Provider behavior:

```text
return events with after_event_id < event_id <= up_to_event_id
honor max_events and max_bytes
return fewer events when a segment boundary or provider page boundary is reached
set has_more when more events remain at or below up_to_event_id
never include uncommitted rows
never include future events beyond up_to_event_id
```

Backends may internally read history segments, journal ranges, rows, pages, or object chunks. The runtime-facing API is chunked event streaming with byte and event backpressure.

External appends that commit after the claim's `replay_target_event_id` are not part of that recovery stream. They either produce a later workflow task or cause the current workflow task commit to return `CommitOutcome::Conflict`, after which the worker drops the recovered future and replays or catches up from the newer tail.

Live operational state is separate from replay history:

```text
unconsumed signal inbox rows are not streamed during replay
pending timer rows are not streamed during replay
activity tasks and leases are not streamed during replay
ready rows are not streamed during replay
```

When replay reaches `replay_target_event_id`, the workflow switches to live mode. Live durable futures may then consult bounded operational indexes:

```text
signal(name):
  read the lowest unconsumed matching signal, if any
  buffer SignalConsumed plus signal_id for the workflow task commit

timer:
  wait for TimerFired to be appended by the timer service

activity/child:
  wait for completion facts appended by activity or child dispatch
```

Those live reads do not mutate correctness state by themselves. The signal remains unconsumed until `commit_workflow_task` atomically appends `SignalConsumed` and consumes the signal id. If the worker crashes before commit, the signal remains available and the recovered workflow observes it again.

## 4.6 Recovery flow control

Streaming replay bounds per-workflow memory, but it does not by itself protect
the durability provider from a fleet-wide recovery storm. Recovery must have
explicit admission control and throughput limits separate from normal cached
workflow progress.

Worker/runtime policy owns semantic recovery throttling:

```text
max_concurrent_recoveries
max_replay_events_per_second
max_replay_bytes_per_second
recovery_prefetch_chunks
recovery_burst
per_queue or per_namespace recovery limits
separate cached-wake and cold-replay budgets
```

The worker must acquire recovery capacity before recreating a missing workflow
future and streaming history. Cached workflow wakes should not be starved behind
cold replay. When recovery budget is unavailable, the worker should release or
defer the workflow task through generic delayed visibility, not hold leases while
idle.

Provider implementations own physical protection for the storage system:

```text
honor max_events and max_bytes on every history stream request
enforce optional provider-wide read budgets by namespace, queue, or shard
return generic backpressure or retry-after signals when saturated
rate-limit provider startup replay and derived-index rebuilds
keep provider throttling independent of workflow semantics
```

Providers must not know whether replay is caused by cache eviction, worker
restart, nondeterminism retry, or deployment churn. They may expose generic
mechanisms such as delayed task visibility, recovery permits, stream budgets, or
retry-after responses. The worker chooses semantic policy and retry timing.

Provider conformance should include delayed release, bounded history stream
requests, and provider backpressure behavior. Simulation should include recovery
storms, cache eviction storms, provider read-budget exhaustion, and fairness
between hot cached workflows and cold recoveries.

---

# 5. History Model

## 5.1 History is segmented

Do not store history as one large row.

```text
history_segments
  namespace
  workflow_id
  run_id
  segment_id
  first_event_id
  last_event_id
  event_count
  byte_count
  compressed_payload_ref
  created_at
```

Default segment target:

```text
256 KiB to 4 MiB compressed
```

Configurable by backend.

## 5.2 History event envelope

```rust
pub struct HistoryEvent {
    pub event_id: EventId,
    pub event_time: Timestamp,
    pub event_type: HistoryEventType,
    pub payload_ref: PayloadRef,
}
```

## 5.3 Required event types

```rust
pub enum HistoryEventType {
    WorkflowStarted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowCancelled,
    WorkflowContinuedAsNew,

    WorkflowTaskStarted,

    ActivityScheduled,
    ActivityMapScheduled,
    ActivityMapCompleted,
    ActivityMapFailed,
    ActivityMapCancelled,
    ActivityCompleted,
    ActivityFailed,
    ActivityTimedOut,
    ActivityCancelled,

    TimerStarted,
    TimerFired,
    TimerCancelled,

    SignalConsumed,

    ChildWorkflowStartRequested,
    ChildWorkflowStarted,
    ChildWorkflowCompleted,
    ChildWorkflowFailed,
    ChildWorkflowCancelled,

    VersionMarker,
    PatchMarker,
    DeprecatedPatchMarker,

    SideEffectMarker,
    QueryStatePublished,

    ExternalSignalSent,
    ExternalCancellationRequested,
}
```

`SignalReceived` may exist in an audit/inbox table, but replay only needs `SignalConsumed`, because unconsumed signals should not force workflow-local state changes.

## 5.4 Command events versus completion facts

For deterministic replay, record both command scheduling and observed results.

Example activity lifecycle:

```text
ActivityScheduled {
  command_id,
  command_seq,
  activity_name,
  input_ref,
  options_hash,
}

ActivityCompleted {
  command_id,
  result_ref,
}
```

Example activity map lifecycle:

```text
ActivityMapScheduled {
  command_id,
  command_seq,
  activity_name,
  input_manifest_ref,
  result_manifest_ref,
  max_in_flight,
  options_hash,
}

ActivityMapCompleted {
  command_id,
  result_manifest_ref,
  item_count,
  success_count,
  failure_count,
}
```

Why record scheduling?

```text
It lets replay detect if code changed activity type, input, options, or command order.
It lets recovery know an activity was already scheduled but not yet completed.
It prevents duplicate scheduling.
```

---

# 6. Durable Command Matching

## 6.1 Command sequence

Each workflow run has a deterministic command sequence.

```rust
pub struct CommandId {
    pub run_id: RunId,
    pub command_seq: u64,
}
```

Each durable command encountered by workflow code increments `command_seq`.

During replay, the runtime expects the workflow code to emit the same command sequence.

## 6.2 Command fingerprint

Each command has a fingerprint:

```rust
pub struct CommandFingerprint {
    pub kind: CommandKind,
    pub site_id: Option<SiteId>,
    pub name: String,
    pub input_digest: Option<Sha256>,
    pub options_digest: Sha256,
}
```

During replay:

```text
If emitted command fingerprint != recorded command fingerprint:
    raise NondeterminismError
```

When a workflow task raises nondeterminism, the worker must abort that task
without appending `WorkflowFailed`. The worker releases the run with a
worker-configured retry backoff so bad workflow code does not hot-loop. Providers
must only enforce generic delayed workflow-task visibility; they do not classify
nondeterminism or choose retry durations.

This catches unsafe changes such as:

```text
renaming an activity
changing an activity input before a version branch
changing timer duration before a version branch
reordering command-producing awaits
removing a command-producing await
```

Temporal’s docs call out adding, removing, or reordering command-producing awaits, such as activities and timers, as common causes of nondeterminism. ([Temporal Docs][2])

## 6.3 Activity future behavior

For:

```rust
let quote = durust::call_activity!(price_quote(input)).await?;
```

The scheduled activity records the resolved task queue, retry policy,
per-attempt start-to-close timeout, and optional heartbeat timeout after merging
current workflow defaults with per-call overrides. The activity command
fingerprint includes that resolved option set, so changing defaults or
overrides before a recorded activity command is a nondeterministic replay change
unless it is protected by a version marker.

Heartbeat enforcement is disabled by default. If a scheduled activity has a
heartbeat timeout, the provider starts an operational heartbeat deadline when
the activity task is claimed. Activity code may call
`durust::heartbeat_activity().await?`; providers must accept only the currently
claimed activity token, reject stale heartbeat claims, and refresh the deadline.
Missed heartbeats are handled by the generic activity timeout scanner: retry
attempts are rescheduled according to the stored retry policy, and only the
terminal miss appends `ActivityTimedOut`.

Activity and workflow errors must be represented as a serializable Durust
failure envelope before they are written to history:

```text
DurableFailure {
  error_type
  message
  non_retryable
  details_payload_ref?
}
```

The worker converts returned `durust::Error` values into this envelope.
Providers do not classify application errors; they only honor the generic
`non_retryable` flag on a failed activity request. If `non_retryable` is true,
the provider records the terminal activity failure immediately even when the
stored retry policy has remaining attempts.

The durable future behaves like this:

```text
Replay mode:
  If ActivityScheduled exists:
      validate fingerprint.
  If ActivityCompleted exists:
      return recorded result.
  If ActivityFailed exists:
      return recorded durable failure.
  If ActivityTimedOut exists:
      return recorded timeout.
  If scheduled but incomplete:
      return Pending.
  If history cursor is at tail:
      schedule new activity and return Pending.

Live cached mode:
  If already scheduled and waiting:
      return Pending.
  If completion event was delivered:
      return Ready(result).
  If timeout event was delivered:
      return Ready(timeout error).
  If first poll:
      buffer ActivityScheduled command and return Pending.
```

## 6.4 Activity map scheduling

Large fanout should not require one workflow command per activity or one in-memory `Vec` of inputs. Provide one manifest-based API for map-style fanout:

```rust
let partitions = durust::call_activity!(partition_input(input.source_ref))
    .task_queue("storage")
    .await?;

let mapped = durust::activity_map(map_chunk)
    .task_queue("mappers")
    .input_manifest(partitions.manifest_ref)
    .max_in_flight(10_000)
    .result_manifest("partials")
    .spawn()
    .await?;

let output = durust::call_activity!(reduce_manifest(mapped.result_manifest().await?))
    .task_queue("reducers")
    .await?;
```

`map_chunk` is a normal user-defined activity. It is invoked once per item in the input manifest, with provider-managed leases, retries, and backpressure.

Semantics:

```text
one durable command schedules one manifest-backed map operation
the map command increments command_seq once
the map fingerprint includes activity name, input_manifest digest/ref, result_manifest config, max_in_flight, and options digest
workflow history records ActivityMapScheduled and terminal ActivityMapCompleted/Failed facts
workflow history does not record one ActivityScheduled or ActivityCompleted fact per item
the provider pages through the input manifest and materializes item tasks up to max_in_flight
each item gets deterministic identity: map_command_id + manifest item ordinal
per-item state lives in provider map/activity tables and indexes
results are written to the result manifest, not collected into workflow memory
replay reconstructs the same map handle from ActivityMapScheduled
workflow cancellation is recorded as WorkflowCancelled and atomically clears provider-owned waits, activity tasks, and activity-map state for the run
```

The canonical input is a paged manifest, not an iterator:

```text
input_manifest_ref -> ActivityMapInputManifest {
  item_count
  page_lengths
  pages: [PayloadRef<ActivityMapInputPage>, ...]
}

page 0 -> input refs 0..9999
page 1 -> input refs 10000..19999
page 2 -> input refs 20000..29999
```

Result manifests use the same root-plus-pages shape. `ActivityMapCompleted`
stores the result root manifest ref, and result pages contain ordered result
refs using the same page boundaries as the input manifest. This keeps both input
and result manifests out of one oversized provider row.

Small in-memory inputs should be converted to a manifest before scheduling:

```rust
let manifest_ref = durust::manifest::from_iter(chunk_refs)
    .spill_to_blob()
    .await?;

let mapped = durust::activity_map(map_chunk)
    .task_queue("mappers")
    .input_manifest(manifest_ref)
    .max_in_flight(100)
    .result_manifest("partials")
    .spawn()
    .await?;
```

The manifest helper is convenience sugar. The durable scheduling semantics remain manifest-based.

When called from workflow code, manifest creation must itself be a durable API with provider-owned payload handling and idempotent commit behavior. For large or externally sourced inputs, prefer an activity such as `partition_input` that writes the manifest and returns its ref.

Activity maps must preserve local activity preference. A workflow worker may
execute materialized map items locally when the map activity is registered
locally and `max_local_activities_per_workflow_task` has available slots;
otherwise materialized item tasks are claimed by remote activity workers on the
selected task queue.

Provider implementations must enforce manifest page limits, `max_in_flight`, retry policy, cancellation, and result manifest writes without loading all inputs or results into workflow memory or a single durable row.

## 6.5 Activity dispatch locality

Activity workers do not need to run on the same machine as the workflow worker.

Scheduling policy:

```text
If the activity type is registered locally and local capacity is available:
    run it on the same process/machine.
Otherwise:
    dispatch through the durable activity queue to a remote worker.
```

Local preference is only an optimization. The activity still uses the same
durable schedule event, idempotency key, retry policy, timeout, lease fencing,
and completion append as a remote activity. If local execution capacity is zero
or exhausted, the task remains eligible for remote workers according to the
backend's normal claiming rules.

---

# 7. Active Wait Indexes

History is for replay. Active wait indexes are for efficient wakeups.

```text
active_waits
  namespace
  workflow_id
  run_id
  wait_id
  command_id
  wait_kind
  wait_key
  ready_at
  state
  created_at
```

Indexes:

```text
timer index:
  ready_at -> run_id

activity index:
  activity_id -> run_id

signal index:
  workflow_id + signal_name -> run_id

child index:
  child_workflow_id + child_run_id -> parent_run_id

cancellation index:
  workflow_id -> run_id
```

Workers should not scan history to find ready workflows.

---

# 8. Backend Provider Trait

## 8.1 Core backend trait

The backend trait is a logical durability contract, not a SQLite-shaped API.

Provider implementations should optimize the happy path around appending compact durable records and updating the minimal indexes required to wake, query, and fence work. The API must not expose table names, SQL row ids, SQLite transaction modes, or any storage detail that would make Postgres, FoundationDB, DynamoDB, RocksDB, object-backed logs, or custom append logs second-class implementations.

```rust
#[async_trait::async_trait]
pub trait DurableBackend: Clone + Send + Sync + 'static {
    async fn start_workflow(
        &self,
        req: StartWorkflowRequest,
    ) -> durust::Result<StartWorkflowOutcome>;

    async fn cancel_workflow(
        &self,
        req: CancelWorkflowRequest,
    ) -> durust::Result<CancelWorkflowOutcome>;

    async fn current_time(&self) -> durust::Result<TimestampMs>;

    async fn claim_workflow_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimWorkflowTaskOptions,
    ) -> durust::Result<Option<ClaimedWorkflowTask>>;

    async fn stream_history(
        &self,
        req: StreamHistoryRequest,
    ) -> durust::Result<HistoryChunk>;

    async fn commit_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        batch: WorkflowTaskCommit,
    ) -> durust::Result<CommitOutcome>;

    async fn release_workflow_task(
        &self,
        claim: WorkflowTaskClaim,
        release: WorkflowTaskRelease,
    ) -> durust::Result<()>;

    async fn signal_workflow(
        &self,
        req: SignalWorkflowRequest,
    ) -> durust::Result<SignalWorkflowOutcome>;

    async fn read_signal_inbox(
        &self,
        req: ReadSignalInboxRequest,
    ) -> durust::Result<Option<SignalInboxRecord>>;

    async fn fire_due_timers(
        &self,
        req: FireDueTimersRequest,
    ) -> durust::Result<FireDueTimersOutcome>;

    async fn timeout_due_activities(
        &self,
        req: TimeoutDueActivitiesRequest,
    ) -> durust::Result<TimeoutDueActivitiesOutcome>;

    async fn claim_activity_task(
        &self,
        worker_id: WorkerId,
        opts: ClaimActivityOptions,
    ) -> durust::Result<Option<ClaimedActivityTask>>;

    async fn heartbeat_activity(
        &self,
        req: ActivityHeartbeatRequest,
    ) -> durust::Result<ActivityHeartbeatOutcome>;

    async fn complete_activity(
        &self,
        req: CompleteActivityRequest,
    ) -> durust::Result<CompleteActivityOutcome>;

    async fn fail_activity(
        &self,
        req: FailActivityRequest,
    ) -> durust::Result<FailActivityOutcome>;

    async fn dispatch_child_workflow_starts(
        &self,
        req: DispatchChildWorkflowStartsRequest,
    ) -> durust::Result<DispatchChildWorkflowStartsOutcome>;

    async fn query_projection(
        &self,
        req: QueryProjectionRequest,
    ) -> durust::Result<QueryProjectionOutcome>;

    async fn workflow_change_versions(
        &self,
        req: WorkflowChangeVersionsRequest,
    ) -> durust::Result<WorkflowChangeVersionsOutcome>;
}
```

`read_signal_inbox` is a live-tail read for workflow code that reaches `signal(...)` after replay has caught up. It must return a bounded result, usually one record, and it must not mark the signal consumed. Consumption happens only through `commit_workflow_task.consume_signals` in the same atomic commit that appends the corresponding `SignalConsumed` replay fact.

Claim options carry local worker capabilities so providers do not hand out tasks that the worker cannot execute:

```rust
pub struct ClaimWorkflowTaskOptions {
    pub namespace: Namespace,
    pub task_queue: TaskQueue,
    pub registered_workflow_types: Vec<WorkflowType>,
    pub lease_duration: Duration,
}

pub struct ClaimActivityOptions {
    pub namespace: Namespace,
    pub task_queue: TaskQueue,
    pub registered_activity_names: Vec<ActivityName>,
    pub lease_duration: Duration,
}
```

The provider should match both queue and registered type/name. If no matching task exists, it returns `Ok(None)`. A task for an unregistered type must remain claimable by another worker that advertises the matching capability.

## 8.2 Workflow task commit

```rust
pub struct WorkflowTaskCommit {
    pub expected_tail_event_id: EventId,

    pub append_events: Vec<NewHistoryEvent>,

    pub upsert_waits: Vec<WaitRecord>,
    pub delete_waits: Vec<WaitId>,

    pub schedule_activities: Vec<ActivityTask>,
    pub schedule_activity_maps: Vec<ActivityMapTask>,
    pub start_child_workflows: Vec<ChildStartOutboxMessage>,

    pub consume_signals: Vec<SignalId>,
    pub cancel_commands: Vec<CommandId>,

    pub query_projection: Option<QueryProjectionUpdate>,

    pub visibility_patch: VisibilityPatch,
}
```

```rust
pub struct ActivityMapTask {
    pub map_command_id: CommandId,
    pub activity_name: ActivityName,
    pub task_queue: TaskQueue,
    pub input_manifest: PayloadRef,
    pub result_manifest_name: String,
    pub max_in_flight: usize,
    pub start_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub retry_policy_ref: Option<PayloadRef>,
    pub options_hash: Sha256,
}

pub struct ActivityMapItemId {
    pub map_command_id: CommandId,
    pub item_ordinal: u64,
}
```

The provider persists a compact map descriptor and materializes item tasks from
manifest pages subject to `max_in_flight`. The root input manifest contains page
refs and page lengths; providers should load only the page needed for the item
range they are materializing. Result manifests are written as root-plus-page
payloads. The runtime contract is that each item is independently claimable,
lease-fenced, retryable, and completable by `ActivityMapItemId`, while workflow
history remains compact at the map-operation level.

`dispatch_child_workflow_starts` is the provider-neutral child outbox drain. It
claims a bounded number of durable child-start messages, starts each child
idempotently, and appends the corresponding `ChildWorkflowStarted` or
`ChildWorkflowFailed` fact to the parent run. Providers must keep this generic:
the runtime chooses child options and parent close policy; the provider only
persists and dispatches durable visibility.

## 8.3 Atomicity requirement

The backend must atomically:

```text
1. verify workflow task fence token
2. verify expected_tail_event_id
3. append history events
4. update active waits
5. create activity tasks
6. create activity map descriptors
7. enqueue child outbox messages
8. consume signals
9. update query projection
10. mark workflow ready/not ready
```

If another event was appended concurrently, return:

```rust
CommitOutcome::Conflict
```

The worker must drop the cached future and replay from the new tail.

## 8.4 Append-journal provider shape

The preferred storage shape follows the same performance goal as the durability store in `../durable-phases`: accepted mutations append to a journal/log in the happy path, with operational indexes maintained as derived state.

Provider requirements:

```text
append accepted workflow-visible facts in event_id order
append external completions and signals idempotently
atomically update active wait, ready, lease, idempotency, and query indexes
stream ordered history by run without loading the full log
allow implementation-specific compaction outside the runtime contract
```

The runtime contract is append journal only. Backends may keep caches, materialized indexes, or compacted internal representations, but workflow recovery must be based on the start input plus the ordered append history. Provider conformance should prove that deleting in-memory runtime state and replaying from the append history is sufficient.

A SQLite implementation is required for tests and local development. It should exercise the same backend trait and append-journal semantics as production providers, but its schema must not define the public provider abstraction.

## 8.5 Scale-out, shards, and outbox handoff

Mega-scale providers should partition runtime state by logical shards.

Shard key:

```text
namespace
+ workflow_id
+ run_id
```

Default shard assignment:

```text
shard_id = hash(namespace, workflow_id, run_id) % shard_count
```

The authoritative execution state for one workflow run should live on one logical shard:

```text
workflow instance row
append history for the run
active waits
ready workflow task row
signal inbox for the run
activity map descriptors owned by the run
child outbox rows owned by the run
idempotency records scoped to the run
query projection for the run
```

This keeps `commit_workflow_task` shard-local. The provider must not require a distributed transaction to commit a normal workflow task.

Workers claim shard leases or queue leases depending on provider design:

```rust
pub struct ShardLease {
    pub shard_id: ShardId,
    pub owner_id: WorkerId,
    pub lease_epoch: u64,
    pub lease_until: Timestamp,
}
```

Provider implementations may expose shard leasing internally rather than in the public runtime API, but conformance must prove stale lease owners cannot commit workflow tasks, activity completions, or map item completions.

Cross-shard operations use transactional outbox/inbox handoff:

```text
1. Source shard commits local workflow state and an outbox message atomically.
2. Dispatcher reads undispatched outbox messages.
3. Dispatcher delivers to the target shard inbox idempotently.
4. Target shard applies the message and writes any target-side state.
5. Source shard records dispatch/ack state idempotently.
```

Use outbox/inbox handoff for:

```text
parent starting child on another shard
child completion notifying parent on another shard
parent cancellation propagating to child on another shard
signal routing when the caller lands on a non-owner shard
activity map item completion routed back to owner shard if item execution is partitioned elsewhere
```

Outbox message identity must be stable:

```text
source_shard_id
+ source_run_id
+ command_id
+ message_kind
+ target_run_id or target_key
```

Target inbox application must be idempotent. Dispatchers may crash, retry, duplicate messages, or deliver messages out of order. Correctness comes from stable message identity, target-side idempotency, and source/target shard-local commits.

Cross-shard handoff affects visibility latency, not correctness. A committed source outbox row is enough to recover and eventually deliver the message.

Physical partitions are provider implementation details. Logical shards are the runtime scaling unit; providers may map many logical shards onto a database partition, Kafka partition, RocksDB instance, or object-backed log segment.

---

# 9. External Event Appends

External completions may append history independently of workflow-task commits.

Examples:

```text
activity completes
timer fires
child completes
workflow receives cancellation
```

These appends must be atomic and idempotent.

Example activity completion:

```text
1. verify activity exists
2. verify activity not already terminal
3. write ActivityCompleted event to workflow history
4. delete/update activity active wait
5. mark workflow ready
6. store activity result payload
```

If a workflow task was concurrently running against the previous tail, its commit will conflict and replay.

---

# 10. Signals

## 10.1 Signal send

External signal send writes to durable inbox:

```text
signals
  workflow_id
  run_id
  signal_id
  signal_name
  payload_ref
  idempotency_key
  received_sequence
  consumed_at
```

If matching active signal wait exists:

```text
mark workflow ready
```

Do not necessarily append to replay history yet.

## 10.2 Signal consume

When workflow code executes:

```rust
let approval = durust::signal::<Approval>("approved").await?;
```

At tail, if a matching signal is available:

```text
1. read lowest received_sequence matching signal with read_signal_inbox
2. return payload to workflow and buffer SignalConsumed
3. commit SignalConsumed and consume signal_id atomically
```

During replay:

```text
SignalConsumed event returns the recorded payload.
```

This keeps unconsumed signals out of replay history.

If multiple matching signals are already waiting, the runtime must not load them all as part of recovery. Each `signal(...).await` consumes at most one matching signal.

---

# 11. Timers

Core timers are UTC instants.

```rust
durust::sleep(Duration::from_secs(30)).await;
durust::sleep_until(deadline_utc).await;
```

Scheduling appends:

```text
TimerStarted { command_id, fire_at }
```

Timer service later appends:

```text
TimerFired { command_id, fired_at }
```

Replay validates `TimerStarted` and returns when `TimerFired` is reached.

A pending timer is an active wait index row, not a future history row. Recovery streams `TimerStarted` and any committed `TimerFired` event at or below the claimed replay target. If the timer fires after that target while recovery is running, the timer service appends `TimerFired` as a later event; the current workflow task will catch it through a later wakeup or a commit conflict.

For wall-clock schedules, require explicit timezone ambiguity policies:

```rust
durust::schedule::daily_at("America/Los_Angeles", LocalTime::from_hms(9, 0, 0))
    .on_ambiguous_time(AmbiguousTimePolicy::Earlier)
    .on_nonexistent_time(NonexistentTimePolicy::NextValid);
```

---

# 12. Select

Do not use `tokio::select!`.

Provide:

```rust
let outcome = durust::select! {
    approval = durust::signal::<Approval>("approved") => {
        ApprovalOutcome::Approved(approval?)
    }

    cancel = durust::signal::<Cancel>("cancel") => {
        ApprovalOutcome::Cancelled(cancel?)
    }

    _ = durust::sleep_until(deadline) => {
        ApprovalOutcome::TimedOut
    }
};
```

Branches may wait on any durable future:

```text
activity completion
signal receive
timer fire
child start
child result
workflow cancellation
future created by deterministic workflow-local spawn
```

Semantics:

```text
1. Branches are registered in lexical order.
2. Each branch creates a deterministic command/wait.
3. The selected winner's branch ordinal is recorded in history.
4. Replay uses the recorded branch ordinal to evaluate the same branch body.
5. Losing waits are cancelled or ignored according to policy.
```

The value returned by `select!` is the value returned by the winning branch body. Branches should usually return an explicit enum when the caller needs to know which path won:

```rust
enum ApprovalOutcome {
    Approved(Approval),
    Cancelled(Cancel),
    TimedOut,
}
```

A signal named `cancel` is just a user-defined signal. It does not automatically cancel or fail the workflow. The workflow is cancelled only when an external cancellation request is recorded or when user code maps a signal into a terminal return/error.

Replay event:

```rust
SelectWinner {
    select_command_id: CommandId,
    branch_ordinal: u32,
    winning_event_id: EventId,
}
```

Tie-break:

```text
1. earliest history event id
2. lexical branch order
```

---

# 13. Child Workflows

Spawn and wait:

```rust
let child = durust::child!(ship_order(input))
    .workflow_id(format!("ship/{}", order_id))
    .parent_close_policy(ParentClosePolicy::Cancel)
    .spawn()
    .await?;

let shipment = child.result().await?;
```

Spawn and let the parent exit without cancelling the child:

```rust
let child = durust::child!(send_receipt(input))
    .workflow_id(format!("receipt/{}", order_id))
    .parent_close_policy(ParentClosePolicy::Abandon)
    .spawn()
    .await?;

durust::publish(&OrderView {
    receipt_run_id: Some(child.run_id().clone()),
    ..view
})?;
```

Events:

```text
ChildWorkflowStartRequested
ChildWorkflowStarted
ChildWorkflowCompleted
ChildWorkflowFailed
```

Use outbox/inbox to avoid distributed transactions:

```text
1. Parent workflow task appends ChildWorkflowStartRequested.
2. Backend writes child-start outbox message.
3. Dispatcher starts child idempotently.
4. Dispatcher appends ChildWorkflowStarted to parent.
5. Child completion appends ChildWorkflowCompleted to parent.
```

`spawn().await` resolves after `ChildWorkflowStarted`.

`result().await` resolves after child completion.

Parent close policy:

```rust
pub enum ParentClosePolicy {
    Cancel,
    Abandon,
}
```

`Cancel` is the default. When the parent reaches a terminal state, running children with `Cancel` receive a cancellation request. Children with `Abandon` keep running and can be observed through their own workflow id/run id or an explicit query/projection written by the parent before exit.

The child start itself must be durable before `spawn().await` returns. A parent that exits after `spawn().await` and before `result().await` must apply the configured parent close policy; it must not silently orphan a child unless `Abandon` was requested.

---

# 14. Query Model

Because workflow locals live inside an opaque Rust future, generic low-latency queries should use explicit projections.

```rust
#[derive(Serialize, Deserialize)]
pub struct OrderView {
    pub status: OrderStatus,
    pub quote: Option<QuoteSummary>,
}

durust::publish(&view)?;
```

Query handler:

```rust
#[durust::query(workflow = order)]
pub fn status(view: &OrderView) -> OrderStatus {
    view.status.clone()
}
```

Query reads:

```text
latest committed query projection
```

No replay required.

Queries are not durable workflow futures. They should not be called from workflow code, participate in `select!`, or change workflow state. Workflow code updates queryable state with `durust::publish(&view)`, and external callers read the latest committed projection through generated query APIs.

Projection queries are the query model. They read committed query state and avoid replaying workflow code for user-facing reads.

---

# 15. Versioning and Patching

This is the key part for safe workflow code changes without keeping old worker binaries online.

Temporal’s Go SDK `GetVersion` records a marker in Event History for new executions, and future calls for that change ID return the recorded version; workflows that had already passed the call before it was introduced return `DefaultVersion`, while new workflows return the newer max version. ([Temporal Docs][3])

Implement the same concept.

## 15.1 API

```rust
pub const DEFAULT_VERSION: i32 = -1;

let v = durust::get_version("charge-flow", DEFAULT_VERSION, 2)?;

match v {
    DEFAULT_VERSION => {
        // old path
    }

    1 => {
        // version 1 path
    }

    2 => {
        // new path
    }

    _ => unreachable!(),
}
```

Boolean sugar:

```rust
if durust::patched("new-charge-flow")? {
    // new path
} else {
    // old path
}
```

Deprecation bridge:

```rust
durust::deprecate_patch("new-charge-flow")?;
```

## 15.2 `get_version` semantics

```rust
durust::get_version(change_id, min_supported, max_supported)
```

During replay:

```text
If VersionMarker(change_id, version) exists:
    if version < min_supported or version > max_supported:
        fail with UnsupportedWorkflowVersion
    return version

If no marker exists and replay cursor is not at tail:
    if DEFAULT_VERSION < min_supported:
        fail with UnsupportedWorkflowVersion
    return DEFAULT_VERSION

If replay cursor is at tail:
    append VersionMarker(change_id, max_supported)
    return max_supported
```

The marker is part of command history and participates in deterministic replay.
Because `get_version` is a synchronous workflow API while recovery streams
history in bounded chunks, workers preload the provider-maintained
`workflow_change_versions` index for the claimed run. This index is bounded by
the number of recorded change markers, not by history length. If a marker exists
but its event has not been streamed yet, the runtime returns the indexed version
and records that marker as pre-consumed; when the marker event later reaches the
replay cursor, the runtime validates and skips it before matching subsequent
commands. This preserves deterministic command order without loading full
history.

Unsupported workflow versions and marker-order mismatches abort the workflow
task. They must not append `WorkflowFailed`; the worker releases the task with a
retry backoff just like nondeterminism.

## 15.3 `patched` semantics

```rust
durust::patched("id")
```

Equivalent to:

```rust
durust::get_version("id", DEFAULT_VERSION, 1)? != DEFAULT_VERSION
```

## 15.4 Deployment flow

### Stage 1: original code

```rust
let result = durust::call_activity!(activity_a(input)).await?;
```

### Stage 2: patch in new code

```rust
if durust::patched("replace-a-with-b")? {
    let result = durust::call_activity!(activity_b(input)).await?;
} else {
    let result = durust::call_activity!(activity_a(input)).await?;
}
```

One new worker binary can handle both old and new executions.

No old worker binary needs to stay online.

### Stage 3: after no open workflows need the old branch

```rust
durust::deprecate_patch("replace-a-with-b")?;

let result = durust::call_activity!(activity_b(input)).await?;
```

### Stage 4: after no histories with the patch marker remain relevant

```rust
let result = durust::call_activity!(activity_b(input)).await?;
```

Temporal documents the same general patching lifecycle: introduce the patch branch, later deprecate it, and only remove the deprecation bridge after old executions are gone. ([Temporal Docs][2])

Important limitation:

```text
Patching removes the need to keep old worker binaries online.
It does not remove the need to keep old branch code until no open histories need that branch.
```

That is unavoidable with deterministic replay.

## 15.5 Version marker index

Maintain an index so users know when it is safe to remove branches:

```text
workflow_change_versions
  namespace
  workflow_type
  workflow_id
  run_id
  change_id
  version
  marker_kind
  command_seq
  status
  first_event_id
  last_seen_at
```

CLI:

```text
cargo durable versions list order
cargo durable versions check --change-id replace-a-with-b
cargo durable versions safe-to-remove --change-id replace-a-with-b
```

---

# 16. Determinism Rules

Replay-first removes macro-lowering fragility, but workflow code still must be deterministic.

Forbidden in workflow code:

```rust
tokio::time::sleep(...)
tokio::select! { ... }
tokio::spawn(...)
std::time::SystemTime::now()
std::time::Instant::now()
rand::random()
reqwest::get(...).await
db.query(...).await
ordinary_future.await
HashMap iteration for command-producing logic
```

Use durable replacements:

```rust
durust::sleep(...)
durust::select! { ... }
durust::spawn(...)
durust::now()
durust::side_effect!(...)
durust::call_activity!(...)
BTreeMap or sorted Vec
```

The macro/lint layer should be fail-closed in strict mode:

```rust
#[durust::workflow(strict)]
```

The `#[workflow]` macro should run a best-effort AST lint pass over the annotated workflow function. Strict mode should reject:

```text
unknown .await
tokio APIs
system time
randomness
native spawn
native select
known network/database calls
```

Diagnostics should point to the offending expression and suggest the durable replacement:

```text
tokio::time::sleep      -> durust::sleep
std::time::Instant::now -> durust::now
std::time::SystemTime   -> durust::now
tokio::select!          -> durust::select!
tokio::spawn            -> durust::spawn or durust::join!
rand/uuid generation    -> durust::side_effect!
direct network/db calls -> durust::call_activity!
```

The lint is intentionally a guardrail, not the correctness mechanism. It may miss nondeterminism hidden behind helper functions, aliases, trait dispatch, dependencies, or data-structure iteration that is not syntactically obvious. Correctness still depends on replay and command fingerprint checks detecting divergent durable command sequences.

---

# 17. Side Effects and Workflow Time

## 17.1 Deterministic time

```rust
let now = durust::now();
```

`durust::now()` returns deterministic workflow time derived from recorded workflow-task/event timestamps, not system time.

## 17.2 Side effect marker

```rust
let id = durust::side_effect("make-id", || Uuid::new_v4())?;
```

Behavior:

```text
Replay with marker:
    return recorded value

At tail:
    run closure
    append SideEffectMarker
    return value
```

The closure may run again if the workflow task crashes before commit, so it must not perform external side effects. External side effects belong in activities.

---

# 18. Payload Offloading

Every event payload is a `PayloadRef`.

The public workflow, activity, signal, child, and query APIs are blind to inline versus blob storage. User code passes and receives typed payloads. The durability provider implementation owns serialization, compression, encryption, inline/blob selection, blob upload/download, and `PayloadRef` persistence.

```rust
pub enum PayloadRef {
    Inline {
        codec: CodecId,
        schema_fingerprint: SchemaFingerprint,
        compression: CompressionId,
        encryption: Option<EncryptionMetadata>,
        bytes: Bytes,
    },

    Blob {
        codec: CodecId,
        schema_fingerprint: SchemaFingerprint,
        compression: CompressionId,
        encryption: Option<EncryptionMetadata>,
        digest: Sha256Digest,
        size: u64,
        uri: String,
    },
}
```

Pipeline:

```text
serde encode with configured codec
optional compression
optional encryption
inline if small
blob if large
```

Default durable payload codec:

```text
MessagePack via rmp-serde
```

Supported codec policy:

```text
MessagePack:
  default durable payload codec.
  Serde-native, compact, fast, and portable.

JSON:
  supported for debugging, export, CLI inspection, and explicit provider config.
  not the default durable payload codec.

Protobuf:
  opt-in codec for users who want explicit generated schemas.
  not the default because it changes the user type contract.

FlatBuffers:
  not a default codec.
  only add if a benchmarked zero-copy use case needs it.
```

Default inline threshold:

```text
4 KiB to 16 KiB, backend configurable
```

Provider config should expose this as an explicit knob:

```rust
pub struct PayloadStorageConfig {
    pub codec: CodecId,
    pub inline_threshold_bytes: usize,
    pub blob_store: Option<BlobStoreConfig>,
}

pub enum CodecId {
    MessagePack,
    Json,
    #[cfg(feature = "protobuf-codec")]
    Protobuf,
}

pub enum BlobStoreConfig {
    S3Compatible {
        bucket: String,
        endpoint: Option<String>,
        region: String,
        prefix: String,
    },
}
```

Providers must implement `MessagePack` and `Json`. `MessagePack` is the default for durable payloads. `Json` is required for debug/export flows and explicit provider configuration. `Protobuf` is reserved for an opt-in feature that uses explicit generated schemas.

Durability implementation docs should recommend offloading larger workflow inputs, activity parameters, activity results, signals, child results, side effects, and query projections to object storage such as S3, then storing only a `PayloadRef` in the durable store. Large DB rows reduce write throughput, increase WAL/journal pressure, and make hot indexes and history scans more expensive.

The inline threshold is a performance default, not a correctness boundary. Backends may choose a smaller limit for databases where row size strongly affects write amplification.

The SQLite provider should support `PayloadStorageConfig` and an S3-compatible blob store. Tests should use local Garage as the S3-compatible service so SQLite-plus-blob behavior is covered without depending on AWS.

Provider conformance should test both inline and blob-backed payloads through the same public API so application code cannot accidentally depend on where the bytes are stored. Conformance should force both paths by setting a tiny inline threshold and then a larger threshold.

Providers should expose generic payload garbage collection for provider-owned
blob stores. GC treats workflow history, activity tasks, activity map manifests
and results, child outbox entries, signal inbox rows, and query projections as
roots. A dry-run mode must report retained and deleted blob counts without
mutating storage. If a committed reachable `PayloadRef::Blob` is missing or
fails digest/size validation, GC must fail rather than deleting unrelated blobs.

---

# 19. Storage Shape

This is a logical storage shape, not a required table schema. Implementations may store accepted mutations as an append journal plus derived indexes, normalized relational tables, partitioned logs, or an embedded engine layout, as long as they satisfy the backend trait and replay semantics.

```text
workflow_instances
  namespace
  shard_id
  workflow_id
  run_id
  workflow_type
  status
  current_event_id
  task_queue
  query_projection_ref
  output_ref
  failure_ref
  created_at
  updated_at

history_segments
  namespace
  shard_id
  workflow_id
  run_id
  segment_id
  first_event_id
  last_event_id
  event_count
  byte_count
  payload_ref

active_waits
  namespace
  shard_id
  workflow_id
  run_id
  wait_id
  command_id
  wait_kind
  wait_key
  ready_at
  state

ready_workflows
  namespace
  shard_id
  workflow_id
  run_id
  latest_event_id
  reason
  lease_owner
  lease_until

signals
  namespace
  shard_id
  workflow_id
  run_id
  signal_id
  signal_name
  payload_ref
  idempotency_key
  received_sequence
  consumed_at

activities
  activity_id
  workflow_id
  run_id
  command_id
  map_command_id
  map_item_ordinal
  activity_name
  task_queue
  input_ref
  result_ref
  failure_ref
  attempt
  retry_policy_ref
  lease_owner
  lease_until
  status

activity_maps
  workflow_id
  run_id
  map_command_id
  activity_name
  task_queue
  input_manifest_ref
  result_manifest_ref
  max_in_flight
  item_count
  success_count
  failure_count
  status

child_outbox
  outbox_id
  source_shard_id
  target_shard_id
  parent_workflow_id
  parent_run_id
  command_id
  child_workflow_id
  child_run_id
  payload_ref
  idempotency_key
  dispatched_at

cross_shard_inbox
  target_shard_id
  message_id
  source_shard_id
  source_run_id
  target_run_id
  message_kind
  payload_ref
  applied_at

shard_leases
  shard_id
  owner_id
  lease_epoch
  lease_until

workflow_change_versions
  namespace
  workflow_type
  workflow_id
  run_id
  change_id
  version
  status
  first_event_id
  last_seen_at

query_projections
  namespace
  workflow_id
  run_id
  event_id
  payload_ref

idempotency
  scope
  key
  result_ref
  expires_at
```

---

# 20. Concurrency

## 20.1 Bounded coordination

Support:

```rust
durust::select! { ... }
durust::join!(...)
durust::join_all(branches)
durust::select_all(branches)
```

`durust::join!` should register multiple durable operations in deterministic order, then wait until all have completed. It is the bounded fanout-and-collect primitive for launching all branches before observing any one result.

Example:

```rust
let (a, b) = durust::join!(
    durust::call_activity!(task_a()),
    durust::call_activity!(task_b()),
).await?;
```

Plain Rust futures are lazy, so creating variables and then awaiting them one by one must not be treated as concurrent durable launch. If code awaits `task_a` before `task_b` has been registered, then the operations are sequential. For bounded concurrent launch, use `durust::join!` so the runtime registers every branch in deterministic lexical order before waiting for completions.

`join!` should work over the same durable future families as `select!`: activities, signals, timers, child starts, child results, and deterministic workflow-local spawned futures.

Use `join_all` for bounded dynamic collect-all fanout when the branch count is
known at runtime:

```rust
let mut branches = Vec::new();
for item in items {
    let handle = durust::call_activity!(work_item(item)).spawn().await?;
    branches.push(handle.result());
}

let outputs = durust::join_all(branches).await?;
```

`join_all` polls and registers branches in deterministic collection order and
returns outputs in that same order, regardless of completion order. It records no
extra replay fact; each branch's own command fingerprint and terminal event are
the replay contract. Plain Rust futures must not be accepted. Empty input
returns an empty `Vec`.

Use explicit spawned activity handles when users need to launch work now and collect later:

```rust
let a = durust::call_activity!(task_a()).spawn().await?;
let b = durust::call_activity!(task_b()).spawn().await?;

let a = a.result().await?;
let b = b.result().await?;
```

`spawn().await` emits the deterministic `ActivityScheduled` command immediately.
The backend must make that schedule durable in the same atomic workflow-task
commit before the activity can execute. The handle can then be awaited
sequentially because the durable work has already been launched.

Use `select_all` for bounded dynamic races and worker-pool style refill loops:

```rust
let mut branches = Vec::new();
for item in items {
    let handle = durust::call_activity!(score(item)).spawn().await?;
    branches.push(handle.result().map_ok(RaceWinner::Score).boxed());
}
branches.push(durust::sleep(deadline).map_ok(|_| RaceWinner::Timeout).boxed());

let winner = durust::select_all(branches).await?;
```

`select_all` accepts a deterministic ordered collection of durable select
branches whose outputs have been mapped to one Rust type. It records the winner
with the same `SelectWinner` replay fact as `select!`; the branch ordinal is the
collection index. Winner choice is by earliest ready event id, then branch
ordinal. Reordering the collection or changing which event wins is detected
during replay through the recorded ordinal and event id. The branch-count digest
detects obvious collection-size changes; command fingerprints remain the
backstop for activity/timer/signal/child command changes.

Loser policy matches `select!`: pending timers and signals remove waits,
pending spawned activity results cancel their activity command, and child-result
losers are ignored rather than implicitly cancelling the child. Already-ready
losing terminal facts may be ignored during replay before the recorded
`SelectWinner`.

`select_all` is not the large collect-all primitive. For very large activity
fanout, use manifest-backed `activity_map` so workflow history and workflow
memory stay bounded.

## 20.2 V2 deterministic fibers

Support:

```rust
let h1 = durust::spawn(async move {
    durust::call_activity!(task_a()).await
});

let h2 = durust::spawn(async move {
    durust::signal::<Approval>("approved").await
});

let result = durust::select! {
    r = h1 => r?,
    approval = h2 => approval?,
};
```

Internally:

```text
No Tokio task.
No OS thread.
No nondeterministic scheduler.
```

Use a deterministic workflow-local scheduler:

```rust
pub struct WorkflowFiber {
    pub fiber_id: FiberId,
    pub future: Pin<Box<dyn Future<Output = FiberOutput>>>,
    pub status: FiberStatus,
}
```

Polling order:

```text
lowest fiber_id first
```

Command IDs include:

```text
run_id + fiber_id + command_seq
```

---

# 21. Continue-As-New

Even though there is no hard history length limit, users need a way to cap recovery time.

```rust
if durust::history().event_count() > 100_000 {
    return durust::continue_as_new(SumInput {
        start_i: i + 1,
        initial_x: x,
        n: input.n,
    });
}
```

`continue_as_new`:

```text
1. completes current run with WorkflowContinuedAsNew
2. starts new run with same workflow_id
3. resets replay history
4. passes compacted state as new input
```

This is not required for correctness. It is an operational latency tool.
The continued run keeps the same workflow type, task queue, and parent link when
the continuing workflow is itself a child. A parent waiting on that child is
not notified by the intermediate `WorkflowContinuedAsNew`; it is woken only
when the latest continued run completes, fails, or is cancelled. Query
projections are keyed by workflow id, so the last committed projection remains
visible across the transition until the new run publishes a replacement.

---

# 22. Testing Plan, Including DST

DST here means deterministic simulation testing.

The deterministic simulation harness is part of the core test model and is used for each feature as it lands. The implementation plan expands fault profiles and seed counts as runtime behavior grows.

## 22.1 Replay tests

Required:

```text
replay simple workflow
replay loop with 10,000 activity completions
stream history in tiny chunks
stream history up to a claimed tail watermark
do not stream rows beyond the claimed tail watermark
stream large payload refs lazily
recover with no full-history allocation
recover after cache eviction
recover after worker crash
recover without loading unconsumed signal inbox rows
recover without loading pending timer rows
detect activity reorder nondeterminism
detect changed activity input fingerprint
detect removed timer
detect changed select branch order
```

## 22.2 Versioning tests

Required:

```text
get_version returns DEFAULT_VERSION for old history
get_version records max version at tail
recorded version is stable on replay
unsupported min version fails
patched returns false for pre-patch history
patched returns true for new history
deprecate_patch bridges existing patched histories
removing patch too early causes nondeterminism
version marker index is updated
safe-to-remove query works
```

## 22.3 Streaming tests

Required:

```text
history chunk size = 1 event
history chunk size = 1 byte-ish boundary
large compressed segments
blob payloads fetched only when observed
prefetch buffer stays bounded
replay pauses to fetch more history
```

## 22.4 Active wait tests

Required:

```text
activity completion wakes workflow
timer firing wakes workflow
timer firing during recovery appends a later event or causes commit conflict
signal matching active wait wakes workflow
signal before wait is buffered
signal before wait is read at live tail and consumed only on commit
child completion wakes parent
duplicate ready rows do not double-run workflow
```

## 22.5 Crash/fault tests

Inject crashes at:

```text
after command generated before commit
after history append before ready update
after activity completion append before wake
after signal append before wake
after timer fire before wake
after child outbox dispatch before mark dispatched
after source outbox commit before cross-shard dispatch
after target inbox write before apply
after target apply before source ack
after payload upload before history commit
after history commit before worker ack
```

Invariants:

```text
no duplicate activity schedule for same command
no duplicate signal consumption
no missing committed payload ref
no two events share event_id
workflow event_id strictly increases
stale workflow task commit fails
stale activity lease completion fails
completed workflow cannot append new commands
```

## 22.6 Deterministic simulation

```rust
durable_test::sim()
    .seed(12345)
    .workers(8)
    .activity_workers(8)
    .history_chunk_events(3)
    .fault_profile(FaultProfile::Aggressive)
    .run(order_scenario())
    .assert_invariants();
```

Run profiles:

```text
no faults
cache eviction storm
worker crash storm
activity duplicate completion
timer duplicate fire
signal storm
history streaming tiny chunks
blob store transient errors
commit conflicts
shard lease loss
cross-shard outbox duplicate delivery
cross-shard outbox delayed delivery
child fanout
version patch rollout
```

CI should run many seeds.

## 22.7 Example tests

Every public example should be compiled and exercised in CI.

Required:

```text
examples compile with stable toolchain
examples run against memory backend
SQLite-compatible examples run against SQLite backend
examples use the same public APIs documented in the spec
example assertions verify the behavior being demonstrated
example comments stay focused on durable semantics
```

Examples should avoid hidden test-only helpers except for fixture setup. The goal is for users and implementation agents to copy the example shape directly.

## 22.8 Provider conformance and benchmarks

Every `DurableBackend` implementation must pass the same conformance suite. Provider-specific tests may exist, but they cannot replace the shared suite.

Shape:

```rust
durable_test::provider_conformance()
    .provider("memory", memory_provider_factory)
    .provider("sqlite", sqlite_provider_factory)
    .run_all()
    .await?;
```

Required:

```text
memory backend passes provider conformance
SQLite backend passes the same conformance suite as memory backend
new provider implementations are not accepted without conformance registration
append-journal backend recovers from append history alone
backend trait tests do not depend on SQLite-specific behavior
local activity registration is preferred over remote dispatch
remote activity worker still works when no local registration exists
```

Conformance cases:

```text
start workflow idempotency and conflict policy
worker registry rejects duplicate workflow identity
worker registry rejects duplicate activity identity
default durable names include package and module path
claim workflow task lease fencing
claim workflow task filters by task_queue and workflow_type
workflow task commit is shard-local for one run
stale shard lease owner cannot commit
stream_history honors up_to_event_id, max_events, and max_bytes
stream_history never returns uncommitted or future events
commit_workflow_task detects stale expected_tail_event_id
append event ids are strictly ordered per run
claim activity task filters by task_queue and activity_name
activity map scheduling creates a compact map descriptor
activity map materializes deterministic item tasks from manifest pages
activity map item completions are idempotent by map_command_id and item_ordinal
activity map replay reconstructs the same map handle without per-item history
activity map respects max_in_flight across retries and worker restarts
external activity completion is idempotent
stale activity lease completion is rejected
signal send idempotency key returns first signal record
read_signal_inbox does not consume the signal
SignalConsumed and signal consumption commit atomically
timer due index wakes the workflow without history scanning
query projection reads latest committed projection only
child outbox dispatch is idempotent
cross-shard outbox delivery is idempotent
cross-shard inbox apply is idempotent
cross-shard child start and completion survive dispatcher crash
parent close policy is persisted and enforced
inline and blob-backed payloads behave identically through public APIs
SQLite provider offloads payloads above configured threshold
SQLite provider passes blob payload conformance against local Garage
derived indexes can be rebuilt from append history
provider restart loses no committed facts
terminal workflow rejects new workflow-visible commands
```

The suite should include crash/restart variants for each provider that can persist across process boundaries. For SQLite, tests should close and reopen the provider and verify recovery from the append journal, not from in-memory state.

Throughput targets should be equal to or higher than comparable `../durable-phases` benchmark modes. Use the `durable-phases` Rust benchmark dimensions as the baseline vocabulary: workflows per second, activations per second, mixed actions per second, worker count, shard count, activation concurrency, prefetch limit, and commit batch size.

Benchmark profiles:

```text
warm cached workflow happy path
worker crash plus streaming replay
activity local-preferred
activity remote-only
signal-heavy
timer-heavy
child-workflow fanout
manifest-backed activity map fanout
large payload refs without DB row inflation
SQLite test backend
production-oriented append backend
```

Report warm-cache throughput separately from recovery throughput. The main performance target is the steady-state cached path, where workflows stay resident until terminal state and persistence is mostly append-only.

---

# 23. Final Shape

The durable runtime should be:

```text
Replay-first
Streaming-history
Append-journal optimized
Workflow-cache optimized
Patch-marker versioned
Backend-agnostic
SQLite-testable without being SQLite-shaped
Payload-ref based
Deterministically tested
```

The most important workflow invariant:

```text
Workflow start input plus streamed replay history is sufficient to reconstruct local state and continue execution.
```

The most important backend invariant:

```text
Every workflow-visible fact is appended exactly once to the workflow’s ordered replay history, and every replay emits the same durable command sequence or fails with nondeterminism.
```

This gives you the Temporal-like authoring model, avoids fragile Rust syntax lowering, removes DB-row and memory pressure from long histories, and leaves recovery latency as the explicit, accepted cost.

[1]: https://pkg.go.dev/go.temporal.io/sdk/workflow "workflow package - go.temporal.io/sdk/workflow - Go Packages"
[2]: https://docs.temporal.io/develop/typescript/workflows/versioning "Versioning - TypeScript SDK | Temporal Platform Documentation"
[3]: https://docs.temporal.io/develop/go/workflows/versioning "Versioning - Go SDK | Temporal Platform Documentation"
