---
id: 0002
title: Durable activity, timer, signal, and activity map
status: complete
depends_on: [0001]
labels: [activities, timers, signals, activity-map, task-queues, examples]
---

# Durable Activity, Timer, Signal, And Activity Map

Add the first durable wait families and the scalable manifest-backed fanout
primitive.

## Scope

- `call_activity!(...)`.
- `activity_map(...)`.
- `sleep`.
- `sleep_until`.
- `signal`.
- Active wait indexes.
- Activity task queue routing.
- Activity worker.
- Timer worker.
- Signal inbox.
- Local activity preference.
- Signal, timer, local/remote activity, activity map, and map-reduce examples.

## Acceptance

- Signal before wait buffers.
- Signal after wait wakes.
- Timer fires after virtual time.
- Activity completion wakes workflow.
- Locally registered activity runs locally before remote dispatch.
- Activity-only worker on another process can claim registered activity tasks.
- Activity map schedules manifest-backed fanout with one workflow command.
- Activity map replay does not duplicate item task creation.
- Activity map enforces `max_in_flight` without loading all inputs.
- Activity map example compiles and runs.
- Map-reduce example compiles and runs.

## Current State

Implemented and covered:

- Activity scheduling/completion/failure/timeout with durable retry attempts,
  serializable failure envelopes, and non-retryable activity failures.
- Workflow-local default activity options for task queue and retry policy.
- Durable timers, signal inbox reads/consumption, and nondeterminism retry backoff.
- Explicit local activity preference for activities and activity-map items with
  remote fallback when local capacity is zero.
- Generic delayed workflow-task release visibility in memory and SQLite providers.
- Workflow cancellation that records a terminal history fact and clears
  provider-owned timer waits, activity tasks, and activity-map item state.
- Manifest-backed activity map scheduling, bounded item materialization, result
  manifest writes, terminal failure, retry attempts, root-plus-page manifests,
  and SQLite restart recovery.
- Runnable signal wait, timer wait, local/remote activity, activity-map, and
  map-reduce examples with assertions.
- Criterion coverage for workflow scheduling/claim/commit/replay, activity
  claim/complete, timer wakeup, signal send/consume, and activity-map
  materialization/completion hot paths.

Remaining before this phase is done:

- None.

## Required Tests

- Activity success, failure, retry, timeout, stale lease, and duplicate completion.
- Timer scheduling, firing, replay, cancellation, and timer firing during recovery.
- Signal idempotency, signal-before-wait, signal-after-wait, atomic consume, and replay.
- Local activity preference with remote fallback.
- Activity map manifest paging, item lease fencing, retry, cancellation, restart recovery, and result manifest writes.
- Provider conformance for activity, timer, signal, and activity map cases.

## Simulation Profiles

- Activity duplicate completion.
- Timer duplicate fire.
- Signal storm.
- Worker crash between schedule and commit.
- Activity map worker crash with in-flight items.
- Manifest page delays and blob store latency.

## Performance Gate

- Criterion benchmarks for activity claim/complete, timer due scan/wakeup, signal send/consume, and activity map item materialization/completion throughput.
