---
id: 0002
title: Durable activity, timer, signal, and activity map
status: not_started
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
