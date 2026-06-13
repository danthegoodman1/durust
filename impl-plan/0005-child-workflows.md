---
id: 0005
title: Child workflows
status: complete
depends_on: [0002, 0003]
labels: [child-workflows, outbox, cross-shard, examples]
---

# Child Workflows

Add durable child workflow start, result waiting, parent close policy, and
outbox/inbox handoff.

## Scope

- `child!(...)`.
- `spawn().await`.
- `result().await`.
- Child outbox.
- Parent wakeup.
- Parent close policy.
- Cross-shard outbox/inbox handoff for child start, completion, and cancellation.
- Child wait, child abandon, and parent close examples.

## Acceptance

- Duplicate outbox dispatch creates one child.
- Child completion wakes parent.
- Parent cancellation propagates by policy.
- Cross-shard child start and completion survive dispatcher crash.
- Child workflow examples compile and run.

## Current State

Implemented and covered:

- `durust::child!(workflow(input))` with `workflow_id`, optional task queue,
  `ParentClosePolicy`, `spawn().await`, typed `result().await`, and deterministic
  replay fingerprints.
- Provider-owned child-start outbox in memory and SQLite, with idempotent
  dispatch and child workflow id conflict handling.
- Child run parent links and parent wakeup events for child start, completion,
  failure, and cancellation.
- Parent close policy enforcement for `Cancel` and `Abandon`, including external
  parent cancellation and parent workflow terminal commits.
- SQLite restart recovery after a committed child-start outbox row and before
  dispatch.
- Public child workflow examples for spawn-and-wait and spawn-and-abandon.
- Criterion coverage for memory child-start dispatch.

Deferred to later scale-out hardening:

- A physically partitioned provider implementation with explicit cross-shard
  inbox rows and crash injection at every source/target ack step. The current
  backend contract keeps this provider-owned and proves the generic idempotent
  outbox semantics in shared conformance.

## Required Tests

- Spawn and wait.
- Spawn and abandon.
- Parent terminal state with `Cancel`.
- Parent terminal state with `Abandon`.
- Duplicate child-start outbox dispatch.
- Child start conflict.
- Child completion routed to parent.
- Cross-shard source outbox commit before dispatch crash.
- Target inbox write before apply crash.
- Target apply before source ack crash.

## Simulation Profiles

- Child fanout.
- Cross-shard outbox duplicate delivery.
- Cross-shard outbox delayed delivery.
- Dispatcher crash at each handoff step.

## Performance Gate

- Criterion benchmark for child start outbox commit, dispatch, and parent wakeup.
