---
id: 0004
title: Query projections
status: complete
depends_on: [0001]
labels: [queries, projections, dx, examples]
---

# Query Projections

Add explicit workflow-published query projections so external reads do not
require replay.

## Scope

- `query_state` workflow attribute.
- `durust::publish`.
- `#[query]`.
- `query_projection` backend reads.
- Query projection example.

## Acceptance

- Query reads latest committed projection without replay.
- Query projection example compiles and runs.

## Required Tests

- Query before first publish.
- Query after publish.
- Query after workflow task conflict.
- Query during a concurrently running workflow task sees a committed projection only.
- Projection update commits atomically with the workflow task.
- Blob-backed projection payload behaves the same as inline payload.

## Current State

Implemented and covered:

- `query_state = Type` workflow macro metadata and manifest schema tracking.
- `durust::publish(&view)` workflow API.
- `#[durust::query(workflow = ...)]` validation that the query view type matches
  the workflow query state.
- `Client::query_projection::<Workflow>(workflow_id)` typed projection reads.
- Generic `DurableBackend::query_projection` raw reads.
- Atomic projection update in workflow-task commit for memory and SQLite.
- Provider conformance for not-found reads, committed raw reads, conflict
  suppression, and blob `PayloadRef` round trip.
- Replay-core coverage for before-publish reads, committed-only visibility while
  a workflow task is claimed, and latest committed projection after workflow
  progress.
- Runnable `query_projection` example with assertions.
- Criterion benchmarks for projection update and projection read.

Remaining before this phase is done:

- None.

## Performance Gate

- Criterion benchmark for projection update and projection read.
