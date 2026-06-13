---
id: 0004
title: Query projections
status: not_started
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

## Performance Gate

- Criterion benchmark for projection update and projection read.
