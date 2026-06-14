# Implementation Plan

This directory tracks implementation work as small issue-shaped Markdown files.
`SPEC.md` describes the design. These files describe the build order, acceptance
gates, and test expectations.

Work through the plan in order unless a user explicitly asks for a different
slice. If a task discovers a design gap, update `SPEC.md` first, then update the
affected plan file.

## Plan Items

1. [`0001-replay-core.md`](0001-replay-core.md)
2. [`0002-durable-activity-timer-signal.md`](0002-durable-activity-timer-signal.md)
3. [`0003-select-and-join.md`](0003-select-and-join.md)
4. [`0004-query-projections.md`](0004-query-projections.md)
5. [`0005-child-workflows.md`](0005-child-workflows.md)
6. [`0006-versioning.md`](0006-versioning.md)
7. [`0007-payload-offloading-and-continue-as-new.md`](0007-payload-offloading-and-continue-as-new.md)
8. [`0008-deterministic-simulation-hardening.md`](0008-deterministic-simulation-hardening.md)
9. [`0009-recovery-flow-control.md`](0009-recovery-flow-control.md)
10. [`0011-postgres-provider.md`](0011-postgres-provider.md)
11. [`0012-performance-hardening.md`](0012-performance-hardening.md)
12. [`0013-postgres-shard-native.md`](0013-postgres-shard-native.md)

## Shared Gate

Every item must leave behind:

- Focused unit tests for local invariants.
- Deterministic replay or simulation tests for workflow behavior.
- Provider conformance coverage for backend behavior.
- Example coverage when the item adds public DX.
- Criterion benchmarks for new hot paths.
- Clear docs when public behavior changes.
- A public API budget for every new first-class API, explaining why existing
  primitives are insufficient and what scalability or determinism invariant the
  API protects.
