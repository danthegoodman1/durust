---
id: 0001
title: Replay core
status: not_started
depends_on: []
labels: [runtime, replay, testing, provider-conformance, examples]
---

# Replay Core

Build the smallest trustworthy runtime loop: typed workflow/activity
registration, append history, streaming replay, deterministic simulation
infrastructure, provider conformance harness, and the first memory/SQLite
providers.

## Scope

- `Workflow` trait.
- `Activity` trait.
- `#[workflow]` registration macro.
- `#[activity]` registration macro.
- Durable manifest generation, check, diff, and accept CLI.
- Task-local workflow context.
- History event model.
- Streaming history cursor.
- Deterministic simulation harness.
- Virtual clock.
- Seeded task scheduler.
- Determinism lint pass in `#[workflow]`.
- Provider conformance harness.
- Worker builder and local registration registry.
- Example harness and CI runner.
- Memory backend.
- SQLite test backend.
- Workflow worker loop.

## Acceptance

- Simple workflow starts, schedules activity, and completes.
- Worker crash causes replay from streamed history.
- Recovery never bulk-loads full history.
- Cached workflow does not replay at each wait boundary.
- Compile-fail tests reject obvious nondeterministic workflow APIs.
- Diagnostics suggest durable replacements.
- Manifest check detects vanished workflow/activity identities.
- Manifest check detects schema changes that need versioning or compatibility review.
- Manifest check exits nonzero for CI conflicts.
- Manifest accept updates the checked-in baseline after intentional review.
- Same scenario is reproducible by seed.
- Virtual clock drives timer tests.
- Memory and SQLite backends pass initial provider conformance.
- Workflow worker claims only registered workflow types from configured queue.
- `hello_activity` and `worker_registration` examples compile and run.

## Required Tests

- Replay simple workflow.
- Replay after worker crash.
- Stream history with chunk size one.
- Determinism lint compile-fail tests for `tokio::time::sleep`, native time, native select, native spawn, randomness, and unknown `.await`.
- Provider conformance smoke suite for memory and SQLite.
- SQLite close/reopen recovery from append history.

## Performance Gate

- Criterion baseline for workflow task claim, replay of a small history, append commit, and cached wake/poll.
