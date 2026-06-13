---
id: 0003
title: Select and join
status: complete
depends_on: [0002]
labels: [select, join, determinism, examples]
---

# Select And Join

Add deterministic coordination over durable futures.

## Scope

- `durust::select!`.
- `durust::join!`.
- `SelectWinner` history event.
- Losing wait cancellation policy.
- Deterministic tie-break.
- Select and join examples.

## Acceptance

- Select replay is deterministic.
- Branch reorder is detected.
- Late losing completion is ignored safely.
- Select approval example compiles and runs.
- Join activities example compiles and runs.

## Required Tests

- Select over signal, timer, and activity.
- Deterministic tie-break by event id, then lexical branch order.
- Replay uses recorded branch ordinal.
- Losing waits are cancelled or ignored according to policy.
- `join!` registers branches in deterministic lexical order before waiting.
- Plain Rust futures are not treated as concurrent durable launch.

Deferred to later phases:

- Select/join over child start and child result belongs to `0005-child-workflows.md`.
- Select/join over deterministic workflow-local spawned futures belongs to the
  deterministic fiber work in `0008-deterministic-simulation-hardening.md`.
- Select over workflow cancellation needs a non-terminal cancellation wait API;
  current external cancellation records a terminal workflow fact and clears
  provider-owned operational state.

## Current State

Implemented and covered:

- Variadic `durust::select!` and `durust::join!` procedural macros.
- `SelectWinner` replay fact with branch digest, winner ordinal, and winning
  event id.
- Deterministic winner selection by ready event id, then lexical branch order.
- Replay validation for branch reorder, changed winner, and changed winning
  event id.
- Losing timer, signal, and activity cancellation/ignore policy, including
  losing completions that race before `SelectWinner` is appended.
- Durable branch trait gating so `join!` rejects plain Rust futures at compile
  time.
- Runnable `select_approval` and `join_activities` examples with assertions.
- Criterion benchmarks for select registration, select replay, and bounded join
  fanout.

Remaining before this phase is done:

- None for the durable future families implemented before child workflows and
  deterministic fibers.

## Simulation Profiles

- Same-tick timer and signal race.
- Activity completion racing cancellation.
- Late losing completion after select winner commit.
- Branch reorder nondeterminism.

## Performance Gate

- Criterion benchmark for select registration, select replay, and bounded join fanout.
