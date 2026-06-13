---
id: 0003
title: Select and join
status: not_started
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

- Select over signal, timer, activity, child start, child result, and workflow cancellation.
- Deterministic tie-break by event id, then lexical branch order.
- Replay uses recorded branch ordinal.
- Losing waits are cancelled or ignored according to policy.
- `join!` registers branches in deterministic lexical order before waiting.
- Plain Rust futures are not treated as concurrent durable launch.

## Simulation Profiles

- Same-tick timer and signal race.
- Activity completion racing cancellation.
- Late losing completion after select winner commit.
- Branch reorder nondeterminism.

## Performance Gate

- Criterion benchmark for select registration, select replay, and bounded join fanout.
