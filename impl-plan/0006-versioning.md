---
id: 0006
title: Versioning and patching
status: not_started
depends_on: [0001]
labels: [versioning, replay, cli, examples]
---

# Versioning And Patching

Add replay-safe workflow code evolution with recorded version markers and patch
markers.

## Scope

- `get_version`.
- `patched`.
- `deprecate_patch`.
- `VersionMarker` events.
- `workflow_change_versions` index.
- Version CLI.
- Replay safety tests.
- Version branch example.

## Acceptance

- New worker binary can run old and new branches.
- Old worker binary is not required.
- Removing branch too early fails clearly.
- Version branch example compiles and runs.

## Required Tests

- `get_version` returns `DEFAULT_VERSION` for old history.
- `get_version` records max version at tail.
- Recorded version is stable on replay.
- Unsupported min version fails.
- `patched` returns false for pre-patch history.
- `patched` returns true for new history.
- `deprecate_patch` bridges existing patched histories.
- Removing patch too early causes nondeterminism.
- Version marker index updates.
- Safe-to-remove query works.

## Performance Gate

- Criterion benchmark for version marker lookup during replay.
