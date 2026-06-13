---
id: 0006
title: Versioning and patching
status: complete
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

## Current State

Implemented and covered:

- `durust::get_version`, `durust::patched`, `durust::deprecate_patch`, and
  `durust::DEFAULT_VERSION`.
- `VersionMarker` and `DeprecatedPatchMarker` history events with deterministic
  command sequence validation.
- Worker preload of the generic `workflow_change_versions` index so synchronous
  version APIs remain compatible with bounded streamed replay.
- Workflow-task abort behavior for unsupported workflow versions, without
  appending `WorkflowFailed`.
- Generic `DurableBackend::workflow_change_versions` query with memory and
  SQLite implementations, open/closed status derivation, and safe-to-remove
  helper.
- SQLite marker index schema, restart-safe marker visibility, and
  `cargo durable versions <list|check|safe-to-remove> --sqlite ...`.
- Replay-core coverage for default-version old histories, marker recording,
  streamed replay stability, unsupported version ranges, patch deprecation
  bridge markers, and early bridge removal nondeterminism.
- Provider conformance coverage for marker index updates and safe-to-remove
  status.
- Runnable `version_branch` example with assertions.
- Criterion benchmark `version_marker_lookup_replay_memory`.

Remaining before this phase is done:

- None.

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
