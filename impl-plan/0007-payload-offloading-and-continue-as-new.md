---
id: 0007
title: Payload offloading and continue-as-new
status: in_progress
depends_on: [0001, 0002]
labels: [payloads, blob-storage, continue-as-new, examples]
---

# Payload Offloading And Continue-As-New

Add provider-owned payload offloading and the operational tool for capping replay
distance.

## Scope

- `PayloadRef`.
- MessagePack default payload codec via `rmp-serde`.
- JSON payload codec for debug/export and explicit provider config.
- Codec, schema fingerprint, compression, encryption, digest, and size metadata.
- Inline/blob threshold.
- Provider config knob for inline/offload threshold.
- SQLite provider integration with S3-compatible blob offload.
- Local Garage-backed S3 test fixture.
- Compression.
- Blob GC.
- Large activity, signal, child, query, side-effect, and workflow payloads.
- Payload offload example.
- `continue_as_new`.
- Continue-as-new example.

## Acceptance

- Large payload not stored inline.
- MessagePack is the default durable payload codec.
- JSON codec is available through provider config.
- Payload refs record codec and schema fingerprint metadata.
- SQLite provider offloads payloads above configured threshold.
- SQLite provider can use local Garage as its S3-compatible blob store in tests.
- Streaming replay does not hydrate large payload until observed.
- Orphan blob GC works.
- Payload offload example compiles and runs.
- Continue-as-new example compiles and runs.

## Current State

Implemented and covered:

- `durust::continue_as_new(input)` workflow API.
- `WorkflowContinuedAsNew` history event.
- Worker handling that records continue-as-new without appending
  `WorkflowFailed`.
- Memory and SQLite provider support for closing the current run and making a
  fresh run with the same workflow id claimable.
- SQLite close/reopen recovery for a continued run.
- Query projection behavior across continue-as-new: the previous committed
  projection remains visible until the new run publishes a replacement.
- Parent/child behavior for a child that continues as new before completing:
  the parent is woken by the final continued child completion, not the
  intermediate continuation.
- Runnable `continue_as_new` example with assertions.
- `PayloadStorageConfig` with a default MessagePack codec and 8 KiB inline
  threshold.
- Explicit JSON payload encode/decode helpers for debug/export paths.
- Provider-owned inline/blob threshold enforcement for `MemoryBackend` and
  `SqliteBackend`.
- SQLite `payload_blobs` storage that persists compact `PayloadRef::Blob`
  references across close/reopen.
- Digest and size validation for provider-owned blob refs, plus missing blob
  rejection.
- Public API hydration coverage for large workflow input, activity input,
  activity result, signal, and query projection payloads through memory and
  SQLite providers.
- Blob-path public API coverage for child workflow start input and child
  completion result payloads.
- Recursive activity map manifest handling for root manifests, page refs, and
  item/result refs, with forced-offload conformance for memory and SQLite.
- Runnable `payload_offload` example that forces provider-owned blob storage.
- Criterion benchmark coverage for MessagePack/JSON encode/decode and
  inline-vs-blob history streaming over 64 KiB payloads.

Remaining before this phase is done:

- Provider-configured codec selection for typed workflow/activity APIs. The
  current runtime still encodes typed values with MessagePack before providers
  see them.
- SQLite S3-compatible/Garage blob-store integration, transient upload failure
  behavior, and orphan blob GC.
- Lazy nested payload hydration during replay. Current public reads hydrate
  returned payload refs before handing them to runtime code.
- Blob-path coverage for side effects once side effects are implemented.
- Public payload-offload documentation for production object stores once S3
  compatible storage lands.
- Payload codec/offload benchmark baselines and regression thresholds.

## Required Tests

- Inline and blob-backed payloads behave identically through public APIs.
- MessagePack round-trips all public payload families.
- JSON round-trips all public payload families when configured.
- Codec mismatch or schema fingerprint mismatch fails clearly.
- SQLite provider threshold forces inline payload below threshold and blob ref above threshold.
- SQLite plus Garage round-trips activity, signal, query, child, side-effect, and workflow payload refs.
- Blob upload before history commit crash.
- History commit before blob GC crash.
- Garage unavailable during upload returns a retryable provider error without committing a missing payload ref.
- Missing blob detection.
- Payload digest validation.
- Continue-as-new starts a new run with the same workflow id.
- Continued run receives compacted input.
- Parent/child and query behavior remains correct across continue-as-new.

## Performance Gate

- Criterion benchmark for MessagePack encode/decode, JSON encode/decode, inline payload refs, blob payload refs, and replay over large blob refs.
