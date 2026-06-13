---
id: 0007
title: Payload offloading and continue-as-new
status: not_started
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
