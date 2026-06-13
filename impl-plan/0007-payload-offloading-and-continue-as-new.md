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
- Inline/blob threshold.
- Compression.
- Blob GC.
- Large activity, signal, child, query, side-effect, and workflow payloads.
- Payload offload example.
- `continue_as_new`.
- Continue-as-new example.

## Acceptance

- Large payload not stored inline.
- Streaming replay does not hydrate large payload until observed.
- Orphan blob GC works.
- Payload offload example compiles and runs.
- Continue-as-new example compiles and runs.

## Required Tests

- Inline and blob-backed payloads behave identically through public APIs.
- Blob upload before history commit crash.
- History commit before blob GC crash.
- Missing blob detection.
- Payload digest validation.
- Continue-as-new starts a new run with the same workflow id.
- Continued run receives compacted input.
- Parent/child and query behavior remains correct across continue-as-new.

## Performance Gate

- Criterion benchmark for inline encode/decode, blob-ref encode/decode, and replay over large blob refs.
