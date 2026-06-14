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
- Provider-agnostic S3-compatible blob offload through a durability-provider
  wrapper.
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
- Provider-configured codec selection for typed workflow/activity APIs:
  `Client`, `Worker`, workflow-side durable APIs, activity outputs, child
  starts, query projections, and activity-map manifests encode new payloads with
  the backend's configured codec.
- Codec-aware generic payload decoding that dispatches from the recorded
  `PayloadRef` codec, allowing replay to read histories containing MessagePack
  and JSON payloads.
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
- Criterion benchmark coverage for cold workflow replay over a 64 KiB workflow
  input with inline and provider-blob-backed payload storage. Local memory
  provider baselines were about 32 us for inline replay and 87 us for blob
  replay.
- Criterion single-file SQLite baseline coverage for workflow task claim,
  workflow task append/commit, activity claim/complete, one-activity workflow
  execution, and a 1k mixed-workflow drain with four workers.
- Generic dry-run-capable provider payload GC, with memory and SQLite
  implementations that retain blobs reachable from history, activities, maps,
  child outbox, signals, and query projections.
- `BlobStoreConfig::LocalDirectory` and SQLite external object payload storage,
  with content-addressed local object writes, digest/size validation on hydrate,
  close/reopen coverage, object GC coverage, and upload-failure coverage proving
  a missing payload ref is not committed.
- Provider conformance coverage proving overwritten query-projection blobs are
  collected while the retained projection remains readable after SQLite
  close/reopen.
- Replay-core regression coverage proving JSON-configured typed client,
  workflow, activity, signal, and query APIs round-trip through normal worker
  execution.
- Provider conformance coverage proving JSON-configured nested activity-map
  manifests and result manifests hydrate after provider offload, including
  SQLite close/reopen.
- `PayloadBackend<B, S>` provider wrapper that offloads external payload blobs
  before delegating durable writes to an inner provider and hydrates them after
  reads, keeping object-store behavior provider-agnostic.
- `PayloadBlobStore`, `MemoryBlobStore`, and async `S3BlobStore` types. The S3
  implementation uses the `rust-s3` package with the Tokio/Rustls client rather
  than hand-rolled signing or blocking S3 I/O.
- Provider conformance coverage for `PayloadBackend` over memory and SQLite,
  including SQLite close/reopen with an external blob store and upload-failure
  coverage proving a missing external payload ref is not committed.
- Generic provider payload-root scanning plus `PayloadBackend` external object
  GC. Concrete providers expose roots without S3/Garage policy, and wrapper GC
  validates recursively reachable activity-map manifests before deleting
  wrapper-owned unreachable blobs.
- Provider conformance coverage proving `PayloadBackend` hydrates activity-map
  payloads over memory and SQLite, survives SQLite close/reopen, and collects
  overwritten query-projection blobs from the wrapper object store.
- Local Garage fixture and CI/local conformance path for
  `PayloadBackend<SqliteBackend, S3BlobStore>`, using Garage's single-node
  default-bucket mode rather than MinIO.
- S3 upload-failure coverage proving an unavailable object store does not
  commit a missing external payload ref.
- Criterion benchmark coverage for Zstd compression/decompression over 64 KiB
  MessagePack payloads and env-gated Garage object-store put/get over 64 KiB
  payload blobs. Local Garage fixture measurements were about 1.18 ms for a
  64 KiB get and 3.48 ms for a unique 64 KiB put; Zstd level 3 was about
  5.47 us compress / 13.17 us decompress for repetitive payloads and
  55.33 us compress / 58.64 us decompress for mixed payloads.

Remaining before this phase is done:

- Compression policy for object-store payloads. Compression remains
  unimplemented in runtime code until payload corpus, network, storage, and CPU
  budget data justify a specific default or explicit provider option.
- Lazy nested payload hydration during replay. Current public reads hydrate
  returned payload refs before handing them to runtime code.
- Blob-path coverage for side effects once side effects are implemented.
- Broader production payload-offload documentation after compression and lazy
  hydration policy settle.
- Checked-in payload codec/offload benchmark regression thresholds.
- Partitioned SQLite shard-file throughput baselines remain part of the
  dedicated performance-hardening phase; the current SQLite numbers are
  single-database-file baselines.

## Required Tests

- Inline and blob-backed payloads behave identically through public APIs.
- MessagePack round-trips all public payload families.
- JSON round-trips all public payload families when configured.
- Codec mismatch or schema fingerprint mismatch fails clearly.
- SQLite provider threshold forces inline payload below threshold and blob ref above threshold.
- SQLite local-directory blob store round-trips public payload refs across close/reopen.
- SQLite local-directory blob GC deletes unreachable object-store blobs.
- PayloadBackend over memory and SQLite recursively hydrates activity-map inputs/results and deletes unreachable wrapper-owned blobs.
- PayloadBackend plus Garage round-trips activity, signal, query, child, map, and workflow payload refs.
- Blob upload before history commit crash.
- History commit before blob GC crash.
- Garage unavailable during upload returns a provider error without committing a missing payload ref.
- Missing blob detection.
- Payload digest validation.
- Continue-as-new starts a new run with the same workflow id.
- Continued run receives compacted input.
- Parent/child and query behavior remains correct across continue-as-new.

## Performance Gate

- Criterion benchmark for MessagePack encode/decode, JSON encode/decode, Zstd
  compression/decompression, inline payload refs, blob payload refs, replay over
  large blob refs, and env-gated Garage put/get over 64 KiB object-store blobs.

## Public API Budget

- `PayloadBackend<B, S>` is a first-class provider wrapper because production
  object stores must work across SQLite, partitioned SQLite, Postgres, and future
  providers without duplicating S3/Garage behavior in each concrete provider.
  It also keeps network I/O outside provider transactions: the wrapper uploads
  bytes before delegating compact refs to the inner provider and hydrates refs
  after reads.
- `PayloadBlobStore` is the minimal composable primitive the wrapper needs:
  put, get, list, delete, and URI ownership checks by digest/URI. It does not
  expose workflow concepts or provider-specific state.
- `S3BlobStore` is async and package-backed (`rust-s3` with Tokio/Rustls). We do
  not maintain custom S3 signing, request construction, or blocking S3 calls.
