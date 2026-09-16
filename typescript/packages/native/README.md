# @durust/native

The Rust durability providers behind the TypeScript `DurableBackend`
contract. `NativeBackend.memory()`, `NativeBackend.sqlite(path)`, and
`await NativeBackend.postgres(url)` wrap the `durust-node` napi-rs addon:
every call encodes its request as MessagePack, runs on the addon's tokio
runtime, and decodes the outcome in the shape the contract describes, so the
worker, the client, and the shared conformance suite see an ordinary
provider.

## The addon

The package resolves its addon in this order:

1. `DURUST_NATIVE_LIBRARY_PATH`, when set, for a build placed anywhere.
2. `durust-node.<target>.node` next to this package's `package.json`, which
   is where `npm run build:native --workspace @durust/native` puts a local
   build (`napi build --platform --release` over `../../../durust-node`).
3. `@durust/native-<target>`, the platform package `optionalDependencies`
   installs for the running platform: `linux-x64-gnu`, `linux-arm64-gnu`,
   `darwin-x64`, or `darwin-arm64`. Linux builds link glibc 2.34 or newer.

Inside this repository, build once before running any TypeScript test or
example; `release.yml` builds the four platform packages on their own
runners and publishes them before this package.

## Options

Every constructor takes `nowMs`, the clock the provider follows (`Date.now`,
read at call time, by default): each call moves the Rust provider clock up to
that reading first, so a stubbed clock drives leases, deadlines, retries, due
scans, and `currentTime()`.

`payload` turns on offload through the Rust payload backend: payloads over
`inlineThresholdBytes` go to `blobStore` (`LocalDirectory`, `S3`, or
`Memory`) and come back inline on every read. `gcPayloadBlobs({ minAgeMs,
dryRun })` sweeps blobs no root reaches.

`postgres(url, options)` also takes `schema` (`durust` by default),
`maxPoolSize`, `logicalShards`, `physicalPartitions`, `statementTimeoutMs`,
and `lockTimeoutMs`. `close()` releases the provider; `destroy()` drops the
Postgres schema and then closes.

## Tests

`test/native-conformance.test.ts` runs the shared provider conformance cases
over memory and SQLite, and over Postgres when `DURUST_POSTGRES_URL` is set,
plus the clock, payload offload, garbage collection, and lifecycle checks.
The behavioural corpus and the map fanout table run over this package from
`packages/core/test`.
