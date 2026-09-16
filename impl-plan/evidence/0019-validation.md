# 0019 Validation Evidence

Baseline: `46091e5`. Measurements and tests run locally on 2026-09-16.

## Initial Complete Validation (`d783aa5`)

- Original F1/F3/F4 failures: `0019-review-probes.rs`, run only against baseline.
- `cargo test --locked --workspace --all-features`, with
  `DURUST_REQUIRE_POSTGRES=1`, `DURUST_REQUIRE_S3=1`, and configured local
  Postgres 17/S3Proxy fixtures: 482 tests pass; one pre-existing ignored test.
- Shared provider suite includes 58 scenarios, including destructive-GC refusal
  and a rejected-map commit followed by a successful corrected same-claim retry.
- `npm run check` in `typescript/`, with Postgres configured and required:
  546 tests pass, one skipped; build, negative types, 31 test-file type checks,
  determinism lint, shared fixtures/corpus, and four package dry runs pass.
- `cargo test --locked --lib`: 93 pass, one ignored, with default features.
- `cargo clippy --locked --workspace --all-features --all-targets -- -D warnings
  -A clippy::result_large_err` and `cargo fmt --all --check` pass.
- `cargo check --locked --no-default-features` with no extra features and with
  `postgres`, `s3`, and `postgres,s3` all pass.
- All compiled Rust examples execute successfully; `cargo bench --locked
  --no-run` passes. `node scripts/check-parity-names.mjs` resolves all 111 cited
  tests. `git diff --check` passes.

## Before/after Criterion

`benches/durability_boundaries.rs` runs unchanged in the isolated baseline and
current checkout. The baseline checkout uses the same Cargo target directory.
Commands use `--sample-size 10 --measurement-time 1 --warm-up-time 1`, and
`--save-baseline review-before` / `--baseline review-before` respectively.

Same-host Criterion central estimates (short local samples, not production SLAs):

| Profile | Baseline | Updated | Interpretation |
| --- | ---: | ---: | --- |
| 100k-event one-event tail read | 422.87 µs | 92.83 ns | Removes repeated prefix scans |
| New nested local 64 KiB blob | 39.36 µs | 43.70 µs | Includes durability barriers; host filesystem measurement |
| Deduplicated local 64 KiB blob | 17.18 µs | 25.74 µs | Includes byte comparison and durability barriers |
| Claim/release, 1 / 1k / 10k / 100k runs | 0.180 / 0.186 / 0.184 / 0.189 µs | 0.252 / 0.345 / 0.375 / 0.397 µs | Validated in-place lease updates |
| Claim/commit, 1 / 1k / 10k / 100k runs | 0.407 / 0.447 / 0.468 / 0.468 µs | 1.352 / 5.588 / 6.334 / 6.997 µs | Transactional projection/wait update |

The `memory_transaction_unrelated_runs` benchmark measures claim/release;
`memory_commit_unrelated_runs` exercises a staged commit while retaining a
constant history and record count. Persistent ordered tables copy touched paths
and records, avoiding a full-state clone. The measured 100k-run commit increases
by about 6.53 µs (roughly 15x); this is explicitly accepted to restore rollback on
all late failures. SQL providers retain their existing native transactions.
Claim/release/heartbeat validate before their first write and stay in place.

Publication overhead is accepted for acknowledged-write safety; no assertion is
made about physical device latency from these local filesystem measurements.
Existing small-memory benchmarks also expose the transaction cost:

| Existing benchmark | Baseline | Updated |
| --- | ---: | ---: |
| `workflow_task_append_commit_memory` | 1.050 µs | 3.102 µs |
| `workflow_cached_wake_poll_memory` | 3.901 µs | 6.338 µs |
| `activity_claim_complete_memory` | 1.111 µs | 3.522 µs |

The memory provider serves development, testing, and deterministic simulation.
These increases are accepted in that role to retain simple, shared atomic
rollback and provider conformance. They do not require a further optimization
pass before merging. The persistent tables avoid a linear database-size copy;
the retained benchmarks guard practical test costs and scaling. Further
optimization should respond to development or simulation needs and preserve
the same conformance and fault guarantees. Persistent providers retain their
production performance gates.

## Existing CI Counter Correction

The unchanged TypeScript runtime from `46091e5`, selected through an isolated
Vitest alias, reproduces the Postgres smoke baseline's counter failure: history
cache misses=12, execution cache hits=20, execution cache misses=12 (old expected
8/24/8). All other exact comparisons pass. Only these three stale expected
values are corrected; speed, statement, error and correctness gates remain.

## Independent Review

The dedicated reviewer found a missing S3-only unit-test guard and a mixed
hot/cold batch scheduling stall. Both were fixed. The new fairness regression
failed before the batch-pipeline fix, then passed for both claim orders and
commit batch sizes 1/2/128. A separate injected commit-RPC failure proves pending
recovery claims and admission slots are released without advancing virtual time.
Default-feature library tests are now an explicit CI gate. The reviewer then
reran the independent mixed-workload probe successfully and reported no
remaining actionable findings.

## Delayed-Release CI Follow-up

CI run `35158712212` failed the Postgres delayed-release test because it assumed
its next claim RPC finished before a 25 ms wall-clock delay expired. Replacing
wall-clock timing with the existing `ProviderClock` also exposed a shared SQL
bug: delayed release ignored the configured clock and used wall time directly.

Before the fix, controlled-clock tests passed on memory but failed on SQLite,
Postgres, and SQLite reopen. After passing `self.clock.now()` into the shared
release-deadline helper, all four pass: hidden at release and at 24 ms, visible
at exactly 25 ms. SQLite drops the client and provider before reopening, proving
the stored deadline survives a real close. A unit table covers immediate release
and saturated deadlines. No new API or clock mechanism was added.

Follow-up validation: 378 unit/provider/replay/simulation tests pass with required
Postgres using `cargo test --locked --workspace --all-features --lib --test
provider_conformance --test sim_worker --test replay_core`; S3 was not configured
for this focused run. Default-feature library tests pass (94, one ignored), as
do clippy, formatting, the four sqlite-free feature checks, and diff whitespace
checks. The dedicated reviewer inspected the red/green evidence and final patch
and found no actionable issues.

## Limits

- Destructive blob collection is offline. The caller must actually stop and
  drain every writer sharing the provider/store; the new flag does not stop them.
- Publication tests inject faults at production operation boundaries and retry
  after every cut. They exercise barrier ordering and fail-closed acknowledgement,
  not power removal from a physical device. Successful syncs must be honored by
  the filesystem/device. No VM/device power-cut experiment was run.
- Replay quanta bound work between cooperative yields, not wall-clock read rate.
  Existing concurrency admission and provider backpressure remain responsible
  for aggregate storage pressure and retry timing.
