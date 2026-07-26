#!/usr/bin/env node
/**
 * Runs every shared-fixture runner, in both languages, from one place.
 *
 * The files under `typescript/fixtures/contract/` are the cross-language
 * contract: each one is read by a Rust runner and a TypeScript runner, and a
 * fixture only means anything if BOTH runners actually execute. This script is
 * that guarantee, and it is wired into `npm run check` — and so into CI's
 * "Run TypeScript checks" step — rather than being an opt-in command nobody
 * calls. CI runs the Rust halves a second time through
 * `cargo test --workspace --all-features`; the duplication is deliberate, so
 * neither job can be the only thing standing between a fixture and a
 * regression.
 *
 * Adding a fixture means adding its two runners here. A runner that exists but
 * is not listed is a fixture nobody gates on — which is exactly how
 * `map-transitions.json`'s two runners went ungated after Phase 6.
 */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const workspaceRoot = fileURLToPath(new URL("..", import.meta.url));
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));

/**
 * `fixtures` names the checked-in artefact each runner reads, so the mapping
 * from file to runners is reviewable rather than implied by a test name.
 */
const steps = [
  {
    name: "TypeScript neutral contract fixture tests",
    fixtures: ["core-events.json", "provider-io.json", "benchmark-output.json"],
    command: "npm",
    args: [
      "run",
      "test",
      "--",
      "packages/core/test/fixtures.test.ts",
      "packages/benchmark/test/fixtures.test.ts"
    ],
    cwd: workspaceRoot
  },
  {
    name: "Rust neutral contract fixture tests",
    fixtures: ["core-events.json", "provider-io.json", "benchmark-output.json"],
    command: "cargo",
    args: ["test", "--test", "contract_fixtures"],
    cwd: repoRoot
  },
  {
    name: "TypeScript shared map transition table",
    fixtures: ["map-transitions.json"],
    command: "npm",
    args: ["run", "test", "--", "packages/core/test/map-engine.test.ts"],
    cwd: workspaceRoot
  },
  {
    name: "Rust shared map transition table",
    fixtures: ["map-transitions.json"],
    command: "cargo",
    args: ["test", "--test", "map_transitions"],
    cwd: repoRoot
  },
  {
    name: "TypeScript behavioural corpus",
    fixtures: ["behavioral-corpus.json"],
    command: "npm",
    args: ["run", "test", "--", "packages/core/test/behavioral-corpus.test.ts"],
    cwd: workspaceRoot
  },
  {
    name: "Rust behavioural corpus",
    fixtures: ["behavioral-corpus.json"],
    command: "cargo",
    args: ["test", "--test", "behavioral_corpus"],
    cwd: repoRoot
  },
  {
    // The corpus's `workerStartJitter` table. `MaintenanceJitter` is private to
    // `src/worker.rs`, so the Rust half of this one section lives in that
    // module's unit tests rather than in `tests/`, and a `--test` filter would
    // miss it.
    name: "Rust worker start-jitter corpus table",
    fixtures: ["behavioral-corpus.json"],
    command: "cargo",
    args: ["test", "--lib", "worker::tests::maintenance_jitter_matches_the_shared_corpus_table"],
    cwd: repoRoot
  }
];

for (const step of steps) {
  console.log(`\n==> ${step.name} [${step.fixtures.join(", ")}]`);
  const result = spawnSync(step.command, step.args, {
    cwd: step.cwd,
    stdio: "inherit",
    env: process.env
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
