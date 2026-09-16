#!/usr/bin/env node
/**
 * Runs the TypeScript half of every shared-fixture runner from one place.
 *
 * The files under `typescript/fixtures/contract/` are the cross-language
 * contract: each one is read by a Rust runner and a TypeScript runner, and a
 * fixture only means anything if BOTH runners execute. Every TypeScript
 * runner drives the Rust providers through `@durust/native`, so the addon
 * has to be built first (`npm run build:native --workspace @durust/native`).
 * The `transitions` half of `map-transitions.json` has a Rust runner only,
 * because the map engine lives in Rust. The Rust halves are
 * ordinary Rust tests (`tests/contract_fixtures.rs`, `tests/map_transitions.rs`,
 * `tests/behavioral_corpus.rs`, and the worker's start-jitter unit test), so
 * `cargo test --locked --workspace --all-features` runs them; this script is
 * wired into `npm run check` so the TypeScript halves are gated the same way.
 * CI runs both commands.
 *
 * Adding a fixture means adding its TypeScript runner here and its Rust
 * runner under `tests/`. A runner that exists but is not listed is a fixture
 * nobody gates on, which is exactly how `map-transitions.json`'s two runners
 * went ungated after Phase 6.
 */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const workspaceRoot = fileURLToPath(new URL("..", import.meta.url));

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
    name: "TypeScript shared map transition table fanouts",
    fixtures: ["map-transitions.json"],
    command: "npm",
    args: ["run", "test", "--", "packages/core/test/map-fanouts.test.ts"],
    cwd: workspaceRoot
  },
  {
    name: "TypeScript behavioural corpus, TypeScript worker over the Rust memory provider",
    fixtures: ["behavioral-corpus.json"],
    command: "npm",
    args: ["run", "test", "--", "packages/core/test/behavioral-corpus.test.ts"],
    cwd: workspaceRoot
  }
];

for (const step of steps) {
  console.log(`\n==> ${step.name} [${step.fixtures.join(", ")}]`);
  const result = spawnSync(step.command, step.args, {
    cwd: step.cwd,
    stdio: "inherit",
    env: { ...process.env, ...(step.env ?? {}) }
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
