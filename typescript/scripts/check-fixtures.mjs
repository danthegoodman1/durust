#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const workspaceRoot = fileURLToPath(new URL("..", import.meta.url));
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));

const steps = [
  {
    name: "TypeScript neutral contract fixture tests",
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
    command: "cargo",
    args: ["test", "--test", "contract_fixtures"],
    cwd: repoRoot
  }
];

for (const step of steps) {
  console.log(`\n==> ${step.name}`);
  const result = spawnSync(step.command, step.args, {
    cwd: step.cwd,
    stdio: "inherit",
    env: process.env
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
