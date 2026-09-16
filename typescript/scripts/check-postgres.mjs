#!/usr/bin/env node
import { spawnSync } from "node:child_process";

const requiredEnv = "DURUST_POSTGRES_URL";
const postgresUrl = process.env[requiredEnv];

if (typeof postgresUrl !== "string" || postgresUrl.trim().length === 0) {
  console.error(`npm run check:postgres requires ${requiredEnv} to point at a test database`);
  process.exit(1);
}

// This script is where the benchmark thresholds are the performance gate, so
// the variable that lifts the speed comparisons for a shared runner would make
// it pass while measuring nothing. It fails here rather than reporting a green
// run over correctness gates alone.
const skipSpeedEnv = "DURUST_BENCHMARK_SKIP_SPEED_THRESHOLDS";
if (process.env[skipSpeedEnv] !== undefined) {
  console.error(
    `npm run check:postgres compares throughput and latency against the baselines, so ${skipSpeedEnv} ` +
      "must be unset; run it on the controlled machine that recorded them"
  );
  process.exit(1);
}

const steps = [
  {
    name: "Postgres provider conformance",
    args: ["run", "test", "--", "packages/native/test/native-conformance.test.ts"]
  },
  {
    name: "Benchmark thresholds including Postgres smoke",
    args: ["run", "test:benchmark-thresholds"]
  }
];

for (const step of steps) {
  console.log(`\n==> ${step.name}`);
  const result = spawnSync("npm", step.args, {
    stdio: "inherit",
    env: process.env
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
