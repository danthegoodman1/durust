#!/usr/bin/env node
import { spawnSync } from "node:child_process";

const requiredEnv = "DURUST_POSTGRES_URL";
const postgresUrl = process.env[requiredEnv];

if (typeof postgresUrl !== "string" || postgresUrl.trim().length === 0) {
  console.error(`npm run check:postgres requires ${requiredEnv} to point at a test database`);
  process.exit(1);
}

const steps = [
  {
    name: "Postgres provider conformance",
    args: ["run", "test", "--", "packages/postgres/test/postgres-conformance.test.ts"]
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
