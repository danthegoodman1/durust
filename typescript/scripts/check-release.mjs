#!/usr/bin/env node
import { spawnSync } from "node:child_process";

const dryRun = process.argv.includes("--dry-run");
const requiredEnv = "DURUST_POSTGRES_URL";

const steps = [
  {
    name: "Fast workspace gate",
    args: ["run", "check"]
  },
  {
    name: "Cross-runtime contract fixtures",
    args: ["run", "check:fixtures"]
  },
  {
    name: "Hot execution cache soak",
    args: ["run", "test:soak"]
  },
  {
    name: "Postgres release gate",
    args: ["run", "check:postgres"],
    requiredEnv
  }
];

if (!dryRun) {
  for (const step of steps) {
    if (
      step.requiredEnv !== undefined &&
      (typeof process.env[step.requiredEnv] !== "string" ||
        process.env[step.requiredEnv].trim().length === 0)
    ) {
      console.error(
        `npm run check:release requires ${step.requiredEnv} because it runs npm ${formatCommand(step.args)}`
      );
      process.exit(1);
    }
  }
}

for (const step of steps) {
  console.log(`\n==> ${step.name}`);
  console.log(`npm ${formatCommand(step.args)}`);
  if (dryRun) {
    continue;
  }

  const result = spawnSync("npm", step.args, {
    stdio: "inherit",
    env: process.env
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function formatCommand(args) {
  return args.join(" ");
}
