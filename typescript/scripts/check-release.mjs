#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const dryRun = process.argv.includes("--dry-run");
const requiredEnv = "DURUST_POSTGRES_URL";

// `check:fixtures` used to run here as its own step, immediately after
// `npm run check` — which already runs it. That is several minutes of duplicated
// work, because `check-fixtures.mjs` shells out to `cargo` four times.
//
// It is not merely deleted, because the duplicate was load-bearing in one narrow
// way: if `check:fixtures` ever fell out of `npm run check`, the explicit step
// here would still have run it. That is not hypothetical — `check:fixtures`
// being invoked by nothing at all is the first catalogued instance of this
// repository's recurring defect, and the reason it is wired into `check` today.
//
// So the re-run is replaced by an assertion, which is strictly stronger: the
// re-run silently *compensated* for the script falling out of `check`, leaving
// the hole open for CI, which invokes `check` and not this file. The assertion
// fails loudly on the same edit and points at the real problem.
assertCheckStillRunsFixtures();

const steps = [
  {
    name: "Fast workspace gate (includes check:fixtures)",
    args: ["run", "check"]
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

function assertCheckStillRunsFixtures() {
  const packageJsonPath = join(dirname(dirname(fileURLToPath(import.meta.url))), "package.json");
  const scripts = JSON.parse(readFileSync(packageJsonPath, "utf8")).scripts ?? {};
  const check = scripts.check;
  if (typeof check !== "string") {
    console.error(
      `npm run check:release expects a "check" script in ${packageJsonPath}; it is missing, so the release gate no longer runs anything it claims to`
    );
    process.exit(1);
  }
  if (!/\bcheck:fixtures\b/.test(check)) {
    console.error(
      `npm run check:release relies on "npm run check" to run check:fixtures, and it no longer does.\n` +
        `  check = ${check}\n` +
        `The cross-runtime contract fixtures are the only thing that proves the Rust and TypeScript runtimes agree. ` +
        `Restore check:fixtures to the "check" script, or add it back as an explicit step here — but do not leave it unreachable, ` +
        `which is the state it was in before Phase 7 and the reason this assertion exists.`
    );
    process.exit(1);
  }
}

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
