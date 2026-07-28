#!/usr/bin/env node

/**
 * Type-checks the test suite, and proves it actually checked it.
 *
 * The second half is the point. `vitest.config.ts` previously carried
 * `typecheck: { enabled: true, tsconfig: "tsconfig.json" }`, and that tsconfig
 * resolved **zero** test files — so every `expectTypeOf` in the suite was an
 * unconditional pass while the run cheerfully printed `Type Errors  no errors`.
 * Measured at the time: a deliberately false `expectTypeOf` reported `13 passed`.
 *
 * A bare `tsc -p tsconfig.tests.json --noEmit` wired into `npm run check` would
 * reproduce that defect exactly. `tsc` exits 0 when it has nothing to check, so
 * one bad `include` glob — a directory rename, a moved package — turns this gate
 * green and silent in the same edit.
 *
 * So this does not assert a count, and deliberately not a floor either: a floor
 * derived from the file list cannot fail on the list it is derived from, which is
 * the shape of several bugs already found in this repository. It asserts a
 * *correspondence* — every test file on disk must appear in the set `tsc` says it
 * read — plus the one clause that keeps the correspondence from being vacuously
 * true when the disk glob itself matches nothing.
 */

import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { glob } from "tinyglobby";

const workspaceRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const project = "tsconfig.tests.json";
const testGlob = "packages/*/test/**/*.ts";

const onDisk = (
  await glob(testGlob, {
    absolute: true,
    cwd: workspaceRoot,
    onlyFiles: true,
    ignore: ["**/node_modules/**", "**/dist/**"]
  })
).map((path) => resolve(path));

if (onDisk.length === 0) {
  fail(
    `${testGlob} matched no files under ${workspaceRoot}.\n` +
      `Without this clause the coverage check below would pass trivially — "every file on disk was ` +
      `checked" is true when there are no files — and this script would report success having type-checked ` +
      `nothing, which is the exact defect it exists to prevent.`
  );
}

const result = spawnSync(
  "npx",
  ["tsc", "-p", project, "--noEmit", "--listFiles", "--pretty", "false"],
  { cwd: workspaceRoot, encoding: "utf8", env: process.env }
);

if (result.error !== undefined) {
  fail(`could not run tsc: ${result.error.message}`);
}

const stdout = result.stdout ?? "";
const diagnostics = stdout
  .split("\n")
  .filter((line) => /error TS\d+:/.test(line))
  .join("\n");

const checked = new Set(
  stdout
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.endsWith(".ts") || line.endsWith(".tsx"))
    .map((line) => resolve(workspaceRoot, line))
);

const unchecked = onDisk.filter((path) => !checked.has(path)).sort();
if (unchecked.length > 0) {
  fail(
    `${unchecked.length} of ${onDisk.length} test files are not covered by ${project}, so they are ` +
      `not type-checked by anything:\n` +
      unchecked.map((path) => `  ${path}`).join("\n") +
      `\nFix the "include" globs in ${project}. A test file that no tsconfig resolves is checked by ` +
      `nothing at all, and nothing else in this repository will notice.`
  );
}

if (diagnostics.length > 0) {
  console.error(diagnostics);
  fail(
    `${project} reports type errors in the test suite (${onDisk.length} test files checked).\n` +
      `These are errors in tests, so they do not break the published build — which is precisely why they ` +
      `went unnoticed until this project existed. Fix the test, or the fixture that drifted from the type ` +
      `it stands in for; do not silence it with a cast, which erases the finding rather than the defect.`
  );
}

if (result.status !== 0) {
  fail(`tsc exited ${result.status} without emitting a recognisable diagnostic:\n${stdout}`);
}

console.log(`${project}: ${onDisk.length} test files checked, no type errors`);

function fail(message) {
  console.error(`check:test-types failed.\n${message}`);
  process.exit(1);
}
