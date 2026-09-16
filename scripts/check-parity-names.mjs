#!/usr/bin/env node
/**
 * Verifies that every test `PARITY.md` cites still exists, on both sides.
 *
 * Rust citations look like `tests/replay_core.rs::name` or
 * `src/runtime.rs::runtime::tests::name`: the file must exist and declare
 * `fn <last segment>`. TypeScript citations look like `runtime.test.ts` — `a
 * test title` (several titles may follow one file, separated by `;`), and
 * shared provider cases look like shared case `a title`: the title must
 * appear verbatim in the cited file or, because the conformance files run
 * the shared cases by name, in `packages/testing/src/index.ts`. The "How to
 * read this" section holds placeholder citations and is skipped. A citation
 * that names nothing fails the script, so a renamed test cannot leave the
 * ledger pointing at air.
 */
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const whole = readFileSync(join(repoRoot, "PARITY.md"), "utf8");
const ledgerStart = whole.indexOf("\n## 1.");
if (ledgerStart < 0) {
  console.error("PARITY.md: section `## 1.` not found");
  process.exit(1);
}
// Titles wrap across lines in prose; a citation never contains a blank line.
const parity = whole.slice(ledgerStart).replace(/\n(?!\n)/g, " ");
const failures = [];
let checked = 0;

for (const raw of new Set(parity.match(/`[A-Za-z0-9_/.-]+\.rs::[A-Za-z0-9_:]+`/g) ?? [])) {
  const citation = raw.slice(1, -1);
  const [file, ...segments] = citation.split("::");
  const name = segments.at(-1);
  const path = join(repoRoot, file);
  checked += 1;
  if (!existsSync(path)) {
    failures.push(`${citation}: ${file} does not exist`);
    continue;
  }
  if (!new RegExp(`\\bfn ${name}\\b`).test(readFileSync(path, "utf8"))) {
    failures.push(`${citation}: no \`fn ${name}\` in ${file}`);
  }
}

function testFiles(dir, out = []) {
  for (const entry of readdirSync(dir)) {
    if (entry === "node_modules" || entry === "dist") continue;
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) testFiles(path, out);
    else if (entry.endsWith(".test.ts")) out.push(path);
  }
  return out;
}
const tsFiles = testFiles(join(repoRoot, "typescript", "packages"));
const shared = readFileSync(join(repoRoot, "typescript/packages/testing/src/index.ts"), "utf8");
const titleIn = (source, title) =>
  source.includes(`"${title}"`) || source.includes(`'${title}'`) || source.includes(`\`${title}\``);

for (const match of parity.matchAll(/`([A-Za-z0-9_/.-]+\.test\.ts)` — ((?:`[^`]+`(?:;\s*|,\s*| and )?)+)/g)) {
  const cited = match[1];
  const candidates = tsFiles.filter((path) => path.endsWith(cited.includes("/") ? cited : `/${cited}`));
  if (candidates.length !== 1) {
    failures.push(`${cited}: ${candidates.length} test files match under typescript/packages`);
    continue;
  }
  const source = readFileSync(candidates[0], "utf8");
  for (const [, title] of match[2].matchAll(/`([^`]+)`/g)) {
    // The list ends where the next file citation begins; a `<case>` is a template.
    if (title.endsWith(".test.ts")) break;
    if (title.includes("<")) continue;
    checked += 1;
    if (!titleIn(source, title) && !titleIn(shared, title)) {
      failures.push(`${cited} — \`${title}\`: not in ${relative(repoRoot, candidates[0])} or the shared cases`);
    }
  }
}

for (const match of parity.matchAll(/shared cases? ((?:`[^`]+`(?:,\s*| and |, and )?)+)/g)) {
  for (const [, title] of match[1].matchAll(/`([^`]+)`/g)) {
    checked += 1;
    if (!titleIn(shared, title)) {
      failures.push(`shared case \`${title}\`: not in typescript/packages/testing/src/index.ts`);
    }
  }
}

if (failures.length > 0) {
  console.error(`PARITY.md cites ${failures.length} test(s) that do not exist:\n  ${failures.join("\n  ")}`);
  process.exit(1);
}
console.log(`PARITY.md: ${checked} test citations resolve`);
