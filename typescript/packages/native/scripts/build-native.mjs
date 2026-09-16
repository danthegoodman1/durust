#!/usr/bin/env node
/**
 * Builds the `durust-node` crate in release mode through the napi CLI and
 * places the addon next to this package's `package.json`, named for the
 * platform it was built on (`durust-node.linux-x64-gnu.node`, say), which is
 * where `src/index.ts` looks before falling back to the platform package.
 * Pass `--target <triple>` through to cross-build.
 */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const repoRoot = join(packageRoot, "..", "..", "..");
const result = spawnSync(
  "npx",
  [
    "napi",
    "build",
    "--manifest-path",
    join(repoRoot, "durust-node", "Cargo.toml"),
    "--package-json-path",
    join(packageRoot, "package.json"),
    "--output-dir",
    packageRoot,
    "--platform",
    "--release",
    "--no-js",
    "--dts",
    "native.d.ts",
    "--no-dts-header",
    ...process.argv.slice(2)
  ],
  { cwd: packageRoot, stdio: "inherit" }
);
process.exit(result.status ?? 1);
