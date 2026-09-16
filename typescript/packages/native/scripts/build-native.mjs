#!/usr/bin/env node
/**
 * Builds the shared binding twice: memory/Postgres without SQLite, then the
 * system-linked SQLite companion. Both addons live next to package.json and
 * ship in the same platform package; the adapter loads each only when needed.
 * Pass `--target <triple>` through to cross-build.
 */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const repoRoot = join(packageRoot, "..", "..", "..");
for (const provider of ["postgres", "sqlite"]) {
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
      "--no-default-features",
      "--features",
      provider,
      ...(provider === "sqlite" ? ["--config-path", join(packageRoot, "napi.sqlite.json")] : []),
      "--no-js",
      "--dts",
      "native.d.ts",
      "--no-dts-header",
      ...process.argv.slice(2)
    ],
    { cwd: packageRoot, stdio: "inherit" }
  );
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
