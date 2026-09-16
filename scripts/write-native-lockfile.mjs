#!/usr/bin/env node
/**
 * Keeps the lockfile entries for the four `@durust/native-<target>` platform
 * packages in step with `typescript/packages/native/package.json`. They are
 * optional dependencies of the facade, and npm refuses a lockfile that lacks
 * an entry for a dependency the manifest names, so each gets an entry with
 * the version and the registry tarball it will resolve to. `--integrity`
 * (the release job, after the addons are in place) adds the `sha512` npm
 * computes for the packed directory; without it the entry carries none, and
 * npm records the published tarball's on the next install.
 */
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const nativeRoot = join(repoRoot, "typescript/packages/native");
const lockfilePath = join(repoRoot, "typescript/package-lock.json");
const withIntegrity = process.argv.includes("--integrity");
const CARRIED_FIELDS = ["cpu", "libc", "license", "os", "engines"];

export function writeNativeLockfile({ integrity = withIntegrity } = {}) {
  const facade = readJson(join(nativeRoot, "package.json"));
  const lockfile = readJson(lockfilePath);
  const written = [];
  for (const [name, version] of Object.entries(facade.optionalDependencies ?? {})) {
    if (!name.startsWith("@durust/native-")) {
      continue;
    }
    const target = name.slice("@durust/native-".length);
    const packageDir = join(nativeRoot, "npm", target);
    const manifest = readJson(join(packageDir, "package.json"));
    if (manifest.version !== version) {
      throw new Error(`${name} manifest is ${manifest.version}, expected ${version}`);
    }
    const entry = {
      version,
      resolved: `https://registry.npmjs.org/${name}/-/${name.split("/")[1]}-${version}.tgz`,
      optional: true
    };
    if (integrity) {
      entry.integrity = packIntegrity(packageDir);
    }
    for (const field of CARRIED_FIELDS) {
      if (manifest[field] !== undefined) {
        entry[field] = manifest[field];
      }
    }
    lockfile.packages[`node_modules/${name}`] = Object.fromEntries(
      Object.entries(entry).sort(([left], [right]) => left.localeCompare(right))
    );
    written.push(name);
  }
  writeFileSync(lockfilePath, `${JSON.stringify(lockfile, null, 2)}\n`);
  return written;
}

function packIntegrity(packageDir) {
  const reported = JSON.parse(
    execFileSync("npm", ["pack", "--json", "--dry-run", packageDir], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"]
    })
  );
  const [packed] = Array.isArray(reported) ? reported : Object.values(reported ?? {});
  if (typeof packed?.integrity !== "string" || !packed.integrity.startsWith("sha512-")) {
    throw new Error(`npm pack reported no sha512 integrity for ${packageDir}`);
  }
  return packed.integrity;
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  for (const name of writeNativeLockfile()) {
    console.log(`lockfile entry for ${name}`);
  }
}
