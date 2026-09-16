#!/usr/bin/env node
/**
 * Pins the four `@durust/native-<target>` platform packages to the facade:
 * one directory per napi target, each manifest at the facade's version with
 * the `os`, `cpu`, and `libc` constraints npm uses to pick it, shipping
 * exactly the addon files the loader in `packages/native/src/index.ts` looks
 * for. With `--require-binaries` (the release job) every addon must be
 * present; without it (CI) the manifests alone are checked.
 */
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const nativeRoot = join(repoRoot, "typescript/packages/native");
const requireBinaries = process.argv.includes("--require-binaries");

export const nativeTargets = [
  { triple: "aarch64-apple-darwin", target: "darwin-arm64", os: ["darwin"], cpu: ["arm64"] },
  { triple: "x86_64-apple-darwin", target: "darwin-x64", os: ["darwin"], cpu: ["x64"] },
  { triple: "aarch64-unknown-linux-gnu", target: "linux-arm64-gnu", os: ["linux"], cpu: ["arm64"], libc: ["glibc"] },
  { triple: "x86_64-unknown-linux-gnu", target: "linux-x64-gnu", os: ["linux"], cpu: ["x64"], libc: ["glibc"] }
];

const failures = [];
const facade = readJson(join(nativeRoot, "package.json"));
const binaryName = facade.napi?.binaryName;
if (binaryName !== "durust-node") {
  failures.push(`facade napi.binaryName must be durust-node, got ${String(binaryName)}`);
}
if (JSON.stringify(facade.napi?.targets) !== JSON.stringify(nativeTargets.map((t) => t.triple))) {
  failures.push("facade napi.targets must list the four supported triples in order");
}
if ((facade.files ?? []).some((pattern) => pattern.includes(".node"))) {
  failures.push("the facade must not ship addon binaries; the platform packages do");
}

for (const target of nativeTargets) {
  const name = `@durust/native-${target.target}`;
  const dir = join(nativeRoot, "npm", target.target);
  const manifestPath = join(dir, "package.json");
  if (!existsSync(manifestPath)) {
    failures.push(`${name}: missing ${manifestPath}`);
    continue;
  }
  const manifest = readJson(manifestPath);
  const binary = `${binaryName}.${target.target}.node`;
  const binaries = [binary, `durust-sqlite.${target.target}.node`];
  const checks = [
    [manifest.name === name, `name must be ${name}`],
    [manifest.version === facade.version, `version must be ${facade.version}, got ${manifest.version}`],
    [sameArray(manifest.os, target.os), `os must be ${JSON.stringify(target.os)}`],
    [sameArray(manifest.cpu, target.cpu), `cpu must be ${JSON.stringify(target.cpu)}`],
    [sameArray(manifest.libc, target.libc), `libc must be ${JSON.stringify(target.libc ?? null)}`],
    [manifest.main === binary, `main must be ${binary}`],
    [sameArray(manifest.files, binaries), `files must be exactly ${JSON.stringify(binaries)}`],
    [manifest.license === facade.license, `license must match the facade`],
    [manifest.engines?.node === facade.engines?.node, `engines.node must match the facade`],
    [facade.optionalDependencies?.[name] === facade.version, `facade optionalDependencies must pin ${name} to ${facade.version}`]
  ];
  for (const [ok, message] of checks) {
    if (!ok) {
      failures.push(`${name}: ${message}`);
    }
  }
  for (const file of binaries) {
    if (requireBinaries && !existsSync(join(dir, file))) {
      failures.push(`${name}: ${file} is missing from ${dir}`);
    }
  }
}

if (failures.length > 0) {
  console.error("native platform packages do not match the facade:");
  for (const failure of failures) {
    console.error(`- ${failure}`);
  }
  process.exit(1);
}
console.log(`native platform packages match @durust/native ${facade.version}`);

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function sameArray(actual, expected) {
  if (expected === undefined) {
    return actual === undefined;
  }
  return Array.isArray(actual) && JSON.stringify(actual) === JSON.stringify(expected);
}
