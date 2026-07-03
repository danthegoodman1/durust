#!/usr/bin/env node
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const rustManifests = ["Cargo.toml", "durust-macros/Cargo.toml", "benchtools/Cargo.toml"];
const rustPackageNames = new Map([
  ["Cargo.toml", "durust"],
  ["durust-macros/Cargo.toml", "durust-macros"],
  ["benchtools/Cargo.toml", "durust-benchtools"]
]);
const typescriptPackagesRoot = join(repoRoot, "typescript/packages");
const publishableNpmPackages = [
  "@durust/core",
  "@durust/payload",
  "@durust/sqlite",
  "@durust/postgres",
  "@durust/testing",
  "@durust/eslint-plugin"
];

const [command, ...args] = process.argv.slice(2);

if (command === "next") {
  const explicitBump = readOption(args, "--bump");
  const message = readOption(args, "--message") ?? "";
  console.log(nextVersion(readCurrentVersion(), releaseBump(explicitBump, message)));
} else if (command === "apply") {
  applyVersion(requiredVersionArg(args));
} else if (command === "check") {
  checkVersion(requiredVersionArg(args));
} else {
  fail("usage: release-version.mjs next [--bump patch|minor|major] [--message text] | apply <version> | check <version>");
}

function readCurrentVersion() {
  const corePackage = readJson(join(typescriptPackagesRoot, "core/package.json"));
  const rustVersion = readRustPackageVersion("Cargo.toml");
  if (corePackage.version !== rustVersion) {
    fail(`lockstep versions disagree: @durust/core=${corePackage.version}, durust=${rustVersion}`);
  }
  return rustVersion;
}

function releaseBump(explicitBump, message) {
  if (explicitBump && explicitBump !== "auto") {
    if (!["patch", "minor", "major"].includes(explicitBump)) {
      fail(`unsupported release bump ${explicitBump}`);
    }
    return explicitBump;
  }
  if (message.includes("#major")) {
    return "major";
  }
  if (message.includes("#minor")) {
    return "minor";
  }
  return "patch";
}

function nextVersion(version, bump) {
  const parts = parseVersion(version);
  if (bump === "major") {
    return `${parts.major + 1}.0.0`;
  }
  if (bump === "minor") {
    return `${parts.major}.${parts.minor + 1}.0`;
  }
  return `${parts.major}.${parts.minor}.${parts.patch + 1}`;
}

function applyVersion(version) {
  parseVersion(version);
  for (const manifest of rustManifests) {
    updateRustPackageVersion(manifest, version);
  }
  updateRootDurustMacrosDependency(version);
  for (const packageJsonPath of typescriptPackageJsonPaths()) {
    updateTypescriptPackage(packageJsonPath, version);
  }
}

function checkVersion(version) {
  parseVersion(version);
  for (const manifest of rustManifests) {
    const actual = readRustPackageVersion(manifest);
    if (actual !== version) {
      fail(`${manifest} version is ${actual}, expected ${version}`);
    }
  }

  const rootCargo = readFile("Cargo.toml");
  if (!rootCargo.includes(`durust-macros = { version = "${version}", path = "durust-macros" }`)) {
    fail(`Cargo.toml durust-macros dependency must use ${version}`);
  }

  for (const packageJsonPath of typescriptPackageJsonPaths()) {
    const packageJson = readJson(packageJsonPath);
    if (packageJson.version !== version) {
      fail(`${packageJson.name} version is ${packageJson.version}, expected ${version}`);
    }
    for (const field of ["dependencies", "devDependencies"]) {
      const deps = packageJson[field];
      if (!deps) {
        continue;
      }
      for (const [name, range] of Object.entries(deps)) {
        if (name.startsWith("@durust/") && range !== `^${version}`) {
          fail(`${packageJson.name} ${field}.${name} is ${range}, expected ^${version}`);
        }
      }
    }
  }
}

function readRustPackageVersion(manifest) {
  const contents = readFile(manifest);
  const packageName = rustPackageNames.get(manifest);
  const packageBlock = matchPackageBlock(contents, manifest);
  if (!packageBlock.includes(`name = "${packageName}"`)) {
    fail(`${manifest} package block does not describe ${packageName}`);
  }
  const match = packageBlock.match(/^version = "([^"]+)"$/mu);
  if (!match) {
    fail(`${manifest} package block is missing version`);
  }
  return match[1];
}

function updateRustPackageVersion(manifest, version) {
  const contents = readFile(manifest);
  const updated = replacePackageBlock(contents, manifest, (block) =>
    block.replace(/^version = "[^"]+"$/mu, `version = "${version}"`)
  );
  writeFile(manifest, updated);
}

function updateRootDurustMacrosDependency(version) {
  const manifest = "Cargo.toml";
  const contents = readFile(manifest);
  const dependencyPattern = /^durust-macros = \{ version = "[^"]+", path = "durust-macros" \}$/mu;
  if (!dependencyPattern.test(contents)) {
    fail("Cargo.toml durust-macros dependency was not found");
  }
  const updated = contents.replace(
    dependencyPattern,
    `durust-macros = { version = "${version}", path = "durust-macros" }`
  );
  writeFile(manifest, updated);
}

function updateTypescriptPackage(packageJsonPath, version) {
  const packageJson = readJson(packageJsonPath);
  packageJson.version = version;
  for (const field of ["dependencies", "devDependencies"]) {
    const deps = packageJson[field];
    if (!deps) {
      continue;
    }
    for (const name of Object.keys(deps)) {
      if (name.startsWith("@durust/")) {
        deps[name] = `^${version}`;
      }
    }
  }
  writeJson(packageJsonPath, packageJson);
}

function typescriptPackageJsonPaths() {
  return readdirSync(typescriptPackagesRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => join(typescriptPackagesRoot, entry.name, "package.json"))
    .sort();
}

function matchPackageBlock(contents, manifest) {
  const match = contents.match(/^\[package\]\n(?<block>(?:^[^\[\n].*\n?)*)/mu);
  if (!match?.groups?.block) {
    fail(`${manifest} is missing a [package] block`);
  }
  return match.groups.block;
}

function replacePackageBlock(contents, manifest, update) {
  return contents.replace(/^\[package\]\n(?<block>(?:^[^\[\n].*\n?)*)/mu, (match, block) => {
    const updatedBlock = update(block);
    return `[package]\n${updatedBlock}`;
  });
}

function parseVersion(version) {
  const match = version.match(/^(?<major>0|[1-9]\d*)\.(?<minor>0|[1-9]\d*)\.(?<patch>0|[1-9]\d*)$/u);
  if (!match?.groups) {
    fail(`unsupported semver version ${version}`);
  }
  return {
    major: Number(match.groups.major),
    minor: Number(match.groups.minor),
    patch: Number(match.groups.patch)
  };
}

function requiredVersionArg(args) {
  const version = args[0];
  if (!version) {
    fail(`${command} requires a version`);
  }
  return version;
}

function readOption(args, name) {
  const index = args.indexOf(name);
  if (index < 0) {
    return undefined;
  }
  return args[index + 1] ?? "";
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function readFile(path) {
  return readFileSync(join(repoRoot, path), "utf8");
}

function writeFile(path, contents) {
  writeFileSync(join(repoRoot, path), contents);
}

function fail(message) {
  console.error(message);
  process.exit(1);
}
