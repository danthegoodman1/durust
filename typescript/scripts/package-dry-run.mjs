#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const workspaceRoot = fileURLToPath(new URL("..", import.meta.url));
const packagesRoot = join(workspaceRoot, "packages");
const expectedLicense = "MIT";
const expectedRepository = {
  type: "git",
  url: "git+https://github.com/danthegoodman1/durust.git"
};
const expectedFilesField = [
  "dist/**/*.d.ts",
  "dist/**/*.d.ts.map",
  "dist/**/*.js",
  "dist/**/*.js.map",
  "dist/**/*.json"
];
const checkedPackages = [];
const skippedPackages = [];
const failures = [];

const cacheDir = mkdtempSync(join(tmpdir(), "durust-npm-pack-cache-"));

try {
  for (const entry of readdirSync(packagesRoot, { withFileTypes: true }).sort((left, right) =>
    left.name.localeCompare(right.name)
  )) {
    if (!entry.isDirectory()) {
      continue;
    }

    const packageDir = join(packagesRoot, entry.name);
    const packageJsonPath = join(packageDir, "package.json");
    const packageJson = readJson(packageJsonPath);
    if (!hasPublishSurface(packageJson)) {
      skippedPackages.push(packageJson.name ?? entry.name);
      continue;
    }

    checkedPackages.push(packageJson.name);
    validateManifest(packageJson);
    const packument = npmPackDryRun(packageDir, packageJson.name);
    if (packument !== null) {
      validatePackedFiles(packageJson, packument.files.map((file) => file.path).sort());
    }
  }
} finally {
  rmSync(cacheDir, { force: true, recursive: true });
}

if (failures.length > 0) {
  console.error("package dry-run validation failed:");
  for (const failure of failures) {
    console.error(`- ${failure}`);
  }
  process.exit(1);
}

console.log(`package dry-run validation checked ${checkedPackages.length} packages`);
if (skippedPackages.length > 0) {
  console.log(`skipped non-publishable packages: ${skippedPackages.join(", ")}`);
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function hasPublishSurface(packageJson) {
  return (
    Array.isArray(packageJson.files) ||
    packageJson.exports !== undefined ||
    packageJson.bin !== undefined ||
    packageJson.main !== undefined ||
    packageJson.types !== undefined
  );
}

function validateManifest(packageJson) {
  const packageName = packageJson.name;
  if (packageJson.license !== expectedLicense) {
    failures.push(`${packageName} package.json license must be ${expectedLicense}`);
  }

  if (
    packageJson.repository === null ||
    typeof packageJson.repository !== "object" ||
    packageJson.repository.type !== expectedRepository.type ||
    packageJson.repository.url !== expectedRepository.url
  ) {
    failures.push(
      `${packageName} package.json repository must point to ${expectedRepository.url}`
    );
  }

  if (!sameStringArray(packageJson.files, expectedFilesField)) {
    failures.push(
      `${packageName} package.json files must list only built dist artifact globs`
    );
  }

  for (const [field, value] of [
    ["main", packageJson.main],
    ["types", packageJson.types]
  ]) {
    if (typeof value !== "string" || !value.startsWith("./dist/")) {
      failures.push(`${packageName} package.json ${field} must point into ./dist`);
    }
  }

  const rootExport = packageJson.exports?.["."];
  if (
    rootExport === null ||
    typeof rootExport !== "object" ||
    typeof rootExport.import !== "string" ||
    typeof rootExport.types !== "string" ||
    !rootExport.import.startsWith("./dist/") ||
    !rootExport.types.startsWith("./dist/")
  ) {
    failures.push(`${packageName} package.json exports["."] must expose dist import/types`);
  }

  if (packageJson.bin !== undefined) {
    if (typeof packageJson.bin !== "object" || packageJson.bin === null) {
      failures.push(`${packageName} package.json bin must be an object`);
    } else {
      for (const [binName, binPath] of Object.entries(packageJson.bin)) {
        if (typeof binPath !== "string" || !binPath.startsWith("./dist/")) {
          failures.push(`${packageName} bin ${binName} must point into ./dist`);
        }
      }
    }
  }
}

function npmPackDryRun(packageDir, packageName) {
  const result = spawnSync(
    "npm",
    ["pack", "--dry-run", "--json", "--ignore-scripts", "--loglevel=error"],
    {
      cwd: packageDir,
      encoding: "utf8",
      env: {
        ...process.env,
        npm_config_cache: cacheDir
      }
    }
  );

  if (result.status !== 0) {
    failures.push(
      `${packageName} npm pack --dry-run failed: ${result.stderr.trim() || result.stdout.trim()}`
    );
    return null;
  }

  const jsonStart = result.stdout.indexOf("[");
  if (jsonStart < 0) {
    failures.push(`${packageName} npm pack --dry-run did not return JSON`);
    return null;
  }

  const packuments = JSON.parse(result.stdout.slice(jsonStart));
  if (!Array.isArray(packuments) || packuments.length !== 1) {
    failures.push(`${packageName} npm pack --dry-run returned unexpected JSON`);
    return null;
  }
  return packuments[0];
}

function validatePackedFiles(packageJson, paths) {
  const packageName = packageJson.name;
  const pathSet = new Set(paths);
  for (const required of requiredPackageFiles(packageJson)) {
    if (!pathSet.has(required)) {
      failures.push(`${packageName} package is missing required file ${required}`);
    }
  }

  for (const path of paths) {
    if (path === "package.json" || isRootDoc(path)) {
      continue;
    }
    if (!path.startsWith("dist/")) {
      failures.push(`${packageName} package includes non-dist file ${path}`);
      continue;
    }
    if (path.split("/").some((part) => part.startsWith("."))) {
      failures.push(`${packageName} package includes hidden build file ${path}`);
      continue;
    }
    if (!isAllowedDistArtifact(path)) {
      failures.push(`${packageName} package includes unexpected dist artifact ${path}`);
    }
  }
}

function requiredPackageFiles(packageJson) {
  const required = ["package.json", normalizePackagePath(packageJson.main), normalizePackagePath(packageJson.types)];
  const rootExport = packageJson.exports?.["."];
  if (rootExport && typeof rootExport === "object") {
    required.push(normalizePackagePath(rootExport.import), normalizePackagePath(rootExport.types));
  }
  if (packageJson.bin && typeof packageJson.bin === "object") {
    for (const binPath of Object.values(packageJson.bin)) {
      required.push(normalizePackagePath(binPath));
    }
  }
  return [...new Set(required.filter((path) => path.length > 0))];
}

function normalizePackagePath(path) {
  return typeof path === "string" ? path.replace(/^\.\//u, "") : "";
}

function sameStringArray(left, right) {
  return (
    Array.isArray(left) &&
    left.length === right.length &&
    left.every((value, index) => value === right[index])
  );
}

function isRootDoc(path) {
  return /^(?:readme|license|licence|changelog)(?:\..*)?$/iu.test(path);
}

function isAllowedDistArtifact(path) {
  return (
    path.endsWith(".d.ts") ||
    path.endsWith(".d.ts.map") ||
    path.endsWith(".js") ||
    path.endsWith(".js.map") ||
    path.endsWith(".json")
  );
}
