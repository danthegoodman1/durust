#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import type { DurableManifest } from "./manifest.js";

type ManifestCommand = "write" | "accept" | "check" | "diff";

interface ManifestCliIo {
  readonly cwd?: string;
  readonly stdout?: (line: string) => void;
  readonly stderr?: (line: string) => void;
}

interface ManifestCliArgs {
  readonly command: ManifestCommand;
  readonly modulePath: string;
  readonly exportName: string | null;
  readonly manifestPath: string;
}

interface ManifestExportingRegistry {
  exportManifest(): DurableManifest;
}

export async function runManifestCli(
  argv: readonly string[],
  io: ManifestCliIo = {}
): Promise<number> {
  const cwd = io.cwd ?? process.cwd();
  const stdout = io.stdout ?? ((line) => console.log(line));
  const stderr = io.stderr ?? ((line) => console.error(line));

  if (argv.includes("--help") || argv.includes("-h")) {
    stdout(manifestCliUsage());
    return 0;
  }

  try {
    const args = parseManifestCliArgs(argv);
    const current = await loadManifestFromModule(args.modulePath, {
      cwd,
      exportName: args.exportName
    });
    const manifestPath = resolve(cwd, args.manifestPath);
    const currentJson = stableManifestJson(current);

    if (args.command === "write" || args.command === "accept") {
      await writeFile(manifestPath, currentJson);
      stdout(`wrote durable manifest ${manifestPath}`);
      return 0;
    }

    const baseline = await readManifestFile(manifestPath);
    const baselineJson = stableManifestJson(baseline);
    if (baselineJson === currentJson) {
      stdout(`durable manifest matches ${manifestPath}`);
      return 0;
    }

    const diff = manifestDiff(baselineJson, currentJson, manifestPath);
    if (args.command === "diff") {
      stdout(diff);
    } else {
      stderr(`durable manifest differs from ${manifestPath}`);
      stderr(diff);
    }
    return 1;
  } catch (error) {
    stderr(error instanceof Error ? error.message : String(error));
    stderr(manifestCliUsage());
    return 1;
  }
}

async function loadManifestFromModule(
  modulePath: string,
  options: { readonly cwd?: string; readonly exportName?: string | null } = {}
): Promise<DurableManifest> {
  const cwd = options.cwd ?? process.cwd();
  const moduleUrl = pathToFileURL(resolve(cwd, modulePath)).href;
  const imported = await import(moduleUrl);
  const candidate =
    options.exportName === null || options.exportName === undefined
      ? imported.registry ?? imported.manifest ?? imported.default
      : imported[options.exportName];
  if (candidate === undefined) {
    throw new Error(
      options.exportName === null || options.exportName === undefined
        ? `module ${modulePath} must export registry, manifest, or default`
        : `module ${modulePath} does not export ${options.exportName}`
    );
  }
  return normalizeManifest(await resolveManifestCandidate(candidate));
}

function stableManifestJson(manifest: DurableManifest): string {
  return `${JSON.stringify(normalizeManifest(manifest), null, 2)}\n`;
}

function manifestDiff(
  baselineJson: string,
  currentJson: string,
  baselineLabel: string
): string {
  const baselineLines = baselineJson.trimEnd().split("\n");
  const currentLines = currentJson.trimEnd().split("\n");
  return [
    `--- ${baselineLabel}`,
    "+++ current",
    ...baselineLines.map((line) => `-${line}`),
    ...currentLines.map((line) => `+${line}`)
  ].join("\n");
}

function manifestCliUsage(): string {
  return [
    "usage: durust-manifest <write|check|diff|accept> --module FILE [options]",
    "",
    "Options:",
    "  --module, -m FILE       ESM module exporting a Registry, manifest, or factory",
    "  --export, -e NAME       export name to load; default tries registry, manifest, default",
    "  --manifest FILE         manifest baseline path; default durable.manifest.json",
    "  --out FILE              alias for --manifest when writing or accepting",
    "",
    "The module export may be a DurableManifest, a Registry with exportManifest(),",
    "or a function returning either value."
  ].join("\n");
}

function parseManifestCliArgs(argv: readonly string[]): ManifestCliArgs {
  const command = argv[0];
  if (
    command !== "write" &&
    command !== "accept" &&
    command !== "check" &&
    command !== "diff"
  ) {
    throw new Error("manifest command must be write, check, diff, or accept");
  }

  let modulePath: string | null = null;
  let exportName: string | null = null;
  let manifestPath = "durable.manifest.json";

  for (let index = 1; index < argv.length; index += 1) {
    const raw = argv[index] as string;
    const [flag, inlineValue] = raw.split("=", 2);
    const value = (name: string): string => {
      if (inlineValue !== undefined) {
        return inlineValue;
      }
      const next = argv[++index];
      if (next === undefined) {
        throw new Error(`${name} requires a value`);
      }
      return next;
    };

    switch (flag) {
      case "--module":
      case "-m":
        modulePath = value(flag);
        break;
      case "--export":
      case "-e":
        exportName = value(flag);
        break;
      case "--manifest":
        manifestPath = value(flag);
        break;
      case "--out":
        manifestPath = value(flag);
        break;
      default:
        throw new Error(`unknown manifest option ${raw}`);
    }
  }

  if (modulePath === null) {
    throw new Error("--module is required");
  }

  return { command, modulePath, exportName, manifestPath };
}

async function readManifestFile(path: string): Promise<DurableManifest> {
  const raw = await readFile(path, "utf8");
  return normalizeManifest(JSON.parse(raw) as unknown);
}

async function resolveManifestCandidate(candidate: unknown): Promise<unknown> {
  const resolved = typeof candidate === "function" ? await candidate() : candidate;
  if (isRegistry(resolved)) {
    return resolved.exportManifest();
  }
  return resolved;
}

function normalizeManifest(value: unknown): DurableManifest {
  if (!isManifestLike(value)) {
    throw new Error("durable manifest export must be a Durust TypeScript manifest");
  }
  return {
    manifestVersion: 1,
    runtime: "durust-typescript",
    workflows: [...value.workflows].map(normalizeWorkflowEntry).sort(compareWorkflowEntries),
    activities: [...value.activities].map(normalizeActivityEntry).sort(compareActivityEntries)
  };
}

function isManifestLike(value: unknown): value is DurableManifest {
  return (
    value !== null &&
    typeof value === "object" &&
    (value as { readonly manifestVersion?: unknown }).manifestVersion === 1 &&
    (value as { readonly runtime?: unknown }).runtime === "durust-typescript" &&
    Array.isArray((value as { readonly workflows?: unknown }).workflows) &&
    Array.isArray((value as { readonly activities?: unknown }).activities)
  );
}

function isRegistry(value: unknown): value is ManifestExportingRegistry {
  return (
    value !== null &&
    typeof value === "object" &&
    typeof (value as { readonly exportManifest?: unknown }).exportManifest === "function"
  );
}

function normalizeWorkflowEntry(
  entry: DurableManifest["workflows"][number]
): DurableManifest["workflows"][number] {
  return {
    name: entry.name,
    version: entry.version,
    sourcePath: entry.sourcePath ?? null,
    inputSchemaFingerprint: entry.inputSchemaFingerprint ?? null,
    outputSchemaFingerprint: entry.outputSchemaFingerprint ?? null,
    queryStateSchemaFingerprint: entry.queryStateSchemaFingerprint ?? null
  };
}

function normalizeActivityEntry(
  entry: DurableManifest["activities"][number]
): DurableManifest["activities"][number] {
  return {
    name: entry.name,
    sourcePath: entry.sourcePath ?? null,
    inputSchemaFingerprint: entry.inputSchemaFingerprint ?? null,
    outputSchemaFingerprint: entry.outputSchemaFingerprint ?? null
  };
}

function compareWorkflowEntries(
  left: DurableManifest["workflows"][number],
  right: DurableManifest["workflows"][number]
): number {
  return left.name.localeCompare(right.name) || left.version - right.version;
}

function compareActivityEntries(
  left: DurableManifest["activities"][number],
  right: DurableManifest["activities"][number]
): number {
  return left.name.localeCompare(right.name);
}

const invokedPath = process.argv[1] === undefined ? null : resolve(process.argv[1]);
if (invokedPath !== null && fileURLToPath(import.meta.url) === invokedPath) {
  process.exitCode = await runManifestCli(process.argv.slice(2));
}
