#!/usr/bin/env node

import { readFile, stat } from "node:fs/promises";
import { relative, resolve } from "node:path";
import ts from "typescript";
import { glob } from "tinyglobby";
import {
  checkConstructor,
  checkIdentifierCall,
  checkModuleSpecifier,
  checkStaticCall
} from "../packages/eslint-plugin/dist/index.js";

const SOURCE_EXTENSIONS = new Set([".ts", ".tsx", ".mts", ".cts"]);
const DEFAULT_CONFIG_FILES = ["durust.config.json", "package.json"];
const DEFAULT_IGNORES = ["**/node_modules/**", "**/dist/**", "**/coverage/**"];

function usage() {
  return [
    "usage: node scripts/determinism-lint.mjs [--config FILE] [--workflow-source GLOB]... [file-or-directory-or-glob]...",
    "",
    "Scans workflow source files for APIs that are nondeterministic under Durust replay.",
    "",
    "When no source is provided, reads workflowSources from durust.config.json or package.json durust.workflowSources."
  ].join("\n");
}

async function main(argv) {
  if (argv.includes("--help") || argv.includes("-h")) {
    console.log(usage());
    return 0;
  }

  const parsed = parseArgs(argv);
  const sourceSpecs =
    parsed.sources.length > 0 ? parsed.sources : await loadConfiguredSources(parsed.configPath);
  if (sourceSpecs.length === 0) {
    console.error(usage());
    console.error("");
    console.error("durust determinism lint needs at least one workflow source glob or path");
    return 1;
  }

  const files = [];
  for (const source of sourceSpecs) {
    files.push(...await expandSourceSpec(source));
  }
  const uniqueFiles = [...new Set(files)].sort();
  const diagnostics = [];
  for (const file of uniqueFiles) {
    diagnostics.push(...await lintFile(file));
  }

  if (diagnostics.length > 0) {
    for (const diagnostic of diagnostics) {
      console.error(formatDiagnostic(diagnostic));
    }
    return 1;
  }
  return 0;
}

function parseArgs(argv) {
  const sources = [];
  let configPath = null;
  for (let index = 0; index < argv.length; index += 1) {
    const raw = argv[index];
    const [flag, inlineValue] = raw.split("=", 2);
    const value = (name) => {
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
      case "--config":
        configPath = value(flag);
        break;
      case "--workflow-source":
      case "--workflow-source-glob":
        sources.push(value(flag));
        break;
      default:
        sources.push(raw);
        break;
    }
  }
  return { configPath, sources };
}

async function loadConfiguredSources(configPath) {
  if (configPath !== null) {
    return await workflowSourcesFromConfigFile(resolve(configPath));
  }
  for (const candidate of DEFAULT_CONFIG_FILES) {
    const path = resolve(candidate);
    if (await fileExists(path)) {
      const sources = await workflowSourcesFromConfigFile(path);
      if (sources.length > 0) {
        return sources;
      }
    }
  }
  return [];
}

async function workflowSourcesFromConfigFile(path) {
  const raw = JSON.parse(await readFile(path, "utf8"));
  const sources =
    raw.workflowSources ??
    raw.workflowSourceGlobs ??
    raw.durust?.workflowSources ??
    raw.durust?.workflowSourceGlobs ??
    [];
  if (!Array.isArray(sources) || !sources.every((source) => typeof source === "string")) {
    throw new Error(`${path} workflowSources must be an array of strings`);
  }
  return sources;
}

async function fileExists(path) {
  try {
    const info = await stat(path);
    return info.isFile();
  } catch {
    return false;
  }
}

async function expandSourceSpec(source) {
  if (hasGlobMagic(source)) {
    return await sourceFilesFromGlob(source);
  }
  const path = resolve(source);
  const info = await stat(path);
  if (info.isFile()) {
    return isSourceFile(path) ? [path] : [];
  }
  if (info.isDirectory()) {
    return await sourceFilesFromGlob(`${toPosix(relative(process.cwd(), path))}/**/*.{ts,tsx,mts,cts}`);
  }
  return [];
}

async function sourceFilesFromGlob(pattern) {
  const matches = await glob(pattern, {
    absolute: true,
    cwd: process.cwd(),
    dot: true,
    onlyFiles: true,
    ignore: DEFAULT_IGNORES
  });
  return matches.filter(isSourceFile);
}

function hasGlobMagic(source) {
  return /[*?[\]{}]/.test(source);
}

function toPosix(path) {
  const normalized = path.split("\\").join("/");
  if (normalized.length === 0) {
    return ".";
  }
  return normalized;
}

function isSourceFile(path) {
  const dotIndex = path.lastIndexOf(".");
  if (dotIndex < 0) {
    return false;
  }
  return SOURCE_EXTENSIONS.has(path.slice(dotIndex));
}

async function lintFile(path) {
  const text = await readFile(path, "utf8");
  const sourceFile = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true);
  const diagnostics = [];

  function report(node, code, message) {
    const position = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
    diagnostics.push({
      path,
      line: position.line + 1,
      column: position.character + 1,
      code,
      message
    });
  }

  function reportViolation(node, violation) {
    if (violation !== null) {
      report(node, violation.code, violation.message);
    }
  }

  function visit(node) {
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
      const moduleName = moduleSpecifierText(node.moduleSpecifier);
      if (moduleName !== null) {
        reportViolation(node, checkModuleSpecifier(moduleName));
      }
    }

    if (ts.isCallExpression(node)) {
      const expression = node.expression;
      if (expression.kind === ts.SyntaxKind.ImportKeyword) {
        const moduleName = firstStringArgument(node);
        if (moduleName !== null) {
          reportViolation(node, checkModuleSpecifier(moduleName));
        }
      } else if (ts.isIdentifier(expression)) {
        if (expression.text === "require") {
          const moduleName = firstStringArgument(node);
          if (moduleName !== null) {
            reportViolation(node, checkModuleSpecifier(moduleName));
          }
        }
        reportViolation(node, checkIdentifierCall(expression.text));
      } else if (ts.isPropertyAccessExpression(expression)) {
        const staticName = propertyAccessName(expression);
        if (staticName !== null) {
          reportViolation(node, checkStaticCall(staticName));
        }
      }
    }

    if (ts.isNewExpression(node) && ts.isIdentifier(node.expression)) {
      reportViolation(node, checkConstructor(node.expression.text));
    }

    ts.forEachChild(node, visit);
  }

  visit(sourceFile);
  return diagnostics;
}

function moduleSpecifierText(moduleSpecifier) {
  if (moduleSpecifier === undefined || !ts.isStringLiteralLike(moduleSpecifier)) {
    return null;
  }
  return moduleSpecifier.text;
}

function firstStringArgument(node) {
  const first = node.arguments[0];
  return first !== undefined && ts.isStringLiteralLike(first) ? first.text : null;
}

function propertyAccessName(node) {
  if (!ts.isIdentifier(node.expression)) {
    return null;
  }
  return `${node.expression.text}.${node.name.text}`;
}

function formatDiagnostic(diagnostic) {
  const displayPath = relative(process.cwd(), diagnostic.path);
  return `${displayPath}:${diagnostic.line}:${diagnostic.column} ${diagnostic.code} ${diagnostic.message}`;
}

try {
  const exitCode = await main(process.argv.slice(2));
  process.exitCode = exitCode;
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}
