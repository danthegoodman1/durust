#!/usr/bin/env node

import { readFile, stat } from "node:fs/promises";
import { relative, resolve } from "node:path";
import ts from "typescript";
import { glob } from "tinyglobby";
import {
  checkActivityOnlyWorkflowApi,
  checkAwaitExpression,
  checkConstructor,
  checkIdentifierCall,
  checkIdentifierReference,
  checkImportBinding,
  checkModuleSpecifier,
  checkStaticCall,
  checkStaticRead,
  checkStaticReference
} from "../packages/eslint-plugin/dist/index.js";

const SOURCE_EXTENSIONS = new Set([".ts", ".tsx", ".mts", ".cts"]);
const DEFAULT_CONFIG_FILES = ["durust.config.json", "package.json"];
const DEFAULT_IGNORES = ["**/node_modules/**", "**/dist/**", "**/coverage/**"];
const DURUST_CORE_MODULES = new Set(["@durust/core"]);
const ACTIVITY_ONLY_WORKFLOW_APIS = new Set(["heartbeat"]);

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
  const durableAwaitIdentifiers = collectDurableAwaitIdentifiers(sourceFile);
  const durustImports = collectDurustWorkflowApiImports(sourceFile);

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
    if (ts.isVariableDeclaration(node)) {
      reportForbiddenStaticReferenceAliases(node);
    }

    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
      const moduleName = moduleSpecifierText(node.moduleSpecifier);
      if (moduleName !== null) {
        reportViolation(node, checkModuleSpecifier(moduleName));
      }
      if (ts.isImportDeclaration(node) && moduleName !== null) {
        reportForbiddenImportBindings(node, moduleName);
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
        const activityOnlyApi = durustImports.activityOnlyIdentifiers.get(expression.text);
        if (activityOnlyApi !== undefined) {
          reportViolation(
            node,
            checkActivityOnlyWorkflowApi(activityOnlyApi, `${expression.text}()`)
          );
        }
        if (expression.text === "require") {
          const moduleName = firstStringArgument(node);
          if (moduleName !== null) {
            reportViolation(node, checkModuleSpecifier(moduleName));
          }
        }
        reportViolation(node, checkIdentifierCall(expression.text));
      } else if (isMemberAccessExpression(expression)) {
        const activityOnlyViolation = checkDurustCoreNamespaceActivityOnlyCall(
          expression,
          durustImports.durustCoreNamespaces
        );
        if (activityOnlyViolation !== null) {
          reportViolation(node, activityOnlyViolation);
        }
        const staticName = memberAccessName(expression);
        if (staticName !== null) {
          reportViolation(node, checkStaticCall(staticName));
        }
      }
    }

    if (isMemberAccessExpression(node)) {
      const staticName = memberAccessName(node);
      if (staticName !== null) {
        reportViolation(node, checkStaticRead(staticName));
      }
    }

    if (ts.isNewExpression(node) && ts.isIdentifier(node.expression)) {
      reportViolation(node, checkConstructor(node.expression.text, {
        argumentCount: node.arguments?.length ?? 0
      }));
    }
    if (ts.isNewExpression(node) && isMemberAccessExpression(node.expression)) {
      const staticName = memberAccessName(node.expression);
      if (staticName !== null) {
        reportViolation(node, checkConstructor(staticName, {
          argumentCount: node.arguments?.length ?? 0
        }));
      }
    }

    if (ts.isAwaitExpression(node)) {
      reportViolation(
        node,
        checkAwaitExpression(awaitDescriptor(node.expression, durableAwaitIdentifiers, sourceFile))
      );
    }

    ts.forEachChild(node, visit);
  }

  function reportForbiddenStaticReferenceAliases(node) {
    const initializer = node.initializer;
    if (initializer === undefined) {
      return;
    }
    if (isMemberAccessExpression(initializer)) {
      const staticName = memberAccessName(initializer);
      if (staticName !== null) {
        reportViolation(node, checkStaticReference(staticName));
      }
      return;
    }
    if (ts.isIdentifier(initializer) && !ts.isObjectBindingPattern(node.name)) {
      reportViolation(node, checkIdentifierReference(initializer.text));
      return;
    }
    if (!ts.isObjectBindingPattern(node.name) || !ts.isIdentifier(initializer)) {
      return;
    }
    for (const element of node.name.elements) {
      const propertyName = objectBindingElementPropertyName(element);
      if (propertyName !== null) {
        reportViolation(element, checkStaticReference(`${initializer.text}.${propertyName}`));
      }
    }
  }

  function reportForbiddenImportBindings(node, moduleName) {
    const importClause = node.importClause;
    if (importClause === undefined || importClause.isTypeOnly) {
      return;
    }
    if (importClause.name !== undefined) {
      reportViolation(importClause.name, checkImportBinding(moduleName, "default"));
    }
    const namedBindings = importClause.namedBindings;
    if (namedBindings === undefined) {
      return;
    }
    if (ts.isNamespaceImport(namedBindings)) {
      reportViolation(namedBindings, checkImportBinding(moduleName, "*"));
      return;
    }
    for (const element of namedBindings.elements) {
      if (element.isTypeOnly) {
        continue;
      }
      const importedName = (element.propertyName ?? element.name).text;
      reportViolation(element, checkImportBinding(moduleName, importedName));
    }
  }

  visit(sourceFile);
  return diagnostics;
}

function collectDurustWorkflowApiImports(sourceFile) {
  const activityOnlyIdentifiers = new Map();
  const durustCoreNamespaces = new Set();

  function visit(node) {
    if (ts.isImportDeclaration(node)) {
      const moduleName = moduleSpecifierText(node.moduleSpecifier);
      if (moduleName !== null && DURUST_CORE_MODULES.has(moduleName)) {
        const importClause = node.importClause;
        if (importClause !== undefined && !importClause.isTypeOnly) {
          const namedBindings = importClause.namedBindings;
          if (namedBindings !== undefined) {
            if (ts.isNamespaceImport(namedBindings)) {
              durustCoreNamespaces.add(namedBindings.name.text);
            } else {
              for (const element of namedBindings.elements) {
                if (element.isTypeOnly) {
                  continue;
                }
                const importedName = (element.propertyName ?? element.name).text;
                if (ACTIVITY_ONLY_WORKFLOW_APIS.has(importedName)) {
                  activityOnlyIdentifiers.set(element.name.text, importedName);
                }
              }
            }
          }
        }
      }
    }
    ts.forEachChild(node, visit);
  }

  visit(sourceFile);
  return { activityOnlyIdentifiers, durustCoreNamespaces };
}

function collectDurableAwaitIdentifiers(sourceFile) {
  const identifiers = new Set();
  function visit(node) {
    if (
      ts.isVariableDeclaration(node) &&
      ts.isIdentifier(node.name) &&
      node.initializer !== undefined &&
      checkAwaitExpression(awaitDescriptor(node.initializer, identifiers, sourceFile)) === null
    ) {
      identifiers.add(node.name.text);
    }
    ts.forEachChild(node, visit);
  }
  visit(sourceFile);
  return identifiers;
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

function isMemberAccessExpression(node) {
  return ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node);
}

function memberAccessName(node) {
  const objectName = memberAccessObjectName(node.expression);
  if (objectName === null) {
    return null;
  }
  const propertyName = ts.isPropertyAccessExpression(node)
    ? node.name.text
    : elementAccessStringName(node);
  return propertyName === null ? null : `${objectName}.${propertyName}`;
}

function memberAccessObjectName(node) {
  if (ts.isIdentifier(node)) {
    return node.text;
  }
  if (isMemberAccessExpression(node)) {
    return memberAccessName(node);
  }
  return null;
}

function elementAccessStringName(node) {
  const argument = node.argumentExpression;
  return argument !== undefined && ts.isStringLiteralLike(argument) ? argument.text : null;
}

function objectBindingElementPropertyName(element) {
  const name = element.propertyName ?? element.name;
  if (ts.isIdentifier(name) || ts.isStringLiteralLike(name)) {
    return name.text;
  }
  return null;
}

function checkDurustCoreNamespaceActivityOnlyCall(node, durustCoreNamespaces) {
  if (!ts.isIdentifier(node.expression)) {
    return null;
  }
  const objectName = node.expression.text;
  if (!durustCoreNamespaces.has(objectName)) {
    return null;
  }
  const apiName = ts.isPropertyAccessExpression(node)
    ? node.name.text
    : elementAccessStringName(node);
  if (apiName === null) {
    return null;
  }
  return checkActivityOnlyWorkflowApi(apiName, `${objectName}.${apiName}()`);
}

function awaitDescriptor(node, durableAwaitIdentifiers, sourceFile) {
  if (ts.isIdentifier(node)) {
    return durableAwaitIdentifiers.has(node.text)
      ? { kind: "durableIdentifier", name: node.text }
      : { kind: "identifier", name: node.text };
  }
  if (ts.isCallExpression(node)) {
    const expression = node.expression;
    if (ts.isIdentifier(expression)) {
      return { kind: "call", name: expression.text };
    }
    if (isMemberAccessExpression(expression)) {
      return {
        kind: "memberCall",
        name: ts.isPropertyAccessExpression(expression)
          ? expression.name.text
          : elementAccessStringName(expression) ?? "member",
        displayName: expression.getText(sourceFile)
      };
    }
    return { kind: "other", name: "call expression" };
  }
  if (isMemberAccessExpression(node)) {
    return { kind: "other", name: node.getText(sourceFile) };
  }
  if (ts.isNewExpression(node)) {
    return { kind: "other", name: node.getText(sourceFile) };
  }
  return { kind: "other", name: ts.SyntaxKind[node.kind] ?? "expression" };
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
