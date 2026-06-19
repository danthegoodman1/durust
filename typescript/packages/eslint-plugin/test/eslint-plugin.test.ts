import { describe, expect, it } from "vitest";
import plugin, {
  checkConstructor,
  checkIdentifierCall,
  checkModuleSpecifier,
  checkStaticCall,
  noWorkflowNondeterminismRule
} from "@durust/eslint-plugin";

describe("@durust/eslint-plugin", () => {
  it("exports the workflow nondeterminism rule and recommended flat config", () => {
    expect(plugin.rules["no-workflow-nondeterminism"]).toBe(noWorkflowNondeterminismRule);
    expect(plugin.configs.recommended.rules).toEqual({
      "durust/no-workflow-nondeterminism": "error"
    });
  });

  it("classifies hidden IO and native async APIs", () => {
    expect(checkModuleSpecifier("node:fs")).toMatchObject({
      code: "durust/no-hidden-io",
      messageId: "hiddenIo"
    });
    expect(checkModuleSpecifier("node:http")?.message).toContain("network I/O");
    expect(checkIdentifierCall("fetch")?.message).toContain("network I/O");
    expect(checkStaticCall("Promise.race")?.message).toContain("select()");
    expect(checkConstructor("WebSocket")?.message).toContain("network I/O");
    expect(checkModuleSpecifier("@durust/core")).toBeNull();
  });

  it("reports ESLint-style diagnostics from rule visitors", () => {
    const reports: unknown[] = [];
    const listeners = noWorkflowNondeterminismRule.create({
      report(report) {
        reports.push(report);
      }
    });

    listeners.ImportDeclaration({
      type: "ImportDeclaration",
      source: { type: "Literal", value: "node:fs" }
    });
    listeners.CallExpression({
      type: "CallExpression",
      callee: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "Promise" },
        property: { type: "Identifier", name: "all" }
      },
      arguments: []
    });
    listeners.NewExpression({
      type: "NewExpression",
      callee: { type: "Identifier", name: "WebSocket" }
    });

    expect(reports).toHaveLength(3);
    expect(reports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ messageId: "hiddenIo" }),
        expect.objectContaining({ messageId: "nativeAsync" })
      ])
    );
  });
});
