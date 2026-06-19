export const ruleName = "no-workflow-nondeterminism";

export type WorkflowDeterminismMessageId = "hiddenIo" | "nativeAsync";

export interface WorkflowDeterminismViolation {
  readonly code: `durust/${typeof ruleName}` | "durust/no-hidden-io" | "durust/no-native-async";
  readonly messageId: WorkflowDeterminismMessageId;
  readonly message: string;
}

export interface EslintRuleContext {
  report(descriptor: {
    readonly node: unknown;
    readonly messageId: WorkflowDeterminismMessageId;
    readonly data: { readonly message: string };
  }): void;
}

export interface EslintRuleModule {
  readonly meta: {
    readonly type: "problem";
    readonly docs: {
      readonly description: string;
      readonly recommended: true;
    };
    readonly schema: readonly unknown[];
    readonly messages: Record<WorkflowDeterminismMessageId, string>;
  };
  create(context: EslintRuleContext): Record<string, (node: EslintNode) => void>;
}

export interface EslintNode {
  readonly type: string;
  readonly [key: string]: unknown;
}

export interface EslintPlugin {
  readonly meta: {
    readonly name: string;
    readonly version: string;
  };
  readonly rules: Record<typeof ruleName, EslintRuleModule>;
  configs: {
    recommended: {
      readonly plugins: {
        readonly durust: EslintPlugin;
      };
      readonly rules: Record<`durust/${typeof ruleName}`, "error">;
    };
  };
}

const FORBIDDEN_MODULES = new Map([
  ["fs", "filesystem I/O is not allowed in workflow code"],
  ["node:fs", "filesystem I/O is not allowed in workflow code"],
  ["fs/promises", "filesystem I/O is not allowed in workflow code"],
  ["node:fs/promises", "filesystem I/O is not allowed in workflow code"],
  ["http", "network I/O is not allowed in workflow code"],
  ["node:http", "network I/O is not allowed in workflow code"],
  ["https", "network I/O is not allowed in workflow code"],
  ["node:https", "network I/O is not allowed in workflow code"],
  ["net", "network I/O is not allowed in workflow code"],
  ["node:net", "network I/O is not allowed in workflow code"],
  ["tls", "network I/O is not allowed in workflow code"],
  ["node:tls", "network I/O is not allowed in workflow code"],
  ["dns", "network I/O is not allowed in workflow code"],
  ["node:dns", "network I/O is not allowed in workflow code"],
  ["child_process", "child processes are not allowed in workflow code"],
  ["node:child_process", "child processes are not allowed in workflow code"],
  ["worker_threads", "worker threads are not allowed in workflow code"],
  ["node:worker_threads", "worker threads are not allowed in workflow code"],
  ["undici", "network I/O is not allowed in workflow code"]
]);

const FORBIDDEN_IDENTIFIER_CALLS = new Map([
  ["setTimeout", "use durust sleep() or sleepUntil()"],
  ["setInterval", "use durust sleep() or recurring workflow timers"],
  ["queueMicrotask", "use durust durable operations"],
  ["fetch", "network I/O is not allowed in workflow code"]
]);

const FORBIDDEN_CONSTRUCTORS = new Map([
  ["XMLHttpRequest", "network I/O is not allowed in workflow code"],
  ["WebSocket", "network I/O is not allowed in workflow code"]
]);

const FORBIDDEN_STATIC_CALLS = new Map([
  ["Date.now", "use workflow time APIs such as sleepUntil()"],
  ["Math.random", "use sideEffect() for recorded nondeterministic values"],
  ["Promise.all", "use durust join() or joinAll()"],
  ["Promise.race", "use durust select() or selectAll()"],
  ["Promise.allSettled", "use durust join()/joinAll() plus explicit error handling"],
  ["Promise.any", "use durust select() or selectAll()"]
]);

export function checkModuleSpecifier(moduleName: string): WorkflowDeterminismViolation | null {
  const reason = FORBIDDEN_MODULES.get(moduleName);
  if (reason === undefined) {
    return null;
  }
  return {
    code: "durust/no-hidden-io",
    messageId: "hiddenIo",
    message: `${reason}: ${moduleName}`
  };
}

export function checkIdentifierCall(name: string): WorkflowDeterminismViolation | null {
  const replacement = FORBIDDEN_IDENTIFIER_CALLS.get(name);
  if (replacement === undefined) {
    return null;
  }
  return {
    code: "durust/no-native-async",
    messageId: "nativeAsync",
    message: `${name}() is not allowed in workflow code; ${replacement}`
  };
}

export function checkStaticCall(name: string): WorkflowDeterminismViolation | null {
  const replacement = FORBIDDEN_STATIC_CALLS.get(name);
  if (replacement === undefined) {
    return null;
  }
  return {
    code: "durust/no-native-async",
    messageId: "nativeAsync",
    message: `${name}() is not allowed in workflow code; ${replacement}`
  };
}

export function checkConstructor(name: string): WorkflowDeterminismViolation | null {
  const replacement = FORBIDDEN_CONSTRUCTORS.get(name);
  if (replacement === undefined) {
    return null;
  }
  return {
    code: "durust/no-hidden-io",
    messageId: "hiddenIo",
    message: `${name} is not allowed in workflow code; ${replacement}`
  };
}

export const noWorkflowNondeterminismRule: EslintRuleModule = {
  meta: {
    type: "problem",
    docs: {
      description: "disallow nondeterministic APIs inside Durust workflow code",
      recommended: true
    },
    schema: [],
    messages: {
      hiddenIo: "{{message}}",
      nativeAsync: "{{message}}"
    }
  },
  create(context) {
    function report(node: EslintNode, violation: WorkflowDeterminismViolation | null): void {
      if (violation === null) {
        return;
      }
      context.report({
        node,
        messageId: violation.messageId,
        data: { message: violation.message }
      });
    }

    function reportModule(node: EslintNode): void {
      const moduleName = sourceString(node);
      if (moduleName !== null) {
        report(node, checkModuleSpecifier(moduleName));
      }
    }

    return {
      ImportDeclaration: reportModule,
      ExportNamedDeclaration: reportModule,
      ExportAllDeclaration: reportModule,
      CallExpression(node) {
        const callee = asNode(node.callee);
        if (callee === null) {
          return;
        }
        if (callee.type === "Import") {
          const moduleName = firstStringArgument(node);
          if (moduleName !== null) {
            report(node, checkModuleSpecifier(moduleName));
          }
          return;
        }
        if (callee.type === "Identifier") {
          const name = identifierName(callee);
          if (name === "require") {
            const moduleName = firstStringArgument(node);
            if (moduleName !== null) {
              report(node, checkModuleSpecifier(moduleName));
            }
          }
          report(node, checkIdentifierCall(name));
          return;
        }
        if (callee.type === "MemberExpression") {
          report(node, checkStaticCall(memberExpressionName(callee)));
        }
      },
      ImportExpression(node) {
        const moduleName = sourceString(node);
        if (moduleName !== null) {
          report(node, checkModuleSpecifier(moduleName));
        }
      },
      NewExpression(node) {
        const callee = asNode(node.callee);
        if (callee?.type === "Identifier") {
          report(node, checkConstructor(identifierName(callee)));
        }
      }
    };
  }
};

const plugin = {
  meta: {
    name: "@durust/eslint-plugin",
    version: "0.0.0"
  },
  rules: {
    [ruleName]: noWorkflowNondeterminismRule
  },
  configs: {} as EslintPlugin["configs"]
} as EslintPlugin;

plugin.configs.recommended = {
  plugins: {
    durust: plugin
  },
  rules: {
    "durust/no-workflow-nondeterminism": "error"
  }
};

export const rules = plugin.rules;
export const configs = plugin.configs;

export default plugin;

function sourceString(node: EslintNode): string | null {
  const source = asNode(node.source);
  return source === null ? null : literalString(source);
}

function firstStringArgument(node: EslintNode): string | null {
  const args = Array.isArray(node.arguments) ? node.arguments : [];
  const first = asNode(args[0]);
  return first === null ? null : literalString(first);
}

function memberExpressionName(node: EslintNode): string {
  const object = asNode(node.object);
  const property = asNode(node.property);
  if (object?.type !== "Identifier" || property?.type !== "Identifier") {
    return "";
  }
  return `${identifierName(object)}.${identifierName(property)}`;
}

function identifierName(node: EslintNode): string {
  return typeof node.name === "string" ? node.name : "";
}

function literalString(node: EslintNode): string | null {
  if (typeof node.value === "string") {
    return node.value;
  }
  return null;
}

function asNode(value: unknown): EslintNode | null {
  return value !== null && typeof value === "object" && "type" in value
    ? (value as EslintNode)
    : null;
}
