import { describe, expect, it } from "vitest";
import plugin, {
  checkActivityOnlyWorkflowApi,
  checkAwaitExpression,
  checkConstructor,
  checkIdentifierCall,
  checkIdentifierReference,
  checkImportBinding,
  checkModuleSpecifier,
  checkStaticCall,
  checkStaticRead,
  checkStaticReference,
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
    expect(checkModuleSpecifier("node:http2")?.message).toContain("network I/O");
    expect(checkModuleSpecifier("node:dgram")?.message).toContain("network I/O");
    expect(checkModuleSpecifier("node:readline/promises")?.message).toContain("terminal I/O");
    expect(checkModuleSpecifier("node:repl")?.message).toContain("dynamic evaluation");
    expect(checkModuleSpecifier("node:vm")?.message).toContain("dynamic code evaluation");
    expect(checkModuleSpecifier("node:inspector")?.message).toContain("debugger/process inspection");
    expect(checkModuleSpecifier("node:cluster")?.message).toContain("process clustering");
    expect(checkModuleSpecifier("node:os")?.message).toContain("host/process state");
    expect(checkModuleSpecifier("node:timers/promises")?.message).toContain("native timer APIs");
    expect(checkModuleSpecifier("node:perf_hooks")?.message).toContain("host timing APIs");
    expect(checkImportBinding("node:crypto", "randomBytes")?.message).toContain("node crypto random bytes");
    expect(checkImportBinding("node:crypto", "randomUUID")?.message).toContain("node crypto random UUIDs");
    expect(checkImportBinding("node:crypto", "*")?.message).toContain("namespace import");
    expect(checkImportBinding("node:crypto", "createHash")).toBeNull();
    expect(checkIdentifierCall("fetch")?.message).toContain("network I/O");
    expect(checkIdentifierCall("Date")?.message).toContain("workflow time");
    expect(checkIdentifierCall("eval")?.message).toContain("dynamic code evaluation");
    expect(checkIdentifierCall("Function")?.message).toContain("dynamic code generation");
    expect(checkIdentifierReference("eval")?.message).toContain("dynamic code evaluation");
    expect(checkIdentifierReference("Function")?.message).toContain("dynamic code generation");
    expect(checkIdentifierCall("setImmediate")?.message).toContain("durust durable operations");
    expect(checkStaticCall("AbortSignal.timeout")?.message).toContain("sleep()");
    expect(checkStaticCall("Atomics.wait")?.message).toContain("native blocking waits");
    expect(checkStaticCall("Atomics.waitAsync")?.message).toContain("native async waits");
    expect(checkStaticCall("caches.open")?.message).toContain("browser cache storage");
    expect(checkStaticCall("console.log")?.message).toContain("replay-visible side effect");
    expect(checkStaticCall("console.time")?.message).toContain("console timing");
    expect(checkStaticCall("document.querySelector")?.message).toContain("DOM reads");
    expect(checkStaticCall("history.pushState")?.message).toContain("browser history mutation");
    expect(checkStaticCall("indexedDB.open")?.message).toContain("browser database access");
    expect(checkStaticCall("localStorage.getItem")?.message).toContain("browser storage");
    expect(checkStaticCall("navigator.clipboard.readText")?.message).toContain("clipboard access");
    expect(checkStaticCall("navigator.geolocation.getCurrentPosition")?.message).toContain("geolocation");
    expect(checkStaticCall("navigator.locks.request")?.message).toContain("native lock scheduling");
    expect(checkStaticCall("navigator.sendBeacon")?.message).toContain("network I/O");
    expect(checkStaticCall("navigator.serviceWorker.register")?.message).toContain("service worker");
    expect(checkStaticCall("Promise.race")?.message).toContain("select()");
    expect(checkStaticCall("performance.now")?.message).toContain("workflow time");
    expect(checkStaticCall("location.reload")?.message).toContain("browser navigation mutation");
    expect(checkStaticCall("sessionStorage.setItem")?.message).toContain("browser storage mutation");
    expect(checkStaticCall("crypto.randomUUID")?.message).toContain("sideEffect()");
    expect(checkStaticCall("process.cpuUsage")?.message).toContain("process runtime usage");
    expect(checkStaticCall("process.cwd")?.message).toContain("working directory");
    expect(checkStaticCall("process.abort")?.message).toContain("process termination");
    expect(checkStaticCall("process.chdir")?.message).toContain("working directory changes");
    expect(checkStaticCall("process.hrtime.bigint")?.message).toContain("workflow time");
    expect(checkStaticCall("process.emitWarning")?.message).toContain("process warnings");
    expect(checkStaticCall("process.exit")?.message).toContain("process termination");
    expect(checkStaticCall("process.kill")?.message).toContain("process signalling");
    expect(checkStaticCall("process.memoryUsage")?.message).toContain("process runtime usage");
    expect(checkStaticCall("process.memoryUsage.rss")?.message).toContain("process runtime usage");
    expect(checkStaticCall("process.nextTick")?.message).toContain("durust durable operations");
    expect(checkStaticCall("process.report.writeReport")?.message).toContain("hidden I/O");
    expect(checkStaticCall("process.resourceUsage")?.message).toContain("process runtime usage");
    expect(checkStaticCall("process.stderr.write")?.message).toContain("stderr writes");
    expect(checkStaticCall("process.stdin.read")?.message).toContain("stdin reads");
    expect(checkStaticCall("process.stdout.write")?.message).toContain("stdout writes");
    expect(checkStaticCall("process.uptime")?.message).toContain("runtime uptime");
    expect(checkStaticCall("WebAssembly.compile")?.message).toContain("WebAssembly native code compilation");
    expect(checkStaticCall("WebAssembly.instantiate")?.message).toContain("WebAssembly native code execution");
    expect(checkStaticCall("WebAssembly.compileStreaming")?.message).toContain("hidden host I/O");
    expect(checkStaticCall("WebAssembly.instantiateStreaming")?.message).toContain("hidden host I/O");
    expect(checkStaticCall("WebAssembly.validate")?.message).toContain("WebAssembly native code validation");
    expect(checkStaticReference("Date.now")?.message).toContain("workflow time");
    expect(checkStaticRead("process.pid")?.message).toContain("process identity reads");
    expect(checkStaticRead("process.argv")?.message).toContain("process argument reads");
    expect(checkStaticRead("process.env")).toMatchObject({
      code: "durust/no-hidden-io",
      messageId: "hiddenIo"
    });
    expect(checkStaticRead("process.exitCode")?.message).toContain("process exit state");
    expect(checkStaticRead("process.stdout")?.message).toContain("process stdout");
    expect(checkStaticRead("document.cookie")?.message).toContain("browser cookie reads");
    expect(checkStaticRead("location.href")?.message).toContain("browser location reads");
    expect(checkStaticRead("navigator.userAgent")?.message).toContain("browser user-agent reads");
    expect(checkStaticRead("navigator.onLine")?.message).toContain("browser connectivity reads");
    expect(checkStaticRead("performance.timeOrigin")?.message).toContain("host timing origin");
    expect(checkStaticRead("window.localStorage")?.message).toContain("browser storage reads");
    expect(checkStaticReference("process.env")?.message).toContain("environment variable reads");
    expect(checkStaticReference("console.log")?.message).toContain("replay-visible side effect");
    expect(checkStaticReference("process.exit")?.message).toContain("process termination");
    expect(checkStaticReference("AbortSignal.timeout")?.message).toContain("sleep()");
    expect(checkStaticReference("crypto.getRandomValues")?.message).toContain("sideEffect()");
    expect(checkActivityOnlyWorkflowApi("heartbeat")).toMatchObject({
      code: "durust/no-activity-api-in-workflow",
      messageId: "activityApi"
    });
    expect(checkActivityOnlyWorkflowApi("callActivity")).toBeNull();
    expect(checkConstructor("WebSocket")?.message).toContain("network I/O");
    expect(checkConstructor("Function")?.message).toContain("dynamic code generation");
    expect(checkConstructor("AsyncFunction")?.message).toContain("dynamic async code generation");
    expect(checkConstructor("GeneratorFunction")?.message).toContain("dynamic generator code generation");
    expect(checkConstructor("AsyncGeneratorFunction")?.message).toContain("dynamic async generator code generation");
    expect(checkConstructor("Worker")?.message).toContain("native workers");
    expect(checkConstructor("SharedWorker")?.message).toContain("native workers");
    expect(checkConstructor("MessageChannel")?.message).toContain("native message channels");
    expect(checkConstructor("BroadcastChannel")?.message).toContain("native broadcast channels");
    expect(checkConstructor("WebAssembly.Module")?.message).toContain("WebAssembly native code compilation");
    expect(checkConstructor("WebAssembly.Instance")?.message).toContain("WebAssembly native execution");
    expect(checkConstructor("Date")?.message).toContain("workflow time");
    expect(checkConstructor("Date", { argumentCount: 1 })).toBeNull();
    expect(checkAwaitExpression({ kind: "call", name: "callActivity" })).toBeNull();
    expect(checkAwaitExpression({ kind: "memberCall", name: "result" })).toBeNull();
    expect(checkAwaitExpression({ kind: "identifier", name: "nativePromise" })).toMatchObject({
      code: "durust/no-unknown-await",
      messageId: "unknownAwait"
    });
    expect(checkModuleSpecifier("@durust/core")).toBeNull();
  });

  it("reports dynamic code and WebAssembly execution paths", () => {
    const reports: unknown[] = [];
    const listeners = noWorkflowNondeterminismRule.create({
      report(report) {
        reports.push(report);
      }
    });

    listeners.ImportDeclaration({
      type: "ImportDeclaration",
      source: { type: "Literal", value: "node:vm" }
    });
    listeners.CallExpression({
      type: "CallExpression",
      callee: { type: "Identifier", name: "eval" },
      arguments: []
    });
    listeners.VariableDeclarator({
      type: "VariableDeclarator",
      id: { type: "Identifier", name: "unsafeEval" },
      init: { type: "Identifier", name: "eval" }
    });
    listeners.CallExpression({
      type: "CallExpression",
      callee: { type: "Identifier", name: "Function" },
      arguments: []
    });
    listeners.NewExpression({
      type: "NewExpression",
      callee: { type: "Identifier", name: "Function" },
      arguments: []
    });
    listeners.CallExpression({
      type: "CallExpression",
      callee: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "WebAssembly" },
        property: { type: "Identifier", name: "instantiate" }
      },
      arguments: []
    });
    listeners.VariableDeclarator({
      type: "VariableDeclarator",
      id: { type: "Identifier", name: "wasmCompile" },
      init: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "WebAssembly" },
        property: { type: "Identifier", name: "compile" }
      }
    });
    listeners.NewExpression({
      type: "NewExpression",
      callee: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "WebAssembly" },
        property: { type: "Identifier", name: "Module" }
      },
      arguments: []
    });

    expect(reports).toHaveLength(8);
    expect(reports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          messageId: "hiddenIo",
          data: expect.objectContaining({ message: expect.stringContaining("node:vm") })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("eval()") })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("eval reference") })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("Function()") })
        }),
        expect.objectContaining({
          messageId: "hiddenIo",
          data: expect.objectContaining({ message: expect.stringContaining("Function is not allowed") })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("WebAssembly.instantiate()") })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("WebAssembly.compile reference") })
        }),
        expect.objectContaining({
          messageId: "hiddenIo",
          data: expect.objectContaining({ message: expect.stringContaining("WebAssembly.Module is not allowed") })
        })
      ])
    );
  });

  it("reports imported Node crypto random APIs without banning deterministic named imports", () => {
    const reports: unknown[] = [];
    const listeners = noWorkflowNondeterminismRule.create({
      report(report) {
        reports.push(report);
      }
    });

    listeners.ImportDeclaration({
      type: "ImportDeclaration",
      source: { type: "Literal", value: "node:crypto" },
      specifiers: [
        {
          type: "ImportSpecifier",
          imported: { type: "Identifier", name: "randomBytes" },
          local: { type: "Identifier", name: "nodeRandomBytes" }
        },
        {
          type: "ImportSpecifier",
          imported: { type: "Identifier", name: "createHash" },
          local: { type: "Identifier", name: "createHash" }
        }
      ]
    });
    listeners.ImportDeclaration({
      type: "ImportDeclaration",
      source: { type: "Literal", value: "node:crypto" },
      specifiers: [
        {
          type: "ImportNamespaceSpecifier",
          local: { type: "Identifier", name: "nodeCrypto" }
        }
      ]
    });

    expect(reports).toHaveLength(2);
    expect(reports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({
            message: expect.stringContaining("node:crypto.randomBytes import")
          })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({
            message: expect.stringContaining("node:crypto namespace import")
          })
        })
      ])
    );
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
    listeners.CallExpression({
      type: "CallExpression",
      callee: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "process" },
        property: { type: "Identifier", name: "cwd" }
      },
      arguments: []
    });
    listeners.MemberExpression({
      type: "MemberExpression",
      object: { type: "Identifier", name: "process" },
      property: { type: "Identifier", name: "pid" }
    });
    listeners.NewExpression({
      type: "NewExpression",
      callee: { type: "Identifier", name: "WebSocket" }
    });
    listeners.NewExpression({
      type: "NewExpression",
      callee: { type: "Identifier", name: "Date" },
      arguments: []
    });
    listeners.AwaitExpression({
      type: "AwaitExpression",
      argument: {
        type: "CallExpression",
        callee: { type: "Identifier", name: "readFromCache" }
      }
    });

    expect(reports).toHaveLength(7);
    expect(reports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ messageId: "hiddenIo" }),
        expect.objectContaining({ messageId: "nativeAsync" }),
        expect.objectContaining({ messageId: "unknownAwait" })
      ])
    );
  });

  it("reports aliases of forbidden static APIs", () => {
    const reports: unknown[] = [];
    const listeners = noWorkflowNondeterminismRule.create({
      report(report) {
        reports.push(report);
      }
    });

    listeners.VariableDeclarator({
      type: "VariableDeclarator",
      id: { type: "Identifier", name: "now" },
      init: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "Date" },
        property: { type: "Identifier", name: "now" }
      }
    });
    listeners.VariableDeclarator({
      type: "VariableDeclarator",
      id: {
        type: "ObjectPattern",
        properties: [
          {
            type: "Property",
            key: { type: "Identifier", name: "random" },
            value: { type: "Identifier", name: "unsafeRandom" }
          }
        ]
      },
      init: { type: "Identifier", name: "Math" }
    });
    listeners.VariableDeclarator({
      type: "VariableDeclarator",
      id: {
        type: "ObjectPattern",
        properties: [
          {
            type: "Property",
            key: { type: "Identifier", name: "all" },
            value: { type: "Identifier", name: "promiseAll" }
          }
        ]
      },
      init: { type: "Identifier", name: "Promise" }
    });
    listeners.VariableDeclarator({
      type: "VariableDeclarator",
      id: { type: "Identifier", name: "hrtimeBigint" },
      init: {
        type: "MemberExpression",
        object: {
          type: "MemberExpression",
          object: { type: "Identifier", name: "process" },
          property: { type: "Identifier", name: "hrtime" }
        },
        property: { type: "Identifier", name: "bigint" }
      }
    });

    expect(reports).toHaveLength(4);
    expect(reports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("Date.now reference") })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("Math.random reference") })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("Promise.all reference") })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("process.hrtime.bigint reference") })
        })
      ])
    );
  });

  it("reports computed string member access like ordinary static access", () => {
    const reports: unknown[] = [];
    const listeners = noWorkflowNondeterminismRule.create({
      report(report) {
        reports.push(report);
      }
    });

    listeners.CallExpression({
      type: "CallExpression",
      callee: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "Math" },
        property: { type: "Literal", value: "random" },
        computed: true
      },
      arguments: []
    });
    listeners.VariableDeclarator({
      type: "VariableDeclarator",
      id: { type: "Identifier", name: "computedNow" },
      init: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "Date" },
        property: { type: "Literal", value: "now" },
        computed: true
      }
    });
    listeners.MemberExpression({
      type: "MemberExpression",
      object: { type: "Identifier", name: "process" },
      property: { type: "Literal", value: "env" },
      computed: true
    });
    listeners.VariableDeclarator({
      type: "VariableDeclarator",
      id: { type: "Identifier", name: "computedHrtimeBigint" },
      init: {
        type: "MemberExpression",
        object: {
          type: "MemberExpression",
          object: { type: "Identifier", name: "process" },
          property: { type: "Literal", value: "hrtime" },
          computed: true
        },
        property: { type: "Literal", value: "bigint" },
        computed: true
      }
    });

    expect(reports).toHaveLength(4);
    expect(reports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("Math.random()") })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("Date.now reference") })
        }),
        expect.objectContaining({
          messageId: "hiddenIo",
          data: expect.objectContaining({ message: expect.stringContaining("process.env read") })
        }),
        expect.objectContaining({
          messageId: "nativeAsync",
          data: expect.objectContaining({ message: expect.stringContaining("process.hrtime.bigint reference") })
        })
      ])
    );
  });

  it("reports activity-only Durust APIs in workflow source", () => {
    const reports: unknown[] = [];
    const listeners = noWorkflowNondeterminismRule.create({
      report(report) {
        reports.push(report);
      }
    });

    listeners.ImportDeclaration({
      type: "ImportDeclaration",
      source: { type: "Literal", value: "@durust/core" },
      specifiers: [
        {
          type: "ImportSpecifier",
          imported: { type: "Identifier", name: "heartbeat" },
          local: { type: "Identifier", name: "activityHeartbeat" }
        },
        {
          type: "ImportNamespaceSpecifier",
          local: { type: "Identifier", name: "durust" }
        }
      ]
    });
    listeners.CallExpression({
      type: "CallExpression",
      callee: { type: "Identifier", name: "activityHeartbeat" },
      arguments: []
    });
    listeners.CallExpression({
      type: "CallExpression",
      callee: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "durust" },
        property: { type: "Identifier", name: "heartbeat" }
      },
      arguments: []
    });

    expect(reports).toHaveLength(2);
    expect(reports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          messageId: "activityApi",
          data: expect.objectContaining({
            message: expect.stringContaining("activityHeartbeat() is activity-only")
          })
        }),
        expect.objectContaining({
          messageId: "activityApi",
          data: expect.objectContaining({
            message: expect.stringContaining("durust.heartbeat() is activity-only")
          })
        })
      ])
    );
  });

  it("reports process environment reads and aliases", () => {
    const reports: unknown[] = [];
    const listeners = noWorkflowNondeterminismRule.create({
      report(report) {
        reports.push(report);
      }
    });

    listeners.MemberExpression({
      type: "MemberExpression",
      object: { type: "Identifier", name: "process" },
      property: { type: "Identifier", name: "env" }
    });
    listeners.MemberExpression({
      type: "MemberExpression",
      object: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "globalThis" },
        property: { type: "Identifier", name: "process" }
      },
      property: { type: "Identifier", name: "env" }
    });
    listeners.VariableDeclarator({
      type: "VariableDeclarator",
      id: { type: "Identifier", name: "env" },
      init: {
        type: "MemberExpression",
        object: { type: "Identifier", name: "process" },
        property: { type: "Identifier", name: "env" }
      }
    });
    listeners.VariableDeclarator({
      type: "VariableDeclarator",
      id: {
        type: "ObjectPattern",
        properties: [
          {
            type: "Property",
            key: { type: "Identifier", name: "env" },
            value: { type: "Identifier", name: "processEnv" }
          }
        ]
      },
      init: { type: "Identifier", name: "process" }
    });

    expect(reports).toHaveLength(4);
    expect(reports).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          messageId: "hiddenIo",
          data: expect.objectContaining({ message: expect.stringContaining("process.env read") })
        }),
        expect.objectContaining({
          messageId: "hiddenIo",
          data: expect.objectContaining({
            message: expect.stringContaining("globalThis.process.env read")
          })
        })
      ])
    );
  });
});
