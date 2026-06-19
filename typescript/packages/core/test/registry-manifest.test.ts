import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  Client,
  MemoryBackend,
  Registry,
  activity,
  activityMap,
  activityMapManifest,
  callActivity,
  childWorkflow,
  childWorkflowMap,
  decodeActivityMapResults,
  decodeChildWorkflowMapSuccesses,
  decodePayload,
  encodePayload,
  signal,
  workflow
} from "@durust/core";
import type { SchemaAdapter } from "@durust/core";
import { runManifestCli } from "../src/manifest-cli.js";

interface Input {
  readonly value: string;
}

interface Output {
  readonly ok: true;
}

const inputSchema: SchemaAdapter<Input> = {
  fingerprint: "sha256:input",
  rootKind: "object"
};

const outputSchema: SchemaAdapter<Output> = {
  fingerprint: "sha256:output"
};

describe("registry and manifest", () => {
  it("rejects duplicate workflow identity by name and version", () => {
    const first = workflow({
      name: "orders.checkout",
      version: 1,
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });
    const duplicate = workflow({
      name: "orders.checkout",
      version: 1,
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });

    const registry = new Registry().registerWorkflow(first);
    expect(() => registry.registerWorkflow(duplicate)).toThrow(
      "workflow already registered: orders.checkout@1"
    );
  });

  it("allows the same workflow name at different versions", () => {
    const v1 = workflow({
      name: "orders.checkout",
      version: 1,
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });
    const v2 = workflow({
      name: "orders.checkout",
      version: 2,
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });

    const registry = new Registry().registerWorkflow(v2).registerWorkflow(v1);
    expect(registry.workflows().map((definition) => definition.version).sort()).toEqual([1, 2]);
  });

  it("rejects duplicate activity identity by name", () => {
    const first = activity({
      name: "payments.quote",
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });
    const duplicate = activity({
      name: "payments.quote",
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });

    const registry = new Registry().registerActivity(first);
    expect(() => registry.registerActivity(duplicate)).toThrow(
      "activity already registered: payments.quote"
    );
  });

  it("exports a deterministic manifest with optional schema fingerprints", () => {
    const checkout = workflow({
      name: "orders.checkout",
      version: 1,
      inputSchema,
      outputSchema,
      queryStateSchema: { fingerprint: "sha256:query" } as SchemaAdapter<{ status: string }>,
      sourcePath: "src/workflows/checkout.ts",
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });
    const ship = workflow({
      name: "orders.ship",
      version: 1,
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });
    const quote = activity({
      name: "payments.quote",
      inputSchema,
      outputSchema,
      sourcePath: "src/activities/payments.ts",
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });

    const registry = new Registry()
      .registerWorkflow(ship)
      .registerActivity(quote)
      .registerWorkflow(checkout);

    expect(registry.exportManifest()).toEqual({
      manifestVersion: 1,
      runtime: "durust-typescript",
      workflows: [
        {
          name: "orders.checkout",
          version: 1,
          sourcePath: "src/workflows/checkout.ts",
          inputSchemaFingerprint: "sha256:input",
          outputSchemaFingerprint: "sha256:output",
          queryStateSchemaFingerprint: "sha256:query"
        },
        {
          name: "orders.ship",
          version: 1,
          sourcePath: null,
          inputSchemaFingerprint: null,
          outputSchemaFingerprint: null,
          queryStateSchemaFingerprint: null
        }
      ],
      activities: [
        {
          name: "payments.quote",
          sourcePath: "src/activities/payments.ts",
          inputSchemaFingerprint: "sha256:input",
          outputSchemaFingerprint: "sha256:output"
        }
      ]
    });
  });

  it("rejects input schemas whose declared root is not object", () => {
    expect(() =>
      activity({
        name: "bad.array-input",
        inputSchema: {
          fingerprint: "sha256:array",
          rootKind: "array"
        } as SchemaAdapter<Input>,
        handler: (input: Input): Output => {
          void input;
          return { ok: true };
        }
      })
    ).toThrow("activity bad.array-input input schema must describe an object root");

    expect(() =>
      workflow({
        name: "bad.primitive-input",
        version: 1,
        inputSchema: {
          fingerprint: "sha256:primitive",
          rootKind: "primitive"
        } as SchemaAdapter<Input>,
        handler: (input: Input): Output => {
          void input;
          return { ok: true };
        }
      })
    ).toThrow("workflow bad.primitive-input input schema must describe an object root");

    expect(() =>
      workflow({
        name: "bad.primitive-query-state",
        version: 1,
        queryStateSchema: {
          fingerprint: "sha256:query-primitive",
          rootKind: "primitive"
        } as SchemaAdapter<Input>,
        handler: (input: Input): Output => {
          void input;
          return { ok: true };
        }
      })
    ).toThrow("workflow bad.primitive-query-state query state schema must describe an object root");

    expect(() =>
      signal<Input>("bad.signal-array-payload", {
        schema: {
          fingerprint: "sha256:signal-array",
          rootKind: "array"
        } as SchemaAdapter<Input>
      })
    ).toThrow("signal bad.signal-array-payload payload schema must describe an object root");
  });

  it("rejects durable handlers without exactly one runtime input parameter", () => {
    expect(() =>
      activity({
        name: "bad.no-input-runtime",
        handler: (() => ({ ok: true })) as (input: Input) => Output
      })
    ).toThrow("activity bad.no-input-runtime handler must accept exactly one durable input object");

    expect(() =>
      activity({
        name: "bad.positional-runtime",
        handler: ((left: Input, right: Input) => {
          void left;
          void right;
          return { ok: true };
        }) as (input: Input) => Output
      })
    ).toThrow("activity bad.positional-runtime handler must accept exactly one durable input object");

    expect(() =>
      workflow({
        name: "bad.workflow-no-input-runtime",
        version: 1,
        handler: (() => ({ ok: true })) as (input: Input) => Output
      })
    ).toThrow(
      "workflow bad.workflow-no-input-runtime handler must accept exactly one durable input object"
    );

    expect(() =>
      workflow({
        name: "bad.workflow-positional-runtime",
        version: 1,
        handler: ((left: Input, right: Input) => {
          void left;
          void right;
          return { ok: true };
        }) as (input: Input) => Output
      })
    ).toThrow(
      "workflow bad.workflow-positional-runtime handler must accept exactly one durable input object"
    );
  });

  it("rejects non-object durable inputs at runtime public call sites", async () => {
    const quote = activity({
      name: "payments.runtime-quote",
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });
    const ship = workflow({
      name: "orders.runtime-ship",
      version: 1,
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });
    const client = new Client(new MemoryBackend());
    const approved = signal<Input>("runtime-approved");

    expect(() => callActivity(quote, "sku-1" as unknown as Input)).toThrow(
      "activity payments.runtime-quote input must be a durable input object"
    );
    expect(() => callActivity(quote, ["sku-1"] as unknown as Input)).toThrow(
      "activity payments.runtime-quote input must be a durable input object"
    );
    expect(() => childWorkflow(ship, null as unknown as Input, { workflowId: "ship/null" }))
      .toThrow("child workflow orders.runtime-ship input must be a durable input object");
    expect(() => childWorkflow(ship, (() => ({ value: "x" })) as unknown as Input, {
      workflowId: "ship/function"
    })).toThrow("child workflow orders.runtime-ship input must be a durable input object");

    await expect(
      client.startWorkflow(ship, "runtime/bad-start", "orders", 1 as unknown as Input)
    ).rejects.toThrow("workflow orders.runtime-ship input must be a durable input object");
    await expect(
      client.sendSignal({
        workflowId: "runtime/bad-signal",
        signal: approved,
        payload: false as unknown as Input
      })
    ).rejects.toThrow("signal runtime-approved payload must be a durable input object");

    expect(() => activityMapManifest<Input>([{ value: "ok" }, undefined as unknown as Input]))
      .toThrow("activityMapManifest item 1 must be a durable input object");
    expect(() =>
      activityMapManifest<Input>([{ value: "ok" }], {
        itemSchema: {
          fingerprint: "sha256:map-array",
          rootKind: "array"
        } as SchemaAdapter<Input>
      })
    ).toThrow("activityMapManifest item schema must describe an object root");
    expect(() => activityMapManifest<Input>([{ value: "ok" }])).not.toThrow();
  });

  it("encodes activity-map manifest items through optional item schemas", () => {
    const itemSchema: SchemaAdapter<Input> = {
      fingerprint: "sha256:map-item",
      rootKind: "object",
      encode: (value) => ({ wire_value: value.value }),
      decode: (value) => ({
        value: (value as { readonly wire_value: string }).wire_value
      })
    };

    const manifestRef = activityMapManifest<Input>([{ value: "one" }, { value: "two" }], {
      pageSize: 1,
      itemCodec: "Json",
      itemSchema
    });
    const manifest = decodePayload(manifestRef);
    const firstPageRef = manifest.pages[0];
    if (firstPageRef === undefined) {
      throw new Error("expected first manifest page");
    }
    const firstPage = decodePayload(firstPageRef);
    const firstItem = firstPage.items[0];
    if (firstItem === undefined) {
      throw new Error("expected first manifest item");
    }

    expect(manifestRef.codec).toBe("MessagePack");
    expect(manifest.pageLengths).toEqual([1, 1]);
    expect(firstItem.codec).toBe("Json");
    expect(firstItem.schemaFingerprint).toBe("sha256:map-item");
    expect(decodePayload<{ readonly wire_value: string }>(firstItem)).toEqual({
      wire_value: "one"
    });
    expect(decodePayload(firstItem, itemSchema)).toEqual({ value: "one" });
  });

  it("decodes map result manifests through optional result schemas", () => {
    const resultSchema: SchemaAdapter<Output> = {
      fingerprint: "sha256:map-result",
      encode: (value) => ({ wire_ok: value.ok }),
      decode: (value) => ({
        ok: (value as { readonly wire_ok: true }).wire_ok
      })
    };
    const resultRef = encodePayload<Output>({ ok: true }, {
      codec: "Json",
      schema: resultSchema
    });
    const activityPageRef = encodePayload({
      results: [resultRef]
    });
    const activityManifestRef = encodePayload({
      name: "activity-results",
      itemCount: 1,
      pageLengths: [1],
      pages: [activityPageRef]
    });
    const childPageRef = encodePayload({
      outcomes: [{ kind: "Succeeded" as const, result: resultRef }]
    });
    const childManifestRef = encodePayload({
      name: "child-results",
      itemCount: 1,
      pageLengths: [1],
      pages: [childPageRef]
    });

    expect(decodeActivityMapResults(activityManifestRef, resultSchema)).toEqual([{ ok: true }]);
    expect(decodeChildWorkflowMapSuccesses(childManifestRef, resultSchema)).toEqual([
      { ok: true }
    ]);
  });

  it("rejects invalid map scheduling options at public call sites", () => {
    const quote = activity({
      name: "payments.map-runtime-quote",
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });
    const child = workflow({
      name: "orders.map-runtime-child",
      version: 1,
      handler: (input: Input): Output => {
        void input;
        return { ok: true };
      }
    });
    const inputManifest = activityMapManifest<Input>([{ value: "ok" }]);

    expect(() =>
      activityMap(quote, {
        inputManifest,
        resultManifest: "",
        maxInFlight: 1
      })
    ).toThrow("activityMap resultManifest must not be empty");
    expect(() =>
      activityMap(quote, {
        inputManifest,
        resultManifest: "results",
        maxInFlight: 0
      })
    ).toThrow("activityMap maxInFlight must be a positive integer");
    expect(() =>
      activityMap(quote, {
        inputManifest,
        resultManifest: "results",
        maxInFlight: 1.5
      })
    ).toThrow("activityMap maxInFlight must be a positive integer");

    expect(() =>
      childWorkflowMap(child, {
        inputManifest,
        resultManifest: "results",
        workflowIdPrefix: " ",
        maxInFlight: 1
      })
    ).toThrow("childWorkflowMap workflowIdPrefix must not be empty");
    expect(() =>
      childWorkflowMap(child, {
        inputManifest,
        resultManifest: " ",
        workflowIdPrefix: "child-map",
        maxInFlight: 1
      })
    ).toThrow("childWorkflowMap resultManifest must not be empty");
    expect(() =>
      childWorkflowMap(child, {
        inputManifest,
        resultManifest: "results",
        workflowIdPrefix: "child-map",
        maxInFlight: Number.NaN
      })
    ).toThrow("childWorkflowMap maxInFlight must be a positive integer");
  });

  it("writes, checks, and diffs a durable manifest baseline from a module export", async () => {
    const temp = await mkdtemp(join(tmpdir(), "durust-manifest-cli-"));
    try {
      const modulePath = join(temp, "registry.mjs");
      const manifestPath = join(temp, "durable.manifest.json");
      await writeFile(
        modulePath,
        [
          "export const registry = {",
          "  exportManifest() {",
          "    return {",
          "      manifestVersion: 1,",
          "      runtime: 'durust-typescript',",
          "      workflows: [",
          "        {",
          "          name: 'orders.checkout',",
          "          version: 1,",
          "          sourcePath: 'src/workflows/checkout.ts',",
          "          inputSchemaFingerprint: 'sha256:input',",
          "          outputSchemaFingerprint: 'sha256:output',",
          "          queryStateSchemaFingerprint: null",
          "        }",
          "      ],",
          "      activities: [",
          "        {",
          "          name: 'payments.quote',",
          "          sourcePath: 'src/activities/payments.ts',",
          "          inputSchemaFingerprint: 'sha256:input',",
          "          outputSchemaFingerprint: 'sha256:output'",
          "        }",
          "      ]",
          "    };",
          "  }",
          "};",
          ""
        ].join("\n")
      );

      const writeStdout: string[] = [];
      await expect(
        runManifestCli(["write", "--module", modulePath, "--out", manifestPath], {
          stdout: (line) => writeStdout.push(line)
        })
      ).resolves.toBe(0);
      expect(writeStdout.join("\n")).toContain("wrote durable manifest");
      expect(JSON.parse(await readFile(manifestPath, "utf8"))).toEqual({
        manifestVersion: 1,
        runtime: "durust-typescript",
        workflows: [
          {
            name: "orders.checkout",
            version: 1,
            sourcePath: "src/workflows/checkout.ts",
            inputSchemaFingerprint: "sha256:input",
            outputSchemaFingerprint: "sha256:output",
            queryStateSchemaFingerprint: null
          }
        ],
        activities: [
          {
            name: "payments.quote",
            sourcePath: "src/activities/payments.ts",
            inputSchemaFingerprint: "sha256:input",
            outputSchemaFingerprint: "sha256:output"
          }
        ]
      });

      const checkStdout: string[] = [];
      await expect(
        runManifestCli(["check", "--module", modulePath, "--manifest", manifestPath], {
          stdout: (line) => checkStdout.push(line)
        })
      ).resolves.toBe(0);
      expect(checkStdout.join("\n")).toContain("durable manifest matches");

      await writeFile(
        manifestPath,
        JSON.stringify({
          manifestVersion: 1,
          runtime: "durust-typescript",
          workflows: [],
          activities: []
        })
      );
      const checkStderr: string[] = [];
      await expect(
        runManifestCli(["check", "--module", modulePath, "--manifest", manifestPath], {
          stderr: (line) => checkStderr.push(line)
        })
      ).resolves.toBe(1);
      expect(checkStderr.join("\n")).toContain("durable manifest differs");
      expect(checkStderr.join("\n")).toContain("orders.checkout");

      const diffStdout: string[] = [];
      await expect(
        runManifestCli(["diff", "--module", modulePath, "--manifest", manifestPath], {
          stdout: (line) => diffStdout.push(line)
        })
      ).resolves.toBe(1);
      expect(diffStdout.join("\n")).toContain("---");
      expect(diffStdout.join("\n")).toContain("+++ current");
      expect(diffStdout.join("\n")).toContain("orders.checkout");
    } finally {
      await rm(temp, { force: true, recursive: true });
    }
  });
});
