import { describe, expect, it } from "vitest";
import { Registry, activity, workflow } from "@durust/core";
import type { SchemaAdapter } from "@durust/core";

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
  });
});
