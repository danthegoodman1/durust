import { describe, expect, expectTypeOf, it } from "vitest";
import {
  Client,
  RetryPolicy,
  activity,
  activityMap,
  callActivity,
  childWorkflow,
  childWorkflowMap,
  encodePayload,
  decodeActivityMapResults,
  join,
  joinAll,
  select,
  selectAll,
  signal,
  sleepUntil,
  workflow,
  type ActivityMapHandle,
  type ActivityMapInputManifest,
  type ChildWorkflowHandle,
  type ChildWorkflowMapHandle,
  type ChildWorkflowStart,
  type DurablePromise,
  type SchemaAdapter,
  type WorkflowHandle
} from "@durust/core";

interface QuoteInput {
  readonly sku: string;
}

interface QuoteOutput {
  readonly cents: number;
}

interface ShipInput {
  readonly orderId: string;
}

interface ShipOutput {
  readonly shipmentId: string;
}

interface CheckoutInput {
  readonly orderId: string;
}

interface CheckoutOutput {
  readonly ok: true;
}

type NoInput = {};

interface OrderView {
  readonly status: string;
}

const approvalSchema: SchemaAdapter<{ readonly approvalId: string }> = {
  fingerprint: "sha256:approval",
  rootKind: "object"
};

const priceQuote = activity({
  name: "payments.price-quote",
  handler: async (input: QuoteInput): Promise<QuoteOutput> => ({ cents: input.sku.length })
});

const shipOrder = workflow({
  name: "orders.ship",
  version: 1,
  handler: async (input: ShipInput): Promise<ShipOutput> => ({
    shipmentId: `shipment/${input.orderId}`
  })
});

const checkout = workflow({
  name: "orders.checkout",
  version: 1,
  queryStateType: {} as OrderView,
  handler: async (input: CheckoutInput): Promise<CheckoutOutput> => {
    void input;
    return { ok: true };
  }
});

const noInputWorkflow = workflow({
  name: "maintenance.no-input",
  version: 1,
  handler: async (input: NoInput): Promise<void> => {
    void input;
  }
});

describe("typed public API contract", () => {
  it("infers activity input and output at call sites", () => {
    const result = callActivity(priceQuote, { sku: "sku-1" }, {
      taskQueue: "payments",
      retry: RetryPolicy.exponential({ maxAttempts: 5 })
    });

    expectTypeOf(result).toEqualTypeOf<DurablePromise<QuoteOutput>>();
  });

  it("preserves child workflow output through spawn and result handles", () => {
    const start = childWorkflow(shipOrder, { orderId: "o-1" }, { workflowId: "ship/o-1" });

    expectTypeOf(start).toEqualTypeOf<ChildWorkflowStart<ShipOutput>>();
    expectTypeOf<Awaited<ReturnType<typeof start.spawn>>>().toEqualTypeOf<
      ChildWorkflowHandle<ShipOutput>
    >();
  });

  it("preserves workflow start and query result types", () => {
    const client = new Client();
    const handle = client.startWorkflow(checkout, "checkout/o-1", "orders", { orderId: "o-1" });
    const query = client.queryWorkflow(checkout, "checkout/o-1");
    void handle.catch(() => undefined);
    void query.catch(() => undefined);

    expectTypeOf(handle).toEqualTypeOf<Promise<WorkflowHandle<CheckoutOutput, OrderView>>>();
    expectTypeOf(query).toEqualTypeOf<Promise<OrderView>>();
  });

  it("accepts a named empty object for no-input workflows", () => {
    const client = new Client();
    const handle = client.startWorkflow(noInputWorkflow, "maintenance/no-input", "maintenance", {});
    void handle.catch(() => undefined);

    expectTypeOf(handle).toEqualTypeOf<Promise<WorkflowHandle<void, unknown>>>();
  });

  it("preserves signal payload types", () => {
    const approved = signal<{ approvalId: string }>("approved", { schema: approvalSchema });
    const client = new Client();

    const sent = client.sendSignal({
      workflowId: "checkout/o-1",
      signal: approved,
      payload: { approvalId: "a-1" }
    });
    void sent.catch(() => undefined);

    expectTypeOf(sent).toEqualTypeOf<Promise<void>>();
    expect(approved.payloadSchema).toBe(approvalSchema);
  });

  it("preserves named join result types", () => {
    const joined = join({
      quote: callActivity(priceQuote, { sku: "sku-1" }),
      approved: signal<{ approvalId: string }>("approved"),
      delay: sleepUntil(1)
    });

    expectTypeOf(joined).toEqualTypeOf<
      PromiseLike<{
        readonly quote: QuoteOutput;
        readonly approved: { approvalId: string };
        readonly delay: void;
      }>
    >();
  });

  it("preserves tuple joinAll result types", () => {
    const joined = joinAll([
      callActivity(priceQuote, { sku: "sku-1" }),
      signal<{ approvalId: string }>("approved"),
      sleepUntil(1)
    ] as const);

    expectTypeOf(joined).toEqualTypeOf<
      PromiseLike<readonly [QuoteOutput, { approvalId: string }, void]>
    >();
  });

  it("preserves named select winner types", () => {
    const selected = select({
      quote: callActivity(priceQuote, { sku: "sku-1" }),
      approved: signal<{ approvalId: string }>("approved"),
      delay: sleepUntil(1)
    });

    expectTypeOf(selected).toEqualTypeOf<
      PromiseLike<
        | { readonly branch: "quote"; readonly value: QuoteOutput }
        | { readonly branch: "approved"; readonly value: { approvalId: string } }
        | { readonly branch: "delay"; readonly value: void }
      >
    >();
  });

  it("preserves selectAll winner value types", () => {
    const selected = selectAll([
      callActivity(priceQuote, { sku: "sku-1" }),
      signal<{ approvalId: string }>("approved"),
      sleepUntil(1)
    ] as const);

    expectTypeOf(selected).toEqualTypeOf<
      PromiseLike<{
        readonly index: number;
        readonly value: QuoteOutput | { approvalId: string } | void;
      }>
    >();
  });

  it("preserves activity-map and child-workflow-map manifest types", () => {
    const activityManifest = encodePayload<ActivityMapInputManifest<QuoteInput>>({
      itemCount: 1,
      pageLengths: [1],
      pages: []
    });
    const activityMapped = activityMap(priceQuote, {
      inputManifest: activityManifest,
      resultManifest: "quotes",
      maxInFlight: 16
    });

    const childManifest = encodePayload<ActivityMapInputManifest<ShipInput>>({
      itemCount: 1,
      pageLengths: [1],
      pages: []
    });
    const childMapped = childWorkflowMap(shipOrder, {
      inputManifest: childManifest,
      resultManifest: "shipments",
      workflowIdPrefix: "ship",
      maxInFlight: 16
    });

    expectTypeOf(activityMapped).toEqualTypeOf<ActivityMapHandle<QuoteOutput>>();
    type DecodedActivityMapResults = ReturnType<typeof decodeActivityMapResults<QuoteOutput>>;
    expectTypeOf<DecodedActivityMapResults>().toEqualTypeOf<readonly QuoteOutput[]>();
    expectTypeOf(childMapped).toEqualTypeOf<ChildWorkflowMapHandle<ShipOutput>>();
  });

  it("keeps definition metadata inspectable without invoking runtime stubs", () => {
    expect(priceQuote.name).toBe("payments.price-quote");
    expect(shipOrder.workflowType).toEqual({ name: "orders.ship", version: 1 });
  });
});
