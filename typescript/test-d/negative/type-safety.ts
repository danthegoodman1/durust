import {
  Client,
  activity,
  activityMap,
  activityMapManifest,
  childWorkflow,
  childWorkflowMap,
  callActivity,
  encodePayload,
  join,
  joinAll,
  select,
  selectAll,
  signal,
  workflow,
  type ActivityMapInputManifest
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

type NoInput = {};

interface OrderView {
  readonly status: string;
}

const priceQuote = activity({
  name: "payments.price-quote",
  handler: (input: QuoteInput): QuoteOutput => ({ cents: input.sku.length })
});

activity({
  name: "bad.primitive-input",
  // @ts-expect-error activity input must be a single object, not a primitive
  handler: (input: string): QuoteOutput => {
    void input;
    return { cents: 1 };
  }
});

activity({
  name: "bad.number-input",
  // @ts-expect-error activity input must be a single object, not a number
  handler: (input: number): QuoteOutput => {
    void input;
    return { cents: 1 };
  }
});

activity({
  name: "bad.boolean-input",
  // @ts-expect-error activity input must be a single object, not a boolean
  handler: (input: boolean): QuoteOutput => {
    void input;
    return { cents: 1 };
  }
});

activity({
  name: "bad.void-input",
  // @ts-expect-error activity input must be a named empty object, not void
  handler: (input: void): QuoteOutput => {
    void input;
    return { cents: 1 };
  }
});

activity({
  name: "bad.null-input",
  // @ts-expect-error activity input must be a named empty object, not null
  handler: (input: null): QuoteOutput => {
    void input;
    return { cents: 1 };
  }
});

activity({
  name: "bad.undefined-input",
  // @ts-expect-error activity input must be a named empty object, not undefined
  handler: (input: undefined): QuoteOutput => {
    void input;
    return { cents: 1 };
  }
});

activity({
  name: "bad.array-input",
  // @ts-expect-error activity input must be a single object, not an array
  handler: (input: readonly string[]): QuoteOutput => {
    void input;
    return { cents: 1 };
  }
});

activity({
  name: "bad.tuple-input",
  // @ts-expect-error activity input must be a single object, not a tuple
  handler: (input: readonly [string, number]): QuoteOutput => {
    void input;
    return { cents: 1 };
  }
});

activity({
  name: "bad.no-input",
  // @ts-expect-error no-input handlers must use a named empty object input
  handler: (): QuoteOutput => ({ cents: 1 })
});

activity({
  name: "bad.positional-inputs",
  // @ts-expect-error positional parameters are not allowed for durable handlers
  handler: (orderId: string, amountCents: number): QuoteOutput => {
    void orderId;
    void amountCents;
    return { cents: 1 };
  }
});

workflow({
  name: "bad.workflow-primitive-input",
  version: 1,
  // @ts-expect-error workflow input must be a single object, not a primitive
  handler: async (input: string): Promise<void> => {
    void input;
  }
});

workflow({
  name: "bad.workflow-no-input",
  version: 1,
  // @ts-expect-error no-input workflows must use a named empty object input
  handler: async (): Promise<void> => undefined
});

workflow({
  name: "bad.workflow-positional-inputs",
  version: 1,
  // @ts-expect-error workflow handlers cannot use positional parameters
  handler: async (orderId: string, amountCents: number): Promise<void> => {
    void orderId;
    void amountCents;
  }
});

const otherActivity = activity({
  name: "other",
  handler: (input: { readonly id: number }): { readonly ok: true } => {
    void input;
    return { ok: true };
  }
});

const noInputActivity = activity({
  name: "maintenance.no-input-activity",
  handler: (input: NoInput): void => {
    void input;
  }
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
  handler: async (input: { readonly orderId: string }): Promise<{ readonly ok: true }> => {
    void input;
    return { ok: true };
  }
});

const noInputWorkflow = workflow({
  name: "maintenance.no-input-workflow",
  version: 1,
  handler: async (input: NoInput): Promise<void> => {
    void input;
  }
});

callActivity(priceQuote, { sku: "sku-1" });

// @ts-expect-error activity input must match activity handler input
callActivity(priceQuote, { productId: "sku-1" });

// @ts-expect-error activity calls must pass one object-shaped input
callActivity(priceQuote, "sku-1");

// @ts-expect-error activity calls must not pass arrays
callActivity(priceQuote, ["sku-1"]);

callActivity(noInputActivity, {});

// @ts-expect-error no-input activity calls must still pass an object
callActivity(noInputActivity, "not-object");

// @ts-expect-error no-input activity calls must not pass arrays
callActivity(noInputActivity, []);

childWorkflow(shipOrder, { orderId: "o-1" }, { workflowId: "ship/o-1" });

// @ts-expect-error child workflow input must match workflow handler input
childWorkflow(shipOrder, { sku: "sku-1" }, { workflowId: "ship/o-1" });

// @ts-expect-error child workflow starts must pass one object-shaped input
childWorkflow(shipOrder, "o-1", { workflowId: "ship/o-1" });

childWorkflow(noInputWorkflow, {}, { workflowId: "maintenance/no-input-child" });

// @ts-expect-error no-input child workflow starts must still pass an object
childWorkflow(noInputWorkflow, 1, { workflowId: "maintenance/no-input-child" });

const client = new Client();
client.startWorkflow(checkout, "checkout/o-1", "orders", { orderId: "o-1" });

// @ts-expect-error workflow start input must match workflow handler input
client.startWorkflow(checkout, "checkout/o-1", "orders", { sku: "sku-1" });

// @ts-expect-error workflow starts must pass one object-shaped input
client.startWorkflow(checkout, "checkout/o-1", "orders", "o-1");

client.startWorkflow(noInputWorkflow, "maintenance/no-input", "maintenance", {});

// @ts-expect-error no-input workflow starts must still pass an object
client.startWorkflow(noInputWorkflow, "maintenance/no-input", "maintenance", null);

const approved = signal<{ readonly approvalId: string }>("approved");
const noSignalPayload = signal<NoInput>("maintenance.no-input-signal");

// @ts-expect-error signal payloads must be object-shaped
signal<string>("bad-signal");

// @ts-expect-error signal payloads must not be primitive numbers
signal<number>("bad-number-signal");

// @ts-expect-error signal payloads must not be primitive booleans
signal<boolean>("bad-boolean-signal");

// @ts-expect-error signal payloads must not be arrays
signal<readonly string[]>("bad-array-signal");

client.sendSignal({
  workflowId: "checkout/o-1",
  signal: approved,
  payload: { approvalId: "a-1" }
});

client.queryWorkflow(checkout, "checkout/o-1").then((view) => {
  const status: string = view.status;
  void status;
});

// @ts-expect-error signal payload must match signal definition
client.sendSignal({ workflowId: "checkout/o-1", signal: approved, payload: { id: "a-1" } });

client.sendSignal({
  workflowId: "maintenance/no-input",
  signal: noSignalPayload,
  payload: {}
});

client.sendSignal({
  workflowId: "maintenance/no-input",
  signal: noSignalPayload,
  // @ts-expect-error no-input signal payloads must still pass an object
  payload: true
});

// @ts-expect-error query state type is OrderView, not a numeric count view
const wrongQuery: PromiseLike<{ readonly count: number }> = client.queryWorkflow(
  checkout,
  "checkout/o-1"
);

const quoteManifest = encodePayload<ActivityMapInputManifest<QuoteInput>>({
  itemCount: 1,
  pageLengths: [1],
  pages: []
});

activityMap(priceQuote, {
  inputManifest: quoteManifest,
  resultManifest: "quotes",
  maxInFlight: 4
});

// @ts-expect-error activity map input manifests must contain object-shaped items
activityMapManifest(["sku-1"]);

// @ts-expect-error activity map input manifests must not contain primitive numbers
activityMapManifest([1]);

// @ts-expect-error activity map input manifests must not contain array items
activityMapManifest([["sku-1"]]);

activityMap(otherActivity, {
  // @ts-expect-error activity map manifest item type must match activity input
  inputManifest: quoteManifest,
  resultManifest: "bad",
  maxInFlight: 4
});

childWorkflowMap(shipOrder, {
  // @ts-expect-error child workflow map manifest item type must match child workflow input
  inputManifest: quoteManifest,
  resultManifest: "bad",
  workflowIdPrefix: "ship",
  maxInFlight: 4
});

join({
  quote: callActivity(priceQuote, { sku: "sku-1" }),
  // @ts-expect-error join accepts only durable branches, not native promises
  native: Promise.resolve({ ok: true })
});

select({
  quote: callActivity(priceQuote, { sku: "sku-1" }),
  // @ts-expect-error select accepts only durable branches, not native promises
  native: Promise.resolve({ ok: true })
});

joinAll([
  callActivity(priceQuote, { sku: "sku-1" }),
  // @ts-expect-error joinAll accepts only durable branches, not native promises
  Promise.resolve({ ok: true })
]);

selectAll([
  callActivity(priceQuote, { sku: "sku-1" }),
  // @ts-expect-error selectAll accepts only durable branches, not native promises
  Promise.resolve({ ok: true })
]);
