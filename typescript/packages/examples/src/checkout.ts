import {
  Client,
  MemoryBackend,
  Registry,
  Worker,
  activity,
  callActivity,
  childWorkflow,
  workflow
} from "@durust/core";

export interface CheckoutInput {
  readonly orderId: string;
  readonly sku: string;
  readonly quantity: number;
}

export interface CheckoutOutput {
  readonly orderId: string;
  readonly paymentId: string;
  readonly shipmentId: string;
}

export interface PriceQuoteInput {
  readonly sku: string;
  readonly quantity: number;
}

export interface PriceQuoteOutput {
  readonly amountCents: number;
}

export interface ChargeCardInput {
  readonly orderId: string;
  readonly amountCents: number;
}

export interface ChargeCardOutput {
  readonly paymentId: string;
}

export interface ShipOrderInput {
  readonly orderId: string;
  readonly paymentId: string;
}

export interface ShipOrderOutput {
  readonly shipmentId: string;
}

export const priceQuote = activity({
  name: "examples.price-quote",
  handler: async (input: PriceQuoteInput): Promise<PriceQuoteOutput> => ({
    amountCents: input.sku.length * input.quantity * 100
  })
});

export const chargeCard = activity({
  name: "examples.charge-card",
  handler: async (input: ChargeCardInput): Promise<ChargeCardOutput> => ({
    paymentId: `payment/${input.orderId}/${input.amountCents}`
  })
});

export const shipOrder = workflow({
  name: "examples.ship-order",
  version: 1,
  handler: async (input: ShipOrderInput): Promise<ShipOrderOutput> => ({
    shipmentId: `shipment/${input.orderId}/${input.paymentId}`
  })
});

export const checkout = workflow({
  name: "examples.checkout",
  version: 1,
  handler: async (input: CheckoutInput): Promise<CheckoutOutput> => {
    const quote = await callActivity(
      priceQuote,
      { sku: input.sku, quantity: input.quantity },
      { taskQueue: "activities" }
    );
    const payment = await callActivity(
      chargeCard,
      { orderId: input.orderId, amountCents: quote.amountCents },
      { taskQueue: "activities" }
    );
    const shipping = await childWorkflow(
      shipOrder,
      { orderId: input.orderId, paymentId: payment.paymentId },
      { workflowId: `ship/${input.orderId}`, taskQueue: "workflows" }
    ).spawn();
    const shipment = await shipping.result();
    return {
      orderId: input.orderId,
      paymentId: payment.paymentId,
      shipmentId: shipment.shipmentId
    };
  }
});

export async function runMemoryCheckoutExample(): Promise<CheckoutOutput> {
  const backend = new MemoryBackend();
  const registry = new Registry()
    .registerWorkflow(checkout)
    .registerWorkflow(shipOrder)
    .registerActivity(priceQuote)
    .registerActivity(chargeCard);
  const client = new Client(backend, { payloadCodec: "Json" });
  const worker = new Worker({
    backend,
    registry,
    workerId: "examples-worker",
    workflowTaskQueue: "workflows",
    activityTaskQueue: "activities",
    maxLocalActivitiesPerWorkflowTask: 4,
    payloadCodec: "Json"
  });

  const handle = await client.startWorkflow(
    checkout,
    "checkout/order-1",
    "workflows",
    { orderId: "order-1", sku: "sku-1", quantity: 2 }
  );

  for (let index = 0; index < 8; index += 1) {
    await worker.runWorkflowTaskOnce();
  }

  return await handle.result();
}
