import { describe, expect, it } from "vitest";
import { runMemoryCheckoutExample } from "../src/checkout.js";

describe("checkout example", () => {
  it("runs against the memory backend", async () => {
    await expect(runMemoryCheckoutExample()).resolves.toEqual({
      orderId: "order-1",
      paymentId: "payment/order-1/1000",
      shipmentId: "shipment/order-1/payment/order-1/1000"
    });
  });
});
