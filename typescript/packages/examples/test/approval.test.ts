import { describe, expect, it } from "vitest";
import { runMemoryApprovalExample } from "../src/approval.js";

describe("approval example", () => {
  it("uses signal/select/timer waits and query projection against the memory backend", async () => {
    await expect(runMemoryApprovalExample()).resolves.toEqual({
      waiting: {
        orderId: "order-1",
        status: "waiting"
      },
      completed: {
        orderId: "order-1",
        status: "approved",
        reviewer: "ops"
      },
      output: {
        orderId: "order-1",
        status: "approved",
        approvalId: "approval-1"
      }
    });
  });
});
