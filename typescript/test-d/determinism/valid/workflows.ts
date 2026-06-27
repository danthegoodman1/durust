import {
  callActivity,
  select,
  sideEffect,
  sleep,
  signal,
  workflow,
  activity
} from "@durust/core";

interface NoInput {}

interface QuoteInput {
  readonly sku: string;
}

interface Approval {
  readonly id: string;
}

const quoteActivity = activity({
  name: "lint.valid.quote",
  handler: async (input: QuoteInput): Promise<{ readonly cents: number }> => ({
    cents: input.sku.length
  })
});

const approvalSignal = signal<Approval>("approved");

export const validWorkflow = workflow({
  name: "lint.valid.workflow",
  version: 1,
  handler: async (_input: NoInput): Promise<{ readonly id: string; readonly cents: number }> => {
    const durableNap = sleep(1);
    await durableNap;
    const quote = await callActivity(quoteActivity, { sku: "sku-1" });
    const recorded = await sideEffect("stable-id", () => "recorded-id");
    const result = await select({
      approved: approvalSignal,
      timeout: sleep(10)
    });
    return {
      id: result.branch === "approved" ? result.value.id : recorded,
      cents: quote.cents
    };
  }
});
