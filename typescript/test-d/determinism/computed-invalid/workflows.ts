import { workflow } from "@durust/core";

interface NoInput {}

export const computedInvalidWorkflow = workflow({
  name: "lint.invalid.computed.workflow",
  version: 1,
  handler: async (_input: NoInput): Promise<string> => {
    const timestamp = Date["now"]();
    const random = Math["random"]();
    const promiseAll = Promise["all"];
    const processEnv = process["env"];
    const hrtimeBigint = process["hrtime"]["bigint"];
    return `${timestamp}:${random}:${promiseAll}:${processEnv}:${hrtimeBigint}`;
  }
});
