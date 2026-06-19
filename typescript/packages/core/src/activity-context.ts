import { AsyncLocalStorage } from "node:async_hooks";
import type {
  ActivityHeartbeatOutcome,
  ActivityHeartbeatRequest
} from "./backend.js";

interface ActivityExecutionContext {
  heartbeat(req: ActivityHeartbeatRequest): Promise<ActivityHeartbeatOutcome>;
  readonly heartbeatRequest: ActivityHeartbeatRequest;
}

const activityContext = new AsyncLocalStorage<ActivityExecutionContext>();

export function runWithActivityExecutionContext<T>(
  context: ActivityExecutionContext,
  execute: () => Promise<T>
): Promise<T> {
  return activityContext.run(context, execute);
}

export async function heartbeat(): Promise<ActivityHeartbeatOutcome> {
  const context = activityContext.getStore();
  if (context === undefined) {
    throw new Error("heartbeat can only be called from an activity handler");
  }
  return context.heartbeat(context.heartbeatRequest);
}
