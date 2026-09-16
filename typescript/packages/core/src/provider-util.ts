/**
 * Small pure helpers over the contract's identifiers and history. The
 * durable providers live in Rust behind `@durust/native`; what remains here
 * is what the runtime, the worker, and the shared conformance cases read.
 */
import { eventId } from "./types.js";
import type { HistoryEvent } from "./history.js";
import type { CommandId, EventId, Namespace, WorkflowId } from "./types.js";

/** The workflow-state fields `tailEventId` reads. */
export interface WorkflowHistoryTail {
  readonly history: readonly HistoryEvent[];
}

export function workflowKey(namespace: Namespace | string, workflowId: WorkflowId | string): string {
  return `${namespace}/${workflowId}`;
}

export function commandKey(id: CommandId): string {
  return `${id.runId}:${id.seq}`;
}

/**
 * Command identity, compared exactly as `CommandId` declares it: a branded
 * `number` seq under a run id. `Number(left.seq) === Number(right.seq)` would
 * accept `null`, `""`, `false`, and `[]` as seq 0, and every caller acts on a
 * match by an irreversible write against the matched command.
 */
export function sameCommandId(left: CommandId, right: CommandId): boolean {
  return left.runId === right.runId && left.seq === right.seq;
}

export function tailEventId(state: WorkflowHistoryTail): EventId {
  return state.history.at(-1)?.eventId ?? eventId(0);
}
