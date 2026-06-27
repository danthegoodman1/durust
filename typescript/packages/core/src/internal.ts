import type { CommandId } from "./types.js";

// Pure helpers shared across the core runtime, worker, and backend so command
// identity and durable-input validation cannot drift between them. Not exported
// from the package index; these are internal to @durust/core.

export function commandKey(id: CommandId): string {
  return `${id.runId}:${id.seq}`;
}

export function sameCommandId(left: CommandId, right: CommandId): boolean {
  return left.runId === right.runId && left.seq === right.seq;
}

export function assertDurableInputValue(value: unknown, label: string): void {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be a durable input object`);
  }
}
