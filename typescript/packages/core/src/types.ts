export type MaybePromise<T> = T | PromiseLike<T>;

export type Brand<T, Name extends string> = T & { readonly __brand: Name };

export type DurableInputObject = object;

export type DurableInput<T> =
  T extends DurableInputObject
    ? T extends readonly unknown[]
      ? never
      : T extends (...args: readonly unknown[]) => unknown
        ? never
        : T
    : never;

export type DurableCallInput<
  Expected extends DurableInputObject,
  Actual extends Expected
> = DurableInput<Actual> extends never ? never : Actual;

export type DurableHandlerInput<Handler extends (...args: readonly any[]) => unknown> =
  Parameters<Handler> extends [infer Input] ? DurableInput<Input> : never;

export type OneDurableInputHandler<Handler extends (...args: readonly any[]) => unknown> =
  Parameters<Handler> extends [infer Input]
    ? DurableInput<Input> extends never
      ? never
      : Handler
    : never;

export type Namespace = Brand<string, "Namespace">;
export type WorkflowId = Brand<string, "WorkflowId">;
export type RunId = Brand<string, "RunId">;
export type WorkerId = Brand<string, "WorkerId">;
export type TaskQueue = Brand<string, "TaskQueue">;
export type ActivityName = Brand<string, "ActivityName">;
export type SignalName = Brand<string, "SignalName">;
export type SignalId = Brand<string, "SignalId">;
export type WaitId = Brand<string, "WaitId">;

export type EventId = Brand<number, "EventId">;
export type CommandSeq = Brand<number, "CommandSeq">;
export type TimestampMs = Brand<number, "TimestampMs">;

export interface WorkflowType {
  readonly name: string;
  readonly version: number;
}

export interface CommandId {
  readonly runId: RunId;
  readonly seq: CommandSeq;
}

export function namespace(value = "default"): Namespace {
  return value as Namespace;
}

export function workflowId(value: string): WorkflowId {
  return value as WorkflowId;
}

export function runId(value: string): RunId {
  return value as RunId;
}

export function workerId(value: string): WorkerId {
  return value as WorkerId;
}

export function taskQueue(value = "default"): TaskQueue {
  return value as TaskQueue;
}

export function activityName(value: string): ActivityName {
  return value as ActivityName;
}

export function signalName(value: string): SignalName {
  return value as SignalName;
}

export function signalId(value: string): SignalId {
  return value as SignalId;
}

export function waitId(value: string): WaitId {
  return value as WaitId;
}

export function eventId(value: number): EventId {
  return value as EventId;
}

export function commandSeq(value: number): CommandSeq {
  return value as CommandSeq;
}

export function timestampMs(value: number): TimestampMs {
  return value as TimestampMs;
}

export function commandId(runIdValue: RunId, seq: number): CommandId {
  return { runId: runIdValue, seq: commandSeq(seq) };
}

export function workflowType(name: string, version: number): WorkflowType {
  return { name, version };
}
