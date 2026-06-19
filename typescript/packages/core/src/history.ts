import type { ActivityName, CommandId, EventId, RunId, SignalId, SignalName, TaskQueue, TimestampMs, WorkflowId, WorkflowType } from "./types.js";
import type { CommandFingerprint } from "./fingerprint.js";
import type { ChildWorkflowMapFailureMode, ParentClosePolicy, RetryPolicy } from "./options.js";
import type { DurableFailure } from "./api.js";
import type { PayloadRef } from "./payload.js";

export type HistoryEventType =
  | "WorkflowStarted"
  | "WorkflowCompleted"
  | "WorkflowFailed"
  | "WorkflowCancelled"
  | "WorkflowContinuedAsNew"
  | "WorkflowTaskStarted"
  | "ActivityScheduled"
  | "ActivityMapScheduled"
  | "ActivityMapCompleted"
  | "ActivityMapFailed"
  | "ActivityCompleted"
  | "ActivityFailed"
  | "ActivityTimedOut"
  | "ChildWorkflowStartRequested"
  | "ChildWorkflowStarted"
  | "ChildWorkflowCompleted"
  | "ChildWorkflowFailed"
  | "ChildWorkflowCancelled"
  | "ChildWorkflowMapScheduled"
  | "ChildWorkflowMapCompleted"
  | "ChildWorkflowMapFailed"
  | "TimerStarted"
  | "TimerFired"
  | "SignalConsumed"
  | "SelectWinner"
  | "VersionMarker"
  | "DeprecatedPatchMarker"
  | "SideEffectMarker";

export interface HistoryEvent {
  readonly eventId: EventId;
  readonly eventType: HistoryEventType;
  readonly data: HistoryEventData;
}

export type HistoryEventData =
  | { readonly kind: "WorkflowStarted"; readonly workflowType: WorkflowType; readonly input: PayloadRef }
  | { readonly kind: "WorkflowCompleted"; readonly result: PayloadRef }
  | { readonly kind: "WorkflowFailed"; readonly failure: DurableFailure }
  | { readonly kind: "WorkflowCancelled"; readonly reason: string }
  | { readonly kind: "WorkflowContinuedAsNew"; readonly input: PayloadRef }
  | { readonly kind: "WorkflowTaskStarted" }
  | { readonly kind: "ActivityScheduled"; readonly scheduled: ActivityScheduled }
  | { readonly kind: "ActivityMapScheduled"; readonly scheduled: ActivityMapScheduled }
  | { readonly kind: "ActivityMapCompleted"; readonly completed: ActivityMapCompleted }
  | { readonly kind: "ActivityMapFailed"; readonly failed: ActivityMapFailed }
  | { readonly kind: "ActivityCompleted"; readonly completed: ActivityCompleted }
  | { readonly kind: "ActivityFailed"; readonly failed: ActivityFailed }
  | { readonly kind: "ActivityTimedOut"; readonly timedOut: ActivityTimedOut }
  | { readonly kind: "ChildWorkflowStartRequested"; readonly requested: ChildWorkflowStartRequested }
  | { readonly kind: "ChildWorkflowStarted"; readonly started: ChildWorkflowStarted }
  | { readonly kind: "ChildWorkflowCompleted"; readonly completed: ChildWorkflowCompleted }
  | { readonly kind: "ChildWorkflowFailed"; readonly failed: ChildWorkflowFailed }
  | { readonly kind: "ChildWorkflowCancelled"; readonly cancelled: ChildWorkflowCancelled }
  | { readonly kind: "ChildWorkflowMapScheduled"; readonly scheduled: ChildWorkflowMapScheduled }
  | { readonly kind: "ChildWorkflowMapCompleted"; readonly completed: ChildWorkflowMapCompleted }
  | { readonly kind: "ChildWorkflowMapFailed"; readonly failed: ChildWorkflowMapFailed }
  | { readonly kind: "TimerStarted"; readonly started: TimerStarted }
  | { readonly kind: "TimerFired"; readonly fired: TimerFired }
  | { readonly kind: "SignalConsumed"; readonly consumed: SignalConsumed }
  | { readonly kind: "SelectWinner"; readonly winner: SelectWinner }
  | { readonly kind: "VersionMarker"; readonly marker: VersionMarker }
  | { readonly kind: "DeprecatedPatchMarker"; readonly marker: DeprecatedPatchMarker }
  | { readonly kind: "SideEffectMarker"; readonly marker: SideEffectMarker };

export interface ActivityScheduled {
  readonly commandId: CommandId;
  readonly activityName: ActivityName | string;
  readonly taskQueue: TaskQueue | string;
  readonly retryPolicy: RetryPolicy;
  readonly startToCloseTimeoutMs: number | null;
  readonly heartbeatTimeoutMs: number | null;
  readonly input: PayloadRef;
  readonly fingerprint: CommandFingerprint;
}

export interface ActivityTask {
  readonly activityId: string;
  readonly runId: RunId;
  readonly commandId: CommandId;
  readonly activityName: ActivityName | string;
  readonly taskQueue: TaskQueue | string;
  readonly retryPolicy: RetryPolicy;
  readonly startToCloseTimeoutMs: number | null;
  readonly heartbeatTimeoutMs: number | null;
  readonly attempt: number;
  readonly input: PayloadRef;
  readonly mapItem: ActivityMapItem | null;
}

export function activityTaskFromScheduled(scheduled: ActivityScheduled): ActivityTask {
  return {
    activityId: `${scheduled.commandId.runId}:${scheduled.commandId.seq}`,
    runId: scheduled.commandId.runId,
    commandId: scheduled.commandId,
    activityName: scheduled.activityName,
    taskQueue: scheduled.taskQueue,
    retryPolicy: scheduled.retryPolicy,
    startToCloseTimeoutMs: scheduled.startToCloseTimeoutMs,
    heartbeatTimeoutMs: scheduled.heartbeatTimeoutMs,
    attempt: 1,
    input: scheduled.input,
    mapItem: null
  };
}

export interface ActivityMapItem {
  readonly mapCommandId: CommandId;
  readonly itemOrdinal: number;
}

export interface ActivityCompleted {
  readonly commandId: CommandId;
  readonly result: PayloadRef;
}

export interface ActivityFailed {
  readonly commandId: CommandId;
  readonly failure: DurableFailure;
}

export interface ActivityTimedOut {
  readonly commandId: CommandId;
  readonly message: string;
}

export interface ActivityMapScheduled {
  readonly commandId: CommandId;
  readonly activityName: ActivityName | string;
  readonly taskQueue: TaskQueue | string;
  readonly retryPolicy: RetryPolicy;
  readonly startToCloseTimeoutMs: number | null;
  readonly heartbeatTimeoutMs: number | null;
  readonly inputManifest: PayloadRef;
  readonly resultManifestName: string;
  readonly maxInFlight: number;
  readonly fingerprint: CommandFingerprint;
}

export interface ActivityMapTask {
  readonly mapCommandId: CommandId;
  readonly activityName: ActivityName | string;
  readonly taskQueue: TaskQueue | string;
  readonly retryPolicy: RetryPolicy;
  readonly startToCloseTimeoutMs: number | null;
  readonly heartbeatTimeoutMs: number | null;
  readonly inputManifest: PayloadRef;
  readonly resultManifestName: string;
  readonly maxInFlight: number;
}

export interface ActivityMapCompleted {
  readonly commandId: CommandId;
  readonly resultManifest: PayloadRef;
  readonly itemCount: number;
  readonly successCount: number;
  readonly failureCount: number;
}

export interface ActivityMapFailed {
  readonly commandId: CommandId;
  readonly failure: DurableFailure;
}

export interface ChildWorkflowStartRequested {
  readonly commandId: CommandId;
  readonly workflowType: WorkflowType;
  readonly workflowId: WorkflowId | string;
  readonly taskQueue: TaskQueue | string;
  readonly input: PayloadRef;
  readonly parentClosePolicy: ParentClosePolicy;
  readonly fingerprint: CommandFingerprint;
}

export interface ChildWorkflowStarted {
  readonly commandId: CommandId;
  readonly workflowId: WorkflowId | string;
  readonly runId: RunId;
}

export interface ChildWorkflowCompleted {
  readonly commandId: CommandId;
  readonly result: PayloadRef;
}

export interface ChildWorkflowFailed {
  readonly commandId: CommandId;
  readonly failure: DurableFailure;
}

export interface ChildWorkflowCancelled {
  readonly commandId: CommandId;
  readonly reason: string;
}

export interface ChildWorkflowMapScheduled {
  readonly commandId: CommandId;
  readonly workflowType: WorkflowType;
  readonly taskQueue: TaskQueue | string;
  readonly inputManifest: PayloadRef;
  readonly resultManifestName: string;
  readonly workflowIdPrefix: string;
  readonly maxInFlight: number;
  readonly parentClosePolicy: ParentClosePolicy;
  readonly failureMode: ChildWorkflowMapFailureMode;
  readonly fingerprint: CommandFingerprint;
}

export interface ChildWorkflowMapItem {
  readonly mapCommandId: CommandId;
  readonly itemOrdinal: number;
}

export interface ChildWorkflowMapTask {
  readonly mapCommandId: CommandId;
  readonly workflowType: WorkflowType;
  readonly taskQueue: TaskQueue | string;
  readonly inputManifest: PayloadRef;
  readonly resultManifestName: string;
  readonly workflowIdPrefix: string;
  readonly maxInFlight: number;
  readonly parentClosePolicy: ParentClosePolicy;
  readonly failureMode: ChildWorkflowMapFailureMode;
}

export interface ChildWorkflowMapCompleted {
  readonly commandId: CommandId;
  readonly resultManifest: PayloadRef;
  readonly itemCount: number;
  readonly successCount: number;
  readonly failureCount: number;
  readonly cancellationCount: number;
}

export interface ChildWorkflowMapFailed {
  readonly commandId: CommandId;
  readonly failure: DurableFailure;
}

export interface TimerStarted {
  readonly commandId: CommandId;
  readonly fireAt: TimestampMs;
  readonly fingerprint: CommandFingerprint;
}

export interface TimerFired {
  readonly commandId: CommandId;
  readonly firedAt: TimestampMs;
}

export interface SignalConsumed {
  readonly commandId: CommandId;
  readonly signalId: SignalId | string;
  readonly signalName: SignalName | string;
  readonly payload: PayloadRef;
  readonly fingerprint: CommandFingerprint;
}

export interface SelectWinner {
  readonly selectCommandId: CommandId;
  readonly branchOrdinal: number;
  readonly winningEventId: EventId;
  readonly branchesDigest: string;
}

export interface VersionMarker {
  readonly commandId: CommandId;
  readonly changeId: string;
  readonly version: number;
}

export interface DeprecatedPatchMarker {
  readonly commandId: CommandId;
  readonly patchId: string;
}

export interface SideEffectMarker {
  readonly commandId: CommandId;
  readonly key: string;
  readonly value: PayloadRef;
}

export function historyEventType(data: HistoryEventData): HistoryEventType {
  return data.kind;
}

export function newHistoryEvent(eventId: EventId, data: HistoryEventData): HistoryEvent {
  return {
    eventId,
    eventType: historyEventType(data),
    data
  };
}
