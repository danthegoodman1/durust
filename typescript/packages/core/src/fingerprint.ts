import type { ActivityName, SignalName, TimestampMs, WorkflowId, WorkflowType, TaskQueue } from "./types.js";
import type { ChildWorkflowMapFailureMode, ParentClosePolicy } from "./options.js";

export type CommandKind =
  | "Activity"
  | "ActivityMap"
  | "ChildWorkflow"
  | "ChildWorkflowMap"
  | "Timer"
  | "Signal"
  | "VersionMarker";

export interface CommandFingerprint {
  readonly kind: CommandKind;
  readonly name: string;
  readonly inputDigest: string | null;
  readonly optionsDigest: string;
}

export function activityFingerprint(
  name: ActivityName | string,
  inputDigest: string,
  optionsDigest: string
): CommandFingerprint {
  return {
    kind: "Activity",
    name: activityNameValue(name),
    inputDigest,
    optionsDigest
  };
}

export function activityMapFingerprint(
  name: ActivityName | string,
  inputManifestDigest: string,
  resultManifestName: string,
  maxInFlight: number,
  optionsDigest: string
): CommandFingerprint {
  return {
    kind: "ActivityMap",
    name: activityNameValue(name),
    inputDigest: inputManifestDigest,
    optionsDigest: `${optionsDigest}:result=${resultManifestName}:max=${maxInFlight}`
  };
}

export function childWorkflowFingerprint(
  childType: WorkflowType,
  childWorkflowId: WorkflowId | string,
  inputDigest: string,
  queue: TaskQueue | string,
  parentClosePolicy: ParentClosePolicy
): CommandFingerprint {
  return {
    kind: "ChildWorkflow",
    name: `${childType.name}@${childType.version}`,
    inputDigest,
    optionsDigest: `workflow_id=${childWorkflowId}:task_queue=${queue}:parent_close_policy=${parentClosePolicy}`
  };
}

export function childWorkflowMapFingerprint(
  childType: WorkflowType,
  inputManifestDigest: string,
  resultManifestName: string,
  workflowIdPrefix: string,
  maxInFlight: number,
  queue: TaskQueue | string,
  parentClosePolicy: ParentClosePolicy,
  failureMode: ChildWorkflowMapFailureMode
): CommandFingerprint {
  return {
    kind: "ChildWorkflowMap",
    name: `${childType.name}@${childType.version}`,
    inputDigest: inputManifestDigest,
    optionsDigest: `result=${resultManifestName}:prefix=${workflowIdPrefix}:max=${maxInFlight}:task_queue=${queue}:parent_close_policy=${parentClosePolicy}:failure_mode=${failureMode}`
  };
}

export function timerFingerprint(kind: string, deadline: TimestampMs | number): CommandFingerprint {
  return {
    kind: "Timer",
    name: kind,
    inputDigest: null,
    optionsDigest: `timestamp-ms:${deadline}`
  };
}

export function signalFingerprint(name: SignalName | string): CommandFingerprint {
  return {
    kind: "Signal",
    name: signalNameValue(name),
    inputDigest: null,
    optionsDigest: "sha256:default"
  };
}

export function versionMarkerFingerprint(changeId: string, version: number): CommandFingerprint {
  return {
    kind: "VersionMarker",
    name: changeId,
    inputDigest: null,
    optionsDigest: `version:${version}`
  };
}

function activityNameValue(name: ActivityName | string): string {
  return name as string;
}

function signalNameValue(name: SignalName | string): string {
  return name as string;
}
