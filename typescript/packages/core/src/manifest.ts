import type { ActivityDefinition, WorkflowDefinition } from "./api.js";

export interface DurableManifest {
  readonly manifestVersion: 1;
  readonly runtime: "durust-typescript";
  readonly workflows: readonly WorkflowManifestEntry[];
  readonly activities: readonly ActivityManifestEntry[];
}

export interface WorkflowManifestEntry {
  readonly name: string;
  readonly version: number;
  readonly sourcePath: string | null;
  readonly inputSchemaFingerprint: string | null;
  readonly outputSchemaFingerprint: string | null;
  readonly queryStateSchemaFingerprint: string | null;
}

export interface ActivityManifestEntry {
  readonly name: string;
  readonly sourcePath: string | null;
  readonly inputSchemaFingerprint: string | null;
  readonly outputSchemaFingerprint: string | null;
}

export function exportManifest(
  workflows: Iterable<WorkflowDefinition<any, any, any, string>>,
  activities: Iterable<ActivityDefinition<any, any, string>>
): DurableManifest {
  return {
    manifestVersion: 1,
    runtime: "durust-typescript",
    workflows: [...workflows].map(workflowManifestEntry).sort(compareWorkflowEntries),
    activities: [...activities].map(activityManifestEntry).sort(compareActivityEntries)
  };
}

function workflowManifestEntry(
  definition: WorkflowDefinition<any, any, any, string>
): WorkflowManifestEntry {
  return {
    name: definition.name,
    version: definition.version,
    sourcePath: definition.sourcePath ?? null,
    inputSchemaFingerprint: definition.inputSchema?.fingerprint ?? null,
    outputSchemaFingerprint: definition.outputSchema?.fingerprint ?? null,
    queryStateSchemaFingerprint: definition.queryStateSchema?.fingerprint ?? null
  };
}

function activityManifestEntry(
  definition: ActivityDefinition<any, any, string>
): ActivityManifestEntry {
  return {
    name: definition.name,
    sourcePath: definition.sourcePath ?? null,
    inputSchemaFingerprint: definition.inputSchema?.fingerprint ?? null,
    outputSchemaFingerprint: definition.outputSchema?.fingerprint ?? null
  };
}

function compareWorkflowEntries(left: WorkflowManifestEntry, right: WorkflowManifestEntry): number {
  return left.name.localeCompare(right.name) || left.version - right.version;
}

function compareActivityEntries(left: ActivityManifestEntry, right: ActivityManifestEntry): number {
  return left.name.localeCompare(right.name);
}
