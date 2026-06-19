import type { ActivityDefinition, WorkflowDefinition } from "./api.js";
import { exportManifest, type DurableManifest } from "./manifest.js";

export class Registry {
  readonly #workflows = new Map<string, WorkflowDefinition<any, any, any, string>>();
  readonly #activities = new Map<string, ActivityDefinition<any, any, string>>();

  registerWorkflow<W extends WorkflowDefinition<any, any, any, string>>(definition: W): this {
    const key = workflowRegistryKey(definition);
    if (this.#workflows.has(key)) {
      throw new Error(`workflow already registered: ${definition.name}@${definition.version}`);
    }
    this.#workflows.set(key, definition);
    return this;
  }

  registerActivity<A extends ActivityDefinition<any, any, string>>(definition: A): this {
    if (this.#activities.has(definition.name)) {
      throw new Error(`activity already registered: ${definition.name}`);
    }
    this.#activities.set(definition.name, definition);
    return this;
  }

  workflow<W extends WorkflowDefinition<any, any, any, string>>(
    name: W["name"],
    version: number
  ): WorkflowDefinition<any, any, any, string> | undefined {
    return this.#workflows.get(`${name}@${version}`);
  }

  activity<A extends ActivityDefinition<any, any, string>>(
    name: A["name"]
  ): ActivityDefinition<any, any, string> | undefined {
    return this.#activities.get(name);
  }

  workflows(): readonly WorkflowDefinition<any, any, any, string>[] {
    return [...this.#workflows.values()];
  }

  activities(): readonly ActivityDefinition<any, any, string>[] {
    return [...this.#activities.values()];
  }

  exportManifest(): DurableManifest {
    return exportManifest(this.#workflows.values(), this.#activities.values());
  }
}

export function workflowRegistryKey(
  definition: Pick<WorkflowDefinition<any, any, any, string>, "name" | "version">
): string {
  return `${definition.name}@${definition.version}`;
}
