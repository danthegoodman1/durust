// Pure helpers shared across the core runtime and worker so durable-input
// validation cannot drift between them. Not exported from the package index;
// these are internal to @durust/core.
//
// `commandKey` and `sameCommandId` used to live here on the same terms. They
// moved to `./provider-util.ts`, which *is* exported from the index, because
// the two SQL provider packages carried their own copies of both and
// `sameCommandId` had already drifted between them.

export function assertDurableInputValue(value: unknown, label: string): void {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be a durable input object`);
  }
}
