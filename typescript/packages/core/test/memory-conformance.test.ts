import { describe, it } from "vitest";
import { MemoryBackend } from "@durust/core";
import { basicProviderConformanceCases } from "@durust/testing";

describe("MemoryBackend basic provider conformance", () => {
  for (const conformanceCase of basicProviderConformanceCases()) {
    it(conformanceCase.name, async () => {
      await conformanceCase.run(() => new MemoryBackend());
    });
  }
});
