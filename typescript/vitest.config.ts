import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["packages/*/test/**/*.test.ts"],
    typecheck: {
      enabled: true,
      tsconfig: "tsconfig.json"
    }
  },
  resolve: {
    alias: {
      "@durust/benchmark": new URL("./packages/benchmark/src/index.ts", import.meta.url).pathname,
      "@durust/core": new URL("./packages/core/src/index.ts", import.meta.url).pathname,
      "@durust/eslint-plugin": new URL("./packages/eslint-plugin/src/index.ts", import.meta.url).pathname,
      "@durust/payload": new URL("./packages/payload/src/index.ts", import.meta.url).pathname,
      "@durust/postgres": new URL("./packages/postgres/src/index.ts", import.meta.url).pathname,
      "@durust/sqlite": new URL("./packages/sqlite/src/index.ts", import.meta.url).pathname,
      "@durust/testing": new URL("./packages/testing/src/index.ts", import.meta.url).pathname
    }
  }
});
