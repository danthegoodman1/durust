import { defineConfig } from "vitest/config";

// No `test.typecheck` block, and re-adding one the way it was written before
// is worse than having none. It read:
//
//     typecheck: { enabled: true, tsconfig: "tsconfig.json" }
//
// and printed `Type Errors  no errors` on every run while checking **zero**
// files, for two independent reasons, both measured:
//
//  1. `typecheck.include` defaults to `['**/*.{test,spec}-d.?(c|m)[jt]s?(x)']`
//     (vitest 4.1.9, `dist/chunks/defaults.*.js`) and this workspace contains
//     zero files matching it — the type-level suites are `*.test.ts`, not
//     `*.test-d.ts`.
//  2. `tsconfig.json` is the solution file: `files: []` plus `references`.
//     `tsc -p tsconfig.json --noEmit --listFiles` lists 0 files.
//
// Verified by appending `expectTypeOf<string>().toEqualTypeOf<number>()` — a
// flatly false assertion — to `packages/core/test/api-types.test.ts` and
// running `npm run test`: `13 passed`, `Type Errors  no errors`, exit 0. So
// the 15 `expectTypeOf` assertions in that file were unconditional passes, and
// no `*.test.ts` in the workspace was type-checked by anything: the package
// tsconfigs `include` only `src/**/*.ts`, `test:types` covers only `test-d/**`,
// and vitest transpiles tests with esbuild, which strips types without
// checking them.
//
// `tsconfig.tests.json` is the config that actually covers
// `packages/*/test/**/*.ts` (33 test files, 481 files total, 26.9k lines of
// test source). It reports 85 pre-existing errors and catches the false probe
// above, so it is a real check but not yet a green one — it is deliberately
// not wired into `npm run check` until those are triaged. Turn typecheck back
// on here only against that config, and only once it is at zero; pointing it
// at `tsconfig.json` again just restores the green line for work nothing does.
export default defineConfig({
  test: {
    include: ["packages/*/test/**/*.test.ts"]
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
