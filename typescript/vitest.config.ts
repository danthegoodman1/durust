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
// `packages/*/test/**/*.ts`, and it catches the false probe above. It reported
// 85 errors when it was introduced; those are now at **zero**, and it runs in
// `npm run check` via `check:test-types`, so the test suite is type-checked on
// every CI run — by that script rather than from here.
//
// It is a script rather than a bare `tsc -p` because `tsc` exits 0 when it has
// nothing to check: one bad `include` glob would turn the gate green and silent
// in the same edit, which is the defect this block exists to remember.
// `scripts/check-test-types.mjs` therefore also asserts that every test file on
// disk was in the set `tsc` read, that each expected package still has tests,
// that no file carries `@ts-nocheck`, and that the strictness options have not
// been weakened.
//
// So there is still no reason to re-add `typecheck` here. If you ever do, point
// it at `tsconfig.tests.json` and nothing else; pointing it back at
// `tsconfig.json` restores the green line for work nothing does.
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
