import { execFileSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import { nativeTarget } from "@durust/native";

// Isolate the installed-package path from the workspace's local addon fallback.
// Use real binaries and a fresh process so addon caching cannot mask eager loads.
const nativeRoot = fileURLToPath(new URL("..", import.meta.url));
const root = mkdtempSync(join(tmpdir(), "durust-native-loading-"));
const facade = join(root, "facade");
const platform = join(root, "node_modules", "@durust", `native-${nativeTarget()}`);
mkdirSync(join(facade, "dist"), { recursive: true });
mkdirSync(platform, { recursive: true });
copyFileSync(join(nativeRoot, "package.json"), join(facade, "package.json"));
copyFileSync(join(nativeRoot, "dist", "index.js"), join(facade, "dist", "index.js"));
copyFileSync(join(nativeRoot, "npm", nativeTarget(), "package.json"), join(platform, "package.json"));
symlinkSync(join(nativeRoot, "..", "core"), join(root, "node_modules", "@durust", "core"));
symlinkSync(join(nativeRoot, "..", "..", "node_modules", "@msgpack"), join(root, "node_modules", "@msgpack"));
for (const [binary, override] of [
  ["durust-node", process.env.DURUST_NATIVE_LIBRARY_PATH],
  ["durust-sqlite", process.env.DURUST_SQLITE_LIBRARY_PATH]
] as const) {
  const name = `${binary}.${nativeTarget()}.node`;
  copyFileSync(override ?? join(nativeRoot, name), join(platform, name));
}

afterAll(() => rmSync(root, { recursive: true, force: true }));

function run(code: string, overrides: NodeJS.ProcessEnv = {}): void {
  const env = { ...process.env };
  delete env.DURUST_NATIVE_LIBRARY_PATH;
  delete env.DURUST_SQLITE_LIBRARY_PATH;
  execFileSync(process.execPath, ["--input-type=module", "-e", `
    import assert from 'node:assert/strict';
    import { NativeBackend } from ${JSON.stringify(pathToFileURL(join(facade, "dist", "index.js")).href)};
    const databasePath = ${JSON.stringify(join(root, "loading.sqlite3"))};
    ${code}
  `], { env: { ...env, ...overrides }, stdio: "pipe" });
}

describe("native addon loading", () => {
  it("loads both packaged addons and keeps their handles and lifecycles independent", () => {
    run(`
      const memory = NativeBackend.memory({ nowMs: () => 11 });
      const sqlite = NativeBackend.sqlite(databasePath, { nowMs: () => 29 });
      assert.equal(await memory.currentTime(), 11);
      assert.equal(await sqlite.currentTime(), 29);
      sqlite.close();
      assert.equal(await memory.currentTime(), 11);
      const reopened = NativeBackend.sqlite(databasePath, { nowMs: () => 41 });
      memory.close();
      assert.equal(await reopened.currentTime(), 41);
      reopened.close();
    `);
  });

  it("loads memory/Postgres without touching SQLite and recovers from a failed SQLite load", () => {
    run(`
      const memory = NativeBackend.memory({ nowMs: () => 11 });
      assert.equal(await memory.currentTime(), 11);
      assert.throws(() => NativeBackend.sqlite(databasePath), error =>
        error.message.includes('system shared library') && error.cause.code === 'MODULE_NOT_FOUND');
      assert.equal(await memory.currentTime(), 11);
      if (process.env.DURUST_POSTGRES_URL) {
        const postgres = await NativeBackend.postgres(process.env.DURUST_POSTGRES_URL, {
          schema: 'durust_loading_' + process.pid,
          nowMs: () => 13
        });
        try { assert.equal(await postgres.currentTime(), 13); }
        finally { await postgres.destroy(); }
      }
      delete process.env.DURUST_SQLITE_LIBRARY_PATH;
      const sqlite = NativeBackend.sqlite(databasePath);
      sqlite.close();
      memory.close();
    `, { DURUST_SQLITE_LIBRARY_PATH: join(root, "missing-sqlite.node") });
  });

  it("loads SQLite independently of the memory/Postgres addon", () => {
    run(`
      const sqlite = NativeBackend.sqlite(databasePath, { nowMs: () => 29 });
      assert.equal(await sqlite.currentTime(), 29);
      assert.throws(() => NativeBackend.memory(), error => error.cause.code === 'MODULE_NOT_FOUND');
      sqlite.close();
    `, { DURUST_NATIVE_LIBRARY_PATH: join(root, "missing-default.node") });
  });

  it("honors explicit addon paths without requiring a supported prebuilt target", () => {
    run(`
      Object.defineProperty(process, 'arch', { value: 'custom-target' });
      const memory = NativeBackend.memory({ nowMs: () => 11 });
      const sqlite = NativeBackend.sqlite(databasePath, { nowMs: () => 29 });
      assert.equal(await memory.currentTime(), 11);
      assert.equal(await sqlite.currentTime(), 29);
      memory.close();
      sqlite.close();
    `, {
      DURUST_NATIVE_LIBRARY_PATH: join(platform, `durust-node.${nativeTarget()}.node`),
      DURUST_SQLITE_LIBRARY_PATH: join(platform, `durust-sqlite.${nativeTarget()}.node`)
    });
  });

  it("packs both binaries into the existing platform package", () => {
    const [packed] = JSON.parse(execFileSync("npm", ["pack", "--dry-run", "--json", platform], {
      env: { ...process.env, npm_config_cache: join(root, "npm-cache") },
      encoding: "utf8"
    })) as { files: { path: string }[] }[];
    const manifest = JSON.parse(readFileSync(join(platform, "package.json"), "utf8")) as { files: string[] };
    expect(packed?.files.map((file) => file.path).sort()).toEqual([...manifest.files, "package.json"].sort());
    expect(manifest.files).toHaveLength(2);
  });
});
