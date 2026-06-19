import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { beforeAll, describe, expect, it } from "vitest";

const execFileAsync = promisify(execFile);
const workspaceRootUrl = new URL("../../..", import.meta.url);
const workspaceRoot = fileURLToPath(workspaceRootUrl);
const lintScript = fileURLToPath(new URL("scripts/determinism-lint.mjs", workspaceRootUrl));

describe("workflow determinism lint", () => {
  beforeAll(async () => {
    await execFileAsync("npm", ["run", "build", "--workspace", "@durust/eslint-plugin"], {
      cwd: workspaceRoot
    });
  });

  it("accepts workflow source from package configuration", async () => {
    await expect(runLint()).resolves.toMatchObject({
      stdout: "",
      stderr: ""
    });
  });

  it("accepts explicit workflow source globs", async () => {
    await expect(
      runLint("--workflow-source", "test-d/determinism/valid/**/*.ts")
    ).resolves.toMatchObject({
      stdout: "",
      stderr: ""
    });
  });

  it("rejects representative hidden IO and native async APIs", async () => {
    await expect(runLint("--workflow-source", "test-d/determinism/invalid/**/*.ts")).rejects.toMatchObject({
      stderr: expect.stringContaining("durust/no-hidden-io")
    });

    try {
      await runLint("--workflow-source", "test-d/determinism/invalid/**/*.ts");
      throw new Error("expected determinism lint to fail");
    } catch (error) {
      const stderr = String((error as { readonly stderr?: unknown }).stderr ?? "");
      expect(stderr).toContain("node:fs");
      expect(stderr).toContain("node:http");
      expect(stderr).toContain("node:worker_threads");
      expect(stderr).toContain("Date.now()");
      expect(stderr).toContain("Math.random()");
      expect(stderr).toContain("setTimeout()");
      expect(stderr).toContain("queueMicrotask()");
      expect(stderr).toContain("Promise.race()");
      expect(stderr).toContain("Promise.allSettled()");
      expect(stderr).toContain("fetch()");
      expect(stderr).toContain("node:fs/promises");
      expect(stderr).toContain("node:child_process");
      expect(stderr).toContain("WebSocket");
    }
  });
});

async function runLint(...args: string[]): Promise<{ readonly stdout: string; readonly stderr: string }> {
  return await execFileAsync(process.execPath, [lintScript, ...args], {
    cwd: workspaceRoot
  });
}
