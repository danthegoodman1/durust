import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { copyFile, mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

const execFileAsync = promisify(execFile);
const workspaceRootUrl = new URL("../../..", import.meta.url);
const workspaceRoot = fileURLToPath(workspaceRootUrl);
const checkReleaseScript = fileURLToPath(new URL("scripts/check-release.mjs", workspaceRootUrl));
const checkPostgresScript = fileURLToPath(new URL("scripts/check-postgres.mjs", workspaceRootUrl));

describe("release gate scripts", () => {
  it("prints the aggregate release gate command list in dry-run mode without Postgres", async () => {
    const result = await execFileAsync(process.execPath, [checkReleaseScript, "--dry-run"], {
      cwd: workspaceRoot,
      env: withoutPostgresUrl()
    });

    expect(result.stderr).toBe("");
    expect(result.stdout).toContain("==> Fast workspace gate");
    expect(result.stdout).toContain("npm run check");
    expect(result.stdout).toContain("==> Hot execution cache soak");
    expect(result.stdout).toContain("npm run test:soak");
    expect(result.stdout).toContain("==> Postgres release gate");
    expect(result.stdout).toContain("npm run check:postgres");
  });

  // `check:fixtures` used to be a step of its own here, listed immediately after
  // `npm run check` — which already runs it. Removing that duplicate would have
  // quietly cost this file its only statement about the cross-runtime fixtures,
  // so the step's expectations are not simply deleted: they are replaced by the
  // assertion that took the step's place, which is what now guarantees the
  // fixtures run at all.
  //
  // The two gates below are checked one at a time rather than together. A single
  // fixture missing both would pass while only one clause worked — and a fixture
  // that reproduces the *realistic* edit, dropping exactly one script, is the one
  // that proves each clause independently.
  const REQUIRED_IN_CHECK = ["check:fixtures", "check:test-types"] as const;

  for (const required of REQUIRED_IN_CHECK) {
    it(`refuses to run the release gate when npm run check no longer runs ${required}`, async () => {
      const root = await mkdtemp(join(tmpdir(), "durust-release-gate-"));
      await mkdir(join(root, "scripts"));
      await copyFile(checkReleaseScript, join(root, "scripts", "check-release.mjs"));
      // Everything the real `check` runs *except* this one script — the shape of
      // the edit that once left the shared corpus ungated in CI.
      const check = REQUIRED_IN_CHECK.filter((name) => name !== required)
        .map((name) => `npm run ${name}`)
        .concat("npm run build", "npm run test", "npm run lint")
        .join(" && ");
      await writeFile(
        join(root, "package.json"),
        JSON.stringify({ name: "release-gate-fixture", scripts: { check } })
      );

      await expect(
        execFileAsync(process.execPath, [join(root, "scripts", "check-release.mjs"), "--dry-run"], {
          cwd: root,
          env: withoutPostgresUrl()
        })
      ).rejects.toMatchObject({
        stderr: expect.stringContaining(required)
      });
    });
  }

  // The companion to the cases above: with the real `package.json` the same
  // assertion must *pass*, or they would be failing for a reason unrelated to
  // the edit they are meant to catch and would prove nothing.
  it("accepts the workspace package.json, whose check script runs every required gate", () => {
    const check = JSON.parse(
      readFileSync(fileURLToPath(new URL("package.json", workspaceRootUrl)), "utf8")
    ).scripts.check;
    for (const required of REQUIRED_IN_CHECK) {
      expect(check).toContain(required);
    }
  });

  it("fails before running the aggregate release gate when Postgres is not configured", async () => {
    await expect(
      execFileAsync(process.execPath, [checkReleaseScript], {
        cwd: workspaceRoot,
        env: withoutPostgresUrl()
      })
    ).rejects.toMatchObject({
      stdout: "",
      stderr: expect.stringContaining(
        "npm run check:release requires DURUST_POSTGRES_URL because it runs npm run check:postgres"
      )
    });
  });

  it("fails the standalone Postgres gate when Postgres is not configured", async () => {
    await expect(
      execFileAsync(process.execPath, [checkPostgresScript], {
        cwd: workspaceRoot,
        env: withoutPostgresUrl()
      })
    ).rejects.toMatchObject({
      stdout: "",
      stderr: expect.stringContaining(
        "npm run check:postgres requires DURUST_POSTGRES_URL to point at a test database"
      )
    });
  });
});

function withoutPostgresUrl(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  delete env.DURUST_POSTGRES_URL;
  return env;
}
