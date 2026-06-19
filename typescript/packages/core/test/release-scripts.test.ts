import { execFile } from "node:child_process";
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
    expect(result.stdout).toContain("==> Cross-runtime contract fixtures");
    expect(result.stdout).toContain("npm run check:fixtures");
    expect(result.stdout).toContain("==> Hot execution cache soak");
    expect(result.stdout).toContain("npm run test:soak");
    expect(result.stdout).toContain("==> Postgres release gate");
    expect(result.stdout).toContain("npm run check:postgres");
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
