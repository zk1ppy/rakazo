import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

const root = path.resolve(import.meta.dirname, "../../..");

function readRepo(relative: string): string {
  return readFileSync(path.join(root, relative), "utf8");
}

describe("worker shutdown signal", () => {
  it.skipIf(process.platform === "win32")(
    "finishes stop after SIGTERM, including a second signal",
    async () => {
      const childPath = path.join(import.meta.dirname, "shutdown-signal.child.ts");
      const child = spawn(process.execPath, ["--import", "tsx", childPath, "400"], {
        cwd: root,
        stdio: ["ignore", "pipe", "pipe"],
      });
      let output = "";
      let errors = "";
      child.stdout?.on("data", (chunk: Buffer) => {
        output += chunk.toString();
      });
      child.stderr?.on("data", (chunk: Buffer) => {
        errors += chunk.toString();
      });
      try {
        const ready = Date.now();
        while (!output.includes("ready")) {
          if (Date.now() - ready > 5_000) {
            throw new Error(`child did not become ready\n${output}\n${errors}`);
          }
          await new Promise((resolve) => setTimeout(resolve, 20));
        }
        const started = Date.now();
        child.kill("SIGTERM");
        const stopping = Date.now();
        while (!output.includes("stopping")) {
          if (Date.now() - stopping > 2_000) {
            throw new Error(`child did not start shutdown\n${output}\n${errors}`);
          }
          await new Promise((resolve) => setTimeout(resolve, 20));
        }
        child.kill("SIGTERM");
        const exitCode = await new Promise<number | null>((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error("shutdown timed out")), 5_000);
          child.once("exit", (code) => {
            clearTimeout(timer);
            resolve(code);
          });
        });
        expect(exitCode, errors).toBe(0);
        expect(output).toContain("stopped");
        expect(Date.now() - started).toBeGreaterThanOrEqual(350);
      } finally {
        child.kill("SIGKILL");
      }
    },
  );
});

describe("container entrypoints", () => {
  it("runs the api and the worker with node, not pnpm start", () => {
    const apiStart = readRepo("infra/compose/start-api.sh");
    expect(apiStart).toContain("exec node --enable-source-maps dist/index.js");
    const apiCommands = apiStart
      .split("\n")
      .filter((line) => !line.trim().startsWith("#"))
      .join("\n");
    expect(apiCommands).not.toContain("pnpm");

    for (const dockerfile of ["infra/compose/Dockerfile", "infra/compose/Dockerfile.topology"]) {
      const source = readRepo(dockerfile);
      expect(source, dockerfile).toContain('CMD ["/app/infra/compose/start-api.sh"]');
      expect(source, dockerfile).not.toContain("pnpm --filter @rakazo/api start");
    }

    const railway = readRepo("infra/railway/start.sh");
    expect(railway).not.toContain("pnpm --filter @rakazo/api start");
    expect(railway).not.toContain("pnpm --filter @rakazo/worker start");
    expect(railway).toContain("(cd /app/apps/api && exec node --enable-source-maps dist/index.js)");
    expect(railway).toContain(
      "(cd /app/apps/worker && exec node --enable-source-maps dist/index.js)",
    );

    for (const compose of [
      "infra/compose/docker-compose.yml",
      "infra/compose/docker-compose.prod.yml",
      "infra/compose/docker-compose.images.yml",
      "infra/compose/docker-compose.topology.yml",
    ]) {
      const source = readRepo(compose);
      expect(source, compose).toContain(
        'command: ["node", "--enable-source-maps", "dist/index.js"]',
      );
      expect(source, compose).toMatch(
        /exec (?:env -u DATABASE_MIGRATION_URL )?node --enable-source-maps dist\/index\.js/,
      );
      expect(source, compose).not.toContain("pnpm --filter @rakazo/worker start");
      expect(source, compose).not.toContain("pnpm --filter @rakazo/api start");
    }
  });
});
