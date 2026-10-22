import { currentDbContext } from "@rakazo/db";
import { describe, expect, it } from "vitest";
import { runCleanupGraph } from "./run-cleanup-graph.js";

describe("runCleanupGraph", () => {
  it("loads spaces as system so row-level security does not hide secrets", async () => {
    let seen = currentDbContext();
    const text = await runCleanupGraph({
      prisma: {
        spaceMemoryConfig: {
          findMany: async () => {
            seen = currentDbContext();
            return [];
          },
        },
      } as never,
      secrets: { load: async () => "{}" },
      options: { dryRun: true },
    });
    expect(seen).toEqual({ kind: "system", reason: "cleanup graph CLI" });
    expect(text).toContain("dry-run true");
    expect(text).toContain("spaces 0");
    expect(text).toContain("failed 0");
    expect(text).toContain("deleted dry-run");
  });

  it("prints the cause when a space cannot be opened", async () => {
    const text = await runCleanupGraph({
      prisma: {
        spaceMemoryConfig: {
          findMany: async () => [
            { spaceId: "space-1", userId: "user-1", settings: {}, secret: null },
          ],
        },
      } as never,
      secrets: { load: async () => "{}" },
      options: { dryRun: true, spaceId: "space-1" },
    });
    expect(text).toContain("space space-1 failed memory secret is not visible");
    expect(text).toContain("failed 1");
    expect(text).toContain("entities 0");
  });
});
