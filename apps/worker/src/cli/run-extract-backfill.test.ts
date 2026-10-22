import type { JobPublisher } from "@rakazo/adapter-kit";
import { currentDbContext } from "@rakazo/db";
import { describe, expect, it, vi } from "vitest";
import { runExtractBackfill } from "./run-extract-backfill.js";

function jobs(): JobPublisher {
  return { enqueue: vi.fn(async () => undefined), cancel: vi.fn(), close: vi.fn() };
}

describe("runExtractBackfill", () => {
  it("loads spaces as system so row-level security does not hide secrets", async () => {
    let seen = currentDbContext();
    const text = await runExtractBackfill({
      prisma: {
        spaceMemoryConfig: {
          findMany: async () => {
            seen = currentDbContext();
            return [];
          },
        },
      } as never,
      secrets: { load: async () => "{}" },
      jobs: jobs(),
      options: { limit: 10, dryRun: true, force: false, repairOnly: false },
    });
    expect(seen).toEqual({ kind: "system", reason: "backfill extract CLI" });
    expect(text).toContain("spaces 0");
    expect(text).toContain("failed 0");
  });

  it("prints the cause when a space cannot be opened", async () => {
    const text = await runExtractBackfill({
      prisma: {
        spaceMemoryConfig: {
          findMany: async () => [
            { spaceId: "space-1", userId: "user-1", settings: {}, secret: null },
          ],
        },
      } as never,
      secrets: { load: async () => "{}" },
      jobs: jobs(),
      options: { limit: 10, dryRun: true, force: false, repairOnly: false },
    });
    expect(text).toContain("space space-1 failed memory secret is not visible");
    expect(text).toContain("failed 1");
    expect(text).toContain("enqueued 0");
  });
});
