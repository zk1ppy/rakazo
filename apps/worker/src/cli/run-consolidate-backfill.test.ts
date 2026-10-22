import type { JobPublisher } from "@rakazo/adapter-kit";
import { currentDbContext } from "@rakazo/db";
import { createLogger, createTestSink, installLogger } from "@rakazo/logging";
import { afterEach, describe, expect, it, vi } from "vitest";
import { describeConsolidatePreview, runConsolidateBackfill } from "./run-consolidate-backfill.js";

afterEach(() => {
  installLogger(createLogger({ service: "rakazo", level: "off", sinks: [] }));
});

function jobs(): JobPublisher {
  return { enqueue: vi.fn(async () => undefined), cancel: vi.fn(), close: vi.fn() };
}

describe("runConsolidateBackfill", () => {
  it("loads spaces as system so row-level security does not hide secrets", async () => {
    let seen = currentDbContext();
    const text = await runConsolidateBackfill({
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
      options: { limit: 10, dryRun: false },
    });
    expect(seen).toEqual({ kind: "system", reason: "backfill consolidate CLI" });
    expect(text).toBe("dry-run false\nspaces 0\nfailed 0\nenqueued 0\n");
  });

  it("prints the cause when a space cannot be opened", async () => {
    const text = await runConsolidateBackfill({
      prisma: {
        spaceMemoryConfig: {
          findMany: async () => [
            { spaceId: "space-1", userId: "user-1", settings: {}, secret: null },
          ],
        },
      } as never,
      secrets: { load: async () => "{}" },
      jobs: jobs(),
      options: { limit: 10, dryRun: false },
    });
    expect(text).toContain("spaces 0");
    expect(text).toContain("space space-1 failed memory secret is not visible");
    expect(text).toContain("failed 1");
    expect(text).toContain("enqueued 0");
  });
});

describe("describeConsolidatePreview", () => {
  it("keeps the cause of a preview failure", async () => {
    const sink = createTestSink();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    const described = await describeConsolidatePreview([
      {
        spaceId: "space-1",
        userId: "user-1",
        preview: async () => {
          throw new Error("confirmation timed out");
        },
      },
    ]);
    expect(described.failed).toBe(1);
    expect(described.lines).toEqual(["space space-1 failed confirmation timed out"]);
    expect(sink.events[0]?.error).toMatchObject({ message: "confirmation timed out" });
  });
});
