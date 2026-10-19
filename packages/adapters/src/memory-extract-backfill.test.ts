import type { BackgroundJob, JobPublisher } from "@rakazo/adapter-kit";
import { describe, expect, it, vi } from "vitest";
import type { BackfillFact, BackfillSpace } from "./memory-extract-backfill.js";
import { backfillMemoryExtraction, parseBackfillExtractArgs } from "./memory-extract-backfill.js";

const fact = (overrides: Partial<BackfillFact> = {}): BackfillFact => ({
  factId: "fact-1",
  ns: "shared",
  botId: "bot-1",
  text: "Ada Lovelace founded the Analytical Society in London.",
  ...overrides,
});

function space(
  listFacts: BackfillSpace["listFacts"],
  overrides: Partial<BackfillSpace> = {},
): BackfillSpace {
  return {
    spaceId: "space-1",
    userId: "user-1",
    extractionEnabled: true,
    maxChunkChars: 4_000,
    listFacts,
    ...overrides,
  };
}

function jobs(): JobPublisher & {
  enqueue: ReturnType<typeof vi.fn<(job: BackgroundJob) => Promise<void>>>;
} {
  return {
    enqueue: vi.fn(async (_job: BackgroundJob) => undefined),
    cancel: vi.fn(),
    close: vi.fn(),
  };
}

describe("parseBackfillExtractArgs", () => {
  it("defaults to a limit of 100 and accepts dry-run, space, and limit", () => {
    expect(parseBackfillExtractArgs([])).toEqual({ limit: 100, dryRun: false });
    expect(
      parseBackfillExtractArgs(["--dry-run", "--space", " space-9 ", "--limit", "25"]),
    ).toEqual({ spaceId: "space-9", limit: 25, dryRun: true });
  });

  it("skips a pnpm -- separator", () => {
    expect(parseBackfillExtractArgs(["--", "--dry-run"])).toEqual({ limit: 100, dryRun: true });
    expect(parseBackfillExtractArgs(["--", "--space", "X", "--limit", "10"])).toEqual({
      spaceId: "X",
      limit: 10,
      dryRun: false,
    });
  });

  it("rejects an unknown flag and a limit outside 1 to 5000", () => {
    expect(() => parseBackfillExtractArgs(["--nope"])).toThrow(/Usage/);
    expect(() => parseBackfillExtractArgs(["--limit", "0"])).toThrow(/--limit/);
    expect(() => parseBackfillExtractArgs(["--limit", "5001"])).toThrow(/--limit/);
    expect(() => parseBackfillExtractArgs(["--space"])).toThrow(/--space/);
  });
});

describe("backfillMemoryExtraction", () => {
  it("enqueues unextracted facts on the serial queue and skips the rest", async () => {
    const queue = jobs();
    const counts = await backfillMemoryExtraction(
      [
        space(async () => [
          fact(),
          fact({ factId: "fact-2", text: "ok" }),
          fact({ factId: "", text: "Ada Lovelace founded the Analytical Society in London." }),
        ]),
        space(async () => [fact({ factId: "fact-3" })], {
          spaceId: "space-off",
          extractionEnabled: false,
        }),
      ],
      queue,
      { limit: 100, dryRun: false },
    );
    expect(counts).toMatchObject({
      spaces: 2,
      disabled: 1,
      skipped: 2,
      enqueued: 1,
      relations: 0,
      entities: 0,
      dryRun: false,
    });
    expect(queue.enqueue).toHaveBeenCalledTimes(1);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.name).toBe("memory.extract");
    expect(job?.queueName).toBe("memory-extract");
    expect(job?.replaceKey).toBe("memory.extract:fact-1:0");
    expect(job?.maxAttempts).toBe(1);
    if (job?.name !== "memory.extract") throw new Error("expected memory.extract");
    expect(job.payload).toMatchObject({
      factId: "fact-1",
      spaceId: "space-1",
      userId: "user-1",
      scheduleGeneration: 0,
      chunkIndex: 0,
      attempt: 0,
    });
    expect(JSON.stringify(job.payload)).not.toContain("Lovelace");
  });

  it("counts a dry run without enqueueing and stops at the limit", async () => {
    const queue = jobs();
    const counts = await backfillMemoryExtraction(
      [space(async () => [fact(), fact({ factId: "fact-2" }), fact({ factId: "fact-3" })])],
      queue,
      { limit: 2, dryRun: true },
    );
    expect(counts.enqueued).toBe(2);
    expect(counts.dryRun).toBe(true);
    expect(queue.enqueue).not.toHaveBeenCalled();
  });

  it("realigns existing edges without enqueueing a model job", async () => {
    const queue = jobs();
    const realignTimes = vi.fn(async () => ({ relations: 4, entities: 2 }));
    const skipped = vi.fn(async () => ({ relations: 9, entities: 9 }));
    const counts = await backfillMemoryExtraction(
      [
        space(async () => [], { realignTimes }),
        space(async () => [fact()], {
          spaceId: "space-off",
          extractionEnabled: false,
          realignTimes: skipped,
        }),
      ],
      queue,
      { limit: 100, dryRun: false },
    );
    expect(realignTimes).toHaveBeenCalledOnce();
    expect(skipped).not.toHaveBeenCalled();
    expect(queue.enqueue).not.toHaveBeenCalled();
    expect(counts).toMatchObject({ relations: 4, entities: 2, enqueued: 0, disabled: 1 });
  });

  it("does not write timestamps on a dry run", async () => {
    const realignTimes = vi.fn(async () => ({ relations: 4, entities: 2 }));
    const counts = await backfillMemoryExtraction(
      [space(async () => [fact()], { realignTimes })],
      jobs(),
      { limit: 100, dryRun: true },
    );
    expect(realignTimes).not.toHaveBeenCalled();
    expect(counts).toMatchObject({ enqueued: 1, relations: 0, entities: 0, dryRun: true });
  });

  it("keeps paging when a full page is trivial", async () => {
    const all = [
      fact({ factId: "t1", text: "ok" }),
      fact({ factId: "t2", text: "thanks" }),
      fact({ factId: "real" }),
    ];
    const queue = jobs();
    const counts = await backfillMemoryExtraction(
      [space(async ({ limit, skip }) => all.slice(skip, skip + limit))],
      queue,
      { limit: 1, dryRun: false },
    );
    expect(counts).toMatchObject({ skipped: 2, enqueued: 1 });
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.replaceKey).toBe("memory.extract:real:0");
  });
});
