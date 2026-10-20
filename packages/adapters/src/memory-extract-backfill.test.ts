import type { BackgroundJob, JobPublisher } from "@rakazo/adapter-kit";
import { describe, expect, it, vi } from "vitest";
import type {
  BackfillExtractOptions,
  BackfillFact,
  BackfillSpace,
} from "./memory-extract-backfill.js";
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

function options(overrides: Partial<BackfillExtractOptions> = {}): BackfillExtractOptions {
  return { limit: 100, dryRun: false, force: false, repairOnly: false, ...overrides };
}

describe("parseBackfillExtractArgs", () => {
  it("defaults to a limit of 100 and accepts dry-run, space, and limit", () => {
    expect(parseBackfillExtractArgs([])).toEqual({
      limit: 100,
      dryRun: false,
      force: false,
      repairOnly: false,
    });
    expect(
      parseBackfillExtractArgs(["--dry-run", "--space", " space-9 ", "--limit", "25"]),
    ).toEqual({ spaceId: "space-9", limit: 25, dryRun: true, force: false, repairOnly: false });
  });

  it("skips a pnpm -- separator", () => {
    expect(parseBackfillExtractArgs(["--", "--dry-run"])).toEqual({
      limit: 100,
      dryRun: true,
      force: false,
      repairOnly: false,
    });
    expect(parseBackfillExtractArgs(["--", "--space", "X", "--limit", "10"])).toEqual({
      spaceId: "X",
      limit: 10,
      dryRun: false,
      force: false,
      repairOnly: false,
    });
  });

  it("accepts force and repair-only, and rejects using both", () => {
    expect(parseBackfillExtractArgs(["--force"])).toMatchObject({ force: true, repairOnly: false });
    expect(parseBackfillExtractArgs(["--", "--repair-only"])).toMatchObject({
      force: false,
      repairOnly: true,
    });
    expect(() => parseBackfillExtractArgs(["--force", "--repair-only"])).toThrow(
      /cannot be used together/,
    );
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
      options(),
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
      options({ limit: 2, dryRun: true }),
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
      options(),
    );
    expect(realignTimes).toHaveBeenCalledOnce();
    expect(skipped).not.toHaveBeenCalled();
    expect(queue.enqueue).not.toHaveBeenCalled();
    expect(counts).toMatchObject({
      relations: 4,
      entities: 2,
      repaired: 6,
      enqueued: 0,
      disabled: 1,
    });
  });

  it("does not write timestamps on a dry run", async () => {
    const realignTimes = vi.fn(async () => ({ relations: 4, entities: 2 }));
    const stampExtractedFacts = vi.fn(async () => 3);
    const counts = await backfillMemoryExtraction(
      [space(async () => [fact()], { realignTimes, stampExtractedFacts })],
      jobs(),
      options({ dryRun: true }),
    );
    expect(realignTimes).not.toHaveBeenCalled();
    expect(stampExtractedFacts).not.toHaveBeenCalled();
    expect(counts).toMatchObject({
      enqueued: 1,
      repaired: 0,
      marked: 0,
      relations: 0,
      entities: 0,
      dryRun: true,
    });
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
      options({ limit: 1 }),
    );
    expect(counts).toMatchObject({ skipped: 2, enqueued: 1 });
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.replaceKey).toBe("memory.extract:real:0");
  });

  it("adds no extract job when every fact is already extracted", async () => {
    const queue = jobs();
    const listFacts = vi.fn(async () => []);
    const countExtractedFacts = vi.fn(async () => 8);
    const stampExtractedFacts = vi.fn(async () => 8);
    const counts = await backfillMemoryExtraction(
      [
        space(listFacts, {
          countExtractedFacts,
          stampExtractedFacts,
          realignTimes: async () => ({ relations: 1, entities: 0 }),
        }),
      ],
      queue,
      options(),
    );
    expect(listFacts).toHaveBeenCalledWith({ limit: 100, skip: 0, includeExtracted: false });
    expect(stampExtractedFacts).toHaveBeenCalledOnce();
    expect(queue.enqueue).not.toHaveBeenCalled();
    expect(counts).toMatchObject({
      skipped: 8,
      enqueued: 0,
      repaired: 1,
      marked: 8,
      force: false,
    });
  });

  it("skips an extracted fact returned by the lister unless forced", async () => {
    const queue = jobs();
    const counts = await backfillMemoryExtraction(
      [space(async () => [fact({ extracted: true }), fact({ factId: "fact-2" })])],
      queue,
      options(),
    );
    expect(counts).toMatchObject({ skipped: 1, enqueued: 1 });
    expect(queue.enqueue).toHaveBeenCalledTimes(1);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.replaceKey).toBe("memory.extract:fact-2:0");

    const forced = jobs();
    const countExtractedFacts = vi.fn(async () => 9);
    const forcedCounts = await backfillMemoryExtraction(
      [space(async () => [fact({ extracted: true })], { countExtractedFacts })],
      forced,
      options({ force: true }),
    );
    expect(countExtractedFacts).not.toHaveBeenCalled();
    expect(forcedCounts).toMatchObject({ skipped: 0, enqueued: 1, force: true });
    const forcedJob = forced.enqueue.mock.calls[0]?.[0];
    expect(forcedJob?.replaceKey).toBe("memory.extract:fact-1:0");
  });

  it("repairs timestamps and enqueues nothing in repair-only mode", async () => {
    const queue = jobs();
    const realignTimes = vi.fn(async () => ({ relations: 3, entities: 1 }));
    const stampExtractedFacts = vi.fn(async () => 2);
    const listFacts = vi.fn(async () => [fact()]);
    const countExtractedFacts = vi.fn(async () => 5);
    const counts = await backfillMemoryExtraction(
      [space(listFacts, { realignTimes, stampExtractedFacts, countExtractedFacts })],
      queue,
      options({ repairOnly: true }),
    );
    expect(realignTimes).toHaveBeenCalledOnce();
    expect(stampExtractedFacts).toHaveBeenCalledOnce();
    expect(listFacts).not.toHaveBeenCalled();
    expect(queue.enqueue).not.toHaveBeenCalled();
    expect(counts).toMatchObject({
      repaired: 4,
      relations: 3,
      entities: 1,
      marked: 2,
      skipped: 5,
      enqueued: 0,
      repairOnly: true,
    });
  });
});
