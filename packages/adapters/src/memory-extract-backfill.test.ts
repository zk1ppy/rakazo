import type { BackgroundJob, JobPublisher } from "@rakazo/adapter-kit";
import { createLogger, createTestSink, installLogger } from "@rakazo/logging";
import { afterEach, describe, expect, it, vi } from "vitest";
import type {
  BackfillExtractOptions,
  BackfillFact,
  BackfillSpace,
} from "./memory-extract-backfill.js";
import {
  backfillMemoryExtraction,
  loadNeo4jBackfillSpaces,
  parseBackfillExtractArgs,
} from "./memory-extract-backfill.js";

afterEach(() => {
  installLogger(createLogger({ service: "rakazo", level: "off", sinks: [] }));
});

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
  return {
    limit: 100,
    dryRun: false,
    force: false,
    repairOnly: false,
    retryEmpty: false,
    includeFailed: false,
    ...overrides,
  };
}

describe("parseBackfillExtractArgs", () => {
  it("defaults to a limit of 100 and accepts dry-run, space, and limit", () => {
    expect(parseBackfillExtractArgs([])).toEqual({
      limit: 100,
      dryRun: false,
      force: false,
      repairOnly: false,
      retryEmpty: false,
      includeFailed: false,
    });
    expect(
      parseBackfillExtractArgs(["--dry-run", "--space", " space-9 ", "--limit", "25"]),
    ).toEqual({
      spaceId: "space-9",
      limit: 25,
      dryRun: true,
      force: false,
      repairOnly: false,
      retryEmpty: false,
      includeFailed: false,
    });
  });

  it("skips a pnpm -- separator", () => {
    expect(parseBackfillExtractArgs(["--", "--space", "X", "--dry-run"])).toMatchObject({
      spaceId: "X",
      dryRun: true,
    });
    expect(parseBackfillExtractArgs(["--", "--dry-run"])).toEqual({
      limit: 100,
      dryRun: true,
      force: false,
      repairOnly: false,
      retryEmpty: false,
      includeFailed: false,
    });
    expect(parseBackfillExtractArgs(["--", "--space", "X", "--limit", "10"])).toEqual({
      spaceId: "X",
      limit: 10,
      dryRun: false,
      force: false,
      repairOnly: false,
      retryEmpty: false,
      includeFailed: false,
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

  it("accepts retry-empty with since, including a leading separator", () => {
    expect(
      parseBackfillExtractArgs([
        "--",
        "--retry-empty",
        "--since",
        "2026-10-22T16:00:00Z",
        "--space",
        "space-1",
        "--limit",
        "14",
        "--dry-run",
      ]),
    ).toEqual({
      spaceId: "space-1",
      limit: 14,
      dryRun: true,
      force: false,
      repairOnly: false,
      retryEmpty: true,
      includeFailed: false,
      since: "2026-10-22T16:00:00.000Z",
    });
    expect(parseBackfillExtractArgs(["--retry-empty"])).toMatchObject({
      retryEmpty: true,
      dryRun: false,
    });
    expect(parseBackfillExtractArgs(["--retry-empty"])).not.toHaveProperty("since");
  });

  it("rejects since without retry-empty, combined modes, and a bad date", () => {
    expect(() => parseBackfillExtractArgs(["--since", "2026-10-22T16:00:00Z"])).toThrow(
      /--since requires --retry-empty/,
    );
    expect(() => parseBackfillExtractArgs(["--include-failed"])).toThrow(
      /--include-failed requires --retry-empty/,
    );
    expect(parseBackfillExtractArgs(["--retry-empty", "--include-failed"])).toMatchObject({
      retryEmpty: true,
      includeFailed: true,
    });
    expect(() => parseBackfillExtractArgs(["--retry-empty", "--force"])).toThrow(
      /cannot be used together/,
    );
    expect(() => parseBackfillExtractArgs(["--retry-empty", "--repair-only"])).toThrow(
      /cannot be used together/,
    );
    expect(() => parseBackfillExtractArgs(["--retry-empty", "--since"])).toThrow(/ISO date/);
    expect(() => parseBackfillExtractArgs(["--retry-empty", "--since", "yesterday"])).toThrow(
      /ISO date/,
    );
    expect(() => parseBackfillExtractArgs(["--retry-empty", "--since", "2026-13-40"])).toThrow(
      /ISO date/,
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

  it("re-enqueues empty extractions and clears the marker only outside a dry run", async () => {
    const listed = vi.fn(
      async (input: { limit: number; skip: number; since?: string; includeFailed?: boolean }) => {
        expect(input.since).toBe("2026-10-22T16:00:00.000Z");
        expect(input.includeFailed).toBe(false);
        if (input.skip > 0) return [];
        return [
          fact({ factId: "empty-1", text: "ok" }),
          fact({ factId: "empty-2" }),
          fact({ factId: "empty-3" }),
          fact({ factId: "kept-edge" }),
        ];
      },
    );
    const listFacts = vi.fn(async () => [fact({ factId: "unextracted" })]);
    const clearExtractedAt = vi.fn(async (ids: readonly string[]) =>
      ids.filter((id) => id !== "kept-edge"),
    );
    const queue = jobs();
    const counts = await backfillMemoryExtraction(
      [space(listFacts, { listEmptyExtractedFacts: listed, clearExtractedAt })],
      queue,
      options({
        retryEmpty: true,
        since: "2026-10-22T16:00:00.000Z",
        limit: 10,
      }),
    );
    expect(listFacts).not.toHaveBeenCalled();
    expect(clearExtractedAt).toHaveBeenCalledWith(["empty-2", "empty-3", "kept-edge"]);
    expect(queue.enqueue).toHaveBeenCalledTimes(2);
    const ids = queue.enqueue.mock.calls.map((call) => {
      const job = call[0];
      if (job?.name !== "memory.extract") throw new Error("expected memory.extract");
      return job.payload.factId;
    });
    expect(ids).toEqual(["empty-2", "empty-3"]);
    expect(counts).toMatchObject({
      enqueued: 2,
      cleared: 2,
      skipped: 1,
      sample: ["empty-2", "empty-3"],
      retryEmpty: true,
      dryRun: false,
    });

    const dryQueue = jobs();
    const dryClear = vi.fn(async (ids: readonly string[]) => ids);
    const dryCounts = await backfillMemoryExtraction(
      [space(async () => [], { listEmptyExtractedFacts: listed, clearExtractedAt: dryClear })],
      dryQueue,
      options({ retryEmpty: true, since: "2026-10-22T16:00:00.000Z", dryRun: true, limit: 10 }),
    );
    expect(dryClear).not.toHaveBeenCalled();
    expect(dryQueue.enqueue).not.toHaveBeenCalled();
    expect(dryCounts).toMatchObject({
      enqueued: 3,
      cleared: 0,
      skipped: 1,
      sample: ["empty-2", "empty-3", "kept-edge"],
      dryRun: true,
    });
  });

  it("forwards --include-failed to the empty-extraction list", async () => {
    const listed = vi.fn(async () => [fact({ factId: "failed-1" })]);
    const counts = await backfillMemoryExtraction(
      [space(async () => [], { listEmptyExtractedFacts: listed })],
      jobs(),
      options({ retryEmpty: true, includeFailed: true, dryRun: true, limit: 5 }),
    );
    expect(listed).toHaveBeenCalledWith(
      expect.objectContaining({ includeFailed: true, limit: 5, skip: 0 }),
    );
    expect(counts).toMatchObject({ enqueued: 1, includeFailed: true, dryRun: true });
  });

  it("logs the cause when realigning one space fails", async () => {
    const sink = createTestSink();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    const counts = await backfillMemoryExtraction(
      [
        space(async () => [], {
          realignTimes: async () => Promise.reject(new Error("neo4j down")),
        }),
      ],
      jobs(),
      options(),
    );
    expect(counts.failed).toBe(1);
    expect(sink.events[0]?.error).toMatchObject({ message: "neo4j down" });
  });
});

describe("loadNeo4jBackfillSpaces", () => {
  it("records and logs a space whose secret row-level security hid", async () => {
    const sink = createTestSink();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    const loaded = await loadNeo4jBackfillSpaces(
      {
        spaceMemoryConfig: {
          findMany: async () => [
            { spaceId: "space-1", userId: "user-1", settings: {}, secret: null },
          ],
        },
      } as never,
      { load: async () => "{}" },
    );
    expect(loaded.spaces).toEqual([]);
    expect(loaded.failures).toEqual([
      { spaceId: "space-1", reason: "memory secret is not visible" },
    ]);
    expect(sink.events[0]).toMatchObject({
      message: "memory backfill skipped a space",
      error: { message: "memory secret is not visible" },
    });
  });
});
