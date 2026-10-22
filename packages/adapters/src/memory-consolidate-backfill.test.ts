import type { BackgroundJob, JobPublisher } from "@rakazo/adapter-kit";
import { createLogger, createTestSink, installLogger } from "@rakazo/logging";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConsolidateSpace } from "./memory-consolidate-backfill.js";
import {
  backfillMemoryConsolidation,
  loadNeo4jConsolidationSpaces,
  parseBackfillConsolidateArgs,
} from "./memory-consolidate-backfill.js";

afterEach(() => {
  installLogger(createLogger({ service: "rakazo", level: "off", sinks: [] }));
});

function jobs(): JobPublisher & {
  enqueue: ReturnType<typeof vi.fn<(job: BackgroundJob) => Promise<void>>>;
} {
  return {
    enqueue: vi.fn(async () => undefined),
    cancel: vi.fn(),
    close: vi.fn(),
  };
}

const space = (overrides: Partial<ConsolidateSpace> = {}): ConsolidateSpace => ({
  spaceId: "space-1",
  userId: "user-1",
  dreamEnabled: false,
  ...overrides,
});

describe("parseBackfillConsolidateArgs", () => {
  it("defaults to a limit of 100 and accepts dry-run, space, namespace, and limit", () => {
    expect(parseBackfillConsolidateArgs([])).toEqual({ limit: 100, dryRun: false });
    expect(
      parseBackfillConsolidateArgs([
        "--dry-run",
        "--space",
        " space-9 ",
        "--ns",
        " shared ",
        "--limit",
        "25",
      ]),
    ).toEqual({ spaceId: "space-9", ns: "shared", limit: 25, dryRun: true });
    expect(parseBackfillConsolidateArgs(["--", "--dry-run"])).toEqual({
      limit: 100,
      dryRun: true,
    });
    expect(parseBackfillConsolidateArgs(["--", "--space", "X", "--dry-run"])).toEqual({
      spaceId: "X",
      limit: 100,
      dryRun: true,
    });
    expect(() => parseBackfillConsolidateArgs(["--nope"])).toThrow(/Usage/);
    expect(() => parseBackfillConsolidateArgs(["--limit", "0"])).toThrow(/--limit/);
    expect(() => parseBackfillConsolidateArgs(["--space"])).toThrow(/--space/);
    expect(() => parseBackfillConsolidateArgs(["--ns"])).toThrow(/--ns/);
  });
});

describe("backfillMemoryConsolidation", () => {
  it("enqueues one serial consolidate job per space", async () => {
    const queue = jobs();
    const counts = await backfillMemoryConsolidation(
      [space(), space({ spaceId: "space-2", userId: "user-2" })],
      queue,
      { limit: 10, dryRun: false, ns: "shared" },
    );
    expect(counts).toEqual({ spaces: 2, failed: 0, enqueued: 2, dryRun: false });
    expect(queue.enqueue).toHaveBeenCalledTimes(2);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job).toMatchObject({
      name: "memory.consolidate",
      queueName: "memory-extract",
      payload: {
        spaceId: "space-1",
        userId: "user-1",
        ns: "shared",
        attempt: 0,
        scheduleGeneration: 0,
      },
    });
  });

  it("counts a dry run without enqueueing", async () => {
    const queue = jobs();
    const counts = await backfillMemoryConsolidation([space()], queue, {
      limit: 10,
      dryRun: true,
    });
    expect(counts.enqueued).toBe(1);
    expect(queue.enqueue).not.toHaveBeenCalled();
  });

  it("logs a space that has no id instead of dropping it quietly", async () => {
    const sink = createTestSink();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    const counts = await backfillMemoryConsolidation(
      [space({ spaceId: " ", userId: "" })],
      jobs(),
      {
        limit: 10,
        dryRun: false,
      },
    );
    expect(counts.failed).toBe(1);
    expect(counts.enqueued).toBe(0);
    expect(sink.events[0]?.error).toMatchObject({ message: "space id or user id is empty" });
  });
});

describe("loadNeo4jConsolidationSpaces", () => {
  it("records and logs a space whose secret row-level security hid", async () => {
    const sink = createTestSink();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    const loaded = await loadNeo4jConsolidationSpaces(
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
      message: "memory consolidate skipped a space",
      "memory.space_id": "space-1",
      error: { message: "memory secret is not visible" },
    });
  });
});
