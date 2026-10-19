import type {
  AgentRunRequest,
  AgentRuntime,
  BackgroundJob,
  BackgroundJobPayloads,
  JobPublisher,
} from "@rakazo/adapter-kit";
import { TransientModelError } from "@rakazo/adapter-kit";
import { describe, expect, it, vi } from "vitest";
import type { MemoryExtractDeps, MemoryExtractStore } from "./memory-extract.js";
import {
  extractMemoryGraph,
  isTrivialMemoryText,
  parseMemoryExtraction,
  planExtractionChunks,
  readNeo4jExtractionModel,
} from "./memory-extract.js";

const payload = (
  overrides: Partial<BackgroundJobPayloads["memory.extract"]> = {},
): BackgroundJobPayloads["memory.extract"] => ({
  spaceId: "space-1",
  userId: "user-1",
  botId: "bot-1",
  factId: "fact-1",
  ns: "shared",
  maxChunkChars: 4_000,
  chunkIndex: 0,
  attempt: 0,
  scheduleGeneration: 0,
  ...overrides,
});

const model = {
  provider: "openai",
  id: "gpt-test",
  apiKey: "test-key",
} satisfies AgentRunRequest["model"];

function longText(): string {
  const sentence = "Ada Lovelace founded the Analytical Society laboratory in London.";
  const paragraph = [sentence, sentence, sentence, sentence].join(" ");
  return `${paragraph}\n\n${paragraph}`;
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

function store(
  text: string | null,
  options?: { enabled?: boolean; model?: { provider: string; id: string } | null },
): MemoryExtractStore & { applyExtraction: ReturnType<typeof vi.fn> } {
  return {
    extractionEnabled: () => options?.enabled ?? true,
    extractionModel: () => options?.model ?? null,
    readFactText: vi.fn(async () => text),
    applyExtraction: vi.fn(
      async (_input: Parameters<MemoryExtractStore["applyExtraction"]>[0]) => undefined,
    ),
  };
}

function runtime(run: AgentRuntime["run"], scripted = false): AgentRuntime {
  return {
    describe: () => ({ id: "test", capabilities: { scripted } }),
    run,
  } as unknown as AgentRuntime;
}

function deps(
  run: AgentRuntime["run"],
  queue = jobs(),
  scripted = false,
): MemoryExtractDeps & {
  jobs: JobPublisher & { enqueue: ReturnType<typeof vi.fn> };
} {
  return {
    runtime: runtime(run, scripted),
    prisma: { usageRecord: { create: vi.fn() } } as unknown as MemoryExtractDeps["prisma"],
    jobs: queue,
    resolveModel: vi.fn(async () => model),
  };
}

function done(text: string): AgentRuntime["run"] {
  return async function* () {
    yield { type: "done" as const, text };
  };
}

describe("memory extraction planning", () => {
  it("skips trivial text and widens chunks so a save stays within eight calls", () => {
    expect(isTrivialMemoryText("ok")).toBe(true);
    expect(isTrivialMemoryText("thanks")).toBe(true);
    expect(isTrivialMemoryText("Prefer metric units.")).toBe(false);
    const wide = planExtractionChunks("word ".repeat(4_000), 400);
    expect(wide.chunks.length).toBeLessThanOrEqual(8);
    expect(wide.maxChunkChars).toBeGreaterThan(400);
  });

  it("ignores an extraction model that is incomplete or too long", () => {
    expect(
      readNeo4jExtractionModel({
        extractModelProvider: " synthetic ",
        extractModelId: " model-a ",
      }),
    ).toEqual({ provider: "synthetic", id: "model-a" });
    expect(readNeo4jExtractionModel({ extractModelProvider: "synthetic" })).toBeNull();
    expect(
      readNeo4jExtractionModel({
        extractModelProvider: "x".repeat(81),
        extractModelId: "model-a",
      }),
    ).toBeNull();
  });

  it("drops invalid entities and relations that do not resolve", () => {
    const extracted = parseMemoryExtraction(
      JSON.stringify({
        entities: [
          { type: "person", name: "Ada Lovelace", aliases: ["Ada"], attributes: { role: 1 } },
          { type: "Bot", name: "Nope" },
          { type: "Organization", name: "Analytical Society" },
        ],
        relations: [
          { source: "Ada", target: "Analytical Society", type: "founded", time: "1843" },
          { source: "Ada", target: "Unknown Person", type: "knows" },
        ],
      }),
      "space-1",
      "shared",
    );
    expect(extracted.entities.map((entity) => entity.type)).toEqual(["Person", "Organization"]);
    expect(extracted.entities[0]?.summary).toContain("role: 1");
    expect(extracted.relations).toEqual([
      {
        fromNorm: "ada lovelace",
        toNorm: "analytical society",
        type: "founded",
        time: "1843",
      },
    ]);
  });
});

describe("extractMemoryGraph", () => {
  it("processes chunks sequentially and writes the parsed graph", async () => {
    const order: string[] = [];
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let calls = 0;
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      const index = calls;
      calls += 1;
      order.push(`start-${index}`);
      if (index === 0) await firstGate;
      order.push(`end-${index}`);
      yield {
        type: "done" as const,
        text: JSON.stringify({
          entities: [{ type: "Person", name: index === 0 ? "Ada Lovelace" : "Charles Babbage" }],
          relations: [],
        }),
      };
    });
    const memory = store(longText());
    const pending = extractMemoryGraph(deps(run), payload({ maxChunkChars: 400 }), memory);
    await vi.waitFor(() => expect(order).toEqual(["start-0"]));
    releaseFirst();
    await pending;
    expect(order).toEqual(["start-0", "end-0", "start-1", "end-1"]);
    expect(calls).toBe(2);
    expect(run.mock.calls[0]?.[0]).toMatchObject({
      providerRetries: 0,
      tools: [],
      usageOperationKind: "retrieval",
      visibleOutputTokens: 1_024,
      model: { thinkingLevel: "off", maxTokens: 1_024 },
    });
    expect(memory.applyExtraction).toHaveBeenCalledTimes(2);
    expect(memory.applyExtraction.mock.calls[0]?.[0].entities[0].type).toBe("Person");
  });

  it("reschedules the failing chunk on the serial queue instead of calling the next one", async () => {
    let calls = 0;
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      calls += 1;
      if (calls > 1) throw new TransientModelError("429 rate limit");
      yield { type: "done" as const, text: '{"entities":[],"relations":[]}' };
    });
    const queue = jobs();
    const before = Date.now();
    await extractMemoryGraph(deps(run, queue), payload({ maxChunkChars: 400 }), store(longText()));
    expect(calls).toBe(2);
    expect(queue.enqueue).toHaveBeenCalledTimes(1);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.queueName).toBe("memory-extract");
    expect(job?.maxAttempts).toBe(1);
    expect(job?.replaceKey).toBe("memory.extract:fact-1:1");
    expect(job?.name).toBe("memory.extract");
    if (job?.name !== "memory.extract") throw new Error("expected memory.extract");
    expect(job.payload).toMatchObject({ chunkIndex: 1, attempt: 1, scheduleGeneration: 1 });
    expect(job.availableAt?.getTime()).toBeGreaterThanOrEqual(before + 14_000);
    expect(JSON.stringify(job.payload)).not.toContain("Lovelace");
  });

  it("reschedules a non-JSON rate-limit reply and fails a permanent model error", async () => {
    const limited = jobs();
    await extractMemoryGraph(
      deps(done("429 too many requests"), limited),
      payload(),
      store("Ada Lovelace founded the Analytical Society in London."),
    );
    expect(limited.enqueue).toHaveBeenCalledTimes(1);
    const limitedJob = limited.enqueue.mock.calls[0]?.[0];
    expect(limitedJob?.name).toBe("memory.extract");
    if (limitedJob?.name !== "memory.extract") throw new Error("expected memory.extract");
    expect(limitedJob.payload.chunkIndex).toBe(0);

    const permanent = jobs();
    const run = vi.fn<AgentRuntime["run"]>(() => {
      throw new Error("The model hit its output limit before writing a response.");
    });
    await expect(
      extractMemoryGraph(
        deps(run, permanent),
        payload(),
        store("Ada Lovelace founded the Analytical Society in London."),
      ),
    ).rejects.toThrow("output limit");
    expect(permanent.enqueue).not.toHaveBeenCalled();
    expect(run).toHaveBeenCalledTimes(1);
  });

  it("does not call the model when extraction is off, the runtime is scripted, or text is trivial", async () => {
    const run = vi.fn();
    await extractMemoryGraph(
      deps(run),
      payload(),
      store("Ada Lovelace founded the Analytical Society in London.", { enabled: false }),
    );
    await extractMemoryGraph(
      deps(run, jobs(), true),
      payload(),
      store("Ada Lovelace founded the Analytical Society in London."),
    );
    await extractMemoryGraph(deps(run), payload(), store("ok"));
    expect(run).not.toHaveBeenCalled();
  });

  it("uses the pinned extraction model and falls back to the space default", async () => {
    const text = "Ada Lovelace founded the Analytical Society in London.";
    const reply = '{"entities":[],"relations":[]}';
    const pinned = {
      provider: "synthetic",
      id: "extract-model",
      apiKey: "pin-key",
    } satisfies AgentRunRequest["model"];

    const pinnedRun = vi.fn<AgentRuntime["run"]>(done(reply));
    const resolveConnectedModel = vi.fn(async () => pinned);
    const resolveModel = vi.fn(async () => model);
    await extractMemoryGraph(
      { ...deps(pinnedRun), resolveConnectedModel, resolveModel },
      payload(),
      store(text, { model: { provider: "synthetic", id: "extract-model" } }),
    );
    expect(resolveConnectedModel).toHaveBeenCalledWith(
      { userId: "user-1", spaceId: "space-1" },
      "synthetic",
      "extract-model",
    );
    expect(resolveModel).not.toHaveBeenCalled();
    expect(pinnedRun.mock.calls[0]?.[0].model).toMatchObject({
      provider: "synthetic",
      id: "extract-model",
    });

    const fallbackRun = vi.fn<AgentRuntime["run"]>(done(reply));
    const missing = vi.fn(async () => {
      throw new Error("Connect that model provider first");
    });
    const spaceDefault = vi.fn<NonNullable<MemoryExtractDeps["resolveModel"]>>(async () => model);
    await extractMemoryGraph(
      { ...deps(fallbackRun), resolveConnectedModel: missing, resolveModel: spaceDefault },
      payload(),
      store(text, { model: { provider: "synthetic", id: "gone" } }),
    );
    expect(spaceDefault).toHaveBeenCalledWith({ userId: "user-1", spaceId: "space-1" });
    expect(spaceDefault.mock.calls[0]?.[0]).not.toHaveProperty("botId");
    expect(fallbackRun.mock.calls[0]?.[0].model).toMatchObject({ id: "gpt-test" });

    const unsetRun = vi.fn<AgentRuntime["run"]>(done(reply));
    const unusedPin = vi.fn(async () => pinned);
    const unsetDefault = vi.fn<NonNullable<MemoryExtractDeps["resolveModel"]>>(async () => model);
    await extractMemoryGraph(
      { ...deps(unsetRun), resolveConnectedModel: unusedPin, resolveModel: unsetDefault },
      payload(),
      store(text),
    );
    expect(unusedPin).not.toHaveBeenCalled();
    expect(unsetDefault).toHaveBeenCalledWith({ userId: "user-1", spaceId: "space-1" });
    expect(unsetDefault.mock.calls[0]?.[0]).not.toHaveProperty("botId");
  });
});
