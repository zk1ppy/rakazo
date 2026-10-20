import type {
  AgentRunRequest,
  AgentRuntime,
  BackgroundJob,
  BackgroundJobPayloads,
  JobPublisher,
} from "@rakazo/adapter-kit";
import { TransientModelError } from "@rakazo/adapter-kit";
import { createLogger, createTestSink, getLogger, installLogger } from "@rakazo/logging";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { MemoryExtractDeps, MemoryExtractStore } from "./memory-extract.js";
import {
  assertOwnerAliases,
  extractMemoryGraph,
  isNonReferentName,
  isTrivialMemoryText,
  MEMORY_EXTRACT_INSTRUCTIONS,
  memoryExtractPrompt,
  memorySubjectName,
  normalizeRelationType,
  parseMemoryExtraction,
  planExtractionChunks,
  readNeo4jExtractionModel,
  readOwnerAliases,
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
  options?: {
    enabled?: boolean;
    model?: { provider: string; id: string } | null;
    aliases?: readonly string[];
  },
): MemoryExtractStore & { applyExtraction: ReturnType<typeof vi.fn> } {
  return {
    extractionEnabled: () => options?.enabled ?? true,
    extractionModel: () => options?.model ?? null,
    ownerAliases: () => options?.aliases ?? readOwnerAliases({}),
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
  resolved: AgentRunRequest["model"] = model,
): MemoryExtractDeps & {
  jobs: JobPublisher & { enqueue: ReturnType<typeof vi.fn> };
} {
  return {
    runtime: runtime(run, scripted),
    prisma: { usageRecord: { create: vi.fn() } } as unknown as MemoryExtractDeps["prisma"],
    jobs: queue,
    resolveModel: vi.fn(async () => resolved),
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
    expect(extracted.entities[0]?.summary).toBe("");
    expect(extracted.entities[0]?.aliases).toEqual(["Ada"]);
    expect(extracted.relations).toEqual([
      {
        fromNorm: "ada lovelace",
        toNorm: "analytical society",
        type: "founded",
        time: "1843",
      },
    ]);
  });

  it("treats lives in, LIVES_IN, and lives_in as one relation", () => {
    expect(normalizeRelationType("lives in")).toBe("lives_in");
    expect(normalizeRelationType("LIVES_IN")).toBe("lives_in");
    expect(normalizeRelationType("lives_in")).toBe("lives_in");
    const extracted = parseMemoryExtraction(
      JSON.stringify({
        entities: [
          { type: "Person", name: "Ada Lovelace" },
          { type: "Location", name: "The Lab" },
        ],
        relations: [
          { source: "Ada Lovelace", target: "The Lab", type: "lives in" },
          { source: "Ada Lovelace", target: "The Lab", type: "LIVES_IN" },
          { source: "Ada Lovelace", target: "The Lab", type: "lives_in" },
        ],
      }),
      "space-1",
      "shared",
    );
    expect(extracted.relations).toEqual([
      { fromNorm: "ada lovelace", toNorm: "the lab", type: "lives_in", time: null },
    ]);
  });
});

const nicolasFact =
  "Nicolas (l'utilisateur, propriétaire de l'instance Home Assistant ha.skippy-home.xyz) — 2026-10-18 : son identifiant/nom d'utilisateur est « nicolas »...";

/** Reasoning restates the empty shape, then the answer is a fenced object. */
const glmNicolasReply = [
  'The memory names the owner and the instance. The empty shape {"entities":[],"relations":[]} does not apply.',
  "",
  "```json",
  JSON.stringify({
    entities: [
      {
        type: "Person",
        name: "Nicolas",
        aliases: ["nicolas"],
        attributes: { role: "utilisateur" },
      },
      {
        type: "Object",
        name: "Home Assistant",
        aliases: ["ha.skippy-home.xyz"],
        attributes: { host: "ha.skippy-home.xyz" },
      },
    ],
    relations: [{ source: "Nicolas", target: "Home Assistant", type: "owns" }],
  }),
  "```",
].join("\n");

describe("extraction role and measurement guards", () => {
  it("tells the model that roles, agents, and measurements are not entity names", () => {
    expect(MEMORY_EXTRACT_INSTRUCTIONS).toContain("software agent, bot, or assistant is an Object");
    expect(MEMORY_EXTRACT_INSTRUCTIONS).toContain("never in aliases");
    expect(MEMORY_EXTRACT_INSTRUCTIONS).toContain("Dated measurements");
    expect(memorySubjectName("  Ada Lovelace  ")).toBe("Ada Lovelace");
    expect(memorySubjectName("ada@example.test")).toBeNull();
    expect(memoryExtractPrompt("note", "Ada Lovelace")).toContain("<subject>");
    expect(memoryExtractPrompt("note", "ada@example.test")).not.toContain("<subject>");
    expect(MEMORY_EXTRACT_INSTRUCTIONS).toContain("owner-aliases");
    expect(MEMORY_EXTRACT_INSTRUCTIONS).toContain("Option A");
    expect(memoryExtractPrompt("note", "Ada Lovelace", ["runner", "le coureur"])).toContain(
      "<owner-aliases>",
    );
    expect(memoryExtractPrompt("note", null, ["runner"])).not.toContain("<owner-aliases>");
  });

  it("drops a role alias and dated measurements, and keeps a handle", () => {
    const extracted = parseMemoryExtraction(
      JSON.stringify({
        entities: [
          {
            type: "Person",
            name: "Nicolas Tremblier",
            aliases: ["candidat", "ada42", "Nicolas"],
            attributes: { role: "candidat", host: "db.example.test" },
          },
        ],
        relations: [],
      }),
      "space-1",
      "shared",
    );
    expect(extracted.entities[0]).toMatchObject({
      name: "Nicolas Tremblier",
      aliases: ["ada42", "Nicolas"],
      summary: "role: candidat; host: db.example.test",
    });
    expect(extracted.entities[0]?.aliasNorms).not.toContain("candidat");
  });

  it("does not mint a person for a role that only holds measurements", () => {
    const extracted = parseMemoryExtraction(
      JSON.stringify({
        entities: [
          {
            type: "Person",
            name: "Runner",
            aliases: ["athlete"],
            attributes: {
              score_16_18_oct: "LOW",
              load_ratio: "2.5-3.0",
              readiness_18_oct: "LOW",
            },
          },
        ],
        relations: [],
      }),
      "space-1",
      "shared",
      "Nicolas Tremblier",
    );
    expect(extracted.entities).toEqual([
      expect.objectContaining({
        type: "Person",
        name: "Nicolas Tremblier",
        normName: "nicolas tremblier",
        summary: "",
        aliases: [],
      }),
    ]);
  });

  it("moves a role's relations onto the one named person and leaves another person in place", () => {
    const extracted = parseMemoryExtraction(
      JSON.stringify({
        entities: [
          {
            type: "Person",
            name: "Runner",
            aliases: ["athlete"],
            attributes: { score_16_oct: "LOW" },
          },
          { type: "Person", name: "Nicolas Tremblier", aliases: ["ada42"] },
          { type: "Person", name: "Marie Curie" },
          { type: "Location", name: "Strasbourg" },
        ],
        relations: [
          { source: "Runner", target: "Strasbourg", type: "trains_in" },
          { source: "Marie Curie", target: "Strasbourg", type: "lives_in" },
        ],
      }),
      "space-1",
      "shared",
    );
    expect(extracted.entities.map((entity) => entity.name).sort()).toEqual([
      "Marie Curie",
      "Nicolas Tremblier",
      "Strasbourg",
    ]);
    expect(extracted.relations).toEqual([
      expect.objectContaining({ fromNorm: "marie curie", toNorm: "strasbourg" }),
    ]);
  });

  it("attaches a metric role to the only concrete person in the same memory", () => {
    const extracted = parseMemoryExtraction(
      JSON.stringify({
        entities: [
          {
            type: "Person",
            name: "Runner",
            aliases: ["athlete"],
            attributes: { load_ratio: "2.5" },
          },
          { type: "Person", name: "Nicolas Tremblier" },
          { type: "Location", name: "Strasbourg" },
        ],
        relations: [{ source: "Runner", target: "Strasbourg", type: "trains_in" }],
      }),
      "space-1",
      "shared",
    );
    expect(extracted.entities.map((entity) => entity.name).sort()).toEqual([
      "Nicolas Tremblier",
      "Strasbourg",
    ]);
    expect(extracted.relations).toEqual([
      expect.objectContaining({
        fromNorm: "nicolas tremblier",
        toNorm: "strasbourg",
        type: "trains_in",
      }),
    ]);
  });

  it("attaches an owner alias to the space member and drops option labels", () => {
    const extracted = parseMemoryExtraction(
      JSON.stringify({
        entities: [
          { type: "Person", name: "Runner" },
          { type: "Person", name: "The Runner", aliases: ["athlete"] },
          { type: "Event", name: "Sunday session 18 October 2026" },
          { type: "Event", name: "Séance du dimanche 18 octobre 2026" },
          { type: "Event", name: "Option B" },
          { type: "Event", name: "Step 1" },
          { type: "Object", name: "N/A" },
        ],
        relations: [
          {
            source: "Runner",
            target: "Sunday session 18 October 2026",
            type: "performed_by",
          },
          {
            source: "The Runner",
            target: "Séance du dimanche 18 octobre 2026",
            type: "planned_for",
          },
        ],
      }),
      "space-1",
      "shared",
      "Ada Lovelace",
    );
    expect(extracted.entities.map((entity) => entity.name).sort()).toEqual([
      "Ada Lovelace",
      "Sunday session 18 October 2026",
      "Séance du dimanche 18 octobre 2026",
    ]);
    expect(extracted.entities.find((entity) => entity.name === "Ada Lovelace")).toMatchObject({
      type: "Person",
      normName: "ada lovelace",
      aliases: [],
    });
    expect(extracted.relations).toEqual([
      expect.objectContaining({
        fromNorm: "ada lovelace",
        toNorm: "sunday session 18 october 2026",
        type: "performed_by",
      }),
      expect.objectContaining({
        fromNorm: "ada lovelace",
        toNorm: "séance du dimanche 18 octobre 2026",
        type: "planned_for",
      }),
    ]);
  });

  it("prefers the space member when an alias only holds measurements", () => {
    const extracted = parseMemoryExtraction(
      JSON.stringify({
        entities: [
          { type: "Person", name: "Runner", attributes: { load_ratio: "2.5" } },
          { type: "Person", name: "Marie Curie" },
          { type: "Location", name: "London" },
        ],
        relations: [{ source: "Runner", target: "London", type: "trains_in" }],
      }),
      "space-1",
      "shared",
      "Ada Lovelace",
    );
    expect(extracted.entities.map((entity) => entity.name).sort()).toEqual([
      "Ada Lovelace",
      "London",
      "Marie Curie",
    ]);
    expect(extracted.relations).toEqual([
      expect.objectContaining({ fromNorm: "ada lovelace", toNorm: "london", type: "trains_in" }),
    ]);
  });

  it("uses a stored alias list instead of the built-in roles", () => {
    const extracted = parseMemoryExtraction(
      JSON.stringify({
        entities: [
          { type: "Person", name: "Runner" },
          { type: "Person", name: "Pilote" },
          { type: "Event", name: "Sunday session 18 October 2026" },
        ],
        relations: [
          { source: "Pilote", target: "Sunday session 18 October 2026", type: "performed_by" },
          { source: "Runner", target: "Sunday session 18 October 2026", type: "watched" },
        ],
      }),
      "space-1",
      "shared",
      "Ada Lovelace",
      ["pilote"],
    );
    expect(extracted.entities.map((entity) => entity.name).sort()).toEqual([
      "Ada Lovelace",
      "Runner",
      "Sunday session 18 October 2026",
    ]);
    expect(extracted.relations.map((relation) => relation.fromNorm).sort()).toEqual([
      "ada lovelace",
      "runner",
    ]);
  });

  it("leaves an owner-alias person in place when the space member has no name", () => {
    const extracted = parseMemoryExtraction(
      JSON.stringify({
        entities: [
          { type: "Person", name: "Runner" },
          { type: "Event", name: "Sunday session 18 October 2026" },
        ],
        relations: [
          { source: "Runner", target: "Sunday session 18 October 2026", type: "performed_by" },
        ],
      }),
      "space-1",
      "shared",
    );
    expect(extracted.entities.map((entity) => entity.name).sort()).toEqual([
      "Runner",
      "Sunday session 18 October 2026",
    ]);
  });

  it("reads owner aliases and rejects placeholders", () => {
    expect(readOwnerAliases({})).toContain("runner");
    expect(readOwnerAliases({ ownerAliases: "pilote" })).toEqual(["pilote"]);
    expect(assertOwnerAliases("  pilote, le coureur ")).toBe("pilote, le coureur");
    expect(assertOwnerAliases("")).toBeUndefined();
    expect(() => assertOwnerAliases("!!!")).toThrow(/Owner aliases/);
    expect(isNonReferentName("Option B")).toBe(true);
    expect(isNonReferentName("Plan A")).toBe(true);
    expect(isNonReferentName("Step 1")).toBe(true);
    expect(isNonReferentName("N/A")).toBe(true);
    expect(isNonReferentName("Séance du dimanche 18 octobre 2026")).toBe(false);
    expect(isNonReferentName("Sunday session 18 October 2026")).toBe(false);
    expect(isNonReferentName("Option B training")).toBe(false);
    expect(isNonReferentName("Step 1: warmup")).toBe(false);
  });
});

describe("extractMemoryGraph", () => {
  afterEach(() => {
    installLogger(createLogger({ service: "rakazo", level: "off", sinks: [] }));
  });

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
    await expect(pending).resolves.toBe(true);
    expect(order).toEqual(["start-0", "end-0", "start-1", "end-1"]);
    expect(calls).toBe(2);
    expect(run.mock.calls[0]?.[0]).toMatchObject({
      providerRetries: 0,
      tools: [],
      usageOperationKind: "retrieval",
      visibleOutputTokens: 1_024,
      model: { thinkingLevel: "off", id: "gpt-test" },
    });
    expect(run.mock.calls[0]?.[0].model.maxTokens).toBeUndefined();
    expect(memory.applyExtraction).toHaveBeenCalledTimes(2);
    expect(memory.applyExtraction.mock.calls[0]?.[0].entities[0].type).toBe("Person");
  });

  it("uses the connection minimum effort and keeps its output cap", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"entities":[],"relations":[]}' };
    });
    const glm = {
      provider: "openai-compatible",
      id: "hf:zai-org/GLM-5.3-Flash",
      apiKey: "test-key",
      reasoning: true,
      minReasoningEffort: "low" as const,
      maxTokens: 65_536,
      contextWindow: 524_288,
    } satisfies AgentRunRequest["model"];
    await extractMemoryGraph(
      deps(run, jobs(), false, glm),
      payload(),
      store("Ada Lovelace founded the Analytical Society in London."),
    );
    expect(run).toHaveBeenCalledTimes(1);
    expect(run.mock.calls[0]?.[0]).toMatchObject({
      providerRetries: 0,
      visibleOutputTokens: 1_024,
      model: {
        id: "hf:zai-org/GLM-5.3-Flash",
        thinkingLevel: "low",
        minReasoningEffort: "low",
        maxTokens: 65_536,
      },
    });
  });

  it("treats an unset effort as the default without reading the model id", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"entities":[],"relations":[]}' };
    });
    const named = {
      provider: "openai-compatible",
      id: "hf:zai-org/GLM-5.3-Flash",
      apiKey: "test-key",
      reasoning: true,
      thinkingLevel: null,
      maxTokens: 65_536,
    } satisfies AgentRunRequest["model"];
    await extractMemoryGraph(
      deps(run, jobs(), false, named),
      payload(),
      store("Ada Lovelace founded the Analytical Society in London."),
    );
    expect(run.mock.calls[0]?.[0].model).toMatchObject({
      thinkingLevel: "medium",
      maxTokens: 65_536,
    });
  });

  it("keeps thinking off when the connection asks for off", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"entities":[],"relations":[]}' };
    });
    const named = {
      provider: "openai-compatible",
      id: "hf:zai-org/GLM-5.3-Flash",
      apiKey: "test-key",
      reasoning: true,
      thinkingLevel: "off" as const,
      maxTokens: 65_536,
    } satisfies AgentRunRequest["model"];
    await extractMemoryGraph(
      deps(run, jobs(), false, named),
      payload(),
      store("Ada Lovelace founded the Analytical Society in London."),
    );
    expect(run.mock.calls[0]?.[0].model).toMatchObject({
      thinkingLevel: "off",
      maxTokens: 65_536,
    });
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

  it("keeps the fenced answer when a GLM reply leads with reasoning", async () => {
    const sink = createTestSink();
    installLogger(createLogger({ service: "rakazo", sinks: [sink] }));
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: glmNicolasReply };
    });
    const memory = store(nicolasFact);
    await extractMemoryGraph(deps(run), payload(), memory);
    expect(memory.applyExtraction).toHaveBeenCalledTimes(1);
    const written = memory.applyExtraction.mock.calls[0]?.[0];
    expect(
      written?.entities.map((entity: { type: string; name: string }) => [entity.type, entity.name]),
    ).toEqual([
      ["Person", "Nicolas"],
      ["Object", "Home Assistant"],
    ]);
    expect(written?.relations).toEqual([
      expect.objectContaining({ type: "owns", fromNorm: "nicolas", toNorm: "home assistant" }),
    ]);
    expect(sink.events).toContainEqual(
      expect.objectContaining({
        message: "memory.extract wrote a graph",
        "memory.entities_parsed": 2,
        "memory.entities_kept": 2,
        "memory.edges_written": 3,
      }),
    );
    expect(sink.events.some((event) => event.level === "warn")).toBe(false);
  });

  it("warns when a non-trivial fact yields no entities", async () => {
    const sink = createTestSink();
    installLogger(createLogger({ service: "rakazo", sinks: [sink] }));
    const reply = 'Nothing here.\n{"entities":[],"relations":[]}';
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield {
        type: "usage" as const,
        provider: "openai-compatible",
        model: "glm",
        inputTokens: 40,
        outputTokens: 12,
        reasoningTokens: 8,
      };
      yield {
        type: "done" as const,
        text: reply,
        stopReason: "stop",
      };
    });
    const memory = store(nicolasFact);
    await extractMemoryGraph(deps(run), payload(), memory);
    expect(memory.applyExtraction).not.toHaveBeenCalled();
    expect(sink.events).toContainEqual(
      expect.objectContaining({
        level: "warn",
        message: "memory.extract kept no entities",
        "memory.fact_id": "fact-1",
        "memory.entities_parsed": 0,
        "memory.entities_kept": 0,
        "memory.edges_written": 0,
        "model.stop_reason": "stop",
        "usage.input": 40,
        "usage.output": 12,
        "usage.reasoning": 8,
        "memory.content_length": reply.trim().length,
        "memory.json_found": true,
      }),
    );
    expect(JSON.stringify(sink.events)).not.toContain("Nothing here");
    expect(JSON.stringify(sink.events)).not.toContain("nicolas");
  });

  it("records an empty completion that never produced a JSON object", async () => {
    const sink = createTestSink();
    installLogger(createLogger({ service: "rakazo", sinks: [sink] }));
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield {
        type: "usage" as const,
        provider: "openai-compatible",
        model: "glm",
        inputTokens: 11,
        outputTokens: 4,
        reasoningTokens: 4,
      };
      yield { type: "done" as const, stopReason: "stop" };
    });
    await extractMemoryGraph(
      deps(run),
      payload(),
      store("Ada Lovelace founded the Analytical Society in London."),
    );
    expect(sink.events).toContainEqual(
      expect.objectContaining({
        level: "warn",
        message: "memory.extract kept no entities",
        "model.stop_reason": "stop",
        "usage.input": 11,
        "usage.output": 4,
        "usage.reasoning": 4,
        "memory.content_length": 0,
        "memory.json_found": false,
      }),
    );
  });

  it("logs output and reasoning tokens when extraction fails permanently", async () => {
    const sink = createTestSink();
    const previous = getLogger();
    installLogger(createLogger({ service: "rakazo", sinks: [sink] }));
    try {
      const run = vi.fn<AgentRuntime["run"]>(async function* () {
        yield {
          type: "usage" as const,
          provider: "openai-compatible",
          model: "glm",
          inputTokens: 100,
          outputTokens: 3_072,
          reasoningTokens: 3_000,
        };
        throw new Error("The model hit its output limit before writing a response.");
      });
      await expect(extractMemoryGraph(deps(run), payload(), store(nicolasFact))).rejects.toThrow(
        "output limit",
      );
      expect(sink.events).toContainEqual(
        expect.objectContaining({
          message: "memory.extract failed",
          "job.outcome": "permanent",
          "usage.output": 3_072,
          "usage.reasoning": 3_000,
        }),
      );
    } finally {
      installLogger(previous);
    }
  });

  it("does not call the model when extraction is off, the runtime is scripted, or text is trivial", async () => {
    const run = vi.fn();
    await expect(
      extractMemoryGraph(
        deps(run),
        payload(),
        store("Ada Lovelace founded the Analytical Society in London.", { enabled: false }),
      ),
    ).resolves.toBe(false);
    await expect(
      extractMemoryGraph(
        deps(run, jobs(), true),
        payload(),
        store("Ada Lovelace founded the Analytical Society in London."),
      ),
    ).resolves.toBe(false);
    await expect(extractMemoryGraph(deps(run), payload(), store("ok"))).resolves.toBe(false);
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
