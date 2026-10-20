import type {
  AgentRunRequest,
  AgentRuntime,
  BackgroundJob,
  JobPublisher,
} from "@rakazo/adapter-kit";
import { TransientModelError } from "@rakazo/adapter-kit";
import type { PrismaClient } from "@rakazo/db";
import { createLogger, createTestSink, getLogger, installLogger } from "@rakazo/logging";
import { describe, expect, it, vi } from "vitest";
import type { MemoryConsolidateDeps, MemoryConsolidateStore } from "./memory-consolidate.js";
import {
  assertConsolidateTimeoutMs,
  classifyConsolidationPair,
  consolidateMemoryGraph,
  DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
  DEFAULT_CONSOLIDATE_THINKING_LEVEL,
  DEFAULT_CONSOLIDATE_TIMEOUT_MS,
  MAX_CONSOLIDATE_ATTEMPTS,
  MAX_CONSOLIDATE_LLM_CALLS,
  memoryConsolidatePrompt,
  parseConsolidationDecision,
  parseConsolidationTypeDecision,
  planCanonicalEntity,
  planCanonicalRename,
  readConsolidateModelSettings,
} from "./memory-consolidate.js";
import type {
  Neo4jConsolidationEntity,
  Neo4jDuplicateMerge,
  Neo4jEntityRename,
} from "./neo4j-memory-client.js";
import { REASONING_MODEL_MAX_TOKENS } from "./pi-runtime-limits.js";

const payload = {
  spaceId: "space-1",
  userId: "user-1",
  attempt: 0,
  scheduleGeneration: 0,
};

const model = {
  provider: "openai",
  id: "gpt-test",
  apiKey: "test-key",
} satisfies AgentRunRequest["model"];

function entity(overrides: Partial<Neo4jConsolidationEntity> = {}): Neo4jConsolidationEntity {
  return {
    id: "nicolas",
    ns: "shared",
    type: "Person",
    name: "Nicolas",
    normName: "nicolas",
    aliases: [],
    aliasNorms: [],
    summary: "",
    degree: 1,
    neighborIds: [],
    factIds: [],
    ...overrides,
  };
}

function jobs(): JobPublisher & {
  enqueue: ReturnType<typeof vi.fn<(job: BackgroundJob) => Promise<void>>>;
} {
  return {
    enqueue: vi.fn(async () => undefined),
    cancel: vi.fn(),
    close: vi.fn(),
  };
}

function pageStore(
  entities: Neo4jConsolidationEntity[],
  options?: { pageFull?: boolean; pageIds?: string[] },
): MemoryConsolidateStore & {
  merges: Neo4jDuplicateMerge[];
  renames: Neo4jEntityRename[];
  collapses: Array<{ spaceId: string; ids: string[]; ns?: string }>;
} {
  const merges: Neo4jDuplicateMerge[] = [];
  const renames: Neo4jEntityRename[] = [];
  const collapses: Array<{ spaceId: string; ids: string[]; ns?: string }> = [];
  return {
    merges,
    renames,
    collapses,
    extractionModel: () => null,
    consolidateModelSettings: () => readConsolidateModelSettings({}),
    listConsolidationPage: async () => ({
      entities,
      pageIds: options?.pageIds ?? entities.map((item) => item.id),
      pageFull: options?.pageFull ?? false,
    }),
    mergeDuplicateEntity: async (input) => {
      merges.push(input);
      return { merged: true };
    },
    renameEntity: async (input) => {
      renames.push(input);
      return true;
    },
    collapseParallelRelations: async (input) => {
      collapses.push(input);
      return { relations: 0, about: 0, from: 0 };
    },
  };
}

function runtime(run: AgentRuntime["run"], scripted = false): AgentRuntime {
  return {
    describe: () => ({ id: "test", capabilities: { scripted } }),
    run,
  } as unknown as AgentRuntime;
}

function deps(
  run: AgentRuntime["run"] = async function* () {
    yield { type: "done" as const, text: '{"same":true}' };
  },
  queue = jobs(),
  scripted = false,
): MemoryConsolidateDeps & { jobs: ReturnType<typeof jobs> } {
  return {
    runtime: runtime(run, scripted),
    prisma: { usageRecord: { create: vi.fn() } } as unknown as Pick<PrismaClient, "usageRecord">,
    jobs: queue,
    resolveModel: vi.fn(async () => model),
  };
}

describe("consolidation decisions", () => {
  it("merges when alias norms overlap and keeps the more specific name", () => {
    const full = entity({
      id: "ada-full",
      name: "Ada Lovelace",
      normName: "ada lovelace",
      aliases: ["Ada"],
      aliasNorms: ["ada"],
      degree: 2,
    });
    const short = entity({
      id: "ada-short",
      name: "Ada",
      normName: "ada",
      degree: 1,
    });
    expect(classifyConsolidationPair(full, short)).toEqual({ action: "merge", reason: "alias" });
    const plan = planCanonicalEntity(full, short);
    expect(plan.keep.id).toBe("ada-full");
    expect(plan.drop.id).toBe("ada-short");
    expect(plan.name).toBe("Ada Lovelace");
    expect(plan.normName).toBe("ada lovelace");
    expect(plan.aliases).toContain("Ada");
    expect(plan.aliasNorms).toContain("ada");
    expect(plan.aliasNorms).not.toContain("ada lovelace");
  });

  it("asks before merging a short name into a full name, and merges when they share a neighbor", () => {
    const short = entity({ id: "short", neighborIds: ["paris"] });
    const full = entity({
      id: "full",
      name: "Nicolas Tremblier",
      normName: "nicolas tremblier",
      degree: 2,
    });
    expect(classifyConsolidationPair(short, full)).toEqual({ action: "confirm", reason: "prefix" });
    expect(
      classifyConsolidationPair(short, { ...full, neighborIds: ["paris"], factIds: ["fact-1"] }),
    ).toEqual({ action: "merge", reason: "prefix-evidence" });
    const plan = planCanonicalEntity(short, full);
    expect(plan.name).toBe("Nicolas Tremblier");
    expect(plan.keep.id).toBe("full");
    expect(plan.aliases).toEqual(["Nicolas"]);
    expect(plan.aliasNorms).toEqual(["nicolas"]);
  });

  it("keeps the busier hub and gives it the more specific name", () => {
    const hub = entity({ id: "hub", degree: 20 });
    const full = entity({
      id: "full",
      name: "Nicolas Tremblier",
      normName: "nicolas tremblier",
      degree: 2,
    });
    const plan = planCanonicalEntity(hub, full);
    expect(plan.name).toBe("Nicolas Tremblier");
    expect(plan.normName).toBe("nicolas tremblier");
    expect(plan.keep.id).toBe("hub");
    expect(plan.drop.id).toBe("full");
    expect(plan.aliases).toContain("Nicolas");
    expect(plan.aliasNorms).toContain("nicolas");
    expect(plan.aliasNorms).not.toContain("nicolas tremblier");
  });

  it("promotes the only longer alias without changing the node id", () => {
    const plan = planCanonicalRename(
      entity({
        aliases: ["Nicolas Tremblier", "ada42"],
        aliasNorms: ["nicolas tremblier", "ada42"],
      }),
    );
    expect(plan).toMatchObject({
      name: "Nicolas Tremblier",
      normName: "nicolas tremblier",
    });
    expect(plan?.aliases).toContain("Nicolas");
    expect(plan?.aliases).toContain("ada42");
    expect(plan?.aliasNorms).toContain("nicolas");
    expect(plan?.aliasNorms).not.toContain("nicolas tremblier");
    expect(
      planCanonicalRename(
        entity({
          aliases: ["Nicolas Tremblier", "Nicolas Dupont"],
          aliasNorms: ["nicolas tremblier", "nicolas dupont"],
        }),
      ),
    ).toBeNull();
  });

  it("does not treat a role alias as immediate merge evidence", () => {
    const nicolas = entity({
      id: "nicolas",
      aliases: ["candidat", "ada42"],
      aliasNorms: ["candidat", "ada42"],
    });
    const marie = entity({
      id: "marie",
      name: "Marie",
      normName: "marie",
      aliases: ["candidat"],
      aliasNorms: ["candidat"],
    });
    expect(classifyConsolidationPair(nicolas, marie)).toEqual({ action: "skip", reason: "weak" });
    const handle = entity({ id: "handle", name: "ada42", normName: "ada42" });
    expect(classifyConsolidationPair(nicolas, handle)).toEqual({
      action: "merge",
      reason: "alias",
    });
  });

  it("asks before attaching a role-named person that shares a fact", () => {
    const runner = entity({
      id: "runner",
      name: "Runner",
      normName: "runner",
      aliases: ["athlete"],
      aliasNorms: ["athlete"],
      factIds: ["fact-1"],
      summary: "score_16_oct: LOW",
    });
    const named = entity({
      id: "full",
      name: "Nicolas Tremblier",
      normName: "nicolas tremblier",
      factIds: ["fact-1"],
    });
    const stranger = entity({
      id: "ada",
      name: "Ada Lovelace",
      normName: "ada lovelace",
    });
    expect(classifyConsolidationPair(runner, named)).toEqual({ action: "confirm", reason: "role" });
    expect(classifyConsolidationPair(runner, stranger)).toEqual({ action: "skip", reason: "weak" });
    const plan = planCanonicalEntity(runner, named);
    expect(plan.keep.id).toBe("full");
    expect(plan.name).toBe("Nicolas Tremblier");
    expect(plan.summary).toBe("");
  });

  it("asks which type a same-name pair should keep", () => {
    const org = entity({
      id: "org",
      type: "Organization",
      name: "Quill",
      normName: "quill",
    });
    const object = entity({
      id: "object",
      type: "Object",
      name: "Quill",
      normName: "quill",
    });
    expect(classifyConsolidationPair(org, object)).toEqual({
      action: "confirm",
      reason: "type-conflict",
    });
    expect(
      classifyConsolidationPair(
        org,
        entity({
          id: "other",
          type: "Object",
          name: "Quill",
          normName: "quill",
          ns: "isolated:bot",
        }),
      ).action,
    ).toBe("skip");
    expect(parseConsolidationTypeDecision('{"same":true,"type":"object"}')).toEqual({
      same: true,
      type: "Object",
    });
    expect(parseConsolidationTypeDecision('{"same":false}')).toEqual({ same: false, type: null });
    expect(parseConsolidationTypeDecision('{"same":true}')).toEqual({ same: true, type: null });
    expect(parseConsolidationTypeDecision('{"same":true,"type":"Animal"}')).toEqual({
      same: true,
      type: null,
    });
    expect(
      parseConsolidationTypeDecision(
        '<reasoning>{"same":"false"}</reasoning>\n{"same":"true","type":"Object"}',
      ),
    ).toEqual({ same: true, type: "Object" });
  });

  it("refuses a merge across types, namespaces, or merely similar words", () => {
    const person = entity();
    expect(classifyConsolidationPair(person, entity({ id: "place", type: "Location" }))).toEqual({
      action: "confirm",
      reason: "type-conflict",
    });
    expect(
      classifyConsolidationPair(person, entity({ id: "other", ns: "isolated:bot" })).action,
    ).toBe("skip");
    expect(
      classifyConsolidationPair(
        entity({ id: "ann", name: "Ann", normName: "ann" }),
        entity({ id: "annecy", name: "Annecy", normName: "annecy", type: "Location" }),
      ).action,
    ).toBe("skip");
    expect(
      classifyConsolidationPair(
        entity({ id: "a", name: "Nicolas Tremblier", normName: "nicolas tremblier" }),
        entity({ id: "b", name: "Nicolas Dupont", normName: "nicolas dupont" }),
      ).action,
    ).toBe("skip");
  });

  it("reads a same decision and ignores a reasoning preface", () => {
    expect(parseConsolidationDecision('```json\n{"same":true}\n```')).toBe(true);
    expect(parseConsolidationDecision('{"same":false}')).toBe(false);
    expect(parseConsolidationDecision('<think>{"same":true}</think>\n{"same":false}')).toBe(false);
    expect(parseConsolidationDecision("maybe")).toBeNull();
    expect(parseConsolidationDecision('{"same":"true"}')).toBe(true);
    expect(parseConsolidationDecision('{"same":" no "}')).toBe(false);
    expect(parseConsolidationDecision('{"decision":{"same":"yes"}}')).toBe(true);
    expect(parseConsolidationDecision('<think>still comparing\n{"same":"false"}')).toBe(false);
    expect(parseConsolidationDecision('<think>{"same":true}</think>')).toBe(true);
  });
});

describe("consolidateMemoryGraph", () => {
  it("collapses parallel relations even when the page has nothing to merge", async () => {
    const store = pageStore([entity({ id: "ada", name: "Ada", normName: "ada" })]);
    await consolidateMemoryGraph(deps(), payload, store);
    expect(store.merges).toHaveLength(0);
    expect(store.collapses).toEqual([
      { spaceId: "space-1", ids: ["ada"] },
      { spaceId: "space-1", ids: ["ada"] },
    ]);
  });

  it("collapses relations on entities carried from the previous page", async () => {
    const store = pageStore(
      [
        entity({ id: "ada", name: "Ada", normName: "ada" }),
        entity({ id: "bea", name: "Bea", normName: "bea" }),
      ],
      { pageIds: ["ada"] },
    );
    await consolidateMemoryGraph(deps(), payload, store);
    expect(store.merges).toHaveLength(0);
    expect(store.collapses[0]?.ids).toEqual(["ada", "bea"]);
    expect(store.collapses[1]?.ids).toEqual(["ada", "bea"]);
  });

  it("does not collapse an empty page", async () => {
    const store = pageStore([], { pageIds: [] });
    await consolidateMemoryGraph(deps(), payload, store);
    expect(store.collapses).toEqual([]);
  });

  it("merges overlapping alias norms without calling the model", async () => {
    const run = vi.fn(async function* () {
      yield { type: "done" as const, text: '{"same":true}' };
    });
    const store = pageStore([
      entity({
        id: "ada-full",
        name: "Ada Lovelace",
        normName: "ada lovelace",
        aliases: ["Ada"],
        aliasNorms: ["ada"],
      }),
      entity({ id: "ada-short", name: "Ada", normName: "ada" }),
    ]);
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(run).not.toHaveBeenCalled();
    expect(store.merges).toHaveLength(1);
    expect(store.merges[0]).toMatchObject({
      spaceId: "space-1",
      keepId: "ada-full",
      dropId: "ada-short",
      type: "Person",
      name: "Ada Lovelace",
      normName: "ada lovelace",
    });
    expect(store.merges[0]?.aliases).toContain("Ada");
    expect(store.merges[0]?.aliasNorms).toContain("ada");
    expect(store.collapses.map((call) => call.ids)).toEqual([
      ["ada-full", "ada-short"],
      ["ada-full", "ada-short"],
    ]);
  });

  it("merges Nicolas into Nicolas Tremblier after one confirmation", async () => {
    const prompts: string[] = [];
    const run: AgentRuntime["run"] = async function* (request) {
      prompts.push(request.prompt);
      yield { type: "done" as const, text: '{"same": true}' };
    };
    const store = pageStore([
      entity({ id: "short", summary: "Works at SDIS 68" }),
      entity({
        id: "full",
        name: "Nicolas Tremblier",
        normName: "nicolas tremblier",
        summary: "Lives in Strasbourg",
        degree: 2,
        neighborIds: ["strasbourg"],
      }),
      entity({ id: "strasbourg", name: "Strasbourg", normName: "strasbourg", type: "Location" }),
    ]);
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(prompts).toHaveLength(1);
    expect(prompts[0]).toContain("Nicolas Tremblier");
    expect(prompts[0]).toContain("SDIS 68");
    expect(store.merges).toEqual([
      expect.objectContaining({
        keepId: "full",
        dropId: "short",
        type: "Person",
        name: "Nicolas Tremblier",
        normName: "nicolas tremblier",
        aliases: ["Nicolas"],
        aliasNorms: ["nicolas"],
      }),
    ]);
    const asked = memoryConsolidatePrompt(
      entity(),
      entity({ id: "full", name: "Nicolas Tremblier", normName: "nicolas tremblier" }),
      new Map(),
    );
    expect(asked).toContain('"type":"Person"');
  });

  it("merges a prefixed name with a shared neighbor and does not call the model", async () => {
    const run = vi.fn(async function* () {
      yield { type: "done" as const, text: '{"same":false}' };
    });
    const store = pageStore([
      entity({ id: "short", neighborIds: ["paris", "full"], factIds: ["fact-1"] }),
      entity({
        id: "full",
        name: "Nicolas Tremblier",
        normName: "nicolas tremblier",
        neighborIds: ["paris", "short"],
        factIds: ["fact-1"],
      }),
    ]);
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(run).not.toHaveBeenCalled();
    expect(store.merges).toHaveLength(1);
    expect(store.merges[0]).toMatchObject({
      keepId: "full",
      dropId: "short",
      name: "Nicolas Tremblier",
    });
  });

  it("does not merge a same-name type conflict without a chosen type", async () => {
    const run = vi.fn(async function* () {
      yield { type: "done" as const, text: '{"same":true}' };
    });
    const store = pageStore([
      entity({ aliasNorms: ["nicolas tremblier"] }),
      entity({
        id: "place",
        type: "Location",
        name: "Nicolas",
        normName: "nicolas",
      }),
    ]);
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(run).toHaveBeenCalledTimes(1);
    expect(store.merges).toEqual([]);
  });

  it("leaves the pair split when the model says they differ", async () => {
    const run: AgentRuntime["run"] = async function* () {
      yield { type: "done" as const, text: '{"same":false}' };
    };
    const store = pageStore([
      entity({ id: "short" }),
      entity({ id: "full", name: "Nicolas Tremblier", normName: "nicolas tremblier" }),
    ]);
    const queue = jobs();
    await consolidateMemoryGraph(deps(run, queue), payload, store);
    expect(store.merges).toEqual([]);
    expect(queue.enqueue).not.toHaveBeenCalled();
  });

  it("confirms ambiguous pairs one at a time and reschedules the rest", async () => {
    let active = 0;
    let maxActive = 0;
    const run: AgentRuntime["run"] = async function* () {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active -= 1;
      yield { type: "done" as const, text: '{"same":true}' };
    };
    const people = ["Nicolas", "Marie", "Ada", "Grace", "Alan"].flatMap((name) => [
      entity({ id: `${name}-short`, name, normName: name.toLowerCase() }),
      entity({
        id: `${name}-full`,
        name: `${name} Example`,
        normName: `${name.toLowerCase()} example`,
      }),
    ]);
    const store = pageStore(people);
    const queue = jobs();
    await consolidateMemoryGraph(deps(run, queue), payload, store);
    expect(maxActive).toBe(1);
    expect(store.merges).toHaveLength(MAX_CONSOLIDATE_LLM_CALLS);
    expect(queue.enqueue).toHaveBeenCalledTimes(1);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job).toMatchObject({
      name: "memory.consolidate",
      queueName: "memory-extract",
      payload: {
        spaceId: "space-1",
        attempt: 0,
        scheduleGeneration: 1,
      },
    });
  });

  it("reschedules a rate limit without starting another confirmation", async () => {
    let calls = 0;
    const run: AgentRuntime["run"] = async function* () {
      calls += 1;
      yield { type: "text" as const, text: "" };
      throw new TransientModelError("429 too many requests");
    };
    const store = pageStore([
      entity({ id: "short" }),
      entity({ id: "full", name: "Nicolas Tremblier", normName: "nicolas tremblier" }),
      entity({ id: "ada", name: "Ada", normName: "ada" }),
      entity({ id: "ada-full", name: "Ada Lovelace", normName: "ada lovelace" }),
    ]);
    const queue = jobs();
    await consolidateMemoryGraph(deps(run, queue), payload, store);
    expect(calls).toBe(1);
    expect(store.merges).toEqual([]);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.name).toBe("memory.consolidate");
    if (job?.name !== "memory.consolidate") throw new Error("expected memory.consolidate");
    expect(job.payload.attempt).toBe(1);
    expect(job.payload.scheduleGeneration).toBe(1);
    expect(job.queueName).toBe("memory-extract");
    expect(job.availableAt).toBeInstanceOf(Date);
  });

  it("confirms with the consolidate budget instead of the chat max", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"same":true}' };
    });
    const chat = {
      ...model,
      thinkingLevel: "medium" as const,
      minReasoningEffort: "medium" as const,
      maxTokens: 65_536,
    };
    const store = pageStore([
      entity({ id: "short" }),
      entity({ id: "full", name: "Nicolas Tremblier", normName: "nicolas tremblier" }),
    ]);
    await consolidateMemoryGraph(
      { ...deps(run), resolveModel: vi.fn(async () => chat) },
      payload,
      store,
    );
    expect(run.mock.calls[0]?.[0]).toMatchObject({
      visibleOutputTokens: 256,
      taskOutput: {
        task: "memory.consolidate",
        thinkingLevel: "low",
        maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      },
      model: { thinkingLevel: "low", maxTokens: 65_536 },
    });
  });

  it("merges a role person onto the named person after confirmation and drops measurements", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"same":true}' };
    });
    const store = pageStore([
      entity({
        id: "runner",
        name: "Runner",
        normName: "runner",
        aliases: ["athlete"],
        aliasNorms: ["athlete"],
        factIds: ["fact-1"],
        summary: "score_16_oct: LOW; load_ratio: 2.5",
      }),
      entity({
        id: "full",
        name: "Nicolas Tremblier",
        normName: "nicolas tremblier",
        factIds: ["fact-1"],
        summary: "Lives in Strasbourg",
      }),
    ]);
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(run).toHaveBeenCalledTimes(1);
    expect(store.merges[0]).toMatchObject({
      keepId: "full",
      dropId: "runner",
      name: "Nicolas Tremblier",
      normName: "nicolas tremblier",
      summary: "Lives in Strasbourg",
    });
    expect(store.merges[0]?.summary).not.toContain("score_16");
    expect(store.merges[0]?.aliases).toContain("Runner");
  });

  it("relabels a same-name type conflict when the model picks a type", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"same":true,"type":"Object"}' };
    });
    const store = pageStore([
      entity({ id: "org", type: "Organization", name: "Quill", normName: "quill", degree: 4 }),
      entity({ id: "tool", type: "Object", name: "Quill", normName: "quill", degree: 1 }),
    ]);
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(store.merges).toEqual([
      expect.objectContaining({
        keepId: "org",
        dropId: "tool",
        type: "Object",
        sourceTypes: ["Organization", "Object"],
        name: "Quill",
      }),
    ]);
  });

  it("leaves a person and an organization split when the model says they differ", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"same":false}' };
    });
    const store = pageStore([
      entity({ id: "person", name: "Apple", normName: "apple" }),
      entity({ id: "org", type: "Organization", name: "Apple", normName: "apple" }),
    ]);
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(store.merges).toEqual([]);
  });

  it("promotes a stored short name when no other node claims the longer one", async () => {
    const store = pageStore([
      entity({
        id: "hub",
        aliases: ["Nicolas Tremblier"],
        aliasNorms: ["nicolas tremblier"],
      }),
    ]);
    await consolidateMemoryGraph(
      deps(async function* () {}),
      payload,
      store,
    );
    expect(store.merges).toEqual([]);
    expect(store.renames).toEqual([
      expect.objectContaining({
        id: "hub",
        fromNorm: "nicolas",
        name: "Nicolas Tremblier",
        normName: "nicolas tremblier",
      }),
    ]);
  });

  it("reads consolidate model settings and falls back when they are blank or invalid", () => {
    expect(DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS).toBe(REASONING_MODEL_MAX_TOKENS);
    expect(DEFAULT_CONSOLIDATE_TIMEOUT_MS).toBe(180_000);
    expect(readConsolidateModelSettings({})).toEqual({
      thinkingLevel: DEFAULT_CONSOLIDATE_THINKING_LEVEL,
      maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      timeoutMs: DEFAULT_CONSOLIDATE_TIMEOUT_MS,
    });
    expect(
      readConsolidateModelSettings({
        consolidateThinkingLevel: " minimal ",
        consolidateMaxOutputTokens: "2048",
        consolidateTimeoutMs: " 180000 ",
      }),
    ).toEqual({ thinkingLevel: "minimal", maxOutputTokens: 2048, timeoutMs: 180_000 });
    expect(
      readConsolidateModelSettings({
        consolidateThinkingLevel: "turbo",
        consolidateMaxOutputTokens: "4",
        consolidateTimeoutMs: "1000",
      }),
    ).toEqual({
      thinkingLevel: DEFAULT_CONSOLIDATE_THINKING_LEVEL,
      maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      timeoutMs: DEFAULT_CONSOLIDATE_TIMEOUT_MS,
    });
    expect(assertConsolidateTimeoutMs("")).toBeUndefined();
    expect(assertConsolidateTimeoutMs("180000")).toBe("180000");
    expect(() => assertConsolidateTimeoutMs("4")).toThrow(/consolidateTimeoutMs/);
  });

  it("carries a short name forward when the page is full", async () => {
    const store = pageStore([entity({ id: "nicolas" })], {
      pageFull: true,
      pageIds: ["nicolas"],
    });
    const queue = jobs();
    await consolidateMemoryGraph(
      deps(async function* () {}, queue),
      payload,
      store,
    );
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.name).toBe("memory.consolidate");
    if (job?.name !== "memory.consolidate") throw new Error("expected memory.consolidate");
    expect(job.payload.afterId).toBe("nicolas");
    expect(job.payload.carryIds).toEqual(["nicolas"]);
    expect(job.payload.automatic).toBeUndefined();
    expect(store.merges).toEqual([]);
  });

  it("keeps an automatic run automatic when it reschedules", async () => {
    const store = pageStore([entity({ id: "nicolas" })], {
      pageFull: true,
      pageIds: ["nicolas"],
    });
    const queue = jobs();
    await consolidateMemoryGraph(
      deps(async function* () {}, queue),
      { ...payload, automatic: true },
      store,
    );
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.name).toBe("memory.consolidate");
    if (job?.name !== "memory.consolidate") throw new Error("expected memory.consolidate");
    expect(job.payload.automatic).toBe(true);
    expect(job.payload.scheduleGeneration).toBe(1);
  });

  it("continues the page when one confirmation is aborted and retries only that pair", async () => {
    const calls: string[] = [];
    const run: AgentRuntime["run"] = async function* (request, context) {
      calls.push(request.prompt);
      if (calls.length === 2) {
        await new Promise<void>((resolve) => {
          const signal = context?.signal;
          if (!signal || signal.aborted) {
            resolve();
            return;
          }
          signal.addEventListener("abort", () => resolve(), { once: true });
        });
        throw new Error("Request was aborted");
      }
      yield { type: "done" as const, text: '{"same":true}' };
    };
    const people = ["Ada", "Bea", "Cleo"].map((name) => [
      entity({ id: `${name}-short`, name, normName: name.toLowerCase() }),
      entity({
        id: `${name}-full`,
        name: `${name} Example`,
        normName: `${name.toLowerCase()} example`,
      }),
    ]);
    const store = pageStore(people.flat());
    store.consolidateModelSettings = () => ({
      thinkingLevel: "low",
      maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      timeoutMs: 30,
    });
    const queue = jobs();
    const sink = createTestSink();
    const previousLogger = getLogger();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    try {
      await consolidateMemoryGraph(deps(run, queue), payload, store);
    } finally {
      installLogger(previousLogger);
    }
    expect(calls).toHaveLength(3);
    expect(store.merges.map((merge) => merge.dropId).sort()).toEqual(["Ada-short", "Cleo-short"]);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.name).toBe("memory.consolidate");
    if (job?.name !== "memory.consolidate") throw new Error("expected memory.consolidate");
    expect(job.payload.attempt).toBe(1);
    expect(job.payload.afterId).toBeUndefined();
    expect(job.payload.rejected ?? []).not.toContain("Bea-full\nBea-short");
    expect(job.availableAt).toBeInstanceOf(Date);
    expect(sink.events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          level: "warn",
          message: "memory.consolidate confirmation timed out",
          "job.outcome": "transient",
        }),
      ]),
    );
  });

  it("remembers a permanent model failure and still merges the next pair", async () => {
    let calls = 0;
    const run: AgentRuntime["run"] = async function* () {
      calls += 1;
      if (calls === 1) throw new Error("invalid api key");
      yield { type: "done" as const, text: '{"same":true}' };
    };
    const store = pageStore([
      entity({ id: "ada-short", name: "Ada", normName: "ada" }),
      entity({ id: "ada-full", name: "Ada Example", normName: "ada example" }),
      entity({ id: "bea-short", name: "Bea", normName: "bea" }),
      entity({ id: "bea-full", name: "Bea Example", normName: "bea example" }),
    ]);
    const queue = jobs();
    await consolidateMemoryGraph(deps(run, queue), payload, store);
    expect(calls).toBe(2);
    expect(store.merges.map((merge) => merge.dropId)).toEqual(["bea-short"]);
    expect(queue.enqueue).not.toHaveBeenCalled();
  });

  it("does not retry a confirmation that keeps timing out", async () => {
    const run: AgentRuntime["run"] = async function* (_request, context) {
      await new Promise<void>((resolve) => {
        const signal = context?.signal;
        if (!signal || signal.aborted) {
          resolve();
          return;
        }
        signal.addEventListener("abort", () => resolve(), { once: true });
      });
      throw new Error("Request was aborted");
    };
    const store = pageStore([
      entity({ id: "ada-short", name: "Ada", normName: "ada" }),
      entity({ id: "ada-full", name: "Ada Example", normName: "ada example" }),
    ]);
    store.consolidateModelSettings = () => ({
      thinkingLevel: "low",
      maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      timeoutMs: 20,
    });
    const queue = jobs();
    await consolidateMemoryGraph(
      deps(run, queue),
      { ...payload, attempt: MAX_CONSOLIDATE_ATTEMPTS - 1 },
      store,
    );
    expect(store.merges).toEqual([]);
    expect(queue.enqueue).not.toHaveBeenCalled();
  });

  it("logs an unreadable decision snippet at debug and declines that pair", async () => {
    let calls = 0;
    const run: AgentRuntime["run"] = async function* () {
      calls += 1;
      yield {
        type: "done" as const,
        text: calls === 1 ? "not a decision" : '{"same":true}',
      };
    };
    const store = pageStore([
      entity({ id: "ada-short", name: "Ada", normName: "ada" }),
      entity({ id: "ada-full", name: "Ada Example", normName: "ada example" }),
      entity({ id: "bea-short", name: "Bea", normName: "bea" }),
      entity({ id: "bea-full", name: "Bea Example", normName: "bea example" }),
    ]);
    const queue = jobs();
    const sink = createTestSink();
    const previousLogger = getLogger();
    installLogger(createLogger({ service: "test", level: "debug", sinks: [sink] }));
    try {
      await consolidateMemoryGraph(deps(run, queue), payload, store);
    } finally {
      installLogger(previousLogger);
    }
    expect(store.merges.map((merge) => merge.dropId)).toEqual(["bea-short"]);
    const debug = sink.events.find(
      (event) => event.message === "memory.consolidate unreadable decision text",
    );
    expect(debug).toMatchObject({
      level: "debug",
      "memory.decision_snippet": "not a decision",
    });
    expect(queue.enqueue).not.toHaveBeenCalled();
  });
});
