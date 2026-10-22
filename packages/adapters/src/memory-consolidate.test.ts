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
import type {
  ConsolidationDecline,
  MemoryConsolidateDeps,
  MemoryConsolidateStore,
} from "./memory-consolidate.js";
import {
  assertConsolidateTimeoutMs,
  chooseWinningType,
  classifyConsolidationPair,
  consolidateMemoryGraph,
  consolidationEntityFingerprint,
  DEFAULT_CONSOLIDATE_JOB_BUDGET_MS,
  DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
  DEFAULT_CONSOLIDATE_THINKING_LEVEL,
  DEFAULT_CONSOLIDATE_TIMEOUT_MS,
  declinedPairIdentity,
  MAX_CONSOLIDATE_LLM_CALLS,
  memoryBotIdFromNamespace,
  memoryConsolidatePrompt,
  parseConsolidationDecision,
  parseConsolidationTypeDecision,
  planCanonicalEntity,
  planCanonicalRename,
  previewMemoryConsolidation,
  readConsolidateCollapseRelationTypes,
  readConsolidateJobBudgetMs,
  readConsolidateMaxPairTimeouts,
  readConsolidateModelSettings,
  readConsolidateSkipTypes,
} from "./memory-consolidate.js";
import type {
  Neo4jConsolidationEntity,
  Neo4jDuplicateMerge,
  Neo4jEntityRename,
} from "./neo4j-memory-client.js";

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
  options?: {
    pageFull?: boolean;
    pageIds?: string[];
    aliases?: readonly string[];
    people?: Neo4jConsolidationEntity[];
  },
): MemoryConsolidateStore & {
  merges: Neo4jDuplicateMerge[];
  renames: Neo4jEntityRename[];
  declines: ConsolidationDecline[];
  forgotten: Array<{ leftId: string; rightId: string }>;
  deferred: Array<ConsolidationDecline & { timeouts: number }>;
  collapses: Array<{
    spaceId: string;
    ids: string[];
    ns?: string;
    scope?: "page" | "space";
    dryRun?: boolean;
  }>;
} {
  const merges: Neo4jDuplicateMerge[] = [];
  const renames: Neo4jEntityRename[] = [];
  const declines: ConsolidationDecline[] = [];
  const forgotten: Array<{ leftId: string; rightId: string }> = [];
  const deferred: Array<ConsolidationDecline & { timeouts: number }> = [];
  const collapses: Array<{
    spaceId: string;
    ids: string[];
    ns?: string;
    scope?: "page" | "space";
    dryRun?: boolean;
  }> = [];
  return {
    merges,
    renames,
    declines,
    forgotten,
    deferred,
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
    ownerAliases: () => options?.aliases ?? [],
    findPeopleByNorms: async () => options?.people ?? [],
    listDeclinedPairs: async (input) =>
      declines.filter(
        (decline) =>
          input.ids.includes(decline.leftId) &&
          input.ids.includes(decline.rightId) &&
          (!input.ns || decline.ns === input.ns),
      ),
    rememberDeclinedPair: async (input) => {
      const index = declines.findIndex(
        (decline) => decline.leftId === input.leftId && decline.rightId === input.rightId,
      );
      const stored = {
        leftId: input.leftId,
        rightId: input.rightId,
        ns: input.ns,
        botId: input.botId,
        leftFingerprint: input.leftFingerprint,
        rightFingerprint: input.rightFingerprint,
      };
      if (index >= 0) declines[index] = stored;
      else declines.push(stored);
    },
    forgetDeclinedPair: async (input) => {
      forgotten.push({ leftId: input.leftId, rightId: input.rightId });
      const index = declines.findIndex(
        (decline) => decline.leftId === input.leftId && decline.rightId === input.rightId,
      );
      if (index >= 0) declines.splice(index, 1);
    },
    listDeferredPairs: async (input) =>
      deferred.filter(
        (marker) =>
          input.ids.includes(marker.leftId) &&
          input.ids.includes(marker.rightId) &&
          (!input.ns || marker.ns === input.ns),
      ),
    notePairTimeout: async (input) => {
      const index = deferred.findIndex(
        (marker) => marker.leftId === input.leftId && marker.rightId === input.rightId,
      );
      if (index >= 0) {
        const current = deferred[index];
        if (!current) return 1;
        current.timeouts += 1;
        current.leftFingerprint = input.leftFingerprint;
        current.rightFingerprint = input.rightFingerprint;
        return current.timeouts;
      }
      deferred.push({
        leftId: input.leftId,
        rightId: input.rightId,
        ns: input.ns,
        botId: input.botId,
        leftFingerprint: input.leftFingerprint,
        rightFingerprint: input.rightFingerprint,
        timeouts: 1,
      });
      return 1;
    },
    forgetDeferredPair: async (input) => {
      const index = deferred.findIndex(
        (marker) => marker.leftId === input.leftId && marker.rightId === input.rightId,
      );
      if (index >= 0) deferred.splice(index, 1);
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
  subjectName: string | null = null,
): MemoryConsolidateDeps & { jobs: ReturnType<typeof jobs> } {
  return {
    runtime: runtime(run, scripted),
    prisma: { usageRecord: { create: vi.fn() } } as unknown as Pick<PrismaClient, "usageRecord">,
    jobs: queue,
    resolveModel: vi.fn(async () => model),
    resolveSubjectName: async () => subjectName,
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
    expect(
      classifyConsolidationPair(runner, stranger, {
        subjectNorm: "ada lovelace",
        aliases: new Set(["runner"]),
      }),
    ).toEqual({ action: "confirm", reason: "role" });
    const titled = entity({ id: "titled", name: "The Runner", normName: "the runner" });
    expect(classifyConsolidationPair(titled, stranger)).toEqual({ action: "skip", reason: "weak" });
    expect(
      classifyConsolidationPair(titled, stranger, {
        subjectNorm: "ada lovelace",
        aliases: new Set(["the runner"]),
      }),
    ).toEqual({ action: "confirm", reason: "role" });
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
      { spaceId: "space-1", ids: ["ada"], scope: "space" },
      { spaceId: "space-1", ids: ["ada"], scope: "page" },
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
    expect(store.collapses.map((call) => call.scope)).toEqual(["space", "page"]);
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

  it("asks to merge an owner alias that shares nothing, including one loaded off the page", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"same":true}' };
    });
    const owner = entity({
      id: "ada",
      name: "Ada Lovelace",
      normName: "ada lovelace",
      degree: 1,
    });
    const runner = entity({
      id: "runner",
      name: "Runner",
      normName: "runner",
      degree: 4,
    });
    const together = pageStore([runner, owner], { aliases: ["runner"] });
    await consolidateMemoryGraph(deps(run, jobs(), false, "Ada Lovelace"), payload, together);
    expect(run).toHaveBeenCalledTimes(1);
    expect(together.merges[0]).toMatchObject({
      keepId: "ada",
      dropId: "runner",
      name: "Ada Lovelace",
    });

    const declined = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"same":false}' };
    });
    const refused = pageStore([runner], { aliases: ["runner"], people: [owner] });
    await consolidateMemoryGraph(deps(declined, jobs(), false, "Ada Lovelace"), payload, refused);
    expect(declined).toHaveBeenCalledTimes(1);
    expect(refused.merges).toEqual([]);

    const offPage = pageStore([runner], { aliases: ["runner"], people: [owner] });
    await consolidateMemoryGraph(deps(run, jobs(), false, "Ada Lovelace"), payload, offPage);
    expect(offPage.merges[0]).toMatchObject({ keepId: "ada", dropId: "runner" });
  });

  it("merges a confirmed cross-type pair when the model omits the type", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"same":true}' };
    });
    const store = pageStore([
      entity({
        id: "vigie-object",
        type: "Object",
        name: "Vigierisc /ATRISC",
        normName: "vigierisc atrisc",
        factIds: ["fact-1"],
      }),
      entity({
        id: "vigie-org",
        type: "Organization",
        name: "VigieRisc",
        normName: "vigierisc",
        degree: 6,
        factIds: ["fact-2", "fact-3"],
      }),
    ]);
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(run).toHaveBeenCalledTimes(1);
    expect(store.merges).toEqual([
      expect.objectContaining({
        keepId: "vigie-org",
        dropId: "vigie-object",
        type: "Organization",
        sourceTypes: ["Object", "Organization"],
      }),
    ]);
    expect(store.merges[0]?.name).toBe("Vigierisc /ATRISC");
    expect(store.merges[0]?.aliases).toContain("VigieRisc");
    expect(store.merges[0]?.aliasNorms).toContain("vigierisc");
  });

  it("does not write during a dry run and is stable when repeated", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"same":true,"type":"Organization"}' };
    });
    const entities = [
      entity({
        id: "vigie-object",
        type: "Object",
        name: "Vigierisc /ATRISC",
        normName: "vigierisc atrisc",
      }),
      entity({
        id: "vigie-org",
        type: "Organization",
        name: "VigieRisc",
        normName: "vigierisc",
        factIds: ["fact-1"],
      }),
    ];
    const store = pageStore(entities);
    await consolidateMemoryGraph(deps(run), { ...payload, dryRun: true }, store);
    await consolidateMemoryGraph(deps(run), { ...payload, dryRun: true }, store);
    expect(run).not.toHaveBeenCalled();
    expect(store.merges).toEqual([]);
    expect(store.renames).toEqual([]);
    expect(store.collapses.every((call) => call.dryRun && call.scope === "space")).toBe(true);
    const preview = await previewMemoryConsolidation(store, { spaceId: "space-1" });
    expect(preview.confirms).toEqual([
      expect.objectContaining({
        leftName: "Vigierisc /ATRISC",
        rightName: "VigieRisc",
        type: "Organization",
        reason: "type-conflict",
      }),
    ]);
    expect(preview.merges).toEqual([]);
    const again = await previewMemoryConsolidation(store, { spaceId: "space-1" });
    expect(again.confirms).toEqual(preview.confirms);
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

  it("confirms VigieRisc across types when a slash segment matches", () => {
    const project = entity({
      id: "vigie-object",
      type: "Object",
      name: "Vigierisc /ATRISC",
      normName: "vigierisc atrisc",
      degree: 2,
      factIds: ["fact-1"],
    });
    const org = entity({
      id: "vigie-org",
      type: "Organization",
      name: "VigieRisc",
      normName: "vigierisc",
      degree: 6,
      factIds: ["fact-2", "fact-3"],
    });
    expect(classifyConsolidationPair(project, org)).toEqual({
      action: "confirm",
      reason: "type-conflict",
    });
    expect(classifyConsolidationPair({ ...project, type: "Organization" }, org)).toEqual({
      action: "merge",
      reason: "alias",
    });
    const accented = entity({
      id: "school",
      type: "Organization",
      name: "École",
      normName: "école",
    });
    const plain = entity({
      id: "school-plain",
      type: "Object",
      name: "Ecole",
      normName: "ecole",
    });
    expect(classifyConsolidationPair(accented, plain)).toEqual({
      action: "confirm",
      reason: "type-conflict",
    });
    expect(chooseWinningType(project, org, "Object")).toBe("Organization");
    const plan = planCanonicalEntity(project, org, "Organization");
    expect(plan.keep.id).toBe("vigie-org");
    expect(plan.type).toBe("Organization");
    expect(plan.name).toBe("Vigierisc /ATRISC");
    expect(plan.aliases).toContain("VigieRisc");
    expect(plan.aliasNorms).toContain("vigierisc");
    expect(plan.aliasNorms).not.toContain("vigierisc atrisc");
  });

  it("does not treat a shared word as a cross-type name", () => {
    expect(
      classifyConsolidationPair(
        entity({ id: "person", name: "Nicolas", normName: "nicolas" }),
        entity({
          id: "org",
          type: "Organization",
          name: "Nicolas Dupont",
          normName: "nicolas dupont",
        }),
      ),
    ).toEqual({ action: "skip", reason: "type" });
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

  it("keeps a 20 minute consolidate job budget unless a bounded value is set", () => {
    expect(DEFAULT_CONSOLIDATE_JOB_BUDGET_MS).toBe(1_200_000);
    expect(readConsolidateJobBudgetMs({})).toBe(DEFAULT_CONSOLIDATE_JOB_BUDGET_MS);
    expect(readConsolidateJobBudgetMs({ MEMORY_CONSOLIDATE_JOB_BUDGET_MS: "" })).toBe(
      DEFAULT_CONSOLIDATE_JOB_BUDGET_MS,
    );
    expect(readConsolidateJobBudgetMs({ MEMORY_CONSOLIDATE_JOB_BUDGET_MS: "nope" })).toBe(
      DEFAULT_CONSOLIDATE_JOB_BUDGET_MS,
    );
    expect(readConsolidateJobBudgetMs({ MEMORY_CONSOLIDATE_JOB_BUDGET_MS: "1000" })).toBe(
      DEFAULT_CONSOLIDATE_JOB_BUDGET_MS,
    );
    expect(readConsolidateJobBudgetMs({ MEMORY_CONSOLIDATE_JOB_BUDGET_MS: "3600000" })).toBe(
      DEFAULT_CONSOLIDATE_JOB_BUDGET_MS,
    );
    expect(readConsolidateJobBudgetMs({ MEMORY_CONSOLIDATE_JOB_BUDGET_MS: "60000" })).toBe(60_000);
  });

  it("stops the page at the consolidate time budget and continues the same page", async () => {
    const previousBudget = process.env.MEMORY_CONSOLIDATE_JOB_BUDGET_MS;
    process.env.MEMORY_CONSOLIDATE_JOB_BUDGET_MS = "60000";
    let now = 0;
    const nowSpy = vi.spyOn(Date, "now").mockImplementation(() => now);
    let calls = 0;
    const run: AgentRuntime["run"] = async function* () {
      calls += 1;
      now += 60_000;
      yield { type: "done" as const, text: '{"same":true}' };
    };
    const store = pageStore([
      entity({ id: "short" }),
      entity({ id: "full", name: "Nicolas Tremblier", normName: "nicolas tremblier" }),
      entity({ id: "ada", name: "Ada", normName: "ada" }),
      entity({ id: "ada-full", name: "Ada Lovelace", normName: "ada lovelace" }),
    ]);
    const queue = jobs();
    const sink = createTestSink();
    const previousLogger = getLogger();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    try {
      await consolidateMemoryGraph(deps(run, queue), payload, store);
    } finally {
      nowSpy.mockRestore();
      installLogger(previousLogger);
      if (previousBudget === undefined) delete process.env.MEMORY_CONSOLIDATE_JOB_BUDGET_MS;
      else process.env.MEMORY_CONSOLIDATE_JOB_BUDGET_MS = previousBudget;
    }
    expect(calls).toBe(1);
    expect(store.merges).toHaveLength(1);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.name).toBe("memory.consolidate");
    if (job?.name !== "memory.consolidate") throw new Error("expected memory.consolidate");
    expect(job.payload.attempt).toBe(0);
    expect(job.payload.scheduleGeneration).toBe(1);
    expect(job.payload.afterId).toBeUndefined();
    expect(sink.events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          message: "memory.consolidate finished a page",
          "memory.follow_up": true,
          "memory.llm_calls": 1,
        }),
        expect.objectContaining({
          message: "memory.consolidate stopped at the time budget",
          "memory.budget_ms": 60_000,
          "memory.follow_up": true,
        }),
      ]),
    );
  });

  it("reads consolidate model settings and falls back when they are blank or invalid", () => {
    expect(DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS).toBe(4_096);
    expect(DEFAULT_CONSOLIDATE_TIMEOUT_MS).toBe(180_000);
    expect(readConsolidateModelSettings({}, {})).toEqual({
      thinkingLevel: DEFAULT_CONSOLIDATE_THINKING_LEVEL,
      maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      timeoutMs: DEFAULT_CONSOLIDATE_TIMEOUT_MS,
    });
    expect(
      readConsolidateModelSettings(
        {},
        {
          MEMORY_CONSOLIDATE_THINKING_LEVEL: "high",
          MEMORY_CONSOLIDATE_MAX_OUTPUT_TOKENS: "8192",
          MEMORY_CONSOLIDATE_TIMEOUT_MS: "300000",
        },
      ),
    ).toEqual({ thinkingLevel: "high", maxOutputTokens: 8192, timeoutMs: 300_000 });
    expect(
      readConsolidateModelSettings(
        { consolidateThinkingLevel: "low", consolidateTimeoutMs: "180000" },
        {
          MEMORY_CONSOLIDATE_THINKING_LEVEL: "high",
          MEMORY_CONSOLIDATE_TIMEOUT_MS: "300000",
        },
      ),
    ).toEqual({
      thinkingLevel: "low",
      maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      timeoutMs: 180_000,
    });
    expect(
      readConsolidateModelSettings(
        {},
        {
          MEMORY_CONSOLIDATE_THINKING_LEVEL: "turbo",
          MEMORY_CONSOLIDATE_MAX_OUTPUT_TOKENS: "4",
          MEMORY_CONSOLIDATE_TIMEOUT_MS: "nope",
        },
      ),
    ).toEqual({
      thinkingLevel: DEFAULT_CONSOLIDATE_THINKING_LEVEL,
      maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      timeoutMs: DEFAULT_CONSOLIDATE_TIMEOUT_MS,
    });
    expect(
      readConsolidateModelSettings(
        {
          consolidateThinkingLevel: " minimal ",
          consolidateMaxOutputTokens: "2048",
          consolidateTimeoutMs: " 180000 ",
        },
        {},
      ),
    ).toEqual({ thinkingLevel: "minimal", maxOutputTokens: 2048, timeoutMs: 180_000 });
    expect(
      readConsolidateModelSettings(
        {
          consolidateThinkingLevel: "turbo",
          consolidateMaxOutputTokens: "4",
          consolidateTimeoutMs: "1000",
        },
        {},
      ),
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
    expect(queue.enqueue).not.toHaveBeenCalled();
    expect(store.declines).toEqual([]);
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

  it("advances a full page instead of retrying a timed-out pair", async () => {
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
    const store = pageStore(
      [
        entity({ id: "ada-short", name: "Ada", normName: "ada" }),
        entity({ id: "ada-full", name: "Ada Example", normName: "ada example" }),
      ],
      { pageFull: true, pageIds: ["ada-short", "ada-full"] },
    );
    store.consolidateModelSettings = () => ({
      thinkingLevel: "low",
      maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      timeoutMs: 20,
    });
    const queue = jobs();
    await consolidateMemoryGraph(deps(run, queue), payload, store);
    expect(store.merges).toEqual([]);
    expect(store.declines).toEqual([]);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.name).toBe("memory.consolidate");
    if (job?.name !== "memory.consolidate") throw new Error("expected memory.consolidate");
    expect(job.payload.afterId).toBe("ada-full");
    expect(job.payload.attempt).toBe(0);
    expect(job.payload.skipped).toEqual(["ada-full\nada-short"]);
    expect(job.payload.rejected ?? []).not.toContain("ada-full\nada-short");
    expect(job.availableAt).toBeUndefined();
  });

  it("asks the next pairs after a page of confirmation timeouts", async () => {
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
    const people = ["Ada", "Bea", "Cleo", "Dora", "Eve"].flatMap((name) => [
      entity({ id: `${name}-short`, name, normName: name.toLowerCase() }),
      entity({
        id: `${name}-full`,
        name: `${name} Example`,
        normName: `${name.toLowerCase()} example`,
      }),
    ]);
    const store = pageStore(people);
    store.consolidateModelSettings = () => ({
      thinkingLevel: "low",
      maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      timeoutMs: 20,
    });
    const queue = jobs();
    await consolidateMemoryGraph(deps(run, queue), payload, store);
    expect(store.merges).toEqual([]);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.name).toBe("memory.consolidate");
    if (job?.name !== "memory.consolidate") throw new Error("expected memory.consolidate");
    expect(job.payload.afterId).toBeUndefined();
    expect(job.payload.attempt).toBe(0);
    expect(job.payload.skipped).toHaveLength(MAX_CONSOLIDATE_LLM_CALLS);
    const second = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"same":true}' };
    });
    const next = jobs();
    await consolidateMemoryGraph(deps(second, next), job.payload, store);
    expect(second).toHaveBeenCalledTimes(1);
    expect(store.merges.map((merge) => merge.dropId)).toEqual(["Eve-short"]);
    expect(next.enqueue).not.toHaveBeenCalled();
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
    await consolidateMemoryGraph(deps(run, queue), payload, store);
    expect(store.merges).toEqual([]);
    expect(store.declines).toEqual([]);
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

  it("does not ask again after the model refuses, until a name, type, or alias changes", async () => {
    const run = vi.fn<AgentRuntime["run"]>(async function* () {
      yield { type: "done" as const, text: '{"same":false}' };
    });
    const person = entity({ id: "apple-person", name: "Apple", normName: "apple" });
    const org = entity({
      id: "apple-org",
      ns: "isolated:bot-1",
      type: "Organization",
      name: "Apple",
      normName: "apple",
    });
    person.ns = "isolated:bot-1";
    const store = pageStore([person, org]);
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(run).toHaveBeenCalledTimes(1);
    expect(store.merges).toEqual([]);
    expect(store.declines).toEqual([
      expect.objectContaining({
        leftId: "apple-org",
        rightId: "apple-person",
        ns: "isolated:bot-1",
        botId: "bot-1",
        leftFingerprint: consolidationEntityFingerprint(org),
        rightFingerprint: consolidationEntityFingerprint(person),
      }),
    ]);
    expect(memoryBotIdFromNamespace("shared")).toBe("");
    expect(memoryBotIdFromNamespace("history:bot-9")).toBe("bot-9");

    run.mockClear();
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(run).not.toHaveBeenCalled();

    org.aliasNorms = ["apple inc"];
    org.aliases = ["Apple Inc"];
    await consolidateMemoryGraph(deps(run), payload, store);
    expect(store.forgotten).toEqual([{ leftId: "apple-org", rightId: "apple-person" }]);
    expect(run).toHaveBeenCalledTimes(1);
    expect(declinedPairIdentity(person, org).botId).toBe("bot-1");
  });

  it("does not pair path segments or events whose dates differ", () => {
    expect(readConsolidateSkipTypes({})).toEqual(new Set(["Event"]));
    expect(readConsolidateSkipTypes({ MEMORY_CONSOLIDATE_SKIP_TYPES: "-" })).toEqual(new Set());
    expect(readConsolidateMaxPairTimeouts({})).toBe(2);
    expect(readConsolidateMaxPairTimeouts({ MEMORY_CONSOLIDATE_MAX_PAIR_TIMEOUTS: "1" })).toBe(1);
    expect(readConsolidateCollapseRelationTypes({})).toBe(false);
    expect(
      readConsolidateCollapseRelationTypes({ MEMORY_CONSOLIDATE_COLLAPSE_RELATION_TYPES: "on" }),
    ).toBe(true);
    const path = entity({
      id: "path",
      type: "Object",
      name: "shared/fabriq/prep-notes",
      normName: "shared fabriq prep notes",
    });
    const segment = entity({ id: "fabriq", type: "Object", name: "fabriq", normName: "fabriq" });
    expect(classifyConsolidationPair(path, segment)).toEqual({ action: "skip", reason: "weak" });
    const monday = entity({
      id: "hiit-22",
      type: "Event",
      name: "Brief du 22/10 9h",
      normName: "brief du 22 10 9h",
    });
    const tuesday = entity({
      id: "hiit-23",
      type: "Event",
      name: "Brief du 23/10 9h",
      normName: "brief du 23 10 9h",
    });
    expect(classifyConsolidationPair(monday, tuesday, null, { skipTypes: new Set() })).toEqual({
      action: "skip",
      reason: "numeric",
    });
    expect(classifyConsolidationPair(monday, tuesday)).toEqual({
      action: "skip",
      reason: "numeric",
    });
    const morning = entity({
      id: "hiit-am",
      type: "Event",
      name: "Test HIIT matin",
      normName: "test hiit matin",
    });
    const evening = entity({
      id: "hiit-pm",
      type: "Event",
      name: "Test HIIT soir",
      normName: "test hiit soir",
    });
    expect(classifyConsolidationPair(morning, evening)).toEqual({
      action: "skip",
      reason: "skipped-type",
    });
  });

  it("skips a pair after the timeout cap and still asks it once before that", async () => {
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
    const calls = vi.fn(run);
    await consolidateMemoryGraph(deps(calls), payload, store);
    expect(calls).toHaveBeenCalledTimes(1);
    expect(store.deferred).toHaveLength(1);
    expect(store.deferred[0]?.timeouts).toBe(1);
    expect(store.declines).toEqual([]);
    calls.mockClear();
    await consolidateMemoryGraph(deps(calls), payload, store);
    expect(calls).toHaveBeenCalledTimes(1);
    expect(store.deferred[0]?.timeouts).toBe(2);
    calls.mockClear();
    await consolidateMemoryGraph(deps(calls), payload, store);
    expect(calls).not.toHaveBeenCalled();
    expect(store.declines).toEqual([]);
  });

  it("advances a full page when timeouts use up the confirmation budget", async () => {
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
    const people = ["Ada", "Bea", "Cleo", "Dora", "Eve"].flatMap((name) => [
      entity({ id: `${name}-short`, name, normName: name.toLowerCase() }),
      entity({
        id: `${name}-full`,
        name: `${name} Example`,
        normName: `${name.toLowerCase()} example`,
      }),
    ]);
    const store = pageStore(people, { pageFull: true });
    store.consolidateModelSettings = () => ({
      thinkingLevel: "low",
      maxOutputTokens: DEFAULT_CONSOLIDATE_MAX_OUTPUT_TOKENS,
      timeoutMs: 20,
    });
    const queue = jobs();
    await consolidateMemoryGraph(deps(run, queue), payload, store);
    const job = queue.enqueue.mock.calls[0]?.[0];
    expect(job?.name).toBe("memory.consolidate");
    if (job?.name !== "memory.consolidate") throw new Error("expected memory.consolidate");
    expect(job.payload.afterId).toBe("Eve-full");
    expect(job.payload.skipped).toHaveLength(MAX_CONSOLIDATE_LLM_CALLS);
    expect(store.declines).toEqual([]);
    expect(store.deferred).toHaveLength(MAX_CONSOLIDATE_LLM_CALLS);
  });
});
