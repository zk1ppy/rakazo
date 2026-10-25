import { randomUUID } from "node:crypto";
import type { AdapterContext, BackgroundJob, JobPublisher } from "@rakazo/adapter-kit";
import { createLogger, createTestSink, getLogger, installLogger } from "@rakazo/logging";
import { describe, expect, it, vi } from "vitest";
import { createMemoryProvider } from "./memory-provider-factory.js";
import type { Neo4jMemoryDriver, Neo4jMemoryRows, Neo4jRunOptions } from "./neo4j-memory-client.js";
import { NEO4J_MEMORY_SCHEMA_STATEMENTS } from "./neo4j-memory-client.js";
import {
  acquireNeo4jMemoryDriver,
  neo4jRequiresDeploymentOwner,
  classifyNeo4jConnectionSettings,
  createNeo4jProvider,
  Neo4jMemoryProvider,
  parseNeo4jUri,
  prepareNeo4jConnection,
} from "./neo4j-memory-provider.js";
import { MemoryProviderPrivateEndpointRequiredError } from "./serenity-memory-provider.js";

const context: AdapterContext = {
  operationId: "op-1",
  traceId: "trace-1",
  spaceId: "space-1",
  userId: "user-1",
  botId: "bot-1",
  runId: "run-1",
  signal: new AbortController().signal,
};

class FakeDriver implements Neo4jMemoryDriver {
  readonly calls: Array<{
    cypher: string;
    params: Record<string, unknown>;
    options: Neo4jRunOptions;
  }> = [];
  rows: Neo4jMemoryRows[] = [];
  closed = false;
  databases: string[] = [];

  verifyConnectivity(options: { database: string }): Promise<void> {
    this.databases.push(options.database);
    return Promise.resolve();
  }

  run(
    cypher: string,
    params: Record<string, unknown>,
    options: Neo4jRunOptions,
  ): Promise<Neo4jMemoryRows> {
    this.calls.push({ cypher, params, options });
    if (options.schema) return Promise.resolve({ records: [] });
    return Promise.resolve(this.rows.shift() ?? { records: [] });
  }

  close(): Promise<void> {
    this.closed = true;
    return Promise.resolve();
  }
}

function row(values: Record<string, unknown>) {
  return { get: (key: string) => values[key] };
}

function queries(driver: FakeDriver) {
  return driver.calls.filter((call) => !call.options.schema);
}

function providerFor(driver: FakeDriver, options?: { jobs?: JobPublisher; extract?: boolean }) {
  return new Neo4jMemoryProvider(
    {
      uri: "bolt://127.0.0.1:7687",
      database: "neo4j",
      username: "neo4j",
      password: "secret",
      endpointTrust: "private",
    },
    () => driver,
    options,
  );
}

describe("Neo4jMemoryProvider", () => {
  it("describes the semantic memory contract", () => {
    const created = createNeo4jProvider(
      { uri: "neo4j+s://db.example.test", username: "reader" },
      { password: "secret" },
    );
    expect(created).toBeInstanceOf(Neo4jMemoryProvider);
    if (!(created instanceof Neo4jMemoryProvider)) throw new Error("expected Neo4j");
    expect(created.dreamEnabled()).toBe(false);
    const dreaming = createNeo4jProvider(
      { uri: "neo4j+s://db.example.test", username: "reader", dream: "on" },
      { password: "secret" },
    );
    if (!(dreaming instanceof Neo4jMemoryProvider)) throw new Error("expected Neo4j");
    expect(dreaming.dreamEnabled()).toBe(true);
    expect(created.describe()).toEqual({
      id: "neo4j",
      contractVersion: "1",
      adapterVersion: "0.1.0",
      capabilities: { recall: true, save: true, purgeHistory: true, sharedScope: true },
    });
    expect(
      createMemoryProvider(
        "neo4j",
        { uri: "bolt://localhost:7687", username: "neo4j" },
        { password: "secret" },
      ).describe().id,
    ).toBe("neo4j");
  });

  it("normalizes URIs and rejects credentials, queries, and unknown schemes", () => {
    expect(parseNeo4jUri("bolt://LOCALHOST:7687").uri).toBe("bolt://localhost:7687");
    expect(parseNeo4jUri("neo4j+s://DB.Example.test").uri).toBe("neo4j+s://db.example.test:7687");
    expect(parseNeo4jUri("neo4j+ssc://[::1]").encrypted).toBe(true);
    expect(() => parseNeo4jUri("bolt://user:secret@localhost")).toThrow(/credentials/);
    expect(() => parseNeo4jUri("bolt://localhost:7687?x=1")).toThrow(/query/);
    expect(() => parseNeo4jUri("http://localhost:7687")).toThrow(/scheme/);
  });

  it("requires a deployment owner for private and cluster-local hosts only", () => {
    expect(neo4jRequiresDeploymentOwner({ uri: "bolt://127.0.0.1:7687" })).toBe(
      true,
    );
    expect(neo4jRequiresDeploymentOwner({ uri: "bolt://neo4j:7687" })).toBe(true);
    expect(
      neo4jRequiresDeploymentOwner({ uri: "bolt://host.docker.internal:7687" }),
    ).toBe(true);
    expect(
      neo4jRequiresDeploymentOwner({
        uri: "neo4j://graph.svc.cluster.local:7687",
      }),
    ).toBe(true);
    expect(
      neo4jRequiresDeploymentOwner({
        uri: "neo4j+s://example.databases.neo4j.io",
      }),
    ).toBe(false);
    expect(
      neo4jRequiresDeploymentOwner({
        uri: "neo4j+s://db.example.test",
        endpointTrust: "private",
      }),
    ).toBe(true);
    expect(neo4jRequiresDeploymentOwner({})).toBe(false);
  });

  it("classifies private DNS without opening a driver", async () => {
    let opened = false;
    const classified = await classifyNeo4jConnectionSettings(
      { uri: "neo4j+s://db.example.test", username: "neo4j", database: "" },
      {
        resolveHostname: async () => [{ address: "10.8.0.2", family: 4 }],
        openDriver: () => {
          opened = true;
          return new FakeDriver();
        },
      },
    );
    expect(classified).toEqual({
      uri: "neo4j+s://db.example.test:7687",
      database: "neo4j",
      username: "neo4j",
      endpointTrust: "private",
    });
    expect(opened).toBe(false);
    expect(neo4jRequiresDeploymentOwner(classified)).toBe(true);
  });

  it("keeps extraction settings and rejects an invalid chunk size", async () => {
    const classified = await classifyNeo4jConnectionSettings(
      {
        uri: "neo4j+s://db.example.test",
        username: "neo4j",
        extract: "false",
        maxChunkChars: "800",
      },
      { resolveHostname: async () => [{ address: "203.0.113.10", family: 4 }] },
    );
    expect(classified).toMatchObject({
      uri: "neo4j+s://db.example.test:7687",
      extract: "false",
      maxChunkChars: "800",
    });
    const dreamed = await classifyNeo4jConnectionSettings(
      {
        uri: "neo4j+s://db.example.test",
        username: "neo4j",
        dream: " true ",
      },
      { resolveHostname: async () => [{ address: "203.0.113.10", family: 4 }] },
    );
    expect(dreamed.dream).toBe("true");
    expect(
      (
        await classifyNeo4jConnectionSettings(
          { uri: "neo4j+s://db.example.test", username: "neo4j" },
          { resolveHostname: async () => [{ address: "203.0.113.10", family: 4 }] },
        )
      ).dream,
    ).toBeUndefined();
    await expect(
      classifyNeo4jConnectionSettings(
        { uri: "neo4j+s://db.example.test", username: "neo4j", maxChunkChars: "10" },
        { resolveHostname: async () => [{ address: "203.0.113.10", family: 4 }] },
      ),
    ).rejects.toThrow(/maxChunkChars/);
  });

  it("keeps a complete extraction model and drops a partial or oversized one", async () => {
    const publicHost = { resolveHostname: async () => [{ address: "203.0.113.10", family: 4 }] };
    const classified = await classifyNeo4jConnectionSettings(
      {
        uri: "neo4j+s://db.example.test",
        username: "neo4j",
        extractModelProvider: " synthetic ",
        extractModelId: " model-a ",
      },
      publicHost,
    );
    expect(classified).toMatchObject({
      extractModelProvider: "synthetic",
      extractModelId: "model-a",
    });

    const partial = await classifyNeo4jConnectionSettings(
      {
        uri: "neo4j+s://db.example.test",
        username: "neo4j",
        extractModelProvider: "synthetic",
      },
      publicHost,
    );
    expect(partial.extractModelProvider).toBeUndefined();
    expect(partial.extractModelId).toBeUndefined();

    const oversized = await classifyNeo4jConnectionSettings(
      {
        uri: "neo4j+s://db.example.test",
        username: "neo4j",
        extractModelProvider: "x".repeat(81),
        extractModelId: "model-a",
      },
      publicHost,
    );
    expect(oversized.extractModelProvider).toBeUndefined();
  });

  it("keeps consolidate model settings and rejects an invalid budget", async () => {
    const publicHost = { resolveHostname: async () => [{ address: "203.0.113.10", family: 4 }] };
    const classified = await classifyNeo4jConnectionSettings(
      {
        uri: "neo4j+s://db.example.test",
        username: "neo4j",
        consolidateThinkingLevel: " minimal ",
        consolidateMaxOutputTokens: "2048",
        consolidateTimeoutMs: " 180000 ",
      },
      publicHost,
    );
    expect(classified).toMatchObject({
      consolidateThinkingLevel: "minimal",
      consolidateMaxOutputTokens: "2048",
      consolidateTimeoutMs: "180000",
    });
    await expect(
      classifyNeo4jConnectionSettings(
        {
          uri: "neo4j+s://db.example.test",
          username: "neo4j",
          consolidateMaxOutputTokens: "4",
        },
        publicHost,
      ),
    ).rejects.toThrow(/consolidateMaxOutputTokens/);
    await expect(
      classifyNeo4jConnectionSettings(
        {
          uri: "neo4j+s://db.example.test",
          username: "neo4j",
          consolidateTimeoutMs: "1000",
        },
        publicHost,
      ),
    ).rejects.toThrow(/consolidateTimeoutMs/);
  });

  it("refuses a private endpoint before connecting when the caller is not the owner", async () => {
    const driver = new FakeDriver();
    await expect(
      prepareNeo4jConnection(
        { uri: "bolt://127.0.0.1:7687", username: "neo4j" },
        { password: "secret" },
        { openDriver: () => driver },
        { allowPrivateEndpoint: false },
      ),
    ).rejects.toBeInstanceOf(MemoryProviderPrivateEndpointRequiredError);
    expect(driver.databases).toEqual([]);
  });

  it("rejects cleartext URIs once DNS says the host is public", async () => {
    await expect(
      prepareNeo4jConnection(
        { uri: "bolt://db.example.test:7687", username: "neo4j" },
        { password: "secret" },
        { resolveHostname: async () => [{ address: "203.0.113.10", family: 4 }] },
      ),
    ).rejects.toThrow(/cleartext/);
  });

  it("verifies connectivity and creates the schema before saving settings", async () => {
    const driver = new FakeDriver();
    const prepared = await prepareNeo4jConnection(
      { uri: "neo4j+s://db.example.test", username: " neo4j ", database: "graph" },
      { password: "secret" },
      {
        resolveHostname: async () => [{ address: "203.0.113.10", family: 4 }],
        openDriver: () => driver,
      },
    );
    expect(prepared).toEqual({
      settings: {
        uri: "neo4j+s://db.example.test:7687",
        database: "graph",
        username: "neo4j",
      },
      credentials: { password: "secret" },
    });
    expect(driver.databases).toEqual(["graph"]);
    expect(driver.closed).toBe(true);
    expect(driver.calls.map((call) => call.cypher)).toEqual([...NEO4J_MEMORY_SCHEMA_STATEMENTS]);
    expect(JSON.stringify(prepared.settings)).not.toContain("secret");
  });

  it("saves a durable fact on an isolated or shared namespace and replays one run", async () => {
    const driver = new FakeDriver();
    const provider = providerFor(driver);
    driver.rows.push({ records: [row({ id: "fact-a" })] }, { records: [row({ id: "fact-a" })] });
    const request = {
      content: " Prefer metric units. ",
      scope: "isolated" as const,
      botId: "bot-1",
      source: { kind: "durable" as const },
    };
    await expect(provider.save(request, context)).resolves.toEqual({ ok: true, value: undefined });
    await provider.save(request, { ...context, operationId: "op-2" });
    const writes = queries(driver);
    expect(writes).toHaveLength(2);
    expect(writes[0]?.params).toMatchObject({
      spaceId: "space-1",
      ns: "isolated:bot-1",
      kind: "memory",
      text: "Prefer metric units.",
      provenance: "rakazo space:space-1 bot:bot-1",
      botId: "bot-1",
    });
    expect(writes[0]?.params.factId).toBe(writes[1]?.params.factId);
    expect(writes[0]?.cypher).toContain(":Rkz:Fact");
    expect(writes[0]?.cypher).toContain(":Rkz:Source");
    expect(JSON.stringify(writes[0]?.params)).not.toContain("secret");

    driver.rows.push({ records: [row({ id: "fact-b" })] });
    await provider.save({ ...request, scope: "shared" }, context);
    expect(queries(driver).at(-1)?.params.ns).toBe("shared");
    expect(driver.calls.filter((call) => call.options.schema)).toHaveLength(
      NEO4J_MEMORY_SCHEMA_STATEMENTS.length,
    );
  });

  it("stores chat compaction on a history source", async () => {
    const driver = new FakeDriver();
    driver.rows.push({ records: [row({ id: "fact-h" })] });
    await providerFor(driver).save(
      {
        content: "Talked about shipping.",
        scope: "isolated",
        botId: "bot-1",
        source: { kind: "history", generation: 4 },
      },
      context,
    );
    expect(queries(driver)[0]?.params).toMatchObject({
      kind: "chat",
      ns: "history:bot-1",
      generation: 4,
      uri: "rakazo://chat/space-1/bot-1/4",
      provenance: "rakazo space:space-1 bot:bot-1 history:4",
      threadId: null,
    });
    expect(queries(driver)).toHaveLength(1);
  });

  it("replaces the previous chat summary of the same thread", async () => {
    const driver = new FakeDriver();
    driver.rows.push({ records: [row({ id: "fact-h" })] }, { records: [row({ removed: 1 })] });
    await providerFor(driver).save(
      {
        content: "Talked about shipping and training.",
        scope: "isolated",
        botId: "bot-1",
        source: { kind: "history", generation: 4 },
      },
      { ...context, operationId: "history-compact:thread-1" },
    );
    const writes = queries(driver);
    expect(writes).toHaveLength(2);
    expect(writes[0]?.params.threadId).toBe("thread-1");
    expect(writes[1]?.cypher).toContain("threadId: $threadId");
    expect(writes[1]?.cypher).toContain("DETACH DELETE f");
    expect(writes[1]?.params).toMatchObject({
      spaceId: "space-1",
      botId: "bot-1",
      threadId: "thread-1",
      keepFactId: writes[0]?.params.factId,
    });
  });

  it("rejects empty and oversized facts without writing", async () => {
    const driver = new FakeDriver();
    const provider = providerFor(driver);
    await expect(
      provider.save(
        { content: "  ", scope: "isolated", botId: "bot-1", source: { kind: "durable" } },
        context,
      ),
    ).resolves.toEqual({ ok: false, error: "content is required" });
    await expect(
      provider.save(
        {
          content: "x".repeat(20_001),
          scope: "isolated",
          botId: "bot-1",
          source: { kind: "durable" },
        },
        context,
      ),
    ).resolves.toEqual({ ok: false, error: "content is too long" });
    expect(driver.calls).toHaveLength(0);
  });

  it("ranks recalled facts and searches shared memory together with this bot", async () => {
    const driver = new FakeDriver();
    driver.rows.push({
      records: [
        row({
          id: "fact-1",
          memory: "Ada founded the lab.",
          provenance: "rakazo space:space-1 bot:bot-1",
          entity: "Ada",
          updatedAt: "2026-01-02T00:00:00.000Z",
          score: 2,
        }),
      ],
    });
    const result = await providerFor(driver).recall(
      { query: "Ada", scope: "shared", botId: "bot-1", historyGeneration: 3, limit: 5 },
      context,
    );
    expect(result).toEqual({
      ok: true,
      value: [
        {
          id: "fact-1",
          memory: "Ada founded the lab.",
          score: 2,
          provenance: "rakazo space:space-1 bot:bot-1",
          entity: "Ada",
          updatedAt: "2026-01-02T00:00:00.000Z",
        },
      ],
    });
    expect(queries(driver)[0]?.params).toMatchObject({
      spaceId: "space-1",
      namespaces: ["shared", "isolated:bot-1", "history:bot-1"],
      historyNs: "history:bot-1",
      historyGeneration: 3,
      limit: 5,
    });
    expect(queries(driver)[0]?.params.q).toBe("Ada");
  });

  it("does not search history until a thread has compacted", async () => {
    const driver = new FakeDriver();
    driver.rows.push({ records: [] });
    await providerFor(driver).recall(
      { query: "c++ guide", scope: "isolated", botId: "bot-1", limit: 5 },
      context,
    );
    expect(queries(driver)[0]?.params).toMatchObject({
      namespaces: ["isolated:bot-1"],
      historyNs: null,
      historyGeneration: null,
      q: "c\\+\\+ guide",
    });
  });

  it("soft-forgets a fact in this bot's visible namespaces", async () => {
    const driver = new FakeDriver();
    driver.rows.push({ records: [row({ id: "fact-1", reason: "cleanup" })] });
    const result = await providerFor(driver).forget(
      { id: "fact-1", scope: "shared", botId: "bot-1", reason: " cleanup ", entity: "Ada" },
      context,
    );
    expect(result).toEqual({
      ok: true,
      value: { id: "fact-1", expired: true, reason: "cleanup" },
    });
    const call = queries(driver)[0];
    expect(call?.cypher).toContain("f.expiredAt = coalesce(f.expiredAt, $now)");
    expect(call?.params).toMatchObject({
      id: "fact-1",
      spaceId: "space-1",
      reason: "cleanup",
      namespaces: ["shared", "isolated:bot-1", "history:bot-1"],
    });

    driver.rows.push({ records: [] });
    await expect(
      providerFor(driver).forget({ id: "missing", scope: "shared", botId: "bot-1" }, context),
    ).resolves.toEqual({
      ok: true,
      value: { id: "missing", expired: false, reason: null },
    });
  });

  it("deletes chat sources for the requested compaction generations", async () => {
    const driver = new FakeDriver();
    await expect(
      providerFor(driver).purgeHistory({ botId: "bot-1", generations: [2, 3, 3, 1.5] }, context),
    ).resolves.toEqual({ ok: true, value: undefined });
    const writes = queries(driver);
    expect(writes).toHaveLength(2);
    expect(writes[0]?.params).toMatchObject({
      spaceId: "space-1",
      botId: "bot-1",
      generations: [2, 3],
    });
    expect(writes[0]?.cypher).toContain('kind: "chat"');

    const idle = new FakeDriver();
    await providerFor(idle).purgeHistory({ botId: "bot-1", generations: [] }, context);
    expect(idle.calls).toHaveLength(0);
  });

  it("closes the cached driver when the password or uri changes", async () => {
    const stamp = randomUUID();
    const opened: FakeDriver[] = [];
    const open = () => {
      const driver = new FakeDriver();
      opened.push(driver);
      return driver;
    };
    const base = {
      uri: `bolt://db-${stamp}.internal:7687`,
      database: "neo4j",
      username: "neo4j",
      password: `pool-one-${stamp}`,
      endpointTrust: "private" as const,
    };
    const first = await acquireNeo4jMemoryDriver(base, open);
    expect(await acquireNeo4jMemoryDriver({ ...base, database: "graphs" }, open)).toBe(first);
    expect(opened).toHaveLength(1);
    expect((first as FakeDriver).closed).toBe(false);

    const rotated = await acquireNeo4jMemoryDriver(
      { ...base, password: `pool-two-${stamp}` },
      open,
    );
    expect(rotated).not.toBe(first);
    expect((first as FakeDriver).closed).toBe(true);

    const moved = await acquireNeo4jMemoryDriver(
      { ...base, password: `pool-two-${stamp}`, uri: `bolt://other-${stamp}.internal:7687` },
      open,
    );
    expect((rotated as FakeDriver).closed).toBe(true);
    expect((moved as FakeDriver).closed).toBe(false);

    const separate = await acquireNeo4jMemoryDriver(
      {
        ...base,
        uri: `bolt://elsewhere-${stamp}.internal:7687`,
        username: "other",
        password: `pool-three-${stamp}`,
      },
      open,
    );
    expect((moved as FakeDriver).closed).toBe(false);
    expect((separate as FakeDriver).closed).toBe(false);
  });

  it("returns driver failures without the password", async () => {
    const driver = new FakeDriver();
    driver.run = () => Promise.reject(new Error("password=secret refused"));
    const result = await providerFor(driver).recall(
      { query: "Ada", scope: "isolated", botId: "bot-1", limit: 1 },
      context,
    );
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).not.toContain("secret");
  });

  it("enqueues one extraction job on the serial queue and omits the memory text", async () => {
    const driver = new FakeDriver();
    const enqueue = vi.fn(async (_job: BackgroundJob) => undefined);
    const jobs: JobPublisher = { enqueue, cancel: vi.fn(), close: vi.fn() };
    const provider = providerFor(driver, { jobs });
    driver.rows.push({ records: [row({ id: "fact-a" })] });
    const text = "Ada Lovelace founded the Analytical Society in London.";
    await expect(
      provider.save(
        { content: text, scope: "isolated", botId: "bot-1", source: { kind: "durable" } },
        context,
      ),
    ).resolves.toEqual({ ok: true, value: undefined });
    expect(enqueue).toHaveBeenCalledTimes(1);
    const job = enqueue.mock.calls[0]?.[0];
    expect(job?.queueName).toBe("memory-extract");
    expect(job?.payload).toMatchObject({ chunkIndex: 0, attempt: 0, scheduleGeneration: 0 });
    expect(JSON.stringify(job?.payload)).not.toContain("Lovelace");
    expect(job?.payload).not.toHaveProperty("text");
  });

  it("does not enqueue trivial text, a disabled extractor, or a provider with no jobs", async () => {
    const sink = createTestSink();
    const previousLogger = getLogger();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    try {
      const driver = new FakeDriver();
      const enqueue = vi.fn(async () => undefined);
      const jobs: JobPublisher = { enqueue, cancel: vi.fn(), close: vi.fn() };
      const content = "Ada Lovelace founded the Analytical Society in London.";
      driver.rows.push({ records: [row({ id: "fact-ok" })] });
      await providerFor(driver, { jobs }).save(
        { content: "ok", scope: "isolated", botId: "bot-1", source: { kind: "durable" } },
        context,
      );
      driver.rows.push({ records: [row({ id: "fact-off" })] });
      await providerFor(driver, { jobs, extract: false }).save(
        { content, scope: "isolated", botId: "bot-1", source: { kind: "durable" } },
        context,
      );
      driver.rows.push({ records: [row({ id: "fact-no-jobs" })] });
      await providerFor(driver).save(
        { content, scope: "isolated", botId: "bot-1", source: { kind: "durable" } },
        context,
      );
      expect(enqueue).not.toHaveBeenCalled();
      expect(sink.events.map((event) => event.message)).toEqual([
        "memory.extract enqueue skipped: trivial text",
        "memory.extract enqueue skipped: extraction is off",
        "memory.extract enqueue skipped: no job publisher",
      ]);
    } finally {
      installLogger(previousLogger);
    }
  });
});
