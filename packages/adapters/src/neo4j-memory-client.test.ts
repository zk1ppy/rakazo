import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import type { Neo4jMemoryDriver, Neo4jMemoryRows, Neo4jRunOptions } from "./neo4j-memory-client.js";
import {
  ensureNeo4jMemorySchema,
  escapeLuceneQuery,
  linkNeo4jEntities,
  linkNeo4jFactEntity,
  listNeo4jConsolidationEntities,
  listUnextractedNeo4jFacts,
  mergeExtractedNeo4jEntity,
  mergeNeo4jDuplicateEntity,
  NEO4J_ENTITY_FULLTEXT_INDEX,
  NEO4J_FACT_FULLTEXT_INDEX,
  NEO4J_MEMORY_SCHEMA_STATEMENTS,
  purgeNeo4jChatSources,
  readNeo4jFactLearnedAt,
  readNeo4jFactText,
  realignNeo4jFactTimes,
  recallNeo4jFacts,
  renameNeo4jEntity,
  resolveNeo4jBoltAddress,
  saveNeo4jFact,
  upsertNeo4jEntity,
} from "./neo4j-memory-client.js";

class FakeDriver implements Neo4jMemoryDriver {
  readonly calls: Array<{
    cypher: string;
    params: Record<string, unknown>;
    options: Neo4jRunOptions;
  }> = [];
  rows: Neo4jMemoryRows[] = [];

  verifyConnectivity(): Promise<void> {
    return Promise.resolve();
  }

  run(
    cypher: string,
    params: Record<string, unknown>,
    options: Neo4jRunOptions,
  ): Promise<Neo4jMemoryRows> {
    this.calls.push({ cypher, params, options });
    return Promise.resolve(this.rows.shift() ?? { records: [] });
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}

function row(values: Record<string, unknown>) {
  return { get: (key: string) => values[key] };
}

const fact = {
  factId: "fact-1",
  sourceId: "source-1",
  spaceId: "space-1",
  ns: "isolated:bot-1",
  botId: "bot-1",
  text: "Prefer metric units.",
  kind: "memory" as const,
  uri: "rakazo://memory/space-1/isolated:bot-1/abc",
  title: "Memory",
  contentHash: "abc",
  provenance: "rakazo space:space-1 bot:bot-1",
  confidence: 1,
  generation: null,
  now: "2026-01-01T00:00:00.000Z",
};

describe("Neo4j memory client", () => {
  it("imports neo4j-driver from the client module only", () => {
    const root = fileURLToPath(new URL("../../../", import.meta.url));
    const skip = new Set(["node_modules", "dist", "build", "coverage", ".git", ".expo"]);
    const hits: string[] = [];
    const pending = [root];
    while (pending.length > 0) {
      const dir = pending.pop();
      if (!dir) break;
      for (const entry of readdirSync(dir)) {
        if (skip.has(entry)) continue;
        const path = join(dir, entry);
        const info = statSync(path);
        if (info.isDirectory()) {
          pending.push(path);
          continue;
        }
        if (!/\.(ts|tsx|js|mjs|cjs)$/.test(entry)) continue;
        const source = readFileSync(path, "utf8");
        if (/from\s+["']neo4j-driver["']|import\(\s*["']neo4j-driver["']\s*\)/.test(source)) {
          hits.push(path.slice(root.length));
        }
      }
    }
    expect(hits).toEqual(["packages/adapters/src/neo4j-memory-client.ts"]);
  });

  it("escapes Lucene syntax and keeps words", () => {
    expect(escapeLuceneQuery("project memory")).toBe("project memory");
    expect(escapeLuceneQuery("c++")).toBe("c\\+\\+");
    expect(escapeLuceneQuery("a/b")).toBe("a\\/b");
    expect(escapeLuceneQuery('say "hi"')).toBe('say \\"hi\\"');
  });

  it("pins Bolt addresses to the classified trust boundary", async () => {
    const loopback = async () => [{ address: "127.0.0.1", family: 4 }];
    await expect(resolveNeo4jBoltAddress("localhost:7687", "private", loopback)).resolves.toEqual([
      "127.0.0.1:7687",
    ]);
    await expect(resolveNeo4jBoltAddress("[::1]:7687", "private", loopback)).resolves.toEqual([
      "[::1]:7687",
    ]);
    await expect(
      resolveNeo4jBoltAddress("db.example.test:7687", "public", async () => [
        { address: "203.0.113.10", family: 4 },
      ]),
    ).resolves.toEqual(["203.0.113.10:7687"]);
    await expect(
      resolveNeo4jBoltAddress("db.example.test:7687", "public", async () => [
        { address: "10.8.0.2", family: 4 },
      ]),
    ).rejects.toThrow(/private address/);
    await expect(
      resolveNeo4jBoltAddress("neo4j:7687", "private", async () => [
        { address: "203.0.113.10", family: 4 },
      ]),
    ).rejects.toThrow(/private address/);
    await expect(
      resolveNeo4jBoltAddress("169.254.169.254:7687", "private", loopback),
    ).rejects.toThrow(/blocked address/);
  });

  it("creates Rakazo constraints, indexes, and fulltext indexes", async () => {
    const driver = new FakeDriver();
    await ensureNeo4jMemorySchema(driver, "neo4j");
    expect(driver.calls.map((call) => call.cypher)).toEqual([...NEO4J_MEMORY_SCHEMA_STATEMENTS]);
    expect(
      driver.calls.every((call) => call.options.schema && call.options.database === "neo4j"),
    ).toBe(true);
    const cypher = driver.calls.map((call) => call.cypher).join("\n");
    expect(cypher).toContain("FOR (n:Rkz) REQUIRE n.id IS UNIQUE");
    expect(cypher).toContain(NEO4J_FACT_FULLTEXT_INDEX);
    expect(cypher).toContain(NEO4J_ENTITY_FULLTEXT_INDEX);
    expect(cypher).toContain("ON EACH [n.text]");
    expect(cypher).toContain("ON EACH [n.name, n.normName, n.aliases, n.summary]");
  });

  it("stores a fact linked to a source with provenance", async () => {
    const driver = new FakeDriver();
    driver.rows.push({ records: [row({ id: "fact-1" })] });
    await expect(saveNeo4jFact(driver, "neo4j", fact)).resolves.toEqual({ id: "fact-1" });
    const call = driver.calls[0];
    expect(call?.cypher).toContain("MERGE (s:Rkz:Source {id: $sourceId})");
    expect(call?.cypher).toContain("MERGE (f:Rkz:Fact {id: $factId})");
    expect(call?.cypher).toContain("MERGE (f)-[rel:FROM]->(s)");
    expect(call?.cypher).toContain("rel.provenance = $provenance");
    expect(call?.params).toMatchObject({
      spaceId: "space-1",
      ns: "isolated:bot-1",
      text: "Prefer metric units.",
      provenance: "rakazo space:space-1 bot:bot-1",
    });
    expect(call?.cypher).not.toContain("Prefer metric units.");
  });

  it("recalls with fulltext plus one- and two-hop expansion", async () => {
    const driver = new FakeDriver();
    driver.rows.push({
      records: [
        row({
          id: "fact-1",
          memory: "Ada founded the lab.",
          provenance: "rakazo space:space-1 bot:bot-1",
          entity: "Ada",
          updatedAt: "2026-01-01T00:00:00.000Z",
          score: 1.5,
        }),
      ],
    });
    const hits = await recallNeo4jFacts(driver, "graphs", {
      q: "Ada",
      spaceId: "space-1",
      namespaces: ["shared", "isolated:bot-1"],
      historyNs: null,
      historyGeneration: null,
      limit: 5,
    });
    expect(hits).toEqual([
      {
        id: "fact-1",
        memory: "Ada founded the lab.",
        provenance: "rakazo space:space-1 bot:bot-1",
        entity: "Ada",
        updatedAt: "2026-01-01T00:00:00.000Z",
        score: 1.5,
      },
    ]);
    const cypher = driver.calls[0]?.cypher ?? "";
    expect(cypher).toContain(`queryNodes('${NEO4J_FACT_FULLTEXT_INDEX}'`);
    expect(cypher).toContain(`queryNodes('${NEO4J_ENTITY_FULLTEXT_INDEX}'`);
    expect(cypher).toContain("[:ABOUT]");
    expect(cypher).toContain("[rel:RKZ_REL]");
    expect(cypher).toContain("expiredAt IS NULL");
    expect(cypher).toContain("score * 0.9");
    expect(cypher).toContain("score * 0.7");
    expect(cypher).toContain("score * 0.6");
    expect(driver.calls[0]?.options.access).toBe("read");
    expect(driver.calls[0]?.params).toMatchObject({ spaceId: "space-1", limit: 5 });
  });

  it("purges chat sources and the facts taken from them", async () => {
    const driver = new FakeDriver();
    await purgeNeo4jChatSources(driver, "neo4j", {
      spaceId: "space-1",
      botId: "bot-1",
      generations: [2, 3],
    });
    expect(driver.calls).toHaveLength(2);
    expect(driver.calls[0]?.cypher).toContain('kind: "chat"');
    expect(driver.calls[0]?.cypher).toContain("DETACH DELETE f");
    expect(driver.calls[1]?.cypher).toContain("DETACH DELETE s");
    expect(driver.calls[0]?.params).toMatchObject({ generations: [2, 3] });
    await purgeNeo4jChatSources(driver, "neo4j", {
      spaceId: "space-1",
      botId: "bot-1",
      generations: [],
    });
    expect(driver.calls).toHaveLength(2);
  });

  it("upserts only POLE+O labels and links facts and entities inside one namespace", async () => {
    const driver = new FakeDriver();
    await upsertNeo4jEntity(driver, "neo4j", {
      id: "ada",
      type: "Person",
      spaceId: "space-1",
      ns: "shared",
      name: "Ada",
      normName: "ada",
      aliases: ["A. Lovelace"],
      summary: "Founder",
      now: "2026-01-01T00:00:00.000Z",
    });
    expect(driver.calls[0]?.cypher).toContain("MERGE (e:Rkz:Entity:Person {id: $id})");
    expect(driver.calls[0]?.cypher).toContain("e.firstSeen = $now");
    await expect(
      upsertNeo4jEntity(driver, "neo4j", {
        id: "x",
        type: "Bot" as never,
        spaceId: "space-1",
        ns: "shared",
        name: "x",
        normName: "x",
        aliases: [],
        summary: "",
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).rejects.toThrow(/Person, Object, Location, Event, or Organization/);
    driver.rows.push({ records: [row({ id: "fact-1" })] }, { records: [row({ id: "ada" })] });
    await linkNeo4jFactEntity(driver, "neo4j", {
      factId: "fact-1",
      entityId: "ada",
      spaceId: "space-1",
    });
    await linkNeo4jEntities(driver, "neo4j", {
      fromId: "ada",
      toId: "lab",
      spaceId: "space-1",
      ns: "shared",
      type: "founded",
      factId: "fact-1",
      now: "2026-01-01T00:00:00.000Z",
    });
    expect(driver.calls[1]?.cypher).toContain("MERGE (f)-[:ABOUT]->(e)");
    expect(driver.calls[1]?.cypher).not.toContain("validFrom");
    const relation = driver.calls[2]?.cypher ?? "";
    expect(relation).toContain("MERGE (a)-[r:RKZ_REL {type: $type, factId: $factId}]->(b)");
    expect(relation).toContain("MATCH (f:Rkz:Fact {id: $factId, spaceId: $spaceId})");
    expect(relation).toContain("r.validFrom = coalesce(f.createdAt, f.validFrom, $now)");
    expect(relation).toContain("r.createdAt = $now");
    const matched = relation.split("ON MATCH SET")[1] ?? "";
    expect(matched).toContain("r.validFrom = coalesce(f.createdAt, f.validFrom, r.validFrom)");
    expect(matched).not.toContain("r.createdAt");
    expect(driver.calls[2]?.params).toMatchObject({
      time: null,
      now: "2026-01-01T00:00:00.000Z",
    });
  });

  it("reads the fact's stored time and realigns existing edges without a model", async () => {
    const driver = new FakeDriver();
    driver.rows.push({ records: [row({ learnedAt: "2026-10-18T00:00:00.000Z" })] });
    await expect(
      readNeo4jFactLearnedAt(driver, "neo4j", {
        factId: "fact-1",
        spaceId: "space-1",
        ns: "shared",
      }),
    ).resolves.toBe("2026-10-18T00:00:00.000Z");
    expect(driver.calls[0]?.cypher).toContain("coalesce(f.createdAt, f.validFrom)");
    expect(driver.calls[0]?.options.access).toBe("read");

    driver.rows.push({ records: [row({ updated: 2 })] }, { records: [row({ updated: 1 })] });
    await expect(realignNeo4jFactTimes(driver, "neo4j", { spaceId: "space-1" })).resolves.toEqual({
      relations: 2,
      entities: 1,
    });
    const relation = driver.calls[1]?.cypher ?? "";
    const entity = driver.calls[2]?.cypher ?? "";
    expect(relation).toContain("SET r.validFrom = learned");
    expect(relation).not.toContain("r.createdAt");
    expect(relation).toContain("r.validFrom IS NULL OR r.validFrom <> learned");
    expect(entity).toContain("SET e.firstSeen = learned");
    expect(entity).toContain("e.firstSeen IS NULL OR e.firstSeen > learned");
    expect(entity).not.toContain("lastSeen");
    expect(driver.calls[1]?.options.access).toBe("write");
  });

  it("merges an extracted entity onto an alias match and skips expired facts", async () => {
    const driver = new FakeDriver();
    driver.rows.push({
      records: [
        row({
          id: "existing",
          aliases: ["Ada"],
          aliasNorms: ["ada"],
          normName: "ada lovelace",
        }),
      ],
    });
    const id = await mergeExtractedNeo4jEntity(driver, "neo4j", {
      id: "computed",
      type: "Person",
      spaceId: "space-1",
      ns: "shared",
      name: "Ada",
      normName: "ada",
      aliases: ["Ada Lovelace"],
      aliasNorms: ["ada lovelace"],
      summary: "Founder",
      now: "2026-01-01T00:00:00.000Z",
      learnedAt: "2026-10-18T00:00:00.000Z",
    });
    expect(id).toBe("existing");
    expect(driver.calls[0]?.cypher).toContain("aliasNorms");
    expect(driver.calls[0]?.cypher).toContain("Entity:Person");
    expect(driver.calls[1]?.cypher).toContain("e.firstSeen = $learnedAt");
    expect(driver.calls[1]?.cypher).toContain("e.lastSeen = $now");
    expect(driver.calls[1]?.cypher).toContain("e.firstSeen > $learnedAt THEN $learnedAt");
    expect(driver.calls[1]?.params).toMatchObject({
      id: "existing",
      normName: "ada",
      learnedAt: "2026-10-18T00:00:00.000Z",
      now: "2026-01-01T00:00:00.000Z",
    });

    driver.rows.push({ records: [row({ text: "Ada founded the lab." })] });
    await expect(
      readNeo4jFactText(driver, "neo4j", {
        factId: "fact-1",
        spaceId: "space-1",
        ns: "shared",
      }),
    ).resolves.toBe("Ada founded the lab.");
    expect(driver.calls.at(-1)?.cypher).toContain("expiredAt IS NULL");
  });

  it("lists live facts that have no ABOUT edge", async () => {
    const driver = new FakeDriver();
    driver.rows.push({
      records: [
        row({ factId: "fact-1", ns: "shared", botId: "bot-1", text: "Ada founded the lab." }),
        row({ factId: "fact-2", ns: "shared", text: "missing bot" }),
      ],
    });
    await expect(
      listUnextractedNeo4jFacts(driver, "neo4j", { spaceId: "space-1", limit: 10, skip: 0 }),
    ).resolves.toEqual([
      { factId: "fact-1", ns: "shared", botId: "bot-1", text: "Ada founded the lab." },
    ]);
    const cypher = driver.calls[0]?.cypher ?? "";
    expect(cypher).toContain("expiredAt IS NULL");
    expect(cypher).toContain("NOT (f)-[:ABOUT]->()");
    expect(cypher).toContain("ORDER BY f.id");
  });

  it("lists consolidation entities in id order without interpolating names", async () => {
    const driver = new FakeDriver();
    driver.rows.push({
      records: [
        row({
          id: "nicolas-full",
          ns: "shared",
          labels: ["Entity", "Person"],
          entityType: "Person",
          name: "Nicolas Tremblier",
          normName: "nicolas tremblier",
          aliases: ["Nicolas"],
          aliasNorms: ["nicolas"],
          summary: "",
          neighborIds: ["paris"],
          factIds: ["fact-1"],
          degree: 4,
        }),
        row({
          id: "both-types",
          ns: "shared",
          labels: ["Person", "Location"],
          entityType: "Person",
          name: "Paris",
          normName: "paris",
          aliases: [],
          aliasNorms: [],
          summary: "",
          neighborIds: [],
          factIds: [],
          degree: 1,
        }),
      ],
    });
    await expect(
      listNeo4jConsolidationEntities(driver, "neo4j", {
        spaceId: "space-1",
        ns: "shared",
        limit: 50,
        afterId: "ada",
      }),
    ).resolves.toEqual([
      {
        id: "nicolas-full",
        ns: "shared",
        type: "Person",
        name: "Nicolas Tremblier",
        normName: "nicolas tremblier",
        aliases: ["Nicolas"],
        aliasNorms: ["nicolas"],
        summary: "",
        degree: 4,
        neighborIds: ["paris"],
        factIds: ["fact-1"],
      },
    ]);
    const cypher = driver.calls[0]?.cypher ?? "";
    expect(cypher).toContain("spaceId: $spaceId");
    expect(cypher).toContain("ORDER BY e.id");
    expect(cypher).toContain("COUNT { (e)--() }");
    expect(cypher).toContain("aliasNorms");
    expect(cypher).toContain("RKZ_REL");
    expect(cypher).not.toContain("Nicolas");
    expect(driver.calls[0]?.params).toMatchObject({
      spaceId: "space-1",
      ns: "shared",
      afterId: "ada",
      limit: 50,
    });
    expect(driver.calls[0]?.options.access).toBe("read");
  });

  it("refuses a duplicate merge when the nodes are not the same type", async () => {
    const driver = new FakeDriver();
    driver.rows.push({ records: [] });
    await expect(
      mergeNeo4jDuplicateEntity(driver, "neo4j", {
        spaceId: "space-1",
        keepId: "keep",
        dropId: "drop",
        type: "Person",
        name: "Nicolas",
        normName: "nicolas",
        aliases: [],
        aliasNorms: [],
        summary: "",
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).resolves.toEqual({ merged: false, reason: "guard" });
    expect(driver.calls).toHaveLength(1);
    expect(driver.calls[0]?.cypher).toContain("$type IN labels(keep)");
    expect(driver.calls[0]?.cypher).toContain("$type IN labels(drop)");
    expect(driver.calls[0]?.options.access).toBe("read");
  });

  it("leaves the duplicate in place when an edge type is not safe to copy", async () => {
    const driver = new FakeDriver();
    driver.rows.push(
      { records: [row({ id: "keep" })] },
      { records: [row({ foreign: 0 })] },
      { records: [row({ relType: "bad-type" })] },
    );
    await expect(
      mergeNeo4jDuplicateEntity(driver, "neo4j", {
        spaceId: "space-1",
        keepId: "keep",
        dropId: "drop",
        type: "Person",
        name: "Nicolas Tremblier",
        normName: "nicolas tremblier",
        aliases: ["Nicolas"],
        aliasNorms: ["nicolas"],
        summary: "",
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).resolves.toEqual({ merged: false, reason: "unsafe" });
    expect(driver.calls).toHaveLength(3);
    expect(driver.calls.every((call) => call.options.access === "read")).toBe(true);
    expect(driver.calls.map((call) => call.cypher).join("\n")).not.toContain("bad-type");
  });

  it("rewires ABOUT, RKZ_REL, and FROM onto the kept entity before deleting the duplicate", async () => {
    const driver = new FakeDriver();
    const empty = { records: [] as Array<{ get(key: string): unknown }> };
    driver.rows.push(
      { records: [row({ id: "keep" })] },
      { records: [row({ foreign: 0 })] },
      {
        records: [
          row({ relType: "ABOUT" }),
          row({ relType: "RKZ_REL" }),
          row({ relType: "FROM" }),
          row({ relType: "MENTIONS" }),
        ],
      },
      empty,
      empty,
      empty,
      empty,
      empty,
      empty,
      empty,
      empty,
      empty,
      { records: [row({ id: "keep" })] },
    );
    await expect(
      mergeNeo4jDuplicateEntity(driver, "neo4j", {
        spaceId: "space-1",
        keepId: "keep",
        dropId: "drop",
        type: "Person",
        name: "Nicolas Tremblier",
        normName: "nicolas tremblier",
        aliases: ["Nicolas"],
        aliasNorms: ["nicolas"],
        summary: "Lives in Paris",
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).resolves.toEqual({ merged: true });

    const writes = driver.calls.filter((call) => call.options.access === "write");
    const cypher = writes.map((call) => call.cypher);
    expect(cypher[0]).toContain("MERGE (src)-[:ABOUT]->(keep)");
    expect(cypher[0]).toContain("DELETE rel");
    expect(cypher[1]).toContain("-[rel:RKZ_REL]->(other)");
    expect(cypher[1]).toContain(
      "MERGE (keep)-[moved:RKZ_REL {type: coalesce(rel.type, ''), factId: coalesce(rel.factId, '')}]->(other)",
    );
    expect(cypher[1]).toContain("moved.validFrom = rel.validFrom");
    expect(cypher[2]).toContain("-[rel:RKZ_REL]->(drop:Rkz:Entity");
    expect(cypher[2]).toContain(
      "MERGE (other)-[moved:RKZ_REL {type: coalesce(rel.type, ''), factId: coalesce(rel.factId, '')}]->(keep)",
    );
    expect(cypher[3]).toContain("-[rel:FROM]->(other)");
    expect(cypher[3]).toContain("MERGE (keep)-[moved:FROM]->(other)");
    expect(cypher[4]).toContain("-[rel:FROM]->(drop:Rkz:Entity");
    expect(cypher[4]).toContain("MERGE (other)-[moved:FROM]->(keep)");
    expect(cypher[5]).toContain("-[rel:MENTIONS]->(other)");
    expect(cypher[5]).toContain("MERGE (keep)-[moved:MENTIONS]->(other)");
    expect(cypher[6]).toContain("-[rel:MENTIONS]->(drop:Rkz:Entity");
    expect(cypher[7]).toContain("DELETE rel");
    expect(cypher[8]).toContain("-[rel]-(drop)");
    const commit = cypher[9] ?? "";
    expect(commit).toContain("SET keep:Person");
    expect(commit).toContain("REMOVE keep:Object, keep:Location, keep:Event, keep:Organization");
    expect(commit).toContain("DELETE drop");
    expect(commit).toContain("NOT (drop)--()");
    expect(commit).not.toContain("DETACH DELETE");
    expect(writes[9]?.params).toMatchObject({
      keepId: "keep",
      dropId: "drop",
      spaceId: "space-1",
      type: "Person",
      name: "Nicolas Tremblier",
      normName: "nicolas tremblier",
      aliases: ["Nicolas"],
      aliasNorms: ["nicolas"],
    });
    expect(writes.every((call) => call.params.spaceId === "space-1")).toBe(true);
  });

  it("relabels the survivor when the duplicate has a different type", async () => {
    const driver = new FakeDriver();
    const empty = { records: [] as Array<{ get(key: string): unknown }> };
    driver.rows.push(
      { records: [row({ id: "keep" })] },
      { records: [row({ foreign: 0 })] },
      { records: [] },
      empty,
      empty,
      empty,
      empty,
      empty,
      empty,
      empty,
      { records: [row({ id: "keep" })] },
    );
    await expect(
      mergeNeo4jDuplicateEntity(driver, "neo4j", {
        spaceId: "space-1",
        keepId: "keep",
        dropId: "drop",
        type: "Object",
        sourceTypes: ["Organization", "Object"],
        name: "Quill",
        normName: "quill",
        aliases: [],
        aliasNorms: [],
        summary: "",
        now: "2026-01-01T00:00:00.000Z",
      }),
    ).resolves.toEqual({ merged: true });
    const guard = driver.calls[0]?.cypher ?? "";
    expect(guard).toContain("$crossType = true");
    expect(driver.calls[0]?.params).toMatchObject({
      crossType: true,
      sourceTypes: ["Organization", "Object"],
    });
    const commit =
      driver.calls.filter((call) => call.options.access === "write").at(-1)?.cypher ?? "";
    expect(commit).toContain("SET keep:Object");
    expect(commit).toContain("REMOVE keep:Person, keep:Location, keep:Event, keep:Organization");
    expect(commit).not.toContain("REMOVE keep:Object");
  });

  it("renames an entity without changing its id", async () => {
    const driver = new FakeDriver();
    driver.rows.push({ records: [row({ id: "hub" })] });
    await expect(
      renameNeo4jEntity(driver, "neo4j", {
        spaceId: "space-1",
        id: "hub",
        type: "Person",
        fromNorm: "nicolas",
        name: "Nicolas Tremblier",
        normName: "nicolas tremblier",
        aliases: ["Nicolas"],
        aliasNorms: ["nicolas"],
      }),
    ).resolves.toBe(true);
    const call = driver.calls[0];
    expect(call?.cypher).toContain("e.normName = $fromNorm");
    expect(call?.cypher).not.toContain("DELETE");
    expect(call?.params).toMatchObject({
      id: "hub",
      fromNorm: "nicolas",
      normName: "nicolas tremblier",
    });
    expect(call?.options.access).toBe("write");
  });
});
