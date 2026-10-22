import { randomUUID } from "node:crypto";
import { connect } from "node:net";
import type { AdapterContext, SemanticMemoryResult } from "@rakazo/adapter-kit";
import { beforeAll, describe, expect, it } from "vitest";
import type { Neo4jMemoryConnection, Neo4jMemoryDriver } from "./neo4j-memory-client.js";
import {
  deleteNeo4jSpace,
  linkNeo4jEntities,
  linkNeo4jFactEntity,
  openNeo4jMemoryDriver,
  upsertNeo4jEntity,
} from "./neo4j-memory-client.js";
import {
  Neo4jMemoryProvider,
  neo4jEndpointRequiresDeploymentOwner,
  prepareNeo4jConnection,
} from "./neo4j-memory-provider.js";

/**
 * Optional. Runs against a local Neo4j (Docker or NEO4J_TEST_URI) and skips when
 * nothing is listening. Loopback without NEO4J_TEST_PASSWORD tries the image
 * defaults neo4j/password and neo4j/neo4j. Nodes stay on one generated space id.
 */
describe("Neo4j memory integration", () => {
  let connection: Neo4jMemoryConnection | null = null;
  const spaceId = `neo4j-test-${randomUUID()}`;

  beforeAll(async () => {
    connection = await probeLocalNeo4j();
  }, 20_000);

  it("saves, recalls, forgets, and purges without crossing spaces", async (ctx) => {
    if (!connection) ctx.skip();
    const live = connection!;
    const prepared = await prepareNeo4jConnection(
      { uri: live.uri, database: live.database, username: live.username },
      { password: live.password },
      { openDriver: (input) => openNeo4jMemoryDriver(input, { connectionTimeoutMs: 5_000 }) },
      { allowPrivateEndpoint: true },
    );
    const provider = new Neo4jMemoryProvider({
      uri: prepared.settings.uri ?? live.uri,
      database: prepared.settings.database ?? live.database,
      username: prepared.settings.username ?? live.username,
      password: prepared.credentials.password ?? live.password,
      endpointTrust: prepared.settings.endpointTrust === "private" ? "private" : "public",
    });
    const here = memoryContext(spaceId);
    const elsewhere = memoryContext(`other-${spaceId}`);
    const driver = openNeo4jMemoryDriver(
      { ...live, endpointTrust: live.endpointTrust },
      { connectionTimeoutMs: 5_000 },
    );
    try {
      expect(
        await provider.save(
          {
            content: "Ada prefers metric units.",
            scope: "isolated",
            botId: "bot-1",
            source: { kind: "durable" },
          },
          here,
        ),
      ).toEqual({ ok: true, value: undefined });
      await refreshFulltext(driver, live.database);

      const found = await recall(provider, "metric", here);
      expect(found.map((item) => item.memory)).toContain("Ada prefers metric units.");
      const hit = found.find((item) => item.memory === "Ada prefers metric units.");
      expect(hit?.provenance).toContain(`space:${spaceId}`);
      expect(hit?.updatedAt).toBeTruthy();
      expect(hit?.score).toBeGreaterThan(0);
      expect(await recall(provider, "metric", elsewhere, "shared")).toEqual([]);
      expect(
        await recall(provider, "metric", { ...here, botId: "bot-2" }, "isolated", "bot-2"),
      ).toEqual([]);

      expect(
        await provider.forget(
          { id: hit!.id!, scope: "isolated", botId: "bot-1", reason: "cleanup" },
          here,
        ),
      ).toEqual({
        ok: true,
        value: { id: hit!.id, expired: true, reason: "cleanup" },
      });
      expect(await recall(provider, "metric", here)).toEqual([]);

      expect(
        await provider.save(
          {
            content: "Compacted chat about shipping.",
            scope: "isolated",
            botId: "bot-1",
            source: { kind: "history", generation: 4 },
          },
          here,
        ),
      ).toEqual({ ok: true, value: undefined });
      await refreshFulltext(driver, live.database);
      expect(
        (await recall(provider, "shipping", here, "isolated", "bot-1", 4)).map(
          (item) => item.memory,
        ),
      ).toContain("Compacted chat about shipping.");
      expect(await provider.purgeHistory({ botId: "bot-1", generations: [4] }, here)).toEqual({
        ok: true,
        value: undefined,
      });
      await refreshFulltext(driver, live.database);
      expect(await recall(provider, "shipping", here, "isolated", "bot-1", 4)).toEqual([]);

      expect(
        await provider.save(
          {
            content: "Ada founded the lab.",
            scope: "shared",
            botId: "bot-1",
            source: { kind: "durable" },
          },
          { ...here, runId: "entity-run" },
        ),
      ).toEqual({ ok: true, value: undefined });
      expect(
        await provider.save(
          {
            content: "The lab ships on Tuesdays.",
            scope: "shared",
            botId: "bot-1",
            source: { kind: "durable" },
          },
          { ...here, runId: "lab-run" },
        ),
      ).toEqual({ ok: true, value: undefined });
      await refreshFulltext(driver, live.database);
      const founded = (await recall(provider, "founded", here, "shared")).find((item) =>
        item.memory.includes("founded"),
      );
      const tuesdays = (await recall(provider, "Tuesdays", here, "shared")).find((item) =>
        item.memory.includes("Tuesdays"),
      );
      expect(founded?.id).toBeTruthy();
      expect(tuesdays?.id).toBeTruthy();
      const now = new Date().toISOString();
      await upsertNeo4jEntity(driver, live.database, {
        id: `${spaceId}-ada`,
        type: "Person",
        spaceId,
        ns: "shared",
        name: "Ada Lovelace",
        normName: "ada lovelace",
        aliases: ["Ada"],
        summary: "Founder",
        now,
      });
      await upsertNeo4jEntity(driver, live.database, {
        id: `${spaceId}-lab`,
        type: "Organization",
        spaceId,
        ns: "shared",
        name: "Analytical Lab",
        normName: "analytical lab",
        aliases: [],
        summary: "",
        now,
      });
      await linkNeo4jFactEntity(driver, live.database, {
        factId: founded!.id!,
        entityId: `${spaceId}-ada`,
        spaceId,
      });
      await linkNeo4jFactEntity(driver, live.database, {
        factId: tuesdays!.id!,
        entityId: `${spaceId}-lab`,
        spaceId,
      });
      await linkNeo4jEntities(driver, live.database, {
        fromId: `${spaceId}-ada`,
        toId: `${spaceId}-lab`,
        spaceId,
        ns: "shared",
        type: "founded",
        factId: founded!.id!,
        now,
      });
      await refreshFulltext(driver, live.database);
      const expanded = (await recall(provider, "Lovelace", here, "shared")).map(
        (item) => item.memory,
      );
      expect(expanded).toEqual(
        expect.arrayContaining(["Ada founded the lab.", "The lab ships on Tuesdays."]),
      );
    } finally {
      await deleteNeo4jSpace(driver, live.database, spaceId).catch(() => undefined);
      await deleteNeo4jSpace(driver, live.database, `other-${spaceId}`).catch(() => undefined);
      await driver.close().catch(() => undefined);
    }
  });
});

function memoryContext(spaceId: string): AdapterContext {
  return {
    operationId: `op-${spaceId}`,
    traceId: `trace-${spaceId}`,
    spaceId,
    userId: "user-1",
    botId: "bot-1",
    runId: `run-${spaceId}`,
    signal: new AbortController().signal,
  };
}

async function recall(
  provider: Neo4jMemoryProvider,
  query: string,
  context: AdapterContext,
  scope: "isolated" | "shared" = "isolated",
  botId = "bot-1",
  historyGeneration?: number,
): Promise<SemanticMemoryResult[]> {
  const result = await provider.recall(
    {
      query,
      scope,
      botId,
      limit: 5,
      ...(historyGeneration === undefined ? {} : { historyGeneration }),
    },
    { ...context, botId },
  );
  if (!result.ok) throw new Error(result.error);
  return result.value;
}

async function refreshFulltext(driver: Neo4jMemoryDriver, database: string): Promise<void> {
  await driver.run(
    "CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh()",
    {},
    {
      database,
      access: "read",
    },
  );
}

async function probeLocalNeo4j(): Promise<Neo4jMemoryConnection | null> {
  const uri = process.env.NEO4J_TEST_URI ?? "bolt://127.0.0.1:7687";
  let host = "127.0.0.1";
  let port = 7687;
  try {
    const url = new URL(uri);
    host = url.hostname.replace(/^\[|\]$/g, "") || host;
    port = Number(url.port || 7687);
  } catch {
    return null;
  }
  if (!(await tcpOpen(host, port))) return null;
  const username = process.env.NEO4J_TEST_USERNAME ?? "neo4j";
  const database = process.env.NEO4J_TEST_DATABASE ?? "neo4j";
  const loopback = host === "localhost" || host === "127.0.0.1" || host === "::1";
  const passwords = process.env.NEO4J_TEST_PASSWORD
    ? [process.env.NEO4J_TEST_PASSWORD]
    : loopback
      ? ["password", "neo4j"]
      : [];
  if (passwords.length === 0) return null;
  let endpointTrust: "private" | "public" = "private";
  try {
    endpointTrust = neo4jEndpointRequiresDeploymentOwner(uri) ? "private" : "public";
  } catch {
    return null;
  }
  let lastError: unknown;
  for (const password of passwords) {
    const connection: Neo4jMemoryConnection = {
      uri,
      database,
      username,
      password,
      endpointTrust,
    };
    const driver = openNeo4jMemoryDriver(connection, { connectionTimeoutMs: 2_000 });
    try {
      await driver.verifyConnectivity({ database });
      await driver.close();
      return connection;
    } catch (error) {
      lastError = error;
      await driver.close().catch(() => undefined);
    }
  }
  if (process.env.NEO4J_TEST_PASSWORD) {
    throw lastError instanceof Error ? lastError : new Error("Neo4j connectivity failed.");
  }
  return null;
}

function tcpOpen(host: string, port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = connect({ host, port, timeout: 300 });
    const finish = (open: boolean) => {
      socket.removeAllListeners();
      socket.destroy();
      resolve(open);
    };
    socket.once("connect", () => finish(true));
    socket.once("timeout", () => finish(false));
    socket.once("error", () => finish(false));
  });
}
