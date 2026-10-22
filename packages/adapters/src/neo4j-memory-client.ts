import { lookup } from "node:dns/promises";
import { isIP } from "node:net";
import type { Driver } from "neo4j-driver";
import neo4j from "neo4j-driver";
import type { ResolvedAddress, ResolveHostname } from "./network-address.js";
import { isCloudMetadataAddress, isPrivateAddress } from "./network-address.js";
import {
  canonicalRelationType,
  MAX_RELATION_PROVENANCE_IDS,
  readRelationVocabulary,
  relationTypeCase,
} from "./relation-type.js";

/**
 * The only module that imports `neo4j-driver`. Graph reads and writes stay on the
 * shared :Rkz label so Rakazo can share one Neo4j Community database with other apps.
 */

export const NEO4J_FACT_FULLTEXT_INDEX = "rkz_fact_text";
export const NEO4J_ENTITY_FULLTEXT_INDEX = "rkz_entity_name";
/** Entity-to-entity edges. Prefixed so they do not share another application's relationship type. */
export const NEO4J_ENTITY_RELATIONSHIP = "RKZ_REL";
/** A model refusal to merge two entities. Scoped by spaceId, namespace, and bot id. */
export const NEO4J_DECLINED_RELATIONSHIP = "RKZ_DECLINED";
/**
 * A confirmation that timed out too many times. Not a refusal and not a business
 * edge: consolidation skips the pair until a name, type, or alias changes.
 */
export const NEO4J_DEFERRED_RELATIONSHIP = "RKZ_DEFERRED";

export const NEO4J_ENTITY_TYPES = [
  "Person",
  "Object",
  "Location",
  "Event",
  "Organization",
] as const;

export type Neo4jEntityType = (typeof NEO4J_ENTITY_TYPES)[number];

const ENTITY_LABELS: Record<Neo4jEntityType, Neo4jEntityType> = {
  Person: "Person",
  Object: "Object",
  Location: "Location",
  Event: "Event",
  Organization: "Organization",
};

export interface Neo4jMemoryConnection {
  uri: string;
  database: string;
  username: string;
  password: string;
  endpointTrust: "private" | "public";
}

export interface Neo4jMemoryRows {
  records: Array<{ get(key: string): unknown }>;
}

export interface Neo4jRunOptions {
  database: string;
  access: "read" | "write";
  signal?: AbortSignal;
  /** Schema statements must auto-commit; explicit transactions reject them. */
  schema?: boolean;
}

export interface Neo4jMemoryDriver {
  verifyConnectivity(options: { database: string }): Promise<void>;
  run(
    cypher: string,
    params: Record<string, unknown>,
    options: Neo4jRunOptions,
  ): Promise<Neo4jMemoryRows>;
  close(): Promise<void>;
}

export type Neo4jMemoryDriverFactory = (
  connection: Neo4jMemoryConnection,
) => Neo4jMemoryDriver | Promise<Neo4jMemoryDriver>;

export interface Neo4jDriverDeps {
  resolveHostname?: ResolveHostname;
  connectionTimeoutMs?: number;
}

const defaultResolveHostname: ResolveHostname = (hostname) =>
  lookup(hostname, { all: true, verbatim: true });

export const NEO4J_MEMORY_SCHEMA_STATEMENTS = [
  `CREATE CONSTRAINT rkz_node_id IF NOT EXISTS
   FOR (n:Rkz) REQUIRE n.id IS UNIQUE`,
  `CREATE INDEX rkz_scope IF NOT EXISTS
   FOR (n:Rkz) ON (n.spaceId, n.ns)`,
  `CREATE INDEX rkz_entity_norm IF NOT EXISTS
   FOR (n:Rkz) ON (n.spaceId, n.ns, n.normName)`,
  `CREATE INDEX rkz_source_chat IF NOT EXISTS
   FOR (n:Rkz) ON (n.spaceId, n.botId, n.kind, n.generation)`,
  `CREATE INDEX rkz_rel_fact IF NOT EXISTS
   FOR ()-[r:${NEO4J_ENTITY_RELATIONSHIP}]-() ON (r.factId)`,
  `CREATE INDEX rkz_declined_scope IF NOT EXISTS
   FOR ()-[r:${NEO4J_DECLINED_RELATIONSHIP}]-() ON (r.spaceId, r.ns)`,
  `CREATE FULLTEXT INDEX ${NEO4J_FACT_FULLTEXT_INDEX} IF NOT EXISTS
   FOR (n:Rkz) ON EACH [n.text]`,
  `CREATE FULLTEXT INDEX ${NEO4J_ENTITY_FULLTEXT_INDEX} IF NOT EXISTS
   FOR (n:Rkz) ON EACH [n.name, n.normName, n.aliases, n.summary]`,
] as const;

const ACTIVE_FACT = `fact:Rkz AND fact:Fact
  AND fact.spaceId = $spaceId
  AND fact.ns IN $namespaces
  AND fact.expiredAt IS NULL
  AND ($historyNs IS NULL OR fact.ns <> $historyNs OR fact.generation = $historyGeneration)`;

/** Zero when the edge has no mention count yet, so older edges keep their rank. */
const RELATION_MENTION_BOOST = `log10(toFloat(CASE WHEN rel.mentions IS NULL OR rel.mentions < 1 THEN 1 ELSE rel.mentions END)) * 0.05`;

/** Fulltext match, then one :RKZ_REL hop from the matched entities to their facts. */
const RECALL_CYPHER = `
CALL {
  CALL db.index.fulltext.queryNodes('${NEO4J_FACT_FULLTEXT_INDEX}', $q) YIELD node AS fact, score
  WHERE ${ACTIVE_FACT}
  WITH fact, score
  ORDER BY score DESC
  LIMIT 25
  OPTIONAL MATCH (fact)-[:ABOUT]->(entity:Rkz:Entity)
  WHERE entity.spaceId = $spaceId AND entity.ns IN $namespaces
  RETURN fact, score, entity.name AS entityName
  UNION ALL
  CALL db.index.fulltext.queryNodes('${NEO4J_ENTITY_FULLTEXT_INDEX}', $q) YIELD node AS entity, score
  WHERE entity:Rkz AND entity:Entity
    AND entity.spaceId = $spaceId AND entity.ns IN $namespaces
  WITH entity, score
  ORDER BY score DESC
  LIMIT 15
  MATCH (fact:Rkz:Fact)-[:ABOUT]->(entity)
  WHERE ${ACTIVE_FACT}
  RETURN fact, score * 0.9 AS score, entity.name AS entityName
  LIMIT 25
  UNION ALL
  CALL db.index.fulltext.queryNodes('${NEO4J_ENTITY_FULLTEXT_INDEX}', $q) YIELD node AS entity, score
  WHERE entity:Rkz AND entity:Entity
    AND entity.spaceId = $spaceId AND entity.ns IN $namespaces
  WITH entity, score
  ORDER BY score DESC
  LIMIT 10
  MATCH (entity)-[rel:${NEO4J_ENTITY_RELATIONSHIP}]-(neighbor:Rkz:Entity)
  WHERE neighbor.spaceId = $spaceId AND neighbor.ns IN $namespaces AND rel.validTo IS NULL
  MATCH (fact:Rkz:Fact)-[:ABOUT]->(neighbor)
  WHERE ${ACTIVE_FACT}
  RETURN fact, score * 0.7 + ${RELATION_MENTION_BOOST} AS score, neighbor.name AS entityName
  LIMIT 25
  UNION ALL
  CALL db.index.fulltext.queryNodes('${NEO4J_FACT_FULLTEXT_INDEX}', $q) YIELD node AS hit, score
  WHERE hit:Rkz AND hit:Fact
    AND hit.spaceId = $spaceId AND hit.ns IN $namespaces AND hit.expiredAt IS NULL
    AND ($historyNs IS NULL OR hit.ns <> $historyNs OR hit.generation = $historyGeneration)
  WITH hit, score
  ORDER BY score DESC
  LIMIT 10
  MATCH (hit)-[:ABOUT]->(:Rkz:Entity)-[rel:${NEO4J_ENTITY_RELATIONSHIP}]-(neighbor:Rkz:Entity)
  WHERE neighbor.spaceId = $spaceId AND neighbor.ns IN $namespaces AND rel.validTo IS NULL
  MATCH (fact:Rkz:Fact)-[:ABOUT]->(neighbor)
  WHERE ${ACTIVE_FACT} AND fact.id <> hit.id
  RETURN fact, score * 0.6 + ${RELATION_MENTION_BOOST} AS score, neighbor.name AS entityName
  LIMIT 25
}
WITH fact, max(score) AS score,
     head([name IN collect(entityName) WHERE name IS NOT NULL]) AS entityName
RETURN fact.id AS id,
       fact.text AS memory,
       fact.provenance AS provenance,
       coalesce(entityName, fact.ns) AS entity,
       fact.createdAt AS updatedAt,
       score
ORDER BY score DESC, fact.id ASC
LIMIT $limit
`;

export function escapeLuceneQuery(value: string): string {
  return value.replace(/[+\-!(){}[\]^"~*?:\\/&|]/g, (char) => `\\${char}`);
}

export function splitBoltAddress(address: string): { host: string; port: string } {
  if (address.startsWith("[")) {
    const end = address.indexOf("]");
    if (end < 0) throw new Error("Neo4j address is invalid.");
    return {
      host: address.slice(1, end),
      port: address.slice(end + 1).replace(/^:/, "") || "7687",
    };
  }
  const colon = address.lastIndexOf(":");
  if (colon <= 0) return { host: address, port: "7687" };
  return { host: address.slice(0, colon), port: address.slice(colon + 1) };
}

function formatBoltAddress(host: string, port: string): string {
  return host.includes(":") ? `[${host}]:${port}` : `${host}:${port}`;
}

function assertResolvedAddresses(
  addresses: ResolvedAddress[],
  endpointTrust: "private" | "public",
): void {
  if (addresses.length === 0) throw new Error("Neo4j URI did not resolve to an address.");
  for (const entry of addresses) {
    if (isCloudMetadataAddress(entry.address)) {
      throw new Error("Neo4j URI targets a blocked address.");
    }
    const privateAddress = isPrivateAddress(entry.address);
    if (endpointTrust === "private" && !privateAddress) {
      throw new Error("Neo4j URI no longer resolves to a private address.");
    }
    if (endpointTrust === "public" && privateAddress) {
      throw new Error("Neo4j URI resolves to a private address.");
    }
  }
}

/** Pin one Bolt address to the classified trust boundary. The driver calls this per hop. */
export async function resolveNeo4jBoltAddress(
  address: string,
  endpointTrust: "private" | "public",
  resolveHostname: ResolveHostname,
): Promise<string[]> {
  const { host, port } = splitBoltAddress(address);
  const hostname = host.replace(/^\[|\]$/g, "");
  if (!/^\d+$/.test(port) || Number(port) === 0 || Number(port) > 65535) {
    throw new Error("Neo4j address port is invalid.");
  }
  if (isIP(hostname) !== 0) {
    const family = isIP(hostname) === 6 ? 6 : 4;
    assertResolvedAddresses([{ address: hostname, family }], endpointTrust);
    return [formatBoltAddress(hostname, port)];
  }
  const resolved = await resolveHostname(hostname);
  assertResolvedAddresses(resolved, endpointTrust);
  return resolved.map((entry) => formatBoltAddress(entry.address, port));
}

export function openNeo4jMemoryDriver(
  connection: Neo4jMemoryConnection,
  deps: Neo4jDriverDeps = {},
): Neo4jMemoryDriver {
  const resolve = deps.resolveHostname ?? defaultResolveHostname;
  const timeout = deps.connectionTimeoutMs ?? 10_000;
  const bolt = neo4j.driver(
    connection.uri,
    neo4j.auth.basic(connection.username, connection.password),
    {
      disableLosslessIntegers: true,
      telemetryDisabled: true,
      connectionTimeout: timeout,
      connectionAcquisitionTimeout: timeout,
      maxConnectionPoolSize: 8,
      userAgent: "rakazo-neo4j-memory/0.1.0",
      resolver: (address) => resolveNeo4jBoltAddress(address, connection.endpointTrust, resolve),
    },
  );
  return new Neo4jBoltDriver(bolt);
}

class Neo4jBoltDriver implements Neo4jMemoryDriver {
  constructor(private readonly bolt: Driver) {}

  verifyConnectivity(options: { database: string }): Promise<void> {
    return this.bolt.verifyConnectivity({ database: options.database });
  }

  async run(
    cypher: string,
    params: Record<string, unknown>,
    options: Neo4jRunOptions,
  ): Promise<Neo4jMemoryRows> {
    throwIfAborted(options.signal);
    const bound = integerParameters(params);
    if (options.schema) {
      const session = this.bolt.session({ database: options.database });
      try {
        const result = await session.run(cypher, bound);
        return { records: result.records };
      } finally {
        await session.close();
      }
    }
    const result = await this.bolt.executeQuery(cypher, bound, {
      database: options.database,
      routing: options.access === "read" ? "READ" : "WRITE",
      signal: options.signal,
    });
    return { records: result.records };
  }

  close(): Promise<void> {
    return this.bolt.close();
  }
}

function throwIfAborted(signal?: AbortSignal): void {
  if (!signal?.aborted) return;
  const reason = signal.reason;
  throw reason instanceof Error ? reason : new Error("The operation was aborted.");
}

/** The driver sends plain numbers as floats. LIMIT and generation equality need integers. */
function integerParameters(params: Record<string, unknown>): Record<string, unknown> {
  const bound: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(params)) {
    bound[key] = integerParameter(value);
  }
  return bound;
}

function integerParameter(value: unknown): unknown {
  if (typeof value === "number" && Number.isInteger(value)) return neo4j.int(value);
  if (Array.isArray(value)) return value.map(integerParameter);
  return value;
}

export async function ensureNeo4jMemorySchema(
  driver: Neo4jMemoryDriver,
  database: string,
  signal?: AbortSignal,
): Promise<void> {
  for (const cypher of NEO4J_MEMORY_SCHEMA_STATEMENTS) {
    await driver.run(cypher, {}, { database, access: "write", schema: true, signal });
  }
}

export interface Neo4jFactWrite {
  factId: string;
  sourceId: string;
  spaceId: string;
  ns: string;
  botId: string;
  text: string;
  kind: "memory" | "chat";
  uri: string;
  title: string;
  contentHash: string;
  provenance: string;
  confidence: number;
  generation: number | null;
  /** Set on a chat summary so the next compaction of that thread can replace it. */
  threadId?: string | null;
  now: string;
}

export async function saveNeo4jFact(
  driver: Neo4jMemoryDriver,
  database: string,
  fact: Neo4jFactWrite,
  signal?: AbortSignal,
): Promise<{ id: string }> {
  const result = await driver.run(
    `MERGE (s:Rkz:Source {id: $sourceId})
     ON CREATE SET
       s.spaceId = $spaceId,
       s.ns = $ns,
       s.kind = $kind,
       s.uri = $uri,
       s.title = $title,
       s.fetchedAt = $now,
       s.contentHash = $contentHash,
       s.botId = $botId,
       s.generation = $generation
     MERGE (f:Rkz:Fact {id: $factId})
     ON CREATE SET
       f.spaceId = $spaceId,
       f.ns = $ns,
       f.text = $text,
       f.validFrom = $now,
       f.validTo = null,
       f.createdAt = $now,
       f.expiredAt = null,
       f.confidence = $confidence,
       f.provenance = $provenance,
       f.botId = $botId,
       f.generation = $generation,
       f.threadId = $threadId
     SET f.threadId = CASE WHEN $threadId IS NULL THEN f.threadId ELSE $threadId END
     MERGE (f)-[rel:FROM]->(s)
     ON CREATE SET rel.provenance = $provenance
     RETURN f.id AS id`,
    { ...fact, threadId: fact.threadId ?? null },
    { database, access: "write", signal },
  );
  const id = result.records[0]?.get("id");
  if (typeof id !== "string" || !id) throw new Error("Neo4j did not return a fact id.");
  return { id };
}

export interface Neo4jRecallHit {
  id: string;
  memory: string;
  provenance: string;
  entity: string;
  updatedAt: string;
  score: number;
}

export async function recallNeo4jFacts(
  driver: Neo4jMemoryDriver,
  database: string,
  query: {
    q: string;
    spaceId: string;
    namespaces: string[];
    historyNs: string | null;
    historyGeneration: number | null;
    limit: number;
  },
  signal?: AbortSignal,
): Promise<Neo4jRecallHit[]> {
  const result = await driver.run(RECALL_CYPHER, query, { database, access: "read", signal });
  return result.records.map((record) => ({
    id: asString(record.get("id")),
    memory: asString(record.get("memory")),
    provenance: asString(record.get("provenance")),
    entity: asString(record.get("entity")),
    updatedAt: asString(record.get("updatedAt")),
    score: asNumber(record.get("score")),
  }));
}

export async function forgetNeo4jFact(
  driver: Neo4jMemoryDriver,
  database: string,
  input: {
    id: string;
    spaceId: string;
    namespaces: string[] | null;
    now: string;
    reason: string | null;
  },
  signal?: AbortSignal,
): Promise<{ id: string; expired: boolean; reason: string | null } | null> {
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {id: $id, spaceId: $spaceId})
     WHERE $namespaces IS NULL OR f.ns IN $namespaces
     SET f.expiredAt = coalesce(f.expiredAt, $now),
         f.forgetReason = CASE WHEN $reason IS NULL THEN f.forgetReason ELSE $reason END
     RETURN f.id AS id, f.forgetReason AS reason`,
    input,
    { database, access: "write", signal },
  );
  const record = result.records[0];
  if (!record) return null;
  const reason = record.get("reason");
  return {
    id: asString(record.get("id")),
    expired: true,
    reason: typeof reason === "string" ? reason : null,
  };
}

export async function purgeNeo4jChatSources(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; botId: string; generations: number[] },
  signal?: AbortSignal,
): Promise<void> {
  if (input.generations.length === 0) return;
  const options: Neo4jRunOptions = { database, access: "write", signal };
  await driver.run(
    `MATCH (s:Rkz:Source {spaceId: $spaceId, botId: $botId, kind: "chat"})
     WHERE s.generation IN $generations
     OPTIONAL MATCH (s)-[:HAS_CHUNK]->(c:Rkz:Chunk)
     WITH collect(DISTINCT s) AS sources, collect(DISTINCT c) AS chunks
     WITH [source IN sources | source.id] AS doomedSourceIds,
          sources + [chunk IN chunks WHERE chunk IS NOT NULL] AS endpoints
     UNWIND endpoints AS endpoint
     WITH endpoint, doomedSourceIds
     WHERE endpoint IS NOT NULL
     MATCH (f:Rkz:Fact)-[:FROM]->(endpoint)
     WHERE f.spaceId = $spaceId
     WITH collect(DISTINCT f) AS facts,
          collect(DISTINCT f.id) AS doomedFactIds,
          head(collect(doomedSourceIds)) AS doomedSourceIds
     OPTIONAL MATCH (a:Rkz)-[rel:${NEO4J_ENTITY_RELATIONSHIP}]-(b:Rkz)
     WHERE a.spaceId = $spaceId AND b.spaceId = $spaceId
       AND (
         rel.factId IN doomedFactIds
         OR any(id IN coalesce(rel.factIds, []) WHERE id IN doomedFactIds)
       )
     WITH facts, doomedFactIds, doomedSourceIds, collect(DISTINCT rel) AS matched
     WITH facts, doomedFactIds, doomedSourceIds,
          [rel IN matched WHERE rel IS NOT NULL] AS rels
     WITH facts, doomedFactIds, doomedSourceIds, rels,
          [rel IN rels WHERE size(${cypherDropIds("rel", "doomedFactIds")}) = 0] AS emptyRels,
          [rel IN rels WHERE size(${cypherDropIds("rel", "doomedFactIds")}) > 0] AS keepRels
     FOREACH (rel IN keepRels |
       SET rel.factIds = ${cypherDropIds("rel", "doomedFactIds")},
           rel.factId = head(${cypherDropIds("rel", "doomedFactIds")}),
           rel.sourceIds = ${cypherDropIds("rel", "doomedSourceIds", "sourceIds")},
           rel.mentions = size(${cypherDropIds("rel", "doomedFactIds")})
     )
     FOREACH (rel IN emptyRels | DELETE rel)
     WITH DISTINCT facts
     UNWIND facts AS f
     DETACH DELETE f`,
    input,
    options,
  );
  await driver.run(
    `MATCH (s:Rkz:Source {spaceId: $spaceId, botId: $botId, kind: "chat"})
     WHERE s.generation IN $generations
     OPTIONAL MATCH (s)-[:HAS_CHUNK]->(c:Rkz:Chunk)
     WITH s, collect(c) AS chunks
     DETACH DELETE s
     WITH chunks
     UNWIND chunks AS chunk
     DETACH DELETE chunk`,
    input,
    options,
  );
}

/**
 * Delete earlier chat summaries for one thread and the edges that exist only
 * because of them. ABOUT leaves with the fact. A relation that also cites
 * another fact keeps that fact id. The source node is shared by generation
 * and stays.
 */
export async function replaceNeo4jThreadChatFact(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; botId: string; threadId: string; keepFactId: string },
  signal?: AbortSignal,
): Promise<number> {
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {spaceId: $spaceId, botId: $botId, kind: "chat", threadId: $threadId})
     WHERE f.id <> $keepFactId
     WITH collect(DISTINCT f) AS facts, collect(DISTINCT f.id) AS doomedFactIds
     WITH facts, doomedFactIds, [] AS doomedSourceIds
     WHERE size(doomedFactIds) > 0
     OPTIONAL MATCH (a:Rkz)-[rel:${NEO4J_ENTITY_RELATIONSHIP}]-(b:Rkz)
     WHERE a.spaceId = $spaceId AND b.spaceId = $spaceId
       AND (
         rel.factId IN doomedFactIds
         OR any(id IN coalesce(rel.factIds, []) WHERE id IN doomedFactIds)
       )
     WITH facts, doomedFactIds, doomedSourceIds, collect(DISTINCT rel) AS matched
     WITH facts, doomedFactIds, doomedSourceIds,
          [rel IN matched WHERE rel IS NOT NULL] AS rels
     WITH facts, doomedFactIds, doomedSourceIds, rels,
          [rel IN rels WHERE size(${cypherDropIds("rel", "doomedFactIds")}) = 0] AS emptyRels,
          [rel IN rels WHERE size(${cypherDropIds("rel", "doomedFactIds")}) > 0] AS keepRels
     FOREACH (rel IN keepRels |
       SET rel.factIds = ${cypherDropIds("rel", "doomedFactIds")},
           rel.factId = head(${cypherDropIds("rel", "doomedFactIds")}),
           rel.sourceIds = ${cypherDropIds("rel", "doomedSourceIds", "sourceIds")},
           rel.mentions = size(${cypherDropIds("rel", "doomedFactIds")})
     )
     FOREACH (rel IN emptyRels | DELETE rel)
     WITH DISTINCT facts
     UNWIND facts AS f
     DETACH DELETE f
     RETURN count(f) AS removed`,
    input,
    { database, access: "write", signal },
  );
  return asNumber(result.records[0]?.get("removed"));
}

export interface Neo4jEntityWrite {
  id: string;
  type: Neo4jEntityType;
  spaceId: string;
  ns: string;
  name: string;
  normName: string;
  aliases: string[];
  summary: string;
  now: string;
}

export async function upsertNeo4jEntity(
  driver: Neo4jMemoryDriver,
  database: string,
  entity: Neo4jEntityWrite,
  signal?: AbortSignal,
): Promise<void> {
  const label = entityLabel(entity.type);
  await driver.run(
    `MERGE (e:Rkz:Entity:${label} {id: $id})
     ON CREATE SET
       e.spaceId = $spaceId,
       e.ns = $ns,
       e.name = $name,
       e.normName = $normName,
       e.aliases = $aliases,
       e.summary = $summary,
       e.firstSeen = $now,
       e.lastSeen = $now
     ON MATCH SET
       e.name = $name,
       e.normName = $normName,
       e.aliases = $aliases,
       e.summary = $summary,
       e.lastSeen = $now`,
    {
      id: entity.id,
      spaceId: entity.spaceId,
      ns: entity.ns,
      name: entity.name,
      normName: entity.normName,
      aliases: entity.aliases,
      summary: entity.summary,
      now: entity.now,
    },
    { database, access: "write", signal },
  );
}

export async function linkNeo4jFactEntity(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { factId: string; entityId: string; spaceId: string },
  signal?: AbortSignal,
): Promise<void> {
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {id: $factId, spaceId: $spaceId})
     MATCH (e:Rkz:Entity {id: $entityId, spaceId: $spaceId})
     MERGE (f)-[:ABOUT]->(e)
     RETURN f.id AS id`,
    input,
    { database, access: "write", signal },
  );
  if (result.records.length === 0) throw new Error("Neo4j fact or entity was not found.");
}

export async function linkNeo4jEntities(
  driver: Neo4jMemoryDriver,
  database: string,
  input: {
    fromId: string;
    toId: string;
    spaceId: string;
    ns: string;
    type: string;
    factId: string;
    /** Write time. Stored on createdAt and left alone when the edge already exists. */
    now: string;
    /** Optional time from extraction. Null leaves an existing value alone. */
    time?: string | null;
  },
  signal?: AbortSignal,
): Promise<void> {
  const type = canonicalRelationType(input.type);
  if (!type) throw new Error("Neo4j relation type is empty.");
  const params = { ...input, type, time: input.time ?? null };
  const options: Neo4jRunOptions = { database, access: "write", signal };
  // MERGE on {type} would update every parallel edge instead of collapsing them.
  // The queue is serialized, so a read of the current edges then one write is safe.
  const updated = await driver.run(LINK_EXISTING_RELATION, params, options);
  if (updated.records.length > 0) return;
  const created = await driver.run(LINK_NEW_RELATION, params, options);
  if (created.records.length === 0) throw new Error("Neo4j fact or entities were not found.");
}

/**
 * Point existing relation edges and entity first-seen times at the fact that
 * produced them. Idempotent: a second run changes nothing once the stamps match.
 * createdAt on the edge stays the original write time.
 */
export async function realignNeo4jFactTimes(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string },
  signal?: AbortSignal,
): Promise<{ relations: number; entities: number }> {
  const options: Neo4jRunOptions = { database, access: "write", signal };
  const relations = await driver.run(
    `MATCH (f:Rkz:Fact {spaceId: $spaceId})
     WITH f, coalesce(f.createdAt, f.validFrom) AS learned
     WHERE learned IS NOT NULL
     MATCH (a:Rkz)-[r:${NEO4J_ENTITY_RELATIONSHIP}]-(b:Rkz)
     WHERE a.spaceId = $spaceId AND b.spaceId = $spaceId
       AND (r.factId = f.id OR f.id IN coalesce(r.factIds, []))
     WITH r, min(learned) AS earliest, max(learned) AS latest
     WHERE r.firstSeen IS NULL OR r.firstSeen > earliest
        OR r.validFrom IS NULL OR r.validFrom > earliest
        OR r.lastSeen IS NULL OR r.lastSeen < latest
     SET r.firstSeen = CASE
           WHEN r.firstSeen IS NULL OR earliest < r.firstSeen THEN earliest
           ELSE r.firstSeen
         END,
         r.validFrom = CASE
           WHEN r.validFrom IS NULL OR earliest < r.validFrom THEN earliest
           ELSE r.validFrom
         END,
         r.lastSeen = CASE
           WHEN r.lastSeen IS NULL OR latest > r.lastSeen THEN latest
           ELSE r.lastSeen
         END
     RETURN count(r) AS updated`,
    input,
    options,
  );
  const entities = await driver.run(
    `MATCH (f:Rkz:Fact {spaceId: $spaceId})-[:ABOUT]->(e:Rkz:Entity)
     WHERE f.expiredAt IS NULL AND coalesce(f.createdAt, f.validFrom) IS NOT NULL
     WITH e, min(coalesce(f.createdAt, f.validFrom)) AS learned
     WHERE e.firstSeen IS NULL OR e.firstSeen > learned
     SET e.firstSeen = learned
     RETURN count(e) AS updated`,
    input,
    options,
  );
  return {
    relations: asNumber(relations.records[0]?.get("updated")),
    entities: asNumber(entities.records[0]?.get("updated")),
  };
}

export interface ParallelRelationCollapse {
  /** Parallel RKZ_REL edges deleted. The surviving edge keeps the unioned provenance. */
  relations: number;
  /** Extra ABOUT edges deleted. Writers already merge one ABOUT per pair. */
  about: number;
  /** Extra FROM edges deleted. Writers already merge one FROM per pair. */
  from: number;
  /** Edges whose business type was rewritten to the vocabulary before the collapse. */
  rewritten?: number;
}

/**
 * Collapse parallel edges that share a type between the same directed pair.
 * One page of entity ids is enough: an edge is included when either end is in
 * the page. Comparison ignores case, spaces, and hyphens. Different types stay.
 * The reverse direction stays. No model call.
 */
export async function collapseNeo4jParallelRelations(
  driver: Neo4jMemoryDriver,
  database: string,
  input: {
    spaceId: string;
    ids: string[];
    ns?: string;
    /** `space` ignores `ids` and scans every RKZ_REL in the space. */
    scope?: "page" | "space";
    /** Count duplicates without writing. */
    dryRun?: boolean;
    /**
     * Rewrite business types through the vocabulary, then collapse parallel
     * edges that share a direction. Fact ids on the surviving edge are the union.
     */
    collapseTypes?: boolean;
  },
  signal?: AbortSignal,
): Promise<ParallelRelationCollapse> {
  const scope = input.scope ?? "page";
  if (scope === "page" && input.ids.length === 0) return { relations: 0, about: 0, from: 0 };
  const options: Neo4jRunOptions = {
    database,
    access: input.dryRun ? "read" : "write",
    signal,
  };
  const vocabulary = readRelationVocabulary();
  const params = {
    spaceId: input.spaceId,
    ids: input.ids,
    ns: input.ns ?? "",
    scope,
    types: [...NEO4J_ENTITY_TYPES],
    relationAllowed: [...vocabulary.allowed],
  };
  let rewritten = 0;
  if (input.collapseTypes && !vocabulary.passthrough && !input.dryRun) {
    const rewrite = await driver.run(
      `MATCH (a:Rkz)-[r:${NEO4J_ENTITY_RELATIONSHIP}]->(b:Rkz)
       WHERE a.spaceId = $spaceId AND b.spaceId = $spaceId
         AND a.id <> b.id
         AND ${RELATION_ENDPOINT.replaceAll("node", "a")}
         AND ${RELATION_ENDPOINT.replaceAll("node", "b")}
         AND ($scope = 'space' OR a.id IN $ids OR b.id IN $ids)
         AND ($ns = '' OR (a.ns = $ns AND b.ns = $ns))
       WITH r, ${cypherRelationType("r.type")} AS relType
       WHERE relType <> ''
       WITH r, relType, ${relationTypeCase("relType", vocabulary)} AS canonical
       WHERE relType <> canonical
       SET r.type = canonical
       RETURN count(r) AS rewritten`,
      params,
      options,
    );
    rewritten = asNumber(rewrite.records[0]?.get("rewritten"));
  }
  const relations = await driver.run(
    input.dryRun ? COLLAPSE_RELATIONS_PREVIEW : COLLAPSE_RELATIONS,
    params,
    options,
  );
  const about = await driver.run(
    input.dryRun ? COLLAPSE_ABOUT_PREVIEW : COLLAPSE_ABOUT,
    params,
    options,
  );
  const fromEdges = await driver.run(
    input.dryRun ? COLLAPSE_FROM_PREVIEW : COLLAPSE_FROM,
    params,
    options,
  );
  return {
    relations: asNumber(relations.records[0]?.get("removed")),
    about: asNumber(about.records[0]?.get("removed")),
    from: asNumber(fromEdges.records[0]?.get("removed")),
    ...(input.collapseTypes ? { rewritten } : {}),
  };
}

/** When the fact was stored. Extraction uses this instead of the time the job runs. */
export async function readNeo4jFactLearnedAt(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { factId: string; spaceId: string; ns: string },
  signal?: AbortSignal,
): Promise<string | null> {
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {id: $factId, spaceId: $spaceId, ns: $ns})
     WHERE f.expiredAt IS NULL
     RETURN coalesce(f.createdAt, f.validFrom) AS learnedAt`,
    input,
    { database, access: "read", signal },
  );
  const learnedAt = result.records[0]?.get("learnedAt");
  return typeof learnedAt === "string" && learnedAt.trim() ? learnedAt : null;
}

export interface Neo4jUnextractedFact {
  factId: string;
  ns: string;
  botId: string;
  text: string;
  /**
   * True when `extractedAt` is set or the fact has an outgoing :ABOUT edge.
   * The node stays :Rkz:Fact. The marker is a property, not a label.
   */
  extracted: boolean;
}

/**
 * Live facts in one space.
 *
 * Already extracted means `extractedAt` is set or the fact has an outgoing
 * :ABOUT edge. The extraction job writes `extractedAt` after the last chunk,
 * including a complete result that stores no entities. A truncated reply does
 * not. A fact with `extractFailedAt` is a durable failure and stays out of
 * this list. Facts extracted before the property existed are recognized by the
 * :ABOUT edge. `includeExtracted` returns those facts as well.
 */
export async function listUnextractedNeo4jFacts(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; limit: number; skip: number; includeExtracted?: boolean },
  signal?: AbortSignal,
): Promise<Neo4jUnextractedFact[]> {
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {spaceId: $spaceId})
     WHERE f.expiredAt IS NULL
       AND f.extractFailedAt IS NULL
       AND (
         $includeExtracted
         OR (f.extractedAt IS NULL AND NOT (f)-[:ABOUT]->())
       )
     RETURN f.id AS factId, f.ns AS ns, f.botId AS botId, f.text AS text,
            CASE
              WHEN f.extractedAt IS NOT NULL OR (f)-[:ABOUT]->() THEN true
              ELSE false
            END AS extracted
     ORDER BY f.id
     SKIP $skip
     LIMIT $limit`,
    {
      spaceId: input.spaceId,
      limit: input.limit,
      skip: input.skip,
      includeExtracted: input.includeExtracted === true,
    },
    { database, access: "read", signal },
  );
  const facts: Neo4jUnextractedFact[] = [];
  for (const record of result.records) {
    const factId = asString(record.get("factId"));
    const ns = asString(record.get("ns"));
    const botId = asString(record.get("botId"));
    const text = asString(record.get("text"));
    if (!factId || !ns || !botId) continue;
    facts.push({ factId, ns, botId, text, extracted: record.get("extracted") === true });
  }
  return facts;
}

/** Live facts that already have extractedAt or an outgoing :ABOUT edge. */
export async function countExtractedNeo4jFacts(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string },
  signal?: AbortSignal,
): Promise<number> {
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {spaceId: $spaceId})
     WHERE f.expiredAt IS NULL
       AND (f.extractedAt IS NOT NULL OR (f)-[:ABOUT]->())
     RETURN count(f) AS extracted`,
    input,
    { database, access: "read", signal },
  );
  return asNumber(result.records[0]?.get("extracted"));
}

/**
 * Set extractedAt on facts that already have an :ABOUT edge and no marker.
 * The value is the fact's stored time. No model call. Facts stay :Rkz:Fact.
 */
export async function stampNeo4jExtractedFacts(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; extractedAt: string },
  signal?: AbortSignal,
): Promise<number> {
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {spaceId: $spaceId})-[:ABOUT]->()
     WHERE f.expiredAt IS NULL AND f.extractedAt IS NULL AND f.extractFailedAt IS NULL
     SET f.extractedAt = coalesce(f.createdAt, f.validFrom, $extractedAt)
     RETURN count(f) AS marked`,
    input,
    { database, access: "write", signal },
  );
  return asNumber(result.records[0]?.get("marked"));
}

/**
 * Live facts whose extraction stored nothing: `extractedAt` is set, the fact
 * has no outgoing :ABOUT edge, and `extractFailedAt` is unset. `includeFailed`
 * also returns durable stalls. `since` keeps a fact when `extractedAt`,
 * `extractFailedAt`, or the fact's stored time is at or after an ISO timestamp.
 * Null `since` keeps every matching fact.
 */
export async function listEmptyExtractedNeo4jFacts(
  driver: Neo4jMemoryDriver,
  database: string,
  input: {
    spaceId: string;
    limit: number;
    skip: number;
    since?: string | null;
    includeFailed?: boolean;
  },
  signal?: AbortSignal,
): Promise<Neo4jUnextractedFact[]> {
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {spaceId: $spaceId})
     WHERE f.expiredAt IS NULL
       AND (
         (
           f.extractedAt IS NOT NULL
           AND f.extractFailedAt IS NULL
           AND NOT (f)-[:ABOUT]->()
         )
         OR ($includeFailed AND f.extractFailedAt IS NOT NULL)
       )
       AND (
         $since IS NULL
         OR f.extractedAt >= $since
         OR f.extractFailedAt >= $since
         OR coalesce(f.createdAt, f.validFrom) >= $since
       )
     RETURN f.id AS factId, f.ns AS ns, f.botId AS botId, f.text AS text
     ORDER BY f.id
     SKIP $skip
     LIMIT $limit`,
    {
      spaceId: input.spaceId,
      limit: input.limit,
      skip: input.skip,
      since: input.since ?? null,
      includeFailed: input.includeFailed === true,
    },
    { database, access: "read", signal },
  );
  const facts: Neo4jUnextractedFact[] = [];
  for (const record of result.records) {
    const factId = asString(record.get("factId"));
    const ns = asString(record.get("ns"));
    const botId = asString(record.get("botId"));
    const text = asString(record.get("text"));
    if (!factId || !ns || !botId) continue;
    facts.push({ factId, ns, botId, text, extracted: true });
  }
  return facts;
}

/**
 * Drop `extractedAt` on facts that still have no outgoing :ABOUT edge, and
 * drop `extractFailedAt` on a durable stall even when that fact has an edge.
 * Returns the ids that were cleared. An empty id list does not touch the graph.
 */
export async function clearNeo4jExtractedAt(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; factIds: readonly string[] },
  signal?: AbortSignal,
): Promise<string[]> {
  if (input.factIds.length === 0) return [];
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {spaceId: $spaceId})
     WHERE f.expiredAt IS NULL
       AND f.id IN $factIds
       AND (
         (f.extractedAt IS NOT NULL AND f.extractFailedAt IS NULL AND NOT (f)-[:ABOUT]->())
         OR f.extractFailedAt IS NOT NULL
       )
     REMOVE f.extractedAt, f.extractFailedAt, f.extractFailedReason
     RETURN f.id AS factId`,
    { spaceId: input.spaceId, factIds: [...input.factIds] },
    { database, access: "write", signal },
  );
  const cleared: string[] = [];
  for (const record of result.records) {
    const factId = asString(record.get("factId"));
    if (factId) cleared.push(factId);
  }
  return cleared;
}

/**
 * Record a durable stall. `extractedAt` stays unset, so backfill does not
 * treat the fact as extracted. A later success removes these properties.
 */
export async function markNeo4jFactExtractFailed(
  driver: Neo4jMemoryDriver,
  database: string,
  input: {
    factId: string;
    spaceId: string;
    ns: string;
    extractFailedAt: string;
    reason: string;
  },
  signal?: AbortSignal,
): Promise<void> {
  await driver.run(
    `MATCH (f:Rkz:Fact {id: $factId, spaceId: $spaceId, ns: $ns})
     WHERE f.expiredAt IS NULL
     SET f.extractFailedAt = $extractFailedAt, f.extractFailedReason = $reason`,
    input,
    { database, access: "write", signal },
  );
}

/** Record that extraction finished for this fact. Overwrites an older marker. */
export async function markNeo4jFactExtracted(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { factId: string; spaceId: string; ns: string; extractedAt: string },
  signal?: AbortSignal,
): Promise<void> {
  await driver.run(
    `MATCH (f:Rkz:Fact {id: $factId, spaceId: $spaceId, ns: $ns})
     WHERE f.expiredAt IS NULL
     SET f.extractedAt = $extractedAt
     REMOVE f.extractFailedAt, f.extractFailedReason`,
    input,
    { database, access: "write", signal },
  );
}

export async function readNeo4jFactText(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { factId: string; spaceId: string; ns: string },
  signal?: AbortSignal,
): Promise<string | null> {
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {id: $factId, spaceId: $spaceId, ns: $ns})
     WHERE f.expiredAt IS NULL
     RETURN f.text AS text`,
    input,
    { database, access: "read", signal },
  );
  const text = result.records[0]?.get("text");
  return typeof text === "string" && text.trim() ? text : null;
}

export interface Neo4jExtractedEntityMerge {
  id: string;
  type: Neo4jEntityType;
  spaceId: string;
  ns: string;
  name: string;
  normName: string;
  aliases: string[];
  aliasNorms: string[];
  summary: string;
  /** Write time. lastSeen uses this. */
  now: string;
  /** Fact created/learned time. Absent uses `now`. */
  learnedAt?: string;
}

/** MERGE by space, namespace, type, and normalized name or alias. Returns the kept id. */
export async function mergeExtractedNeo4jEntity(
  driver: Neo4jMemoryDriver,
  database: string,
  entity: Neo4jExtractedEntityMerge,
  signal?: AbortSignal,
): Promise<string> {
  const label = entityLabel(entity.type);
  const keys = uniqueStrings([entity.normName, ...entity.aliasNorms]);
  const options: Neo4jRunOptions = { database, access: "write", signal };
  const found = await driver.run(
    `MATCH (e:Rkz:Entity:${label} {spaceId: $spaceId, ns: $ns})
     WHERE e.normName IN $keys
        OR any(alias IN coalesce(e.aliasNorms, []) WHERE alias IN $keys)
     RETURN e.id AS id, e.aliases AS aliases, e.aliasNorms AS aliasNorms, e.normName AS normName
     ORDER BY CASE WHEN e.normName = $normName THEN 0 ELSE 1 END, e.id
     LIMIT 1`,
    { spaceId: entity.spaceId, ns: entity.ns, keys, normName: entity.normName },
    options,
  );
  const record = found.records[0];
  const existingId = record?.get("id");
  const id = typeof existingId === "string" && existingId ? existingId : entity.id;
  const keptNorm =
    record && typeof record.get("normName") === "string"
      ? String(record.get("normName"))
      : entity.normName;
  const aliasNorms = uniqueStrings(
    [...asStringList(record?.get("aliasNorms")), entity.normName, ...entity.aliasNorms],
    keptNorm,
  );
  const aliases = uniqueStrings(
    [...asStringList(record?.get("aliases")), ...entity.aliases],
    entity.name,
  );
  await driver.run(
    `MERGE (e:Rkz:Entity:${label} {id: $id})
     ON CREATE SET
       e.spaceId = $spaceId,
       e.ns = $ns,
       e.entityType = $type,
       e.name = $name,
       e.normName = $normName,
       e.aliases = $aliases,
       e.aliasNorms = $aliasNorms,
       e.summary = $summary,
       e.firstSeen = $learnedAt,
       e.lastSeen = $now
     ON MATCH SET
       e.entityType = $type,
       e.aliases = $aliases,
       e.aliasNorms = $aliasNorms,
       e.summary = CASE WHEN $summary = "" THEN e.summary ELSE $summary END,
       e.firstSeen = CASE
         WHEN e.firstSeen IS NULL OR e.firstSeen > $learnedAt THEN $learnedAt
         ELSE e.firstSeen
       END,
       e.lastSeen = $now,
       e.name = CASE WHEN e.normName = $normName THEN $name ELSE e.name END
     RETURN e.id AS id`,
    {
      id,
      type: entity.type,
      spaceId: entity.spaceId,
      ns: entity.ns,
      name: entity.name,
      normName: entity.normName,
      aliases,
      aliasNorms,
      summary: entity.summary,
      now: entity.now,
      learnedAt: entity.learnedAt?.trim() || entity.now,
    },
    options,
  );
  return id;
}

export interface Neo4jConsolidationEntity {
  id: string;
  ns: string;
  type: Neo4jEntityType;
  name: string;
  normName: string;
  aliases: string[];
  aliasNorms: string[];
  summary: string;
  degree: number;
  neighborIds: string[];
  factIds: string[];
}

export interface Neo4jConsolidationList {
  spaceId: string;
  ns?: string;
  limit: number;
  /** Exclusive lower bound on entity id. Ignored when `ids` is set. */
  afterId?: string;
  /** Load these entities instead of the cursor page. */
  ids?: string[];
}

/** A POLE+O node. :Entity is normal. A type label or entityType alone still counts. */
const CONSOLIDATION_NODE = `(e:Entity OR e:Person OR e:Object OR e:Location OR e:Event OR e:Organization OR coalesce(e.entityType, '') IN ['Person','Object','Location','Event','Organization'])`;
const CONSOLIDATION_NEIGHBOR = `(neighbor:Entity OR neighbor:Person OR neighbor:Object OR neighbor:Location OR neighbor:Event OR neighbor:Organization OR coalesce(neighbor.entityType, '') IN ['Person','Object','Location','Event','Organization'])`;

const CONSOLIDATION_ENTITY_QUERY = `
WITH e
ORDER BY e.id
LIMIT $limit
OPTIONAL MATCH (e)-[:${NEO4J_ENTITY_RELATIONSHIP}]-(neighbor:Rkz {spaceId: e.spaceId, ns: e.ns})
WHERE ${CONSOLIDATION_NEIGHBOR}
WITH e, [id IN collect(DISTINCT neighbor.id) WHERE id IS NOT NULL AND id <> e.id][0..24] AS neighborIds
OPTIONAL MATCH (fact:Rkz:Fact {spaceId: e.spaceId})-[:ABOUT]->(e)
WITH e, neighborIds, [id IN collect(DISTINCT fact.id) WHERE id IS NOT NULL][0..16] AS factIds
RETURN e.id AS id,
       e.ns AS ns,
       labels(e) AS labels,
       coalesce(e.entityType, '') AS entityType,
       coalesce(e.name, '') AS name,
       coalesce(e.normName, '') AS normName,
       e.aliases AS aliases,
       e.aliasNorms AS aliasNorms,
       coalesce(e.summary, '') AS summary,
       neighborIds,
       factIds,
       COUNT { (e)-[:${NEO4J_ENTITY_RELATIONSHIP}|ABOUT]-() } AS degree
`;

/** People whose normalized name or alias is in the list. An empty list does no query. */
export async function findNeo4jPeopleByNorms(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; ns?: string; norms: string[] },
  signal?: AbortSignal,
): Promise<Neo4jConsolidationEntity[]> {
  const norms = [
    ...new Set(input.norms.map((norm) => norm.trim()).filter((norm) => norm.length >= 2)),
  ];
  if (norms.length === 0) return [];
  const result = await driver.run(
    `MATCH (e:Rkz:Entity:Person {spaceId: $spaceId})
     WHERE ($ns = '' OR e.ns = $ns)
       AND (
         e.normName IN $norms
         OR any(alias IN coalesce(e.aliasNorms, []) WHERE alias IN $norms)
       )
     ${CONSOLIDATION_ENTITY_QUERY}`,
    {
      spaceId: input.spaceId,
      ns: input.ns?.trim() ?? "",
      norms,
      limit: 40,
    },
    { database, access: "read", signal },
  );
  const entities: Neo4jConsolidationEntity[] = [];
  for (const record of result.records) {
    const entity = consolidationEntity(record);
    if (entity) entities.push(entity);
  }
  return entities;
}

/** One page of entities in a space, optionally restricted to a namespace or an id list. */
export async function listNeo4jConsolidationEntities(
  driver: Neo4jMemoryDriver,
  database: string,
  input: Neo4jConsolidationList,
  signal?: AbortSignal,
): Promise<Neo4jConsolidationEntity[]> {
  const byIds = Boolean(input.ids);
  const result = await driver.run(
    byIds
      ? `MATCH (e:Rkz {spaceId: $spaceId})
         WHERE e.id IN $ids AND ($ns = '' OR e.ns = $ns)
           AND ${CONSOLIDATION_NODE}
         ${CONSOLIDATION_ENTITY_QUERY}`
      : `MATCH (e:Rkz {spaceId: $spaceId})
         WHERE ($ns = '' OR e.ns = $ns) AND ($afterId = '' OR e.id > $afterId)
           AND ${CONSOLIDATION_NODE}
         ${CONSOLIDATION_ENTITY_QUERY}`,
    {
      spaceId: input.spaceId,
      ns: input.ns?.trim() ?? "",
      afterId: input.afterId?.trim() ?? "",
      ids: input.ids ?? [],
      limit: input.limit,
    },
    { database, access: "read", signal },
  );
  const entities: Neo4jConsolidationEntity[] = [];
  for (const record of result.records) {
    const entity = consolidationEntity(record);
    if (entity) entities.push(entity);
  }
  return entities;
}

export interface Neo4jDeclinedPair {
  leftId: string;
  rightId: string;
  ns: string;
  botId: string;
  leftFingerprint: string;
  rightFingerprint: string;
}

export interface Neo4jDeclinedPairWrite extends Neo4jDeclinedPair {
  spaceId: string;
  now: string;
}

/**
 * Refusals whose endpoints are both in `ids`. `spaceId` is the tenant boundary
 * used by every other Rakazo node. `ns` and `botId` keep a shared refusal off
 * an isolated bot, and the reverse.
 */
export async function listNeo4jDeclinedPairs(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; ns?: string; ids: string[] },
  signal?: AbortSignal,
): Promise<Neo4jDeclinedPair[]> {
  const ids = [...new Set(input.ids.filter((id) => id.trim().length > 0))];
  if (ids.length === 0) return [];
  const result = await driver.run(
    `MATCH (left:Rkz {spaceId: $spaceId})-[d:${NEO4J_DECLINED_RELATIONSHIP} {spaceId: $spaceId}]-(right:Rkz {spaceId: $spaceId})
     WHERE left.id IN $ids AND right.id IN $ids
       AND left.id < right.id
       AND left.spaceId = $spaceId AND right.spaceId = $spaceId
       AND ($ns = '' OR (d.ns = $ns AND left.ns = $ns AND right.ns = $ns))
     RETURN left.id AS leftId,
            right.id AS rightId,
            coalesce(d.ns, '') AS ns,
            coalesce(d.botId, '') AS botId,
            coalesce(d.leftFingerprint, '') AS leftFingerprint,
            coalesce(d.rightFingerprint, '') AS rightFingerprint`,
    {
      spaceId: input.spaceId,
      ns: input.ns?.trim() ?? "",
      ids,
    },
    { database, access: "read", signal },
  );
  const pairs: Neo4jDeclinedPair[] = [];
  for (const record of result.records) {
    const leftId = asString(record.get("leftId"));
    const rightId = asString(record.get("rightId"));
    if (!leftId || !rightId) continue;
    pairs.push({
      leftId,
      rightId,
      ns: asString(record.get("ns")),
      botId: asString(record.get("botId")),
      leftFingerprint: asString(record.get("leftFingerprint")),
      rightFingerprint: asString(record.get("rightFingerprint")),
    });
  }
  return pairs;
}

/** One refusal between two entities in the same space and namespace. Ids are ordered by the caller. */
export async function rememberNeo4jDeclinedPair(
  driver: Neo4jMemoryDriver,
  database: string,
  input: Neo4jDeclinedPairWrite,
  signal?: AbortSignal,
): Promise<void> {
  if (!input.leftId || !input.rightId || input.leftId === input.rightId) return;
  if (input.leftId > input.rightId) return;
  await driver.run(
    `MATCH (left:Rkz {id: $leftId, spaceId: $spaceId})
     MATCH (right:Rkz {id: $rightId, spaceId: $spaceId})
     WHERE left.ns = $ns AND right.ns = $ns AND left.id < right.id
     MERGE (left)-[d:${NEO4J_DECLINED_RELATIONSHIP} {spaceId: $spaceId}]->(right)
     SET d.ns = $ns,
         d.botId = $botId,
         d.leftFingerprint = $leftFingerprint,
         d.rightFingerprint = $rightFingerprint,
         d.decidedAt = $now`,
    {
      spaceId: input.spaceId,
      leftId: input.leftId,
      rightId: input.rightId,
      ns: input.ns,
      botId: input.botId,
      leftFingerprint: input.leftFingerprint,
      rightFingerprint: input.rightFingerprint,
      now: input.now,
    },
    { database, access: "write", signal },
  );
}

export interface Neo4jDeferredPair extends Neo4jDeclinedPair {
  timeouts: number;
}

export interface Neo4jDeferredPairWrite extends Neo4jDeclinedPairWrite {}

/**
 * Timeouts whose endpoints are both in `ids`. A pair at the cap is skipped.
 * A fingerprint mismatch is stale and should be forgotten.
 */
export async function listNeo4jDeferredPairs(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; ns?: string; ids: string[] },
  signal?: AbortSignal,
): Promise<Neo4jDeferredPair[]> {
  const ids = [...new Set(input.ids.filter((id) => id.trim().length > 0))];
  if (ids.length === 0) return [];
  const result = await driver.run(
    `MATCH (left:Rkz {spaceId: $spaceId})-[d:${NEO4J_DEFERRED_RELATIONSHIP} {spaceId: $spaceId}]-(right:Rkz {spaceId: $spaceId})
     WHERE left.id IN $ids AND right.id IN $ids
       AND left.id < right.id
       AND left.spaceId = $spaceId AND right.spaceId = $spaceId
       AND ($ns = '' OR (d.ns = $ns AND left.ns = $ns AND right.ns = $ns))
     RETURN left.id AS leftId,
            right.id AS rightId,
            coalesce(d.ns, '') AS ns,
            coalesce(d.botId, '') AS botId,
            coalesce(d.leftFingerprint, '') AS leftFingerprint,
            coalesce(d.rightFingerprint, '') AS rightFingerprint,
            coalesce(d.timeouts, 0) AS timeouts`,
    {
      spaceId: input.spaceId,
      ns: input.ns?.trim() ?? "",
      ids,
    },
    { database, access: "read", signal },
  );
  const pairs: Neo4jDeferredPair[] = [];
  for (const record of result.records) {
    const leftId = asString(record.get("leftId"));
    const rightId = asString(record.get("rightId"));
    if (!leftId || !rightId) continue;
    pairs.push({
      leftId,
      rightId,
      ns: asString(record.get("ns")),
      botId: asString(record.get("botId")),
      leftFingerprint: asString(record.get("leftFingerprint")),
      rightFingerprint: asString(record.get("rightFingerprint")),
      timeouts: asNumber(record.get("timeouts")),
    });
  }
  return pairs;
}

/** Increment the timeout counter. Returns the count after this timeout. */
export async function noteNeo4jPairTimeout(
  driver: Neo4jMemoryDriver,
  database: string,
  input: Neo4jDeferredPairWrite,
  signal?: AbortSignal,
): Promise<number> {
  if (!input.leftId || !input.rightId || input.leftId === input.rightId) return 0;
  if (input.leftId > input.rightId) return 0;
  const result = await driver.run(
    `MATCH (left:Rkz {id: $leftId, spaceId: $spaceId})
     MATCH (right:Rkz {id: $rightId, spaceId: $spaceId})
     WHERE left.ns = $ns AND right.ns = $ns AND left.id < right.id
     MERGE (left)-[d:${NEO4J_DEFERRED_RELATIONSHIP} {spaceId: $spaceId}]->(right)
     SET d.ns = $ns,
         d.botId = $botId,
         d.leftFingerprint = $leftFingerprint,
         d.rightFingerprint = $rightFingerprint,
         d.timeouts = coalesce(d.timeouts, 0) + 1,
         d.updatedAt = $now
     RETURN d.timeouts AS timeouts`,
    {
      spaceId: input.spaceId,
      leftId: input.leftId,
      rightId: input.rightId,
      ns: input.ns,
      botId: input.botId,
      leftFingerprint: input.leftFingerprint,
      rightFingerprint: input.rightFingerprint,
      now: input.now,
    },
    { database, access: "write", signal },
  );
  return asNumber(result.records[0]?.get("timeouts"));
}

/** Drop a timeout marker after one endpoint's name, type, or aliases changed. */
export async function forgetNeo4jDeferredPair(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; leftId: string; rightId: string },
  signal?: AbortSignal,
): Promise<void> {
  await driver.run(
    `MATCH (left:Rkz {id: $leftId, spaceId: $spaceId})-[d:${NEO4J_DEFERRED_RELATIONSHIP} {spaceId: $spaceId}]-(right:Rkz {id: $rightId, spaceId: $spaceId})
     WHERE left.spaceId = $spaceId AND right.spaceId = $spaceId
     DELETE d`,
    input,
    { database, access: "write", signal },
  );
}

/** Remove a refusal after one endpoint's name, type, or aliases changed. */
export async function forgetNeo4jDeclinedPair(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; leftId: string; rightId: string },
  signal?: AbortSignal,
): Promise<void> {
  await driver.run(
    `MATCH (left:Rkz {id: $leftId, spaceId: $spaceId})-[d:${NEO4J_DECLINED_RELATIONSHIP} {spaceId: $spaceId}]-(right:Rkz {id: $rightId, spaceId: $spaceId})
     WHERE left.spaceId = $spaceId AND right.spaceId = $spaceId
     DELETE d`,
    input,
    { database, access: "write", signal },
  );
}

export interface Neo4jCleanupEntity {
  id: string;
  name: string;
  type: string;
}

/** One page of named entities. The caller decides which names to delete. */
export async function listNeo4jCleanupEntities(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; skip: number; limit: number },
  signal?: AbortSignal,
): Promise<Neo4jCleanupEntity[]> {
  const result = await driver.run(
    `MATCH (e:Rkz {spaceId: $spaceId})
     WHERE e.name IS NOT NULL
       AND (e:Entity OR e:Person OR e:Object OR e:Location OR e:Event OR e:Organization
            OR coalesce(e.entityType, '') IN $types)
     RETURN e.id AS id, coalesce(e.name, '') AS name, coalesce(e.entityType, '') AS entityType
     ORDER BY e.id
     SKIP $skip
     LIMIT $limit`,
    {
      spaceId: input.spaceId,
      skip: input.skip,
      limit: input.limit,
      types: [...NEO4J_ENTITY_TYPES],
    },
    { database, access: "read", signal },
  );
  const entities: Neo4jCleanupEntity[] = [];
  for (const record of result.records) {
    const id = asString(record.get("id"));
    const name = asString(record.get("name"));
    if (!id || !name) continue;
    entities.push({ id, name, type: asString(record.get("entityType")) });
  }
  return entities;
}

/** DETACH DELETE removes every edge on the node, including ABOUT and RKZ_REL. */
export async function deleteNeo4jEntities(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; ids: string[] },
  signal?: AbortSignal,
): Promise<number> {
  const ids = [...new Set(input.ids.filter((id) => id.trim().length > 0))];
  if (ids.length === 0) return 0;
  const result = await driver.run(
    `MATCH (e:Rkz {spaceId: $spaceId})
     WHERE e.id IN $ids AND e.spaceId = $spaceId
     DETACH DELETE e
     RETURN count(e) AS deleted`,
    { spaceId: input.spaceId, ids },
    { database, access: "write", signal },
  );
  return asNumber(result.records[0]?.get("deleted"));
}

export interface Neo4jCleanupDeclined {
  leftId: string;
  rightId: string;
  leftFingerprint: string;
  rightFingerprint: string;
  leftType: string;
  leftNorm: string;
  leftAliasNorms: string[];
  rightType: string;
  rightNorm: string;
  rightAliasNorms: string[];
}

/** Refusals in one space, with the endpoints' current name, type, and aliases. */
export async function listNeo4jCleanupDeclined(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string },
  signal?: AbortSignal,
): Promise<Neo4jCleanupDeclined[]> {
  const result = await driver.run(
    `MATCH (left:Rkz {spaceId: $spaceId})-[d:${NEO4J_DECLINED_RELATIONSHIP} {spaceId: $spaceId}]-(right:Rkz {spaceId: $spaceId})
     WHERE left.id < right.id
       AND left.spaceId = $spaceId AND right.spaceId = $spaceId
     RETURN left.id AS leftId,
            right.id AS rightId,
            coalesce(d.leftFingerprint, '') AS leftFingerprint,
            coalesce(d.rightFingerprint, '') AS rightFingerprint,
            coalesce(left.entityType, '') AS leftType,
            coalesce(left.normName, '') AS leftNorm,
            left.aliasNorms AS leftAliasNorms,
            coalesce(right.entityType, '') AS rightType,
            coalesce(right.normName, '') AS rightNorm,
            right.aliasNorms AS rightAliasNorms`,
    { spaceId: input.spaceId },
    { database, access: "read", signal },
  );
  const pairs: Neo4jCleanupDeclined[] = [];
  for (const record of result.records) {
    const leftId = asString(record.get("leftId"));
    const rightId = asString(record.get("rightId"));
    if (!leftId || !rightId) continue;
    pairs.push({
      leftId,
      rightId,
      leftFingerprint: asString(record.get("leftFingerprint")),
      rightFingerprint: asString(record.get("rightFingerprint")),
      leftType: asString(record.get("leftType")),
      leftNorm: asString(record.get("leftNorm")),
      leftAliasNorms: asStringList(record.get("leftAliasNorms")),
      rightType: asString(record.get("rightType")),
      rightNorm: asString(record.get("rightNorm")),
      rightAliasNorms: asStringList(record.get("rightAliasNorms")),
    });
  }
  return pairs;
}

export type Neo4jDuplicateMergeReason = "guard" | "foreign" | "unsafe" | "leftover";

export interface Neo4jDuplicateMerge {
  spaceId: string;
  keepId: string;
  dropId: string;
  type: Neo4jEntityType;
  /**
   * Type labels present on the two nodes when they differ. The survivor is
   * relabeled to `type`, which may be one of these or a third POLE+O type.
   */
  sourceTypes?: Neo4jEntityType[];
  name: string;
  normName: string;
  aliases: string[];
  aliasNorms: string[];
  summary: string;
  now: string;
}

export interface Neo4jEntityRename {
  spaceId: string;
  id: string;
  type: Neo4jEntityType;
  fromNorm: string;
  name: string;
  normName: string;
  aliases: string[];
  aliasNorms: string[];
}

export interface Neo4jDuplicateMergeResult {
  merged: boolean;
  reason?: Neo4jDuplicateMergeReason;
}

const HANDLED_MERGE_RELATIONSHIPS = new Set([
  "ABOUT",
  NEO4J_ENTITY_RELATIONSHIP,
  "FROM",
  NEO4J_DECLINED_RELATIONSHIP,
  NEO4J_DEFERRED_RELATIONSHIP,
]);
const SAFE_RELATIONSHIP_TYPE = /^[A-Z][A-Z0-9_]{0,63}$/;

/**
 * Move one duplicate entity onto the canonical node, then delete it.
 * Refuses a different namespace or space. A different type is refused unless
 * `sourceTypes` lists the labels on the two nodes; the survivor is then relabeled
 * to `type` and the other POLE+O labels are removed. Known edges (ABOUT, RKZ_REL, FROM)
 * keep their endpoints and properties. RKZ_DECLINED and RKZ_DEFERRED are deleted,
 * not copied. Any other safe relationship type is copied.
 * An unsafe type or a cross-space edge leaves both nodes in place.
 * A second call is a no-op once the duplicate id is gone.
 * The kept id is not rewritten.
 */
export async function mergeNeo4jDuplicateEntity(
  driver: Neo4jMemoryDriver,
  database: string,
  input: Neo4jDuplicateMerge,
  signal?: AbortSignal,
): Promise<Neo4jDuplicateMergeResult> {
  const label = entityLabel(input.type);
  if (input.keepId === input.dropId) return { merged: false, reason: "guard" };
  const sourceTypes = mergeSourceTypes(input);
  const crossType = sourceTypes.length >= 2;
  const read: Neo4jRunOptions = { database, access: "read", signal };
  const write: Neo4jRunOptions = { database, access: "write", signal };
  const scope = {
    spaceId: input.spaceId,
    keepId: input.keepId,
    dropId: input.dropId,
    type: input.type,
    types: [...NEO4J_ENTITY_TYPES],
    sourceTypes,
    crossType,
  };
  const guard = await driver.run(
    `MATCH (keep:Rkz {id: $keepId, spaceId: $spaceId})
     MATCH (drop:Rkz {id: $dropId, spaceId: $spaceId})
     WHERE keep.id <> drop.id
       AND keep.ns = drop.ns
       AND keep.ns IS NOT NULL
       AND (
         (
           $crossType = false
           AND ($type IN labels(keep) OR keep.entityType = $type)
           AND ($type IN labels(drop) OR drop.entityType = $type)
           AND none(label IN labels(keep) WHERE label IN $types AND label <> $type)
           AND none(label IN labels(drop) WHERE label IN $types AND label <> $type)
         )
         OR (
           $crossType = true
           AND all(label IN [label IN labels(keep) WHERE label IN $types] WHERE label IN $sourceTypes)
           AND all(label IN [label IN labels(drop) WHERE label IN $types] WHERE label IN $sourceTypes)
           AND (
             size([label IN labels(keep) WHERE label IN $sourceTypes]) = 1
             OR (size([label IN labels(keep) WHERE label IN $types]) = 0 AND keep.entityType IN $sourceTypes)
           )
           AND (
             size([label IN labels(drop) WHERE label IN $sourceTypes]) = 1
             OR (size([label IN labels(drop) WHERE label IN $types]) = 0 AND drop.entityType IN $sourceTypes)
           )
         )
       )
     RETURN keep.id AS id`,
    scope,
    read,
  );
  if (!guard.records[0]) return { merged: false, reason: "guard" };

  const foreign = await driver.run(
    `MATCH (drop:Rkz {id: $dropId, spaceId: $spaceId})-[rel]-(other)
     WHERE other.spaceId IS NULL OR other.spaceId <> $spaceId
     RETURN count(rel) AS foreign`,
    scope,
    read,
  );
  if (asNumber(foreign.records[0]?.get("foreign")) > 0) {
    return { merged: false, reason: "foreign" };
  }

  const typed = await driver.run(
    `MATCH (drop:Rkz {id: $dropId, spaceId: $spaceId})-[rel]-()
     RETURN DISTINCT type(rel) AS relType`,
    scope,
    read,
  );
  const relTypes = typed.records
    .map((record) => asString(record.get("relType")))
    .filter((relType) => relType.length > 0);
  if (relTypes.some((relType) => !SAFE_RELATIONSHIP_TYPE.test(relType))) {
    return { merged: false, reason: "unsafe" };
  }

  await driver.run(
    `MATCH (keep:Rkz {id: $keepId, spaceId: $spaceId})
     MATCH (src)-[rel:ABOUT]->(drop:Rkz {id: $dropId, spaceId: $spaceId})
     WHERE src.spaceId = $spaceId AND src <> keep AND src <> drop
     MERGE (src)-[:ABOUT]->(keep)
     DELETE rel`,
    scope,
    write,
  );
  await driver.run(rewireEntityRelation("out"), scope, write);
  await driver.run(rewireEntityRelation("in"), scope, write);
  await driver.run(rewireUntypedEntityRelation("out"), scope, write);
  await driver.run(rewireUntypedEntityRelation("in"), scope, write);
  await driver.run(rewireFrom("out"), scope, write);
  await driver.run(rewireFrom("in"), scope, write);
  for (const relType of relTypes) {
    if (HANDLED_MERGE_RELATIONSHIPS.has(relType)) continue;
    await driver.run(rewireCopiedRelationship(relType, "out"), scope, write);
    await driver.run(rewireCopiedRelationship(relType, "in"), scope, write);
  }
  await driver.run(
    `MATCH (drop:Rkz {id: $dropId, spaceId: $spaceId})-[d:${NEO4J_DECLINED_RELATIONSHIP}|${NEO4J_DEFERRED_RELATIONSHIP}]-()
     DELETE d`,
    scope,
    write,
  );
  await driver.run(
    `MATCH (keep:Rkz {id: $keepId, spaceId: $spaceId})-[rel]-(drop:Rkz {id: $dropId, spaceId: $spaceId})
     DELETE rel`,
    scope,
    write,
  );
  await driver.run(
    `MATCH (drop:Rkz {id: $dropId, spaceId: $spaceId})-[rel]-(drop)
     DELETE rel`,
    scope,
    write,
  );
  const committed = await driver.run(
    `MATCH (keep:Rkz {id: $keepId, spaceId: $spaceId})
     MATCH (drop:Rkz {id: $dropId, spaceId: $spaceId})
     WHERE keep.ns = drop.ns AND keep.id <> drop.id AND NOT (drop)--()
     SET keep:Entity,
         keep:${label},
         keep.entityType = $type,
         keep.name = $name,
         keep.normName = $normName,
         keep.aliases = $aliases,
         keep.aliasNorms = $aliasNorms,
         keep.summary = $summary,
         keep.firstSeen = CASE
           WHEN keep.firstSeen IS NULL THEN drop.firstSeen
           WHEN drop.firstSeen IS NULL THEN keep.firstSeen
           WHEN drop.firstSeen < keep.firstSeen THEN drop.firstSeen
           ELSE keep.firstSeen
         END,
         keep.lastSeen = CASE
           WHEN drop.lastSeen IS NULL AND keep.lastSeen IS NULL THEN $now
           WHEN drop.lastSeen IS NULL THEN keep.lastSeen
           WHEN keep.lastSeen IS NULL THEN drop.lastSeen
           WHEN drop.lastSeen > keep.lastSeen THEN drop.lastSeen
           ELSE keep.lastSeen
         END
     ${removeOtherTypeLabels(label)}
     DELETE drop
     RETURN keep.id AS id`,
    {
      ...scope,
      name: input.name,
      normName: input.normName,
      aliases: input.aliases,
      aliasNorms: input.aliasNorms,
      summary: input.summary,
      now: input.now,
    },
    write,
  );
  if (typeof committed.records[0]?.get("id") !== "string") {
    return { merged: false, reason: "leftover" };
  }
  return { merged: true };
}

function mergeSourceTypes(input: Neo4jDuplicateMerge): Neo4jEntityType[] {
  const allowed = new Set<string>(NEO4J_ENTITY_TYPES);
  const types = (input.sourceTypes ?? []).filter((type): type is Neo4jEntityType =>
    allowed.has(type),
  );
  return [...new Set(types)];
}

function removeOtherTypeLabels(result: Neo4jEntityType): string {
  const others = NEO4J_ENTITY_TYPES.filter((type) => type !== result);
  return `REMOVE keep:${others.join(", keep:")}`;
}

/** Change name and normName on one entity. The id stays so edges and lookups keep working. */
export async function renameNeo4jEntity(
  driver: Neo4jMemoryDriver,
  database: string,
  input: Neo4jEntityRename,
  signal?: AbortSignal,
): Promise<boolean> {
  entityLabel(input.type);
  const result = await driver.run(
    `MATCH (e:Rkz:Entity {id: $id, spaceId: $spaceId})
     WHERE e.normName = $fromNorm
       AND e.ns IS NOT NULL
       AND ($type IN labels(e) OR e.entityType = $type)
     SET e.name = $name,
         e.normName = $normName,
         e.aliases = $aliases,
         e.aliasNorms = $aliasNorms
     RETURN e.id AS id`,
    {
      id: input.id,
      spaceId: input.spaceId,
      type: input.type,
      fromNorm: input.fromNorm,
      name: input.name,
      normName: input.normName,
      aliases: input.aliases,
      aliasNorms: input.aliasNorms,
    },
    { database, access: "write", signal },
  );
  return typeof result.records[0]?.get("id") === "string";
}

function rewireEntityRelation(direction: "in" | "out"): string {
  const match =
    direction === "out"
      ? `MATCH (drop:Rkz {id: $dropId, spaceId: $spaceId})-[rel:${NEO4J_ENTITY_RELATIONSHIP}]->(other)`
      : `MATCH (other)-[rel:${NEO4J_ENTITY_RELATIONSHIP}]->(drop:Rkz {id: $dropId, spaceId: $spaceId})`;
  const existing =
    direction === "out"
      ? `OPTIONAL MATCH (keep)-[existing:${NEO4J_ENTITY_RELATIONSHIP}]->(other)`
      : `OPTIONAL MATCH (other)-[existing:${NEO4J_ENTITY_RELATIONSHIP}]->(keep)`;
  const merge =
    direction === "out"
      ? `MERGE (keep)-[moved:${NEO4J_ENTITY_RELATIONSHIP} {type: relType}]->(other)`
      : `MERGE (other)-[moved:${NEO4J_ENTITY_RELATIONSHIP} {type: relType}]->(keep)`;
  const factIds = cypherUnionIds(
    cypherEdgeFactIds("rel"),
    cypherUnionIds(cypherEdgeFactIds("existing"), "[]", "baseIds", "baseItem"),
    "nextIds",
    "nextItem",
  );
  const sourceIds = cypherUnionIds(
    `CASE WHEN rel IS NULL THEN [] ELSE coalesce(rel.sourceIds, []) END`,
    cypherUnionIds(
      `CASE WHEN existing IS NULL THEN [] ELSE coalesce(existing.sourceIds, []) END`,
      "[]",
      "baseSources",
      "baseSource",
    ),
    "nextSources",
    "nextSource",
  );
  return `MATCH (keep:Rkz {id: $keepId, spaceId: $spaceId})
     ${match}
     WHERE other.spaceId = $spaceId AND other <> keep AND other <> drop
     WITH keep, drop, other, rel, ${cypherRelationType("rel.type")} AS relType
     WHERE relType <> ''
     ${existing}
     WHERE ${cypherRelationType("existing.type")} = relType AND existing <> rel
     WITH keep, drop, other, rel, relType, existing
     ORDER BY coalesce(existing.createdAt, '9999'), elementId(existing)
     WITH keep, drop, other, rel, relType, head(collect(existing)) AS existing
     WITH keep, drop, other, rel, relType,
          ${factIds} AS factIds,
          ${sourceIds} AS sourceIds,
          ${cypherEarlier(cypherEdgeSeen("existing"), cypherEdgeSeen("rel"))} AS firstSeen,
          ${cypherLater(cypherEdgeLast("existing"), cypherEdgeLast("rel"))} AS lastSeen,
          ${cypherValidTo("existing", "rel")} AS validTo,
          ${cypherEarlier(cypherProp("existing", "createdAt"), cypherProp("rel", "createdAt"))} AS createdAt,
          ${cypherPreferTime("existing", "rel")} AS edgeTime
     ${merge}
     ON CREATE SET
       moved.factIds = factIds,
       moved.factId = head(factIds),
       moved.sourceIds = sourceIds,
       moved.mentions = size(factIds),
       moved.firstSeen = firstSeen,
       moved.lastSeen = lastSeen,
       moved.validFrom = firstSeen,
       moved.validTo = validTo,
       moved.createdAt = createdAt,
       moved.time = edgeTime
     ON MATCH SET
       moved.factIds = factIds,
       moved.factId = head(factIds),
       moved.sourceIds = sourceIds,
       moved.mentions = size(factIds),
       moved.firstSeen = firstSeen,
       moved.lastSeen = lastSeen,
       moved.validFrom = firstSeen,
       moved.validTo = validTo,
       moved.createdAt = createdAt,
       moved.time = CASE WHEN moved.time IS NULL THEN edgeTime ELSE moved.time END
     DELETE rel`;
}

/**
 * Move edges that have no business type onto the survivor, one for one.
 * They are not folded into a typed edge. Leaving them on the duplicate makes
 * `NOT (drop)--()` fail and the confirmed merge returns merged=0.
 */
function rewireUntypedEntityRelation(direction: "in" | "out"): string {
  const match =
    direction === "out"
      ? `MATCH (drop:Rkz {id: $dropId, spaceId: $spaceId})-[rel:${NEO4J_ENTITY_RELATIONSHIP}]->(other)`
      : `MATCH (other)-[rel:${NEO4J_ENTITY_RELATIONSHIP}]->(drop:Rkz {id: $dropId, spaceId: $spaceId})`;
  const create =
    direction === "out"
      ? `CREATE (keep)-[moved:${NEO4J_ENTITY_RELATIONSHIP}]->(other)`
      : `CREATE (other)-[moved:${NEO4J_ENTITY_RELATIONSHIP}]->(keep)`;
  return `MATCH (keep:Rkz {id: $keepId, spaceId: $spaceId})
     ${match}
     WHERE other.spaceId = $spaceId AND other <> keep AND other <> drop
       AND ${cypherRelationType("rel.type")} = ''
     ${create}
     SET moved = properties(rel)
     DELETE rel`;
}

function rewireFrom(direction: "in" | "out"): string {
  if (direction === "out") {
    return `MATCH (keep:Rkz {id: $keepId, spaceId: $spaceId})
     MATCH (drop:Rkz {id: $dropId, spaceId: $spaceId})-[rel:FROM]->(other)
     WHERE other.spaceId = $spaceId AND other <> keep AND other <> drop
     MERGE (keep)-[moved:FROM]->(other)
     ON CREATE SET moved = properties(rel)
     DELETE rel`;
  }
  return `MATCH (keep:Rkz {id: $keepId, spaceId: $spaceId})
     MATCH (other)-[rel:FROM]->(drop:Rkz {id: $dropId, spaceId: $spaceId})
     WHERE other.spaceId = $spaceId AND other <> keep AND other <> drop
     MERGE (other)-[moved:FROM]->(keep)
     ON CREATE SET moved = properties(rel)
     DELETE rel`;
}

function rewireCopiedRelationship(relType: string, direction: "in" | "out"): string {
  if (!SAFE_RELATIONSHIP_TYPE.test(relType)) {
    throw new Error("relationship type is not safe to rewire");
  }
  if (direction === "out") {
    return `MATCH (keep:Rkz {id: $keepId, spaceId: $spaceId})
     MATCH (drop:Rkz {id: $dropId, spaceId: $spaceId})-[rel:${relType}]->(other)
     WHERE other <> keep AND other <> drop
     MERGE (keep)-[moved:${relType}]->(other)
     ON CREATE SET moved = properties(rel)
     DELETE rel`;
  }
  return `MATCH (keep:Rkz {id: $keepId, spaceId: $spaceId})
     MATCH (other)-[rel:${relType}]->(drop:Rkz {id: $dropId, spaceId: $spaceId})
     WHERE other <> keep AND other <> drop
     MERGE (other)-[moved:${relType}]->(keep)
     ON CREATE SET moved = properties(rel)
     DELETE rel`;
}

/**
 * Cypher counterpart of normalizeRelationType for case, trim, spaces, and hyphens.
 * Other punctuation is stripped in TypeScript before a write. Stored values that
 * still contain it are grouped only by this expression.
 */
function cypherRelationType(expr: string): string {
  let value = `toLower(trim(coalesce(${expr}, '')))`;
  value = `replace(replace(${value}, '-', '_'), ' ', '_')`;
  for (let step = 0; step < 6; step += 1) value = `replace(${value}, '__', '_')`;
  return value;
}

function cypherEdgeFactIds(rel: string): string {
  return `CASE WHEN ${rel} IS NULL THEN [] ELSE (coalesce(${rel}.factIds, []) + CASE WHEN ${rel}.factId IS NULL OR ${rel}.factId IN coalesce(${rel}.factIds, []) THEN [] ELSE [${rel}.factId] END) END`;
}

function cypherUnionIds(listExpr: string, into: string, acc: string, item: string): string {
  return `reduce(${acc} = ${into}, ${item} IN ${listExpr} | CASE WHEN ${item} IS NULL OR ${item} IN ${acc} OR size(${acc}) >= ${MAX_RELATION_PROVENANCE_IDS} THEN ${acc} ELSE ${acc} + [${item}] END)`;
}

function cypherUnionEdgeFacts(rels: string): string {
  return `reduce(factAcc = [], factEdge IN ${rels} | reduce(step = factAcc, factItem IN ${cypherEdgeFactIds("factEdge")} | CASE WHEN factItem IS NULL OR factItem IN step OR size(step) >= ${MAX_RELATION_PROVENANCE_IDS} THEN step ELSE step + [factItem] END))`;
}

function cypherUnionEdgeSources(rels: string): string {
  return `reduce(sourceAcc = [], sourceEdge IN ${rels} | reduce(step = sourceAcc, sourceItem IN coalesce(sourceEdge.sourceIds, []) | CASE WHEN sourceItem IS NULL OR sourceItem IN step OR size(step) >= ${MAX_RELATION_PROVENANCE_IDS} THEN step ELSE step + [sourceItem] END))`;
}

function cypherDropIds(rel: string, doomed: string, property?: "sourceIds"): string {
  const list = property === "sourceIds" ? `coalesce(${rel}.sourceIds, [])` : cypherEdgeFactIds(rel);
  return `reduce(kept = [], item IN ${list} | CASE WHEN item IS NULL OR item IN ${doomed} OR item IN kept OR size(kept) >= ${MAX_RELATION_PROVENANCE_IDS} THEN kept ELSE kept + [item] END)`;
}

function cypherProp(rel: string, prop: string): string {
  return `CASE WHEN ${rel} IS NULL THEN null ELSE ${rel}.${prop} END`;
}

function cypherEarlier(left: string, right: string): string {
  return `CASE WHEN ${left} IS NULL THEN ${right} WHEN ${right} IS NULL THEN ${left} WHEN ${right} < ${left} THEN ${right} ELSE ${left} END`;
}

function cypherLater(left: string, right: string): string {
  return `CASE WHEN ${left} IS NULL THEN ${right} WHEN ${right} IS NULL THEN ${left} WHEN ${right} > ${left} THEN ${right} ELSE ${left} END`;
}

function cypherEdgeSeen(rel: string): string {
  return cypherEarlier(cypherProp(rel, "firstSeen"), cypherProp(rel, "validFrom"));
}

function cypherEdgeLast(rel: string): string {
  return cypherLater(cypherProp(rel, "lastSeen"), cypherProp(rel, "validFrom"));
}

function cypherValidTo(existing: string, rel: string): string {
  return `CASE WHEN ${existing} IS NULL THEN ${rel}.validTo WHEN ${existing}.validTo IS NULL OR ${rel}.validTo IS NULL THEN null ELSE ${existing}.validTo END`;
}

function cypherPreferTime(existing: string, rel: string): string {
  return `CASE WHEN ${existing} IS NOT NULL AND ${existing}.time IS NOT NULL AND ${existing}.time <> '' THEN ${existing}.time ELSE ${rel}.time END`;
}

function cypherReduceEarlier(rels: string): string {
  return `reduce(seen = null, stampEdge IN ${rels} | CASE WHEN coalesce(stampEdge.firstSeen, stampEdge.validFrom) IS NULL THEN seen WHEN seen IS NULL OR coalesce(stampEdge.firstSeen, stampEdge.validFrom) < seen THEN coalesce(stampEdge.firstSeen, stampEdge.validFrom) ELSE seen END)`;
}

function cypherReduceLater(rels: string): string {
  return `reduce(seen = null, stampEdge IN ${rels} | CASE WHEN coalesce(stampEdge.lastSeen, stampEdge.validFrom, stampEdge.firstSeen) IS NULL THEN seen WHEN seen IS NULL OR coalesce(stampEdge.lastSeen, stampEdge.validFrom, stampEdge.firstSeen) > seen THEN coalesce(stampEdge.lastSeen, stampEdge.validFrom, stampEdge.firstSeen) ELSE seen END)`;
}

function cypherReduceTime(rels: string): string {
  return `reduce(kept = null, timed IN ${rels} | CASE WHEN kept IS NOT NULL THEN kept WHEN timed.time IS NULL OR timed.time = '' THEN null ELSE timed.time END)`;
}

const LINK_EXISTING_RELATION = `
MATCH (f:Rkz:Fact {id: $factId, spaceId: $spaceId})
MATCH (a:Rkz:Entity {id: $fromId, spaceId: $spaceId, ns: $ns})
MATCH (b:Rkz:Entity {id: $toId, spaceId: $spaceId, ns: $ns})
OPTIONAL MATCH (f)-[:FROM]->(src:Rkz)
WHERE src.spaceId = $spaceId
WITH f, a, b, collect(DISTINCT src.id) AS learnedSourceIds
WITH f, a, b, [id IN learnedSourceIds WHERE id IS NOT NULL] AS learnedSourceIds
OPTIONAL MATCH (a)-[r:${NEO4J_ENTITY_RELATIONSHIP}]->(b)
WHERE ${cypherRelationType("r.type")} = $type
WITH f, a, b, learnedSourceIds, r
ORDER BY coalesce(r.createdAt, '9999'), elementId(r)
WITH f, a, b, learnedSourceIds, collect(r) AS matched
WITH f, a, b, learnedSourceIds, [rel IN matched WHERE rel IS NOT NULL] AS rels
WITH f, a, b, learnedSourceIds, rels, head(rels) AS keeper
WHERE keeper IS NOT NULL
WITH keeper, rels, learnedSourceIds, coalesce(f.createdAt, f.validFrom, $now) AS learned,
     ${cypherUnionEdgeFacts("rels")} AS edgeFacts,
     ${cypherUnionEdgeSources("rels")} AS edgeSources,
     ${cypherReduceEarlier("rels")} AS priorFirst,
     ${cypherReduceLater("rels")} AS priorLast,
     ${cypherReduceTime("rels")} AS priorTime
WITH keeper, tail(rels) AS extras, learned, priorFirst, priorLast, priorTime,
     ${cypherUnionIds(`CASE WHEN $factId IN edgeFacts OR size(edgeFacts) >= ${MAX_RELATION_PROVENANCE_IDS} THEN [] ELSE [$factId] END`, "edgeFacts", "withFact", "factItem")} AS factIds,
     ${cypherUnionIds("[id IN learnedSourceIds WHERE NOT id IN edgeSources]", "edgeSources", "withSource", "sourceItem")} AS sourceIds
SET keeper.type = $type,
    keeper.factIds = factIds,
    keeper.factId = head(factIds),
    keeper.sourceIds = sourceIds,
    keeper.mentions = size(factIds),
    keeper.firstSeen = ${cypherEarlier("priorFirst", "learned")},
    keeper.validFrom = ${cypherEarlier("priorFirst", "learned")},
    keeper.lastSeen = ${cypherLater("priorLast", "learned")},
    keeper.validTo = null,
    keeper.time = CASE
      WHEN priorTime IS NOT NULL THEN priorTime
      WHEN $time IS NULL THEN keeper.time
      ELSE $time
    END
FOREACH (extra IN extras | DELETE extra)
RETURN keeper.type AS type
`;

const LINK_NEW_RELATION = `
MATCH (f:Rkz:Fact {id: $factId, spaceId: $spaceId})
MATCH (a:Rkz:Entity {id: $fromId, spaceId: $spaceId, ns: $ns})
MATCH (b:Rkz:Entity {id: $toId, spaceId: $spaceId, ns: $ns})
OPTIONAL MATCH (f)-[:FROM]->(src:Rkz)
WHERE src.spaceId = $spaceId
WITH f, a, b, collect(DISTINCT src.id) AS learnedSourceIds
WITH a, b, [id IN learnedSourceIds WHERE id IS NOT NULL][0..${MAX_RELATION_PROVENANCE_IDS}] AS sourceIds,
     coalesce(f.createdAt, f.validFrom, $now) AS learned
CREATE (a)-[r:${NEO4J_ENTITY_RELATIONSHIP} {type: $type}]->(b)
SET r.factIds = [$factId],
    r.factId = $factId,
    r.sourceIds = sourceIds,
    r.mentions = 1,
    r.firstSeen = learned,
    r.lastSeen = learned,
    r.validFrom = learned,
    r.validTo = null,
    r.createdAt = $now,
    r.time = $time
RETURN a.id AS id
`;

const RELATION_ENDPOINT = `(node:Entity OR node:Person OR node:Object OR node:Location OR node:Event OR node:Organization OR coalesce(node.entityType, '') IN $types)`;

/** Directed pairs. An empty business type is its own bucket and is not mixed with a named type. */
const COLLAPSE_RELATION_GROUPS = `
MATCH (a:Rkz)-[r:${NEO4J_ENTITY_RELATIONSHIP}]->(b:Rkz)
WHERE a.spaceId = $spaceId AND b.spaceId = $spaceId
  AND a.id <> b.id
  AND ${RELATION_ENDPOINT.replaceAll("node", "a")}
  AND ${RELATION_ENDPOINT.replaceAll("node", "b")}
  AND ($scope = 'space' OR a.id IN $ids OR b.id IN $ids)
  AND ($ns = '' OR (a.ns = $ns AND b.ns = $ns))
WITH a, b, ${cypherRelationType("r.type")} AS relType, r
ORDER BY coalesce(r.createdAt, '9999'), elementId(r)
WITH a, b, relType, collect(r) AS rels
`;

const COLLAPSE_RELATIONS = `
${COLLAPSE_RELATION_GROUPS}
WHERE size(rels) > 1
   OR any(edge IN rels WHERE coalesce(edge.type, '') <> relType OR edge.factIds IS NULL OR edge.mentions IS NULL)
WITH a, b, relType, rels, head(rels) AS keeper
WHERE keeper IS NOT NULL
WITH keeper, rels, relType,
     ${cypherUnionEdgeFacts("rels")} AS factIds,
     ${cypherUnionEdgeSources("rels")} AS edgeSources,
     ${cypherReduceEarlier("rels")} AS firstSeen,
     ${cypherReduceLater("rels")} AS lastSeen,
     ${cypherReduceTime("rels")} AS priorTime,
     any(edge IN rels WHERE edge.validTo IS NULL) AS active,
     reduce(weight = 0, edge IN rels | weight + CASE WHEN edge.mentions IS NULL OR edge.mentions < 1 THEN 1 ELSE edge.mentions END) AS weight
OPTIONAL MATCH (f:Rkz:Fact)-[:FROM]->(src:Rkz)
WHERE f.spaceId = $spaceId AND src.spaceId = $spaceId AND f.id IN factIds
WITH keeper, rels, relType, factIds, edgeSources, firstSeen, lastSeen, priorTime, active, weight,
     collect(DISTINCT src.id) AS lookedUp
WITH keeper, rels, relType, factIds, firstSeen, lastSeen, priorTime, active, weight,
     ${cypherUnionIds("[id IN lookedUp WHERE id IS NOT NULL]", "edgeSources", "joinedSources", "joinedSource")} AS sourceIds
SET keeper.type = relType,
    keeper.factIds = factIds,
    keeper.factId = head(factIds),
    keeper.sourceIds = sourceIds,
    keeper.mentions = CASE WHEN size(factIds) > 0 THEN size(factIds) ELSE weight END,
    keeper.firstSeen = coalesce(firstSeen, keeper.firstSeen),
    keeper.validFrom = coalesce(firstSeen, keeper.validFrom),
    keeper.lastSeen = coalesce(lastSeen, keeper.lastSeen),
    keeper.validTo = CASE WHEN active THEN null ELSE keeper.validTo END,
    keeper.time = CASE
      WHEN keeper.time IS NULL OR keeper.time = '' THEN priorTime
      ELSE keeper.time
    END
WITH tail(rels) AS extras
FOREACH (extra IN extras | DELETE extra)
WITH sum(size(extras)) AS removed
RETURN removed
`;

const COLLAPSE_RELATIONS_PREVIEW = `
${COLLAPSE_RELATION_GROUPS}
WHERE size(rels) > 1
WITH sum(size(rels) - 1) AS removed
RETURN removed
`;

const COLLAPSE_ABOUT_GROUPS = `
MATCH (a:Rkz)-[r:ABOUT]->(b:Rkz)
WHERE a.spaceId = $spaceId AND b.spaceId = $spaceId
  AND (
    $scope = 'space'
    OR a.id IN $ids OR b.id IN $ids
    OR EXISTS {
      MATCH (a)-[:ABOUT]->(mentioned:Rkz)
      WHERE mentioned.id IN $ids AND mentioned.spaceId = $spaceId
    }
  )
  AND ($ns = '' OR a.ns = $ns OR b.ns = $ns)
WITH a, b, r
ORDER BY elementId(r)
WITH a, b, collect(r) AS rels
`;

const COLLAPSE_ABOUT = `
${COLLAPSE_ABOUT_GROUPS}
WHERE size(rels) > 1
WITH tail(rels) AS extras
FOREACH (extra IN extras | DELETE extra)
WITH sum(size(extras)) AS removed
RETURN removed
`;

const COLLAPSE_ABOUT_PREVIEW = `
${COLLAPSE_ABOUT_GROUPS}
WHERE size(rels) > 1
WITH sum(size(rels) - 1) AS removed
RETURN removed
`;

const COLLAPSE_FROM_GROUPS = `
MATCH (a:Rkz)-[r:FROM]->(b:Rkz)
WHERE a.spaceId = $spaceId AND b.spaceId = $spaceId
  AND (
    $scope = 'space'
    OR a.id IN $ids OR b.id IN $ids
    OR EXISTS {
      MATCH (a)-[:ABOUT]->(mentioned:Rkz)
      WHERE mentioned.id IN $ids AND mentioned.spaceId = $spaceId
    }
  )
  AND ($ns = '' OR a.ns = $ns OR b.ns = $ns)
WITH a, b, r
ORDER BY elementId(r)
WITH a, b, collect(r) AS rels
`;

const COLLAPSE_FROM = `
${COLLAPSE_FROM_GROUPS}
WHERE size(rels) > 1
WITH head(rels) AS keeper, rels, tail(rels) AS extras
SET keeper.provenance = coalesce(
  keeper.provenance,
  head([edge IN rels WHERE edge.provenance IS NOT NULL | edge.provenance])
)
WITH extras
FOREACH (extra IN extras | DELETE extra)
WITH sum(size(extras)) AS removed
RETURN removed
`;

const COLLAPSE_FROM_PREVIEW = `
${COLLAPSE_FROM_GROUPS}
WHERE size(rels) > 1
WITH sum(size(rels) - 1) AS removed
RETURN removed
`;

function consolidationEntity(record: {
  get(key: string): unknown;
}): Neo4jConsolidationEntity | null {
  const id = asString(record.get("id")).trim();
  const ns = asString(record.get("ns")).trim();
  const type = consolidationType(
    asStringList(record.get("labels")),
    asString(record.get("entityType")),
  );
  if (!id || !ns || !type) return null;
  const normName = asString(record.get("normName")).trim();
  const name = asString(record.get("name")).trim() || normName;
  if (!name) return null;
  return {
    id,
    ns,
    type,
    name,
    normName,
    aliases: asStringList(record.get("aliases")),
    aliasNorms: asStringList(record.get("aliasNorms")),
    summary: asString(record.get("summary")),
    degree: asNumber(record.get("degree")),
    neighborIds: asStringList(record.get("neighborIds")).filter((neighborId) => neighborId !== id),
    factIds: asStringList(record.get("factIds")),
  };
}

function consolidationType(labels: string[], stored: string): Neo4jEntityType | null {
  const known = labels.filter((label): label is Neo4jEntityType =>
    (NEO4J_ENTITY_TYPES as readonly string[]).includes(label),
  );
  if (known.length === 1) return known[0] ?? null;
  if (known.length > 1) return null;
  if ((NEO4J_ENTITY_TYPES as readonly string[]).includes(stored)) return stored as Neo4jEntityType;
  return null;
}

export async function deleteNeo4jSpace(
  driver: Neo4jMemoryDriver,
  database: string,
  spaceId: string,
): Promise<void> {
  await driver.run(
    `MATCH (n:Rkz {spaceId: $spaceId}) DETACH DELETE n`,
    { spaceId },
    { database, access: "write" },
  );
}

function entityLabel(type: string): Neo4jEntityType {
  if (!Object.hasOwn(ENTITY_LABELS, type)) {
    throw new Error("entity type must be Person, Object, Location, Event, or Organization");
  }
  return ENTITY_LABELS[type as Neo4jEntityType];
}

function uniqueStrings(values: string[], exclude?: string): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed || trimmed === exclude || seen.has(trimmed)) continue;
    seen.add(trimmed);
    result.push(trimmed);
  }
  return result;
}

function asStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string");
}

function asString(value: unknown): string {
  if (typeof value === "string") return value;
  if (value == null) return "";
  return String(value);
}

function asNumber(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "bigint") return Number(value);
  if (
    value &&
    typeof value === "object" &&
    "toNumber" in value &&
    typeof value.toNumber === "function"
  ) {
    const numeric = value.toNumber();
    return typeof numeric === "number" && Number.isFinite(numeric) ? numeric : 0;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}
