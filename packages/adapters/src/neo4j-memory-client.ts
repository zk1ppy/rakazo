import { lookup } from "node:dns/promises";
import { isIP } from "node:net";
import type { Driver } from "neo4j-driver";
import neo4j from "neo4j-driver";
import type { ResolvedAddress, ResolveHostname } from "./network-address.js";
import { isCloudMetadataAddress, isPrivateAddress } from "./network-address.js";

/**
 * The only module that imports `neo4j-driver`. Graph reads and writes stay on the
 * shared :Rkz label so Rakazo can share one Neo4j Community database with other apps.
 */

export const NEO4J_FACT_FULLTEXT_INDEX = "rkz_fact_text";
export const NEO4J_ENTITY_FULLTEXT_INDEX = "rkz_entity_name";
/** Entity-to-entity edges. Prefixed so they do not share another application's relationship type. */
export const NEO4J_ENTITY_RELATIONSHIP = "RKZ_REL";

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
  RETURN fact, score * 0.7 AS score, neighbor.name AS entityName
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
  RETURN fact, score * 0.6 AS score, neighbor.name AS entityName
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
       f.generation = $generation
     MERGE (f)-[rel:FROM]->(s)
     ON CREATE SET rel.provenance = $provenance
     RETURN f.id AS id`,
    { ...fact },
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
     WITH collect(DISTINCT s) + collect(DISTINCT c) AS endpoints
     UNWIND endpoints AS endpoint
     WITH endpoint WHERE endpoint IS NOT NULL
     MATCH (f:Rkz:Fact)-[:FROM]->(endpoint)
     OPTIONAL MATCH ()-[rel:${NEO4J_ENTITY_RELATIONSHIP} {factId: f.id}]-()
     DELETE rel
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
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {id: $factId, spaceId: $spaceId})
     MATCH (a:Rkz:Entity {id: $fromId, spaceId: $spaceId, ns: $ns})
     MATCH (b:Rkz:Entity {id: $toId, spaceId: $spaceId, ns: $ns})
     MERGE (a)-[r:${NEO4J_ENTITY_RELATIONSHIP} {type: $type, factId: $factId}]->(b)
     ON CREATE SET
       r.validFrom = coalesce(f.createdAt, f.validFrom, $now),
       r.validTo = null,
       r.createdAt = $now,
       r.time = $time
     ON MATCH SET
       r.validFrom = coalesce(f.createdAt, f.validFrom, r.validFrom),
       r.time = CASE WHEN $time IS NULL THEN r.time ELSE $time END
     RETURN a.id AS id`,
    { ...input, time: input.time ?? null },
    { database, access: "write", signal },
  );
  if (result.records.length === 0) throw new Error("Neo4j fact or entities were not found.");
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
     MATCH ()-[r:${NEO4J_ENTITY_RELATIONSHIP} {factId: f.id}]-()
     WHERE r.validFrom IS NULL OR r.validFrom <> learned
     SET r.validFrom = learned
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
}

/** Facts in one space that have not been linked to an entity yet. */
export async function listUnextractedNeo4jFacts(
  driver: Neo4jMemoryDriver,
  database: string,
  input: { spaceId: string; limit: number; skip: number },
  signal?: AbortSignal,
): Promise<Neo4jUnextractedFact[]> {
  const result = await driver.run(
    `MATCH (f:Rkz:Fact {spaceId: $spaceId})
     WHERE f.expiredAt IS NULL AND NOT (f)-[:ABOUT]->()
     RETURN f.id AS factId, f.ns AS ns, f.botId AS botId, f.text AS text
     ORDER BY f.id
     SKIP $skip
     LIMIT $limit`,
    input,
    { database, access: "read", signal },
  );
  const facts: Neo4jUnextractedFact[] = [];
  for (const record of result.records) {
    const factId = asString(record.get("factId"));
    const ns = asString(record.get("ns"));
    const botId = asString(record.get("botId"));
    const text = asString(record.get("text"));
    if (!factId || !ns || !botId) continue;
    facts.push({ factId, ns, botId, text });
  }
  return facts;
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

const CONSOLIDATION_ENTITY_QUERY = `
WITH e
ORDER BY e.id
LIMIT $limit
OPTIONAL MATCH (e)-[:${NEO4J_ENTITY_RELATIONSHIP}]-(neighbor:Rkz:Entity {spaceId: e.spaceId, ns: e.ns})
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
       COUNT { (e)--() } AS degree
`;

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
      ? `MATCH (e:Rkz:Entity {spaceId: $spaceId})
         WHERE e.id IN $ids AND ($ns = '' OR e.ns = $ns)
         ${CONSOLIDATION_ENTITY_QUERY}`
      : `MATCH (e:Rkz:Entity {spaceId: $spaceId})
         WHERE ($ns = '' OR e.ns = $ns) AND ($afterId = '' OR e.id > $afterId)
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

const HANDLED_MERGE_RELATIONSHIPS = new Set(["ABOUT", NEO4J_ENTITY_RELATIONSHIP, "FROM"]);
const SAFE_RELATIONSHIP_TYPE = /^[A-Z][A-Z0-9_]{0,63}$/;

/**
 * Move one duplicate entity onto the canonical node, then delete it.
 * Refuses a different namespace or space. A different type is refused unless
 * `sourceTypes` lists the labels on the two nodes; the survivor is then relabeled
 * to `type` and the other POLE+O labels are removed. Known edges (ABOUT, RKZ_REL, FROM)
 * keep their endpoints and properties. Any other safe relationship type is copied.
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
    `MATCH (keep:Rkz:Entity {id: $keepId, spaceId: $spaceId})
     MATCH (drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})
     WHERE keep.id <> drop.id
       AND keep.ns = drop.ns
       AND keep.ns IS NOT NULL
       AND (
         (
           $crossType = false
           AND (
             ($type IN labels(keep) AND $type IN labels(drop))
             OR (
               keep.entityType = $type
               AND drop.entityType = $type
               AND none(label IN labels(keep) WHERE label IN $types AND label <> $type)
               AND none(label IN labels(drop) WHERE label IN $types AND label <> $type)
             )
           )
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
    `MATCH (drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})-[rel]-(other)
     WHERE other.spaceId IS NULL OR other.spaceId <> $spaceId
     RETURN count(rel) AS foreign`,
    scope,
    read,
  );
  if (asNumber(foreign.records[0]?.get("foreign")) > 0) {
    return { merged: false, reason: "foreign" };
  }

  const typed = await driver.run(
    `MATCH (drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})-[rel]-()
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
    `MATCH (keep:Rkz:Entity {id: $keepId, spaceId: $spaceId})
     MATCH (src)-[rel:ABOUT]->(drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})
     WHERE src.spaceId = $spaceId AND src <> keep AND src <> drop
     MERGE (src)-[:ABOUT]->(keep)
     DELETE rel`,
    scope,
    write,
  );
  await driver.run(rewireEntityRelation("out"), scope, write);
  await driver.run(rewireEntityRelation("in"), scope, write);
  await driver.run(rewireFrom("out"), scope, write);
  await driver.run(rewireFrom("in"), scope, write);
  for (const relType of relTypes) {
    if (HANDLED_MERGE_RELATIONSHIPS.has(relType)) continue;
    await driver.run(rewireCopiedRelationship(relType, "out"), scope, write);
    await driver.run(rewireCopiedRelationship(relType, "in"), scope, write);
  }
  await driver.run(
    `MATCH (keep:Rkz:Entity {id: $keepId, spaceId: $spaceId})-[rel]-(drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})
     DELETE rel`,
    scope,
    write,
  );
  await driver.run(
    `MATCH (drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})-[rel]-(drop)
     DELETE rel`,
    scope,
    write,
  );
  const committed = await driver.run(
    `MATCH (keep:Rkz:Entity {id: $keepId, spaceId: $spaceId})
     MATCH (drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})
     WHERE keep.ns = drop.ns AND keep.id <> drop.id AND NOT (drop)--()
     SET keep:${label},
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
      ? `MATCH (drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})-[rel:${NEO4J_ENTITY_RELATIONSHIP}]->(other)`
      : `MATCH (other)-[rel:${NEO4J_ENTITY_RELATIONSHIP}]->(drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})`;
  const merge =
    direction === "out"
      ? `MERGE (keep)-[moved:${NEO4J_ENTITY_RELATIONSHIP} {type: coalesce(rel.type, ''), factId: coalesce(rel.factId, '')}]->(other)`
      : `MERGE (other)-[moved:${NEO4J_ENTITY_RELATIONSHIP} {type: coalesce(rel.type, ''), factId: coalesce(rel.factId, '')}]->(keep)`;
  return `MATCH (keep:Rkz:Entity {id: $keepId, spaceId: $spaceId})
     ${match}
     WHERE other.spaceId = $spaceId AND other <> keep AND other <> drop
     ${merge}
     ON CREATE SET
       moved.validFrom = rel.validFrom,
       moved.validTo = rel.validTo,
       moved.createdAt = rel.createdAt,
       moved.time = rel.time
     ON MATCH SET
       moved.validFrom = coalesce(moved.validFrom, rel.validFrom),
       moved.createdAt = coalesce(moved.createdAt, rel.createdAt),
       moved.time = coalesce(moved.time, rel.time),
       moved.validTo = CASE
         WHEN moved.validTo IS NULL OR rel.validTo IS NULL THEN null
         ELSE moved.validTo
       END
     DELETE rel`;
}

function rewireFrom(direction: "in" | "out"): string {
  if (direction === "out") {
    return `MATCH (keep:Rkz:Entity {id: $keepId, spaceId: $spaceId})
     MATCH (drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})-[rel:FROM]->(other)
     WHERE other.spaceId = $spaceId AND other <> keep AND other <> drop
     MERGE (keep)-[moved:FROM]->(other)
     ON CREATE SET moved = properties(rel)
     DELETE rel`;
  }
  return `MATCH (keep:Rkz:Entity {id: $keepId, spaceId: $spaceId})
     MATCH (other)-[rel:FROM]->(drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})
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
    return `MATCH (keep:Rkz:Entity {id: $keepId, spaceId: $spaceId})
     MATCH (drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})-[rel:${relType}]->(other)
     WHERE other <> keep AND other <> drop
     MERGE (keep)-[moved:${relType}]->(other)
     ON CREATE SET moved = properties(rel)
     DELETE rel`;
  }
  return `MATCH (keep:Rkz:Entity {id: $keepId, spaceId: $spaceId})
     MATCH (other)-[rel:${relType}]->(drop:Rkz:Entity {id: $dropId, spaceId: $spaceId})
     WHERE other <> keep AND other <> drop
     MERGE (other)-[moved:${relType}]->(keep)
     ON CREATE SET moved = properties(rel)
     DELETE rel`;
}

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
