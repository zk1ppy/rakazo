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

const ENTITY_LABELS = {
  Person: "Person",
  Object: "Object",
  Location: "Location",
  Event: "Event",
  Organization: "Organization",
} as const;

export type Neo4jEntityType = keyof typeof ENTITY_LABELS;

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
    now: string;
  },
  signal?: AbortSignal,
): Promise<void> {
  const result = await driver.run(
    `MATCH (a:Rkz:Entity {id: $fromId, spaceId: $spaceId, ns: $ns})
     MATCH (b:Rkz:Entity {id: $toId, spaceId: $spaceId, ns: $ns})
     MERGE (a)-[r:${NEO4J_ENTITY_RELATIONSHIP} {type: $type, factId: $factId}]->(b)
     ON CREATE SET r.validFrom = $now, r.validTo = null
     RETURN a.id AS id`,
    input,
    { database, access: "write", signal },
  );
  if (result.records.length === 0) throw new Error("Neo4j entities were not found.");
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

function entityLabel(type: string): string {
  if (!Object.hasOwn(ENTITY_LABELS, type)) {
    throw new Error("entity type must be Person, Object, Location, Event, or Organization");
  }
  return ENTITY_LABELS[type as Neo4jEntityType];
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
