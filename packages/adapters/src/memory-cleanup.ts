import type { SecretStore } from "@rakazo/adapter-kit";
import type { PrismaClient } from "@rakazo/db";
import { getLogger } from "@rakazo/logging";
import { consolidationEntityFingerprint } from "./memory-consolidate.js";
import type { DroppedNameOptions } from "./memory-extract.js";
import {
  isDroppedEntityName,
  readDroppedNameOptions,
  readExtractMaxEventDegree,
} from "./memory-extract.js";
import { toStringRecord } from "./memory-provider-factory.js";
import type { Neo4jCleanupDeclined, Neo4jCleanupEntity } from "./neo4j-memory-client.js";
import { createNeo4jProvider, Neo4jMemoryProvider } from "./neo4j-memory-provider.js";

const PAGE_SIZE = 500;
const MAX_PAGES = 40;
const SAMPLE_LIMIT = 8;

export interface CleanupEntity {
  id: string;
  name: string;
  type: string;
  degree?: number;
}

export type CleanupHubReason = "event-hub" | "object-hub";

export interface CleanupHub extends CleanupEntity {
  degree: number;
  reason: CleanupHubReason;
}

export interface CleanupDeclined {
  leftId: string;
  rightId: string;
  leftFingerprint: string;
  rightFingerprint: string;
  currentLeft: string;
  currentRight: string;
}

export interface CleanupSpace {
  spaceId: string;
  listEntities(input: { skip: number; limit: number }): Promise<CleanupEntity[]>;
  deleteEntities(ids: string[]): Promise<number>;
  listDeclined(): Promise<CleanupDeclined[]>;
  forgetDeclined(pairs: Array<{ leftId: string; rightId: string }>): Promise<number>;
}

export interface CleanupGraphOptions {
  dryRun: boolean;
}

export interface CleanupGraphCounts {
  spaces: number;
  failed: number;
  entities: number;
  eventHubs: number;
  objectHubs: number;
  declinedStale: number;
  deleted: number;
  forgotten: number;
  dryRun: boolean;
  sample: string[];
  hubSample: string[];
}

/** Objects at or above this RKZ_REL degree are edge-prune candidates. `0` lists none. */
export function readCleanupObjectHubDegree(env: NodeJS.ProcessEnv = process.env): number {
  const trimmed = env.MEMORY_CLEANUP_OBJECT_HUB_DEGREE?.trim() ?? "";
  if (!trimmed) return 12;
  if (!/^\d+$/.test(trimmed)) return 12;
  const parsed = Number(trimmed);
  if (parsed > 10_000) return 12;
  return parsed;
}

const USAGE = "Usage: memory:cleanup-graph [--] [--apply] [--dry-run] [--space SPACE_ID]";

export function parseCleanupGraphArgs(argv: readonly string[]): {
  spaceId?: string;
  dryRun: boolean;
} {
  let spaceId: string | undefined;
  let apply = false;
  let dryRun = false;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--") continue;
    if (arg === "--apply") {
      apply = true;
      continue;
    }
    if (arg === "--dry-run") {
      dryRun = true;
      continue;
    }
    if (arg === "--space") {
      const value = argv[index + 1]?.trim();
      index += 1;
      if (!value) throw new Error("--space needs a space id");
      spaceId = value;
      continue;
    }
    throw new Error(USAGE);
  }
  if (apply && dryRun) throw new Error("--apply and --dry-run cannot be used together");
  return { ...(spaceId ? { spaceId } : {}), dryRun: !apply };
}

/** Names the extractor would now refuse: paths, file names, snake-case ids, operational traces, and extra patterns. */
export function isCleanupEntityName(
  name: string,
  options: DroppedNameOptions = readDroppedNameOptions(),
): boolean {
  return isDroppedEntityName(name, options);
}

/**
 * Noise names are deletion candidates. An Event past the cumulative cap, or an
 * Object past the hub degree, is an edge-prune candidate. The node stays.
 * People, places, and organizations are not hubs here.
 */
export function classifyCleanupEntity(
  entity: CleanupEntity,
  options: {
    names?: DroppedNameOptions;
    eventDegree?: number;
    objectHubDegree?: number;
  } = {},
): "delete" | CleanupHubReason | "keep" {
  const names = options.names ?? readDroppedNameOptions();
  if (isCleanupEntityName(entity.name, names)) return "delete";
  const degree = entity.degree ?? 0;
  const eventDegree = options.eventDegree ?? readExtractMaxEventDegree();
  if (entity.type === "Event" && eventDegree > 0 && degree > eventDegree) return "event-hub";
  const objectHubDegree = options.objectHubDegree ?? readCleanupObjectHubDegree();
  if (entity.type === "Object" && objectHubDegree > 0 && degree >= objectHubDegree) {
    return "object-hub";
  }
  return "keep";
}

export function staleDeclinedPairs(pairs: readonly CleanupDeclined[]): CleanupDeclined[] {
  return pairs.filter(
    (pair) =>
      pair.leftFingerprint !== pair.currentLeft || pair.rightFingerprint !== pair.currentRight,
  );
}

/**
 * List noise entities, event hubs, and object hubs, plus stale refusals.
 * `--apply` deletes noise entities and stale refusals. Hubs are listed only:
 * their edges are not rewritten.
 */
export async function cleanupMemoryGraph(
  spaces: readonly CleanupSpace[],
  options: CleanupGraphOptions,
  nameOptions: DroppedNameOptions = readDroppedNameOptions(),
  thresholds: { eventDegree?: number; objectHubDegree?: number } = {},
): Promise<CleanupGraphCounts> {
  const counts: CleanupGraphCounts = {
    spaces: spaces.length,
    failed: 0,
    entities: 0,
    eventHubs: 0,
    objectHubs: 0,
    declinedStale: 0,
    deleted: 0,
    forgotten: 0,
    dryRun: options.dryRun,
    sample: [],
    hubSample: [],
  };
  const eventDegree = thresholds.eventDegree ?? readExtractMaxEventDegree();
  const objectHubDegree = thresholds.objectHubDegree ?? readCleanupObjectHubDegree();
  for (const space of spaces) {
    try {
      const found = await collectEntities(space, nameOptions, eventDegree, objectHubDegree);
      counts.entities += found.noise.length;
      counts.eventHubs += found.eventHubs.length;
      counts.objectHubs += found.objectHubs.length;
      for (const entity of found.noise) {
        if (counts.sample.length >= SAMPLE_LIMIT) break;
        counts.sample.push(entity.name);
      }
      for (const hub of [...found.eventHubs, ...found.objectHubs]) {
        if (counts.hubSample.length >= SAMPLE_LIMIT) break;
        counts.hubSample.push(`${hub.reason} ${hub.name} ${hub.degree}`);
      }
      const declined = staleDeclinedPairs(await space.listDeclined());
      counts.declinedStale += declined.length;
      if (options.dryRun) continue;
      if (found.noise.length > 0) {
        counts.deleted += await space.deleteEntities(found.noise.map((entity) => entity.id));
      }
      if (declined.length > 0) {
        counts.forgotten += await space.forgetDeclined(
          declined.map((pair) => ({ leftId: pair.leftId, rightId: pair.rightId })),
        );
      }
      getLogger().info("memory cleanup wrote a space", {
        "memory.space_id": space.spaceId,
        "memory.entities": found.noise.length,
        "memory.event_hubs": found.eventHubs.length,
        "memory.object_hubs": found.objectHubs.length,
        "memory.declined": declined.length,
      });
    } catch (error) {
      counts.failed += 1;
      getLogger().error("memory cleanup skipped a space", error, {
        "memory.space_id": space.spaceId,
      });
    }
  }
  return counts;
}

async function collectEntities(
  space: CleanupSpace,
  names: DroppedNameOptions,
  eventDegree: number,
  objectHubDegree: number,
): Promise<{ noise: CleanupEntity[]; eventHubs: CleanupHub[]; objectHubs: CleanupHub[] }> {
  const noise: CleanupEntity[] = [];
  const eventHubs: CleanupHub[] = [];
  const objectHubs: CleanupHub[] = [];
  for (let page = 0; page < MAX_PAGES; page += 1) {
    const entities = await space.listEntities({ skip: page * PAGE_SIZE, limit: PAGE_SIZE });
    for (const entity of entities) {
      const kind = classifyCleanupEntity(entity, { names, eventDegree, objectHubDegree });
      if (kind === "delete") noise.push(entity);
      else if (kind === "event-hub" || kind === "object-hub") {
        const hub: CleanupHub = {
          ...entity,
          degree: entity.degree ?? 0,
          reason: kind,
        };
        if (kind === "event-hub") eventHubs.push(hub);
        else objectHubs.push(hub);
      }
    }
    if (entities.length < PAGE_SIZE) return { noise, eventHubs, objectHubs };
  }
  getLogger().warn("memory cleanup stopped at the entity page cap", {
    "memory.space_id": space.spaceId,
  });
  return { noise, eventHubs, objectHubs };
}

export function formatCleanupReport(
  counts: CleanupGraphCounts,
  failures: ReadonlyArray<{ spaceId: string; reason: string }>,
): string {
  const lines = [
    `dry-run ${counts.dryRun}`,
    `spaces ${counts.spaces}`,
    ...failures.map((failure) => `space ${failure.spaceId} failed ${failure.reason}`),
    `failed ${counts.failed + failures.length}`,
    `entities ${counts.entities}`,
    `event-hubs ${counts.eventHubs}`,
    `object-hubs ${counts.objectHubs}`,
    `declined-stale ${counts.declinedStale}`,
    ...counts.sample.map((name) => `sample ${name}`),
    ...counts.hubSample.map((line) => `sample-hub ${line}`),
    `deleted ${counts.dryRun ? "dry-run" : counts.deleted}`,
    `forgotten ${counts.dryRun ? "dry-run" : counts.forgotten}`,
  ];
  return `${lines.join("\n")}\n`;
}

export function cleanupDeclinedFromRow(row: Neo4jCleanupDeclined): CleanupDeclined {
  return {
    leftId: row.leftId,
    rightId: row.rightId,
    leftFingerprint: row.leftFingerprint,
    rightFingerprint: row.rightFingerprint,
    currentLeft: consolidationEntityFingerprint({
      type: row.leftType,
      normName: row.leftNorm,
      aliasNorms: row.leftAliasNorms,
    }),
    currentRight: consolidationEntityFingerprint({
      type: row.rightType,
      normName: row.rightNorm,
      aliasNorms: row.rightAliasNorms,
    }),
  };
}

export function cleanupEntityFromRow(row: Neo4jCleanupEntity): CleanupEntity {
  return { id: row.id, name: row.name, type: row.type, degree: row.degree };
}

export async function loadNeo4jCleanupSpaces(
  prisma: Pick<PrismaClient, "spaceMemoryConfig">,
  secrets: Pick<SecretStore, "load">,
  spaceId?: string,
): Promise<{
  spaces: CleanupSpace[];
  failed: number;
  failures: Array<{ spaceId: string; reason: string }>;
}> {
  const configs = await prisma.spaceMemoryConfig.findMany({
    where: { provider: "neo4j", ...(spaceId ? { spaceId } : {}) },
    include: { secret: true },
  });
  const spaces: CleanupSpace[] = [];
  const failures: Array<{ spaceId: string; reason: string }> = [];
  for (const config of configs) {
    try {
      const secret = config.secret;
      if (!secret?.ciphertext || !secret.id) throw new Error("memory secret is not visible");
      const settings = toStringRecord(config.settings);
      const plaintext = await secrets.load(secret.ciphertext, secret.id);
      const credentials = toStringRecord(JSON.parse(plaintext) as unknown);
      const provider = createNeo4jProvider(settings, credentials);
      if (!(provider instanceof Neo4jMemoryProvider)) throw new Error("provider is not neo4j");
      spaces.push(cleanupSpace(provider, config.spaceId));
    } catch (error) {
      const reason = error instanceof Error && error.message ? error.message : "connection failed";
      failures.push({ spaceId: config.spaceId, reason: reason.replace(/\s+/g, " ").slice(0, 300) });
      getLogger().error("memory cleanup skipped a space", error, {
        "memory.space_id": config.spaceId,
      });
    }
  }
  return { spaces, failed: failures.length, failures };
}

function cleanupSpace(provider: Neo4jMemoryProvider, spaceId: string): CleanupSpace {
  return {
    spaceId,
    listEntities: async (input) =>
      (await provider.listCleanupEntities(spaceId, input)).map(cleanupEntityFromRow),
    deleteEntities: (ids) => provider.deleteCleanupEntities(spaceId, ids),
    listDeclined: async () =>
      (await provider.listCleanupDeclined(spaceId)).map(cleanupDeclinedFromRow),
    forgetDeclined: async (pairs) => {
      for (const pair of pairs) {
        await provider.forgetDeclinedPair({ spaceId, ...pair });
      }
      return pairs.length;
    },
  };
}
