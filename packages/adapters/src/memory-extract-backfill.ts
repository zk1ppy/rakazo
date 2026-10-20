import type { JobPublisher, SecretStore } from "@rakazo/adapter-kit";
import { memoryExtractJob } from "@rakazo/adapter-kit";
import type { PrismaClient } from "@rakazo/db";
import { getLogger } from "@rakazo/logging";
import { isTrivialMemoryText, neo4jMaxChunkChars, planExtractionChunks } from "./memory-extract.js";
import { toStringRecord } from "./memory-provider-factory.js";
import { createNeo4jProvider, Neo4jMemoryProvider } from "./neo4j-memory-provider.js";

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 5_000;

export interface BackfillFact {
  factId: string;
  ns: string;
  botId: string;
  text: string;
  /** Set when the fact already has extractedAt or an outgoing :ABOUT edge. */
  extracted?: boolean;
}

export interface BackfillSpace {
  spaceId: string;
  userId: string;
  extractionEnabled: boolean;
  maxChunkChars: number;
  listFacts(input: {
    limit: number;
    skip: number;
    includeExtracted: boolean;
  }): Promise<BackfillFact[]>;
  /** Repair fact-time stamps on edges already written. No model call. */
  realignTimes?(): Promise<{ relations: number; entities: number }>;
  /** Live facts that already have extractedAt or an outgoing :ABOUT edge. */
  countExtractedFacts?(): Promise<number>;
  /**
   * Set extractedAt from the fact's stored time when an :ABOUT edge already
   * exists and the marker does not. No model call.
   */
  stampExtractedFacts?(): Promise<number>;
}

export interface BackfillExtractOptions {
  limit: number;
  dryRun: boolean;
  /** Re-enqueue facts that are already extracted. */
  force: boolean;
  /** Repair stamps and enqueue nothing. */
  repairOnly: boolean;
}

export interface BackfillExtractCounts {
  spaces: number;
  disabled: number;
  failed: number;
  /** Graph stamps rewritten: relation validFrom/lastSeen plus entity firstSeen. */
  repaired: number;
  /** Facts left out of the queue. */
  skipped: number;
  enqueued: number;
  /** Facts that gained extractedAt from an existing :ABOUT edge. */
  marked: number;
  relations: number;
  entities: number;
  dryRun: boolean;
  force: boolean;
  repairOnly: boolean;
}

const USAGE =
  "Usage: memory:backfill-extract [--] [--dry-run] [--repair-only] [--force] [--space SPACE_ID] [--limit N]";

export function parseBackfillExtractArgs(argv: readonly string[]): {
  spaceId?: string;
  limit: number;
  dryRun: boolean;
  force: boolean;
  repairOnly: boolean;
} {
  let spaceId: string | undefined;
  let limit = DEFAULT_LIMIT;
  let dryRun = false;
  let force = false;
  let repairOnly = false;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    // pnpm run forwards the `--` separator, so `-- --dry-run` arrives as ["--", "--dry-run"].
    if (arg === "--") continue;
    if (arg === "--dry-run") {
      dryRun = true;
      continue;
    }
    if (arg === "--force") {
      force = true;
      continue;
    }
    if (arg === "--repair-only") {
      repairOnly = true;
      continue;
    }
    if (arg === "--limit") {
      const value = argv[index + 1];
      index += 1;
      if (!value || !/^\d+$/.test(value)) throw new Error("--limit needs a whole number");
      limit = Number(value);
      if (limit < 1 || limit > MAX_LIMIT) {
        throw new Error(`--limit must be between 1 and ${MAX_LIMIT}`);
      }
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
  if (force && repairOnly) {
    throw new Error("--force and --repair-only cannot be used together");
  }
  return { ...(spaceId ? { spaceId } : {}), limit, dryRun, force, repairOnly };
}

/**
 * Repair fact-time stamps, then enqueue extraction for facts that are not
 * extracted yet. Each job uses the global memory-extract queue and the fact's
 * replace key. The repair does not call a model.
 *
 * A fact is already extracted when it has `extractedAt` or an outgoing :ABOUT
 * edge. The extraction job sets `extractedAt` after every chunk finishes,
 * including a result with no entities. Facts written before that property
 * existed have no marker; an :ABOUT edge is the record that extraction stored
 * a graph. This pass copies the fact's stored time onto `extractedAt` for
 * those facts. A finished extraction that stored nothing and predates the
 * property has neither signal, so the next default run enqueues it once.
 * `--force` ignores both signals. `--repair-only` enqueues nothing.
 */
export async function backfillMemoryExtraction(
  spaces: readonly BackfillSpace[],
  jobs: JobPublisher,
  options: BackfillExtractOptions,
): Promise<BackfillExtractCounts> {
  const counts: BackfillExtractCounts = {
    spaces: spaces.length,
    disabled: 0,
    failed: 0,
    repaired: 0,
    skipped: 0,
    enqueued: 0,
    marked: 0,
    relations: 0,
    entities: 0,
    dryRun: options.dryRun,
    force: options.force,
    repairOnly: options.repairOnly,
  };
  let remaining = options.limit;
  for (const space of spaces) {
    if (!space.extractionEnabled) {
      counts.disabled += 1;
      continue;
    }
    if (!options.dryRun && space.realignTimes) {
      try {
        const repaired = await space.realignTimes();
        counts.relations += repaired.relations;
        counts.entities += repaired.entities;
        counts.repaired += repaired.relations + repaired.entities;
        if (repaired.relations > 0 || repaired.entities > 0) {
          getLogger().info("memory backfill realigned fact times", {
            "memory.space_id": space.spaceId,
            "memory.relations": repaired.relations,
            "memory.entities": repaired.entities,
          });
        }
      } catch (error) {
        counts.failed += 1;
        getLogger().error("memory backfill could not realign fact times", {
          "memory.space_id": space.spaceId,
          message: error instanceof Error ? error.message : "realign failed",
        });
      }
    }
    if (!options.dryRun && space.stampExtractedFacts) {
      try {
        const marked = await space.stampExtractedFacts();
        counts.marked += marked;
        if (marked > 0) {
          getLogger().info("memory backfill stamped extracted facts", {
            "memory.space_id": space.spaceId,
            "memory.marked": marked,
          });
        }
      } catch (error) {
        counts.failed += 1;
        getLogger().error("memory backfill could not stamp extracted facts", {
          "memory.space_id": space.spaceId,
          message: error instanceof Error ? error.message : "stamp failed",
        });
      }
    }
    if (!options.force && space.countExtractedFacts && (options.repairOnly || remaining > 0)) {
      try {
        counts.skipped += await space.countExtractedFacts();
      } catch (error) {
        counts.failed += 1;
        getLogger().error("memory backfill could not count extracted facts", {
          "memory.space_id": space.spaceId,
          message: error instanceof Error ? error.message : "count failed",
        });
      }
    }
    if (options.repairOnly || remaining <= 0) continue;
    let skip = 0;
    while (remaining > 0) {
      const requested = remaining;
      const facts = await space.listFacts({
        limit: requested,
        skip,
        includeExtracted: options.force,
      });
      if (facts.length === 0) break;
      for (const fact of facts) {
        if (remaining <= 0) break;
        if (!enqueueableFact(fact, options.force)) {
          counts.skipped += 1;
          continue;
        }
        if (!options.dryRun) {
          const planned = planExtractionChunks(fact.text, space.maxChunkChars);
          await jobs.enqueue(
            memoryExtractJob({
              spaceId: space.spaceId,
              userId: space.userId,
              botId: fact.botId,
              factId: fact.factId,
              ns: fact.ns,
              maxChunkChars: planned.maxChunkChars,
              chunkIndex: 0,
              attempt: 0,
              scheduleGeneration: 0,
            }),
          );
        }
        counts.enqueued += 1;
        remaining -= 1;
      }
      skip += facts.length;
      if (facts.length < requested) break;
    }
  }
  return counts;
}

function enqueueableFact(fact: BackfillFact, force: boolean): boolean {
  if (fact.extracted && !force) return false;
  return Boolean(
    fact.factId.trim() && fact.ns.trim() && fact.botId.trim() && !isTrivialMemoryText(fact.text),
  );
}

export async function loadNeo4jBackfillSpaces(
  prisma: Pick<PrismaClient, "spaceMemoryConfig">,
  secrets: Pick<SecretStore, "load">,
  spaceId?: string,
): Promise<{ spaces: BackfillSpace[]; failed: number }> {
  const configs = await prisma.spaceMemoryConfig.findMany({
    where: { provider: "neo4j", ...(spaceId ? { spaceId } : {}) },
    include: { secret: true },
  });
  const spaces: BackfillSpace[] = [];
  let failed = 0;
  for (const config of configs) {
    try {
      const settings = toStringRecord(config.settings);
      const plaintext = await secrets.load(config.secret.ciphertext, config.secret.id);
      const credentials = toStringRecord(JSON.parse(plaintext) as unknown);
      const provider = createNeo4jProvider(settings, credentials);
      if (!(provider instanceof Neo4jMemoryProvider)) {
        failed += 1;
        continue;
      }
      spaces.push({
        spaceId: config.spaceId,
        userId: config.userId,
        extractionEnabled: provider.extractionEnabled(),
        maxChunkChars: neo4jMaxChunkChars(settings),
        listFacts: (input) => provider.listUnextractedFacts(config.spaceId, input),
        realignTimes: () => provider.realignExtractionTimes(config.spaceId),
        countExtractedFacts: () => provider.countExtractedFacts(config.spaceId),
        stampExtractedFacts: () => provider.stampExtractedFacts(config.spaceId),
      });
    } catch (error) {
      failed += 1;
      getLogger().error("memory backfill skipped a space", {
        "memory.space_id": config.spaceId,
        message: error instanceof Error ? error.message : "connection failed",
      });
    }
  }
  return { spaces, failed };
}
