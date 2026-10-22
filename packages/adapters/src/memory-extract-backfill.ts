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
  /**
   * Facts with extractedAt and no outgoing :ABOUT edge. `since` keeps a fact
   * when the marker or the fact's stored time is at or after that ISO instant.
   */
  listEmptyExtractedFacts?(input: {
    limit: number;
    skip: number;
    since?: string;
  }): Promise<BackfillFact[]>;
  /**
   * Remove extractedAt from facts that still have no outgoing :ABOUT edge.
   * Returns the ids that were cleared.
   */
  clearExtractedAt?(factIds: readonly string[]): Promise<readonly string[]>;
}

export interface BackfillExtractOptions {
  limit: number;
  dryRun: boolean;
  /** Re-enqueue facts that are already extracted. */
  force: boolean;
  /** Repair stamps and enqueue nothing. */
  repairOnly: boolean;
  /**
   * Re-enqueue facts that have extractedAt and no outgoing :ABOUT edge.
   * Clears that marker only when this is not a dry run.
   */
  retryEmpty: boolean;
  /** ISO instant. With --retry-empty, keeps facts at or after this time. */
  since?: string;
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
  /** Facts whose extractedAt was removed. A dry run leaves this at 0. */
  cleared: number;
  /** Fact ids that were, or would be, re-enqueued. Capped. */
  sample: string[];
  relations: number;
  entities: number;
  dryRun: boolean;
  force: boolean;
  repairOnly: boolean;
  retryEmpty: boolean;
}

const EMPTY_EXTRACT_SAMPLE = 20;

const USAGE =
  "Usage: memory:backfill-extract [--] [--dry-run] [--repair-only] [--force] [--retry-empty] [--since ISO_DATE] [--space SPACE_ID] [--limit N]";

export function parseBackfillExtractArgs(argv: readonly string[]): {
  spaceId?: string;
  limit: number;
  dryRun: boolean;
  force: boolean;
  repairOnly: boolean;
  retryEmpty: boolean;
  since?: string;
} {
  let spaceId: string | undefined;
  let limit = DEFAULT_LIMIT;
  let dryRun = false;
  let force = false;
  let repairOnly = false;
  let retryEmpty = false;
  let since: string | undefined;
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
    if (arg === "--retry-empty") {
      retryEmpty = true;
      continue;
    }
    if (arg === "--since") {
      since = parseSince(argv[index + 1]);
      index += 1;
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
  if (retryEmpty && force) {
    throw new Error("--retry-empty and --force cannot be used together");
  }
  if (retryEmpty && repairOnly) {
    throw new Error("--retry-empty and --repair-only cannot be used together");
  }
  if (since && !retryEmpty) throw new Error("--since requires --retry-empty");
  return {
    ...(spaceId ? { spaceId } : {}),
    limit,
    dryRun,
    force,
    repairOnly,
    retryEmpty,
    ...(since ? { since } : {}),
  };
}

function parseSince(value: string | undefined): string {
  const trimmed = value?.trim() ?? "";
  if (
    !/^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}:\d{2})?)?$/.test(
      trimmed,
    )
  ) {
    throw new Error("--since needs an ISO date");
  }
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) throw new Error("--since needs an ISO date");
  return parsed.toISOString();
}

/**
 * Repair fact-time stamps, then enqueue extraction for facts that are not
 * extracted yet. Each job uses the global memory-extract queue and the fact's
 * replace key. The repair does not call a model.
 *
 * A fact is already extracted when it has `extractedAt` or an outgoing :ABOUT
 * edge. The extraction job sets `extractedAt` after every chunk finishes,
 * including a complete result with no entities. A truncated reply does not
 * set the marker. Facts written before that property existed have no marker;
 * an :ABOUT edge is the record that extraction stored a graph. This pass
 * copies the fact's stored time onto `extractedAt` for those facts. A finished
 * extraction that stored nothing and predates the property has neither signal,
 * so the next default run enqueues it once. `--force` ignores both signals.
 * `--repair-only` enqueues nothing. `--retry-empty` enqueues only facts that
 * have the marker and no outgoing :ABOUT edge, and clears the marker when it
 * is not a dry run. `--since` limits that pass to a fact whose stored time or
 * `extractedAt` is at or after the given instant.
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
    cleared: 0,
    sample: [],
    relations: 0,
    entities: 0,
    dryRun: options.dryRun,
    force: options.force,
    repairOnly: options.repairOnly,
    retryEmpty: options.retryEmpty,
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
        getLogger().error("memory backfill could not realign fact times", error, {
          "memory.space_id": space.spaceId,
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
        getLogger().error("memory backfill could not stamp extracted facts", error, {
          "memory.space_id": space.spaceId,
        });
      }
    }
    if (
      !options.force &&
      !options.retryEmpty &&
      space.countExtractedFacts &&
      (options.repairOnly || remaining > 0)
    ) {
      try {
        counts.skipped += await space.countExtractedFacts();
      } catch (error) {
        counts.failed += 1;
        getLogger().error("memory backfill could not count extracted facts", error, {
          "memory.space_id": space.spaceId,
        });
      }
    }
    if (options.retryEmpty) {
      if (remaining > 0) {
        remaining = await enqueueEmptyExtractions(space, jobs, options, counts, remaining);
      }
      continue;
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
        if (!options.dryRun) await enqueueExtractJob(space, jobs, fact);
        counts.enqueued += 1;
        remaining -= 1;
      }
      skip += facts.length;
      if (facts.length < requested) break;
    }
  }
  return counts;
}

/**
 * Re-queue facts that were marked extracted and stored no :ABOUT edge.
 * The list is not mutated until every selected page is known, so a dry run
 * and a real run page the same way. The marker is cleared before enqueue, and
 * only for ids that still have no outgoing :ABOUT edge. A fact that gained an
 * edge between the read and the clear stays marked and is not enqueued.
 */
async function enqueueEmptyExtractions(
  space: BackfillSpace,
  jobs: JobPublisher,
  options: BackfillExtractOptions,
  counts: BackfillExtractCounts,
  remaining: number,
): Promise<number> {
  if (!space.listEmptyExtractedFacts) {
    counts.failed += 1;
    getLogger().error("memory backfill cannot list empty extractions", {
      "memory.space_id": space.spaceId,
    });
    return remaining;
  }
  const selected: BackfillFact[] = [];
  let skip = 0;
  while (remaining > 0) {
    const requested = remaining;
    let facts: BackfillFact[];
    try {
      facts = await space.listEmptyExtractedFacts({
        limit: requested,
        skip,
        ...(options.since ? { since: options.since } : {}),
      });
    } catch (error) {
      counts.failed += 1;
      getLogger().error("memory backfill could not list empty extractions", error, {
        "memory.space_id": space.spaceId,
      });
      break;
    }
    if (facts.length === 0) break;
    for (const fact of facts) {
      if (remaining <= 0) break;
      if (!retryableEmptyFact(fact)) {
        counts.skipped += 1;
        continue;
      }
      selected.push(fact);
      remaining -= 1;
    }
    skip += facts.length;
    if (facts.length < requested) break;
  }
  if (selected.length === 0) return remaining;
  let queued = selected;
  if (!options.dryRun) {
    if (!space.clearExtractedAt) {
      counts.failed += 1;
      getLogger().error("memory backfill cannot clear extractedAt", {
        "memory.space_id": space.spaceId,
      });
      return remaining + selected.length;
    }
    try {
      const cleared = new Set(await space.clearExtractedAt(selected.map((fact) => fact.factId)));
      counts.cleared += cleared.size;
      queued = selected.filter((fact) => cleared.has(fact.factId));
    } catch (error) {
      counts.failed += 1;
      getLogger().error("memory backfill could not clear extractedAt", error, {
        "memory.space_id": space.spaceId,
      });
      return remaining + selected.length;
    }
  }
  for (const fact of queued) {
    if (!options.dryRun) await enqueueExtractJob(space, jobs, fact);
    counts.enqueued += 1;
    if (counts.sample.length < EMPTY_EXTRACT_SAMPLE) counts.sample.push(fact.factId);
  }
  return remaining + (selected.length - queued.length);
}

async function enqueueExtractJob(
  space: BackfillSpace,
  jobs: JobPublisher,
  fact: BackfillFact,
): Promise<void> {
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

function retryableEmptyFact(fact: BackfillFact): boolean {
  return Boolean(
    fact.factId.trim() && fact.ns.trim() && fact.botId.trim() && !isTrivialMemoryText(fact.text),
  );
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
): Promise<{
  spaces: BackfillSpace[];
  failed: number;
  failures: Array<{ spaceId: string; reason: string }>;
}> {
  const configs = await prisma.spaceMemoryConfig.findMany({
    where: { provider: "neo4j", ...(spaceId ? { spaceId } : {}) },
    include: { secret: true },
  });
  const spaces: BackfillSpace[] = [];
  const failures: Array<{ spaceId: string; reason: string }> = [];
  for (const config of configs) {
    try {
      const secret = config.secret;
      if (!secret?.ciphertext || !secret.id) {
        throw new Error("memory secret is not visible");
      }
      const settings = toStringRecord(config.settings);
      const plaintext = await secrets.load(secret.ciphertext, secret.id);
      const credentials = toStringRecord(JSON.parse(plaintext) as unknown);
      const provider = createNeo4jProvider(settings, credentials);
      if (!(provider instanceof Neo4jMemoryProvider)) {
        throw new Error("provider is not neo4j");
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
        listEmptyExtractedFacts: (input) => provider.listEmptyExtractedFacts(config.spaceId, input),
        clearExtractedAt: (factIds) => provider.clearExtractedAt(config.spaceId, factIds),
      });
    } catch (error) {
      const reason = failureReason(error, "connection failed");
      failures.push({ spaceId: config.spaceId, reason });
      getLogger().error("memory backfill skipped a space", error, {
        "memory.space_id": config.spaceId,
      });
    }
  }
  return { spaces, failed: failures.length, failures };
}

function failureReason(error: unknown, fallback: string): string {
  const text = error instanceof Error && error.message ? error.message : fallback;
  return text.replace(/\s+/g, " ").slice(0, 300);
}
