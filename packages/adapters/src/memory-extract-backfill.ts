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
}

export interface BackfillSpace {
  spaceId: string;
  userId: string;
  extractionEnabled: boolean;
  maxChunkChars: number;
  listFacts(input: { limit: number; skip: number }): Promise<BackfillFact[]>;
}

export interface BackfillExtractOptions {
  limit: number;
  dryRun: boolean;
}

export interface BackfillExtractCounts {
  spaces: number;
  disabled: number;
  failed: number;
  skipped: number;
  enqueued: number;
  dryRun: boolean;
}

export function parseBackfillExtractArgs(argv: readonly string[]): {
  spaceId?: string;
  limit: number;
  dryRun: boolean;
} {
  let spaceId: string | undefined;
  let limit = DEFAULT_LIMIT;
  let dryRun = false;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    // pnpm run forwards the `--` separator, so `-- --dry-run` arrives as ["--", "--dry-run"].
    if (arg === "--") continue;
    if (arg === "--dry-run") {
      dryRun = true;
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
    throw new Error(
      "Usage: memory:backfill-extract [--] [--dry-run] [--space SPACE_ID] [--limit N]",
    );
  }
  return { ...(spaceId ? { spaceId } : {}), limit, dryRun };
}

/**
 * Enqueue extraction for facts that have no :ABOUT edge yet.
 * Each job uses the global memory-extract queue and the fact's replace key.
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
    skipped: 0,
    enqueued: 0,
    dryRun: options.dryRun,
  };
  let remaining = options.limit;
  for (const space of spaces) {
    if (remaining <= 0) break;
    if (!space.extractionEnabled) {
      counts.disabled += 1;
      continue;
    }
    let skip = 0;
    while (remaining > 0) {
      const requested = remaining;
      const facts = await space.listFacts({ limit: requested, skip });
      if (facts.length === 0) break;
      for (const fact of facts) {
        if (remaining <= 0) break;
        if (!enqueueableFact(fact)) {
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

function enqueueableFact(fact: BackfillFact): boolean {
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
