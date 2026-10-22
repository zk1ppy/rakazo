import type { JobPublisher, SecretStore } from "@rakazo/adapter-kit";
import { memoryConsolidateJob } from "@rakazo/adapter-kit";
import type { PrismaClient } from "@rakazo/db";
import { getLogger } from "@rakazo/logging";
import type { ConsolidationPreview } from "./memory-consolidate.js";
import { previewMemoryConsolidation } from "./memory-consolidate.js";
import { neo4jDreamEnabled } from "./memory-dream.js";
import { toStringRecord } from "./memory-provider-factory.js";
import { createNeo4jProvider, Neo4jMemoryProvider } from "./neo4j-memory-provider.js";

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 5_000;

/**
 * Enqueue `memory.consolidate` for Neo4j spaces. Each job scans one space (or
 * one namespace) and merges duplicate entities. Jobs share the memory-extract
 * queue, so consolidate and extract model calls stay serial. The worker must
 * be running to execute them.
 *
 *   pnpm --filter @rakazo/worker memory:backfill-consolidate -- --space SPACE_ID
 *   pnpm --filter @rakazo/worker memory:backfill-consolidate -- --space SPACE_ID --dry-run
 *   pnpm --filter @rakazo/worker memory:backfill-consolidate -- --space SPACE_ID --ns shared
 *
 * `--dry-run` reads the graph and prints pairs that would merge, pairs that
 * would be sent to the model, and how many parallel relations would collapse.
 * It does not enqueue a job and does not write.
 */
export interface ConsolidateSpace {
  spaceId: string;
  userId: string;
  /** Set by the loader. Manual backfill ignores it and still enqueues. */
  dreamEnabled?: boolean;
  /** Read-only plan. Present when the loader opened a Neo4j provider. */
  preview?: (ns?: string) => Promise<ConsolidationPreview>;
}

export interface BackfillConsolidateOptions {
  limit: number;
  dryRun: boolean;
  ns?: string;
}

export interface BackfillConsolidateCounts {
  spaces: number;
  failed: number;
  enqueued: number;
  dryRun: boolean;
}

export function parseBackfillConsolidateArgs(argv: readonly string[]): {
  spaceId?: string;
  ns?: string;
  limit: number;
  dryRun: boolean;
} {
  let spaceId: string | undefined;
  let ns: string | undefined;
  let limit = DEFAULT_LIMIT;
  let dryRun = false;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
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
    if (arg === "--ns") {
      const value = argv[index + 1]?.trim();
      index += 1;
      if (!value) throw new Error("--ns needs a namespace");
      ns = value;
      continue;
    }
    throw new Error(
      "Usage: memory:backfill-consolidate [--] [--dry-run] [--space SPACE_ID] [--ns NAMESPACE] [--limit N]",
    );
  }
  return { ...(spaceId ? { spaceId } : {}), ...(ns ? { ns } : {}), limit, dryRun };
}

/** Enqueue one consolidate job per space, up to the limit. A dry run only counts. */
export async function backfillMemoryConsolidation(
  spaces: readonly ConsolidateSpace[],
  jobs: JobPublisher,
  options: BackfillConsolidateOptions,
): Promise<BackfillConsolidateCounts> {
  const counts: BackfillConsolidateCounts = {
    spaces: spaces.length,
    failed: 0,
    enqueued: 0,
    dryRun: options.dryRun,
  };
  for (const space of spaces) {
    if (counts.enqueued >= options.limit) break;
    if (!space.spaceId.trim() || !space.userId.trim()) {
      counts.failed += 1;
      continue;
    }
    if (!options.dryRun) {
      await jobs.enqueue(
        memoryConsolidateJob({
          spaceId: space.spaceId,
          userId: space.userId,
          ...(options.ns ? { ns: options.ns } : {}),
          attempt: 0,
          scheduleGeneration: 0,
        }),
      );
    }
    counts.enqueued += 1;
  }
  return counts;
}

export async function loadNeo4jConsolidationSpaces(
  prisma: Pick<PrismaClient, "spaceMemoryConfig">,
  secrets: Pick<SecretStore, "load">,
  spaceId?: string,
): Promise<{ spaces: ConsolidateSpace[]; failed: number }> {
  const configs = await prisma.spaceMemoryConfig.findMany({
    where: { provider: "neo4j", ...(spaceId ? { spaceId } : {}) },
    include: { secret: true },
  });
  const spaces: ConsolidateSpace[] = [];
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
        dreamEnabled: neo4jDreamEnabled(settings),
        preview: (ns) =>
          previewMemoryConsolidation(provider, {
            spaceId: config.spaceId,
            ...(ns ? { ns } : {}),
          }),
      });
    } catch (error) {
      failed += 1;
      getLogger().error("memory consolidate skipped a space", {
        "memory.space_id": config.spaceId,
        message: error instanceof Error ? error.message : "connection failed",
      });
    }
  }
  return { spaces, failed };
}
