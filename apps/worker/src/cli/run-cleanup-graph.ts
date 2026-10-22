import type { SecretStore } from "@rakazo/adapter-kit";
import { cleanupMemoryGraph, formatCleanupReport, loadNeo4jCleanupSpaces } from "@rakazo/adapters";
import type { PrismaClient } from "@rakazo/db";
import { runAsSystem } from "@rakazo/db";

export interface CleanupGraphCliOptions {
  spaceId?: string;
  dryRun: boolean;
}

/**
 * List noise entities, event hubs, object hubs, and stale refusals.
 * The default is a dry run. `--apply` deletes noise entities and stale refusals.
 * Hubs are listed and left in place.
 */
export async function runCleanupGraph(deps: {
  prisma: Pick<PrismaClient, "spaceMemoryConfig">;
  secrets: Pick<SecretStore, "load">;
  options: CleanupGraphCliOptions;
}): Promise<string> {
  return runAsSystem("cleanup graph CLI", async () => {
    const loaded = await loadNeo4jCleanupSpaces(deps.prisma, deps.secrets, deps.options.spaceId);
    const counts = await cleanupMemoryGraph(loaded.spaces, { dryRun: deps.options.dryRun });
    return formatCleanupReport({ ...counts, failed: counts.failed }, loaded.failures);
  });
}
