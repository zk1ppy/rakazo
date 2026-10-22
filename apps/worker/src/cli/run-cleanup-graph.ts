import type { SecretStore } from "@rakazo/adapter-kit";
import { cleanupMemoryGraph, formatCleanupReport, loadNeo4jCleanupSpaces } from "@rakazo/adapters";
import type { PrismaClient } from "@rakazo/db";
import { runAsSystem } from "@rakazo/db";

export interface CleanupGraphCliOptions {
  spaceId?: string;
  dryRun: boolean;
}

/**
 * List path-shaped entities and stale refusals. The default is a dry run.
 * `--apply` deletes the entities and the stale refusals.
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
