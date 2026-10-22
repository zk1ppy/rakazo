import type { JobPublisher, SecretStore } from "@rakazo/adapter-kit";
import { backfillMemoryExtraction, loadNeo4jBackfillSpaces } from "@rakazo/adapters";
import type { PrismaClient } from "@rakazo/db";
import { runAsSystem } from "@rakazo/db";

export interface ExtractBackfillCliOptions {
  spaceId?: string;
  limit: number;
  dryRun: boolean;
  force: boolean;
  repairOnly: boolean;
  retryEmpty: boolean;
  includeFailed: boolean;
  since?: string;
}

/**
 * Read every Neo4j space, including secrets row-level security would hide from
 * the runtime login, then repair fact times and enqueue extraction.
 */
export async function runExtractBackfill(deps: {
  prisma: Pick<PrismaClient, "spaceMemoryConfig">;
  secrets: Pick<SecretStore, "load">;
  jobs: JobPublisher;
  options: ExtractBackfillCliOptions;
}): Promise<string> {
  return runAsSystem("backfill extract CLI", async () => {
    const loaded = await loadNeo4jBackfillSpaces(deps.prisma, deps.secrets, deps.options.spaceId);
    const counts = await backfillMemoryExtraction(loaded.spaces, deps.jobs, {
      limit: deps.options.limit,
      dryRun: deps.options.dryRun,
      force: deps.options.force,
      repairOnly: deps.options.repairOnly,
      retryEmpty: deps.options.retryEmpty,
      includeFailed: deps.options.includeFailed,
      ...(deps.options.since ? { since: deps.options.since } : {}),
    });
    const lines = [
      `dry-run ${deps.options.dryRun}`,
      `repair-only ${deps.options.repairOnly}`,
      `force ${deps.options.force}`,
      `retry-empty ${deps.options.retryEmpty}`,
      `include-failed ${deps.options.includeFailed}`,
      `since ${deps.options.since ?? "-"}`,
      `spaces ${counts.spaces}`,
      `disabled ${counts.disabled}`,
      ...loaded.failures.map((failure) => `space ${failure.spaceId} failed ${failure.reason}`),
      `failed ${counts.failed + loaded.failed}`,
      `repaired ${deps.options.dryRun ? "dry-run" : counts.repaired}`,
      `skipped ${counts.skipped}`,
      `enqueued ${counts.enqueued}`,
      `cleared ${deps.options.dryRun ? "dry-run" : counts.cleared}`,
      `sample ${counts.sample.join(" ") || "-"}`,
      `marked ${deps.options.dryRun ? "dry-run" : counts.marked}`,
      `valid-from ${deps.options.dryRun ? "dry-run" : counts.relations}`,
      `first-seen ${deps.options.dryRun ? "dry-run" : counts.entities}`,
    ];
    return `${lines.join("\n")}\n`;
  });
}
