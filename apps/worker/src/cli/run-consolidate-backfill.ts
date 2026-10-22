import type { JobPublisher, SecretStore } from "@rakazo/adapter-kit";
import type { ConsolidateSpace } from "@rakazo/adapters";
import { backfillMemoryConsolidation, loadNeo4jConsolidationSpaces } from "@rakazo/adapters";
import type { PrismaClient } from "@rakazo/db";
import { runAsSystem } from "@rakazo/db";
import { getLogger } from "@rakazo/logging";

export interface ConsolidateBackfillCliOptions {
  spaceId?: string;
  ns?: string;
  limit: number;
  dryRun: boolean;
}

/**
 * Read every Neo4j space, including secrets row-level security would hide from
 * the runtime login, then enqueue or describe the consolidate jobs.
 */
export async function runConsolidateBackfill(deps: {
  prisma: Pick<PrismaClient, "spaceMemoryConfig">;
  secrets: Pick<SecretStore, "load">;
  jobs: JobPublisher;
  options: ConsolidateBackfillCliOptions;
}): Promise<string> {
  return runAsSystem("backfill consolidate CLI", async () => {
    const loaded = await loadNeo4jConsolidationSpaces(
      deps.prisma,
      deps.secrets,
      deps.options.spaceId,
    );
    const lines = [`dry-run ${deps.options.dryRun}`, `spaces ${loaded.spaces.length}`];
    for (const failure of loaded.failures) {
      lines.push(`space ${failure.spaceId} failed ${failure.reason}`);
    }
    if (deps.options.dryRun) {
      const preview = await describeConsolidatePreview(loaded.spaces, deps.options.ns);
      lines.push(...preview.lines, `failed ${loaded.failed + preview.failed}`);
    } else {
      const counts = await backfillMemoryConsolidation(loaded.spaces, deps.jobs, {
        limit: deps.options.limit,
        dryRun: false,
        ...(deps.options.ns ? { ns: deps.options.ns } : {}),
      });
      lines.push(`failed ${counts.failed + loaded.failed}`, `enqueued ${counts.enqueued}`);
    }
    return `${lines.join("\n")}\n`;
  });
}

export async function describeConsolidatePreview(
  spaces: readonly ConsolidateSpace[],
  ns?: string,
): Promise<{ lines: string[]; failed: number }> {
  const lines: string[] = [];
  let failed = 0;
  for (const space of spaces) {
    if (!space.preview) {
      failed += 1;
      const reason = "preview is unavailable";
      lines.push(`space ${space.spaceId} failed ${reason}`);
      getLogger().error("memory consolidate preview failed", new Error(reason), {
        "memory.space_id": space.spaceId,
      });
      continue;
    }
    try {
      const preview = await space.preview(ns);
      lines.push(
        [
          `space ${space.spaceId}`,
          `would-merge ${preview.merges.length}`,
          `would-confirm ${preview.confirms.length}`,
          `would-relations ${preview.relations}`,
          `would-about ${preview.about}`,
          `would-from ${preview.from}`,
          `pages ${preview.pages}`,
          `truncated ${preview.truncated}`,
        ].join(" "),
      );
      for (const pair of [...preview.merges, ...preview.confirms].slice(0, 40)) {
        lines.push(
          `pair ${pair.action} ${pair.reason} ${pair.leftType}:${pair.leftName} -> ${pair.rightType}:${pair.rightName} type=${pair.type}`,
        );
      }
    } catch (error) {
      failed += 1;
      const reason = error instanceof Error && error.message ? error.message : "preview failed";
      const line = reason.replace(/\s+/g, " ").slice(0, 300);
      lines.push(`space ${space.spaceId} failed ${line}`);
      getLogger().error("memory consolidate preview failed", error, {
        "memory.space_id": space.spaceId,
      });
    }
  }
  return { lines, failed };
}
