import { loadRootEnv } from "@rakazo/core/node/load-root-env";

loadRootEnv();

import {
  backfillMemoryConsolidation,
  closeNeo4jMemoryDrivers,
  GraphileJobPublisher,
  loadNeo4jConsolidationSpaces,
  parseBackfillConsolidateArgs,
} from "@rakazo/adapters";
import { createDb } from "@rakazo/db";
import { loadWorkerConfig } from "../config.js";
import { createWorkerSecretStore } from "../secret-store.js";

/**
 * Enqueue memory.consolidate. The worker process runs the job.
 * `--dry-run` prints the merge plan and relation counts and does not write.
 * See memory-consolidate.ts for the merge rules and the model budget.
 */
const options = parseBackfillConsolidateArgs(process.argv.slice(2));
const config = loadWorkerConfig();

const { prisma, pool } = createDb(config.databaseUrl);
const secrets = await createWorkerSecretStore(config.env);
const jobs = new GraphileJobPublisher(pool);
try {
  const loaded = await loadNeo4jConsolidationSpaces(prisma, secrets, options.spaceId);
  const lines = [`dry-run ${options.dryRun}`, `spaces ${loaded.spaces.length}`];
  if (options.dryRun) {
    let failed = loaded.failed;
    for (const space of loaded.spaces) {
      if (!space.preview) {
        failed += 1;
        continue;
      }
      try {
        const preview = await space.preview(options.ns);
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
      } catch {
        failed += 1;
        lines.push(`space ${space.spaceId} preview-failed`);
      }
    }
    lines.push(`failed ${failed}`);
  } else {
    const counts = await backfillMemoryConsolidation(loaded.spaces, jobs, {
      limit: options.limit,
      dryRun: false,
      ...(options.ns ? { ns: options.ns } : {}),
    });
    lines.push(`failed ${counts.failed + loaded.failed}`, `enqueued ${counts.enqueued}`);
  }
  process.stdout.write(`${lines.join("\n")}\n`);
} finally {
  await closeNeo4jMemoryDrivers();
  await jobs.close();
  await secrets.close();
  await prisma.$disconnect().catch(() => undefined);
  await pool.end().catch(() => undefined);
}
