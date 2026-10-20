import { loadRootEnv } from "@rakazo/core/node/load-root-env";

loadRootEnv();

import {
  backfillMemoryExtraction,
  closeNeo4jMemoryDrivers,
  GraphileJobPublisher,
  loadNeo4jBackfillSpaces,
  parseBackfillExtractArgs,
} from "@rakazo/adapters";
import { createDb } from "@rakazo/db";
import { loadWorkerConfig } from "../config.js";
import { createWorkerSecretStore } from "../secret-store.js";

/**
 * Repair fact times, then enqueue memory.extract for facts that are not
 * extracted yet. --repair-only skips the queue. --force re-enqueues extracted
 * facts. See memory-extract-backfill.ts for how an extracted fact is detected.
 */
const options = parseBackfillExtractArgs(process.argv.slice(2));
const config = loadWorkerConfig();

const { prisma, pool } = createDb(config.databaseUrl);
const secrets = await createWorkerSecretStore(config.env);
const jobs = new GraphileJobPublisher(pool);
try {
  const loaded = await loadNeo4jBackfillSpaces(prisma, secrets, options.spaceId);
  const counts = await backfillMemoryExtraction(loaded.spaces, jobs, {
    limit: options.limit,
    dryRun: options.dryRun,
    force: options.force,
    repairOnly: options.repairOnly,
  });
  const lines = [
    `dry-run ${options.dryRun}`,
    `repair-only ${options.repairOnly}`,
    `force ${options.force}`,
    `spaces ${counts.spaces}`,
    `disabled ${counts.disabled}`,
    `failed ${counts.failed + loaded.failed}`,
    `repaired ${options.dryRun ? "dry-run" : counts.repaired}`,
    `skipped ${counts.skipped}`,
    `enqueued ${counts.enqueued}`,
    `marked ${options.dryRun ? "dry-run" : counts.marked}`,
    `valid-from ${options.dryRun ? "dry-run" : counts.relations}`,
    `first-seen ${options.dryRun ? "dry-run" : counts.entities}`,
  ];
  process.stdout.write(`${lines.join("\n")}\n`);
} finally {
  await closeNeo4jMemoryDrivers();
  await jobs.close();
  await secrets.close();
  await prisma.$disconnect().catch(() => undefined);
  await pool.end().catch(() => undefined);
}
