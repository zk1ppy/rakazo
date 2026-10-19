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
 * See memory-consolidate.ts for the merge rules and the model budget.
 */
const options = parseBackfillConsolidateArgs(process.argv.slice(2));
const config = loadWorkerConfig();

const { prisma, pool } = createDb(config.databaseUrl);
const secrets = await createWorkerSecretStore(config.env);
const jobs = new GraphileJobPublisher(pool);
try {
  const loaded = await loadNeo4jConsolidationSpaces(prisma, secrets, options.spaceId);
  const counts = await backfillMemoryConsolidation(loaded.spaces, jobs, {
    limit: options.limit,
    dryRun: options.dryRun,
    ...(options.ns ? { ns: options.ns } : {}),
  });
  const lines = [
    `dry-run ${options.dryRun}`,
    `spaces ${counts.spaces}`,
    `failed ${counts.failed + loaded.failed}`,
    `${options.dryRun ? "would-enqueue" : "enqueued"} ${counts.enqueued}`,
  ];
  process.stdout.write(`${lines.join("\n")}\n`);
} finally {
  await closeNeo4jMemoryDrivers();
  await jobs.close();
  await secrets.close();
  await prisma.$disconnect().catch(() => undefined);
  await pool.end().catch(() => undefined);
}
