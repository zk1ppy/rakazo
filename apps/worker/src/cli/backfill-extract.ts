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
  });
  const lines = [
    `dry-run ${options.dryRun}`,
    `spaces ${counts.spaces}`,
    `disabled ${counts.disabled}`,
    `failed ${counts.failed + loaded.failed}`,
    `skipped ${counts.skipped}`,
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
