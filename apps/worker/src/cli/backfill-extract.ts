import { loadRootEnv } from "@rakazo/core/node/load-root-env";

loadRootEnv();

import {
  closeNeo4jMemoryDrivers,
  GraphileJobPublisher,
  parseBackfillExtractArgs,
} from "@rakazo/adapters";
import { createDb } from "@rakazo/db";
import { SERVICE_NAMES } from "@rakazo/logging";
import { createRootLogger } from "@rakazo/logging/axiom";
import { loadWorkerConfig } from "../config.js";
import { createWorkerSecretStore } from "../secret-store.js";
import { runExtractBackfill } from "./run-extract-backfill.js";

/**
 * Repair fact times, then enqueue memory.extract for facts that are not
 * extracted yet. --repair-only skips the queue. --force re-enqueues extracted
 * facts. See memory-extract-backfill.ts for how an extracted fact is detected.
 *
 *   pnpm --filter @rakazo/worker memory:backfill-extract -- --space SPACE_ID --dry-run
 */
const options = parseBackfillExtractArgs(process.argv.slice(2));
const config = loadWorkerConfig();
const logger = createRootLogger(SERVICE_NAMES.worker);

const { prisma, pool } = createDb(config.databaseUrl);
const secrets = await createWorkerSecretStore(config.env);
const jobs = new GraphileJobPublisher(pool);
try {
  const text = await runExtractBackfill({ prisma, secrets, jobs, options });
  process.stdout.write(text);
} finally {
  await closeNeo4jMemoryDrivers();
  await jobs.close();
  await secrets.close();
  await prisma.$disconnect().catch(() => undefined);
  await pool.end().catch(() => undefined);
  await logger.flush({ timeoutMs: 2_000 });
}
