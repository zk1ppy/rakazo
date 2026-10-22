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
 * facts. --retry-empty re-enqueues facts that have extractedAt and no outgoing
 * :ABOUT edge, and clears that marker unless this is a dry run. --since limits
 * that pass to an ISO date. See memory-extract-backfill.ts.
 *
 *   pnpm --filter @rakazo/worker memory:backfill-extract -- --space SPACE_ID --dry-run
 *   pnpm --filter @rakazo/worker memory:backfill-extract -- --retry-empty --since 2026-10-22T16:00:00Z --dry-run
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
