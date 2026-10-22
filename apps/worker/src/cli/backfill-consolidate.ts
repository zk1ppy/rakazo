import { loadRootEnv } from "@rakazo/core/node/load-root-env";

loadRootEnv();

import {
  closeNeo4jMemoryDrivers,
  GraphileJobPublisher,
  parseBackfillConsolidateArgs,
} from "@rakazo/adapters";
import { createDb } from "@rakazo/db";
import { SERVICE_NAMES } from "@rakazo/logging";
import { createRootLogger } from "@rakazo/logging/axiom";
import { loadWorkerConfig } from "../config.js";
import { createWorkerSecretStore } from "../secret-store.js";
import { runConsolidateBackfill } from "./run-consolidate-backfill.js";

/**
 * Enqueue memory.consolidate. The worker process runs the job.
 * `--dry-run` prints the merge plan and relation counts and does not write.
 * See memory-consolidate.ts for the merge rules and the model budget.
 *
 *   pnpm --filter @rakazo/worker memory:backfill-consolidate -- --space SPACE_ID --dry-run
 */
const options = parseBackfillConsolidateArgs(process.argv.slice(2));
const config = loadWorkerConfig();
const logger = createRootLogger(SERVICE_NAMES.worker);

const { prisma, pool } = createDb(config.databaseUrl);
const secrets = await createWorkerSecretStore(config.env);
const jobs = new GraphileJobPublisher(pool);
try {
  const text = await runConsolidateBackfill({ prisma, secrets, jobs, options });
  process.stdout.write(text);
} finally {
  await closeNeo4jMemoryDrivers();
  await jobs.close();
  await secrets.close();
  await prisma.$disconnect().catch(() => undefined);
  await pool.end().catch(() => undefined);
  await logger.flush({ timeoutMs: 2_000 });
}
