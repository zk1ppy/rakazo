import { loadRootEnv } from "@rakazo/core/node/load-root-env";

loadRootEnv();

import { closeNeo4jMemoryDrivers, parseCleanupGraphArgs } from "@rakazo/adapters";
import { createDb } from "@rakazo/db";
import { SERVICE_NAMES } from "@rakazo/logging";
import { createRootLogger } from "@rakazo/logging/axiom";
import { loadWorkerConfig } from "../config.js";
import { createWorkerSecretStore } from "../secret-store.js";
import { runCleanupGraph } from "./run-cleanup-graph.js";

/**
 * List entities the extractor would now drop, and refusals whose fingerprint
 * no longer matches. Dry-run is the default. `--apply` deletes them.
 *
 *   pnpm --filter @rakazo/worker memory:cleanup-graph -- --space SPACE_ID
 *   pnpm --filter @rakazo/worker memory:cleanup-graph -- --space SPACE_ID --apply
 */
const options = parseCleanupGraphArgs(process.argv.slice(2));
const config = loadWorkerConfig();
const logger = createRootLogger(SERVICE_NAMES.worker);

const { prisma, pool } = createDb(config.databaseUrl);
const secrets = await createWorkerSecretStore(config.env);
try {
  const text = await runCleanupGraph({ prisma, secrets, options });
  process.stdout.write(text);
} finally {
  await closeNeo4jMemoryDrivers();
  await secrets.close();
  await prisma.$disconnect().catch(() => undefined);
  await pool.end().catch(() => undefined);
  await logger.flush({ timeoutMs: 2_000 });
}
