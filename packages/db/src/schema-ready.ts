/**
 * The worker starts beside the API, and the API is the process that applies
 * migrations and the runtime grants. Until that finishes, the runtime login
 * cannot see `space_keys` or use `graphile_worker`. Those failures are the
 * deploy race, so the worker waits for them instead of exiting.
 */
const NOT_READY = new Set(["42P01", "3F000", "42501"]);

const PROBES = [
  "SELECT 1 FROM space_keys LIMIT 0",
  "SELECT 1 FROM graphile_worker._private_jobs LIMIT 0",
] as const;

export interface SchemaReadyOptions {
  /** Whole wait. 0 probes once and throws if the schema is not ready. */
  timeoutMs: number;
  delayMs: number;
  maxDelayMs: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
  onWaiting?: (detail: { attempt: number; delayMs: number; reason: string }) => void;
  /** Called only after at least one wait, when the probes then succeed. */
  onReady?: () => void;
}

/** True for a missing relation or schema, or a grant the migration has not applied yet. */
export function isSchemaNotReady(error: unknown): boolean {
  const code = postgresCode(error);
  return code !== undefined && NOT_READY.has(code);
}

/**
 * Probe the tables the worker needs. Retries only the migration race.
 * Any other database error (authentication, saturation) is thrown at once.
 */
export async function waitForRuntimeSchema(
  db: { query: (sql: string) => Promise<unknown> },
  options: SchemaReadyOptions,
): Promise<void> {
  const now = options.now ?? Date.now;
  const sleep =
    options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const started = now();
  let delayMs = Math.max(1, options.delayMs);
  const maxDelayMs = Math.max(delayMs, options.maxDelayMs);
  let attempt = 0;
  for (;;) {
    try {
      for (const sql of PROBES) await db.query(sql);
      if (attempt > 0) options.onReady?.();
      return;
    } catch (error) {
      if (!isSchemaNotReady(error)) throw error;
      const elapsed = now() - started;
      const remaining = options.timeoutMs - elapsed;
      if (remaining <= 0) {
        const reason = error instanceof Error ? error.message : "schema is not ready";
        throw new Error(
          `Database migrations or runtime grants were not ready after ${options.timeoutMs}ms: ${reason}`,
          { cause: error instanceof Error ? error : undefined },
        );
      }
      const wait = Math.min(delayMs, maxDelayMs, remaining);
      options.onWaiting?.({ attempt, delayMs: wait, reason: errorText(error) });
      await sleep(wait);
      delayMs = Math.min(delayMs * 2, maxDelayMs);
      attempt += 1;
    }
  }
}

function errorText(error: unknown): string {
  return error instanceof Error && error.message ? error.message : "schema is not ready";
}

function postgresCode(error: unknown, seen = new Set<unknown>()): string | undefined {
  if (!error || typeof error !== "object" || seen.has(error)) return undefined;
  seen.add(error);
  const direct = (error as { code?: unknown }).code;
  if (typeof direct === "string" && /^[0-9A-Z]{5}$/.test(direct)) return direct;
  const meta = (error as { meta?: { code?: unknown } }).meta?.code;
  if (typeof meta === "string" && /^[0-9A-Z]{5}$/.test(meta)) return meta;
  return postgresCode((error as { cause?: unknown }).cause, seen);
}
