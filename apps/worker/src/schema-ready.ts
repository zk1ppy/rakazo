/**
 * How long the worker waits for the API's `db:deploy` to create the schema and
 * grant it to the runtime login. Unset or invalid values keep the defaults.
 * A timeout of 0 probes once.
 */
export const DEFAULT_SCHEMA_READY_TIMEOUT_MS = 120_000;
export const DEFAULT_SCHEMA_READY_DELAY_MS = 1_000;
export const DEFAULT_SCHEMA_READY_MAX_DELAY_MS = 10_000;

const MAX_TIMEOUT_MS = 600_000;
const MAX_DELAY_MS = 60_000;

export interface WorkerSchemaReady {
  timeoutMs: number;
  delayMs: number;
  maxDelayMs: number;
}

export function readWorkerSchemaReady(env: NodeJS.ProcessEnv = process.env): WorkerSchemaReady {
  return {
    timeoutMs:
      parseBounded(env.WORKER_SCHEMA_READY_TIMEOUT_MS, 0, MAX_TIMEOUT_MS) ??
      DEFAULT_SCHEMA_READY_TIMEOUT_MS,
    delayMs:
      parseBounded(env.WORKER_SCHEMA_READY_DELAY_MS, 50, MAX_DELAY_MS) ??
      DEFAULT_SCHEMA_READY_DELAY_MS,
    maxDelayMs:
      parseBounded(env.WORKER_SCHEMA_READY_MAX_DELAY_MS, 50, MAX_DELAY_MS) ??
      DEFAULT_SCHEMA_READY_MAX_DELAY_MS,
  };
}

function parseBounded(value: string | undefined, min: number, max: number): number | undefined {
  const trimmed = value?.trim() ?? "";
  if (!/^\d+$/.test(trimmed)) return undefined;
  const parsed = Number(trimmed);
  if (parsed < min || parsed > max) return undefined;
  return parsed;
}
