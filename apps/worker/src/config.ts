import { readFileSync } from "node:fs";

/**
 * Database and secret-store settings the worker reads from its own environment.
 * The image does not bake these in. Compose sets DATABASE_URL from POSTGRES_*
 * and ENCRYPTION_KEY from the env file; a chart may pass the same names or a
 * *_FILE path. kubectl exec inherits that container env and does not need
 * another process's environment.
 */
const FILE_BACKED_KEYS = [
  "DATABASE_URL",
  "REALTIME_DATABASE_URL",
  "POSTGRES_PASSWORD",
  "PGPASSWORD",
  "ENCRYPTION_KEY",
  "INFISICAL_CLIENT_SECRET",
] as const;

const MISSING_DATABASE_URL =
  "Missing DATABASE_URL. The worker also reads DATABASE_URL_FILE, or POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB (PGHOST, PGUSER, PGPASSWORD, PGDATABASE).";

export interface WorkerConfig {
  databaseUrl: string;
  realtimeDatabaseUrl: string;
  /** Caller env with file-backed secrets copied onto the value keys. */
  env: NodeJS.ProcessEnv;
}

export interface WorkerConfigDeps {
  readFile?: (path: string) => string;
}

export function loadWorkerConfig(
  source: NodeJS.ProcessEnv = process.env,
  deps: WorkerConfigDeps = {},
): WorkerConfig {
  const readFile = deps.readFile ?? ((path: string) => readFileSync(path, "utf8"));
  const env = materializeFiles({ ...source }, readFile);
  const databaseUrl = databaseUrlFrom(env, readFile);
  if (!databaseUrl) throw new Error(MISSING_DATABASE_URL);
  return {
    databaseUrl,
    realtimeDatabaseUrl: readValue(env, "REALTIME_DATABASE_URL", readFile) ?? databaseUrl,
    env,
  };
}

function databaseUrlFrom(
  env: NodeJS.ProcessEnv,
  readFile: (path: string) => string,
): string | undefined {
  const direct = readValue(env, "DATABASE_URL", readFile);
  if (direct) return direct;
  const host = readValue(env, "POSTGRES_HOST", readFile) ?? readValue(env, "PGHOST", readFile);
  const user = readValue(env, "POSTGRES_USER", readFile) ?? readValue(env, "PGUSER", readFile);
  const password =
    readValue(env, "POSTGRES_PASSWORD", readFile) ?? readValue(env, "PGPASSWORD", readFile);
  const database =
    readValue(env, "POSTGRES_DB", readFile) ?? readValue(env, "PGDATABASE", readFile);
  if (!host || !user || !password || !database) return undefined;
  const port =
    readValue(env, "POSTGRES_PORT", readFile) ?? readValue(env, "PGPORT", readFile) ?? "5432";
  if (!/^\d+$/.test(port)) throw new Error("POSTGRES_PORT must be a whole number");
  const userName = encodeURIComponent(user);
  const secret = encodeURIComponent(password);
  const name = encodeURIComponent(database);
  return `postgres://${userName}:${secret}@${host}:${port}/${name}`;
}

function readValue(
  env: NodeJS.ProcessEnv,
  key: string,
  readFile: (path: string) => string,
): string | undefined {
  const direct = env[key]?.trim();
  if (direct) return direct;
  const filePath = env[`${key}_FILE`]?.trim();
  if (!filePath) return undefined;
  let text: string;
  try {
    text = readFile(filePath);
  } catch (error) {
    const message = error instanceof Error ? error.message : "unreadable";
    throw new Error(`${key}_FILE could not be read: ${message}`);
  }
  return text.trim() || undefined;
}

function materializeFiles(
  env: NodeJS.ProcessEnv,
  readFile: (path: string) => string,
): NodeJS.ProcessEnv {
  const next = { ...env };
  for (const key of FILE_BACKED_KEYS) {
    if (next[key]?.trim()) continue;
    const value = readValue(next, key, readFile);
    if (value) next[key] = value;
  }
  return next;
}
