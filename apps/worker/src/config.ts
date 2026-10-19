import { readdirSync, readFileSync } from "node:fs";

/**
 * Keys the worker needs to open Postgres and the secret store that holds Neo4j
 * credentials. A kubectl exec does not inherit values that exist only on the
 * already-running worker, so a blank caller env can take them from that process.
 */
const CONFIG_KEYS = [
  "DATABASE_URL",
  "REALTIME_DATABASE_URL",
  "POSTGRES_USER",
  "POSTGRES_PASSWORD",
  "POSTGRES_DB",
  "POSTGRES_HOST",
  "POSTGRES_PORT",
  "PGHOST",
  "PGPORT",
  "PGUSER",
  "PGPASSWORD",
  "PGDATABASE",
  "ENCRYPTION_KEY",
  "SECRET_STORE",
  "INFISICAL_URL",
  "INFISICAL_ALLOW_INSECURE_HTTP",
  "INFISICAL_CLIENT_ID",
  "INFISICAL_CLIENT_SECRET",
  "INFISICAL_PROJECT_ID",
  "INFISICAL_ENVIRONMENT",
  "INFISICAL_FOLDER",
  "DB_POOL_MAX",
  "NODE_ENV",
  "RAKAZO_ALLOW_DEV_SECRETS",
  "VITEST",
] as const;

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
  /** Caller env plus any file-backed or sibling-process values the worker uses. */
  env: NodeJS.ProcessEnv;
}

export interface WorkerConfigDeps {
  readFile?: (path: string) => string;
  processEnvirons?: () => readonly NodeJS.ProcessEnv[];
}

export function loadWorkerConfig(
  source: NodeJS.ProcessEnv = process.env,
  deps: WorkerConfigDeps = {},
): WorkerConfig {
  const readFile = deps.readFile ?? ((path: string) => readFileSync(path, "utf8"));
  const processEnvirons = deps.processEnvirons ?? containerProcessEnvirons;
  let env = materializeFiles({ ...source }, readFile);
  if (!databaseUrlFrom(env, readFile)) {
    env = materializeFiles(inheritConfig(env, processEnvirons()), readFile);
  }
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

function inheritConfig(
  env: NodeJS.ProcessEnv,
  environs: readonly NodeJS.ProcessEnv[],
): NodeJS.ProcessEnv {
  const next = { ...env };
  for (const extra of environs) {
    for (const key of CONFIG_KEYS) {
      if (!next[key]?.trim() && extra[key]?.trim()) next[key] = extra[key];
      const fileKey = `${key}_FILE`;
      if (!next[fileKey]?.trim() && extra[fileKey]?.trim()) next[fileKey] = extra[fileKey];
    }
  }
  return next;
}

function containerProcessEnvirons(): NodeJS.ProcessEnv[] {
  const environs: NodeJS.ProcessEnv[] = [];
  const seen = new Set<number>([process.pid]);
  const pids = [1];
  try {
    for (const name of readdirSync("/proc")) {
      if (!/^\d+$/.test(name)) continue;
      const pid = Number(name);
      if (!seen.has(pid)) pids.push(pid);
    }
  } catch {
    return environs;
  }
  for (const pid of pids) {
    if (seen.has(pid)) continue;
    seen.add(pid);
    const env = readProcEnviron(pid);
    if (env) environs.push(env);
  }
  return environs;
}

function readProcEnviron(pid: number): NodeJS.ProcessEnv | undefined {
  try {
    const raw = readFileSync(`/proc/${pid}/environ`);
    const env: NodeJS.ProcessEnv = {};
    for (const entry of raw.toString("utf8").split("\0")) {
      const split = entry.indexOf("=");
      if (split <= 0) continue;
      env[entry.slice(0, split)] = entry.slice(split + 1);
    }
    return env;
  } catch {
    return undefined;
  }
}
