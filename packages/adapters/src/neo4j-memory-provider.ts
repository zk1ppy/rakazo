import { createHash } from "node:crypto";
import { isIP } from "node:net";
import type {
  AdapterContext,
  DurableMemoryScope,
  SemanticMemoryForgetRequest,
  SemanticMemoryProvider,
  SemanticMemoryPurgeHistoryRequest,
  SemanticMemoryRecallRequest,
  SemanticMemoryResponse,
  SemanticMemoryResult,
  SemanticMemorySaveRequest,
} from "@rakazo/adapter-kit";
import { isCloudMetadataHost, isLocalMcpHost } from "@rakazo/contracts";
import type {
  Neo4jMemoryConnection,
  Neo4jMemoryDriver,
  Neo4jMemoryDriverFactory,
} from "./neo4j-memory-client.js";
import {
  ensureNeo4jMemorySchema,
  escapeLuceneQuery,
  forgetNeo4jFact,
  openNeo4jMemoryDriver,
  purgeNeo4jChatSources,
  recallNeo4jFacts,
  saveNeo4jFact,
} from "./neo4j-memory-client.js";
import type { ResolveHostname } from "./network-address.js";
import { isCloudMetadataAddress, isPrivateAddress } from "./network-address.js";
import { isPrivateRemoteMcpHostname } from "./remote-mcp.js";
import {
  classifySerenityEndpointTrust,
  serenityEndpointRequiresDeploymentOwner,
} from "./serenity-client.js";
import { MemoryProviderDeploymentOwnerRequiredError } from "./serenity-memory-provider.js";

export const NEO4J_PROVIDER_ID = "neo4j";

const MAX_FACT_CHARS = 20_000;
const MAX_RECALL_LIMIT = 50;
const SHARED_NS = "shared";

export interface Neo4jConnectionDeps {
  resolveHostname?: ResolveHostname;
  openDriver?: Neo4jMemoryDriverFactory;
}

export interface ParsedNeo4jUri {
  uri: string;
  hostname: string;
  port: string;
  scheme: string;
  encrypted: boolean;
}

const NEO4J_SCHEMES = new Set(["neo4j", "neo4j+s", "neo4j+ssc", "bolt", "bolt+s", "bolt+ssc"]);

function hasControlCharacter(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 31 || code === 127) return true;
  }
  return false;
}

function requiredValue(values: Record<string, string>, key: string): string {
  const value = values[key]?.trim();
  if (!value) throw new Error(`${key} is required`);
  return value;
}

export function parseNeo4jUri(value: string): ParsedNeo4jUri {
  const trimmed = value.trim();
  if (!trimmed) throw new Error("uri is required");
  if (trimmed.length > 2048) throw new Error("uri is too long");
  let url: URL;
  try {
    url = new URL(trimmed);
  } catch {
    throw new Error("uri must be a neo4j or bolt URL");
  }
  const scheme = url.protocol.slice(0, -1).toLowerCase();
  if (!NEO4J_SCHEMES.has(scheme)) {
    throw new Error("uri scheme must be neo4j, neo4j+s, neo4j+ssc, bolt, bolt+s, or bolt+ssc");
  }
  if (url.username || url.password) {
    throw new Error("uri must not contain credentials; put the password in the credential");
  }
  if (url.search || url.hash) throw new Error("uri must not contain a query or fragment");
  if (url.pathname && url.pathname !== "/") throw new Error("uri must not contain a path");
  const hostname = url.hostname
    .replace(/^\[|\]$/g, "")
    .replace(/\.$/, "")
    .toLowerCase();
  if (!hostname) throw new Error("uri host is required");
  const port = url.port || "7687";
  if (!/^\d+$/.test(port) || Number(port) === 0 || Number(port) > 65535) {
    throw new Error("uri port is invalid");
  }
  const encrypted = scheme.includes("+s");
  const host = hostname.includes(":") ? `[${hostname}]` : hostname;
  return { uri: `${scheme}://${host}:${port}`, hostname, port, scheme, encrypted };
}

function trustProbeUrl(hostname: string): string {
  const host = hostname.includes(":") ? `[${hostname}]` : hostname;
  return `https://${host}/mcp`;
}

function isMetadataHost(hostname: string): boolean {
  if (isCloudMetadataHost(hostname)) return true;
  return isIP(hostname) !== 0 && isCloudMetadataAddress(hostname);
}

/** Docker/Kubernetes short names, loopback, and private LAN hosts. */
export function isClusterLocalNeo4jHost(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/\.$/, "");
  if (isLocalMcpHost(host) || isPrivateRemoteMcpHostname(host)) return true;
  return isIP(host) === 0 && !host.includes(".");
}

export function neo4jEndpointRequiresDeploymentOwner(uri: string): boolean {
  const parsed = parseNeo4jUri(uri);
  if (isMetadataHost(parsed.hostname) || isClusterLocalNeo4jHost(parsed.hostname)) return true;
  return serenityEndpointRequiresDeploymentOwner(trustProbeUrl(parsed.hostname));
}

export function neo4jRequiresDeploymentOwner(settings: Record<string, string>): boolean {
  if (settings.endpointTrust === "private") return true;
  const uri = settings.uri?.trim();
  if (!uri) return false;
  try {
    return neo4jEndpointRequiresDeploymentOwner(uri);
  } catch {
    return false;
  }
}

async function classifyNeo4jUri(
  uri: string,
  resolveHostname?: ResolveHostname,
): Promise<{ parsed: ParsedNeo4jUri; endpointTrust: "private" | "public" }> {
  const parsed = parseNeo4jUri(uri);
  if (isMetadataHost(parsed.hostname)) throw new Error("Neo4j URI targets a blocked address.");
  let endpointTrust: "private" | "public";
  if (isClusterLocalNeo4jHost(parsed.hostname)) endpointTrust = "private";
  else if (isIP(parsed.hostname) !== 0) {
    endpointTrust = isPrivateAddress(parsed.hostname) ? "private" : "public";
  } else {
    endpointTrust = await classifySerenityEndpointTrust(
      trustProbeUrl(parsed.hostname),
      resolveHostname,
    );
  }
  if (endpointTrust === "public" && !parsed.encrypted) {
    throw new Error(
      "Neo4j cleartext URIs are limited to loopback and private hosts; use neo4j+s:// or bolt+s:// for public hosts.",
    );
  }
  return { parsed, endpointTrust };
}

function parseDatabase(value: string | undefined): string {
  const database = value?.trim() || "neo4j";
  if (!/^[A-Za-z][A-Za-z0-9.-]{0,62}$/.test(database)) {
    throw new Error(
      "database must start with a letter and contain only letters, numbers, dots, or dashes",
    );
  }
  return database;
}

function parseUsername(value: string | undefined): string {
  const username = requiredValue({ username: value ?? "" }, "username");
  if (username.length > 256 || hasControlCharacter(username)) {
    throw new Error("username contains invalid characters");
  }
  return username;
}

function parsePassword(value: string | undefined): string {
  const password = value ?? "";
  if (!password) throw new Error("password is required");
  if (password.length > 1024) throw new Error("password is too long");
  return password;
}

export async function classifyNeo4jConnectionSettings(
  settings: Record<string, string>,
  deps?: Neo4jConnectionDeps,
): Promise<Record<string, string>> {
  const { parsed, endpointTrust } = await classifyNeo4jUri(
    requiredValue(settings, "uri"),
    deps?.resolveHostname,
  );
  const classified: Record<string, string> = {
    uri: parsed.uri,
    database: parseDatabase(settings.database),
    username: parseUsername(settings.username),
  };
  if (endpointTrust === "private") classified.endpointTrust = "private";
  return classified;
}

export async function prepareNeo4jConnection(
  settings: Record<string, string>,
  credentials: Record<string, string>,
  deps?: Neo4jConnectionDeps,
  options?: { allowPrivateEndpoint?: boolean },
): Promise<{ settings: Record<string, string>; credentials: Record<string, string> }> {
  const classified = await classifyNeo4jConnectionSettings(settings, deps);
  if (classified.endpointTrust === "private" && options?.allowPrivateEndpoint === false) {
    throw new MemoryProviderDeploymentOwnerRequiredError();
  }
  const password = parsePassword(credentials.password);
  const connection: Neo4jMemoryConnection = {
    uri: classified.uri ?? "",
    database: classified.database ?? "neo4j",
    username: classified.username ?? "",
    password,
    endpointTrust: classified.endpointTrust === "private" ? "private" : "public",
  };
  const open = deps?.openDriver ?? openNeo4jMemoryDriver;
  const driver = await open(connection);
  try {
    await driver.verifyConnectivity({ database: connection.database });
    await ensureNeo4jMemorySchema(driver, connection.database);
  } finally {
    await driver.close();
  }
  return { settings: classified, credentials: { password } };
}

export function createNeo4jProvider(
  settings: Record<string, string>,
  credentials: Record<string, string>,
): SemanticMemoryProvider {
  const parsed = parseNeo4jUri(requiredValue(settings, "uri"));
  if (isMetadataHost(parsed.hostname)) throw new Error("Neo4j URI targets a blocked address.");
  const endpointTrust =
    settings.endpointTrust === "private" || neo4jEndpointRequiresDeploymentOwner(parsed.uri)
      ? "private"
      : "public";
  if (endpointTrust === "public" && !parsed.encrypted) {
    throw new Error(
      "Neo4j cleartext URIs are limited to loopback and private hosts; use neo4j+s:// or bolt+s:// for public hosts.",
    );
  }
  return new Neo4jMemoryProvider({
    uri: parsed.uri,
    database: parseDatabase(settings.database),
    username: parseUsername(settings.username),
    password: parsePassword(credentials.password),
    endpointTrust,
  });
}

function isolatedNs(botId: string): string {
  return `isolated:${botId}`;
}

function historyNs(botId: string): string {
  return `history:${botId}`;
}

function saveNamespace(scope: DurableMemoryScope, botId: string, kind: "memory" | "chat"): string {
  if (kind === "chat") return historyNs(botId);
  return scope === "shared" ? SHARED_NS : isolatedNs(botId);
}

function recallNamespaces(
  scope: DurableMemoryScope,
  botId: string,
  historyGeneration: number | undefined,
): { namespaces: string[]; historyNs: string | null; historyGeneration: number | null } {
  const namespaces = scope === "shared" ? [SHARED_NS, isolatedNs(botId)] : [isolatedNs(botId)];
  if (historyGeneration === undefined) {
    return { namespaces, historyNs: null, historyGeneration: null };
  }
  const history = historyNs(botId);
  return {
    namespaces: [...namespaces, history],
    historyNs: history,
    historyGeneration,
  };
}

function visibleNamespaces(botId: string | undefined): string[] | null {
  if (!botId) return null;
  return [SHARED_NS, isolatedNs(botId), historyNs(botId)];
}

function digest(parts: string[]): string {
  return createHash("sha256").update(parts.join("\n")).digest("hex").slice(0, 32);
}

function safeMemoryError(error: unknown): string {
  const message = error instanceof Error ? error.message : "Neo4j memory request failed";
  return message.replace(/password[=:][^\s,)]+/gi, "password=(redacted)").slice(0, 500);
}

// One pool per connection. Resolving memory for each run would otherwise open a driver and never close it.
const driverPools = new Map<string, Promise<Neo4jMemoryDriver>>();
const schemaReady = new WeakMap<Neo4jMemoryDriver, Promise<void>>();

function poolKey(connection: Neo4jMemoryConnection): string {
  return createHash("sha256")
    .update(
      [
        connection.uri,
        connection.database,
        connection.username,
        connection.endpointTrust,
        connection.password,
      ].join("\n"),
    )
    .digest("hex");
}

function sharedDriver(connection: Neo4jMemoryConnection): Promise<Neo4jMemoryDriver> {
  const key = poolKey(connection);
  const existing = driverPools.get(key);
  if (existing) return existing;
  const created = Promise.resolve(openNeo4jMemoryDriver(connection)).catch((error: unknown) => {
    driverPools.delete(key);
    throw error;
  });
  driverPools.set(key, created);
  return created;
}

export class Neo4jMemoryProvider implements SemanticMemoryProvider {
  private driverTask?: Promise<Neo4jMemoryDriver>;

  constructor(
    private readonly connection: Neo4jMemoryConnection,
    private readonly openDriver: Neo4jMemoryDriverFactory = openNeo4jMemoryDriver,
  ) {}

  describe() {
    return {
      id: NEO4J_PROVIDER_ID,
      contractVersion: "1",
      adapterVersion: "0.1.0",
      capabilities: {
        recall: true,
        save: true,
        purgeHistory: true,
        sharedScope: true,
      } as const,
    };
  }

  async recall(
    request: SemanticMemoryRecallRequest,
    context: AdapterContext,
  ): Promise<SemanticMemoryResponse<SemanticMemoryResult[]>> {
    try {
      const q = escapeQuery(request.query);
      if (!q) return { ok: true, value: [] };
      const scope = recallNamespaces(request.scope, request.botId, request.historyGeneration);
      const limit = clampLimit(request.limit);
      if (limit === 0) return { ok: true, value: [] };
      const hits = await recallNeo4jFacts(
        await this.ready(context.signal),
        this.connection.database,
        {
          q,
          spaceId: context.spaceId,
          namespaces: scope.namespaces,
          historyNs: scope.historyNs,
          historyGeneration: scope.historyGeneration,
          limit,
        },
        context.signal,
      );
      return {
        ok: true,
        value: hits.slice(0, limit).map((hit) => ({
          id: hit.id,
          memory: hit.memory,
          score: hit.score,
          provenance: hit.provenance,
          entity: hit.entity,
          ...(hit.updatedAt ? { updatedAt: hit.updatedAt } : {}),
        })),
      };
    } catch (error) {
      return { ok: false, error: safeMemoryError(error) };
    }
  }

  async save(
    request: SemanticMemorySaveRequest,
    context: AdapterContext,
  ): Promise<SemanticMemoryResponse> {
    try {
      const text = request.content.trim();
      if (!text) return { ok: false, error: "content is required" };
      if (text.length > MAX_FACT_CHARS) return { ok: false, error: "content is too long" };
      const kind = request.source.kind === "history" ? "chat" : "memory";
      const generation = request.source.kind === "history" ? request.source.generation : null;
      const ns = saveNamespace(request.scope, request.botId, kind);
      const contentHash = digest([text]);
      const provenance = [
        `rakazo space:${context.spaceId} bot:${request.botId}`,
        kind === "chat" && generation != null ? `history:${generation}` : "",
      ]
        .filter(Boolean)
        .join(" ");
      const uri =
        kind === "chat"
          ? `rakazo://chat/${context.spaceId}/${request.botId}/${generation}`
          : `rakazo://memory/${context.spaceId}/${ns}/${contentHash}`;
      const runScope = context.runId ?? context.operationId;
      const saved = await saveNeo4jFact(
        await this.ready(context.signal),
        this.connection.database,
        {
          factId: digest([
            "fact",
            context.spaceId,
            ns,
            runScope,
            kind,
            generation == null ? "" : String(generation),
            text,
          ]),
          sourceId: digest(["source", context.spaceId, kind, uri]),
          spaceId: context.spaceId,
          ns,
          botId: request.botId,
          text,
          kind,
          uri,
          title: kind === "chat" ? `Chat ${generation}` : "Memory",
          contentHash,
          provenance,
          confidence: kind === "chat" ? 0.8 : 1,
          generation,
          now: new Date().toISOString(),
        },
        context.signal,
      );
      return saved.id
        ? { ok: true, value: undefined }
        : { ok: false, error: "Neo4j did not store the fact." };
    } catch (error) {
      return { ok: false, error: safeMemoryError(error) };
    }
  }

  async purgeHistory(
    request: SemanticMemoryPurgeHistoryRequest,
    context: AdapterContext,
  ): Promise<SemanticMemoryResponse> {
    try {
      const generations = [
        ...new Set(request.generations.filter((generation) => Number.isInteger(generation))),
      ];
      if (generations.length === 0) return { ok: true, value: undefined };
      await purgeNeo4jChatSources(
        await this.ready(context.signal),
        this.connection.database,
        { spaceId: context.spaceId, botId: request.botId, generations },
        context.signal,
      );
      return { ok: true, value: undefined };
    } catch (error) {
      return { ok: false, error: safeMemoryError(error) };
    }
  }

  async forget(
    request: SemanticMemoryForgetRequest,
    context: AdapterContext,
  ): Promise<SemanticMemoryResponse<{ id: string; expired: boolean; reason: string | null }>> {
    try {
      const id = request.id.trim();
      if (!id) return { ok: false, error: "id is required" };
      const reason = request.reason?.trim() || null;
      const forgotten = await forgetNeo4jFact(
        await this.ready(context.signal),
        this.connection.database,
        {
          id,
          spaceId: context.spaceId,
          namespaces: visibleNamespaces(context.botId),
          now: new Date().toISOString(),
          reason,
        },
        context.signal,
      );
      return {
        ok: true,
        value: forgotten ?? { id, expired: false, reason: null },
      };
    } catch (error) {
      return { ok: false, error: safeMemoryError(error) };
    }
  }

  private async ready(signal?: AbortSignal) {
    const driver = await this.driver();
    await this.schema(driver, signal);
    return driver;
  }

  private driver() {
    if (this.openDriver === openNeo4jMemoryDriver) return sharedDriver(this.connection);
    this.driverTask ??= Promise.resolve(this.openDriver(this.connection)).catch(
      (error: unknown) => {
        this.driverTask = undefined;
        throw error;
      },
    );
    return this.driverTask;
  }

  private schema(driver: Neo4jMemoryDriver, signal?: AbortSignal) {
    const existing = schemaReady.get(driver);
    if (existing) return existing;
    const ready = ensureNeo4jMemorySchema(driver, this.connection.database, signal).catch(
      (error: unknown) => {
        schemaReady.delete(driver);
        throw error;
      },
    );
    schemaReady.set(driver, ready);
    return ready;
  }
}

function escapeQuery(value: string): string {
  return escapeLuceneQuery(value.trim().slice(0, 500)).trim();
}

function clampLimit(limit: number): number {
  if (!Number.isFinite(limit) || limit <= 0) return 0;
  return Math.min(Math.floor(limit), MAX_RECALL_LIMIT);
}
