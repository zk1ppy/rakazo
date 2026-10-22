import { createHash } from "node:crypto";
import { isIP } from "node:net";
import type {
  AdapterContext,
  DurableMemoryScope,
  JobPublisher,
  SemanticMemoryForgetRequest,
  SemanticMemoryProvider,
  SemanticMemoryPurgeHistoryRequest,
  SemanticMemoryRecallRequest,
  SemanticMemoryResponse,
  SemanticMemoryResult,
  SemanticMemorySaveRequest,
} from "@rakazo/adapter-kit";
import { memoryExtractJob } from "@rakazo/adapter-kit";
import { isCloudMetadataHost, isLocalMcpHost } from "@rakazo/contracts";
import { getLogger } from "@rakazo/logging";
import type { ConsolidateModelSettings, ConsolidationPage } from "./memory-consolidate.js";
import {
  assertConsolidateMaxOutputTokens,
  assertConsolidateThinkingLevel,
  assertConsolidateTimeoutMs,
  readConsolidateModelSettings,
} from "./memory-consolidate.js";
import { neo4jDreamEnabled } from "./memory-dream.js";
import type {
  ExtractedEntityWrite,
  ExtractedRelationWrite,
  Neo4jExtractionModel,
} from "./memory-extract.js";
import {
  assertNeo4jMaxChunkChars,
  isTrivialMemoryText,
  neo4jExtractionEnabled,
  neo4jMaxChunkChars,
  planExtractionChunks,
  readNeo4jExtractionModel,
  readOwnerAliases,
} from "./memory-extract.js";
import type {
  Neo4jConsolidationEntity,
  Neo4jDuplicateMerge,
  Neo4jMemoryConnection,
  Neo4jMemoryDriver,
  Neo4jMemoryDriverFactory,
} from "./neo4j-memory-client.js";
import {
  collapseNeo4jParallelRelations,
  countExtractedNeo4jFacts,
  ensureNeo4jMemorySchema,
  escapeLuceneQuery,
  findNeo4jPeopleByNorms,
  forgetNeo4jFact,
  linkNeo4jEntities,
  linkNeo4jFactEntity,
  listNeo4jConsolidationEntities,
  listUnextractedNeo4jFacts,
  markNeo4jFactExtracted,
  mergeExtractedNeo4jEntity,
  mergeNeo4jDuplicateEntity,
  openNeo4jMemoryDriver,
  purgeNeo4jChatSources,
  readNeo4jFactLearnedAt,
  readNeo4jFactText,
  realignNeo4jFactTimes,
  recallNeo4jFacts,
  renameNeo4jEntity,
  saveNeo4jFact,
  stampNeo4jExtractedFacts,
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
  if (settings.extract?.trim()) {
    classified.extract = neo4jExtractionEnabled(settings) ? "true" : "false";
  }
  if (settings.dream?.trim()) {
    classified.dream = neo4jDreamEnabled(settings) ? "true" : "false";
  }
  const maxChunkChars = assertNeo4jMaxChunkChars(settings.maxChunkChars);
  if (maxChunkChars) classified.maxChunkChars = maxChunkChars;
  const extractionModel = readNeo4jExtractionModel(settings);
  if (extractionModel) {
    classified.extractModelProvider = extractionModel.provider;
    classified.extractModelId = extractionModel.id;
  }
  const consolidateThinkingLevel = assertConsolidateThinkingLevel(
    settings.consolidateThinkingLevel,
  );
  if (consolidateThinkingLevel) classified.consolidateThinkingLevel = consolidateThinkingLevel;
  const consolidateMaxOutputTokens = assertConsolidateMaxOutputTokens(
    settings.consolidateMaxOutputTokens,
  );
  if (consolidateMaxOutputTokens)
    classified.consolidateMaxOutputTokens = consolidateMaxOutputTokens;
  const consolidateTimeoutMs = assertConsolidateTimeoutMs(settings.consolidateTimeoutMs);
  if (consolidateTimeoutMs) classified.consolidateTimeoutMs = consolidateTimeoutMs;
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
  deps?: { jobs?: JobPublisher },
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
  return new Neo4jMemoryProvider(
    {
      uri: parsed.uri,
      database: parseDatabase(settings.database),
      username: parseUsername(settings.username),
      password: parsePassword(credentials.password),
      endpointTrust,
    },
    openNeo4jMemoryDriver,
    {
      jobs: deps?.jobs,
      extract: neo4jExtractionEnabled(settings),
      dream: neo4jDreamEnabled(settings),
      maxChunkChars: neo4jMaxChunkChars(settings),
      extractionModel: readNeo4jExtractionModel(settings),
      consolidateModelSettings: readConsolidateModelSettings(settings),
      ownerAliases: readOwnerAliases(settings),
    },
  );
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

// One driver per uri, username, and password. A new password or username for that
// uri, or the same username and password on a new uri, closes the driver it replaces.
interface PooledDriver {
  uri: string;
  username: string;
  passwordHash: string;
  endpointTrust: Neo4jMemoryConnection["endpointTrust"];
  driver: Promise<Neo4jMemoryDriver>;
}

const driverPools = new Map<string, PooledDriver>();
const schemaReady = new WeakMap<Neo4jMemoryDriver, Map<string, Promise<void>>>();

function poolKey(connection: Neo4jMemoryConnection): string {
  return createHash("sha256")
    .update([connection.uri, connection.username, connection.password].join("\n"))
    .digest("hex");
}

function hashPassword(password: string): string {
  return createHash("sha256").update(password).digest("hex");
}

function replacesCachedDriver(entry: PooledDriver, connection: Neo4jMemoryConnection): boolean {
  const sameUri = entry.uri === connection.uri;
  const sameUser = entry.username === connection.username;
  const samePassword = entry.passwordHash === hashPassword(connection.password);
  if (sameUri && (!sameUser || !samePassword)) return true;
  if (!sameUri && sameUser && samePassword) return true;
  return sameUri && sameUser && samePassword && entry.endpointTrust !== connection.endpointTrust;
}

export async function closeNeo4jMemoryDrivers(): Promise<void> {
  const entries = [...driverPools.values()];
  driverPools.clear();
  await Promise.all(entries.map((entry) => closePooledDriver(entry)));
}

function closePooledDriver(entry: PooledDriver): Promise<void> {
  return entry.driver
    .then((driver) => driver.close())
    .then(
      () => undefined,
      () => undefined,
    );
}

/** Reuse the driver for this uri, username, and password, closing the one a change replaces. */
export function acquireNeo4jMemoryDriver(
  connection: Neo4jMemoryConnection,
  open: Neo4jMemoryDriverFactory = openNeo4jMemoryDriver,
): Promise<Neo4jMemoryDriver> {
  const key = poolKey(connection);
  const existing = driverPools.get(key);
  if (existing && existing.endpointTrust === connection.endpointTrust) return existing.driver;

  const driver = (async () => {
    const closing: Promise<void>[] = [];
    if (existing) closing.push(closePooledDriver(existing));
    for (const [oldKey, entry] of driverPools) {
      if (oldKey === key || !replacesCachedDriver(entry, connection)) continue;
      driverPools.delete(oldKey);
      closing.push(closePooledDriver(entry));
    }
    await Promise.all(closing);
    return open(connection);
  })().catch((error: unknown) => {
    const current = driverPools.get(key);
    if (current?.driver === driver) driverPools.delete(key);
    throw error;
  });
  driverPools.set(key, {
    uri: connection.uri,
    username: connection.username,
    passwordHash: hashPassword(connection.password),
    endpointTrust: connection.endpointTrust,
    driver,
  });
  return driver;
}

export interface Neo4jProviderOptions {
  jobs?: JobPublisher;
  extract?: boolean;
  /** Dream mode. Absent stays off, unlike extraction. */
  dream?: boolean;
  maxChunkChars?: number;
  extractionModel?: Neo4jExtractionModel | null;
  consolidateModelSettings?: ConsolidateModelSettings;
  ownerAliases?: readonly string[];
}

export class Neo4jMemoryProvider implements SemanticMemoryProvider {
  private driverTask?: Promise<Neo4jMemoryDriver>;
  private readonly jobs?: JobPublisher;
  private readonly extractEnabled: boolean;
  private readonly dreamMode: boolean;
  private readonly maxChunkChars: number;
  private readonly pinnedModel: Neo4jExtractionModel | null;
  private readonly consolidateSettings: ConsolidateModelSettings;
  private readonly ownerAliasList: readonly string[];

  constructor(
    private readonly connection: Neo4jMemoryConnection,
    private readonly openDriver: Neo4jMemoryDriverFactory = openNeo4jMemoryDriver,
    options?: Neo4jProviderOptions,
  ) {
    this.jobs = options?.jobs;
    this.extractEnabled = options?.extract ?? true;
    this.dreamMode = options?.dream ?? false;
    this.maxChunkChars = options?.maxChunkChars ?? neo4jMaxChunkChars({});
    this.pinnedModel = options?.extractionModel ?? null;
    this.consolidateSettings =
      options?.consolidateModelSettings ?? readConsolidateModelSettings({});
    this.ownerAliasList = options?.ownerAliases ?? readOwnerAliases({});
  }

  extractionEnabled(): boolean {
    return this.extractEnabled;
  }

  dreamEnabled(): boolean {
    return this.dreamMode;
  }

  extractionModel(): Neo4jExtractionModel | null {
    return this.pinnedModel;
  }

  consolidateModelSettings(): ConsolidateModelSettings {
    return this.consolidateSettings;
  }

  ownerAliases(): readonly string[] {
    return this.ownerAliasList;
  }

  extractionChunkChars(): number {
    return this.maxChunkChars;
  }

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
      const factId = digest([
        "fact",
        context.spaceId,
        ns,
        runScope,
        kind,
        generation == null ? "" : String(generation),
        text,
      ]);
      const saved = await saveNeo4jFact(
        await this.ready(context.signal),
        this.connection.database,
        {
          factId,
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
      if (!saved.id) return { ok: false, error: "Neo4j did not store the fact." };
      await this.enqueueExtraction({
        spaceId: context.spaceId,
        userId: context.userId,
        botId: request.botId,
        factId,
        ns,
        text,
      });
      return { ok: true, value: undefined };
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

  async realignExtractionTimes(
    spaceId: string,
    signal?: AbortSignal,
  ): Promise<{ relations: number; entities: number }> {
    return realignNeo4jFactTimes(
      await this.ready(signal),
      this.connection.database,
      { spaceId },
      signal,
    );
  }

  async listUnextractedFacts(
    spaceId: string,
    input: { limit: number; skip: number; includeExtracted?: boolean },
    signal?: AbortSignal,
  ) {
    return listUnextractedNeo4jFacts(
      await this.ready(signal),
      this.connection.database,
      {
        spaceId,
        limit: input.limit,
        skip: input.skip,
        includeExtracted: input.includeExtracted === true,
      },
      signal,
    );
  }

  async countExtractedFacts(spaceId: string, signal?: AbortSignal): Promise<number> {
    return countExtractedNeo4jFacts(
      await this.ready(signal),
      this.connection.database,
      { spaceId },
      signal,
    );
  }

  /** Copy a stored time onto extractedAt for facts that already have :ABOUT. */
  async stampExtractedFacts(spaceId: string, signal?: AbortSignal): Promise<number> {
    return stampNeo4jExtractedFacts(
      await this.ready(signal),
      this.connection.database,
      { spaceId, extractedAt: new Date().toISOString() },
      signal,
    );
  }

  async markExtracted(
    input: { factId: string; spaceId: string; ns: string; extractedAt: string },
    signal?: AbortSignal,
  ): Promise<void> {
    await markNeo4jFactExtracted(await this.ready(signal), this.connection.database, input, signal);
  }

  async listConsolidationPage(
    input: {
      spaceId: string;
      ns?: string;
      limit: number;
      afterId?: string;
      carryIds?: string[];
    },
    signal?: AbortSignal,
  ): Promise<ConsolidationPage> {
    const driver = await this.ready(signal);
    const database = this.connection.database;
    const page = await listNeo4jConsolidationEntities(
      driver,
      database,
      {
        spaceId: input.spaceId,
        ...(input.ns ? { ns: input.ns } : {}),
        limit: input.limit,
        ...(input.afterId ? { afterId: input.afterId } : {}),
      },
      signal,
    );
    const seen = new Set(page.map((entity) => entity.id));
    const missing = (input.carryIds ?? []).filter((id) => !seen.has(id));
    const carry = missing.length
      ? await listNeo4jConsolidationEntities(
          driver,
          database,
          {
            spaceId: input.spaceId,
            ...(input.ns ? { ns: input.ns } : {}),
            ids: missing,
            limit: missing.length,
          },
          signal,
        )
      : [];
    const entities: Neo4jConsolidationEntity[] = [];
    const ids = new Set<string>();
    for (const entity of [...page, ...carry]) {
      if (ids.has(entity.id)) continue;
      ids.add(entity.id);
      entities.push(entity);
    }
    return {
      entities,
      pageIds: page.map((entity) => entity.id),
      pageFull: page.length >= input.limit,
    };
  }

  async mergeDuplicateEntity(input: Neo4jDuplicateMerge, signal?: AbortSignal) {
    return mergeNeo4jDuplicateEntity(
      await this.ready(signal),
      this.connection.database,
      input,
      signal,
    );
  }

  async renameEntity(
    input: Parameters<typeof renameNeo4jEntity>[2],
    signal?: AbortSignal,
  ): Promise<boolean> {
    return renameNeo4jEntity(await this.ready(signal), this.connection.database, input, signal);
  }

  async findPeopleByNorms(
    input: { spaceId: string; ns?: string; norms: string[] },
    signal?: AbortSignal,
  ) {
    return findNeo4jPeopleByNorms(
      await this.ready(signal),
      this.connection.database,
      input,
      signal,
    );
  }

  async collapseParallelRelations(
    input: {
      spaceId: string;
      ids: string[];
      ns?: string;
      scope?: "page" | "space";
      dryRun?: boolean;
    },
    signal?: AbortSignal,
  ) {
    return collapseNeo4jParallelRelations(
      await this.ready(signal),
      this.connection.database,
      input,
      signal,
    );
  }

  async readFactText(
    input: { factId: string; spaceId: string; ns: string },
    signal?: AbortSignal,
  ): Promise<string | null> {
    return readNeo4jFactText(await this.ready(signal), this.connection.database, input, signal);
  }

  async applyExtraction(
    input: {
      factId: string;
      spaceId: string;
      ns: string;
      entities: ExtractedEntityWrite[];
      relations: ExtractedRelationWrite[];
      now: string;
    },
    signal?: AbortSignal,
  ): Promise<void> {
    const driver = await this.ready(signal);
    const database = this.connection.database;
    const learnedAt =
      (await readNeo4jFactLearnedAt(
        driver,
        database,
        { factId: input.factId, spaceId: input.spaceId, ns: input.ns },
        signal,
      )) ?? input.now;
    const ids = new Map<string, string>();
    for (const entity of input.entities) {
      const id = await mergeExtractedNeo4jEntity(
        driver,
        database,
        {
          id: entity.id,
          type: entity.type,
          spaceId: input.spaceId,
          ns: input.ns,
          name: entity.name,
          normName: entity.normName,
          aliases: entity.aliases,
          aliasNorms: entity.aliasNorms,
          summary: entity.summary,
          now: input.now,
          learnedAt,
        },
        signal,
      );
      ids.set(entity.normName, id);
      for (const alias of entity.aliasNorms) ids.set(alias, id);
      await linkNeo4jFactEntity(
        driver,
        database,
        { factId: input.factId, entityId: id, spaceId: input.spaceId },
        signal,
      );
    }
    for (const relation of input.relations) {
      const fromId = ids.get(relation.fromNorm);
      const toId = ids.get(relation.toNorm);
      if (!fromId || !toId || fromId === toId) continue;
      await linkNeo4jEntities(
        driver,
        database,
        {
          fromId,
          toId,
          spaceId: input.spaceId,
          ns: input.ns,
          type: relation.type,
          factId: input.factId,
          now: input.now,
          time: relation.time,
        },
        signal,
      );
    }
  }

  private async enqueueExtraction(input: {
    spaceId: string;
    userId: string;
    botId: string;
    factId: string;
    ns: string;
    text: string;
  }): Promise<void> {
    if (!this.jobs) {
      getLogger().info("memory.extract enqueue skipped: no job publisher");
      return;
    }
    if (!this.extractEnabled) {
      getLogger().info("memory.extract enqueue skipped: extraction is off");
      return;
    }
    if (isTrivialMemoryText(input.text)) {
      getLogger().info("memory.extract enqueue skipped: trivial text");
      return;
    }
    const planned = planExtractionChunks(input.text, this.maxChunkChars);
    if (planned.chunks.length === 0) {
      getLogger().info("memory.extract enqueue skipped: no chunks");
      return;
    }
    try {
      await this.jobs.enqueue(
        memoryExtractJob({
          spaceId: input.spaceId,
          userId: input.userId,
          botId: input.botId,
          factId: input.factId,
          ns: input.ns,
          maxChunkChars: planned.maxChunkChars,
          chunkIndex: 0,
          attempt: 0,
          scheduleGeneration: 0,
        }),
      );
    } catch (error) {
      getLogger().error("memory.extract enqueue failed", error);
    }
  }

  private async ready(signal?: AbortSignal) {
    const driver = await this.driver();
    await this.schema(driver, signal);
    return driver;
  }

  private driver() {
    if (this.openDriver === openNeo4jMemoryDriver) return acquireNeo4jMemoryDriver(this.connection);
    this.driverTask ??= Promise.resolve(this.openDriver(this.connection)).catch(
      (error: unknown) => {
        this.driverTask = undefined;
        throw error;
      },
    );
    return this.driverTask;
  }

  private schema(driver: Neo4jMemoryDriver, signal?: AbortSignal) {
    const databases = schemaReady.get(driver) ?? new Map<string, Promise<void>>();
    schemaReady.set(driver, databases);
    const database = this.connection.database;
    const existing = databases.get(database);
    if (existing) return existing;
    const ready = ensureNeo4jMemorySchema(driver, database, signal).catch((error: unknown) => {
      if (databases.get(database) === ready) databases.delete(database);
      throw error;
    });
    databases.set(database, ready);
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
