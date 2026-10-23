import { createHash, randomBytes } from "node:crypto";
import type {
  AdapterContext,
  CloudAgentHandle,
  CloudAgentImage,
  CloudAgentLaunchRequest,
  CloudAgentProvider,
  CloudAgentReplyRequest,
  CloudAgentSnapshot,
  CloudAgentStatus,
} from "@rakazo/adapter-kit";
import { CloudAgentRequestRejected, CloudAgentRetryLater } from "@rakazo/adapter-kit";
import { z } from "zod";
import { readBodyCapped } from "./web-ssrf.js";

const DEFAULT_WORKSPACE_ROOT = "/workspace/repos";
const PROMPT_ADMIT_MS = 15_000;
const PROMPT_TIMEOUT_MS = 15 * 60_000;

const sessionSchema = z.object({
  id: z.string().min(1),
  title: z.string().optional(),
  outcome: z.string().optional(),
  time: z
    .object({
      idle: z.number().optional(),
      archived: z.number().optional(),
    })
    .partial()
    .optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

const messageSchema = z.object({
  id: z.string().optional(),
  type: z.string().optional(),
  text: z.string().optional(),
  finish: z.string().optional(),
  error: z.unknown().optional(),
  content: z.unknown().optional(),
  payload: z.unknown().optional(),
});

const vcsSchema = z.object({
  branch: z
    .object({
      current: z.string().optional(),
      default: z.string().optional(),
    })
    .partial()
    .optional(),
});

export interface OpenCodeCloudAgentOptions {
  serverUrl: string;
  publicUrl?: string;
  username?: string;
  password: string;
  workspaceRoot?: string;
  defaultModel?: string;
  fetch?: typeof fetch;
  timeoutMs?: number;
  /** How long launch/reply wait for prompt admission before detaching the HTTP call. */
  promptAdmitMs?: number;
  /** Ceiling for a detached prompt request. Independent of the caller's abort signal. */
  promptTimeoutMs?: number;
}

interface PreparedServer {
  apiBase: string;
  publicOrigin: string;
  username: string;
  password: string;
  authorization: string;
  workspaceRoot: string;
  defaultModel?: string;
}

/**
 * OpenCode v2 (`opencode serve`) sessions as cloud agents.
 * `POST /api/session/{id}/prompt` admits work and returns; older builds that
 * hold the connection until the turn ends are detached so launch/reply return.
 */
export class OpenCodeCloudAgentProvider implements CloudAgentProvider {
  private readonly server: PreparedServer;
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;
  private readonly promptAdmitMs: number;
  private readonly promptTimeoutMs: number;
  /** Prompt HTTP failures observed after launch/reply already returned. */
  private readonly promptFailures = new Map<string, true>();

  constructor(options: OpenCodeCloudAgentOptions) {
    this.server = prepareServer(options);
    this.fetchImpl = options.fetch ?? fetch;
    this.timeoutMs = options.timeoutMs ?? 30_000;
    this.promptAdmitMs = options.promptAdmitMs ?? PROMPT_ADMIT_MS;
    this.promptTimeoutMs = options.promptTimeoutMs ?? PROMPT_TIMEOUT_MS;
  }

  describe() {
    return {
      id: "opencode",
      contractVersion: "1",
      adapterVersion: "0.1.0",
      capabilities: { launch: true, reply: true, cancel: true, offline: false },
    };
  }

  async launch(
    request: CloudAgentLaunchRequest,
    context: AdapterContext,
  ): Promise<CloudAgentHandle> {
    const signal = request.signal ?? context.signal;
    const sessionId = opencodeSessionId(request.idempotencyKey);
    const messageId = opencodeMessageId(request.idempotencyKey);
    const directory = repositoryDirectory(this.server.workspaceRoot, request.repository);
    const session = await this.createSession(sessionId, request, directory, signal);
    if (!(await this.runVisible(sessionId, messageId, signal))) {
      await this.sendPrompt(
        sessionId,
        messageId,
        taskPrompt(request.prompt, request.repository, directory, request.openPr),
        request.images,
        signal,
      );
    }
    return this.get(session.id || sessionId, context, messageId);
  }

  async get(id: string, context: AdapterContext, runId?: string): Promise<CloudAgentSnapshot> {
    const session = await this.session(id, context.signal);
    const [active, messages, inbox, vcs] = await Promise.all([
      this.activeSessions(context.signal),
      this.messages(id, context.signal),
      this.inbox(id, context.signal),
      this.vcsBranch(metadataDirectory(session.metadata), context.signal),
    ]);
    return this.snapshot(session, active, messages, inbox, vcs, runId);
  }

  async reply(
    id: string,
    request: CloudAgentReplyRequest,
    context: AdapterContext,
  ): Promise<CloudAgentHandle> {
    const signal = request.signal ?? context.signal;
    // A failed read means nothing was sent, so the caller can safely resend.
    const session = await this.session(id, signal).catch((error: unknown) => {
      if (error instanceof CloudAgentRequestRejected || error instanceof CloudAgentRetryLater)
        throw error;
      throw new CloudAgentRetryLater("unavailable");
    });
    if (session.time?.archived !== undefined) throw new CloudAgentRequestRejected("expired");
    const messageId = opencodeMessageId(randomBytes(16).toString("hex"));
    await this.sendPrompt(id, messageId, request.prompt, request.images, signal);
    return {
      id: session.id,
      url: this.webUrl(session.id),
      title: session.title || "Cloud agent",
      status: "running",
      latestRunId: messageId,
    };
  }

  async cancel(id: string, context: AdapterContext, runId?: string): Promise<CloudAgentSnapshot> {
    const snapshot = await this.get(id, context, runId);
    if (snapshot.status !== "running") return snapshot;
    await this.request(
      "POST",
      `/api/session/${encodeURIComponent(id)}/interrupt`,
      undefined,
      context.signal,
    );
    return this.get(id, context, snapshot.latestRunId);
  }

  private async createSession(
    sessionId: string,
    request: CloudAgentLaunchRequest,
    directory: string | undefined,
    signal: AbortSignal,
  ) {
    const title = sessionTitle(request.prompt);
    const response = await this.request(
      "POST",
      "/api/session",
      {
        id: sessionId,
        title,
        location: { directory: this.server.workspaceRoot },
        permissions: [{ action: "*", resource: "*", effect: "allow" }],
        ...(directory
          ? { metadata: { rakazoDirectory: directory, rakazoRepository: request.repository } }
          : {}),
        ...(this.server.defaultModel ? { model: modelRef(this.server.defaultModel) } : {}),
      },
      signal,
    );
    const parsed = sessionSchema.safeParse(unwrapData(response));
    if (!parsed.success || parsed.data.id !== sessionId)
      throw new Error("Invalid cloud agent response");
    return parsed.data;
  }

  private async session(id: string, signal: AbortSignal) {
    const parsed = sessionSchema.safeParse(
      unwrapData(
        await this.request("GET", `/api/session/${encodeURIComponent(id)}`, undefined, signal),
      ),
    );
    if (!parsed.success || parsed.data.id !== id) throw new Error("Invalid cloud agent response");
    return parsed.data;
  }

  private async activeSessions(signal: AbortSignal): Promise<Set<string>> {
    try {
      const body = unwrapData(await this.request("GET", "/api/session/active", undefined, signal));
      if (!body || typeof body !== "object" || Array.isArray(body)) return new Set();
      return new Set(Object.keys(body));
    } catch (error) {
      // A missing active list is not proof the session is gone; session.get already checked.
      if (error instanceof CloudAgentRetryLater) throw error;
      return new Set();
    }
  }

  private async messages(id: string, signal: AbortSignal) {
    const body = unwrapData(
      await this.request(
        "GET",
        `/api/session/${encodeURIComponent(id)}/message?limit=100&order=desc`,
        undefined,
        signal,
      ),
    );
    const rows = Array.isArray(body) ? body : [];
    return rows.flatMap((row) => {
      const parsed = messageSchema.safeParse(row);
      return parsed.success ? [parsed.data] : [];
    });
  }

  private async inbox(id: string, signal: AbortSignal) {
    try {
      const body = unwrapData(
        await this.request(
          "GET",
          `/api/session/${encodeURIComponent(id)}/inbox`,
          undefined,
          signal,
        ),
      );
      const rows = Array.isArray(body) ? body : [];
      return rows.flatMap((row) => {
        const parsed = messageSchema.safeParse(row);
        return parsed.success ? [parsed.data] : [];
      });
    } catch (error) {
      // Session existence was already checked. A missing inbox route is an empty queue.
      if (error instanceof CloudAgentRetryLater) throw error;
      return [];
    }
  }

  private async vcsBranch(directory: string | undefined, signal: AbortSignal) {
    if (!directory) return undefined;
    try {
      const path = `/api/vcs?${new URLSearchParams({ "location[directory]": directory }).toString()}`;
      const parsed = vcsSchema.safeParse(
        unwrapData(await this.request("GET", path, undefined, signal)),
      );
      return parsed.success ? safeBranch(parsed.data.branch?.current) : undefined;
    } catch {
      return undefined;
    }
  }

  private async runVisible(sessionId: string, messageId: string, signal: AbortSignal) {
    const [messages, inbox] = await Promise.all([
      this.messages(sessionId, signal).catch(() => []),
      this.inbox(sessionId, signal).catch((error: unknown) => {
        if (error instanceof CloudAgentRetryLater || error instanceof CloudAgentRequestRejected)
          throw error;
        return [];
      }),
    ]);
    return [...messages, ...inbox].some((message) => message.id === messageId);
  }

  /**
   * OpenCode 2.0.15–2.1.4 admit a prompt and return. If the HTTP call stays
   * open for the whole turn, keep it running and let get() observe the session.
   */
  private async sendPrompt(
    sessionId: string,
    messageId: string,
    text: string,
    images: CloudAgentImage[] | undefined,
    callerSignal: AbortSignal,
  ) {
    const files = promptFiles(images);
    const background = new AbortController();
    const lifetime = setTimeout(() => background.abort(), this.promptTimeoutMs);
    let detached = false;
    const abortIfAttached = () => {
      if (!detached) background.abort();
    };
    if (callerSignal.aborted) abortIfAttached();
    else callerSignal.addEventListener("abort", abortIfAttached, { once: true });
    const pending = this.request(
      "POST",
      `/api/session/${encodeURIComponent(sessionId)}/prompt`,
      {
        id: messageId,
        text,
        delivery: "steer",
        ...(files ? { files } : {}),
      },
      background.signal,
      this.promptTimeoutMs,
    ).finally(() => clearTimeout(lifetime));

    let admitTimer: ReturnType<typeof setTimeout> | undefined;
    const outcome = await Promise.race([
      pending.then(
        () => "completed" as const,
        (error: unknown) => ({ error }),
      ),
      new Promise<"admitted">((resolve) => {
        admitTimer = setTimeout(() => resolve("admitted"), this.promptAdmitMs);
      }),
      new Promise<"aborted">((resolve) => {
        if (callerSignal.aborted) resolve("aborted");
        else callerSignal.addEventListener("abort", () => resolve("aborted"), { once: true });
      }),
    ]);
    if (admitTimer) clearTimeout(admitTimer);

    if (outcome === "aborted") {
      background.abort();
      void pending.catch(() => undefined);
      callerSignal.removeEventListener("abort", abortIfAttached);
      throw abortError(callerSignal);
    }
    if (outcome === "admitted") {
      detached = true;
      callerSignal.removeEventListener("abort", abortIfAttached);
      void pending.then(
        () => undefined,
        (error: unknown) => {
          if (!isAbort(error)) this.promptFailures.set(failureKey(sessionId, messageId), true);
        },
      );
      return;
    }
    callerSignal.removeEventListener("abort", abortIfAttached);
    if (typeof outcome === "object") throw outcome.error;
  }

  private snapshot(
    session: z.infer<typeof sessionSchema>,
    active: Set<string>,
    messages: z.infer<typeof messageSchema>[],
    inbox: z.infer<typeof messageSchema>[],
    vcsBranch: string | undefined,
    runId?: string,
  ): CloudAgentSnapshot {
    const historyIds = messages
      .filter((message) => message.type === "user" && message.id)
      .map((message) => message.id!);
    // Inbox is oldest-first. Message history was requested newest-first.
    const pendingIds = inbox
      .filter((message) => (message.type === "user" || message.type === undefined) && message.id)
      .map((message) => message.id!);
    const newest = pendingIds.at(-1) ?? historyIds[0];
    const target = runId ?? newest;
    const text = [...messages].map(messageText).join("\n");
    const branch = safeBranch(branchFromText(text)) ?? vcsBranch;
    const prUrl = pullRequestUrl(text);
    const base = {
      id: session.id,
      url: this.webUrl(session.id),
      title: session.title || "Cloud agent",
      ...(branch ? { branch } : {}),
      ...(prUrl ? { prUrl } : {}),
      ...(target ? { latestRunId: target } : {}),
    };
    return {
      ...base,
      status: this.status(session, active, historyIds, pendingIds, messages, target),
    };
  }

  private status(
    session: z.infer<typeof sessionSchema>,
    active: Set<string>,
    historyIds: string[],
    pendingIds: string[],
    messages: z.infer<typeof messageSchema>[],
    target: string | undefined,
  ): CloudAgentStatus {
    const newest = pendingIds.at(-1) ?? historyIds[0];
    const visible =
      target !== undefined && (historyIds.includes(target) || pendingIds.includes(target));
    const latest = target === undefined || target === newest || pendingIds.includes(target);
    if (target && pendingIds.includes(target)) return "running";
    if (active.has(session.id) && latest) return "running";
    if (target && !visible) {
      if (this.promptFailures.get(failureKey(session.id, target))) return "failed";
      return "running";
    }
    if (target && newest && target !== newest) return "finished";
    if (session.outcome === "failed" || (!session.outcome && assistantFailed(messages)))
      return "failed";
    if (session.outcome === "interrupted") return "cancelled";
    if (session.outcome === "succeeded" || session.time?.idle !== undefined) return "finished";
    if (session.time?.archived !== undefined) return "expired";
    if (active.has(session.id)) return "running";
    return "running";
  }

  private webUrl(sessionId: string) {
    const key = Buffer.from(this.server.publicOrigin, "utf8").toString("base64url");
    return `${this.server.publicOrigin}/server/${key}/session/${encodeURIComponent(sessionId)}`;
  }

  private async request(
    method: string,
    path: string,
    body: unknown,
    signal: AbortSignal,
    timeoutMs = this.timeoutMs,
  ): Promise<unknown> {
    const boundedSignal = AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]);
    const url = apiUrl(this.server.apiBase, path);
    let response: Response;
    try {
      response = await this.fetchImpl(url, {
        method,
        redirect: "error",
        signal: boundedSignal,
        headers: {
          Authorization: this.server.authorization,
          Accept: "application/json",
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    } catch (error) {
      if (isAbort(error) || signal.aborted)
        throw abortError(signal.aborted ? signal : boundedSignal);
      throw new Error("Cloud agent request failed");
    }
    if (!response.ok) {
      void response.body?.cancel().catch(() => undefined);
      if (response.status === 404) throw new CloudAgentRequestRejected("not_found");
      if ([400, 409, 422].includes(response.status)) throw new CloudAgentRequestRejected();
      if (response.status === 401 || response.status === 403)
        throw new CloudAgentRetryLater("credential");
      if (response.status === 429)
        throw new CloudAgentRetryLater("rate_limited", retryAfterMs(response.headers));
      throw new Error(`Cloud agent request failed (${response.status})`);
    }
    if (response.status === 204) {
      void response.body?.cancel().catch(() => undefined);
      return {};
    }
    const bytes = await readBodyCapped(response, 1_000_000, boundedSignal);
    try {
      return JSON.parse(new TextDecoder().decode(bytes));
    } catch {
      throw new Error("Invalid cloud agent response");
    }
  }
}

export function opencodeSessionId(key: string) {
  return `ses_${createHash("sha256").update(`session:${key}`).digest("hex")}`;
}

export function opencodeMessageId(key: string) {
  return `msg_${createHash("sha256").update(`message:${key}`).digest("hex")}`;
}

function prepareServer(options: OpenCodeCloudAgentOptions): PreparedServer {
  const password = options.password.trim();
  if (!password) throw new Error("OPENCODE_PASSWORD is required");
  const apiBase = normalizeServerUrl(options.serverUrl);
  const username = options.username?.trim() || "opencode";
  const publicOrigin = publicOriginOf(options.publicUrl, apiBase);
  return {
    apiBase,
    publicOrigin,
    username,
    password,
    authorization: `Basic ${Buffer.from(`${username}:${password}`, "utf8").toString("base64")}`,
    workspaceRoot: workspaceRoot(options.workspaceRoot),
    defaultModel: options.defaultModel?.trim() || undefined,
  };
}

/** http(s) base with no userinfo. The password stays in the Authorization header only. */
function normalizeServerUrl(value: string) {
  let url: URL;
  try {
    url = new URL(value.trim());
  } catch {
    throw new Error("OPENCODE_SERVER_URL must be an http(s) URL");
  }
  if (url.username || url.password)
    throw new Error("OPENCODE_SERVER_URL must not include credentials");
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("OPENCODE_SERVER_URL must be an http(s) URL");
  }
  url.hash = "";
  url.search = "";
  url.username = "";
  url.password = "";
  url.pathname = url.pathname.replace(/\/+$/, "");
  return url.toString().replace(/\/$/, "");
}

function publicOriginOf(publicUrl: string | undefined, apiBase: string) {
  const candidate = publicUrl?.trim();
  if (candidate) {
    try {
      const url = new URL(candidate);
      if (
        !url.username &&
        !url.password &&
        (url.protocol === "http:" || url.protocol === "https:")
      ) {
        return url.origin;
      }
    } catch {
      // Fall back to the server origin.
    }
  }
  return new URL(apiBase).origin;
}

function workspaceRoot(value: string | undefined) {
  const trimmed = value?.trim() || DEFAULT_WORKSPACE_ROOT;
  const parts = trimmed.split("/");
  if (!trimmed.startsWith("/") || parts.includes("..") || parts.includes("."))
    return DEFAULT_WORKSPACE_ROOT;
  return trimmed.replace(/\/+$/, "") || DEFAULT_WORKSPACE_ROOT;
}

function apiUrl(base: string, path: string) {
  const url = new URL(base);
  const prefix = url.pathname.replace(/\/+$/, "");
  const [pathname, search] = path.split("?");
  url.pathname = `${prefix}${pathname}`;
  url.search = search ? `?${search}` : "";
  url.hash = "";
  url.username = "";
  url.password = "";
  return url.toString();
}

function modelRef(value: string) {
  const providerEnd = value.indexOf("/");
  if (providerEnd <= 0) throw new CloudAgentRequestRejected();
  const providerID = value.slice(0, providerEnd);
  const variantStart = value.indexOf("#", providerEnd + 1);
  const id = value.slice(providerEnd + 1, variantStart === -1 ? undefined : variantStart);
  const variant = variantStart === -1 ? undefined : value.slice(variantStart + 1);
  if (!id || providerID.includes("#") || variant === "") throw new CloudAgentRequestRejected();
  return { providerID, id, ...(variant ? { variant } : {}) };
}

function sessionTitle(prompt: string) {
  const line = prompt.split(/\r?\n/, 1)[0]?.trim() || "Cloud agent";
  return line.slice(0, 80);
}

function repositoryDirectory(root: string, repository: string | undefined) {
  if (!repository?.trim()) return undefined;
  const folder = repositoryFolder(repository);
  const directory = `${root}/${folder}`;
  return directory.startsWith(`${root}/`) ? directory : `${root}/repo`;
}

function repositoryFolder(repository: string) {
  const trimmed = repository.trim().replace(/\.git$/i, "");
  const segment = trimmed.split(/[/:]/).filter(Boolean).pop() ?? "repo";
  const folder = segment.replace(/[^A-Za-z0-9._-]/g, "");
  return folder && folder !== "." && folder !== ".." ? folder : "repo";
}

function taskPrompt(
  prompt: string,
  repository: string | undefined,
  directory: string | undefined,
  openPr: boolean | undefined,
) {
  if (!repository || !directory) return prompt;
  const pullRequest =
    openPr === true
      ? "When the work is ready, open a pull request with `gh pr create`. Never merge the pull request."
      : openPr === false
        ? "Do not open a pull request."
        : "Open a pull request with `gh pr create` only if the task asks for one. Never merge a pull request.";
  return [
    `Repository: ${repository.trim()}`,
    `Working directory: ${directory}`,
    "If that directory is missing, clone the repository into it. If it already exists, fetch it.",
    "Create a dedicated branch for this task before changing files. Do not commit on the default branch.",
    pullRequest,
    "In the final message include a line `Branch: <name>` and, when a pull request exists, `Pull request: <https://github.com/org/repo/pull/N>`.",
    "",
    prompt,
  ].join("\n");
}

function promptFiles(images: CloudAgentImage[] | undefined) {
  if (!images?.length) return undefined;
  const files = images.flatMap((image) => {
    if ("url" in image && image.url) return [{ uri: image.url }];
    if ("data" in image && image.data && image.mimeType) {
      return [{ uri: `data:${image.mimeType};base64,${image.data}` }];
    }
    return [];
  });
  return files.length ? files : undefined;
}

function unwrapData(value: unknown): unknown {
  if (value && typeof value === "object" && !Array.isArray(value) && "data" in value) {
    return value.data;
  }
  return value;
}

function metadataDirectory(metadata: Record<string, unknown> | undefined) {
  const value = metadata?.rakazoDirectory;
  return typeof value === "string" && value.startsWith("/") && !value.split("/").includes("..")
    ? value
    : undefined;
}

function messageText(message: z.infer<typeof messageSchema>) {
  return [message.text, textOf(message.payload), textOf(message.content)]
    .filter((part): part is string => typeof part === "string" && part.length > 0)
    .join("\n");
}

function textOf(value: unknown): string {
  if (typeof value === "string") return value;
  if (!value || typeof value !== "object") return "";
  if (Array.isArray(value)) return value.map(textOf).filter(Boolean).join("\n");
  const record = value as Record<string, unknown>;
  return [record.text, record.content, record.output].map(textOf).filter(Boolean).join("\n");
}

function branchFromText(text: string) {
  const match = text.match(/(?:^|\n)\s*Branch:\s*([A-Za-z0-9][A-Za-z0-9._/-]{0,200})/);
  return match?.[1];
}

function pullRequestUrl(text: string) {
  const labeled = text.match(
    /(?:^|\n)\s*Pull request:\s*(https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/pull\/\d+)/,
  );
  if (labeled?.[1]) return labeled[1];
  return text.match(/https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/pull\/\d+/)?.[0];
}

function safeBranch(value: string | undefined) {
  if (!value) return undefined;
  return /^[A-Za-z0-9][A-Za-z0-9._/-]{0,200}$/.test(value) ? value : undefined;
}

function assistantFailed(messages: z.infer<typeof messageSchema>[]) {
  const assistant = messages.find((message) => message.type === "assistant");
  if (!assistant) return false;
  return assistant.finish === "error" || assistant.error !== undefined;
}

function failureKey(sessionId: string, messageId: string) {
  return `${sessionId}:${messageId}`;
}

function retryAfterMs(headers: Headers) {
  const value = headers.get("retry-after")?.trim();
  if (!value) return undefined;
  const ms = /^\d+$/.test(value) ? Number(value) * 1_000 : Date.parse(value) - Date.now();
  return Number.isFinite(ms) ? Math.max(0, ms) : undefined;
}

function isAbort(error: unknown) {
  return (
    (error instanceof Error && (error.name === "AbortError" || error.name === "TimeoutError")) ||
    (typeof error === "object" &&
      error !== null &&
      "name" in error &&
      (error.name === "AbortError" || error.name === "TimeoutError"))
  );
}

function abortError(signal: AbortSignal) {
  if (signal.reason instanceof Error) return signal.reason;
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}
