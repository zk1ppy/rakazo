import type { AdapterContext } from "@rakazo/adapter-kit";
import { CloudAgentRequestRejected, CloudAgentRetryLater } from "@rakazo/adapter-kit";
import { describe, expect, it } from "vitest";
import { cloudAgentsEnabled, createCloudAgentConnection } from "./cloud-agent-factory.js";
import { resolveCloudAgentProvider } from "./cloud-agent-provider-env.js";
import {
  OpenCodeCloudAgentProvider,
  opencodeMessageId,
  opencodeSessionId,
} from "./opencode-cloud-agent.js";

const PASSWORD = "super-secret-password";
const SERVER = "http://opencode.example:5003";
const PUBLIC = "https://opencode.example";

const context: AdapterContext = {
  operationId: "test",
  traceId: "test",
  spaceId: "test-space",
  userId: "test-user",
  signal: new AbortController().signal,
};

interface StoredSession {
  id: string;
  title: string;
  outcome?: string;
  idle?: number;
  archived?: number;
  active: boolean;
  directory?: string;
  messages: Array<Record<string, unknown>>;
  inbox: Array<Record<string, unknown>>;
  branch?: { current?: string; default?: string };
}

class OpenCodeWire {
  readonly requests: Array<{
    method: string;
    path: string;
    body: unknown;
    url: string;
    authorization: string | null;
    redirect: RequestRedirect | undefined;
  }> = [];
  readonly sessions = new Map<string, StoredSession>();
  readonly promptSignals: AbortSignal[] = [];
  failNext: number | undefined;
  hangPrompt = false;

  fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(String(input));
    const body = init?.body ? JSON.parse(String(init.body)) : undefined;
    this.requests.push({
      method: init?.method ?? "GET",
      path: `${url.pathname}${url.search}`,
      body,
      url: String(input),
      authorization: new Headers(init?.headers).get("authorization"),
      redirect: init?.redirect,
    });
    expect(String(input)).not.toContain(PASSWORD);
    if (init?.signal?.aborted) {
      const error = new Error("The operation was aborted");
      error.name = "AbortError";
      throw error;
    }
    if (this.failNext !== undefined) {
      const status = this.failNext;
      this.failNext = undefined;
      return new Response(`leaked ${PASSWORD}`, { status });
    }
    if (url.pathname === "/api/session" && init?.method === "POST") return this.create(body);
    if (url.pathname === "/api/session/active") return this.active();
    const sessionMatch = url.pathname.match(/^\/api\/session\/([^/]+)$/);
    if (sessionMatch && (init?.method ?? "GET") === "GET")
      return this.read(decodeURIComponent(sessionMatch[1]!));
    const messages = url.pathname.match(/^\/api\/session\/([^/]+)\/message$/);
    if (messages) return this.listMessages(decodeURIComponent(messages[1]!));
    const inbox = url.pathname.match(/^\/api\/session\/([^/]+)\/inbox$/);
    if (inbox) return this.listInbox(decodeURIComponent(inbox[1]!));
    const prompt = url.pathname.match(/^\/api\/session\/([^/]+)\/prompt$/);
    if (prompt && init?.method === "POST")
      return this.prompt(decodeURIComponent(prompt[1]!), body, init);
    const interrupt = url.pathname.match(/^\/api\/session\/([^/]+)\/interrupt$/);
    if (interrupt && init?.method === "POST")
      return this.interrupt(decodeURIComponent(interrupt[1]!));
    if (url.pathname === "/api/vcs") return this.vcs(url);
    return new Response(null, { status: 404 });
  };

  private create(body: { id: string; title?: string; metadata?: { rakazoDirectory?: string } }) {
    const existing = this.sessions.get(body.id);
    if (existing) return Response.json({ data: this.info(existing) });
    const session: StoredSession = {
      id: body.id,
      title: body.title || "Cloud agent",
      active: false,
      directory: body.metadata?.rakazoDirectory,
      messages: [],
      inbox: [],
    };
    this.sessions.set(body.id, session);
    return Response.json({ data: this.info(session) });
  }

  private read(id: string) {
    const session = this.sessions.get(id);
    if (!session) return new Response(null, { status: 404 });
    return Response.json({ data: this.info(session) });
  }

  private info(session: StoredSession) {
    return {
      id: session.id,
      title: session.title,
      ...(session.outcome ? { outcome: session.outcome } : {}),
      time: {
        created: 1,
        updated: 1,
        ...(session.idle !== undefined ? { idle: session.idle } : {}),
        ...(session.archived !== undefined ? { archived: session.archived } : {}),
      },
      ...(session.directory ? { metadata: { rakazoDirectory: session.directory } } : {}),
    };
  }

  private active() {
    const data: Record<string, { type: "running" }> = {};
    for (const session of this.sessions.values()) {
      if (session.active) data[session.id] = { type: "running" };
    }
    return Response.json({ data });
  }

  private listMessages(id: string) {
    const session = this.sessions.get(id);
    if (!session) return new Response(null, { status: 404 });
    return Response.json({ data: [...session.messages].reverse() });
  }

  private listInbox(id: string) {
    const session = this.sessions.get(id);
    if (!session) return new Response(null, { status: 404 });
    return Response.json({ data: session.inbox });
  }

  private prompt(
    id: string,
    body: { id: string; text: string; delivery?: string; files?: unknown },
    init: RequestInit,
  ) {
    const session = this.sessions.get(id);
    if (!session) return new Response(null, { status: 404 });
    if (this.hangPrompt) {
      if (init.signal) this.promptSignals.push(init.signal);
      return new Promise<Response>((_resolve, reject) => {
        init.signal?.addEventListener("abort", () => {
          const error = new Error("The operation was aborted");
          error.name = "AbortError";
          reject(error);
        });
      });
    }
    const message = { id: body.id, type: "user", text: body.text };
    session.messages.push(message);
    session.active = true;
    session.outcome = undefined;
    session.idle = undefined;
    return Response.json({ data: { id: body.id, delivery: body.delivery ?? "steer" } });
  }

  private interrupt(id: string) {
    const session = this.sessions.get(id);
    if (!session) return new Response(null, { status: 404 });
    session.active = false;
    session.outcome = "interrupted";
    return Response.json({ interrupted: true });
  }

  private vcs(url: URL) {
    const directory = url.searchParams.get("location[directory]");
    const session = [...this.sessions.values()].find((item) => item.directory === directory);
    if (!session?.branch) return new Response(null, { status: 404 });
    return Response.json({ data: { provider: "git", branch: session.branch } });
  }
}

function promptText(wire: OpenCodeWire) {
  const body = wire.requests.find((request) => request.path.endsWith("/prompt"))?.body;
  if (!body || typeof body !== "object" || !("text" in body)) throw new Error("missing prompt");
  return String(body.text);
}

function provider(
  wire: OpenCodeWire,
  options: Partial<ConstructorParameters<typeof OpenCodeCloudAgentProvider>[0]> = {},
) {
  return new OpenCodeCloudAgentProvider({
    serverUrl: SERVER,
    publicUrl: PUBLIC,
    password: PASSWORD,
    fetch: wire.fetch,
    ...options,
  });
}

describe("OpenCode cloud agent", () => {
  it("creates a session, admits a steered prompt, and builds the public session URL", async () => {
    const wire = new OpenCodeWire();
    const result = await provider(wire).launch(
      {
        idempotencyKey: "operation",
        prompt: "Add a README",
        repository: "https://github.com/example/rakazo",
        openPr: true,
        images: [{ data: "ZmFrZQ==", mimeType: "image/png" }],
      },
      context,
    );
    const sessionId = opencodeSessionId("operation");
    const messageId = opencodeMessageId("operation");
    expect(result).toMatchObject({
      id: sessionId,
      status: "running",
      latestRunId: messageId,
      title: "Add a README",
      url: `${PUBLIC}/server/${Buffer.from(PUBLIC).toString("base64url")}/session/${sessionId}`,
    });
    const create = wire.requests.find(
      (request) => request.method === "POST" && request.path === "/api/session",
    );
    expect(create?.body).toMatchObject({
      id: sessionId,
      title: "Add a README",
      location: { directory: "/workspace/repos" },
      permissions: [{ action: "*", resource: "*", effect: "allow" }],
      metadata: {
        rakazoDirectory: "/workspace/repos/rakazo",
        rakazoRepository: "https://github.com/example/rakazo",
      },
    });
    expect(create?.authorization).toBe(
      `Basic ${Buffer.from(`opencode:${PASSWORD}`).toString("base64")}`,
    );
    expect(create?.redirect).toBe("error");
    const prompt = wire.requests.find((request) => request.path.endsWith("/prompt"));
    expect(prompt?.body).toMatchObject({
      id: messageId,
      delivery: "steer",
      files: [{ uri: "data:image/png;base64,ZmFrZQ==" }],
    });
    const text = promptText(wire);
    expect(text).toContain("Working directory: /workspace/repos/rakazo");
    expect(text).toContain("gh pr create");
    expect(text).toContain("Never merge");
    expect(text).toContain("Add a README");
  });

  it("replays launch onto the same session without a second prompt", async () => {
    const wire = new OpenCodeWire();
    const agent = provider(wire);
    const first = await agent.launch({ idempotencyKey: "same", prompt: "Task" }, context);
    const prompts = () => wire.requests.filter((request) => request.path.endsWith("/prompt"));
    expect(prompts()).toHaveLength(1);
    const second = await agent.launch({ idempotencyKey: "same", prompt: "Task" }, context);
    expect(second.id).toBe(first.id);
    expect(prompts()).toHaveLength(1);
  });

  it("returns while a prompt request is still open and does not abort it", async () => {
    const wire = new OpenCodeWire();
    wire.hangPrompt = true;
    const agent = provider(wire, { promptAdmitMs: 40, promptTimeoutMs: 400 });
    const started = Date.now();
    const result = await agent.launch({ idempotencyKey: "hang", prompt: "Task" }, context);
    expect(Date.now() - started).toBeLessThan(500);
    expect(result.status).toBe("running");
    expect(result.latestRunId).toBe(opencodeMessageId("hang"));
    expect(wire.promptSignals[0]?.aborted).toBe(false);
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(wire.promptSignals[0]?.aborted).toBe(true);
  });

  it("aborts an in-flight prompt when the caller aborts before admission", async () => {
    const wire = new OpenCodeWire();
    const controller = new AbortController();
    const inner = wire.fetch;
    wire.fetch = async (input, init) => {
      if (String(input).endsWith("/prompt")) {
        controller.abort();
        return await new Promise<Response>((_resolve, reject) => {
          const fail = () => {
            const error = new Error("The operation was aborted");
            error.name = "AbortError";
            reject(error);
          };
          if (init?.signal?.aborted) fail();
          else init?.signal?.addEventListener("abort", fail, { once: true });
        });
      }
      return inner(input, init);
    };
    const agent = provider(wire, { promptAdmitMs: 5_000, promptTimeoutMs: 5_000 });
    await expect(
      agent.launch(
        { idempotencyKey: "abort", prompt: "Task" },
        { ...context, signal: controller.signal },
      ),
    ).rejects.toThrow();
  });

  it("maps a running, finished, failed, cancelled, and errored session", async () => {
    const wire = new OpenCodeWire();
    const agent = provider(wire);
    const running = await agent.launch(
      { idempotencyKey: "status", prompt: "Task", repository: "example/demo" },
      context,
    );
    expect((await agent.get(running.id, context)).status).toBe("running");
    const session = wire.sessions.get(running.id)!;
    session.active = false;
    session.outcome = "succeeded";
    session.idle = 10;
    session.branch = { current: "feature/from-vcs", default: "main" };
    session.messages.push({
      id: "msg_assistant",
      type: "assistant",
      content: [
        {
          type: "text",
          text: "Branch: feature/readme\nPull request: https://github.com/example/demo/pull/7",
        },
      ],
    });
    const done = await agent.get(running.id, context);
    expect(done).toMatchObject({
      status: "finished",
      branch: "feature/readme",
      prUrl: "https://github.com/example/demo/pull/7",
      latestRunId: running.latestRunId,
    });
    session.messages.pop();
    const fromVcs = await agent.get(running.id, context);
    expect(fromVcs.branch).toBe("feature/from-vcs");
    session.outcome = "failed";
    expect((await agent.get(running.id, context)).status).toBe("failed");
    session.outcome = "interrupted";
    expect((await agent.get(running.id, context)).status).toBe("cancelled");
    session.outcome = undefined;
    session.idle = undefined;
    session.messages.push({
      id: "msg_err",
      type: "assistant",
      finish: "error",
      error: { message: PASSWORD },
    });
    const failed = await agent.get(running.id, context);
    expect(failed.status).toBe("failed");
    expect(JSON.stringify(failed)).not.toContain(PASSWORD);
  });

  it("keeps an older run finished after a follow-up and cancels the active one", async () => {
    const wire = new OpenCodeWire();
    const agent = provider(wire);
    const first = await agent.launch({ idempotencyKey: "follow", prompt: "Task" }, context);
    wire.sessions.get(first.id)!.active = false;
    wire.sessions.get(first.id)!.outcome = "succeeded";
    const followup = await agent.reply(first.id, { prompt: "Also add tests" }, context);
    expect(followup.latestRunId).not.toBe(first.latestRunId);
    expect(followup.status).toBe("running");
    expect((await agent.get(first.id, context, first.latestRunId)).status).toBe("finished");
    expect((await agent.get(first.id, context, followup.latestRunId)).status).toBe("running");
    expect((await agent.cancel(first.id, context)).status).toBe("cancelled");
    expect(wire.requests.some((request) => request.path.endsWith("/interrupt"))).toBe(true);
    expect((await agent.cancel(first.id, context)).status).toBe("cancelled");
  });

  it("does not ask for a pull request when openPr is false and uses a short repo folder", async () => {
    const wire = new OpenCodeWire();
    await provider(wire).launch(
      {
        idempotencyKey: "no-pr",
        prompt: "Task",
        repository: "zk1ppy/servers-config",
        openPr: false,
      },
      context,
    );
    const text = promptText(wire);
    expect(text).toContain("Working directory: /workspace/repos/servers-config");
    expect(text).toContain("Do not open a pull request.");
    expect(text).not.toContain("gh pr create");
  });

  it("sends the configured model and a custom workspace root", async () => {
    const wire = new OpenCodeWire();
    await provider(wire, {
      workspaceRoot: "/workspace/repos",
      defaultModel: "anthropic/claude#fast",
    }).launch({ idempotencyKey: "model", prompt: "Task" }, context);
    expect(wire.requests[0]?.body).toMatchObject({
      model: { providerID: "anthropic", id: "claude", variant: "fast" },
    });
  });

  it("refuses a follow-up to an archived session before sending a prompt", async () => {
    const wire = new OpenCodeWire();
    const agent = provider(wire);
    const first = await agent.launch({ idempotencyKey: "archived", prompt: "Task" }, context);
    wire.sessions.get(first.id)!.archived = 5;
    wire.sessions.get(first.id)!.active = false;
    const prompts = wire.requests.filter((request) => request.path.endsWith("/prompt")).length;
    const error = await agent
      .reply(first.id, { prompt: "More" }, context)
      .catch((caught) => caught);
    expect(error).toMatchObject({ name: "CloudAgentRequestRejected", reason: "expired" });
    expect(wire.requests.filter((request) => request.path.endsWith("/prompt"))).toHaveLength(
      prompts,
    );
  });

  it.each([
    [401, CloudAgentRetryLater, { reason: "credential" }],
    [403, CloudAgentRetryLater, { reason: "credential" }],
    [404, CloudAgentRequestRejected, { reason: "not_found" }],
    [400, CloudAgentRequestRejected, { reason: "invalid" }],
    [429, CloudAgentRetryLater, { reason: "rate_limited", retryAfterMs: 30_000 }],
  ])("classifies a %s response without the secret", async (status, type, fields) => {
    const agent = new OpenCodeCloudAgentProvider({
      serverUrl: SERVER,
      password: PASSWORD,
      fetch: async () =>
        new Response(`nope ${PASSWORD}`, { status, headers: { "retry-after": "30" } }),
    });
    const error = await agent.get("ses_test", context).catch((caught) => caught);
    expect(error).toBeInstanceOf(type);
    expect(error).toMatchObject(fields);
    expect(String(error)).not.toContain(PASSWORD);
  });

  it("hides a 500 body and a credential in the server URL", async () => {
    const agent = new OpenCodeCloudAgentProvider({
      serverUrl: SERVER,
      password: PASSWORD,
      fetch: async () => new Response(`down ${PASSWORD}`, { status: 500 }),
    });
    const error = await agent.get("ses_test", context).catch((caught: Error) => caught);
    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(CloudAgentRetryLater);
    expect(String(error)).toContain("500");
    expect(String(error)).not.toContain(PASSWORD);
    expect(
      () =>
        new OpenCodeCloudAgentProvider({
          serverUrl: `http://opencode:${PASSWORD}@opencode.example`,
          password: PASSWORD,
        }),
    ).toThrow(/must not include credentials/);
    try {
      new OpenCodeCloudAgentProvider({
        serverUrl: `http://opencode:${PASSWORD}@opencode.example`,
        password: PASSWORD,
      });
    } catch (caught) {
      expect(String(caught)).not.toContain(PASSWORD);
    }
  });

  it("uses a custom username and falls back to the server origin", async () => {
    const wire = new OpenCodeWire();
    const agent = new OpenCodeCloudAgentProvider({
      serverUrl: `${SERVER}/`,
      password: PASSWORD,
      username: "bot",
      fetch: wire.fetch,
    });
    const result = await agent.launch({ idempotencyKey: "basic", prompt: "Task" }, context);
    expect(wire.requests[0]?.authorization).toBe(
      `Basic ${Buffer.from(`bot:${PASSWORD}`).toString("base64")}`,
    );
    expect(result.url.startsWith(`${SERVER}/server/`)).toBe(true);
    expect(result.url).not.toContain(PASSWORD);
  });
});

describe("OpenCode cloud agent composition", () => {
  const configured = {
    CLOUD_AGENT_PROVIDER: "opencode",
    OPENCODE_SERVER_URL: SERVER,
    OPENCODE_PASSWORD: PASSWORD,
  };

  it("fails closed without a server URL and password", () => {
    for (const source of [
      { CLOUD_AGENT_PROVIDER: "opencode" },
      { CLOUD_AGENT_PROVIDER: "opencode", OPENCODE_SERVER_URL: SERVER },
      { CLOUD_AGENT_PROVIDER: "opencode", OPENCODE_PASSWORD: PASSWORD },
      {
        CLOUD_AGENT_PROVIDER: "opencode",
        OPENCODE_SERVER_URL: `http://opencode:${PASSWORD}@opencode.example`,
        OPENCODE_PASSWORD: PASSWORD,
      },
      {
        CLOUD_AGENT_PROVIDER: "opencode",
        OPENCODE_SERVER_URL: "ftp://opencode.example",
        OPENCODE_PASSWORD: PASSWORD,
      },
    ]) {
      expect(resolveCloudAgentProvider(source)).toBe("none");
      expect(createCloudAgentConnection(source)).toBeNull();
    }
  });

  it("accepts the server-password alias and scopes a named Space", () => {
    const alias = createCloudAgentConnection({
      CLOUD_AGENT_PROVIDER: "opencode",
      OPENCODE_SERVER_URL: SERVER,
      OPENCODE_SERVER_PASSWORD: PASSWORD,
    })!;
    expect(alias.allSpaces).toBe(true);
    expect(alias.spaceId).toBeUndefined();
    expect(cloudAgentsEnabled(alias, "test-space")).toBe(true);
    expect(cloudAgentsEnabled(alias, "other-space")).toBe(true);
    expect(alias.key).not.toContain(PASSWORD);
    const starred = createCloudAgentConnection({ ...configured, CLOUD_AGENT_SPACE_ID: "*" })!;
    expect(starred.allSpaces).toBe(true);
    expect(cloudAgentsEnabled(starred, "another-space")).toBe(true);
    const bound = createCloudAgentConnection({
      ...configured,
      CLOUD_AGENT_SPACE_ID: "test-space",
    })!;
    expect(bound.allSpaces).toBeUndefined();
    expect(cloudAgentsEnabled(bound, "test-space")).toBe(true);
    expect(cloudAgentsEnabled(bound, "other-space")).toBe(false);
    const rotated = createCloudAgentConnection({
      ...configured,
      OPENCODE_PASSWORD: "other-secret",
    })!;
    expect(rotated.key).not.toBe(alias.key);
    expect(rotated.key).not.toContain("other-secret");
  });

  it("does not treat an unscoped provider object as enabled", () => {
    expect(
      cloudAgentsEnabled(
        {
          provider: new OpenCodeCloudAgentProvider({ serverUrl: SERVER, password: PASSWORD }),
          key: "test",
        },
        "test-space",
      ),
    ).toBe(false);
  });
});
