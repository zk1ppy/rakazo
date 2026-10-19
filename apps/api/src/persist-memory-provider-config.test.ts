import { MemoryProviderDeploymentOwnerRequiredError } from "@rakazo/adapters";
import type { PrismaClient } from "@rakazo/db";
import { describe, expect, it, vi } from "vitest";
import {
  disconnectMemoryProvider,
  persistMemoryProviderConfig,
  updateMemoryProviderDefaultScope,
  updateNeo4jExtraction,
} from "./memory-provider-config.js";

const actor = {
  userId: "user-1",
  spaceId: "ws-1",
  email: "a@b.com",
  isDeploymentOwner: false,
};
const deploymentOwner = { ...actor, isDeploymentOwner: true };

function makeDeps(
  overrides: {
    existing?: { id: string; secretId: string } | null;
    upsertResult?: {
      provider: string;
      settings: Record<string, string>;
      defaultMemoryScope: string;
      updatedAt: Date;
    };
    updateResult?: {
      provider: string;
      settings: Record<string, string>;
      defaultMemoryScope: string;
      updatedAt: Date;
    };
    spaceOwner?: boolean;
    memberRole?: string;
  } = {},
) {
  const secretCreate = vi.fn().mockResolvedValue({ id: "secret-new" });
  const secretDeleteMany = vi.fn().mockResolvedValue({ count: 1 });
  const findUnique = vi.fn().mockResolvedValue(overrides.existing ?? null);
  const upsert = vi.fn().mockResolvedValue(
    overrides.upsertResult ?? {
      provider: "supermemory",
      settings: { mode: "cloud", baseUrl: "https://api.supermemory.ai" },
      defaultMemoryScope: "isolated",
      updatedAt: new Date("2026-08-19T00:00:00.000Z"),
    },
  );
  const update = vi.fn().mockResolvedValue(
    overrides.updateResult ?? {
      provider: "supermemory",
      settings: { mode: "cloud", baseUrl: "https://api.supermemory.ai" },
      defaultMemoryScope: "shared",
      updatedAt: new Date("2026-08-20T00:00:00.000Z"),
    },
  );
  const deleteConfig = vi.fn().mockResolvedValue({ id: "cfg-1" });
  const prisma = {
    spaceMember: {
      findUnique: vi
        .fn()
        .mockResolvedValue(
          overrides.spaceOwner === false ? null : { role: overrides.memberRole ?? "owner" },
        ),
    },
    spaceMemoryConfig: { findUnique, update, upsert, delete: deleteConfig },
    secret: { create: secretCreate, deleteMany: secretDeleteMany },
    $transaction: vi.fn(),
  };
  prisma.$transaction.mockImplementation(async (callback: (tx: typeof prisma) => unknown) =>
    callback(prisma),
  );
  const deps = {
    prisma: prisma as unknown as PrismaClient,
    secrets: { put: vi.fn().mockResolvedValue({ id: "secret-new", ciphertext: "cipher" }) },
  };
  return {
    deps,
    secretCreate,
    secretDeleteMany,
    findUnique,
    upsert,
    update,
    deleteConfig,
    transaction: prisma.$transaction,
  };
}

function connectionInput(mode: "cloud" | "local", baseUrl?: string) {
  return {
    provider: "supermemory",
    settings: { mode, ...(baseUrl ? { baseUrl } : {}) },
    credentials: { apiKey: "sm_test_key_12345" },
    defaultMemoryScope: "isolated" as const,
  };
}

describe("persistMemoryProviderConfig", () => {
  it("rejects unknown providers as bad requests without probing or writing", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { deps, transaction } = makeDeps();
    try {
      await expect(
        persistMemoryProviderConfig(deps, actor, {
          ...connectionInput("cloud"),
          provider: "unknown-provider",
        }),
      ).rejects.toMatchObject({ code: "BAD_REQUEST" });
      expect(fetchMock).not.toHaveBeenCalled();
      expect(transaction).not.toHaveBeenCalled();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it.each(["#", "?action=delete", "?"])(
    "rejects ambiguous local URLs even for the deployment owner: %s",
    async (suffix) => {
      const fetchMock = vi.fn();
      vi.stubGlobal("fetch", fetchMock);
      const { deps, transaction } = makeDeps();
      try {
        await expect(
          persistMemoryProviderConfig(
            deps,
            deploymentOwner,
            connectionInput("local", `http://127.0.0.1:8123/internal-action${suffix}`),
          ),
        ).rejects.toMatchObject({ code: "BAD_REQUEST" });
        expect(fetchMock).not.toHaveBeenCalled();
        expect(transaction).not.toHaveBeenCalled();
      } finally {
        vi.unstubAllGlobals();
      }
    },
  );

  it.each([
    "http://127.0.0.1:8123/internal-action#",
    "http://localhost:6767",
    "http://10.0.0.8:6767",
    "http://supermemory:6767",
  ])("rejects ordinary Space owners before any local probe or write: %s", async (baseUrl) => {
    const fetchMock = vi.fn().mockResolvedValue(new Response("[]"));
    vi.stubGlobal("fetch", fetchMock);
    const { deps, transaction } = makeDeps();
    try {
      await expect(
        persistMemoryProviderConfig(deps, actor, connectionInput("local", baseUrl)),
      ).rejects.toMatchObject({ code: "FORBIDDEN" });
      expect(fetchMock).not.toHaveBeenCalled();
      expect(deps.secrets.put).not.toHaveBeenCalled();
      expect(transaction).not.toHaveBeenCalled();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("still requires Space ownership for a deployment owner", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { deps, transaction } = makeDeps({ spaceOwner: false });
    try {
      await expect(
        persistMemoryProviderConfig(
          deps,
          deploymentOwner,
          connectionInput("local", "http://localhost:6767"),
        ),
      ).rejects.toMatchObject({ code: "FORBIDDEN" });
      expect(fetchMock).not.toHaveBeenCalled();
      expect(transaction).not.toHaveBeenCalled();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("rejects non-owners before probing or writing Space configuration", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { deps, upsert } = makeDeps({ spaceOwner: false });

    await expect(
      persistMemoryProviderConfig(deps, actor, connectionInput("cloud")),
    ).rejects.toThrow();

    expect(fetchMock).not.toHaveBeenCalled();
    expect(upsert).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });

  it("rejects local mode without a baseUrl, without touching the database", async () => {
    const { deps, upsert } = makeDeps();
    await expect(
      persistMemoryProviderConfig(deps, deploymentOwner, connectionInput("local")),
    ).rejects.toThrow(/baseUrl/);
    expect(upsert).not.toHaveBeenCalled();
  });

  it("rejects a metadata baseUrl in local mode without probing or touching the database", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { deps, upsert } = makeDeps();
    await expect(
      persistMemoryProviderConfig(
        deps,
        deploymentOwner,
        connectionInput("local", "http://169.254.169.254/latest/meta-data/"),
      ),
    ).rejects.toThrow(/blocked address/);
    expect(fetchMock).not.toHaveBeenCalled();
    expect(upsert).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });

  it("rejects a public HTTP base URL in local mode without probing", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const { deps, upsert } = makeDeps();
    await expect(
      persistMemoryProviderConfig(
        deps,
        deploymentOwner,
        connectionInput("local", "http://203.0.113.10:6767"),
      ),
    ).rejects.toThrow(/private-network/);
    expect(fetchMock).not.toHaveBeenCalled();
    expect(upsert).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });

  it("accepts a private-network base URL in local mode for the deployment owner", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response("[]", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const { deps, upsert } = makeDeps();

    await persistMemoryProviderConfig(
      deps,
      deploymentOwner,
      connectionInput("local", "http://10.0.0.8:6767"),
    );

    expect(fetchMock.mock.calls[0]![0]).toBe("http://10.0.0.8:6767/v3/container-tags/list");
    expect(upsert).toHaveBeenCalledWith(
      expect.objectContaining({
        create: expect.objectContaining({
          settings: { mode: "local", baseUrl: "http://10.0.0.8:6767" },
        }),
      }),
    );
    vi.unstubAllGlobals();
  });

  it("probes before persisting, and rejects (without writing) when the probe fails", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("", { status: 401 })));
    const { deps, upsert } = makeDeps();
    await expect(
      persistMemoryProviderConfig(deps, deploymentOwner, {
        ...connectionInput("local", "http://localhost:6767"),
        credentials: { apiKey: "sm_bad_key" },
      }),
    ).rejects.toThrow();
    expect(upsert).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });

  it("accepts a bracketed IPv6 loopback base URL in local mode", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response("[]", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const { deps, upsert } = makeDeps();

    await persistMemoryProviderConfig(
      deps,
      deploymentOwner,
      connectionInput("local", "http://[::1]:6767"),
    );

    expect(fetchMock.mock.calls[0]![0]).toBe("http://[::1]:6767/v3/container-tags/list");
    expect(upsert).toHaveBeenCalled();
    vi.unstubAllGlobals();
  });

  it("connects cloud mode, defaulting the base URL, and returns the serialized config", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("[]", { status: 200 })));
    const { deps, upsert, transaction } = makeDeps();
    const result = await persistMemoryProviderConfig(deps, actor, connectionInput("cloud"));
    expect(upsert).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { spaceId: "ws-1" },
        create: expect.objectContaining({
          provider: "supermemory",
          settings: { mode: "cloud", baseUrl: "https://api.supermemory.ai" },
        }),
      }),
    );
    expect(result).toEqual({
      provider: "supermemory",
      settings: { mode: "cloud", baseUrl: "https://api.supermemory.ai" },
      defaultMemoryScope: "isolated",
      updatedAt: "2026-08-19T00:00:00.000Z",
    });
    expect(transaction).toHaveBeenCalledOnce();
    vi.unstubAllGlobals();
  });

  it("deletes the old secret when replacing an existing config with a new key", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("[]", { status: 200 })));
    const { deps, secretDeleteMany } = makeDeps({
      existing: { id: "cfg-1", secretId: "secret-old" },
    });
    await persistMemoryProviderConfig(deps, actor, {
      ...connectionInput("cloud"),
      credentials: { apiKey: "sm_new_key_12345" },
    });
    expect(secretDeleteMany).toHaveBeenCalledWith({ where: { id: "secret-old" } });
    vi.unstubAllGlobals();
  });

  it("rejects non-deployment-owners for public-looking Serenity hostnames that resolve private, without probing", async () => {
    const prepareConnection = vi.fn();
    const { deps, transaction } = makeDeps();
    await expect(
      persistMemoryProviderConfig(
        {
          ...deps,
          classifySettings: async () => ({
            endpoint: "https://serenity.example.test/mcp",
            endpointTrust: "private",
          }),
          prepareConnection,
        },
        actor,
        {
          provider: "serenity",
          settings: { endpoint: "https://serenity.example.test/mcp", allowWrites: "false" },
          credentials: { token: "serenity_test_token" },
          defaultMemoryScope: "isolated",
        },
      ),
    ).rejects.toMatchObject({ code: "FORBIDDEN" });
    expect(prepareConnection).not.toHaveBeenCalled();
    expect(transaction).not.toHaveBeenCalled();
  });

  it("rejects non-owners when prepare reclassifies a hostname as private before probing", async () => {
    const prepareConnection = vi.fn(async () => {
      throw new MemoryProviderDeploymentOwnerRequiredError();
    });
    const { deps, transaction } = makeDeps();
    await expect(
      persistMemoryProviderConfig(
        {
          ...deps,
          classifySettings: async () => ({
            endpoint: "https://serenity.example.test/mcp",
          }),
          prepareConnection,
        },
        actor,
        {
          provider: "serenity",
          settings: { endpoint: "https://serenity.example.test/mcp", allowWrites: "false" },
          credentials: { token: "serenity_test_token" },
          defaultMemoryScope: "isolated",
        },
      ),
    ).rejects.toMatchObject({ code: "FORBIDDEN" });
    expect(prepareConnection).toHaveBeenCalledWith(
      expect.objectContaining({ allowPrivateEndpoint: false }),
    );
    expect(transaction).not.toHaveBeenCalled();
  });
});

describe("disconnectMemoryProvider", () => {
  it("rejects non-owners before opening a transaction", async () => {
    const { deps, transaction, secretDeleteMany } = makeDeps({ spaceOwner: false });

    await expect(disconnectMemoryProvider(deps, actor)).rejects.toThrow();

    expect(transaction).not.toHaveBeenCalled();
    expect(secretDeleteMany).not.toHaveBeenCalled();
  });

  it("disconnects an existing provider and removes its secret in the same transaction", async () => {
    const { deps, transaction, deleteConfig, secretDeleteMany } = makeDeps({
      existing: { id: "cfg-1", secretId: "secret-old" },
    });

    await expect(disconnectMemoryProvider(deps, actor)).resolves.toEqual({ ok: true });

    expect(transaction).toHaveBeenCalledExactlyOnceWith(expect.any(Function), {
      isolationLevel: "Serializable",
    });
    expect(deleteConfig).toHaveBeenCalledWith({ where: { id: "cfg-1" } });
    expect(secretDeleteMany).toHaveBeenCalledWith({ where: { id: "secret-old" } });
  });

  it("leaves secrets untouched when no provider is configured", async () => {
    const { deps, deleteConfig, secretDeleteMany } = makeDeps();

    await expect(disconnectMemoryProvider(deps, actor)).resolves.toEqual({ ok: true });

    expect(deleteConfig).not.toHaveBeenCalled();
    expect(secretDeleteMany).not.toHaveBeenCalled();
  });
});

describe("updateMemoryProviderDefaultScope", () => {
  it("accepts owners with additional Better Auth roles", async () => {
    const { deps, update } = makeDeps({
      existing: { id: "cfg-1", secretId: "secret-existing" },
      memberRole: "owner,admin",
    });

    await updateMemoryProviderDefaultScope(deps, actor, "shared");

    expect(update).toHaveBeenCalled();
  });

  it("updates only the generic scope setting and retains the provider secret", async () => {
    const { deps, update, secretCreate, secretDeleteMany } = makeDeps({
      existing: { id: "cfg-1", secretId: "secret-existing" },
    });

    const result = await updateMemoryProviderDefaultScope(deps, actor, "shared");

    expect(update).toHaveBeenCalledWith({
      where: { id: "cfg-1" },
      data: { defaultMemoryScope: "shared" },
    });
    expect(secretCreate).not.toHaveBeenCalled();
    expect(secretDeleteMany).not.toHaveBeenCalled();
    expect(result.defaultMemoryScope).toBe("shared");
  });

  it("rejects non-owners without updating provider configuration", async () => {
    const { deps, update } = makeDeps({
      existing: { id: "cfg-1", secretId: "secret-existing" },
      spaceOwner: false,
    });

    await expect(updateMemoryProviderDefaultScope(deps, actor, "shared")).rejects.toThrow();
    expect(update).not.toHaveBeenCalled();
  });
});

describe("updateNeo4jExtraction", () => {
  it("patches extract and chunk size without touching the secret", async () => {
    const { deps, update, findUnique, secretCreate, secretDeleteMany } = makeDeps({
      existing: { id: "cfg-1", secretId: "secret-existing" },
      updateResult: {
        provider: "neo4j",
        settings: { uri: "bolt://localhost:7687", extract: "false" },
        defaultMemoryScope: "isolated",
        updatedAt: new Date("2026-08-20T00:00:00.000Z"),
      },
    });
    findUnique.mockResolvedValue({
      id: "cfg-1",
      secretId: "secret-existing",
      provider: "neo4j",
      settings: {
        uri: "bolt://localhost:7687",
        username: "neo4j",
        extract: "true",
        maxChunkChars: "800",
      },
    });

    const result = await updateNeo4jExtraction(deps, actor, { extract: false, maxChunkChars: " " });

    expect(update).toHaveBeenCalledWith({
      where: { id: "cfg-1" },
      data: {
        settings: { uri: "bolt://localhost:7687", username: "neo4j", extract: "false" },
      },
    });
    expect(secretCreate).not.toHaveBeenCalled();
    expect(secretDeleteMany).not.toHaveBeenCalled();
    expect(result.settings.extract).toBe("false");
  });

  it("rejects a chunk size outside the allowed range and a non-Neo4j provider", async () => {
    const neo4j = makeDeps({ existing: { id: "cfg-1", secretId: "secret-existing" } });
    neo4j.findUnique.mockResolvedValue({
      id: "cfg-1",
      secretId: "secret-existing",
      provider: "neo4j",
      settings: { uri: "bolt://localhost:7687" },
    });
    await expect(
      updateNeo4jExtraction(neo4j.deps, actor, { extract: true, maxChunkChars: "10" }),
    ).rejects.toThrow(/maxChunkChars/);
    expect(neo4j.update).not.toHaveBeenCalled();

    const other = makeDeps({ existing: { id: "cfg-1", secretId: "secret-existing" } });
    other.findUnique.mockResolvedValue({
      id: "cfg-1",
      secretId: "secret-existing",
      provider: "supermemory",
      settings: { mode: "cloud" },
    });
    await expect(updateNeo4jExtraction(other.deps, actor, { extract: false })).rejects.toThrow();
    expect(other.update).not.toHaveBeenCalled();
  });

  it("stores and clears the extraction model without touching the secret", async () => {
    const stored = {
      uri: "bolt://localhost:7687",
      username: "neo4j",
      extract: "true",
    };
    const { deps, update, findUnique, secretCreate } = makeDeps({
      existing: { id: "cfg-1", secretId: "secret-existing" },
    });
    findUnique.mockResolvedValue({
      id: "cfg-1",
      secretId: "secret-existing",
      provider: "neo4j",
      settings: stored,
    });

    await updateNeo4jExtraction(deps, actor, {
      extract: true,
      extractModelProvider: " synthetic ",
      extractModelId: " model-a ",
    });
    expect(update).toHaveBeenCalledWith({
      where: { id: "cfg-1" },
      data: {
        settings: {
          ...stored,
          extractModelProvider: "synthetic",
          extractModelId: "model-a",
        },
      },
    });
    expect(secretCreate).not.toHaveBeenCalled();

    findUnique.mockResolvedValue({
      id: "cfg-1",
      secretId: "secret-existing",
      provider: "neo4j",
      settings: { ...stored, extractModelProvider: "synthetic", extractModelId: "model-a" },
    });
    await updateNeo4jExtraction(deps, actor, {
      extract: true,
      extractModelProvider: " ",
      extractModelId: "",
    });
    expect(update).toHaveBeenLastCalledWith({
      where: { id: "cfg-1" },
      data: { settings: stored },
    });
  });

  it("rejects one side of an extraction model and leaves an omitted model in place", async () => {
    const stored = {
      uri: "bolt://localhost:7687",
      username: "neo4j",
      extractModelProvider: "synthetic",
      extractModelId: "model-a",
    };
    const { deps, update, findUnique } = makeDeps({
      existing: { id: "cfg-1", secretId: "secret-existing" },
    });
    findUnique.mockResolvedValue({
      id: "cfg-1",
      secretId: "secret-existing",
      provider: "neo4j",
      settings: stored,
    });
    await expect(
      updateNeo4jExtraction(deps, actor, { extract: true, extractModelProvider: "synthetic" }),
    ).rejects.toThrow(/provider and a model/);
    expect(update).not.toHaveBeenCalled();

    await updateNeo4jExtraction(deps, actor, { extract: false });
    expect(update).toHaveBeenCalledWith({
      where: { id: "cfg-1" },
      data: { settings: { ...stored, extract: "false" } },
    });
  });

  it("stores consolidate model settings without changing the chat connection", async () => {
    const stored = {
      uri: "bolt://localhost:7687",
      username: "neo4j",
      extract: "true",
    };
    const { deps, update, findUnique, secretCreate } = makeDeps({
      existing: { id: "cfg-1", secretId: "secret-existing" },
    });
    findUnique.mockResolvedValue({
      id: "cfg-1",
      secretId: "secret-existing",
      provider: "neo4j",
      settings: stored,
    });
    await updateNeo4jExtraction(deps, actor, {
      extract: true,
      consolidateThinkingLevel: " minimal ",
      consolidateMaxOutputTokens: "2048",
      consolidateTimeoutMs: " 180000 ",
    });
    expect(secretCreate).not.toHaveBeenCalled();
    expect(update).toHaveBeenCalledWith({
      where: { id: "cfg-1" },
      data: {
        settings: {
          ...stored,
          consolidateThinkingLevel: "minimal",
          consolidateMaxOutputTokens: "2048",
          consolidateTimeoutMs: "180000",
        },
      },
    });
    await expect(
      updateNeo4jExtraction(deps, actor, { extract: true, consolidateMaxOutputTokens: "4" }),
    ).rejects.toThrow(/consolidateMaxOutputTokens/);
    await expect(
      updateNeo4jExtraction(deps, actor, { extract: true, consolidateTimeoutMs: "1000" }),
    ).rejects.toThrow(/consolidateTimeoutMs/);
  });

  it("stores dream mode without dropping extraction settings, and leaves it when omitted", async () => {
    const stored = {
      uri: "bolt://localhost:7687",
      username: "neo4j",
      extract: "true",
      dream: "false",
    };
    const { deps, update, findUnique } = makeDeps({
      existing: { id: "cfg-1", secretId: "secret-existing" },
    });
    findUnique.mockResolvedValue({
      id: "cfg-1",
      secretId: "secret-existing",
      provider: "neo4j",
      settings: stored,
    });

    await updateNeo4jExtraction(deps, actor, { extract: true, dream: true });
    expect(update).toHaveBeenCalledWith({
      where: { id: "cfg-1" },
      data: { settings: { ...stored, dream: "true" } },
    });

    findUnique.mockResolvedValue({
      id: "cfg-1",
      secretId: "secret-existing",
      provider: "neo4j",
      settings: { ...stored, dream: "true" },
    });
    await updateNeo4jExtraction(deps, actor, { extract: false });
    expect(update).toHaveBeenLastCalledWith({
      where: { id: "cfg-1" },
      data: { settings: { ...stored, dream: "true", extract: "false" } },
    });

    findUnique.mockResolvedValue({
      id: "cfg-1",
      secretId: "secret-existing",
      provider: "neo4j",
      settings: { ...stored, dream: "true", extract: "false" },
    });
    await updateNeo4jExtraction(deps, actor, {
      extract: false,
      consolidateThinkingLevel: "minimal",
      consolidateMaxOutputTokens: "2048",
    });
    expect(update).toHaveBeenLastCalledWith({
      where: { id: "cfg-1" },
      data: {
        settings: {
          ...stored,
          dream: "true",
          extract: "false",
          consolidateThinkingLevel: "minimal",
          consolidateMaxOutputTokens: "2048",
        },
      },
    });
  });
});
