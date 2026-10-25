import type {
  AgentHomeStore,
  AgentRuntime,
  ArtifactStore,
  BackgroundJob,
  JobPublisher,
  MessagingSurface,
  SandboxProvider,
} from "@rakazo/adapter-kit";
import type { PrismaClient, ThreadEvents } from "@rakazo/db";
import { currentDbContext } from "@rakazo/db";
import { createLogger, createTestSink, getLogger, installLogger } from "@rakazo/logging";
import { describe, expect, it, vi } from "vitest";
import { createBackgroundJobHandlers } from "./background-job-handlers.js";
import { createRunExecutor } from "./executor.js";
import { compactHistory } from "./history-compaction.js";
import { MemoryConsolidatePermanentError } from "./memory-consolidate.js";
import { dreamConsolidateJobKey } from "./memory-dream.js";
import { deliverMessagingOutbound, mirrorMessagingOutbound } from "./messaging-delivery.js";
import { Neo4jMemoryProvider } from "./neo4j-memory-provider.js";
import type { EncryptedSecretStore } from "./secrets.js";

vi.mock("./history-compaction.js", () => ({ compactHistory: vi.fn(async () => undefined) }));
vi.mock("./messaging-delivery.js", () => ({
  deliverMessagingOutbound: vi.fn(async () => undefined),
  mirrorMessagingOutbound: vi.fn(async () => undefined),
}));

const owner = { spaceId: "space-1", userId: "user-1" };
const owned = () => ({ findUnique: vi.fn(async () => owner) });

describe("createBackgroundJobHandlers", () => {
  it("delivers directly when shutdown rejects a completed run's mirror job", async () => {
    const enqueueError = new Error("Background job publisher is closing");
    const jobs = {
      enqueue: vi.fn(async () => {
        throw enqueueError;
      }),
    } as unknown as JobPublisher;
    const sink = createTestSink();
    installLogger(createLogger({ service: "rakazo-worker", sinks: [sink] }));
    const handlers = createBackgroundJobHandlers({
      executor: {
        continueRun: vi.fn(async () => undefined),
      } as unknown as ReturnType<typeof createRunExecutor>,
      prisma: { run: owned() } as unknown as PrismaClient,
      sandbox: {} as unknown as SandboxProvider,
      home: {} as unknown as AgentHomeStore,
      artifacts: {} as unknown as ArtifactStore,
      jobs,
      events: {} as unknown as ThreadEvents,
      workerId: "worker-1",
      runtime: {} as unknown as AgentRuntime,
      secretStore: {} as unknown as EncryptedSecretStore,
      memoryProviders: { resolve: vi.fn(async () => null) },
      messaging: {} as unknown as MessagingSurface,
    });

    await handlers["run.continue"]({ runId: "run-1" });

    expect(mirrorMessagingOutbound).toHaveBeenCalledWith(
      expect.objectContaining({ prisma: expect.anything(), messaging: expect.anything(), jobs }),
      "run-1",
    );
    expect(deliverMessagingOutbound).toHaveBeenCalledWith(
      expect.objectContaining({ prisma: expect.anything(), messaging: expect.anything(), jobs }),
      {},
      expect.objectContaining({ operationId: "messaging.deliver:drain" }),
    );
    expect(sink.events.some((event) => event.message === "messaging.deliver enqueue error")).toBe(
      true,
    );
    installLogger(createLogger({ service: "rakazo-worker", level: "off", sinks: [] }));
  });

  it("compacts the requested thread with the runtime, job publisher, and model key it was given", async () => {
    const prisma = { thread: owned() } as unknown as PrismaClient;
    const runtime = {} as unknown as AgentRuntime;
    const jobs = {} as unknown as JobPublisher;
    const secretStore = {} as unknown as EncryptedSecretStore;
    const memoryProviders = { resolve: vi.fn(async () => null) };
    const resolveModel = vi.fn();
    const handlers = createBackgroundJobHandlers({
      executor: { resolveModel, contextStrategy: "current" } as unknown as ReturnType<
        typeof createRunExecutor
      >,
      prisma,
      sandbox: {} as unknown as SandboxProvider,
      home: {} as unknown as AgentHomeStore,
      artifacts: {} as unknown as ArtifactStore,
      jobs,
      events: {} as unknown as ThreadEvents,
      workerId: "worker-1",
      runtime,
      secretStore,
      memoryProviders,
      deploymentModelKey: "openrouter-key",
      deploymentModelConfigured: true,
    });

    await handlers["history.compact"]({ threadId: "thread-1" });

    expect(compactHistory).toHaveBeenCalledWith(
      {
        prisma,
        runtime,
        jobs,
        memoryProviders,
        deploymentModelKey: "openrouter-key",
        deploymentModelConfigured: true,
        resolveModel,
      },
      "thread-1",
    );
  });

  it.each([undefined, "retrieval", "snapshots", "cache-aware"] as const)(
    "does not spend on a queued legacy compaction after switching to %s",
    async (contextStrategy) => {
      vi.mocked(compactHistory).mockClear();
      const runtime = { run: vi.fn() } as unknown as AgentRuntime;
      const handlers = createBackgroundJobHandlers({
        executor:
          contextStrategy === undefined
            ? createRunExecutor({ prisma: {} as PrismaClient } as Parameters<
                typeof createRunExecutor
              >[0])
            : ({ contextStrategy } as ReturnType<typeof createRunExecutor>),
        prisma: { thread: owned() } as unknown as PrismaClient,
        sandbox: {} as unknown as SandboxProvider,
        home: {} as unknown as AgentHomeStore,
        artifacts: {} as unknown as ArtifactStore,
        jobs: {} as unknown as JobPublisher,
        events: {} as unknown as ThreadEvents,
        workerId: "worker-fixture",
        runtime,
        secretStore: {} as unknown as EncryptedSecretStore,
        memoryProviders: { resolve: vi.fn(async () => null) },
      });

      await handlers["history.compact"]({ threadId: "thread-fixture" });

      expect(compactHistory).not.toHaveBeenCalled();
      expect(runtime.run).not.toHaveBeenCalled();
    },
  );

  it("runs a job as the tenant of the row it acts for, and skips one whose row is gone", async () => {
    const seen: unknown[] = [];
    const continueRun = vi.fn(async () => {
      seen.push(currentDbContext());
    });
    const lookups = { findUnique: vi.fn(async () => owner) };
    const handlers = createBackgroundJobHandlers({
      executor: { continueRun } as unknown as ReturnType<typeof createRunExecutor>,
      prisma: { run: lookups } as unknown as PrismaClient,
      sandbox: {} as unknown as SandboxProvider,
      home: {} as unknown as AgentHomeStore,
      artifacts: {} as unknown as ArtifactStore,
      jobs: {} as unknown as JobPublisher,
      events: {} as unknown as ThreadEvents,
      workerId: "worker-1",
      runtime: {} as unknown as AgentRuntime,
      secretStore: {} as unknown as EncryptedSecretStore,
      memoryProviders: { resolve: vi.fn(async () => null) },
    });

    await handlers["run.continue"]({ runId: "run-1" });
    expect(seen).toEqual([{ kind: "tenant", ...owner }]);
    expect(lookups.findUnique).toHaveBeenCalledWith({
      where: { id: "run-1" },
      select: { spaceId: true, userId: true },
    });

    const onRunStarted = vi.fn();
    await handlers["run.continue"]({ runId: "run-1" }, { onRunStarted });
    expect(continueRun).toHaveBeenLastCalledWith("run-1", "worker-1", onRunStarted);

    lookups.findUnique.mockResolvedValueOnce(null as never);
    await handlers["run.continue"]({ runId: "run-gone" });
    expect(continueRun).toHaveBeenCalledTimes(2);
  });

  it("does not call the model when the space has no Neo4j provider", async () => {
    const runtime = { run: vi.fn() } as unknown as AgentRuntime;
    const handlers = createBackgroundJobHandlers({
      executor: { resolveModel: vi.fn() } as unknown as ReturnType<typeof createRunExecutor>,
      prisma: {} as unknown as PrismaClient,
      sandbox: {} as unknown as SandboxProvider,
      home: {} as unknown as AgentHomeStore,
      jobs: {} as unknown as JobPublisher,
      events: {} as unknown as ThreadEvents,
      workerId: "worker-1",
      runtime,
      secretStore: {} as unknown as EncryptedSecretStore,
      memoryProviders: { resolve: vi.fn(async () => null) },
    });

    const sink = createTestSink();
    const previousLogger = getLogger();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    try {
      await handlers["memory.extract"]({
        spaceId: "space-1",
        userId: "user-1",
        botId: "bot-1",
        factId: "fact-1",
        ns: "shared",
        maxChunkChars: 4_000,
        chunkIndex: 0,
        attempt: 0,
        scheduleGeneration: 0,
      });
    } finally {
      installLogger(previousLogger);
    }

    expect(runtime.run).not.toHaveBeenCalled();
    expect(sink.events.map((event) => event.message)).toEqual([
      "memory.extract skipped: no memory provider",
    ]);
  });

  it("does not call the model when consolidate has no Neo4j provider", async () => {
    const runtime = { run: vi.fn() } as unknown as AgentRuntime;
    const handlers = createBackgroundJobHandlers({
      executor: { resolveModel: vi.fn() } as unknown as ReturnType<typeof createRunExecutor>,
      prisma: {} as unknown as PrismaClient,
      sandbox: {} as unknown as SandboxProvider,
      home: {} as unknown as AgentHomeStore,
      jobs: {} as unknown as JobPublisher,
      events: {} as unknown as ThreadEvents,
      workerId: "worker-1",
      runtime,
      secretStore: {} as unknown as EncryptedSecretStore,
      memoryProviders: { resolve: vi.fn(async () => null) },
    });

    const sink = createTestSink();
    const previousLogger = getLogger();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    try {
      await handlers["memory.consolidate"]({
        spaceId: "space-1",
        userId: "user-1",
        attempt: 0,
        scheduleGeneration: 0,
      });
    } finally {
      installLogger(previousLogger);
    }

    expect(runtime.run).not.toHaveBeenCalled();
    expect(sink.events.map((event) => event.message)).toEqual([
      "memory.consolidate skipped: no memory provider",
    ]);
  });

  it("schedules one dream consolidate after an extraction write and skips it when dream mode is off", async () => {
    class WritingNeo4j extends Neo4jMemoryProvider {
      override async readFactText() {
        return "Ada Lovelace founded the Analytical Society in London.";
      }

      override async applyExtraction() {
        return { eventDegreeCapped: 0, toolDegreeCapped: 0 };
      }

      override async markExtracted() {}
    }
    const connection = {
      uri: "bolt://127.0.0.1:7687",
      database: "neo4j",
      username: "neo4j",
      password: "secret",
      endpointTrust: "private" as const,
    };
    const openDriver = () => {
      throw new Error("driver");
    };
    const run = vi.fn(async function* () {
      yield {
        type: "done" as const,
        text: '{"entities":[{"type":"Person","name":"Ada Lovelace"}],"relations":[]}',
      };
    });
    const runtime = {
      describe: () => ({ id: "test", capabilities: { scripted: false } }),
      run,
    } as unknown as AgentRuntime;
    const enqueue = vi.fn(async (_job: BackgroundJob) => undefined);
    const jobs = { enqueue, cancel: vi.fn(), close: vi.fn() } as unknown as JobPublisher;
    const handlersFor = (dream: boolean) =>
      createBackgroundJobHandlers({
        executor: {
          resolveModel: vi.fn(async () => ({ provider: "openai", id: "gpt-test", apiKey: "k" })),
        } as unknown as ReturnType<typeof createRunExecutor>,
        prisma: {} as unknown as PrismaClient,
        sandbox: {} as unknown as SandboxProvider,
        home: {} as unknown as AgentHomeStore,
        jobs,
        events: {} as unknown as ThreadEvents,
        workerId: "worker-1",
        runtime,
        secretStore: {} as unknown as EncryptedSecretStore,
        memoryProviders: {
          resolve: vi.fn(async () => ({
            provider: new WritingNeo4j(connection, openDriver, { dream }),
            defaultScope: "isolated" as const,
          })),
        },
      });
    const extractPayload = {
      spaceId: "space-1",
      userId: "user-1",
      botId: "bot-1",
      factId: "fact-1",
      ns: "shared",
      maxChunkChars: 4_000,
      chunkIndex: 0,
      attempt: 0,
      scheduleGeneration: 0,
    };

    await handlersFor(false)["memory.extract"](extractPayload);
    expect(enqueue).not.toHaveBeenCalled();

    await handlersFor(true)["memory.extract"](extractPayload);
    expect(enqueue).toHaveBeenCalledTimes(1);
    const job = enqueue.mock.calls[0]?.[0];
    expect(job).toMatchObject({
      name: "memory.consolidate",
      replaceKey: dreamConsolidateJobKey("space-1"),
      queueName: "memory-extract",
      payload: {
        spaceId: "space-1",
        userId: "user-1",
        automatic: true,
        attempt: 0,
        scheduleGeneration: 0,
      },
    });
    expect(job?.availableAt).toBeInstanceOf(Date);
    expect(job).not.toHaveProperty("keepExisting");

    const sink = createTestSink();
    const previousLogger = getLogger();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    try {
      await handlersFor(false)["memory.consolidate"]({
        spaceId: "space-1",
        userId: "user-1",
        attempt: 0,
        scheduleGeneration: 0,
        automatic: true,
      });
    } finally {
      installLogger(previousLogger);
    }
    expect(run).toHaveBeenCalledTimes(2);
    expect(sink.events.map((event) => event.message)).toContain(
      "memory.consolidate skipped: dream mode is off",
    );
  });

  it("returns after a permanent consolidate failure instead of asking Graphile to retry", async () => {
    class PermanentNeo4j extends Neo4jMemoryProvider {
      override listConsolidationPage(): Promise<never> {
        throw new MemoryConsolidatePermanentError("invalid api key");
      }
    }
    const handlers = createBackgroundJobHandlers({
      executor: {} as unknown as ReturnType<typeof createRunExecutor>,
      prisma: {} as unknown as PrismaClient,
      sandbox: {} as unknown as SandboxProvider,
      home: {} as unknown as AgentHomeStore,
      jobs: {} as unknown as JobPublisher,
      events: {} as unknown as ThreadEvents,
      workerId: "worker-1",
      runtime: { run: vi.fn() } as unknown as AgentRuntime,
      secretStore: {} as unknown as EncryptedSecretStore,
      memoryProviders: {
        resolve: vi.fn(async () => ({
          provider: new PermanentNeo4j(
            {
              uri: "bolt://127.0.0.1:7687",
              database: "neo4j",
              username: "neo4j",
              password: "secret",
              endpointTrust: "private",
            },
            () => {
              throw new Error("driver");
            },
          ),
          defaultScope: "isolated" as const,
        })),
      },
    });
    const sink = createTestSink();
    const previousLogger = getLogger();
    installLogger(createLogger({ service: "test", sinks: [sink] }));
    try {
      await handlers["memory.consolidate"]({
        spaceId: "space-1",
        userId: "user-1",
        attempt: 0,
        scheduleGeneration: 0,
      });
    } finally {
      installLogger(previousLogger);
    }
    expect(sink.events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          level: "error",
          message: "memory.consolidate stopped",
          "job.outcome": "permanent",
        }),
      ]),
    );
  });

  it("rethrows an unexpected consolidate failure so a dropped connection can retry", async () => {
    class BrokenNeo4j extends Neo4jMemoryProvider {
      override listConsolidationPage(): Promise<never> {
        throw new Error("connection reset");
      }
    }
    const handlers = createBackgroundJobHandlers({
      executor: {} as unknown as ReturnType<typeof createRunExecutor>,
      prisma: {} as unknown as PrismaClient,
      sandbox: {} as unknown as SandboxProvider,
      home: {} as unknown as AgentHomeStore,
      jobs: {} as unknown as JobPublisher,
      events: {} as unknown as ThreadEvents,
      workerId: "worker-1",
      runtime: { run: vi.fn() } as unknown as AgentRuntime,
      secretStore: {} as unknown as EncryptedSecretStore,
      memoryProviders: {
        resolve: vi.fn(async () => ({
          provider: new BrokenNeo4j(
            {
              uri: "bolt://127.0.0.1:7687",
              database: "neo4j",
              username: "neo4j",
              password: "secret",
              endpointTrust: "private",
            },
            () => {
              throw new Error("driver");
            },
          ),
          defaultScope: "isolated" as const,
        })),
      },
    });
    await expect(
      handlers["memory.consolidate"]({
        spaceId: "space-1",
        userId: "user-1",
        attempt: 0,
        scheduleGeneration: 0,
      }),
    ).rejects.toThrow("connection reset");
  });

  it("enqueues nightly consolidation only for spaces with dream mode on", async () => {
    const enqueue = vi.fn(async (_job: BackgroundJob) => undefined);
    const handlers = createBackgroundJobHandlers({
      executor: {} as unknown as ReturnType<typeof createRunExecutor>,
      prisma: {
        spaceMemoryConfig: {
          findMany: vi.fn(async () => [
            {
              spaceId: "space-on",
              userId: "user-on",
              settings: {
                uri: "neo4j+s://db.example.test",
                username: "neo4j",
                dream: "true",
              },
              secret: { ciphertext: "cipher", id: "secret-on" },
            },
            {
              spaceId: "space-off",
              userId: "user-off",
              settings: {
                uri: "neo4j+s://db.example.test",
                username: "neo4j",
                dream: "false",
              },
              secret: { ciphertext: "cipher", id: "secret-off" },
            },
          ]),
        },
      } as unknown as PrismaClient,
      sandbox: {} as unknown as SandboxProvider,
      home: {} as unknown as AgentHomeStore,
      jobs: { enqueue, cancel: vi.fn(), close: vi.fn() } as unknown as JobPublisher,
      events: {} as unknown as ThreadEvents,
      workerId: "worker-1",
      runtime: { run: vi.fn() } as unknown as AgentRuntime,
      secretStore: {
        load: vi.fn(async () => JSON.stringify({ password: "secret" })),
      } as unknown as EncryptedSecretStore,
      memoryProviders: { resolve: vi.fn(async () => null) },
    });

    await handlers["memory.dream"]({});
    expect(enqueue).toHaveBeenCalledTimes(1);
    expect(enqueue.mock.calls[0]?.[0]).toMatchObject({
      replaceKey: dreamConsolidateJobKey("space-on"),
      keepExisting: true,
      payload: { spaceId: "space-on", automatic: true },
    });
  });

  it("resolves the deployment model when no user credential is configured", async () => {
    const prisma = {
      spaceModelPreference: { findFirst: vi.fn(async () => null) },
      spaceModelRoleChoice: { findMany: vi.fn(async () => []) },
      userModelCredential: { findFirst: vi.fn(async () => null) },
      deploymentSettings: { findUnique: vi.fn(async () => null) },
    } as unknown as PrismaClient;
    const executor = createRunExecutor({
      prisma,
      deploymentModelKey: "deployment-key",
      deploymentModelConfigured: true,
    } as Parameters<typeof createRunExecutor>[0]);

    await expect(
      executor.resolveModel({ userId: "user-1", spaceId: "workspace-1" }),
    ).resolves.toEqual({
      provider: "openrouter",
      id: "openai/gpt-6-luna",
      apiKey: "deployment-key",
      baseUrl: undefined,
      thinkingLevel: null,
      oauth: undefined,
    });
  });

  it("preserves a configured local model when resolving background compaction", async () => {
    const prisma = {
      spaceModelPreference: { findFirst: vi.fn(async () => null) },
      spaceModelRoleChoice: { findMany: vi.fn(async () => []) },
      userModelCredential: { findFirst: vi.fn(async () => null) },
      deploymentSettings: {
        findUnique: vi.fn(async () => ({
          defaultModelProvider: "local",
          defaultModelId: "qwen3:4b",
        })),
      },
    } as unknown as PrismaClient;
    const executor = createRunExecutor({
      prisma,
    } as Parameters<typeof createRunExecutor>[0]);

    await expect(
      executor.resolveModel({ userId: "user-1", spaceId: "workspace-1" }),
    ).resolves.toEqual({
      provider: "local",
      id: "qwen3:4b",
      apiKey: undefined,
      baseUrl: undefined,
      thinkingLevel: null,
      oauth: undefined,
    });
  });
});

it("retries retired messaging credentials without deleting a currently referenced credential", async () => {
  const remove = vi.fn(async () => undefined);
  const findUnique = vi.fn(async () => ({ ciphertext: "active-ref" }));
  const handlers = createBackgroundJobHandlers({
    prisma: { messagingInstallation: { findUnique } },
    secretStore: { delete: remove },
  } as unknown as Parameters<typeof createBackgroundJobHandlers>[0]);
  await handlers["messaging.credential-cleanup"]({
    installationId: "install-a",
    ref: "active-ref",
  });
  expect(remove).not.toHaveBeenCalled();
  await handlers["messaging.credential-cleanup"]({
    installationId: "install-a",
    ref: "retired-ref",
  });
  expect(remove).toHaveBeenCalledWith(
    "retired-ref",
    expect.objectContaining({ recordId: "install-a" }),
  );
  remove.mockRejectedValueOnce(new Error("secret store temporarily unavailable") as never);
  await expect(
    handlers["messaging.credential-cleanup"]({ installationId: "install-a", ref: "retired-ref" }),
  ).rejects.toThrow("temporarily unavailable");
});
