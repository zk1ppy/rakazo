import { describe, expect, it, vi } from "vitest";
import {
  dispatchBackgroundJob,
  HISTORY_COMPACT_MAX_ATTEMPTS,
  historyCompactJob,
  historyCompactJobKey,
  MEMORY_CONSOLIDATE_MAX_ATTEMPTS,
  MEMORY_EXTRACT_MAX_ATTEMPTS,
  MEMORY_EXTRACT_QUEUE_NAME,
  memoryConsolidateJob,
  memoryConsolidateJobKey,
  memoryExtractJob,
  memoryExtractJobKey,
  messagingDeliverJob,
  parseBackgroundJob,
  routineRetryJob,
} from "./background-jobs.js";
import type { BackgroundJobHandlers } from "./types.js";

function handlers(): BackgroundJobHandlers {
  return {
    "run.continue": vi.fn(async () => undefined),
    "routine.wakeup": vi.fn(async () => undefined),
    "routine.retry": vi.fn(async () => undefined),
    "computer.update": vi.fn(async () => undefined),
    "computer.sleep": vi.fn(async () => undefined),
    "computer.control-expire": vi.fn(async () => undefined),
    "skill.teaching-expire": vi.fn(async () => undefined),
    "call.expire": vi.fn(async () => undefined),
    "history.compact": vi.fn(async () => undefined),
    "messaging.deliver": vi.fn(async () => undefined),
    "messaging.credential-cleanup": vi.fn(async () => undefined),
    "cloud_agent.poll": vi.fn(async () => undefined),
    "memory.extract": vi.fn(async () => undefined),
    "memory.consolidate": vi.fn(async () => undefined),
    "memory.dream": vi.fn(async () => undefined),
    "push.receipt": vi.fn(async () => undefined),
  };
}

describe("background job contracts", () => {
  it("accepts an empty dream sweep, drops the cron marker, and rejects a payload", () => {
    expect(parseBackgroundJob("memory.dream", {})).toEqual({
      name: "memory.dream",
      payload: {},
    });
    expect(
      parseBackgroundJob("memory.dream", {
        _cron: { ts: "2026-10-20T03:00:00.000Z", backfilled: false },
      }),
    ).toEqual({ name: "memory.dream", payload: {} });
    expect(() => parseBackgroundJob("memory.dream", { spaceId: "space-1" })).toThrow();
  });

  it("validates and dispatches messaging.deliver", async () => {
    const target = handlers();
    await dispatchBackgroundJob(target, "messaging.deliver", { runId: "run-1" });
    expect(target["messaging.deliver"]).toHaveBeenCalledWith({ runId: "run-1" });
    expect(messagingDeliverJob("run-1")).toEqual({
      name: "messaging.deliver",
      payload: { runId: "run-1" },
      replaceKey: "messaging.deliver:run-1",
    });
    expect(messagingDeliverJob()).toEqual({
      name: "messaging.deliver",
      payload: {},
      replaceKey: "messaging.deliver:drain",
    });
  });

  it("validates and dispatches retired messaging credential cleanup", async () => {
    const target = handlers();
    const payload = { installationId: "install-a", ref: "encrypted-reference" };
    await dispatchBackgroundJob(target, "messaging.credential-cleanup", payload);
    expect(target["messaging.credential-cleanup"]).toHaveBeenCalledWith(payload);
    expect(() =>
      parseBackgroundJob("messaging.credential-cleanup", { ref: "encrypted-reference" }),
    ).toThrow();
  });

  it("validates and dispatches a typed job", async () => {
    const target = handlers();
    await dispatchBackgroundJob(target, "routine.wakeup", {
      routineId: "routine-1",
      scheduledFor: "2026-08-15T12:00:00.000Z",
    });
    expect(target["routine.wakeup"]).toHaveBeenCalledWith({
      routineId: "routine-1",
      scheduledFor: "2026-08-15T12:00:00.000Z",
    });
  });

  it("validates and dispatches a routine retry keyed to the failed run", async () => {
    const target = handlers();
    await dispatchBackgroundJob(target, "routine.retry", { runId: "run-1" });
    expect(target["routine.retry"]).toHaveBeenCalledWith({ runId: "run-1" });
    expect(() => parseBackgroundJob("routine.retry", { runId: "" })).toThrow();
    const availableAt = new Date("2026-08-15T12:12:00.000Z");
    expect(routineRetryJob("run-1", availableAt)).toEqual({
      name: "routine.retry",
      payload: { runId: "run-1" },
      availableAt,
      replaceKey: "routine.retry:run-1",
    });
  });

  it("rejects unknown names and malformed deliveries", () => {
    expect(() => parseBackgroundJob("unknown", {})).toThrow("Unknown background job");
    expect(() =>
      parseBackgroundJob("routine.wakeup", {
        routineId: "routine-1",
        scheduledFor: "not-a-date",
      }),
    ).toThrow();
    expect(() => parseBackgroundJob("run.continue", { runId: "" })).toThrow();
    expect(() =>
      parseBackgroundJob("computer.control-expire", {
        computerId: "computer-1",
        leaseId: "",
      }),
    ).toThrow();
  });
});

describe("memoryExtractJob", () => {
  const payload = {
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

  it("uses one global serial queue and a single attempt", () => {
    expect(memoryExtractJob(payload)).toEqual({
      name: "memory.extract",
      payload,
      replaceKey: memoryExtractJobKey("fact-1", 0),
      queueName: MEMORY_EXTRACT_QUEUE_NAME,
      maxAttempts: MEMORY_EXTRACT_MAX_ATTEMPTS,
    });
    expect(MEMORY_EXTRACT_QUEUE_NAME).toBe("memory-extract");
    expect(MEMORY_EXTRACT_MAX_ATTEMPTS).toBe(1);
  });

  it("alternates the replace key so a successful attempt can reschedule", () => {
    expect(memoryExtractJobKey("fact-1", 0)).toBe("memory.extract:fact-1:0");
    expect(memoryExtractJobKey("fact-1", 1)).toBe("memory.extract:fact-1:1");
    expect(memoryExtractJobKey("fact-1", 2)).toBe(memoryExtractJobKey("fact-1", 0));
    const availableAt = new Date("2030-01-01T00:00:15.000Z");
    expect(
      memoryExtractJob({ ...payload, scheduleGeneration: 1, attempt: 1 }, availableAt).availableAt,
    ).toBe(availableAt);
  });
});

describe("memoryConsolidateJob", () => {
  const payload = {
    spaceId: "space-1",
    userId: "user-1",
    attempt: 0,
    scheduleGeneration: 0,
  };

  it("shares the memory model queue and alternates its replace key", () => {
    expect(memoryConsolidateJob(payload)).toEqual({
      name: "memory.consolidate",
      payload,
      replaceKey: memoryConsolidateJobKey("space-1", undefined, 0),
      queueName: MEMORY_EXTRACT_QUEUE_NAME,
      maxAttempts: MEMORY_CONSOLIDATE_MAX_ATTEMPTS,
    });
    expect(memoryConsolidateJobKey("space-1", "shared", 0)).toBe(
      "memory.consolidate:space-1:shared:0",
    );
    expect(memoryConsolidateJobKey("space-1", undefined, 1)).toBe("memory.consolidate:space-1:*:1");
    expect(memoryConsolidateJobKey("space-1", undefined, 2)).toBe(
      memoryConsolidateJobKey("space-1", undefined, 0),
    );
    const availableAt = new Date("2030-01-01T00:00:15.000Z");
    expect(
      memoryConsolidateJob({ ...payload, scheduleGeneration: 1, attempt: 1 }, availableAt)
        .availableAt,
    ).toBe(availableAt);
  });
});

describe("historyCompactJob", () => {
  it("builds a job with a replace key scoped to the thread", () => {
    expect(historyCompactJob("thread-1")).toEqual({
      name: "history.compact",
      payload: { threadId: "thread-1" },
      replaceKey: historyCompactJobKey("thread-1"),
      maxAttempts: HISTORY_COMPACT_MAX_ATTEMPTS,
    });
  });

  it("caps attempts below the queue default so a stuck thread cannot storm", () => {
    expect(HISTORY_COMPACT_MAX_ATTEMPTS).toBeLessThan(25);
  });

  it("keys different threads differently", () => {
    expect(historyCompactJobKey("thread-1")).not.toBe(historyCompactJobKey("thread-2"));
  });
});
