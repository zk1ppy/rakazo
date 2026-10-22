import { createLogger, createTestSink, installLogger } from "@rakazo/logging";
import type { Pool } from "pg";
import { afterEach, describe, expect, it } from "vitest";
import {
  DEFAULT_WORKER_SHUTDOWN_GRACE_MS,
  readStaleLockAfterMs,
  readWorkerShutdownGraceMs,
  releaseOwnWorkerLocks,
  unlockStaleWorkerLocks,
} from "./worker-locks.js";

afterEach(() => {
  installLogger(createLogger({ service: "rakazo-worker", level: "off", sinks: [] }));
});

describe("worker lock settings", () => {
  it("keeps a shutdown grace default and disables stale unlock unless asked", () => {
    expect(readWorkerShutdownGraceMs(undefined)).toBe(DEFAULT_WORKER_SHUTDOWN_GRACE_MS);
    expect(readWorkerShutdownGraceMs("")).toBe(DEFAULT_WORKER_SHUTDOWN_GRACE_MS);
    expect(readWorkerShutdownGraceMs("nope")).toBe(DEFAULT_WORKER_SHUTDOWN_GRACE_MS);
    expect(readWorkerShutdownGraceMs("20000")).toBe(20_000);
    expect(readWorkerShutdownGraceMs("0")).toBe(0);
    expect(readWorkerShutdownGraceMs("600001")).toBe(DEFAULT_WORKER_SHUTDOWN_GRACE_MS);

    expect(readStaleLockAfterMs(undefined)).toBeUndefined();
    expect(readStaleLockAfterMs("")).toBeUndefined();
    expect(readStaleLockAfterMs("0")).toBeUndefined();
    expect(readStaleLockAfterMs("-1")).toBeUndefined();
    expect(readStaleLockAfterMs("15min")).toBeUndefined();
    expect(readStaleLockAfterMs("900000")).toBe(900_000);
  });
});

describe("unlockStaleWorkerLocks", () => {
  it("logs each job and queue released for another worker", async () => {
    const sink = createTestSink();
    installLogger(createLogger({ service: "rakazo-worker", sinks: [sink] }));
    const queries: Array<{ sql: string; params: unknown[] }> = [];
    const pool = {
      query: async (sql: string, params?: unknown[]) => {
        queries.push({ sql, params: params ?? [] });
        return {
          rows: [
            { kind: "job", id: "15", queue: "memory-extract", worker_id: "pool-dead" },
            { kind: "queue", id: "4", queue: "memory-extract", worker_id: "pool-dead" },
          ],
        };
      },
    } as unknown as Pool;

    await expect(unlockStaleWorkerLocks(pool, 900_000)).resolves.toBe(2);
    expect(queries[0]?.params).toEqual([900_000, null]);
    expect(queries[0]?.sql).toContain("locked_by is distinct from $2::text");
    expect(queries[0]?.sql).toContain("_private_job_queues");
    const warnings = sink.events.filter((event) => event.level === "warn");
    expect(warnings).toEqual([
      expect.objectContaining({
        message: "released stale graphile lock",
        "job.id": "15",
        "job.queue": "memory-extract",
        "graphile.worker_id": "pool-dead",
      }),
      expect.objectContaining({
        message: "released stale graphile lock",
        "graphile.queue_id": "4",
        "job.queue": "memory-extract",
        "graphile.worker_id": "pool-dead",
      }),
    ]);
  });

  it("does not query when the threshold is disabled", async () => {
    const pool = { query: () => Promise.reject(new Error("should not query")) } as unknown as Pool;
    await expect(unlockStaleWorkerLocks(pool, 0)).resolves.toBe(0);
  });
});

describe("releaseOwnWorkerLocks", () => {
  it("refunds an interrupted attempt and force-unlocks its own pool", async () => {
    const sink = createTestSink();
    installLogger(createLogger({ service: "rakazo-worker", sinks: [sink] }));
    const queries: Array<{ sql: string; params: unknown[] }> = [];
    const query = async (sql: string, params?: unknown[]) => {
      queries.push({ sql, params: params ?? [] });
      if (sql.includes("force_unlock_workers") || sql === "begin" || sql === "commit") {
        return { rows: [] };
      }
      return { rows: [{ id: "42" }] };
    };
    const pool = {
      connect: async () => ({ query, release: () => undefined }),
    } as unknown as Pool;

    await releaseOwnWorkerLocks(pool, ["pool-self", "pool-self"], ["42"]);
    const refund = queries.find((entry) => entry.sql.includes("greatest(jobs.attempts - 1, 0)"));
    expect(refund?.sql).toContain("jobs.id = any($2::bigint[])");
    expect(refund?.params).toEqual([["pool-self"], ["42"]]);
    const unlock = queries.find((entry) => entry.sql.includes("force_unlock_workers"));
    expect(unlock?.params).toEqual([["pool-self"]]);
    expect(queries.map((entry) => entry.sql)).toEqual([
      "begin",
      expect.stringContaining("greatest(jobs.attempts - 1, 0)"),
      expect.stringContaining("force_unlock_workers"),
      "commit",
    ]);
    expect(sink.events.map((event) => event.message)).toEqual([
      "released graphile lock on shutdown",
      "force-unlocked graphile workers",
    ]);
  });

  it("unlocks its pool without refunding a job that was not interrupted", async () => {
    const queries: string[] = [];
    const query = async (sql: string) => {
      queries.push(sql);
      return { rows: [] };
    };
    const pool = {
      connect: async () => ({ query, release: () => undefined }),
    } as unknown as Pool;
    await releaseOwnWorkerLocks(pool, ["pool-self"], []);
    expect(queries.some((sql) => sql.includes("attempts - 1"))).toBe(false);
    expect(queries.some((sql) => sql.includes("force_unlock_workers"))).toBe(true);
  });

  it("does nothing without a pool id or an interrupted job", async () => {
    const pool = { query: () => Promise.reject(new Error("should not query")) } as unknown as Pool;
    await releaseOwnWorkerLocks(pool, [], []);
  });
});
