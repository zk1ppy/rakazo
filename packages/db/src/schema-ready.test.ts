import { describe, expect, it, vi } from "vitest";
import { isSchemaNotReady, waitForRuntimeSchema } from "./schema-ready.js";

function pgError(code: string, message: string): Error & { code: string } {
  return Object.assign(new Error(message), { code });
}

describe("isSchemaNotReady", () => {
  it("recognises a missing relation, a missing schema, and a grant that is not applied yet", () => {
    expect(isSchemaNotReady(pgError("42P01", 'relation "space_keys" does not exist'))).toBe(true);
    expect(isSchemaNotReady(pgError("3F000", 'schema "graphile_worker" does not exist'))).toBe(
      true,
    );
    expect(isSchemaNotReady(pgError("42501", "permission denied for schema graphile_worker"))).toBe(
      true,
    );
    expect(
      isSchemaNotReady(Object.assign(new Error("wrapped"), { cause: pgError("42P01", "no") })),
    ).toBe(true);
  });

  it("does not treat authentication or saturation as a migration race", () => {
    expect(isSchemaNotReady(pgError("28P01", "password authentication failed"))).toBe(false);
    expect(isSchemaNotReady(pgError("53300", "sorry, too many clients already"))).toBe(false);
    expect(isSchemaNotReady(new Error("socket hang up"))).toBe(false);
  });
});

describe("waitForRuntimeSchema", () => {
  it("returns once both probes succeed", async () => {
    const query = vi.fn(async (_sql: string) => undefined);
    await waitForRuntimeSchema({ query }, { timeoutMs: 1_000, delayMs: 100, maxDelayMs: 400 });
    expect(query.mock.calls.map((call) => call[0])).toEqual([
      "SELECT 1 FROM space_keys LIMIT 0",
      "SELECT 1 FROM graphile_worker._private_jobs LIMIT 0",
    ]);
  });

  it("waits through a missing table and a missing grant, then continues", async () => {
    const errors = [
      pgError("42P01", 'relation "space_keys" does not exist'),
      pgError("42501", "permission denied for schema graphile_worker"),
    ];
    const query = vi.fn(async (sql: string) => {
      if (sql.includes("space_keys") && errors.length > 0) throw errors.shift();
    });
    const waits: number[] = [];
    let clock = 0;
    const ready = vi.fn();
    await waitForRuntimeSchema(
      { query },
      {
        timeoutMs: 5_000,
        delayMs: 100,
        maxDelayMs: 150,
        now: () => clock,
        sleep: async (ms) => {
          waits.push(ms);
          clock += ms;
        },
        onReady: ready,
      },
    );
    expect(waits).toEqual([100, 150]);
    expect(ready).toHaveBeenCalledOnce();
  });

  it("stops at the timeout and keeps the database cause", async () => {
    const query = vi.fn(async () => {
      throw pgError("42P01", 'relation "space_keys" does not exist');
    });
    let clock = 0;
    const waiting = vi.fn();
    const error = await waitForRuntimeSchema(
      { query },
      {
        timeoutMs: 250,
        delayMs: 100,
        maxDelayMs: 100,
        now: () => clock,
        sleep: async (ms) => {
          clock += ms;
        },
        onWaiting: waiting,
      },
    ).then(
      () => {
        throw new Error("expected the wait to fail");
      },
      (caught: unknown) => caught,
    );
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(/not ready after 250ms/);
    expect((error as Error).message).toMatch(/space_keys/);
    expect((error as { cause?: Error }).cause?.message).toMatch(/space_keys/);
    expect(waiting).toHaveBeenCalled();
  });

  it("probes once when the timeout is 0", async () => {
    const sleep = vi.fn(async () => undefined);
    const query = vi.fn(async () => {
      throw pgError("42501", "permission denied for schema graphile_worker");
    });
    await expect(
      waitForRuntimeSchema({ query }, { timeoutMs: 0, delayMs: 100, maxDelayMs: 100, sleep }),
    ).rejects.toThrow(/after 0ms/);
    expect(sleep).not.toHaveBeenCalled();
    expect(query).toHaveBeenCalledOnce();
  });

  it("does not retry a connection failure", async () => {
    const sleep = vi.fn(async () => undefined);
    const query = vi.fn(async () => {
      throw pgError("53300", "sorry, too many clients already");
    });
    await expect(
      waitForRuntimeSchema({ query }, { timeoutMs: 5_000, delayMs: 100, maxDelayMs: 100, sleep }),
    ).rejects.toThrow(/too many clients/);
    expect(sleep).not.toHaveBeenCalled();
  });
});
