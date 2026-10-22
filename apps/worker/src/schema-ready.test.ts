import { describe, expect, it } from "vitest";
import {
  DEFAULT_SCHEMA_READY_DELAY_MS,
  DEFAULT_SCHEMA_READY_MAX_DELAY_MS,
  DEFAULT_SCHEMA_READY_TIMEOUT_MS,
  readWorkerSchemaReady,
} from "./schema-ready.js";

describe("readWorkerSchemaReady", () => {
  it("keeps the built-in wait when the variables are unset or invalid", () => {
    expect(readWorkerSchemaReady({})).toEqual({
      timeoutMs: DEFAULT_SCHEMA_READY_TIMEOUT_MS,
      delayMs: DEFAULT_SCHEMA_READY_DELAY_MS,
      maxDelayMs: DEFAULT_SCHEMA_READY_MAX_DELAY_MS,
    });
    expect(
      readWorkerSchemaReady({
        WORKER_SCHEMA_READY_TIMEOUT_MS: "nope",
        WORKER_SCHEMA_READY_DELAY_MS: "0",
        WORKER_SCHEMA_READY_MAX_DELAY_MS: "999999",
      }),
    ).toEqual({
      timeoutMs: DEFAULT_SCHEMA_READY_TIMEOUT_MS,
      delayMs: DEFAULT_SCHEMA_READY_DELAY_MS,
      maxDelayMs: DEFAULT_SCHEMA_READY_MAX_DELAY_MS,
    });
    expect(DEFAULT_SCHEMA_READY_TIMEOUT_MS).toBe(120_000);
  });

  it("accepts a configured wait, including a single probe", () => {
    expect(
      readWorkerSchemaReady({
        WORKER_SCHEMA_READY_TIMEOUT_MS: "0",
        WORKER_SCHEMA_READY_DELAY_MS: "250",
        WORKER_SCHEMA_READY_MAX_DELAY_MS: "2000",
      }),
    ).toEqual({ timeoutMs: 0, delayMs: 250, maxDelayMs: 2_000 });
  });
});
