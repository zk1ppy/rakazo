import { describe, expect, it } from "vitest";
import {
  combineJobAbort,
  currentJobAbortSignal,
  JobAbortedError,
  runWithJobAbort,
  throwIfJobAborted,
} from "./job-abort.js";

describe("job abort signal", () => {
  it("is visible only inside the job and aborts model calls that combine it", async () => {
    const controller = new AbortController();
    expect(currentJobAbortSignal()).toBeUndefined();
    await runWithJobAbort(controller.signal, async () => {
      expect(currentJobAbortSignal()).toBe(controller.signal);
      const combined = combineJobAbort(AbortSignal.timeout(60_000));
      expect(combined.aborted).toBe(false);
      controller.abort();
      expect(combined.aborted).toBe(true);
      expect(() => throwIfJobAborted()).toThrow(JobAbortedError);
    });
    expect(currentJobAbortSignal()).toBeUndefined();
    expect(combineJobAbort(AbortSignal.timeout(60_000)).aborted).toBe(false);
  });
});
