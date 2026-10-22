import { AsyncLocalStorage } from "node:async_hooks";

const current = new AsyncLocalStorage<AbortSignal>();

/**
 * Graphile aborts this signal after `gracefulShutdownAbortTimeout`. Model calls
 * started inside `run` must observe it, or a redeploy waits out the whole call
 * and the process is killed with the queue still locked.
 */
export function runWithJobAbort<T>(signal: AbortSignal, run: () => Promise<T>): Promise<T> {
  return current.run(signal, run);
}

export function currentJobAbortSignal(): AbortSignal | undefined {
  return current.getStore();
}

export function combineJobAbort(signal: AbortSignal): AbortSignal {
  const job = current.getStore();
  return job ? AbortSignal.any([signal, job]) : signal;
}

/** The worker is exiting. Callers must not treat this as a model timeout or a permanent failure. */
export class JobAbortedError extends Error {
  constructor() {
    super("background job aborted during worker shutdown");
    this.name = "JobAbortedError";
  }
}

export function throwIfJobAborted(): void {
  if (current.getStore()?.aborted) throw new JobAbortedError();
}
