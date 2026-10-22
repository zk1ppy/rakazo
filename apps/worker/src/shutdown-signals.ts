/**
 * One shutdown for SIGTERM and SIGINT. A second signal is ignored so a process
 * supervisor that delivers it twice cannot exit before `stop` finishes.
 * The process exits only after `stop` settles. Graphile's own handlers are off,
 * so without this exit the listener would swallow SIGTERM and stay up.
 */
export function installWorkerShutdownSignals(stop: () => Promise<void>): void {
  let shutdown: Promise<void> | undefined;
  const exitAfterStop = (code: number) => {
    if (shutdown) return;
    shutdown = stop().finally(() => {
      process.exit(code);
    });
  };
  process.on("SIGTERM", () => exitAfterStop(0));
  process.on("SIGINT", () => exitAfterStop(0));
}
