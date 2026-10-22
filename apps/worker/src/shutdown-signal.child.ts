import { installWorkerShutdownSignals } from "./shutdown-signals.js";

const holdMs = Number(process.env.HOLD_MS ?? "400");
const keepAlive = setInterval(() => undefined, 60_000);
const log = (line: string) => {
  process.stdout.write(`${line}\n`);
};
log("ready");
installWorkerShutdownSignals(async () => {
  clearInterval(keepAlive);
  log("stopping");
  await new Promise((resolve) => setTimeout(resolve, holdMs));
  log("stopped");
});
