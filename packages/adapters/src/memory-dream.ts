import type { BackgroundJob, JobPublisher } from "@rakazo/adapter-kit";
import { memoryConsolidateJob } from "@rakazo/adapter-kit";
import type { CronItem } from "graphile-worker";

/**
 * Dream mode arms `memory.consolidate` for one Space.
 *
 * The switch is stored on that Space's Neo4j config (`settings.dream`) and is
 * off when the key is absent. The manual backfill command does not read it.
 *
 * A burst of extraction writes shares one job on the `memory-extract` queue.
 * Each write replaces that job and moves it {@link DREAM_DEBOUNCE_MS} past the
 * latest write, so the run starts after the burst goes quiet. Its key is
 * `memory.consolidate:<space>:*:dream`, not the generation key a scan uses to
 * continue, so the debounce cannot delete a follow-up or a manual enqueue.
 * Extraction holds the queue while it schedules, so that replace never lands
 * on a locked consolidate row.
 *
 * Graphile cron runs `memory.dream` at 03:00 UTC. The sweep enqueues the same
 * per-space job with `keepExisting`, which leaves a queued or locked run
 * alone. The shared queue still runs one memory model job at a time. The cron
 * item is parsed in code because a crontab line cannot name a task that
 * contains a dot.
 */

/** Quiet period after the latest extraction write before one consolidate run. */
export const DREAM_DEBOUNCE_MS = 5 * 60 * 1000;

/**
 * Nightly catch-up. Graphile evaluates crontab timestamps in UTC and has no
 * timezone field. 03:00 UTC is still night in Europe.
 */
export const DREAM_NIGHTLY_CRON = "0 3 * * *";

/** How far back a returning worker may catch a missed 03:00 sweep. */
export const DREAM_BACKFILL_MS = 12 * 60 * 60 * 1000;

/**
 * Stable identifier so a later schedule edit does not double-fire.
 * Backfill covers a worker that was down at 03:00 and is back the same day.
 * A task the worker has never seen is not backfilled.
 */
export const DREAM_CRON_ITEM = {
  task: "memory.dream",
  match: DREAM_NIGHTLY_CRON,
  identifier: "memory-dream",
  options: {
    backfillPeriod: DREAM_BACKFILL_MS,
    maxAttempts: 3,
  },
} satisfies CronItem;

/** Absent, empty, and any value other than true/on/1 stay off. */
export function neo4jDreamEnabled(settings: Record<string, string>): boolean {
  const value = settings.dream?.trim().toLowerCase();
  return value === "true" || value === "on" || value === "1";
}

export function dreamConsolidateJobKey(spaceId: string): string {
  return `memory.consolidate:${spaceId}:*:dream`;
}

export function dreamConsolidationJob(
  space: { spaceId: string; userId: string },
  availableAt: Date,
  keepExisting = false,
): BackgroundJob {
  const job = memoryConsolidateJob(
    {
      spaceId: space.spaceId,
      userId: space.userId,
      attempt: 0,
      scheduleGeneration: 0,
      automatic: true,
    },
    availableAt,
  );
  return {
    ...job,
    replaceKey: dreamConsolidateJobKey(space.spaceId),
    ...(keepExisting ? { keepExisting: true } : {}),
  };
}

/** Arm or push the one automatic consolidate job for this space. */
export async function scheduleDreamConsolidation(
  jobs: JobPublisher,
  space: { spaceId: string; userId: string },
  options: { now?: Date; delayMs: number; keepExisting?: boolean },
): Promise<void> {
  const now = options.now ?? new Date();
  await jobs.enqueue(
    dreamConsolidationJob(
      space,
      new Date(now.getTime() + options.delayMs),
      options.keepExisting ?? false,
    ),
  );
}

/** Schedule only after an extraction write, and only while dream mode is on. */
export async function maybeScheduleDreamConsolidation(
  jobs: JobPublisher,
  dreamEnabled: boolean,
  space: { spaceId: string; userId: string },
  wroteEntities: boolean,
  now = new Date(),
): Promise<void> {
  if (!wroteEntities || !dreamEnabled) return;
  await scheduleDreamConsolidation(jobs, space, { now, delayMs: DREAM_DEBOUNCE_MS });
}

/**
 * Nightly catch-up. `keepExisting` does not pull an already-armed debounce
 * earlier and does not replace a locked run.
 */
export async function sweepDreamConsolidation(
  spaces: readonly { spaceId: string; userId: string; dreamEnabled?: boolean }[],
  jobs: JobPublisher,
  now = new Date(),
): Promise<number> {
  let scheduled = 0;
  for (const space of spaces) {
    if (!space.dreamEnabled || !space.spaceId.trim() || !space.userId.trim()) continue;
    await scheduleDreamConsolidation(jobs, space, { now, delayMs: 0, keepExisting: true });
    scheduled += 1;
  }
  return scheduled;
}
