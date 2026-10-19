import { memoryConsolidateJob, memoryConsolidateJobKey } from "@rakazo/adapter-kit";
import { parseCronItems, parseCrontab } from "graphile-worker";
import { describe, expect, it } from "vitest";
import {
  DREAM_BACKFILL_MS,
  DREAM_CRON_ITEM,
  DREAM_DEBOUNCE_MS,
  DREAM_NIGHTLY_CRON,
  dreamConsolidateJobKey,
  maybeScheduleDreamConsolidation,
  neo4jDreamEnabled,
  scheduleDreamConsolidation,
  sweepDreamConsolidation,
} from "./memory-dream.js";
import { InMemoryJobQueue } from "./wakeup.js";

const space = { spaceId: "space-1", userId: "user-1" };
const noon = new Date(Date.now() + 60 * 60 * 1000);

describe("neo4jDreamEnabled", () => {
  it("stays off unless the setting is explicitly on", () => {
    expect(neo4jDreamEnabled({})).toBe(false);
    expect(neo4jDreamEnabled({ dream: "" })).toBe(false);
    expect(neo4jDreamEnabled({ dream: "false" })).toBe(false);
    expect(neo4jDreamEnabled({ dream: "off" })).toBe(false);
    expect(neo4jDreamEnabled({ dream: " true " })).toBe(true);
    expect(neo4jDreamEnabled({ dream: "on" })).toBe(true);
    expect(neo4jDreamEnabled({ dream: "1" })).toBe(true);
  });
});

describe("dream consolidation scheduling", () => {
  it("coalesces a burst onto one later run and leaves manual jobs alone", async () => {
    const queue = new InMemoryJobQueue();
    await queue.enqueue(
      memoryConsolidateJob({
        spaceId: space.spaceId,
        userId: space.userId,
        attempt: 0,
        scheduleGeneration: 0,
      }),
    );
    await maybeScheduleDreamConsolidation(queue, false, space, true, noon);
    expect(queue.keyedJob(dreamConsolidateJobKey(space.spaceId))).toBeUndefined();

    await maybeScheduleDreamConsolidation(queue, true, space, false, noon);
    expect(queue.keyedJob(dreamConsolidateJobKey(space.spaceId))).toBeUndefined();

    await maybeScheduleDreamConsolidation(queue, true, space, true, noon);
    await scheduleDreamConsolidation(queue, space, {
      now: new Date(noon.getTime() + 60_000),
      delayMs: DREAM_DEBOUNCE_MS,
    });

    const dream = queue.keyedJob(dreamConsolidateJobKey(space.spaceId));
    expect(dream?.availableAt?.toISOString()).toBe(
      new Date(noon.getTime() + 60_000 + DREAM_DEBOUNCE_MS).toISOString(),
    );
    expect(queue.keyedJob(memoryConsolidateJobKey(space.spaceId, undefined, 0))).toEqual(
      expect.objectContaining({ attempts: 0 }),
    );
    expect(dreamConsolidateJobKey(space.spaceId)).toBe("memory.consolidate:space-1:*:dream");
  });

  it("arms a nightly run now and does not pull an armed debounce earlier", async () => {
    const queued = new InMemoryJobQueue();
    await scheduleDreamConsolidation(queued, space, { now: noon, delayMs: DREAM_DEBOUNCE_MS });
    const scheduled = await sweepDreamConsolidation(
      [
        { ...space, dreamEnabled: true },
        { spaceId: "space-2", userId: "user-2", dreamEnabled: false },
        { spaceId: " ", userId: "user-3", dreamEnabled: true },
      ],
      queued,
      new Date(noon.getTime() + 24 * 60 * 60 * 1000),
    );
    expect(scheduled).toBe(1);
    expect(queued.keyedJob(dreamConsolidateJobKey(space.spaceId))?.availableAt?.toISOString()).toBe(
      new Date(noon.getTime() + DREAM_DEBOUNCE_MS).toISOString(),
    );

    const empty = new InMemoryJobQueue();
    await sweepDreamConsolidation([{ ...space, dreamEnabled: true }], empty, noon);
    expect(empty.keyedJob(dreamConsolidateJobKey(space.spaceId))?.availableAt?.toISOString()).toBe(
      noon.toISOString(),
    );
  });

  it("schedules the catch-up at 03:00 UTC", () => {
    expect(DREAM_NIGHTLY_CRON).toBe("0 3 * * *");
    expect(DREAM_DEBOUNCE_MS).toBe(5 * 60 * 1000);
    expect(DREAM_CRON_ITEM).toMatchObject({
      task: "memory.dream",
      identifier: "memory-dream",
      options: { backfillPeriod: DREAM_BACKFILL_MS, maxAttempts: 3 },
    });
    // A crontab line cannot name a task that contains a dot.
    expect(() => parseCrontab("0 3 * * * memory.dream")).toThrow(/command/);
    const [item] = parseCronItems([DREAM_CRON_ITEM]);
    expect(item?.task).toBe("memory.dream");
    expect(item?.match({ min: 0, hour: 3, date: 19, month: 10, dow: 1 })).toBe(true);
    expect(item?.match({ min: 0, hour: 4, date: 19, month: 10, dow: 1 })).toBe(false);
    expect(item?.match({ min: 30, hour: 3, date: 19, month: 10, dow: 1 })).toBe(false);
  });
});
