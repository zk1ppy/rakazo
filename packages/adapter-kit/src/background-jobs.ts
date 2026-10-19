import { z } from "zod";
import type {
  BackgroundJob,
  BackgroundJobHandlers,
  BackgroundJobName,
  BackgroundJobPayloads,
} from "./types.js";

const payloadSchemas = {
  "run.continue": z.object({ runId: z.string().min(1) }),
  "routine.wakeup": z.object({
    routineId: z.string().min(1),
    scheduledFor: z.string().datetime({ offset: true }),
  }),
  "routine.retry": z.object({ runId: z.string().min(1) }),
  "computer.update": z.object({ updateId: z.string().min(1) }),
  "computer.sleep": z.object({ computerId: z.string().min(1) }),
  "computer.control-expire": z.object({
    computerId: z.string().min(1),
    leaseId: z.string().min(1),
  }),
  "skill.teaching-expire": z.object({ skillId: z.string().min(1) }),
  "history.compact": z.object({ threadId: z.string().min(1) }),
  "messaging.deliver": z.object({ runId: z.string().min(1).optional() }),
  "cloud_agent.poll": z.object({ agentId: z.string().min(1) }),
  "memory.extract": z.object({
    spaceId: z.string().min(1),
    userId: z.string().min(1),
    botId: z.string().min(1),
    factId: z.string().min(1),
    ns: z.string().min(1),
    maxChunkChars: z.number().int().min(400).max(20_000),
    chunkIndex: z.number().int().min(0).max(7),
    attempt: z.number().int().min(0).max(8),
    scheduleGeneration: z.number().int().min(0).max(80),
  }),
  "memory.consolidate": z.object({
    spaceId: z.string().min(1),
    userId: z.string().min(1),
    ns: z.string().min(1).max(200).optional(),
    afterId: z.string().min(1).max(200).optional(),
    carryIds: z.array(z.string().min(1).max(200)).max(40).optional(),
    rejected: z.array(z.string().min(3).max(400)).max(80).optional(),
    attempt: z.number().int().min(0).max(8),
    scheduleGeneration: z.number().int().min(0).max(400),
    automatic: z.boolean().optional(),
  }),
  // Graphile cron merges `{ _cron: { ts, backfilled } }` into the payload.
  "memory.dream": z.preprocess((value) => {
    if (!value || typeof value !== "object" || Array.isArray(value)) return value;
    const record = { ...(value as Record<string, unknown>) };
    delete record._cron;
    return record;
  }, z.object({}).strict()),
} satisfies { [Name in BackgroundJobName]: z.ZodType<BackgroundJobPayloads[Name]> };

export function parseBackgroundJob(name: string, payload: unknown): BackgroundJob {
  if (!(name in payloadSchemas)) throw new Error(`Unknown background job: ${name}`);
  const typedName = name as BackgroundJobName;
  const parsed = payloadSchemas[typedName].parse(payload);
  return { name: typedName, payload: parsed } as BackgroundJob;
}

export async function dispatchBackgroundJob(
  handlers: BackgroundJobHandlers,
  name: string,
  payload: unknown,
): Promise<void> {
  const job = parseBackgroundJob(name, payload);
  const handler = handlers[job.name] as (payload: typeof job.payload) => Promise<void>;
  await handler(job.payload);
}

export function runJobKey(runId: string): string {
  return `run:${runId}`;
}

export function routineJobKey(routineId: string): string {
  return `routine:${routineId}`;
}

export function computerSleepJobKey(computerId: string): string {
  return `computer.sleep:${computerId}`;
}

export function computerControlExpireJobKey(computerId: string, leaseId?: string): string {
  return leaseId
    ? `computer.control-expire:${computerId}:${leaseId}`
    : `computer.control-expire:${computerId}`;
}

export function skillTeachingExpireJobKey(skillId: string): string {
  return `skill.teaching-expire:${skillId}`;
}

export function runContinueJob(runId: string): BackgroundJob {
  return {
    name: "run.continue",
    payload: { runId },
    replaceKey: runJobKey(runId),
  };
}

export function routineWakeupJob(routineId: string, scheduledFor: Date): BackgroundJob {
  return {
    name: "routine.wakeup",
    payload: { routineId, scheduledFor: scheduledFor.toISOString() },
    availableAt: scheduledFor,
    replaceKey: routineJobKey(routineId),
  };
}

/** Keyed to the failed run, so a repeated enqueue keeps a single retry. */
export function routineRetryJob(runId: string, availableAt: Date): BackgroundJob {
  return {
    name: "routine.retry",
    payload: { runId },
    availableAt,
    replaceKey: `routine.retry:${runId}`,
  };
}

export function computerSleepJob(computerId: string, availableAt: Date): BackgroundJob {
  return {
    name: "computer.sleep",
    payload: { computerId },
    availableAt,
    replaceKey: computerSleepJobKey(computerId),
  };
}

export function computerControlExpireJob(
  computerId: string,
  leaseId: string,
  availableAt: Date,
): BackgroundJob {
  return {
    name: "computer.control-expire",
    payload: { computerId, leaseId },
    availableAt,
    replaceKey: computerControlExpireJobKey(computerId, leaseId),
  };
}

export function skillTeachingExpireJob(skillId: string, availableAt: Date): BackgroundJob {
  return {
    name: "skill.teaching-expire",
    payload: { skillId },
    availableAt,
    replaceKey: skillTeachingExpireJobKey(skillId),
  };
}

export function historyCompactJobKey(threadId: string): string {
  return `history.compact:${threadId}`;
}

/**
 * Each attempt runs a summarizer completion that can take up to the summarizer
 * timeout, so the job queue's default attempt count turns one permanently
 * failing thread into hours of paid retries. A few tries ride out transient
 * provider errors without storming.
 */
export const HISTORY_COMPACT_MAX_ATTEMPTS = 4;

export function messagingDeliverJob(runId?: string, availableAt?: Date): BackgroundJob {
  return {
    name: "messaging.deliver",
    payload: runId ? { runId } : {},
    replaceKey: `messaging.deliver:${runId ?? "drain"}`,
    ...(availableAt ? { availableAt } : {}),
  };
}

export function historyCompactJob(threadId: string): BackgroundJob {
  return {
    name: "history.compact",
    payload: { threadId },
    replaceKey: historyCompactJobKey(threadId),
    maxAttempts: HISTORY_COMPACT_MAX_ATTEMPTS,
  };
}

export function cloudAgentPollJobKey(agentId: string): string {
  return `cloud_agent.poll:${agentId}`;
}

/**
 * One global queue for memory model work. Graphile runs a single job from a
 * named queue at a time, including across worker replicas, and TaskSpec has no
 * concurrency field for that queue. Worker `concurrency` still applies to
 * other jobs. `memory.extract` and `memory.consolidate` share this queue so
 * their model calls cannot overlap.
 */
export const MEMORY_EXTRACT_QUEUE_NAME = "memory-extract";

/**
 * Rate limits reschedule a new row instead of failing this one, so graphile
 * must not also retry it. The key alternates with scheduleGeneration because
 * a successful attempt deletes its own row: the follow-up has to be a different
 * key or that delete removes the reschedule.
 */
export const MEMORY_EXTRACT_MAX_ATTEMPTS = 1;

export function memoryExtractJobKey(factId: string, scheduleGeneration: number): string {
  return `memory.extract:${factId}:${scheduleGeneration % 2}`;
}

export function memoryExtractJob(
  payload: BackgroundJobPayloads["memory.extract"],
  availableAt?: Date,
): BackgroundJob {
  return {
    name: "memory.extract",
    payload,
    replaceKey: memoryExtractJobKey(payload.factId, payload.scheduleGeneration),
    queueName: MEMORY_EXTRACT_QUEUE_NAME,
    maxAttempts: MEMORY_EXTRACT_MAX_ATTEMPTS,
    ...(availableAt ? { availableAt } : {}),
  };
}

/**
 * A dropped database connection can retry this row. Rate limits and confirmation
 * timeouts do not: the handler reschedules a new row and returns. A permanent
 * model failure also returns, so Graphile does not spend the remaining attempts.
 * The key alternates with scheduleGeneration so that follow-up survives the
 * successful row's delete.
 */
export const MEMORY_CONSOLIDATE_MAX_ATTEMPTS = 3;

export function memoryConsolidateJobKey(
  spaceId: string,
  ns: string | undefined,
  scheduleGeneration: number,
): string {
  return `memory.consolidate:${spaceId}:${ns?.trim() || "*"}:${scheduleGeneration % 2}`;
}

export function memoryConsolidateJob(
  payload: BackgroundJobPayloads["memory.consolidate"],
  availableAt?: Date,
): BackgroundJob {
  return {
    name: "memory.consolidate",
    payload,
    replaceKey: memoryConsolidateJobKey(payload.spaceId, payload.ns, payload.scheduleGeneration),
    queueName: MEMORY_EXTRACT_QUEUE_NAME,
    maxAttempts: MEMORY_CONSOLIDATE_MAX_ATTEMPTS,
    ...(availableAt ? { availableAt } : {}),
  };
}

export function cloudAgentPollJob(
  payload: BackgroundJobPayloads["cloud_agent.poll"],
  availableAt?: Date,
): BackgroundJob {
  return {
    name: "cloud_agent.poll",
    payload,
    replaceKey: cloudAgentPollJobKey(payload.agentId),
    ...(availableAt ? { availableAt } : {}),
  };
}
