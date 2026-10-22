import { getLogger } from "@rakazo/logging";
import type { Pool } from "pg";

/**
 * How long `gracefulShutdown` waits before aborting in-flight jobs.
 * Short enough to finish inside Kubernetes' default 30s grace and Compose's
 * worker `stop_grace_period` once the abort has time to land.
 */
export const DEFAULT_WORKER_SHUTDOWN_GRACE_MS = 10_000;

/**
 * After the abort, time for a model call to stop and for `runner.stop()` to
 * return before we unlock whatever is still held. The process stop grace must
 * cover grace + settle.
 */
export const WORKER_SHUTDOWN_SETTLE_MS = 10_000;

const MAX_SHUTDOWN_GRACE_MS = 600_000;

/**
 * Blank or invalid keeps the default. `0` aborts as soon as shutdown starts.
 * Values above the longest memory-model timeout fall back to the default.
 */
export function readWorkerShutdownGraceMs(value: string | undefined): number {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) return DEFAULT_WORKER_SHUTDOWN_GRACE_MS;
  if (!/^\d+$/.test(trimmed)) return DEFAULT_WORKER_SHUTDOWN_GRACE_MS;
  const parsed = Number(trimmed);
  if (parsed > MAX_SHUTDOWN_GRACE_MS) return DEFAULT_WORKER_SHUTDOWN_GRACE_MS;
  return parsed;
}

/**
 * Unset, blank, `0`, or not a whole number disables the stale-lock release.
 * Any other positive whole number of milliseconds is how old another process's
 * lock must be before this process releases it. `1` and `60000` are both
 * accepted: there is no higher minimum. The lock is held for the whole job.
 * This process never releases a pool id it has registered, including a runner
 * started again in the same process. Leave this unset when more than one worker
 * uses the database: a rolling update starts the new process while the old one
 * still holds the lock, and a short age lets both run the job. One replica with
 * a Recreate strategy can set 60000. That stops the old pod before the new one
 * starts, so a dead worker's lock is released once it is a minute old.
 */
export function readStaleLockAfterMs(value: string | undefined): number | undefined {
  const trimmed = value?.trim() ?? "";
  if (!trimmed || trimmed === "0") return undefined;
  if (!/^\d+$/.test(trimmed)) return undefined;
  const parsed = Number(trimmed);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) return undefined;
  return parsed;
}

/** Repeat the stale-lock release this often when the age threshold is set. */
export const DEFAULT_STALE_LOCK_INTERVAL_MS = 300_000;

/**
 * Off when the age threshold is off. Unset uses 300000. `0` keeps the startup
 * pass and does not repeat. Blank is unset. Anything else that is not a positive
 * whole number uses 300000.
 */
export function readStaleLockIntervalMs(
  value: string | undefined,
  staleLockAfterMs: number | undefined,
): number | undefined {
  if (staleLockAfterMs === undefined) return undefined;
  const trimmed = value?.trim() ?? "";
  if (!trimmed) return DEFAULT_STALE_LOCK_INTERVAL_MS;
  if (trimmed === "0") return undefined;
  if (!/^\d+$/.test(trimmed)) return DEFAULT_STALE_LOCK_INTERVAL_MS;
  const parsed = Number(trimmed);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) return DEFAULT_STALE_LOCK_INTERVAL_MS;
  return parsed;
}

const processGraphileWorkerIds = new Set<string>();

/** Pool ids this process has registered. A relaunched runner stays in the set. */
export function rememberGraphileWorkerId(id: string): void {
  if (id.length > 0) processGraphileWorkerIds.add(id);
}

export function graphileWorkerIds(): readonly string[] {
  return [...processGraphileWorkerIds];
}

/** Tests only. Production keeps every id so a sweep cannot unlock this process. */
export function clearGraphileWorkerIds(): void {
  processGraphileWorkerIds.clear();
}

interface LockRow {
  kind: string;
  id: string;
  queue: string | null;
  worker_id: string;
}

/**
 * Release job and queue locks older than `afterMs` held by a worker other than
 * `exceptWorkerIds`. Attempts stay as graphile left them when it locked the row
 * (`resetLockedAt` does the same): a job already at `max_attempts` stays
 * failed, and the queue lock is what was blocking everyone else.
 * Graphile's own sweep waits 4 hours.
 */
export async function unlockStaleWorkerLocks(
  pool: Pool,
  afterMs: number,
  exceptWorkerIds?: readonly string[],
): Promise<number> {
  if (!Number.isSafeInteger(afterMs) || afterMs <= 0) return 0;
  const exceptIds = [...new Set((exceptWorkerIds ?? []).filter((id) => id.length > 0))];
  const { rows } = await pool.query<LockRow>(
    `with released_jobs as (
       update graphile_worker._private_jobs as jobs
       set locked_at = null,
           locked_by = null,
           run_at = greatest(jobs.run_at, now())
       from (
         select id, locked_by, job_queue_id
         from graphile_worker._private_jobs
         where locked_by is not null
           and locked_by <> all($2::text[])
           and locked_at < now() - ($1::bigint * interval '1 millisecond')
       ) as stale
       where jobs.id = stale.id
         and jobs.locked_by = stale.locked_by
       returning jobs.id::text as id, stale.locked_by as worker_id, stale.job_queue_id
     ),
     named_jobs as (
       select released_jobs.id, released_jobs.worker_id, queues.queue_name as queue
       from released_jobs
       left join graphile_worker._private_job_queues as queues
         on queues.id = released_jobs.job_queue_id
     ),
     released_queues as (
       update graphile_worker._private_job_queues as queues
       set locked_at = null, locked_by = null
       from (
         select id, queue_name, locked_by
         from graphile_worker._private_job_queues
         where locked_by is not null
           and locked_by <> all($2::text[])
           and locked_at < now() - ($1::bigint * interval '1 millisecond')
       ) as stale
       where queues.id = stale.id
         and queues.locked_by = stale.locked_by
       returning queues.id::text as id, stale.queue_name as queue, stale.locked_by as worker_id
     )
     select 'job'::text as kind, id, queue, worker_id from named_jobs
     union all
     select 'queue'::text as kind, id, queue, worker_id from released_queues`,
    [afterMs, exceptIds],
  );
  for (const row of rows) {
    getLogger().warn("released stale graphile lock", {
      ...(row.kind === "job" ? { "job.id": row.id } : { "graphile.queue_id": row.id }),
      ...(row.queue ? { "job.queue": row.queue } : {}),
      "graphile.worker_id": row.worker_id,
    });
  }
  return rows.length;
}

/**
 * Repeat `unlockStaleWorkerLocks`. `exceptWorkerIds` is read on each pass so a
 * pool started again in this process is excluded as soon as it is registered.
 * Stop before closing the pool. `finished` is the pass in flight.
 */
export function startStaleLockSweep(
  pool: Pool,
  afterMs: number,
  intervalMs: number,
  exceptWorkerIds: () => readonly string[],
): { stop: () => void; finished: () => Promise<void> } {
  if (!Number.isSafeInteger(intervalMs) || intervalMs <= 0) {
    return { stop() {}, finished: () => Promise.resolve() };
  }
  let stopped = false;
  let pending: Promise<void> = Promise.resolve();
  const timer = setInterval(() => {
    if (stopped) return;
    const except = exceptWorkerIds();
    pending = pending.then(() => {
      if (stopped) return undefined;
      return unlockStaleWorkerLocks(pool, afterMs, except)
        .then(() => undefined)
        .catch((error: unknown) => {
          getLogger().error("stale graphile lock release failed", error);
        });
    });
  }, intervalMs);
  timer.unref?.();
  return {
    stop() {
      stopped = true;
      clearInterval(timer);
    },
    finished() {
      return pending;
    },
  };
}

interface RefundedJob {
  id: string;
}

/**
 * Unlock this process's pool and put an interrupted job back without the
 * attempt `get_job` already counted. `force_unlock_workers` clears job and
 * queue locks but leaves `attempts` alone; a memory extract has one attempt,
 * so a deploy would otherwise fail it permanently.
 *
 * The refund is limited to ids whose abort signal fired. A job that already
 * finished is deleted by graphile's `completeJob` without checking `locked_by`;
 * refunding every row still locked by this pool would requeue that success if
 * the delete lost the race. Rows another worker has since locked are left
 * alone. `failJobs` during shutdown is not awaited, so this runs in one
 * transaction: it waits on that row lock, then clears the error and the
 * backoff, and the same commit releases the queue.
 */
export async function releaseOwnWorkerLocks(
  pool: Pool,
  poolIds: readonly string[],
  interruptedJobIds: readonly string[],
): Promise<void> {
  const workers = [...new Set(poolIds.filter((id) => id.length > 0))];
  const jobs = [...new Set(interruptedJobIds.filter((id) => id.length > 0))];
  if (workers.length === 0 && jobs.length === 0) return;
  const client = await pool.connect();
  const refunded: RefundedJob[] = [];
  try {
    await client.query("begin");
    if (jobs.length > 0) {
      const result = await client.query<RefundedJob>(
        `update graphile_worker._private_jobs as jobs
         set attempts = greatest(jobs.attempts - 1, 0),
             last_error = null,
             run_at = now(),
             locked_at = null,
             locked_by = null
         where jobs.id = any($2::bigint[])
           and jobs.attempts > 0
           and (
             jobs.locked_by = any($1::text[])
             or jobs.locked_by is null
           )
         returning jobs.id::text as id`,
        [workers, jobs],
      );
      refunded.push(...result.rows);
    }
    if (workers.length > 0) {
      await client.query(`select graphile_worker.force_unlock_workers($1::text[])`, [workers]);
    }
    await client.query("commit");
  } catch (error) {
    await client.query("rollback").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
  for (const row of refunded) {
    getLogger().info("released graphile lock on shutdown", { "job.id": row.id });
  }
  if (workers.length > 0) {
    getLogger().info("force-unlocked graphile workers", { "graphile.worker_ids": workers });
  }
}
