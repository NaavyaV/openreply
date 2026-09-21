/**
 * What the comment sweep already learned, kept in Redis.
 *
 * A quiet sweep used to `SELECT` DmLog every 11 minutes for the same comments
 * it already knew were sent. Each of those queries woke Neon Free for ~5
 * minutes. Handled ids are remembered for the poll lookback; anything still
 * open is rechecked at most once an hour, which is enough for the safety net.
 */

import { getRedisConnection } from "@/lib/queue/client";

const HANDLED_TTL_SECONDS = 72 * 60 * 60;
const RECHECK_MS = 60 * 60 * 1000;

function handledKey(automationId: string) {
  return `poll:handled:${automationId}`;
}

function checkedKey(automationId: string) {
  return `poll:checked:${automationId}`;
}

export async function commentIdsDueForDb(
  automationId: string,
  commentIds: string[]
): Promise<string[]> {
  if (commentIds.length === 0) return [];

  const redis = getRedisConnection();
  const [handledFlags, checkedAt] = await Promise.all([
    redis.smismember(handledKey(automationId), ...commentIds),
    redis.hmget(checkedKey(automationId), ...commentIds),
  ]);

  const now = Date.now();
  return commentIds.filter((id, index) => {
    if (Number(handledFlags[index]) === 1) return false;
    const seen = checkedAt[index] ? Number(checkedAt[index]) : 0;
    return !seen || now - seen >= RECHECK_MS;
  });
}

export async function rememberSweepComments(
  automationId: string,
  handledIds: string[],
  checkedIds: string[]
): Promise<void> {
  if (handledIds.length === 0 && checkedIds.length === 0) return;

  const redis = getRedisConnection();
  const now = Date.now().toString();
  const pipe = redis.pipeline();

  if (handledIds.length > 0) {
    pipe.sadd(handledKey(automationId), ...handledIds);
    pipe.expire(handledKey(automationId), HANDLED_TTL_SECONDS);
  }
  if (checkedIds.length > 0) {
    pipe.hset(
      checkedKey(automationId),
      ...checkedIds.flatMap((id) => [id, now])
    );
    pipe.expire(checkedKey(automationId), HANDLED_TTL_SECONDS);
  }

  await pipe.exec();
}
