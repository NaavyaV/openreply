/**
 * Rate Limiter
 *
 * Redis-based rate limiter for Instagram private replies.
 *
 * The cap matches Meta's documented limit for this exact call: 750 private
 * replies per hour per Instagram professional account, for comments on posts
 * and reels. Exceeding it risks 429s and app-level restrictions, so the worker
 * parks the same job until the hour window has a free slot, instead of sending
 * through or giving up after a couple of tries.
 * https://developers.facebook.com/docs/graph-api/overview/rate-limiting/
 *
 * Note this is a hard ceiling with no headroom. If Meta throttles before the
 * documented limit, or other calls on the same account share the bucket, lower
 * this value.
 *
 * A viral hour (thousands of matching comments) still sends at 750/hour; the
 * rest wait in Redis and drain over subsequent hours. Skip only after the job
 * has been parked for many windows (~3 days) so Redis cannot hold a job forever.
 */

import { getRedisConnection } from "@/lib/queue/client";

const RATE_LIMIT_MAX = 750; // private replies per hour, per Meta's documented cap
const RATE_LIMIT_WINDOW = 3600; // 1 hour in seconds
const REQUEUE_DELAY_MS = 60 * 1000; // fallback when Redis has no TTL on the counter
const MAX_REQUEUE_DELAY_MS = 60 * 60 * 1000; // never sleep a job longer than one hour
const MAX_REQUEUE_ATTEMPTS = 72; // hourly parks ≈ 3 days, enough to drain ~50k at 750/hour

function getRedis() {
  return getRedisConnection();
}

function jitterMs(salt: string): number {
  let n = 0;
  for (let i = 0; i < salt.length; i += 1) {
    n = (n + salt.charCodeAt(i) * (i + 1)) % 15_000;
  }
  return n;
}

/**
 * Wait until the current hour counter expires, plus a small per-job stagger
 * so thousands of parked comments do not all wake on the same millisecond.
 */
export function delayFromPttl(pttl: number, salt = ""): number {
  const wait = pttl > 0 ? pttl + 3_000 : REQUEUE_DELAY_MS;
  return Math.min(wait + jitterMs(salt), MAX_REQUEUE_DELAY_MS);
}

export interface RateLimitResult {
  allowed: boolean;
  currentCount: number;
  remainingDMs: number;
  shouldRequeue: boolean;
  requeueDelayMs: number;
  shouldSkip: boolean;
  reserved: boolean;
}

const RESERVE_DM_SLOT_SCRIPT = `
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local max = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

if current >= max then
  local pttl = redis.call("PTTL", KEYS[1])
  return {0, current, 0, pttl}
end

local next_count = redis.call("INCR", KEYS[1])
if next_count == 1 then
  redis.call("EXPIRE", KEYS[1], ttl)
end

return {1, next_count, max - next_count, 0}
`;

function toScriptNumber(value: unknown): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") return Number.parseInt(value, 10);
  return 0;
}

function blockedResult(
  count: number,
  requeueAttempt: number,
  pttl: number,
  salt: string
): RateLimitResult {
  if (requeueAttempt >= MAX_REQUEUE_ATTEMPTS) {
    return {
      allowed: false,
      currentCount: count,
      remainingDMs: 0,
      shouldRequeue: false,
      requeueDelayMs: 0,
      shouldSkip: true,
      reserved: false,
    };
  }

  return {
    allowed: false,
    currentCount: count,
    remainingDMs: 0,
    shouldRequeue: true,
    requeueDelayMs: delayFromPttl(pttl, salt),
    shouldSkip: false,
    reserved: false,
  };
}

/**
 * Check if an Instagram account is within its DM rate limit.
 *
 * Uses a Redis counter with a 1-hour TTL per account.
 * Key pattern: `rate:dm:{instagramAccountId}`
 *
 * @param instagramAccountId - The Instagram account ID to check
 * @param requeueAttempt - How many times this job has been requeued (0 = first attempt)
 * @returns Rate limit result with action recommendations
 */
export async function checkRateLimit(
  instagramAccountId: string,
  requeueAttempt: number = 0
): Promise<RateLimitResult> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;

  const [currentCount, pttl] = await Promise.all([
    client.get(key),
    client.pttl(key),
  ]);
  const count = currentCount ? parseInt(currentCount, 10) : 0;

  if (count >= RATE_LIMIT_MAX) {
    return blockedResult(count, requeueAttempt, pttl, instagramAccountId);
  }

  return {
    allowed: true,
    currentCount: count,
    remainingDMs: RATE_LIMIT_MAX - count,
    shouldRequeue: false,
    requeueDelayMs: 0,
    shouldSkip: false,
    reserved: false,
  };
}

/**
 * Atomically reserve a DM send slot for an Instagram account.
 * This is the worker-safe path; it prevents concurrent jobs from all passing
 * the rate-limit check before any of them increments the Redis counter.
 */
export async function reserveDMSlot(
  instagramAccountId: string,
  requeueAttempt: number = 0
): Promise<RateLimitResult> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;

  const result = await client.eval(
    RESERVE_DM_SLOT_SCRIPT,
    1,
    key,
    RATE_LIMIT_MAX,
    RATE_LIMIT_WINDOW
  );
  const values = Array.isArray(result) ? result : [];
  const allowedFlag = toScriptNumber(values[0]);
  const count = toScriptNumber(values[1]);
  const remaining = toScriptNumber(values[2]);
  const pttl = toScriptNumber(values[3]);

  if (allowedFlag !== 1) {
    return blockedResult(count, requeueAttempt, pttl, instagramAccountId);
  }

  return {
    allowed: true,
    currentCount: count,
    remainingDMs: remaining,
    shouldRequeue: false,
    requeueDelayMs: 0,
    shouldSkip: false,
    reserved: true,
  };
}

/**
 * Backwards-compatible helper for tests and admin scripts.
 * Prefer reserveDMSlot in workers.
 */
export async function incrementDMCounter(
  instagramAccountId: string
): Promise<number> {
  const result = await reserveDMSlot(instagramAccountId, MAX_REQUEUE_ATTEMPTS);
  return result.currentCount;
}

/**
 * Get the current DM count for an Instagram account.
 */
export async function getCurrentDMCount(
  instagramAccountId: string
): Promise<number> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;
  const count = await client.get(key);
  return count ? parseInt(count, 10) : 0;
}

/**
 * Reset the rate limiter for an account (useful for testing).
 */
export async function resetRateLimit(
  instagramAccountId: string
): Promise<void> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;
  await client.del(key);
}

// Export constants for use in tests
export {
  RATE_LIMIT_MAX,
  RATE_LIMIT_WINDOW,
  REQUEUE_DELAY_MS,
  MAX_REQUEUE_ATTEMPTS,
  MAX_REQUEUE_DELAY_MS,
};
