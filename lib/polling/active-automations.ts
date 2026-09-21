/**
 * Active-campaign cache for the comment poller.
 *
 * Neon Free always waits ~5 minutes after the last connection before it
 * sleeps, and that timeout cannot be shortened on the free plan. The poller
 * used to `findMany` campaigns every cycle, which woke compute just to learn
 * nothing had changed. Redis already holds the send queue, so the live
 * campaign list lives there too. Quiet sweeps stay on Instagram + Redis;
 * Postgres is opened only on a cache miss or when a comment actually needs
 * a DmLog lookup.
 */

import { getRedisConnection } from "@/lib/queue/client";
import { prisma } from "@/lib/db/client";

export const ACTIVE_AUTOMATIONS_CACHE_KEY = "cache:active-automations";
const CACHE_TTL_SECONDS = 30 * 60;

export type PollerAutomation = {
  id: string;
  name: string;
  postId: string | null;
  matchAnyPost: boolean;
  matchAnyWord: boolean;
  keywords: string[];
  wholeWordMatch: boolean;
  publicReplyEnabled: boolean;
  workspaceId: string;
  instagramAccount: {
    id: string;
    instagramId: string;
    username: string;
    accessToken: string;
  };
};

const POLLER_AUTOMATION_SELECT = {
  id: true,
  name: true,
  postId: true,
  matchAnyPost: true,
  matchAnyWord: true,
  keywords: true,
  wholeWordMatch: true,
  publicReplyEnabled: true,
  workspaceId: true,
  instagramAccount: {
    select: {
      id: true,
      instagramId: true,
      username: true,
      accessToken: true,
    },
  },
} as const;

export async function invalidateActiveAutomationsCache(): Promise<void> {
  try {
    await getRedisConnection().del(ACTIVE_AUTOMATIONS_CACHE_KEY);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    console.error(
      "[Active automations cache] Failed to invalidate:",
      message
    );
  }
}

export async function getActiveAutomationsForPoll(): Promise<
  PollerAutomation[]
> {
  const redis = getRedisConnection();
  try {
    const cached = await redis.get(ACTIVE_AUTOMATIONS_CACHE_KEY);
    if (cached) {
      return JSON.parse(cached) as PollerAutomation[];
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    console.error("[Active automations cache] Failed to read:", message);
  }

  const automations = await prisma.automation.findMany({
    where: { isActive: true },
    select: POLLER_AUTOMATION_SELECT,
  });

  try {
    await redis.set(
      ACTIVE_AUTOMATIONS_CACHE_KEY,
      JSON.stringify(automations),
      "EX",
      CACHE_TTL_SECONDS
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    console.error("[Active automations cache] Failed to write:", message);
  }

  return automations;
}
