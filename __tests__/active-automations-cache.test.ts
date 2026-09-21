import { describe, it, expect, vi, beforeEach } from "vitest";

const { mockGet, mockSet, mockDel, mockFindMany } = vi.hoisted(() => ({
  mockGet: vi.fn(),
  mockSet: vi.fn(),
  mockDel: vi.fn(),
  mockFindMany: vi.fn(),
}));

vi.mock("@/lib/queue/client", () => ({
  getRedisConnection: () => ({
    get: mockGet,
    set: mockSet,
    del: mockDel,
  }),
}));

vi.mock("@/lib/db/client", () => ({
  prisma: {
    automation: { findMany: mockFindMany },
  },
}));

import {
  ACTIVE_AUTOMATIONS_CACHE_KEY,
  getActiveAutomationsForPoll,
  invalidateActiveAutomationsCache,
} from "../lib/polling/active-automations";

const sample = [
  {
    id: "auto_1",
    name: "Link",
    postId: "media_1",
    matchAnyPost: false,
    matchAnyWord: false,
    keywords: ["LINK"],
    wholeWordMatch: true,
    publicReplyEnabled: false,
    workspaceId: "ws_1",
    instagramAccount: {
      id: "ig_row",
      instagramId: "ig_1",
      username: "bigvig_",
      accessToken: "enc",
    },
  },
];

beforeEach(() => {
  vi.clearAllMocks();
});

describe("active automations cache", () => {
  it("returns the Redis payload without querying Postgres", async () => {
    mockGet.mockResolvedValue(JSON.stringify(sample));

    const result = await getActiveAutomationsForPoll();

    expect(result).toEqual(sample);
    expect(mockFindMany).not.toHaveBeenCalled();
    expect(mockSet).not.toHaveBeenCalled();
  });

  it("loads from Postgres on a miss and stores the result", async () => {
    mockGet.mockResolvedValue(null);
    mockFindMany.mockResolvedValue(sample);

    const result = await getActiveAutomationsForPoll();

    expect(result).toEqual(sample);
    expect(mockFindMany).toHaveBeenCalledWith(
      expect.objectContaining({ where: { isActive: true } })
    );
    expect(mockSet).toHaveBeenCalledWith(
      ACTIVE_AUTOMATIONS_CACHE_KEY,
      JSON.stringify(sample),
      "EX",
      6 * 60 * 60
    );
  });

  it("invalidates the cache key", async () => {
    await invalidateActiveAutomationsCache();
    expect(mockDel).toHaveBeenCalledWith(ACTIVE_AUTOMATIONS_CACHE_KEY);
  });
});
