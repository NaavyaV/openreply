import { describe, it, expect, vi, beforeEach } from "vitest";

const { mockSmismember, mockHmget } = vi.hoisted(() => ({
  mockSmismember: vi.fn(),
  mockHmget: vi.fn(),
}));

vi.mock("@/lib/queue/client", () => ({
  getRedisConnection: () => ({
    smismember: mockSmismember,
    hmget: mockHmget,
  }),
}));

import { commentIdsDueForDb } from "../lib/polling/sweep-memory";

beforeEach(() => {
  vi.clearAllMocks();
});

describe("commentIdsDueForDb", () => {
  it("skips comments already marked handled or checked within the hour", async () => {
    const recent = String(Date.now() - 10 * 60 * 1000);
    const stale = String(Date.now() - 2 * 60 * 60 * 1000);
    mockSmismember.mockResolvedValue([1, 0, 0, 0]);
    mockHmget.mockResolvedValue([null, recent, stale, null]);

    const due = await commentIdsDueForDb("auto_1", [
      "handled",
      "recent",
      "stale",
      "new",
    ]);

    expect(due).toEqual(["stale", "new"]);
  });
});
