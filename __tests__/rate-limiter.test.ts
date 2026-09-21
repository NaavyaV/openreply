/**
 * Rate Limiter — Unit Tests
 *
 * Tests the hourly private-reply cap enforcement using mocked Redis.
 * Assertions derive from RATE_LIMIT_MAX so they survive a change to the cap.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";

const { mockGet, mockEval, mockDel, mockPttl } = vi.hoisted(() => ({
  mockGet: vi.fn(),
  mockEval: vi.fn(),
  mockDel: vi.fn(),
  mockPttl: vi.fn(),
}));

vi.mock("@/lib/queue/client", () => ({
  getRedisConnection: () => ({
    get: mockGet,
    eval: mockEval,
    del: mockDel,
    pttl: mockPttl,
  }),
}));

vi.stubEnv("REDIS_URL", "redis://localhost:6379");

import {
  checkRateLimit,
  delayFromPttl,
  incrementDMCounter,
  reserveDMSlot,
  MAX_REQUEUE_ATTEMPTS,
  MAX_REQUEUE_DELAY_MS,
  RATE_LIMIT_MAX,
} from "../lib/utils/rate-limiter";

beforeEach(() => {
  vi.clearAllMocks();
  mockPttl.mockResolvedValue(-2);
});

describe("delayFromPttl", () => {
  it("waits until the Redis counter expires, plus stagger", () => {
    const delay = delayFromPttl(120_000, "account_123");
    expect(delay).toBeGreaterThanOrEqual(123_000);
    expect(delay).toBeLessThanOrEqual(138_000);
  });

  it("falls back to one minute when Redis has no TTL", () => {
    const delay = delayFromPttl(0, "account_123");
    expect(delay).toBeGreaterThanOrEqual(60_000);
    expect(delay).toBeLessThanOrEqual(75_000);
  });

  it("never parks a job longer than one hour", () => {
    expect(delayFromPttl(10 * 60 * 60 * 1000, "account_123")).toBe(
      MAX_REQUEUE_DELAY_MS
    );
  });
});

describe("checkRateLimit", () => {
  it("should allow when count is below limit", async () => {
    mockGet.mockResolvedValue("50");

    const result = await checkRateLimit("account_123");

    expect(result.allowed).toBe(true);
    expect(result.currentCount).toBe(50);
    expect(result.remainingDMs).toBe(RATE_LIMIT_MAX - 50);
    expect(result.shouldRequeue).toBe(false);
    expect(result.shouldSkip).toBe(false);
    expect(result.reserved).toBe(false);
  });

  it("should allow when no previous count exists", async () => {
    mockGet.mockResolvedValue(null);

    const result = await checkRateLimit("account_123");

    expect(result.allowed).toBe(true);
    expect(result.currentCount).toBe(0);
    expect(result.remainingDMs).toBe(RATE_LIMIT_MAX);
  });

  it("should deny when count reaches the limit and wait for the window", async () => {
    mockGet.mockResolvedValue(String(RATE_LIMIT_MAX));
    mockPttl.mockResolvedValue(120_000);

    const result = await checkRateLimit("account_123");

    expect(result.allowed).toBe(false);
    expect(result.shouldRequeue).toBe(true);
    expect(result.shouldSkip).toBe(false);
    expect(result.requeueDelayMs).toBe(delayFromPttl(120_000, "account_123"));
  });

  it("should skip after max requeue attempts", async () => {
    mockGet.mockResolvedValue(String(RATE_LIMIT_MAX));

    const result = await checkRateLimit("account_123", MAX_REQUEUE_ATTEMPTS);

    expect(result.allowed).toBe(false);
    expect(result.shouldRequeue).toBe(false);
    expect(result.shouldSkip).toBe(true);
  });
});

describe("reserveDMSlot", () => {
  it("should atomically reserve a slot when below the hourly cap", async () => {
    mockEval.mockResolvedValue([1, 51, 139, 0]);

    const result = await reserveDMSlot("account_123");

    expect(mockEval).toHaveBeenCalledWith(
      expect.any(String),
      1,
      "rate:dm:account_123",
      RATE_LIMIT_MAX,
      3600
    );
    expect(result.allowed).toBe(true);
    expect(result.reserved).toBe(true);
    expect(result.currentCount).toBe(51);
    expect(result.remainingDMs).toBe(139);
  });

  it("should recommend requeue when the atomic reserve is denied", async () => {
    mockEval.mockResolvedValue([0, RATE_LIMIT_MAX, 0, 180_000]);

    const result = await reserveDMSlot("account_123", 0);

    expect(result.allowed).toBe(false);
    expect(result.reserved).toBe(false);
    expect(result.shouldRequeue).toBe(true);
    expect(result.shouldSkip).toBe(false);
    expect(result.requeueDelayMs).toBe(delayFromPttl(180_000, "account_123"));
  });

  it("should skip after max requeue attempts", async () => {
    mockEval.mockResolvedValue(["0", String(RATE_LIMIT_MAX), "0", "0"]);

    const result = await reserveDMSlot(
      "account_123",
      MAX_REQUEUE_ATTEMPTS
    );

    expect(result.allowed).toBe(false);
    expect(result.shouldRequeue).toBe(false);
    expect(result.shouldSkip).toBe(true);
  });
});

describe("incrementDMCounter", () => {
  it("should use the atomic reservation path", async () => {
    mockEval.mockResolvedValue([1, 51, 139, 0]);

    const count = await incrementDMCounter("account_123");

    expect(mockEval).toHaveBeenCalled();
    expect(count).toBe(51);
  });
});
