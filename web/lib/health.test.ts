import { expect, test } from "vitest";
import { QueueHealth, Tier, oldestReady, reasons, starving } from "./health";

function health(over: Partial<QueueHealth> = {}): QueueHealth {
  return {
    queue: {
      id: "q",
      name: "email",
      paused: false,
      max_concurrency: 4,
      shards: 1,
      in_flight: 0,
      breaker_state: "closed",
      rl_limit_per_sec: 0,
    },
    tiers: [{ priority: 0, oldest_ready_seconds: 10, ready: 3 }],
    live_workers: 1,
    breaker_open_until: null,
    rate_limited: false,
    saturated: false,
    last_hour: { completed: 0, failed: 0, dead_lettered: 0 },
    duration_ms_p95: null,
    ...over,
  };
}

test("draining queue reports no blocker", () => {
  const [first] = reasons(health());
  expect(first.level).toBe("ok");
  expect(first.text).toContain("draining");
});

test("paused leads the reasons", () => {
  const out = reasons(health({ queue: { ...health().queue, paused: true } }));
  expect(out[0].level).toBe("bad");
  expect(out[0].text).toContain("paused");
});

test("open breaker is reported with its deadline", () => {
  const out = reasons(health({ breaker_open_until: "2026-01-01T00:00:00Z" }));
  expect(out.some((r) => r.text.includes("2026-01-01T00:00:00Z"))).toBe(true);
});

test("no worker is a blocker", () => {
  const out = reasons(health({ live_workers: 0 }));
  expect(out.some((r) => r.level === "bad" && r.text.includes("no live worker"))).toBe(true);
});

test("saturation and rate limiting are warnings not blockers", () => {
  const out = reasons(health({ saturated: true, rate_limited: true }));
  expect(out.every((r) => r.level !== "bad")).toBe(true);
  expect(out.length).toBe(2);
});

test("an idle queue with nothing ready is not a fault", () => {
  const out = reasons(health({ tiers: [] }));
  expect(out[0].level).toBe("ok");
  expect(out[0].text).toContain("nothing is ready");
});

test("every blocker is listed, not just the first", () => {
  const out = reasons(
    health({ queue: { ...health().queue, paused: true }, live_workers: 0, saturated: true }),
  );
  expect(out.length).toBe(3);
});

test("oldest ready takes the max across tiers, not the first", () => {
  const tiers: Tier[] = [
    { priority: 3, oldest_ready_seconds: 5, ready: 1 },
    { priority: 0, oldest_ready_seconds: 900, ready: 4 },
  ];
  expect(oldestReady(tiers)).toBe(900);
  expect(oldestReady([])).toBeNull();
});

test("starvation is the low tier waiting far longer than the high one", () => {
  const starved = starving([
    { priority: 3, oldest_ready_seconds: 5, ready: 2 },
    { priority: 0, oldest_ready_seconds: 900, ready: 4 },
  ]);
  expect(starved?.priority).toBe(0);
});

test("a single tier is never starvation", () => {
  expect(starving([{ priority: 0, oldest_ready_seconds: 900, ready: 4 }])).toBeNull();
});

test("tiers draining at a similar age are not starvation", () => {
  const starved = starving([
    { priority: 3, oldest_ready_seconds: 60, ready: 2 },
    { priority: 0, oldest_ready_seconds: 90, ready: 4 },
  ]);
  expect(starved).toBeNull();
});
