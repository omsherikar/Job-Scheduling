export type Queue = {
  id: string;
  name: string;
  paused: boolean;
  max_concurrency: number;
  in_flight: number;
  breaker_state: string;
  rl_limit_per_sec: number;
  rl_burst?: number;
  default_priority?: number;
};

export type Tier = {
  priority: number;
  oldest_ready_seconds: number;
  ready: number;
};

export type QueueHealth = {
  queue: Queue;
  tiers: Tier[];
  live_workers: number;
  breaker_open_until: string | null;
  rate_limited: boolean;
  saturated: boolean;
  last_hour: { completed: number; failed: number; dead_lettered: number };
  duration_ms_p95: number | null;
};

export type QueueStats = QueueHealth & {
  status_counts: Record<string, number>;
  oldest_ready_seconds: number | null;
  duration_ms_p50: number | null;
};

export type Reason = {
  level: "bad" | "warn" | "ok";
  text: string;
};

export function reasons(s: QueueHealth): Reason[] {
  const out: Reason[] = [];
  const ready = s.tiers.reduce((n, t) => n + t.ready, 0);

  if (s.queue.paused) {
    out.push({ level: "bad", text: "queue is paused, nothing will be claimed" });
  }
  if (s.breaker_open_until) {
    out.push({
      level: "bad",
      text: `breaker is ${s.queue.breaker_state} until ${s.breaker_open_until}`,
    });
  }
  if (s.live_workers === 0) {
    out.push({ level: "bad", text: "no live worker is subscribed to this queue" });
  }
  if (s.saturated) {
    out.push({
      level: "warn",
      text: `all ${s.queue.max_concurrency} concurrency slots are in flight`,
    });
  }
  if (s.rate_limited) {
    out.push({
      level: "warn",
      text: `rate limit of ${s.queue.rl_limit_per_sec}/s has no tokens left`,
    });
  }
  if (ready === 0) {
    out.push({ level: "ok", text: "nothing is ready to run" });
  }

  if (out.length === 0) {
    out.push({ level: "ok", text: "no blocker found, the queue is draining" });
  }
  return out;
}

export function oldestReady(tiers: Tier[]): number | null {
  if (tiers.length === 0) return null;
  return Math.max(...tiers.map((t) => t.oldest_ready_seconds));
}

export function starving(tiers: Tier[]): Tier | null {
  const low = tiers.filter((t) => t.ready > 0).sort((a, b) => a.priority - b.priority)[0];
  const high = tiers.filter((t) => t.ready > 0).sort((a, b) => b.priority - a.priority)[0];
  if (!low || !high || low.priority === high.priority) return null;
  if (low.oldest_ready_seconds > high.oldest_ready_seconds * 4) return low;
  return null;
}
