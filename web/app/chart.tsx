"use client";

import { millis } from "@/lib/format";

export type Minute = {
  minute: string;
  completed: number;
  failed: number;
  dead_lettered: number;
  duration_ms_p50: number | null;
  duration_ms_p95: number | null;
};

export function Throughput({ series }: { series: Minute[] }) {
  const peak = Math.max(1, ...series.map((m) => m.completed + m.failed + m.dead_lettered));

  return (
    <>
      <div className="bars">
        {series.map((m) => {
          const total = m.completed + m.failed + m.dead_lettered;
          const bad = m.failed + m.dead_lettered;
          return (
            <div
              key={m.minute}
              className="bar"
              title={`${m.minute} ${m.completed} done, ${bad} failed`}
              style={{
                height: `${(total / peak) * 100}%`,
                background: bad > m.completed ? "var(--bad)" : "var(--accent)",
              }}
            />
          );
        })}
      </div>
      <p className="dim">
        peak {peak} per minute over {series.length} minutes
      </p>
    </>
  );
}

export function Latency({ series }: { series: Minute[] }) {
  const points = series.filter((m) => m.duration_ms_p95 !== null);
  const peak = Math.max(1, ...points.map((m) => m.duration_ms_p95 as number));

  if (points.length === 0) return <p className="dim">no completed attempts yet</p>;

  return (
    <>
      <div className="bars">
        {points.map((m) => (
          <div
            key={m.minute}
            className="bar"
            title={`${m.minute} p95 ${millis(m.duration_ms_p95)}`}
            style={{ height: `${((m.duration_ms_p95 as number) / peak) * 100}%` }}
          />
        ))}
      </div>
      <p className="dim">peak p95 {millis(peak)}</p>
    </>
  );
}
