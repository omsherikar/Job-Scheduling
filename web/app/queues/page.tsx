"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { get, project } from "@/lib/api";
import { duration } from "@/lib/format";
import { QueueHealth, oldestReady, reasons } from "@/lib/health";

export default function Queues() {
  const [stats, setStats] = useState<QueueHealth[]>([]);
  const [error, setError] = useState("");

  useEffect(() => {
    const p = project();
    if (!p) return;

    const load = async () => {
      try {
        const res = await get<{ items: QueueHealth[] }>(`/projects/${p}/queue-health`);
        setStats(res.items);
        setError("");
      } catch (err) {
        setError(err instanceof Error ? err.message : "load failed");
      }
    };

    load();
    const timer = setInterval(load, 5000);
    return () => clearInterval(timer);
  }, []);

  return (
    <>
      <h1>queue health</h1>
      {error && <div className="error">{error}</div>}
      <div className="scroll">
        <table>
          <thead>
            <tr>
              <th>queue</th>
              <th>oldest ready</th>
              <th>ready</th>
              <th>in flight</th>
              <th>workers</th>
              <th>last hour</th>
              <th>p95</th>
              <th>state</th>
            </tr>
          </thead>
          <tbody>
            {stats.map((s) => {
              const ready = s.tiers.reduce((n, t) => n + t.ready, 0);
              const oldest = oldestReady(s.tiers);
              const worst = reasons(s)[0];
              return (
                <tr key={s.queue.id}>
                  <td>
                    <Link href={`/queues/${s.queue.id}`}>{s.queue.name}</Link>
                  </td>
                  <td className={oldest !== null && oldest > 300 ? "bad" : ""}>
                    {duration(oldest)}
                  </td>
                  <td>{ready}</td>
                  <td>
                    {s.queue.in_flight}/{s.queue.max_concurrency}
                  </td>
                  <td className={s.live_workers === 0 ? "bad" : ""}>{s.live_workers}</td>
                  <td>
                    <span className="ok">{s.last_hour.completed}</span>
                    {" / "}
                    <span className="warn">{s.last_hour.failed}</span>
                    {" / "}
                    <span className="bad">{s.last_hour.dead_lettered}</span>
                  </td>
                  <td>{s.duration_ms_p95 ?? "-"}</td>
                  <td className={worst.level}>{worst.text}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {stats.length === 0 && !error && <p className="dim">no queues in this project</p>}
    </>
  );
}
