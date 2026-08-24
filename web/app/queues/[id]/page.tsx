"use client";

import { use, useEffect, useState } from "react";
import Link from "next/link";
import { get, post } from "@/lib/api";
import { duration, millis } from "@/lib/format";
import { QueueStats, reasons, starving } from "@/lib/health";
import { Latency, Minute, Throughput } from "../../chart";
import Config from "./config";
import Failures from "./failures";
import Schedules from "./schedules";
import { poll } from "@/lib/poll";

export default function Queue({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const [stats, setStats] = useState<QueueStats | null>(null);
  const [series, setSeries] = useState<Minute[]>([]);
  const [error, setError] = useState("");

  const load = async () => {
    try {
      const [s, hist] = await Promise.all([
        get<QueueStats>(`/stats/queues/${id}`),
        get<{ items: Minute[] }>(`/stats/queues/${id}/series?minutes=60`),
      ]);
      setStats(s);
      setSeries(hist.items);
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "load failed");
    }
  };

  useEffect(() => {
    return poll(load, 5000);
  }, [id]);

  if (error) return <div className="error">{error}</div>;
  if (!stats) return <p className="dim">loading</p>;

  const toggle = async () => {
    await post(`/queues/${id}/${stats.queue.paused ? "resume" : "pause"}`);
    load();
  };

  const starved = starving(stats.tiers);

  return (
    <>
      <h1>{stats.queue.name}</h1>

      <div className="controls">
        <button onClick={toggle}>
          {stats.queue.paused ? "resume" : "pause"}
        </button>
        <Config queue={stats.queue} onSaved={load} />
        <Link href={`/jobs?queue=${id}`}>jobs in this queue</Link>
      </div>

      <h2>why</h2>
      {reasons(stats).map((r) => (
        <div
          key={r.text}
          className={r.level === "ok" ? "reason clear" : "reason"}
        >
          <span className={r.level}>{r.text}</span>
        </div>
      ))}

      <div className="cards">
        <div className="card">
          <div className="label">oldest ready</div>
          <div className="value">{duration(stats.oldest_ready_seconds)}</div>
        </div>
        <div className="card">
          <div className="label">in flight</div>
          <div className="value">
            {stats.queue.in_flight}/{stats.queue.max_concurrency}
            {stats.queue.shards > 1 && (
              <span className="dim"> over {stats.queue.shards} shards</span>
            )}
          </div>
        </div>
        <div className="card">
          <div className="label">live workers</div>
          <div className="value">{stats.live_workers}</div>
        </div>
        <div className="card">
          <div className="label">p50</div>
          <div className="value">{millis(stats.duration_ms_p50)}</div>
        </div>
        <div className="card">
          <div className="label">p95</div>
          <div className="value">{millis(stats.duration_ms_p95)}</div>
        </div>
      </div>

      <h2>oldest ready by priority</h2>
      {starved && (
        <div className="reason">
          <span className="bad">
            priority {starved.priority} has waited{" "}
            {duration(starved.oldest_ready_seconds)} while higher tiers drain,
            which is starvation
          </span>
        </div>
      )}
      <div className="scroll">
        <table>
          <thead>
            <tr>
              <th>priority</th>
              <th>ready</th>
              <th>oldest</th>
            </tr>
          </thead>
          <tbody>
            {stats.tiers.map((t) => (
              <tr key={t.priority}>
                <td>{t.priority}</td>
                <td>{t.ready}</td>
                <td>{duration(t.oldest_ready_seconds)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {stats.tiers.length === 0 && <p className="dim">nothing ready</p>}

      <Schedules queueID={id} />

      <h2>throughput, last hour</h2>
      <Throughput series={series} />

      <h2>p95 duration, last hour</h2>
      <Latency series={series} />

      <h2>what is failing</h2>
      <Failures id={id} />

      <h2>jobs by status</h2>
      <div className="scroll">
        <table>
          <tbody>
            {Object.entries(stats.status_counts).map(([status, n]) => (
              <tr key={status}>
                <td>{status}</td>
                <td>{n}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
