"use client";

import { use, useEffect, useState } from "react";
import Link from "next/link";
import { get, post } from "@/lib/api";
import { clock, millis } from "@/lib/format";

type Job = {
  id: string;
  queue_id: string;
  type: string;
  status: string;
  priority: number;
  fence: number;
  attempt_count: number;
  replay_generation: number;
  payload: unknown;
  run_at: string;
  created_at: string;
  finished_at: string | null;
};

type Lease = { progress: unknown; expires_at: string };

type Log = { id: number; ts: string; level: string; message: string };

type Execution = {
  id: string;
  attempt: number;
  replay_generation: number;
  fence: number;
  worker_id: string;
  outcome: string | null;
  error_class: string | null;
  error_message: string | null;
  started_at: string;
  finished_at: string | null;
  duration_ms: number | null;
  logs: Log[];
};

export default function JobDetail({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const [job, setJob] = useState<Job | null>(null);
  const [execs, setExecs] = useState<Execution[]>([]);
  const [lease, setLease] = useState<Lease | null>(null);
  const [error, setError] = useState("");

  const load = async () => {
    try {
      const res = await get<{ job: Job; executions: Execution[]; lease?: Lease }>(`/jobs/${id}`);
      setJob(res.job);
      setExecs(res.executions);
      setLease(res.lease ?? null);
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "load failed");
    }
  };

  useEffect(() => {
    load();
    const timer = setInterval(load, 3000);
    return () => clearInterval(timer);
  }, [id]);

  if (error) return <div className="error">{error}</div>;
  if (!job) return <p className="dim">loading</p>;

  const act = async (path: string) => {
    try {
      await post(`/jobs/${id}/${path}`);
      load();
    } catch (err) {
      setError(err instanceof Error ? err.message : "action failed");
    }
  };

  const terminal = ["completed", "dead_letter", "cancelled"].includes(job.status);

  return (
    <>
      <h1>
        {job.type} <span className="dim">{job.id}</span>
      </h1>

      <div className="controls">
        <button disabled={terminal} onClick={() => act("cancel")}>
          cancel
        </button>
        <button disabled={job.status !== "retry_wait" && job.status !== "scheduled"} onClick={() => act("retry")}>
          run now
        </button>
        <Link href={`/queues/${job.queue_id}`}>queue</Link>
      </div>

      <div className="cards">
        <div className="card">
          <div className="label">status</div>
          <div className="value">{job.status}</div>
        </div>
        <div className="card">
          <div className="label">attempts</div>
          <div className="value">{job.attempt_count}</div>
        </div>
        <div className="card">
          <div className="label">fence</div>
          <div className="value">{job.fence}</div>
        </div>
        <div className="card">
          <div className="label">replay</div>
          <div className="value">{job.replay_generation}</div>
        </div>
      </div>

      {lease && lease.progress !== null && (
        <>
          <h2>progress, lease expires {clock(lease.expires_at)}</h2>
          <pre>{JSON.stringify(lease.progress, null, 2)}</pre>
        </>
      )}

      <h2>payload</h2>
      <pre>{JSON.stringify(job.payload, null, 2)}</pre>

      <h2>attempts</h2>
      <table>
        <thead>
          <tr>
            <th>attempt</th>
            <th>gen</th>
            <th>fence</th>
            <th>outcome</th>
            <th>started</th>
            <th>duration</th>
            <th>error</th>
          </tr>
        </thead>
        <tbody>
          {execs.map((e) => (
            <tr key={e.id}>
              <td>{e.attempt}</td>
              <td>{e.replay_generation}</td>
              <td>{e.fence}</td>
              <td className={e.outcome === "success" ? "ok" : e.outcome ? "bad" : "dim"}>
                {e.outcome ?? "running"}
              </td>
              <td>{clock(e.started_at)}</td>
              <td>{millis(e.duration_ms)}</td>
              <td>{e.error_message ?? "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {execs.length === 0 && <p className="dim">no attempts yet</p>}

      {execs
        .filter((e) => e.logs.length > 0)
        .map((e) => (
          <div key={e.id}>
            <h2>logs for attempt {e.attempt}</h2>
            <pre>
              {e.logs.map((l) => `${clock(l.ts)} ${l.level} ${l.message}`).join("\n")}
            </pre>
          </div>
        ))}
    </>
  );
}
