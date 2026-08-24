"use client";

import { use, useEffect, useState } from "react";
import Link from "next/link";
import { get, post } from "@/lib/api";
import { clock, millis } from "@/lib/format";
import { poll } from "@/lib/poll";

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

type Kin = { id: string; type: string; status: string };

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
  const [parents, setParents] = useState<Kin[]>([]);
  const [children, setChildren] = useState<Kin[]>([]);
  const [error, setError] = useState("");

  const load = async () => {
    try {
      const res = await get<{
        job: Job;
        executions: Execution[];
        lease?: Lease;
        parents?: Kin[];
        children?: Kin[];
      }>(`/jobs/${id}`);
      setJob(res.job);
      setExecs(res.executions);
      setLease(res.lease ?? null);
      setParents(res.parents ?? []);
      setChildren(res.children ?? []);
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "load failed");
    }
  };

  useEffect(() => {
    return poll(load, 3000);
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

  const terminal = done(job.status);

  return (
    <>
      <h1>
        {job.type} <span className="dim">{job.id}</span>
      </h1>

      <div className="controls">
        <button disabled={terminal} onClick={() => act("cancel")}>
          cancel
        </button>
        <button
          disabled={job.status !== "retry_wait" && job.status !== "scheduled"}
          onClick={() => act("retry")}
        >
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

      {parents.length > 0 && (
        <>
          <h2>waiting on</h2>
          {job.status === "scheduled" && (
            <div className="reason">
              <span className="bad">
                {parents.filter((p) => !done(p.status)).length} of {parents.length} parents have
                not finished
              </span>
            </div>
          )}
          <KinTable rows={parents} />
        </>
      )}

      {children.length > 0 && (
        <>
          <h2>blocks</h2>
          <KinTable rows={children} />
        </>
      )}

      <h2>payload</h2>
      <pre>{JSON.stringify(job.payload, null, 2)}</pre>

      <h2>attempts</h2>
      <div className="scroll">
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
      </div>
      {execs.length === 0 && <p className="dim">no attempts yet</p>}

      {execs
        .filter((e) => e.logs.length > 0)
        .map((e) => (
          <div key={e.id}>
            <h2>logs for attempt {e.attempt}</h2>
            <pre>{e.logs.map((l) => `${clock(l.ts)} ${l.level} ${l.message}`).join("\n")}</pre>
          </div>
        ))}
    </>
  );
}

function done(status: string) {
  return status === "completed" || status === "cancelled" || status === "dead_letter";
}

function KinTable({ rows }: { rows: Kin[] }) {
  return (
    <div className="scroll">
      <table>
        <thead>
          <tr>
            <th>id</th>
            <th>type</th>
            <th>status</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((k) => (
            <tr key={k.id}>
              <td>
                <Link href={`/jobs/${k.id}`}>{k.id.slice(0, 8)}</Link>
              </td>
              <td>{k.type}</td>
              <td className={k.status === "completed" ? "ok" : done(k.status) ? "bad" : "dim"}>
                {k.status}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
