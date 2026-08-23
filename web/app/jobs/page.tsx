"use client";

import Link from "next/link";
import { Suspense, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { get, project } from "@/lib/api";
import { ago, clock } from "@/lib/format";

type Job = {
  id: string;
  queue_id: string;
  type: string;
  status: string;
  priority: number;
  attempt_count: number;
  run_at: string;
  created_at: string;
};

const statuses = [
  "",
  "scheduled",
  "queued",
  "claimed",
  "running",
  "completed",
  "retry_wait",
  "dead_letter",
  "cancelled",
];

function JobList() {
  const search = useSearchParams();
  const queue = search.get("queue") ?? "";
  const [status, setStatus] = useState("");
  const [jobs, setJobs] = useState<Job[]>([]);
  const [cursor, setCursor] = useState("");
  const [next, setNext] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    const p = project();
    if (!p && !queue) return;

    const params = new URLSearchParams();
    if (queue) params.set("queue", queue);
    else params.set("project", p as string);
    if (status) params.set("status", status);
    if (cursor) params.set("cursor", cursor);

    get<{ items: Job[]; next_cursor: string }>(`/jobs?${params}`)
      .then((res) => {
        setJobs(res.items);
        setNext(res.next_cursor);
        setError("");
      })
      .catch((err) => setError(err instanceof Error ? err.message : "load failed"));
  }, [queue, status, cursor]);

  return (
    <>
      <h1>jobs</h1>
      <div className="controls">
        <select
          value={status}
          onChange={(e) => {
            setCursor("");
            setStatus(e.target.value);
          }}
        >
          {statuses.map((s) => (
            <option key={s} value={s}>
              {s || "any status"}
            </option>
          ))}
        </select>
        <button disabled={!cursor} onClick={() => setCursor("")}>
          first page
        </button>
        <button disabled={!next} onClick={() => setCursor(next)}>
          next page
        </button>
      </div>
      {error && <div className="error">{error}</div>}
      <div className="scroll">
        <table>
          <thead>
            <tr>
              <th>id</th>
              <th>type</th>
              <th>status</th>
              <th>priority</th>
              <th>attempts</th>
              <th>run at</th>
              <th>age</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((j) => (
              <tr key={j.id}>
                <td>
                  <Link href={`/jobs/${j.id}`}>{j.id.slice(0, 8)}</Link>
                </td>
                <td>{j.type}</td>
                <td>{j.status}</td>
                <td>{j.priority}</td>
                <td>{j.attempt_count}</td>
                <td>{clock(j.run_at)}</td>
                <td>{ago(j.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {jobs.length === 0 && !error && <p className="dim">no jobs match</p>}
    </>
  );
}

export default function Jobs() {
  return (
    <Suspense fallback={<p className="dim">loading</p>}>
      <JobList />
    </Suspense>
  );
}
