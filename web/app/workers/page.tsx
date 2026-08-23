"use client";

import { useEffect, useState } from "react";
import { get, project } from "@/lib/api";
import { ago } from "@/lib/format";

type Worker = {
  id: string;
  hostname: string;
  pid: number;
  state: string;
  max_concurrency: number;
  leases: number;
  started_at: string;
  last_seen_at: string;
};

export default function Workers() {
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [error, setError] = useState("");

  useEffect(() => {
    const p = project();
    if (!p) return;

    const load = () =>
      get<{ items: Worker[] }>(`/workers?project=${p}`)
        .then((res) => {
          setWorkers(res.items);
          setError("");
        })
        .catch((err) => setError(err instanceof Error ? err.message : "load failed"));

    load();
    const timer = setInterval(load, 5000);
    return () => clearInterval(timer);
  }, []);

  const stale = (seen: string) => Date.now() - new Date(seen).getTime() > 30000;

  return (
    <>
      <h1>workers</h1>
      {error && <div className="error">{error}</div>}
      <table>
        <thead>
          <tr>
            <th>host</th>
            <th>pid</th>
            <th>state</th>
            <th>leases</th>
            <th>capacity</th>
            <th>up</th>
            <th>last seen</th>
          </tr>
        </thead>
        <tbody>
          {workers.map((w) => (
            <tr key={w.id}>
              <td>{w.hostname}</td>
              <td>{w.pid}</td>
              <td className={w.state === "active" ? "ok" : "warn"}>{w.state}</td>
              <td>{w.leases}</td>
              <td>{w.max_concurrency}</td>
              <td>{ago(w.started_at)}</td>
              <td className={stale(w.last_seen_at) ? "bad" : ""}>{ago(w.last_seen_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {workers.length === 0 && !error && <p className="dim">no workers registered</p>}
    </>
  );
}
