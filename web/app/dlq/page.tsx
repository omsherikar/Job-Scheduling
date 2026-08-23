"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { get, post, project } from "@/lib/api";
import { ago } from "@/lib/format";

type Dead = {
  job_id: string;
  queue_id: string;
  reason: string;
  last_error_class: string | null;
  last_error_message: string | null;
  dead_at: string;
  replayed_at: string | null;
};

export default function Dlq() {
  const [items, setItems] = useState<Dead[]>([]);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState("");

  const load = () => {
    const p = project();
    if (!p) return;
    get<{ items: Dead[] }>(`/dlq?project=${p}`)
      .then((res) => {
        setItems(res.items);
        setError("");
      })
      .catch((err) => setError(err instanceof Error ? err.message : "load failed"));
  };

  useEffect(load, []);

  const replay = async (jobID: string) => {
    setBusy(jobID);
    try {
      await post(`/dlq/${jobID}/replay`);
      load();
    } catch (err) {
      setError(err instanceof Error ? err.message : "replay failed");
    } finally {
      setBusy("");
    }
  };

  return (
    <>
      <h1>dead letter queue</h1>
      {error && <div className="error">{error}</div>}
      <table>
        <thead>
          <tr>
            <th>job</th>
            <th>reason</th>
            <th>error</th>
            <th>dead</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {items.map((d) => (
            <tr key={d.job_id}>
              <td>
                <Link href={`/jobs/${d.job_id}`}>{d.job_id.slice(0, 8)}</Link>
              </td>
              <td>{d.reason}</td>
              <td>{d.last_error_message ?? "-"}</td>
              <td>{ago(d.dead_at)}</td>
              <td>
                {d.replayed_at ? (
                  <span className="dim">replayed</span>
                ) : (
                  <button disabled={busy === d.job_id} onClick={() => replay(d.job_id)}>
                    replay
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {items.length === 0 && !error && <p className="dim">nothing dead lettered</p>}
    </>
  );
}
