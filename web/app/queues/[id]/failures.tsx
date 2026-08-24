"use client";

import { useEffect, useState } from "react";
import { get } from "@/lib/api";

type Group = {
  error_class: string;
  count: number;
  distinct_messages: number;
  latest_message: string;
  first_seen: string;
  last_seen: string;
};

type Summary = {
  window_hours: number;
  failures: Group[];
  summary: string | null;
  model?: string;
  generated_at?: string;
  state: "current" | "stale" | "pending" | "unavailable";
};

const note: Record<Summary["state"], string> = {
  current: "",
  stale: "these failures changed since the summary was written",
  pending: "no summary yet, the scheduler writes one within a few minutes",
  unavailable: "no summary, the scheduler has no ANTHROPIC_API_KEY",
};

export default function Failures({ id }: { id: string }) {
  const [data, setData] = useState<Summary | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    const load = async () => {
      try {
        setData(await get<Summary>(`/queues/${id}/failure-summary`));
        setError("");
      } catch (err) {
        setError(err instanceof Error ? err.message : "load failed");
      }
    };
    load();
    const timer = setInterval(load, 30000);
    return () => clearInterval(timer);
  }, [id]);

  if (error) return <div className="error">{error}</div>;
  if (!data) return <p className="dim">loading</p>;
  if (data.failures.length === 0) {
    return <p className="dim">nothing failed in the last {data.window_hours} hours</p>;
  }

  return (
    <>
      {data.summary && <p>{data.summary}</p>}
      {note[data.state] && <p className="dim">{note[data.state]}</p>}
      {data.summary && data.model && (
        <p className="dim">
          written by {data.model} at{" "}
          {new Date(data.generated_at!).toLocaleString()}
        </p>
      )}
      <div className="scroll">
        <table>
          <thead>
            <tr>
              <th>error class</th>
              <th>failures</th>
              <th>distinct messages</th>
              <th>latest</th>
              <th>last seen</th>
            </tr>
          </thead>
          <tbody>
            {data.failures.map((f) => (
              <tr key={f.error_class}>
                <td>{f.error_class}</td>
                <td>{f.count}</td>
                <td>{f.distinct_messages}</td>
                <td className="wrap">{f.latest_message}</td>
                <td>{new Date(f.last_seen).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
