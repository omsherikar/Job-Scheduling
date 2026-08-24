"use client";

import { useEffect, useState } from "react";
import { api, get, post } from "@/lib/api";
import { clock } from "@/lib/format";

type Schedule = {
  id: string;
  name: string;
  cron_expr: string;
  timezone: string;
  job_type: string;
  enabled: boolean;
  overlap_policy: string;
  catchup_policy: string;
  next_run_at: string;
  last_fired_for: string | null;
};

const blank = { name: "", cron_expr: "", timezone: "UTC", job_type: "" };

export default function Schedules({ queueID }: { queueID: string }) {
  const [rows, setRows] = useState<Schedule[]>([]);
  const [draft, setDraft] = useState(blank);
  const [open, setOpen] = useState(false);
  const [error, setError] = useState("");

  const load = () =>
    get<{ items: Schedule[] }>(`/queues/${queueID}/schedules`)
      .then((res) => {
        setRows(res.items);
        setError("");
      })
      .catch((err) =>
        setError(err instanceof Error ? err.message : "load failed"),
      );

  useEffect(() => {
    load();
  }, [queueID]);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    try {
      await post(`/queues/${queueID}/schedules`, draft);
      setDraft(blank);
      setOpen(false);
      load();
    } catch (err) {
      setError(err instanceof Error ? err.message : "create failed");
    }
  };

  const act = async (id: string, what: string) => {
    setError("");
    try {
      if (what === "delete")
        await api(`/schedules/${id}`, { method: "DELETE" });
      else await post(`/schedules/${id}/${what}`);
      load();
    } catch (err) {
      setError(err instanceof Error ? err.message : `${what} failed`);
    }
  };

  return (
    <>
      <h2>schedules</h2>
      {error && <div className="error">{error}</div>}

      <div className="controls">
        <button onClick={() => setOpen(!open)}>
          {open ? "cancel" : "new schedule"}
        </button>
      </div>

      {open && (
        <form className="panel" onSubmit={create}>
          {(["name", "cron_expr", "job_type", "timezone"] as const).map((k) => (
            <label key={k} className="field">
              {k.replace("_", " ")}
              <input
                value={draft[k]}
                onChange={(e) => setDraft({ ...draft, [k]: e.target.value })}
              />
            </label>
          ))}
          <button type="submit">create</button>
        </form>
      )}

      {rows.length === 0 ? (
        <p className="dim">no schedules on this queue</p>
      ) : (
        <div className="scroll">
          <table>
            <thead>
              <tr>
                <th>name</th>
                <th>cron</th>
                <th>timezone</th>
                <th>type</th>
                <th>next run</th>
                <th>state</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((s) => (
                <tr key={s.id}>
                  <td>{s.name}</td>
                  <td>{s.cron_expr}</td>
                  <td>{s.timezone}</td>
                  <td>{s.job_type}</td>
                  <td>{clock(s.next_run_at)}</td>
                  <td className={s.enabled ? "ok" : "warn"}>
                    {s.enabled ? "enabled" : "paused"}
                  </td>
                  <td>
                    <button
                      onClick={() => act(s.id, s.enabled ? "pause" : "resume")}
                    >
                      {s.enabled ? "pause" : "resume"}
                    </button>{" "}
                    <button onClick={() => act(s.id, "delete")}>delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}
