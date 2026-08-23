"use client";

import { useState } from "react";
import { api } from "@/lib/api";
import { Queue } from "@/lib/health";

const fields = [
  { key: "max_concurrency", label: "max concurrency" },
  { key: "rl_limit_per_sec", label: "rate limit per second" },
  { key: "rl_burst", label: "rate burst" },
  { key: "default_priority", label: "default priority" },
] as const;

export default function Config({ queue, onSaved }: { queue: Queue; onSaved: () => void }) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<Record<string, string>>({});
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const start = () => {
    setDraft({
      max_concurrency: String(queue.max_concurrency),
      rl_limit_per_sec: String(queue.rl_limit_per_sec),
      rl_burst: String(queue.rl_burst ?? 0),
      default_priority: String(queue.default_priority ?? 0),
    });
    setError("");
    setOpen(true);
  };

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const body: Record<string, number> = {};
      for (const f of fields) {
        const n = Number(draft[f.key]);
        if (!Number.isFinite(n)) {
          throw new Error(`${f.label} must be a number`);
        }
        body[f.key] = n;
      }
      await api(`/queues/${queue.id}`, {
        method: "PATCH",
        body: JSON.stringify(body),
      });
      setOpen(false);
      onSaved();
    } catch (err) {
      setError(err instanceof Error ? err.message : "save failed");
    } finally {
      setBusy(false);
    }
  };

  if (!open) {
    return <button onClick={start}>configure</button>;
  }

  return (
    <form className="panel" onSubmit={save}>
      {error && <div className="error">{error}</div>}
      {fields.map((f) => (
        <label key={f.key} className="field">
          {f.label}
          <input
            value={draft[f.key] ?? ""}
            onChange={(e) => setDraft({ ...draft, [f.key]: e.target.value })}
          />
        </label>
      ))}
      <div className="controls">
        <button type="submit" disabled={busy}>
          save
        </button>
        <button type="button" onClick={() => setOpen(false)}>
          cancel
        </button>
      </div>
    </form>
  );
}
