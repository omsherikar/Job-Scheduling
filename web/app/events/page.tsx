"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { project, token } from "@/lib/api";
import { clock } from "@/lib/format";
import { StreamEvent, StreamState, openStream } from "@/lib/stream";

const keep = 200;

export default function Events() {
  const [events, setEvents] = useState<StreamEvent[]>([]);
  const [state, setState] = useState<StreamState>("connecting");
  const [gaps, setGaps] = useState(0);
  const seen = useRef(0);

  useEffect(() => {
    const p = project();
    const t = token();
    if (!p || !t) return;

    return openStream(p, t, {
      onEvents: (batch) => {
        seen.current += batch.length;
        setEvents((prev) => [...batch].reverse().concat(prev).slice(0, keep));
      },
      onState: setState,
      onGap: () => setGaps((n) => n + 1),
    });
  }, []);

  const label = state === "live" ? "ok" : state === "gap" ? "bad" : "warn";

  return (
    <>
      <h1>event stream</h1>
      <div className="controls">
        <span className={`tag ${label}`}>{state}</span>
        <span className="dim">{seen.current} received</span>
        {gaps > 0 && <span className="bad">{gaps} gaps, history was dropped</span>}
      </div>
      <div className="scroll">
        <table>
          <thead>
            <tr>
              <th>id</th>
              <th>at</th>
              <th>topic</th>
              <th>entity</th>
            </tr>
          </thead>
          <tbody>
            {events.map((e) => (
              <tr key={e.id}>
                <td>{e.id}</td>
                <td>{clock(e.created_at)}</td>
                <td>{e.topic}</td>
                <td>
                  <Link href={`/jobs/${e.entity_id}`}>{e.entity_id.slice(0, 8)}</Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {events.length === 0 && <p className="dim">waiting for events</p>}
    </>
  );
}
