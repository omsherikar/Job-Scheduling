export type StreamEvent = {
  id: number;
  topic: string;
  entity_id: string;
  payload: unknown;
  created_at: string;
};

type Frame = {
  type: string;
  prev_id: number;
  next?: number;
  more?: boolean;
  oldest_available?: number;
  events?: StreamEvent[];
};

export type StreamState = "connecting" | "live" | "reconnecting" | "gap";

type Handlers = {
  onEvents: (events: StreamEvent[]) => void;
  onState: (state: StreamState) => void;
  onGap: () => void;
};

const apiOrigin = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:3001";

export function openStream(projectId: string, token: string, h: Handlers): () => void {
  let socket: WebSocket | null = null;
  let cursor = 0;
  let closed = false;
  let retry = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const connect = () => {
    if (closed) return;
    h.onState("connecting");

    const url = new URL(`/projects/${projectId}/events/stream`, apiOrigin);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("after", String(cursor));

    socket = new WebSocket(url.toString(), [`fl.token.${token}`]);

    socket.onopen = () => {
      retry = 0;
      h.onState("live");
    };

    socket.onmessage = (ev) => {
      let frame: Frame;
      try {
        frame = JSON.parse(ev.data) as Frame;
      } catch {
        return;
      }

      if (frame.type === "cursor_too_old") {
        cursor = frame.oldest_available ? frame.oldest_available - 1 : 0;
        h.onGap();
        h.onState("gap");
        return;
      }

      if (frame.prev_id !== cursor) {
        cursor = frame.next ?? cursor;
        h.onGap();
        h.onState("gap");
        if (frame.events?.length) h.onEvents(frame.events);
        return;
      }

      if (frame.events?.length) {
        h.onEvents(frame.events);
        cursor = frame.next ?? cursor;
      }
      h.onState("live");
    };

    socket.onclose = () => {
      if (closed) return;
      h.onState("reconnecting");
      retry = Math.min(retry + 1, 5);
      timer = setTimeout(connect, retry * 1000 + Math.random() * 500);
    };

    socket.onerror = () => socket?.close();
  };

  connect();

  return () => {
    closed = true;
    if (timer) clearTimeout(timer);
    socket?.close();
  };
}
