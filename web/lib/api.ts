const tokenKey = "fl.token";
const projectKey = "fl.project";
const timeoutMs = 10000;

export function token(): string | null {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem(tokenKey);
}

export function setToken(value: string | null) {
  if (value === null) window.localStorage.removeItem(tokenKey);
  else window.localStorage.setItem(tokenKey, value);
}

export function project(): string | null {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem(projectKey);
}

export function setProject(value: string | null) {
  if (value === null) window.localStorage.removeItem(projectKey);
  else window.localStorage.setItem(projectKey, value);
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  const t = token();
  if (t) headers.set("Authorization", `Bearer ${t}`);
  if (init.body) headers.set("Content-Type", "application/json");

  let res: Response;
  try {
    res = await fetch(`/api${path}`, {
      ...init,
      headers,
      cache: "no-store",
      signal: init.signal ?? AbortSignal.timeout(timeoutMs),
    });
  } catch (err) {
    if (err instanceof DOMException && err.name === "TimeoutError") {
      throw new Error(`${path} timed out after ${timeoutMs / 1000}s`);
    }
    throw new Error(`cannot reach the api: ${path}`);
  }

  const text = await res.text();
  let body: unknown = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = null;
    }
  }

  if (!res.ok) {
    if (res.status === 401 && t) {
      setToken(null);
      window.location.href = "/login";
    }
    const problem = body as { detail?: string; title?: string } | null;
    throw new Error(
      problem?.detail ??
        problem?.title ??
        `${res.status} ${res.statusText || "request failed"}`,
    );
  }
  return body as T;
}

export function get<T>(path: string) {
  return api<T>(path);
}

export function post<T>(path: string, body?: unknown) {
  return api<T>(path, { method: "POST", body: body ? JSON.stringify(body) : undefined });
}
