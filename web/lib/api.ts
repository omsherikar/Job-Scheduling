const tokenKey = "fl.token";
const projectKey = "fl.project";

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

  const res = await fetch(`/api${path}`, { ...init, headers, cache: "no-store" });
  const text = await res.text();
  const body = text ? JSON.parse(text) : null;

  if (!res.ok) {
    if (res.status === 401 && t) {
      setToken(null);
      window.location.href = "/login";
    }
    throw new Error(body?.detail ?? body?.title ?? res.statusText);
  }
  return body as T;
}

export function get<T>(path: string) {
  return api<T>(path);
}

export function post<T>(path: string, body?: unknown) {
  return api<T>(path, { method: "POST", body: body ? JSON.stringify(body) : undefined });
}
