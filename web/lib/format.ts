export function duration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined) return "-";
  const s = Math.floor(seconds);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}

export function millis(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return "-";
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

export function ago(iso: string | null | undefined): string {
  if (!iso) return "-";
  return duration((Date.now() - new Date(iso).getTime()) / 1000);
}

export function clock(iso: string | null | undefined): string {
  if (!iso) return "-";
  return new Date(iso).toISOString().replace("T", " ").slice(0, 19);
}
