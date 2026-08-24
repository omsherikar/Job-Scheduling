export function poll(fn: () => Promise<void>, every: number) {
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const tick = async () => {
    await fn();
    if (stopped) return;
    timer = setTimeout(tick, every);
  };
  tick();

  return () => {
    stopped = true;
    if (timer) clearTimeout(timer);
  };
}
