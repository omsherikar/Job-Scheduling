"use client";

export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <>
      <h1>this page broke</h1>
      <div className="error">{error.message || "something threw while rendering"}</div>
      {error.digest && <p className="dim">digest {error.digest}</p>}
      <div className="controls">
        <button onClick={reset}>try again</button>
      </div>
    </>
  );
}
