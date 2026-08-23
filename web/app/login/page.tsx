"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { post, setToken } from "@/lib/api";

export default function Login() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    try {
      const res = await post<{ token: string }>("/auth/login", { email, password });
      setToken(res.token);
      router.push("/queues");
    } catch (err) {
      setError(err instanceof Error ? err.message : "sign in failed");
    }
  };

  return (
    <form className="login" onSubmit={submit}>
      <h1>sign in</h1>
      {error && <div className="error">{error}</div>}
      <label>
        email
        <input value={email} onChange={(e) => setEmail(e.target.value)} autoComplete="username" />
      </label>
      <label>
        password
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoComplete="current-password"
        />
      </label>
      <button type="submit">sign in</button>
    </form>
  );
}
