"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { token } from "@/lib/api";

export default function Home() {
  const router = useRouter();
  useEffect(() => {
    router.replace(token() ? "/queues" : "/login");
  }, [router]);
  return null;
}
