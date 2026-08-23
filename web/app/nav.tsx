"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { get, project, setProject, setToken, token } from "@/lib/api";

type Org = { id: string; name: string };
type Project = { id: string; name: string };

export default function Nav() {
  const path = usePathname();
  const router = useRouter();
  const [projects, setProjects] = useState<Project[]>([]);
  const [current, setCurrent] = useState<string | null>(null);

  useEffect(() => {
    if (!token()) return;
    setCurrent(project());
    get<{ items: Org[] }>("/orgs")
      .then(async (orgs) => {
        const all: Project[] = [];
        for (const org of orgs.items) {
          const res = await get<{ items: Project[] }>(`/orgs/${org.id}/projects`);
          all.push(...res.items);
        }
        setProjects(all);
        if (!project() && all.length > 0) {
          setProject(all[0].id);
          setCurrent(all[0].id);
          router.refresh();
        }
      })
      .catch(() => undefined);
  }, [path, router]);

  if (path === "/login") return null;

  const pick = (id: string) => {
    setProject(id);
    setCurrent(id);
    router.refresh();
  };

  const out = () => {
    setToken(null);
    setProject(null);
    router.push("/login");
  };

  return (
    <nav>
      <span className="name">fenceline</span>
      <Link href="/queues">queues</Link>
      <Link href="/jobs">jobs</Link>
      <Link href="/workers">workers</Link>
      <Link href="/dlq">dead letter</Link>
      <Link href="/events">events</Link>
      <span className="spacer" />
      <select value={current ?? ""} onChange={(e) => pick(e.target.value)}>
        {projects.map((p) => (
          <option key={p.id} value={p.id}>
            {p.name}
          </option>
        ))}
      </select>
      <button onClick={out}>sign out</button>
    </nav>
  );
}
