import Link from "next/link";

export default function NotFound() {
  return (
    <>
      <h1>no such page</h1>
      <p className="dim">
        <Link href="/queues">back to queues</Link>
      </p>
    </>
  );
}
