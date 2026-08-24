# Fenceline

A job scheduler built on Postgres. Every state change is guarded by a fencing token, so a
worker that was declared dead cannot report on a job that has moved on without it. Claiming
reads a partial index over ready jobs, so its cost grows far more slowly than the table
does: fifty times the rows costs under twice the reads, measured in
[BENCHMARKS.md](BENCHMARKS.md). Jobs that reach a terminal state move to cold storage,
which keeps the working set small as history grows.

Go services and a Next.js dashboard, with Postgres as the only coordination point.

## The rest of the documentation

| | |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | the processes, the claim path, how a lost worker is handled |
| [API.md](API.md) | all fifty endpoints, with roles, payloads and error semantics |
| [SCHEMA.md](SCHEMA.md) | the tables, the keys, and why the shape is what it is |
| [DECISIONS.md](DECISIONS.md) | the trade-offs, including the ones measured and reversed |
| [BENCHMARKS.md](BENCHMARKS.md) | what was measured, on what hardware, and what it means |
| [TESTING.md](TESTING.md) | what is covered, what is not, and how to run it |
| [DIAGRAMS.md](DIAGRAMS.md) | lifecycle and entity diagrams, generated from a live database |

## What you need

Docker, Go 1.25, and Node 20 if you want the dashboard.

## Setting it up

Postgres runs in docker on 5433.

```
make db-up
make migrate
make check
```

`make db-up` waits for the container to accept connections, `make migrate` applies every
migration in order, and `make check` runs format, vet, build and the test suite. If all three
pass, the install is good.

Migrations are embedded in the binaries and take an advisory lock, so running `make migrate`
twice, or from two machines at once, is safe.

## The binaries

Four long-running services and a migrator, each taking `DATABASE_URL`:

```
go run ./cmd/api
go run ./cmd/scheduler
go run ./cmd/worker
go run ./cmd/archiver
go run ./cmd/migrate
```

| | |
|---|---|
| `api` | http and websocket, port 3001 |
| `scheduler` | promoter, cron, breaker, reaper, notifier, partitions, summaries |
| `worker` | claims and runs jobs |
| `archiver` | hot to cold, idempotency pruning |
| `migrate` | applies migrations, safe to run twice |

A worker claims from one queue, so `WORKER_QUEUE` is required and must be a queue id:

```
DATABASE_URL=... WORKER_QUEUE=<queue id> WORKER_CONCURRENCY=4 go run ./cmd/worker
```

The binary ships three handlers for smoke tests and benchmarks: `noop` returns immediately,
`sleep` waits for the milliseconds in its payload, and `fail` always errors. Real handlers are
yours to write against `worker.Run`, which [ARCHITECTURE.md](ARCHITECTURE.md) covers.

## Configuration

[.env.example](.env.example) holds the full set. Only `DATABASE_URL` is always required.

| | |
|---|---|
| `DATABASE_URL` | every binary |
| `TEST_DATABASE_URL` | the test suite, pointed at `postgres` so it can create databases |
| `API_PORT`, `API_ADDR` | where the api listens, 3001 if unset |
| `API_ALLOWED_ORIGINS` | comma separated, for websockets from another origin |
| `WORKER_QUEUE` | the queue id a worker claims from, required |
| `WORKER_CONCURRENCY` | how many jobs one worker runs at once, 4 if unset |
| `ANTHROPIC_API_KEY` | the scheduler writes failure summaries only when this is set |

Without `ANTHROPIC_API_KEY` every other sweep runs as usual and the summary sweep is a no-op.

## The dashboard

```
cd web && npm install && npm run dev
```

It proxies `/api` to `API_URL`, so http calls are same origin. The event stream connects
straight to the API, which means two variables when the dashboard is not served from the API's
own origin: `NEXT_PUBLIC_API_URL` on the dashboard, and `API_ALLOWED_ORIGINS` on the API, a
comma separated list. Without the second, only same origin streams are accepted.

## Regenerating the diagrams

```
make diagram
```

Reads a migrated database and rewrites [DIAGRAMS.md](DIAGRAMS.md): the job state machine out of
`job_transitions`, the entities out of the catalog. Neither is maintained by hand, so neither
can drift from the schema it describes.

## Running the tests

Tests need a running Postgres and `TEST_DATABASE_URL`. Each test clones its own database from
a migrated template, so they run concurrently.

```
TEST_DATABASE_URL=postgres://fenceline:fenceline@localhost:5433/postgres go test -race ./...
```

Time is injectable: `set fl.testing = 'on'` and `set fl.now = '...'` move the clock,
`set fl.jitter = 'off'` makes backoff deterministic. [TESTING.md](TESTING.md) covers
what the critical tests prove and where the coverage is thin.
