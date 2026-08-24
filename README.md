# Fenceline

A job scheduler built on Postgres. Every state change is guarded by a fencing token, so a
worker that was declared dead cannot report on a job that has moved on without it. Claiming
reads a partial index that holds only the jobs eligible to run, so it does not scan the
table. Jobs that reach a terminal state move to cold tables.

Go services and a Next.js dashboard, with Postgres as the only coordination point.
[BENCHMARKS.md](BENCHMARKS.md) has what that costs, measured.

## What it does

**Submitting work**

| | |
|---|---|
| immediate | `POST /queues/{id}/jobs` and a worker claims it on the next poll or notify |
| delayed | set `run_at` and it waits in `scheduled` until the promoter releases it |
| recurring | five field cron with an IANA timezone, one job per tick however many schedulers run |
| batch | up to 1000 jobs in one transaction, with a batch progress endpoint |
| dependent | `depends_on` holds a job until every parent finishes, cycles rejected at submission |
| idempotent | an `Idempotency-Key` per queue, honoured for 24 hours |

**Not losing work**

| | |
|---|---|
| fencing tokens | every write carries the fence it claimed under, so a stale worker changes nothing |
| leases and a reaper | recovery is bounded by the lease, not by noticing a worker died |
| retry policies | fixed, linear or exponential backoff, with jitter and an attempt ceiling |
| snooze | a handler can defer a job without burning an attempt |
| dead letter queue | exhausted jobs keep their full attempt history, replayable one at a time or in bulk |
| circuit breaker | a failing queue stops itself and probes one job at a time while half open |
| transactional outbox | events are written in the same transaction as the state change |
| advisory locks | used where ordering or single execution matters: the outbox, partition upkeep, migrations |
| archiver | terminal jobs and their ledger move to cold tables instead of being deleted |

**Controlling throughput**

| | |
|---|---|
| per queue concurrency | a hard cap enforced in the database, not in the worker |
| rate limiting | a token bucket applied in the claim gate |
| shards | split the concurrency counter across rows when one row becomes the limit |
| priority and aging | higher priority first, with a sweep that lifts jobs that have waited too long |
| depth cap | an advisory bound on how much can be queued at once |
| pause and resume | per queue and per schedule, without deleting anything |

**Running jobs**

| | |
|---|---|
| worker library | `worker.Run` takes your handlers and runs the claim, execute, report loop |
| capability routing | a worker claims only the job types it announced, so new types can be enqueued early |
| lease extension | long jobs keep their lease and report progress on the heartbeat |
| graceful drain | SIGTERM finishes what is running and releases the rest |
| batched completion | completions are grouped to cut round trips |
| wake on notify | `pg_notify` when the event tail moves, so a worker does not wait out its poll |

**Seeing what happened**

| | |
|---|---|
| execution ledger | one row per attempt, never overwritten, with per attempt logs |
| event stream | websocket or cursor paging, with gap detection and replay from an id |
| queue health | depth by priority tier, live workers, saturation, breaker state, last hour outcomes |
| metrics | per minute throughput and duration percentiles from a rollup, not from the ledger |
| operational views | queue age, fenced write count, and reaper lag, read from the database |
| audit log | every privileged mutation records who did what to which entity |
| failure summaries | recent failures grouped by error class, with a written summary when a key is set |
| dashboard | queues, job explorer, workers, dead letter, and the live event stream |

**Access**

| | |
|---|---|
| accounts | argon2id passwords, sessions as rows so logout revokes |
| tenancy | users, organizations, projects, queues, checked by join on every request |
| roles | owner, admin, member and viewer, held per organization |

## Bonus features

| | |
|---|---|
| workflow dependencies | [API.md](API.md) for the cancel and replay rules |
| rate limiting | a token bucket in `fl.queue_admit`, at the claim gate |
| distributed locking | [ARCHITECTURE.md](ARCHITECTURE.md) for the lock order and where each lock is taken |
| queue sharding | [BENCHMARKS.md](BENCHMARKS.md) for the throughput against shard count |
| event-driven execution | [BENCHMARKS.md](BENCHMARKS.md) for enqueue to start with and without notify |
| websocket live updates | [API.md](API.md) for subprotocol auth and gap detection |
| role-based access control | [API.md](API.md) for what each role may do |
| ai failure summaries | the only one that calls anything outside Postgres, see Configuration |

## The rest of the documentation

| | |
|---|---|
| [RUNBOOK.md](RUNBOOK.md) | a walkthrough from empty database to a job running, with expected output |
| [ARCHITECTURE.md](ARCHITECTURE.md) | the processes, the claim path, how a lost worker is handled |
| [API.md](API.md) | all fifty endpoints, with roles, payloads and error semantics |
| [SCHEMA.md](SCHEMA.md) | the tables, the keys, and why the shape is what it is |
| [DECISIONS.md](DECISIONS.md) | the trade-offs, including the ones measured and reversed |
| [BENCHMARKS.md](BENCHMARKS.md) | what was measured, on what hardware, and what it means |
| [TESTING.md](TESTING.md) | what is covered, what is not, and how to run it |
| [DIAGRAMS.md](DIAGRAMS.md) | lifecycle and entity diagrams, generated from a live database |

## What you need

Docker and Go 1.25, plus Node 22 if you want the dashboard. Those are the versions CI
builds against.

On Windows the services and the dashboard run fine, but `make` and the shell used
throughout the docs do not. Use WSL2 and everything below works unchanged. The native
PowerShell route is in [RUNBOOK.md](RUNBOOK.md).

## Quick setup

```
git clone https://github.com/shrutu0929/Job-Scheduling.git
cd Job-Scheduling

make db-up
make migrate
make check
```

Postgres comes up in docker on 5433. `make db-up` waits for it to accept connections,
`make migrate` applies every migration in order, and `make check` runs format, vet,
build and the test suite. Migrations are embedded in the binaries and take an advisory
lock, so two of them running at once is safe.

That gets the repository building against a real database, which is as far as setup
goes. Running the system is a separate thing, and it does not start with the services:
a worker needs a queue to claim from before it can start at all.

[RUNBOOK.md](RUNBOOK.md) is a self-contained walkthrough that does all of it from the
database up, with the output you should see at each step. The sections below are the
reference for the pieces it uses.

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
| `API_URL` | where the dashboard proxies `/api`, server side, 3001 if unset |
| `NEXT_PUBLIC_API_URL` | where the browser opens the websocket, 3001 if unset |
| `ANTHROPIC_API_KEY` | the scheduler writes failure summaries only when this is set |

Without `ANTHROPIC_API_KEY` every other scheduler sweep runs as usual, the summary sweep does
nothing, and `GET /queues/{id}/failure-summary` still returns the grouped failures with `state`
reporting `unavailable`.

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

A worker claims from one queue, so `WORKER_QUEUE` is required and must be the id of a
queue that already exists. Create one over the api first; [RUNBOOK.md](RUNBOOK.md) walks
through it.

```
DATABASE_URL=... WORKER_QUEUE=<queue id> WORKER_CONCURRENCY=4 go run ./cmd/worker
```

The binary ships three handlers for smoke tests and benchmarks: `noop` returns immediately,
`sleep` waits for the milliseconds in its payload, and `fail` always errors. Real handlers are
yours to write against `worker.Run`, which [ARCHITECTURE.md](ARCHITECTURE.md) covers.

`go run` is fine for one process in the foreground. If you are backgrounding several,
build them instead: `go run` executes a binary named `exe/api`, so `pkill -f cmd/api`
will not find it and you end up with a stale server on the port.

## The dashboard

```
cd web && npm install && npm run dev
```

Http calls go through a Next rewrite, so they are same origin and need nothing configured.
The websocket cannot, since it connects straight to the api. Serving the dashboard from a
different origin therefore needs `NEXT_PUBLIC_API_URL` set on the dashboard and that origin
listed in `API_ALLOWED_ORIGINS` on the api, or the connection is refused.

## Regenerating the diagrams

```
make diagram
```

Reads a migrated database and rewrites [DIAGRAMS.md](DIAGRAMS.md): the job state machine out of
`job_transitions`, the entities out of the catalog. Both are generated, not hand edited.

## Running the tests

Tests need a running Postgres and `TEST_DATABASE_URL`. Each test clones its own database from
a migrated template, so they run concurrently.

```
TEST_DATABASE_URL=postgres://fenceline:fenceline@localhost:5433/postgres go test -race ./...
```

Time is injectable: `set fl.testing = 'on'` and `set fl.now = '...'` move the clock,
`set fl.jitter = 'off'` makes backoff deterministic. [TESTING.md](TESTING.md) covers
what the critical tests prove and where the coverage is thin.
