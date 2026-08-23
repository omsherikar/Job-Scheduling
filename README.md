# Fenceline

A job scheduler built on Postgres. Every state change is guarded by a fencing token, so a
worker that was declared dead cannot report on a job that has moved on without it. Claiming
reads a partial index over ready jobs, so what it costs does not follow the size of the table:
measured at 54 buffers whether the table holds twenty thousand rows or ten million. Jobs that
reach a terminal state move to cold storage, which bounds the footprint rather than the
latency.

## Running it

Postgres runs in docker on 5433.

```
make db-up
make migrate
make check
```

The services are four binaries, each taking `DATABASE_URL`:

```
go run ./cmd/api        # http, default port 3001, API_PORT to change it
go run ./cmd/scheduler  # promoter, cron, breaker, reaper, notifier, partitions
go run ./cmd/archiver   # hot to cold, idempotency pruning
go run ./cmd/migrate    # applies migrations, safe to run twice
```

The dashboard lives in `web`:

```
cd web && npm install && npm run dev
```

It proxies `/api` to `API_URL`, so http calls are same origin. The event stream connects
straight to the API, which means two variables when the dashboard is not served from the
API's own origin: `NEXT_PUBLIC_API_URL` on the dashboard, and `API_ALLOWED_ORIGINS` on the
API, a comma separated list. Without the second, only same origin streams are accepted.

## Authentication

`POST /auth/register` creates a user and returns it. `POST /auth/login` returns a session
token. Send it as `Authorization: Bearer <token>`; the API also reads an `fl_session` cookie.
Browsers cannot set headers on a websocket, so the stream accepts the token as an
`fl.token.<token>` subprotocol instead.

Roles are owner, admin, member and viewer, held per organization. A viewer sees everything
and changes nothing. Every privileged mutation writes an audit row.

## Jobs

```
POST /queues/{id}/jobs
{
  "type": "send.email",
  "payload": {"to": "someone@example.com"},
  "priority": 0,
  "run_at": "2026-01-01T00:00:00Z",
  "timeout_ms": 300000,
  "depends_on": ["<job id>"]
}
```

`Idempotency-Key` is honoured for 24 hours. Replaying the key with a different payload is a
409; replaying it with the same payload returns the original job.

A job with `depends_on` is created `scheduled` with a pending count and becomes runnable when
every parent finishes. Dead lettering a parent cancels its descendants, and replaying a parent
whose descendants were cancelled is refused rather than silently reviving them. Cycles are
rejected at submission.

`GET /jobs` filters on `status`, `type`, `queue` and pages on an opaque cursor. `GET /jobs/{id}`
returns the job, every attempt with its logs, the live lease with whatever progress the handler
reported, and the job's parents and children.

`POST /jobs/{id}/cancel` is best effort against a job already running: the worker notices on
its next heartbeat. `POST /jobs/{id}/retry` moves a waiting job to the front of the queue.

## Queues

`POST /projects/{id}/queues` and `PATCH /queues/{id}` carry the operational knobs:

| field | meaning |
|---|---|
| `max_concurrency` | how many jobs may be claimed and running at once |
| `rl_limit_per_sec`, `rl_burst` | token bucket applied in the claim gate |
| `max_depth` | best effort cap on queued jobs, see below |
| `default_priority` | priority given to jobs that do not set one |
| `retry_policy_id` | which policy governs backoff and attempts |

`max_depth` is advisory. It is checked with a count at submission rather than held by a
constraint, because the counter that would enforce it was a contended row on the write path.
Two callers can pass the check at once and overshoot by a little. It bounds growth; it is not
an invariant.

`POST /queues/{id}/pause` and `/resume` stop and start claiming. A queue also stops on its own
when the breaker trips, and probes one job at a time while half open.

## Retry, snooze and the dead letter queue

A handler that returns an error burns an attempt and the job comes back after the policy's
backoff. A handler that returns `worker.Snooze{After: d}` does not: the fence moves, the
attempt count does not, and the job comes back after `d`, clamped to the policy's
`max_delay_ms`. Snoozing is capped by `max_attempts` so a job cannot be parked forever.

Exhausted jobs land in the dead letter queue with their full attempt history.
`POST /dlq/{jobId}/replay` replays one. `POST /queues/{id}/dlq/replay` drains many:

```
{"limit": 100, "rate_per_sec": 10}
```

It stops at the queue's depth cap, staggers `run_at` so the queue is not flooded, and skips
any parent whose descendants were cancelled.

## Workers

`worker.Run` takes a worker row that already exists, announces the job types it has handlers
for, and claims only those. That is what makes it safe to enqueue a new type before deploying
the workers that run it: the jobs wait instead of failing. Workers extend their lease while
running and carry progress on the heartbeat.

There is no worker binary yet and nothing registers a worker row on startup, so running one
outside the tests means inserting the row yourself. `cmd/worker` is empty.

## Events

Every transition writes to an outbox. `GET /projects/{id}/events?after=<id>` replays from a
cursor; `GET /projects/{id}/events/stream` is the same thing over a websocket. Frames carry
`prev_id` so a client can detect a gap without trusting the server, and a cursor older than
the retention window is rejected with `cursor_too_old` and the oldest id still available.

Event ids are ordered by commit within a project, not globally, so a tailer filtered to one
project sees a contiguous sequence. Ordering is bought with an advisory lock held per project;
it costs throughput on a single very busy project and nothing across projects.

## Retention

Terminal jobs move to `jobs_archive` with their ledger after a day, dead lettered ones after
thirty. Events and logs are kept seven days, archives ninety, each by dropping whole daily
partitions rather than deleting rows. Partitions are cut thirty days ahead; if the scheduler
stops for longer than that, writes begin to fail.

## Testing

Tests need a running Postgres and `TEST_DATABASE_URL`. Each test clones its own database from
a migrated template, so they run concurrently. Time is injectable: `set fl.testing = 'on'` and
`set fl.now = '...'` move the clock, `set fl.jitter = 'off'` makes backoff deterministic.
