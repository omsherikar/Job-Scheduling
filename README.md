# Fenceline

A job scheduler built on Postgres. Every state change is guarded by a fencing token, so a
worker that was declared dead cannot report on a job that has moved on without it. Claiming
reads a partial index over ready jobs, so what it costs barely follows the size of the table:
fifty times the rows costs under twice the reads, and the numbers are below. Jobs that reach a
terminal state move to cold storage, which bounds the footprint rather than the latency.

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
go run ./cmd/scheduler  # promoter, cron, breaker, reaper, notifier, partitions, summaries
go run ./cmd/archiver   # hot to cold, idempotency pruning
go run ./cmd/worker     # claims and runs jobs, see below
go run ./cmd/migrate    # applies migrations, safe to run twice
```

The dashboard lives in `web`:

```
cd web && npm install && npm run dev
```

The scheduler writes failure summaries only when `ANTHROPIC_API_KEY` is set. Without it every
other sweep runs as usual and the summary sweep is a no-op.

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

A recurring job is a schedule on a queue: `POST /queues/{id}/schedules` with a five field cron
expression and a timezone, and the cron materialiser turns each tick into an ordinary job.

```
{"name": "nightly", "cron_expr": "0 3 * * *", "timezone": "America/New_York", "job_type": "report"}
```

The expression and the timezone are checked when you write them, not when the tick is due, and
`next_run_at` comes back computed. `overlap_policy` decides whether a tick fires while the last
one is still running, `catchup_policy` decides what a gap produces when the scheduler was down,
and a tick fires exactly once because `(schedule_id, scheduled_for)` is unique on `jobs`.
`POST /schedules/{id}/pause` and `/resume` stop and start it without deleting it.

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
| `shards` | how many rows the concurrency counter is split across, 1 to 64 |

`max_depth` is advisory. It is checked with a count at submission rather than held by a
constraint, because the counter that would enforce it was a contended row on the write path.
Two callers can pass the check at once and overshoot by a little. It bounds growth; it is not
an invariant.

`POST /queues/{id}/pause` and `/resume` stop and start claiming. A queue also stops on its own
when the breaker trips, and probes one job at a time while half open.

### Shards

Admission takes a row lock so the concurrency cap can be a single comparison. That row is the
only serialisation point in claiming, and past a few dozen simultaneous claimers it becomes the
limit. `shards` splits the counter across that many rows. `max_concurrency` is divided between
them, remainder to the low shards, so eight shards over a cap of ten give three, three, two,
two. The cap still holds exactly; what changes is how many claimers can be admitted at once.

A claimer reads the shard rows unlocked, picks one that has room, and locks only that one. It
never holds two, so there is no lock ordering to get wrong. The shard a job was admitted on is
written to `jobs.shard` and read back when the slot is released, so a completion returns the
slot to the shard that lent it.

Sharding costs something and buys nothing until the queue row is actually contended. Measured
on one queue with a warm cache:

| concurrent claimers | shards=1 | shards=2 | shards=4 | shards=8 |
|---|---|---|---|---|
| 16 | 1455/s | 1189/s | 1437/s | 1554/s |
| 128 | 410/s | 485/s | 707/s | 989/s |

At sixteen the round trip dominates and the counter is not the bottleneck, so shards make no
difference. At a hundred and twenty eight the row lock is the bottleneck and eight shards carry
about two and a half times the admissions. A second run of the same measurement put the
hundred and twenty eight row at 265, 595, 694 and 1159. The multiplier moves; the shape does not.

Leave it at 1 unless a queue is measurably starved on admission.

`shards` can only change while the queue holds no running job, and the database refuses the
change otherwise. Splitting a counter that is already at its limit would hand the new shards a
full allowance each while the old one still holds every slot it lent, and the cap would be
exceeded for as long as those jobs ran. Pause the queue, let it drain, then resize.

### Failure summaries

`GET /queues/{id}/failure-summary` groups the last day of failed executions by error class and
returns them with a written summary:

```json
{
  "window_hours": 24,
  "failures": [
    {"error_class": "timeout", "count": 90, "distinct_messages": 2,
     "latest_message": "context deadline exceeded", "last_seen": "..."}
  ],
  "summary": "Nearly every failure is a timeout against the billing host ...",
  "model": "claude-opus-5",
  "state": "current"
}
```

The grouping is computed on request. The summary is not: the scheduler writes it every few
minutes for queues whose failures have changed, because a model call is too slow and too
expensive to sit on an endpoint the dashboard polls. `state` says what you are looking at.
`current` means the summary matches the failures below it, `stale` means new failures have
arrived since it was written, `pending` means one is coming, and `unavailable` means the
scheduler has no `ANTHROPIC_API_KEY` and none will be written. The table is returned in every
case; only the prose depends on the key.

Error text from jobs is passed to the model as data, and the prompt says so. Nothing in a job's
error message is treated as an instruction.

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

A worker registers itself against a queue, announces the job types it has handlers for, and
claims only those. That is what makes it safe to enqueue a new type before deploying the
workers that run it: the jobs wait instead of failing. Workers extend their lease while
running, carry progress on the heartbeat, and mark themselves draining then dead on the way
out, which is what the dashboard reads to tell you a queue has nobody to run it.

```
DATABASE_URL=... WORKER_QUEUE=<queue id> WORKER_CONCURRENCY=4 go run ./cmd/worker
```

`worker.Run` is the library underneath, and the handlers are yours. The binary carries three
for smoke tests and benchmarks: `noop` returns immediately, `sleep` waits for the milliseconds
in its payload, and `fail` always errors.

## Events

The scheduler turns new events into one payload-less `notify`, coalesced, and workers wake on
it instead of waiting out their poll. A worker is correct either way; the difference it makes
is under Numbers.

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

## Numbers

Measured on PostgreSQL 17.11 in docker on an 8 core, 8 GB laptop, `shared_buffers=1GB`,
`random_page_cost=1.1`, `work_mem=16MB`, `fsync=on`, `synchronous_commit=on`. Treat the shape
as the result and the absolute figures as this machine.

**Claim latency against table size.** Server-side execution of the claim, batch 50, everything
but the row count held identical and the transaction rolled back between repetitions. Taken
before the buffer and planner settings above were raised, at `shared_buffers=128MB` and
`random_page_cost=4`:

| rows in `jobs` | table size | claim | candidate select |
|---|---|---|---|
| 20,000 | 8 MB | 4.61 ms | 54 buffers |
| 100,000 | 39 MB | 4.69 ms | 54 buffers |
| 1,000,000 | 392 MB | 5.94 ms | 54 buffers |
| 10,000,000 | 3,919 MB | 5.79 ms | 54 buffers |

Five hundred times the rows for twenty six percent more work, and under those controlled
repetitions the candidate select read the same fifty four buffers throughout, because
`idx_jobs_claimable` is partial on `queued` and does not grow with the table. It holds with
the archiver switched off; archiving bounds the footprint, not this.

A single cold run on the settings above reads 54 buffers at twenty thousand rows and 104 at a
million — fifty times the rows for under twice the reads. Sublinear rather than flat once the
repetitions stop hiding the cold index descent, which is the honest version of the same claim.

**Enqueue to start**, 200 jobs arriving 15 ms apart, concurrency 8, one second poll:

| | p50 | p95 | p99 |
|---|---|---|---|
| polling only | 7,472 ms | 13,958 ms | 14,624 ms |
| notify on | 17 ms | 174 ms | 278 ms |

The tail is queueing rather than wakeup latency: without the push path a worker claims once
per interval, so arrivals faster than one batch per poll back up. A shorter poll narrows the
gap.

**Correctness under fault injection.** Four workers on a 150 ms lease, a reaper loop, a
goroutine terminating idle-in-transaction backends and another expiring random leases, over
400 jobs: every job reaches a terminal state, no job has two successful executions in one
replay generation, and 120 to 200 fenced writes are recorded, which is the point. The counter
a queue keeps of what is in flight is never lower than what is actually running; it can be
left high by a connection dying between reserving a slot and claiming it, and
`fl.reconcile_in_flight` restores it exactly.

**Recovery after `kill -9`.** Four worker processes holding sixteen jobs with open executions,
killed outright, reaper every half second, default thirty second lease: **31.1 s** until every
job was runnable again, all sixteen executions closed as `lost`, no lease orphaned and no
counter drift. Recovery is bounded by the lease, not by how the worker died; a shorter lease
recovers sooner and costs more heartbeats.

**Cost per job**, two thousand jobs claimed, run and completed at concurrency 8, on an
otherwise idle database with the statistics reset first:

| | per job | per million |
|---|---|---|
| WAL written | 4,824 bytes | 4.8 GB |
| database active time | 3.25 ms | 0.9 cpu-hours |
| commits | 1.33 | 1.33 M |

Most of that one and a third commits is starting the attempt, which is the one step still done
a job at a time: claiming takes two commits but spreads them over everything it claims, and
reporting spreads one over the batch it reports. The WAL figure is a steady-state one — a cold
run pays several times that in full-page images right after a checkpoint.

## Watching it

Three views expose the operational numbers, so they come from the database rather than
from counters that go wrong after a crash or disagree between instances:

| view | what it answers |
|---|---|
| `fl.queue_age` | oldest ready job age per queue, broken out per priority tier |
| `fl.fenced_writes` | how many times the fence has actually rejected a write |
| `fl.reaper_lag` | how far behind the reaper is on leases that have already expired |

Age is the latency SLI; depth tells an operator nothing about whether a queue is moving. The
fenced write count is worth publishing because a zero there means the mechanism has never
fired, which is a different thing from it working.

`make diagram` prints the state machine and the schema as mermaid, read from `job_transitions`
and the catalog rather than kept up to date by hand.

`SCHEMA.md` explains the tables: what the keys are, which deletes cascade and which refuse,
where the design is deliberately denormalized and what that buys, and why the claim index is
partial.

## Testing

Tests need a running Postgres and `TEST_DATABASE_URL`. Each test clones its own database from
a migrated template, so they run concurrently. Time is injectable: `set fl.testing = 'on'` and
`set fl.now = '...'` move the clock, `set fl.jitter = 'off'` makes backoff deterministic.
