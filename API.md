# The HTTP API

Fifty endpoints. JSON in, JSON out. Errors are `application/problem+json`.

Everything below is scoped to something you are a member of. There are no global
reads: a request for a job proves membership of the organization that owns the
project that owns the queue that owns the job, in one join, on every request. Nothing
is cached.

## Getting a session

`POST /auth/register` and `POST /auth/login` both take `{"email": ..., "password": ...}`
and return `{"token": ..., "user_id": ..., "issued_at": ..., "expires_at": ...}`. Send
the token back either way:

```
Authorization: Bearer <token>
```

An `fl_session` cookie is accepted as an alternative if a client sets one, but no
response sets it for you: the token comes back in the body and it is yours to store.
`POST /auth/logout` ends the session.

Passwords are argon2id. Sessions are rows with a 30 day expiry, so logging out
actually revokes rather than hoping a token expires.

## Roles

Membership carries a role on the pairing, not on the user, so the same person can be
an admin of one organization and a viewer of another. They nest: an owner can do
anything an admin can, and so on down.

| role | can |
|---|---|
| `viewer` | read anything in the organization |
| `member` | submit, cancel, retry, replay, pause a queue |
| `admin` | create and change queues, policies, schedules, projects |
| `owner` | delete the organization |

A request below the required role gets `403`. A request for something outside your
organizations gets `404` rather than `403`, so existence is not leaked to non-members.

Every privileged mutation writes an audit row naming the actor, the action, and the
entity.

## Errors

```json
{
  "type": "about:blank",
  "title": "job is not in the dead letter queue",
  "status": 409,
  "request_id": "0f8c...",
  "detail": "..."
}
```

`request_id` echoes `X-Request-Id` if you sent one and is generated if you did not. It
is on every response, including successes, and it is in the server log line for the
same request.

| status | means |
|---|---|
| 400 | the body or a parameter is wrong |
| 401 | no session, or it expired |
| 403 | your role is too low |
| 404 | it does not exist, or it is not yours |
| 409 | it exists but not in a state that allows this |
| 429 | queue depth cap reached |
| 503 | the database took too long |

Serialization failures and deadlocks are retried inside the request up to five times
with jittered backoff before any of that is reached. Callers do not see them.

## Organizations and projects

| method | path | role |
|---|---|---|
| POST | `/orgs` | viewer |
| GET | `/orgs` | viewer |
| GET | `/orgs/{id}` | viewer |
| PATCH | `/orgs/{id}` | admin |
| DELETE | `/orgs/{id}` | owner |
| POST | `/orgs/{id}/members` | admin |
| POST | `/orgs/{orgId}/projects` | admin |
| GET | `/orgs/{orgId}/projects` | viewer |
| GET | `/projects/{id}` | viewer |
| PATCH | `/projects/{id}` | admin |
| DELETE | `/projects/{id}` | admin |

Creating an organization makes you its owner. `POST /orgs/{id}/members` takes
`{"email": ..., "role": ...}` and adds an existing user.

Deleting an organization removes its projects, their queues, and every job in them.
This is a real cascade in the database, not a soft delete.

## Retry policies

| method | path | role |
|---|---|---|
| POST | `/projects/{projectId}/retry-policies` | admin |
| GET | `/projects/{projectId}/retry-policies` | viewer |
| GET | `/retry-policies/{id}` | viewer |
| PATCH | `/retry-policies/{id}` | admin |
| DELETE | `/retry-policies/{id}` | admin |

```json
{
  "name": "standard",
  "kind": "exponential",
  "max_attempts": 5,
  "max_lost_attempts": 3,
  "base_delay_ms": 1000,
  "max_delay_ms": 300000,
  "jitter": true
}
```

`kind` is `fixed`, `linear`, or `exponential`.

`max_lost_attempts` is stored and range-checked but nothing reads it. It was meant to
cap reclaims after a lease expiry separately from ordinary handler failures, so a job
that keeps killing its worker would not retry as far as one that merely returned an
error. That is not wired up: a lost attempt burns an ordinary attempt and
`max_attempts` is the only limit that applies. The field is left in place because
removing it from the API would break callers already sending it, but treat it as
inert until it is implemented.

Deleting a policy that a queue or job still references is refused. The reference is
evidence of how something was retried, and it does not get to vanish quietly.

## Queues

| method | path | role |
|---|---|---|
| POST | `/projects/{projectId}/queues` | admin |
| GET | `/projects/{projectId}/queues` | viewer |
| GET | `/queues/{id}` | viewer |
| PATCH | `/queues/{id}` | admin |
| DELETE | `/queues/{id}` | admin |
| POST | `/queues/{id}/pause` | member |
| POST | `/queues/{id}/resume` | member |

```json
{
  "name": "email",
  "retry_policy_id": "...",
  "max_concurrency": 10,
  "shards": 1,
  "max_depth": 100000,
  "default_priority": 0,
  "rl_limit_per_sec": 50,
  "rl_burst": 100
}
```

`max_depth` is advisory. It is a count at submission rather than a constraint, because
the counter that would enforce it was a contended row on the write path. Two callers
can pass the check at once and overshoot a little. It bounds growth; it is not an
invariant. Hitting it returns `429`.

`shards` splits the concurrency counter across that many rows so admission is not one
lock. Changing it while the queue holds a running job is a `409`. See [README.md](README.md)
for when it is worth raising, which is later than you would think.

A paused queue stops being claimed from immediately. Jobs already running finish.

A queue also stops on its own when its circuit breaker trips, and probes one job at a
time while half open until it either reopens or trips again.

## Submitting jobs

| method | path | role |
|---|---|---|
| POST | `/queues/{id}/jobs` | member |
| POST | `/queues/{id}/jobs/batch` | member |

```json
{
  "type": "send_welcome",
  "payload": {"user_id": 41},
  "priority": 5,
  "run_at": "2026-08-25T09:00:00Z",
  "timeout_ms": 30000,
  "retry_policy_id": "...",
  "depends_on": ["<job id>", "<job id>"]
}
```

Only `type` is required. Omit `run_at` and it runs now; set it and the job sits in
`scheduled` until the promoter releases it. `depends_on` holds it in `scheduled`
until every parent has finished, whatever `run_at` says.

Send an `Idempotency-Key` header and a repeat of the same key on the same queue
returns the original job instead of making a second one. Replaying a key with a
different payload is a `409`. Keys are honoured for 24 hours and are unique per queue
rather than globally, so two teams can use `order-1234` without colliding.

A job with `depends_on` is created `scheduled` with a pending count and becomes
runnable once every parent has finished. Cycles are rejected at submission.

The batch endpoint takes `{"jobs": [...]}`, at most 1000, and inserts them in one
transaction. `GET /batches/{id}` reports how a batch is progressing.

## Reading jobs

| method | path | role |
|---|---|---|
| GET | `/jobs` | viewer |
| GET | `/jobs/{id}` | viewer |
| POST | `/jobs/{id}/cancel` | member |
| POST | `/jobs/{id}/retry` | member |
| GET | `/batches/{id}` | viewer |

`GET /jobs` filters on `queue`, `status`, `type`, and pages by cursor:

```
GET /jobs?queue=<id>&status=dead_letter&limit=50&cursor=<next_cursor>
```

The cursor is a keyset on `(created_at desc, id desc)`, not an offset, so page 400 is
as fast as page 1 and rows do not shift under you while you page.

`GET /jobs/{id}` returns the job with every execution attempt and every log line for
each attempt, its dependencies, and its dependents. Nothing is overwritten on retry,
so attempt 1's failure is still there after attempt 4 succeeds.

`GET /jobs/{id}` also carries the live lease with whatever progress the handler last
reported on its heartbeat.

`cancel` is a compare-and-swap: it returns `409` if the job already reached a terminal
state. Against a running job it is best effort in the sense that the handler is not
interrupted: the API closes the execution, drops the lease, and releases the
concurrency slot, and the worker finds out on its next heartbeat. Its next write is
fenced either way.

`retry` moves a `retry_wait` or `scheduled` job to `queued` immediately, skipping the
rest of its backoff. It refuses if dependencies are still outstanding.

## Dead letter queue

| method | path | role |
|---|---|---|
| GET | `/dlq` | viewer |
| POST | `/dlq/{jobId}/replay` | member |
| POST | `/queues/{id}/dlq/replay` | member |
| GET | `/queues/{id}/failure-summary` | viewer |

A handler that returns an error burns an attempt and the job comes back after the
policy's backoff. A handler that returns `worker.Snooze{After: d}` does not: the fence
moves, the attempt count does not, and the job comes back after `d`, clamped to the
policy's `max_delay_ms`. Snoozing is capped by `max_attempts` so a job cannot be parked
forever.

A dead-lettered job keeps its full attempt history in `execution_history`.

Replaying one resets `attempt_count`, bumps `replay_generation`, and queues it. If the
job had descendants that were cancelled when it died, the replay is refused with the
list of ids, because reviving a parent without its children would leave a half-run
graph.

Bulk replay takes `{"limit": 100, "rate_per_sec": 10}`, stops at the queue's depth cap,
staggers `run_at` so a drained dead letter queue does not immediately flood the live
one, and skips any parent whose descendants were cancelled.

`failure-summary` groups the last day of failures by error class and, if the scheduler
has an `ANTHROPIC_API_KEY`, includes a written summary:

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

The grouping is computed on request. The summary is not: the scheduler writes it every
few minutes for queues whose failures have changed, because a model call is too slow and
too expensive to sit on an endpoint the dashboard polls. `state` says what you are
looking at. `current` means the summary matches the failures beside it, `stale` means
new failures have arrived since it was written, `pending` means one is coming, and
`unavailable` means the scheduler has no key and none will be written. The table comes
back in every case; only the prose depends on the key.

Job error text can contain anything a handler chose to put in it, so the prompt states
that the ledger is data to describe and not instructions to follow.

## Schedules

| method | path | role |
|---|---|---|
| POST | `/queues/{id}/schedules` | admin |
| GET | `/queues/{id}/schedules` | viewer |
| GET | `/schedules/{id}` | viewer |
| PATCH | `/schedules/{id}` | admin |
| DELETE | `/schedules/{id}` | admin |
| POST | `/schedules/{id}/pause` | member |
| POST | `/schedules/{id}/resume` | member |

```json
{
  "name": "nightly report",
  "cron_expr": "0 3 * * *",
  "timezone": "America/New_York",
  "job_type": "report",
  "payload": {},
  "overlap_policy": "skip",
  "catchup_policy": "skip"
}
```

Five-field cron. The expression and the timezone are both validated when you write
them, not when the tick is due, and `next_run_at` comes back computed. The timezone is
a foreign key to a table seeded from `pg_timezone_names`, so a schedule Postgres cannot
interpret is rejected at creation.

`0 3 * * *` in `America/New_York` fires at 3am local across daylight saving
transitions, not at a fixed UTC offset.

`overlap_policy: "skip"` will not start a tick while the previous one is still
running. `catchup_policy` decides what happens after downtime: `skip` forgets missed
ticks, `fire_once` runs one.

A unique index on `(schedule_id, scheduled_for)` is what makes a tick fire exactly
once, no matter how many schedulers are running or how often they retry.

## Events

| method | path | role |
|---|---|---|
| GET | `/projects/{projectId}/events` | viewer |
| GET | `/projects/{projectId}/events/stream` | viewer |

Events are written in the same transaction as the state change they describe, so there
is no window where a job is completed but its event is missing.

`GET /events` pages by cursor. The stream is a WebSocket carrying the same events live.
It authenticates by subprotocol rather than header, since browsers cannot set headers
on a WebSocket:

```js
new WebSocket(url, [`fl.token.${token}`])
```

Origins must be in `API_ALLOWED_ORIGINS`. Without it, only same-origin connections are
accepted.

Frames carry `prev_id`, so a client can detect a gap without trusting the server.
Reconnect with the last id you saw and you get everything since; a cursor older than
the retention window is rejected with `cursor_too_old` and the oldest id still
available. The stream falls back to a two-second poll if `pg_notify` is not arriving,
so a missed notification costs latency and not events.

Event ids are ordered by commit within a project, not globally, so a tailer filtered to
one project sees a contiguous sequence.

## Fleet and metrics

| method | path | role |
|---|---|---|
| GET | `/workers` | viewer |
| GET | `/projects/{projectId}/queue-health` | viewer |
| GET | `/stats/queues/{id}` | viewer |
| GET | `/stats/queues/{id}/series` | viewer |

`/workers` lists registered workers with hostname, pid, state, max concurrency, how
many leases each currently holds, when it started, and when it was last seen. A worker
that has not been seen in 30 seconds stops counting as live in the health endpoints,
though its row stays until it is reaped.

`/queue-health` is one call for a whole project: per queue, its depth by priority
tier, live workers, breaker state, whether it is rate limited or saturated, the last
hour's outcomes, and p95 duration. It is what the dashboard's landing page reads.

`/stats/queues/{id}` is the same for one queue with job counts by status and the age
of the oldest ready job. `/series?minutes=60` returns per-minute throughput and
latency percentiles from the rollup table rather than recomputing them from the
ledger, which was measured at 635 ms on an endpoint that polls.
