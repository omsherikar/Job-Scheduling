# Tests

142 Go tests and 15 in the dashboard. What matters is not the count — it is that the
hard properties are tested against the real database under real concurrency, rather
than against mocks that agree with the code.

```
make db-up
make migrate
TEST_DATABASE_URL=postgres://fenceline:fenceline@localhost:5433/postgres go test -race ./...
```

`make check` runs format, vet, build, and the suite together.

## How a test gets a database

The first test that runs builds `fenceline_test_template` by applying every migration,
under an advisory lock so parallel packages do not race to create it. Every test after
that clones it:

```go
pool := testdb.New(t)
```

The clone is a real database, dropped on cleanup. Tests never share state, never need
truncation between cases, and can run concurrently — which matters, because most of
them are about concurrency and a serialised suite would take an hour.

The template is built once per `go test` invocation and reused, so the migration cost
is paid once rather than 142 times.

## Moving the clock instead of sleeping

```sql
set fl.testing = 'on';
set fl.now = '2026-08-25 03:00:00+00';
set fl.jitter = 'off';
```

Every timestamp in the system goes through `fl.now()`, so a test can put a job's
backoff three hours in the past and check that the promoter releases it, without
waiting three hours or stubbing anything. Backoff jitter is switchable so a retry
delay is exact.

This is why the retry, cron, lease-expiry, and archival paths are tested against the
real elapsed-time logic. Nothing in those tests is a mock.

## The invariants every test can assert

`testdb.CheckInvariants` is called at the end of the tests that move a lot of state.
It checks four things that must never be true:

- a job with two open executions
- a `running` job with no lease
- a shard whose `in_flight` disagrees with the jobs actually claimed or running on it
- job status counts that do not sum to the number of jobs

**The invariant checker passed on the lock-order bug.** Two jobs whose handlers had
already succeeded were reported as `lost` and re-run, and nothing structural was
wrong: the ledger simply said something false. Structural invariants catch structural
damage. They do not catch a system that is consistently recording the wrong thing.

## What the critical tests actually prove

**`TestClaimExactlyOnce`, `TestClaimCapHolds`** — N workers race for the same jobs. No
job is claimed twice and the concurrency cap is never exceeded. This is the property
the whole design exists for.

**`TestStaleWorkerFenced`** — a worker claims, loses its lease, the reaper reassigns,
and then the original worker tries to report. Its write matches nothing. The test
asserts the fence moved and the stale report changed no rows.

**`TestFaultInjection`** — four workers on a 150 ms lease, a reaper loop, one goroutine
terminating idle-in-transaction backends and another expiring random leases at random,
over 400 jobs. Asserts every job reaches a terminal state, no job has two successful
executions in one replay generation, and that fenced writes actually happened — 120 to
200 of them, because a run with zero fenced writes proves the fault injection was not
biting rather than proving the system is safe.

**`TestCancelAgainstFail`** — the API cancelling a job at the same moment a worker
reports it failed. This is the pair that produced 391 deadlocks in 20 trials before the
lock order was fixed.

**`TestShardedCap`, `TestShardResizeWithWork`** — the cap holds across shards, work
spreads across more than one, and resizing is refused while the queue holds running
jobs. The last one exists because the first version of sharding did not have that rule
and over-admitted 12 against a cap of 10.

**`TestGracefulShutdown`** — SIGTERM with jobs in flight. Running jobs finish or are
released cleanly, nothing is stranded claimed.

**`TestSpringForward`, `TestFallBack`** — `0 12 * * *` in `America/New_York` across
both daylight saving transitions lands on noon local the next day, and the elapsed
time is 23 hours in March and 25 in November. A schedule pinned to a UTC offset would
give 24 both times and drift an hour off local noon.

**`TestHalfOpenProbe`, `TestBreakerCooldownProbe`** — a tripped breaker admits exactly
one job at a time while half open, and the probe budget is not raced by concurrent
claimers.

**`TestIdempotency`** — replaying a key returns the original job with `200` rather
than creating a second one, reusing a key with a different payload is a `409` rather
than silently returning the old job, and once the key has expired the same key creates
a fresh job.

**`TestKeysetPagination`, `TestPageSizeCap`** — the cursor does not skip or repeat
rows when rows are inserted while paging, and a caller cannot ask for an unbounded
page.

## Where the tests are thin

Stated rather than left for someone to discover.

**The model call in `internal/ai` has never run.** There is no API key in CI, so what
is tested is prompt assembly, the clipping, the caching and staleness logic, and the
path where no key is configured. The request shape was checked against the SDK source
rather than against the API.

**The dashboard has no end-to-end test.** The 15 vitest cases cover the health
derivations — what counts as saturated, starved, rate limited — because that is the
logic with real branching. Rendering is not tested.

**Multi-node behaviour is simulated, not distributed.** Concurrency tests run many
goroutines and many connections against one Postgres. That exercises the same locks
and the same races a second machine would, but it does not test network partitions
between a worker host and the database.

**Recovery time is measured, not asserted.** `kill -9` recovery was measured at 31.1 s,
bounded by the lease. No test fails if that regresses; the number is in `README.md`
and would have to be re-measured to notice.

## Running one thing

```
go test -race -run TestFaultInjection ./internal/worker/
go test -race -count=5 -run TestShard ./internal/jobs/
```

`-count` is worth using on anything concurrent. A race that appears once in six runs
is a race.

Under parallel load the `internal/jobs` package takes about 130 s on its own; running
several packages at once against one Postgres can push it past the default 10 minute
timeout, which is contention rather than a hang. `-timeout 900s` if the machine is
busy.

## Before committing

`.claude/agents/slop-check` reads the diff and flags anything that reads as
machine-written — comments that should not exist, test names that are sentences,
failures that narrate instead of stating values. Run it before every commit; it has
caught a vacuous assertion that could never fail and a test whose name promised more
than it checked.
