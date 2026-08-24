# Decisions

What was chosen, what it cost, and what was tried and thrown away. Every number here
was measured on this codebase; the machine and settings are in the Numbers section of
[README.md](README.md).

## Postgres is the only coordination point

**Chosen:** one database, no broker, no Redis, no lock service.

The alternative is the usual split: Redis or a broker for the hot queue, Postgres for
the durable record. That is faster per operation and it is the standard answer.

It was rejected because it makes the interesting failure unrecoverable. When the queue
and the ledger are different systems, a crash between "pop from Redis" and "write to
Postgres" leaves a job that is neither queued nor recorded, and no amount of care in
the application layer closes that window: you can only make it narrower. With one
database the pop and the record are the same transaction and the window does not
exist.

**What it costs:** a claim is a network round trip and a commit rather than an
in-memory pop. Measured at 4,824 bytes of WAL, 3.25 ms of database time, and 1.33
commits per job. Throughput is in the hundreds to low thousands per second on one
node, not the hundreds of thousands. If your workload needs that, this is the wrong
design and no amount of tuning inside it will get you there.

## Fencing tokens rather than heartbeat trust

**Chosen:** every write carries the fence it was claimed under. `rowCount == 0` means
you were fenced.

The alternative is trusting liveness: the worker heartbeats, and if it stops the job
is reassigned. That is simpler and it is wrong, because liveness is not observable
across a network. A worker that missed three heartbeats is indistinguishable from one
that was paused by the operating system and is about to write a result.

Fencing makes the distinction unnecessary. Reassign whenever you like; the old owner's
write fails because the row moved on. A lease expiry that turns out to be wrong costs
a duplicate execution, never a duplicate completion.

**What it costs:** every state-changing query grows a `where fence = $n`, and callers
must treat zero rows as an answer rather than an error. Getting that backwards is the
one mistake that breaks the guarantee. An earlier version classified a fenced write
as a retryable database error and re-ran jobs that had already succeeded.

## Two counters instead of one

`attempt_count` counts work. `fence` counts ownership. They look redundant until a
snooze, which moves ownership without burning an attempt, or a replay, which resets
attempts without rewinding ownership.

Collapsing them means either snoozing consumes retries, or replay lets a stale worker
back in. `fence >= attempt_count` is a check constraint so the two cannot be quietly
swapped.

## `SKIP LOCKED` for claiming, a lock for admission

**Chosen:** admission and claiming are separate transactions.

Admission compares a counter against a cap, and that has to serialise. Claiming reads
rows, and with `for update skip locked` it never blocks: two workers take disjoint
sets instead of queueing. Splitting them keeps the serialised part as short as
possible.

Measured at 1.8 to 2.0 times the single-queue throughput of doing both in one
transaction.

**What it costs:** a worker can be granted five slots and find three jobs, so it has
to give two back. If it dies in between, the counter is left high and the queue admits
fewer jobs than it could until `fl.reconcile_in_flight()` repairs it. The drift is
always in the safe direction. A counter that could drift low would admit past the cap,
which is not a bug you can live with.

## Sharding the concurrency counter

**Chosen:** `queues.shards` splits the counter across N rows, default 1.

Sharding buys nothing at ordinary concurrency. At sixteen claimers the round trip
dominates and the counter is not the bottleneck; past that the row lock becomes the
limit and splitting it helps. [BENCHMARKS.md](BENCHMARKS.md) has the throughput against shard count,
and the comparison against simply using more queues, which achieves the same effect by
hand. Sharding is for when the work has to stay in one logical queue.

**What it costs:** a table, a column on `jobs`, and a rule that `shards` cannot change
while the queue holds a running job. That rule exists because the first version did
not have it: raising `shards` on a queue at its limit gave the new shards a full
allowance each while the original still held every slot it had lent, and the cap was
exceeded, measured at 12 in flight against a cap of 10. Resharding a hot queue now
means pausing it and letting it drain.

Redistributing counters live instead would have avoided the pause, but it races with
concurrent admissions unless every admission takes a shared lock on the queue row,
which is exactly the serialisation sharding exists to remove.

## One canonical lock order

**Chosen:** `jobs`, then `job_executions`, `job_leases`, `queue_stats_minute`,
`queues`, `queue_shards`, and `outbox` last.

This is the decision with the best evidence behind it, because the version without it
was measured. `fl.report_failure` emitted its event before the caller released the
queue slot, while the success path released first. Under load that produced **391
deadlocks in 20 trials**, and two completions out of 2400 had already-successful
handlers reported as `lost` and re-run.

The invariant checker passed on that state. Nothing was structurally broken; the
ledger just said something false. Ordering locks by a fixed rule is what prevents
that, since ordering them by whatever each function happens to need next is how the
inversion got in.

## The outbox lock is per project, not global

Events are written in the same transaction as the state change, under an advisory lock
so ids are totally ordered and the notifier only has to say "the tail moved".

A single global lock did that correctly and serialised every write in the system.
Per project, two projects never wait on each other, and within a project the ordering
consumers actually depend on still holds. Cross-project ordering is given up
deliberately; nothing consumes it.

## `pg_notify` is an optimisation, never a requirement

Enqueue to start is 7,472 ms at p50 on polling alone and 17 ms with notify on.

Nothing depends on it. Every path that notify accelerates also has a poll behind it,
and the WebSocket stream falls back to a two second poll if notifications stop
arriving. `pg_notify` does not survive a connection drop and has no
delivery guarantee, so a system that needs it to be correct is a system that breaks
quietly. A missed notification here costs latency and nothing else.

## Drop partitions instead of deleting rows

Deleting two million event rows was measured at 5.5 s and left bloat that had to be
earned back with `vacuum full` every cycle. Dropping a daily partition is instant and
leaves nothing behind.

**There are no default partitions, deliberately.** A default partition silently
swallows rows for a day nobody created, and then blocks that day's partition from
being created later. Without one, if the scheduler stops cutting partitions for more
than thirty days, inserts start failing, which is noisy but easier to diagnose than
rows quietly landing in the wrong day.

## The database enforces what it can

The state machine is a trigger over nineteen edges in `job_transitions`, asserted at
migration time to match `fl.legal_transition` in both directions. Two workers holding
one job is unrepresentable because `job_leases` is keyed by `job_id`. A cron tick
fires once because of a unique index on `(schedule_id, scheduled_for)`, not because
the scheduler is careful. A schedule cannot store a timezone Postgres does not know,
because the timezone is a foreign key to a table seeded from `pg_timezone_names`.

Where an invariant can be a constraint, it is one. Enforcing it in application code
instead works until a second caller is added that does not know to.

## Denormalizations, and what each one bought

Three, each with a specific measurement behind it.

`queue_shards.in_flight`. Deriving the concurrency count on every claim means an index
scan proportional to concurrency on the hottest path. The counter makes the cap a
single-row comparison. Cost: it can drift high, and `fl.reconcile_in_flight()` repairs
it.

`jobs.pending_deps`. Without it the promoter joins `job_dependencies` on every sweep.
With it the partial index carries `pending_deps = 0` in its predicate and never reads
the edges.

`queue_stats_minute`. Computing throughput and percentiles from the ledger on demand
was 635 ms on a dashboard endpoint that polls. The rollup makes it an index lookup.

## `max_depth` is advisory and says so

A real queue depth cap needs a counter on the write path, and that counter is a
contended row on every submit. It is a count at submission instead, so two callers can
pass the check at once and overshoot slightly.

It is documented as advisory for that reason. Overshooting slightly is tolerable when
callers know to expect it, and misleading if the docs describe it as a hard cap.

## Indexes are added on evidence and removed on evidence

Every index on `jobs` makes each state change write another entry and prevents HOT
updates, because `status` appears in several predicates. Measured at about 4,800 bytes
of WAL per job.

Five have been dropped again: one an exact duplicate of a unique constraint, two
strict prefixes of another, one with no reader at all, and one whose 834x saving on a
page a human opens twice a day did not justify a write on every completion.

Two proposals were rejected after checking rather than accepting: dropping
`idx_org_members_user`, which is not a prefix of the `(org_id, user_id)` primary key,
and dropping `idx_jobs_inflight`, which `fl.reconcile_in_flight` does use.

## The failure summary runs in the scheduler, not the request

The obvious place for an AI summary is the endpoint that returns it. That would put a
multi-second, paid model call on a path the dashboard polls every five seconds.

The scheduler writes summaries every few minutes for queues whose failure digest
changed; the endpoint serves what is cached and reports `current`, `stale`, `pending`,
or `unavailable` so the reader knows what they are looking at. The grouped failure
table is computed per request and is always there, and only the prose depends on a key
being configured.

Job error text goes into the prompt as data, and the prompt says so, because error
messages can contain anything a handler chose to put in them.

## Time is injectable

`fl.now()` is `clock_timestamp()` in production and a GUC under test. Backoff jitter
can be switched off.

Without this, testing retry, cron, and lease expiry means either sleeping or mocking.
Sleeping would make the suite take hours, and mocking would test the mock. With an
injectable clock the suite exercises the real elapsed-time logic and finishes in
minutes.

**What it costs:** every timestamp must go through `fl.now()`. A single stray `now()`
would be invisible in production and would break tests in a confusing way. One did get
in, where schedules computed `next_run_at` from the Go wall clock, and it took a test that
pins the tick to 03:00 America/New_York to catch it.

## What was deliberately not built

**TLA+ or a formal model.** Considered for the claim protocol. The fault injection
suite exercises the same properties against the real implementation, including the
parts a model would abstract away: connection death mid-transaction, the counter
drift, the reaper racing a live worker. A model of the protocol would not have caught
the lock-order inversion, because the protocol was fine and the implementation was not.

**A generic plugin system for handlers.** Handlers are a `map[string]Handler`. Two
callers would justify an abstraction; there is one shape of caller.

**Priority as a separate queue per level.** Priority is a column and an index prefix.
Separate queues per level would multiply the admission rows and make starvation
detection harder, and starvation is already handled by the aging sweep raising the
priority of jobs that have waited too long.
