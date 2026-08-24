# The schema

Twenty nine tables, plus a daily partition each for events, logs and the three archives.
The shape follows one rule: the database holds the truth about a job, and every process that
touches one is expected to crash at the worst possible moment.

## Who owns what

```
users ──< org_members >── organizations ──< projects ──< queues ──< jobs
                                                │           │        │
                                          retry_policies    │   job_executions ──< job_logs
                                                            │        │
                                                        schedules  job_leases
```

`users` and `organizations` are many-to-many through `org_members`, which carries the role
(`owner`, `admin`, `member`, `viewer`) as an attribute of the pairing rather than of either
side, because the same person is an admin of one organization and a viewer of another. A
project belongs to one organization, a queue to one project, a job to one queue.

Every authorization check walks that chain in a single join back to `org_members`, so a request
for a job proves membership of the organization that owns the project that owns the queue that
owns the job. No process caches the answer.

## Keys

Most tables take a synthetic `uuid` primary key from `fl.uuidv7()`. v7 puts a millisecond
timestamp in the high bits, so successive ids sort together and inserts land at the end of the
index rather than scattering across it. That matters on `jobs`, which is the table under
constant insert pressure.

Where a row is really a pairing, the pair is the key and there is no separate id:

| table | key | why |
|---|---|---|
| `org_members` | `(org_id, user_id)` | a person is in an organization once |
| `worker_queues` | `(worker_id, queue_id)` | a worker subscribes to a queue once |
| `job_dependencies` | `(parent_id, child_id)` | an edge exists once |
| `idempotency_keys` | `(queue_id, key)` | a key is unique within its queue, not globally |
| `job_transitions` | `(from_status, to_status)` | the edge is the fact |
| `dead_letter_jobs` | `job_id` | a job is dead lettered at most once |
| `job_leases` | `job_id` | a job has at most one lease, by construction |

`job_leases` deserves a note. Making `job_id` the primary key is what makes "two workers
believe they hold the same job" unrepresentable rather than merely unlikely.

Partitioned tables must carry the partition key in the primary key, so `events` is
`(created_at, id)`, `job_logs` and `job_logs_archive` are `(ts, id)`, `jobs_archive` is
`(terminal_at, id)` and `job_executions_archive` is `(finished_at, id)`. The id alone is still
unique — the sequence guarantees it — but Postgres needs the partition column in the key.

`events_retention` has a primary key of `only_row boolean` with `check (only_row)`. It is a
one-row table and the key says so.

## What happens when you delete something

Cascade behaviour is not uniform, and the differences are deliberate.

**Cascade — the child has no meaning without the parent.** Deleting an organization removes its
projects, their queues, their jobs and everything hanging off them. Deleting a queue removes its
schedules, its idempotency keys, its per-minute stats and its worker subscriptions. A job takes
its dependency edges and its lease with it.

**Restrict — the reference is evidence and must not disappear quietly.**
`jobs.retry_policy_id`, `queues.retry_policy_id`, `jobs.worker_id`, `job_leases.worker_id` and
`job_executions.worker_id` all restrict. You cannot delete a retry policy that queues still use,
and you cannot delete a worker whose name appears in the execution ledger. The ledger is the
record of what ran where; a foreign key that let a worker vanish would leave it lying.

**Set null — the reference is context, not substance.** `audit_log.actor_id` and
`dead_letter_jobs.replayed_by` null out when a user is deleted: the action still happened and
the audit row must survive it. `jobs.schedule_id` nulls out when a schedule is deleted, because
the job it already produced is a real job and should finish.

`job_executions.job_id` and `job_logs.execution_id` carry **no foreign key at all**. That is
what lets the archiver move a job to cold storage without a cascade dragging its ledger along,
and it is why the archiver deletes children before parents by hand instead of trusting the
database to get the order right.

`schedules.timezone` is a foreign key to `tz_names`, a table seeded from
`pg_timezone_names`. A timezone that Postgres does not recognise cannot be stored, so a
schedule can never be created that the cron materialiser will later fail to interpret.

## Normalization, and where it is broken on purpose

The relational core is in third normal form. Roles live on the membership, not duplicated onto
the user; retry policy is referenced by queues and jobs rather than copied into each; timezone
names are a lookup table.

Three denormalizations are deliberate, and each buys something specific.

**`queues.in_flight`** counts claimed and running jobs. It is derivable — count the jobs — but
deriving it on every claim means an index scan proportional to concurrency on the hottest path
in the system. Holding the counter turns the concurrency cap into a single-row comparison. The
cost is that it can drift if a process dies between reserving a slot and claiming it, always
in the safe direction (too high, so the queue admits fewer), and `fl.reconcile_in_flight()`
repairs it.

**`jobs.pending_deps`** counts unfinished parents. Same trade: without it, deciding whether a
job is runnable means joining `job_dependencies` on every promotion sweep. With it, the
promoter's partial index can carry `pending_deps = 0` in its predicate and never look at the
edges at all.

**`queue_stats_minute`** is a rollup of what the ledger already knows. Computing throughput
and percentiles from `job_executions` on demand was measured at 635 ms on a dashboard endpoint
that polls; the rollup makes it an index lookup.

`job_executions.queue_id` duplicates the job's queue. It exists so the circuit breaker can read
a queue's recent outcomes without joining back to `jobs`.

## Indexes

The index that matters most is on `jobs`:

```sql
create index idx_jobs_claimable on jobs (queue_id, priority desc, run_at, id)
  where status = 'queued';
```

The `where` clause is the whole design. The index contains only rows that can be claimed, so it
stays the size of the backlog rather than the size of the table. A claim reads 54 buffers at
twenty thousand rows and 104 at a million — fifty times the rows for under twice the reads. The
column order matches the claim's `order by` exactly, so the index supplies the ordering and
there is no sort.

The rest of the `jobs` indexes are partial wherever the predicate excludes terminal rows, which
keeps them small and keeps them from being written when a job finishes:

| index | serves |
|---|---|
| `idx_jobs_promote (run_at) where scheduled/retry_wait and pending_deps = 0` | the promoter |
| `idx_jobs_queued_age (created_at) where queued` | the priority aging sweep |
| `idx_jobs_archivable (finished_at) where terminal` | the archiver picking work |
| `idx_jobs_inflight (queue_id) where claimed/running` | reconciling the counter |
| `idx_jobs_list (project_id, status, created_at desc, id desc)` | the job explorer's keyset pages |
| `uq_jobs_schedule_tick (schedule_id, scheduled_for)` | one job per cron tick, ever |

That last one is a correctness device, not a performance one. A unique index on immutable
columns is what makes a cron tick fire exactly once no matter how many schedulers are running
or how often they retry.

Elsewhere: `uq_exec_one_open on job_executions (job_id) where finished_at is null` makes two
simultaneously-open attempts on one job impossible. `idx_events_tail (project_id, id)` serves
the event stream's cursor. `idx_exec_breaker (queue_id, finished_at desc) where outcome is not
null` serves the circuit breaker's window.

Every index on `jobs` is also a cost: no update to a job can be a HOT update, because `status`
appears in the key or predicate of several of them, so each transition writes a new heap tuple
and an entry in every index. Measured at about 4,800 bytes of WAL per job. Indexes here are
added on evidence: five have been dropped again, one an exact duplicate of a unique constraint,
two strict prefixes of one, one with no reader, and one whose 834x saving on a page a human
opens did not justify a write on every completion.

## Partitioning and retention

`events`, `job_logs` and the three archive tables are partitioned by day. Retention is
`drop table`, not `delete`, because deleting two million event rows was measured at 5.5 s and
left permanent bloat that had to be earned back with `vacuum full` every cycle. Dropping a
partition is instant and leaves nothing behind.

There are **no default partitions**, deliberately: a default partition silently swallows rows
that belong to a day nobody created, and then blocks that day's partition from being created
later. The scheduler cuts partitions thirty days ahead. If it stops for longer than that,
inserts start failing — which is loud, and preferable to silent misfiling.

Hot data (events, logs) is kept seven days; the archives ninety. They are separate settings
because an archive that cannot outlive the hot table is not an archive.

## The constraints that carry the invariants

Structure is enforced where it can be, rather than in application code:

- `jobs_terminal_has_finished` — a job is terminal exactly when it has a `finished_at`.
- `jobs_active_has_owner` — claimed or running implies a worker, a claim time and a deadline.
- `jobs_deps_only_while_scheduled` — a job waiting on parents is `scheduled` and nothing else.
- `jobs_fence_matches_attempts` — `fence >= attempt_count`, so the two counters cannot be
  confused for one another.
- `je_outcome_with_finish` — an execution has an outcome exactly when it has finished.
- `queues_open_has_deadline` — an open circuit breaker has a time it reopens at.

The state machine itself is a trigger checking `fl.legal_transition`, whose nineteen edges are
asserted at migration time to match the `job_transitions` table. `make diagram` draws that table,
so the picture cannot drift from the rule.

## Reading the ledger

`job_executions` is append-only per attempt: one row per try, carrying the attempt number, the
replay generation, the fence it held, the worker that ran it, the outcome, the error and the
duration. `job_logs` hangs off the execution, so a log line is attributed to a specific attempt
rather than to the job in general. Nothing is overwritten on retry, which is what makes
`GET /jobs/{id}` able to show every attempt with its own logs and its own failure.
