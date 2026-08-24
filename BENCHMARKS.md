# Numbers

Measured on PostgreSQL 17.11 in docker on an 8 core, 8 GB laptop, `shared_buffers=1GB`,
`random_page_cost=1.1`, `work_mem=16MB`, `fsync=on`, `synchronous_commit=on`. The
absolute figures are specific to that machine; what carries over is how each number
moves with load or table size.

## Claim latency against table size

Server-side execution of the claim, batch 50, everything
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
million, fifty times the rows for under twice the reads. Once the repetitions stop hiding the
cold index descent the growth is sublinear, not flat.

## Enqueue to start

200 jobs arriving 15 ms apart, concurrency 8, one second poll:

| | p50 | p95 | p99 |
|---|---|---|---|
| polling only | 7,472 ms | 13,958 ms | 14,624 ms |
| notify on | 17 ms | 174 ms | 278 ms |

The tail is queueing rather than wakeup latency: without the push path a worker claims once
per interval, so arrivals faster than one batch per poll back up. A shorter poll narrows the
gap.

## Correctness under fault injection

Four workers on a 150 ms lease, a reaper loop, a
goroutine terminating idle-in-transaction backends and another expiring random leases, over
400 jobs: every job reaches a terminal state, no job has two successful executions in one
replay generation, and 120 to 200 fenced writes are recorded. The counter
a queue keeps of what is in flight is never lower than what is actually running; it can be
left high by a connection dying between reserving a slot and claiming it, and
`fl.reconcile_in_flight` restores it exactly.

## Recovery after `kill -9`

Four worker processes holding sixteen jobs with open executions,
killed outright, reaper every half second, default thirty second lease: **31.1 s** until every
job was runnable again, all sixteen executions closed as `lost`, no lease orphaned and no
counter drift. The lease length is what bounds recovery; a shorter lease recovers sooner and
costs more heartbeats.

## Cost per job

Two thousand jobs claimed, run and completed at concurrency 8, on an otherwise idle
database with the statistics reset first:

| | per job | per million |
|---|---|---|
| WAL written | 4,824 bytes | 4.8 GB |
| database active time | 3.25 ms | 0.9 cpu-hours |
| commits | 1.33 | 1.33 M |

Most of that one and a third commits is starting the attempt, which is the one step still done
a job at a time: claiming takes two commits but spreads them over everything it claims, and
reporting spreads one over the batch it reports. The WAL figure is a steady-state one; a cold
run pays several times that in full-page images right after a checkpoint.

## Admission throughput against shard count

`shards` splits a queue's concurrency counter across that many rows so admission is not
one lock. Measured on one queue with a warm cache, N goroutines each reserving and
immediately releasing four slots:

| concurrent claimers | shards=1 | shards=2 | shards=4 | shards=8 |
|---|---|---|---|---|
| 16 | 1455/s | 1189/s | 1437/s | 1554/s |
| 128 | 410/s | 485/s | 707/s | 989/s |

At sixteen the round trip dominates and the counter is not the bottleneck, so shards make
no difference. At a hundred and twenty eight the row lock is the bottleneck and eight
shards carry about two and a half times the admissions. A second run of the same
measurement put the hundred and twenty eight row at 265, 595, 694 and 1159, so the
multiplier varies between runs even though the ordering holds.

Spreading the same workers over eight separate queues was measured at 4.8 times the
throughput of one queue, which is the manual version of the same effect. Leave `shards`
at 1 unless a queue is measurably starved on admission.
