package jobs

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const releaseGrace = 5 * time.Second

const admitSQL = `select fl.queue_admit($1, $2)`

const claimSQL = `
with cand as materialized (
  select j.id
    from jobs j
   where j.queue_id = $1 and j.status = 'queued' and j.run_at <= fl.now()
     and ($5::text[] is null or j.type = any($5))
   order by j.priority desc, j.run_at asc, j.id asc
   for update skip locked
   limit $2
),
upd as (
  update jobs j set
    status = 'claimed',
    fence = j.fence + 1,
    worker_id = $3,
    claimed_at = fl.now(),
    deadline_at = fl.now() + make_interval(secs => j.timeout_ms::double precision / 1000)
  from cand
  where j.id = cand.id
  returning j.id, j.fence, j.type, j.payload, j.attempt_count, j.deadline_at
),
lease as (
  insert into job_leases (job_id, fence, worker_id, expires_at, updated_at)
  select id, fence, $3, fl.now() + make_interval(secs => $4), fl.now()
    from upd
  on conflict (job_id) do update set
    fence = excluded.fence,
    worker_id = excluded.worker_id,
    expires_at = excluded.expires_at,
    updated_at = excluded.updated_at
)
select id, fence, type, payload, attempt_count, deadline_at from upd`

func admit(ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID, freeSlots int) (int, error) {
	var n int
	err := pool.QueryRow(ctx, admitSQL, queueID, freeSlots).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}

func Claim(ctx context.Context, pool *pgxpool.Pool, req ClaimRequest) ([]Claimed, error) {
	adm, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseGrace)
	n, err := admit(adm, pool, req.QueueID, req.FreeSlots)
	cancel()
	if err != nil || n == 0 {
		return nil, err
	}

	out, err := take(ctx, pool, req, n)
	if err != nil {
		give, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseGrace)
		pool.Exec(give, releaseSQL, req.QueueID, n)
		cancel()
		return nil, err
	}
	return out, nil
}

func take(ctx context.Context, pool *pgxpool.Pool, req ClaimRequest, n int) ([]Claimed, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, claimSQL, req.QueueID, n, req.WorkerID, req.Lease.Seconds(), req.Types)
	if err != nil {
		return nil, err
	}
	var out []Claimed
	for rows.Next() {
		var c Claimed
		if err := rows.Scan(&c.ID, &c.Fence, &c.Type, &c.Payload, &c.AttemptCount, &c.DeadlineAt); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(out) < n {
		if err := Release(ctx, tx, req.QueueID, n-len(out)); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
