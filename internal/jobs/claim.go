package jobs

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const admitSQL = `
select least(
         $2::int,
         greatest(max_concurrency - in_flight, 0),
         case when rl_limit_per_sec = 0 then $2::int
              else floor(least(rl_burst,
                     rl_tokens + extract(epoch from (fl.now() - rl_refilled_at)) * rl_limit_per_sec))::int
         end,
         case when breaker_state = 'closed' then $2::int else 1 end
       )
  from queues
 where id = $1
   and not paused
   and (breaker_state = 'closed'
        or (breaker_state = 'half_open' and breaker_probe_budget > 0)
        or (breaker_state = 'open' and coalesce(breaker_open_until, fl.now()) <= fl.now()))
 for no key update`

const claimSQL = `
with cand as materialized (
  select j.id
    from jobs j
   where j.queue_id = $1 and j.status = 'queued' and j.run_at <= fl.now()
   order by j.priority desc, j.run_at asc, j.id asc
   for update skip locked
   limit $2
),
acct as (
  update queues set
    in_flight = in_flight + (select count(*) from cand),
    rl_tokens = case when rl_limit_per_sec = 0 then rl_tokens
                     else least(rl_burst,
                                rl_tokens + extract(epoch from (fl.now() - rl_refilled_at)) * rl_limit_per_sec)
                          - (select count(*) from cand)
                end,
    rl_refilled_at = case when rl_limit_per_sec = 0 then rl_refilled_at else fl.now() end,
    breaker_state = case when breaker_state = 'open'
                          and coalesce(breaker_open_until, fl.now()) <= fl.now()
                         then 'half_open' else breaker_state end,
    breaker_probe_budget = case
      when breaker_state = 'open' and coalesce(breaker_open_until, fl.now()) <= fl.now()
        then 1 - (select count(*) from cand)::int
      when breaker_state = 'half_open'
        then breaker_probe_budget - (select count(*) from cand)::int
      else breaker_probe_budget
    end
  where id = $1
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

func admit(ctx context.Context, tx pgx.Tx, queueID uuid.UUID, freeSlots int) (int, error) {
	var n int
	err := tx.QueryRow(ctx, admitSQL, queueID, freeSlots).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}

func Claim(ctx context.Context, pool *pgxpool.Pool, req ClaimRequest) ([]Claimed, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	n, err := admit(ctx, tx, req.QueueID, req.FreeSlots)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}

	rows, err := tx.Query(ctx, claimSQL, pgx.QueryExecModeExec,
		req.QueueID, n, req.WorkerID, req.Lease.Seconds())
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

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
