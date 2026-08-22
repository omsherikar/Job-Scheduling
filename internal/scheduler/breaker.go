package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const breakerQueuesSQL = `select id, project_id, breaker_state::text, breaker_open_until from queues`

const breakerWindowSQL = `
select outcome::text from job_executions
 where queue_id = $1 and outcome is not null
 order by finished_at desc
 limit $2`

const breakerProbeSQL = `
select outcome::text from job_executions
 where queue_id = $1 and outcome is not null and finished_at >= $2
 order by finished_at desc
 limit 1`

const breakerOpenSQL = `
with sw as (
  update queues set breaker_state = 'open',
         breaker_open_until = fl.now() + make_interval(secs => $2),
         breaker_probe_budget = 0
   where id = $1 and breaker_state = 'closed'
  returning id, project_id
),
ev as (
  insert into events (topic, entity_id, project_id, payload)
  select 'queue.breaker_opened', id, project_id, '{}'::jsonb from sw
)
select count(*) from sw`

const breakerReopenSQL = `
with sw as (
  update queues set breaker_state = 'open',
         breaker_open_until = fl.now() + make_interval(secs => $2),
         breaker_probe_budget = 0
   where id = $1 and breaker_state = 'half_open'
  returning id, project_id
),
ev as (
  insert into events (topic, entity_id, project_id, payload)
  select 'queue.breaker_reopened', id, project_id, '{}'::jsonb from sw
)
select count(*) from sw`

const breakerCloseSQL = `
with sw as (
  update queues set breaker_state = 'closed', breaker_open_until = null, breaker_probe_budget = 0
   where id = $1 and breaker_state = 'half_open'
  returning id, project_id
),
ev as (
  insert into events (topic, entity_id, project_id, payload)
  select 'queue.breaker_closed', id, project_id, '{}'::jsonb from sw
)
select count(*) from sw`

type queueBreaker struct {
	id        uuid.UUID
	projectID uuid.UUID
	state     string
	openUntil *time.Time
}

func isFailure(outcome string) bool {
	switch outcome {
	case "retryable_error", "permanent_error", "timeout", "lost":
		return true
	}
	return false
}

func Breaker(ctx context.Context, pool *pgxpool.Pool, window, trip int, cooldown time.Duration) (int, error) {
	rows, err := pool.Query(ctx, breakerQueuesSQL)
	if err != nil {
		return 0, err
	}
	var qs []queueBreaker
	for rows.Next() {
		var q queueBreaker
		if err := rows.Scan(&q.id, &q.projectID, &q.state, &q.openUntil); err != nil {
			rows.Close()
			return 0, err
		}
		qs = append(qs, q)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	changed := 0
	for _, q := range qs {
		switch q.state {
		case "closed":
			failures, err := countFailures(ctx, pool, q.id, window)
			if err != nil {
				return 0, err
			}
			if failures >= trip {
				var n int
				if err := pool.QueryRow(ctx, breakerOpenSQL, q.id, cooldown.Seconds()).Scan(&n); err != nil {
					return 0, err
				}
				changed += n
			}
		case "half_open":
			var outcome string
			err := pool.QueryRow(ctx, breakerProbeSQL, q.id, q.openUntil).Scan(&outcome)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return 0, err
			}
			if outcome == "success" {
				var n int
				if err := pool.QueryRow(ctx, breakerCloseSQL, q.id).Scan(&n); err != nil {
					return 0, err
				}
				changed += n
			} else if isFailure(outcome) {
				var n int
				if err := pool.QueryRow(ctx, breakerReopenSQL, q.id, cooldown.Seconds()).Scan(&n); err != nil {
					return 0, err
				}
				changed += n
			}
		}
	}

	return changed, nil
}

func countFailures(ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID, window int) (int, error) {
	rows, err := pool.Query(ctx, breakerWindowSQL, queueID, window)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var outcome string
		if err := rows.Scan(&outcome); err != nil {
			return 0, err
		}
		if isFailure(outcome) {
			n++
		}
	}
	return n, rows.Err()
}
