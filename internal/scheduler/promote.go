package scheduler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const promoteSelectSQL = `
select id from jobs
 where status in ('scheduled', 'retry_wait') and run_at <= fl.now() and pending_deps = 0
 order by run_at
 for update skip locked
 limit $1`

const promoteUpdateSQL = `
with promoted as (
  update jobs set status = 'queued' where id = any($1) returning id, project_id
),
ev as (
  select fl.emit('job.queued', id, project_id, '{}'::jsonb) from promoted
)
select count(*) from ev`

const ageSQL = `
update jobs set priority = priority + 1
 where id in (
   select id from jobs
    where status = 'queued'
      and created_at < fl.now() - make_interval(secs => $1)
      and priority < $2
    limit $3
 )`

func Promote(ctx context.Context, pool *pgxpool.Pool, limit int) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, promoteSelectSQL, limit)
	if err != nil {
		return 0, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	var n int
	if err := tx.QueryRow(ctx, promoteUpdateSQL, ids).Scan(&n); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

func Age(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration, ceiling, limit int) (int, error) {
	tag, err := pool.Exec(ctx, ageSQL, olderThan.Seconds(), ceiling, limit)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
