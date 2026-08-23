package archiver

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pickSQL = `
select id from jobs j
 where j.status in ('completed', 'cancelled', 'dead_letter')
   and j.finished_at < fl.now() - make_interval(secs => $1)
   and (j.status <> 'dead_letter' or j.finished_at < fl.now() - make_interval(secs => $2))
   and not exists (select 1 from job_executions x where x.job_id = j.id and x.finished_at is null)
 order by j.finished_at
 for update skip locked
 limit $3`

const jobPartitionsSQL = `
select fl.ensure_partitions('jobs_archive', min(finished_at)::date, max(finished_at)::date)
  from jobs where id = any($1)`

const execPartitionsSQL = `
select fl.ensure_partitions('job_executions_archive', min(finished_at)::date, max(finished_at)::date)
  from job_executions where job_id = any($1)`

const logPartitionsSQL = `
select fl.ensure_partitions('job_logs_archive', min(l.ts)::date, max(l.ts)::date)
  from job_logs l join job_executions x on x.id = l.execution_id
 where x.job_id = any($1)`

const deadLetterPartitionsSQL = `
select fl.ensure_partitions('dead_letter_jobs_archive', min(dead_at)::date, max(dead_at)::date)
  from dead_letter_jobs where job_id = any($1)`

const moveLogsSQL = `
with moved as (
  delete from job_logs l
   using job_executions x
   where x.id = l.execution_id and x.job_id = any($1)
  returning l.id, l.execution_id, l.ts, l.level, l.message, l.meta
)
insert into job_logs_archive (id, execution_id, ts, level, message, meta)
select * from moved`

const moveDeadLettersSQL = `
with moved as (
  delete from dead_letter_jobs where job_id = any($1)
  returning job_id, queue_id, project_id, reason, last_error_class, last_error_message,
            execution_history, dead_at, replayed_at, replayed_by
)
insert into dead_letter_jobs_archive
  (job_id, queue_id, project_id, reason, last_error_class, last_error_message,
   execution_history, dead_at, replayed_at, replayed_by)
select * from moved`

const moveExecsSQL = `
with moved as (
  delete from job_executions where job_id = any($1)
  returning id, job_id, queue_id, attempt, replay_generation, fence, worker_id, outcome,
            error_class, error_message, error_stack, started_at, finished_at, duration_ms
)
insert into job_executions_archive
  (id, job_id, queue_id, attempt, replay_generation, fence, worker_id, outcome,
   error_class, error_message, error_stack, started_at, finished_at, duration_ms)
select * from moved`

const moveJobsSQL = `
with moved as (
  delete from jobs where id = any($1)
  returning id, project_id, queue_id, schedule_id, scheduled_for, batch_id, type, payload, status,
            priority, run_at, fence, attempt_count, replay_generation, snooze_count, worker_id,
            deadline_at, timeout_ms, retry_policy_id, pending_deps, created_at, claimed_at,
            started_at, finished_at
)
insert into jobs_archive
  (id, project_id, queue_id, schedule_id, scheduled_for, batch_id, type, payload, status,
   priority, run_at, fence, attempt_count, replay_generation, snooze_count, worker_id,
   deadline_at, timeout_ms, retry_policy_id, pending_deps, created_at, claimed_at,
   started_at, finished_at, terminal_at)
select *, finished_at from moved`

const pruneSQL = `delete from idempotency_keys where expires_at < fl.now()`

type Config struct {
	Every      time.Duration
	Prune      time.Duration
	After      time.Duration
	DeadLetter time.Duration
	Batch      int
}

func DefaultConfig() Config {
	return Config{
		Every:      time.Minute,
		Prune:      10 * time.Minute,
		After:      24 * time.Hour,
		DeadLetter: 30 * 24 * time.Hour,
		Batch:      500,
	}
}

func Archive(ctx context.Context, pool *pgxpool.Pool, after, deadLetter time.Duration, limit int) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, pickSQL, after.Seconds(), deadLetter.Seconds(), limit)
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

	for _, q := range []string{logPartitionsSQL, execPartitionsSQL, jobPartitionsSQL, deadLetterPartitionsSQL} {
		if _, err := tx.Exec(ctx, q, ids); err != nil {
			return 0, err
		}
	}
	for _, q := range []string{moveLogsSQL, moveExecsSQL, moveDeadLettersSQL, moveJobsSQL} {
		if _, err := tx.Exec(ctx, q, ids); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(ids), nil
}

func Prune(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	tag, err := pool.Exec(ctx, pruneSQL)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func Run(ctx context.Context, pool *pgxpool.Pool, cfg Config) error {
	move := time.NewTicker(cfg.Every)
	defer move.Stop()
	prune := time.NewTicker(cfg.Prune)
	defer prune.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-move.C:
			for {
				n, err := Archive(ctx, pool, cfg.After, cfg.DeadLetter, cfg.Batch)
				if err != nil {
					if ctx.Err() == nil {
						log.Printf("archive: %v", err)
					}
					break
				}
				if n < cfg.Batch {
					break
				}
			}
		case <-prune.C:
			if _, err := Prune(ctx, pool); err != nil && ctx.Err() == nil {
				log.Printf("prune: %v", err)
			}
		}
	}
}
