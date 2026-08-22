package jobs

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrPermanent = errors.New("permanent")
	ErrFenced    = errors.New("fenced")
)

type Status string

const (
	StatusFenced    Status = "fenced"
	StatusCancelled Status = "cancelled"
	StatusTimeout   Status = "timeout"
	StatusLive      Status = "live"
)

type Execution struct {
	ID      uuid.UUID
	Attempt int
}

const startSQL = `
with upd as (
  update jobs set
    status = 'running',
    started_at = fl.now(),
    attempt_count = attempt_count + 1
  where id = $1 and fence = $2 and status = 'claimed'
  returning id, queue_id, attempt_count, replay_generation, fence
)
insert into job_executions (job_id, queue_id, attempt, replay_generation, fence, worker_id)
select id, queue_id, attempt_count, replay_generation, fence, $3 from upd
returning id, attempt`

const completeJobSQL = `
update jobs set status = 'completed', finished_at = fl.now(), worker_id = null
 where id = $1 and fence = $2 and status = 'running'
returning queue_id, project_id`

const closeExecSQL = `
update job_executions set outcome = $2::exec_outcome, finished_at = fl.now(),
       error_class = $3, error_message = $4
 where id = $1 and finished_at is null`

const dropLeaseSQL = `delete from job_leases where job_id = $1`

const failJobSQL = `
update jobs j set
  status = case
    when $3 then 'dead_letter'::job_status
    when j.attempt_count >= rp.max_attempts then 'dead_letter'::job_status
    else 'retry_wait'::job_status end,
  run_at = case
    when $3 or j.attempt_count >= rp.max_attempts then j.run_at
    else fl.now() + fl.backoff(rp.kind, rp.base_delay_ms, rp.max_delay_ms, rp.jitter, j.attempt_count) end,
  finished_at = case when $3 or j.attempt_count >= rp.max_attempts then fl.now() else null end,
  worker_id = null
from retry_policies rp
where j.id = $1 and j.fence = $2 and j.status = 'running' and rp.id = j.retry_policy_id
returning j.queue_id, j.project_id, j.status`

const deadLetterSQL = `
insert into dead_letter_jobs
  (job_id, queue_id, project_id, reason, last_error_class, last_error_message, execution_history)
values ($1, $2, $3, $4, $5, $6,
  coalesce((select jsonb_agg(to_jsonb(e) order by e.attempt)
              from job_executions e where e.job_id = $1), '[]'::jsonb))
on conflict (job_id) do nothing`

const eventSQL = `
insert into events (topic, entity_id, project_id, payload)
values ($1, $2, $3, jsonb_build_object('status', $4::text, 'outcome', $5::text))`

func Start(ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID, fence int64, workerID uuid.UUID) (Execution, error) {
	var e Execution
	err := pool.QueryRow(ctx, startSQL, jobID, fence, workerID).Scan(&e.ID, &e.Attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Execution{}, ErrFenced
	}
	if err != nil {
		return Execution{}, err
	}
	return e, nil
}

func Complete(ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID, fence int64, exec uuid.UUID) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var queueID, projectID uuid.UUID
	err = tx.QueryRow(ctx, completeJobSQL, jobID, fence).Scan(&queueID, &projectID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrFenced
	}
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, closeExecSQL, exec, "success", nil, nil); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, dropLeaseSQL, jobID); err != nil {
		return err
	}
	if err := Release(ctx, tx, queueID, 1); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, eventSQL, "job.completed", jobID, projectID, "completed", "success"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func Fail(ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID, fence int64, exec uuid.UUID, cause error) error {
	permanent := errors.Is(cause, ErrPermanent)
	outcome := "retryable_error"
	if permanent {
		outcome = "permanent_error"
	}
	var msg string
	if cause != nil {
		msg = cause.Error()
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var queueID, projectID uuid.UUID
	var status string
	err = tx.QueryRow(ctx, failJobSQL, jobID, fence, permanent).Scan(&queueID, &projectID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrFenced
	}
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, closeExecSQL, exec, outcome, outcome, msg); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, dropLeaseSQL, jobID); err != nil {
		return err
	}

	topic := "job.failed"
	if status == "dead_letter" {
		topic = "job.dead_lettered"
		reason := "attempts_exhausted"
		if permanent {
			reason = "permanent_error"
		}
		if _, err := tx.Exec(ctx, deadLetterSQL, jobID, queueID, projectID, reason, outcome, msg); err != nil {
			return err
		}
	}
	if err := Release(ctx, tx, queueID, 1); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, eventSQL, topic, jobID, projectID, status, outcome); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
