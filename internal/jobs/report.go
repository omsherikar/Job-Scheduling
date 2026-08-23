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

const closeExecSQL = `
update job_executions set outcome = $2::exec_outcome, finished_at = fl.now(),
       error_class = $3, error_message = $4
 where id = $1 and finished_at is null`

const reportSuccessSQL = `select queue from fl.report_success($1, $2, $3)`

const reportFailureSQL = `select queue from fl.report_failure($1, $2, $3::exec_outcome, $4, $5, $6)`

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
	var queueID uuid.UUID
	err := pool.QueryRow(ctx, reportSuccessSQL, jobID, fence, exec).Scan(&queueID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrFenced
	}
	return err
}

func Fail(ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID, fence int64, cause error) error {
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

	var queueID uuid.UUID
	err = tx.QueryRow(ctx, reportFailureSQL, jobID, fence, outcome, outcome, msg, permanent).Scan(&queueID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrFenced
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

const reportBatchSQL = `select job from fl.report_success_batch($1, $2, $3)`

type Done struct {
	JobID uuid.UUID
	Fence int64
	Exec  uuid.UUID
}

func CompleteBatch(ctx context.Context, pool *pgxpool.Pool, batch []Done) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(batch))
	fences := make([]int64, len(batch))
	execs := make([]uuid.UUID, len(batch))
	for i, d := range batch {
		ids[i], fences[i], execs[i] = d.JobID, d.Fence, d.Exec
	}

	rows, err := pool.Query(ctx, reportBatchSQL, ids, fences, execs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
