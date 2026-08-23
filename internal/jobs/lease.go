package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const extendSQL = `
update job_leases l set
  expires_at = least(fl.now() + make_interval(secs => $3), j.deadline_at),
  progress = coalesce($4::jsonb, l.progress),
  updated_at = fl.now()
from jobs j
where l.job_id = $1 and j.id = $1 and j.fence = $2 and j.status = 'running'`

const probeSQL = `
select j.fence, j.status::text, (j.deadline_at is not null and j.deadline_at < fl.now())
  from jobs j where j.id = $1`

const violationSQL = `
insert into job_fence_violations
  (job_id, worker_id, attempted, held_fence, actual_fence, actual_status)
values ($1, $2, 'report', $3, $4, $5::job_status)`

func ExtendLease(ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID, fence int64, lease time.Duration, progress json.RawMessage) (bool, error) {
	var p any
	if len(progress) > 0 {
		p = string(progress)
	}
	tag, err := pool.Exec(ctx, extendSQL, jobID, fence, lease.Seconds(), p)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func Probe(ctx context.Context, pool *pgxpool.Pool, jobID uuid.UUID, fence int64, workerID uuid.UUID) (Status, error) {
	var curFence int64
	var status string
	var pastDeadline bool
	err := pool.QueryRow(ctx, probeSQL, jobID).Scan(&curFence, &status, &pastDeadline)
	if errors.Is(err, pgx.ErrNoRows) {
		return StatusFenced, violation(ctx, pool, jobID, workerID, fence, nil, nil)
	}
	if err != nil {
		return "", err
	}

	switch {
	case curFence != fence:
		return StatusFenced, violation(ctx, pool, jobID, workerID, fence, &curFence, &status)
	case status == "claimed" || status == "running":
		return StatusLive, nil
	case status == "cancelled":
		return StatusCancelled, nil
	case pastDeadline:
		return StatusTimeout, nil
	default:
		return StatusFenced, violation(ctx, pool, jobID, workerID, fence, &curFence, &status)
	}
}

func CloseExecution(ctx context.Context, pool *pgxpool.Pool, exec uuid.UUID, st Status) error {
	outcome, ok := execOutcome[st]
	if !ok {
		return nil
	}
	_, err := pool.Exec(ctx, closeExecSQL, exec, outcome, nil, nil)
	return err
}

var execOutcome = map[Status]string{
	StatusCancelled: "cancelled",
	StatusFenced:    "fenced",
	StatusTimeout:   "timeout",
}

func violation(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, held int64, actualFence *int64, actualStatus *string) error {
	_, err := pool.Exec(ctx, violationSQL, jobID, workerID, held, actualFence, actualStatus)
	return err
}

const appendLogsSQL = `
insert into job_logs (execution_id, ts, level, message)
select $1, fl.now(), l.level, l.message
  from unnest($2::text[], $3::text[]) as l(level, message)`

type LogLine struct {
	Level   string
	Message string
}

func AppendLogs(ctx context.Context, pool *pgxpool.Pool, exec uuid.UUID, lines []LogLine) error {
	levels := make([]string, len(lines))
	messages := make([]string, len(lines))
	for i, l := range lines {
		levels[i], messages[i] = l.Level, l.Message
	}
	_, err := pool.Exec(ctx, appendLogsSQL, exec, levels, messages)
	return err
}
