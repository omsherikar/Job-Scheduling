package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type dlqItem struct {
	JobID        uuid.UUID  `json:"job_id"`
	QueueID      uuid.UUID  `json:"queue_id"`
	Reason       string     `json:"reason"`
	ErrorClass   *string    `json:"last_error_class"`
	ErrorMessage *string    `json:"last_error_message"`
	DeadAt       time.Time  `json:"dead_at"`
	ReplayedAt   *time.Time `json:"replayed_at"`
}

const listDLQHead = `
select job_id, queue_id, reason, last_error_class, last_error_message, dead_at, replayed_at
from dead_letter_jobs`

const listDLQTail = `order by dead_at desc, job_id desc`

const replaySQL = `
update jobs set
  status = 'queued',
  attempt_count = 0,
  replay_generation = replay_generation + 1,
  run_at = fl.now(),
  finished_at = null,
  worker_id = null
where id = $1 and status = 'dead_letter'
returning replay_generation`

const markReplayedSQL = `update dead_letter_jobs set replayed_at = fl.now(), replayed_by = $2 where job_id = $1`

func (s *Server) listDLQ(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var queueFilter *uuid.UUID
	if v := r.URL.Query().Get("queue"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			return result{}, badRequest("invalid queue")
		}
		queueFilter = &id
	}
	cursorTime, cursorID, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		return result{}, err
	}
	limit := pageLimit(r)

	var qb query
	qb.eq("project_id", sc.projectID)
	if queueFilter != nil {
		qb.eq("queue_id", *queueFilter)
	}
	qb.before("(dead_at, job_id)", cursorTime, cursorID)

	rows, err := tx.Query(ctx, qb.build(listDLQHead, listDLQTail, limit), qb.args...)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()
	out := []dlqItem{}
	for rows.Next() {
		var it dlqItem
		if err := rows.Scan(&it.JobID, &it.QueueID, &it.Reason, &it.ErrorClass,
			&it.ErrorMessage, &it.DeadAt, &it.ReplayedAt); err != nil {
			return result{}, err
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return result{}, err
	}

	next := ""
	if len(out) == limit {
		last := out[len(out)-1]
		next = encodeCursor(last.DeadAt, last.JobID)
	}
	return result{body: map[string]any{"items": out, "next_cursor": next}}, nil
}

func (s *Server) replayJob(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var gen int
	err := tx.QueryRow(ctx, replaySQL, sc.entityID).Scan(&gen)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, conflict("job is not in the dead letter queue")
	}
	if err != nil {
		return result{}, err
	}
	if _, err := tx.Exec(ctx, markReplayedSQL, sc.entityID, sc.userID); err != nil {
		return result{}, err
	}
	if err := emit(ctx, tx, "job.replayed", sc.entityID, sc.projectID, map[string]any{"replay_generation": gen}); err != nil {
		return result{}, err
	}
	return result{
		status: http.StatusOK,
		body:   map[string]any{"id": sc.entityID, "status": "queued", "replay_generation": gen},
		audit:  map[string]any{"replay_generation": gen},
	}, nil
}
