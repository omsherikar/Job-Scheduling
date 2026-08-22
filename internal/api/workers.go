package api

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type workerView struct {
	ID             uuid.UUID `json:"id"`
	Hostname       string    `json:"hostname"`
	PID            int       `json:"pid"`
	State          string    `json:"state"`
	MaxConcurrency int       `json:"max_concurrency"`
	Leases         int       `json:"leases"`
	StartedAt      time.Time `json:"started_at"`
	LastSeenAt     time.Time `json:"last_seen_at"`
}

const listWorkersSQL = `
select w.id, w.hostname, w.pid, w.state, w.max_concurrency, w.started_at, w.last_seen_at,
       (select count(*) from job_leases l where l.worker_id = w.id)
from workers w
where w.project_id = $1
order by w.last_seen_at desc`

func (s *Server) listWorkers(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	rows, err := tx.Query(ctx, listWorkersSQL, sc.projectID)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()
	out := []workerView{}
	for rows.Next() {
		var w workerView
		if err := rows.Scan(&w.ID, &w.Hostname, &w.PID, &w.State, &w.MaxConcurrency,
			&w.StartedAt, &w.LastSeenAt, &w.Leases); err != nil {
			return result{}, err
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return result{}, err
	}
	return result{status: http.StatusOK, body: map[string]any{"items": out}}, nil
}
