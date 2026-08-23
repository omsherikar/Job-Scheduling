package api

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const projectQueuesSQL = `
select q.id, q.name, q.paused, q.max_concurrency, q.in_flight, q.breaker_state::text,
       q.breaker_open_until, q.rl_limit_per_sec::double precision,
       q.rl_limit_per_sec > 0 and q.rl_tokens < 1,
       (select count(*)::int from workers w
          join worker_queues wq on wq.worker_id = w.id
         where wq.queue_id = q.id and w.state = 'active'
           and w.last_seen_at > fl.now() - make_interval(secs => $2))
from queues q
where q.project_id = $1
order by q.name`

const projectTiersSQL = `
select queue_id, priority, extract(epoch from (fl.now() - min(run_at)))::int, count(*)::int
from jobs
where queue_id = any($1) and status = 'queued' and run_at <= fl.now()
group by queue_id, priority
order by queue_id, priority desc`

const projectThroughputSQL = `
select queue_id, coalesce(sum(completed), 0)::int, coalesce(sum(failed), 0)::int,
       coalesce(sum(dead_lettered), 0)::int, max(duration_ms_p95)
from queue_stats_minute
where queue_id = any($1) and minute >= fl.now() - interval '60 minutes'
group by queue_id`

type queueHealth struct {
	Queue        queueBrief `json:"queue"`
	Tiers        []tier     `json:"tiers"`
	LiveWorkers  int        `json:"live_workers"`
	BreakerUntil *time.Time `json:"breaker_open_until"`
	RateLimited  bool       `json:"rate_limited"`
	Saturated    bool       `json:"saturated"`
	LastHour     lastHour   `json:"last_hour"`
	P95          *int       `json:"duration_ms_p95"`
}

type queueBrief struct {
	ID             uuid.UUID `json:"id"`
	Name           string    `json:"name"`
	Paused         bool      `json:"paused"`
	MaxConcurrency int       `json:"max_concurrency"`
	InFlight       int       `json:"in_flight"`
	BreakerState   string    `json:"breaker_state"`
	RateLimit      float64   `json:"rl_limit_per_sec"`
}

type lastHour struct {
	Completed    int `json:"completed"`
	Failed       int `json:"failed"`
	DeadLettered int `json:"dead_lettered"`
}

func (s *Server) projectHealth(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	rows, err := tx.Query(ctx, projectQueuesSQL, sc.projectID, staleWorker.Seconds())
	if err != nil {
		return result{}, err
	}
	out := []queueHealth{}
	at := map[uuid.UUID]int{}
	for rows.Next() {
		var h queueHealth
		h.Tiers = []tier{}
		if err := rows.Scan(&h.Queue.ID, &h.Queue.Name, &h.Queue.Paused, &h.Queue.MaxConcurrency,
			&h.Queue.InFlight, &h.Queue.BreakerState, &h.BreakerUntil, &h.Queue.RateLimit,
			&h.RateLimited, &h.LiveWorkers); err != nil {
			rows.Close()
			return result{}, err
		}
		h.Saturated = h.Queue.InFlight >= h.Queue.MaxConcurrency
		at[h.Queue.ID] = len(out)
		out = append(out, h)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result{}, err
	}
	if len(out) == 0 {
		return result{body: map[string]any{"items": out}}, nil
	}

	ids := make([]uuid.UUID, 0, len(out))
	for _, h := range out {
		ids = append(ids, h.Queue.ID)
	}

	rows, err = tx.Query(ctx, projectTiersSQL, ids)
	if err != nil {
		return result{}, err
	}
	for rows.Next() {
		var id uuid.UUID
		var t tier
		if err := rows.Scan(&id, &t.Priority, &t.OldestSeconds, &t.Ready); err != nil {
			rows.Close()
			return result{}, err
		}
		out[at[id]].Tiers = append(out[at[id]].Tiers, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result{}, err
	}

	rows, err = tx.Query(ctx, projectThroughputSQL, ids)
	if err != nil {
		return result{}, err
	}
	for rows.Next() {
		var id uuid.UUID
		var h lastHour
		var p95 *int
		if err := rows.Scan(&id, &h.Completed, &h.Failed, &h.DeadLettered, &p95); err != nil {
			rows.Close()
			return result{}, err
		}
		out[at[id]].LastHour = h
		out[at[id]].P95 = p95
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result{}, err
	}

	return result{body: map[string]any{"items": out}}, nil
}
