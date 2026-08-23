package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

const staleWorker = 30 * time.Second

const statusCountsSQL = `select status::text, count(*) from jobs where queue_id = $1 group by status`

const oldestReadySQL = `
select extract(epoch from (fl.now() - min(run_at)))
from jobs
where queue_id = $1 and status = 'queued' and run_at <= fl.now()`

const oldestByTierSQL = `
select priority, extract(epoch from (fl.now() - min(run_at)))::int, count(*)::int
from jobs
where queue_id = $1 and status = 'queued' and run_at <= fl.now()
group by priority
order by priority desc`

const queueWorkersSQL = `
select count(*)::int from workers w
join worker_queues wq on wq.worker_id = w.id
where wq.queue_id = $1 and w.state = 'active'
  and w.last_seen_at > fl.now() - make_interval(secs => $2)`

const breakerUntilSQL = `select breaker_open_until, rl_limit_per_sec > 0 and rl_tokens < 1 from queues where id = $1`

const throughputSQL = `
select coalesce(sum(completed), 0), coalesce(sum(failed), 0), coalesce(sum(dead_lettered), 0)
from queue_stats_minute
where queue_id = $1 and minute >= fl.now() - interval '60 minutes'`

const latencySQL = `
select duration_ms_p50, duration_ms_p95
from queue_stats_minute
where queue_id = $1 and duration_ms_p50 is not null
order by minute desc limit 1`

type tier struct {
	Priority      int `json:"priority"`
	OldestSeconds int `json:"oldest_ready_seconds"`
	Ready         int `json:"ready"`
}

func (s *Server) queueStats(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var q queueView
	if err := scanQueue(tx.QueryRow(ctx, getQueueSQL, sc.entityID), &q); err != nil {
		return result{}, err
	}

	counts := map[string]int{}
	rows, err := tx.Query(ctx, statusCountsSQL, sc.entityID)
	if err != nil {
		return result{}, err
	}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			rows.Close()
			return result{}, err
		}
		counts[st] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result{}, err
	}

	var oldest *float64
	if err := tx.QueryRow(ctx, oldestReadySQL, sc.entityID).Scan(&oldest); err != nil {
		return result{}, err
	}

	tiers := []tier{}
	rows, err = tx.Query(ctx, oldestByTierSQL, sc.entityID)
	if err != nil {
		return result{}, err
	}
	for rows.Next() {
		var t tier
		if err := rows.Scan(&t.Priority, &t.OldestSeconds, &t.Ready); err != nil {
			rows.Close()
			return result{}, err
		}
		tiers = append(tiers, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result{}, err
	}

	var workers int
	if err := tx.QueryRow(ctx, queueWorkersSQL, sc.entityID, staleWorker.Seconds()).Scan(&workers); err != nil {
		return result{}, err
	}

	var breakerUntil *time.Time
	var throttled bool
	if err := tx.QueryRow(ctx, breakerUntilSQL, sc.entityID).Scan(&breakerUntil, &throttled); err != nil {
		return result{}, err
	}

	var completed, failed, deadLettered int
	if err := tx.QueryRow(ctx, throughputSQL, sc.entityID).Scan(&completed, &failed, &deadLettered); err != nil {
		return result{}, err
	}

	var p50, p95 *int
	err = tx.QueryRow(ctx, latencySQL, sc.entityID).Scan(&p50, &p95)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return result{}, err
	}

	return result{
		status: http.StatusOK,
		body: map[string]any{
			"queue":                q,
			"status_counts":        counts,
			"oldest_ready_seconds": oldest,
			"tiers":                tiers,
			"live_workers":         workers,
			"breaker_open_until":   breakerUntil,
			"rate_limited":         throttled,
			"saturated":            q.InFlight >= q.MaxConcurrency,
			"last_hour": map[string]any{
				"completed":     completed,
				"failed":        failed,
				"dead_lettered": deadLettered,
			},
			"duration_ms_p50": p50,
			"duration_ms_p95": p95,
		},
	}, nil
}
