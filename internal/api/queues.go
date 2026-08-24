package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type queueView struct {
	ID              uuid.UUID `json:"id"`
	ProjectID       uuid.UUID `json:"project_id"`
	Name            string    `json:"name"`
	Paused          bool      `json:"paused"`
	RetryPolicyID   uuid.UUID `json:"retry_policy_id"`
	DefaultPriority int       `json:"default_priority"`
	MaxConcurrency  int       `json:"max_concurrency"`
	Shards          int       `json:"shards"`
	InFlight        int       `json:"in_flight"`
	MaxDepth        *int      `json:"max_depth"`
	RateLimitPerSec float64   `json:"rl_limit_per_sec"`
	RateBurst       float64   `json:"rl_burst"`
	BreakerState    string    `json:"breaker_state"`
	CreatedAt       time.Time `json:"created_at"`
}

type queueReq struct {
	Name            *string  `json:"name"`
	RetryPolicyID   *string  `json:"retry_policy_id"`
	MaxConcurrency  *int     `json:"max_concurrency"`
	Shards          *int     `json:"shards"`
	MaxDepth        *int     `json:"max_depth"`
	DefaultPriority *int     `json:"default_priority"`
	RateLimitPerSec *float64 `json:"rl_limit_per_sec"`
	RateBurst       *float64 `json:"rl_burst"`
}

const policyOwnedSQL = `select 1 from retry_policies where id = $1 and project_id = $2`

const insertQueueSQL = `
insert into queues
  (project_id, name, retry_policy_id, max_concurrency, max_depth, default_priority,
   rl_limit_per_sec, rl_burst, shards)
values ($1, $2, $3, $4, $5, $6, $7::numeric, $8::numeric, $9)
returning id, paused, breaker_state, created_at`

const listQueuesSQL = `
select id, project_id, name, paused, retry_policy_id, default_priority, max_concurrency,
       shards, fl.in_flight(id), max_depth,
       rl_limit_per_sec::double precision, rl_burst::double precision,
       breaker_state, created_at
from queues where project_id = $1 order by name`

const getQueueSQL = `
select id, project_id, name, paused, retry_policy_id, default_priority, max_concurrency,
       shards, fl.in_flight(id), max_depth,
       rl_limit_per_sec::double precision, rl_burst::double precision,
       breaker_state, created_at
from queues where id = $1`

const updateQueueSQL = `
update queues set
  name = coalesce($2, name),
  max_concurrency = coalesce($3, max_concurrency),
  max_depth = coalesce($4, max_depth),
  default_priority = coalesce($5, default_priority),
  rl_limit_per_sec = coalesce($6::numeric, rl_limit_per_sec),
  rl_burst = coalesce($7::numeric, rl_burst),
  retry_policy_id = coalesce($8, retry_policy_id),
  shards = coalesce($9, shards)
where id = $1
returning id, project_id, name, paused, retry_policy_id, default_priority, max_concurrency,
          shards, fl.in_flight(id), max_depth,
          rl_limit_per_sec::double precision, rl_burst::double precision,
          breaker_state, created_at`

const deleteQueueSQL = `delete from queues where id = $1`
const setPausedSQL = `update queues set paused = $2 where id = $1 returning paused`

func (s *Server) createQueue(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req queueReq
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	name := ""
	if req.Name != nil {
		name = strings.TrimSpace(*req.Name)
	}
	if name == "" {
		return result{}, badRequest("name required")
	}
	if req.RetryPolicyID == nil {
		return result{}, badRequest("retry_policy_id required")
	}
	policyID, err := uuid.Parse(*req.RetryPolicyID)
	if err != nil {
		return result{}, badRequest("invalid retry_policy_id")
	}
	if err := s.assertPolicyOwned(ctx, tx, policyID, sc.projectID); err != nil {
		return result{}, err
	}

	q := queueView{
		ProjectID:       sc.projectID,
		Name:            name,
		RetryPolicyID:   policyID,
		MaxConcurrency:  derefInt(req.MaxConcurrency, 10),
		MaxDepth:        req.MaxDepth,
		DefaultPriority: derefInt(req.DefaultPriority, 0),
		RateLimitPerSec: derefFloat(req.RateLimitPerSec, 0),
		RateBurst:       derefFloat(req.RateBurst, 0),
		Shards:          derefInt(req.Shards, 1),
	}
	err = tx.QueryRow(ctx, insertQueueSQL, sc.projectID, name, policyID, q.MaxConcurrency,
		q.MaxDepth, q.DefaultPriority, q.RateLimitPerSec, q.RateBurst, q.Shards).
		Scan(&q.ID, &q.Paused, &q.BreakerState, &q.CreatedAt)
	if isUnique(err) {
		return result{}, conflict("queue name already used in this project")
	}
	if isCheck(err) {
		return result{}, badRequest("queue values out of range")
	}
	if err != nil {
		return result{}, err
	}
	return result{status: http.StatusCreated, body: q, entityID: q.ID}, nil
}

func (s *Server) assertPolicyOwned(ctx context.Context, tx pgx.Tx, policyID, projectID uuid.UUID) error {
	var one int
	err := tx.QueryRow(ctx, policyOwnedSQL, policyID, projectID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return badRequest("retry_policy_id does not belong to this project")
	}
	return err
}

func scanQueue(row pgx.Row, q *queueView) error {
	return row.Scan(&q.ID, &q.ProjectID, &q.Name, &q.Paused, &q.RetryPolicyID, &q.DefaultPriority,
		&q.MaxConcurrency, &q.Shards, &q.InFlight, &q.MaxDepth, &q.RateLimitPerSec, &q.RateBurst,
		&q.BreakerState, &q.CreatedAt)
}

func (s *Server) listQueues(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	rows, err := tx.Query(ctx, listQueuesSQL, sc.projectID)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()
	out := []queueView{}
	for rows.Next() {
		var q queueView
		if err := scanQueue(rows, &q); err != nil {
			return result{}, err
		}
		out = append(out, q)
	}
	if err := rows.Err(); err != nil {
		return result{}, err
	}
	return result{body: map[string]any{"items": out}}, nil
}

func (s *Server) getQueue(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var q queueView
	err := scanQueue(tx.QueryRow(ctx, getQueueSQL, sc.entityID), &q)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, notFound("queue")
	}
	if err != nil {
		return result{}, err
	}
	return result{body: q}, nil
}

func (s *Server) updateQueue(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req queueReq
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	var policyID *uuid.UUID
	if req.RetryPolicyID != nil {
		id, err := uuid.Parse(*req.RetryPolicyID)
		if err != nil {
			return result{}, badRequest("invalid retry_policy_id")
		}
		if err := s.assertPolicyOwned(ctx, tx, id, sc.projectID); err != nil {
			return result{}, err
		}
		policyID = &id
	}
	var q queueView
	err := scanQueue(tx.QueryRow(ctx, updateQueueSQL, sc.entityID, req.Name, req.MaxConcurrency,
		req.MaxDepth, req.DefaultPriority, req.RateLimitPerSec, req.RateBurst, policyID,
		req.Shards), &q)
	if isUnique(err) {
		return result{}, conflict("queue name already used in this project")
	}
	if isInUse(err) {
		return result{}, conflict("shards cannot change while the queue holds running jobs")
	}
	if isCheck(err) {
		return result{}, badRequest("queue values out of range")
	}
	if err != nil {
		return result{}, err
	}
	return result{body: q}, nil
}

func (s *Server) deleteQueue(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	if _, err := tx.Exec(ctx, deleteQueueSQL, sc.entityID); err != nil {
		return result{}, err
	}
	return result{status: http.StatusNoContent}, nil
}

func (s *Server) pauseQueue(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	return s.setPaused(ctx, tx, sc, true, "queue.paused")
}

func (s *Server) resumeQueue(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	return s.setPaused(ctx, tx, sc, false, "queue.resumed")
}

func (s *Server) setPaused(ctx context.Context, tx pgx.Tx, sc scope, paused bool, topic string) (result, error) {
	var got bool
	if err := tx.QueryRow(ctx, setPausedSQL, sc.entityID, paused).Scan(&got); err != nil {
		return result{}, err
	}
	if err := emit(ctx, tx, topic, sc.entityID, sc.projectID, map[string]any{"paused": got}); err != nil {
		return result{}, err
	}
	return result{body: map[string]any{"id": sc.entityID, "paused": got}}, nil
}

func derefString(p *string, d string) string {
	if p != nil {
		return *p
	}
	return d
}

func derefFloat(p *float64, d float64) float64 {
	if p != nil {
		return *p
	}
	return d
}
