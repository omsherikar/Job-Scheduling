package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type policyView struct {
	ID              uuid.UUID `json:"id"`
	ProjectID       uuid.UUID `json:"project_id"`
	Name            string    `json:"name"`
	Kind            string    `json:"kind"`
	MaxAttempts     int       `json:"max_attempts"`
	MaxLostAttempts int       `json:"max_lost_attempts"`
	BaseDelayMs     int       `json:"base_delay_ms"`
	MaxDelayMs      int       `json:"max_delay_ms"`
	Jitter          bool      `json:"jitter"`
}

type policyReq struct {
	Name            *string `json:"name"`
	Kind            *string `json:"kind"`
	MaxAttempts     *int    `json:"max_attempts"`
	MaxLostAttempts *int    `json:"max_lost_attempts"`
	BaseDelayMs     *int    `json:"base_delay_ms"`
	MaxDelayMs      *int    `json:"max_delay_ms"`
	Jitter          *bool   `json:"jitter"`
}

const insertPolicySQL = `
insert into retry_policies
  (project_id, name, kind, max_attempts, max_lost_attempts, base_delay_ms, max_delay_ms, jitter)
values ($1, $2, $3::backoff_kind, $4, $5, $6, $7, $8)
returning id, kind, max_attempts, max_lost_attempts, base_delay_ms, max_delay_ms, jitter`

const listPoliciesSQL = `
select id, project_id, name, kind, max_attempts, max_lost_attempts, base_delay_ms, max_delay_ms, jitter
from retry_policies where project_id = $1 order by name`

const getPolicySQL = `
select id, project_id, name, kind, max_attempts, max_lost_attempts, base_delay_ms, max_delay_ms, jitter
from retry_policies where id = $1`

const updatePolicySQL = `
update retry_policies set
  name = coalesce($2, name),
  kind = coalesce($3::backoff_kind, kind),
  max_attempts = coalesce($4, max_attempts),
  max_lost_attempts = coalesce($5, max_lost_attempts),
  base_delay_ms = coalesce($6, base_delay_ms),
  max_delay_ms = coalesce($7, max_delay_ms),
  jitter = coalesce($8, jitter)
where id = $1
returning project_id, name, kind, max_attempts, max_lost_attempts, base_delay_ms, max_delay_ms, jitter`

const deletePolicySQL = `delete from retry_policies where id = $1`

func validKind(s string) bool {
	switch s {
	case "fixed", "linear", "exponential":
		return true
	}
	return false
}

func (s *Server) createPolicy(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req policyReq
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
	kind := "exponential"
	if req.Kind != nil {
		kind = *req.Kind
	}
	if !validKind(kind) {
		return result{}, badRequest("kind must be fixed, linear, or exponential")
	}
	p := policyView{
		ProjectID:       sc.projectID,
		Name:            name,
		Kind:            kind,
		MaxAttempts:     derefInt(req.MaxAttempts, 5),
		MaxLostAttempts: derefInt(req.MaxLostAttempts, 3),
		BaseDelayMs:     derefInt(req.BaseDelayMs, 1000),
		MaxDelayMs:      derefInt(req.MaxDelayMs, 3600000),
		Jitter:          derefBool(req.Jitter, true),
	}

	err := tx.QueryRow(ctx, insertPolicySQL, sc.projectID, name, kind,
		p.MaxAttempts, p.MaxLostAttempts, p.BaseDelayMs, p.MaxDelayMs, p.Jitter).
		Scan(&p.ID, &p.Kind, &p.MaxAttempts, &p.MaxLostAttempts, &p.BaseDelayMs, &p.MaxDelayMs, &p.Jitter)
	if isUnique(err) {
		return result{}, conflict("policy name already used in this project")
	}
	if isCheck(err) {
		return result{}, badRequest("policy values out of range")
	}
	if err != nil {
		return result{}, err
	}
	return result{status: http.StatusCreated, body: p, entityID: p.ID}, nil
}

func (s *Server) listPolicies(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	rows, err := tx.Query(ctx, listPoliciesSQL, sc.projectID)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()
	out := []policyView{}
	for rows.Next() {
		var p policyView
		if err := rows.Scan(&p.ID, &p.ProjectID, &p.Name, &p.Kind, &p.MaxAttempts,
			&p.MaxLostAttempts, &p.BaseDelayMs, &p.MaxDelayMs, &p.Jitter); err != nil {
			return result{}, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return result{}, err
	}
	return result{body: map[string]any{"items": out}}, nil
}

func (s *Server) getPolicy(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var p policyView
	err := tx.QueryRow(ctx, getPolicySQL, sc.entityID).Scan(&p.ID, &p.ProjectID, &p.Name, &p.Kind,
		&p.MaxAttempts, &p.MaxLostAttempts, &p.BaseDelayMs, &p.MaxDelayMs, &p.Jitter)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, notFound("retry_policy")
	}
	if err != nil {
		return result{}, err
	}
	return result{body: p}, nil
}

func (s *Server) updatePolicy(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req policyReq
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	if req.Kind != nil && !validKind(*req.Kind) {
		return result{}, badRequest("kind must be fixed, linear, or exponential")
	}
	var p policyView
	p.ID = sc.entityID
	err := tx.QueryRow(ctx, updatePolicySQL, sc.entityID, req.Name, req.Kind, req.MaxAttempts,
		req.MaxLostAttempts, req.BaseDelayMs, req.MaxDelayMs, req.Jitter).
		Scan(&p.ProjectID, &p.Name, &p.Kind, &p.MaxAttempts, &p.MaxLostAttempts, &p.BaseDelayMs, &p.MaxDelayMs, &p.Jitter)
	if isUnique(err) {
		return result{}, conflict("policy name already used in this project")
	}
	if isCheck(err) {
		return result{}, badRequest("policy values out of range")
	}
	if err != nil {
		return result{}, err
	}
	return result{body: p}, nil
}

func (s *Server) deletePolicy(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	if _, err := tx.Exec(ctx, deletePolicySQL, sc.entityID); err != nil {
		if isFKViolation(err) {
			return result{}, conflict("policy still referenced by a queue or job")
		}
		return result{}, err
	}
	return result{status: http.StatusNoContent}, nil
}

func derefInt(p *int, d int) int {
	if p != nil {
		return *p
	}
	return d
}

func derefBool(p *bool, d bool) bool {
	if p != nil {
		return *p
	}
	return d
}
