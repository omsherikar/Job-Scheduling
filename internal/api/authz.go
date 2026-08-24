package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type role int

const (
	viewer role = iota
	member
	admin
	owner
)

func parseRole(s string) role {
	switch s {
	case "owner":
		return owner
	case "admin":
		return admin
	case "member":
		return member
	default:
		return viewer
	}
}

type scope struct {
	userID    uuid.UUID
	sessionID uuid.UUID
	orgID     uuid.UUID
	projectID uuid.UUID
	entityID  uuid.UUID
	role      role
}

type locator func(ctx context.Context, tx pgx.Tx, r *http.Request, uid uuid.UUID) (scope, bool, error)

type scoped func(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error)

type entityScope func(ctx context.Context, tx pgx.Tx, uid, id uuid.UUID) (scope, bool, error)

func byPath(name string, fn entityScope) locator {
	return func(ctx context.Context, tx pgx.Tx, r *http.Request, uid uuid.UUID) (scope, bool, error) {
		id, err := uuid.Parse(r.PathValue(name))
		if err != nil {
			return scope{}, false, badRequest("invalid " + name)
		}
		return fn(ctx, tx, uid, id)
	}
}

func (s *Server) none(ctx context.Context, tx pgx.Tx, r *http.Request, uid uuid.UUID) (scope, bool, error) {
	return scope{role: owner}, true, nil
}

const sessionSQL = `select user_id from sessions where id = $1 and revoked_at is null and expires_at > fl.now()`

const tokenProtocol = "fl.token."

func bearer(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimSpace(h[len("Bearer "):])
	}
	if c, err := r.Cookie("fl_session"); err == nil {
		return c.Value
	}
	for _, p := range subprotocols(r) {
		if t, ok := strings.CutPrefix(p, tokenProtocol); ok {
			return t
		}
	}
	return ""
}

func (s *Server) authuser(ctx context.Context, tx pgx.Tx, r *http.Request) (uuid.UUID, uuid.UUID, error) {
	tok := bearer(r)
	if tok == "" {
		return uuid.Nil, uuid.Nil, unauthorized()
	}
	sid, err := uuid.Parse(tok)
	if err != nil {
		return uuid.Nil, uuid.Nil, unauthorized()
	}
	var uid uuid.UUID
	err = tx.QueryRow(ctx, sessionSQL, sid).Scan(&uid)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, uuid.Nil, unauthorized()
	}
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return uid, sid, nil
}

func (s *Server) guard(min role, action, entity string, loc locator, h scoped) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := s.tx(r.Context(), func(tx pgx.Tx) (result, error) {
			uid, sid, err := s.authuser(r.Context(), tx, r)
			if err != nil {
				return result{}, err
			}
			sc, found, err := loc(r.Context(), tx, r, uid)
			if err != nil {
				return result{}, err
			}
			if !found {
				return result{}, notFound(entity)
			}
			if sc.role < min {
				return result{}, forbidden()
			}
			sc.userID = uid
			sc.sessionID = sid

			res, err := h(r.Context(), tx, r, sc)
			if err != nil {
				return result{}, err
			}

			proj := sc.projectID
			if proj == uuid.Nil {
				proj = res.projectID
			}
			ent := res.entityID
			if ent == uuid.Nil {
				ent = sc.entityID
			}
			if action != "" && proj != uuid.Nil {
				if err := s.audit(r.Context(), tx, proj, uid, action, entity, ent, res.audit); err != nil {
					return result{}, err
				}
			}
			return res, nil
		})
		s.write(w, r, res, err)
	}
}

func (s *Server) public(h scoped) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := s.tx(r.Context(), func(tx pgx.Tx) (result, error) {
			return h(r.Context(), tx, r, scope{})
		})
		s.write(w, r, res, err)
	}
}

const auditSQL = `
insert into audit_log (project_id, actor_id, action, entity, entity_id, detail)
values ($1, $2, $3, $4, $5, $6::jsonb)`

func (s *Server) audit(ctx context.Context, tx pgx.Tx, project, actor uuid.UUID, action, entity string, entityID uuid.UUID, detail map[string]any) error {
	var eid *uuid.UUID
	if entityID != uuid.Nil {
		eid = &entityID
	}
	var d any
	if detail != nil {
		b, err := json.Marshal(detail)
		if err != nil {
			return err
		}
		d = string(b)
	}
	_, err := tx.Exec(ctx, auditSQL, project, actor, action, entity, eid, d)
	return err
}

const orgScopeSQL = `
select m.role
from organizations o
join org_members m on m.org_id = o.id and m.user_id = $2
where o.id = $1`

func (s *Server) orgScope(ctx context.Context, tx pgx.Tx, uid, id uuid.UUID) (scope, bool, error) {
	var r string
	err := tx.QueryRow(ctx, orgScopeSQL, id, uid).Scan(&r)
	if errors.Is(err, pgx.ErrNoRows) {
		return scope{}, false, nil
	}
	if err != nil {
		return scope{}, false, err
	}
	return scope{orgID: id, entityID: id, role: parseRole(r)}, true, nil
}

const projectScopeSQL = `
select p.id, p.org_id, m.role
from projects p
join org_members m on m.org_id = p.org_id and m.user_id = $2
where p.id = $1`

func scopeBy(ctx context.Context, tx pgx.Tx, query string, uid, id uuid.UUID) (scope, bool, error) {
	var proj, org uuid.UUID
	var r string
	err := tx.QueryRow(ctx, query, id, uid).Scan(&proj, &org, &r)
	if errors.Is(err, pgx.ErrNoRows) {
		return scope{}, false, nil
	}
	if err != nil {
		return scope{}, false, err
	}
	return scope{orgID: org, projectID: proj, entityID: id, role: parseRole(r)}, true, nil
}

func (s *Server) projectScope(ctx context.Context, tx pgx.Tx, uid, id uuid.UUID) (scope, bool, error) {
	return scopeBy(ctx, tx, projectScopeSQL, uid, id)
}

const queueScopeSQL = `
select q.project_id, p.org_id, m.role
from queues q
join projects p on p.id = q.project_id
join org_members m on m.org_id = p.org_id and m.user_id = $2
where q.id = $1`

func (s *Server) queueScope(ctx context.Context, tx pgx.Tx, uid, id uuid.UUID) (scope, bool, error) {
	return scopeBy(ctx, tx, queueScopeSQL, uid, id)
}

const policyScopeSQL = `
select rp.project_id, p.org_id, m.role
from retry_policies rp
join projects p on p.id = rp.project_id
join org_members m on m.org_id = p.org_id and m.user_id = $2
where rp.id = $1`

func (s *Server) policyScope(ctx context.Context, tx pgx.Tx, uid, id uuid.UUID) (scope, bool, error) {
	return scopeBy(ctx, tx, policyScopeSQL, uid, id)
}

const jobScopeSQL = `
select j.project_id, p.org_id, m.role
from jobs j
join projects p on p.id = j.project_id
join org_members m on m.org_id = p.org_id and m.user_id = $2
where j.id = $1`

const scheduleScopeSQL = `
select q.project_id, p.org_id, m.role
from schedules s
join queues q on q.id = s.queue_id
join projects p on p.id = q.project_id
join org_members m on m.org_id = p.org_id and m.user_id = $2
where s.id = $1`

func (s *Server) scheduleScope(ctx context.Context, tx pgx.Tx, uid, id uuid.UUID) (scope, bool, error) {
	return scopeBy(ctx, tx, scheduleScopeSQL, uid, id)
}

func (s *Server) jobScope(ctx context.Context, tx pgx.Tx, uid, id uuid.UUID) (scope, bool, error) {
	return scopeBy(ctx, tx, jobScopeSQL, uid, id)
}

const batchScopeSQL = `
select j.project_id, p.org_id, m.role
from jobs j
join projects p on p.id = j.project_id
join org_members m on m.org_id = p.org_id and m.user_id = $2
where j.batch_id = $1
limit 1`

func (s *Server) batchScope(ctx context.Context, tx pgx.Tx, uid, id uuid.UUID) (scope, bool, error) {
	return scopeBy(ctx, tx, batchScopeSQL, uid, id)
}

func (s *Server) byProjectOrQueue(ctx context.Context, tx pgx.Tx, r *http.Request, uid uuid.UUID) (scope, bool, error) {
	if q := r.URL.Query().Get("queue"); q != "" {
		id, err := uuid.Parse(q)
		if err != nil {
			return scope{}, false, badRequest("invalid queue")
		}
		return s.queueScope(ctx, tx, uid, id)
	}
	if p := r.URL.Query().Get("project"); p != "" {
		id, err := uuid.Parse(p)
		if err != nil {
			return scope{}, false, badRequest("invalid project")
		}
		return s.projectScope(ctx, tx, uid, id)
	}
	return scope{}, false, badRequest("project or queue query parameter required")
}

func (s *Server) byProjectQuery(ctx context.Context, tx pgx.Tx, r *http.Request, uid uuid.UUID) (scope, bool, error) {
	p := r.URL.Query().Get("project")
	if p == "" {
		return scope{}, false, badRequest("project query parameter required")
	}
	id, err := uuid.Parse(p)
	if err != nil {
		return scope{}, false, badRequest("invalid project")
	}
	return s.projectScope(ctx, tx, uid, id)
}
