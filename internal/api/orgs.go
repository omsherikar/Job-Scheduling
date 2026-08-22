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

type orgView struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Role      string    `json:"role,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

const insertOrgSQL = `insert into organizations (name) values ($1) returning id, created_at`
const insertOwnerSQL = `insert into org_members (org_id, user_id, role) values ($1, $2, 'owner')`
const listOrgsSQL = `
select o.id, o.name, o.created_at, m.role
from organizations o
join org_members m on m.org_id = o.id and m.user_id = $1
order by o.created_at desc`
const getOrgSQL = `select id, name, created_at from organizations where id = $1`
const updateOrgSQL = `update organizations set name = $2 where id = $1 returning name, created_at`
const deleteOrgSQL = `delete from organizations where id = $1`
const findUserByEmailSQL = `select id from users where email = $1`
const upsertMemberSQL = `
insert into org_members (org_id, user_id, role) values ($1, $2, $3)
on conflict (org_id, user_id) do update set role = excluded.role`

func (s *Server) createOrg(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req struct {
		Name string `json:"name"`
	}
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		return result{}, badRequest("name required")
	}

	var id uuid.UUID
	var created time.Time
	if err := tx.QueryRow(ctx, insertOrgSQL, req.Name).Scan(&id, &created); err != nil {
		return result{}, err
	}
	if _, err := tx.Exec(ctx, insertOwnerSQL, id, sc.userID); err != nil {
		return result{}, err
	}
	return result{
		status: http.StatusCreated,
		body:   orgView{ID: id, Name: req.Name, Role: "owner", CreatedAt: created},
	}, nil
}

func (s *Server) listOrgs(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	rows, err := tx.Query(ctx, listOrgsSQL, sc.userID)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()

	out := []orgView{}
	for rows.Next() {
		var o orgView
		if err := rows.Scan(&o.ID, &o.Name, &o.CreatedAt, &o.Role); err != nil {
			return result{}, err
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return result{}, err
	}
	return result{body: map[string]any{"items": out}}, nil
}

func (s *Server) getOrg(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var o orgView
	err := tx.QueryRow(ctx, getOrgSQL, sc.orgID).Scan(&o.ID, &o.Name, &o.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, notFound("org")
	}
	if err != nil {
		return result{}, err
	}
	return result{body: o}, nil
}

func (s *Server) updateOrg(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req struct {
		Name string `json:"name"`
	}
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		return result{}, badRequest("name required")
	}
	var o orgView
	o.ID = sc.orgID
	if err := tx.QueryRow(ctx, updateOrgSQL, sc.orgID, req.Name).Scan(&o.Name, &o.CreatedAt); err != nil {
		return result{}, err
	}
	return result{body: o}, nil
}

func (s *Server) deleteOrg(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	if _, err := tx.Exec(ctx, deleteOrgSQL, sc.orgID); err != nil {
		return result{}, err
	}
	return result{status: http.StatusNoContent}, nil
}

func (s *Server) addMember(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req struct {
		Email string `json:"email"`
		Role  string `json:"role"`
	}
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))
	if req.Email == "" {
		return result{}, badRequest("email required")
	}
	if !validRole(req.Role) {
		return result{}, badRequest("role must be one of owner, admin, member, viewer")
	}

	var uid uuid.UUID
	err := tx.QueryRow(ctx, findUserByEmailSQL, req.Email).Scan(&uid)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, notFound("user")
	}
	if err != nil {
		return result{}, err
	}
	if _, err := tx.Exec(ctx, upsertMemberSQL, sc.orgID, uid, req.Role); err != nil {
		return result{}, err
	}
	return result{
		status: http.StatusCreated,
		body:   map[string]any{"org_id": sc.orgID, "user_id": uid, "role": req.Role},
	}, nil
}

func validRole(s string) bool {
	switch s {
	case "owner", "admin", "member", "viewer":
		return true
	}
	return false
}
