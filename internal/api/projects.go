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

type projectView struct {
	ID        uuid.UUID `json:"id"`
	OrgID     uuid.UUID `json:"org_id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

const insertProjectSQL = `insert into projects (org_id, name) values ($1, $2) returning id, created_at`
const listProjectsSQL = `select id, org_id, name, created_at from projects where org_id = $1 order by created_at desc`
const getProjectSQL = `select id, org_id, name, created_at from projects where id = $1`
const updateProjectSQL = `update projects set name = $2 where id = $1 returning org_id, name, created_at`
const deleteProjectSQL = `delete from projects where id = $1`

func (s *Server) createProject(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
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
	err := tx.QueryRow(ctx, insertProjectSQL, sc.orgID, req.Name).Scan(&id, &created)
	if isUnique(err) {
		return result{}, conflict("project name already used in this org")
	}
	if err != nil {
		return result{}, err
	}
	return result{
		status:    http.StatusCreated,
		body:      projectView{ID: id, OrgID: sc.orgID, Name: req.Name, CreatedAt: created},
		entityID:  id,
		projectID: id,
	}, nil
}

func (s *Server) listProjects(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	rows, err := tx.Query(ctx, listProjectsSQL, sc.orgID)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()

	out := []projectView{}
	for rows.Next() {
		var p projectView
		if err := rows.Scan(&p.ID, &p.OrgID, &p.Name, &p.CreatedAt); err != nil {
			return result{}, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return result{}, err
	}
	return result{body: map[string]any{"items": out}}, nil
}

func (s *Server) getProject(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var p projectView
	err := tx.QueryRow(ctx, getProjectSQL, sc.projectID).Scan(&p.ID, &p.OrgID, &p.Name, &p.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, notFound("project")
	}
	if err != nil {
		return result{}, err
	}
	return result{body: p}, nil
}

func (s *Server) updateProject(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
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
	var p projectView
	p.ID = sc.projectID
	err := tx.QueryRow(ctx, updateProjectSQL, sc.projectID, req.Name).Scan(&p.OrgID, &p.Name, &p.CreatedAt)
	if isUnique(err) {
		return result{}, conflict("project name already used in this org")
	}
	if err != nil {
		return result{}, err
	}
	return result{body: p}, nil
}

func (s *Server) deleteProject(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	if _, err := tx.Exec(ctx, deleteProjectSQL, sc.projectID); err != nil {
		return result{}, err
	}
	return result{status: http.StatusNoContent}, nil
}
