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

const sessionTTL = 30 * 24 * time.Hour

type registerReq struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Name     string `json:"name"`
}

type loginReq struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

const insertUserSQL = `
insert into users (email, password_hash, name) values ($1, $2, $3) returning id, created_at`

const findUserSQL = `select id, password_hash from users where email = $1`

const insertSessionSQL = `
insert into sessions (id, user_id, expires_at)
values ($1, $2, fl.now() + make_interval(secs => $3))
returning issued_at, expires_at`

const revokeSessionSQL = `update sessions set revoked_at = fl.now() where id = $1 and revoked_at is null`

func (s *Server) register(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req registerReq
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))
	if req.Email == "" || !strings.Contains(req.Email, "@") {
		return result{}, badRequest("valid email required")
	}
	if len(req.Password) < 8 {
		return result{}, badRequest("password must be at least 8 characters")
	}

	hash, err := hashPassword(req.Password)
	if err != nil {
		return result{}, err
	}

	var name *string
	if n := strings.TrimSpace(req.Name); n != "" {
		name = &n
	}

	var id uuid.UUID
	var created time.Time
	err = tx.QueryRow(ctx, insertUserSQL, req.Email, hash, name).Scan(&id, &created)
	if isUnique(err) {
		return result{}, conflict("email already registered")
	}
	if err != nil {
		return result{}, err
	}

	return result{
		status: http.StatusCreated,
		body: map[string]any{
			"id":         id,
			"email":      req.Email,
			"name":       req.Name,
			"created_at": created,
		},
	}, nil
}

func (s *Server) login(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req loginReq
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))

	var uid uuid.UUID
	var hash string
	err := tx.QueryRow(ctx, findUserSQL, req.Email).Scan(&uid, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, unauthorized()
	}
	if err != nil {
		return result{}, err
	}

	ok, err := verifyPassword(req.Password, hash)
	if err != nil {
		return result{}, err
	}
	if !ok {
		return result{}, unauthorized()
	}

	sid, err := uuid.NewRandom()
	if err != nil {
		return result{}, err
	}
	var issued, expires time.Time
	err = tx.QueryRow(ctx, insertSessionSQL, sid, uid, sessionTTL.Seconds()).Scan(&issued, &expires)
	if err != nil {
		return result{}, err
	}

	return result{
		status: http.StatusOK,
		body: map[string]any{
			"token":      sid.String(),
			"user_id":    uid,
			"issued_at":  issued,
			"expires_at": expires,
		},
	}, nil
}

func (s *Server) logout(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	if _, err := tx.Exec(ctx, revokeSessionSQL, sc.sessionID); err != nil {
		return result{}, err
	}
	return result{status: http.StatusNoContent}, nil
}
