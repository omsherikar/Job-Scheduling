package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	maxBody      = 1 << 20
	maxTxRetries = 5
	retryBackoff = 20 * time.Millisecond
)

var errConflictRetry = errors.New("idempotency conflict retry")

type result struct {
	status    int
	body      any
	entityID  uuid.UUID
	projectID uuid.UUID
	audit     map[string]any
}

type ctxKey int

const ridKey ctxKey = 0

func requestID(ctx context.Context) string {
	if v, ok := ctx.Value(ridKey).(string); ok {
		return v
	}
	return ""
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-Id")
		if rid == "" {
			rid = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", rid)
		ctx := context.WithValue(r.Context(), ridKey, rid)
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("request %s panic: %v", rid, rec)
				s.writeError(w, rid, &apiError{status: 500, title: "internal error"})
			}
		}()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBody))
	if err := dec.Decode(v); err != nil {
		return badRequest("invalid json body")
	}
	return nil
}

func (s *Server) tx(ctx context.Context, fn func(pgx.Tx) (result, error)) (result, error) {
	var last error
	for i := 0; i < maxTxRetries; i++ {
		tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			return result{}, err
		}
		res, err := fn(tx)
		if err != nil {
			tx.Rollback(ctx)
			if retryable(err) {
				last = err
				backoff(ctx, i)
				continue
			}
			return result{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			tx.Rollback(ctx)
			if retryable(err) {
				last = err
				backoff(ctx, i)
				continue
			}
			return result{}, err
		}
		return res, nil
	}
	return result{}, last
}

func backoff(ctx context.Context, attempt int) {
	d := retryBackoff * time.Duration(1<<attempt)
	select {
	case <-ctx.Done():
	case <-time.After(d/2 + time.Duration(rand.Int64N(int64(d/2)+1))):
	}
}

func retryable(err error) bool {
	if errors.Is(err, errConflictRetry) {
		return true
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code == "40001" || pg.Code == "40P01"
	}
	return false
}

func isTimeout(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "57014"
}

func isUnique(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

func isCheck(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23514"
}

func isFKViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23503"
}

func (s *Server) write(w http.ResponseWriter, r *http.Request, res result, err error) {
	rid := requestID(r.Context())
	if err != nil {
		s.writeError(w, rid, err)
		return
	}
	status := res.status
	if status == 0 {
		status = http.StatusOK
	}
	if res.body == nil {
		w.WriteHeader(status)
		return
	}
	buf, merr := json.Marshal(res.body)
	if merr != nil {
		s.writeError(w, rid, merr)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(buf)
}

func (s *Server) writeError(w http.ResponseWriter, rid string, err error) {
	var ae *apiError
	if !errors.As(err, &ae) {
		log.Printf("request %s: %v", rid, err)
		if isTimeout(err) {
			ae = unavailable("the database took too long")
		} else {
			ae = &apiError{status: 500, title: "internal error"}
		}
	}
	for k, v := range ae.headers {
		w.Header().Set(k, v)
	}
	body := map[string]any{
		"type":       "about:blank",
		"title":      ae.title,
		"status":     ae.status,
		"request_id": rid,
	}
	if ae.detail != "" {
		body["detail"] = ae.detail
	}
	buf, _ := json.Marshal(body)
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(ae.status)
	w.Write(buf)
}
