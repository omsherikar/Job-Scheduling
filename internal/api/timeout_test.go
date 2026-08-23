package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestTimeoutStatus(t *testing.T) {
	s := &Server{}
	w := httptest.NewRecorder()
	s.writeError(w, "test", &pgconn.PgError{Code: "57014", Message: "canceling statement"})

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
	if got := w.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", got)
	}
}

func TestInternalErrorStatus(t *testing.T) {
	s := &Server{}
	w := httptest.NewRecorder()
	s.writeError(w, "test", &pgconn.PgError{Code: "42601", Message: "syntax error"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}
