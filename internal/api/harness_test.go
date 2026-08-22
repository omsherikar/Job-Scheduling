package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/api"
	"github.com/shrutu0929/fenceline/internal/testdb"
)

type rawBody []byte

type tenant struct {
	orgID     uuid.UUID
	projectID uuid.UUID
	policyID  uuid.UUID
	queueID   uuid.UUID
}

func server(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	s := httptest.NewServer((&api.Server{Pool: pool}).Handler())
	t.Cleanup(s.Close)
	return s.URL
}

func setup(t *testing.T, ctx context.Context, pool *pgxpool.Pool) tenant {
	t.Helper()
	projectID, policyID := testdb.Base(t, ctx, pool)
	var orgID uuid.UUID
	testdb.Must(t, pool.QueryRow(ctx, `select org_id from projects where id = $1`, projectID).Scan(&orgID))
	queueID := testdb.NewQueue(t, ctx, pool, projectID, policyID, 10)
	return tenant{orgID, projectID, policyID, queueID}
}

func actor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID uuid.UUID, role string) (uuid.UUID, string) {
	t.Helper()
	uid := testdb.NewUser(t, ctx, pool)
	testdb.AddMember(t, ctx, pool, orgID, uid, role)
	return uid, testdb.NewSession(t, ctx, pool, uid)
}

func do(t *testing.T, base, token, method, path string, body any, hdr map[string]string) (int, http.Header, []byte) {
	t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case nil:
	case rawBody:
		r = bytes.NewReader(b)
	default:
		buf, err := json.Marshal(body)
		testdb.Must(t, err)
		r = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, base+path, r)
	testdb.Must(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	testdb.Must(t, err)
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	testdb.Must(t, err)
	return resp.StatusCode, resp.Header, out
}

func asMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	testdb.Must(t, json.Unmarshal(raw, &m))
	return m
}

func numField(t *testing.T, m map[string]any, key string) int {
	t.Helper()
	f, ok := m[key].(float64)
	if !ok {
		t.Fatalf("%s = %v, want number", key, m[key])
	}
	return int(f)
}

func strField(t *testing.T, m map[string]any, key string) string {
	t.Helper()
	s, ok := m[key].(string)
	if !ok {
		t.Fatalf("%s = %v, want string", key, m[key])
	}
	return s
}
