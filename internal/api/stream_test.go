package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

type streamFrame struct {
	Type   string `json:"type"`
	PrevID int64  `json:"prev_id"`
	Next   int64  `json:"next"`
	More   bool   `json:"more"`
	Oldest int64  `json:"oldest_available"`
	Events []struct {
		ID    int64  `json:"id"`
		Topic string `json:"topic"`
	} `json:"events"`
}

func dial(t *testing.T, ctx context.Context, base, token, path string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(base, "http") + path
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + token}},
	})
	testdb.Must(t, err)
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

func read(t *testing.T, ctx context.Context, conn *websocket.Conn) streamFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var raw json.RawMessage
	testdb.Must(t, wsjson.Read(ctx, conn, &raw))
	var f streamFrame
	testdb.Must(t, json.Unmarshal(raw, &f))
	return f
}

func emitN(t *testing.T, ctx context.Context, pool *pgxpool.Pool, projectID any, topic string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := pool.Exec(ctx, `select fl.emit($1, $2, $2, '{}'::jsonb)`, topic, projectID)
		testdb.Must(t, err)
	}
}

func TestStreamBacklog(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	emitN(t, ctx, pool, ten.projectID, "a", 3)

	conn := dial(t, ctx, base, token, "/projects/"+ten.projectID.String()+"/events/stream")
	f := read(t, ctx, conn)
	if f.Type != "batch" {
		t.Fatalf("type = %q, want batch", f.Type)
	}
	if len(f.Events) != 3 {
		t.Fatalf("events = %d, want 3", len(f.Events))
	}
	if f.PrevID != 0 {
		t.Errorf("prev_id = %d, want 0", f.PrevID)
	}
	if f.Next != f.Events[2].ID {
		t.Errorf("next = %d, want %d", f.Next, f.Events[2].ID)
	}
	if f.More {
		t.Errorf("more = true, want false")
	}
}

func TestStreamLive(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	emitN(t, ctx, pool, ten.projectID, "first", 1)
	conn := dial(t, ctx, base, token, "/projects/"+ten.projectID.String()+"/events/stream")
	first := read(t, ctx, conn)
	if len(first.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(first.Events))
	}

	emitN(t, ctx, pool, ten.projectID, "second", 1)
	_, err := pool.Exec(ctx, `select pg_notify('fl_events', '')`)
	testdb.Must(t, err)

	start := time.Now()
	next := read(t, ctx, conn)
	if waited := time.Since(start); waited > time.Second {
		t.Errorf("push latency = %s, want < 1s", waited)
	}
	if len(next.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(next.Events))
	}
	if next.Events[0].Topic != "second" {
		t.Errorf("topic = %q, want second", next.Events[0].Topic)
	}
	if next.PrevID != first.Next {
		t.Errorf("prev_id = %d, want %d", next.PrevID, first.Next)
	}
}

func TestStreamResume(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	emitN(t, ctx, pool, ten.projectID, "a", 3)
	var third int64
	testdb.Must(t, pool.QueryRow(ctx,
		`select max(id) from events where project_id = $1`, ten.projectID).Scan(&third))

	emitN(t, ctx, pool, ten.projectID, "b", 2)

	conn := dial(t, ctx, base, token,
		"/projects/"+ten.projectID.String()+"/events/stream?after="+strconv.FormatInt(third, 10))
	f := read(t, ctx, conn)
	if len(f.Events) != 2 {
		t.Fatalf("events = %d, want 2", len(f.Events))
	}
	if f.PrevID != third {
		t.Errorf("prev_id = %d, want %d", f.PrevID, third)
	}
	if f.Events[0].Topic != "b" {
		t.Errorf("topic = %q, want b", f.Events[0].Topic)
	}
}

func TestStreamCursorTooOld(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	emitN(t, ctx, pool, ten.projectID, "a", 1)
	_, err := pool.Exec(ctx, `update events_retention set low_water_id = 500`)
	testdb.Must(t, err)

	conn := dial(t, ctx, base, token, "/projects/"+ten.projectID.String()+"/events/stream?after=10")
	f := read(t, ctx, conn)
	if f.Type != "cursor_too_old" {
		t.Fatalf("type = %q, want cursor_too_old", f.Type)
	}
	if f.Oldest != 500 {
		t.Errorf("oldest_available = %d, want 500", f.Oldest)
	}
}

func TestStreamUnauthorized(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	base := server(t, pool)

	url := "ws" + strings.TrimPrefix(base, "http") + "/projects/" + ten.projectID.String() + "/events/stream"
	_, resp, err := websocket.Dial(ctx, url, nil)
	if err == nil {
		t.Fatal("dial without a token succeeded, want failure")
	}
	if resp == nil {
		t.Fatal("no response, want 401")
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestStreamSubprotocolToken(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	emitN(t, ctx, pool, ten.projectID, "a", 1)

	url := "ws" + strings.TrimPrefix(base, "http") + "/projects/" + ten.projectID.String() + "/events/stream"
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{"fl.token." + token},
	})
	testdb.Must(t, err)
	defer conn.CloseNow()

	if got := conn.Subprotocol(); got != "fl.token."+token {
		t.Errorf("subprotocol = %q, want the offered token", got)
	}
	f := read(t, ctx, conn)
	if len(f.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(f.Events))
	}
}

func TestStreamPerUserCap(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	testdb.SetNow(t, pool, testdb.Epoch)
	ten := setup(t, ctx, pool)
	_, token := actor(t, ctx, pool, ten.orgID, "viewer")
	base := server(t, pool)

	emitN(t, ctx, pool, ten.projectID, "a", 1)

	path := "/projects/" + ten.projectID.String() + "/events/stream"
	for i := 0; i < 4; i++ {
		conn := dial(t, ctx, base, token, path)
		read(t, ctx, conn)
	}

	url := "ws" + strings.TrimPrefix(base, "http") + path
	_, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + token}},
	})
	if err == nil {
		t.Fatal("fifth stream succeeded, want rejection")
	}
	if resp == nil {
		t.Fatal("no response, want 429")
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
}
