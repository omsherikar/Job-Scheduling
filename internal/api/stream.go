package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	streamsPerUser = 4
	streamBatch    = 1000
	streamPoll     = 2 * time.Second
	streamWrite    = 10 * time.Second
)

type frame struct {
	Type   string  `json:"type"`
	PrevID int64   `json:"prev_id"`
	Next   int64   `json:"next,omitempty"`
	More   bool    `json:"more,omitempty"`
	Oldest int64   `json:"oldest_available,omitempty"`
	Events []event `json:"events,omitempty"`
}

func subprotocols(r *http.Request) []string {
	var out []string
	for _, h := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, p := range strings.Split(h, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

func (s *Server) subscribe(uid uuid.UUID) (chan struct{}, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.streams == nil {
		s.streams = map[uuid.UUID]int{}
		s.wakers = map[chan struct{}]struct{}{}
	}
	if s.streams[uid] >= streamsPerUser {
		return nil, nil, tooMany("too many event streams open", 30)
	}
	s.streams[uid]++

	ch := make(chan struct{}, 1)
	s.wakers[ch] = struct{}{}
	if len(s.wakers) == 1 {
		ctx, cancel := context.WithCancel(context.Background())
		s.stopListen = cancel
		go s.listen(ctx)
	}

	return ch, func() { s.release(uid, ch) }, nil
}

func (s *Server) release(uid uuid.UUID, ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.wakers, ch)
	s.streams[uid]--
	if s.streams[uid] == 0 {
		delete(s.streams, uid)
	}
	if len(s.wakers) == 0 && s.stopListen != nil {
		s.stopListen()
		s.stopListen = nil
	}
}

func (s *Server) wake() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.wakers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (s *Server) listen(ctx context.Context) {
	for ctx.Err() == nil {
		conn, err := s.Pool.Acquire(ctx)
		if err != nil {
			return
		}
		_, err = conn.Exec(ctx, "listen fl_events")
		for err == nil {
			_, err = conn.Conn().WaitForNotification(ctx)
			if err == nil {
				s.wake()
			}
		}
		conn.Release()
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request) {
	rid := requestID(r.Context())

	var sc scope
	var uid uuid.UUID
	_, err := s.tx(r.Context(), func(tx pgx.Tx) (result, error) {
		id, _, err := s.authuser(r.Context(), tx, r)
		if err != nil {
			return result{}, err
		}
		found := false
		sc, found, err = byPath("projectId", s.projectScope)(r.Context(), tx, r, id)
		if err != nil {
			return result{}, err
		}
		if !found {
			return result{}, notFound("project")
		}
		if sc.role < viewer {
			return result{}, forbidden()
		}
		uid = id
		return result{}, nil
	})
	if err != nil {
		s.writeError(w, rid, err)
		return
	}

	after, err := afterID(r)
	if err != nil {
		s.writeError(w, rid, err)
		return
	}

	waker, done, err := s.subscribe(uid)
	if err != nil {
		s.writeError(w, rid, err)
		return
	}
	defer done()

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:   subprotocols(r),
		OriginPatterns: s.AllowedOrigins,
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()

	ctx := conn.CloseRead(r.Context())
	if err := s.pump(ctx, conn, sc.projectID, after, waker); err != nil {
		return
	}
	conn.Close(websocket.StatusNormalClosure, "")
}

func (s *Server) pump(ctx context.Context, conn *websocket.Conn, projectID uuid.UUID, after int64, waker chan struct{}) error {
	var low int64
	if err := s.Pool.QueryRow(ctx, lowWaterSQL).Scan(&low); err != nil {
		return err
	}
	if after+1 < low {
		return send(ctx, conn, frame{Type: "cursor_too_old", PrevID: after, Oldest: low})
	}

	for {
		rows, err := s.Pool.Query(ctx, listEventsSQL, projectID, after, streamBatch)
		if err != nil {
			return err
		}
		events, err := scanEvents(rows)
		if err != nil {
			return err
		}

		if len(events) > 0 {
			next := events[len(events)-1].ID
			f := frame{
				Type:   "batch",
				PrevID: after,
				Next:   next,
				More:   len(events) == streamBatch,
				Events: events,
			}
			if err := send(ctx, conn, f); err != nil {
				return err
			}
			after = next
			if f.More {
				continue
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waker:
		case <-time.After(streamPoll):
		}
	}
}

func send(ctx context.Context, conn *websocket.Conn, f frame) error {
	ctx, cancel := context.WithTimeout(ctx, streamWrite)
	defer cancel()
	return wsjson.Write(ctx, conn, f)
}
