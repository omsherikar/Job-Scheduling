package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const apiEventSQL = `select fl.emit($1, $2, $3, $4::jsonb)`

func emit(ctx context.Context, tx pgx.Tx, topic string, entity, project uuid.UUID, payload map[string]any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, apiEventSQL, topic, entity, project, string(b))
	return err
}

type event struct {
	ID        int64           `json:"id"`
	Topic     string          `json:"topic"`
	EntityID  uuid.UUID       `json:"entity_id"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

const lowWaterSQL = `select low_water_id from events_retention`

const listEventsSQL = `
select id, topic, entity_id, payload, created_at from events
 where project_id = $1 and id > $2
 order by id
 limit $3`

func afterID(r *http.Request) (int64, error) {
	v := r.URL.Query().Get("after")
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, badRequest("invalid after")
	}
	return n, nil
}

func scanEvents(rows pgx.Rows) ([]event, error) {
	defer rows.Close()
	out := []event{}
	for rows.Next() {
		var e event
		if err := rows.Scan(&e.ID, &e.Topic, &e.EntityID, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Server) listEvents(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	after, err := afterID(r)
	if err != nil {
		return result{}, err
	}

	var low int64
	if err := tx.QueryRow(ctx, lowWaterSQL).Scan(&low); err != nil {
		return result{}, err
	}
	if after+1 < low {
		return result{}, tooOld("events before " + strconv.FormatInt(low, 10) + " have been dropped")
	}

	limit := pageLimit(r)
	rows, err := tx.Query(ctx, listEventsSQL, sc.projectID, after, limit)
	if err != nil {
		return result{}, err
	}
	out, err := scanEvents(rows)
	if err != nil {
		return result{}, err
	}

	next := after
	if len(out) > 0 {
		next = out[len(out)-1].ID
	}
	return result{body: map[string]any{"items": out, "next": next}}, nil
}
