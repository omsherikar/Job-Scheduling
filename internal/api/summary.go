package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/shrutu0929/fenceline/internal/ai"
	"github.com/shrutu0929/fenceline/internal/jobs"
)

type failureView struct {
	Class    string    `json:"error_class"`
	Count    int       `json:"count"`
	Variants int       `json:"distinct_messages"`
	Sample   string    `json:"latest_message"`
	First    time.Time `json:"first_seen"`
	Last     time.Time `json:"last_seen"`
}

const cachedSummarySQL = `
select s.summary, s.model, s.generated_at, s.digest = fl.failure_digest($1, $2)
from failure_summaries s where s.queue_id = $1`

const summaryGroups = 20

func (s *Server) failureSummary(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	fails, err := jobs.Failures(ctx, tx, sc.entityID, jobs.FailureWindow, summaryGroups)
	if err != nil {
		return result{}, err
	}
	out := make([]failureView, len(fails))
	for i, f := range fails {
		out[i] = failureView{
			Class:    f.Class,
			Count:    f.Count,
			Variants: f.Variants,
			Sample:   f.Sample,
			First:    f.First,
			Last:     f.Last,
		}
	}

	body := map[string]any{
		"queue_id":     sc.entityID,
		"window_hours": jobs.FailureWindow,
		"failures":     out,
		"summary":      nil,
		"state":        "unavailable",
	}
	if ai.Ready() {
		body["state"] = "pending"
	}

	var text, model string
	var at time.Time
	var fresh bool
	err = tx.QueryRow(ctx, cachedSummarySQL, sc.entityID, jobs.FailureWindow).
		Scan(&text, &model, &at, &fresh)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{body: body}, nil
	}
	if err != nil {
		return result{}, err
	}

	body["summary"] = text
	body["model"] = model
	body["generated_at"] = at
	if fresh {
		body["state"] = "current"
	} else {
		body["state"] = "stale"
	}
	return result{body: body}, nil
}
