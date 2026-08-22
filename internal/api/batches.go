package api

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5"
)

const batchCountsSQL = `select status::text, count(*) from jobs where batch_id = $1 group by status`

func (s *Server) getBatch(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	rows, err := tx.Query(ctx, batchCountsSQL, sc.entityID)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()
	counts := map[string]int{}
	total := 0
	done := 0
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return result{}, err
		}
		counts[st] = n
		total += n
		if st == "completed" || st == "dead_letter" || st == "cancelled" {
			done += n
		}
	}
	if err := rows.Err(); err != nil {
		return result{}, err
	}
	if total == 0 {
		return result{}, notFound("batch")
	}
	return result{
		status: http.StatusOK,
		body: map[string]any{
			"batch_id":      sc.entityID,
			"total":         total,
			"done":          done,
			"complete":      done == total,
			"status_counts": counts,
		},
	}, nil
}
