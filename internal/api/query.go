package api

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type query struct {
	where []string
	args  []any
}

func (q *query) eq(col string, v any) {
	q.args = append(q.args, v)
	q.where = append(q.where, fmt.Sprintf("%s = $%d", col, len(q.args)))
}

func (q *query) before(cols string, t *time.Time, id uuid.UUID) {
	if t == nil {
		return
	}
	q.args = append(q.args, *t, id)
	q.where = append(q.where, fmt.Sprintf("%s < ($%d, $%d)", cols, len(q.args)-1, len(q.args)))
}

func (q *query) build(head, tail string, limit int) string {
	q.args = append(q.args, limit)
	return fmt.Sprintf("%s where %s %s limit $%d", head, strings.Join(q.where, " and "), tail, len(q.args))
}
