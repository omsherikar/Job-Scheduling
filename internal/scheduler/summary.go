package scheduler

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shrutu0929/fenceline/internal/ai"
	"github.com/shrutu0929/fenceline/internal/jobs"
)

const staleSummariesSQL = `
select q.id, q.name, fl.failure_digest(q.id, $1)
  from queues q
 where exists (select 1 from job_executions x
                where x.queue_id = q.id
                  and x.outcome in ('retryable_error', 'permanent_error', 'timeout', 'lost')
                  and x.finished_at >= fl.now() - make_interval(hours => $1))
   and coalesce((select s.digest from failure_summaries s where s.queue_id = q.id), '')
       is distinct from fl.failure_digest(q.id, $1)
 order by q.id
 limit $2`

const saveSummarySQL = `
insert into failure_summaries (queue_id, digest, summary, model, generated_at)
values ($1, $2, $3, $4, fl.now())
on conflict (queue_id) do update set
  digest = excluded.digest, summary = excluded.summary,
  model = excluded.model, generated_at = excluded.generated_at`

const summaryGroups = 20

type stale struct {
	id     uuid.UUID
	name   string
	digest string
}

func Summarize(ctx context.Context, pool *pgxpool.Pool, limit int) (int, error) {
	if !ai.Ready() {
		return 0, nil
	}

	rows, err := pool.Query(ctx, staleSummariesSQL, jobs.FailureWindow, limit)
	if err != nil {
		return 0, err
	}
	var todo []stale
	for rows.Next() {
		var s stale
		if err := rows.Scan(&s.id, &s.name, &s.digest); err != nil {
			rows.Close()
			return 0, err
		}
		todo = append(todo, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	written := 0
	var last error
	for _, s := range todo {
		fails, err := jobs.Failures(ctx, pool, s.id, jobs.FailureWindow, summaryGroups)
		if err != nil {
			return written, err
		}
		if len(fails) == 0 {
			continue
		}
		text, err := ai.Summarize(ctx, s.name, fails)
		if err != nil {
			last = err
			continue
		}
		if _, err := pool.Exec(ctx, saveSummarySQL, s.id, s.digest, text, ai.Model); err != nil {
			return written, err
		}
		written++
	}
	return written, last
}
