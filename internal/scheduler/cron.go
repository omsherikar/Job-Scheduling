package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

const dueSchedulesSQL = `
select s.id, s.queue_id, q.project_id, q.retry_policy_id, s.cron_expr, s.timezone,
       s.job_type, s.payload, s.overlap_policy, s.catchup_policy, s.next_run_at
  from schedules s join queues q on q.id = s.queue_id
 where s.enabled and s.next_run_at <= fl.now()
 order by s.next_run_at
 for update of s skip locked
 limit $1`

const overlapSQL = `
select not exists(
  select 1 from jobs
   where schedule_id = $1
     and status in ('scheduled', 'queued', 'claimed', 'running', 'retry_wait'))`

const insertScheduledJobSQL = `
insert into jobs (project_id, queue_id, schedule_id, type, payload, status, scheduled_for, run_at, retry_policy_id)
values ($1, $2, $3, $4, $5, 'queued', $6, $6, $7)
on conflict (schedule_id, scheduled_for) where schedule_id is not null do nothing
returning id`

const scheduledEventSQL = `select fl.emit('job.scheduled', $1, $2, '{}'::jsonb)`

const overlapEventSQL = `select fl.emit('schedule.tick_skipped_overlap', $1, $2, '{}'::jsonb)`

const ticksSkippedEventSQL = `
select fl.emit('schedule.ticks_skipped', $1, $2, jsonb_build_object('count', $3::int))`

const advanceScheduleSQL = `
update schedules set next_run_at = $2, last_fired_for = coalesce($3, last_fired_for) where id = $1`

const disableScheduleSQL = `update schedules set enabled = false where id = $1`

const scheduleErrorEventSQL = `select fl.emit('schedule.error', $1, $2, '{}'::jsonb)`

func NextTick(expr, tz string, after time.Time) (time.Time, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.Time{}, err
	}
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(after.In(loc)), nil
}

type due struct {
	id        uuid.UUID
	queueID   uuid.UUID
	projectID uuid.UUID
	policyID  uuid.UUID
	expr      string
	tz        string
	jobType   string
	payload   []byte
	overlap   string
	catchup   string
	nextRunAt time.Time
}

func Materialize(ctx context.Context, pool *pgxpool.Pool, limit int) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var now time.Time
	if err := tx.QueryRow(ctx, "select fl.now()").Scan(&now); err != nil {
		return 0, err
	}

	rows, err := tx.Query(ctx, dueSchedulesSQL, limit)
	if err != nil {
		return 0, err
	}
	var dues []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.id, &d.queueID, &d.projectID, &d.policyID, &d.expr, &d.tz,
			&d.jobType, &d.payload, &d.overlap, &d.catchup, &d.nextRunAt); err != nil {
			rows.Close()
			return 0, err
		}
		dues = append(dues, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	materialized := 0
sched:
	for _, d := range dues {
		last := d.nextRunAt
		count := 0
		for t := d.nextRunAt; !t.After(now); {
			last = t
			count++
			nt, err := NextTick(d.expr, d.tz, t)
			if err != nil {
				if err := disableSchedule(ctx, tx, d.id, d.projectID); err != nil {
					return 0, err
				}
				continue sched
			}
			t = nt
			if count > 100000 {
				break
			}
		}
		nextFuture, err := NextTick(d.expr, d.tz, last)
		if err != nil {
			if err := disableSchedule(ctx, tx, d.id, d.projectID); err != nil {
				return 0, err
			}
			continue sched
		}

		target := last
		skipped := 0
		fire := true
		if count > 1 {
			if d.catchup == "fire_once" {
				skipped = count - 1
			} else {
				fire = false
				skipped = count
			}
		}

		overlapSkip := false
		if fire && d.overlap == "skip" {
			var clear bool
			if err := tx.QueryRow(ctx, overlapSQL, d.id).Scan(&clear); err != nil {
				return 0, err
			}
			if !clear {
				fire = false
				overlapSkip = true
			}
		}

		var firedFor *time.Time
		if fire {
			var jobID uuid.UUID
			err := tx.QueryRow(ctx, insertScheduledJobSQL, d.projectID, d.queueID, d.id,
				d.jobType, d.payload, target, d.policyID).Scan(&jobID)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return 0, err
			}
			if err == nil {
				materialized++
				firedFor = &target
				if _, err := tx.Exec(ctx, scheduledEventSQL, jobID, d.projectID); err != nil {
					return 0, err
				}
			}
		}

		if overlapSkip {
			if _, err := tx.Exec(ctx, overlapEventSQL, d.id, d.projectID); err != nil {
				return 0, err
			}
		}
		if skipped > 0 {
			if _, err := tx.Exec(ctx, ticksSkippedEventSQL, d.id, d.projectID, skipped); err != nil {
				return 0, err
			}
		}
		if _, err := tx.Exec(ctx, advanceScheduleSQL, d.id, nextFuture, firedFor); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return materialized, nil
}

func disableSchedule(ctx context.Context, tx pgx.Tx, id, projectID uuid.UUID) error {
	if _, err := tx.Exec(ctx, disableScheduleSQL, id); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, scheduleErrorEventSQL, id, projectID)
	return err
}
