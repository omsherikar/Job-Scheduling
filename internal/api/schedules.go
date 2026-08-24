package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/shrutu0929/fenceline/internal/scheduler"
)

type scheduleView struct {
	ID           uuid.UUID       `json:"id"`
	QueueID      uuid.UUID       `json:"queue_id"`
	Name         string          `json:"name"`
	CronExpr     string          `json:"cron_expr"`
	Timezone     string          `json:"timezone"`
	JobType      string          `json:"job_type"`
	Payload      json.RawMessage `json:"payload"`
	Enabled      bool            `json:"enabled"`
	Overlap      string          `json:"overlap_policy"`
	Catchup      string          `json:"catchup_policy"`
	NextRunAt    time.Time       `json:"next_run_at"`
	LastFiredFor *time.Time      `json:"last_fired_for"`
	CreatedAt    time.Time       `json:"created_at"`
}

type scheduleReq struct {
	Name     *string         `json:"name"`
	CronExpr *string         `json:"cron_expr"`
	Timezone *string         `json:"timezone"`
	JobType  *string         `json:"job_type"`
	Payload  json.RawMessage `json:"payload"`
	Overlap  *string         `json:"overlap_policy"`
	Catchup  *string         `json:"catchup_policy"`
	Enabled  *bool           `json:"enabled"`
}

const insertScheduleSQL = `
insert into schedules
  (queue_id, name, cron_expr, timezone, job_type, payload, overlap_policy, catchup_policy, next_run_at)
values ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
returning id, enabled, last_fired_for, created_at`

const scheduleSelect = `
select id, queue_id, name, cron_expr, timezone, job_type, payload, enabled,
       overlap_policy, catchup_policy, next_run_at, last_fired_for, created_at
  from schedules`

const getScheduleSQL = scheduleSelect + ` where id = $1`

const listSchedulesSQL = scheduleSelect + ` where queue_id = $1 order by name`

const updateScheduleSQL = `
update schedules set
  name = coalesce($2, name),
  cron_expr = coalesce($3, cron_expr),
  timezone = coalesce($4, timezone),
  job_type = coalesce($5, job_type),
  payload = coalesce($6::jsonb, payload),
  overlap_policy = coalesce($7, overlap_policy),
  catchup_policy = coalesce($8, catchup_policy),
  enabled = coalesce($9, enabled),
  next_run_at = coalesce($10, next_run_at)
where id = $1
returning id, queue_id, name, cron_expr, timezone, job_type, payload, enabled,
          overlap_policy, catchup_policy, next_run_at, last_fired_for, created_at`

const deleteScheduleSQL = `delete from schedules where id = $1`

const setScheduleEnabledSQL = `update schedules set enabled = $2 where id = $1 returning enabled`

var overlapPolicies = map[string]bool{"allow": true, "skip": true}

var catchupPolicies = map[string]bool{"skip": true, "fire_once": true}

func scanSchedule(row pgx.Row, s *scheduleView) error {
	return row.Scan(&s.ID, &s.QueueID, &s.Name, &s.CronExpr, &s.Timezone, &s.JobType, &s.Payload,
		&s.Enabled, &s.Overlap, &s.Catchup, &s.NextRunAt, &s.LastFiredFor, &s.CreatedAt)
}

func checkCron(ctx context.Context, tx pgx.Tx, expr, tz string) (time.Time, error) {
	if _, err := time.LoadLocation(tz); err != nil {
		return time.Time{}, badRequest("unknown timezone")
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "select fl.now()").Scan(&now); err != nil {
		return time.Time{}, err
	}
	next, err := scheduler.NextTick(expr, tz, now)
	if err != nil {
		return time.Time{}, badRequest("cron_expr is not a valid five field expression")
	}
	return next, nil
}

func (s *Server) createSchedule(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req scheduleReq
	if err := decode(r, &req); err != nil {
		return result{}, err
	}

	v := scheduleView{QueueID: sc.entityID, Timezone: "UTC", Overlap: "skip", Catchup: "skip"}
	v.Name = strings.TrimSpace(derefString(req.Name, ""))
	v.CronExpr = strings.TrimSpace(derefString(req.CronExpr, ""))
	v.JobType = strings.TrimSpace(derefString(req.JobType, ""))
	if v.Name == "" || v.CronExpr == "" || v.JobType == "" {
		return result{}, badRequest("name, cron_expr and job_type required")
	}
	if req.Timezone != nil {
		v.Timezone = strings.TrimSpace(*req.Timezone)
	}
	if req.Overlap != nil {
		v.Overlap = *req.Overlap
	}
	if req.Catchup != nil {
		v.Catchup = *req.Catchup
	}
	if !overlapPolicies[v.Overlap] {
		return result{}, badRequest("overlap_policy must be allow or skip")
	}
	if !catchupPolicies[v.Catchup] {
		return result{}, badRequest("catchup_policy must be skip or fire_once")
	}

	v.Payload = json.RawMessage("{}")
	if len(req.Payload) > 0 {
		if !json.Valid(req.Payload) {
			return result{}, badRequest("payload is not valid json")
		}
		v.Payload = req.Payload
	}

	next, err := checkCron(ctx, tx, v.CronExpr, v.Timezone)
	if err != nil {
		return result{}, err
	}
	v.NextRunAt = next

	err = tx.QueryRow(ctx, insertScheduleSQL, v.QueueID, v.Name, v.CronExpr, v.Timezone,
		v.JobType, string(v.Payload), v.Overlap, v.Catchup, v.NextRunAt).
		Scan(&v.ID, &v.Enabled, &v.LastFiredFor, &v.CreatedAt)
	if isUnique(err) {
		return result{}, conflict("schedule name already used in this queue")
	}
	if isFKViolation(err) {
		return result{}, badRequest("unknown timezone")
	}
	if err != nil {
		return result{}, err
	}
	return result{status: http.StatusCreated, body: v, entityID: v.ID}, nil
}

func (s *Server) listSchedules(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	rows, err := tx.Query(ctx, listSchedulesSQL, sc.entityID)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()
	out := []scheduleView{}
	for rows.Next() {
		var v scheduleView
		if err := scanSchedule(rows, &v); err != nil {
			return result{}, err
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return result{}, err
	}
	return result{body: map[string]any{"items": out}}, nil
}

func (s *Server) getSchedule(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var v scheduleView
	err := scanSchedule(tx.QueryRow(ctx, getScheduleSQL, sc.entityID), &v)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, notFound("schedule")
	}
	if err != nil {
		return result{}, err
	}
	return result{body: v}, nil
}

func (s *Server) updateSchedule(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req scheduleReq
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	var cur scheduleView
	if err := scanSchedule(tx.QueryRow(ctx, getScheduleSQL, sc.entityID), &cur); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return result{}, notFound("schedule")
		}
		return result{}, err
	}

	if req.Overlap != nil && !overlapPolicies[*req.Overlap] {
		return result{}, badRequest("overlap_policy must be allow or skip")
	}
	if req.Catchup != nil && !catchupPolicies[*req.Catchup] {
		return result{}, badRequest("catchup_policy must be skip or fire_once")
	}
	if len(req.Payload) > 0 && !json.Valid(req.Payload) {
		return result{}, badRequest("payload is not valid json")
	}

	var next *time.Time
	if req.CronExpr != nil || req.Timezone != nil {
		expr := derefString(req.CronExpr, cur.CronExpr)
		tz := derefString(req.Timezone, cur.Timezone)
		at, err := checkCron(ctx, tx, strings.TrimSpace(expr), strings.TrimSpace(tz))
		if err != nil {
			return result{}, err
		}
		next = &at
	}

	var payload *string
	if len(req.Payload) > 0 {
		p := string(req.Payload)
		payload = &p
	}

	var v scheduleView
	err := scanSchedule(tx.QueryRow(ctx, updateScheduleSQL, sc.entityID, req.Name, req.CronExpr,
		req.Timezone, req.JobType, payload, req.Overlap, req.Catchup, req.Enabled, next), &v)
	if isUnique(err) {
		return result{}, conflict("schedule name already used in this queue")
	}
	if isFKViolation(err) {
		return result{}, badRequest("unknown timezone")
	}
	if err != nil {
		return result{}, err
	}
	return result{body: v}, nil
}

func (s *Server) deleteSchedule(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	if _, err := tx.Exec(ctx, deleteScheduleSQL, sc.entityID); err != nil {
		return result{}, err
	}
	return result{status: http.StatusNoContent}, nil
}

func (s *Server) pauseSchedule(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	return s.setScheduleEnabled(ctx, tx, sc, false)
}

func (s *Server) resumeSchedule(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	return s.setScheduleEnabled(ctx, tx, sc, true)
}

func (s *Server) setScheduleEnabled(ctx context.Context, tx pgx.Tx, sc scope, enabled bool) (result, error) {
	var got bool
	if err := tx.QueryRow(ctx, setScheduleEnabledSQL, sc.entityID, enabled).Scan(&got); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return result{}, notFound("schedule")
		}
		return result{}, err
	}
	return result{body: map[string]any{"id": sc.entityID, "enabled": got}}, nil
}
