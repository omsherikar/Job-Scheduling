package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/shrutu0929/fenceline/internal/jobs"
)

type submitReq struct {
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
	Priority      *int            `json:"priority"`
	RunAt         *time.Time      `json:"run_at"`
	TimeoutMs     *int            `json:"timeout_ms"`
	RetryPolicyID *string         `json:"retry_policy_id"`
}

type submitView struct {
	ID        uuid.UUID `json:"id"`
	QueueID   uuid.UUID `json:"queue_id"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	Priority  int       `json:"priority"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
}

type jobListItem struct {
	ID               uuid.UUID  `json:"id"`
	QueueID          uuid.UUID  `json:"queue_id"`
	Type             string     `json:"type"`
	Status           string     `json:"status"`
	Priority         int        `json:"priority"`
	AttemptCount     int        `json:"attempt_count"`
	ReplayGeneration int        `json:"replay_generation"`
	RunAt            time.Time  `json:"run_at"`
	CreatedAt        time.Time  `json:"created_at"`
	BatchID          *uuid.UUID `json:"batch_id"`
}

type jobFull struct {
	ID               uuid.UUID       `json:"id"`
	ProjectID        uuid.UUID       `json:"project_id"`
	QueueID          uuid.UUID       `json:"queue_id"`
	ScheduleID       *uuid.UUID      `json:"schedule_id"`
	ScheduledFor     *time.Time      `json:"scheduled_for"`
	BatchID          *uuid.UUID      `json:"batch_id"`
	Type             string          `json:"type"`
	Payload          json.RawMessage `json:"payload"`
	Status           string          `json:"status"`
	Priority         int             `json:"priority"`
	RunAt            time.Time       `json:"run_at"`
	Fence            int64           `json:"fence"`
	AttemptCount     int             `json:"attempt_count"`
	ReplayGeneration int             `json:"replay_generation"`
	SnoozeCount      int             `json:"snooze_count"`
	WorkerID         *uuid.UUID      `json:"worker_id"`
	DeadlineAt       *time.Time      `json:"deadline_at"`
	TimeoutMs        int             `json:"timeout_ms"`
	RetryPolicyID    uuid.UUID       `json:"retry_policy_id"`
	PendingDeps      int             `json:"pending_deps"`
	CreatedAt        time.Time       `json:"created_at"`
	ClaimedAt        *time.Time      `json:"claimed_at"`
	StartedAt        *time.Time      `json:"started_at"`
	FinishedAt       *time.Time      `json:"finished_at"`
}

type execView struct {
	ID               uuid.UUID  `json:"id"`
	Attempt          int        `json:"attempt"`
	ReplayGeneration int        `json:"replay_generation"`
	Fence            int64      `json:"fence"`
	WorkerID         uuid.UUID  `json:"worker_id"`
	Outcome          *string    `json:"outcome"`
	ErrorClass       *string    `json:"error_class"`
	ErrorMessage     *string    `json:"error_message"`
	StartedAt        time.Time  `json:"started_at"`
	FinishedAt       *time.Time `json:"finished_at"`
	DurationMs       *int64     `json:"duration_ms"`
	Logs             []logView  `json:"logs"`
}

type logView struct {
	ID          int64           `json:"id"`
	ExecutionID uuid.UUID       `json:"execution_id"`
	Ts          time.Time       `json:"ts"`
	Level       string          `json:"level"`
	Message     string          `json:"message"`
	Meta        json.RawMessage `json:"meta"`
}

var jobStatuses = map[string]bool{
	"scheduled": true, "queued": true, "claimed": true, "running": true,
	"completed": true, "retry_wait": true, "dead_letter": true, "cancelled": true,
}

const queueDefaultsSQL = `select retry_policy_id, default_priority, max_depth from queues where id = $1`
const queuedCountSQL = `select count(*) from jobs where queue_id = $1 and status = 'queued'`

const insertJobSQL = `
insert into jobs (queue_id, project_id, type, payload, priority, run_at, timeout_ms, retry_policy_id, status)
values ($1, $2, $3, $4::jsonb, $5, coalesce($6, fl.now()), $7, $8,
        case when coalesce($6, fl.now()) > fl.now() then 'scheduled' else 'queued' end::job_status)
returning id, status::text, run_at, created_at`

const submitViewSQL = `select id, queue_id, type, status::text, priority, run_at, created_at from jobs where id = $1`

const leaseSQL = `select progress, expires_at from job_leases where job_id = $1`

const findIdemSQL = `select job_id, fingerprint, expires_at <= fl.now() from idempotency_keys where queue_id = $1 and key = $2`
const deleteIdemSQL = `delete from idempotency_keys where queue_id = $1 and key = $2`
const insertIdemSQL = `insert into idempotency_keys (queue_id, key, job_id, fingerprint) values ($1, $2, $3, $4)`

const insertBatchSQL = `
insert into jobs (queue_id, project_id, type, payload, priority, run_at, timeout_ms, retry_policy_id, batch_id, status)
select $1, $2, t.type, t.payload::jsonb, t.priority, coalesce(t.run_at, fl.now()), t.timeout_ms, $3, $4,
       case when coalesce(t.run_at, fl.now()) > fl.now() then 'scheduled' else 'queued' end::job_status
from unnest($5::text[], $6::text[], $7::int[], $8::timestamptz[], $9::int[])
  as t(type, payload, priority, run_at, timeout_ms)
returning id`

const getJobSQL = `
select id, project_id, queue_id, schedule_id, scheduled_for, batch_id, type, payload, status::text,
       priority, run_at, fence, attempt_count, replay_generation, snooze_count,
       worker_id, deadline_at, timeout_ms, retry_policy_id, pending_deps,
       created_at, claimed_at, started_at, finished_at
from jobs where id = $1`

const jobExecsSQL = `
select id, attempt, replay_generation, fence, worker_id, outcome::text, error_class, error_message,
       started_at, finished_at, duration_ms
from job_executions where job_id = $1 order by replay_generation, attempt`

const jobLogsSQL = `
select id, execution_id, ts, level, message, meta
from job_logs where execution_id = any($1) order by ts, id`

const listJobsSQL = `
select id, queue_id, type, status::text, priority, attempt_count, replay_generation, run_at, created_at, batch_id
from jobs
where project_id = $1
  and ($2::job_status is null or status = $2::job_status)
  and ($3::text is null or type = $3)
  and ($4::uuid is null or queue_id = $4)
  and ($5::timestamptz is null or (created_at, id) < ($5, $6))
order by created_at desc, id desc
limit $7`

const cancelSQL = `
with prev as (
  select id, status as old_status, queue_id, project_id from jobs where id = $1 for update
),
upd as (
  update jobs j set status = 'cancelled', finished_at = fl.now(), worker_id = null
  from prev
  where j.id = prev.id and prev.old_status not in ('completed', 'dead_letter', 'cancelled')
  returning j.id
)
select prev.old_status::text, prev.queue_id, prev.project_id, (select count(*) from upd) > 0
from prev`

const cancelLeaseSQL = `delete from job_leases where job_id = $1`
const cancelExecSQL = `update job_executions set outcome = 'cancelled', finished_at = fl.now() where job_id = $1 and finished_at is null`

const retrySQL = `
update jobs set status = 'queued', run_at = fl.now()
where id = $1 and status in ('retry_wait', 'scheduled') and pending_deps = 0
returning status::text`

func fingerprint(typ string, payload []byte, priority, timeout int, policy string, runAt *time.Time) string {
	canon := payload
	var v any
	if json.Unmarshal(payload, &v) == nil {
		if b, err := json.Marshal(v); err == nil {
			canon = b
		}
	}
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%d\x00%d\x00%s\x00", typ, priority, timeout, policy)
	if runAt != nil {
		h.Write([]byte(runAt.UTC().Format(time.RFC3339Nano)))
	}
	h.Write([]byte{0})
	h.Write(canon)
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Server) submitJob(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req submitReq
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	req.Type = strings.TrimSpace(req.Type)
	if req.Type == "" {
		return result{}, badRequest("type required")
	}
	payload := []byte(req.Payload)
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	if !json.Valid(payload) {
		return result{}, badRequest("payload is not valid json")
	}

	var defPolicy uuid.UUID
	var defPriority int
	var maxDepth *int
	if err := tx.QueryRow(ctx, queueDefaultsSQL, sc.entityID).Scan(&defPolicy, &defPriority, &maxDepth); err != nil {
		return result{}, err
	}

	priority := derefInt(req.Priority, defPriority)
	timeout := derefInt(req.TimeoutMs, 300000)
	policy := defPolicy
	if req.RetryPolicyID != nil {
		id, err := uuid.Parse(*req.RetryPolicyID)
		if err != nil {
			return result{}, badRequest("invalid retry_policy_id")
		}
		if err := s.assertPolicyOwned(ctx, tx, id, sc.projectID); err != nil {
			return result{}, err
		}
		policy = id
	}

	if maxDepth != nil && req.RunAt == nil {
		var depth int
		if err := tx.QueryRow(ctx, queuedCountSQL, sc.entityID).Scan(&depth); err != nil {
			return result{}, err
		}
		if depth >= *maxDepth {
			return result{}, tooMany("queue depth cap reached", 1)
		}
	}

	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	fp := ""
	if key != "" {
		fp = fingerprint(req.Type, payload, priority, timeout, policy.String(), req.RunAt)
		var existing uuid.UUID
		var storedFP string
		var expired bool
		err := tx.QueryRow(ctx, findIdemSQL, sc.entityID, key).Scan(&existing, &storedFP, &expired)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return result{}, err
		}
		if err == nil {
			if !expired {
				if storedFP != fp {
					return result{}, conflict("idempotency key reused with a different payload")
				}
				v, err := loadSubmitView(ctx, tx, existing)
				if err != nil {
					return result{}, err
				}
				return result{status: http.StatusOK, body: v, entityID: existing}, nil
			}
			if _, err := tx.Exec(ctx, deleteIdemSQL, sc.entityID, key); err != nil {
				return result{}, err
			}
		}
	}

	var v submitView
	v.QueueID = sc.entityID
	v.Type = req.Type
	v.Priority = priority
	err := tx.QueryRow(ctx, insertJobSQL, sc.entityID, sc.projectID, req.Type, string(payload),
		priority, req.RunAt, timeout, policy).Scan(&v.ID, &v.Status, &v.RunAt, &v.CreatedAt)
	if err != nil {
		return result{}, err
	}

	if key != "" {
		if _, err := tx.Exec(ctx, insertIdemSQL, sc.entityID, key, v.ID, fp); err != nil {
			if isUnique(err) {
				return result{}, errConflictRetry
			}
			return result{}, err
		}
	}

	if err := emit(ctx, tx, "job.submitted", v.ID, sc.projectID, map[string]any{"status": v.Status, "type": req.Type}); err != nil {
		return result{}, err
	}
	return result{status: http.StatusCreated, body: v, entityID: v.ID}, nil
}

func loadSubmitView(ctx context.Context, tx pgx.Tx, id uuid.UUID) (submitView, error) {
	var v submitView
	err := tx.QueryRow(ctx, submitViewSQL, id).Scan(&v.ID, &v.QueueID, &v.Type, &v.Status, &v.Priority, &v.RunAt, &v.CreatedAt)
	return v, err
}

type batchReq struct {
	Jobs []submitReq `json:"jobs"`
}

func (s *Server) submitBatch(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var req batchReq
	if err := decode(r, &req); err != nil {
		return result{}, err
	}
	if len(req.Jobs) == 0 {
		return result{}, badRequest("jobs required")
	}
	if len(req.Jobs) > 1000 {
		return result{}, badRequest("batch limited to 1000 jobs")
	}

	var defPolicy uuid.UUID
	var defPriority int
	var maxDepth *int
	if err := tx.QueryRow(ctx, queueDefaultsSQL, sc.entityID).Scan(&defPolicy, &defPriority, &maxDepth); err != nil {
		return result{}, err
	}

	if maxDepth != nil {
		var depth int
		if err := tx.QueryRow(ctx, queuedCountSQL, sc.entityID).Scan(&depth); err != nil {
			return result{}, err
		}
		if depth+len(req.Jobs) > *maxDepth {
			return result{}, tooMany("queue depth cap reached", 1)
		}
	}

	types := make([]string, len(req.Jobs))
	payloads := make([]string, len(req.Jobs))
	priorities := make([]int, len(req.Jobs))
	runAts := make([]*time.Time, len(req.Jobs))
	timeouts := make([]int, len(req.Jobs))
	for i, j := range req.Jobs {
		t := strings.TrimSpace(j.Type)
		if t == "" {
			return result{}, badRequest("each job requires a type")
		}
		payload := []byte(j.Payload)
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		if !json.Valid(payload) {
			return result{}, badRequest("a job payload is not valid json")
		}
		types[i] = t
		payloads[i] = string(payload)
		priorities[i] = derefInt(j.Priority, defPriority)
		runAts[i] = j.RunAt
		timeouts[i] = derefInt(j.TimeoutMs, 300000)
	}

	batchID := uuid.New()
	rows, err := tx.Query(ctx, insertBatchSQL, sc.entityID, sc.projectID, defPolicy, batchID,
		types, payloads, priorities, runAts, timeouts)
	if err != nil {
		return result{}, err
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return result{}, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result{}, err
	}

	if err := emit(ctx, tx, "batch.submitted", batchID, sc.projectID, map[string]any{"count": len(ids)}); err != nil {
		return result{}, err
	}
	return result{
		status:   http.StatusCreated,
		body:     map[string]any{"batch_id": batchID, "count": len(ids), "job_ids": ids},
		entityID: batchID,
	}, nil
}

func (s *Server) listJobs(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	q := r.URL.Query()

	var status *string
	if v := q.Get("status"); v != "" {
		if !jobStatuses[v] {
			return result{}, badRequest("invalid status")
		}
		status = &v
	}
	var typ *string
	if v := q.Get("type"); v != "" {
		typ = &v
	}
	var queueFilter *uuid.UUID
	if v := q.Get("queue"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			return result{}, badRequest("invalid queue")
		}
		queueFilter = &id
	}

	cursorTime, cursorID, err := decodeCursor(q.Get("cursor"))
	if err != nil {
		return result{}, err
	}
	limit := pageLimit(r)

	rows, err := tx.Query(ctx, listJobsSQL, sc.projectID, status, typ, queueFilter, cursorTime, cursorID, limit)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()
	out := []jobListItem{}
	for rows.Next() {
		var it jobListItem
		if err := rows.Scan(&it.ID, &it.QueueID, &it.Type, &it.Status, &it.Priority,
			&it.AttemptCount, &it.ReplayGeneration, &it.RunAt, &it.CreatedAt, &it.BatchID); err != nil {
			return result{}, err
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return result{}, err
	}

	next := ""
	if len(out) == limit {
		last := out[len(out)-1]
		next = encodeCursor(last.CreatedAt, last.ID)
	}
	return result{body: map[string]any{"items": out, "next_cursor": next}}, nil
}

func (s *Server) getJob(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var j jobFull
	err := tx.QueryRow(ctx, getJobSQL, sc.entityID).Scan(
		&j.ID, &j.ProjectID, &j.QueueID, &j.ScheduleID, &j.ScheduledFor, &j.BatchID, &j.Type, &j.Payload, &j.Status,
		&j.Priority, &j.RunAt, &j.Fence, &j.AttemptCount, &j.ReplayGeneration, &j.SnoozeCount,
		&j.WorkerID, &j.DeadlineAt, &j.TimeoutMs, &j.RetryPolicyID, &j.PendingDeps,
		&j.CreatedAt, &j.ClaimedAt, &j.StartedAt, &j.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, notFound("job")
	}
	if err != nil {
		return result{}, err
	}

	execs, err := loadExecutions(ctx, tx, j.ID)
	if err != nil {
		return result{}, err
	}

	body := map[string]any{"job": j, "executions": execs}
	var progress json.RawMessage
	var expires time.Time
	err = tx.QueryRow(ctx, leaseSQL, j.ID).Scan(&progress, &expires)
	if err == nil {
		body["lease"] = map[string]any{"progress": progress, "expires_at": expires}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return result{}, err
	}
	return result{body: body}, nil
}

func loadExecutions(ctx context.Context, tx pgx.Tx, jobID uuid.UUID) ([]execView, error) {
	rows, err := tx.Query(ctx, jobExecsSQL, jobID)
	if err != nil {
		return nil, err
	}
	execs := []execView{}
	byID := map[uuid.UUID]int{}
	for rows.Next() {
		var e execView
		e.Logs = []logView{}
		if err := rows.Scan(&e.ID, &e.Attempt, &e.ReplayGeneration, &e.Fence, &e.WorkerID, &e.Outcome,
			&e.ErrorClass, &e.ErrorMessage, &e.StartedAt, &e.FinishedAt, &e.DurationMs); err != nil {
			rows.Close()
			return nil, err
		}
		byID[e.ID] = len(execs)
		execs = append(execs, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(execs) == 0 {
		return execs, nil
	}

	ids := make([]uuid.UUID, 0, len(execs))
	for _, e := range execs {
		ids = append(ids, e.ID)
	}
	lrows, err := tx.Query(ctx, jobLogsSQL, ids)
	if err != nil {
		return nil, err
	}
	defer lrows.Close()
	for lrows.Next() {
		var l logView
		if err := lrows.Scan(&l.ID, &l.ExecutionID, &l.Ts, &l.Level, &l.Message, &l.Meta); err != nil {
			return nil, err
		}
		if idx, ok := byID[l.ExecutionID]; ok {
			execs[idx].Logs = append(execs[idx].Logs, l)
		}
	}
	if err := lrows.Err(); err != nil {
		return nil, err
	}
	return execs, nil
}

func (s *Server) cancelJob(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var oldStatus string
	var queueID, projectID uuid.UUID
	var cancelled bool
	err := tx.QueryRow(ctx, cancelSQL, sc.entityID).Scan(&oldStatus, &queueID, &projectID, &cancelled)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, notFound("job")
	}
	if err != nil {
		return result{}, err
	}
	if !cancelled {
		return result{}, conflict("job is already in a terminal state: " + oldStatus)
	}

	if oldStatus == "claimed" || oldStatus == "running" {
		if _, err := tx.Exec(ctx, cancelExecSQL, sc.entityID); err != nil {
			return result{}, err
		}
		if _, err := tx.Exec(ctx, cancelLeaseSQL, sc.entityID); err != nil {
			return result{}, err
		}
		if err := jobs.Release(ctx, tx, queueID, 1); err != nil {
			return result{}, err
		}
	}

	if err := emit(ctx, tx, "job.cancelled", sc.entityID, projectID, map[string]any{"from": oldStatus}); err != nil {
		return result{}, err
	}
	return result{
		body:  map[string]any{"id": sc.entityID, "status": "cancelled", "from": oldStatus},
		audit: map[string]any{"from": oldStatus},
	}, nil
}

func (s *Server) retryJob(ctx context.Context, tx pgx.Tx, r *http.Request, sc scope) (result, error) {
	var status string
	err := tx.QueryRow(ctx, retrySQL, sc.entityID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return result{}, conflict("job is not waiting to retry")
	}
	if err != nil {
		return result{}, err
	}
	if err := emit(ctx, tx, "job.retried", sc.entityID, sc.projectID, map[string]any{"status": status}); err != nil {
		return result{}, err
	}
	return result{body: map[string]any{"id": sc.entityID, "status": status}}, nil
}
