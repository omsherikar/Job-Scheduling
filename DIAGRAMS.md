# Diagrams

Generated from a migrated database by `make diagram`. Everything here is read
out of the live catalog, so it cannot drift from the schema it describes.

## The job lifecycle

Every edge is a row in `job_transitions`. A trigger rejects any move not listed
here, so this is the whole state machine and not a summary of it.

```mermaid
stateDiagram-v2
    claimed --> cancelled: api
    claimed --> dead_letter: reaper, attempts exhausted
    claimed --> queued: drain release, no attempt burned
    claimed --> retry_wait: reaper, lease expired before start
    claimed --> running: worker start
    dead_letter --> queued: replay
    queued --> cancelled: api
    queued --> claimed: worker claim
    retry_wait --> cancelled: api
    retry_wait --> dead_letter: breaker or manual
    retry_wait --> queued: promoter, backoff elapsed
    running --> cancelled: api
    running --> completed: worker success
    running --> dead_letter: permanent error or attempts exhausted
    running --> queued: drain release past deadline
    running --> retry_wait: worker failure with attempts left
    running --> scheduled: snooze
    scheduled --> cancelled: api
    scheduled --> queued: promoter, due or deps met
```

## Entities

Key columns only. The label on each line is what happens to the child when the
parent is deleted.

```mermaid
erDiagram
    users ||--o{ audit_log : "actor_id, set null"
    users ||--o{ dead_letter_jobs : "replayed_by, set null"
    queues ||--|{ failure_summaries : "queue_id, cascade"
    queues ||--|{ idempotency_keys : "queue_id, cascade"
    jobs ||--|{ job_dependencies : "child_id, cascade"
    jobs ||--|{ job_dependencies : "parent_id, cascade"
    workers ||--|{ job_executions : "worker_id, restrict"
    jobs ||--|{ job_leases : "job_id, cascade"
    workers ||--|{ job_leases : "worker_id, restrict"
    queues ||--|{ jobs : "project_id, queue_id, cascade"
    retry_policies ||--|{ jobs : "retry_policy_id, restrict"
    schedules ||--o{ jobs : "schedule_id, set null"
    workers ||--o{ jobs : "worker_id, restrict"
    organizations ||--|{ org_members : "org_id, cascade"
    users ||--|{ org_members : "user_id, cascade"
    organizations ||--|{ projects : "org_id, cascade"
    queues ||--|{ queue_shards : "queue_id, cascade"
    queues ||--|{ queue_stats_minute : "queue_id, cascade"
    projects ||--|{ queues : "project_id, cascade"
    retry_policies ||--|{ queues : "retry_policy_id, restrict"
    projects ||--|{ retry_policies : "project_id, cascade"
    queues ||--|{ schedules : "queue_id, cascade"
    tz_names ||--|{ schedules : "timezone, restrict"
    users ||--|{ sessions : "user_id, cascade"
    queues ||--|{ worker_queues : "queue_id, cascade"
    workers ||--|{ worker_queues : "worker_id, cascade"
    projects ||--|{ workers : "project_id, cascade"
    audit_log {
        int8 id PK
        uuid actor_id FK
    }
    dead_letter_jobs {
        uuid job_id PK
        uuid replayed_by FK
    }
    dead_letter_jobs_archive {
        uuid job_id PK
        timestamptz dead_at PK
    }
    events {
        int8 id PK
        timestamptz created_at PK
    }
    events_retention {
        bool only_row PK
    }
    failure_summaries {
        uuid queue_id PK, FK
    }
    idempotency_keys {
        uuid queue_id PK, FK
        text key PK
    }
    job_dependencies {
        uuid parent_id PK, FK
        uuid child_id PK, FK
    }
    job_executions {
        uuid id PK
        uuid worker_id FK
    }
    job_executions_archive {
        uuid id PK
        timestamptz finished_at PK
    }
    job_fence_violations {
        int8 id PK
    }
    job_leases {
        uuid job_id PK, FK
        uuid worker_id FK
    }
    job_logs {
        int8 id PK
        timestamptz ts PK
    }
    job_logs_archive {
        int8 id PK
        timestamptz ts PK
    }
    job_transitions {
        job_status from_status PK
        job_status to_status PK
    }
    jobs {
        uuid id PK
        uuid project_id FK
        uuid queue_id FK
        uuid schedule_id FK
        uuid worker_id FK
        uuid retry_policy_id FK
    }
    jobs_archive {
        uuid id PK
        timestamptz terminal_at PK
    }
    org_members {
        uuid org_id PK, FK
        uuid user_id PK, FK
    }
    organizations {
        uuid id PK
    }
    projects {
        uuid id PK
        uuid org_id FK
    }
    queue_shards {
        uuid queue_id PK, FK
        int2 shard PK
    }
    queue_stats_minute {
        uuid queue_id PK, FK
        timestamptz minute PK
    }
    queues {
        uuid id PK
        uuid project_id FK
        uuid retry_policy_id FK
    }
    retry_policies {
        uuid id PK
        uuid project_id FK
    }
    schedules {
        uuid id PK
        uuid queue_id FK
        text timezone FK
    }
    sessions {
        uuid id PK
        uuid user_id FK
    }
    tz_names {
        text name PK
    }
    users {
        uuid id PK
    }
    worker_queues {
        uuid worker_id PK, FK
        uuid queue_id PK, FK
    }
    workers {
        uuid id PK
        uuid project_id FK
    }
```

## Every table

Cylinders are partitioned by day.

```mermaid
graph LR
    audit_log[audit_log]
    dead_letter_jobs[dead_letter_jobs]
    dead_letter_jobs_archive[(dead_letter_jobs_archive)]
    events[(events)]
    events_retention[events_retention]
    failure_summaries[failure_summaries]
    idempotency_keys[idempotency_keys]
    job_dependencies[job_dependencies]
    job_executions[job_executions]
    job_executions_archive[(job_executions_archive)]
    job_fence_violations[job_fence_violations]
    job_leases[job_leases]
    job_logs[(job_logs)]
    job_logs_archive[(job_logs_archive)]
    job_transitions[job_transitions]
    jobs[jobs]
    jobs_archive[(jobs_archive)]
    org_members[org_members]
    organizations[organizations]
    projects[projects]
    queue_shards[queue_shards]
    queue_stats_minute[queue_stats_minute]
    queues[queues]
    retry_policies[retry_policies]
    schedules[schedules]
    sessions[sessions]
    tz_names[tz_names]
    users[users]
    worker_queues[worker_queues]
    workers[workers]
    audit_log --> users
    dead_letter_jobs --> users
    failure_summaries --> queues
    idempotency_keys --> queues
    job_dependencies --> jobs
    job_executions --> workers
    job_leases --> jobs
    job_leases --> workers
    jobs --> queues
    jobs --> retry_policies
    jobs --> schedules
    jobs --> workers
    org_members --> organizations
    org_members --> users
    projects --> organizations
    queue_shards --> queues
    queue_stats_minute --> queues
    queues --> projects
    queues --> retry_policies
    retry_policies --> projects
    schedules --> queues
    schedules --> tz_names
    sessions --> users
    worker_queues --> queues
    worker_queues --> workers
    workers --> projects
```
