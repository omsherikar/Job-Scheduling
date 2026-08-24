# Running it by hand

A walkthrough that takes an empty database to a job running end to end, with the
commands and the output you should see at each step. It takes about ten minutes and
needs docker, Go, `curl`, `jq`, `psql`, and Node if you want the dashboard.

Everything here is local and disposable. The credentials below are throwaway values
for a development database on your own machine.

The commands assume a POSIX shell. On macOS and Linux they were run as written. On
Windows, see [Windows](#windows) at the end before starting.

## Use `go build`, not `go run`

`go run` compiles to a temporary binary and executes that, so the running process is
named `exe/api` and not `cmd/api`. `pkill -f cmd/api` will not kill it. A stale api on
3001 pointed at an old database is a nasty failure to debug: every request succeeds and
goes to the wrong place.

Build the binaries once and run those.

## 1. Database

```
make db-up
make migrate
```

`make db-up` starts Postgres on 5433 and waits for it to accept connections.
`make migrate` applies all thirty migrations and prints `applied 30`.

If `make db-up` fails with `the container name "/fenceline-pg" is already in use`,
there is a container of that name that compose did not create, so it cannot manage it.
Check what it is before removing it:

```
docker inspect fenceline-pg --format '{{index .Config.Labels "com.docker.compose.project"}}'
docker inspect fenceline-pg --format '{{range .Mounts}}{{.Name}}{{end}}'
```

An empty project label means it was started by hand rather than by this compose file,
and a random hex volume name means its data is in an anonymous volume that compose
will not reuse. If you do not need what is in it, `docker rm -f fenceline-pg` and run
`make db-up` again.

## 2. Build and start the services

```
go build -o /tmp/fl-api ./cmd/api
go build -o /tmp/fl-scheduler ./cmd/scheduler
go build -o /tmp/fl-worker ./cmd/worker

export DATABASE_URL="postgres://fenceline:fenceline@localhost:5433/fenceline"
/tmp/fl-api       > /tmp/api.log   2>&1 &
/tmp/fl-scheduler > /tmp/sched.log 2>&1 &
```

Check the api came up:

```
cat /tmp/api.log
```

```
api listening on :3001
```

If it says `address already in use`, something is already on 3001. Find it with
`lsof -i :3001` and kill it before going on.

The worker is not started yet. It takes a queue id at startup and the queue does not
exist until step 3.

## 3. Create a user, an organization, a project, a policy and a queue

```
A=http://localhost:3001
EMAIL=demo@example.com
PASS=demo-password

curl -s -X POST $A/auth/register -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}" | jq -r .email

TOKEN=$(curl -s -X POST $A/auth/login -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}" | jq -r .token)

ORG=$(curl -s -X POST $A/orgs \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"demo org"}' | jq -r .id)

PROJ=$(curl -s -X POST $A/orgs/$ORG/projects \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"demo project"}' | jq -r .id)

POL=$(curl -s -X POST $A/projects/$PROJ/retry-policies \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"standard","kind":"exponential","max_attempts":3,
       "base_delay_ms":500,"max_delay_ms":5000}' | jq -r .id)

QUEUE=$(curl -s -X POST $A/projects/$PROJ/queues \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"name\":\"demo\",\"retry_policy_id\":\"$POL\",\"max_concurrency\":4}" | jq -r .id)

echo "project $PROJ"
echo "queue   $QUEUE"
```

The password has to be at least eight characters. Registering the same email twice
returns a `409`; if you are starting over, either pick a new email or drop and
recreate the database.

Keep this shell open. `TOKEN`, `PROJ` and `QUEUE` are used by every step below.

## 4. Start the worker

```
WORKER_QUEUE=$QUEUE WORKER_CONCURRENCY=4 /tmp/fl-worker > /tmp/worker.log 2>&1 &

curl -s "$A/workers?project=$PROJ" -H "Authorization: Bearer $TOKEN" \
  | jq '.items[0] | {state, max_concurrency, hostname}'
```

```json
{
  "state": "active",
  "max_concurrency": 4,
  "hostname": "your-machine"
}
```

`queue not found` in `/tmp/worker.log` means `WORKER_QUEUE` is not a queue id in the
database the worker is pointed at. Check `echo $QUEUE` and `echo $DATABASE_URL`.

## 5. Submit some jobs

`cmd/worker` carries three handlers. `noop` returns immediately, `sleep` waits for the
milliseconds in its payload, and `fail` always errors.

```
for t in noop sleep fail; do
  curl -s -X POST $A/queues/$QUEUE/jobs \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"type\":\"$t\",\"payload\":{\"ms\":500}}" | jq -r '"\(.type) \(.id) \(.status)"'
done
```

```
noop  01a0337f-5ca1-7639-93b5-e16734d40dd9 queued
sleep 01a0337f-5cf8-7821-b101-54a3a9da69bb queued
fail  01a0337f-5d24-7cb0-b50a-dc212507121c queued
```

## 6. Watch them finish

Wait a few seconds, then:

```
psql "$DATABASE_URL" -c "select status, count(*) from jobs group by status order by status"
```

```
   status    | count
-------------+-------
 completed   |     2
 dead_letter |     1
```

The ledger keeps every attempt:

```
psql "$DATABASE_URL" -c \
  "select j.type, x.attempt, x.outcome from job_executions x
     join jobs j on j.id = x.job_id order by j.type, x.attempt"
```

```
 type  | attempt |     outcome
-------+---------+-----------------
 fail  |       1 | retryable_error
 fail  |       2 | retryable_error
 fail  |       3 | retryable_error
 noop  |       1 | success
 sleep |       1 | success
```

`fail` burned three attempts and then dead lettered, which is `max_attempts` from the
policy in step 3. Nothing is overwritten on retry, so attempt 1 is still there after
attempt 3.

## 7. Read it back over the api

The dead letter queue, with the error that put it there:

```
curl -s "$A/dlq?project=$PROJ" -H "Authorization: Bearer $TOKEN" \
  | jq '.items[] | {reason, last_error_message}'
```

```json
{
  "reason": "attempts_exhausted",
  "last_error_message": "deliberate failure"
}
```

Failures grouped by class:

```
curl -s "$A/queues/$QUEUE/failure-summary" -H "Authorization: Bearer $TOKEN" \
  | jq '{state, summary, failures}'
```

```json
{
  "state": "unavailable",
  "summary": null,
  "failures": [
    {"error_class": "retryable_error", "count": 3, "latest_message": "deliberate failure"}
  ]
}
```

`state: "unavailable"` means the scheduler has no `ANTHROPIC_API_KEY`, so the grouping
is there and the written summary is not. Set the key and restart the scheduler if you
want the prose.

Queue health, which is what the dashboard's landing page reads:

```
curl -s "$A/projects/$PROJ/queue-health" -H "Authorization: Bearer $TOKEN" \
  | jq '.items[0] | {name: .queue.name, in_flight: .queue.in_flight,
                     cap: .queue.max_concurrency, live_workers, last_hour}'
```

```json
{
  "name": "demo",
  "in_flight": 0,
  "cap": 4,
  "live_workers": 1,
  "last_hour": {"completed": 2, "failed": 2, "dead_lettered": 1}
}
```

`failed: 2` counts the retries, `dead_lettered: 1` counts the job that ran out of them.

## 8. The dashboard

```
cd web && npm install && npm run dev
```

Open `http://localhost:3000` and sign in with the same email and password. The project
is selected for you. If something is already on 3000, Next will take 3001 instead and
collide with the api, so free the port first.

Worth clicking through:

- **queues** shows depth by priority tier, live workers, and why a queue is not moving
- **jobs** filters by status and type, and a job page shows every attempt with its logs
- **dead letter** lists exhausted jobs and replays them one at a time or in bulk
- **events** should show the tag `live`; submit another job and watch rows arrive

## 9. Other paths worth trying

Retries with a longer backoff, so you can watch a job sit in `retry_wait`:

```
curl -s -X POST $A/queues/$QUEUE/jobs \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"type":"fail"}' | jq -r .id
psql "$DATABASE_URL" -c "select status, attempt_count, run_at from jobs where type='fail'"
```

Kill the worker while a job is running and watch the reaper recover it. Two waits
matter here: the job has to be claimed before you kill anything, and the lease has to
expire before the reaper acts. The default lease is thirty seconds.

```
curl -s -X POST $A/queues/$QUEUE/jobs \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"type":"sleep","payload":{"ms":60000}}' | jq -r .id

until psql "$DATABASE_URL" -tAc \
  "select count(*) from jobs where type='sleep' and status='running'" | grep -q 1
do sleep 1; done
pkill -9 -f /tmp/fl-worker

until psql "$DATABASE_URL" -tAc \
  "select status from jobs where type='sleep' order by created_at desc limit 1" \
  | grep -qvE "running|claimed"
do sleep 3; done

psql "$DATABASE_URL" -c \
  "select status, attempt_count from jobs where type='sleep' order by created_at desc limit 1"
psql "$DATABASE_URL" -c \
  "select outcome from job_executions x join jobs j on j.id = x.job_id
    where j.type='sleep' order by x.started_at desc limit 1"
```

```
   status   | attempt_count
------------+---------------
 retry_wait |             1

 outcome
---------
 lost
```

You will see `retry_wait` or `queued` depending on when you looked. The reaper reports
the job lost and the retry policy puts it in `retry_wait` with a backoff; the promoter
moves it to `queued` once that backoff elapses. Either way the attempt is burned and
the execution is closed as `lost`.

Recovery is driven by the expired lease, which is why it works the same whether the
worker crashed, hung, or lost its network. Start the worker again with the command
from step 4 and the job runs:

```
WORKER_QUEUE=$QUEUE WORKER_CONCURRENCY=4 /tmp/fl-worker > /tmp/worker.log 2>&1 &
```

Concurrency, by submitting more than the cap and watching `in_flight` hold at four:

```
for i in $(seq 1 20); do
  curl -s -X POST $A/queues/$QUEUE/jobs \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"type":"sleep","payload":{"ms":2000}}' > /dev/null
done

for i in 1 2 3 4 5 6; do
  psql "$DATABASE_URL" -tAc \
    "select 'in_flight ' || fl.in_flight(id) || ' / cap ' || max_concurrency from queues"
  sleep 2
done
```

```
in_flight 4 / cap 4
in_flight 4 / cap 4
in_flight 4 / cap 4
in_flight 3 / cap 4
in_flight 1 / cap 4
```

Twenty jobs queued and four running at a time. Whether you see the count tail off
depends on how much work is left when you sample; what should never appear is a number
above the cap.

## Stopping

```
pkill -f "/tmp/fl-"
```

`go run` processes need killing by pid, which is the reason for building the binaries
in step 2. To reset completely:

```
psql "postgres://fenceline:fenceline@localhost:5433/postgres" \
  -c "drop database fenceline" -c "create database fenceline"
make migrate
```

## When something is wrong

| symptom | cause |
|---|---|
| `make db-up` says the container name is in use | a `fenceline-pg` container compose did not create, see step 1 |
| `address already in use` on 3001 | an api from an earlier run is still up, `lsof -i :3001` |
| requests succeed but the data is not there | a stale api on 3001 pointed at another database |
| `queue not found` from the worker | `WORKER_QUEUE` is not a queue id in this database |
| `401` on every call | `TOKEN` is unset or the shell was reopened |
| `project query parameter required` | `/workers` and `/dlq` need `?project=$PROJ` |
| jobs stay `queued` | no worker, or the worker has no handler for that type |
| jobs stay `scheduled` | `run_at` is in the future, or a dependency has not finished |
| dashboard shows nothing | no project selected, or the api is not on 3001 |

## Windows

The Go services and the dashboard run on Windows. All six binaries cross-compile clean
for `GOOS=windows` and `go vet` passes. What does not work is the tooling this
walkthrough uses: `make` is not present by default, and the Makefile, this document,
and the teardown steps are all POSIX shell.

### WSL2

Install WSL2 with a distribution of your choice, install Docker Desktop with WSL2
integration enabled, then clone and follow this document from the top with nothing
changed. Docker Desktop shares the daemon, so `make db-up` reaches the same engine and
`localhost:5433` resolves from both sides.

This is the route to take.

### Native PowerShell

If you would rather not use WSL2, the substitutions below cover every command in this
document. Docker Desktop, Go and Node all work natively; only the shell differs.

**These were written from the documented behaviour of the cmdlets, not executed.**
Everything else in this file was run and its output pasted. This section was not,
because the machine it was written on has no Windows.

| this document | PowerShell |
|---|---|
| `export VAR=value` | `$env:VAR = "value"` |
| `VAR=$(command)` | `$VAR = command` |
| `/tmp/fl-api` | `"$env:TEMP\fl-api.exe"` |
| `go build -o /tmp/fl-api ./cmd/api` | `go build -o "$env:TEMP\fl-api.exe" ./cmd/api` |
| `/tmp/fl-api > /tmp/api.log 2>&1 &` | `Start-Process "$env:TEMP\fl-api.exe" -RedirectStandardOutput "$env:TEMP\api.log" -RedirectStandardError "$env:TEMP\api.err"` |
| `pkill -f "/tmp/fl-"` | `Get-Process fl-api,fl-scheduler,fl-worker -ErrorAction SilentlyContinue \| Stop-Process` |
| `lsof -i :3001` | `Get-NetTCPConnection -LocalPort 3001` |
| `curl -s ... \| jq -r .token` | `(Invoke-RestMethod ...).token` |
| `until ...; do sleep 1; done` | `do { Start-Sleep 1 } until (...)` |

`make` has no default Windows equivalent. Run the underlying commands instead:

| target | what it runs |
|---|---|
| `make db-up` | `docker compose up -d postgres`, then wait for `pg_isready` |
| `make migrate` | `go run ./cmd/migrate` |
| `make check` | `gofmt -l .`, `go vet ./...`, `go build ./...`, `go test -race ./...` |
| `make diagram` | `go run ./cmd/diagram` |

Step 3 is the one worth rewriting rather than translating line by line, because
`Invoke-RestMethod` parses JSON into objects and removes the need for `jq`:

```powershell
$A = "http://localhost:3001"
$body = @{ email = "demo@example.com"; password = "demo-password" } | ConvertTo-Json

Invoke-RestMethod -Method Post "$A/auth/register" -Body $body -ContentType application/json
$token = (Invoke-RestMethod -Method Post "$A/auth/login" -Body $body -ContentType application/json).token
$h = @{ Authorization = "Bearer $token" }

$org = (Invoke-RestMethod -Method Post "$A/orgs" -Headers $h `
  -Body (@{ name = "demo org" } | ConvertTo-Json) -ContentType application/json).id

$proj = (Invoke-RestMethod -Method Post "$A/orgs/$org/projects" -Headers $h `
  -Body (@{ name = "demo project" } | ConvertTo-Json) -ContentType application/json).id

$pol = (Invoke-RestMethod -Method Post "$A/projects/$proj/retry-policies" -Headers $h `
  -Body (@{ name = "standard"; kind = "exponential"; max_attempts = 3
            base_delay_ms = 500; max_delay_ms = 5000 } | ConvertTo-Json) `
  -ContentType application/json).id

$queue = (Invoke-RestMethod -Method Post "$A/projects/$proj/queues" -Headers $h `
  -Body (@{ name = "demo"; retry_policy_id = $pol; max_concurrency = 4 } | ConvertTo-Json) `
  -ContentType application/json).id

"project $proj"
"queue   $queue"
```

Then the worker, which needs the queue id from above:

```powershell
$env:WORKER_QUEUE = $queue
$env:WORKER_CONCURRENCY = "4"
Start-Process "$env:TEMP\fl-worker.exe" -RedirectStandardOutput "$env:TEMP\worker.log" `
  -RedirectStandardError "$env:TEMP\worker.err"
```

`psql` is not installed with Docker Desktop. Either install the Postgres client tools,
or run queries inside the container:

```powershell
docker compose exec -T postgres psql -U fenceline -d fenceline -c "select status, count(*) from jobs group by status"
```

`Ctrl+C` reaches a service as SIGINT rather than SIGTERM. The services handle both, so
a worker still drains. `Start-Process` detaches, so a service outlives the shell that
started it; stop them with `Stop-Process` rather than by closing the window.
