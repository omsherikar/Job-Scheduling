# Running Fenceline on Windows

Everything from a clean machine to a job running end to end, in PowerShell.

The Go services and the dashboard run natively on Windows. All six binaries
cross-compile clean for `GOOS=windows` and `go vet` passes. What does not carry over
is the tooling the other documents assume: `make`, and a POSIX shell.

**These commands were not executed.** Every command in [RUNBOOK.md](RUNBOOK.md) was run
on macOS and its output pasted; this document was written from the documented behaviour
of each cmdlet, on a machine with no Windows. The Go and SQL behaviour it describes is
verified, the PowerShell syntax around it is not.

If you would rather not deal with that, [WSL2](#wsl2-instead) at the end is the shorter
route and [RUNBOOK.md](RUNBOOK.md) then applies unchanged.

## 1. Install what you need

| | |
|---|---|
| Git for Windows | https://git-scm.com/download/win |
| Docker Desktop | https://docker.com/products/docker-desktop, WSL2 backend |
| Go 1.25 | https://go.dev/dl |
| Node 22 | https://nodejs.org, only if you want the dashboard |

Open a new PowerShell window after installing so `PATH` is picked up, then check:

```powershell
git --version
docker --version
go version
node --version
```

Start Docker Desktop and wait for the whale icon to stop animating. `docker ps` should
return an empty table rather than an error.

## 2. Clone

```powershell
git clone https://github.com/omsherikar/Job-Scheduling.git
cd Job-Scheduling
```

## 3. Check the database port is free

Fenceline's Postgres listens on 5433 so it stays clear of an existing install on the
default 5432, but another container or a second install can still hold it.

```powershell
Get-NetTCPConnection -LocalPort 5433 -State Listen -ErrorAction SilentlyContinue |
  Select-Object LocalAddress, LocalPort, OwningProcess
```

No output means the port is free and you can go on to step 4.

If something comes back, either stop it, or give Fenceline a different port by editing
the `ports` line in `docker-compose.yml`:

```yaml
    ports:
      - "5434:5432"
```

Only the left number changes. Use that same port everywhere `DATABASE_URL` appears
below.

To see what is holding it:

```powershell
Get-Process -Id (Get-NetTCPConnection -LocalPort 5433 -State Listen).OwningProcess
```

## 4. Start Postgres

```powershell
docker compose up -d postgres
```

Wait for it to accept connections:

```powershell
do {
  Start-Sleep -Seconds 1
  docker compose exec -T postgres pg_isready -U fenceline -d fenceline | Out-Null
} until ($LASTEXITCODE -eq 0)
"postgres is up"
```

## 5. Set the environment

These last for the life of the PowerShell window. Open a new window and you set them
again.

```powershell
$env:DATABASE_URL      = "postgres://fenceline:fenceline@localhost:5433/fenceline"
$env:TEST_DATABASE_URL = "postgres://fenceline:fenceline@localhost:5433/postgres"
```

If you changed the port in step 3, change it in both.

## 6. Apply the migrations

```powershell
go run ./cmd/migrate
```

```
001_types
002_core
...
030_shard_resize
applied 30
```

Running it twice is safe. It takes an advisory lock and the second run prints
`up to date`.

## 7. Build the binaries

Build rather than using `go run`. `go run` compiles to a temporary executable with a
different name, which makes the process hard to find and stop later.

```powershell
New-Item -ItemType Directory -Force -Path bin, logs | Out-Null

go build -o bin\fl-api.exe       ./cmd/api
go build -o bin\fl-scheduler.exe ./cmd/scheduler
go build -o bin\fl-worker.exe    ./cmd/worker
go build -o bin\fl-archiver.exe  ./cmd/archiver
```

## 8. Start the api and the scheduler

`-PassThru` hands back the process object so you can stop it precisely later. Standard
output and standard error have to go to different files.

```powershell
$api = Start-Process -FilePath .\bin\fl-api.exe -NoNewWindow -PassThru `
  -RedirectStandardOutput logs\api.log -RedirectStandardError logs\api.err

$sched = Start-Process -FilePath .\bin\fl-scheduler.exe -NoNewWindow -PassThru `
  -RedirectStandardOutput logs\sched.log -RedirectStandardError logs\sched.err

do { Start-Sleep -Seconds 1 } until (
  Test-NetConnection localhost -Port 3001 -InformationLevel Quiet -WarningAction SilentlyContinue
)
Get-Content logs\api.log
```

```
api listening on :3001
```

`Start-Process` inherits the environment of the window that launched it, so both
services pick up the `DATABASE_URL` from step 5.

The worker is not started yet. It claims from one queue and takes that queue's id at
startup, and no queue exists in a fresh database.

## 9. Create a user, an organization, a project, a policy and a queue

`Invoke-RestMethod` parses the JSON response into an object, so there is no need for
anything like `jq`.

```powershell
$A = "http://localhost:3001"
$cred = @{ email = "demo@example.com"; password = "demo-password" } | ConvertTo-Json

Invoke-RestMethod -Method Post "$A/auth/register" -Body $cred -ContentType application/json

$token = (Invoke-RestMethod -Method Post "$A/auth/login" -Body $cred -ContentType application/json).token
$h = @{ Authorization = "Bearer $token" }

$org = (Invoke-RestMethod -Method Post "$A/orgs" -Headers $h -ContentType application/json `
  -Body (@{ name = "demo org" } | ConvertTo-Json)).id

$proj = (Invoke-RestMethod -Method Post "$A/orgs/$org/projects" -Headers $h -ContentType application/json `
  -Body (@{ name = "demo project" } | ConvertTo-Json)).id

$pol = (Invoke-RestMethod -Method Post "$A/projects/$proj/retry-policies" -Headers $h -ContentType application/json `
  -Body (@{ name = "standard"; kind = "exponential"; max_attempts = 3
            base_delay_ms = 500; max_delay_ms = 5000 } | ConvertTo-Json)).id

$queue = (Invoke-RestMethod -Method Post "$A/projects/$proj/queues" -Headers $h -ContentType application/json `
  -Body (@{ name = "demo"; retry_policy_id = $pol; max_concurrency = 4 } | ConvertTo-Json)).id

"project $proj"
"queue   $queue"
```

The password has to be at least eight characters. Registering the same email twice
returns a `409`.

## 10. Start the worker

```powershell
$env:WORKER_QUEUE = $queue
$env:WORKER_CONCURRENCY = "4"

$worker = Start-Process -FilePath .\bin\fl-worker.exe -NoNewWindow -PassThru `
  -RedirectStandardOutput logs\worker.log -RedirectStandardError logs\worker.err

Start-Sleep -Seconds 3
(Invoke-RestMethod "$A/workers?project=$proj" -Headers $h).items |
  Select-Object state, max_concurrency, hostname
```

```
state  max_concurrency hostname
-----  --------------- --------
active               4 YOUR-PC
```

`queue not found` in `logs\worker.log` means `WORKER_QUEUE` is not a queue id in the
database the worker is pointed at.

## 11. Submit some jobs

The worker ships three handlers for exactly this: `noop` returns immediately, `sleep`
waits for the milliseconds in its payload, and `fail` always errors.

```powershell
foreach ($t in "noop", "sleep", "fail") {
  $job = Invoke-RestMethod -Method Post "$A/queues/$queue/jobs" -Headers $h -ContentType application/json `
    -Body (@{ type = $t; payload = @{ ms = 500 } } | ConvertTo-Json)
  "$($job.type)  $($job.id)  $($job.status)"
}
```

## 12. Watch them finish

Wait a few seconds. `psql` does not come with Docker Desktop, so run queries inside the
container:

```powershell
docker compose exec -T postgres psql -U fenceline -d fenceline `
  -c "select status, count(*) from jobs group by status order by status"
```

```
   status    | count
-------------+-------
 completed   |     2
 dead_letter |     1
```

Every attempt is kept:

```powershell
docker compose exec -T postgres psql -U fenceline -d fenceline `
  -c "select j.type, x.attempt, x.outcome from job_executions x join jobs j on j.id = x.job_id order by j.type, x.attempt"
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
policy in step 9. Nothing is overwritten on retry, so attempt 1 is still readable after
attempt 3.

## 13. Read it back over the api

```powershell
(Invoke-RestMethod "$A/dlq?project=$proj" -Headers $h).items |
  Select-Object reason, last_error_message

Invoke-RestMethod "$A/queues/$queue/failure-summary" -Headers $h |
  Select-Object state, summary -ExpandProperty failures

(Invoke-RestMethod "$A/projects/$proj/queue-health" -Headers $h).items |
  ForEach-Object { $_.queue.name, $_.queue.in_flight, $_.live_workers, $_.last_hour }
```

`state` coming back as `unavailable` on the failure summary means no
`ANTHROPIC_API_KEY` is set, so the failures are grouped but no written summary is
produced. That is the expected result unless you set one.

## 14. The dashboard

```powershell
cd web
npm install
npm run dev
```

Open `http://localhost:3000` and sign in with the same email and password. The project
is selected for you.

If something already holds 3000, Next will take 3001 and collide with the api. Free the
port first, or run `npm run dev -- --port 3005`.

## 15. Stopping

```powershell
Stop-Process -Id $api.Id, $sched.Id, $worker.Id -ErrorAction SilentlyContinue
docker compose down
```

If you lost the variables by closing the window:

```powershell
Get-Process fl-api, fl-scheduler, fl-worker -ErrorAction SilentlyContinue | Stop-Process
```

To reset the database completely, `docker compose down -v` also removes the volume, and
step 4 onward rebuilds from empty.

## WSL2 instead

Install WSL2 with a distribution of your choice, enable WSL2 integration in Docker
Desktop, then clone inside the Linux filesystem and follow [RUNBOOK.md](RUNBOOK.md)
with nothing changed. Docker Desktop shares one daemon, so `make db-up` reaches the
same engine and `localhost` resolves from both sides.

Clone into the Linux side, `~/Job-Scheduling` rather than `/mnt/c/...`. Go builds and
Docker volume mounts across the `/mnt/c` boundary are slow enough to be noticeable.

Everything in that document was run on a POSIX shell, so under WSL2 it behaves as
written.

## When something is wrong

| symptom | cause |
|---|---|
| `docker: command not found` | Docker Desktop is not running, or PATH predates the install |
| `ports are not available` on 5433 | something already holds it, see step 3 |
| `address already in use` on 3001 | an api from an earlier run, `Get-Process fl-api \| Stop-Process` |
| requests succeed but the data is missing | a stale api on 3001 pointed at another database |
| `queue not found` from the worker | `WORKER_QUEUE` is not a queue id in this database |
| `401` on every call | `$token` is unset, or you opened a new window |
| `project query parameter required` | `/workers` and `/dlq` need `?project=$proj` |
| jobs stay `queued` | no worker running, or no handler for that type |
| jobs stay `scheduled` | `run_at` is in the future, or a dependency has not finished |
| dashboard shows nothing | no project selected, or the api is not on 3001 |
| `running scripts is disabled` | only applies to saved `.ps1` files, not to commands typed in |
