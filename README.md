# GoFlow

**A concurrent job processing and worker orchestration system, built in Go.**

GoFlow is a local job-processing platform: clients submit jobs over a REST API, jobs are persisted in SQLite and placed on an in-memory priority queue, and a pool of goroutine-based workers pulls from that queue, executes the job, and reports the outcome back into SQLite — with automatic retries, exponential backoff, timeouts, cancellation, scheduling, dead-lettering, and dynamic worker scaling along the way.

It's deliberately **not** a CRUD app with a database behind it. The point of the project is the concurrency: goroutines, channels, mutexes, `context` cancellation, and SQLite transactions doing real coordination work, with tests (including `-race`) to back that up.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [How It Works](#how-it-works)
- [Job Lifecycle](#job-lifecycle)
- [Worker Architecture](#worker-architecture)
- [Priority Queue](#priority-queue)
- [Priority Aging](#priority-aging)
- [Retry Strategy](#retry-strategy)
- [Scheduling](#scheduling)
- [Concurrency Design](#concurrency-design)
- [SQLite Design](#sqlite-design)
- [Dynamic Worker Scaling](#dynamic-worker-scaling)
- [API Documentation](#api-documentation)
- [Database Schema](#database-schema)
- [Dashboard](#dashboard)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Running Locally](#running-locally)
- [Running with Docker](#running-with-docker)
- [Running Tests](#running-tests)
- [Race Detector](#race-detector)
- [Example API Requests](#example-api-requests)
- [Design Decisions](#design-decisions)
- [Challenges](#challenges)
- [Future Improvements](#future-improvements)

---

## Features

- **REST API** for submitting, listing, inspecting, cancelling, retrying, and deleting jobs
- **In-memory priority queue** (HIGH / MEDIUM / LOW) built on `container/heap`, no Redis
- **Priority aging** — a job waiting in the queue too long is boosted one level at a time, so sustained HIGH-priority load cannot starve older LOW jobs
- **Dynamically-scaled worker pool** — the number of goroutines pulling jobs grows and shrinks with queue depth
- **Per-job-type concurrency limits** — cap how many jobs of a given type run at once (e.g. `JOB_TYPE_LIMITS=image_processing=2`), independent of overall worker count
- **Automatic retries with exponential backoff**, configurable max attempts, and a DEAD letter state after retries are exhausted
- **Per-job timeouts** via `context.WithTimeout`, distinct from cancellation
- **Manual job cancellation** that actually interrupts a *running* job via `context` cancellation, not just a database flag
- **Scheduled jobs** — submit now, run later; a scheduler goroutine promotes due jobs
- **Worker heartbeats and stale-worker detection**
- **SQLite as the single source of truth**, with a claim-transaction that guarantees two workers can never process the same job
- **Prometheus-compatible `/metrics`** endpoint (hand-rolled, stdlib-only — see [Design Decisions](#design-decisions))
- **Graceful shutdown** on SIGINT/SIGTERM: stop new jobs → stop the scheduler → let in-flight jobs finish (up to a timeout) → force-cancel stragglers → close the database
- **A small vanilla JS/CSS dashboard** polling the same REST API
- Meaningful, concurrency-focused tests, all passing under `go test -race ./...`

## Architecture

```
Client
   |
   v
REST API  ───────────────►  SQLite (source of truth for job state)
   |                              ▲
   v                              │
Job Manager  ────────────────────-┘
   |
   v
Priority Queue (in-memory, container/heap)
   |
   ├──► Worker 1 ──┐
   ├──► Worker 2 ──┼──► Job Handlers (email, report_generation,
   └──► Worker N ──┘     data_processing, image_processing, webhook)
```

SQLite is the persistent source of truth for every job's state. The in-memory priority queue only ever holds **job IDs that are ready to run right now** — it is not a second copy of job state, just a work-dispatch structure. If the process restarts, the queue is empty but SQLite still knows exactly what was PENDING, RETRYING, or (if a worker died mid-job) RUNNING, and `cmd/server/main.go` rebuilds the queue from that on startup (see [Challenges](#challenges)).

The **Job Manager** (`internal/manager`) is the only component allowed to push onto the queue or drive a job's state transitions. Both the REST API and the worker pool go through it, which is what keeps submit/claim/complete/fail/cancel semantics identical no matter which side triggers them.

## Tech Stack

| Layer          | Choice                                                      |
|-----------------|--------------------------------------------------------------|
| Language        | Go 1.22 (standard library wherever practical)                |
| Database        | SQLite, via `github.com/mattn/go-sqlite3` (cgo)               |
| HTTP routing    | `net/http` + Go 1.22's method/wildcard `ServeMux` patterns — no router dependency |
| Metrics         | Hand-rolled Prometheus text-exposition exporter (stdlib only) |
| Frontend        | Plain HTML/CSS/vanilla JS — no React, no Node.js               |
| Logging         | `log/slog`                                                    |
| Containerization| Docker + Docker Compose                                        |

No Redis, Kafka, RabbitMQ, PostgreSQL, or Kubernetes — the priority queue, retry scheduling, and worker coordination are all built from `container/heap`, channels, mutexes, `context`, and SQLite transactions.

## How It Works

1. A client `POST`s a job to `/api/jobs`.
2. The Job Manager validates it, writes a row to SQLite (`PENDING`, or `SCHEDULED` if `scheduled_at` is in the future), and — if PENDING — pushes the job ID onto the priority queue.
3. A worker goroutine, blocked on `queue.Pop`, wakes up, and asks the manager to **claim** the job: an SQLite transaction flips it to `RUNNING` only if it's still `PENDING`/`RETRYING` (see [SQLite Design](#sqlite-design)).
4. The worker looks up the registered handler for the job's `type`, wraps it in a `context.WithTimeout`, and runs it.
5. On success → `COMPLETED`. On failure → retry with backoff, or `DEAD` if retries are exhausted. On the manager cancelling the job's context → `CANCELLED`.
6. Every attempt (start, end, duration, error) is logged to `job_attempts`, independent of the job's current state, so failure history survives retries.

## Job Lifecycle

```
                 ┌──────────┐
   POST /jobs    │          │  scheduled_at due
  ──────────────►│ PENDING  │◄────────────────────┐
  (no schedule)   │          │                     │
                 └────┬─────┘              ┌───────┴────┐
                      │ claimed             │ SCHEDULED  │
                      ▼                     └────────────┘
                 ┌──────────┐                      ▲
                 │ RUNNING  │                       │ scheduled_at
                 └───┬──┬───┘                       │ in the future
        success       │  │ failure                  │
       ┌──────────────┘  └───────────────┐    POST /jobs (scheduled_at set)
       ▼                                  ▼
 ┌────────────┐                    ┌────────────┐   retries remain
 │ COMPLETED  │                    │  RETRYING  │──────────────────► back to queue → RUNNING
 └────────────┘                    └─────┬──────┘
                                          │ retries exhausted
                                          ▼
                                     ┌────────┐   POST /retry (manual)
                                     │  DEAD  │─────────────────────► PENDING
                                     └────────┘

   cancel (any non-terminal state) ──► CANCELLED
```

`COMPLETED`, `CANCELLED`, and `DEAD` are terminal. `FAILED` exists in the schema/enum for a single failed attempt that hasn't yet been classified into `RETRYING` or `DEAD` by the manager, and is also used as a legal source state for a manual retry alongside `DEAD`.

## Worker Architecture

Each worker is one goroutine running `internal/workers.Pool.runWorker`:

```go
for {
    jobID, ok := queue.Pop(workerCtx)   // blocks until work or shutdown
    if !ok { return }                   // pool told us to stop pulling
    processJob(workerCtx, workerID, jobID)
}
```

A worker's `workerCtx` only controls whether it keeps *pulling new* jobs — cancelling it (for scale-down or shutdown) does **not** abort a job it's already executing. That job runs under its own `context.WithTimeout(context.Background(), job.Timeout())`, registered with the manager's cancel registry so an explicit `POST /cancel` (or a hard-shutdown timeout) can still reach in and stop it. A separate heartbeat goroutine per worker pulses `workers.last_heartbeat` on a fixed interval regardless of whether the worker is busy.

A panic inside a handler is recovered inside `processJob` and converted into a normal failed attempt — it goes through the same retry/dead-letter path as any other error, and the worker goroutine survives.

## Priority Queue

`internal/queue` is a `container/heap` of `{JobID, Weight, seq}` guarded by a mutex and a `sync.Cond`. `Weight` comes from `Priority.Weight()` (HIGH=0, MEDIUM=1, LOW=2 — lower sorts first); `seq` is a monotonically increasing insertion counter used as a tie-breaker, so jobs of equal priority come out in FIFO order. `Pop(ctx)` blocks on the condition variable until an item is pushed, the queue is `Close()`d, or `ctx` is cancelled — the last case is handled with `context.AfterFunc` broadcasting the condition variable, since `sync.Cond` has no native context support.

No external broker. At the scale this is built for, a heap plus a mutex is simpler to reason about, easier to explain in an interview, and just as correct.

## Priority Aging

A pure priority queue has a starvation hole: under a sustained stream of HIGH-priority submissions, a LOW job can wait forever. GoFlow closes it with **priority aging** — a `scheduler.Ager` goroutine ticks every `AGING_INTERVAL_SECONDS` (default `60`, set to `0` to disable) and boosts every queued job one weight level (`LOW` → `MEDIUM` → `HIGH`), implemented as `Queue.Age(floor)` decrementing weights under the queue mutex and re-establishing the heap invariant with `heap.Init`. The floor is HIGH's weight, so aging never invents a new priority level, and the FIFO `seq` tie-breaker is untouched — a boosted LOW job competes with fresh HIGH jobs in arrival order. The boost lives only in the in-memory queue: the priority stored in SQLite is never rewritten, so a restart simply rebuilds the queue from the original priorities and aging starts over.

## Retry Strategy

Failures go through `Manager.Fail`, which increments `retry_count` in a single SQLite transaction and decides the next state:

| Attempt | Outcome if it fails | Delay before next attempt |
|---------|----------------------|-----------------------------|
| 1       | → RETRYING (if `max_retries` > 0) | immediate (no delay) |
| 2       | → RETRYING | 2s  |
| 3       | → RETRYING | 4s  |
| 4       | → DEAD (if `max_retries` = 3) | — |

The delay before retry *N* is `2^(N-1)` seconds, implemented as `time.AfterFunc` in `internal/manager`. The job stays `RETRYING` in SQLite for the whole backoff window; only its placement on the in-memory queue is delayed. `max_retries` is configurable per job (`max_retries` in the request body) or globally via `MAX_RETRIES`.

## Scheduling

`internal/scheduler` runs a single goroutine that polls SQLite every `SCHEDULER_POLL_INTERVAL_SECONDS` for `SCHEDULED` jobs whose `scheduled_at` has passed, promotes each to `PENDING` in a conditional `UPDATE ... WHERE status = 'SCHEDULED'`, and pushes it onto the priority queue. Submit a job with a future `scheduled_at` and it starts `SCHEDULED`; everything else about it (priority, handler, retries) behaves identically once it's promoted.

## Concurrency Design

| Primitive | Where | What it protects |
|---|---|---|
| `container/heap` + `sync.Mutex` + `sync.Cond` | `internal/queue` | The priority queue's internal slice |
| SQLite transaction | `Store.ClaimJob` | Exactly one worker can flip a job PENDING/RETRYING → RUNNING |
| SQLite transaction | `Store.FailJob`, `Store.CancelJob`, `Store.Heartbeat` (+ prune) | Multi-statement state changes stay atomic |
| `context.WithTimeout` | per-job execution | Job timeouts, independent of cancellation |
| `context.CancelFunc` registry (mutex-guarded map) | `internal/manager` | Manual cancellation reaches a *running* goroutine, not just a DB row |
| `sync/atomic` counters | `internal/metrics` | Lock-free metric increments under concurrent worker access |
| `chan struct{}` semaphore per limited type | `internal/jobs.TypeLimiter` | At most N concurrent executions of a capped job type |
| `sync.WaitGroup` | `internal/workers.Pool` | Graceful shutdown waits for every worker goroutine to actually exit |

`go test -race ./...` passes across the whole module (see [Race Detector](#race-detector)); the worker pool tests in particular spin up many goroutines claiming, failing, cancelling, and completing jobs concurrently against a real SQLite database.

## SQLite Design

Five tables (`migrations/0001_init.sql`):

- **`jobs`** — the mutable current state of every job. Indexed on `status`, `(status, priority)`, and `scheduled_at`.
- **`job_attempts`** — an append-only audit log of every execution attempt (worker, start/end time, outcome, error, duration), independent of the job's current status. This is how `GET /api/jobs/{id}/attempts` can show full failure history even after a job has moved through several retries.
- **`workers`** — one row per worker goroutine: status, current job, processed/failed counts, last heartbeat.
- **`worker_heartbeats`** — an append-only heartbeat log, pruned to the most recent 50 rows per worker on every insert (see `Store.Heartbeat`), so it gives a short recent history instead of either a single timestamp or an unbounded table.
- **`users`** — seeded with one `system` user; `jobs.created_by` references it. Present so job attribution has somewhere to live if/when GoFlow grows multi-user submission; not exposed as an API concept today.

The one transaction worth reading closely is `Store.ClaimJob`: it `SELECT`s the job, checks its status is `PENDING` or `RETRYING`, and `UPDATE`s it to `RUNNING` **with `WHERE status = <the status it just read>`** in the same transaction, then checks `RowsAffected()`. If two workers ever raced to claim the same job, the loser's `UPDATE` would affect zero rows and it gets `ErrConflict` back. Combined with the fact that a job ID only ever comes off the in-memory queue once, this is belt-and-suspenders: the queue is the fast path, the transaction is the correctness guarantee. `TestClaimJob_ConcurrentClaimsAreMutuallyExclusive` fires 20 goroutines at the same job ID and asserts exactly one wins.

## Dynamic Worker Scaling

`internal/workers.Pool` re-evaluates target worker count every `SCALING_CHECK_INTERVAL_SECONDS` against `SCALING_THRESHOLDS` (default `10:2,50:5,999999:10` — queue size ≤10 → 2 workers, ≤50 → 5, otherwise → 10). Scaling up spawns new worker goroutines immediately; scaling down cancels a worker's *pull-loop* context, which lets it finish any job it's currently running before it exits and deregisters itself. Thresholds are fully configurable via the `SCALING_THRESHOLDS` env var.

## Per-Type Concurrency Limits

`JOB_TYPE_LIMITS` (default empty, i.e. every type unlimited) caps concurrent executions per job type: `JOB_TYPE_LIMITS=image_processing=2,email_sending=5`. This is for types that thrash a scarce downstream resource — a rate-limited API, a single GPU — even when the pool has idle workers ready to run more of them.

Mechanics, honestly stated: a worker pops a job off the queue and claims it as usual, then waits for a free type slot before invoking the handler. That wait is part of the attempt and bounded by the job's own timeout — a wait that times out consumes an attempt and follows the normal retry path, and a manual cancel interrupts it like any other cancellation. Uncapped types never wait (one map read per job). One caveat worth knowing: a worker waiting for a type slot holds its claim and does not pull other jobs in the meantime, so a badly-chosen cap can create head-of-line blocking behind a long-running job of the same type. Keep caps at or above the count of workers you expect to dedicate to that type.

## API Documentation

All request/response bodies are JSON. Errors are `{"error": "message"}` with an appropriate 4xx/5xx status.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/jobs` | Submit a new job |
| `GET` | `/api/jobs?status=&limit=` | List jobs, optionally filtered by status |
| `GET` | `/api/jobs/{id}` | Get a single job |
| `GET` | `/api/jobs/{id}/attempts` | Full attempt history for a job (useful for dead-letter inspection) |
| `POST` | `/api/jobs/{id}/cancel` | Cancel a job (interrupts it if currently running) |
| `POST` | `/api/jobs/{id}/retry` | Manually resurrect a DEAD/FAILED job back to PENDING |
| `DELETE` | `/api/jobs/{id}` | Delete a job permanently |
| `GET` | `/api/workers` | List all workers and their stats |
| `GET` | `/api/workers/{id}` | Get a single worker |
| `GET` | `/api/workers/{id}/history?limit=` | A worker's recent job attempts, newest first (from `job_attempts`) |
| `GET` | `/api/stats` | Aggregate job counts, queue size, active workers, avg processing time |
| `GET` | `/api/health` | Liveness/readiness check (pings the database) |
| `GET` | `/metrics` | Prometheus text-exposition metrics |

### Create-job request body

```json
{
  "type": "report_generation",
  "payload": { "report_name": "q3-summary", "rows": 500 },
  "priority": "HIGH",
  "max_retries": 3,
  "timeout_seconds": 30,
  "scheduled_at": "2026-09-10T10:00:00Z"
}
```

Only `type` and `priority` are required (`priority` must be `HIGH`, `MEDIUM`, or `LOW`); everything else falls back to server defaults. Registered `type`s: `email`, `report_generation`, `data_processing`, `image_processing`, `webhook`.

## Database Schema

See `migrations/0001_init.sql` for the authoritative schema. Summary:

```
users(id, username, api_key, created_at)
jobs(id, type, payload, priority, status, created_at, started_at, completed_at,
     scheduled_at, retry_count, max_retries, timeout_secs, error_message,
     worker_id, created_by → users.id)
job_attempts(id, job_id → jobs.id, attempt_number, worker_id, started_at,
             completed_at, status, error_message, duration_ms)
workers(id, status, current_job_id, jobs_processed, jobs_failed,
        started_at, last_heartbeat, stopped_at)
worker_heartbeats(id, worker_id → workers.id, heartbeat_at)
```

## Dashboard

A single static page (`web/index.html` + `style.css` + `app.js`) served directly by the Go binary, polling `/api/stats`, `/api/jobs`, and `/api/workers` every 2 seconds. It shows live counts by status, a worker table (status/current job/processed/failed/last heartbeat), a recent-jobs table, and a small form to submit a job directly against the real API. No build step, no framework — open `http://localhost:8080` after starting the server.

*(No screenshot image is checked into the repo — run it locally and look; a static PNG would just go stale.)*

## Project Structure

```
goflow/
├── cmd/server/main.go        # wiring + graceful shutdown
├── internal/
│   ├── api/                  # HTTP handlers, router, middleware
│   ├── config/                # env-based configuration
│   ├── database/               # SQLite connection, migrations, repositories
│   ├── jobs/                   # Job model, Status/Priority, handler registry + example handlers
│   ├── manager/                 # Job Manager: submit/claim/complete/fail/cancel orchestration
│   ├── metrics/                  # stdlib-only Prometheus exporter
│   ├── queue/                     # in-memory priority queue
│   ├── scheduler/                  # promotes due SCHEDULED jobs
│   └── workers/                     # worker pool: execution, scaling, heartbeats
├── web/                        # dashboard (HTML/CSS/JS)
├── migrations/                  # SQL schema
├── tests/                        # black-box integration tests
├── data/                          # SQLite lives here at runtime (gitignored)
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── go.mod / go.sum
```

## Installation

Requires **Go 1.22+** and a C toolchain (gcc/clang — `go-sqlite3` uses cgo). On Debian/Ubuntu:

```bash
sudo apt-get install -y golang-go gcc
```

Clone and fetch dependencies:

```bash
git clone <this-repo-url> goflow
cd goflow
go mod download
```

## Running Locally

```bash
cp .env.example .env   # optional; sane defaults work with no .env at all
go run ./cmd/server
```

Every supported variable is listed in `.env.example` with its default. Real environment variables take precedence over `.env`.

The server listens on `:8080` by default, auto-creates `./data/goflow.db`, and applies migrations on startup. Visit `http://localhost:8080` for the dashboard, or:

```bash
curl http://localhost:8080/api/health
```

To build a standalone binary instead:

```bash
go build -o goflow-server ./cmd/server
./goflow-server
```

## Running with Docker

```bash
docker compose up --build
```

This builds the image (multi-stage: `golang:1.22-bookworm` for the cgo build, `debian:bookworm-slim` for the runtime), starts the container, and persists SQLite in the `goflow-data` named volume. The dashboard is at `http://localhost:8080`.

## Running Tests

```bash
go test ./...
```

Covers the priority queue, job handlers, the full database layer (including the concurrent-claim safety test), the worker pool (execution, retries, timeouts, cancellation, panics, concurrency, dynamic scaling, graceful shutdown), the scheduler, every HTTP handler, and end-to-end integration tests that exercise a fully wired server through real HTTP requests.

## Race Detector

```bash
go test -race ./...
```

This is the test suite worth running before touching the concurrency code: it exercises the worker pool test (`internal/workers`) with many goroutines claiming, executing, cancelling, and completing jobs against a shared queue and a shared SQLite connection simultaneously, alongside the queue package's own concurrent producer/consumer test.

## Example API Requests

**Submit a high-priority job:**

```bash
curl -X POST localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"email","priority":"HIGH","payload":{"to":"a@b.com","subject":"hi","body":"hello"}}'
```

**Submit a scheduled job:**

```bash
curl -X POST localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"report_generation","priority":"MEDIUM","payload":{"report_name":"q3","rows":500},"scheduled_at":"2026-09-10T10:00:00Z"}'
```

**Cancel a running job:**

```bash
curl -X POST localhost:8080/api/jobs/<job-id>/cancel
```

**Inspect a dead job's failure history and resurrect it:**

```bash
curl localhost:8080/api/jobs/<job-id>/attempts
curl -X POST localhost:8080/api/jobs/<job-id>/retry
```

**Stats and metrics:**

```bash
curl localhost:8080/api/stats
curl localhost:8080/metrics
```

## Design Decisions

- **Hand-rolled metrics instead of `client_golang`.** The official Prometheus client pulls in a transitive dependency tree (`golang.org/x/sys`, `google.golang.org/protobuf`, etc.) for a metric surface that's about nine series here. `internal/metrics` is ~150 lines of `sync/atomic` counters/gauges and a manually-written text-exposition histogram — same `/metrics` output, zero extra dependencies.
- **`mattn/go-sqlite3` (cgo) over a pure-Go driver.** It's the most battle-tested SQLite driver for Go and needs no code beyond the standard `database/sql` interface. The tradeoff is a cgo build (a C toolchain is required — see [Installation](#installation) and the Dockerfile's build stage).
- **A single SQLite connection (`db.SetMaxOpenConns(1)`).** SQLite only supports one writer at a time regardless of connection count; letting `database/sql` serialize through one connection (with WAL mode and a busy timeout) is simpler than fighting `SQLITE_BUSY` errors under concurrent worker load, and at this project's scale it's not a bottleneck.
- **No router library.** Go 1.22 added method + wildcard patterns (`"POST /api/jobs/{id}/cancel"`) to `net/http.ServeMux`, which covers every route this API needs without a third-party dependency.
- **Job execution context is rooted at `context.Background()`, not derived from the worker's pull-loop context.** A scale-down or graceful-shutdown signal cancels the *pull loop* (stop taking new work) without immediately yanking the job a worker is already running out from under it. Only an explicit `/cancel` call or the hard-shutdown timeout reaches in via the manager's cancel-func registry.

## Challenges

- **Distinguishing "cancelled by a user" from "cancelled because the process is shutting down."** Both end up calling the same `context.CancelFunc`. The fix: `Manager.CancelJob` writes `CANCELLED` to SQLite *before* invoking the cancel func, so by the time the worker's handler returns `context.Canceled`, `FailJob` can check the row's current status and no-op instead of bouncing an intentionally-cancelled job into `RETRYING`.
- **Orphaned `RUNNING` jobs after a crash.** If the process dies mid-job, that job's row is stuck at `RUNNING` forever with no worker coming back to finish it. `cmd/server/main.go` calls `Store.ResetOrphanedRunningJobs()` on startup, which resets anything still `RUNNING` back to `PENDING` and re-enqueues it — a `RUNNING` row can only mean a previous process was killed mid-flight, since a clean shutdown always drives jobs to a terminal state or leaves them `PENDING`/`RETRYING` for exactly this recovery path to pick up.
- **A worker that doesn't return promptly after its context is cancelled will still block the worker goroutine.** Go doesn't preempt a goroutine that's ignoring `ctx.Done()`. Every example handler in `internal/jobs/handlers.go` is written around a `sleepWithContext` helper specifically so timeouts and cancellation are honored quickly — a real-world handler wrapping a blocking library call (e.g. an HTTP request without a context-aware client) wouldn't get that for free, and that's a genuine limitation worth knowing about rather than a bug in this design.

## Future Improvements

- An API-key-scoped auth middleware built on the existing `users` table
- Structured job payload schemas (JSON Schema validation per job `type`) instead of handler-level ad-hoc validation
