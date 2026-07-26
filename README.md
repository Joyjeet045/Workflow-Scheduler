# Workflow Scheduler — Distributed Workflow Orchestration Platform

A production-style workflow orchestrator written in Go. It executes JSON and
YAML workflow definitions as dependency-aware DAGs, exposes an HTTP API for
workflow and run control, and ships with scheduling, retries, compensation,
async queue-backed execution, SQLite durability, scoped API auth, and
Prometheus-compatible monitoring.

---

## Features

| Area | What's implemented |
| --- | --- |
| **Core orchestration** | DAG-aware task execution with `dependsOn`, bounded parallelism via `maxConcurrency`, structured run results, and workflow status aggregation |
| **Workflow model** | JSON/YAML workflow loading, task input maps, workflow metadata (`id`, `name`, `description`, `version`), task and workflow timeouts |
| **Resilience** | Per-task retry policies (`maxAttempts`, `backoffBase`), `failFast`, optional `allowFailure`, and task start delays with `runAfter` |
| **Conditional routing** | `on_success`, `on_failed`, and `output_contains` conditions for branch-style task execution |
| **Input templating** | Upstream result interpolation such as `${task.output}` and `${task.status}` in downstream task input |
| **Compensation** | Saga-style compensation actions executed in reverse completion order when `compensateOnFailure` is enabled |
| **Scheduling** | Interval-based and cron-based scheduling, `runOnStartup`, overlap control, leader lock file, and persisted schedule registrations |
| **Persistence** | File-backed workflow/run snapshots for simple deployments, plus durable SQLite storage for workflows, runs, jobs, and transition history |
| **Async execution** | Queue-backed async run submission, lease-based worker processing, job cancellation, dead-letter handling, replay, and retention cleanup |
| **API platform** | Workflow CRUD, run submission/query/cancel, schedule APIs, job inspection/history, bulk cancel/requeue/purge, pagination, ordering, filters, and envelope responses |
| **Security** | Optional admin/read/write API token scopes over `Authorization: Bearer` or `X-API-Token` |
| **Monitoring** | Prometheus-compatible `/metrics`, plus `/health`, `/ready`, job stats, and run visibility endpoints |

---

## Architecture

```mermaid
flowchart TB
    subgraph client[Clients]
        cli[CLI run / schedule / worker]
        sdk[HTTP clients / automation]
    end

    subgraph node[Workflow Scheduler Node]
        loader[Workflow loader JSON/YAML] --> exec[Executor]
        exec --> graph[DAG validation + task readiness]
        exec --> store[(WorkflowStore)]
        exec --> telem[Telemetry collector]
        api[HTTP API server] --> exec
        api --> store
        api --> sched[Scheduler service]
        api --> q[(RunQueue)]
        sched --> exec
        sched --> q
        worker[Worker service] --> q
        worker --> exec
    end

    cli --> loader
    sdk --> api
    store --> file[(File store)]
    store --> sqlite[(SQLite store)]
    telem --> prom[/metrics HTTP/]
```

The orchestrator centers on a validated workflow DAG plus a structured run/job
state model. The executor resolves dependencies, conditions, retries, timeouts,
and compensation, while the API, scheduler, and worker modes are simply
different ways of feeding work into the same execution core. SQLite-backed mode
adds a durable shared state boundary so API and worker processes can coordinate
async execution safely across multiple processes.

---

## Project layout

```text
cmd/
  orchestrator/     # the main binary (run + schedule + serve + worker)
internal/
  actions/          # built-in actions and registry wiring
  api/              # HTTP API, auth, filtering, pagination, bulk operations
  domain/           # workflows, runs, jobs, schedules, and interfaces
  engine/           # DAG validation and executor
  queue/            # in-memory run queue
  scheduler/        # interval/cron scheduling service
  store/            # memory, file, and SQLite persistence
  telemetry/        # Prometheus-style metrics collector
  worker/           # lease-based async worker processing
pkg/
  utils/            # ID helpers and workflow loader
examples/           # sample workflow JSON/YAML definitions
docs/               # API scaffold / reference files
tests/              # executor, graph, scheduler, and loader tests
.github/workflows/  # CI automation
TESTING.md          # detailed unit, manual, and architecture scenarios
```

---

## Build & run

Requires Go 1.25+.

```powershell
# Build the project
go build ./...
```

Run a workflow once locally:

```powershell
go run ./cmd/orchestrator -mode run -workflow examples/sample_workflow.json -data-dir data
```

Run it on a recurring interval:

```powershell
go run ./cmd/orchestrator -mode schedule -workflow examples/sample_workflow.json -data-dir data -interval 10s -run-for 1m -allow-overlap=false -scheduler-lock-file data/leader.lock -schedule-store-file data/schedules.json
```

Run it from cron-like scheduling:

```powershell
go run ./cmd/orchestrator -mode schedule -workflow examples/sample_workflow.yaml -data-dir data -cron "@every 15s" -run-for 1m -run-on-startup=true
```

Start the HTTP API locally:

```powershell
go run ./cmd/orchestrator -mode serve -workflow examples/sample_workflow.json -data-dir data -api-addr :8080 -api-token my-token
```

Run a dedicated worker against a durable SQLite queue/store:

```powershell
go run ./cmd/orchestrator -mode worker -workflow examples/sample_workflow.json -store-backend sqlite -sqlite-path data/orchestrator.db -worker-id worker-1 -worker-poll 500ms -worker-lease 30s
```

---

## Testing

The project ships with an automated test suite plus repeatable manual scenarios
for the behaviors that are awkward to fully assert in a single unit test
(scheduler coordination, async worker flows, bulk queue operations, and API
integration paths).

### Run the automated suite

```powershell
# Everything
go test ./...

# Everything with the race detector (recommended)
go test -race ./...

# With coverage across all packages
go test -cover ./...

# A single package
go test -race ./internal/api/

# A single test, verbose
go test -race -run TestServerRunIdempotencyKey -v ./internal/api/

# HTML coverage report for one package
go test -coverprofile=cover.out ./internal/store/
go tool cover -html=cover.out
```

Static checks used in CI-style runs:

```powershell
gofmt -l .
go test ./...
go test ./internal/api -run TestDistributedAsyncLifecycleSmoke -count=1
```

### Coverage snapshot

Coverage varies as the code evolves, but test coverage is intentionally spread
across these areas:

| Package | Primary coverage focus | Package | Primary coverage focus |
| --- | --- | --- | --- |
| `tests` | executor, graph validation, scheduler, workflow loading | `internal/api` | auth, idempotency, pagination, job/schedule endpoints |
| `internal/store` | SQLite lifecycle, dead-letter, replay metadata | `internal/worker` | drain, cancellation, lease-driven execution |
| `internal/queue` | memory queue lease/retry/requeue semantics | `cmd/orchestrator` | startup diagnostics, port probing, fallback behavior |

`cmd/orchestrator` coverage is intentionally narrower than the core packages:
most of the remaining logic is flag parsing, mode selection, and process wiring,
which is better validated by manual and integration-style scenarios.

### How the tests are written

- **Executor-driven assertions.** Workflow tests use deterministic fake actions
  to assert retries, dependency ordering, input interpolation, fail-fast logic,
  and compensation behavior without depending on external services.
- **Real HTTP handlers.** API tests run against the real `httptest` server stack,
  so auth, pagination, bulk endpoints, idempotency, and schedule/job lifecycles
  are exercised through actual request/response behavior.
- **Short-lived worker/scheduler loops.** Worker and scheduler tests use temp
  files, time-bounded contexts, and short polling intervals to verify drain,
  leader locking, and cancellation without long-running harnesses.
- **Durable store lifecycle coverage.** SQLite tests verify queue/job state
  transitions directly against the store implementation that production async
  mode uses.

### Scenario guides

The repository includes a dedicated scenario guide for manual and system-level
validation:

- `TESTING.md` — unit test map, end-to-end commands, architecture scenarios,
  API smoke tests, async worker flows, compensation cases, and troubleshooting.
- `docs/openapi.yaml` — current API scaffold for endpoint evolution.

---

## Running the platform in different modes

This project does not ship a multi-node Docker cluster like a distributed cache;
instead, it exposes four operational modes that compose into a distributed-ish
workflow platform:

| Mode | Responsibility |
| --- | --- |
| `run` | One-shot workflow execution and JSON run output |
| `schedule` | Local recurring execution with interval/cron triggers |
| `serve` | Long-running API process for workflows, runs, schedules, and queue control |
| `worker` | Separate process for leasing and executing async jobs from a shared queue |

Example async setup:

```powershell
# Terminal 1: API + scheduler + durable state
go run ./cmd/orchestrator -mode serve -workflow examples/sample_workflow.json -store-backend sqlite -sqlite-path data/orchestrator.db -api-addr :8080 -api-token my-token

# Terminal 2: Worker process
go run ./cmd/orchestrator -mode worker -workflow examples/sample_workflow.json -store-backend sqlite -sqlite-path data/orchestrator.db -worker-id worker-1 -worker-poll 500ms -worker-lease 30s

# Terminal 3: Submit async run
curl -X POST http://localhost:8080/runs -H "Authorization: Bearer my-token" -H "Content-Type: application/json" -d '{"workflowId":"wf-order-pipeline","async":true,"maxAttempts":3}'
```

---

## Server flags

| Flag | Default | Description |
| --- | --- | --- |
| `-mode` | `run` | `run`, `schedule`, `serve`, or `worker` |
| `-workflow` | `examples/sample_workflow.json` | Path to workflow JSON or YAML |
| `-store-backend` | `file` | Store backend: `file` or `sqlite` |
| `-data-dir` | `data` | File-store data directory |
| `-sqlite-path` | `data/orchestrator.db` | SQLite database path |
| `-api-addr` | `:8080` | HTTP API listen address in serve mode |
| `-api-auto-port` | `false` | Probe the next free port if `-api-addr` is unavailable |
| `-api-auto-port-max-offset` | `20` | Maximum port increments to probe |
| `-api-token` | _(none)_ | Legacy full-access API token |
| `-api-read-token` | _(none)_ | Read-only API token |
| `-api-write-token` | _(none)_ | Write API token |
| `-interval` | `15s` | Schedule interval |
| `-cron` | _(none)_ | Cron expression for schedule mode |
| `-run-on-startup` | `true` | Trigger one run immediately when schedule starts |
| `-allow-overlap` | `false` | Allow concurrent scheduled runs |
| `-scheduler-lock-file` | _(none)_ | Optional single-leader scheduler lock file |
| `-schedule-store-file` | _(none)_ | Optional persisted scheduler registrations |
| `-worker-id` | _(none)_ | Worker identity for job leasing |
| `-worker-poll` | `500ms` | Queue poll interval |
| `-worker-lease` | `30s` | Queue lease duration |
| `-worker-drain-timeout` | `10s` | Grace period for worker shutdown |

---

## Command reference

**Workflow management:** `GET /workflows`, `GET /workflows/{id}`, `POST /workflows`, `PUT /workflows/{id}`, `PATCH /workflows/{id}`  
**Runs:** `POST /runs`, `GET /runs`, `GET /runs/{id}`, `DELETE /runs/{id}`  
**Async jobs:** `GET /jobs`, `GET /jobs/{id}`, `GET /jobs/stats`, `GET /jobs/history/{id}`, `GET /jobs/dead-letter`, `DELETE /jobs/{id}`, `POST /jobs/cancel`, `POST /jobs/requeue/{id}`, `POST /jobs/requeue`, `POST /jobs/purge`  
**Schedules:** `GET /schedules`, `POST /schedules`, `DELETE /schedules/{workflowId}`, `POST /schedules/pause/{workflowId}`, `POST /schedules/resume/{workflowId}`  
**Health & metrics:** `GET /health`, `GET /ready`, `GET /metrics`

---

## Workflow definition model

Single workflow:

```json
{
  "id": "wf-order-pipeline",
  "name": "Order Processing Pipeline",
  "version": "1.0.0",
  "maxConcurrency": 3,
  "failFast": false,
  "compensateOnFailure": true,
  "timeout": "2m",
  "tasks": [
    {
      "id": "validate-order",
      "action": "print",
      "input": { "message": "Order validated" }
    },
    {
      "id": "reserve-inventory",
      "action": "randomFail",
      "dependsOn": ["validate-order"],
      "condition": "on_success:validate-order",
      "retryPolicy": { "maxAttempts": 3, "backoffBase": "200ms" }
    }
  ]
}
```

Supported model features include dependency graphs, conditional branches,
template interpolation, retries, timeouts, task delays, compensation steps,
and workflow-level fail-fast / compensation policy.

---

## Persistence & async coordination

With the default `file` backend, workflows and runs are persisted under `data/`
and async jobs live in an in-memory queue inside the process. With
`-store-backend sqlite`, workflows, runs, queue jobs, and job transition
history are all stored in a single SQLite database, allowing a `serve` process
and one or more `worker` processes to coordinate through shared durable state.

The async job model uses leased execution rather than blind polling. Jobs move
through `QUEUED`, `LEASED`, `SUCCEEDED`, `FAILED`, `DEAD_LETTER`, and
`CANCELLED`, with replay metadata captured when dead-letter jobs are requeued.

## Monitoring

The API exposes Prometheus-compatible metrics at `/metrics`, plus health and
readiness endpoints at `/health` and `/ready`. Metrics are backed by the
telemetry collector used by the executor and HTTP layer, and are intended for
Prometheus scraping or lightweight local inspection.

---

## Design notes

- **Validated DAG execution.** Workflows are checked for invalid dependencies,
  cycles, and invalid compensation configuration before execution begins.
- **Dataflow in the workflow definition.** Input interpolation using upstream
  task results keeps task coordination declarative rather than hard-coded in Go.
- **Single execution core, multiple entrypoints.** CLI, scheduler, API, and
  worker modes all route work through the same executor semantics.
- **Lease-based async processing.** SQLite-backed async mode allows multiple
  worker processes to coordinate safely without double-executing the same job.
- **Backward-compatible API lists.** Collection endpoints return arrays by
  default and only wrap results in metadata envelopes when explicitly requested.
- **Developer-friendly startup behavior.** API port probing and descriptive bind
  errors make local Windows development less brittle.
# Workflow Scheduler

A production-style workflow orchestration platform written in Go. It executes
JSON or YAML workflow definitions as dependency-aware DAGs, exposes an HTTP API
for workflow and run management, supports scheduled and async execution,
persists state to file or SQLite, and includes queue-backed worker processing,
compensation flows, scoped API auth, and Prometheus-style metrics.

---

## Features

| Area | What's implemented |
| --- | --- |
| Core execution | DAG-aware workflow execution with dependency ordering, bounded parallelism, per-task outputs, and run-level status aggregation |
| Workflow model | JSON/YAML workflow loading, named tasks, task input maps, workflow/task timeouts, and versioned workflow metadata |
| Resilience | Per-task retry policies with backoff, `failFast`, optional `allowFailure`, conditional execution, `runAfter` delays |
| Compensation | Saga-style compensation on workflow failure with reverse-completion execution order |
| Scheduling | Interval and cron schedules, optional startup run, overlap control, runtime registration through API |
| Persistence | In-memory queue, file-backed workflow/run store, and durable SQLite store with queue support |
| Async execution | Queue-backed async run submission, lease-based worker processing, replay, cancellation, dead-letter handling, and retention cleanup |
| API | Workflow CRUD, run submission/query/cancel, schedule management, queue inspection, bulk cancel/requeue/purge, pagination, filters, envelope responses |
| Security | Optional admin/read/write token scopes via `Authorization: Bearer` or `X-API-Token` |
| Observability | `/health`, `/ready`, `/metrics`, run/job statistics, queue history, and OpenAPI scaffold |

---

## Architecture

```mermaid
flowchart TB
    subgraph clients[Clients]
        cli[CLI mode]
        apiClient[HTTP clients / automation]
        workerProc[Worker process]
    end

    subgraph node[Workflow Scheduler Node]
        loader[Workflow loader JSON/YAML] --> exec[Executor]
        exec --> graph[DAG validation + dependency resolution]
        exec --> store[(WorkflowStore)]
        exec --> metrics[Telemetry collector]
        exec --> queue[(RunQueue)]
        api[HTTP API server] --> store
        api --> exec
        api --> sched[Scheduler service]
        api --> queue
        sched --> exec
        sched --> queue
        worker[Worker service] --> queue
        worker --> exec
    end

    cli --> loader
    apiClient --> api
    workerProc --> worker
    store --> file[(File snapshot)]
    store --> sqlite[(SQLite store)]
    metrics --> prom[/Prometheus-compatible metrics/]
```

The execution engine is centered around a validated workflow DAG. Tasks become
ready only when dependency and condition checks pass, then execute with retry,
timeout, and compensation semantics. The same workflow model can be triggered in
four modes: one-shot CLI execution, scheduler-driven recurring execution,
HTTP-triggered synchronous or async runs, and queue-backed worker processing.

---

## Project layout

```text
cmd/
  orchestrator/     # CLI entrypoint: run, schedule, serve, worker
internal/
  actions/          # built-in actions and registry
  api/              # HTTP server, auth, filters, pagination, bulk endpoints
  domain/           # workflow, run, schedule, queue, and interface models
  engine/           # DAG validation and executor
  queue/            # in-memory run queue implementation
  scheduler/        # recurring workflow scheduling service
  store/            # file store, memory store, SQLite store
  telemetry/        # metrics collector and tracing hooks
  worker/           # leased async job processor
pkg/
  utils/            # IDs and workflow loader utilities
examples/           # sample JSON and YAML workflows
docs/
  openapi.yaml      # API scaffold
tests/              # workflow-level executor/scheduler/loader tests
.github/workflows/  # CI pipeline
TESTING.md          # full test scenarios and manual validation guide
```

---

## Build & run

Requires Go 1.25+.

```powershell
# Build the orchestrator
go build ./...
```

Run a single workflow once:

```powershell
go run ./cmd/orchestrator -mode run -workflow examples/sample_workflow.json -data-dir data
```

Run on a schedule:

```powershell
go run ./cmd/orchestrator -mode schedule `
  -workflow examples/sample_workflow.json `
  -data-dir data `
  -interval 10s `
  -run-for 1m `
  -allow-overlap=false `
  -scheduler-lock-file data/leader.lock `
  -schedule-store-file data/schedules.json
```

Run the HTTP service:

```powershell
go run ./cmd/orchestrator -mode serve `
  -workflow examples/sample_workflow.json `
  -data-dir data `
  -api-addr :8080 `
  -api-token my-token
```

Run a dedicated async worker against SQLite-backed state:

```powershell
go run ./cmd/orchestrator -mode worker `
  -workflow examples/sample_workflow.json `
  -store-backend sqlite `
  -sqlite-path data/orchestrator.db `
  -worker-id worker-1 `
  -worker-poll 500ms `
  -worker-lease 30s
```

---

## Operating modes

| Mode | Purpose |
| --- | --- |
| `run` | Execute one workflow immediately and print the full run record as JSON |
| `schedule` | Register a schedule, execute repeatedly for a fixed window, then print summary output |
| `serve` | Start the HTTP API, scheduler runtime, and optional retention cleanup loop |
| `worker` | Poll the queue, lease async jobs, execute them, and update job lifecycle state |

---

## Example workflow capabilities

The sample workflows exercise the richer parts of the model:

- Dependency ordering with `dependsOn`
- Conditional routing with `on_success`, `on_failed`, and `output_contains`
- Input templating such as `${taskId.output}` and `${taskId.status}`
- Delayed task start with `runAfter`
- Retries via `retryPolicy.maxAttempts` and `retryPolicy.backoffBase`
- Compensation steps triggered when `compensateOnFailure` is enabled

Example excerpt:

```json
{
  "id": "wf-order-pipeline",
  "maxConcurrency": 3,
  "compensateOnFailure": true,
  "tasks": [
    {
      "id": "validate-order",
      "action": "print",
      "input": { "message": "Order validated" }
    },
    {
      "id": "reserve-inventory",
      "action": "randomFail",
      "dependsOn": ["validate-order"],
      "condition": "on_success:validate-order",
      "retryPolicy": { "maxAttempts": 3, "backoffBase": "200ms" }
    }
  ]
}
```

---

## Testing

The project includes automated tests across executor, graph validation,
scheduler, queue, SQLite persistence, API behavior, worker lifecycle, CLI error
handling, and workflow loading.

### Run the automated suite

```powershell
# Full suite
go test ./...

# Recommended concurrency safety check
go test -race ./...

# Verbose output
go test ./... -v

# Focus a package
go test ./internal/api -v
go test ./internal/store -v
go test ./internal/worker -v
go test ./tests -v
```

Static checks used in CI:

```powershell
gofmt -l .
go test ./...
go test ./internal/api -run TestDistributedAsyncLifecycleSmoke -count=1
```

### How the tests are written

- Executor tests use custom fake actions to deterministically assert retries,
  ordering, conditions, fail-fast behavior, and compensation.
- API tests use `httptest` against the real handler stack, including auth,
  pagination, idempotency, filtering, and bulk operations.
- Queue and SQLite tests validate full job lifecycle transitions: queued,
  leased, retried, succeeded, dead-lettered, requeued, and cancelled.
- Worker and scheduler tests use short-lived contexts and temp files to verify
  leader-lock lifecycle, graceful drain, and async cancellation behavior.

### Scenario guides

- `TESTING.md` contains the full end-to-end test matrix, manual scenarios, and
  architecture-level validation steps.
- `docs/openapi.yaml` contains the current API scaffold.

---

## HTTP API

Start the service:

```powershell
go run ./cmd/orchestrator -mode serve `
  -workflow examples/sample_workflow.json `
  -store-backend sqlite `
  -sqlite-path data/orchestrator.db `
  -api-addr :8080 `
  -api-read-token read-token `
  -api-write-token write-token
```

Core endpoint groups:

| Area | Endpoints |
| --- | --- |
| Health | `GET /health`, `GET /ready`, `GET /metrics` |
| Workflows | `GET /workflows`, `GET /workflows/{id}`, `POST /workflows`, `PUT /workflows/{id}`, `PATCH /workflows/{id}` |
| Runs | `GET /runs`, `GET /runs/{id}`, `POST /runs`, `DELETE /runs/{id}` |
| Schedules | `GET /schedules`, `POST /schedules`, `DELETE /schedules/{workflowId}`, `POST /schedules/pause/{workflowId}`, `POST /schedules/resume/{workflowId}` |
| Jobs | `GET /jobs`, `GET /jobs/stats`, `GET /jobs/{id}`, `GET /jobs/history/{id}`, `GET /jobs/dead-letter`, `DELETE /jobs/{id}`, `POST /jobs/cancel`, `POST /jobs/requeue/{id}`, `POST /jobs/requeue`, `POST /jobs/purge` |

Example run submission:

```powershell
curl -X POST http://localhost:8080/runs `
  -H "Authorization: Bearer write-token" `
  -H "Content-Type: application/json" `
  -d '{"workflowId":"wf-order-pipeline","async":true,"maxAttempts":5}'
```

The API supports:

- Idempotency via `Idempotency-Key` or `X-Idempotency-Key`
- Filtering on runs and jobs by workflow, status, request ID, and time range
- Pagination via `limit`, `offset`, and `order`
- Envelope responses via `envelope=true` or `X-List-Envelope: true`
- Bulk cancel, replay, and purge result codes with structured failure details

---

## Persistence and async execution

### File-backed mode

- Default `file` backend persists workflows and runs under `data/`
- Async jobs use the in-memory queue in this mode
- Good for local development and single-process usage

### SQLite-backed mode

- `-store-backend sqlite -sqlite-path data/orchestrator.db`
- Uses one durable store for workflows, runs, jobs, and transition history
- Enables multi-process async execution with API and worker processes sharing
  the same database

### Async job lifecycle

Jobs move through these states:

`QUEUED -> LEASED -> SUCCEEDED`

Failure branches:

`LEASED -> FAILED -> DEAD_LETTER`

Operator-driven flows:

`DEAD_LETTER -> QUEUED` via replay, and `QUEUED/LEASED -> CANCELLED` via run or job cancellation.

---

## Scheduling and coordination

- Interval and cron-based registrations
- Optional `runOnStartup`
- Optional non-overlap protection with `allowOverlap=false`
- Optional leader lock file for a single active scheduler coordinator
- Optional persisted schedule registrations via `-schedule-store-file`
- With queue-backed execution enabled, schedules enqueue jobs instead of running
  inline, which allows worker-based fan-out

---

## Security and auth

Auth is optional. When enabled, the server accepts either one admin token or
split read/write/admin scopes.

| Token type | Access |
| --- | --- |
| Admin | Read and write endpoints |
| Read | `GET` endpoints only |
| Write | Read plus mutating endpoints |

Flags:

| Flag | Description |
| --- | --- |
| `-api-token` | Legacy full-access token |
| `-api-read-token` | Read-only token |
| `-api-write-token` | Write token |

Environment variables:

```powershell
set ORCHESTRATOR_API_TOKEN=my-token
set ORCHESTRATOR_API_READ_TOKEN=read-token
set ORCHESTRATOR_API_WRITE_TOKEN=write-token
```

---

## Key CLI flags

| Flag | Default | Description |
| --- | --- | --- |
| `-mode` | `run` | `run`, `schedule`, `serve`, or `worker` |
| `-workflow` | `examples/sample_workflow.json` | Workflow JSON or YAML path |
| `-store-backend` | `file` | `file` or `sqlite` |
| `-data-dir` | `data` | File-store directory |
| `-sqlite-path` | `data/orchestrator.db` | SQLite DB path |
| `-api-addr` | `:8080` | HTTP listen address in serve mode |
| `-api-auto-port` | `false` | Probe next ports when `-api-addr` is unavailable |
| `-interval` | `15s` | Scheduler interval |
| `-cron` | empty | Cron expression for schedule mode |
| `-run-for` | `1m` | Lifetime of the schedule-mode process |
| `-allow-overlap` | `false` | Permit concurrent scheduled runs |
| `-scheduler-lock-file` | empty | Single-leader coordination lock file |
| `-schedule-store-file` | empty | Persist schedule registrations |
| `-worker-id` | empty | Worker identity |
| `-worker-poll` | `500ms` | Queue poll interval |
| `-worker-lease` | `30s` | Job lease duration |
| `-worker-drain-timeout` | `10s` | Graceful drain timeout |

---

## Design notes

- The execution engine validates workflows before running them, including cycle
  detection and compensation configuration checks.
- Task inputs can reference upstream outputs and statuses, which keeps dataflow
  in the workflow definition rather than hard-coded in actions.
- Compensation runs in reverse completion order, which matches typical saga
  rollback semantics.
- Queue-backed async execution uses leasing so multiple workers can share the
  same durable job store safely.
- API list endpoints keep backward-compatible array responses by default and
  add metadata envelopes only when explicitly requested.
- Startup port probing improves Windows developer ergonomics when a preferred
  port is already bound.