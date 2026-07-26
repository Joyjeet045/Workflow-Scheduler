# Workflow Scheduler

A workflow orchestration platform written in Go. It executes JSON and YAML
workflow definitions as dependency-aware DAGs, exposes an HTTP API for
workflow and run management, supports scheduled and async execution, persists
state to file or SQLite, and includes queue-backed worker processing,
compensation flows, scoped API auth, and Prometheus-compatible metrics.

---

## Features

| Area | What's implemented |
| --- | --- |
| Core orchestration | DAG-aware task execution with `dependsOn`, bounded parallelism via `maxConcurrency`, structured task results, and run-level status aggregation |
| Workflow model | JSON/YAML workflow loading, task input maps, workflow metadata, task timeouts, and workflow-level timeouts |
| Resilience | Per-task retry policies, `failFast`, optional `allowFailure`, and task start delays with `runAfter` |
| Conditional routing | `on_success`, `on_failed`, and `output_contains` conditions for branch-style execution |
| Input templating | Upstream result interpolation such as `${task.output}` and `${task.status}` in downstream task input |
| Compensation | Reverse-order compensation actions when `compensateOnFailure` is enabled |
| Scheduling | Interval schedules, cron schedules, `runOnStartup`, overlap control, leader lock file, and persisted schedule registrations |
| Persistence | File-backed workflow/run snapshots and durable SQLite storage for workflows, runs, jobs, and transition history |
| Async execution | Queue-backed async run submission, lease-based worker processing, cancellation, dead-letter handling, replay, and retention cleanup |
| API platform | Workflow CRUD, run submission/query/cancel, schedule APIs, job inspection/history, bulk cancel/requeue/purge, pagination, ordering, filters, and envelope responses |
| Security | Optional admin/read/write API token scopes over `Authorization: Bearer` or `X-API-Token` |
| Monitoring | `/metrics`, `/health`, `/ready`, job stats, and run visibility endpoints |

---

## Architecture

The system is organized around one execution core and several entrypoints.

| Component | Responsibility |
| --- | --- |
| Workflow loader | Loads workflow definitions from JSON or YAML files |
| Executor | Validates DAGs, resolves dependencies, evaluates conditions, runs tasks, applies retries, and records results |
| Workflow store | Persists workflows and run records using either file storage or SQLite |
| Run queue | Holds async jobs for worker-based execution |
| HTTP API | Exposes workflow, run, schedule, and job management endpoints |
| Scheduler | Triggers workflows from interval or cron registrations |
| Worker | Leases async jobs from the queue and executes them through the same executor |
| Telemetry collector | Exposes Prometheus-compatible metrics and run statistics |

Execution flow:

1. A workflow is loaded and saved into the configured store.
2. Work enters the system through `run`, `schedule`, `serve`, or `worker` mode.
3. The executor validates the workflow graph before running any task.
4. Ready tasks are executed with timeout, retry, condition, and compensation rules.
5. Results are persisted, metrics are updated, and async jobs are transitioned through their lifecycle states.

SQLite-backed mode adds a shared durable state boundary so API and worker
processes can coordinate async execution across multiple processes.

---

## Project layout

```text
cmd/
  orchestrator/     # main binary: run, schedule, serve, worker
internal/
  actions/          # built-in actions and registry wiring
  api/              # HTTP API, auth, filtering, pagination, bulk operations
  domain/           # workflows, runs, jobs, schedules, and interfaces
  engine/           # DAG validation and executor
  queue/            # in-memory run queue
  scheduler/        # interval/cron scheduling service
  store/            # memory, file, and SQLite persistence
  telemetry/        # metrics collector
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

## Build and run

Requires Go 1.25+.

```powershell
go build ./...
```

Run a workflow once:

```powershell
go run ./cmd/orchestrator -mode run -workflow examples/sample_workflow.json -data-dir data
```

Run on an interval:

```powershell
go run ./cmd/orchestrator -mode schedule -workflow examples/sample_workflow.json -data-dir data -interval 10s -run-for 1m -allow-overlap=false -scheduler-lock-file data/leader.lock -schedule-store-file data/schedules.json
```

Run from a cron expression:

```powershell
go run ./cmd/orchestrator -mode schedule -workflow examples/sample_workflow.yaml -data-dir data -cron "@every 15s" -run-for 1m -run-on-startup=true
```

Start the HTTP API:

```powershell
go run ./cmd/orchestrator -mode serve -workflow examples/sample_workflow.json -data-dir data -api-addr :8080 -api-token my-token
```

Run a worker against SQLite-backed state:

```powershell
go run ./cmd/orchestrator -mode worker -workflow examples/sample_workflow.json -store-backend sqlite -sqlite-path data/orchestrator.db -worker-id worker-1 -worker-poll 500ms -worker-lease 30s
```

---

## Testing

The project includes automated tests plus repeatable manual scenarios for
scheduler coordination, async worker flows, bulk queue operations, and API
integration paths.

### Run the automated suite

```powershell
go test ./...
go test -race ./...
go test -cover ./...
go test -race ./internal/api/
go test -race -run TestServerRunIdempotencyKey -v ./internal/api/
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

| Package | Primary coverage focus | Package | Primary coverage focus |
| --- | --- | --- | --- |
| `tests` | executor, graph validation, scheduler, workflow loading | `internal/api` | auth, idempotency, pagination, job and schedule endpoints |
| `internal/store` | SQLite lifecycle, dead-letter, replay metadata | `internal/worker` | drain, cancellation, lease-driven execution |
| `internal/queue` | memory queue lease, retry, and requeue semantics | `cmd/orchestrator` | startup diagnostics, port probing, fallback behavior |

### How the tests are written

- Workflow tests use deterministic fake actions to assert retries,
  dependency ordering, input interpolation, fail-fast logic, and compensation.
- API tests use the real `httptest` handler stack for auth, pagination,
  idempotency, filtering, schedules, and bulk operations.
- Worker and scheduler tests use temp files, short polling intervals, and
  time-bounded contexts to validate drain, leader locking, and cancellation.
- SQLite tests validate queue and job lifecycle transitions against the same
  store implementation used by async mode.

### Scenario guides

- `TESTING.md` contains unit, end-to-end, and architecture scenarios.
- `docs/openapi.yaml` contains the current API scaffold.

---

## Running the platform in different modes

| Mode | Responsibility |
| --- | --- |
| `run` | One-shot workflow execution and JSON run output |
| `schedule` | Local recurring execution with interval or cron triggers |
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
**Health and metrics:** `GET /health`, `GET /ready`, `GET /metrics`

---

## Workflow definition model

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
and workflow-level fail-fast and compensation policy.

---

## Persistence and async coordination

With the default `file` backend, workflows and runs are persisted under `data/`
and async jobs live in an in-memory queue inside the process. With
`-store-backend sqlite`, workflows, runs, queue jobs, and job transition
history are all stored in a single SQLite database, allowing a `serve` process
and one or more `worker` processes to coordinate through shared durable state.

The async job model uses leased execution rather than blind polling. Jobs move
through `QUEUED`, `LEASED`, `SUCCEEDED`, `FAILED`, `DEAD_LETTER`, and
`CANCELLED`, with replay metadata captured when dead-letter jobs are requeued.

---

## Monitoring

The API exposes Prometheus-compatible metrics at `/metrics`, plus health and
readiness endpoints at `/health` and `/ready`. Metrics are backed by the
telemetry collector used by the executor and HTTP layer.

---

## Design notes

- Workflows are validated before execution, including cycle detection and
  compensation configuration checks.
- Input interpolation keeps task coordination declarative inside the workflow
  definition instead of hard-coded in Go.
- CLI, scheduler, API, and worker modes all route work through the same
  executor semantics.
- SQLite-backed async mode allows multiple worker processes to coordinate
  safely without double-executing the same job.
- API list endpoints return arrays by default and only return metadata
  envelopes when explicitly requested.
- API port probing and descriptive bind errors make local development less
  brittle when a preferred port is already in use.