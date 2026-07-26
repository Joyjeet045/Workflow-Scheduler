# Testing Guide — Workflow Scheduler

This document is a complete testing reference for the Workflow Scheduler project.
It covers every layer of the system: unit tests, integration tests, manual API
smoke-tests, architecture scenarios, and performance considerations.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Running the Full Test Suite](#running-the-full-test-suite)
3. [Unit Test Inventory](#unit-test-inventory)
   - [Graph Validation](#1-graph-validation)
   - [Executor Engine](#2-executor-engine)
   - [Scheduler Service](#3-scheduler-service)
   - [Worker Service](#4-worker-service)
   - [Memory Queue](#5-memory-queue)
   - [SQLite Store](#6-sqlite-store)
   - [API Server](#7-api-server)
   - [CLI Startup](#8-cli-startup)
   - [Workflow Loader](#9-workflow-loader)
4. [Manual / End-to-End Scenarios](#manual--end-to-end-scenarios)
   - [Scenario A — One-Shot Run](#scenario-a--one-shot-run)
   - [Scenario B — Interval Scheduler](#scenario-b--interval-scheduler)
   - [Scenario C — Cron Scheduler](#scenario-c--cron-scheduler)
   - [Scenario D — HTTP API (Serve Mode)](#scenario-d--http-api-serve-mode)
   - [Scenario E — SQLite Backend + Worker](#scenario-e--sqlite-backend--worker)
   - [Scenario F — Scoped Token Auth](#scenario-f--scoped-token-auth)
   - [Scenario G — Async Run + Queue Inspection](#scenario-g--async-run--queue-inspection)
   - [Scenario H — Run Cancellation](#scenario-h--run-cancellation)
   - [Scenario I — Idempotent Run Submission](#scenario-i--idempotent-run-submission)
   - [Scenario J — Pagination and Envelope Mode](#scenario-j--pagination-and-envelope-mode)
   - [Scenario K — Compensation (Saga Pattern)](#scenario-k--compensation-saga-pattern)
   - [Scenario L — Fail-Fast Pipeline](#scenario-l--fail-fast-pipeline)
   - [Scenario M — Metrics Endpoint](#scenario-m--metrics-endpoint)
   - [Scenario N — Port Conflict & Auto-Fallback](#scenario-n--port-conflict--auto-fallback)
5. [Architecture-Level Test Scenarios](#architecture-level-test-scenarios)
   - [DAG Engine](#dag-engine)
   - [Retry + Backoff](#retry--backoff)
   - [Condition-Based Routing](#condition-based-routing)
   - [Input Template Interpolation](#input-template-interpolation)
   - [Distributed Worker / Lease Model](#distributed-worker--lease-model)
   - [Dead-Letter and Requeue](#dead-letter-and-requeue)
   - [Leader Lock (Single-Node Non-Overlap)](#leader-lock-single-node-non-overlap)
6. [Test Coverage Report](#test-coverage-report)
7. [Race Detector](#race-detector)
8. [Tips and Troubleshooting](#tips-and-troubleshooting)

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Go   | 1.22+   | Build and test |
| curl | any     | Manual API smoke tests |
| SQLite | (bundled via `modernc.org/sqlite`) | No external install needed |

Verify Go is installed:

```powershell
go version
```

All commands below assume you are in the repository root:

```powershell
cd "path\to\Workflow Scheduler"
```

---

## Running the Full Test Suite

```powershell
# Run every test in every package
go test ./...

# Verbose output (see each test name pass/fail)
go test ./... -v

# With race detector (catches data races)
go test -race ./...

# Run a single package
go test ./internal/api -v
go test ./tests -v
go test ./internal/store -v
go test ./internal/worker -v
go test ./internal/queue -v
go test ./cmd/orchestrator -v
```

Expected: all tests `PASS` with no `FAIL` or `DATA RACE` lines.

---

## Unit Test Inventory

### 1. Graph Validation

**Package:** `tests/`  **File:** `graph_test.go`

| Test | What it verifies |
|------|-----------------|
| `TestValidateWorkflowDetectsCycle` | A→B→C→A cycle is rejected |
| `TestValidateWorkflowHappyPath` | Linear A→B DAG is accepted |
| `TestValidateWorkflowCompensationInputWithoutAction` | Compensation with `input` but no `action` is rejected |

Run:
```powershell
go test ./tests -run TestValidate -v
```

---

### 2. Executor Engine

**Package:** `tests/`  **File:** `executor_test.go`

| Test | What it verifies |
|------|-----------------|
| `TestExecutorRetriesAndSucceeds` | Task failing twice then succeeding on attempt 3 |
| `TestExecutorDependencyOrdering` | Task B runs after A when `dependsOn` is set |
| `TestExecutorResolvesInputTemplatesAndConditions` | `${seed.output}` interpolation; `on_success`/`on_failed` conditions |
| `TestExecutorFailFastStopsPipeline` | Root failure + `failFast:true` skips all children |
| `TestExecutorRunsCompensationOnFailure` | Saga compensation runs in reverse order on failure |

Run:
```powershell
go test ./tests -run TestExecutor -v
```

---

### 3. Scheduler Service

**Package:** `tests/`  **File:** `scheduler_test.go`

| Test | What it verifies |
|------|-----------------|
| `TestScheduleRegisterValidation` | Empty config, missing trigger, conflicting trigger all fail |
| `TestScheduleRegisterValidCron` | Valid cron expression `@every 1m` is accepted |
| `TestScheduleListAndUnregister` | Register two schedules, unregister one, only one remains |
| `TestSchedulerLeaderLockLifecycle` | Lock file is created on `Start`, removed on cancel |

Run:
```powershell
go test ./tests -run TestSchedule -v
```

---

### 4. Worker Service

**Package:** `internal/worker/`  **File:** `service_test.go`

| Test | What it verifies |
|------|-----------------|
| `TestWorkerDrainStopsLeasing` | `Drain()` causes the worker loop to exit cleanly |
| `TestWorkerCancelsQueueJobWhenRunCancelled` | Workflow timeout causes the job to be marked `CANCELLED` |
| `TestWorkerHonorsExternalJobCancellation` | External `DELETE /jobs/{id}` cancels an active lease |

Run:
```powershell
go test ./internal/worker -v
```

---

### 5. Memory Queue

**Package:** `internal/queue/`  **File:** `memory_queue_test.go`

| Test | What it verifies |
|------|-----------------|
| `TestMemoryRunQueueLeaseRetryAndComplete` | Enqueue → lease → extend lease → retry → re-lease → complete full lifecycle |
| `TestMemoryRunQueueDeadLetterAndRequeue` | Job exhausts retries → `DEAD_LETTER` → requeue resets to `QUEUED` |

Run:
```powershell
go test ./internal/queue -v
```

---

### 6. SQLite Store

**Package:** `internal/store/`  **File:** `sqlite_store_test.go`

| Test | What it verifies |
|------|-----------------|
| `TestSQLiteStoreRunQueueLifecycle` | Enqueue → lease → extend → complete → lookup by request ID |
| `TestSQLiteStoreDeadLetterAndRequeue` | Dead-letter transition + requeue with audit metadata (`replayCount`, `replayedBy`) |

Run:
```powershell
go test ./internal/store -v
```

---

### 7. API Server

**Package:** `internal/api/`  **Files:** `server_test.go`, `server_golden_test.go`

| Test | What it verifies |
|------|-----------------|
| `TestServerAuthMiddleware` | No token → 401; read token on GET → 200; read token on POST → 401; write token on POST → 201 |
| `TestServerMetricsEndpoint` | `/metrics` returns `orchestrator_runs_total` |
| `TestServerHealthRichPayloadAndReady` | `/health` JSON has `status:ok` and `authEnabled:true`; `/ready` returns 200 |
| `TestServerScheduleListAndDelete` | Create → list → delete schedule lifecycle |
| `TestServerScheduleListEnvelope` | `?envelope=true` returns `{items, total, returned}` |
| `TestServerRunIdempotencyKey` | Same `Idempotency-Key` header returns same run ID on second request |
| `TestServerRunFilters` | `?workflowId=&status=&requestId=` filters correctly |
| `TestServerPaginationOnWorkflows` | `?limit=1&offset=1&order=asc` returns correct page |

Run:
```powershell
go test ./internal/api -v
```

---

### 8. CLI Startup

**Package:** `cmd/orchestrator/`  **File:** `main_test.go`

| Test | What it verifies |
|------|-----------------|
| `TestAPIServerStartErrorPortInUseHint` | Port-in-use error surfaces `already in use` + `-api-addr` hint |
| `TestAPIServerStartErrorPermissionHint` | Privileged port error surfaces `insufficient permission` hint |
| `TestAPIServerStartErrorFallback` | Generic error falls back to raw message |
| `TestCheckAPIAddrAvailableDetectsConflict` | Preflight check detects an already-bound address |
| `TestResolveAPIAddrWithCheckNoFallbackWhenAvailable` | No fallback attempted when port is free |
| `TestResolveAPIAddrWithCheckFallbackEnabled` | Auto-fallback probes +1, +2 until a free port is found |
| `TestResolveAPIAddrWithCheckFallbackDisabled` | Returns error immediately when fallback is disabled |

Run:
```powershell
go test ./cmd/orchestrator -v
```

---

### 9. Workflow Loader

**Package:** `tests/`  **File:** `workflow_loader_test.go`

| Test | What it verifies |
|------|-----------------|
| `TestLoadWorkflowFromYAML` | YAML file with all fields (failFast, compensateOnFailure, conditions, compensation) loads correctly |

Run:
```powershell
go test ./tests -run TestLoad -v
```

---

## Manual / End-to-End Scenarios

### Scenario A — One-Shot Run

Validates: executor, DAG resolution, file store persistence.

```powershell
go run ./cmd/orchestrator -mode run `
  -workflow examples/sample_workflow.json `
  -data-dir data
```

**Expected output:** Each task printed in dependency order. Final status `SUCCESS`.  
**Check:** `data/snapshot.json` is updated with the run record.

---

### Scenario B — Interval Scheduler

Validates: scheduler, non-overlapping execution, leader lock.

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

**Expected:** Workflow runs every 10 s for 1 minute (≈6 runs). The lock file
`data/leader.lock` is present during execution and removed when the process exits.

---

### Scenario C — Cron Scheduler

Validates: cron trigger, `--run-on-startup` flag.

```powershell
go run ./cmd/orchestrator -mode schedule `
  -workflow examples/sample_workflow.yaml `
  -data-dir data `
  -cron "@every 15s" `
  -run-for 1m `
  -run-on-startup=true
```

**Expected:** An immediate run fires at startup, then every 15 s.

---

### Scenario D — HTTP API (Serve Mode)

Validates: HTTP server, token auth, workflow CRUD, run submission.

```powershell
# Start the server
go run ./cmd/orchestrator -mode serve `
  -workflow examples/sample_workflow.json `
  -data-dir data `
  -api-addr :8080 `
  -api-token my-token
```

In a second terminal:

```powershell
# Health check (no auth required)
curl http://localhost:8080/health

# List workflows (auth required)
curl -H "Authorization: Bearer my-token" http://localhost:8080/workflows

# Submit a synchronous run
curl -X POST http://localhost:8080/runs `
  -H "Authorization: Bearer my-token" `
  -H "Content-Type: application/json" `
  -d '{"workflowId":"wf-order-pipeline"}'

# Get run result (replace RUN_ID)
curl -H "Authorization: Bearer my-token" http://localhost:8080/runs/RUN_ID
```

**Expected:** workflow list returns the loaded workflow; run returns `status:SUCCESS`.

---

### Scenario E — SQLite Backend + Worker

Validates: durable store, async queue, worker lease model.

**Terminal 1 — API server with SQLite:**
```powershell
go run ./cmd/orchestrator -mode serve `
  -workflow examples/sample_workflow.json `
  -store-backend sqlite `
  -sqlite-path data/orchestrator.db `
  -api-addr :8080 `
  -api-token my-token
```

**Terminal 2 — Worker:**
```powershell
go run ./cmd/orchestrator -mode worker `
  -workflow examples/sample_workflow.json `
  -store-backend sqlite `
  -sqlite-path data/orchestrator.db `
  -worker-id worker-1 `
  -worker-poll 500ms `
  -worker-lease 30s
```

**Terminal 3 — Enqueue an async run:**
```powershell
curl -X POST http://localhost:8080/runs `
  -H "Authorization: Bearer my-token" `
  -H "Content-Type: application/json" `
  -d '{"workflowId":"wf-order-pipeline","async":true,"maxAttempts":3}'
```

**Inspect the queue:**
```powershell
curl -H "Authorization: Bearer my-token" http://localhost:8080/jobs
curl -H "Authorization: Bearer my-token" http://localhost:8080/jobs/stats
```

**Expected:** Job appears as `QUEUED`, the worker picks it up (`LEASED`), then
marks it `SUCCEEDED`. The run record updates to `SUCCESS`.

---

### Scenario F — Scoped Token Auth

Validates: read-only vs write-only token enforcement.

```powershell
go run ./cmd/orchestrator -mode serve `
  -workflow examples/sample_workflow.json `
  -data-dir data `
  -api-addr :8080 `
  -api-read-token read-token `
  -api-write-token write-token
```

```powershell
# Read with read-token — should 200
curl -H "Authorization: Bearer read-token" http://localhost:8080/workflows

# Write with read-token — should 401
curl -X POST http://localhost:8080/runs `
  -H "Authorization: Bearer read-token" `
  -H "Content-Type: application/json" `
  -d '{"workflowId":"wf-order-pipeline"}'

# Write with write-token — should 201
curl -X POST http://localhost:8080/runs `
  -H "Authorization: Bearer write-token" `
  -H "Content-Type: application/json" `
  -d '{"workflowId":"wf-order-pipeline"}'
```

---

### Scenario G — Async Run + Queue Inspection

Validates: async dispatch, job status transitions, dead-letter, requeue.

```powershell
# Submit async run with low maxAttempts to force dead-letter quickly
curl -X POST http://localhost:8080/runs `
  -H "Authorization: Bearer my-token" `
  -H "Content-Type: application/json" `
  -d '{"workflowId":"wf-order-pipeline","async":true,"maxAttempts":1}'

# Check jobs
curl -H "Authorization: Bearer my-token" "http://localhost:8080/jobs?status=DEAD_LETTER"

# Requeue a dead-letter job (replace JOB_ID)
curl -X POST http://localhost:8080/jobs/requeue/JOB_ID `
  -H "Authorization: Bearer my-token"

# History for a specific job
curl -H "Authorization: Bearer my-token" http://localhost:8080/jobs/history/JOB_ID
```

---

### Scenario H — Run Cancellation

Validates: cancel in-flight runs, related job cancellation.

```powershell
# Submit a long-running async workflow (use a wait task)
curl -X POST http://localhost:8080/runs `
  -H "Authorization: Bearer my-token" `
  -H "Content-Type: application/json" `
  -d '{"workflowId":"wf-order-pipeline","async":true}'

# Cancel by run ID while LEASED
curl -X DELETE "http://localhost:8080/runs/RUN_ID?reason=manual-test" `
  -H "Authorization: Bearer my-token"

# Cancel by job ID directly
curl -X DELETE "http://localhost:8080/jobs/JOB_ID?reason=manual-test" `
  -H "Authorization: Bearer my-token"

# Bulk cancel all jobs in QUEUED status
curl -X POST http://localhost:8080/jobs/cancel `
  -H "Authorization: Bearer my-token" `
  -H "Content-Type: application/json" `
  -d '{"status":"QUEUED"}'
```

**Expected:** Run transitions to `CANCELLED`; related queue job becomes `CANCELLED`.

---

### Scenario I — Idempotent Run Submission

Validates: exactly-once run semantics for retried clients.

```powershell
# First submission — 201 Created
curl -X POST http://localhost:8080/runs `
  -H "Authorization: Bearer my-token" `
  -H "Content-Type: application/json" `
  -H "Idempotency-Key: order-request-42" `
  -d '{"workflowId":"wf-order-pipeline"}'

# Duplicate submission — 200 OK with same run ID
curl -X POST http://localhost:8080/runs `
  -H "Authorization: Bearer my-token" `
  -H "Content-Type: application/json" `
  -H "Idempotency-Key: order-request-42" `
  -d '{"workflowId":"wf-order-pipeline"}'
```

**Expected:** Both responses return the same `id` field.

---

### Scenario J — Pagination and Envelope Mode

Validates: `limit`, `offset`, `order`, `envelope` query parameters.

```powershell
# Register several workflows first (or use the API)
curl -H "Authorization: Bearer my-token" `
  "http://localhost:8080/workflows?limit=2&offset=0&order=asc"

# Envelope mode — returns metadata wrapper
curl -H "Authorization: Bearer my-token" `
  "http://localhost:8080/workflows?limit=2&offset=0&envelope=true"

# Header-based envelope
curl -H "Authorization: Bearer my-token" `
     -H "X-List-Envelope: true" `
  "http://localhost:8080/runs?limit=5"
```

**Envelope response shape:**
```json
{
  "items": [...],
  "total": 10,
  "limit": 2,
  "offset": 0,
  "returned": 2
}
```

---

### Scenario K — Compensation (Saga Pattern)

Validates: reverse-order compensation when workflow fails mid-way.

`examples/sample_workflow.json` has `"compensateOnFailure": true` and a
`randomFail` task for `reserve-inventory`. Run it repeatedly until a failure
triggers compensation:

```powershell
go run ./cmd/orchestrator -mode run `
  -workflow examples/sample_workflow.json `
  -data-dir data
```

**Expected on failure run:** Console output shows compensation tasks executing
in reverse completion order (e.g., `reserve-inventory` compensation fires after
`charge` fails).

---

### Scenario L — Fail-Fast Pipeline

Validates: `failFast:true` aborts the remaining DAG on first failure.

Create `data/failfast_workflow.json`:

```json
{
  "id": "wf-failfast",
  "maxConcurrency": 3,
  "failFast": true,
  "tasks": [
    { "id": "root", "action": "randomFail", "input": { "failPercent": "100" } },
    { "id": "child-a", "action": "print", "dependsOn": ["root"], "input": { "message": "A" } },
    { "id": "child-b", "action": "print", "dependsOn": ["root"], "input": { "message": "B" } }
  ]
}
```

```powershell
go run ./cmd/orchestrator -mode run -workflow data/failfast_workflow.json -data-dir data
```

**Expected:** `root` fails, `child-a` and `child-b` are `SKIPPED`. Final run
status `FAILED`.

---

### Scenario M — Metrics Endpoint

Validates: Prometheus-compatible metrics exposition.

```powershell
curl http://localhost:8080/metrics
```

**Expected response** (partial):
```
# HELP orchestrator_runs_total Total workflow runs executed
# TYPE orchestrator_runs_total counter
orchestrator_runs_total ...
orchestrator_run_duration_seconds_bucket ...
```

---

### Scenario N — Port Conflict & Auto-Fallback

Validates: helpful error messages and auto-port probing.

```powershell
# Start an unrelated listener on 8080 to create a conflict
$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Any, 8080)
$listener.Start()

# Try to start on 8080 — expect clear error with -api-addr hint
go run ./cmd/orchestrator -mode serve `
  -workflow examples/sample_workflow.json `
  -data-dir data `
  -api-addr :8080 `
  -api-token my-token

# Use auto-port fallback — will bind to 8081 (or next available)
go run ./cmd/orchestrator -mode serve `
  -workflow examples/sample_workflow.json `
  -data-dir data `
  -api-addr :8080 `
  -api-auto-port=true `
  -api-auto-port-max-offset 5 `
  -api-token my-token

$listener.Stop()
```

---

## Architecture-Level Test Scenarios

### DAG Engine

- **Topological sort:** tasks run in correct dependency order even with diamond
  dependencies (`A→B`, `A→C`, `B→D`, `C→D` — D runs last).
- **Parallel slots:** set `maxConcurrency:2` and observe that two independent
  tasks start simultaneously.
- **Cycle detection:** submit a workflow with `A dependsOn B, B dependsOn A` —
  `ValidateWorkflow` returns an error before any execution.

### Retry + Backoff

- Set `maxAttempts:3` and `backoffBase:200ms`. A `randomFail` task with
  `failPercent:100` exhausts all 3 attempts and the task status becomes
  `FAILED`.
- Set `maxAttempts:3` and `failPercent:50`. Observe attempt count in the run
  result (`attempts` field) varies between 1 and 3 across multiple runs.

### Condition-Based Routing

| Condition | Trigger |
|-----------|---------|
| `on_success:<taskId>` | Task executes only when the upstream task succeeded |
| `on_failed:<taskId>` | Task executes only when the upstream task failed |
| `output_contains:<taskId>:<substr>` | Task executes only when upstream output contains the substring |

Test by mixing `on_success` and `on_failed` tasks on the same upstream; only
one branch should execute per run.

### Input Template Interpolation

Template variables resolved at execution time:

| Expression | Resolves to |
|------------|-------------|
| `${taskId.output}` | Output string of the named task |
| `${taskId.status}` | Status string (`SUCCESS`, `FAILED`, …) |

Test by setting downstream task input to `"message": "result=${upstream.output}"` and
verifying the rendered output.

### Distributed Worker / Lease Model

1. Start two worker processes against the same SQLite database with different
   `worker-id` values.
2. Enqueue multiple async jobs via the API.
3. Observe jobs distributed across both workers (check `leaseOwner` in
   `GET /jobs`).
4. Kill one worker mid-execution; verify the lease expires and the surviving
   worker re-leases and retries the job (up to `maxAttempts`).

```powershell
# Worker 1
go run ./cmd/orchestrator -mode worker -workflow examples/sample_workflow.json `
  -store-backend sqlite -sqlite-path data/orchestrator.db `
  -worker-id worker-1 -worker-poll 300ms -worker-lease 10s

# Worker 2 (separate terminal)
go run ./cmd/orchestrator -mode worker -workflow examples/sample_workflow.json `
  -store-backend sqlite -sqlite-path data/orchestrator.db `
  -worker-id worker-2 -worker-poll 300ms -worker-lease 10s
```

### Dead-Letter and Requeue

1. Submit an async run with `maxAttempts:1` targeting a workflow with a
   `randomFail` task at 100% failure.
2. Worker picks up the job, fails it, and moves it to `DEAD_LETTER`.
3. Verify via `GET /jobs/dead-letter`.
4. Requeue with `POST /jobs/requeue/JOB_ID`.
5. Verify the job re-appears in `QUEUED` with `replayCount:1`.

### Leader Lock (Single-Node Non-Overlap)

Start two scheduler processes pointing at the same `--scheduler-lock-file`.
Only one should acquire the lock and run workflows; the other should log that
it could not acquire leadership and remain idle. Kill the leader; the follower
should acquire the lock and start running within one tick.

---

## Test Coverage Report

```powershell
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out

# Optional: open HTML report in browser
go tool cover -html=coverage.out -o coverage.html
start coverage.html
```

Key packages and their primary coverage targets:

| Package | Focus |
|---------|-------|
| `internal/engine` | Graph validation, DAG executor, retry, compensation |
| `internal/api` | Auth, CRUD endpoints, pagination, idempotency |
| `internal/scheduler` | Interval + cron triggers, leader lock |
| `internal/worker` | Lease lifecycle, drain, cancellation |
| `internal/queue` | Memory queue transitions |
| `internal/store` | SQLite job/run persistence |
| `pkg/utils` | YAML/JSON workflow loader |

---

## Race Detector

The project uses goroutines for parallel task execution, worker polling, and
scheduler ticks. Always run the race detector before merging:

```powershell
go test -race ./...
```

Packages most susceptible to races: `internal/engine`, `internal/worker`,
`internal/queue`, `internal/scheduler`.

---

## Tips and Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `address already in use` on `:8080` | Previous run still listening | Use `-api-addr :8092` or `-api-auto-port=true` |
| `401 Unauthorized` on every request | Wrong or missing token | Pass `-H "Authorization: Bearer <token>"` |
| Worker never picks up jobs | Worker points to different SQLite path | Ensure `--sqlite-path` matches the server |
| Compensation not running | `compensateOnFailure` not set in workflow JSON | Add `"compensateOnFailure": true` |
| YAML workflow fails to load | Incorrect indentation | Validate with `go test ./tests -run TestLoad -v` |
| Tests fail on Windows with path errors | Backslash in paths | Use forward slashes or `filepath.Join` — the loader handles both |
| `DATA RACE` in test output | Concurrent map/slice access | Run `go test -race ./...` and check the reported goroutines |

---

*Generated for Workflow Scheduler v1 — Go 1.22+ / July 2026*
