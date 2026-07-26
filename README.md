# Workflow Orchestrator (Go)

A production-style workflow orchestrator built with object-oriented design principles in Go.

## What You Asked
- End-to-end implementation
- Basic to advanced features
- Proper folder structure
- OOP principles

This project delivers all of the above with layered architecture, interfaces, polymorphic actions, and clear separation of concerns.

## Implementation Plan (Basic -> Advanced)

### Phase 1: Foundation (Basic)
- Define domain models: Workflow, TaskNode, Run, TaskResult
- Define interfaces: Action, ActionRegistry, WorkflowStore
- Build workflow validation with dependency checks

### Phase 2: Core Orchestration (Intermediate)
- DAG-aware execution engine
- Dependency resolution and topological readiness
- Parallel execution with MaxConcurrency
- Run-state aggregation and structured results

### Phase 3: Resilience (Advanced)
- Retry policy per task (max attempts + exponential backoff)
- Task-level and workflow-level timeouts
- Failure propagation and skip-on-upstream-failure behavior
- Optional allow-failure per task
- Workflow-level fail-fast mode for critical pipelines

### Phase 4: Operability (Advanced)
- Pluggable action registry (strategy pattern)
- Persistent store: memory + file snapshot
- Scheduler service for interval-based repeated execution
- Optional non-overlapping scheduled runs
- Cron-based scheduling support
- CLI modes: one-shot run and scheduled mode

### Phase 5: Platform Interface (Advanced)
- HTTP API for workflow management and run control
- Runtime schedule registration through API
- Runtime schedule list and unschedule support
- Serve mode for long-running orchestration service
- Token-based API authentication
- Scoped read/write/admin API tokens
- Prometheus-compatible metrics endpoint
- Run duration histogram metrics
- API pagination and ordering on list endpoints
- Idempotent run submission using request keys

### Phase 6: Dynamic Execution (Advanced)
- Condition-based task execution (on_success, on_failed, output_contains)
- Input template interpolation from upstream task results
- Task start delay via runAfter

### Phase 7: Recovery (Advanced)
- Compensation execution for successful prior tasks when workflow fails
- Compensation order is reverse completion order

### Phase 8: Quality
- Unit tests for graph validation and execution behavior

### Phase 9: Distributed Execution (Advanced)
- Run queue abstraction for async/distributed orchestration
- In-memory queue adapter for single-node deployments
- Durable SQLite-backed store+queue adapter
- Dedicated worker mode for leased queue processing
- Scheduler queue handoff for multi-process safe execution
- Async API run submission and queue job inspection endpoints

## Folder Structure

```text
Workflow Scheduler/
  cmd/
    orchestrator/
      main.go
  internal/
    actions/
      registry.go
    api/
      server.go
    domain/
      interfaces.go
      types.go
    engine/
      executor.go
      graph.go
    queue/
      memory_queue.go
    scheduler/
      service.go
    store/
      file_store.go
      memory_store.go
      sqlite_store.go
    telemetry/
      collector.go
    worker/
      service.go
  pkg/
    utils/
      id.go
      workflow_loader.go
  examples/
    sample_workflow.json
  tests/
    executor_test.go
    graph_test.go
  go.mod
  README.md
```

## OOP Principles Applied
- Encapsulation: state and behavior grouped in types like Executor, Service, Registry
- Abstraction: ActionRegistry and WorkflowStore interfaces hide implementation details
- Polymorphism: multiple Action implementations (print, wait, randomFail)
- Composition over inheritance: services are composed from interfaces and concrete collaborators
- Single Responsibility: each package has one clear concern

## How to Run

1) Install Go 1.22+
2) From project root:

```bash
go test ./...
go run ./cmd/orchestrator -mode run -workflow examples/sample_workflow.json -data-dir data
```

Scheduled mode:

```bash
go run ./cmd/orchestrator -mode schedule -workflow examples/sample_workflow.json -data-dir data -interval 10s -run-for 1m -allow-overlap=false -scheduler-lock-file data/leader.lock -schedule-store-file data/schedules.json
```

Cron schedule mode:

```bash
go run ./cmd/orchestrator -mode schedule -workflow examples/sample_workflow.yaml -data-dir data -cron "@every 15s" -run-for 1m -run-on-startup=true
```

Serve mode (HTTP API):

```bash
go run ./cmd/orchestrator -mode serve -workflow examples/sample_workflow.json -data-dir data -api-addr :8080 -api-token my-token
```

Serve mode troubleshooting:
- If startup fails with "address is already in use" or Windows bind conflicts, use a different port with -api-addr (for example :8092) or stop the process already bound to that port.
- Optional auto-port fallback:
  - Set -api-auto-port=true to probe the next available port when -api-addr is unavailable.
  - Set -api-auto-port-max-offset (default 20) to control the probe range.

Serve mode with durable SQLite backend:

```bash
go run ./cmd/orchestrator -mode serve -workflow examples/sample_workflow.json -store-backend sqlite -sqlite-path data/orchestrator.db -api-addr :8080 -api-token my-token
```

Worker mode (processes queued runs):

```bash
go run ./cmd/orchestrator -mode worker -workflow examples/sample_workflow.json -store-backend sqlite -sqlite-path data/orchestrator.db -worker-id worker-1 -worker-poll 500ms -worker-lease 30s
```

Worker graceful drain options:

```bash
go run ./cmd/orchestrator -mode worker -workflow examples/sample_workflow.json -store-backend sqlite -sqlite-path data/orchestrator.db -worker-drain-timeout 15s
```

Scoped token mode (recommended):

```bash
go run ./cmd/orchestrator -mode serve -workflow examples/sample_workflow.json -data-dir data -api-addr :8080 -api-read-token read-token -api-write-token write-token
```

You can also set token from environment:

```bash
set ORCHESTRATOR_API_TOKEN=my-token
set ORCHESTRATOR_API_READ_TOKEN=read-token
set ORCHESTRATOR_API_WRITE_TOKEN=write-token
```

API endpoints:
- GET /health
- GET /ready
- GET /metrics
- GET /workflows
- GET /workflows/{id}
- PUT /workflows/{id}
- PATCH /workflows/{id}
- POST /workflows
- GET /runs?workflowId={id}
- GET /runs/{id}
- DELETE /runs/{id}?reason={text}
- POST /runs
- GET /jobs
- GET /jobs/stats
- GET /jobs/{id}
- GET /jobs/history/{id}
- DELETE /jobs/{id}?reason={text}
- POST /jobs/cancel
- POST /jobs/requeue/{id}
- POST /jobs/requeue
- GET /jobs/dead-letter
- POST /jobs/purge
- GET /schedules
- POST /schedules
- DELETE /schedules/{workflowId}
- POST /schedules/pause/{workflowId}
- POST /schedules/resume/{workflowId}

List endpoint query options:
- limit: page size (default 50, max 1000)
- offset: start offset
- order: asc or desc
- envelope: optional bool (true/false). When true, list endpoints return
  metadata envelope with items, total, limit, offset, returned.
- X-List-Envelope: true header also enables envelope mode.

List response behavior:
- Default: list endpoints return arrays for backward compatibility.
- Envelope mode: set `envelope=true` (or `X-List-Envelope: true`) to receive:
  - items
  - total
  - limit
  - offset
  - returned

Auth headers for protected endpoints:
- Authorization: Bearer my-token
- X-API-Token: my-token

Token scope behavior:
- read token: GET endpoints only
- write token: GET + write endpoints
- admin token: GET + write endpoints

Idempotent run submission:
- POST /runs with header Idempotency-Key or X-Idempotency-Key
- Same workflowId + key returns the same run record on retries

Async run submission:
- POST /runs with body field async=true to enqueue
- Optional maxAttempts controls job retry cap (default 5)
- Jobs can be inspected via GET /jobs, GET /jobs/{id}, and GET /jobs/stats
- GET /jobs supports filters: status, workflowId, requestId, limit, offset
- GET /jobs supports time filters: updatedAfter, updatedBefore (RFC3339)
- GET /jobs/dead-letter supports time filters: updatedAfter, updatedBefore (RFC3339)
- GET /runs supports filters: workflowId, status, requestId, limit, offset, order
- GET /runs supports time filters: startedAfter, startedBefore (RFC3339)
- status filters are strictly validated and return validation errors on unknown values
- Jobs that exhaust retries are moved to DEAD_LETTER
- QUEUED and LEASED jobs can be cancelled with DELETE /jobs/{id} or POST /jobs/cancel
- Active async runs can be cancelled with DELETE /runs/{id}, which cancels related queue jobs
- Job lifecycle transitions can be inspected with GET /jobs/history/{id}
- Job history supports filters: toStatus, actor, createdAfter, createdBefore, limit, offset
- Job history default response is an array (backward compatible)
- Job history envelope metadata is available with `envelope=true`
- Bulk cancel supports filters by status, workflowId, requestId, or explicit jobIds
- DEAD_LETTER and FAILED jobs can be replayed with POST /jobs/requeue/{id}
- Bulk replay supports POST /jobs/requeue with status filter or explicit jobIds
- Replay audit metadata supports replayedBy and replayReason on replay endpoints
- Queue retention cleanup supports POST /jobs/purge with status, olderThan, and limit
- Purge supports dry-run mode via body field dryRun=true or query parameter dryRun=true
- Bulk cancel/requeue responses include resultCode and failedDetails in addition to failed
- Purge responses include resultCode (for limit reached / dry-run limit reached)
- With scheduler + queue configured, schedule triggers enqueue jobs instead of local execution
- Scheduler registrations can persist across restarts with -schedule-store-file

List endpoints supporting envelope mode:
- GET /workflows
- GET /runs
- GET /jobs
- GET /jobs/dead-letter
- GET /schedules

Bulk result codes:
- BULK_OK: all requested operations succeeded
- BULK_PARTIAL_FAILURE: mix of successes and failures
- BULK_ALL_FAILED: all requested operations failed
- PURGE_LIMIT_REACHED: purge hit requested limit
- PURGE_DRY_RUN_LIMIT_REACHED: dry-run matched rows reached requested limit

Bulk failure detail codes:
- JOB_NOT_FOUND: target job does not exist
- INVALID_JOB_STATE: job state does not allow requested operation
- JOB_OPERATION_FAILED: unexpected operation failure

OpenAPI scaffold:
- API scaffold spec: docs/openapi.yaml
- The spec is intentionally minimal and designed to be expanded as endpoints evolve.

Example bulk cancel response (partial failure):

```json
{
  "cancelled": [
    {
      "id": "job-1",
      "status": "CANCELLED"
    }
  ],
  "resultCode": "BULK_PARTIAL_FAILURE",
  "failed": {
    "job-2": "run job cannot be cancelled in current state"
  },
  "failedDetails": {
    "job-2": {
      "code": "INVALID_JOB_STATE",
      "message": "run job cannot be cancelled in current state"
    }
  }
}
```

Example bulk requeue response (partial failure):

```json
{
  "requeued": [
    {
      "id": "job-9",
      "status": "QUEUED"
    }
  ],
  "resultCode": "BULK_PARTIAL_FAILURE",
  "failed": {
    "job-10": "only failed or dead-letter jobs can be requeued"
  },
  "failedDetails": {
    "job-10": {
      "code": "INVALID_JOB_STATE",
      "message": "only failed or dead-letter jobs can be requeued"
    }
  }
}
```

Example purge dry-run response:

```json
{
  "deleted": 0,
  "dryRun": true,
  "matched": 25,
  "resultCode": "PURGE_DRY_RUN_LIMIT_REACHED"
}
```

Example purge execution response:

```json
{
  "deleted": 100,
  "resultCode": "PURGE_LIMIT_REACHED"
}
```

Workflow update semantics:
- PUT /workflows/{id} replaces the workflow definition
- PATCH /workflows/{id} applies partial updates
- Duration fields accept human-readable values like 10ms, 2s, 1m

Validation errors:
- Validation failures return a structured payload:
  - error: validation failed
  - details: list of field + message entries

Example run trigger:

```bash
curl -X POST http://localhost:8080/runs -H "Content-Type: application/json" -d '{"workflowId":"wf-order-pipeline"}'
```

## Workflow JSON Notes
- Use duration strings like 200ms, 2s, 1m
- retryPolicy controls attempts and backoff
- dependsOn forms DAG edges
- allowFailure allows downstream progress even if task fails
- failFast stops the pipeline after the first critical failure
- condition supports: always, on_success:<taskId>, on_failed:<taskId>, output_contains:<taskId>:<text>
- runAfter delays task start after dependencies are satisfied
- templates in input values support: ${taskId.output}, ${taskId.status}, ${taskId.error}
- YAML files are supported in addition to JSON (use .yaml or .yml)
- compensateOnFailure enables rollback-style compensation when a run fails
- task compensation block supports action, input, and timeout
- metrics include run duration histogram via orchestrator_run_duration_seconds_bucket
- scheduler-lock-file allows only one process instance to hold active scheduler leadership

## Extension Ideas
- Cron expressions instead of fixed interval
- HTTP API + dashboard
- Distributed queue-backed workers
- Dead-letter handling and compensating transactions
- Metrics/trace export (Prometheus/OpenTelemetry)
