package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"workflowscheduler/internal/actions"
	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/engine"
	"workflowscheduler/internal/queue"
	"workflowscheduler/internal/scheduler"
	"workflowscheduler/internal/store"
	"workflowscheduler/internal/telemetry"
	"workflowscheduler/internal/worker"
)

func TestServerAuthMiddleware(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)

	srv := NewServer(":0", "read:read123,write:write123", memStore, nil, executor, nil, telemetry.NewCollector())

	unauthReq := httptest.NewRequest(http.MethodGet, "/workflows", nil)
	unauthRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(unauthRes, unauthReq)
	if unauthRes.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized, got %d", unauthRes.Code)
	}

	readReq := httptest.NewRequest(http.MethodGet, "/workflows", nil)
	readReq.Header.Set("Authorization", "Bearer read123")
	readRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(readRes, readReq)
	if readRes.Code != http.StatusOK {
		t.Fatalf("expected read OK, got %d", readRes.Code)
	}

	body := strings.NewReader(`{"id":"wf","tasks":[{"id":"a","action":"print"}]}`)
	writeDeniedReq := httptest.NewRequest(http.MethodPost, "/workflows", body)
	writeDeniedReq.Header.Set("Authorization", "Bearer read123")
	writeDeniedRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(writeDeniedRes, writeDeniedReq)
	if writeDeniedRes.Code != http.StatusUnauthorized {
		t.Fatalf("expected write unauthorized for read token, got %d", writeDeniedRes.Code)
	}

	body2 := strings.NewReader(`{"id":"wf2","tasks":[{"id":"a","action":"print"}]}`)
	writeReq := httptest.NewRequest(http.MethodPost, "/workflows", body2)
	writeReq.Header.Set("Authorization", "Bearer write123")
	writeRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(writeRes, writeReq)
	if writeRes.Code != http.StatusCreated {
		t.Fatalf("expected write OK, got %d", writeRes.Code)
	}
}

func TestServerMetricsEndpoint(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	collector := telemetry.NewCollector()
	srv := NewServer(":0", "", memStore, nil, executor, nil, collector)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}
	if !strings.Contains(res.Body.String(), "orchestrator_runs_total") {
		t.Fatalf("expected metrics payload, got %q", res.Body.String())
	}
}

func TestServerHealthRichPayloadAndReady(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "read:r,write:w", memStore, runQueue, executor, nil, telemetry.NewCollector())

	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(healthRes, healthReq)
	if healthRes.Code != http.StatusOK {
		t.Fatalf("expected health OK, got %d", healthRes.Code)
	}
	var health map[string]any
	if err := json.Unmarshal(healthRes.Body.Bytes(), &health); err != nil {
		t.Fatalf("unmarshal health: %v", err)
	}
	if health["status"] != "ok" {
		t.Fatalf("expected status ok, got %v", health["status"])
	}
	if health["authEnabled"] != true {
		t.Fatalf("expected auth enabled")
	}

	readyReq := httptest.NewRequest(http.MethodGet, "/ready", nil)
	readyRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(readyRes, readyReq)
	if readyRes.Code != http.StatusOK {
		t.Fatalf("expected ready OK, got %d: %s", readyRes.Code, readyRes.Body.String())
	}
}

func TestServerScheduleListAndDelete(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	svc := scheduler.NewService(executor, memStore)
	_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{
		ID: "wf-schedule",
		Tasks: []domain.TaskNode{
			{ID: "a", Action: "print"},
		},
	})

	srv := NewServer(":0", "admin:admin123", memStore, nil, executor, svc, telemetry.NewCollector())

	createReq := httptest.NewRequest(http.MethodPost, "/schedules", strings.NewReader(`{"workflowId":"wf-schedule","interval":"1m"}`))
	createReq.Header.Set("Authorization", "Bearer admin123")
	createRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected schedule created, got %d", createRes.Code)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	listReq.Header.Set("Authorization", "Bearer admin123")
	listRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(listRes, listReq)
	if listRes.Code != http.StatusOK {
		t.Fatalf("expected schedule list OK, got %d", listRes.Code)
	}
	if !strings.Contains(listRes.Body.String(), "wf-schedule") {
		t.Fatalf("expected schedule list to contain workflow")
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/schedules/wf-schedule", nil)
	deleteReq.Header.Set("Authorization", "Bearer admin123")
	deleteRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusOK {
		t.Fatalf("expected schedule delete OK, got %d", deleteRes.Code)
	}
}

func TestServerScheduleListEnvelope(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	svc := scheduler.NewService(executor, memStore)
	_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{ID: "wf-schedule-env", Tasks: []domain.TaskNode{{ID: "a", Action: "print"}}})

	srv := NewServer(":0", "admin:admin123", memStore, nil, executor, svc, telemetry.NewCollector())

	createReq := httptest.NewRequest(http.MethodPost, "/schedules", strings.NewReader(`{"workflowId":"wf-schedule-env","interval":"1m"}`))
	createReq.Header.Set("Authorization", "Bearer admin123")
	createRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected schedule created, got %d", createRes.Code)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/schedules?envelope=true&limit=10&offset=0", nil)
	listReq.Header.Set("Authorization", "Bearer admin123")
	listRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(listRes, listReq)
	if listRes.Code != http.StatusOK {
		t.Fatalf("expected schedule list OK, got %d", listRes.Code)
	}

	var payload struct {
		Items    []domain.ScheduleConfig `json:"items"`
		Total    int                     `json:"total"`
		Returned int                     `json:"returned"`
	}
	if err := json.Unmarshal(listRes.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal schedule envelope: %v", err)
	}
	if payload.Total != 1 || payload.Returned != 1 || len(payload.Items) != 1 || payload.Items[0].WorkflowID != "wf-schedule-env" {
		t.Fatalf("unexpected schedule envelope payload: %+v", payload)
	}
}

func TestServerRunIdempotencyKey(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{
		ID: "wf-idem",
		Tasks: []domain.TaskNode{
			{ID: "a", Action: "print", Input: map[string]string{"message": "ok"}},
		},
	})

	srv := NewServer(":0", "admin:admin123", memStore, nil, executor, nil, telemetry.NewCollector())
	body := `{"workflowId":"wf-idem"}`

	firstReq := httptest.NewRequest(http.MethodPost, "/runs", strings.NewReader(body))
	firstReq.Header.Set("Authorization", "Bearer admin123")
	firstReq.Header.Set("Idempotency-Key", "abc-1")
	firstRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(firstRes, firstReq)
	if firstRes.Code != http.StatusCreated {
		t.Fatalf("expected first create status, got %d", firstRes.Code)
	}

	secondReq := httptest.NewRequest(http.MethodPost, "/runs", strings.NewReader(body))
	secondReq.Header.Set("Authorization", "Bearer admin123")
	secondReq.Header.Set("Idempotency-Key", "abc-1")
	secondRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(secondRes, secondReq)
	if secondRes.Code != http.StatusOK {
		t.Fatalf("expected idempotent replay status OK, got %d", secondRes.Code)
	}

	var firstRun domain.Run
	var secondRun domain.Run
	if err := json.Unmarshal(firstRes.Body.Bytes(), &firstRun); err != nil {
		t.Fatalf("unmarshal first run: %v", err)
	}
	if err := json.Unmarshal(secondRes.Body.Bytes(), &secondRun); err != nil {
		t.Fatalf("unmarshal second run: %v", err)
	}
	if firstRun.ID != secondRun.ID {
		t.Fatalf("expected same run id for idempotent requests")
	}
}

func TestServerRunFilters(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)

	_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{ID: "wf-run-filter", Tasks: []domain.TaskNode{{ID: "a", Action: "print"}}})
	_ = memStore.SaveRun(context.Background(), domain.Run{ID: "run-filter-1", WorkflowID: "wf-run-filter", RequestID: "req-f-1", Status: domain.RunStatusSuccess, StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC()})
	_ = memStore.SaveRun(context.Background(), domain.Run{ID: "run-filter-2", WorkflowID: "wf-run-filter", RequestID: "req-f-2", Status: domain.RunStatusFailed, StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC()})

	srv := NewServer(":0", "admin:admin123", memStore, nil, executor, nil, telemetry.NewCollector())
	req := httptest.NewRequest(http.MethodGet, "/runs?workflowId=wf-run-filter&status=SUCCESS&requestId=req-f-1", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected run list OK, got %d", res.Code)
	}
	var runs []domain.Run
	if err := json.Unmarshal(res.Body.Bytes(), &runs); err != nil {
		t.Fatalf("unmarshal runs: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != "run-filter-1" {
		t.Fatalf("unexpected run filter result: %+v", runs)
	}
}

func TestServerPaginationOnWorkflows(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)

	for _, id := range []string{"wf-a", "wf-b", "wf-c"} {
		_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{
			ID: id,
			Tasks: []domain.TaskNode{
				{ID: "t", Action: "print"},
			},
		})
	}

	srv := NewServer(":0", "admin:admin123", memStore, nil, executor, nil, telemetry.NewCollector())
	req := httptest.NewRequest(http.MethodGet, "/workflows?limit=1&offset=1&order=asc", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}
	var items []domain.Workflow
	if err := json.Unmarshal(res.Body.Bytes(), &items); err != nil {
		t.Fatalf("unmarshal workflows: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one workflow, got %d", len(items))
	}
	if items[0].ID != "wf-b" {
		t.Fatalf("expected wf-b on offset page, got %s", items[0].ID)
	}
}

func TestServerAsyncRunQueueAndIdempotency(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)

	_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{
		ID: "wf-async",
		Tasks: []domain.TaskNode{
			{ID: "a", Action: "print", Input: map[string]string{"message": "queued"}},
		},
	})

	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())
	body := `{"workflowId":"wf-async","async":true}`

	firstReq := httptest.NewRequest(http.MethodPost, "/runs", strings.NewReader(body))
	firstReq.Header.Set("Authorization", "Bearer admin123")
	firstReq.Header.Set("Idempotency-Key", "async-k1")
	firstRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(firstRes, firstReq)
	if firstRes.Code != http.StatusAccepted {
		t.Fatalf("expected accepted for async submission, got %d", firstRes.Code)
	}

	var job domain.RunQueueJob
	if err := json.Unmarshal(firstRes.Body.Bytes(), &job); err != nil {
		t.Fatalf("unmarshal async job: %v", err)
	}
	if job.WorkflowID != "wf-async" || job.Status != domain.RunJobStatusQueued {
		t.Fatalf("unexpected queued job payload: %+v", job)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	listReq.Header.Set("Authorization", "Bearer admin123")
	listRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(listRes, listReq)
	if listRes.Code != http.StatusOK {
		t.Fatalf("expected jobs list OK, got %d", listRes.Code)
	}

	secondReq := httptest.NewRequest(http.MethodPost, "/runs", strings.NewReader(body))
	secondReq.Header.Set("Authorization", "Bearer admin123")
	secondReq.Header.Set("Idempotency-Key", "async-k1")
	secondRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(secondRes, secondReq)
	if secondRes.Code != http.StatusOK {
		t.Fatalf("expected idempotent queued replay status OK, got %d", secondRes.Code)
	}

	var replayJob domain.RunQueueJob
	if err := json.Unmarshal(secondRes.Body.Bytes(), &replayJob); err != nil {
		t.Fatalf("unmarshal replay job: %v", err)
	}
	if replayJob.ID != job.ID {
		t.Fatalf("expected same queued job for idempotent async request")
	}
}

func TestServerRequeueDeadLetterJob(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)

	now := time.Now().UTC()
	job := domain.RunQueueJob{
		ID:          "job-dead-1",
		WorkflowID:  "wf-dead-1",
		RunID:       "run-dead-1",
		Status:      domain.RunJobStatusDead,
		MaxAttempts: 3,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := runQueue.EnqueueRunJob(context.Background(), job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())
	req := httptest.NewRequest(http.MethodPost, "/jobs/requeue/job-dead-1", strings.NewReader(`{"availableAfter":"0s"}`))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}

	var updated domain.RunQueueJob
	if err := json.Unmarshal(res.Body.Bytes(), &updated); err != nil {
		t.Fatalf("unmarshal requeued job: %v", err)
	}
	if updated.Status != domain.RunJobStatusQueued {
		t.Fatalf("expected queued status, got %s", updated.Status)
	}
}

func TestServerDeadLetterJobsEndpoint(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{
		ID:          "job-dead-a",
		WorkflowID:  "wf-1",
		RunID:       "run-1",
		Status:      domain.RunJobStatusDead,
		MaxAttempts: 1,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{
		ID:          "job-ok-b",
		WorkflowID:  "wf-1",
		RunID:       "run-2",
		Status:      domain.RunJobStatusSucceeded,
		MaxAttempts: 1,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	})

	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())
	req := httptest.NewRequest(http.MethodGet, "/jobs/dead-letter", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}
	var jobs []domain.RunQueueJob
	if err := json.Unmarshal(res.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("unmarshal jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-dead-a" {
		t.Fatalf("expected only dead-letter job, got %+v", jobs)
	}
}

func TestServerListEnvelopeMode(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{ID: "wf-env", Tasks: []domain.TaskNode{{ID: "a", Action: "print"}}})
	_ = memStore.SaveRun(context.Background(), domain.Run{ID: "run-env-1", WorkflowID: "wf-env", Status: domain.RunStatusSuccess, StartedAt: now.Add(-time.Minute), FinishedAt: now})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-env-1", WorkflowID: "wf-env", RunID: "run-env-1", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	req := httptest.NewRequest(http.MethodGet, "/runs?workflowId=wf-env&envelope=true&limit=10&offset=0", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}
	var payload struct {
		Items    []domain.Run `json:"items"`
		Total    int          `json:"total"`
		Returned int          `json:"returned"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal envelope payload: %v", err)
	}
	if payload.Total != 1 || payload.Returned != 1 || len(payload.Items) != 1 || payload.Items[0].ID != "run-env-1" {
		t.Fatalf("unexpected envelope payload: %+v", payload)
	}
}

func TestServerCreateWorkflowDurationStrings(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, nil, executor, nil, telemetry.NewCollector())

	body := `{
		"id":"wf-duration-api",
		"name":"Duration API",
		"timeout":"30s",
		"tasks":[
			{
				"id":"t1",
				"action":"wait",
				"input":{"durationMs":"1"},
				"timeout":"2s",
				"runAfter":"10ms",
				"retryPolicy":{"maxAttempts":2,"backoffBase":"15ms"},
				"compensation":{"action":"print","input":{"message":"rollback"},"timeout":"1s"}
			}
		]
	}`

	req := httptest.NewRequest(http.MethodPost, "/workflows", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusCreated {
		t.Fatalf("expected created, got %d body=%s", res.Code, res.Body.String())
	}

	stored, err := memStore.GetWorkflow(context.Background(), "wf-duration-api")
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if stored.Timeout != 30*time.Second {
		t.Fatalf("expected workflow timeout 30s, got %s", stored.Timeout)
	}
	if len(stored.Tasks) != 1 {
		t.Fatalf("expected one task")
	}
	if stored.Tasks[0].RetryPolicy.BackoffBase != 15*time.Millisecond {
		t.Fatalf("expected backoff 15ms, got %s", stored.Tasks[0].RetryPolicy.BackoffBase)
	}
}

func TestServerMetricsIncludesQueueDepth(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-q-1", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-q-2", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	collector := telemetry.NewCollector()
	srv := NewServer(":0", "", memStore, runQueue, executor, nil, collector)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}
	body := res.Body.String()
	if !strings.Contains(body, "orchestrator_queue_depth{status=\"QUEUED\"} 1") {
		t.Fatalf("expected queued depth metric, got %s", body)
	}
	if !strings.Contains(body, "orchestrator_queue_depth{status=\"DEAD_LETTER\"} 1") {
		t.Fatalf("expected dead-letter depth metric, got %s", body)
	}
}

func TestServerValidationErrorPayload(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, nil, executor, nil, telemetry.NewCollector())

	req := httptest.NewRequest(http.MethodPost, "/runs", strings.NewReader(`{"async":true}`))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request, got %d", res.Code)
	}
	if !strings.Contains(res.Body.String(), `"error":"validation failed"`) {
		t.Fatalf("expected validation error envelope, got %s", res.Body.String())
	}
	if !strings.Contains(res.Body.String(), `"field":"workflowId"`) {
		t.Fatalf("expected workflowId field detail, got %s", res.Body.String())
	}
}

func TestServerJobsPurgeEndpoint(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC().Add(-2 * time.Hour)
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-purge-1", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusSucceeded, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-purge-2", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	req := httptest.NewRequest(http.MethodPost, "/jobs/purge", strings.NewReader(`{"status":"SUCCEEDED","olderThan":"1h","limit":50}`))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}
	if _, err := runQueue.GetRunJob(context.Background(), "job-purge-1"); err == nil {
		t.Fatalf("expected succeeded job to be purged")
	}
	if _, err := runQueue.GetRunJob(context.Background(), "job-purge-2"); err != nil {
		t.Fatalf("expected non-matching job to remain")
	}
}

func TestServerJobsPurgeDryRun(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC().Add(-2 * time.Hour)
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-purge-dry-1", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusSucceeded, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	req := httptest.NewRequest(http.MethodPost, "/jobs/purge?dryRun=true", strings.NewReader(`{"status":"SUCCEEDED","olderThan":"1h","limit":10}`))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}
	if _, err := runQueue.GetRunJob(context.Background(), "job-purge-dry-1"); err != nil {
		t.Fatalf("expected dry-run to keep job, got error: %v", err)
	}
	if !strings.Contains(res.Body.String(), `"dryRun":true`) {
		t.Fatalf("expected dryRun true in response: %s", res.Body.String())
	}
}

func TestDistributedAsyncLifecycleSmoke(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	sqlStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = sqlStore.Close() }()

	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, sqlStore)
	collector := telemetry.NewCollector()
	executor.SetMetricsCollector(collector)

	workflow := domain.Workflow{ID: "wf-smoke", Tasks: []domain.TaskNode{{ID: "t1", Action: "print", Input: map[string]string{"message": "ok"}}}}
	if err := sqlStore.SaveWorkflow(context.Background(), workflow); err != nil {
		t.Fatalf("save workflow: %v", err)
	}

	srv := NewServer(":0", "", sqlStore, sqlStore, executor, nil, collector)
	httpSrv := httptest.NewServer(srv.httpSrv.Handler)
	defer httpSrv.Close()

	workerSvc := worker.NewService("smoke-worker", sqlStore, sqlStore, executor)
	workerSvc.SetPolling(10*time.Millisecond, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go workerSvc.Start(ctx)

	resp, err := http.Post(httpSrv.URL+"/runs", "application/json", strings.NewReader(`{"workflowId":"wf-smoke","async":true}`))
	if err != nil {
		t.Fatalf("post async run: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected accepted, got %d", resp.StatusCode)
	}

	var queued domain.RunQueueJob
	if err := json.NewDecoder(resp.Body).Decode(&queued); err != nil {
		t.Fatalf("decode queued job: %v", err)
	}

	var finalJob domain.RunQueueJob
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		jResp, err := http.Get(httpSrv.URL + "/jobs/" + queued.ID)
		if err == nil {
			_ = json.NewDecoder(jResp.Body).Decode(&finalJob)
			jResp.Body.Close()
			if finalJob.Status == domain.RunJobStatusSucceeded {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	if finalJob.Status != domain.RunJobStatusSucceeded {
		t.Fatalf("expected succeeded job, got %s", finalJob.Status)
	}

	runResp, err := http.Get(httpSrv.URL + "/runs/" + queued.RunID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	defer runResp.Body.Close()
	if runResp.StatusCode != http.StatusOK {
		t.Fatalf("expected run record status OK, got %d", runResp.StatusCode)
	}
}

func TestServerCancelJobEndpoint(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-cancel-api-1", WorkflowID: "wf", RunID: "r", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	req := httptest.NewRequest(http.MethodDelete, "/jobs/job-cancel-api-1?reason=manual", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}
	job, _ := runQueue.GetRunJob(context.Background(), "job-cancel-api-1")
	if job.Status != domain.RunJobStatusCancelled {
		t.Fatalf("expected cancelled status, got %s", job.Status)
	}
}

func TestServerBulkCancelAndJobFilters(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-filter-1", WorkflowID: "wf-a", RunID: "r1", RequestID: "req-1", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-filter-2", WorkflowID: "wf-b", RunID: "r2", RequestID: "req-2", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	bulkReq := httptest.NewRequest(http.MethodPost, "/jobs/cancel", strings.NewReader(`{"status":"QUEUED","reason":"maintenance"}`))
	bulkReq.Header.Set("Authorization", "Bearer admin123")
	bulkRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(bulkRes, bulkReq)
	if bulkRes.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", bulkRes.Code, bulkRes.Body.String())
	}

	filterReq := httptest.NewRequest(http.MethodGet, "/jobs?status=CANCELLED&workflowId=wf-a&requestId=req-1", nil)
	filterReq.Header.Set("Authorization", "Bearer admin123")
	filterRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(filterRes, filterReq)
	if filterRes.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", filterRes.Code)
	}
	var jobs []domain.RunQueueJob
	if err := json.Unmarshal(filterRes.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("unmarshal filtered jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-filter-1" {
		t.Fatalf("unexpected filtered jobs: %+v", jobs)
	}
}

func TestServerBulkCancelTypedPartialFailureCodes(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-bulk-ok", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-bulk-no", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusSucceeded, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(time.Millisecond), UpdatedAt: now.Add(time.Millisecond)})

	req := httptest.NewRequest(http.MethodPost, "/jobs/cancel", strings.NewReader(`{"jobIds":["job-bulk-ok","job-bulk-no"],"reason":"maintenance"}`))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}
	var payload struct {
		ResultCode    string                       `json:"resultCode"`
		Failed        map[string]string            `json:"failed"`
		FailedDetails map[string]bulkFailureDetail `json:"failedDetails"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal bulk cancel payload: %v", err)
	}
	if payload.ResultCode != "BULK_PARTIAL_FAILURE" {
		t.Fatalf("expected BULK_PARTIAL_FAILURE, got %s", payload.ResultCode)
	}
	if payload.FailedDetails["job-bulk-no"].Code != "INVALID_JOB_STATE" {
		t.Fatalf("expected INVALID_JOB_STATE code, got %+v", payload.FailedDetails)
	}
}

func TestServerJobsPurgeDryRunResultCode(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC().Add(-2 * time.Hour)
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-purge-code-1", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusSucceeded, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	req := httptest.NewRequest(http.MethodPost, "/jobs/purge?dryRun=true", strings.NewReader(`{"status":"SUCCEEDED","olderThan":"1h","limit":1}`))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), `"resultCode":"PURGE_DRY_RUN_LIMIT_REACHED"`) {
		t.Fatalf("expected dry-run resultCode, got %s", res.Body.String())
	}
}

func TestServerBulkRequeueTypedFailureCodes(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-rq-good", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-rq-bad", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusSucceeded, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(time.Millisecond), UpdatedAt: now.Add(time.Millisecond)})

	req := httptest.NewRequest(http.MethodPost, "/jobs/requeue", strings.NewReader(`{"jobIds":["job-rq-good","job-rq-bad"],"availableAfter":"0s"}`))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}
	var payload struct {
		ResultCode    string                       `json:"resultCode"`
		FailedDetails map[string]bulkFailureDetail `json:"failedDetails"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal bulk requeue payload: %v", err)
	}
	if payload.ResultCode != "BULK_PARTIAL_FAILURE" {
		t.Fatalf("expected BULK_PARTIAL_FAILURE, got %s", payload.ResultCode)
	}
	if payload.FailedDetails["job-rq-bad"].Code != "INVALID_JOB_STATE" {
		t.Fatalf("expected INVALID_JOB_STATE code, got %+v", payload.FailedDetails)
	}
}

func TestServerBulkCancelScopedByWorkflowAndRequest(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-scope-1", WorkflowID: "wf-scope", RunID: "r1", RequestID: "req-scope", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-scope-2", WorkflowID: "wf-scope", RunID: "r2", RequestID: "req-other", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	req := httptest.NewRequest(http.MethodPost, "/jobs/cancel", strings.NewReader(`{"status":"QUEUED","workflowId":"wf-scope","requestId":"req-scope"}`))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}

	job1, _ := runQueue.GetRunJob(context.Background(), "job-scope-1")
	job2, _ := runQueue.GetRunJob(context.Background(), "job-scope-2")
	if job1.Status != domain.RunJobStatusCancelled {
		t.Fatalf("expected job-scope-1 cancelled, got %s", job1.Status)
	}
	if job2.Status != domain.RunJobStatusQueued {
		t.Fatalf("expected job-scope-2 unchanged, got %s", job2.Status)
	}
}

func TestServerJobStatsEndpoint(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-stats-1", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-stats-2", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	req := httptest.NewRequest(http.MethodGet, "/jobs/stats", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}
	if !strings.Contains(res.Body.String(), `"total":2`) {
		t.Fatalf("expected total=2, got %s", res.Body.String())
	}
	if !strings.Contains(res.Body.String(), `"QUEUED":1`) {
		t.Fatalf("expected queued count in stats: %s", res.Body.String())
	}
}

func TestServerRunCancelEndpoint(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = memStore.SaveRun(context.Background(), domain.Run{ID: "run-cancel-api", WorkflowID: "wf", Status: domain.RunStatusRunning, StartedAt: now})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-run-cancel-1", WorkflowID: "wf", RunID: "run-cancel-api", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	req := httptest.NewRequest(http.MethodDelete, "/runs/run-cancel-api?reason=stop-now", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}
	job, _ := runQueue.GetRunJob(context.Background(), "job-run-cancel-1")
	if job.Status != domain.RunJobStatusCancelled {
		t.Fatalf("expected cancelled job status, got %s", job.Status)
	}
	run, _ := memStore.GetRun(context.Background(), "run-cancel-api")
	if run.Status != domain.RunStatusCancelled {
		t.Fatalf("expected cancelled run status, got %s", run.Status)
	}
}

func TestServerJobHistoryEndpoint(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-history-api", WorkflowID: "wf", RunID: "r", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_, _ = runQueue.LeaseNextRunJob(context.Background(), "worker-history", time.Second)
	_ = runQueue.CancelRunJob(context.Background(), "job-history-api", "manual")

	req := httptest.NewRequest(http.MethodGet, "/jobs/history/job-history-api", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}
	var items []domain.RunJobTransition
	if err := json.Unmarshal(res.Body.Bytes(), &items); err != nil {
		t.Fatalf("unmarshal history: %v", err)
	}
	if len(items) < 3 {
		t.Fatalf("expected at least 3 history entries, got %d", len(items))
	}
}

func TestServerRunsAndJobsTimeFilters(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	oldRunTime := now.Add(-2 * time.Hour)
	recentRunTime := now.Add(-5 * time.Minute)
	_ = memStore.SaveRun(context.Background(), domain.Run{ID: "run-old", WorkflowID: "wf", Status: domain.RunStatusSuccess, StartedAt: oldRunTime, FinishedAt: oldRunTime.Add(time.Second)})
	_ = memStore.SaveRun(context.Background(), domain.Run{ID: "run-recent", WorkflowID: "wf", Status: domain.RunStatusSuccess, StartedAt: recentRunTime, FinishedAt: recentRunTime.Add(time.Second)})

	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-old", WorkflowID: "wf", RunID: "run-old", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: oldRunTime, CreatedAt: oldRunTime, UpdatedAt: oldRunTime})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-recent", WorkflowID: "wf", RunID: "run-recent", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: recentRunTime, CreatedAt: recentRunTime, UpdatedAt: recentRunTime})

	runsReq := httptest.NewRequest(http.MethodGet, "/runs?startedAfter="+now.Add(-30*time.Minute).Format(time.RFC3339), nil)
	runsReq.Header.Set("Authorization", "Bearer admin123")
	runsRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(runsRes, runsReq)
	if runsRes.Code != http.StatusOK {
		t.Fatalf("expected runs filter OK, got %d: %s", runsRes.Code, runsRes.Body.String())
	}
	var runs []domain.Run
	if err := json.Unmarshal(runsRes.Body.Bytes(), &runs); err != nil {
		t.Fatalf("unmarshal runs: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != "run-recent" {
		t.Fatalf("unexpected runs filtered result: %+v", runs)
	}

	jobsReq := httptest.NewRequest(http.MethodGet, "/jobs?updatedAfter="+now.Add(-30*time.Minute).Format(time.RFC3339), nil)
	jobsReq.Header.Set("Authorization", "Bearer admin123")
	jobsRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(jobsRes, jobsReq)
	if jobsRes.Code != http.StatusOK {
		t.Fatalf("expected jobs filter OK, got %d: %s", jobsRes.Code, jobsRes.Body.String())
	}
	var jobs []domain.RunQueueJob
	if err := json.Unmarshal(jobsRes.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("unmarshal jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-recent" {
		t.Fatalf("unexpected jobs filtered result: %+v", jobs)
	}
}

func TestServerJobHistoryFiltersAndPagination(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-history-filter", WorkflowID: "wf", RunID: "r", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_, _ = runQueue.LeaseNextRunJob(context.Background(), "worker-history", time.Second)
	_ = runQueue.CancelRunJob(context.Background(), "job-history-filter", "operator-cancel")

	req := httptest.NewRequest(http.MethodGet, "/jobs/history/job-history-filter?toStatus=CANCELLED&actor=api&limit=1&offset=0&envelope=true", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected history filter OK, got %d: %s", res.Code, res.Body.String())
	}
	var payload struct {
		Items    []domain.RunJobTransition `json:"items"`
		Total    int                       `json:"total"`
		Returned int                       `json:"returned"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal history payload: %v", err)
	}
	if payload.Total < 1 || payload.Returned != 1 || len(payload.Items) != 1 {
		t.Fatalf("unexpected history pagination payload: %+v", payload)
	}
	if payload.Items[0].ToStatus != domain.RunJobStatusCancelled || payload.Items[0].Actor != "api" {
		t.Fatalf("unexpected filtered history item: %+v", payload.Items[0])
	}
}

func TestServerRejectsInvalidStatusFilters(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	jobsReq := httptest.NewRequest(http.MethodGet, "/jobs?status=NOPE", nil)
	jobsReq.Header.Set("Authorization", "Bearer admin123")
	jobsRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(jobsRes, jobsReq)
	if jobsRes.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request for jobs status filter, got %d", jobsRes.Code)
	}

	runsReq := httptest.NewRequest(http.MethodGet, "/runs?status=BOGUS", nil)
	runsReq.Header.Set("Authorization", "Bearer admin123")
	runsRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(runsRes, runsReq)
	if runsRes.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request for runs status filter, got %d", runsRes.Code)
	}
}

func TestServerQueueRetentionLoop(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC().Add(-2 * time.Hour)
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-ret-1", WorkflowID: "wf", RunID: "r", Status: domain.RunJobStatusSucceeded, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	srv.SetQueueRetention(string(domain.RunJobStatusSucceeded), time.Hour, 20*time.Millisecond, 100)
	ctx, cancel := context.WithCancel(context.Background())
	go srv.runQueueRetentionLoop(ctx)
	defer cancel()

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := runQueue.GetRunJob(context.Background(), "job-ret-1"); err != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected retention loop to purge job")
}

func TestServerWorkflowPutAndPatch(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, nil, executor, nil, telemetry.NewCollector())

	initial := domain.Workflow{ID: "wf-update", Tasks: []domain.TaskNode{{ID: "t1", Action: "print"}}}
	_ = memStore.SaveWorkflow(context.Background(), initial)

	putBody := `{"id":"wf-update","name":"Updated","timeout":"45s","tasks":[{"id":"t1","action":"wait","input":{"durationMs":"1"},"retryPolicy":{"maxAttempts":1}}]}`
	putReq := httptest.NewRequest(http.MethodPut, "/workflows/wf-update", strings.NewReader(putBody))
	putReq.Header.Set("Authorization", "Bearer admin123")
	putRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(putRes, putReq)
	if putRes.Code != http.StatusOK {
		t.Fatalf("expected put OK, got %d: %s", putRes.Code, putRes.Body.String())
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/workflows/wf-update", strings.NewReader(`{"description":"patched"}`))
	patchReq.Header.Set("Authorization", "Bearer admin123")
	patchRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(patchRes, patchReq)
	if patchRes.Code != http.StatusOK {
		t.Fatalf("expected patch OK, got %d: %s", patchRes.Code, patchRes.Body.String())
	}

	stored, err := memStore.GetWorkflow(context.Background(), "wf-update")
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if stored.Name != "Updated" || stored.Description != "patched" {
		t.Fatalf("unexpected workflow update result: %+v", stored)
	}
}

func TestServerBulkRequeueWithAudit(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-bulk-1", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-bulk-2", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	body := `{"status":"DEAD_LETTER","replayedBy":"ops-user","replayReason":"incident recovery"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs/requeue", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}

	job, err := runQueue.GetRunJob(context.Background(), "job-bulk-1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != domain.RunJobStatusQueued || job.ReplayCount != 1 || job.ReplayedBy != "ops-user" {
		t.Fatalf("unexpected replayed job state: %+v", job)
	}
}

func TestServerSchedulePauseResume(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	svc := scheduler.NewService(executor, memStore)
	_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{ID: "wf-paused", Tasks: []domain.TaskNode{{ID: "a", Action: "print"}}})

	err := svc.Register(domain.ScheduleConfig{WorkflowID: "wf-paused", Interval: time.Second})
	if err != nil {
		t.Fatalf("register schedule: %v", err)
	}

	srv := NewServer(":0", "admin:admin123", memStore, nil, executor, svc, telemetry.NewCollector())
	pauseReq := httptest.NewRequest(http.MethodPost, "/schedules/pause/wf-paused", nil)
	pauseReq.Header.Set("Authorization", "Bearer admin123")
	pauseRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(pauseRes, pauseReq)
	if pauseRes.Code != http.StatusOK {
		t.Fatalf("expected pause OK, got %d", pauseRes.Code)
	}
	if !svc.IsPaused("wf-paused") {
		t.Fatalf("expected schedule paused state")
	}

	resumeReq := httptest.NewRequest(http.MethodPost, "/schedules/resume/wf-paused", nil)
	resumeReq.Header.Set("Authorization", "Bearer admin123")
	resumeRes := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(resumeRes, resumeReq)
	if resumeRes.Code != http.StatusOK {
		t.Fatalf("expected resume OK, got %d", resumeRes.Code)
	}
	if svc.IsPaused("wf-paused") {
		t.Fatalf("expected schedule resumed state")
	}
}

func TestServerDeadLetterJobsTimeFilter(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	oldTs := now.Add(-3 * time.Hour)
	recentTs := now.Add(-10 * time.Minute)
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-dead-old", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: oldTs, CreatedAt: oldTs, UpdatedAt: oldTs})
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-dead-recent", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: recentTs, CreatedAt: recentTs, UpdatedAt: recentTs})

	req := httptest.NewRequest(http.MethodGet, "/jobs/dead-letter?updatedAfter="+now.Add(-30*time.Minute).Format(time.RFC3339), nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}
	var jobs []domain.RunQueueJob
	if err := json.Unmarshal(res.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("unmarshal dead-letter jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-dead-recent" {
		t.Fatalf("unexpected filtered dead-letter jobs: %+v", jobs)
	}
}

func TestServerEnvelopePaginationMetadataAcrossEndpoints(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	svc := scheduler.NewService(executor, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, svc, telemetry.NewCollector())

	now := time.Now().UTC()
	for i := 1; i <= 3; i++ {
		wfID := "wf-page-" + strconv.Itoa(i)
		runID := "run-page-" + strconv.Itoa(i)
		jobID := "job-page-" + strconv.Itoa(i)
		_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{ID: wfID, Tasks: []domain.TaskNode{{ID: "a", Action: "print"}}})
		_ = memStore.SaveRun(context.Background(), domain.Run{ID: runID, WorkflowID: wfID, Status: domain.RunStatusSuccess, StartedAt: now.Add(time.Duration(i) * time.Minute), FinishedAt: now.Add(time.Duration(i)*time.Minute + time.Second)})
		status := domain.RunJobStatusQueued
		if i <= 2 {
			status = domain.RunJobStatusDead
		}
		_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: jobID, WorkflowID: wfID, RunID: runID, Status: status, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(time.Duration(i) * time.Second), UpdatedAt: now.Add(time.Duration(i) * time.Second)})
		createReq := httptest.NewRequest(http.MethodPost, "/schedules", strings.NewReader(`{"workflowId":"`+wfID+`","interval":"1m"}`))
		createReq.Header.Set("Authorization", "Bearer admin123")
		createRes := httptest.NewRecorder()
		srv.httpSrv.Handler.ServeHTTP(createRes, createReq)
		if createRes.Code != http.StatusCreated {
			t.Fatalf("expected schedule create OK, got %d: %s", createRes.Code, createRes.Body.String())
		}
	}

	assertEnvelope := func(path string, expectedTotal int) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer admin123")
		res := httptest.NewRecorder()
		srv.httpSrv.Handler.ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			t.Fatalf("expected OK for %s, got %d: %s", path, res.Code, res.Body.String())
		}
		var payload struct {
			Items    []json.RawMessage `json:"items"`
			Total    int               `json:"total"`
			Limit    int               `json:"limit"`
			Offset   int               `json:"offset"`
			Returned int               `json:"returned"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
			t.Fatalf("unmarshal %s envelope: %v", path, err)
		}
		if payload.Total != expectedTotal || payload.Limit != 1 || payload.Offset != 1 || payload.Returned != len(payload.Items) || payload.Returned != 1 {
			t.Fatalf("unexpected envelope metadata for %s: %+v", path, payload)
		}
	}

	assertEnvelope("/workflows?envelope=true&limit=1&offset=1", 3)
	assertEnvelope("/runs?envelope=true&limit=1&offset=1", 3)
	assertEnvelope("/jobs?envelope=true&limit=1&offset=1", 3)
	assertEnvelope("/jobs/dead-letter?envelope=true&limit=1&offset=1", 2)
	assertEnvelope("/schedules?envelope=true&limit=1&offset=1", 3)
}

func TestServerRunCancelPaginatesAllJobs(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Now().UTC()
	targetRunID := "run-cancel-many"
	_ = memStore.SaveRun(context.Background(), domain.Run{ID: targetRunID, WorkflowID: "wf", Status: domain.RunStatusRunning, StartedAt: now})

	for i := 0; i < 1100; i++ {
		id := "job-bulk-" + strconv.Itoa(i)
		runID := "run-other"
		if i == 1099 {
			runID = targetRunID
		}
		_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: id, WorkflowID: "wf", RunID: runID, Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(time.Duration(i) * time.Millisecond), UpdatedAt: now.Add(time.Duration(i) * time.Millisecond)})
	}

	req := httptest.NewRequest(http.MethodDelete, "/runs/"+targetRunID+"?reason=stop", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d: %s", res.Code, res.Body.String())
	}
	job, err := runQueue.GetRunJob(context.Background(), "job-bulk-1099")
	if err != nil {
		t.Fatalf("get target job: %v", err)
	}
	if job.Status != domain.RunJobStatusCancelled {
		t.Fatalf("expected cancelled status, got %s", job.Status)
	}
}
