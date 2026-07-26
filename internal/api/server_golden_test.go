package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"workflowscheduler/internal/actions"
	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/engine"
	"workflowscheduler/internal/queue"
	"workflowscheduler/internal/store"
	"workflowscheduler/internal/telemetry"
)

func TestGoldenWorkflowsEnvelopeResponse(t *testing.T) {
	memStore := store.NewMemoryStore()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, nil, executor, nil, telemetry.NewCollector())

	_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{ID: "wf-golden-a", Tasks: []domain.TaskNode{{ID: "a", Action: "print"}}})
	_ = memStore.SaveWorkflow(context.Background(), domain.Workflow{ID: "wf-golden-b", Tasks: []domain.TaskNode{{ID: "b", Action: "print"}}})

	req := httptest.NewRequest(http.MethodGet, "/workflows?envelope=true&limit=1&offset=1", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}
	assertGoldenResponse(t, "workflows_envelope.golden", res.Body.String())
}

func TestGoldenBulkCancelAllFailedResponse(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-known", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusSucceeded, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	req := httptest.NewRequest(http.MethodPost, "/jobs/cancel", strings.NewReader(`{"jobIds":["job-missing"],"reason":"x"}`))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}
	assertGoldenResponse(t, "bulk_cancel_all_failed.golden", res.Body.String())
}

func TestGoldenJobHistoryEnvelopeResponse(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	req := httptest.NewRequest(http.MethodGet, "/jobs/history/job-unknown?envelope=true&limit=5&offset=0", nil)
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}
	assertGoldenResponse(t, "job_history_envelope_empty.golden", res.Body.String())
}

func TestGoldenJobsPurgeDryRunResponse(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	srv := NewServer(":0", "admin:admin123", memStore, runQueue, executor, nil, telemetry.NewCollector())

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	_ = runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-purge-golden", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusSucceeded, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	req := httptest.NewRequest(http.MethodPost, "/jobs/purge?dryRun=true", strings.NewReader(`{"status":"SUCCEEDED","olderThan":"1h","limit":1}`))
	req.Header.Set("Authorization", "Bearer admin123")
	res := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected OK, got %d", res.Code)
	}
	assertGoldenResponse(t, "jobs_purge_dry_run.golden", res.Body.String())
}

func assertGoldenResponse(t *testing.T, goldenFile string, actual string) {
	t.Helper()
	path := filepath.Join("testdata", goldenFile)
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(path, []byte(actual), 0o644); err != nil {
			t.Fatalf("update golden: %v", err)
		}
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if actual != string(expected) {
		t.Fatalf("golden mismatch for %s\nexpected:\n%s\nactual:\n%s", goldenFile, string(expected), actual)
	}
}
