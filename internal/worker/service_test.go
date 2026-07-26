package worker

import (
	"context"
	"testing"
	"time"

	"workflowscheduler/internal/actions"
	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/engine"
	"workflowscheduler/internal/queue"
	"workflowscheduler/internal/store"
)

func TestWorkerDrainStopsLeasing(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	service := NewService("worker-drain", runQueue, memStore, executor)
	service.SetPolling(10*time.Millisecond, 100*time.Millisecond)
	service.Drain()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		service.Start(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("worker did not stop after drain signal")
	}
}

func TestWorkerCancelsQueueJobWhenRunCancelled(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	service := NewService("worker-cancel", runQueue, memStore, executor)
	service.SetPolling(10*time.Millisecond, 50*time.Millisecond)

	workflow := domain.Workflow{
		ID:      "wf-cancel-worker",
		Timeout: 10 * time.Millisecond,
		Tasks: []domain.TaskNode{
			{ID: "wait", Action: "wait", Input: map[string]string{"durationMs": "100"}},
		},
	}
	if err := memStore.SaveWorkflow(context.Background(), workflow); err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	now := time.Now().UTC()
	if err := runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-worker-cancel", WorkflowID: workflow.ID, RunID: "run-worker-cancel", Status: domain.RunJobStatusQueued, MaxAttempts: 2, AvailableAt: now, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	go service.Start(ctx)

	deadline := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(deadline) {
		job, err := runQueue.GetRunJob(context.Background(), "job-worker-cancel")
		if err == nil && job.Status == domain.RunJobStatusCancelled {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected queue job to transition to CANCELLED")
}

func TestWorkerHonorsExternalJobCancellation(t *testing.T) {
	memStore := store.NewMemoryStore()
	runQueue := queue.NewMemoryRunQueue()
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, memStore)
	service := NewService("worker-ext-cancel", runQueue, memStore, executor)
	service.SetPolling(10*time.Millisecond, 80*time.Millisecond)

	workflow := domain.Workflow{
		ID: "wf-ext-cancel",
		Tasks: []domain.TaskNode{
			{ID: "wait", Action: "wait", Input: map[string]string{"durationMs": "600"}},
		},
	}
	if err := memStore.SaveWorkflow(context.Background(), workflow); err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	now := time.Now().UTC()
	if err := runQueue.EnqueueRunJob(context.Background(), domain.RunQueueJob{ID: "job-ext-cancel", WorkflowID: workflow.ID, RunID: "run-ext-cancel", Status: domain.RunJobStatusQueued, MaxAttempts: 2, AvailableAt: now, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go service.Start(ctx)

	deadline := time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		job, err := runQueue.GetRunJob(context.Background(), "job-ext-cancel")
		if err == nil && job.Status == domain.RunJobStatusLeased {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := runQueue.CancelRunJob(context.Background(), "job-ext-cancel", "external-stop"); err != nil {
		t.Fatalf("cancel job externally: %v", err)
	}

	deadline = time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		job, err := runQueue.GetRunJob(context.Background(), "job-ext-cancel")
		if err == nil && job.Status == domain.RunJobStatusCancelled {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected externally cancelled job to remain CANCELLED")
}
