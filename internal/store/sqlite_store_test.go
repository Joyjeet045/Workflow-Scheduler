package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"workflowscheduler/internal/domain"
)

func TestSQLiteStoreRunQueueLifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	job := domain.RunQueueJob{
		ID:          "job-sql-1",
		WorkflowID:  "wf-sql-1",
		RunID:       "run-sql-1",
		RequestID:   "req-1",
		Status:      domain.RunJobStatusQueued,
		MaxAttempts: 3,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := s.EnqueueRunJob(ctx, job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	leased, err := s.LeaseNextRunJob(ctx, "worker-sql", 2*time.Second)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if leased.Status != domain.RunJobStatusLeased || leased.Attempts != 1 {
		t.Fatalf("unexpected lease result: %+v", leased)
	}
	if err := s.ExtendRunJobLease(ctx, leased.ID, "worker-sql", 3*time.Second); err != nil {
		t.Fatalf("extend lease: %v", err)
	}

	if err := s.CompleteRunJob(ctx, leased.ID); err != nil {
		t.Fatalf("complete: %v", err)
	}
	completed, err := s.GetRunJob(ctx, leased.ID)
	if err != nil {
		t.Fatalf("get completed: %v", err)
	}
	if completed.Status != domain.RunJobStatusSucceeded {
		t.Fatalf("expected succeeded status, got %s", completed.Status)
	}

	foundByReq, err := s.GetRunJobByRequest(ctx, "wf-sql-1", "req-1")
	if err != nil {
		t.Fatalf("get by request: %v", err)
	}
	if foundByReq.ID != job.ID {
		t.Fatalf("expected request lookup to return %s, got %s", job.ID, foundByReq.ID)
	}
}

func TestSQLiteStoreDeadLetterAndRequeue(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	job := domain.RunQueueJob{
		ID:          "job-sql-2",
		WorkflowID:  "wf-sql-2",
		RunID:       "run-sql-2",
		Status:      domain.RunJobStatusQueued,
		MaxAttempts: 2,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := s.EnqueueRunJob(ctx, job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := s.LeaseNextRunJob(ctx, "worker-sql", 2*time.Second); err != nil {
		t.Fatalf("lease: %v", err)
	}
	if err := s.DeadLetterRunJob(ctx, job.ID, "terminal"); err != nil {
		t.Fatalf("dead-letter: %v", err)
	}

	dead, err := s.GetRunJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get dead-letter job: %v", err)
	}
	if dead.Status != domain.RunJobStatusDead {
		t.Fatalf("expected DEAD_LETTER, got %s", dead.Status)
	}

	if err := s.RequeueRunJob(ctx, job.ID, 0, "tester", "manual replay"); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	requeued, err := s.GetRunJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get requeued job: %v", err)
	}
	if requeued.Status != domain.RunJobStatusQueued {
		t.Fatalf("expected QUEUED after requeue, got %s", requeued.Status)
	}
	if requeued.ReplayCount != 1 || requeued.ReplayedBy != "tester" {
		t.Fatalf("expected replay audit metadata to be set, got %+v", requeued)
	}
}

func TestSQLiteStoreRejectInvalidTransition(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	job := domain.RunQueueJob{ID: "job-sql-invalid", WorkflowID: "wf", RunID: "run", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now}
	if err := s.EnqueueRunJob(ctx, job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := s.CompleteRunJob(ctx, job.ID); err == nil {
		t.Fatalf("expected completion without lease to be rejected")
	}
}

func TestSQLiteStoreCountByStatus(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-count-1", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-count-2", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	counts, err := s.CountRunJobsByStatus(ctx)
	if err != nil {
		t.Fatalf("count status: %v", err)
	}
	if counts[domain.RunJobStatusQueued] != 1 {
		t.Fatalf("expected queued count 1, got %d", counts[domain.RunJobStatusQueued])
	}
	if counts[domain.RunJobStatusDead] != 1 {
		t.Fatalf("expected dead-letter count 1, got %d", counts[domain.RunJobStatusDead])
	}
}

func TestSQLiteStoreLeaseExpiryRecovery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-sql-exp", WorkflowID: "wf", RunID: "run", Status: domain.RunJobStatusQueued, MaxAttempts: 2, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	first, err := s.LeaseNextRunJob(ctx, "worker-a", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("first lease: %v", err)
	}
	if first.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", first.Attempts)
	}

	time.Sleep(30 * time.Millisecond)
	second, err := s.LeaseNextRunJob(ctx, "worker-b", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("second lease after expiry: %v", err)
	}
	if second.LeaseOwner != "worker-b" || second.Attempts != 2 {
		t.Fatalf("expected lease takeover by worker-b with attempts=2, got %+v", second)
	}
}

func TestSQLiteStorePurgeByStatus(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC().Add(-2 * time.Hour)
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-sql-p-1", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusSucceeded, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-sql-p-2", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	deleted, err := s.PurgeRunJobs(ctx, string(domain.RunJobStatusSucceeded), time.Hour, 100)
	if err != nil {
		t.Fatalf("purge jobs: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected one purged job, got %d", deleted)
	}
	if _, err := s.GetRunJob(ctx, "job-sql-p-1"); err == nil {
		t.Fatalf("expected job-sql-p-1 to be purged")
	}
	if _, err := s.GetRunJob(ctx, "job-sql-p-2"); err != nil {
		t.Fatalf("expected job-sql-p-2 to remain: %v", err)
	}
}

func TestSQLiteStoreCancelTransition(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-sql-cancel", WorkflowID: "wf", RunID: "r", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	if err := s.CancelRunJob(ctx, "job-sql-cancel", "operator-request"); err != nil {
		t.Fatalf("cancel job: %v", err)
	}
	job, err := s.GetRunJob(ctx, "job-sql-cancel")
	if err != nil {
		t.Fatalf("get cancelled job: %v", err)
	}
	if job.Status != domain.RunJobStatusCancelled {
		t.Fatalf("expected CANCELLED, got %s", job.Status)
	}
}

func TestSQLiteStoreTransitionHistory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-sql-h-1", WorkflowID: "wf", RunID: "r", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	if _, err := s.LeaseNextRunJob(ctx, "worker-1", 2*time.Second); err != nil {
		t.Fatalf("lease: %v", err)
	}
	if err := s.CancelRunJob(ctx, "job-sql-h-1", "manual"); err != nil {
		t.Fatalf("cancel leased: %v", err)
	}

	history, err := s.ListRunJobTransitions(ctx, "job-sql-h-1", 20, 0)
	if err != nil {
		t.Fatalf("list history: %v", err)
	}
	if len(history) < 3 {
		t.Fatalf("expected at least 3 transitions, got %d", len(history))
	}
	if history[len(history)-1].ToStatus != domain.RunJobStatusCancelled {
		t.Fatalf("expected final status CANCELLED, got %s", history[len(history)-1].ToStatus)
	}
}

func TestSQLiteStoreRunScopedAndFilteredTransitionQueries(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-run-a", WorkflowID: "wf", RunID: "run-a", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-run-b", WorkflowID: "wf", RunID: "run-b", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(time.Millisecond), UpdatedAt: now.Add(time.Millisecond)})

	jobs, err := s.ListRunJobsByRun(ctx, "run-a", 10, 0)
	if err != nil {
		t.Fatalf("list jobs by run: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-run-a" {
		t.Fatalf("unexpected run-scoped jobs: %+v", jobs)
	}

	if _, err := s.LeaseNextRunJob(ctx, "worker-a", time.Second); err != nil {
		t.Fatalf("lease: %v", err)
	}
	if err := s.CancelRunJob(ctx, "job-run-a", "manual"); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	items, err := s.ListRunJobTransitionsFiltered(ctx, "job-run-a", string(domain.RunJobStatusCancelled), "api", time.Time{}, time.Time{}, 10, 0)
	if err != nil {
		t.Fatalf("list filtered transitions: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one filtered transition, got %d", len(items))
	}
	count, err := s.CountRunJobTransitionsFiltered(ctx, "job-run-a", string(domain.RunJobStatusCancelled), "api", time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("count filtered transitions: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
}

func TestSQLiteStoreFilteredJobQueries(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-f-1", WorkflowID: "wf-a", RunID: "run-a", RequestID: "req-a", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-f-2", WorkflowID: "wf-a", RunID: "run-b", RequestID: "req-b", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(time.Millisecond), UpdatedAt: now.Add(2 * time.Minute)})
	_ = s.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-f-3", WorkflowID: "wf-b", RunID: "run-c", RequestID: "req-c", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(2 * time.Millisecond), UpdatedAt: now.Add(4 * time.Minute)})

	updatedAfter := now.Add(time.Minute)
	jobs, err := s.ListRunJobsFiltered(ctx, string(domain.RunJobStatusDead), "wf-a", "", updatedAfter, time.Time{}, 10, 0)
	if err != nil {
		t.Fatalf("list filtered jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-f-2" {
		t.Fatalf("unexpected filtered jobs: %+v", jobs)
	}

	count, err := s.CountRunJobsFiltered(ctx, string(domain.RunJobStatusDead), "", "", updatedAfter, time.Time{})
	if err != nil {
		t.Fatalf("count filtered jobs: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected filtered dead count 2, got %d", count)
	}
}

func TestSQLiteStoreFilteredRunQueries(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "orchestrator.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()
	_ = s.SaveRun(ctx, domain.Run{ID: "run-f-1", WorkflowID: "wf-r", RequestID: "req-1", Status: domain.RunStatusSuccess, StartedAt: now.Add(-2 * time.Hour), FinishedAt: now.Add(-90 * time.Minute)})
	_ = s.SaveRun(ctx, domain.Run{ID: "run-f-2", WorkflowID: "wf-r", RequestID: "req-2", Status: domain.RunStatusFailed, StartedAt: now.Add(-1 * time.Hour), FinishedAt: now.Add(-50 * time.Minute)})
	_ = s.SaveRun(ctx, domain.Run{ID: "run-f-3", WorkflowID: "wf-r", RequestID: "req-3", Status: domain.RunStatusFailed, StartedAt: now.Add(-30 * time.Minute), FinishedAt: now.Add(-20 * time.Minute)})

	items, err := s.ListRunsFiltered(ctx, "wf-r", domain.RunStatusFailed, "", now.Add(-70*time.Minute), time.Time{}, true, 1, 0)
	if err != nil {
		t.Fatalf("list filtered runs: %v", err)
	}
	if len(items) != 1 || items[0].ID != "run-f-3" {
		t.Fatalf("unexpected filtered run page: %+v", items)
	}

	count, err := s.CountRunsFiltered(ctx, "wf-r", domain.RunStatusFailed, "", now.Add(-70*time.Minute), time.Time{})
	if err != nil {
		t.Fatalf("count filtered runs: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected filtered run count 2, got %d", count)
	}
}
