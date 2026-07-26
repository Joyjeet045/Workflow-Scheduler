package queue

import (
	"context"
	"testing"
	"time"

	"workflowscheduler/internal/domain"
)

func TestMemoryRunQueueLeaseRetryAndComplete(t *testing.T) {
	q := NewMemoryRunQueue()
	ctx := context.Background()
	now := time.Now().UTC()

	job := domain.RunQueueJob{
		ID:          "job-1",
		WorkflowID:  "wf-1",
		RunID:       "run-1",
		Status:      domain.RunJobStatusQueued,
		MaxAttempts: 3,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := q.EnqueueRunJob(ctx, job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	leased, err := q.LeaseNextRunJob(ctx, "worker-a", 2*time.Second)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if leased.Status != domain.RunJobStatusLeased || leased.Attempts != 1 {
		t.Fatalf("unexpected lease state: %+v", leased)
	}
	originalLeaseUntil := leased.LeaseUntil
	if err := q.ExtendRunJobLease(ctx, leased.ID, "worker-a", 3*time.Second); err != nil {
		t.Fatalf("extend lease: %v", err)
	}
	extended, err := q.GetRunJob(ctx, leased.ID)
	if err != nil {
		t.Fatalf("get extended lease job: %v", err)
	}
	if !extended.LeaseUntil.After(originalLeaseUntil) {
		t.Fatalf("expected extended lease deadline")
	}

	if err := q.RetryRunJob(ctx, leased.ID, 20*time.Millisecond, "boom"); err != nil {
		t.Fatalf("retry: %v", err)
	}

	time.Sleep(25 * time.Millisecond)
	leasedAgain, err := q.LeaseNextRunJob(ctx, "worker-b", 2*time.Second)
	if err != nil {
		t.Fatalf("lease after retry: %v", err)
	}
	if leasedAgain.Attempts != 2 || leasedAgain.LeaseOwner != "worker-b" {
		t.Fatalf("unexpected re-lease state: %+v", leasedAgain)
	}

	if err := q.CompleteRunJob(ctx, leasedAgain.ID); err != nil {
		t.Fatalf("complete: %v", err)
	}
	final, err := q.GetRunJob(ctx, leasedAgain.ID)
	if err != nil {
		t.Fatalf("get final: %v", err)
	}
	if final.Status != domain.RunJobStatusSucceeded {
		t.Fatalf("expected succeeded status, got %s", final.Status)
	}
}

func TestMemoryRunQueueDeadLetterAndRequeue(t *testing.T) {
	q := NewMemoryRunQueue()
	ctx := context.Background()
	now := time.Now().UTC()

	job := domain.RunQueueJob{
		ID:          "job-2",
		WorkflowID:  "wf-2",
		RunID:       "run-2",
		Status:      domain.RunJobStatusQueued,
		MaxAttempts: 2,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := q.EnqueueRunJob(ctx, job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := q.LeaseNextRunJob(ctx, "worker-a", 2*time.Second); err != nil {
		t.Fatalf("lease: %v", err)
	}
	if err := q.DeadLetterRunJob(ctx, job.ID, "terminal failure"); err != nil {
		t.Fatalf("dead-letter: %v", err)
	}

	stored, err := q.GetRunJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get dead-letter job: %v", err)
	}
	if stored.Status != domain.RunJobStatusDead {
		t.Fatalf("expected DEAD_LETTER, got %s", stored.Status)
	}

	if err := q.RequeueRunJob(ctx, job.ID, 0, "tester", "manual replay"); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	requeued, err := q.GetRunJob(ctx, job.ID)
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

func TestMemoryRunQueueCountByStatus(t *testing.T) {
	q := NewMemoryRunQueue()
	ctx := context.Background()
	now := time.Now().UTC()

	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-c-1", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-c-2", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-c-3", WorkflowID: "wf", RunID: "r3", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	counts, err := q.CountRunJobsByStatus(ctx)
	if err != nil {
		t.Fatalf("count status: %v", err)
	}
	if counts[domain.RunJobStatusQueued] != 1 {
		t.Fatalf("expected queued count 1, got %d", counts[domain.RunJobStatusQueued])
	}
	if counts[domain.RunJobStatusDead] != 2 {
		t.Fatalf("expected dead-letter count 2, got %d", counts[domain.RunJobStatusDead])
	}
}

func TestMemoryRunQueueLeaseExpiryRecovery(t *testing.T) {
	q := NewMemoryRunQueue()
	ctx := context.Background()
	now := time.Now().UTC()
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-exp-1", WorkflowID: "wf", RunID: "run", Status: domain.RunJobStatusQueued, MaxAttempts: 2, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	first, err := q.LeaseNextRunJob(ctx, "worker-a", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("first lease: %v", err)
	}
	if first.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", first.Attempts)
	}

	time.Sleep(30 * time.Millisecond)
	second, err := q.LeaseNextRunJob(ctx, "worker-b", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("second lease after expiry: %v", err)
	}
	if second.LeaseOwner != "worker-b" || second.Attempts != 2 {
		t.Fatalf("expected lease takeover by worker-b with attempts=2, got %+v", second)
	}
}

func TestMemoryRunQueuePurgeByStatus(t *testing.T) {
	q := NewMemoryRunQueue()
	ctx := context.Background()
	now := time.Now().UTC().Add(-2 * time.Hour)
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-p-1", WorkflowID: "wf", RunID: "r1", Status: domain.RunJobStatusSucceeded, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-p-2", WorkflowID: "wf", RunID: "r2", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	deleted, err := q.PurgeRunJobs(ctx, string(domain.RunJobStatusSucceeded), time.Hour, 10)
	if err != nil {
		t.Fatalf("purge jobs: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected one purged job, got %d", deleted)
	}
	if _, err := q.GetRunJob(ctx, "job-p-1"); err == nil {
		t.Fatalf("expected job-p-1 to be purged")
	}
	if _, err := q.GetRunJob(ctx, "job-p-2"); err != nil {
		t.Fatalf("expected job-p-2 to remain: %v", err)
	}
}

func TestMemoryRunQueueCancelTransition(t *testing.T) {
	q := NewMemoryRunQueue()
	ctx := context.Background()
	now := time.Now().UTC()
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-cancel-1", WorkflowID: "wf", RunID: "r", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	if err := q.CancelRunJob(ctx, "job-cancel-1", "operator-request"); err != nil {
		t.Fatalf("cancel job: %v", err)
	}
	job, err := q.GetRunJob(ctx, "job-cancel-1")
	if err != nil {
		t.Fatalf("get cancelled job: %v", err)
	}
	if job.Status != domain.RunJobStatusCancelled {
		t.Fatalf("expected CANCELLED, got %s", job.Status)
	}
}

func TestMemoryRunQueueTransitionHistory(t *testing.T) {
	q := NewMemoryRunQueue()
	ctx := context.Background()
	now := time.Now().UTC()
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-h-1", WorkflowID: "wf", RunID: "r", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})

	if _, err := q.LeaseNextRunJob(ctx, "worker-a", 2*time.Second); err != nil {
		t.Fatalf("lease: %v", err)
	}
	if err := q.CancelRunJob(ctx, "job-h-1", "manual-cancel"); err != nil {
		t.Fatalf("cancel leased job: %v", err)
	}

	history, err := q.ListRunJobTransitions(ctx, "job-h-1", 20, 0)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) < 3 {
		t.Fatalf("expected at least 3 transitions, got %d", len(history))
	}
	if history[0].ToStatus != domain.RunJobStatusQueued {
		t.Fatalf("expected first transition to QUEUED, got %s", history[0].ToStatus)
	}
	if history[len(history)-1].ToStatus != domain.RunJobStatusCancelled {
		t.Fatalf("expected last transition to CANCELLED, got %s", history[len(history)-1].ToStatus)
	}
}

func TestMemoryRunQueueListByRunAndTransitionFilters(t *testing.T) {
	q := NewMemoryRunQueue()
	ctx := context.Background()
	now := time.Now().UTC()
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-run-a", WorkflowID: "wf", RunID: "run-a", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-run-b", WorkflowID: "wf", RunID: "run-b", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(time.Millisecond), UpdatedAt: now.Add(time.Millisecond)})

	jobs, err := q.ListRunJobsByRun(ctx, "run-a", 10, 0)
	if err != nil {
		t.Fatalf("list by run: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-run-a" {
		t.Fatalf("unexpected run-scoped jobs: %+v", jobs)
	}

	_, _ = q.LeaseNextRunJob(ctx, "worker-a", time.Second)
	_ = q.CancelRunJob(ctx, "job-run-a", "manual")

	filtered, err := q.ListRunJobTransitionsFiltered(ctx, "job-run-a", string(domain.RunJobStatusCancelled), "api", time.Time{}, time.Time{}, 10, 0)
	if err != nil {
		t.Fatalf("filtered transitions: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected one filtered transition, got %d", len(filtered))
	}
	count, err := q.CountRunJobTransitionsFiltered(ctx, "job-run-a", string(domain.RunJobStatusCancelled), "api", time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("count filtered transitions: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected filtered count 1, got %d", count)
	}
}

func TestMemoryRunQueueJobFilters(t *testing.T) {
	q := NewMemoryRunQueue()
	ctx := context.Background()
	now := time.Now().UTC()

	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-f-1", WorkflowID: "wf-a", RunID: "run-a", RequestID: "req-a", Status: domain.RunJobStatusQueued, MaxAttempts: 1, AvailableAt: now, CreatedAt: now, UpdatedAt: now})
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-f-2", WorkflowID: "wf-a", RunID: "run-b", RequestID: "req-b", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(time.Millisecond), UpdatedAt: now.Add(2 * time.Minute)})
	_ = q.EnqueueRunJob(ctx, domain.RunQueueJob{ID: "job-f-3", WorkflowID: "wf-b", RunID: "run-c", RequestID: "req-c", Status: domain.RunJobStatusDead, MaxAttempts: 1, AvailableAt: now, CreatedAt: now.Add(2 * time.Millisecond), UpdatedAt: now.Add(4 * time.Minute)})

	updatedAfter := now.Add(time.Minute)
	jobs, err := q.ListRunJobsFiltered(ctx, string(domain.RunJobStatusDead), "wf-a", "", updatedAfter, time.Time{}, 10, 0)
	if err != nil {
		t.Fatalf("list filtered jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-f-2" {
		t.Fatalf("unexpected filtered jobs: %+v", jobs)
	}

	count, err := q.CountRunJobsFiltered(ctx, string(domain.RunJobStatusDead), "", "", updatedAfter, time.Time{})
	if err != nil {
		t.Fatalf("count filtered jobs: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected filtered dead count 2, got %d", count)
	}
}
