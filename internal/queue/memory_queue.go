package queue

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"workflowscheduler/internal/domain"
)

type MemoryRunQueue struct {
	mu          sync.Mutex
	jobs        map[string]domain.RunQueueJob
	transitions map[string][]domain.RunJobTransition
}

func NewMemoryRunQueue() *MemoryRunQueue {
	return &MemoryRunQueue{jobs: map[string]domain.RunQueueJob{}, transitions: map[string][]domain.RunJobTransition{}}
}

func (q *MemoryRunQueue) EnqueueRunJob(_ context.Context, job domain.RunQueueJob) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, exists := q.jobs[job.ID]; exists {
		return nil
	}
	now := time.Now().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = now
	}
	if job.AvailableAt.IsZero() {
		job.AvailableAt = now
	}
	if job.Status == "" {
		job.Status = domain.RunJobStatusQueued
	}
	q.jobs[job.ID] = job
	q.appendTransition(job.ID, "", job.Status, "enqueued", "system")
	return nil
}

func (q *MemoryRunQueue) GetRunJob(_ context.Context, jobID string) (domain.RunQueueJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[jobID]
	if !ok {
		return domain.RunQueueJob{}, errors.New("run job not found")
	}
	return job, nil
}

func (q *MemoryRunQueue) GetRunJobByRequest(_ context.Context, workflowID string, requestID string) (domain.RunQueueJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, job := range q.jobs {
		if job.WorkflowID == workflowID && job.RequestID == requestID {
			return job, nil
		}
	}
	return domain.RunQueueJob{}, errors.New("run job not found")
}

func (q *MemoryRunQueue) LeaseNextRunJob(_ context.Context, workerID string, leaseDuration time.Duration) (domain.RunQueueJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now().UTC()
	candidates := make([]domain.RunQueueJob, 0)
	for _, job := range q.jobs {
		if job.Status == domain.RunJobStatusQueued && !job.AvailableAt.After(now) {
			candidates = append(candidates, job)
			continue
		}
		if job.Status == domain.RunJobStatusLeased && !job.LeaseUntil.IsZero() && job.LeaseUntil.Before(now) {
			candidates = append(candidates, job)
		}
	}
	if len(candidates) == 0 {
		return domain.RunQueueJob{}, errors.New("no run job available")
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
	})
	job := candidates[0]
	fromStatus := job.Status
	job.Status = domain.RunJobStatusLeased
	job.LeaseOwner = workerID
	job.LeaseUntil = now.Add(leaseDuration)
	job.Attempts++
	job.UpdatedAt = now
	q.jobs[job.ID] = job
	q.appendTransition(job.ID, fromStatus, domain.RunJobStatusLeased, "leased", workerID)
	return job, nil
}

func (q *MemoryRunQueue) ExtendRunJobLease(_ context.Context, jobID string, workerID string, leaseDuration time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[jobID]
	if !ok {
		return errors.New("run job not found")
	}
	if job.Status != domain.RunJobStatusLeased {
		return errors.New("run job is not leased")
	}
	if job.LeaseOwner != workerID {
		return errors.New("run job lease owner mismatch")
	}
	job.LeaseUntil = time.Now().UTC().Add(leaseDuration)
	job.UpdatedAt = time.Now().UTC()
	q.jobs[jobID] = job
	return nil
}

func (q *MemoryRunQueue) CompleteRunJob(_ context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[jobID]
	if !ok {
		return errors.New("run job not found")
	}
	if job.Status != domain.RunJobStatusLeased {
		return errors.New("run job is not leased")
	}
	fromStatus := job.Status
	job.Status = domain.RunJobStatusSucceeded
	job.LeaseOwner = ""
	job.LeaseUntil = time.Time{}
	job.UpdatedAt = time.Now().UTC()
	q.jobs[jobID] = job
	q.appendTransition(jobID, fromStatus, job.Status, "completed", "worker")
	return nil
}

func (q *MemoryRunQueue) CancelRunJob(_ context.Context, jobID string, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[jobID]
	if !ok {
		return errors.New("run job not found")
	}
	if job.Status == domain.RunJobStatusSucceeded {
		return errors.New("run job cannot be cancelled in current state")
	}
	if job.Status == domain.RunJobStatusCancelled {
		return nil
	}
	fromStatus := job.Status
	job.Status = domain.RunJobStatusCancelled
	job.LastError = reason
	job.LeaseOwner = ""
	job.LeaseUntil = time.Time{}
	job.UpdatedAt = time.Now().UTC()
	q.jobs[jobID] = job
	q.appendTransition(jobID, fromStatus, job.Status, reason, "api")
	return nil
}

func (q *MemoryRunQueue) RetryRunJob(_ context.Context, jobID string, retryAfter time.Duration, lastError string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[jobID]
	if !ok {
		return errors.New("run job not found")
	}
	if job.Status != domain.RunJobStatusLeased {
		return errors.New("run job is not leased")
	}
	fromStatus := job.Status
	job.Status = domain.RunJobStatusQueued
	job.AvailableAt = time.Now().UTC().Add(retryAfter)
	job.LastError = lastError
	job.LeaseOwner = ""
	job.LeaseUntil = time.Time{}
	job.UpdatedAt = time.Now().UTC()
	q.jobs[jobID] = job
	q.appendTransition(jobID, fromStatus, job.Status, lastError, "worker")
	return nil
}

func (q *MemoryRunQueue) FailRunJob(_ context.Context, jobID string, lastError string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[jobID]
	if !ok {
		return errors.New("run job not found")
	}
	if job.Status != domain.RunJobStatusLeased {
		return errors.New("run job is not leased")
	}
	fromStatus := job.Status
	job.Status = domain.RunJobStatusFailed
	job.LastError = lastError
	job.LeaseOwner = ""
	job.LeaseUntil = time.Time{}
	job.UpdatedAt = time.Now().UTC()
	q.jobs[jobID] = job
	q.appendTransition(jobID, fromStatus, job.Status, lastError, "worker")
	return nil
}

func (q *MemoryRunQueue) DeadLetterRunJob(_ context.Context, jobID string, lastError string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[jobID]
	if !ok {
		return errors.New("run job not found")
	}
	if job.Status != domain.RunJobStatusLeased {
		return errors.New("run job is not leased")
	}
	fromStatus := job.Status
	job.Status = domain.RunJobStatusDead
	job.LastError = lastError
	job.LeaseOwner = ""
	job.LeaseUntil = time.Time{}
	job.UpdatedAt = time.Now().UTC()
	q.jobs[jobID] = job
	q.appendTransition(jobID, fromStatus, job.Status, lastError, "worker")
	return nil
}

func (q *MemoryRunQueue) RequeueRunJob(_ context.Context, jobID string, availableAfter time.Duration, replayedBy string, replayReason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[jobID]
	if !ok {
		return errors.New("run job not found")
	}
	if job.Status != domain.RunJobStatusDead && job.Status != domain.RunJobStatusFailed {
		return errors.New("only failed or dead-letter jobs can be requeued")
	}
	fromStatus := job.Status
	job.Status = domain.RunJobStatusQueued
	job.LeaseOwner = ""
	job.LeaseUntil = time.Time{}
	job.AvailableAt = time.Now().UTC().Add(availableAfter)
	job.ReplayCount++
	job.ReplayedBy = replayedBy
	job.ReplayReason = replayReason
	job.ReplayedAt = time.Now().UTC()
	job.UpdatedAt = time.Now().UTC()
	q.jobs[jobID] = job
	q.appendTransition(jobID, fromStatus, job.Status, replayReason, replayedBy)
	return nil
}

func (q *MemoryRunQueue) ListRunJobs(_ context.Context, status string, limit int, offset int) ([]domain.RunQueueJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := make([]domain.RunQueueJob, 0, len(q.jobs))
	for _, job := range q.jobs {
		if status == "" || string(job.Status) == status {
			items = append(items, job)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ID < items[j].ID
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []domain.RunQueueJob{}, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], nil
}

func (q *MemoryRunQueue) ListRunJobsFiltered(_ context.Context, status string, workflowID string, requestID string, updatedAfter time.Time, updatedBefore time.Time, limit int, offset int) ([]domain.RunQueueJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	items := make([]domain.RunQueueJob, 0, len(q.jobs))
	for _, job := range q.jobs {
		if status != "" && string(job.Status) != status {
			continue
		}
		if workflowID != "" && job.WorkflowID != workflowID {
			continue
		}
		if requestID != "" && job.RequestID != requestID {
			continue
		}
		if !updatedAfter.IsZero() && job.UpdatedAt.Before(updatedAfter) {
			continue
		}
		if !updatedBefore.IsZero() && job.UpdatedAt.After(updatedBefore) {
			continue
		}
		items = append(items, job)
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ID < items[j].ID
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})

	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []domain.RunQueueJob{}, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], nil
}

func (q *MemoryRunQueue) CountRunJobsFiltered(_ context.Context, status string, workflowID string, requestID string, updatedAfter time.Time, updatedBefore time.Time) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	count := 0
	for _, job := range q.jobs {
		if status != "" && string(job.Status) != status {
			continue
		}
		if workflowID != "" && job.WorkflowID != workflowID {
			continue
		}
		if requestID != "" && job.RequestID != requestID {
			continue
		}
		if !updatedAfter.IsZero() && job.UpdatedAt.Before(updatedAfter) {
			continue
		}
		if !updatedBefore.IsZero() && job.UpdatedAt.After(updatedBefore) {
			continue
		}
		count++
	}

	return count, nil
}

func (q *MemoryRunQueue) ListRunJobsByRun(_ context.Context, runID string, limit int, offset int) ([]domain.RunQueueJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := make([]domain.RunQueueJob, 0)
	for _, job := range q.jobs {
		if job.RunID == runID {
			items = append(items, job)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ID < items[j].ID
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []domain.RunQueueJob{}, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], nil
}

func (q *MemoryRunQueue) CountRunJobsByStatus(_ context.Context) (map[domain.RunJobStatus]int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	counts := map[domain.RunJobStatus]int{}
	for _, job := range q.jobs {
		counts[job.Status]++
	}
	return counts, nil
}

func (q *MemoryRunQueue) PurgeRunJobs(_ context.Context, status string, olderThan time.Duration, limit int) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if limit <= 0 {
		limit = 1000
	}
	cutoff := time.Time{}
	if olderThan > 0 {
		cutoff = time.Now().UTC().Add(-olderThan)
	}

	type candidate struct {
		id        string
		updatedAt time.Time
	}
	candidates := make([]candidate, 0)
	for id, job := range q.jobs {
		if status != "" && string(job.Status) != status {
			continue
		}
		if !cutoff.IsZero() && job.UpdatedAt.After(cutoff) {
			continue
		}
		candidates = append(candidates, candidate{id: id, updatedAt: job.UpdatedAt})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].updatedAt.Equal(candidates[j].updatedAt) {
			return candidates[i].id < candidates[j].id
		}
		return candidates[i].updatedAt.Before(candidates[j].updatedAt)
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	for _, c := range candidates {
		delete(q.jobs, c.id)
		delete(q.transitions, c.id)
	}
	return len(candidates), nil
}

func (q *MemoryRunQueue) ListRunJobTransitions(_ context.Context, jobID string, limit int, offset int) ([]domain.RunJobTransition, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	items := append([]domain.RunJobTransition(nil), q.transitions[jobID]...)
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []domain.RunJobTransition{}, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], nil
}

func (q *MemoryRunQueue) ListRunJobTransitionsFiltered(_ context.Context, jobID string, toStatus string, actor string, createdAfter time.Time, createdBefore time.Time, limit int, offset int) ([]domain.RunJobTransition, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	items := make([]domain.RunJobTransition, 0, len(q.transitions[jobID]))
	for _, item := range q.transitions[jobID] {
		if toStatus != "" && string(item.ToStatus) != toStatus {
			continue
		}
		if actor != "" && item.Actor != actor {
			continue
		}
		if !createdAfter.IsZero() && item.CreatedAt.Before(createdAfter) {
			continue
		}
		if !createdBefore.IsZero() && item.CreatedAt.After(createdBefore) {
			continue
		}
		items = append(items, item)
	}
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []domain.RunJobTransition{}, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], nil
}

func (q *MemoryRunQueue) CountRunJobTransitionsFiltered(_ context.Context, jobID string, toStatus string, actor string, createdAfter time.Time, createdBefore time.Time) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	count := 0
	for _, item := range q.transitions[jobID] {
		if toStatus != "" && string(item.ToStatus) != toStatus {
			continue
		}
		if actor != "" && item.Actor != actor {
			continue
		}
		if !createdAfter.IsZero() && item.CreatedAt.Before(createdAfter) {
			continue
		}
		if !createdBefore.IsZero() && item.CreatedAt.After(createdBefore) {
			continue
		}
		count++
	}
	return count, nil
}

func (q *MemoryRunQueue) appendTransition(jobID string, fromStatus domain.RunJobStatus, toStatus domain.RunJobStatus, reason string, actor string) {
	q.transitions[jobID] = append(q.transitions[jobID], domain.RunJobTransition{
		JobID:      jobID,
		FromStatus: fromStatus,
		ToStatus:   toStatus,
		Reason:     reason,
		Actor:      actor,
		CreatedAt:  time.Now().UTC(),
	})
}
