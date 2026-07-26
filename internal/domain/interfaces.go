package domain

import (
	"context"
	"time"
)

type Action interface {
	Execute(ctx context.Context, input map[string]string) (string, error)
}

type ActionRegistry interface {
	Get(name string) (Action, bool)
	Register(name string, action Action)
}

type WorkflowStore interface {
	SaveWorkflow(ctx context.Context, workflow Workflow) error
	GetWorkflow(ctx context.Context, workflowID string) (Workflow, error)
	ListWorkflows(ctx context.Context) ([]Workflow, error)
	SaveRun(ctx context.Context, run Run) error
	GetRun(ctx context.Context, runID string) (Run, error)
	ListRuns(ctx context.Context, workflowID string) ([]Run, error)
	ListRunsFiltered(ctx context.Context, workflowID string, status RunStatus, requestID string, startedAfter time.Time, startedBefore time.Time, orderDesc bool, limit int, offset int) ([]Run, error)
	CountRunsFiltered(ctx context.Context, workflowID string, status RunStatus, requestID string, startedAfter time.Time, startedBefore time.Time) (int, error)
}

type RunQueue interface {
	EnqueueRunJob(ctx context.Context, job RunQueueJob) error
	GetRunJob(ctx context.Context, jobID string) (RunQueueJob, error)
	GetRunJobByRequest(ctx context.Context, workflowID string, requestID string) (RunQueueJob, error)
	LeaseNextRunJob(ctx context.Context, workerID string, leaseDuration time.Duration) (RunQueueJob, error)
	ExtendRunJobLease(ctx context.Context, jobID string, workerID string, leaseDuration time.Duration) error
	CompleteRunJob(ctx context.Context, jobID string) error
	CancelRunJob(ctx context.Context, jobID string, reason string) error
	RetryRunJob(ctx context.Context, jobID string, retryAfter time.Duration, lastError string) error
	FailRunJob(ctx context.Context, jobID string, lastError string) error
	DeadLetterRunJob(ctx context.Context, jobID string, lastError string) error
	RequeueRunJob(ctx context.Context, jobID string, availableAfter time.Duration, replayedBy string, replayReason string) error
	ListRunJobs(ctx context.Context, status string, limit int, offset int) ([]RunQueueJob, error)
	ListRunJobsFiltered(ctx context.Context, status string, workflowID string, requestID string, updatedAfter time.Time, updatedBefore time.Time, limit int, offset int) ([]RunQueueJob, error)
	CountRunJobsFiltered(ctx context.Context, status string, workflowID string, requestID string, updatedAfter time.Time, updatedBefore time.Time) (int, error)
	ListRunJobsByRun(ctx context.Context, runID string, limit int, offset int) ([]RunQueueJob, error)
	CountRunJobsByStatus(ctx context.Context) (map[RunJobStatus]int, error)
	PurgeRunJobs(ctx context.Context, status string, olderThan time.Duration, limit int) (int, error)
	ListRunJobTransitions(ctx context.Context, jobID string, limit int, offset int) ([]RunJobTransition, error)
	ListRunJobTransitionsFiltered(ctx context.Context, jobID string, toStatus string, actor string, createdAfter time.Time, createdBefore time.Time, limit int, offset int) ([]RunJobTransition, error)
	CountRunJobTransitionsFiltered(ctx context.Context, jobID string, toStatus string, actor string, createdAfter time.Time, createdBefore time.Time) (int, error)
}
