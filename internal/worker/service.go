package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/engine"
	"workflowscheduler/internal/telemetry"
)

type Service struct {
	workerID      string
	queue         domain.RunQueue
	store         domain.WorkflowStore
	executor      *engine.Executor
	metrics       *telemetry.Collector
	pollInterval  time.Duration
	leaseDuration time.Duration

	mu       sync.Mutex
	draining bool
}

func NewService(workerID string, queue domain.RunQueue, store domain.WorkflowStore, executor *engine.Executor) *Service {
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", time.Now().UTC().UnixNano())
	}
	return &Service{
		workerID:      workerID,
		queue:         queue,
		store:         store,
		executor:      executor,
		pollInterval:  500 * time.Millisecond,
		leaseDuration: 30 * time.Second,
	}
}

func (s *Service) SetPolling(pollInterval time.Duration, leaseDuration time.Duration) {
	if pollInterval > 0 {
		s.pollInterval = pollInterval
	}
	if leaseDuration > 0 {
		s.leaseDuration = leaseDuration
	}
}

func (s *Service) SetMetricsCollector(metrics *telemetry.Collector) {
	s.metrics = metrics
}

func (s *Service) Drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.draining = true
}

func (s *Service) IsDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.draining
}

func (s *Service) Start(ctx context.Context) {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		if s.IsDraining() {
			return
		}
		if err := s.processOne(ctx); err == nil {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) processOne(ctx context.Context) error {
	job, err := s.queue.LeaseNextRunJob(ctx, s.workerID, s.leaseDuration)
	if err != nil {
		return err
	}
	ctx, span := telemetry.StartSpan(ctx, "worker.process_job",
		attribute.String("worker.id", s.workerID),
		attribute.String("job.id", job.ID),
		attribute.String("run.id", job.RunID),
		attribute.String("workflow.id", job.WorkflowID),
	)
	defer span.End()
	if s.metrics != nil {
		s.metrics.RecordWorkerLease()
	}

	workflow, err := s.store.GetWorkflow(ctx, job.WorkflowID)
	if err != nil {
		_ = s.queue.FailRunJob(ctx, job.ID, err.Error())
		return err
	}

	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		interval := s.leaseDuration / 2
		if interval <= 0 {
			interval = 2 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				_ = s.queue.ExtendRunJobLease(ctx, job.ID, s.workerID, s.leaseDuration)
			}
		}
	}()

	defer func() {
		heartbeatCancel()
		<-heartbeatDone
	}()

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	cancelWatchCtx, cancelWatch := context.WithCancel(ctx)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		watchTicker := time.NewTicker(100 * time.Millisecond)
		defer watchTicker.Stop()
		for {
			select {
			case <-cancelWatchCtx.Done():
				return
			case <-watchTicker.C:
				current, err := s.queue.GetRunJob(ctx, job.ID)
				if err != nil {
					continue
				}
				if current.Status == domain.RunJobStatusCancelled {
					runCancel()
					return
				}
			}
		}
	}()

	run, execErr := s.executor.RunWithID(runCtx, workflow, job.RunID, job.RequestID)
	cancelWatch()
	<-watchDone
	if execErr == nil && run.Status != domain.RunStatusFailed && run.Status != domain.RunStatusCancelled {
		_ = s.queue.CompleteRunJob(ctx, job.ID)
		span.SetAttributes(attribute.String("job.status", string(domain.RunJobStatusSucceeded)))
		if s.metrics != nil {
			s.metrics.RecordWorkerSuccess()
		}
		return nil
	}
	if execErr == nil && run.Status == domain.RunStatusCancelled {
		message := run.Error
		if message == "" {
			message = "run cancelled"
		}
		_ = s.queue.CancelRunJob(ctx, job.ID, message)
		span.SetAttributes(attribute.String("job.status", string(domain.RunJobStatusCancelled)))
		if s.metrics != nil {
			s.metrics.RecordWorkerFailure()
		}
		return errorsWithContext(message)
	}

	attempts := job.Attempts
	if attempts < job.MaxAttempts {
		backoff := time.Duration(1<<(attempts-1)) * time.Second
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
		message := "run failed"
		if execErr != nil {
			message = execErr.Error()
		} else if run.Error != "" {
			message = run.Error
		}
		_ = s.queue.RetryRunJob(ctx, job.ID, backoff, message)
		span.SetAttributes(attribute.String("job.status", string(domain.RunJobStatusQueued)))
		if s.metrics != nil {
			s.metrics.RecordWorkerRetry()
			s.metrics.RecordWorkerFailure()
		}
		return errorsWithContext(message)
	}

	message := "run failed"
	if execErr != nil {
		message = execErr.Error()
	} else if run.Error != "" {
		message = run.Error
	}
	_ = s.queue.DeadLetterRunJob(ctx, job.ID, message)
	span.SetAttributes(attribute.String("job.status", string(domain.RunJobStatusDead)))
	if s.metrics != nil {
		s.metrics.RecordWorkerDeadLetter()
		s.metrics.RecordWorkerFailure()
	}
	return errorsWithContext(message)
}

type workerError struct{ message string }

func (e workerError) Error() string { return e.message }

func errorsWithContext(message string) error {
	return workerError{message: message}
}
