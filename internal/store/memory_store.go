package store

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"workflowscheduler/internal/domain"
)

type MemoryStore struct {
	mu        sync.RWMutex
	workflows map[string]domain.Workflow
	runs      map[string]domain.Run
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflows: map[string]domain.Workflow{},
		runs:      map[string]domain.Run{},
	}
}

func (s *MemoryStore) SaveWorkflow(_ context.Context, workflow domain.Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflows[workflow.ID] = workflow
	return nil
}

func (s *MemoryStore) GetWorkflow(_ context.Context, workflowID string) (domain.Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	workflow, ok := s.workflows[workflowID]
	if !ok {
		return domain.Workflow{}, errors.New("workflow not found")
	}
	return workflow, nil
}

func (s *MemoryStore) ListWorkflows(_ context.Context) ([]domain.Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.Workflow, 0, len(s.workflows))
	for _, w := range s.workflows {
		items = append(items, w)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}

func (s *MemoryStore) SaveRun(_ context.Context, run domain.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[run.ID] = run
	return nil
}

func (s *MemoryStore) GetRun(_ context.Context, runID string) (domain.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run, ok := s.runs[runID]
	if !ok {
		return domain.Run{}, errors.New("run not found")
	}
	return run, nil
}

func (s *MemoryStore) ListRuns(_ context.Context, workflowID string) ([]domain.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.Run, 0)
	for _, r := range s.runs {
		if workflowID == "" || r.WorkflowID == workflowID {
			items = append(items, r)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].StartedAt.Before(items[j].StartedAt) })
	return items, nil
}

func (s *MemoryStore) ListRunsFiltered(_ context.Context, workflowID string, status domain.RunStatus, requestID string, startedAfter time.Time, startedBefore time.Time, orderDesc bool, limit int, offset int) ([]domain.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]domain.Run, 0, len(s.runs))
	for _, run := range s.runs {
		if workflowID != "" && run.WorkflowID != workflowID {
			continue
		}
		if status != "" && run.Status != status {
			continue
		}
		if requestID != "" && run.RequestID != requestID {
			continue
		}
		if !startedAfter.IsZero() && run.StartedAt.Before(startedAfter) {
			continue
		}
		if !startedBefore.IsZero() && run.StartedAt.After(startedBefore) {
			continue
		}
		items = append(items, run)
	}

	sort.Slice(items, func(i, j int) bool {
		if orderDesc {
			return items[i].StartedAt.After(items[j].StartedAt)
		}
		return items[i].StartedAt.Before(items[j].StartedAt)
	})

	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []domain.Run{}, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], nil
}

func (s *MemoryStore) CountRunsFiltered(_ context.Context, workflowID string, status domain.RunStatus, requestID string, startedAfter time.Time, startedBefore time.Time) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, run := range s.runs {
		if workflowID != "" && run.WorkflowID != workflowID {
			continue
		}
		if status != "" && run.Status != status {
			continue
		}
		if requestID != "" && run.RequestID != requestID {
			continue
		}
		if !startedAfter.IsZero() && run.StartedAt.Before(startedAfter) {
			continue
		}
		if !startedBefore.IsZero() && run.StartedAt.After(startedBefore) {
			continue
		}
		count++
	}
	return count, nil
}
