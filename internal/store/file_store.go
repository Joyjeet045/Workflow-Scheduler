package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"workflowscheduler/internal/domain"
)

type FileStore struct {
	mu      sync.Mutex
	baseDir string
	mem     *MemoryStore
}

func NewFileStore(baseDir string) (*FileStore, error) {
	if baseDir == "" {
		return nil, errors.New("baseDir is required")
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}

	store := &FileStore{baseDir: baseDir, mem: NewMemoryStore()}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *FileStore) SaveWorkflow(ctx context.Context, workflow domain.Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.SaveWorkflow(ctx, workflow); err != nil {
		return err
	}
	return s.flushLocked()
}

func (s *FileStore) GetWorkflow(ctx context.Context, workflowID string) (domain.Workflow, error) {
	return s.mem.GetWorkflow(ctx, workflowID)
}

func (s *FileStore) ListWorkflows(ctx context.Context) ([]domain.Workflow, error) {
	return s.mem.ListWorkflows(ctx)
}

func (s *FileStore) SaveRun(ctx context.Context, run domain.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.SaveRun(ctx, run); err != nil {
		return err
	}
	return s.flushLocked()
}

func (s *FileStore) GetRun(ctx context.Context, runID string) (domain.Run, error) {
	return s.mem.GetRun(ctx, runID)
}

func (s *FileStore) ListRuns(ctx context.Context, workflowID string) ([]domain.Run, error) {
	return s.mem.ListRuns(ctx, workflowID)
}

func (s *FileStore) ListRunsFiltered(ctx context.Context, workflowID string, status domain.RunStatus, requestID string, startedAfter time.Time, startedBefore time.Time, orderDesc bool, limit int, offset int) ([]domain.Run, error) {
	return s.mem.ListRunsFiltered(ctx, workflowID, status, requestID, startedAfter, startedBefore, orderDesc, limit, offset)
}

func (s *FileStore) CountRunsFiltered(ctx context.Context, workflowID string, status domain.RunStatus, requestID string, startedAfter time.Time, startedBefore time.Time) (int, error) {
	return s.mem.CountRunsFiltered(ctx, workflowID, status, requestID, startedAfter, startedBefore)
}

type persistedSnapshot struct {
	Workflows []domain.Workflow `json:"workflows"`
	Runs      []domain.Run      `json:"runs"`
}

func (s *FileStore) snapshotPath() string {
	return filepath.Join(s.baseDir, "snapshot.json")
}

func (s *FileStore) load() error {
	path := s.snapshotPath()
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var snapshot persistedSnapshot
	if err := json.Unmarshal(content, &snapshot); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	ctx := context.Background()
	for _, w := range snapshot.Workflows {
		if err := s.mem.SaveWorkflow(ctx, w); err != nil {
			return err
		}
	}
	for _, r := range snapshot.Runs {
		if err := s.mem.SaveRun(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStore) flushLocked() error {
	workflows, err := s.mem.ListWorkflows(context.Background())
	if err != nil {
		return err
	}
	runs, err := s.mem.ListRuns(context.Background(), "")
	if err != nil {
		return err
	}

	snapshot := persistedSnapshot{Workflows: workflows, Runs: runs}
	content, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.snapshotPath(), content, 0o644)
}
