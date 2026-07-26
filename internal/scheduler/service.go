package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/engine"
	"workflowscheduler/pkg/utils"
)

type Service struct {
	executor *engine.Executor
	store    domain.WorkflowStore
	queue    domain.RunQueue

	mu             sync.Mutex
	jobs           map[string]domain.ScheduleConfig
	paused         map[string]bool
	running        map[string]bool
	cronEntryIDs   map[string]cron.EntryID
	intervalStop   map[string]context.CancelFunc
	cronEngine     *cron.Cron
	started        bool
	ctx            context.Context
	leaderLockPath string
	leaderLockHeld bool
	scheduleStore  string
}

type persistedSchedules struct {
	Jobs   []domain.ScheduleConfig `json:"jobs"`
	Paused []string                `json:"paused"`
}

func (s *Service) SetRunQueue(queue domain.RunQueue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = queue
}

func NewService(executor *engine.Executor, store domain.WorkflowStore) *Service {
	return &Service{
		executor:     executor,
		store:        store,
		jobs:         map[string]domain.ScheduleConfig{},
		paused:       map[string]bool{},
		running:      map[string]bool{},
		cronEntryIDs: map[string]cron.EntryID{},
		intervalStop: map[string]context.CancelFunc{},
		cronEngine: cron.New(cron.WithParser(cron.NewParser(
			cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
		))),
	}
}

func (s *Service) Pause(workflowID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[workflowID]; !ok {
		return errors.New("schedule not found")
	}
	s.paused[workflowID] = true
	_ = s.persistLocked()
	return nil
}

func (s *Service) Resume(workflowID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[workflowID]; !ok {
		return errors.New("schedule not found")
	}
	delete(s.paused, workflowID)
	_ = s.persistLocked()
	return nil
}

func (s *Service) IsPaused(workflowID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.paused[workflowID]
}

func (s *Service) SetLeaderLockPath(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaderLockPath = path
}

func (s *Service) SetScheduleStorePath(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduleStore = strings.TrimSpace(path)
	_ = s.loadLocked()
}

func (s *Service) Register(config domain.ScheduleConfig) error {
	if config.WorkflowID == "" {
		return errors.New("workflowID is required")
	}
	if config.Interval <= 0 && config.Cron == "" {
		return errors.New("either interval or cron must be provided")
	}
	if config.Interval > 0 && config.Cron != "" {
		return errors.New("interval and cron cannot both be set")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.unscheduleLocked(config.WorkflowID)
	s.jobs[config.WorkflowID] = config
	_ = s.persistLocked()

	if !s.started {
		return nil
	}

	return s.startJobLocked(s.ctx, config)
}

func (s *Service) List() []domain.ScheduleConfig {
	s.mu.Lock()
	defer s.mu.Unlock()

	items := make([]domain.ScheduleConfig, 0, len(s.jobs))
	for _, cfg := range s.jobs {
		items = append(items, cfg)
	}
	return items
}

func (s *Service) Unregister(workflowID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.jobs, workflowID)
	delete(s.paused, workflowID)
	s.unscheduleLocked(workflowID)
	_ = s.persistLocked()
}

func (s *Service) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	_ = s.loadLocked()
	if !s.tryAcquireLeaderLockLocked() {
		s.started = true
		s.ctx = ctx
		s.mu.Unlock()
		go func() { <-ctx.Done() }()
		return
	}
	s.started = true
	s.ctx = ctx
	jobs := make([]domain.ScheduleConfig, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	s.cronEngine.Start()
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		stopCtx := s.cronEngine.Stop()
		<-stopCtx.Done()
		s.releaseLeaderLock()
	}()

	for _, job := range jobs {
		s.mu.Lock()
		_ = s.startJobLocked(ctx, job)
		s.mu.Unlock()
	}
}

func (s *Service) tryAcquireLeaderLockLocked() bool {
	if s.leaderLockPath == "" {
		s.leaderLockHeld = true
		return true
	}
	if err := os.MkdirAll(filepath.Dir(s.leaderLockPath), 0o755); err != nil {
		return false
	}
	file, err := os.OpenFile(s.leaderLockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return false
	}
	_ = file.Close()
	s.leaderLockHeld = true
	return true
}

func (s *Service) releaseLeaderLock() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.leaderLockHeld || s.leaderLockPath == "" {
		return
	}
	_ = os.Remove(s.leaderLockPath)
	s.leaderLockHeld = false
}

func (s *Service) startJobLocked(ctx context.Context, job domain.ScheduleConfig) error {
	if job.Cron != "" {
		if existingID, ok := s.cronEntryIDs[job.WorkflowID]; ok {
			s.cronEngine.Remove(existingID)
			delete(s.cronEntryIDs, job.WorkflowID)
		}
		if stop, ok := s.intervalStop[job.WorkflowID]; ok {
			stop()
			delete(s.intervalStop, job.WorkflowID)
		}

		entryID, err := s.cronEngine.AddFunc(job.Cron, func() {
			_ = s.executeOnce(ctx, job)
		})
		if err != nil {
			return err
		}
		s.cronEntryIDs[job.WorkflowID] = entryID
		if job.RunOnStartup {
			go func() {
				_ = s.executeOnce(ctx, job)
			}()
		}
		return nil
	}

	if existingID, ok := s.cronEntryIDs[job.WorkflowID]; ok {
		s.cronEngine.Remove(existingID)
		delete(s.cronEntryIDs, job.WorkflowID)
	}
	if stop, ok := s.intervalStop[job.WorkflowID]; ok {
		stop()
		delete(s.intervalStop, job.WorkflowID)
	}

	jobCtx, cancel := context.WithCancel(ctx)
	s.intervalStop[job.WorkflowID] = cancel
	go s.startIntervalJob(jobCtx, job)
	return nil
}

func (s *Service) startIntervalJob(ctx context.Context, job domain.ScheduleConfig) {
	interval := job.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if job.RunOnStartup {
		_ = s.executeOnce(ctx, job)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.executeOnce(ctx, job)
		}
	}
}

func (s *Service) executeOnce(ctx context.Context, job domain.ScheduleConfig) error {
	s.mu.Lock()
	if s.paused[job.WorkflowID] {
		s.mu.Unlock()
		return nil
	}
	if s.running[job.WorkflowID] && !job.AllowOverlap {
		s.mu.Unlock()
		return nil
	}
	s.running[job.WorkflowID] = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running[job.WorkflowID] = false
		s.mu.Unlock()
	}()

	workflow, err := s.store.GetWorkflow(ctx, job.WorkflowID)
	if err != nil {
		return err
	}

	if s.queue != nil {
		runID := utils.NewRunID(workflow.ID)
		queueJob := domain.RunQueueJob{
			ID:          fmt.Sprintf("job-%s", runID),
			WorkflowID:  workflow.ID,
			RunID:       runID,
			Status:      domain.RunJobStatusQueued,
			MaxAttempts: 5,
			AvailableAt: time.Now().UTC(),
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}
		return s.queue.EnqueueRunJob(ctx, queueJob)
	}

	_, err = s.executor.Run(ctx, workflow)
	return err
}

func (s *Service) unscheduleLocked(workflowID string) {
	if existingID, ok := s.cronEntryIDs[workflowID]; ok {
		s.cronEngine.Remove(existingID)
		delete(s.cronEntryIDs, workflowID)
	}
	if stop, ok := s.intervalStop[workflowID]; ok {
		stop()
		delete(s.intervalStop, workflowID)
	}
}

func (s *Service) loadLocked() error {
	if s.scheduleStore == "" {
		return nil
	}
	bytes, err := os.ReadFile(s.scheduleStore)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var persisted persistedSchedules
	if err := json.Unmarshal(bytes, &persisted); err != nil {
		return err
	}
	s.jobs = map[string]domain.ScheduleConfig{}
	for _, cfg := range persisted.Jobs {
		s.jobs[cfg.WorkflowID] = cfg
	}
	s.paused = map[string]bool{}
	for _, workflowID := range persisted.Paused {
		s.paused[workflowID] = true
	}
	return nil
}

func (s *Service) persistLocked() error {
	if s.scheduleStore == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.scheduleStore), 0o755); err != nil {
		return err
	}
	persisted := persistedSchedules{
		Jobs:   make([]domain.ScheduleConfig, 0, len(s.jobs)),
		Paused: make([]string, 0, len(s.paused)),
	}
	for _, cfg := range s.jobs {
		persisted.Jobs = append(persisted.Jobs, cfg)
	}
	sort.Slice(persisted.Jobs, func(i, j int) bool { return persisted.Jobs[i].WorkflowID < persisted.Jobs[j].WorkflowID })
	for workflowID, paused := range s.paused {
		if paused {
			persisted.Paused = append(persisted.Paused, workflowID)
		}
	}
	sort.Strings(persisted.Paused)

	bytes, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.scheduleStore, bytes, 0o644)
}
