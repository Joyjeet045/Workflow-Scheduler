package tests

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"workflowscheduler/internal/actions"
	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/engine"
	"workflowscheduler/internal/scheduler"
	"workflowscheduler/internal/store"
)

func TestScheduleRegisterValidation(t *testing.T) {
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	executor := engine.NewExecutor(registry, store.NewMemoryStore())
	svc := scheduler.NewService(executor, store.NewMemoryStore())

	if err := svc.Register(domain.ScheduleConfig{}); err == nil {
		t.Fatalf("expected error for empty config")
	}
	if err := svc.Register(domain.ScheduleConfig{WorkflowID: "wf"}); err == nil {
		t.Fatalf("expected error for missing trigger")
	}
	if err := svc.Register(domain.ScheduleConfig{WorkflowID: "wf", Interval: 1, Cron: "@every 5s"}); err == nil {
		t.Fatalf("expected error for conflicting trigger config")
	}
}

func TestScheduleRegisterValidCron(t *testing.T) {
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	memStore := store.NewMemoryStore()
	executor := engine.NewExecutor(registry, memStore)
	svc := scheduler.NewService(executor, memStore)

	if err := svc.Register(domain.ScheduleConfig{WorkflowID: "wf", Cron: "@every 1m"}); err != nil {
		t.Fatalf("expected valid cron schedule, got error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.Start(ctx)
}

func TestScheduleListAndUnregister(t *testing.T) {
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	memStore := store.NewMemoryStore()
	executor := engine.NewExecutor(registry, memStore)
	svc := scheduler.NewService(executor, memStore)

	err := svc.Register(domain.ScheduleConfig{WorkflowID: "wf-a", Interval: 30 * time.Second})
	if err != nil {
		t.Fatalf("register schedule: %v", err)
	}
	err = svc.Register(domain.ScheduleConfig{WorkflowID: "wf-b", Cron: "@every 1m"})
	if err != nil {
		t.Fatalf("register schedule: %v", err)
	}

	items := svc.List()
	if len(items) != 2 {
		t.Fatalf("expected 2 schedules, got %d", len(items))
	}

	svc.Unregister("wf-a")
	items = svc.List()
	if len(items) != 1 {
		t.Fatalf("expected 1 schedule after unregister, got %d", len(items))
	}
	if items[0].WorkflowID != "wf-b" {
		t.Fatalf("expected wf-b to remain, got %s", items[0].WorkflowID)
	}
}

func TestSchedulerLeaderLockLifecycle(t *testing.T) {
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	memStore := store.NewMemoryStore()
	executor := engine.NewExecutor(registry, memStore)
	svc := scheduler.NewService(executor, memStore)

	lockPath := filepath.Join(t.TempDir(), "leader.lock")
	svc.SetLeaderLockPath(lockPath)

	err := svc.Register(domain.ScheduleConfig{WorkflowID: "wf-a", Interval: 1 * time.Minute})
	if err != nil {
		t.Fatalf("register schedule: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	svc.Start(ctx)

	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("expected lock file to exist: %v", err)
	}

	cancel()
	time.Sleep(20 * time.Millisecond)

	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("expected lock file to be removed after shutdown")
	}
}

func TestSchedulerPauseResumeState(t *testing.T) {
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	memStore := store.NewMemoryStore()
	executor := engine.NewExecutor(registry, memStore)
	svc := scheduler.NewService(executor, memStore)

	err := svc.Register(domain.ScheduleConfig{WorkflowID: "wf-pause", Interval: 10 * time.Second})
	if err != nil {
		t.Fatalf("register schedule: %v", err)
	}

	if err := svc.Pause("wf-pause"); err != nil {
		t.Fatalf("pause schedule: %v", err)
	}
	if !svc.IsPaused("wf-pause") {
		t.Fatalf("expected paused state")
	}

	if err := svc.Resume("wf-pause"); err != nil {
		t.Fatalf("resume schedule: %v", err)
	}
	if svc.IsPaused("wf-pause") {
		t.Fatalf("expected resumed state")
	}
}

func TestSchedulerPersistsSchedules(t *testing.T) {
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	memStore := store.NewMemoryStore()
	executor := engine.NewExecutor(registry, memStore)
	svc := scheduler.NewService(executor, memStore)

	storePath := filepath.Join(t.TempDir(), "schedules.json")
	svc.SetScheduleStorePath(storePath)

	if err := svc.Register(domain.ScheduleConfig{WorkflowID: "wf-persist-1", Interval: time.Minute}); err != nil {
		t.Fatalf("register schedule: %v", err)
	}
	if err := svc.Register(domain.ScheduleConfig{WorkflowID: "wf-persist-2", Cron: "@every 1m"}); err != nil {
		t.Fatalf("register schedule: %v", err)
	}
	if err := svc.Pause("wf-persist-2"); err != nil {
		t.Fatalf("pause schedule: %v", err)
	}

	raw, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read persisted schedules: %v", err)
	}
	var payload struct {
		Jobs   []domain.ScheduleConfig `json:"jobs"`
		Paused []string                `json:"paused"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal persisted schedules: %v", err)
	}
	if len(payload.Jobs) != 2 {
		t.Fatalf("expected 2 persisted jobs, got %d", len(payload.Jobs))
	}
	if len(payload.Paused) != 1 || payload.Paused[0] != "wf-persist-2" {
		t.Fatalf("unexpected paused payload: %+v", payload.Paused)
	}
}

func TestSchedulerLoadsPersistedSchedulesOnStart(t *testing.T) {
	registry := actions.NewRegistry()
	actions.RegisterBuiltins(registry)
	memStore := store.NewMemoryStore()
	executor := engine.NewExecutor(registry, memStore)

	storePath := filepath.Join(t.TempDir(), "schedules.json")
	seed := scheduler.NewService(executor, memStore)
	seed.SetScheduleStorePath(storePath)
	if err := seed.Register(domain.ScheduleConfig{WorkflowID: "wf-load-1", Interval: 2 * time.Minute}); err != nil {
		t.Fatalf("seed register: %v", err)
	}
	if err := seed.Pause("wf-load-1"); err != nil {
		t.Fatalf("seed pause: %v", err)
	}

	loaded := scheduler.NewService(executor, memStore)
	loaded.SetScheduleStorePath(storePath)
	ctx, cancel := context.WithCancel(context.Background())
	loaded.Start(ctx)
	defer cancel()

	items := loaded.List()
	if len(items) != 1 || items[0].WorkflowID != "wf-load-1" {
		t.Fatalf("expected loaded schedule wf-load-1, got %+v", items)
	}
	if !loaded.IsPaused("wf-load-1") {
		t.Fatalf("expected loaded paused state")
	}
}
