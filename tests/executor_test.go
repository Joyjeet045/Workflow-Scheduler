package tests

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"workflowscheduler/internal/actions"
	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/engine"
	"workflowscheduler/internal/store"
)

type failNTimesAction struct {
	mu        sync.Mutex
	remaining int
}

func (a *failNTimesAction) Execute(_ context.Context, _ map[string]string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.remaining > 0 {
		a.remaining--
		return "", errors.New("transient failure")
	}
	return "ok", nil
}

type recordOrderAction struct {
	mu    sync.Mutex
	order *[]string
	name  string
}

func (a *recordOrderAction) Execute(_ context.Context, _ map[string]string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	*a.order = append(*a.order, a.name)
	return strconv.Itoa(len(*a.order)), nil
}

type echoInputAction struct{}

func (a *echoInputAction) Execute(_ context.Context, input map[string]string) (string, error) {
	return input["message"], nil
}

type alwaysFailAction struct{}

func (a *alwaysFailAction) Execute(_ context.Context, _ map[string]string) (string, error) {
	return "", errors.New("always fails")
}

type appendAction struct {
	mu      sync.Mutex
	records *[]string
	prefix  string
}

func (a *appendAction) Execute(_ context.Context, input map[string]string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	value := a.prefix + input["value"]
	*a.records = append(*a.records, value)
	return value, nil
}

func TestExecutorRetriesAndSucceeds(t *testing.T) {
	registry := actions.NewRegistry()
	registry.Register("flaky", &failNTimesAction{remaining: 2})

	executor := engine.NewExecutor(registry, store.NewMemoryStore())
	workflow := domain.Workflow{
		ID:             "wf-retry",
		MaxConcurrency: 1,
		Tasks: []domain.TaskNode{
			{
				ID:     "t1",
				Action: "flaky",
				RetryPolicy: domain.RetryPolicy{
					MaxAttempts: 3,
					BackoffBase: 1 * time.Millisecond,
				},
			},
		},
	}

	run, err := executor.Run(context.Background(), workflow)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}

	res := run.Results["t1"]
	if res.Status != domain.TaskStatusSuccess {
		t.Fatalf("expected SUCCESS, got %s", res.Status)
	}
	if res.Attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", res.Attempts)
	}
}

func TestExecutorDependencyOrdering(t *testing.T) {
	registry := actions.NewRegistry()
	order := []string{}
	registry.Register("a", &recordOrderAction{name: "a", order: &order})
	registry.Register("b", &recordOrderAction{name: "b", order: &order})

	executor := engine.NewExecutor(registry, store.NewMemoryStore())
	workflow := domain.Workflow{
		ID:             "wf-order",
		MaxConcurrency: 2,
		Tasks: []domain.TaskNode{
			{ID: "a", Action: "a"},
			{ID: "b", Action: "b", DependsOn: []string{"a"}},
		},
	}

	run, err := executor.Run(context.Background(), workflow)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}

	if run.Results["a"].Status != domain.TaskStatusSuccess {
		t.Fatalf("task a should succeed")
	}
	if run.Results["b"].Status != domain.TaskStatusSuccess {
		t.Fatalf("task b should succeed")
	}
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("unexpected execution order: %#v", order)
	}
}

func TestExecutorResolvesInputTemplatesAndConditions(t *testing.T) {
	registry := actions.NewRegistry()
	registry.Register("echo", &echoInputAction{})

	executor := engine.NewExecutor(registry, store.NewMemoryStore())
	workflow := domain.Workflow{
		ID:             "wf-template",
		MaxConcurrency: 2,
		Tasks: []domain.TaskNode{
			{ID: "seed", Action: "echo", Input: map[string]string{"message": "alpha"}},
			{
				ID:        "render",
				Action:    "echo",
				DependsOn: []string{"seed"},
				Condition: "on_success:seed",
				Input: map[string]string{
					"message": "seed=${seed.output}|status=${seed.status}",
				},
			},
			{
				ID:        "skip-on-failure",
				Action:    "echo",
				DependsOn: []string{"seed"},
				Condition: "on_failed:seed",
				Input: map[string]string{
					"message": "should skip",
				},
			},
		},
	}

	run, err := executor.Run(context.Background(), workflow)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}

	if run.Results["seed"].Status != domain.TaskStatusSuccess {
		t.Fatalf("seed should succeed")
	}
	if run.Results["render"].Status != domain.TaskStatusSuccess {
		t.Fatalf("render should succeed")
	}
	if !strings.Contains(run.Results["render"].Output, "seed=alpha") {
		t.Fatalf("expected template output, got %q", run.Results["render"].Output)
	}
	if run.Results["skip-on-failure"].Status != domain.TaskStatusSkipped {
		t.Fatalf("conditional task should be skipped")
	}
}

func TestExecutorFailFastStopsPipeline(t *testing.T) {
	registry := actions.NewRegistry()
	registry.Register("fail", &alwaysFailAction{})
	registry.Register("echo", &echoInputAction{})

	executor := engine.NewExecutor(registry, store.NewMemoryStore())
	workflow := domain.Workflow{
		ID:             "wf-fail-fast",
		MaxConcurrency: 3,
		FailFast:       true,
		Tasks: []domain.TaskNode{
			{ID: "root", Action: "fail"},
			{ID: "child-a", Action: "echo", DependsOn: []string{"root"}, Input: map[string]string{"message": "a"}},
			{ID: "child-b", Action: "echo", DependsOn: []string{"root"}, Input: map[string]string{"message": "b"}},
		},
	}

	run, err := executor.Run(context.Background(), workflow)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}

	if run.Status != domain.RunStatusFailed {
		t.Fatalf("expected FAILED status, got %s", run.Status)
	}
	if run.Results["root"].Status != domain.TaskStatusFailed {
		t.Fatalf("root should fail")
	}
	if run.Results["child-a"].Status != domain.TaskStatusSkipped {
		t.Fatalf("child-a should be skipped")
	}
	if run.Results["child-b"].Status != domain.TaskStatusSkipped {
		t.Fatalf("child-b should be skipped")
	}
}

func TestExecutorRunsCompensationOnFailure(t *testing.T) {
	registry := actions.NewRegistry()
	compOps := []string{}
	registry.Register("echo", &echoInputAction{})
	registry.Register("fail", &alwaysFailAction{})
	registry.Register("undo", &appendAction{records: &compOps, prefix: "undo-"})

	executor := engine.NewExecutor(registry, store.NewMemoryStore())
	workflow := domain.Workflow{
		ID:                  "wf-comp",
		MaxConcurrency:      1,
		CompensateOnFailure: true,
		Tasks: []domain.TaskNode{
			{
				ID:     "reserve",
				Action: "echo",
				Input:  map[string]string{"message": "reserved"},
				Compensation: domain.CompensationSpec{
					Action: "undo",
					Input:  map[string]string{"value": "${reserve.output}"},
				},
			},
			{ID: "charge", Action: "fail", DependsOn: []string{"reserve"}},
		},
	}

	run, err := executor.Run(context.Background(), workflow)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}

	if run.Status != domain.RunStatusFailed {
		t.Fatalf("expected failed run, got %s", run.Status)
	}
	if run.CompensationStatus != domain.CompensationStatusSuccess {
		t.Fatalf("expected successful compensation, got %s", run.CompensationStatus)
	}
	if len(run.CompensationResults) != 1 {
		t.Fatalf("expected 1 compensation result, got %d", len(run.CompensationResults))
	}
	if run.CompensationResults[0].Status != domain.TaskStatusSuccess {
		t.Fatalf("expected successful compensation result")
	}
	if len(compOps) != 1 || compOps[0] != "undo-reserved" {
		t.Fatalf("unexpected compensation operations: %#v", compOps)
	}
}
