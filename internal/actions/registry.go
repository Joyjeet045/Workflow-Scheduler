package actions

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"workflowscheduler/internal/domain"
)

type Registry struct {
	mu      sync.RWMutex
	actions map[string]domain.Action
}

func NewRegistry() *Registry {
	return &Registry{actions: map[string]domain.Action{}}
}

func (r *Registry) Register(name string, action domain.Action) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions[name] = action
}

func (r *Registry) Get(name string) (domain.Action, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.actions[name]
	return a, ok
}

func RegisterBuiltins(registry *Registry) {
	registry.Register("print", PrintAction{})
	registry.Register("wait", WaitAction{})
	registry.Register("randomFail", RandomFailAction{})
}

type PrintAction struct{}

func (a PrintAction) Execute(_ context.Context, input map[string]string) (string, error) {
	message := input["message"]
	if message == "" {
		message = "print action executed"
	}
	return message, nil
}

type WaitAction struct{}

func (a WaitAction) Execute(ctx context.Context, input map[string]string) (string, error) {
	msRaw := input["durationMs"]
	if msRaw == "" {
		msRaw = "100"
	}

	durationMs, err := strconv.Atoi(msRaw)
	if err != nil {
		return "", fmt.Errorf("invalid durationMs: %w", err)
	}

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(time.Duration(durationMs) * time.Millisecond):
		return fmt.Sprintf("waited for %dms", durationMs), nil
	}
}

type RandomFailAction struct{}

func (a RandomFailAction) Execute(_ context.Context, input map[string]string) (string, error) {
	threshold := 50
	if val, ok := input["failPercent"]; ok && val != "" {
		parsed, err := strconv.Atoi(val)
		if err != nil {
			return "", fmt.Errorf("invalid failPercent: %w", err)
		}
		if parsed < 0 || parsed > 100 {
			return "", errors.New("failPercent must be between 0 and 100")
		}
		threshold = parsed
	}

	if rand.Intn(100) < threshold {
		return "", errors.New("random failure triggered")
	}
	return "random success", nil
}
