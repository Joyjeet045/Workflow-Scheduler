package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/telemetry"
	"workflowscheduler/pkg/utils"
)

type Executor struct {
	registry domain.ActionRegistry
	store    domain.WorkflowStore
	metrics  *telemetry.Collector
}

func NewExecutor(registry domain.ActionRegistry, store domain.WorkflowStore) *Executor {
	return &Executor{registry: registry, store: store}
}

func (e *Executor) SetMetricsCollector(collector *telemetry.Collector) {
	e.metrics = collector
}

type taskExecutionResult struct {
	taskID string
	result domain.TaskResult
}

func (e *Executor) Run(ctx context.Context, workflow domain.Workflow) (domain.Run, error) {
	return e.runWithID(ctx, workflow, "", "")
}

func (e *Executor) RunWithID(ctx context.Context, workflow domain.Workflow, runID string, requestID string) (domain.Run, error) {
	return e.runWithID(ctx, workflow, runID, requestID)
}

func (e *Executor) runWithID(ctx context.Context, workflow domain.Workflow, forcedRunID string, requestID string) (domain.Run, error) {
	if err := ValidateWorkflow(workflow); err != nil {
		return domain.Run{}, err
	}

	runID := forcedRunID
	if strings.TrimSpace(runID) == "" {
		runID = utils.NewRunID(workflow.ID)
	}

	ctx, span := telemetry.StartSpan(ctx, "workflow.run",
		attribute.String("workflow.id", workflow.ID),
		attribute.String("run.id", runID),
		attribute.String("request.id", requestID),
	)
	defer span.End()

	run := domain.Run{
		ID:                 runID,
		WorkflowID:         workflow.ID,
		RequestID:          requestID,
		Status:             domain.RunStatusRunning,
		CompensationStatus: domain.CompensationStatusNotRequired,
		StartedAt:          time.Now().UTC(),
		Results:            make(map[string]domain.TaskResult, len(workflow.Tasks)),
	}

	if e.store != nil {
		_ = e.store.SaveRun(ctx, run)
	}

	baseCtx, cancelBase := context.WithCancel(ctx)
	defer cancelBase()

	runCtx := baseCtx
	cancelTimeout := func() {}
	if workflow.Timeout > 0 {
		runCtx, cancelTimeout = context.WithTimeout(baseCtx, workflow.Timeout)
	}
	defer cancelTimeout()

	tasksByID := map[string]domain.TaskNode{}
	remainingDeps := map[string]int{}
	dependents := map[string][]string{}
	blockedByFailure := map[string]bool{}

	for _, t := range workflow.Tasks {
		tasksByID[t.ID] = t
		remainingDeps[t.ID] = len(t.DependsOn)
		for _, dep := range t.DependsOn {
			dependents[dep] = append(dependents[dep], t.ID)
		}
	}

	maxConcurrency := workflow.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}

	ready := make([]string, 0, len(workflow.Tasks))
	for id, deps := range remainingDeps {
		if deps == 0 {
			ready = append(ready, id)
		}
	}

	resultCh := make(chan taskExecutionResult, len(workflow.Tasks))
	active := 0
	completed := 0
	failFastTriggered := false

	startTask := func(taskID string, task domain.TaskNode) {
		active++
		go func() {
			res := e.runTask(runCtx, task)
			resultCh <- taskExecutionResult{taskID: taskID, result: res}
		}()
	}

	for completed < len(workflow.Tasks) {
		for active < maxConcurrency && len(ready) > 0 {
			taskID := ready[0]
			ready = ready[1:]
			task := tasksByID[taskID]

			if blockedByFailure[taskID] {
				res := domain.TaskResult{
					TaskID:      taskID,
					Status:      domain.TaskStatusSkipped,
					StartedAt:   time.Now().UTC(),
					FinishedAt:  time.Now().UTC(),
					Duration:    0,
					Error:       "skipped due to upstream failure",
					UpstreamRef: append([]string(nil), tasksByID[taskID].DependsOn...),
				}
				run.Results[taskID] = res
				completed++
				for _, child := range dependents[taskID] {
					remainingDeps[child]--
					blockedByFailure[child] = true
					if remainingDeps[child] == 0 {
						ready = append(ready, child)
					}
				}
				continue
			}

			shouldRun, skipReason := evaluateCondition(task, run.Results)
			if !shouldRun {
				res := domain.TaskResult{
					TaskID:      taskID,
					Status:      domain.TaskStatusSkipped,
					StartedAt:   time.Now().UTC(),
					FinishedAt:  time.Now().UTC(),
					Duration:    0,
					Error:       skipReason,
					UpstreamRef: append([]string(nil), task.DependsOn...),
				}
				run.Results[taskID] = res
				completed++
				for _, child := range dependents[taskID] {
					remainingDeps[child]--
					if remainingDeps[child] == 0 {
						ready = append(ready, child)
					}
				}
				continue
			}

			resolvedInput, err := resolveInputTemplates(task.Input, run.Results)
			if err != nil {
				res := domain.TaskResult{
					TaskID:      taskID,
					Status:      domain.TaskStatusFailed,
					StartedAt:   time.Now().UTC(),
					FinishedAt:  time.Now().UTC(),
					Duration:    0,
					Attempts:    1,
					Error:       err.Error(),
					UpstreamRef: append([]string(nil), task.DependsOn...),
				}
				run.Results[taskID] = res
				completed++
				for _, child := range dependents[taskID] {
					blockedByFailure[child] = true
					remainingDeps[child]--
					if remainingDeps[child] == 0 {
						ready = append(ready, child)
					}
				}
				if workflow.FailFast && !task.AllowFailure {
					failFastTriggered = true
					cancelBase()
				}
				continue
			}

			task.Input = resolvedInput
			if task.RunAfter > 0 {
				select {
				case <-runCtx.Done():
					break
				case <-time.After(task.RunAfter):
				}
			}

			startTask(taskID, task)
		}

		if active == 0 {
			if len(ready) == 0 {
				break
			}
			continue
		}

		select {
		case <-runCtx.Done():
			if failFastTriggered {
				run.Status = domain.RunStatusFailed
				run.Error = "fail-fast: stopped after critical task failure"
			} else {
				run.Status = domain.RunStatusCancelled
				run.Error = runCtx.Err().Error()
			}
			goto FINISH
		case execResult := <-resultCh:
			active--
			completed++

			run.Results[execResult.taskID] = execResult.result

			currentTask := tasksByID[execResult.taskID]
			failed := execResult.result.Status == domain.TaskStatusFailed
			if failed && !currentTask.AllowFailure {
				for _, child := range dependents[execResult.taskID] {
					blockedByFailure[child] = true
				}
				if workflow.FailFast {
					failFastTriggered = true
					cancelBase()
				}
			}

			for _, child := range dependents[execResult.taskID] {
				remainingDeps[child]--
				if remainingDeps[child] == 0 {
					ready = append(ready, child)
				}
			}
		}
	}

FINISH:
	markUnfinishedTasksAsSkipped(workflow.Tasks, run.Results, run.Error)
	run.FinishedAt = time.Now().UTC()
	run.Duration = run.FinishedAt.Sub(run.StartedAt)

	if run.Status == domain.RunStatusRunning {
		run.Status = deriveFinalStatus(run.Results)
		if run.Status == domain.RunStatusFailed {
			run.Error = firstTaskError(run.Results)
		}
	}

	if (run.Status == domain.RunStatusFailed || run.Status == domain.RunStatusCancelled) && workflow.CompensateOnFailure {
		run.CompensationResults = e.runCompensation(ctx, workflow, run)
		run.CompensationStatus = deriveCompensationStatus(run.CompensationResults)
	} else if run.Status == domain.RunStatusSuccess {
		run.CompensationStatus = domain.CompensationStatusNotRequired
	} else {
		run.CompensationStatus = domain.CompensationStatusSkipped
	}

	if e.store != nil {
		if err := e.store.SaveRun(ctx, run); err != nil {
			return run, err
		}
	}

	if e.metrics != nil {
		e.metrics.RecordRun(run)
	}
	span.SetAttributes(attribute.String("run.status", string(run.Status)))

	return run, nil
}

func deriveCompensationStatus(results []domain.TaskResult) domain.CompensationStatus {
	if len(results) == 0 {
		return domain.CompensationStatusSkipped
	}
	for _, result := range results {
		if result.Status == domain.TaskStatusFailed {
			return domain.CompensationStatusFailed
		}
	}
	return domain.CompensationStatusSuccess
}

func (e *Executor) runCompensation(ctx context.Context, workflow domain.Workflow, run domain.Run) []domain.TaskResult {
	eligible := make([]domain.TaskNode, 0)
	tasksByID := make(map[string]domain.TaskNode, len(workflow.Tasks))
	for _, task := range workflow.Tasks {
		tasksByID[task.ID] = task
	}

	for taskID, result := range run.Results {
		if result.Status != domain.TaskStatusSuccess {
			continue
		}
		task, ok := tasksByID[taskID]
		if !ok {
			continue
		}
		if strings.TrimSpace(task.Compensation.Action) == "" {
			continue
		}
		eligible = append(eligible, task)
	}

	sort.Slice(eligible, func(i, j int) bool {
		ri := run.Results[eligible[i].ID]
		rj := run.Results[eligible[j].ID]
		if ri.FinishedAt.Equal(rj.FinishedAt) {
			return eligible[i].ID > eligible[j].ID
		}
		return ri.FinishedAt.After(rj.FinishedAt)
	})

	results := make([]domain.TaskResult, 0, len(eligible))
	for _, task := range eligible {
		result := e.runCompensationTask(ctx, task, run.Results)
		results = append(results, result)
	}
	return results
}

func (e *Executor) runCompensationTask(ctx context.Context, task domain.TaskNode, allResults map[string]domain.TaskResult) domain.TaskResult {
	start := time.Now().UTC()
	res := domain.TaskResult{
		TaskID:      task.ID + ".compensation",
		Status:      domain.TaskStatusRunning,
		StartedAt:   start,
		Attempts:    1,
		UpstreamRef: append([]string(nil), task.DependsOn...),
	}

	action, ok := e.registry.Get(task.Compensation.Action)
	if !ok {
		res.Status = domain.TaskStatusFailed
		res.FinishedAt = time.Now().UTC()
		res.Duration = res.FinishedAt.Sub(start)
		res.Error = "compensation action not found: " + task.Compensation.Action
		return res
	}

	input, err := resolveInputTemplates(task.Compensation.Input, allResults)
	if err != nil {
		res.Status = domain.TaskStatusFailed
		res.FinishedAt = time.Now().UTC()
		res.Duration = res.FinishedAt.Sub(start)
		res.Error = err.Error()
		return res
	}

	runCtx := ctx
	cancel := func() {}
	if task.Compensation.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, task.Compensation.Timeout)
	}
	defer cancel()

	output, err := action.Execute(runCtx, input)
	if err != nil {
		res.Status = domain.TaskStatusFailed
		res.Error = err.Error()
	} else {
		res.Status = domain.TaskStatusSuccess
		res.Output = output
	}

	res.FinishedAt = time.Now().UTC()
	res.Duration = res.FinishedAt.Sub(start)
	return res
}

func deriveFinalStatus(results map[string]domain.TaskResult) domain.RunStatus {
	if len(results) == 0 {
		return domain.RunStatusFailed
	}

	failed := false
	for _, r := range results {
		if r.Status == domain.TaskStatusFailed {
			failed = true
			break
		}
	}
	if failed {
		return domain.RunStatusFailed
	}
	return domain.RunStatusSuccess
}

func firstTaskError(results map[string]domain.TaskResult) string {
	for _, r := range results {
		if r.Status == domain.TaskStatusFailed {
			if r.Error != "" {
				return r.Error
			}
			return "task failed without explicit error"
		}
	}
	return ""
}

func (e *Executor) runTask(ctx context.Context, task domain.TaskNode) domain.TaskResult {
	start := time.Now().UTC()
	res := domain.TaskResult{
		TaskID:    task.ID,
		Status:    domain.TaskStatusRunning,
		StartedAt: start,
	}

	action, ok := e.registry.Get(task.Action)
	if !ok {
		res.Status = domain.TaskStatusFailed
		res.FinishedAt = time.Now().UTC()
		res.Duration = res.FinishedAt.Sub(res.StartedAt)
		res.Error = fmt.Sprintf("action not found: %s", task.Action)
		return res
	}

	maxAttempts := task.RetryPolicy.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	backoff := task.RetryPolicy.BackoffBase
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		res.Attempts = attempt

		taskCtx := ctx
		cancel := func() {}
		if task.Timeout > 0 {
			taskCtx, cancel = context.WithTimeout(ctx, task.Timeout)
		}

		output, err := action.Execute(taskCtx, task.Input)
		cancel()

		if err == nil {
			res.Status = domain.TaskStatusSuccess
			res.Output = output
			res.FinishedAt = time.Now().UTC()
			res.Duration = res.FinishedAt.Sub(res.StartedAt)
			return res
		}

		lastErr = err
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			break
		}

		if attempt < maxAttempts {
			wait := backoff * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				lastErr = ctx.Err()
				attempt = maxAttempts
			case <-time.After(wait):
			}
		}
	}

	res.Status = domain.TaskStatusFailed
	res.Error = lastErr.Error()
	res.FinishedAt = time.Now().UTC()
	res.Duration = res.FinishedAt.Sub(res.StartedAt)
	return res
}

func evaluateCondition(task domain.TaskNode, results map[string]domain.TaskResult) (bool, string) {
	condition := strings.TrimSpace(task.Condition)
	if condition == "" || condition == "always" {
		return true, ""
	}

	if strings.HasPrefix(condition, "on_success:") {
		taskID := strings.TrimSpace(strings.TrimPrefix(condition, "on_success:"))
		res, ok := results[taskID]
		if !ok || res.Status != domain.TaskStatusSuccess {
			return false, "condition not met: " + condition
		}
		return true, ""
	}

	if strings.HasPrefix(condition, "on_failed:") {
		taskID := strings.TrimSpace(strings.TrimPrefix(condition, "on_failed:"))
		res, ok := results[taskID]
		if !ok || res.Status != domain.TaskStatusFailed {
			return false, "condition not met: " + condition
		}
		return true, ""
	}

	if strings.HasPrefix(condition, "output_contains:") {
		parts := strings.SplitN(strings.TrimPrefix(condition, "output_contains:"), ":", 2)
		if len(parts) != 2 {
			return false, "invalid condition syntax: " + condition
		}
		taskID := strings.TrimSpace(parts[0])
		needle := parts[1]
		res, ok := results[taskID]
		if !ok || !strings.Contains(res.Output, needle) {
			return false, "condition not met: " + condition
		}
		return true, ""
	}

	return false, "unsupported condition: " + condition
}

func resolveInputTemplates(input map[string]string, results map[string]domain.TaskResult) (map[string]string, error) {
	resolved := make(map[string]string, len(input))
	for k, v := range input {
		next, err := resolveValue(v, results)
		if err != nil {
			return nil, err
		}
		resolved[k] = next
	}
	return resolved, nil
}

func resolveValue(raw string, results map[string]domain.TaskResult) (string, error) {
	const (
		prefix = "${"
		suffix = "}"
	)

	output := raw
	for {
		start := strings.Index(output, prefix)
		if start < 0 {
			break
		}
		endRelative := strings.Index(output[start:], suffix)
		if endRelative < 0 {
			return "", fmt.Errorf("unclosed template in input: %s", raw)
		}
		end := start + endRelative

		token := strings.TrimSpace(output[start+len(prefix) : end])
		parts := strings.Split(token, ".")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid template token %q", token)
		}

		taskID := strings.TrimSpace(parts[0])
		field := strings.TrimSpace(parts[1])
		res, ok := results[taskID]
		if !ok {
			return "", fmt.Errorf("template references unknown task %q", taskID)
		}

		replacement := ""
		switch field {
		case "output":
			replacement = res.Output
		case "status":
			replacement = string(res.Status)
		case "error":
			replacement = res.Error
		default:
			return "", fmt.Errorf("unsupported template field %q", field)
		}

		output = output[:start] + replacement + output[end+len(suffix):]
	}

	return output, nil
}

func markUnfinishedTasksAsSkipped(tasks []domain.TaskNode, results map[string]domain.TaskResult, reason string) {
	if reason == "" {
		reason = "not executed"
	}
	now := time.Now().UTC()
	for _, t := range tasks {
		if _, ok := results[t.ID]; ok {
			continue
		}
		results[t.ID] = domain.TaskResult{
			TaskID:      t.ID,
			Status:      domain.TaskStatusSkipped,
			StartedAt:   now,
			FinishedAt:  now,
			Duration:    0,
			Error:       reason,
			UpstreamRef: append([]string(nil), t.DependsOn...),
		}
	}
}
