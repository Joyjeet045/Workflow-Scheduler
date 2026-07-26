package engine

import (
	"errors"
	"fmt"
	"strings"

	"workflowscheduler/internal/domain"
)

func ValidateWorkflow(workflow domain.Workflow) error {
	if workflow.ID == "" {
		return errors.New("workflow id is required")
	}
	if len(workflow.Tasks) == 0 {
		return errors.New("workflow must contain at least one task")
	}

	seen := map[string]struct{}{}
	tasksByID := map[string]domain.TaskNode{}
	for _, t := range workflow.Tasks {
		if t.ID == "" {
			return errors.New("task id is required")
		}
		if _, exists := seen[t.ID]; exists {
			return fmt.Errorf("duplicate task id: %s", t.ID)
		}
		seen[t.ID] = struct{}{}
		tasksByID[t.ID] = t
	}

	for _, t := range workflow.Tasks {
		if t.RunAfter < 0 {
			return fmt.Errorf("task %s has negative runAfter", t.ID)
		}
		if t.Compensation.Action == "" && len(t.Compensation.Input) > 0 {
			return fmt.Errorf("task %s has compensation input without compensation action", t.ID)
		}
		if t.Compensation.Timeout < 0 {
			return fmt.Errorf("task %s has negative compensation timeout", t.ID)
		}
		for _, dep := range t.DependsOn {
			if _, ok := tasksByID[dep]; !ok {
				return fmt.Errorf("task %s depends on unknown task %s", t.ID, dep)
			}
		}
		if err := validateConditionReference(t, tasksByID); err != nil {
			return err
		}
	}

	if hasCycle(workflow.Tasks) {
		return errors.New("workflow contains cyclic dependencies")
	}

	return nil
}

func validateConditionReference(task domain.TaskNode, tasksByID map[string]domain.TaskNode) error {
	condition := strings.TrimSpace(task.Condition)
	if condition == "" || condition == "always" {
		return nil
	}

	requireTaskExists := func(prefix string) error {
		taskID := strings.TrimSpace(strings.TrimPrefix(condition, prefix))
		if taskID == "" {
			return fmt.Errorf("task %s has invalid condition syntax", task.ID)
		}
		if _, ok := tasksByID[taskID]; !ok {
			return fmt.Errorf("task %s condition references unknown task %s", task.ID, taskID)
		}
		return nil
	}

	if strings.HasPrefix(condition, "on_success:") {
		return requireTaskExists("on_success:")
	}
	if strings.HasPrefix(condition, "on_failed:") {
		return requireTaskExists("on_failed:")
	}
	if strings.HasPrefix(condition, "output_contains:") {
		parts := strings.SplitN(strings.TrimPrefix(condition, "output_contains:"), ":", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return fmt.Errorf("task %s has invalid condition syntax", task.ID)
		}
		if _, ok := tasksByID[strings.TrimSpace(parts[0])]; !ok {
			return fmt.Errorf("task %s condition references unknown task %s", task.ID, strings.TrimSpace(parts[0]))
		}
		return nil
	}

	return fmt.Errorf("task %s has unsupported condition %q", task.ID, condition)
}

func hasCycle(tasks []domain.TaskNode) bool {
	adj := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		for _, dep := range t.DependsOn {
			adj[dep] = append(adj[dep], t.ID)
		}
		if _, exists := adj[t.ID]; !exists {
			adj[t.ID] = nil
		}
	}

	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)
	state := map[string]int{}

	var dfs func(string) bool
	dfs = func(node string) bool {
		if state[node] == visiting {
			return true
		}
		if state[node] == visited {
			return false
		}

		state[node] = visiting
		for _, next := range adj[node] {
			if dfs(next) {
				return true
			}
		}
		state[node] = visited
		return false
	}

	for n := range adj {
		if state[n] == unvisited && dfs(n) {
			return true
		}
	}
	return false
}
