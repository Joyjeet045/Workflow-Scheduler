package tests

import (
	"testing"

	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/engine"
)

func TestValidateWorkflowDetectsCycle(t *testing.T) {
	workflow := domain.Workflow{
		ID: "wf",
		Tasks: []domain.TaskNode{
			{ID: "a", DependsOn: []string{"c"}},
			{ID: "b", DependsOn: []string{"a"}},
			{ID: "c", DependsOn: []string{"b"}},
		},
	}

	err := engine.ValidateWorkflow(workflow)
	if err == nil {
		t.Fatal("expected cycle validation error, got nil")
	}
}

func TestValidateWorkflowHappyPath(t *testing.T) {
	workflow := domain.Workflow{
		ID: "wf",
		Tasks: []domain.TaskNode{
			{ID: "a"},
			{ID: "b", DependsOn: []string{"a"}},
		},
	}

	if err := engine.ValidateWorkflow(workflow); err != nil {
		t.Fatalf("expected valid workflow, got error: %v", err)
	}
}

func TestValidateWorkflowCompensationInputWithoutAction(t *testing.T) {
	workflow := domain.Workflow{
		ID: "wf",
		Tasks: []domain.TaskNode{
			{
				ID:     "a",
				Action: "print",
				Compensation: domain.CompensationSpec{
					Input: map[string]string{"message": "undo"},
				},
			},
		},
	}

	err := engine.ValidateWorkflow(workflow)
	if err == nil {
		t.Fatal("expected invalid compensation configuration error")
	}
}
