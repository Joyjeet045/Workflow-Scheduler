package tests

import (
	"os"
	"path/filepath"
	"testing"

	"workflowscheduler/pkg/utils"
)

func TestLoadWorkflowFromYAML(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "workflow.yaml")

	yamlContent := `id: wf-yaml
name: YAML Workflow
version: 1.0.0
maxConcurrency: 2
failFast: true
compensateOnFailure: true
timeout: 20s
tasks:
  - id: t1
    name: First
    action: print
    input:
      message: hello
    retryPolicy:
      maxAttempts: 2
      backoffBase: 100ms
  - id: t2
    name: Second
    action: print
    dependsOn: [t1]
    condition: on_success:t1
    runAfter: 50ms
    timeout: 1s
    retryPolicy:
      maxAttempts: 1
      backoffBase: 100ms
    compensation:
      action: print
      input:
        message: undo
      timeout: 200ms
`

	if err := os.WriteFile(path, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}

	workflow, err := utils.LoadWorkflowFromFile(path)
	if err != nil {
		t.Fatalf("load yaml workflow: %v", err)
	}

	if workflow.ID != "wf-yaml" {
		t.Fatalf("unexpected workflow ID: %s", workflow.ID)
	}
	if !workflow.FailFast {
		t.Fatalf("expected failFast true")
	}
	if !workflow.CompensateOnFailure {
		t.Fatalf("expected compensateOnFailure true")
	}
	if len(workflow.Tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(workflow.Tasks))
	}
	if workflow.Tasks[1].Condition != "on_success:t1" {
		t.Fatalf("unexpected condition: %s", workflow.Tasks[1].Condition)
	}
	if workflow.Tasks[1].Compensation.Action != "print" {
		t.Fatalf("unexpected compensation action: %s", workflow.Tasks[1].Compensation.Action)
	}
}
