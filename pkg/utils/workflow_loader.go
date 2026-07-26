package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"workflowscheduler/internal/domain"
)

type workflowFile struct {
	ID                  string       `json:"id" yaml:"id"`
	Name                string       `json:"name" yaml:"name"`
	Description         string       `json:"description" yaml:"description"`
	Version             string       `json:"version" yaml:"version"`
	MaxConcurrency      int          `json:"maxConcurrency" yaml:"maxConcurrency"`
	FailFast            bool         `json:"failFast" yaml:"failFast"`
	CompensateOnFailure bool         `json:"compensateOnFailure" yaml:"compensateOnFailure"`
	Timeout             string       `json:"timeout" yaml:"timeout"`
	Tasks               []taskConfig `json:"tasks" yaml:"tasks"`
}

type taskConfig struct {
	ID           string             `json:"id" yaml:"id"`
	Name         string             `json:"name" yaml:"name"`
	Action       string             `json:"action" yaml:"action"`
	Input        map[string]string  `json:"input" yaml:"input"`
	DependsOn    []string           `json:"dependsOn" yaml:"dependsOn"`
	Condition    string             `json:"condition" yaml:"condition"`
	RunAfter     string             `json:"runAfter" yaml:"runAfter"`
	Timeout      string             `json:"timeout" yaml:"timeout"`
	AllowFailure bool               `json:"allowFailure" yaml:"allowFailure"`
	RetryPolicy  retryPolicyConfig  `json:"retryPolicy" yaml:"retryPolicy"`
	Compensation compensationConfig `json:"compensation" yaml:"compensation"`
}

type retryPolicyConfig struct {
	MaxAttempts int    `json:"maxAttempts" yaml:"maxAttempts"`
	BackoffBase string `json:"backoffBase" yaml:"backoffBase"`
}

type compensationConfig struct {
	Action  string            `json:"action" yaml:"action"`
	Input   map[string]string `json:"input" yaml:"input"`
	Timeout string            `json:"timeout" yaml:"timeout"`
}

func LoadWorkflowFromFile(path string) (domain.Workflow, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return domain.Workflow{}, err
	}

	var cfg workflowFile
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(content, &cfg); err != nil {
			return domain.Workflow{}, fmt.Errorf("decode workflow yaml: %w", err)
		}
	default:
		if err := json.Unmarshal(content, &cfg); err != nil {
			return domain.Workflow{}, fmt.Errorf("decode workflow json: %w", err)
		}
	}

	w := domain.Workflow{
		ID:                  cfg.ID,
		Name:                cfg.Name,
		Description:         cfg.Description,
		Version:             cfg.Version,
		MaxConcurrency:      cfg.MaxConcurrency,
		FailFast:            cfg.FailFast,
		CompensateOnFailure: cfg.CompensateOnFailure,
	}

	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return domain.Workflow{}, fmt.Errorf("invalid workflow timeout: %w", err)
		}
		w.Timeout = t
	}

	w.Tasks = make([]domain.TaskNode, 0, len(cfg.Tasks))
	for _, task := range cfg.Tasks {
		node := domain.TaskNode{
			ID:           task.ID,
			Name:         task.Name,
			Action:       task.Action,
			Input:        task.Input,
			DependsOn:    task.DependsOn,
			Condition:    task.Condition,
			AllowFailure: task.AllowFailure,
			RetryPolicy: domain.RetryPolicy{
				MaxAttempts: task.RetryPolicy.MaxAttempts,
			},
			Compensation: domain.CompensationSpec{
				Action: task.Compensation.Action,
				Input:  task.Compensation.Input,
			},
		}

		if task.RunAfter != "" {
			d, err := time.ParseDuration(task.RunAfter)
			if err != nil {
				return domain.Workflow{}, fmt.Errorf("invalid runAfter for %s: %w", task.ID, err)
			}
			node.RunAfter = d
		}

		if task.Timeout != "" {
			t, err := time.ParseDuration(task.Timeout)
			if err != nil {
				return domain.Workflow{}, fmt.Errorf("invalid task timeout for %s: %w", task.ID, err)
			}
			node.Timeout = t
		}
		if task.RetryPolicy.BackoffBase != "" {
			b, err := time.ParseDuration(task.RetryPolicy.BackoffBase)
			if err != nil {
				return domain.Workflow{}, fmt.Errorf("invalid backoffBase for %s: %w", task.ID, err)
			}
			node.RetryPolicy.BackoffBase = b
		}
		if task.Compensation.Timeout != "" {
			ct, err := time.ParseDuration(task.Compensation.Timeout)
			if err != nil {
				return domain.Workflow{}, fmt.Errorf("invalid compensation timeout for %s: %w", task.ID, err)
			}
			node.Compensation.Timeout = ct
		}

		w.Tasks = append(w.Tasks, node)
	}

	return w, nil
}
