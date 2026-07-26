package domain

import "time"

type TaskStatus string

const (
	TaskStatusPending TaskStatus = "PENDING"
	TaskStatusRunning TaskStatus = "RUNNING"
	TaskStatusSuccess TaskStatus = "SUCCESS"
	TaskStatusFailed  TaskStatus = "FAILED"
	TaskStatusSkipped TaskStatus = "SKIPPED"
)

type RunStatus string

const (
	RunStatusPending   RunStatus = "PENDING"
	RunStatusRunning   RunStatus = "RUNNING"
	RunStatusSuccess   RunStatus = "SUCCESS"
	RunStatusFailed    RunStatus = "FAILED"
	RunStatusCancelled RunStatus = "CANCELLED"
)

type CompensationStatus string

const (
	CompensationStatusNotRequired CompensationStatus = "NOT_REQUIRED"
	CompensationStatusSuccess     CompensationStatus = "SUCCESS"
	CompensationStatusFailed      CompensationStatus = "FAILED"
	CompensationStatusSkipped     CompensationStatus = "SKIPPED"
)

type RetryPolicy struct {
	MaxAttempts int           `json:"maxAttempts"`
	BackoffBase time.Duration `json:"backoffBase"`
}

type CompensationSpec struct {
	Action  string            `json:"action"`
	Input   map[string]string `json:"input"`
	Timeout time.Duration     `json:"timeout"`
}

type TaskNode struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Action       string            `json:"action"`
	Input        map[string]string `json:"input"`
	DependsOn    []string          `json:"dependsOn"`
	Condition    string            `json:"condition"`
	RunAfter     time.Duration     `json:"runAfter"`
	Timeout      time.Duration     `json:"timeout"`
	RetryPolicy  RetryPolicy       `json:"retryPolicy"`
	AllowFailure bool              `json:"allowFailure"`
	Compensation CompensationSpec  `json:"compensation"`
}

type Workflow struct {
	ID                  string        `json:"id"`
	Name                string        `json:"name"`
	Description         string        `json:"description"`
	Version             string        `json:"version"`
	MaxConcurrency      int           `json:"maxConcurrency"`
	FailFast            bool          `json:"failFast"`
	CompensateOnFailure bool          `json:"compensateOnFailure"`
	Timeout             time.Duration `json:"timeout"`
	Tasks               []TaskNode    `json:"tasks"`
}

type TaskResult struct {
	TaskID      string        `json:"taskId"`
	Status      TaskStatus    `json:"status"`
	StartedAt   time.Time     `json:"startedAt"`
	FinishedAt  time.Time     `json:"finishedAt"`
	Attempts    int           `json:"attempts"`
	Duration    time.Duration `json:"duration"`
	Output      string        `json:"output"`
	Error       string        `json:"error"`
	UpstreamRef []string      `json:"upstreamRef"`
}

type Run struct {
	ID                  string                `json:"id"`
	WorkflowID          string                `json:"workflowId"`
	RequestID           string                `json:"requestId"`
	Status              RunStatus             `json:"status"`
	CompensationStatus  CompensationStatus    `json:"compensationStatus"`
	CompensationResults []TaskResult          `json:"compensationResults"`
	StartedAt           time.Time             `json:"startedAt"`
	FinishedAt          time.Time             `json:"finishedAt"`
	Duration            time.Duration         `json:"duration"`
	Results             map[string]TaskResult `json:"results"`
	Error               string                `json:"error"`
}

type ScheduleConfig struct {
	WorkflowID   string        `json:"workflowId"`
	Interval     time.Duration `json:"interval"`
	Cron         string        `json:"cron"`
	RunOnStartup bool          `json:"runOnStartup"`
	AllowOverlap bool          `json:"allowOverlap"`
}

type RunJobStatus string

const (
	RunJobStatusQueued    RunJobStatus = "QUEUED"
	RunJobStatusLeased    RunJobStatus = "LEASED"
	RunJobStatusSucceeded RunJobStatus = "SUCCEEDED"
	RunJobStatusFailed    RunJobStatus = "FAILED"
	RunJobStatusDead      RunJobStatus = "DEAD_LETTER"
	RunJobStatusCancelled RunJobStatus = "CANCELLED"
)

type RunQueueJob struct {
	ID           string       `json:"id"`
	WorkflowID   string       `json:"workflowId"`
	RunID        string       `json:"runId"`
	RequestID    string       `json:"requestId"`
	Status       RunJobStatus `json:"status"`
	Attempts     int          `json:"attempts"`
	MaxAttempts  int          `json:"maxAttempts"`
	LastError    string       `json:"lastError"`
	LeaseOwner   string       `json:"leaseOwner"`
	LeaseUntil   time.Time    `json:"leaseUntil"`
	AvailableAt  time.Time    `json:"availableAt"`
	ReplayCount  int          `json:"replayCount"`
	ReplayedBy   string       `json:"replayedBy"`
	ReplayReason string       `json:"replayReason"`
	ReplayedAt   time.Time    `json:"replayedAt"`
	CreatedAt    time.Time    `json:"createdAt"`
	UpdatedAt    time.Time    `json:"updatedAt"`
}

type RunJobTransition struct {
	JobID      string       `json:"jobId"`
	FromStatus RunJobStatus `json:"fromStatus"`
	ToStatus   RunJobStatus `json:"toStatus"`
	Reason     string       `json:"reason"`
	Actor      string       `json:"actor"`
	CreatedAt  time.Time    `json:"createdAt"`
}
