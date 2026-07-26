package telemetry

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"workflowscheduler/internal/domain"
)

type Collector struct {
	totalRuns     atomic.Uint64
	runsSuccess   atomic.Uint64
	runsFailed    atomic.Uint64
	runsCancelled atomic.Uint64
	totalTasks    atomic.Uint64
	tasksSuccess  atomic.Uint64
	tasksFailed   atomic.Uint64
	tasksSkipped  atomic.Uint64
	compSuccess   atomic.Uint64
	compFailed    atomic.Uint64
	compSkipped   atomic.Uint64
	workerLeased  atomic.Uint64
	workerSuccess atomic.Uint64
	workerRetry   atomic.Uint64
	workerDead    atomic.Uint64
	workerFailed  atomic.Uint64

	mu           sync.Mutex
	runDurationS float64
	runCount     uint64
	buckets      []float64
	bucketCounts []uint64
	httpByKey    map[string]uint64
	queueDepth   map[string]int
}

func NewCollector() *Collector {
	return &Collector{
		httpByKey:    make(map[string]uint64),
		queueDepth:   make(map[string]int),
		buckets:      []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		bucketCounts: make([]uint64, 9),
	}
}

func (c *Collector) RecordRun(run domain.Run) {
	c.totalRuns.Add(1)
	switch run.Status {
	case domain.RunStatusSuccess:
		c.runsSuccess.Add(1)
	case domain.RunStatusFailed:
		c.runsFailed.Add(1)
	case domain.RunStatusCancelled:
		c.runsCancelled.Add(1)
	}

	for _, result := range run.Results {
		c.totalTasks.Add(1)
		switch result.Status {
		case domain.TaskStatusSuccess:
			c.tasksSuccess.Add(1)
		case domain.TaskStatusFailed:
			c.tasksFailed.Add(1)
		case domain.TaskStatusSkipped:
			c.tasksSkipped.Add(1)
		}
	}

	for _, result := range run.CompensationResults {
		switch result.Status {
		case domain.TaskStatusSuccess:
			c.compSuccess.Add(1)
		case domain.TaskStatusFailed:
			c.compFailed.Add(1)
		case domain.TaskStatusSkipped:
			c.compSkipped.Add(1)
		}
	}

	c.mu.Lock()
	durationS := run.Duration.Seconds()
	c.runDurationS += durationS
	c.runCount++
	for i, upper := range c.buckets {
		if durationS <= upper {
			c.bucketCounts[i]++
		}
	}
	c.mu.Unlock()
}

func (c *Collector) RecordHTTPRequest(method string, route string, statusCode int) {
	key := fmt.Sprintf("%s|%s|%d", method, route, statusCode)
	c.mu.Lock()
	c.httpByKey[key]++
	c.mu.Unlock()
}

func (c *Collector) RecordWorkerLease() {
	c.workerLeased.Add(1)
}

func (c *Collector) RecordWorkerSuccess() {
	c.workerSuccess.Add(1)
}

func (c *Collector) RecordWorkerRetry() {
	c.workerRetry.Add(1)
}

func (c *Collector) RecordWorkerDeadLetter() {
	c.workerDead.Add(1)
}

func (c *Collector) RecordWorkerFailure() {
	c.workerFailed.Add(1)
}

func (c *Collector) SetQueueDepthByStatus(counts map[domain.RunJobStatus]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueDepth = make(map[string]int, len(counts))
	for status, count := range counts {
		c.queueDepth[string(status)] = count
	}
}

func (c *Collector) Prometheus() string {
	builder := &strings.Builder{}
	writeMetric := func(line string) {
		builder.WriteString(line)
		builder.WriteByte('\n')
	}

	writeMetric("# HELP orchestrator_runs_total Total workflow runs")
	writeMetric("# TYPE orchestrator_runs_total counter")
	writeMetric(fmt.Sprintf("orchestrator_runs_total %d", c.totalRuns.Load()))
	writeMetric(fmt.Sprintf("orchestrator_runs_success_total %d", c.runsSuccess.Load()))
	writeMetric(fmt.Sprintf("orchestrator_runs_failed_total %d", c.runsFailed.Load()))
	writeMetric(fmt.Sprintf("orchestrator_runs_cancelled_total %d", c.runsCancelled.Load()))

	writeMetric("# HELP orchestrator_tasks_total Total task executions")
	writeMetric("# TYPE orchestrator_tasks_total counter")
	writeMetric(fmt.Sprintf("orchestrator_tasks_total %d", c.totalTasks.Load()))
	writeMetric(fmt.Sprintf("orchestrator_tasks_success_total %d", c.tasksSuccess.Load()))
	writeMetric(fmt.Sprintf("orchestrator_tasks_failed_total %d", c.tasksFailed.Load()))
	writeMetric(fmt.Sprintf("orchestrator_tasks_skipped_total %d", c.tasksSkipped.Load()))

	writeMetric("# HELP orchestrator_compensation_total Compensation task outcomes")
	writeMetric("# TYPE orchestrator_compensation_total counter")
	writeMetric(fmt.Sprintf("orchestrator_compensation_success_total %d", c.compSuccess.Load()))
	writeMetric(fmt.Sprintf("orchestrator_compensation_failed_total %d", c.compFailed.Load()))
	writeMetric(fmt.Sprintf("orchestrator_compensation_skipped_total %d", c.compSkipped.Load()))

	c.mu.Lock()
	runDurationS := c.runDurationS
	runCount := c.runCount
	buckets := append([]float64(nil), c.buckets...)
	bucketCounts := append([]uint64(nil), c.bucketCounts...)
	httpCopy := make(map[string]uint64, len(c.httpByKey))
	for k, v := range c.httpByKey {
		httpCopy[k] = v
	}
	queueDepthCopy := make(map[string]int, len(c.queueDepth))
	for k, v := range c.queueDepth {
		queueDepthCopy[k] = v
	}
	c.mu.Unlock()

	writeMetric("# HELP orchestrator_run_duration_seconds Workflow run duration histogram")
	writeMetric("# TYPE orchestrator_run_duration_seconds histogram")
	for i, upper := range buckets {
		writeMetric(fmt.Sprintf("orchestrator_run_duration_seconds_bucket{le=\"%g\"} %d", upper, bucketCounts[i]))
	}
	writeMetric(fmt.Sprintf("orchestrator_run_duration_seconds_bucket{le=\"+Inf\"} %d", runCount))

	writeMetric("# HELP orchestrator_run_duration_seconds_sum Total run duration in seconds")
	writeMetric("# TYPE orchestrator_run_duration_seconds_sum counter")
	writeMetric(fmt.Sprintf("orchestrator_run_duration_seconds_sum %f", runDurationS))
	writeMetric("# HELP orchestrator_run_duration_seconds_count Number of completed runs")
	writeMetric("# TYPE orchestrator_run_duration_seconds_count counter")
	writeMetric(fmt.Sprintf("orchestrator_run_duration_seconds_count %d", runCount))

	writeMetric("# HELP orchestrator_http_requests_total API request count")
	writeMetric("# TYPE orchestrator_http_requests_total counter")
	keys := make([]string, 0, len(httpCopy))
	for k := range httpCopy {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts := strings.SplitN(k, "|", 3)
		if len(parts) != 3 {
			continue
		}
		writeMetric(fmt.Sprintf(
			"orchestrator_http_requests_total{method=\"%s\",route=\"%s\",status=\"%s\"} %d",
			parts[0], escape(parts[1]), parts[2], httpCopy[k],
		))
	}

	writeMetric("# HELP orchestrator_worker_leases_total Queue jobs leased by workers")
	writeMetric("# TYPE orchestrator_worker_leases_total counter")
	writeMetric(fmt.Sprintf("orchestrator_worker_leases_total %d", c.workerLeased.Load()))
	writeMetric("# HELP orchestrator_worker_success_total Queue jobs completed successfully")
	writeMetric("# TYPE orchestrator_worker_success_total counter")
	writeMetric(fmt.Sprintf("orchestrator_worker_success_total %d", c.workerSuccess.Load()))
	writeMetric("# HELP orchestrator_worker_retry_total Queue jobs retried by workers")
	writeMetric("# TYPE orchestrator_worker_retry_total counter")
	writeMetric(fmt.Sprintf("orchestrator_worker_retry_total %d", c.workerRetry.Load()))
	writeMetric("# HELP orchestrator_worker_dead_letter_total Queue jobs moved to dead letter")
	writeMetric("# TYPE orchestrator_worker_dead_letter_total counter")
	writeMetric(fmt.Sprintf("orchestrator_worker_dead_letter_total %d", c.workerDead.Load()))
	writeMetric("# HELP orchestrator_worker_failure_total Queue job processing failures")
	writeMetric("# TYPE orchestrator_worker_failure_total counter")
	writeMetric(fmt.Sprintf("orchestrator_worker_failure_total %d", c.workerFailed.Load()))

	writeMetric("# HELP orchestrator_queue_depth Current queue depth by status")
	writeMetric("# TYPE orchestrator_queue_depth gauge")
	queueStatuses := make([]string, 0, len(queueDepthCopy))
	for status := range queueDepthCopy {
		queueStatuses = append(queueStatuses, status)
	}
	sort.Strings(queueStatuses)
	for _, status := range queueStatuses {
		writeMetric(fmt.Sprintf("orchestrator_queue_depth{status=\"%s\"} %d", escape(status), queueDepthCopy[status]))
	}

	return builder.String()
}

func escape(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return s
}
