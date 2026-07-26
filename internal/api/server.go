package api

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"workflowscheduler/internal/domain"
	"workflowscheduler/internal/engine"
	"workflowscheduler/internal/scheduler"
	"workflowscheduler/internal/telemetry"
)

type Server struct {
	addr      string
	store     domain.WorkflowStore
	queue     domain.RunQueue
	executor  *engine.Executor
	scheduler *scheduler.Service
	metrics   *telemetry.Collector
	authRead  map[string]struct{}
	authWrite map[string]struct{}
	authOn    bool
	httpSrv   *http.Server
	startedAt time.Time

	queueRetentionStatus    string
	queueRetentionOlderThan time.Duration
	queueRetentionInterval  time.Duration
	queueRetentionBatch     int
}

type bulkFailureDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type listResponse[T any] struct {
	Items    []T `json:"items"`
	Total    int `json:"total"`
	Limit    int `json:"limit"`
	Offset   int `json:"offset"`
	Returned int `json:"returned"`
}

type bulkResult struct {
	ResultCode    string                       `json:"resultCode"`
	Failed        map[string]string            `json:"failed"`
	FailedDetails map[string]bulkFailureDetail `json:"failedDetails"`
}

type bulkCancelResponse struct {
	Cancelled []domain.RunQueueJob `json:"cancelled"`
	bulkResult
}

type bulkRequeueResponse struct {
	Requeued []domain.RunQueueJob `json:"requeued"`
	bulkResult
}

type purgeResponse struct {
	Deleted    int    `json:"deleted"`
	ResultCode string `json:"resultCode"`
	DryRun     *bool  `json:"dryRun,omitempty"`
	Matched    *int   `json:"matched,omitempty"`
}

func NewServer(addr string, authConfig string, store domain.WorkflowStore, queue domain.RunQueue, executor *engine.Executor, schedulerService *scheduler.Service, metricsCollector *telemetry.Collector) *Server {
	if addr == "" {
		addr = ":8080"
	}
	if metricsCollector == nil {
		metricsCollector = telemetry.NewCollector()
	}

	readTokens, writeTokens := parseAuthConfig(authConfig)

	s := &Server{
		addr:      addr,
		store:     store,
		queue:     queue,
		executor:  executor,
		scheduler: schedulerService,
		metrics:   metricsCollector,
		authRead:  readTokens,
		authWrite: writeTokens,
		authOn:    len(readTokens) > 0 || len(writeTokens) > 0,
		startedAt: time.Now().UTC(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.withMiddleware("/health", false, s.handleHealth))
	mux.HandleFunc("/ready", s.withMiddleware("/ready", false, s.handleReady))
	mux.HandleFunc("/metrics", s.withMiddleware("/metrics", false, s.handleMetrics))
	mux.HandleFunc("/workflows", s.withMiddleware("/workflows", true, s.handleWorkflows))
	mux.HandleFunc("/workflows/", s.withMiddleware("/workflows/", true, s.handleWorkflowByID))
	mux.HandleFunc("/runs", s.withMiddleware("/runs", true, s.handleRuns))
	mux.HandleFunc("/runs/", s.withMiddleware("/runs/", true, s.handleRunByID))
	mux.HandleFunc("/jobs", s.withMiddleware("/jobs", true, s.handleJobs))
	mux.HandleFunc("/jobs/stats", s.withMiddleware("/jobs/stats", true, s.handleJobStats))
	mux.HandleFunc("/jobs/history/", s.withMiddleware("/jobs/history/", true, s.handleJobHistoryByID))
	mux.HandleFunc("/jobs/", s.withMiddleware("/jobs/", true, s.handleJobByID))
	mux.HandleFunc("/jobs/dead-letter", s.withMiddleware("/jobs/dead-letter", true, s.handleDeadLetterJobs))
	mux.HandleFunc("/jobs/purge", s.withMiddleware("/jobs/purge", true, s.handleJobsPurge))
	mux.HandleFunc("/jobs/cancel", s.withMiddleware("/jobs/cancel", true, s.handleJobsBulkCancel))
	mux.HandleFunc("/jobs/requeue", s.withMiddleware("/jobs/requeue", true, s.handleJobsBulkRequeue))
	mux.HandleFunc("/jobs/requeue/", s.withMiddleware("/jobs/requeue/", true, s.handleJobRequeueByID))
	mux.HandleFunc("/schedules", s.withMiddleware("/schedules", true, s.handleSchedules))
	mux.HandleFunc("/schedules/", s.withMiddleware("/schedules/", true, s.handleScheduleByWorkflowID))
	mux.HandleFunc("/schedules/pause/", s.withMiddleware("/schedules/pause/", true, s.handleSchedulePauseByWorkflowID))
	mux.HandleFunc("/schedules/resume/", s.withMiddleware("/schedules/resume/", true, s.handleScheduleResumeByWorkflowID))

	s.httpSrv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

func (s *Server) withMiddleware(route string, requireAuth bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if requireAuth && !s.isAuthorized(r) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			if s.metrics != nil {
				s.metrics.RecordHTTPRequest(r.Method, route, http.StatusUnauthorized)
			}
			return
		}

		writer := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next(writer, r)
		if s.metrics != nil {
			s.metrics.RecordHTTPRequest(r.Method, route, writer.statusCode)
		}
	}
}

func (s *Server) isAuthorized(r *http.Request) bool {
	if !s.authOn {
		return true
	}
	token := extractToken(r)
	if token == "" {
		return false
	}

	_, canRead := s.authRead[token]
	_, canWrite := s.authWrite[token]
	if isWriteMethod(r.Method) {
		return canWrite
	}
	if canRead || canWrite {
		return true
	}
	return false
}

func isWriteMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}

func extractToken(r *http.Request) string {
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		return strings.TrimSpace(authHeader[len("Bearer "):])
	}
	return strings.TrimSpace(r.Header.Get("X-API-Token"))
}

func parseAuthConfig(raw string) (map[string]struct{}, map[string]struct{}) {
	read := map[string]struct{}{}
	write := map[string]struct{}{}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return read, write
	}

	if !strings.Contains(trimmed, ":") {
		read[trimmed] = struct{}{}
		write[trimmed] = struct{}{}
		return read, write
	}

	parts := strings.Split(trimmed, ",")
	for _, part := range parts {
		segment := strings.TrimSpace(part)
		if segment == "" {
			continue
		}
		kv := strings.SplitN(segment, ":", 2)
		if len(kv) != 2 {
			continue
		}
		scope := strings.ToLower(strings.TrimSpace(kv[0]))
		token := strings.TrimSpace(kv[1])
		if token == "" {
			continue
		}
		switch scope {
		case "read":
			read[token] = struct{}{}
		case "write":
			write[token] = struct{}{}
		case "admin":
			read[token] = struct{}{}
			write[token] = struct{}{}
		}
	}
	return read, write
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	if s.metrics == nil {
		_, _ = w.Write([]byte(""))
		return
	}
	if s.queue != nil && s.metrics != nil {
		if counts, err := s.queue.CountRunJobsByStatus(r.Context()); err == nil {
			s.metrics.SetQueueDepthByStatus(counts)
		}
	}
	_, _ = w.Write([]byte(s.metrics.Prometheus()))
}

func (s *Server) Start(ctx context.Context) error {
	if s.queue != nil && s.queueRetentionInterval > 0 {
		go s.runQueueRetentionLoop(ctx)
	}

	errCh := make(chan error, 1)
	go func() {
		err := s.httpSrv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpSrv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *Server) SetQueueRetention(status string, olderThan time.Duration, interval time.Duration, batch int) {
	s.queueRetentionStatus = strings.TrimSpace(status)
	s.queueRetentionOlderThan = olderThan
	s.queueRetentionInterval = interval
	s.queueRetentionBatch = batch
}

func (s *Server) runQueueRetentionLoop(ctx context.Context) {
	ticker := time.NewTicker(s.queueRetentionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.queue.PurgeRunJobs(ctx, s.queueRetentionStatus, s.queueRetentionOlderThan, s.queueRetentionBatch)
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	resp := map[string]any{
		"status":              "ok",
		"uptimeSeconds":       int(time.Since(s.startedAt).Seconds()),
		"authEnabled":         s.authOn,
		"queueConfigured":     s.queue != nil,
		"schedulerEnabled":    s.scheduler != nil,
		"retentionConfigured": s.queue != nil && s.queueRetentionInterval > 0,
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if _, err := s.store.ListWorkflows(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "error": err.Error()})
		return
	}
	if s.queue != nil {
		if _, err := s.queue.CountRunJobsByStatus(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleWorkflows(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		workflows, err := s.store.ListWorkflows(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if strings.EqualFold(r.URL.Query().Get("order"), "desc") {
			sort.Slice(workflows, func(i, j int) bool { return workflows[i].ID > workflows[j].ID })
		}
		limit, offset := parseLimitOffset(r)
		total := len(workflows)
		workflows = paginateWorkflows(workflows, r)
		writeListResponse(w, r, workflows, total, limit, offset)
	case http.MethodPost:
		workflow, err := decodeWorkflowRequest(r.Body)
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "workflow", err.Error())
			return
		}
		if err := engine.ValidateWorkflow(workflow); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.store.SaveWorkflow(r.Context(), workflow); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, workflow)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleWorkflowByID(w http.ResponseWriter, r *http.Request) {
	workflowID := strings.TrimPrefix(r.URL.Path, "/workflows/")
	if workflowID == "" {
		writeValidationError(w, http.StatusBadRequest, "workflowId", "workflow id is required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		workflow, err := s.store.GetWorkflow(r.Context(), workflowID)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, workflow)
	case http.MethodPut:
		workflow, err := decodeWorkflowRequest(r.Body)
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "workflow", err.Error())
			return
		}
		if strings.TrimSpace(workflow.ID) == "" {
			workflow.ID = workflowID
		}
		if workflow.ID != workflowID {
			writeValidationError(w, http.StatusBadRequest, "id", "workflow id in body must match path")
			return
		}
		if err := engine.ValidateWorkflow(workflow); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.store.SaveWorkflow(r.Context(), workflow); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, workflow)
	case http.MethodPatch:
		existing, err := s.store.GetWorkflow(r.Context(), workflowID)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		workflow, err := decodeWorkflowPatchRequest(r.Body, existing)
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "workflow", err.Error())
			return
		}
		if workflow.ID != workflowID {
			writeValidationError(w, http.StatusBadRequest, "id", "workflow id in body must match path")
			return
		}
		if err := engine.ValidateWorkflow(workflow); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.store.SaveWorkflow(r.Context(), workflow); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, workflow)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		workflowID := r.URL.Query().Get("workflowId")
		statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
		requestID := strings.TrimSpace(r.URL.Query().Get("requestId"))
		startedAfter, hasStartedAfter, err := parseOptionalTime(r.URL.Query().Get("startedAfter"))
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "startedAfter", "invalid RFC3339 timestamp")
			return
		}
		startedBefore, hasStartedBefore, err := parseOptionalTime(r.URL.Query().Get("startedBefore"))
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "startedBefore", "invalid RFC3339 timestamp")
			return
		}
		if statusFilter != "" && !isValidRunStatus(statusFilter) {
			writeValidationError(w, http.StatusBadRequest, "status", "invalid run status")
			return
		}
		if hasStartedAfter && hasStartedBefore && startedAfter.After(startedBefore) {
			writeValidationError(w, http.StatusBadRequest, "startedAfter", "startedAfter must be before startedBefore")
			return
		}
		runStatus := domain.RunStatus("")
		if statusFilter != "" {
			runStatus = domain.RunStatus(strings.ToUpper(statusFilter))
		}
		limit, offset := parseLimitOffset(r)
		orderDesc := strings.EqualFold(r.URL.Query().Get("order"), "desc")
		runs, err := s.store.ListRunsFiltered(r.Context(), workflowID, runStatus, requestID, startedAfter, startedBefore, orderDesc, limit, offset)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if useListEnvelope(r) {
			total, err := s.store.CountRunsFiltered(r.Context(), workflowID, runStatus, requestID, startedAfter, startedBefore)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeListResponse(w, r, runs, total, limit, offset)
			return
		}
		writeJSON(w, http.StatusOK, runs)
	case http.MethodPost:
		var req struct {
			WorkflowID  string `json:"workflowId"`
			Async       bool   `json:"async"`
			MaxAttempts int    `json:"maxAttempts"`
		}
		if err := decodeJSON(r.Body, &req); err != nil {
			writeValidationError(w, http.StatusBadRequest, "body", err.Error())
			return
		}
		if req.WorkflowID == "" {
			writeValidationError(w, http.StatusBadRequest, "workflowId", "workflowId is required")
			return
		}

		workflow, err := s.store.GetWorkflow(r.Context(), req.WorkflowID)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}

		idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if idempotencyKey == "" {
			idempotencyKey = strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
		}
		if idempotencyKey != "" && !req.Async {
			runID := deterministicRunID(req.WorkflowID, idempotencyKey)
			if existingRun, err := s.store.GetRun(r.Context(), runID); err == nil {
				writeJSON(w, http.StatusOK, existingRun)
				return
			}
			run, err := s.executor.RunWithID(r.Context(), workflow, runID, idempotencyKey)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusCreated, run)
			return
		}

		if req.Async {
			if s.queue == nil {
				writeError(w, http.StatusNotImplemented, "async queue is not configured")
				return
			}
			runID := ""
			if idempotencyKey != "" {
				runID = deterministicRunID(req.WorkflowID, idempotencyKey)
				if existingRun, err := s.store.GetRun(r.Context(), runID); err == nil {
					writeJSON(w, http.StatusOK, existingRun)
					return
				}
				if existingJob, err := s.queue.GetRunJobByRequest(r.Context(), req.WorkflowID, idempotencyKey); err == nil {
					writeJSON(w, http.StatusOK, existingJob)
					return
				}
			} else {
				runID = deterministicRunID(req.WorkflowID, fmt.Sprintf("async-%d", time.Now().UTC().UnixNano()))
			}

			maxAttempts := req.MaxAttempts
			if maxAttempts <= 0 {
				maxAttempts = 5
			}
			now := time.Now().UTC()
			job := domain.RunQueueJob{
				ID:          "job-" + runID,
				WorkflowID:  req.WorkflowID,
				RunID:       runID,
				RequestID:   idempotencyKey,
				Status:      domain.RunJobStatusQueued,
				Attempts:    0,
				MaxAttempts: maxAttempts,
				AvailableAt: now,
				CreatedAt:   now,
				UpdatedAt:   now,
			}
			if err := s.queue.EnqueueRunJob(r.Context(), job); err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusAccepted, job)
			return
		}

		run, err := s.executor.Run(r.Context(), workflow)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, run)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleJobStats(w http.ResponseWriter, r *http.Request) {
	if s.queue == nil {
		writeError(w, http.StatusNotImplemented, "async queue is not configured")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	counts, err := s.queue.CountRunJobsByStatus(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": total, "byStatus": counts})
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	if s.queue == nil {
		writeError(w, http.StatusNotImplemented, "async queue is not configured")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status != "" && !isValidRunJobStatus(status) {
		writeValidationError(w, http.StatusBadRequest, "status", "invalid job status")
		return
	}
	workflowID := strings.TrimSpace(r.URL.Query().Get("workflowId"))
	requestID := strings.TrimSpace(r.URL.Query().Get("requestId"))
	updatedAfter, hasUpdatedAfter, err := parseOptionalTime(r.URL.Query().Get("updatedAfter"))
	if err != nil {
		writeValidationError(w, http.StatusBadRequest, "updatedAfter", "invalid RFC3339 timestamp")
		return
	}
	updatedBefore, hasUpdatedBefore, err := parseOptionalTime(r.URL.Query().Get("updatedBefore"))
	if err != nil {
		writeValidationError(w, http.StatusBadRequest, "updatedBefore", "invalid RFC3339 timestamp")
		return
	}
	if hasUpdatedAfter && hasUpdatedBefore && updatedAfter.After(updatedBefore) {
		writeValidationError(w, http.StatusBadRequest, "updatedAfter", "updatedAfter must be before updatedBefore")
		return
	}
	limit, offset := parseLimitOffset(r)
	jobs, err := s.queue.ListRunJobsFiltered(r.Context(), status, workflowID, requestID, updatedAfter, updatedBefore, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if useListEnvelope(r) {
		total, err := s.queue.CountRunJobsFiltered(r.Context(), status, workflowID, requestID, updatedAfter, updatedBefore)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeListResponse(w, r, jobs, total, limit, offset)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) handleDeadLetterJobs(w http.ResponseWriter, r *http.Request) {
	if s.queue == nil {
		writeError(w, http.StatusNotImplemented, "async queue is not configured")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	updatedAfter, hasUpdatedAfter, err := parseOptionalTime(r.URL.Query().Get("updatedAfter"))
	if err != nil {
		writeValidationError(w, http.StatusBadRequest, "updatedAfter", "invalid RFC3339 timestamp")
		return
	}
	updatedBefore, hasUpdatedBefore, err := parseOptionalTime(r.URL.Query().Get("updatedBefore"))
	if err != nil {
		writeValidationError(w, http.StatusBadRequest, "updatedBefore", "invalid RFC3339 timestamp")
		return
	}
	if hasUpdatedAfter && hasUpdatedBefore && updatedAfter.After(updatedBefore) {
		writeValidationError(w, http.StatusBadRequest, "updatedAfter", "updatedAfter must be before updatedBefore")
		return
	}

	limit, offset := parseLimitOffset(r)
	jobs, err := s.queue.ListRunJobsFiltered(r.Context(), string(domain.RunJobStatusDead), "", "", updatedAfter, updatedBefore, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if useListEnvelope(r) {
		total, err := s.queue.CountRunJobsFiltered(r.Context(), string(domain.RunJobStatusDead), "", "", updatedAfter, updatedBefore)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeListResponse(w, r, jobs, total, limit, offset)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) handleJobByID(w http.ResponseWriter, r *http.Request) {
	if s.queue == nil {
		writeError(w, http.StatusNotImplemented, "async queue is not configured")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/jobs/requeue/") {
		s.handleJobRequeueByID(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/jobs/"))
	if jobID == "" {
		writeValidationError(w, http.StatusBadRequest, "jobId", "job id is required")
		return
	}

	if r.Method == http.MethodDelete {
		reason := strings.TrimSpace(r.URL.Query().Get("reason"))
		if err := s.queue.CancelRunJob(r.Context(), jobID, reason); err != nil {
			writeValidationError(w, http.StatusConflict, "status", err.Error())
			return
		}
		job, _ := s.queue.GetRunJob(r.Context(), jobID)
		writeJSON(w, http.StatusOK, job)
		return
	}

	job, err := s.queue.GetRunJob(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleJobRequeueByID(w http.ResponseWriter, r *http.Request) {
	if s.queue == nil {
		writeError(w, http.StatusNotImplemented, "async queue is not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	jobID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/jobs/requeue/"))
	if jobID == "" {
		writeError(w, http.StatusBadRequest, "job id is required")
		return
	}

	var req struct {
		AvailableAfter string `json:"availableAfter"`
		ReplayedBy     string `json:"replayedBy"`
		ReplayReason   string `json:"replayReason"`
	}
	if err := decodeJSON(r.Body, &req); err != nil {
		writeValidationError(w, http.StatusBadRequest, "body", err.Error())
		return
	}

	delay := time.Duration(0)
	if strings.TrimSpace(req.AvailableAfter) != "" {
		parsed, err := time.ParseDuration(req.AvailableAfter)
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "availableAfter", "invalid availableAfter duration")
			return
		}
		delay = parsed
	}

	job, err := s.queue.GetRunJob(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	if job.Status != domain.RunJobStatusDead && job.Status != domain.RunJobStatusFailed {
		writeValidationError(w, http.StatusConflict, "status", "only failed or dead-letter jobs can be requeued")
		return
	}

	if err := s.queue.RequeueRunJob(r.Context(), jobID, delay, strings.TrimSpace(req.ReplayedBy), strings.TrimSpace(req.ReplayReason)); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	requeued, err := s.queue.GetRunJob(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, requeued)
}

func (s *Server) handleJobsBulkCancel(w http.ResponseWriter, r *http.Request) {
	if s.queue == nil {
		writeError(w, http.StatusNotImplemented, "async queue is not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		JobIDs     []string `json:"jobIds"`
		Status     string   `json:"status"`
		WorkflowID string   `json:"workflowId"`
		RequestID  string   `json:"requestId"`
		Limit      int      `json:"limit"`
		Reason     string   `json:"reason"`
	}
	if err := decodeJSON(r.Body, &req); err != nil {
		writeValidationError(w, http.StatusBadRequest, "body", err.Error())
		return
	}
	if strings.TrimSpace(req.Status) != "" && !isValidRunJobStatus(req.Status) {
		writeValidationError(w, http.StatusBadRequest, "status", "invalid job status")
		return
	}

	jobIDs := make([]string, 0, len(req.JobIDs))
	for _, id := range req.JobIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			jobIDs = append(jobIDs, id)
		}
	}
	if len(jobIDs) == 0 {
		status := strings.TrimSpace(req.Status)
		if status == "" {
			status = string(domain.RunJobStatusQueued)
		}
		workflowID := strings.TrimSpace(req.WorkflowID)
		requestID := strings.TrimSpace(req.RequestID)
		limit := req.Limit
		if limit <= 0 {
			limit = 100
		}
		jobs, err := s.queue.ListRunJobsFiltered(r.Context(), status, workflowID, requestID, time.Time{}, time.Time{}, limit, 0)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		for _, job := range jobs {
			jobIDs = append(jobIDs, job.ID)
		}
	}

	cancelled := make([]domain.RunQueueJob, 0, len(jobIDs))
	failed := map[string]string{}
	failedDetails := map[string]bulkFailureDetail{}
	for _, jobID := range jobIDs {
		if err := s.queue.CancelRunJob(r.Context(), jobID, strings.TrimSpace(req.Reason)); err != nil {
			message := err.Error()
			failed[jobID] = message
			failedDetails[jobID] = bulkFailureDetail{Code: classifyBulkJobError(message), Message: message}
			continue
		}
		job, err := s.queue.GetRunJob(r.Context(), jobID)
		if err != nil {
			message := err.Error()
			failed[jobID] = message
			failedDetails[jobID] = bulkFailureDetail{Code: classifyBulkJobError(message), Message: message}
			continue
		}
		cancelled = append(cancelled, job)
	}

	writeJSON(w, http.StatusOK, bulkCancelResponse{
		Cancelled: cancelled,
		bulkResult: bulkResult{
			ResultCode:    bulkResultCode(len(cancelled), len(failed)),
			Failed:        failed,
			FailedDetails: failedDetails,
		},
	})
}

func (s *Server) handleJobsPurge(w http.ResponseWriter, r *http.Request) {
	if s.queue == nil {
		writeError(w, http.StatusNotImplemented, "async queue is not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Status    string `json:"status"`
		OlderThan string `json:"olderThan"`
		Limit     int    `json:"limit"`
		DryRun    bool   `json:"dryRun"`
	}
	if err := decodeJSON(r.Body, &req); err != nil {
		writeValidationError(w, http.StatusBadRequest, "body", err.Error())
		return
	}
	if strings.TrimSpace(req.Status) != "" && !isValidRunJobStatus(req.Status) {
		writeValidationError(w, http.StatusBadRequest, "status", "invalid job status")
		return
	}
	queryDryRun, hasDryRun := parseOptionalBool(r.URL.Query().Get("dryRun"))
	if hasDryRun {
		req.DryRun = queryDryRun
	}
	d := time.Duration(0)
	if strings.TrimSpace(req.OlderThan) != "" {
		parsed, err := time.ParseDuration(req.OlderThan)
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "olderThan", "invalid olderThan duration")
			return
		}
		d = parsed
	}
	if req.DryRun {
		status := strings.TrimSpace(req.Status)
		updatedBefore := time.Time{}
		if d > 0 {
			updatedBefore = time.Now().UTC().Add(-d)
		}
		matched, err := s.queue.CountRunJobsFiltered(r.Context(), status, "", "", time.Time{}, updatedBefore)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if req.Limit > 0 && matched > req.Limit {
			matched = req.Limit
		}
		resultCode := "BULK_OK"
		if req.Limit > 0 && matched >= req.Limit {
			resultCode = "PURGE_DRY_RUN_LIMIT_REACHED"
		}
		dryRun := true
		writeJSON(w, http.StatusOK, purgeResponse{Deleted: 0, ResultCode: resultCode, DryRun: &dryRun, Matched: &matched})
		return
	}
	deleted, err := s.queue.PurgeRunJobs(r.Context(), strings.TrimSpace(req.Status), d, req.Limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resultCode := "BULK_OK"
	if req.Limit > 0 && deleted >= req.Limit {
		resultCode = "PURGE_LIMIT_REACHED"
	}
	writeJSON(w, http.StatusOK, purgeResponse{Deleted: deleted, ResultCode: resultCode})
}

func (s *Server) handleJobsBulkRequeue(w http.ResponseWriter, r *http.Request) {
	if s.queue == nil {
		writeError(w, http.StatusNotImplemented, "async queue is not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		JobIDs         []string `json:"jobIds"`
		Status         string   `json:"status"`
		Limit          int      `json:"limit"`
		AvailableAfter string   `json:"availableAfter"`
		ReplayedBy     string   `json:"replayedBy"`
		ReplayReason   string   `json:"replayReason"`
	}
	if err := decodeJSON(r.Body, &req); err != nil {
		writeValidationError(w, http.StatusBadRequest, "body", err.Error())
		return
	}
	if strings.TrimSpace(req.Status) != "" && !isValidRunJobStatus(req.Status) {
		writeValidationError(w, http.StatusBadRequest, "status", "invalid job status")
		return
	}

	delay := time.Duration(0)
	if strings.TrimSpace(req.AvailableAfter) != "" {
		parsed, err := time.ParseDuration(req.AvailableAfter)
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "availableAfter", "invalid availableAfter duration")
			return
		}
		delay = parsed
	}

	jobIDs := make([]string, 0, len(req.JobIDs))
	for _, id := range req.JobIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			jobIDs = append(jobIDs, id)
		}
	}

	if len(jobIDs) == 0 {
		status := strings.TrimSpace(req.Status)
		if status == "" {
			status = string(domain.RunJobStatusDead)
		}
		if status != string(domain.RunJobStatusDead) && status != string(domain.RunJobStatusFailed) {
			writeValidationError(w, http.StatusBadRequest, "status", "status must be DEAD_LETTER or FAILED")
			return
		}
		limit := req.Limit
		if limit <= 0 {
			limit = 100
		}
		jobs, err := s.queue.ListRunJobsFiltered(r.Context(), status, "", "", time.Time{}, time.Time{}, limit, 0)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		for _, job := range jobs {
			jobIDs = append(jobIDs, job.ID)
		}
	}

	requeued := make([]domain.RunQueueJob, 0, len(jobIDs))
	failed := map[string]string{}
	failedDetails := map[string]bulkFailureDetail{}
	for _, jobID := range jobIDs {
		job, err := s.queue.GetRunJob(r.Context(), jobID)
		if err != nil {
			message := err.Error()
			failed[jobID] = message
			failedDetails[jobID] = bulkFailureDetail{Code: classifyBulkJobError(message), Message: message}
			continue
		}
		if job.Status != domain.RunJobStatusDead && job.Status != domain.RunJobStatusFailed {
			message := "only failed or dead-letter jobs can be requeued"
			failed[jobID] = message
			failedDetails[jobID] = bulkFailureDetail{Code: classifyBulkJobError(message), Message: message}
			continue
		}
		if err := s.queue.RequeueRunJob(r.Context(), jobID, delay, strings.TrimSpace(req.ReplayedBy), strings.TrimSpace(req.ReplayReason)); err != nil {
			message := err.Error()
			failed[jobID] = message
			failedDetails[jobID] = bulkFailureDetail{Code: classifyBulkJobError(message), Message: message}
			continue
		}
		updated, err := s.queue.GetRunJob(r.Context(), jobID)
		if err != nil {
			message := err.Error()
			failed[jobID] = message
			failedDetails[jobID] = bulkFailureDetail{Code: classifyBulkJobError(message), Message: message}
			continue
		}
		requeued = append(requeued, updated)
	}

	writeJSON(w, http.StatusOK, bulkRequeueResponse{
		Requeued: requeued,
		bulkResult: bulkResult{
			ResultCode:    bulkResultCode(len(requeued), len(failed)),
			Failed:        failed,
			FailedDetails: failedDetails,
		},
	})
}

func (s *Server) handleRunByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	runID := strings.TrimPrefix(r.URL.Path, "/runs/")
	if runID == "" {
		writeError(w, http.StatusBadRequest, "run id is required")
		return
	}

	if r.Method == http.MethodDelete {
		if s.queue == nil {
			writeError(w, http.StatusNotImplemented, "async queue is not configured")
			return
		}
		reason := strings.TrimSpace(r.URL.Query().Get("reason"))
		if reason == "" {
			reason = "run cancelled"
		}
		jobs := make([]domain.RunQueueJob, 0)
		offset := 0
		for {
			batch, err := s.queue.ListRunJobsByRun(r.Context(), runID, 500, offset)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if len(batch) == 0 {
				break
			}
			jobs = append(jobs, batch...)
			offset += len(batch)
			if len(batch) < 500 {
				break
			}
		}
		cancelled := make([]domain.RunQueueJob, 0)
		for _, job := range jobs {
			if job.RunID != runID {
				continue
			}
			if err := s.queue.CancelRunJob(r.Context(), job.ID, reason); err != nil {
				continue
			}
			updated, err := s.queue.GetRunJob(r.Context(), job.ID)
			if err == nil {
				cancelled = append(cancelled, updated)
			}
		}
		if len(cancelled) == 0 {
			writeValidationError(w, http.StatusConflict, "runId", "no cancellable jobs found for run")
			return
		}
		run, err := s.store.GetRun(r.Context(), runID)
		if err == nil {
			run.Status = domain.RunStatusCancelled
			run.Error = reason
			run.FinishedAt = time.Now().UTC()
			run.Duration = run.FinishedAt.Sub(run.StartedAt)
			_ = s.store.SaveRun(r.Context(), run)
		}
		writeJSON(w, http.StatusOK, map[string]any{"runId": runID, "cancelledJobs": cancelled})
		return
	}

	run, err := s.store.GetRun(r.Context(), runID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleJobHistoryByID(w http.ResponseWriter, r *http.Request) {
	if s.queue == nil {
		writeError(w, http.StatusNotImplemented, "async queue is not configured")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	jobID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/jobs/history/"))
	if jobID == "" {
		writeValidationError(w, http.StatusBadRequest, "jobId", "job id is required")
		return
	}
	toStatus := strings.TrimSpace(r.URL.Query().Get("toStatus"))
	if toStatus != "" && !isValidRunJobStatus(toStatus) {
		writeValidationError(w, http.StatusBadRequest, "toStatus", "invalid job status")
		return
	}
	actor := strings.TrimSpace(r.URL.Query().Get("actor"))
	createdAfter, hasCreatedAfter, err := parseOptionalTime(r.URL.Query().Get("createdAfter"))
	if err != nil {
		writeValidationError(w, http.StatusBadRequest, "createdAfter", "invalid RFC3339 timestamp")
		return
	}
	createdBefore, hasCreatedBefore, err := parseOptionalTime(r.URL.Query().Get("createdBefore"))
	if err != nil {
		writeValidationError(w, http.StatusBadRequest, "createdBefore", "invalid RFC3339 timestamp")
		return
	}
	if hasCreatedAfter && hasCreatedBefore && createdAfter.After(createdBefore) {
		writeValidationError(w, http.StatusBadRequest, "createdAfter", "createdAfter must be before createdBefore")
		return
	}
	limit, offset := parseLimitOffset(r)
	items, err := s.queue.ListRunJobTransitionsFiltered(r.Context(), jobID, strings.ToUpper(toStatus), actor, createdAfter, createdBefore, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !useListEnvelope(r) {
		writeJSON(w, http.StatusOK, items)
		return
	}
	total, err := s.queue.CountRunJobTransitionsFiltered(r.Context(), jobID, strings.ToUpper(toStatus), actor, createdAfter, createdBefore)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeListResponse(w, r, items, total, limit, offset)
}

func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		writeError(w, http.StatusNotImplemented, "scheduler is not configured")
		return
	}

	if r.Method == http.MethodGet {
		items := s.scheduler.List()
		sort.Slice(items, func(i, j int) bool {
			return items[i].WorkflowID < items[j].WorkflowID
		})
		if useListEnvelope(r) {
			limit, offset := parseLimitOffset(r)
			total := len(items)
			start := offset
			if start > len(items) {
				start = len(items)
			}
			end := start + limit
			if end > len(items) {
				end = len(items)
			}
			page := items[start:end]
			writeListResponse(w, r, page, total, limit, offset)
			return
		}
		writeJSON(w, http.StatusOK, items)
		return
	}

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		WorkflowID   string `json:"workflowId"`
		Interval     string `json:"interval"`
		Cron         string `json:"cron"`
		RunOnStartup bool   `json:"runOnStartup"`
		AllowOverlap bool   `json:"allowOverlap"`
	}
	if err := decodeJSON(r.Body, &req); err != nil {
		writeValidationError(w, http.StatusBadRequest, "body", err.Error())
		return
	}

	cfg := domain.ScheduleConfig{
		WorkflowID:   req.WorkflowID,
		Cron:         req.Cron,
		RunOnStartup: req.RunOnStartup,
		AllowOverlap: req.AllowOverlap,
	}
	if req.Interval != "" {
		d, err := time.ParseDuration(req.Interval)
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "interval", "invalid interval duration")
			return
		}
		cfg.Interval = d
	}

	if err := s.scheduler.Register(cfg); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, cfg)
}

func (s *Server) handleScheduleByWorkflowID(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		writeError(w, http.StatusNotImplemented, "scheduler is not configured")
		return
	}
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workflowID := strings.TrimPrefix(r.URL.Path, "/schedules/")
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		writeValidationError(w, http.StatusBadRequest, "workflowId", "workflow id is required")
		return
	}

	s.scheduler.Unregister(workflowID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "unscheduled", "workflowId": workflowID})
}

func (s *Server) handleSchedulePauseByWorkflowID(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		writeError(w, http.StatusNotImplemented, "scheduler is not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	workflowID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/schedules/pause/"))
	if workflowID == "" {
		writeValidationError(w, http.StatusBadRequest, "workflowId", "workflow id is required")
		return
	}
	if err := s.scheduler.Pause(workflowID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "paused", "workflowId": workflowID})
}

func (s *Server) handleScheduleResumeByWorkflowID(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		writeError(w, http.StatusNotImplemented, "scheduler is not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	workflowID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/schedules/resume/"))
	if workflowID == "" {
		writeValidationError(w, http.StatusBadRequest, "workflowId", "workflow id is required")
		return
	}
	if err := s.scheduler.Resume(workflowID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "resumed", "workflowId": workflowID})
}

func paginateWorkflows(items []domain.Workflow, r *http.Request) []domain.Workflow {
	limit, offset := parseLimitOffset(r)
	if offset >= len(items) {
		return []domain.Workflow{}
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}

func paginateRuns(items []domain.Run, r *http.Request) []domain.Run {
	limit, offset := parseLimitOffset(r)
	if offset >= len(items) {
		return []domain.Run{}
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}

func parseLimitOffset(r *http.Request) (int, int) {
	limit := 50
	offset := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if val, err := strconv.Atoi(raw); err == nil && val > 0 && val <= 1000 {
			limit = val
		}
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("offset")); raw != "" {
		if val, err := strconv.Atoi(raw); err == nil && val >= 0 {
			offset = val
		}
	}
	return limit, offset
}

func parseOptionalBool(raw string) (bool, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return false, false
	}
	val, err := strconv.ParseBool(trimmed)
	if err != nil {
		return false, false
	}
	return val, true
}

func useListEnvelope(r *http.Request) bool {
	if raw := strings.TrimSpace(r.URL.Query().Get("envelope")); raw != "" {
		if val, ok := parseOptionalBool(raw); ok {
			return val
		}
	}
	if raw := strings.TrimSpace(r.Header.Get("X-List-Envelope")); raw != "" {
		if val, ok := parseOptionalBool(raw); ok {
			return val
		}
	}
	return false
}

func writeListResponse[T any](w http.ResponseWriter, r *http.Request, items []T, total int, limit int, offset int) {
	if useListEnvelope(r) {
		writeJSON(w, http.StatusOK, listResponse[T]{Items: items, Total: total, Limit: limit, Offset: offset, Returned: len(items)})
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func bulkResultCode(success int, failed int) string {
	if failed == 0 {
		return "BULK_OK"
	}
	if success == 0 {
		return "BULK_ALL_FAILED"
	}
	return "BULK_PARTIAL_FAILURE"
}

func classifyBulkJobError(message string) string {
	msg := strings.ToLower(strings.TrimSpace(message))
	switch {
	case strings.Contains(msg, "not found"):
		return "JOB_NOT_FOUND"
	case strings.Contains(msg, "only failed or dead-letter"):
		return "INVALID_JOB_STATE"
	case strings.Contains(msg, "cannot be cancelled"):
		return "INVALID_JOB_STATE"
	case strings.Contains(msg, "transition rejected"):
		return "INVALID_JOB_STATE"
	default:
		return "JOB_OPERATION_FAILED"
	}
}

func listAllRunJobsPaged(ctx context.Context, queue domain.RunQueue, status string, pageSize int) ([]domain.RunQueueJob, error) {
	if pageSize <= 0 {
		pageSize = 500
	}
	offset := 0
	all := make([]domain.RunQueueJob, 0)
	for {
		items, err := queue.ListRunJobs(ctx, status, pageSize, offset)
		if err != nil {
			return nil, err
		}
		if len(items) == 0 {
			break
		}
		all = append(all, items...)
		offset += len(items)
		if len(items) < pageSize {
			break
		}
	}
	return all, nil
}

func parseOptionalTime(raw string) (time.Time, bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, false, nil
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err == nil {
		return parsed.UTC(), true, nil
	}
	parsed, err = time.Parse(time.RFC3339Nano, trimmed)
	if err != nil {
		return time.Time{}, false, err
	}
	return parsed.UTC(), true, nil
}

func isValidRunStatus(raw string) bool {
	status := domain.RunStatus(strings.ToUpper(strings.TrimSpace(raw)))
	switch status {
	case domain.RunStatusPending, domain.RunStatusRunning, domain.RunStatusSuccess, domain.RunStatusFailed, domain.RunStatusCancelled:
		return true
	default:
		return false
	}
}

func isValidRunJobStatus(raw string) bool {
	status := domain.RunJobStatus(strings.ToUpper(strings.TrimSpace(raw)))
	switch status {
	case domain.RunJobStatusQueued, domain.RunJobStatusLeased, domain.RunJobStatusSucceeded, domain.RunJobStatusFailed, domain.RunJobStatusDead, domain.RunJobStatusCancelled:
		return true
	default:
		return false
	}
}

func deterministicRunID(workflowID string, key string) string {
	sum := sha1.Sum([]byte(workflowID + "|" + key))
	return "idem-" + hex.EncodeToString(sum[:])
}

func decodeJSON(body io.ReadCloser, out any) error {
	defer body.Close()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

type validationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func writeValidationError(w http.ResponseWriter, status int, field string, message string) {
	writeJSON(w, status, map[string]any{
		"error":   "validation failed",
		"details": []validationError{{Field: field, Message: message}},
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.statusCode = code
	s.ResponseWriter.WriteHeader(code)
}

type workflowRequest struct {
	ID                  string            `json:"id"`
	Name                string            `json:"name"`
	Description         string            `json:"description"`
	Version             string            `json:"version"`
	MaxConcurrency      int               `json:"maxConcurrency"`
	FailFast            bool              `json:"failFast"`
	CompensateOnFailure bool              `json:"compensateOnFailure"`
	Timeout             string            `json:"timeout"`
	Tasks               []taskNodeRequest `json:"tasks"`
}

type taskNodeRequest struct {
	ID           string                  `json:"id"`
	Name         string                  `json:"name"`
	Action       string                  `json:"action"`
	Input        map[string]string       `json:"input"`
	DependsOn    []string                `json:"dependsOn"`
	Condition    string                  `json:"condition"`
	RunAfter     string                  `json:"runAfter"`
	Timeout      string                  `json:"timeout"`
	AllowFailure bool                    `json:"allowFailure"`
	RetryPolicy  retryPolicyRequest      `json:"retryPolicy"`
	Compensation compensationSpecRequest `json:"compensation"`
}

type retryPolicyRequest struct {
	MaxAttempts int    `json:"maxAttempts"`
	BackoffBase string `json:"backoffBase"`
}

type compensationSpecRequest struct {
	Action  string            `json:"action"`
	Input   map[string]string `json:"input"`
	Timeout string            `json:"timeout"`
}

type workflowPatchRequest struct {
	ID                  *string            `json:"id"`
	Name                *string            `json:"name"`
	Description         *string            `json:"description"`
	Version             *string            `json:"version"`
	MaxConcurrency      *int               `json:"maxConcurrency"`
	FailFast            *bool              `json:"failFast"`
	CompensateOnFailure *bool              `json:"compensateOnFailure"`
	Timeout             *string            `json:"timeout"`
	Tasks               *[]taskNodeRequest `json:"tasks"`
}

func decodeWorkflowRequest(body io.ReadCloser) (domain.Workflow, error) {
	var req workflowRequest
	if err := decodeJSON(body, &req); err != nil {
		return domain.Workflow{}, err
	}

	workflow := domain.Workflow{
		ID:                  req.ID,
		Name:                req.Name,
		Description:         req.Description,
		Version:             req.Version,
		MaxConcurrency:      req.MaxConcurrency,
		FailFast:            req.FailFast,
		CompensateOnFailure: req.CompensateOnFailure,
		Tasks:               make([]domain.TaskNode, 0, len(req.Tasks)),
	}

	if strings.TrimSpace(req.Timeout) != "" {
		d, err := time.ParseDuration(req.Timeout)
		if err != nil {
			return domain.Workflow{}, fmt.Errorf("invalid workflow timeout: %w", err)
		}
		workflow.Timeout = d
	}

	for _, taskReq := range req.Tasks {
		task := domain.TaskNode{
			ID:           taskReq.ID,
			Name:         taskReq.Name,
			Action:       taskReq.Action,
			Input:        taskReq.Input,
			DependsOn:    taskReq.DependsOn,
			Condition:    taskReq.Condition,
			AllowFailure: taskReq.AllowFailure,
			RetryPolicy: domain.RetryPolicy{
				MaxAttempts: taskReq.RetryPolicy.MaxAttempts,
			},
			Compensation: domain.CompensationSpec{
				Action: taskReq.Compensation.Action,
				Input:  taskReq.Compensation.Input,
			},
		}

		if strings.TrimSpace(taskReq.RunAfter) != "" {
			d, err := time.ParseDuration(taskReq.RunAfter)
			if err != nil {
				return domain.Workflow{}, fmt.Errorf("invalid runAfter for %s: %w", taskReq.ID, err)
			}
			task.RunAfter = d
		}
		if strings.TrimSpace(taskReq.Timeout) != "" {
			d, err := time.ParseDuration(taskReq.Timeout)
			if err != nil {
				return domain.Workflow{}, fmt.Errorf("invalid task timeout for %s: %w", taskReq.ID, err)
			}
			task.Timeout = d
		}
		if strings.TrimSpace(taskReq.RetryPolicy.BackoffBase) != "" {
			d, err := time.ParseDuration(taskReq.RetryPolicy.BackoffBase)
			if err != nil {
				return domain.Workflow{}, fmt.Errorf("invalid backoffBase for %s: %w", taskReq.ID, err)
			}
			task.RetryPolicy.BackoffBase = d
		}
		if strings.TrimSpace(taskReq.Compensation.Timeout) != "" {
			d, err := time.ParseDuration(taskReq.Compensation.Timeout)
			if err != nil {
				return domain.Workflow{}, fmt.Errorf("invalid compensation timeout for %s: %w", taskReq.ID, err)
			}
			task.Compensation.Timeout = d
		}

		workflow.Tasks = append(workflow.Tasks, task)
	}

	return workflow, nil
}

func decodeWorkflowPatchRequest(body io.ReadCloser, base domain.Workflow) (domain.Workflow, error) {
	var patch workflowPatchRequest
	if err := decodeJSON(body, &patch); err != nil {
		return domain.Workflow{}, err
	}

	workflow := base
	if patch.ID != nil {
		workflow.ID = strings.TrimSpace(*patch.ID)
	}
	if patch.Name != nil {
		workflow.Name = *patch.Name
	}
	if patch.Description != nil {
		workflow.Description = *patch.Description
	}
	if patch.Version != nil {
		workflow.Version = *patch.Version
	}
	if patch.MaxConcurrency != nil {
		workflow.MaxConcurrency = *patch.MaxConcurrency
	}
	if patch.FailFast != nil {
		workflow.FailFast = *patch.FailFast
	}
	if patch.CompensateOnFailure != nil {
		workflow.CompensateOnFailure = *patch.CompensateOnFailure
	}
	if patch.Timeout != nil {
		timeoutRaw := strings.TrimSpace(*patch.Timeout)
		if timeoutRaw == "" {
			workflow.Timeout = 0
		} else {
			d, err := time.ParseDuration(timeoutRaw)
			if err != nil {
				return domain.Workflow{}, fmt.Errorf("invalid workflow timeout: %w", err)
			}
			workflow.Timeout = d
		}
	}
	if patch.Tasks != nil {
		tasks := make([]domain.TaskNode, 0, len(*patch.Tasks))
		for _, taskReq := range *patch.Tasks {
			task := domain.TaskNode{
				ID:           taskReq.ID,
				Name:         taskReq.Name,
				Action:       taskReq.Action,
				Input:        taskReq.Input,
				DependsOn:    taskReq.DependsOn,
				Condition:    taskReq.Condition,
				AllowFailure: taskReq.AllowFailure,
				RetryPolicy: domain.RetryPolicy{
					MaxAttempts: taskReq.RetryPolicy.MaxAttempts,
				},
				Compensation: domain.CompensationSpec{
					Action: taskReq.Compensation.Action,
					Input:  taskReq.Compensation.Input,
				},
			}

			if strings.TrimSpace(taskReq.RunAfter) != "" {
				d, err := time.ParseDuration(taskReq.RunAfter)
				if err != nil {
					return domain.Workflow{}, fmt.Errorf("invalid runAfter for %s: %w", taskReq.ID, err)
				}
				task.RunAfter = d
			}
			if strings.TrimSpace(taskReq.Timeout) != "" {
				d, err := time.ParseDuration(taskReq.Timeout)
				if err != nil {
					return domain.Workflow{}, fmt.Errorf("invalid task timeout for %s: %w", taskReq.ID, err)
				}
				task.Timeout = d
			}
			if strings.TrimSpace(taskReq.RetryPolicy.BackoffBase) != "" {
				d, err := time.ParseDuration(taskReq.RetryPolicy.BackoffBase)
				if err != nil {
					return domain.Workflow{}, fmt.Errorf("invalid backoffBase for %s: %w", taskReq.ID, err)
				}
				task.RetryPolicy.BackoffBase = d
			}
			if strings.TrimSpace(taskReq.Compensation.Timeout) != "" {
				d, err := time.ParseDuration(taskReq.Compensation.Timeout)
				if err != nil {
					return domain.Workflow{}, fmt.Errorf("invalid compensation timeout for %s: %w", taskReq.ID, err)
				}
				task.Compensation.Timeout = d
			}

			tasks = append(tasks, task)
		}
		workflow.Tasks = tasks
	}

	return workflow, nil
}
