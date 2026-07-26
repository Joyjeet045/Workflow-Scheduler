package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"workflowscheduler/internal/domain"
)

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	if dbPath == "" {
		return nil, errors.New("dbPath is required")
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()
	for _, pragma := range []string{
		`PRAGMA busy_timeout = 5000;`,
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA synchronous = NORMAL;`,
		`PRAGMA foreign_keys = ON;`,
	} {
		if err := execWithBusyRetry(ctx, db, pragma); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	store := &SQLiteStore{db: db}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS workflows (
			id TEXT PRIMARY KEY,
			payload TEXT NOT NULL,
			updated_at INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS runs (
			id TEXT PRIMARY KEY,
			workflow_id TEXT NOT NULL,
			request_id TEXT NOT NULL DEFAULT '',
			started_at INTEGER NOT NULL,
			payload TEXT NOT NULL,
			updated_at INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_runs_workflow_started ON runs(workflow_id, started_at);`,
		`CREATE INDEX IF NOT EXISTS idx_runs_request_workflow ON runs(request_id, workflow_id);`,
		`CREATE TABLE IF NOT EXISTS run_queue (
			id TEXT PRIMARY KEY,
			workflow_id TEXT NOT NULL,
			run_id TEXT NOT NULL,
			request_id TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			attempts INTEGER NOT NULL,
			max_attempts INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			lease_owner TEXT NOT NULL,
			lease_until INTEGER NOT NULL,
			available_at INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_run_queue_state ON run_queue(status, available_at, created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_run_queue_request ON run_queue(workflow_id, request_id);`,
		`CREATE TABLE IF NOT EXISTS run_queue_transitions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id TEXT NOT NULL,
			from_status TEXT NOT NULL,
			to_status TEXT NOT NULL,
			reason TEXT NOT NULL,
			actor TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			FOREIGN KEY(job_id) REFERENCES run_queue(id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_run_queue_transitions_job_created ON run_queue_transitions(job_id, created_at);`,
	}

	for _, stmt := range stmts {
		if err := execWithBusyRetry(ctx, s.db, stmt); err != nil {
			return err
		}
	}

	for _, alter := range []string{
		`ALTER TABLE run_queue ADD COLUMN replay_count INTEGER NOT NULL DEFAULT 0;`,
		`ALTER TABLE run_queue ADD COLUMN replayed_by TEXT NOT NULL DEFAULT '';`,
		`ALTER TABLE run_queue ADD COLUMN replay_reason TEXT NOT NULL DEFAULT '';`,
		`ALTER TABLE run_queue ADD COLUMN replayed_at INTEGER NOT NULL DEFAULT 0;`,
	} {
		if err := execIgnoreDuplicateColumn(ctx, s.db, alter); err != nil {
			return err
		}
	}
	return nil
}

func execWithBusyRetry(ctx context.Context, db *sql.DB, stmt string) error {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		_, err := db.ExecContext(ctx, stmt)
		if err == nil {
			return nil
		}
		if !isSQLiteBusyError(err) {
			return err
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("sqlite busy retry exhausted")
}

func isSQLiteBusyError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "sqlite_busy") || strings.Contains(message, "database is locked")
}

func execIgnoreDuplicateColumn(ctx context.Context, db *sql.DB, stmt string) error {
	_, err := db.ExecContext(ctx, stmt)
	if err == nil {
		return nil
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "duplicate column name") {
		return nil
	}
	return err
}

func (s *SQLiteStore) SaveWorkflow(ctx context.Context, workflow domain.Workflow) error {
	payload, err := json.Marshal(workflow)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(
		ctx,
		`INSERT INTO workflows(id, payload, updated_at) VALUES(?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at`,
		workflow.ID,
		string(payload),
		time.Now().UTC().UnixNano(),
	)
	return err
}

func (s *SQLiteStore) GetWorkflow(ctx context.Context, workflowID string) (domain.Workflow, error) {
	var payload string
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM workflows WHERE id = ?`, workflowID).Scan(&payload)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Workflow{}, errors.New("workflow not found")
		}
		return domain.Workflow{}, err
	}
	var workflow domain.Workflow
	if err := json.Unmarshal([]byte(payload), &workflow); err != nil {
		return domain.Workflow{}, err
	}
	return workflow, nil
}

func (s *SQLiteStore) ListWorkflows(ctx context.Context) ([]domain.Workflow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payload FROM workflows`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.Workflow, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var workflow domain.Workflow
		if err := json.Unmarshal([]byte(payload), &workflow); err != nil {
			return nil, err
		}
		items = append(items, workflow)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}

func (s *SQLiteStore) SaveRun(ctx context.Context, run domain.Run) error {
	payload, err := json.Marshal(run)
	if err != nil {
		return err
	}
	startedAt := run.StartedAt.UnixNano()
	if run.StartedAt.IsZero() {
		startedAt = time.Now().UTC().UnixNano()
	}
	_, err = s.db.ExecContext(
		ctx,
		`INSERT INTO runs(id, workflow_id, request_id, started_at, payload, updated_at)
		 VALUES(?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET workflow_id=excluded.workflow_id, request_id=excluded.request_id,
		 started_at=excluded.started_at, payload=excluded.payload, updated_at=excluded.updated_at`,
		run.ID,
		run.WorkflowID,
		run.RequestID,
		startedAt,
		string(payload),
		time.Now().UTC().UnixNano(),
	)
	return err
}

func (s *SQLiteStore) GetRun(ctx context.Context, runID string) (domain.Run, error) {
	var payload string
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM runs WHERE id = ?`, runID).Scan(&payload)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Run{}, errors.New("run not found")
		}
		return domain.Run{}, err
	}
	var run domain.Run
	if err := json.Unmarshal([]byte(payload), &run); err != nil {
		return domain.Run{}, err
	}
	return run, nil
}

func (s *SQLiteStore) ListRuns(ctx context.Context, workflowID string) ([]domain.Run, error) {
	query := `SELECT payload FROM runs`
	args := []any{}
	if workflowID != "" {
		query += ` WHERE workflow_id = ?`
		args = append(args, workflowID)
	}
	query += ` ORDER BY started_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.Run, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var run domain.Run
		if err := json.Unmarshal([]byte(payload), &run); err != nil {
			return nil, err
		}
		items = append(items, run)
	}
	return items, nil
}

func (s *SQLiteStore) ListRunsFiltered(ctx context.Context, workflowID string, status domain.RunStatus, requestID string, startedAfter time.Time, startedBefore time.Time, orderDesc bool, limit int, offset int) ([]domain.Run, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	order := `ASC`
	if orderDesc {
		order = `DESC`
	}

	query := `SELECT payload FROM runs`
	args := make([]any, 0, 8)
	clauses := make([]string, 0, 4)
	if workflowID != "" {
		clauses = append(clauses, "workflow_id = ?")
		args = append(args, workflowID)
	}
	if requestID != "" {
		clauses = append(clauses, "request_id = ?")
		args = append(args, requestID)
	}
	if !startedAfter.IsZero() {
		clauses = append(clauses, "started_at >= ?")
		args = append(args, startedAfter.UnixNano())
	}
	if !startedBefore.IsZero() {
		clauses = append(clauses, "started_at <= ?")
		args = append(args, startedBefore.UnixNano())
	}
	if len(clauses) > 0 {
		query += ` WHERE ` + strings.Join(clauses, ` AND `)
	}
	query += ` ORDER BY started_at ` + order

	if status == "" {
		query += ` LIMIT ? OFFSET ?`
		args = append(args, limit, offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.Run, 0)
	if status == "" {
		for rows.Next() {
			var payload string
			if err := rows.Scan(&payload); err != nil {
				return nil, err
			}
			var run domain.Run
			if err := json.Unmarshal([]byte(payload), &run); err != nil {
				return nil, err
			}
			items = append(items, run)
		}
		return items, nil
	}

	seen := 0
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var run domain.Run
		if err := json.Unmarshal([]byte(payload), &run); err != nil {
			return nil, err
		}
		if run.Status != status {
			continue
		}
		if seen < offset {
			seen++
			continue
		}
		if len(items) >= limit {
			break
		}
		items = append(items, run)
	}
	return items, nil
}

func (s *SQLiteStore) CountRunsFiltered(ctx context.Context, workflowID string, status domain.RunStatus, requestID string, startedAfter time.Time, startedBefore time.Time) (int, error) {
	query := `SELECT payload FROM runs`
	args := make([]any, 0, 6)
	clauses := make([]string, 0, 4)
	if workflowID != "" {
		clauses = append(clauses, "workflow_id = ?")
		args = append(args, workflowID)
	}
	if requestID != "" {
		clauses = append(clauses, "request_id = ?")
		args = append(args, requestID)
	}
	if !startedAfter.IsZero() {
		clauses = append(clauses, "started_at >= ?")
		args = append(args, startedAfter.UnixNano())
	}
	if !startedBefore.IsZero() {
		clauses = append(clauses, "started_at <= ?")
		args = append(args, startedBefore.UnixNano())
	}
	if len(clauses) > 0 {
		query += ` WHERE ` + strings.Join(clauses, ` AND `)
	}

	if status == "" {
		countQuery := `SELECT COUNT(*) FROM runs`
		if len(clauses) > 0 {
			countQuery += ` WHERE ` + strings.Join(clauses, ` AND `)
		}
		var count int
		if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&count); err != nil {
			return 0, err
		}
		return count, nil
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return 0, err
		}
		var run domain.Run
		if err := json.Unmarshal([]byte(payload), &run); err != nil {
			return 0, err
		}
		if run.Status == status {
			count++
		}
	}
	return count, nil
}

func (s *SQLiteStore) EnqueueRunJob(ctx context.Context, job domain.RunQueueJob) error {
	now := time.Now().UTC()
	if job.ID == "" {
		return errors.New("job id is required")
	}
	if job.Status == "" {
		job.Status = domain.RunJobStatusQueued
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = now
	}
	if job.AvailableAt.IsZero() {
		job.AvailableAt = now
	}
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 5
	}

	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO run_queue(id, workflow_id, run_id, request_id, status, attempts, max_attempts, last_error, lease_owner, lease_until, available_at, replay_count, replayed_by, replay_reason, replayed_at, created_at, updated_at)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO NOTHING`,
		job.ID,
		job.WorkflowID,
		job.RunID,
		job.RequestID,
		string(job.Status),
		job.Attempts,
		job.MaxAttempts,
		job.LastError,
		job.LeaseOwner,
		job.LeaseUntil.UnixNano(),
		job.AvailableAt.UnixNano(),
		job.ReplayCount,
		job.ReplayedBy,
		job.ReplayReason,
		job.ReplayedAt.UnixNano(),
		job.CreatedAt.UnixNano(),
		job.UpdatedAt.UnixNano(),
	)
	if err != nil {
		return err
	}
	if err := s.appendTransition(ctx, job.ID, "", job.Status, "enqueued", "system"); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) GetRunJob(ctx context.Context, jobID string) (domain.RunQueueJob, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, workflow_id, run_id, request_id, status, attempts, max_attempts, last_error, lease_owner, lease_until, available_at, replay_count, replayed_by, replay_reason, replayed_at, created_at, updated_at FROM run_queue WHERE id = ?`, jobID)
	return scanRunJob(row)
}

func (s *SQLiteStore) GetRunJobByRequest(ctx context.Context, workflowID string, requestID string) (domain.RunQueueJob, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, workflow_id, run_id, request_id, status, attempts, max_attempts, last_error, lease_owner, lease_until, available_at, replay_count, replayed_by, replay_reason, replayed_at, created_at, updated_at FROM run_queue WHERE workflow_id = ? AND request_id = ? ORDER BY created_at DESC LIMIT 1`, workflowID, requestID)
	return scanRunJob(row)
}

func (s *SQLiteStore) LeaseNextRunJob(ctx context.Context, workerID string, leaseDuration time.Duration) (domain.RunQueueJob, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.RunQueueJob{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC().UnixNano()
	row := tx.QueryRowContext(ctx, `SELECT id, workflow_id, run_id, request_id, status, attempts, max_attempts, last_error, lease_owner, lease_until, available_at, replay_count, replayed_by, replay_reason, replayed_at, created_at, updated_at
	FROM run_queue
	WHERE (status = ? OR (status = ? AND lease_until < ?)) AND available_at <= ?
	ORDER BY created_at ASC LIMIT 1`, string(domain.RunJobStatusQueued), string(domain.RunJobStatusLeased), now, now)
	job, err := scanRunJob(row)
	if err != nil {
		if strings.Contains(err.Error(), "not found") || errors.Is(err, sql.ErrNoRows) {
			return domain.RunQueueJob{}, errors.New("no run job available")
		}
		return domain.RunQueueJob{}, err
	}

	leaseUntil := time.Now().UTC().Add(leaseDuration).UnixNano()
	updatedAt := time.Now().UTC().UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE run_queue SET status=?, lease_owner=?, lease_until=?, attempts=attempts+1, updated_at=? WHERE id=? AND (status=? OR (status=? AND lease_until < ?))`, string(domain.RunJobStatusLeased), workerID, leaseUntil, updatedAt, job.ID, string(domain.RunJobStatusQueued), string(domain.RunJobStatusLeased), now)
	if err != nil {
		return domain.RunQueueJob{}, err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return domain.RunQueueJob{}, errors.New("run job lease lost")
	}
	if err := tx.Commit(); err != nil {
		return domain.RunQueueJob{}, err
	}

	fromStatus := job.Status
	job.Status = domain.RunJobStatusLeased
	job.LeaseOwner = workerID
	job.LeaseUntil = time.Unix(0, leaseUntil).UTC()
	job.Attempts++
	job.UpdatedAt = time.Unix(0, updatedAt).UTC()
	if err := s.appendTransition(ctx, job.ID, fromStatus, domain.RunJobStatusLeased, "leased", workerID); err != nil {
		return domain.RunQueueJob{}, err
	}
	return job, nil
}

func (s *SQLiteStore) ExtendRunJobLease(ctx context.Context, jobID string, workerID string, leaseDuration time.Duration) error {
	leaseUntil := time.Now().UTC().Add(leaseDuration).UnixNano()
	updatedAt := time.Now().UTC().UnixNano()
	result, err := s.db.ExecContext(ctx, `UPDATE run_queue SET lease_until=?, updated_at=? WHERE id=? AND status=? AND lease_owner=?`, leaseUntil, updatedAt, jobID, string(domain.RunJobStatusLeased), workerID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return errors.New("run job lease extension failed")
	}
	return nil
}

func (s *SQLiteStore) CompleteRunJob(ctx context.Context, jobID string) error {
	job, err := s.GetRunJob(ctx, jobID)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE run_queue SET status=?, lease_owner='', lease_until=0, updated_at=? WHERE id=? AND status=?`, string(domain.RunJobStatusSucceeded), time.Now().UTC().UnixNano(), jobID, string(domain.RunJobStatusLeased))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return errors.New("run job complete transition rejected")
	}
	if err := s.appendTransition(ctx, jobID, job.Status, domain.RunJobStatusSucceeded, "completed", "worker"); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) CancelRunJob(ctx context.Context, jobID string, reason string) error {
	job, err := s.GetRunJob(ctx, jobID)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE run_queue SET status=?, last_error=?, lease_owner='', lease_until=0, updated_at=? WHERE id=? AND status IN (?, ?, ?, ?)`,
		string(domain.RunJobStatusCancelled), reason, time.Now().UTC().UnixNano(), jobID,
		string(domain.RunJobStatusQueued), string(domain.RunJobStatusFailed), string(domain.RunJobStatusDead), string(domain.RunJobStatusLeased))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return errors.New("run job cancel transition rejected")
	}
	if err := s.appendTransition(ctx, jobID, job.Status, domain.RunJobStatusCancelled, reason, "api"); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) RetryRunJob(ctx context.Context, jobID string, retryAfter time.Duration, lastError string) error {
	job, err := s.GetRunJob(ctx, jobID)
	if err != nil {
		return err
	}
	availableAt := time.Now().UTC().Add(retryAfter).UnixNano()
	result, err := s.db.ExecContext(ctx, `UPDATE run_queue SET status=?, available_at=?, lease_owner='', lease_until=0, last_error=?, updated_at=? WHERE id=? AND status=?`, string(domain.RunJobStatusQueued), availableAt, lastError, time.Now().UTC().UnixNano(), jobID, string(domain.RunJobStatusLeased))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return errors.New("run job retry transition rejected")
	}
	if err := s.appendTransition(ctx, jobID, job.Status, domain.RunJobStatusQueued, lastError, "worker"); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) FailRunJob(ctx context.Context, jobID string, lastError string) error {
	job, err := s.GetRunJob(ctx, jobID)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE run_queue SET status=?, lease_owner='', lease_until=0, last_error=?, updated_at=? WHERE id=? AND status=?`, string(domain.RunJobStatusFailed), lastError, time.Now().UTC().UnixNano(), jobID, string(domain.RunJobStatusLeased))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return errors.New("run job fail transition rejected")
	}
	if err := s.appendTransition(ctx, jobID, job.Status, domain.RunJobStatusFailed, lastError, "worker"); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) DeadLetterRunJob(ctx context.Context, jobID string, lastError string) error {
	job, err := s.GetRunJob(ctx, jobID)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE run_queue SET status=?, lease_owner='', lease_until=0, last_error=?, updated_at=? WHERE id=? AND status=?`, string(domain.RunJobStatusDead), lastError, time.Now().UTC().UnixNano(), jobID, string(domain.RunJobStatusLeased))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return errors.New("run job dead-letter transition rejected")
	}
	if err := s.appendTransition(ctx, jobID, job.Status, domain.RunJobStatusDead, lastError, "worker"); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) RequeueRunJob(ctx context.Context, jobID string, availableAfter time.Duration, replayedBy string, replayReason string) error {
	job, err := s.GetRunJob(ctx, jobID)
	if err != nil {
		return err
	}
	availableAt := time.Now().UTC().Add(availableAfter).UnixNano()
	replayedAt := time.Now().UTC().UnixNano()
	result, err := s.db.ExecContext(ctx, `UPDATE run_queue SET status=?, available_at=?, lease_owner='', lease_until=0, replay_count=replay_count+1, replayed_by=?, replay_reason=?, replayed_at=?, updated_at=? WHERE id=? AND status IN (?, ?)`, string(domain.RunJobStatusQueued), availableAt, replayedBy, replayReason, replayedAt, time.Now().UTC().UnixNano(), jobID, string(domain.RunJobStatusDead), string(domain.RunJobStatusFailed))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return errors.New("run job requeue transition rejected")
	}
	if err := s.appendTransition(ctx, jobID, job.Status, domain.RunJobStatusQueued, replayReason, replayedBy); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) ListRunJobs(ctx context.Context, status string, limit int, offset int) ([]domain.RunQueueJob, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	query := `SELECT id, workflow_id, run_id, request_id, status, attempts, max_attempts, last_error, lease_owner, lease_until, available_at, replay_count, replayed_by, replay_reason, replayed_at, created_at, updated_at FROM run_queue`
	args := []any{}
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY created_at ASC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.RunQueueJob, 0)
	for rows.Next() {
		var (
			job         domain.RunQueueJob
			statusValue string
			leaseUntil  int64
			availableAt int64
			replayedAt  int64
			createdAt   int64
			updatedAt   int64
		)
		if err := rows.Scan(&job.ID, &job.WorkflowID, &job.RunID, &job.RequestID, &statusValue, &job.Attempts, &job.MaxAttempts, &job.LastError, &job.LeaseOwner, &leaseUntil, &availableAt, &job.ReplayCount, &job.ReplayedBy, &job.ReplayReason, &replayedAt, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		job.Status = domain.RunJobStatus(statusValue)
		job.LeaseUntil = time.Unix(0, leaseUntil).UTC()
		job.AvailableAt = time.Unix(0, availableAt).UTC()
		job.ReplayedAt = time.Unix(0, replayedAt).UTC()
		job.CreatedAt = time.Unix(0, createdAt).UTC()
		job.UpdatedAt = time.Unix(0, updatedAt).UTC()
		items = append(items, job)
	}
	return items, nil
}

func (s *SQLiteStore) ListRunJobsFiltered(ctx context.Context, status string, workflowID string, requestID string, updatedAfter time.Time, updatedBefore time.Time, limit int, offset int) ([]domain.RunQueueJob, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	query := `SELECT id, workflow_id, run_id, request_id, status, attempts, max_attempts, last_error, lease_owner, lease_until, available_at, replay_count, replayed_by, replay_reason, replayed_at, created_at, updated_at FROM run_queue`
	args := make([]any, 0, 8)
	clauses := make([]string, 0, 5)
	if status != "" {
		clauses = append(clauses, "status = ?")
		args = append(args, status)
	}
	if workflowID != "" {
		clauses = append(clauses, "workflow_id = ?")
		args = append(args, workflowID)
	}
	if requestID != "" {
		clauses = append(clauses, "request_id = ?")
		args = append(args, requestID)
	}
	if !updatedAfter.IsZero() {
		clauses = append(clauses, "updated_at >= ?")
		args = append(args, updatedAfter.UnixNano())
	}
	if !updatedBefore.IsZero() {
		clauses = append(clauses, "updated_at <= ?")
		args = append(args, updatedBefore.UnixNano())
	}
	if len(clauses) > 0 {
		query += ` WHERE ` + strings.Join(clauses, ` AND `)
	}
	query += ` ORDER BY created_at ASC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.RunQueueJob, 0)
	for rows.Next() {
		var (
			job         domain.RunQueueJob
			statusValue string
			leaseUntil  int64
			availableAt int64
			replayedAt  int64
			createdAt   int64
			updatedAt   int64
		)
		if err := rows.Scan(&job.ID, &job.WorkflowID, &job.RunID, &job.RequestID, &statusValue, &job.Attempts, &job.MaxAttempts, &job.LastError, &job.LeaseOwner, &leaseUntil, &availableAt, &job.ReplayCount, &job.ReplayedBy, &job.ReplayReason, &replayedAt, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		job.Status = domain.RunJobStatus(statusValue)
		job.LeaseUntil = time.Unix(0, leaseUntil).UTC()
		job.AvailableAt = time.Unix(0, availableAt).UTC()
		job.ReplayedAt = time.Unix(0, replayedAt).UTC()
		job.CreatedAt = time.Unix(0, createdAt).UTC()
		job.UpdatedAt = time.Unix(0, updatedAt).UTC()
		items = append(items, job)
	}
	return items, nil
}

func (s *SQLiteStore) CountRunJobsFiltered(ctx context.Context, status string, workflowID string, requestID string, updatedAfter time.Time, updatedBefore time.Time) (int, error) {
	query := `SELECT COUNT(*) FROM run_queue`
	args := make([]any, 0, 6)
	clauses := make([]string, 0, 5)
	if status != "" {
		clauses = append(clauses, "status = ?")
		args = append(args, status)
	}
	if workflowID != "" {
		clauses = append(clauses, "workflow_id = ?")
		args = append(args, workflowID)
	}
	if requestID != "" {
		clauses = append(clauses, "request_id = ?")
		args = append(args, requestID)
	}
	if !updatedAfter.IsZero() {
		clauses = append(clauses, "updated_at >= ?")
		args = append(args, updatedAfter.UnixNano())
	}
	if !updatedBefore.IsZero() {
		clauses = append(clauses, "updated_at <= ?")
		args = append(args, updatedBefore.UnixNano())
	}
	if len(clauses) > 0 {
		query += ` WHERE ` + strings.Join(clauses, ` AND `)
	}

	var count int
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *SQLiteStore) ListRunJobsByRun(ctx context.Context, runID string, limit int, offset int) ([]domain.RunQueueJob, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, workflow_id, run_id, request_id, status, attempts, max_attempts, last_error, lease_owner, lease_until, available_at, replay_count, replayed_by, replay_reason, replayed_at, created_at, updated_at FROM run_queue WHERE run_id = ? ORDER BY created_at ASC LIMIT ? OFFSET ?`, runID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.RunQueueJob, 0)
	for rows.Next() {
		var (
			job         domain.RunQueueJob
			statusValue string
			leaseUntil  int64
			availableAt int64
			replayedAt  int64
			createdAt   int64
			updatedAt   int64
		)
		if err := rows.Scan(&job.ID, &job.WorkflowID, &job.RunID, &job.RequestID, &statusValue, &job.Attempts, &job.MaxAttempts, &job.LastError, &job.LeaseOwner, &leaseUntil, &availableAt, &job.ReplayCount, &job.ReplayedBy, &job.ReplayReason, &replayedAt, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		job.Status = domain.RunJobStatus(statusValue)
		job.LeaseUntil = time.Unix(0, leaseUntil).UTC()
		job.AvailableAt = time.Unix(0, availableAt).UTC()
		job.ReplayedAt = time.Unix(0, replayedAt).UTC()
		job.CreatedAt = time.Unix(0, createdAt).UTC()
		job.UpdatedAt = time.Unix(0, updatedAt).UTC()
		items = append(items, job)
	}
	return items, nil
}

func (s *SQLiteStore) CountRunJobsByStatus(ctx context.Context) (map[domain.RunJobStatus]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM run_queue GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := map[domain.RunJobStatus]int{}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[domain.RunJobStatus(status)] = count
	}
	return counts, nil
}

func (s *SQLiteStore) PurgeRunJobs(ctx context.Context, status string, olderThan time.Duration, limit int) (int, error) {
	if limit <= 0 {
		limit = 1000
	}

	query := `SELECT id FROM run_queue`
	args := make([]any, 0, 3)
	clauses := make([]string, 0, 2)
	if status != "" {
		clauses = append(clauses, "status = ?")
		args = append(args, status)
	}
	if olderThan > 0 {
		cutoff := time.Now().UTC().Add(-olderThan).UnixNano()
		clauses = append(clauses, "updated_at <= ?")
		args = append(args, cutoff)
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += ` ORDER BY updated_at ASC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	deleted := 0
	for _, id := range ids {
		res, err := tx.ExecContext(ctx, `DELETE FROM run_queue WHERE id = ?`, id)
		if err != nil {
			return 0, err
		}
		affected, _ := res.RowsAffected()
		deleted += int(affected)
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return deleted, nil
}

func (s *SQLiteStore) ListRunJobTransitions(ctx context.Context, jobID string, limit int, offset int) ([]domain.RunJobTransition, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.db.QueryContext(ctx, `SELECT job_id, from_status, to_status, reason, actor, created_at FROM run_queue_transitions WHERE job_id = ? ORDER BY created_at ASC LIMIT ? OFFSET ?`, jobID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.RunJobTransition, 0)
	for rows.Next() {
		var item domain.RunJobTransition
		var fromStatus string
		var toStatus string
		var createdAt int64
		if err := rows.Scan(&item.JobID, &fromStatus, &toStatus, &item.Reason, &item.Actor, &createdAt); err != nil {
			return nil, err
		}
		item.FromStatus = domain.RunJobStatus(fromStatus)
		item.ToStatus = domain.RunJobStatus(toStatus)
		item.CreatedAt = time.Unix(0, createdAt).UTC()
		items = append(items, item)
	}
	return items, nil
}

func (s *SQLiteStore) ListRunJobTransitionsFiltered(ctx context.Context, jobID string, toStatus string, actor string, createdAfter time.Time, createdBefore time.Time, limit int, offset int) ([]domain.RunJobTransition, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	query := `SELECT job_id, from_status, to_status, reason, actor, created_at FROM run_queue_transitions WHERE job_id = ?`
	args := []any{jobID}
	if toStatus != "" {
		query += ` AND to_status = ?`
		args = append(args, toStatus)
	}
	if actor != "" {
		query += ` AND actor = ?`
		args = append(args, actor)
	}
	if !createdAfter.IsZero() {
		query += ` AND created_at >= ?`
		args = append(args, createdAfter.UnixNano())
	}
	if !createdBefore.IsZero() {
		query += ` AND created_at <= ?`
		args = append(args, createdBefore.UnixNano())
	}
	query += ` ORDER BY created_at ASC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.RunJobTransition, 0)
	for rows.Next() {
		var item domain.RunJobTransition
		var fromStatus string
		var statusValue string
		var createdAt int64
		if err := rows.Scan(&item.JobID, &fromStatus, &statusValue, &item.Reason, &item.Actor, &createdAt); err != nil {
			return nil, err
		}
		item.FromStatus = domain.RunJobStatus(fromStatus)
		item.ToStatus = domain.RunJobStatus(statusValue)
		item.CreatedAt = time.Unix(0, createdAt).UTC()
		items = append(items, item)
	}
	return items, nil
}

func (s *SQLiteStore) CountRunJobTransitionsFiltered(ctx context.Context, jobID string, toStatus string, actor string, createdAfter time.Time, createdBefore time.Time) (int, error) {
	query := `SELECT COUNT(*) FROM run_queue_transitions WHERE job_id = ?`
	args := []any{jobID}
	if toStatus != "" {
		query += ` AND to_status = ?`
		args = append(args, toStatus)
	}
	if actor != "" {
		query += ` AND actor = ?`
		args = append(args, actor)
	}
	if !createdAfter.IsZero() {
		query += ` AND created_at >= ?`
		args = append(args, createdAfter.UnixNano())
	}
	if !createdBefore.IsZero() {
		query += ` AND created_at <= ?`
		args = append(args, createdBefore.UnixNano())
	}
	var count int
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *SQLiteStore) appendTransition(ctx context.Context, jobID string, fromStatus domain.RunJobStatus, toStatus domain.RunJobStatus, reason string, actor string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO run_queue_transitions(job_id, from_status, to_status, reason, actor, created_at) VALUES(?, ?, ?, ?, ?, ?)`,
		jobID,
		string(fromStatus),
		string(toStatus),
		reason,
		actor,
		time.Now().UTC().UnixNano(),
	)
	return err
}

func scanRunJob(row scanner) (domain.RunQueueJob, error) {
	var (
		job         domain.RunQueueJob
		statusValue string
		leaseUntil  int64
		availableAt int64
		replayedAt  int64
		createdAt   int64
		updatedAt   int64
	)
	err := row.Scan(&job.ID, &job.WorkflowID, &job.RunID, &job.RequestID, &statusValue, &job.Attempts, &job.MaxAttempts, &job.LastError, &job.LeaseOwner, &leaseUntil, &availableAt, &job.ReplayCount, &job.ReplayedBy, &job.ReplayReason, &replayedAt, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.RunQueueJob{}, errors.New("run job not found")
		}
		return domain.RunQueueJob{}, err
	}
	job.Status = domain.RunJobStatus(statusValue)
	job.LeaseUntil = time.Unix(0, leaseUntil).UTC()
	job.AvailableAt = time.Unix(0, availableAt).UTC()
	job.ReplayedAt = time.Unix(0, replayedAt).UTC()
	job.CreatedAt = time.Unix(0, createdAt).UTC()
	job.UpdatedAt = time.Unix(0, updatedAt).UTC()
	return job, nil
}

type scanner interface {
	Scan(dest ...any) error
}
