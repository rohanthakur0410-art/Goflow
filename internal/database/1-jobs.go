package database

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"goflow/internal/jobs"
)

// ErrNotFound is returned when a lookup by ID matches no row.
var ErrNotFound = errors.New("not found")

// ErrConflict is returned when a state-transition precondition is not met,
// e.g. trying to claim a job that another worker already claimed.
var ErrConflict = errors.New("conflict")

// CreateJob inserts a new job row. The caller is responsible for choosing an
// initial Status (PENDING for immediate jobs, SCHEDULED for future ones).
func (s *Store) CreateJob(j *jobs.Job) error {
	_, err := s.DB.Exec(`
		INSERT INTO jobs (id, type, payload, priority, status, created_at,
			scheduled_at, retry_count, max_retries, timeout_secs, error_message, created_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.Type, j.Payload, string(j.Priority), string(j.Status), j.CreatedAt,
		j.ScheduledAt, j.RetryCount, j.MaxRetries, j.TimeoutSecs, j.ErrorMessage, j.CreatedBy,
	)
	if err != nil {
		return fmt.Errorf("inserting job: %w", err)
	}
	return nil
}

// GetJob loads a single job by ID.
func (s *Store) GetJob(id string) (*jobs.Job, error) {
	row := s.DB.QueryRow(`
		SELECT id, type, payload, priority, status, created_at, started_at,
			completed_at, scheduled_at, retry_count, max_retries, timeout_secs,
			error_message, worker_id, created_by
		FROM jobs WHERE id = ?`, id)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

// ListJobs returns jobs, optionally filtered by status, newest first,
// capped at limit (0 means "use a sensible default").
func (s *Store) ListJobs(status string, limit int) ([]*jobs.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT id, type, payload, priority, status, created_at, started_at,
			completed_at, scheduled_at, retry_count, max_retries, timeout_secs,
			error_message, worker_id, created_by
		FROM jobs`
	args := []any{}
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("listing jobs: %w", err)
	}
	defer rows.Close()

	var out []*jobs.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// DeleteJob removes a job row entirely (its attempts cascade-delete).
func (s *Store) DeleteJob(id string) error {
	res, err := s.DB.Exec(`DELETE FROM jobs WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("deleting job: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ClaimJob atomically transitions a job into RUNNING for the given worker,
// but only if it is currently in a claimable state (PENDING or RETRYING).
// This, combined with the fact that each job ID is only ever popped off the
// in-memory queue once, is what guarantees two workers can never process the
// same job concurrently: the queue provides the fast path, this transaction
// is the source-of-truth safety net.
func (s *Store) ClaimJob(id, workerID string) (*jobs.Job, error) {
	tx, err := s.DB.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRow(`
		SELECT id, type, payload, priority, status, created_at, started_at,
			completed_at, scheduled_at, retry_count, max_retries, timeout_secs,
			error_message, worker_id, created_by
		FROM jobs WHERE id = ?`, id)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if j.Status != jobs.StatusPending && j.Status != jobs.StatusRetrying {
		return nil, ErrConflict
	}

	now := time.Now().UTC()
	res, err := tx.Exec(`
		UPDATE jobs SET status = ?, started_at = ?, worker_id = ?
		WHERE id = ? AND status = ?`,
		string(jobs.StatusRunning), now, workerID, id, string(j.Status))
	if err != nil {
		return nil, fmt.Errorf("claiming job: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Someone else claimed it between our SELECT and UPDATE.
		return nil, ErrConflict
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	j.Status = jobs.StatusRunning
	j.StartedAt = &now
	j.WorkerID = &workerID
	return j, nil
}

// CompleteJob marks a job COMPLETED.
func (s *Store) CompleteJob(id string) error {
	now := time.Now().UTC()
	_, err := s.DB.Exec(`UPDATE jobs SET status = ?, completed_at = ?, error_message = '' WHERE id = ?`,
		string(jobs.StatusCompleted), now, id)
	return err
}

// FailJob records a failed attempt. If retries remain it transitions the job
// to RETRYING and bumps retry_count; otherwise it transitions to DEAD.
// Returns the job's new status and current retry_count for the caller to
// decide when to re-enqueue.
func (s *Store) FailJob(id, errMsg string) (newStatus jobs.Status, retryCount int, err error) {
	tx, txErr := s.DB.Begin()
	if txErr != nil {
		return "", 0, txErr
	}
	defer tx.Rollback()

	var status string
	var retries, maxRetries int
	if err := tx.QueryRow(`SELECT status, retry_count, max_retries FROM jobs WHERE id = ?`, id).
		Scan(&status, &retries, &maxRetries); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, ErrNotFound
		}
		return "", 0, err
	}

	// A cancelled job that fails mid-flight (context.Canceled) should stay
	// CANCELLED, not bounce into FAILED/RETRYING.
	if status == string(jobs.StatusCancelled) {
		if err := tx.Commit(); err != nil {
			return "", 0, err
		}
		return jobs.StatusCancelled, retries, nil
	}

	retries++
	var next jobs.Status
	now := time.Now().UTC()
	if retries > maxRetries {
		next = jobs.StatusDead
		if _, err := tx.Exec(`UPDATE jobs SET status = ?, retry_count = ?, error_message = ?, completed_at = ? WHERE id = ?`,
			string(next), retries, errMsg, now, id); err != nil {
			return "", 0, err
		}
	} else {
		next = jobs.StatusRetrying
		if _, err := tx.Exec(`UPDATE jobs SET status = ?, retry_count = ?, error_message = ? WHERE id = ?`,
			string(next), retries, errMsg, id); err != nil {
			return "", 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return "", 0, err
	}
	return next, retries, nil
}

// CancelJob marks a job CANCELLED if it is not already in a terminal state.
// Returns the job's status *before* cancellation so the caller can decide
// whether a running worker needs to be interrupted via context cancellation.
func (s *Store) CancelJob(id string) (previousStatus jobs.Status, err error) {
	tx, txErr := s.DB.Begin()
	if txErr != nil {
		return "", txErr
	}
	defer tx.Rollback()

	var status string
	if err := tx.QueryRow(`SELECT status FROM jobs WHERE id = ?`, id).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	prev := jobs.Status(status)
	if prev.IsTerminal() {
		return prev, ErrConflict
	}

	now := time.Now().UTC()
	if _, err := tx.Exec(`UPDATE jobs SET status = ?, completed_at = ? WHERE id = ?`,
		string(jobs.StatusCancelled), now, id); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return prev, nil
}

// RequeueDeadJob resets a DEAD job back to PENDING with a fresh retry
// budget, for manual operator-triggered recovery.
func (s *Store) RequeueDeadJob(id string) (*jobs.Job, error) {
	tx, err := s.DB.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var status string
	if err := tx.QueryRow(`SELECT status FROM jobs WHERE id = ?`, id).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if status != string(jobs.StatusDead) && status != string(jobs.StatusFailed) {
		return nil, ErrConflict
	}

	if _, err := tx.Exec(`UPDATE jobs SET status = ?, retry_count = 0, error_message = '', worker_id = NULL, completed_at = NULL WHERE id = ?`,
		string(jobs.StatusPending), id); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetJob(id)
}

// ResetOrphanedRunningJobs flips any job still marked RUNNING back to
// PENDING and re-enqueues it. Called once at startup: a RUNNING row can only
// mean a previous process crashed or was killed mid-job, since a clean
// shutdown always drives jobs to a terminal state or leaves them for the
// scheduler/retry timer to re-submit. Returns the affected job IDs and
// priorities so the caller can push them onto the in-memory queue.
func (s *Store) ResetOrphanedRunningJobs() ([]*jobs.Job, error) {
	rows, err := s.DB.Query(`
		SELECT id, type, payload, priority, status, created_at, started_at,
			completed_at, scheduled_at, retry_count, max_retries, timeout_secs,
			error_message, worker_id, created_by
		FROM jobs WHERE status = ?`, string(jobs.StatusRunning))
	if err != nil {
		return nil, err
	}
	var orphans []*jobs.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		orphans = append(orphans, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, j := range orphans {
		if _, err := s.DB.Exec(`UPDATE jobs SET status = ?, worker_id = NULL, started_at = NULL WHERE id = ? AND status = ?`,
			string(jobs.StatusPending), j.ID, string(jobs.StatusRunning)); err != nil {
			return nil, err
		}
		j.Status = jobs.StatusPending
	}
	return orphans, nil
}

// DueScheduledJobs returns SCHEDULED jobs whose scheduled_at is <= now,
// used by the scheduler goroutine to promote them into PENDING + enqueue.
func (s *Store) DueScheduledJobs(now time.Time, limit int) ([]*jobs.Job, error) {
	rows, err := s.DB.Query(`
		SELECT id, type, payload, priority, status, created_at, started_at,
			completed_at, scheduled_at, retry_count, max_retries, timeout_secs,
			error_message, worker_id, created_by
		FROM jobs WHERE status = ? AND scheduled_at <= ? ORDER BY scheduled_at ASC LIMIT ?`,
		string(jobs.StatusScheduled), now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*jobs.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// PromoteScheduledJob flips a SCHEDULED job to PENDING once the scheduler
// has decided it is due, so it can be pushed onto the in-memory queue.
func (s *Store) PromoteScheduledJob(id string) error {
	res, err := s.DB.Exec(`UPDATE jobs SET status = ? WHERE id = ? AND status = ?`,
		string(jobs.StatusPending), id, string(jobs.StatusScheduled))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrConflict
	}
	return nil
}

// RecordAttemptStart inserts a new job_attempts row and returns its ID.
func (s *Store) RecordAttemptStart(jobID, workerID string, attemptNumber int, startedAt time.Time) (int64, error) {
	res, err := s.DB.Exec(`
		INSERT INTO job_attempts (job_id, attempt_number, worker_id, started_at, status)
		VALUES (?, ?, ?, ?, ?)`,
		jobID, attemptNumber, workerID, startedAt, string(jobs.StatusRunning))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// RecordAttemptEnd finalizes a job_attempts row with its outcome.
func (s *Store) RecordAttemptEnd(attemptID int64, status jobs.Status, errMsg string, completedAt time.Time, durationMS int64) error {
	_, err := s.DB.Exec(`
		UPDATE job_attempts SET status = ?, error_message = ?, completed_at = ?, duration_ms = ?
		WHERE id = ?`, string(status), errMsg, completedAt, durationMS, attemptID)
	return err
}

// ListAttempts returns every recorded attempt for a job, oldest first.
func (s *Store) ListAttempts(jobID string) ([]JobAttempt, error) {
	rows, err := s.DB.Query(`
		SELECT id, job_id, attempt_number, worker_id, started_at, completed_at, status, error_message, duration_ms
		FROM job_attempts WHERE job_id = ? ORDER BY attempt_number ASC`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []JobAttempt
	for rows.Next() {
		var a JobAttempt
		var completedAt sql.NullTime
		if err := rows.Scan(&a.ID, &a.JobID, &a.AttemptNumber, &a.WorkerID, &a.StartedAt, &completedAt, &a.Status, &a.ErrorMessage, &a.DurationMS); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			a.CompletedAt = &completedAt.Time
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ListWorkerAttempts returns the most recent attempts recorded for a worker,
// newest first, capped at limit rows (a non-positive limit falls back to 50).
func (s *Store) ListWorkerAttempts(workerID string, limit int) ([]JobAttempt, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.DB.Query(`
		SELECT id, job_id, attempt_number, worker_id, started_at, completed_at, status, error_message, duration_ms
		FROM job_attempts WHERE worker_id = ? ORDER BY started_at DESC, id DESC LIMIT ?`, workerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []JobAttempt
	for rows.Next() {
		var a JobAttempt
		var completedAt sql.NullTime
		if err := rows.Scan(&a.ID, &a.JobID, &a.AttemptNumber, &a.WorkerID, &a.StartedAt, &completedAt, &a.Status, &a.ErrorMessage, &a.DurationMS); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			a.CompletedAt = &completedAt.Time
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// JobAttempt mirrors the job_attempts table for API responses.
type JobAttempt struct {
	ID            int64      `json:"id"`
	JobID         string     `json:"job_id"`
	AttemptNumber int        `json:"attempt_number"`
	WorkerID      string     `json:"worker_id"`
	StartedAt     time.Time  `json:"started_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	Status        string     `json:"status"`
	ErrorMessage  string     `json:"error_message,omitempty"`
	DurationMS    int64      `json:"duration_ms"`
}

// JobStats aggregates counts used by GET /api/stats and the dashboard.
type JobStats struct {
	Total           int     `json:"total"`
	Pending         int     `json:"pending"`
	Scheduled       int     `json:"scheduled"`
	Running         int     `json:"running"`
	Completed       int     `json:"completed"`
	Failed          int     `json:"failed"`
	Retrying        int     `json:"retrying"`
	Cancelled       int     `json:"cancelled"`
	Dead            int     `json:"dead"`
	AvgProcessingMS float64 `json:"avg_processing_ms"`
}

// Stats computes job counts by status and average completed-job duration.
func (s *Store) Stats() (*JobStats, error) {
	rows, err := s.DB.Query(`SELECT status, COUNT(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := &JobStats{}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		stats.Total += count
		switch jobs.Status(status) {
		case jobs.StatusPending:
			stats.Pending = count
		case jobs.StatusScheduled:
			stats.Scheduled = count
		case jobs.StatusRunning:
			stats.Running = count
		case jobs.StatusCompleted:
			stats.Completed = count
		case jobs.StatusFailed:
			stats.Failed = count
		case jobs.StatusRetrying:
			stats.Retrying = count
		case jobs.StatusCancelled:
			stats.Cancelled = count
		case jobs.StatusDead:
			stats.Dead = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var avg sql.NullFloat64
	err = s.DB.QueryRow(`
		SELECT AVG((julianday(completed_at) - julianday(started_at)) * 86400000.0)
		FROM jobs WHERE status = 'COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL
	`).Scan(&avg)
	if err != nil {
		return nil, err
	}
	if avg.Valid {
		stats.AvgProcessingMS = avg.Float64
	}
	return stats, nil
}

// rowScanner abstracts over *sql.Row and *sql.Rows for scanJob.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (*jobs.Job, error) {
	var j jobs.Job
	var priority, status string
	var startedAt, completedAt, scheduledAt sql.NullTime
	var workerID sql.NullString
	var createdBy sql.NullInt64

	err := row.Scan(&j.ID, &j.Type, &j.Payload, &priority, &status, &j.CreatedAt,
		&startedAt, &completedAt, &scheduledAt, &j.RetryCount, &j.MaxRetries,
		&j.TimeoutSecs, &j.ErrorMessage, &workerID, &createdBy)
	if err != nil {
		return nil, err
	}

	j.Priority = jobs.Priority(priority)
	j.Status = jobs.Status(status)
	if startedAt.Valid {
		t := startedAt.Time
		j.StartedAt = &t
	}
	if completedAt.Valid {
		t := completedAt.Time
		j.CompletedAt = &t
	}
	if scheduledAt.Valid {
		t := scheduledAt.Time
		j.ScheduledAt = &t
	}
	if workerID.Valid {
		w := workerID.String
		j.WorkerID = &w
	}
	if createdBy.Valid {
		j.CreatedBy = createdBy.Int64
	}
	return &j, nil
}
