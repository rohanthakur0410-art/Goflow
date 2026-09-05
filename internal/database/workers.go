package database

import (
	"database/sql"
	"errors"
	"time"
)

// WorkerRecord mirrors the workers table for API responses and pool
// bookkeeping.
type WorkerRecord struct {
	ID            string     `json:"id"`
	Status        string     `json:"status"` // IDLE, BUSY, STOPPED, STALE
	CurrentJobID  *string    `json:"current_job_id,omitempty"`
	JobsProcessed int        `json:"jobs_processed"`
	JobsFailed    int        `json:"jobs_failed"`
	StartedAt     time.Time  `json:"started_at"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	StoppedAt     *time.Time `json:"stopped_at,omitempty"`
}

// RegisterWorker inserts (or resurrects) a worker row when a worker
// goroutine starts.
func (s *Store) RegisterWorker(id string, startedAt time.Time) error {
	_, err := s.DB.Exec(`
		INSERT INTO workers (id, status, jobs_processed, jobs_failed, started_at, last_heartbeat)
		VALUES (?, 'IDLE', 0, 0, ?, ?)
		ON CONFLICT(id) DO UPDATE SET status = 'IDLE', started_at = excluded.started_at,
			last_heartbeat = excluded.last_heartbeat, stopped_at = NULL`,
		id, startedAt, startedAt)
	return err
}

// SetWorkerBusy marks a worker BUSY with the job it just claimed.
func (s *Store) SetWorkerBusy(id, jobID string) error {
	_, err := s.DB.Exec(`UPDATE workers SET status = 'BUSY', current_job_id = ? WHERE id = ?`, jobID, id)
	return err
}

// SetWorkerIdle marks a worker IDLE and records the outcome of the job it
// just finished.
func (s *Store) SetWorkerIdle(id string, succeeded bool) error {
	if succeeded {
		_, err := s.DB.Exec(`UPDATE workers SET status = 'IDLE', current_job_id = NULL, jobs_processed = jobs_processed + 1 WHERE id = ?`, id)
		return err
	}
	_, err := s.DB.Exec(`UPDATE workers SET status = 'IDLE', current_job_id = NULL, jobs_failed = jobs_failed + 1 WHERE id = ?`, id)
	return err
}

// StopWorker marks a worker STOPPED, used both for graceful shutdown and for
// scale-down.
func (s *Store) StopWorker(id string) error {
	_, err := s.DB.Exec(`UPDATE workers SET status = 'STOPPED', stopped_at = ? WHERE id = ?`, time.Now().UTC(), id)
	return err
}

// Heartbeat records a liveness pulse from a worker, both updating the
// worker's last_heartbeat column and appending to the worker_heartbeats
// audit log. The log is pruned to the most recent 50 rows per worker so it
// never grows unbounded.
func (s *Store) Heartbeat(id string, at time.Time) error {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`UPDATE workers SET last_heartbeat = ? WHERE id = ?`, at, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO worker_heartbeats (worker_id, heartbeat_at) VALUES (?, ?)`, id, at); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		DELETE FROM worker_heartbeats WHERE worker_id = ? AND id NOT IN (
			SELECT id FROM worker_heartbeats WHERE worker_id = ? ORDER BY id DESC LIMIT 50
		)`, id, id); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkStaleWorkers flips any worker whose last_heartbeat is older than
// olderThan to STALE, and returns their IDs. Called periodically by the pool
// monitor.
func (s *Store) MarkStaleWorkers(olderThan time.Time) ([]string, error) {
	rows, err := s.DB.Query(`SELECT id FROM workers WHERE last_heartbeat < ? AND status NOT IN ('STOPPED','STALE')`, olderThan)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()

	if len(ids) == 0 {
		return nil, nil
	}
	_, err = s.DB.Exec(`UPDATE workers SET status = 'STALE' WHERE last_heartbeat < ? AND status NOT IN ('STOPPED','STALE')`, olderThan)
	return ids, err
}

// GetWorker loads a single worker by ID.
func (s *Store) GetWorker(id string) (*WorkerRecord, error) {
	row := s.DB.QueryRow(`
		SELECT id, status, current_job_id, jobs_processed, jobs_failed, started_at, last_heartbeat, stopped_at
		FROM workers WHERE id = ?`, id)
	w, err := scanWorker(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return w, err
}

// ListWorkers returns every worker, most recently started first.
func (s *Store) ListWorkers() ([]*WorkerRecord, error) {
	rows, err := s.DB.Query(`
		SELECT id, status, current_job_id, jobs_processed, jobs_failed, started_at, last_heartbeat, stopped_at
		FROM workers ORDER BY started_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*WorkerRecord
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func scanWorker(row rowScanner) (*WorkerRecord, error) {
	var w WorkerRecord
	var currentJobID sql.NullString
	var stoppedAt sql.NullTime
	err := row.Scan(&w.ID, &w.Status, &currentJobID, &w.JobsProcessed, &w.JobsFailed, &w.StartedAt, &w.LastHeartbeat, &stoppedAt)
	if err != nil {
		return nil, err
	}
	if currentJobID.Valid {
		v := currentJobID.String
		w.CurrentJobID = &v
	}
	if stoppedAt.Valid {
		t := stoppedAt.Time
		w.StoppedAt = &t
	}
	return &w, nil
}
