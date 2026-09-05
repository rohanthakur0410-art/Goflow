// Package jobs defines the core Job domain model shared across the API,
// queue, database, and worker layers, plus the handler registry that maps a
// job Type to the function that executes it.
package jobs

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Status represents the lifecycle state of a job.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusScheduled Status = "SCHEDULED"
	StatusRunning   Status = "RUNNING"
	StatusCompleted Status = "COMPLETED"
	StatusFailed    Status = "FAILED"
	StatusRetrying  Status = "RETRYING"
	StatusCancelled Status = "CANCELLED"
	StatusDead      Status = "DEAD"
)

// Priority represents job scheduling priority. Lower Weight() means it is
// processed sooner.
type Priority string

const (
	PriorityHigh   Priority = "HIGH"
	PriorityMedium Priority = "MEDIUM"
	PriorityLow    Priority = "LOW"
)

// Weight returns the heap weight for a priority: lower sorts first.
func (p Priority) Weight() int {
	switch p {
	case PriorityHigh:
		return 0
	case PriorityMedium:
		return 1
	case PriorityLow:
		return 2
	default:
		return 1 // unknown priorities behave like MEDIUM
	}
}

// Valid reports whether p is one of the three known priority levels.
func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityMedium, PriorityLow:
		return true
	}
	return false
}

// Job is the canonical representation of a unit of work in GoFlow. The same
// struct is used for API responses, in-memory queue payload lookups, and
// database rows.
type Job struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Priority     Priority        `json:"priority"`
	Status       Status          `json:"status"`
	CreatedAt    time.Time       `json:"created_at"`
	StartedAt    *time.Time      `json:"started_at,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
	ScheduledAt  *time.Time      `json:"scheduled_at,omitempty"`
	RetryCount   int             `json:"retry_count"`
	MaxRetries   int             `json:"max_retries"`
	TimeoutSecs  int             `json:"timeout_seconds"`
	ErrorMessage string          `json:"error_message,omitempty"`
	WorkerID     *string         `json:"worker_id,omitempty"`
	CreatedBy    int64           `json:"-"`
}

// Timeout returns the job's execution timeout as a time.Duration.
func (j *Job) Timeout() time.Duration {
	return time.Duration(j.TimeoutSecs) * time.Second
}

// IsTerminal reports whether the job has reached a state it cannot leave.
func (j *Job) IsTerminal() bool {
	return j.Status.IsTerminal()
}

// IsTerminal reports whether a status is a terminal (non-transitioning)
// state: COMPLETED, CANCELLED, or DEAD.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusCompleted, StatusCancelled, StatusDead:
		return true
	}
	return false
}

// NewID generates a URL-safe, collision-resistant job identifier without
// pulling in an external UUID dependency. It is not a RFC 4122 UUID, but it
// gives the same practical guarantees for this system's scale.
func NewID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing is effectively unrecoverable, but fall back to
		// a timestamp-based id rather than panicking.
		return fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("job_%s%d", hex.EncodeToString(buf), time.Now().UnixNano()%1000)
}
