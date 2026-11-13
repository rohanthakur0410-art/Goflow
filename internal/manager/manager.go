// Package manager implements the "Job Manager" layer from GoFlow's
// architecture diagram: it sits between the REST API and the priority
// queue/database, and is the only place that is allowed to push job IDs
// onto the queue or reach into SQLite for state transitions. Keeping that
// logic in one place is what lets both the API and the worker pool share
// identical retry/cancel/complete semantics.
package manager

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"goflow/internal/database"
	"goflow/internal/jobs"
	"goflow/internal/metrics"
	"goflow/internal/queue"
	"goflow/internal/validation"
)

// Manager coordinates job submission, claiming, completion, retries, and
// cancellation across the queue and the database.
type Manager struct {
	store   *database.Store
	queue   *queue.Queue
	metrics *metrics.Metrics
	log     *slog.Logger

	maxRetriesDefault int
	timeoutDefault    time.Duration

	mu          sync.Mutex
	cancelFuncs map[string]context.CancelFunc // jobID -> cancel for currently-running jobs
	validator   *validation.Validator         // nil = no payload schema checks
	retryTimers map[string]*time.Timer        // jobID -> pending backoff timer

	shuttingDown bool
}

// New builds a Manager. maxRetriesDefault/timeoutDefault are used when a job
// submission does not specify its own values.
func New(store *database.Store, q *queue.Queue, m *metrics.Metrics, log *slog.Logger, maxRetriesDefault int, timeoutDefault time.Duration) *Manager {
	return &Manager{
		store:             store,
		queue:             q,
		metrics:           m,
		log:               log,
		maxRetriesDefault: maxRetriesDefault,
		timeoutDefault:    timeoutDefault,
		cancelFuncs:       make(map[string]context.CancelFunc),
		retryTimers:       make(map[string]*time.Timer),
	}
}

// SetValidator attaches a payload-schema validator. Passing nil (or never
// calling it) leaves payload validation off.
func (mgr *Manager) SetValidator(v *validation.Validator) {
	mgr.validator = v
}

// SubmitInput describes a new job request from the API layer.
type SubmitInput struct {
	Type        string
	Payload     []byte
	Priority    jobs.Priority
	MaxRetries  *int
	TimeoutSecs *int
	ScheduledAt *time.Time
}

// Submit validates and persists a new job, enqueueing it immediately unless
// it is scheduled for the future (the scheduler goroutine picks those up
// when they become due).
func (mgr *Manager) Submit(in SubmitInput) (*jobs.Job, error) {
	if in.Type == "" {
		return nil, fmt.Errorf("%w: type is required", ErrValidation)
	}
	if !in.Priority.Valid() {
		return nil, fmt.Errorf("%w: priority must be HIGH, MEDIUM, or LOW", ErrValidation)
	}

	maxRetries := mgr.maxRetriesDefault
	if in.MaxRetries != nil {
		if *in.MaxRetries < 0 {
			return nil, fmt.Errorf("%w: max_retries cannot be negative", ErrValidation)
		}
		maxRetries = *in.MaxRetries
	}

	timeoutSecs := int(mgr.timeoutDefault.Seconds())
	if in.TimeoutSecs != nil {
		if *in.TimeoutSecs <= 0 {
			return nil, fmt.Errorf("%w: timeout_seconds must be positive", ErrValidation)
		}
		timeoutSecs = *in.TimeoutSecs
	}

	if mgr.validator != nil {
		if err := mgr.validator.Validate(in.Type, in.Payload); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrValidation, err)
		}
	}

	status := jobs.StatusPending
	if in.ScheduledAt != nil && in.ScheduledAt.After(time.Now()) {
		status = jobs.StatusScheduled
	}

	j := &jobs.Job{
		ID:          jobs.NewID(),
		Type:        in.Type,
		Payload:     in.Payload,
		Priority:    in.Priority,
		Status:      status,
		CreatedAt:   time.Now().UTC(),
		ScheduledAt: in.ScheduledAt,
		MaxRetries:  maxRetries,
		TimeoutSecs: timeoutSecs,
		CreatedBy:   1, // seeded system user; see migrations/0001_init.sql
	}

	if err := mgr.store.CreateJob(j); err != nil {
		return nil, err
	}

	mgr.metrics.IncJobsSubmitted()

	if status == jobs.StatusPending {
		mgr.queue.Push(j.ID, j.Priority.Weight())
		mgr.metrics.SetQueueSize(mgr.queue.Len())
	}

	return j, nil
}

// Claim attempts to transition a job into RUNNING for workerID. It returns
// database.ErrConflict if another worker (or a cancellation) beat this
// caller to it -- workers treat that as "skip and pull the next job", not an
// error worth logging loudly.
func (mgr *Manager) Claim(jobID, workerID string) (*jobs.Job, error) {
	return mgr.store.ClaimJob(jobID, workerID)
}

// RegisterRunning associates a running job with the context.CancelFunc that
// will stop it, so that CancelJob can interrupt in-flight work instead of
// only flipping a database column.
func (mgr *Manager) RegisterRunning(jobID string, cancel context.CancelFunc) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.cancelFuncs[jobID] = cancel
}

// UnregisterRunning removes the bookkeeping entry once a job finishes.
func (mgr *Manager) UnregisterRunning(jobID string) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	delete(mgr.cancelFuncs, jobID)
}

// Complete marks a job COMPLETED.
func (mgr *Manager) Complete(jobID string) error {
	mgr.metrics.IncJobsCompleted()
	return mgr.store.CompleteJob(jobID)
}

// Fail records a failed attempt and, if retries remain, schedules the job to
// be re-enqueued after an exponential backoff delay (immediate -> 2s -> 4s
// -> 8s per the README's retry strategy). Once retries are exhausted the job
// becomes DEAD and is left for manual inspection/requeue.
func (mgr *Manager) Fail(jobID, errMsg string) {
	status, retryCount, err := mgr.store.FailJob(jobID, errMsg)
	if err != nil {
		mgr.log.Error("failing job", "job_id", jobID, "error", err)
		return
	}

	mgr.metrics.IncJobsFailed()

	switch status {
	case jobs.StatusRetrying:
		mgr.metrics.IncJobsRetried()
		backoff := backoffFor(retryCount)
		mgr.scheduleRetry(jobID, backoff)
	case jobs.StatusDead:
		mgr.log.Warn("job moved to DEAD after exhausting retries", "job_id", jobID, "retry_count", retryCount)
	case jobs.StatusCancelled:
		// Job was cancelled while running; nothing further to do.
	}
}

// backoffFor returns the exponential backoff delay before the Nth retry
// (retryCount is the count *after* incrementing on this failure): the 1st
// retry waits 2s, the 2nd waits 4s, the 3rd waits 8s, and so on.
func backoffFor(retryCount int) time.Duration {
	if retryCount < 1 {
		return 0
	}
	d := time.Duration(1) << uint(retryCount) // 2^retryCount seconds
	return d * time.Second
}

// scheduleRetry re-enqueues a job after d, unless the manager is shutting
// down. The job stays in RETRYING/SQLite in the meantime; only the in-memory
// queue placement is delayed.
func (mgr *Manager) scheduleRetry(jobID string, d time.Duration) {
	push := func() {
		mgr.mu.Lock()
		delete(mgr.retryTimers, jobID)
		down := mgr.shuttingDown
		mgr.mu.Unlock()
		if down {
			return
		}
		j, err := mgr.store.GetJob(jobID)
		if err != nil || j.Status != jobs.StatusRetrying {
			return
		}
		mgr.queue.Push(jobID, j.Priority.Weight())
		mgr.metrics.SetQueueSize(mgr.queue.Len())
	}

	if d <= 0 {
		push()
		return
	}

	mgr.mu.Lock()
	mgr.retryTimers[jobID] = time.AfterFunc(d, push)
	mgr.mu.Unlock()
}

// Cancel cancels a job: if it is currently running, its context is
// cancelled so the handler can stop promptly; if it is still pending in the
// queue, it is marked CANCELLED in the database and the worker that
// eventually dequeues it will see the terminal status and skip execution.
func (mgr *Manager) Cancel(jobID string) (wasRunning bool, err error) {
	prev, err := mgr.store.CancelJob(jobID)
	if err != nil {
		return false, err
	}

	mgr.metrics.IncJobsCancelled()

	if prev == jobs.StatusRunning {
		mgr.mu.Lock()
		cancel, ok := mgr.cancelFuncs[jobID]
		mgr.mu.Unlock()
		if ok {
			cancel()
		}
		return true, nil
	}

	// If it had a pending retry timer, stop it so it doesn't resurrect the
	// job after we just cancelled it.
	mgr.mu.Lock()
	if t, ok := mgr.retryTimers[jobID]; ok {
		t.Stop()
		delete(mgr.retryTimers, jobID)
	}
	mgr.mu.Unlock()

	return false, nil
}

// RequeueDead resets a DEAD (or FAILED) job back to PENDING and enqueues it.
func (mgr *Manager) RequeueDead(jobID string) (*jobs.Job, error) {
	j, err := mgr.store.RequeueDeadJob(jobID)
	if err != nil {
		return nil, err
	}
	mgr.queue.Push(j.ID, j.Priority.Weight())
	mgr.metrics.SetQueueSize(mgr.queue.Len())
	return j, nil
}

// EnqueueExisting pushes an already-persisted job ID onto the queue. Used by
// the scheduler once a SCHEDULED job becomes due.
func (mgr *Manager) EnqueueExisting(jobID string, priority jobs.Priority) {
	mgr.queue.Push(jobID, priority.Weight())
	mgr.metrics.SetQueueSize(mgr.queue.Len())
}

// AgeQueue applies one round of priority aging to every job waiting in the
// queue: each item's weight moves one step toward floor (lower = sooner),
// never crossing it. The priority stored in SQLite is untouched - the boost
// is temporary and lasts only until the job is dequeued.
func (mgr *Manager) AgeQueue(floor int) {
	mgr.queue.Age(floor)
}

// CancelAllRunning force-cancels every currently-registered running job's
// context. Used as the last resort at the end of the graceful shutdown
// window, once ShutdownTimeout has elapsed and jobs still haven't finished
// on their own.
func (mgr *Manager) CancelAllRunning() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	n := len(mgr.cancelFuncs)
	for id, cancel := range mgr.cancelFuncs {
		cancel()
		delete(mgr.cancelFuncs, id)
	}
	return n
}

// Shutdown stops accepting new retry re-enqueues and cancels any pending
// backoff timers, then closes the queue so blocked worker Pop() calls wake
// up. Call this once, after new-job submission has already been cut off at
// the API layer.
func (mgr *Manager) Shutdown() {
	mgr.mu.Lock()
	mgr.shuttingDown = true
	for id, t := range mgr.retryTimers {
		t.Stop()
		delete(mgr.retryTimers, id)
	}
	mgr.mu.Unlock()
	mgr.queue.Close()
}

// Store exposes the underlying database.Store for read-only queries (list,
// get, stats) that don't belong to a state-transition method above.
func (mgr *Manager) Store() *database.Store { return mgr.store }

// Queue exposes the underlying queue for length/metrics reporting.
func (mgr *Manager) Queue() *queue.Queue { return mgr.queue }

// ErrValidation wraps input validation failures so the API layer can map
// them to HTTP 400 without string-matching error text.
var ErrValidation = fmt.Errorf("validation error")
