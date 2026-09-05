package workers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"goflow/internal/database"
	"goflow/internal/jobs"
)

// runWorker is the main loop for a single worker goroutine. It pulls job
// IDs from the shared priority queue and executes them one at a time until
// its context is cancelled (either by Pool.stopWorker during a scale-down,
// or by the pool-wide shutdown context).
func (p *Pool) runWorker(ctx context.Context, id string) {
	defer p.wg.Done()
	defer p.removeWorker(id)
	defer func() {
		if err := p.mgr.Store().StopWorker(id); err != nil {
			p.log.Error("marking worker stopped", "worker_id", id, "error", err)
		}
	}()

	stopHeartbeat := p.startHeartbeat(ctx, id)
	defer stopHeartbeat()

	for {
		jobID, ok := p.mgr.Queue().Pop(ctx)
		if !ok {
			return
		}
		p.metrics.SetQueueSize(p.mgr.Queue().Len())
		p.processJob(ctx, id, jobID)
	}
}

// startHeartbeat launches a goroutine that pulses the worker's liveness into
// the database on a fixed interval, independent of whether the worker is
// currently busy executing a job. It returns a function that stops the
// heartbeat goroutine.
func (p *Pool) startHeartbeat(ctx context.Context, id string) (stop func()) {
	interval := p.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := p.mgr.Store().Heartbeat(id, time.Now().UTC()); err != nil {
					p.log.Error("recording heartbeat", "worker_id", id, "error", err)
				}
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

// processJob claims, executes, and finalizes a single job. It never lets a
// panicking handler take down the worker goroutine: a recovered panic is
// treated as a failed attempt, which is subject to the same retry/dead-letter
// rules as any other error.
func (p *Pool) processJob(workerCtx context.Context, workerID, jobID string) {
	j, err := p.mgr.Claim(jobID, workerID)
	if err != nil {
		if errors.Is(err, database.ErrConflict) || errors.Is(err, database.ErrNotFound) {
			// Already claimed by someone else, cancelled, or deleted between
			// being enqueued and being popped -- safe to just move on.
			return
		}
		p.log.Error("claiming job", "worker_id", workerID, "job_id", jobID, "error", err)
		return
	}

	if err := p.mgr.Store().SetWorkerBusy(workerID, jobID); err != nil {
		p.log.Error("marking worker busy", "worker_id", workerID, "error", err)
	}

	handler, ok := p.registry.Lookup(j.Type)
	if !ok {
		p.mgr.Fail(jobID, fmt.Sprintf("no handler registered for job type %q", j.Type))
		p.finishBookkeeping(workerID, false)
		return
	}

	timeout := j.Timeout()
	if timeout <= 0 {
		timeout = p.cfg.DefaultJobTimeout
	}
	// Deliberately rooted at context.Background(), not workerCtx: a
	// scale-down or graceful-shutdown signal (which cancels workerCtx)
	// should let this job finish naturally within its own timeout, not be
	// aborted immediately. Only an explicit job cancellation (via the
	// manager's cancel registry) or a hard-shutdown force-cancel reaches in
	// and stops it early.
	jobCtx, cancel := context.WithTimeout(context.Background(), timeout)
	p.mgr.RegisterRunning(jobID, cancel)

	attemptNumber := j.RetryCount + 1
	startedAt := time.Now().UTC()
	attemptID, attemptErr := p.mgr.Store().RecordAttemptStart(jobID, workerID, attemptNumber, startedAt)
	if attemptErr != nil {
		p.log.Error("recording attempt start", "job_id", jobID, "error", attemptErr)
	}

	execErr := p.runHandler(handler, jobCtx, j.Payload)

	cancel()
	p.mgr.UnregisterRunning(jobID)

	completedAt := time.Now().UTC()
	durationMS := completedAt.Sub(startedAt).Milliseconds()
	p.metrics.ObserveJobDuration(float64(durationMS))

	if execErr == nil {
		if attemptErr == nil {
			_ = p.mgr.Store().RecordAttemptEnd(attemptID, jobs.StatusCompleted, "", completedAt, durationMS)
		}
		if err := p.mgr.Complete(jobID); err != nil {
			p.log.Error("completing job", "job_id", jobID, "error", err)
		}
		p.finishBookkeeping(workerID, true)
		p.log.Info("job completed", "job_id", jobID, "worker_id", workerID, "duration_ms", durationMS)
		return
	}

	outcomeStatus := jobs.StatusFailed
	if errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
		execErr = fmt.Errorf("job timed out after %s: %w", timeout, execErr)
	} else if errors.Is(execErr, context.Canceled) {
		outcomeStatus = jobs.StatusCancelled
	}

	if attemptErr == nil {
		_ = p.mgr.Store().RecordAttemptEnd(attemptID, outcomeStatus, execErr.Error(), completedAt, durationMS)
	}

	if outcomeStatus == jobs.StatusCancelled {
		// The manager's CancelJob already set the row to CANCELLED before
		// invoking our cancel func; Fail() is a no-op for already-cancelled
		// jobs, but we still want the attempt log and worker bookkeeping.
		p.finishBookkeeping(workerID, false)
		p.log.Info("job cancelled", "job_id", jobID, "worker_id", workerID)
		return
	}

	p.mgr.Fail(jobID, execErr.Error())
	p.finishBookkeeping(workerID, false)
	p.log.Warn("job failed", "job_id", jobID, "worker_id", workerID, "error", execErr.Error(), "attempt", attemptNumber)
}

// runHandler invokes the handler with panic recovery, converting a panic
// into a regular error so it flows through the normal retry/dead-letter
// path instead of crashing the worker goroutine.
func (p *Pool) runHandler(handler jobs.Handler, ctx context.Context, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			p.metrics.IncWorkerFailures()
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return handler(ctx, payload)
}

func (p *Pool) finishBookkeeping(workerID string, succeeded bool) {
	if err := p.mgr.Store().SetWorkerIdle(workerID, succeeded); err != nil {
		p.log.Error("marking worker idle", "worker_id", workerID, "error", err)
	}
}
