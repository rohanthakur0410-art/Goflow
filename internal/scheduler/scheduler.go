// Package scheduler implements the goroutine that periodically checks
// SQLite for SCHEDULED jobs whose scheduled_at has arrived and promotes
// them into the active priority queue.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"goflow/internal/manager"
)

// Scheduler polls for due scheduled jobs on a fixed interval.
type Scheduler struct {
	mgr      *manager.Manager
	interval time.Duration
	log      *slog.Logger
}

// New builds a Scheduler that checks every interval.
func New(mgr *manager.Manager, interval time.Duration, log *slog.Logger) *Scheduler {
	return &Scheduler{mgr: mgr, interval: interval, log: log}
}

// Run blocks, polling until ctx is cancelled. Intended to be launched in its
// own goroutine from main.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopping")
			return
		case <-ticker.C:
			s.tick()
		}
	}
}

func (s *Scheduler) tick() {
	due, err := s.mgr.Store().DueScheduledJobs(time.Now().UTC(), 100)
	if err != nil {
		s.log.Error("scheduler: querying due jobs", "error", err)
		return
	}
	for _, j := range due {
		if err := s.mgr.Store().PromoteScheduledJob(j.ID); err != nil {
			// Benign race: another tick (shouldn't happen with a single
			// scheduler, but defensive) or a cancellation already moved it.
			continue
		}
		s.mgr.EnqueueExisting(j.ID, j.Priority)
		s.log.Info("promoted scheduled job", "job_id", j.ID, "type", j.Type)
	}
}
