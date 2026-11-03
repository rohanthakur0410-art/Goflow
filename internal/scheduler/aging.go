package scheduler

import (
	"context"
	"log/slog"
	"time"

	"goflow/internal/manager"
)

// Ager periodically applies priority aging to the jobs waiting in the
// queue: every interval, each waiting job's effective priority is boosted
// one level (never above floor, normally HIGH's weight), so a sustained
// stream of HIGH-priority submissions cannot starve older MEDIUM/LOW jobs
// forever. The boost lives only in the in-memory queue - the priority
// stored in SQLite is never rewritten.
type Ager struct {
	mgr      *manager.Manager
	floor    int
	interval time.Duration
	log      *slog.Logger
}

// NewAger builds an Ager that ages the queue every interval.
func NewAger(mgr *manager.Manager, floor int, interval time.Duration, log *slog.Logger) *Ager {
	return &Ager{mgr: mgr, floor: floor, interval: interval, log: log}
}

// Run blocks, aging the queue until ctx is cancelled. Intended to be
// launched in its own goroutine from main, like the Scheduler.
func (a *Ager) Run(ctx context.Context) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.log.Info("priority ager stopping")
			return
		case <-ticker.C:
			a.mgr.AgeQueue(a.floor)
		}
	}
}
