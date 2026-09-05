// Package workers implements the worker pool: a dynamically-sized set of
// goroutines that pull job IDs off the priority queue and execute them
// through the registered handlers.
package workers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"goflow/internal/config"
	"goflow/internal/jobs"
	"goflow/internal/manager"
	"goflow/internal/metrics"
)

type workerHandle struct {
	cancel context.CancelFunc
}

// Pool owns the lifecycle of every worker goroutine: initial startup,
// dynamic scaling in response to queue depth, heartbeat-based stale
// detection, and coordinated shutdown.
type Pool struct {
	mgr      *manager.Manager
	registry *jobs.Registry
	metrics  *metrics.Metrics
	cfg      *config.Config
	log      *slog.Logger

	mu      sync.Mutex
	workers map[string]*workerHandle
	counter int

	wg sync.WaitGroup
}

// New builds a Pool. Call Start to launch the initial workers and the
// background monitor goroutines.
func New(mgr *manager.Manager, registry *jobs.Registry, m *metrics.Metrics, cfg *config.Config, log *slog.Logger) *Pool {
	return &Pool{
		mgr:      mgr,
		registry: registry,
		metrics:  m,
		cfg:      cfg,
		log:      log,
		workers:  make(map[string]*workerHandle),
	}
}

// Start launches the configured initial number of workers plus the scaling
// and heartbeat-monitor goroutines. All of it stops when ctx is cancelled,
// except that in-flight jobs keep running until their own context ends (see
// package workers' worker.go and internal/manager's cancel registry) -- Pool
// only controls whether workers keep *pulling new* jobs.
func (p *Pool) Start(ctx context.Context) {
	initial := p.cfg.WorkersForQueueSize(0)
	if initial < p.cfg.WorkerCount {
		initial = p.cfg.WorkerCount
	}
	for i := 0; i < initial; i++ {
		p.spawnWorker(ctx)
	}

	p.wg.Add(2)
	go p.scalingLoop(ctx)
	go p.heartbeatMonitorLoop(ctx)
}

// Wait blocks until every worker goroutine (and the two monitor goroutines)
// has returned.
func (p *Pool) Wait() {
	p.wg.Wait()
}

// WaitTimeout waits up to d for all workers to finish, returning true if
// they did.
func (p *Pool) WaitTimeout(d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// ActiveCount returns the number of currently-registered worker goroutines
// (workers that have not yet been scaled down or shut down).
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

func (p *Pool) spawnWorker(ctx context.Context) {
	p.mu.Lock()
	p.counter++
	id := fmt.Sprintf("worker-%d", p.counter)
	workerCtx, cancel := context.WithCancel(ctx)
	p.workers[id] = &workerHandle{cancel: cancel}
	count := len(p.workers)
	p.mu.Unlock()

	p.metrics.SetActiveWorkers(count)

	if err := p.mgr.Store().RegisterWorker(id, time.Now().UTC()); err != nil {
		p.log.Error("registering worker", "worker_id", id, "error", err)
	}

	p.wg.Add(1)
	go p.runWorker(workerCtx, id)
}

// stopWorker cancels a specific worker's pull-loop context. The worker
// finishes any job it is currently executing, then its own goroutine
// removes it from the active set via removeWorker (see worker.go) -- doing
// the removal there, rather than here, avoids a race between this call and
// the worker's normal ctx-cancelled exit path.
func (p *Pool) stopWorker(id string) {
	p.mu.Lock()
	h, ok := p.workers[id]
	p.mu.Unlock()
	if ok {
		h.cancel()
	}
}

// removeWorker deletes a worker from the active set and refreshes the
// active-worker gauge. Safe to call more than once for the same id.
func (p *Pool) removeWorker(id string) {
	p.mu.Lock()
	delete(p.workers, id)
	count := len(p.workers)
	p.mu.Unlock()
	p.metrics.SetActiveWorkers(count)
}

// scalingLoop periodically compares the queue depth against the configured
// thresholds and spawns or stops workers to match.
func (p *Pool) scalingLoop(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cfg.ScalingCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.rescale(ctx)
		}
	}
}

func (p *Pool) rescale(ctx context.Context) {
	queueSize := p.mgr.Queue().Len()
	desired := p.cfg.WorkersForQueueSize(queueSize)

	p.mu.Lock()
	current := len(p.workers)
	var toStop []string
	if current > desired {
		for id := range p.workers {
			if len(toStop) >= current-desired {
				break
			}
			toStop = append(toStop, id)
		}
	}
	p.mu.Unlock()

	switch {
	case current < desired:
		for i := 0; i < desired-current; i++ {
			p.spawnWorker(ctx)
		}
		p.log.Info("scaled up", "queue_size", queueSize, "from", current, "to", desired)
	case current > desired:
		for _, id := range toStop {
			p.stopWorker(id)
		}
		p.log.Info("scaled down", "queue_size", queueSize, "from", current, "to", desired)
	}
}

// heartbeatMonitorLoop periodically sweeps the database for workers whose
// last heartbeat is older than the configured staleness threshold.
func (p *Pool) heartbeatMonitorLoop(ctx context.Context) {
	defer p.wg.Done()
	interval := p.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().UTC().Add(-p.cfg.StaleWorkerThreshold)
			stale, err := p.mgr.Store().MarkStaleWorkers(cutoff)
			if err != nil {
				p.log.Error("checking for stale workers", "error", err)
				continue
			}
			for _, id := range stale {
				p.log.Warn("worker marked stale (missed heartbeats)", "worker_id", id)
			}
		}
	}
}

// ShutdownAll cancels every worker's pull-loop context, which lets each
// worker finish any job it is currently running and then exit.
func (p *Pool) ShutdownAll() {
	p.mu.Lock()
	ids := make([]string, 0, len(p.workers))
	for id := range p.workers {
		ids = append(ids, id)
	}
	p.mu.Unlock()

	for _, id := range ids {
		p.stopWorker(id)
	}
}
