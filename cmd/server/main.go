// Command server is the GoFlow entrypoint. It wires together configuration,
// the SQLite-backed store, the in-memory priority queue, the job manager,
// the dynamically-scaled worker pool, the scheduler, and the REST API, then
// runs until it receives SIGINT/SIGTERM and shuts everything down cleanly.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"goflow/internal/api"
	"goflow/internal/config"
	"goflow/internal/database"
	"goflow/internal/jobs"
	"goflow/internal/manager"
	"goflow/internal/metrics"
	"goflow/internal/queue"
	"goflow/internal/scheduler"
	"goflow/internal/workers"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := run(log); err != nil {
		log.Error("fatal startup error", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	store, err := database.Open(cfg.DatabasePath)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := store.Migrate("migrations"); err != nil {
		return err
	}
	log.Info("database ready", "path", cfg.DatabasePath)

	registry := jobs.NewRegistry()
	jobs.RegisterDefaultHandlers(registry)

	m := metrics.New()
	q := queue.New()
	mgr := manager.New(store, q, m, log, cfg.MaxRetries, cfg.DefaultJobTimeout)

	// Recover any job that was RUNNING when a previous process died, and
	// re-enqueue it so it isn't stuck forever.
	orphans, err := store.ResetOrphanedRunningJobs()
	if err != nil {
		return err
	}
	for _, j := range orphans {
		q.Push(j.ID, j.Priority.Weight())
		log.Warn("recovered orphaned running job from previous run", "job_id", j.ID)
	}
	// Also seed the queue with anything left PENDING/RETRYING from a
	// previous run (e.g. process was killed before a retry timer fired).
	pending, err := store.ListJobs(string(jobs.StatusPending), 10000)
	if err != nil {
		return err
	}
	for _, j := range pending {
		q.Push(j.ID, j.Priority.Weight())
	}
	retrying, err := store.ListJobs(string(jobs.StatusRetrying), 10000)
	if err != nil {
		return err
	}
	for _, j := range retrying {
		q.Push(j.ID, j.Priority.Weight())
	}
	m.SetQueueSize(q.Len())

	pool := workers.New(mgr, registry, m, cfg, log)
	poolCtx, poolCancel := context.WithCancel(context.Background())
	pool.Start(poolCtx)
	log.Info("worker pool started", "initial_workers", cfg.WorkerCount)

	schedCtx, schedCancel := context.WithCancel(context.Background())
	sched := scheduler.New(mgr, cfg.SchedulerPollInterval, log)
	go sched.Run(schedCtx)

	if cfg.AgingInterval > 0 {
		ager := scheduler.NewAger(mgr, jobs.PriorityHigh.Weight(), cfg.AgingInterval, log)
		go ager.Run(schedCtx)
		log.Info("priority aging enabled", "interval", cfg.AgingInterval)
	}

	handlers := api.NewHandlers(mgr, pool, m, log)
	router := api.NewRouter(handlers, "web", log)
	httpServer := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	serveErrCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serveErrCh <- err
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-sigCtx.Done():
		log.Info("shutdown signal received")
	case err := <-serveErrCh:
		log.Error("http server error", "error", err)
	}

	return shutdown(log, cfg, httpServer, schedCancel, poolCancel, pool, mgr)
}

// shutdown implements the eight-step graceful shutdown sequence documented
// in the README: stop new jobs -> stop the scheduler -> signal workers ->
// let active jobs finish -> force-cancel after the timeout -> persist state
// (already continuous) -> close SQLite -> exit.
func shutdown(log *slog.Logger, cfg *config.Config, httpServer *http.Server,
	schedCancel, poolCancel context.CancelFunc, pool *workers.Pool, mgr *manager.Manager) error {

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	// 1. Stop accepting new jobs (and any other new HTTP requests).
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("http server shutdown", "error", err)
	}
	log.Info("http server stopped accepting new requests")

	// 2. Stop scheduling new jobs.
	schedCancel()

	// 3. Signal workers to stop pulling new jobs, and stop new retry
	// re-enqueues. Existing in-flight jobs keep running.
	mgr.Shutdown()
	poolCancel()
	log.Info("workers signaled to stop pulling new jobs; waiting for in-flight jobs", "timeout", cfg.ShutdownTimeout)

	// 4/5. Allow active jobs to finish, up to the shutdown timeout; force
	// cancel anything still running after that.
	if pool.WaitTimeout(cfg.ShutdownTimeout) {
		log.Info("all workers finished cleanly")
	} else {
		n := mgr.CancelAllRunning()
		log.Warn("shutdown timeout elapsed; force-cancelling remaining jobs", "cancelled", n)
		pool.WaitTimeout(5 * time.Second)
	}

	// 6/7. Final states are persisted continuously as jobs complete/fail;
	// closing the store is handled by the deferred store.Close() in run().
	log.Info("shutdown complete")
	return nil
}
