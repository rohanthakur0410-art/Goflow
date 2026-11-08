package workers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"goflow/internal/config"
	"goflow/internal/database"
	"goflow/internal/jobs"
	"goflow/internal/manager"
	"goflow/internal/metrics"
	"goflow/internal/queue"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(newDiscard(), nil))
}

type discard struct{}

func newDiscard() *discard                     { return &discard{} }
func (d *discard) Write(p []byte) (int, error) { return len(p), nil }

func testConfig() *config.Config {
	return &config.Config{
		WorkerCount:           2,
		QueueSize:             100,
		MaxRetries:            3,
		DefaultJobTimeout:     2 * time.Second,
		ShutdownTimeout:       5 * time.Second,
		SchedulerPollInterval: 100 * time.Millisecond,
		ScalingCheckInterval:  50 * time.Millisecond,
		HeartbeatInterval:     100 * time.Millisecond,
		StaleWorkerThreshold:  time.Second,
		ScalingRules:          []config.ScalingRule{{QueueSize: 999999, WorkerCount: 2}},
	}
}

func newTestStore(t *testing.T) *database.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := database.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("opening test store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	migrationsDir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatalf("resolving migrations dir: %v", err)
	}
	if err := s.Migrate(migrationsDir); err != nil {
		t.Fatalf("migrating test store: %v", err)
	}
	return s
}

// harness bundles everything needed to exercise the worker pool end to end
// against a real (temp-file) SQLite database.
type harness struct {
	store   *database.Store
	queue   *queue.Queue
	mgr     *manager.Manager
	metrics *metrics.Metrics
	pool    *Pool
	cfg     *config.Config
}

func newHarness(t *testing.T, registry *jobs.Registry) *harness {
	t.Helper()
	store := newTestStore(t)
	q := queue.New()
	m := metrics.New()
	log := testLogger()
	cfg := testConfig()
	mgr := manager.New(store, q, m, log, cfg.MaxRetries, cfg.DefaultJobTimeout)
	pool := New(mgr, registry, m, cfg, log)

	return &harness{store: store, queue: q, mgr: mgr, metrics: m, pool: pool, cfg: cfg}
}

func waitForStatus(t *testing.T, store *database.Store, jobID string, want jobs.Status, timeout time.Duration) *jobs.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := store.GetJob(jobID)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if j.Status == want {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	j, _ := store.GetJob(jobID)
	t.Fatalf("job %s did not reach status %s within %s (last status: %v)", jobID, want, timeout, j)
	return nil
}

func TestPool_ExecutesJobSuccessfully(t *testing.T) {
	registry := jobs.NewRegistry()
	var executed atomic.Bool
	registry.Register("test_ok", func(ctx context.Context, payload []byte) error {
		executed.Store(true)
		return nil
	})

	h := newHarness(t, registry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.pool.Start(ctx)

	j, err := h.mgr.Submit(manager.SubmitInput{Type: "test_ok", Payload: []byte(`{}`), Priority: jobs.PriorityHigh})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	waitForStatus(t, h.store, j.ID, jobs.StatusCompleted, 2*time.Second)
	if !executed.Load() {
		t.Fatal("handler was never invoked")
	}
}

func TestPool_PriorityOrderRespected(t *testing.T) {
	registry := jobs.NewRegistry()
	var mu sync.Mutex
	var order []string
	registry.Register("track_order", func(ctx context.Context, payload []byte) error {
		mu.Lock()
		order = append(order, string(payload))
		mu.Unlock()
		return nil
	})

	h := newHarness(t, registry)
	h.cfg.ScalingRules = []config.ScalingRule{{QueueSize: 999999, WorkerCount: 1}} // single worker: strict ordering
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Submit before starting the pool so all three are queued up before any
	// worker begins dequeuing.
	low, _ := h.mgr.Submit(manager.SubmitInput{Type: "track_order", Payload: []byte(`"low"`), Priority: jobs.PriorityLow})
	high, _ := h.mgr.Submit(manager.SubmitInput{Type: "track_order", Payload: []byte(`"high"`), Priority: jobs.PriorityHigh})
	medium, _ := h.mgr.Submit(manager.SubmitInput{Type: "track_order", Payload: []byte(`"medium"`), Priority: jobs.PriorityMedium})

	h.pool.Start(ctx)

	waitForStatus(t, h.store, low.ID, jobs.StatusCompleted, 2*time.Second)
	waitForStatus(t, h.store, high.ID, jobs.StatusCompleted, 2*time.Second)
	waitForStatus(t, h.store, medium.ID, jobs.StatusCompleted, 2*time.Second)

	mu.Lock()
	defer mu.Unlock()
	want := []string{`"high"`, `"medium"`, `"low"`}
	if len(order) != 3 || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] {
		t.Fatalf("got order %v, want %v", order, want)
	}
}

func TestPool_RetriesWithBackoffThenDies(t *testing.T) {
	registry := jobs.NewRegistry()
	var attempts atomic.Int32
	registry.Register("always_fails", func(ctx context.Context, payload []byte) error {
		attempts.Add(1)
		return errors.New("simulated failure")
	})

	h := newHarness(t, registry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.pool.Start(ctx)

	maxRetries := 2
	j, err := h.mgr.Submit(manager.SubmitInput{
		Type: "always_fails", Payload: []byte(`{}`), Priority: jobs.PriorityHigh, MaxRetries: &maxRetries,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// 1 initial attempt + 2 retries (backoff 2s, 4s) => allow generous time.
	final := waitForStatus(t, h.store, j.ID, jobs.StatusDead, 10*time.Second)
	if final.RetryCount != 3 {
		t.Fatalf("expected retry_count 3 (exceeds max_retries=2), got %d", final.RetryCount)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected handler invoked 3 times, got %d", attempts.Load())
	}

	attemptsLog, err := h.store.ListAttempts(j.ID)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(attemptsLog) != 3 {
		t.Fatalf("expected 3 recorded attempts, got %d", len(attemptsLog))
	}
}

func TestPool_JobTimeout(t *testing.T) {
	registry := jobs.NewRegistry()
	registry.Register("slow", func(ctx context.Context, payload []byte) error {
		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	h := newHarness(t, registry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.pool.Start(ctx)

	timeout := 1
	maxRetries := 0
	j, err := h.mgr.Submit(manager.SubmitInput{
		Type: "slow", Payload: []byte(`{}`), Priority: jobs.PriorityHigh,
		TimeoutSecs: &timeout, MaxRetries: &maxRetries,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// With max_retries=0 the single timed-out attempt goes straight to DEAD.
	final := waitForStatus(t, h.store, j.ID, jobs.StatusDead, 5*time.Second)
	if final.ErrorMessage == "" {
		t.Fatal("expected a timeout error message to be recorded")
	}
}

func TestPool_CancelRunningJob(t *testing.T) {
	registry := jobs.NewRegistry()
	started := make(chan struct{})
	registry.Register("cancelable", func(ctx context.Context, payload []byte) error {
		close(started)
		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	h := newHarness(t, registry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.pool.Start(ctx)

	timeout := 30
	j, err := h.mgr.Submit(manager.SubmitInput{
		Type: "cancelable", Payload: []byte(`{}`), Priority: jobs.PriorityHigh, TimeoutSecs: &timeout,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	wasRunning, err := h.mgr.Cancel(j.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !wasRunning {
		t.Fatal("expected wasRunning=true")
	}

	waitForStatus(t, h.store, j.ID, jobs.StatusCancelled, 2*time.Second)
}

func TestPool_CancelPendingJob(t *testing.T) {
	registry := jobs.NewRegistry()
	registry.Register("noop", func(ctx context.Context, payload []byte) error { return nil })

	h := newHarness(t, registry)
	// Don't start the pool yet -- job stays PENDING in the queue.
	j, err := h.mgr.Submit(manager.SubmitInput{Type: "noop", Payload: []byte(`{}`), Priority: jobs.PriorityHigh})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	wasRunning, err := h.mgr.Cancel(j.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if wasRunning {
		t.Fatal("expected wasRunning=false for a still-pending job")
	}

	got, _ := h.store.GetJob(j.ID)
	if got.Status != jobs.StatusCancelled {
		t.Fatalf("expected CANCELLED, got %s", got.Status)
	}

	// Now start the pool: the worker will dequeue the cancelled job ID and
	// must skip execution rather than running it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.pool.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	got, _ = h.store.GetJob(j.ID)
	if got.Status != jobs.StatusCancelled {
		t.Fatalf("expected job to remain CANCELLED, got %s", got.Status)
	}
}

func TestPool_HandlerPanicRecovered(t *testing.T) {
	registry := jobs.NewRegistry()
	registry.Register("panics", func(ctx context.Context, payload []byte) error {
		panic("boom")
	})

	h := newHarness(t, registry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.pool.Start(ctx)

	maxRetries := 0
	j, err := h.mgr.Submit(manager.SubmitInput{
		Type: "panics", Payload: []byte(`{}`), Priority: jobs.PriorityHigh, MaxRetries: &maxRetries,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	final := waitForStatus(t, h.store, j.ID, jobs.StatusDead, 3*time.Second)
	if final.ErrorMessage == "" {
		t.Fatal("expected panic to be recorded as an error message")
	}

	if h.metrics.Snapshot().JobsFailed == 0 {
		t.Fatal("expected jobs_failed metric to be incremented")
	}
}

func TestPool_ConcurrentWorkersProcessDistinctJobs(t *testing.T) {
	registry := jobs.NewRegistry()
	var mu sync.Mutex
	seenWorkers := make(map[string]bool)
	registry.Register("track_worker", func(ctx context.Context, payload []byte) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	h := newHarness(t, registry)
	h.cfg.ScalingRules = []config.ScalingRule{{QueueSize: 999999, WorkerCount: 4}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.pool.Start(ctx)

	const n = 20
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		j, err := h.mgr.Submit(manager.SubmitInput{
			Type: "track_worker", Payload: []byte(fmt.Sprintf(`{"i":%d}`, i)), Priority: jobs.PriorityMedium,
		})
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
		ids[i] = j.ID
	}

	for _, id := range ids {
		final := waitForStatus(t, h.store, id, jobs.StatusCompleted, 5*time.Second)
		if final.WorkerID != nil {
			mu.Lock()
			seenWorkers[*final.WorkerID] = true
			mu.Unlock()
		}
	}

	if len(seenWorkers) < 2 {
		t.Fatalf("expected multiple distinct workers to have processed jobs, saw %v", seenWorkers)
	}
}

func TestPool_DynamicScaling(t *testing.T) {
	registry := jobs.NewRegistry()
	registry.Register("slow_noop", func(ctx context.Context, payload []byte) error {
		time.Sleep(300 * time.Millisecond)
		return nil
	})

	h := newHarness(t, registry)
	h.cfg.WorkerCount = 1
	h.cfg.ScalingRules = []config.ScalingRule{
		{QueueSize: 0, WorkerCount: 1},
		{QueueSize: 999999, WorkerCount: 3},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.pool.Start(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for h.pool.ActiveCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := h.pool.ActiveCount(); got != 1 {
		t.Fatalf("expected pool to settle at 1 worker for an empty queue, got %d", got)
	}

	// Submit enough slow jobs that, with only one worker draining them, the
	// queue stays non-empty long enough for the scaling loop to notice and
	// scale up.
	for i := 0; i < 10; i++ {
		if _, err := h.mgr.Submit(manager.SubmitInput{
			Type: "slow_noop", Payload: []byte(`{}`), Priority: jobs.PriorityMedium,
		}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	deadline = time.Now().Add(3 * time.Second)
	for h.pool.ActiveCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := h.pool.ActiveCount(); got < 3 {
		t.Fatalf("expected pool to scale up to 3 workers, got %d", got)
	}
}

func TestPool_GracefulShutdownLetsInFlightJobFinish(t *testing.T) {
	registry := jobs.NewRegistry()
	started := make(chan struct{})
	registry.Register("slow_but_finishes", func(ctx context.Context, payload []byte) error {
		close(started)
		time.Sleep(300 * time.Millisecond)
		return nil
	})

	h := newHarness(t, registry)
	ctx, cancel := context.WithCancel(context.Background())
	h.pool.Start(ctx)

	j, err := h.mgr.Submit(manager.SubmitInput{Type: "slow_but_finishes", Payload: []byte(`{}`), Priority: jobs.PriorityHigh})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	<-started
	h.mgr.Shutdown()
	cancel() // stop pull loops; in-flight job keeps running (its own ctx is independent)

	if !h.pool.WaitTimeout(2 * time.Second) {
		t.Fatal("pool did not shut down within timeout")
	}

	final, _ := h.store.GetJob(j.ID)
	if final.Status != jobs.StatusCompleted {
		t.Fatalf("expected in-flight job to complete despite shutdown, got %s", final.Status)
	}
}

// TestPool_PerTypeConcurrencyLimit proves a capped job type cannot exceed
// its configured concurrency even when the pool has idle workers, and that
// an uncapped type keeps flowing while a capped one holds its only slot.
func TestPool_PerTypeConcurrencyLimit(t *testing.T) {
	registry := jobs.NewRegistry()

	firstStarted := make(chan struct{})
	unblockFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	var firstOnce, secondOnce sync.Once

	registry.Register("capped", func(ctx context.Context, payload []byte) error {
		switch string(payload) {
		case `"first"`:
			firstOnce.Do(func() { close(firstStarted) })
			select {
			case <-unblockFirst:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		default:
			secondOnce.Do(func() { close(secondStarted) })
			return nil
		}
	})
	var freeRan atomic.Bool
	registry.Register("free", func(ctx context.Context, payload []byte) error {
		freeRan.Store(true)
		return nil
	})

	store := newTestStore(t)
	q := queue.New()
	m := metrics.New()
	log := testLogger()
	cfg := testConfig()
	cfg.JobTypeLimits = map[string]int{"capped": 1}
	mgr := manager.New(store, q, m, log, cfg.MaxRetries, cfg.DefaultJobTimeout)
	pool := New(mgr, registry, m, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	defer func() {
		cancel() // stops the monitor goroutines and every worker's pull loop
		pool.ShutdownAll()
		if !pool.WaitTimeout(5 * time.Second) {
			t.Error("pool did not shut down within 5s")
		}
	}()

	first, err := mgr.Submit(manager.SubmitInput{Type: "capped", Payload: []byte(`"first"`), Priority: jobs.PriorityHigh})
	if err != nil {
		t.Fatalf("Submit first: %v", err)
	}
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first capped job did not start")
	}

	// While the first capped job holds the only slot, an uncapped job must
	// still run on the other worker.
	free, err := mgr.Submit(manager.SubmitInput{Type: "free", Payload: []byte(`{}`), Priority: jobs.PriorityHigh})
	if err != nil {
		t.Fatalf("Submit free: %v", err)
	}
	waitForStatus(t, store, free.ID, jobs.StatusCompleted, 3*time.Second)
	if !freeRan.Load() {
		t.Fatal("uncapped job did not run while capped slot was held")
	}

	// A second capped job is claimed but must NOT start: the only slot is held.
	second, err := mgr.Submit(manager.SubmitInput{Type: "capped", Payload: []byte(`"second"`), Priority: jobs.PriorityHigh})
	if err != nil {
		t.Fatalf("Submit second: %v", err)
	}
	select {
	case <-secondStarted:
		t.Fatal("second capped job started while the only type slot was still held")
	case <-time.After(300 * time.Millisecond):
	}

	// Releasing the first job frees the slot; the second must now run.
	close(unblockFirst)
	waitForStatus(t, store, first.ID, jobs.StatusCompleted, 3*time.Second)
	select {
	case <-secondStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("second capped job never started after the slot was released")
	}
	waitForStatus(t, store, second.ID, jobs.StatusCompleted, 3*time.Second)
}

// TestPool_TypeSlotWaitTimesOutAsAttempt covers the failure path: a job
// that cannot get a type slot within its own timeout fails that attempt and
// follows the normal retry path, rather than hanging forever.
func TestPool_TypeSlotWaitTimesOutAsAttempt(t *testing.T) {
	registry := jobs.NewRegistry()
	unblock := make(chan struct{})
	var unblockOnce sync.Once
	closeOnce := func() { unblockOnce.Do(func() { close(unblock) }) }
	registry.Register("capped", func(ctx context.Context, payload []byte) error {
		if string(payload) == `"holder"` {
			select {
			case <-unblock:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	store := newTestStore(t)
	q := queue.New()
	m := metrics.New()
	log := testLogger()
	cfg := testConfig()
	cfg.JobTypeLimits = map[string]int{"capped": 1}
	mgr := manager.New(store, q, m, log, cfg.MaxRetries, cfg.DefaultJobTimeout)
	pool := New(mgr, registry, m, cfg, log)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	defer func() {
		closeOnce()
		cancel()
		pool.ShutdownAll()
		pool.WaitTimeout(5 * time.Second)
	}()

	holder, err := mgr.Submit(manager.SubmitInput{Type: "capped", Payload: []byte(`"holder"`), Priority: jobs.PriorityHigh})
	if err != nil {
		t.Fatalf("Submit holder: %v", err)
	}
	// Wait until the holder actually owns the slot.
	deadline := time.Now().Add(3 * time.Second)
	for {
		j, err := store.GetJob(holder.ID)
		if err == nil && j.Status == jobs.StatusRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("holder job never reached RUNNING")
		}
		time.Sleep(10 * time.Millisecond)
	}

	timeout := 1 // second; far shorter than the holder's block
	waiter, err := mgr.Submit(manager.SubmitInput{
		Type: "capped", Payload: []byte(`"waiter"`), Priority: jobs.PriorityHigh, TimeoutSecs: &timeout,
	})
	if err != nil {
		t.Fatalf("Submit waiter: %v", err)
	}

	// The waiter's first attempt must time out waiting for the slot; with
	// MaxRetries=3 the job lands back in RETRYING (then PENDING on
	// promotion), not COMPLETED, and the attempt log records the timeout.
	time.Sleep(2500 * time.Millisecond)
	attempts, err := store.ListAttempts(waiter.ID)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(attempts) == 0 {
		t.Fatal("no attempts recorded for the waiter")
	}
	foundTimeout := false
	for _, a := range attempts {
		if a.ErrorMessage != "" && a.Status == string(jobs.StatusFailed) {
			foundTimeout = true
		}
	}
	if !foundTimeout {
		t.Fatalf("no failed (timed-out) attempt recorded for the waiter: %+v", attempts)
	}

	// Free the slot: the retried waiter must now complete.
	closeOnce()
	waitForStatus(t, store, holder.ID, jobs.StatusCompleted, 3*time.Second)
	waitForStatus(t, store, waiter.ID, jobs.StatusCompleted, 5*time.Second)
}
