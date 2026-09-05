package scheduler

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"goflow/internal/database"
	"goflow/internal/jobs"
	"goflow/internal/manager"
	"goflow/internal/metrics"
	"goflow/internal/queue"
)

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

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestScheduler_PromotesDueJobs(t *testing.T) {
	store := newTestStore(t)
	q := queue.New()
	m := metrics.New()
	log := discardLogger()
	mgr := manager.New(store, q, m, log, 3, 30*time.Second)

	past := time.Now().UTC().Add(-time.Second)
	j := &jobs.Job{
		ID: "due-job", Type: "email", Payload: []byte(`{}`), Priority: jobs.PriorityHigh,
		Status: jobs.StatusScheduled, CreatedAt: time.Now().UTC(), ScheduledAt: &past,
		MaxRetries: 3, TimeoutSecs: 30, CreatedBy: 1,
	}
	if err := store.CreateJob(j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	future := time.Now().UTC().Add(time.Hour)
	notDue := &jobs.Job{
		ID: "not-due-job", Type: "email", Payload: []byte(`{}`), Priority: jobs.PriorityHigh,
		Status: jobs.StatusScheduled, CreatedAt: time.Now().UTC(), ScheduledAt: &future,
		MaxRetries: 3, TimeoutSecs: 30, CreatedBy: 1,
	}
	if err := store.CreateJob(notDue); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	sched := New(mgr, 50*time.Millisecond, log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sched.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for q.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if q.Len() != 1 {
		t.Fatalf("expected exactly 1 job promoted to the queue, got queue len %d", q.Len())
	}
	id, ok := q.Pop(context.Background())
	if !ok || id != "due-job" {
		t.Fatalf("expected due-job to be enqueued, got %q ok=%v", id, ok)
	}

	got, err := store.GetJob("due-job")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != jobs.StatusPending {
		t.Fatalf("expected due-job to be PENDING after promotion, got %s", got.Status)
	}

	stillScheduled, err := store.GetJob("not-due-job")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if stillScheduled.Status != jobs.StatusScheduled {
		t.Fatalf("expected not-due-job to remain SCHEDULED, got %s", stillScheduled.Status)
	}
}

func TestScheduler_StopsOnContextCancel(t *testing.T) {
	store := newTestStore(t)
	q := queue.New()
	m := metrics.New()
	log := discardLogger()
	mgr := manager.New(store, q, m, log, 3, 30*time.Second)

	sched := New(mgr, 10*time.Millisecond, log)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		sched.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after context cancellation")
	}
}
