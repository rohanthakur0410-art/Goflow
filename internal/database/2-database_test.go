package database

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"goflow/internal/jobs"
)

// newTestStore returns a fresh, migrated SQLite store backed by a temp file
// (not :memory:, so that WAL mode and multi-connection behavior match
// production as closely as possible).
func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "test.db"))
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

func newTestJob(id string, priority jobs.Priority) *jobs.Job {
	return &jobs.Job{
		ID:          id,
		Type:        "email",
		Payload:     []byte(`{"to":"a@b.com"}`),
		Priority:    priority,
		Status:      jobs.StatusPending,
		CreatedAt:   time.Now().UTC(),
		MaxRetries:  3,
		TimeoutSecs: 30,
		CreatedBy:   1,
	}
}

func TestCreateAndGetJob(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityHigh)

	if err := s.CreateJob(j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	got, err := s.GetJob("job-1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.ID != j.ID || got.Type != j.Type || got.Priority != j.Priority {
		t.Fatalf("got %+v, want fields matching %+v", got, j)
	}
	if got.Status != jobs.StatusPending {
		t.Fatalf("expected PENDING status, got %s", got.Status)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.GetJob("nope"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestListJobs_FilterByStatus(t *testing.T) {
	s := newTestStore(t)
	j1 := newTestJob("job-1", jobs.PriorityHigh)
	j2 := newTestJob("job-2", jobs.PriorityLow)
	j2.Status = jobs.StatusCompleted
	s.CreateJob(j1)
	s.CreateJob(j2)

	pending, err := s.ListJobs(string(jobs.StatusPending), 0)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "job-1" {
		t.Fatalf("expected only job-1 pending, got %+v", pending)
	}

	all, err := s.ListJobs("", 0)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 jobs total, got %d", len(all))
	}
}

func TestDeleteJob(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityMedium)
	s.CreateJob(j)

	if err := s.DeleteJob("job-1"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if _, err := s.GetJob("job-1"); err != ErrNotFound {
		t.Fatalf("expected job to be gone, got err=%v", err)
	}
	if err := s.DeleteJob("job-1"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound deleting again, got %v", err)
	}
}

func TestClaimJob_Success(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityHigh)
	s.CreateJob(j)

	claimed, err := s.ClaimJob("job-1", "worker-1")
	if err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	if claimed.Status != jobs.StatusRunning {
		t.Fatalf("expected RUNNING, got %s", claimed.Status)
	}
	if claimed.WorkerID == nil || *claimed.WorkerID != "worker-1" {
		t.Fatalf("expected worker_id=worker-1, got %v", claimed.WorkerID)
	}
}

func TestClaimJob_ConflictWhenAlreadyClaimed(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityHigh)
	s.CreateJob(j)

	if _, err := s.ClaimJob("job-1", "worker-1"); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := s.ClaimJob("job-1", "worker-2"); err != ErrConflict {
		t.Fatalf("expected ErrConflict on second claim, got %v", err)
	}
}

// TestClaimJob_ConcurrentClaimsAreMutuallyExclusive is the load-bearing test
// for "two workers cannot claim the same job": many goroutines race to claim
// the same job ID, and exactly one must win.
func TestClaimJob_ConcurrentClaimsAreMutuallyExclusive(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityHigh)
	s.CreateJob(j)

	const attempts = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			workerID := "worker-x"
			_, err := s.ClaimJob("job-1", workerID)
			if err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			} else if err != ErrConflict {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("expected exactly 1 successful claim out of %d attempts, got %d", attempts, successes)
	}
}

func TestFailJob_RetriesThenDies(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityHigh)
	j.MaxRetries = 2
	s.CreateJob(j)

	status, retries, err := s.FailJob("job-1", "boom 1")
	if err != nil {
		t.Fatalf("FailJob (1): %v", err)
	}
	if status != jobs.StatusRetrying || retries != 1 {
		t.Fatalf("expected RETRYING/1, got %s/%d", status, retries)
	}

	status, retries, err = s.FailJob("job-1", "boom 2")
	if err != nil {
		t.Fatalf("FailJob (2): %v", err)
	}
	if status != jobs.StatusRetrying || retries != 2 {
		t.Fatalf("expected RETRYING/2, got %s/%d", status, retries)
	}

	status, retries, err = s.FailJob("job-1", "boom 3")
	if err != nil {
		t.Fatalf("FailJob (3): %v", err)
	}
	if status != jobs.StatusDead || retries != 3 {
		t.Fatalf("expected DEAD/3 (exceeded max_retries=2), got %s/%d", status, retries)
	}

	got, _ := s.GetJob("job-1")
	if got.Status != jobs.StatusDead {
		t.Fatalf("expected persisted status DEAD, got %s", got.Status)
	}
	if got.ErrorMessage != "boom 3" {
		t.Fatalf("expected last error message persisted, got %q", got.ErrorMessage)
	}
}

func TestCancelJob(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityHigh)
	s.CreateJob(j)

	prev, err := s.CancelJob("job-1")
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if prev != jobs.StatusPending {
		t.Fatalf("expected previous status PENDING, got %s", prev)
	}

	got, _ := s.GetJob("job-1")
	if got.Status != jobs.StatusCancelled {
		t.Fatalf("expected CANCELLED, got %s", got.Status)
	}

	// Cancelling an already-terminal job is a conflict, not a silent no-op.
	if _, err := s.CancelJob("job-1"); err != ErrConflict {
		t.Fatalf("expected ErrConflict cancelling an already-cancelled job, got %v", err)
	}
}

func TestRequeueDeadJob(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityHigh)
	j.MaxRetries = 0
	s.CreateJob(j)

	if _, _, err := s.FailJob("job-1", "fatal"); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	got, _ := s.GetJob("job-1")
	if got.Status != jobs.StatusDead {
		t.Fatalf("expected DEAD before requeue, got %s", got.Status)
	}

	requeued, err := s.RequeueDeadJob("job-1")
	if err != nil {
		t.Fatalf("RequeueDeadJob: %v", err)
	}
	if requeued.Status != jobs.StatusPending {
		t.Fatalf("expected PENDING after requeue, got %s", requeued.Status)
	}
	if requeued.RetryCount != 0 {
		t.Fatalf("expected retry_count reset to 0, got %d", requeued.RetryCount)
	}
}

func TestRequeueDeadJob_RejectsNonDeadJob(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityHigh)
	s.CreateJob(j) // still PENDING

	if _, err := s.RequeueDeadJob("job-1"); err != ErrConflict {
		t.Fatalf("expected ErrConflict requeuing a PENDING job, got %v", err)
	}
}

func TestDueScheduledJobs_AndPromote(t *testing.T) {
	s := newTestStore(t)
	past := time.Now().UTC().Add(-time.Minute)
	future := time.Now().UTC().Add(time.Hour)

	dueJob := newTestJob("due", jobs.PriorityHigh)
	dueJob.Status = jobs.StatusScheduled
	dueJob.ScheduledAt = &past
	s.CreateJob(dueJob)

	notDueJob := newTestJob("not-due", jobs.PriorityHigh)
	notDueJob.Status = jobs.StatusScheduled
	notDueJob.ScheduledAt = &future
	s.CreateJob(notDueJob)

	due, err := s.DueScheduledJobs(time.Now().UTC(), 100)
	if err != nil {
		t.Fatalf("DueScheduledJobs: %v", err)
	}
	if len(due) != 1 || due[0].ID != "due" {
		t.Fatalf("expected only 'due' job, got %+v", due)
	}

	if err := s.PromoteScheduledJob("due"); err != nil {
		t.Fatalf("PromoteScheduledJob: %v", err)
	}
	got, _ := s.GetJob("due")
	if got.Status != jobs.StatusPending {
		t.Fatalf("expected PENDING after promotion, got %s", got.Status)
	}
}

func TestJobAttempts_RecordStartAndEnd(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityHigh)
	s.CreateJob(j)

	start := time.Now().UTC()
	attemptID, err := s.RecordAttemptStart("job-1", "worker-1", 1, start)
	if err != nil {
		t.Fatalf("RecordAttemptStart: %v", err)
	}

	end := start.Add(150 * time.Millisecond)
	if err := s.RecordAttemptEnd(attemptID, jobs.StatusCompleted, "", end, 150); err != nil {
		t.Fatalf("RecordAttemptEnd: %v", err)
	}

	attempts, err := s.ListAttempts("job-1")
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(attempts))
	}
	if attempts[0].Status != string(jobs.StatusCompleted) || attempts[0].DurationMS != 150 {
		t.Fatalf("unexpected attempt record: %+v", attempts[0])
	}
}

func TestListWorkerAttempts(t *testing.T) {
	s := newTestStore(t)
	s.CreateJob(newTestJob("job-1", jobs.PriorityHigh))
	s.CreateJob(newTestJob("job-2", jobs.PriorityHigh))

	base := time.Now().UTC()
	// Two attempts by worker-1 (oldest first) and one by worker-2.
	for i, jobID := range []string{"job-1", "job-2"} {
		id, err := s.RecordAttemptStart(jobID, "worker-1", 1, base.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatalf("RecordAttemptStart: %v", err)
		}
		if err := s.RecordAttemptEnd(id, jobs.StatusCompleted, "", base.Add(time.Duration(i)*time.Second+100*time.Millisecond), 100); err != nil {
			t.Fatalf("RecordAttemptEnd: %v", err)
		}
	}
	if _, err := s.RecordAttemptStart("job-1", "worker-2", 2, base.Add(2*time.Second)); err != nil {
		t.Fatalf("RecordAttemptStart: %v", err)
	}

	attempts, err := s.ListWorkerAttempts("worker-1", 50)
	if err != nil {
		t.Fatalf("ListWorkerAttempts: %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("expected 2 attempts for worker-1, got %d", len(attempts))
	}
	// Newest first: job-2's attempt started later.
	if attempts[0].JobID != "job-2" || attempts[1].JobID != "job-1" {
		t.Fatalf("expected newest-first order [job-2 job-1], got [%s %s]", attempts[0].JobID, attempts[1].JobID)
	}

	// Limit caps the result to the single most recent attempt.
	limited, err := s.ListWorkerAttempts("worker-1", 1)
	if err != nil {
		t.Fatalf("ListWorkerAttempts limit: %v", err)
	}
	if len(limited) != 1 || limited[0].JobID != "job-2" {
		t.Fatalf("expected only the newest attempt, got %+v", limited)
	}

	// A worker with no attempts yields an empty slice, not an error.
	none, err := s.ListWorkerAttempts("worker-unknown", 50)
	if err != nil {
		t.Fatalf("ListWorkerAttempts unknown worker: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("expected no attempts, got %d", len(none))
	}
}

func TestStats(t *testing.T) {
	s := newTestStore(t)
	s.CreateJob(newTestJob("p1", jobs.PriorityHigh))
	completed := newTestJob("c1", jobs.PriorityHigh)
	completed.Status = jobs.StatusCompleted
	now := time.Now().UTC()
	started := now.Add(-100 * time.Millisecond)
	completed.StartedAt = &started
	completed.CompletedAt = &now
	s.CreateJob(completed)

	stats, err := s.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Total != 2 || stats.Pending != 1 || stats.Completed != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestWorkerLifecycleAndHeartbeatPruning(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()

	if err := s.RegisterWorker("worker-1", now); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if err := s.SetWorkerBusy("worker-1", "job-1"); err != nil {
		t.Fatalf("SetWorkerBusy: %v", err)
	}

	w, err := s.GetWorker("worker-1")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if w.Status != "BUSY" || w.CurrentJobID == nil || *w.CurrentJobID != "job-1" {
		t.Fatalf("unexpected worker state: %+v", w)
	}

	// Push more than the 50-row pruning cap and confirm it's enforced.
	for i := 0; i < 60; i++ {
		if err := s.Heartbeat("worker-1", now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("Heartbeat: %v", err)
		}
	}
	var count int
	if err := s.DB.QueryRow(`SELECT COUNT(*) FROM worker_heartbeats WHERE worker_id = ?`, "worker-1").Scan(&count); err != nil {
		t.Fatalf("counting heartbeats: %v", err)
	}
	if count != 50 {
		t.Fatalf("expected heartbeat log pruned to 50 rows, got %d", count)
	}

	if err := s.SetWorkerIdle("worker-1", true); err != nil {
		t.Fatalf("SetWorkerIdle: %v", err)
	}
	w, _ = s.GetWorker("worker-1")
	if w.Status != "IDLE" || w.JobsProcessed != 1 {
		t.Fatalf("unexpected worker state after idle: %+v", w)
	}
}

func TestMarkStaleWorkers(t *testing.T) {
	s := newTestStore(t)
	oldTime := time.Now().UTC().Add(-time.Hour)
	freshTime := time.Now().UTC()

	s.RegisterWorker("stale-worker", oldTime)
	s.Heartbeat("stale-worker", oldTime)
	s.RegisterWorker("fresh-worker", freshTime)
	s.Heartbeat("fresh-worker", freshTime)

	stale, err := s.MarkStaleWorkers(time.Now().UTC().Add(-time.Minute))
	if err != nil {
		t.Fatalf("MarkStaleWorkers: %v", err)
	}
	if len(stale) != 1 || stale[0] != "stale-worker" {
		t.Fatalf("expected only stale-worker to be flagged, got %v", stale)
	}

	w, _ := s.GetWorker("stale-worker")
	if w.Status != "STALE" {
		t.Fatalf("expected STALE status, got %s", w.Status)
	}
	fw, _ := s.GetWorker("fresh-worker")
	if fw.Status == "STALE" {
		t.Fatal("fresh worker should not be marked stale")
	}
}

func TestResetOrphanedRunningJobs(t *testing.T) {
	s := newTestStore(t)
	j := newTestJob("job-1", jobs.PriorityHigh)
	s.CreateJob(j)
	if _, err := s.ClaimJob("job-1", "worker-1"); err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}

	orphans, err := s.ResetOrphanedRunningJobs()
	if err != nil {
		t.Fatalf("ResetOrphanedRunningJobs: %v", err)
	}
	if len(orphans) != 1 || orphans[0].ID != "job-1" {
		t.Fatalf("expected job-1 to be recovered, got %+v", orphans)
	}

	got, _ := s.GetJob("job-1")
	if got.Status != jobs.StatusPending {
		t.Fatalf("expected job reset to PENDING, got %s", got.Status)
	}
	if got.WorkerID != nil {
		t.Fatalf("expected worker_id cleared, got %v", got.WorkerID)
	}
}

func TestMigrate_IsIdempotent(t *testing.T) {
	s := newTestStore(t)
	migrationsDir, _ := filepath.Abs("../../migrations")
	// Migrating an already-migrated store must be a no-op, not an error.
	if err := s.Migrate(migrationsDir); err != nil {
		t.Fatalf("re-running Migrate: %v", err)
	}
}
