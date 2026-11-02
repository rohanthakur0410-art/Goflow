package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"goflow/internal/config"
	"goflow/internal/database"
	"goflow/internal/jobs"
	"goflow/internal/manager"
	"goflow/internal/metrics"
	"goflow/internal/queue"
	"goflow/internal/workers"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// testServer builds a fully wired API (real SQLite, real queue, real worker
// pool with the default job handlers) for black-box HTTP testing.
func testServer(t *testing.T) (http.Handler, *manager.Manager, func()) {
	t.Helper()
	dir := t.TempDir()
	store, err := database.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	migrationsDir, _ := filepath.Abs("../../migrations")
	if err := store.Migrate(migrationsDir); err != nil {
		t.Fatalf("migrating: %v", err)
	}

	registry := jobs.NewRegistry()
	jobs.RegisterDefaultHandlers(registry)

	m := metrics.New()
	q := queue.New()
	log := discardLogger()
	mgr := manager.New(store, q, m, log, 3, 2*time.Second)

	cfg := &config.Config{
		WorkerCount: 2, QueueSize: 100, MaxRetries: 3,
		DefaultJobTimeout: 2 * time.Second, ScalingCheckInterval: 100 * time.Millisecond,
		HeartbeatInterval: 200 * time.Millisecond, StaleWorkerThreshold: 5 * time.Second,
		ScalingRules: []config.ScalingRule{{QueueSize: 999999, WorkerCount: 2}},
	}
	pool := workers.New(mgr, registry, m, cfg, log)
	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	handlers := NewHandlers(mgr, pool, m, log)
	router := NewRouter(handlers, "", log)

	cleanup := func() {
		cancel()
		store.Close()
	}
	return router, mgr, cleanup
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encoding request body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAPI_CreateAndGetJob(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	rec := doJSON(t, h, http.MethodPost, "/api/jobs", map[string]any{
		"type": "email", "priority": "HIGH",
		"payload": map[string]string{"to": "a@b.com", "subject": "hi", "body": "hello"},
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var created jobs.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if created.ID == "" || created.Status != jobs.StatusPending {
		t.Fatalf("unexpected created job: %+v", created)
	}

	rec = doJSON(t, h, http.MethodGet, "/api/jobs/"+created.ID, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAPI_CreateJob_Validation(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	rec := doJSON(t, h, http.MethodPost, "/api/jobs", map[string]any{"priority": "HIGH"}) // missing type
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing type, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doJSON(t, h, http.MethodPost, "/api/jobs", map[string]any{"type": "email", "priority": "URGENT"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid priority, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAPI_GetJob_NotFound(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	rec := doJSON(t, h, http.MethodGet, "/api/jobs/does-not-exist", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestAPI_ListJobs(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	for i := 0; i < 3; i++ {
		rec := doJSON(t, h, http.MethodPost, "/api/jobs", map[string]any{
			"type": "email", "priority": "LOW", "payload": map[string]string{"to": "a@b.com"},
		})
		if rec.Code != http.StatusCreated {
			t.Fatalf("create job: %d %s", rec.Code, rec.Body.String())
		}
	}

	rec := doJSON(t, h, http.MethodGet, "/api/jobs", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var list []jobs.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(list))
	}
}

func TestAPI_CancelJob(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	rec := doJSON(t, h, http.MethodPost, "/api/jobs", map[string]any{
		"type": "email", "priority": "LOW", "payload": map[string]string{"to": "a@b.com"},
	})
	var created jobs.Job
	json.Unmarshal(rec.Body.Bytes(), &created)

	rec = doJSON(t, h, http.MethodPost, "/api/jobs/"+created.ID+"/cancel", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Cancelling again should conflict.
	rec = doJSON(t, h, http.MethodPost, "/api/jobs/"+created.ID+"/cancel", nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 on double-cancel, got %d", rec.Code)
	}
}

func TestAPI_CancelJob_NotFound(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	rec := doJSON(t, h, http.MethodPost, "/api/jobs/nope/cancel", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestAPI_RetryDeadJob(t *testing.T) {
	h, mgr, cleanup := testServer(t)
	defer cleanup()

	maxRetries := 0
	j, err := mgr.Submit(manager.SubmitInput{
		Type: "webhook", Payload: []byte(`{"url":"http://fail.test/x","method":"POST"}`),
		Priority: jobs.PriorityHigh, MaxRetries: &maxRetries,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := mgr.Store().GetJob(j.ID)
		if got != nil && got.Status == jobs.StatusDead {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	rec := doJSON(t, h, http.MethodPost, "/api/jobs/"+j.ID+"/retry", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var retried jobs.Job
	json.Unmarshal(rec.Body.Bytes(), &retried)
	if retried.Status != jobs.StatusPending {
		t.Fatalf("expected PENDING after retry, got %s", retried.Status)
	}
}

func TestAPI_DeleteJob(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	rec := doJSON(t, h, http.MethodPost, "/api/jobs", map[string]any{
		"type": "email", "priority": "LOW", "payload": map[string]string{"to": "a@b.com"},
	})
	var created jobs.Job
	json.Unmarshal(rec.Body.Bytes(), &created)

	rec = doJSON(t, h, http.MethodDelete, "/api/jobs/"+created.ID, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}

	rec = doJSON(t, h, http.MethodGet, "/api/jobs/"+created.ID, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", rec.Code)
	}
}

func TestAPI_ListWorkers(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	time.Sleep(50 * time.Millisecond) // let the pool register its initial workers
	rec := doJSON(t, h, http.MethodGet, "/api/workers", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var list []database.WorkerRecord
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 registered workers, got %d", len(list))
	}
}

func TestAPI_WorkerHistory(t *testing.T) {
	h, mgr, cleanup := testServer(t)
	defer cleanup()

	// Run a real job so a worker records an attempt.
	j, err := mgr.Submit(manager.SubmitInput{
		Type: "email", Payload: []byte(`{"to":"a@b.com"}`),
		Priority: jobs.PriorityHigh,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := mgr.Store().GetJob(j.ID)
		if got != nil && got.Status == jobs.StatusCompleted {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	attempts, err := mgr.Store().ListAttempts(j.ID)
	if err != nil || len(attempts) == 0 {
		t.Fatalf("expected a recorded attempt, got %v, %v", attempts, err)
	}
	workerID := attempts[0].WorkerID

	rec := doJSON(t, h, http.MethodGet, "/api/workers/"+workerID+"/history", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var history []database.JobAttempt
	if err := json.Unmarshal(rec.Body.Bytes(), &history); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	found := false
	for _, a := range history {
		if a.JobID == j.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected history for %s to include job %s, got %+v", workerID, j.ID, history)
	}

	// Unknown worker -> 404; out-of-contract limits -> 400.
	rec = doJSON(t, h, http.MethodGet, "/api/workers/nope/history", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown worker, got %d", rec.Code)
	}
	for _, bad := range []string{"abc", "0", "-1", "201"} {
		rec = doJSON(t, h, http.MethodGet, "/api/workers/"+workerID+"/history?limit="+bad, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for limit=%s, got %d", bad, rec.Code)
		}
	}

	// A valid limit within range is accepted.
	rec = doJSON(t, h, http.MethodGet, "/api/workers/"+workerID+"/history?limit=1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for limit=1, got %d", rec.Code)
	}
	var one []database.JobAttempt
	json.Unmarshal(rec.Body.Bytes(), &one)
	if len(one) != 1 {
		t.Fatalf("expected exactly 1 attempt with limit=1, got %d", len(one))
	}

	// A registered worker with no attempts -> 200 with an empty list.
	workers, err := mgr.Store().ListWorkers()
	if err != nil || len(workers) == 0 {
		t.Fatalf("expected registered workers, got %v, %v", workers, err)
	}
	idle := ""
	for _, wr := range workers {
		if wr.ID != workerID {
			idle = wr.ID
			break
		}
	}
	if idle == "" {
		t.Fatal("expected a second registered worker")
	}
	rec = doJSON(t, h, http.MethodGet, "/api/workers/"+idle+"/history", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for idle worker, got %d", rec.Code)
	}
	var empty []database.JobAttempt
	json.Unmarshal(rec.Body.Bytes(), &empty)
	if len(empty) != 0 {
		t.Fatalf("expected empty history for idle worker, got %+v", empty)
	}
}

func TestAPI_WorkerHistory_StoreError(t *testing.T) {
	h, mgr, cleanup := testServer(t)
	defer cleanup()

	// Closing the database makes GetWorker fail with a non-NotFound error;
	// the handler must surface a 500, not proceed to the history query.
	mgr.Store().DB.Close()
	rec := doJSON(t, h, http.MethodGet, "/api/workers/any/history", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on store error, got %d", rec.Code)
	}
}

func TestAPI_Stats(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	rec := doJSON(t, h, http.MethodGet, "/api/stats", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAPI_Health(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	rec := doJSON(t, h, http.MethodGet, "/api/health", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestAPI_Metrics(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	rec := doJSON(t, h, http.MethodGet, "/metrics", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("goflow_jobs_submitted_total")) {
		t.Fatal("expected Prometheus metrics output to include goflow_jobs_submitted_total")
	}
}

func TestAPI_MalformedJSON(t *testing.T) {
	h, _, cleanup := testServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewBufferString("{not valid json"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed JSON, got %d", rec.Code)
	}
}
