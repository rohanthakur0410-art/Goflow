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
