// Package tests contains black-box integration tests that exercise GoFlow
// as a whole: a real HTTP server, a real SQLite database, a real worker
// pool with the actual registered handlers, and the scheduler -- wired up
// exactly as cmd/server/main.go wires them, just without the OS signal
// handling.
package tests

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
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

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// stack bundles a fully-wired GoFlow instance for integration testing.
type stack struct {
	server *httptest.Server
	mgr    *manager.Manager
	cancel context.CancelFunc
}

func startStack(t *testing.T) *stack {
	t.Helper()
	dir := t.TempDir()

	store, err := database.Open(filepath.Join(dir, "goflow.db"))
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	migrationsDir, err := filepath.Abs("../migrations")
	if err != nil {
		t.Fatalf("resolving migrations path: %v", err)
	}
	if err := store.Migrate(migrationsDir); err != nil {
		t.Fatalf("migrating: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	registry := jobs.NewRegistry()
	jobs.RegisterDefaultHandlers(registry)

	m := metrics.New()
	q := queue.New()
	log := slog.New(slog.NewTextHandler(discardWriter{}, nil))
	mgr := manager.New(store, q, m, log, 3, 2*time.Second)

	cfg := &config.Config{
		WorkerCount: 2, QueueSize: 100, MaxRetries: 3,
		DefaultJobTimeout: 2 * time.Second, ScalingCheckInterval: 100 * time.Millisecond,
		HeartbeatInterval: 200 * time.Millisecond, StaleWorkerThreshold: 5 * time.Second,
		SchedulerPollInterval: 100 * time.Millisecond,
		ScalingRules:          []config.ScalingRule{{QueueSize: 999999, WorkerCount: 2}},
	}

	poolCtx, cancel := context.WithCancel(context.Background())
	pool := workers.New(mgr, registry, m, cfg, log)
	pool.Start(poolCtx)

	sched := scheduler.New(mgr, cfg.SchedulerPollInterval, log)
	go sched.Run(poolCtx)

	handlers := api.NewHandlers(mgr, pool, m, log)
	router := api.NewRouter(handlers, "", log)
	server := httptest.NewServer(router)

	t.Cleanup(func() {
		server.Close()
		cancel()
	})

	return &stack{server: server, mgr: mgr, cancel: cancel}
}

func (s *stack) post(t *testing.T, path string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf []byte
	if body != nil {
		var err error
		buf, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshaling body: %v", err)
		}
	}
	req, err := http.NewRequest(http.MethodPost, s.server.URL+path, bytesReader(buf))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return resp, decodeJSON(t, resp)
}

func (s *stack) get(t *testing.T, path string) (*http.Response, map[string]any) {
	t.Helper()
	resp, err := http.Get(s.server.URL + path)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return resp, decodeJSON(t, resp)
}

func decodeJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if len(body) == 0 {
		return nil
	}
	var m map[string]any
	// Some endpoints return arrays; callers that need those decode manually.
	_ = json.Unmarshal(body, &m)
	return m
}

func bytesReader(b []byte) *bytesBuf { return &bytesBuf{b: b} }

// bytesBuf is a minimal io.Reader over a byte slice, avoiding an extra
// bytes.NewReader import juggling act in this file.
type bytesBuf struct {
	b   []byte
	pos int
}

func (r *bytesBuf) Read(p []byte) (int, error) {
	if r.pos >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.pos:])
	r.pos += n
	return n, nil
}

// TestEndToEnd_FullJobLifecycle submits a job through the real HTTP API and
// verifies it flows all the way through the queue, a real worker, and back
// into SQLite as COMPLETED -- the same path a genuine client would exercise.
func TestEndToEnd_FullJobLifecycle(t *testing.T) {
	s := startStack(t)

	resp, created := s.post(t, "/api/jobs", map[string]any{
		"type":     "email",
		"priority": "HIGH",
		"payload":  map[string]string{"to": "recruiter@example.com", "subject": "GoFlow demo", "body": "It works."},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	id := created["id"].(string)

	deadline := time.Now().Add(3 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		_, job := s.get(t, "/api/jobs/"+id)
		status, _ = job["status"].(string)
		if status == "COMPLETED" {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if status != "COMPLETED" {
		t.Fatalf("job did not complete in time, last status: %s", status)
	}

	_, stats := s.get(t, "/api/stats")
	if stats["completed"].(float64) < 1 {
		t.Fatalf("expected at least 1 completed job in stats, got %+v", stats)
	}
}

// TestEndToEnd_DeadLetterAndManualRetry exercises the full failure path: a
// job that always fails should end up DEAD, be inspectable via the attempts
// endpoint, and then be resurrected via the retry endpoint.
func TestEndToEnd_DeadLetterAndManualRetry(t *testing.T) {
	s := startStack(t)

	resp, created := s.post(t, "/api/jobs", map[string]any{
		"type": "webhook", "priority": "HIGH", "max_retries": 0,
		"payload": map[string]string{"url": "http://fail.test/hook", "method": "POST"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	id := created["id"].(string)

	deadline := time.Now().Add(3 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		_, job := s.get(t, "/api/jobs/"+id)
		status, _ = job["status"].(string)
		if status == "DEAD" {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if status != "DEAD" {
		t.Fatalf("expected job to reach DEAD, last status: %s", status)
	}

	resp, _ = s.get(t, "/api/jobs/"+id+"/attempts")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from attempts endpoint, got %d", resp.StatusCode)
	}

	resp, retried := s.post(t, "/api/jobs/"+id+"/retry", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from retry, got %d", resp.StatusCode)
	}
	if retried["status"] != "PENDING" {
		t.Fatalf("expected PENDING after manual retry, got %v", retried["status"])
	}
}

// TestEndToEnd_ScheduledJobRunsWhenDue confirms a job submitted with a
// future scheduled_at stays SCHEDULED until the scheduler promotes it.
func TestEndToEnd_ScheduledJobRunsWhenDue(t *testing.T) {
	s := startStack(t)

	scheduledAt := time.Now().UTC().Add(300 * time.Millisecond).Format(time.RFC3339Nano)
	resp, created := s.post(t, "/api/jobs", map[string]any{
		"type": "data_processing", "priority": "MEDIUM", "scheduled_at": scheduledAt,
		"payload": map[string]string{"dataset": "d1", "operation": "agg"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	if created["status"] != "SCHEDULED" {
		t.Fatalf("expected SCHEDULED at submission time, got %v", created["status"])
	}
	id := created["id"].(string)

	deadline := time.Now().Add(3 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		_, job := s.get(t, "/api/jobs/"+id)
		status, _ = job["status"].(string)
		if status == "COMPLETED" {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if status != "COMPLETED" {
		t.Fatalf("expected scheduled job to eventually complete, last status: %s", status)
	}
}
