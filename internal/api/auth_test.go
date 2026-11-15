package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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

// authServer builds the same fully wired API as testServer but with API-key
// auth enabled on the mutating endpoints. It returns the store too so tests
// can seed keys and inspect attribution.
func authServer(t *testing.T) (http.Handler, *database.Store, func()) {
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
	if _, err := store.HashPlaintextAPIKeys(); err != nil {
		t.Fatalf("hashing keys: %v", err)
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
	router := NewRouter(handlers, "", log, NewAuthenticator(store))

	cleanup := func() {
		cancel()
		store.Close()
	}
	return router, store, cleanup
}

func postJob(t *testing.T, router http.Handler, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	body := bytes.NewBufferString(`{"type":"email","payload":{"to":"a@b.c"},"priority":"LOW"}`)
	req := httptest.NewRequest("POST", "/api/jobs", body)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	return rr
}

func TestRequireKey_MissingKey(t *testing.T) {
	router, _, cleanup := authServer(t)
	defer cleanup()

	rr := postJob(t, router, nil)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (%s)", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("WWW-Authenticate"); got == "" {
		t.Fatal("401 should carry WWW-Authenticate")
	}
	var e errorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &e); err != nil || e.Error == "" {
		t.Fatalf("expected standard error body, got %q", rr.Body.String())
	}
}

func TestRequireKey_InvalidKey(t *testing.T) {
	router, _, cleanup := authServer(t)
	defer cleanup()

	rr := postJob(t, router, map[string]string{"Authorization": "Bearer totally-wrong"})
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong key, got %d", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "totally-wrong") {
		t.Fatal("error body must not echo the presented key")
	}
}

func TestRequireKey_ValidKeyBothHeaders(t *testing.T) {
	router, store, cleanup := authServer(t)
	defer cleanup()

	if err := store.SetUserAPIKeyHash(1, "gf_test_admin"); err != nil {
		t.Fatalf("seeding key: %v", err)
	}

	rr := postJob(t, router, map[string]string{"Authorization": "Bearer gf_test_admin"})
	if rr.Code != http.StatusCreated {
		t.Fatalf("bearer key should authenticate, got %d (%s)", rr.Code, rr.Body.String())
	}
	rr = postJob(t, router, map[string]string{"X-API-Key": "gf_test_admin"})
	if rr.Code != http.StatusCreated {
		t.Fatalf("X-API-Key should authenticate, got %d (%s)", rr.Code, rr.Body.String())
	}
}

func TestRequireKey_AttributesJobToKeyOwner(t *testing.T) {
	router, store, cleanup := authServer(t)
	defer cleanup()

	if _, err := store.DB.Exec(
		`INSERT INTO users (username, api_key) VALUES (?, ?)`,
		"rohan", database.HashAPIKey("gf_rohan_key"),
	); err != nil {
		t.Fatalf("inserting second user: %v", err)
	}

	rr := postJob(t, router, map[string]string{"X-API-Key": "gf_rohan_key"})
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", rr.Code, rr.Body.String())
	}
	list, err := store.ListJobs("", 10)
	if err != nil || len(list) == 0 {
		t.Fatalf("listing jobs: %v", err)
	}
	if list[0].CreatedBy != 2 {
		t.Fatalf("job should be attributed to key owner (user 2), got %d", list[0].CreatedBy)
	}
}

func TestRequireKey_ReadsStayOpen(t *testing.T) {
	router, _, cleanup := authServer(t)
	defer cleanup()

	for _, path := range []string{"/api/jobs", "/api/health", "/api/stats", "/api/workers"} {
		req := httptest.NewRequest("GET", path, nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("GET %s should stay open without a key, got %d", path, rr.Code)
		}
	}
}

func TestExtractAPIKey(t *testing.T) {
	bearer := httptest.NewRequest("POST", "/", nil)
	bearer.Header.Set("Authorization", "Bearer gf_abc")
	if got := extractAPIKey(bearer); got != "gf_abc" {
		t.Fatalf("bearer: got %q", got)
	}

	xkey := httptest.NewRequest("POST", "/", nil)
	xkey.Header.Set("X-API-Key", "gf_xyz")
	if got := extractAPIKey(xkey); got != "gf_xyz" {
		t.Fatalf("x-api-key: got %q", got)
	}

	none := httptest.NewRequest("POST", "/", nil)
	if got := extractAPIKey(none); got != "" {
		t.Fatalf("no headers should give empty key, got %q", got)
	}

	basic := httptest.NewRequest("POST", "/", nil)
	basic.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	if got := extractAPIKey(basic); got != "" {
		t.Fatalf("non-bearer Authorization should give empty key, got %q", got)
	}
}
