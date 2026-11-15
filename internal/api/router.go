package api

import (
	"log/slog"
	"net/http"
	"time"
)

// NewRouter builds the complete HTTP handler for GoFlow: the JSON REST API,
// the Prometheus metrics endpoint, and the static dashboard files. When auth
// is non-nil, the mutating endpoints (job submit/cancel/retry/delete)
// require a valid API key; read endpoints, /metrics, /health and the
// dashboard stay open so monitoring keeps working without secrets.
func NewRouter(h *Handlers, webDir string, log *slog.Logger, auth *Authenticator) http.Handler {
	mux := http.NewServeMux()

	guard := func(next http.HandlerFunc) http.HandlerFunc { return next }
	if auth != nil {
		guard = auth.RequireKey
	}

	mux.HandleFunc("POST /api/jobs", guard(h.CreateJob))
	mux.HandleFunc("GET /api/jobs", h.ListJobs)
	mux.HandleFunc("GET /api/jobs/{id}", h.GetJob)
	mux.HandleFunc("GET /api/jobs/{id}/attempts", h.ListAttempts)
	mux.HandleFunc("POST /api/jobs/{id}/cancel", guard(h.CancelJob))
	mux.HandleFunc("POST /api/jobs/{id}/retry", guard(h.RetryJob))
	mux.HandleFunc("DELETE /api/jobs/{id}", guard(h.DeleteJob))

	mux.HandleFunc("GET /api/workers", h.ListWorkers)
	mux.HandleFunc("GET /api/workers/{id}", h.GetWorker)
	mux.HandleFunc("GET /api/workers/{id}/history", h.GetWorkerHistory)

	mux.HandleFunc("GET /api/stats", h.Stats)
	mux.HandleFunc("GET /api/health", h.Health)
	mux.HandleFunc("GET /metrics", h.Metrics)

	if webDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(webDir)))
	}

	return recoverMiddleware(logMiddleware(mux, log), log)
}

// logMiddleware logs method, path, status, and duration for every request
// using structured logging (log/slog), matching the fields used elsewhere
// in the app (job_id, worker_id, etc. where applicable).
func logMiddleware(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		log.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// recoverMiddleware converts a panic in any handler into a 500 response
// instead of crashing the server.
func recoverMiddleware(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic in http handler", "error", rec, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.status = code
	sw.ResponseWriter.WriteHeader(code)
}
