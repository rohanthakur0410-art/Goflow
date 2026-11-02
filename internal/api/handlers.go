package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"goflow/internal/database"
	"goflow/internal/jobs"
	"goflow/internal/manager"
	"goflow/internal/metrics"
	"goflow/internal/workers"
)

// Handlers holds every dependency the HTTP layer needs. It has no state of
// its own beyond these references.
type Handlers struct {
	mgr     *manager.Manager
	pool    *workers.Pool
	metrics *metrics.Metrics
	log     *slog.Logger
}

// NewHandlers builds the Handlers struct used to construct the router.
func NewHandlers(mgr *manager.Manager, pool *workers.Pool, m *metrics.Metrics, log *slog.Logger) *Handlers {
	return &Handlers{mgr: mgr, pool: pool, metrics: m, log: log}
}

// CreateJob handles POST /api/jobs.
func (h *Handlers) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	payload := []byte(req.Payload)
	if payload == nil {
		payload = []byte("{}")
	}

	j, err := h.mgr.Submit(manager.SubmitInput{
		Type:        req.Type,
		Payload:     payload,
		Priority:    req.Priority,
		MaxRetries:  req.MaxRetries,
		TimeoutSecs: req.TimeoutSecs,
		ScheduledAt: req.ScheduledAt,
	})
	if err != nil {
		if errors.Is(err, manager.ErrValidation) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		h.log.Error("submitting job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to submit job")
		return
	}

	writeJSON(w, http.StatusCreated, j)
}

// ListJobs handles GET /api/jobs?status=&limit=.
func (h *Handlers) ListJobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "limit must be a non-negative integer")
			return
		}
		limit = n
	}

	list, err := h.mgr.Store().ListJobs(status, limit)
	if err != nil {
		h.log.Error("listing jobs", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}
	if list == nil {
		list = make([]*jobs.Job, 0)
	}
	writeJSON(w, http.StatusOK, list)
}

// GetJob handles GET /api/jobs/{id}.
func (h *Handlers) GetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := h.mgr.Store().GetJob(id)
	if errors.Is(err, database.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		h.log.Error("getting job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// ListAttempts handles GET /api/jobs/{id}/attempts, giving detailed failure
// history for a job -- what the README's dead-letter section calls
// "inspecting failure information".
func (h *Handlers) ListAttempts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.mgr.Store().GetJob(id); errors.Is(err, database.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	attempts, err := h.mgr.Store().ListAttempts(id)
	if err != nil {
		h.log.Error("listing attempts", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list attempts")
		return
	}
	if attempts == nil {
		attempts = []database.JobAttempt{}
	}
	writeJSON(w, http.StatusOK, attempts)
}

// CancelJob handles POST /api/jobs/{id}/cancel.
func (h *Handlers) CancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	wasRunning, err := h.mgr.Cancel(id)
	if errors.Is(err, database.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if errors.Is(err, database.ErrConflict) {
		writeError(w, http.StatusConflict, "job is already in a terminal state and cannot be cancelled")
		return
	}
	if err != nil {
		h.log.Error("cancelling job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "cancelled": true, "was_running": wasRunning})
}

// RetryJob handles POST /api/jobs/{id}/retry, used to manually resurrect a
// DEAD (or FAILED) job with a fresh retry budget.
func (h *Handlers) RetryJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := h.mgr.RequeueDead(id)
	if errors.Is(err, database.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if errors.Is(err, database.ErrConflict) {
		writeError(w, http.StatusConflict, "only FAILED or DEAD jobs can be manually retried")
		return
	}
	if err != nil {
		h.log.Error("retrying job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to retry job")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// DeleteJob handles DELETE /api/jobs/{id}.
func (h *Handlers) DeleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := h.mgr.Store().DeleteJob(id)
	if errors.Is(err, database.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		h.log.Error("deleting job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete job")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListWorkers handles GET /api/workers.
func (h *Handlers) ListWorkers(w http.ResponseWriter, r *http.Request) {
	list, err := h.mgr.Store().ListWorkers()
	if err != nil {
		h.log.Error("listing workers", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list workers")
		return
	}
	if list == nil {
		list = make([]*database.WorkerRecord, 0)
	}
	writeJSON(w, http.StatusOK, list)
}

// GetWorker handles GET /api/workers/{id}.
func (h *Handlers) GetWorker(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, err := h.mgr.Store().GetWorker(id)
	if errors.Is(err, database.ErrNotFound) {
		writeError(w, http.StatusNotFound, "worker not found")
		return
	}
	if err != nil {
		h.log.Error("getting worker", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get worker")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

// GetWorkerHistory handles GET /api/workers/{id}/history?limit= and returns
// the worker's most recent job attempts, newest first, from job_attempts.
func (h *Handlers) GetWorkerHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.mgr.Store().GetWorker(id); errors.Is(err, database.ErrNotFound) {
		writeError(w, http.StatusNotFound, "worker not found")
		return
	} else if err != nil {
		h.log.Error("getting worker", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get worker")
		return
	}

	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and 200")
			return
		}
		limit = n
	}

	attempts, err := h.mgr.Store().ListWorkerAttempts(id, limit)
	if err != nil {
		h.log.Error("listing worker history", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list worker history")
		return
	}
	if attempts == nil {
		attempts = []database.JobAttempt{}
	}
	writeJSON(w, http.StatusOK, attempts)
}

// statsResponse combines database-derived job counts with live in-process
// metrics for the dashboard's single summary call.
type statsResponse struct {
	*database.JobStats
	QueueSize     int   `json:"queue_size"`
	ActiveWorkers int   `json:"active_workers"`
	JobsRetried   int64 `json:"jobs_retried_total"`
}

// Stats handles GET /api/stats.
func (h *Handlers) Stats(w http.ResponseWriter, r *http.Request) {
	dbStats, err := h.mgr.Store().Stats()
	if err != nil {
		h.log.Error("computing stats", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to compute stats")
		return
	}
	snap := h.metrics.Snapshot()
	writeJSON(w, http.StatusOK, statsResponse{
		JobStats:      dbStats,
		QueueSize:     h.mgr.Queue().Len(),
		ActiveWorkers: h.pool.ActiveCount(),
		JobsRetried:   snap.JobsRetried,
	})
}

// Health handles GET /api/health.
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	if err := h.mgr.Store().DB.Ping(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Metrics handles GET /metrics in Prometheus text exposition format.
func (h *Handlers) Metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(h.metrics.Render()))
}
