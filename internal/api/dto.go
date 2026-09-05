package api

import (
	"encoding/json"
	"net/http"
	"time"

	"goflow/internal/jobs"
)

// createJobRequest is the JSON body accepted by POST /api/jobs.
type createJobRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    jobs.Priority   `json:"priority"`
	MaxRetries  *int            `json:"max_retries,omitempty"`
	TimeoutSecs *int            `json:"timeout_seconds,omitempty"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
}

// errorResponse is the standard error body shape returned by every handler.
type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
