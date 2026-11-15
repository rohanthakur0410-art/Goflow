package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"goflow/internal/database"
)

// Authenticator validates API keys against the users table. Keys arrive as
// "Authorization: Bearer <key>" or "X-API-Key: <key>"; only their SHA-256
// hash is ever compared against storage.
type Authenticator struct {
	store *database.Store
}

// NewAuthenticator builds an Authenticator over the given store.
func NewAuthenticator(store *database.Store) *Authenticator {
	return &Authenticator{store: store}
}

type contextKey string

const authenticatedUserKey contextKey = "goflow.authenticatedUserID"

// UserIDFromContext returns the id of the API-key-authenticated user, or 0
// when the request carried no authenticated identity (auth disabled, or a
// read-only endpoint that does not require a key).
func UserIDFromContext(ctx context.Context) int64 {
	if v, ok := ctx.Value(authenticatedUserKey).(int64); ok {
		return v
	}
	return 0
}

// RequireKey wraps a handler so it only runs for requests presenting a valid
// API key. Failures are 401 with the standard error body; no detail about
// whether the key format or the lookup failed is leaked. Keys are never
// logged.
func (a *Authenticator) RequireKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := extractAPIKey(r)
		if key == "" {
			rejectUnauthorized(w, "missing API key")
			return
		}
		u, err := a.store.GetUserByAPIKey(key)
		if errors.Is(err, database.ErrNotFound) {
			rejectUnauthorized(w, "invalid API key")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to authenticate")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), authenticatedUserKey, u.ID)))
	}
}

func rejectUnauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="goflow"`)
	writeError(w, http.StatusUnauthorized, msg)
}

// extractAPIKey reads the key from the Authorization bearer header first,
// then the X-API-Key header. The two headers are never combined.
func extractAPIKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if strings.HasPrefix(h, "Bearer ") {
			return strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
		}
		return ""
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}
