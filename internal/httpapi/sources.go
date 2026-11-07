package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func (h *Handler) sources(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Query)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	start, err := time.Parse(time.RFC3339Nano, r.URL.Query().Get("start"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid statistics start")
		return
	}
	end, err := time.Parse(time.RFC3339Nano, r.URL.Query().Get("end"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid statistics end")
		return
	}
	release, ok := acquireCapacity(w, h.searches, h.tenantSearches, identity.DeploymentID)
	if !ok {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	sources, err := h.store.SourceStats(ctx, identity.DeploymentID, start, end)
	switch {
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid statistics window")
	case errors.Is(err, store.ErrQueryBudget):
		writeError(w, http.StatusUnprocessableEntity, "source statistics limit exceeded; narrow the window")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, "source statistics timed out or were canceled")
	case err != nil:
		slog.Error("logger source statistics failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "source statistics failed")
	default:
		writeJSON(w, http.StatusOK, map[string]any{"sources": sources})
	}
}
