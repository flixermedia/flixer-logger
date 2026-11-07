package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func (h *Handler) ingestionPolicy(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Manage)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if r.Method == http.MethodGet {
		policy, err := h.store.IngestionPolicy(identity.DeploymentID)
		if err != nil {
			slog.Error("read ingestion policy failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "ingestion policy unavailable")
			return
		}
		writeJSON(w, http.StatusOK, policy)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var policy store.IngestionPolicy
	if err := decoder.Decode(&policy); err != nil {
		writeError(w, http.StatusBadRequest, "invalid ingestion policy")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid ingestion policy")
		return
	}
	if err := h.store.SetIngestionPolicy(identity.DeploymentID, policy); err != nil {
		if errors.Is(err, store.ErrInvalidInput) {
			writeError(w, http.StatusBadRequest, "invalid ingestion policy")
		} else {
			slog.Error("save ingestion policy failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "ingestion policy could not be saved")
		}
		return
	}
	writeJSON(w, http.StatusOK, policy)
}
