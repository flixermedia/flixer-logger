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

func (h *Handler) retention(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Manage)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if r.Method == http.MethodGet {
		policy, err := h.store.RetentionPolicy(identity.DeploymentID)
		if err != nil {
			slog.Error("read retention policy failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "retention policy unavailable")
			return
		}
		writeJSON(w, http.StatusOK, policy)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input struct {
		Enabled      *bool          `json:"enabled"`
		Days         *int           `json:"days"`
		SeverityDays map[string]int `json:"severity_days"`
	}
	if err := decoder.Decode(&input); err != nil || input.Enabled == nil || input.Days == nil {
		writeError(w, http.StatusBadRequest, "invalid retention policy")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid retention policy")
		return
	}
	policy := store.RetentionPolicy{Enabled: *input.Enabled, Days: *input.Days, SeverityDays: input.SeverityDays}
	if err := h.store.SetRetentionPolicy(identity.DeploymentID, policy); err != nil {
		if errors.Is(err, store.ErrInvalidInput) {
			writeError(w, http.StatusBadRequest, "invalid retention policy")
		} else {
			slog.Error("save retention policy failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "retention policy could not be saved")
		}
		return
	}
	writeJSON(w, http.StatusOK, policy)
}
