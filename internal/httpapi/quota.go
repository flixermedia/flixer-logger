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

func (h *Handler) quota(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Manage)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if r.Method == http.MethodPut {
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var request struct {
			MaxBytes *uint64 `json:"max_bytes"`
		}
		if err := decoder.Decode(&request); err != nil || request.MaxBytes == nil {
			writeError(w, http.StatusBadRequest, "invalid quota policy")
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			writeError(w, http.StatusBadRequest, "invalid quota policy")
			return
		}
		if err := h.store.SetQuotaPolicy(identity.DeploymentID, store.QuotaPolicy{MaxBytes: *request.MaxBytes}); err != nil {
			if errors.Is(err, store.ErrInvalidInput) {
				writeError(w, http.StatusBadRequest, "invalid quota policy")
			} else {
				slog.Error("save logger quota failed", "error", err)
				writeError(w, http.StatusServiceUnavailable, "quota update failed")
			}
			return
		}
	}
	policy, err := h.store.QuotaPolicy(identity.DeploymentID)
	if err != nil {
		slog.Error("read logger quota failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "quota unavailable")
		return
	}
	usage, err := h.store.LogicalUsage(identity.DeploymentID)
	if err != nil {
		slog.Error("read logger logical usage failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "quota unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"max_bytes": policy.MaxBytes, "used_bytes": usage})
}
