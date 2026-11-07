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

func (h *Handler) ownerCapacityPolicy(w http.ResponseWriter, r *http.Request) {
	if _, err := h.authorize(r, auth.Owner); err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, h.store.CapacityPolicy())
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		MaxDatabaseBytes *uint64 `json:"max_database_bytes"`
		MinFreeBytes     *uint64 `json:"min_free_bytes"`
	}
	if err := decoder.Decode(&request); err != nil || request.MaxDatabaseBytes == nil || request.MinFreeBytes == nil {
		writeError(w, http.StatusBadRequest, "invalid capacity policy")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid capacity policy")
		return
	}
	policy := store.CapacityPolicy{MaxDatabaseBytes: *request.MaxDatabaseBytes, MinFreeBytes: *request.MinFreeBytes}
	if err := h.store.SetCapacityPolicy(policy); err != nil {
		if errors.Is(err, store.ErrInvalidInput) {
			writeError(w, http.StatusBadRequest, "invalid capacity policy")
		} else {
			slog.Error("save logger capacity policy failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "capacity policy could not be saved")
		}
		return
	}
	writeJSON(w, http.StatusOK, policy)
}
