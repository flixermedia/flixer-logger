package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/registry"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func validOwnerDeployment(value string) bool {
	return value != "" && len(value) <= 128 && utf8.ValidString(value) && strings.TrimSpace(value) == value
}

func (h *Handler) ownerCredentials(w http.ResponseWriter, r *http.Request) {
	if _, err := h.authorize(r, auth.Owner); err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if h.registry == nil {
		writeError(w, http.StatusServiceUnavailable, "credential registry unavailable")
		return
	}
	if r.Method == http.MethodGet {
		values := r.URL.Query()["deployment_id"]
		if len(values) != 1 || !validOwnerDeployment(values[0]) {
			writeError(w, http.StatusBadRequest, "invalid deployment")
			return
		}
		records := h.registry.ListManagement(values[0])
		items := make([]credentialInfo, 0, len(records))
		for _, record := range records {
			items = append(items, info(record))
		}
		writeJSON(w, http.StatusOK, map[string]any{"credentials": items})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		DeploymentID string    `json:"deployment_id"`
		ExpiresAt    time.Time `json:"expires_at"`
	}
	if err := decoder.Decode(&request); err != nil || !validOwnerDeployment(request.DeploymentID) {
		writeError(w, http.StatusBadRequest, "invalid management credential request")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid management credential request")
		return
	}
	record, token, err := h.registry.IssueManagement(request.DeploymentID, request.ExpiresAt, time.Now())
	switch {
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid management credential request")
	case errors.Is(err, registry.ErrLimit):
		writeError(w, http.StatusTooManyRequests, "credential limit reached; revoke unused credentials")
	case err != nil:
		slog.Error("issue logger management credential failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "credential could not be issued")
	default:
		writeJSON(w, http.StatusCreated, struct {
			Credential credentialInfo `json:"credential"`
			Token      string         `json:"token"`
		}{info(record), token})
	}
}

func (h *Handler) revokeOwnerCredential(w http.ResponseWriter, r *http.Request) {
	if _, err := h.authorize(r, auth.Owner); err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if h.registry == nil {
		writeError(w, http.StatusServiceUnavailable, "credential registry unavailable")
		return
	}
	values := r.URL.Query()["deployment_id"]
	if len(values) != 1 || !validOwnerDeployment(values[0]) {
		writeError(w, http.StatusBadRequest, "invalid deployment")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/owner/credentials/")
	if err := h.registry.RevokeManagement(values[0], id); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			writeError(w, http.StatusNotFound, "credential not found")
		} else {
			slog.Error("revoke logger management credential failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "credential could not be revoked")
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
