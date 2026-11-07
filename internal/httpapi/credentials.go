package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/registry"
	"github.com/flixermedia/flixer-logger/internal/store"
)

type credentialInfo struct {
	ID        string     `json:"id"`
	Scope     auth.Scope `json:"scope"`
	App       string     `json:"app,omitempty"`
	ExpiresAt time.Time  `json:"expires_at"`
}

func info(record store.StoredCredential) credentialInfo {
	return credentialInfo{ID: record.ID, Scope: record.Scope, App: record.App, ExpiresAt: record.ExpiresAt}
}

func (h *Handler) credentials(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Manage)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if h.registry == nil {
		writeError(w, http.StatusServiceUnavailable, "credential registry unavailable")
		return
	}
	if r.Method == http.MethodGet {
		records := h.registry.List(identity.DeploymentID)
		credentials := make([]credentialInfo, 0, len(records))
		for _, record := range records {
			credentials = append(credentials, info(record))
		}
		writeJSON(w, http.StatusOK, map[string]any{"credentials": credentials})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		Scope     auth.Scope `json:"scope"`
		App       string     `json:"app"`
		ExpiresAt time.Time  `json:"expires_at"`
	}
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid credential request")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid credential request")
		return
	}
	record, token, err := h.registry.IssueForApp(identity.DeploymentID, request.Scope, request.App, request.ExpiresAt, time.Now())
	switch {
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid credential request")
	case errors.Is(err, registry.ErrLimit):
		writeError(w, http.StatusTooManyRequests, "credential limit reached; revoke unused credentials")
	case err != nil:
		slog.Error("issue logger credential failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "credential could not be issued")
	default:
		writeJSON(w, http.StatusCreated, struct {
			Credential credentialInfo `json:"credential"`
			Token      string         `json:"token"`
		}{info(record), token})
	}
}

func (h *Handler) revokeCredential(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Manage)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if h.registry == nil {
		writeError(w, http.StatusServiceUnavailable, "credential registry unavailable")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/logger/credentials/")
	if err := h.registry.Revoke(identity.DeploymentID, id); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			writeError(w, http.StatusNotFound, "credential not found")
		} else {
			slog.Error("revoke logger credential failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "credential could not be revoked")
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
