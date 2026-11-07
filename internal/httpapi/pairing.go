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

type pairingInfo struct {
	ID               string    `json:"id"`
	ExpiresAt        time.Time `json:"expires_at"`
	ManagementExpiry time.Time `json:"management_expiry"`
}

func pairingMetadata(pairing store.Pairing) pairingInfo {
	return pairingInfo{ID: pairing.ID, ExpiresAt: pairing.ExpiresAt, ManagementExpiry: pairing.ManagementExpiry}
}

func (h *Handler) ownerPairings(w http.ResponseWriter, r *http.Request) {
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
		pairings := h.registry.ListPairings(values[0], time.Now())
		items := make([]pairingInfo, 0, len(pairings))
		for _, pairing := range pairings {
			items = append(items, pairingMetadata(pairing))
		}
		writeJSON(w, http.StatusOK, map[string]any{"pairings": items})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		DeploymentID     string    `json:"deployment_id"`
		ExpiresAt        time.Time `json:"expires_at"`
		ManagementExpiry time.Time `json:"management_expiry"`
	}
	if err := decoder.Decode(&request); err != nil || !validOwnerDeployment(request.DeploymentID) {
		writeError(w, http.StatusBadRequest, "invalid pairing request")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid pairing request")
		return
	}
	pairing, token, err := h.registry.CreatePairing(request.DeploymentID, request.ExpiresAt, request.ManagementExpiry, time.Now())
	switch {
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid pairing request")
	case errors.Is(err, registry.ErrLimit):
		writeError(w, http.StatusTooManyRequests, "pairing limit reached")
	case err != nil:
		slog.Error("create logger pairing failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "pairing could not be created")
	default:
		writeJSON(w, http.StatusCreated, struct {
			Pairing pairingInfo `json:"pairing"`
			Token   string      `json:"token"`
		}{pairingMetadata(pairing), token})
	}
}

func (h *Handler) revokePairing(w http.ResponseWriter, r *http.Request) {
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
	id := strings.TrimPrefix(r.URL.Path, "/api/owner/pairings/")
	if err := h.registry.RevokePairing(values[0], id); err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			writeError(w, http.StatusNotFound, "pairing not found")
		} else {
			slog.Error("revoke logger pairing failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "pairing could not be revoked")
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) redeemPairing(w http.ResponseWriter, r *http.Request) {
	if h.registry == nil {
		writeError(w, http.StatusServiceUnavailable, "credential registry unavailable")
		return
	}
	select {
	case h.pairingRequests <- struct{}{}:
		defer func() { <-h.pairingRequests }()
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "pairing capacity exhausted")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		Token string `json:"token"`
	}
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid pairing request")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid pairing request")
		return
	}
	record, token, err := h.registry.RedeemPairing(request.Token, time.Now())
	switch {
	case errors.Is(err, registry.ErrNotFound):
		writeError(w, http.StatusUnauthorized, "invalid or expired pairing")
	case errors.Is(err, registry.ErrLimit):
		writeError(w, http.StatusTooManyRequests, "credential limit reached")
	case err != nil:
		slog.Error("redeem logger pairing failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "pairing could not be redeemed")
	default:
		writeJSON(w, http.StatusCreated, struct {
			DeploymentID string         `json:"deployment_id"`
			Credential   credentialInfo `json:"credential"`
			Token        string         `json:"token"`
		}{record.DeploymentID, info(record), token})
	}
}
