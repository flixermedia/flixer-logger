package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/flixermedia/flixer-logger/internal/auth"
)

func (h *Handler) clearLogs(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Manage)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	cleared, err := h.store.ClearDeployment(identity.DeploymentID)
	if err != nil {
		slog.Error("clear deployment logs failed", "deployment", identity.DeploymentID, "error", err)
		writeError(w, http.StatusServiceUnavailable, "log history could not be cleared")
		return
	}
	writeJSON(w, http.StatusOK, map[string]uint64{"cleared": cleared})
}
