// Package httpapi implements deployment-scoped logger endpoints.
package httpapi

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/redact"
	"github.com/flixermedia/flixer-logger/internal/registry"
	"github.com/flixermedia/flixer-logger/internal/retention"
	"github.com/flixermedia/flixer-logger/internal/store"
)

const maxBodyBytes = 2 * 1024 * 1024
const schemaHeader = "X-Flixer-Log-Schema"

//go:embed openapi.json
var openAPI []byte

type Handler struct {
	store           *store.Store
	verifier        *auth.Verifier
	live            *liveHub
	searches        chan struct{}
	ingests         chan struct{}
	pairingRequests chan struct{}
	tenantSearches  *tenantCapacity
	tenantIngests   *tenantCapacity
	rates           *rateLimiter
	registry        *registry.Registry
	liveOrdering    sync.Mutex
	retentionStatus *retention.Tracker
}

func NewWithRegistry(s *store.Store, credentials *registry.Registry) (*Handler, error) {
	if credentials == nil {
		return nil, errors.New("credential registry is required")
	}
	handler, err := New(s, credentials.Verifier())
	if err != nil {
		return nil, err
	}
	handler.registry = credentials
	return handler, nil
}

func New(s *store.Store, verifier *auth.Verifier) (*Handler, error) {
	if s == nil || verifier == nil {
		return nil, errors.New("store and credential verifier are required")
	}
	return &Handler{
		store: s, verifier: verifier, live: newLiveHub(),
		searches: make(chan struct{}, 8), ingests: make(chan struct{}, 16), pairingRequests: make(chan struct{}, 4),
		tenantSearches: newTenantCapacity(4), tenantIngests: newTenantCapacity(4),
		rates:           newRateLimiter(),
		retentionStatus: retention.NewTracker(10000),
	}, nil
}

// RetentionTracker connects the background worker to deployment status.
func (h *Handler) RetentionTracker() *retention.Tracker { return h.retentionStatus }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(schemaHeader, "1")
	if browserReadRoute(r.URL.Path) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Last-Event-ID")
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api/openapi.json":
		if _, err := w.Write(openAPI); err != nil {
			slog.Error("logger API contract response failed", "error", err)
		}
	case r.Method == http.MethodPost && r.URL.Path == "/api/logs/batch":
		h.ingest(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/logs":
		h.query(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/logs/trace/"):
		h.trace(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/api/logs/search":
		h.search(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/logs/stats":
		h.stats(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/logs/sources":
		h.sources(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/logs/errors":
		h.errorGroups(w, r)
	case (r.Method == http.MethodGet || r.Method == http.MethodPut) && r.URL.Path == "/api/logger/retention":
		h.retention(w, r)
	case (r.Method == http.MethodGet || r.Method == http.MethodPut) && r.URL.Path == "/api/logger/ingestion-policy":
		h.ingestionPolicy(w, r)
	case (r.Method == http.MethodGet || r.Method == http.MethodPut) && r.URL.Path == "/api/logger/quota":
		h.quota(w, r)
	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && r.URL.Path == "/api/logger/credentials":
		h.credentials(w, r)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/logger/credentials/"):
		h.revokeCredential(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/logs/stream":
		h.stream(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/health/live":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodGet && r.URL.Path == "/api/status":
		h.status(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/owner/capacity":
		h.ownerCapacity(w, r)
	case (r.Method == http.MethodGet || r.Method == http.MethodPut) && r.URL.Path == "/api/owner/capacity-policy":
		h.ownerCapacityPolicy(w, r)
	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && r.URL.Path == "/api/owner/credentials":
		h.ownerCredentials(w, r)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/owner/credentials/"):
		h.revokeOwnerCredential(w, r)
	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && r.URL.Path == "/api/owner/pairings":
		h.ownerPairings(w, r)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/owner/pairings/"):
		h.revokePairing(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/api/logger/pairings/redeem":
		h.redeemPairing(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/health/ready":
		if err := h.store.Ready(); err != nil {
			slog.Error("logger readiness check failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "storage not ready")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (h *Handler) ownerCapacity(w http.ResponseWriter, r *http.Request) {
	if _, err := h.authorize(r, auth.Owner); err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	capacity, err := h.store.Capacity()
	if err != nil {
		slog.Error("logger capacity check failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "capacity unavailable")
		return
	}
	writeJSON(w, http.StatusOK, capacity)
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Manage)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if err := h.store.Ready(); err != nil {
		slog.Error("logger status readiness failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "storage not ready")
		return
	}
	events, err := h.store.Recent(identity.DeploymentID, 1)
	if err != nil {
		slog.Error("logger status latest event failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "deployment status unavailable")
		return
	}
	var lastIngestedAt *time.Time
	if len(events) > 0 {
		lastIngestedAt = &events[0].IngestedAt
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":            "ready",
		"last_ingested_at":  lastIngestedAt,
		"live_subscribers":  h.live.count(identity.DeploymentID),
		"active_ingestions": h.tenantIngests.count(identity.DeploymentID),
		"active_queries":    h.tenantSearches.count(identity.DeploymentID),
		"max_ingestions":    h.tenantIngests.limit,
		"max_queries":       h.tenantSearches.limit,
		"retention":         h.retentionStatus.Get(identity.DeploymentID),
	})
}

func browserReadRoute(path string) bool {
	if strings.HasPrefix(path, "/api/logs/trace/") {
		return true
	}
	switch path {
	case "/api/logs", "/api/logs/search", "/api/logs/stats", "/api/logs/sources", "/api/logs/errors", "/api/logs/stream":
		return true
	default:
		return false
	}
}

func (h *Handler) trace(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Query)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	traceID := strings.TrimPrefix(r.URL.Path, "/api/logs/trace/")
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 1000 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
	}
	release, ok := acquireCapacity(w, h.searches, h.tenantSearches, identity.DeploymentID)
	if !ok {
		return
	}
	defer release()
	events, next, err := h.store.QueryIndex(identity.DeploymentID, "trace", traceID, r.URL.Query().Get("cursor"), limit)
	switch {
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid trace query")
	case err != nil:
		slog.Error("logger trace query failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "trace query failed")
	default:
		writeJSON(w, http.StatusOK, map[string]any{"events": events, "next_cursor": next})
	}
}

func (h *Handler) errorGroups(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Query)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	start, err := time.Parse(time.RFC3339Nano, r.URL.Query().Get("start"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid error grouping start")
		return
	}
	end, err := time.Parse(time.RFC3339Nano, r.URL.Query().Get("end"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid error grouping end")
		return
	}
	release, ok := acquireCapacity(w, h.searches, h.tenantSearches, identity.DeploymentID)
	if !ok {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	groups, err := h.store.ErrorGroups(ctx, identity.DeploymentID, start, end)
	switch {
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid error grouping window")
	case errors.Is(err, store.ErrQueryBudget):
		writeError(w, http.StatusUnprocessableEntity, "error grouping limit exceeded; narrow the window")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, "error grouping timed out or was canceled")
	case err != nil:
		slog.Error("logger error grouping failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "error grouping failed")
	default:
		writeJSON(w, http.StatusOK, map[string]any{"groups": groups})
	}
}

func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
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
	buckets, err := h.store.LevelStats(ctx, identity.DeploymentID, start, end)
	switch {
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid statistics window")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, "statistics query timed out or was canceled")
	case err != nil:
		slog.Error("logger statistics failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "statistics query failed")
	default:
		writeJSON(w, http.StatusOK, map[string]any{"buckets": buckets})
	}
}

func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Query)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	release, ok := acquireCapacity(w, h.searches, h.tenantSearches, identity.DeploymentID)
	if !ok {
		return
	}
	defer release()
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		store.SearchRequest
		Projection []string `json:"projection"`
	}
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid search request")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid search request")
		return
	}
	if err := validateProjection(request.Projection); err != nil {
		writeError(w, http.StatusBadRequest, "invalid search projection")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	events, next, err := h.store.Search(ctx, identity.DeploymentID, request.SearchRequest)
	switch {
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid search request")
	case errors.Is(err, store.ErrQueryBudget):
		writeError(w, http.StatusUnprocessableEntity, "search candidate limit exceeded; narrow the search")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, "search timed out or was canceled")
	case err != nil:
		slog.Error("logger search failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "search failed")
	default:
		if request.Projection == nil {
			writeJSON(w, http.StatusOK, map[string]any{"events": events, "next_cursor": next})
			return
		}
		projected, err := projectEvents(events, request.Projection)
		if err != nil {
			slog.Error("logger search projection failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "search projection failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"events": projected, "next_cursor": next})
	}
}

func (h *Handler) authorize(r *http.Request, scope auth.Scope) (auth.Identity, error) {
	headers := r.Header.Values("Authorization")
	if len(headers) != 1 || !strings.HasPrefix(headers[0], "Bearer ") {
		return auth.Identity{}, auth.ErrUnauthorized
	}
	return h.verifier.Verify(strings.TrimPrefix(headers[0], "Bearer "), scope, time.Now())
}

func (h *Handler) ingest(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Ingest)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if versions := r.Header.Values(schemaHeader); len(versions) > 1 || (len(versions) == 1 && strings.TrimSpace(versions[0]) != "1") {
		writeError(w, http.StatusConflict, "unsupported log schema")
		return
	}
	release, ok := acquireCapacity(w, h.ingests, h.tenantIngests, identity.DeploymentID)
	if !ok {
		return
	}
	defer release()
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var body io.Reader = r.Body
	switch strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))) {
	case "", "identity":
	case "gzip":
		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid gzip batch")
			return
		}
		defer reader.Close()
		body = http.MaxBytesReader(w, reader, maxBodyBytes)
	default:
		writeError(w, http.StatusUnsupportedMediaType, "unsupported content encoding")
		return
	}
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var request struct {
		Events []store.Event `json:"events"`
	}
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid batch")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid batch")
		return
	}
	if len(request.Events) == 0 || len(request.Events) > 1000 {
		writeError(w, http.StatusBadRequest, "invalid batch size")
		return
	}
	if identity.App != "" {
		for _, event := range request.Events {
			if event.App != identity.App {
				writeError(w, http.StatusForbidden, "credential is restricted to another application")
				return
			}
		}
	}
	policy, err := h.store.IngestionPolicy(identity.DeploymentID)
	if err != nil {
		slog.Error("read ingestion policy failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "ingestion policy unavailable")
		return
	}
	if policy.ServiceEventsPerSecond > 0 && identity.App == "" {
		writeError(w, http.StatusForbidden, "application-bound credential required by service ingestion policy")
		return
	}
	digest := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
	if allowed, retry := h.rates.allow(identity.DeploymentID, digest, identity.App, len(request.Events), policy, time.Now()); !allowed {
		w.Header().Set("Retry-After", strconv.Itoa(retry))
		writeError(w, http.StatusTooManyRequests, "deployment, service or credential ingestion rate exceeded")
		return
	}
	for i := range request.Events {
		event := &request.Events[i]
		event.Message = redact.Text(event.Message)
		event.Source = redact.Text(event.Source)
		event.Code = redact.Text(event.Code)
		event.App = redact.Text(event.App)
		event.InstanceID = redact.Text(event.InstanceID)
		event.HostID = redact.Text(event.HostID)
		event.TraceID = redact.Text(event.TraceID)
		event.SpanID = redact.Text(event.SpanID)
		event.RequestID = redact.Text(event.RequestID)
		event.TaskID = redact.Text(event.TaskID)
		if event.Error != nil {
			event.Error.Type = redact.Text(event.Error.Type)
			event.Error.Code = redact.Text(event.Error.Code)
			event.Error.Stack = redact.Text(event.Error.Stack)
		}
		if event.Fields != nil {
			event.Fields, err = redact.Fields(event.Fields)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid event fields")
				return
			}
		}
	}
	h.liveOrdering.Lock()
	inserted, err := h.store.PutBatchEvents(identity.DeploymentID, request.Events)
	if err != nil {
		h.liveOrdering.Unlock()
		if errors.Is(err, store.ErrCapacityExceeded) {
			w.Header().Set("Retry-After", "60")
			writeError(w, http.StatusServiceUnavailable, "logger storage capacity threshold exceeded")
			return
		}
		if errors.Is(err, store.ErrQuotaExceeded) {
			w.Header().Set("Retry-After", "60")
			writeError(w, http.StatusTooManyRequests, "deployment logical storage quota exceeded")
			return
		}
		if errors.Is(err, store.ErrInvalidInput) {
			writeError(w, http.StatusBadRequest, "invalid event")
			return
		}
		slog.Error("logger batch storage failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "batch could not be stored")
		return
	}
	if err := h.live.publish(inserted); err != nil {
		slog.Error("logger live publication failed", "error", err)
	}
	h.liveOrdering.Unlock()
	writeJSON(w, http.StatusOK, map[string]int{"accepted": len(request.Events), "inserted": len(inserted)})
}

func (h *Handler) query(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authorize(r, auth.Query)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	release, ok := acquireCapacity(w, h.searches, h.tenantSearches, identity.DeploymentID)
	if !ok {
		return
	}
	defer release()
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 1000 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
	}
	field, value, cursor := r.URL.Query().Get("field"), r.URL.Query().Get("value"), r.URL.Query().Get("cursor")
	if field == "" {
		if value != "" || cursor != "" {
			writeError(w, http.StatusBadRequest, "index field required")
			return
		}
		events, err := h.store.Recent(identity.DeploymentID, limit)
		if err != nil {
			slog.Error("logger recent query failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "query failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"events": events})
		return
	}
	events, next, err := h.store.QueryIndex(identity.DeploymentID, field, value, cursor, limit)
	if err != nil {
		if errors.Is(err, store.ErrInvalidInput) {
			writeError(w, http.StatusBadRequest, "invalid index query")
			return
		}
		slog.Error("logger indexed query failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events, "next_cursor": next})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("logger response failed", "error", err)
	}
}
