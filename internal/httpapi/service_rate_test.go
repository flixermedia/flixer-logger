package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestServiceRateRequiresBindingAndSharesLimitAcrossCredentials(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	policy := store.IngestionPolicy{EventsPerSecond: 1, Burst: 3000, ServiceEventsPerSecond: 1, ServiceBurst: 1000}
	if err := storage.SetIngestionPolicy("one", policy); err != nil {
		t.Fatal(err)
	}
	a, b, c, legacy := strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32), strings.Repeat("l", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(a)), DeploymentID: "one", Scope: auth.Ingest, App: "flixer"},
		{Digest: sha256.Sum256([]byte(b)), DeploymentID: "one", Scope: auth.Ingest, App: "flixer"},
		{Digest: sha256.Sum256([]byte(c)), DeploymentID: "one", Scope: auth.Ingest, App: "flixer-indexer"},
		{Digest: sha256.Sum256([]byte(legacy)), DeploymentID: "one", Scope: auth.Ingest},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	send := func(token, app string) *httptest.ResponseRecorder {
		event := store.Event{SchemaVersion: 1, EventID: "id", Time: time.Now(), Level: "info", InstanceID: "node", App: app, Message: "ready"}
		events := make([]store.Event, 1000)
		for i := range events {
			events[i] = event
		}
		body, err := json.Marshal(struct {
			Events []store.Event `json:"events"`
		}{events})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/logs/batch", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := send(legacy, "flixer"); rec.Code != http.StatusForbidden {
		t.Fatalf("unbound credential accepted: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(a, "flixer"); rec.Code != http.StatusOK {
		t.Fatalf("first service burst rejected: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(b, "flixer"); rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("second credential bypassed service limit: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(c, "flixer-indexer"); rec.Code != http.StatusOK {
		t.Fatalf("other service starved or denied batch consumed deployment tokens: %d %s", rec.Code, rec.Body.String())
	}
}
