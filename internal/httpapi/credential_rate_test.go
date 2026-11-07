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

func TestCredentialRateIsEnforcedOverHTTP(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	policy := store.IngestionPolicy{EventsPerSecond: 1, Burst: 3000, CredentialEventsPerSecond: 1, CredentialBurst: 1000}
	if err := storage.SetIngestionPolicy("one", policy); err != nil {
		t.Fatal(err)
	}
	a, b := strings.Repeat("a", 32), strings.Repeat("b", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(a)), DeploymentID: "one", Scope: auth.Ingest},
		{Digest: sha256.Sum256([]byte(b)), DeploymentID: "one", Scope: auth.Ingest},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	event := store.Event{SchemaVersion: 1, EventID: "id", Time: time.Now(), Level: "info", InstanceID: "node", App: "api", Message: "ready"}
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
	send := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/logs/batch", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := send(a); rec.Code != http.StatusOK {
		t.Fatalf("first credential rejected: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(a); rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("credential limit not enforced: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(b); rec.Code != http.StatusOK {
		t.Fatalf("second credential starved: %d %s", rec.Code, rec.Body.String())
	}
}
