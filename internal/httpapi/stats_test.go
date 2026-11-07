package httpapi

import (
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestStatsIsolationAndBounds(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	query, other := strings.Repeat("q", 32), strings.Repeat("o", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(query)), DeploymentID: "a", Scope: auth.Query},
		{Digest: sha256.Sum256([]byte(other)), DeploymentID: "b", Scope: auth.Query},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	event := store.Event{SchemaVersion: 1, EventID: "one", Time: now, Level: "error", App: "api", InstanceID: "node", Message: "failed"}
	if _, err := storage.PutBatch("a", []store.Event{event}); err != nil {
		t.Fatal(err)
	}
	start := now.Truncate(time.Minute)
	params := url.Values{"start": {start.Format(time.RFC3339)}, "end": {start.Add(2 * time.Minute).Format(time.RFC3339)}}
	for _, tc := range []struct {
		token  string
		status int
		has    bool
	}{
		{query, http.StatusOK, true},
		{other, http.StatusOK, false},
		{"", http.StatusUnauthorized, false},
	} {
		req := httptest.NewRequest(http.MethodGet, "/api/logs/stats?"+params.Encode(), nil)
		if tc.token != "" {
			req.Header.Set("Authorization", "Bearer "+tc.token)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != tc.status || strings.Contains(rec.Body.String(), `"error":1`) != tc.has {
			t.Fatalf("stats response: %d %s", rec.Code, rec.Body.String())
		}
	}
	params.Set("start", now.Format(time.RFC3339Nano))
	req := httptest.NewRequest(http.MethodGet, "/api/logs/stats?"+params.Encode(), nil)
	req.Header.Set("Authorization", "Bearer "+query)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unaligned bounds: %d %s", rec.Code, rec.Body.String())
	}
}

func TestIngestionOverloadIsExplicit(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	token := strings.Repeat("i", 32)
	verifier, err := auth.New([]auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "a", Scope: auth.Ingest}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	for range cap(handler.ingests) {
		handler.ingests <- struct{}{}
	}
	req := httptest.NewRequest(http.MethodPost, "/api/logs/batch", strings.NewReader(`{"events":[]}`))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("overload response: %d %s", rec.Code, rec.Body.String())
	}
}
