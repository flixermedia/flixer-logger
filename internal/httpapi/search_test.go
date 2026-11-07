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

func TestSearchEndpointAuthorizationAndFiltering(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	token, other := strings.Repeat("q", 32), strings.Repeat("o", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(token)), DeploymentID: "a", Scope: auth.Query},
		{Digest: sha256.Sum256([]byte(other)), DeploymentID: "b", Scope: auth.Query},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	event := store.Event{SchemaVersion: 1, EventID: "event", Time: now, Level: "error", App: "api", InstanceID: "node", Message: "playback failed"}
	if _, err := storage.PutBatch("a", []store.Event{event}); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(store.SearchRequest{Start: now.Add(-time.Minute), End: now.Add(time.Minute), Fields: map[string]string{"level": "error"}, Text: "playback failed", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		token  string
		status int
		has    bool
	}{
		{token, http.StatusOK, true},
		{other, http.StatusOK, false},
		{"", http.StatusUnauthorized, false},
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/logs/search", bytes.NewReader(body))
		if tc.token != "" {
			req.Header.Set("Authorization", "Bearer "+tc.token)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != tc.status || strings.Contains(rec.Body.String(), `"event_id":"event"`) != tc.has {
			t.Fatalf("unexpected search response: %d %s", rec.Code, rec.Body.String())
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/api/logs/search", strings.NewReader(`{"unexpected":true}`))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid request: %d %s", rec.Code, rec.Body.String())
	}
}
