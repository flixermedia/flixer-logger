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

func TestErrorDetailsAreRedactedAndIndexed(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	ingest, query := strings.Repeat("i", 32), strings.Repeat("q", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "a", Scope: auth.Ingest},
		{Digest: sha256.Sum256([]byte(query)), DeploymentID: "a", Scope: auth.Query},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	event := store.Event{
		SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "error", App: "api", InstanceID: "node",
		Message: "request failed", Error: &store.ErrorDetails{Type: "UpstreamError", Code: "HTTP_503", Stack: "timeout at https://private.example/token"},
	}
	body, err := json.Marshal(map[string]any{"events": []store.Event{event}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/logs/batch", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+ingest)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ingest: %d %s", rec.Code, rec.Body.String())
	}
	for _, tc := range []struct {
		term string
		has  bool
	}{{"timeout", true}, {"upstreamerror", true}, {"private", false}} {
		req = httptest.NewRequest(http.MethodGet, "/api/logs?field=text&value="+tc.term, nil)
		req.Header.Set("Authorization", "Bearer "+query)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"event_id":"one"`) != tc.has ||
			strings.Contains(rec.Body.String(), "private.example") {
			t.Fatalf("search %s: %d %s", tc.term, rec.Code, rec.Body.String())
		}
	}
	if n, err := storage.DeleteBefore("a", time.Now().Add(time.Second), 10); err != nil || n != 1 {
		t.Fatalf("retention: %d %v", n, err)
	}
	events, _, err := storage.QueryIndex("a", "text", "timeout", "", 10)
	if err != nil || len(events) != 0 {
		t.Fatalf("retention left error postings: %+v %v", events, err)
	}
}
