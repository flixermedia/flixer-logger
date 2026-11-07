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

func TestRegexSearchHTTP(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	token, other := strings.Repeat("a", 32), strings.Repeat("b", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(token)), DeploymentID: "one", Scope: auth.Query},
		{Digest: sha256.Sum256([]byte(other)), DeploymentID: "two", Scope: auth.Query},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	event := store.Event{
		SchemaVersion: 1, EventID: "match", Time: now, Level: "error", App: "api",
		InstanceID: "node", Message: "failed 503",
	}
	if _, err := storage.PutBatch("one", []store.Event{event}); err != nil {
		t.Fatal(err)
	}
	send := func(token, pattern string) *httptest.ResponseRecorder {
		t.Helper()
		body, err := json.Marshal(store.SearchRequest{
			Start: now.Add(-time.Minute), End: now.Add(time.Minute),
			Text: pattern, TextMode: "regex", Limit: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/logs/search", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := send(token, `failed 5[0-9]{2}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"event_id":"match"`) {
		t.Fatalf("regex search: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(other, `failed 5[0-9]{2}`); rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"event_id":"match"`) {
		t.Fatalf("cross-tenant search: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(token, "["); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid regex: %d %s", rec.Code, rec.Body.String())
	}
}
