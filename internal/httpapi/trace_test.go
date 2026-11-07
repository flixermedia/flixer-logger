package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestTraceEndpointPaginationAndTenantIsolation(t *testing.T) {
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
	event := store.Event{SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "error", App: "api", InstanceID: "node", Message: "failed", TraceID: "trace-1"}
	if _, err := storage.PutBatch("a", []store.Event{event}); err != nil {
		t.Fatal(err)
	}
	event.EventID = "two"
	if _, err := storage.PutBatch("a", []store.Event{event}); err != nil {
		t.Fatal(err)
	}
	send := func(token, path string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}
	first := send(query, "/api/logs/trace/trace-1?limit=1")
	var page struct {
		Events []store.Event `json:"events"`
		Next   string        `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || first.Code != http.StatusOK || len(page.Events) != 1 || page.Events[0].EventID != "two" || page.Next == "" {
		t.Fatalf("first trace page: %d %s %v", first.Code, first.Body.String(), err)
	}
	path := "/api/logs/trace/trace-1?limit=1&cursor=" + url.QueryEscape(page.Next)
	second := send(query, path)
	if err := json.Unmarshal(second.Body.Bytes(), &page); err != nil || second.Code != http.StatusOK || len(page.Events) != 1 || page.Events[0].EventID != "one" || page.Next != "" {
		t.Fatalf("second trace page: %d %s %v", second.Code, second.Body.String(), err)
	}
	isolated := send(other, "/api/logs/trace/trace-1")
	if isolated.Code != http.StatusOK || strings.Contains(isolated.Body.String(), `"event_id"`) {
		t.Fatalf("tenant isolation: %d %s", isolated.Code, isolated.Body.String())
	}
	if response := send(other, path); response.Code != http.StatusBadRequest {
		t.Fatalf("cross-tenant cursor: %d", response.Code)
	}
	if response := send("", "/api/logs/trace/trace-1"); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized: %d", response.Code)
	}
}
