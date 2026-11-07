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

func TestIngestionAndQueryIsolation(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	defer storage.Close()
	ingest := strings.Repeat("i", 32)
	viewA := strings.Repeat("a", 32)
	viewB := strings.Repeat("b", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "a", Scope: auth.Ingest},
		{Digest: sha256.Sum256([]byte(viewA)), DeploymentID: "a", Scope: auth.Query},
		{Digest: sha256.Sum256([]byte(viewB)), DeploymentID: "b", Scope: auth.Query},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	event := store.Event{
		SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "error", App: "api",
		InstanceID: "node", DeploymentID: "b", Message: "password=secret https://example.com",
		Fields: map[string]any{"api_key": "do-not-store"},
	}
	body, err := json.Marshal(map[string]any{"events": []store.Event{event}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/logs/batch", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+ingest)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("ingest: %d %s", response.Code, response.Body.String())
	}
	for _, tc := range []struct {
		token string
		want  int
		has   bool
	}{
		{viewA, http.StatusOK, true},
		{viewB, http.StatusOK, false},
		{ingest, http.StatusUnauthorized, false},
		{"", http.StatusUnauthorized, false},
	} {
		request = httptest.NewRequest(http.MethodGet, "/api/logs", nil)
		if tc.token != "" {
			request.Header.Set("Authorization", "Bearer "+tc.token)
		}
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != tc.want {
			t.Fatalf("query: got %d want %d", response.Code, tc.want)
		}
		text := response.Body.String()
		if strings.Contains(text, `"event_id":"one"`) != tc.has ||
			strings.Contains(text, "do-not-store") || strings.Contains(text, "example.com") ||
			strings.Contains(text, "password=secret") {
			t.Fatalf("isolation or redaction failed: %s", text)
		}
	}
}

func TestRejectsInvalidBatchesAndQueries(t *testing.T) {
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
	for _, tc := range []struct {
		method, path, body, token string
	}{
		{"POST", "/api/logs/batch", `{"events":[]}`, ingest},
		{"POST", "/api/logs/batch", `{"events":[]} {}`, ingest},
		{"POST", "/api/logs/batch", `{"events":[{}]}`, ingest},
		{"POST", "/api/logs/batch", `{"events":[],"unexpected":true}`, ingest},
		{"GET", "/api/logs?limit=1001", "", query},
		{"GET", "/api/logs?field=unknown&value=x", "", query},
		{"GET", "/api/logs?cursor=x", "", query},
	} {
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		request.Header.Set("Authorization", "Bearer "+tc.token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, response.Code, response.Body.String())
		}
	}
}
