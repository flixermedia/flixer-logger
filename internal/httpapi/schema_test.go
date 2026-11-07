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

func TestIngestionSchemaNegotiation(t *testing.T) {
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
	event := store.Event{SchemaVersion: 1, EventID: "schema-test", Time: time.Now(), Level: "info", InstanceID: "node", App: "api", Message: "ready"}
	body, err := json.Marshal(map[string]any{"events": []store.Event{event}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		versions []string
		status   int
	}{
		{[]string{"2"}, http.StatusConflict},
		{[]string{"1", "1"}, http.StatusConflict},
		{[]string{"1, 2"}, http.StatusConflict},
		{nil, http.StatusOK},
		{[]string{"1"}, http.StatusOK},
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/logs/batch", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		for _, version := range test.versions {
			req.Header.Add(schemaHeader, version)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != test.status || rec.Header().Get(schemaHeader) != "1" {
			t.Fatalf("versions %v: status %d, headers %v", test.versions, rec.Code, rec.Header())
		}
	}
	events, err := storage.Recent("a", 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("invalid versions must not insert events; compatible retry must deduplicate: %d %v", len(events), err)
	}
}
