package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestTextIndexUsesRedactedMessage(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	ingest, query, other := strings.Repeat("i", 32), strings.Repeat("q", 32), strings.Repeat("z", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "a", Scope: auth.Ingest},
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
	tokens := make([]string, 200)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("token%d", i)
	}
	longToken := strings.Repeat("x", 200)
	event := store.Event{SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "error", App: "api", InstanceID: "node", Message: "playback failed https://example.com " + strings.Join(tokens, " ") + " " + longToken}
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
		token, term string
		has         bool
	}{
		{query, "playback", true},
		{query, "token199", true},
		{query, longToken, true},
		{query, "example", false},
		{other, "playback", false},
		{other, longToken, false},
	} {
		request = httptest.NewRequest(http.MethodGet, "/api/logs?field=text&value="+tc.term, nil)
		request.Header.Set("Authorization", "Bearer "+tc.token)
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"event_id":"one"`) != tc.has {
			t.Fatalf("text query %s: %d %s", tc.term, response.Code, response.Body.String())
		}
	}
}
