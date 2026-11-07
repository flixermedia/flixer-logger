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

func TestSourceStatsAuthorizationAndIsolation(t *testing.T) {
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
	start := time.Now().UTC().Truncate(time.Minute)
	event := store.Event{SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "info", InstanceID: "node", App: "api", Source: "scanner", Message: "ready"}
	if _, err := storage.PutBatch("a", []store.Event{event}); err != nil {
		t.Fatal(err)
	}
	params := url.Values{"start": {start.Format(time.RFC3339)}, "end": {start.Add(2 * time.Minute).Format(time.RFC3339)}}
	send := func(token, queryString string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, "/api/logs/sources?"+queryString, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	if response := send("", params.Encode()); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized response: %d", response.Code)
	}
	if response := send(query, "start=invalid"); response.Code != http.StatusBadRequest {
		t.Fatalf("invalid range response: %d", response.Code)
	}
	for _, tc := range []struct {
		token string
		count int
	}{{query, 1}, {other, 0}} {
		response := send(tc.token, params.Encode())
		var payload struct {
			Sources []store.SourceCount `json:"sources"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || response.Code != http.StatusOK || len(payload.Sources) != tc.count {
			t.Fatalf("response: %d %s %v", response.Code, response.Body.String(), err)
		}
		if tc.count == 1 && payload.Sources[0] != (store.SourceCount{App: "api", Source: "scanner", Count: 1}) {
			t.Fatalf("incorrect source count: %+v", payload.Sources)
		}
	}
}
