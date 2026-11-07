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

func TestSearchProjection(t *testing.T) {
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
	for _, id := range []string{"event-one", "event-two"} {
		_, err := storage.PutBatch("a", []store.Event{{
			SchemaVersion: 1, EventID: id, Time: now, Level: "error", App: "api",
			InstanceID: "node", Message: "message", Fields: map[string]any{"count": 5},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	body := map[string]any{
		"start": now.Add(-time.Minute), "end": now.Add(time.Minute),
		"limit": 1, "projection": []string{"message", "fields", "source"},
	}
	send := func(token string) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/api/logs/search", bytes.NewReader(data))
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder
	}
	first := send(token)
	if first.Code != http.StatusOK {
		t.Fatalf("search failed: %d %s", first.Code, first.Body.String())
	}
	var result struct {
		Events []map[string]any `json:"events"`
		Cursor string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 1 || len(result.Events[0]) != 3 || result.Events[0]["event_id"] == nil ||
		result.Events[0]["message"] != "message" || result.Events[0]["fields"].(map[string]any)["count"] != float64(5) ||
		result.Cursor == "" {
		t.Fatalf("unexpected projected page: %+v", result)
	}
	body["cursor"] = result.Cursor
	body["projection"] = []string{}
	second := send(token)
	result.Events = nil
	if err := json.Unmarshal(second.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if second.Code != http.StatusOK || len(result.Events) != 1 || len(result.Events[0]) != 1 || result.Events[0]["event_id"] == nil {
		t.Fatalf("unexpected second page: %d %s", second.Code, second.Body.String())
	}
	if response := send(other); response.Code != http.StatusBadRequest {
		t.Fatalf("cross-tenant cursor accepted: %d", response.Code)
	}
	delete(body, "cursor")
	for _, invalid := range [][]string{{"unsupported"}, {"message", "message"}} {
		body["projection"] = invalid
		if response := send(token); response.Code != http.StatusBadRequest {
			t.Fatalf("invalid projection accepted: %d", response.Code)
		}
	}
	body["projection"] = []string{"message"}
	if response := send(other); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"events":[]`) {
		t.Fatalf("cross-tenant data visible: %d %s", response.Code, response.Body.String())
	}
	delete(body, "projection")
	if response := send(token); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"schema_version":1`) {
		t.Fatalf("default full event changed: %d %s", response.Code, response.Body.String())
	}
}
