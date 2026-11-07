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

func TestSearchTaskQueueHTTP(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	token := strings.Repeat("q", 32)
	verifier, err := auth.New([]auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "a", Scope: auth.Query}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, queue := range []string{"scan", "probe"} {
		_, err := storage.PutBatch("a", []store.Event{{
			SchemaVersion: 1, EventID: queue, Time: now, Level: "info", App: "flixer-indexer",
			InstanceID: "node", Message: "ready", TaskID: "same", Fields: map[string]any{"task_queue": queue},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	send := func(fields map[string]string) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(map[string]any{"start": now.Add(-time.Minute), "end": now.Add(time.Minute), "limit": 10, "fields": fields})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/api/logs/search", bytes.NewReader(data))
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	response := send(map[string]string{"task": "same", "task_queue": "scan"})
	var result struct {
		Events []store.Event `json:"events"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || len(result.Events) != 1 || result.Events[0].EventID != "scan" {
		t.Fatalf("unexpected task queue search: %d %s", response.Code, response.Body.String())
	}
	if response := send(map[string]string{"task_queue": "scan"}); response.Code != http.StatusBadRequest {
		t.Fatalf("queue without task accepted: %d %s", response.Code, response.Body.String())
	}
}
