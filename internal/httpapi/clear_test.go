package httpapi

import (
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestClearLogsRequiresManagementAndIsDeploymentScoped(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	manager, query := strings.Repeat("m", 32), strings.Repeat("q", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(manager)), DeploymentID: "a", Scope: auth.Manage},
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
		SchemaVersion: 1, EventID: "same", Time: time.Now(), Level: "info",
		InstanceID: "one", App: "api", Message: "ready",
	}
	if _, err := storage.PutBatch("a", []store.Event{event}); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.PutBatch("b", []store.Event{event}); err != nil {
		t.Fatal(err)
	}
	send := func(token string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/api/logs/clear", nil)
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	for _, token := range []string{"", query} {
		if response := send(token); response.Code != http.StatusUnauthorized {
			t.Fatalf("unauthorized clear status: %d", response.Code)
		}
	}
	if response := send(manager); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"cleared":1`) {
		t.Fatalf("clear response: %d %s", response.Code, response.Body.String())
	}
	if events, err := storage.Recent("a", 10); err != nil || len(events) != 0 {
		t.Fatalf("deployment a history: %+v, %v", events, err)
	}
	if events, err := storage.Recent("b", 10); err != nil || len(events) != 1 {
		t.Fatalf("deployment b history: %+v, %v", events, err)
	}
}
