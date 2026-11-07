package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/registry"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestServiceCredentialEnforcesApplicationAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	storage, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { storage.Close() }()
	manager := strings.Repeat("m", 32)
	base := []auth.Credential{{Digest: sha256.Sum256([]byte(manager)), DeploymentID: "a", Scope: auth.Manage}}
	credentials, err := registry.New(storage, base)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewWithRegistry(storage, credentials)
	if err != nil {
		t.Fatal(err)
	}
	send := func(method, path, token, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}
	expiry := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	for _, invalid := range []string{
		fmt.Sprintf(`{"scope":"query","app":"flixer-indexer","expires_at":%q}`, expiry),
		fmt.Sprintf(`{"scope":"ingest","app":"../bad","expires_at":%q}`, expiry),
	} {
		if response := send("POST", "/api/logger/credentials", manager, invalid); response.Code != http.StatusBadRequest {
			t.Fatalf("invalid restriction accepted: %d %s", response.Code, response.Body.String())
		}
	}
	response := send("POST", "/api/logger/credentials", manager, fmt.Sprintf(`{"scope":"ingest","app":"flixer-indexer","expires_at":%q}`, expiry))
	var issued struct {
		Credential credentialInfo `json:"credential"`
		Token      string         `json:"token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &issued); err != nil || response.Code != http.StatusCreated || issued.Credential.App != "flixer-indexer" {
		t.Fatalf("issue credential: %d %s %v", response.Code, response.Body.String(), err)
	}
	event := store.Event{SchemaVersion: 1, EventID: "good", Time: time.Now(), Level: "info", InstanceID: "node", App: "flixer-indexer", Message: "ready"}
	wrong := event
	wrong.EventID, wrong.App = "bad", "flixer-streamer"
	batch := func(events ...store.Event) string {
		data, err := json.Marshal(map[string]any{"events": events})
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	if response := send("POST", "/api/logs/batch", issued.Token, batch(event, wrong)); response.Code != http.StatusForbidden {
		t.Fatalf("mixed application batch accepted: %d %s", response.Code, response.Body.String())
	}
	if events, err := storage.Recent("a", 10); err != nil || len(events) != 0 {
		t.Fatalf("rejected batch stored events: %+v %v", events, err)
	}
	if response := send("POST", "/api/logs/batch", issued.Token, batch(event)); response.Code != http.StatusOK {
		t.Fatalf("authorized application rejected: %d %s", response.Code, response.Body.String())
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	storage = reopened
	credentials, err = registry.New(storage, base)
	if err != nil {
		t.Fatal(err)
	}
	handler, err = NewWithRegistry(storage, credentials)
	if err != nil {
		t.Fatal(err)
	}
	if response := send("POST", "/api/logs/batch", issued.Token, batch(wrong)); response.Code != http.StatusForbidden {
		t.Fatalf("restriction lost after restart: %d %s", response.Code, response.Body.String())
	}
	if response := send("GET", "/api/logger/credentials", manager, ""); !strings.Contains(response.Body.String(), `"app":"flixer-indexer"`) {
		t.Fatalf("restriction missing from credential metadata: %s", response.Body.String())
	}
}
