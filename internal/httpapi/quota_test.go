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

func TestQuotaManagementAndIngestionIsolation(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	manager, ingest, other := strings.Repeat("m", 32), strings.Repeat("i", 32), strings.Repeat("o", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(manager)), DeploymentID: "a", Scope: auth.Manage},
		{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "a", Scope: auth.Ingest},
		{Digest: sha256.Sum256([]byte(other)), DeploymentID: "b", Scope: auth.Ingest},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	send := func(method, path, token string, body []byte) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := send(http.MethodPut, "/api/logger/quota", ingest, []byte(`{"max_bytes":1}`)); rec.Code != http.StatusUnauthorized {
		t.Fatalf("ingest token managed quota: %d", rec.Code)
	}
	if rec := send(http.MethodPut, "/api/logger/quota", manager, []byte(`{}`)); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing quota: %d", rec.Code)
	}
	if rec := send(http.MethodPut, "/api/logger/quota", manager, []byte(`{"max_bytes":1}`)); rec.Code != http.StatusOK {
		t.Fatalf("set quota: %d %s", rec.Code, rec.Body.String())
	}
	event := store.Event{SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "info", InstanceID: "node", App: "api", Message: "ready"}
	body, err := json.Marshal(map[string]any{"events": []store.Event{event}})
	if err != nil {
		t.Fatal(err)
	}
	if rec := send(http.MethodPost, "/api/logs/batch", ingest, body); rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("quota rejection: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(http.MethodPost, "/api/logs/batch", other, body); rec.Code != http.StatusOK {
		t.Fatalf("other deployment: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(http.MethodPut, "/api/logger/quota", manager, []byte(`{"max_bytes":0}`)); rec.Code != http.StatusOK {
		t.Fatalf("disable quota: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(http.MethodPost, "/api/logs/batch", ingest, body); rec.Code != http.StatusOK {
		t.Fatalf("ingest after disabling quota: %d %s", rec.Code, rec.Body.String())
	}
	rec := send(http.MethodGet, "/api/logger/quota", manager, nil)
	var result struct {
		MaxBytes  uint64 `json:"max_bytes"`
		UsedBytes uint64 `json:"used_bytes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil || rec.Code != http.StatusOK || result.MaxBytes != 0 || result.UsedBytes == 0 {
		t.Fatalf("quota read: %d %s %v", rec.Code, rec.Body.String(), err)
	}
}
