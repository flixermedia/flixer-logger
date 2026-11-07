package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestOwnerCapacityDoesNotGrantTenantAccess(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	t.Run("owner policy controls ingestion", func(t *testing.T) {
		storage, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer storage.Close()
		owner, manage, ingest, query := strings.Repeat("o", 32), strings.Repeat("m", 32), strings.Repeat("i", 32), strings.Repeat("q", 32)
		verifier, err := auth.New([]auth.Credential{
			{Digest: sha256.Sum256([]byte(owner)), DeploymentID: "owner", Scope: auth.Owner},
			{Digest: sha256.Sum256([]byte(manage)), DeploymentID: "tenant", Scope: auth.Manage},
			{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "tenant", Scope: auth.Ingest},
			{Digest: sha256.Sum256([]byte(query)), DeploymentID: "tenant", Scope: auth.Query},
		})
		if err != nil {
			t.Fatal(err)
		}
		handler, err := New(storage, verifier)
		if err != nil {
			t.Fatal(err)
		}
		send := func(method, path, token, body string) *httptest.ResponseRecorder {
			t.Helper()
			request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
			request.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			return response
		}
		policy := `{"max_database_bytes":1,"min_free_bytes":0}`
		if response := send(http.MethodPut, "/api/owner/capacity-policy", manage, policy); response.Code != 401 {
			t.Fatalf("tenant updated owner policy: %d", response.Code)
		}
		for _, invalid := range []string{
			`{"max_database_bytes":1}`, `{"max_database_bytes":-1,"min_free_bytes":0}`,
			`{"max_database_bytes":1125899906842625,"min_free_bytes":0}`, `{"max_database_bytes":1,"min_free_bytes":0,"other":true}`,
		} {
			if response := send(http.MethodPut, "/api/owner/capacity-policy", owner, invalid); response.Code != 400 {
				t.Fatalf("invalid policy accepted: %d", response.Code)
			}
		}
		if response := send(http.MethodPut, "/api/owner/capacity-policy", owner, policy); response.Code != 200 {
			t.Fatalf("policy update failed: %d %s", response.Code, response.Body.String())
		}
		if response := send(http.MethodGet, "/api/owner/capacity-policy", owner, ""); response.Code != 200 || !strings.Contains(response.Body.String(), `"max_database_bytes":1`) {
			t.Fatalf("policy read failed: %d %s", response.Code, response.Body.String())
		}
		event := `{"events":[{"schema_version":1,"event_id":"one","time":"2026-01-01T00:00:00Z","level":"info","instance_id":"node","app":"flixer","message":"ready"}]}`
		response := send(http.MethodPost, "/api/logs/batch", ingest, event)
		if response.Code != 503 || response.Header().Get("Retry-After") == "" {
			t.Fatalf("capacity rejection failed: %d %s", response.Code, response.Body.String())
		}
		if response := send(http.MethodGet, "/api/logs", query, ""); response.Code != 200 || strings.Contains(response.Body.String(), `"event_id"`) {
			t.Fatalf("rejected event stored: %d %s", response.Code, response.Body.String())
		}
		if response := send(http.MethodPut, "/api/owner/capacity-policy", owner, `{"max_database_bytes":0,"min_free_bytes":0}`); response.Code != 200 {
			t.Fatalf("policy disable failed: %d", response.Code)
		}
		if response := send(http.MethodPost, "/api/logs/batch", ingest, event); response.Code != 200 {
			t.Fatalf("ingest did not recover: %d %s", response.Code, response.Body.String())
		}
	})
	defer storage.Close()
	owner, manage, query, ingest := strings.Repeat("o", 32), strings.Repeat("m", 32), strings.Repeat("q", 32), strings.Repeat("i", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(owner)), DeploymentID: "owner", Scope: auth.Owner},
		{Digest: sha256.Sum256([]byte(manage)), DeploymentID: "tenant", Scope: auth.Manage},
		{Digest: sha256.Sum256([]byte(query)), DeploymentID: "tenant", Scope: auth.Query},
		{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "tenant", Scope: auth.Ingest},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		token string
		path  string
		code  int
	}{
		{owner, "/api/owner/capacity", 200},
		{manage, "/api/owner/capacity", 401},
		{query, "/api/owner/capacity", 401},
		{ingest, "/api/owner/capacity", 401},
		{"", "/api/owner/capacity", 401},
		{owner, "/api/status", 401},
		{owner, "/api/logs", 401},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		req.Header.Set("Authorization", "Bearer "+tc.token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != tc.code {
			t.Fatalf("%s: got %d want %d", tc.path, rec.Code, tc.code)
		}
		if tc.code == 200 {
			var capacity store.Capacity
			if err := json.Unmarshal(rec.Body.Bytes(), &capacity); err != nil || capacity.FilesystemTotalBytes == 0 ||
				capacity.FilesystemAvailableBytes > capacity.FilesystemTotalBytes || capacity.DatabaseBytes == 0 {
				t.Fatalf("invalid owner capacity: %+v %v", capacity, err)
			}
		}
	}
}
