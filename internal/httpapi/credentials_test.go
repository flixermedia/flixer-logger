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

func TestIssuedCredentialsPersistAndAreTenantScoped(t *testing.T) {
	dir := t.TempDir()
	storage, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	manager, other := strings.Repeat("m", 32), strings.Repeat("o", 32)
	base := []auth.Credential{
		{Digest: sha256.Sum256([]byte(manager)), DeploymentID: "a", Scope: auth.Manage},
		{Digest: sha256.Sum256([]byte(other)), DeploymentID: "b", Scope: auth.Manage},
	}
	credentials, err := registry.New(storage, base)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewWithRegistry(storage, credentials)
	if err != nil {
		t.Fatal(err)
	}
	do := func(method, path, token, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	expiry := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	rec := do("POST", "/api/logger/credentials", manager, fmt.Sprintf(`{"scope":"query","expires_at":%q}`, expiry))
	if rec.Code != http.StatusCreated {
		t.Fatalf("issue: %d %s", rec.Code, rec.Body.String())
	}
	var issued struct {
		Credential credentialInfo `json:"credential"`
		Token      string         `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &issued); err != nil {
		t.Fatal(err)
	}
	if len(issued.Token) < 32 || issued.Credential.ID == "" {
		t.Fatal("issued credential is missing its ID or token")
	}
	if rec := do("GET", "/api/logger/credentials", manager, ""); rec.Code != http.StatusOK ||
		strings.Contains(rec.Body.String(), issued.Token) || strings.Contains(rec.Body.String(), "digest") ||
		!strings.Contains(rec.Body.String(), issued.Credential.ID) {
		t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do("GET", "/api/logger/credentials", other, ""); strings.Contains(rec.Body.String(), issued.Credential.ID) {
		t.Fatalf("tenant listing leaked credential: %s", rec.Body.String())
	}
	if rec := do("DELETE", "/api/logger/credentials/"+issued.Credential.ID, other, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant revocation: %d", rec.Code)
	}
	if rec := do("GET", "/api/logs", issued.Token, ""); rec.Code != http.StatusOK {
		t.Fatalf("issued query credential rejected: %d", rec.Code)
	}
	if rec := do("GET", "/api/logger/credentials", issued.Token, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("query credential can manage: %d", rec.Code)
	}
	if rec := do("POST", "/api/logger/credentials", manager, fmt.Sprintf(`{"scope":"manage","expires_at":%q}`, expiry)); rec.Code != http.StatusBadRequest {
		t.Fatalf("issued manage credential: %d", rec.Code)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err = store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	credentials, err = registry.New(storage, base)
	if err != nil {
		t.Fatal(err)
	}
	handler, err = NewWithRegistry(storage, credentials)
	if err != nil {
		t.Fatal(err)
	}
	if rec := do("GET", "/api/logs", issued.Token, ""); rec.Code != http.StatusOK {
		t.Fatalf("credential did not survive restart: %d", rec.Code)
	}
	if rec := do("DELETE", "/api/logger/credentials/"+issued.Credential.ID, manager, ""); rec.Code != http.StatusNoContent {
		t.Fatalf("revoke: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do("GET", "/api/logs", issued.Token, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked credential accepted: %d", rec.Code)
	}
}
