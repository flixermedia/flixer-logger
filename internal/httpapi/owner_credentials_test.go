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

func TestOwnerManagementCredentialLifecycle(t *testing.T) {
	dir := t.TempDir()
	storage, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	owner, tenant := strings.Repeat("o", 32), strings.Repeat("m", 32)
	base := []auth.Credential{
		{Digest: sha256.Sum256([]byte(owner)), DeploymentID: "owner", Scope: auth.Owner},
		{Digest: sha256.Sum256([]byte(tenant)), DeploymentID: "tenant", Scope: auth.Manage},
	}
	credentials, err := registry.New(storage, base)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewWithRegistry(storage, credentials)
	if err != nil {
		t.Fatal(err)
	}
	send := func(method, path, token, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	expiry := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	body := fmt.Sprintf(`{"deployment_id":"tenant","expires_at":%q}`, expiry)
	if response := send(http.MethodPost, "/api/owner/credentials", tenant, body); response.Code != http.StatusUnauthorized {
		t.Fatalf("tenant issued management credential: %d", response.Code)
	}
	for _, invalid := range []string{
		`{"deployment_id":"","expires_at":"2099-01-01T00:00:00Z"}`,
		`{"deployment_id":"tenant","expires_at":"2099-01-01T00:00:00Z"}`,
		fmt.Sprintf(`{"deployment_id":"tenant","expires_at":%q,"scope":"owner"}`, expiry),
	} {
		if response := send(http.MethodPost, "/api/owner/credentials", owner, invalid); response.Code != http.StatusBadRequest {
			t.Fatalf("invalid request accepted: %d", response.Code)
		}
	}
	response := send(http.MethodPost, "/api/owner/credentials", owner, body)
	if response.Code != http.StatusCreated {
		t.Fatalf("management issuance failed: %d %s", response.Code, response.Body.String())
	}
	var issued struct {
		Credential credentialInfo `json:"credential"`
		Token      string         `json:"token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &issued); err != nil {
		t.Fatal(err)
	}
	if issued.Credential.Scope != auth.Manage || len(issued.Token) < 32 || issued.Credential.ID == "" {
		t.Fatal("invalid issued management credential")
	}
	if response := send(http.MethodGet, "/api/logger/retention", issued.Token, ""); response.Code != http.StatusOK {
		t.Fatalf("issued management credential rejected: %d", response.Code)
	}
	if response := send(http.MethodGet, "/api/owner/capacity", issued.Token, ""); response.Code != http.StatusUnauthorized {
		t.Fatalf("management credential granted owner access: %d", response.Code)
	}
	list := send(http.MethodGet, "/api/owner/credentials?deployment_id=tenant", owner, "")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), issued.Credential.ID) ||
		strings.Contains(list.Body.String(), issued.Token) || strings.Contains(list.Body.String(), "digest") {
		t.Fatalf("invalid owner listing: %d %s", list.Code, list.Body.String())
	}
	if response := send(http.MethodGet, "/api/owner/credentials?deployment_id=other", owner, ""); strings.Contains(response.Body.String(), issued.Credential.ID) {
		t.Fatal("credential leaked into another deployment")
	}
	if response := send(http.MethodGet, "/api/logger/credentials", issued.Token, ""); strings.Contains(response.Body.String(), issued.Credential.ID) {
		t.Fatal("management credential visible in tenant list")
	}
	if response := send(http.MethodDelete, "/api/logger/credentials/"+issued.Credential.ID, issued.Token, ""); response.Code != http.StatusNotFound {
		t.Fatalf("tenant revoked owner-issued management credential: %d", response.Code)
	}
	if response := send(http.MethodDelete, "/api/owner/credentials/"+issued.Credential.ID+"?deployment_id=other", owner, ""); response.Code != http.StatusNotFound {
		t.Fatalf("cross-deployment revocation succeeded: %d", response.Code)
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
	if response := send(http.MethodGet, "/api/logger/retention", issued.Token, ""); response.Code != http.StatusOK {
		t.Fatalf("issued credential lost on restart: %d", response.Code)
	}
	if response := send(http.MethodDelete, "/api/owner/credentials/"+issued.Credential.ID+"?deployment_id=tenant", owner, ""); response.Code != http.StatusNoContent {
		t.Fatalf("owner revocation failed: %d", response.Code)
	}
	if response := send(http.MethodGet, "/api/logger/retention", issued.Token, ""); response.Code != http.StatusUnauthorized {
		t.Fatalf("revoked credential remains active: %d", response.Code)
	}
}
