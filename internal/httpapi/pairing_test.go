package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/registry"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestPairingLifecycleAndConcurrentRedemption(t *testing.T) {
	dir := t.TempDir()
	storage, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close() }()
	owner, manager := strings.Repeat("o", 32), strings.Repeat("m", 32)
	base := []auth.Credential{
		{Digest: sha256.Sum256([]byte(owner)), DeploymentID: "owner", Scope: auth.Owner},
		{Digest: sha256.Sum256([]byte(manager)), DeploymentID: "tenant", Scope: auth.Manage},
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
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder
	}
	now := time.Now()
	body := fmt.Sprintf(`{"deployment_id":"tenant","expires_at":%q,"management_expiry":%q}`,
		now.Add(5*time.Minute).UTC().Format(time.RFC3339), now.Add(time.Hour).UTC().Format(time.RFC3339))
	if response := send(http.MethodPost, "/api/owner/pairings", manager, body); response.Code != http.StatusUnauthorized {
		t.Fatalf("tenant created pairing: %d", response.Code)
	}
	response := send(http.MethodPost, "/api/owner/pairings", owner, body)
	if response.Code != http.StatusCreated {
		t.Fatalf("create pairing failed: %d %s", response.Code, response.Body.String())
	}
	var issued struct {
		Pairing pairingInfo `json:"pairing"`
		Token   string      `json:"token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &issued); err != nil {
		t.Fatal(err)
	}
	if len(issued.Token) != 43 || issued.Pairing.ID == "" {
		t.Fatal("invalid pairing")
	}
	list := send(http.MethodGet, "/api/owner/pairings?deployment_id=tenant", owner, "")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), issued.Pairing.ID) ||
		strings.Contains(list.Body.String(), issued.Token) || strings.Contains(list.Body.String(), "digest") {
		t.Fatalf("invalid pairing listing: %d %s", list.Code, list.Body.String())
	}
	if response := send(http.MethodDelete, "/api/owner/pairings/"+issued.Pairing.ID+"?deployment_id=other", owner, ""); response.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant pairing revocation: %d", response.Code)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err = store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err = registry.New(storage, base)
	if err != nil {
		t.Fatal(err)
	}
	handler, err = NewWithRegistry(storage, credentials)
	if err != nil {
		t.Fatal(err)
	}
	const attempts = 4
	results := make(chan *httptest.ResponseRecorder, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- send(http.MethodPost, "/api/logger/pairings/redeem", "", fmt.Sprintf(`{"token":%q}`, issued.Token))
		}()
	}
	wg.Wait()
	close(results)
	success := 0
	var managementToken string
	for result := range results {
		switch result.Code {
		case http.StatusCreated:
			success++
			var payload struct {
				DeploymentID string         `json:"deployment_id"`
				Credential   credentialInfo `json:"credential"`
				Token        string         `json:"token"`
			}
			if err := json.Unmarshal(result.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.DeploymentID != "tenant" || payload.Credential.Scope != auth.Manage {
				t.Fatal("incorrect pairing identity")
			}
			managementToken = payload.Token
		case http.StatusUnauthorized:
		default:
			t.Fatalf("unexpected redeem response: %d %s", result.Code, result.Body.String())
		}
	}
	if success != 1 || managementToken == "" {
		t.Fatalf("expected one successful redemption, got %d", success)
	}
	if response := send(http.MethodGet, "/api/logger/retention", managementToken, ""); response.Code != http.StatusOK {
		t.Fatalf("redeemed management credential rejected: %d", response.Code)
	}
	if response := send(http.MethodPost, "/api/logger/pairings/redeem", "", fmt.Sprintf(`{"token":%q}`, issued.Token)); response.Code != http.StatusUnauthorized {
		t.Fatalf("pairing reused: %d", response.Code)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err = store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err = registry.New(storage, base)
	if err != nil {
		t.Fatal(err)
	}
	handler, err = NewWithRegistry(storage, credentials)
	if err != nil {
		t.Fatal(err)
	}
	if response := send(http.MethodGet, "/api/logger/retention", managementToken, ""); response.Code != http.StatusOK {
		t.Fatalf("redeemed credential lost on restart: %d", response.Code)
	}
	if response := send(http.MethodPost, "/api/logger/pairings/redeem", "", fmt.Sprintf(`{"token":%q}`, issued.Token)); response.Code != http.StatusUnauthorized {
		t.Fatalf("consumed pairing reused after restart: %d", response.Code)
	}
}
