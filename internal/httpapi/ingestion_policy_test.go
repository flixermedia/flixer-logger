package httpapi

import (
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestIngestionPolicyRequiresManageAndIsTenantScoped(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	manager, query := strings.Repeat("m", 32), strings.Repeat("q", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(manager)), DeploymentID: "one", Scope: auth.Manage},
		{Digest: sha256.Sum256([]byte(query)), DeploymentID: "one", Scope: auth.Query},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	send := func(method, token, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/api/logger/ingestion-policy", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := send(http.MethodGet, query, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("query credential accepted: %d", rec.Code)
	}
	if rec := send(http.MethodPut, manager, `{"events_per_second":500,"burst":1000}`); rec.Code != http.StatusOK {
		t.Fatalf("policy rejected: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(http.MethodPut, manager, `{"events_per_second":500,"burst":1000,"credential_events_per_second":10,"credential_burst":1000,"service_events_per_second":20,"service_burst":1000}`); rec.Code != http.StatusOK {
		t.Fatalf("credential policy rejected: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(http.MethodGet, manager, ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"credential_events_per_second":10`) {
		t.Fatalf("credential policy missing: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send(http.MethodPut, manager, `{"events_per_second":500,"burst":1000,"credential_events_per_second":10}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("incomplete credential policy accepted: %d", rec.Code)
	}
	if rec := send(http.MethodPut, manager, `{"events_per_second":500,"burst":1000,"service_burst":1000}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("incomplete service policy accepted: %d", rec.Code)
	}
	if rec := send(http.MethodPut, manager, `{"events_per_second":500,"burst":1}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid policy accepted: %d", rec.Code)
	}
	other, err := storage.IngestionPolicy("other")
	if err != nil || other.EventsPerSecond != 2000 {
		t.Fatalf("policy leaked: %+v %v", other, err)
	}
}
