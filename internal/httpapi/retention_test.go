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

func TestRetentionManagementAuthorizationAndValidation(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	manager, other, query := strings.Repeat("m", 32), strings.Repeat("o", 32), strings.Repeat("q", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(manager)), DeploymentID: "a", Scope: auth.Manage},
		{Digest: sha256.Sum256([]byte(other)), DeploymentID: "b", Scope: auth.Manage},
		{Digest: sha256.Sum256([]byte(query)), DeploymentID: "a", Scope: auth.Query},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, token, body string
		status              int
		contains            string
	}{
		{"GET", query, "", http.StatusUnauthorized, ""},
		{"PUT", query, `{"enabled":true,"days":14}`, http.StatusUnauthorized, ""},
		{"PUT", manager, `{"days":14}`, http.StatusBadRequest, ""},
		{"PUT", manager, `{"enabled":true,"days":0}`, http.StatusBadRequest, ""},
		{"PUT", manager, `{"enabled":true,"days":14,"deployment_id":"b"}`, http.StatusBadRequest, ""},
		{"PUT", manager, `{"enabled":true,"days":14} {}`, http.StatusBadRequest, ""},
		{"PUT", manager, `{"enabled":true,"days":14,"severity_days":{"bad":1}}`, http.StatusBadRequest, ""},
		{"PUT", manager, `{"enabled":true,"days":14,"severity_days":{"error":30,"trace":1}}`, http.StatusOK, `"error":30`},
		{"GET", manager, "", http.StatusOK, `"severity_days"`},
		{"PUT", manager, `{"enabled":true,"days":14}`, http.StatusOK, `"enabled":true`},
		{"GET", other, "", http.StatusOK, `"enabled":false`},
		{"GET", manager, "", http.StatusOK, `"days":14`},
	} {
		request := httptest.NewRequest(tc.method, "/api/logger/retention", strings.NewReader(tc.body))
		request.Header.Set("Authorization", "Bearer "+tc.token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != tc.status || (tc.contains != "" && !strings.Contains(response.Body.String(), tc.contains)) {
			t.Fatalf("%s %s: %d %s", tc.method, tc.body, response.Code, response.Body.String())
		}
	}
}
