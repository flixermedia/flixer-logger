package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestErrorGroupsEndpoint(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	token := strings.Repeat("q", 32)
	verifier, err := auth.New([]auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "one", Scope: auth.Query}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	event := store.Event{SchemaVersion: 1, EventID: "id", Time: now, Level: "error", App: "api", InstanceID: "node", Message: "failed"}
	if _, err := storage.PutBatch("one", []store.Event{event}); err != nil {
		t.Fatal(err)
	}
	params := url.Values{"start": {now.Add(-time.Minute).Format(time.RFC3339Nano)}, "end": {now.Add(time.Minute).Format(time.RFC3339Nano)}}
	path := "/api/logs/errors?" + params.Encode()
	for _, tc := range []struct {
		token  string
		status int
	}{
		{"", http.StatusUnauthorized},
		{token, http.StatusOK},
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if tc.token != "" {
			req.Header.Set("Authorization", "Bearer "+tc.token)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != tc.status {
			t.Fatalf("unexpected status: %d %s", rec.Code, rec.Body.String())
		}
		if tc.status == http.StatusOK {
			var result struct {
				Groups []store.ErrorGroup `json:"groups"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil || len(result.Groups) != 1 || result.Groups[0].Count != 1 {
				t.Fatalf("unexpected groups: %+v %v", result, err)
			}
		}
	}
}
