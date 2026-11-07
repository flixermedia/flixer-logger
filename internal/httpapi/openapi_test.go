package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestOpenAPIContract(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	verifier, err := auth.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/openapi.json", nil))
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("contract response: %d %v", rec.Code, rec.Header())
	}
	var document map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document["openapi"] != "3.1.0" {
		t.Fatalf("unexpected OpenAPI version: %v", document["openapi"])
	}
	paths, ok := document["paths"].(map[string]any)
	if !ok {
		t.Fatal("contract paths missing")
	}
	expected := map[string][]string{
		"/api/openapi.json": {"get"}, "/api/health/live": {"get"}, "/api/health/ready": {"get"},
		"/api/status": {"get"}, "/api/owner/capacity": {"get"}, "/api/owner/capacity-policy": {"get", "put"}, "/api/logs": {"get"}, "/api/logs/batch": {"post"},
		"/api/owner/credentials": {"get", "post"}, "/api/owner/credentials/{id}": {"delete"},
		"/api/owner/pairings": {"get", "post"}, "/api/owner/pairings/{id}": {"delete"}, "/api/logger/pairings/redeem": {"post"},
		"/api/logs/search": {"post"}, "/api/logs/trace/{trace_id}": {"get"},
		"/api/logs/stats": {"get"}, "/api/logs/sources": {"get"}, "/api/logs/errors": {"get"}, "/api/logs/stream": {"get"},
		"/api/logger/retention": {"get", "put"}, "/api/logger/ingestion-policy": {"get", "put"},
		"/api/logger/quota": {"get", "put"}, "/api/logger/credentials": {"get", "post"},
		"/api/logger/credentials/{id}": {"delete"},
	}
	if len(paths) != len(expected) {
		t.Fatalf("unexpected path count: %d", len(paths))
	}
	for path, methods := range expected {
		operations, ok := paths[path].(map[string]any)
		if !ok || len(operations) != len(methods) {
			t.Fatalf("unexpected operations for %s", path)
		}
		for _, method := range methods {
			operation, ok := operations[method].(map[string]any)
			if !ok || operation["responses"] == nil {
				t.Fatalf("missing operation or responses: %s %s", method, path)
			}
		}
	}
	var checkRefs func(any)
	checkRefs = func(value any) {
		switch v := value.(type) {
		case []any:
			for _, child := range v {
				checkRefs(child)
			}
		case map[string]any:
			for key, child := range v {
				if key == "$ref" {
					ref, ok := child.(string)
					if !ok || !strings.HasPrefix(ref, "#/") {
						t.Fatalf("invalid reference: %v", child)
					}
					var target any = document
					for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
						object, ok := target.(map[string]any)
						if !ok {
							t.Fatalf("unresolved reference: %s", ref)
						}
						target, ok = object[part]
						if !ok {
							t.Fatalf("unresolved reference: %s", ref)
						}
					}
				} else {
					checkRefs(child)
				}
			}
		}
	}
	checkRefs(document)
}
