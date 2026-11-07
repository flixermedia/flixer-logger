package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestBrowserCorsExcludesManagementAndIngestion(t *testing.T) {
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
	for _, tc := range []struct {
		path    string
		allowed bool
	}{
		{"/api/logs/search", true},
		{"/api/logs/errors", true},
		{"/api/logs/stream", true},
		{"/api/logs/batch", false},
		{"/api/logger/credentials", false},
		{"/api/logger/ingestion-policy", false},
		{"/api/logger/quota", false},
	} {
		req := httptest.NewRequest(http.MethodOptions, tc.path, nil)
		req.Header.Set("Origin", "https://web.example")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if (rec.Header().Get("Access-Control-Allow-Origin") == "*") != tc.allowed ||
			(rec.Code == http.StatusNoContent) != tc.allowed || rec.Header().Get("Access-Control-Allow-Credentials") != "" {
			t.Fatalf("unexpected CORS response for %s: %d %v", tc.path, rec.Code, rec.Header())
		}
		if tc.path == "/api/logs/stream" && !strings.Contains(rec.Header().Get("Access-Control-Allow-Headers"), "Last-Event-ID") {
			t.Fatal("live replay header is not allowed by CORS")
		}
	}
}
