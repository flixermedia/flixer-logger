package httpapi

import (
	"bytes"
	"compress/gzip"
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

func TestCompressedIngestion(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	token := strings.Repeat("i", 32)
	verifier, err := auth.New([]auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "a", Scope: auth.Ingest}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	event := store.Event{SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "info", App: "api", InstanceID: "node", Message: "ready"}
	body, err := json.Marshal(map[string]any{"events": []store.Event{event}})
	if err != nil {
		t.Fatal(err)
	}
	compress := func(input []byte) []byte {
		t.Helper()
		var buffer bytes.Buffer
		writer := gzip.NewWriter(&buffer)
		if _, err := writer.Write(input); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		return buffer.Bytes()
	}
	invalidChecksum := compress(body)
	invalidChecksum[len(invalidChecksum)-1] ^= 0xff
	for _, tc := range []struct {
		name, encoding string
		body           []byte
		status         int
	}{
		{"gzip", "gzip", compress(body), http.StatusOK},
		{"corrupt", "gzip", invalidChecksum, http.StatusBadRequest},
		{"oversized inflated", "gzip", compress([]byte(`{"events":[],"padding":"` + strings.Repeat("x", maxBodyBytes) + `"}`)), http.StatusBadRequest},
		{"unsupported", "br", body, http.StatusUnsupportedMediaType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/logs/batch", bytes.NewReader(tc.body))
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("Content-Encoding", tc.encoding)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("got %d, want %d: %s", response.Code, tc.status, response.Body.String())
			}
		})
	}
	events, err := storage.Recent("a", 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("invalid requests altered storage: %+v %v", events, err)
	}
}
