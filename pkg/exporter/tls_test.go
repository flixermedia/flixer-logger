package exporter

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoggerHTTPClientTrustsConfiguredCA(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	caPath := filepath.Join(t.TempDir(), "logger-ca.pem")
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.TLS.Certificates[0].Certificate[0]})
	if err := os.WriteFile(caPath, cert, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLIXER_LOGGER_CA_FILE", caPath)
	client, err := newLoggerHTTPClient(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", response.StatusCode)
	}
}

func TestLoggerHTTPClientRejectsInvalidConfiguredCA(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logger-ca.pem")
	if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLIXER_LOGGER_CA_FILE", path)
	if _, err := newLoggerHTTPClient(time.Second); err == nil {
		t.Fatal("invalid CA file was accepted")
	}
}
