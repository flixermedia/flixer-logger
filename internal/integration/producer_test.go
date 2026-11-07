package integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/pkg/exporter"
)

func TestProducerExportsToRealLogger(t *testing.T) {
	binary := os.Getenv("FLIXER_LOGGER_TEST_BINARY")
	if binary == "" {
		t.Skip("set FLIXER_LOGGER_TEST_BINARY to run the real logger integration")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	origin := "http://" + address
	ownerPath := filepath.Join(dir, "owner")
	process := exec.Command(binary, "--data-dir", filepath.Join(dir, "data"), "--bootstrap-owner-file", ownerPath, "--listen", address)
	process.Stderr = os.Stderr
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if process.ProcessState == nil {
			_ = process.Process.Kill()
		}
		_ = process.Wait()
	}()
	client := &http.Client{Timeout: 2 * time.Second}
	waitUntil(t, func() bool {
		resp, err := client.Get(origin + "/api/health/ready")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	owner, err := os.ReadFile(ownerPath)
	if err != nil {
		t.Fatal(err)
	}
	request := func(path, token string, body any) map[string]any {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodPost, origin+path, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("%s returned %d", path, resp.StatusCode)
		}
		var result map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	expiry := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	invitation := request("/api/owner/pairings", strings.TrimSpace(string(owner)), map[string]any{
		"deployment_id": "integration", "expires_at": time.Now().Add(5 * time.Minute).UTC().Format(time.RFC3339),
		"management_expiry": expiry,
	})
	manage := request("/api/logger/pairings/redeem", "", map[string]any{"token": invitation["token"]})["token"].(string)
	tokens := make(map[string]string)
	for _, app := range []string{"flixer-indexer", "flixer-streamer", "flixer-updater"} {
		tokens[app] = request("/api/logger/credentials", manage, map[string]any{
			"scope": "ingest", "app": app, "expires_at": expiry,
		})["token"].(string)
	}
	query := request("/api/logger/credentials", manage, map[string]any{"scope": "query", "expires_at": expiry})["token"].(string)
	secret := strings.Repeat("s", 32)
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		app := "flixer-" + strings.TrimPrefix(r.URL.Path, "/api/internal/logger/")
		empty := sha256.Sum256(nil)
		mac := hmac.New(sha256.New, []byte(secret))
		fmt.Fprintf(mac, "flixer-indexer:v1:%s:GET:%s:%x", r.Header.Get("X-Indexer-Timestamp"), r.URL.Path, empty)
		if tokens[app] == "" || !hmac.Equal([]byte(r.Header.Get("X-Indexer-Signature")), []byte(hex.EncodeToString(mac.Sum(nil)))) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"endpoint": origin, "token": tokens[app], "expiresAt": expiry})
	}))
	defer control.Close()
	for _, app := range []string{"flixer-indexer", "flixer-streamer", "flixer-updater"} {
		handler, service, err := exporter.StartService(slog.NewJSONHandler(io.Discard, nil), control.URL, app, []byte(secret), io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		logger := slog.New(handler)
		waitUntil(t, func() bool {
			logger.Info("real-producer-integration", "application", app)
			req, _ := http.NewRequest(http.MethodGet, origin+"/api/logs?field=app&value="+app, nil)
			req.Header.Set("Authorization", "Bearer "+query)
			resp, err := client.Do(req)
			if err != nil {
				return false
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			return err == nil && strings.Contains(string(body), "real-producer-integration")
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = service.Close(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func waitUntil(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("timed out waiting for integration readiness")
}
