package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestReloadHelper(t *testing.T) {
	if os.Getenv("LOGGER_RELOAD_HELPER") != "1" {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := run(ctx, []string{"--data-dir", os.Getenv("LOGGER_TEST_DIR"), "--credentials-file", os.Getenv("LOGGER_TEST_CREDENTIALS"), "--listen", os.Getenv("LOGGER_TEST_LISTEN")}); err != nil {
		t.Fatal(err)
	}
}

func TestSIGHUPReloadRevokesCredential(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")
	token := strings.Repeat("x", 32)
	digest := sha256.Sum256([]byte(token))
	contents := fmt.Sprintf(`{"credentials":[{"sha256":"%s","deployment_id":"tenant","scope":"query"}]}`, hex.EncodeToString(digest[:]))
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestReloadHelper$")
	cmd.Env = append(os.Environ(), "LOGGER_RELOAD_HELPER=1", "LOGGER_TEST_DIR="+dir, "LOGGER_TEST_CREDENTIALS="+path, "LOGGER_TEST_LISTEN="+address)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	client := &http.Client{Timeout: time.Second}
	waitStatus := func(want int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			req, err := http.NewRequest(http.MethodGet, "http://"+address+"/api/logs", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+token)
			resp, err := client.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode == want {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("server did not return HTTP %d", want)
	}
	waitStatus(http.StatusOK)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	waitStatus(http.StatusOK)
	replacement := filepath.Join(dir, "replacement.json")
	if err := os.WriteFile(replacement, []byte(`{"credentials":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitStatus(http.StatusUnauthorized)
}
