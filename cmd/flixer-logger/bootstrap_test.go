package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
)

func TestBootstrapOwnerCreatesAndRestoresPrivateIdentity(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "owner-token")
	first, err := bootstrapOwner(path, dir, true)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 || len(data) != 43 {
		t.Fatalf("unexpected owner file: %v %v", info, err)
	}
	verifier, err := auth.New(first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(string(data), auth.Owner, time.Now()); err != nil {
		t.Fatal("generated owner token is not authorized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := run(ctx, []string{"--data-dir", dir, "--bootstrap-owner-file", path, "--listen", "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	second, err := bootstrapOwner(path, dir, true)
	if err != nil || second[0].Digest != first[0].Digest {
		t.Fatalf("bootstrap identity did not survive restart: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapOwner(path, dir, true); err == nil {
		t.Fatal("missing owner file was silently regenerated for an existing store")
	}
	if _, err := bootstrapOwner(path, dir, false); err == nil {
		t.Fatal("reload generated a missing owner file")
	}
}

func TestBootstrapOwnerRejectsUnsafeFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "owner-token")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 43)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapOwner(path, dir, true); err == nil {
		t.Fatal("accepted a public owner file")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapOwner(path, dir, true); err == nil {
		t.Fatal("accepted an invalid owner token")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapOwner(link, dir, true); err == nil {
		t.Fatal("accepted a symlink")
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapOwner(fifo, dir, true); err == nil {
		t.Fatal("accepted a FIFO")
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapOwner(path, dir, true); err == nil {
		t.Fatal("accepted a public bootstrap directory")
	}
}

func TestBootstrapRejectsAmbiguousOptions(t *testing.T) {
	dir := t.TempDir()
	err := run(context.Background(), []string{
		"--data-dir", dir, "--credentials-file", filepath.Join(dir, "digests"),
		"--bootstrap-owner-file", filepath.Join(dir, "owner"),
	})
	if err == nil {
		t.Fatal("accepted two bootstrap methods")
	}
}

func TestGeneratedOwnerAuthenticatesLiveServer(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "owner-token")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"--data-dir", dir, "--bootstrap-owner-file", path, "--listen", address})
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("logger shutdown: %v", err)
		}
	}()
	client := &http.Client{Timeout: time.Second}
	url := "http://" + address
	ready := false
	for i := 0; i < 100; i++ {
		response, err := client.Get(url + "/api/health/ready")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("logger did not become ready")
	}
	token, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		code int
	}{{"/api/owner/capacity", http.StatusOK}, {"/api/status", http.StatusUnauthorized}} {
		request, err := http.NewRequest(http.MethodGet, url+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+string(token))
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != tc.code {
			t.Fatalf("%s: got status %d, want %d", tc.path, response.StatusCode, tc.code)
		}
	}
}
