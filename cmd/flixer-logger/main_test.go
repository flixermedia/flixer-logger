package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
)

func TestLoadCredentials(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")
	token := strings.Repeat("x", 32)
	hash := sha256.Sum256([]byte(token))
	data := fmt.Sprintf(`{"credentials":[{"sha256":"%s","deployment_id":"tenant","scope":"query"}]}`, hex.EncodeToString(hash[:]))
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	verifier, err := loadCredentials(path)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := verifier.Verify(token, auth.Query, time.Now())
	if err != nil || identity.DeploymentID != "tenant" {
		t.Fatalf("unexpected identity: %+v %v", identity, err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCredentials(path); err == nil {
		t.Fatal("insecure file permissions accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data+` {}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCredentials(path); err == nil {
		t.Fatal("trailing data accepted")
	}
}

func TestCredentialFileRejectsSymlinksAndFIFO(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(path, []byte(`{"credentials":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readCredentials(link); err == nil {
		t.Fatal("symbolic link accepted")
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := readCredentials(fifo)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("reading FIFO blocked")
	}
}

func TestRunRequiresTLSForRemoteAndShutsDown(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(path, []byte(`{"credentials":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--data-dir", dir, "--credentials-file", path}
	if err := run(context.Background(), append(args, "--listen", "0.0.0.0:0")); err == nil {
		t.Fatal("remote plaintext listener accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := run(ctx, append(args, "--listen", "127.0.0.1:0")); err != nil {
		t.Fatalf("start and shutdown: %v", err)
	}
}
