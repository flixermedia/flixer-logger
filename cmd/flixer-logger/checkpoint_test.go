package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestCheckpointDirectoryAndCreation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "checkpoints")
	if err := prepareCheckpointDirectory(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := prepareCheckpointDirectory(path); err == nil {
		t.Fatal("insecure checkpoint directory accepted")
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := prepareCheckpointDirectory(filepath.Join(root, "link")); err == nil {
		t.Fatal("symlink accepted")
	}
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	destination, err := createCheckpoint(storage, path, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(destination); err != nil || !info.IsDir() {
		t.Fatalf("checkpoint not created: %v", err)
	}
}
