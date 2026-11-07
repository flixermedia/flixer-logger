package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/flixermedia/flixer-logger/internal/store"
)

func prepareCheckpointDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) == string(filepath.Separator) {
		return errors.New("checkpoint directory must be an absolute non-root path")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create checkpoint directory: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect checkpoint directory: %w", err)
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 || !ok || owner.Uid != uint32(os.Geteuid()) {
		return errors.New("checkpoint directory must be owned by the logger user and inaccessible to other users")
	}
	return nil
}

func createCheckpoint(storage *store.Store, directory string, now time.Time) (string, error) {
	if err := prepareCheckpointDirectory(directory); err != nil {
		return "", err
	}
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate checkpoint name: %w", err)
	}
	name := now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(suffix[:])
	destination := filepath.Join(directory, name)
	if err := storage.Checkpoint(destination); err != nil {
		return "", err
	}
	return destination, nil
}
