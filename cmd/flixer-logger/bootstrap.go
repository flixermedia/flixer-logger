package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/flixermedia/flixer-logger/internal/auth"
)

// bootstrapOwner creates an owner secret only for an uninitialized store.
// Reloads never create or replace a missing secret.
func bootstrapOwner(path, dataDir string, create bool) ([]auth.Credential, error) {
	parent := filepath.Dir(path)
	info, err := os.Lstat(parent)
	if err != nil {
		return nil, fmt.Errorf("inspect bootstrap directory: %w", err)
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || !ok || owner.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("bootstrap directory must be owned by the logger user and inaccessible to other users")
	}

	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, syscall.ENOENT) && create {
		_, statErr := os.Lstat(filepath.Join(dataDir, "logs", "logger", "pebble"))
		if statErr == nil {
			return nil, errors.New("bootstrap owner file is missing for an existing logger store")
		}
		if !errors.Is(statErr, os.ErrNotExist) {
			return nil, fmt.Errorf("inspect logger store: %w", statErr)
		}
		var random [32]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, fmt.Errorf("generate owner secret: %w", err)
		}
		token := base64.RawURLEncoding.EncodeToString(random[:])
		fd, err = syscall.Open(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
		if err != nil {
			return nil, fmt.Errorf("create bootstrap owner file: %w", err)
		}
		file := os.NewFile(uintptr(fd), path)
		_, writeErr := file.WriteString(token)
		syncErr := file.Sync()
		closeErr := file.Close()
		if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
			return nil, fmt.Errorf("persist bootstrap owner file: %w", err)
		}
		dir, err := os.Open(parent)
		if err != nil {
			return nil, fmt.Errorf("open bootstrap directory: %w", err)
		}
		err = errors.Join(dir.Sync(), dir.Close())
		if err != nil {
			return nil, fmt.Errorf("persist bootstrap directory: %w", err)
		}
		return []auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "owner", Scope: auth.Owner}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open bootstrap owner file: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err = file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat bootstrap owner file: %w", err)
	}
	owner, ok = info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || owner.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("bootstrap owner file must be owned by the logger user and inaccessible to other users")
	}
	data, err := io.ReadAll(io.LimitReader(file, 44))
	if err != nil {
		return nil, fmt.Errorf("read bootstrap owner file: %w", err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(string(data))
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != string(data) {
		return nil, errors.New("invalid bootstrap owner file")
	}
	return []auth.Credential{{Digest: sha256.Sum256(data), DeploymentID: "owner", Scope: auth.Owner}}, nil
}
