package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/httpapi"
	"github.com/flixermedia/flixer-logger/internal/monitoring"
	"github.com/flixermedia/flixer-logger/internal/registry"
	"github.com/flixermedia/flixer-logger/internal/retention"
	"github.com/flixermedia/flixer-logger/internal/store"
)

type credentialFile struct {
	Credentials []struct {
		Digest       string     `json:"sha256"`
		DeploymentID string     `json:"deployment_id"`
		Scope        auth.Scope `json:"scope"`
		App          string     `json:"app"`
		ExpiresAt    time.Time  `json:"expires_at"`
	} `json:"credentials"`
}

func loadCredentials(path string) (*auth.Verifier, error) {
	credentials, err := readCredentials(path)
	if err != nil {
		return nil, err
	}
	return auth.New(credentials)
}

func readCredentials(path string) ([]auth.Credential, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open credential file: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat credential file: %w", err)
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || !ok || owner.Uid != uint32(os.Geteuid()) {
		return nil, errors.New("credential file must be a regular file owned by the logger user and inaccessible to other users")
	}
	reader := io.LimitReader(file, 1024*1024+1)
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read credential file: %w", err)
	}
	if len(content) > 1024*1024 {
		return nil, errors.New("credential file exceeds size limit")
	}
	var data credentialFile
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&data); err != nil {
		return nil, fmt.Errorf("parse credential file: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("credential file contains trailing data")
	}
	credentials := make([]auth.Credential, 0, len(data.Credentials))
	for _, entry := range data.Credentials {
		digest, err := hex.DecodeString(entry.Digest)
		if err != nil || len(digest) != 32 {
			return nil, errors.New("invalid credential digest")
		}
		credential := auth.Credential{DeploymentID: entry.DeploymentID, Scope: entry.Scope, App: entry.App, ExpiresAt: entry.ExpiresAt}
		copy(credential.Digest[:], digest)
		credentials = append(credentials, credential)
	}
	return credentials, nil
}

func run(ctx context.Context, args []string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	flags := flag.NewFlagSet("flixer-logger", flag.ContinueOnError)
	dataDir := flags.String("data-dir", "", "Flixer data root")
	listen := flags.String("listen", "127.0.0.1:8098", "listen address")
	credentialsPath := flags.String("credentials-file", "", "owner-only credential digest file")
	ownerPath := flags.String("bootstrap-owner-file", "", "private owner token file, generated only for a new store")
	certPath := flags.String("tls-cert", "", "TLS certificate path")
	keyPath := flags.String("tls-key", "", "TLS private key path")
	checkpointDir := flags.String("checkpoint-dir", "", "owner-only directory for SIGUSR1 checkpoints")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *dataDir == "" || (*credentialsPath == "") == (*ownerPath == "") {
		return errors.New("data-dir and exactly one of credentials-file or bootstrap-owner-file are required")
	}
	if (*certPath == "") != (*keyPath == "") {
		return errors.New("TLS certificate and key must be provided together")
	}
	if *checkpointDir != "" {
		if err := prepareCheckpointDirectory(*checkpointDir); err != nil {
			return err
		}
	}
	host, _, err := net.SplitHostPort(*listen)
	if err != nil {
		return fmt.Errorf("invalid listen address: %w", err)
	}
	ip := net.ParseIP(host)
	if *certPath == "" && (ip == nil || !ip.IsLoopback()) {
		return errors.New("TLS is required for non-loopback listeners")
	}
	readBootstrap := func(create bool) ([]auth.Credential, error) {
		if *ownerPath != "" {
			return bootstrapOwner(*ownerPath, *dataDir, create)
		}
		return readCredentials(*credentialsPath)
	}
	bootstrap, err := readBootstrap(true)
	if err != nil {
		return err
	}
	storage, err := store.Open(*dataDir)
	if err != nil {
		return err
	}
	defer storage.Close()
	credentials, err := registry.New(storage, bootstrap)
	if err != nil {
		return fmt.Errorf("initialize credential registry: %w", err)
	}
	handler, err := httpapi.NewWithRegistry(storage, credentials)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr: *listen, Handler: monitoring.Middleware(handler), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()
	retentionStopped := make(chan struct{})
	go func() {
		defer monitoring.Recover()
		defer close(retentionStopped)
		retention.RunWithTracker(ctx, storage, handler.RetentionTracker())
	}()
	defer func() {
		cancel()
		<-retentionStopped
	}()
	if *checkpointDir != "" {
		checkpoints := make(chan os.Signal, 1)
		signal.Notify(checkpoints, syscall.SIGUSR1)
		checkpointStopped := make(chan struct{})
		go func() {
			defer monitoring.Recover()
			defer close(checkpointStopped)
			for {
				select {
				case <-ctx.Done():
					return
				case <-checkpoints:
					path, err := createCheckpoint(storage, *checkpointDir, time.Now())
					if err != nil {
						slog.Error("checkpoint failed", "error", err)
					} else {
						slog.Info("checkpoint created", "path", path)
					}
				}
			}
		}()
		defer func() {
			signal.Stop(checkpoints)
			cancel()
			<-checkpointStopped
		}()
	}
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	defer signal.Stop(reload)
	go func() {
		defer monitoring.Recover()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reload:
				bootstrap, err := readBootstrap(false)
				if err == nil {
					err = credentials.ReplaceBootstrap(bootstrap)
				}
				if err != nil {
					slog.Error("credential reload failed; previous credentials remain active", "error", err)
				} else {
					slog.Info("credentials reloaded")
				}
			}
		}
	}()
	stopped := make(chan struct{})
	go func() {
		defer monitoring.Recover()
		defer close(stopped)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("logger shutdown failed", "error", err)
		}
	}()
	slog.Info("logger listening", "address", listener.Addr().String())
	if *certPath != "" {
		err = server.ServeTLS(listener, *certPath, *keyPath)
	} else {
		err = server.Serve(listener)
	}
	if !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve logger: %w", err)
	}
	<-stopped
	return nil
}

func configureMonitoringLogger() {
	slog.SetDefault(monitoring.WrapLogger(slog.New(slog.NewTextHandler(os.Stderr, nil))))
}

func main() {
	configureMonitoringLogger()
	defer monitoring.Recover()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	baseURL := os.Getenv("FLIXER_URL")
	if baseURL == "" {
		baseURL = "http://127.0.0.1:8083"
	}
	monitoringDone := make(chan struct{})
	go func() {
		defer close(monitoringDone)
		defer monitoring.Recover()
		monitoring.Load(ctx, baseURL, os.Getenv("SENTRY_ENVIRONMENT"), &http.Client{Timeout: 5 * time.Second}, 5*time.Second)
	}()
	err := run(ctx, os.Args[1:])
	cancel()
	<-monitoringDone
	if err != nil {
		slog.Error("logger stopped", "error", err)
		monitoring.Flush()
		os.Exit(1)
	}
	monitoring.Flush()
}
