package httpapi

import (
	"context"
	"crypto/sha256"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
	"github.com/flixermedia/flixer-logger/pkg/exporter"
)

func TestExporterDeliversToAuthenticatedHandler(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	defer storage.Close()
	token := strings.Repeat("x", 32)
	verifier, err := auth.New([]auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "tenant", Scope: auth.Ingest}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	failures := make(chan error, 1)
	client, err := exporter.New(exporter.Config{
		Endpoint: server.URL, Token: token, QueueSize: 10, BatchSize: 10,
		FlushInterval: time.Hour, Timeout: time.Second, RetryDelay: time.Millisecond,
		MaxAttempts: 2, OnError: func(err error) { failures <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Enqueue(exporter.Event{
		SchemaVersion: 1, EventID: "event", Time: time.Now(), Level: "info",
		InstanceID: "node", App: "api", Message: "password=secret",
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failures:
		t.Fatalf("delivery failure: %v", err)
	default:
	}
	events, err := storage.Recent("tenant", 10)
	if err != nil || len(events) != 1 || strings.Contains(events[0].Message, "secret") {
		t.Fatalf("delivery/redaction: %+v %v", events, err)
	}
}
