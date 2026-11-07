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

func TestExporterRotatesToAuthorizedCredentialBeforeFlush(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	oldToken, newToken := strings.Repeat("x", 32), strings.Repeat("y", 32)
	credential := func(token string) auth.Credential {
		return auth.Credential{
			Digest: sha256.Sum256([]byte(token)), DeploymentID: "tenant", Scope: auth.Ingest, App: "flixer-indexer",
		}
	}
	verifier, err := auth.New([]auth.Credential{credential(oldToken), credential(newToken)})
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
		Endpoint: server.URL, Token: oldToken, QueueSize: 10, BatchSize: 10,
		FlushInterval: time.Hour, Timeout: time.Second, RetryDelay: time.Millisecond,
		MaxAttempts: 2, OnError: func(err error) { failures <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Enqueue(exporter.Event{
		SchemaVersion: 1, EventID: "rotated", Time: time.Now(), Level: "info",
		InstanceID: "node", App: "flixer-indexer", Message: "queued before rotation",
	}); err != nil {
		t.Fatal(err)
	}
	if err := client.RotateToken(newToken); err != nil {
		t.Fatal(err)
	}
	if err := verifier.Replace([]auth.Credential{credential(newToken)}); err != nil {
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
	if err != nil || len(events) != 1 || events[0].EventID != "rotated" {
		t.Fatalf("rotated delivery: %+v %v", events, err)
	}
}
