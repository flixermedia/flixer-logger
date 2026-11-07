package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestAcknowledgedIngestionSurvivesProcessCrash(t *testing.T) {
	const token = "crash-test-ingestion-token-0000000000"
	if os.Getenv("FLIXER_CRASH_TEST_CHILD") == "1" {
		storage, err := store.Open(os.Getenv("FLIXER_CRASH_TEST_DATA"))
		if err != nil {
			t.Fatal(err)
		}
		verifier, err := auth.New([]auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "a", Scope: auth.Ingest}})
		if err != nil {
			t.Fatal(err)
		}
		handler, err := New(storage, verifier)
		if err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("CRASH_TEST_READY=%s\n", listener.Addr())
		if err := http.Serve(listener, handler); err != nil {
			t.Fatal(err)
		}
		return
	}

	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAcknowledgedIngestionSurvivesProcessCrash$")
	cmd.Env = append(os.Environ(), "FLIXER_CRASH_TEST_CHILD=1", "FLIXER_CRASH_TEST_DATA="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	addresses := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if address, ok := strings.CutPrefix(scanner.Text(), "CRASH_TEST_READY="); ok {
				addresses <- address
				return
			}
		}
		close(addresses)
	}()
	var address string
	select {
	case address = <-addresses:
	case <-ctx.Done():
		t.Fatal("logger subprocess did not become ready")
	}
	if address == "" {
		t.Fatal("logger subprocess stopped before readiness")
	}
	event := store.Event{SchemaVersion: 1, EventID: "acknowledged", Time: time.Now(), Level: "error", App: "api", Source: "crash-test", InstanceID: "node", Message: "durable event"}
	payload, err := json.Marshal(map[string]any{"events": []store.Event{event}})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/api/logs/batch", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var ack struct {
		Accepted int `json:"accepted"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&ack)
	closeErr := response.Body.Close()
	if decodeErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || ack.Accepted != 1 {
		t.Fatalf("ingest not acknowledged: status=%d ack=%+v decode=%v close=%v", response.StatusCode, ack, decodeErr, closeErr)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	waited = true
	storage, err := store.Open(dir)
	if err != nil {
		t.Fatalf("reopen after crash: %v; subprocess stderr: %s", err, stderr.String())
	}
	defer storage.Close()
	events, _, err := storage.QueryIndex("a", "source", "crash-test", "", 10)
	if err != nil || len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("acknowledged event missing from index after crash: %+v %v", events, err)
	}
	if inserted, err := storage.PutBatch("a", []store.Event{event}); err != nil || inserted != 0 {
		t.Fatalf("deduplication lost after crash: inserted=%d error=%v", inserted, err)
	}
	start := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	counts, err := storage.SourceStats(context.Background(), "a", start, start.Add(3*time.Minute))
	if err != nil || len(counts) != 1 || counts[0].Count != 1 {
		t.Fatalf("source aggregates lost after crash: %+v %v", counts, err)
	}
}
