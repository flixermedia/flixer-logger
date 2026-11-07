package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func BenchmarkHTTPBatchIngestion(b *testing.B) {
	benchmarkHTTPBatchIngestion(b, 100)
}

func BenchmarkHTTPBurstBatchIngestion(b *testing.B) {
	benchmarkHTTPBatchIngestion(b, 500)
}

func benchmarkHTTPBatchIngestion(b *testing.B, batchSize int) {
	storage, err := store.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()
	token := strings.Repeat("i", 32)
	verifier, err := auth.New([]auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "benchmark", Scope: auth.Ingest}})
	if err != nil {
		b.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		b.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &http.Client{Timeout: 30 * time.Second}
	jobs := make(chan int)
	done := make(chan struct{})
	failures := make(chan error, 4)
	var workers sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for batchID := range jobs {
				events := make([]store.Event, batchSize)
				for i := range events {
					events[i] = store.Event{
						SchemaVersion: 1, EventID: fmt.Sprintf("%d-%d", batchID, i), Time: time.Now().UTC(),
						Level: "info", InstanceID: "node-1", App: "flixer", Source: "benchmark",
						Message: "media playback request completed successfully",
					}
				}
				body, err := json.Marshal(struct {
					Events []store.Event `json:"events"`
				}{events})
				if err != nil {
					failures <- err
					return
				}
				req, err := http.NewRequest(http.MethodPost, server.URL+"/api/logs/batch", bytes.NewReader(body))
				if err != nil {
					failures <- err
					return
				}
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := client.Do(req)
				if err != nil {
					failures <- err
					return
				}
				_, readErr := io.Copy(io.Discard, resp.Body)
				closeErr := resp.Body.Close()
				if resp.StatusCode != http.StatusOK || readErr != nil || closeErr != nil {
					failures <- fmt.Errorf("HTTP %d: read=%v close=%v", resp.StatusCode, readErr, closeErr)
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for i := 0; i < b.N; i++ {
			select {
			case jobs <- i:
			case <-done:
				return
			}
		}
	}()
	workers.Wait()
	close(done)
	b.StopTimer()
	close(failures)
	for err := range failures {
		b.Error(err)
	}
	if b.Elapsed() > 0 {
		b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "events/s")
	}
}
