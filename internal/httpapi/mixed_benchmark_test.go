package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

// Run with -benchtime=10x to stay within the default ingestion burst.
func BenchmarkHTTPMixedBurst(b *testing.B) {
	storage, err := store.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()
	ingest, query := strings.Repeat("i", 32), strings.Repeat("q", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "benchmark", Scope: auth.Ingest},
		{Digest: sha256.Sum256([]byte(query)), DeploymentID: "benchmark", Scope: auth.Query},
	})
	if err != nil {
		b.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		b.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &http.Client{Timeout: 30 * time.Second}
	stream, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/logs/stream", nil)
	if err != nil {
		b.Fatal(err)
	}
	stream.Header.Set("Authorization", "Bearer "+query)
	response, err := client.Do(stream)
	if err != nil {
		b.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		b.Fatalf("stream status: %d", response.StatusCode)
	}
	const batchSize = 500
	expected := b.N * batchSize
	streamResult := make(chan error, 1)
	go func() {
		remaining := make(map[string]bool, expected)
		for batch := 0; batch < b.N; batch++ {
			for i := 0; i < batchSize; i++ {
				remaining[fmt.Sprintf("%d-%d", batch, i)] = true
			}
		}
		scanner := bufio.NewScanner(response.Body)
		scanner.Buffer(make([]byte, 4096), 128*1024)
		count := 0
		for scanner.Scan() {
			if scanner.Text() == "event: overflow" {
				streamResult <- fmt.Errorf("live overflow after %d events", count)
				return
			}
			if strings.HasPrefix(scanner.Text(), "data: ") {
				var event struct {
					EventID string `json:"event_id"`
				}
				if err := json.Unmarshal([]byte(strings.TrimPrefix(scanner.Text(), "data: ")), &event); err != nil || !remaining[event.EventID] {
					streamResult <- fmt.Errorf("invalid or duplicate live event %q: %v", event.EventID, err)
					return
				}
				delete(remaining, event.EventID)
				count++
				if count == expected {
					streamResult <- nil
					return
				}
			}
		}
		streamResult <- fmt.Errorf("live stream ended after %d events: %v", count, scanner.Err())
	}()
	start := time.Now().Add(-time.Minute).UTC()
	searchBody, err := json.Marshal(map[string]any{
		"start": start, "end": start.Add(time.Hour), "limit": 10, "fields": map[string]string{"app": "flixer"},
	})
	if err != nil {
		b.Fatal(err)
	}
	var searches atomic.Int64
	searchDone := make(chan error, 1)
	startSearch := make(chan struct{})
	stopSearch := make(chan struct{})
	go func() {
		<-startSearch
		for ctx.Err() == nil {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/api/logs/search", bytes.NewReader(searchBody))
			if err != nil {
				searchDone <- err
				return
			}
			req.Header.Set("Authorization", "Bearer "+query)
			resp, err := client.Do(req)
			if err != nil {
				if ctx.Err() == nil {
					searchDone <- err
				} else {
					searchDone <- nil
				}
				return
			}
			var page struct {
				Events []store.Event `json:"events"`
			}
			readErr := json.NewDecoder(resp.Body).Decode(&page)
			closeErr := resp.Body.Close()
			if resp.StatusCode != http.StatusOK || readErr != nil || closeErr != nil {
				searchDone <- fmt.Errorf("search HTTP %d: read=%v close=%v", resp.StatusCode, readErr, closeErr)
				return
			}
			if page.Events == nil || len(page.Events) > 10 {
				searchDone <- fmt.Errorf("search returned an invalid event page")
				return
			}
			for _, event := range page.Events {
				if event.App != "flixer" || event.DeploymentID != "benchmark" {
					searchDone <- fmt.Errorf("search returned unexpected event: %+v", event)
					return
				}
			}
			searches.Add(1)
			select {
			case <-ctx.Done():
				searchDone <- nil
				return
			case <-stopSearch:
				searchDone <- nil
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
		searchDone <- nil
	}()
	jobs := make(chan int)
	failures := make(chan error, 4)
	var workers sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	close(startSearch)
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for batch := range jobs {
				events := make([]store.Event, batchSize)
				for i := range events {
					events[i] = store.Event{SchemaVersion: 1, EventID: fmt.Sprintf("%d-%d", batch, i),
						Time: time.Now().UTC(), Level: "info", InstanceID: "node", App: "flixer",
						Source: "benchmark", Message: "media playback request completed successfully"}
				}
				body, err := json.Marshal(map[string]any{"events": events})
				if err != nil {
					failures <- err
					cancel()
					return
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/api/logs/batch", bytes.NewReader(body))
				if err != nil {
					failures <- err
					cancel()
					return
				}
				req.Header.Set("Authorization", "Bearer "+ingest)
				resp, err := client.Do(req)
				if err != nil {
					failures <- err
					cancel()
					return
				}
				_, readErr := io.Copy(io.Discard, resp.Body)
				closeErr := resp.Body.Close()
				if resp.StatusCode != http.StatusOK || readErr != nil || closeErr != nil {
					failures <- fmt.Errorf("ingest HTTP %d: read=%v close=%v", resp.StatusCode, readErr, closeErr)
					cancel()
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
			case <-ctx.Done():
				return
			}
		}
	}()
	workers.Wait()
	select {
	case err := <-streamResult:
		if err != nil {
			b.Error(err)
		}
	case <-time.After(30 * time.Second):
		b.Error("timed out waiting for live delivery")
	}
	b.StopTimer()
	close(stopSearch)
	if err := <-searchDone; err != nil {
		b.Error(err)
	}
	cancel()
	close(failures)
	for err := range failures {
		b.Error(err)
	}
	if searches.Load() == 0 {
		b.Error("no historical searches completed")
	}
	b.ReportMetric(float64(expected)/b.Elapsed().Seconds(), "events/s")
	b.ReportMetric(float64(searches.Load()), "searches")
}
