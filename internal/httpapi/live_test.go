package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestLiveWriteAfterIdleLongerThanWriteDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		controller := http.NewResponseController(w)
		if err := writeLive(controller, w, ": connected\n\n"); err != nil {
			t.Errorf("initial write: %v", err)
			return
		}
		time.Sleep(11 * time.Second)
		if err := writeLive(controller, w, "event: log\ndata: {}\n\n"); err != nil {
			t.Errorf("write after idle: %v", err)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read live stream: %v", err)
	}
	if !strings.Contains(string(body), "event: log") {
		t.Fatalf("missing event after idle: %q", body)
	}
}

func TestLiveSubscriberLimitIsDeploymentScoped(t *testing.T) {
	hub := newLiveHub()
	var subscriptions []*subscription
	for range maxSubscribersPerDeployment {
		sub, err := hub.subscribe("a", nil)
		if err != nil {
			t.Fatal(err)
		}
		subscriptions = append(subscriptions, sub)
	}
	if _, err := hub.subscribe("a", nil); !errors.Is(err, errDeploymentSubscriberLimit) {
		t.Fatalf("expected deployment limit, got %v", err)
	}
	other, err := hub.subscribe("b", nil)
	if err != nil {
		t.Fatalf("another deployment could not subscribe: %v", err)
	}
	hub.unsubscribe(other)
	hub.unsubscribe(subscriptions[0])
	replacement, err := hub.subscribe("a", nil)
	if err != nil {
		t.Fatalf("subscriber capacity was not released: %v", err)
	}
	hub.unsubscribe(replacement)
	for _, sub := range subscriptions[1:] {
		hub.unsubscribe(sub)
	}
}

func TestLiveHubIsolationAndOverflow(t *testing.T) {
	hub := newLiveHub()
	a, err := hub.subscribe("a", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := hub.subscribe("b", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.unsubscribe(a)
	defer hub.unsubscribe(b)
	for range liveQueueSize + 1 {
		if err := hub.publish([]store.Event{{DeploymentID: "a", Message: "message"}}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-a.overflow:
	default:
		t.Fatal("slow subscriber was not disconnected")
	}
	select {
	case <-b.events:
		t.Fatal("event leaked to another deployment")
	default:
	}
	if err := hub.publish([]store.Event{{DeploymentID: "b", Message: "message"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.events:
	default:
		t.Fatal("other deployment did not receive its event")
	}
}

func TestLiveHubBuffersMaximumBatchAndBoundsBytes(t *testing.T) {
	hub := newLiveHub()
	sub, err := hub.subscribe("a", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.unsubscribe(sub)
	events := make([]store.Event, 1000)
	for i := range events {
		events[i] = store.Event{DeploymentID: "a", EventID: fmt.Sprintf("event-%04d", i), Message: "ready"}
	}
	if err := hub.publish(events); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub.overflow:
		t.Fatal("one maximum-size batch overflowed an empty subscription")
	default:
	}
	for i := range events {
		select {
		case item := <-sub.events:
			hub.consumed(sub, item)
			var event store.Event
			if err := json.Unmarshal(item.data, &event); err != nil || event.EventID != events[i].EventID {
				t.Fatalf("unexpected event %d: %+v %v", i, event, err)
			}
		default:
			t.Fatalf("missing event %d", i)
		}
	}
	if sub.queuedBytes != 0 {
		t.Fatalf("queue retained %d bytes after draining", sub.queuedBytes)
	}
	large := store.Event{DeploymentID: "a", Message: strings.Repeat("x", 64*1024)}
	for range 65 {
		if err := hub.publish([]store.Event{large}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-sub.overflow:
	default:
		t.Fatal("byte limit did not disconnect a slow subscriber")
	}
	if sub.queuedBytes > liveQueueBytes {
		t.Fatalf("queue exceeded byte limit: %d", sub.queuedBytes)
	}
}

func TestLiveSerializationFailureDisconnectsAffectedDeployment(t *testing.T) {
	hub := newLiveHub()
	a, err := hub.subscribe("a", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := hub.subscribe("b", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.unsubscribe(a)
	defer hub.unsubscribe(b)
	if err := hub.publish([]store.Event{{DeploymentID: "a", Fields: map[string]any{"invalid": math.NaN()}}}); err == nil {
		t.Fatal("expected serialization failure")
	}
	select {
	case <-a.overflow:
	default:
		t.Fatal("subscriber was not notified of missing events")
	}
	select {
	case <-b.overflow:
		t.Fatal("unaffected deployment was disconnected")
	default:
	}
}

func TestLiveFiltersApplyBeforeQueueing(t *testing.T) {
	hub := newLiveHub()
	filters, err := parseLiveFilters(map[string][]string{"app": {"api"}, "task": {"task-1"}, "task_queue": {"scan"}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := hub.subscribe("a", filters)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.unsubscribe(sub)
	for range liveQueueSize + 1 {
		if err := hub.publish([]store.Event{{DeploymentID: "a", App: "api", TaskID: "other"}}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-sub.overflow:
		t.Fatal("unmatched events overflowed a filtered subscription")
	default:
	}
	if err := hub.publish([]store.Event{
		{DeploymentID: "b", App: "api", TaskID: "task-1", Fields: map[string]any{"task_queue": "scan"}},
		{DeploymentID: "a", App: "api", TaskID: "task-1", Fields: map[string]any{"task_queue": "probe"}},
		{DeploymentID: "a", App: "api", TaskID: "task-1", Fields: map[string]any{"task_queue": "scan"}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-sub.events:
		var decoded store.Event
		if err := json.Unmarshal(event.data, &decoded); err != nil || decoded.DeploymentID != "a" || decoded.TaskID != "task-1" {
			t.Fatalf("unexpected filtered event: %+v %v", decoded, err)
		}
	default:
		t.Fatal("matching event not delivered")
	}
	for _, invalid := range []map[string][]string{
		{"unknown": {"x"}}, {"app": {""}}, {"app": {"a", "b"}}, {"task": {strings.Repeat("x", 1025)}}, {"task_queue": {"scan"}},
	} {
		if _, err := parseLiveFilters(invalid); err == nil {
			t.Fatalf("accepted invalid filters: %v", invalid)
		}
	}
}

func TestLiveFiltersApplyToReplay(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	token := strings.Repeat("q", 32)
	verifier, err := auth.New([]auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "a", Scope: auth.Query}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	base := store.Event{SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "info", InstanceID: "node", App: "api", Message: "ready"}
	written, err := storage.PutBatchEvents("a", []store.Event{base})
	if err != nil {
		t.Fatal(err)
	}
	base.EventID, base.TaskID = "two", "other"
	if _, err := storage.PutBatchEvents("a", []store.Event{base}); err != nil {
		t.Fatal(err)
	}
	base.EventID, base.TaskID, base.Fields = "wrong-queue", "match", map[string]any{"task_queue": "probe"}
	if _, err := storage.PutBatchEvents("a", []store.Event{base}); err != nil {
		t.Fatal(err)
	}
	base.EventID, base.TaskID, base.Fields = "three", "match", map[string]any{"task_queue": "scan"}
	if _, err := storage.PutBatchEvents("a", []store.Event{base}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/logs/stream?task=match&task_queue=scan", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Last-Event-ID", store.LiveCursor(written[0]))
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data: ") {
			var event store.Event
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil || event.EventID != "three" {
				t.Fatalf("unexpected replay event: %+v %v", event, err)
			}
			break
		}
	}
}

func TestLiveStreamReceivesMaximumIngestBatch(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	query, ingest := strings.Repeat("q", 32), strings.Repeat("i", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(query)), DeploymentID: "a", Scope: auth.Query},
		{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "a", Scope: auth.Ingest},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &http.Client{Timeout: 30 * time.Second}
	stream, err := http.NewRequest(http.MethodGet, server.URL+"/api/logs/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	stream.Header.Set("Authorization", "Bearer "+query)
	response, err := client.Do(stream)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("stream status: %d", response.StatusCode)
	}
	events := make([]store.Event, 1000)
	for i := range events {
		events[i] = store.Event{SchemaVersion: 1, EventID: fmt.Sprintf("event-%04d", i), Time: time.Now(),
			Level: "info", InstanceID: "node", App: "api", Message: "ready"}
	}
	body, err := json.Marshal(map[string]any{"events": events})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/logs/batch", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+ingest)
	ingested, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.Copy(io.Discard, ingested.Body)
	closeErr := ingested.Body.Close()
	if ingested.StatusCode != http.StatusOK || readErr != nil || closeErr != nil {
		t.Fatalf("ingest status %d: read=%v close=%v", ingested.StatusCode, readErr, closeErr)
	}
	reader := bufio.NewReader(response.Body)
	seen := make(map[string]bool, len(events))
	previous := ""
	for i := 0; i < len(events); {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read event %d: %v", i, err)
		}
		if line == "event: overflow\n" {
			t.Fatalf("unexpected overflow after %d events", i)
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event store.Event
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatalf("unexpected event %d: %+v %v", i, event, err)
		}
		key, err := base64.RawURLEncoding.DecodeString(store.LiveCursor(event))
		if err != nil || (previous != "" && string(key) <= previous) || seen[event.EventID] {
			t.Fatalf("event %d was duplicated or out of storage order: %+v %v", i, event, err)
		}
		seen[event.EventID] = true
		previous = string(key)
		i++
	}
}

func TestLiveReplayUnderConcurrentIngestion(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	query, ingest := strings.Repeat("q", 32), strings.Repeat("i", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(query)), DeploymentID: "a", Scope: auth.Query},
		{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "a", Scope: auth.Ingest},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	base := store.Event{SchemaVersion: 1, EventID: "base", Time: time.Now(), Level: "info", InstanceID: "one", App: "api", Message: "ready"}
	written, err := storage.PutBatchEvents("a", []store.Event{base})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &http.Client{Timeout: 15 * time.Second}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 40; i++ {
			event := base
			event.EventID = fmt.Sprintf("event-%02d", i)
			body, err := json.Marshal(map[string]any{"events": []store.Event{event}})
			if err != nil {
				done <- err
				return
			}
			req, err := http.NewRequest(http.MethodPost, server.URL+"/api/logs/batch", bytes.NewReader(body))
			if err != nil {
				done <- err
				return
			}
			req.Header.Set("Authorization", "Bearer "+ingest)
			resp, err := client.Do(req)
			if err != nil {
				done <- err
				return
			}
			_, readErr := io.Copy(io.Discard, resp.Body)
			closeErr := resp.Body.Close()
			if resp.StatusCode != http.StatusOK || readErr != nil || closeErr != nil {
				done <- fmt.Errorf("ingest status %d, read %v, close %v", resp.StatusCode, readErr, closeErr)
				return
			}
		}
		done <- nil
	}()
	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/logs/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+query)
	req.Header.Set("Last-Event-ID", store.LiveCursor(written[0]))
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("replay status %d", response.StatusCode)
	}
	reader := bufio.NewReader(response.Body)
	for i := 0; i < 40; {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event store.Event
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		if event.EventID != fmt.Sprintf("event-%02d", i) {
			t.Fatalf("event %d out of order or missing: %q", i, event.EventID)
		}
		i++
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLiveStreamRejectsUnauthorizedAndInvalidReplay(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	token := strings.Repeat("q", 32)
	verifier, err := auth.New([]auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "a", Scope: auth.Query}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/logs/stream", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized stream status: %d", unauthorized.Code)
	}
	resume := httptest.NewRequest(http.MethodGet, "/api/logs/stream", nil)
	resume.Header.Set("Authorization", "Bearer "+token)
	resume.Header.Set("Last-Event-ID", "old-event")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, resume)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid replay status: %d", response.Code)
	}
	invalid := httptest.NewRequest(http.MethodGet, "/api/logs/stream?app=a&app=b", nil)
	invalid.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, invalid)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid filters status: %d", response.Code)
	}
	for range maxSubscribersPerDeployment {
		if _, err := handler.live.subscribe("a", nil); err != nil {
			t.Fatal(err)
		}
	}
	limited := httptest.NewRequest(http.MethodGet, "/api/logs/stream", nil)
	limited.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, limited)
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") == "" {
		t.Fatalf("subscriber limit status: %d headers: %v", response.Code, response.Header())
	}
}

func TestLiveReplayAndTransition(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	query, ingest := strings.Repeat("q", 32), strings.Repeat("i", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(query)), DeploymentID: "a", Scope: auth.Query},
		{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "a", Scope: auth.Ingest},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	post := func(id string) {
		t.Helper()
		event := store.Event{SchemaVersion: 1, EventID: id, Time: time.Now(), Level: "info", InstanceID: "one", App: "api", Message: "hello"}
		body, err := json.Marshal(map[string]any{"events": []store.Event{event}})
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodPost, server.URL+"/api/logs/batch", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+ingest)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("ingest status %d", resp.StatusCode)
		}
	}
	post("one")
	recent, err := storage.Recent("a", 1)
	if err != nil || len(recent) != 1 {
		t.Fatalf("recent: %+v %v", recent, err)
	}
	cursor := store.LiveCursor(recent[0])
	post("two")
	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/logs/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+query)
	req.Header.Set("Last-Event-ID", cursor)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	readEvent := func() (string, string) {
		t.Helper()
		var id, data string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(line, "id: ") {
				id = strings.TrimSpace(strings.TrimPrefix(line, "id: "))
			}
			if strings.HasPrefix(line, "data: ") {
				data = line
				break
			}
		}
		return id, data
	}
	id, data := readEvent()
	if id == "" || !strings.Contains(data, `"event_id":"two"`) {
		t.Fatalf("replayed event: %q %q", id, data)
	}
	post("three")
	id, data = readEvent()
	if id == "" || !strings.Contains(data, `"event_id":"three"`) {
		t.Fatalf("live event: %q %q", id, data)
	}
	if _, err := storage.DeleteBefore("a", time.Now().Add(time.Minute), 10); err != nil {
		t.Fatal(err)
	}
	req, err = http.NewRequest(http.MethodGet, server.URL+"/api/logs/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+query)
	req.Header.Set("Last-Event-ID", cursor)
	expired, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer expired.Body.Close()
	if expired.StatusCode != http.StatusGone {
		t.Fatalf("expired replay: %d", expired.StatusCode)
	}
}

func TestLiveStreamOnlySendsCommittedNewEvents(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	defer storage.Close()
	query, ingest := strings.Repeat("q", 32), strings.Repeat("i", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(query)), DeploymentID: "a", Scope: auth.Query},
		{Digest: sha256.Sum256([]byte(ingest)), DeploymentID: "a", Scope: auth.Ingest},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequest(http.MethodGet, server.URL+"/api/logs/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+query)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != ": connected\n" {
		t.Fatalf("stream initialization: %q %v", line, err)
	}
	event := store.Event{SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "info", InstanceID: "one", App: "api", Message: "hello"}
	body, err := json.Marshal(map[string]any{"events": []store.Event{event}})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/api/logs/batch", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+ingest)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("ingest status %d", resp.StatusCode)
		}
	}
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data: ") {
			break
		}
	}
	if !strings.Contains(line, `"event_id":"one"`) {
		t.Fatalf("unexpected event: %q", line)
	}
	handler.live.mu.Lock()
	for sub := range handler.live.subscribers {
		if len(sub.events) != 0 {
			t.Error("duplicate retry was broadcast")
		}
	}
	handler.live.mu.Unlock()
}
