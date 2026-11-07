package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestSearchIntersectionPaginationAndIsolation(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now().Add(-time.Second)
	events := []Event{sample("a"), sample("b"), sample("c"), sample("d")}
	events[0].Message, events[1].Message = "Playback failed now", "FAILED playback again"
	events[2].Message, events[3].Message = "playback working", "failed playback"
	events[3].Level = "warn"
	if _, err := s.PutBatch("one", events); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("two", events); err != nil {
		t.Fatal(err)
	}
	request := SearchRequest{Start: start, End: time.Now().Add(time.Second), Fields: map[string]string{"app": "api", "level": "info"}, Text: "PLAYBACK failed", Limit: 1}
	first, cursor, err := s.Search(context.Background(), "one", request)
	if err != nil || len(first) != 1 || cursor == "" {
		t.Fatalf("first page: %+v %q %v", first, cursor, err)
	}
	request.Cursor = cursor
	second, _, err := s.Search(context.Background(), "one", request)
	if err != nil || len(second) != 1 || first[0].EventID == second[0].EventID {
		t.Fatalf("second page: %+v %v", second, err)
	}
	if _, _, err := s.Search(context.Background(), "two", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cross-tenant cursor: %v", err)
	}
	request.Text = "playback"
	if _, _, err := s.Search(context.Background(), "one", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("changed query cursor: %v", err)
	}
	request.Cursor = ""
	request.End = start
	if _, _, err := s.Search(context.Background(), "one", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid time range: %v", err)
	}
}

func TestSearchTaskQueueIsolationAndCursor(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now().Add(-time.Minute)
	events := []Event{sample("a"), sample("b"), sample("c"), sample("missing")}
	for i := range events {
		events[i].TaskID = "task-1"
	}
	events[0].Fields = map[string]any{"task_queue": "scan"}
	events[1].Fields = map[string]any{"task_queue": "probe"}
	events[2].Fields = map[string]any{"task_queue": "scan"}
	if _, err := s.PutBatch("one", events); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("two", events); err != nil {
		t.Fatal(err)
	}
	request := SearchRequest{Start: start, End: time.Now().Add(time.Minute), Fields: map[string]string{"task": "task-1", "task_queue": "scan"}, Limit: 1}
	first, cursor, err := s.Search(context.Background(), "one", request)
	if err != nil || len(first) != 1 || cursor == "" {
		t.Fatalf("first page: %+v %q %v", first, cursor, err)
	}
	request.Cursor = cursor
	second, _, err := s.Search(context.Background(), "one", request)
	if err != nil || len(second) != 1 || first[0].EventID == second[0].EventID {
		t.Fatalf("second page: %+v %v", second, err)
	}
	for _, event := range append(first, second...) {
		if event.DeploymentID != "one" || event.Fields["task_queue"] != "scan" {
			t.Fatalf("incorrect queue or deployment: %+v", event)
		}
	}
	request.Fields["task_queue"] = "probe"
	if _, _, err := s.Search(context.Background(), "one", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cursor accepted for different queue: %v", err)
	}
	request.Cursor = ""
	delete(request.Fields, "task")
	if _, _, err := s.Search(context.Background(), "one", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("queue without task accepted: %v", err)
	}
}

func TestSearchWithoutFiltersPagesInTimeOrderAndIsolatesTenants(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now().Add(-time.Minute)
	if _, err := s.PutBatch("one", []Event{sample("a"), sample("b"), sample("c")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("two", []Event{sample("other")}); err != nil {
		t.Fatal(err)
	}
	request := SearchRequest{Start: start, End: time.Now().Add(time.Minute), Limit: 2}
	first, cursor, err := s.Search(context.Background(), "one", request)
	if err != nil || len(first) != 2 || cursor == "" {
		t.Fatalf("first page: %+v %q %v", first, cursor, err)
	}
	request.Cursor = cursor
	second, next, err := s.Search(context.Background(), "one", request)
	if err != nil || len(second) != 1 || next != "" {
		t.Fatalf("second page: %+v %q %v", second, next, err)
	}
	seen := map[string]bool{}
	for _, event := range append(first, second...) {
		if event.DeploymentID != "one" || seen[event.EventID] {
			t.Fatalf("unexpected or repeated event: %+v", event)
		}
		seen[event.EventID] = true
	}
	if _, _, err := s.Search(context.Background(), "two", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cross-tenant cursor: %v", err)
	}
	request.Cursor = ""
	request.End = start.Add(time.Second)
	events, _, err := s.Search(context.Background(), "one", request)
	if err != nil || len(events) != 0 {
		t.Fatalf("out-of-window events: %+v %v", events, err)
	}
}

func TestSearchOldestPaginationAndOrderBoundCursor(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now().Add(-time.Minute)
	for _, id := range []string{"first", "second", "third"} {
		event := sample(id)
		event.TaskID = "task"
		event.Fields = map[string]any{"task_queue": "scan"}
		if _, err := s.PutBatch("one", []Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	request := SearchRequest{Start: start, End: time.Now().Add(time.Minute), Fields: map[string]string{"task": "task", "task_queue": "scan"}, Order: "oldest", Limit: 1}
	for i, expected := range []string{"first", "second", "third"} {
		page, cursor, err := s.Search(context.Background(), "one", request)
		if err != nil || len(page) != 1 || page[0].EventID != expected {
			t.Fatalf("page %d: %+v %q %v", i, page, cursor, err)
		}
		if i < 2 && cursor == "" {
			t.Fatalf("page %d missing cursor", i)
		}
		if i == 0 {
			other := request
			other.Cursor, other.Order = cursor, "newest"
			if _, _, err := s.Search(context.Background(), "one", other); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("oldest cursor accepted for newest search: %v", err)
			}
		}
		if i == 2 && cursor != "" {
			t.Fatalf("unexpected final cursor %q", cursor)
		}
		request.Cursor = cursor
	}
	request.Cursor = ""
	request.Order = "ascending"
	if _, _, err := s.Search(context.Background(), "one", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid order accepted: %v", err)
	}
}

func TestSearchFollowResumesAfterLastAvailableEvent(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	request := SearchRequest{
		Start: time.Now().Add(-time.Minute), End: time.Now().Add(time.Hour), Limit: 10,
		Order: "oldest", Follow: true, Fields: map[string]string{"task": "task"},
	}
	first := sample("first")
	first.TaskID = "task"
	if _, err := s.PutBatch("one", []Event{first}); err != nil {
		t.Fatal(err)
	}
	events, cursor, err := s.Search(context.Background(), "one", request)
	if err != nil || len(events) != 1 || cursor == "" {
		t.Fatalf("initial page: %+v %q %v", events, cursor, err)
	}
	request.Cursor = cursor
	events, next, err := s.Search(context.Background(), "one", request)
	if err != nil || len(events) != 0 || next != "" {
		t.Fatalf("empty page: %+v %q %v", events, next, err)
	}
	second := sample("second")
	second.TaskID = "task"
	if _, err := s.PutBatch("one", []Event{second}); err != nil {
		t.Fatal(err)
	}
	events, next, err = s.Search(context.Background(), "one", request)
	if err != nil || len(events) != 1 || events[0].EventID != "second" || next == "" || next == cursor {
		t.Fatalf("follow-up page: %+v %q %v", events, next, err)
	}
	request.Cursor = ""
	request.Order = "newest"
	if _, _, err := s.Search(context.Background(), "one", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("newest follow accepted: %v", err)
	}
}

func TestSearchPhraseAndSubstring(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now().Add(-time.Minute)
	events := []Event{sample("a"), sample("b"), sample("c")}
	events[0].Message = "Playback failed immediately"
	events[1].Message = "Playback has failed"
	events[2].Message = "Unrelated"
	events[2].Error = &ErrorDetails{Stack: "connection timeout"}
	if _, err := s.PutBatch("one", events); err != nil {
		t.Fatal(err)
	}
	request := SearchRequest{Start: start, End: time.Now().Add(time.Minute), Text: "playback failed", TextMode: "phrase", Limit: 10}
	found, _, err := s.Search(context.Background(), "one", request)
	if err != nil || len(found) != 1 || found[0].EventID != "a" {
		t.Fatalf("phrase: %+v %v", found, err)
	}
	request.Text, request.TextMode = "back fail", "substring"
	found, _, err = s.Search(context.Background(), "one", request)
	if err != nil || len(found) != 1 || found[0].EventID != "a" {
		t.Fatalf("substring: %+v %v", found, err)
	}
	request.Text = "nection time"
	found, _, err = s.Search(context.Background(), "one", request)
	if err != nil || len(found) != 1 || found[0].EventID != "c" {
		t.Fatalf("error substring: %+v %v", found, err)
	}
	request.TextMode = "unknown"
	if _, _, err := s.Search(context.Background(), "one", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid mode accepted: %v", err)
	}
}

func TestSearchCandidateBudgetAndCancellation(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now().Add(-time.Second)
	for batch := 0; batch < 11; batch++ {
		n := 1000
		if batch == 10 {
			n = 1
		}
		events := make([]Event, n)
		for i := range events {
			events[i] = sample(fmt.Sprintf("%d-%d", batch, i))
		}
		if _, err := s.PutBatch("a", events); err != nil {
			t.Fatal(err)
		}
	}
	request := SearchRequest{Start: start, End: time.Now().Add(time.Second), Fields: map[string]string{"app": "api"}, Text: "missing", Limit: 10}
	// A selective trace index avoids scanning a broad application index.
	selective := SearchRequest{Start: start, End: request.End, Fields: map[string]string{"app": "api", "trace": "missing"}, Limit: 10}
	if events, _, err := s.Search(context.Background(), "a", selective); err != nil || len(events) != 0 {
		t.Fatalf("selective query: %+v %v", events, err)
	}
	// A common text token with a nonmatching application exhausts the budget.
	request.Text = "ready"
	request.Fields["app"] = "missing"
	if _, _, err := s.Search(context.Background(), "a", request); !errors.Is(err, ErrQueryBudget) {
		t.Fatalf("expected query budget error: %v", err)
	}
	request.Text, request.TextMode = "^does-not-match$", "regex"
	request.Fields["app"] = "api"
	if _, _, err := s.Search(context.Background(), "a", request); !errors.Is(err, ErrQueryBudget) {
		t.Fatalf("expected regex query budget error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.Search(ctx, "a", request); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled query: %v", err)
	}
}
