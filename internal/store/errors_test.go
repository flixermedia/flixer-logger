package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestErrorGroupsAreTenantScopedAndRanked(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	defer s.Close()
	start := time.Now().Add(-time.Minute)
	events := []Event{sample("a"), sample("b"), sample("c"), sample("d")}
	for i := range events {
		events[i].Level = "error"
		events[i].Message = "request failed"
		events[i].Error = &ErrorDetails{Type: "NetworkError", Code: "TIMEOUT"}
	}
	events[2].Level = "fatal"
	events[3].Message = "different failure"
	if _, err := s.PutBatch("one", events); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("other", events); err != nil {
		t.Fatal(err)
	}
	groups, err := s.ErrorGroups(context.Background(), "one", start, time.Now().Add(time.Minute))
	if err != nil || len(groups) != 2 || groups[0].Count != 3 || groups[1].Count != 1 ||
		groups[0].Type != "NetworkError" || groups[0].Code != "TIMEOUT" || groups[0].Fingerprint == "" {
		t.Fatalf("unexpected groups: %+v %v", groups, err)
	}
	empty, err := s.ErrorGroups(context.Background(), "missing", start, time.Now().Add(time.Minute))
	if err != nil || len(empty) != 0 {
		t.Fatalf("other deployment groups: %+v %v", empty, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ErrorGroups(ctx, "one", start, time.Now().Add(time.Minute)); err != context.Canceled {
		t.Fatalf("canceled grouping: %v", err)
	}
}

func TestErrorGroupsRejectsExcessiveCardinality(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now().Add(-time.Minute)
	events := make([]Event, 1000)
	for i := range events {
		events[i] = sample(fmt.Sprintf("id-%d", i))
		events[i].Level = "error"
		events[i].Message = fmt.Sprintf("failure number %d", i)
	}
	if _, err := s.PutBatch("one", events); err != nil {
		t.Fatal(err)
	}
	extra := sample("extra")
	extra.Level, extra.Message = "fatal", "one more failure"
	if _, err := s.PutBatch("one", []Event{extra}); err != nil {
		t.Fatal(err)
	}
	groups, err := s.ErrorGroups(context.Background(), "one", start, time.Now().Add(time.Minute))
	if !errors.Is(err, ErrQueryBudget) || groups != nil {
		t.Fatalf("expected explicit cardinality error, got %d groups: %v", len(groups), err)
	}
}
