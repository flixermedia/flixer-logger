package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestSourceStatsRejectsExcessCardinalityAndCanceledQueries(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now().UTC().Truncate(time.Minute)
	events := make([]Event, 1000)
	for i := range events {
		events[i] = sample(fmt.Sprintf("event-%d", i))
		events[i].Source = fmt.Sprintf("source-%d", i)
	}
	if _, err := s.PutBatch("a", events); err != nil {
		t.Fatal(err)
	}
	extra := sample("extra")
	extra.Source = "extra"
	if _, err := s.PutBatch("a", []Event{extra}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SourceStats(context.Background(), "a", start, start.Add(2*time.Minute)); !errors.Is(err, ErrQueryBudget) {
		t.Fatalf("expected cardinality limit, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SourceStats(ctx, "a", start, start.Add(2*time.Minute)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled query, got %v", err)
	}
}
