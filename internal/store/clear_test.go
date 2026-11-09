package store

import (
	"context"
	"testing"
	"time"
)

func TestClearDeploymentRemovesHistoryAndDerivedDataOnly(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	event := sample("same")
	event.Level = "error"
	event.Source = "worker"
	event.TraceID = "trace"
	event.Message = "failure"
	if _, err := s.PutBatch("a", []Event{event}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("b", []Event{event}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetQuotaPolicy("a", QuotaPolicy{MaxBytes: 1_000_000}); err != nil {
		t.Fatal(err)
	}
	if usage, err := s.LogicalUsage("a"); err != nil || usage == 0 {
		t.Fatalf("expected nonzero initial usage, got %d, %v", usage, err)
	}

	cleared, err := s.ClearDeployment("a")
	if err != nil || cleared != 1 {
		t.Fatalf("clear: %d, %v", cleared, err)
	}
	if events, err := s.Recent("a", 10); err != nil || len(events) != 0 {
		t.Fatalf("deployment a history: %+v, %v", events, err)
	}
	for _, field := range []string{"trace", "text"} {
		value := "trace"
		if field == "text" {
			value = "failure"
		}
		if events, _, err := s.QueryIndex("a", field, value, "", 10); err != nil || len(events) != 0 {
			t.Fatalf("deployment a %s index: %+v, %v", field, events, err)
		}
	}
	if usage, err := s.LogicalUsage("a"); err != nil || usage != 0 {
		t.Fatalf("deployment a usage: %d, %v", usage, err)
	}
	if policy, err := s.QuotaPolicy("a"); err != nil || policy.MaxBytes != 1_000_000 {
		t.Fatalf("deployment a quota policy: %+v, %v", policy, err)
	}
	if events, err := s.Recent("b", 10); err != nil || len(events) != 1 {
		t.Fatalf("deployment b history: %+v, %v", events, err)
	}

	end := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
	start := end.Add(-24 * time.Hour)
	for _, tenant := range []string{"a", "b"} {
		buckets, err := s.LevelStats(context.Background(), tenant, start, end)
		if err != nil {
			t.Fatal(err)
		}
		sources, err := s.SourceStats(context.Background(), tenant, start, end)
		if err != nil {
			t.Fatal(err)
		}
		want := 1
		if tenant == "a" {
			want = 0
		}
		if len(buckets) != want || len(sources) != want {
			t.Fatalf("deployment %s aggregates: buckets=%+v sources=%+v", tenant, buckets, sources)
		}
	}
}
