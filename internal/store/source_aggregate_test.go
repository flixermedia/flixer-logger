package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestSourceStatsIsolationRetentionAndRebuild(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	start := time.Now().UTC().Truncate(time.Minute)
	end := start.Add(2 * time.Minute)
	for _, entry := range []struct{ id, app, source string }{
		{"one", "api", "scanner"}, {"two", "api", "scanner"}, {"three", "indexer", "scanner"}, {"four", "api", ""},
	} {
		event := sample(entry.id)
		event.App, event.Source = entry.app, entry.source
		if _, err := s.PutBatch("a", []Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.PutBatch("b", []Event{sample("other")}); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		stats, err := s.SourceStats(context.Background(), "a", start, end)
		if err != nil || len(stats) != 3 || stats[0] != (SourceCount{App: "api", Source: "scanner", Count: 2}) {
			t.Fatalf("unexpected source stats: %+v %v", stats, err)
		}
		other, err := s.SourceStats(context.Background(), "b", start, end)
		if err != nil || len(other) != 1 || other[0].Count != 1 {
			t.Fatalf("incorrect tenant stats: %+v %v", other, err)
		}
	}
	check()
	if err := s.db.Delete([]byte(sourceAggregateMarker), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s = reopened
	check()
	if err := s.SetRetentionPolicy("a", RetentionPolicy{Enabled: true, Days: 1, SeverityDays: map[string]int{"info": 2}}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.SweepRetention("a", time.Now().Add(3*24*time.Hour), 10); err != nil || n != 4 {
		t.Fatalf("severity retention: %d %v", n, err)
	}
	stats, err := s.SourceStats(context.Background(), "a", start, end)
	if err != nil || len(stats) != 0 {
		t.Fatalf("expired source counts remain: %+v %v", stats, err)
	}
	if _, err := s.SourceStats(context.Background(), "b", start, start.Add(32*24*time.Hour)); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid range accepted: %v", err)
	}
}
