package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestLevelStatsDedupRetentionAndRebuild(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Minute)
	end := start.Add(2 * time.Minute)
	info, failure := sample("one"), sample("two")
	failure.Level = "error"
	if _, err := s.PutBatch("a", []Event{info, failure, info}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("a", []Event{info}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("b", []Event{info}); err != nil {
		t.Fatal(err)
	}
	check := func(store *Store, tenant string, info, failure uint64) {
		t.Helper()
		buckets, err := store.LevelStats(context.Background(), tenant, start, end)
		if err != nil || len(buckets) != 1 || buckets[0].Counts["info"] != info || buckets[0].Counts["error"] != failure {
			t.Fatalf("stats %s: %+v %v", tenant, buckets, err)
		}
	}
	check(s, "a", 1, 1)
	check(s, "b", 1, 0)
	if err := s.db.Delete([]byte(aggregateMarker), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	check(s, "a", 1, 1)
	if n, err := s.DeleteBefore("a", time.Now().Add(time.Second), 1); err != nil || n != 1 {
		t.Fatalf("retention: %d %v", n, err)
	}
	buckets, err := s.LevelStats(context.Background(), "a", start, end)
	if err != nil || len(buckets) != 1 || buckets[0].Counts["info"]+buckets[0].Counts["error"] != 1 {
		t.Fatalf("retention counts: %+v %v", buckets, err)
	}
	check(s, "b", 1, 0)
	if _, err := s.LevelStats(context.Background(), "a", start.Add(time.Second), end); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unaligned bounds accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.LevelStats(ctx, "a", start, end); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled query: %v", err)
	}
}
