package store

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestQuotaAtomicityDedupRetentionAndRebuild(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("a", []Event{sample("one")}); err != nil {
		t.Fatal(err)
	}
	usage, err := s.LogicalUsage("a")
	if err != nil || usage == 0 {
		t.Fatalf("usage: %d %v", usage, err)
	}
	if err := s.SetQuotaPolicy("a", QuotaPolicy{MaxBytes: usage + 32}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.PutBatch("a", []Event{sample("one")}); err != nil || n != 0 {
		t.Fatalf("duplicate retry at quota: %d %v", n, err)
	}
	if _, err := s.PutBatch("a", []Event{sample("two"), sample("three")}); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("over quota: %v", err)
	}
	if got, err := s.LogicalUsage("a"); err != nil || got != usage {
		t.Fatalf("rejected batch changed usage: %d %v", got, err)
	}
	if _, err := s.PutBatch("b", []Event{sample("other")}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Delete([]byte(quotaMarker), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Set(usageKey("stale"), []byte("100"), pebble.Sync); err != nil {
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
	if got, err := s.LogicalUsage("a"); err != nil || got != usage {
		t.Fatalf("rebuilt usage: %d %v", got, err)
	}
	if got, err := s.LogicalUsage("stale"); err != nil || got != 0 {
		t.Fatalf("stale rebuilt usage: %d %v", got, err)
	}
	if n, err := s.DeleteBefore("a", time.Now().Add(time.Minute), 10); err != nil || n != 1 {
		t.Fatalf("retention: %d %v", n, err)
	}
	if got, err := s.LogicalUsage("a"); err != nil || got != 0 {
		t.Fatalf("retention usage: %d %v", got, err)
	}
	if n, err := s.PutBatch("a", []Event{sample("one")}); err != nil || n != 1 {
		t.Fatalf("retention did not restore capacity: %d %v", n, err)
	}
}

func TestConcurrentQuotaEnforcement(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.PutBatch("a", []Event{sample("one")}); err != nil {
		t.Fatal(err)
	}
	usage, err := s.LogicalUsage("a")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetQuotaPolicy("a", QuotaPolicy{MaxBytes: usage*2 + 32}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, id := range []string{"two", "six"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if _, err := s.PutBatch("a", []Event{sample(id)}); err != nil && !errors.Is(err, ErrQuotaExceeded) {
				t.Error(err)
			}
		}(id)
	}
	wg.Wait()
	events, err := s.Recent("a", 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("quota was not enforced concurrently: %d %v", len(events), err)
	}
}
