package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestReplaySnapshotOrderingIsolationAndRetention(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.PutBatchEvents("a", []Event{sample("z"), sample("a")})
	if err != nil {
		t.Fatal(err)
	}
	cursor := LiveCursor(first[0])
	if _, err := s.PutBatch("b", []Event{sample("other")}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.NewReplaySnapshot("a", cursor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("a", []Event{sample("later")}); err != nil {
		t.Fatal(err)
	}
	replayed, err := snapshot.Events(context.Background(), 1000)
	if err != nil || len(replayed) != 1 || replayed[0].EventID != first[1].EventID {
		t.Fatalf("snapshot replay: %+v %v", replayed, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.NewReplaySnapshot("b", cursor); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cross-tenant cursor: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	snapshot, err = s.NewReplaySnapshot("a", cursor)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err = snapshot.Events(context.Background(), 1000)
	if err != nil || len(replayed) != 2 || replayed[1].EventID != "later" {
		t.Fatalf("restart replay: %+v %v", replayed, err)
	}
	if _, err := snapshot.Events(context.Background(), 1); !errors.Is(err, ErrReplayUnavailable) {
		t.Fatalf("expected replay limit: %v", err)
	}
	snapshot.Close()
	if n, err := s.DeleteBefore("a", time.Now().Add(time.Minute), 10); err != nil || n != 3 {
		t.Fatalf("retention: %d %v", n, err)
	}
	if _, err := s.NewReplaySnapshot("a", cursor); !errors.Is(err, ErrReplayUnavailable) {
		t.Fatalf("expired cursor: %v", err)
	}
}

func TestReplayByteLimitDoesNotReturnPartialEvents(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first, err := s.PutBatchEvents("a", []Event{sample("base")})
	if err != nil {
		t.Fatal(err)
	}
	events := make([]Event, 150)
	for i := range events {
		events[i] = sample(fmt.Sprintf("large-%d", i))
		events[i].Message = strings.Repeat("x", 60_000)
	}
	if _, err := s.PutBatch("a", events); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.NewReplaySnapshot("a", LiveCursor(first[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	got, err := snapshot.Events(context.Background(), 1000)
	if !errors.Is(err, ErrReplayUnavailable) || got != nil {
		t.Fatalf("expected byte limit error without partial results: %d %v", len(got), err)
	}
}

func TestIngestionTimeRemainsMonotonicAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.lastIngestNS = time.Now().Add(time.Minute).UnixNano()
	first, err := s.PutBatchEvents("a", []Event{sample("one")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Delete([]byte("metadata/last-ingest-ns"), pebble.Sync); err != nil {
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
	second, err := s.PutBatchEvents("a", []Event{sample("two")})
	if err != nil || !second[0].IngestedAt.After(first[0].IngestedAt) {
		t.Fatalf("non-monotonic timestamps: %v %+v", err, second)
	}
	snapshot, err := s.NewReplaySnapshot("a", LiveCursor(first[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	events, err := snapshot.Events(context.Background(), 10)
	if err != nil || len(events) != 1 || events[0].EventID != "two" {
		t.Fatal(fmt.Sprintf("incorrect replay: %+v %v", events, err))
	}
}
