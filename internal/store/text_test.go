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

func TestRebuildTextIndexForExistingDatabase(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	event := sample("legacy")
	longToken := strings.Repeat("x", maxInlineTokenSize+1)
	parts := make([]string, 500)
	for i := range parts {
		parts[i] = fmt.Sprintf("token%d", i)
	}
	event.Message = "old playback failure " + longToken + " " + strings.Join(parts, " ")
	written, err := s.PutBatchEvents("deployment", []Event{event})
	if err != nil {
		t.Fatal(err)
	}
	batch := s.db.NewBatch()
	if err := batch.Delete([]byte("metadata/text-index-v2"), nil); err != nil {
		t.Fatal(err)
	}
	tokens, err := eventTextTokens(written[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range tokens {
		if err := batch.Delete(indexKey(written[0], "text", token), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
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
	for _, token := range []string{"playback", "token499", longToken} {
		matches, _, err := s.QueryIndex("deployment", "text", token, "", 10)
		if err != nil || len(matches) != 1 || matches[0].EventID != "legacy" {
			t.Fatalf("rebuild failed for %q: %+v %v", token, matches, err)
		}
	}
}

func TestTextIndexPaginationRetentionAndIsolation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	a, b := sample("a"), sample("b")
	a.Message = "Playback FAILED: failed to load"
	b.Message = "failed café"
	if _, err := s.PutBatch("one", []Event{a, b}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("two", []Event{a}); err != nil {
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
	first, cursor, err := s.QueryIndex("one", "text", "FAILED", "", 1)
	if err != nil || len(first) != 1 || cursor == "" {
		t.Fatalf("first page: %+v %q %v", first, cursor, err)
	}
	second, next, err := s.QueryIndex("one", "text", "failed", cursor, 1)
	if err != nil || len(second) != 1 || next != "" || second[0].EventID == first[0].EventID {
		t.Fatalf("second page: %+v %q %v", second, next, err)
	}
	if _, _, err := s.QueryIndex("two", "text", "failed", cursor, 1); err == nil {
		t.Fatal("cross-deployment cursor accepted")
	}
	unicodeMatches, _, err := s.QueryIndex("one", "text", "CAFÉ", "", 10)
	if err != nil || len(unicodeMatches) != 1 || unicodeMatches[0].EventID != "b" {
		t.Fatalf("unicode search: %+v %v", unicodeMatches, err)
	}
	for _, invalid := range []string{"failed playback", "fail*", "failed!", ""} {
		if _, _, err := s.QueryIndex("one", "text", invalid, "", 10); err == nil {
			t.Fatalf("invalid query accepted: %q", invalid)
		}
	}
	if n, err := s.DeleteBefore("one", time.Now().Add(time.Second), 10); err != nil || n != 2 {
		t.Fatalf("retention: %d %v", n, err)
	}
	matches, _, err := s.QueryIndex("one", "text", "failed", "", 10)
	if err != nil || len(matches) != 0 {
		t.Fatalf("stale text postings: %+v %v", matches, err)
	}
	other, _, err := s.QueryIndex("two", "text", "failed", "", 10)
	if err != nil || len(other) != 1 {
		t.Fatalf("other tenant affected: %+v %v", other, err)
	}
}

func TestLongAndHighCardinalityTextIsIndexedAndRetained(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	longToken := strings.Repeat("x", maxInlineTokenSize+1)
	parts := make([]string, 500)
	for i := range parts {
		parts[i] = fmt.Sprintf("token%d", i)
	}
	event := sample("large")
	event.Message = strings.Join(parts, " ") + " " + longToken
	event.Error = &ErrorDetails{Stack: "stacktoken"}
	start := time.Now().Add(-time.Minute)
	if _, err := s.PutBatch("a", []Event{event}); err != nil {
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
	for _, token := range []string{"token499", strings.ToUpper(longToken), "stacktoken"} {
		matches, _, err := s.QueryIndex("a", "text", token, "", 10)
		if err != nil || len(matches) != 1 || matches[0].EventID != event.EventID {
			t.Fatalf("index lookup %q: %+v %v", token, matches, err)
		}
	}
	request := SearchRequest{Start: start, End: time.Now().Add(time.Minute), Text: "token499 " + longToken, Limit: 10}
	matches, _, err := s.Search(context.Background(), "a", request)
	if err != nil || len(matches) != 1 || matches[0].EventID != event.EventID {
		t.Fatalf("intersected search: %+v %v", matches, err)
	}
	if n, err := s.DeleteBefore("a", time.Now().Add(time.Second), 10); err != nil || n != 1 {
		t.Fatalf("retention: %d %v", n, err)
	}
	matches, _, err = s.QueryIndex("a", "text", longToken, "", 10)
	if err != nil || len(matches) != 0 {
		t.Fatalf("stale hashed posting: %+v %v", matches, err)
	}
}

func TestBatchTextIndexBudgetIsAtomic(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	parts := make([]string, 1000)
	for i := range parts {
		parts[i] = fmt.Sprintf("token%d", i)
	}
	events := make([]Event, maxBatchTextTokens/len(parts)+1)
	for i := range events {
		events[i] = sample(fmt.Sprintf("event%d", i))
		events[i].Message = strings.Join(parts, " ")
	}
	if _, err := s.PutBatch("a", events); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected batch index budget error: %v", err)
	}
	stored, err := s.Recent("a", 10)
	if err != nil || len(stored) != 0 {
		t.Fatalf("over-budget batch wrote records: %+v %v", stored, err)
	}
	if n, err := s.PutBatch("a", events[:1]); err != nil || n != 1 {
		t.Fatalf("smaller batch failed: %d %v", n, err)
	}
}
