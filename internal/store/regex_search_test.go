package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRegexSearchIsolationPaginationAndValidation(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now().Add(-time.Minute)
	events := []Event{sample("one"), sample("two"), sample("three")}
	events[0].Message = "Playback failed: 500"
	events[1].Message = "playback failed: 503"
	events[2].Message = "unrelated"
	events[2].Error = &ErrorDetails{Stack: "connection timeout"}
	for _, tenant := range []string{"a", "b"} {
		if _, err := s.PutBatch(tenant, events); err != nil {
			t.Fatal(err)
		}
	}
	request := SearchRequest{
		Start: start, End: time.Now().Add(time.Minute), Fields: map[string]string{"app": "api"},
		Text: `(?i)playback failed: 50[03]`, TextMode: "regex", Limit: 1,
	}
	first, cursor, err := s.Search(context.Background(), "a", request)
	if err != nil || len(first) != 1 || cursor == "" || first[0].DeploymentID != "a" {
		t.Fatalf("first page: %+v %q %v", first, cursor, err)
	}
	request.Cursor = cursor
	second, _, err := s.Search(context.Background(), "a", request)
	if err != nil || len(second) != 1 || second[0].EventID == first[0].EventID || second[0].DeploymentID != "a" {
		t.Fatalf("second page: %+v %v", second, err)
	}
	if _, _, err := s.Search(context.Background(), "b", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cross-tenant cursor: %v", err)
	}
	request.Text = `different`
	if _, _, err := s.Search(context.Background(), "a", request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("changed pattern cursor: %v", err)
	}
	request.Cursor, request.Text = "", `connection\s+timeout`
	found, _, err := s.Search(context.Background(), "a", request)
	if err != nil || len(found) != 1 || found[0].EventID != "three" {
		t.Fatalf("error stack regex: %+v %v", found, err)
	}
	request.Text = `^playback`
	found, _, err = s.Search(context.Background(), "a", request)
	if err != nil || len(found) != 1 || found[0].EventID != "two" {
		t.Fatalf("case-sensitive regex: %+v %v", found, err)
	}
	for _, invalid := range []string{"[", strings.Repeat("ü", 129)} {
		request.Text = invalid
		if _, _, err := s.Search(context.Background(), "a", request); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("invalid pattern accepted: %v", err)
		}
	}
}
