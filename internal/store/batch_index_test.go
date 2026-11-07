package store

import (
	"strings"
	"testing"
	"time"
)

func TestBatchIndexKeysMatchQueriesAndRetention(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	longToken := strings.Repeat("x", maxInlineTokenSize+1)
	first, second := sample("first"), sample("second")
	first.Message = "café " + longToken
	first.Source = "index\x00text"
	second.Message = first.Message
	second.Source = first.Source
	if _, err := s.PutBatch("tenant", []Event{first, second}); err != nil {
		t.Fatal(err)
	}
	for _, field := range []struct{ name, value string }{
		{"source", first.Source}, {"text", "café"}, {"text", longToken},
	} {
		got, _, err := s.QueryIndex("tenant", field.name, field.value, "", 10)
		if err != nil || len(got) != 2 {
			t.Fatalf("query %s: got %d events, error %v", field.name, len(got), err)
		}
	}
	if deleted, err := s.DeleteBefore("tenant", time.Now().Add(time.Hour), 10); err != nil || deleted != 2 {
		t.Fatalf("delete: %d, %v", deleted, err)
	}
	for _, token := range []string{"café", longToken} {
		got, _, err := s.QueryIndex("tenant", "text", token, "", 10)
		if err != nil || len(got) != 0 {
			t.Fatalf("retained text index %q: %v, %v", token, got, err)
		}
	}
}
