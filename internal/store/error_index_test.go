package store

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestErrorIndexRebuildFromPriorVersion(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	event := sample("one")
	event.Error = &ErrorDetails{Type: "UpstreamError", Stack: "timeout"}
	inserted, err := s.PutBatchEvents("a", []Event{event})
	if err != nil {
		t.Fatal(err)
	}
	batch := s.db.NewBatch()
	if err := batch.Delete([]byte("metadata/text-index-v2"), nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Set([]byte("metadata/text-index-v1"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Delete(indexKey(inserted[0], "text", "timeout"), nil); err != nil {
		t.Fatal(err)
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
	matches, _, err := s.QueryIndex("a", "text", "timeout", "", 10)
	if err != nil || len(matches) != 1 {
		t.Fatalf("error index was not rebuilt: %+v %v", matches, err)
	}
}
