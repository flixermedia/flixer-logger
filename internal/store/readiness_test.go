package store

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestReadyChecksMetadata(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Ready(); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Delete([]byte(aggregateMarker), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.Ready(); err == nil {
		t.Fatal("missing metadata accepted")
	}
}
