package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointRestoresEventsIndexesAndRetentionPolicy(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := sample("one")
	e.Message = "restore checkpoint"
	if _, err := s.PutBatch("a", []Event{e}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRetentionPolicy("a", RetentionPolicy{Enabled: true, Days: 14}); err != nil {
		t.Fatal(err)
	}
	restoreRoot := t.TempDir()
	parent := filepath.Join(restoreRoot, "logs", "logger")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(parent, "pebble")
	if err := s.Checkpoint(destination); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("a", []Event{sample("later")}); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(restoreRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	events, _, err := restored.QueryIndex("a", "text", "checkpoint", "", 10)
	if err != nil || len(events) != 1 || events[0].EventID != "one" {
		t.Fatalf("restore index: %+v %v", events, err)
	}
	all, err := restored.Recent("a", 10)
	if err != nil || len(all) != 1 {
		t.Fatalf("unexpected snapshot contents: %+v %v", all, err)
	}
	if policy, err := restored.RetentionPolicy("a"); err != nil || !policy.Enabled || policy.Days != 14 {
		t.Fatalf("restore policy: %+v %v", policy, err)
	}
	if err := s.Checkpoint(destination); err == nil {
		t.Fatal("checkpoint overwrote existing snapshot")
	}
	if err := s.Checkpoint("relative"); err == nil {
		t.Fatal("relative checkpoint path accepted")
	}
}
