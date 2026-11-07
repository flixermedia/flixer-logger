package store

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOpenRejectsUnsafeStoreDirectories(t *testing.T) {
	t.Run("world writable parent", func(t *testing.T) {
		dir := t.TempDir()
		logs := filepath.Join(dir, "logs")
		if err := os.Mkdir(logs, 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(logs, 0o777); err != nil {
			t.Fatal(err)
		}
		if s, err := Open(dir); err == nil {
			s.Close()
			t.Fatal("world-writable log directory accepted")
		}
	})
	t.Run("symlinked log directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Symlink(t.TempDir(), filepath.Join(dir, "logs")); err != nil {
			t.Fatal(err)
		}
		if s, err := Open(dir); err == nil {
			s.Close()
			t.Fatal("symlinked log directory accepted")
		}
	})
	t.Run("public logger directory", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "logs", "logger")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if s, err := Open(dir); err == nil {
			s.Close()
			t.Fatal("public logger directory accepted")
		}
	})
}

func sample(id string) Event {
	return Event{SchemaVersion: 1, EventID: id, Time: time.Now(), Level: "info", InstanceID: "one", App: "api", Message: "ready"}
}

func TestDurabilityDeduplicationAndIsolation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e := sample("same")
	e.DeploymentID = "spoofed"
	if n, err := s.PutBatch("a", []Event{e, e}); err != nil || n != 1 {
		t.Fatalf("first batch: %d %v", n, err)
	}
	if n, err := s.PutBatch("a", []Event{e}); err != nil || n != 0 {
		t.Fatalf("retry: %d %v", n, err)
	}
	if n, err := s.PutBatch("b", []Event{e}); err != nil || n != 1 {
		t.Fatalf("other deployment: %d %v", n, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, tenant := range []string{"a", "b"} {
		events, err := s.Recent(tenant, 10)
		if err != nil || len(events) != 1 || events[0].DeploymentID != tenant || events[0].IngestedAt.IsZero() {
			t.Fatalf("deployment %s: %+v %v", tenant, events, err)
		}
	}
	if second, err := Open(dir); err == nil {
		second.Close()
		t.Fatal("opening the same database twice unexpectedly succeeded")
	}
}

func TestInvalidBatchDoesNotWrite(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	bad := sample("bad")
	bad.Level = "invalid"
	if _, err := s.PutBatch("a", []Event{sample("good"), bad}); err == nil {
		t.Fatal("expected invalid batch")
	}
	events, err := s.Recent("a", 10)
	if err != nil || len(events) != 0 {
		t.Fatalf("partial write: %+v %v", events, err)
	}
	oversized := sample("oversized")
	oversized.TraceID = strings.Repeat("x", 1025)
	if _, err := s.PutBatch("a", []Event{oversized}); err == nil {
		t.Fatal("expected oversized index value to be rejected")
	}
}

func TestIndexedPaginationAndRetentionIsolation(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, b := sample("first"), sample("second")
	a.TraceID, b.TraceID = "trace", "trace"
	if _, err := s.PutBatch("a", []Event{a, b}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("b", []Event{a}); err != nil {
		t.Fatal(err)
	}
	first, cursor, err := s.QueryIndex("a", "trace", "trace", "", 1)
	if err != nil || len(first) != 1 || cursor == "" {
		t.Fatalf("first page: %+v %q %v", first, cursor, err)
	}
	second, next, err := s.QueryIndex("a", "trace", "trace", cursor, 1)
	if err != nil || len(second) != 1 || next != "" || first[0].EventID == second[0].EventID {
		t.Fatalf("second page: %+v %q %v", second, next, err)
	}
	if _, _, err := s.QueryIndex("b", "trace", "trace", cursor, 1); err == nil {
		t.Fatal("cross-deployment cursor accepted")
	}
	cutoff := time.Now().Add(time.Second)
	if n, err := s.DeleteBefore("a", cutoff, 1); err != nil || n != 1 {
		t.Fatalf("first retention batch: %d %v", n, err)
	}
	if n, err := s.DeleteBefore("a", cutoff, 10); err != nil || n != 1 {
		t.Fatalf("second retention batch: %d %v", n, err)
	}
	remaining, _, err := s.QueryIndex("a", "trace", "trace", "", 10)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("stale index: %+v %v", remaining, err)
	}
	other, _, err := s.QueryIndex("b", "trace", "trace", "", 10)
	if err != nil || len(other) != 1 {
		t.Fatalf("other deployment affected: %+v %v", other, err)
	}
	if n, err := s.PutBatch("a", []Event{a}); err != nil || n != 1 {
		t.Fatalf("retained ID was not removed: %d %v", n, err)
	}
}

func TestIndexPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	event := sample("persisted")
	event.TaskID = "task-1"
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
	matches, _, err := s.QueryIndex("a", "task", "task-1", "", 10)
	if err != nil || len(matches) != 1 || matches[0].EventID != "persisted" {
		t.Fatalf("index did not survive reopen: %+v %v", matches, err)
	}
}

func TestConcurrentRetries(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.PutBatch("a", []Event{sample("same")}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	events, err := s.Recent("a", 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("duplicates: %+v %v", events, err)
	}
}
