package store

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestCapacityReportsFilesystemAndPebble(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	capacity, err := storage.Capacity()
	if err != nil {
		t.Fatal(err)
	}
	if capacity.FilesystemTotalBytes == 0 || capacity.FilesystemAvailableBytes > capacity.FilesystemTotalBytes ||
		capacity.DatabaseBytes == 0 || capacity.CompactionsInProgress < 0 {
		t.Fatalf("invalid capacity: %+v", capacity)
	}
}

func TestCapacityPolicyPersistsAndAllowsQueriesAndRetention(t *testing.T) {
	dir := t.TempDir()
	storage, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	event := Event{SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "info", InstanceID: "node", App: "flixer", Message: "ready"}
	if _, err := storage.PutBatch("tenant", []Event{event}); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetCapacityPolicy(CapacityPolicy{MaxDatabaseBytes: 1}); err != nil {
		t.Fatal(err)
	}
	event.EventID = "two"
	if _, err := storage.PutBatch("tenant", []Event{event}); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("capacity did not reject new event: %v", err)
	}
	event.EventID = "one"
	if count, err := storage.PutBatch("tenant", []Event{event}); err != nil || count != 0 {
		t.Fatalf("duplicate retry failed: %d %v", count, err)
	}
	if got, err := storage.Recent("tenant", 10); err != nil || len(got) != 1 {
		t.Fatalf("query failed: %v %v", got, err)
	}
	if err := storage.SetRetentionPolicy("tenant", RetentionPolicy{Enabled: true, Days: 1}); err != nil {
		t.Fatal(err)
	}
	if count, err := storage.SweepRetention("tenant", time.Now().Add(48*time.Hour), 10); err != nil || count != 1 {
		t.Fatalf("retention failed under capacity guard: %d %v", count, err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if got := storage.CapacityPolicy(); got.MaxDatabaseBytes != 1 {
		t.Fatalf("policy did not survive restart: %+v", got)
	}
	if err := storage.SetCapacityPolicy(CapacityPolicy{}); err != nil {
		t.Fatal(err)
	}
	event.EventID = "three"
	if count, err := storage.PutBatch("tenant", []Event{event}); err != nil || count != 1 {
		t.Fatalf("ingest did not recover: %d %v", count, err)
	}
}

func TestFreeSpaceThresholdAndInvalidPolicy(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if err := storage.SetCapacityPolicy(CapacityPolicy{MinFreeBytes: 1 << 50}); err != nil {
		t.Fatal(err)
	}
	event := Event{SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "info", InstanceID: "node", App: "flixer", Message: "ready"}
	if capacity, err := storage.Capacity(); err != nil {
		t.Fatal(err)
	} else if capacity.FilesystemAvailableBytes <= 1<<50 {
		if _, err := storage.PutBatch("tenant", []Event{event}); !errors.Is(err, ErrCapacityExceeded) {
			t.Fatalf("free-space threshold not enforced: %v", err)
		}
	}
	if err := storage.SetCapacityPolicy(CapacityPolicy{MaxDatabaseBytes: 1<<50 + 1}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid threshold accepted: %v", err)
	}
}

func TestFilesystemBytesOverflowAndInvalidBlockSize(t *testing.T) {
	if value, err := filesystemBytes(12, 4096); err != nil || value != 49152 {
		t.Fatalf("unexpected capacity: %d %v", value, err)
	}

	t.Run("capacity threshold boundaries", func(t *testing.T) {
		capacity := Capacity{DatabaseBytes: 100, FilesystemAvailableBytes: 1000}
		for _, tc := range []struct {
			name    string
			policy  CapacityPolicy
			batch   uint64
			allowed bool
		}{
			{"disabled", CapacityPolicy{}, math.MaxUint64, true},
			{"database at limit", CapacityPolicy{MaxDatabaseBytes: 100}, 1, false},
			{"database exactly fits", CapacityPolicy{MaxDatabaseBytes: 110}, 10, true},
			{"database exceeds", CapacityPolicy{MaxDatabaseBytes: 110}, 11, false},
			{"free space exactly fits", CapacityPolicy{MinFreeBytes: 990}, 10, true},
			{"free space exceeded", CapacityPolicy{MinFreeBytes: 990}, 11, false},
			{"free space at reserve", CapacityPolicy{MinFreeBytes: 1000}, 1, false},
			{"large batch cannot overflow", CapacityPolicy{MaxDatabaseBytes: 110}, math.MaxUint64, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if got := capacityAllows(tc.policy, capacity, tc.batch); got != tc.allowed {
					t.Fatalf("allowed=%t, want %t", got, tc.allowed)
				}
			})
		}
	})
	for _, blockSize := range []int64{0, -1} {
		if _, err := filesystemBytes(1, blockSize); err == nil {
			t.Fatal("invalid block size accepted")
		}
	}
	if _, err := filesystemBytes(math.MaxUint64, 4096); err == nil {
		t.Fatal("overflow accepted")
	}
}
