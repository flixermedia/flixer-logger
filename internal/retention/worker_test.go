package retention

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestWorkerSweepsEnabledPolicyAndStops(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if err := storage.SetRetentionPolicy("a", store.RetentionPolicy{Enabled: true, Days: 14}); err != nil {
		t.Fatal(err)
	}
	events := make([]store.Event, batchSize+1)
	for i := range events {
		events[i] = store.Event{SchemaVersion: 1, EventID: fmt.Sprint(i), Time: time.Now(), Level: "info", App: "api", InstanceID: "one", Message: "ready"}
	}
	if _, err := storage.PutBatch("a", events); err != nil {
		t.Fatal(err)
	}
	ticks := make(chan time.Time, 1)
	ticks <- time.Now().Add(15 * 24 * time.Hour)
	close(ticks)
	tracker := NewTracker(2)
	runWithTracker(context.Background(), storage, ticks, tracker)
	if status := tracker.Get("a"); status == nil || status.Failed || !status.BatchFull || status.Deleted != batchSize || status.CompletedAt.IsZero() {
		t.Fatalf("unexpected first sweep status: %+v", status)
	}
	remaining, err := storage.Recent("a", 1000)
	if err != nil || len(remaining) != 1 {
		t.Fatalf("worker did not respect batch limit: %d %v", len(remaining), err)
	}
	ticks = make(chan time.Time, 1)
	ticks <- time.Now().Add(15 * 24 * time.Hour)
	close(ticks)
	runWithTracker(context.Background(), storage, ticks, tracker)
	if status := tracker.Get("a"); status == nil || status.Failed || status.BatchFull || status.Deleted != 1 {
		t.Fatalf("unexpected second sweep status: %+v", status)
	}
	remaining, err = storage.Recent("a", 10)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("worker did not complete retention: %d %v", len(remaining), err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(ctx, storage, make(chan time.Time))
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
}

func TestTrackerBoundsAndIsolatesStatus(t *testing.T) {
	tracker := NewTracker(2)
	tracker.Record("a", SweepStatus{Deleted: 1})
	tracker.Record("b", SweepStatus{Failed: true})
	tracker.Record("a", SweepStatus{Deleted: 2})
	tracker.Record("c", SweepStatus{Deleted: 3})
	if tracker.Get("b") != nil || tracker.Get("a").Deleted != 2 || tracker.Get("c").Deleted != 3 || tracker.Get("other") != nil {
		t.Fatal("tracker did not preserve the most recently recorded deployments")
	}
	copy := tracker.Get("a")
	copy.Deleted = 99
	if tracker.Get("a").Deleted != 2 {
		t.Fatal("caller mutated the stored status")
	}
}
