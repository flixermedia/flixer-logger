package store

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkConcurrentBatchIngestion(b *testing.B) {
	storage, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()
	var sequence atomic.Uint64
	b.ReportAllocs()
	b.SetParallelism(2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			batchID := sequence.Add(1)
			events := make([]Event, 100)
			for i := range events {
				events[i] = Event{
					SchemaVersion: 1, EventID: fmt.Sprintf("%d-%d", batchID, i), Time: time.Now().UTC(),
					Level: "info", InstanceID: "node-1", App: "flixer", Source: "benchmark",
					Message: "media playback request completed successfully",
				}
			}
			if _, err := storage.PutBatch("benchmark", events); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	if b.Elapsed() > 0 {
		b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "events/s")
	}
}
