// Package retention runs bounded background cleanup of configured tenants.
package retention

import (
	"context"
	"log/slog"
	"time"

	"github.com/flixermedia/flixer-logger/internal/store"
)

const (
	pageSize  = 100
	batchSize = 500
	interval  = time.Minute
)

// Run stops before returning when ctx is canceled. Each pass touches at most
// one bounded page of deployments and one bounded batch per deployment.
func Run(ctx context.Context, storage *store.Store) {
	RunWithTracker(ctx, storage, nil)
}

// RunWithTracker reports bounded per-deployment sweep outcomes when a tracker is supplied.
func RunWithTracker(ctx context.Context, storage *store.Store, tracker *Tracker) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	runWithTracker(ctx, storage, ticker.C, tracker)
}

func run(ctx context.Context, storage *store.Store, ticks <-chan time.Time) {
	runWithTracker(ctx, storage, ticks, nil)
}

func runWithTracker(ctx context.Context, storage *store.Store, ticks <-chan time.Time, tracker *Tracker) {
	cursor := ""
	for {
		select {
		case <-ctx.Done():
			return
		case now, ok := <-ticks:
			if !ok {
				return
			}
			deployments, next, err := storage.RetentionDeployments(cursor, pageSize)
			if err != nil {
				slog.Error("retention policy listing failed", "error", err)
				continue
			}
			for _, deployment := range deployments {
				if ctx.Err() != nil {
					return
				}
				deleted, err := storage.SweepRetention(deployment, now, batchSize)
				if tracker != nil {
					tracker.Record(deployment, SweepStatus{CompletedAt: time.Now().UTC(), Deleted: deleted, BatchFull: deleted == batchSize, Failed: err != nil})
				}
				if err != nil {
					slog.Error("retention sweep failed", "deployment", deployment, "error", err)
				}
			}
			cursor = next
		}
	}
}
