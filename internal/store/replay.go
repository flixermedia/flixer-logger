package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble"
)

var ErrReplayUnavailable = errors.New("live replay cursor is no longer available")

// LiveCursor identifies a committed event in its deployment's ordered stream.
func LiveCursor(event Event) string {
	key := fmt.Sprintf("%s%020d/%s", prefix(event.DeploymentID), event.IngestedAt.UnixNano(), segment(event.EventID))
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

// ReplaySnapshot pins a consistent view while a subscriber catches up.
type ReplaySnapshot struct {
	snapshot   *pebble.Snapshot
	deployment string
	start      []byte
}

func (s *Store) NewReplaySnapshot(deployment, cursor string) (*ReplaySnapshot, error) {
	if deployment == "" || cursor == "" || len(cursor) > 1024 {
		return nil, fmt.Errorf("%w: invalid live cursor", ErrInvalidInput)
	}
	key, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || !strings.HasPrefix(string(key), prefix(deployment)) {
		return nil, fmt.Errorf("%w: invalid live cursor", ErrInvalidInput)
	}
	snapshot := s.db.NewSnapshot()
	_, closer, err := snapshot.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		snapshot.Close()
		return nil, ErrReplayUnavailable
	}
	if err != nil {
		snapshot.Close()
		return nil, err
	}
	if err := closer.Close(); err != nil {
		snapshot.Close()
		return nil, err
	}
	return &ReplaySnapshot{snapshot: snapshot, deployment: deployment, start: key}, nil
}

func (r *ReplaySnapshot) Close() error { return r.snapshot.Close() }

// Events returns at most limit events, failing rather than silently truncating.
func (r *ReplaySnapshot) Events(ctx context.Context, limit int) ([]Event, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("%w: invalid replay limit", ErrInvalidInput)
	}
	p := prefix(r.deployment)
	iter, err := r.snapshot.NewIter(&pebble.IterOptions{LowerBound: r.start, UpperBound: []byte(p + "~")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	events := make([]Event, 0)
	size := 0
	if iter.SeekGE(r.start) {
		for valid := iter.Next(); valid; valid = iter.Next() {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if len(events) == limit {
				return nil, ErrReplayUnavailable
			}
			size += len(iter.Value())
			if size > 8*1024*1024 {
				return nil, ErrReplayUnavailable
			}
			var event Event
			if err := json.Unmarshal(iter.Value(), &event); err != nil {
				return nil, fmt.Errorf("decode replay event: %w", err)
			}
			events = append(events, event)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return events, nil
}
