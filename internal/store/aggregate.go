package store

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/pebble"
)

const aggregateMarker = "metadata/level-aggregate-v1"
const sourceAggregateMarker = "metadata/source-aggregate-v1"
const sourceAggregatePrefix = "aggregate-source/"

func aggregatePrefix(deployment string) string {
	return "aggregate/" + segment(deployment) + "/"
}

func aggregateKey(event Event) string {
	return fmt.Sprintf("%s%012d/%s", aggregatePrefix(event.DeploymentID), event.IngestedAt.Unix()/60, event.Level)
}

func sourceAggregateKey(event Event) string {
	return fmt.Sprintf("%s%s/%012d/%s/%s", sourceAggregatePrefix, segment(event.DeploymentID), event.IngestedAt.Unix()/60, segment(event.App), segment(event.Source))
}

func changeEventCounts(changes map[string]int64, event Event, delta int64) {
	changes[aggregateKey(event)] += delta
	changes[sourceAggregateKey(event)] += delta
}

// applyCounts runs under the store write lock (or before opening the store).
func applyCounts(db *pebble.DB, batch *pebble.Batch, changes map[string]int64) error {
	for key, change := range changes {
		data, closer, err := db.Get([]byte(key))
		var count uint64
		if err == nil {
			if len(data) != 8 {
				closer.Close()
				return errors.New("invalid aggregate count")
			}
			count = binary.BigEndian.Uint64(data)
			if err := closer.Close(); err != nil {
				return err
			}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
		if change < 0 && uint64(-change) > count {
			return errors.New("aggregate count underflow")
		}
		if change > 0 && count > ^uint64(0)-uint64(change) {
			return errors.New("aggregate count overflow")
		}
		if change < 0 {
			count -= uint64(-change)
		} else {
			count += uint64(change)
		}
		if count == 0 {
			if err := batch.Delete([]byte(key), nil); err != nil {
				return err
			}
		} else {
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], count)
			if err := batch.Set([]byte(key), encoded[:], nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// A missing marker causes a complete restartable rebuild before accepting
// writes. Interrupted rebuilds are discarded before starting again.
func rebuildAggregates(db *pebble.DB) error {
	return rebuildAggregate(db, aggregateMarker, "aggregate/", "aggregate0", aggregateKey)
}

func rebuildSourceAggregates(db *pebble.DB) error {
	return rebuildAggregate(db, sourceAggregateMarker, sourceAggregatePrefix, "aggregate-source0", sourceAggregateKey)
}

func rebuildAggregate(db *pebble.DB, marker, lower, upper string, key func(Event) string) error {
	_, closer, err := db.Get([]byte(marker))
	if err == nil {
		return closer.Close()
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	if err := db.DeleteRange([]byte(lower), []byte(upper), pebble.Sync); err != nil {
		return err
	}
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("events/"), UpperBound: []byte("events0")})
	if err != nil {
		return err
	}
	defer iter.Close()
	changes := make(map[string]int64)
	flush := func() error {
		batch := db.NewBatch()
		defer batch.Close()
		if err := applyCounts(db, batch, changes); err != nil {
			return err
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		clear(changes)
		return nil
	}
	n := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		var event Event
		if err := json.Unmarshal(iter.Value(), &event); err != nil {
			return err
		}
		changes[key(event)]++
		n++
		if n == 128 {
			if err := flush(); err != nil {
				return err
			}
			n = 0
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	return db.Set([]byte(marker), []byte("1"), pebble.Sync)
}

type LevelBucket struct {
	Start  time.Time         `json:"start"`
	Counts map[string]uint64 `json:"counts"`
}

// LevelStats returns complete minute-aligned buckets without loading events.
func (s *Store) LevelStats(ctx context.Context, deployment string, start, end time.Time) ([]LevelBucket, error) {
	if deployment == "" || len(deployment) > 128 || !utf8.ValidString(deployment) || start.Before(time.Unix(0, 0)) ||
		!start.Before(end) || end.Sub(start) > 31*24*time.Hour ||
		start.Unix()%60 != 0 || end.Unix()%60 != 0 || start.Nanosecond() != 0 || end.Nanosecond() != 0 {
		return nil, fmt.Errorf("%w: invalid statistics bounds", ErrInvalidInput)
	}
	p := aggregatePrefix(deployment)
	lower := fmt.Sprintf("%s%012d/", p, start.Unix()/60)
	upper := fmt.Sprintf("%s%012d/", p, end.Unix()/60)
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: []byte(lower), UpperBound: []byte(upper)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	buckets := make([]LevelBucket, 0)
	var lastMinute int64 = -1
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := strings.TrimPrefix(string(iter.Key()), p)
		parts := strings.Split(key, "/")
		if len(parts) != 2 || len(iter.Value()) != 8 {
			return nil, errors.New("invalid aggregate entry")
		}
		minute, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid aggregate minute: %w", err)
		}
		switch parts[1] {
		case "trace", "debug", "info", "warn", "error", "fatal":
		default:
			return nil, errors.New("invalid aggregate level")
		}
		if minute != lastMinute {
			buckets = append(buckets, LevelBucket{Start: time.Unix(minute*60, 0).UTC(), Counts: make(map[string]uint64)})
			lastMinute = minute
		}
		buckets[len(buckets)-1].Counts[parts[1]] = binary.BigEndian.Uint64(iter.Value())
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return buckets, nil
}

type SourceCount struct {
	App    string `json:"app"`
	Source string `json:"source"`
	Count  uint64 `json:"count"`
}

// SourceStats aggregates minute buckets without fetching event bodies.
func (s *Store) SourceStats(ctx context.Context, deployment string, start, end time.Time) ([]SourceCount, error) {
	if !validDeployment(deployment) || start.Before(time.Unix(0, 0)) || !start.Before(end) ||
		end.Sub(start) > 31*24*time.Hour || start.Unix()%60 != 0 || end.Unix()%60 != 0 ||
		start.Nanosecond() != 0 || end.Nanosecond() != 0 {
		return nil, fmt.Errorf("%w: invalid statistics bounds", ErrInvalidInput)
	}
	p := sourceAggregatePrefix + segment(deployment) + "/"
	lower := fmt.Sprintf("%s%012d/", p, start.Unix()/60)
	upper := fmt.Sprintf("%s%012d/", p, end.Unix()/60)
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: []byte(lower), UpperBound: []byte(upper)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	type sourceKey struct{ app, source string }
	groups := make(map[sourceKey]uint64)
	scanned := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		scanned++
		if scanned > 100000 {
			return nil, ErrQueryBudget
		}
		parts := strings.Split(strings.TrimPrefix(string(iter.Key()), p), "/")
		if len(parts) != 3 || len(parts[0]) != 12 || len(iter.Value()) != 8 {
			return nil, errors.New("invalid source aggregate entry")
		}
		app, appErr := base64.RawURLEncoding.DecodeString(parts[1])
		source, sourceErr := base64.RawURLEncoding.DecodeString(parts[2])
		if appErr != nil || sourceErr != nil || len(app) == 0 || !utf8.Valid(app) || !utf8.Valid(source) {
			return nil, errors.New("invalid source aggregate dimensions")
		}
		key := sourceKey{app: string(app), source: string(source)}
		if _, ok := groups[key]; !ok && len(groups) == 1000 {
			return nil, ErrQueryBudget
		}
		count := binary.BigEndian.Uint64(iter.Value())
		if ^uint64(0)-groups[key] < count {
			return nil, errors.New("source aggregate count overflow")
		}
		groups[key] += count
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	result := make([]SourceCount, 0, len(groups))
	for key, count := range groups {
		result = append(result, SourceCount{App: key.app, Source: key.source, Count: count})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Count != result[j].Count {
			return result[i].Count > result[j].Count
		}
		if result[i].App != result[j].App {
			return result[i].App < result[j].App
		}
		return result[i].Source < result[j].Source
	})
	return result, nil
}
