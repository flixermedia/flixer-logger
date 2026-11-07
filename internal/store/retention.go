package store

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/pebble"
)

const retentionPrefix = "retention/policy/"

type RetentionPolicy struct {
	Enabled      bool           `json:"enabled"`
	Days         int            `json:"days"`
	SeverityDays map[string]int `json:"severity_days,omitempty"`
}

var retentionLevels = []string{"trace", "debug", "info", "warn", "error", "fatal"}

func validRetention(policy RetentionPolicy) bool {
	if policy.Days < 1 || policy.Days > 3650 {
		return false
	}
	for level, days := range policy.SeverityDays {
		switch level {
		case "trace", "debug", "info", "warn", "error", "fatal":
		default:
			return false
		}
		if days < 1 || days > 3650 {
			return false
		}
	}
	return true
}

func validDeployment(deployment string) bool {
	return deployment != "" && len(deployment) <= 128 && utf8.ValidString(deployment) && strings.TrimSpace(deployment) == deployment
}

func retentionKey(deployment string) []byte {
	return []byte(retentionPrefix + segment(deployment))
}

// RetentionPolicy retrieves a configured policy. The zero policy is disabled.
func (s *Store) RetentionPolicy(deployment string) (RetentionPolicy, error) {
	if !validDeployment(deployment) {
		return RetentionPolicy{}, fmt.Errorf("%w: invalid deployment", ErrInvalidInput)
	}
	return s.retentionPolicy(deployment)
}

func (s *Store) retentionPolicy(deployment string) (RetentionPolicy, error) {
	data, closer, err := s.db.Get(retentionKey(deployment))
	if errors.Is(err, pebble.ErrNotFound) {
		return RetentionPolicy{}, nil
	}
	if err != nil {
		return RetentionPolicy{}, err
	}
	var policy RetentionPolicy
	decodeErr := json.Unmarshal(data, &policy)
	closeErr := closer.Close()
	if decodeErr != nil || closeErr != nil {
		return RetentionPolicy{}, errors.Join(decodeErr, closeErr)
	}
	if !validRetention(policy) {
		return RetentionPolicy{}, errors.New("invalid stored retention policy")
	}
	return policy, nil
}

// SetRetentionPolicy durably updates a deployment's policy. An update is
// serialized with the deletion batch so a disabled policy cannot race a sweep.
func (s *Store) SetRetentionPolicy(deployment string, policy RetentionPolicy) error {
	if !validDeployment(deployment) || !validRetention(policy) {
		return fmt.Errorf("%w: invalid retention policy", ErrInvalidInput)
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Set(retentionKey(deployment), data, pebble.Sync); err != nil {
		return fmt.Errorf("save retention policy: %w", err)
	}
	return nil
}

// RetentionDeployments returns a bounded page of deployment IDs for sweeping.
func (s *Store) RetentionDeployments(after string, limit int) ([]string, string, error) {
	if limit < 1 || limit > 1000 || (after != "" && !validDeployment(after)) {
		return nil, "", fmt.Errorf("%w: invalid retention page", ErrInvalidInput)
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(retentionPrefix), UpperBound: []byte("retention/policy0")})
	if err != nil {
		return nil, "", err
	}
	defer iter.Close()
	valid := iter.First()
	if after != "" {
		valid = iter.SeekGE(retentionKey(after))
		if valid && string(iter.Key()) == string(retentionKey(after)) {
			valid = iter.Next()
		}
	}
	deployments := make([]string, 0, limit)
	for valid && len(deployments) < limit {
		encoded := strings.TrimPrefix(string(iter.Key()), retentionPrefix)
		value, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil || !validDeployment(string(value)) || segment(string(value)) != encoded {
			return nil, "", errors.New("invalid retention policy key")
		}
		deployments = append(deployments, string(value))
		valid = iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, "", err
	}
	if valid {
		return deployments, deployments[len(deployments)-1], nil
	}
	return deployments, "", nil
}

// SweepRetention deletes at most limit events, using the current policy.
func (s *Store) SweepRetention(deployment string, now time.Time, limit int) (int, error) {
	if !validDeployment(deployment) || now.IsZero() || limit < 1 || limit > 1000 {
		return 0, fmt.Errorf("%w: invalid retention sweep", ErrInvalidInput)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	policy, err := s.retentionPolicy(deployment)
	if err != nil {
		return 0, err
	}
	if !policy.Enabled {
		return 0, nil
	}
	if len(policy.SeverityDays) == 0 {
		return s.deleteBeforeLocked(deployment, now.Add(-time.Duration(policy.Days)*24*time.Hour), limit)
	}
	return s.sweepSeverityLocked(deployment, policy, now, limit)
}

func (s *Store) sweepSeverityLocked(deployment string, policy RetentionPolicy, now time.Time, limit int) (int, error) {
	type levelCursor struct {
		iter   *pebble.Iterator
		prefix string
		cutoff time.Time
		valid  bool
	}
	cursors := make([]levelCursor, 0, len(retentionLevels))
	defer func() {
		for _, cursor := range cursors {
			cursor.iter.Close()
		}
	}()
	for _, level := range retentionLevels {
		p := indexPrefix(deployment, "level", level)
		iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(p), UpperBound: []byte(p + "~")})
		if err != nil {
			return 0, err
		}
		days := policy.Days
		if override, ok := policy.SeverityDays[level]; ok {
			days = override
		}
		cursors = append(cursors, levelCursor{iter: iter, prefix: p, cutoff: now.Add(-time.Duration(days) * 24 * time.Hour), valid: iter.First()})
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	counts := make(map[string]int64)
	var deletedBytes uint64
	deleted := 0
	for deleted < limit {
		chosen := -1
		var earliest []byte
		for i := range cursors {
			cursor := &cursors[i]
			if !cursor.valid {
				if err := cursor.iter.Error(); err != nil {
					return 0, err
				}
				continue
			}
			suffix := cursor.iter.Key()[len(cursor.prefix):]
			if len(suffix) < 21 || suffix[20] != '/' {
				return 0, errors.New("invalid severity index key")
			}
			ingestedNS, err := strconv.ParseInt(string(suffix[:20]), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid severity index time: %w", err)
			}
			if !time.Unix(0, ingestedNS).Before(cursor.cutoff) {
				continue
			}
			if chosen < 0 || bytes.Compare(suffix, earliest) < 0 {
				chosen, earliest = i, suffix
			}
		}
		if chosen < 0 {
			break
		}
		cursor := &cursors[chosen]
		key := cursor.iter.Value()
		data, closer, err := s.db.Get(key)
		if err != nil {
			return 0, fmt.Errorf("read severity-indexed event: %w", err)
		}
		var event Event
		decodeErr := json.Unmarshal(data, &event)
		length := len(data)
		closeErr := closer.Close()
		if decodeErr != nil || closeErr != nil {
			return 0, errors.Join(decodeErr, closeErr)
		}
		expectedKey := []byte(fmt.Sprintf("%s%020d/%s", prefix(deployment), event.IngestedAt.UnixNano(), segment(event.EventID)))
		if event.DeploymentID != deployment || !bytes.Equal(expectedKey, key) || !bytes.Equal(indexKey(event, "level", event.Level), cursor.iter.Key()) || !event.IngestedAt.Before(cursor.cutoff) {
			return 0, errors.New("inconsistent severity index")
		}
		if err := deleteRetainedEvent(batch, key, event); err != nil {
			return 0, err
		}
		deletedBytes += uint64(length)
		changeEventCounts(counts, event, -1)
		deleted++
		cursor.valid = cursor.iter.Next()
	}
	if err := s.commitRetention(batch, deployment, deletedBytes, counts); err != nil {
		return 0, err
	}
	return deleted, nil
}
