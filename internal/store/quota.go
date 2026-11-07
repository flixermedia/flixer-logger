package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/cockroachdb/pebble"
)

var ErrQuotaExceeded = errors.New("deployment log quota exceeded")

const quotaMarker = "metadata/logical-usage-v1"

type QuotaPolicy struct {
	MaxBytes uint64 `json:"max_bytes"`
}

func quotaKey(deployment string) []byte {
	return []byte("quota/policy/" + segment(deployment))
}

func usageKey(deployment string) []byte {
	return []byte("quota/usage/" + segment(deployment))
}

// rebuildUsage is run once before serving traffic, and is restart-safe.
func rebuildUsage(db *pebble.DB) error {
	_, closer, err := db.Get([]byte(quotaMarker))
	if err == nil {
		return closer.Close()
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("events/"), UpperBound: []byte("events0")})
	if err != nil {
		return err
	}
	defer iter.Close()
	counts := make(map[string]uint64)
	for valid := iter.First(); valid; valid = iter.Next() {
		var event Event
		if err := json.Unmarshal(iter.Value(), &event); err != nil {
			return fmt.Errorf("decode usage event: %w", err)
		}
		if !validDeployment(event.DeploymentID) {
			return errors.New("invalid deployment in usage event")
		}
		size := uint64(len(iter.Value()))
		if ^uint64(0)-counts[event.DeploymentID] < size {
			return errors.New("deployment usage overflow")
		}
		counts[event.DeploymentID] += size
	}
	if err := iter.Error(); err != nil {
		return err
	}
	batch := db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange([]byte("quota/usage/"), []byte("quota/usage0"), nil); err != nil {
		return err
	}
	for deployment, usage := range counts {
		if err := batch.Set(usageKey(deployment), []byte(strconv.FormatUint(usage, 10)), nil); err != nil {
			return err
		}
	}
	if err := batch.Set([]byte(quotaMarker), []byte("1"), nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) QuotaPolicy(deployment string) (QuotaPolicy, error) {
	if !validDeployment(deployment) {
		return QuotaPolicy{}, fmt.Errorf("%w: invalid deployment", ErrInvalidInput)
	}
	value, closer, err := s.db.Get(quotaKey(deployment))
	if errors.Is(err, pebble.ErrNotFound) {
		return QuotaPolicy{}, nil
	}
	if err != nil {
		return QuotaPolicy{}, err
	}
	var policy QuotaPolicy
	decodeErr := json.Unmarshal(value, &policy)
	return policy, errors.Join(decodeErr, closer.Close())
}

func (s *Store) SetQuotaPolicy(deployment string, policy QuotaPolicy) error {
	if !validDeployment(deployment) || policy.MaxBytes > 1<<50 {
		return fmt.Errorf("%w: invalid deployment quota", ErrInvalidInput)
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Set(quotaKey(deployment), data, pebble.Sync)
}

// LogicalUsage counts serialized event bodies, not Pebble indexes or disk space.
func (s *Store) LogicalUsage(deployment string) (uint64, error) {
	if !validDeployment(deployment) {
		return 0, fmt.Errorf("%w: invalid deployment", ErrInvalidInput)
	}
	value, closer, err := s.db.Get(usageKey(deployment))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	usage, parseErr := strconv.ParseUint(string(value), 10, 64)
	return usage, errors.Join(parseErr, closer.Close())
}

func setUsage(batch *pebble.Batch, deployment string, usage uint64) error {
	return batch.Set(usageKey(deployment), []byte(strconv.FormatUint(usage, 10)), nil)
}
