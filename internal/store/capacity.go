package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"syscall"

	"github.com/cockroachdb/pebble"
)

var ErrCapacityExceeded = errors.New("logger storage capacity threshold exceeded")

var capacityPolicyKey = []byte("capacity/global-policy-v1")

// CapacityPolicy is a global, owner-controlled best-effort ingestion guard.
// Zero disables the corresponding threshold.
type CapacityPolicy struct {
	MaxDatabaseBytes uint64 `json:"max_database_bytes"`
	MinFreeBytes     uint64 `json:"min_free_bytes"`
}

func readCapacityPolicy(db *pebble.DB) (CapacityPolicy, error) {
	value, closer, err := db.Get(capacityPolicyKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return CapacityPolicy{}, nil
	}
	if err != nil {
		return CapacityPolicy{}, err
	}
	var policy CapacityPolicy
	decodeErr := json.Unmarshal(value, &policy)
	closeErr := closer.Close()
	if decodeErr != nil || closeErr != nil {
		return CapacityPolicy{}, errors.Join(decodeErr, closeErr)
	}
	if policy.MaxDatabaseBytes > 1<<50 || policy.MinFreeBytes > 1<<50 {
		return CapacityPolicy{}, errors.New("invalid stored capacity policy")
	}
	return policy, nil
}

func (s *Store) CapacityPolicy() CapacityPolicy {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.capacityPolicy
}

// SetCapacityPolicy serializes policy updates with event ingestion.
func (s *Store) SetCapacityPolicy(policy CapacityPolicy) error {
	if policy.MaxDatabaseBytes > 1<<50 || policy.MinFreeBytes > 1<<50 {
		return fmt.Errorf("%w: capacity threshold exceeds 1 PiB", ErrInvalidInput)
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Set(capacityPolicyKey, data, pebble.Sync); err != nil {
		return fmt.Errorf("save capacity policy: %w", err)
	}
	s.capacityPolicy = policy
	return nil
}

func (s *Store) checkCapacityLocked(batchBytes uint64) error {
	policy := s.capacityPolicy
	if policy.MaxDatabaseBytes == 0 && policy.MinFreeBytes == 0 {
		return nil
	}
	capacity, err := s.Capacity()
	if err != nil {
		return fmt.Errorf("check ingestion capacity: %w", err)
	}
	if !capacityAllows(policy, capacity, batchBytes) {
		return ErrCapacityExceeded
	}
	return nil
}

func capacityAllows(policy CapacityPolicy, capacity Capacity, batchBytes uint64) bool {
	if (policy.MaxDatabaseBytes > 0 && (capacity.DatabaseBytes >= policy.MaxDatabaseBytes ||
		batchBytes > policy.MaxDatabaseBytes-capacity.DatabaseBytes)) ||
		(policy.MinFreeBytes > 0 && (capacity.FilesystemAvailableBytes <= policy.MinFreeBytes ||
			batchBytes > capacity.FilesystemAvailableBytes-policy.MinFreeBytes)) {
		return false
	}
	return true
}

// Capacity reports global storage information and must not be exposed to tenants.
type Capacity struct {
	FilesystemTotalBytes     uint64 `json:"filesystem_total_bytes"`
	FilesystemAvailableBytes uint64 `json:"filesystem_available_bytes"`
	DatabaseBytes            uint64 `json:"database_bytes"`
	CompactionDebtBytes      uint64 `json:"compaction_debt_bytes"`
	CompactionsInProgress    int64  `json:"compactions_in_progress"`
}

func filesystemBytes(blocks uint64, blockSize int64) (uint64, error) {
	if blockSize <= 0 || blocks > math.MaxUint64/uint64(blockSize) {
		return 0, errors.New("invalid filesystem capacity")
	}
	return blocks * uint64(blockSize), nil
}

// Capacity returns filesystem availability and Pebble-reported disk usage.
func (s *Store) Capacity() (Capacity, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(s.path, &fs); err != nil {
		return Capacity{}, fmt.Errorf("inspect logger filesystem: %w", err)
	}
	total, err := filesystemBytes(fs.Blocks, fs.Bsize)
	if err != nil {
		return Capacity{}, err
	}
	available, err := filesystemBytes(fs.Bavail, fs.Bsize)
	if err != nil {
		return Capacity{}, err
	}
	metrics := s.db.Metrics()
	return Capacity{
		FilesystemTotalBytes: total, FilesystemAvailableBytes: available,
		DatabaseBytes: metrics.DiskSpaceUsage(), CompactionDebtBytes: metrics.Compact.EstimatedDebt,
		CompactionsInProgress: metrics.Compact.NumInProgress,
	}, nil
}
