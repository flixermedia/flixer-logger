package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

const pairingPrefix = "pairings/"

type Pairing struct {
	ID               string    `json:"id"`
	Digest           [32]byte  `json:"digest"`
	DeploymentID     string    `json:"deployment_id"`
	ExpiresAt        time.Time `json:"expires_at"`
	ManagementExpiry time.Time `json:"management_expiry"`
}

func pairingKey(id string) []byte {
	return []byte(pairingPrefix + id)
}

func validPairing(pairing Pairing) bool {
	return len(pairing.ID) == 32 && strings.Trim(pairing.ID, "0123456789abcdef") == "" &&
		pairing.Digest != ([32]byte{}) && validDeployment(pairing.DeploymentID) &&
		!pairing.ExpiresAt.IsZero() && pairing.ManagementExpiry.After(pairing.ExpiresAt)
}

// SavePairing atomically saves a challenge and removes expired challenges.
func (s *Store) SavePairing(pairing Pairing, expired []Pairing) error {
	if !validPairing(pairing) {
		return fmt.Errorf("%w: invalid pairing", ErrInvalidInput)
	}
	data, err := json.Marshal(pairing)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, entry := range expired {
		if !validPairing(entry) {
			return fmt.Errorf("%w: invalid expired pairing", ErrInvalidInput)
		}
		if err := batch.Delete(pairingKey(entry.ID), nil); err != nil {
			return err
		}
	}
	if err := batch.Set(pairingKey(pairing.ID), data, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) DeletePairing(pairing Pairing) error {
	if !validPairing(pairing) {
		return fmt.Errorf("%w: invalid pairing", ErrInvalidInput)
	}
	return s.db.Delete(pairingKey(pairing.ID), pebble.Sync)
}

func (s *Store) LoadPairings() ([]Pairing, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(pairingPrefix), UpperBound: []byte("pairings0")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	pairings := make([]Pairing, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if len(pairings) >= 10000 {
			return nil, errors.New("stored pairing limit exceeded")
		}
		var pairing Pairing
		if err := json.Unmarshal(iter.Value(), &pairing); err != nil {
			return nil, fmt.Errorf("decode pairing: %w", err)
		}
		if !validPairing(pairing) || string(iter.Key()) != string(pairingKey(pairing.ID)) {
			return nil, errors.New("invalid stored pairing")
		}
		pairings = append(pairings, pairing)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return pairings, nil
}
