package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/flixermedia/flixer-logger/internal/auth"
)

const credentialPrefix = "credentials/"

type StoredCredential struct {
	ID           string     `json:"id"`
	Digest       [32]byte   `json:"digest"`
	DeploymentID string     `json:"deployment_id"`
	Scope        auth.Scope `json:"scope"`
	App          string     `json:"app,omitempty"`
	ExpiresAt    time.Time  `json:"expires_at"`
}

func credentialKey(deployment, id string) []byte {
	return []byte(credentialPrefix + segment(deployment) + "/" + id)
}

func (s *Store) SaveCredential(record StoredCredential) error {
	return s.ReplaceCredentials(record, nil)
}

// ReplaceCredentials atomically issues one credential and removes expired ones.
func (s *Store) ReplaceCredentials(record StoredCredential, expired []StoredCredential) error {
	return s.replaceCredentials(record, expired, nil)
}

// RedeemPairing atomically consumes a pairing and issues its management credential.
func (s *Store) RedeemPairing(record StoredCredential, expired []StoredCredential, pairing Pairing) error {
	if !validPairing(pairing) || record.DeploymentID != pairing.DeploymentID || record.Scope != auth.Manage ||
		!record.ExpiresAt.Equal(pairing.ManagementExpiry) {
		return fmt.Errorf("%w: invalid pairing credential", ErrInvalidInput)
	}
	return s.replaceCredentials(record, expired, &pairing)
}

func (s *Store) replaceCredentials(record StoredCredential, expired []StoredCredential, pairing *Pairing) error {
	if !validDeployment(record.DeploymentID) || len(record.ID) != 32 || strings.Trim(record.ID, "0123456789abcdef") != "" {
		return fmt.Errorf("%w: invalid credential identity", ErrInvalidInput)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if pairing != nil {
		if err := batch.Delete(pairingKey(pairing.ID), nil); err != nil {
			return err
		}
	}
	for _, entry := range expired {
		if !validDeployment(entry.DeploymentID) || len(entry.ID) != 32 || strings.Trim(entry.ID, "0123456789abcdef") != "" {
			return fmt.Errorf("%w: invalid expired credential identity", ErrInvalidInput)
		}
		if err := batch.Delete(credentialKey(entry.DeploymentID, entry.ID), nil); err != nil {
			return err
		}
	}
	if err := batch.Set(credentialKey(record.DeploymentID, record.ID), data, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) DeleteCredential(deployment, id string) error {
	if !validDeployment(deployment) || len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" {
		return fmt.Errorf("%w: invalid credential identity", ErrInvalidInput)
	}
	return s.db.Delete(credentialKey(deployment, id), pebble.Sync)
}

func (s *Store) LoadCredentials() ([]StoredCredential, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(credentialPrefix), UpperBound: []byte("credentials0")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	records := make([]StoredCredential, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		if len(records) >= 10000 {
			return nil, errors.New("stored credential limit exceeded")
		}
		var record StoredCredential
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			return nil, fmt.Errorf("decode credential: %w", err)
		}
		if !validDeployment(record.DeploymentID) || len(record.ID) != 32 ||
			strings.Trim(record.ID, "0123456789abcdef") != "" || string(iter.Key()) != string(credentialKey(record.DeploymentID, record.ID)) {
			return nil, errors.New("invalid stored credential")
		}
		records = append(records, record)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return records, nil
}
