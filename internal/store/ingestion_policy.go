package store

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

type IngestionPolicy struct {
	EventsPerSecond           int `json:"events_per_second"`
	Burst                     int `json:"burst"`
	CredentialEventsPerSecond int `json:"credential_events_per_second,omitempty"`
	CredentialBurst           int `json:"credential_burst,omitempty"`
	ServiceEventsPerSecond    int `json:"service_events_per_second,omitempty"`
	ServiceBurst              int `json:"service_burst,omitempty"`
}

func validOptionalRate(rate, burst int) bool {
	return (rate == 0 && burst == 0) || (rate >= 1 && rate <= 100000 && burst >= 1000 && burst <= 1000000)
}

func validIngestionPolicy(policy IngestionPolicy) bool {
	return policy.EventsPerSecond >= 1 && policy.EventsPerSecond <= 100000 &&
		policy.Burst >= 1000 && policy.Burst <= 1000000 &&
		validOptionalRate(policy.CredentialEventsPerSecond, policy.CredentialBurst) &&
		validOptionalRate(policy.ServiceEventsPerSecond, policy.ServiceBurst)
}

func ingestionPolicyKey(deployment string) []byte {
	return []byte("ingestion/policy/" + segment(deployment))
}

func (s *Store) IngestionPolicy(deployment string) (IngestionPolicy, error) {
	if !validDeployment(deployment) {
		return IngestionPolicy{}, fmt.Errorf("%w: invalid deployment", ErrInvalidInput)
	}
	data, closer, err := s.db.Get(ingestionPolicyKey(deployment))
	if errors.Is(err, pebble.ErrNotFound) {
		return IngestionPolicy{EventsPerSecond: 2000, Burst: 5000}, nil
	}
	if err != nil {
		return IngestionPolicy{}, err
	}
	var policy IngestionPolicy
	decodeErr := json.Unmarshal(data, &policy)
	closeErr := closer.Close()
	if decodeErr != nil || closeErr != nil {
		return IngestionPolicy{}, errors.Join(decodeErr, closeErr)
	}
	if !validIngestionPolicy(policy) {
		return IngestionPolicy{}, errors.New("invalid stored ingestion policy")
	}
	return policy, nil
}

func (s *Store) SetIngestionPolicy(deployment string, policy IngestionPolicy) error {
	if !validDeployment(deployment) || !validIngestionPolicy(policy) {
		return fmt.Errorf("%w: invalid ingestion policy", ErrInvalidInput)
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	if err := s.db.Set(ingestionPolicyKey(deployment), data, pebble.Sync); err != nil {
		return fmt.Errorf("save ingestion policy: %w", err)
	}
	return nil
}
