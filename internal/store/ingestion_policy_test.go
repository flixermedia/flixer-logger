package store

import (
	"errors"
	"testing"
)

func TestIngestionPolicyPersistsAndIsIsolated(t *testing.T) {
	directory := t.TempDir()
	s, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	policy := IngestionPolicy{EventsPerSecond: 100, Burst: 1000, CredentialEventsPerSecond: 10, CredentialBurst: 1000, ServiceEventsPerSecond: 20, ServiceBurst: 1000}
	if err := s.SetIngestionPolicy("one", policy); err != nil {
		t.Fatal(err)
	}
	if err := s.SetIngestionPolicy("one", IngestionPolicy{EventsPerSecond: 0, Burst: 1000}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid policy accepted: %v", err)
	}
	if err := s.SetIngestionPolicy("one", IngestionPolicy{EventsPerSecond: 100, Burst: 1000, CredentialEventsPerSecond: 10}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("incomplete credential policy accepted: %v", err)
	}
	if err := s.SetIngestionPolicy("one", IngestionPolicy{EventsPerSecond: 100, Burst: 1000, ServiceBurst: 1000}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("incomplete service policy accepted: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.IngestionPolicy("one")
	if err != nil || got != policy {
		t.Fatalf("policy not persisted: %+v %v", got, err)
	}
	other, err := s.IngestionPolicy("two")
	if err != nil || other == policy {
		t.Fatalf("policy leaked: %+v %v", other, err)
	}
}
