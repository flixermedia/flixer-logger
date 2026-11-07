package store

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestRetentionPoliciesAreDurableIsolatedAndBounded(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	event := sample("event")
	for _, tenant := range []string{"a", "b", "c"} {
		if _, err := s.PutBatch(tenant, []Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetRetentionPolicy("a", RetentionPolicy{Enabled: true, Days: 14}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRetentionPolicy("b", RetentionPolicy{Enabled: false, Days: 14}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s = reopened
	defer s.Close()
	if policy, err := s.RetentionPolicy("a"); err != nil || !policy.Enabled || policy.Days != 14 {
		t.Fatalf("policy did not persist: %+v %v", policy, err)
	}
	if err := s.SetRetentionPolicy("a", RetentionPolicy{Enabled: true, Days: 0}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid policy accepted: %v", err)
	}
	future := time.Now().Add(15 * 24 * time.Hour)
	for _, tc := range []struct {
		tenant string
		want   int
	}{{"a", 1}, {"b", 0}, {"c", 0}} {
		if n, err := s.SweepRetention(tc.tenant, future, 1); err != nil || n != tc.want {
			t.Fatalf("sweep %s: %d %v", tc.tenant, n, err)
		}
	}
	for _, tenant := range []string{"b", "c"} {
		events, err := s.Recent(tenant, 10)
		if err != nil || len(events) != 1 {
			t.Fatalf("tenant %s affected: %+v %v", tenant, events, err)
		}
	}
	if err := s.SetRetentionPolicy("a", RetentionPolicy{Enabled: false, Days: 14}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutBatch("a", []Event{event}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.SweepRetention("a", future, 10); err != nil || n != 0 {
		t.Fatalf("disabled policy deleted events: %d %v", n, err)
	}
}

func TestSeverityRetentionUsesIndexedLevelsAndPreservesAccounting(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	levels := []string{"error", "info", "trace", "warn"}
	for _, level := range levels {
		event := sample(level)
		event.Level = level
		if _, err := s.PutBatch("a", []Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.PutBatch("b", []Event{sample("other")}); err != nil {
		t.Fatal(err)
	}
	policy := RetentionPolicy{Enabled: true, Days: 14, SeverityDays: map[string]int{"error": 30, "trace": 1}}
	if err := s.SetRetentionPolicy("a", policy); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s = reopened
	future := time.Now().Add(15 * 24 * time.Hour)
	for i := 0; i < 3; i++ {
		if n, err := s.SweepRetention("a", future, 1); err != nil || n != 1 {
			t.Fatalf("sweep %d: %d %v", i, n, err)
		}
	}
	if n, err := s.SweepRetention("a", future, 1); err != nil || n != 0 {
		t.Fatalf("retained error swept: %d %v", n, err)
	}
	events, err := s.Recent("a", 10)
	if err != nil || len(events) != 1 || events[0].Level != "error" {
		t.Fatalf("unexpected retained events: %+v %v", events, err)
	}
	indexed, _, err := s.QueryIndex("a", "level", "trace", "", 10)
	if err != nil || len(indexed) != 0 {
		t.Fatalf("expired severity index remains: %+v %v", indexed, err)
	}
	if policy, err := s.RetentionPolicy("a"); err != nil || policy.SeverityDays["error"] != 30 || policy.SeverityDays["trace"] != 1 {
		t.Fatalf("severity policy not retained: %+v %v", policy, err)
	}
	usage, err := s.LogicalUsage("a")
	if err != nil || usage == 0 {
		t.Fatalf("invalid remaining usage: %d %v", usage, err)
	}
	if n, err := s.SweepRetention("a", time.Now().Add(31*24*time.Hour), 10); err != nil || n != 1 {
		t.Fatalf("final sweep: %d %v", n, err)
	}
	if usage, err := s.LogicalUsage("a"); err != nil || usage != 0 {
		t.Fatalf("usage not cleared: %d %v", usage, err)
	}
	if events, err := s.Recent("b", 10); err != nil || len(events) != 1 {
		t.Fatalf("other deployment affected: %+v %v", events, err)
	}
	for _, overrides := range []map[string]int{{"bad": 1}, {"warn": 0}, {"info": 3651}} {
		if err := s.SetRetentionPolicy("a", RetentionPolicy{Days: 14, SeverityDays: overrides}); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("accepted invalid overrides %v: %v", overrides, err)
		}
	}
}

func TestRetentionDeploymentPagination(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 7; i++ {
		if err := s.SetRetentionPolicy(fmt.Sprintf("tenant-%d", i), RetentionPolicy{Days: 14}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	cursor := ""
	for {
		items, next, err := s.RetentionDeployments(cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			if seen[item] {
				t.Fatalf("duplicate deployment: %s", item)
			}
			seen[item] = true
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 7 {
		t.Fatalf("missing deployments: %v", seen)
	}
}
