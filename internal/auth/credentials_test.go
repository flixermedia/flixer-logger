package auth

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCredentialScopeExpiryAndRevocation(t *testing.T) {
	now := time.Now()
	token := strings.Repeat("x", 32)
	credential := Credential{Digest: sha256.Sum256([]byte(token)), DeploymentID: "tenant-a", Scope: Ingest, ExpiresAt: now.Add(time.Minute)}
	verifier, err := New([]Credential{credential})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := verifier.Verify(token, Ingest, now)
	if err != nil || identity.DeploymentID != "tenant-a" || identity.Scope != Ingest {
		t.Fatalf("unexpected identity: %+v %v", identity, err)
	}
	for _, tc := range []struct {
		token string
		scope Scope
		now   time.Time
	}{
		{token, Query, now},
		{token, Ingest, credential.ExpiresAt},
		{strings.Repeat("y", 32), Ingest, now},
		{"short", Ingest, now},
	} {
		if _, err := verifier.Verify(tc.token, tc.scope, tc.now); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("expected authorization failure, got %v", err)
		}
	}
	revoked, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := revoked.Verify(token, Ingest, now); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("revoked credential accepted: %v", err)
	}
}

func TestInvalidAndDuplicateCredentials(t *testing.T) {
	valid := Credential{Digest: sha256.Sum256([]byte(strings.Repeat("x", 32))), DeploymentID: "tenant", Scope: Query}
	if _, err := New([]Credential{valid, valid}); err == nil {
		t.Fatal("duplicate credential accepted")
	}
	for _, credential := range []Credential{
		{},
		{Digest: valid.Digest, DeploymentID: " tenant", Scope: Query},
		{Digest: valid.Digest, DeploymentID: "tenant", Scope: "admin"},
	} {
		if _, err := New([]Credential{credential}); err == nil {
			t.Fatal("invalid credential accepted")
		}
	}
}

func TestVerifierCopiesInput(t *testing.T) {
	token := strings.Repeat("z", 32)
	credentials := []Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "original", Scope: Query}}
	verifier, err := New(credentials)
	if err != nil {
		t.Fatal(err)
	}

	credentials[0].DeploymentID = "changed"
	identity, err := verifier.Verify(token, Query, time.Now())
	if err != nil || identity.DeploymentID != "original" {
		t.Fatalf("credential config mutated: %+v %v", identity, err)
	}

}
