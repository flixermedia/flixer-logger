package auth

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReplaceIsAtomicAndRevokes(t *testing.T) {
	token := strings.Repeat("x", 32)
	credential := Credential{Digest: sha256.Sum256([]byte(token)), DeploymentID: "a", Scope: Query}
	verifier, err := New([]Credential{credential})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Replace([]Credential{credential, credential}); err == nil {
		t.Fatal("invalid replacement accepted")
	}
	if _, err := verifier.Verify(token, Query, time.Now()); err != nil {
		t.Fatalf("invalid replacement changed active credentials: %v", err)
	}
	if err := verifier.Replace(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(token, Query, time.Now()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("revoked credential accepted: %v", err)
	}
}
