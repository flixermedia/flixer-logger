package registry

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestBootstrapReloadPreservesIssuedCredentials(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	manager := strings.Repeat("m", 32)
	base := []auth.Credential{{Digest: sha256.Sum256([]byte(manager)), DeploymentID: "a", Scope: auth.Manage}}
	r, err := New(storage, base)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	record, token, err := r.Issue("a", auth.Query, now.Add(time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	conflict := auth.Credential{Digest: record.Digest, DeploymentID: "b", Scope: auth.Query}
	if err := r.ReplaceBootstrap([]auth.Credential{conflict}); err == nil {
		t.Fatal("duplicate credential accepted")
	}
	if _, err := r.Verifier().Verify(manager, auth.Manage, now); err != nil {
		t.Fatalf("invalid reload changed credentials: %v", err)
	}
	if err := r.ReplaceBootstrap(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Verifier().Verify(manager, auth.Manage, now); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatal("removed bootstrap credential remains active")
	}
	if identity, err := r.Verifier().Verify(token, auth.Query, now); err != nil || identity.DeploymentID != "a" {
		t.Fatalf("issued credential lost on reload: %+v %v", identity, err)
	}
}
