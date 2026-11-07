// Package auth validates deployment-scoped logger credentials.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Scope distinguishes ingestion credentials from read-only viewer credentials.
type Scope string

const (
	Ingest Scope = "ingest"
	Query  Scope = "query"
	Manage Scope = "manage"
	Owner  Scope = "owner"
)

// Credential is a provisioned credential. Digest is the SHA-256 digest of a
// high-entropy token, not the token itself. Revocation is represented by
// removing the credential when replacing the verifier's configuration.
type Credential struct {
	Digest       [sha256.Size]byte
	DeploymentID string
	Scope        Scope
	App          string
	ExpiresAt    time.Time
}

// Identity is derived exclusively from an authenticated credential.
type Identity struct {
	DeploymentID string
	Scope        Scope
	App          string
}

// ValidApp identifies a canonical service name without allowing control characters.
func ValidApp(app string) bool {
	if len(app) < 1 || len(app) > 64 || app[0] < 'a' || app[0] > 'z' {
		return false
	}
	for _, c := range app {
		if !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '-' {
			return false
		}
	}
	return true
}

type credentialSet struct {
	credentials []Credential
}

// Verifier atomically replaces immutable credential snapshots for concurrent readers.
type Verifier struct {
	current atomic.Pointer[credentialSet]
}

// New rejects ambiguous credentials instead of allowing configuration order to
// determine which deployment receives a request.
func New(credentials []Credential) (*Verifier, error) {
	v := &Verifier{}
	if err := v.Replace(credentials); err != nil {
		return nil, err
	}
	return v, nil
}

// Replace validates the complete set before atomically applying it.
func (v *Verifier) Replace(credentials []Credential) error {
	seen := make(map[[sha256.Size]byte]bool, len(credentials))
	copyOfCredentials := make([]Credential, len(credentials))
	for i, credential := range credentials {
		if credential.Digest == ([sha256.Size]byte{}) {
			return fmt.Errorf("credential %d has an empty digest", i)
		}
		if credential.DeploymentID == "" || len(credential.DeploymentID) > 128 ||
			!utf8.ValidString(credential.DeploymentID) || strings.TrimSpace(credential.DeploymentID) != credential.DeploymentID {
			return fmt.Errorf("credential %d has an invalid deployment", i)
		}
		if credential.Scope != Ingest && credential.Scope != Query && credential.Scope != Manage && credential.Scope != Owner {
			return fmt.Errorf("credential %d has an invalid scope", i)
		}
		if credential.App != "" && (credential.Scope != Ingest || !ValidApp(credential.App)) {
			return fmt.Errorf("credential %d has an invalid application restriction", i)
		}
		if seen[credential.Digest] {
			return fmt.Errorf("credential %d has a duplicate digest", i)
		}
		seen[credential.Digest] = true
		copyOfCredentials[i] = credential
	}
	v.current.Store(&credentialSet{credentials: copyOfCredentials})
	return nil
}

var ErrUnauthorized = errors.New("invalid or expired credential")

// Verify authenticates the token and checks its scope and expiration. No
// deployment supplied by the request is used to establish identity.
func (v *Verifier) Verify(token string, scope Scope, now time.Time) (Identity, error) {
	if len(token) < 32 || len(token) > 1024 {
		return Identity{}, ErrUnauthorized
	}
	digest := sha256.Sum256([]byte(token))
	var identity Identity
	matched := 0
	set := v.current.Load()
	if set == nil {
		return Identity{}, ErrUnauthorized
	}
	for _, credential := range set.credentials {
		equal := subtle.ConstantTimeCompare(digest[:], credential.Digest[:])
		if equal == 1 && credential.Scope == scope &&
			(credential.ExpiresAt.IsZero() || now.Before(credential.ExpiresAt)) {
			identity = Identity{DeploymentID: credential.DeploymentID, Scope: credential.Scope, App: credential.App}
			matched = 1
		}
	}
	if matched != 1 {
		return Identity{}, ErrUnauthorized
	}
	return identity, nil
}
