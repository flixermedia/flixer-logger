// Package registry manages deployment-scoped, dynamically issued credentials.
package registry

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

var ErrNotFound = errors.New("credential not found")
var ErrLimit = errors.New("credential limit reached")

type Registry struct {
	mu       sync.Mutex
	store    *store.Store
	verifier *auth.Verifier
	base     []auth.Credential
	records  []store.StoredCredential
	pairings []store.Pairing
}

func New(storage *store.Store, base []auth.Credential) (*Registry, error) {
	if storage == nil {
		return nil, errors.New("credential storage is required")
	}
	records, err := storage.LoadCredentials()
	if err != nil {
		return nil, fmt.Errorf("load credentials: %w", err)
	}
	pairings, err := storage.LoadPairings()
	if err != nil {
		return nil, fmt.Errorf("load pairings: %w", err)
	}
	credentials, err := combine(base, records)
	if err != nil {
		return nil, err
	}
	verifier, err := auth.New(credentials)
	if err != nil {
		return nil, fmt.Errorf("validate credentials: %w", err)
	}
	return &Registry{store: storage, verifier: verifier, base: append([]auth.Credential(nil), base...), records: records, pairings: pairings}, nil
}

func combine(base []auth.Credential, records []store.StoredCredential) ([]auth.Credential, error) {
	credentials := append([]auth.Credential(nil), base...)
	for _, record := range records {
		if (record.Scope != auth.Ingest && record.Scope != auth.Query && record.Scope != auth.Manage) || record.ExpiresAt.IsZero() {
			return nil, errors.New("invalid stored credential scope or expiry")
		}
		credentials = append(credentials, auth.Credential{
			Digest: record.Digest, DeploymentID: record.DeploymentID, Scope: record.Scope, App: record.App, ExpiresAt: record.ExpiresAt,
		})
	}
	return credentials, nil
}

func (r *Registry) Verifier() *auth.Verifier { return r.verifier }

// ReplaceBootstrap reloads operator-managed credentials without discarding
// credentials issued through the registry.
func (r *Registry) ReplaceBootstrap(base []auth.Credential) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	credentials, err := combine(base, r.records)
	if err != nil {
		return err
	}
	if err := r.verifier.Replace(credentials); err != nil {
		return err
	}
	r.base = append([]auth.Credential(nil), base...)
	return nil
}

// Issue returns the token exactly once; only its digest is persisted.
func (r *Registry) Issue(deployment string, scope auth.Scope, expiry, now time.Time) (store.StoredCredential, string, error) {
	return r.IssueForApp(deployment, scope, "", expiry, now)
}

// IssueForApp optionally restricts an ingestion credential to one application.
func (r *Registry) IssueForApp(deployment string, scope auth.Scope, app string, expiry, now time.Time) (store.StoredCredential, string, error) {
	if scope != auth.Ingest && scope != auth.Query {
		return store.StoredCredential{}, "", fmt.Errorf("%w: invalid credential scope", store.ErrInvalidInput)
	}
	return r.issue(deployment, scope, app, expiry, now)
}

// IssueManagement is reserved for authenticated logger-owner operations.
func (r *Registry) IssueManagement(deployment string, expiry, now time.Time) (store.StoredCredential, string, error) {
	return r.issue(deployment, auth.Manage, "", expiry, now)
}

func (r *Registry) issue(deployment string, scope auth.Scope, app string, expiry, now time.Time) (store.StoredCredential, string, error) {
	if (app != "" && (scope != auth.Ingest || !auth.ValidApp(app))) ||
		!expiry.After(now) || expiry.After(now.Add(365*24*time.Hour)) {
		return store.StoredCredential{}, "", fmt.Errorf("%w: invalid credential scope or expiry", store.ErrInvalidInput)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.issueLocked(deployment, scope, app, expiry, now, nil)
}

func (r *Registry) issueLocked(deployment string, scope auth.Scope, app string, expiry, now time.Time, pairing *store.Pairing) (store.StoredCredential, string, error) {
	records := make([]store.StoredCredential, 0, len(r.records)+1)
	var expired []store.StoredCredential
	for _, record := range r.records {
		if !now.Before(record.ExpiresAt) {
			expired = append(expired, record)
		} else {
			records = append(records, record)
		}
	}
	if len(records) >= 10000 {
		return store.StoredCredential{}, "", ErrLimit
	}
	count := 0
	for _, record := range records {
		if record.DeploymentID == deployment {
			count++
		}
	}
	if count >= 100 {
		return store.StoredCredential{}, "", ErrLimit
	}
	var tokenBytes [32]byte
	var id [16]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return store.StoredCredential{}, "", err
	}
	if _, err := rand.Read(id[:]); err != nil {
		return store.StoredCredential{}, "", err
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes[:])
	record := store.StoredCredential{ID: hex.EncodeToString(id[:]), Digest: sha256.Sum256([]byte(token)), DeploymentID: deployment, Scope: scope, App: app, ExpiresAt: expiry.UTC()}
	for _, existing := range records {
		if existing.ID == record.ID && existing.DeploymentID == deployment {
			return store.StoredCredential{}, "", errors.New("credential ID collision")
		}
	}
	records = append(records, record)
	credentials, err := combine(r.base, records)
	if err != nil {
		return store.StoredCredential{}, "", err
	}
	if _, err := auth.New(credentials); err != nil {
		return store.StoredCredential{}, "", fmt.Errorf("validate issued credential: %w", err)
	}
	var persistErr error
	if pairing == nil {
		persistErr = r.store.ReplaceCredentials(record, expired)
	} else {
		persistErr = r.store.RedeemPairing(record, expired, *pairing)
	}
	if persistErr != nil {
		return store.StoredCredential{}, "", fmt.Errorf("persist credential: %w", persistErr)
	}
	if err := r.verifier.Replace(credentials); err != nil {
		return store.StoredCredential{}, "", fmt.Errorf("activate credential: %w", err)
	}
	r.records = records
	return record, token, nil
}

func (r *Registry) Revoke(deployment, id string) error {
	return r.revoke(deployment, id, false)
}

// RevokeManagement only revokes owner-issued management credentials.
func (r *Registry) RevokeManagement(deployment, id string) error {
	return r.revoke(deployment, id, true)
}

func (r *Registry) revoke(deployment, id string, management bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := -1
	for i, record := range r.records {
		if record.DeploymentID == deployment && record.ID == id && (record.Scope == auth.Manage) == management {
			index = i
			break
		}
	}
	if index < 0 {
		return ErrNotFound
	}
	records := append([]store.StoredCredential(nil), r.records[:index]...)
	records = append(records, r.records[index+1:]...)
	credentials, err := combine(r.base, records)
	if err != nil {
		return err
	}
	if err := r.store.DeleteCredential(deployment, id); err != nil {
		return fmt.Errorf("delete credential: %w", err)
	}
	if err := r.verifier.Replace(credentials); err != nil {
		return fmt.Errorf("revoke credential: %w", err)
	}
	r.records = records
	return nil
}

func (r *Registry) List(deployment string) []store.StoredCredential {
	r.mu.Lock()
	defer r.mu.Unlock()
	var records []store.StoredCredential
	for _, record := range r.records {
		if record.DeploymentID == deployment && record.Scope != auth.Manage {
			records = append(records, record)
		}
	}
	return records
}

// ListManagement returns only owner-issued management credential metadata.
func (r *Registry) ListManagement(deployment string) []store.StoredCredential {
	r.mu.Lock()
	defer r.mu.Unlock()
	records := make([]store.StoredCredential, 0)
	for _, record := range r.records {
		if record.DeploymentID == deployment && record.Scope == auth.Manage {
			records = append(records, record)
		}
	}
	return records
}
