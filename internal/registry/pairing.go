package registry

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

// CreatePairing issues a short-lived, single-use enrollment capability.
func (r *Registry) CreatePairing(deployment string, expiresAt, managementExpiry, now time.Time) (store.Pairing, string, error) {
	if !expiresAt.After(now) || expiresAt.After(now.Add(15*time.Minute)) ||
		!managementExpiry.After(expiresAt) || managementExpiry.After(now.Add(365*24*time.Hour)) {
		return store.Pairing{}, "", fmt.Errorf("%w: invalid pairing expiry", store.ErrInvalidInput)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	active := make([]store.Pairing, 0, len(r.pairings)+1)
	var expired []store.Pairing
	count := 0
	for _, entry := range r.pairings {
		if !now.Before(entry.ExpiresAt) {
			expired = append(expired, entry)
		} else {
			active = append(active, entry)
			if entry.DeploymentID == deployment {
				count++
			}
		}
	}
	if len(active) >= 10000 || count >= 100 {
		return store.Pairing{}, "", ErrLimit
	}
	var tokenBytes [32]byte
	var id [16]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return store.Pairing{}, "", err
	}
	if _, err := rand.Read(id[:]); err != nil {
		return store.Pairing{}, "", err
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes[:])
	pairing := store.Pairing{
		ID: hex.EncodeToString(id[:]), Digest: sha256.Sum256([]byte(token)), DeploymentID: deployment,
		ExpiresAt: expiresAt.UTC(), ManagementExpiry: managementExpiry.UTC(),
	}
	for _, entry := range active {
		if entry.ID == pairing.ID || subtle.ConstantTimeCompare(entry.Digest[:], pairing.Digest[:]) == 1 {
			return store.Pairing{}, "", fmt.Errorf("pairing collision")
		}
	}
	if err := r.store.SavePairing(pairing, expired); err != nil {
		return store.Pairing{}, "", fmt.Errorf("save pairing: %w", err)
	}
	r.pairings = append(active, pairing)
	return pairing, token, nil
}

// RedeemPairing consumes the capability and commits a management credential atomically.
func (r *Registry) RedeemPairing(token string, now time.Time) (store.StoredCredential, string, error) {
	if len(token) != 43 {
		return store.StoredCredential{}, "", ErrNotFound
	}
	digest := sha256.Sum256([]byte(token))
	r.mu.Lock()
	defer r.mu.Unlock()
	index := -1
	for i := range r.pairings {
		if subtle.ConstantTimeCompare(digest[:], r.pairings[i].Digest[:]) == 1 {
			index = i
		}
	}
	if index < 0 || !now.Before(r.pairings[index].ExpiresAt) {
		return store.StoredCredential{}, "", ErrNotFound
	}
	pairing := r.pairings[index]
	record, issued, err := r.issueLocked(pairing.DeploymentID, auth.Manage, "", pairing.ManagementExpiry, now, &pairing)
	if err != nil {
		return store.StoredCredential{}, "", err
	}
	r.pairings = append(r.pairings[:index], r.pairings[index+1:]...)
	return record, issued, nil
}

// RevokePairing cancels an unredeemed enrollment capability.
func (r *Registry) RevokePairing(deployment, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, pairing := range r.pairings {
		if pairing.ID != id || pairing.DeploymentID != deployment {
			continue
		}
		if err := r.store.DeletePairing(pairing); err != nil {
			return fmt.Errorf("delete pairing: %w", err)
		}
		r.pairings = append(r.pairings[:i], r.pairings[i+1:]...)
		return nil
	}
	return ErrNotFound
}

// ListPairings returns pending pairing metadata for one deployment.
func (r *Registry) ListPairings(deployment string, now time.Time) []store.Pairing {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]store.Pairing, 0)
	for _, pairing := range r.pairings {
		if pairing.DeploymentID == deployment && now.Before(pairing.ExpiresAt) {
			result = append(result, pairing)
		}
	}
	return result
}
