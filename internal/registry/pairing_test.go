package registry

import (
	"errors"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestPairingExpiryCancellationAndFailure(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	registry, err := New(storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, expiry := range []time.Time{now, now.Add(15*time.Minute + time.Second)} {
		if _, _, err := registry.CreatePairing("tenant", expiry, now.Add(time.Hour), now); !errors.Is(err, store.ErrInvalidInput) {
			t.Fatalf("invalid pairing expiry accepted: %v", err)
		}
	}
	if _, _, err := registry.CreatePairing("tenant", now.Add(time.Minute), now.Add(time.Minute), now); !errors.Is(err, store.ErrInvalidInput) {
		t.Fatalf("invalid management expiry accepted: %v", err)
	}
	pairing, token, err := registry.CreatePairing("tenant", now.Add(time.Minute), now.Add(time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.RedeemPairing(token, now.Add(time.Minute)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired pairing redeemed: %v", err)
	}
	if err := registry.RevokePairing("other", pairing.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant revocation succeeded: %v", err)
	}
	if err := registry.RevokePairing("tenant", pairing.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.RedeemPairing(token, now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoked pairing redeemed: %v", err)
	}
	if _, _, err := registry.CreatePairing("", now.Add(time.Minute), now.Add(time.Hour), now); !errors.Is(err, store.ErrInvalidInput) {
		t.Fatalf("invalid deployment accepted: %v", err)
	}
}
