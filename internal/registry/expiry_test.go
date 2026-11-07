package registry

import (
	"errors"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestIssuancePrunesExpiredCredentialsDurably(t *testing.T) {
	dir := t.TempDir()
	storage, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i := 0; i < 100; i++ {
		if _, _, err := r.Issue("a", auth.Query, now.Add(time.Second), now); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := r.Issue("a", auth.Query, now.Add(time.Hour), now); !errors.Is(err, ErrLimit) {
		t.Fatalf("expected limit: %v", err)
	}
	later := now.Add(2 * time.Second)
	if _, _, err := r.Issue("a", auth.Query, later.Add(time.Hour), later); err != nil {
		t.Fatal(err)
	}
	if len(r.List("a")) != 1 {
		t.Fatal("expired credentials were not removed")
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err = store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	r, err = New(storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.List("a")) != 1 {
		t.Fatal("expired credentials were not durably removed")
	}
}
