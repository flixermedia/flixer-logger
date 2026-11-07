package httpapi

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestConcurrentServiceLimitDoesNotOverspend(t *testing.T) {
	limiter := newRateLimiter()
	policy := store.IngestionPolicy{
		EventsPerSecond: 1, Burst: 3000,
		CredentialEventsPerSecond: 1, CredentialBurst: 1000,
		ServiceEventsPerSecond: 1, ServiceBurst: 1000,
	}
	now := time.Now()
	credentials := [2][32]byte{sha256.Sum256([]byte("one")), sha256.Sum256([]byte("two"))}
	var accepted atomic.Int32
	var workers sync.WaitGroup
	for i := 0; i < 1500; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			if ok, _ := limiter.allow("tenant", credentials[index%2], "flixer", 1, policy, now); ok {
				accepted.Add(1)
			}
		}(i)
	}
	workers.Wait()
	if got := accepted.Load(); got != 1000 {
		t.Fatalf("service limit allowed %d events, want 1000", got)
	}
}
