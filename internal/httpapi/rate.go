package httpapi

import (
	"crypto/sha256"
	"math"
	"sync"
	"time"

	"github.com/flixermedia/flixer-logger/internal/store"
)

type rateBucket struct {
	tokens float64
	at     time.Time
	used   time.Time
}

type rateLimiter struct {
	mu      sync.Mutex
	buckets map[rateKey]rateBucket
}

type rateKey struct {
	deployment string
	credential [sha256.Size]byte
	app        string
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{buckets: make(map[rateKey]rateBucket)}
}

func (l *rateLimiter) allow(tenant string, credential [sha256.Size]byte, app string, count int, policy store.IngestionPolicy, now time.Time) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	type limit struct {
		key   rateKey
		rate  int
		burst int
	}
	limits := []limit{{key: rateKey{deployment: tenant}, rate: policy.EventsPerSecond, burst: policy.Burst}}
	if policy.CredentialEventsPerSecond > 0 {
		limits = append(limits, limit{key: rateKey{deployment: tenant, credential: credential}, rate: policy.CredentialEventsPerSecond, burst: policy.CredentialBurst})
	}
	if policy.ServiceEventsPerSecond > 0 {
		limits = append(limits, limit{key: rateKey{deployment: tenant, app: app}, rate: policy.ServiceEventsPerSecond, burst: policy.ServiceBurst})
	}
	if len(l.buckets)+len(limits) > 10000 {
		for id, entry := range l.buckets {
			if now.Sub(entry.used) >= 12*24*time.Hour {
				delete(l.buckets, id)
			}
		}
	}
	missing := 0
	for _, limit := range limits {
		if _, ok := l.buckets[limit.key]; !ok {
			missing++
		}
	}
	if len(l.buckets)+missing > 10000 {
		return false, 60
	}
	buckets := make([]rateBucket, len(limits))
	wait := 0
	for i, limit := range limits {
		bucket, ok := l.buckets[limit.key]
		if !ok {
			bucket = rateBucket{tokens: float64(limit.burst), at: now}
		}
		if elapsed := now.Sub(bucket.at).Seconds(); elapsed > 0 {
			bucket.tokens = math.Min(float64(limit.burst), bucket.tokens+elapsed*float64(limit.rate))
			bucket.at = now
		}
		bucket.tokens = math.Min(bucket.tokens, float64(limit.burst))
		bucket.used = now
		if bucket.tokens < float64(count) {
			wait = max(wait, int(math.Ceil((float64(count)-bucket.tokens)/float64(limit.rate))))
		}
		buckets[i] = bucket
	}
	// Charge all applicable buckets only when every limit admits the batch.
	for i, limit := range limits {
		if wait == 0 {
			buckets[i].tokens -= float64(count)
		}
		l.buckets[limit.key] = buckets[i]
	}
	return wait == 0, wait
}
