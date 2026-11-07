package httpapi

import (
	"net/http"
	"sync"
)

type tenantCapacity struct {
	mu     sync.Mutex
	active map[string]int
	limit  int
}

func newTenantCapacity(limit int) *tenantCapacity {
	return &tenantCapacity{active: make(map[string]int), limit: limit}
}

func (c *tenantCapacity) count(tenant string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active[tenant]
}

func (c *tenantCapacity) acquire(tenant string) (func(), bool) {
	c.mu.Lock()
	if c.active[tenant] >= c.limit {
		c.mu.Unlock()
		return nil, false
	}
	c.active[tenant]++
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.active[tenant]--
		if c.active[tenant] == 0 {
			delete(c.active, tenant)
		}
	}, true
}

func acquireCapacity(w http.ResponseWriter, slots chan struct{}, tenants *tenantCapacity, tenant string) (func(), bool) {
	select {
	case slots <- struct{}{}:
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "request capacity exceeded")
		return nil, false
	}
	releaseTenant, ok := tenants.acquire(tenant)
	if !ok {
		<-slots
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "deployment request capacity exceeded")
		return nil, false
	}
	return func() {
		releaseTenant()
		<-slots
	}, true
}
