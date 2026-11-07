package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCapacityIsolatesTenantsAndReleasesSlots(t *testing.T) {
	tenants := newTenantCapacity(2)
	global := make(chan struct{}, 3)
	rec := httptest.NewRecorder()
	first, ok := acquireCapacity(rec, global, tenants, "one")
	if !ok {
		t.Fatal("first request denied")
	}
	second, ok := acquireCapacity(rec, global, tenants, "one")
	if !ok {
		t.Fatal("second request denied")
	}
	denied := httptest.NewRecorder()
	if _, ok := acquireCapacity(denied, global, tenants, "one"); ok || denied.Code != http.StatusTooManyRequests || denied.Header().Get("Retry-After") == "" {
		t.Fatalf("tenant limit not enforced: %d", denied.Code)
	}
	third, ok := acquireCapacity(rec, global, tenants, "two")
	if !ok {
		t.Fatal("other tenant starved")
	}
	full := httptest.NewRecorder()
	if _, ok := acquireCapacity(full, global, tenants, "three"); ok || full.Code != http.StatusServiceUnavailable {
		t.Fatalf("global limit not enforced: %d", full.Code)
	}
	first()
	second()
	third()
	if len(global) != 0 || len(tenants.active) != 0 {
		t.Fatal("capacity was not released")
	}
}
