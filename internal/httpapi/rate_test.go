package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestRateLimiterRefillsAndIsolatesDeployments(t *testing.T) {
	limiter := newRateLimiter()
	policy := store.IngestionPolicy{EventsPerSecond: 100, Burst: 1000}
	now := time.Now()
	credential := sha256.Sum256([]byte("credential"))
	if allowed, _ := limiter.allow("one", credential, "", 1000, policy, now); !allowed {
		t.Fatal("initial burst denied")
	}
	if allowed, wait := limiter.allow("one", credential, "", 100, policy, now); allowed || wait != 1 {
		t.Fatalf("expected one second retry: %v %d", allowed, wait)
	}
	if allowed, _ := limiter.allow("two", credential, "", 1000, policy, now); !allowed {
		t.Fatal("other deployment starved")
	}
	if allowed, _ := limiter.allow("one", credential, "", 100, policy, now.Add(time.Second)); !allowed {
		t.Fatal("tokens not refilled")
	}
}

func TestCredentialAndDeploymentLimitsAreAtomic(t *testing.T) {
	limiter := newRateLimiter()
	now := time.Now()
	a, b := sha256.Sum256([]byte("a")), sha256.Sum256([]byte("b"))
	policy := store.IngestionPolicy{EventsPerSecond: 1, Burst: 3000, CredentialEventsPerSecond: 1, CredentialBurst: 1000}
	if allowed, _ := limiter.allow("one", a, "", 1000, policy, now); !allowed {
		t.Fatal("first credential burst rejected")
	}
	if allowed, wait := limiter.allow("one", a, "", 1000, policy, now); allowed || wait != 1000 {
		t.Fatalf("credential limit not enforced: %v %d", allowed, wait)
	}
	if allowed, _ := limiter.allow("one", b, "", 1000, policy, now); !allowed {
		t.Fatal("rejected credential consumed deployment tokens or starved another credential")
	}
	if allowed, _ := limiter.allow("one", b, "", 1, policy, now); allowed {
		t.Fatal("second credential bypassed its limit")
	}
	if allowed, _ := limiter.allow("two", a, "", 1000, policy, now); !allowed {
		t.Fatal("another deployment was starved")
	}
	if allowed, _ := limiter.allow("one", a, "", 1000, policy, now.Add(1000*time.Second)); !allowed {
		t.Fatal("credential tokens did not refill")
	}
}

func TestIngestionRateIsEnforced(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	defer storage.Close()
	if err := storage.SetIngestionPolicy("one", store.IngestionPolicy{EventsPerSecond: 1, Burst: 1000}); err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("i", 32)
	verifier, err := auth.New([]auth.Credential{{Digest: sha256.Sum256([]byte(token)), DeploymentID: "one", Scope: auth.Ingest}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	event := store.Event{SchemaVersion: 1, EventID: "id", Time: time.Now(), Level: "info", InstanceID: "node", App: "api", Message: "ready"}
	events := make([]store.Event, 1000)
	for i := range events {
		events[i] = event
	}
	send := func(events []store.Event) *httptest.ResponseRecorder {
		body, err := json.Marshal(struct {
			Events []store.Event `json:"events"`
		}{events})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/logs/batch", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := send(events); rec.Code != http.StatusOK {
		t.Fatalf("burst rejected: %d %s", rec.Code, rec.Body.String())
	}
	if rec := send([]store.Event{event}); rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("rate not enforced: %d %s", rec.Code, rec.Body.String())
	}
}
