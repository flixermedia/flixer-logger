package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/retention"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestStatusRequiresManageAndIsDeploymentScoped(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	defer storage.Close()
	a, b, q := strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("q", 32)
	verifier, err := auth.New([]auth.Credential{
		{Digest: sha256.Sum256([]byte(a)), DeploymentID: "a", Scope: auth.Manage},
		{Digest: sha256.Sum256([]byte(b)), DeploymentID: "b", Scope: auth.Manage},
		{Digest: sha256.Sum256([]byte(q)), DeploymentID: "a", Scope: auth.Query},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	handler.RetentionTracker().Record("a", retention.SweepStatus{CompletedAt: time.Now().UTC(), Deleted: 12, BatchFull: true})
	if _, err := storage.PutBatch("a", []store.Event{{
		SchemaVersion: 1, EventID: "one", Time: time.Now(), Level: "info",
		InstanceID: "node", App: "api", Message: "ready",
	}}); err != nil {
		t.Fatal(err)
	}
	sub, err := handler.live.subscribe("a", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer handler.live.unsubscribe(sub)
	for _, tc := range []struct {
		token       string
		code        int
		hasLatest   bool
		subscribers float64
	}{{a, 200, true, 1}, {b, 200, false, 0}, {q, 401, false, 0}} {
		req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
		req.Header.Set("Authorization", "Bearer "+tc.token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != tc.code {
			t.Fatalf("status: %d %s", rec.Code, rec.Body.String())
		}
		if tc.code == http.StatusOK {
			var data map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &data); err != nil {
				t.Fatal(err)
			}
			if (data["last_ingested_at"] != nil) != tc.hasLatest || data["live_subscribers"] != tc.subscribers ||
				data["max_queries"] != float64(4) || data["max_ingestions"] != float64(4) {
				t.Fatalf("unexpected status: %+v", data)
			}
			if tc.token == a {
				progress, ok := data["retention"].(map[string]any)
				if !ok || progress["deleted"] != float64(12) || progress["batch_full"] != true {
					t.Fatalf("unexpected retention progress: %+v", data)
				}
			} else if data["retention"] != nil {
				t.Fatalf("retention progress leaked across deployments: %+v", data)
			}
		}
	}
}
