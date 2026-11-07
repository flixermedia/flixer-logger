package httpapi

import (
	"bufio"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flixermedia/flixer-logger/internal/auth"
	"github.com/flixermedia/flixer-logger/internal/store"
)

func TestLiveStreamClosesWhenCredentialIsReassigned(t *testing.T) {
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	token := strings.Repeat("q", 32)
	credential := auth.Credential{Digest: sha256.Sum256([]byte(token)), DeploymentID: "a", Scope: auth.Query}
	verifier, err := auth.New([]auth.Credential{credential})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(storage, verifier)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	request, err := http.NewRequest(http.MethodGet, server.URL+"/api/logs/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	if line, err := reader.ReadString('\n'); err != nil || line != ": connected\n" {
		t.Fatalf("stream initialization: %q %v", line, err)
	}
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	credential.DeploymentID = "b"
	if err := verifier.Replace([]auth.Credential{credential}); err != nil {
		t.Fatal(err)
	}
	if err := handler.live.publish([]store.Event{{DeploymentID: "a", Message: "private event"}}); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadString('\n')
	if err == nil || line != "" {
		t.Fatalf("reassigned credential received old tenant stream: %q %v", line, err)
	}
}
