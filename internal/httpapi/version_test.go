package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLiveHealthIncludesVersion(t *testing.T) {
	handler := &Handler{}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/health/live", nil))
	var body struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK || body.Version != strings.TrimSpace(sourceVersion)+"-dev" {
		t.Fatalf("unexpected health response: %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestLiveHealthIncludesReleaseVersion(t *testing.T) {
	previous := Version
	Version = "1.2.3"
	defer func() { Version = previous }()
	recorder := httptest.NewRecorder()
	(&Handler{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/health/live", nil))
	var body struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK || body.Version != "1.2.3" {
		t.Fatalf("unexpected health response: %d %s", recorder.Code, recorder.Body.String())
	}
}
