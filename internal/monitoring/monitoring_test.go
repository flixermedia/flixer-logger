package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

func TestLoadRetriesWithoutBlockingAndHonorsDisabled(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/sentry/config/logger" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"enabled":false,"dsn":null}`))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	Load(ctx, server.URL, "", server.Client(), time.Millisecond)
	if calls.Load() != 2 {
		t.Fatalf("expected retry, got %d calls", calls.Load())
	}
}

func TestFetchValidatesConfig(t *testing.T) {
	for _, body := range []string{`{}`, `{"enabled":true,"dsn":null}`, `{"enabled":true,"dsn":""}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer server.Close()
			if _, err := fetch(context.Background(), server.Client(), server.URL); err == nil {
				t.Fatal("expected invalid config error")
			}
		})
	}
}

func TestLoadAppliesEnabledConfiguration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"enabled":true,"dsn":"https://public@example.invalid/1"}`))
	}))
	defer server.Close()
	previous := sentry.CurrentHub().Client()
	defer sentry.CurrentHub().BindClient(previous)

	Load(context.Background(), server.URL, "test", server.Client(), time.Millisecond)
	client := sentry.CurrentHub().Client()
	if client == nil || client.Options().Dsn != "https://public@example.invalid/1" || client.Options().Environment != "test" {
		t.Fatal("dashboard configuration was not applied")
	}
}

type memoryTransport struct{ events []*sentry.Event }

func (*memoryTransport) Configure(sentry.ClientOptions) {}
func (*memoryTransport) Flush(time.Duration) bool       { return true }
func (t *memoryTransport) SendEvent(event *sentry.Event) {
	t.events = append(t.events, event)
}

func TestDisabledAndInvalidConfiguration(t *testing.T) {
	if enabled, err := Start("", ""); err != nil || enabled {
		t.Fatalf("disabled: enabled=%v err=%v", enabled, err)
	}
	if enabled, err := Start("invalid", ""); err == nil || enabled {
		t.Fatalf("invalid: enabled=%v err=%v", enabled, err)
	}
}

func TestErrorLogsAndPanicsSendOnlySanitizedEvents(t *testing.T) {
	transport := &memoryTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn: "https://public@example.invalid/1", Transport: transport, BeforeSend: sanitizeEvent,
	})
	if err != nil {
		t.Fatal(err)
	}
	previous := sentry.CurrentHub().Client()
	sentry.CurrentHub().BindClient(client)
	defer sentry.CurrentHub().BindClient(previous)

	var logs bytes.Buffer
	logger := WrapLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	logger.With("user", "private-user").Error("private message", "error", errors.New("private-secret"))
	if !strings.Contains(logs.String(), "private message") {
		t.Fatal("local logs were changed")
	}
	func() {
		defer func() {
			if recover() != "private-panic" {
				t.Fatal("middleware did not preserve the panic")
			}
		}()
		handler := Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("private-panic")
		}))
		request := httptest.NewRequest(http.MethodGet, "/private/path", nil)
		request.Header.Set("Authorization", "private-token")
		handler.ServeHTTP(httptest.NewRecorder(), request)
	}()
	if len(transport.events) != 2 {
		t.Fatalf("expected two events, got %d", len(transport.events))
	}
	for _, event := range transport.events {
		data, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "private") || !strings.Contains(string(data), "Flixer logger internal error") {
			t.Fatalf("unexpected event: %s", data)
		}
	}
}
